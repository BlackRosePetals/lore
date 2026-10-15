// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;

use bytes::Bytes;
use bytes::BytesMut;
use lore_error_set::prelude::*;
use lore_io::IoDriver;
use lore_io::IoFile;
use lore_io::OpenOptions;
use tokio::sync::Mutex;
use tokio::sync::OwnedRwLockReadGuard;
use tokio::sync::RwLock;
use tokio::sync::RwLockReadGuard;
use tokio::sync::RwLockWriteGuard;
use zerocopy::FromBytes;
use zerocopy::Immutable;
use zerocopy::IntoBytes;

#[error_set]
pub enum PackfileError {}

#[derive(Debug)]
pub struct PackStoreRef {
    pub id: u32,
    pub offset: u32,
}

/// A packfile's id and the bytes reserved in it, as a bucket file records it, see
/// [`PackStore::unsettled_sizes`].
#[repr(C)]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, IntoBytes, FromBytes, Immutable)]
pub struct PackfileSize {
    pub id: u32,
    pub size: u32,
}

struct PackFile {
    id: u32,
    file: Option<IoFile>,
    /// The length of the file when the packfile was opened, or emptied.
    opened: u32,
    /// Marks writes not yet synced: set by a write once it lands, and claimed by the sync that
    /// covers it.
    dirty: AtomicBool,
    /// Set once the packfile takes no more writes and a sync covers every write to it, so that a
    /// crash cannot leave it shorter than an entry pointing into it. A packfile opened at the
    /// size limit is taken to be settled, by the sync that followed its last write.
    settled: AtomicBool,
    /// Set by a failed sync. A later sync could report success without the writes the failed one
    /// dropped, so every later sync fails, see [`PackStore::sync_packfile`].
    sync_failed: AtomicBool,
    syncing: Mutex<()>,
    contents: RwLock<Contents>,
}

/// What a packfile's lock guards.
struct Contents {
    /// Bytes reserved, including the ranges of writes still in flight.
    size: usize,
    /// The bytes of a packfile held in memory, which has no file.
    buffer: Vec<u8>,
}

impl PackFile {
    fn new(id: u32, file: Option<IoFile>, size: usize) -> Self {
        PackFile {
            id,
            file,
            opened: u32::try_from(size).unwrap_or(u32::MAX),
            dirty: AtomicBool::default(),
            settled: AtomicBool::new(size as u64 >= PACKSTORE_SIZE_LIMIT),
            sync_failed: AtomicBool::default(),
            syncing: Mutex::default(),
            contents: RwLock::new(Contents {
                size,
                buffer: vec![],
            }),
        }
    }
}

async fn packfile_read(file: &IoFile, offset: usize, size: usize) -> Result<Bytes, PackfileError> {
    Ok(
        crate::fs_util::retry_transient(|| file.read_exact_at(size, offset as u64))
            .await
            .internal("Failed reading from packstore file")?,
    )
}

/// A caller-owned destination for a scattering read. The pointer and length stay valid and
/// untouched for the duration of the read.
pub struct CallerBuffer {
    ptr: *mut u8,
    len: usize,
}

impl CallerBuffer {
    /// # Safety
    ///
    /// `ptr` must point to `len` writable bytes that stay valid, and are not
    /// read or written by anyone else, until the read using this buffer
    /// completes.
    pub unsafe fn new(ptr: *mut u8, len: usize) -> Self {
        CallerBuffer { ptr, len }
    }

    /// The address the buffer starts at, for a read that takes ownership of its destination.
    fn address(&self) -> usize {
        self.ptr as usize
    }

    /// The capacity available, in bytes.
    pub fn len(&self) -> usize {
        self.len
    }

    /// Whether the buffer has no capacity.
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// The destination as a slice.
    pub fn as_mut_slice(&mut self) -> &mut [u8] {
        // SAFETY: as CallerBuffer::new.
        unsafe { std::slice::from_raw_parts_mut(self.ptr, self.len) }
    }

    /// Split at `at`, keeping `..at` and returning `at..`, as [`bytes::BytesMut::split_off`] does.
    ///
    /// The two name disjoint memory, so each can be written on its own.
    pub fn split_off(&mut self, at: usize) -> CallerBuffer {
        assert!(at <= self.len, "split index out of bounds");
        // SAFETY: `at` is within this buffer, so the tail names a part of the memory the contract
        // on `new` already covers, and the head gives it up by shrinking.
        let tail = unsafe { CallerBuffer::new(self.ptr.add(at), self.len - at) };
        self.len = at;
        tail
    }
}

// SAFETY: one read holds the buffer at a time, and the contract on `new` makes the memory
// exclusive for that read's duration.
unsafe impl Send for CallerBuffer {}

impl lore_io::StableBufListMut for CallerBuffer {
    fn byte_segments_mut(&mut self) -> impl Iterator<Item = &mut [u8]> {
        // SAFETY: as CallerBuffer::new.
        std::iter::once(unsafe { std::slice::from_raw_parts_mut(self.ptr, self.len) })
    }
}

/// Read `len` bytes at `offset` into `dst`, scattering straight into it and allocating nothing.
async fn packfile_read_into(
    file: &IoFile,
    offset: usize,
    dst: &mut CallerBuffer,
    len: usize,
) -> Result<(), PackfileError> {
    let address = dst.address();
    crate::fs_util::retry_transient(move || {
        // SAFETY: `dst` is borrowed for this whole call, so the memory it names stays valid and
        // reaches nobody else. A scattering read consumes the buffer it is given, so each attempt
        // takes its own handle to that memory; `retry_transient` never overlaps two of them.
        let buffer = unsafe { CallerBuffer::new(address as *mut u8, len) };
        file.read_exact_vectored_at(buffer, offset as u64)
    })
    .await
    .internal("Failed reading from packstore file into caller buffer")?;
    Ok(())
}

/// A payload being written to a packfile, holding the packfile list read-locked while any copy of
/// it lives. The driver keeps a copy until the write completes, even when the future that issued
/// it is dropped, so that no stop, truncate or discard of the packfile overtakes the write.
#[derive(Clone)]
struct PackfileWrite(Arc<HeldWrite>);

struct HeldWrite {
    payload: Bytes,
    packfiles: OwnedRwLockReadGuard<Vec<PackFile>>,
    index: usize,
}

impl PackfileWrite {
    fn new(payload: Bytes, packfiles: OwnedRwLockReadGuard<Vec<PackFile>>, index: usize) -> Self {
        PackfileWrite(Arc::new(HeldWrite {
            payload,
            packfiles,
            index,
        }))
    }

    /// The packfile written to.
    fn packfile(&self) -> &PackFile {
        &self.0.packfiles[self.0.index]
    }

    /// Write the payload at `offset` in the packfile's file.
    async fn write_at(&self, offset: usize) -> Result<(), PackfileError> {
        #[cfg(feature = "test-util")]
        if crate::test_util::take_packfile_write_failure() {
            return Err(PackfileError::internal("Failed writing to packstore file"));
        }
        #[cfg(feature = "test-util")]
        crate::test_util::park_packfile_write(self).await;
        let Some(file) = self.packfile().file.as_ref() else {
            return Err(PackfileError::internal(
                "Packfile held in memory has no file to write",
            ));
        };
        crate::fs_util::retry_transient(|| file.write_all_at(self.clone(), offset as u64))
            .await
            .internal("Failed writing to packstore file")?;
        Ok(())
    }
}

impl AsRef<[u8]> for PackfileWrite {
    fn as_ref(&self) -> &[u8] {
        &self.0.payload
    }
}

impl lore_io::StableBuf for PackfileWrite {}

/// Maximum size of a single packfile
const PACKSTORE_SIZE_LIMIT: u64 = 3 * 1024 * 1024 * 1024;

/// The size at which a write marks its packfile full, [`PACKSTORE_SIZE_LIMIT`] unless a test set
/// it lower.
#[inline]
fn size_limit() -> u64 {
    #[cfg(feature = "test-util")]
    if let Some(limit) = crate::test_util::packfile_size_limit() {
        return limit;
    }
    PACKSTORE_SIZE_LIMIT
}

pub struct PackStore {
    path: Option<PathBuf>,
    min_count: usize,
    /// Shared with the writes in flight, see `PackfileWrite`.
    packfile: Arc<RwLock<Vec<PackFile>>>,
    /// Ids of the packfiles that take writes, ascending.
    writeable: RwLock<Vec<u32>>,
    /// Shared per-store GC counters; `resume()` feeds loaded packfile sizes into them
    /// so an over-cap store fires compaction without a startup scan. `None` for stores
    /// that don't participate in automatic GC (e.g. migration/scratch packstores).
    gc_counters: Option<Arc<crate::maintenance::GcCounters>>,
}

impl PackStore {
    pub fn new(
        path: Option<PathBuf>,
        min_count: usize,
        gc_counters: Option<Arc<crate::maintenance::GcCounters>>,
    ) -> Self {
        PackStore {
            path: path.map(|path| {
                let mut path = path;
                path.push("pack");
                path
            }),
            min_count,
            packfile: Arc::default(),
            writeable: RwLock::default(),
            gc_counters,
        }
    }

    /// Load the packfiles from disk unless they are loaded, and open packfiles to writes up to
    /// the minimum count. A load that fails leaves no packfile loaded, so that a later call loads
    /// them all again.
    pub async fn resume(&self) -> Result<(), PackfileError> {
        let mut packfile = self.packfile.write().await;
        let mut writeable = self.writeable.write().await;

        if packfile.is_empty()
            && let Some(path) = self.path.as_ref()
        {
            let (loaded, loaded_writeable, loaded_size) = Self::load_packfiles(path).await?;
            *packfile = loaded;
            *writeable = loaded_writeable;
            lore_base::lore_trace!("{} writable packfiles", writeable.len());
            if let Some(gc) = &self.gc_counters {
                gc.add_loaded_size(loaded_size);
            }
        }

        let _ = self.fill_writeable(packfile, writeable).await;
        Ok(())
    }

    /// The packfiles in `path`, the packfile with id `n` at index `n - 1` up to the highest id
    /// found, with the ascending ids of those under the size limit and their total size in bytes.
    /// A packfile missing below the highest id is created empty.
    async fn load_packfiles(path: &Path) -> Result<(Vec<PackFile>, Vec<u32>, u64), PackfileError> {
        if !path.exists() {
            IoDriver::global()
                .create_dir_all(path)
                .await
                .internal_with(|| {
                    format!("Failed to create packstore directory {}", path.display())
                })?;
        }

        let paths = std::fs::read_dir(path)
            .internal_with(|| format!("Failed to read packstore directory {}", path.display()))?;
        let mut packfile_count = 0;
        for entry in paths {
            let Ok(entry) = entry else {
                continue;
            };
            let Ok(file_meta) = IoDriver::global().metadata(entry.path()).await else {
                continue;
            };
            if !file_meta.is_file() {
                continue;
            }

            let file_name = entry.file_name();
            let Some(file_id) = file_name.to_str() else {
                continue;
            };
            let Ok(file_id) = file_id.parse::<u32>() else {
                continue;
            };
            packfile_count = packfile_count.max(file_id);
        }

        let mut packfiles = Vec::with_capacity(packfile_count as usize);
        let mut writeable = vec![];
        let mut loaded_size = 0;
        for file_id in 1..=packfile_count {
            let mut file = Self::open_packfile(path, file_id).await?;
            let size = file.contents.get_mut().size;
            lore_base::lore_trace!("Resuming packfile {file_id}");
            loaded_size += size as u64;
            if (size as u64) < PACKSTORE_SIZE_LIMIT {
                writeable.push(file_id);
            }
            packfiles.push(file);
        }
        Ok((packfiles, writeable, loaded_size))
    }

    /// Open packfile `id` in `path` with its size, creating it empty when there is no file. An
    /// existing file is opened as it is, never truncated, since an entry may point into it. A
    /// packfile is the store's own: on Windows nothing outside this process may write it while it
    /// is open, where the driver shares the files it opens by default.
    async fn open_packfile(path: &Path, id: u32) -> Result<PackFile, PackfileError> {
        let file_path = path.join(id.to_string());
        let file_options = OpenOptions::new().read(true).write(true).create(true);
        #[cfg(target_family = "windows")]
        let file_options =
            file_options.share_mode(windows_sys::Win32::Storage::FileSystem::FILE_SHARE_READ);
        let file = IoDriver::global()
            .open(file_path.as_path(), &file_options)
            .await
            .internal_with(|| format!("Failed opening packstore file {}", file_path.display()))?;
        let size = file
            .metadata()
            .await
            .internal_with(|| {
                format!("Failed reading packstore file size {}", file_path.display())
            })?
            .len();
        Ok(PackFile::new(id, Some(file), size as usize))
    }

    /// Take packfile `id` off the writeable list, as when it is full or a sync of it failed, which
    /// more than one caller may do, and open packfiles to keep the minimum count taking writes.
    /// The caller that takes it off settles it, see [`PackStore::settle`].
    async fn mark_full(&self, id: u32) {
        let packfile = self.packfile.write().await;
        let mut writeable = self.writeable.write().await;
        let taken_off = writeable
            .iter()
            .position(|writeable_id| *writeable_id == id)
            .map(|position| writeable.remove(position))
            .is_some();

        let _ = self.fill_writeable(packfile, writeable).await;
        if taken_off {
            self.settle(id);
        }
    }

    /// Sync packfile `id`, taken off the writeable list, in a task of its own, and mark it settled
    /// once the sync succeeds, see [`PackFile::settled`]. Taking it off waited for every write to
    /// it in flight, so the sync covers them all. The task holds the packfile list, so that no
    /// truncate empties the packfile before it is marked.
    fn settle(&self, id: u32) {
        let packfiles = self.packfile.clone();
        lore_base::lore_spawn_guarded!(async move {
            let packfiles = packfiles.read_owned().await;
            if let Some(packfile) = packfiles.get(id as usize - 1)
                && Self::sync_packfile(packfile).await.is_ok()
            {
                packfile.settled.store(true, Ordering::Release);
            }
        });
    }

    /// Open packfiles after the last until the minimum count take writes. A file already at the
    /// path of a new packfile is kept and appended to, see [`PackStore::open_packfile`].
    async fn fill_writeable<'a>(
        &'a self,
        mut packfile: RwLockWriteGuard<'a, Vec<PackFile>>,
        mut writeable: RwLockWriteGuard<'a, Vec<u32>>,
    ) -> Result<(), PackfileError> {
        while writeable.len() < self.min_count {
            let id = (packfile.len() + 1) as u32;
            lore_base::lore_trace!("Create additional packfile {id}");
            let mut file = match self.path.as_ref() {
                Some(path) => Self::open_packfile(path, id).await?,
                None => PackFile::new(id, None, 0),
            };
            let takes_writes = (file.contents.get_mut().size as u64) < PACKSTORE_SIZE_LIMIT;
            packfile.push(file);
            if takes_writes {
                writeable.push(id);
            }
        }

        Ok(())
    }

    /// The packfile list, loaded from disk first if it is not yet.
    async fn loaded_packfiles(&self) -> Result<RwLockReadGuard<'_, Vec<PackFile>>, PackfileError> {
        let packfiles = self.packfile.read().await;
        if !packfiles.is_empty() {
            return Ok(packfiles);
        }
        drop(packfiles);
        self.resume().await?;
        Ok(self.packfile.read().await)
    }

    /// As [`PackStore::loaded_packfiles`], with a guard that owns its hold on the list.
    async fn loaded_packfiles_owned(
        &self,
    ) -> Result<OwnedRwLockReadGuard<Vec<PackFile>>, PackfileError> {
        let packfiles = self.packfile.clone().read_owned().await;
        if !packfiles.is_empty() {
            return Ok(packfiles);
        }
        drop(packfiles);
        self.resume().await?;
        Ok(self.packfile.clone().read_owned().await)
    }

    /// The bytes reserved in every packfile, including the ranges of writes still in flight, the
    /// packfile with id `n` at index `n - 1`.
    pub async fn sizes(&self) -> Result<Vec<usize>, PackfileError> {
        if self.path.is_none() {
            return Err(PackfileError::internal(
                "Packfile sizes need a packstore on disk",
            ));
        }

        let packfiles = self.loaded_packfiles().await?;
        let mut sizes = Vec::with_capacity(packfiles.len());
        for packfile in packfiles.iter() {
            sizes.push(packfile.contents.read().await.size);
        }
        Ok(sizes)
    }

    /// Write the bytes reserved in each packfile not settled, see [`PackFile::settled`], to the
    /// start of `sizes`, and return how many, or `None` when `sizes` cannot hold them. Every entry
    /// added before this call points below the size written for its packfile, or into a settled
    /// packfile, which a crash cannot leave shorter than the entry. Packfiles not yet loaded are
    /// left out, and not loaded: no write in this process went to them, since a write loads them
    /// first, and no entry that points into them was loaded from a file recording sizes, since
    /// loading one compares the sizes with them, see [`PackStore::short_of`].
    pub async fn unsettled_sizes(&self, sizes: &mut [PackfileSize]) -> Option<usize> {
        let packfiles = self.packfile.read().await;
        let mut count = 0;
        for packfile in packfiles
            .iter()
            .filter(|packfile| !packfile.settled.load(Ordering::Acquire))
        {
            let recorded = sizes.get_mut(count)?;
            let size = packfile.contents.read().await.size;
            *recorded = PackfileSize {
                id: packfile.id,
                size: u32::try_from(size).unwrap_or(u32::MAX),
            };
            count += 1;
        }
        Some(count)
    }

    /// Whether the packfile of one of `sizes` was shorter when opened than recorded, a missing
    /// packfile taken to be empty, as when a crash lost the end of a packfile that a bucket file
    /// recording `sizes` points into, see [`PackFile::opened`].
    pub async fn short_of(
        &self,
        sizes: impl IntoIterator<Item = PackfileSize>,
    ) -> Result<bool, PackfileError> {
        let packfiles = self.loaded_packfiles().await?;
        Ok(sizes.into_iter().any(|recorded| {
            let opened = (recorded.id as usize)
                .checked_sub(1)
                .and_then(|index| packfiles.get(index))
                .map_or(0, |packfile| packfile.opened);
            opened < recorded.size
        }))
    }

    /// The length of each packfile's file when it was opened, see [`PackFile::opened`], the
    /// packfile with id `n` at index `n - 1`.
    pub async fn opened_lengths(&self) -> Result<Vec<u32>, PackfileError> {
        let packfiles = self.loaded_packfiles().await?;
        Ok(packfiles.iter().map(|packfile| packfile.opened).collect())
    }

    /// Stop writes to the packfiles `ids` and return their sizes in bytes, in the same order,
    /// opening packfiles to keep the minimum count taking writes. An invalid id stops none.
    pub async fn stop_writes(&self, ids: &[u32]) -> Result<Vec<usize>, PackfileError> {
        if self.path.is_none() {
            return Err(PackfileError::internal(
                "Stopping writes needs a packstore on disk",
            ));
        }
        drop(self.loaded_packfiles().await?);

        let mut packfile = self.packfile.write().await;
        let mut writeable = self.writeable.write().await;
        let mut sizes = Vec::with_capacity(ids.len());
        for id in ids {
            let Some(stopped) = (*id as usize)
                .checked_sub(1)
                .and_then(|index| packfile.get_mut(index))
            else {
                return Err(PackfileError::internal("Invalid packfile"));
            };
            sizes.push(stopped.contents.get_mut().size);
        }
        writeable.retain(|id| !ids.contains(id));
        let _ = self.fill_writeable(packfile, writeable).await;
        Ok(sizes)
    }

    /// Truncate packfile `id`, which nothing may reference, to zero bytes and let it take writes
    /// again, after discarding the empty packfiles at the end, see
    /// [`PackStore::discard_empty_tail`]. A packfile that is itself discarded is removed once
    /// truncated. A failed truncate or sync is reported and leaves the packfile out of writes, see
    /// [`PackStore::empty_packfile`].
    pub async fn truncate(&self, id: u32) -> Result<(), PackfileError> {
        if self.path.is_none() {
            return Err(PackfileError::internal(
                "Truncating a packfile needs a packstore on disk",
            ));
        }

        if id == 0 {
            return Err(PackfileError::internal("Invalid packfile"));
        }

        let mut packfile = self.packfile.write().await;
        let mut writeable = self.writeable.write().await;

        if writeable.contains(&id) {
            lore_base::lore_warn!("Tried truncating a writeable packfile {id}");
            return Err(PackfileError::internal(
                "Cannot truncate a writeable packfile",
            ));
        }

        let index = (id - 1) as usize;
        if index >= packfile.len() {
            return Err(PackfileError::internal("Invalid packfile"));
        }

        Self::empty_packfile(&mut packfile[index]).await?;
        self.discard_empty_tail(&mut packfile, &mut writeable).await;
        if index < packfile.len() {
            lore_base::lore_debug!("Truncated compacted packfile {id} to zero");
            if let Err(position) = writeable.binary_search(&id) {
                writeable.insert(position, id);
            }
        }
        let _ = self.fill_writeable(packfile, writeable).await;

        Ok(())
    }

    /// Truncate `packfile`'s file to zero bytes and sync it, then record the packfile empty and not
    /// settled. A failed truncate leaves the packfile as it was. A failed sync after it records the
    /// packfile empty, as its file now is, and is reported, and fails later syncs of it, see
    /// [`PackFile::sync_failed`].
    async fn empty_packfile(packfile: &mut PackFile) -> Result<(), PackfileError> {
        let synced = match packfile.file.as_ref() {
            Some(file) => {
                file.set_len(0)
                    .await
                    .internal("Failed truncating packstore file")?;
                file.sync_all().await
            }
            None => Ok(()),
        };
        packfile.opened = 0;
        *packfile.settled.get_mut() = false;
        *packfile.sync_failed.get_mut() = synced.is_err();
        packfile.dirty.store(false, Ordering::Release);
        let contents = packfile.contents.get_mut();
        contents.buffer.clear();
        contents.size = 0;
        synced.internal("Failed syncing truncated packstore file")?;
        Ok(())
    }

    /// Discard the empty packfiles at the end, beyond the minimum count, whether they take writes
    /// or are stopped, and remove their files, so that emptied packfiles do not accumulate.
    async fn discard_empty_tail(&self, packfile: &mut Vec<PackFile>, writeable: &mut Vec<u32>) {
        while packfile.len() > self.min_count
            && packfile
                .last_mut()
                .is_some_and(|last| last.contents.get_mut().size == 0)
        {
            let last_id = packfile.len() as u32;
            writeable.retain(|writeable_id| *writeable_id != last_id);
            drop(packfile.pop());

            if let Some(path) = self.path.as_ref() {
                let file_path = path.join(last_id.to_string());
                lore_base::lore_debug!(
                    "Discard compacted and truncated packfile {}",
                    file_path.display()
                );
                let _ = IoDriver::global().remove_file(file_path.as_path()).await;
            }
        }
    }

    /// Let those of the packfiles `ids` under the size limit, whose syncs have not failed, take
    /// writes again after [`PackStore::stop_writes`], then discard the empty packfiles at the end,
    /// see [`PackStore::discard_empty_tail`]. A packstore not loaded is left alone.
    pub async fn restart_writes(&self, ids: &[u32]) {
        let mut packfile = self.packfile.write().await;
        let mut writeable = self.writeable.write().await;
        if packfile.is_empty() {
            return;
        }
        for id in ids {
            let Some(restarted) = (*id as usize)
                .checked_sub(1)
                .and_then(|index| packfile.get_mut(index))
            else {
                continue;
            };
            if restarted.contents.get_mut().size >= PACKSTORE_SIZE_LIMIT as usize
                || *restarted.sync_failed.get_mut()
            {
                continue;
            }
            if let Err(position) = writeable.binary_search(id) {
                writeable.insert(position, *id);
            }
        }
        self.discard_empty_tail(&mut packfile, &mut writeable).await;
        let _ = self.fill_writeable(packfile, writeable).await;
    }

    pub async fn load(&self, id: u32, offset: u32, size: u32) -> Result<Bytes, PackfileError> {
        if size == 0 {
            return Ok(Bytes::default());
        }
        if id == 0 {
            return Err(PackfileError::internal("Invalid packfile"));
        }

        let index = (id - 1) as usize;
        let size = size as usize;
        let offset = offset as usize;

        let packfiles = self.loaded_packfiles().await?;

        if index >= packfiles.len() {
            return Err(PackfileError::internal("Invalid packfile"));
        }

        let packfile = &packfiles[index];
        if packfile.id != id {
            return Err(PackfileError::internal("Packfile ID mismatch index"));
        }

        if let Some(file) = packfile.file.as_ref() {
            return packfile_read(file, offset, size).await;
        }

        let contents = packfile.contents.read().await;
        if (offset + size) > contents.buffer.len() {
            return Err(PackfileError::internal(
                "Failed reading from packstore buffer, boundary violation",
            ));
        }

        Ok(Bytes::copy_from_slice(
            &contents.buffer[offset..(offset + size)],
        ))
    }

    /// As [`load`](Self::load), reading into `dst` instead of allocating a buffer.
    pub async fn load_into(
        &self,
        id: u32,
        offset: u32,
        size: u32,
        dst: &mut CallerBuffer,
    ) -> Result<(), PackfileError> {
        if size == 0 {
            return Ok(());
        }
        if id == 0 {
            return Err(PackfileError::internal("Invalid packfile"));
        }
        if size as usize > dst.len() {
            return Err(PackfileError::internal(
                "Destination buffer is smaller than the payload to read",
            ));
        }

        let index = (id - 1) as usize;
        let size = size as usize;
        let offset = offset as usize;

        let packfiles = self.loaded_packfiles().await?;

        if index >= packfiles.len() {
            return Err(PackfileError::internal("Invalid packfile"));
        }

        let packfile = &packfiles[index];
        if packfile.id != id {
            return Err(PackfileError::internal("Packfile ID mismatch index"));
        }

        if let Some(file) = packfile.file.as_ref() {
            return packfile_read_into(file, offset, dst, size).await;
        }

        let contents = packfile.contents.read().await;
        let Some(stored) = offset
            .checked_add(size)
            .and_then(|end| contents.buffer.get(offset..end))
        else {
            return Err(PackfileError::internal(
                "Failed reading from packstore buffer, boundary violation",
            ));
        };
        let Some(target) = dst.as_mut_slice().get_mut(..size) else {
            return Err(PackfileError::internal(
                "Destination buffer is smaller than the payload to read",
            ));
        };

        // An in-memory packstore holds the bytes already, so this copy is unavoidable.
        target.copy_from_slice(stored);
        Ok(())
    }

    /// Append `payload` to a packfile taking writes, and return where it landed.
    ///
    /// The range is reserved under the packfile's lock and written once the lock is released, so
    /// that loads from the packfile and other writes to it go on during the write. The packfile
    /// list stays read-locked until the write lands, even when the future is dropped mid-write,
    /// see `PackfileWrite`, which keeps every stop, truncate and discard of the packfile out of a
    /// range still being written. The packfile is marked dirty only once the write lands, so that
    /// a sync claiming the mark meanwhile leaves it for the sync that covers the write. A packfile
    /// at the size limit takes no reservation, so that no write starts at or past the limit, where
    /// an offset no longer fits an entry's 32 bits; one held in memory as well. A failed write gives
    /// its range back unless another was reserved after it, which leaves a hole no entry points
    /// into.
    pub async fn store(&self, payload: Bytes) -> Result<PackStoreRef, PackfileError> {
        let size = payload.len();
        if size == 0 {
            return Err(PackfileError::internal(
                "Failed to store invalid zero sized buffer",
            ));
        }

        loop {
            let packfiles = self.loaded_packfiles_owned().await?;
            let (index, mut contents) = self
                .packfile_to_write(&packfiles, size + payload.as_ptr() as usize)
                .await?;
            let packfile = &packfiles[index];
            let id = packfile.id;

            let reserved = u32::try_from(contents.size)
                .ok()
                .filter(|offset| u64::from(*offset) < size_limit());
            let Some(offset) = reserved else {
                drop(contents);
                drop(packfiles);
                self.mark_full(id).await;
                continue;
            };
            contents.size += size;
            let full = contents.size as u64 >= size_limit();

            if packfile.file.is_none() {
                if (contents.buffer.len() + size) > contents.buffer.capacity() {
                    contents.buffer.reserve(4 * 1024 * 1024);
                }
                contents.buffer.extend_from_slice(&payload[..size]);
                packfile.dirty.store(true, Ordering::Release);
                drop(contents);
                drop(packfiles);
                if full {
                    self.mark_full(id).await;
                }
                return Ok(PackStoreRef { id, offset });
            }
            drop(contents);

            let write = PackfileWrite::new(payload, packfiles, index);
            #[cfg(feature = "test-util")]
            crate::test_util::pause_point(crate::test_util::PausePoint::Reserved).await;

            if let Err(err) = write.write_at(offset as usize).await {
                let mut contents = write.packfile().contents.write().await;
                if contents.size == offset as usize + size {
                    contents.size = offset as usize;
                }
                return Err(err);
            }
            write.packfile().dirty.store(true, Ordering::Release);
            drop(write);

            if full {
                self.mark_full(id).await;
            }
            return Ok(PackStoreRef { id, offset });
        }
    }

    /// The index of the lowest-numbered writeable packfile whose lock is free, with the lock, so
    /// that writes fill low packfiles first and high packfiles empty and can be discarded. When
    /// every one is locked, the one `spread` picks, so that waiting writes spread over them.
    /// Writeable ids name packfiles in `packfiles`: an id leaves the packfile list only after
    /// leaving the writeable list, under both locks.
    async fn packfile_to_write<'a>(
        &self,
        packfiles: &'a [PackFile],
        spread: usize,
    ) -> Result<(usize, RwLockWriteGuard<'a, Contents>), PackfileError> {
        let writeable = self.writeable.read().await;
        if writeable.is_empty() {
            return Err(PackfileError::internal("No more packfiles"));
        }
        for id in writeable.iter() {
            let index = (*id - 1) as usize;
            if let Ok(contents) = packfiles[index].contents.try_write() {
                return Ok((index, contents));
            }
        }
        let index = (writeable[spread % writeable.len()] - 1) as usize;
        drop(writeable);
        Ok((index, packfiles[index].contents.write().await))
    }

    /// Overwrite `size` bytes at `offset` in packfile `id` with zeros, then sync the packfile. A
    /// failed sync is not reported, and takes the packfile off the writeable list, see
    /// [`PackStore::sync_packfile`], in a call boxed as it runs only after a failure. A packfile
    /// held in memory is left as it is.
    pub async fn obliterate(&self, id: u32, offset: u32, size: u32) -> Result<(), PackfileError> {
        let packfiles = self.loaded_packfiles_owned().await?;

        if id == 0 || id as usize > packfiles.len() {
            return Err(PackfileError::internal("Invalid packfile"));
        }

        let index = (id - 1) as usize;
        let packfile = &packfiles[index];
        if packfile.id != id {
            return Err(PackfileError::internal("Packfile ID mismatch index"));
        }
        if packfile.file.is_none() {
            return Ok(());
        }

        let write = PackfileWrite::new(BytesMut::zeroed(size as usize).freeze(), packfiles, index);
        write.write_at(offset as usize).await?;
        write.packfile().dirty.store(true, Ordering::Release);
        if Self::sync_packfile(write.packfile()).await.is_err() {
            drop(write);
            Box::pin(self.mark_full(id)).await;
        }

        Ok(())
    }

    /// With `sync_data`, sync packfile `id` when it holds writes not yet synced, see
    /// [`PackStore::sync_packfile`]. A failed sync takes the packfile off the writeable list, see
    /// [`PackStore::stop_writes_after_failed_sync`].
    pub async fn flush(&self, id: u32, sync_data: bool) -> Result<(), PackfileError> {
        let packfiles = self.packfile.read().await;

        if id == 0 || id as usize > packfiles.len() {
            return Err(PackfileError::internal("Invalid packfile"));
        }

        let packfile = &packfiles[(id - 1) as usize];
        if packfile.id != id {
            return Err(PackfileError::internal("Packfile ID mismatch index"));
        }
        if !sync_data {
            return Ok(());
        }
        let Err(err) = Self::sync_packfile(packfile).await else {
            return Ok(());
        };
        drop(packfiles);
        Err(Box::pin(self.stop_writes_after_failed_sync([id], err)).await)
    }

    /// With `sync_data`, sync every packfile holding writes not yet synced, see
    /// [`PackStore::sync_packfile`]. Returns the first error. A failed sync takes its packfile off
    /// the writeable list, see [`PackStore::stop_writes_after_failed_sync`].
    pub async fn flush_all(&self, sync_data: bool) -> Result<(), PackfileError> {
        if !sync_data {
            return Ok(());
        }
        #[cfg(feature = "test-util")]
        crate::test_util::pause_point(crate::test_util::PausePoint::Syncing).await;
        let packfiles = self.packfile.read().await;
        let synced = futures::future::join_all(packfiles.iter().map(Self::sync_packfile)).await;
        let failed: Vec<u32> = packfiles
            .iter()
            .zip(&synced)
            .filter(|(_, synced)| synced.is_err())
            .map(|(packfile, _)| packfile.id)
            .collect();
        drop(packfiles);
        let Err(err) = synced.into_iter().collect::<Result<(), _>>() else {
            return Ok(());
        };
        Err(Box::pin(self.stop_writes_after_failed_sync(failed, err)).await)
    }

    /// Take the packfiles `ids`, whose sync failed with `err`, off the writeable list, and return
    /// `err`. Its callers box it, as it runs only after a failure, and pass `ids` and `err` in so
    /// that they hold nothing across it.
    async fn stop_writes_after_failed_sync(
        &self,
        ids: impl IntoIterator<Item = u32>,
        err: PackfileError,
    ) -> PackfileError {
        for id in ids {
            self.mark_full(id).await;
        }
        err
    }

    /// Sync `packfile` when it holds writes not yet synced. A sync of it already running is
    /// waited for, so that a flush finding it clean returns only once the writes before it are
    /// synced. A write sets the mark once it lands, so a claim sees every write that landed before
    /// it, and a write landing after it sets the mark again for the next sync. A failed sync
    /// leaves the packfile dirty and fails every later sync of it, see [`PackFile::sync_failed`],
    /// and the caller takes the packfile off the writeable list.
    async fn sync_packfile(packfile: &PackFile) -> Result<(), PackfileError> {
        let _syncing = packfile.syncing.lock().await;
        if packfile.sync_failed.load(Ordering::Relaxed) {
            return Err(PackfileError::internal(
                "Packstore file failed an earlier sync",
            ));
        }
        if !packfile.dirty.swap(false, Ordering::AcqRel) {
            return Ok(());
        }
        let Some(file) = packfile.file.as_ref() else {
            return Ok(());
        };
        let synced = file.sync_data().await;
        #[cfg(feature = "test-util")]
        let synced = synced.and_then(|()| crate::test_util::packfile_sync_result());
        if synced.is_err() {
            packfile.dirty.store(true, Ordering::Relaxed);
            packfile.sync_failed.store(true, Ordering::Relaxed);
        }
        synced.internal("Failed syncing packstore file")?;
        Ok(())
    }

    pub async fn total_size(&self) -> Result<usize, PackfileError> {
        let mut size = 0;
        let packfiles = self.loaded_packfiles().await?;
        for packfile in packfiles.iter() {
            size += packfile.contents.read().await.size;
        }
        Ok(size)
    }
}

#[cfg(feature = "test-util")]
pub mod test_util {
    use std::sync::atomic::Ordering;

    use super::PackStore;

    impl PackStore {
        /// Whether packfile `id` holds a write that no sync has claimed.
        pub async fn is_dirty(&self, id: u32) -> bool {
            self.packfile.read().await[(id - 1) as usize]
                .dirty
                .load(Ordering::Acquire)
        }

        /// Whether packfile `id` is settled, see `PackFile::settled`.
        pub async fn is_settled(&self, id: u32) -> bool {
            self.packfile.read().await[(id - 1) as usize]
                .settled
                .load(Ordering::Acquire)
        }

        /// The ids of the packfiles that take writes, ascending.
        pub async fn writeable_ids(&self) -> Vec<u32> {
            self.writeable.read().await.clone()
        }

        /// Whether a change to the packfile list, such as marking a packfile full, holds the list
        /// or waits for it.
        pub fn list_change_pending(&self) -> bool {
            self.packfile.try_read().is_err()
        }
    }
}
