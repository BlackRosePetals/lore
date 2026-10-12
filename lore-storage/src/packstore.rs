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
use tokio::sync::RwLock;
use tokio::sync::RwLockReadGuard;
use tokio::sync::RwLockWriteGuard;

#[error_set]
pub enum PackfileError {}

#[derive(Debug)]
pub struct PackStoreRef {
    pub id: u32,
    pub offset: u32,
}

struct PackFile {
    id: u32,
    size: usize,
    file: Option<IoFile>,
    buffer: Vec<u8>,
    dirty: AtomicBool,
    syncing: Arc<Mutex<()>>,
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

async fn packfile_write(file: &IoFile, buffer: Bytes, offset: usize) -> Result<(), PackfileError> {
    crate::fs_util::retry_transient(|| file.write_all_at(buffer.clone(), offset as u64))
        .await
        .internal("Failed writing to packstore file")?;
    Ok(())
}

/// Maximum size of a single packfile
const PACKSTORE_SIZE_LIMIT: u64 = 3 * 1024 * 1024 * 1024;

pub struct PackStore {
    path: Option<PathBuf>,
    min_count: usize,
    packfile: RwLock<Vec<RwLock<PackFile>>>,
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
            packfile: RwLock::default(),
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
    async fn load_packfiles(
        path: &Path,
    ) -> Result<(Vec<RwLock<PackFile>>, Vec<u32>, u64), PackfileError> {
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
            let file = Self::open_packfile(path, file_id).await?;
            lore_base::lore_trace!("Resuming packfile {file_id}");
            loaded_size += file.size as u64;
            if (file.size as u64) < PACKSTORE_SIZE_LIMIT {
                writeable.push(file_id);
            }
            packfiles.push(RwLock::new(file));
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
        Ok(PackFile {
            id,
            size: size as usize,
            file: Some(file),
            buffer: vec![],
            dirty: AtomicBool::new(false),
            syncing: Arc::default(),
        })
    }

    /// Take packfile `id` off the writeable list, which more than one store finding it full may
    /// do, and open packfiles to keep the minimum count taking writes.
    async fn mark_full(&self, id: u32) {
        let packfile = self.packfile.write().await;
        let mut writeable = self.writeable.write().await;
        writeable.retain(|writeable_id| *writeable_id != id);

        let _ = self.fill_writeable(packfile, writeable).await;
    }

    /// Open packfiles after the last until the minimum count take writes. A file already at the
    /// path of a new packfile is kept and appended to, see [`PackStore::open_packfile`].
    async fn fill_writeable<'a>(
        &'a self,
        mut packfile: RwLockWriteGuard<'a, Vec<RwLock<PackFile>>>,
        mut writeable: RwLockWriteGuard<'a, Vec<u32>>,
    ) -> Result<(), PackfileError> {
        while writeable.len() < self.min_count {
            let id = (packfile.len() + 1) as u32;
            lore_base::lore_trace!("Create additional packfile {id}");
            let file = match self.path.as_ref() {
                Some(path) => Self::open_packfile(path, id).await?,
                None => PackFile {
                    id,
                    size: 0,
                    file: None,
                    buffer: vec![],
                    dirty: AtomicBool::new(false),
                    syncing: Arc::default(),
                },
            };
            let takes_writes = (file.size as u64) < PACKSTORE_SIZE_LIMIT;
            packfile.push(RwLock::new(file));
            if takes_writes {
                writeable.push(id);
            }
        }

        Ok(())
    }

    /// The packfile list, loaded from disk first if it is not yet.
    async fn loaded_packfiles(
        &self,
    ) -> Result<RwLockReadGuard<'_, Vec<RwLock<PackFile>>>, PackfileError> {
        let packfiles = self.packfile.read().await;
        if !packfiles.is_empty() {
            return Ok(packfiles);
        }
        drop(packfiles);
        self.resume().await?;
        Ok(self.packfile.read().await)
    }

    /// Size in bytes of every packfile, the packfile with id `n` at index `n - 1`.
    pub async fn sizes(&self) -> Result<Vec<usize>, PackfileError> {
        if self.path.is_none() {
            return Err(PackfileError::internal(
                "Packfile sizes need a packstore on disk",
            ));
        }

        let packfiles = self.loaded_packfiles().await?;
        let mut sizes = Vec::with_capacity(packfiles.len());
        for packfile in packfiles.iter() {
            sizes.push(packfile.read().await.size);
        }
        Ok(sizes)
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
            sizes.push(stopped.get_mut().size);
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

        Self::empty_packfile(packfile[index].get_mut()).await?;
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

    /// Truncate `packfile`'s file to zero bytes and sync it, then record the packfile empty. A
    /// failed truncate leaves the packfile as it was. A failed sync after it records the packfile
    /// empty, as its file now is, and is reported.
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
        packfile.dirty.store(false, Ordering::Release);
        packfile.buffer.clear();
        packfile.size = 0;
        synced.internal("Failed syncing truncated packstore file")?;
        Ok(())
    }

    /// Discard the empty packfiles at the end, beyond the minimum count, whether they take writes
    /// or are stopped, and remove their files, so that emptied packfiles do not accumulate.
    async fn discard_empty_tail(
        &self,
        packfile: &mut Vec<RwLock<PackFile>>,
        writeable: &mut Vec<u32>,
    ) {
        while packfile.len() > self.min_count
            && packfile
                .last_mut()
                .is_some_and(|last| last.get_mut().size == 0)
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

    /// Let those of the packfiles `ids` under the size limit take writes again after
    /// [`PackStore::stop_writes`], then discard the empty packfiles at the end, see
    /// [`PackStore::discard_empty_tail`]. A packstore not loaded is left alone.
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
            if restarted.get_mut().size >= PACKSTORE_SIZE_LIMIT as usize {
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

        let packfile = packfiles[index].read().await;
        if packfile.id != id {
            return Err(PackfileError::internal("Packfile ID mismatch index"));
        }

        // We got lock on the right packfile, load data
        if let Some(file) = packfile.file.as_ref() {
            return packfile_read(file, offset, size).await;
        }

        if (offset + size) > packfile.buffer.len() {
            return Err(PackfileError::internal(
                "Failed reading from packstore buffer, boundary violation",
            ));
        }

        Ok(Bytes::copy_from_slice(
            &packfile.buffer[offset..(offset + size)],
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

        let packfile = packfiles[index].read().await;
        if packfile.id != id {
            return Err(PackfileError::internal("Packfile ID mismatch index"));
        }

        if let Some(file) = packfile.file.as_ref() {
            return packfile_read_into(file, offset, dst, size).await;
        }

        let Some(stored) = offset
            .checked_add(size)
            .and_then(|end| packfile.buffer.get(offset..end))
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

    pub async fn store(&self, buffer: Bytes) -> Result<PackStoreRef, PackfileError> {
        let size = buffer.len();
        if size == 0 {
            return Err(PackfileError::internal(
                "Failed to store invalid zero sized buffer",
            ));
        }

        let packfiles = self.loaded_packfiles().await?;

        let mut packfile = self
            .packfile_to_write(&packfiles, size + buffer.as_ptr() as usize)
            .await?;

        let offset = packfile.size;
        let id = packfile.id;
        let mut full = false;

        packfile.dirty.store(true, Ordering::Relaxed);

        if let Some(file) = packfile.file.as_ref() {
            packfile_write(file, buffer, offset).await?;
            packfile.size += size;
            if packfile.size >= PACKSTORE_SIZE_LIMIT as usize {
                full = true;
            }
        } else {
            if (packfile.buffer.len() + size) > packfile.buffer.capacity() {
                packfile.buffer.reserve(4 * 1024 * 1024);
            }

            packfile.buffer.extend_from_slice(&buffer[..size]);
            packfile.size += size;
        }

        drop(packfile);
        drop(packfiles);

        if full {
            self.mark_full(id).await;
        }

        Ok(PackStoreRef {
            id,
            offset: offset as u32,
        })
    }

    /// The lowest-numbered writeable packfile whose lock is free, so that writes fill low
    /// packfiles first and high packfiles empty and can be discarded. When every one is locked,
    /// the one `spread` picks, so that waiting writes spread over them. Writeable ids name
    /// packfiles in `packfiles`: an id leaves the packfile list only after leaving the writeable
    /// list, under both locks.
    async fn packfile_to_write<'a>(
        &self,
        packfiles: &'a [RwLock<PackFile>],
        spread: usize,
    ) -> Result<RwLockWriteGuard<'a, PackFile>, PackfileError> {
        let writeable = self.writeable.read().await;
        if writeable.is_empty() {
            return Err(PackfileError::internal("No more packfiles"));
        }
        for id in writeable.iter() {
            if let Ok(guard) = packfiles[(*id - 1) as usize].try_write() {
                return Ok(guard);
            }
        }
        let waited_index = (writeable[spread % writeable.len()] - 1) as usize;
        drop(writeable);
        Ok(packfiles[waited_index].write().await)
    }

    pub async fn obliterate(&self, id: u32, offset: u32, size: u32) -> Result<(), PackfileError> {
        let offset = offset as usize;
        let size = size as usize;
        let zeros = BytesMut::zeroed(size).freeze();

        let packfiles = self.loaded_packfiles().await?;

        if id == 0 || id as usize > packfiles.len() {
            return Err(PackfileError::internal("Invalid packfile"));
        }

        let index = (id - 1) as usize;
        let packfile = packfiles[index].write().await;
        if packfile.id != id {
            return Err(PackfileError::internal("Packfile ID mismatch index"));
        }

        packfile.dirty.store(true, Ordering::Relaxed);

        if let Some(file) = packfile.file.as_ref() {
            packfile_write(file, zeros, offset).await?;
            let _ = file.sync_data().await;
        }

        Ok(())
    }

    /// With `sync_data`, sync packfile `id` when it holds writes not yet synced, see
    /// [`PackStore::sync_packfile`].
    pub async fn flush(&self, id: u32, sync_data: bool) -> Result<(), PackfileError> {
        let packfiles = self.packfile.read().await;

        if id == 0 || id as usize > packfiles.len() {
            return Err(PackfileError::internal("Invalid packfile"));
        }

        let packfile = &packfiles[(id - 1) as usize];
        if packfile.read().await.id != id {
            return Err(PackfileError::internal("Packfile ID mismatch index"));
        }
        if !sync_data {
            return Ok(());
        }
        Self::sync_packfile(packfile).await
    }

    /// With `sync_data`, sync every packfile holding writes not yet synced, see
    /// [`PackStore::sync_packfile`]. Returns the first error.
    pub async fn flush_all(&self, sync_data: bool) -> Result<(), PackfileError> {
        if !sync_data {
            return Ok(());
        }
        let packfiles = self.packfile.read().await;
        futures::future::join_all(packfiles.iter().map(Self::sync_packfile))
            .await
            .into_iter()
            .collect()
    }

    /// Sync `packfile` when it holds writes not yet synced. A sync of it already running is
    /// waited for, so that a flush finding it clean returns only once the writes before it are
    /// synced. A failed sync leaves it dirty. The packfile lock is held only to claim the dirty
    /// mark, which a store sets under the write lock, so that stores go on during the sync.
    async fn sync_packfile(packfile: &RwLock<PackFile>) -> Result<(), PackfileError> {
        let syncing = packfile.read().await.syncing.clone();
        let _syncing = syncing.lock().await;
        let file = {
            let packfile = packfile.read().await;
            if !packfile.dirty.swap(false, Ordering::Relaxed) {
                return Ok(());
            }
            packfile.file.clone()
        };
        let Some(file) = file else {
            return Ok(());
        };
        let synced = file.sync_data().await;
        if synced.is_err() {
            packfile.read().await.dirty.store(true, Ordering::Relaxed);
        }
        synced.internal("Failed syncing packstore file")?;
        Ok(())
    }

    pub async fn total_size(&self) -> Result<usize, PackfileError> {
        let mut size = 0;
        let packfiles = self.loaded_packfiles().await?;
        for packfile in packfiles.iter() {
            size += packfile.read().await.size;
        }
        Ok(size)
    }
}
