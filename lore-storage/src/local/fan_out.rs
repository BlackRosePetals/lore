// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT
//! Fan-out helpers for the lazy progressive bucket layout used by
//! `LocalImmutableStore` and `LocalMutableStore`.

use std::path::Path;
use std::path::PathBuf;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;

use tokio::sync::OwnedMutexGuard;
use zerocopy::FromBytes;
use zerocopy::FromZeros;
use zerocopy::Immutable;
use zerocopy::IntoBytes;

use crate::Hash;

/// The set of valid bucket counts a group can be at. Fan-out only ever moves a
/// group to the next-higher value in this list.
pub const LEVEL_LADDER: [usize; 5] = [1, 32, 64, 128, 256];

/// Maximum bucket count per group. Equals the last entry in `LEVEL_LADDER` and matches the
/// existing `BUCKET_COUNT` in the local store implementations. Server-mode stores start here.
pub const FAN_OUT_LEVEL_MAX: usize = 256;

/// Default per-bucket entry threshold that triggers fan-out at the next serialize. Used by
/// `MutableStoreSettings::default()` and `ImmutableStoreSettings::default()`.
pub const FAN_OUT_THRESHOLD_DEFAULT: usize = 1000;

/// Filename of the per-group level marker, relative to the group directory.
pub const MARKER_FILENAME: &str = "level";

/// Filename of the per-group two-phase commit sentinel, relative to the group directory.
/// Present iff a fan-out commit started but did not yet reach the marker write step. Recovery
/// during store open detects this and rolls forward.
pub const LEVEL_PENDING_FILENAME: &str = "level.pending";

/// Filename prefix of a bucket index file inside a group directory.
pub const BUCKET_FILENAME_PREFIX: &str = "index_";

/// Filename suffix appended to bucket files during a fan-out commit. Bucket file `index_<bb>`
/// gets written to `index_<bb>.new` first; the rename to the final name only happens after
/// every `.new` is on disk and the `level.pending` sentinel has been written.
pub const BUCKET_NEW_SUFFIX: &str = ".new";

/// Magic value at the start of the marker file. Bytes spell `LVNO` on disk.
#[lore_macro::test_pub]
const MARKER_MAGIC: u32 = u32::from_le_bytes(*b"LVNO");

/// Marker file format version. Independent of `MutableStoreVersion` /
/// `ImmutableStoreVersion`; bumped only if the marker file's binary layout changes.
const MARKER_VERSION: u32 = 1;

#[repr(C)]
#[derive(Default, IntoBytes, FromBytes, Immutable)]
struct LevelMarkerHeader {
    magic: u32,
    version: u32,
    bucket_count: u32,
    _reserved: u32,
}

/// Compute the bucket index for `key` at a group whose bucket count is `bucket_count`.
///
/// The same hash byte (`key.data()[1]`) is interpreted at different bit widths depending on
/// the level: level 1 always returns 0; level `N` (a power of two in `{32, 64, 128, 256}`)
/// returns the high `log2(N)` bits of `key.data()[1]`. Going from level `N` to level `M` (with
/// `M > N`, both in the ladder) splits each old bucket into `M / N` new buckets — the new
/// index is the same hash byte right-shifted by a smaller amount, so an entry's new bucket
/// is always within the contiguous range `[old_idx * M/N, old_idx * M/N + M/N - 1]`.
///
/// # Arguments
/// * `key` — the entry's hash. Only `data()[1]` is consulted; `data()[0]` selected the group
///   and is irrelevant here.
/// * `bucket_count` — the group's current `bucket_count`. Must be `1` or a power of two in
///   `[2, 256]`.
///
/// # Returns
/// The bucket index in `[0, bucket_count)`.
///
/// # Invariants
/// * `bucket_count == 1 || (bucket_count.is_power_of_two() && bucket_count <= 256)`. Violation
///   is a `debug_assert!` (release builds skip the check).
/// * Result is always in `[0, bucket_count)`.
pub fn bucket_index_for(key: &Hash, bucket_count: usize) -> usize {
    debug_assert!(
        bucket_count == 1 || (bucket_count.is_power_of_two() && bucket_count <= 256),
        "bucket_count must be 1 or a power of two ≤ 256, got {bucket_count}"
    );
    if bucket_count <= 1 {
        return 0;
    }
    let shift = (256usize / bucket_count).trailing_zeros();
    (key.data()[1] as usize) >> shift
}

/// Compute the target level for a group whose largest bucket has overshot the threshold.
///
/// Returns the smallest value in `LEVEL_LADDER` such that splitting the group's worst bucket
/// `M / current_level` ways would bring its expected post-split entry count to or below
/// `threshold`. Concretely: returns the smallest `M ∈ LEVEL_LADDER` with
/// `M ≥ ceil(current_level * b_max / threshold)`, capped at 256.
///
/// # Arguments
/// * `current_level` — the group's current `bucket_count`. Typically a value from
///   `LEVEL_LADDER`.
/// * `b_max` — the maximum entry count observed across the group's buckets.
/// * `threshold` — the per-bucket fan-out threshold from the store's settings.
///
/// # Returns
/// A value from `LEVEL_LADDER` (always ≤ 256). When `b_max ≤ threshold` the function returns
/// the smallest ladder value ≥ `current_level`, which for typical inputs equals
/// `current_level` itself (no transition needed).
///
/// # Invariants
/// * `threshold > 0`. Violation is a `debug_assert!`.
/// * Multiplication `current_level * b_max` saturates at `usize::MAX` rather than overflowing.
/// * Result is always a member of `LEVEL_LADDER`.
pub fn level_for(current_level: usize, b_max: usize, threshold: usize) -> usize {
    debug_assert!(threshold > 0, "threshold must be positive");
    let product = current_level.saturating_mul(b_max);
    let required = product.div_ceil(threshold);
    LEVEL_LADDER
        .iter()
        .copied()
        .find(|&m| m >= required)
        .unwrap_or(256)
}

/// Read the level marker file in `group_path`, returning the recorded bucket count.
///
/// # Arguments
/// * `group_path` — the per-group directory (e.g., `<store>/index/<gg>/`).
///
/// # Returns
/// * `Ok(None)` when no marker file exists. Such a group is either pre-fan-out or never
///   written; [`read_group_level`] tells the two apart.
/// * `Ok(Some(level))` when the marker is present, has a valid magic and version, and
///   parses to a level value.
/// * `Err(io::Error)` for I/O failures, truncated files, mismatched magic, or unsupported
///   version. The error kind is `InvalidData` for corruption.
///
/// # Invariants
/// * The marker file is always exactly `size_of::<LevelMarkerHeader>()` bytes.
/// * A successfully-parsed marker has `magic == MARKER_MAGIC` and `version == MARKER_VERSION`.
pub async fn read_level_marker(group_path: &Path) -> std::io::Result<Option<usize>> {
    read_level_header_file(&group_path.join(MARKER_FILENAME)).await
}

/// Write the level marker file in `group_path`, recording the current bucket count.
///
/// # Arguments
/// * `group_path` — the per-group directory. Must already exist.
/// * `level` — the bucket count to record. Should be a value from `LEVEL_LADDER`.
/// * `sync_data` — when true, sync the file before it replaces the marker, and its directory
///   after.
///
/// # Returns
/// * `Ok(())` on success.
/// * `Err(io::Error)` for any I/O failure during file creation, write, sync or rename.
///
/// # Invariants
/// * The marker is replaced whole, see [`write_level_header_file`].
/// * `level` is cast to u32; must fit (always true for ladder values ≤ 256).
pub async fn write_level_marker(
    group_path: &Path,
    level: usize,
    sync_data: bool,
) -> std::io::Result<()> {
    write_level_header_file(&group_path.join(MARKER_FILENAME), level, sync_data).await
}

/// Number of bytes a bucket file name occupies: the prefix plus two hex digits.
const BUCKET_FILENAME_LEN: usize = BUCKET_FILENAME_PREFIX.len() + 2;

/// Bytes a marker path adds to a store root: `/index`, `/<gg>` and `/<MARKER_FILENAME>`.
const MARKER_PATH_LEN: usize = "/index".len() + 1 + 2 + 1 + MARKER_FILENAME.len();

/// Write `value` as lowercase two-digit hex into the first two bytes of `out`.
///
/// Path components are built through this rather than `format!`: on the per-bucket flush and
/// recovery paths that is one `String` allocated and dropped per bucket, for two bytes.
#[lore_macro::test_pub]
pub(crate) fn write_hex_byte(out: &mut [u8], value: u8) {
    const HEX: [u8; 16] = *b"0123456789abcdef";
    out[0] = HEX[(value >> 4) as usize];
    out[1] = HEX[(value & 0x0f) as usize];
}

/// Append the group directory component for `group_index`: the lowercase 2-digit hex of
/// `group_index as u8`.
pub fn push_group_dir(path: &mut PathBuf, group_index: usize) {
    let mut name = [0u8; 2];
    write_hex_byte(&mut name, group_index as u8);
    path.push(std::str::from_utf8(&name).unwrap_or_default());
}

/// Format the path for a group directory inside an index directory: `<index_path>/<gg>` where
/// `<gg>` is the lowercase 2-digit hex of `group_index as u8`.
pub fn group_dir_path(index_path: &Path, group_index: usize) -> PathBuf {
    let mut path = index_path.to_path_buf();
    push_group_dir(&mut path, group_index);
    path
}

/// Which of the `group_count` groups have a directory under `index_path`, found with one
/// listing rather than a probe per group: a store open asks this of every group, and a small
/// store has written only a few of them.
///
/// # Returns
/// * One flag per group, all `false` when `index_path` does not exist.
/// * `None` when the listing fails for another reason, or an entry named like a group cannot be
///   resolved to what it is, so the caller can probe each group as it would without this.
pub async fn existing_group_dirs(index_path: &Path, group_count: usize) -> Option<Vec<bool>> {
    let mut existing = vec![false; group_count];
    let mut entries = match lore_io::IoDriver::global().read_dir(index_path).await {
        Ok(entries) => entries,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Some(existing),
        Err(_) => return None,
    };
    while let Some(entry) = entries.next().await {
        let entry = entry.ok()?;
        let Some(group_index) = entry.file_name.to_str().and_then(parse_group_dir_name) else {
            continue;
        };
        // A name that could be a group but whose metadata did not resolve may be a group that
        // is there, so it is not an absent one: the caller probes.
        let metadata = entry.metadata.as_ref()?;
        if !metadata.is_dir() {
            continue;
        }
        if let Some(flag) = existing.get_mut(group_index) {
            *flag = true;
        }
    }
    Some(existing)
}

/// The group a directory name stands for, when it is exactly the name [`push_group_dir`] gives
/// one: two lowercase hex digits.
#[lore_macro::test_pub]
fn parse_group_dir_name(name: &str) -> Option<usize> {
    let &[high, low] = name.as_bytes() else {
        return None;
    };
    let digit = |byte: u8| match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        _ => None,
    };
    Some(usize::from(digit(high)? << 4 | digit(low)?))
}

/// Format the path for a bucket index file inside a group directory: `<group_path>/index_<bb>`
/// where `<bb>` is the lowercase 2-digit hex of `bucket_index as u8`.
///
/// # Invariants
/// * `bucket_index` is cast to `u8`; values ≥ 256 wrap, but `bucket_index` is always in
///   `[0, 256)` per the level ladder.
pub fn bucket_path(group_path: &Path, bucket_index: usize) -> PathBuf {
    let mut name = [0u8; BUCKET_FILENAME_LEN];
    name[..BUCKET_FILENAME_PREFIX.len()].copy_from_slice(BUCKET_FILENAME_PREFIX.as_bytes());
    write_hex_byte(
        &mut name[BUCKET_FILENAME_PREFIX.len()..],
        bucket_index as u8,
    );
    group_path.join(std::str::from_utf8(&name).unwrap_or_default())
}

/// What a group's directory records about the bucket layout it holds.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GroupLevel {
    /// The level marker records this bucket count.
    Marked(usize),
    /// Bucket files but no marker: the pre-fan-out layout, entries spread over all
    /// [`FAN_OUT_LEVEL_MAX`] buckets.
    PreFanOut,
    /// Neither marker nor bucket files, so the group has never been written. The level it
    /// starts at is the caller's to choose.
    Unwritten,
}

/// The bucket count a [`GroupLevel::Unwritten`] group starts at.
///
/// A fan-out-aware store records the level a group starts at in its level marker, so it may
/// start anywhere on the ladder. A store still serializing at a pre-fan-out version writes no
/// marker, so the next open has only the bucket files to go by and reads such a group at
/// [`FAN_OUT_LEVEL_MAX`]; starting it lower would put its entries where that read cannot find
/// them.
pub fn unwritten_group_level(fan_out_aware: bool, initial_fan_out_level: usize) -> usize {
    if fan_out_aware {
        initial_fan_out_level
    } else {
        FAN_OUT_LEVEL_MAX
    }
}

/// Read what `group_path` records about its bucket layout. Any pending fan-out commit must
/// already have been recovered, or a group caught mid-commit reads as [`GroupLevel::PreFanOut`].
///
/// # Returns
/// * `Ok(GroupLevel)` classifying the group.
/// * `Err(io::Error)` for the failures [`read_level_marker`] reports.
pub async fn read_group_level(group_path: &Path) -> std::io::Result<GroupLevel> {
    match read_level_marker(group_path).await? {
        Some(level) => Ok(GroupLevel::Marked(level)),
        None if group_has_buckets(group_path).await => Ok(GroupLevel::PreFanOut),
        None => Ok(GroupLevel::Unwritten),
    }
}

/// Record the level a group's bucket files are laid out at, when the group has no marker yet.
/// `store_path` is the store root; the group directory is `<store_path>/index/<gg>`.
/// `_flush_guard` is the group's held flush lock, which orders the `Relaxed` `committed_level`
/// store against `flush_all`'s reads of it; it is taken to make the caller prove it holds one.
///
/// A bucket file is only interpretable at the level it was written at, and [`read_group_level`]
/// reads a marker-less group holding bucket files as [`GroupLevel::PreFanOut`]. Anything a lower
/// level wrote then sits in a file no lookup opens again. Every path that writes a bucket file
/// outside the two-phase commit therefore owes the group a marker.
///
/// A group that already has a marker is left alone, as committing a level is the two-phase
/// commit's business. So is a group at [`FAN_OUT_LEVEL_MAX`], which already reads back at that
/// level — keeping this inert for pre-fan-out and server stores, whose groups are all there.
pub async fn commit_if_initial_level(
    _flush_guard: &OwnedMutexGuard<()>,
    committed_level: &AtomicUsize,
    bucket_count: &AtomicUsize,
    store_path: &Path,
    group_index: usize,
    sync_data: bool,
) {
    if committed_level.load(Ordering::Relaxed) != 0 {
        return;
    }
    let active_buckets = bucket_count.load(Ordering::Relaxed);
    if active_buckets == FAN_OUT_LEVEL_MAX {
        return;
    }
    let mut marker_path = PathBuf::with_capacity(store_path.as_os_str().len() + MARKER_PATH_LEN);
    marker_path.push(store_path);
    marker_path.push("index");
    push_group_dir(&mut marker_path, group_index);
    marker_path.push(MARKER_FILENAME);
    match write_level_header_file(&marker_path, active_buckets, sync_data).await {
        Ok(()) => committed_level.store(active_buckets, Ordering::Relaxed),
        Err(err) => {
            lore_base::lore_warn!("Failed to write level marker for group {group_index}: {err}");
        }
    }
}

/// Whether `name` names a committed bucket file, `index_<bb>`, rather than a `.new` twin or a
/// temporary file.
pub(crate) fn is_bucket_file_name(name: &str) -> bool {
    name.len() == BUCKET_FILENAME_LEN && name.starts_with(BUCKET_FILENAME_PREFIX)
}

/// Whether `group_path` holds at least one committed bucket file.
///
/// A group is serialized as a whole, so this separates a group that has been written
/// from one that has not. `.new` twins are ignored: they belong to a fan-out commit
/// that never reached its commit point and record no committed layout. Temporary files
/// are ignored too: they belong to a write that never reached its rename.
///
/// # Returns
/// * `true` when the directory holds a file named `index_<bb>`.
/// * `false` when it holds none, or cannot be read at all.
#[lore_macro::test_pub]
pub(crate) async fn group_has_buckets(group_path: &Path) -> bool {
    let Ok(mut entries) = lore_io::IoDriver::global().read_dir(group_path).await else {
        return false;
    };
    while let Some(entry) = entries.next().await {
        let Ok(entry) = entry else {
            continue;
        };
        let Some(name) = entry.file_name.to_str() else {
            continue;
        };
        if is_bucket_file_name(name) {
            return true;
        }
    }
    false
}

/// Format the path for the in-progress (`.new`) twin of a bucket index file used during
/// fan-out commits.
pub fn bucket_new_path(group_path: &Path, bucket_index: usize) -> PathBuf {
    let mut name = [0u8; BUCKET_FILENAME_LEN + BUCKET_NEW_SUFFIX.len()];
    name[..BUCKET_FILENAME_PREFIX.len()].copy_from_slice(BUCKET_FILENAME_PREFIX.as_bytes());
    write_hex_byte(
        &mut name[BUCKET_FILENAME_PREFIX.len()..],
        bucket_index as u8,
    );
    name[BUCKET_FILENAME_LEN..].copy_from_slice(BUCKET_NEW_SUFFIX.as_bytes());
    group_path.join(std::str::from_utf8(&name).unwrap_or_default())
}

/// Read the `level.pending` sentinel in `group_path`, returning the recorded target bucket count.
///
/// The pending file shares the binary layout of the level marker (same 16-byte header) so the
/// existing read path can be reused.
///
/// # Returns
/// * `Ok(None)` when no pending file exists.
/// * `Ok(Some(level))` when present and well-formed.
/// * `Err(io::Error)` for I/O failures, truncation, mismatched magic, or unsupported version.
pub async fn read_level_pending(group_path: &Path) -> std::io::Result<Option<usize>> {
    read_level_header_file(&group_path.join(LEVEL_PENDING_FILENAME)).await
}

/// Write the `level.pending` sentinel in `group_path` with the recorded target bucket count.
/// Same binary layout as the level marker.
pub async fn write_level_pending(
    group_path: &Path,
    level: usize,
    sync_data: bool,
) -> std::io::Result<()> {
    write_level_header_file(&group_path.join(LEVEL_PENDING_FILENAME), level, sync_data).await
}

/// Delete the `level.pending` sentinel. Returns `Ok(())` whether or not it existed.
pub async fn delete_level_pending(group_path: &Path) -> std::io::Result<()> {
    match lore_io::IoDriver::global()
        .remove_file(group_path.join(LEVEL_PENDING_FILENAME))
        .await
    {
        Ok(()) => Ok(()),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(err) => Err(err),
    }
}

/// Read a level-header file (marker or pending) from an explicit path. Shared format helper;
/// one backend dispatch covering open, read and close, the file being the header itself.
async fn read_level_header_file(path: &Path) -> std::io::Result<Option<usize>> {
    let bytes = match lore_io::IoDriver::global().read_file_bytes(path).await {
        Ok(bytes) => bytes,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(err) => return Err(err),
    };
    let mut header = LevelMarkerHeader::new_zeroed();
    let expected = size_of::<LevelMarkerHeader>();
    if bytes.len() < expected {
        return Err(std::io::Error::new(
            std::io::ErrorKind::UnexpectedEof,
            format!(
                "level header file is {} bytes, expected {expected}",
                bytes.len()
            ),
        ));
    }
    header.as_mut_bytes().copy_from_slice(&bytes[..expected]);
    if header.magic != MARKER_MAGIC {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!(
                "level header file has invalid magic 0x{:08x}, expected 0x{:08x}",
                header.magic, MARKER_MAGIC
            ),
        ));
    }
    if header.version != MARKER_VERSION {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!(
                "level header file has unsupported version {}, expected {}",
                header.version, MARKER_VERSION
            ),
        ));
    }
    Ok(Some(header.bucket_count as usize))
}

/// The serialized level header as the one segment a gather writes.
///
/// Fixed size — the header's length is a compile-time constant — and behind a pointer, because
/// [`lore_io::StableBufList`] requires a segment to keep its address when the value moves and the
/// ring backend moves the segment list into its operation entry after taking the pointers.
struct LevelHeaderSegment(Box<[u8; size_of::<LevelMarkerHeader>()]>);

impl lore_io::StableBufList for LevelHeaderSegment {
    fn byte_segments(&self) -> impl Iterator<Item = &[u8]> {
        std::iter::once(self.0.as_ref().as_slice())
    }
}

/// Write a level-header file (marker or pending) to a temporary file renamed over `path`, so that
/// a process stopped during the write leaves the file it last wrote, or none. With `sync_data`,
/// the file is synced before the rename and its directory after.
async fn write_level_header_file(
    path: &Path,
    level: usize,
    sync_data: bool,
) -> std::io::Result<()> {
    let header = LevelMarkerHeader {
        magic: MARKER_MAGIC,
        version: MARKER_VERSION,
        bucket_count: level as u32,
        _reserved: 0,
    };
    let mut segment = Box::new([0u8; size_of::<LevelMarkerHeader>()]);
    segment.copy_from_slice(header.as_bytes());
    lore_io::IoDriver::global()
        .write_file_segments_atomic(
            temporary_path(path),
            path,
            &lore_io::OpenOptions::new()
                .write(true)
                .create(true)
                .truncate(true),
            LevelHeaderSegment(segment),
            sync_data,
        )
        .await
}

/// The file a write of `path` goes to before it is renamed over `path`: `path` with `.tmp`
/// appended, so that the temporary file of `index_<bb>.new` is `index_<bb>.new.tmp` and does not
/// collide with that of `index_<bb>`.
pub(crate) fn temporary_path(path: &Path) -> PathBuf {
    let mut temporary = std::ffi::OsString::with_capacity(path.as_os_str().len() + 4);
    temporary.push(path.as_os_str());
    temporary.push(".tmp");
    PathBuf::from(temporary)
}

/// Mark `buckets` dirty again after a fan-out commit that failed once their `.new` files were
/// written, so that the next flush writes them again rather than leaving their entries only in
/// those files.
pub(crate) fn mark_uncommitted_dirty(dirty: &[AtomicBool], buckets: &[usize]) {
    for &bucket in buckets {
        dirty[bucket].store(true, Ordering::Relaxed);
    }
}

/// Recover from a possibly-interrupted fan-out commit in `group_path`.
///
/// If `level.pending` exists, this function rolls forward by:
/// 1. Renaming `index_<bb>.new` → `index_<bb>` for each `bb in 0..target` whose `.new` file
///    is still present (already-renamed buckets are silently skipped).
/// 2. Writing the level marker for `target`.
/// 3. Deleting `level.pending`.
///
/// All steps are idempotent, so a recovery interrupted mid-way and re-run on the next open
/// converges to the same state.
///
/// # Returns
/// * `Ok(None)` if no recovery was needed (no `level.pending` present).
/// * `Ok(Some(level))` if recovery rolled forward to `level`.
/// * `Err(io::Error)` if recovery encountered an I/O error, such as a `.new` rename that failed.
///   `level.pending` is then kept, so that the next open retries.
pub async fn recover_level_transition(
    group_path: &Path,
    sync_data: bool,
) -> std::io::Result<Option<usize>> {
    let target = match read_level_pending(group_path).await? {
        Some(level) => level,
        None => return Ok(None),
    };

    for bb in 0..target {
        let new_path = bucket_new_path(group_path, bb);
        let final_path = bucket_path(group_path, bb);
        match lore_io::IoDriver::global()
            .rename(&new_path, &final_path)
            .await
        {
            Ok(()) => {}
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
                // Either already renamed in a previous (interrupted) recovery attempt, or
                // never existed (an empty bucket at an index ≥ N skipped writing `.new`).
            }
            Err(err) => {
                lore_base::lore_warn!(
                    "Recovery rename {} -> {} failed: {err}",
                    new_path.display(),
                    final_path.display()
                );
                return Err(err);
            }
        }
    }

    write_level_marker(group_path, target, sync_data).await?;
    delete_level_pending(group_path).await?;
    Ok(Some(target))
}
