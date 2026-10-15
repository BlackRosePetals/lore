// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT
use std::collections::HashMap;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic;
use std::sync::atomic::AtomicUsize;
use std::time::Duration;

use bytes::Bytes;
use lore_storage::Address;
use lore_storage::Context;
use lore_storage::Fragment;
use lore_storage::FragmentFlags;
use lore_storage::FragmentReference;
use lore_storage::Hash;
use lore_storage::PackfileSize;
use lore_storage::Partition;
use lore_storage::hash;
use lore_storage::immutable_store::CopyBehavior;
use lore_storage::immutable_store::StoreError;
use lore_storage::local::immutable_store::info::info_path_for_store_root;
use lore_storage::store_types::StoreMatch;
use lore_storage::store_types::StoreMatchResult;
use lore_storage::test_util::PausePoint;
use lore_storage::test_util::pause_at;
use zerocopy::FromBytes;
use zerocopy::FromZeros;
use zerocopy::IntoBytes;

mod info;
#[cfg(feature = "oodle")]
mod oodle_migration;
mod payload_gate;

use lore_storage::local::immutable_store::*;

use crate::store_fixture::assert_payloads_read_back;
use crate::store_fixture::compact_group_toward;
use crate::store_fixture::group_entries;
use crate::store_fixture::payloads_in_one_group;
use crate::store_fixture::payloads_of_size_in_one_group;
use crate::store_fixture::put_payload;
use crate::store_fixture::try_compact_group_toward;
use crate::store_fixture::try_put_payload;
use crate::store_fixture::wait_until;

fn write_bucket_file(path: &Path, version: u32) {
    let entry = ImmutableStoreEntry::default();
    let mut header = ImmutableStoreHeader::new_zeroed();
    header.version = version;
    header.count = 1;
    let mut bytes = Vec::with_capacity(
        size_of::<ImmutableStoreHeader>() + 4 + size_of::<ImmutableStoreEntry>(),
    );
    bytes.extend_from_slice(header.as_bytes());
    bytes.extend_from_slice(&0u32.to_le_bytes());
    bytes.extend_from_slice(entry.as_bytes());
    std::fs::write(path, bytes).unwrap();
}

/// A bucket larger than [`BUCKET_HEAD_READ`] does not fit the head the composite open
/// returns, so it is loaded by scattering one vectored read straight into the chunk
/// allocations of both `GrowVec`s. Entry contents are distinct per index, so a scatter that
/// landed a chunk at the wrong offset would show up as swapped entries rather than as a
/// length mismatch.
#[tokio::test]
async fn deserialize_scatters_a_bucket_larger_than_the_head_read() {
    let dir = lore_base::test_util::TempDir::new("is_scatter_");
    let path = dir.path().join("bucket");

    let per_entry = size_of::<u32>() + size_of::<ImmutableStoreEntry>();
    let count = (BUCKET_HEAD_READ / per_entry) + 64;
    assert!(
        size_of::<ImmutableStoreHeader>() + count * per_entry > BUCKET_HEAD_READ,
        "the bucket has to exceed the head read for this to test anything"
    );

    let mut header = ImmutableStoreHeader::new_zeroed();
    header.version = ImmutableStoreVersion::LazyFanOut as u32;
    header.count = count as u32;

    let mut bytes = Vec::with_capacity(size_of::<ImmutableStoreHeader>() + count * per_entry);
    bytes.extend_from_slice(header.as_bytes());
    for index in 0..count {
        bytes.extend_from_slice(&(index as u32).to_le_bytes());
    }
    for index in 0..count {
        let mut entry = ImmutableStoreEntry::default();
        entry.address.hash = Hash::from([index as u8; 32]);
        entry.data.size_content = index as u64;
        entry.data.pack_offset = index as u32;
        bytes.extend_from_slice(entry.as_bytes());
    }
    std::fs::write(&path, &bytes).unwrap();

    let file = ImmutableStoreBucket::deserialize_files(path).await.unwrap();
    let (sorted_index, entry) = (file.sorted_index, file.entry);

    assert_eq!(sorted_index.len(), count);
    assert_eq!(entry.len(), count);
    for index in 0..count {
        assert_eq!(sorted_index[index], index as u32, "sorted index at {index}");
        assert_eq!(
            entry[index].address.hash,
            Hash::from([index as u8; 32]),
            "entry hash at {index}"
        );
        assert_eq!(entry[index].data.size_content, index as u64);
        assert_eq!(entry[index].data.pack_offset, index as u32);
    }
}

/// Client-shaped settings: the ones a repository store is opened with.
fn client_settings() -> ImmutableStoreSettings {
    ImmutableStoreSettings {
        protect_local_fragment: true,
        implicit_durable_stored: false,
        ..Default::default()
    }
}

/// A client store over `dir` flushing after no delay, in the background as `flush_background`
/// says.
async fn open_flushing(dir: &Path, flush_background: bool) -> Arc<LocalImmutableStore> {
    LocalImmutableStore::new(
        Some(dir.to_path_buf()),
        ImmutableStoreSettings {
            flush_background,
            flush_delay_seconds: 0,
            ..client_settings()
        },
    )
    .await
    .expect("store opens")
}

/// Whether a delayed flush was pending for `address`'s group, having awaited it.
async fn awaited_flush(store: &LocalImmutableStore, address: Address) -> bool {
    store.group[address.hash.data()[0] as usize]
        .flush
        .lock()
        .await
        .join_next()
        .await
        .is_some()
}

/// A fragment of `payload` stored with `flags`.
fn stored_fragment(payload: &Bytes, flags: FragmentFlags) -> Fragment {
    Fragment {
        flags: flags.as_u32(),
        size_payload: payload.len() as u32,
        size_content: payload.len() as u64,
    }
}

/// A durable write schedules the delayed flush only with `flush_background`, so only then does
/// its entry reach the on-disk index without an explicit flush.
#[tokio::test]
async fn a_durable_write_reaches_the_index_only_with_background_flush() {
    let partition = Partition::from([9u8; 16]);
    let (address, payload) = payload_off_bucket_zero(0);
    let fragment = stored_fragment(&payload, FragmentFlags::PayloadStoredDurable);
    for flush_background in [false, true] {
        let dir = lore_base::test_util::TempDir::new("is_durable_flush_");
        {
            let store = open_flushing(dir.path(), flush_background).await;
            let dyn_store: Arc<dyn lore_storage::immutable_store::ImmutableStore> = store.clone();
            dyn_store
                .put(partition, address, fragment, Some(payload.clone()), false)
                .await
                .expect("put succeeds");
            assert_eq!(awaited_flush(&store, address).await, flush_background);
        }
        let store: Arc<dyn lore_storage::immutable_store::ImmutableStore> =
            open_flushing(dir.path(), flush_background).await;
        assert_eq!(
            store.get(partition, address).await.is_ok(),
            flush_background,
            "flush_background {flush_background}"
        );
    }
}

/// Marking a flushed local entry durable, without a payload to store, reaches the on-disk index
/// in the background only with `flush_background`.
#[tokio::test]
async fn a_durable_mark_on_a_local_entry_reaches_the_index_only_with_background_flush() {
    let partition = Partition::from([9u8; 16]);
    let (address, payload) = payload_off_bucket_zero(0);
    for flush_background in [false, true] {
        let dir = lore_base::test_util::TempDir::new("is_durable_mark_");
        {
            let store = open_flushing(dir.path(), flush_background).await;
            let dyn_store: Arc<dyn lore_storage::immutable_store::ImmutableStore> = store.clone();
            dyn_store
                .clone()
                .put(
                    partition,
                    address,
                    stored_fragment(&payload, FragmentFlags::PayloadStoredLocal),
                    Some(payload.clone()),
                    false,
                )
                .await
                .expect("local put succeeds");
            assert!(awaited_flush(&store, address).await);
            dyn_store
                .put(
                    partition,
                    address,
                    stored_fragment(&payload, FragmentFlags::PayloadStoredDurable),
                    None,
                    false,
                )
                .await
                .expect("durable mark succeeds");
            assert_eq!(awaited_flush(&store, address).await, flush_background);
        }
        let found = open_flushing(dir.path(), flush_background)
            .await
            .find(partition, address)
            .await
            .expect("the local entry was flushed");
        assert_eq!(
            found.data.flags & FragmentFlags::PayloadStoredDurable
                == FragmentFlags::PayloadStoredDurable,
            flush_background,
            "flush_background {flush_background}"
        );
    }
}

/// A durable copy into another partition reaches the on-disk index in the background only with
/// `flush_background`.
#[tokio::test]
async fn a_durable_copy_reaches_the_index_only_with_background_flush() {
    let source = Partition::from([9u8; 16]);
    let destination = Partition::from([10u8; 16]);
    let (address, payload) = payload_off_bucket_zero(0);
    for flush_background in [false, true] {
        let dir = lore_base::test_util::TempDir::new("is_durable_copy_");
        {
            let store = open_flushing(dir.path(), flush_background).await;
            let dyn_store: Arc<dyn lore_storage::immutable_store::ImmutableStore> = store.clone();
            dyn_store
                .clone()
                .put(
                    source,
                    address,
                    stored_fragment(&payload, FragmentFlags::PayloadStoredLocal),
                    Some(payload.clone()),
                    false,
                )
                .await
                .expect("local put succeeds");
            assert!(awaited_flush(&store, address).await);
            dyn_store
                .copy(
                    source,
                    address,
                    destination,
                    address.context,
                    CopyBehavior {
                        durable: true,
                        do_not_replicate: false,
                    },
                )
                .await
                .expect("copy succeeds");
            assert_eq!(awaited_flush(&store, address).await, flush_background);
        }
        let found = open_flushing(dir.path(), flush_background)
            .await
            .find(destination, address)
            .await
            .expect("the source entry was flushed");
        assert_eq!(
            found.partition == destination,
            flush_background,
            "flush_background {flush_background}"
        );
    }
}

/// Server-shaped settings: groups start at the full 256 buckets.
fn server_settings() -> ImmutableStoreSettings {
    ImmutableStoreSettings {
        protect_local_fragment: false,
        implicit_durable_stored: true,
        initial_fan_out_level: BUCKET_COUNT,
        ..Default::default()
    }
}

/// Store `count` fragments and return their addresses.
async fn put_fragments(
    store: &Arc<LocalImmutableStore>,
    partition: Partition,
    count: u16,
) -> Vec<Address> {
    let dyn_store: Arc<dyn lore_storage::immutable_store::ImmutableStore> = store.clone();
    let mut addresses = Vec::new();
    for seed in 0..count {
        let (address, payload) = payload_off_bucket_zero(seed);
        let fragment = Fragment {
            flags: 0,
            size_payload: payload.len() as u32,
            size_content: payload.len() as u64,
        };
        dyn_store
            .clone()
            .put(partition, address, fragment, Some(payload), false)
            .await
            .expect("put succeeds");
        addresses.push(address);
    }
    addresses
}

/// Run the background timer's flush for every group, and nothing else.
async fn run_delayed_flush(store: &Arc<LocalImmutableStore>) {
    let weak = Arc::downgrade(store);
    for group_index in 0..GROUP_COUNT {
        ImmutableStoreGroup::flush_delayed(weak.clone(), group_index, 0).await;
    }
}

/// Every `level` marker under a store's index directory.
fn level_markers(index_root: &Path) -> Vec<PathBuf> {
    let mut found = Vec::new();
    let Ok(groups) = std::fs::read_dir(index_root) else {
        return found;
    };
    for group in groups.flatten() {
        let marker = group
            .path()
            .join(lore_storage::local::fan_out::MARKER_FILENAME);
        if marker.exists() {
            found.push(marker);
        }
    }
    found
}

/// Content whose hash puts it in a bucket other than 0 once a group is at level 256, so a
/// group misread as pre-fan-out sends a lookup to a different bucket than the one holding it.
fn payload_off_bucket_zero(seed: u16) -> (Address, Bytes) {
    for salt in 0..1024u16 {
        let payload = Bytes::from(seed.to_le_bytes().repeat(32 + salt as usize));
        let hash = hash::hash_slice(payload.as_ref());
        if hash.data()[1] != 0 {
            return (
                Address {
                    hash,
                    context: Context::from([0u8; 16]),
                },
                payload,
            );
        }
    }
    panic!("no payload hashed away from bucket 0");
}

/// The delayed background flush writes bucket files without going through the two-phase
/// commit, and clears the dirty flags as it goes — so the flush that ends the command finds
/// the group clean and skips it. Left like that, the group has committed bucket files and no
/// level marker, which [`crate::local::fan_out::read_group_level`] reads back as a legacy
/// pre-fan-out layout at 256 buckets. A client group is written at level 1, where everything
/// lands in `index_00`; at 256 the hash maps somewhere else entirely and that file is never
/// opened again, so the entry is on disk and unreachable — `Address not found` with the
/// payload still sitting in the packstore.
#[tokio::test]
async fn a_group_the_delayed_flush_persisted_reopens_at_its_written_level() {
    let dir = lore_base::test_util::TempDir::new("is_delayed_level_");
    let partition = Partition::from([7u8; 16]);

    let addresses = {
        let store = LocalImmutableStore::new(Some(dir.path().to_path_buf()), client_settings())
            .await
            .expect("store opens");
        assert_eq!(
            store.group[0].bucket_count.load(atomic::Ordering::Relaxed),
            1,
            "a client store starts its groups at level 1"
        );

        let addresses = put_fragments(&store, partition, 32).await;
        // Persist exactly the way the background timer does, and nothing else: no
        // `flush`, so the two-phase commit that would write the markers never runs.
        run_delayed_flush(&store).await;
        addresses
    };

    let index_root = dir.path().join("immutable").join("index");
    let mut checked = 0;
    for group in std::fs::read_dir(&index_root)
        .expect("index dir exists")
        .flatten()
    {
        let has_bucket = std::fs::read_dir(group.path())
            .expect("group dir")
            .flatten()
            .any(|entry| {
                entry
                    .file_name()
                    .to_str()
                    .is_some_and(|name| name.starts_with("index_"))
            });
        if !has_bucket {
            continue;
        }
        checked += 1;
        assert_eq!(
            lore_storage::local::fan_out::read_level_marker(&group.path())
                .await
                .expect("marker readable"),
            Some(1),
            "group {} must record the level its bucket files were written at",
            group.path().display()
        );
    }
    assert!(
        checked > 0,
        "the delayed flush has to have written something"
    );

    let store = LocalImmutableStore::new(Some(dir.path().to_path_buf()), client_settings())
        .await
        .expect("store reopens");
    let dyn_store: Arc<dyn lore_storage::immutable_store::ImmutableStore> = store.clone();
    for address in &addresses {
        let group = &store.group[address.hash.data()[0] as usize];
        assert_eq!(
            group.bucket_count.load(atomic::Ordering::Relaxed),
            1,
            "group for {address} reopened at the wrong level"
        );
        dyn_store
            .clone()
            .get(partition, *address)
            .await
            .unwrap_or_else(|err| {
                panic!("{address} was written and persisted but reads back as {err:?}")
            });
    }
}

mod oodle_migration_seed {
    use super::*;

    /// A store with fragments on disk and no info file, as one written by a binary predating
    /// the file. Reports the highest group index holding data.
    async fn store_predating_the_info_file(dir: &lore_base::test_util::TempDir) -> i32 {
        let highest = {
            let store = LocalImmutableStore::new(Some(dir.path().to_path_buf()), client_settings())
                .await
                .expect("store opens");
            let addresses = put_fragments(&store, Partition::from([7u8; 16]), 32).await;
            let dyn_store: Arc<dyn lore_storage::immutable_store::ImmutableStore> = store.clone();
            dyn_store.flush(true).await.expect("store flushes");
            addresses
                .iter()
                .map(|address| address.hash.data()[0] as i32)
                .max()
                .expect("fragments were written")
        };
        std::fs::remove_file(info_path_for_store_root(&dir.path().join("immutable")))
            .expect("the info file a newer binary wrote is removed");
        highest
    }

    /// A group that was never written holds no payload of any codec, so a store made only of
    /// them starts with nothing to migrate - and says so rather than sweeping all 256.
    #[tokio::test]
    async fn a_store_with_no_written_group_has_nothing_to_migrate() {
        let dir = lore_base::test_util::TempDir::new("is_seed_fresh_");
        let store = LocalImmutableStore::new(Some(dir.path().to_path_buf()), client_settings())
            .await
            .expect("store opens");

        assert_eq!(
            store.info.read().await.next_group_index_to_migrate_oodle,
            -1
        );
    }

    /// A store written before the info file existed is seeded from the groups actually on
    /// disk, so the pass starts at the highest one holding data.
    #[cfg(feature = "oodle")]
    #[tokio::test]
    async fn a_store_predating_the_info_file_is_seeded_from_its_written_groups() {
        let dir = lore_base::test_util::TempDir::new("is_seed_existing_");
        let highest = store_predating_the_info_file(&dir).await;

        let store = LocalImmutableStore::new(Some(dir.path().to_path_buf()), client_settings())
            .await
            .expect("store reopens");
        assert_eq!(
            store.info.read().await.next_group_index_to_migrate_oodle,
            highest
        );
    }

    /// A binary that cannot decode Oodle can do nothing about a store that holds it, so it
    /// records no work rather than a bookmark it could never act on.
    #[cfg(not(feature = "oodle"))]
    #[tokio::test]
    async fn a_binary_without_oodle_records_no_migration() {
        let dir = lore_base::test_util::TempDir::new("is_seed_no_oodle_");
        store_predating_the_info_file(&dir).await;

        let store = LocalImmutableStore::new(Some(dir.path().to_path_buf()), client_settings())
            .await
            .expect("store reopens");
        assert_eq!(
            store.info.read().await.next_group_index_to_migrate_oodle,
            -1
        );
    }
}

/// A store written before lazy fan-out carries no level markers and a flat 256-bucket
/// layout, which `read_group_level` reports as `PreFanOut`. Such a group already reads back
/// at the level it was written at, so nothing may start writing markers into it: that would
/// change which flush path the next flush takes on a store this version has to leave alone.
#[tokio::test]
async fn a_pre_fan_out_store_gains_no_level_markers() {
    let dir = lore_base::test_util::TempDir::new("is_prefanout_");
    let partition = Partition::from([3u8; 16]);
    let index_root = dir.path().join("immutable").join("index");

    // A group as the previous format left it: one bucket file at the pre-fan-out version,
    // no marker beside it.
    let legacy_group = index_root.join("2a");
    std::fs::create_dir_all(&legacy_group).expect("group dir");
    write_bucket_file(
        &legacy_group.join("index_10"),
        ImmutableStoreVersion::LastAccessInEntry as u32,
    );

    let addresses = {
        let store = LocalImmutableStore::new(Some(dir.path().to_path_buf()), client_settings())
            .await
            .expect("store opens");
        assert_eq!(
            store.group[0x2a]
                .bucket_count
                .load(atomic::Ordering::Relaxed),
            BUCKET_COUNT,
            "a marker-less group holding bucket files opens pre-fan-out"
        );
        assert_eq!(
            store.group[0].bucket_count.load(atomic::Ordering::Relaxed),
            BUCKET_COUNT,
            "a legacy store keeps even its fresh groups at the flat layout"
        );

        let addresses = put_fragments(&store, partition, 32).await;
        run_delayed_flush(&store).await;
        addresses
    };

    assert_eq!(
        level_markers(&index_root),
        Vec::<PathBuf>::new(),
        "a pre-fan-out store must come back out with the layout it went in with"
    );

    let store = LocalImmutableStore::new(Some(dir.path().to_path_buf()), client_settings())
        .await
        .expect("store reopens");
    let dyn_store: Arc<dyn lore_storage::immutable_store::ImmutableStore> = store.clone();
    for address in &addresses {
        let group = &store.group[address.hash.data()[0] as usize];
        assert_eq!(
            group.bucket_count.load(atomic::Ordering::Relaxed),
            BUCKET_COUNT,
            "group for {address} reopened at the wrong level"
        );
        dyn_store
            .clone()
            .get(partition, *address)
            .await
            .unwrap_or_else(|err| panic!("{address} reads back as {err:?}"));
    }
}

/// A server store starts its groups at the full 256 buckets, where a missing marker is read
/// back as exactly that. It gains no markers either, for the same reason.
#[tokio::test]
async fn a_server_shaped_store_gains_no_level_markers() {
    let dir = lore_base::test_util::TempDir::new("is_server_level_");
    let partition = Partition::from([9u8; 16]);
    let index_root = dir.path().join("immutable").join("index");

    let addresses = {
        let store = LocalImmutableStore::new(Some(dir.path().to_path_buf()), server_settings())
            .await
            .expect("store opens");
        assert_eq!(
            store.group[0].bucket_count.load(atomic::Ordering::Relaxed),
            BUCKET_COUNT,
            "a server store starts at the flat layout"
        );

        let addresses = put_fragments(&store, partition, 32).await;
        run_delayed_flush(&store).await;
        addresses
    };

    assert_eq!(
        level_markers(&index_root),
        Vec::<PathBuf>::new(),
        "a group already at 256 reads back at 256 without a marker"
    );

    let store = LocalImmutableStore::new(Some(dir.path().to_path_buf()), server_settings())
        .await
        .expect("store reopens");
    let dyn_store: Arc<dyn lore_storage::immutable_store::ImmutableStore> = store.clone();
    for address in &addresses {
        let group = &store.group[address.hash.data()[0] as usize];
        assert_eq!(
            group.bucket_count.load(atomic::Ordering::Relaxed),
            BUCKET_COUNT,
            "group for {address} reopened at the wrong level"
        );
        dyn_store
            .clone()
            .get(partition, *address)
            .await
            .unwrap_or_else(|err| panic!("{address} reads back as {err:?}"));
    }
}

/// A group that already carries a marker keeps the level it records: the initial-level write
/// is for groups that have never had one, and must not overwrite a committed level.
#[tokio::test]
async fn a_marked_group_keeps_the_level_it_recorded() {
    let dir = lore_base::test_util::TempDir::new("is_marked_level_");
    let partition = Partition::from([5u8; 16]);
    let index_root = dir.path().join("immutable").join("index");

    {
        let store = LocalImmutableStore::new(Some(dir.path().to_path_buf()), client_settings())
            .await
            .expect("store opens");
        let dyn_store: Arc<dyn lore_storage::immutable_store::ImmutableStore> = store.clone();
        let _ = put_fragments(&store, partition, 8).await;
        // A real flush commits the level through the two-phase path.
        dyn_store.flush(false).await.expect("flush succeeds");
    }

    let before: Vec<(PathBuf, Vec<u8>)> = level_markers(&index_root)
        .into_iter()
        .map(|path| {
            let bytes = std::fs::read(&path).expect("marker readable");
            (path, bytes)
        })
        .collect();
    assert!(
        !before.is_empty(),
        "the flush has to have committed a level"
    );

    {
        let store = LocalImmutableStore::new(Some(dir.path().to_path_buf()), client_settings())
            .await
            .expect("store reopens");
        let _ = put_fragments(&store, partition, 16).await;
        run_delayed_flush(&store).await;
    }

    for (path, bytes) in before {
        assert_eq!(
            std::fs::read(&path).expect("marker still readable"),
            bytes,
            "marker at {} was rewritten",
            path.display()
        );
    }
}

#[test]
fn lazy_fan_out_version_is_five() {
    assert_eq!(ImmutableStoreVersion::LazyFanOut as u32, 5);
}

#[test]
fn format_bucket_path_is_index_group_bucket() {
    let root = Path::new("/store");
    for index in [0usize, 0xab, 255] {
        let byte = index as u8;
        assert_eq!(
            format_bucket_path(root, index, index),
            root.join("index").join(format!("{byte:02x}")).join(format!(
                "{}{byte:02x}",
                lore_storage::local::fan_out::BUCKET_FILENAME_PREFIX
            ))
        );
    }
    assert_eq!(
        format_bucket_path(root, 0x0f, 0xf0),
        Path::new("/store/index/0f/index_f0")
    );
}

#[tokio::test]
async fn deserialize_accepts_last_access_in_entry_v4() {
    let dir = lore_base::test_util::TempDir::new("is_v4_");
    let path = dir.path().join("bucket");
    write_bucket_file(&path, ImmutableStoreVersion::LastAccessInEntry as u32);
    let result = ImmutableStoreBucket::deserialize_files(path).await;
    assert!(
        result.is_ok(),
        "v4 (LastAccessInEntry) bucket should deserialize"
    );
}

#[tokio::test]
async fn deserialize_accepts_lazy_fan_out_v5() {
    let dir = lore_base::test_util::TempDir::new("is_v5_");
    let path = dir.path().join("bucket");
    write_bucket_file(&path, ImmutableStoreVersion::LazyFanOut as u32);
    let result = ImmutableStoreBucket::deserialize_files(path).await;
    assert!(result.is_ok(), "v5 (LazyFanOut) bucket should deserialize");
}

#[tokio::test]
async fn deserialize_rejects_unknown_future_version() {
    let dir = lore_base::test_util::TempDir::new("is_v100_");
    let path = dir.path().join("bucket");
    write_bucket_file(&path, 100);
    let result = ImmutableStoreBucket::deserialize_files(path.clone()).await;
    assert!(result.is_err(), "v100 bucket should be rejected as too new");
    // Future-version files MUST be preserved on disk — recovery would clobber data
    // written by a newer binary.
    assert!(
        path.exists(),
        "future-version bucket file must be preserved, not deleted"
    );
}

/// Write a v5 bucket file whose header claims `header_count` entries but contains
/// only `actual_entries_on_disk` entry slots — the crash-mid-flush shape.
fn write_bucket_file_with_count_mismatch(
    path: &Path,
    header_count: u32,
    actual_entries_on_disk: u32,
) {
    let mut header = ImmutableStoreHeader::new_zeroed();
    header.version = ImmutableStoreVersion::LazyFanOut as u32;
    header.count = header_count;
    let mut bytes = Vec::new();
    bytes.extend_from_slice(header.as_bytes());
    for i in 0..actual_entries_on_disk {
        bytes.extend_from_slice(&i.to_le_bytes());
    }
    for _ in 0..actual_entries_on_disk {
        let entry = ImmutableStoreEntry::default();
        bytes.extend_from_slice(entry.as_bytes());
    }
    std::fs::write(path, bytes).unwrap();
}

#[tokio::test]
async fn deserialize_recovers_from_bad_count_header() {
    // Mirrors the production server log shape (header.count > what file fits).
    let dir = lore_base::test_util::TempDir::new("is_badcount_");
    let path = dir.path().join("bucket");
    write_bucket_file_with_count_mismatch(&path, 670, 518);
    let result = ImmutableStoreBucket::deserialize_files(path.clone()).await;
    let BucketFile {
        sorted_index,
        entry,
        mark_dirty,
        ..
    } = result.expect("count-mismatch corruption must recover");
    assert!(sorted_index.is_empty());
    assert!(entry.is_empty());
    assert!(!mark_dirty);
    assert!(!path.exists(), "corrupt bucket file must be removed");
}

#[tokio::test]
async fn deserialize_recovers_from_invalid_version() {
    // 0xFFFF is above the future-version sentinel range, so it's corruption.
    let dir = lore_base::test_util::TempDir::new("is_badver_");
    let path = dir.path().join("bucket");
    write_bucket_file(&path, 0xFFFF);
    let result = ImmutableStoreBucket::deserialize_files(path.clone()).await;
    let BucketFile {
        sorted_index,
        entry,
        ..
    } = result.expect("invalid-version corruption must recover");
    assert!(sorted_index.is_empty());
    assert!(entry.is_empty());
    assert!(!path.exists(), "corrupt bucket file must be removed");
}

#[tokio::test]
async fn deserialize_recovers_from_truncated_entries() {
    let dir = lore_base::test_util::TempDir::new("is_trunc_");
    let path = dir.path().join("bucket");
    let mut header = ImmutableStoreHeader::new_zeroed();
    header.version = ImmutableStoreVersion::LazyFanOut as u32;
    header.count = 3;
    let mut bytes = Vec::new();
    bytes.extend_from_slice(header.as_bytes());
    for i in 0u32..3 {
        bytes.extend_from_slice(&i.to_le_bytes());
    }
    let entry = ImmutableStoreEntry::default();
    bytes.extend_from_slice(entry.as_bytes());
    bytes.extend_from_slice(&entry.as_bytes()[..size_of::<ImmutableStoreEntry>() / 2]);
    std::fs::write(&path, bytes).unwrap();

    let result = ImmutableStoreBucket::deserialize_files(path.clone()).await;
    let BucketFile {
        sorted_index,
        entry,
        ..
    } = result.expect("truncated-entries corruption must recover");
    assert!(sorted_index.is_empty());
    assert!(entry.is_empty());
    assert!(!path.exists(), "corrupt bucket file must be removed");
}

#[tokio::test]
async fn deserialize_recovers_from_short_header() {
    // File too small to even hold the header.
    let dir = lore_base::test_util::TempDir::new("is_shorthdr_");
    let path = dir.path().join("bucket");
    std::fs::write(&path, [0u8; 4]).unwrap();
    let result = ImmutableStoreBucket::deserialize_files(path.clone()).await;
    let BucketFile {
        sorted_index,
        entry,
        ..
    } = result.expect("short-header corruption must recover");
    assert!(sorted_index.is_empty());
    assert!(entry.is_empty());
    assert!(!path.exists(), "corrupt bucket file must be removed");
}

#[tokio::test]
async fn store_recovers_from_corrupt_bucket_and_remains_usable() {
    // End-to-end: corrupt a bucket file and verify the store is still usable for
    // writes and reads on that bucket. Original content is lost (expected).
    use lore_storage::options::ReadOptions;
    use lore_storage::options::WriteOptions;
    use lore_storage::read::read;
    use lore_storage::write::StoreResult;
    use lore_storage::write::write_content;

    let dir = lore_base::test_util::TempDir::new("is_e2e_recover_");
    let store_path = dir.path().to_path_buf();
    let partition = Partition::from([0x42u8; 16]);
    let context = Context::from([0x07u8; 16]);
    let payload = Bytes::from(vec![0xCDu8; 256]);

    let address = {
        let store: Arc<dyn lore_storage::immutable_store::ImmutableStore> = create(
            Some(&store_path),
            ImmutableStoreCreateOptions::none(),
            false,
            ImmutableStoreSettings {
                initial_fan_out_level: 1,
                ..Default::default()
            },
        )
        .await
        .unwrap();

        let StoreResult { address, .. } = write_content(
            store.clone(),
            partition,
            context,
            payload.clone(),
            WriteOptions::default(),
            None,
            lore_storage::write_tracker::WriteContext::none(),
            None,
        )
        .await
        .unwrap();

        store.clone().flush(true).await.unwrap();
        address
    };

    // initial_fan_out_level=1 → bucket index is always 0; group is hash[0].
    let group_index = address.hash.data()[0] as usize;
    let bucket_path = store_path
        .join("immutable")
        .join("index")
        .join(format!("{group_index:02x}"))
        .join("index_00");
    assert!(
        bucket_path.exists(),
        "bucket file should exist after flush at {bucket_path:?}"
    );

    // Crash-mid-flush shape: header claims N entries, body is short.
    let mut header = ImmutableStoreHeader::new_zeroed();
    header.version = ImmutableStoreVersion::LazyFanOut as u32;
    header.count = 4096;
    let mut bytes = Vec::new();
    bytes.extend_from_slice(header.as_bytes());
    bytes.extend_from_slice(&[0u8; size_of::<u32>() * 4]);
    std::fs::write(&bucket_path, bytes).unwrap();

    let store: Arc<dyn lore_storage::immutable_store::ImmutableStore> = create(
        Some(&store_path),
        ImmutableStoreCreateOptions::none(),
        false,
        ImmutableStoreSettings {
            initial_fan_out_level: 1,
            ..Default::default()
        },
    )
    .await
    .unwrap();

    // Original content is gone (data lost), but the bucket is operational.
    let read_result = read(
        store.clone(),
        partition,
        address,
        None,
        ReadOptions::default(),
        None,
    )
    .await;
    assert!(
        read_result.is_err(),
        "originally stored content must be reported missing after recovery"
    );

    let StoreResult {
        address: new_address,
        ..
    } = write_content(
        store.clone(),
        partition,
        context,
        payload.clone(),
        WriteOptions::default(),
        None,
        lore_storage::write_tracker::WriteContext::none(),
        None,
    )
    .await
    .unwrap();
    assert_eq!(new_address, address);

    let (_fragment, bytes) = read(
        store.clone(),
        partition,
        new_address,
        None,
        ReadOptions::default(),
        None,
    )
    .await
    .unwrap();
    assert_eq!(bytes.as_ref(), payload.as_ref());
}

#[test]
fn immutable_store_settings_default_includes_fan_out_fields() {
    let s = ImmutableStoreSettings::default();
    assert_eq!(s.initial_fan_out_level, 1);
    assert_eq!(
        s.fan_out_threshold,
        lore_storage::local::fan_out::FAN_OUT_THRESHOLD_DEFAULT
    );
}

#[tokio::test]
async fn store_initializes_group_bucket_count_from_settings_level_1() {
    use std::sync::atomic::Ordering;
    let store = LocalImmutableStore::new(
        None,
        ImmutableStoreSettings {
            initial_fan_out_level: 1,
            ..Default::default()
        },
    )
    .await
    .unwrap();
    for group in &store.group {
        assert_eq!(group.bucket_count.load(Ordering::Relaxed), 1);
    }
}

#[tokio::test]
async fn store_initializes_group_bucket_count_from_settings_level_256() {
    use std::sync::atomic::Ordering;
    let store = LocalImmutableStore::new(
        None,
        ImmutableStoreSettings {
            initial_fan_out_level: lore_storage::local::fan_out::FAN_OUT_LEVEL_MAX,
            ..Default::default()
        },
    )
    .await
    .unwrap();
    for group in &store.group {
        assert_eq!(
            group.bucket_count.load(Ordering::Relaxed),
            lore_storage::local::fan_out::FAN_OUT_LEVEL_MAX
        );
    }
}

/// A group compacted to the end reports complete, so the resume point moves past it, and is
/// rewritten down to its non-durable payloads.
#[tokio::test]
async fn group_compaction_reports_a_finished_group_complete() {
    use lore_storage::immutable_store::ImmutableStore;

    let dir = lore_base::test_util::TempDir::new("is_stop_report_");
    let store =
        LocalImmutableStore::new(Some(dir.to_path_buf()), ImmutableStoreSettings::default())
            .await
            .unwrap();

    let partition = Partition::from([0x0cu8; 16]);
    let (group_index, payloads) = payloads_in_one_group(8);
    for (index, payload) in payloads.iter().enumerate() {
        put_payload(
            &store,
            partition,
            Context::default(),
            payload,
            index % 2 == 0,
        )
        .await;
    }
    store.clone().flush(true).await.unwrap();

    assert!(
        compact_group(&store, group_index).await,
        "a group that ran to the end must report complete"
    );
    assert_eq!(
        store.group[group_index]
            .packstore
            .total_size()
            .await
            .unwrap(),
        4 * 4096,
        "the sweep must have rewritten the group down to its non-durable payloads"
    );
}

/// Whether each entry still points at a payload, by whether it is durable.
fn payload_kept_by_durability(entries: &[ImmutableStoreEntry]) -> Vec<(bool, bool)> {
    let durable = FragmentFlags::PayloadStoredDurable.bits();
    let mut kept: Vec<(bool, bool)> = entries
        .iter()
        .map(|entry| (entry.data.flags & durable != 0, entry.data.pack_file != 0))
        .collect();
    kept.sort_unstable();
    kept
}

/// Local payloads count toward a group's size target although they are never evicted, so a
/// group they alone keep over the target loses every durable payload and keeps every local one:
/// 8 KiB of durable payloads, under the 12 KiB target alone, beside 16 KiB of local ones.
#[tokio::test]
async fn size_eviction_counts_local_payloads_toward_the_target() {
    let dir = lore_base::test_util::TempDir::new("is_evict_local_");
    let store = LocalImmutableStore::new(Some(dir.to_path_buf()), client_settings())
        .await
        .unwrap();

    let partition = Partition::from([0x0eu8; 16]);
    let (group_index, payloads) = payloads_in_one_group(6);
    for (index, payload) in payloads.iter().enumerate() {
        put_payload(&store, partition, Context::default(), payload, index < 2).await;
    }

    let evicted = store
        .clone()
        .evict_group_sized(group_index, 3 * 4096, store.path.clone(), true, false)
        .await;

    assert_eq!(evicted, (2, 2 * 4096), "both durable payloads are evicted");
    let local = FragmentFlags::PayloadStoredLocal.bits();
    assert!(
        group_entries(&store, group_index)
            .await
            .iter()
            .all(|entry| (entry.data.pack_file != 0) == (entry.data.flags & local != 0)),
        "an entry claims a local payload exactly when it holds one"
    );
    assert_eq!(
        payload_kept_by_durability(&group_entries(&store, group_index).await),
        [(false, true); 4]
            .into_iter()
            .chain([(true, false); 2])
            .collect::<Vec<_>>(),
        "every local payload is kept"
    );
}

/// Each bucket is held to its own share of the group's target: a bucket over its share loses its
/// durable payloads, while one within its share keeps them, though the group is over its target.
/// The group holds 160 KiB over a 128 KiB target, a share of 4 KiB in each of 32 buckets.
#[tokio::test]
async fn size_eviction_leaves_a_bucket_within_its_share() {
    use lore_storage::local::fan_out::bucket_index_for;

    const BUCKET_COUNT: usize = 32;

    let store = LocalImmutableStore::new(
        None,
        ImmutableStoreSettings {
            initial_fan_out_level: BUCKET_COUNT,
            ..client_settings()
        },
    )
    .await
    .unwrap();

    let partition = Partition::from([0x18u8; 16]);
    let (group_index, payloads) = payloads_in_one_group(40);
    let mut by_bucket: HashMap<usize, Vec<&Vec<u8>>> = HashMap::new();
    for payload in &payloads {
        let bucket_index = bucket_index_for(&hash::hash_slice(payload), BUCKET_COUNT);
        by_bucket.entry(bucket_index).or_default().push(payload);
    }
    let over = *by_bucket
        .iter()
        .find(|(_, payloads)| payloads.len() > 1)
        .expect("a bucket holds two payloads")
        .0;
    let within = *by_bucket
        .iter()
        .find(|(_, payloads)| payloads.len() == 1)
        .expect("a bucket holds one payload")
        .0;
    for (bucket_index, payloads) in &by_bucket {
        for (index, payload) in payloads.iter().enumerate() {
            let durable = (*bucket_index == over || *bucket_index == within) && index == 0;
            put_payload(&store, partition, Context::default(), payload, durable).await;
        }
    }

    let evicted = store
        .clone()
        .evict_group_sized(group_index, BUCKET_COUNT * 4096, None, true, false)
        .await;

    assert_eq!(evicted, (1, 4096), "only the bucket over its share evicts");
    let kept_in = |bucket_index: usize, entries: &[ImmutableStoreEntry]| {
        payload_kept_by_durability(
            &entries
                .iter()
                .filter(|entry| bucket_index_for(&entry.address.hash, BUCKET_COUNT) == bucket_index)
                .copied()
                .collect::<Vec<_>>(),
        )
    };
    let entries = group_entries(&store, group_index).await;
    assert_eq!(
        kept_in(within, &entries),
        vec![(true, true)],
        "the bucket within its share keeps its durable payload"
    );
    assert!(
        kept_in(over, &entries).contains(&(true, false)),
        "the bucket over its share evicts its durable payload"
    );
}

/// A bucket with nothing it may evict is left unmarked, so it is not written again, though its
/// group evicts from another bucket. The target equals the durable payloads' size, and both
/// buckets exceed their share. The store is in memory, where no write after eviction clears the
/// marks.
#[tokio::test]
async fn size_eviction_leaves_a_bucket_with_nothing_evictable_clean() {
    use lore_storage::local::fan_out::bucket_index_for;

    const BUCKET_COUNT: usize = 32;
    const PAYLOAD_SIZE: usize = 64;

    let store = LocalImmutableStore::new(
        None,
        ImmutableStoreSettings {
            initial_fan_out_level: BUCKET_COUNT,
            ..client_settings()
        },
    )
    .await
    .unwrap();

    let partition = Partition::from([0x1au8; 16]);
    let (group_index, payloads) = payloads_of_size_in_one_group(8 * BUCKET_COUNT, PAYLOAD_SIZE);
    let mut by_bucket: HashMap<usize, Vec<&Vec<u8>>> = HashMap::new();
    for payload in &payloads {
        let bucket_index = bucket_index_for(&hash::hash_slice(payload), BUCKET_COUNT);
        by_bucket.entry(bucket_index).or_default().push(payload);
    }
    let (&durable_bucket, durable_payloads) = by_bucket
        .iter()
        .max_by_key(|(_, payloads)| payloads.len())
        .unwrap();
    let (&local_bucket, local_payloads) = by_bucket
        .iter()
        .find(|(bucket_index, _)| **bucket_index != durable_bucket)
        .unwrap();
    for payload in durable_payloads {
        put_payload(&store, partition, Context::default(), payload, true).await;
    }
    for payload in local_payloads {
        put_payload(&store, partition, Context::default(), payload, false).await;
    }
    let group = &store.group[group_index];
    for dirty in &group.dirty {
        dirty.store(false, atomic::Ordering::Relaxed);
    }

    let target_size = durable_payloads.len() * PAYLOAD_SIZE;
    assert!(local_payloads.len() * PAYLOAD_SIZE > target_size / BUCKET_COUNT);
    let (evicted, _) = store
        .clone()
        .evict_group_sized(group_index, target_size, None, true, false)
        .await;

    assert!(evicted > 0, "the bucket of durable payloads evicts");
    assert!(group.dirty[durable_bucket].load(atomic::Ordering::Relaxed));
    assert!(
        !group.dirty[local_bucket].load(atomic::Ordering::Relaxed),
        "the bucket of local payloads is not marked for writing"
    );
}

/// Local fragments count toward a bucket's capacity target although they are never evicted, so
/// a bucket they alone keep over the target loses its durable fragments. A capacity of 732 gives
/// each of the 256 single-bucket groups a target of 2 fragments: above the one durable fragment,
/// below the four in all.
#[tokio::test]
async fn capacity_eviction_counts_local_fragments_toward_the_target() {
    use lore_storage::immutable_store::ImmutableStore;

    let store = LocalImmutableStore::new(None, client_settings())
        .await
        .unwrap();

    let partition = Partition::from([0x14u8; 16]);
    let (group_index, payloads) = payloads_in_one_group(4);
    for (index, payload) in payloads.iter().enumerate() {
        put_payload(&store, partition, Context::default(), payload, index == 0).await;
    }

    let evicted = store.clone().evict(732, false, None).await.unwrap();

    assert_eq!(evicted, 1, "the durable fragment is evicted");
    assert_eq!(
        payload_kept_by_durability(&group_entries(&store, group_index).await),
        vec![(false, true); 3],
        "every local fragment is kept"
    );
}

/// A bucket over its capacity target with nothing it may evict starts no eviction, so a store
/// that local fragments alone keep over the target reports no eviction to the command. The
/// target is 2 fragments per bucket, under the four local fragments.
#[tokio::test]
async fn capacity_eviction_with_nothing_evictable_reports_nothing() {
    use lore_storage::immutable_store::ImmutableStore;

    #[derive(Default)]
    struct EvictionSink {
        begins: AtomicUsize,
    }

    impl lore_storage::gc_event::GcEventSink for EvictionSink {
        fn eviction_begin(&self, _target_fragments: u64) {
            self.begins.fetch_add(1, atomic::Ordering::Relaxed);
        }
        fn eviction_progress(&self, _evicted: u64) {}
        fn eviction_end(&self, _total_evicted: u64) {}
        fn compaction_begin(&self, _target_bytes: u64) {}
        fn compaction_progress(&self, _compacted_bytes: u64) {}
        fn compaction_end(&self, _total_compacted_bytes: u64) {}
    }

    let store = LocalImmutableStore::new(None, client_settings())
        .await
        .unwrap();

    let partition = Partition::from([0x16u8; 16]);
    let (_group_index, payloads) = payloads_in_one_group(4);
    for payload in &payloads {
        put_payload(&store, partition, Context::default(), payload, false).await;
    }

    let sink = Arc::new(EvictionSink::default());
    let evicted = store
        .clone()
        .evict(732, false, Some(sink.clone()))
        .await
        .unwrap();

    assert_eq!(evicted, 0);
    assert_eq!(
        sink.begins.load(atomic::Ordering::Relaxed),
        0,
        "no eviction is announced"
    );
}

/// The packfile and offset each entry points at, by hash.
fn payload_locations(entries: &[ImmutableStoreEntry]) -> Vec<(Hash, u32, u32)> {
    entries
        .iter()
        .map(|entry| {
            (
                entry.address.hash,
                entry.data.pack_file,
                entry.data.pack_offset,
            )
        })
        .collect()
}

/// Compact `group_index` of a client `store` towards a target of one byte, so that the size
/// never ends the sweep. Returns whether the group was left complete.
async fn compact_group(store: &Arc<LocalImmutableStore>, group_index: usize) -> bool {
    compact_group_toward(store, group_index, 1, true).await
}

/// Rewriting packfiles whose payloads are all still referenced reclaims nothing, so
/// compaction leaves them in place however far the group is over its target. A rewrite copies
/// payloads in hash order, so payloads stored in reverse hash order move under any rewrite, even
/// one that ends in the packfile it started from.
#[tokio::test]
async fn compaction_leaves_a_fully_referenced_group_in_place() {
    use lore_storage::immutable_store::ImmutableStore;

    let dir = lore_base::test_util::TempDir::new("is_compact_skip_");
    let store = LocalImmutableStore::new(Some(dir.to_path_buf()), client_settings())
        .await
        .unwrap();

    let partition = Partition::from([0x10u8; 16]);
    let (group_index, mut payloads) = payloads_in_one_group(4);
    payloads.sort_unstable_by_key(|payload| std::cmp::Reverse(hash::hash_slice(payload)));
    for payload in &payloads {
        put_payload(&store, partition, Context::default(), payload, false).await;
    }
    store.clone().flush(true).await.unwrap();

    let group = &store.group[group_index];
    assert_eq!(group.packstore.sizes().await.unwrap(), vec![4 * 4096]);
    let stored_at = payload_locations(&group_entries(&store, group_index).await);

    assert!(
        compact_group(&store, group_index).await,
        "a group with nothing to reclaim is complete"
    );
    assert_eq!(
        payload_locations(&group_entries(&store, group_index).await),
        stored_at,
        "the packfile is not rewritten"
    );
}

/// A packfile holding payloads that eviction orphaned is rewritten once, down to the payloads
/// still referenced, which read back afterwards. Packfile 1 stops taking writes for its rewrite,
/// so its payloads move to a new packfile 2, which is fully referenced and not copied back.
#[tokio::test]
async fn compaction_rewrites_a_packfile_with_orphaned_payloads() {
    use lore_storage::immutable_store::ImmutableStore;

    let dir = lore_base::test_util::TempDir::new("is_compact_orphaned_");
    let store = LocalImmutableStore::new(Some(dir.to_path_buf()), client_settings())
        .await
        .unwrap();

    let partition = Partition::from([0x12u8; 16]);
    let (group_index, payloads) = payloads_in_one_group(4);
    let mut local = vec![];
    for (index, payload) in payloads.iter().enumerate() {
        let durable = index < 2;
        let address = put_payload(&store, partition, Context::default(), payload, durable).await;
        if !durable {
            local.push((address, payload.clone()));
        }
    }
    store.clone().flush(true).await.unwrap();

    assert!(compact_group(&store, group_index).await);
    assert_eq!(
        store.group[group_index]
            .packstore
            .total_size()
            .await
            .unwrap(),
        2 * 4096,
        "the durable payloads eviction orphaned are reclaimed"
    );
    assert!(
        group_entries(&store, group_index)
            .await
            .iter()
            .filter(|entry| entry.data.pack_file != 0)
            .all(|entry| entry.data.pack_file == 2),
        "the rewritten payloads are copied once"
    );
    assert_payloads_read_back(&store, partition, local).await;
}

/// Store one durable and `local` non-durable payloads of 4 KiB in one group, in reverse hash
/// order so that any rewrite moves them, and compact it once eviction has orphaned the durable
/// one. Returns the group's packfile bytes, and whether the local payloads stayed where they were
/// stored.
async fn compact_with_one_orphan(dir_prefix: &str, local: usize) -> (usize, bool) {
    use lore_storage::immutable_store::ImmutableStore;

    let dir = lore_base::test_util::TempDir::new(dir_prefix);
    let store = LocalImmutableStore::new(Some(dir.to_path_buf()), client_settings())
        .await
        .unwrap();

    let partition = Partition::from([0x1eu8; 16]);
    let (group_index, mut payloads) = payloads_in_one_group(1 + local);
    payloads.sort_unstable_by_key(|payload| std::cmp::Reverse(hash::hash_slice(payload)));
    for (index, payload) in payloads.iter().enumerate() {
        put_payload(&store, partition, Context::default(), payload, index == 0).await;
    }
    store.clone().flush(true).await.unwrap();

    let durable = FragmentFlags::PayloadStoredDurable.bits();
    let local_locations = |entries: Vec<ImmutableStoreEntry>| {
        payload_locations(
            &entries
                .into_iter()
                .filter(|entry| entry.data.flags & durable == 0)
                .collect::<Vec<_>>(),
        )
    };
    let stored_at = local_locations(group_entries(&store, group_index).await);

    assert!(compact_group(&store, group_index).await);
    (
        store.group[group_index]
            .packstore
            .total_size()
            .await
            .unwrap(),
        local_locations(group_entries(&store, group_index).await) == stored_at,
    )
}

/// One orphaned payload in eleven is under the share of unreferenced bytes that makes a rewrite
/// worth its copying, so compaction leaves the group's packfile in place.
#[tokio::test]
async fn compaction_leaves_a_group_below_the_unreferenced_threshold() {
    let (stored_size, in_place) = compact_with_one_orphan("is_compact_below_", 10).await;

    assert!(in_place, "the packfile is not rewritten");
    assert_eq!(
        stored_size,
        11 * 4096,
        "the orphaned payload is not reclaimed"
    );
}

/// One orphaned payload in ten is the share of unreferenced bytes that makes a rewrite worth its
/// copying, so compaction reclaims it.
#[tokio::test]
async fn compaction_rewrites_a_group_at_the_unreferenced_threshold() {
    let (stored_size, in_place) = compact_with_one_orphan("is_compact_at_", 9).await;

    assert!(!in_place, "the packfile is rewritten");
    assert_eq!(stored_size, 9 * 4096, "the orphaned payload is reclaimed");
}

/// A store over `dir` holding the payloads of `layout[n]` in packfile `n + 1` of their group, each
/// durable as its flag says, each packfile closed to writes to open the next, reopened as a later
/// process finds it: every packfile under the size limit takes writes. Returns the store and the
/// address of each payload.
async fn store_in_packfiles(
    dir: &Path,
    partition: Partition,
    layout: &[Vec<(&Vec<u8>, bool)>],
) -> (Arc<LocalImmutableStore>, Vec<Address>) {
    use lore_storage::immutable_store::ImmutableStore;

    let mut addresses = vec![];
    {
        let store = LocalImmutableStore::new(Some(dir.to_path_buf()), client_settings())
            .await
            .unwrap();
        let mut group_index = 0;
        for (index, payloads) in layout.iter().enumerate() {
            if index > 0 {
                store.group[group_index]
                    .packstore
                    .stop_writes(&[index as u32])
                    .await
                    .unwrap();
            }
            for (payload, durable) in payloads {
                group_index = hash::hash_slice(payload).data()[0] as usize;
                addresses.push(
                    put_payload(&store, partition, Context::default(), payload, *durable).await,
                );
            }
        }
        store.clone().flush(true).await.unwrap();
    }
    let store = LocalImmutableStore::new(Some(dir.to_path_buf()), client_settings())
        .await
        .unwrap();
    store.deserialize_all_buckets().await.unwrap();
    (store, addresses)
}

/// The packfile an entry for `address` points at.
fn packfile_of(entries: &[ImmutableStoreEntry], address: Address) -> u32 {
    entries
        .iter()
        .find(|entry| entry.address == address)
        .expect("an entry for the address")
        .data
        .pack_file
}

/// Compaction closes every packfile it rewrites before rewriting any, so a payload it copies
/// lands in a packfile it leaves alone and is copied once. Copied into packfile 2, packfile 1's
/// payload would be copied again when packfile 2 is rewritten.
#[tokio::test]
async fn compaction_copies_each_payload_once() {
    let dir = lore_base::test_util::TempDir::new("is_compact_once_");
    let partition = Partition::from([0x22u8; 16]);
    let (group_index, payloads) = payloads_in_one_group(4);
    let (store, addresses) = store_in_packfiles(
        dir.as_ref(),
        partition,
        &[
            vec![(&payloads[0], true), (&payloads[1], false)],
            vec![(&payloads[2], true), (&payloads[3], false)],
        ],
    )
    .await;

    assert!(compact_group(&store, group_index).await);

    assert_eq!(
        packfile_of(&group_entries(&store, group_index).await, addresses[1]),
        3,
        "packfile 1's payload is copied into a packfile the pass leaves alone"
    );
    assert_eq!(
        store.group[group_index]
            .packstore
            .total_size()
            .await
            .unwrap(),
        2 * 4096
    );
}

/// However many packfiles a group has, compaction rewrites only those holding unreferenced bytes:
/// every orphaned payload is reclaimed, and a fully referenced packfile keeps its payloads where
/// they are. The odd packfiles hold a durable payload for eviction to orphan, the even ones only
/// local payloads.
#[tokio::test]
async fn compaction_rewrites_only_the_packfiles_with_orphans() {
    const PACKFILE_COUNT: usize = 5;

    let dir = lore_base::test_util::TempDir::new("is_compact_many_");
    let partition = Partition::from([0x20u8; 16]);
    let (group_index, payloads) = payloads_in_one_group(2 * PACKFILE_COUNT);
    let layout: Vec<_> = payloads
        .chunks(2)
        .enumerate()
        .map(|(index, pair)| vec![(&pair[0], index % 2 == 0), (&pair[1], false)])
        .collect();
    let (store, addresses) = store_in_packfiles(dir.as_ref(), partition, &layout).await;
    let group = &store.group[group_index];
    assert_eq!(
        group.packstore.sizes().await.unwrap(),
        vec![2 * 4096; PACKFILE_COUNT]
    );
    let untouched_at: Vec<_> = group_entries(&store, group_index)
        .await
        .into_iter()
        .filter(|entry| entry.data.pack_file % 2 == 0)
        .map(|entry| (entry.address, entry.data.pack_file, entry.data.pack_offset))
        .collect();

    assert!(compact_group(&store, group_index).await);

    let entries = group_entries(&store, group_index).await;
    for location in &untouched_at {
        assert!(
            entries.iter().any(|entry| {
                (entry.address, entry.data.pack_file, entry.data.pack_offset) == *location
            }),
            "a payload of a fully referenced packfile stays where it is"
        );
    }
    let local: Vec<_> = layout
        .iter()
        .flatten()
        .zip(&addresses)
        .filter(|((_, durable), _)| !durable)
        .collect();
    assert_eq!(
        group.packstore.total_size().await.unwrap(),
        local.len() * 4096,
        "every orphaned payload is reclaimed"
    );
    assert_payloads_read_back(
        &store,
        partition,
        local
            .into_iter()
            .map(|((payload, _), address)| (*address, (*payload).clone())),
    )
    .await;
}

/// A stop can cut eviction short, so a group compacted with a stop pending is reported
/// incomplete, which keeps the resume point at it, whatever its packfiles hold.
#[tokio::test]
async fn compaction_with_a_stop_pending_reports_incomplete() {
    let dir = lore_base::test_util::TempDir::new("is_compact_stopped_");
    let store = LocalImmutableStore::new(Some(dir.to_path_buf()), client_settings())
        .await
        .unwrap();

    let partition = Partition::from([0x24u8; 16]);
    let (group_index, payloads) = payloads_in_one_group(4);
    for (index, payload) in payloads.iter().enumerate() {
        put_payload(&store, partition, Context::default(), payload, index < 2).await;
    }

    let _stopped = GcStopRequest::raise(&store.stop_requests, false);
    assert!(!compact_group(&store, group_index).await);
}

/// A pass that ends before rewriting a packfile it closed, here because the group fell below its
/// target, opens that packfile to writes again. Both durable payloads are evicted first. Of
/// 16 KiB over a 13 KiB target, rewriting packfile 1 leaves 12 KiB. With packfile 1 then closed,
/// packfile 2 is the lowest that takes writes.
#[tokio::test]
async fn compaction_reopens_the_packfiles_it_does_not_rewrite() {
    let dir = lore_base::test_util::TempDir::new("is_compact_reopen_");
    let partition = Partition::from([0x26u8; 16]);
    let (group_index, payloads) = payloads_in_one_group(5);
    let (store, _) = store_in_packfiles(
        dir.as_ref(),
        partition,
        &[
            vec![(&payloads[0], true), (&payloads[1], false)],
            vec![(&payloads[2], true), (&payloads[3], false)],
        ],
    )
    .await;

    store
        .clone()
        .evict_group_sized(group_index, 1, store.path.clone(), true, false)
        .await;
    assert!(compact_group_toward(&store, group_index, 13 * 1024, true).await);

    let packstore = &store.group[group_index].packstore;
    packstore.stop_writes(&[1]).await.unwrap();
    let address = put_payload(&store, partition, Context::default(), &payloads[4], false).await;
    assert_eq!(
        packfile_of(&group_entries(&store, group_index).await, address),
        2,
        "packfile 2, left unrewritten, takes writes again"
    );
}

/// Passes over a group that keeps taking batches of durable and local payloads keep its number
/// of packfiles bounded: writes fill the lowest packfile, so a pass finds orphaned payloads in
/// one packfile and copies them into another it leaves alone.
#[tokio::test]
async fn repeated_compaction_keeps_the_packfile_count_bounded() {
    const BATCH: usize = 16;

    let dir = lore_base::test_util::TempDir::new("is_compact_bounded_");
    let store = LocalImmutableStore::new(Some(dir.to_path_buf()), client_settings())
        .await
        .unwrap();

    let partition = Partition::from([0x28u8; 16]);
    let (group_index, payloads) = payloads_of_size_in_one_group(10 * BATCH, 256);
    for (round, batch) in payloads.chunks(BATCH).enumerate() {
        for (index, payload) in batch.iter().enumerate() {
            put_payload(
                &store,
                partition,
                Context::default(),
                payload,
                index % 4 < 2,
            )
            .await;
        }
        assert!(compact_group(&store, group_index).await);
        let count = store.group[group_index]
            .packstore
            .sizes()
            .await
            .unwrap()
            .len();
        assert!(count <= 2, "round {round} leaves {count} packfiles");
    }
}

/// A bucket that fails to load holds none of its entries, so compaction leaves its group alone
/// rather than truncate the payloads only that bucket references. A bucket file from a newer
/// format stands in for the real one, which is set aside and then restored.
#[tokio::test]
async fn compaction_skips_a_group_with_a_bucket_that_failed_to_load() {
    use lore_storage::immutable_store::ImmutableStore;

    let dir = lore_base::test_util::TempDir::new("is_compact_unloaded_");
    let partition = Partition::from([0x2au8; 16]);
    let (group_index, payloads) = payloads_in_one_group(4);
    let mut addresses = vec![];
    {
        let store = LocalImmutableStore::new(Some(dir.to_path_buf()), client_settings())
            .await
            .unwrap();
        for (index, payload) in payloads.iter().enumerate() {
            addresses
                .push(put_payload(&store, partition, Context::default(), payload, index < 2).await);
        }
        store.clone().flush(true).await.unwrap();
    }

    let bucket_path = format_bucket_path(&dir.path().join("immutable"), group_index, 0);
    let aside = dir.path().join("bucket-aside");
    std::fs::rename(&bucket_path, &aside).unwrap();
    write_bucket_file(&bucket_path, 100);
    {
        let store = LocalImmutableStore::new(Some(dir.to_path_buf()), client_settings())
            .await
            .unwrap();
        assert!(store.deserialize_all_buckets().await.is_err());

        assert!(
            compact_group(&store, group_index).await,
            "the group is left for a later process"
        );
        assert_eq!(
            store.group[group_index].packstore.sizes().await.unwrap(),
            vec![4 * 4096],
            "the packfile is not rewritten"
        );
    }

    std::fs::rename(&aside, &bucket_path).unwrap();
    let store = LocalImmutableStore::new(Some(dir.to_path_buf()), client_settings())
        .await
        .unwrap();
    assert_payloads_read_back(&store, partition, addresses.into_iter().zip(payloads)).await;
}

/// A reopened store loads its index on demand, and eviction sees only the entries loaded.
/// Loading the whole index lets it see every entry. The target is 2 fragments per bucket, under
/// the four durable fragments.
#[tokio::test]
async fn loading_the_index_lets_eviction_see_every_entry() {
    use lore_storage::immutable_store::ImmutableStore;

    let dir = lore_base::test_util::TempDir::new("is_load_index_");
    let partition = Partition::from([0x2cu8; 16]);
    let (_group_index, payloads) = payloads_in_one_group(4);
    {
        let store = LocalImmutableStore::new(Some(dir.to_path_buf()), client_settings())
            .await
            .unwrap();
        for payload in &payloads {
            put_payload(&store, partition, Context::default(), payload, true).await;
        }
        store.clone().flush(true).await.unwrap();
    }

    let store = LocalImmutableStore::new(Some(dir.to_path_buf()), client_settings())
        .await
        .unwrap();
    assert_eq!(
        store.clone().evict(732, false, None).await.unwrap(),
        0,
        "no bucket is loaded to evict from"
    );

    store.clone().load_index().await.unwrap();
    assert!(
        store.clone().evict(732, false, None).await.unwrap() > 0,
        "the loaded bucket is evicted from"
    );
}

/// A payload that a local entry also holds is not evicted, though a durable entry holding it
/// sorts first: evicting the durable entry would free nothing.
#[tokio::test]
async fn size_eviction_leaves_a_payload_a_local_entry_holds() {
    let store = LocalImmutableStore::new(None, client_settings())
        .await
        .unwrap();

    let partition = Partition::from([0x2eu8; 16]);
    let (group_index, payloads) = payloads_in_one_group(2);
    put_payload(
        &store,
        partition,
        Context::from([0x01u8; 16]),
        &payloads[0],
        true,
    )
    .await;
    put_payload(
        &store,
        partition,
        Context::from([0x02u8; 16]),
        &payloads[0],
        false,
    )
    .await;
    put_payload(&store, partition, Context::default(), &payloads[1], false).await;

    assert_eq!(
        store
            .clone()
            .evict_group_sized(group_index, 1, None, true, false)
            .await,
        (0, 0)
    );
    assert!(
        group_entries(&store, group_index)
            .await
            .iter()
            .all(|entry| entry.data.pack_file != 0),
        "every entry keeps its payload"
    );
}

/// A server store, which protects no payload, evicts every payload of a group compacted toward
/// one byte and reclaims all of its packfile bytes.
#[tokio::test]
async fn server_compaction_reclaims_the_evicted_payloads() {
    let dir = lore_base::test_util::TempDir::new("is_compact_server_");
    let store = LocalImmutableStore::new(
        Some(dir.to_path_buf()),
        ImmutableStoreSettings {
            protect_local_fragment: false,
            ..Default::default()
        },
    )
    .await
    .unwrap();

    let partition = Partition::from([0x30u8; 16]);
    let (group_index, payloads) = payloads_in_one_group(4);
    for payload in &payloads {
        put_payload(&store, partition, Context::default(), payload, false).await;
    }

    assert!(compact_group_toward(&store, group_index, 1, false).await);
    assert_eq!(
        store.group[group_index].packstore.sizes().await.unwrap(),
        vec![0]
    );
}

/// Loading the index reports a bucket that fails to load on every call, not only the first.
#[tokio::test]
async fn loading_the_index_reports_a_failed_load_each_time() {
    use lore_storage::immutable_store::ImmutableStore;

    let dir = lore_base::test_util::TempDir::new("is_load_failed_");
    let (group_index, payloads) = payloads_in_one_group(1);
    {
        let store = LocalImmutableStore::new(Some(dir.to_path_buf()), client_settings())
            .await
            .unwrap();
        put_payload(
            &store,
            Partition::from([0x32u8; 16]),
            Context::default(),
            &payloads[0],
            false,
        )
        .await;
        store.clone().flush(true).await.unwrap();
    }
    write_bucket_file(
        &format_bucket_path(&dir.path().join("immutable"), group_index, 0),
        100,
    );

    let store = LocalImmutableStore::new(Some(dir.to_path_buf()), client_settings())
        .await
        .unwrap();
    assert!(store.clone().load_index().await.is_err());
    assert!(store.clone().load_index().await.is_err());
}

/// A packfile compaction cannot empty, because a payload in it cannot be copied, is kept, with
/// the entry still pointing into it, while the payloads copied out of it read back.
#[tokio::test]
async fn compaction_keeps_a_packfile_it_cannot_empty() {
    use lore_storage::immutable_store::ImmutableStore;

    let dir = lore_base::test_util::TempDir::new("is_compact_kept_");
    let store = LocalImmutableStore::new(Some(dir.to_path_buf()), client_settings())
        .await
        .unwrap();

    let partition = Partition::from([0x34u8; 16]);
    let (group_index, payloads) = payloads_in_one_group(4);
    let mut local = vec![];
    for (index, payload) in payloads.iter().enumerate() {
        let address = put_payload(&store, partition, Context::default(), payload, index < 2).await;
        if index >= 2 {
            local.push((address, payload.clone()));
        }
    }
    store.clone().flush(true).await.unwrap();

    let mut unreadable_hash = [0xffu8; 32];
    unreadable_hash[0] = group_index as u8;
    let unreadable = ImmutableData {
        flags: FragmentFlags::PayloadStoredLocal.bits(),
        size_payload: 64,
        size_content: 64,
        pack_offset: 1 << 20,
        pack_file: 1,
        last_access: 0,
    };
    {
        let mut bucket = store.group[group_index].bucket(0).write().await;
        let index = bucket.entry.len() as u32;
        bucket.entry.push(ImmutableStoreEntry {
            address: Address {
                hash: Hash::from(unreadable_hash),
                context: Context::default(),
            },
            partition,
            data: unreadable,
        });
        bucket.sorted_index.push(index);
    }

    assert!(compact_group(&store, group_index).await);

    assert_eq!(
        store.group[group_index].packstore.sizes().await.unwrap()[0],
        4 * 4096,
        "the packfile is not truncated"
    );
    assert!(
        group_entries(&store, group_index)
            .await
            .iter()
            .any(|entry| entry.data == unreadable),
        "the entry still points into it"
    );
    assert_payloads_read_back(&store, partition, local).await;
}

/// A payload two entries share counts once toward what a packfile references, as a rewrite
/// copies it once, so an orphan beside it is still reclaimed. Counted per entry, the shared
/// payload would make up for the orphaned durable one.
#[tokio::test]
async fn compaction_counts_a_shared_payload_once() {
    use lore_storage::immutable_store::ImmutableStore;

    let dir = lore_base::test_util::TempDir::new("is_compact_shared_");
    let store = LocalImmutableStore::new(Some(dir.to_path_buf()), client_settings())
        .await
        .unwrap();

    let partition = Partition::from([0x1cu8; 16]);
    let (group_index, payloads) = payloads_in_one_group(3);
    let shared = &payloads[0];
    let mut local = vec![];
    for context in [Context::from([0x01u8; 16]), Context::from([0x02u8; 16])] {
        let address = put_payload(&store, partition, context, shared, false).await;
        local.push((address, shared.clone()));
    }
    let address = put_payload(&store, partition, Context::default(), &payloads[1], false).await;
    local.push((address, payloads[1].clone()));
    put_payload(&store, partition, Context::default(), &payloads[2], true).await;
    store.clone().flush(true).await.unwrap();

    assert_eq!(
        store.group[group_index].packstore.sizes().await.unwrap()[0],
        3 * 4096,
        "the two entries share one payload"
    );

    assert!(compact_group(&store, group_index).await);
    assert_eq!(
        store.group[group_index]
            .packstore
            .total_size()
            .await
            .unwrap(),
        2 * 4096,
        "the orphaned durable payload is reclaimed"
    );
    assert_payloads_read_back(&store, partition, local).await;
}

/// A step that commits to work reports one begin and owes exactly one end; a call that
/// gives up before committing reports neither, and does no work from the resume point.
#[tokio::test]
async fn compaction_reports_one_end_for_every_begin() {
    use lore_storage::immutable_store::ImmutableStore;

    #[derive(Default)]
    struct CountingSink {
        begins: AtomicUsize,
        ends: AtomicUsize,
    }

    impl lore_storage::gc_event::GcEventSink for CountingSink {
        fn eviction_begin(&self, _target_fragments: u64) {}
        fn eviction_progress(&self, _evicted: u64) {}
        fn eviction_end(&self, _total_evicted: u64) {}
        fn compaction_begin(&self, _target_bytes: u64) {
            self.begins.fetch_add(1, atomic::Ordering::Relaxed);
        }
        fn compaction_progress(&self, _compacted_bytes: u64) {}
        fn compaction_end(&self, _total_compacted_bytes: u64) {
            self.ends.fetch_add(1, atomic::Ordering::Relaxed);
        }
    }

    let dir = lore_base::test_util::TempDir::new("is_sink_pairing_");
    let store =
        LocalImmutableStore::new(Some(dir.to_path_buf()), ImmutableStoreSettings::default())
            .await
            .unwrap();

    // Content, so the pass finds itself above the limit and announces a begin.
    let payload = vec![0x5au8; 4096];
    store
        .clone()
        .put(
            Partition::from([0x0du8; 16]),
            Address {
                hash: lore_storage::hash::hash_slice(&payload),
                context: Context::default(),
            },
            Fragment {
                flags: 0,
                size_payload: payload.len() as u32,
                size_content: payload.len() as u64,
            },
            Some(Bytes::from(payload)),
            false,
        )
        .await
        .unwrap();
    store.clone().flush(true).await.unwrap();

    let sink = Arc::new(CountingSink::default());

    let resume = store
        .clone()
        .compact_packfiles(1, None, false, Some(sink.clone()))
        .await
        .unwrap()
        .expect("a step over a 256 group store leaves groups to come");

    assert_eq!(
        sink.begins.load(atomic::Ordering::Relaxed),
        1,
        "a committed step announces exactly one begin"
    );
    assert_eq!(
        sink.ends.load(atomic::Ordering::Relaxed),
        1,
        "a committed step owes an end for the begin it reported"
    );

    let _stopped = GcStopRequest::raise(&store.stop_requests, false);

    assert_eq!(
        store
            .clone()
            .compact_packfiles(1, Some(resume), false, Some(sink.clone()))
            .await
            .unwrap(),
        None,
        "a stopped call must not take another round from the resume point"
    );
    assert_eq!(
        sink.begins.load(atomic::Ordering::Relaxed),
        1,
        "a call that gives up before committing must not announce a begin"
    );
    assert_eq!(
        sink.ends.load(atomic::Ordering::Relaxed),
        1,
        "a call that gives up before committing reports neither begin nor end"
    );
}

/// A stop asks the passes in flight to give up; it is not a switch that stays off. The
/// store is shared by path, so a caller quiescing it leaves the others collecting.
#[tokio::test]
async fn a_stop_lifts_once_it_has_drained() {
    use lore_storage::immutable_store::ImmutableStore;

    let store = LocalImmutableStore::new(None, ImmutableStoreSettings::default())
        .await
        .unwrap();

    store.clone().stop_gc(false).await;

    assert!(
        !store.gc_stop_requested(),
        "a stop that is not terminating must lift so a shared store keeps collecting"
    );
}

/// Two callers overlap whenever handles closing on one path race each other or a
/// shutdown. The first to drain must not lift the second's request, or the second waits
/// out a whole pass instead of the pass giving up at its next packfile.
#[tokio::test]
async fn a_stop_stays_raised_while_another_is_outstanding() {
    use lore_storage::immutable_store::ImmutableStore;

    let store = LocalImmutableStore::new(None, ImmutableStoreSettings::default())
        .await
        .unwrap();

    {
        let _outstanding = GcStopRequest::raise(&store.stop_requests, false);
        store.clone().stop_gc(false).await;
        assert!(
            store.gc_stop_requested(),
            "a drain that completes must leave another caller's request raised"
        );
    }

    assert!(
        !store.gc_stop_requested(),
        "the outstanding request going away leaves the store collecting again"
    );
}

/// A last-access stamp far enough in the past that a resolve marks the bucket for rewrite.
const STALE_ACCESS: u64 = 1;

/// Answer the group and bucket index of the first bucket in `store` holding an entry. The
/// hash decides where a put lands, so a test that has to reach the entry it stored searches
/// rather than derives.
async fn populated_bucket(store: &Arc<LocalImmutableStore>) -> (usize, usize) {
    for (group_index, group) in store.group.iter().enumerate() {
        for (bucket_index, cell) in group.bucket.iter().enumerate() {
            if let Some(bucket) = cell.get()
                && !bucket.read().await.entry.is_empty()
            {
                return (group_index, bucket_index);
            }
        }
    }
    panic!("a put must populate a bucket");
}

/// Store one fragment in `store`, set its last-access stamp to `stamp`, and clear the dirty
/// flag of the bucket it landed in. Returns that bucket and the address naming the entry.
async fn backdated_fragment(
    store: &Arc<LocalImmutableStore>,
    stamp: u64,
) -> ((usize, usize), Partition, Address) {
    use lore_storage::immutable_store::ImmutableStore;

    let partition = Partition::from([0x11u8; 16]);
    let payload = vec![0x22u8; 128];
    let address = Address {
        hash: lore_storage::hash::hash_slice(&payload),
        context: Context::from([0x33u8; 16]),
    };
    let fragment = Fragment {
        flags: 0,
        size_payload: payload.len() as u32,
        size_content: payload.len() as u64,
    };
    store
        .clone()
        .put(
            partition,
            address,
            fragment,
            Some(Bytes::from(payload)),
            false,
        )
        .await
        .unwrap();

    let (group_index, bucket_index) = populated_bucket(store).await;
    let group = &store.group[group_index];
    group.bucket(bucket_index).write().await.entry[0]
        .data
        .last_access = stamp;
    group.dirty[bucket_index].store(false, atomic::Ordering::Relaxed);

    ((group_index, bucket_index), partition, address)
}

/// Resolve one backdated fragment in an in-memory store. Returns the stamp its entry carries
/// afterward and whether the resolve marked the bucket for rewrite.
async fn resolve_one_fragment(atime: bool, stamp: u64) -> (u64, bool) {
    use lore_storage::immutable_store::ImmutableStore;

    let store = LocalImmutableStore::new(
        None,
        ImmutableStoreSettings {
            atime,
            ..Default::default()
        },
    )
    .await
    .unwrap();

    let ((group_index, bucket_index), partition, address) = backdated_fragment(&store, stamp).await;

    let mut results = [StoreMatchResult::default(); 1];
    store
        .clone()
        .query(partition, &[address], &mut results)
        .await
        .unwrap();

    let group = &store.group[group_index];
    (
        group.bucket(bucket_index).read().await.entry[0]
            .data
            .last_access,
        group.dirty[bucket_index].load(atomic::Ordering::Relaxed),
    )
}

/// Eviction and compaction rank entries by last access, so a resolve moves the stamp to now
/// whatever its age — ranking by write time reclaims a fragment every command reads ahead of
/// one nothing has touched since it landed. A move this small rides along with whatever
/// writes the bucket next rather than rewriting it on its own.
#[tokio::test]
async fn a_small_move_advances_the_stamp_without_dirtying_the_bucket() {
    let recent = LocalImmutableStore::last_access().saturating_sub(10);

    let (last_access, dirty) = resolve_one_fragment(true, recent).await;

    assert!(last_access > recent, "a resolve always advances the stamp");
    assert!(!dirty, "a small move must not schedule a rewrite");
}

/// A stamp that moved past the window is worth a bucket rewrite of its own.
#[tokio::test]
async fn a_stale_stamp_dirties_the_bucket_holding_it() {
    let (_last_access, dirty) = resolve_one_fragment(true, STALE_ACCESS).await;

    assert!(dirty, "a stamp this far behind has to reach disk");
}

/// A store that never reclaims records no access, so a resolve neither moves the stamp nor
/// dirties the bucket holding it.
#[tokio::test]
async fn a_resolve_records_nothing_without_atime() {
    let (last_access, dirty) = resolve_one_fragment(false, STALE_ACCESS).await;

    assert_eq!(last_access, STALE_ACCESS);
    assert!(!dirty);
}

/// Ranking by access is only worth anything if a stamp outlives the process that made it, so
/// this reads the bucket back off disk rather than out of the store that wrote it.
#[tokio::test]
async fn a_stamp_reaches_the_bucket_file() {
    use lore_storage::immutable_store::ImmutableStore;

    let dir = lore_base::test_util::TempDir::new("is_atime_persist_");
    let store =
        LocalImmutableStore::new(Some(dir.to_path_buf()), ImmutableStoreSettings::default())
            .await
            .unwrap();

    let ((group_index, bucket_index), partition, address) =
        backdated_fragment(&store, STALE_ACCESS).await;

    let mut results = [StoreMatchResult::default(); 1];
    store
        .clone()
        .query(partition, &[address], &mut results)
        .await
        .unwrap();
    store.clone().flush(true).await.unwrap();

    let root = store.path.clone().expect("a disk-backed store has a path");
    let entry = ImmutableStoreBucket::deserialize_files(format_bucket_path(
        &root,
        group_index,
        bucket_index,
    ))
    .await
    .unwrap()
    .entry;

    assert_eq!(entry.len(), 1, "the stamp must have dirtied the bucket");
    assert!(
        entry[0].data.last_access > STALE_ACCESS,
        "the stamp a resolve made must survive the flush"
    );
}

fn payload_data(pack_file: u32, encoding: u32, storage: u32) -> ImmutableData {
    ImmutableData {
        flags: encoding | storage,
        size_payload: if pack_file == 0 { 0 } else { 100 },
        size_content: 256,
        pack_offset: if pack_file == 0 { 0 } else { 200 },
        pack_file,
        last_access: 0,
    }
}

#[test]
fn merge_from_copy_source_adopts_payload_and_encoding() {
    // Target had its own uncompressed payload. Source has the same content stored
    // compressed in a different pack file. After merge, target adopts source's pack
    // pointer and the encoding flag that describes those bytes — keeping target's
    // pre-existing flags would mis-describe the new payload.
    let mut target = payload_data(1, 0, 0);
    let source = payload_data(2, FragmentFlags::PayloadCompressedZstd.bits(), 0);

    target.merge_from_copy_source(source, false);

    assert_eq!(target.pack_file, 2, "target adopts source's pack_file");
    assert_eq!(target.pack_offset, 200);
    assert_eq!(target.size_payload, 100);
    assert_ne!(
        target.flags & FragmentFlags::PayloadCompressedZstd.bits(),
        0,
        "encoding flag must follow the adopted payload",
    );
    assert_ne!(
        target.flags & FragmentFlags::PayloadStoredLocal.bits(),
        0,
        "adopted bytes are locally available",
    );
}

#[test]
fn merge_from_copy_source_preserves_target_durable() {
    // Target had PayloadStoredDurable from a prior remote round-trip on the destination
    // tuple. A subsequent local copy must not unset that bit.
    let mut target = payload_data(0, 0, FragmentFlags::PayloadStoredDurable.bits());
    let source = payload_data(2, 0, FragmentFlags::PayloadStoredDurable.bits());

    target.merge_from_copy_source(source, false);

    assert_ne!(
        target.flags & FragmentFlags::PayloadStoredDurable.bits(),
        0,
        "target's prior Durable must be preserved",
    );
}

#[test]
fn merge_from_copy_source_durable_only_from_caller() {
    // Source carries Durable; target had none. With `durable=false`, source's Durable
    // must NOT propagate. With `durable=true`, the caller's intent sets the bit.
    let source = payload_data(2, 0, FragmentFlags::PayloadStoredDurable.bits());

    let mut local_only = payload_data(0, 0, 0);
    local_only.merge_from_copy_source(source, false);
    assert_eq!(
        local_only.flags & FragmentFlags::PayloadStoredDurable.bits(),
        0,
        "Durable must not propagate from source on a local-only copy",
    );

    let mut remote_confirmed = payload_data(0, 0, 0);
    remote_confirmed.merge_from_copy_source(source, true);
    assert_ne!(
        remote_confirmed.flags & FragmentFlags::PayloadStoredDurable.bits(),
        0,
        "caller's `durable=true` sets the bit",
    );
}

#[tokio::test]
async fn copy_adopts_source_payload_and_decompresses_through_target_partition() {
    // Prime target with uncompressed payload at one address, prime source with the same
    // hash but compressed payload, then copy source → target. The target entry must
    // adopt source's payload pointer along with the matching encoding flag so a read
    // against the target partition decompresses correctly and returns the original bytes.
    use std::sync::atomic::Ordering;

    use lore_storage::compress::COMPRESSION_MODE;
    use lore_storage::compress::CompressionMode;
    use lore_storage::options::ReadOptions;
    use lore_storage::options::WriteOptions;
    use lore_storage::read::read;
    use lore_storage::write::StoreResult;
    use lore_storage::write::write_content;

    let store: Arc<dyn lore_storage::immutable_store::ImmutableStore> = create(
        None::<&Path>,
        ImmutableStoreCreateOptions::none(),
        false,
        ImmutableStoreSettings::default(),
    )
    .await
    .unwrap();

    let target_partition = Partition::from([0x01u8; 16]);
    let source_partition = Partition::from([0x02u8; 16]);
    let context = Context::from([0x03u8; 16]);
    // Highly compressible content so compression actually triggers when enabled.
    let payload: Vec<u8> = vec![0xABu8; 4096];

    // Prime target (uncompressed).
    let prev_mode = COMPRESSION_MODE.swap(CompressionMode::NoCompression as u32, Ordering::AcqRel);
    let StoreResult {
        address: target_address,
        ..
    } = write_content(
        store.clone(),
        target_partition,
        context,
        Bytes::from(payload.clone()),
        WriteOptions::default(),
        None,
        lore_storage::write_tracker::WriteContext::none(),
        None,
    )
    .await
    .unwrap();

    // Prime source (compressed).
    COMPRESSION_MODE.store(CompressionMode::Zstd as u32, Ordering::Release);
    let StoreResult {
        address: source_address,
        ..
    } = write_content(
        store.clone(),
        source_partition,
        context,
        Bytes::from(payload.clone()),
        WriteOptions::default(),
        None,
        lore_storage::write_tracker::WriteContext::none(),
        None,
    )
    .await
    .unwrap();
    // Restore mode for any other tests sharing this process.
    COMPRESSION_MODE.store(prev_mode, Ordering::Release);

    assert_eq!(target_address, source_address, "same content → same hash");

    // Copy source → target with durable=false (pure local). Pass the source context as the
    // destination context so the address tuple is preserved (cross-partition copy with the
    // hash + context invariant the original test relied on).
    store
        .clone()
        .copy(
            source_partition,
            source_address,
            target_partition,
            source_address.context,
            CopyBehavior {
                durable: false,
                do_not_replicate: false,
            },
        )
        .await
        .unwrap();

    // Read from target partition: payload bytes must round-trip identically. The helper
    // must have adopted source's pack pointer AND encoding flag together — if encoding
    // and bytes were ever desynchronized, decompression in `read` would fail or return
    // garbage.
    let (_fragment, bytes) = read(
        store.clone(),
        target_partition,
        target_address,
        None,
        ReadOptions::default(),
        None,
    )
    .await
    .unwrap();
    assert_eq!(bytes.as_ref(), payload.as_slice());
}

#[tokio::test]
async fn copy_same_partition_new_context_adopts_payload_without_transfer() {
    // Same partition, different context — the in-partition deduplication path. The destination
    // entry must adopt the source's payload pointer (no payload transfer) and the read against
    // the new `(partition, hash, target_context)` tuple must return the same bytes that were
    // originally written under the source context.
    use lore_storage::options::ReadOptions;
    use lore_storage::options::WriteOptions;
    use lore_storage::read::read;
    use lore_storage::write::StoreResult;
    use lore_storage::write::write_content;

    let store: Arc<dyn lore_storage::immutable_store::ImmutableStore> = create(
        None::<&Path>,
        ImmutableStoreCreateOptions::none(),
        false,
        ImmutableStoreSettings::default(),
    )
    .await
    .unwrap();

    let partition = Partition::from([0xA1u8; 16]);
    let source_context = Context::from([0xB1u8; 16]);
    let target_context = Context::from([0xB2u8; 16]);
    let payload: Vec<u8> = b"in-partition new-context dedup payload".to_vec();

    // Seed the source tuple `(partition, hash, source_context)`.
    let StoreResult {
        address: source_address,
        ..
    } = write_content(
        store.clone(),
        partition,
        source_context,
        Bytes::from(payload.clone()),
        WriteOptions::default(),
        None,
        lore_storage::write_tracker::WriteContext::none(),
        None,
    )
    .await
    .unwrap();
    assert_eq!(source_address.context, source_context);

    // Copy within the same partition, retagging the destination with `target_context`. The
    // store's `copy` is the only call we make — there must be no payload transfer; the
    // destination tuple gets its own entry that points at the source's payload data.
    store
        .clone()
        .copy(
            partition,
            source_address,
            partition,
            target_context,
            CopyBehavior {
                durable: false,
                do_not_replicate: false,
            },
        )
        .await
        .unwrap();

    // The destination address shares the source's hash but takes the target context.
    let destination_address = Address {
        hash: source_address.hash,
        context: target_context,
    };

    let (_fragment, bytes) = read(
        store.clone(),
        partition,
        destination_address,
        None,
        ReadOptions::default(),
        None,
    )
    .await
    .unwrap();
    assert_eq!(bytes.as_ref(), payload.as_slice());

    // Source tuple must remain readable independently — copy creates a new entry, it does
    // not consume or repoint the source.
    let (_fragment, bytes) = read(
        store.clone(),
        partition,
        source_address,
        None,
        ReadOptions::default(),
        None,
    )
    .await
    .unwrap();
    assert_eq!(bytes.as_ref(), payload.as_slice());
}

/// The local store is the reference implementation of the store contract: it resolves an
/// address in a single bucket pass, so it can establish every level at no extra cost and has
/// no reason to under-report any of them.
#[tokio::test]
async fn satisfies_the_immutable_store_contract() {
    let dir = lore_base::test_util::TempDir::new("is_conformance_");
    let store = LocalImmutableStore::new(
        Some(std::path::PathBuf::from(dir.as_ref())),
        ImmutableStoreSettings::default(),
    )
    .await
    .expect("create store");

    lore_storage::conformance::verify_immutable_store(
        store,
        lore_storage::conformance::Capabilities::new("LocalImmutableStore").stores_metadata_only(),
    )
    .await;
}

/// Healing a payload that no longer verifies has to leave the store answering absence, so
/// that the content is offered again.
///
/// Clearing the payload pointer alone would not: an entry still answers a full match, which
/// tells a peer the store holds content it can no longer serve, and a peer told that stops
/// offering it. The association the caller asked about and its siblings under other contexts
/// go together, since they name the one payload that failed.
///
/// A payload is shared by every association that deduplicated onto it, so one proven bad is
/// bad in whichever partition names it, and all of them go. Leaving one behind leaves that
/// partition being told the content is here until it happens to verify for itself.
#[tokio::test]
async fn healing_a_corrupt_payload_drops_the_associations_that_named_it() {
    use lore_storage::immutable_store::ImmutableStore;

    let dir = lore_base::test_util::TempDir::new("is_heal_drop_");
    let store = LocalImmutableStore::new(
        Some(std::path::PathBuf::from(dir.as_ref())),
        ImmutableStoreSettings {
            isolate_partitions: true,
            ..Default::default()
        },
    )
    .await
    .expect("create store");

    let payload = Bytes::from_static(b"a payload that will stop verifying");
    let hash = lore_storage::hash::hash_slice(payload.as_ref());
    let fragment = Fragment {
        flags: FragmentFlags::PayloadStoredLocal.bits(),
        size_payload: payload.len() as u32,
        size_content: payload.len() as u64,
    };

    let partition = Partition::from([0x61u8; 16]);
    let address = Address {
        hash,
        context: Context::from([0x62u8; 16]),
    };
    let sibling = Address {
        hash,
        context: Context::from([0x63u8; 16]),
    };
    let neighbour = Partition::from([0x64u8; 16]);

    for (in_partition, at) in [
        (partition, address),
        (partition, sibling),
        (neighbour, address),
    ] {
        store
            .clone()
            .put(in_partition, at, fragment, Some(payload.clone()), false)
            .await
            .expect("put the payload");
    }

    // Corrupt the stored bytes in place, which is what verification is there to notice.
    let group_index = hash.data()[0] as usize;
    let found = store
        .clone()
        .find(partition, address)
        .await
        .expect("the entry is there to corrupt");
    store.group[group_index]
        .packstore
        .obliterate(
            found.data.pack_file,
            found.data.pack_offset,
            found.data.size_payload,
        )
        .await
        .expect("overwrite the payload");

    let result = store
        .clone()
        .verify_fragment(address, partition, StoreMatch::MatchFull, true)
        .await
        .expect("verify answers");
    assert!(
        result.healed,
        "a payload that does not verify was not healed"
    );

    for gone in [address, sibling] {
        let resolved = lore_storage::immutable_store::query_one(
            &(store.clone() as Arc<dyn ImmutableStore>),
            partition,
            gone,
        )
        .await
        .expect("query answers");
        assert_eq!(
            resolved.match_made,
            StoreMatch::MatchNone,
            "a healed association still answers a match, so nothing will offer {gone} again"
        );
    }

    let resolved = lore_storage::immutable_store::query_one(
        &(store.clone() as Arc<dyn ImmutableStore>),
        neighbour,
        address,
    )
    .await
    .expect("query answers");
    assert_eq!(
        resolved.match_made,
        StoreMatch::MatchNone,
        "another partition was left naming the payload this store proved it cannot serve"
    );
}

/// An entry that names no payload is the wedged state itself, and verifying has to be able
/// to undo it.
///
/// Nothing failed here - there is no payload to fail - so this is the state left behind by
/// whatever dropped one, and left alone it is permanent: the entry answers a full match, the
/// store is asked for bytes it does not have, and the peer that holds them is told not to
/// send them. Reporting it as verified is what makes it permanent, so it counts as a failure
/// and heals the same way.
#[tokio::test]
async fn verifying_an_entry_with_no_payload_reports_it_and_heals_the_wedge() {
    use lore_storage::immutable_store::ImmutableStore;

    let dir = lore_base::test_util::TempDir::new("is_heal_wedge_");
    let store = LocalImmutableStore::new(
        Some(std::path::PathBuf::from(dir.as_ref())),
        ImmutableStoreSettings {
            isolate_partitions: true,
            ..Default::default()
        },
    )
    .await
    .expect("create store");

    let payload = Bytes::from_static(b"bytes this store will never hold");
    let hash = lore_storage::hash::hash_slice(payload.as_ref());
    let partition = Partition::from([0x71u8; 16]);
    let address = Address {
        hash,
        context: Context::from([0x72u8; 16]),
    };

    // A header with no payload, which is what an entry left behind by a drop looks like.
    store
        .clone()
        .put(
            partition,
            address,
            Fragment {
                flags: 0,
                size_payload: payload.len() as u32,
                size_content: payload.len() as u64,
            },
            None,
            false,
        )
        .await
        .expect("put the header alone");

    let before = lore_storage::immutable_store::query_one(
        &(store.clone() as Arc<dyn ImmutableStore>),
        partition,
        address,
    )
    .await
    .expect("query answers");
    assert_eq!(
        before.match_made,
        StoreMatch::MatchFull,
        "the wedge this heals is an entry with no payload answering a full match"
    );

    let reported = store
        .clone()
        .verify_fragment(address, partition, StoreMatch::MatchFull, false)
        .await
        .expect("verify answers");
    assert!(
        reported.verification_result.is_err(),
        "an entry the store cannot serve was reported as verified"
    );
    assert!(!reported.healed, "verify healed without being asked to");

    let healed = store
        .clone()
        .verify_fragment(address, partition, StoreMatch::MatchFull, true)
        .await
        .expect("verify answers");
    assert!(healed.healed, "the wedged entry was not healed");

    let after = lore_storage::immutable_store::query_one(
        &(store.clone() as Arc<dyn ImmutableStore>),
        partition,
        address,
    )
    .await
    .expect("query answers");
    assert_eq!(
        after.match_made,
        StoreMatch::MatchNone,
        "the entry survived healing, so the address stays wedged"
    );
}

/// A flush without syncing replaces a bucket's file rather than rewriting it, so that a process
/// stopped during the write leaves the file it last wrote, as a hard link to that file shows.
#[tokio::test]
async fn an_unsynced_flush_replaces_the_bucket_file() {
    use lore_storage::immutable_store::ImmutableStore;

    let dir = lore_base::test_util::TempDir::new("is_unsynced_replace_");
    let store = LocalImmutableStore::new(Some(dir.to_path_buf()), client_settings())
        .await
        .unwrap();
    let partition = Partition::from([0x5au8; 16]);
    let (group_index, payloads) = payloads_in_one_group(2);
    let bucket_path = format_bucket_path(&dir.join("immutable"), group_index, 0);
    put_payload(&store, partition, Context::default(), &payloads[0], false).await;
    store.clone().flush(false).await.unwrap();
    let written = std::fs::read(&bucket_path).unwrap();
    let link = dir.join("last_written");
    std::fs::hard_link(&bucket_path, &link).unwrap();

    put_payload(&store, partition, Context::default(), &payloads[1], false).await;
    store.clone().flush(false).await.unwrap();

    assert_eq!(
        std::fs::read(&link).unwrap(),
        written,
        "the file last written is left whole"
    );
    assert_ne!(
        std::fs::read(&bucket_path).unwrap(),
        written,
        "the bucket's file holds the new write"
    );
}

/// Healing has to survive a reopen, which means the emptied bucket reaching disk.
///
/// Dropping the last entry a bucket holds leaves nothing to write, and a flush that treats
/// that as nothing to do leaves the file the bucket was last written to in place. The store
/// then reports the association again the next time it opens, healed in memory only.
#[tokio::test]
async fn healing_the_last_entry_in_a_bucket_survives_reopening_the_store() {
    use lore_storage::immutable_store::ImmutableStore;

    let dir = lore_base::test_util::TempDir::new("is_heal_reopen_");
    let path = std::path::PathBuf::from(dir.as_ref());

    let payload = Bytes::from_static(b"a payload that will not survive its own bucket");
    let hash = lore_storage::hash::hash_slice(payload.as_ref());
    let partition = Partition::from([0x91u8; 16]);
    let address = Address {
        hash,
        context: Context::from([0x92u8; 16]),
    };

    {
        let store = LocalImmutableStore::new(
            Some(path.clone()),
            ImmutableStoreSettings {
                isolate_partitions: true,
                ..Default::default()
            },
        )
        .await
        .expect("create store");

        store
            .clone()
            .put(
                partition,
                address,
                Fragment {
                    flags: FragmentFlags::PayloadStoredLocal.bits(),
                    size_payload: payload.len() as u32,
                    size_content: payload.len() as u64,
                },
                Some(payload.clone()),
                false,
            )
            .await
            .expect("put the payload");

        // On disk before anything goes wrong, so the file healing has to account for exists.
        store.clone().flush(true).await.expect("flush the bucket");

        let group_index = hash.data()[0] as usize;
        let found = store
            .clone()
            .find(partition, address)
            .await
            .expect("the entry is there to corrupt");
        store.group[group_index]
            .packstore
            .obliterate(
                found.data.pack_file,
                found.data.pack_offset,
                found.data.size_payload,
            )
            .await
            .expect("overwrite the payload");

        let healed = store
            .clone()
            .verify_fragment(address, partition, StoreMatch::MatchFull, true)
            .await
            .expect("verify answers");
        assert!(healed.healed, "the corrupt payload was not healed");

        store.clone().flush(true).await.expect("flush the removal");
    }

    let reopened = LocalImmutableStore::new(
        Some(path),
        ImmutableStoreSettings {
            isolate_partitions: true,
            ..Default::default()
        },
    )
    .await
    .expect("reopen store");

    let resolved = lore_storage::immutable_store::query_one(
        &(reopened as Arc<dyn ImmutableStore>),
        partition,
        address,
    )
    .await
    .expect("query answers");
    assert_eq!(
        resolved.match_made,
        StoreMatch::MatchNone,
        "reopening the store loaded back the association healing removed"
    );
}

/// An obliterated fragment is meant to hold no payload, so verifying one reports nothing
/// wrong and heals nothing.
///
/// A tombstone is indistinguishable from a wedged entry by its pack file alone - both name
/// none - so what separates them is the flag. Reporting a deletion as corruption would put
/// an operator onto a fault that is not there, and healing one would flush the bucket and
/// claim a repair having changed nothing.
#[tokio::test]
async fn verifying_an_obliterated_fragment_reports_nothing_and_heals_nothing() {
    use lore_storage::immutable_store::ImmutableStore;

    let dir = lore_base::test_util::TempDir::new("is_heal_tombstone_");
    let store = LocalImmutableStore::new(
        Some(std::path::PathBuf::from(dir.as_ref())),
        ImmutableStoreSettings {
            isolate_partitions: true,
            ..Default::default()
        },
    )
    .await
    .expect("create store");

    let payload = Bytes::from_static(b"content that is about to be obliterated");
    let partition = Partition::from([0x81u8; 16]);
    let address = Address {
        hash: lore_storage::hash::hash_slice(payload.as_ref()),
        context: Context::from([0x82u8; 16]),
    };

    store
        .clone()
        .put(
            partition,
            address,
            Fragment {
                flags: FragmentFlags::PayloadStoredLocal.bits(),
                size_payload: payload.len() as u32,
                size_content: payload.len() as u64,
            },
            Some(payload),
            false,
        )
        .await
        .expect("put the payload");

    store
        .clone()
        .obliterate(
            partition,
            address,
            Arc::new(lore_storage::store_types::StoreObliterateStats::default()),
        )
        .await
        .expect("obliterate the fragment");

    let tombstone = store
        .clone()
        .find(partition, address)
        .await
        .expect("the tombstone is in the index");
    assert_eq!(tombstone.matching, StoreMatch::MatchFull);
    assert_ne!(
        tombstone.data.flags & FragmentFlags::PayloadObliterated.bits(),
        0,
        "obliterate did not leave a tombstone to verify against"
    );

    let reported = store
        .clone()
        .verify_fragment(address, partition, StoreMatch::MatchFull, false)
        .await
        .expect("verify answers");
    assert!(
        reported.verification_result.is_ok(),
        "an intentional deletion was reported as a fault"
    );

    let healed = store
        .clone()
        .verify_fragment(address, partition, StoreMatch::MatchFull, true)
        .await
        .expect("verify answers");
    assert!(
        !healed.healed,
        "healing claimed a repair on a fragment that is meant to hold no payload"
    );

    let after = store
        .clone()
        .find(partition, address)
        .await
        .expect("the tombstone is still in the index");
    assert_eq!(
        after.matching,
        StoreMatch::MatchFull,
        "healing took the tombstone with it"
    );
    assert_ne!(
        after.data.flags & FragmentFlags::PayloadObliterated.bits(),
        0,
        "healing cleared the tombstone's flag"
    );
}

/// A store that isolates partitions reports further than it reads, and this is the only
/// implementation of that split.
///
/// A sibling context in the same partition is a partition match, which `query` reports so a
/// caller can duplicate the association with a copy, and which `get` refuses so that nothing
/// crossing a wire without its level is mistaken for an association of the caller's own. The
/// battery bounds the reported level from above and cannot assert this, because a store that
/// resolves associations alone is entitled to answer `MatchNone` here instead.
#[tokio::test]
async fn an_isolating_store_reports_further_than_it_reads() {
    use lore_storage::immutable_store::ImmutableStore;

    let dir = lore_base::test_util::TempDir::new("is_scope_split_");
    let store = LocalImmutableStore::new(
        Some(std::path::PathBuf::from(dir.as_ref())),
        ImmutableStoreSettings {
            isolate_partitions: true,
            ..Default::default()
        },
    )
    .await
    .expect("create store");

    let partition = Partition::from([0x51u8; 16]);
    let payload = Bytes::from_static(b"one hash, two contexts, one partition");
    let stored = Address {
        hash: lore_storage::hash::hash_slice(payload.as_ref()),
        context: Context::from([0x52u8; 16]),
    };
    let fragment = Fragment {
        flags: FragmentFlags::PayloadStoredLocal.bits(),
        size_payload: payload.len() as u32,
        size_content: payload.len() as u64,
    };

    store
        .clone()
        .put(partition, stored, fragment, Some(payload), false)
        .await
        .expect("put under the storing context");

    let sibling = Address {
        hash: stored.hash,
        context: Context::from([0x53u8; 16]),
    };

    let resolved = lore_storage::immutable_store::query_one(
        &(store.clone() as Arc<dyn ImmutableStore>),
        partition,
        sibling,
    )
    .await
    .expect("query a sibling context");
    assert_eq!(
        resolved.match_made,
        StoreMatch::MatchPartition,
        "an isolating store must still report the partition match a copy would act on"
    );
    assert_eq!(resolved.partition, partition);

    assert!(
        store
            .clone()
            .get_metadata(partition, sibling)
            .await
            .expect("get_metadata answers rather than failing")
            .match_made
            == StoreMatch::MatchNone,
        "an isolating store described an association it does not hold"
    );
    assert!(
        store.clone().get(partition, sibling).await.is_err(),
        "an isolating store served a sibling context's payload"
    );
}

/// An entry stored without its payload holds nothing locally, whatever flags it was stored with:
/// `get_metadata` reports no `PayloadStoredLocal`, as `query` reports no `stored_local`.
#[tokio::test]
async fn an_entry_stored_without_its_payload_is_not_reported_local() {
    use lore_storage::immutable_store::ImmutableStore;

    let store = LocalImmutableStore::new(None, ImmutableStoreSettings::default())
        .await
        .expect("an in-memory store");
    let partition = Partition::from([0x71u8; 16]);
    let payload = Bytes::from_static(b"the payload the entry is stored without");
    let address = Address {
        hash: hash::hash_slice(&payload),
        context: Context::from([0x71u8; 16]),
    };
    let fragment = Fragment {
        flags: FragmentFlags::PayloadStoredLocal.bits(),
        size_payload: payload.len() as u32,
        size_content: payload.len() as u64,
    };
    store
        .clone()
        .put(partition, address, fragment, None, false)
        .await
        .expect("the entry is stored without its payload");

    let metadata = store
        .clone()
        .get_metadata(partition, address)
        .await
        .expect("get_metadata answers");
    assert_ne!(
        metadata.match_made,
        StoreMatch::MatchNone,
        "the entry is indexed"
    );
    assert_eq!(
        metadata.fragment.flags & FragmentFlags::PayloadStoredLocal,
        0,
        "get_metadata reported a payload the entry does not hold"
    );
    let resolved = lore_storage::immutable_store::query_one(
        &(store.clone() as Arc<dyn ImmutableStore>),
        partition,
        address,
    )
    .await
    .expect("query the entry");
    assert!(!resolved.stored_local);
}

/// The source forms `copy` accepts: an exact association, and any association a partition
/// holds. A caller acting on a partition match only ever has the second.
mod copy_source {
    use lore_storage::immutable_store::ImmutableStore;

    use super::*;

    type Store = Arc<dyn ImmutableStore>;

    async fn store_with(entries: &[(Partition, Context)], payload: &[u8]) -> (Store, Address) {
        let store = create(
            None::<&Path>,
            ImmutableStoreCreateOptions::none(),
            false,
            ImmutableStoreSettings::default(),
        )
        .await
        .expect("create store");
        let address = Address {
            hash: lore_storage::hash::hash_slice(payload),
            context: Context::default(),
        };
        let fragment = Fragment {
            flags: 0,
            size_payload: payload.len() as u32,
            size_content: payload.len() as u64,
        };
        for (partition, context) in entries {
            store
                .clone()
                .put(
                    *partition,
                    Address {
                        hash: address.hash,
                        context: *context,
                    },
                    fragment,
                    Some(Bytes::copy_from_slice(payload)),
                    false,
                )
                .await
                .expect("seed association");
        }
        (store, address)
    }

    async fn readable(
        store: &Store,
        partition: Partition,
        address: Address,
        payload: &[u8],
    ) -> bool {
        lore_storage::read::read(
            store.clone(),
            partition,
            address,
            None,
            lore_storage::options::ReadOptions::default(),
            None,
        )
        .await
        .is_ok_and(|(_fragment, bytes)| bytes.as_ref() == payload)
    }

    #[tokio::test]
    async fn a_zero_context_takes_any_association_in_the_partition() {
        let payload = b"zero context names any association".as_slice();
        let partition = Partition::from([0x11u8; 16]);
        let held = Context::from([0x12u8; 16]);
        let wanted = Context::from([0x13u8; 16]);
        let (store, address) = store_with(&[(partition, held)], payload).await;

        store
            .clone()
            .copy(
                partition,
                Address::zero_context_hash(address.hash),
                partition,
                wanted,
                CopyBehavior {
                    durable: false,
                    do_not_replicate: false,
                },
            )
            .await
            .expect("a partition holding the hash must answer a source naming no context");

        assert!(
            readable(
                &store,
                partition,
                Address {
                    hash: address.hash,
                    context: wanted
                },
                payload
            )
            .await
        );
    }

    #[tokio::test]
    async fn a_zero_context_crosses_partitions() {
        let payload = b"zero context across partitions".as_slice();
        let source = Partition::from([0x21u8; 16]);
        let destination = Partition::from([0x22u8; 16]);
        let held = Context::from([0x23u8; 16]);
        let wanted = Context::from([0x24u8; 16]);
        let (store, address) = store_with(&[(source, held)], payload).await;

        store
            .clone()
            .copy(
                source,
                Address::zero_context_hash(address.hash),
                destination,
                wanted,
                CopyBehavior {
                    durable: false,
                    do_not_replicate: false,
                },
            )
            .await
            .expect("copy from a source partition naming no context");

        assert!(
            readable(
                &store,
                destination,
                Address {
                    hash: address.hash,
                    context: wanted
                },
                payload
            )
            .await
        );
    }

    /// The partition is still the boundary: naming no context widens the search inside one
    /// partition, never across them.
    #[tokio::test]
    async fn a_zero_context_does_not_reach_another_partition() {
        let payload = b"zero context stays in its partition".as_slice();
        let held_in = Partition::from([0x31u8; 16]);
        let asked_of = Partition::from([0x32u8; 16]);
        let (store, address) = store_with(&[(held_in, Context::from([0x33u8; 16]))], payload).await;

        let err = store
            .clone()
            .copy(
                asked_of,
                Address::zero_context_hash(address.hash),
                Partition::from([0x34u8; 16]),
                Context::from([0x35u8; 16]),
                CopyBehavior {
                    durable: false,
                    do_not_replicate: false,
                },
            )
            .await
            .expect_err("a partition holding nothing has no association to name");
        assert!(matches!(err, StoreError::AddressNotFound(_)));
    }

    /// A named context is resolved exactly. A sibling holding the same hash is not a fallback,
    /// which is the whole difference between the two forms.
    #[tokio::test]
    async fn a_named_context_does_not_widen_to_a_sibling() {
        let payload = b"an exact source is exact".as_slice();
        let partition = Partition::from([0x41u8; 16]);
        let (store, address) =
            store_with(&[(partition, Context::from([0x42u8; 16]))], payload).await;

        let err = store
            .clone()
            .copy(
                partition,
                Address {
                    hash: address.hash,
                    context: Context::from([0x43u8; 16]),
                },
                partition,
                Context::from([0x44u8; 16]),
                CopyBehavior {
                    durable: false,
                    do_not_replicate: false,
                },
            )
            .await
            .expect_err("a context the partition does not hold must not resolve to a sibling");
        assert!(matches!(err, StoreError::AddressNotFound(_)));
    }

    /// Obliterating a fragment tree must terminate when a child shares its
    /// parent's bucket.
    ///
    /// Obliterating an address takes the write lock on the bucket that address
    /// lives in, and a child chooses its own bucket from its own hash.
    /// `tokio::sync::RwLock` is not reentrant, so a child that lands in the
    /// bucket its parent is holding used to wait on a lock the same task
    /// already owned, and the obliterate never returned. At one bucket to a
    /// group - where a client store starts - every child in the parent's group
    /// collides, which is one child in 256; a 3.4 MB file of 53 chunks hung one
    /// run in five.
    ///
    /// The collision is searched for rather than written down because both
    /// hashes are content-derived: the first byte chooses the group, and with
    /// one bucket in it the group is the bucket.
    #[tokio::test]
    async fn a_child_in_its_parent_bucket_does_not_deadlock_the_obliterate() {
        let partition = Partition::from([0x61u8; 16]);
        let context = Context::from([0x62u8; 16]);

        let (payload, leaf_hash, root_hash, references) = (0u32..)
            .find_map(|salt| {
                let payload = format!("leaf payload {salt}").into_bytes();
                let leaf_hash = lore_storage::hash::hash_slice(&payload);
                let references = vec![FragmentReference {
                    hash: leaf_hash,
                    offset_content: 0,
                }];
                let root_hash = lore_storage::hash::hash_slice(references.as_bytes());
                (root_hash.data()[0] == leaf_hash.data()[0])
                    .then_some((payload, leaf_hash, root_hash, references))
            })
            .expect("a leaf hashing into its own list's group");

        let store = create(
            None::<&Path>,
            ImmutableStoreCreateOptions::none(),
            false,
            ImmutableStoreSettings::default(),
        )
        .await
        .expect("create store");

        store
            .clone()
            .put(
                partition,
                Address {
                    hash: leaf_hash,
                    context,
                },
                Fragment {
                    flags: 0,
                    size_payload: payload.len() as u32,
                    size_content: payload.len() as u64,
                },
                Some(Bytes::copy_from_slice(&payload)),
                false,
            )
            .await
            .expect("put leaf");

        let references = Bytes::copy_from_slice(references.as_bytes());
        store
            .clone()
            .put(
                partition,
                Address {
                    hash: root_hash,
                    context,
                },
                Fragment {
                    flags: FragmentFlags::PayloadFragmented.bits(),
                    size_payload: references.len() as u32,
                    size_content: payload.len() as u64,
                },
                Some(references),
                false,
            )
            .await
            .expect("put fragment list");

        let stats = Arc::new(lore_storage::store_types::StoreObliterateStats::default());
        tokio::time::timeout(
            std::time::Duration::from_secs(30),
            store.clone().obliterate(
                partition,
                Address {
                    hash: root_hash,
                    context,
                },
                stats.clone(),
            ),
        )
        .await
        .expect("obliterating a tree whose child shares its parent's bucket must terminate")
        .expect("obliterate");

        assert_eq!(
            stats.num_fragments.load(atomic::Ordering::Relaxed),
            2,
            "both the list and the leaf it references are fragments"
        );

        let addresses = [
            Address {
                hash: root_hash,
                context,
            },
            Address {
                hash: leaf_hash,
                context,
            },
        ];
        let mut results = [StoreMatchResult::default(); 2];
        store
            .clone()
            .query(partition, &addresses, &mut results)
            .await
            .expect("query");
        for result in results {
            assert_eq!(
                result.match_made,
                StoreMatch::MatchNone,
                "an obliterated address must not resolve"
            );
        }
    }

    /// A tombstone is not a representation to adopt, so the walk passes over it and copies the
    /// live association beside it.
    #[tokio::test]
    async fn a_zero_context_skips_an_obliterated_association() {
        let payload = b"one obliterated reference, one alive".as_slice();
        let partition = Partition::from([0x51u8; 16]);
        let doomed = Context::from([0x52u8; 16]);
        let alive = Context::from([0x53u8; 16]);
        let wanted = Context::from([0x54u8; 16]);
        let (store, address) =
            store_with(&[(partition, doomed), (partition, alive)], payload).await;

        store
            .clone()
            .obliterate(
                partition,
                Address {
                    hash: address.hash,
                    context: doomed,
                },
                Arc::new(lore_storage::store_types::StoreObliterateStats::default()),
            )
            .await
            .expect("obliterate one reference");

        store
            .clone()
            .copy(
                partition,
                Address::zero_context_hash(address.hash),
                partition,
                wanted,
                CopyBehavior {
                    durable: false,
                    do_not_replicate: false,
                },
            )
            .await
            .expect("the surviving association is the one to copy from");

        assert!(
            readable(
                &store,
                partition,
                Address {
                    hash: address.hash,
                    context: wanted
                },
                payload
            )
            .await
        );
    }

    #[tokio::test]
    async fn an_obliterated_source_is_not_copied() {
        let payload = b"the only reference is obliterated".as_slice();
        let partition = Partition::from([0x61u8; 16]);
        let doomed = Context::from([0x62u8; 16]);
        let (store, address) = store_with(&[(partition, doomed)], payload).await;

        let source = Address {
            hash: address.hash,
            context: doomed,
        };
        store
            .clone()
            .obliterate(
                partition,
                source,
                Arc::new(lore_storage::store_types::StoreObliterateStats::default()),
            )
            .await
            .expect("obliterate the only reference");

        for named in [source, Address::zero_context_hash(address.hash)] {
            let err = store
                .clone()
                .copy(
                    partition,
                    named,
                    partition,
                    Context::from([0x63u8; 16]),
                    CopyBehavior {
                        durable: false,
                        do_not_replicate: false,
                    },
                )
                .await
                .expect_err("a tombstone is not an association to copy from");
            assert!(matches!(err, StoreError::AddressNotFound(_)));
        }
    }

    /// A hash the partition holds only the representation of. The walk records it as the
    /// fallback rather than passing over it, so the copy still registers the destination — as it
    /// does for an exact source that has no payload either.
    #[tokio::test]
    async fn a_zero_context_falls_back_to_an_association_without_its_payload() {
        let payload = b"representation held without its payload".as_slice();
        let partition = Partition::from([0x81u8; 16]);
        let held = Context::from([0x82u8; 16]);
        let wanted = Context::from([0x83u8; 16]);

        let store = create(
            None::<&Path>,
            ImmutableStoreCreateOptions::none(),
            false,
            ImmutableStoreSettings::default(),
        )
        .await
        .expect("create store");
        let address = Address {
            hash: lore_storage::hash::hash_slice(payload),
            context: held,
        };
        store
            .clone()
            .put(
                partition,
                address,
                Fragment {
                    flags: 0,
                    size_payload: payload.len() as u32,
                    size_content: payload.len() as u64,
                },
                None,
                false,
            )
            .await
            .expect("seed the representation alone");

        store
            .clone()
            .copy(
                partition,
                Address::zero_context_hash(address.hash),
                partition,
                wanted,
                CopyBehavior {
                    durable: false,
                    do_not_replicate: false,
                },
            )
            .await
            .expect("the representation alone is still a source");

        let resolved = lore_storage::immutable_store::query_one(
            &store,
            partition,
            Address {
                hash: address.hash,
                context: wanted,
            },
        )
        .await
        .expect("query the destination");
        assert_eq!(resolved.match_made, StoreMatch::MatchFull);
    }

    /// A `query` naming a context hands back a source `copy` resolves exactly, which is the
    /// pairing the write path relies on to avoid the wider search.
    #[tokio::test]
    async fn a_partition_match_names_a_context_copy_resolves_exactly() {
        let payload = b"query names the association copy reads".as_slice();
        let partition = Partition::from([0x71u8; 16]);
        let held = Context::from([0x72u8; 16]);
        let wanted = Context::from([0x73u8; 16]);
        let (store, address) = store_with(&[(partition, held)], payload).await;

        let resolved = lore_storage::immutable_store::query_one(
            &store,
            partition,
            Address {
                hash: address.hash,
                context: wanted,
            },
        )
        .await
        .expect("query a sibling context");
        assert_eq!(resolved.match_made, StoreMatch::MatchPartition);
        assert_eq!(resolved.context, held);

        store
            .clone()
            .copy(
                resolved.partition,
                resolved.source_address(address.hash),
                partition,
                wanted,
                CopyBehavior {
                    durable: false,
                    do_not_replicate: false,
                },
            )
            .await
            .expect("the source a match named must be one copy resolves");
    }
}

/// Set the last access of each entry of the group for which `stamp` names one.
async fn stamp_entries(
    store: &LocalImmutableStore,
    group_index: usize,
    stamp: impl Fn(&ImmutableStoreEntry) -> Option<u64>,
) {
    let group = &store.group[group_index];
    for bucket_index in 0..group.bucket_count.load(atomic::Ordering::Relaxed) {
        let Some(bucket) = group.try_bucket(bucket_index) else {
            continue;
        };
        for entry in bucket.write().await.entry.iter_mut() {
            if let Some(last_access) = stamp(entry) {
                entry.data.last_access = last_access;
            }
        }
    }
}

/// Size eviction takes the least recently accessed payloads first, each ranked by the newest
/// access among the entries holding it, and stops once the bucket is below its share: of five
/// payloads, the two accessed longest ago go, and the one an old entry shares with a recent one
/// stays.
#[tokio::test]
async fn size_eviction_takes_the_least_recently_accessed_payloads_first() {
    let store = LocalImmutableStore::new(None, client_settings())
        .await
        .unwrap();
    let partition = Partition::from([0x41u8; 16]);
    let (group_index, payloads) = payloads_in_one_group(5);
    assert_eq!(
        store.group[group_index]
            .bucket_count
            .load(atomic::Ordering::Relaxed),
        1
    );
    let mut addresses = vec![];
    for payload in &payloads {
        addresses.push(put_payload(&store, partition, Context::default(), payload, true).await);
    }
    let recent = put_payload(
        &store,
        partition,
        Context::from([0x02u8; 16]),
        &payloads[4],
        true,
    )
    .await;
    stamp_entries(&store, group_index, |entry| {
        if entry.address == recent {
            return Some(50);
        }
        addresses
            .iter()
            .position(|address| *address == entry.address)
            .map(|index| [10, 20, 30, 40, 5][index])
    })
    .await;

    assert_eq!(
        store
            .clone()
            .evict_group_sized(group_index, 14_000, None, true, false)
            .await,
        (2, 2 * 4096)
    );

    let entries = group_entries(&store, group_index).await;
    let holds_payload = |address: Address| {
        entries
            .iter()
            .any(|entry| entry.address == address && entry.data.pack_file != 0)
    };
    assert!(!holds_payload(addresses[0]) && !holds_payload(addresses[1]));
    assert!(addresses[2..].iter().all(|address| holds_payload(*address)));
    assert!(holds_payload(recent));
}

/// Size eviction takes a payload only from the entries it may take it from, so a local entry
/// keeps the payload it shares with a durable one even where an index out of order lists
/// another entry between the two.
#[tokio::test]
async fn size_eviction_keeps_a_local_payload_the_index_lists_apart() {
    let store = LocalImmutableStore::new(None, client_settings())
        .await
        .unwrap();
    let partition = Partition::from([0x42u8; 16]);
    let (group_index, payloads) = payloads_in_one_group(2);
    let durable = put_payload(
        &store,
        partition,
        Context::from([0x01u8; 16]),
        &payloads[0],
        true,
    )
    .await;
    let other = put_payload(&store, partition, Context::default(), &payloads[1], false).await;
    let local = put_payload(
        &store,
        partition,
        Context::from([0x02u8; 16]),
        &payloads[0],
        false,
    )
    .await;
    {
        let mut bucket = store.group[group_index].bucket(0).write().await;
        let position = |address: Address| {
            bucket
                .entry
                .iter()
                .position(|entry| entry.address == address)
                .unwrap() as u32
        };
        let order = [position(durable), position(other), position(local)];
        bucket.sorted_index = Default::default();
        for index in order {
            bucket.sorted_index.push(index);
        }
    }

    store
        .clone()
        .evict_group_sized(group_index, 1, None, true, false)
        .await;

    let entries = group_entries(&store, group_index).await;
    let local_entry = entries.iter().find(|entry| entry.address == local).unwrap();
    assert_ne!(
        local_entry.data.pack_file, 0,
        "the local entry keeps its payload"
    );
    assert_ne!(
        local_entry.data.flags & FragmentFlags::PayloadStoredLocal.bits(),
        0
    );
}

/// Compaction copies every payload of a packfile it can, past one it cannot copy, and goes on
/// to rewrite the next packfile: the packfile with the payload it cannot copy is kept, the
/// local payload in it moves, and the next packfile is truncated.
#[tokio::test]
async fn compaction_continues_past_a_payload_it_cannot_copy() {
    let dir = lore_base::test_util::TempDir::new("is_compact_past_failure_");
    let partition = Partition::from([0x43u8; 16]);
    let (group_index, payloads) = payloads_in_one_group(5);
    let layout = vec![
        vec![
            (&payloads[0], true),
            (&payloads[1], true),
            (&payloads[2], false),
        ],
        vec![(&payloads[3], true), (&payloads[4], false)],
    ];
    let (store, addresses) = store_in_packfiles(dir.as_ref(), partition, &layout).await;
    let mut unreadable_hash = [0u8; 32];
    unreadable_hash[0] = group_index as u8;
    {
        let mut bucket = store.group[group_index].bucket(0).write().await;
        let index = bucket.entry.len() as u32;
        bucket.entry.push(ImmutableStoreEntry {
            address: Address {
                hash: Hash::from(unreadable_hash),
                context: Context::default(),
            },
            partition,
            data: ImmutableData {
                flags: FragmentFlags::PayloadStoredLocal.bits(),
                size_payload: 7000,
                size_content: 7000,
                pack_offset: 6000,
                pack_file: 1,
                last_access: 0,
            },
        });
        bucket.sorted_index.push(index);
    }

    assert!(compact_group(&store, group_index).await);

    let sizes = store.group[group_index].packstore.sizes().await.unwrap();
    assert_eq!(sizes[0], 3 * 4096, "the packfile it cannot empty is kept");
    assert_eq!(sizes[1], 0, "the next packfile is truncated");
    let entries = group_entries(&store, group_index).await;
    assert_ne!(
        packfile_of(&entries, addresses[2]),
        1,
        "the local payload moves"
    );
    assert_payloads_read_back(
        &store,
        partition,
        [
            (addresses[2], payloads[2].clone()),
            (addresses[4], payloads[4].clone()),
        ],
    )
    .await;
}

/// Compaction sweeps every bucket of a fanned-out group: the local payloads in each read back
/// from their copies, and the durable payloads it evicts are reclaimed.
#[tokio::test]
async fn compaction_rewrites_a_packfile_spread_over_many_buckets() {
    use lore_storage::immutable_store::ImmutableStore;
    use lore_storage::local::fan_out::bucket_index_for;

    let dir = lore_base::test_util::TempDir::new("is_compact_buckets_");
    let store = LocalImmutableStore::new(
        Some(dir.to_path_buf()),
        ImmutableStoreSettings {
            initial_fan_out_level: 32,
            ..client_settings()
        },
    )
    .await
    .unwrap();
    let partition = Partition::from([0x44u8; 16]);
    let (group_index, payloads) = payloads_in_one_group(16);
    let mut local = vec![];
    for (index, payload) in payloads.iter().enumerate() {
        let address = put_payload(
            &store,
            partition,
            Context::default(),
            payload,
            index % 2 == 0,
        )
        .await;
        if index % 2 == 1 {
            local.push((address, payload.clone()));
        }
    }
    store.clone().flush(true).await.unwrap();
    store.deserialize_all_buckets().await.unwrap();
    assert_eq!(
        store.group[group_index]
            .bucket_count
            .load(atomic::Ordering::Relaxed),
        32
    );
    let mut buckets: Vec<_> = local
        .iter()
        .map(|(address, _)| bucket_index_for(&address.hash, 32))
        .collect();
    buckets.dedup();
    assert!(buckets.len() > 1, "the local payloads span buckets");

    assert!(compact_group(&store, group_index).await);

    assert_eq!(
        store.group[group_index]
            .packstore
            .total_size()
            .await
            .unwrap(),
        local.len() * 4096
    );
    assert_payloads_read_back(&store, partition, local).await;
}

/// A bucket write that fails leaves the bucket dirty for the next flush, and no temporary file.
#[tokio::test]
async fn a_failed_bucket_write_leaves_the_bucket_dirty() {
    use lore_storage::immutable_store::ImmutableStore;

    let dir = lore_base::test_util::TempDir::new("is_failed_write_");
    let store = LocalImmutableStore::new(Some(dir.to_path_buf()), client_settings())
        .await
        .unwrap();
    let partition = Partition::from([0x45u8; 16]);
    let (group_index, payloads) = payloads_in_one_group(2);
    put_payload(&store, partition, Context::default(), &payloads[0], false).await;
    store.clone().flush(false).await.unwrap();
    let bucket_path = format_bucket_path(&dir.join("immutable"), group_index, 0);
    std::fs::remove_file(&bucket_path).unwrap();
    std::fs::create_dir(&bucket_path).unwrap();
    std::fs::write(bucket_path.join("blocker"), b"x").unwrap();

    put_payload(&store, partition, Context::default(), &payloads[1], false).await;
    let _ = store.clone().flush(false).await;

    assert!(store.group[group_index].dirty[0].load(atomic::Ordering::Relaxed));
    let mut temporary = bucket_path.into_os_string();
    temporary.push(".tmp");
    assert!(!Path::new(&temporary).exists());
}

/// A fan-out commit that fails once a bucket's `.new` file is written leaves the bucket dirty,
/// so that the next flush writes it again, and its payload reads back once the store reopens.
#[tokio::test]
async fn a_failed_fan_out_commit_leaves_its_buckets_dirty() {
    use lore_storage::immutable_store::ImmutableStore;

    let dir = lore_base::test_util::TempDir::new("is_failed_commit_");
    let partition = Partition::from([0x46u8; 16]);
    let (group_index, payloads) = payloads_in_one_group(1);
    let pending = format_bucket_path(&dir.join("immutable"), group_index, 0)
        .with_file_name(lore_storage::local::fan_out::LEVEL_PENDING_FILENAME);
    let address = {
        let store = LocalImmutableStore::new(Some(dir.to_path_buf()), client_settings())
            .await
            .unwrap();
        let address = put_payload(&store, partition, Context::default(), &payloads[0], false).await;
        std::fs::create_dir_all(&pending).unwrap();
        std::fs::write(pending.join("blocker"), b"x").unwrap();

        assert!(store.clone().flush(false).await.is_err());
        assert!(store.group[group_index].dirty[0].load(atomic::Ordering::Relaxed));

        std::fs::remove_dir_all(&pending).unwrap();
        store.clone().flush(false).await.unwrap();
        address
    };

    let store = LocalImmutableStore::new(Some(dir.to_path_buf()), client_settings())
        .await
        .unwrap();
    assert_payloads_read_back(&store, partition, [(address, payloads[0].clone())]).await;
}

/// A group whose buckets are laid out for a fan-out that no commit recorded on disk is neither
/// evicted nor rewritten.
#[tokio::test]
async fn compaction_skips_a_group_whose_fan_out_is_not_committed() {
    use lore_storage::immutable_store::ImmutableStore;

    let dir = lore_base::test_util::TempDir::new("is_compact_uncommitted_");
    let store = LocalImmutableStore::new(Some(dir.to_path_buf()), client_settings())
        .await
        .unwrap();
    let partition = Partition::from([0x47u8; 16]);
    let (group_index, payloads) = payloads_in_one_group(4);
    for (index, payload) in payloads.iter().enumerate() {
        put_payload(&store, partition, Context::default(), payload, index < 2).await;
    }
    store.clone().flush(true).await.unwrap();
    store.deserialize_all_buckets().await.unwrap();
    let group = &store.group[group_index];
    group.committed_level.store(32, atomic::Ordering::Relaxed);

    assert!(compact_group(&store, group_index).await);

    assert_eq!(group.packstore.sizes().await.unwrap(), vec![4 * 4096]);
    assert!(
        group_entries(&store, group_index)
            .await
            .iter()
            .all(|entry| entry.data.pack_file != 0),
        "nothing is evicted"
    );
}

/// A packfile is kept when a bucket file cannot be written after its payloads are copied, the
/// error is returned, and the copies reach disk with the next flush, so that every payload reads
/// back once the store reopens.
#[tokio::test]
async fn compaction_keeps_a_packfile_when_a_bucket_write_fails() {
    use lore_storage::immutable_store::ImmutableStore;

    let dir = lore_base::test_util::TempDir::new("is_compact_write_fails_");
    let partition = Partition::from([0x48u8; 16]);
    let (group_index, payloads) = payloads_in_one_group(4);
    let bucket_path = format_bucket_path(&dir.join("immutable"), group_index, 0);
    let mut local = vec![];
    {
        let store = LocalImmutableStore::new(Some(dir.to_path_buf()), client_settings())
            .await
            .unwrap();
        for (index, payload) in payloads.iter().enumerate() {
            let address =
                put_payload(&store, partition, Context::default(), payload, index < 2).await;
            if index >= 2 {
                local.push((address, payload.clone()));
            }
        }
        store.clone().flush(true).await.unwrap();
        std::fs::remove_file(&bucket_path).unwrap();
        std::fs::create_dir(&bucket_path).unwrap();
        std::fs::write(bucket_path.join("blocker"), b"x").unwrap();

        let failed = try_compact_group_toward(&store, group_index, 1, true).await;
        assert!(failed.is_err(), "the failed bucket write is returned");

        assert_eq!(
            store.group[group_index].packstore.sizes().await.unwrap()[0],
            4 * 4096,
            "the packfile is not truncated"
        );
        std::fs::remove_dir_all(&bucket_path).unwrap();
        store.clone().flush(true).await.unwrap();
    }

    let store = LocalImmutableStore::new(Some(dir.to_path_buf()), client_settings())
        .await
        .unwrap();
    assert_payloads_read_back(&store, partition, local).await;
}

/// A rewrite that fails to write a bucket file ends the compaction pass and returns the error:
/// the next packfile is neither copied nor truncated.
#[tokio::test]
async fn a_failed_rewrite_ends_the_compaction_pass() {
    let dir = lore_base::test_util::TempDir::new("is_compact_failure_ends_");
    let partition = Partition::from([0x4au8; 16]);
    let (group_index, payloads) = payloads_in_one_group(4);
    let layout = vec![
        vec![(&payloads[0], true), (&payloads[1], false)],
        vec![(&payloads[2], true), (&payloads[3], false)],
    ];
    let (store, _) = store_in_packfiles(dir.as_ref(), partition, &layout).await;
    let bucket_path = format_bucket_path(&dir.join("immutable"), group_index, 0);
    std::fs::remove_file(&bucket_path).unwrap();
    std::fs::create_dir(&bucket_path).unwrap();
    std::fs::write(bucket_path.join("blocker"), b"x").unwrap();

    let failed = try_compact_group_toward(&store, group_index, 1, true).await;
    assert!(failed.is_err(), "the failed bucket write is returned");

    assert_eq!(
        store.group[group_index].packstore.sizes().await.unwrap(),
        vec![2 * 4096, 2 * 4096, 4096],
        "only the first packfile's local payload is copied"
    );
}

/// While a group's fan-out is not committed on disk, a delayed flush writes none of its bucket
/// files, which are laid out for the committed level, and leaves the buckets dirty, so that the
/// next flush commits the fan-out and every payload reads back once the store reopens.
#[tokio::test]
async fn no_bucket_file_is_written_while_a_fan_out_is_uncommitted() {
    use lore_storage::immutable_store::ImmutableStore;

    let dir = lore_base::test_util::TempDir::new("is_uncommitted_fan_out_");
    let settings = || ImmutableStoreSettings {
        fan_out_threshold: 2,
        ..client_settings()
    };
    let partition = Partition::from([0x4bu8; 16]);
    let (group_index, payloads) = payloads_in_one_group(4);
    let bucket_path = format_bucket_path(&dir.join("immutable"), group_index, 0);
    let pending = bucket_path.with_file_name(lore_storage::local::fan_out::LEVEL_PENDING_FILENAME);
    let mut addresses = vec![];
    {
        let store = LocalImmutableStore::new(Some(dir.to_path_buf()), settings())
            .await
            .unwrap();
        for payload in &payloads[..2] {
            addresses
                .push(put_payload(&store, partition, Context::default(), payload, false).await);
        }
        store.clone().flush(false).await.unwrap();
        let committed = std::fs::read(&bucket_path).unwrap();
        for payload in &payloads[2..] {
            addresses
                .push(put_payload(&store, partition, Context::default(), payload, false).await);
        }
        std::fs::create_dir_all(&pending).unwrap();
        std::fs::write(pending.join("blocker"), b"x").unwrap();
        assert!(store.clone().flush(false).await.is_err());
        let group = &store.group[group_index];
        assert_ne!(
            group.bucket_count.load(atomic::Ordering::Relaxed),
            group.committed_level.load(atomic::Ordering::Relaxed),
            "the group fanned out without committing"
        );

        ImmutableStoreGroup::flush_delayed(Arc::downgrade(&store), group_index, 0).await;

        assert_eq!(std::fs::read(&bucket_path).unwrap(), committed);
        assert!(!format_bucket_path(&dir.join("immutable"), group_index, 1).exists());
        assert!(group.dirty[0].load(atomic::Ordering::Relaxed));
        std::fs::remove_dir_all(&pending).unwrap();
        store.clone().flush(false).await.unwrap();
    }

    let store = LocalImmutableStore::new(Some(dir.to_path_buf()), settings())
        .await
        .unwrap();
    assert_payloads_read_back(&store, partition, addresses.into_iter().zip(payloads)).await;
}

/// A group whose packfiles fail to load is left out of compaction while the groups after it
/// load and compact, and its payloads read back once its packfiles can be read again.
#[tokio::test]
async fn compaction_goes_on_past_a_group_whose_packfiles_fail_to_load() {
    use lore_storage::immutable_store::ImmutableStore;

    use crate::store_fixture::payloads_in_two_groups;

    let dir = lore_base::test_util::TempDir::new("is_compact_unloadable_");
    let partition = Partition::from([0x49u8; 16]);
    let [mut first, mut second] = payloads_in_two_groups(4);
    if first.0 > second.0 {
        std::mem::swap(&mut first, &mut second);
    }
    let ((failing, failing_payloads), (loading, loading_payloads)) = (first, second);
    let mut failing_local = vec![];
    {
        let store = LocalImmutableStore::new(Some(dir.to_path_buf()), client_settings())
            .await
            .unwrap();
        for (group_payloads, local) in [
            (&failing_payloads, Some(&mut failing_local)),
            (&loading_payloads, None),
        ] {
            let mut local = local;
            for (index, payload) in group_payloads.iter().enumerate() {
                let address =
                    put_payload(&store, partition, Context::default(), payload, index < 2).await;
                if index >= 2
                    && let Some(local) = local.as_mut()
                {
                    local.push((address, payload.clone()));
                }
            }
        }
        store.clone().flush(true).await.unwrap();
    }
    let pack = dir
        .join("immutable")
        .join("index")
        .join(format!("{failing:02x}"))
        .join("pack");
    let packfile = pack.join("1");
    let aside = pack.join("1.aside");
    std::fs::rename(&packfile, &aside).unwrap();
    std::fs::create_dir(&packfile).unwrap();
    std::fs::write(pack.join("2"), b"").unwrap();

    {
        let store = LocalImmutableStore::new(Some(dir.to_path_buf()), client_settings())
            .await
            .unwrap();
        assert!(store.clone().load_index().await.is_err());
        assert!(compact_group(&store, failing).await);
        assert!(compact_group(&store, loading).await);
        assert_eq!(
            store.group[loading].packstore.total_size().await.unwrap(),
            2 * 4096,
            "the group that loads is compacted"
        );
    }

    std::fs::remove_dir(&packfile).unwrap();
    std::fs::rename(&aside, &packfile).unwrap();
    std::fs::remove_file(pack.join("2")).unwrap();
    assert_eq!(std::fs::metadata(&packfile).unwrap().len(), 4 * 4096);
    let store = LocalImmutableStore::new(Some(dir.to_path_buf()), client_settings())
        .await
        .unwrap();
    assert_payloads_read_back(&store, partition, failing_local).await;
}

/// A client store over `dir` holding `payloads`, all in one group, the first `durable` of them
/// durable and the rest local, flushed, so that compacting the group evicts the durable ones and
/// rewrites packfile 1 down to the local ones.
async fn store_for_rewrite(
    dir: &Path,
    partition: Partition,
    payloads: &[Vec<u8>],
    durable: usize,
) -> Arc<LocalImmutableStore> {
    use lore_storage::immutable_store::ImmutableStore;

    let store = LocalImmutableStore::new(Some(dir.to_path_buf()), client_settings())
        .await
        .unwrap();
    for (index, payload) in payloads.iter().enumerate() {
        put_payload(
            &store,
            partition,
            Context::default(),
            payload,
            index < durable,
        )
        .await;
    }
    store.clone().flush(true).await.unwrap();
    store
}

/// A payload written into a packfile before compaction stopped writes to it, whose entry is not
/// in the index yet, is copied by the rewrite once the entry is in, rather than truncated away
/// under the entry: compaction waits for it.
#[tokio::test]
async fn compaction_waits_for_an_entry_written_into_a_packfile_it_rewrites() {
    let dir = lore_base::test_util::TempDir::new("is_compact_unindexed_");
    let partition = Partition::from([0x31u8; 16]);
    let (group_index, payloads) = payloads_in_one_group(5);
    let store = store_for_rewrite(&dir, partition, &payloads[..4], 2).await;

    let selected = pause_at(PausePoint::Selected);
    let compaction = {
        let store = store.clone();
        lore_base::lore_spawn!(async move { compact_group(&store, group_index).await })
    };
    selected.reached.notified().await;

    let written = pause_at(PausePoint::Written);
    let put = {
        let store = store.clone();
        let payload = payloads[4].clone();
        lore_base::lore_spawn!(async move {
            put_payload(&store, partition, Context::default(), &payload, false).await
        })
    };
    written.reached.notified().await;
    selected.resume.notify_one();

    let gate = &store.group[group_index].payload_gate;
    wait_until(|| gate.is_draining(), &compaction).await;
    assert!(!compaction.is_finished(), "compaction waits for the entry");
    written.resume.notify_one();
    put.await.unwrap();
    assert!(compaction.await.unwrap());

    assert!(
        group_entries(&store, group_index)
            .await
            .iter()
            .filter(|entry| entry.data.pack_file != 0)
            .all(|entry| entry.data.pack_file == 2),
        "the rewrite copied the late entry's payload with the others"
    );
    assert_payloads_read_back(
        &store,
        partition,
        payloads[2..].iter().map(|payload| {
            (
                Address::zero_context_hash(hash::hash_slice(payload)),
                payload.clone(),
            )
        }),
    )
    .await;
}

/// A payload written but not yet in the index does not make its packfile look reclaimable:
/// compaction waits for the entry before it measures what the index references, and leaves the
/// fully referenced group in place.
#[tokio::test]
async fn compaction_measures_a_written_payload_once_its_entry_is_in() {
    let dir = lore_base::test_util::TempDir::new("is_compact_measure_unindexed_");
    let partition = Partition::from([0x32u8; 16]);
    let (group_index, payloads) = payloads_in_one_group(4);
    let store = store_for_rewrite(&dir, partition, &payloads[..3], 0).await;

    let written = pause_at(PausePoint::Written);
    let put = {
        let store = store.clone();
        let payload = payloads[3].clone();
        lore_base::lore_spawn!(async move {
            put_payload(&store, partition, Context::default(), &payload, false).await
        })
    };
    written.reached.notified().await;
    let compaction = {
        let store = store.clone();
        lore_base::lore_spawn!(async move { compact_group(&store, group_index).await })
    };

    let gate = &store.group[group_index].payload_gate;
    wait_until(|| gate.is_draining(), &compaction).await;
    assert!(!compaction.is_finished(), "compaction waits for the entry");
    written.resume.notify_one();
    put.await.unwrap();
    assert!(compaction.await.unwrap());

    assert!(
        group_entries(&store, group_index)
            .await
            .iter()
            .all(|entry| entry.data.pack_file == 1),
        "the packfile is not rewritten"
    );
    assert_eq!(
        store.group[group_index].packstore.sizes().await.unwrap(),
        vec![4 * 4096]
    );
}

/// Read `address`'s payload through `get`, or through `get_into` with `into`.
async fn read_payload(
    store: Arc<LocalImmutableStore>,
    partition: Partition,
    address: Address,
    into: bool,
) -> Result<Vec<u8>, StoreError> {
    use lore_storage::immutable_store::ImmutableStore;

    if !into {
        let (_fragment, payload) = store.get(partition, address).await?.into_payload()?;
        return Ok(payload.to_vec());
    }
    let mut buffer = vec![0u8; lore_storage::FRAGMENT_SIZE_THRESHOLD];
    // SAFETY: `buffer` outlives the read and nothing else touches it until the read returns.
    let mut dst = unsafe { lore_storage::CallerBuffer::new(buffer.as_mut_ptr(), buffer.len()) };
    let (fragment, read) = store.get_into(partition, address, &mut dst).await?;
    Ok(match read {
        lore_storage::store_types::PayloadRead::Returned(payload) => payload.to_vec(),
        lore_storage::store_types::PayloadRead::IntoBuffer => {
            buffer.truncate(fragment.size_payload as usize);
            buffer
        }
    })
}

/// A read, through `get` or `get_into`, that found its payload's location before compaction moved
/// the entry loads it before the packfile is truncated: compaction waits for the read.
#[tokio::test]
async fn compaction_waits_for_a_read_of_a_packfile_it_truncates() {
    for into in [false, true] {
        let dir = lore_base::test_util::TempDir::new("is_compact_reading_");
        let partition = Partition::from([0x33u8; 16]);
        let (group_index, payloads) = payloads_in_one_group(4);
        let store = store_for_rewrite(&dir, partition, &payloads, 2).await;

        let stopped = pause_at(PausePoint::Stopped);
        let compaction = {
            let store = store.clone();
            lore_base::lore_spawn!(async move { compact_group(&store, group_index).await })
        };
        stopped.reached.notified().await;

        let located = pause_at(PausePoint::Located);
        let read = lore_base::lore_spawn!(read_payload(
            store.clone(),
            partition,
            Address::zero_context_hash(hash::hash_slice(&payloads[2])),
            into
        ));
        located.reached.notified().await;
        stopped.resume.notify_one();

        let gate = &store.group[group_index].payload_gate;
        wait_until(|| gate.is_draining(), &compaction).await;
        assert!(
            !compaction.is_finished(),
            "compaction waits for the read before it truncates"
        );
        located.resume.notify_one();
        let read = read.await.unwrap().expect("the read loads its payload");
        assert_eq!(read, payloads[2]);
        assert!(compaction.await.unwrap());

        assert_eq!(
            store.group[group_index].packstore.sizes().await.unwrap(),
            vec![0, 2 * 4096],
            "packfile 1 is truncated once the read is done"
        );
        assert_payloads_read_back(
            &store,
            partition,
            payloads[2..].iter().map(|payload| {
                (
                    Address::zero_context_hash(hash::hash_slice(payload)),
                    payload.clone(),
                )
            }),
        )
        .await;
    }
}

/// A bucket being written to disk holds up neither a lookup nor an insert in it, and the insert
/// made during the write reaches the next one.
#[tokio::test]
async fn a_bucket_write_in_flight_holds_up_no_lookup_or_insert() {
    use lore_storage::immutable_store::ImmutableStore;

    let dir = lore_base::test_util::TempDir::new("is_bucket_write_in_flight_");
    let settings = || ImmutableStoreSettings {
        initial_fan_out_level: 1,
        ..client_settings()
    };
    let store = LocalImmutableStore::new(Some(dir.to_path_buf()), settings())
        .await
        .unwrap();
    let partition = Partition::from([0x37u8; 16]);
    let (_, payloads) = payloads_in_one_group(2);
    let first = put_payload(&store, partition, Context::default(), &payloads[0], false).await;

    let serializing = pause_at(PausePoint::Serializing);
    let flush = {
        let store = store.clone();
        lore_base::lore_spawn!(async move { store.flush(false).await })
    };
    serializing.reached.notified().await;

    let during = tokio::time::timeout(Duration::from_secs(10), async {
        let second = put_payload(&store, partition, Context::default(), &payloads[1], false).await;
        assert_payloads_read_back(&store, partition, [(first, payloads[0].clone())]).await;
        second
    })
    .await;
    serializing.resume.notify_one();
    let second = during.expect("the insert and the lookup do not wait for the bucket write");
    flush.await.unwrap().unwrap();

    store.clone().flush(false).await.unwrap();
    drop(store);
    let reopened = LocalImmutableStore::new(Some(dir.to_path_buf()), settings())
        .await
        .unwrap();
    assert_payloads_read_back(
        &reopened,
        partition,
        [(first, payloads[0].clone()), (second, payloads[1].clone())],
    )
    .await;
}

/// Run the operation `start` spawns until it reaches [`PausePoint::Located`], and assert that a
/// drain of `group_index`'s payload gate waits for it there. Returns what the operation returns.
async fn assert_held_while_located<T: Send + 'static>(
    store: &Arc<LocalImmutableStore>,
    group_index: usize,
    start: impl FnOnce() -> tokio::task::JoinHandle<T>,
) -> T {
    let located = pause_at(PausePoint::Located);
    let operation = start();
    located.reached.notified().await;
    let drain = {
        let store = store.clone();
        lore_base::lore_spawn!(async move { store.group[group_index].payload_gate.drain().await })
    };

    let gate = &store.group[group_index].payload_gate;
    wait_until(|| gate.is_draining(), &drain).await;
    assert!(!drain.is_finished(), "the drain waits for the load");
    located.resume.notify_one();
    drain.await.unwrap();
    operation.await.unwrap()
}

/// Obliterating a fragment list holds the group's payload gate from its lookup until it has loaded
/// the list, so that a drain, as compaction runs before it truncates a packfile, waits for the
/// load.
#[tokio::test]
async fn obliterate_holds_the_payload_gate_while_it_loads_the_fragment_list() {
    use lore_storage::immutable_store::ImmutableStore;

    let dir = lore_base::test_util::TempDir::new("is_obliterate_gate_");
    let store = LocalImmutableStore::new(Some(dir.to_path_buf()), client_settings())
        .await
        .unwrap();
    let partition = Partition::from([0x38u8; 16]);
    let leaf_payload = b"the one leaf of the list";
    let leaf = put_payload(&store, partition, Context::default(), leaf_payload, false).await;
    let references = [FragmentReference {
        hash: leaf.hash,
        offset_content: 0,
    }];
    let list = Address::zero_context_hash(hash::hash_slice(references.as_bytes()));
    store
        .clone()
        .put(
            partition,
            list,
            Fragment {
                flags: (FragmentFlags::PayloadFragmented | FragmentFlags::PayloadStoredLocal)
                    .bits(),
                size_payload: references.as_bytes().len() as u32,
                size_content: leaf_payload.len() as u64,
            },
            Some(Bytes::copy_from_slice(references.as_bytes())),
            false,
        )
        .await
        .unwrap();
    let group_index = list.hash.data()[0] as usize;

    assert_held_while_located(&store, group_index, || {
        let store = store.clone();
        lore_base::lore_spawn!(async move {
            store
                .obliterate(
                    partition,
                    list,
                    Arc::new(lore_storage::store_types::StoreObliterateStats::default()),
                )
                .await
        })
    })
    .await
    .unwrap();
    assert!(
        store.clone().get(partition, leaf).await.is_err(),
        "the list loaded, and its leaf is obliterated"
    );
}

/// Verifying a fragment holds the group's payload gate from its lookup until it has loaded the
/// payloads it checks, so that a drain waits for the load.
#[tokio::test]
async fn verify_holds_the_payload_gate_while_it_loads_the_payload() {
    let dir = lore_base::test_util::TempDir::new("is_verify_gate_");
    let store = LocalImmutableStore::new(Some(dir.to_path_buf()), client_settings())
        .await
        .unwrap();
    let partition = Partition::from([0x39u8; 16]);
    let (group_index, payloads) = payloads_in_one_group(1);
    let address = put_payload(&store, partition, Context::default(), &payloads[0], false).await;

    let verified = assert_held_while_located(&store, group_index, || {
        lore_base::lore_spawn!(store.clone().verify_fragment(
            address,
            partition,
            StoreMatch::MatchFull,
            false
        ))
    })
    .await
    .unwrap();
    assert!(verified.verification_result.is_ok());
}

/// A store verifying its write holds the group's payload gate from its lookup until it has loaded
/// the payload it checks, so that a drain waits for the load.
#[tokio::test]
async fn a_verified_store_holds_the_payload_gate_while_it_loads_the_payload() {
    let dir = lore_base::test_util::TempDir::new("is_verify_write_gate_");
    let store = LocalImmutableStore::new(
        Some(dir.to_path_buf()),
        ImmutableStoreSettings {
            verify_write: true,
            ..client_settings()
        },
    )
    .await
    .unwrap();
    let partition = Partition::from([0x3cu8; 16]);
    let (group_index, payloads) = payloads_in_one_group(1);

    let address = assert_held_while_located(&store, group_index, || {
        let store = store.clone();
        let payload = payloads[0].clone();
        lore_base::lore_spawn!(async move {
            put_payload(&store, partition, Context::default(), &payload, false).await
        })
    })
    .await;
    assert_payloads_read_back(&store, partition, [(address, payloads[0].clone())]).await;
}

/// Stores of one new hash in flight together each write the payload. Every entry takes the
/// payload of the store that inserted first, the other copy stays unreferenced, and each address
/// reads the payload back.
#[tokio::test]
async fn stores_of_one_new_hash_in_flight_together_read_back() {
    let dir = lore_base::test_util::TempDir::new("is_same_hash_stores_");
    let store = LocalImmutableStore::new(Some(dir.to_path_buf()), client_settings())
        .await
        .unwrap();
    let partition = Partition::from([0x3au8; 16]);
    let (group_index, payloads) = payloads_in_one_group(1);
    let payload = payloads[0].clone();

    let written = pause_at(PausePoint::Written);
    let first = {
        let store = store.clone();
        let payload = payload.clone();
        lore_base::lore_spawn!(async move {
            put_payload(&store, partition, Context::from([1u8; 16]), &payload, false).await
        })
    };
    written.reached.notified().await;
    let second = put_payload(&store, partition, Context::from([2u8; 16]), &payload, false).await;
    written.resume.notify_one();
    let first = first.await.unwrap();

    let entries = group_entries(&store, group_index).await;
    assert_eq!(entries.len(), 2);
    assert!(
        entries
            .iter()
            .all(|entry| (entry.data.pack_file, entry.data.pack_offset) == (1, 4096)),
        "both entries take the payload the second store inserted"
    );
    assert_eq!(
        store.group[group_index].packstore.sizes().await.unwrap(),
        vec![2 * 4096],
        "the first store's copy stays in the packfile, unreferenced"
    );
    assert_payloads_read_back(
        &store,
        partition,
        [(first, payload.clone()), (second, payload)],
    )
    .await;
}

/// Compaction syncs the group's packfiles before it writes a bucket it rewrote, without holding
/// the bucket, so that a lookup and an insert in it go on during the sync, and the insert reaches
/// the next bucket write.
#[tokio::test]
async fn a_compaction_sync_holds_up_no_lookup_or_insert() {
    use lore_storage::immutable_store::ImmutableStore;

    let dir = lore_base::test_util::TempDir::new("is_compact_sync_");
    let partition = Partition::from([0x3bu8; 16]);
    let (group_index, payloads) = payloads_in_one_group(5);
    let store = store_for_rewrite(&dir, partition, &payloads[..4], 2).await;

    let syncing = pause_at(PausePoint::Syncing);
    let compaction = {
        let store = store.clone();
        lore_base::lore_spawn!(async move { compact_group(&store, group_index).await })
    };
    syncing.reached.notified().await;

    let during = tokio::time::timeout(Duration::from_secs(10), async {
        let late = put_payload(&store, partition, Context::default(), &payloads[4], false).await;
        assert_payloads_read_back(
            &store,
            partition,
            [(
                Address::zero_context_hash(hash::hash_slice(&payloads[2])),
                payloads[2].clone(),
            )],
        )
        .await;
        late
    })
    .await;
    syncing.resume.notify_one();
    let late = during.expect("the insert and the lookup do not wait for the sync");
    assert!(compaction.await.unwrap());

    store.clone().flush(true).await.unwrap();
    drop(store);
    let reopened = LocalImmutableStore::new(Some(dir.to_path_buf()), client_settings())
        .await
        .unwrap();
    assert_payloads_read_back(
        &reopened,
        partition,
        payloads[2..4]
            .iter()
            .map(|payload| {
                (
                    Address::zero_context_hash(hash::hash_slice(payload)),
                    payload.clone(),
                )
            })
            .chain([(late, payloads[4].clone())]),
    )
    .await;
}

/// A store whose payload write fails reports the failure and leaves no entry, and the next store
/// of the payload stores it.
#[tokio::test]
async fn a_failed_payload_write_leaves_no_entry() {
    let dir = lore_base::test_util::TempDir::new("is_failed_payload_write_");
    let store = LocalImmutableStore::new(Some(dir.to_path_buf()), client_settings())
        .await
        .unwrap();
    let partition = Partition::from([0x3du8; 16]);
    let (group_index, payloads) = payloads_in_one_group(1);

    lore_storage::test_util::fail_packfile_writes(1);
    let failed = try_put_payload(
        &store,
        partition,
        Context::default(),
        &payloads[0],
        false,
        false,
    )
    .await;
    assert!(failed.is_err());
    assert!(group_entries(&store, group_index).await.is_empty());

    let address = put_payload(&store, partition, Context::default(), &payloads[0], false).await;
    assert_payloads_read_back(&store, partition, [(address, payloads[0].clone())]).await;
}

/// A forced store writes the payload again, though one is stored for its hash, and the entry
/// points at the new copy.
#[tokio::test]
async fn a_forced_store_points_the_entry_at_a_new_copy() {
    let dir = lore_base::test_util::TempDir::new("is_forced_store_");
    let store = LocalImmutableStore::new(Some(dir.to_path_buf()), client_settings())
        .await
        .unwrap();
    let partition = Partition::from([0x3eu8; 16]);
    let (group_index, payloads) = payloads_in_one_group(1);
    put_payload(&store, partition, Context::default(), &payloads[0], false).await;

    let address = try_put_payload(
        &store,
        partition,
        Context::default(),
        &payloads[0],
        false,
        true,
    )
    .await
    .unwrap();

    let entries = group_entries(&store, group_index).await;
    assert_eq!(entries.len(), 1);
    assert_eq!(
        (entries[0].data.pack_file, entries[0].data.pack_offset),
        (1, 4096)
    );
    assert_payloads_read_back(&store, partition, [(address, payloads[0].clone())]).await;
}

/// A store giving a payload to an entry held without one changes the entry's stored flags only
/// as it inserts the payload, so that until then the entry is as it was.
#[tokio::test]
async fn an_entry_takes_new_flags_with_its_payload() {
    use lore_storage::immutable_store::ImmutableStore;

    let dir = lore_base::test_util::TempDir::new("is_flags_with_payload_");
    let store = LocalImmutableStore::new(Some(dir.to_path_buf()), client_settings())
        .await
        .unwrap();
    let partition = Partition::from([0x3fu8; 16]);
    let (group_index, payloads) = payloads_in_one_group(1);
    let address = Address::zero_context_hash(hash::hash_slice(&payloads[0]));
    let durable = FragmentFlags::PayloadStoredDurable.bits();
    store
        .clone()
        .put(
            partition,
            address,
            Fragment {
                flags: durable,
                size_payload: payloads[0].len() as u32,
                size_content: payloads[0].len() as u64,
            },
            None,
            false,
        )
        .await
        .unwrap();

    let written = pause_at(PausePoint::Written);
    let put = {
        let store = store.clone();
        let payload = payloads[0].clone();
        lore_base::lore_spawn!(async move {
            put_payload(&store, partition, Context::default(), &payload, false).await
        })
    };
    written.reached.notified().await;
    let entries = group_entries(&store, group_index).await;
    assert_eq!(
        (entries[0].data.flags, entries[0].data.pack_file),
        (durable, 0),
        "the entry is as it was while the payload is written"
    );
    written.resume.notify_one();
    put.await.unwrap();

    let entries = group_entries(&store, group_index).await;
    assert_eq!(
        entries[0].data.flags,
        durable | FragmentFlags::PayloadStoredLocal.bits()
    );
    assert_ne!(entries[0].data.pack_file, 0);
}

/// A durable flush syncs the payloads of the entries in each bucket file it writes, including
/// entries inserted after the flush began: a payload stored while the flush writes another bucket
/// is synced by the time the flush returns.
#[tokio::test]
async fn a_durable_flush_syncs_the_payloads_its_bucket_files_point_at() {
    use lore_storage::immutable_store::ImmutableStore;

    let dir = lore_base::test_util::TempDir::new("is_durable_flush_");
    let store = LocalImmutableStore::new(
        Some(dir.to_path_buf()),
        ImmutableStoreSettings {
            initial_fan_out_level: 32,
            flush_delay_seconds: 3600,
            ..client_settings()
        },
    )
    .await
    .unwrap();
    let partition = Partition::from([0x40u8; 16]);
    let (group_index, mut payloads) = payloads_in_one_group(16);
    payloads.sort_by_key(|payload| {
        lore_storage::local::fan_out::bucket_index_for(&hash::hash_slice(payload), 32)
    });
    let late = payloads.pop().unwrap();
    put_payload(&store, partition, Context::default(), &payloads[0], false).await;
    store.clone().flush(true).await.unwrap();
    put_payload(&store, partition, Context::default(), &payloads[0], true).await;

    let serializing = pause_at(PausePoint::Serializing);
    let flush = {
        let store = store.clone();
        lore_base::lore_spawn!(async move { store.flush(true).await })
    };
    serializing.reached.notified().await;
    put_payload(&store, partition, Context::default(), &late, false).await;
    serializing.resume.notify_one();
    flush.await.unwrap().unwrap();

    assert!(
        !store.group[group_index].packstore.is_dirty(1).await,
        "the late payload is synced"
    );
}

/// The directory group `group_index` of a store over `dir` keeps its packfiles in.
fn group_pack_dir(dir: &Path, group_index: usize) -> PathBuf {
    lore_storage::local::fan_out::group_dir_path(&dir.join("immutable").join("index"), group_index)
        .join("pack")
}

/// The header and bytes of the file of bucket `bucket_index` of group `group_index`, in a store
/// over `dir`.
fn read_bucket_file(
    dir: &Path,
    group_index: usize,
    bucket_index: usize,
) -> (ImmutableStoreHeader, Vec<u8>) {
    let bytes = std::fs::read(format_bucket_path(
        &dir.join("immutable"),
        group_index,
        bucket_index,
    ))
    .unwrap();
    let (header, _) = ImmutableStoreHeader::read_from_prefix(&bytes).unwrap();
    (header, bytes)
}

/// Rewrite the header of the file of bucket 0 of group `group_index`, in a store over `dir`.
fn edit_bucket_header(
    dir: &Path,
    group_index: usize,
    edit: impl FnOnce(&mut ImmutableStoreHeader),
) {
    let (mut header, mut bytes) = read_bucket_file(dir, group_index, 0);
    edit(&mut header);
    bytes[..size_of::<ImmutableStoreHeader>()].copy_from_slice(header.as_bytes());
    std::fs::write(
        format_bucket_path(&dir.join("immutable"), group_index, 0),
        bytes,
    )
    .unwrap();
}

/// The bytes of a bucket file holding `count` entries and no packfile sizes after them.
fn bucket_file_len(count: usize) -> usize {
    size_of::<ImmutableStoreHeader>()
        + count * (size_of::<u32>() + size_of::<ImmutableStoreEntry>())
}

/// A bucket file records the size of the packfile taking writes in its header, and none when the
/// write synced every packfile first.
#[tokio::test]
async fn a_bucket_file_records_the_size_of_the_packfile_taking_writes() {
    use lore_storage::immutable_store::ImmutableStore;

    let dir = lore_base::test_util::TempDir::new("is_sizes_recorded_");
    let store = LocalImmutableStore::new(Some(dir.to_path_buf()), client_settings())
        .await
        .unwrap();
    let partition = Partition::from([0x41u8; 16]);
    let (group_index, payloads) = payloads_in_one_group(3);
    for payload in &payloads[..2] {
        put_payload(&store, partition, Context::default(), payload, false).await;
    }
    store.clone().flush(false).await.unwrap();

    let (header, bytes) = read_bucket_file(&dir, group_index, 0);
    assert_eq!(
        (header.packfile_sizes, header.first_packfile_size),
        (SIZES_RECORDED | (1 << SIZES_COUNT_SHIFT) | 1, 8192)
    );
    assert_eq!(bytes.len(), bucket_file_len(2));

    put_payload(&store, partition, Context::default(), &payloads[2], false).await;
    store.clone().flush(true).await.unwrap();
    let (header, bytes) = read_bucket_file(&dir, group_index, 0);
    assert_eq!(header.packfile_sizes, SIZES_RECORDED);
    assert_eq!(bytes.len(), bucket_file_len(3));
}

/// A bucket file holds the packfile sizes its header has no room for after its entries, in fewer
/// bytes than an entry, so that a reader that does not know of them counts the entries alone. The
/// store reopens with the entries intact.
#[tokio::test]
async fn a_bucket_file_holds_the_sizes_its_header_has_no_room_for_after_its_entries() {
    use lore_storage::immutable_store::ImmutableStore;

    let dir = lore_base::test_util::TempDir::new("is_sizes_trailer_");
    let partition = Partition::from([0x42u8; 16]);
    let (group_index, payloads) = payloads_in_one_group(1);
    let pack = group_pack_dir(&dir, group_index);
    std::fs::create_dir_all(&pack).unwrap();
    for (id, len) in [(1, 64), (2, 32), (3, 16)] {
        std::fs::write(pack.join(id.to_string()), vec![0u8; len]).unwrap();
    }
    let (address, bucket_index) = {
        let store = LocalImmutableStore::new(Some(dir.to_path_buf()), client_settings())
            .await
            .unwrap();
        let address = put_payload(&store, partition, Context::default(), &payloads[0], false).await;
        store.clone().flush(false).await.unwrap();
        let bucket_count = store.group[group_index]
            .bucket_count
            .load(atomic::Ordering::Relaxed);
        let bucket_index =
            lore_storage::local::fan_out::bucket_index_for(&address.hash, bucket_count);
        (address, bucket_index)
    };

    let (header, bytes) = read_bucket_file(&dir, group_index, bucket_index);
    assert_eq!(
        (header.packfile_sizes, header.first_packfile_size),
        (SIZES_RECORDED | (3 << SIZES_COUNT_SHIFT) | 1, 64 + 4096)
    );
    let entries_end = bucket_file_len(1);
    assert_eq!(
        &bytes[entries_end..],
        [
            PackfileSize { id: 2, size: 32 },
            PackfileSize { id: 3, size: 16 }
        ]
        .as_bytes()
    );
    assert_eq!(
        (bytes.len() - size_of::<ImmutableStoreHeader>())
            / (size_of::<u32>() + size_of::<ImmutableStoreEntry>()),
        1
    );

    let store = LocalImmutableStore::new(Some(dir.to_path_buf()), client_settings())
        .await
        .unwrap();
    assert_payloads_read_back(&store, partition, [(address, payloads[0].clone())]).await;
    assert!(!store.group[group_index].dirty[bucket_index].load(atomic::Ordering::Relaxed));
}

/// A bucket write that would record more packfile sizes than a bucket file holds syncs every
/// packfile of the group instead, and records none.
#[tokio::test]
async fn more_packfiles_to_record_than_a_bucket_file_holds_are_synced_instead() {
    use lore_storage::immutable_store::ImmutableStore;

    let dir = lore_base::test_util::TempDir::new("is_sizes_overflow_");
    let partition = Partition::from([0x43u8; 16]);
    let (group_index, payloads) = payloads_in_one_group(1);
    let pack = group_pack_dir(&dir, group_index);
    std::fs::create_dir_all(&pack).unwrap();
    for id in 1..=TRAILER_SIZES + 2 {
        std::fs::write(pack.join(id.to_string()), [0u8; 16]).unwrap();
    }
    let store = LocalImmutableStore::new(Some(dir.to_path_buf()), client_settings())
        .await
        .unwrap();
    let address = put_payload(&store, partition, Context::default(), &payloads[0], false).await;
    store.clone().flush(false).await.unwrap();
    let bucket_count = store.group[group_index]
        .bucket_count
        .load(atomic::Ordering::Relaxed);
    let bucket_index = lore_storage::local::fan_out::bucket_index_for(&address.hash, bucket_count);

    let (header, bytes) = read_bucket_file(&dir, group_index, bucket_index);
    assert_eq!(header.packfile_sizes, SIZES_RECORDED);
    assert_eq!(bytes.len(), bucket_file_len(1));
    assert!(!store.group[group_index].packstore.is_dirty(1).await);
}

/// A store over `dir` that wrote two payloads of one group, at offsets 0 and 4096 of packfile 1,
/// to their bucket file without a sync, and closed. Returns the group, the two addresses, and the
/// two payloads with a third of the group.
async fn two_payloads_written(
    dir: &Path,
    partition: Partition,
) -> (usize, Vec<Address>, Vec<Vec<u8>>) {
    use lore_storage::immutable_store::ImmutableStore;

    let (group_index, payloads) = payloads_in_one_group(3);
    let store = LocalImmutableStore::new(Some(dir.to_path_buf()), client_settings())
        .await
        .unwrap();
    let mut addresses = vec![];
    for payload in &payloads[..2] {
        addresses.push(put_payload(&store, partition, Context::default(), payload, false).await);
    }
    store.clone().flush(false).await.unwrap();
    (group_index, addresses, payloads)
}

/// The entry for `address` among the loaded entries of group `group_index`.
async fn loaded_entry(
    store: &LocalImmutableStore,
    group_index: usize,
    address: Address,
) -> ImmutableStoreEntry {
    group_entries(store, group_index)
        .await
        .into_iter()
        .find(|entry| entry.address == address)
        .expect("the entry is loaded")
}

/// A bucket file pointing past the end of its packfile, as after a crash lost the end of the
/// packfile, has those payload locations cleared when it is loaded and is written again: the
/// payload past the end is no longer found, a write at the new end does not take its place, and
/// storing it again stores it.
#[tokio::test]
async fn payloads_past_the_end_of_their_packfile_are_cleared_when_their_bucket_loads() {
    use lore_storage::immutable_store::ImmutableStore;

    let dir = lore_base::test_util::TempDir::new("is_sizes_lost_tail_");
    let partition = Partition::from([0x44u8; 16]);
    let (group_index, addresses, payloads) = two_payloads_written(&dir, partition).await;
    std::fs::OpenOptions::new()
        .write(true)
        .open(group_pack_dir(&dir, group_index).join("1"))
        .unwrap()
        .set_len(4096)
        .unwrap();

    let store = LocalImmutableStore::new(Some(dir.to_path_buf()), client_settings())
        .await
        .unwrap();
    assert_payloads_read_back(&store, partition, [(addresses[0], payloads[0].clone())]).await;
    let lost = loaded_entry(&store, group_index, addresses[1]).await;
    assert_eq!(
        (
            lost.data.pack_file,
            lost.data.flags & FragmentFlags::PayloadStoredLocal.bits()
        ),
        (0, 0)
    );
    assert!(store.group[group_index].dirty[0].load(atomic::Ordering::Relaxed));
    assert!(store.clone().get(partition, addresses[1]).await.is_err());

    let third = put_payload(&store, partition, Context::default(), &payloads[2], false).await;
    put_payload(&store, partition, Context::default(), &payloads[1], false).await;
    assert_payloads_read_back(
        &store,
        partition,
        [
            (addresses[0], payloads[0].clone()),
            (addresses[1], payloads[1].clone()),
            (third, payloads[2].clone()),
        ],
    )
    .await;
}

/// A bucket file pointing into a packfile missing when the store opens has those payload locations
/// cleared when it is loaded.
#[tokio::test]
async fn payloads_in_a_missing_packfile_are_cleared_when_their_bucket_loads() {
    use lore_storage::immutable_store::ImmutableStore;

    let dir = lore_base::test_util::TempDir::new("is_sizes_missing_packfile_");
    let partition = Partition::from([0x45u8; 16]);
    let (group_index, payloads) = payloads_in_one_group(1);
    let address = {
        let store = LocalImmutableStore::new(Some(dir.to_path_buf()), client_settings())
            .await
            .unwrap();
        store.group[group_index]
            .packstore
            .stop_writes(&[1])
            .await
            .unwrap();
        let address = put_payload(&store, partition, Context::default(), &payloads[0], false).await;
        store.clone().flush(false).await.unwrap();
        address
    };
    std::fs::remove_file(group_pack_dir(&dir, group_index).join("2")).unwrap();

    let store = LocalImmutableStore::new(Some(dir.to_path_buf()), client_settings())
        .await
        .unwrap();
    assert!(store.clone().get(partition, address).await.is_err());
    assert_eq!(
        loaded_entry(&store, group_index, address)
            .await
            .data
            .pack_file,
        0
    );
}

/// A bucket file recording no packfile sizes, as an earlier version writes, is trusted: its
/// entries are loaded as they are, though one points past the end of its packfile.
#[tokio::test]
async fn a_bucket_file_recording_no_sizes_is_trusted() {
    use lore_storage::immutable_store::ImmutableStore;

    let dir = lore_base::test_util::TempDir::new("is_sizes_unrecorded_");
    let partition = Partition::from([0x46u8; 16]);
    let (group_index, addresses, _) = two_payloads_written(&dir, partition).await;
    edit_bucket_header(&dir, group_index, |header| {
        header.packfile_sizes = 0;
        header.first_packfile_size = 0;
    });
    std::fs::OpenOptions::new()
        .write(true)
        .open(group_pack_dir(&dir, group_index).join("1"))
        .unwrap()
        .set_len(4096)
        .unwrap();

    let store = LocalImmutableStore::new(Some(dir.to_path_buf()), client_settings())
        .await
        .unwrap();
    store.clone().get(partition, addresses[0]).await.unwrap();
    assert_eq!(
        loaded_entry(&store, group_index, addresses[1])
            .await
            .data
            .pack_file,
        1
    );
    assert!(!store.group[group_index].dirty[0].load(atomic::Ordering::Relaxed));
}

/// A bucket file whose header counts more packfile sizes than follow its entries, as when a crash
/// cut it short, has its entries checked: those past the end of their packfile are cleared.
#[tokio::test]
async fn a_bucket_file_with_torn_sizes_has_its_entries_checked() {
    let dir = lore_base::test_util::TempDir::new("is_sizes_torn_");
    let partition = Partition::from([0x47u8; 16]);
    let (group_index, addresses, payloads) = two_payloads_written(&dir, partition).await;
    edit_bucket_header(&dir, group_index, |header| {
        header.packfile_sizes = SIZES_RECORDED | (2 << SIZES_COUNT_SHIFT) | 1;
    });
    std::fs::OpenOptions::new()
        .write(true)
        .open(group_pack_dir(&dir, group_index).join("1"))
        .unwrap()
        .set_len(4096)
        .unwrap();

    let store = LocalImmutableStore::new(Some(dir.to_path_buf()), client_settings())
        .await
        .unwrap();
    assert_payloads_read_back(&store, partition, [(addresses[0], payloads[0].clone())]).await;
    assert_eq!(
        loaded_entry(&store, group_index, addresses[1])
            .await
            .data
            .pack_file,
        0
    );
}
