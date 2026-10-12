// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic;

use lore_storage::Hash;
use lore_storage::Partition;
use lore_storage::immutable_store::ImmutableStore;
use lore_storage::local::immutable_store::format_bucket_path;
use lore_storage::local::mutable_store::*;
use lore_storage::store_types::KeyType;
use zerocopy::FromZeros;
use zerocopy::IntoBytes;

fn write_bucket_file(path: &Path, version: u32) {
    let entry = MutableStoreEntry::default();
    let mut header = MutableStoreHeader::new_zeroed();
    header.version = version;
    header.count = 1;
    let mut bytes =
        Vec::with_capacity(size_of::<MutableStoreHeader>() + 4 + size_of::<MutableStoreEntry>());
    bytes.extend_from_slice(header.as_bytes());
    bytes.extend_from_slice(&0u32.to_le_bytes());
    bytes.extend_from_slice(entry.as_bytes());
    std::fs::write(path, bytes).unwrap();
}

/// A bucket larger than the head the composite open returns is loaded by scattering one
/// vectored read into both `GrowVec`s. Per-index contents make a misplaced chunk visible as
/// swapped entries rather than as a length mismatch.
#[tokio::test]
async fn deserialize_scatters_a_bucket_larger_than_the_head_read() {
    let dir = lore_base::test_util::TempDir::new("ms_scatter_");
    let path = dir.path().join("bucket");

    let head = lore_storage::local::immutable_store::BUCKET_HEAD_READ;
    let per_entry = size_of::<u32>() + size_of::<MutableStoreEntry>();
    let count = (head / per_entry) + 64;
    assert!(
        size_of::<MutableStoreHeader>() + count * per_entry > head,
        "the bucket has to exceed the head read for this to test anything"
    );

    let mut header = MutableStoreHeader::new_zeroed();
    header.version = MutableStoreVersion::LazyFanOut as u32;
    header.count = count as u32;

    let mut bytes = Vec::with_capacity(size_of::<MutableStoreHeader>() + count * per_entry);
    bytes.extend_from_slice(header.as_bytes());
    for index in 0..count {
        bytes.extend_from_slice(&(index as u32).to_le_bytes());
    }
    for index in 0..count {
        let entry = MutableStoreEntry {
            key: Hash::from([index as u8; 32]),
            ..Default::default()
        };
        bytes.extend_from_slice(entry.as_bytes());
    }
    std::fs::write(&path, &bytes).unwrap();

    let (sorted_index, entry, version) = MutableStoreBucket::deserialize_files(path, false)
        .await
        .unwrap();

    assert_eq!(version, MutableStoreVersion::LazyFanOut as u32);
    assert_eq!(sorted_index.len(), count);
    assert_eq!(entry.len(), count);
    for index in 0..count {
        assert_eq!(sorted_index[index], index as u32, "sorted index at {index}");
        assert_eq!(
            entry[index].key,
            Hash::from([index as u8; 32]),
            "key at {index}"
        );
    }
}

#[tokio::test]
async fn deserialize_accepts_typed_items_v2() {
    let dir = lore_base::test_util::TempDir::new("ms_v2_");
    let path = dir.path().join("bucket");
    write_bucket_file(&path, MutableStoreVersion::TypedItems as u32);
    let result = MutableStoreBucket::deserialize_files(path, false).await;
    assert!(result.is_ok(), "v2 (TypedItems) bucket should deserialize");
}

#[tokio::test]
async fn deserialize_accepts_lazy_fan_out_v3() {
    let dir = lore_base::test_util::TempDir::new("ms_v3_");
    let path = dir.path().join("bucket");
    write_bucket_file(&path, MutableStoreVersion::LazyFanOut as u32);
    let result = MutableStoreBucket::deserialize_files(path, false).await;
    assert!(result.is_ok(), "v3 (LazyFanOut) bucket should deserialize");
}

#[tokio::test]
async fn deserialize_rejects_unknown_future_version() {
    let dir = lore_base::test_util::TempDir::new("ms_v100_");
    let path = dir.path().join("bucket");
    write_bucket_file(&path, 100);
    let result = MutableStoreBucket::deserialize_files(path, false).await;
    assert!(result.is_err(), "v100 bucket should be rejected as too new");
}

/// A torn write can leave a bucket file at its correct byte length but entirely
/// zero-filled, so the header reads count=0 while the size implies a nonzero count. This
/// recovers to an empty bucket rather than hard-erroring.
#[tokio::test]
async fn deserialize_recovers_zero_filled_bucket() {
    let dir = lore_base::test_util::TempDir::new("ms_zerofill_");
    let path = dir.path().join("bucket");
    // Correct length for a 1-entry bucket, but all zeros.
    let len = size_of::<MutableStoreHeader>() + size_of::<u32>() + size_of::<MutableStoreEntry>();
    std::fs::write(&path, vec![0u8; len]).unwrap();

    let (sorted_index, entry, version) = MutableStoreBucket::deserialize_files(path, false)
        .await
        .expect("zero-filled bucket should recover to empty");
    assert_eq!(sorted_index.len(), 0);
    assert_eq!(entry.len(), 0);
    assert_eq!(version, MutableStoreVersion::LazyFanOut as u32);
}

#[tokio::test]
async fn deserialize_authoritative_errors_and_preserves_corrupt_bucket() {
    let dir = lore_base::test_util::TempDir::new("ms_auth_corrupt_");
    let path = dir.path().join("bucket");
    let len = size_of::<MutableStoreHeader>() + size_of::<u32>() + size_of::<MutableStoreEntry>();
    std::fs::write(&path, vec![0u8; len]).unwrap();

    let result = MutableStoreBucket::deserialize_files(path.clone(), true).await;
    assert!(
        result.is_err(),
        "authoritative store must not reset a corrupt bucket"
    );
    assert!(
        path.exists(),
        "authoritative store must preserve the corrupt bucket file"
    );
}

/// Client-shaped settings: groups start at level 1.
fn client_settings() -> MutableStoreSettings {
    MutableStoreSettings {
        initial_fan_out_level: 1,
        ..Default::default()
    }
}

/// Store `count` keys spread across groups, at bucket bytes that route away from bucket 0
/// once a group is at 256 — so a group misread as pre-fan-out sends a lookup to a different
/// bucket than the one holding it.
async fn store_keys(store: &Arc<LocalMutableStore>, partition: Partition, count: u8) -> Vec<Hash> {
    use lore_storage::mutable_store::MutableStore;
    let dyn_store: Arc<dyn MutableStore> = store.clone();
    let mut keys = Vec::new();
    for index in 0..count {
        let mut key = Hash::default();
        key.data_mut()[0] = index;
        key.data_mut()[1] = 0xAB;
        dyn_store
            .clone()
            .store(
                partition,
                key,
                Hash::from_u64(index as u64 + 1),
                KeyType::BranchMetadata,
            )
            .await
            .expect("store succeeds");
        keys.push(key);
    }
    keys
}

/// Run the background timer's flush for every group's bucket 0, and nothing else. At level 1
/// that is the only addressable bucket.
async fn run_delayed_flush(store: &Arc<LocalMutableStore>) {
    let weak = Arc::downgrade(store);
    for group_index in 0..GROUP_COUNT {
        LocalMutableStore::flush_delayed(weak.clone(), group_index, 0, 0)
            .await
            .expect("delayed flush joins");
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

/// The mutable twin of the immutable store's regression: a group persisted only by the
/// delayed flush must be reopened at the level it was written at. Left marker-less, a level-1
/// group is read back as a pre-fan-out 256-bucket layout and everything in `index_00` moves
/// out of reach — here that is branch heads and revision metadata.
///
/// Reachable when a sub-256 store is given a non-zero flush delay. The client's level-1
/// default sets the delay to 0, which stops `mark_dirty` spawning the task, and the server
/// runs the delayed flush with groups at 256, where a missing marker reads back correctly;
/// this pins the invariant rather than leaving it to those defaults.
#[tokio::test]
async fn a_group_the_delayed_flush_persisted_reopens_at_its_written_level() {
    use lore_storage::mutable_store::MutableStore;
    let dir = lore_base::test_util::TempDir::new("ms_delayed_level_");
    let partition = Partition::default();
    let index_root = dir.path().join("mutable").join("index");

    let keys = {
        let store = Arc::new(
            LocalMutableStore::new(
                Some(dir.path()),
                client_settings(),
                make_in_memory_immutable().await,
            )
            .await
            .expect("store opens"),
        );
        assert_eq!(
            store.group[0].bucket_count.load(atomic::Ordering::Relaxed),
            1,
            "a client store starts its groups at level 1"
        );

        let keys = store_keys(&store, partition, 32).await;
        // Persist the way the background timer does, and nothing else: no `flush`, so the
        // two-phase commit that would write the markers never runs.
        run_delayed_flush(&store).await;
        keys
    };

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

    let store = Arc::new(
        LocalMutableStore::new(
            Some(dir.path()),
            client_settings(),
            make_in_memory_immutable().await,
        )
        .await
        .expect("store reopens"),
    );
    let dyn_store: Arc<dyn MutableStore> = store.clone();
    for (index, key) in keys.iter().enumerate() {
        assert_eq!(
            store.group[key.data()[0] as usize]
                .bucket_count
                .load(atomic::Ordering::Relaxed),
            1,
            "group for {key} reopened at the wrong level"
        );
        let loaded = dyn_store
            .clone()
            .load(partition, *key, KeyType::BranchMetadata)
            .await
            .unwrap_or_else(|err| {
                panic!("{key} was stored and persisted but reads back as {err:?}")
            });
        assert_eq!(loaded, Hash::from_u64(index as u64 + 1));
    }
}

/// A store at the flat 256-bucket layout — every legacy store, and every server store —
/// gains no markers: such a group already reads back at the level it was written at.
#[tokio::test]
async fn a_flat_layout_store_gains_no_level_markers() {
    use lore_storage::mutable_store::MutableStore;
    let dir = lore_base::test_util::TempDir::new("ms_flat_level_");
    let partition = Partition::default();
    let index_root = dir.path().join("mutable").join("index");
    let settings = || MutableStoreSettings {
        initial_fan_out_level: lore_storage::local::fan_out::FAN_OUT_LEVEL_MAX,
        ..Default::default()
    };

    let keys = {
        let store = Arc::new(
            LocalMutableStore::new(
                Some(dir.path()),
                settings(),
                make_in_memory_immutable().await,
            )
            .await
            .expect("store opens"),
        );
        assert_eq!(
            store.group[0].bucket_count.load(atomic::Ordering::Relaxed),
            BUCKET_COUNT,
            "this store starts at the flat layout"
        );

        let keys = store_keys(&store, partition, 32).await;
        // Bucket 0xAB is where these keys live at 256, so flush that one.
        let weak = Arc::downgrade(&store);
        for group_index in 0..GROUP_COUNT {
            LocalMutableStore::flush_delayed(weak.clone(), group_index, 0xAB, 0)
                .await
                .expect("delayed flush joins");
        }
        keys
    };

    assert_eq!(
        level_markers(&index_root),
        Vec::<PathBuf>::new(),
        "a group already at 256 reads back at 256 without a marker"
    );

    let store = Arc::new(
        LocalMutableStore::new(
            Some(dir.path()),
            settings(),
            make_in_memory_immutable().await,
        )
        .await
        .expect("store reopens"),
    );
    let dyn_store: Arc<dyn MutableStore> = store.clone();
    for (index, key) in keys.iter().enumerate() {
        assert_eq!(
            store.group[key.data()[0] as usize]
                .bucket_count
                .load(atomic::Ordering::Relaxed),
            BUCKET_COUNT,
            "group for {key} reopened at the wrong level"
        );
        let loaded = dyn_store
            .clone()
            .load(partition, *key, KeyType::BranchMetadata)
            .await
            .unwrap_or_else(|err| panic!("{key} reads back as {err:?}"));
        assert_eq!(loaded, Hash::from_u64(index as u64 + 1));
    }
}

/// A group that already carries a marker keeps the level it records: the initial-level write
/// is for groups that have never had one, and must not overwrite a committed level.
#[tokio::test]
async fn a_marked_group_keeps_the_level_it_recorded() {
    use lore_storage::mutable_store::MutableStore;
    let dir = lore_base::test_util::TempDir::new("ms_marked_level_");
    let partition = Partition::default();
    let index_root = dir.path().join("mutable").join("index");

    {
        let store = Arc::new(
            LocalMutableStore::new(
                Some(dir.path()),
                client_settings(),
                make_in_memory_immutable().await,
            )
            .await
            .expect("store opens"),
        );
        let _ = store_keys(&store, partition, 8).await;
        let dyn_store: Arc<dyn MutableStore> = store.clone();
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
        let store = Arc::new(
            LocalMutableStore::new(
                Some(dir.path()),
                client_settings(),
                make_in_memory_immutable().await,
            )
            .await
            .expect("store reopens"),
        );
        let _ = store_keys(&store, partition, 16).await;
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
fn lazy_fan_out_version_is_three() {
    assert_eq!(MutableStoreVersion::LazyFanOut as u32, 3);
}

#[tokio::test]
async fn latest_version_constant_in_deserialize_path_matches_lazy_fan_out() {
    let dir = lore_base::test_util::TempDir::new("ms_latest_");
    let path = dir.path().join("bucket");
    write_bucket_file(&path, MutableStoreVersion::LazyFanOut as u32);
    let (_, _, version) = MutableStoreBucket::deserialize_files(path, false)
        .await
        .unwrap();
    assert_eq!(version, MutableStoreVersion::LazyFanOut as u32);
}

#[test]
fn mutable_store_settings_default_is_client_friendly() {
    let s = MutableStoreSettings::default();
    assert_eq!(s.flush_delay_seconds, DEFAULT_FLUSH_DELAY_SECONDS);
    assert_eq!(s.initial_fan_out_level, 1);
    assert_eq!(
        s.fan_out_threshold,
        lore_storage::local::fan_out::FAN_OUT_THRESHOLD_DEFAULT
    );
}

/// End-to-end: after a bucket file is zero-filled, the store still opens and the bucket is
/// usable for a store/load round-trip.
#[tokio::test]
async fn store_recovers_from_zero_filled_bucket_and_remains_usable() {
    use lore_storage::mutable_store::MutableStore;

    let dir = lore_base::test_util::TempDir::new("ms_e2e_recover_");
    let store_path = dir.path().to_path_buf();
    let partition = Partition::default();
    let mut key = Hash::default();
    key.data_mut()[0] = 0x10;
    key.data_mut()[1] = 0xAB;
    let value = Hash::from_u64(42);

    {
        let store: Arc<dyn MutableStore> = Arc::new(
            LocalMutableStore::new(
                Some(&store_path),
                MutableStoreSettings {
                    initial_fan_out_level: 1,
                    ..Default::default()
                },
                make_in_memory_immutable().await,
            )
            .await
            .unwrap(),
        );
        store
            .clone()
            .store(partition, key, value, KeyType::BranchMetadata)
            .await
            .unwrap();
        store.clone().flush(true).await.unwrap();
    }

    // initial_fan_out_level=1 → bucket index is always 0; group is (typed) key[0].
    // `LocalMutableStore::new` roots the store under a `mutable/` subdirectory.
    let group_index = key.data()[0] as usize;
    let bucket_path = format_bucket_path(&store_path.join("mutable"), group_index, 0);
    assert!(
        bucket_path.exists(),
        "bucket file should exist after flush at {bucket_path:?}"
    );

    // Torn write: correct byte length, entirely zero-filled.
    let len = std::fs::metadata(&bucket_path).unwrap().len() as usize;
    std::fs::write(&bucket_path, vec![0u8; len]).unwrap();

    let store: Arc<dyn MutableStore> = Arc::new(
        LocalMutableStore::new(
            Some(&store_path),
            MutableStoreSettings {
                initial_fan_out_level: 1,
                ..Default::default()
            },
            make_in_memory_immutable().await,
        )
        .await
        .unwrap(),
    );

    store
        .clone()
        .store(partition, key, value, KeyType::BranchMetadata)
        .await
        .unwrap();
    let reloaded = store
        .clone()
        .load(partition, key, KeyType::BranchMetadata)
        .await
        .unwrap();
    assert_eq!(reloaded, value, "bucket must be usable after recovery");
}

/// A flush without syncing replaces a bucket's file rather than rewriting it, so that a process
/// stopped during the write leaves the file it last wrote, as a hard link to that file shows.
#[tokio::test]
async fn an_unsynced_flush_replaces_the_bucket_file() {
    use lore_storage::mutable_store::MutableStore;

    let dir = lore_base::test_util::TempDir::new("ms_unsynced_replace_");
    let store: Arc<dyn MutableStore> = Arc::new(
        LocalMutableStore::new(
            Some(dir.path()),
            MutableStoreSettings {
                initial_fan_out_level: 1,
                ..Default::default()
            },
            make_in_memory_immutable().await,
        )
        .await
        .unwrap(),
    );
    let partition = Partition::default();
    let key = |index: u8| {
        let mut key = Hash::default();
        key.data_mut()[0] = 0x21;
        key.data_mut()[1] = index;
        key
    };
    let bucket_path = format_bucket_path(&dir.path().join("mutable"), 0x21, 0);
    store
        .clone()
        .store(
            partition,
            key(1),
            Hash::from_u64(1),
            KeyType::BranchMetadata,
        )
        .await
        .unwrap();
    store.clone().flush(false).await.unwrap();
    let written = std::fs::read(&bucket_path).unwrap();
    let link = dir.path().join("last_written");
    std::fs::hard_link(&bucket_path, &link).unwrap();

    store
        .clone()
        .store(
            partition,
            key(2),
            Hash::from_u64(2),
            KeyType::BranchMetadata,
        )
        .await
        .unwrap();
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

#[tokio::test]
async fn local_mutable_store_satisfies_conformance_battery() {
    let store = lore_storage::local::mutable_store::create(
        None::<&std::path::Path>,
        MutableStoreSettings::default(),
        make_in_memory_immutable().await,
    )
    .await
    .expect("create store");
    lore_storage::mutable_conformance::verify_mutable_store(
        store,
        lore_storage::mutable_conformance::Capabilities::new("LocalMutableStore"),
    )
    .await;
}

async fn make_in_memory_immutable() -> Arc<dyn ImmutableStore> {
    lore_storage::local::immutable_store::create(
        None::<&str>,
        lore_storage::local::immutable_store::ImmutableStoreCreateOptions::none(),
        false,
        lore_storage::local::immutable_store::ImmutableStoreSettings::default(),
    )
    .await
    .expect("Failed to create in-memory immutable store")
}

#[tokio::test]
async fn store_initializes_group_bucket_count_from_settings_level_1() {
    use std::sync::atomic::Ordering;
    let store = LocalMutableStore::new(
        None::<&Path>,
        MutableStoreSettings {
            initial_fan_out_level: 1,
            ..Default::default()
        },
        make_in_memory_immutable().await,
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
    let store = LocalMutableStore::new(
        None::<&Path>,
        MutableStoreSettings {
            initial_fan_out_level: lore_storage::local::fan_out::FAN_OUT_LEVEL_MAX,
            ..Default::default()
        },
        make_in_memory_immutable().await,
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

#[tokio::test]
async fn level_1_store_and_load_round_trip() {
    use lore_storage::mutable_store::MutableStore;
    let store: Arc<dyn MutableStore> = Arc::new(
        LocalMutableStore::new(
            None::<&Path>,
            MutableStoreSettings {
                initial_fan_out_level: 1,
                ..Default::default()
            },
            make_in_memory_immutable().await,
        )
        .await
        .unwrap(),
    );
    let partition = Partition::default();
    let mut key = Hash::default();
    // Set bytes that, at level 256, would route to bucket 0xAB; at level 1 must still route to bucket 0.
    key.data_mut()[0] = 0x10;
    key.data_mut()[1] = 0xAB;
    let value = Hash::from_u64(42);
    store
        .clone()
        .store(partition, key, value, KeyType::BranchMetadata)
        .await
        .unwrap();
    let loaded = store
        .clone()
        .load(partition, key, KeyType::BranchMetadata)
        .await
        .unwrap();
    assert_eq!(loaded, value);
}

/// At fan-out levels < 256 a single bucket holds entries spanning several bucket-byte
/// (`data[1]`) values, so within the bucket the full-hash sort orders entries primarily
/// by bucket byte and only secondarily by `data[2]` (the key-type byte). A binary
/// search that compares only `data[2]` can land on an entry whose bucket byte differs
/// from the target's and erroneously conclude no match exists, missing entries that
/// are actually present. The fix carves the bucket's `sorted_index` into one slice per
/// bucket-byte value before running the per-slice key-type search. This regression
/// test inserts one `Instance` and one `BranchMetadata` entry into the same bucket
/// at each level in the ladder and verifies `list(Instance)` returns the `Instance`
/// entry; phase two adds two more `Instance` entries plus a mix of filler entries
/// across the bucket's bucket-byte range and verifies all three `Instance` entries
/// are enumerated.
#[tokio::test]
async fn list_finds_typed_entries_at_each_fan_out_level() {
    use futures::StreamExt;
    use lore_storage::mutable_store::MutableStore;

    for &level in &[1usize, 32, 64, 128, 256] {
        let store: Arc<dyn MutableStore> = Arc::new(
            LocalMutableStore::new(
                None::<&Path>,
                MutableStoreSettings {
                    initial_fan_out_level: level,
                    ..Default::default()
                },
                make_in_memory_immutable().await,
            )
            .await
            .unwrap(),
        );

        let partition = Partition::default();
        let stride = 256 / level;

        // Phase 1: insert one Instance and one BranchMetadata in the same bucket and
        // verify list(Instance) finds the Instance. The simple two-entry shape is the
        // original failing case from the test_background_prune_during_clone smoke
        // flake.
        let d1_inst1 = 0u8;
        let d1_meta = if stride >= 2 { 1u8 } else { 0u8 };

        let mut k_inst1 = Hash::default();
        k_inst1.data_mut()[0] = 0x42;
        k_inst1.data_mut()[1] = d1_inst1;

        let mut k_meta = Hash::default();
        k_meta.data_mut()[0] = 0x42;
        k_meta.data_mut()[1] = d1_meta;
        if d1_inst1 == d1_meta {
            k_meta.data_mut()[3] = 1;
        }

        let v_inst1 = Hash::from_u64(1);
        let v_meta = Hash::from_u64(2);
        store
            .clone()
            .store(partition, k_inst1, v_inst1, KeyType::Instance)
            .await
            .unwrap();
        store
            .clone()
            .store(partition, k_meta, v_meta, KeyType::BranchMetadata)
            .await
            .unwrap();

        let mut stream = store
            .clone()
            .list(partition, KeyType::Instance)
            .await
            .unwrap();
        let mut found_phase1: Vec<(Hash, Hash)> = Vec::new();
        while let Some(item) = stream.next().await {
            found_phase1.push(item);
        }
        assert_eq!(
            found_phase1.len(),
            1,
            "level {level} phase 1: list(Instance) returned {} entries, expected 1",
            found_phase1.len()
        );
        assert_eq!(
            found_phase1[0].1, v_inst1,
            "level {level} phase 1: wrong value returned"
        );

        // Phase 2: insert two more Instance entries plus a mix of non-Instance entries
        // — all into the same bucket. At fan-out levels < 256 the entries take distinct
        // bucket-byte values within the single bucket's range, exercising the per-slice
        // walk over scattered Instance entries. At level 256 only one bucket-byte value
        // routes to a given bucket, so the two extras share `data[1]` with the first
        // and are differentiated via `data[5]`; this exercises the within-bucket
        // `stride == 1` fast path with multiple Instance entries packed together.
        let (d1_inst2, d1_inst3) = if stride >= 2 {
            ((stride / 2) as u8, (stride - 1) as u8)
        } else {
            (0u8, 0u8)
        };

        let mut k_inst2 = Hash::default();
        k_inst2.data_mut()[0] = 0x42;
        k_inst2.data_mut()[1] = d1_inst2;
        k_inst2.data_mut()[5] = 1;

        let mut k_inst3 = Hash::default();
        k_inst3.data_mut()[0] = 0x42;
        k_inst3.data_mut()[1] = d1_inst3;
        k_inst3.data_mut()[5] = 2;

        let v_inst2 = Hash::from_u64(11);
        let v_inst3 = Hash::from_u64(12);
        store
            .clone()
            .store(partition, k_inst2, v_inst2, KeyType::Instance)
            .await
            .unwrap();
        store
            .clone()
            .store(partition, k_inst3, v_inst3, KeyType::Instance)
            .await
            .unwrap();

        let other_kts = [
            KeyType::BranchMetadata,
            KeyType::BranchId,
            KeyType::BranchLatestPointer,
            KeyType::RepositoryMetadata,
            KeyType::RepositoryId,
        ];
        let filler_d1_max = stride.min(8);
        let mut counter: u64 = 100;
        for d1_idx in 0..filler_d1_max {
            let d1 = d1_idx as u8;
            for &kt in &other_kts {
                let mut k = Hash::default();
                k.data_mut()[0] = 0x42;
                k.data_mut()[1] = d1;
                k.data_mut()[6] = (counter & 0xff) as u8;
                k.data_mut()[7] = ((counter >> 8) & 0xff) as u8;
                store
                    .clone()
                    .store(partition, k, Hash::from_u64(counter), kt)
                    .await
                    .unwrap();
                counter += 1;
            }
        }

        // Phase 3: list(Instance) must return all three Instance entries despite the
        // filler entries scattered through the bucket.
        let mut stream = store
            .clone()
            .list(partition, KeyType::Instance)
            .await
            .unwrap();
        let mut found_phase3: Vec<Hash> = Vec::new();
        while let Some((_k, v)) = stream.next().await {
            found_phase3.push(v);
        }
        found_phase3.sort();
        let mut expected = vec![v_inst1, v_inst2, v_inst3];
        expected.sort();
        assert_eq!(
            found_phase3, expected,
            "level {level} phase 3: expected three Instance entries, got {found_phase3:?}"
        );

        // Phase 4 (fan-out levels > 1 only): populate two additional buckets — bucket 5
        // and bucket 10 — each with one Instance entry plus filler entries spanning the
        // full bucket-byte sub-range of that bucket. This exercises cross-bucket
        // enumeration AND the per-slice walk inside each non-zero bucket: at fan-out
        // levels < 256 the new buckets each hold entries with `stride` distinct
        // bucket-byte values, so finding the Instance still requires walking past
        // non-matching slices. Skipped at level 1 because only bucket 0 exists.
        if level > 1 {
            let extra_buckets = [5usize, 10usize];
            let mut extra_inst_values: Vec<Hash> = Vec::new();
            for (next_inst_value, &bucket_idx) in (13u64..).zip(extra_buckets.iter()) {
                let d1_lo = bucket_idx * stride;
                let d1_hi = d1_lo + stride;

                let mut k_inst_extra = Hash::default();
                k_inst_extra.data_mut()[0] = 0x42;
                k_inst_extra.data_mut()[1] = d1_lo as u8;
                let v_inst_extra = Hash::from_u64(next_inst_value);
                store
                    .clone()
                    .store(partition, k_inst_extra, v_inst_extra, KeyType::Instance)
                    .await
                    .unwrap();
                extra_inst_values.push(v_inst_extra);

                for d1_value in d1_lo..d1_hi {
                    for &kt in &other_kts {
                        let mut k = Hash::default();
                        k.data_mut()[0] = 0x42;
                        k.data_mut()[1] = d1_value as u8;
                        k.data_mut()[6] = (counter & 0xff) as u8;
                        k.data_mut()[7] = ((counter >> 8) & 0xff) as u8;
                        store
                            .clone()
                            .store(partition, k, Hash::from_u64(counter), kt)
                            .await
                            .unwrap();
                        counter += 1;
                    }
                }
            }

            let mut stream = store
                .clone()
                .list(partition, KeyType::Instance)
                .await
                .unwrap();
            let mut found_phase4: Vec<Hash> = Vec::new();
            while let Some((_k, v)) = stream.next().await {
                found_phase4.push(v);
            }
            found_phase4.sort();
            let mut expected = vec![v_inst1, v_inst2, v_inst3];
            expected.extend(extra_inst_values);
            expected.sort();
            assert_eq!(
                found_phase4,
                expected,
                "level {level} phase 4: expected {} Instance entries across multiple \
                     buckets, got {found_phase4:?}",
                expected.len()
            );
        }
    }
}
