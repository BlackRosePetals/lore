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

/// A flush waits for a bucket write holding the group's flush lock, which may have claimed a
/// bucket's mark and not written its file yet: the group reads clean, and skipping it would let
/// the flush return with an entry acknowledged before it still unwritten.
#[tokio::test]
async fn a_flush_waits_for_a_bucket_write_holding_the_group() {
    use lore_storage::mutable_store::MutableStore;
    let dir = lore_base::test_util::TempDir::new("ms_flush_claimed_");
    let store = Arc::new(
        LocalMutableStore::new(
            Some(dir.path()),
            MutableStoreSettings {
                flush_delay_seconds: 3600,
                ..client_settings()
            },
            make_in_memory_immutable().await,
        )
        .await
        .expect("store opens"),
    );
    let keys = store_keys(&store, Partition::default(), 1).await;
    let group = &store.group[keys[0].data()[0] as usize];

    // A write that took the lock and claimed the mark, and has not written the file.
    let writing = group.flush_lock.clone().lock_owned().await;
    assert!(group.dirty[0].swap(false, atomic::Ordering::Relaxed));
    let flushing: Arc<dyn MutableStore> = store.clone();
    let mut flush = lore_base::lore_spawn!(async move { flushing.flush(false).await });
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(200), &mut flush)
            .await
            .is_err(),
        "the flush returned while a claimed bucket write held the group"
    );
    drop(writing);
    flush.await.expect("flush task").expect("flush");
}

/// A delayed flush that cannot write its group's level marker finishes and leaves the bucket
/// dirty. The next store into the bucket starts another, which writes it once the marker can be
/// written.
#[tokio::test]
async fn a_store_after_a_failed_delayed_flush_starts_another() {
    use lore_storage::mutable_store::MutableStore;
    let dir = lore_base::test_util::TempDir::new("ms_delayed_retry_");
    let store = Arc::new(
        LocalMutableStore::new(
            Some(dir.path()),
            MutableStoreSettings {
                flush_delay_seconds: 1,
                ..client_settings()
            },
            make_in_memory_immutable().await,
        )
        .await
        .expect("store opens"),
    );
    let dyn_store: Arc<dyn MutableStore> = store.clone();
    let store_key = |byte: u8| {
        let dyn_store = dyn_store.clone();
        async move {
            let mut key = Hash::default();
            key.data_mut()[0] = 7;
            key.data_mut()[1] = byte;
            dyn_store
                .store(
                    Partition::default(),
                    key,
                    Hash::from_u64(u64::from(byte) + 1),
                    KeyType::BranchMetadata,
                )
                .await
                .expect("store succeeds");
        }
    };
    let group_path = dir.path().join("mutable").join("index").join("07");
    let marker = group_path.join(lore_storage::local::fan_out::MARKER_FILENAME);
    let bucket_file = lore_storage::local::fan_out::bucket_path(&group_path, 0);
    let bucket = store.group[7].bucket(0).clone();
    std::fs::create_dir_all(marker.join("blocker")).expect("block the level marker");

    store_key(0).await;
    wait_for_delayed_flush(&bucket).await;
    assert!(store.group[7].dirty[0].load(atomic::Ordering::Relaxed));
    assert!(!bucket_file.exists());

    std::fs::remove_dir_all(&marker).expect("unblock the level marker");
    store_key(1).await;
    assert!(
        bucket.read().await.flush_pending(),
        "the store into a bucket a failed delayed flush left dirty started no delayed flush"
    );
    wait_for_delayed_flush(&bucket).await;
    assert!(!store.group[7].dirty[0].load(atomic::Ordering::Relaxed));
    assert!(bucket_file.exists() && marker.exists());
}

/// Wait until no delayed flush of `bucket` is pending, for at most 10 s.
async fn wait_for_delayed_flush(bucket: &tokio::sync::RwLock<MutableStoreBucket>) {
    for _ in 0..1000 {
        if !bucket.read().await.flush_pending() {
            return;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    panic!("the delayed flush did not finish");
}

/// A fan-out commit that fails after writing its `level.pending` is one the next open's recovery
/// completes, renaming its `.new` files over whatever is there. Until a flush rolls it forward, no
/// bucket file is written outside it: here a group's first commit fails at one rename, and a newer
/// value in that bucket, written and flushed since, must survive the recovery rather than be
/// renamed over by the commit's older file.
#[tokio::test]
async fn a_commit_failing_after_its_pending_marker_is_rolled_forward_before_any_write() {
    use lore_storage::mutable_store::MutableStore;
    let dir = lore_base::test_util::TempDir::new("ms_commit_tail_");
    let settings = || MutableStoreSettings {
        fan_out_threshold: 2,
        ..client_settings()
    };
    let key = |byte: u8| {
        let mut key = Hash::default();
        key.data_mut()[0] = 7;
        key.data_mut()[1] = byte;
        key
    };
    let group_path = dir.path().join("mutable").join("index").join("07");
    let level = lore_storage::local::fan_out::level_for(1, 6, 2);
    let bucket = lore_storage::local::fan_out::bucket_index_for(&key(0), level);
    let blocked = lore_storage::local::fan_out::bucket_path(&group_path, bucket);
    {
        let store = Arc::new(
            LocalMutableStore::new(
                Some(dir.path()),
                settings(),
                make_in_memory_immutable().await,
            )
            .await
            .expect("store opens"),
        );
        let dyn_store: Arc<dyn MutableStore> = store.clone();
        for byte in 0..6u8 {
            dyn_store
                .clone()
                .store(
                    Partition::default(),
                    key(byte),
                    Hash::from_u64(u64::from(byte) + 1),
                    KeyType::BranchMetadata,
                )
                .await
                .expect("store succeeds");
        }
        std::fs::create_dir_all(&blocked).expect("block the bucket's rename");
        std::fs::write(blocked.join("blocker"), b"x").expect("block the bucket's rename");
        assert!(
            dyn_store.clone().flush(false).await.is_err(),
            "the commit's rename fails"
        );
        std::fs::remove_dir_all(&blocked).expect("unblock the bucket");

        dyn_store
            .clone()
            .store(
                Partition::default(),
                key(0),
                Hash::from_u64(100),
                KeyType::BranchMetadata,
            )
            .await
            .expect("store the newer value");
        LocalMutableStore::flush_delayed(Arc::downgrade(&store), 7, bucket, 0)
            .await
            .expect("delayed flush joins");
        dyn_store.clone().flush(false).await.expect("flush");
    }

    let reopened: Arc<dyn MutableStore> = Arc::new(
        LocalMutableStore::new(
            Some(dir.path()),
            settings(),
            make_in_memory_immutable().await,
        )
        .await
        .expect("store reopens"),
    );
    assert_eq!(
        reopened
            .load(Partition::default(), key(0), KeyType::BranchMetadata)
            .await
            .expect("load"),
        Hash::from_u64(100),
        "recovery renamed the failed commit's older file over the value flushed since"
    );
}

/// A key in group `07` that is in bucket `bucket` once the group is at 32 buckets.
fn spread_key(bucket: u8) -> Hash {
    let mut key = Hash::default();
    key.data_mut()[0] = 7;
    key.data_mut()[1] = bucket << 3;
    key
}

/// Store a value in each of group `07`'s `buckets` at 32 buckets, `bucket + 1` in `bucket`. Six of
/// them in one bucket at level 1 fan the group out to 32 at its next flush, at a fan-out threshold
/// of 2.
async fn store_spread_keys(
    store: &Arc<dyn lore_storage::mutable_store::MutableStore>,
    buckets: std::ops::Range<u8>,
) {
    for bucket in buckets {
        store
            .clone()
            .store(
                Partition::default(),
                spread_key(bucket),
                Hash::from_u64(u64::from(bucket) + 1),
                KeyType::BranchMetadata,
            )
            .await
            .expect("store succeeds");
    }
}

/// A flush dropped part way through a fan-out commit, as a caller giving up on it drops it, leaves
/// the commit running to its end under the group's flush lock: once the lock is free, the group is
/// committed, and every entry reads back after a reopen. The flush here is the group's first,
/// dropped writing its second `.new` file.
#[tokio::test]
async fn a_flush_dropped_while_its_commit_writes_new_files_runs_the_commit_to_its_end() {
    use lore_storage::mutable_store::MutableStore;
    use lore_storage::test_util::PausePoint;
    let dir = lore_base::test_util::TempDir::new("ms_dropped_commit_");
    let settings = || MutableStoreSettings {
        fan_out_threshold: 2,
        flush_delay_seconds: 3600,
        ..client_settings()
    };
    {
        let local = Arc::new(
            LocalMutableStore::new(
                Some(dir.path()),
                settings(),
                make_in_memory_immutable().await,
            )
            .await
            .expect("store opens"),
        );
        let store: Arc<dyn MutableStore> = local.clone();
        store_spread_keys(&store, 0..6).await;

        let first = lore_storage::test_util::pause_at(PausePoint::Serializing);
        let flushing = store.clone();
        let flush = lore_base::lore_spawn!(async move { flushing.flush(false).await });
        first.reached.notified().await;
        let second = lore_storage::test_util::pause_at(PausePoint::Serializing);
        first.resume.notify_one();
        second.reached.notified().await;
        flush.abort();
        assert!(flush.await.is_err(), "the flush was dropped");
        second.resume.notify_one();

        let group = &local.group[7];
        drop(group.flush_lock.lock().await);
        assert_eq!(group.pending_level.load(atomic::Ordering::Relaxed), 0);
        assert_eq!(
            group.committed_level.load(atomic::Ordering::Relaxed),
            group.bucket_count.load(atomic::Ordering::Relaxed),
            "the dropped flush's commit ran to its end"
        );
        store.clone().flush(false).await.expect("the next flush");
    }

    let reopened: Arc<dyn MutableStore> = Arc::new(
        LocalMutableStore::new(
            Some(dir.path()),
            settings(),
            make_in_memory_immutable().await,
        )
        .await
        .expect("store reopens"),
    );
    for bucket in 0..6u8 {
        assert_eq!(
            reopened
                .clone()
                .load(
                    Partition::default(),
                    spread_key(bucket),
                    KeyType::BranchMetadata
                )
                .await
                .expect("load"),
            Hash::from_u64(u64::from(bucket) + 1),
            "value in bucket {bucket} after reopening"
        );
    }
}

/// A group directory that cannot be stat'ed at open, as on a transient I/O error, fails the open
/// rather than being taken for a group never written. Taken for one, the group was laid out at the
/// initial level, where lookups missed its bucket files, and a write then committed that level
/// over them. The next open reads the group at the level it was written at.
#[tokio::test]
async fn a_group_directory_that_cannot_be_read_fails_the_open() {
    use lore_storage::mutable_store::MutableStore;
    let dir = lore_base::test_util::TempDir::new("ms_group_stat_");
    let settings = || MutableStoreSettings {
        fan_out_threshold: 2,
        flush_delay_seconds: 3600,
        ..client_settings()
    };
    let level = {
        let store = Arc::new(
            LocalMutableStore::new(
                Some(dir.path()),
                settings(),
                make_in_memory_immutable().await,
            )
            .await
            .expect("store opens"),
        );
        let dyn_store: Arc<dyn MutableStore> = store.clone();
        store_spread_keys(&dyn_store, 0..6).await;
        dyn_store.flush(false).await.expect("flush");
        store.group[7].bucket_count.load(atomic::Ordering::Relaxed)
    };
    assert!(level > 1, "the group fanned out");

    lore_storage::test_util::fail_directory_stats(1);
    assert!(
        LocalMutableStore::new(
            Some(dir.path()),
            settings(),
            make_in_memory_immutable().await,
        )
        .await
        .is_err(),
        "an open that could not read a group directory went on as if the group were unwritten"
    );

    let reopened = Arc::new(
        LocalMutableStore::new(
            Some(dir.path()),
            settings(),
            make_in_memory_immutable().await,
        )
        .await
        .expect("store reopens"),
    );
    assert_eq!(
        reopened.group[7]
            .bucket_count
            .load(atomic::Ordering::Relaxed),
        level
    );
    let reopened: Arc<dyn MutableStore> = reopened;
    for bucket in 0..6u8 {
        assert_eq!(
            reopened
                .clone()
                .load(
                    Partition::default(),
                    spread_key(bucket),
                    KeyType::BranchMetadata
                )
                .await
                .expect("load"),
            Hash::from_u64(u64::from(bucket) + 1),
            "value in bucket {bucket} after reopening"
        );
    }
}

/// A flush dropped once its fan-out commit's `level.pending` is on disk, before the write returned
/// to the commit, leaves the commit running to its end under the group's flush lock, so a delayed
/// bucket write, which writes the bucket file directly, waits for it. Otherwise the commit left
/// `level.pending` and a `.new` file older than that bucket file on disk, and the next open rolled
/// the commit forward over it, losing what it held since. Here the commit is the group's first.
#[tokio::test]
async fn a_flush_dropped_while_its_commit_writes_level_pending_runs_the_commit_to_its_end() {
    use lore_storage::mutable_store::MutableStore;
    use lore_storage::test_util::PausePoint;
    let dir = lore_base::test_util::TempDir::new("ms_dropped_pending_");
    let settings = || MutableStoreSettings {
        fan_out_threshold: 2,
        flush_delay_seconds: 3600,
        ..client_settings()
    };
    {
        let store = Arc::new(
            LocalMutableStore::new(
                Some(dir.path()),
                settings(),
                make_in_memory_immutable().await,
            )
            .await
            .expect("store opens"),
        );
        let dyn_store: Arc<dyn MutableStore> = store.clone();
        store_spread_keys(&dyn_store, 0..6).await;

        let committing = lore_storage::test_util::pause_at(PausePoint::Committing);
        let flushing = dyn_store.clone();
        let flush = lore_base::lore_spawn!(async move { flushing.flush(false).await });
        committing.reached.notified().await;
        flush.abort();
        assert!(flush.await.is_err(), "the flush was dropped");
        committing.resume.notify_one();

        dyn_store
            .clone()
            .store(
                Partition::default(),
                spread_key(0),
                Hash::from_u64(100),
                KeyType::BranchMetadata,
            )
            .await
            .expect("store the newer value");
        LocalMutableStore::flush_delayed(Arc::downgrade(&store), 7, 0, 0)
            .await
            .expect("delayed flush joins");
        dyn_store.clone().flush(false).await.expect("flush");
    }

    let reopened: Arc<dyn MutableStore> = Arc::new(
        LocalMutableStore::new(
            Some(dir.path()),
            settings(),
            make_in_memory_immutable().await,
        )
        .await
        .expect("store reopens"),
    );
    for bucket in 0..6u8 {
        let expected = if bucket == 0 {
            100
        } else {
            u64::from(bucket) + 1
        };
        assert_eq!(
            reopened
                .clone()
                .load(
                    Partition::default(),
                    spread_key(bucket),
                    KeyType::BranchMetadata
                )
                .await
                .expect("load"),
            Hash::from_u64(expected),
            "value in bucket {bucket} after reopening"
        );
    }
}

/// After a group's second fan-out commit fails once its `level.pending` is on disk and the group is
/// flushed again, every entry reads back once the store reopens, and each bucket file of the group
/// holds only the entries of its own bucket at the committed level. The commit fails at the rename
/// of bucket 0, so that its level-1 file, holding every entry, stays until a roll-forward renames
/// the commit's file over it.
#[tokio::test]
async fn a_second_commit_failing_after_its_pending_marker_keeps_every_entry_in_its_bucket() {
    use lore_storage::mutable_store::MutableStore;
    let dir = lore_base::test_util::TempDir::new("ms_failed_fan_out_");
    let settings = || MutableStoreSettings {
        fan_out_threshold: 2,
        flush_delay_seconds: 3600,
        ..client_settings()
    };
    {
        let store: Arc<dyn MutableStore> = Arc::new(
            LocalMutableStore::new(
                Some(dir.path()),
                settings(),
                make_in_memory_immutable().await,
            )
            .await
            .expect("store opens"),
        );
        // Bucket 0 is left empty at 32 buckets, so that only the commit replaces its file.
        store_spread_keys(&store, 1..3).await;
        store.clone().flush(false).await.expect("flush at level 1");
        store_spread_keys(&store, 3..7).await;
        let blocked = format_bucket_path(&dir.path().join("mutable"), 7, 0);
        let level_one = dir.path().join("level_one_bucket");
        std::fs::rename(&blocked, &level_one).expect("hold the level-1 file");
        super::fail_commit_at_rename(&blocked, || store.clone().flush(false)).await;
        std::fs::rename(&level_one, &blocked).expect("restore the level-1 file");

        store_spread_keys(&store, 7..10).await;
        store.clone().flush(false).await.expect("the next flush");
    }

    let reopened = Arc::new(
        LocalMutableStore::new(
            Some(dir.path()),
            settings(),
            make_in_memory_immutable().await,
        )
        .await
        .expect("store reopens"),
    );
    let level = reopened.group[7]
        .committed_level
        .load(atomic::Ordering::Relaxed);
    let dyn_store: Arc<dyn MutableStore> = reopened;
    for bucket in 1..10u8 {
        assert_eq!(
            dyn_store
                .clone()
                .load(
                    Partition::default(),
                    spread_key(bucket),
                    KeyType::BranchMetadata
                )
                .await
                .expect("load"),
            Hash::from_u64(u64::from(bucket) + 1),
            "value in bucket {bucket} after reopening"
        );
    }
    assert_eq!(level, 32, "the group fanned out");
    for bucket_index in 0..BUCKET_COUNT {
        let path = format_bucket_path(&dir.path().join("mutable"), 7, bucket_index);
        if !path.exists() {
            continue;
        }
        let (_, entries, _) = MutableStoreBucket::deserialize_files(path, true)
            .await
            .expect("the bucket file reads");
        for entry in entries.iter() {
            assert_eq!(
                lore_storage::local::fan_out::bucket_index_for(&entry.key, level),
                bucket_index,
                "an entry in the file of bucket {bucket_index}"
            );
        }
    }
}

/// A delayed flush rolls a fan-out commit that failed once its `level.pending` was on disk forward
/// before it writes a bucket, and then writes the bucket.
#[tokio::test]
async fn a_delayed_flush_rolls_a_pending_commit_forward_before_it_writes_a_bucket() {
    use lore_storage::mutable_store::MutableStore;
    let dir = lore_base::test_util::TempDir::new("ms_delayed_pending_");
    let settings = || MutableStoreSettings {
        fan_out_threshold: 2,
        flush_delay_seconds: 3600,
        ..client_settings()
    };
    let pending = dir
        .path()
        .join("mutable")
        .join("index")
        .join("07")
        .join(lore_storage::local::fan_out::LEVEL_PENDING_FILENAME);
    {
        let store = Arc::new(
            LocalMutableStore::new(
                Some(dir.path()),
                settings(),
                make_in_memory_immutable().await,
            )
            .await
            .expect("store opens"),
        );
        let dyn_store: Arc<dyn MutableStore> = store.clone();
        store_spread_keys(&dyn_store, 0..6).await;
        let blocked = format_bucket_path(&dir.path().join("mutable"), 7, 0);
        super::fail_commit_at_rename(&blocked, || dyn_store.clone().flush(false)).await;

        for bucket in 0..32 {
            LocalMutableStore::flush_delayed(Arc::downgrade(&store), 7, bucket, 0)
                .await
                .expect("delayed flush joins");
        }

        let group = &store.group[7];
        assert_eq!(group.pending_level.load(atomic::Ordering::Relaxed), 0);
        assert_eq!(group.committed_level.load(atomic::Ordering::Relaxed), 32);
        assert!(!pending.exists(), "the commit left its level.pending");
        assert!(
            group
                .dirty
                .iter()
                .all(|dirty| !dirty.load(atomic::Ordering::Relaxed)),
            "a bucket was left dirty"
        );
    }

    let reopened: Arc<dyn MutableStore> = Arc::new(
        LocalMutableStore::new(
            Some(dir.path()),
            settings(),
            make_in_memory_immutable().await,
        )
        .await
        .expect("store reopens"),
    );
    for bucket in 0..6u8 {
        assert_eq!(
            reopened
                .clone()
                .load(
                    Partition::default(),
                    spread_key(bucket),
                    KeyType::BranchMetadata
                )
                .await
                .expect("load"),
            Hash::from_u64(u64::from(bucket) + 1),
            "value in bucket {bucket} after reopening"
        );
    }
}

/// A fan-out commit removes the `.new` files it did not write before it writes `level.pending`,
/// so that recovery, which renames every `.new` file it finds, cannot give a bucket the commit
/// left empty an older commit's file.
#[tokio::test]
async fn a_fan_out_commit_removes_new_files_it_did_not_write() {
    use lore_storage::mutable_store::MutableStore;
    let dir = lore_base::test_util::TempDir::new("ms_stale_new_");
    let store = Arc::new(
        LocalMutableStore::new(
            Some(dir.path()),
            MutableStoreSettings {
                fan_out_threshold: 4,
                ..client_settings()
            },
            make_in_memory_immutable().await,
        )
        .await
        .expect("store opens"),
    );
    let dyn_store: Arc<dyn MutableStore> = store.clone();
    let key = |bucket_byte: u8| {
        let mut key = Hash::default();
        key.data_mut()[0] = 7;
        key.data_mut()[1] = bucket_byte;
        key
    };
    let store_key = |byte: u8| {
        let dyn_store = dyn_store.clone();
        async move {
            dyn_store
                .store(
                    Partition::default(),
                    key(byte),
                    Hash::from_u64(u64::from(byte) + 1),
                    KeyType::BranchMetadata,
                )
                .await
                .expect("store succeeds");
        }
    };
    for byte in [0, 1] {
        store_key(byte).await;
    }
    dyn_store
        .clone()
        .flush(false)
        .await
        .expect("flush at level 1");
    let group_path = dir.path().join("mutable").join("index").join("07");
    let stale_path = lore_storage::local::fan_out::bucket_new_path(&group_path, 5);
    std::fs::copy(group_path.join("index_00"), &stale_path).expect("an interrupted commit's file");

    for byte in [2, 3, 4] {
        store_key(byte).await;
    }
    dyn_store
        .clone()
        .flush(false)
        .await
        .expect("flush fanning out");
    assert_eq!(
        store.group[7].bucket_count.load(atomic::Ordering::Relaxed),
        32,
        "the group fanned out"
    );
    assert!(
        !stale_path.exists(),
        "the commit left a .new file it did not write for recovery to rename"
    );
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
