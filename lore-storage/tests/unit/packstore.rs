// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use lore_base::test_util::TempDir;
use lore_storage::packstore::PackStore;
use lore_storage::packstore::PackfileSize;
use lore_storage::test_util::PausePoint;
use lore_storage::test_util::fail_directory_stats;
use lore_storage::test_util::fail_packfile_opens;
use lore_storage::test_util::fail_packfile_stats;
use lore_storage::test_util::fail_packfile_syncs;
use lore_storage::test_util::fail_packfile_writes;
use lore_storage::test_util::hold_packfile_write;
use lore_storage::test_util::limit_packfile_size;
use lore_storage::test_util::pause_at;

use crate::store_fixture::wait_until;

fn payload(byte: u8) -> Bytes {
    Bytes::from(vec![byte; 64])
}

/// A write goes to the lowest-numbered packfile that takes writes, so that writes fill low
/// packfiles first: reopened with packfiles 1 and 2, the store writes to packfile 1.
#[tokio::test]
async fn a_write_goes_to_the_lowest_writeable_packfile() {
    let dir = TempDir::new("packstore-lowest-");
    {
        let packstore = PackStore::new(Some(dir.to_path_buf()), 1, None);
        assert_eq!(packstore.store(payload(1)).await.unwrap().id, 1);
        packstore.stop_writes(&[1]).await.unwrap();
        assert_eq!(packstore.store(payload(2)).await.unwrap().id, 2);
    }

    let packstore = PackStore::new(Some(dir.to_path_buf()), 1, None);
    for byte in 3..8 {
        assert_eq!(packstore.store(payload(byte)).await.unwrap().id, 1);
    }
}

/// A packfile whose metadata cannot be read as the packfiles load, as after a transient I/O
/// error, fails the load rather than being left out. Left out, packfile 2 would be taken for one
/// never written, as packfile 1 takes writes and no other is opened, and every payload in it for
/// lost. The next load reads both.
#[tokio::test]
async fn a_failed_packfile_stat_fails_the_load_rather_than_dropping_the_packfile() {
    let dir = TempDir::new("packstore-stat-");
    {
        let packstore = PackStore::new(Some(dir.to_path_buf()), 1, None);
        packstore.store(payload(1)).await.unwrap();
        packstore.stop_writes(&[1]).await.unwrap();
        assert_eq!(packstore.store(payload(2)).await.unwrap().id, 2);
    }

    let packstore = PackStore::new(Some(dir.to_path_buf()), 1, None);
    fail_packfile_stats(1);
    assert!(packstore.sizes().await.is_err());
    assert_eq!(packstore.sizes().await.unwrap(), vec![64, 64]);
}

/// A packstore directory that cannot be stat'ed as the packfiles load fails the load, with the
/// error the stat met, rather than being taken for one missing and created. The next load reads
/// the packfiles.
#[tokio::test]
async fn a_failed_directory_stat_fails_the_load() {
    let dir = TempDir::new("packstore-dir-stat-");
    {
        let packstore = PackStore::new(Some(dir.to_path_buf()), 1, None);
        packstore.store(payload(1)).await.unwrap();
    }

    let packstore = PackStore::new(Some(dir.to_path_buf()), 1, None);
    fail_directory_stats(1);
    assert!(packstore.sizes().await.is_err());
    assert_eq!(packstore.sizes().await.unwrap(), vec![64]);
}

/// Opening the next packfile when one fills can fail, as when no descriptor is left, which leaves
/// none taking writes. The next write opens one, rather than every write failing until the store
/// is reopened.
#[tokio::test]
async fn a_write_after_a_failed_packfile_open_opens_one() {
    let dir = TempDir::new("packstore-refill-");
    let packstore = PackStore::new(Some(dir.to_path_buf()), 1, None);
    limit_packfile_size(100);
    assert_eq!(packstore.store(payload(1)).await.unwrap().id, 1);
    fail_packfile_opens(1);
    // Filling packfile 1 takes it off writes, and opening packfile 2 in its place fails.
    assert_eq!(packstore.store(payload(2)).await.unwrap().id, 1);

    assert_eq!(packstore.store(payload(3)).await.unwrap().id, 2);
    limit_packfile_size(0);
}

/// Truncating a packfile discards every empty packfile at the end, keeping the minimum count
/// open to writes: truncating packfile 3 discards packfiles 2 to 4, and as packfile 1 takes no
/// writes, a new packfile 2 opens in their place.
#[tokio::test]
async fn truncate_discards_the_empty_packfiles_at_the_end() {
    let dir = TempDir::new("packstore-truncate-");
    let packstore = PackStore::new(Some(dir.to_path_buf()), 1, None);
    packstore.store(payload(1)).await.unwrap();
    packstore.stop_writes(&[1]).await.unwrap();
    packstore.stop_writes(&[2]).await.unwrap();
    assert_eq!(packstore.store(payload(2)).await.unwrap().id, 3);
    packstore.stop_writes(&[3]).await.unwrap();
    assert_eq!(packstore.sizes().await.unwrap(), vec![64, 0, 64, 0]);

    packstore.truncate(3, ()).await.unwrap();

    assert_eq!(packstore.sizes().await.unwrap(), vec![64, 0]);
    assert_eq!(packstore.store(payload(3)).await.unwrap().id, 2);
}

/// A packfile that stopped taking writes takes them again once restarted. Restarting an id that
/// names no packfile does nothing.
#[tokio::test]
async fn a_restarted_packfile_takes_writes_again() {
    let dir = TempDir::new("packstore-restart-");
    let packstore = PackStore::new(Some(dir.to_path_buf()), 1, None);
    packstore.store(payload(1)).await.unwrap();
    packstore.stop_writes(&[1]).await.unwrap();

    packstore.restart_writes(&[0, 1, 9]).await;

    assert_eq!(packstore.store(payload(2)).await.unwrap().id, 1);
    assert_eq!(packstore.sizes().await.unwrap().len(), 1);
}

/// Restarting packfiles discards the empty packfile opened when they stopped taking writes.
#[tokio::test]
async fn restarting_discards_the_empty_packfile_at_the_end() {
    let dir = TempDir::new("packstore-restart-tail-");
    let packstore = PackStore::new(Some(dir.to_path_buf()), 1, None);
    packstore.store(payload(1)).await.unwrap();
    assert_eq!(packstore.stop_writes(&[1]).await.unwrap(), vec![64]);
    assert_eq!(packstore.sizes().await.unwrap(), vec![64, 0]);

    packstore.restart_writes(&[1]).await;

    assert_eq!(packstore.sizes().await.unwrap(), vec![64]);
}

/// Resuming a packstore whose packfiles are loaded loads none again, though none takes writes.
#[tokio::test]
async fn resume_loads_the_packfiles_once() {
    let dir = TempDir::new("packstore-resume-");
    PackStore::new(Some(dir.to_path_buf()), 1, None)
        .store(payload(1))
        .await
        .unwrap();

    let packstore = PackStore::new(Some(dir.to_path_buf()), 0, None);
    packstore.stop_writes(&[1]).await.unwrap();
    packstore.resume().await.unwrap();

    assert_eq!(packstore.sizes().await.unwrap(), vec![64]);
}

/// The directory a packstore over `dir` keeps its packfiles in, holding a file for each of
/// `files`, packfile `n + 1` holding `files[n]`.
fn packfiles_on_disk(dir: &TempDir, files: &[&[u8]]) -> std::path::PathBuf {
    let pack = dir.join("pack");
    std::fs::create_dir_all(&pack).unwrap();
    for (index, bytes) in files.iter().enumerate() {
        std::fs::write(pack.join((index + 1).to_string()), bytes).unwrap();
    }
    pack
}

/// A load that fails part way leaves no packfile loaded, so that the next call loads them all
/// rather than treating the first ones as every packfile there is.
#[tokio::test]
async fn a_load_that_fails_part_way_loads_nothing() {
    let dir = TempDir::new("packstore-failed-load-");
    let pack = packfiles_on_disk(&dir, &[&[1; 64], &[2; 64], &[3; 64]]);
    let second = pack.join("2");
    let aside = pack.join("2.aside");
    std::fs::rename(&second, &aside).unwrap();
    std::fs::create_dir(&second).unwrap();
    let packstore = PackStore::new(Some(dir.to_path_buf()), 1, None);

    assert!(packstore.resume().await.is_err());
    assert!(packstore.sizes().await.is_err());

    std::fs::remove_dir(&second).unwrap();
    std::fs::rename(&aside, &second).unwrap();
    assert_eq!(packstore.sizes().await.unwrap(), vec![64, 64, 64]);
    assert_eq!(std::fs::read(&second).unwrap(), vec![2; 64]);
    assert_eq!(std::fs::read(pack.join("3")).unwrap(), vec![3; 64]);
}

/// Reopening packfiles to writes before the packstore is loaded leaves the packfiles on disk as
/// they are.
#[tokio::test]
async fn restarting_writes_before_loading_leaves_the_packfiles_alone() {
    let dir = TempDir::new("packstore-restart-unloaded-");
    let pack = packfiles_on_disk(&dir, &[&[1; 64]]);
    let packstore = PackStore::new(Some(dir.to_path_buf()), 1, None);

    packstore.restart_writes(&[1]).await;

    assert_eq!(std::fs::read(pack.join("1")).unwrap(), vec![1; 64]);
    assert_eq!(packstore.load(1, 0, 64).await.unwrap(), payload(1));
}

/// A packfile opened to writes over a file already at its path keeps the file's bytes and takes
/// writes after them.
#[tokio::test]
async fn a_new_packfile_keeps_a_file_already_at_its_path() {
    let dir = TempDir::new("packstore-stray-file-");
    let packstore = PackStore::new(Some(dir.to_path_buf()), 1, None);
    packstore.store(payload(1)).await.unwrap();
    std::fs::write(dir.join("pack").join("2"), [2; 64]).unwrap();

    packstore.stop_writes(&[1]).await.unwrap();
    let stored = packstore.store(payload(3)).await.unwrap();

    assert_eq!((stored.id, stored.offset), (2, 64));
    assert_eq!(packstore.load(2, 0, 64).await.unwrap(), payload(2));
    assert_eq!(packstore.load(2, 64, 64).await.unwrap(), payload(3));
}

/// Truncating a packfile empties its file, and the packfiles discarded at the end lose their
/// files: truncating packfiles 1 and 2, with packfile 3 empty, leaves packfile 1 empty on disk.
#[tokio::test]
async fn truncating_empties_or_removes_the_packfile_files() {
    let dir = TempDir::new("packstore-truncate-disk-");
    let pack = dir.join("pack");
    let packstore = PackStore::new(Some(dir.to_path_buf()), 1, None);
    packstore.store(payload(1)).await.unwrap();
    packstore.stop_writes(&[1]).await.unwrap();
    assert_eq!(packstore.store(payload(2)).await.unwrap().id, 2);
    packstore.stop_writes(&[2]).await.unwrap();

    packstore.truncate(1, ()).await.unwrap();
    assert_eq!(std::fs::metadata(pack.join("1")).unwrap().len(), 0);
    packstore.truncate(2, ()).await.unwrap();

    assert_eq!(packstore.sizes().await.unwrap(), vec![0]);
    assert_eq!(std::fs::metadata(pack.join("1")).unwrap().len(), 0);
    assert!(!pack.join("2").exists());
    assert!(!pack.join("3").exists());
}

/// A truncate that fails is reported and leaves the packfile out of writes, rather than taking
/// writes into a file that still holds its bytes. A FIFO at the packfile's path opens for reading
/// and writing but cannot be truncated.
#[cfg(target_os = "linux")]
#[tokio::test]
async fn a_failed_truncate_is_reported_and_takes_no_writes() {
    let dir = TempDir::new("packstore-truncate-fails-");
    let pack = packfiles_on_disk(&dir, &[&[1; 64], &[2; 64]]);
    std::fs::remove_file(pack.join("1")).unwrap();
    let made = std::process::Command::new("mkfifo")
        .arg(pack.join("1"))
        .status()
        .unwrap();
    assert!(made.success());
    let packstore = PackStore::new(Some(dir.to_path_buf()), 1, None);
    packstore.stop_writes(&[1]).await.unwrap();

    assert!(packstore.truncate(1, ()).await.is_err());

    assert_ne!(packstore.store(payload(3)).await.unwrap().id, 1);
}

/// Stopping writes with an id that names no packfile fails and stops none of them.
#[tokio::test]
async fn stopping_writes_with_an_invalid_id_stops_none() {
    let dir = TempDir::new("packstore-stop-invalid-");
    let packstore = PackStore::new(Some(dir.to_path_buf()), 1, None);
    packstore.store(payload(1)).await.unwrap();

    assert!(packstore.stop_writes(&[1, 9]).await.is_err());

    assert_eq!(packstore.store(payload(2)).await.unwrap().id, 1);
}

/// A packfile that takes writes is not truncated.
#[tokio::test]
async fn a_writeable_packfile_is_not_truncated() {
    let dir = TempDir::new("packstore-truncate-writeable-");
    let packstore = PackStore::new(Some(dir.to_path_buf()), 1, None);
    packstore.store(payload(1)).await.unwrap();

    assert!(packstore.truncate(1, ()).await.is_err());

    assert_eq!(packstore.load(1, 0, 64).await.unwrap(), payload(1));
}

/// A packfile missing below the highest id on disk loads as an empty packfile, keeping the ids
/// of the packfiles after it.
#[tokio::test]
async fn a_missing_packfile_loads_empty() {
    let dir = TempDir::new("packstore-missing-");
    let pack = packfiles_on_disk(&dir, &[&[1; 64], &[2; 64], &[3; 64]]);
    std::fs::remove_file(pack.join("2")).unwrap();
    let packstore = PackStore::new(Some(dir.to_path_buf()), 1, None);

    assert_eq!(packstore.sizes().await.unwrap(), vec![64, 0, 64]);
    assert_eq!(packstore.load(3, 0, 64).await.unwrap(), payload(3));
}

/// Stores in flight together on one packfile each reserve their own range, so that none
/// overlaps another and each reads back.
#[tokio::test]
async fn concurrent_stores_take_disjoint_ranges() {
    let dir = TempDir::new("packstore-concurrent-");
    let packstore = PackStore::new(Some(dir.to_path_buf()), 1, None);
    let payloads: Vec<Bytes> = (0..32u8)
        .map(|byte| Bytes::from(vec![byte; 100 + byte as usize]))
        .collect();

    let stored = futures::future::join_all(
        payloads
            .iter()
            .map(|payload| packstore.store(payload.clone())),
    )
    .await;

    let mut ranges = vec![];
    for (payload, stored) in payloads.iter().zip(stored) {
        let stored = stored.unwrap();
        assert_eq!(stored.id, 1);
        let size = payload.len() as u32;
        assert_eq!(
            packstore.load(1, stored.offset, size).await.unwrap(),
            *payload
        );
        ranges.push((stored.offset, stored.offset + size));
    }
    ranges.sort_unstable();
    assert!(
        ranges.windows(2).all(|pair| pair[0].1 == pair[1].0),
        "the ranges are disjoint and leave no gap"
    );
    assert_eq!(
        packstore.sizes().await.unwrap(),
        vec![ranges.last().unwrap().1 as usize]
    );
}

/// A load from a packfile, and another write to it, do not wait for a write to it that is still in
/// flight.
#[tokio::test]
async fn a_load_and_a_write_go_on_during_a_write_to_their_packfile() {
    let dir = TempDir::new("packstore-load-during-write-");
    let packstore = Arc::new(PackStore::new(Some(dir.to_path_buf()), 1, None));
    let first = packstore.store(payload(1)).await.unwrap();

    let reserved = pause_at(PausePoint::Reserved);
    let writer = {
        let packstore = packstore.clone();
        lore_base::lore_spawn!(async move { packstore.store(payload(2)).await })
    };
    reserved.reached.notified().await;

    let loaded =
        tokio::time::timeout(Duration::from_secs(10), packstore.load(1, first.offset, 64)).await;
    let third = tokio::time::timeout(Duration::from_secs(10), packstore.store(payload(3))).await;
    reserved.resume.notify_one();
    assert_eq!(
        loaded
            .expect("the load does not wait for the write")
            .unwrap(),
        payload(1)
    );
    let third = third
        .expect("the other write does not wait for the write")
        .unwrap();
    assert_eq!((third.id, third.offset), (1, 128));
    let second = writer.await.unwrap().unwrap();
    assert_eq!((second.id, second.offset), (1, 64));
    assert_eq!(packstore.load(1, 64, 64).await.unwrap(), payload(2));
    assert_eq!(packstore.load(1, 128, 64).await.unwrap(), payload(3));
}

/// A sync that claims the dirty mark while a write is in flight does not cover the write, so the
/// packfile is dirty again once the write lands, for the next sync to cover it.
#[tokio::test]
async fn a_write_landing_after_a_sync_leaves_the_packfile_dirty() {
    let dir = TempDir::new("packstore-dirty-after-sync-");
    let packstore = Arc::new(PackStore::new(Some(dir.to_path_buf()), 1, None));
    packstore.store(payload(1)).await.unwrap();
    packstore.flush(1, true).await.unwrap();
    assert!(!packstore.is_dirty(1).await);

    let reserved = pause_at(PausePoint::Reserved);
    let writer = {
        let packstore = packstore.clone();
        lore_base::lore_spawn!(async move { packstore.store(payload(2)).await })
    };
    reserved.reached.notified().await;
    packstore.flush(1, true).await.unwrap();
    reserved.resume.notify_one();
    writer.await.unwrap().unwrap();

    assert!(
        packstore.is_dirty(1).await,
        "the write is left for the next sync"
    );
}

/// A failed write gives back the range it reserved, so the next write takes it.
#[tokio::test]
async fn a_failed_write_gives_its_range_back() {
    let dir = TempDir::new("packstore-failed-write-");
    let packstore = PackStore::new(Some(dir.to_path_buf()), 1, None);
    packstore.store(payload(1)).await.unwrap();

    fail_packfile_writes(1);
    assert!(packstore.store(payload(2)).await.is_err());

    assert_eq!(packstore.sizes().await.unwrap(), vec![64]);
    let stored = packstore.store(payload(3)).await.unwrap();
    assert_eq!((stored.id, stored.offset), (1, 64));
    assert_eq!(packstore.load(1, 64, 64).await.unwrap(), payload(3));
}

/// A failed write with another range reserved after it keeps its range, as a hole, so that the
/// later write stays where it landed.
#[tokio::test]
async fn a_failed_write_before_another_leaves_a_hole() {
    let dir = TempDir::new("packstore-failed-write-hole-");
    let packstore = Arc::new(PackStore::new(Some(dir.to_path_buf()), 1, None));

    let reserved = pause_at(PausePoint::Reserved);
    let failing = {
        let packstore = packstore.clone();
        lore_base::lore_spawn!(async move { packstore.store(payload(1)).await })
    };
    reserved.reached.notified().await;
    let later = packstore.store(payload(2)).await.unwrap();
    assert_eq!((later.id, later.offset), (1, 64));

    fail_packfile_writes(1);
    reserved.resume.notify_one();
    assert!(failing.await.unwrap().is_err());

    assert_eq!(packstore.sizes().await.unwrap(), vec![128]);
    assert_eq!(packstore.load(1, 64, 64).await.unwrap(), payload(2));
}

/// A packfile at the size limit takes no reservation, even before it is marked full: with a write
/// reserved across the limit still in flight, the next write goes to a new packfile rather than
/// further past the limit. The packfile is a sparse file just under the limit.
#[cfg(target_os = "linux")]
#[tokio::test]
async fn a_packfile_at_the_size_limit_takes_no_reservation() {
    const LIMIT: u64 = 3 * 1024 * 1024 * 1024;
    let dir = TempDir::new("packstore-size-limit-");
    let pack = dir.join("pack");
    std::fs::create_dir_all(&pack).unwrap();
    std::fs::File::create(pack.join("1"))
        .unwrap()
        .set_len(LIMIT - 32)
        .unwrap();
    let packstore = Arc::new(PackStore::new(Some(dir.to_path_buf()), 1, None));

    let reserved = pause_at(PausePoint::Reserved);
    let crossing = {
        let packstore = packstore.clone();
        lore_base::lore_spawn!(async move { packstore.store(payload(1)).await })
    };
    reserved.reached.notified().await;
    let next = {
        let packstore = packstore.clone();
        lore_base::lore_spawn!(async move { packstore.store(payload(2)).await })
    };
    wait_until(|| packstore.list_change_pending(), &next).await;
    assert!(
        !next.is_finished(),
        "the next write marks packfile 1 full, behind the crossing write"
    );
    reserved.resume.notify_one();

    let crossing = crossing.await.unwrap().unwrap();
    assert_eq!((crossing.id, u64::from(crossing.offset)), (1, LIMIT - 32));
    let next = next.await.unwrap().unwrap();
    assert_eq!((next.id, next.offset), (2, 0));
}

/// A packfile held in memory stops taking writes at the size limit, as one on disk does: the write
/// that reaches the limit lands in it, and the next goes to a new packfile, rather than past the
/// limit, where its offset would no longer fit an entry's 32 bits.
#[tokio::test]
async fn a_packfile_in_memory_stops_at_the_size_limit() {
    limit_packfile_size(128);
    let packstore = PackStore::new(None, 1, None);

    let mut stored = vec![];
    for byte in 1..=3 {
        stored.push((packstore.store(payload(byte)).await.unwrap(), byte));
    }
    limit_packfile_size(0);

    let locations: Vec<_> = stored.iter().map(|(at, _)| (at.id, at.offset)).collect();
    assert_eq!(locations, vec![(1, 0), (1, 64), (2, 0)]);
    for (at, byte) in stored {
        assert_eq!(
            packstore.load(at.id, at.offset, 64).await.unwrap(),
            payload(byte)
        );
    }
}

/// A write abandoned in flight holds the packfile list until the driver is done with its buffer,
/// so that stopping writes to the packfile, which a truncate follows, waits for the write and not
/// for the store that issued it.
#[tokio::test]
async fn an_abandoned_write_holds_the_packfile_list_until_it_completes() {
    let dir = TempDir::new("packstore-abandoned-write-");
    let packstore = Arc::new(PackStore::new(Some(dir.to_path_buf()), 1, None));

    let in_flight = hold_packfile_write();
    let store = {
        let packstore = packstore.clone();
        lore_base::lore_spawn!(async move { packstore.store(payload(1)).await })
    };
    in_flight.issued.notified().await;
    store.abort();
    assert!(store.await.unwrap_err().is_cancelled());

    let stop = {
        let packstore = packstore.clone();
        lore_base::lore_spawn!(async move { packstore.stop_writes(&[1]).await })
    };
    wait_until(|| packstore.list_change_pending(), &stop).await;
    assert!(
        !stop.is_finished(),
        "stopping writes waits for the write in flight"
    );
    in_flight.complete();
    assert_eq!(stop.await.unwrap().unwrap(), vec![64]);
}

/// A truncate abandoned in flight keeps its completion until it lands, as the driver keeps a
/// truncate it was handed, so the share of its group's writes a store passes keeps the flock, and
/// the group's next flush, waiting until the packfile is empty.
#[tokio::test]
async fn an_abandoned_truncate_keeps_its_completion_until_it_lands() {
    let dir = TempDir::new("packstore-abandoned-truncate-");
    let packstore = Arc::new(PackStore::new(Some(dir.to_path_buf()), 1, None));
    packstore.store(payload(1)).await.unwrap();
    packstore.stop_writes(&[1]).await.unwrap();
    let writes: Arc<tokio::sync::RwLock<()>> = Arc::default();

    let in_flight = lore_storage::test_util::hold_store_write();
    let truncate = {
        let packstore = packstore.clone();
        let share = Arc::clone(&writes).read_owned().await;
        lore_base::lore_spawn!(async move { packstore.truncate(1, share).await })
    };
    in_flight.issued.notified().await;
    truncate.abort();
    assert!(truncate.await.unwrap_err().is_cancelled());
    assert!(
        writes.try_write().is_err(),
        "the abandoned truncate keeps its completion"
    );

    in_flight.complete();
    assert!(writes.try_write().is_ok(), "until it lands");
}

/// Obliterating a range overwrites it with zeros and syncs the packfile, leaving the payloads
/// beside it as they were.
#[tokio::test]
async fn obliterating_a_range_zeroes_and_syncs_it() {
    let dir = TempDir::new("packstore-obliterate-");
    let packstore = PackStore::new(Some(dir.to_path_buf()), 1, None);
    let doomed = packstore.store(payload(1)).await.unwrap();
    let kept = packstore.store(payload(2)).await.unwrap();

    packstore.obliterate(1, doomed.offset, 64).await.unwrap();

    assert_eq!(
        packstore.load(1, doomed.offset, 64).await.unwrap(),
        Bytes::from(vec![0; 64])
    );
    assert_eq!(
        packstore.load(1, kept.offset, 64).await.unwrap(),
        payload(2)
    );
    assert!(!packstore.is_dirty(1).await, "the zeros are synced");
}

/// A failed sync leaves the packfile dirty and fails every later sync of it, as a later sync could
/// succeed without the writes the failed one dropped, and takes the packfile out of writes.
#[tokio::test]
async fn a_failed_sync_fails_every_later_sync_of_the_packfile_and_stops_its_writes() {
    let dir = TempDir::new("packstore-failed-sync-");
    let packstore = PackStore::new(Some(dir.to_path_buf()), 1, None);
    assert_eq!(packstore.store(payload(1)).await.unwrap().id, 1);

    fail_packfile_syncs(1);
    assert!(packstore.flush_all(true).await.is_err());
    assert!(
        packstore.flush_all(true).await.is_err(),
        "a later sync fails too"
    );
    assert!(packstore.is_dirty(1).await);
    assert!(!packstore.writeable_ids().await.contains(&1));
    assert_eq!(packstore.store(payload(2)).await.unwrap().id, 2);
}

/// A failed sync of one packfile, as a flush of it or an obliterate in it runs, takes the packfile
/// out of writes.
#[tokio::test]
async fn a_failed_sync_of_one_packfile_stops_its_writes() {
    let dir = TempDir::new("packstore-failed-flush-");
    let packstore = PackStore::new(Some(dir.to_path_buf()), 1, None);
    packstore.store(payload(1)).await.unwrap();
    fail_packfile_syncs(1);
    assert!(packstore.flush(1, true).await.is_err());
    assert!(!packstore.writeable_ids().await.contains(&1));

    assert_eq!(packstore.store(payload(2)).await.unwrap().id, 2);
    fail_packfile_syncs(1);
    packstore.obliterate(2, 0, 64).await.unwrap();
    assert!(!packstore.writeable_ids().await.contains(&2));
}

/// Restarting writes leaves out a packfile whose sync failed.
#[tokio::test]
async fn restarting_writes_leaves_out_a_packfile_whose_sync_failed() {
    let dir = TempDir::new("packstore-restart-failed-");
    let packstore = PackStore::new(Some(dir.to_path_buf()), 1, None);
    packstore.store(payload(1)).await.unwrap();
    packstore.stop_writes(&[1]).await.unwrap();
    fail_packfile_syncs(1);
    assert!(packstore.flush(1, true).await.is_err());

    packstore.restart_writes(&[1]).await;
    assert!(!packstore.writeable_ids().await.contains(&1));
}

/// A packfile that fills is synced and then settled, so that a bucket file need not record its
/// size, while the packfile opened in its place is recorded. The packfile is a sparse file just
/// under the limit.
#[cfg(target_os = "linux")]
#[tokio::test]
async fn a_packfile_that_fills_is_synced_and_settled() {
    const LIMIT: u64 = 3 * 1024 * 1024 * 1024;
    let dir = TempDir::new("packstore-settle-");
    let pack = packfiles_on_disk(&dir, &[]);
    std::fs::File::create(pack.join("1"))
        .unwrap()
        .set_len(LIMIT - 32)
        .unwrap();
    let packstore = PackStore::new(Some(dir.to_path_buf()), 1, None);
    assert_eq!(packstore.store(payload(1)).await.unwrap().id, 1);

    for _ in 0..10_000 {
        if packstore.is_settled(1).await {
            break;
        }
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
    assert!(packstore.is_settled(1).await);
    assert!(!packstore.is_dirty(1).await, "the sync covers the write");
    let mut sizes = [PackfileSize::default(); 4];
    assert_eq!(packstore.unsettled_sizes(&mut sizes).await, Some(1));
    assert_eq!(sizes[0], PackfileSize { id: 2, size: 0 });
}

/// A packfile filled and closed before its settling sync took the packfile list reopens dirty and
/// not settled, so that bucket files go on recording its size and the next flush syncs it.
#[tokio::test]
async fn a_packfile_closed_before_it_settled_reopens_unsettled() {
    const LIMIT: u64 = 3 * 1024 * 1024 * 1024;
    let dir = TempDir::new("packstore-closed-unsettled-");
    let pack = packfiles_on_disk(&dir, &[]);
    std::fs::File::create(pack.join("1"))
        .unwrap()
        .set_len(LIMIT - 32)
        .unwrap();
    let packstore = PackStore::new(Some(dir.to_path_buf()), 1, None);
    assert_eq!(packstore.store(payload(1)).await.unwrap().id, 1);
    assert!(
        packstore.release_files(),
        "closed before the settling sync ran"
    );

    packstore.resume().await.unwrap();
    assert!(!packstore.is_settled(1).await);
    assert!(packstore.is_dirty(1).await);
    let mut sizes = [PackfileSize::default(); 4];
    let count = packstore.unsettled_sizes(&mut sizes).await.unwrap();
    assert!(sizes[..count].contains(&PackfileSize {
        id: 1,
        size: (LIMIT + 32) as u32
    }));
}

/// A packfile opened at the size limit is taken to be settled, until it is truncated.
#[cfg(target_os = "linux")]
#[tokio::test]
async fn a_packfile_opened_at_the_size_limit_is_settled() {
    const LIMIT: u64 = 3 * 1024 * 1024 * 1024;
    let dir = TempDir::new("packstore-opened-full-");
    let pack = packfiles_on_disk(&dir, &[]);
    std::fs::File::create(pack.join("1"))
        .unwrap()
        .set_len(LIMIT)
        .unwrap();
    let packstore = PackStore::new(Some(dir.to_path_buf()), 1, None);
    packstore.resume().await.unwrap();

    let mut sizes = [PackfileSize::default(); 4];
    assert_eq!(packstore.unsettled_sizes(&mut sizes).await, Some(1));
    assert_eq!(sizes[0], PackfileSize { id: 2, size: 0 });

    packstore.truncate(1, ()).await.unwrap();
    assert_eq!(packstore.unsettled_sizes(&mut sizes).await, Some(1));
    assert_eq!(sizes[0], PackfileSize { id: 1, size: 0 });
}

/// The sizes of the packfiles not settled are the bytes reserved in them, and a slice too short
/// to hold them gets none.
#[tokio::test]
async fn unsettled_sizes_are_the_bytes_reserved_in_the_packfiles_not_settled() {
    let dir = TempDir::new("packstore-unsettled-");
    packfiles_on_disk(&dir, &[&[1; 64], &[2; 32]]);
    let packstore = PackStore::new(Some(dir.to_path_buf()), 1, None);
    assert_eq!(packstore.store(payload(3)).await.unwrap().id, 1);

    let mut sizes = [PackfileSize::default(); 2];
    assert_eq!(packstore.unsettled_sizes(&mut sizes).await, Some(2));
    assert_eq!(
        sizes,
        [
            PackfileSize { id: 1, size: 128 },
            PackfileSize { id: 2, size: 32 }
        ]
    );
    assert_eq!(packstore.unsettled_sizes(&mut sizes[..1]).await, None);
}

/// Packfiles not yet loaded are left out of the sizes, and stay unloaded: no write in the process
/// went to them. Loaded, the store would open a packfile to take writes.
#[tokio::test]
async fn unsettled_sizes_leave_out_packfiles_not_loaded() {
    let dir = TempDir::new("packstore-unloaded-");
    let pack = packfiles_on_disk(&dir, &[]);
    let packstore = PackStore::new(Some(dir.to_path_buf()), 1, None);

    let mut sizes = [PackfileSize::default(); 2];
    assert_eq!(packstore.unsettled_sizes(&mut sizes).await, Some(0));
    assert!(!pack.join("1").exists(), "no packfile is opened");
}

/// A packfile closed holding a write no sync covered keeps its size among the unsettled sizes
/// without being opened again, and the next flush that syncs opens and syncs it. Once synced, a
/// closed packfile is left out.
#[tokio::test]
async fn a_closed_packfile_keeps_its_unsettled_size_and_its_sync() {
    let dir = TempDir::new("packstore-released-");
    let packstore = PackStore::new(Some(dir.to_path_buf()), 1, None);
    assert_eq!(packstore.store(payload(1)).await.unwrap().id, 1);
    packstore.release_files();

    let mut sizes = [PackfileSize::default(); 2];
    assert_eq!(packstore.unsettled_sizes(&mut sizes).await, Some(1));
    assert_eq!(sizes[0], PackfileSize { id: 1, size: 64 });
    assert_eq!(
        packstore.open_packfiles().await,
        0,
        "and no packfile is opened"
    );

    packstore.flush_all(true).await.unwrap();
    assert!(!packstore.is_dirty(1).await, "the flush synced it");
    packstore.release_files();
    assert_eq!(packstore.unsettled_sizes(&mut sizes).await, Some(0));
}

/// A packfile shorter when opened than recorded, or missing, is short of the recorded size, and a
/// truncated packfile counts as opened empty.
#[tokio::test]
async fn a_packfile_shorter_when_opened_than_recorded_is_short_of_it() {
    let dir = TempDir::new("packstore-short-");
    packfiles_on_disk(&dir, &[&[1; 64], &[2; 64]]);
    let packstore = PackStore::new(Some(dir.to_path_buf()), 1, None);
    assert_eq!(packstore.store(payload(3)).await.unwrap().id, 1);

    let recorded = |id, size| [PackfileSize { id, size }];
    assert!(!packstore.short_of(recorded(1, 64)).await.unwrap());
    assert!(
        packstore.short_of(recorded(1, 128)).await.unwrap(),
        "a write after opening does not count"
    );
    assert!(packstore.short_of(recorded(3, 1)).await.unwrap());
    assert!(!packstore.short_of(recorded(3, 0)).await.unwrap());
    assert_eq!(packstore.opened_lengths().await.unwrap(), vec![64, 64]);

    packstore.stop_writes(&[1]).await.unwrap();
    packstore.truncate(1, ()).await.unwrap();
    assert_eq!(packstore.opened_lengths().await.unwrap(), vec![0, 64]);
}
