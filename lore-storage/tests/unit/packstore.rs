// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT
use bytes::Bytes;
use lore_base::test_util::TempDir;
use lore_storage::packstore::PackStore;

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

    packstore.truncate(3).await.unwrap();

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

    packstore.truncate(1).await.unwrap();
    assert_eq!(std::fs::metadata(pack.join("1")).unwrap().len(), 0);
    packstore.truncate(2).await.unwrap();

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

    assert!(packstore.truncate(1).await.is_err());

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

    assert!(packstore.truncate(1).await.is_err());

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
