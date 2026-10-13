// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT
use std::path::PathBuf;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;

use lore_storage::Hash;
use lore_storage::local::fan_out::*;
use tokio::sync::OwnedMutexGuard;

fn key_with_byte_one(byte: u8) -> Hash {
    let mut hash = Hash::default();
    hash.data_mut()[1] = byte;
    hash
}

#[test]
fn level_ladder_is_powers_of_two_starting_at_one() {
    assert_eq!(LEVEL_LADDER, [1, 32, 64, 128, 256]);
    for &m in &LEVEL_LADDER[1..] {
        assert!(m.is_power_of_two(), "level {m} must be power of two");
        assert!(
            m <= FAN_OUT_LEVEL_MAX,
            "level {m} must be <= FAN_OUT_LEVEL_MAX"
        );
    }
}

#[test]
fn fan_out_level_max_matches_top_of_ladder() {
    assert_eq!(FAN_OUT_LEVEL_MAX, LEVEL_LADDER[LEVEL_LADDER.len() - 1]);
}

#[test]
fn fan_out_threshold_default_is_one_thousand() {
    assert_eq!(FAN_OUT_THRESHOLD_DEFAULT, 1000);
}

#[test]
fn bucket_index_for_level_one_always_zero() {
    for byte in [0u8, 0x05, 0x80, 0xFF] {
        let key = key_with_byte_one(byte);
        assert_eq!(bucket_index_for(&key, 1), 0);
    }
}

#[test]
fn bucket_index_for_level_256_returns_full_byte() {
    for byte in [0u8, 0x05, 0x42, 0x80, 0xFF] {
        let key = key_with_byte_one(byte);
        assert_eq!(bucket_index_for(&key, 256), byte as usize);
    }
}

#[test]
fn bucket_index_for_level_32_takes_top_5_bits() {
    // byte = 0b101_01010 (0xAA): top 5 bits = 0b10101 = 21
    let key = key_with_byte_one(0xAA);
    assert_eq!(bucket_index_for(&key, 32), 21);
    // byte = 0xFF: top 5 bits = 0b11111 = 31
    let key = key_with_byte_one(0xFF);
    assert_eq!(bucket_index_for(&key, 32), 31);
    // byte = 0x00: 0
    let key = key_with_byte_one(0x00);
    assert_eq!(bucket_index_for(&key, 32), 0);
}

#[test]
fn bucket_index_for_level_64_takes_top_6_bits() {
    // byte = 0xAA = 0b10101010: top 6 bits = 0b101010 = 42
    let key = key_with_byte_one(0xAA);
    assert_eq!(bucket_index_for(&key, 64), 42);
}

#[test]
fn bucket_index_for_level_128_takes_top_7_bits() {
    // byte = 0xAA = 0b10101010: top 7 bits = 0b1010101 = 85
    let key = key_with_byte_one(0xAA);
    assert_eq!(bucket_index_for(&key, 128), 85);
}

#[test]
fn split_n_to_2n_preserves_bucket_membership_via_high_bit() {
    // For any byte, the bucket at level 2N is either 2*idx_N or 2*idx_N + 1.
    for byte in 0..=255u8 {
        let key = key_with_byte_one(byte);
        let idx_32 = bucket_index_for(&key, 32);
        let idx_64 = bucket_index_for(&key, 64);
        assert!(
            idx_64 == 2 * idx_32 || idx_64 == 2 * idx_32 + 1,
            "byte 0x{byte:02x}: idx_32={idx_32}, idx_64={idx_64}"
        );
    }
}

#[test]
fn level_for_below_threshold_is_current_level() {
    // b_max ≤ threshold ⇒ no fan-out required ⇒ current_level returned (still in ladder).
    assert_eq!(level_for(1, 800, 1000), 1);
    assert_eq!(level_for(1, 1000, 1000), 1);
    assert_eq!(level_for(32, 999, 1000), 32);
}

#[test]
fn level_for_5k_at_level_1_is_32() {
    // 1 * 5000 / 1000 = 5, smallest ladder M >= 5 is 32
    assert_eq!(level_for(1, 5000, 1000), 32);
}

#[test]
fn level_for_1500_at_level_32_is_64() {
    // 32 * 1500 / 1000 = 48, smallest ladder M >= 48 is 64
    assert_eq!(level_for(32, 1500, 1000), 64);
}

#[test]
fn level_for_1500_at_level_128_is_256() {
    // 128 * 1500 / 1000 = 192, smallest ladder M >= 192 is 256
    assert_eq!(level_for(128, 1500, 1000), 256);
}

#[test]
fn level_for_caps_at_256() {
    // Even an extreme b_max can't push us past 256.
    assert_eq!(level_for(128, 10_000, 1000), 256);
    assert_eq!(level_for(256, 10_000, 1000), 256);
}

#[test]
fn level_for_uses_ceiling_division() {
    // 1*1001/1000 ceiling is 2, smallest ladder M ≥ 2 is 32; floor would wrongly return current_level=1 even though trigger has fired.
    assert_eq!(level_for(1, 1001, 1000), 32);
}

fn temp_group_dir() -> lore_base::test_util::TempDir {
    lore_base::test_util::TempDir::new("fan_out_marker_")
}

#[tokio::test]
async fn read_level_marker_missing_returns_none() {
    let dir = temp_group_dir();
    assert_eq!(read_level_marker(dir.path()).await.unwrap(), None);
}

/// A held flush guard, standing in for the one a caller owns.
async fn held_flush_guard() -> OwnedMutexGuard<()> {
    std::sync::Arc::new(tokio::sync::Mutex::new(()))
        .lock_owned()
        .await
}

/// A store root whose group directory exists, ready for a marker write.
fn temp_store_with_group(group_index: usize) -> (lore_base::test_util::TempDir, PathBuf) {
    let dir = lore_base::test_util::TempDir::new("fan_out_commit_");
    let mut group_path = dir.path().to_path_buf();
    group_path.push("index");
    push_group_dir(&mut group_path, group_index);
    std::fs::create_dir_all(&group_path).expect("group dir");
    (dir, group_path)
}

#[tokio::test]
async fn commit_if_initial_level_records_the_level_below_the_maximum() {
    for &level in &LEVEL_LADDER[..LEVEL_LADDER.len() - 1] {
        let (dir, group_path) = temp_store_with_group(0x2a);
        let committed = AtomicUsize::new(0);
        let bucket_count = AtomicUsize::new(level);

        commit_if_initial_level(
            &held_flush_guard().await,
            &committed,
            &bucket_count,
            dir.path(),
            0x2a,
            false,
        )
        .await;

        assert_eq!(read_level_marker(&group_path).await.unwrap(), Some(level));
        assert_eq!(committed.load(Ordering::Relaxed), level);
    }
}

#[tokio::test]
async fn commit_if_initial_level_leaves_a_committed_group_alone() {
    let (dir, group_path) = temp_store_with_group(0x2a);
    let committed = AtomicUsize::new(32);
    let bucket_count = AtomicUsize::new(64);

    commit_if_initial_level(
        &held_flush_guard().await,
        &committed,
        &bucket_count,
        dir.path(),
        0x2a,
        false,
    )
    .await;

    assert_eq!(read_level_marker(&group_path).await.unwrap(), None);
    assert_eq!(committed.load(Ordering::Relaxed), 32);
}

#[tokio::test]
async fn commit_if_initial_level_leaves_a_group_at_the_maximum_alone() {
    let (dir, group_path) = temp_store_with_group(0x2a);
    let committed = AtomicUsize::new(0);
    let bucket_count = AtomicUsize::new(FAN_OUT_LEVEL_MAX);

    commit_if_initial_level(
        &held_flush_guard().await,
        &committed,
        &bucket_count,
        dir.path(),
        0x2a,
        false,
    )
    .await;

    assert_eq!(read_level_marker(&group_path).await.unwrap(), None);
    assert_eq!(committed.load(Ordering::Relaxed), 0);
}

#[tokio::test]
async fn write_then_read_round_trip_every_ladder_value() {
    for &level in &LEVEL_LADDER {
        let dir = temp_group_dir();
        write_level_marker(dir.path(), level, true).await.unwrap();
        assert_eq!(read_level_marker(dir.path()).await.unwrap(), Some(level));
    }
}

#[tokio::test]
async fn read_level_marker_with_corrupt_magic_errors() {
    let dir = temp_group_dir();
    let marker = dir.path().join(MARKER_FILENAME);
    std::fs::write(
        &marker,
        [0xDE, 0xAD, 0xBE, 0xEF, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],
    )
    .unwrap();
    let err = read_level_marker(dir.path()).await.unwrap_err();
    assert_eq!(err.kind(), std::io::ErrorKind::InvalidData);
}

#[tokio::test]
async fn read_level_marker_truncated_errors() {
    let dir = temp_group_dir();
    let marker = dir.path().join(MARKER_FILENAME);
    std::fs::write(&marker, [b'L', b'V', b'N', b'O', 1, 0, 0, 0]).unwrap();
    let err = read_level_marker(dir.path()).await.unwrap_err();
    assert_eq!(err.kind(), std::io::ErrorKind::UnexpectedEof);
}

#[tokio::test]
async fn read_level_marker_unsupported_version_errors() {
    let dir = temp_group_dir();
    let marker = dir.path().join(MARKER_FILENAME);
    let mut bytes = vec![];
    bytes.extend_from_slice(&MARKER_MAGIC.to_le_bytes());
    bytes.extend_from_slice(&999u32.to_le_bytes());
    bytes.extend_from_slice(&32u32.to_le_bytes());
    bytes.extend_from_slice(&0u32.to_le_bytes());
    std::fs::write(&marker, bytes).unwrap();
    let err = read_level_marker(dir.path()).await.unwrap_err();
    assert_eq!(err.kind(), std::io::ErrorKind::InvalidData);
}

#[tokio::test]
async fn write_level_marker_overwrites_existing() {
    let dir = temp_group_dir();
    write_level_marker(dir.path(), 1, false).await.unwrap();
    write_level_marker(dir.path(), 64, false).await.unwrap();
    assert_eq!(read_level_marker(dir.path()).await.unwrap(), Some(64));
}

/// A marker write without syncing replaces the marker's file rather than rewriting it, so that a
/// process stopped during the write leaves the marker it last wrote, as a hard link to it shows.
#[tokio::test]
async fn an_unsynced_marker_write_replaces_the_file() {
    let dir = temp_group_dir();
    let marker = dir.path().join(MARKER_FILENAME);
    write_level_marker(dir.path(), 1, false).await.unwrap();
    let written = std::fs::read(&marker).unwrap();
    let link = dir.path().join("last_written");
    std::fs::hard_link(&marker, &link).unwrap();

    write_level_marker(dir.path(), 64, false).await.unwrap();

    assert_eq!(read_level_marker(dir.path()).await.unwrap(), Some(64));
    assert_eq!(
        std::fs::read(&link).unwrap(),
        written,
        "the marker last written is left whole"
    );
}

#[tokio::test]
async fn marker_file_is_exactly_16_bytes() {
    let dir = temp_group_dir();
    write_level_marker(dir.path(), 32, false).await.unwrap();
    let metadata = std::fs::metadata(dir.path().join(MARKER_FILENAME)).unwrap();
    assert_eq!(metadata.len(), 16);
}

#[tokio::test]
async fn read_level_pending_missing_returns_none() {
    let dir = temp_group_dir();
    assert_eq!(read_level_pending(dir.path()).await.unwrap(), None);
}

#[tokio::test]
async fn level_pending_round_trip_every_ladder_value() {
    for &level in &LEVEL_LADDER {
        let dir = temp_group_dir();
        write_level_pending(dir.path(), level, false).await.unwrap();
        assert_eq!(read_level_pending(dir.path()).await.unwrap(), Some(level));
    }
}

#[tokio::test]
async fn delete_level_pending_is_idempotent() {
    let dir = temp_group_dir();
    // Delete on missing file succeeds.
    delete_level_pending(dir.path()).await.unwrap();
    // Delete after write removes it.
    write_level_pending(dir.path(), 32, false).await.unwrap();
    delete_level_pending(dir.path()).await.unwrap();
    assert_eq!(read_level_pending(dir.path()).await.unwrap(), None);
    // Second delete is also fine.
    delete_level_pending(dir.path()).await.unwrap();
}

#[test]
fn bucket_path_lowercase_two_digit_hex() {
    let dir = temp_group_dir();
    assert_eq!(
        bucket_path(dir.path(), 0).file_name().unwrap(),
        std::ffi::OsStr::new("index_00")
    );
    assert_eq!(
        bucket_path(dir.path(), 0xab).file_name().unwrap(),
        std::ffi::OsStr::new("index_ab")
    );
    assert_eq!(
        bucket_path(dir.path(), 255).file_name().unwrap(),
        std::ffi::OsStr::new("index_ff")
    );
}

#[test]
fn bucket_new_path_appends_dot_new() {
    let dir = temp_group_dir();
    assert_eq!(
        bucket_new_path(dir.path(), 0xab).file_name().unwrap(),
        std::ffi::OsStr::new("index_ab.new")
    );
}

#[test]
fn group_dir_path_lowercase_two_digit_hex() {
    let dir = temp_group_dir();
    assert_eq!(
        group_dir_path(dir.path(), 0).file_name().unwrap(),
        std::ffi::OsStr::new("00")
    );
    assert_eq!(
        group_dir_path(dir.path(), 255).file_name().unwrap(),
        std::ffi::OsStr::new("ff")
    );
}

#[test]
fn every_path_formatter_agrees_with_hex_formatting() {
    let dir = temp_group_dir();
    for index in 0..=255usize {
        let byte = index as u8;
        let mut hex = [0u8; 2];
        write_hex_byte(&mut hex, byte);
        assert_eq!(std::str::from_utf8(&hex).unwrap(), format!("{byte:02x}"));
        assert_eq!(
            group_dir_path(dir.path(), index).file_name().unwrap(),
            std::ffi::OsStr::new(&format!("{byte:02x}"))
        );
        assert_eq!(
            bucket_path(dir.path(), index).file_name().unwrap(),
            std::ffi::OsStr::new(&format!("{BUCKET_FILENAME_PREFIX}{byte:02x}"))
        );
        assert_eq!(
            bucket_new_path(dir.path(), index).file_name().unwrap(),
            std::ffi::OsStr::new(&format!(
                "{BUCKET_FILENAME_PREFIX}{byte:02x}{BUCKET_NEW_SUFFIX}"
            ))
        );
    }
}

/// A `.new` file recovery cannot rename keeps `level.pending`, so that the next open retries
/// rather than committing a level whose bucket file is missing.
#[tokio::test]
async fn recovery_keeps_the_pending_level_when_a_rename_fails() {
    let dir = temp_group_dir();
    write_level_pending(dir.path(), 32, false).await.unwrap();
    std::fs::write(bucket_new_path(dir.path(), 0), b"twin").unwrap();
    std::fs::create_dir(bucket_path(dir.path(), 0)).unwrap();
    std::fs::write(bucket_path(dir.path(), 0).join("blocker"), b"x").unwrap();

    assert!(recover_level_transition(dir.path(), false).await.is_err());

    assert_eq!(read_level_pending(dir.path()).await.unwrap(), Some(32));
    assert_eq!(read_level_marker(dir.path()).await.unwrap(), None);
}

#[tokio::test]
async fn group_has_buckets_missing_directory_is_false() {
    let dir = temp_group_dir();
    assert!(!group_has_buckets(&dir.path().join("absent")).await);
}

#[tokio::test]
async fn group_has_buckets_empty_directory_is_false() {
    let dir = temp_group_dir();
    assert!(!group_has_buckets(dir.path()).await);
}

#[tokio::test]
async fn group_has_buckets_level_files_alone_are_false() {
    let dir = temp_group_dir();
    write_level_marker(dir.path(), 32, false).await.unwrap();
    write_level_pending(dir.path(), 64, false).await.unwrap();
    assert!(!group_has_buckets(dir.path()).await);
}

#[tokio::test]
async fn group_has_buckets_uncommitted_new_twins_alone_are_false() {
    let dir = temp_group_dir();
    for bb in 0..4 {
        std::fs::write(bucket_new_path(dir.path(), bb), [bb as u8; 8]).unwrap();
    }
    assert!(!group_has_buckets(dir.path()).await);
}

/// A temporary file a bucket write left behind, stopped before its rename, records no committed
/// layout.
#[tokio::test]
async fn group_has_buckets_temporary_files_alone_are_false() {
    let dir = temp_group_dir();
    let mut temporary = bucket_path(dir.path(), 0x37).into_os_string();
    temporary.push(".tmp");
    std::fs::write(temporary, b"partial").unwrap();
    assert!(!group_has_buckets(dir.path()).await);
}

#[tokio::test]
async fn group_has_buckets_committed_bucket_is_true() {
    let dir = temp_group_dir();
    std::fs::write(bucket_path(dir.path(), 0x37), b"bucket").unwrap();
    std::fs::write(bucket_new_path(dir.path(), 0x38), b"twin").unwrap();
    assert!(group_has_buckets(dir.path()).await);
}

#[test]
fn unwritten_group_level_follows_the_initial_level_when_fan_out_aware() {
    for &level in &LEVEL_LADDER {
        assert_eq!(unwritten_group_level(true, level), level);
    }
}

#[test]
fn unwritten_group_level_is_max_without_fan_out_awareness() {
    for &level in &LEVEL_LADDER {
        assert_eq!(unwritten_group_level(false, level), FAN_OUT_LEVEL_MAX);
    }
}

#[tokio::test]
async fn read_group_level_marker_wins_over_bucket_files() {
    let dir = temp_group_dir();
    std::fs::write(bucket_path(dir.path(), 0x37), b"bucket").unwrap();
    write_level_marker(dir.path(), 64, false).await.unwrap();
    assert_eq!(
        read_group_level(dir.path()).await.unwrap(),
        GroupLevel::Marked(64)
    );
}

#[tokio::test]
async fn read_group_level_buckets_without_marker_are_pre_fan_out() {
    let dir = temp_group_dir();
    std::fs::write(bucket_path(dir.path(), 0x37), b"bucket").unwrap();
    assert_eq!(
        read_group_level(dir.path()).await.unwrap(),
        GroupLevel::PreFanOut
    );
}

#[tokio::test]
async fn read_group_level_empty_group_is_unwritten() {
    let dir = temp_group_dir();
    assert_eq!(
        read_group_level(dir.path()).await.unwrap(),
        GroupLevel::Unwritten
    );
}

#[tokio::test]
async fn read_group_level_propagates_a_corrupt_marker() {
    let dir = temp_group_dir();
    std::fs::write(dir.path().join(MARKER_FILENAME), [0xDE; 16]).unwrap();
    let err = read_group_level(dir.path()).await.unwrap_err();
    assert_eq!(err.kind(), std::io::ErrorKind::InvalidData);
}

#[tokio::test]
async fn recover_level_transition_no_pending_is_noop() {
    let dir = temp_group_dir();
    assert_eq!(
        recover_level_transition(dir.path(), false).await.unwrap(),
        None
    );
    // No marker should be created when there's nothing to recover.
    assert_eq!(read_level_marker(dir.path()).await.unwrap(), None);
}

#[tokio::test]
async fn recover_level_transition_renames_all_new_files() {
    let dir = temp_group_dir();
    // Set up "after pending, before any rename" state for target=4: four .new files exist with synthetic content; pending says target=4; no marker yet.
    for bb in 0..4 {
        std::fs::write(bucket_new_path(dir.path(), bb), [bb as u8; 8]).unwrap();
    }
    write_level_pending(dir.path(), 4, false).await.unwrap();

    let recovered = recover_level_transition(dir.path(), false).await.unwrap();
    assert_eq!(recovered, Some(4));

    // All .new files renamed to final.
    for bb in 0..4 {
        assert!(!bucket_new_path(dir.path(), bb).exists());
        let bytes = std::fs::read(bucket_path(dir.path(), bb)).unwrap();
        assert_eq!(bytes, vec![bb as u8; 8]);
    }
    // Marker reflects target.
    assert_eq!(read_level_marker(dir.path()).await.unwrap(), Some(4));
    // Pending deleted.
    assert!(!dir.path().join(LEVEL_PENDING_FILENAME).exists());
}

#[tokio::test]
async fn recover_level_transition_skips_already_renamed_buckets() {
    let dir = temp_group_dir();
    // Mid-rename state: bb=0 already renamed (final present, no .new); bb=1 still .new.
    std::fs::write(bucket_path(dir.path(), 0), b"final-0").unwrap();
    std::fs::write(bucket_new_path(dir.path(), 1), b"new-1").unwrap();
    write_level_pending(dir.path(), 2, false).await.unwrap();

    let recovered = recover_level_transition(dir.path(), false).await.unwrap();
    assert_eq!(recovered, Some(2));

    assert_eq!(
        std::fs::read(bucket_path(dir.path(), 0)).unwrap(),
        b"final-0"
    );
    assert_eq!(std::fs::read(bucket_path(dir.path(), 1)).unwrap(), b"new-1");
    assert!(!bucket_new_path(dir.path(), 1).exists());
    assert_eq!(read_level_marker(dir.path()).await.unwrap(), Some(2));
    assert!(!dir.path().join(LEVEL_PENDING_FILENAME).exists());
}

#[tokio::test]
async fn recover_level_transition_is_idempotent() {
    let dir = temp_group_dir();
    for bb in 0..2 {
        std::fs::write(bucket_new_path(dir.path(), bb), [bb as u8; 4]).unwrap();
    }
    write_level_pending(dir.path(), 2, false).await.unwrap();

    let first = recover_level_transition(dir.path(), false).await.unwrap();
    let second = recover_level_transition(dir.path(), false).await.unwrap();
    // First run rolls forward; second run is a no-op since pending is gone.
    assert_eq!(first, Some(2));
    assert_eq!(second, None);
    assert_eq!(read_level_marker(dir.path()).await.unwrap(), Some(2));
}

#[test]
fn a_group_dir_name_parses_only_in_the_form_group_dirs_are_given() {
    assert_eq!(parse_group_dir_name("00"), Some(0));
    assert_eq!(parse_group_dir_name("7f"), Some(0x7f));
    assert_eq!(parse_group_dir_name("ff"), Some(0xff));
    for name in ["", "0", "000", "FF", "0g", "-1", "pack"] {
        assert_eq!(
            parse_group_dir_name(name),
            None,
            "{name:?} is not a group dir"
        );
    }
}

/// The listing finds the group directories a store has written and nothing else: a file named
/// like a group, and a name in a form no group is given, are not groups.
#[tokio::test]
async fn existing_group_dirs_finds_the_written_groups() {
    let dir = temp_group_dir();
    let index_path = dir.path().join("index");
    std::fs::create_dir_all(group_dir_path(&index_path, 0x07)).unwrap();
    std::fs::create_dir_all(group_dir_path(&index_path, 0xde)).unwrap();
    std::fs::write(index_path.join("4a"), b"").unwrap();
    std::fs::create_dir_all(index_path.join("XY")).unwrap();

    let existing = existing_group_dirs(&index_path, 256)
        .await
        .expect("the index directory lists");

    let found: Vec<usize> = (0..256).filter(|&group| existing[group]).collect();
    assert_eq!(found, vec![0x07, 0xde]);
}

/// An entry named like a group whose metadata does not resolve, here a link to nothing, may be a
/// group that is there, so the listing gives no answer and the caller probes rather than
/// treating it as absent. A link that does not look like a group does not matter.
#[cfg(unix)]
#[tokio::test]
async fn existing_group_dirs_with_an_unresolved_group_entry_is_no_answer() {
    let dir = temp_group_dir();
    let index_path = dir.path().join("index");
    std::fs::create_dir_all(&index_path).unwrap();
    std::os::unix::fs::symlink(dir.path().join("gone"), index_path.join("not-a-group")).unwrap();
    assert!(existing_group_dirs(&index_path, 256).await.is_some());

    std::os::unix::fs::symlink(dir.path().join("gone"), group_dir_path(&index_path, 0x31)).unwrap();

    assert!(existing_group_dirs(&index_path, 256).await.is_none());
}

/// A store that has written no index yet has no groups, rather than a failure that would send
/// the caller back to probing every group.
#[tokio::test]
async fn existing_group_dirs_of_a_missing_index_is_no_groups() {
    let dir = temp_group_dir();

    let existing = existing_group_dirs(&dir.path().join("index"), 256)
        .await
        .expect("a missing index is an answer");

    assert!(existing.iter().all(|&exists| !exists));
}
