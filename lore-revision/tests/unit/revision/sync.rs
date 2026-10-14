// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT
#![allow(clippy::disallowed_methods)] // Test fixtures writing the copies in a temporary directory.

use std::path::Path;
use std::sync::Arc;

use lore_base::test_util::TempDir;
use lore_revision::fs::filesystem_provider::FilesystemProvider;
use lore_revision::fs::filesystem_provider::InstanceOperationImpl;
use lore_revision::fs::os::OsFilesystem;
use lore_revision::repository::MERGE_ARTIFACT_SUFFIXES;
use lore_revision::repository::MINE_SUFFIX;
use lore_revision::revision::sync::*;
use lore_revision::util::path::RelativePath;

use crate::fs::filesystem_provider::TestFilesystemProvider;

/// An operation rooted at `root`, which is what the helpers name their paths against.
async fn os_operation(root: &Path) -> Arc<InstanceOperationImpl> {
    FilesystemProvider::begin_operation(&OsFilesystem::new(root))
        .await
        .expect("beginning an operation over the OS filesystem")
}

fn relative(path: &str) -> RelativePath {
    RelativePath::new_from_initial_path(path).expect("relative path")
}

/// Every copy on its own, so a helper that reads one suffix and stops is not mistaken for
/// one that reads all three.
#[tokio::test]
async fn a_copy_under_any_suffix_is_found() {
    for suffix in MERGE_ARTIFACT_SUFFIXES {
        let dir = TempDir::new("lore-merge-artifact-");
        let operation = os_operation(dir.path()).await;
        std::fs::write(dir.path().join(format!("file.txt{suffix}")), b"side").expect("write copy");

        assert!(
            exist_merge_artifacts(&operation, &relative("file.txt")).await,
            "the copy under {suffix} was not found"
        );
    }
}

#[tokio::test]
async fn a_file_no_merge_left_copies_beside_reports_none() {
    let dir = TempDir::new("lore-merge-artifact-");
    let operation = os_operation(dir.path()).await;
    std::fs::write(dir.path().join("file.txt"), b"merged").expect("write file");

    assert!(!exist_merge_artifacts(&operation, &relative("file.txt")).await);
}

/// The file the copies belong to is not one of them, and stays.
#[tokio::test]
async fn removing_takes_every_copy_and_leaves_the_file() {
    let dir = TempDir::new("lore-merge-artifact-");
    let operation = os_operation(dir.path()).await;
    std::fs::write(dir.path().join("file.txt"), b"merged").expect("write file");
    for suffix in MERGE_ARTIFACT_SUFFIXES {
        std::fs::write(dir.path().join(format!("file.txt{suffix}")), b"side").expect("write copy");
    }

    unlink_merge_artifacts(&operation, &relative("file.txt")).await;

    assert!(!exist_merge_artifacts(&operation, &relative("file.txt")).await);
    for suffix in MERGE_ARTIFACT_SUFFIXES {
        assert!(
            !dir.path().join(format!("file.txt{suffix}")).exists(),
            "the copy under {suffix} was left behind"
        );
    }
    assert!(
        dir.path().join("file.txt").exists(),
        "the file the copies belong to was removed"
    );
}

/// Read from the working tree rather than from the tree the path is tracked in: a
/// provider serving tracked content virtually holds no node for a sidecar, so one asked
/// through [`InstanceOperation::file_info`] answers that every copy is absent.
#[tokio::test]
async fn a_copy_is_looked_for_outside_the_tracked_tree() {
    let provider = Arc::new(TestFilesystemProvider::holding_every_path());
    let operation = provider
        .begin_operation()
        .await
        .expect("beginning an operation over the test provider");

    assert!(exist_merge_artifacts(&operation, &relative("file.txt")).await);
    assert_eq!(
        0,
        provider.file_infos(),
        "the copies were looked up through the tracked tree"
    );
}

/// A path under a directory, so the suffix lands on the name rather than anywhere in the
/// path it is reached by.
#[tokio::test]
async fn a_copy_beside_a_nested_file_is_found_and_removed() {
    let dir = TempDir::new("lore-merge-artifact-");
    let operation = os_operation(dir.path()).await;
    std::fs::create_dir_all(dir.path().join("sub")).expect("create directory");
    let copy = dir
        .path()
        .join("sub")
        .join(format!("file.txt{MINE_SUFFIX}"));
    std::fs::write(&copy, b"side").expect("write copy");

    let nested = relative("sub/file.txt");
    assert!(exist_merge_artifacts(&operation, &nested).await);

    unlink_merge_artifacts(&operation, &nested).await;

    assert!(
        !copy.exists(),
        "the copy beside a nested file was left behind"
    );
}

/// A filesystem error a sync meets keeps its kind: local modifications and a missing file reach
/// the caller as themselves, not as an internal error.
#[test]
fn a_filesystem_error_keeps_its_kind_through_a_sync() {
    use lore_base::error::FileNotFound;
    use lore_base::error::LocalModifications;
    use lore_revision::fs::filesystem_provider::FsError;

    let modified = SyncError::from(FsError::from(LocalModifications));
    assert!(
        matches!(modified, SyncError::LocalModifications(_)),
        "{modified:?}"
    );

    let missing = SyncError::from(FsError::from(FileNotFound {
        resource: "gone.txt".to_string(),
    }));
    assert!(matches!(missing, SyncError::FileNotFound(_)), "{missing:?}");
}
