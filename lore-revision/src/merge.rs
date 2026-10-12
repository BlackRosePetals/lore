// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT
use std::path::PathBuf;

use lore_base::lore_spawn;
use lore_error_set::prelude::*;

use crate::fs::filesystem_provider::FsError;
use crate::fs::filesystem_provider::InstanceOperation;
use crate::fs::filesystem_provider::InstanceOperationImpl;
use crate::repository::RepositoryWriteToken;
use crate::util::path::RelativePath;

/// Merge two files given a common ancestor.
///
/// # Arguments
///
/// * `base` - A &str that holds the common ancestor of mine and theirs.
/// * `mine` - A &str that holds the mine / left / current version.
/// * `theirs` - A &str that holds the theirs / right / incoming version.
/// * `base_marker` - An optional &str that holds the text to mark the common ancestor version. Defaults to 'original'.
/// * `mine_marker` - An optional &str that holds the text to mark the mine / left / current version. Defaults to 'ours'.
/// * `theirs_marker` - An optional &str that holds the text to mark the theirs / right / incoming version. Defaults to 'theirs'.
///
/// # Return value
///
/// * `Ok(String)` if there was a successful merge.
/// * `Err(String)` if there were conflicts, with the conflicting regions marked with conflict markers.
///
pub fn merge3_text(
    base: &str,
    mine: &str,
    theirs: &str,
    base_marker: Option<&str>,
    mine_marker: Option<&str>,
    theirs_marker: Option<&str>,
) -> Result<String, String> {
    // `Git`, not diffy's `Diff3` default: `Diff3` glues the next marker onto a
    // final line that lacks a newline, which is unparsable.
    let merge_result = diffy::MergeOptions::new()
        .set_incomplete_hunk_style(diffy::IncompleteHunkStyle::Git)
        .merge(base, mine, theirs);
    let merge_conflicts = merge_result.is_err();
    let mut merge_output = match merge_result {
        Ok(str) | Err(str) => str,
    };

    if merge_conflicts {
        if let Some(str) = base_marker {
            merge_output = merge_output.replace("||||||| original", &format!("||||||| {str}"));
        }
        if let Some(str) = mine_marker {
            merge_output = merge_output.replace("<<<<<<< ours", &format!("<<<<<<< {str}"));
        }
        if let Some(str) = theirs_marker {
            merge_output = merge_output.replace(">>>>>>> theirs", &format!(">>>>>>> {str}"));
        }

        Err(merge_output)
    } else {
        Ok(merge_output)
    }
}

/// Whether a text merge should persist its output to disk.
///
/// `DryRun` computes the merge and reports conflicts without writing. `Write`
/// performs the same computation and then writes the merged result to the
/// `result` path; because writing is a repository mutation it carries a
/// borrowed [`RepositoryWriteToken`] as compile-time proof of authorization.
pub enum MergeTextMode<'a> {
    DryRun,
    Write(&'a RepositoryWriteToken),
}

/// Merges the three texts: whether they conflicted, and the merged text where `mode` asks for it
/// to be written.
///
/// A conflict yields text like any other merge, the conflicting regions carrying markers, so the
/// caller writes it either way. Bytes that are not text are read lossily, there being no
/// encoding to refuse them under.
fn merge3_text_outcome(
    base: &[u8],
    mine: &[u8],
    theirs: &[u8],
    mode: &MergeTextMode<'_>,
) -> (bool, Option<String>) {
    let base = String::from_utf8_lossy(base);
    let mine = String::from_utf8_lossy(mine);
    let theirs = String::from_utf8_lossy(theirs);

    let merged = merge3_text(&base, &mine, &theirs, None, None, None);
    let conflicted = merged.is_err();
    let output = matches!(mode, MergeTextMode::Write(_)).then(|| match merged {
        Err(text) | Ok(text) => text,
    });
    (conflicted, output)
}

/// One side of a merge, as the task spawned to read it left it.
fn merge_side(
    read: Result<Result<bytes::Bytes, lore_storage::StorageError>, tokio::task::JoinError>,
) -> Result<bytes::Bytes, FsError> {
    read.map_err(std::io::Error::other)?
        .forward_any::<FsError>("Failed to read a side of the merge")
}

/// [`merge3_text`] over three files the operation names, writing the result at `result` where
/// `mode` asks for it.
///
/// The three are read at once: none of them waits on another, and a merge is as slow as the
/// slowest of them rather than the sum.
#[lore_macro::test_pub]
pub(crate) async fn merge3_text_in_operation(
    operation: &InstanceOperationImpl,
    base: &RelativePath,
    mine: &RelativePath,
    theirs: &RelativePath,
    result: &RelativePath,
    mode: MergeTextMode<'_>,
) -> Result<bool, FsError> {
    let (base_source, mine_source, theirs_source) = tokio::try_join!(
        operation.content_source(base),
        operation.content_source(mine),
        operation.content_source(theirs)
    )
    .forward::<FsError>("naming the sides of the merge")?;

    let base_read = lore_spawn!(async move { base_source.read_all().await });
    let mine_read = lore_spawn!(async move { mine_source.read_all().await });
    let theirs_read = lore_spawn!(async move { theirs_source.read_all().await });

    let base_buffer = merge_side(base_read.await)?;
    let mine_buffer = merge_side(mine_read.await)?;
    let theirs_buffer = merge_side(theirs_read.await)?;

    let (conflicted, output) =
        merge3_text_outcome(&base_buffer, &mine_buffer, &theirs_buffer, &mode);
    if let Some(output) = output {
        operation
            .write_file(result, bytes::Bytes::from(output))
            .await?;
    }

    Ok(conflicted)
}

/// [`merge3_text`] over three files named by absolute path, for a caller merging outside any
/// operation: the sidecars an auto-resolve builds in a temporary directory.
pub async fn merge3_text_by_pathbuf(
    base: PathBuf,
    mine: PathBuf,
    theirs: PathBuf,
    result: PathBuf,
    mode: MergeTextMode<'_>,
) -> std::io::Result<bool> {
    let base_read =
        lore_spawn!(async move { lore_io::IoDriver::global().read_file_bytes(base).await });
    let mine_read =
        lore_spawn!(async move { lore_io::IoDriver::global().read_file_bytes(mine).await });
    let theirs_read =
        lore_spawn!(async move { lore_io::IoDriver::global().read_file_bytes(theirs).await });

    let base_buffer = base_read.await.map_err(std::io::Error::other)??;
    let mine_buffer = mine_read.await.map_err(std::io::Error::other)??;
    let theirs_buffer = theirs_read.await.map_err(std::io::Error::other)??;

    let (conflicted, output) =
        merge3_text_outcome(&base_buffer, &mine_buffer, &theirs_buffer, &mode);
    if let Some(output) = output {
        lore_io::IoDriver::global()
            .write_file_bytes(result, bytes::Bytes::from(output), false)
            .await?;
    }

    Ok(conflicted)
}
