// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT
use lore_base::error::FileNotFound;
use lore_base::lore_spawn;
use lore_base::runtime::LORE_CONTEXT;
use lore_revision::commit::*;
use tokio::sync::mpsc;

use crate::fs::filesystem_provider::setup_test_execution;

/// A commit whose fragmenting stops on a file it cannot read reports that failure. Discovery, which
/// sends files to the fragmenting until it can no longer pass them on, then fails as well. The
/// commit does not report that second failure.
#[tokio::test]
async fn a_commit_reports_a_failed_file_read_rather_than_the_stopped_discovery() {
    LORE_CONTEXT
        .scope(setup_test_execution(), async {
            let (file_tx, mut file_rx) = mpsc::channel::<u64>(1);
            let discovery = lore_spawn!(async move {
                for file in 0.. {
                    if file_tx.send(file).await.is_err() {
                        return Err(CommitError::internal("Recursion task failed"));
                    }
                }
                Ok(())
            });
            let fragmenting = lore_spawn!(async move {
                file_rx.recv().await;
                Err(CommitError::from(FileNotFound {
                    resource: "missing.txt".to_string(),
                }))
            });

            let result = join_commit_tasks(discovery, fragmenting).await;

            assert!(
                matches!(result, Err(CommitError::FileNotFound(_))),
                "the commit reported {result:?} rather than the file it could not read"
            );
        })
        .await;
}
