// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;

use lore_base::runtime::LORE_CONTEXT;
use lore_revision::commit::*;
use lore_revision::interface::ExecutionContext;
use lore_revision::interface::LoreGlobalArgs;
use lore_revision::relay::EventDispatcher;

/// Run `body` under an execution context asking for `stats`.
async fn at_statistics_level<T>(stats: u32, body: impl Future<Output = T>) -> T {
    let globals = LoreGlobalArgs {
        stats,
        ..Default::default()
    };
    let execution = Arc::new(ExecutionContext::new_client(
        globals,
        EventDispatcher::no_dispatch(),
    ));
    LORE_CONTEXT.scope(execution, body).await
}

/// Statistics level zero reports nothing, so it keeps nothing beyond what a
/// progress event reads: the content read off disk, which it reports as the
/// bytes transferred.
#[tokio::test]
async fn a_count_is_kept_only_where_something_reports_it() {
    for (level, kept) in [(0, 0), (1, 1)] {
        let files = at_statistics_level(level, async {
            let stats = CommitStats::new();
            stats.file_read(4096);
            stats.record_file_action(0, 4096);
            stats.file_stats()
        })
        .await;

        assert_eq!(files.files, kept, "level {level}");
        assert_eq!(files.files_read, kept, "level {level}");
        assert_eq!(files.file_bytes, kept * 4096, "level {level}");
        assert_eq!(files.bytes_transferred, 4096, "level {level}");
    }
}

/// A progress snapshot taken while directories and files are counted never reads a count ahead
/// of its total.
#[test]
fn a_progress_snapshot_never_reads_a_count_ahead_of_its_total() {
    let stats = CommitStats::new();
    let counted = Arc::new(AtomicBool::new(false));
    let counter = std::thread::spawn({
        let (stats, counted) = (stats.clone(), counted.clone());
        move || {
            for _ in 0..10_000_000 {
                stats.count_directory();
                stats.count_file();
            }
            counted.store(true, Ordering::SeqCst);
        }
    });
    while !counted.load(Ordering::SeqCst) {
        let snapshot = LoreRevisionCommitCountData::new(stats.clone());
        assert!(
            snapshot.directory_count <= snapshot.directory_total,
            "{} directories counted of {}",
            snapshot.directory_count,
            snapshot.directory_total
        );
        assert!(
            snapshot.file_count <= snapshot.file_total,
            "{} files counted of {}",
            snapshot.file_count,
            snapshot.file_total
        );
    }
    counter.join().expect("the counter thread");
}
