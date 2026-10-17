// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT
use std::num::NonZeroU32;
use std::path::PathBuf;

use lore_base::runtime::LORE_CONTEXT;
use lore_base::test_util::TempDir;
use lore_base::types::Address;
use lore_base::types::Hash;
use lore_revision::node::NodeBlockData;
use lore_revision::node::NodeBlockFormat;
use lore_revision::repository::prefetch::*;
use zerocopy::FromZeros;
use zerocopy::IntoBytes;

use crate::fs::filesystem_provider::setup_test_execution;

fn spelled(paths: &[RecordedPath]) -> Vec<(String, bool)> {
    paths
        .iter()
        .map(|recorded| (recorded.path.as_str().to_string(), recorded.directory))
        .collect()
}

#[test]
fn recording_keeps_first_access_order_and_drops_repeats() {
    let (parsed, _) = parse_recording(b"b.txt\na/\na/c.txt\nb.txt\na/\n");
    assert_eq!(
        spelled(&parsed),
        vec![
            ("b.txt".to_string(), false),
            ("a".to_string(), true),
            ("a/c.txt".to_string(), false),
        ]
    );
}

#[test]
fn recording_reads_the_root_and_directories_by_their_trailing_slash() {
    let (parsed, _) = parse_recording(b"/\nEngine/\nEngine/Build.version\n");
    assert_eq!(
        spelled(&parsed),
        vec![
            (String::new(), true),
            ("Engine".to_string(), true),
            ("Engine/Build.version".to_string(), false),
        ]
    );
}

#[test]
fn recording_skips_blank_lines_and_paths_that_leave_the_tree() {
    let (parsed, _) = parse_recording(b"\r\n\nkeep.txt\r\n../outside.txt\nC:/drive.txt\n");
    assert_eq!(spelled(&parsed), vec![("keep.txt".to_string(), false)]);
}

/// A path is the line as spelled, spaces included, and paths are compared as parsed: a leading `/`
/// or a doubled separator repeats a path recorded before.
#[test]
fn recording_keeps_spaces_and_compares_paths_as_parsed() {
    let (parsed, _) = parse_recording(b" spaced .txt \n/a\na\na//b\na/b\n");
    assert_eq!(
        spelled(&parsed),
        vec![
            (" spaced .txt ".to_string(), false),
            ("a".to_string(), false),
            ("a/b".to_string(), false),
        ]
    );
}

/// A line that is not UTF-8, or names the root as a file, is skipped, and the lines around it are
/// read.
#[test]
fn recording_skips_a_line_not_utf8_or_naming_the_root_as_a_file() {
    let (parsed, _) = parse_recording(b"keep.txt\n\xff\xfe.txt\n.\nalso.txt\n");
    assert_eq!(
        spelled(&parsed),
        vec![
            ("keep.txt".to_string(), false),
            ("also.txt".to_string(), false)
        ]
    );
}

#[test]
fn a_file_and_a_directory_of_one_name_are_both_kept() {
    let (parsed, _) = parse_recording(b"a\na/\n");
    assert_eq!(
        spelled(&parsed),
        vec![("a".to_string(), false), ("a".to_string(), true)]
    );
}

#[test]
fn recording_reads_block_hints_after_a_tab_in_first_need_order_each_once() {
    let (parsed, blocks) = parse_recording(b"/\t3 1\nkeep/\t1 7 x\nkeep/a.txt\n");
    assert_eq!(
        spelled(&parsed),
        vec![
            (String::new(), true),
            ("keep".to_string(), true),
            ("keep/a.txt".to_string(), false),
        ]
    );
    assert_eq!(blocks, vec![3, 1, 7]);
}

/// A node block's content names its name table where it is in the current format and names one;
/// content of another length, a newer format or a zero name table names none.
#[test]
fn a_node_block_names_its_name_table_in_the_current_format() {
    let mut block = NodeBlockData::new_box_zeroed().expect("a zeroed node block");
    let name_table = Hash::from([7u8; 32]);
    block.version = NodeBlockFormat::Nametable as u32;
    block.name_table = name_table;
    assert_eq!(
        name_table_of(block.as_bytes()),
        Some(Address::zero_context_hash(name_table))
    );
    assert_eq!(name_table_of(&block.as_bytes()[1..]), None);

    block.version = NodeBlockFormat::NoTimestamp as u32;
    assert_eq!(name_table_of(block.as_bytes()), None);

    block.version = NodeBlockFormat::Nametable as u32;
    block.name_table = Hash::default();
    assert_eq!(name_table_of(block.as_bytes()), None);
}

#[test]
fn tree_prefetch_parses_levels_all_and_blocks() {
    assert_eq!(TreePrefetch::parse("0"), Some(TreePrefetch::Off));
    let levels = |count| TreePrefetch::Levels(NonZeroU32::new(count).expect("a level count"));
    assert_eq!(TreePrefetch::parse("2"), Some(levels(2)));
    assert_eq!(TreePrefetch::parse(" 5 "), Some(levels(5)));
    assert_eq!(TreePrefetch::parse("all"), Some(TreePrefetch::View));
    assert_eq!(TreePrefetch::parse("ALL"), Some(TreePrefetch::View));
    assert_eq!(TreePrefetch::parse("blocks"), Some(TreePrefetch::Blocks));
    assert_eq!(TreePrefetch::parse("-1"), None);
    assert_eq!(TreePrefetch::parse("deep"), None);
    assert_eq!(TreePrefetch::parse(""), None);
}

/// A file of at least `len` bytes in a temporary directory named after `test`, alive for as long as
/// the directory is: `lines` distinct file paths of 66 bytes and a line end each, then a hole up to
/// `len`.
#[allow(clippy::disallowed_methods)] // A test fixture in its own temporary directory.
fn prefetch_file(test: &str, lines: usize, len: u64) -> (TempDir, PathBuf) {
    let dir = TempDir::new(test);
    let path = dir.path().join("recording.txt");
    let text: String = (0..lines)
        .map(|line| format!("dir/{line:058}.txt\n"))
        .collect();
    std::fs::write(&path, text).expect("write the recording");
    std::fs::File::options()
        .write(true)
        .open(&path)
        .and_then(|file| file.set_len(len.max(67 * lines as u64)))
        .expect("size the recording");
    (dir, path)
}

/// A recording of 160,000 lines of 67 bytes, 10.7 MB, past lore-io's 8 MiB whole-file limit, is
/// read whole.
#[tokio::test]
async fn a_recording_past_the_whole_file_limit_is_read() {
    let (_dir, path) = prefetch_file("a_recording_past_the_whole_file_limit_is_read-", 160_000, 0);
    LORE_CONTEXT
        .scope(setup_test_execution(), async {
            let source = path.to_str().expect("a UTF-8 path");
            let plan = plan(Some(TreePrefetch::Off), Some(source)).await;
            let recording = plan.recording.expect("the recording is read");
            assert_eq!(recording.paths.len(), 160_000);
        })
        .await;
}

/// A recording whose hole takes it past the prefetch file limit is refused on its length alone,
/// before anything is read.
#[tokio::test]
async fn a_recording_past_the_prefetch_file_limit_is_refused() {
    let (_dir, path) = prefetch_file(
        "a_recording_past_the_prefetch_file_limit_is_refused-",
        1,
        PREFETCH_FILE_LIMIT as u64 + 1,
    );
    LORE_CONTEXT
        .scope(setup_test_execution(), async {
            let source = path.to_str().expect("a UTF-8 path");
            let plan = plan(Some(TreePrefetch::Off), Some(source)).await;
            assert!(plan.recording.is_none());
        })
        .await;
}
