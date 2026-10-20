// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT
use std::borrow::Cow;

use lore_revision::file::diff::*;

#[test]
fn make_diff_content_empty_is_text() {
    let c = make_diff_content(b"");
    assert!(!c.is_binary());
    assert_eq!(c.text(), "");
}

#[test]
fn make_diff_content_valid_utf8_is_text() {
    let c = make_diff_content(b"hello\nworld\n");
    assert!(!c.is_binary());
    assert_eq!(c.text(), "hello\nworld\n");
}

#[test]
fn make_diff_content_utf16_le_bom_is_text() {
    // UTF-16 BOM is exempt: the diff path renders it as readable text,
    // matching the test_file_diff_utf16be smoke test.
    let mut bytes = vec![0xFF, 0xFE];
    bytes.extend("Hi\n".encode_utf16().flat_map(u16::to_le_bytes));
    let c = make_diff_content(&bytes);
    assert!(!c.is_binary(), "UTF-16 LE BOM must remain diffable as text");
    assert_eq!(c.text(), "Hi\n");
}

#[test]
fn make_diff_content_null_bytes_is_binary() {
    let c = make_diff_content(&[0x00, 0x01, 0x02, 0xFF, 0xFE, 0x00]);
    assert!(c.is_binary());
}

#[test]
fn make_diff_content_invalid_utf8_is_binary() {
    let c = make_diff_content(&[0xFF, 0xFF, 0xFF, 0xFF, 0xFF]);
    assert!(c.is_binary());
}

#[test]
fn diff_content_empty_constructor_is_text() {
    let c = DiffContent::empty();
    assert!(!c.is_binary());
    assert_eq!(c.text(), "");
}

#[test]
fn normalise_text_strips_trailing_whitespace() {
    assert_eq!(normalise_text("foo   \n", true, false), "foo\n");
    assert_eq!(normalise_text("foo\t\t\n", true, false), "foo\n");
    assert_eq!(normalise_text("foo", true, false), "foo");
    assert_eq!(normalise_text("foo   ", true, false), "foo");
}

#[test]
fn normalise_text_collapses_runs() {
    assert_eq!(normalise_text("a  b   c\n", false, true), "a b c\n");
    assert_eq!(normalise_text("a\t\tb\n", false, true), "a b\n");
    // Internal whitespace gone entirely is NOT invented back.
    assert_eq!(normalise_text("abc\n", false, true), "abc\n");
}

#[test]
fn normalise_text_both_flags() {
    assert_eq!(normalise_text("a  b   \n", true, true), "a b\n");
}

/// A whole side normalises line by line, so it keeps its line count and each
/// line follows the same rules as it would alone.
#[test]
fn normalise_text_normalises_every_line_and_keeps_the_line_count() {
    let text = "a  b   \n\tc\t\r\n\nlast  word  ";
    assert_eq!(normalise_text(text, true, false), "a  b\n\tc\n\nlast  word");
    assert_eq!(normalise_text(text, false, true), "a b \n c \n\nlast word ");
    assert_eq!(normalise_text(text, true, true), "a b\n c\n\nlast word");
    assert_eq!(normalise_text(text, false, false), text);
    assert_eq!(normalise_text("", true, true), "");
}

/// With no rule selected the side is compared as written, so it is not copied.
#[test]
fn normalise_text_without_rules_borrows_the_text() {
    let text = "a  b   \n";
    assert!(matches!(
        normalise_text(text, false, false),
        Cow::Borrowed(borrowed) if std::ptr::eq(borrowed, text)
    ));
    assert!(matches!(normalise_text(text, true, false), Cow::Owned(_)));
}

#[test]
fn diff_content_into_text_moves_the_text_out() {
    assert_eq!(make_diff_content(b"a\nb\n").into_text(), "a\nb\n");
    assert_eq!(DiffContent::Binary.into_text(), "");
}

#[test]
fn normalise_text_crlf_trailing_treated_as_whitespace() {
    // CRLF inputs (e.g. Python text-mode writes on Windows) must compare equal
    // to LF inputs under ignore_eol — the trailing `\r` counts as EOL whitespace.
    assert_eq!(normalise_text("foo   \r\n", true, false), "foo\n");
    assert_eq!(normalise_text("foo\r\n", true, false), "foo\n");
    // No-flag path keeps the `\r` intact.
    assert_eq!(normalise_text("foo\r\n", false, false), "foo\r\n");
}

#[test]
fn ignore_eol_trailing_space_no_diff() {
    let old = "foo\nbar\n";
    let new = "foo  \nbar\n";
    // With the flag on, no hunks should be produced.
    assert!(format_patch_preserving_originals(old, new, 3, true, false).is_none());
}

#[test]
fn ignore_eol_preserves_originals_in_real_change() {
    // Two lines: line 1 has trailing-whitespace-only diff, line 2 has a real diff.
    let old = "foo  \nbar\nbaz\n";
    let new = "foo  \nBAR\nbaz\n";
    let out = format_patch_preserving_originals(old, new, 3, true, false)
        .expect("real change should produce a hunk");
    // The unchanged "foo  " line must keep its trailing whitespace in the options.
    assert!(
        out.contains(" foo  \n"),
        "context line should show original whitespace:\n{out}"
    );
    assert!(out.contains("-bar\n"));
    assert!(out.contains("+BAR\n"));
}

#[test]
fn ignore_inline_collapses_runs_no_diff() {
    let old = "a b c\n";
    let new = "a  b   c\n";
    assert!(format_patch_preserving_originals(old, new, 3, false, true).is_none());
}

#[test]
fn ignore_inline_does_not_invent_whitespace() {
    // "abc" → "a bc" introduces whitespace where none existed; must still diff.
    let old = "abc\n";
    let new = "a bc\n";
    let out = format_patch_preserving_originals(old, new, 3, false, true)
        .expect("introducing whitespace must still register as a change");
    assert!(out.contains("-abc\n"));
    assert!(out.contains("+a bc\n"));
}

#[test]
fn both_flags_combined_suppress_all_whitespace_only_diffs() {
    let old = "foo\nbar  baz\n";
    let new = "foo   \nbar baz\n";
    assert!(format_patch_preserving_originals(old, new, 3, true, true).is_none());
}

#[test]
fn context_lines_respected_with_flags() {
    let old = "a\nb\nc\nx\ne\nf\ng\n";
    let new = "a\nb\nc\nX\ne\nf\ng\n";
    let out = format_patch_preserving_originals(old, new, 0, true, false)
        .expect("real change should produce a hunk");
    // context=0 means no surrounding lines in the hunk.
    assert!(out.contains("@@ -4 +4 @@\n"), "got:\n{out}");
    assert!(out.contains("-x\n"));
    assert!(out.contains("+X\n"));
    // No surrounding context lines.
    assert!(!out.contains(" c\n"));
    assert!(!out.contains(" e\n"));
}

#[test]
fn missing_newline_marker_when_input_lacks_terminator() {
    let old = "foo";
    let new = "bar";
    let out = format_patch_preserving_originals(old, new, 3, true, false)
        .expect("differing single-line files should diff");
    assert!(
        out.contains("\\ No newline at end of file\n"),
        "expected no-newline marker, got:\n{out}"
    );
}
