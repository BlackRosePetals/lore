// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT
use lore::file::*;

#[test]
fn a_zero_bound_is_the_default_rather_than_no_bound() {
    assert_eq!(audit_bound(0), Some(DEFAULT_MAX_REVISIONS));
}

#[test]
fn the_largest_bound_is_no_bound() {
    assert_eq!(audit_bound(u32::MAX), None);
}

#[test]
fn a_named_number_is_the_bound() {
    assert_eq!(audit_bound(1), Some(1));
    assert_eq!(audit_bound(250), Some(250));
}
