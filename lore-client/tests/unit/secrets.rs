// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT
use std::ffi::OsString;

use clap::Parser;
use lore_client::cli::LoreCli;
use lore_client::cli::resolve_secret;

fn env(value: &str) -> Option<OsString> {
    Some(OsString::from(value))
}

/// The flag outranks the variable, which is read only when the flag is absent.
#[test]
fn the_flag_outranks_the_variable() {
    assert_eq!(
        resolve_secret(Some("from-flag"), env("from-env")).as_deref(),
        Some("from-flag")
    );
    assert_eq!(
        resolve_secret(None, env("from-env")).as_deref(),
        Some("from-env")
    );
    assert_eq!(resolve_secret(None, None), None);
}

#[test]
fn a_blank_variable_reads_as_unset() {
    assert_eq!(resolve_secret(None, env("")), None);
    assert_eq!(resolve_secret(None, env("  ")), None);
}

/// A client login is its own kind of login: it takes no token, and a client secret
/// without a client is a mistake.
#[test]
fn client_credentials_exclude_a_token_and_need_a_client() {
    let parses = |args: &[&str]| LoreCli::try_parse_from(args).is_ok();

    assert!(parses(&["lore", "auth", "login", "--client-id", "ci-bot"]));
    assert!(parses(&[
        "lore",
        "auth",
        "login",
        "--client-id",
        "ci-bot",
        "--client-secret",
        "s3cr3t"
    ]));
    assert!(!parses(&[
        "lore",
        "auth",
        "login",
        "--client-secret",
        "s3cr3t"
    ]));
    assert!(!parses(&[
        "lore",
        "auth",
        "login",
        "--client-id",
        "ci-bot",
        "--token-type",
        "api-key"
    ]));
    assert!(!parses(&[
        "lore",
        "auth",
        "login",
        "--client-id",
        "ci-bot",
        "--token",
        "t"
    ]));
    assert!(!parses(&["lore", "auth", "login", "--client-id", ""]));
}
