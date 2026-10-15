// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT
use lore::storage::get_resolved::fragments_read_options;
use lore_storage::options::ReadOptions;

/// `fragments` delivery reads payloads as stored from a trusted remote, whatever options the item
/// otherwise reads with, so no leaf is decoded or hashed on its way to the caller.
#[test]
fn fragments_delivery_reads_as_stored_from_a_trusted_remote() {
    for options in [
        ReadOptions::default(),
        ReadOptions::default().with_cache(),
        ReadOptions::default().no_local(),
    ] {
        let fragments = fragments_read_options(options);
        assert!(!fragments.decompress, "a leaf is expanded");
        assert!(fragments.trust_remote, "a fetched leaf is checked");
    }
}
