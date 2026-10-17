// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT
//! `RemoteImmutableStore` checking what a remote serves on receipt, and healing what fails.

use std::sync::Arc;

use lore_revision::repository::RepositoryContext;
use lore_revision::store::remote::RemoteImmutableStore;
use lore_storage::ImmutableStore;
use lore_transport::Connection;

use super::serving::Serving;
use super::serving::ServingStorage;
use super::serving::remote_copy;
use super::serving::serving_remote;
use super::session_tests::under_execution_context;

/// A remote store whose connection for `source`'s partition reaches `remote`.
fn store_over(
    source: &Arc<RepositoryContext>,
    remote: &Arc<ServingStorage>,
) -> Arc<RemoteImmutableStore> {
    Arc::new(RemoteImmutableStore::with_connection(
        source.id,
        Connection::detached(vec![remote.clone()]),
    ))
}

/// A payload the remote serves altered is fetched again once the remote heals it, and served as the
/// heal restored it.
#[tokio::test]
async fn a_payload_failing_its_check_is_served_as_healed() {
    let content = b"content the remote serves altered until it heals it";
    let (source, address, _) = remote_copy(content, 0x71, 0).await;
    let remote = serving_remote(
        &source,
        Serving {
            tampered: Some(address),
            heals: true,
            ..Serving::default()
        },
    );
    let store = store_over(&source, &remote);

    let served = under_execution_context(|| store.clone().get(source.id, address))
        .await
        .expect("the healed payload is served");

    assert_eq!(served.payload.as_deref(), Some(&content[..]));
    assert_eq!((remote.gets(), remote.verifies()), (2, 1));
}

/// A payload still altered after a heal the remote reports as made is refused, after that one
/// heal.
#[tokio::test]
async fn a_payload_still_failing_after_its_heal_is_refused() {
    let content = b"content the remote serves altered through its heal";
    let (source, address, _) = remote_copy(content, 0x72, 0).await;
    let remote = serving_remote(
        &source,
        Serving {
            tampered: Some(address),
            heals: true,
            heal_fails: true,
            ..Serving::default()
        },
    );
    let store = store_over(&source, &remote);

    let served = under_execution_context(|| store.clone().get(source.id, address)).await;

    assert!(
        served.as_ref().is_err_and(|err| err.is_internal()),
        "got {served:?}"
    );
    assert_eq!((remote.gets(), remote.verifies()), (2, 1));
}

/// A payload in a codec this build cannot decode, one no build assigns, is refused as
/// `NotSupported`, and no heal is asked for it: it is not corrupt.
#[tokio::test]
async fn an_undecodable_payload_asks_no_heal() {
    use lore_base::types::FragmentFlags;
    let content = b"content stored in a codec this build cannot decode";
    let unassigned = FragmentFlags::PayloadCompressed
        & !(FragmentFlags::PayloadCompressedLZ4
            | FragmentFlags::PayloadCompressedOodle2
            | FragmentFlags::PayloadCompressedZstd);
    let (source, address, _) = lore_storage::test_util::accepting_unvalidated_puts(remote_copy(
        content,
        0x73,
        unassigned.bits(),
    ))
    .await;
    let remote = serving_remote(&source, Serving::default());
    let store = store_over(&source, &remote);

    let served = under_execution_context(|| store.clone().get(source.id, address)).await;

    assert!(
        served.as_ref().is_err_and(|err| err.is_not_supported()),
        "got {served:?}"
    );
    assert_eq!((remote.gets(), remote.verifies()), (1, 0));
}

/// A get the remote refuses as unsupported fails as an internal error, as a remote the store cannot
/// reach does, not as `NotSupported`, which is a payload this build cannot decode, and asks no heal.
#[tokio::test]
async fn a_get_the_remote_does_not_support_is_an_internal_failure() {
    let content = b"content behind a remote that supports no get";
    let (source, address, _) = remote_copy(content, 0x74, 0).await;
    let remote = serving_remote(
        &source,
        Serving {
            unsupported: true,
            ..Serving::default()
        },
    );
    let store = store_over(&source, &remote);

    let served = under_execution_context(|| store.clone().get(source.id, address)).await;

    assert!(
        served.as_ref().is_err_and(|err| err.is_internal()),
        "got {served:?}"
    );
    assert_eq!((remote.gets(), remote.verifies()), (1, 0));
}
