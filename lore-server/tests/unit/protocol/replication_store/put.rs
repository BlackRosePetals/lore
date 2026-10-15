// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT
use bytes::Bytes;
use lore_base::types::Context;
use lore_base::types::FRAGMENT_SIZE_THRESHOLD;
use lore_base::types::Fragment;
use lore_revision::fragment;
use lore_server::protocol::replication_store::header::ReplicationHeader;
use lore_server::protocol::replication_store::put::*;
use lore_server::protocol::storage::messages::MessageParseError;
use lore_server::quic::replication_store_service::MAX_CHUNK_SIZE;
use lore_server::quic::tests::collapse_bytes_without_header;
use lore_transport::quic::command_header::CommandHeader;
use rand::random;
use uuid::Uuid;

#[test]
fn is_under_max_chunk_size() {
    // base request plus max payload size
    let max_request_size = BASE_REQUEST_SIZE + FRAGMENT_SIZE_THRESHOLD;
    assert!(max_request_size + size_of::<CommandHeader>() <= MAX_CHUNK_SIZE);
}

#[test]
fn parsing_without_payload_works() {
    let repository = random::<Context>();
    let (fragment, address, _) = fragment::generate_random();

    let input = Put {
        header: ReplicationHeader {
            correlation_id: Uuid::new_v4(),
            repository,
        },
        address,
        fragment,
        flags: 0,
        payload: None,
    };
    let input_bytes = collapse_bytes_without_header(&input.clone().to_quic_chunks());
    let output = parse(input_bytes).expect("parse should work");

    assert_eq!(input, output);
}

#[test]
fn parsing_with_payload_works() {
    let repository = random::<Context>();
    let (fragment, address, payload) = fragment::generate_random();

    let input = Put {
        header: ReplicationHeader {
            correlation_id: Uuid::new_v4(),
            repository,
        },
        address,
        fragment,
        flags: 1,
        payload: Some(payload),
    };
    let input_bytes = collapse_bytes_without_header(&input.clone().to_quic_chunks());
    let output = parse(input_bytes).expect("parse should work");

    assert_eq!(input, output);
}

#[test]
fn parsing_fails_if_too_small() {
    let repository = random::<Context>();
    let (fragment, address, _) = fragment::generate_random();

    let input = Put {
        header: ReplicationHeader {
            correlation_id: Uuid::new_v4(),
            repository,
        },
        address,
        fragment,
        flags: 0,
        payload: None,
    };
    let input_bytes = collapse_bytes_without_header(&input.to_quic_chunks());
    let output = parse(input_bytes.slice(0..input_bytes.len() - 1)).expect_err("parse should fail");

    assert_eq!(output, MessageParseError::InvalidFieldLength);
}

/// A peer is trusted, and a replicated payload still has its sizes checked: one longer than its
/// fragment gives, and a plain one claiming more content than it holds, are refused.
#[test]
fn parsing_fails_if_the_payload_sizes_disagree() {
    let (fragment, address, payload) = fragment::generate_random();
    let mut longer = payload.to_vec();
    longer.push(0);
    for (label, fragment, payload) in [
        (
            "a payload longer than its fragment",
            fragment,
            Bytes::from(longer),
        ),
        (
            "a plain payload claiming more content",
            Fragment {
                size_content: fragment.size_content + 1,
                ..fragment
            },
            payload,
        ),
    ] {
        let input = Put {
            header: ReplicationHeader {
                correlation_id: Uuid::new_v4(),
                repository: random::<Context>(),
            },
            address,
            fragment,
            flags: 0,
            payload: Some(payload),
        };
        let input_bytes = collapse_bytes_without_header(&input.to_quic_chunks());

        assert_eq!(
            parse(input_bytes),
            Err(MessageParseError::InvalidFieldLength),
            "{label}"
        );
    }
}

#[test]
fn set_flags_are_parsed() {
    let input = PutFlags { force: true };
    let data: u8 = input.clone().into();
    let output: PutFlags = data.into();

    assert_eq!(input, output);
}

#[test]
fn no_flags_are_parsed() {
    let input = PutFlags { force: false };
    let data: u8 = input.clone().into();
    let output: PutFlags = data.into();

    assert_eq!(input, output);
}
