// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT
use lore_base::types::Context;
use lore_revision::fragment;
use lore_server::protocol::replication_store::get_metadata::*;
use lore_server::protocol::storage::messages::MessageParseError;
use lore_server::quic::replication_store_service::client::ReplicationStoreClientError;
use lore_server::quic::tests::collapse_bytes;
use lore_server::quic::tests::collapse_bytes_without_header;
use lore_storage::StoreGetData;
use lore_storage::StoreMatch;
use rand::random;
use uuid::Uuid;

mod request {
    use lore_base::types::Address;
    use lore_server::protocol::replication_store::header::ReplicationHeader;

    use super::*;

    #[test]
    fn parsing_works() {
        let repository = random::<Context>();
        let (_, address, _) = fragment::generate_random();

        let input = GetMetadata {
            header: ReplicationHeader {
                correlation_id: Uuid::new_v4(),
                repository,
            },
            address,
        };
        let input_bytes = collapse_bytes_without_header(&input.clone().to_quic_chunks());
        let output = GetMetadata::parse(input_bytes).expect("parse should work");

        assert_eq!(input, output);
    }

    #[test]
    fn parsing_fails_if_truncated() {
        let repository = random::<Context>();

        let input = GetMetadata {
            header: ReplicationHeader {
                correlation_id: Uuid::new_v4(),
                repository,
            },
            address: Address::default(),
        };
        let mut input_bytes = collapse_bytes_without_header(&input.to_quic_chunks());
        input_bytes.truncate(input_bytes.len() - 1);
        let output = GetMetadata::parse(input_bytes).expect_err("parse should fail");

        assert!(matches!(output, MessageParseError::InvalidFieldLength));
    }
}

mod response {
    use super::*;

    #[test]
    fn serialize_response_has_three_chunks() {
        let (fragment, _, _) = fragment::generate_random();
        let data = StoreGetData {
            fragment,
            match_made: StoreMatch::MatchFull,
            partition: random::<lore_base::types::Partition>(),
            payload: None,
            content: None,
        };
        // if this assertion fails, then new servers sending
        // this response to old clients will be silently wrong
        // and could end up parsing incorrect payloads.
        assert_eq!(serialize_response(data).len(), 3);
    }

    #[test]
    fn response_roundtrips() {
        let (fragment, _, _) = fragment::generate_random();
        let original = StoreGetData {
            fragment,
            match_made: StoreMatch::MatchFull,
            partition: random::<lore_base::types::Partition>(),
            payload: None,
            content: None,
        };
        let bytes = serialize_response(original.clone());
        let reparsed = parse_response(collapse_bytes(&bytes)).expect("parse should work");
        assert_eq!(reparsed.fragment, original.fragment);
        assert_eq!(reparsed.match_made, original.match_made);
        assert_eq!(reparsed.partition, original.partition);
        assert_eq!(reparsed.payload, None);
    }

    #[test]
    fn a_response_shorter_than_its_header_is_refused() {
        let (fragment, _, _) = fragment::generate_random();
        let data = StoreGetData {
            fragment,
            match_made: StoreMatch::MatchFull,
            partition: random::<lore_base::types::Partition>(),
            payload: None,
            content: None,
        };
        let bytes = collapse_bytes(&serialize_response(data));
        let parsed = parse_response(bytes.slice(..bytes.len() - 1));
        assert!(
            matches!(parsed, Err(ReplicationStoreClientError::ResponseError(_))),
            "{parsed:?}"
        );
    }
}
