// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT
use std::sync::Arc;

use bytes::Bytes;
use lore_base::runtime::LORE_CONTEXT;
use lore_base::types::Address;
use lore_base::types::Context;
use lore_base::types::FragmentFlags;
use lore_revision::fragment;
use lore_server::protocol::attribute_map::AttributeMap;
use lore_server::protocol::client_identify::ClientIdentify;
use lore_server::protocol::client_identify::UserAgentValue;
use lore_server::protocol::replication_store::get;
use lore_server::protocol::replication_store::get::Get;
use lore_server::protocol::replication_store::get_metadata;
use lore_server::protocol::replication_store::get_metadata::GetMetadata;
use lore_server::protocol::replication_store::header::ReplicationHeader;
use lore_server::protocol::replication_store::obliterate::Obliterate;
use lore_server::protocol::replication_store::put::Put;
use lore_server::protocol::replication_store::query::Query;
use lore_server::protocol::replication_store::query::QueryResponse;
use lore_server::quic::QuicService;
use lore_server::quic::replication_store_service::server::*;
use lore_server::quic::replication_store_service::*;
use lore_server::quic::tests::collapse_bytes;
use lore_server::quic::tests::collapse_bytes_without_header;
use lore_storage::ImmutableStore;
use lore_storage::StoreMatch;
use lore_storage::StoreMatchResult;
use lore_telemetry::user_agent_filter::UserAgentFilter;
use lore_transport::quic::QuicOpCode;
use lore_transport::quic::command_header::CommandHeader;
use rand::random;
use uuid::Uuid;
use zerocopy::IntoBytes;

use crate::store::test_support::test_store_create;

fn make_service_with_filter(
    immutable_store: Arc<dyn lore_storage::ImmutableStore>,
    user_agent_filter: Arc<UserAgentFilter>,
) -> ReplicationStoreService {
    ReplicationStoreService::new(immutable_store.clone(), immutable_store, user_agent_filter)
}

mod client_identify {
    use super::*;

    #[tokio::test]
    async fn parse_returns_variant() {
        let (immutable_store, _, _exec) =
            test_store_create().await.expect("Failed to create stores");
        let service =
            make_service_with_filter(immutable_store, Arc::new(UserAgentFilter::default()));

        let header = CommandHeader::new(Command::ClientIdentify as QuicOpCode, 0, 0);
        let parsed = service
            .parse_request_bytes(&header, bytes::Bytes::from("my-client/1.0"))
            .expect("parse should succeed");

        assert!(matches!(
            parsed,
            ParsedReplicationStoreRequest::ClientIdentify(_)
        ));
    }

    #[tokio::test]
    async fn handler_returns_empty_ok() {
        let (immutable_store, _, _exec) =
            test_store_create().await.expect("Failed to create stores");
        let service =
            make_service_with_filter(immutable_store, Arc::new(UserAgentFilter::default()));

        let request = ParsedReplicationStoreRequest::ClientIdentify(ClientIdentifyHandler {
            message: ClientIdentify {
                user_agent: Some("my-client/1.0".to_string()),
                is_trusted: true,
            },
        });

        let result = service
            .run_request_handler(Arc::new(AttributeMap::default()), request)
            .await
            .expect("handler should not fail");

        assert!(result.is_empty());
    }

    #[tokio::test]
    async fn handler_stores_user_agent_in_context() {
        let (immutable_store, _, _exec) =
            test_store_create().await.expect("Failed to create stores");
        let service =
            make_service_with_filter(immutable_store, Arc::new(UserAgentFilter::default()));

        let context = Arc::new(AttributeMap::default());
        let request = ParsedReplicationStoreRequest::ClientIdentify(ClientIdentifyHandler {
            message: ClientIdentify {
                user_agent: Some("my-client/1.0".to_string()),
                is_trusted: true,
            },
        });

        service
            .run_request_handler(context.clone(), request)
            .await
            .expect("handler should not fail");

        let stored = context
            .get::<UserAgentValue>()
            .expect("UserAgentValue should have been inserted into context");
        assert_eq!(&*stored.0, "my-client/1.0");
    }
}

#[tokio::test]
async fn immutable_put_works_end_to_end() {
    let (immutable_store, _, execution) =
        test_store_create().await.expect("Failed to create stores");

    let repository = random::<Context>();
    let (fragment, address, payload) = fragment::generate_random();

    // sanity check the above address does not exist in the store
    {
        let immutable_store = immutable_store.clone();
        LORE_CONTEXT
            .scope(execution.clone(), async move {
                assert!(
                    immutable_store
                        .clone()
                        .get(repository.into(), address)
                        .await
                        .unwrap_err()
                        .is_address_not_found()
                );
            })
            .await;
    }

    let request = Put {
        header: ReplicationHeader {
            correlation_id: Uuid::new_v4(),
            repository,
        },
        address,
        fragment,
        flags: 0,
        payload: Some(payload.clone()),
    };

    let service = ReplicationStoreService::new(
        immutable_store.clone(),
        immutable_store.clone(),
        Arc::new(UserAgentFilter::default()),
    );

    let parse_output = service
        .parse_request_bytes(
            &CommandHeader::new(Command::ImmutablePut as QuicOpCode, 0, 0),
            collapse_bytes_without_header(&request.to_quic_chunks()),
        )
        .expect("Failed to parse");
    assert!(matches!(
        parse_output,
        ParsedReplicationStoreRequest::Put(_)
    ));

    let handle_output = service
        .run_request_handler(AttributeMap::default().into(), parse_output)
        .await
        .expect("handler failed");
    assert!(handle_output.is_empty());

    LORE_CONTEXT
        .scope(execution, async move {
            let get_output = immutable_store
                .get(repository.into(), address)
                .await
                .and_then(lore_storage::StoreGetData::into_payload)
                .expect("get should have worked");
            assert_eq!(get_output.1, payload);
        })
        .await;
}

#[tokio::test]
async fn immutable_query_works_end_to_end() {
    let (immutable_store, _, execution) =
        test_store_create().await.expect("Failed to create stores");

    let repository = random::<Context>();

    let address_match_full = {
        let (fragment, address, payload) = fragment::generate_random();
        let immutable_store = immutable_store.clone();
        LORE_CONTEXT
            .scope(execution.clone(), async move {
                immutable_store
                    .put(repository.into(), address, fragment, Some(payload), false)
                    .await
                    .expect("put should work");
            })
            .await;
        address
    };

    let address_other_repository = {
        let other_repository = random::<Context>();
        let (fragment, address, payload) = fragment::generate_random();
        let immutable_store = immutable_store.clone();
        LORE_CONTEXT
            .scope(execution.clone(), async move {
                immutable_store
                    .put(
                        other_repository.into(),
                        address,
                        fragment,
                        Some(payload),
                        false,
                    )
                    .await
                    .expect("put should work");
            })
            .await;
        address
    };

    let address_different_context = {
        let (fragment, address, payload) = fragment::generate_random();
        let immutable_store = immutable_store.clone();
        LORE_CONTEXT
            .scope(execution.clone(), async move {
                immutable_store
                    .put(repository.into(), address, fragment, Some(payload), false)
                    .await
                    .expect("put should work");
            })
            .await;
        Address {
            hash: address.hash,
            context: random::<Context>(),
        }
    };

    let (_, address_no_match, _) = fragment::generate_random();

    let addresses = vec![
        address_match_full,
        address_other_repository,
        address_different_context,
        address_no_match,
    ];

    let request = Query {
        header: ReplicationHeader {
            correlation_id: Uuid::new_v4(),
            repository,
        },
        addresses: addresses.clone(),
    };

    let service = ReplicationStoreService::new(
        immutable_store.clone(),
        immutable_store.clone(),
        Arc::new(UserAgentFilter::default()),
    );

    let parse_output = service
        .parse_request_bytes(
            &CommandHeader::new(Command::ImmutableQuery as QuicOpCode, 0, 0),
            collapse_bytes_without_header(&request.to_quic_chunks()),
        )
        .expect("Failed to parse");
    assert!(matches!(
        parse_output,
        ParsedReplicationStoreRequest::Query(_)
    ));

    let handle_output = service
        .run_request_handler(AttributeMap::default().into(), parse_output)
        .await
        .expect("handler failed");

    // the response matches what the store returns directly
    let direct_store_output = LORE_CONTEXT
        .scope(execution.clone(), async move {
            let mut resolved = vec![StoreMatchResult::default(); addresses.len()];
            immutable_store
                .query(repository.into(), &addresses, &mut resolved)
                .await
                .expect("direct should work");
            resolved
        })
        .await;

    let response =
        QueryResponse::parse(collapse_bytes(&handle_output)).expect("response parse should work");
    assert_eq!(response.results, direct_store_output);
}

#[tokio::test]
async fn immutable_get_works_end_to_end() {
    let (immutable_store, _, execution) =
        test_store_create().await.expect("Failed to create stores");

    let repository = random::<Context>();

    let (fragment, address, payload) = fragment::generate_random();
    {
        let payload = payload.clone();
        let immutable_store = immutable_store.clone();
        LORE_CONTEXT
            .scope(execution.clone(), async move {
                immutable_store
                    .clone()
                    .put(repository.into(), address, fragment, Some(payload), false)
                    .await
                    .expect("put should work");
            })
            .await;
    };

    let request = Get {
        header: ReplicationHeader {
            correlation_id: Uuid::new_v4(),
            repository,
        },
        address,
    };

    let service = ReplicationStoreService::new(
        immutable_store.clone(),
        immutable_store.clone(),
        Arc::new(UserAgentFilter::default()),
    );

    let parse_output = service
        .parse_request_bytes(
            &CommandHeader::new(Command::ImmutableGet as QuicOpCode, 0, 0),
            collapse_bytes_without_header(&request.to_quic_chunks()),
        )
        .expect("Failed to parse");
    assert!(matches!(
        parse_output,
        ParsedReplicationStoreRequest::Get(_)
    ));

    let handle_output = service
        .run_request_handler(AttributeMap::default().into(), parse_output)
        .await
        .expect("handler failed");
    let response_parsed =
        get::parse_response(collapse_bytes(&handle_output)).expect("response parse should work");
    // The store that served it holds the payload, and says so.
    let mut held = fragment;
    held.flags |= FragmentFlags::PayloadStoredLocal.bits();
    assert_eq!(response_parsed.fragment, held);
    assert_eq!(response_parsed.payload, Some(payload));
}

#[tokio::test]
async fn obliterate_works_end_to_end() {
    let (immutable_store, _, execution) =
        test_store_create().await.expect("Failed to create stores");

    let repository = random::<Context>();
    let (fragment, address, payload) = fragment::generate_random();

    let get_address = || {
        let execution = execution.clone();
        let immutable_store = immutable_store.clone();
        async move {
            LORE_CONTEXT
                .scope(execution, async move {
                    immutable_store.get(repository.into(), address).await
                })
                .await
        }
    };

    // set up an address for deletion
    {
        let immutable_store = immutable_store.clone();
        LORE_CONTEXT
            .scope(execution.clone(), async move {
                immutable_store
                    .clone()
                    .put(repository.into(), address, fragment, Some(payload), false)
                    .await
                    .expect("put should work");
            })
            .await;
    }
    get_address().await.expect("address should exist");

    let request = Obliterate {
        header: ReplicationHeader {
            correlation_id: Uuid::new_v4(),
            repository,
        },
        address,
    };

    let service = ReplicationStoreService::new(
        immutable_store.clone(),
        immutable_store.clone(),
        Arc::new(UserAgentFilter::default()),
    );

    let parse_output = service
        .parse_request_bytes(
            &CommandHeader::new(Command::ImmutableObliterate as QuicOpCode, 0, 0),
            collapse_bytes_without_header(&request.to_quic_chunks()),
        )
        .expect("Failed to parse");
    assert!(matches!(
        parse_output,
        ParsedReplicationStoreRequest::Obliterate(_)
    ));

    let handle_output = service
        .run_request_handler(AttributeMap::default().into(), parse_output)
        .await
        .expect("handler failed");
    assert_eq!(
        handle_output,
        vec![
            Bytes::copy_from_slice(1u64.as_bytes()),
            Bytes::copy_from_slice(1u64.as_bytes()),
        ]
    );

    get_address()
        .await
        .expect_err("address should have been obliterated");
}

#[tokio::test]
async fn query_works_end_to_end() {
    let (immutable_store, _, execution) =
        test_store_create().await.expect("Failed to create stores");

    let repository = random::<Context>();

    let (fragment, address, payload) = fragment::generate_random();
    {
        let immutable_store = immutable_store.clone();
        LORE_CONTEXT
            .scope(execution.clone(), async move {
                immutable_store
                    .clone()
                    .put(repository.into(), address, fragment, Some(payload), false)
                    .await
                    .expect("put should work");
            })
            .await;

        (fragment, address)
    };

    let request = GetMetadata {
        header: ReplicationHeader {
            correlation_id: Uuid::new_v4(),
            repository,
        },
        address,
    };

    let service = ReplicationStoreService::new(
        immutable_store.clone(),
        immutable_store.clone(),
        Arc::new(UserAgentFilter::default()),
    );

    let parse_output = service
        .parse_request_bytes(
            &CommandHeader::new(Command::ImmutableGetMetadata as QuicOpCode, 0, 0),
            collapse_bytes_without_header(&request.to_quic_chunks()),
        )
        .expect("Failed to parse");
    assert!(matches!(
        parse_output,
        ParsedReplicationStoreRequest::GetMetadata(_)
    ));

    let service_output = service
        .run_request_handler(AttributeMap::default().into(), parse_output)
        .await
        .expect("handler failed");
    let parsed_response =
        get_metadata::parse_response(collapse_bytes(&service_output)).expect("Failed to parse");

    let store_direct_output = LORE_CONTEXT
        .scope(execution.clone(), async move {
            immutable_store
                .clone()
                .get_metadata(repository.into(), address)
                .await
                .expect("get_metadata should work")
        })
        .await;

    assert_eq!(parsed_response.fragment, store_direct_output.fragment);
    assert_eq!(parsed_response.match_made, store_direct_output.match_made);
    assert_eq!(parsed_response.partition, store_direct_output.partition);
}

#[tokio::test]
async fn get_metadata_works_end_to_end() {
    let (immutable_store, _, execution) =
        test_store_create().await.expect("Failed to create stores");

    let repository = random::<Context>();
    let (fragment, address, payload) = fragment::generate_random();
    {
        let immutable_store = immutable_store.clone();
        LORE_CONTEXT
            .scope(execution.clone(), async move {
                immutable_store
                    .clone()
                    .put(repository.into(), address, fragment, Some(payload), false)
                    .await
                    .expect("put should work");
            })
            .await;
    }

    let request = GetMetadata {
        header: ReplicationHeader {
            correlation_id: Uuid::new_v4(),
            repository,
        },
        address,
    };

    let service = ReplicationStoreService::new(
        immutable_store.clone(),
        immutable_store.clone(),
        Arc::new(UserAgentFilter::default()),
    );

    let parse_output = service
        .parse_request_bytes(
            &CommandHeader::new(Command::ImmutableGetMetadata as QuicOpCode, 0, 0),
            collapse_bytes_without_header(&request.to_quic_chunks()),
        )
        .expect("Failed to parse");
    assert!(matches!(
        parse_output,
        ParsedReplicationStoreRequest::GetMetadata(_)
    ));

    let service_output = service
        .run_request_handler(AttributeMap::default().into(), parse_output)
        .await
        .expect("handler failed");
    let parsed_response =
        get_metadata::parse_response(collapse_bytes(&service_output)).expect("Failed to parse");

    let store_direct_output = LORE_CONTEXT
        .scope(execution.clone(), async move {
            immutable_store
                .clone()
                .get_metadata(repository.into(), address)
                .await
                .expect("get_metadata should work")
        })
        .await;

    assert_eq!(parsed_response.fragment, store_direct_output.fragment);
    assert_eq!(parsed_response.match_made, store_direct_output.match_made);
    assert_eq!(parsed_response.partition, store_direct_output.partition);
}

#[tokio::test]
async fn immutable_local_get_metadata_routes_to_local_store() {
    let (main_store, local_store, execution) = create_two_stores().await;

    let repository = random::<Context>();
    let (fragment, address, payload) = fragment::generate_random();

    // put data only in the local store
    {
        let local_store = local_store.clone();
        LORE_CONTEXT
            .scope(execution.clone(), async move {
                local_store
                    .put(repository.into(), address, fragment, Some(payload), false)
                    .await
                    .expect("put should work");
            })
            .await;
    }

    let request = GetMetadata {
        header: ReplicationHeader {
            correlation_id: Uuid::new_v4(),
            repository,
        },
        address,
    };

    let service = ReplicationStoreService::new(
        main_store.clone(),
        local_store.clone(),
        Arc::new(UserAgentFilter::default()),
    );

    // ImmutableLocalGetMetadata should find the data via the local store
    let parse_output = service
        .parse_request_bytes(
            &CommandHeader::new(Command::ImmutableLocalGetMetadata as QuicOpCode, 0, 0),
            collapse_bytes_without_header(&request.clone().to_quic_chunks()),
        )
        .expect("Failed to parse");
    assert!(matches!(
        parse_output,
        ParsedReplicationStoreRequest::GetMetadata(_)
    ));

    let service_output = service
        .run_request_handler(AttributeMap::default().into(), parse_output)
        .await
        .expect("handler failed");
    let parsed_response =
        get_metadata::parse_response(collapse_bytes(&service_output)).expect("Failed to parse");
    assert_eq!(parsed_response.match_made, StoreMatch::MatchFull);

    // Regular ImmutableGetMetadata should NOT find it (main store is empty)
    let parse_output = service
        .parse_request_bytes(
            &CommandHeader::new(Command::ImmutableGetMetadata as QuicOpCode, 0, 0),
            collapse_bytes_without_header(&request.to_quic_chunks()),
        )
        .expect("Failed to parse");

    let service_output = service
        .run_request_handler(AttributeMap::default().into(), parse_output)
        .await
        .expect("handler should succeed even for a miss");
    let parsed_response =
        get_metadata::parse_response(collapse_bytes(&service_output)).expect("Failed to parse");
    assert_eq!(parsed_response.match_made, StoreMatch::MatchNone);
}

/// Helper to create a second independent store for local-store routing tests
async fn create_two_stores() -> (
    Arc<dyn ImmutableStore>,
    Arc<dyn ImmutableStore>,
    Arc<lore_revision::interface::ExecutionContext>,
) {
    let (main_store, _, execution) = test_store_create()
        .await
        .expect("Failed to create main store");
    let (local_store, _, _) = test_store_create()
        .await
        .expect("Failed to create local store");
    (main_store, local_store, execution)
}

#[tokio::test]
async fn immutable_local_query_routes_to_local_store() {
    let (main_store, local_store, execution) = create_two_stores().await;

    let repository = random::<Context>();
    let (fragment, address, payload) = fragment::generate_random();

    // put data only in the local store
    {
        let local_store = local_store.clone();
        LORE_CONTEXT
            .scope(execution.clone(), async move {
                local_store
                    .put(repository.into(), address, fragment, Some(payload), false)
                    .await
                    .expect("put should work");
            })
            .await;
    }

    let request = Query {
        header: ReplicationHeader {
            correlation_id: Uuid::new_v4(),
            repository,
        },
        addresses: vec![address],
    };

    let service = ReplicationStoreService::new(
        main_store.clone(),
        local_store.clone(),
        Arc::new(UserAgentFilter::default()),
    );

    // ImmutableLocalQuery should find the data via the local store
    let parse_output = service
        .parse_request_bytes(
            &CommandHeader::new(Command::ImmutableLocalQuery as QuicOpCode, 0, 0),
            collapse_bytes_without_header(&request.clone().to_quic_chunks()),
        )
        .expect("Failed to parse");
    assert!(matches!(
        parse_output,
        ParsedReplicationStoreRequest::Query(_)
    ));

    let handle_output = service
        .run_request_handler(AttributeMap::default().into(), parse_output)
        .await
        .expect("handler failed");
    let response =
        QueryResponse::parse(collapse_bytes(&handle_output)).expect("response parse should work");
    assert_eq!(response.results[0].match_made, StoreMatch::MatchFull);

    // ImmutableQuery should NOT find it (main store is empty)
    let parse_output = service
        .parse_request_bytes(
            &CommandHeader::new(Command::ImmutableQuery as QuicOpCode, 0, 0),
            collapse_bytes_without_header(&request.to_quic_chunks()),
        )
        .expect("Failed to parse");

    let handle_output = service
        .run_request_handler(AttributeMap::default().into(), parse_output)
        .await
        .expect("handler failed");
    let response =
        QueryResponse::parse(collapse_bytes(&handle_output)).expect("response parse should work");
    assert_eq!(response.results[0].match_made, StoreMatch::MatchNone);
}

#[tokio::test]
async fn immutable_local_get_routes_to_local_store() {
    let (main_store, local_store, execution) = create_two_stores().await;

    let repository = random::<Context>();
    let (fragment, address, payload) = fragment::generate_random();

    // put data only in the local store
    {
        let payload = payload.clone();
        let local_store = local_store.clone();
        LORE_CONTEXT
            .scope(execution.clone(), async move {
                local_store
                    .put(repository.into(), address, fragment, Some(payload), false)
                    .await
                    .expect("put should work");
            })
            .await;
    }

    let request = Get {
        header: ReplicationHeader {
            correlation_id: Uuid::new_v4(),
            repository,
        },
        address,
    };

    let service = ReplicationStoreService::new(
        main_store.clone(),
        local_store.clone(),
        Arc::new(UserAgentFilter::default()),
    );

    // ImmutableLocalGet should find the data via the local store
    let parse_output = service
        .parse_request_bytes(
            &CommandHeader::new(Command::ImmutableLocalGet as QuicOpCode, 0, 0),
            collapse_bytes_without_header(&request.clone().to_quic_chunks()),
        )
        .expect("Failed to parse");
    assert!(matches!(
        parse_output,
        ParsedReplicationStoreRequest::Get(_)
    ));

    let handle_output = service
        .run_request_handler(AttributeMap::default().into(), parse_output)
        .await
        .expect("handler failed");
    let response_parsed =
        get::parse_response(collapse_bytes(&handle_output)).expect("response parse should work");
    // The store that served it holds the payload, and says so.
    let mut held = fragment;
    held.flags |= FragmentFlags::PayloadStoredLocal.bits();
    assert_eq!(response_parsed.fragment, held);
    assert_eq!(response_parsed.payload, Some(payload));

    // Regular ImmutableGet should NOT find it (main store is empty)
    let parse_output = service
        .parse_request_bytes(
            &CommandHeader::new(Command::ImmutableGet as QuicOpCode, 0, 0),
            collapse_bytes_without_header(&request.to_quic_chunks()),
        )
        .expect("Failed to parse");

    let handle_output = service
        .run_request_handler(AttributeMap::default().into(), parse_output)
        .await;
    assert!(handle_output.unwrap_err().is_address_not_found());
}

#[tokio::test]
async fn immutable_local_put_routes_to_local_store() {
    let (main_store, local_store, execution) = create_two_stores().await;

    let repository = random::<Context>();
    let (fragment, address, payload) = fragment::generate_random();

    // sanity check the address does not exist in either store
    {
        let local_store = local_store.clone();
        LORE_CONTEXT
            .scope(execution.clone(), async move {
                assert!(
                    local_store
                        .get(repository.into(), address)
                        .await
                        .unwrap_err()
                        .is_address_not_found()
                );
            })
            .await;
    }

    let request = Put {
        header: ReplicationHeader {
            correlation_id: Uuid::new_v4(),
            repository,
        },
        address,
        fragment,
        flags: 0,
        payload: Some(payload.clone()),
    };

    let service = ReplicationStoreService::new(
        main_store.clone(),
        local_store.clone(),
        Arc::new(UserAgentFilter::default()),
    );

    // ImmutableLocalPut should write to the local store
    let parse_output = service
        .parse_request_bytes(
            &CommandHeader::new(Command::ImmutableLocalPut as QuicOpCode, 0, 0),
            collapse_bytes_without_header(&request.to_quic_chunks()),
        )
        .expect("Failed to parse");
    assert!(matches!(
        parse_output,
        ParsedReplicationStoreRequest::Put(_)
    ));

    let handle_output = service
        .run_request_handler(AttributeMap::default().into(), parse_output)
        .await
        .expect("handler failed");
    assert!(handle_output.is_empty());

    // Verify the data landed in the local store
    {
        let local_store = local_store.clone();
        LORE_CONTEXT
            .scope(execution.clone(), async move {
                let get_output = local_store
                    .get(repository.into(), address)
                    .await
                    .and_then(lore_storage::StoreGetData::into_payload)
                    .expect("get from local store should work");
                assert_eq!(get_output.1, payload);
            })
            .await;
    }

    // Verify the data is NOT in the main store
    LORE_CONTEXT
        .scope(execution, async move {
            assert!(
                main_store
                    .get(repository.into(), address)
                    .await
                    .unwrap_err()
                    .is_address_not_found()
            );
        })
        .await;
}
