// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT
use std::sync::Arc;

use bytes::Buf;
use bytes::Bytes;
use lore_base::runtime::LORE_CONTEXT;
use lore_base::types::Address;
use lore_base::types::Fragment;
use lore_base::types::Partition;
use lore_storage::ImmutableStore;
use lore_storage::StoreError;
use lore_storage::StoreGetData;
use lore_storage::StoreMatch;
use lore_telemetry::tracing::fields::ADDRESS;
use lore_telemetry::tracing::fields::CORRELATION_ID;
use lore_telemetry::tracing::fields::REPOSITORY_ID;
use tracing::Span;
use tracing::debug;
use tracing::info_span;
use tracing::warn;
use zerocopy::IntoBytes;

use crate::protocol::replication_store::REPLICATION_SERVICE_USER_ID;
use crate::protocol::replication_store::header::ReplicationHeader;
use crate::protocol::storage::messages::MessageParseError;
use crate::quic::replication_store_service::client::ReplicationStoreClientError;
use crate::quic::replication_store_service::server::ParsedReplicationStoreRequest;
use crate::quic::replication_store_service::server::RequestHandler;
use crate::util::setup_execution;

pub const BASE_REQUEST_SIZE: usize = size_of::<ReplicationHeader>() + size_of::<Address>();

/// The fragment, the match level and the partition a `get` or `get_metadata` response carries
/// ahead of any payload.
pub(crate) const RESPONSE_HEADER_SIZE: usize = size_of::<Fragment>() + 1 + size_of::<Partition>();

#[derive(Clone, Debug, PartialEq)]
pub struct Get {
    pub header: ReplicationHeader,
    pub address: Address,
}

impl Get {
    pub fn to_quic_chunks(self) -> [Bytes; 3] {
        [
            Bytes::default(), // command header
            Bytes::from_owner(self.header),
            Bytes::from_owner(self.address),
        ]
    }

    pub fn parse(mut bytes: Bytes) -> Result<Get, MessageParseError> {
        if bytes.len() < BASE_REQUEST_SIZE {
            return Err(MessageParseError::InvalidFieldLength);
        };

        let header: ReplicationHeader = bytes.split_to(size_of::<ReplicationHeader>()).into();
        let address: Address = bytes.split_to(size_of::<Address>()).into();

        Ok(Get { header, address })
    }
}

#[lore_macro::test_pub]
fn serialize_response(data: StoreGetData) -> Vec<Bytes> {
    let match_made: u8 = data.match_made.into();
    let mut response = vec![
        Bytes::copy_from_slice(data.fragment.as_bytes()),
        Bytes::copy_from_slice(&[match_made]),
        Bytes::copy_from_slice(data.partition.as_bytes()),
    ];
    if let Some(payload) = data.payload {
        response.push(payload);
    }
    response
}

/// Parses a replica's get response, refusing a payload whose sizes disagree with its fragment
/// ([`lore_storage::check_payload_sizes`]): a replica is trusted, and every payload still has its
/// sizes checked (ADR-00023).
pub fn parse_response(mut bytes: Bytes) -> Result<StoreGetData, ReplicationStoreClientError> {
    if bytes.len() < RESPONSE_HEADER_SIZE {
        return Err(ReplicationStoreClientError::ResponseError(
            "get response shorter than its header",
        ));
    }
    let fragment: Fragment = bytes.split_to(size_of::<Fragment>()).into();
    let match_made: StoreMatch = bytes[0].try_into().map_err(|error| {
        warn!(?error, "failed to parse match_made");
        ReplicationStoreClientError::ResponseError("failed to parse match_made from get response")
    })?;
    bytes.advance(1);
    let partition: Partition = bytes.split_to(size_of::<Partition>()).into();
    lore_storage::check_payload_sizes(&fragment, bytes.len()).map_err(|error| {
        warn!(?error, "replica payload sizes disagree with its fragment");
        ReplicationStoreClientError::ResponseError(
            "payload sizes disagree with the fragment in get response",
        )
    })?;
    Ok(StoreGetData {
        fragment,
        match_made,
        partition,
        payload: Some(bytes),
        content: None,
    })
}

pub fn create_handler(
    bytes: Bytes,
    immutable_store: Arc<dyn ImmutableStore>,
    message_context: &'static str,
) -> Result<ParsedReplicationStoreRequest, MessageParseError> {
    let request = Get::parse(bytes)?;
    let handler = GetHandler {
        immutable_store,
        request,
        message_context,
    };

    Ok(ParsedReplicationStoreRequest::Get(handler))
}

#[derive(Debug)]
pub struct GetHandler {
    immutable_store: Arc<dyn ImmutableStore>,
    pub request: Get,
    message_context: &'static str,
}

#[async_trait::async_trait]
impl RequestHandler for GetHandler {
    fn span(&self) -> Span {
        info_span!("get",
            {CORRELATION_ID} = %self.request.header.correlation_id.as_hyphenated(),
            {REPOSITORY_ID} = %self.request.header.repository,
            message_context = self.message_context)
    }

    async fn run(self) -> Result<Vec<Bytes>, StoreError> {
        debug!(
            {{ ADDRESS }} = %self.request.address,
            "get request"
        );

        let execution = setup_execution(
            module_path!(),
            self.request.header.correlation_id.to_string(),
            REPLICATION_SERVICE_USER_ID.to_string(),
        );

        let result = LORE_CONTEXT
            .scope(execution, async move {
                self.immutable_store
                    .get(self.request.header.repository.into(), self.request.address)
                    .await
            })
            .await?;

        Ok(serialize_response(result))
    }
}
