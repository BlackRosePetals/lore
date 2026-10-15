// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT
use std::sync::Arc;

use bytes::Buf;
use bytes::Bytes;
use lore_base::runtime::LORE_CONTEXT;
use lore_base::types::Address;
use lore_base::types::Fragment;
use lore_storage::ImmutableStore;
use lore_storage::StoreError;
use lore_telemetry::tracing::fields::ADDRESS;
use lore_telemetry::tracing::fields::CORRELATION_ID;
use lore_telemetry::tracing::fields::REPOSITORY_ID;
use tracing::Span;
use tracing::debug;
use tracing::info_span;

use crate::protocol::replication_store::REPLICATION_SERVICE_USER_ID;
use crate::protocol::replication_store::header::ReplicationHeader;
use crate::protocol::storage::messages::MessageParseError;
use crate::quic::replication_store_service::server::ParsedReplicationStoreRequest;
use crate::quic::replication_store_service::server::RequestHandler;
use crate::util::setup_execution;

pub const BASE_REQUEST_SIZE: usize = size_of::<ReplicationHeader>() +
        size_of::<Address>() +
        size_of::<Fragment>() +
        // flags
        1;

#[derive(Clone, Debug, PartialEq)]
pub struct Put {
    pub header: ReplicationHeader,
    pub address: Address,
    pub fragment: Fragment,
    pub flags: u8,
    pub payload: Option<Bytes>,
}

impl Put {
    pub fn to_quic_chunks(self) -> [Bytes; 6] {
        [
            Bytes::default(), // command header
            Bytes::from_owner(self.header),
            Bytes::from_owner(self.address),
            Bytes::from_owner(self.fragment),
            Bytes::copy_from_slice(&[self.flags]),
            self.payload.unwrap_or_default(),
        ]
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct PutFlags {
    pub force: bool,
}

impl From<u8> for PutFlags {
    fn from(value: u8) -> Self {
        PutFlags { force: value == 1 }
    }
}

impl From<PutFlags> for u8 {
    fn from(flags: PutFlags) -> Self {
        if flags.force { 1 } else { 0 }
    }
}

/// Parses a replicated put, refusing one whose payload's sizes disagree with its fragment
/// ([`lore_storage::check_payload_sizes`]): a peer is trusted, and every payload still has its
/// sizes checked (ADR-00023).
pub fn parse(mut bytes: Bytes) -> Result<Put, MessageParseError> {
    if bytes.len() < BASE_REQUEST_SIZE {
        return Err(MessageParseError::InvalidFieldLength);
    };

    let header: ReplicationHeader = bytes.split_to(size_of::<ReplicationHeader>()).into();
    let address: Address = bytes.split_to(size_of::<Address>()).into();
    let fragment: Fragment = bytes.split_to(size_of::<Fragment>()).into();
    let flags: u8 = {
        let flags = bytes[0];
        bytes.advance(1);
        flags
    };
    let payload = if !bytes.is_empty() { Some(bytes) } else { None };
    let sizes = match payload.as_ref() {
        Some(payload) => lore_storage::check_payload_sizes(&fragment, payload.len()).is_ok(),
        None => lore_storage::validate_fragment_size(&fragment).is_ok(),
    };
    if !sizes {
        return Err(MessageParseError::InvalidFieldLength);
    }

    Ok(Put {
        header,
        address,
        fragment,
        flags,
        payload,
    })
}

pub fn create_handler(
    bytes: Bytes,
    immutable_store: Arc<dyn ImmutableStore>,
    message_context: &'static str,
) -> Result<ParsedReplicationStoreRequest, MessageParseError> {
    let request = parse(bytes)?;
    let handler = PutHandler {
        immutable_store,
        request,
        message_context,
    };

    Ok(ParsedReplicationStoreRequest::Put(handler))
}

#[derive(Debug)]
pub struct PutHandler {
    immutable_store: Arc<dyn ImmutableStore>,
    pub request: Put,
    message_context: &'static str,
}

#[async_trait::async_trait]
impl RequestHandler for PutHandler {
    fn span(&self) -> Span {
        info_span!("put",
            {CORRELATION_ID} = %self.request.header.correlation_id.as_hyphenated(),
            {REPOSITORY_ID} = %self.request.header.repository,
            message_context = self.message_context)
    }

    async fn run(self) -> Result<Vec<Bytes>, StoreError> {
        debug!({ADDRESS} = %self.request.address,
            payload_length = self.request.payload
                .as_ref().map_or(0, |b| b.len()),
            "put request");

        let execution = setup_execution(
            module_path!(),
            self.request.header.correlation_id.to_string(),
            REPLICATION_SERVICE_USER_ID.to_string(),
        );

        let flags = PutFlags::from(self.request.flags);

        LORE_CONTEXT
            .scope(execution, async move {
                self.immutable_store
                    .put(
                        self.request.header.repository.into(),
                        self.request.address,
                        self.request.fragment,
                        self.request.payload,
                        flags.force,
                    )
                    .await
            })
            .await?;

        Ok(vec![])
    }
}
