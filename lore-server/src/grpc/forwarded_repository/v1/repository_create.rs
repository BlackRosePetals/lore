// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT
use std::sync::Arc;

use lore_proto::lore::repository::v1::RepositoryCreateRequest;
use lore_proto::lore::repository::v1::RepositoryCreateResponse;
use lore_telemetry::InstrumentProvider;
use tonic::Request;
use tonic::Response;
use tonic::Status;

use crate::authnz::repository_registry::RepositoryRegistry;
use crate::grpc::forwarded_requests::CallerContext;
use crate::grpc::repository::v1::repository_create::repository_create_implementation;
use crate::hooks::HookDispatcher;

/// Handler that takes a `RepositoryCreate` request forwarded on from peer's `RepositoryService`
/// and executes it, returning the result to the other server for forwarding on to its
/// client
#[tracing::instrument(name = "ForwardedRepository::v1::RepositoryCreate::Handler", skip_all)]
pub async fn handler(
    request: Request<RepositoryCreateRequest>,
    registry: &RepositoryRegistry,
    immutable_store: Arc<dyn lore_storage::ImmutableStore>,
    mutable_store: Arc<dyn lore_storage::MutableStore>,
    hook_dispatcher: &HookDispatcher,
    instrument_provider: &impl InstrumentProvider,
) -> Result<Response<RepositoryCreateResponse>, Status> {
    let caller_context = CallerContext::from_forwarded_request(&request)?;

    repository_create_implementation(
        request.into_inner(),
        caller_context,
        registry,
        immutable_store,
        mutable_store,
        hook_dispatcher,
        instrument_provider,
    )
    .await
}
