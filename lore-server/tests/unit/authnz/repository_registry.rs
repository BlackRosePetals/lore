// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT
use std::str::FromStr;
use std::sync::Arc;
use std::sync::Mutex;

use lore_base::types::Context;
use lore_proto::lore::user::v1::PartitionAddRequest;
use lore_proto::lore::user::v1::PartitionAddResponse;
use lore_proto::lore::user::v1::PartitionListRequest;
use lore_proto::lore::user::v1::PartitionListResponse;
use lore_proto::lore::user::v1::PartitionRemoveRequest;
use lore_proto::lore::user::v1::PartitionRemoveResponse;
use lore_proto::lore::user::v1::UserFindRequest;
use lore_proto::lore::user::v1::UserFindResponse;
use lore_proto::lore::user::v1::UserGetRequest;
use lore_proto::lore::user::v1::UserGetResponse;
use lore_proto::lore::user::v1::user_service_server::UserService;
use lore_proto::lore::user::v1::user_service_server::UserServiceServer;
use lore_proto::rebac::CreateResourceRequest;
use lore_revision::environment::EnvironmentConfig;
use lore_revision::lore::RepositoryId;
use lore_server::authnz::rebac::RebacApiClient;
use lore_server::authnz::repository_registry::*;
use lore_transport::Endpoint;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::Code;
use tonic::Request;
use tonic::Status;

const AUTH_URL: &str = "https://auth.example.com";
const USER_URL: &str = "https://directory.example.com";

fn repository() -> RepositoryId {
    Context::from_str("2fc8bf934117e250152eba9a1fc78e71")
        .unwrap()
        .into()
}

#[test]
fn selection_follows_user_url_then_auth_url() {
    let user_service = RepositoryRegistry::UserService(USER_URL.to_string());
    let auth_service = RepositoryRegistry::AuthService(AUTH_URL.to_string());
    let cases = [
        (Some(AUTH_URL), Some(USER_URL), &user_service),
        (None, Some(USER_URL), &user_service),
        (Some(AUTH_URL), None, &auth_service),
        (Some(AUTH_URL), Some(""), &auth_service),
        (None, Some(""), &RepositoryRegistry::Unregistered),
        (None, None, &RepositoryRegistry::Unregistered),
    ];
    for (auth_url, user_url, expected) in cases {
        assert_eq!(
            &RepositoryRegistry::select(auth_url, user_url),
            expected,
            "auth_url {auth_url:?}, user_url {user_url:?}"
        );
    }
}

#[test]
fn selection_reads_the_environment_endpoint() {
    let environment = EnvironmentConfig {
        endpoint: Some(Endpoint {
            auth_url: Some(AUTH_URL.to_string()),
            user_url: Some(USER_URL.to_string()),
            ..Endpoint::default()
        }),
        ..EnvironmentConfig::default()
    };
    assert_eq!(
        RepositoryRegistry::of(&environment),
        RepositoryRegistry::UserService(USER_URL.to_string())
    );
    assert_eq!(
        RepositoryRegistry::of(&EnvironmentConfig::default()),
        RepositoryRegistry::Unregistered
    );
}

#[test]
fn display_names_the_registry() {
    assert_eq!(RepositoryRegistry::Unregistered.to_string(), "none");
    assert_eq!(
        RepositoryRegistry::AuthService(AUTH_URL.to_string()).to_string(),
        format!("RebacApi({AUTH_URL})")
    );
    assert_eq!(
        RepositoryRegistry::UserService(USER_URL.to_string()).to_string(),
        format!("UserService({USER_URL})")
    );
}

#[test]
fn add_outcome_maps_the_registration_status() {
    let outcome = |status: Status| add_outcome(Err(status), repository(), "org/repo", "rpc");
    add_outcome(Ok(()), repository(), "org/repo", "rpc").expect("OK continues the create");
    outcome(Status::already_exists("")).expect("AlreadyExists continues the create");
    let cases = [
        (Status::permission_denied(""), Code::PermissionDenied),
        (Status::unauthenticated(""), Code::Unauthenticated),
        (Status::not_found(""), Code::FailedPrecondition),
        (Status::unavailable("down"), Code::Internal),
    ];
    for (status, expected) in cases {
        assert_eq!(outcome(status).unwrap_err().code(), expected);
    }
    let err = outcome(Status::unavailable("down")).unwrap_err();
    assert!(err.message().contains("Failed to call rpc: "), "{err}");
}

#[test]
fn remove_outcome_maps_the_unregistration_status() {
    remove_outcome(Ok(()), "rpc").expect("OK continues the delete");
    let cases = [
        (Status::permission_denied(""), Code::PermissionDenied),
        (Status::unauthenticated(""), Code::Unauthenticated),
        (Status::not_found(""), Code::Internal),
    ];
    for (status, expected) in cases {
        assert_eq!(
            remove_outcome(Err(status), "rpc").unwrap_err().code(),
            expected
        );
    }
}

#[tokio::test]
async fn unregistered_sends_nothing() {
    RepositoryRegistry::Unregistered
        .add(None, repository(), "org/repo")
        .await
        .expect("nothing to register with");
    RepositoryRegistry::Unregistered
        .remove(None, repository())
        .await
        .expect("nothing to unregister from");
}

/// Records `PartitionAdd` and `PartitionRemove` requests with their
/// `authorization` header, answering each with `status`, `OK` when `None`.
#[derive(Default)]
struct Directory {
    status: Option<Code>,
    adds: Mutex<Vec<(PartitionAddRequest, Option<String>)>>,
    removes: Mutex<Vec<(PartitionRemoveRequest, Option<String>)>>,
}

fn authorization<T>(request: &tonic::Request<T>) -> Option<String> {
    request
        .metadata()
        .get("authorization")
        .map(|value| value.to_str().unwrap().to_string())
}

impl Directory {
    fn answer<T: Default>(&self) -> Result<tonic::Response<T>, Status> {
        match self.status {
            None => Ok(tonic::Response::new(T::default())),
            Some(code) => Err(Status::new(code, "answered by the test")),
        }
    }
}

#[tonic::async_trait]
impl UserService for Directory {
    async fn user_get(
        &self,
        _request: tonic::Request<UserGetRequest>,
    ) -> Result<tonic::Response<UserGetResponse>, Status> {
        Err(Status::unimplemented("not under test"))
    }

    async fn user_find(
        &self,
        _request: tonic::Request<UserFindRequest>,
    ) -> Result<tonic::Response<UserFindResponse>, Status> {
        Err(Status::unimplemented("not under test"))
    }

    type PartitionListStream = tokio_stream::Empty<Result<PartitionListResponse, Status>>;

    async fn partition_list(
        &self,
        _request: tonic::Request<PartitionListRequest>,
    ) -> Result<tonic::Response<Self::PartitionListStream>, Status> {
        Err(Status::unimplemented("not under test"))
    }

    async fn partition_add(
        &self,
        request: tonic::Request<PartitionAddRequest>,
    ) -> Result<tonic::Response<PartitionAddResponse>, Status> {
        let authorization = authorization(&request);
        self.adds
            .lock()
            .unwrap()
            .push((request.into_inner(), authorization));
        self.answer()
    }

    async fn partition_remove(
        &self,
        request: tonic::Request<PartitionRemoveRequest>,
    ) -> Result<tonic::Response<PartitionRemoveResponse>, Status> {
        let authorization = authorization(&request);
        self.removes
            .lock()
            .unwrap()
            .push((request.into_inner(), authorization));
        self.answer()
    }
}

async fn serve(status: Option<Code>) -> (RepositoryRegistry, Arc<Directory>) {
    let directory = Arc::new(Directory {
        status,
        ..Directory::default()
    });
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let service = directory.clone();
    lore_base::lore_spawn!(async move {
        tonic::transport::Server::builder()
            .add_service(UserServiceServer::from_arc(service))
            .serve_with_incoming(TcpListenerStream::new(listener))
            .await
    });
    let registry = RepositoryRegistry::select(Some(AUTH_URL), Some(&format!("http://{address}")));
    (registry, directory)
}

/// The directory is sent the repository id bytes and name, under the
/// caller's token, and nothing reaches the `auth_url`, which does not resolve.
#[tokio::test]
async fn user_service_registers_and_unregisters_the_partition() {
    let (registry, directory) = serve(None).await;
    let bearer = Some("Bearer caller".to_string());

    registry
        .add(bearer.clone(), repository(), "org/repo")
        .await
        .expect("the directory adds the partition");
    registry
        .remove(bearer.clone(), repository())
        .await
        .expect("the directory removes the partition");

    let expected_partition: bytes::Bytes = repository().into();
    let adds = directory.adds.lock().unwrap();
    let [(add, add_authorization)] = adds.as_slice() else {
        panic!("one PartitionAdd, got {}", adds.len());
    };
    assert_eq!(add.partition, expected_partition);
    assert_eq!(add.name, "org/repo");
    assert_eq!(add_authorization, &bearer);
    let removes = directory.removes.lock().unwrap();
    let [(remove, remove_authorization)] = removes.as_slice() else {
        panic!("one PartitionRemove, got {}", removes.len());
    };
    assert_eq!(remove.partition, expected_partition);
    assert_eq!(remove_authorization, &bearer);
}

#[tokio::test]
async fn user_service_already_exists_is_a_name_conflict() {
    let (registry, _directory) = serve(Some(Code::AlreadyExists)).await;
    let err = registry
        .add(None, repository(), "org/repo")
        .await
        .expect_err("the partition is listed under another name");
    assert_eq!(err.code(), Code::AlreadyExists);
}

#[tokio::test]
async fn user_service_refusals_map_as_the_rebac_ones() {
    for (answer, add_expected, remove_expected) in [
        (
            Code::PermissionDenied,
            Code::PermissionDenied,
            Code::PermissionDenied,
        ),
        (
            Code::Unauthenticated,
            Code::Unauthenticated,
            Code::Unauthenticated,
        ),
        (Code::NotFound, Code::FailedPrecondition, Code::Internal),
    ] {
        let (registry, _directory) = serve(Some(answer)).await;
        let add = registry.add(None, repository(), "org/repo").await;
        assert_eq!(
            add.unwrap_err().code(),
            add_expected,
            "add answered {answer:?}"
        );
        let remove = registry.remove(None, repository()).await;
        assert_eq!(
            remove.unwrap_err().code(),
            remove_expected,
            "remove answered {answer:?}"
        );
    }
}

mod repository_create_auth_resource_tests {
    use lore_proto::rebac::CreateResourceResponse;
    use lore_proto::rebac::DeleteResourceRequest;
    use lore_proto::rebac::DeleteResourceResponse;
    use lore_server::authnz::rebac::RebacApiResult;

    use super::*;

    mockall::mock! {

        pub MockRebacApiClient {}

        #[async_trait::async_trait]
        impl RebacApiClient for MockRebacApiClient {
            async fn create_resource(
                &mut self,
                request: Request<CreateResourceRequest>,
            ) -> RebacApiResult<CreateResourceResponse>;

            async fn delete_resource(
                &mut self,
                request: Request<DeleteResourceRequest>,
            ) -> RebacApiResult<DeleteResourceResponse>;
        }
    }

    #[tokio::test]
    async fn permission_denied_propagated_to_client() {
        let repo_name = "2fc8bf934117e250152eba9a1fc78e71";
        let repository: RepositoryId = Context::from_str(repo_name)
            .expect("Failed to create repository")
            .into();

        let mut client = MockMockRebacApiClient::new();
        client
            .expect_create_resource()
            .return_once(|_| Err(Status::permission_denied("")));

        let error = repository_create_auth_resource(Box::new(client), None, repository, repo_name)
            .await
            .expect_err("Should have errored");
        assert_eq!(error.code(), Code::PermissionDenied);
        assert_eq!(
            error.message(),
            "Failed to create repository, permission denied"
        );
    }

    #[tokio::test]
    async fn missing_auth_dependencies_returns_failed_precondition() {
        let repo_name = "2fc8bf934117e250152eba9a1fc78e71";
        let repository: RepositoryId = Context::from_str(repo_name)
            .expect("Failed to create repository")
            .into();

        let mut client = MockMockRebacApiClient::new();
        client
            .expect_create_resource()
            .return_once(|_| Err(Status::not_found("")));

        let error = repository_create_auth_resource(Box::new(client), None, repository, repo_name)
            .await
            .expect_err("Should have errored");
        assert_eq!(error.code(), Code::FailedPrecondition);
        assert_eq!(error.message(), "A required Auth entity was not found");
    }

    #[tokio::test]
    async fn invalid_repository_name_returns_invalid_argument() {
        let repo_name = "2fc8bf934117e250152eba9a1fc78e71";
        let repository: RepositoryId = Context::from_str(repo_name)
            .expect("Failed to create repository")
            .into();

        let mut client = MockMockRebacApiClient::new();
        client.expect_create_resource().return_once(|_| {
            Err(Status::invalid_argument(
                "Missing resource context in resourceName",
            ))
        });

        let error = repository_create_auth_resource(Box::new(client), None, repository, repo_name)
            .await
            .expect_err("Should have errored");
        assert_eq!(error.code(), Code::InvalidArgument);
        assert_eq!(
            error.message(),
            "Invalid repository name - missing Organization context"
        );
    }

    #[tokio::test]
    async fn already_exists_treated_as_success() {
        let repo_name = "2fc8bf934117e250152eba9a1fc78e71";
        let repository: RepositoryId = Context::from_str(repo_name)
            .expect("Failed to create repository")
            .into();

        let mut client = MockMockRebacApiClient::new();
        client
            .expect_create_resource()
            .return_once(|_| Err(Status::already_exists("")));

        repository_create_auth_resource(Box::new(client), None, repository, repo_name)
            .await
            .expect("AlreadyExists should be treated as success");
    }

    // the default case for errors that aren't specially handled
    #[tokio::test]
    async fn other_errors_return_internal_error() {
        let repo_name = "2fc8bf934117e250152eba9a1fc78e71";
        let repository: RepositoryId = Context::from_str(repo_name)
            .expect("Failed to create repository")
            .into();

        let mut client = MockMockRebacApiClient::new();
        client
            .expect_create_resource()
            .return_once(|_| Err(Status::invalid_argument("You used my api wrong!")));

        let error = repository_create_auth_resource(Box::new(client), None, repository, repo_name)
            .await
            .expect_err("Should have errored");
        assert_eq!(error.code(), Code::Internal);
        assert!(
            error
                .message()
                .contains("Failed to call auth create_resource"),
        );
        assert!(error.message().contains("You used my api wrong!"),);
    }
}
