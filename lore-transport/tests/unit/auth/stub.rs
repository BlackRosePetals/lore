// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT
//! An `Authentication` that serves repository exchanges from a closure, so a test drives the
//! authorization exchange without a network, and tokens for it to trade.

use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;

use lore_base::types::RepositoryId;
use lore_transport::error::ProtocolError;
use lore_transport::traits::Authentication;
use lore_transport::types::AuthSession;
use lore_transport::types::AuthSessionPoll;
use lore_transport::types::AuthenticationToken;
use lore_transport::types::AuthorizationToken;

/// `sub` alice, `aud` example.com, expiring in 2033.
pub(crate) const AUTHN_ONE: &str = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJsb3JlIiwic3ViIjoiYWxpY2UiLCJleHAiOjIwMDAwMDAwMDAsImF1ZCI6WyJleGFtcGxlLmNvbSJdLCJuYW1lIjoiQWxpY2UifQ.signature";
/// The same user, a different credential -- note the `scope` claim.
pub(crate) const AUTHN_TWO: &str = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJsb3JlIiwic3ViIjoiYWxpY2UiLCJleHAiOjIwMDAwMDAwMDAsImF1ZCI6WyJleGFtcGxlLmNvbSJdLCJuYW1lIjoiQWxpY2UiLCJzY29wZSI6InJlYWQifQ.signature";
/// Authorizations for alice on example.com, expiring in 2033, told apart by an `authz` claim.
pub(crate) const AUTHZ_ONE: &str = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJsb3JlIiwic3ViIjoiYWxpY2UiLCJleHAiOjIwMDAwMDAwMDAsImF1ZCI6WyJleGFtcGxlLmNvbSJdLCJuYW1lIjoiQWxpY2UiLCJhdXRoeiI6Im9uZSJ9.signature";
pub(crate) const AUTHZ_TWO: &str = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJsb3JlIiwic3ViIjoiYWxpY2UiLCJleHAiOjIwMDAwMDAwMDAsImF1ZCI6WyJleGFtcGxlLmNvbSJdLCJuYW1lIjoiQWxpY2UiLCJhdXRoeiI6InR3byJ9.signature";

/// Names the authorization for the authentication token traded in and the number of exchanges
/// served before it.
type Authorize = Box<dyn Fn(&str, usize) -> &'static str + Send + Sync>;

/// Hands out the authorization `authorize` names, and counts the exchanges.
pub(crate) struct StubAuthentication {
    exchanges: AtomicUsize,
    authorize: Authorize,
}

impl StubAuthentication {
    pub(crate) fn new(
        authorize: impl Fn(&str, usize) -> &'static str + Send + Sync + 'static,
    ) -> Arc<Self> {
        Arc::new(Self {
            exchanges: AtomicUsize::new(0),
            authorize: Box::new(authorize),
        })
    }

    /// Repository exchanges served so far.
    pub(crate) fn exchanges(&self) -> usize {
        self.exchanges.load(Ordering::Acquire)
    }
}

#[async_trait::async_trait]
impl Authentication for StubAuthentication {
    async fn exchange_for_repository(
        &self,
        _auth_url: &str,
        authn_token: &str,
        _repository: RepositoryId,
        _correlation_id: &str,
    ) -> Result<AuthorizationToken, ProtocolError> {
        let served = self.exchanges.fetch_add(1, Ordering::AcqRel);
        Ok(AuthorizationToken {
            token: (self.authorize)(authn_token, served).to_string(),
            expires_ms: 0,
            acceptable_root_domains: vec!["example.com".to_string()],
        })
    }

    async fn start_auth_session(
        &self,
        _auth_url: &str,
        _client_state: &str,
        _correlation_id: &str,
    ) -> Result<AuthSession, ProtocolError> {
        Err(ProtocolError::internal(
            "the stub only serves authorization exchanges",
        ))
    }

    async fn poll_auth_session(
        &self,
        _auth_url: &str,
        _client_state: &str,
        _session_code: &str,
        _correlation_id: &str,
    ) -> Result<AuthSessionPoll, ProtocolError> {
        Err(ProtocolError::internal(
            "the stub only serves authorization exchanges",
        ))
    }

    async fn exchange_external_token(
        &self,
        _auth_url: &str,
        _token: &str,
        _token_type: &str,
        _correlation_id: &str,
    ) -> Result<AuthenticationToken, ProtocolError> {
        Err(ProtocolError::internal(
            "the stub only serves authorization exchanges",
        ))
    }

    async fn refresh_authentication(
        &self,
        _auth_url: &str,
        _refresh_token: &str,
        _correlation_id: &str,
    ) -> Result<AuthenticationToken, ProtocolError> {
        Err(ProtocolError::internal(
            "the stub only serves authorization exchanges",
        ))
    }

    async fn exchange_for_custom_resource(
        &self,
        _auth_url: &str,
        _authn_token: &str,
        _resource_id: &str,
        _correlation_id: &str,
    ) -> Result<AuthorizationToken, ProtocolError> {
        Err(ProtocolError::internal(
            "the stub only serves repository exchanges",
        ))
    }
}
