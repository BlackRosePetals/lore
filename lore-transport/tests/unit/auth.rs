// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT
use std::sync::Arc;

mod exchange;
mod mode;
mod oidc;
mod select;
mod token_only;
mod ucs_auth;

use lore_transport::auth::*;

/// A scheme with no user service still resolves users: the
/// bearer from their token, everyone else as their ID.
#[tokio::test]
async fn an_unregistered_scheme_gets_the_token_only_service() {
    /// `{"iss":"lore","sub":"alice","name":"Alice","exp":2000000000,"aud":["example.com"]}`
    const ALICE_TOKEN: &str = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJsb3JlIiwic3ViIjoiYWxpY2UiLCJuYW1lIjoiQWxpY2UiLCJleHAiOjIwMDAwMDAwMDAsImF1ZCI6WyJleGFtcGxlLmNvbSJdfQ.signature";
    const AUTH_URL: &str = "no-service-here://auth.test.invalid";

    let users = user_service::find(AUTH_URL)
        .get_user_info(
            AUTH_URL,
            ALICE_TOKEN,
            lore_base::types::RepositoryId::default(),
            &["alice".to_string(), "bob".to_string()],
            "corr",
        )
        .await
        .expect("the default service never fails");
    let names: Vec<&str> = users.iter().map(|u| u.user_name.as_str()).collect();
    assert_eq!(names, ["Alice", "bob"]);
}

/// The builtin schemes resolve through the auth service, exactly as
/// before the service was split from authentication.
#[test]
fn builtin_schemes_are_served_by_the_auth_service() {
    for scheme in lore_transport::auth::ucs_auth::SCHEMES {
        let auth_url = format!("{scheme}://auth.example.com");
        let service = user_service::find(&auth_url);
        let auth = authentication::find(&auth_url).expect("a builtin scheme");
        assert!(
            std::ptr::addr_eq(Arc::as_ptr(&service), Arc::as_ptr(&auth)),
            "scheme {scheme} should resolve users through the same auth service instance"
        );
    }
}
