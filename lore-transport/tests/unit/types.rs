// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT
use lore_transport::types::*;

const FALLBACK: &str = "grpc://fallback.example:1234";

fn env_with(endpoint: Endpoint) -> EnvironmentConfig {
    EnvironmentConfig {
        endpoint: Some(endpoint),
        config: None,
        oidc: None,
    }
}

#[test]
fn service_url_returns_override_when_set() {
    let env = env_with(Endpoint {
        storage_url: Some("quic://storage.example:7000".into()),
        ..Default::default()
    });
    assert_eq!(env.storage_url(FALLBACK), "quic://storage.example:7000");
}

#[test]
fn service_url_falls_back_when_field_is_none() {
    let env = env_with(Endpoint::default());
    assert_eq!(env.storage_url(FALLBACK), FALLBACK);
    assert_eq!(env.revision_url(FALLBACK), FALLBACK);
    assert_eq!(env.lock_url(FALLBACK), FALLBACK);
    assert_eq!(env.repository_url(FALLBACK), FALLBACK);
    assert_eq!(env.notification_url(FALLBACK), FALLBACK);
}

#[test]
fn service_url_falls_back_when_field_is_empty_string() {
    // An empty Option<String> from proto decoding must behave identically
    // to None — the field is "unset."
    let env = env_with(Endpoint {
        storage_url: Some(String::new()),
        revision_url: Some(String::new()),
        ..Default::default()
    });
    assert_eq!(env.storage_url(FALLBACK), FALLBACK);
    assert_eq!(env.revision_url(FALLBACK), FALLBACK);
}

#[test]
fn service_url_falls_back_when_endpoint_section_missing() {
    let env = EnvironmentConfig {
        endpoint: None,
        config: None,
        oidc: None,
    };
    assert_eq!(env.storage_url(FALLBACK), FALLBACK);
    assert_eq!(env.repository_url(FALLBACK), FALLBACK);
}

#[test]
fn per_service_overrides_are_independent() {
    // Only some services have overrides; the others must fall back.
    let env = env_with(Endpoint {
        storage_url: Some("quic://storage.example:7000".into()),
        lock_url: Some("grpc://lock.example:8000".into()),
        ..Default::default()
    });
    assert_eq!(env.storage_url(FALLBACK), "quic://storage.example:7000");
    assert_eq!(env.lock_url(FALLBACK), "grpc://lock.example:8000");
    assert_eq!(env.revision_url(FALLBACK), FALLBACK);
    assert_eq!(env.repository_url(FALLBACK), FALLBACK);
    assert_eq!(env.notification_url(FALLBACK), FALLBACK);
}
