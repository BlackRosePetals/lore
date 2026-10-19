// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT
use lore_transport::auth::user_directory::UserDirectory;
use lore_transport::types::Endpoint;
use lore_transport::types::EnvironmentConfig;

const AUTH_URL: &str = "ucs-auth://auth.example.com";
const USER_URL: &str = "ucs-auth://directory.example.com";

fn environment(auth_url: Option<&str>, user_url: Option<&str>) -> EnvironmentConfig {
    EnvironmentConfig {
        endpoint: Some(Endpoint {
            auth_url: auth_url.map(Into::into),
            user_url: user_url.map(Into::into),
            ..Default::default()
        }),
        ..Default::default()
    }
}

#[test]
fn an_advertised_user_url_is_the_directory() {
    assert_eq!(
        UserDirectory::of(&environment(Some(AUTH_URL), Some(USER_URL))),
        UserDirectory::UserService(USER_URL.into())
    );
    assert_eq!(
        UserDirectory::of(&environment(None, Some(USER_URL))),
        UserDirectory::UserService(USER_URL.into())
    );
}

#[test]
fn without_a_user_url_the_auth_service_is_the_directory() {
    for user_url in [None, Some("")] {
        assert_eq!(
            UserDirectory::of(&environment(Some(AUTH_URL), user_url)),
            UserDirectory::UrcAuth(AUTH_URL.into())
        );
    }
}

#[test]
fn without_either_url_the_token_names_the_user() {
    for environment in [
        EnvironmentConfig::default(),
        environment(Some(""), Some("")),
    ] {
        let directory = UserDirectory::of(&environment);
        assert_eq!(directory, UserDirectory::TokenOnly);
        assert_eq!(directory.url(), "");
    }
}
