// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT
use base64::prelude::BASE64_STANDARD;
use base64::prelude::Engine as _;
use lore_credential::IdentityToken;
use lore_credential::token_store::RemoteIdentity;
use lore_credential::token_store::TokenMap;
use lore_credential::token_store::load_user_token;
use lore_credential::token_store::load_user_token_from_store;
use lore_credential::token_store::remove_user_token_if;
use lore_credential::token_store::store_user_token;
use lore_credential::token_store::test_util::encryption_key_from_stored;
use lore_credential::token_store::test_util::generate_encryption_key;
use lore_credential::token_store::test_util::is_entry_for_auth_url;
use lore_credential::token_store::test_util::open_token;
use lore_credential::token_store::test_util::seal_token;
use lore_credential::token_store::test_util::single_nonce_sequence;
use lore_credential::token_store::test_util::split_remote_resource;
use lore_credential::token_store::test_util::store_file;
use lore_credential::token_store::tokens_only_for_recipient_domain;
use lore_credential::token_store::vulnerable_all_tokens;
use ring::aead::AES_256_GCM;
use ring::aead::NONCE_LEN;
use ring::aead::NonceSequence;

#[test]
fn refresh_token_serde_default_none() {
    // Old store format without refresh_token field
    let toml_str = r#"
user_id = "user-1"
token = "encrypted-token"
acceptable_root_domains = ["example.com"]
"#;
    let token: IdentityToken = toml::from_str(toml_str).unwrap();
    assert!(token.refresh_token().is_none());
    assert_eq!(token.user_id(), "user-1");
    assert_eq!(token.token(), "encrypted-token");
}

#[test]
fn refresh_token_serde_roundtrip() {
    let token = IdentityToken::new(
        "user-1",
        "encrypted-auth",
        vec!["example.com".into()],
        Some("encrypted-refresh".into()),
    );
    let serialized = toml::to_string_pretty(&token).unwrap();
    let deserialized: IdentityToken = toml::from_str(&serialized).unwrap();
    assert_eq!(deserialized.refresh_token(), Some("encrypted-refresh"));
    assert_eq!(deserialized.user_id(), "user-1");
}

#[test]
fn identity_token_without_refresh_backward_compat() {
    // Simulates an old store file structure
    let toml_str = r#"
[[remotes]]
remote = "https://auth.example.com"

[[remotes.token]]
user_id = "alice"
token = "tok-a"
acceptable_root_domains = ["example.com"]

[[remotes.token]]
user_id = "bob"
token = "tok-b"
"#;
    let map: TokenMap = toml::from_str(toml_str).unwrap();
    assert_eq!(map.remotes().len(), 1);
    assert_eq!(map.remotes()[0].tokens().len(), 2);
    assert!(map.remotes()[0].tokens()[0].refresh_token().is_none());
    assert!(map.remotes()[0].tokens()[1].refresh_token().is_none());
}

#[test]
fn token_map_with_refresh_token_roundtrip() {
    let map = TokenMap::new(vec![RemoteIdentity::new(
        "https://auth.example.com",
        vec![IdentityToken::new(
            "alice",
            "auth-tok",
            vec!["example.com".into()],
            Some("refresh-tok".into()),
        )],
    )]);
    let serialized = toml::to_string_pretty(&map).unwrap();
    let deserialized: TokenMap = toml::from_str(&serialized).unwrap();
    assert_eq!(
        deserialized.remotes()[0].tokens()[0].refresh_token(),
        Some("refresh-tok")
    );
}

#[test]
fn encryption_key_from_stored_accepts_a_bare_key() {
    let key = generate_encryption_key().unwrap();
    assert_eq!(key.len(), AES_256_GCM.key_len());
    assert_eq!(encryption_key_from_stored(&key).as_ref(), Some(&key));
}

#[test]
fn encryption_key_from_stored_rejects_malformed() {
    assert!(encryption_key_from_stored(&[]).is_none());
    assert!(encryption_key_from_stored(&[0u8; 16]).is_none());
    // The layout earlier versions wrote: a nonce counter ahead of the key.
    assert!(encryption_key_from_stored(&[0u8; 4 + 32]).is_none());
}

#[test]
fn seal_and_open_token_round_trip() {
    let key = generate_encryption_key().unwrap();
    let sealed = seal_token(&key, "a-user-token").unwrap();
    assert_eq!(open_token(&key, &sealed).unwrap(), "a-user-token");
}

#[test]
fn seal_token_prefixes_the_nonce() {
    let key = generate_encryption_key().unwrap();
    let blob = BASE64_STANDARD
        .decode(seal_token(&key, "a-user-token").unwrap())
        .unwrap();
    assert_eq!(blob.len(), NONCE_LEN + "a-user-token".len() + 16);
}

#[test]
fn seal_token_draws_a_fresh_nonce_each_time() {
    let key = generate_encryption_key().unwrap();
    let first = seal_token(&key, "a-user-token").unwrap();
    let second = seal_token(&key, "a-user-token").unwrap();
    assert_ne!(first, second);
}

#[test]
fn open_token_rejects_short_blobs() {
    let key = generate_encryption_key().unwrap();
    let short = BASE64_STANDARD.encode([0u8; NONCE_LEN - 1]);
    assert!(open_token(&key, &short).is_err());
}

#[test]
fn open_token_rejects_a_foreign_key() {
    let sealed = seal_token(&generate_encryption_key().unwrap(), "a-user-token").unwrap();
    assert!(open_token(&generate_encryption_key().unwrap(), &sealed).is_err());
}

#[test]
fn single_nonce_sequence_refuses_second_advance() {
    let mut sequence = single_nonce_sequence([0u8; NONCE_LEN]);
    assert!(sequence.advance().is_ok());
    assert!(sequence.advance().is_err());
}

#[test]
fn split_remote_resource_new_format() {
    let (auth, resource) =
        split_remote_resource("https://auth.example.com/00112233445566778899aabbccddeeff");
    assert_eq!(auth, "https://auth.example.com");
    assert_eq!(resource, "00112233445566778899aabbccddeeff");
}

#[test]
fn split_remote_resource_legacy_format() {
    let (auth, resource) =
        split_remote_resource("https://auth.example.com/urc-00112233445566778899aabbccddeeff");
    assert_eq!(auth, "https://auth.example.com");
    assert_eq!(resource, "urc-00112233445566778899aabbccddeeff");
}

#[test]
fn split_remote_resource_no_resource() {
    let (auth, resource) = split_remote_resource("https://auth.example.com");
    assert_eq!(auth, "https://auth.example.com");
    assert!(resource.is_empty());
}

#[test]
fn split_remote_resource_scheme_with_hostname() {
    let (auth, resource) =
        split_remote_resource("ucs-auth://auth.example.com/aabbccdd00112233aabbccdd00112233");
    assert_eq!(auth, "ucs-auth://auth.example.com");
    assert_eq!(resource, "aabbccdd00112233aabbccdd00112233");
}

#[test]
fn is_entry_for_auth_url_base() {
    assert!(is_entry_for_auth_url(
        "https://auth.example.com",
        "https://auth.example.com"
    ));
}

#[test]
fn is_entry_for_auth_url_new_format() {
    assert!(is_entry_for_auth_url(
        "https://auth.example.com/00112233445566778899aabbccddeeff",
        "https://auth.example.com"
    ));
}

#[test]
fn is_entry_for_auth_url_legacy_format() {
    assert!(is_entry_for_auth_url(
        "https://auth.example.com/urc-00112233445566778899aabbccddeeff",
        "https://auth.example.com"
    ));
}

#[test]
fn is_entry_for_auth_url_different_host() {
    assert!(!is_entry_for_auth_url(
        "https://other.example.com/00112233445566778899aabbccddeeff",
        "https://auth.example.com"
    ));
}

#[test]
fn is_entry_for_auth_url_non_hex_suffix() {
    assert!(!is_entry_for_auth_url(
        "https://auth.example.com/not-a-resource",
        "https://auth.example.com"
    ));
}

/// A supplied token is passed through untouched, so it need not be a JWT.
const SUPPLIED_TOKEN: &str = "supplied-authentication-token";

#[tokio::test]
async fn supplied_identity_token_is_used_without_the_store() {
    // No auth endpoint and no store entry: the supplied token is returned on
    // its own, where a store read would report TokenNotFound.
    let token = load_user_token(
        "",
        "alice",
        tokens_only_for_recipient_domain("nowhere.example".to_string()),
        SUPPLIED_TOKEN,
        "",
    )
    .await
    .expect("the supplied token is used as given");
    assert_eq!(token, SUPPLIED_TOKEN);

    // An access token alongside it changes nothing: the identity token is
    // still the authentication token to use.
    let token = load_user_token(
        "",
        "alice",
        tokens_only_for_recipient_domain("nowhere.example".to_string()),
        SUPPLIED_TOKEN,
        "supplied-access-token",
    )
    .await
    .expect("the supplied token is used as given");
    assert_eq!(token, SUPPLIED_TOKEN);
}

#[tokio::test]
async fn no_supplied_token_reads_the_store() {
    // Nothing supplied, so this is a plain store read, which has no entry
    // for an empty endpoint.
    let result = load_user_token(
        "",
        "alice",
        tokens_only_for_recipient_domain("nowhere.example".to_string()),
        "",
        "",
    )
    .await;
    assert!(result.is_err());
}

/// A token removed only where it is a given one is compared with the store as its file holds it,
/// so a token another process stored in its place after this process cached the store is left
/// alone, and the given one is removed. The test stands in for the other process by writing the
/// store's file back as it was before this process stored the given token.
#[tokio::test]
async fn a_conditional_removal_leaves_a_token_another_process_stored() {
    const ENDPOINT: &str = "https://auth.conditional-removal.test.invalid/repository";
    let file = store_file().expect("the token store's file");
    store_user_token(ENDPOINT, "alice", "newer", Vec::new())
        .await
        .expect("store the newer token");
    let newer = std::fs::read(&file).expect("read the store holding the newer token");
    store_user_token(ENDPOINT, "alice", "refused", Vec::new())
        .await
        .expect("store the refused token");
    std::fs::write(&file, newer).expect("store the newer token from another process");

    remove_user_token_if(ENDPOINT, "alice", "refused")
        .await
        .expect("remove the refused token");
    let kept = load_user_token_from_store(ENDPOINT, "alice", vulnerable_all_tokens()).await;
    assert_eq!(kept.ok().as_deref(), Some("newer"));

    remove_user_token_if(ENDPOINT, "alice", "newer")
        .await
        .expect("remove the newer token");
    let removed = load_user_token_from_store(ENDPOINT, "alice", vulnerable_all_tokens()).await;
    assert!(removed.is_err(), "the given token was left in the store");
}
