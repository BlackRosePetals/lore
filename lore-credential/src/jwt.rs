// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT
use jsonwebtoken::TokenData;
use lore_base::lore_debug;
use lore_error_set::prelude::*;
use serde::Deserialize;
use serde_with::OneOrMany;
use serde_with::formats::PreferMany;
use serde_with::serde_as;

use crate::token_store::IdentityToken;

#[derive(Debug, Default, Clone)]
pub struct UserInfo {
    pub id: String,
    pub name: String,
    pub token: String,
    pub preferred_username: String,
    pub is_service_account: bool,
    pub expires: u64,
}

// An AuthN or an AuthZ token
#[serde_as]
#[derive(Debug, Deserialize, Clone)]
pub struct JWTUserInfo {
    #[serde(rename = "iss")]
    pub issuer: String,
    #[serde(rename = "sub")]
    pub user_id: String,
    pub name: Option<String>,
    pub preferred_username: Option<String>,
    pub is_service_account: Option<bool>,
    #[serde(rename = "exp")]
    pub expires: u64,
    #[serde_as(as = "OneOrMany<_, PreferMany>")]
    #[serde(rename = "aud")]
    pub audience: Vec<String>,
    #[serde_as(as = "Option<OneOrMany<_, PreferMany>>")]
    #[serde(default)]
    pub root_domains: Option<Vec<String>>,
    #[serde_as(as = "Option<OneOrMany<_, PreferMany>>")]
    #[serde(default, rename = "https://lore.org/claims/root_domains")]
    pub root_domains_namespaced: Option<Vec<String>>,
}

impl JWTUserInfo {
    /// The root domains this token may be sent to, read from claims the issuer signed.
    ///
    /// The `root_domains` claim, or its namespaced alias, wins. Otherwise `aud` entries with a
    /// usable shape contribute: an `https` URI its host, a domain-shaped entry a suffix. Names
    /// are fixed so an environment response cannot point the client at a different claim.
    pub fn acceptable_root_domains(&self) -> Vec<String> {
        let from_claim: Vec<String> = [&self.root_domains, &self.root_domains_namespaced]
            .into_iter()
            .flatten()
            .flatten()
            .filter(|domain| !domain.is_empty())
            .cloned()
            .collect();
        if !from_claim.is_empty() {
            return from_claim;
        }

        self.audience
            .iter()
            .filter_map(|aud| domain_from_audience(aud))
            .collect()
    }
}

fn domain_from_audience(aud: &str) -> Option<String> {
    if aud.contains("://") {
        let url = url::Url::parse(aud).ok()?;
        if url.scheme() != "https" {
            return None;
        }
        return url.host_str().map(str::to_string);
    }

    let apex = aud.strip_prefix('.').unwrap_or(aud);
    let domain_shaped = apex.contains('.')
        && apex.split('.').all(|label| {
            !label.is_empty()
                && label
                    .chars()
                    .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_')
        });
    domain_shaped.then(|| aud.to_string())
}

pub async fn user_info<P>(
    auth_url: &str,
    identity: &str,
    token_filter: P,
    identity_token: &str,
    access_token: &str,
) -> Option<UserInfo>
where
    P: FnMut(&&IdentityToken) -> bool,
{
    lore_debug!("Get user {identity} info from {auth_url}");

    let Ok(token) = crate::token_store::load_user_token(
        auth_url,
        identity,
        token_filter,
        identity_token,
        access_token,
    )
    .await
    else {
        return None;
    };

    user_info_from_token(token)
}

pub fn identity_from_token(token: &str) -> String {
    user_info_from_token(token.to_string())
        .map(|info| info.id)
        .unwrap_or_default()
}

pub fn insecure_decode_token(
    token: &str,
) -> Result<TokenData<JWTUserInfo>, jsonwebtoken::errors::Error> {
    let header = jsonwebtoken::decode_header(token)?;
    let key = jsonwebtoken::DecodingKey::from_secret(&[]);
    let mut validation = jsonwebtoken::Validation::new(header.alg);
    validation.insecure_disable_signature_validation();
    validation.validate_aud = false;
    validation.validate_exp = false;
    validation.validate_nbf = false;
    jsonwebtoken::decode::<JWTUserInfo>(token, &key, &validation)
}

pub fn user_info_from_token(token: String) -> Option<UserInfo> {
    let Ok(token_data) = insecure_decode_token(&token) else {
        return None;
    };
    Some(UserInfo {
        id: token_data.claims.user_id.clone(),
        name: token_data.claims.name.clone().unwrap_or_default(),
        token,
        preferred_username: token_data.claims.preferred_username.unwrap_or_default(),
        is_service_account: token_data.claims.is_service_account.unwrap_or_default(),
        // JWT has number of seconds since UNIX epoch in UTC - we want milliseconds like
        // all other timestamps in Lore, also in UTC
        expires: token_data.claims.expires * 1000,
    })
}

#[error_set]
pub enum JwtUsageError {}

pub fn domain_in_root_domains(domain: &str, root_domains: &[String]) -> bool {
    root_domains.iter().any(|acceptable_root| {
        // Require a label boundary, not a raw suffix, so `epicgames.net`
        // rejects a look-alike such as `evilepicgames.net`. A leading `.`
        // is optional and does not change the match.
        let apex = acceptable_root.strip_prefix('.').unwrap_or(acceptable_root);
        domain == apex || domain.ends_with(&format!(".{apex}"))
    })
}

pub fn verify_jwt_usage_for_remote(
    jwt: &JWTUserInfo,
    remote_domain: &str,
) -> Result<(), JwtUsageError> {
    let root_domains = jwt.acceptable_root_domains();
    if root_domains.is_empty() {
        return Err(JwtUsageError::internal(
            "JWT names no domain it may be sent to: have the issuer add a 'root_domains' claim, \
             or set the token audience to the deployment's https URL or domain",
        ));
    }
    if domain_in_root_domains(remote_domain, &root_domains) {
        return Ok(());
    }

    lore_debug!(
        "JWT acceptable domains '{root_domains:?}' does not contain '{remote_domain}' - forbidding JWT leak"
    );
    Err(JwtUsageError::internal(format!(
        "JWT does not specify remote domain '{remote_domain}'"
    )))
}
