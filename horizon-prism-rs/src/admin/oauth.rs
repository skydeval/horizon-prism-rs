//! Operator sign-in with ATProto OAuth (admin-ui design §5.2, §5.3;
//! membership design §8): a public client, scope `atproto`, PAR + PKCE +
//! DPoP, the client metadata served at `/admin/oauth/client-metadata.json`.
//!
//! Every OAuth fetch goes through [`NetFetch`], a `FetchHandler` over a
//! `NetClient` of `Intent::Public`, so the network guard, the IP-literal
//! checks and the body cap apply (§5.10). The authorization server is
//! found from the operator DID's own `#atproto_pds`, so the token is bound
//! to the server authoritative for that DID. Prism needs only the
//! identity: the tokens are revoked (best effort) and dropped.

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use proto_blue::common::fetch::{FetchError, FetchHandler, HttpHeaders, HttpMethod, HttpRequest, HttpResponse};
use proto_blue::oauth::{AuthState, OAuthClient, OAuthClientMetadata};
use reqwest::Method;
use reqwest::header::{HeaderName, HeaderValue};
use serde_json::Value;

use crate::net::{BodyCap, NetClient, NetError, RequestOptions};

/// Body cap for every OAuth response.
pub const BODY_CAP: BodyCap = BodyCap(256 * 1024);
/// Total time for one OAuth call.
pub const TIMEOUT: Duration = Duration::from_secs(10);
/// The path of the client metadata document, which is also the client id.
pub const CLIENT_METADATA_PATH: &str = "/admin/oauth/client-metadata.json";
/// The callback the authorization server returns to.
pub const CALLBACK_PATH: &str = "/admin/oauth/callback";

/// A `FetchHandler` over the guarded client.
pub struct NetFetch(pub NetClient);

#[async_trait]
impl FetchHandler for NetFetch {
    async fn fetch(&self, req: HttpRequest) -> Result<HttpResponse, FetchError> {
        let method = match req.method {
            HttpMethod::Get => Method::GET,
            HttpMethod::Post => Method::POST,
            other => return Err(FetchError::Other(format!("{} is not used by sign-in", other.as_str()))),
        };
        let mut options = RequestOptions::new(BODY_CAP).timeout(TIMEOUT);
        for (name, value) in &req.headers {
            let name = HeaderName::from_bytes(name.as_bytes()).map_err(|e| FetchError::Other(e.to_string()))?;
            let value = HeaderValue::from_str(value).map_err(|e| FetchError::Other(e.to_string()))?;
            options = options.header(name, value);
        }
        let (response, headers) = self.0.request_raw(method, &req.url, req.body, &options).await.map_err(|e| match e {
            NetError::Timeout => FetchError::Timeout,
            NetError::InvalidUrl { .. } => FetchError::InvalidUrl(e.to_string()),
            e => FetchError::Network(e.to_string()),
        })?;
        let mut out = HttpHeaders::new();
        for (name, value) in &headers {
            if let Ok(v) = value.to_str() {
                out.insert(name.as_str().to_ascii_lowercase(), v.to_string());
            }
        }
        Ok(HttpResponse {
            status: response.status.as_u16(),
            headers: out,
            body: response.body,
        })
    }
}

/// The client metadata document (membership design §8).
#[must_use]
pub fn client_metadata(public_hostname: &str) -> OAuthClientMetadata {
    OAuthClientMetadata {
        client_id: format!("https://{public_hostname}{CLIENT_METADATA_PATH}"),
        redirect_uris: vec![format!("https://{public_hostname}{CALLBACK_PATH}")],
        response_types: Some(vec!["code".to_string()]),
        grant_types: Some(vec!["authorization_code".to_string()]),
        scope: Some("atproto".to_string()),
        token_endpoint_auth_method: Some("none".to_string()),
        token_endpoint_auth_signing_alg: None,
        application_type: Some("web".to_string()),
        dpop_bound_access_tokens: Some(true),
        client_name: Some(format!("Prism admin ({public_hostname})")),
        client_uri: Some(format!("https://{public_hostname}/admin")),
        logo_uri: None,
    }
}

/// The OAuth client, fetching through `fetcher`.
#[must_use]
pub fn client(public_hostname: &str, fetcher: Arc<dyn FetchHandler>) -> OAuthClient {
    OAuthClient::with_fetch_handler(client_metadata(public_hostname), fetcher)
}

/// A started flow: where to send the browser, and what to keep.
pub struct Started {
    pub authorize_url: String,
    pub state: String,
    pub issuer: String,
    pub verifier: String,
    pub dpop_jwk: Value,
}

/// Discover the authorization server from the PDS endpoint (protected
/// resource metadata, then the server's own), and push the authorization
/// request.
///
/// # Errors
///
/// A plain-words failure.
pub async fn start(client: &OAuthClient, pds_endpoint: &str) -> Result<Started, String> {
    let resource = client
        .discover_resource(pds_endpoint)
        .await
        .map_err(|e| format!("your account's server does not offer OAuth sign-in ({e})"))?;
    let issuer = resource
        .authorization_servers
        .and_then(|s| s.into_iter().next())
        .ok_or_else(|| "your account's server names no authorization server".to_string())?;
    let metadata = client
        .discover_server(&issuer)
        .await
        .map_err(|e| format!("your account's authorization server could not be read ({e})"))?;
    let (url, auth) = client
        .authorize(&metadata)
        .await
        .map_err(|e| format!("your account's authorization server refused to start sign-in ({e})"))?;
    let state = auth.app_state.clone().ok_or_else(|| "no state was made".to_string())?;
    Ok(Started {
        authorize_url: url.to_string(),
        state,
        issuer: auth.issuer,
        verifier: auth.verifier,
        dpop_jwk: auth.dpop_key,
    })
}

/// What a stored flow carries into the callback.
pub struct Pending {
    pub state: String,
    pub did: String,
    pub issuer: String,
    pub verifier: String,
    pub dpop_jwk: Value,
    pub pds_endpoint: String,
}

/// Finish a flow: `iss` must be the stored issuer, the code is exchanged
/// at that issuer, and the token's `sub` must be the expected DID. The
/// tokens are then revoked, best effort, and dropped.
///
/// # Errors
///
/// A plain-words failure.
pub async fn finish(client: &Arc<OAuthClient>, pending: &Pending, code: &str, iss: Option<&str>) -> Result<(), String> {
    let stored = pending.issuer.trim_end_matches('/');
    if iss.map(|i| i.trim_end_matches('/')) != Some(stored) {
        return Err("the sign-in came back from a different server than it started at".to_string());
    }
    let metadata = client
        .discover_server(&pending.issuer)
        .await
        .map_err(|e| format!("your account's authorization server could not be read ({e})"))?;
    let auth = AuthState {
        issuer: pending.issuer.clone(),
        verifier: pending.verifier.clone(),
        dpop_key: pending.dpop_jwk.clone(),
        app_state: Some(pending.state.clone()),
    };
    let tokens = client
        .callback_verified(code, iss, Some(&pending.pds_endpoint), Some(&pending.did), &auth, &metadata)
        .await
        .map_err(|e| format!("your account's server did not confirm the sign-in ({e})"))?;
    // Only the identity was needed.
    let client = Arc::clone(client);
    tokio::spawn(async move {
        for token in [Some(tokens.access_token), tokens.refresh_token].into_iter().flatten() {
            if let Err(error) = client.revoke_token(&metadata, &token).await {
                tracing::debug!(event = "admin_token_revoke_failed", %error, "revoking a sign-in token failed");
            }
        }
    });
    Ok(())
}
