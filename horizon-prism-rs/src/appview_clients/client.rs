//! One outbound AppView client per routing-table network (design-v2 §8.1,
//! §8.3, §8.5).
//!
//! - Calls go through `net` (SSRF-guarded), with the §8.3 limits: connect
//!   1 s (the client's), total 3 s per call, 4 MiB body cap.
//! - `unauth` sends no `Authorization`; `service_jwt` sends a fresh
//!   AppView-signed token per call (`aud` = the network's `appview_did`,
//!   `lxm` = the method). Inbound request headers are never copied.
//! - Non-2xx answers are returned, not turned into errors: the caller
//!   classifies them (§8.3).

use std::time::Duration;

use axum::http::{HeaderName, HeaderValue};
use url::Url;

use super::jwt;
use crate::config::{ConstellationConfig, LOCAL_APPVIEW, NetworkAuth};
use crate::kryphocron_glue::ServiceKey;
use crate::net::{BodyCap, NetClient, NetError, NetResponse, RequestOptions};

/// Total time per upstream call (§8.3).
pub const CALL_TIMEOUT: Duration = Duration::from_secs(3);

/// `getPosts` parameters (≤ 25 URIs).
fn posts_params(uris: &[String]) -> Vec<(&'static str, String)> {
    uris.iter().take(25).map(|u| ("uris", u.clone())).collect()
}

/// `getPostThread` parameters.
fn thread_params(uri: &str, depth: u32, parent_height: u32) -> Vec<(&'static str, String)> {
    vec![("uri", uri.to_string()), ("depth", depth.to_string()), ("parentHeight", parent_height.to_string())]
}

fn accept_labelers(value: &HeaderValue) -> (HeaderName, HeaderValue) {
    (HeaderName::from_static(super::accept_labelers::ACCEPT_LABELERS), value.clone())
}

/// How a network is called.
#[derive(Clone)]
pub enum AppViewAuth {
    Unauth,
    ServiceJwt { aud: String, key: Box<ServiceKey> },
}

/// An outbound AppView client. Cheap to clone.
#[derive(Clone)]
pub struct AppViewClient {
    network: String,
    base: String,
    auth: AppViewAuth,
    net: NetClient,
}

impl AppViewClient {
    #[must_use]
    pub fn new(network: &str, base_url: &str, auth: AppViewAuth, net: NetClient) -> Self {
        Self {
            network: network.to_string(),
            base: base_url.trim_end_matches('/').to_string(),
            auth,
            net,
        }
    }

    #[must_use]
    pub fn network(&self) -> &str {
        &self.network
    }

    /// The base URL calls go to.
    #[must_use]
    pub fn base_url(&self) -> &str {
        &self.base
    }

    /// `GET {base}/xrpc/{method}?params`.
    ///
    /// # Errors
    ///
    /// Transport failures ([`NetError`]); statuses are in the response.
    pub async fn query(&self, method: &str, params: &[(&str, String)]) -> Result<NetResponse, NetError> {
        self.query_within(method, params, CALL_TIMEOUT).await
    }

    /// [`Self::query`] with a time limit of the caller's.
    ///
    /// # Errors
    ///
    /// Transport failures ([`NetError`]); statuses are in the response.
    pub async fn query_within(&self, method: &str, params: &[(&str, String)], timeout: Duration) -> Result<NetResponse, NetError> {
        let url = Url::parse_with_params(&format!("{}/xrpc/{method}", self.base), params).map_err(|e| NetError::InvalidUrl {
            url: self.base.clone(),
            reason: e.to_string(),
        })?;
        let mut options = RequestOptions::new(BodyCap::APPVIEW).timeout(timeout);
        if let AppViewAuth::ServiceJwt { aud, key } = &self.auth {
            let token = jwt::mint(key, aud, method, chrono::Utc::now().timestamp());
            let value = HeaderValue::from_str(&format!("Bearer {token}")).map_err(|e| NetError::Encode(e.to_string()))?;
            options = options.header(HeaderName::from_static("authorization"), value);
        }
        self.net.get(url.as_str(), &options).await
    }

    /// [`Self::query`] with extra request headers, returning the response's
    /// headers too (Refract's labeler reads through an AppView, refract
    /// design §4.5: `atproto-accept-labelers` out, `atproto-content-labelers`
    /// back).
    ///
    /// # Errors
    ///
    /// Transport failures ([`NetError`]); statuses are in the response.
    pub async fn query_with_headers(
        &self,
        method: &str,
        params: &[(&str, String)],
        headers: &[(HeaderName, HeaderValue)],
    ) -> Result<(NetResponse, axum::http::HeaderMap), NetError> {
        let url = Url::parse_with_params(&format!("{}/xrpc/{method}", self.base), params).map_err(|e| NetError::InvalidUrl {
            url: self.base.clone(),
            reason: e.to_string(),
        })?;
        let mut options = RequestOptions::new(BodyCap::APPVIEW).timeout(CALL_TIMEOUT);
        if let AppViewAuth::ServiceJwt { aud, key } = &self.auth {
            let token = jwt::mint(key, aud, method, chrono::Utc::now().timestamp());
            let value = HeaderValue::from_str(&format!("Bearer {token}")).map_err(|e| NetError::Encode(e.to_string()))?;
            options = options.header(HeaderName::from_static("authorization"), value);
        }
        for (name, value) in headers {
            options = options.header(name.clone(), value.clone());
        }
        self.net.request_raw(axum::http::Method::GET, url.as_str(), None, &options).await
    }

    /// `app.bsky.actor.getProfile`.
    ///
    /// # Errors
    ///
    /// Transport failures.
    pub async fn get_profile(&self, actor: &str) -> Result<NetResponse, NetError> {
        self.query("app.bsky.actor.getProfile", &[("actor", actor.to_string())]).await
    }

    /// `app.bsky.actor.getProfiles` (≤ 25 actors).
    ///
    /// # Errors
    ///
    /// Transport failures.
    pub async fn get_profiles(&self, actors: &[String]) -> Result<NetResponse, NetError> {
        let params: Vec<(&str, String)> = actors.iter().take(25).map(|a| ("actors", a.clone())).collect();
        self.query("app.bsky.actor.getProfiles", &params).await
    }

    /// `app.bsky.feed.getPosts` (≤ 25 URIs).
    ///
    /// # Errors
    ///
    /// Transport failures.
    pub async fn get_posts(&self, uris: &[String]) -> Result<NetResponse, NetError> {
        self.query("app.bsky.feed.getPosts", &posts_params(uris)).await
    }

    /// `app.bsky.feed.getPostThread`.
    ///
    /// # Errors
    ///
    /// Transport failures.
    pub async fn get_post_thread(&self, uri: &str, depth: u32, parent_height: u32) -> Result<NetResponse, NetError> {
        self.query("app.bsky.feed.getPostThread", &thread_params(uri, depth, parent_height)).await
    }

    /// [`Self::get_posts`] carrying `atproto-accept-labelers`, with the
    /// response's headers (threadlens-hydration design §3.6 (b)).
    ///
    /// # Errors
    ///
    /// Transport failures.
    pub async fn get_posts_labelled(&self, uris: &[String], labelers: &HeaderValue) -> Result<(NetResponse, axum::http::HeaderMap), NetError> {
        self.query_with_headers("app.bsky.feed.getPosts", &posts_params(uris), &[accept_labelers(labelers)]).await
    }

    /// [`Self::get_post_thread`] carrying `atproto-accept-labelers`, with
    /// the response's headers.
    ///
    /// # Errors
    ///
    /// Transport failures.
    pub async fn get_post_thread_labelled(
        &self,
        uri: &str,
        depth: u32,
        parent_height: u32,
        labelers: &HeaderValue,
    ) -> Result<(NetResponse, axum::http::HeaderMap), NetError> {
        self.query_with_headers("app.bsky.feed.getPostThread", &thread_params(uri, depth, parent_height), &[accept_labelers(labelers)]).await
    }

    /// `app.bsky.feed.getAuthorFeed`.
    ///
    /// # Errors
    ///
    /// Transport failures.
    pub async fn get_author_feed(&self, actor: &str, limit: u32, cursor: Option<&str>, filter: &str) -> Result<NetResponse, NetError> {
        let mut params = vec![("actor", actor.to_string()), ("limit", limit.to_string()), ("filter", filter.to_string())];
        if let Some(c) = cursor {
            params.push(("cursor", c.to_string()));
        }
        self.query("app.bsky.feed.getAuthorFeed", &params).await
    }

    /// `app.bsky.graph.getFollows`.
    ///
    /// # Errors
    ///
    /// Transport failures.
    pub async fn get_follows(&self, actor: &str, limit: u32, cursor: Option<&str>) -> Result<NetResponse, NetError> {
        let mut params = vec![("actor", actor.to_string()), ("limit", limit.to_string())];
        if let Some(c) = cursor {
            params.push(("cursor", c.to_string()));
        }
        self.query("app.bsky.graph.getFollows", &params).await
    }

    /// `app.bsky.graph.getList`.
    ///
    /// # Errors
    ///
    /// Any [`NetError`].
    pub async fn get_list(&self, list: &str, limit: u32, cursor: Option<&str>) -> Result<NetResponse, NetError> {
        let mut params = vec![("list", list.to_string()), ("limit", limit.to_string())];
        if let Some(c) = cursor {
            params.push(("cursor", c.to_string()));
        }
        self.query("app.bsky.graph.getList", &params).await
    }

    /// `app.bsky.feed.getListFeed`.
    ///
    /// # Errors
    ///
    /// Any [`NetError`].
    pub async fn get_list_feed(&self, list: &str, limit: u32, cursor: Option<&str>) -> Result<NetResponse, NetError> {
        let mut params = vec![("list", list.to_string()), ("limit", limit.to_string())];
        if let Some(c) = cursor {
            params.push(("cursor", c.to_string()));
        }
        self.query("app.bsky.feed.getListFeed", &params).await
    }

    /// `app.bsky.graph.getRelationships`.
    ///
    /// # Errors
    ///
    /// Any [`NetError`].
    pub async fn get_relationships(&self, actor: &str, others: &[String]) -> Result<NetResponse, NetError> {
        let mut params = vec![("actor", actor.to_string())];
        params.extend(others.iter().map(|o| ("others", o.clone())));
        self.query("app.bsky.graph.getRelationships", &params).await
    }

    /// [`Self::get_relationships`] with a time limit of the caller's.
    ///
    /// # Errors
    ///
    /// Any [`NetError`].
    pub async fn get_relationships_within(&self, actor: &str, others: &[String], timeout: Duration) -> Result<NetResponse, NetError> {
        let mut params = vec![("actor", actor.to_string())];
        params.extend(others.iter().map(|o| ("others", o.clone())));
        self.query_within("app.bsky.graph.getRelationships", &params, timeout).await
    }

    /// `app.bsky.graph.getStarterPack`.
    ///
    /// # Errors
    ///
    /// Any [`NetError`].
    pub async fn get_starter_pack(&self, uri: &str) -> Result<NetResponse, NetError> {
        self.query("app.bsky.graph.getStarterPack", &[("starterPack", uri.to_string())]).await
    }

    /// `app.bsky.graph.getActorStarterPacks`.
    ///
    /// # Errors
    ///
    /// Any [`NetError`].
    pub async fn get_actor_starter_packs(&self, actor: &str, limit: u32, cursor: Option<&str>) -> Result<NetResponse, NetError> {
        let mut params = vec![("actor", actor.to_string()), ("limit", limit.to_string())];
        if let Some(c) = cursor {
            params.push(("cursor", c.to_string()));
        }
        self.query("app.bsky.graph.getActorStarterPacks", &params).await
    }

    /// `app.bsky.graph.getLists`, with `purposes` as full tokens.
    ///
    /// # Errors
    ///
    /// Any [`NetError`].
    pub async fn get_lists(&self, actor: &str, limit: u32, cursor: Option<&str>, purposes: &[String]) -> Result<NetResponse, NetError> {
        let mut params = vec![("actor", actor.to_string()), ("limit", limit.to_string())];
        if let Some(c) = cursor {
            params.push(("cursor", c.to_string()));
        }
        params.extend(purposes.iter().map(|p| ("purposes", p.clone())));
        self.query("app.bsky.graph.getLists", &params).await
    }
}

/// One client per enabled network with an outbound AppView (the `community`
/// network is served locally and gets none). Config validation guarantees
/// that an enabled `service_jwt` network has an `appview_did`.
#[must_use]
pub fn from_config(cfg: &ConstellationConfig, net: &NetClient, key: &ServiceKey) -> Vec<AppViewClient> {
    cfg.networks
        .iter()
        .filter(|n| n.enabled && n.appview_url != LOCAL_APPVIEW)
        .map(|n| {
            let auth = match n.auth {
                NetworkAuth::Unauth => AppViewAuth::Unauth,
                NetworkAuth::ServiceJwt => AppViewAuth::ServiceJwt {
                    aud: n.appview_did.clone(),
                    key: Box::new(key.clone()),
                },
            };
            AppViewClient::new(&n.name, &n.appview_url, auth, net.clone())
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use axum::Router;
    use axum::extract::RawQuery;
    use axum::http::HeaderMap;
    use axum::routing::get;
    use serde_json::{Value, json};

    use super::*;
    use crate::config::NetworkConfig;
    use crate::test_support::serve_fake;

    async fn echo(headers: HeaderMap, RawQuery(q): RawQuery) -> axum::Json<Value> {
        axum::Json(json!({
            "authorization": headers.get("authorization").and_then(|v| v.to_str().ok()),
            "query": q,
        }))
    }

    #[tokio::test]
    async fn auth_modes_and_params() {
        let base = serve_fake(Router::new().route("/xrpc/{method}", get(echo))).await;
        let net = NetClient::loopback_for_tests();
        let key = ServiceKey::new("prism.example.com", &[5u8; 32]).expect("key");

        let open = AppViewClient::new("bluesky", &base, AppViewAuth::Unauth, net.clone());
        let r: Value = open.get_posts(&["at://a/b/1".into(), "at://a/b/2".into()]).await.expect("call").json().expect("json");
        assert_eq!(r["authorization"], Value::Null, "unauth sends no Authorization");
        assert_eq!(r["query"], "uris=at%3A%2F%2Fa%2Fb%2F1&uris=at%3A%2F%2Fa%2Fb%2F2");

        let signed = AppViewClient::new(
            "signed",
            &base,
            AppViewAuth::ServiceJwt {
                aud: "did:web:appview.signed.example".into(),
                key: Box::new(key),
            },
            net,
        );
        let r: Value = signed.get_profile("alice.test").await.expect("call").json().expect("json");
        let bearer = r["authorization"].as_str().expect("bearer");
        let token = bearer.strip_prefix("Bearer ").expect("scheme");
        use base64::Engine;
        let claims: Value = serde_json::from_slice(
            &base64::engine::general_purpose::URL_SAFE_NO_PAD
                .decode(token.split('.').nth(1).expect("payload"))
                .expect("b64"),
        )
        .expect("json");
        assert_eq!(claims["lxm"], "app.bsky.actor.getProfile");
        assert_eq!(claims["aud"], "did:web:appview.signed.example");
    }

    #[test]
    fn clients_exist_only_for_enabled_outbound_networks() {
        let mut cfg = ConstellationConfig::default();
        cfg.networks.push(NetworkConfig {
            name: "extra".into(),
            pds_patterns: vec![],
            appview_url: "https://appview.extra.example".into(),
            auth: NetworkAuth::ServiceJwt,
            appview_did: "did:web:appview.extra.example".into(),
            rate_per_sec: Some(1),
            burst: Some(1),
            enabled: true,
            background_rate_per_sec: crate::config::background_rate_per_sec(),
            background_burst: crate::config::background_burst(),
        });
        let key = ServiceKey::new("prism.example.com", &[5u8; 32]).expect("key");
        let clients = from_config(&cfg, &NetClient::loopback_for_tests(), &key);
        let names: Vec<&str> = clients.iter().map(AppViewClient::network).collect();
        // community is local.
        assert_eq!(names, ["bluesky", "community-a", "extra"]);
    }
}
