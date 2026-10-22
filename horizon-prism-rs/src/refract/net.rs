//! Refract's outbound calls (refract design §4.4, §4.5), each through its
//! own bucket: Bluesky's `getProfile` for rule 1, `getRepoStatus` on the
//! DID's PDS for rule 3 (a `refract` share per host), the community
//! AppView for rule 4 and the label reads, and a labeler's `queryLabels`.
//! All are background work through the internal transport, with no user
//! identity.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use axum::http::{HeaderMap, HeaderName, HeaderValue};
use serde_json::Value;

use crate::appview_clients::AppViewClient;
use crate::constellation::Cascade;
use crate::net::{BodyCap, NetClient, NetError, NetResponse, RequestOptions};
use crate::xrpc::pools::{BackgroundBucket, Requester, SharedBucket};

/// The request header naming the labelers a client accepts.
pub const ACCEPT_LABELERS: &str = "atproto-accept-labelers";
/// The response header naming the labelers an AppView applied.
pub const CONTENT_LABELERS: &str = "atproto-content-labelers";
/// Per-call timeout of a PDS or labeler call.
pub const CALL_TIMEOUT: Duration = Duration::from_secs(10);
/// The Bluesky bucket's burst (§3.3, a constant).
pub const BLUESKY_BURST: u32 = 5;
/// The sweep bucket's burst (§3.3, a constant).
pub const SWEEP_BURST: u32 = 4;
/// `queryLabels` page size.
pub const LABEL_PAGE: u32 = 250;
/// The network whose AppView is Bluesky's (Terms).
pub const BLUESKY: &str = "bluesky";

/// A call's answer: the status and the decoded body, or a transport
/// failure.
#[derive(Debug, Clone)]
pub enum Answer {
    Reply { status: u16, body: Value, headers: HeaderMap },
    Failed(String),
}

impl Answer {
    fn from(result: Result<(NetResponse, HeaderMap), NetError>) -> Self {
        match result {
            Ok((r, headers)) => Self::Reply {
                status: r.status.as_u16(),
                body: serde_json::from_slice(&r.body).unwrap_or(Value::Null),
                headers,
            },
            Err(e) => Self::Failed(e.to_string()),
        }
    }

    /// The XRPC `error` of a non-2xx answer.
    #[must_use]
    pub fn error(&self) -> Option<&str> {
        match self {
            Self::Reply { status, body, .. } if !(200..300).contains(status) => body.get("error").and_then(Value::as_str),
            _ => None,
        }
    }

    /// The XRPC `message` of an answer.
    #[must_use]
    pub fn message(&self) -> Option<&str> {
        match self {
            Self::Reply { body, .. } => body.get("message").and_then(Value::as_str),
            Self::Failed(_) => None,
        }
    }

    #[must_use]
    pub fn status(&self) -> Option<u16> {
        match self {
            Self::Reply { status, .. } => Some(*status),
            Self::Failed(_) => None,
        }
    }

    /// Whether the response's `atproto-content-labelers` names `labeler`.
    #[must_use]
    pub fn applied_labeler(&self, labeler: &str) -> bool {
        let Self::Reply { headers, .. } = self else {
            return false;
        };
        headers
            .get_all(CONTENT_LABELERS)
            .iter()
            .filter_map(|v| v.to_str().ok())
            .flat_map(|v| v.split(','))
            .any(|item| item.split(';').next().map(str::trim) == Some(labeler))
    }
}

/// Refract's clients and buckets. Cheap to clone.
#[derive(Clone)]
pub struct RefractNet {
    inner: Arc<Inner>,
}

struct Inner {
    net: NetClient,
    /// Network name to its AppView client (Bluesky's among them).
    appviews: HashMap<String, AppViewClient>,
    bluesky_bucket: BackgroundBucket,
    /// The community networks' background buckets, shared with the
    /// profile refresh when the cascade gives them.
    background: HashMap<String, BackgroundBucket>,
    /// The community networks' request buckets (the serve-time check).
    request: HashMap<String, SharedBucket>,
    /// The sweep's own bucket per network.
    sweep: Mutex<HashMap<String, BackgroundBucket>>,
    sweep_rate: f64,
    /// The `refract` share of each PDS host.
    per_pds: Mutex<HashMap<String, BackgroundBucket>>,
    per_pds_rate: f64,
}

impl RefractNet {
    /// The clients of `appviews` (one per outbound network), drawing on
    /// `cascade`'s background and request buckets when given.
    #[must_use]
    pub fn new(net: NetClient, appviews: Vec<AppViewClient>, cascade: Option<&Cascade>, cfg: &crate::config::RefractConfig) -> Self {
        let mut background = HashMap::new();
        let mut request = HashMap::new();
        for c in &appviews {
            let name = c.network().to_string();
            let bucket = cascade.and_then(|k| k.background_bucket(&name)).unwrap_or_else(|| BackgroundBucket::new(1.0, 2));
            background.insert(name.clone(), bucket);
            if let Some(b) = cascade.and_then(|k| k.request_bucket(&name)) {
                request.insert(name, b);
            }
        }
        Self {
            inner: Arc::new(Inner {
                net,
                appviews: appviews.into_iter().map(|c| (c.network().to_string(), c)).collect(),
                bluesky_bucket: BackgroundBucket::new(cfg.bluesky_rate_per_sec.0, BLUESKY_BURST),
                background,
                request,
                sweep: Mutex::new(HashMap::new()),
                sweep_rate: cfg.sweep_rate_per_sec.0,
                per_pds: Mutex::new(HashMap::new()),
                per_pds_rate: cfg.per_pds_rate_per_sec.0,
            }),
        }
    }

    /// Whether `network` has an AppView client here.
    #[must_use]
    pub fn has_appview(&self, network: &str) -> bool {
        self.inner.appviews.contains_key(network)
    }

    async fn query(&self, network: &str, method: &str, params: &[(&str, String)], labeler: Option<&str>) -> Answer {
        let Some(client) = self.inner.appviews.get(network) else {
            return Answer::Failed(format!("no AppView client for network {network}"));
        };
        let mut headers = Vec::new();
        if let Some(l) = labeler {
            match HeaderValue::from_str(l) {
                Ok(v) => headers.push((HeaderName::from_static(ACCEPT_LABELERS), v)),
                Err(e) => return Answer::Failed(e.to_string()),
            }
        }
        Answer::from(client.query_with_headers(method, params, &headers).await)
    }

    /// Rule 1: Bluesky's own `getProfile`, through Refract's bucket on the
    /// `bluesky` network (not the cascade, §4.4).
    pub async fn bluesky_profile(&self, did: &str) -> Answer {
        self.inner.bluesky_bucket.acquire().await;
        self.query(BLUESKY, "app.bsky.actor.getProfile", &[("actor", did.to_string())], None).await
    }

    /// Rule 4, tier 1: the community AppView's `getProfile`, with the
    /// labeler header when there is a labeler, through that network's
    /// background bucket.
    pub async fn community_profile(&self, network: &str, did: &str, labeler: Option<&str>) -> Answer {
        if let Some(b) = self.inner.background.get(network) {
            b.acquire().await;
        }
        self.query(network, "app.bsky.actor.getProfile", &[("actor", did.to_string())], labeler).await
    }

    /// The sweep's `getPosts` (≤ 25 URIs), through the sweep's own bucket on
    /// that network (§4.5).
    pub async fn sweep_posts(&self, network: &str, uris: &[String], labeler: &str) -> Answer {
        let bucket = {
            let mut map = self.inner.sweep.lock().unwrap_or_else(PoisonError::into_inner);
            map.entry(network.to_string()).or_insert_with(|| BackgroundBucket::new(self.inner.sweep_rate, SWEEP_BURST)).clone()
        };
        bucket.acquire().await;
        self.posts(network, uris, labeler).await
    }

    /// The serve-time check's `getPosts` (≤ 25 URIs), drawing one token of
    /// the network's request bucket on `req`'s share. `None`: no budget.
    pub async fn request_posts(&self, req: &Requester, network: &str, uris: &[String], labeler: &str) -> Option<Answer> {
        if let Some(b) = self.inner.request.get(network)
            && !req.draw(b)
        {
            return None;
        }
        let left = req.deadline().saturating_duration_since(std::time::Instant::now());
        if left.is_zero() {
            return None;
        }
        tokio::time::timeout(left, self.posts(network, uris, labeler)).await.ok()
    }

    async fn posts(&self, network: &str, uris: &[String], labeler: &str) -> Answer {
        let params: Vec<(&str, String)> = uris.iter().take(25).map(|u| ("uris", u.clone())).collect();
        self.query(network, "app.bsky.feed.getPosts", &params, Some(labeler)).await
    }

    fn host_bucket(&self, endpoint: &str) -> BackgroundBucket {
        let host = crate::constellation::matcher::pds_host(endpoint).unwrap_or_default();
        let mut map = self.inner.per_pds.lock().unwrap_or_else(PoisonError::into_inner);
        if map.len() > 10_000 {
            map.clear();
        }
        map.entry(host).or_insert_with(|| BackgroundBucket::new(self.inner.per_pds_rate, 1)).clone()
    }

    /// Rule 3: `getRepoStatus` on the PDS, through the `refract` share of
    /// the host's budget.
    pub async fn repo_status(&self, endpoint: &str, did: &str) -> Answer {
        self.host_bucket(endpoint).acquire().await;
        let url = format!("{}/xrpc/com.atproto.sync.getRepoStatus?did={did}", endpoint.trim_end_matches('/'));
        let options = RequestOptions::new(BodyCap::REPO_STATUS).timeout(CALL_TIMEOUT);
        Answer::from(self.inner.net.request_raw(axum::http::Method::GET, &url, None, &options).await)
    }

    /// One `queryLabels` page from a labeler's own endpoint.
    pub async fn query_labels(&self, endpoint: &str, patterns: &[String], source: &str, cursor: Option<&str>) -> Answer {
        let mut url = match url::Url::parse(&format!("{}/xrpc/com.atproto.label.queryLabels", endpoint.trim_end_matches('/'))) {
            Ok(u) => u,
            Err(e) => return Answer::Failed(e.to_string()),
        };
        {
            let mut q = url.query_pairs_mut();
            for p in patterns {
                q.append_pair("uriPatterns", p);
            }
            q.append_pair("sources", source);
            q.append_pair("limit", &LABEL_PAGE.to_string());
            if let Some(c) = cursor {
                q.append_pair("cursor", c);
            }
        }
        let options = RequestOptions::new(BodyCap::APPVIEW).timeout(CALL_TIMEOUT);
        Answer::from(self.inner.net.request_raw(axum::http::Method::GET, url.as_str(), None, &options).await)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn content_labelers_header_is_read_per_item() {
        let mut headers = HeaderMap::new();
        headers.insert(CONTENT_LABELERS, HeaderValue::from_static("did:plc:a;redact, did:plc:b"));
        let a = Answer::Reply { status: 200, body: Value::Null, headers };
        assert!(a.applied_labeler("did:plc:a") && a.applied_labeler("did:plc:b"));
        assert!(!a.applied_labeler("did:plc:c"));
        assert!(!Answer::Failed("x".into()).applied_labeler("did:plc:a"));
    }
}
