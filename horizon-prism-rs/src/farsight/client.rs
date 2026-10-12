//! The HTTP client for Farsight's `checkBlocks` (design-v3 §5.14.4).
//!
//! Calls go through `net`: `https` only, the resolver guard, no proxies. The
//! one credential sent is the operator's API key, as `Authorization: Bearer`.
//! A viewer's credentials are never forwarded.

use std::time::Duration;

use reqwest::header::{AUTHORIZATION, HeaderValue};
use serde_json::Value;
use url::Url;

use super::answer::ChunkAnswer;
use super::chunks::CHECK_BLOCKS_PATH;
use crate::net::{BodyCap, NetClient, NetError, RequestOptions};

/// Why a call has no answer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CallError {
    /// The call reached its time limit.
    Timeout,
    /// Connect, TLS, reset, or an address the guard refuses.
    Transport,
    /// Any status but 200.
    Status(u16),
    /// A 200 whose body is not a `checkBlocks` answer.
    Parse,
    /// The answer is over the response cap: it was big, which says nothing
    /// about whether Farsight is failing.
    TooLarge,
}

impl CallError {
    /// A revoked or under-scoped key.
    #[must_use]
    pub fn is_auth(self) -> bool {
        matches!(self, Self::Status(401 | 403))
    }

    #[must_use]
    pub fn label(self) -> &'static str {
        match self {
            Self::Timeout => "timeout",
            Self::Transport => "transport",
            Self::Status(_) => "status",
            Self::Parse => "parse",
            Self::TooLarge => "too_large",
        }
    }
}

/// The client. Cheap to clone.
#[derive(Clone)]
pub struct FarsightClient {
    net: NetClient,
    base: String,
    authorization: HeaderValue,
}

impl std::fmt::Debug for FarsightClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FarsightClient").field("base", &self.base).finish_non_exhaustive()
    }
}

impl FarsightClient {
    /// A client for the instance at `base_url`, sending `token` as its API
    /// key. `None` when the token cannot be a header value.
    #[must_use]
    pub fn new(net: NetClient, base_url: &str, token: &str) -> Option<Self> {
        let mut authorization = HeaderValue::from_str(&format!("Bearer {token}")).ok()?;
        authorization.set_sensitive(true);
        Some(Self {
            net,
            base: base_url.trim_end_matches('/').to_string(),
            authorization,
        })
    }

    /// `checkBlocks(actor, others)`, read into pair answers. `others` are
    /// canonical DIDs that fit one call. `includeInactive` is never sent, so
    /// hidden blockers and hidden list owners contribute nothing.
    ///
    /// # Errors
    ///
    /// [`CallError`] when there is no answer.
    pub async fn check_blocks(&self, actor: &str, others: &[String], timeout: Duration) -> Result<ChunkAnswer, CallError> {
        let mut params: Vec<(&str, &str)> = vec![("actor", actor)];
        params.extend(others.iter().map(|o| ("others", o.as_str())));
        let url = Url::parse_with_params(&format!("{}{CHECK_BLOCKS_PATH}", self.base), &params).map_err(|_| CallError::Transport)?;
        let options = RequestOptions::new(BodyCap::APPVIEW)
            .timeout(timeout)
            .header(AUTHORIZATION, self.authorization.clone());
        let response = self.net.get(url.as_str(), &options).await.map_err(|e| match e {
            NetError::Timeout => CallError::Timeout,
            NetError::BodyTooLarge { .. } => CallError::TooLarge,
            _ => CallError::Transport,
        })?;
        if response.status.as_u16() != 200 {
            return Err(CallError::Status(response.status.as_u16()));
        }
        let body: Value = serde_json::from_slice(&response.body).map_err(|_| CallError::Parse)?;
        ChunkAnswer::read(&body, others).ok_or(CallError::Parse)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use axum::Router;
    use axum::extract::{RawQuery, State};
    use axum::http::{HeaderMap, StatusCode};
    use axum::response::IntoResponse;
    use axum::routing::get;
    use serde_json::json;

    use super::*;
    use crate::test_support::serve_fake;

    const V: &str = "did:plc:vvvvvvvvvvvvvvvvvvvvvvvv";
    const A: &str = "did:plc:aaaaaaaaaaaaaaaaaaaaaaaa";
    const B: &str = "did:plc:bbbbbbbbbbbbbbbbbbbbbbbb";
    const LIST: &str = "at://did:plc:llllllllllllllllllllllll/app.bsky.graph.list/1";

    type Seen = Arc<Mutex<Vec<(String, Option<String>)>>>;

    async fn fake(State(seen): State<Seen>, headers: HeaderMap, RawQuery(q): RawQuery) -> axum::response::Response {
        let query = q.unwrap_or_default();
        let auth = headers.get("authorization").and_then(|v| v.to_str().ok()).map(str::to_string);
        seen.lock().expect("seen").push((query.clone(), auth));
        if query.contains("slow") {
            tokio::time::sleep(Duration::from_millis(400)).await;
        }
        if query.contains("denied") {
            return (StatusCode::FORBIDDEN, "{}").into_response();
        }
        if query.contains("garbage") {
            return "not json".into_response();
        }
        if query.contains("huge") {
            return "x".repeat(5 * 1024 * 1024).into_response();
        }
        axum::Json(json!({
            "actor": V,
            "results": [{"did": A, "blocksActor": {"direct": false, "lists": [LIST]}, "blockedByActor": {"direct": false, "lists": []}}],
            "partialFor": [],
            "freshness": {"coverage": {"level": "complete"}},
        }))
        .into_response()
    }

    async fn client() -> (FarsightClient, Seen) {
        let seen = Seen::default();
        let base = serve_fake(Router::new().route(CHECK_BLOCKS_PATH, get(fake)).with_state(Arc::clone(&seen))).await;
        (FarsightClient::new(NetClient::loopback_for_tests(), &base, "fsk_test").expect("client"), seen)
    }

    #[tokio::test]
    async fn a_call_sends_the_key_and_reads_the_answer() {
        let (c, seen) = client().await;
        let others = vec![A.to_string(), B.to_string()];
        let answer = c.check_blocks(V, &others, Duration::from_secs(2)).await.expect("answer");
        assert_eq!(answer.pairs[A].inbound.list.as_deref(), Some(LIST));
        assert!(answer.pairs[A].vouched && answer.pairs[B].vouched && !answer.pairs[B].any_block());
        let (query, auth) = seen.lock().expect("seen")[0].clone();
        assert_eq!(auth.as_deref(), Some("Bearer fsk_test"));
        assert_eq!(query, format!("actor={}&others={}&others={}", V.replace(':', "%3A"), A.replace(':', "%3A"), B.replace(':', "%3A")));
        assert!(!query.contains("includeInactive"));
        // The key never shows in debug output.
        assert!(!format!("{c:?}").contains("fsk_test"));
    }

    #[tokio::test]
    async fn every_failure_is_classified() {
        let (c, _) = client().await;
        let ask = |marker: &'static str, timeout_ms: u64| {
            let c = c.clone();
            async move {
                let others = vec![format!("did:web:{marker}.example.com")];
                c.check_blocks(V, &others, Duration::from_millis(timeout_ms)).await.err()
            }
        };
        assert_eq!(ask("slow", 100).await, Some(CallError::Timeout));
        assert_eq!(ask("denied", 2000).await, Some(CallError::Status(403)));
        assert!(CallError::Status(403).is_auth() && CallError::Status(401).is_auth() && !CallError::Status(500).is_auth());
        assert_eq!(ask("garbage", 2000).await, Some(CallError::Parse));
        assert_eq!(ask("huge", 5000).await, Some(CallError::TooLarge));
        let dead = FarsightClient::new(NetClient::loopback_for_tests(), "http://127.0.0.1:9", "fsk_test").expect("client");
        assert_eq!(dead.check_blocks(V, &[A.to_string()], Duration::from_secs(2)).await.err(), Some(CallError::Transport));
        assert!(FarsightClient::new(NetClient::loopback_for_tests(), "http://x", "bad\nkey").is_none());
    }
}
