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
    /// Connect, TLS, reset, or a rejected redirect.
    Transport,
    /// The call never left: an address the resolver guard refused, a
    /// scheme that is not allowed, or a URL that does not build. A
    /// configuration fault, not Farsight failing.
    RefusedAddress,
    /// `429 RateLimitExceeded`: this key's allowance on Farsight is spent.
    RateLimited,
    /// `503 Overloaded`: Farsight had no place for the call in time, or a
    /// query of it ran too long. Farsight is up, and full.
    Overloaded,
    /// Any other status but 200.
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
            Self::RefusedAddress => "refused_address",
            Self::RateLimited => "rate_limited",
            Self::Overloaded => "overloaded",
            Self::Status(401 | 403) => "auth",
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

    /// Whether `error` means the call never left: an address the §II-8.5
    /// resolver guard refused (a literal, or every address of a hostname),
    /// a scheme that is not allowed, or a URL that does not build. That is
    /// a configuration fault, logged as an error at each refusal
    /// (§5.14.4). The log names the method and `farsight.url`'s host only:
    /// a `checkBlocks` URL carries the accounts asked about.
    fn refused(&self, error: &NetError, method: &'static str) -> bool {
        let reason = match error {
            NetError::BlockedAddress { .. } => "blocked_address",
            NetError::SchemeNotAllowed { .. } => "scheme_not_allowed",
            NetError::InvalidUrl { .. } => "invalid_url",
            _ => return false,
        };
        let host = Url::parse(&self.base).ok().and_then(|u| u.host_str().map(str::to_string)).unwrap_or_default();
        tracing::error!(
            event = "farsight_address_refused",
            method,
            host,
            reason,
            "the Farsight call was refused under the outbound guard; farsight.url must resolve to a public address"
        );
        true
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
        let url = Url::parse_with_params(&format!("{}{CHECK_BLOCKS_PATH}", self.base), &params).map_err(|e| {
            self.refused(&NetError::InvalidUrl { url: String::new(), reason: e.to_string() }, "checkBlocks");
            CallError::RefusedAddress
        })?;
        let options = RequestOptions::new(BodyCap::APPVIEW)
            .timeout(timeout)
            .header(AUTHORIZATION, self.authorization.clone());
        let response = self.net.get(url.as_str(), &options).await.map_err(|e| match e {
            NetError::Timeout => CallError::Timeout,
            NetError::BodyTooLarge { .. } => CallError::TooLarge,
            e if self.refused(&e, "checkBlocks") => CallError::RefusedAddress,
            _ => CallError::Transport,
        })?;
        if response.status.as_u16() != 200 {
            // Farsight uses 429 and 503 for two conditions each, told apart
            // by the error's name, not by the status.
            let named = serde_json::from_slice::<Value>(&response.body).ok();
            let error = named.as_ref().and_then(|v| v.get("error")).and_then(Value::as_str);
            return Err(match (response.status.as_u16(), error) {
                (429, Some("RateLimitExceeded")) => CallError::RateLimited,
                (503, Some("Overloaded")) => CallError::Overloaded,
                (status, _) => CallError::Status(status),
            });
        }
        let body: Value = serde_json::from_slice(&response.body).map_err(|_| CallError::Parse)?;
        ChunkAnswer::read(&body, others).ok_or(CallError::Parse)
    }
}

/// Farsight's own status report.
pub const GET_STATS_PATH: &str = "/xrpc/app.nearhorizon.farsight.query.getStats";

impl FarsightClient {
    /// `query.getStats`, as Farsight returns it.
    ///
    /// # Errors
    ///
    /// A short reason: `timeout`, `transport`, `refused_address`,
    /// `forbidden`, `status` or `parse`.
    pub async fn get_stats(&self) -> Result<Value, &'static str> {
        let url = format!("{}{GET_STATS_PATH}", self.base);
        let options = RequestOptions::new(BodyCap::REPO_STATUS).header(AUTHORIZATION, self.authorization.clone());
        let response = self.net.get(&url, &options).await.map_err(|e| match e {
            NetError::Timeout => "timeout",
            e if self.refused(&e, "getStats") => "refused_address",
            _ => "transport",
        })?;
        match response.status.as_u16() {
            200 => serde_json::from_slice(&response.body).map_err(|_| "parse"),
            401 | 403 => Err("forbidden"),
            _ => Err("status"),
        }
    }
}

/// The method that asks Farsight to read an account's repository.
pub const REQUEST_BACKFILL_PATH: &str = "/xrpc/app.nearhorizon.farsight.admin.requestBackfill";

impl FarsightClient {
    /// `admin.requestBackfill { actor, priority: "normal" }`: ask Farsight
    /// to read `actor`'s own repository now. Any 2xx is success (Farsight
    /// answers 202). Its outcome is never polled.
    ///
    /// # Errors
    ///
    /// A short reason, for counting: `queue_full`, `rate_limited`,
    /// `forbidden`, `status`, `timeout`, `refused_address` or `transport`.
    pub async fn request_backfill(&self, actor: &str, force: bool) -> Result<(), &'static str> {
        let url = format!("{}{REQUEST_BACKFILL_PATH}", self.base);
        let mut body = serde_json::json!({"actor": actor, "priority": "normal"});
        if force {
            body["force"] = Value::Bool(true);
        }
        let options = RequestOptions::new(BodyCap::REPO_STATUS).header(AUTHORIZATION, self.authorization.clone());
        let response = self.net.post_json(&url, &body, &options).await.map_err(|e| match e {
            NetError::Timeout => "timeout",
            e if self.refused(&e, "requestBackfill") => "refused_address",
            _ => "transport",
        })?;
        if response.status.is_success() {
            return Ok(());
        }
        let named = serde_json::from_slice::<Value>(&response.body).ok();
        let error = named.as_ref().and_then(|v| v.get("error")).and_then(Value::as_str);
        Err(match (response.status.as_u16(), error) {
            (_, Some("QueueFull")) => "queue_full",
            (429, _) => "rate_limited",
            (401 | 403, _) => "forbidden",
            _ => "status",
        })
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
        // Farsight's two 429s and two 503s differ only by the error's name.
        for (marker, status, error) in [
            ("ratelimited", StatusCode::TOO_MANY_REQUESTS, "RateLimitExceeded"),
            ("overloaded", StatusCode::SERVICE_UNAVAILABLE, "Overloaded"),
            ("notsetup", StatusCode::SERVICE_UNAVAILABLE, "SetupRequired"),
        ] {
            if query.contains(marker) {
                return (status, axum::Json(json!({"error": error, "message": "no"}))).into_response();
            }
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
        assert_eq!(ask("ratelimited", 2000).await, Some(CallError::RateLimited));
        assert_eq!(ask("overloaded", 2000).await, Some(CallError::Overloaded));
        assert_eq!(ask("notsetup", 2000).await, Some(CallError::Status(503)));
        let labels = [CallError::RateLimited, CallError::Overloaded, CallError::Status(403), CallError::Status(503)].map(CallError::label);
        assert_eq!(labels, ["rate_limited", "overloaded", "auth", "status"]);
        assert_eq!(ask("garbage", 2000).await, Some(CallError::Parse));
        assert_eq!(ask("huge", 5000).await, Some(CallError::TooLarge));
        let dead = FarsightClient::new(NetClient::loopback_for_tests(), "http://127.0.0.1:9", "fsk_test").expect("client");
        assert_eq!(dead.check_blocks(V, &[A.to_string()], Duration::from_secs(2)).await.err(), Some(CallError::Transport));
        assert!(FarsightClient::new(NetClient::loopback_for_tests(), "http://x", "bad\nkey").is_none());
    }

    /// Every name resolves to a private address.
    struct PrivateDns;

    impl crate::net::HostLookup for PrivateDns {
        fn lookup(&self, _host: String) -> futures::future::BoxFuture<'static, Result<Vec<std::net::IpAddr>, crate::net::resolver::BoxError>> {
            Box::pin(async { Ok(vec![std::net::IpAddr::from([192, 168, 1, 7])]) })
        }
    }

    /// The log lines written while a test runs.
    #[derive(Clone, Default)]
    struct Logs(Arc<Mutex<Vec<u8>>>);

    impl std::io::Write for Logs {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            self.0.lock().expect("logs").extend_from_slice(buf);
            Ok(buf.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    /// B-81: `farsight.url` naming a host that resolves to a private
    /// address is refused at each call, counted `refused_address` (not
    /// `transport`), and logged as an error each time, naming no account.
    #[tokio::test]
    async fn a_hostname_resolving_privately_is_refused_and_logged_at_each_call() {
        let logs = Logs::default();
        let writer = logs.clone();
        let subscriber = tracing_subscriber::fmt()
            .with_writer(move || writer.clone())
            .with_ansi(false)
            .with_max_level(tracing::Level::INFO)
            .finish();
        let _guard = tracing::subscriber::set_default(subscriber);
        let net = NetClient::with_lookup(&crate::net::NetConfig::new("appview.example.com", "https://plc.directory"), PrivateDns)
            .expect("net");
        let c = FarsightClient::new(net, "https://farsight.internal", "fsk_test").expect("client");

        for _ in 0..2 {
            let err = c.check_blocks(V, &[A.to_string()], Duration::from_secs(2)).await.err();
            assert_eq!(err, Some(CallError::RefusedAddress));
            assert_eq!(err.map(CallError::label), Some("refused_address"));
        }
        assert_eq!(c.request_backfill(V, false).await, Err("refused_address"));
        assert_eq!(c.get_stats().await.err(), Some("refused_address"));

        let text = String::from_utf8(logs.0.lock().expect("logs").clone()).expect("utf-8");
        let refusals: Vec<&str> = text.lines().filter(|l| l.contains("farsight_address_refused")).collect();
        assert_eq!(refusals.len(), 4, "{text}");
        assert!(refusals.iter().all(|l| l.contains("ERROR") && l.contains("farsight.internal")), "{text}");
        // Neither the accounts asked about nor the key are logged.
        assert!(!text.contains(A) && !text.contains("fsk_test"), "{text}");
    }
}
