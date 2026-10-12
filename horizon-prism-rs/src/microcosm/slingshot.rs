//! Slingshot record client (design-v2 §7.1, §8.3 step 6, §10.4 step 4):
//! record-level fallback when local data and the AppView cascade miss.
//!
//! - `com.atproto.repo.getRecord` on the Slingshot edge cache, through `net`
//!   (SSRF-guarded), 3 s timeout, 256 KiB body cap (§8.5 `getRecord`).
//! - Classification: 2xx with a parseable `{uri, cid?, value}` is a record;
//!   `RecordNotFound` / `NotFound` (400) or 404 is [`SlingshotError::NotFound`];
//!   429, 5xx and transport failures are [`SlingshotError::Transient`];
//!   anything else (including an oversized body) is
//!   [`SlingshotError::Invalid`].

//! - Every call is charged to its request: it draws on the shared Slingshot
//!   request bucket (design-v3 §II-8.3) and is not made once the request's
//!   deadline has passed ([`SlingshotError::NoBudget`]).

use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};
use serde_json::Value;
use url::Url;

use crate::db::locks::uri_author;
use crate::net::{BodyCap, NetClient, RequestOptions};
use crate::xrpc::pools::{Requester, SharedBucket};

/// Per-call timeout (§8.3).
pub const TIMEOUT: Duration = Duration::from_secs(3);

/// A record as Slingshot returns it.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SlingshotRecord {
    pub uri: String,
    #[serde(default)]
    pub cid: Option<String>,
    pub value: Value,
}

/// A failed lookup.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum SlingshotError {
    /// The record does not exist (`RecordNotFound`).
    #[error("record not found")]
    NotFound,
    /// No answer: timeout, connection failure, 429 or 5xx.
    #[error("slingshot unavailable: {0}")]
    Transient(String),
    /// A malformed URI, an unexpected status, or an unusable body.
    #[error("slingshot answer unusable: {0}")]
    Invalid(String),
    /// No call was made: the requester's share or pool of the Slingshot
    /// request bucket is empty, or the request's deadline has passed
    /// (design-v3 §II-8.3). No answer, like `Transient`.
    #[error("no slingshot budget for this request")]
    NoBudget,
}

#[derive(Deserialize)]
struct XrpcErrorBody {
    #[serde(default)]
    error: Option<String>,
}

/// Split `at://did/collection/rkey`.
fn parts(uri: &str) -> Option<(&str, &str, &str)> {
    let did = uri_author(uri)?;
    let rest = uri.strip_prefix("at://")?.strip_prefix(did)?.strip_prefix('/')?;
    let (collection, rkey) = rest.split_once('/')?;
    (!collection.is_empty() && !rkey.is_empty() && !rkey.contains('/')).then_some((did, collection, rkey))
}

/// The client. Cheap to clone.
#[derive(Clone)]
pub struct SlingshotClient {
    net: NetClient,
    base: String,
    bucket: Option<SharedBucket>,
}

impl SlingshotClient {
    #[must_use]
    pub fn new(net: NetClient, base_url: &str) -> Self {
        Self {
            net,
            base: base_url.trim_end_matches('/').to_string(),
            bucket: None,
        }
    }

    /// Draw every call on `bucket`, the shared Slingshot request bucket
    /// (`microcosm.slingshot_rate_per_sec`).
    #[must_use]
    pub fn metered(mut self, bucket: SharedBucket) -> Self {
        self.bucket = Some(bucket);
        self
    }

    /// `getRecord` for an at-uri, charged to `req`.
    ///
    /// # Errors
    ///
    /// [`SlingshotError`].
    pub async fn get_record(&self, req: &Requester, uri: &str) -> Result<SlingshotRecord, SlingshotError> {
        let (repo, collection, rkey) = parts(uri).ok_or_else(|| SlingshotError::Invalid(format!("not a record URI: {uri}")))?;
        if Instant::now() >= req.deadline() || self.bucket.as_ref().is_some_and(|b| !req.draw(b)) {
            return Err(SlingshotError::NoBudget);
        }
        let url = Url::parse_with_params(
            &format!("{}/xrpc/com.atproto.repo.getRecord", self.base),
            &[("repo", repo), ("collection", collection), ("rkey", rkey)],
        )
        .map_err(|e| SlingshotError::Invalid(e.to_string()))?;
        let options = RequestOptions::new(BodyCap::GET_RECORD).timeout(TIMEOUT);
        let response = match self.net.get(url.as_str(), &options).await {
            Ok(r) => r,
            Err(e) if e.is_transient() => return Err(SlingshotError::Transient(e.to_string())),
            // Includes `BodyTooLarge` (the 256 KiB cap).
            Err(e) => return Err(SlingshotError::Invalid(e.to_string())),
        };
        let status = response.status.as_u16();
        if response.is_success() {
            return response.json::<SlingshotRecord>().map_err(|e| SlingshotError::Invalid(e.to_string()));
        }
        let error = serde_json::from_slice::<XrpcErrorBody>(&response.body).ok().and_then(|b| b.error);
        match (status, error.as_deref()) {
            (404, _) | (400, Some("RecordNotFound" | "NotFound")) => Err(SlingshotError::NotFound),
            (429 | 500..=599, _) => Err(SlingshotError::Transient(format!("HTTP {status}"))),
            _ => Err(SlingshotError::Invalid(format!("HTTP {status} {error:?}"))),
        }
    }
}

#[cfg(test)]
mod tests {
    use axum::Router;
    use axum::extract::Query;
    use axum::http::StatusCode;
    use axum::response::IntoResponse;
    use axum::routing::get;
    use serde_json::json;

    use super::*;
    use crate::test_support::serve_fake;

    const DID: &str = "did:plc:ssssssssssssssssssssssss";

    async fn fake(Query(q): Query<std::collections::HashMap<String, String>>) -> axum::response::Response {
        let uri = format!("at://{}/{}/{}", q["repo"], q["collection"], q["rkey"]);
        match q["rkey"].as_str() {
            "hit" => axum::Json(json!({"uri": uri, "cid": "bafyhit", "value": {"$type": "app.bsky.feed.post", "text": "hi"}}))
                .into_response(),
            "miss" => (StatusCode::BAD_REQUEST, axum::Json(json!({"error": "RecordNotFound", "message": "nope"}))).into_response(),
            "down" => StatusCode::SERVICE_UNAVAILABLE.into_response(),
            "huge" => axum::Json(json!({"uri": uri, "value": {"text": "x".repeat(300 * 1024)}})).into_response(),
            _ => StatusCode::IM_A_TEAPOT.into_response(),
        }
    }

    #[tokio::test]
    async fn get_record_classifies_hits_misses_and_caps() {
        let base = serve_fake(Router::new().route("/xrpc/com.atproto.repo.getRecord", get(fake))).await;
        let client = SlingshotClient::new(NetClient::loopback_for_tests(), &base);
        let post = |rkey: &str| format!("at://{DID}/app.bsky.feed.post/{rkey}");

        let req = &Requester::test();

        let hit = client.get_record(req, &post("hit")).await.expect("hit");
        assert_eq!(hit.uri, post("hit"));
        assert_eq!(hit.cid.as_deref(), Some("bafyhit"));
        assert_eq!(hit.value["text"], "hi");

        assert_eq!(client.get_record(req, &post("miss")).await, Err(SlingshotError::NotFound));
        assert!(matches!(client.get_record(req, &post("down")).await, Err(SlingshotError::Transient(_))));
        // 300 KiB exceeds the 256 KiB getRecord cap.
        assert!(matches!(client.get_record(req, &post("huge")).await, Err(SlingshotError::Invalid(e)) if e.contains("cap")));
        assert!(matches!(client.get_record(req, &post("teapot")).await, Err(SlingshotError::Invalid(_))));
        assert!(matches!(client.get_record(req, "https://not-at-uri").await, Err(SlingshotError::Invalid(_))));
    }

    #[tokio::test]
    async fn calls_draw_on_the_request_bucket_and_respect_the_deadline() {
        let base = serve_fake(Router::new().route("/xrpc/com.atproto.repo.getRecord", get(fake))).await;
        // 1/s, burst 2: one token in the non-member pool.
        let bucket = crate::xrpc::pools::Shares::for_tests().bucket("slingshot", 1.0, 2);
        let client = SlingshotClient::new(NetClient::loopback_for_tests(), &base).metered(bucket);
        let uri = format!("at://{DID}/app.bsky.feed.post/hit");
        let req = Requester::test();
        client.get_record(&req, &uri).await.expect("the one token");
        assert_eq!(client.get_record(&req, &uri).await, Err(SlingshotError::NoBudget));
        assert!(req.spent());

        let late = Requester::test().with_deadline(Instant::now());
        let unmetered = SlingshotClient::new(NetClient::loopback_for_tests(), &base);
        assert_eq!(unmetered.get_record(&late, &uri).await, Err(SlingshotError::NoBudget));
        assert!(!late.spent(), "a passed deadline is not an empty pool");
    }

    #[test]
    fn uri_parts() {
        assert_eq!(parts(&format!("at://{DID}/app.bsky.feed.post/3k")), Some((DID, "app.bsky.feed.post", "3k")));
        assert_eq!(parts(&format!("at://{DID}/app.bsky.feed.post")), None);
        assert_eq!(parts(&format!("at://{DID}/app.bsky.feed.post/3k/x")), None);
    }
}
