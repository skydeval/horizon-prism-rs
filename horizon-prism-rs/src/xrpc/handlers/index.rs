//! `app.nearhorizon.index.requestRecord` (design-v2 §5.9): the safety net
//! for an interaction no relay delivered. There is no write proxy; the
//! AppView fetches the caller's own record from the caller's PDS and runs it
//! through the normal write path.
//!
//! - Procedure (POST), authentication required (any authenticated DID, §7.1
//!   `req`): the same-repo check below is what ties a request to its owner,
//!   and the admission predicate (§9.3) decides whether it is indexed.
//! - Rate limit: per DID, 0.5/s, burst 30 (§7.4).
//! - Validation, each its own `400 InvalidRequest` message (#232): the URI's
//!   repo must be the caller's DID; its collection must classify as
//!   `Ecosystem(_)` or `Kryphocron(_)`.
//! - Output: `{status: "indexed" | "deleted" | "out_of_scope" | "unchanged"}`.
//!   The answer concerns only the caller's own record, so it is not an
//!   enumeration surface.

use std::net::SocketAddr;
use std::sync::Arc;

use axum::Json;
use axum::body::Bytes;
use axum::extract::{ConnectInfo, State};
use axum::http::{HeaderMap, StatusCode};
use serde_json::Value;

use super::XrpcResult;
use crate::db::locks::uri_author;
use crate::firehose::backfill::BackfillSource;
use crate::firehose::classifier::{RecordClass, classify};
use crate::firehose::index_request::{IndexError, index_record};
use crate::firehose::writer::Writer;
use crate::hydration::hint::index_status;
use crate::xrpc::AppState;
use crate::xrpc::errors::XrpcError;
use crate::xrpc::auth::{authenticate, request_ip};
use crate::xrpc::rate_limit::Family;

pub const REQUEST_RECORD: &str = "app.nearhorizon.index.requestRecord";
/// §5.9: the URI must name a record in the caller's own repo.
pub const NOT_CALLERS_REPO: &str = "uri must be in the caller's repo";
/// §5.9: the collection must be one the AppView indexes.
pub const UNSUPPORTED_COLLECTION: &str = "uri's collection is not supported";

/// Handler state.
#[derive(Clone)]
pub struct IndexState {
    pub app: AppState,
    pub source: Arc<dyn BackfillSource>,
    pub writer: Writer,
    /// `firehose.deterministic_attempts`.
    pub attempts: u32,
}

/// `at://did/collection/rkey` → its three parts.
fn parts(uri: &str) -> Option<(&str, &str, &str)> {
    let did = uri_author(uri).filter(|d| proto_blue::syntax::Did::is_valid(d))?;
    let rest = uri.strip_prefix("at://")?.strip_prefix(did)?.strip_prefix('/')?;
    let (collection, rkey) = rest.split_once('/')?;
    (!collection.is_empty() && !rkey.is_empty() && !rkey.contains('/')).then_some((did, collection, rkey))
}

pub async fn request_record(
    State(s): State<IndexState>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    body: Bytes,
) -> XrpcResult {
    let authorization = headers.get("authorization").and_then(|v| v.to_str().ok());
    let caller = authenticate(&s.app, authorization, REQUEST_RECORD).await?;
    let ip = request_ip(&s.app, &headers, peer);
    s.app.limits.charge(REQUEST_RECORD, Family::IndexRequest, Some(&caller.did), ip)?;
    let uri = serde_json::from_slice::<Value>(&body)
        .ok()
        .and_then(|v| v.get("uri").and_then(Value::as_str).map(str::to_string))
        .ok_or_else(|| XrpcError::invalid("Error: Input must have the property \"uri\""))?;
    let (repo, collection, rkey) = parts(&uri).ok_or_else(|| XrpcError::invalid("Error: uri must be an at-uri"))?;
    if repo != caller.did {
        return Err(XrpcError::invalid(NOT_CALLERS_REPO));
    }
    if matches!(classify(collection, rkey), RecordClass::Unindexed) {
        return Err(XrpcError::invalid(UNSUPPORTED_COLLECTION));
    }
    match index_record(s.source.as_ref(), &s.writer, repo, collection, rkey, s.attempts, false).await {
        Ok(indexed) => Ok(Json(index_status(indexed.status))),
        Err(IndexError::Fetch(error)) => {
            tracing::info!(event = "request_record_fetch_failed", repo, %error, "the caller's PDS did not answer");
            Err(XrpcError::new(StatusCode::BAD_GATEWAY, "UpstreamFailure", "Could not fetch the record from its PDS"))
        }
        Err(IndexError::Write(error)) => {
            tracing::warn!(event = "request_record_write_failed", repo, %error, "requestRecord write failed");
            Err(XrpcError::internal())
        }
    }
}

#[cfg(test)]
mod tests {
    use async_trait::async_trait;
    use serde_json::json;
    use tokio::net::TcpListener;

    use super::*;
    use crate::aggregations::{ScopeSets, ScopeState};
    use crate::config::ScopePolicy;
    use crate::firehose::backfill::{ListPage, ListedRecord, PrepareError};
    use crate::hydration::testing::{hydrator, post};
    use crate::kryphocron_glue::jwt::tests::{AUDIENCE_HOST, StubFetcher, mint, signing_key, verifier};
    use crate::microcosm::LinksPage;
    use crate::private_tier::DisabledPrivateTier;
    use crate::test_support::fresh_db;
    use crate::xrpc::RateLimits;
    use crate::xrpc::router::index_router;

    const MEMBER: &str = "did:plc:mmmmmmmmmmmmmmmmmmmmmmmm";
    const OUTSIDER: &str = "did:plc:oooooooooooooooooooooooo";
    const SPAMMER: &str = "did:plc:ssssssssssssssssssssssss";

    /// A PDS at rev `3kzzzzzzzzzz2` serving posts whose rkey starts with
    /// `keep`, and nothing else.
    struct Pds;

    #[async_trait]
    impl BackfillSource for Pds {
        async fn prepare(&self, _: &str) -> Result<String, PrepareError> {
            Ok("https://pds.example.com".into())
        }
        async fn latest_rev(&self, _: &str, _: &str) -> Result<String, String> {
            Ok("3kzzzzzzzzzz2".into())
        }
        async fn list_records(&self, _: &str, _: &str, _: &str, _: Option<&str>) -> Result<ListPage, String> {
            Err("unused".into())
        }
        async fn get_record(&self, _: &str, did: &str, collection: &str, rkey: &str) -> Result<Option<ListedRecord>, String> {
            Ok(rkey.starts_with("keep").then(|| ListedRecord {
                uri: format!("at://{did}/{collection}/{rkey}"),
                cid: format!("bafy{rkey}"),
                value: json!({"$type": "app.bsky.feed.post", "text": format!("fetched {rkey}"), "createdAt": "2026-09-26T00:00:00Z"}),
            }))
        }
        async fn inbound_blocks(&self, _: &str, _: Option<&str>) -> Result<LinksPage, String> {
            Err("unused".into())
        }
    }

    #[tokio::test]
    async fn request_record_outcomes_validation_and_rate_limit() {
        let Some(db) = fresh_db("request_record").await else {
            return;
        };
        let p = &db.pool;
        let mut sets = ScopeSets::default();
        sets.scope.insert(MEMBER.into());
        sets.members.insert(MEMBER.into());
        let scope = ScopeState::new(sets.clone(), ScopePolicy::MembersAndOneHop, 10_000);
        let fetcher = StubFetcher::default();
        for (i, d) in [MEMBER, OUTSIDER, SPAMMER].into_iter().enumerate() {
            fetcher.set_key(d, signing_key(u8::try_from(i).expect("small") + 3));
        }
        let (auth, _d) = verifier(fetcher, 10, 4);
        let state = IndexState {
            app: AppState {
                hydrator: hydrator(p, sets),
                auth: Arc::new(auth),
                limits: RateLimits::new(),
                enroller: None,
                trusted_proxy: "172.30.0.10".parse().expect("ip"),
                did_document: Arc::new(json!({})),
            },
            source: Arc::new(Pds),
            writer: Writer::new(p.clone(), scope, Arc::new(DisabledPrivateTier)),
            attempts: 3,
        };
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let addr = listener.local_addr().expect("addr");
        tokio::spawn(async move {
            axum::serve(listener, index_router(state).into_make_service_with_connect_info::<SocketAddr>())
                .await
                .expect("serve");
        });
        let client = reqwest::Client::new();
        let call = |caller: &'static str, key: u8, uri: String| {
            let client = client.clone();
            async move {
                let token = mint(&signing_key(key), caller, &format!("did:web:{AUDIENCE_HOST}"), Some(REQUEST_RECORD));
                let r = client
                    .post(format!("http://{addr}/xrpc/{REQUEST_RECORD}"))
                    .header("authorization", format!("Bearer {token}"))
                    .json(&json!({ "uri": uri }))
                    .send()
                    .await
                    .expect("request");
                (r.status().as_u16(), r.json::<Value>().await.unwrap_or(Value::Null))
            }
        };
        let post_uri = |did: &str, rkey: &str| format!("at://{did}/app.bsky.feed.post/{rkey}");

        // Happy path: fetched from the PDS, admitted, indexed.
        let (status, v) = call(MEMBER, 3, post_uri(MEMBER, "keep1")).await;
        assert_eq!((status, v["status"].as_str()), (200, Some("indexed")), "{v}");
        let text: String = sqlx::query_scalar("SELECT text FROM posts WHERE uri = $1")
            .bind(post_uri(MEMBER, "keep1"))
            .fetch_one(p)
            .await
            .expect("indexed row");
        assert_eq!(text, "fetched keep1");

        // Unchanged: the same rev is already indexed.
        let (status, v) = call(MEMBER, 3, post_uri(MEMBER, "keep1")).await;
        assert_eq!((status, v["status"].as_str()), (200, Some("unchanged")));

        // Deleted: the PDS has no such record; the LWW delete applies.
        let gone = post(p, MEMBER, "gone", "stale", None, None).await;
        let (status, v) = call(MEMBER, 3, gone.clone()).await;
        assert_eq!((status, v["status"].as_str()), (200, Some("deleted")));
        let left: i64 = sqlx::query_scalar("SELECT count(*) FROM posts WHERE uri = $1").bind(&gone).fetch_one(p).await.expect("count");
        assert_eq!(left, 0);

        // Out of scope: a non-member's own post that admission rejects (any
        // authenticated DID may call).
        let (status, v) = call(OUTSIDER, 4, post_uri(OUTSIDER, "keep2")).await;
        assert_eq!((status, v["status"].as_str()), (200, Some("out_of_scope")), "{v}");

        // The two validation errors carry distinct messages (#232).
        let (status, v) = call(MEMBER, 3, post_uri(OUTSIDER, "keep3")).await;
        assert_eq!((status, v["error"].as_str(), v["message"].as_str()), (400, Some("InvalidRequest"), Some(NOT_CALLERS_REPO)));
        let (status, v) = call(MEMBER, 3, format!("at://{MEMBER}/com.example.unsupported/3k")).await;
        assert_eq!((status, v["error"].as_str(), v["message"].as_str()), (400, Some("InvalidRequest"), Some(UNSUPPORTED_COLLECTION)));
        let (status, _) = call(MEMBER, 3, "not-a-uri".into()).await;
        assert_eq!(status, 400);

        // 0.5/s, burst 30 per DID: the 31st immediate request is limited.
        let mut statuses = Vec::new();
        for _ in 0..31 {
            statuses.push(call(SPAMMER, 5, format!("at://{SPAMMER}/com.example.unsupported/3k")).await.0);
        }
        assert!(statuses[..30].iter().all(|s| *s == 400), "{statuses:?}");
        assert_eq!(statuses[30], 429);
        // Other DIDs have their own bucket.
        assert_eq!(call(MEMBER, 3, post_uri(MEMBER, "keep1")).await.0, 200);

        // Authentication is required.
        let r = client
            .post(format!("http://{addr}/xrpc/{REQUEST_RECORD}"))
            .json(&json!({ "uri": post_uri(MEMBER, "keep1") }))
            .send()
            .await
            .expect("request");
        assert_eq!(r.status().as_u16(), 401);
        db.drop_db().await;
    }
}
