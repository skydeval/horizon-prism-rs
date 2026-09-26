//! Route registration (design-v2 §7.1) and the global per-IP backstop
//! (§7.4: any request, 100/s, burst 200).

use std::net::SocketAddr;

use axum::Router;
use axum::extract::{ConnectInfo, Request, State};
use axum::http::HeaderMap;
use axum::middleware::{self, Next};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};

use super::AppState;
use super::auth::request_ip;
use super::errors::XrpcError;
use super::handlers::{actor, did_web, feed, graph, index, mutes, notifications, timeline};
use super::rate_limit::Class;

pub(crate) async fn global_limit(
    State(s): State<AppState>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    request: Request,
    next: Next,
) -> Response {
    let ip = request_ip(&s, &headers, peer);
    match s.limits.check_ip(Class::Global, ip) {
        Ok(()) => next.run(request).await,
        Err(limited) => XrpcError::from(limited).into_response(),
    }
}

fn xrpc(nsid: &str) -> String {
    format!("/xrpc/{nsid}")
}

/// The public router. The caller serves it with
/// `into_make_service_with_connect_info::<SocketAddr>()`.
pub fn router(state: AppState) -> Router {
    Router::new()
        .route(&xrpc(actor::GET_PROFILE), get(actor::get_profile))
        .route(&xrpc(actor::GET_PROFILES), get(actor::get_profiles))
        .route(&xrpc(actor::SEARCH_ACTORS), get(actor::search_actors))
        .route(&xrpc(feed::GET_POSTS), get(feed::get_posts))
        .route(&xrpc(feed::GET_POST_THREAD), get(feed::get_post_thread))
        .route(&xrpc(feed::GET_AUTHOR_FEED), get(feed::get_author_feed))
        .route(&xrpc(feed::GET_LIKES), get(feed::get_likes))
        .route(&xrpc(feed::GET_REPOSTED_BY), get(feed::get_reposted_by))
        .route(&xrpc(feed::GET_QUOTES), get(feed::get_quotes))
        .route(&xrpc(timeline::GET_TIMELINE), get(timeline::get_timeline))
        .route(&xrpc(graph::GET_FOLLOWS), get(graph::get_follows))
        .route(&xrpc(graph::GET_FOLLOWERS), get(graph::get_followers))
        .route(&xrpc(mutes::GET_MUTES), get(mutes::get_mutes))
        .route(&xrpc(mutes::MUTE_ACTOR), post(mutes::mute_actor))
        .route(&xrpc(mutes::UNMUTE_ACTOR), post(mutes::unmute_actor))
        .route(&xrpc(notifications::LIST_NOTIFICATIONS), get(notifications::list_notifications))
        .route(&xrpc(notifications::GET_UNREAD_COUNT), get(notifications::get_unread_count))
        .route(&xrpc(notifications::UPDATE_SEEN), post(notifications::update_seen))
        .route("/.well-known/did.json", get(did_web::did_json))
        .layer(middleware::from_fn_with_state(state.clone(), global_limit))
        .with_state(state)
}

/// The `index.requestRecord` route (§5.9), with its own state (the write
/// path and the PDS source); merged into the app by `serve`.
pub fn index_router(state: index::IndexState) -> Router {
    Router::new()
        .route(&xrpc(index::REQUEST_RECORD), post(index::request_record))
        .layer(middleware::from_fn_with_state(state.app.clone(), global_limit))
        .with_state(state)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use serde_json::{Value, json};
    use tokio::net::TcpListener;

    use super::*;
    use crate::aggregations::ScopeSets;
    use crate::hydration::testing::{actor, exec, hydrator, post};
    use crate::kryphocron_glue::ServiceKey;
    use crate::kryphocron_glue::jwt::tests::{AUDIENCE_HOST, StubFetcher, mint, signing_key, verifier};
    use crate::test_support::fresh_db;
    use crate::xrpc::RateLimits;

    const ALICE: &str = "did:plc:aaaaaaaaaaaaaaaaaaaaaaaa";
    const BOB: &str = "did:plc:bbbbbbbbbbbbbbbbbbbbbbbb";

    async fn serve(state: AppState) -> String {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let addr = listener.local_addr().expect("addr");
        tokio::spawn(async move {
            axum::serve(listener, router(state).into_make_service_with_connect_info::<SocketAddr>())
                .await
                .expect("serve");
        });
        format!("http://{addr}")
    }

    async fn get_json(client: &reqwest::Client, url: &str, token: Option<&str>) -> (u16, Value) {
        let mut req = client.get(url);
        if let Some(t) = token {
            req = req.header("authorization", format!("Bearer {t}"));
        }
        let r = req.send().await.expect("request");
        let status = r.status().as_u16();
        (status, r.json().await.unwrap_or(Value::Null))
    }

    #[tokio::test]
    async fn xrpc_round_trips_auth_gates_and_did_json() {
        let Some(db) = fresh_db("xrpc_round_trips").await else {
            return;
        };
        let p = &db.pool;
        actor(p, ALICE, "alice.test", "Alice").await;
        actor(p, BOB, "bob.test", "Bob").await;
        let root = post(p, BOB, "3kaaaaaaaaaa2", "hello", None, None).await;
        let mut sets = ScopeSets::default();
        for d in [ALICE, BOB] {
            sets.scope.insert(d.into());
        }
        sets.members.insert(ALICE.into());
        let fetcher = StubFetcher::default();
        fetcher.set_key(ALICE, signing_key(3));
        let (auth, _drainer) = verifier(fetcher, 100, 16);
        let key = ServiceKey::new(AUDIENCE_HOST, &[7u8; 32]).expect("key");
        let state = AppState {
            hydrator: hydrator(p, sets),
            auth: Arc::new(auth),
            limits: RateLimits::new(),
            enroller: None,
            trusted_proxy: "172.30.0.10".parse().expect("ip"),
            did_document: Arc::new(key.did_document()),
        };
        let base = serve(state).await;
        let client = reqwest::Client::new();
        let aud = format!("did:web:{AUDIENCE_HOST}");

        // Anonymous profile read.
        let (status, v) = get_json(&client, &format!("{base}/xrpc/app.bsky.actor.getProfile?actor=bob.test"), None).await;
        assert_eq!(status, 200, "{v}");
        assert_eq!(v["did"], BOB);
        assert_eq!(v["viewer"], json!({"muted": false, "blockedBy": false}));

        // getPosts with repeated params.
        let (status, v) = get_json(&client, &format!("{base}/xrpc/app.bsky.feed.getPosts?uris={root}&uris={root}"), None).await;
        assert_eq!(status, 200);
        assert_eq!(v["posts"].as_array().map(Vec::len), Some(1));
        assert_eq!(v["posts"][0]["record"]["text"], "hello");

        // Lexicon errors and missing params.
        let (status, v) = get_json(&client, &format!("{base}/xrpc/app.bsky.feed.getPostThread?uri=at://{BOB}/app.bsky.feed.post/nope"), None).await;
        assert_eq!((status, v["error"].as_str()), (400, Some("NotFound")));
        let (status, v) = get_json(&client, &format!("{base}/xrpc/app.bsky.actor.getProfile"), None).await;
        assert_eq!((status, v["error"].as_str()), (400, Some("InvalidRequest")));

        // Member endpoints: 401 without auth, InvalidToken with a bad token,
        // NotReady before readiness, 200 after.
        let (status, v) = get_json(&client, &format!("{base}/xrpc/app.bsky.feed.getTimeline"), None).await;
        assert_eq!((status, v["error"].as_str()), (401, Some("AuthRequired")));
        let (status, v) = get_json(&client, &format!("{base}/xrpc/app.bsky.feed.getTimeline"), Some("garbage")).await;
        assert_eq!((status, v["error"].as_str()), (401, Some("InvalidToken")));
        let token = mint(&signing_key(3), ALICE, &aud, Some("app.bsky.feed.getTimeline"));
        exec(p, "INSERT INTO members (did, source) VALUES ('did:plc:aaaaaaaaaaaaaaaaaaaaaaaa', 'admin')").await;
        let (status, v) = get_json(&client, &format!("{base}/xrpc/app.bsky.feed.getTimeline"), Some(&token)).await;
        assert_eq!((status, v["error"].as_str()), (400, Some("NotReady")));
        exec(p, "UPDATE members SET ready_at = now()").await;
        let (status, v) = get_json(&client, &format!("{base}/xrpc/app.bsky.feed.getTimeline"), Some(&token)).await;
        assert_eq!(status, 200, "{v}");
        assert!(v["feed"].is_array());
        // A token minted for another method is rejected (lxm).
        let (status, _) = get_json(&client, &format!("{base}/xrpc/app.bsky.notification.getUnreadCount"), Some(&token)).await;
        assert_eq!(status, 401);

        // A non-member with a valid token: NotEnrolled.
        exec(p, "DELETE FROM members").await;
        let (status, v) = get_json(&client, &format!("{base}/xrpc/app.bsky.feed.getTimeline"), Some(&token)).await;
        assert_eq!((status, v["error"].as_str()), (400, Some("NotEnrolled")));

        // did.json with the Multikey.
        let (status, v) = get_json(&client, &format!("{base}/.well-known/did.json"), None).await;
        assert_eq!(status, 200);
        assert_eq!(v["id"], aud);
        assert_eq!(v["verificationMethod"][0]["publicKeyMultibase"], json!(key_multibase()));
        db.drop_db().await;
    }

    fn key_multibase() -> String {
        ServiceKey::new(AUDIENCE_HOST, &[7u8; 32]).expect("key").public_key_multibase()
    }

    #[tokio::test]
    async fn anonymous_reads_are_limited_per_ip() {
        let Some(db) = fresh_db("xrpc_rate_limit").await else {
            return;
        };
        let (auth, _d) = verifier(StubFetcher::default(), 10, 4);
        let state = AppState {
            hydrator: hydrator(&db.pool, ScopeSets::default()),
            auth: Arc::new(auth),
            limits: RateLimits::new(),
            enroller: None,
            trusted_proxy: "172.30.0.10".parse().expect("ip"),
            did_document: Arc::new(json!({})),
        };
        let base = serve(state).await;
        let client = reqwest::Client::new();
        let url = format!("{base}/xrpc/app.bsky.actor.getProfile?actor={ALICE}");
        let mut limited = None;
        for i in 0..80 {
            let r = client.get(&url).send().await.expect("request");
            if r.status().as_u16() == 429 {
                let reset = r.headers().get("ratelimit-reset").is_some();
                let body: Value = r.json().await.expect("json");
                limited = Some((i, body["error"].clone(), reset));
                break;
            }
        }
        let (i, error, reset) = limited.expect("limited within 80 requests");
        assert!(i >= 60, "burst of 60 honoured, limited at {i}");
        assert_eq!(error, "RateLimitExceeded");
        assert!(reset);
        db.drop_db().await;
    }
}
