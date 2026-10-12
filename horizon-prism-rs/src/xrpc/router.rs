//! Route registration (design-v2 §7.1), the global per-IP backstop
//! (§7.4: any request, 100/s, burst 200) and the final router (design-v3
//! §6.3, §6.5): fallbacks, then the rate limit, then CORS outermost.

use std::net::SocketAddr;
use std::time::Duration;

use axum::Json;
use axum::Router;
use axum::extract::{ConnectInfo, Request, State};
use axum::http::header::{ALLOW, AUTHORIZATION, CONTENT_TYPE, RETRY_AFTER};
use axum::http::{HeaderMap, HeaderName, HeaderValue, Method, StatusCode, Uri};
use axum::middleware::{self, Next};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use serde_json::json;
use tower_http::cors::{Any, CorsLayer};

use super::AppState;
use super::auth::request_ip;
use super::backstop::{self, Selected, Selection};
use super::pools::{ChargeKey, Exhausted, Pool};
use super::errors::{NotReadyHint, XrpcError};
use super::handlers::{actor, bookmarks, did_web, drafts, feed, graph, index, labels, mutes, notifications, repo, timeline};
use super::rate_limit::Class;

/// The health route: exempt from the backstop for `GET` and `HEAD` (§6.3;
/// `serve::health` charges it itself when a token is presented).
const HEALTH_PATH: &str = "/xrpc/_health";
/// Preflight cache lifetime (§6.5).
const CORS_MAX_AGE: Duration = Duration::from_secs(600);

/// Settings of the final router.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FinalConfig {
    /// `community.not_ready_retry_secs`.
    pub not_ready_retry_secs: u64,
}

/// Response extension set by the wrong-verb fallback. [`finish`] writes the
/// body once axum has added the route's `Allow` header.
#[derive(Debug, Clone)]
struct WrongVerb(Method);

async fn global_limit(
    State(s): State<AppState>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    request: Request,
    next: Next,
) -> Response {
    let exempt = request.uri().path() == HEALTH_PATH && matches!(*request.method(), Method::GET | Method::HEAD);
    if exempt {
        return next.run(request).await;
    }
    let ip = request_ip(&s, &headers, peer);
    // Who is charged (design-v3 §II-8.3): a ready member whose token
    // verifies against its in-memory key, by DID; everyone else by IP key.
    let selected = match s.limits.backstop() {
        Some(backstop) => {
            let authorization = headers.get(AUTHORIZATION).and_then(|v| v.to_str().ok());
            let method = request.uri().path().strip_prefix("/xrpc/");
            match backstop.select(s.auth.as_ref(), authorization, method, ip).await {
                Selection::Charge(selected) => selected,
                // The member's own ceiling is empty: neither key is charged.
                Selection::Limited { retry_after } => {
                    let exhausted = Exhausted {
                        pool: Pool::Member,
                        retry_after,
                    };
                    return XrpcError::from(exhausted).into_response();
                }
            }
        }
        None => Selected::by_ip(ip),
    };
    let charged = match selected.req.key() {
        ChargeKey::Member(did) => s.limits.check(Class::Global, did),
        ChargeKey::Ip(key) => s.limits.check_ip(Class::Global, *key),
    };
    match charged {
        Ok(()) => backstop::scope(selected, next.run(request)).await,
        Err(limited) => XrpcError::from(limited).into_response(),
    }
}

/// The verb a route expects, read from the `Allow` header axum adds to a
/// method-router fallback's response. Every route here is `GET` or `POST`.
fn expected_verb(allow: Option<&HeaderValue>, requested: &Method) -> &'static str {
    match allow.and_then(|v| v.to_str().ok()) {
        Some(a) if a.contains("GET") => "GET",
        Some(a) if a.contains("POST") => "POST",
        _ if matches!(*requested, Method::GET | Method::HEAD) => "POST",
        _ => "GET",
    }
}

/// Response fix-ups that need the whole router's view of a response: the
/// wrong-verb body (§6.3) and `Retry-After` on `NotReady` (§II-7.1).
async fn finish(State(cfg): State<FinalConfig>, request: Request, next: Next) -> Response {
    let mut response = next.run(request).await;
    if let Some(WrongVerb(requested)) = response.extensions().get::<WrongVerb>().cloned() {
        let allow = response.headers().get(ALLOW).cloned();
        let expected = expected_verb(allow.as_ref(), &requested);
        let mut fixed =
            XrpcError::invalid(format!("Incorrect HTTP method ({requested}) expected {expected}")).into_response();
        if let Some(allow) = allow {
            fixed.headers_mut().insert(ALLOW, allow);
        }
        return fixed;
    }
    if response.extensions().get::<NotReadyHint>().is_some() {
        response
            .headers_mut()
            .insert(RETRY_AFTER, HeaderValue::from(cfg.not_ready_retry_secs));
    }
    response
}

/// The catch-all (§6.3): an unregistered NSID is `501 MethodNotImplemented`,
/// a malformed one `400 InvalidRequest`, and a non-XRPC path a plain 404.
fn unrouted(path: &str) -> Response {
    let Some(rest) = path.strip_prefix("/xrpc") else {
        return StatusCode::NOT_FOUND.into_response();
    };
    let nsid = rest.strip_prefix('/').unwrap_or(rest);
    if rest.starts_with('/') && kryphocron_lexicons::Nsid::new(nsid).is_ok() {
        return (
            StatusCode::NOT_IMPLEMENTED,
            Json(json!({ "error": "MethodNotImplemented", "message": "Method not implemented" })),
        )
            .into_response();
    }
    if rest.is_empty() || rest.starts_with('/') {
        return XrpcError::invalid("Invalid NSID").into_response();
    }
    StatusCode::NOT_FOUND.into_response()
}

fn cors() -> CorsLayer {
    CorsLayer::new()
        .allow_origin(Any)
        .allow_methods([Method::GET, Method::POST])
        .allow_headers([
            AUTHORIZATION,
            CONTENT_TYPE,
            HeaderName::from_static("atproto-proxy"),
            HeaderName::from_static("atproto-accept-labelers"),
        ])
        .expose_headers([
            HeaderName::from_static("x-prism-sort-fallback"),
            HeaderName::from_static("ratelimit-reset"),
            RETRY_AFTER,
            HeaderName::from_static("atproto-content-labelers"),
        ])
        .max_age(CORS_MAX_AGE)
}

/// Build the final router from every merged route (§6.3's order): set the
/// two fallbacks, then the rate limit, then CORS outermost. Nothing may be
/// merged into the result, and no route is served from outside it (§6.5).
/// The caller serves it with
/// `into_make_service_with_connect_info::<SocketAddr>()`.
pub fn finalize(routes: Router, state: AppState, cfg: FinalConfig) -> Router {
    let (wrong, missing) = (state.clone(), state.clone());
    routes
        .method_not_allowed_fallback(
            move |ConnectInfo(peer): ConnectInfo<SocketAddr>, headers: HeaderMap, method: Method| {
                let s = wrong.clone();
                async move {
                    let ip = request_ip(&s, &headers, peer);
                    if let Err(limited) = s.limits.check_ip(Class::AnonRead, ip) {
                        return XrpcError::from(limited).into_response();
                    }
                    let mut response = StatusCode::BAD_REQUEST.into_response();
                    response.extensions_mut().insert(WrongVerb(method));
                    response
                }
            },
        )
        .fallback(
            move |ConnectInfo(peer): ConnectInfo<SocketAddr>, headers: HeaderMap, uri: Uri| {
                let s = missing.clone();
                async move {
                    let ip = request_ip(&s, &headers, peer);
                    match s.limits.check_ip(Class::AnonRead, ip) {
                        Ok(()) => unrouted(uri.path()),
                        Err(limited) => XrpcError::from(limited).into_response(),
                    }
                }
            },
        )
        .layer(middleware::from_fn_with_state(cfg, finish))
        .layer(middleware::from_fn_with_state(state, global_limit))
        .layer(cors())
}

fn xrpc(nsid: &str) -> String {
    format!("/xrpc/{nsid}")
}

/// The public routes, without fallbacks or layers: [`finalize`] adds those
/// once every router is merged.
pub fn router(state: AppState) -> Router {
    Router::new()
        .route(&xrpc(actor::GET_PROFILE), get(actor::get_profile))
        .route(&xrpc(actor::GET_PROFILES), get(actor::get_profiles))
        .route(&xrpc(actor::SEARCH_ACTORS), get(actor::search_actors))
        .route(&xrpc(feed::GET_POSTS), get(feed::get_posts))
        .route(&xrpc(feed::GET_POST_THREAD), get(feed::get_post_thread))
        .route(&xrpc(feed::GET_POST_THREAD_V2), get(feed::get_post_thread_v2))
        .route(&xrpc(feed::GET_POST_THREAD_OTHER_V2), get(feed::get_post_thread_other_v2))
        .route(&xrpc(feed::GET_AUTHOR_FEED), get(feed::get_author_feed))
        .route(&xrpc(feed::GET_LIKES), get(feed::get_likes))
        .route(&xrpc(feed::GET_REPOSTED_BY), get(feed::get_reposted_by))
        .route(&xrpc(feed::GET_QUOTES), get(feed::get_quotes))
        .route(&xrpc(timeline::GET_TIMELINE), get(timeline::get_timeline))
        .route(&xrpc(timeline::GET_LIST_FEED), get(timeline::get_list_feed))
        .route(&xrpc(graph::GET_FOLLOWS), get(graph::get_follows))
        .route(&xrpc(graph::GET_FOLLOWERS), get(graph::get_followers))
        .route(&xrpc(graph::GET_LIST), get(graph::get_list))
        .route(&xrpc(graph::GET_LISTS), get(graph::get_lists))
        .route(&xrpc(graph::GET_KNOWN_FOLLOWERS), get(graph::get_known_followers))
        .route(&xrpc(graph::GET_RELATIONSHIPS),get(graph::get_relationships))
        .route(&xrpc(graph::GET_LISTS_WITH_MEMBERSHIP),get(graph::get_lists_with_membership))
        .route(&xrpc(graph::GET_STARTER_PACKS_WITH_MEMBERSHIP), get(graph::get_starter_packs_with_membership))
        .route(&xrpc(graph::GET_STARTER_PACK), get(graph::get_starter_pack))
        .route(&xrpc(graph::GET_STARTER_PACKS), get(graph::get_starter_packs))
        .route(&xrpc(graph::GET_ACTOR_STARTER_PACKS), get(graph::get_actor_starter_packs))
        .route(&xrpc(mutes::GET_MUTES), get(mutes::get_mutes))
        .route(&xrpc(mutes::MUTE_ACTOR), post(mutes::mute_actor))
        .route(&xrpc(mutes::UNMUTE_ACTOR), post(mutes::unmute_actor))
        .route(&xrpc(mutes::MUTE_THREAD), post(mutes::mute_thread))
        .route(&xrpc(mutes::UNMUTE_THREAD), post(mutes::unmute_thread))
        .route(&xrpc(mutes::GET_BLOCKS), get(mutes::get_blocks))
        .route(&xrpc(notifications::LIST_NOTIFICATIONS), get(notifications::list_notifications))
        .route(&xrpc(notifications::GET_UNREAD_COUNT), get(notifications::get_unread_count))
        .route(&xrpc(notifications::UPDATE_SEEN), post(notifications::update_seen))
        .route(&xrpc(notifications::GET_PREFERENCES), get(notifications::get_preferences))
        .route(&xrpc(notifications::PUT_PREFERENCES), post(notifications::put_preferences))
        .route(&xrpc(notifications::PUT_PREFERENCES_V2), post(notifications::put_preferences_v2))
        .route(&xrpc(bookmarks::CREATE_BOOKMARK), post(bookmarks::create_bookmark))
        .route(&xrpc(bookmarks::DELETE_BOOKMARK), post(bookmarks::delete_bookmark))
        .route(&xrpc(bookmarks::GET_BOOKMARKS), get(bookmarks::get_bookmarks))
        .route(&xrpc(drafts::CREATE_DRAFT), post(drafts::create_draft))
        .route(&xrpc(drafts::UPDATE_DRAFT), post(drafts::update_draft))
        .route(&xrpc(drafts::DELETE_DRAFT), post(drafts::delete_draft))
        .route(&xrpc(drafts::GET_DRAFTS), get(drafts::get_drafts))
        .route(&xrpc(labels::QUERY_LABELS), get(labels::query_labels))
        .route(&xrpc(labels::GET_SERVICES), get(labels::get_services))
        .route(&xrpc(repo::GET_RECORD), get(repo::get_record))
        .route(&xrpc(feed::GET_ACTOR_LIKES), get(feed::get_actor_likes))
        .route(&xrpc(feed::GET_AUTHOR_POLLS), get(feed::get_author_polls))
        .route(&xrpc(notifications::LIST_ACTIVITY_SUBSCRIPTIONS), get(notifications::list_activity_subscriptions))
        .route(&xrpc(notifications::PUT_ACTIVITY_SUBSCRIPTION), post(notifications::put_activity_subscription))
        .route("/.well-known/did.json", get(did_web::did_json))
        .route("/did.json", get(did_web::did_json))
        .with_state(state)
}

/// The `index.requestRecord` route (§5.9), with its own state (the write
/// path and the PDS source); merged into the app by `serve`.
pub fn index_router(state: index::IndexState) -> Router {
    Router::new()
        .route(&xrpc(index::REQUEST_RECORD), post(index::request_record))
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
        let cfg = FinalConfig { not_ready_retry_secs: 7 };
        serve_app(finalize(router(state.clone()), state, cfg)).await
    }

    async fn serve_app(app: Router) -> String {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let addr = listener.local_addr().expect("addr");
        tokio::spawn(async move {
            axum::serve(listener, app.into_make_service_with_connect_info::<SocketAddr>())
                .await
                .expect("serve");
        });
        format!("http://{addr}")
    }

    /// A state with no members; the second value keeps the verifier's
    /// drainer alive for the test.
    fn bare_state(pool: &sqlx::PgPool) -> (AppState, impl Sized + use<>) {
        let (auth, drainer) = verifier(StubFetcher::default(), 10, 4);
        let state = AppState {
            hydrator: hydrator(pool, ScopeSets::default()),
            auth: Arc::new(auth),
            limits: RateLimits::new(),
            enroller: None,
            trusted_proxy: "172.30.0.10".parse().expect("ip"),
            did_document: Arc::new(json!({})),
        };
        (state, drainer)
    }

    /// B-29: unregistered NSIDs are 501, malformed ones 400, wrong verbs the
    /// xrpc-server 400, and only non-XRPC paths are a plain 404.
    #[tokio::test]
    async fn fallback_and_wrong_verb() {
        let Some(db) = fresh_db("xrpc_fallback").await else {
            return;
        };
        let (state, _drainer) = bare_state(&db.pool);
        let base = serve(state).await;
        let client = reqwest::Client::new();

        for nsid in [
            "com.atproto.server.createSession",
            "com.atproto.sync.getBlob",
            "app.example.doesNotExist",
        ] {
            let (status, v) = get_json(&client, &format!("{base}/xrpc/{nsid}"), None).await;
            assert_eq!((status, v["error"].as_str()), (501, Some("MethodNotImplemented")), "{nsid}");
            assert_eq!(v["message"], "Method not implemented");
        }
        for bad in ["/xrpc/1bad..nsid", "/xrpc/", "/xrpc"] {
            let (status, v) = get_json(&client, &format!("{base}{bad}"), None).await;
            assert_eq!((status, v["error"].as_str()), (400, Some("InvalidRequest")), "{bad}");
        }

        let r = client
            .post(format!("{base}/xrpc/app.bsky.feed.getPosts"))
            .send()
            .await
            .expect("request");
        assert_eq!(r.status().as_u16(), 400);
        let v: Value = r.json().await.expect("json");
        assert_eq!(v["error"], "InvalidRequest");
        assert_eq!(v["message"], "Incorrect HTTP method (POST) expected GET");
        let (status, v) = get_json(&client, &format!("{base}/xrpc/app.bsky.graph.muteActor"), None).await;
        assert_eq!(status, 400);
        assert_eq!(v["message"], "Incorrect HTTP method (GET) expected POST");

        // HEAD on a GET route is served by the GET handler.
        let r = client
            .head(format!("{base}/xrpc/app.bsky.actor.getProfile?actor={ALICE}"))
            .send()
            .await
            .expect("request");
        assert_ne!(r.status().as_u16(), 405);

        let r = client.get(format!("{base}/health")).send().await.expect("request");
        assert_eq!(r.status().as_u16(), 404);
        assert!(r.bytes().await.expect("body").is_empty());
        db.drop_db().await;
    }

    /// B-28: deferred endpoints answer an honest 501 from registered routes,
    /// whoever asks; a disabled custom service registers nothing.
    #[tokio::test]
    async fn honest_501_routes() {
        let Some(db) = fresh_db("xrpc_honest_501").await else {
            return;
        };
        let (state, _drainer) = bare_state(&db.pool);
        let cfg = crate::config::FileConfig::parse(concat!(
            "[honest_501]\nstatus_url = \"https://status.example\"\n",
            "[honest_501.messages]\n\"app.bsky.feed.getFeed\" = \"Feeds later.\"\n",
            "[services.eu]\nstate = \"disabled\"\n",
            "methods = { \"eu.example.region.getNotices\" = \"GET\" }\n",
        ))
        .expect("parse");
        // Merging panics on an overlapping route, so this also shows that no
        // deferral shadows a core handler (§6.6).
        let merged = router(state.clone()).merge(crate::honest_501::router(&state, &cfg));
        let base = serve_app(finalize(merged, state, FinalConfig { not_ready_retry_secs: 5 })).await;
        let client = reqwest::Client::new();
        let linked = "This endpoint is planned for a future release. See https://status.example for status.";

        let (status, v) = get_json(&client, &format!("{base}/xrpc/app.bsky.feed.getFeedGenerator"), None).await;
        assert_eq!(status, 501);
        assert_eq!(v, json!({"error": "NotImplemented", "message": linked}));
        // One override leaves every other message in place.
        let (status, v) = get_json(&client, &format!("{base}/xrpc/app.bsky.feed.getFeed"), None).await;
        assert_eq!((status, v["message"].as_str()), (501, Some("Feeds later.")));
        let (status, v) = get_json(&client, &format!("{base}/xrpc/app.bsky.actor.getPreferences"), None).await;
        assert_eq!(status, 501);
        assert!(v["message"].as_str().expect("message").starts_with("Preferences are stored by your PDS."));
        let (_, v) = get_json(&client, &format!("{base}/xrpc/app.bsky.ageassurance.getState"), None).await;
        assert_eq!(v["message"], "Age assurance is not provided by this AppView.");

        // No authentication: a garbage bearer gets the same bytes.
        let push = format!("{base}/xrpc/app.bsky.notification.registerPush");
        let anonymous = client.post(&push).send().await.expect("request");
        assert_eq!(anonymous.status().as_u16(), 501);
        let anonymous = anonymous.bytes().await.expect("body");
        let garbage = client
            .post(&push)
            .header("authorization", "Bearer garbage")
            .send()
            .await
            .expect("request");
        assert_eq!(garbage.status().as_u16(), 501);
        assert_eq!(garbage.bytes().await.expect("body"), anonymous);
        let v: Value = serde_json::from_slice(&anonymous).expect("json");
        assert_eq!(
            v["message"],
            "Push notifications are not available from this AppView yet. See https://status.example for status."
        );

        // The route is registered for its lexicon verb; preflights pass.
        let (status, v) = get_json(&client, &push, None).await;
        assert_eq!(status, 400);
        assert_eq!(v["message"], "Incorrect HTTP method (GET) expected POST");
        let r = client
            .request(reqwest::Method::OPTIONS, &push)
            .header("origin", "https://client.example")
            .header("access-control-request-method", "POST")
            .header("access-control-request-headers", "authorization")
            .send()
            .await
            .expect("preflight");
        assert_eq!(r.status().as_u16(), 200);

        // A disabled custom service leaves its NSID on the router fallback.
        let (status, v) = get_json(&client, &format!("{base}/xrpc/eu.example.region.getNotices"), None).await;
        assert_eq!((status, v["error"].as_str()), (501, Some("MethodNotImplemented")));
        db.drop_db().await;
    }

    /// B-31: preflights are answered by the CORS layer for every kind of
    /// route, and errors carry the access headers.
    #[tokio::test]
    async fn cors_preflight_and_error_headers() {
        let Some(db) = fresh_db("xrpc_cors").await else {
            return;
        };
        let (state, _drainer) = bare_state(&db.pool);
        let base = serve(state).await;
        let client = reqwest::Client::new();

        for (path, method) in [
            ("/xrpc/app.bsky.feed.getPosts", "GET"),
            ("/xrpc/app.bsky.graph.muteActor", "POST"),
            ("/xrpc/app.example.doesNotExist", "POST"),
        ] {
            let r = client
                .request(reqwest::Method::OPTIONS, format!("{base}{path}"))
                .header("origin", "https://client.example")
                .header("access-control-request-method", method)
                .header("access-control-request-headers", "authorization, content-type")
                .send()
                .await
                .expect("preflight");
            assert_eq!(r.status().as_u16(), 200, "{path}");
            let h = r.headers();
            assert_eq!(h["access-control-allow-origin"], "*", "{path}");
            let allowed = h["access-control-allow-headers"].to_str().expect("ascii").to_ascii_lowercase();
            assert!(allowed.contains("authorization") && allowed.contains("content-type"), "{allowed}");
            let methods = h["access-control-allow-methods"].to_str().expect("ascii");
            assert!(methods.contains("GET") && methods.contains("POST"), "{methods}");
            assert_eq!(h["access-control-max-age"], "600");
            assert!(h.get("access-control-allow-credentials").is_none());
        }

        // An error response is readable cross-origin: 401, then the 501.
        for (path, status) in [("/xrpc/app.bsky.feed.getTimeline", 401), ("/xrpc/app.example.doesNotExist", 501)] {
            let r = client
                .get(format!("{base}{path}"))
                .header("origin", "https://client.example")
                .send()
                .await
                .expect("request");
            assert_eq!(r.status().as_u16(), status, "{path}");
            let h = r.headers();
            assert_eq!(h["access-control-allow-origin"], "*", "{path}");
            let exposed = h["access-control-expose-headers"].to_str().expect("ascii").to_ascii_lowercase();
            for name in ["x-prism-sort-fallback", "ratelimit-reset", "retry-after", "atproto-content-labelers"] {
                assert!(exposed.contains(name), "{name} in {exposed}");
            }
            assert!(h.get("access-control-allow-credentials").is_none());
        }
        db.drop_db().await;
    }

    /// R3-90: `_health` is outside the backstop, and a burst of wrong-verb
    /// requests is limited.
    #[tokio::test]
    async fn backstop_exempts_health_and_limits_wrong_verbs() {
        let Some(db) = fresh_db("xrpc_backstop").await else {
            return;
        };
        let (state, _drainer) = bare_state(&db.pool);
        let health = Router::new().route(HEALTH_PATH, get(|| async { "ok" }));
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let addr = listener.local_addr().expect("addr");
        let app = finalize(router(state.clone()).merge(health), state, FinalConfig { not_ready_retry_secs: 5 });
        tokio::spawn(async move {
            axum::serve(listener, app.into_make_service_with_connect_info::<SocketAddr>())
                .await
                .expect("serve");
        });
        let base = format!("http://{addr}");
        let client = reqwest::Client::new();
        for _ in 0..300 {
            let r = client.get(format!("{base}{HEALTH_PATH}")).send().await.expect("request");
            assert_eq!(r.status().as_u16(), 200);
        }
        for _ in 0..300 {
            let r = client.head(format!("{base}{HEALTH_PATH}")).send().await.expect("request");
            assert_eq!(r.status().as_u16(), 200);
        }
        let mut limited = false;
        for _ in 0..300 {
            let r = client
                .post(format!("{base}/xrpc/app.bsky.feed.getPosts"))
                .send()
                .await
                .expect("request");
            if r.status().as_u16() == 429 {
                assert!(r.headers().get("ratelimit-reset").is_some());
                assert_eq!(r.headers()["access-control-allow-origin"], "*");
                limited = true;
                break;
            }
        }
        assert!(limited, "wrong-verb burst limited");
        db.drop_db().await;
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
        // The retry hint (§II-7.1), readable cross-origin.
        let r = client
            .get(format!("{base}/xrpc/app.bsky.feed.getTimeline"))
            .header("authorization", format!("Bearer {token}"))
            .header("origin", "https://client.example")
            .send()
            .await
            .expect("request");
        assert_eq!(r.status().as_u16(), 400);
        assert_eq!(r.headers()["retry-after"], "7");
        assert_eq!(r.headers()["access-control-allow-origin"], "*");
        exec(p, "UPDATE members SET ready_at = now()").await;
        let (status, v) = get_json(&client, &format!("{base}/xrpc/app.bsky.feed.getTimeline"), Some(&token)).await;
        assert_eq!(status, 200, "{v}");
        assert!(v["feed"].is_array());
        // G-5: listNotifications rejects seenAt. G-4: getUnreadCount reads it.
        let list = "app.bsky.notification.listNotifications";
        let list_token = mint(&signing_key(3), ALICE, &aud, Some(list));
        let (status, v) = get_json(&client, &format!("{base}/xrpc/{list}?seenAt=2026-01-01T00:00:00Z"), Some(&list_token)).await;
        assert_eq!((status, v["error"].as_str()), (400, Some("InvalidRequest")), "{v}");
        let (status, v) = get_json(&client, &format!("{base}/xrpc/{list}?priority=true"), Some(&list_token)).await;
        assert_eq!(status, 200, "{v}");
        let count = "app.bsky.notification.getUnreadCount";
        let count_token = mint(&signing_key(3), ALICE, &aud, Some(count));
        let (status, v) = get_json(&client, &format!("{base}/xrpc/{count}?seenAt=yesterday"), Some(&count_token)).await;
        assert_eq!((status, v["error"].as_str()), (400, Some("InvalidRequest")), "{v}");
        let (status, v) = get_json(&client, &format!("{base}/xrpc/{count}?seenAt=2099-01-01T00:00:00Z"), Some(&count_token)).await;
        assert_eq!((status, &v["count"]), (200, &json!(0)), "{v}");
        // Thread mutes and getBlocks are member endpoints.
        let mute = "app.bsky.graph.muteThread";
        let mute_token = mint(&signing_key(3), ALICE, &aud, Some(mute));
        for (body, status) in [(json!({"root": root}), 200), (json!({"root": "nope"}), 400), (json!({}), 400)] {
            let r = client
                .post(format!("{base}/xrpc/{mute}"))
                .header("authorization", format!("Bearer {mute_token}"))
                .json(&body)
                .send()
                .await
                .expect("request");
            assert_eq!(r.status().as_u16(), status, "{body}");
        }
        let r = client.post(format!("{base}/xrpc/{mute}")).json(&json!({"root": root})).send().await.expect("request");
        assert_eq!(r.status().as_u16(), 401);
        let blocks = "app.bsky.graph.getBlocks";
        let blocks_token = mint(&signing_key(3), ALICE, &aud, Some(blocks));
        let (status, v) = get_json(&client, &format!("{base}/xrpc/{blocks}"), Some(&blocks_token)).await;
        assert_eq!((status, &v["blocks"]), (200, &json!([])), "{v}");
        let (status, _) = get_json(&client, &format!("{base}/xrpc/{blocks}"), None).await;
        assert_eq!(status, 401);
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
        // B-30: both paths serve the same bytes, list both services, and
        // carry the CORS and cache headers (no `Origin` needed).
        let mut bodies = Vec::new();
        for path in ["/.well-known/did.json", "/did.json"] {
            let r = client.get(format!("{base}{path}")).send().await.expect("request");
            assert_eq!(r.status().as_u16(), 200, "{path}");
            assert_eq!(r.headers()["access-control-allow-origin"], "*", "{path}");
            assert_eq!(r.headers()["cache-control"], "public, max-age=300", "{path}");
            bodies.push(r.bytes().await.expect("body"));
        }
        assert_eq!(bodies[0], bodies[1]);
        let doc: Value = serde_json::from_slice(&bodies[0]).expect("json");
        let endpoint = format!("https://{AUDIENCE_HOST}");
        assert_eq!(
            doc["service"],
            json!([
                {"id": "#bsky_appview", "type": "BskyAppView", "serviceEndpoint": endpoint},
                {"id": "#bsky_notif", "type": "BskyNotificationService", "serviceEndpoint": endpoint},
            ])
        );
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
