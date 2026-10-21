//! The proxy behind the final router, against a fake target on loopback
//! (design-v3 §6.7; B-67 to B-72). Kept out of `proxy.rs`, which must hold
//! no database or hydration code.

use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use axum::Router;
use axum::extract::{Request, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use serde_json::{Value, json};
use tokio::net::TcpListener;

use super::proxy;
use crate::aggregations::ScopeSets;
use crate::config::FileConfig;
use crate::hydration::testing::hydrator;
use crate::kryphocron_glue::jwt::IssuerDrainer;
use crate::kryphocron_glue::jwt::tests::{AUDIENCE_HOST, StubFetcher, mint, signing_key, verifier};
use crate::kryphocron_glue::service_key::multikey;
use crate::test_support::{TestDb, fresh_db, serve_fake};
use crate::xrpc::backstop::{Backstop, VerifyLimits};
use crate::xrpc::member_keys::MemberKeys;
use crate::xrpc::pools::Shares;
use crate::xrpc::router::{FinalConfig, finalize};
use crate::xrpc::{AppState, RateLimits};

const ALICE: &str = "did:plc:aaaaaaaaaaaaaaaaaaaaaaaa";
const MEMBER: &str = "did:plc:bbbbbbbbbbbbbbbbbbbbbbbb";
const JSON: &str = "application/json; charset=utf-8";

/// One request the fake target received.
#[derive(Debug, Clone)]
struct Seen {
    method: String,
    /// Path and query, as received.
    target: String,
    headers: HeaderMap,
    body: Vec<u8>,
}

type Log = Arc<Mutex<Vec<Seen>>>;

/// The fake target: records every request and answers by method name.
async fn target(State(log): State<Log>, request: Request) -> Response {
    let (parts, body) = request.into_parts();
    let body = axum::body::to_bytes(body, 1 << 24).await.expect("body").to_vec();
    let path = parts.uri.path().to_string();
    log.lock().expect("log").push(Seen {
        method: parts.method.to_string(),
        target: parts.uri.path_and_query().map(ToString::to_string).unwrap_or_default(),
        headers: parts.headers,
        body,
    });
    match path.rsplit('.').next().unwrap_or_default() {
        "getMatches" => (
            [
                ("content-type", JSON),
                ("atproto-content-labelers", "did:plc:labeler"),
                ("set-cookie", "a=b"),
                ("ratelimit-reset", "99"),
            ],
            r#"{"matches": []}"#,
        )
            .into_response(),
        "dismissMatch" => StatusCode::OK.into_response(),
        "getSyncStatus" => (StatusCode::FOUND, [("location", "https://elsewhere.example/x")]).into_response(),
        "removeData" => ([("content-type", "text/html")], "<html>").into_response(),
        "verifyPhone" | "wait" => {
            tokio::time::sleep(Duration::from_millis(600)).await;
            ([("content-type", JSON)], "{}").into_response()
        }
        "startPhoneVerification" => (
            StatusCode::UNAUTHORIZED,
            [("content-type", JSON)],
            r#"{"error":"AuthRequired","message":"the target says no"}"#,
        )
            .into_response(),
        "big" => ([("content-type", JSON)], format!("\"{}\"", "x".repeat(2000))).into_response(),
        _ => ([("content-type", JSON)], "{}").into_response(),
    }
}

struct Harness {
    base: String,
    log: Log,
    db: TestDb,
    fetcher: StubFetcher,
    _drainer: IssuerDrainer<StubFetcher>,
}

impl Harness {
    /// The requests the target saw for method `name`.
    fn seen(&self, name: &str) -> Vec<Seen> {
        let log = self.log.lock().expect("log");
        log.iter().filter(|s| s.target.contains(name)).cloned().collect()
    }

    fn url(&self, nsid: &str) -> String {
        format!("{}/xrpc/{nsid}", self.base)
    }
}

/// Serve the app with the proxy routes of `toml`, in which `{target}` is the
/// fake target's URL.
async fn start(test: &str, toml: &str, backstop: Option<Backstop>) -> Option<Harness> {
    let db = fresh_db(test).await?;
    let log = Log::default();
    let target_url = serve_fake(Router::new().fallback(target).with_state(Arc::clone(&log))).await;
    let cfg = FileConfig::parse(&toml.replace("{target}", &target_url)).expect("parse");
    let mut problems = Vec::new();
    super::problems(&cfg, AUDIENCE_HOST, &mut problems);
    assert_eq!(problems, Vec::<String>::new());

    let fetcher = StubFetcher::default();
    fetcher.set_key(ALICE, signing_key(3));
    let (auth, drainer) = verifier(fetcher.clone(), 100, 16);
    let mut limits = RateLimits::from_config(&cfg.rate_limits);
    if let Some(backstop) = backstop {
        limits = limits.with_backstop(backstop);
    }
    let state = AppState {
        hydrator: hydrator(&db.pool, ScopeSets::default()),
        auth: Arc::new(auth),
        limits,
        enroller: None,
        trusted_proxy: "172.30.0.10".parse().expect("ip"),
        did_document: Arc::new(json!({})),
        membership: Default::default(),
    };
    let routes = crate::xrpc::router(state.clone())
        .merge(crate::honest_501::router(&state, &cfg))
        .merge(proxy::router(&state, &cfg, &Shares::for_tests(), AUDIENCE_HOST).expect("proxy routes"));
    let app = finalize(routes, state, FinalConfig { not_ready_retry_secs: 5, public: crate::honest_501::PublicMessages::from_config(&cfg) });
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().expect("addr");
    tokio::spawn(async move {
        axum::serve(listener, app.into_make_service_with_connect_info::<SocketAddr>())
            .await
            .expect("serve");
    });
    Some(Harness {
        base: format!("http://{addr}"),
        log,
        db,
        fetcher,
        _drainer: drainer,
    })
}

fn aud() -> String {
    format!("did:web:{AUDIENCE_HOST}")
}

fn bearer(key_seed: u8, iss: &str, aud: &str, lxm: &str) -> String {
    format!("Bearer {}", mint(&signing_key(key_seed), iss, aud, Some(lxm)))
}

async fn error_of(r: reqwest::Response) -> (u16, String) {
    let status = r.status().as_u16();
    let v: Value = r.json().await.unwrap_or(Value::Null);
    (status, v["error"].as_str().unwrap_or_default().to_string())
}

const ROOMY: &str = concat!(
    "[rate_limits.service_proxy_ip]\nper_sec = 100.0\nburst = 500\n",
    "[services.contact]\nstate = \"proxy\"\nproxy_url = \"{target}\"\nallow_private_network = true\n",
    "timeout_ms = 300\nmax_request_bytes = 1024\nmax_response_bytes = 1024\nrate_per_sec = 500\nburst = 1000\n",
    "[services.eu]\nstate = \"proxy\"\nproxy_url = \"{target}\"\nallow_private_network = true\n",
    "aud_fragments = [\"eu_notices\"]\nmax_response_bytes = 1024\nrate_per_sec = 500\nburst = 1000\n",
    "methods = { \"eu.example.region.getNotices\" = \"GET\", \"eu.example.region.big\" = \"GET\" }\n",
);

/// B-67, B-68: what is forwarded, and what comes back.
#[tokio::test]
async fn requests_are_forwarded_with_the_allowlist_and_answers_relayed() {
    let Some(h) = start("proxy_forward", ROOMY, None).await else {
        return;
    };
    let client = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .expect("client");

    // An anonymous GET: the query is copied as it came, and only the
    // allowlisted headers travel.
    let query = "q=a%20b&cursor=x%2Fy&q=2";
    let r = client
        .get(format!("{}?{query}", h.url("app.bsky.contact.getMatches")))
        .header("accept-language", "de-DE")
        .header("atproto-accept-labelers", "did:plc:labeler")
        .header("atproto-proxy", "did:web:elsewhere.example#bsky_appview")
        .header("cookie", "session=1")
        .header("x-forwarded-for", "198.51.100.9")
        .header("x-custom", "1")
        .header("user-agent", "client/1")
        .header("origin", "https://client.example")
        .send()
        .await
        .expect("request");
    assert_eq!(r.status().as_u16(), 200);
    let headers = r.headers().clone();
    assert_eq!(headers["content-type"], JSON);
    assert_eq!(headers["atproto-content-labelers"], "did:plc:labeler");
    assert_eq!(headers["x-content-type-options"], "nosniff");
    assert_eq!(headers["access-control-allow-origin"], "*");
    assert!(headers.get("set-cookie").is_none() && headers.get("ratelimit-reset").is_none());
    assert_eq!(r.bytes().await.expect("body").as_ref(), br#"{"matches": []}"#);
    let seen = h.seen("getMatches");
    assert_eq!(seen.len(), 1);
    assert_eq!(seen[0].method, "GET");
    assert_eq!(seen[0].target, format!("/xrpc/app.bsky.contact.getMatches?{query}"));
    let sent = &seen[0].headers;
    assert_eq!(sent["accept-language"], "de-DE");
    assert_eq!(sent["atproto-accept-labelers"], "did:plc:labeler");
    assert!(sent["user-agent"].to_str().expect("ascii").starts_with("horizon-prism-rs/"));
    for dropped in ["authorization", "atproto-proxy", "cookie", "x-forwarded-for", "x-custom", "origin"] {
        assert!(sent.get(dropped).is_none(), "{dropped} was forwarded");
    }

    // A verified token is forwarded unchanged, with the body's bytes.
    let import = "app.bsky.contact.importContacts";
    let token = bearer(3, ALICE, &aud(), import);
    let body = br#"{"contacts":  ["+15550100"]}"#;
    let r = client
        .post(h.url(import))
        .header("authorization", &token)
        .header("content-type", "application/json")
        .body(body.to_vec())
        .send()
        .await
        .expect("request");
    assert_eq!(r.status().as_u16(), 200);
    let seen = h.seen("importContacts");
    assert_eq!(seen.len(), 1);
    assert_eq!((seen[0].method.as_str(), seen[0].body.as_slice()), ("POST", body.as_slice()));
    assert_eq!(seen[0].headers["authorization"].to_str().expect("ascii"), token);
    assert_eq!(seen[0].headers["content-type"], "application/json");
    // `#bsky_appview` is contact's fragment.
    let named = bearer(3, ALICE, &format!("{}#bsky_appview", aud()), import);
    let r = client.post(h.url(import)).header("authorization", &named).send().await.expect("request");
    assert_eq!(r.status().as_u16(), 200);
    assert_eq!(h.seen("importContacts").len(), 2);

    // Credentials that do not verify are answered here, never forwarded.
    for bad in [
        "Bearer garbage".to_string(),
        bearer(9, ALICE, &aud(), import),
        bearer(3, ALICE, &aud(), "app.bsky.contact.getMatches"),
        bearer(3, ALICE, &format!("{}#bsky_notif", aud()), import),
        bearer(3, ALICE, "did:web:elsewhere.example", import),
    ] {
        let r = client.post(h.url(import)).header("authorization", &bad).send().await.expect("request");
        assert_eq!(error_of(r).await, (401, "InvalidToken".to_string()), "{bad}");
    }
    assert_eq!(h.seen("importContacts").len(), 2);

    // A body over the cap is refused before the target hears of it.
    let r = client.post(h.url(import)).body(vec![b'x'; 1025]).send().await.expect("request");
    assert_eq!(error_of(r).await, (413, "PayloadTooLarge".to_string()));
    assert_eq!(h.seen("importContacts").len(), 2);

    // An empty answer is relayed with no content type.
    let r = client.post(h.url("app.bsky.contact.dismissMatch")).send().await.expect("request");
    assert_eq!(r.status().as_u16(), 200);
    assert!(r.headers().get("content-type").is_none());
    assert!(r.bytes().await.expect("body").is_empty());

    // The target's own error is its honest answer.
    let r = client.post(h.url("app.bsky.contact.startPhoneVerification")).send().await.expect("request");
    assert_eq!(r.status().as_u16(), 401);
    let v: Value = r.json().await.expect("json");
    assert_eq!(v["message"], "the target says no");

    // A redirect, another content type and an oversized answer are 502s; a
    // slow target is a 504. None names the target.
    let r = client.get(h.url("app.bsky.contact.getSyncStatus")).send().await.expect("request");
    assert!(r.headers().get("location").is_none());
    assert_eq!(error_of(r).await, (502, "UpstreamFailure".to_string()));
    let r = client.post(h.url("app.bsky.contact.removeData")).send().await.expect("request");
    assert_eq!(error_of(r).await, (502, "UpstreamFailure".to_string()));
    let r = client.get(h.url("eu.example.region.big")).send().await.expect("request");
    assert_eq!(error_of(r).await, (502, "UpstreamFailure".to_string()));
    let r = client.post(h.url("app.bsky.contact.verifyPhone")).send().await.expect("request");
    let text = r.text().await.expect("body");
    assert!(text.contains("UpstreamTimeout") && !text.contains("127.0.0.1"), "{text}");

    // The inbound callback keeps its honest 501, and verbs are the routes'.
    let r = client.post(h.url("app.bsky.contact.sendNotification")).send().await.expect("request");
    assert_eq!(error_of(r).await, (501, "NotImplemented".to_string()));
    assert!(h.seen("sendNotification").is_empty());
    let r = client.get(h.url(import)).send().await.expect("request");
    assert_eq!(error_of(r).await, (400, "InvalidRequest".to_string()));
    // Age assurance is still disabled here.
    let r = client.get(h.url("app.bsky.ageassurance.getConfig")).send().await.expect("request");
    assert_eq!(error_of(r).await, (501, "NotImplemented".to_string()));

    // B-69: a custom service takes its own fragments, and only those.
    let notices = "eu.example.region.getNotices";
    for (aud, status) in [
        (aud(), 200),
        (format!("{}#eu_notices", aud()), 200),
        (format!("{}#bsky_appview", aud()), 401),
    ] {
        let r = client
            .get(h.url(notices))
            .header("authorization", bearer(3, ALICE, &aud, notices))
            .send()
            .await
            .expect("request");
        assert_eq!(r.status().as_u16(), status, "{aud}");
    }
    assert_eq!(h.seen("getNotices").len(), 2);
    h.db.drop_db().await;
}

/// B-70: the class is per service, the target's bucket bounds what it
/// sees, and `max_in_flight` is not queued.
#[tokio::test]
async fn classes_buckets_and_permits() {
    let toml = concat!(
        "[rate_limits.service_proxy_ip]\nper_sec = 0.001\nburst = 3\n",
        // With half the bucket for non-members: one token.
        "[services.contact]\nstate = \"proxy\"\nproxy_url = \"{target}\"\nallow_private_network = true\n",
        "rate_per_sec = 1\nburst = 2\n",
        "[services.age_assurance]\nstate = \"proxy\"\nproxy_url = \"{target}\"\nallow_private_network = true\n",
        "[services.slow]\nstate = \"proxy\"\nproxy_url = \"{target}\"\nallow_private_network = true\n",
        "max_in_flight = 1\nmethods = { \"eu.example.slow.wait\" = \"GET\" }\n",
    );
    let Some(h) = start("proxy_limits", toml, None).await else {
        return;
    };
    let client = reqwest::Client::new();

    // The shared bucket: the second call is refused and never forwarded.
    let matches = h.url("app.bsky.contact.getMatches");
    assert_eq!(client.get(&matches).send().await.expect("request").status().as_u16(), 200);
    let r = client.get(&matches).send().await.expect("request");
    let (_, retry, reset) = limited(&r);
    crate::xrpc::errors::assert_retry_after(retry.as_deref(), reset.as_deref());
    assert_eq!(error_of(r).await, (429, "RateLimitExceeded".to_string()));
    assert_eq!(h.seen("getMatches").len(), 1);

    // Another service has its own class bucket and its own target bucket.
    let config = h.url("app.bsky.ageassurance.getConfig");
    for _ in 0..3 {
        assert_eq!(client.get(&config).send().await.expect("request").status().as_u16(), 200);
    }
    let r = client.get(&config).send().await.expect("request");
    let (_, retry, reset) = limited(&r);
    crate::xrpc::errors::assert_retry_after(retry.as_deref(), reset.as_deref());
    assert_eq!(error_of(r).await, (429, "RateLimitExceeded".to_string()));
    assert_eq!(h.seen("getConfig").len(), 3);

    // One permit: the second concurrent call is a 503, not a wait.
    let wait = h.url("eu.example.slow.wait");
    let (a, b) = tokio::join!(client.get(&wait).send(), async {
        tokio::time::sleep(Duration::from_millis(100)).await;
        client.get(&wait).send().await
    });
    assert_eq!(a.expect("request").status().as_u16(), 200);
    assert_eq!(error_of(b.expect("request")).await, (503, "NotEnoughResources".to_string()));
    assert_eq!(h.seen("wait").len(), 1);
    h.db.drop_db().await;
}

/// Status, `Retry-After` and `ratelimit-reset` of one answer.
fn limited(r: &reqwest::Response) -> (u16, Option<String>, Option<String>) {
    let get = |name: &str| r.headers().get(name).map(|v| v.to_str().expect("ascii").to_string());
    (r.status().as_u16(), get("retry-after"), get("ratelimit-reset"))
}

/// Every 429 names `Retry-After` from its reset: the global backstop's
/// class and the backstop's per-member verification ceiling (a proxied
/// service's class: [`classes_buckets_and_permits`]).
#[tokio::test]
async fn the_backstops_429s_carry_retry_after() {
    // The global class: two requests from this address, then 429.
    let toml = format!("{ROOMY}[rate_limits.global]\nper_sec = 0.001\nburst = 2\n");
    let Some(h) = start("backstop_retry_after", &toml, None).await else {
        return;
    };
    let client = reqwest::Client::new();
    let matches = h.url("app.bsky.contact.getMatches");
    for _ in 0..2 {
        assert_eq!(client.get(&matches).send().await.expect("request").status().as_u16(), 200);
    }
    let r = client.get(&matches).send().await.expect("request");
    let (status, retry, reset) = limited(&r);
    assert_eq!(status, 429);
    crate::xrpc::errors::assert_retry_after(retry.as_deref(), reset.as_deref());
    assert_eq!(error_of(r).await, (429, "RateLimitExceeded".to_string()));
    h.db.drop_db().await;

    // The per-member verification ceiling: one verification, then 429
    // before any verification, charged to neither key.
    let toml = concat!(
        "[auth]\nmember_verify_rate_per_sec = 0.001\nmember_verify_burst = 1\n",
        "[services.contact]\nstate = \"proxy\"\nproxy_url = \"{target}\"\nallow_private_network = true\n",
    );
    let key = signing_key(5);
    let compressed = key.verifying_key().to_encoded_point(true).as_bytes().to_vec();
    let doc = json!({"id": MEMBER, "verificationMethod": [{
        "id": format!("{MEMBER}#atproto"), "type": "Multikey", "controller": MEMBER,
        "publicKeyMultibase": multikey(&compressed),
    }]});
    let cfg = FileConfig::parse(toml.replace("{target}", "http://127.0.0.1:1").as_str()).expect("parse");
    let backstop = Backstop {
        limits: VerifyLimits::from_config(&cfg.auth),
        keys: MemberKeys::default(),
    };
    backstop.keys.insert(MEMBER, &doc, chrono::Utc::now());
    let Some(h) = start("backstop_member_retry_after", toml, Some(backstop)).await else {
        return;
    };
    let token = bearer(5, MEMBER, &aud(), "app.bsky.contact.getMatches");
    let matches = h.url("app.bsky.contact.getMatches");
    let r = client.get(&matches).header("authorization", &token).send().await.expect("request");
    assert_eq!(r.status().as_u16(), 200);
    let r = client.get(&matches).header("authorization", &token).send().await.expect("request");
    let (status, retry, reset) = limited(&r);
    assert_eq!(status, 429);
    crate::xrpc::errors::assert_retry_after(retry.as_deref(), reset.as_deref());
    assert_eq!(h.seen("getMatches").len(), 1, "the refused call was not forwarded");
    h.db.drop_db().await;
}

/// B-71: a ready member is charged by DID from the backstop's key, is not
/// verified twice, and still meets the service's fragment allowlist.
#[tokio::test]
async fn a_member_is_keyed_by_did_and_meets_the_allowlist() {
    let toml = concat!(
        "[rate_limits.service_proxy_member]\nper_sec = 0.001\nburst = 1\n",
        "[services.contact]\nstate = \"proxy\"\nproxy_url = \"{target}\"\nallow_private_network = true\n",
        "[services.eu]\nstate = \"proxy\"\nproxy_url = \"{target}\"\nallow_private_network = true\n",
        "aud_fragments = [\"eu_notices\"]\nmethods = { \"eu.example.region.getNotices\" = \"GET\" }\n",
    );
    let key = signing_key(5);
    let compressed = key.verifying_key().to_encoded_point(true).as_bytes().to_vec();
    let doc = json!({"id": MEMBER, "verificationMethod": [{
        "id": format!("{MEMBER}#atproto"), "type": "Multikey", "controller": MEMBER,
        "publicKeyMultibase": multikey(&compressed),
    }]});
    let backstop = Backstop {
        limits: VerifyLimits::from_config(&FileConfig::default().auth),
        keys: MemberKeys::default(),
    };
    backstop.keys.insert(MEMBER, &doc, chrono::Utc::now());
    let Some(h) = start("proxy_member", toml, Some(backstop)).await else {
        return;
    };
    let client = reqwest::Client::new();

    // The member's PDS-proxied call: verified by the backstop against the
    // key in memory. The verifier's fetcher does not hold that key, so a
    // second verification would fail.
    let matches = "app.bsky.contact.getMatches";
    let token = bearer(5, MEMBER, &aud(), matches);
    let r = client.get(h.url(matches)).header("authorization", &token).send().await.expect("request");
    assert_eq!(r.status().as_u16(), 200);
    assert_eq!(h.fetcher.calls.load(std::sync::atomic::Ordering::SeqCst), 0);
    assert_eq!(h.seen("getMatches")[0].headers["authorization"].to_str().expect("ascii"), token);

    // Its class bucket is its own: one token here, and the IP key's bucket
    // is untouched by it.
    let r = client.get(h.url(matches)).header("authorization", &token).send().await.expect("request");
    assert_eq!(error_of(r).await, (429, "RateLimitExceeded".to_string()));
    assert_eq!(client.get(h.url(matches)).send().await.expect("request").status().as_u16(), 200);
    assert_eq!(h.seen("getMatches").len(), 2);

    // A token the backstop accepts (`#bsky_appview`) on a route that does
    // not take that fragment: refused, and not forwarded.
    let notices = "eu.example.region.getNotices";
    let named = bearer(5, MEMBER, &format!("{}#bsky_appview", aud()), notices);
    let r = client.get(h.url(notices)).header("authorization", &named).send().await.expect("request");
    assert_eq!(error_of(r).await, (401, "InvalidToken".to_string()));
    assert!(h.seen("getNotices").is_empty());
    h.db.drop_db().await;
}
