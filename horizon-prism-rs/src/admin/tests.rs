//! The admin UI end to end over HTTP (admin-ui design §15: T-A1, T-A2,
//! T-A3, T-A4, T-A7, T-A16; membership design T-M9), with a stub identity
//! and a stub authorization server behind the `FetchHandler` seam, served
//! through `xrpc::router::finalize` as `serve` composes it.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use proto_blue::common::fetch::{FetchError, FetchHandler, HttpHeaders, HttpMethod, HttpRequest, HttpResponse};
use reqwest::header::{HeaderMap as RHeaders, LOCATION, SET_COOKIE};
use serde_json::{Value, json};
use sqlx::PgPool;
use tokio::net::TcpListener;

use super::operators::OperatorIdentity;
use super::*;
use crate::aggregations::ScopeSets;
use crate::config::ScopePolicy;
use crate::identity::DidDocInfo;
use crate::kryphocron_glue::jwt::tests::{StubFetcher, verifier};
use crate::test_support::{TestDb, fresh_db};
use crate::xrpc::router::{FinalConfig, finalize};
use crate::xrpc::{AppState, RateLimits};

const HOST: &str = "prism.example.com";
const AS: &str = "https://as.example.com";
const PDS: &str = "https://pds.example.com";
const OTHER_PDS: &str = "https://other.example.net";
const ALICE: &str = "did:plc:aaaaaaaaaaaaaaaaaaaaaaaa";
const BOB: &str = "did:plc:bbbbbbbbbbbbbbbbbbbbbbbb";
const CAROL: &str = "did:plc:cccccccccccccccccccccccc";

/// DID documents and handles, changeable mid-test.
#[derive(Default)]
struct StubIdentity {
    endpoints: Mutex<HashMap<String, String>>,
    handles: Mutex<HashMap<String, String>>,
    fail: AtomicBool,
    fresh_calls: Mutex<usize>,
}

impl StubIdentity {
    fn set(&self, did: &str, endpoint: &str) {
        self.endpoints.lock().expect("lock").insert(did.to_string(), endpoint.to_string());
    }
    fn calls(&self) -> usize {
        *self.fresh_calls.lock().expect("lock")
    }
}

#[async_trait]
impl OperatorIdentity for StubIdentity {
    async fn resolve_handle(&self, handle: &str) -> Option<String> {
        self.handles.lock().expect("lock").get(handle).cloned()
    }
    async fn resolve_fresh(&self, did: &str) -> Result<DidDocInfo, String> {
        *self.fresh_calls.lock().expect("lock") += 1;
        if self.fail.load(Ordering::SeqCst) {
            return Err("resolution failed".into());
        }
        let endpoint = self.endpoints.lock().expect("lock").get(did).cloned().ok_or("not found")?;
        Ok(DidDocInfo { pds_endpoint: Some(endpoint), claimed_handle: None })
    }
}

/// An authorization server for every PDS, answering in process. It
/// records every URL asked; the token's `sub` is set by the test.
#[derive(Default)]
struct StubAs {
    requests: Mutex<Vec<String>>,
    sub: Mutex<String>,
}

impl StubAs {
    fn asked(&self, prefix: &str) -> bool {
        self.requests.lock().expect("lock").iter().any(|u| u.starts_with(prefix))
    }
}

fn json_response(status: u16, body: &Value) -> HttpResponse {
    let mut headers = HttpHeaders::new();
    headers.insert("content-type".into(), "application/json".into());
    HttpResponse { status, headers, body: body.to_string().into_bytes() }
}

#[async_trait]
impl FetchHandler for StubAs {
    async fn fetch(&self, req: HttpRequest) -> Result<HttpResponse, FetchError> {
        self.requests.lock().expect("lock").push(req.url.clone());
        let url = req.url.as_str();
        if let Some(pds) = url.strip_suffix("/.well-known/oauth-protected-resource") {
            return Ok(json_response(200, &json!({ "resource": pds, "authorization_servers": [AS] })));
        }
        Ok(match (req.method, url.strip_prefix(AS)) {
            (HttpMethod::Get, Some("/.well-known/oauth-authorization-server")) => json_response(
                200,
                &json!({
                    "issuer": AS,
                    "authorization_endpoint": format!("{AS}/oauth/authorize"),
                    "token_endpoint": format!("{AS}/oauth/token"),
                    "pushed_authorization_request_endpoint": format!("{AS}/oauth/par"),
                    "revocation_endpoint": format!("{AS}/oauth/revoke"),
                    "authorization_response_iss_parameter_supported": true,
                }),
            ),
            (HttpMethod::Post, Some("/oauth/par")) => json_response(201, &json!({ "request_uri": "urn:ietf:params:oauth:request_uri:stub", "expires_in": 60 })),
            (HttpMethod::Post, Some("/oauth/token")) => json_response(
                200,
                &json!({ "access_token": "at", "token_type": "DPoP", "scope": "atproto", "sub": self.sub.lock().expect("lock").clone() }),
            ),
            (HttpMethod::Post, Some("/oauth/revoke")) => json_response(200, &json!({})),
            _ => json_response(404, &json!({ "error": "not_found" })),
        })
    }
}

struct Harness {
    db: TestDb,
    base: String,
    identity: Arc<StubIdentity>,
    oauth: Arc<StubAs>,
    code_file: PathBuf,
    _drainer: Box<dyn Send>,
}

impl Harness {
    fn pool(&self) -> &PgPool {
        &self.db.pool
    }
}

async fn harness(name: &str) -> Option<Harness> {
    let db = fresh_db(name).await?;
    let identity = Arc::new(StubIdentity::default());
    let oauth = Arc::new(StubAs::default());
    let dir = std::env::temp_dir().join(format!("hprs-admin-{}", rand::random::<u32>()));
    std::fs::create_dir_all(&dir).expect("dir");
    let code_file = setup_code::file_path(&dir);
    let scope = ScopeState::new(ScopeSets::default(), ScopePolicy::MembersAndOneHop, 10_000);
    let settings = AdminSettings {
        public_hostname: HOST.to_string(),
        // The test client is the "proxy", so forwarded addresses count.
        trusted_proxy: "127.0.0.1".parse().expect("ip"),
        session_idle: Duration::from_secs(28_800),
        session_max: Duration::from_secs(604_800),
        setup_code_file: code_file.clone(),
        members: MembersInfo { auto_enroll: false, max_members: 1000, enroll_pds_patterns: Vec::new() },
    };
    let admin = AdminState::new(
        db.pool.clone(),
        scope,
        Arc::clone(&identity) as Arc<dyn OperatorIdentity>,
        Arc::clone(&oauth) as Arc<dyn FetchHandler>,
        settings,
    );
    let (auth, drainer) = verifier(StubFetcher::default(), 10, 4);
    let app = AppState {
        hydrator: crate::hydration::testing::hydrator(&db.pool, ScopeSets::default()),
        auth: Arc::new(auth),
        limits: RateLimits::new(),
        enroller: None,
        trusted_proxy: "127.0.0.1".parse().expect("ip"),
        did_document: Arc::new(json!({})),
        membership: Default::default(),
    };
    let routes = finalize(router(admin), app, FinalConfig { not_ready_retry_secs: 5, public: Default::default() });
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().expect("addr");
    tokio::spawn(async move {
        axum::serve(listener, routes.into_make_service_with_connect_info::<SocketAddr>()).await.expect("serve");
    });
    Some(Harness { db, base: format!("http://{addr}"), identity, oauth, code_file, _drainer: Box::new(drainer) })
}

/// A browser: no redirects followed, cookies kept by hand.
struct Browser {
    http: reqwest::Client,
    cookies: Mutex<HashMap<String, String>>,
    forwarded_for: Option<String>,
}

struct Page {
    status: u16,
    headers: RHeaders,
    body: String,
}

impl Browser {
    fn new() -> Self {
        Self::from_address(None)
    }

    fn from_address(addr: Option<&str>) -> Self {
        Self {
            http: reqwest::Client::builder().redirect(reqwest::redirect::Policy::none()).build().expect("client"),
            cookies: Mutex::new(HashMap::new()),
            forwarded_for: addr.map(str::to_string),
        }
    }

    fn cookie_header(&self) -> String {
        self.cookies.lock().expect("lock").iter().map(|(k, v)| format!("{k}={v}")).collect::<Vec<_>>().join("; ")
    }

    fn cookie(&self, name: &str) -> Option<String> {
        self.cookies.lock().expect("lock").get(name).cloned()
    }

    fn keep(&self, headers: &RHeaders) {
        for v in headers.get_all(SET_COOKIE) {
            let v = v.to_str().expect("cookie");
            let pair = v.split(';').next().expect("pair");
            let (k, val) = pair.split_once('=').expect("k=v");
            let mut jar = self.cookies.lock().expect("lock");
            if val.is_empty() {
                jar.remove(k);
            } else {
                jar.insert(k.to_string(), val.to_string());
            }
        }
    }

    async fn send(&self, req: reqwest::RequestBuilder) -> Page {
        let mut req = req.header("cookie", self.cookie_header());
        if let Some(a) = &self.forwarded_for {
            req = req.header("x-forwarded-for", a);
        }
        let r = req.send().await.expect("request");
        let status = r.status().as_u16();
        let headers = r.headers().clone();
        self.keep(&headers);
        Page { status, headers, body: r.text().await.expect("body") }
    }

    async fn get(&self, url: &str) -> Page {
        self.send(self.http.get(url)).await
    }

    async fn post(&self, url: &str, form: &[(&str, &str)]) -> Page {
        self.send(self.http.post(url).form(form)).await
    }

    async fn post_with(&self, url: &str, form: &[(&str, &str)], origin: &str) -> Page {
        self.send(self.http.post(url).header("origin", origin).form(form)).await
    }
}

/// The value of a hidden field in a page.
fn field(body: &str, name: &str) -> Option<String> {
    let at = body.find(&format!("name=\"{name}\" value=\""))?;
    let rest = &body[at + name.len() + 15..];
    Some(rest[..rest.find('"')?].to_string())
}

async fn latest_state(pool: &PgPool) -> String {
    sqlx::query_scalar("SELECT state FROM admin_oauth_states ORDER BY created_at DESC LIMIT 1").fetch_one(pool).await.expect("state")
}

async fn count(pool: &PgPool, sql: &str) -> i64 {
    sqlx::query_scalar(sql).fetch_one(pool).await.expect(sql)
}

/// Start setup with `code` as `input`; the continuation page.
async fn start_setup(h: &Harness, b: &Browser, code: &str, input: &str) -> Page {
    let form = b.get(&format!("{}/admin/setup", h.base)).await;
    assert_eq!(form.status, 200, "{}", form.body);
    let presession = field(&form.body, "presession").expect("presession");
    b.post(&format!("{}/admin/setup", h.base), &[("code", code), ("input", input), ("presession", &presession)]).await
}

async fn callback(h: &Harness, b: &Browser, state: &str, iss: &str) -> Page {
    b.get(&format!("{}/admin/oauth/callback?state={state}&code=c0de&iss={iss}", h.base)).await
}

/// Setup as `did` on `PDS`, completed; returns the signed-in browser.
async fn complete_setup(h: &Harness, did: &str) -> Browser {
    h.identity.set(did, PDS);
    let open = setup_code::at_startup(h.pool(), &h.code_file).await.expect("code").expect("open");
    let b = Browser::new();
    let page = start_setup(h, &b, &open.code, did).await;
    assert_eq!(page.status, 200, "{}", page.body);
    *h.oauth.sub.lock().expect("lock") = did.to_string();
    let state = latest_state(h.pool()).await;
    let done = callback(h, &b, &state, AS).await;
    assert_eq!(done.status, 200, "{}", done.body);
    b
}

/// Sign in as an operator `did`; the browser and the continuation page of
/// the login POST.
async fn sign_in(h: &Harness, did: &str) -> (Browser, Page) {
    let b = Browser::new();
    let form = b.get(&format!("{}/admin/login", h.base)).await;
    let presession = field(&form.body, "presession").expect("presession");
    let page = b.post(&format!("{}/admin/login", h.base), &[("input", did), ("presession", &presession)]).await;
    if page.status == 200 {
        *h.oauth.sub.lock().expect("lock") = did.to_string();
        let state = latest_state(h.pool()).await;
        let done = callback(h, &b, &state, AS).await;
        assert_eq!(done.status, 200, "{}", done.body);
    }
    (b, page)
}

fn csrf_of(page: &Page) -> String {
    field(&page.body, "csrf").expect("csrf")
}

/// T-A16: the §3 headers.
fn assert_admin_headers(page: &Page, what: &str) {
    let h = |n: &str| page.headers.get(n).and_then(|v| v.to_str().ok()).unwrap_or_default().to_string();
    assert_eq!(h("content-security-policy"), CSP, "{what}");
    assert_eq!(h("x-frame-options"), "DENY", "{what}");
    assert_eq!(h("referrer-policy"), "no-referrer", "{what}");
    assert_eq!(h("x-content-type-options"), "nosniff", "{what}");
    assert!(page.headers.get("access-control-allow-credentials").is_none(), "{what}");
}

/// T-A1: the code's life across starts. The first start writes the row
/// (hash only) and the 0600 file; a restart keeps the same code; an expired
/// row, or a missing, damaged or mismatched file, yields a new one; with an
/// operator, the row and the file go and nothing is returned.
#[tokio::test]
async fn t_a1_setup_code_across_starts() {
    let Some(db) = fresh_db("admin_t_a1").await else {
        return;
    };
    let p = &db.pool;
    let dir = std::env::temp_dir().join(format!("hprs-a1-{}", rand::random::<u32>()));
    std::fs::create_dir_all(&dir).expect("dir");
    let path = setup_code::file_path(&dir);
    let first = setup_code::at_startup(p, &path).await.expect("start").expect("open");
    let stored: Vec<u8> = sqlx::query_scalar("SELECT code_hash FROM admin_setup_code").fetch_one(p).await.expect("row");
    assert_eq!(stored, setup_code::hash(&first.code).to_vec());
    assert!(!String::from_utf8_lossy(&stored).contains(&setup_code::normalize(&first.code)), "only the hash is stored");
    assert_eq!(setup_code::read_file(&path).as_deref(), Some(first.code.as_str()));
    let again = setup_code::at_startup(p, &path).await.expect("start").expect("open");
    assert_eq!(again.code, first.code, "kept across a restart");
    // Expired: a new code, and the old hash is gone.
    sqlx::query("UPDATE admin_setup_code SET expires_at = now() - interval '1 second'").execute(p).await.expect("expire");
    let renewed = setup_code::at_startup(p, &path).await.expect("start").expect("open");
    assert_ne!(renewed.code, first.code);
    let stored: Vec<u8> = sqlx::query_scalar("SELECT code_hash FROM admin_setup_code").fetch_one(p).await.expect("row");
    assert_ne!(stored, setup_code::hash(&first.code).to_vec(), "the old code no longer matches");
    // Missing, damaged, or another code: a new code each time, never empty.
    for damage in [None, Some(""), Some("prs-ABC"), Some("prs-00000-00000-00000-00000-000000")] {
        let before = setup_code::read_file(&path).expect("code");
        match damage {
            None => setup_code::delete_file(&path),
            Some(text) => std::fs::write(&path, text).expect("damage"),
        }
        let next = setup_code::at_startup(p, &path).await.expect("start").expect("open");
        assert!(setup_code::well_formed(&next.code));
        assert_ne!(next.code, before, "{damage:?}");
        assert_ne!(setup_code::normalize(&next.code), setup_code::normalize("prs-00000-00000-00000-00000-000000"));
    }
    // With an operator: row and file deleted, nothing printed.
    sqlx::query("INSERT INTO admin_operators (did, added_by) VALUES ($1, 'cli')").bind(ALICE).execute(p).await.expect("operator");
    assert_eq!(setup_code::at_startup(p, &path).await.expect("start"), None);
    assert_eq!(count(p, "SELECT count(*) FROM admin_setup_code").await, 0);
    assert!(!path.exists());
    let _ = std::fs::remove_dir_all(&dir);
    db.drop_db().await;
}

/// A log capture for the current thread.
#[derive(Clone, Default)]
struct Capture(Arc<Mutex<Vec<u8>>>);

impl std::io::Write for Capture {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.0.lock().expect("lock").extend_from_slice(buf);
        Ok(buf.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for Capture {
    type Writer = Self;
    fn make_writer(&'a self) -> Self {
        self.clone()
    }
}

/// T-A1: the code is printed at start and again while no operator exists,
/// only in `admin_setup_code` lines; once an operator exists the reprint
/// stops and the file goes.
#[tokio::test]
async fn t_a1_reprint_and_only_one_line_carries_the_code() {
    let Some(db) = fresh_db("admin_t_a1_print").await else {
        return;
    };
    let capture = Capture::default();
    let subscriber = tracing_subscriber::fmt().json().with_writer(capture.clone()).with_max_level(tracing::Level::DEBUG).finish();
    let _guard = tracing::subscriber::set_default(subscriber);
    let dir = std::env::temp_dir().join(format!("hprs-a1p-{}", rand::random::<u32>()));
    std::fs::create_dir_all(&dir).expect("dir");
    let path = setup_code::file_path(&dir);
    let open = setup_code::at_startup(&db.pool, &path).await.expect("start").expect("open");
    setup_code::print(&open, HOST);
    let task = setup_code::spawn_reprint_every(db.pool.clone(), path.clone(), HOST.to_string(), Duration::from_millis(50));
    tokio::time::sleep(Duration::from_millis(400)).await;
    sqlx::query("INSERT INTO admin_operators (did, added_by) VALUES ($1, 'cli')").bind(ALICE).execute(&db.pool).await.expect("operator");
    tokio::time::timeout(Duration::from_secs(5), task).await.expect("the reprint stops").expect("task");
    assert!(!path.exists(), "the file goes once an operator exists");
    let log = String::from_utf8(capture.0.lock().expect("lock").clone()).expect("utf8");
    let lines: Vec<&str> = log.lines().filter(|l| l.contains(&open.code) || l.contains(&setup_code::normalize(&open.code))).collect();
    assert!(lines.len() >= 3, "printed at start and again: {log}");
    assert!(lines.iter().all(|l| l.contains("\"event\":\"admin_setup_code\"")), "{lines:?}");
    let _ = std::fs::remove_dir_all(&dir);
    db.drop_db().await;
}

/// T-A2, T-A16: setup end to end with the stub authorization server.
#[tokio::test]
async fn t_a2_setup_flow() {
    let Some(h) = harness("admin_t_a2").await else {
        return;
    };
    let p = h.pool();
    h.identity.set(ALICE, PDS);
    let open = setup_code::at_startup(p, &h.code_file).await.expect("code").expect("open");
    // Before any operator: pages go to setup; metadata and the callback answer.
    let b = Browser::new();
    let home = b.get(&format!("{}/admin", h.base)).await;
    assert_eq!((home.status, home.headers.get(LOCATION).and_then(|v| v.to_str().ok())), (303, Some("/admin/setup")));
    assert_admin_headers(&home, "redirect");
    let meta = b.get(&format!("{}/admin/oauth/client-metadata.json", h.base)).await;
    assert_eq!(meta.status, 200);
    let meta: Value = serde_json::from_str(&meta.body).expect("json");
    assert_eq!(meta["client_id"], format!("https://{HOST}/admin/oauth/client-metadata.json"));
    assert_eq!(meta["redirect_uris"], json!([format!("https://{HOST}/admin/oauth/callback")]));
    assert_eq!((meta["scope"].as_str(), meta["dpop_bound_access_tokens"].as_bool()), (Some("atproto"), Some(true)));
    let bare = b.get(&format!("{}/admin/oauth/callback", h.base)).await;
    assert_eq!(bare.status, 400, "a page, not a redirect");
    // The form, the stylesheet, and the headers.
    let form = b.get(&format!("{}/admin/setup", h.base)).await;
    assert_admin_headers(&form, "setup form");
    assert_eq!(form.headers.get("cache-control").and_then(|v| v.to_str().ok()), Some("no-store"));
    let css = b.get(&format!("{}{}", h.base, *CSS_HREF)).await;
    assert_eq!(css.status, 200);
    assert!(css.headers.get("cache-control").and_then(|v| v.to_str().ok()).is_some_and(|v| v.contains("max-age=31536000")));
    assert!(!form.body.contains("style="), "no inline styles");
    // Without the pre-session token, or from a foreign Origin: refused, nothing started.
    let refused = b.post(&format!("{}/admin/setup", h.base), &[("code", &open.code), ("input", ALICE), ("presession", "nope")]).await;
    assert_eq!(refused.status, 403);
    let presession = field(&form.body, "presession").expect("presession");
    let foreign = b
        .post_with(&format!("{}/admin/setup", h.base), &[("code", &open.code), ("input", ALICE), ("presession", &presession)], "https://evil.example")
        .await;
    assert_eq!(foreign.status, 403);
    assert_eq!(count(p, "SELECT count(*) FROM admin_oauth_states").await, 0);
    // Two flows with the same code; the first to finish wins.
    let first = start_setup(&h, &b, &open.code, ALICE).await;
    assert_eq!(first.status, 200, "a continuation page, never a 3xx: {}", first.body);
    assert!(first.body.contains("http-equiv=\"refresh\"") && first.body.contains("as.example.com/oauth/authorize"));
    assert_admin_headers(&first, "continuation");
    let first_state = latest_state(p).await;
    let b2 = Browser::new();
    h.identity.set(BOB, PDS);
    let second = start_setup(&h, &b2, &open.code.to_lowercase().replace('-', ""), BOB).await;
    assert_eq!(second.status, 200, "{}", second.body);
    let second_state = latest_state(p).await;
    assert_ne!(first_state, second_state);
    *h.oauth.sub.lock().expect("lock") = ALICE.to_string();
    let done = callback(&h, &b, &first_state, AS).await;
    assert_eq!(done.status, 200, "{}", done.body);
    assert!(b.cookie(auth::SESSION_COOKIE).is_some());
    let (operators, hosts, code_rows): (i64, i64, i64) = sqlx::query_as(
        "SELECT (SELECT count(*) FROM admin_operators WHERE added_by = 'setup'), (SELECT count(*) FROM admin_operator_hosts WHERE pattern = 'pds.example.com'), (SELECT count(*) FROM admin_setup_code)",
    )
    .fetch_one(p)
    .await
    .expect("counts");
    assert_eq!((operators, hosts, code_rows), (1, 1, 0));
    assert!(!h.code_file.exists(), "the file goes after commit");
    assert_eq!(count(p, "SELECT count(*) FROM admin_logins WHERE outcome = 'setup'").await, 1);
    let home = b.get(&format!("{}/admin", h.base)).await;
    assert_eq!(home.status, 200, "signed in: {}", home.body);
    assert!(!home.body.contains(&open.code), "the code is in no page");
    *h.oauth.sub.lock().expect("lock") = BOB.to_string();
    let late = callback(&h, &b2, &second_state, AS).await;
    assert_eq!(late.status, 409, "the second flow finds no code: {}", late.body);
    assert_eq!(count(p, "SELECT count(*) FROM admin_operators").await, 1);
    // A replayed callback finds no state.
    let replay = callback(&h, &b, &first_state, AS).await;
    assert_eq!(replay.status, 400);
    // With an operator, setup is gone.
    assert_eq!(b.get(&format!("{}/admin/setup", h.base)).await.status, 404);
    assert_eq!(b.post(&format!("{}/admin/setup", h.base), &[("code", "x"), ("input", ALICE), ("presession", "x")]).await.status, 404);
    h.db.drop_db().await;
}

/// T-A2: wrong codes past five a minute from one address (an IPv6 /64 as
/// one) are delayed, and past the bound answered 429; a correct code is
/// never refused, even from a throttled address.
#[tokio::test]
async fn t_a2_wrong_codes_are_throttled_per_address() {
    let Some(h) = harness("admin_t_a2_throttle").await else {
        return;
    };
    h.identity.set(ALICE, PDS);
    let open = setup_code::at_startup(h.pool(), &h.code_file).await.expect("code").expect("open");
    let wrong = "prs-00000-00000-00000-00000-000000";
    let attacker = Browser::from_address(Some("2001:db8::1"));
    for _ in 0..5 {
        let started = Instant::now();
        assert_eq!(start_setup(&h, &attacker, wrong, ALICE).await.status, 400);
        assert!(started.elapsed() < auth::WRONG_CODE_DELAY);
    }
    // Same /64, another address: delayed.
    let neighbour = Browser::from_address(Some("2001:db8::2"));
    let started = Instant::now();
    assert_eq!(start_setup(&h, &neighbour, wrong, ALICE).await.status, 400);
    assert!(started.elapsed() >= auth::WRONG_CODE_DELAY, "the sixth wrong code is delayed");
    // Past the bound of delayed answers at once: 429.
    let attempts = (0..auth::MAX_DELAYED + 4).map(|_| async {
        let b = Browser::from_address(Some("2001:db8::3"));
        start_setup(&h, &b, wrong, ALICE).await.status
    });
    let statuses = futures::future::join_all(attempts).await;
    assert!(statuses.contains(&429), "{statuses:?}");
    // Another address is not slowed, and the correct code works from the
    // throttled one.
    let other = Browser::from_address(Some("198.51.100.7"));
    let started = Instant::now();
    assert_eq!(start_setup(&h, &other, wrong, ALICE).await.status, 400);
    assert!(started.elapsed() < auth::WRONG_CODE_DELAY);
    let started = Instant::now();
    let ok = start_setup(&h, &attacker, &open.code, ALICE).await;
    assert_eq!(ok.status, 200, "{}", ok.body);
    assert!(started.elapsed() < auth::WRONG_CODE_DELAY, "a correct code is checked first");
    h.db.drop_db().await;
}

/// T-A3, T-M9: only an operator on an allowed host starts a flow, and a
/// refused one makes no request to its server; the token must name the
/// expected DID and come back from the stored issuer; a DID that moves
/// between start and callback is refused.
#[tokio::test]
async fn t_a3_host_check_at_login() {
    let Some(h) = harness("admin_t_a3").await else {
        return;
    };
    let p = h.pool();
    let _alice = complete_setup(&h, ALICE).await;
    // Not an operator: refused, no flow, no fetch.
    h.identity.set(BOB, PDS);
    h.oauth.requests.lock().expect("lock").clear();
    let (_, page) = sign_in(&h, BOB).await;
    assert_eq!(page.status, 403);
    assert!(page.body.contains("not an operator"));
    assert!(h.oauth.requests.lock().expect("lock").is_empty());
    // A PDS URL is refused.
    let (_, page) = sign_in(&h, "https://pds.example.com").await;
    assert_eq!(page.status, 400);
    // An operator on an unlisted host: refused before any request to it.
    sqlx::query("INSERT INTO admin_operators (did, added_by) VALUES ($1, 'cli')").bind(CAROL).execute(p).await.expect("carol");
    h.identity.set(CAROL, OTHER_PDS);
    let (_, page) = sign_in(&h, CAROL).await;
    assert_eq!(page.status, 403);
    assert!(page.body.contains("other.example.net, which is not on this Prism"), "{}", page.body);
    assert!(!h.oauth.asked(OTHER_PDS), "no request to the refused server");
    assert_eq!(count(p, "SELECT count(*) FROM admin_oauth_states").await, 0);
    assert_eq!(count(p, "SELECT count(*) FROM admin_logins WHERE outcome = 'host_not_allowed'").await, 1);
    // Listed: proceeds, and asks that server.
    sqlx::query("INSERT INTO admin_operator_hosts (pattern, added_by) VALUES ('*.example.net', 'cli')").execute(p).await.expect("host");
    let (carol, page) = sign_in(&h, CAROL).await;
    assert_eq!(page.status, 200);
    assert!(h.oauth.asked(OTHER_PDS));
    assert_eq!(carol.get(&format!("{}/admin", h.base)).await.status, 200);
    // A token for another DID, or from another issuer: no session.
    for (sub, iss) in [(BOB, AS), (CAROL, "https://evil.example.com")] {
        let b = Browser::new();
        let form = b.get(&format!("{}/admin/login", h.base)).await;
        let presession = field(&form.body, "presession").expect("presession");
        assert_eq!(b.post(&format!("{}/admin/login", h.base), &[("input", CAROL), ("presession", &presession)]).await.status, 200);
        *h.oauth.sub.lock().expect("lock") = sub.to_string();
        let state = latest_state(p).await;
        let page = callback(&h, &b, &state, iss).await;
        assert_eq!(page.status, 400, "sub {sub}, iss {iss}");
        assert!(b.cookie(auth::SESSION_COOKIE).is_none());
    }
    // The DID moves between start and callback: refused.
    let b = Browser::new();
    let form = b.get(&format!("{}/admin/login", h.base)).await;
    let presession = field(&form.body, "presession").expect("presession");
    assert_eq!(b.post(&format!("{}/admin/login", h.base), &[("input", CAROL), ("presession", &presession)]).await.status, 200);
    h.identity.set(CAROL, "https://moved.example.net");
    *h.oauth.sub.lock().expect("lock") = CAROL.to_string();
    let state = latest_state(p).await;
    let page = callback(&h, &b, &state, AS).await;
    assert_eq!(page.status, 400);
    assert!(page.body.contains("moved"));
    assert!(b.cookie(auth::SESSION_COOKIE).is_none());
    h.db.drop_db().await;
}

/// T-M9: sign-in starts are limited per client address, keyed on the
/// forwarded address from the trusted proxy.
#[tokio::test]
async fn t_m9_login_limit_keys_on_the_forwarded_address() {
    let Some(h) = harness("admin_t_m9_limit").await else {
        return;
    };
    let _alice = complete_setup(&h, ALICE).await;
    let one = Browser::from_address(Some("203.0.113.5"));
    let mut statuses = Vec::new();
    for _ in 0..11 {
        let form = one.get(&format!("{}/admin/login", h.base)).await;
        let presession = field(&form.body, "presession").expect("presession");
        statuses.push(one.post(&format!("{}/admin/login", h.base), &[("input", "did:plc:dddddddddddddddddddddddd"), ("presession", &presession)]).await.status);
    }
    assert_eq!(statuses.last(), Some(&429), "{statuses:?}");
    let two = Browser::from_address(Some("203.0.113.6"));
    let form = two.get(&format!("{}/admin/login", h.base)).await;
    let presession = field(&form.body, "presession").expect("presession");
    assert_eq!(two.post(&format!("{}/admin/login", h.base), &[("input", "did:plc:dddddddddddddddddddddddd"), ("presession", &presession)]).await.status, 400);
    h.db.drop_db().await;
}

/// T-A4: a PDS change ends the session at the next POST, or at the next
/// page after five minutes; a failed resolution refuses the request and
/// keeps the session; an operator removed loses the session.
#[tokio::test]
async fn t_a4_host_recheck() {
    let Some(h) = harness("admin_t_a4").await else {
        return;
    };
    let p = h.pool();
    let alice = complete_setup(&h, ALICE).await;
    let members = alice.get(&format!("{}/admin/members", h.base)).await;
    assert_eq!(members.status, 200);
    let csrf = csrf_of(&members);
    // A failed resolution: refused, the session kept.
    h.identity.fail.store(true, Ordering::SeqCst);
    let page = alice.post(&format!("{}/admin/members/add", h.base), &[("csrf", &csrf), ("input", BOB)]).await;
    assert_eq!(page.status, 503);
    assert_eq!(count(p, "SELECT count(*) FROM admin_sessions").await, 1);
    assert_eq!(count(p, "SELECT count(*) FROM admin_commands").await, 0);
    h.identity.fail.store(false, Ordering::SeqCst);
    tokio::time::sleep(Duration::from_millis(1100)).await;
    // A page within five minutes does not resolve.
    let calls = h.identity.calls();
    assert_eq!(alice.get(&format!("{}/admin/members", h.base)).await.status, 200);
    assert_eq!(h.identity.calls(), calls, "no fresh resolution for a page inside five minutes");
    // The PDS changes, even to another allowed host: the next POST cuts.
    sqlx::query("INSERT INTO admin_operator_hosts (pattern, added_by) VALUES ('*.example.com', 'cli')").execute(p).await.expect("host");
    h.identity.set(ALICE, "https://pds2.example.com");
    let page = alice.post(&format!("{}/admin/members/add", h.base), &[("csrf", &csrf), ("input", BOB)]).await;
    assert_eq!(page.status, 401, "{}", page.body);
    assert!(page.body.contains("moved"));
    assert_eq!(count(p, "SELECT count(*) FROM admin_sessions").await, 0);
    assert_eq!(count(p, "SELECT count(*) FROM admin_commands").await, 0, "nothing was queued");
    assert_eq!(count(p, "SELECT count(*) FROM admin_logins WHERE outcome = 'session_cut:pds_changed'").await, 1);
    // A GET after five minutes re-checks too.
    h.identity.set(ALICE, PDS);
    let (alice, _) = sign_in(&h, ALICE).await;
    h.identity.set(ALICE, "https://pds3.example.com");
    assert_eq!(alice.get(&format!("{}/admin", h.base)).await.status, 200, "inside five minutes");
    sqlx::query("UPDATE admin_sessions SET checked_at = now() - interval '6 minutes'").execute(p).await.expect("age");
    assert_eq!(alice.get(&format!("{}/admin", h.base)).await.status, 401);
    assert_eq!(count(p, "SELECT count(*) FROM admin_sessions").await, 0);
    // Removed from the operators: cut at the next request.
    h.identity.set(ALICE, PDS);
    let (alice, _) = sign_in(&h, ALICE).await;
    sqlx::query("DELETE FROM admin_operators WHERE did = $1").bind(ALICE).execute(p).await.expect("remove");
    sqlx::query("INSERT INTO admin_operators (did, added_by) VALUES ($1, 'cli')").bind(BOB).execute(p).await.expect("bob");
    let page = alice.get(&format!("{}/admin", h.base)).await;
    assert_eq!(page.status, 401);
    assert!(page.body.contains("no longer an operator"));
    h.db.drop_db().await;
}

/// T-A7: a POST without the session's token, with another session's token,
/// or from a foreign Origin changes nothing; a typed action without its
/// word changes nothing; with it, the command is queued with
/// `requested_by`.
#[tokio::test]
async fn t_a7_csrf_and_confirmation() {
    let Some(h) = harness("admin_t_a7").await else {
        return;
    };
    let p = h.pool();
    let alice = complete_setup(&h, ALICE).await;
    sqlx::query("INSERT INTO admin_operators (did, added_by) VALUES ($1, 'cli')").bind(BOB).execute(p).await.expect("bob");
    h.identity.set(BOB, PDS);
    let (bob, _) = sign_in(&h, BOB).await;
    let alice_csrf = csrf_of(&alice.get(&format!("{}/admin/members", h.base)).await);
    let bob_csrf = csrf_of(&bob.get(&format!("{}/admin/members", h.base)).await);
    assert_ne!(alice_csrf, bob_csrf);
    let add = format!("{}/admin/members/add", h.base);
    for (form, origin) in [
        (vec![("input", CAROL)], None),
        (vec![("input", CAROL), ("csrf", bob_csrf.as_str())], None),
        (vec![("input", CAROL), ("csrf", alice_csrf.as_str())], Some("https://evil.example")),
    ] {
        let page = match origin {
            Some(o) => alice.post_with(&add, &form, o).await,
            None => alice.post(&add, &form).await,
        };
        assert_eq!(page.status, 403, "{form:?} {origin:?}");
    }
    assert_eq!(count(p, "SELECT count(*) FROM admin_commands").await, 0);
    // Removal: confirmation, then the typed word (no handle stored: the
    // DID's last eight characters).
    sqlx::query("INSERT INTO members (did, source) VALUES ($1, 'admin')").bind(CAROL).execute(p).await.expect("member");
    let remove = format!("{}/admin/members/remove", h.base);
    let confirm = alice.post(&remove, &[("csrf", &alice_csrf), ("did", CAROL)]).await;
    assert_eq!(confirm.status, 200);
    assert!(confirm.body.contains("cccccccc"), "{}", confirm.body);
    // Changes are limited to one a second per session.
    tokio::time::sleep(Duration::from_millis(1100)).await;
    let wrong = alice.post(&remove, &[("csrf", &alice_csrf), ("did", CAROL), ("confirm", "yes"), ("typed", "nope")]).await;
    assert_eq!(wrong.status, 400);
    tokio::time::sleep(Duration::from_millis(1100)).await;
    let missing = alice.post(&remove, &[("csrf", &alice_csrf), ("did", CAROL), ("confirm", "yes")]).await;
    assert_eq!(missing.status, 400);
    assert_eq!(count(p, "SELECT count(*) FROM admin_commands").await, 0);
    tokio::time::sleep(Duration::from_millis(1100)).await;
    let done = alice.post(&remove, &[("csrf", &alice_csrf), ("did", CAROL), ("confirm", "yes"), ("typed", "CCCCCCCC")]).await;
    assert_eq!(done.status, 303, "{}", done.body);
    let (command, by): (String, Option<String>) =
        sqlx::query_as("SELECT command, requested_by FROM admin_commands").fetch_one(p).await.expect("row");
    assert_eq!((command.as_str(), by.as_deref()), ("member remove", Some(ALICE)));
    let location = done.headers.get(LOCATION).and_then(|v| v.to_str().ok()).expect("location").to_string();
    let page = alice.get(&format!("{}{location}", h.base)).await;
    assert_eq!(page.status, 200);
    assert!(page.body.contains("Waiting to run") && page.body.contains("http-equiv=\"refresh\""), "{}", page.body);
    // The activity page lists it, by who.
    let activity = alice.get(&format!("{}/admin/activity?what=member+remove", h.base)).await;
    assert_eq!(activity.status, 200);
    assert!(activity.body.contains("Remove member did:plc:cccccccccccccccccccccccc"));
    // Sign-out needs the token too.
    assert_eq!(alice.post(&format!("{}/admin/logout", h.base), &[("csrf", "x")]).await.status, 403);
    let out = alice.post(&format!("{}/admin/logout", h.base), &[("csrf", &alice_csrf)]).await;
    assert_eq!(out.status, 303);
    assert_eq!(alice.get(&format!("{}/admin", h.base)).await.headers.get(LOCATION).and_then(|v| v.to_str().ok()), Some("/admin/login"));
    h.db.drop_db().await;
}

/// T-A16: headers on every kind of `/admin` answer, a wrong verb, and an
/// unknown path; CORS never allows credentials.
#[tokio::test]
async fn t_a16_headers_everywhere() {
    let Some(h) = harness("admin_t_a16").await else {
        return;
    };
    let alice = complete_setup(&h, ALICE).await;
    for path in ["/admin", "/admin/members", "/admin/activity", "/admin/operators", "/admin/nowhere", "/admin/login"] {
        let page = alice.send(alice.http.get(format!("{}{path}", h.base)).header("origin", "https://elsewhere.example")).await;
        assert_admin_headers(&page, path);
        if path == "/admin/nowhere" {
            assert_eq!(page.status, 404);
        }
    }
    let wrong_verb = alice.send(alice.http.delete(format!("{}/admin/members", h.base))).await;
    assert_eq!(wrong_verb.status, 405);
    assert_admin_headers(&wrong_verb, "wrong verb");
    h.db.drop_db().await;
}
