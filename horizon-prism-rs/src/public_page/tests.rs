//! The public page's tests: T-P1 to T-P6, T-P9, the relay hosts, and the
//! source checks of T-P15 that concern `/`.

use std::collections::{BTreeMap, BTreeSet};
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use arc_swap::ArcSwap;
use axum::Router;
use serde_json::json;
use tokio::net::TcpListener;

use super::status::{self, Delay, FeedSummary, Observed, RelayFigure, StatusSnapshot};
use super::*;
use crate::aggregations::ScopeSets;
use crate::config::{PublicPageClient, RateClassConfig, RateLimitsConfig};
use crate::honest_501::PublicMessages;
use crate::xrpc::AppState;
use crate::xrpc::router::{FinalConfig, finalize};

pub(super) const HOST: &str = "prism.example.test";

pub(super) fn app_state(pool: &sqlx::PgPool) -> (AppState, impl Sized + use<>) {
    use crate::kryphocron_glue::jwt::tests::{StubFetcher, verifier};
    let (auth, drainer) = verifier(StubFetcher::default(), 10, 4);
    let state = AppState {
        hydrator: crate::hydration::testing::hydrator(pool, ScopeSets::default()),
        auth: Arc::new(auth),
        limits: RateLimits::new(),
        enroller: None,
        trusted_proxy: "172.30.0.10".parse().expect("ip"),
        did_document: Arc::new(json!({})),
        membership: Default::default(),
    };
    (state, drainer)
}

pub(super) fn lazy_pool() -> sqlx::PgPool {
    sqlx::PgPool::connect_lazy("postgres://nobody@127.0.0.1:1/none").expect("lazy pool")
}

pub(super) async fn serve_app(app: Router) -> String {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().expect("addr");
    tokio::spawn(async move {
        axum::serve(listener, app.into_make_service_with_connect_info::<SocketAddr>()).await.expect("serve");
    });
    format!("http://{addr}")
}

/// The page's routes without `/live`.
pub(super) fn public(p: PageState) -> Router {
    router(p, None)
}

/// The XRPC router, `public` merged in when given, finalized as `serve`
/// does.
pub(super) fn finalized(state: &AppState, public: Option<Router>) -> Router {
    let merged = crate::xrpc::router(state.clone()).merge(public.unwrap_or_default());
    finalize(merged, state.clone(), FinalConfig { not_ready_retry_secs: 5, public: PublicMessages::default() })
}

pub(super) fn feed() -> FeedSummary {
    FeedSummary::default()
}

pub(super) fn observed(feed: &FeedSummary) -> Observed<'_> {
    Observed { db_ok: true, redis_ok: true, audit_latched: false, feed }
}

pub(super) fn snapshot() -> StatusSnapshot {
    status::build(&observed(&feed()), &[])
}

pub(super) fn fixed(p: &PublicPageConfig) -> Arc<Fixed> {
    Arc::new(Fixed::from_config(p, HOST))
}

pub(super) fn page_state(snapshot: StatusSnapshot, p: &PublicPageConfig, limits: RateLimits) -> PageState {
    PageState {
        snapshot: Arc::new(ArcSwap::from_pointee(snapshot)),
        fixed: fixed(p),
        limits,
        trusted_proxy: "172.30.0.10".parse().expect("ip"),
        name: None,
    }
}

/// The page as rendered for a snapshot.
pub(super) fn render(s: &StatusSnapshot, p: &PublicPageConfig) -> String {
    use askama::Template;
    let f = fixed(p);
    page::view(s, &f, &f.name).render().expect("render")
}

/// The admin UI's name, when set, is the page's title at once; empty, or
/// not one line, leaves the configured one.
#[tokio::test]
async fn the_name_follows_the_admin_setting() {
    let name = Arc::new(std::sync::Mutex::new(String::new()));
    let mut page = page_state(snapshot(), &PublicPageConfig::default(), RateLimits::new());
    page.name = Some({
        let name = Arc::clone(&name);
        Arc::new(move || name.lock().expect("lock").clone())
    });
    assert_eq!(page.title(), HOST);
    *name.lock().expect("lock") = "  Horizon-Prism  ".to_string();
    assert_eq!(page.title(), "Horizon-Prism");
    *name.lock().expect("lock") = "two\nlines".to_string();
    assert_eq!(page.title(), HOST);
    *name.lock().expect("lock") = "Horizon-Prism".to_string();
    let pool = lazy_pool();
    let (state, _drainer) = app_state(&pool);
    let base = serve_app(finalized(&state, Some(public(page)))).await;
    let body = reqwest::get(format!("{base}/")).await.expect("get").text().await.expect("body");
    assert!(body.contains("<title>Horizon-Prism</title>") && body.contains("<h1>Horizon-Prism</h1>"), "{body}");
}

/// T-P1: `GET /` is 200 HTML with the §5 headers; a wrong verb is 405 with
/// `Allow` {GET, HEAD} and "method not allowed"; with the page off, `/` is
/// the empty 404 it was; an unknown NSID is still 501; the removed routes
/// are gone.
#[tokio::test]
async fn t_p1_route_headers_and_opt_out() {
    let pool = lazy_pool();
    let (state, _drainer) = app_state(&pool);
    let public = public(page_state(snapshot(), &PublicPageConfig::default(), RateLimits::new()));
    let base = serve_app(finalized(&state, Some(public))).await;
    let client = reqwest::Client::new();

    let r = client.get(format!("{base}/")).send().await.expect("get");
    assert_eq!(r.status().as_u16(), 200);
    let h = r.headers().clone();
    assert!(h["content-type"].to_str().expect("ct").starts_with("text/html"), "{h:?}");
    assert_eq!(h["cache-control"], PAGE_CACHE);
    // The live feed is on by default, so `/` loads its one script.
    assert_eq!(h["content-security-policy"], CSP_LIVE);
    assert_eq!(h["x-frame-options"], "DENY");
    assert_eq!(h["referrer-policy"], "same-origin");
    assert_eq!(h["x-content-type-options"], "nosniff");
    assert!(h.get("set-cookie").is_none());
    let body = r.text().await.expect("body");
    assert!(body.contains(page::PRODUCT), "{body}");
    assert!(!body.contains("noindex"), "the page may be indexed");
    assert!(!body.contains("/me") && !body.contains("/lexicons/"), "{body}");

    for method in [reqwest::Method::POST, reqwest::Method::PUT, reqwest::Method::DELETE] {
        let r = client.request(method.clone(), format!("{base}/")).send().await.expect("wrong verb");
        assert_eq!(r.status().as_u16(), 405, "{method}");
        let allow: BTreeSet<String> = r.headers()["allow"].to_str().expect("allow").split(',').map(|t| t.trim().to_string()).collect();
        assert_eq!(allow, BTreeSet::from(["GET".to_string(), "HEAD".to_string()]), "{method}");
        assert_eq!(r.headers()["cache-control"], NO_STORE);
        assert_eq!(r.text().await.expect("body"), "method not allowed");
    }

    // The stylesheet, by its hash; anything else under /static is a 404.
    let r = client.get(format!("{base}{}", *CSS_HREF)).send().await.expect("css");
    assert_eq!(r.status().as_u16(), 200);
    assert_eq!(r.headers()["cache-control"], IMMUTABLE);
    assert_eq!(r.text().await.expect("css"), crate::admin::CSS);
    let r = client.get(format!("{base}/static/site-000000000000.css")).send().await.expect("css");
    assert_eq!(r.status().as_u16(), 404);
    assert_eq!(r.headers()["cache-control"], NO_STORE);

    // The self-check and the lexicons are no longer served.
    for path in ["/me", "/me/oauth/client-metadata.json", "/me/oauth/callback", "/lexicons/app/nearhorizon/actor/getMembership.json"] {
        let r = client.get(format!("{base}{path}")).send().await.expect("removed");
        assert_eq!(r.status().as_u16(), 404, "{path}");
    }

    let r = client.get(format!("{base}/xrpc/app.example.doesNotExist")).send().await.expect("501");
    assert_eq!(r.status().as_u16(), 501);

    // Off: no route is merged, so `/` is the fallback's empty 404.
    let off = serve_app(finalized(&state, None)).await;
    let r = client.get(format!("{off}/")).send().await.expect("get");
    assert_eq!(r.status().as_u16(), 404);
    assert!(r.bytes().await.expect("body").is_empty());
    let r = client.get(format!("{off}/xrpc/app.example.doesNotExist")).send().await.expect("501");
    assert_eq!(r.status().as_u16(), 501);
}

/// T-P2: with the database unreachable, `/` still answers 200 within 3 s, and
/// says nothing about it.
#[tokio::test]
async fn t_p2_no_database() {
    let pool = lazy_pool();
    let started = Instant::now();
    let status = status::Status::start(status::StatusInputs {
        pool: pool.clone(),
        redis: None,
        audit: crate::serve::health::AuditLatch::default(),
        relays: Vec::new(),
        feed: Arc::default(),
    })
    .await;
    let (state, _drainer) = app_state(&pool);
    let mut page = page_state(snapshot(), &PublicPageConfig::default(), RateLimits::new());
    page.snapshot = status.cell();
    let base = serve_app(finalized(&state, Some(public(page)))).await;
    let r = reqwest::get(format!("{base}/")).await.expect("get");
    assert_eq!(r.status().as_u16(), 200);
    let body = r.text().await.expect("body");
    assert!(started.elapsed() < Duration::from_secs(3), "{:?}", started.elapsed());
    assert!(body.contains(page::PRODUCT), "{body}");
    assert!(!body.to_lowercase().contains("database"), "{body}");
}

/// The admin UI's "Public page" switch: off, `/`, `/live` and the static
/// files are the empty 404; on again, they answer, with no restart. The
/// favicon is not behind it.
#[tokio::test]
async fn the_switch_turns_the_page_off_and_on() {
    use std::sync::atomic::{AtomicBool, Ordering};
    let pool = lazy_pool();
    let (state, _drainer) = app_state(&pool);
    let on = Arc::new(AtomicBool::new(false));
    let switch: Switch = {
        let on = Arc::clone(&on);
        Arc::new(move || on.load(Ordering::Relaxed))
    };
    let routes = gated(public(page_state(snapshot(), &PublicPageConfig::default(), RateLimits::new())), switch)
        .merge(favicon_router());
    let base = serve_app(finalized(&state, Some(routes))).await;
    let client = reqwest::Client::new();
    for path in ["/", CSS_HREF.as_str(), JS_HREF.as_str()] {
        let r = client.get(format!("{base}{path}")).send().await.expect("off");
        assert_eq!(r.status().as_u16(), 404, "{path}");
        assert!(r.bytes().await.expect("body").is_empty(), "{path}");
    }
    let r = client.get(format!("{base}/favicon.svg")).send().await.expect("icon");
    assert_eq!(r.status().as_u16(), 200);
    on.store(true, Ordering::Relaxed);
    let r = client.get(format!("{base}/")).send().await.expect("on");
    assert_eq!(r.status().as_u16(), 200);
    on.store(false, Ordering::Relaxed);
    let r = client.get(format!("{base}/")).send().await.expect("off again");
    assert_eq!(r.status().as_u16(), 404);
}

/// T-P3: the handler's state is memory only, and 50 requests with every pool
/// at a closed port each answer within 50 ms.
#[tokio::test]
async fn t_p3_no_query_per_request() {
    // The compile-time fact: `PageState` has exactly these fields, none of
    // them a pool or a Redis connection.
    let page = page_state(snapshot(), &PublicPageConfig::default(), RateLimits::new());
    let PageState { snapshot: _, fixed: _, limits: _, trusted_proxy: _, name: _ } = page.clone();
    let pool = lazy_pool();
    let (state, _drainer) = app_state(&pool);
    let base = serve_app(finalized(&state, Some(public(page)))).await;
    let client = reqwest::Client::new();
    let mut slowest = Duration::ZERO;
    for _ in 0..50 {
        let started = Instant::now();
        let r = client.get(format!("{base}/")).header("x-forwarded-for", "203.0.113.1").send().await.expect("get");
        assert_eq!(r.status().as_u16(), 200);
        let _ = r.bytes().await.expect("body");
        slowest = slowest.max(started.elapsed());
    }
    assert!(slowest < Duration::from_millis(50), "slowest {slowest:?}");
}

/// T-P4: `anon_read` per address; the 429 is never cacheable.
#[tokio::test]
async fn t_p4_rate_limit() {
    let limits = RateLimits::from_config(&RateLimitsConfig { anon_read: RateClassConfig::new(0.001, 5), ..RateLimitsConfig::default() });
    let pool = lazy_pool();
    let (state, _drainer) = app_state(&pool);
    let base = serve_app(finalized(&state, Some(public(page_state(snapshot(), &PublicPageConfig::default(), limits))))).await;
    let client = reqwest::Client::new();
    for _ in 0..5 {
        let r = client.get(format!("{base}/")).send().await.expect("get");
        assert_eq!(r.status().as_u16(), 200);
        assert_eq!(r.headers()["cache-control"], PAGE_CACHE);
    }
    let r = client.get(format!("{base}/")).send().await.expect("get");
    assert_eq!(r.status().as_u16(), 429);
    assert!(r.headers().get("ratelimit-reset").is_some(), "{:?}", r.headers());
    assert_eq!(r.headers()["cache-control"], NO_STORE);
}

/// The configured relays, URL → label: distinctive markers in every part,
/// none of which may reach anything served.
pub(super) fn marker_relays() -> BTreeMap<String, String> {
    BTreeMap::from([
        ("wss://zqrelayalpha.zqmarkhost.invalid/xrpc/com.atproto.sync.subscribeRepos".to_string(), "ZQLABELALPHA".to_string()),
        ("wss://ZQRelayBeta.zqmarkhost.invalid".to_string(), "ZQLABELBETA".to_string()),
        ("wss://zqrelaygamma.zqmarkhost.invalid:8443/x".to_string(), "ZQLABELGAMMA".to_string()),
        ("not a url".to_string(), "ZQLABELDELTA".to_string()),
    ])
}

/// Every marker of [`marker_relays`], lowercased: each label, each part of
/// each host, and the URLs' other parts.
pub(super) const RELAY_MARKERS: [&str; 9] =
    ["zqlabel", "zqrelayalpha", "zqrelaybeta", "zqrelaygamma", "zqmarkhost", "invalid", "subscriberepos", "8443", "wss://"];

/// Whether `text` holds any relay marker (case-insensitive).
pub(super) fn relay_marker_in(text: &str) -> Option<&'static str> {
    let lower = text.to_lowercase();
    RELAY_MARKERS.into_iter().find(|m| lower.contains(m))
}

/// The configured relays the reader counts: one per URL with a host, once
/// each.
#[test]
fn relays_counted_once() {
    let m = marker_relays();
    assert_eq!(status::relay_list(&m).len(), 3);
    assert!(!relay_hosts(&m).contains_key("not a url"));
}

/// T-P5: one line for the relays: how many receive, and the median of their
/// delays, with no relay named.
#[test]
fn t_p5_relay_line() {
    let configured: Vec<String> = ["a", "b", "c"].iter().map(|h| format!("relay-{h}")).collect();
    let mut f = feed();
    f.newest_published_sec = Some(1000);
    let words = |f: &FeedSummary| status::build(&observed(f), &configured).relays;
    assert_eq!(words(&f), "Not receiving from any relay right now");
    f.relays.insert("relay-a".into(), RelayFigure { last_heard_sec: 1000, delay: Some(Delay::Ms(100)) });
    f.relays.insert("relay-b".into(), RelayFigure { last_heard_sec: 999, delay: Some(Delay::Ms(2000)) });
    f.relays.insert("relay-c".into(), RelayFigure { last_heard_sec: 1000, delay: Some(Delay::Ms(300)) });
    assert_eq!(words(&f), "Receiving from 3 relays, 0.3 s behind the network");
    // One quiet for a minute: 2 of 3, the median of the two.
    f.relays.insert("relay-b".into(), RelayFigure { last_heard_sec: 900, delay: Some(Delay::Ms(2000)) });
    assert_eq!(words(&f), "Receiving from 2 of 3 relays, 0.1 s behind the network");
    // A relay not configured counts for nothing.
    f.relays.insert("other".into(), RelayFigure { last_heard_sec: 1000, delay: Some(Delay::Ms(9000)) });
    assert_eq!(words(&f), "Receiving from 2 of 3 relays, 0.1 s behind the network");
    // Commits with no time yet, and the top bucket.
    let mut g = feed();
    g.newest_published_sec = Some(1000);
    g.relays.insert("relay-a".into(), RelayFigure { last_heard_sec: 1000, delay: None });
    assert_eq!(words(&g), "Receiving from 1 of 3 relays (no commits to time yet)");
    g.relays.insert("relay-a".into(), RelayFigure { last_heard_sec: 1000, delay: Some(Delay::OverTenMinutes) });
    assert_eq!(words(&g), "Receiving from 1 of 3 relays, more than 10 minutes behind the network");
    let one = vec!["relay-a".to_string()];
    assert_eq!(status::build(&observed(&g), &one).relays, "Receiving from 1 relay, more than 10 minutes behind the network");
    // The page shows no status block: neither line, nor any relay.
    f.input_per_sec = Some(812.4);
    let page = render(&status::build(&observed(&f), &configured), &PublicPageConfig::default());
    for gone in ["Receiving from", "behind the network", "812", "<span class=\"l\">Relays</span>", "relay-a", "relay-b"] {
        assert!(!page.contains(gone), "{gone}: {page}");
    }
}

/// Redis down or the audit latch set: the page says nothing about it.
#[test]
fn t_p5_no_problem_notice() {
    let f = feed();
    for (redis_ok, audit_latched) in [(false, false), (true, true)] {
        let o = Observed { redis_ok, audit_latched, ..observed(&f) };
        let page = render(&status::build(&o, &[]), &PublicPageConfig::default());
        for word in ["having a problem", "Redis", "redis", "audit", "Audit"] {
            assert!(!page.contains(word), "{word}: {page}");
        }
    }
}

/// T-P6: relay information only. The snapshot has these fields and no other
/// (so nothing fed from the consumer's lag or a watermark can be added
/// unnoticed); the page shows no relay's name, host, URL or label, no
/// processing status, and none of the removed sections.
#[test]
fn t_p6_relay_information_only() {
    let StatusSnapshot { db_ok: _, problem: _, relays: _, input_per_sec: _ } = snapshot();
    let m = marker_relays();
    let mut f = feed();
    for host in status::relay_list(&m) {
        f.relays.insert(host, RelayFigure { last_heard_sec: 1000, delay: Some(Delay::Ms(1200)) });
    }
    f.newest_published_sec = Some(1000);
    let s = status::build(&observed(&f), &status::relay_list(&m));
    assert_eq!(s.relays, "Receiving from 3 relays, 1.2 s behind the network");
    let page = render(&s, &PublicPageConfig::default());
    assert_eq!(relay_marker_in(&page), None, "{page}");
    let lower = page.to_lowercase();
    for word in ["keeping up", "catching up", "backlog", "lag", "moderation", "labels applied", "public data", "methods", "lexicons", "stored", "member here", "rate limit", "burst"] {
        assert!(!lower.contains(word), "{word}: {page}");
    }
    let without_relays = page.replace("behind the network", "");
    assert!(!without_relays.to_lowercase().contains("behind"), "{page}");
}

/// T-P9: the operator's text is escaped; a bad source URL fails validation;
/// the fixed line; the source link only when set.
#[test]
fn t_p9_escaping_and_the_fixed_text() {
    let p = PublicPageConfig {
        about: "<script>x</script>\n\nsecond paragraph".to_string(),
        contact: "mailto:ops@example.test".to_string(),
        clients: vec![PublicPageClient { name: "<b>App</b>".to_string(), url: "https://app.example.test/".to_string() }],
        ..PublicPageConfig::default()
    };
    let page = render(&snapshot(), &p);
    assert!(!page.contains("<script>x"), "{page}");
    assert!(page.contains("&#60;script&#62;x&#60;/script&#62;") || page.contains("&lt;script&gt;x&lt;/script&gt;"), "{page}");
    assert!(page.contains("<p>second paragraph</p>"), "{page}");
    assert!(!page.contains("<b>App</b>"));
    assert!(page.contains("href=\"mailto:ops@example.test\""), "{page}");
    let fixed_line = format!("<strong>{}</strong>, version {}.", page::PRODUCT, env!("CARGO_PKG_VERSION"));
    assert!(page.contains(&fixed_line), "{page}");
    assert_eq!(page::PRODUCT, "Horizon-Prism, an AT Protocol AppView written in Rust");
    assert!(!page.contains("Source"), "no source link by default");
    let with_source = render(&snapshot(), &PublicPageConfig { source_url: "https://code.example.test/prism".to_string(), ..PublicPageConfig::default() });
    assert!(with_source.contains("<a href=\"https://code.example.test/prism\">"), "{with_source}");
    assert!(page.contains(&format!("<title>{HOST}</title>")), "{page}");

    let problems = |p: &PublicPageConfig| {
        let mut out = Vec::new();
        config_problems(p, &mut out);
        out
    };
    let bad = PublicPageConfig { source_url: "javascript:alert(1)".to_string(), ..PublicPageConfig::default() };
    assert_eq!(problems(&bad), vec!["public_page.source_url must be an https: URL".to_string()]);
    let p = PublicPageConfig { clients: vec![PublicPageClient { name: "x".into(), url: "http://x.example".into() }], ..PublicPageConfig::default() };
    assert_eq!(problems(&p).len(), 1);
    let p = PublicPageConfig { live_max_viewers: 0, about: "a".repeat(ABOUT_MAX + 1), ..PublicPageConfig::default() };
    assert_eq!(problems(&p).len(), 2);
    assert!(problems(&PublicPageConfig::default()).is_empty());
    assert_eq!(contact_link("javascript:alert(1)"), None);
    assert_eq!(contact_link("ops at example"), None);
    assert_eq!(contact_link("https://example.test/contact").as_deref(), Some("https://example.test/contact"));
}

/// FNV-1a, 64 bits: the source check below compares words by hash, so the
/// names it looks for are not written in this directory.
fn fnv(word: &str) -> u64 {
    word.bytes().fold(0xcbf2_9ce4_8422_2325, |h, b| (h ^ u64::from(b)).wrapping_mul(0x0100_0000_01b3))
}

/// The text of every file under `src/public_page/`.
pub(super) fn source_words() -> Vec<(String, String)> {
    let dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src/public_page");
    let mut out = Vec::new();
    let mut stack = vec![dir];
    while let Some(d) = stack.pop() {
        for entry in std::fs::read_dir(&d).expect("read dir") {
            let path = entry.expect("entry").path();
            if path.is_dir() {
                stack.push(path);
            } else {
                let text = std::fs::read_to_string(&path).expect("read");
                out.push((path.display().to_string(), text));
            }
        }
    }
    out
}

/// T-P15 (the page's part): no file under `src/public_page/` names another
/// project, a community or a label issuer, or reads a private table's label.
#[test]
fn t_p15_source_checks() {
    // Hashes of lowercased words that must not appear: two other projects'
    // names, two communities' names, and the words for restored communities and label issuers of
    // the cut-back page. "horizon" is the check's own control, which must.
    const FORBIDDEN: [u64; 8] = [
        0x3692_12c4_9f94_1ac1,
        0xf41e_e7de_39e5_6a02,
        0x9eb5_3b8b_cd2a_c9b9,
        0x1930_e459_a2d8_f359,
        0xde52_0b3e_ae32_f67c,
        0xa1f2_57e7_0292_f8ad,
        0x580d_e6fb_2477_89ce,
        0x1729_39be_f71f_0c27,
    ];
    const CONTROL: u64 = 0x5475_4a77_220a_7e58;
    let files = source_words();
    assert!(!files.is_empty());
    let mut control = false;
    for (path, text) in &files {
        for word in text.split(|c: char| !c.is_ascii_alphanumeric()).filter(|w| !w.is_empty()) {
            let h = fnv(&word.to_ascii_lowercase());
            assert!(!FORBIDDEN.contains(&h), "{path}: a forbidden word");
            control |= h == CONTROL;
        }
        assert!(!text.contains(concat!("admin_table", "_labels")), "{path} reads private table labels");
    }
    assert!(control, "the hash check found its control word");
}

/// The mark as the site's icon: `/favicon.svg` and `/favicon.ico` are the
/// same SVG, served even with the page off (the admin pages link it), never
/// the XRPC fallback; both base templates link it, and neither carries the
/// old mark.
#[tokio::test]
async fn the_favicon() {
    let pool = lazy_pool();
    let (state, _drainer) = app_state(&pool);
    // The page off: only the icon's routes merged, as `serve` does.
    let base = serve_app(finalized(&state, Some(favicon_router()))).await;
    let client = reqwest::Client::new();
    for path in ["/favicon.svg", "/favicon.ico"] {
        let r = client.get(format!("{base}{path}")).send().await.expect("icon");
        assert_eq!(r.status().as_u16(), 200, "{path}");
        assert_eq!(r.headers()["content-type"], "image/svg+xml", "{path}");
        assert_eq!(r.headers()["cache-control"], FAVICON_CACHE, "{path}");
        assert_eq!(r.headers()["x-content-type-options"], "nosniff", "{path}");
        assert_eq!(r.text().await.expect("svg"), FAVICON, "{path}");
    }
    // `img-src 'self'` covers it in both policies.
    assert!(CSP.contains("img-src 'self'") && CSP_LIVE.contains("img-src 'self'"));
    let link = "<link rel=\"icon\" href=\"/favicon.svg\" type=\"image/svg+xml\">";
    let admin_base = include_str!("../admin/templates/base.html");
    let public_base = include_str!("templates/public_base.html");
    let old = concat!("M12 2 3", " 20h18z");
    for (name, text) in [("admin base", admin_base), ("public base", public_base)] {
        assert!(text.contains(link), "{name}");
        assert!(!text.contains(old), "{name} carries the old mark");
        assert!(text.contains("<path d=\"M8 15.5 12 6l4 9.5\"/>"), "{name} carries the mark");
    }
    assert!(render(&snapshot(), &PublicPageConfig::default()).contains(link));
}
