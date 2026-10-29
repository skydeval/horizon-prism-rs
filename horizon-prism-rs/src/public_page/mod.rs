//! The public page: a read-only page
//! at `/` on Prism's own origin, the same for every visitor. It says what
//! this instance is, its status (the database, how many relays it receives
//! from, the relays' input rate), and shows the relay events as they
//! arrive. It carries relay information only, and names no relay.
//!
//! Served by `serve` itself, merged before `xrpc::router::finalize`, so it
//! sits inside the global per-IP limit and the CORS layer. `/` and its
//! static files read only memory: the page's text is fixed at startup and
//! its status comes from a snapshot rebuilt every 15 s ([`status`]), so no
//! request to them touches the database. Every route answers a wrong verb
//! with `405`, never the XRPC router's JSON `400`. The live feed at `/live`
//! comes from a reader of its own on the firehose stream ([`reader`],
//! [`decode`]), released by one publisher ([`live`]) to capped viewers
//! ([`viewers`]).

pub mod decode;
pub mod live;
pub mod page;
pub mod reader;
pub mod status;
pub mod viewers;

use std::collections::BTreeMap;
use std::net::{IpAddr, Ipv6Addr, SocketAddr};
use std::sync::{Arc, LazyLock};

use arc_swap::ArcSwap;
use axum::Router;
use axum::extract::{Path, Request};
use axum::http::header::{CACHE_CONTROL, CONTENT_TYPE};
use axum::http::{HeaderMap, HeaderName, HeaderValue, StatusCode};
use axum::middleware::{self, Next};
use axum::response::{IntoResponse, Response};
use axum::routing::{MethodRouter, get};

use crate::config::PublicPageConfig;
use crate::xrpc::RateLimits;
use crate::xrpc::errors::XrpcError;
use crate::xrpc::rate_limit::{Class, client_ip};
use status::StatusSnapshot;

/// The page's stylesheet: the admin stylesheet's bytes, on a path of its
/// own, so the page works with the admin UI off.
pub static CSS_HREF: LazyLock<String> =
    LazyLock::new(|| format!("/static/site-{}.css", &blake3::hash(crate::admin::CSS.as_bytes()).to_hex()[..12]));

/// The `Content-Security-Policy` of every public response but `/` with the
/// live feed on.
pub const CSP: &str = crate::admin::CSP;

/// `/`'s policy with the live feed on: its one script and its stream, from
/// this origin only.
pub const CSP_LIVE: &str = crate::admin::CSP_LIVE;

/// The live feed's script (§6.6), compiled in.
pub const JS: &str = include_str!("static/public-live.js");

/// The script's path, hashed like the stylesheet's.
pub static JS_HREF: LazyLock<String> = LazyLock::new(|| format!("/static/public-live-{}.js", &blake3::hash(JS.as_bytes()).to_hex()[..12]));

/// The response marker by which `/` asks for [`CSP_LIVE`].
#[derive(Debug, Clone, Copy)]
pub struct LivePage;

/// `Cache-Control` of `GET /`'s `200`: an edge may serve it to everyone for
/// 30 s, since it is the same for every visitor.
pub const PAGE_CACHE: &str = "public, max-age=30";

/// Every other public response's `Cache-Control` unless its handler set one,
/// so an edge never serves one visitor's refusal to everyone.
pub const NO_STORE: &str = "no-store";

/// The hashed static files' `Cache-Control`.
pub const IMMUTABLE: &str = "public, max-age=31536000, immutable";

/// The longest `about`, in characters.
pub const ABOUT_MAX: usize = 4000;
/// The most `clients`.
pub const CLIENTS_MAX: usize = 20;
/// The longest one-line value (`name`, `contact`, a client's name).
pub const LINE_MAX: usize = 200;
/// The longest URL.
pub const URL_MAX: usize = 2000;

/// The problems of `[public_page]`, for config validation.
pub fn config_problems(p: &PublicPageConfig, out: &mut Vec<String>) {
    if !p.source_url.is_empty() && !is_https(&p.source_url) {
        out.push("public_page.source_url must be an https: URL".to_string());
    }
    if p.about.chars().count() > ABOUT_MAX {
        out.push(format!("public_page.about must be at most {ABOUT_MAX} characters"));
    }
    for (key, value) in [("name", &p.name), ("contact", &p.contact)] {
        if value.chars().count() > LINE_MAX || value.contains(['\n', '\r']) {
            out.push(format!("public_page.{key} must be one line of at most {LINE_MAX} characters"));
        }
    }
    if p.clients.len() > CLIENTS_MAX {
        out.push(format!("public_page.clients may list at most {CLIENTS_MAX} apps"));
    }
    for c in &p.clients {
        if c.name.trim().is_empty() || c.name.chars().count() > LINE_MAX || c.name.contains(['\n', '\r']) {
            out.push(format!("public_page.clients: every app needs a one-line name of 1 to {LINE_MAX} characters"));
        }
        if !is_https(&c.url) {
            out.push(format!("public_page.clients[{}]: url must be an https: URL", c.name));
        }
    }
    if p.live_max_viewers == 0 {
        out.push("public_page.live_max_viewers must be at least 1".to_string());
    }
}

/// An absolute `https:` URL with a host, of at most [`URL_MAX`] bytes.
#[must_use]
pub fn is_https(value: &str) -> bool {
    value.len() <= URL_MAX && url::Url::parse(value).is_ok_and(|u| u.scheme() == "https" && u.host_str().is_some_and(|h| !h.is_empty()))
}

/// `contact` as a link target, when it is an `https:` or `mailto:` URL.
#[must_use]
pub fn contact_link(contact: &str) -> Option<String> {
    if is_https(contact) {
        return Some(contact.to_string());
    }
    let u = url::Url::parse(contact).ok()?;
    (u.scheme() == "mailto" && !u.path().is_empty() && contact.len() <= URL_MAX).then(|| contact.to_string())
}

/// A relay's key for the reader's per-relay figures: the host of its URL
/// (a key of `firehose.relay_labels`), lowercased. Counted, never shown.
#[must_use]
pub fn relay_host(url: &str) -> Option<String> {
    let host = url::Url::parse(url.trim()).ok()?.host_str()?.to_ascii_lowercase();
    (!host.is_empty()).then_some(host)
}

/// Relay URL → host, for every configured relay whose URL has one.
#[must_use]
pub fn relay_hosts(relay_labels: &BTreeMap<String, String>) -> BTreeMap<String, String> {
    relay_labels.keys().filter_map(|url| relay_host(url).map(|h| (url.clone(), h))).collect()
}

/// The key a per-address limit counts under: an IPv4 address, or an IPv6
/// address's /48.
#[must_use]
pub fn address_key(ip: IpAddr) -> IpAddr {
    match ip {
        IpAddr::V4(_) => ip,
        IpAddr::V6(v6) => v6.to_ipv4_mapped().map_or_else(
            || IpAddr::V6(Ipv6Addr::from(u128::from(v6) & !((1u128 << 80) - 1))),
            IpAddr::V4,
        ),
    }
}

/// What the page says that is fixed at startup.
#[derive(Debug, Clone, Default)]
pub struct Fixed {
    /// `public_page.name`, else the public hostname.
    pub name: String,
    /// The operator's text, in paragraphs.
    pub about: Vec<String>,
    pub contact: String,
    pub contact_link: Option<String>,
    pub source_url: Option<String>,
    /// `(name, url)`.
    pub clients: Vec<(String, String)>,
    /// `/live` is mounted: the page shows the feed and loads its script.
    pub live_feed: bool,
}

impl Fixed {
    /// From the configuration, `hostname` being `APP_PUBLIC_HOSTNAME`.
    #[must_use]
    pub fn from_config(p: &PublicPageConfig, hostname: &str) -> Self {
        Self {
            name: if p.name.trim().is_empty() { hostname.to_string() } else { p.name.trim().to_string() },
            about: paragraphs(&p.about),
            contact: p.contact.trim().to_string(),
            contact_link: contact_link(p.contact.trim()),
            source_url: (!p.source_url.is_empty()).then(|| p.source_url.clone()),
            clients: p.clients.iter().map(|c| (c.name.trim().to_string(), c.url.clone())).collect(),
            live_feed: p.live_feed,
        }
    }
}

/// Plain text cut into paragraphs at blank lines; lines within one are
/// joined by a space.
#[must_use]
pub fn paragraphs(text: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut current: Vec<&str> = Vec::new();
    for line in text.lines() {
        if line.trim().is_empty() {
            if !current.is_empty() {
                out.push(current.join(" "));
                current.clear();
            }
        } else {
            current.push(line.trim());
        }
    }
    if !current.is_empty() {
        out.push(current.join(" "));
    }
    out
}

/// The state of `GET /`: memory only. It holds no pool and no Redis
/// connection (T-P3).
#[derive(Clone)]
pub struct PageState {
    pub snapshot: Arc<ArcSwap<StatusSnapshot>>,
    pub fixed: Arc<Fixed>,
    pub limits: RateLimits,
    pub trusted_proxy: IpAddr,
    /// The admin UI's "Public page name", read on every request; empty or
    /// absent leaves [`Fixed::name`].
    pub name: Option<NameFn>,
}

/// The page's name as the admin UI has it now.
pub type NameFn = Arc<dyn Fn() -> String + Send + Sync>;

impl PageState {
    /// The page's title right now.
    #[must_use]
    pub fn title(&self) -> String {
        let live = self.name.as_ref().map(|f| f()).unwrap_or_default();
        let live = live.trim();
        if live.is_empty() || live.chars().count() > LINE_MAX || live.contains(['\n', '\r']) {
            self.fixed.name.clone()
        } else {
            live.to_string()
        }
    }
}

impl PageState {
    /// The client address, as every per-address rule keys it.
    #[must_use]
    pub fn client(&self, headers: &HeaderMap, peer: SocketAddr) -> IpAddr {
        let xff = headers.get("x-forwarded-for").and_then(|v| v.to_str().ok());
        client_ip(peer.ip(), xff, self.trusted_proxy)
    }

    /// Charge one request to `anon_read` for its address.
    ///
    /// # Errors
    ///
    /// The `429` to answer.
    #[allow(clippy::result_large_err)]
    pub fn charge(&self, headers: &HeaderMap, peer: SocketAddr) -> Result<(), Response> {
        self.limits.check_ip(Class::AnonRead, self.client(headers, peer)).map_err(|l| XrpcError::from(l).into_response())
    }
}

async fn method_not_allowed() -> Response {
    (StatusCode::METHOD_NOT_ALLOWED, "method not allowed").into_response()
}

/// A route's methods, with a fallback of its own, so the XRPC router's
/// wrong-verb answer never reaches a public path.
fn route<S: Clone + Send + Sync + 'static>(m: MethodRouter<S>) -> MethodRouter<S> {
    m.fallback(method_not_allowed)
}

/// The headers of every public response (§5).
async fn headers(request: Request, next: Next) -> Response {
    let mut response = next.run(request).await;
    // `/`'s `200` with the live feed on loads its one script.
    let live = response.extensions().get::<LivePage>().is_some();
    let h = response.headers_mut();
    if !h.contains_key(CACHE_CONTROL) {
        h.insert(CACHE_CONTROL, HeaderValue::from_static(NO_STORE));
    }
    let csp = if live { CSP_LIVE } else { CSP };
    h.insert(HeaderName::from_static("content-security-policy"), HeaderValue::from_static(csp));
    h.insert(HeaderName::from_static("x-frame-options"), HeaderValue::from_static("DENY"));
    h.insert(HeaderName::from_static("referrer-policy"), HeaderValue::from_static("same-origin"));
    h.insert(HeaderName::from_static("x-content-type-options"), HeaderValue::from_static("nosniff"));
    response
}

/// The hashed stylesheet and the live feed's script.
async fn static_file(Path(file): Path<String>) -> Response {
    let path = format!("/static/{file}");
    let (content_type, body) = if path == *CSS_HREF {
        ("text/css; charset=utf-8", crate::admin::CSS)
    } else if path == *JS_HREF {
        ("text/javascript; charset=utf-8", JS)
    } else {
        return StatusCode::NOT_FOUND.into_response();
    };
    (
        [(CONTENT_TYPE, HeaderValue::from_static(content_type)), (CACHE_CONTROL, HeaderValue::from_static(IMMUTABLE))],
        body,
    )
        .into_response()
}

/// The mark, as the site's icon: the same bytes at `/favicon.svg` and at
/// `/favicon.ico` (for browsers that ask for that name).
pub const FAVICON: &str = include_str!("static/favicon.svg");

/// The icon's `Cache-Control`: its path carries no hash, so a day.
pub const FAVICON_CACHE: &str = "public, max-age=86400";

async fn favicon() -> Response {
    (
        [(CONTENT_TYPE, HeaderValue::from_static("image/svg+xml")), (CACHE_CONTROL, HeaderValue::from_static(FAVICON_CACHE))],
        FAVICON,
    )
        .into_response()
}

/// The icon's two routes. `serve` merges them whatever `public_page`
/// says, since the admin pages link the icon too.
pub fn favicon_router() -> Router {
    Router::new()
        .route("/favicon.svg", route(get(favicon)))
        .route("/favicon.ico", route(get(favicon)))
        .layer(middleware::from_fn(headers))
}

/// The public routes, and `/live` when `live` is given. Merge them before
/// `finalize`; with `public_page.enabled` off `serve` merges none.
pub fn router(state: PageState, live: Option<viewers::LiveState>) -> Router {
    let page = Router::new()
        .route("/", route(get(page::index)))
        .route("/static/{file}", route(get(static_file)))
        .with_state(state);
    let page = match live {
        Some(l) => page.merge(Router::new().route("/live", route(get(viewers::open))).with_state(l)),
        None => page,
    };
    page.layer(middleware::from_fn(headers))
}

/// Whether the page is on right now (the admin UI's "Public page" switch).
pub type Switch = Arc<dyn Fn() -> bool + Send + Sync>;

async fn gate(axum::extract::State(on): axum::extract::State<Switch>, request: Request, next: Next) -> Response {
    if on() { next.run(request).await } else { StatusCode::NOT_FOUND.into_response() }
}

/// `routes` answered only while `on` says so; otherwise each is the same
/// empty `404` as a path with no route. Checked on every request, so the
/// switch applies at once, open live streams excepted (they end at their
/// next renewal).
pub fn gated(routes: Router, on: Switch) -> Router {
    routes.layer(middleware::from_fn_with_state(on, gate))
}

#[cfg(test)]
mod live_tests;
#[cfg(test)]
mod tests;
