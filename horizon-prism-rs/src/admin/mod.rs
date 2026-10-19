//! The web admin UI (docs/internal/design/admin-ui.md), slice S1: the
//! shell under `/admin`, first-run setup, operator sign-in with ATProto
//! OAuth and the host re-check, CSRF and confirmations, the members screen
//! (#735), the audit log and the operators page; and Home's live feed of
//! what Prism indexes (admin-ui-live-feed.md).
//!
//! Served by `serve` itself on its own origin, merged before
//! `xrpc::router::finalize`, so it sits inside the global per-IP limit
//! and the CORS layer (which never allows credentials). Pages are askama
//! templates and work without JavaScript; Home alone loads one
//! same-origin script for its live feed. Every change is a command in
//! `admin_commands` with `requested_by`, run by the executor through the
//! same code as the CLI's. Reads query the database directly.

pub mod auth;
pub mod live;
pub mod oauth;
pub mod operators;
pub mod pages;
pub mod setup_code;

use std::net::IpAddr;
use std::ops::Deref;
use std::path::PathBuf;
use std::sync::{Arc, LazyLock};
use std::time::Duration;

use axum::Router;
use axum::extract::{DefaultBodyLimit, Path, Request};
use axum::http::header::{CACHE_CONTROL, CONTENT_TYPE};
use axum::http::{HeaderName, HeaderValue, StatusCode};
use axum::middleware::{self, Next};
use axum::response::{IntoResponse, Response};
use axum::routing::{MethodRouter, get, post};
use proto_blue::common::fetch::FetchHandler;
use proto_blue::oauth::OAuthClient;
use sqlx::PgPool;
use tokio::task::JoinHandle;

use crate::aggregations::ScopeState;
use crate::config::AppConfig;
use auth::Limits;
use operators::OperatorIdentity;

/// Request bodies the admin router accepts (§3).
pub const BODY_LIMIT: usize = 64 * 1024;
/// The one stylesheet (§4), compiled in.
pub const CSS: &str = include_str!("static/admin.css");

/// The stylesheet's path: its content hash in the name, so it may be
/// cached for a year (§4).
pub static CSS_HREF: LazyLock<String> =
    LazyLock::new(|| format!("/admin/static/admin-{}.css", &blake3::hash(CSS.as_bytes()).to_hex()[..12]));

/// Home's live-feed script (admin-ui-live-feed design §5.3), compiled in.
pub const JS: &str = include_str!("static/live.js");

/// The script's path, hashed like the stylesheet's.
pub static JS_HREF: LazyLock<String> =
    LazyLock::new(|| format!("/admin/static/live-{}.js", &blake3::hash(JS.as_bytes()).to_hex()[..12]));

/// The `Content-Security-Policy` of every `/admin` response (§3) but
/// Home's.
pub const CSP: &str =
    "default-src 'none'; style-src 'self'; img-src 'self'; form-action 'self'; frame-ancestors 'none'; base-uri 'none'";

/// Home's policy (admin-ui-live-feed design §8): its one script and its
/// stream, from this origin only.
pub const CSP_LIVE: &str = "default-src 'none'; style-src 'self'; img-src 'self'; script-src 'self'; connect-src 'self'; form-action 'self'; frame-ancestors 'none'; base-uri 'none'";

/// Every `/admin` response's `Cache-Control` unless its handler set one.
pub const NO_STORE: &str = "no-store, no-transform";

/// The response marker by which Home asks for [`CSP_LIVE`]; the headers
/// middleware stays the only writer of the header.
#[derive(Debug, Clone, Copy)]
pub struct LivePage;

/// What the members screen shows of `[community]` (membership design §8).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MembersInfo {
    pub auto_enroll: bool,
    pub max_members: u64,
    pub enroll_pds_patterns: Vec<String>,
}

/// The admin UI's settings, from the configuration.
#[derive(Debug, Clone)]
pub struct AdminSettings {
    pub public_hostname: String,
    pub trusted_proxy: IpAddr,
    pub session_idle: Duration,
    pub session_max: Duration,
    pub setup_code_file: PathBuf,
    pub members: MembersInfo,
}

impl AdminSettings {
    #[must_use]
    pub fn from_config(cfg: &AppConfig) -> Self {
        Self {
            public_hostname: cfg.env.public_hostname.clone(),
            trusted_proxy: cfg.file.http.trusted_proxy,
            session_idle: Duration::from_secs(cfg.file.admin.session_idle_secs),
            session_max: Duration::from_secs(cfg.file.admin.session_max_secs),
            setup_code_file: setup_code::file_path(&cfg.env.data_dir),
            members: MembersInfo {
                auto_enroll: cfg.file.community.auto_enroll,
                max_members: u64::from(cfg.file.community.max_members),
                enroll_pds_patterns: cfg.file.community.enroll_pds_patterns.clone(),
            },
        }
    }
}

/// Everything the admin pages use.
pub struct Inner {
    pub pool: PgPool,
    pub scope: ScopeState,
    pub identity: Arc<dyn OperatorIdentity>,
    pub oauth: Arc<OAuthClient>,
    pub public_hostname: String,
    pub trusted_proxy: IpAddr,
    pub session_idle: Duration,
    pub session_max: Duration,
    pub setup_code_file: PathBuf,
    pub members: MembersInfo,
    pub limits: Limits,
    /// The live feed's hub (admin-ui-live-feed design §4).
    pub live: live::Hub,
}

/// The admin router's state. Cheap to clone.
#[derive(Clone)]
pub struct AdminState(Arc<Inner>);

impl Deref for AdminState {
    type Target = Inner;

    fn deref(&self) -> &Inner {
        &self.0
    }
}

impl AdminState {
    /// The state over `pool`; OAuth fetches go through `fetcher` (a
    /// [`oauth::NetFetch`] in `serve`); `live` is the hub whose tap the
    /// writer was given.
    #[must_use]
    pub fn new(
        pool: PgPool,
        scope: ScopeState,
        identity: Arc<dyn OperatorIdentity>,
        fetcher: Arc<dyn FetchHandler>,
        settings: AdminSettings,
        live: live::Hub,
    ) -> Self {
        let oauth = Arc::new(oauth::client(&settings.public_hostname, fetcher));
        Self(Arc::new(Inner {
            pool,
            scope,
            identity,
            oauth,
            public_hostname: settings.public_hostname,
            trusted_proxy: settings.trusted_proxy,
            session_idle: settings.session_idle,
            session_max: settings.session_max,
            setup_code_file: settings.setup_code_file,
            members: settings.members,
            limits: Limits::default(),
            live,
        }))
    }

    /// Forget the limits' idle keys every minute, for as long as `serve`
    /// runs.
    #[must_use]
    pub fn spawn_housekeeping(&self) -> JoinHandle<()> {
        let state = self.clone();
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(Duration::from_secs(60)).await;
                state.limits.retain_recent();
            }
        })
    }
}

/// The §3 headers on every `/admin` response. The hashed stylesheet and
/// script and the live stream set their own `Cache-Control`; everything
/// else is `no-store, no-transform`. Home's `200` carries [`LivePage`] and
/// gets [`CSP_LIVE`]; every other response [`CSP`].
async fn headers(request: Request, next: Next) -> Response {
    let mut response = next.run(request).await;
    let live = response.extensions().get::<LivePage>().is_some();
    let h = response.headers_mut();
    if !h.contains_key(CACHE_CONTROL) {
        h.insert(CACHE_CONTROL, HeaderValue::from_static(NO_STORE));
    }
    let csp = if live { CSP_LIVE } else { CSP };
    h.insert(HeaderName::from_static("content-security-policy"), HeaderValue::from_static(csp));
    h.insert(HeaderName::from_static("x-frame-options"), HeaderValue::from_static("DENY"));
    // `same-origin`, not `no-referrer`: under `no-referrer` browsers send
    // `Origin: null` on form POSTs (Fetch, "append a request Origin
    // header"), which the Origin check refuses. `same-origin` still sends
    // nothing to other sites.
    h.insert(HeaderName::from_static("referrer-policy"), HeaderValue::from_static("same-origin"));
    h.insert(HeaderName::from_static("x-content-type-options"), HeaderValue::from_static("nosniff"));
    response
}

/// The hashed stylesheet and script (§4; admin-ui-live-feed design §5.3).
async fn static_file(Path(file): Path<String>) -> Response {
    let path = format!("/admin/static/{file}");
    let (content_type, body) = if path == *CSS_HREF {
        ("text/css; charset=utf-8", CSS)
    } else if path == *JS_HREF {
        ("text/javascript; charset=utf-8", JS)
    } else {
        return StatusCode::NOT_FOUND.into_response();
    };
    (
        [
            (CONTENT_TYPE, HeaderValue::from_static(content_type)),
            (CACHE_CONTROL, HeaderValue::from_static("public, max-age=31536000, immutable")),
        ],
        body,
    )
        .into_response()
}

async fn method_not_allowed() -> Response {
    (StatusCode::METHOD_NOT_ALLOWED, "method not allowed").into_response()
}

/// A route's methods, with a fallback of its own, so the XRPC router's
/// wrong-verb answer never reaches an admin path.
fn route(m: MethodRouter<AdminState>) -> MethodRouter<AdminState> {
    m.fallback(method_not_allowed)
}

/// The admin router, nested at `/admin`. Merge it before `finalize`.
pub fn router(state: AdminState) -> Router {
    let admin = Router::new()
        .route("/", route(get(pages::home::page)))
        .route("/tiles", route(get(pages::home::tiles)))
        .route("/live", route(get(live::stream::open)))
        .route("/static/{file}", route(get(static_file)))
        .route("/setup", route(get(pages::setup::form).post(pages::setup::submit)))
        .route("/login", route(get(pages::login::form).post(pages::login::submit)))
        .route("/logout", route(post(pages::login::logout)))
        .route("/oauth/client-metadata.json", route(get(pages::login::client_metadata)))
        .route("/oauth/callback", route(get(pages::login::callback)))
        .route("/members", route(get(pages::members::page)))
        .route("/members/add", route(post(pages::members::add)))
        .route("/members/remove", route(post(pages::members::remove)))
        .route("/members/deny", route(post(pages::members::deny)))
        .route("/members/allow", route(post(pages::members::allow)))
        .route("/commands/{id}", route(get(pages::commands::page)))
        .route("/audit", route(get(pages::audit::page)))
        .route("/activity", route(get(pages::audit::moved)))
        .route("/operators", route(get(pages::operators::page)))
        .route("/operators/add", route(post(pages::operators::add)))
        .route("/operators/remove", route(post(pages::operators::remove)))
        .route("/operators/hosts/add", route(post(pages::operators::host_add)))
        .route("/operators/hosts/remove", route(post(pages::operators::host_remove)))
        .fallback(pages::not_found)
        .layer(DefaultBodyLimit::max(BODY_LIMIT))
        .layer(middleware::from_fn(headers))
        .with_state(state);
    Router::new().nest("/admin", admin)
}

#[cfg(test)]
pub(crate) mod test_support;
#[cfg(test)]
mod tests;
