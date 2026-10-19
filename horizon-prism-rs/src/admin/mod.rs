//! The web admin UI (docs/internal/design/admin-ui.md), slice S1: the
//! shell under `/admin`, first-run setup, operator sign-in with ATProto
//! OAuth and the host re-check, CSRF and confirmations, the members screen
//! (#735), the activity page and the operators page.
//!
//! Served by `serve` itself on its own origin, merged before
//! `xrpc::router::finalize`, so it sits inside the global per-IP limit
//! and the CORS layer (which never allows credentials). Pages are askama
//! templates with no JavaScript; every change is a command in
//! `admin_commands` with `requested_by`, run by the executor through the
//! same code as the CLI's. Reads query the database directly.

pub mod auth;
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

/// The `Content-Security-Policy` of every `/admin` response (§3).
pub const CSP: &str =
    "default-src 'none'; style-src 'self'; img-src 'self'; form-action 'self'; frame-ancestors 'none'; base-uri 'none'";

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
    /// [`oauth::NetFetch`] in `serve`).
    #[must_use]
    pub fn new(
        pool: PgPool,
        scope: ScopeState,
        identity: Arc<dyn OperatorIdentity>,
        fetcher: Arc<dyn FetchHandler>,
        settings: AdminSettings,
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

/// The §3 headers on every `/admin` response. The hashed stylesheet sets
/// its own `Cache-Control`; everything else is `no-store`.
async fn headers(request: Request, next: Next) -> Response {
    let mut response = next.run(request).await;
    let h = response.headers_mut();
    if !h.contains_key(CACHE_CONTROL) {
        h.insert(CACHE_CONTROL, HeaderValue::from_static("no-store"));
    }
    h.insert(HeaderName::from_static("content-security-policy"), HeaderValue::from_static(CSP));
    h.insert(HeaderName::from_static("x-frame-options"), HeaderValue::from_static("DENY"));
    h.insert(HeaderName::from_static("referrer-policy"), HeaderValue::from_static("no-referrer"));
    h.insert(HeaderName::from_static("x-content-type-options"), HeaderValue::from_static("nosniff"));
    response
}

async fn stylesheet(Path(file): Path<String>) -> Response {
    if format!("/admin/static/{file}") != *CSS_HREF {
        return StatusCode::NOT_FOUND.into_response();
    }
    (
        [
            (CONTENT_TYPE, HeaderValue::from_static("text/css; charset=utf-8")),
            (CACHE_CONTROL, HeaderValue::from_static("public, max-age=31536000, immutable")),
        ],
        CSS,
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
        .route("/static/{file}", route(get(stylesheet)))
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
        .route("/activity", route(get(pages::activity::page)))
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
mod tests;
