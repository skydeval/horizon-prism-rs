//! The web admin UI (docs/internal/design/admin-ui.md), slice S1: the
//! shell under `/admin`, first-run setup, operator sign-in with ATProto
//! OAuth and the host re-check, CSRF and confirmations, the members screen
//! (#735), the audit log and the operators page; and Home's live feed of
//! what Prism indexes (admin-ui-live-feed.md). Slice S2: the settings
//! pages, the saved settings layered over `app.toml`, the live values and
//! the restart (`settings`, `pages::settings`). Slice S3: Health, Jobs,
//! Problems and Blocks (Farsight), the checks and Home's tiles over the
//! in-process metrics (`health`). Slice S4: Stats, with the disks and
//! counts (`stats`), endpoint usage (`usage`) and the Disk check. Slice S5:
//! Labels, Look up, and the per-member actions (sync again, Farsight read
//! and forget, list clean-up). Each operator's Appearance (`theme`, #810).
//!
//! Served by `serve` itself on its own origin, merged before
//! `xrpc::router::finalize`, so it sits inside the global per-IP limit
//! and the CORS layer (which never allows credentials). Pages are askama
//! templates and work without JavaScript; Home alone loads one
//! same-origin script for its live feed. Every change is a command in
//! `admin_commands` with `requested_by`, run by the executor through the
//! same code as the CLI's. Reads query the database directly.

pub mod auth;
pub mod health;
pub mod live;
pub mod oauth;
pub mod operators;
pub mod pages;
pub mod settings;
pub mod setup_code;
pub mod stats;
pub mod theme;
pub mod usage;

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
use settings::SettingsContext;

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

/// What the members screen shows of `[community]` (membership design §8),
/// read from the live values (admin-ui design §8.5).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MembersInfo {
    pub auto_enroll: bool,
    pub max_members: u64,
    pub enroll_pds_patterns: Vec<String>,
}

/// The admin UI's settings, from the configuration.
#[derive(Clone)]
pub struct AdminSettings {
    pub public_hostname: String,
    pub trusted_proxy: IpAddr,
    pub session_idle: Duration,
    pub session_max: Duration,
    pub setup_code_file: PathBuf,
    /// The settings pages' view of the configuration (§8).
    pub settings: SettingsContext,
    /// The paths the disk figures read (§10.1).
    pub disks: stats::DiskPaths,
}

impl AdminSettings {
    /// From the configuration the process started with; `settings` holds
    /// it with the file it came from.
    #[must_use]
    pub fn from_config(cfg: &AppConfig, settings: SettingsContext) -> Self {
        Self {
            public_hostname: cfg.env.public_hostname.clone(),
            trusted_proxy: cfg.file.http.trusted_proxy,
            session_idle: Duration::from_secs(cfg.file.admin.session_idle_secs),
            session_max: Duration::from_secs(cfg.file.admin.session_max_secs),
            setup_code_file: setup_code::file_path(&cfg.env.data_dir),
            settings,
            disks: stats::DiskPaths::standard(&cfg.env.data_dir),
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
    pub settings: SettingsContext,
    pub limits: Limits,
    /// The live feed's hub (admin-ui-live-feed design §4).
    pub live: live::Hub,
    /// What Health, Jobs, Problems and Blocks read (§9); empty until
    /// [`AdminState::with_health`].
    pub health: health::HealthInputs,
    /// Farsight's own report, kept 30 seconds (§6.8).
    pub farsight_report: pages::farsight::ReportCache,
    /// The paths the disk figures read (§10.1).
    pub disks: stats::DiskPaths,
    /// Endpoint usage since the start (§10.3); empty until
    /// [`AdminState::with_usage`].
    pub usage: usage::Usage,
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
            settings: settings.settings,
            limits: Limits::default(),
            live,
            health: health::HealthInputs::default(),
            farsight_report: pages::farsight::ReportCache::default(),
            disks: settings.disks,
            usage: usage::Usage::default(),
        }))
    }

    /// The same state, reading `usage` for the stats page: the counters
    /// `xrpc::router::finalize_with` writes. Call it before the state is
    /// cloned.
    #[must_use]
    pub fn with_usage(mut self, usage: usage::Usage) -> Self {
        match Arc::get_mut(&mut self.0) {
            Some(inner) => inner.usage = usage,
            None => tracing::error!(event = "admin_usage_not_set", "the admin state was shared before its usage counters were set"),
        }
        self
    }

    /// The same state, reading `inputs` for Health and its sibling pages.
    /// Call it before the state is cloned.
    #[must_use]
    pub fn with_health(mut self, inputs: health::HealthInputs) -> Self {
        match Arc::get_mut(&mut self.0) {
            Some(inner) => inner.health = inputs,
            None => tracing::error!(event = "admin_health_not_set", "the admin state was shared before its health inputs were set"),
        }
        self
    }

    /// `[community]` as it applies now.
    #[must_use]
    pub fn members(&self) -> MembersInfo {
        let v = self.settings.live.load();
        MembersInfo {
            auto_enroll: v.auto_enroll,
            max_members: u64::from(v.max_members),
            enroll_pds_patterns: v.enroll_pds_patterns.clone(),
        }
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
        .route("/members/resync", route(post(pages::members::resync)))
        .route("/members/farsight-prime", route(post(pages::members::farsight_prime)))
        .route("/members/farsight-forget", route(post(pages::members::farsight_forget)))
        .route("/members/lists-purge", route(post(pages::members::lists_purge)))
        .route("/labels", route(get(pages::labels::page)))
        .route("/labels/add", route(post(pages::labels::add)))
        .route("/labels/remove", route(post(pages::labels::remove)))
        .route("/lookup", route(get(pages::lookup::page)))
        .route("/commands/{id}", route(get(pages::commands::page)))
        .route("/audit", route(get(pages::audit::page)))
        .route("/activity", route(get(pages::audit::moved)))
        .route("/operators", route(get(pages::operators::page)))
        .route("/operators/add", route(post(pages::operators::add)))
        .route("/operators/remove", route(post(pages::operators::remove)))
        .route("/operators/hosts/add", route(post(pages::operators::host_add)))
        .route("/operators/hosts/remove", route(post(pages::operators::host_remove)))
        .route("/settings", route(get(pages::settings::index)))
        .route("/settings/installed", route(get(pages::settings::installed)))
        .route("/settings/running", route(get(pages::settings::running)))
        .route("/settings/reset", route(post(pages::settings::reset)))
        .route("/settings/keep", route(post(pages::settings::keep)))
        .route("/settings/public-scope", route(post(pages::settings::public_scope)))
        .route("/settings/appearance", route(get(pages::appearance::page).post(pages::appearance::save)))
        .route("/settings/{group}", route(get(pages::settings::group).post(pages::settings::save)))
        .route("/restart", route(post(pages::settings::restart)))
        .route("/health", route(get(pages::health::page)))
        .route("/health/resync", route(post(pages::health::resync)))
        .route("/stats", route(get(pages::stats::page)))
        .route("/jobs", route(get(pages::jobs::page)))
        .route("/jobs/retry", route(post(pages::jobs::retry)))
        .route("/jobs/scope-check", route(post(pages::jobs::scope_check)))
        .route("/jobs/scope-rebuild", route(post(pages::jobs::scope_rebuild)))
        .route("/problems", route(get(pages::problems::page)))
        .route("/problems/replay", route(post(pages::problems::replay)))
        .route("/problems/dismiss", route(post(pages::problems::dismiss)))
        .route("/problems/sweep", route(post(pages::problems::sweep)))
        .route("/farsight", route(get(pages::farsight::page)))
        .route("/communities", route(get(pages::communities::page)))
        .route("/communities/switch", route(post(pages::communities::switch)))
        .route("/communities/save", route(post(pages::communities::save)))
        .route("/communities/toggle", route(post(pages::communities::toggle)))
        .route("/communities/remove", route(post(pages::communities::remove)))
        .route("/communities/keep", route(post(pages::communities::keep)))
        .route("/refract", route(get(pages::refract::page)))
        .route("/refract/add", route(post(pages::refract::add)))
        .route("/refract/check", route(post(pages::refract::check)))
        .route("/refract/end", route(post(pages::refract::end)))
        .route("/refract/{did}", route(get(pages::refract::account)))
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
