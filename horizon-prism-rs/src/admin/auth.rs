//! Sessions, CSRF, `Origin`, rate limits and the host re-check (admin-ui
//! design §5.4–§5.6).
//!
//! - The session cookie `__Host-prism_admin` holds 32 random bytes; only
//!   their BLAKE3 hash is stored. Idle and absolute expiry come from
//!   `[admin]`.
//! - Every request checks the session, that its DID is an operator, and
//!   that its recorded PDS host is allowed. A POST other than sign-out, or
//!   a request whose last check is older than 5 minutes, resolves the DID
//!   afresh: a changed endpoint or a host no longer allowed ends the
//!   session; a failed resolution refuses the request and keeps it.
//! - The session CSRF token is a keyed BLAKE3 of the cookie bytes, so it
//!   needs no storage and dies with the session. The setup and sign-in
//!   forms use a pre-session double-submit token instead.

use std::net::{IpAddr, SocketAddr};
use std::num::NonZeroU32;
use std::sync::Arc;
use std::time::Duration;

use axum::http::header::{COOKIE, ORIGIN, SET_COOKIE};
use axum::http::{HeaderMap, HeaderValue, StatusCode};
use axum::response::{IntoResponse, Redirect, Response};
use chrono::{DateTime, Utc};
use governor::{DefaultDirectRateLimiter, DefaultKeyedRateLimiter, Quota, RateLimiter};
use tokio::sync::Semaphore;

use super::operators::{endpoint_host, host_allowed, is_operator, normalize_endpoint, patterns};
use super::{AdminState, pages};
use crate::xrpc::rate_limit::{client_ip, ip_key};

/// The session cookie.
pub const SESSION_COOKIE: &str = "__Host-prism_admin";
/// The setup and sign-in forms' cookie.
pub const PRESESSION_COOKIE: &str = "__Host-prism_presession";
/// The pre-session token's lifetime.
pub const PRESESSION_SECS: u64 = 600;
/// A session's DID is resolved afresh at least this often.
pub const RECHECK: Duration = Duration::from_secs(300);
/// Unfinished sign-ins are forgotten after this long.
pub const STATE_LIFETIME: Duration = Duration::from_secs(600);
/// Wrong setup codes past this many per minute from one address are delayed.
pub const WRONG_CODES_PER_MINUTE: u32 = 5;
/// The delay of each further wrong code.
pub const WRONG_CODE_DELAY: Duration = Duration::from_secs(2);
/// Delayed wrong-code answers at once; beyond, `429`.
pub const MAX_DELAYED: usize = 16;

/// Constant-time equality of two byte strings.
#[must_use]
pub fn ct_eq(a: &[u8], b: &[u8]) -> bool {
    a.len() == b.len() && a.iter().zip(b).fold(0u8, |acc, (x, y)| acc | (x ^ y)) == 0
}

/// 32 random bytes, hex.
#[must_use]
pub fn random_token() -> String {
    hex::encode(rand::random::<[u8; 32]>())
}

/// The session CSRF token for a cookie's bytes (§5.5).
#[must_use]
pub fn csrf_token(cookie: &[u8; 32]) -> String {
    blake3::keyed_hash(cookie, b"prism-admin-csrf").to_hex().to_string()
}

/// A cookie's value from the request.
#[must_use]
pub fn cookie(headers: &HeaderMap, name: &str) -> Option<String> {
    headers
        .get_all(COOKIE)
        .iter()
        .filter_map(|v| v.to_str().ok())
        .flat_map(|v| v.split(';'))
        .filter_map(|pair| pair.trim().split_once('='))
        .find(|(k, _)| *k == name)
        .map(|(_, v)| v.to_string())
}

/// The session cookie's 32 bytes.
#[must_use]
pub fn session_bytes(headers: &HeaderMap) -> Option<[u8; 32]> {
    let value = cookie(headers, SESSION_COOKIE)?;
    hex::decode(value).ok()?.try_into().ok()
}

fn set_cookie(name: &str, value: &str, max_age: u64) -> HeaderValue {
    HeaderValue::from_str(&format!("{name}={value}; Path=/; Secure; HttpOnly; SameSite=Strict; Max-Age={max_age}"))
        .unwrap_or_else(|_| HeaderValue::from_static(""))
}

/// The `Set-Cookie` header for a pre-session token.
#[must_use]
pub fn presession_cookie(token: &str) -> HeaderValue {
    set_cookie(PRESESSION_COOKIE, token, PRESESSION_SECS)
}

/// Whether the form's pre-session token matches its cookie.
#[must_use]
pub fn presession_ok(headers: &HeaderMap, form_token: &str) -> bool {
    cookie(headers, PRESESSION_COOKIE).is_some_and(|c| !c.is_empty() && ct_eq(c.as_bytes(), form_token.as_bytes()))
}

/// Whether a POST's `Origin`, when it has one, is this Prism's (§5.5).
#[must_use]
pub fn origin_ok(headers: &HeaderMap, public_hostname: &str) -> bool {
    match headers.get(ORIGIN) {
        None => true,
        Some(v) => v.to_str().is_ok_and(|o| o == format!("https://{public_hostname}")),
    }
}

/// The request's client address, the trusted proxy's forwarded one
/// included (§5.6).
#[must_use]
pub fn request_ip(state: &AdminState, headers: &HeaderMap, peer: SocketAddr) -> IpAddr {
    let xff = headers.get("x-forwarded-for").and_then(|v| v.to_str().ok());
    client_ip(peer.ip(), xff, state.trusted_proxy)
}

fn per_minute(n: u32) -> Quota {
    Quota::per_minute(NonZeroU32::new(n).unwrap_or(NonZeroU32::MIN))
}

/// The admin UI's limits (§5.2, §5.6). Never a lockout: each delays or
/// answers `429` for a while.
pub struct Limits {
    /// Sign-in starts: 10 per minute per client address.
    pub login: DefaultKeyedRateLimiter<IpAddr>,
    /// Fresh resolutions started by sign-in and setup: 30 per minute in all.
    pub resolutions: DefaultDirectRateLimiter,
    /// Page loads: 10 per second per session.
    pub pages: DefaultKeyedRateLimiter<[u8; 32]>,
    /// Changes: 1 per second per session.
    pub mutations: DefaultKeyedRateLimiter<[u8; 32]>,
    /// Wrong setup codes: 5 per minute per address (an IPv6 /64 as one).
    pub wrong_codes: DefaultKeyedRateLimiter<IpAddr>,
    /// Wrong-code answers being delayed now.
    pub delayed: Arc<Semaphore>,
}

impl Default for Limits {
    fn default() -> Self {
        Self {
            login: RateLimiter::keyed(per_minute(10)),
            resolutions: RateLimiter::direct(per_minute(30)),
            pages: RateLimiter::keyed(Quota::per_second(NonZeroU32::new(10).unwrap_or(NonZeroU32::MIN))),
            mutations: RateLimiter::keyed(Quota::per_second(NonZeroU32::MIN)),
            wrong_codes: RateLimiter::keyed(per_minute(WRONG_CODES_PER_MINUTE)),
            delayed: Arc::new(Semaphore::new(MAX_DELAYED)),
        }
    }
}

impl Limits {
    /// Forget idle keys.
    pub fn retain_recent(&self) {
        self.login.retain_recent();
        self.pages.retain_recent();
        self.mutations.retain_recent();
        self.wrong_codes.retain_recent();
    }

    /// A wrong setup code from `ip`: past the per-minute allowance, the
    /// answer is delayed, and past the bound of delayed answers it is
    /// `Err` (answer `429`).
    pub async fn wrong_code(&self, ip: IpAddr) -> Result<(), ()> {
        if self.wrong_codes.check_key(&ip_key(ip)).is_ok() {
            return Ok(());
        }
        let Ok(_permit) = Arc::clone(&self.delayed).try_acquire_owned() else {
            return Err(());
        };
        tokio::time::sleep(WRONG_CODE_DELAY).await;
        Ok(())
    }
}

/// A signed-in operator.
#[derive(Debug, Clone)]
pub struct Operator {
    pub did: String,
    /// The stored handle, when there is one.
    pub handle: Option<String>,
    pub csrf: String,
    /// What every page says at its top (admin-ui design §6): changes
    /// waiting for a restart, settings that could not be used.
    pub banners: Banners,
}

/// The two banners of every page (admin-ui design §6, §8.4, §8.5).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Banners {
    /// Saved changes that apply after a restart and are not in use yet.
    pub waiting: usize,
    /// Saved settings that could not be used at the last start.
    pub set_aside: usize,
    /// The restart button works here (`APP_SUPERVISED=1`).
    pub supervised: bool,
}

/// The banners now: one small read of the saved settings.
///
/// # Errors
///
/// Database errors.
pub async fn banners(state: &AdminState) -> Result<Banners, sqlx::Error> {
    use super::settings::store;
    let saved = store::rows(&state.pool).await?;
    let s = &state.settings;
    let counts = store::counts(&saved, &s.file_json, &s.running_json, &s.networks());
    Ok(Banners { waiting: counts.waiting, set_aside: counts.set_aside, supervised: s.env.supervised })
}

impl Operator {
    /// How pages name the operator.
    #[must_use]
    pub fn who(&self) -> &str {
        self.handle.as_deref().unwrap_or(&self.did)
    }
}

/// What a request is about to do.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Need {
    /// A page.
    Page,
    /// A POST that may change something: the CSRF token is checked and the
    /// DID resolved afresh.
    Change,
    /// Watching (admin-ui-live-feed design §3.3): the live stream's open,
    /// the Home tiles refresh, Home's reload with JavaScript off. Checked
    /// as a page, counted against the page limit, and it does not move
    /// `last_seen_at`, so an open tab does not keep its session alive.
    Poll,
}

/// Whether [`check_session`] resolves the DID afresh whatever the age of
/// the last check.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Refresh {
    /// Only when the last check is older than [`RECHECK`].
    No,
    /// Always.
    Yes,
}

/// Why a session check failed.
#[derive(Debug)]
pub enum Refusal {
    /// No row: never signed in, or signed out.
    SignedOut,
    /// Past the idle or the absolute limit; the row is deleted.
    Expired,
    /// Cut off (§5.4): the row is deleted and the event logged.
    NotOperator,
    HostNotAllowed,
    PdsChanged,
    /// A fresh resolution failed; the session is kept.
    CouldNotConfirm,
    Db(sqlx::Error),
}

impl Refusal {
    /// The live stream's `end` reason.
    #[must_use]
    pub fn reason(&self) -> &'static str {
        match self {
            Self::SignedOut => "signed_out",
            Self::Expired => "expired",
            Self::NotOperator => "not_operator",
            Self::HostNotAllowed => "host_not_allowed",
            Self::PdsChanged => "pds_changed",
            Self::CouldNotConfirm | Self::Db(_) => "could_not_confirm",
        }
    }
}

/// A checked session.
#[derive(Debug, Clone)]
pub struct Session {
    pub did: String,
    pub endpoint: String,
    pub last_seen: DateTime<Utc>,
    pub expires: DateTime<Utc>,
    pub checked: DateTime<Utc>,
}

impl Session {
    /// When the session ends unless used: the earlier of its absolute
    /// expiry and `last_seen_at` plus the idle limit.
    #[must_use]
    pub fn deadline(&self, idle: Duration) -> DateTime<Utc> {
        self.expires.min(self.last_seen + chrono::Duration::from_std(idle).unwrap_or_default())
    }
}

/// Record a sign-in event (§5.8).
pub async fn log_login(state: &AdminState, did: Option<&str>, outcome: &str, ip: Option<IpAddr>) {
    let result = sqlx::query("INSERT INTO admin_logins (did, outcome, client_ip) VALUES ($1, $2, $3)")
        .bind(did)
        .bind(outcome)
        .bind(ip.map(|i| i.to_string()))
        .execute(&state.pool)
        .await;
    if let Err(error) = result {
        tracing::warn!(event = "admin_login_log_failed", %error, "recording a sign-in event failed");
    }
}

/// Create a session for `did` on `endpoint`; returns its `Set-Cookie`.
///
/// # Errors
///
/// Database errors.
pub async fn create_session(state: &AdminState, did: &str, endpoint: &str) -> Result<HeaderValue, sqlx::Error> {
    let bytes = rand::random::<[u8; 32]>();
    let max = state.session_max;
    sqlx::query(
        "INSERT INTO admin_sessions (id_hash, did, pds_endpoint, expires_at) \
         VALUES ($1, $2, $3, now() + make_interval(secs => $4))",
    )
    .bind(blake3::hash(&bytes).as_bytes().as_slice())
    .bind(did)
    .bind(endpoint)
    .bind(max.as_secs_f64())
    .execute(&state.pool)
    .await?;
    Ok(set_cookie(SESSION_COOKIE, &hex::encode(bytes), max.as_secs()))
}

/// The `Set-Cookie` that clears the session cookie.
#[must_use]
pub fn clear_session_cookie() -> HeaderValue {
    set_cookie(SESSION_COOKIE, "", 0)
}

/// End the request's session, if any (sign-out).
pub async fn end_session(state: &AdminState, headers: &HeaderMap) {
    if let Some(bytes) = session_bytes(headers) {
        let _ = sqlx::query("DELETE FROM admin_sessions WHERE id_hash = $1")
            .bind(blake3::hash(&bytes).as_bytes().as_slice())
            .execute(&state.pool)
            .await;
    }
}

fn to_login() -> Response {
    Redirect::to("/admin/login").into_response()
}

/// Cut a session off (§5.4): the row is deleted and the event logged.
async fn cut(state: &AdminState, id_hash: &[u8; 32], did: &str, reason: &str, ip: Option<IpAddr>) {
    let _ = sqlx::query("DELETE FROM admin_sessions WHERE id_hash = $1").bind(id_hash.as_slice()).execute(&state.pool).await;
    tracing::info!(event = "admin_session_cut", did, reason, "admin session ended");
    metrics::counter!("admin_session_cut_total", "reason" => reason.to_string()).increment(1);
    log_login(state, Some(did), &format!("session_cut:{reason}"), ip).await;
}

/// The page of a cut-off session: why, and the cleared cookie.
fn cut_page(state: &AdminState, reason: &str) -> Response {
    let message = match reason {
        "not_operator" => "You are no longer an operator of this Prism, so you were signed out.",
        "host_not_allowed" => "Your account's server is no longer on this Prism's list of allowed servers, so you were signed out.",
        _ => "Your account moved to another server since you signed in, so you were signed out. Sign in again.",
    };
    let mut response = pages::message(state, None, StatusCode::UNAUTHORIZED, "Signed out", message, Some(("/admin/login", "Sign in")));
    response.headers_mut().insert(SET_COOKIE, clear_session_cookie());
    response
}

type SessionRow = (String, String, DateTime<Utc>, DateTime<Utc>, DateTime<Utc>);

/// The session row, unexpired; an expired row is deleted. Checks nothing
/// else.
///
/// # Errors
///
/// [`Refusal::SignedOut`], [`Refusal::Expired`] or a database error.
pub async fn load_session(state: &AdminState, id_hash: &[u8; 32]) -> Result<Session, Refusal> {
    let row: Option<SessionRow> = sqlx::query_as(
        "SELECT did, pds_endpoint, last_seen_at, expires_at, checked_at FROM admin_sessions WHERE id_hash = $1",
    )
    .bind(id_hash.as_slice())
    .fetch_optional(&state.pool)
    .await
    .map_err(Refusal::Db)?;
    let Some((did, endpoint, last_seen, expires, checked)) = row else {
        return Err(Refusal::SignedOut);
    };
    let now = Utc::now();
    let idle = chrono::Duration::from_std(state.session_idle).unwrap_or_default();
    if now >= expires || now - last_seen >= idle {
        let _ = sqlx::query("DELETE FROM admin_sessions WHERE id_hash = $1").bind(id_hash.as_slice()).execute(&state.pool).await;
        return Err(Refusal::Expired);
    }
    Ok(Session { did, endpoint, last_seen, expires, checked })
}

/// The DID is an operator, its recorded host allowed, and, when `fresh`
/// or the last check is older than [`RECHECK`], a fresh resolution finds
/// the same endpoint on an allowed host (§5.4). A cut-off deletes the row
/// and logs it.
async fn verify(state: &AdminState, id_hash: &[u8; 32], session: &Session, fresh: bool, ip: Option<IpAddr>) -> Result<(), Refusal> {
    let did = &session.did;
    if !is_operator(&state.pool, did).await.map_err(Refusal::Db)? {
        cut(state, id_hash, did, "not_operator", ip).await;
        return Err(Refusal::NotOperator);
    }
    let allowed = {
        let mut conn = state.pool.acquire().await.map_err(Refusal::Db)?;
        patterns(&mut conn).await.map_err(Refusal::Db)?
    };
    let host = endpoint_host(&session.endpoint).unwrap_or_default();
    if !host_allowed(&allowed, &host) {
        cut(state, id_hash, did, "host_not_allowed", ip).await;
        return Err(Refusal::HostNotAllowed);
    }
    let stale = Utc::now() - session.checked >= chrono::Duration::from_std(RECHECK).unwrap_or_default();
    if fresh || stale {
        let resolved = match state.identity.resolve_fresh(did).await {
            Ok(info) => info.pds_endpoint.as_deref().and_then(normalize_endpoint),
            Err(error) => {
                tracing::info!(event = "admin_recheck_failed", did = %did, %error, "resolving an operator afresh failed");
                return Err(Refusal::CouldNotConfirm);
            }
        };
        let Some(resolved) = resolved.filter(|f| *f == session.endpoint) else {
            cut(state, id_hash, did, "pds_changed", ip).await;
            return Err(Refusal::PdsChanged);
        };
        if !host_allowed(&allowed, &endpoint_host(&resolved).unwrap_or_default()) {
            cut(state, id_hash, did, "host_not_allowed", ip).await;
            return Err(Refusal::HostNotAllowed);
        }
        let _ = sqlx::query("UPDATE admin_sessions SET checked_at = now() WHERE id_hash = $1")
            .bind(id_hash.as_slice())
            .execute(&state.pool)
            .await;
        let _ = sqlx::query("UPDATE admin_operators SET pds_endpoint = $2, pds_checked_at = now() WHERE did = $1")
            .bind(did)
            .bind(&resolved)
            .execute(&state.pool)
            .await;
    }
    Ok(())
}

/// A session check without a request (admin-ui-live-feed design §3.3):
/// the row present and unexpired, the DID an operator, the recorded host
/// allowed, and a fresh resolution when the last is older than
/// [`RECHECK`] (or always, with [`Refresh::Yes`]), exactly as [`require`]
/// checks. It moves nothing but `checked_at`. A cut-off deletes the
/// session, as on a page.
///
/// # Errors
///
/// Why the session no longer holds.
pub async fn check_session(state: &AdminState, id_hash: &[u8; 32], refresh: Refresh, ip: Option<IpAddr>) -> Result<Session, Refusal> {
    let session = load_session(state, id_hash).await?;
    verify(state, id_hash, &session, refresh == Refresh::Yes, ip).await?;
    Ok(session)
}

/// The signed-in operator for this request (§5.4), or the response to give
/// instead: a redirect to setup or sign-in, a cut-off, a refusal, or `429`.
/// A [`Need::Change`] also needs the form's CSRF token and an `Origin` of
/// this Prism.
///
/// # Errors
///
/// The response to answer with.
pub async fn require(
    state: &AdminState,
    headers: &HeaderMap,
    peer: SocketAddr,
    need: Need,
    form_csrf: Option<&str>,
) -> Result<Operator, Response> {
    let ip = request_ip(state, headers, peer);
    let internal = |e: sqlx::Error| pages::internal_error(state, &e);
    if !super::setup_code::operators_exist(&state.pool).await.map_err(internal)? {
        return Err(Redirect::to("/admin/setup").into_response());
    }
    let Some(bytes) = session_bytes(headers) else {
        return Err(to_login());
    };
    let csrf = csrf_token(&bytes);
    if need == Need::Change {
        let token_ok = form_csrf.is_some_and(|t| ct_eq(t.as_bytes(), csrf.as_bytes()));
        if !token_ok || !origin_ok(headers, &state.public_hostname) {
            return Err(pages::message(
                state,
                None,
                StatusCode::FORBIDDEN,
                "Not done",
                "This form is out of date or did not come from this page. Go back, reload the page and try again.",
                Some(("/admin", "Home")),
            ));
        }
    }
    let id_hash = *blake3::hash(&bytes).as_bytes();
    let session = match load_session(state, &id_hash).await {
        Ok(s) => s,
        Err(Refusal::Db(e)) => return Err(internal(e)),
        Err(_) => return Err(to_login()),
    };
    // The limits come before any outbound call.
    let limited = match need {
        Need::Page | Need::Poll => state.limits.pages.check_key(&id_hash).is_err(),
        Need::Change => state.limits.mutations.check_key(&id_hash).is_err(),
    };
    if limited {
        return Err(pages::message(
            state,
            None,
            StatusCode::TOO_MANY_REQUESTS,
            "Slow down",
            "Too many requests in a short time. Wait a moment and try again.",
            None,
        ));
    }
    match verify(state, &id_hash, &session, need == Need::Change, Some(ip)).await {
        Ok(()) => {}
        Err(Refusal::Db(e)) => return Err(internal(e)),
        Err(Refusal::CouldNotConfirm) => {
            return Err(pages::message(
                state,
                None,
                StatusCode::SERVICE_UNAVAILABLE,
                "Try again",
                "Prism could not confirm your account just now; try again.",
                None,
            ));
        }
        Err(refusal) => return Err(cut_page(state, refusal.reason())),
    }
    // Watching is not activity (admin-ui-live-feed design §3.3).
    if need != Need::Poll {
        let _ = sqlx::query("UPDATE admin_sessions SET last_seen_at = now() WHERE id_hash = $1")
            .bind(id_hash.as_slice())
            .execute(&state.pool)
            .await;
    }
    let did = session.did;
    let handle: Option<String> = sqlx::query_scalar("SELECT handle FROM actors WHERE did = $1")
        .bind(&did)
        .fetch_optional(&state.pool)
        .await
        .map_err(internal)?
        .flatten();
    // Watching needs no banners.
    let banners = if need == Need::Poll { Banners::default() } else { banners(state).await.map_err(internal)? };
    Ok(Operator { did, handle, csrf, banners })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tokens_and_cookies() {
        let bytes = [7u8; 32];
        let other = [8u8; 32];
        assert_eq!(csrf_token(&bytes), csrf_token(&bytes));
        assert_ne!(csrf_token(&bytes), csrf_token(&other), "another session's token differs");
        let mut headers = HeaderMap::new();
        headers.insert(COOKIE, HeaderValue::from_str(&format!("a=b; {SESSION_COOKIE}={}; c=d", hex::encode(bytes))).expect("header"));
        assert_eq!(session_bytes(&headers), Some(bytes));
        assert!(ct_eq(b"abc", b"abc"));
        assert!(!ct_eq(b"abc", b"abd"));
        assert!(!ct_eq(b"abc", b"ab"));
        let mut h = HeaderMap::new();
        assert!(origin_ok(&h, "prism.example"), "no Origin");
        h.insert(ORIGIN, HeaderValue::from_static("https://prism.example"));
        assert!(origin_ok(&h, "prism.example"));
        h.insert(ORIGIN, HeaderValue::from_static("https://evil.example"));
        assert!(!origin_ok(&h, "prism.example"));
        let c = presession_cookie("tok").to_str().expect("str").to_string();
        assert!(c.starts_with("__Host-prism_presession=tok;") && c.contains("Secure") && c.contains("HttpOnly") && c.contains("SameSite=Strict"));
    }
}
