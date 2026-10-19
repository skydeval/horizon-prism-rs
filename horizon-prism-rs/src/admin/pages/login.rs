//! Sign-in, sign-out and the OAuth callback (admin-ui design §5.2–§5.4).

use std::net::{IpAddr, SocketAddr};

use askama::Template;
use axum::Form;
use axum::Json;
use axum::extract::{ConnectInfo, Query, State};
use axum::http::header::SET_COOKIE;
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Redirect, Response};
use serde::Deserialize;

use super::{css, message, render};
use crate::admin::auth::{
    self, clear_session_cookie, create_session, csrf_token, ct_eq, log_login, origin_ok, presession_cookie, presession_ok,
    random_token, request_ip, session_bytes,
};
use crate::admin::operators::{endpoint_host, host_allowed, is_operator, normalize_endpoint, patterns};
use crate::admin::{AdminState, oauth, setup_code};
use crate::xrpc::rate_limit::ip_key;

#[derive(Template)]
#[template(path = "login.html")]
struct LoginPage {
    css: &'static str,
    nav: Option<super::Nav>,
    title: String,
    presession: String,
    input: String,
    error: Option<String>,
}

/// The page that sends the browser on to a URL (§5.2 step 3): a link and a
/// `meta refresh`, never a redirect, which `form-action 'self'` would
/// refuse after a form POST.
#[derive(Template)]
#[template(path = "continue.html")]
pub struct ContinuePage {
    pub css: &'static str,
    pub nav: Option<super::Nav>,
    pub title: String,
    pub text: String,
    pub url: String,
}

fn login_page(status: StatusCode, input: &str, error: Option<String>) -> Response {
    let presession = random_token();
    let mut response = render(
        status,
        &LoginPage {
            css: css(),
            nav: None,
            title: "Sign in".to_string(),
            presession: presession.clone(),
            input: input.to_string(),
            error,
        },
    );
    response.headers_mut().insert(SET_COOKIE, presession_cookie(&presession));
    response
}

/// `GET /admin/login`.
pub async fn form(State(state): State<AdminState>) -> Response {
    match setup_code::operators_exist(&state.pool).await {
        Ok(false) => Redirect::to("/admin/setup").into_response(),
        Ok(true) => login_page(StatusCode::OK, "", None),
        Err(e) => super::internal_error(&state, &e),
    }
}

#[derive(Debug, Deserialize)]
pub struct LoginForm {
    #[serde(default)]
    pub input: String,
    #[serde(default)]
    pub presession: String,
}

/// A handle or DID typed into a form, resolved afresh (§5.2 step 2, §5.3):
/// the DID and its normalized PDS endpoint. A PDS URL is refused.
///
/// # Errors
///
/// A plain-words problem.
pub(crate) async fn resolve_input(state: &AdminState, input: &str) -> Result<(String, String), String> {
    let input = input.trim().trim_start_matches('@');
    if input.is_empty() {
        return Err("Enter your handle or DID.".to_string());
    }
    if input.contains("://") || input.contains('/') {
        return Err("Enter a handle or a DID, not a server address.".to_string());
    }
    let did = if input.starts_with("did:") {
        if !proto_blue::syntax::Did::is_valid(input) {
            return Err("That is not a valid DID.".to_string());
        }
        input.to_string()
    } else {
        let handle = input.to_ascii_lowercase();
        if !proto_blue::syntax::Handle::is_valid(&handle) {
            return Err("That is not a valid handle.".to_string());
        }
        if state.limits.resolutions.check().is_err() {
            return Err("Too many sign-ins are being started right now; try again in a minute.".to_string());
        }
        state
            .identity
            .resolve_handle(&handle)
            .await
            .ok_or_else(|| "That handle could not be found, or does not point back to its account.".to_string())?
    };
    if state.limits.resolutions.check().is_err() {
        return Err("Too many sign-ins are being started right now; try again in a minute.".to_string());
    }
    let info = state
        .identity
        .resolve_fresh(&did)
        .await
        .map_err(|_| "Prism could not look up that account just now; try again.".to_string())?;
    let endpoint = info
        .pds_endpoint
        .as_deref()
        .and_then(normalize_endpoint)
        .ok_or_else(|| "That account names no server.".to_string())?;
    Ok((did, endpoint))
}

/// Start an OAuth flow for `did` at `endpoint` and keep its state (§5.2
/// step 3): the URL to send the browser to.
///
/// # Errors
///
/// A plain-words problem.
pub(crate) async fn start_flow(
    state: &AdminState,
    did: &str,
    endpoint: &str,
    purpose: &str,
    code_hash: Option<[u8; 32]>,
) -> Result<String, String> {
    let started = oauth::start(&state.oauth, endpoint).await?;
    sqlx::query(
        "INSERT INTO admin_oauth_states (state, did, issuer, verifier, dpop_jwk, purpose, setup_code_hash, pds_endpoint) \
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8)",
    )
    .bind(&started.state)
    .bind(did)
    .bind(&started.issuer)
    .bind(&started.verifier)
    .bind(&started.dpop_jwk)
    .bind(purpose)
    .bind(code_hash.map(|h| h.to_vec()))
    .bind(endpoint)
    .execute(&state.pool)
    .await
    .map_err(|e| {
        tracing::error!(event = "admin_oauth_state_failed", error = %e, "storing a sign-in failed");
        "Prism could not start the sign-in just now; try again.".to_string()
    })?;
    Ok(started.authorize_url)
}

/// The continuation page for an authorization URL.
pub(crate) fn continue_to(url: String) -> Response {
    render(
        StatusCode::OK,
        &ContinuePage {
            css: css(),
            nav: None,
            title: "Continuing".to_string(),
            text: "Continuing to your account's server…".to_string(),
            url,
        },
    )
}

/// `POST /admin/login` (§5.3): only an operator on an allowed host starts a
/// flow; anyone else is refused before any request to their server.
pub async fn submit(
    State(state): State<AdminState>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    Form(form): Form<LoginForm>,
) -> Response {
    let ip = request_ip(&state, &headers, peer);
    match setup_code::operators_exist(&state.pool).await {
        Ok(false) => return Redirect::to("/admin/setup").into_response(),
        Ok(true) => {}
        Err(e) => return super::internal_error(&state, &e),
    }
    if !presession_ok(&headers, &form.presession) || !origin_ok(&headers, &state.public_hostname) {
        return login_page(StatusCode::FORBIDDEN, &form.input, Some("This form expired. Try again.".to_string()));
    }
    if state.limits.login.check_key(&ip_key(ip)).is_err() {
        return login_page(StatusCode::TOO_MANY_REQUESTS, &form.input, Some("Too many sign-in attempts; wait a minute.".to_string()));
    }
    let (did, endpoint) = match resolve_input(&state, &form.input).await {
        Ok(found) => found,
        Err(problem) => return login_page(StatusCode::BAD_REQUEST, &form.input, Some(problem)),
    };
    if let Some(refusal) = refusal(&state, &did, &endpoint, ip).await {
        return refusal;
    }
    match start_flow(&state, &did, &endpoint, "login", None).await {
        Ok(url) => continue_to(url),
        Err(problem) => login_page(StatusCode::BAD_GATEWAY, &form.input, Some(problem)),
    }
}

/// Why `did` on `endpoint` may not sign in, as the response to give, or
/// `None` when it may.
async fn refusal(state: &AdminState, did: &str, endpoint: &str, ip: IpAddr) -> Option<Response> {
    match is_operator(&state.pool, did).await {
        Ok(true) => {}
        Ok(false) => {
            log_login(state, Some(did), "not_operator", Some(ip)).await;
            tracing::info!(event = "admin_login_refused", reason = "not_operator", did, "sign-in refused");
            return Some(login_page(StatusCode::FORBIDDEN, did, Some("That account is not an operator of this Prism.".to_string())));
        }
        Err(e) => return Some(super::internal_error(state, &e)),
    }
    let allowed = match state.pool.acquire().await {
        Ok(mut conn) => match patterns(&mut conn).await {
            Ok(p) => p,
            Err(e) => return Some(super::internal_error(state, &e)),
        },
        Err(e) => return Some(super::internal_error(state, &e)),
    };
    let host = endpoint_host(endpoint).unwrap_or_default();
    if !host_allowed(&allowed, &host) {
        log_login(state, Some(did), "host_not_allowed", Some(ip)).await;
        tracing::info!(event = "admin_login_refused", reason = "host_not_allowed", did, host = %host, "sign-in refused");
        return Some(login_page(
            StatusCode::FORBIDDEN,
            did,
            Some(format!("Your account is hosted on {host}, which is not on this Prism's list of allowed servers.")),
        ));
    }
    None
}

#[derive(Debug, Deserialize)]
pub struct LogoutForm {
    #[serde(default)]
    pub csrf: String,
}

/// `POST /admin/logout`: no re-check (§5.4), the CSRF token and `Origin`
/// only.
pub async fn logout(State(state): State<AdminState>, headers: HeaderMap, Form(form): Form<LogoutForm>) -> Response {
    let token_ok = session_bytes(&headers).is_some_and(|b| ct_eq(csrf_token(&b).as_bytes(), form.csrf.as_bytes()));
    if !token_ok || !origin_ok(&headers, &state.public_hostname) {
        return message(&state, None, StatusCode::FORBIDDEN, "Not done", "This form is out of date. Reload the page and try again.", Some(("/admin", "Home")));
    }
    auth::end_session(&state, &headers).await;
    let mut response = Redirect::to("/admin/login").into_response();
    response.headers_mut().insert(SET_COOKIE, clear_session_cookie());
    response
}

/// `GET /admin/oauth/client-metadata.json`: the client id document.
pub async fn client_metadata(State(state): State<AdminState>) -> Response {
    Json(oauth::client_metadata(&state.public_hostname)).into_response()
}

#[derive(Debug, Deserialize)]
pub struct CallbackQuery {
    pub state: Option<String>,
    pub code: Option<String>,
    pub iss: Option<String>,
    pub error: Option<String>,
}

type StateRow = (String, String, String, serde_json::Value, String, Option<Vec<u8>>, String);

/// `GET /admin/oauth/callback` (§5.2 step 4, §5.3). The state is used
/// once; the DID is resolved afresh and must still be on the endpoint the
/// flow started from. The answer is a page, not a redirect, so the
/// session cookie set here is kept (membership design §8 step 5).
pub async fn callback(
    State(state): State<AdminState>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    Query(q): Query<CallbackQuery>,
) -> Response {
    let ip = request_ip(&state, &headers, peer);
    let failed = |text: &str| message(&state, None, StatusCode::BAD_REQUEST, "Sign-in not completed", text, Some(("/admin/login", "Sign in")));
    let Some(key) = q.state.as_deref() else {
        return failed("This sign-in link is incomplete.");
    };
    let row: Option<StateRow> = match sqlx::query_as(
        "DELETE FROM admin_oauth_states WHERE state = $1 AND created_at > now() - interval '10 minutes' \
         RETURNING did, issuer, verifier, dpop_jwk, purpose, setup_code_hash, pds_endpoint",
    )
    .bind(key)
    .fetch_optional(&state.pool)
    .await
    {
        Ok(r) => r,
        Err(e) => return super::internal_error(&state, &e),
    };
    let Some((did, issuer, verifier, dpop_jwk, purpose, code_hash, endpoint)) = row else {
        return failed("This sign-in has expired or was already used. Start again.");
    };
    let oauth_failed = |reason: String| oauth_failed(&state, &did, ip, reason);
    if q.error.is_some() {
        return oauth_failed("Your account's server did not approve the sign-in.".to_string()).await;
    }
    let Some(code) = q.code.as_deref() else {
        return oauth_failed("This sign-in link is incomplete.".to_string()).await;
    };
    let pending = oauth::Pending {
        state: key.to_string(),
        did: did.clone(),
        issuer,
        verifier,
        dpop_jwk,
        pds_endpoint: endpoint.clone(),
    };
    if let Err(reason) = oauth::finish(&state.oauth, &pending, code, q.iss.as_deref()).await {
        return oauth_failed(reason).await;
    }
    // The DID afresh: still on the endpoint the flow started from.
    let fresh = match state.identity.resolve_fresh(&did).await {
        Ok(info) => info.pds_endpoint.as_deref().and_then(normalize_endpoint),
        Err(_) => return oauth_failed("Prism could not confirm your account just now; try again.".to_string()).await,
    };
    if fresh.as_deref() != Some(endpoint.as_str()) {
        return oauth_failed("Your account moved to another server during sign-in. Sign in again.".to_string()).await;
    }
    let completed = if purpose == "setup" {
        complete_setup(&state, &did, &endpoint, code_hash.as_deref(), ip).await
    } else {
        complete_login(&state, &did, &endpoint, ip).await
    };
    let outcome = match completed {
        Ok(()) => create_session(&state, &did, &endpoint).await,
        Err(response) => return response,
    };
    match outcome {
        Ok(cookie) => {
            let mut response = render(
                StatusCode::OK,
                &ContinuePage {
                    css: css(),
                    nav: None,
                    title: "Signed in".to_string(),
                    text: "You are signed in.".to_string(),
                    url: "/admin".to_string(),
                },
            );
            response.headers_mut().insert(SET_COOKIE, cookie);
            response
        }
        Err(e) => super::internal_error(&state, &e),
    }
}

/// A callback that cannot complete: recorded, and said plainly.
async fn oauth_failed(state: &AdminState, did: &str, ip: IpAddr, reason: String) -> Response {
    log_login(state, Some(did), "oauth_failed", Some(ip)).await;
    tracing::info!(event = "admin_login_refused", reason = "oauth_failed", did, detail = %reason, "sign-in not completed");
    message(state, None, StatusCode::BAD_REQUEST, "Sign-in not completed", &reason, Some(("/admin/login", "Sign in")))
}

/// A sign-in's last checks: still an operator, still on an allowed host.
async fn complete_login(state: &AdminState, did: &str, endpoint: &str, ip: IpAddr) -> Result<(), Response> {
    if let Some(refusal) = refusal(state, did, endpoint, ip).await {
        return Err(refusal);
    }
    let _ = sqlx::query("UPDATE admin_operators SET pds_endpoint = $2, pds_checked_at = now() WHERE did = $1")
        .bind(did)
        .bind(endpoint)
        .execute(&state.pool)
        .await;
    log_login(state, Some(did), "ok", Some(ip)).await;
    tracing::info!(event = "admin_login", did, "operator signed in");
    Ok(())
}

/// Setup's one transaction (§5.2 step 4): the code is consumed, the first
/// operator and its exact host are added. A second flow with the same
/// code finds no row.
async fn complete_setup(state: &AdminState, did: &str, endpoint: &str, code_hash: Option<&[u8]>, ip: IpAddr) -> Result<(), Response> {
    let refused = |text: &str| message(state, None, StatusCode::CONFLICT, "Setup not completed", text, Some(("/admin", "Home")));
    let Some(code_hash) = code_hash else {
        return Err(refused("This setup sign-in carries no code."));
    };
    let Some(host) = endpoint_host(endpoint) else {
        return Err(refused("Your account's server address could not be read."));
    };
    let result: Result<bool, sqlx::Error> = async {
        let mut tx = state.pool.begin().await?;
        let consumed: Option<i16> = sqlx::query_scalar(
            "DELETE FROM admin_setup_code WHERE id = 1 AND code_hash = $1 AND expires_at > now() RETURNING id",
        )
        .bind(code_hash)
        .fetch_optional(&mut *tx)
        .await?;
        if consumed.is_none() {
            return Ok(false);
        }
        sqlx::query("INSERT INTO admin_operators (did, added_by, pds_endpoint, pds_checked_at) VALUES ($1, 'setup', $2, now())")
            .bind(did)
            .bind(endpoint)
            .execute(&mut *tx)
            .await?;
        sqlx::query("INSERT INTO admin_operator_hosts (pattern, added_by) VALUES ($1, 'setup') ON CONFLICT DO NOTHING")
            .bind(&host)
            .execute(&mut *tx)
            .await?;
        tx.commit().await?;
        Ok(true)
    }
    .await;
    match result {
        Ok(true) => {
            setup_code::delete_file(&state.setup_code_file);
            log_login(state, Some(did), "setup", Some(ip)).await;
            metrics::counter!("admin_setup_attempts_total", "outcome" => "completed").increment(1);
            tracing::warn!(event = "admin_setup_completed", did, host = %host, "first-run setup completed: the first operator was added");
            Ok(())
        }
        Ok(false) => Err(refused("Setup was already completed, or the code expired or was replaced.")),
        Err(e) => Err(super::internal_error(state, &e)),
    }
}
