//! First-run setup (admin-ui design §5.2): only while no operator exists;
//! `404` once one does.

use std::net::SocketAddr;

use askama::Template;
use axum::Form;
use axum::extract::{ConnectInfo, State};
use axum::http::header::SET_COOKIE;
use axum::http::{HeaderMap, StatusCode};
use axum::response::Response;
use chrono::{DateTime, Utc};
use serde::Deserialize;

use super::login::{continue_to, resolve_input, start_flow};
use super::{css, not_found, render};
use crate::admin::auth::{ct_eq, origin_ok, presession_cookie, presession_ok, random_token, request_ip};
use crate::admin::{AdminState, setup_code};

#[derive(Template)]
#[template(path = "setup.html")]
struct SetupPage {
    css: &'static str,
    nav: Option<super::Nav>,
    title: String,
    presession: String,
    input: String,
    error: Option<String>,
}

fn setup_page(status: StatusCode, input: &str, error: Option<String>) -> Response {
    let presession = random_token();
    let mut response = render(
        status,
        &SetupPage {
            css: css(),
            nav: None,
            title: "First-run setup".to_string(),
            presession: presession.clone(),
            input: input.to_string(),
            error,
        },
    );
    response.headers_mut().insert(SET_COOKIE, presession_cookie(&presession));
    response
}

/// `GET /admin/setup`.
pub async fn form(State(state): State<AdminState>) -> Response {
    match setup_code::operators_exist(&state.pool).await {
        Ok(false) => setup_page(StatusCode::OK, "", None),
        Ok(true) => not_found().await,
        Err(e) => super::internal_error(&state, &e),
    }
}

#[derive(Debug, Deserialize)]
pub struct SetupForm {
    #[serde(default)]
    pub code: String,
    #[serde(default)]
    pub input: String,
    #[serde(default)]
    pub presession: String,
}

/// `POST /admin/setup` (§5.2 step 1–3). The submitted code is checked
/// first, so a correct code is never refused by a limiter; it is not
/// consumed here. Setup checks no allowed host (there is none yet).
pub async fn submit(
    State(state): State<AdminState>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    Form(form): Form<SetupForm>,
) -> Response {
    match setup_code::operators_exist(&state.pool).await {
        Ok(false) => {}
        Ok(true) => return not_found().await,
        Err(e) => return super::internal_error(&state, &e),
    }
    if !presession_ok(&headers, &form.presession) || !origin_ok(&headers, &state.public_hostname) {
        return setup_page(StatusCode::FORBIDDEN, &form.input, Some("This form expired. Try again.".to_string()));
    }
    let row: Option<(Vec<u8>, DateTime<Utc>)> =
        match sqlx::query_as("SELECT code_hash, expires_at FROM admin_setup_code WHERE id = 1").fetch_optional(&state.pool).await {
            Ok(r) => r,
            Err(e) => return super::internal_error(&state, &e),
        };
    let hash = setup_code::hash(&form.code);
    let valid = setup_code::well_formed(&form.code)
        && row.is_some_and(|(stored, expires_at)| ct_eq(&hash, &stored) && expires_at > Utc::now());
    if !valid {
        let ip = request_ip(&state, &headers, peer);
        metrics::counter!("admin_setup_attempts_total", "outcome" => "wrong_code").increment(1);
        tracing::debug!(event = "admin_setup_code_failures", client_ip = %ip, "a wrong setup code was entered");
        if state.limits.wrong_code(ip).await.is_err() {
            return setup_page(StatusCode::TOO_MANY_REQUESTS, &form.input, Some("Too many wrong codes. Wait a minute and try again.".to_string()));
        }
        return setup_page(
            StatusCode::BAD_REQUEST,
            &form.input,
            Some("That code is not valid. Check it, or see the log for the current one.".to_string()),
        );
    }
    let (did, endpoint) = match resolve_input(&state, &form.input).await {
        Ok(found) => found,
        Err(problem) => return setup_page(StatusCode::BAD_REQUEST, &form.input, Some(problem)),
    };
    metrics::counter!("admin_setup_attempts_total", "outcome" => "started").increment(1);
    match start_flow(&state, &did, &endpoint, "setup", Some(hash)).await {
        Ok(url) => continue_to(url),
        Err(problem) => setup_page(StatusCode::BAD_GATEWAY, &form.input, Some(problem)),
    }
}
