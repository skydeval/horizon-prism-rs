//! Appearance (`/admin/settings/appearance`, #810): the signed-in
//! operator's own look of these pages, light, dark or the device's, and a
//! colour scheme. Saved for that operator alone (`theme`), straight away:
//! no command, since it changes nothing about this server, and so not in
//! the audit log. The form is checked like every other (session, CSRF
//! token, `Origin`, the one-change-a-second limit).

use std::net::SocketAddr;

use askama::Template;
use axum::Form;
use axum::extract::{ConnectInfo, Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Redirect, Response};

use super::{Nav, css, message, render};
use crate::admin::AdminState;
use crate::admin::auth::{Need, require};
use crate::admin::theme::{self, Mode, Scheme, Theme};

/// A choice on the page.
pub struct Choice {
    pub value: &'static str,
    pub label: &'static str,
    pub checked: bool,
}

#[derive(Template)]
#[template(path = "appearance.html")]
struct AppearancePage {
    css: &'static str,
    nav: Option<Nav>,
    title: String,
    csrf: String,
    modes: Vec<Choice>,
    schemes: Vec<Choice>,
    saved: bool,
}

/// `?saved=1` after a save.
#[derive(Debug, Default, serde::Deserialize)]
pub struct PageQuery {
    pub saved: Option<String>,
}

/// `GET /admin/settings/appearance`.
pub async fn page(
    State(state): State<AdminState>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    Query(q): Query<PageQuery>,
) -> Response {
    let op = match require(&state, &headers, peer, Need::Page, None).await {
        Ok(op) => op,
        Err(response) => return response,
    };
    let now = op.theme;
    render(
        StatusCode::OK,
        &AppearancePage {
            css: css(),
            nav: Some(Nav::new(&op, "settings")),
            title: "Appearance".to_string(),
            csrf: op.csrf.clone(),
            modes: Mode::ALL.into_iter().map(|m| Choice { value: m.as_str(), label: m.label(), checked: m == now.mode }).collect(),
            schemes: Scheme::ALL.into_iter().map(|s| Choice { value: s.as_str(), label: s.label(), checked: s == now.scheme }).collect(),
            saved: q.saved.as_deref() == Some("1"),
        },
    )
}

/// The form.
#[derive(Debug, serde::Deserialize)]
pub struct AppearanceForm {
    #[serde(default)]
    pub csrf: String,
    #[serde(default)]
    pub mode: String,
    #[serde(default)]
    pub scheme: String,
}

/// `POST /admin/settings/appearance`: save the operator's choice.
pub async fn save(
    State(state): State<AdminState>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    Form(form): Form<AppearanceForm>,
) -> Response {
    let op = match require(&state, &headers, peer, Need::Change, Some(&form.csrf)).await {
        Ok(op) => op,
        Err(response) => return response,
    };
    let (Some(mode), Some(scheme)) = (Mode::parse(&form.mode), Scheme::parse(&form.scheme)) else {
        return message(
            &state,
            Some(Nav::new(&op, "settings")),
            StatusCode::BAD_REQUEST,
            "Not saved",
            "Choose one of the offered looks.",
            Some(("/admin/settings/appearance", "Appearance")),
        );
    };
    let chosen = Theme { mode, scheme };
    match theme::save(&state.pool, &op.did, chosen).await {
        Ok(_) => {
            tracing::info!(event = "admin_appearance_saved", did = %op.did, mode = mode.as_str(), scheme = scheme.as_str(), "an operator changed their admin appearance");
            Redirect::to("/admin/settings/appearance?saved=1").into_response()
        }
        Err(error) => super::internal_error(&state, &error),
    }
}
