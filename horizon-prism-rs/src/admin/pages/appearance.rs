//! Appearance (`/admin/appearance`, under the operator's name in the
//! sidebar; the old `/admin/settings/appearance` redirects, and its POST
//! still saves): the signed-in operator's own
//! look of these pages, light, dark or the device's, a colour scheme and a
//! font. Saved for that operator alone (`theme`), straight away:
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
use crate::admin::menu::PageId;
use crate::admin::theme::{self, Font, Mode, Scheme, Theme};

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
    fonts: Vec<Choice>,
    saved: bool,
}

/// `?saved=1` after a save.
#[derive(Debug, Default, serde::Deserialize)]
pub struct PageQuery {
    pub saved: Option<String>,
}

/// `GET /admin/appearance`.
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
            nav: Some(Nav::page(&op, PageId::Appearance)),
            title: "Appearance".to_string(),
            csrf: op.csrf.clone(),
            modes: Mode::ALL.into_iter().map(|m| Choice { value: m.as_str(), label: m.label(), checked: m == now.mode }).collect(),
            schemes: Scheme::ALL.into_iter().map(|s| Choice { value: s.as_str(), label: s.label(), checked: s == now.scheme }).collect(),
            fonts: Font::ALL.into_iter().map(|f| Choice { value: f.as_str(), label: f.label(), checked: f == now.font }).collect(),
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
    /// Absent on a form from before the font choice: the font stays.
    #[serde(default)]
    pub font: Option<String>,
}

/// `POST /admin/appearance` (and the old `/admin/settings/appearance`):
/// save the operator's choice.
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
    let font = match form.font.as_deref() {
        None => Some(op.theme.font),
        Some(name) => Font::parse(name),
    };
    let (Some(mode), Some(scheme), Some(font)) = (Mode::parse(&form.mode), Scheme::parse(&form.scheme), font) else {
        return message(
            &state,
            Some(Nav::at(&op, "/admin/appearance")),
            StatusCode::BAD_REQUEST,
            "Not saved",
            "Choose one of the offered looks.",
            Some(("/admin/appearance", "Appearance")),
        );
    };
    let chosen = Theme { mode, scheme, font };
    match theme::save(&state.pool, &op.did, chosen).await {
        Ok(_) => {
            tracing::info!(event = "admin_appearance_saved", did = %op.did, mode = mode.as_str(), scheme = scheme.as_str(), font = font.as_str(), "an operator changed their admin appearance");
            Redirect::to("/admin/appearance?saved=1").into_response()
        }
        Err(error) => super::internal_error(&state, &error),
    }
}
