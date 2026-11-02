//! Thread Lens (`/admin/thread-lens`): what it is, then its settings: who
//! may use it, and, folded, its request limit.

use std::net::SocketAddr;

use askama::Template;
use axum::extract::{ConnectInfo, Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::Response;

use super::{Nav, css, render};
use crate::admin::AdminState;
use crate::admin::auth::{Need, require};
use crate::admin::menu::PageId;
use crate::admin::pages::settings::OpenQuery;
use crate::admin::settings::view::{self, Place, SettingsBlock};

#[derive(Template)]
#[template(path = "thread_lens.html")]
struct ThreadLensPage {
    css: &'static str,
    nav: Option<Nav>,
    title: String,
    /// Who may use it, and its request limit.
    settings: SettingsBlock,
}

/// `GET /admin/thread-lens`.
pub async fn page(State(state): State<AdminState>, ConnectInfo(peer): ConnectInfo<SocketAddr>, headers: HeaderMap, Query(q): Query<OpenQuery>) -> Response {
    let op = match require(&state, &headers, peer, Need::Page, None).await {
        Ok(op) => op,
        Err(response) => return response,
    };
    let sections = view::sections_at(Place::Page(PageId::ThreadLens), &state.settings.networks());
    let settings = match view::settings_block(&state, &op.csrf, &sections, q.open.as_deref() == Some("advanced")).await {
        Ok(b) => b,
        Err(e) => return super::internal_error(&state, &e),
    };
    render(StatusCode::OK, &ThreadLensPage { css: css(), nav: Some(Nav::page(&op, PageId::ThreadLens)), title: PageId::ThreadLens.page().name.to_string(), settings })
}
