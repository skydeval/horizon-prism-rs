//! The Apps pages: What apps see (`/admin/apps`) and Extra services
//! (`/admin/services`), each its settings alone, and Public page
//! (`/admin/public-page`), its switch and name with a link to the page
//! while it is on.

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
#[template(path = "apps.html")]
struct AppsPage {
    css: &'static str,
    nav: Option<Nav>,
    title: String,
    /// What the page is for, in one line.
    intro: &'static str,
    /// The public page's address, while it is on.
    view: Option<String>,
    /// Whether the page holds more than its settings, so they get a heading.
    heading: bool,
    settings: SettingsBlock,
}

/// One of the three pages: its intro, then its settings.
async fn show(state: &AdminState, headers: &HeaderMap, peer: SocketAddr, open: bool, id: PageId, intro: &'static str) -> Response {
    let op = match require(state, headers, peer, Need::Page, None).await {
        Ok(op) => op,
        Err(response) => return response,
    };
    let sections = view::sections_at(Place::Page(id), &state.settings.networks());
    let settings = match view::settings_block(state, &op.csrf, &sections, open).await {
        Ok(b) => b,
        Err(e) => return super::internal_error(state, &e),
    };
    let public = id == PageId::PublicPage;
    let view_it = (public && state.settings.live.load().public_page_enabled).then(|| format!("https://{}/", state.public_hostname));
    render(
        StatusCode::OK,
        &AppsPage { css: css(), nav: Some(Nav::page(&op, id)), title: id.page().name.to_string(), intro, view: view_it, heading: public, settings },
    )
}

fn open(q: &OpenQuery) -> bool {
    q.open.as_deref() == Some("advanced")
}

/// `GET /admin/apps`.
pub async fn apps(State(state): State<AdminState>, ConnectInfo(peer): ConnectInfo<SocketAddr>, headers: HeaderMap, Query(q): Query<OpenQuery>) -> Response {
    let intro = "What apps are told and shown: email confirmation, live now, trending topics, the texts for features Prism does not offer, bookmark counts and where public videos play from.";
    show(&state, &headers, peer, open(&q), PageId::Apps, intro).await
}

/// `GET /admin/services`.
pub async fn services(State(state): State<AdminState>, ConnectInfo(peer): ConnectInfo<SocketAddr>, headers: HeaderMap, Query(q): Query<OpenQuery>) -> Response {
    let intro = "Requests Prism passes on to a service set up for them at install, or answers that it does not offer.";
    show(&state, &headers, peer, open(&q), PageId::Services, intro).await
}

/// `GET /admin/public-page`.
pub async fn public_page(State(state): State<AdminState>, ConnectInfo(peer): ConnectInfo<SocketAddr>, headers: HeaderMap, Query(q): Query<OpenQuery>) -> Response {
    let intro = "The page at this server's own address: whether it is shown, and its name.";
    show(&state, &headers, peer, open(&q), PageId::PublicPage, intro).await
}
