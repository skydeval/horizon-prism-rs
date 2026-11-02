//! The Network pages beside Relays: Account lookups · lodestar
//! (`/admin/account-lookups`), lodestar's state and its counts with its
//! settings, the rate and longest wait folded; Profiles and caching
//! (`/admin/profiles`) and Request limits (`/admin/limits`), each its
//! settings alone, the limits as one table.

use std::net::SocketAddr;

use askama::Template;
use axum::extract::{ConnectInfo, Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::Response;

use super::health::Figure;
use super::{Nav, css, render};
use crate::admin::AdminState;
use crate::admin::auth::{Need, require};
use crate::admin::health::{Snapshot, count};
use crate::admin::menu::PageId;
use crate::admin::pages::settings::OpenQuery;
use crate::admin::settings::view::{self, Place, SettingsBlock};
use crate::admin::stats;

fn fig(label: &str, value: impl Into<String>) -> Figure {
    Figure { label: label.to_string(), value: value.into(), link: None }
}

#[derive(Template)]
#[template(path = "account_lookups.html")]
struct LookupsPage {
    css: &'static str,
    nav: Option<Nav>,
    title: String,
    /// Whether Prism reads from lodestar now, in a sentence.
    state: String,
    /// Lodestar's calls since the start.
    figures: Vec<Figure>,
    settings: SettingsBlock,
}

/// A page that is its settings alone.
#[derive(Template)]
#[template(path = "apps.html")]
struct SettingsOnly {
    css: &'static str,
    nav: Option<Nav>,
    title: String,
    intro: &'static str,
    view: Option<String>,
    heading: bool,
    settings: SettingsBlock,
}

/// Whether Prism reads from lodestar now, from its gate.
#[must_use]
pub fn state_words(state: &AdminState) -> String {
    match &state.settings.lodestar {
        None => "Lodestar is off: no address was set when Prism started.".to_string(),
        Some(l) if l.gate().is_open() => format!("Lodestar ({}) is answering and up to date, so Prism is reading from it.", l.host()),
        Some(l) => format!("Lodestar ({}) is not answering or is behind, so Prism is reading from the PLC directory.", l.host()),
    }
}

/// Lodestar's calls since the start: answered, not answered, not asked.
fn figures(state: &AdminState, s: &Snapshot) -> Vec<Figure> {
    use crate::identity::lodestar::{METRIC_CALLS, METRIC_SKIPPED};
    let Some(l) = &state.settings.lodestar else {
        return vec![fig("Lodestar", "not set up; Prism asks the PLC directory")];
    };
    let all = s.sum(METRIC_CALLS).unwrap_or(0.0);
    let ok = answered(s);
    vec![
        fig("Server", l.host().to_string()),
        fig("Answered by lodestar", count(ok)),
        fig("Not answered (Prism asked the PLC directory)", count((all - ok).max(0.0))),
        fig("Not asked (lodestar behind, or over its rate)", count(s.sum(METRIC_SKIPPED).unwrap_or(0.0))),
    ]
}

/// The lookups lodestar answered since the start.
fn answered(s: &Snapshot) -> f64 {
    ["answered", "not_found", "tombstoned"].iter().filter_map(|o| s.sum_where(crate::identity::lodestar::METRIC_CALLS, "outcome", o)).sum()
}

/// Lodestar in one line, for Stats.
#[must_use]
pub fn summary(state: &AdminState, s: &Snapshot) -> String {
    match &state.settings.lodestar {
        None => "not set up; Prism asks the PLC directory".to_string(),
        Some(l) => format!("{}, {} answered since Prism started", l.host(), count(answered(s))),
    }
}

/// `GET /admin/account-lookups`.
pub async fn account_lookups(State(state): State<AdminState>, ConnectInfo(peer): ConnectInfo<SocketAddr>, headers: HeaderMap, Query(q): Query<OpenQuery>) -> Response {
    let op = match require(&state, &headers, peer, Need::Page, None).await {
        Ok(op) => op,
        Err(response) => return response,
    };
    let sections = view::sections_at(Place::Page(PageId::AccountLookups), &state.settings.networks());
    let settings = match view::settings_block(&state, &op.csrf, &sections, q.open.as_deref() == Some("advanced")).await {
        Ok(b) => b,
        Err(e) => return super::internal_error(&state, &e),
    };
    let s = state.health.metrics.as_ref().map_or_else(|| stats::series(""), |m| stats::series(&m.render()));
    render(
        StatusCode::OK,
        &LookupsPage {
            css: css(),
            nav: Some(Nav::page(&op, PageId::AccountLookups)),
            title: PageId::AccountLookups.page().name.to_string(),
            state: state_words(&state),
            figures: figures(&state, &s),
            settings,
        },
    )
}

/// A page that is its settings alone, as cards or as one table.
async fn settings_only(state: &AdminState, headers: &HeaderMap, peer: SocketAddr, open: bool, id: PageId, intro: &'static str, table: bool) -> Response {
    let op = match require(state, headers, peer, Need::Page, None).await {
        Ok(op) => op,
        Err(response) => return response,
    };
    let sections = view::sections_at(Place::Page(id), &state.settings.networks());
    let settings = match view::settings_block(state, &op.csrf, &sections, open).await {
        Ok(b) if table => b.table(),
        Ok(b) => b,
        Err(e) => return super::internal_error(state, &e),
    };
    render(StatusCode::OK, &SettingsOnly { css: css(), nav: Some(Nav::page(&op, id)), title: id.page().name.to_string(), intro, view: None, heading: false, settings })
}

/// `GET /admin/profiles`.
pub async fn profiles(State(state): State<AdminState>, ConnectInfo(peer): ConnectInfo<SocketAddr>, headers: HeaderMap, Query(q): Query<OpenQuery>) -> Response {
    let intro = "How long profiles from other networks are reused before they are fetched again, and how fast each network's are refreshed in the background.";
    settings_only(&state, &headers, peer, q.open.as_deref() == Some("advanced"), PageId::Profiles, intro, false).await
}

/// `GET /admin/limits`.
pub async fn limits(State(state): State<AdminState>, ConnectInfo(peer): ConnectInfo<SocketAddr>, headers: HeaderMap, Query(q): Query<OpenQuery>) -> Response {
    let intro = "How many requests of each kind one visitor, account or address may make per second, and how many at once. Thread Lens's and Refract's limits are on their own pages.";
    settings_only(&state, &headers, peer, q.open.as_deref() == Some("advanced"), PageId::Limits, intro, true).await
}
