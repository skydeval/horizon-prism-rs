//! Private posts · kryphocron (`/admin/private`): how private posts are
//! read and what is stored of them, then their settings, with the timing
//! floors, the retry hints and how long apps trust a membership answer
//! folded. Nothing but the settings changes anything, and the private
//! tables are named only through the private tier.

use std::net::SocketAddr;
use std::time::Duration;

use askama::Template;
use axum::extract::{ConnectInfo, Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::Response;

use super::health::Figure;
use super::{Nav, css, render};
use crate::admin::AdminState;
use crate::admin::auth::{Need, require};
use crate::admin::health::{PRIVATE_INGEST_SOURCE, PRIVATE_READ_CONTENT, PRIVATE_READ_OUTCOMES, Snapshot, count};
use crate::admin::menu::PageId;
use crate::admin::pages::settings::OpenQuery;
use crate::admin::settings::view::{self, Place, SettingsBlock};
use crate::admin::stats;

const UNKNOWN: &str = "unknown";
const HOUR: Duration = Duration::from_secs(3600);

fn fig(label: &str, value: impl Into<String>) -> Figure {
    Figure { label: label.to_string(), value: value.into(), link: None }
}

#[allow(clippy::cast_precision_loss)]
fn exact(n: i64) -> String {
    count(n as f64)
}

fn or_unknown(v: Option<f64>) -> String {
    v.map_or_else(|| UNKNOWN.to_string(), count)
}

#[derive(Template)]
#[template(path = "private.html")]
struct PrivatePage {
    css: &'static str,
    nav: Option<Nav>,
    title: String,
    /// Reads in each form, bad reads, and where quotes and mentions came from.
    reads: Vec<Figure>,
    /// How many are stored, and how many carry images or a video.
    stored: Vec<Figure>,
    /// How deep, how fresh and how far back, with the rarer ones folded.
    settings: SettingsBlock,
}

/// Since the start: reads in the new and the older form, reads that failed
/// in the last hour, and the quotes and mentions taken from the new fields
/// or the older ones.
fn reads(state: &AdminState, now: Option<&Snapshot>) -> Vec<Figure> {
    let form = |f: &str| now.and_then(|s| s.sum_where(PRIVATE_READ_CONTENT, "form", f));
    // Quotes and mentions only: the same metric also counts media.
    let source = |s: &str| {
        now.and_then(|n| {
            let of = |field: &str| n.sum_where_all(PRIVATE_INGEST_SOURCE, &[("field", field), ("source", s)]);
            match (of("quote"), of("mentions")) {
                (None, None) => None,
                (q, m) => Some(q.unwrap_or(0.0) + m.unwrap_or(0.0)),
            }
        })
    };
    vec![
        fig("Read in the new form (text, links and embeds together)", or_unknown(form("content"))),
        fig("Read in the older form (text only)", or_unknown(form("text"))),
        fig("Could not be read (last hour)", or_unknown(bad_reads(state))),
        fig("Quotes and mentions from the new fields", or_unknown(source("dedicated"))),
        fig("Quotes and mentions from the older fields", or_unknown(source("clear"))),
    ]
}

/// Private reads that could not be read in the last hour.
#[must_use]
pub fn bad_reads(state: &AdminState) -> Option<f64> {
    state.health.ring.rise(HOUR, |s| Some(s.sum_where(PRIVATE_READ_OUTCOMES, "outcome", "bad_content").unwrap_or(0.0)))
}

/// The private posts stored, as Postgres last estimated them.
#[must_use]
pub fn stored_words(s: &Snapshot) -> String {
    s.sum_where(crate::db::sampler::METRIC_TABLE_ROWS, "table", crate::private_tier::stats::admin_posts_table())
        .map_or_else(|| UNKNOWN.to_string(), |n| format!("about {}", count(n)))
}

/// `GET /admin/private`.
pub async fn page(State(state): State<AdminState>, ConnectInfo(peer): ConnectInfo<SocketAddr>, headers: HeaderMap, Query(q): Query<OpenQuery>) -> Response {
    let op = match require(&state, &headers, peer, Need::Page, None).await {
        Ok(op) => op,
        Err(response) => return response,
    };
    let sections = view::sections_at(Place::Page(PageId::Private), &state.settings.networks());
    let settings = match view::settings_block(&state, &op.csrf, &sections, q.open.as_deref() == Some("advanced")).await {
        Ok(b) => b,
        Err(e) => return super::internal_error(&state, &e),
    };
    let s = state.health.metrics.as_ref().map_or_else(|| stats::series(""), |m| stats::series(&m.render()));
    let (images, video) = match crate::private_tier::stats::media_post_counts(&state.pool).await {
        Ok((i, v)) => (exact(i), exact(v)),
        Err(error) => {
            tracing::warn!(event = "admin_private_media_failed", %error, "the private media counts did not finish");
            (UNKNOWN.to_string(), UNKNOWN.to_string())
        }
    };
    let stored = vec![fig("Stored", stored_words(&s)), fig("With images", images), fig("With a video", video)];
    render(
        StatusCode::OK,
        &PrivatePage {
            css: css(),
            nav: Some(Nav::page(&op, PageId::Private)),
            title: PageId::Private.page().name.to_string(),
            reads: reads(&state, state.health.now().as_ref()),
            stored,
            settings,
        },
    )
}
