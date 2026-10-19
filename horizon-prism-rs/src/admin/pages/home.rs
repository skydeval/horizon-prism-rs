//! Home (`/admin`, admin-ui design §6.1): the members' counts in a narrow
//! column beside the live feed (admin-ui-live-feed design §5); the health
//! tiles and attention list come with S3. The script fills the feed;
//! without JavaScript the section shows the totals since Prism started
//! and the last minute's rates, and the page reloads
//! itself every 30 seconds to `/admin?auto=1`, a [`Need::Poll`].
//!
//! `/admin/tiles` is the tiles alone, a fragment the script fetches every
//! 30 seconds instead of reloading the page (§5.4).

use std::net::SocketAddr;

use askama::Template;
use axum::extract::{ConnectInfo, Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::Response;
use chrono::Utc;
use serde::Deserialize;

use super::{Nav, css, render};
use crate::admin::auth::{Need, require};
use crate::admin::{AdminState, JS_HREF, LivePage};
use crate::firehose::feed::{Outcome, Source, Totals};

/// The tiles (§6.1), as of a moment.
pub struct Tiles {
    pub members: i64,
    pub ready: i64,
    pub max_members: u64,
    pub denied: i64,
    /// `HH:MM:SS`, UTC.
    pub as_of: String,
}

/// One figure of the live section: a total since start and the last
/// minute's rate.
pub struct Figure {
    pub label: String,
    pub total: u64,
    pub rate: String,
}

/// A filter choice.
pub struct Choice {
    pub value: &'static str,
    pub label: &'static str,
}

/// The live section as the server renders it (§5.5).
pub struct LiveSection {
    /// When counting began.
    pub started: String,
    pub indexed: Vec<Figure>,
    pub reasons: Vec<Figure>,
    pub other: Vec<Figure>,
    pub kinds: Vec<Choice>,
    pub sources: Vec<Choice>,
}

#[derive(Template)]
#[template(path = "home.html")]
struct HomePage {
    css: &'static str,
    js: &'static str,
    nav: Option<Nav>,
    title: String,
    tiles: Tiles,
    live: LiveSection,
}

#[derive(Template)]
#[template(path = "tiles_fragment.html")]
struct TilesFragment {
    tiles: Tiles,
}

/// `?auto=1`: Home's own reload with JavaScript off.
#[derive(Debug, Default, Deserialize)]
pub struct HomeQuery {
    pub auto: Option<String>,
}

/// The kind filter's choices, as the script and the stream know them.
pub const KIND_CHOICES: [(&str, &str); 11] = [
    ("post", "posts"),
    ("reply", "replies"),
    ("like", "likes"),
    ("repost", "reposts"),
    ("follow", "follows"),
    ("block", "blocks"),
    ("profile", "profiles"),
    ("list", "lists"),
    ("private", "private"),
    ("identity", "identity"),
    ("other", "other"),
];

/// Reasons in words.
#[must_use]
pub fn reason_words(o: Outcome) -> &'static str {
    match o {
        Outcome::Unindexed => "not indexed",
        Outcome::KryphocronUnproduced => "private type not produced",
        Outcome::NotInScope => "not in scope",
        Outcome::PublicOff => "public data off",
        Outcome::AccountDeleted => "account deleted",
        Outcome::OutOfScope => "leaving scope",
        Outcome::MemberRemoved => "member removed",
        Outcome::Written => "written",
        Outcome::ReferenceOnly => "reference only",
        Outcome::Unchanged => "unchanged",
        Outcome::Failed => "failed",
        Outcome::OtherReason => "other",
    }
}

async fn load_tiles(state: &AdminState) -> Result<Tiles, sqlx::Error> {
    let (members, ready, denied): (i64, i64, i64) = sqlx::query_as(
        "SELECT (SELECT count(*) FROM members), (SELECT count(*) FROM members WHERE ready_at IS NOT NULL), \
         (SELECT count(*) FROM member_denials)",
    )
    .fetch_one(&state.pool)
    .await?;
    Ok(Tiles {
        members,
        ready,
        max_members: state.members.max_members,
        denied,
        as_of: Utc::now().format("%H:%M:%S").to_string(),
    })
}

#[allow(clippy::cast_precision_loss)]
fn rate(minute: Option<&(Totals, std::time::Duration)>, of: impl Fn(&Totals) -> u64) -> String {
    match minute {
        Some((delta, span)) => {
            let per_sec = of(delta) as f64 / span.as_secs_f64();
            format!("{per_sec:.1}/s")
        }
        None => "—".to_string(),
    }
}

fn live_section(state: &AdminState) -> LiveSection {
    let hub = &state.live;
    let totals = hub.tap().totals();
    let minute = hub.last_minute();
    let minute = minute.as_ref();
    let indexed = totals
        .indexed_by_kind()
        .into_iter()
        .filter(|(_, n)| *n > 0)
        .map(|(kind, n)| Figure {
            label: kind.words().to_string(),
            total: n,
            rate: rate(minute, |t| {
                t.indexed_by_kind().into_iter().find(|(k, _)| *k == kind).map_or(0, |(_, n)| n)
            }),
        })
        .collect();
    let reasons = totals
        .reasons()
        .into_iter()
        .filter(|(_, n)| *n > 0)
        .map(|(o, n)| Figure { label: reason_words(o).to_string(), total: n, rate: rate(minute, |t| t.outcome(o)) })
        .collect();
    let other = [Outcome::Unchanged, Outcome::Failed]
        .into_iter()
        .map(|o| Figure { label: reason_words(o).to_string(), total: totals.outcome(o), rate: rate(minute, |t| t.outcome(o)) })
        .chain(std::iter::once(Figure {
            label: "identity and account events".to_string(),
            total: totals.identity_events(),
            rate: rate(minute, Totals::identity_events),
        }))
        .collect();
    LiveSection {
        started: hub.started().format("%Y-%m-%d %H:%M UTC").to_string(),
        indexed,
        reasons,
        other,
        kinds: KIND_CHOICES.iter().map(|(value, label)| Choice { value, label }).collect(),
        sources: Source::ALL.iter().map(|s| Choice { value: s.slug(), label: s.slug() }).collect(),
    }
}

/// `GET /admin`.
pub async fn page(
    State(state): State<AdminState>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    Query(q): Query<HomeQuery>,
) -> Response {
    // The reload with JavaScript off is watching, not activity (§5.5).
    let need = if q.auto.as_deref() == Some("1") { Need::Poll } else { Need::Page };
    let op = match require(&state, &headers, peer, need, None).await {
        Ok(op) => op,
        Err(response) => return response,
    };
    let tiles = match load_tiles(&state).await {
        Ok(t) => t,
        Err(e) => return super::internal_error(&state, &e),
    };
    let mut response = render(
        StatusCode::OK,
        &HomePage {
            css: css(),
            js: JS_HREF.as_str(),
            nav: Some(Nav::new(&op, "home")),
            title: "Home".to_string(),
            tiles,
            live: live_section(&state),
        },
    );
    if response.status() == StatusCode::OK {
        response.extensions_mut().insert(LivePage);
    }
    response
}

/// `GET /admin/tiles`: the tiles alone (§5.4), a [`Need::Poll`].
pub async fn tiles(State(state): State<AdminState>, ConnectInfo(peer): ConnectInfo<SocketAddr>, headers: HeaderMap) -> Response {
    if let Err(response) = require(&state, &headers, peer, Need::Poll, None).await {
        return response;
    }
    match load_tiles(&state).await {
        Ok(tiles) => render(StatusCode::OK, &TilesFragment { tiles }),
        Err(e) => super::internal_error(&state, &e),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::firehose::feed::Kind;

    /// The kind choices are the stream's filters, and cover every kind.
    #[test]
    fn kind_choices_cover_every_kind() {
        for k in Kind::ALL {
            assert!(KIND_CHOICES.iter().any(|(v, _)| *v == k.filter()), "{k:?}");
        }
    }
}
