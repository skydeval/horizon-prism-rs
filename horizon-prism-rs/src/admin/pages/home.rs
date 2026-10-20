//! Home (`/admin`, admin-ui design §6.1): what needs attention first (the
//! red and amber checks of §9.2, each linking to where to act), then the
//! tiles in a narrow column beside the live feed (admin-ui-live-feed design
//! §5): members, public data, the network stream, the database against its
//! budget, the fullest disk, Farsight and the last five commands. The
//! script fills the feed;
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

use super::health::CheckView;
use super::{COMMAND_COLUMNS, CommandRow, CommandTuple, Nav, css, render};
use crate::admin::auth::{Banners, Need, banners, require};
use crate::admin::health::gather;
use crate::admin::live::{STAT_SLOTS, since_words};
use crate::admin::{AdminState, JS_HREF, LivePage};
use crate::firehose::feed::{Outcome, Source, Totals};

/// The tiles (§6.1), as of a moment.
pub struct Tiles {
    pub members: i64,
    pub ready: i64,
    pub max_members: u64,
    pub denied: i64,
    /// Public data: on, filling, draining, off, held.
    pub public: &'static str,
    /// The network stream: up to date, or how far behind.
    pub stream: String,
    /// The database against its budget.
    pub database: String,
    /// The fullest disk (§6.1, §10.1).
    pub disk: String,
    pub farsight: &'static str,
    /// The last five commands.
    pub commands: Vec<CommandRow>,
    /// The checks that need attention, worst first.
    pub attention: Vec<CheckView>,
    /// Checks with nothing to judge by yet.
    pub no_data: usize,
    /// `HH:MM:SS`, UTC.
    pub as_of: String,
}

/// One row of the live figures: a total since start and the last minute's
/// rate. Every column has [`STAT_SLOTS`] rows (second pass): a blank row
/// fills a short column, and a long one's last row is "+N more", the rest
/// named in its `title`.
pub struct Figure {
    pub label: String,
    /// `total · rate`; empty on a blank row.
    pub value: String,
    /// The rows "+N more" stands for; empty otherwise.
    pub title: String,
    pub blank: bool,
}

impl Figure {
    fn blank() -> Self {
        Self { label: String::new(), value: String::new(), title: String::new(), blank: true }
    }
}

/// A count before it is laid out in rows.
struct Count {
    label: String,
    total: u64,
    rate: Option<f64>,
}

fn rate_words(rate: Option<f64>) -> String {
    rate.map_or_else(|| "—".to_string(), |r| format!("{r:.1}/s"))
}

impl Count {
    fn value(&self) -> String {
        format!("{} · {}", self.total, rate_words(self.rate))
    }
}

/// Exactly [`STAT_SLOTS`] rows of `counts`, in their order.
fn slots(counts: &[Count]) -> Vec<Figure> {
    let figure = |c: &Count| Figure { label: c.label.clone(), value: c.value(), title: String::new(), blank: false };
    let mut out: Vec<Figure> = if counts.is_empty() {
        vec![Figure { label: "none yet".to_string(), value: String::new(), title: String::new(), blank: false }]
    } else if counts.len() > STAT_SLOTS {
        let (shown, rest) = counts.split_at(STAT_SLOTS - 1);
        let more = Count {
            label: format!("+{} more", rest.len()),
            total: rest.iter().map(|c| c.total).sum(),
            rate: rest.iter().map(|c| c.rate).sum(),
        };
        let title = rest.iter().map(|c| format!("{}: {}", c.label, c.value())).collect::<Vec<_>>().join(", ");
        shown.iter().map(figure).chain(std::iter::once(Figure { title, ..figure(&more) })).collect()
    } else {
        counts.iter().map(figure).collect()
    };
    while out.len() < STAT_SLOTS {
        out.push(Figure::blank());
    }
    out
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
    /// Settings whose `app.toml` value changed under a saved one
    /// (admin-ui design §8.4).
    notices: Vec<super::settings::Notice>,
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
        Outcome::NoMemberSide => "no member side",
        Outcome::Written => "written",
        Outcome::ReferenceOnly => "reference only",
        Outcome::Unchanged => "unchanged",
        Outcome::Failed => "failed",
        Outcome::OtherReason => "other",
    }
}

/// The fullest disk in one phrase, for its tile.
#[must_use]
#[allow(clippy::cast_precision_loss)]
pub fn fullest_disk_words(d: &crate::admin::stats::Disks) -> String {
    d.fullest().map_or_else(
        || "unknown".to_string(),
        |disk| format!("{} free ({:.0}% used), {}", super::bytes(disk.space.free as f64), disk.space.used_ratio() * 100.0, disk.holds_words()),
    )
}

async fn load_tiles(state: &AdminState, banners: Banners) -> Result<Tiles, sqlx::Error> {
    let (members, ready, denied): (i64, i64, i64) = sqlx::query_as(
        "SELECT (SELECT count(*) FROM members), (SELECT count(*) FROM members WHERE ready_at IS NOT NULL), \
         (SELECT count(*) FROM member_denials)",
    )
    .fetch_one(&state.pool)
    .await?;
    let commands: Vec<CommandTuple> = sqlx::query_as(&format!(
        "SELECT {COMMAND_COLUMNS} FROM admin_commands c LEFT JOIN actors a ON a.did = c.requested_by ORDER BY c.id DESC LIMIT 5"
    ))
    .fetch_all(&state.pool)
    .await?;
    let (attention, no_data) = super::health::attention(gather(state, banners).await);
    let now = state.health.now();
    Ok(Tiles {
        members,
        ready,
        max_members: state.members().max_members,
        denied,
        public: super::health::public_word(state.scope.public_mode()),
        stream: super::health::stream_words(now.as_ref()),
        database: super::health::database_words(state, now.as_ref()),
        disk: fullest_disk_words(&crate::admin::stats::read_disks(&state.disks)),
        farsight: super::health::farsight_words(state),
        commands: commands.into_iter().map(CommandRow::from_tuple).collect(),
        attention,
        no_data,
        as_of: Utc::now().format("%H:%M:%S").to_string(),
    })
}

#[allow(clippy::cast_precision_loss)]
fn rate(minute: Option<&(Totals, std::time::Duration)>, of: impl Fn(&Totals) -> u64) -> Option<f64> {
    minute.map(|(delta, span)| of(delta) as f64 / span.as_secs_f64())
}

fn live_section(state: &AdminState) -> LiveSection {
    let hub = &state.live;
    let totals = hub.tap().totals();
    let minute = hub.last_minute();
    let minute = minute.as_ref();
    let mut indexed: Vec<Count> = totals
        .indexed_by_kind()
        .into_iter()
        .filter(|(_, n)| *n > 0)
        .map(|(kind, n)| Count {
            label: kind.words().to_string(),
            total: n,
            rate: rate(minute, |t| {
                t.indexed_by_kind().into_iter().find(|(k, _)| *k == kind).map_or(0, |(_, n)| n)
            }),
        })
        .collect();
    indexed.sort_by(|a, b| b.total.cmp(&a.total));
    let mut reasons: Vec<Count> = totals
        .reasons()
        .into_iter()
        .filter(|(_, n)| *n > 0)
        .map(|(o, n)| Count { label: reason_words(o).to_string(), total: n, rate: rate(minute, |t| t.outcome(o)) })
        .collect();
    reasons.sort_by(|a, b| b.total.cmp(&a.total));
    let other: Vec<Count> = [Outcome::Unchanged, Outcome::Failed]
        .into_iter()
        .map(|o| Count { label: reason_words(o).to_string(), total: totals.outcome(o), rate: rate(minute, |t| t.outcome(o)) })
        .chain(std::iter::once(Count {
            label: "identity and account events".to_string(),
            total: totals.identity_events(),
            rate: rate(minute, Totals::identity_events),
        }))
        .collect();
    LiveSection {
        started: since_words(hub.started()),
        indexed: slots(&indexed),
        reasons: slots(&reasons),
        other: slots(&other),
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
    // A poll carries no banners; the Settings check needs them.
    let b = if need == Need::Poll {
        match banners(&state).await {
            Ok(b) => b,
            Err(e) => return super::internal_error(&state, &e),
        }
    } else {
        op.banners
    };
    let tiles = match load_tiles(&state, b).await {
        Ok(t) => t,
        Err(e) => return super::internal_error(&state, &e),
    };
    let notices = match super::settings::file_notices(&state).await {
        Ok(n) => n,
        Err(e) => return super::internal_error(&state, &e),
    };
    let mut response = render(
        StatusCode::OK,
        &HomePage {
            css: css(),
            js: JS_HREF.as_str(),
            nav: Some(Nav { banners: b, ..Nav::new(&op, "home") }),
            title: "Home".to_string(),
            tiles,
            live: live_section(&state),
            notices,
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
    let b = match banners(&state).await {
        Ok(b) => b,
        Err(e) => return super::internal_error(&state, &e),
    };
    match load_tiles(&state, b).await {
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

    /// Second pass: every column has the same rows whatever is counted;
    /// past them, "+N more" names the rest in its title.
    #[test]
    fn every_column_has_the_same_rows() {
        let count = |label: &str, total| Count { label: label.to_string(), total, rate: Some(1.0) };
        let counts = |n: u64| -> Vec<Count> { (0..n).map(|i| count(&format!("k{i}"), 10 - i)).collect() };
        for n in 0..8 {
            let rows = slots(&counts(n));
            assert_eq!(rows.len(), STAT_SLOTS, "{n} counts");
            let filled = usize::try_from(n.max(1)).expect("small");
            assert_eq!(rows.iter().filter(|r| r.blank).count(), STAT_SLOTS.saturating_sub(filled), "{n} counts");
        }
        let counts = counts(5);
        let rows = slots(&counts);
        assert_eq!(rows[2].label, "+3 more");
        assert_eq!(rows[2].title, "k2: 8 · 1.0/s, k3: 7 · 1.0/s, k4: 6 · 1.0/s");
        assert_eq!(rows[2].value, "21 · 3.0/s");
        assert_eq!(slots(&[])[0].label, "none yet");
    }
}
