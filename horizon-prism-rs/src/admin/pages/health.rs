//! Health (`/admin/health`, admin-ui design §6.4): the checks of §9.2 with
//! their state, and the key figures of §9.1 in words. "Re-sync private
//! data and members" (typed) sits beside a held gap. The page reloads every
//! 30 seconds, to `/admin/health?auto=1`, which is watching and not
//! activity, as Home's reload is (admin-ui-live-feed design §3.3).

use std::net::SocketAddr;
use std::time::Duration;

use askama::Template;
use axum::Form;
use axum::extract::{ConnectInfo, Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::Response;
use serde::Deserialize;

use super::home::HomeQuery;
use super::{Confirm, Nav, ago, bytes, confirmed, css, enqueue, render, when};
use crate::admin::AdminState;
use crate::admin::auth::{Need, banners, require};
use crate::admin::health::{self, Action, Check, PRIVATE_INGEST_SOURCE, PRIVATE_READ_CONTENT, PRIVATE_READ_OUTCOMES, Snapshot, count, gather};
use crate::admin_executor::AdminCommand;
use crate::indexing::PublicMode;

/// A check as the page shows it.
pub struct CheckView {
    pub name: &'static str,
    pub class: &'static str,
    pub word: &'static str,
    pub text: String,
    pub href: String,
    pub link: String,
    /// Offer the typed re-sync beside it.
    pub resync: bool,
}

impl From<Check> for CheckView {
    fn from(c: Check) -> Self {
        let (href, link) = c.link.map_or_else(|| (String::new(), String::new()), |(h, l)| (h.to_string(), l.to_string()));
        Self {
            name: c.name,
            class: c.level.class(),
            word: c.level.word(),
            text: c.text,
            href,
            link,
            resync: c.action == Some(Action::Resync),
        }
    }
}

/// A label and its value in words.
pub struct Figure {
    pub label: String,
    pub value: String,
}

fn fig(label: &str, value: impl Into<String>) -> Figure {
    Figure { label: label.to_string(), value: value.into() }
}

/// A titled group of figures.
pub struct FigureGroup {
    pub title: &'static str,
    pub figures: Vec<Figure>,
    /// A sentence under the figures, when one helps.
    pub note: String,
}

#[derive(Template)]
#[template(path = "health.html")]
struct HealthPage {
    css: &'static str,
    nav: Option<Nav>,
    title: String,
    csrf: String,
    started: String,
    /// Under five minutes since the start: say why checks show no data.
    young: bool,
    checks: Vec<CheckView>,
    groups: Vec<FigureGroup>,
}

const UNKNOWN: &str = "unknown";
const FIVE_MIN: Duration = Duration::from_secs(300);
const HOUR: Duration = Duration::from_secs(3600);

/// The network stream in one phrase, for Home's tile: the furthest relay's
/// delay.
#[must_use]
pub fn stream_words(now: Option<&Snapshot>) -> String {
    let Some(worst) = now.and_then(|s| s.max(crate::firehose::watermark::METRIC_DELAY)) else {
        return UNKNOWN.to_string();
    };
    if worst.is_infinite() {
        "a relay has sent nothing since the start".to_string()
    } else if worst < 120.0 {
        "up to date".to_string()
    } else {
        format!("{:.0} minutes behind", (worst / 60.0).round())
    }
}

/// The database against its budget, for Home's tile and the figures.
#[must_use]
#[allow(clippy::cast_precision_loss)]
pub fn database_words(state: &AdminState, now: Option<&Snapshot>) -> String {
    let budget = state.settings.running.storage.budget_bytes;
    match now.and_then(|s| s.sum(crate::db::sampler::METRIC_DB_SIZE)) {
        None => UNKNOWN.to_string(),
        Some(size) if budget > 0 => format!("{} of {} ({:.0}%)", bytes(size), bytes(budget as f64), size / budget as f64 * 100.0),
        Some(size) => bytes(size),
    }
}

/// Farsight in a word, from Prism's own view of it.
#[must_use]
pub fn farsight_words(state: &AdminState) -> &'static str {
    match &state.health.farsight {
        None if state.settings.running.farsight.url.is_none() => "not set up",
        None => UNKNOWN,
        Some(f) if f.gate().down => "not answering",
        Some(f) if f.gate().answers == 0 => "no answer yet since Prism started",
        Some(_) => "answering",
    }
}

/// The public-data phase in a word.
#[must_use]
pub fn public_word(mode: PublicMode) -> &'static str {
    match mode {
        PublicMode::On => "on",
        PublicMode::Filling => "filling",
        PublicMode::Draining => "draining",
        PublicMode::Off => "off",
        PublicMode::Held => "held",
    }
}

fn or_unknown(v: Option<f64>, f: impl Fn(f64) -> String) -> String {
    v.map_or_else(|| UNKNOWN.to_string(), f)
}

/// The key figures (§6.4).
#[allow(clippy::too_many_lines)]
fn figures(state: &AdminState, now: Option<&Snapshot>) -> Vec<FigureGroup> {
    let ring = &state.health.ring;
    let get = |name: &str| now.and_then(|s| s.sum(name));
    let mut groups = Vec::new();

    let mut stream = Vec::new();
    let mut relays: Vec<String> = state.settings.running.firehose.relay_labels.values().cloned().collect();
    relays.sort();
    relays.dedup();
    for relay in relays {
        let delay = now.and_then(|s| s.sum_where(crate::firehose::watermark::METRIC_DELAY, "relay", &relay));
        stream.push(fig(
            &format!("{relay} delay"),
            match delay {
                None => UNKNOWN.to_string(),
                Some(d) if d.is_infinite() => "nothing received since Prism started".to_string(),
                Some(d) if d < 120.0 => format!("{d:.0} seconds"),
                Some(d) => format!("{:.0} minutes", (d / 60.0).round()),
            },
        ));
    }
    stream.push(fig("Records waiting", or_unknown(get(crate::firehose::consumer::METRIC_LAG), count)));
    stream.push(fig(
        "Processed per second (last 5 minutes)",
        or_unknown(ring.rate(FIVE_MIN, |s| s.sum(crate::firehose::consumer::METRIC_PROCESSED)), |r| format!("{r:.1}")),
    ));
    groups.push(FigureGroup {
        title: "Network stream",
        figures: stream,
        note: "The live feed on Home shows what is indexed as it happens.".to_string(),
    });

    let redis = match (get(crate::db::sampler::METRIC_REDIS_USED), get(crate::db::sampler::METRIC_REDIS_MAX)) {
        (Some(used), Some(max)) if max > 0.0 => format!("{} of {} ({:.0}%)", bytes(used), bytes(max), used / max * 100.0),
        (Some(used), _) => format!("{} (no limit set)", bytes(used)),
        _ => UNKNOWN.to_string(),
    };
    groups.push(FigureGroup {
        title: "Storage",
        figures: vec![fig("Database (of its budget)", database_words(state, now)), fig("Redis memory", redis)],
        note: "Measured every 5 minutes (Redis every 30 seconds).".to_string(),
    });

    let mode = state.scope.public_mode();
    let mut public = vec![fig("Public data", super::settings::phase_words(mode))];
    if mode == PublicMode::Filling {
        public.push(fig("Accounts left to refill", or_unknown(get(crate::indexing::public_fill::METRIC_REMAINING), count)));
    }
    groups.push(FigureGroup { title: "Public data", figures: public, note: String::new() });

    let schema = match &state.health.schema {
        None => UNKNOWN.to_string(),
        Some(s) if s.backfill_done() && s.indexes_ready() => "done".to_string(),
        Some(s) if s.backfill_done() => "rows done; search indexes still being built".to_string(),
        Some(_) => "still filling older rows".to_string(),
    };
    groups.push(FigureGroup {
        title: "Search preparation",
        figures: vec![
            fig("State", schema),
            fig("Rows left to fill", or_unknown(get(crate::db::schema_backfill::METRIC_REMAINING), count)),
            fig("Indexes left to build", or_unknown(get(crate::db::schema_backfill::METRIC_INDEX_PENDING), count)),
        ],
        note: String::new(),
    });

    let lookups = match &state.settings.lodestar {
        None => "off (the PLC directory is asked)".to_string(),
        Some(l) if l.gate().is_open() => format!("{} is up to date; Prism reads from it first", l.host()),
        Some(l) => format!("{} is behind or not answering; the PLC directory is asked", l.host()),
    };
    groups.push(FigureGroup {
        title: "Services Prism asks",
        figures: vec![fig("Farsight (blocks)", farsight_words(state)), fig("Lodestar (account lookups)", lookups)],
        note: String::new(),
    });

    // Private posts' new form (private-embeds design §9).
    let form = |f: &str| now.and_then(|s| s.sum_where(PRIVATE_READ_CONTENT, "form", f));
    // Quotes and mentions only: the same metric also counts `field="media"`
    // (private-media-filters design §8).
    let source = |s: &str| {
        now.and_then(|n| {
            let of = |field: &str| n.sum_where_all(PRIVATE_INGEST_SOURCE, &[("field", field), ("source", s)]);
            match (of("quote"), of("mentions")) {
                (None, None) => None,
                (q, m) => Some(q.unwrap_or(0.0) + m.unwrap_or(0.0)),
            }
        })
    };
    let bad = ring.rise(HOUR, |s| Some(s.sum_where(PRIVATE_READ_OUTCOMES, "outcome", "bad_content").unwrap_or(0.0)));
    groups.push(FigureGroup {
        title: "Private posts",
        figures: vec![
            fig("Read in the new form (text, links and embeds together)", or_unknown(form("content"), count)),
            fig("Read in the older form (text only)", or_unknown(form("text"), count)),
            fig("Could not be read (last hour)", or_unknown(bad, count)),
            fig("Quotes and mentions from the new fields", or_unknown(source("dedicated"), count)),
            fig("Quotes and mentions from the older fields", or_unknown(source("clear"), count)),
        ],
        note: "Counted since Prism started. A post that could not be read means its server and Prism disagree on the new form.".to_string(),
    });
    groups
}

#[derive(Debug, Default, Deserialize)]
pub struct ResyncForm {
    #[serde(default)]
    pub csrf: String,
    pub confirm: Option<String>,
    pub typed: Option<String>,
}

/// `GET /admin/health`; with `?auto=1` (its own reload) a [`Need::Poll`].
pub async fn page(State(state): State<AdminState>, ConnectInfo(peer): ConnectInfo<SocketAddr>, headers: HeaderMap, Query(q): Query<HomeQuery>) -> Response {
    let need = if q.auto.as_deref() == Some("1") { Need::Poll } else { Need::Page };
    let op = match require(&state, &headers, peer, need, None).await {
        Ok(op) => op,
        Err(response) => return response,
    };
    // A poll has no banners; the Settings check needs them.
    let b = if need == Need::Poll {
        match banners(&state).await {
            Ok(b) => b,
            Err(e) => return super::internal_error(&state, &e),
        }
    } else {
        op.banners
    };
    let checks = gather(&state, b).await;
    let now = state.health.now();
    let started = state.settings.started_at;
    render(
        StatusCode::OK,
        &HealthPage {
            css: css(),
            nav: Some(Nav { banners: b, ..Nav::new(&op, "health") }),
            title: "Health".to_string(),
            csrf: op.csrf.clone(),
            started: format!("{} ({})", when(started), ago(started)),
            young: chrono::Utc::now() - started < chrono::TimeDelta::minutes(5),
            checks: checks.into_iter().map(CheckView::from).collect(),
            groups: figures(&state, now.as_ref()),
        },
    )
}

/// `POST /admin/health/resync`: `resync --private-and-members`, typed
/// `RESYNC` (§6.4, §7).
pub async fn resync(State(state): State<AdminState>, ConnectInfo(peer): ConnectInfo<SocketAddr>, headers: HeaderMap, Form(form): Form<ResyncForm>) -> Response {
    let op = match require(&state, &headers, peer, Need::Change, Some(&form.csrf)).await {
        Ok(op) => op,
        Err(response) => return response,
    };
    let page = Confirm {
        title: "Re-sync private data and members",
        what: "Prism reads every member's account again from its server and checks again who may see each private post. Until that is done, members are shown as not ready and their private posts are held. With many members this takes a while and uses their servers' allowance.".to_string(),
        undo: "This cannot be stopped once started; it finishes in the background.",
        action: "/admin/health/resync",
        hidden: Vec::new(),
        typed: Some("RESYNC".to_string()),
        button: "Re-sync",
    }
    .page(&op, "health");
    if let Some(response) = confirmed(page, form.confirm.as_deref(), form.typed.as_deref()) {
        return response;
    }
    enqueue(&state, &op, &AdminCommand::Resync).await
}

/// The checks that need attention, worst first, and how many have no data
/// (Home, §6.1).
#[must_use]
pub fn attention(checks: Vec<health::Check>) -> (Vec<CheckView>, usize) {
    let no_data = checks.iter().filter(|c| c.level == health::Level::NoData).count();
    let mut needing: Vec<health::Check> = checks.into_iter().filter(|c| c.level.needs_attention()).collect();
    needing.sort_by_key(|c| c.level);
    (needing.into_iter().map(CheckView::from).collect(), no_data)
}
