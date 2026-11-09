//! Health (`/admin/health`): the checks with their state, and the key
//! figures in words, each feature's in one line leading to its page.
//! "Re-sync private data and members" (typed) sits beside a held gap. The
//! page reloads every 30 seconds, to `/admin/health?auto=1`, which is
//! watching and not activity, as Home's reload is.

use std::net::SocketAddr;

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
use crate::admin::health::{self, Action, Check, Snapshot, count, gather};
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
    /// The page this figure's subject has, and its name, after a dash.
    pub link: Option<(&'static str, &'static str)>,
}

fn fig(label: &str, value: impl Into<String>) -> Figure {
    Figure { label: label.to_string(), value: value.into(), link: None }
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

/// The network stream in one phrase, for Home's tile: the furthest relay's
/// delay.
#[must_use]
pub fn stream_words(now: Option<&Snapshot>) -> String {
    let Some(worst) = now.and_then(|s| s.max(crate::firehose::watermark::METRIC_DELAY)) else {
        return UNKNOWN.to_string();
    };
    if worst.is_infinite() {
        "a relay has never reported".to_string()
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
    match (now.and_then(|s| s.sum(crate::db::sampler::METRIC_DB_SIZE)), budget) {
        (None, _) => UNKNOWN.to_string(),
        (Some(size), Some(budget)) if budget > 0 => format!("{} of {} ({:.0}%)", bytes(size), bytes(budget as f64), size / budget as f64 * 100.0),
        (Some(size), _) => format!("{} (no budget set)", bytes(size)),
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

/// Where account lookups go, from lodestar's gate.
#[must_use]
pub fn lookups_words(state: &AdminState) -> String {
    match &state.settings.lodestar {
        None => "off (the PLC directory is asked)".to_string(),
        Some(l) if l.gate().is_open() => format!("{} is up to date; Prism reads from it first", l.host()),
        Some(l) => format!("{} is behind or not answering; the PLC directory is asked", l.host()),
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

/// The relays in one phrase: how many, the slowest, and the records
/// waiting.
#[must_use]
pub fn relays_words(state: &AdminState, now: Option<&Snapshot>) -> String {
    let mut relays: Vec<&String> = state.settings.running.firehose.relay_labels.values().collect();
    relays.sort();
    relays.dedup();
    let n = relays.len();
    let slowest = match now.and_then(|s| s.max(crate::firehose::watermark::METRIC_DELAY)) {
        None => "slowest unknown".to_string(),
        Some(d) if d.is_infinite() => "one has never reported".to_string(),
        Some(d) if d < 120.0 => format!("slowest {:.0} s behind", d.max(0.0)),
        Some(d) => format!("slowest {:.0} minutes behind", (d / 60.0).round()),
    };
    let waiting = now.and_then(|s| s.sum(crate::firehose::consumer::METRIC_LAG)).map_or_else(|| "records waiting unknown".to_string(), |l| format!("{} records waiting", count(l)));
    format!("{n} relay{}, {slowest}, {waiting}", if n == 1 { "" } else { "s" })
}

/// The key figures. Each feature's is one line, linking the page that
/// shows it in full.
fn figures(state: &AdminState, now: Option<&Snapshot>) -> Vec<FigureGroup> {
    let get = |name: &str| now.and_then(|s| s.sum(name));
    let mut groups = Vec::new();

    groups.push(FigureGroup {
        title: "Network stream",
        figures: vec![Figure { link: Some(("/admin/relays", "Relays")), ..fig("Relays", relays_words(state, now)) }],
        note: String::new(),
    });

    groups.push(FigureGroup {
        title: "Storage",
        figures: vec![Figure { link: Some(("/admin/storage", "Storage")), ..fig("Database (of its budget)", database_words(state, now)) }],
        note: String::new(),
    });
    groups.push(FigureGroup {
        title: "Public data",
        figures: vec![Figure { link: Some(("/admin/indexing", "Indexing")), ..fig("Public data", super::indexing::phase_words(state.scope.public_mode())) }],
        note: String::new(),
    });

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
    groups.push(FigureGroup {
        title: "Services Prism asks",
        figures: vec![
            Figure { link: Some(("/admin/blocks", "Blocks")), ..fig("Farsight (blocks)", farsight_words(state)) },
            Figure { link: Some(("/admin/account-lookups", "Account lookups")), ..fig("Lodestar (account lookups)", lookups_words(state)) },
        ],
        note: String::new(),
    });

    groups.push(FigureGroup {
        title: "Private posts",
        figures: vec![Figure {
            link: Some(("/admin/private", "Private posts")),
            ..fig("Could not be read (last hour)", or_unknown(super::private::bad_reads(state), count))
        }],
        note: String::new(),
    });
    groups
}

/// Full indexing's figures, while it runs or is being left: the network
/// (relays, histories, labelers, suspensions), the crawl, the writer
/// against the relays and the last full-check; and a trial's block while
/// one exists.
async fn full_groups(state: &AdminState) -> Vec<FigureGroup> {
    let public = state.public();
    let breadth = public.breadth();
    let mut out = Vec::new();
    if breadth != crate::indexing::Breadth::Community
        && let Ok(full) = public.full_status().await
    {
        let v = super::indexing::full_view(&full, breadth);
        out.push(FigureGroup { title: "Full network", figures: v.figures, note: "Its warnings are under Checks.".to_string() });
        if !v.crawl.is_empty() {
            out.push(FigureGroup { title: "Full crawl", figures: v.crawl, note: String::new() });
        }
        if !v.labels.is_empty() {
            out.push(FigureGroup { title: "Labels", figures: v.labels, note: "Labels from other labelers, as full indexing stores or reads them.".to_string() });
        }
        out.push(FigureGroup {
            title: "Suspensions",
            figures: if v.suspended.is_empty() { vec![fig("Accounts a takedown relay reports inactive", "none")] } else { v.suspended },
            note: "Accounts a takedown relay reports suspended are hidden unless Refract lists them, or every one is served under full.suspended_accounts = \"all\"."
                .to_string(),
        });
        let mut writer = Vec::new();
        if let Some(w) = super::indexing::writer_words(state).await {
            writer.push(fig("Against the relays", w));
        }
        writer.push(fig(
            "Last full-check",
            crate::indexing::full_check::last_words(&state.pool).await.ok().flatten().unwrap_or_else(|| "none run on this database".to_string()),
        ));
        out.push(FigureGroup { title: "Writer", figures: writer, note: String::new() });
    }
    if let Ok(trial) = public.trial_status().await {
        let t = super::indexing::trial_view(&trial);
        if !t.figures.is_empty() || t.last.is_some() {
            let mut figures = t.figures;
            if let Some(last) = t.last {
                figures.push(fig("Last result", last));
            }
            out.push(FigureGroup { title: "Full network trial", figures, note: String::new() });
        }
    }
    out
}

#[derive(Debug, Default, Deserialize)]
pub struct ResyncForm {
    #[serde(default)]
    pub csrf: String,
    pub confirm: Option<String>,
    pub typed: Option<String>,
    /// The page the form was on, for Cancel.
    pub back: Option<String>,
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
    let mut groups = figures(&state, now.as_ref());
    groups.extend(full_groups(&state).await);
    render(
        StatusCode::OK,
        &HealthPage {
            css: css(),
            nav: Some(Nav { banners: b, ..Nav::page(&op, crate::admin::menu::PageId::Health) }),
            title: "Health".to_string(),
            csrf: op.csrf.clone(),
            started: format!("{} ({})", when(started), ago(started)),
            young: chrono::Utc::now() - started < chrono::TimeDelta::minutes(5),
            checks: checks.into_iter().map(CheckView::from).collect(),
            groups,
        },
    )
}

/// `POST /admin/health/resync`: `resync --private-and-members`, typed
/// `RESYNC`.
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
    .page(&op, &super::back_to(&state, &headers, form.back.as_deref(), "/admin/health"));
    if let Some(response) = confirmed(page, form.confirm.as_deref(), form.typed.as_deref()) {
        return response;
    }
    enqueue(&state, &op, &AdminCommand::Resync).await
}

/// The checks that need attention, worst first, and how many have no data
/// (Home).
#[must_use]
pub fn attention(checks: Vec<health::Check>) -> (Vec<CheckView>, usize) {
    let no_data = checks.iter().filter(|c| c.level == health::Level::NoData).count();
    let mut needing: Vec<health::Check> = checks.into_iter().filter(|c| c.level.needs_attention()).collect();
    needing.sort_by_key(|c| c.level);
    (needing.into_iter().map(CheckView::from).collect(), no_data)
}
