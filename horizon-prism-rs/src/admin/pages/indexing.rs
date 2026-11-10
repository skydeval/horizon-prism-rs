//! Indexing (`/admin/indexing`): what Prism indexes, and its progress. The
//! switch between Kryphocron only and Community has its own form and
//! command, which moves through phases; turning public data off is typed
//! (`OFF`). While a refill runs, how many accounts are left and when the
//! longest wait ends. Then its setting, the longest wait.

use std::net::SocketAddr;

use askama::Template;
use axum::Form;
use axum::extract::{ConnectInfo, Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::Response;
use chrono::{DateTime, TimeDelta, Utc};
use serde_json::Value;

use super::health::Figure;
use super::{ConfirmPage, Hidden, Nav, ago, back_to, css, enqueue, message, render, typed_matches, when};
use crate::admin::AdminState;
use crate::admin::auth::{Need, require};
use crate::admin::health::count;
use crate::admin::stats;
use crate::admin::menu::PageId;
use crate::admin::pages::settings::OpenQuery;
use crate::admin::settings::store::PUBLIC_SCOPE_KEY;
use crate::admin::settings::view::{self, Place, SettingsBlock};
use crate::admin_executor::AdminCommand;
use crate::indexing::public_mode::{TRANSITION_KEY, parse_ts};
use crate::indexing::{PublicMode, ScopeValue};

/// The page's own path, where the switch's refusals and Cancel go back to.
const HERE: &str = "/admin/indexing";

/// The public-data switch as the page shows it.
pub struct PublicView {
    /// The phase in words.
    pub phase: String,
    /// The saved choice: `kryphocron_only`, `community` or `full`.
    pub chosen: String,
}

/// Full indexing as the page shows it, while it runs or is being left.
#[derive(Default)]
pub struct FullView {
    pub shown: bool,
    pub figures: Vec<Figure>,
    pub crawl: Vec<Figure>,
    pub labels: Vec<Figure>,
    pub suspended: Vec<Figure>,
    pub warnings: Vec<String>,
}

/// The full network trial as the page shows it.
#[derive(Default)]
pub struct TrialView {
    pub figures: Vec<Figure>,
    /// The last result, in words.
    pub last: Option<String>,
    pub warnings: Vec<String>,
    /// A trial may be started now.
    pub can_start: bool,
    /// One is running and may be ended.
    pub can_end: bool,
    /// A failed trial's record may be dismissed.
    pub can_dismiss: bool,
}

#[derive(Template)]
#[template(path = "indexing.html")]
struct IndexingPage {
    css: &'static str,
    nav: Option<Nav>,
    title: String,
    csrf: String,
    public: PublicView,
    /// A refill's progress, while one runs.
    refill: Vec<Figure>,
    full: FullView,
    trial: TrialView,
    /// The longest wait for a refill.
    settings: SettingsBlock,
}

/// The public-data phase in words; Health and About say it too.
#[must_use]
pub fn phase_words(mode: PublicMode) -> &'static str {
    match mode {
        PublicMode::On => "on",
        PublicMode::Filling => "filling: public data is being refilled, and public requests come back when it is done",
        PublicMode::Draining => "draining: stored public data is being deleted in the background",
        PublicMode::Off => "off",
        PublicMode::Held => "held: Prism could not read the state of this switch, so it keeps public data off and changes nothing until it is set again",
    }
}

fn figure(label: &str, value: String) -> Figure {
    Figure { label: label.to_string(), value, link: None }
}

/// When a refill's last accounts started being waited for, from the stored
/// phase; `None` while accounts are still being queued.
async fn waiting_since(state: &AdminState) -> Result<Option<DateTime<Utc>>, sqlx::Error> {
    let raw: Option<Value> = sqlx::query_scalar("SELECT value FROM worker_state WHERE key = $1").bind(TRANSITION_KEY).fetch_optional(&state.pool).await?;
    Ok(raw.as_ref().and_then(|t| t.get("fill")).and_then(|f| f.get("waiting_since")).and_then(parse_ts))
}

/// When the longest wait ends: `hours` after `since`, or, before the wait
/// starts, how long it will be.
#[must_use]
pub fn wait_words(since: Option<DateTime<Utc>>, hours: u32, now: DateTime<Utc>) -> String {
    match since {
        None => format!("{hours} hours from when every account is queued"),
        Some(since) => {
            let end = since + TimeDelta::hours(i64::from(hours));
            if end <= now { "now: public requests are answered once this refill's check runs".to_string() } else { format!("{} (in {})", when(end), until(end - now)) }
        }
    }
}

/// A span ahead, in words.
fn until(d: TimeDelta) -> String {
    let minutes = d.num_minutes().max(1);
    if minutes < 120 {
        format!("{minutes} minutes")
    } else {
        format!("{} hours", (minutes + 30) / 60)
    }
}

/// The switch's saved choice, `kryphocron_only` when none is saved.
///
/// # Errors
///
/// Database errors.
pub async fn chosen(state: &AdminState) -> Result<String, sqlx::Error> {
    let value: Option<Value> = sqlx::query_scalar("SELECT value FROM operator_settings WHERE key = $1").bind(PUBLIC_SCOPE_KEY).fetch_optional(&state.pool).await?;
    Ok(value.as_ref().and_then(Value::as_str).and_then(ScopeValue::parse).unwrap_or(ScopeValue::KryphocronOnly).as_str().to_string())
}

/// Whether Community is the switch's saved choice.
///
/// # Errors
///
/// Database errors.
pub async fn community_chosen(state: &AdminState) -> Result<bool, sqlx::Error> {
    Ok(chosen(state).await? == ScopeValue::Community.as_str())
}

fn text(v: &Value) -> String {
    v.as_str().map_or_else(|| "unknown".to_string(), str::to_string)
}

fn number(v: &Value) -> String {
    v.as_f64().map_or_else(|| "unknown".to_string(), count)
}

fn time_words(v: &Value) -> String {
    v.as_str().and_then(|t| chrono::DateTime::parse_from_rfc3339(t).ok()).map_or_else(|| text(v), |t| when(t.with_timezone(&Utc)))
}

/// A trial's block, from `trial_status`: its state and figures, the last
/// result in words, its warnings.
#[must_use]
pub fn trial_view(trial: &Value) -> TrialView {
    let mut v = TrialView::default();
    let t = &trial["trial"];
    if t.is_object() {
        let state = text(&t["state"]);
        v.figures.push(figure("State", match state.as_str() {
            "running" => "running".to_string(),
            "ending" => "ending: the rows it added are being removed".to_string(),
            "checking" => "checking that everything it added is gone".to_string(),
            "failed" => "its check failed: see the warnings".to_string(),
            other => other.to_string(),
        }));
        v.figures.push(figure("From", text(&t["from"])));
        v.figures.push(figure("Since", time_words(&t["since"])));
        v.figures.push(figure("Ends at", time_words(&t["ends_at"])));
        v.figures.push(figure("What it indexes", if t["live_only"] == Value::Bool(true) { "the live network only".to_string() } else { "the live network and history".to_string() }));
        if let Some(g) = t["database_growth"].as_str() {
            v.figures.push(figure("Database growth", g.to_string()));
        }
        if let Some(rows) = t["recorded"].as_object() {
            let total: f64 = rows.values().filter_map(Value::as_f64).sum();
            v.figures.push(figure("Rows recorded", format!("{} in {} tables", count(total), rows.len())));
        }
        if let Some(b) = t["record_bytes"].as_f64() {
            v.figures.push(figure("Record size", super::bytes(b)));
        }
        if let Some(f) = t["free"].as_str() {
            v.figures.push(figure("Free on the database volume", f.to_string()));
        }
        if state == "checking"
            && let Some(step) = t.pointer("/check/step").and_then(Value::as_str)
        {
            v.figures.push(figure("Check", step.to_string()));
        }
        v.can_end = state == "running";
        v.can_dismiss = state == "failed";
    }
    v.last = trial.pointer("/last/words").and_then(Value::as_str).map(str::to_string);
    v.warnings = trial["warnings"].as_array().into_iter().flatten().filter_map(|w| w.as_str().map(str::to_string)).collect();
    v
}

/// Full indexing's figures, from `full_status`: the relays, each group's
/// history, the crawl and its directory reads, the labelers, the
/// suspensions; and its warnings.
#[must_use]
pub fn full_view(full: &Value, breadth: crate::indexing::Breadth) -> FullView {
    let mut v = FullView { shown: breadth != crate::indexing::Breadth::Community, ..FullView::default() };
    v.warnings = full["warnings"].as_array().into_iter().flatten().filter_map(|w| w.as_str().map(str::to_string)).collect();
    for r in full["relays"].as_array().into_iter().flatten() {
        let tier = match r["tier"].as_str() {
            Some("whole") => "passes everything",
            Some("covered") => "passes what full indexing stores",
            Some("aside") => "set aside for full indexing",
            _ => "unknown",
        };
        let reader = if r["stamped"] == Value::Bool(false) { ", older reader" } else { "" };
        v.figures.push(figure(&format!("Relay {}", text(&r["relay"])), format!("{tier}{reader}")));
    }
    let groups = full.pointer("/groups/groups").and_then(Value::as_object);
    for (name, g) in groups.into_iter().flatten() {
        let state = match g["state"].as_str() {
            Some("complete") if g["incomplete"] == Value::Bool(true) => "complete, marked by hand",
            Some("complete") => "complete",
            Some("crawling") => "history being crawled",
            Some("paused") => "paused",
            Some("removed") => "not indexed",
            _ => "unknown",
        };
        v.figures.push(figure(&format!("History of {}", name.replace('_', " ")), state.to_string()));
    }
    let crawl = &full["crawl"];
    if crawl.is_object() {
        let running = if crawl["paused"] == Value::Bool(true) {
            "paused at the free-space floor"
        } else if crawl["running"] == Value::Bool(true) {
            "running"
        } else if crawl["history"] == Value::Bool(false) {
            "off: live only"
        } else {
            "waiting"
        };
        v.crawl.push(figure("History crawl", running.to_string()));
        v.crawl.push(figure("Accounts crawled since Prism started", number(&crawl["crawled"])));
        let f = &crawl["figures"];
        if let Some(states) = f["states"].as_object() {
            for (state, words) in [("pending", "Accounts waiting"), ("fetching", "Accounts being fetched"), ("done", "Accounts done"), ("failed", "Accounts that failed"), ("gone", "Accounts gone"), ("skipped", "Accounts skipped (suspended)")] {
                if let Some(n) = states.get(state) {
                    v.crawl.push(figure(words, number(n)));
                }
            }
        }
        for r in f["failed_reasons"].as_array().into_iter().flatten() {
            v.crawl.push(figure(&format!("Failed: {}", text(&r["reason"])), number(&r["accounts"])));
        }
        for (g, n) in f["incomplete"].as_object().into_iter().flatten() {
            v.crawl.push(figure(&format!("Left incomplete in {g}"), number(n)));
        }
        for u in f["uncovered"].as_array().into_iter().flatten() {
            v.crawl.push(figure(&format!("Carried only by {}", text(&u["relay"])), number(&u["accounts"])));
        }
        if let Some(share) = f["blocked_share_7d"].as_f64() {
            v.crawl.push(figure("Local serving blocked (last 7 days)", format!("{:.0}%", share * 100.0)));
        }
        let identity = if crawl.pointer("/export/active") == Some(&Value::Bool(true)) {
            let at = f.pointer("/export/after").and_then(Value::as_i64).map_or_else(|| "starting".to_string(), |a| format!("at sequence {}", count(a as f64)));
            format!("the directory's export, {at}")
        } else {
            "the directory, one account at a time".to_string()
        };
        v.crawl.push(figure("Account details read from", identity));
        if let Some(at) = f["at"].as_str() {
            v.crawl.push(figure("Figures read", time_words(&Value::String(at.to_string()))));
        }
    }
    for l in full.pointer("/labels/sources").and_then(Value::as_array).into_iter().flatten() {
        let state = if l["replaying"] == Value::Bool(true) {
            "replaying its history; read from it meanwhile"
        } else if l["caught_up"] == Value::Bool(true) {
            "up to date"
        } else {
            "connecting"
        };
        v.labels.push(figure(&format!("Labeler {}", text(&l["labeler"])), state.to_string()));
        if l["unverified_subjects"].as_i64().unwrap_or(0) > 0 {
            v.labels.push(figure("Subjects with an unchecked label", number(&l["unverified_subjects"])));
        }
    }
    if full.pointer("/labels/mode").and_then(Value::as_str) == Some("upstream") {
        v.labels.push(figure("Labels", "read from each labeler per request".to_string()));
    }
    for x in full["suspended"].as_array().into_iter().flatten() {
        v.suspended.push(figure(&format!("Accounts {} on {}", text(&x["status"]), text(&x["relay"])), number(&x["accounts"])));
    }
    v
}

/// `GET /admin/indexing`.
pub async fn page(State(state): State<AdminState>, ConnectInfo(peer): ConnectInfo<SocketAddr>, headers: HeaderMap, Query(q): Query<OpenQuery>) -> Response {
    let op = match require(&state, &headers, peer, Need::Page, None).await {
        Ok(op) => op,
        Err(response) => return response,
    };
    let sections = view::sections_at(Place::Page(PageId::Indexing), &state.settings.networks());
    let settings = match view::settings_block(&state, &op.csrf, &sections, q.open.as_deref() == Some("advanced")).await {
        Ok(b) => b,
        Err(e) => return super::internal_error(&state, &e),
    };
    let chosen = match chosen(&state).await {
        Ok(c) => c,
        Err(e) => return super::internal_error(&state, &e),
    };
    let public = state.public();
    let (full, trial) = match (public.full_status().await, public.trial_status().await) {
        (Ok(f), Ok(t)) => (f, t),
        (Err(e), _) | (_, Err(e)) => return super::internal_error(&state, &e),
    };
    let breadth = state.scope.public_breadth();
    let mut trial = trial_view(&trial);
    // A trial may start from Community or Kryphocron only, settled, with no
    // record of an earlier one waiting.
    trial.can_start = breadth == crate::indexing::Breadth::Community
        && matches!(state.scope.public_mode(), PublicMode::On | PublicMode::Off)
        && !trial.can_end
        && !trial.can_dismiss
        && trial.figures.is_empty();
    let full = full_view(&full, breadth);
    let mode = state.scope.public_mode();
    let mut refill = Vec::new();
    if mode == PublicMode::Filling {
        let since = match waiting_since(&state).await {
            Ok(s) => s,
            Err(e) => return super::internal_error(&state, &e),
        };
        let left = state.health.now().and_then(|s| s.sum(crate::indexing::public_fill::METRIC_REMAINING));
        refill.push(figure("Accounts left to refill", left.map_or_else(|| "unknown".to_string(), count)));
        if let Some(s) = since {
            refill.push(figure("Waiting for the last accounts since", format!("{} ({})", when(s), ago(s))));
        }
        refill.push(figure("Longest wait ends", wait_words(since, state.settings.running.indexing.fill_max_wait_hours, Utc::now())));
    }
    render(
        StatusCode::OK,
        &IndexingPage {
            css: css(),
            nav: Some(Nav::page(&op, PageId::Indexing)),
            title: PageId::Indexing.page().name.to_string(),
            csrf: op.csrf.clone(),
            public: PublicView { phase: phase_words(mode).to_string(), chosen },
            refill,
            full,
            trial,
            settings,
        },
    )
}

/// The switch's form.
#[derive(Debug, serde::Deserialize)]
pub struct SwitchForm {
    #[serde(default)]
    pub csrf: String,
    #[serde(default)]
    pub value: String,
    pub confirm: Option<String>,
    pub typed: Option<String>,
    /// The page the form was on, for Cancel.
    pub back: Option<String>,
}

/// `POST /admin/indexing/switch`, also at `/admin/settings/public-scope`
/// for pages loaded before it moved: its own command, which moves through
/// phases. Turning public data off is typed (`OFF`).
pub async fn switch(State(state): State<AdminState>, ConnectInfo(peer): ConnectInfo<SocketAddr>, headers: HeaderMap, Form(form): Form<SwitchForm>) -> Response {
    let op = match require(&state, &headers, peer, Need::Change, Some(&form.csrf)).await {
        Ok(op) => op,
        Err(response) => return response,
    };
    let Some(value) = ScopeValue::parse(&form.value) else {
        return message(&state, Some(Nav::at(&op, HERE)), StatusCode::BAD_REQUEST, "Not saved", "Choose Kryphocron only, Community or Full network.", Some((HERE, "Indexing")));
    };
    let full_running = state.scope.public_breadth() == crate::indexing::Breadth::Full;
    let ask: Option<(&str, String, String, String)> = match value {
        ScopeValue::KryphocronOnly if state.scope.public_mode() != PublicMode::Off => Some((
            "OFF",
            "Switch to Kryphocron only".to_string(),
            "Prism stops answering public Bluesky requests and deletes the public data it stored, in the background. Only what kryphocron posts and Horizon features need is kept: blocks, follows involving members, profiles, and members' own records.".to_string(),
            "Switching back to Community refills public data; public requests come back when the refill is done.".to_string(),
        )),
        ScopeValue::Full if !full_running => {
            let used: Option<i64> = sqlx::query_scalar("SELECT pg_database_size(current_database())").fetch_one(&state.pool).await.ok();
            let free = stats::space(&state.disks.postgres).or_else(|| stats::space(&state.disks.data)).map(|s| s.free);
            let running = &state.settings.running;
            let mut warnings = crate::indexing::full::warnings(&running.full.kinds, used.and_then(|u| u64::try_from(u).ok()), free, running.storage.budget_bytes, running.firehose.writer_lanes);
            warnings.extend(writer_words(&state).await);
            warnings.extend(crate::indexing::full::postgres_warnings(&state.pool).await);
            warnings.extend(crate::indexing::full::more_warnings(running, crate::indexing::full_check::last_words(&state.pool).await.ok().flatten()));
            Some((
                "FULL",
                "Switch to Full network".to_string(),
                format!("Prism stores and serves the public network for the enabled record groups. {}", warnings.join(" ")),
                "Switching back to Community deletes what full added and refills S; public requests answer 501 during that refill.".to_string(),
            ))
        }
        ScopeValue::Community if full_running => Some((
            "COMMUNITY",
            "Leave Full network for Community".to_string(),
            "Prism stops storing accounts outside S and deletes the rows full added, in the background, then refills S. Public requests answer 501 during that refill, for up to the fill's longest wait.".to_string(),
            "Switching back to Full network starts full again; what was deleted is stored again from then on.".to_string(),
        )),
        _ => None,
    };
    if let Some((word, title, what, undo)) = ask {
        let back = back_to(&state, &headers, form.back.as_deref(), HERE);
        let mut page = ConfirmPage {
            css: css(),
            nav: Some(Nav::at(&op, &back)),
            back,
            title,
            what,
            undo,
            action: "/admin/indexing/switch".to_string(),
            csrf: op.csrf.clone(),
            hidden: vec![Hidden { name: "value".to_string(), value: value.as_str().to_string() }],
            typed: Some(word.to_string()),
            button: "Switch".to_string(),
            error: None,
        };
        if form.confirm.as_deref() != Some("yes") {
            return render(StatusCode::OK, &page);
        }
        if !typed_matches(word, form.typed.as_deref()) {
            page.error = Some(format!("Type {word} exactly to confirm."));
            return render(StatusCode::BAD_REQUEST, &page);
        }
    }
    enqueue(&state, &op, &AdminCommand::IndexingPublicScope { value }).await
}

/// The live writer against the relays over the last 10 minutes, in words,
/// when the ring has seen both.
pub async fn writer_words(state: &AdminState) -> Option<String> {
    let window = std::time::Duration::from_secs(600);
    let applied = state.health.ring.rate(window, |s| s.sum(crate::firehose::consumer::METRIC_PROCESSED));
    let added = state.health.ring.rate(window, |s| s.sum(crate::firehose::consumer::METRIC_STREAM_ADDED));
    crate::indexing::full::rate_words(applied, added)
}

/// The trial forms' fields.
#[derive(Debug, serde::Deserialize)]
pub struct TrialForm {
    #[serde(default)]
    pub csrf: String,
    pub confirm: Option<String>,
    pub typed: Option<String>,
    /// The page the form was on, for Cancel.
    pub back: Option<String>,
}

/// One trial form's confirm page and command: `word` typed, then `cmd`.
#[allow(clippy::too_many_arguments)]
async fn trial_form(state: &AdminState, op: &crate::admin::auth::Operator, headers: &HeaderMap, form: &TrialForm, action: &str, word: &str, title: &str, what: String, undo: &str, button: &str, cmd: AdminCommand) -> Response {
    let back = back_to(state, headers, form.back.as_deref(), HERE);
    let mut page = ConfirmPage {
        css: css(),
        nav: Some(Nav::at(op, &back)),
        back,
        title: title.to_string(),
        what,
        undo: undo.to_string(),
        action: action.to_string(),
        csrf: op.csrf.clone(),
        hidden: Vec::new(),
        typed: Some(word.to_string()),
        button: button.to_string(),
        error: None,
    };
    if form.confirm.as_deref() != Some("yes") {
        return render(StatusCode::OK, &page);
    }
    if !typed_matches(word, form.typed.as_deref()) {
        page.error = Some(format!("Type {word} exactly to confirm."));
        return render(StatusCode::BAD_REQUEST, &page);
    }
    enqueue(state, op, &cmd).await
}

/// `POST /admin/indexing/trial`: "Try full network", typed `FULL`, with the
/// trial's bounds from `[full_trial]`. Its confirm page says what the CLI's
/// does: full's warnings, the bounds and which ends it first, what a trial
/// does not exercise, and what it removes and does not restore.
pub async fn trial(State(state): State<AdminState>, ConnectInfo(peer): ConnectInfo<SocketAddr>, headers: HeaderMap, Form(form): Form<TrialForm>) -> Response {
    let op = match require(&state, &headers, peer, Need::Change, Some(&form.csrf)).await {
        Ok(op) => op,
        Err(response) => return response,
    };
    let running = &state.settings.running;
    let bounds = crate::indexing::trial::Bounds::from_config(&running.full_trial);
    let used: Option<i64> = sqlx::query_scalar("SELECT pg_database_size(current_database())").fetch_one(&state.pool).await.ok();
    let volume = crate::indexing::trial::volume(&state.disks.data);
    let mut warnings = crate::indexing::full::warnings(&running.full.kinds, used.and_then(|u| u64::try_from(u).ok()), volume.map(|v| v.0), running.storage.budget_bytes, running.firehose.writer_lanes);
    warnings.extend(writer_words(&state).await);
    warnings.extend(crate::indexing::full::postgres_warnings(&state.pool).await);
    let mut file = running.clone();
    file.full.label_backfill &= bounds.history;
    warnings.extend(crate::indexing::full::more_warnings(&file, crate::indexing::full_check::last_words(&state.pool).await.ok().flatten()));
    let from = if state.scope.public_mode() == PublicMode::Off { ScopeValue::KryphocronOnly } else { ScopeValue::Community };
    let first = crate::indexing::trial::first_bound(bounds, volume, crate::indexing::full::GROWTH_BYTES_PER_DAY);
    let what = crate::indexing::trial::confirm_text(from, bounds, &warnings, None, first).join(" ");
    let cmd = AdminCommand::FullTrialStart { hours: None, max_added_gb: None, end_below_free_percent: None, history: bounds.history };
    trial_form(
        &state,
        &op,
        &headers,
        &form,
        "/admin/indexing/trial",
        "FULL",
        "Try full network",
        what,
        "End trial stops it at any time; its end removes what it added and checks that nothing is left.",
        "Start trial",
        cmd,
    )
    .await
}

/// `POST /admin/indexing/trial-end`: "End trial", typed `END`.
pub async fn trial_end(State(state): State<AdminState>, ConnectInfo(peer): ConnectInfo<SocketAddr>, headers: HeaderMap, Form(form): Form<TrialForm>) -> Response {
    let op = match require(&state, &headers, peer, Need::Change, Some(&form.csrf)).await {
        Ok(op) => op,
        Err(response) => return response,
    };
    let what = "Prism leaves the full network now and goes back to the mode the trial started from. What the trial added is removed in the background, then checked; public requests answer as that mode does.".to_string();
    trial_form(&state, &op, &headers, &form, "/admin/indexing/trial-end", "END", "End the full network trial", what, "A new trial can be started once this one's check has passed.", "End trial", AdminCommand::FullTrialEnd).await
}

/// `POST /admin/indexing/trial-dismiss`: drop a failed trial's record, typed
/// `DISMISS`.
pub async fn trial_dismiss(State(state): State<AdminState>, ConnectInfo(peer): ConnectInfo<SocketAddr>, headers: HeaderMap, Form(form): Form<TrialForm>) -> Response {
    let op = match require(&state, &headers, peer, Need::Change, Some(&form.csrf)).await {
        Ok(op) => op,
        Err(response) => return response,
    };
    let what = "The failed trial's record of what it added is dropped and its result is kept as dismissed. Rows its check could not remove stay; the warnings on Health name them.".to_string();
    trial_form(&state, &op, &headers, &form, "/admin/indexing/trial-dismiss", "DISMISS", "Dismiss the trial's result", what, "This cannot be undone.", "Dismiss", AdminCommand::FullTrialDismiss).await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_longest_wait_in_words() {
        let now = parse_ts(&serde_json::json!("2026-11-01T12:00:00Z")).expect("ts");
        assert_eq!(wait_words(None, 24, now), "24 hours from when every account is queued");
        assert_eq!(wait_words(Some(now - TimeDelta::hours(1)), 24, now), "2026-11-02 11:00 UTC (in 23 hours)");
        assert_eq!(wait_words(Some(now - TimeDelta::minutes(30)), 1, now), "2026-11-01 12:30 UTC (in 30 minutes)");
        assert!(wait_words(Some(now - TimeDelta::hours(25)), 24, now).starts_with("now"));
    }
}
