//! Refract accounts (`/admin/refract`, refract design §9): every account
//! Refract knows, with its community, state, checks and, once restored, how
//! far the fetch of its history has got. Add an account (a handle or a
//! DID: `refract check`, so it becomes a candidate checked now), check one
//! again, remove a restored one (`refract end`, typed: its handle), and
//! keep or release pending ends (`refract keep`/`unkeep`). One account's
//! page shows what `admin refract show` does. Handles come from stored
//! `actors` rows, so a page makes no outbound call.

use std::net::SocketAddr;

use askama::Template;
use axum::Form;
use axum::extract::{ConnectInfo, Path, Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::Response;
use chrono::{DateTime, Utc};
use serde::Deserialize;
use serde_json::Value;

use super::communities::PendingView;
use super::{ConfirmPage, Hidden, Nav, confirmed, css, enqueue, message, render, typed_word, when, when_opt};
use crate::admin::AdminState;
use crate::admin::auth::{Need, Operator, require};
use crate::admin_executor::AdminCommand;

/// Accounts per page.
pub const PAGE: i64 = 100;
/// The states, in the order the filter offers them.
const STATES: [&str; 5] = ["candidate", "restored", "grace", "ending", "rejected"];

/// One account in the list.
pub struct AccountRow {
    pub did: String,
    pub handle: String,
    pub community: String,
    pub state: String,
    pub since: String,
    pub checked: String,
    pub next: String,
    pub history: String,
}

#[derive(Template)]
#[template(path = "refract.html")]
struct RefractPage {
    css: &'static str,
    nav: Option<Nav>,
    title: String,
    csrf: String,
    enabled: bool,
    states: Vec<&'static str>,
    communities: Vec<String>,
    state: String,
    community: String,
    rows: Vec<AccountRow>,
    total: i64,
    page: i64,
    prev: Option<String>,
    next: Option<String>,
    pending: Vec<PendingView>,
}

#[derive(Debug, Default, Deserialize)]
pub struct ListQuery {
    pub state: Option<String>,
    pub community: Option<String>,
    pub page: Option<i64>,
}

/// A state in words, for the list and the account's page.
#[must_use]
pub fn state_words(state: &str) -> &'static str {
    match state {
        "candidate" => "candidate: being checked",
        "restored" => "restored: kept and served",
        "grace" => "grace: a check failed, still served for now",
        "ending" => "ending: its data here is being deleted",
        "rejected" => "rejected: not kept",
        _ => "unknown",
    }
}

/// How far a restored account's history fetch (the `refract` sync job and
/// its `refract_cursor`) has got.
#[must_use]
pub fn history(job: Option<&str>, cursor: Option<&Value>, error: Option<&str>) -> String {
    let done = cursor.and_then(|c| c.get("done")).and_then(Value::as_array).map_or(0, Vec::len);
    let now = cursor.and_then(|c| c.get("collection")).and_then(Value::as_str);
    let so_far = match (done, now) {
        (0, None) => String::new(),
        (n, None) => format!(", {n} collection{} fetched", if n == 1 { "" } else { "s" }),
        (0, Some(c)) => format!(", now {c}"),
        (n, Some(c)) => format!(", {n} collection{} fetched, now {c}", if n == 1 { "" } else { "s" }),
    };
    match job {
        None => "—".to_string(),
        Some("done") => "fetched".to_string(),
        Some("queued") => format!("waiting{so_far}"),
        Some("running") => format!("fetching{so_far}"),
        Some("failed") => format!("failed: {}", error.unwrap_or("no reason given")),
        Some("cancelled") => "stopped".to_string(),
        Some(other) => other.to_string(),
    }
}

/// The left join of an account's `refract` sync job.
const JOB_JOIN: &str = "LEFT JOIN backfill_jobs j ON j.did = d.did AND j.depth = 'refract'";

type ListTuple = (String, Option<String>, String, String, Option<DateTime<Utc>>, Option<DateTime<Utc>>, DateTime<Utc>, Option<String>, Option<Value>, Option<String>);

/// The pending ends (§4.6), as the Communities page shows them.
pub async fn pending(state: &AdminState) -> Result<Vec<PendingView>, sqlx::Error> {
    let rows: Vec<(String, DateTime<Utc>, bool, i64)> = sqlx::query_as(
        "SELECT p.scope, p.first_seen, p.keep, (SELECT count(*) FROM refract_pending_dids d WHERE d.scope = p.scope) FROM refract_pending p ORDER BY p.scope",
    )
    .fetch_all(&state.pool)
    .await?;
    let delay = chrono::TimeDelta::hours(i64::from(state.settings.running.refract.disable_delay_hours));
    Ok(rows
        .into_iter()
        .map(|(scope, since, keep, dids)| PendingView {
            ends: if keep { "kept".to_string() } else { when(since + delay) },
            since: when(since),
            scope,
            keep,
            dids,
        })
        .collect())
}

/// The list's link for a state, a community and a page (empty: any).
#[must_use]
pub fn link(state: &str, community: &str, page: i64) -> String {
    let mut q = url::form_urlencoded::Serializer::new(String::new());
    if !state.is_empty() {
        q.append_pair("state", state);
    }
    if !community.is_empty() {
        q.append_pair("community", community);
    }
    if page > 0 {
        q.append_pair("page", &page.to_string());
    }
    let q = q.finish();
    if q.is_empty() { "/admin/refract".to_string() } else { format!("/admin/refract?{q}") }
}

/// `GET /admin/refract`.
pub async fn page(State(state): State<AdminState>, ConnectInfo(peer): ConnectInfo<SocketAddr>, headers: HeaderMap, Query(q): Query<ListQuery>) -> Response {
    let op = match require(&state, &headers, peer, Need::Page, None).await {
        Ok(op) => op,
        Err(response) => return response,
    };
    let filter = q.state.as_deref().map(str::trim).filter(|s| STATES.contains(s)).unwrap_or("").to_string();
    let community = q.community.as_deref().map(str::trim).unwrap_or("").to_string();
    let page = q.page.unwrap_or(0).max(0);
    let state_arg = (!filter.is_empty()).then_some(filter.as_str());
    let community_arg = (!community.is_empty()).then_some(community.as_str());
    let read = async {
        let rows: Vec<ListTuple> = sqlx::query_as(&format!(
            "SELECT d.did, a.handle, d.community, d.state, coalesce(d.since, d.candidate_since, d.created_at), d.checked_at, d.next_check_at, \
                    j.state, j.refract_cursor, j.last_error \
             FROM refract_dids d LEFT JOIN actors a ON a.did = d.did {JOB_JOIN} \
             WHERE ($1::text IS NULL OR d.state = $1) AND ($2::text IS NULL OR d.community = $2) \
             ORDER BY d.community, d.state, d.did LIMIT $3 OFFSET $4"
        ))
        .bind(state_arg)
        .bind(community_arg)
        .bind(PAGE + 1)
        .bind(page * PAGE)
        .fetch_all(&state.pool)
        .await?;
        let total: i64 = sqlx::query_scalar("SELECT count(*) FROM refract_dids WHERE ($1::text IS NULL OR state = $1) AND ($2::text IS NULL OR community = $2)")
            .bind(state_arg)
            .bind(community_arg)
            .fetch_one(&state.pool)
            .await?;
        let communities: Vec<String> = sqlx::query_scalar("SELECT DISTINCT community FROM refract_dids").fetch_all(&state.pool).await?;
        Ok::<_, sqlx::Error>((rows, total, communities))
    };
    let (mut rows, total, mut communities) = match read.await {
        Ok(r) => r,
        Err(e) => return super::internal_error(&state, &e),
    };
    let pending = match pending(&state).await {
        Ok(p) => p,
        Err(e) => return super::internal_error(&state, &e),
    };
    let live = state.settings.live.load();
    for c in &live.refract_communities {
        if !communities.contains(&c.name) {
            communities.push(c.name.clone());
        }
    }
    communities.sort();
    let has_next = rows.len() > usize::try_from(PAGE).unwrap_or(usize::MAX);
    rows.truncate(usize::try_from(PAGE).unwrap_or(usize::MAX));
    let rows = rows
        .into_iter()
        .map(|(did, handle, community, st, since, checked, next, job, cursor, error)| AccountRow {
            history: if matches!(st.as_str(), "restored" | "grace") || job.is_some() {
                history(job.as_deref(), cursor.as_ref(), error.as_deref())
            } else {
                "—".to_string()
            },
            did,
            handle: handle.unwrap_or_default(),
            community,
            since: when_opt(since),
            checked: when_opt(checked),
            next: if matches!(st.as_str(), "candidate" | "restored" | "grace") { when(next) } else { "—".to_string() },
            state: st,
        })
        .collect();
    render(
        StatusCode::OK,
        &RefractPage {
            css: css(),
            nav: Some(Nav::new(&op, "refract")),
            title: "Refract accounts".to_string(),
            csrf: op.csrf.clone(),
            enabled: live.refract_enabled,
            states: STATES.to_vec(),
            communities,
            prev: (page > 0).then(|| link(&filter, &community, page - 1)),
            next: has_next.then(|| link(&filter, &community, page + 1)),
            state: filter,
            community,
            rows,
            total,
            page,
            pending,
        },
    )
}

/// A fact of the account's page.
pub struct Fact {
    pub label: &'static str,
    pub value: String,
}

/// One rule of the last verdict.
pub struct RuleView {
    pub rule: String,
    pub result: String,
    pub detail: String,
}

pub struct LabelView {
    pub labeler: String,
    pub subject: String,
    pub val: String,
    pub cts: String,
    pub exp: String,
    pub gone: bool,
    pub verified: bool,
}

pub struct EventView {
    pub at: String,
    pub from: String,
    pub to: String,
    pub reason: String,
}

#[derive(Template)]
#[template(path = "refract_account.html")]
struct AccountPage {
    css: &'static str,
    nav: Option<Nav>,
    title: String,
    csrf: String,
    enabled: bool,
    did: String,
    state: String,
    state_words: &'static str,
    facts: Vec<Fact>,
    verdict_at: String,
    rules: Vec<RuleView>,
    verdict: String,
    labels: Vec<LabelView>,
    events: Vec<EventView>,
    pending: Vec<String>,
    /// `refract end` is allowed: restored or in grace.
    can_end: bool,
    /// `refract check` makes sense: anything but ending.
    can_check: bool,
}

/// A rule's number in words (refract design §4.1).
fn rule_name(n: i64) -> String {
    match n {
        1 => "1. Bluesky withholds it".to_string(),
        2 => "2. A listed community hosts it".to_string(),
        3 => "3. Active on its server".to_string(),
        4 => "4. Not banned by its community".to_string(),
        5 => "5. No takedown here".to_string(),
        n => format!("{n}."),
    }
}

/// The `refract_dids` columns the account's page shows.
#[derive(sqlx::FromRow)]
struct Row {
    community: String,
    pds_host: Option<String>,
    state: String,
    source: String,
    since: Option<DateTime<Utc>>,
    candidate_since: Option<DateTime<Utc>>,
    checked_at: Option<DateTime<Utc>>,
    next_check_at: DateTime<Utc>,
    attempts: i32,
    last_verdict: Option<Value>,
    grace_until: Option<DateTime<Utc>>,
    ended_reason: Option<String>,
    unverified_since: Option<DateTime<Utc>>,
    awaiting_labels_since: Option<DateTime<Utc>>,
    resync_at: Option<DateTime<Utc>>,
    created_at: DateTime<Utc>,
    handle: Option<String>,
    job: Option<String>,
    cursor: Option<Value>,
    job_error: Option<String>,
}

type LabelRow = (String, String, String, Option<DateTime<Utc>>, Option<DateTime<Utc>>, bool, bool);
type EventRow = (DateTime<Utc>, Option<String>, String, Option<String>);

/// `GET /admin/refract/{did}`: what `admin refract show` gives.
pub async fn account(State(state): State<AdminState>, ConnectInfo(peer): ConnectInfo<SocketAddr>, headers: HeaderMap, Path(did): Path<String>) -> Response {
    let op = match require(&state, &headers, peer, Need::Page, None).await {
        Ok(op) => op,
        Err(response) => return response,
    };
    let read = async {
        let row: Option<Row> = sqlx::query_as(&format!(
            "SELECT d.community, d.pds_host, d.state, d.source, d.since, d.candidate_since, d.checked_at, d.next_check_at, d.attempts, d.last_verdict, \
                    d.grace_until, d.ended_reason, d.unverified_since, d.awaiting_labels_since, d.resync_at, d.created_at, a.handle, \
                    j.state AS job, j.refract_cursor AS cursor, j.last_error AS job_error \
             FROM refract_dids d LEFT JOIN actors a ON a.did = d.did {JOB_JOIN} WHERE d.did = $1"
        ))
        .bind(&did)
        .fetch_optional(&state.pool)
        .await?;
        let labels: Vec<LabelRow> =
            sqlx::query_as("SELECT labeler_did, subject, val, cts, exp, gone, verified FROM refract_labels WHERE did = $1 ORDER BY cts DESC NULLS LAST LIMIT 50")
                .bind(&did)
                .fetch_all(&state.pool)
                .await?;
        let events: Vec<EventRow> = sqlx::query_as("SELECT at, from_state, to_state, reason FROM refract_events WHERE did = $1 ORDER BY at DESC, id DESC LIMIT 50")
            .bind(&did)
            .fetch_all(&state.pool)
            .await?;
        let pending: Vec<String> = sqlx::query_scalar("SELECT scope FROM refract_pending_dids WHERE did = $1 ORDER BY scope").bind(&did).fetch_all(&state.pool).await?;
        Ok::<_, sqlx::Error>((row, labels, events, pending))
    };
    let (row, labels, events, pending) = match read.await {
        Ok(r) => r,
        Err(e) => return super::internal_error(&state, &e),
    };
    let Some(row) = row else {
        return message(
            &state,
            Some(Nav::new(&op, "refract")),
            StatusCode::NOT_FOUND,
            "Not known",
            &format!("Refract has no row for {did}. Add it on the Refract accounts page to check it."),
            Some(("/admin/refract", "Refract accounts")),
        );
    };
    let verdict = row.last_verdict.clone().unwrap_or(Value::Null);
    let rules = verdict
        .get("rules")
        .and_then(Value::as_array)
        .map(|rules| {
            rules
                .iter()
                .map(|r| RuleView {
                    rule: rule_name(r.get("rule").and_then(Value::as_i64).unwrap_or(0)),
                    result: r.get("result").and_then(Value::as_str).unwrap_or("?").replace('_', " "),
                    detail: r.get("detail").and_then(Value::as_str).unwrap_or_default().to_string(),
                })
                .collect()
        })
        .unwrap_or_default();
    let verdict_at = verdict
        .get("at")
        .and_then(Value::as_str)
        .and_then(|t| DateTime::parse_from_rfc3339(t).ok())
        .map(|t| when(t.with_timezone(&Utc)))
        .unwrap_or_default();
    let in_r = matches!(row.state.as_str(), "restored" | "grace");
    let mut facts = vec![
        Fact { label: "Community", value: row.community.clone() },
        Fact { label: "Server", value: row.pds_host.clone().unwrap_or_else(|| "—".to_string()) },
        Fact { label: "Added by", value: if row.source == "admin" { "an operator".to_string() } else { "Prism, from what members see".to_string() } },
        Fact { label: "First known", value: when(row.created_at) },
        Fact { label: "Candidate since", value: when_opt(row.candidate_since) },
        Fact { label: "Restored since", value: when_opt(row.since) },
        Fact { label: "Last checked", value: when_opt(row.checked_at) },
        Fact {
            label: "Next check",
            value: if matches!(row.state.as_str(), "candidate" | "restored" | "grace") { when(row.next_check_at) } else { "—".to_string() },
        },
        Fact { label: "Failed tries in a row", value: row.attempts.to_string() },
    ];
    if in_r || row.job.is_some() {
        facts.push(Fact { label: "History", value: history(row.job.as_deref(), row.cursor.as_ref(), row.job_error.as_deref()) });
        facts.push(Fact { label: "Next full re-read", value: when_opt(row.resync_at) });
    }
    for (label, t) in [("In grace until", row.grace_until), ("Unverified since", row.unverified_since), ("Waiting for its labels since", row.awaiting_labels_since)] {
        if t.is_some() {
            facts.push(Fact { label, value: when_opt(t) });
        }
    }
    if let Some(r) = &row.ended_reason {
        facts.push(Fact { label: "Ended because", value: r.clone() });
    }
    if let Some(s) = verdict.get("suspect") {
        facts.push(Fact { label: "Suspected move", value: s.to_string() });
    }
    let handle = row.handle.clone().unwrap_or_default();
    render(
        StatusCode::OK,
        &AccountPage {
            css: css(),
            nav: Some(Nav::new(&op, "refract")),
            title: if handle.is_empty() { did.clone() } else { handle.clone() },
            csrf: op.csrf.clone(),
            enabled: state.settings.live.load().refract_enabled,
            state_words: state_words(&row.state),
            can_end: in_r,
            can_check: row.state != "ending",
            state: row.state,
            facts,
            verdict_at,
            rules,
            verdict: if verdict.is_null() { String::new() } else { serde_json::to_string_pretty(&verdict).unwrap_or_default() },
            labels: labels
                .into_iter()
                .map(|(labeler, subject, val, cts, exp, gone, verified)| LabelView { labeler, subject, val, cts: when_opt(cts), exp: when_opt(exp), gone, verified })
                .collect(),
            events: events
                .into_iter()
                .map(|(at, from, to, reason)| EventView { at: when(at), from: from.unwrap_or_else(|| "—".to_string()), to, reason: reason.unwrap_or_default() })
                .collect(),
            pending,
            did,
        },
    )
}

/// A refusal on this page.
fn refuse(state: &AdminState, op: &Operator, title: &str, text: &str) -> Response {
    message(state, Some(Nav::new(op, "refract")), StatusCode::BAD_REQUEST, title, text, Some(("/admin/refract", "Refract accounts")))
}

/// The refusal of a check while Refract is off: the command would fail.
fn off(state: &AdminState, op: &Operator) -> Option<Response> {
    (!state.settings.live.load().refract_enabled).then(|| {
        refuse(state, op, "Not checked", "Refract is off, so nothing is checked. Turn it on on the Communities page first.")
    })
}

#[derive(Debug, Default, Deserialize)]
pub struct AddForm {
    #[serde(default)]
    pub csrf: String,
    #[serde(default)]
    pub input: String,
}

/// `POST /admin/refract/add`: a handle or DID → `refract check` (a
/// candidate checked now, or due now if known).
pub async fn add(State(state): State<AdminState>, ConnectInfo(peer): ConnectInfo<SocketAddr>, headers: HeaderMap, Form(form): Form<AddForm>) -> Response {
    let op = match require(&state, &headers, peer, Need::Change, Some(&form.csrf)).await {
        Ok(op) => op,
        Err(response) => return response,
    };
    if let Some(r) = off(&state, &op) {
        return r;
    }
    match super::members::account(&state, &form.input).await {
        Ok(did) => enqueue(&state, &op, &AdminCommand::RefractCheck { did }).await,
        Err(problem) => refuse(&state, &op, "Not added", &problem),
    }
}

#[derive(Debug, Default, Deserialize)]
pub struct DidForm {
    #[serde(default)]
    pub csrf: String,
    #[serde(default)]
    pub did: String,
    pub confirm: Option<String>,
    pub typed: Option<String>,
    pub reason: Option<String>,
}

fn invalid_did(state: &AdminState, op: &Operator, did: &str) -> Option<Response> {
    (!proto_blue::syntax::Did::is_valid(did)).then(|| refuse(state, op, "Not done", "That is not a valid DID."))
}

/// `POST /admin/refract/check`: check a known account again now.
pub async fn check(State(state): State<AdminState>, ConnectInfo(peer): ConnectInfo<SocketAddr>, headers: HeaderMap, Form(form): Form<DidForm>) -> Response {
    let op = match require(&state, &headers, peer, Need::Change, Some(&form.csrf)).await {
        Ok(op) => op,
        Err(response) => return response,
    };
    if let Some(r) = invalid_did(&state, &op, &form.did).or_else(|| off(&state, &op)) {
        return r;
    }
    enqueue(&state, &op, &AdminCommand::RefractCheck { did: form.did.clone() }).await
}

/// `POST /admin/refract/end` (typed: the account's handle): stop keeping a
/// restored account; `refract end`.
pub async fn end(State(state): State<AdminState>, ConnectInfo(peer): ConnectInfo<SocketAddr>, headers: HeaderMap, Form(form): Form<DidForm>) -> Response {
    let op = match require(&state, &headers, peer, Need::Change, Some(&form.csrf)).await {
        Ok(op) => op,
        Err(response) => return response,
    };
    if let Some(r) = invalid_did(&state, &op, &form.did) {
        return r;
    }
    let current: Option<String> = match sqlx::query_scalar("SELECT state FROM refract_dids WHERE did = $1").bind(&form.did).fetch_optional(&state.pool).await {
        Ok(s) => s,
        Err(e) => return super::internal_error(&state, &e),
    };
    if !matches!(current.as_deref(), Some("restored" | "grace")) {
        return refuse(&state, &op, "Not removed", "Only a restored account, or one in grace, can be removed.");
    }
    let word = match typed_word(&state, &form.did).await {
        Ok(w) => w,
        Err(e) => return super::internal_error(&state, &e),
    };
    let reason = form.reason.as_deref().map(str::trim).filter(|r| !r.is_empty()).map(str::to_string);
    let mut hidden = vec![Hidden { name: "did".to_string(), value: form.did.clone() }];
    if let Some(r) = &reason {
        hidden.push(Hidden { name: "reason".to_string(), value: r.clone() });
    }
    let page = ConfirmPage {
        css: css(),
        nav: Some(Nav::new(&op, "refract")),
        title: "Remove a restored account".to_string(),
        what: format!(
            "Prism stops keeping {} and hides it at once, then deletes what it stored for that account only because Refract kept it: its posts, its history and its Refract records. Members' own records that point at it (their likes, follows, replies and quotes) are kept.",
            form.did
        ),
        undo: "This cannot be undone. Adding the account again later checks it from the start and fetches its history again.".to_string(),
        action: "/admin/refract/end".to_string(),
        csrf: op.csrf.clone(),
        hidden,
        typed: Some(word),
        button: "Remove".to_string(),
        error: None,
    };
    if let Some(response) = confirmed(page, form.confirm.as_deref(), form.typed.as_deref()) {
        return response;
    }
    enqueue(&state, &op, &AdminCommand::RefractEnd { did: form.did.clone(), reason }).await
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn history_in_words() {
        assert_eq!(history(None, None, None), "—");
        assert_eq!(history(Some("done"), None, None), "fetched");
        assert_eq!(history(Some("queued"), None, None), "waiting");
        let cursor = json!({"collection": "app.bsky.feed.post", "cursor": "x", "done": ["app.bsky.actor.profile"]});
        assert_eq!(history(Some("running"), Some(&cursor), None), "fetching, 1 collection fetched, now app.bsky.feed.post");
        assert_eq!(history(Some("failed"), None, Some("timeout")), "failed: timeout");
        assert_eq!(link("restored", "black sky", 2), "/admin/refract?state=restored&community=black+sky&page=2");
        assert_eq!(link("", "", 0), "/admin/refract");
    }
}
