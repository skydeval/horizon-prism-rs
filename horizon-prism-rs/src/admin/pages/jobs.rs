//! Jobs (`/admin/jobs`, admin-ui design §6.6): back-fill jobs by kind and
//! state, the 100 newest failed jobs with "try again" (a `backfill` of the
//! same kind), the nightly clean-up of the accounts Prism follows
//! (scope-recency design §3.3: its last run, the next, "check" as a dry run
//! and "clean up now", typed), and a public-data refill's progress when one
//! runs. The counts run under a 2-second statement timeout.

use std::collections::BTreeMap;
use std::net::SocketAddr;

use askama::Template;
use axum::Form;
use axum::extract::{ConnectInfo, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::Response;
use chrono::{DateTime, TimeDelta, Utc};
use serde::Deserialize;

use super::{Confirm, Nav, confirmed, css, enqueue, message, render, when, when_opt};
use crate::admin::AdminState;
use crate::admin::auth::{Need, require};
use crate::admin::health::count;
use crate::admin_executor::AdminCommand;
use crate::aggregations::rebuild::{KEY_LAST_REBUILD, KEY_LAST_SWEEP, METRIC_SWEEP_RUNS, SWEEP_MIN_GAP, most_recent_slot, sweep_due};
use crate::firehose::jobs::Depth;
use crate::indexing::PublicMode;

/// The states of a job, in the order the table shows them.
pub const STATES: [(&str, &str); 5] = [("queued", "Waiting"), ("running", "Running"), ("done", "Done"), ("failed", "Failed"), ("cancelled", "Cancelled")];

/// A kind of job in words.
#[must_use]
pub fn depth_words(depth: &str) -> &str {
    match depth {
        "member" => "Members' first syncs",
        "scope" => "Accounts around members",
        "private" => "Private post owners",
        "authz" => "Private access checks",
        "resync" => "Re-syncs",
        "list_sync" => "Subscribed lists",
        other => other,
    }
}

pub struct DepthRow {
    pub name: String,
    pub tech: String,
    /// One count per [`STATES`] entry, blank for none.
    pub counts: Vec<String>,
}

pub struct FailedRow {
    pub did: String,
    pub who: String,
    pub kind: String,
    pub depth: String,
    pub attempts: i32,
    pub error: String,
    pub when: String,
    /// The kind can be queued again from here.
    pub retry: bool,
}

pub struct SweepView {
    pub last: String,
    pub next: String,
    pub weekly: String,
    pub failed: String,
    pub cap: String,
}

#[derive(Template)]
#[template(path = "jobs.html")]
struct JobsPage {
    css: &'static str,
    nav: Option<Nav>,
    title: String,
    csrf: String,
    /// `None`: the counts did not finish in time.
    depths: Option<Vec<DepthRow>>,
    failed: Option<Vec<FailedRow>>,
    states: Vec<&'static str>,
    sweep: SweepView,
    public: String,
}

type FailedTuple = (String, Option<String>, String, i32, Option<String>, DateTime<Utc>);

/// The two job reads, under the statement timeout.
async fn read_jobs(state: &AdminState) -> Result<(Vec<(String, String, i64)>, Vec<FailedTuple>), sqlx::Error> {
    let mut tx = state.pool.begin().await?;
    sqlx::query("SET LOCAL statement_timeout = '2s'").execute(&mut *tx).await?;
    let counts: Vec<(String, String, i64)> = sqlx::query_as("SELECT depth, state, count(*) FROM backfill_jobs GROUP BY 1, 2").fetch_all(&mut *tx).await?;
    let failed: Vec<FailedTuple> = sqlx::query_as(
        "SELECT j.did, a.handle, j.depth, j.attempts, j.last_error, j.updated_at FROM backfill_jobs j \
         LEFT JOIN actors a ON a.did = j.did WHERE j.state = 'failed' ORDER BY j.updated_at DESC LIMIT 100",
    )
    .fetch_all(&mut *tx)
    .await?;
    tx.commit().await?;
    Ok((counts, failed))
}

async fn worker_time(state: &AdminState, key: &str) -> Option<DateTime<Utc>> {
    sqlx::query_scalar("SELECT (value #>> '{}')::timestamptz FROM worker_state WHERE key = $1")
        .bind(key)
        .fetch_optional(&state.pool)
        .await
        .ok()
        .flatten()
}

/// When the nightly clean-up next runs: the first daily slot after its last
/// run, and at least 23 hours after it (scope-recency design §3.3).
#[must_use]
pub fn next_sweep(last: DateTime<Utc>, now: DateTime<Utc>, slot_secs: u32) -> Option<DateTime<Utc>> {
    if sweep_due(last, now, slot_secs) {
        return None;
    }
    let slot = most_recent_slot(now, slot_secs);
    let next_slot = if last < slot { slot } else { slot + TimeDelta::days(1) };
    Some(next_slot.max(last + SWEEP_MIN_GAP))
}

async fn sweep_view(state: &AdminState) -> SweepView {
    let running = &state.settings.running;
    let slot_secs = (running.retention.run_at_utc.seconds_after_midnight() + 86_400 - 3600) % 86_400;
    let last = worker_time(state, KEY_LAST_SWEEP).await;
    let next = match last {
        None => "unknown".to_string(),
        Some(l) => next_sweep(l, Utc::now(), slot_secs).map_or_else(|| "within 5 minutes".to_string(), |t| format!("at about {}", when(t))),
    };
    let failed = state
        .health
        .now()
        .map_or_else(|| "unknown".to_string(), |s| count(s.sum_where(METRIC_SWEEP_RUNS, "outcome", "failed").unwrap_or(0.0)));
    SweepView {
        last: when_opt(last),
        next,
        weekly: when_opt(worker_time(state, KEY_LAST_REBUILD).await),
        failed,
        cap: count(f64::from(running.scope.sweep_max_purges)),
    }
}

/// `GET /admin/jobs`.
pub async fn page(State(state): State<AdminState>, ConnectInfo(peer): ConnectInfo<SocketAddr>, headers: HeaderMap) -> Response {
    let op = match require(&state, &headers, peer, Need::Page, None).await {
        Ok(op) => op,
        Err(response) => return response,
    };
    let (depths, failed) = match read_jobs(&state).await {
        Ok((counts, failed)) => {
            let mut by_depth: BTreeMap<String, BTreeMap<String, i64>> = BTreeMap::new();
            for (depth, job_state, n) in counts {
                by_depth.entry(depth).or_default().insert(job_state, n);
            }
            let depths = by_depth
                .into_iter()
                .map(|(depth, states)| DepthRow {
                    name: depth_words(&depth).to_string(),
                    counts: STATES.iter().map(|(s, _)| states.get(*s).map_or_else(String::new, |n| count(*n as f64))).collect(),
                    tech: depth,
                })
                .collect();
            let failed = failed
                .into_iter()
                .map(|(did, handle, depth, attempts, error, at)| FailedRow {
                    who: handle.unwrap_or_else(|| did.clone()),
                    kind: depth_words(&depth).to_string(),
                    retry: Depth::parse(&depth).is_some(),
                    did,
                    depth,
                    attempts,
                    error: error.unwrap_or_default().chars().take(300).collect(),
                    when: when(at),
                })
                .collect();
            (Some(depths), Some(failed))
        }
        Err(error) => {
            tracing::warn!(event = "admin_jobs_read_failed", %error, "the jobs page's counts did not finish");
            (None, None)
        }
    };
    let public = match state.scope.public_mode() {
        PublicMode::Filling => {
            let left = state.health.now().and_then(|s| s.sum(crate::indexing::public_fill::METRIC_REMAINING));
            format!(
                "Public data is being refilled: {} accounts left. Public requests come back when it is done.",
                left.map_or_else(|| "an unknown number of".to_string(), count)
            )
        }
        PublicMode::Draining => "Stored public data is being deleted in the background.".to_string(),
        _ => String::new(),
    };
    render(
        StatusCode::OK,
        &JobsPage {
            css: css(),
            nav: Some(Nav::new(&op, "jobs")),
            title: "Jobs".to_string(),
            csrf: op.csrf.clone(),
            depths,
            failed,
            states: STATES.iter().map(|(_, w)| *w).collect(),
            sweep: sweep_view(&state).await,
            public,
        },
    )
}

#[derive(Debug, Default, Deserialize)]
pub struct JobForm {
    #[serde(default)]
    pub csrf: String,
    #[serde(default)]
    pub did: String,
    #[serde(default)]
    pub depth: String,
    pub confirm: Option<String>,
    pub typed: Option<String>,
}

/// `POST /admin/jobs/retry`: a `backfill` of the failed job's kind.
pub async fn retry(State(state): State<AdminState>, ConnectInfo(peer): ConnectInfo<SocketAddr>, headers: HeaderMap, Form(form): Form<JobForm>) -> Response {
    let op = match require(&state, &headers, peer, Need::Change, Some(&form.csrf)).await {
        Ok(op) => op,
        Err(response) => return response,
    };
    let did = form.did.trim().to_string();
    match Depth::parse(form.depth.trim()) {
        Some(depth) if proto_blue::syntax::Did::is_valid(&did) => enqueue(&state, &op, &AdminCommand::Backfill { did, depth: Some(depth) }).await,
        _ => message(
            &state,
            Some(Nav::new(&op, "jobs")),
            StatusCode::BAD_REQUEST,
            "Not done",
            "That job cannot be tried again from here.",
            Some(("/admin/jobs", "Jobs")),
        ),
    }
}

/// `POST /admin/jobs/scope-check`: `scope rebuild --dry-run`, which
/// changes nothing.
pub async fn scope_check(State(state): State<AdminState>, ConnectInfo(peer): ConnectInfo<SocketAddr>, headers: HeaderMap, Form(form): Form<JobForm>) -> Response {
    let op = match require(&state, &headers, peer, Need::Change, Some(&form.csrf)).await {
        Ok(op) => op,
        Err(response) => return response,
    };
    enqueue(&state, &op, &AdminCommand::ScopeRebuild { dry_run: true }).await
}

/// `POST /admin/jobs/scope-rebuild`: one clean-up now, typed `REBUILD`.
pub async fn scope_rebuild(State(state): State<AdminState>, ConnectInfo(peer): ConnectInfo<SocketAddr>, headers: HeaderMap, Form(form): Form<JobForm>) -> Response {
    let op = match require(&state, &headers, peer, Need::Change, Some(&form.csrf)).await {
        Ok(op) => op,
        Err(response) => return response,
    };
    let cap = count(f64::from(state.settings.running.scope.sweep_max_purges));
    let page = Confirm {
        title: "Clean up the accounts Prism follows now",
        what: format!(
            "Prism stops following accounts it no longer needs (no member follows them, has interacted with them lately, or subscribes to a list of theirs), up to {cap} in this run, as the nightly clean-up does. Their stored public posts and profiles are deleted at the next daily clean-up. Use \"Check\" first to see how many."
        ),
        undo: "An account comes back only when a member follows or interacts with it again.",
        action: "/admin/jobs/scope-rebuild",
        hidden: Vec::new(),
        typed: Some("REBUILD".to_string()),
        button: "Clean up now",
    }
    .page(&op, "jobs");
    if let Some(response) = confirmed(page, form.confirm.as_deref(), form.typed.as_deref()) {
        return response;
    }
    enqueue(&state, &op, &AdminCommand::ScopeRebuild { dry_run: false }).await
}

#[cfg(test)]
mod tests {
    use super::*;

    fn t(s: &str) -> DateTime<Utc> {
        DateTime::parse_from_rfc3339(s).expect("time").with_timezone(&Utc)
    }

    /// The next run: the first slot after the last run, at least 23 hours
    /// after it; due now when the slot has passed.
    #[test]
    fn the_next_sweep() {
        let slot = 3 * 3600;
        assert_eq!(next_sweep(t("2026-10-20T03:01:00Z"), t("2026-10-20T12:00:00Z"), slot), Some(t("2026-10-21T03:00:00Z")));
        // A late run (06:00) pushes the next to 05:00 the day after.
        assert_eq!(next_sweep(t("2026-10-20T06:00:00Z"), t("2026-10-20T12:00:00Z"), slot), Some(t("2026-10-21T05:00:00Z")));
        assert_eq!(next_sweep(t("2026-10-19T03:01:00Z"), t("2026-10-20T04:00:00Z"), slot), None, "due now");
        assert_eq!(depth_words("authz"), "Private access checks");
        assert_eq!(depth_words("other"), "other");
    }
}
