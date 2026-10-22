//! Blocks (Farsight) (`/admin/farsight`, admin-ui design §6.8): Farsight's
//! own report in brief and what Prism holds itself, the content of `admin
//! farsight status`, read directly (it writes nothing) and kept for 30
//! seconds; whether Farsight is down; the fallback and the memory of
//! blocks, each with a link to its settings. Per-member actions belong to
//! the members page.

use std::net::SocketAddr;
use std::sync::{Mutex, PoisonError};
use std::time::{Duration, Instant};

use askama::Template;
use axum::extract::{ConnectInfo, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::Response;
use serde_json::Value;

use super::{Nav, css, render};
use crate::admin::AdminState;
use crate::admin::auth::{Need, require};
use crate::admin::health::count;

/// How long Farsight's report is reused.
pub const KEEP: Duration = Duration::from_secs(30);

/// Farsight's last report and when it was asked for.
#[derive(Default)]
pub struct ReportCache(Mutex<Option<(Instant, Result<Value, String>)>>);

impl ReportCache {
    fn fresh(&self) -> Option<Result<Value, String>> {
        let guard = self.0.lock().unwrap_or_else(PoisonError::into_inner);
        guard.as_ref().filter(|(at, _)| at.elapsed() < KEEP).map(|(_, r)| r.clone())
    }

    fn keep(&self, report: Result<Value, String>) {
        *self.0.lock().unwrap_or_else(PoisonError::into_inner) = Some((Instant::now(), report));
    }
}

/// A label and its value.
pub struct Fact {
    pub label: String,
    pub value: String,
}

fn fact(label: &str, value: impl Into<String>) -> Fact {
    Fact { label: label.to_string(), value: value.into() }
}

#[derive(Template)]
#[template(path = "farsight.html")]
struct FarsightPage {
    css: &'static str,
    nav: Option<Nav>,
    title: String,
    configured: bool,
    /// Farsight's answer, or why there is none.
    answering: bool,
    reason: String,
    report: Vec<Fact>,
    /// Farsight's own figures as it sent them.
    raw: String,
    local: Vec<Fact>,
    fallback: Vec<Fact>,
    memory: Vec<Fact>,
}

fn yes_no(v: Option<&Value>) -> String {
    match v.and_then(Value::as_bool) {
        Some(true) => "yes".to_string(),
        Some(false) => "no".to_string(),
        None => "unknown".to_string(),
    }
}

fn num(v: Option<&Value>) -> String {
    v.and_then(Value::as_f64).map_or_else(|| "unknown".to_string(), count)
}

/// `GET /admin/farsight`.
pub async fn page(State(state): State<AdminState>, ConnectInfo(peer): ConnectInfo<SocketAddr>, headers: HeaderMap) -> Response {
    let op = match require(&state, &headers, peer, Need::Page, None).await {
        Ok(op) => op,
        Err(response) => return response,
    };
    let mut page = FarsightPage {
        css: css(),
        nav: Some(Nav::new(&op, "farsight")),
        title: "Blocks".to_string(),
        configured: false,
        answering: false,
        reason: String::new(),
        report: Vec::new(),
        raw: String::new(),
        local: Vec::new(),
        fallback: Vec::new(),
        memory: Vec::new(),
    };
    let Some(farsight) = state.health.farsight.clone() else {
        return render(StatusCode::OK, &page);
    };
    page.configured = true;
    let report = match state.farsight_report.fresh() {
        Some(r) => r,
        None => {
            let r = farsight.status().await.map_err(str::to_string);
            state.farsight_report.keep(r.clone());
            r
        }
    };
    match report {
        Ok(brief) => {
            page.answering = true;
            page.report = vec![
                fact("Farsight's version", brief.get("version").and_then(Value::as_str).unwrap_or("unknown")),
                fact("Prism has stopped asking for a while (too many failures)", yes_no(brief.get("read_breaker_open"))),
                fact("Answers Prism keeps for reuse", num(brief.get("cached_pair_answers"))),
            ];
            let own: serde_json::Map<String, Value> = ["firehose", "sweep", "coverage", "freshness"]
                .into_iter()
                .filter_map(|k| brief.get(k).map(|v| (k.to_string(), v.clone())))
                .collect();
            if !own.is_empty() {
                page.raw = serde_json::to_string_pretty(&Value::Object(own)).unwrap_or_default();
            }
        }
        Err(reason) => page.reason = reason,
    }
    let local = farsight.local_status();
    page.local = vec![
        fact("Answers since Prism started", num(local.get("answered_since_start"))),
        fact("Farsight is down", yes_no(local.get("down"))),
    ];
    page.fallback = match local.get("fallback") {
        Some(Value::Object(fb)) => vec![
            fact("Fallback", "on"),
            fact("Asks", format!("{} ({})", fb.get("network").and_then(Value::as_str).unwrap_or("?"), fb.get("appview_url").and_then(Value::as_str).unwrap_or("?"))),
            fact("Stopped asking for a while", yes_no(fb.get("breaker_open"))),
        ],
        _ => vec![fact("Fallback", "off")],
    };
    page.memory = match local.get("memory") {
        Some(Value::Object(m)) => vec![
            fact("Memory of blocks", "on"),
            fact("Blocks remembered", num(m.get("entries"))),
            fact("Accounts they apply to", num(m.get("viewers"))),
            fact("From Farsight", num(m.get("from_farsight"))),
            fact("From the fallback", num(m.get("from_fallback"))),
            fact("Kept for (hours)", num(m.get("retention_hours"))),
        ],
        _ => vec![fact("Memory of blocks", "off")],
    };
    render(StatusCode::OK, &page)
}
