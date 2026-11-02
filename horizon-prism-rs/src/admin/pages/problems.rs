//! Problems (`/admin/problems`): "Records Prism could not process", the
//! newest 100 of the dead-letter stream by `XREVRANGE`, each with "try
//! again" (`deadletter replay`) and "dismiss" (`deadletter drop`,
//! confirmed), and "clear out" of the records the firehose would now set
//! aside (`deadletter sweep`, confirmed); and the network stream's state in
//! one line, which Relays shows in full.

use std::net::SocketAddr;

use askama::Template;
use axum::Form;
use axum::extract::{ConnectInfo, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::Response;
use chrono::DateTime;
use redis::Value;
use serde::Deserialize;

use super::{Confirm, Nav, confirmed, css, enqueue, hidden_field, message, render, when};
use crate::admin::AdminState;
use crate::admin::auth::{Need, require};
use crate::admin_executor::AdminCommand;
use crate::firehose::stream::parse_entry;

/// Entries shown.
pub const SHOWN: u32 = 100;

pub struct DeadLetter {
    pub id: String,
    pub when: String,
    pub account: String,
    pub path: String,
    pub kind: String,
    pub error: String,
}

#[derive(Template)]
#[template(path = "problems.html")]
struct ProblemsPage {
    css: &'static str,
    nav: Option<Nav>,
    title: String,
    csrf: String,
    /// `None`: Redis could not be read.
    letters: Option<Vec<DeadLetter>>,
    /// The network stream in one phrase.
    stream: String,
}

/// A stream id's time: its first part is milliseconds since the epoch.
pub fn id_time(id: &str) -> String {
    id.split_once('-')
        .and_then(|(ms, _)| ms.parse::<i64>().ok())
        .and_then(DateTime::from_timestamp_millis)
        .map_or_else(|| "?".to_string(), when)
}

/// Whether `id` looks like a stream id (`<ms>-<seq>`).
#[must_use]
pub fn is_stream_id(id: &str) -> bool {
    id.split_once('-')
        .is_some_and(|(a, b)| !a.is_empty() && !b.is_empty() && a.bytes().chain(b.bytes()).all(|c| c.is_ascii_digit()))
}

async fn dead_letters(state: &AdminState) -> Result<Vec<DeadLetter>, String> {
    let Some(redis) = &state.health.redis else {
        return Err("Redis is not connected to these pages".to_string());
    };
    let mut redis = redis.clone();
    let reply: Value = redis::cmd("XREVRANGE")
        .arg(&state.settings.running.firehose.dead_letter_stream)
        .arg("+")
        .arg("-")
        .arg("COUNT")
        .arg(SHOWN)
        .query_async(&mut redis)
        .await
        .map_err(|e| e.to_string())?;
    let Value::Array(items) = reply else {
        return Err("unexpected reply".to_string());
    };
    let mut out = Vec::with_capacity(items.len());
    for item in items {
        let e = parse_entry(item)?;
        let f = |k: &str| e.field(k).unwrap_or_default().to_string();
        out.push(DeadLetter {
            when: id_time(&e.id),
            account: f("repo"),
            path: f("path"),
            kind: match f("type").as_str() {
                "commit" => "a record".to_string(),
                "identity" => "an identity change".to_string(),
                "account" => "an account change".to_string(),
                other => other.to_string(),
            },
            error: f("error").chars().take(300).collect(),
            id: e.id,
        });
    }
    Ok(out)
}

/// `GET /admin/problems`.
pub async fn page(State(state): State<AdminState>, ConnectInfo(peer): ConnectInfo<SocketAddr>, headers: HeaderMap) -> Response {
    let op = match require(&state, &headers, peer, Need::Page, None).await {
        Ok(op) => op,
        Err(response) => return response,
    };
    let letters = match dead_letters(&state).await {
        Ok(l) => Some(l),
        Err(error) => {
            tracing::warn!(event = "admin_problems_read_failed", %error, "the dead letters could not be read");
            None
        }
    };
    let stream = match super::relays::read_stream(&state).await {
        Ok(info) => super::relays::stream_summary(&info),
        Err(_) => "unknown".to_string(),
    };
    render(
        StatusCode::OK,
        &ProblemsPage {
            css: css(),
            nav: Some(Nav::page(&op, crate::admin::menu::PageId::Problems)),
            title: "Problems".to_string(),
            csrf: op.csrf.clone(),
            letters,
            stream,
        },
    )
}

#[derive(Debug, Default, Deserialize)]
pub struct LetterForm {
    #[serde(default)]
    pub csrf: String,
    #[serde(default)]
    pub id: String,
    pub confirm: Option<String>,
    /// The page the form was on, for Cancel.
    pub back: Option<String>,
}

fn bad_id(state: &AdminState, op: &crate::admin::auth::Operator) -> Response {
    message(state, Some(Nav::at(op, "/admin/problems")), StatusCode::BAD_REQUEST, "Not done", "That is not a record on this page.", Some(("/admin/problems", "Problems")))
}

/// `POST /admin/problems/replay`: `deadletter replay`.
pub async fn replay(State(state): State<AdminState>, ConnectInfo(peer): ConnectInfo<SocketAddr>, headers: HeaderMap, Form(form): Form<LetterForm>) -> Response {
    let op = match require(&state, &headers, peer, Need::Change, Some(&form.csrf)).await {
        Ok(op) => op,
        Err(response) => return response,
    };
    let id = form.id.trim().to_string();
    if !is_stream_id(&id) {
        return bad_id(&state, &op);
    }
    enqueue(&state, &op, &AdminCommand::DeadletterReplay { id }).await
}

/// `POST /admin/problems/dismiss`: `deadletter drop`, confirmed.
pub async fn dismiss(State(state): State<AdminState>, ConnectInfo(peer): ConnectInfo<SocketAddr>, headers: HeaderMap, Form(form): Form<LetterForm>) -> Response {
    let op = match require(&state, &headers, peer, Need::Change, Some(&form.csrf)).await {
        Ok(op) => op,
        Err(response) => return response,
    };
    let id = form.id.trim().to_string();
    if !is_stream_id(&id) {
        return bad_id(&state, &op);
    }
    let page = Confirm {
        title: "Dismiss a record",
        what: format!("The record from {} is taken off this list. Prism does not try it again.", id_time(&id)),
        undo: "This cannot be undone; the record stays on its own server and arrives again if its author changes it.",
        action: "/admin/problems/dismiss",
        hidden: vec![hidden_field("id", &id)],
        typed: None,
        button: "Dismiss",
    }
    .page(&op, &super::back_to(&state, &headers, form.back.as_deref(), "/admin/problems"));
    if let Some(response) = confirmed(page, form.confirm.as_deref(), None) {
        return response;
    }
    enqueue(&state, &op, &AdminCommand::DeadletterDrop { id }).await
}

#[derive(Debug, Default, Deserialize)]
pub struct SweepForm {
    #[serde(default)]
    pub csrf: String,
    pub confirm: Option<String>,
    /// The page the form was on, for Cancel.
    pub back: Option<String>,
}

/// `POST /admin/problems/sweep`: `deadletter sweep`, confirmed.
pub async fn sweep(State(state): State<AdminState>, ConnectInfo(peer): ConnectInfo<SocketAddr>, headers: HeaderMap, Form(form): Form<SweepForm>) -> Response {
    let op = match require(&state, &headers, peer, Need::Change, Some(&form.csrf)).await {
        Ok(op) => op,
        Err(response) => return response,
    };
    let page = Confirm {
        title: "Clear out the records Prism would now set aside",
        what: "Records on this list from accounts Prism does not keep are taken off it. Records from members, from accounts Prism follows, or naming a member stay.".to_string(),
        undo: "This cannot be undone; nothing is lost, because Prism would not store these records anyway.",
        action: "/admin/problems/sweep",
        hidden: Vec::new(),
        typed: None,
        button: "Clear out",
    }
    .page(&op, &super::back_to(&state, &headers, form.back.as_deref(), "/admin/problems"));
    if let Some(response) = confirmed(page, form.confirm.as_deref(), None) {
        return response;
    }
    enqueue(&state, &op, &AdminCommand::DeadletterSweep { dry_run: false }).await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ids_in_words() {
        assert!(is_stream_id("1697812345678-0"));
        assert!(!is_stream_id("abc-0") && !is_stream_id("123") && !is_stream_id("-1"));
        assert_eq!(id_time("1760918400000-3"), "2025-10-20 00:00 UTC");
    }
}
