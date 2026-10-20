//! Problems (`/admin/problems`, admin-ui design §6.7): "Records Prism could
//! not process", the newest 100 of the dead-letter stream by `XREVRANGE`,
//! each with "try again" (`deadletter replay`) and "dismiss" (`deadletter
//! drop`, confirmed); and the network stream's state, the content of
//! `admin stream info` in words.

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
use crate::admin::health::count;
use crate::admin_executor::AdminCommand;
use crate::cli::commands::stream_info::{self, StreamInfo};
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
    stream: Vec<String>,
}

/// A stream id's time: its first part is milliseconds since the epoch.
fn id_time(id: &str) -> String {
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

/// The stream's state in sentences.
#[must_use]
pub fn stream_words(info: &StreamInfo) -> Vec<String> {
    let mut out = vec![match info.length {
        Some(n) => format!("The network stream holds {} records waiting for Prism or kept for a while after.", count(n as f64)),
        None => "The network stream's length is unknown.".to_string(),
    }];
    match &info.group {
        None => out.push("Prism's reader is missing, so Prism is not reading the stream. The server's log says why.".to_string()),
        Some(g) => {
            out.push(match g.lag {
                Some(0) => "Prism has read everything in it.".to_string(),
                Some(n) => format!("{} records are not read yet.", count(n as f64)),
                None => "Redis cannot say how far behind Prism is: part of the stream was dropped before Prism read it.".to_string(),
            });
            if let Some(p) = g.pending.filter(|p| *p > 0) {
                out.push(format!("{} records are being processed now.", count(p as f64)));
            }
            if let Some(id) = &g.last_delivered_id {
                out.push(format!("The last record read arrived at {}.", id_time(id)));
            }
        }
    }
    out
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
    let fh = &state.settings.running.firehose;
    let stream = match &state.health.redis {
        None => vec!["The network stream's state is unknown: Redis is not connected to these pages.".to_string()],
        Some(redis) => match stream_info::read(&mut redis.clone(), &fh.stream_key, &fh.group).await {
            Ok(info) => stream_words(&info),
            Err(_) => vec!["The network stream could not be read just now.".to_string()],
        },
    };
    render(
        StatusCode::OK,
        &ProblemsPage {
            css: css(),
            nav: Some(Nav::new(&op, "problems")),
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
}

fn bad_id(state: &AdminState, op: &crate::admin::auth::Operator) -> Response {
    message(state, Some(Nav::new(op, "problems")), StatusCode::BAD_REQUEST, "Not done", "That is not a record on this page.", Some(("/admin/problems", "Problems")))
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
    .page(&op, "problems");
    if let Some(response) = confirmed(page, form.confirm.as_deref(), None) {
        return response;
    }
    enqueue(&state, &op, &AdminCommand::DeadletterDrop { id }).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cli::commands::stream_info::GroupInfo;

    #[test]
    fn ids_and_stream_words() {
        assert!(is_stream_id("1697812345678-0"));
        assert!(!is_stream_id("abc-0") && !is_stream_id("123") && !is_stream_id("-1"));
        assert_eq!(id_time("1760918400000-3"), "2025-10-20 00:00 UTC");
        let behind = StreamInfo {
            length: Some(5000),
            first_entry_id: Some("1-0".into()),
            group: Some(GroupInfo { pending: Some(2), last_delivered_id: Some("1760918400000-0".into()), lag: Some(1234) }),
        };
        let words = stream_words(&behind).join(" ");
        assert!(words.contains("5,000 records") && words.contains("1,234 records are not read yet") && words.contains("2 records are being processed"), "{words}");
        let missing = StreamInfo { length: Some(1), first_entry_id: None, group: None };
        assert!(stream_words(&missing).join(" ").contains("reader is missing"));
    }
}
