//! Activity (`/admin/activity`, admin-ui design §6.11): commands newest
//! first (when, who, what, state, the outcome in words with the raw result
//! behind `<details>`), and sign-ins and setup events. Filters by who,
//! what and state. Settings changes join with S2.

use std::net::SocketAddr;

use askama::Template;
use axum::extract::{ConnectInfo, Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::Response;
use chrono::{DateTime, Utc};
use serde::Deserialize;

use super::{COMMAND_COLUMNS, CommandRow, CommandTuple, Nav, css, render, when};
use crate::admin::AdminState;
use crate::admin::auth::{Need, require};

/// Rows per page.
pub const PAGE: i64 = 100;

pub struct LoginRow {
    pub at: String,
    pub who: String,
    pub outcome: String,
    pub client_ip: String,
}

/// A sign-in event: when, DID, stored handle, outcome, address.
type LoginTuple = (DateTime<Utc>, Option<String>, Option<String>, String, Option<String>);

#[derive(Template)]
#[template(path = "activity.html")]
struct ActivityPage {
    css: &'static str,
    nav: Option<Nav>,
    title: String,
    commands: Vec<CommandRow>,
    logins: Vec<LoginRow>,
    who: String,
    what: String,
    state: String,
    command_names: Vec<&'static str>,
    prev: Option<String>,
    next: Option<String>,
}

#[derive(Debug, Default, Deserialize)]
pub struct Filters {
    pub who: Option<String>,
    pub what: Option<String>,
    pub state: Option<String>,
    pub page: Option<i64>,
}

/// The command names the "what" filter offers.
pub const COMMAND_NAMES: [&str; 22] = [
    "member add",
    "member remove",
    "member deny",
    "member allow",
    "operator add",
    "operator remove",
    "operator host add",
    "operator host remove",
    "label add",
    "label remove",
    "scope rebuild",
    "backfill",
    "resync",
    "deadletter replay",
    "deadletter drop",
    "farsight status",
    "farsight prime",
    "farsight forget",
    "lists sync",
    "lists purge",
    "indexing public-scope",
    "indexing status",
];

fn outcome_words(outcome: &str) -> String {
    match outcome {
        "ok" => "signed in".to_string(),
        "setup" => "completed first-run setup".to_string(),
        "not_operator" => "refused: not an operator".to_string(),
        "host_not_allowed" => "refused: server not allowed".to_string(),
        "oauth_failed" => "sign-in not completed".to_string(),
        other => match other.strip_prefix("session_cut:") {
            Some("pds_changed") => "signed out: account moved to another server".to_string(),
            Some("not_operator") => "signed out: no longer an operator".to_string(),
            Some("host_not_allowed") => "signed out: server no longer allowed".to_string(),
            Some(reason) => format!("signed out: {reason}"),
            None => other.to_string(),
        },
    }
}

fn link(f: &Filters, page: i64) -> String {
    let mut query = url::form_urlencoded::Serializer::new(String::new());
    for (k, v) in [("who", &f.who), ("what", &f.what), ("state", &f.state)] {
        if let Some(v) = v.as_deref().filter(|v| !v.is_empty()) {
            query.append_pair(k, v);
        }
    }
    query.append_pair("page", &page.to_string());
    format!("/admin/activity?{}", query.finish())
}

/// `GET /admin/activity`.
pub async fn page(
    State(state): State<AdminState>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    Query(f): Query<Filters>,
) -> Response {
    let op = match require(&state, &headers, peer, Need::Page, None).await {
        Ok(op) => op,
        Err(response) => return response,
    };
    let page = f.page.unwrap_or(0).max(0);
    let who = f.who.as_deref().map(str::trim).filter(|v| !v.is_empty());
    let what = f.what.as_deref().filter(|v| !v.is_empty());
    let st = f.state.as_deref().filter(|v| !v.is_empty());
    let read = async {
        let commands: Vec<CommandTuple> = sqlx::query_as(&format!(
            "SELECT {COMMAND_COLUMNS} FROM admin_commands c LEFT JOIN actors a ON a.did = c.requested_by \
             WHERE ($1::text IS NULL OR ($1 = 'cli' AND c.requested_by IS NULL) OR c.requested_by = $1 OR a.handle = $1) \
               AND ($2::text IS NULL OR c.command = $2) AND ($3::text IS NULL OR c.state = $3) \
             ORDER BY c.id DESC LIMIT $4 OFFSET $5"
        ))
        .bind(who)
        .bind(what)
        .bind(st)
        .bind(PAGE + 1)
        .bind(page * PAGE)
        .fetch_all(&state.pool)
        .await?;
        let logins: Vec<LoginTuple> = sqlx::query_as(
            "SELECT l.at, l.did, a.handle, l.outcome, l.client_ip FROM admin_logins l LEFT JOIN actors a ON a.did = l.did \
             ORDER BY l.id DESC LIMIT 100",
        )
        .fetch_all(&state.pool)
        .await?;
        Ok::<_, sqlx::Error>((commands, logins))
    };
    let (mut commands, logins) = match read.await {
        Ok(r) => r,
        Err(e) => return super::internal_error(&state, &e),
    };
    let has_next = commands.len() > usize::try_from(PAGE).unwrap_or(usize::MAX);
    commands.truncate(usize::try_from(PAGE).unwrap_or(usize::MAX));
    let logins = logins
        .into_iter()
        .map(|(at, did, handle, outcome, ip)| LoginRow {
            at: when(at),
            who: handle.or(did).unwrap_or_else(|| "—".to_string()),
            outcome: outcome_words(&outcome),
            client_ip: ip.unwrap_or_default(),
        })
        .collect();
    render(
        StatusCode::OK,
        &ActivityPage {
            css: css(),
            nav: Some(Nav::new(&op, "activity")),
            title: "Activity".to_string(),
            commands: commands.into_iter().map(CommandRow::from_tuple).collect(),
            logins,
            who: who.unwrap_or_default().to_string(),
            what: what.unwrap_or_default().to_string(),
            state: st.unwrap_or_default().to_string(),
            command_names: COMMAND_NAMES.to_vec(),
            prev: (page > 0).then(|| link(&f, page - 1)),
            next: has_next.then(|| link(&f, page + 1)),
        },
    )
}
