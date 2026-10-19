//! Home (`/admin`, admin-ui design §6.1). S1 shows the members' counts
//! and the last five commands; the health tiles and attention list come
//! with S3.

use std::net::SocketAddr;

use askama::Template;
use axum::extract::{ConnectInfo, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::Response;

use super::{COMMAND_COLUMNS, CommandRow, CommandTuple, Nav, css, render};
use crate::admin::AdminState;
use crate::admin::auth::{Need, require};

#[derive(Template)]
#[template(path = "home.html")]
struct HomePage {
    css: &'static str,
    nav: Option<Nav>,
    title: String,
    members: i64,
    ready: i64,
    max_members: u64,
    denied: i64,
    recent: Vec<CommandRow>,
}

/// `GET /admin`.
pub async fn page(State(state): State<AdminState>, ConnectInfo(peer): ConnectInfo<SocketAddr>, headers: HeaderMap) -> Response {
    let op = match require(&state, &headers, peer, Need::Page, None).await {
        Ok(op) => op,
        Err(response) => return response,
    };
    let counts: Result<(i64, i64, i64), sqlx::Error> = sqlx::query_as(
        "SELECT (SELECT count(*) FROM members), (SELECT count(*) FROM members WHERE ready_at IS NOT NULL), \
         (SELECT count(*) FROM member_denials)",
    )
    .fetch_one(&state.pool)
    .await;
    let recent: Result<Vec<CommandTuple>, sqlx::Error> = sqlx::query_as(&format!(
        "SELECT {COMMAND_COLUMNS} FROM admin_commands c LEFT JOIN actors a ON a.did = c.requested_by ORDER BY c.id DESC LIMIT 5"
    ))
    .fetch_all(&state.pool)
    .await;
    let ((members, ready, denied), recent) = match (counts, recent) {
        (Ok(c), Ok(r)) => (c, r),
        (Err(e), _) | (_, Err(e)) => return super::internal_error(&state, &e),
    };
    render(
        StatusCode::OK,
        &HomePage {
            css: css(),
            nav: Some(Nav::new(&op, "home")),
            title: "Home".to_string(),
            members,
            ready,
            max_members: state.members.max_members,
            denied,
            recent: recent.into_iter().map(CommandRow::from_tuple).collect(),
        },
    )
}
