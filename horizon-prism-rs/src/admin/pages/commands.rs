//! One command's page (`/admin/commands/{id}`): what was asked, by whom,
//! and how it ended. It reloads itself until the command finishes.

use std::net::SocketAddr;

use askama::Template;
use axum::extract::{ConnectInfo, Path, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::Response;
use serde_json::Value;

use super::{COMMAND_COLUMNS, CommandRow, CommandTuple, Link, Nav, css, message, render};
use crate::admin::AdminState;
use crate::admin::auth::{Need, require};

/// An `operator add` refused for its host can be sent again allowing it.
pub struct HostOffer {
    pub input: String,
    pub note: String,
}

#[derive(Template)]
#[template(path = "command.html")]
struct CommandPage {
    css: &'static str,
    nav: Option<Nav>,
    title: String,
    row: CommandRow,
    offer: Option<HostOffer>,
    csrf: String,
    /// The page of the account a Refract command was about.
    follow: Option<Link>,
    /// A done restart: whether Prism is back.
    restart: Option<bool>,
}

/// `GET /admin/commands/{id}`.
pub async fn page(
    State(state): State<AdminState>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    Path(id): Path<i64>,
) -> Response {
    let op = match require(&state, &headers, peer, Need::Page, None).await {
        Ok(op) => op,
        Err(response) => return response,
    };
    let row: Option<CommandTuple> = match sqlx::query_as(&format!(
        "SELECT {COMMAND_COLUMNS} FROM admin_commands c LEFT JOIN actors a ON a.did = c.requested_by WHERE c.id = $1"
    ))
    .bind(id)
    .fetch_optional(&state.pool)
    .await
    {
        Ok(r) => r,
        Err(e) => return super::internal_error(&state, &e),
    };
    let Some(row) = row else {
        return message(&state, Some(Nav::new(&op, "audit")), StatusCode::NOT_FOUND, "Not found", "There is no such command.", Some(("/admin/audit", "Audit log")));
    };
    let row = CommandRow::from_tuple(row);
    // A restart's page reloads every 5 seconds until this process is a
    // newer one than the one that settled it (admin-ui design §8.5).
    let restart = if row.command == "restart" && row.state == "done" {
        let finished: Option<chrono::DateTime<chrono::Utc>> =
            match sqlx::query_scalar("SELECT finished_at FROM admin_commands WHERE id = $1").bind(id).fetch_one(&state.pool).await {
                Ok(f) => f,
                Err(e) => return super::internal_error(&state, &e),
            };
        Some(finished.is_some_and(|f| state.settings.started_at > f))
    } else {
        None
    };
    let offer = (row.command == "operator add" && row.state == "failed" && row.outcome.contains("not on the allowed list")).then(|| HostOffer {
        input: row.args.get("input").and_then(Value::as_str).unwrap_or_default().to_string(),
        note: row.args.get("note").and_then(Value::as_str).unwrap_or_default().to_string(),
    });
    // A Refract check or end: the account's page, once it has run.
    let follow = row
        .args
        .get("did")
        .and_then(Value::as_str)
        .filter(|_| matches!(row.command.as_str(), "refract check" | "refract end") && !row.pending())
        .map(|did| Link { href: format!("/admin/refract/{did}"), label: "See the account".to_string() });
    render(
        StatusCode::OK,
        &CommandPage {
            css: css(),
            nav: Some(Nav::new(&op, "audit")),
            title: format!("Command {id}"),
            row,
            offer,
            csrf: op.csrf.clone(),
            restart,
            follow,
        },
    )
}
