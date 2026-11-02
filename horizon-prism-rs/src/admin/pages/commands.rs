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
use crate::admin::settings::view;

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
    /// The page of the account a Refract command was about, or of the
    /// settings a change was to.
    follow: Option<Link>,
    /// A done restart: whether Prism is back.
    restart: Option<bool>,
}

/// A `settings set`'s way back: the page its first setting is on.
fn settings_follow(state: &AdminState, row: &CommandRow) -> Option<Link> {
    if row.command != "settings set" {
        return None;
    }
    let key = row.args.get("changes")?.as_array()?.first()?.get("key")?.as_str()?;
    let place = view::place_of(key, &state.settings)?;
    Some(Link { href: view::href(key, &state.settings), label: format!("Back to {}", place.name()) })
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
        "SELECT {COMMAND_COLUMNS} FROM admin_commands c LEFT JOIN dids ad ON ad.did = c.requested_by LEFT JOIN actors a ON a.id = ad.id WHERE c.id = $1"
    ))
    .bind(id)
    .fetch_optional(&state.pool)
    .await
    {
        Ok(r) => r,
        Err(e) => return super::internal_error(&state, &e),
    };
    let Some(row) = row else {
        return message(&state, Some(Nav::at(&op, "/admin/audit")), StatusCode::NOT_FOUND, "Not found", "There is no such command.", Some(("/admin/audit", "Audit log")));
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
    // A Refract check or end: the account's page, once it has run. A
    // settings change: the page of its first setting. The indexing switch:
    // Indexing.
    let follow = row
        .args
        .get("did")
        .and_then(Value::as_str)
        .filter(|_| matches!(row.command.as_str(), "refract check" | "refract end") && !row.pending())
        .map(|did| Link { href: crate::admin::menu::account_href(did), label: "See the account".to_string() })
        .or_else(|| settings_follow(&state, &row))
        .or_else(|| (row.command == "indexing public-scope").then(|| Link { href: "/admin/indexing".to_string(), label: "Back to Indexing".to_string() }));
    render(
        StatusCode::OK,
        &CommandPage {
            css: css(),
            nav: Some(Nav::under(&op, crate::admin::menu::PageId::Audit)),
            title: format!("Command {id}"),
            row,
            offer,
            csrf: op.csrf.clone(),
            restart,
            follow,
        },
    )
}
