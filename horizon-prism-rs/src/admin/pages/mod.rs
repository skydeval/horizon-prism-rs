//! The admin pages (admin-ui design §6), one file per screen, and what
//! they share: the navigation, the message page, command rows in plain
//! words, and enqueuing a command.

pub mod activity;
pub mod commands;
pub mod home;
pub mod login;
pub mod members;
pub mod operators;
pub mod setup;

use askama::Template;
use axum::http::StatusCode;
use axum::response::{Html, IntoResponse, Redirect, Response};
use chrono::{DateTime, Utc};
use serde_json::Value;

use super::auth::Operator;
use super::{AdminState, CSS_HREF};
use crate::admin_executor::AdminCommand;
use crate::cli::dispatch::submit_as;

/// The navigation bar of a signed-in page.
pub struct Nav {
    /// How the operator is named: handle, else DID.
    pub who: String,
    pub csrf: String,
    /// The current screen: `home`, `members`, `activity`, `operators`.
    pub current: &'static str,
}

impl Nav {
    #[must_use]
    pub fn new(op: &Operator, current: &'static str) -> Self {
        Self {
            who: op.who().to_string(),
            csrf: op.csrf.clone(),
            current,
        }
    }
}

/// A link on a message page.
pub struct Link {
    pub href: String,
    pub label: String,
}

#[derive(Template)]
#[template(path = "message.html")]
struct MessagePage {
    css: &'static str,
    nav: Option<Nav>,
    title: String,
    message: String,
    link: Option<Link>,
}

/// The stylesheet's path, for templates.
#[must_use]
pub fn css() -> &'static str {
    CSS_HREF.as_str()
}

/// Render a template with a status.
pub fn render(status: StatusCode, page: &impl Template) -> Response {
    match page.render() {
        Ok(html) => (status, Html(html)).into_response(),
        Err(error) => {
            tracing::error!(event = "admin_render_failed", %error, "an admin page failed to render");
            (StatusCode::INTERNAL_SERVER_ERROR, "page error").into_response()
        }
    }
}

/// A page with one message.
pub fn message(
    _state: &AdminState,
    nav: Option<Nav>,
    status: StatusCode,
    title: &str,
    text: &str,
    link: Option<(&str, &str)>,
) -> Response {
    render(
        status,
        &MessagePage {
            css: css(),
            nav,
            title: title.to_string(),
            message: text.to_string(),
            link: link.map(|(href, label)| Link {
                href: href.to_string(),
                label: label.to_string(),
            }),
        },
    )
}

/// A database failure, said plainly and logged.
pub fn internal_error(state: &AdminState, error: &sqlx::Error) -> Response {
    tracing::error!(event = "admin_page_failed", %error, "an admin page could not read the database");
    message(
        state,
        None,
        StatusCode::INTERNAL_SERVER_ERROR,
        "Something went wrong",
        "Prism could not read its database just now. Try again in a moment.",
        Some(("/admin", "Home")),
    )
}

/// Any other `/admin` path.
pub async fn not_found() -> Response {
    render(
        StatusCode::NOT_FOUND,
        &MessagePage {
            css: css(),
            nav: None,
            title: "Not found".to_string(),
            message: "There is no page here.".to_string(),
            link: Some(Link {
                href: "/admin".to_string(),
                label: "Home".to_string(),
            }),
        },
    )
}

/// Queue a command for the signed-in operator and go to its page.
pub async fn enqueue(state: &AdminState, op: &Operator, cmd: &AdminCommand) -> Response {
    let (command, _) = cmd.to_row();
    match submit_as(&state.pool, cmd, Some(&op.did)).await {
        Ok(id) => {
            metrics::counter!("admin_commands_enqueued_total", "command" => command).increment(1);
            tracing::info!(event = "admin_command_enqueued", id, command, did = %op.did, "admin command queued from the web UI");
            Redirect::to(&format!("/admin/commands/{id}")).into_response()
        }
        Err(error) => internal_error(state, &error),
    }
}

/// A time as pages show it.
#[must_use]
pub fn when(t: DateTime<Utc>) -> String {
    t.format("%Y-%m-%d %H:%M UTC").to_string()
}

/// [`when`] for a time that may be missing.
#[must_use]
pub fn when_opt(t: Option<DateTime<Utc>>) -> String {
    t.map_or_else(|| "—".to_string(), when)
}

fn arg<'a>(args: &'a Value, key: &str) -> &'a str {
    args.get(key).and_then(Value::as_str).unwrap_or("?")
}

/// A command in plain words.
#[must_use]
pub fn describe(command: &str, args: &Value) -> String {
    match command {
        "member add" => format!("Add member {}", arg(args, "did")),
        "member remove" => format!("Remove member {}", arg(args, "did")),
        "member deny" => format!("Deny {}", arg(args, "did")),
        "member allow" => format!("Lift the denial of {}", arg(args, "did")),
        "operator add" => format!("Add operator {}", arg(args, "input")),
        "operator remove" => format!("Remove operator {}", arg(args, "did")),
        "operator host add" => format!("Allow sign-in from {}", arg(args, "pattern")),
        "operator host remove" => format!("Stop allowing sign-in from {}", arg(args, "pattern")),
        "label add" => format!("Add label {} to {}", arg(args, "val"), arg(args, "subject")),
        "label remove" => format!("Remove label {} from {}", arg(args, "val"), arg(args, "subject")),
        "scope rebuild" => "Rebuild the set of accounts Prism follows".to_string(),
        "backfill" => format!("Sync {} again", arg(args, "did")),
        "resync" => "Re-sync private data and members".to_string(),
        "deadletter replay" => format!("Try record {} again", arg(args, "id")),
        "deadletter drop" => format!("Dismiss record {}", arg(args, "id")),
        "farsight status" => "Ask Farsight for its status".to_string(),
        "farsight prime" => format!("Ask Farsight to read {} again", arg(args, "did")),
        "farsight forget" => format!("Forget remembered blocks of {}", arg(args, "did")),
        "lists sync" => format!("Sync list {}", arg(args, "uri")),
        "lists purge" => format!("Clean up the lists of {}", arg(args, "did")),
        "indexing public-scope" => format!("Set public data to {}", arg(args, "value")),
        "indexing status" => "Read the public data status".to_string(),
        other => other.to_string(),
    }
}

/// A finished or waiting command's outcome in plain words.
#[must_use]
pub fn outcome(command: &str, state: &str, result: Option<&Value>) -> String {
    let r = result.cloned().unwrap_or(Value::Null);
    let status = r.get("status").and_then(Value::as_str).unwrap_or("");
    match state {
        "queued" => return "Waiting to run".to_string(),
        "running" => return "Running".to_string(),
        "failed" => {
            let error = r.get("error").and_then(Value::as_str).unwrap_or("no reason given");
            return format!("Failed: {error}");
        }
        _ => {}
    }
    match (command, status) {
        ("member add", "added") => "Added. Their first sync is queued.".to_string(),
        ("member add", "already_member") => "Already a member; nothing changed.".to_string(),
        ("member remove", _) => "Removed. Their data here is being deleted in the background.".to_string(),
        ("member deny", "denied") if r.get("removed") == Some(&Value::Bool(true)) => {
            "Denied, and removed as a member. Their data here is being deleted in the background.".to_string()
        }
        ("member deny", "denied") => "Denied.".to_string(),
        ("member deny", "already_denied") => "Was already denied; nothing changed.".to_string(),
        ("member allow", "allowed") => "The denial is lifted. They are not a member until added.".to_string(),
        ("member allow", "not_denied") => "Was not denied; nothing changed.".to_string(),
        ("operator add", "added" | "already_operator") => {
            let host = r.get("host").and_then(Value::as_str).unwrap_or("?");
            let first = if status == "added" { "Added." } else { "Already an operator; their server was updated." };
            let added = if r.get("host_added") == Some(&Value::Bool(true)) {
                format!(" Sign-in from {host} is now allowed.")
            } else {
                String::new()
            };
            let closed = if r.get("setup_closed") == Some(&Value::Bool(true)) { " First-run setup is closed." } else { "" };
            format!("{first}{added}{closed}")
        }
        ("operator remove", _) => "Removed. Their sessions have ended.".to_string(),
        ("operator host add", _) if r.get("added") == Some(&Value::Bool(false)) => "Was already allowed.".to_string(),
        ("operator host add", _) => "Allowed.".to_string(),
        ("operator host remove", _) if r.get("any_host") == Some(&Value::Bool(true)) => {
            "Removed. The list is now empty, so an operator on any server can sign in.".to_string()
        }
        ("operator host remove", _) => "Removed.".to_string(),
        _ => "Done.".to_string(),
    }
}

/// A command as the pages list it.
pub struct CommandRow {
    pub id: i64,
    pub when: String,
    pub who: String,
    pub what: String,
    pub state: String,
    pub outcome: String,
    pub raw: String,
    /// The command's own name and arguments, for offers on its page.
    pub command: String,
    pub args: Value,
}

/// The row type commands are read as.
pub type CommandTuple = (i64, String, Value, String, Option<Value>, DateTime<Utc>, Option<String>, Option<String>);

/// The columns of a [`CommandTuple`], with the requester's stored handle.
pub const COMMAND_COLUMNS: &str = "c.id, c.command, c.args, c.state, c.result, c.created_at, c.requested_by, a.handle";

impl CommandRow {
    #[must_use]
    pub fn from_tuple((id, command, args, state, result, created, requested_by, handle): CommandTuple) -> Self {
        let who = match (requested_by, handle) {
            (None, _) => "command line".to_string(),
            (Some(_), Some(h)) => h,
            (Some(did), None) => did,
        };
        let raw = result
            .as_ref()
            .map_or_else(String::new, |r| serde_json::to_string_pretty(r).unwrap_or_default());
        Self {
            id,
            when: when(created),
            who,
            what: describe(&command, &args),
            outcome: outcome(&command, &state, result.as_ref()),
            state,
            raw,
            command,
            args,
        }
    }

    /// Whether the command has not finished.
    #[must_use]
    pub fn pending(&self) -> bool {
        matches!(self.state.as_str(), "queued" | "running")
    }
}

/// The word a typed confirmation needs for an account (§5.5): its stored
/// handle, else the last eight characters of its DID.
pub async fn typed_word(state: &AdminState, did: &str) -> Result<String, sqlx::Error> {
    let handle: Option<String> = sqlx::query_scalar("SELECT handle FROM actors WHERE did = $1")
        .bind(did)
        .fetch_optional(&state.pool)
        .await?
        .flatten();
    Ok(handle.unwrap_or_else(|| did.chars().rev().take(8).collect::<Vec<_>>().into_iter().rev().collect()))
}

/// Whether a typed word matches, ignoring case and surrounding space.
#[must_use]
pub fn typed_matches(expected: &str, typed: Option<&str>) -> bool {
    typed.is_some_and(|t| t.trim().trim_start_matches('@').eq_ignore_ascii_case(expected))
}

/// A hidden field of a confirmation form.
pub struct Hidden {
    pub name: String,
    pub value: String,
}

/// The confirmation page of a destructive action (§5.5): what will happen
/// in plain words, and for a typed one, the word to type.
#[derive(Template)]
#[template(path = "confirm.html")]
pub struct ConfirmPage {
    pub css: &'static str,
    pub nav: Option<Nav>,
    pub title: String,
    pub what: String,
    pub undo: String,
    pub action: String,
    pub csrf: String,
    pub hidden: Vec<Hidden>,
    pub typed: Option<String>,
    pub button: String,
    pub error: Option<String>,
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn commands_read_in_plain_words() {
        assert_eq!(describe("member add", &json!({"did": "did:plc:x"})), "Add member did:plc:x");
        assert_eq!(outcome("member add", "done", Some(&json!({"status": "added"}))), "Added. Their first sync is queued.");
        assert_eq!(outcome("member add", "failed", Some(&json!({"error": "denied"}))), "Failed: denied");
        assert_eq!(outcome("member add", "queued", None), "Waiting to run");
        assert!(outcome("operator host remove", "done", Some(&json!({"any_host": true}))).contains("any server"));
        assert!(typed_matches("alice.example.com", Some(" @Alice.Example.com ")));
        assert!(!typed_matches("alice.example.com", Some("bob")));
        assert!(!typed_matches("alice.example.com", None));
    }
}
