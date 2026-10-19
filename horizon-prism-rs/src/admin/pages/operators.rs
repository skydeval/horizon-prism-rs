//! Operators (`/admin/operators`, admin-ui design §6.12): who may sign in,
//! and from which servers. Every change is an operator command (§11).
//!
//! S1 shows each operator's last known server (from the latest fresh
//! resolution, recorded at sign-in and every re-check) rather than
//! resolving every operator when the page loads.

use std::net::SocketAddr;

use askama::Template;
use axum::Form;
use axum::extract::{ConnectInfo, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::Response;
use chrono::{DateTime, Utc};
use serde::Deserialize;

use super::{ConfirmPage, Hidden, Nav, css, enqueue, message, render, typed_matches, typed_word, when};
use crate::admin::AdminState;
use crate::admin::auth::{Need, Operator, require};
use crate::admin::operators::{endpoint_host, host_allowed, parse_pattern};
use crate::admin_executor::AdminCommand;

pub struct OperatorRow {
    pub did: String,
    pub handle: String,
    pub note: String,
    pub host: String,
    pub checked: String,
    pub added_by: String,
    pub added: String,
    pub can_sign_in: bool,
}

pub struct HostRow {
    pub pattern: String,
    pub covers: String,
    pub added_by: String,
    pub added: String,
}

#[derive(Template)]
#[template(path = "operators.html")]
struct OperatorsPage {
    css: &'static str,
    nav: Option<Nav>,
    title: String,
    csrf: String,
    operators: Vec<OperatorRow>,
    hosts: Vec<HostRow>,
}

type OperatorTuple = (String, Option<String>, Option<String>, Option<String>, Option<DateTime<Utc>>, String, DateTime<Utc>);

fn by(label: &str) -> String {
    match label {
        "cli" => "command line".to_string(),
        "setup" => "first-run setup".to_string(),
        other => other.to_string(),
    }
}

/// `GET /admin/operators`.
pub async fn page(State(state): State<AdminState>, ConnectInfo(peer): ConnectInfo<SocketAddr>, headers: HeaderMap) -> Response {
    let op = match require(&state, &headers, peer, Need::Page, None).await {
        Ok(op) => op,
        Err(response) => return response,
    };
    let read = async {
        let operators: Vec<OperatorTuple> = sqlx::query_as(
            "SELECT o.did, a.handle, o.note, o.pds_endpoint, o.pds_checked_at, o.added_by, o.added_at \
             FROM admin_operators o LEFT JOIN actors a ON a.did = o.did ORDER BY o.added_at, o.did",
        )
        .fetch_all(&state.pool)
        .await?;
        let hosts: Vec<(String, String, DateTime<Utc>)> =
            sqlx::query_as("SELECT pattern, added_by, added_at FROM admin_operator_hosts ORDER BY pattern")
                .fetch_all(&state.pool)
                .await?;
        Ok::<_, sqlx::Error>((operators, hosts))
    };
    let (operators, hosts) = match read.await {
        Ok(r) => r,
        Err(e) => return super::internal_error(&state, &e),
    };
    let patterns: Vec<String> = hosts.iter().map(|(p, _, _)| p.clone()).collect();
    let operators: Vec<OperatorRow> = operators
        .into_iter()
        .map(|(did, handle, note, endpoint, checked, added_by, added)| {
            let host = endpoint.as_deref().and_then(endpoint_host).unwrap_or_default();
            OperatorRow {
                can_sign_in: !host.is_empty() && host_allowed(&patterns, &host),
                handle: handle.unwrap_or_default(),
                note: note.unwrap_or_default(),
                host,
                checked: checked.map_or_else(|| "never".to_string(), when),
                added_by: by(&added_by),
                added: when(added),
                did,
            }
        })
        .collect();
    let hosts = hosts
        .into_iter()
        .map(|(pattern, added_by, added)| {
            let covered: Vec<&str> = operators
                .iter()
                .filter(|o| !o.host.is_empty() && host_allowed(std::slice::from_ref(&pattern), &o.host))
                .map(|o| if o.handle.is_empty() { o.did.as_str() } else { o.handle.as_str() })
                .collect();
            HostRow {
                covers: if covered.is_empty() { "no operator".to_string() } else { covered.join(", ") },
                pattern,
                added_by: by(&added_by),
                added: when(added),
            }
        })
        .collect();
    render(
        StatusCode::OK,
        &OperatorsPage {
            css: css(),
            nav: Some(Nav::new(&op, "operators")),
            title: "Operators".to_string(),
            csrf: op.csrf.clone(),
            operators,
            hosts,
        },
    )
}

#[derive(Debug, Deserialize)]
pub struct OperatorForm {
    #[serde(default)]
    pub csrf: String,
    #[serde(default)]
    pub input: String,
    #[serde(default)]
    pub did: String,
    #[serde(default)]
    pub note: String,
    #[serde(default)]
    pub pattern: String,
    pub add_host: Option<String>,
    pub confirm: Option<String>,
    pub typed: Option<String>,
}

fn refused(op: &Operator, state: &AdminState, status: StatusCode, text: &str) -> Response {
    message(state, Some(Nav::new(op, "operators")), status, "Not done", text, Some(("/admin/operators", "Operators")))
}

#[allow(clippy::too_many_arguments)]
fn confirm_page(op: &Operator, title: &str, what: String, undo: &str, action: &str, hidden: Vec<Hidden>, typed: Option<String>, button: &str) -> ConfirmPage {
    ConfirmPage {
        css: css(),
        nav: Some(Nav::new(op, "operators")),
        title: title.to_string(),
        what,
        undo: undo.to_string(),
        action: action.to_string(),
        csrf: op.csrf.clone(),
        hidden,
        typed,
        button: button.to_string(),
        error: None,
    }
}

fn hidden(name: &str, value: &str) -> Hidden {
    Hidden { name: name.to_string(), value: value.to_string() }
}

/// `POST /admin/operators/add`: refused by the command when the account's
/// server is not allowed; its page then offers to allow it too, which is
/// confirmed first.
pub async fn add(
    State(state): State<AdminState>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    Form(form): Form<OperatorForm>,
) -> Response {
    let op = match require(&state, &headers, peer, Need::Change, Some(&form.csrf)).await {
        Ok(op) => op,
        Err(response) => return response,
    };
    let input = form.input.trim().trim_start_matches('@').to_string();
    let valid = proto_blue::syntax::Did::is_valid(&input) || proto_blue::syntax::Handle::is_valid(&input.to_ascii_lowercase());
    if !valid {
        return refused(&op, &state, StatusCode::BAD_REQUEST, "Enter a handle or a DID.");
    }
    let note = Some(form.note.trim().to_string()).filter(|n| !n.is_empty());
    let add_host = form.add_host.as_deref() == Some("yes");
    if add_host && form.confirm.as_deref() != Some("yes") {
        let mut fields = vec![hidden("input", &input), hidden("add_host", "yes")];
        if let Some(n) = &note {
            fields.push(hidden("note", n));
        }
        let page = confirm_page(
            &op,
            "Add an operator and allow their server",
            format!("{input} becomes an operator, and sign-in from their account's server is allowed. Anyone who can make an account on that server with a stolen identity could then try to sign in here."),
            "You can remove the operator and the server again later.",
            "/admin/operators/add",
            fields,
            None,
            "Add and allow",
        );
        return render(StatusCode::OK, &page);
    }
    enqueue(&state, &op, &AdminCommand::OperatorAdd { input: input.to_ascii_lowercase(), note, add_host }).await
}

/// `POST /admin/operators/remove` (typed; never the last operator).
pub async fn remove(
    State(state): State<AdminState>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    Form(form): Form<OperatorForm>,
) -> Response {
    let op = match require(&state, &headers, peer, Need::Change, Some(&form.csrf)).await {
        Ok(op) => op,
        Err(response) => return response,
    };
    let did = form.did.trim().to_string();
    let count: i64 = match sqlx::query_scalar("SELECT count(*) FROM admin_operators").fetch_one(&state.pool).await {
        Ok(n) => n,
        Err(e) => return super::internal_error(&state, &e),
    };
    if count <= 1 {
        return refused(&op, &state, StatusCode::CONFLICT, "This is the last operator. Add another operator first.");
    }
    let word = match typed_word(&state, &did).await {
        Ok(w) => w,
        Err(e) => return super::internal_error(&state, &e),
    };
    let what = if did == op.did {
        format!("You ({did}) stop being an operator and are signed out at once.")
    } else {
        format!("{did} stops being an operator and is signed out at once.")
    };
    let mut page = confirm_page(&op, "Remove an operator", what, "They can be added again later.", "/admin/operators/remove", vec![hidden("did", &did)], Some(word.clone()), "Remove operator");
    if form.confirm.as_deref() != Some("yes") {
        return render(StatusCode::OK, &page);
    }
    if !typed_matches(&word, form.typed.as_deref()) {
        page.error = Some(format!("Type {word} exactly to confirm."));
        return render(StatusCode::BAD_REQUEST, &page);
    }
    enqueue(&state, &op, &AdminCommand::OperatorRemove { did }).await
}

/// `POST /admin/operators/hosts/add`.
pub async fn host_add(
    State(state): State<AdminState>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    Form(form): Form<OperatorForm>,
) -> Response {
    let op = match require(&state, &headers, peer, Need::Change, Some(&form.csrf)).await {
        Ok(op) => op,
        Err(response) => return response,
    };
    match parse_pattern(&form.pattern) {
        Ok(pattern) => enqueue(&state, &op, &AdminCommand::OperatorHostAdd { pattern }).await,
        Err(problem) => refused(&op, &state, StatusCode::BAD_REQUEST, &format!("That is {problem}.")),
    }
}

/// `POST /admin/operators/hosts/remove` (confirmed; removing the last one
/// is typed, `ANY`, since an empty list allows any server).
pub async fn host_remove(
    State(state): State<AdminState>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    Form(form): Form<OperatorForm>,
) -> Response {
    let op = match require(&state, &headers, peer, Need::Change, Some(&form.csrf)).await {
        Ok(op) => op,
        Err(response) => return response,
    };
    let pattern = form.pattern.trim().to_ascii_lowercase();
    let count: i64 = match sqlx::query_scalar("SELECT count(*) FROM admin_operator_hosts").fetch_one(&state.pool).await {
        Ok(n) => n,
        Err(e) => return super::internal_error(&state, &e),
    };
    let last = count <= 1;
    let (what, typed) = if last {
        (
            format!("{pattern} is the last server on the list. With no server listed, an operator on any server can sign in; only the operator list then decides who."),
            Some("ANY".to_string()),
        )
    } else {
        (format!("Sign-in from {pattern} stops. Operators whose account is there are signed out at their next request."), None)
    };
    let mut page = confirm_page(&op, "Stop allowing a server", what, "You can allow it again later.", "/admin/operators/hosts/remove", vec![hidden("pattern", &pattern)], typed.clone(), "Remove");
    if form.confirm.as_deref() != Some("yes") {
        return render(StatusCode::OK, &page);
    }
    if let Some(word) = typed
        && !typed_matches(&word, form.typed.as_deref())
    {
        page.error = Some(format!("Type {word} exactly to confirm."));
        return render(StatusCode::BAD_REQUEST, &page);
    }
    enqueue(&state, &op, &AdminCommand::OperatorHostRemove { pattern }).await
}
