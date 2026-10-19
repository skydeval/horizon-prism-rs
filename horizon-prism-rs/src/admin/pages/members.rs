//! Members (`/admin/members`, admin-ui design §6.2; membership design §8,
//! #735): the members with their readiness, the count against the limit,
//! auto-join and its hosts, and the denials. Add; remove and deny (typed);
//! allow. Handles come from stored `actors` rows, so a page makes no
//! outbound call.

use std::net::SocketAddr;

use askama::Template;
use axum::Form;
use axum::extract::{ConnectInfo, Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::Response;
use chrono::{DateTime, Utc};
use serde::Deserialize;

use super::{ConfirmPage, Hidden, Nav, css, enqueue, message, render, typed_matches, typed_word, when, when_opt};
use crate::admin::AdminState;
use crate::admin::auth::{Need, Operator, require};
use crate::admin_executor::AdminCommand;

/// Members per page.
pub const PAGE: i64 = 100;

pub struct MemberRow {
    pub did: String,
    pub handle: String,
    pub source: String,
    pub added: String,
    pub ready: String,
    pub first_ready: String,
    pub owner: bool,
    pub diverged: String,
}

pub struct DenialRow {
    pub did: String,
    pub handle: String,
    pub reason: String,
    pub by: String,
    pub at: String,
}

#[derive(Template)]
#[template(path = "members.html")]
struct MembersPage {
    css: &'static str,
    nav: Option<Nav>,
    title: String,
    csrf: String,
    rows: Vec<MemberRow>,
    denials: Vec<DenialRow>,
    total: i64,
    ready: i64,
    max_members: u64,
    auto_enroll: bool,
    patterns: String,
    page: i64,
    prev: Option<i64>,
    next: Option<i64>,
}

#[derive(Debug, Deserialize)]
pub struct PageQuery {
    pub page: Option<i64>,
}

type MemberTuple = (String, Option<String>, String, DateTime<Utc>, Option<DateTime<Utc>>, Option<DateTime<Utc>>, Option<DateTime<Utc>>);
type DenialTuple = (String, Option<String>, Option<String>, String, DateTime<Utc>);

/// `GET /admin/members`.
pub async fn page(
    State(state): State<AdminState>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    Query(q): Query<PageQuery>,
) -> Response {
    let op = match require(&state, &headers, peer, Need::Page, None).await {
        Ok(op) => op,
        Err(response) => return response,
    };
    let page = q.page.unwrap_or(0).max(0);
    let read = async {
        let members: Vec<MemberTuple> = sqlx::query_as(
            "SELECT m.did, a.handle, m.source, m.added_at, m.ready_at, m.first_ready_at, s.diverged_since \
             FROM members m LEFT JOIN actors a ON a.did = m.did LEFT JOIN sync_state s ON s.did = m.did \
             ORDER BY m.added_at, m.did LIMIT $1 OFFSET $2",
        )
        .bind(PAGE + 1)
        .bind(page * PAGE)
        .fetch_all(&state.pool)
        .await?;
        let counts: (i64, i64) = sqlx::query_as("SELECT count(*), count(*) FILTER (WHERE ready_at IS NOT NULL) FROM members")
            .fetch_one(&state.pool)
            .await?;
        let denials: Vec<DenialTuple> = sqlx::query_as(
            "SELECT d.did, a.handle, d.reason, d.denied_by, d.denied_at FROM member_denials d \
             LEFT JOIN actors a ON a.did = d.did ORDER BY d.denied_at DESC, d.did",
        )
        .fetch_all(&state.pool)
        .await?;
        Ok::<_, sqlx::Error>((members, counts, denials))
    };
    let (mut members, (total, ready), denials) = match read.await {
        Ok(r) => r,
        Err(e) => return super::internal_error(&state, &e),
    };
    let has_next = members.len() > usize::try_from(PAGE).unwrap_or(usize::MAX);
    members.truncate(usize::try_from(PAGE).unwrap_or(usize::MAX));
    let snapshot = state.scope.read_guard().await;
    let rows = members
        .into_iter()
        .map(|(did, handle, source, added, ready_at, first_ready, diverged)| MemberRow {
            owner: snapshot.is_owner(&did),
            handle: handle.unwrap_or_default(),
            source: if source == "admin" { "added by an operator".to_string() } else { "auto-join".to_string() },
            added: when(added),
            ready: when_opt(ready_at),
            first_ready: when_opt(first_ready),
            diverged: diverged.map(when).unwrap_or_default(),
            did,
        })
        .collect();
    drop(snapshot);
    let denials = denials
        .into_iter()
        .map(|(did, handle, reason, by, at)| DenialRow {
            did,
            handle: handle.unwrap_or_default(),
            reason: reason.unwrap_or_default(),
            by: if by == "cli" { "command line".to_string() } else { by },
            at: when(at),
        })
        .collect();
    let patterns = if state.members.enroll_pds_patterns.is_empty() {
        "the servers of your own network".to_string()
    } else {
        state.members.enroll_pds_patterns.join(", ")
    };
    render(
        StatusCode::OK,
        &MembersPage {
            css: css(),
            nav: Some(Nav::new(&op, "members")),
            title: "Members".to_string(),
            csrf: op.csrf.clone(),
            rows,
            denials,
            total,
            ready,
            max_members: state.members.max_members,
            auto_enroll: state.members.auto_enroll,
            patterns,
            page,
            prev: (page > 0).then(|| page - 1),
            next: has_next.then_some(page + 1),
        },
    )
}

#[derive(Debug, Deserialize)]
pub struct AddForm {
    #[serde(default)]
    pub csrf: String,
    #[serde(default)]
    pub input: String,
}

/// An account typed into a form: a DID as is, a handle resolved (both
/// ways) to its DID.
async fn account(state: &AdminState, input: &str) -> Result<String, String> {
    let input = input.trim().trim_start_matches('@');
    if input.starts_with("did:") {
        return if proto_blue::syntax::Did::is_valid(input) { Ok(input.to_string()) } else { Err("That is not a valid DID.".to_string()) };
    }
    let handle = input.to_ascii_lowercase();
    if !proto_blue::syntax::Handle::is_valid(&handle) {
        return Err("Enter a handle or a DID.".to_string());
    }
    state
        .identity
        .resolve_handle(&handle)
        .await
        .ok_or_else(|| "That handle could not be found, or does not point back to its account.".to_string())
}

/// `POST /admin/members/add`.
pub async fn add(
    State(state): State<AdminState>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    Form(form): Form<AddForm>,
) -> Response {
    let op = match require(&state, &headers, peer, Need::Change, Some(&form.csrf)).await {
        Ok(op) => op,
        Err(response) => return response,
    };
    match account(&state, &form.input).await {
        Ok(did) => enqueue(&state, &op, &AdminCommand::MemberAdd { did }).await,
        Err(problem) => message(&state, Some(Nav::new(&op, "members")), StatusCode::BAD_REQUEST, "Not added", &problem, Some(("/admin/members", "Members"))),
    }
}

#[derive(Debug, Deserialize)]
pub struct DidForm {
    #[serde(default)]
    pub csrf: String,
    #[serde(default)]
    pub did: String,
    pub confirm: Option<String>,
    pub typed: Option<String>,
    pub reason: Option<String>,
}

/// The refusal of a DID that is not one.
fn invalid_did(state: &AdminState, op: &Operator, did: &str) -> Option<Response> {
    (!proto_blue::syntax::Did::is_valid(did)).then(|| {
        message(state, Some(Nav::new(op, "members")), StatusCode::BAD_REQUEST, "Not done", "That is not a valid DID.", Some(("/admin/members", "Members")))
    })
}

/// A typed action's two steps (§5.5): without `confirm=yes`, the
/// confirmation page; with it, the typed word must match, else the page
/// again with the problem. `Ok` once confirmed.
async fn confirmed(state: &AdminState, form: &DidForm, page: ConfirmPage) -> Result<(), Response> {
    let word = typed_word(state, &form.did).await.map_err(|e| super::internal_error(state, &e))?;
    let mut page = ConfirmPage { typed: Some(word.clone()), ..page };
    if form.confirm.as_deref() != Some("yes") {
        return Err(render(StatusCode::OK, &page));
    }
    if !typed_matches(&word, form.typed.as_deref()) {
        page.error = Some(format!("Type {word} exactly to confirm."));
        return Err(render(StatusCode::BAD_REQUEST, &page));
    }
    Ok(())
}

/// `POST /admin/members/remove` (typed: the member's handle).
pub async fn remove(
    State(state): State<AdminState>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    Form(form): Form<DidForm>,
) -> Response {
    let op = match require(&state, &headers, peer, Need::Change, Some(&form.csrf)).await {
        Ok(op) => op,
        Err(response) => return response,
    };
    if let Some(r) = invalid_did(&state, &op, &form.did) {
        return r;
    }
    let page = ConfirmPage {
        css: css(),
        nav: Some(Nav::new(&op, "members")),
        title: "Remove a member".to_string(),
        what: format!("{} stops being a member. Prism deletes what it holds for them as a member: their private reading, their bookmarks and mutes here, and the data only their membership kept.", form.did),
        undo: "This cannot be undone. Adding them again starts their sync from the beginning.".to_string(),
        action: "/admin/members/remove".to_string(),
        csrf: op.csrf.clone(),
        hidden: vec![Hidden { name: "did".to_string(), value: form.did.clone() }],
        typed: None,
        button: "Remove member".to_string(),
        error: None,
    };
    if let Err(r) = confirmed(&state, &form, page).await {
        return r;
    }
    enqueue(&state, &op, &AdminCommand::MemberRemove { did: form.did.clone() }).await
}

/// `POST /admin/members/deny` (typed: the account's handle).
pub async fn deny(
    State(state): State<AdminState>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    Form(form): Form<DidForm>,
) -> Response {
    let op = match require(&state, &headers, peer, Need::Change, Some(&form.csrf)).await {
        Ok(op) => op,
        Err(response) => return response,
    };
    if let Some(r) = invalid_did(&state, &op, &form.did) {
        return r;
    }
    let reason = form.reason.as_deref().map(str::trim).filter(|r| !r.is_empty()).map(str::to_string);
    let mut hidden = vec![Hidden { name: "did".to_string(), value: form.did.clone() }];
    if let Some(r) = &reason {
        hidden.push(Hidden { name: "reason".to_string(), value: r.clone() });
    }
    let page = ConfirmPage {
        css: css(),
        nav: Some(Nav::new(&op, "members")),
        title: "Deny an account".to_string(),
        what: format!("{} can never join by itself, and cannot be added until the denial is lifted. If they are a member now, they are removed and what Prism holds for them as a member is deleted.", form.did),
        undo: "Lifting the denial later does not make them a member again; they must be added.".to_string(),
        action: "/admin/members/deny".to_string(),
        csrf: op.csrf.clone(),
        hidden,
        typed: None,
        button: "Deny".to_string(),
        error: None,
    };
    if let Err(r) = confirmed(&state, &form, page).await {
        return r;
    }
    enqueue(&state, &op, &AdminCommand::MemberDeny { did: form.did.clone(), reason }).await
}

/// `POST /admin/members/allow`: lifts a denial; adds nothing.
pub async fn allow(
    State(state): State<AdminState>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    Form(form): Form<DidForm>,
) -> Response {
    let op = match require(&state, &headers, peer, Need::Change, Some(&form.csrf)).await {
        Ok(op) => op,
        Err(response) => return response,
    };
    if let Some(r) = invalid_did(&state, &op, &form.did) {
        return r;
    }
    enqueue(&state, &op, &AdminCommand::MemberAllow { did: form.did.clone() }).await
}
