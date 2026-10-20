//! Labels (`/admin/labels`, admin-ui design §6.9): the labels this Prism
//! applies, filterable by account or post and by label; add a label (the
//! six values of `LABEL_VALUES`, each explained) and remove one. Adding
//! `!takedown` and removing any label are typed (the account's handle),
//! since a removal deletes a stored row.
//!
//! An account or post is typed as a handle, a DID, an `at://` address or a
//! post's link (`https://<app>/profile/<handle-or-did>/post/<id>`). A link
//! is only read for its path; Prism never fetches it (§5.10). A handle is
//! resolved both ways through the identity resolver.

use std::net::SocketAddr;

use askama::Template;
use axum::Form;
use axum::extract::{ConnectInfo, Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::Response;
use chrono::{DateTime, Utc};
use serde::Deserialize;

use super::{Confirm, Nav, confirmed, css, enqueue, hidden_field, message, render, typed_word, when};
use crate::admin::AdminState;
use crate::admin::auth::{Need, require};
use crate::admin_executor::AdminCommand;
use crate::admin_executor::commands::LABEL_VALUES;
use crate::db::locks::uri_author;

/// Labels per page.
pub const PAGE: i64 = 100;

/// Each label value: its name and what Prism does with it.
pub const MEANINGS: [(&str, &str, &str); 6] = [
    ("!takedown", "Take down", "Prism treats it as if it did not exist, everywhere. On an account, that includes all of its posts."),
    ("!hide", "Hide", "A post is left out of feeds and threads. Opening it directly still shows it, with the label."),
    ("porn", "Porn", "Apps put it behind a warning or hide it, following each person's own content settings."),
    ("sexual", "Sexually suggestive", "Apps put it behind a warning or hide it, following each person's own content settings."),
    ("nudity", "Nudity (not sexual)", "Apps put it behind a warning or hide it, following each person's own content settings."),
    ("graphic-media", "Graphic (violence or gore)", "Apps put it behind a warning or hide it, following each person's own content settings."),
];

/// A label value's name in words.
#[must_use]
pub fn label_name(val: &str) -> &str {
    MEANINGS.iter().find(|(v, _, _)| *v == val).map_or(val, |(_, name, _)| name)
}

/// What an operator typed, as Prism reads it: an account, or a record of
/// one.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Subject {
    pub did: String,
    /// The record's `at://` address, for a post or another record.
    pub uri: Option<String>,
}

impl Subject {
    /// The subject a label is stored under.
    #[must_use]
    pub fn key(&self) -> &str {
        self.uri.as_deref().unwrap_or(&self.did)
    }
}

fn is_did(s: &str) -> bool {
    proto_blue::syntax::Did::is_valid(s)
}

/// A DID or a handle (resolved both ways) to its DID.
async fn actor(state: &AdminState, actor: &str) -> Result<String, String> {
    super::members::account(state, actor).await
}

/// Whether a record key or collection segment is plain enough to store.
fn segment_ok(s: &str) -> bool {
    !s.is_empty() && s.len() <= 512 && s.bytes().all(|b| b.is_ascii_alphanumeric() || b"._:~-".contains(&b))
}

/// What the operator typed, read (§6.9, §6.10). A handle is resolved; a
/// link is read for its path only.
///
/// # Errors
///
/// A plain-words problem.
pub async fn read_subject(state: &AdminState, input: &str) -> Result<Subject, String> {
    let input = input.trim();
    if input.is_empty() {
        return Err("Enter a handle, a DID, or a post's link.".to_string());
    }
    if let Some(rest) = input.strip_prefix("at://") {
        let mut parts = rest.split('/');
        let who = parts.next().unwrap_or_default();
        let did = actor(state, who).await?;
        return match (parts.next(), parts.next(), parts.next()) {
            (None | Some(""), None, None) => Ok(Subject { did, uri: None }),
            (Some(collection), Some(rkey), None) if segment_ok(collection) && segment_ok(rkey) => {
                Ok(Subject { uri: Some(format!("at://{did}/{collection}/{rkey}")), did })
            }
            _ => Err("That at:// address is not an account's or a record's.".to_string()),
        };
    }
    if input.starts_with("https://") || input.starts_with("http://") {
        let unreadable = || "Prism cannot read that link. Paste a post's link (…/profile/…/post/…), its at:// address, a handle or a DID.".to_string();
        let url = url::Url::parse(input).map_err(|_| unreadable())?;
        let path: Vec<&str> = url.path().split('/').filter(|s| !s.is_empty()).collect();
        return match path.as_slice() {
            ["profile", who] => Ok(Subject { did: actor(state, who).await?, uri: None }),
            ["profile", who, "post", rkey] if segment_ok(rkey) => {
                let did = actor(state, who).await?;
                Ok(Subject { uri: Some(format!("at://{did}/app.bsky.feed.post/{rkey}")), did })
            }
            _ => Err(unreadable()),
        };
    }
    Ok(Subject { did: actor(state, input).await?, uri: None })
}

/// A stored subject (a DID, or an at-uri whose author is a DID), as a
/// confirmation sends it back; nothing is resolved.
fn stored_subject(subject: &str) -> Option<Subject> {
    let subject = subject.trim();
    if is_did(subject) {
        return Some(Subject { did: subject.to_string(), uri: None });
    }
    let did = uri_author(subject).filter(|d| is_did(d))?;
    Some(Subject { did: did.to_string(), uri: Some(subject.to_string()) })
}

/// A stored label as the page lists it.
pub struct LabelRow {
    pub subject: String,
    /// "account", "post" or "record".
    pub kind: String,
    /// The account's stored handle, else its DID.
    pub who: String,
    pub val: String,
    pub name: String,
    pub when: String,
}

/// What kind of thing a subject is, in a word.
#[must_use]
pub fn kind(subject: &str) -> &'static str {
    if !subject.starts_with("at://") {
        "account"
    } else if subject.split('/').nth(3) == Some("app.bsky.feed.post") {
        "post"
    } else {
        "record"
    }
}

type LabelTuple = (String, String, DateTime<Utc>, Option<String>);

/// Labels on `did` and its records (or on one `uri`), newest first.
///
/// # Errors
///
/// Database errors.
pub async fn labels_of(state: &AdminState, filter: Option<&Subject>, val: Option<&str>, limit: i64, offset: i64) -> Result<Vec<LabelRow>, sqlx::Error> {
    let (did, uri) = match filter {
        Some(s) => (Some(s.did.clone()), s.uri.clone()),
        None => (None, None),
    };
    let rows: Vec<LabelTuple> = sqlx::query_as(
        "SELECT l.subject, l.val, l.created_at, a.handle FROM labels l \
         LEFT JOIN actors a ON a.did = CASE WHEN l.subject LIKE 'at://%' THEN split_part(substr(l.subject, 6), '/', 1) ELSE l.subject END \
         WHERE ($1::text IS NULL OR ($2::text IS NULL AND (l.subject = $1 OR left(l.subject, length($1) + 6) = 'at://' || $1 || '/')) OR l.subject = $2) \
           AND ($3::text IS NULL OR l.val = $3) \
         ORDER BY l.created_at DESC, l.id DESC LIMIT $4 OFFSET $5",
    )
    .bind(did)
    .bind(uri)
    .bind(val)
    .bind(limit)
    .bind(offset)
    .fetch_all(&state.pool)
    .await?;
    Ok(rows
        .into_iter()
        .map(|(subject, val, at, handle)| {
            let author = uri_author(&subject).unwrap_or(&subject).to_string();
            LabelRow {
                kind: kind(&subject).to_string(),
                who: handle.unwrap_or(author),
                name: label_name(&val).to_string(),
                when: when(at),
                subject,
                val,
            }
        })
        .collect())
}

/// A label value offered in the form.
pub struct Choice {
    pub val: &'static str,
    pub name: &'static str,
    pub meaning: &'static str,
    pub selected: bool,
}

/// The choices, with `selected` marked.
#[must_use]
pub fn choices(selected: &str) -> Vec<Choice> {
    MEANINGS.iter().map(|(val, name, meaning)| Choice { val, name, meaning, selected: *val == selected }).collect()
}

#[derive(Template)]
#[template(path = "labels.html")]
struct LabelsPage {
    css: &'static str,
    nav: Option<Nav>,
    title: String,
    csrf: String,
    q: String,
    val: String,
    problem: Option<String>,
    rows: Vec<LabelRow>,
    filters: Vec<Choice>,
    choices: Vec<Choice>,
    /// The add form's account or post, prefilled from the filter.
    subject: String,
    page: i64,
    prev: Option<String>,
    next: Option<String>,
}

#[derive(Debug, Default, Deserialize)]
pub struct LabelsQuery {
    #[serde(default)]
    pub q: String,
    #[serde(default)]
    pub val: String,
    pub page: Option<i64>,
}

fn page_link(q: &str, val: &str, page: i64) -> String {
    let query: String = url::form_urlencoded::Serializer::new(String::new())
        .append_pair("q", q)
        .append_pair("val", val)
        .append_pair("page", &page.to_string())
        .finish();
    format!("/admin/labels?{query}")
}

/// `GET /admin/labels`.
pub async fn page(State(state): State<AdminState>, ConnectInfo(peer): ConnectInfo<SocketAddr>, headers: HeaderMap, Query(q): Query<LabelsQuery>) -> Response {
    let op = match require(&state, &headers, peer, Need::Page, None).await {
        Ok(op) => op,
        Err(response) => return response,
    };
    let page = q.page.unwrap_or(0).max(0);
    let val = LABEL_VALUES.iter().find(|v| **v == q.val.trim()).copied();
    let text = q.q.trim().to_string();
    let (filter, problem) = if text.is_empty() {
        (None, None)
    } else {
        match read_subject(&state, &text).await {
            Ok(s) => (Some(s), None),
            Err(problem) => (None, Some(problem)),
        }
    };
    let mut rows = if problem.is_some() {
        Vec::new()
    } else {
        match labels_of(&state, filter.as_ref(), val, PAGE + 1, page * PAGE).await {
            Ok(rows) => rows,
            Err(e) => return super::internal_error(&state, &e),
        }
    };
    let has_next = rows.len() > usize::try_from(PAGE).unwrap_or(usize::MAX);
    rows.truncate(usize::try_from(PAGE).unwrap_or(usize::MAX));
    let val = val.unwrap_or_default();
    render(
        StatusCode::OK,
        &LabelsPage {
            css: css(),
            nav: Some(Nav::new(&op, "labels")),
            title: "Labels".to_string(),
            csrf: op.csrf.clone(),
            subject: filter.as_ref().map(|s| s.key().to_string()).unwrap_or_default(),
            q: text.clone(),
            val: val.to_string(),
            problem,
            rows,
            filters: choices(val),
            choices: choices(""),
            page,
            prev: (page > 0).then(|| page_link(&text, val, page - 1)),
            next: has_next.then(|| page_link(&text, val, page + 1)),
        },
    )
}

#[derive(Debug, Default, Deserialize)]
pub struct LabelForm {
    #[serde(default)]
    pub csrf: String,
    /// What the operator typed (add), or the stored subject (remove, and a
    /// confirmation's second step).
    #[serde(default)]
    pub subject: String,
    #[serde(default)]
    pub val: String,
    pub confirm: Option<String>,
    pub typed: Option<String>,
}

fn refuse(state: &AdminState, op: &crate::admin::auth::Operator, text: &str) -> Response {
    message(state, Some(Nav::new(op, "labels")), StatusCode::BAD_REQUEST, "Not done", text, Some(("/admin/labels", "Labels")))
}

/// What a label does to its subject, for a confirmation.
fn on_what(subject: &Subject) -> String {
    match &subject.uri {
        Some(uri) => format!("the {} {uri}", kind(uri)),
        None => format!("the account {}", subject.did),
    }
}

/// `POST /admin/labels/add`: a label on an account or a record. Adding
/// `!takedown` is typed.
pub async fn add(State(state): State<AdminState>, ConnectInfo(peer): ConnectInfo<SocketAddr>, headers: HeaderMap, Form(form): Form<LabelForm>) -> Response {
    let op = match require(&state, &headers, peer, Need::Change, Some(&form.csrf)).await {
        Ok(op) => op,
        Err(response) => return response,
    };
    let Some(val) = LABEL_VALUES.iter().find(|v| **v == form.val.trim()).copied() else {
        return refuse(&state, &op, "Choose one of the labels on the page.");
    };
    // The confirmation sends the stored form back; anything else is read.
    let subject = match stored_subject(&form.subject) {
        Some(s) => s,
        None => match read_subject(&state, &form.subject).await {
            Ok(s) => s,
            Err(problem) => return refuse(&state, &op, &problem),
        },
    };
    if val == "!takedown" {
        let word = match typed_word(&state, &subject.did).await {
            Ok(w) => w,
            Err(e) => return super::internal_error(&state, &e),
        };
        let page = Confirm {
            title: "Take down",
            what: format!(
                "Prism treats {} as if it did not exist, everywhere it answers apps{}. Nothing is deleted; removing the label brings it back.",
                on_what(&subject),
                if subject.uri.is_none() { ", with all of its posts" } else { "" }
            ),
            undo: "People stop seeing it at once.",
            action: "/admin/labels/add",
            hidden: vec![hidden_field("subject", subject.key()), hidden_field("val", val)],
            typed: Some(word),
            button: "Take down",
        }
        .page(&op, "labels");
        if let Some(response) = confirmed(page, form.confirm.as_deref(), form.typed.as_deref()) {
            return response;
        }
    }
    enqueue(&state, &op, &AdminCommand::LabelAdd { subject: subject.key().to_string(), val: val.to_string() }).await
}

/// `POST /admin/labels/remove` (typed: the account's handle).
pub async fn remove(State(state): State<AdminState>, ConnectInfo(peer): ConnectInfo<SocketAddr>, headers: HeaderMap, Form(form): Form<LabelForm>) -> Response {
    let op = match require(&state, &headers, peer, Need::Change, Some(&form.csrf)).await {
        Ok(op) => op,
        Err(response) => return response,
    };
    let (Some(subject), Some(val)) = (stored_subject(&form.subject), LABEL_VALUES.iter().find(|v| **v == form.val.trim()).copied()) else {
        return refuse(&state, &op, "That label cannot be removed from here.");
    };
    let word = match typed_word(&state, &subject.did).await {
        Ok(w) => w,
        Err(e) => return super::internal_error(&state, &e),
    };
    let page = Confirm {
        title: "Remove a label",
        what: format!("Prism deletes the label \"{}\" from {}.", label_name(val), on_what(&subject)),
        undo: "Apps see it without the label at once. Adding the label again puts it back.",
        action: "/admin/labels/remove",
        hidden: vec![hidden_field("subject", subject.key()), hidden_field("val", val)],
        typed: Some(word),
        button: "Remove label",
    }
    .page(&op, "labels");
    if let Some(response) = confirmed(page, form.confirm.as_deref(), form.typed.as_deref()) {
        return response;
    }
    enqueue(&state, &op, &AdminCommand::LabelRemove { subject: subject.key().to_string(), val: val.to_string() }).await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn subjects_and_kinds() {
        let did = "did:plc:aaaaaaaaaaaaaaaaaaaaaaaa";
        assert_eq!(stored_subject(did), Some(Subject { did: did.into(), uri: None }));
        let uri = format!("at://{did}/app.bsky.feed.post/3k");
        assert_eq!(stored_subject(&uri), Some(Subject { did: did.into(), uri: Some(uri.clone()) }));
        assert_eq!(stored_subject("alice.example.com"), None);
        assert_eq!(stored_subject("at://alice.example.com/app.bsky.feed.post/3k"), None);
        assert_eq!(kind(did), "account");
        assert_eq!(kind(&uri), "post");
        assert_eq!(kind(&format!("at://{did}/app.bsky.graph.list/3k")), "record");
        assert_eq!(label_name("graphic-media"), "Graphic (violence or gore)");
        assert!(LABEL_VALUES.iter().all(|v| MEANINGS.iter().any(|(m, _, _)| m == v)));
        assert!(segment_ok("3kabc") && !segment_ok("a/b") && !segment_ok(""));
    }
}
