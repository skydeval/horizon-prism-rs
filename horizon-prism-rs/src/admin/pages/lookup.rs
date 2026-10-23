//! Look up (`/admin/lookup`, admin-ui design §6.10): one account, by
//! handle, DID or post link. It shows the DID and handle (resolved both
//! ways, afresh), its server, whether it is a member, denied, followed by
//! Prism (and what holds it there, scope-recency design), ready and
//! receiving updates; its labels and jobs; the blocks Prism keeps for it
//! (only those with a member on one side, member-blocks-only design); its
//! private posts by media kind (through the private tier's own counts); and
//! links to the actions for it.
//!
//! Identity is read the way admin commands read it: the PLC directory (or
//! the account's own address for `did:web`) and Prism's own handle check.
//! Lodestar is never asked here, even when apps' lookups use it
//! (lodestar-reads design §2.2).

use std::net::SocketAddr;

use askama::Template;
use axum::extract::{ConnectInfo, Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::Response;
use chrono::{DateTime, Utc};
use serde::Deserialize;

use super::farsight::Fact;
use super::jobs::{STATES, depth_words};
use super::labels::{Choice, LabelRow, Subject, choices, labels_of, read_subject};
use super::members::farsight_flags;
use super::{Nav, css, render, when, when_opt};
use crate::admin::AdminState;
use crate::admin::auth::{Need, require};
use crate::admin::health::count;
use crate::admin::operators::endpoint_host;
use crate::aggregations::policy::{REASON_FOLLOWED, REASON_INTERACTED, REASON_MEMBER, REASON_SUBSCRIBED};

fn fact(label: &str, value: impl Into<String>) -> Fact {
    Fact { label: label.to_string(), value: value.into() }
}

/// What holds an account in the set Prism follows, from `scope_dids.reasons`
/// (scope-recency design: member, follow, interaction, subscription).
#[must_use]
pub fn holds(reasons: i16) -> Vec<&'static str> {
    [
        (REASON_MEMBER, "It is a member."),
        (REASON_FOLLOWED, "A member follows it."),
        (REASON_INTERACTED, "A member replied to, quoted, liked or reposted it lately."),
        (REASON_SUBSCRIBED, "A member follows it and is notified of its posts."),
    ]
    .into_iter()
    .filter(|(bit, _)| reasons & bit != 0)
    .map(|(_, words)| words)
    .collect()
}

/// An account status in words.
#[must_use]
pub fn status_words(status: &str) -> String {
    match status {
        "active" => "active".to_string(),
        "deactivated" => "switched off by its owner".to_string(),
        "takendown" => "taken down by its server".to_string(),
        "suspended" => "suspended by its server".to_string(),
        "deleted" => "deleted".to_string(),
        other => other.to_string(),
    }
}

pub struct JobRow {
    pub kind: String,
    pub state: String,
    pub attempts: i32,
    pub error: String,
    pub when: String,
}

/// Everything shown about one account.
pub struct Found {
    pub did: String,
    /// How the page names it: its verified handle, else its DID.
    pub name: String,
    /// The record looked up, when a post or record was given.
    pub record: Option<String>,
    pub record_facts: Vec<Fact>,
    pub identity: Vec<Fact>,
    pub source: String,
    pub membership: Vec<Fact>,
    pub member: bool,
    pub denied: bool,
    pub scope: Vec<Fact>,
    pub holds: Vec<&'static str>,
    pub in_scope: bool,
    pub blocks: Vec<Fact>,
    pub blocks_note: String,
    pub private: Vec<Fact>,
    pub labels: Vec<LabelRow>,
    pub jobs: Vec<JobRow>,
    pub farsight: bool,
    pub memory: bool,
    /// The add-a-label form's subject: the record, else the account.
    pub label_subject: String,
    pub choices: Vec<Choice>,
}

#[derive(Template)]
#[template(path = "lookup.html")]
struct LookupPage {
    css: &'static str,
    nav: Option<Nav>,
    title: String,
    csrf: String,
    q: String,
    problem: Option<String>,
    found: Option<Found>,
}

#[derive(Debug, Default, Deserialize)]
pub struct LookupQuery {
    #[serde(default)]
    pub q: String,
}

type ActorTuple = (Option<String>, Option<String>, String);
type MemberTuple = (String, DateTime<Utc>, Option<DateTime<Utc>>, Option<DateTime<Utc>>);
type DenialTuple = (Option<String>, String, DateTime<Utc>);
type ScopeTuple = (i16, DateTime<Utc>, DateTime<Utc>);
type SyncTuple = (Option<DateTime<Utc>>, Option<DateTime<Utc>>, Option<DateTime<Utc>>);
type JobTuple = (String, String, i32, Option<String>, DateTime<Utc>);

/// The rows Look up reads about one account.
struct Rows {
    actor: Option<ActorTuple>,
    member: Option<MemberTuple>,
    denial: Option<DenialTuple>,
    scope: Option<ScopeTuple>,
    sync: Option<SyncTuple>,
    jobs: Vec<JobTuple>,
    remembered: i64,
    labels: Vec<LabelRow>,
}

async fn read_rows(state: &AdminState, did: &str) -> Result<Rows, sqlx::Error> {
    let pool = &state.pool;
    let actor = sqlx::query_as("SELECT handle, pds_endpoint, account_status FROM actors WHERE did = $1").bind(did).fetch_optional(pool).await?;
    let member = sqlx::query_as("SELECT source, added_at, ready_at, first_ready_at FROM members WHERE did = $1").bind(did).fetch_optional(pool).await?;
    let denial = sqlx::query_as("SELECT reason, denied_by, denied_at FROM member_denials WHERE did = $1").bind(did).fetch_optional(pool).await?;
    let scope = sqlx::query_as("SELECT reasons, added_at, justified_at FROM scope_dids WHERE did = $1").bind(did).fetch_optional(pool).await?;
    let sync = sqlx::query_as("SELECT diverged_since, head_checked_at, last_resync_at FROM sync_state WHERE did = $1").bind(did).fetch_optional(pool).await?;
    let jobs = sqlx::query_as("SELECT depth, state, attempts, last_error, updated_at FROM backfill_jobs WHERE did = $1 ORDER BY updated_at DESC")
        .bind(did)
        .fetch_all(pool)
        .await?;
    let remembered = sqlx::query_scalar("SELECT count(*) FROM learned_inbound_blocks WHERE viewer_did = $1").bind(did).fetch_one(pool).await?;
    let labels = labels_of(state, Some(&Subject { did: did.to_string(), uri: None }), None, 50, 0).await?;
    Ok(Rows { actor, member, denial, scope, sync, jobs, remembered, labels })
}

/// The blocks Prism keeps with this account on one side, under a 2-second
/// statement timeout.
async fn block_counts(state: &AdminState, did: &str) -> Result<(i64, i64), sqlx::Error> {
    let mut tx = state.pool.begin().await?;
    sqlx::query("SET LOCAL statement_timeout = '2s'").execute(&mut *tx).await?;
    let counts = sqlx::query_as("SELECT (SELECT count(*) FROM blocks WHERE author_did = $1), (SELECT count(*) FROM blocks WHERE subject_did = $1)")
        .bind(did)
        .fetch_one(&mut *tx)
        .await?;
    tx.commit().await?;
    Ok(counts)
}

/// Where a DID's document comes from, in words.
fn source_words(did: &str, lodestar_on: bool) -> String {
    let from = if did.starts_with("did:web:") { "the account's own web address" } else { "the PLC directory" };
    let lodestar = if lodestar_on { " Lodestar, which apps' lookups ask first, is never asked here." } else { "" };
    format!("Looked up just now by Prism itself, from {from}; the handle is checked with Prism's own look at the handle's web address and DNS.{lodestar}")
}

/// The identity facts: afresh, both ways, and what Prism has stored.
async fn identity(state: &AdminState, did: &str, actor: Option<&ActorTuple>) -> (Vec<Fact>, String) {
    let mut facts = vec![fact("DID", did)];
    let mut name = did.to_string();
    match state.identity.resolve_fresh(did).await {
        Ok(info) => {
            let handle = match info.claimed_handle.as_deref() {
                Some(claimed) if state.identity.resolve_handle(claimed).await.as_deref() == Some(did) => {
                    name = claimed.to_string();
                    format!("{claimed} (checked both ways)")
                }
                Some(claimed) => format!("{claimed}, but it does not point back to this account"),
                None => "none".to_string(),
            };
            facts.push(fact("Handle", handle));
            let host = info.pds_endpoint.as_deref().and_then(endpoint_host).unwrap_or_else(|| "none named".to_string());
            facts.push(fact("Server (where the account lives)", host));
        }
        Err(e) => facts.push(fact("Handle and server", format!("could not be looked up just now ({e})"))),
    }
    match actor {
        Some((handle, endpoint, status)) => {
            facts.push(fact("Handle Prism has stored", handle.clone().unwrap_or_else(|| "none".to_string())));
            facts.push(fact("Server Prism has stored", endpoint.as_deref().and_then(endpoint_host).unwrap_or_else(|| "none".to_string())));
            facts.push(fact("Account", status_words(status)));
        }
        None => facts.push(fact("Stored by Prism", "nothing about this account yet")),
    }
    (facts, name)
}

/// `GET /admin/lookup`.
pub async fn page(State(state): State<AdminState>, ConnectInfo(peer): ConnectInfo<SocketAddr>, headers: HeaderMap, Query(query): Query<LookupQuery>) -> Response {
    let op = match require(&state, &headers, peer, Need::Page, None).await {
        Ok(op) => op,
        Err(response) => return response,
    };
    let q = query.q.trim().to_string();
    let mut page = LookupPage {
        css: css(),
        nav: Some(Nav::new(&op, "lookup")),
        title: "Look up".to_string(),
        csrf: op.csrf.clone(),
        q: q.clone(),
        problem: None,
        found: None,
    };
    if q.is_empty() {
        return render(StatusCode::OK, &page);
    }
    let subject = match read_subject(&state, &q).await {
        Ok(s) => s,
        Err(problem) => {
            page.problem = Some(problem);
            return render(StatusCode::OK, &page);
        }
    };
    let did = subject.did.clone();
    let rows = match read_rows(&state, &did).await {
        Ok(r) => r,
        Err(e) => return super::internal_error(&state, &e),
    };
    let (identity_facts, name) = identity(&state, &did, rows.actor.as_ref()).await;

    // Membership and readiness.
    let member = rows.member.is_some();
    let denied = rows.denial.is_some();
    let mut membership = Vec::new();
    match &rows.member {
        Some((source, added, ready, first_ready)) => {
            let how = if source == "admin" { "added by an operator" } else { "joined by itself (auto-join)" };
            membership.push(fact("Member", format!("yes, {how}, on {}", when(*added))));
            membership.push(fact("Ready (first sync done)", ready.map_or_else(|| "not yet".to_string(), |t| format!("yes, since {}", when(t)))));
            membership.push(fact("First ready", when_opt(*first_ready)));
        }
        None => membership.push(fact("Member", "no")),
    }
    if let Some((reason, by, at)) = &rows.denial {
        let by = if by == "cli" { "the command line".to_string() } else { by.clone() };
        let note = reason.as_deref().map(|r| format!(" Your note: {r}")).unwrap_or_default();
        membership.push(fact("Denied", format!("yes, by {by} on {}.{note}", when(*at))));
    }
    let snapshot = state.scope.read_guard().await;
    let (in_scope, tracked, owner) = (snapshot.in_scope(&did), snapshot.is_tracked(&did), snapshot.is_owner(&did));
    drop(snapshot);
    if tracked {
        let receiving = match rows.sync.as_ref().and_then(|(diverged, _, _)| *diverged) {
            Some(since) => format!("no, not since {}", when(since)),
            None => "yes".to_string(),
        };
        membership.push(fact("Receiving its updates", receiving));
        if let Some((_, checked, resync)) = &rows.sync {
            membership.push(fact("Last checked against its server", when_opt(*checked)));
            membership.push(fact("Last full re-sync", when_opt(*resync)));
        }
    }
    membership.push(fact("Has private posts here", if owner { "yes" } else { "no" }));
    if member {
        membership.push(fact("Blocks remembered from Farsight", count(rows.remembered as f64)));
    }

    // The set Prism follows, and what holds the account in it.
    let mut scope = vec![fact("Prism follows this account", if in_scope { "yes" } else { "no" })];
    let mut hold_words = Vec::new();
    if let Some((reasons, added, justified)) = rows.scope {
        scope.push(fact("Following since", when(added)));
        scope.push(fact("Last found needed", when(justified)));
        hold_words = holds(reasons);
    }

    // Blocks, under the member-side rule, and the private records.
    let blocks_note = if member {
        "Prism keeps a block only when a member is on one side of it. This account is a member, so all its blocks and every block of it are kept.".to_string()
    } else {
        "Prism keeps a block only when a member is on one side of it. For this account, that is only its blocks of members and members' blocks of it.".to_string()
    };
    let blocks = match block_counts(&state, &did).await {
        Ok((made, of)) => vec![fact("Blocks it made", count(made as f64)), fact("Blocks of it", count(of as f64))],
        Err(error) => {
            tracing::warn!(event = "admin_lookup_counts_failed", %error, "a look-up's block counts did not finish");
            vec![fact("Blocks", "unknown (the count took too long)")]
        }
    };
    let private = match crate::private_tier::stats::account_counts(&state.pool, &did).await {
        Ok(c) => vec![
            fact("Private posts stored", count(c.posts as f64)),
            fact("With images", count(c.with_images as f64)),
            fact("With a video", count(c.with_video as f64)),
        ],
        Err(error) => {
            tracing::warn!(event = "admin_lookup_counts_failed", %error, "a look-up's private counts did not finish");
            vec![fact("Private posts", "unknown (the count took too long)")]
        }
    };

    // The record, when one was given.
    let mut record_facts = Vec::new();
    if let Some(uri) = &subject.uri {
        if super::labels::kind(uri) == "post" {
            let stored: Result<bool, sqlx::Error> = sqlx::query_scalar("SELECT EXISTS (SELECT 1 FROM posts WHERE uri = $1)").bind(uri).fetch_one(&state.pool).await;
            match stored {
                Ok(s) => record_facts.push(fact("Prism holds this post", if s { "yes" } else { "no" })),
                Err(e) => return super::internal_error(&state, &e),
            }
        }
        let on: Vec<String> = rows.labels.iter().filter(|l| &l.subject == uri).map(|l| l.name.clone()).collect();
        record_facts.push(fact("Its labels", if on.is_empty() { "none".to_string() } else { on.join(", ") }));
    }

    let jobs = rows
        .jobs
        .into_iter()
        .map(|(depth, job_state, attempts, error, at)| JobRow {
            kind: depth_words(&depth).to_string(),
            state: STATES.iter().find(|(s, _)| *s == job_state).map_or(job_state.clone(), |(_, w)| (*w).to_string()),
            attempts,
            error: error.unwrap_or_default().chars().take(300).collect(),
            when: when(at),
        })
        .collect();
    let (farsight, memory) = farsight_flags(&state);
    page.found = Some(Found {
        label_subject: subject.key().to_string(),
        record: subject.uri.clone(),
        record_facts,
        identity: identity_facts,
        source: source_words(&did, state.settings.lodestar.is_some()),
        membership,
        member,
        denied,
        scope,
        holds: hold_words,
        in_scope,
        blocks,
        blocks_note,
        private,
        labels: rows.labels,
        jobs,
        farsight,
        memory,
        choices: choices(""),
        name,
        did,
    });
    render(StatusCode::OK, &page)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn holds_and_words() {
        assert_eq!(holds(0), Vec::<&str>::new());
        assert_eq!(holds(REASON_FOLLOWED | REASON_SUBSCRIBED), ["A member follows it.", "A member follows it and is notified of its posts."]);
        assert_eq!(holds(REASON_MEMBER).len(), 1);
        assert_eq!(status_words("takendown"), "taken down by its server");
        assert!(source_words("did:web:example.com", false).contains("own web address"));
        assert!(source_words("did:plc:x", true).contains("Lodestar"));
        assert!(!source_words("did:plc:x", false).contains("Lodestar"));
    }
}
