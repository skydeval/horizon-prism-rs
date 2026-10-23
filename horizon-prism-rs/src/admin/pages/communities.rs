//! Communities (`/admin/communities`, refract design §9.1): the accounts
//! Prism keeps serving because a listed community still hosts them after
//! Bluesky took them down.
//!
//! - The feature switch `refract.enabled`, and each community with its
//!   fields and the counts of its accounts by state.
//! - Add, edit, enable, disable and remove a community. Every change saves
//!   `refract.communities` as one whole value through `settings set`, so
//!   the executor validates the whole configuration and the settings
//!   history records it; the Refract config task applies it.
//! - A change that would make restored accounts pending removal (the
//!   switch off, a community disabled, removed, or given other servers)
//!   says how many first and asks for confirmation.
//! - Pending ends with Keep and Unkeep (`refract keep`/`unkeep`).

use std::collections::HashMap;
use std::net::SocketAddr;

use askama::Template;
use axum::Form;
use axum::extract::{ConnectInfo, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::Response;
use chrono::{DateTime, Utc};
use serde::Deserialize;
use serde_json::Value;

use super::{Confirm, Nav, confirmed, css, enqueue, hidden_field, message, render, when};
use crate::admin::AdminState;
use crate::admin::auth::{Need, Operator, require};
use crate::admin::settings::store::Change;
use crate::admin_executor::AdminCommand;
use crate::config::{LabelSource, RefractCommunity, default_gone_labels};

const ENABLED: &str = "refract.enabled";
const LIST: &str = "refract.communities";
const STATES: [&str; 5] = ["candidate", "rejected", "restored", "grace", "ending"];

/// One community as the page shows and edits it.
pub struct CommunityView {
    pub name: String,
    pub tier: u8,
    pub patterns: String,
    pub appview: String,
    pub labeler: String,
    pub source: String,
    pub gone: String,
    pub enabled: bool,
    /// Accounts by state, in [`STATES`]' order.
    pub counts: Vec<i64>,
}

/// Accounts of a community no longer listed.
pub struct Unlisted {
    pub name: String,
    pub count: i64,
}

/// A pending end (§4.6).
pub struct PendingView {
    pub scope: String,
    pub since: String,
    pub ends: String,
    pub keep: bool,
    pub dids: i64,
}

#[derive(Template)]
#[template(path = "communities.html")]
struct CommunitiesPage {
    css: &'static str,
    nav: Option<Nav>,
    title: String,
    csrf: String,
    enabled: bool,
    states: Vec<&'static str>,
    communities: Vec<CommunityView>,
    pending: Vec<PendingView>,
    unlisted: Vec<Unlisted>,
}

/// The value `serve` runs with, and the saved row's version.
fn running(state: &AdminState) -> (bool, Vec<RefractCommunity>) {
    let live = state.settings.live.load();
    (live.refract_enabled, live.refract_communities.clone())
}

async fn version(state: &AdminState, key: &str) -> Result<i64, sqlx::Error> {
    crate::cli::commands::settings::version(&state.pool, key).await
}

/// Accounts by community and state.
async fn counts(state: &AdminState) -> Result<HashMap<(String, String), i64>, sqlx::Error> {
    let rows: Vec<(String, String, i64)> = sqlx::query_as("SELECT community, state, count(*) FROM refract_dids GROUP BY community, state").fetch_all(&state.pool).await?;
    Ok(rows.into_iter().map(|(c, s, n)| ((c, s), n)).collect())
}

/// Restored accounts (`restored` or `grace`), of `community` or of all.
async fn restored(state: &AdminState, community: Option<&str>) -> Result<i64, sqlx::Error> {
    sqlx::query_scalar("SELECT count(*) FROM refract_dids WHERE state IN ('restored', 'grace') AND ($1::text IS NULL OR community = $1)")
        .bind(community)
        .fetch_one(&state.pool)
        .await
}

fn view(c: &RefractCommunity, counts: &HashMap<(String, String), i64>) -> CommunityView {
    CommunityView {
        name: c.name.clone(),
        tier: c.tier,
        patterns: c.pds_patterns.join(", "),
        appview: c.appview_network.clone().unwrap_or_default(),
        labeler: c.labeler_did.clone().unwrap_or_default(),
        source: c.label_source.map(LabelSource::as_str).unwrap_or_default().to_string(),
        gone: c.gone_labels.join(", "),
        enabled: c.enabled,
        counts: STATES.iter().map(|s| counts.get(&(c.name.clone(), (*s).to_string())).copied().unwrap_or(0)).collect(),
    }
}

/// `GET /admin/communities`.
pub async fn page(State(state): State<AdminState>, ConnectInfo(peer): ConnectInfo<SocketAddr>, headers: HeaderMap) -> Response {
    let op = match require(&state, &headers, peer, Need::Page, None).await {
        Ok(op) => op,
        Err(response) => return response,
    };
    let (enabled, list) = running(&state);
    let counts = match counts(&state).await {
        Ok(c) => c,
        Err(e) => return super::internal_error(&state, &e),
    };
    let pending: Vec<(String, DateTime<Utc>, bool, i64)> = match sqlx::query_as(
        "SELECT p.scope, p.first_seen, p.keep, (SELECT count(*) FROM refract_pending_dids d WHERE d.scope = p.scope) FROM refract_pending p ORDER BY p.scope",
    )
    .fetch_all(&state.pool)
    .await
    {
        Ok(p) => p,
        Err(e) => return super::internal_error(&state, &e),
    };
    let delay = chrono::TimeDelta::hours(i64::from(state.settings.running.refract.disable_delay_hours));
    let mut unlisted: Vec<Unlisted> = Vec::new();
    for ((community, _), n) in &counts {
        if !list.iter().any(|c| &c.name == community) {
            match unlisted.iter_mut().find(|u| &u.name == community) {
                Some(u) => u.count += n,
                None => unlisted.push(Unlisted { name: community.clone(), count: *n }),
            }
        }
    }
    unlisted.sort_by(|a, b| a.name.cmp(&b.name));
    render(
        StatusCode::OK,
        &CommunitiesPage {
            css: css(),
            nav: Some(Nav::new(&op, "communities")),
            title: "Communities".to_string(),
            csrf: op.csrf.clone(),
            enabled,
            states: STATES.to_vec(),
            communities: list.iter().map(|c| view(c, &counts)).collect(),
            pending: pending
                .into_iter()
                .map(|(scope, since, keep, dids)| PendingView {
                    ends: if keep { "kept".to_string() } else { when(since + delay) },
                    since: when(since),
                    scope,
                    keep,
                    dids,
                })
                .collect(),
            unlisted,
        },
    )
}

fn refuse(state: &AdminState, op: &Operator, text: &str) -> Response {
    message(state, Some(Nav::new(op, "communities")), StatusCode::BAD_REQUEST, "Not saved", text, Some(("/admin/communities", "Communities")))
}

/// Save the whole list (and nothing else) as one `settings set`.
async fn save_list(state: &AdminState, op: &Operator, list: &[RefractCommunity]) -> Response {
    let value = match serde_json::to_value(list) {
        Ok(v) => v,
        Err(e) => return refuse(state, op, &e.to_string()),
    };
    let base_version = match version(state, LIST).await {
        Ok(v) => v,
        Err(e) => return super::internal_error(state, &e),
    };
    enqueue(state, op, &AdminCommand::SettingsSet { changes: vec![Change { key: LIST.to_string(), value: Some(value), base_version }] }).await
}

/// What a change would make pending, for its confirmation.
fn pending_words(n: i64, delay: u32) -> String {
    format!(
        "{n} restored account{} will be hidden at once and ended after {delay} hours, unless you keep them on this page or undo the change first.",
        if n == 1 { "" } else { "s" }
    )
}

/// The feature switch's form.
#[derive(Debug, Default, Deserialize)]
pub struct SwitchForm {
    #[serde(default)]
    pub csrf: String,
    #[serde(default)]
    pub enabled: String,
    pub confirm: Option<String>,
}

/// `POST /admin/communities/switch`: `refract.enabled`. Turning it off
/// with restored accounts asks first.
pub async fn switch(State(state): State<AdminState>, ConnectInfo(peer): ConnectInfo<SocketAddr>, headers: HeaderMap, Form(form): Form<SwitchForm>) -> Response {
    let op = match require(&state, &headers, peer, Need::Change, Some(&form.csrf)).await {
        Ok(op) => op,
        Err(response) => return response,
    };
    let on = form.enabled == "true";
    if !on {
        let n = match restored(&state, None).await {
            Ok(n) => n,
            Err(e) => return super::internal_error(&state, &e),
        };
        if n > 0 {
            let page = Confirm {
                title: "Turn off",
                what: pending_words(n, state.settings.running.refract.disable_delay_hours),
                undo: "Turning it back on before then keeps them.",
                action: "/admin/communities/switch",
                hidden: vec![hidden_field("enabled", "false")],
                typed: None,
                button: "Turn off",
            }
            .page(&op, "communities");
            if let Some(response) = confirmed(page, form.confirm.as_deref(), None) {
                return response;
            }
        }
    }
    let base_version = match version(&state, ENABLED).await {
        Ok(v) => v,
        Err(e) => return super::internal_error(&state, &e),
    };
    enqueue(&state, &op, &AdminCommand::SettingsSet { changes: vec![Change { key: ENABLED.to_string(), value: Some(Value::Bool(on)), base_version }] }).await
}

/// A community's form: add (no `original`) or edit.
#[derive(Debug, Default, Deserialize)]
pub struct CommunityForm {
    #[serde(default)]
    pub csrf: String,
    /// The name it had, for an edit.
    #[serde(default)]
    pub original: String,
    #[serde(default)]
    pub name: String,
    #[serde(default)]
    pub tier: String,
    #[serde(default)]
    pub pds_patterns: String,
    #[serde(default)]
    pub appview_network: String,
    #[serde(default)]
    pub labeler_did: String,
    #[serde(default)]
    pub label_source: String,
    #[serde(default)]
    pub gone_labels: String,
    pub enabled: Option<String>,
    pub confirm: Option<String>,
}

fn words_list(text: &str) -> Vec<String> {
    text.split([',', '\n']).map(str::trim).filter(|s| !s.is_empty()).map(str::to_string).collect()
}

fn blank(s: &str) -> Option<String> {
    let s = s.trim();
    (!s.is_empty()).then(|| s.to_string())
}

/// The entry the form describes.
///
/// # Errors
///
/// A plain-words problem.
pub fn entry(form: &CommunityForm) -> Result<RefractCommunity, String> {
    let name = form.name.trim().to_string();
    if name.is_empty() {
        return Err("Give the community a name.".to_string());
    }
    let tier: u8 = form.tier.trim().parse().map_err(|_| "Tier is 1 or 2.".to_string())?;
    let label_source = match form.label_source.trim() {
        "" => None,
        s => Some(serde_json::from_value::<LabelSource>(Value::String(s.to_string())).map_err(|_| "Label source is labeler or appview.".to_string())?),
    };
    let gone = words_list(&form.gone_labels);
    Ok(RefractCommunity {
        name,
        pds_patterns: words_list(&form.pds_patterns),
        tier,
        appview_network: blank(&form.appview_network),
        labeler_did: blank(&form.labeler_did),
        label_source,
        gone_labels: if gone.is_empty() { default_gone_labels() } else { gone },
        enabled: form.enabled.is_some(),
    })
}

/// Whether replacing `old` with `new` can make its accounts pending: it is
/// disabled, or its servers change.
#[must_use]
pub fn may_make_pending(old: &RefractCommunity, new: &RefractCommunity) -> bool {
    (old.enabled && !new.enabled) || old.pds_patterns != new.pds_patterns
}

/// Ask first when `n` restored accounts of `name` would become pending.
async fn ask(state: &AdminState, op: &Operator, name: &str, title: &'static str, action: &'static str, hidden: Vec<super::Hidden>, confirm: Option<&str>) -> Option<Response> {
    let n = match restored(state, Some(name)).await {
        Ok(n) => n,
        Err(e) => return Some(super::internal_error(state, &e)),
    };
    if n == 0 {
        return None;
    }
    let page = Confirm {
        title,
        what: pending_words(n, state.settings.running.refract.disable_delay_hours),
        undo: "Undoing the change before then keeps them.",
        action,
        hidden,
        typed: None,
        button: title,
    }
    .page(op, "communities");
    confirmed(page, confirm, None)
}

fn form_fields(form: &CommunityForm) -> Vec<super::Hidden> {
    let mut out = vec![
        hidden_field("original", &form.original),
        hidden_field("name", &form.name),
        hidden_field("tier", &form.tier),
        hidden_field("pds_patterns", &form.pds_patterns),
        hidden_field("appview_network", &form.appview_network),
        hidden_field("labeler_did", &form.labeler_did),
        hidden_field("label_source", &form.label_source),
        hidden_field("gone_labels", &form.gone_labels),
    ];
    if form.enabled.is_some() {
        out.push(hidden_field("enabled", "on"));
    }
    out
}

/// `POST /admin/communities/save`: add or edit one community.
pub async fn save(State(state): State<AdminState>, ConnectInfo(peer): ConnectInfo<SocketAddr>, headers: HeaderMap, Form(form): Form<CommunityForm>) -> Response {
    let op = match require(&state, &headers, peer, Need::Change, Some(&form.csrf)).await {
        Ok(op) => op,
        Err(response) => return response,
    };
    let new = match entry(&form) {
        Ok(e) => e,
        Err(problem) => return refuse(&state, &op, &problem),
    };
    let (_, mut list) = running(&state);
    let original = form.original.trim();
    if list.iter().any(|c| c.name == new.name && c.name != original) {
        return refuse(&state, &op, &format!("A community named {} is already listed.", new.name));
    }
    if original.is_empty() {
        list.push(new);
    } else {
        let Some(at) = list.iter().position(|c| c.name == original) else {
            return refuse(&state, &op, "That community is no longer listed; reload the page.");
        };
        if may_make_pending(&list[at], &new)
            && let Some(response) = ask(&state, &op, original, "Save", "/admin/communities/save", form_fields(&form), form.confirm.as_deref()).await
        {
            return response;
        }
        list[at] = new;
    }
    save_list(&state, &op, &list).await
}

/// A one-community action's form.
#[derive(Debug, Default, Deserialize)]
pub struct NameForm {
    #[serde(default)]
    pub csrf: String,
    #[serde(default)]
    pub name: String,
    pub confirm: Option<String>,
}

/// `POST /admin/communities/toggle`: enable or disable one community.
pub async fn toggle(State(state): State<AdminState>, ConnectInfo(peer): ConnectInfo<SocketAddr>, headers: HeaderMap, Form(form): Form<NameForm>) -> Response {
    let op = match require(&state, &headers, peer, Need::Change, Some(&form.csrf)).await {
        Ok(op) => op,
        Err(response) => return response,
    };
    let (_, mut list) = running(&state);
    let Some(at) = list.iter().position(|c| c.name == form.name) else {
        return refuse(&state, &op, "That community is no longer listed; reload the page.");
    };
    if list[at].enabled
        && let Some(response) = ask(&state, &op, &form.name, "Disable", "/admin/communities/toggle", vec![hidden_field("name", &form.name)], form.confirm.as_deref()).await
    {
        return response;
    }
    list[at].enabled = !list[at].enabled;
    save_list(&state, &op, &list).await
}

/// `POST /admin/communities/remove`: take one community off the list.
pub async fn remove(State(state): State<AdminState>, ConnectInfo(peer): ConnectInfo<SocketAddr>, headers: HeaderMap, Form(form): Form<NameForm>) -> Response {
    let op = match require(&state, &headers, peer, Need::Change, Some(&form.csrf)).await {
        Ok(op) => op,
        Err(response) => return response,
    };
    let (_, mut list) = running(&state);
    let Some(at) = list.iter().position(|c| c.name == form.name) else {
        return refuse(&state, &op, "That community is no longer listed; reload the page.");
    };
    if let Some(response) = ask(&state, &op, &form.name, "Remove", "/admin/communities/remove", vec![hidden_field("name", &form.name)], form.confirm.as_deref()).await {
        return response;
    }
    list.remove(at);
    save_list(&state, &op, &list).await
}

/// A pending end's form.
#[derive(Debug, Default, Deserialize)]
pub struct KeepForm {
    #[serde(default)]
    pub csrf: String,
    #[serde(default)]
    pub scope: String,
    #[serde(default)]
    pub keep: String,
}

/// `POST /admin/communities/keep`: keep or release a pending end.
pub async fn keep(State(state): State<AdminState>, ConnectInfo(peer): ConnectInfo<SocketAddr>, headers: HeaderMap, Form(form): Form<KeepForm>) -> Response {
    let op = match require(&state, &headers, peer, Need::Change, Some(&form.csrf)).await {
        Ok(op) => op,
        Err(response) => return response,
    };
    enqueue(&state, &op, &AdminCommand::RefractKeep { scope: form.scope.trim().to_string(), keep: form.keep == "true" }).await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_form_reads_as_an_entry() {
        let form = CommunityForm {
            name: " sky ".into(),
            tier: "2".into(),
            pds_patterns: "*.sky.example, sky.example\n".into(),
            labeler_did: "did:plc:labeler".into(),
            label_source: "labeler".into(),
            enabled: Some("on".into()),
            ..CommunityForm::default()
        };
        let e = entry(&form).expect("entry");
        assert_eq!(e.name, "sky");
        assert_eq!(e.pds_patterns, ["*.sky.example", "sky.example"]);
        assert_eq!(e.label_source, Some(LabelSource::Labeler));
        assert_eq!(e.gone_labels, default_gone_labels());
        assert!(e.enabled && e.appview_network.is_none());
        assert!(entry(&CommunityForm { tier: "x".into(), ..form }).is_err());
        let mut narrower = e.clone();
        narrower.pds_patterns.pop();
        assert!(may_make_pending(&e, &narrower));
        let mut renamed = e.clone();
        renamed.name = "other".into();
        assert!(!may_make_pending(&e, &renamed));
        let mut off = e.clone();
        off.enabled = false;
        assert!(may_make_pending(&e, &off) && !may_make_pending(&off, &e));
    }
}
