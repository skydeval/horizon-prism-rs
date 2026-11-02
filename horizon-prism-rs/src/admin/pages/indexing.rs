//! Indexing (`/admin/indexing`): what Prism indexes, and its progress. The
//! switch between Kryphocron only and Community has its own form and
//! command, which moves through phases; turning public data off is typed
//! (`OFF`). While a refill runs, how many accounts are left and when the
//! longest wait ends. Then its setting, the longest wait.

use std::net::SocketAddr;

use askama::Template;
use axum::Form;
use axum::extract::{ConnectInfo, Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::Response;
use chrono::{DateTime, TimeDelta, Utc};
use serde_json::Value;

use super::health::Figure;
use super::{ConfirmPage, Hidden, Nav, ago, back_to, css, enqueue, message, render, typed_matches, when};
use crate::admin::AdminState;
use crate::admin::auth::{Need, require};
use crate::admin::health::count;
use crate::admin::menu::PageId;
use crate::admin::pages::settings::OpenQuery;
use crate::admin::settings::store::PUBLIC_SCOPE_KEY;
use crate::admin::settings::view::{self, Place, SettingsBlock};
use crate::admin_executor::AdminCommand;
use crate::indexing::public_mode::{TRANSITION_KEY, parse_ts};
use crate::indexing::{PublicMode, ScopeValue};

/// The page's own path, where the switch's refusals and Cancel go back to.
const HERE: &str = "/admin/indexing";

/// The public-data switch as the page shows it.
pub struct PublicView {
    /// The phase in words.
    pub phase: String,
    /// Community is chosen.
    pub on: bool,
}

#[derive(Template)]
#[template(path = "indexing.html")]
struct IndexingPage {
    css: &'static str,
    nav: Option<Nav>,
    title: String,
    csrf: String,
    public: PublicView,
    /// A refill's progress, while one runs.
    refill: Vec<Figure>,
    /// The longest wait for a refill.
    settings: SettingsBlock,
}

/// The public-data phase in words; Health and About say it too.
#[must_use]
pub fn phase_words(mode: PublicMode) -> &'static str {
    match mode {
        PublicMode::On => "on",
        PublicMode::Filling => "filling: public data is being refilled, and public requests come back when it is done",
        PublicMode::Draining => "draining: stored public data is being deleted in the background",
        PublicMode::Off => "off",
        PublicMode::Held => "held: Prism could not read the state of this switch, so it keeps public data off and changes nothing until it is set again",
    }
}

fn figure(label: &str, value: String) -> Figure {
    Figure { label: label.to_string(), value, link: None }
}

/// When a refill's last accounts started being waited for, from the stored
/// phase; `None` while accounts are still being queued.
async fn waiting_since(state: &AdminState) -> Result<Option<DateTime<Utc>>, sqlx::Error> {
    let raw: Option<Value> = sqlx::query_scalar("SELECT value FROM worker_state WHERE key = $1").bind(TRANSITION_KEY).fetch_optional(&state.pool).await?;
    Ok(raw.as_ref().and_then(|t| t.get("fill")).and_then(|f| f.get("waiting_since")).and_then(parse_ts))
}

/// When the longest wait ends: `hours` after `since`, or, before the wait
/// starts, how long it will be.
#[must_use]
pub fn wait_words(since: Option<DateTime<Utc>>, hours: u32, now: DateTime<Utc>) -> String {
    match since {
        None => format!("{hours} hours from when every account is queued"),
        Some(since) => {
            let end = since + TimeDelta::hours(i64::from(hours));
            if end <= now { "now: public requests are answered once this refill's check runs".to_string() } else { format!("{} (in {})", when(end), until(end - now)) }
        }
    }
}

/// A span ahead, in words.
fn until(d: TimeDelta) -> String {
    let minutes = d.num_minutes().max(1);
    if minutes < 120 {
        format!("{minutes} minutes")
    } else {
        format!("{} hours", (minutes + 30) / 60)
    }
}

/// Whether Community is the switch's saved choice.
///
/// # Errors
///
/// Database errors.
pub async fn community_chosen(state: &AdminState) -> Result<bool, sqlx::Error> {
    let value: Option<Value> = sqlx::query_scalar("SELECT value FROM operator_settings WHERE key = $1").bind(PUBLIC_SCOPE_KEY).fetch_optional(&state.pool).await?;
    Ok(value.as_ref().and_then(Value::as_str) == Some(ScopeValue::Community.as_str()))
}

/// `GET /admin/indexing`.
pub async fn page(State(state): State<AdminState>, ConnectInfo(peer): ConnectInfo<SocketAddr>, headers: HeaderMap, Query(q): Query<OpenQuery>) -> Response {
    let op = match require(&state, &headers, peer, Need::Page, None).await {
        Ok(op) => op,
        Err(response) => return response,
    };
    let sections = view::sections_at(Place::Page(PageId::Indexing), &state.settings.networks());
    let settings = match view::settings_block(&state, &op.csrf, &sections, q.open.as_deref() == Some("advanced")).await {
        Ok(b) => b,
        Err(e) => return super::internal_error(&state, &e),
    };
    let on = match community_chosen(&state).await {
        Ok(on) => on,
        Err(e) => return super::internal_error(&state, &e),
    };
    let mode = state.scope.public_mode();
    let mut refill = Vec::new();
    if mode == PublicMode::Filling {
        let since = match waiting_since(&state).await {
            Ok(s) => s,
            Err(e) => return super::internal_error(&state, &e),
        };
        let left = state.health.now().and_then(|s| s.sum(crate::indexing::public_fill::METRIC_REMAINING));
        refill.push(figure("Accounts left to refill", left.map_or_else(|| "unknown".to_string(), count)));
        if let Some(s) = since {
            refill.push(figure("Waiting for the last accounts since", format!("{} ({})", when(s), ago(s))));
        }
        refill.push(figure("Longest wait ends", wait_words(since, state.settings.running.indexing.fill_max_wait_hours, Utc::now())));
    }
    render(
        StatusCode::OK,
        &IndexingPage {
            css: css(),
            nav: Some(Nav::page(&op, PageId::Indexing)),
            title: PageId::Indexing.page().name.to_string(),
            csrf: op.csrf.clone(),
            public: PublicView { phase: phase_words(mode).to_string(), on },
            refill,
            settings,
        },
    )
}

/// The switch's form.
#[derive(Debug, serde::Deserialize)]
pub struct SwitchForm {
    #[serde(default)]
    pub csrf: String,
    #[serde(default)]
    pub value: String,
    pub confirm: Option<String>,
    pub typed: Option<String>,
    /// The page the form was on, for Cancel.
    pub back: Option<String>,
}

/// `POST /admin/indexing/switch`, also at `/admin/settings/public-scope`
/// for pages loaded before it moved: its own command, which moves through
/// phases. Turning public data off is typed (`OFF`).
pub async fn switch(State(state): State<AdminState>, ConnectInfo(peer): ConnectInfo<SocketAddr>, headers: HeaderMap, Form(form): Form<SwitchForm>) -> Response {
    let op = match require(&state, &headers, peer, Need::Change, Some(&form.csrf)).await {
        Ok(op) => op,
        Err(response) => return response,
    };
    let Some(value) = ScopeValue::parse(&form.value) else {
        return message(&state, Some(Nav::at(&op, HERE)), StatusCode::BAD_REQUEST, "Not saved", "Choose Kryphocron only or Community.", Some((HERE, "Indexing")));
    };
    if value == ScopeValue::KryphocronOnly && state.scope.public_mode() != PublicMode::Off {
        let word = "OFF";
        let back = back_to(&state, &headers, form.back.as_deref(), HERE);
        let mut page = ConfirmPage {
            css: css(),
            nav: Some(Nav::at(&op, &back)),
            back,
            title: "Switch to Kryphocron only".to_string(),
            what: "Prism stops answering public Bluesky requests and deletes the public data it stored, in the background. Only what kryphocron posts and Horizon features need is kept: blocks, follows involving members, profiles, and members' own records.".to_string(),
            undo: "Switching back to Community refills public data; public requests come back when the refill is done.".to_string(),
            action: "/admin/indexing/switch".to_string(),
            csrf: op.csrf.clone(),
            hidden: vec![Hidden { name: "value".to_string(), value: value.as_str().to_string() }],
            typed: Some(word.to_string()),
            button: "Switch".to_string(),
            error: None,
        };
        if form.confirm.as_deref() != Some("yes") {
            return render(StatusCode::OK, &page);
        }
        if !typed_matches(word, form.typed.as_deref()) {
            page.error = Some(format!("Type {word} exactly to confirm."));
            return render(StatusCode::BAD_REQUEST, &page);
        }
    }
    enqueue(&state, &op, &AdminCommand::IndexingPublicScope { value }).await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_longest_wait_in_words() {
        let now = parse_ts(&serde_json::json!("2026-11-01T12:00:00Z")).expect("ts");
        assert_eq!(wait_words(None, 24, now), "24 hours from when every account is queued");
        assert_eq!(wait_words(Some(now - TimeDelta::hours(1)), 24, now), "2026-11-02 11:00 UTC (in 23 hours)");
        assert_eq!(wait_words(Some(now - TimeDelta::minutes(30)), 1, now), "2026-11-01 12:30 UTC (in 30 minutes)");
        assert!(wait_words(Some(now - TimeDelta::hours(25)), 24, now).starts_with("now"));
    }
}
