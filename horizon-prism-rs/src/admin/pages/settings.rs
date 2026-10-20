//! Settings (`/admin/settings`, admin-ui design §6.3, §8): one page per
//! group, each a form with one section per setting: its explanation, the
//! value saved now, the default, whether it applies at once or after a
//! restart, the value `app.toml` writes if it writes one, and the last
//! change. Saving a group sends one `settings set` command with every
//! changed field; the executor validates the whole configuration before
//! anything is saved. Public data has its own form and command. Two
//! read-only pages: what was set at install, and everything Prism is
//! running with. And the restart (C-A4).

use std::collections::BTreeMap;
use std::net::SocketAddr;

use askama::Template;
use axum::Form;
use axum::extract::{ConnectInfo, Path, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::Response;
use serde_json::Value;

use super::{ConfirmPage, Hidden, Nav, css, enqueue, message, render, setting_name, typed_matches, when};
use crate::admin::AdminState;
use crate::admin::auth::{Need, Operator, require};
use crate::admin::settings::registry::{self, Group, Kind, Section};
use crate::admin::settings::store::{self, Change, PUBLIC_SCOPE_KEY, Row};
use crate::admin::settings::{SettingsContext, overlay};
use crate::admin_executor::AdminCommand;
use crate::indexing::{PublicMode, ScopeValue};

/// A choice of a select.
pub struct Opt {
    pub value: String,
    pub label: String,
    pub selected: bool,
}

/// One setting on its page.
pub struct FieldView {
    pub key: String,
    pub label: String,
    /// `select`, `text` or `textarea`.
    pub input: &'static str,
    pub value: String,
    pub options: Vec<Opt>,
    /// The unit beside a number.
    pub unit: String,
    pub current: String,
    pub default: String,
    /// What `app.toml` writes, when it writes the key.
    pub file: Option<String>,
    pub version: i64,
    /// "by …, on …" when a value is saved.
    pub saved: Option<String>,
    /// Why the saved value could not be used at the last start.
    pub set_aside: Option<String>,
    /// `app.toml` changed under the saved value: its new value.
    pub file_notice: Option<String>,
    /// A restart key whose saved value is not in use yet: what is.
    pub running: Option<String>,
    /// The reset button's words, when a value is saved.
    pub reset: Option<String>,
}

/// Settings explained together.
pub struct SectionView {
    pub title: String,
    pub help: String,
    pub applies: &'static str,
    pub warning: Option<String>,
    pub fields: Vec<FieldView>,
}

/// The public-data form (§6.3).
pub struct PublicView {
    pub value: String,
    pub phase: String,
    pub on: bool,
}

/// A group in the index.
pub struct GroupLink {
    pub slug: &'static str,
    pub title: &'static str,
    pub summary: &'static str,
}

#[derive(Template)]
#[template(path = "settings.html")]
struct IndexPage {
    css: &'static str,
    nav: Option<Nav>,
    title: String,
    groups: Vec<GroupLink>,
    aside: Vec<Notice>,
    waiting: Vec<Notice>,
}

#[derive(Template)]
#[template(path = "settings_group.html")]
struct GroupPage {
    css: &'static str,
    nav: Option<Nav>,
    title: String,
    slug: &'static str,
    csrf: String,
    sections: Vec<SectionView>,
    public: Option<PublicView>,
    lodestar: Option<String>,
}

/// A read-only row: what, its value, and why it is fixed.
pub struct Fixed {
    pub what: String,
    pub value: String,
    pub why: String,
}

#[derive(Template)]
#[template(path = "settings_installed.html")]
struct InstalledPage {
    css: &'static str,
    nav: Option<Nav>,
    title: String,
    rows: Vec<Fixed>,
}

#[derive(Template)]
#[template(path = "settings_running.html")]
struct RunningPage {
    css: &'static str,
    nav: Option<Nav>,
    title: String,
    text: String,
}

/// A setting named in a list, with where to change it.
pub struct Notice {
    pub name: String,
    pub href: String,
    pub text: String,
}

fn group_of(key: &str, ctx: &SettingsContext) -> Option<Group> {
    registry::lookup(key, &ctx.networks()).map(|(s, _)| s.group)
}

fn href(key: &str, ctx: &SettingsContext) -> String {
    group_of(key, ctx).map_or_else(|| "/admin/settings".to_string(), |g| format!("/admin/settings/{}", g.slug()))
}

/// The saved value of `key`, else the file's.
fn saved_value(key: &str, rows: &BTreeMap<String, Row>, ctx: &SettingsContext) -> Value {
    rows.get(key).map_or_else(|| registry::value_at(&ctx.file_json, key), |r| r.value.clone())
}

fn words_of(key: &str, v: &Value, ctx: &SettingsContext) -> String {
    let rctx = ctx.registry();
    registry::lookup(key, &rctx.networks).map_or_else(|| v.to_string(), |(_, f)| registry::words(&f.kind, v, &rctx))
}

/// Settings whose `app.toml` value changed under a saved one (§8.4), for
/// Home: listed until each is kept or reset.
///
/// # Errors
///
/// Database errors.
pub async fn file_notices(state: &AdminState) -> Result<Vec<Notice>, sqlx::Error> {
    let ctx = &state.settings;
    Ok(store::rows(&state.pool)
        .await?
        .into_iter()
        .filter(|r| r.file_notice)
        .map(|r| {
            let file = r.file_value.as_ref().and_then(|f| f.get("value")).cloned().unwrap_or(Value::Null);
            Notice {
                name: setting_name(&r.key),
                href: href(&r.key, ctx),
                text: format!("app.toml now says {}; your saved value {} still applies.", words_of(&r.key, &file, ctx), words_of(&r.key, &r.value, ctx)),
            }
        })
        .collect())
}

fn by(label: &str) -> String {
    match label {
        "cli" => "the command line".to_string(),
        "file" => "app.toml".to_string(),
        other => other.to_string(),
    }
}

async fn saved_rows(state: &AdminState) -> Result<BTreeMap<String, Row>, sqlx::Error> {
    Ok(store::rows(&state.pool).await?.into_iter().map(|r| (r.key.clone(), r)).collect())
}

/// `GET /admin/settings`.
pub async fn index(State(state): State<AdminState>, ConnectInfo(peer): ConnectInfo<SocketAddr>, headers: HeaderMap) -> Response {
    let op = match require(&state, &headers, peer, Need::Page, None).await {
        Ok(op) => op,
        Err(response) => return response,
    };
    let saved = match store::rows(&state.pool).await {
        Ok(r) => r,
        Err(e) => return super::internal_error(&state, &e),
    };
    let ctx = &state.settings;
    let aside = saved
        .iter()
        .filter_map(|r| {
            let problem = r.set_aside.as_ref()?.get("problem").and_then(Value::as_str).unwrap_or("a problem").to_string();
            Some(Notice { name: setting_name(&r.key), href: href(&r.key, ctx), text: problem })
        })
        .collect();
    let by_key: BTreeMap<String, Row> = saved.iter().map(|r| (r.key.clone(), r.clone())).collect();
    let waiting = store::waiting(&saved, &ctx.file_json, &ctx.running_json, &ctx.networks())
        .into_iter()
        .map(|key| {
            let text = format!(
                "Saved: {}. In use until a restart: {}.",
                words_of(&key, &saved_value(&key, &by_key, ctx), ctx),
                words_of(&key, &registry::value_at(&ctx.running_json, &key), ctx)
            );
            Notice { name: setting_name(&key), href: href(&key, ctx), text }
        })
        .collect();
    let groups = Group::ALL.into_iter().map(|g| GroupLink { slug: g.slug(), title: g.title(), summary: g.summary() }).collect();
    render(StatusCode::OK, &IndexPage { css: css(), nav: Some(Nav::new(&op, "settings")), title: "Settings".to_string(), groups, aside, waiting })
}

fn field_view(section: &Section, f: &registry::Field, rows: &BTreeMap<String, Row>, ctx: &SettingsContext) -> FieldView {
    let rctx = ctx.registry();
    let row = rows.get(&f.key);
    let current = saved_value(&f.key, rows, ctx);
    let default = registry::value_at(&crate::admin::settings::overlay::json(&crate::config::FileConfig::default()), &f.key);
    let file_explicit = overlay::has(&ctx.file_table, &f.key);
    let file_value = registry::value_at(&ctx.file_json, &f.key);
    let running = registry::value_at(&ctx.running_json, &f.key);
    let words = |v: &Value| registry::words(&f.kind, v, &rctx);
    let mut options: Vec<Opt> = registry::options(&f.kind, &rctx)
        .into_iter()
        .map(|(value, label)| Opt { selected: value == registry::form_text(&f.kind, &current), value, label })
        .collect();
    // A saved or file value the choices do not offer stays selectable.
    if registry::is_choice(&f.kind) && !options.iter().any(|o| o.selected) {
        let text = registry::form_text(&f.kind, &current);
        options.push(Opt { label: text.clone(), value: text, selected: true });
    }
    let unit = match &f.kind {
        Kind::Whole { unit, .. } | Kind::Decimal { unit, .. } => (*unit).to_string(),
        Kind::Gigabytes => "GB".to_string(),
        Kind::TimeOfDay => "UTC".to_string(),
        _ => String::new(),
    };
    let set_aside = row.and_then(|r| r.set_aside.as_ref()).map(|a| a.get("problem").and_then(Value::as_str).unwrap_or("a problem").to_string());
    let waiting = !section.live && set_aside.is_none() && !overlay::same(&current, &running);
    FieldView {
        key: f.key.clone(),
        label: f.label.clone(),
        input: if registry::is_choice(&f.kind) {
            "select"
        } else if f.kind.multiline() {
            "textarea"
        } else {
            "text"
        },
        value: registry::form_text(&f.kind, &current),
        options,
        unit,
        current: words(&current),
        default: words(&default),
        file: file_explicit.then(|| words(&file_value)),
        version: row.map_or(0, |r| r.version),
        saved: row.map(|r| format!("saved by {}, {}", by(&r.updated_by), when(r.updated_at))),
        set_aside,
        file_notice: row.filter(|r| r.file_notice).map(|r| {
            let file = r.file_value.as_ref().and_then(|f| f.get("value")).cloned().unwrap_or(Value::Null);
            words(&file)
        }),
        running: waiting.then(|| words(&running)),
        reset: row.map(|_| if file_explicit { format!("Use app.toml's value ({})", words(&file_value)) } else { format!("Use the default ({})", words(&default)) }),
    }
}

/// A warning a section needs: floors set on only some reads.
fn section_warning(section: &Section, rows: &BTreeMap<String, Row>, ctx: &SettingsContext) -> Option<String> {
    let floors: Vec<i64> = section
        .fields
        .iter()
        .filter(|f| f.key.starts_with("private_read.floor_ms."))
        .map(|f| saved_value(&f.key, rows, ctx).as_i64().unwrap_or(0))
        .collect();
    let set = floors.iter().filter(|ms| **ms > 0).count();
    (set > 0 && set < floors.len()).then(|| {
        "Only some floors are set. The reads without one still answer as soon as they can, so their timing can still hint at what the floors hide.".to_string()
    })
}

fn not_found(state: &AdminState, op: &Operator) -> Response {
    message(state, Some(Nav::new(op, "settings")), StatusCode::NOT_FOUND, "Not found", "There is no such settings page.", Some(("/admin/settings", "Settings")))
}

/// `GET /admin/settings/{group}`.
pub async fn group(
    State(state): State<AdminState>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    Path(slug): Path<String>,
) -> Response {
    let op = match require(&state, &headers, peer, Need::Page, None).await {
        Ok(op) => op,
        Err(response) => return response,
    };
    let Some(group) = Group::from_slug(&slug) else {
        return not_found(&state, &op);
    };
    let rows = match saved_rows(&state).await {
        Ok(r) => r,
        Err(e) => return super::internal_error(&state, &e),
    };
    let ctx = &state.settings;
    let sections = registry::sections(&ctx.networks())
        .into_iter()
        .filter(|s| s.group == group)
        .map(|s| SectionView {
            title: s.title.clone(),
            help: s.help.clone(),
            applies: if s.live { "Applies at once" } else { "Applies after a restart" },
            warning: section_warning(&s, &rows, ctx),
            fields: s.fields.iter().map(|f| field_view(&s, f, &rows, ctx)).collect(),
        })
        .collect();
    let public = if group == Group::Storage {
        let value: Option<Value> = match sqlx::query_scalar("SELECT value FROM operator_settings WHERE key = $1").bind(PUBLIC_SCOPE_KEY).fetch_optional(&state.pool).await {
            Ok(v) => v,
            Err(e) => return super::internal_error(&state, &e),
        };
        let value = value.and_then(|v| v.as_str().map(str::to_string)).unwrap_or_else(|| "off".to_string());
        let mode = state.scope.public_mode();
        Some(PublicView { on: value == ScopeValue::CommunityScoped.as_str(), value, phase: phase_words(mode).to_string() })
    } else {
        None
    };
    let lodestar = (group == Group::Lookups).then(|| match &ctx.lodestar {
        None => "Lodestar is off: no address was set when Prism started.".to_string(),
        Some(l) if l.gate().is_open() => format!("Lodestar ({}) is answering and up to date, so Prism is reading from it.", l.host()),
        Some(l) => format!("Lodestar ({}) is not answering or is behind, so Prism is reading from the PLC directory.", l.host()),
    });
    render(
        StatusCode::OK,
        &GroupPage {
            css: css(),
            nav: Some(Nav::new(&op, "settings")),
            title: group.title().to_string(),
            slug: group.slug(),
            csrf: op.csrf.clone(),
            sections,
            public,
            lodestar,
        },
    )
}

fn phase_words(mode: PublicMode) -> &'static str {
    match mode {
        PublicMode::On => "on",
        PublicMode::Filling => "filling: public data is being refilled, and public requests come back when it is done",
        PublicMode::Draining => "draining: stored public data is being deleted in the background",
        PublicMode::Off => "off",
        PublicMode::Held => "held: Prism could not read the state of this switch, so it keeps public data off and changes nothing until it is set again",
    }
}

fn refused(state: &AdminState, op: &Operator, back: &str, text: &str) -> Response {
    message(state, Some(Nav::new(op, "settings")), StatusCode::BAD_REQUEST, "Not saved", text, Some((back, "Back to the settings")))
}

/// What a typed change does, in words.
fn typed_what(key: &str) -> &'static str {
    match key {
        "farsight.fallback.enabled" => {
            "The fallback is turned on after the next restart. While Farsight is down, another AppView then learns which of your members' pages were loaded."
        }
        "farsight.memory.enabled" => "Remembering blocks is turned off after the next restart, and what was remembered is deleted then.",
        _ => "This change needs confirming.",
    }
}

fn first<'a>(pairs: &'a [(String, String)], name: &str) -> Option<&'a str> {
    pairs.iter().find(|(k, _)| k == name).map(|(_, v)| v.as_str())
}

/// The confirmation of typed changes, carrying the whole form again.
fn confirm_typed(op: &Operator, action: &str, pairs: &[(String, String)], what: String, word: String, error: Option<String>) -> ConfirmPage {
    ConfirmPage {
        css: css(),
        nav: Some(Nav::new(op, "settings")),
        title: "Confirm the change".to_string(),
        what,
        undo: "You can change it back later.".to_string(),
        action: action.to_string(),
        csrf: op.csrf.clone(),
        hidden: pairs
            .iter()
            .filter(|(k, _)| !matches!(k.as_str(), "csrf" | "confirm" | "typed"))
            .map(|(name, value)| Hidden { name: name.clone(), value: value.clone() })
            .collect(),
        typed: Some(word),
        button: "Save".to_string(),
        error,
    }
}

/// `POST /admin/settings/{group}`: one `settings set` with every field the
/// form changed (§8.3); typed changes are confirmed first (§5.5).
pub async fn save(
    State(state): State<AdminState>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    Path(slug): Path<String>,
    Form(pairs): Form<Vec<(String, String)>>,
) -> Response {
    let op = match require(&state, &headers, peer, Need::Change, first(&pairs, "csrf")).await {
        Ok(op) => op,
        Err(response) => return response,
    };
    let Some(group) = Group::from_slug(&slug) else {
        return not_found(&state, &op);
    };
    let back = format!("/admin/settings/{slug}");
    let rows = match saved_rows(&state).await {
        Ok(r) => r,
        Err(e) => return super::internal_error(&state, &e),
    };
    let ctx = &state.settings;
    let rctx = ctx.registry();
    let (mut changes, mut problems, mut words) = (Vec::new(), Vec::new(), Vec::new());
    let mut whats = Vec::new();
    for section in registry::sections(&rctx.networks).into_iter().filter(|s| s.group == group) {
        for f in &section.fields {
            let Some(text) = first(&pairs, &f.key) else {
                continue;
            };
            let value = match registry::parse(&f.kind, text, &rctx) {
                Ok(v) => v,
                Err(e) => {
                    problems.push(format!("{}: {e}.", setting_name(&f.key)));
                    continue;
                }
            };
            let current = saved_value(&f.key, &rows, ctx);
            if overlay::same(&value, &current) {
                continue;
            }
            if let Some(word) = registry::typed_word(f, &current, &value) {
                words.push(word);
                whats.push(typed_what(&f.key));
            }
            let base_version = first(&pairs, &format!("version:{}", f.key)).and_then(|v| v.parse().ok()).unwrap_or(0);
            changes.push(Change { key: f.key.clone(), value: Some(value), base_version });
        }
    }
    if !problems.is_empty() {
        return refused(&state, &op, &back, &format!("Nothing was saved. {}", problems.join(" ")));
    }
    if changes.is_empty() {
        return message(&state, Some(Nav::new(&op, "settings")), StatusCode::OK, "Nothing changed", "The form had no changes to save.", Some((&back, "Back to the settings")));
    }
    if !words.is_empty() {
        let word = words.join(" ");
        let typed = first(&pairs, "typed");
        if first(&pairs, "confirm") != Some("yes") || !typed_matches(&word, typed) {
            let error = (first(&pairs, "confirm") == Some("yes")).then(|| format!("Type {word} exactly to confirm."));
            let status = if error.is_some() { StatusCode::BAD_REQUEST } else { StatusCode::OK };
            return render(status, &confirm_typed(&op, &back, &pairs, whats.join(" "), word, error));
        }
    }
    enqueue(&state, &op, &AdminCommand::SettingsSet { changes }).await
}

/// The key and version of a one-setting form.
#[derive(Debug, serde::Deserialize)]
pub struct KeyForm {
    #[serde(default)]
    pub csrf: String,
    #[serde(default)]
    pub key: String,
    #[serde(default)]
    pub version: i64,
    pub confirm: Option<String>,
    pub typed: Option<String>,
}

/// `POST /admin/settings/reset`: remove the saved value, back to the
/// file's or the default (§8.3).
pub async fn reset(
    State(state): State<AdminState>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    Form(form): Form<KeyForm>,
) -> Response {
    let op = match require(&state, &headers, peer, Need::Change, Some(&form.csrf)).await {
        Ok(op) => op,
        Err(response) => return response,
    };
    let ctx = &state.settings;
    let Some((_, field)) = registry::lookup(&form.key, &ctx.networks()) else {
        return refused(&state, &op, "/admin/settings", "That is not a setting that can be changed here.");
    };
    let rows = match saved_rows(&state).await {
        Ok(r) => r,
        Err(e) => return super::internal_error(&state, &e),
    };
    let current = saved_value(&form.key, &rows, ctx);
    let to = registry::value_at(&ctx.file_json, &form.key);
    if let Some(word) = registry::typed_word(&field, &current, &to) {
        let confirmed = form.confirm.as_deref() == Some("yes");
        if !confirmed || !typed_matches(word, form.typed.as_deref()) {
            let pairs = vec![("key".to_string(), form.key.clone()), ("version".to_string(), form.version.to_string())];
            let error = confirmed.then(|| format!("Type {word} exactly to confirm."));
            let status = if error.is_some() { StatusCode::BAD_REQUEST } else { StatusCode::OK };
            return render(status, &confirm_typed(&op, "/admin/settings/reset", &pairs, typed_what(&form.key).to_string(), word.to_string(), error));
        }
    }
    enqueue(&state, &op, &AdminCommand::SettingsSet { changes: vec![Change { key: form.key, value: None, base_version: form.version }] }).await
}

/// `POST /admin/settings/keep`: keep the saved value although `app.toml`
/// changed under it (§8.4); the notice goes.
pub async fn keep(
    State(state): State<AdminState>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    Form(form): Form<KeyForm>,
) -> Response {
    let op = match require(&state, &headers, peer, Need::Change, Some(&form.csrf)).await {
        Ok(op) => op,
        Err(response) => return response,
    };
    let rows = match saved_rows(&state).await {
        Ok(r) => r,
        Err(e) => return super::internal_error(&state, &e),
    };
    let Some(row) = rows.get(&form.key) else {
        return refused(&state, &op, "/admin/settings", "That setting has no saved value any more.");
    };
    let change = Change { key: form.key.clone(), value: Some(row.value.clone()), base_version: form.version };
    enqueue(&state, &op, &AdminCommand::SettingsSet { changes: vec![change] }).await
}

/// The public-data form.
#[derive(Debug, serde::Deserialize)]
pub struct PublicForm {
    #[serde(default)]
    pub csrf: String,
    #[serde(default)]
    pub value: String,
    pub confirm: Option<String>,
    pub typed: Option<String>,
}

/// `POST /admin/settings/public-scope`: its own command, which moves
/// through phases (§6.3). Turning it off is typed (`OFF`).
pub async fn public_scope(
    State(state): State<AdminState>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    Form(form): Form<PublicForm>,
) -> Response {
    let op = match require(&state, &headers, peer, Need::Change, Some(&form.csrf)).await {
        Ok(op) => op,
        Err(response) => return response,
    };
    let Some(value) = ScopeValue::parse(&form.value) else {
        return refused(&state, &op, "/admin/settings/storage", "Choose on or off.");
    };
    if value == ScopeValue::Off && state.scope.public_mode() != PublicMode::Off {
        let word = "OFF";
        let mut page = ConfirmPage {
            css: css(),
            nav: Some(Nav::new(&op, "settings")),
            title: "Turn public data off".to_string(),
            what: "Prism stops answering public Bluesky requests and deletes the public data it stored, in the background. Only what private posts need is kept: blocks, follows involving members, profiles, and members' own records.".to_string(),
            undo: "Turning it back on refills public data; public requests come back when the refill is done.".to_string(),
            action: "/admin/settings/public-scope".to_string(),
            csrf: op.csrf.clone(),
            hidden: vec![Hidden { name: "value".to_string(), value: value.as_str().to_string() }],
            typed: Some(word.to_string()),
            button: "Turn off".to_string(),
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

/// The confirmation of a restart.
#[derive(Debug, serde::Deserialize)]
pub struct RestartForm {
    #[serde(default)]
    pub csrf: String,
    pub confirm: Option<String>,
}

/// `POST /admin/restart` (§8.5, C-A4): confirmed, then a `restart`
/// command.
pub async fn restart(
    State(state): State<AdminState>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    Form(form): Form<RestartForm>,
) -> Response {
    let op = match require(&state, &headers, peer, Need::Change, Some(&form.csrf)).await {
        Ok(op) => op,
        Err(response) => return response,
    };
    if !state.settings.env.supervised {
        return message(
            &state,
            Some(Nav::new(&op, "settings")),
            StatusCode::CONFLICT,
            "Cannot restart here",
            "Prism cannot restart itself on this server; ask whoever runs it to restart Prism.",
            Some(("/admin/settings", "Settings")),
        );
    }
    if form.confirm.as_deref() != Some("yes") {
        return render(
            StatusCode::OK,
            &ConfirmPage {
                css: css(),
                nav: Some(Nav::new(&op, "settings")),
                title: "Restart Prism".to_string(),
                what: "Prism will be unavailable for about a minute. Apps will show errors until it is back. Background work (removals, clean-ups, syncs) stops and carries on after the restart.".to_string(),
                undo: "Saved changes that wait for a restart are applied when Prism starts again.".to_string(),
                action: "/admin/restart".to_string(),
                csrf: op.csrf.clone(),
                hidden: Vec::new(),
                typed: None,
                button: "Restart".to_string(),
                error: None,
            },
        );
    }
    enqueue(&state, &op, &AdminCommand::Restart).await
}

/// A URL with any `user:password@` removed (§5.9).
#[must_use]
pub fn without_credentials(text: &str) -> String {
    match url::Url::parse(text) {
        Ok(mut u) if !u.username().is_empty() || u.password().is_some() => {
            let _ = u.set_username("");
            let _ = u.set_password(None);
            u.to_string()
        }
        _ => text.to_string(),
    }
}

fn strip(v: &mut toml::Value) {
    match v {
        toml::Value::String(s) => *s = without_credentials(s),
        toml::Value::Array(a) => a.iter_mut().for_each(strip),
        toml::Value::Table(t) => t.iter_mut().for_each(|(_, v)| strip(v)),
        _ => {}
    }
}

/// The whole running configuration as TOML, URL credentials removed.
#[must_use]
pub fn running_text(cfg: &crate::config::FileConfig) -> String {
    let Ok(mut v) = toml::Value::try_from(cfg) else {
        return String::new();
    };
    strip(&mut v);
    toml::to_string_pretty(&v).unwrap_or_default()
}

/// `GET /admin/settings/running`: everything Prism is running with.
pub async fn running(State(state): State<AdminState>, ConnectInfo(peer): ConnectInfo<SocketAddr>, headers: HeaderMap) -> Response {
    let op = match require(&state, &headers, peer, Need::Page, None).await {
        Ok(op) => op,
        Err(response) => return response,
    };
    render(
        StatusCode::OK,
        &RunningPage {
            css: css(),
            nav: Some(Nav::new(&op, "settings")),
            title: "Everything Prism is running with".to_string(),
            text: running_text(&state.settings.running),
        },
    )
}

fn list<T: std::fmt::Display>(items: impl IntoIterator<Item = T>) -> String {
    let v: Vec<String> = items.into_iter().map(|i| i.to_string()).collect();
    if v.is_empty() { "none".to_string() } else { v.join(", ") }
}

/// The rows of "Set when Prism was installed" (§8.6).
#[must_use]
pub fn installed_rows(ctx: &SettingsContext) -> Vec<Fixed> {
    let c = &ctx.running;
    let row = |what: &str, value: String, why: &str| Fixed { what: what.to_string(), value, why: why.to_string() };
    let mut rows = vec![
        row("This Prism's address", ctx.env.public_hostname.clone(), "Apps and other services know Prism by it; it is set in .env."),
        row("Service identity", format!("did:web:{}", ctx.env.public_hostname), "It follows from the address."),
    ];
    for (name, set) in &ctx.env.secrets {
        rows.push(row(name, if *set { "set" } else { "not set" }.to_string(), "A secret or an address with a password, kept in .env. It is never shown."));
    }
    rows.extend([
        row("Listening address", c.http.bind.to_string(), "A wrong value makes Prism unreachable."),
        row("Trusted proxy", c.http.trusted_proxy.to_string(), "Prism trusts the visitor addresses this proxy reports; a wrong value breaks rate limits."),
        row("Metrics address", c.metrics.bind.to_string(), "A wrong value hides Prism's metrics."),
        row("DNS servers", format!("public: {}; internal: {}", list(&c.net.public_nameservers), list(&c.net.internal_nameservers)), "A wrong value leaves Prism unable to find other servers."),
        row(
            "Network stream",
            format!("{} (group {}, consumer {}, problems to {})", c.firehose.stream_key, c.firehose.group, c.firehose.consumer_name, c.firehose.dead_letter_stream),
            "These must match the relay readers set up beside Prism.",
        ),
        row("Relays", list(c.firehose.relay_labels.iter().map(|(url, label)| format!("{label} = {url}"))), "These must match the relay readers set up beside Prism."),
        row("Relays private posts wait for", list(&c.firehose.private_relays), "These must match the relay readers set up beside Prism."),
        row("Farsight address", c.farsight.url.as_deref().map_or_else(|| "not set".to_string(), without_credentials), "It goes with FARSIGHT_TOKEN in .env."),
        row(
            "Networks",
            list(c.constellation.networks.iter().map(|n| format!("{} ({}{})", n.name, without_credentials(&n.appview_url), if n.enabled { "" } else { ", off" }))),
            "Which AppView serves which servers is part of the install; only each network's background refresh rate is a setting.",
        ),
        row("Order networks are asked in", list(&c.constellation.cascade.order), "Part of the install, with the networks."),
        row(
            "Which accounts Prism keeps",
            registry::value_at(&ctx.running_json, "scope.policy").as_str().unwrap_or_default().replace('_', " "),
            "Changing it re-indexes everything; it needs its own plan.",
        ),
    ]);
    for (name, s) in &c.services {
        rows.push(row(
            &format!("Extra service: {name}"),
            format!(
                "{}{}",
                s.proxy_url.as_deref().map_or_else(|| "no address".to_string(), without_credentials),
                if s.allow_private_network == Some(true) { ", private network allowed" } else { "" }
            ),
            "Where requests go is a decision about Prism's network safety, made at install.",
        ));
    }
    rows.push(row("Admin, database and private-post settings", "see everything Prism is running with".to_string(), "Set in app.toml: they change how Prism itself runs."));
    rows
}

/// `GET /admin/settings/installed`: what was set at install, read-only.
pub async fn installed(State(state): State<AdminState>, ConnectInfo(peer): ConnectInfo<SocketAddr>, headers: HeaderMap) -> Response {
    let op = match require(&state, &headers, peer, Need::Page, None).await {
        Ok(op) => op,
        Err(response) => return response,
    };
    render(
        StatusCode::OK,
        &InstalledPage {
            css: css(),
            nav: Some(Nav::new(&op, "settings")),
            title: "Set when Prism was installed".to_string(),
            rows: installed_rows(&state.settings),
        },
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    /// T-A15 (settings): the running configuration never shows a URL's
    /// password.
    #[test]
    fn credentials_are_removed() {
        assert_eq!(without_credentials("https://user:secret@farsight.example.net/x"), "https://farsight.example.net/x");
        assert_eq!(without_credentials("https://farsight.example.net"), "https://farsight.example.net");
        let mut cfg = crate::config::FileConfig::default();
        cfg.farsight.url = Some("https://user:secret@farsight.example.net".to_string());
        let text = running_text(&cfg);
        assert!(text.contains("farsight.example.net") && !text.contains("secret"), "{text}");
    }
}
