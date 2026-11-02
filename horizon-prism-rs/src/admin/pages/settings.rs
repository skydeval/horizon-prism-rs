//! Settings (`/admin/settings`): the index of every page that holds
//! settings, the old group pages for the settings whose feature page does
//! not hold them yet, and the one save every settings form posts to. A save
//! sends one `settings set` command with every changed field; the executor
//! validates the whole configuration before anything is saved. Two
//! read-only pages: what was set at install, and everything Prism is
//! running with. And the restart.

use std::collections::BTreeMap;
use std::net::SocketAddr;

use askama::Template;
use axum::Form;
use axum::extract::rejection::{PathRejection, QueryRejection};
use axum::extract::{ConnectInfo, Path, Query, RawQuery, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::Response;
use serde_json::Value;

use super::{ConfirmPage, Hidden, Link, Nav, back_to, css, enqueue, message, render, setting_name, typed_matches};
use crate::admin::AdminState;
use crate::admin::auth::{Need, Operator, require};
use crate::admin::menu::{self, MENU, PageId, SETTINGS_LIVE};
use crate::admin::settings::SettingsContext;
use crate::admin::settings::overlay;
use crate::admin::settings::registry::{self, Group};
use crate::admin::settings::store::{self, Change, Row};
use crate::admin::settings::view::{self, Place, SettingsBlock, href, saved_rows, saved_value};
use crate::admin_executor::AdminCommand;

/// A group in the index, with the titles of the settings still on it.
pub struct GroupLink {
    pub slug: &'static str,
    pub title: &'static str,
    pub summary: String,
}

#[derive(Template)]
#[template(path = "settings.html")]
struct IndexPage {
    css: &'static str,
    nav: Option<Nav>,
    title: String,
    /// The pages that hold their own settings.
    own: Vec<Link>,
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
    lodestar: Option<String>,
    settings: SettingsBlock,
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

/// The pages that hold their own settings, in the menu's order:
/// Communities, whose own forms are there, and every page live as a home.
fn own_pages() -> Vec<Link> {
    MENU.iter()
        .flat_map(|(_, ids)| ids.iter())
        .filter(|id| **id == PageId::Communities || SETTINGS_LIVE.contains(id))
        .map(|id| Link { href: id.page().href.to_string(), label: id.title() })
        .collect()
}

/// The groups with settings still on their page, each summed up by the
/// titles of those settings.
fn groups_left(ctx: &SettingsContext) -> Vec<GroupLink> {
    let networks = ctx.networks();
    Group::ALL
        .into_iter()
        .filter_map(|g| {
            let titles: Vec<String> = view::sections_at(Place::Group(g), &networks).into_iter().map(|s| s.title).collect();
            (!titles.is_empty()).then(|| GroupLink { slug: g.slug(), title: g.title(), summary: titles.join(", ") })
        })
        .collect()
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
    render(
        StatusCode::OK,
        &IndexPage {
            css: css(),
            nav: Some(Nav::page(&op, PageId::Settings)),
            title: "All settings".to_string(),
            own: own_pages(),
            groups: groups_left(ctx),
            aside,
            waiting,
        },
    )
}

/// The settings group an old page slug names: one of
/// [`crate::admin::menu::OLD_GROUP_SLUGS`], else `None`.
fn old_group(slug: Result<Path<String>, PathRejection>) -> Option<Group> {
    let Ok(Path(slug)) = slug else {
        return None;
    };
    menu::OLD_GROUP_SLUGS.contains(&slug.as_str()).then(|| Group::from_slug(&slug)).flatten()
}

/// A settings page's query.
#[derive(Debug, Default, serde::Deserialize)]
pub struct OpenQuery {
    /// `advanced` opens the Advanced fold.
    pub open: Option<String>,
}

/// `GET /admin/settings/{group}`: a group's page, with the settings whose
/// feature page does not hold them yet; a group with none left answers
/// `308` to the page that holds most of them, without a session. Any other
/// slug, or a segment that is not text, is the plain 404 page, without a
/// session.
pub async fn group(
    State(state): State<AdminState>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    slug: Result<Path<String>, PathRejection>,
    RawQuery(raw): RawQuery,
    q: Result<Query<OpenQuery>, QueryRejection>,
) -> Response {
    if let Ok(Path(s)) = &slug
        && let Some((to, fragment)) = menu::group_redirect(s)
    {
        return crate::admin::moved_to(to, raw.as_deref(), fragment).await;
    }
    let Some(group) = old_group(slug) else {
        return super::not_found().await;
    };
    let op = match require(&state, &headers, peer, Need::Page, None).await {
        Ok(op) => op,
        Err(response) => return response,
    };
    let ctx = &state.settings;
    let sections = view::sections_at(Place::Group(group), &ctx.networks());
    let open = q.is_ok_and(|Query(q)| q.open.as_deref() == Some("advanced"));
    let settings = match view::settings_block(&state, &op.csrf, &sections, open).await {
        Ok(b) => b,
        Err(e) => return super::internal_error(&state, &e),
    };
    let lodestar = (group == Group::Lookups).then(|| match &ctx.lodestar {
        None => "Lodestar is off: no address was set when Prism started.".to_string(),
        Some(l) if l.gate().is_open() => format!("Lodestar ({}) is answering and up to date, so Prism is reading from it.", l.host()),
        Some(l) => format!("Lodestar ({}) is not answering or is behind, so Prism is reading from the PLC directory.", l.host()),
    });
    render(
        StatusCode::OK,
        &GroupPage { css: css(), nav: Some(Nav::under(&op, PageId::Settings)), title: group.title().to_string(), lodestar, settings },
    )
}

/// The name of the page at `back`, for a "Back to …" link.
fn back_label(back: &str) -> String {
    let path = back.split_once('?').map_or(back, |(p, _)| p);
    if let Some(g) = path.strip_prefix("/admin/settings/").and_then(Group::from_slug) {
        return format!("Back to {}", g.title());
    }
    match menu::locate(back) {
        Some((id, _, _)) => format!("Back to {}", id.page().name),
        None => "Back to the settings".to_string(),
    }
}

fn refused(state: &AdminState, op: &Operator, back: &str, text: &str) -> Response {
    message(state, Some(Nav::at(op, back)), StatusCode::BAD_REQUEST, "Not saved", text, Some((back, &back_label(back))))
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

/// The confirmation of typed changes, carrying the whole form again, with
/// Cancel going to `back`.
#[allow(clippy::too_many_arguments)]
fn confirm_typed(op: &Operator, back: String, action: &str, pairs: &[(String, String)], what: String, word: String, error: Option<String>) -> ConfirmPage {
    ConfirmPage {
        css: css(),
        nav: Some(Nav::at(op, &back)),
        back,
        title: "Confirm the change".to_string(),
        what,
        undo: "You can change it back later.".to_string(),
        action: action.to_string(),
        csrf: op.csrf.clone(),
        hidden: pairs
            .iter()
            .filter(|(k, _)| !matches!(k.as_str(), "csrf" | "confirm" | "typed" | "back"))
            .map(|(name, value)| Hidden { name: name.clone(), value: value.clone() })
            .collect(),
        typed: Some(word),
        button: "Save".to_string(),
        error,
    }
}

/// `POST /admin/settings/save`: one `settings set` with every field the
/// form changed, whichever page the form was on; typed changes are
/// confirmed first. Settings with forms of their own are refused.
pub async fn save(State(state): State<AdminState>, ConnectInfo(peer): ConnectInfo<SocketAddr>, headers: HeaderMap, Form(pairs): Form<Vec<(String, String)>>) -> Response {
    save_form(&state, peer, &headers, &pairs).await
}

/// `POST /admin/settings/{group}`: the save, kept at the old group pages'
/// address for pages loaded before they moved. Only the old groups' slugs
/// answer; any other is the plain 404 page.
pub async fn save_at_group(
    State(state): State<AdminState>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    slug: Result<Path<String>, PathRejection>,
    Form(pairs): Form<Vec<(String, String)>>,
) -> Response {
    if old_group(slug).is_none() {
        return super::not_found().await;
    }
    save_form(&state, peer, &headers, &pairs).await
}

async fn save_form(state: &AdminState, peer: SocketAddr, headers: &HeaderMap, pairs: &[(String, String)]) -> Response {
    let op = match require(state, headers, peer, Need::Change, first(pairs, "csrf")).await {
        Ok(op) => op,
        Err(response) => return response,
    };
    let rows = match saved_rows(state).await {
        Ok(r) => r,
        Err(e) => return super::internal_error(state, &e),
    };
    let ctx = &state.settings;
    let rctx = ctx.registry();
    let sections = registry::sections(&rctx.networks);
    let named = |s: &registry::Section| s.fields.iter().any(|f| first(pairs, &f.key).is_some());
    // A setting with forms of its own is never saved here.
    if let Some(own) = sections.iter().find(|s| s.own_form && named(s)) {
        let back = back_to(state, headers, first(pairs, "back"), &view::place(own).path());
        return refused(state, &op, &back, "That setting is changed on its own page.");
    }
    let (mut changes, mut problems, mut words) = (Vec::new(), Vec::new(), Vec::new());
    let mut whats = Vec::new();
    // Where Back and Cancel go without a `back`: the page of the first
    // setting changed, else of the first the form named.
    let (mut changed_at, mut named_at) = (None, None);
    for section in sections.iter().filter(|s| !s.own_form) {
        for f in &section.fields {
            let Some(text) = first(pairs, &f.key) else {
                continue;
            };
            named_at.get_or_insert_with(|| view::place(section).path());
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
            changed_at.get_or_insert_with(|| view::place(section).path());
            if let Some(word) = registry::typed_word(f, &current, &value) {
                words.push(word);
                whats.push(typed_what(&f.key));
            }
            let base_version = first(pairs, &format!("version:{}", f.key)).and_then(|v| v.parse().ok()).unwrap_or(0);
            changes.push(Change { key: f.key.clone(), value: Some(value), base_version });
        }
    }
    let fallback = changed_at.or(named_at).filter(|_| !problems.is_empty() || !changes.is_empty()).unwrap_or_else(|| PageId::Settings.page().href.to_string());
    let back = back_to(state, headers, first(pairs, "back"), &fallback);
    if !problems.is_empty() {
        return refused(state, &op, &back, &format!("Nothing was saved. {}", problems.join(" ")));
    }
    if changes.is_empty() {
        return message(state, Some(Nav::at(&op, &back)), StatusCode::OK, "Nothing changed", "The form had no changes to save.", Some((&back, &back_label(&back))));
    }
    if !words.is_empty() {
        let word = words.join(" ");
        let typed = first(pairs, "typed");
        if first(pairs, "confirm") != Some("yes") || !typed_matches(&word, typed) {
            let error = (first(pairs, "confirm") == Some("yes")).then(|| format!("Type {word} exactly to confirm."));
            let status = if error.is_some() { StatusCode::BAD_REQUEST } else { StatusCode::OK };
            return render(status, &confirm_typed(&op, back, "/admin/settings/save", pairs, whats.join(" "), word, error));
        }
    }
    enqueue(state, &op, &AdminCommand::SettingsSet { changes }).await
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
    /// The page the form was on, for Cancel and Back.
    pub back: Option<String>,
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
    let Some((section, field)) = registry::lookup(&form.key, &ctx.networks()) else {
        let back = back_to(&state, &headers, form.back.as_deref(), "/admin/settings");
        return refused(&state, &op, &back, "That is not a setting that can be changed here.");
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
            // Cancel goes back to the page the reset was asked from, carried
            // through a wrong typed word; else to where the setting is.
            let cancel = back_to(&state, &headers, form.back.as_deref(), &view::place(&section).path());
            return render(status, &confirm_typed(&op, cancel, "/admin/settings/reset", &pairs, typed_what(&form.key).to_string(), word.to_string(), error));
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
        let back = back_to(&state, &headers, form.back.as_deref(), "/admin/settings");
        return refused(&state, &op, &back, "That setting has no saved value any more.");
    };
    let change = Change { key: form.key.clone(), value: Some(row.value.clone()), base_version: form.version };
    enqueue(&state, &op, &AdminCommand::SettingsSet { changes: vec![change] }).await
}

/// The confirmation of a restart.
#[derive(Debug, serde::Deserialize)]
pub struct RestartForm {
    #[serde(default)]
    pub csrf: String,
    pub confirm: Option<String>,
    /// The page the form was on, for Cancel.
    pub back: Option<String>,
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
            Some(Nav::at(&op, "/admin/settings")),
            StatusCode::CONFLICT,
            "Cannot restart here",
            "Prism cannot restart itself on this server; ask whoever runs it to restart Prism.",
            Some(("/admin/settings", "All settings")),
        );
    }
    if form.confirm.as_deref() != Some("yes") {
        let back = back_to(&state, &headers, form.back.as_deref(), "/admin/settings");
        return render(
            StatusCode::OK,
            &ConfirmPage {
                css: css(),
                nav: Some(Nav::at(&op, &back)),
                back,
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
            nav: Some(Nav::under(&op, PageId::Settings)),
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
            nav: Some(Nav::under(&op, PageId::Settings)),
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
