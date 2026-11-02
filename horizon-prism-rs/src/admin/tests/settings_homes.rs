//! Settings on their feature pages over HTTP: every key renders on exactly
//! one page; Members holds its settings, with the Advanced fold; the one
//! save, reset and keep work from it and from the old group address; the
//! command page leads back; All settings lists where everything is; Look up
//! shows an account's Refract state.

use serde_json::{Value, json};

use super::*;
use crate::admin::menu::{PAGES, TABS};
use crate::admin::settings::registry::{self, Group};
use crate::admin::settings::view::{self, Place};

type Queued = (String, Value);

async fn queued(pool: &PgPool) -> Vec<Queued> {
    sqlx::query_as("SELECT command, args FROM admin_commands ORDER BY id").fetch_all(pool).await.expect("commands")
}

/// A page past the ten-pages-a-second limit.
async fn get(b: &Browser, url: &str) -> Page {
    tokio::time::sleep(Duration::from_millis(120)).await;
    b.get(url).await
}

/// A change past the one-a-second limit.
async fn post(b: &Browser, url: &str, form: &[(&str, &str)]) -> Page {
    tokio::time::sleep(Duration::from_millis(1100)).await;
    b.post(url, form).await
}

/// A change with a `Referer`.
async fn post_from(b: &Browser, url: &str, form: &[(&str, &str)], referer: &str) -> Page {
    tokio::time::sleep(Duration::from_millis(1100)).await;
    b.send(b.http.post(url).header("referer", referer).form(form)).await
}

fn location(page: &Page) -> &str {
    page.headers.get(LOCATION).and_then(|v| v.to_str().ok()).unwrap_or_default()
}

/// The `href` of a page's "Back…" or "Cancel" link.
fn link_before(body: &str, text: &str) -> String {
    let at = body.find(&format!("\">{text}")).unwrap_or_else(|| panic!("no {text} link: {body}"));
    let start = body[..at].rfind("href=\"").expect("href") + "href=\"".len();
    body[start..at].to_string()
}

/// Whether the element named `name="{key}"` sits inside the Advanced fold.
fn folded(body: &str, key: &str) -> bool {
    let at = body.find(&format!("name=\"{key}\"")).expect("field");
    body[..at].rfind("<details class=\"advanced\"").is_some_and(|open| !body[open..at].contains("</details>"))
}

/// A card's markup, `id="s-{slug}"` to its end.
fn card<'a>(body: &'a str, slug: &str) -> &'a str {
    let start = body.find(&format!("id=\"s-{slug}\"")).unwrap_or_else(|| panic!("no card {slug}"));
    let end = body[start..].find("technical name").map_or(body.len(), |e| start + e);
    &body[start..end]
}

/// Every key is on exactly one page: on its home once that page holds its
/// settings, with forms of its own on Communities, and else on its old
/// group page; a folded key sits inside the Advanced fold.
#[tokio::test]
async fn every_key_renders_on_one_page() {
    let Some(h) = harness("admin_homes_one_page").await else {
        return;
    };
    let alice = complete_setup(&h, ALICE).await;
    let url = |path: &str| format!("{}{path}", h.base);
    let networks = crate::config::FileConfig::default().constellation.networks.iter().map(|n| n.name.clone()).collect::<Vec<_>>();
    let mut pages: Vec<(String, String)> = Vec::new();
    let mut paths: Vec<String> = PAGES.iter().filter(|p| p.id.built()).map(|p| p.href.to_string()).collect();
    for t in TABS {
        if !paths.iter().any(|p| p == t.href()) {
            paths.push(t.href().to_string());
        }
    }
    for g in Group::ALL {
        // A group with settings left is its page; one with none redirects.
        let path = format!("/admin/settings/{}", g.slug());
        if view::sections_at(Place::Group(g), &networks).is_empty() {
            assert_eq!(get(&alice, &url(&path)).await.status, 308, "{path}");
        } else {
            paths.push(path);
        }
    }
    for path in paths {
        let page = get(&alice, &url(&path)).await;
        assert_eq!(page.status, 200, "{path}");
        pages.push((path, page.body));
    }
    for s in registry::sections(&networks) {
        let want = view::place(&s).path();
        for f in &s.fields {
            let marker = match (s.own_form, f.key.as_str()) {
                (true, "refract.enabled") => "id=\"refract-switch\"".to_string(),
                (true, "refract.communities") => "id=\"refract-communities\"".to_string(),
                (true, other) => panic!("{other} has a form of its own but no marker"),
                (false, key) => format!("name=\"{key}\""),
            };
            let on: Vec<&str> = pages.iter().filter(|(_, body)| body.contains(&marker)).map(|(p, _)| p.as_str()).collect();
            assert_eq!(on, [want.as_str()], "{}", f.key);
            if s.own_form {
                assert!(!pages.iter().any(|(_, body)| body.contains(&format!("name=\"{}\"", f.key))), "{} is in a settings block", f.key);
                continue;
            }
            let body = &pages.iter().find(|(p, _)| *p == want).expect("page").1;
            assert_eq!(folded(body, &f.key), s.advanced, "{} on {want}", f.key);
        }
    }
    h.db.drop_db().await;
}

/// Members holds auto-join and the member limit: each card says whether it
/// applies at once or after a restart, the rarely changed ones are folded,
/// and the fold opens when asked or when a folded setting has news.
#[tokio::test]
async fn members_holds_its_settings() {
    let Some(h) = harness("admin_homes_members").await else {
        return;
    };
    let p = h.pool();
    let alice = complete_setup(&h, ALICE).await;
    let url = |path: &str| format!("{}{path}", h.base);
    let members = get(&alice, &url("/admin/members")).await;
    let body = &members.body;
    assert!(body.contains("<form id=\"settings\" class=\"stack\" method=\"post\" action=\"/admin/settings/save\">"), "{body}");
    assert!(body.contains("<h2>Member limit</h2>") && body.contains("Auto-join stops once Prism has this many members."), "{body}");
    assert!(body.contains("<span class=\"l\">Member limit</span>"), "{body}");
    assert!(!body.contains("Most members") && !body.contains("Limit for auto-join"), "{body}");
    // The fold is closed until asked for.
    assert!(body.contains("<details class=\"advanced\" id=\"advanced\"><summary>Advanced</summary>"), "{body}");
    let opened = get(&alice, &url("/admin/members?open=advanced")).await;
    assert!(opened.body.contains("<details class=\"advanced\" id=\"advanced\" open>"), "{}", opened.body);
    // Each card's pill.
    let networks: Vec<String> = Vec::new();
    for s in view::sections_at(Place::Page(crate::admin::menu::PageId::Members), &networks) {
        let pill = if s.live { "<span class=\"pill on\">Applies at once</span>" } else { "<span class=\"pill\">Applies after a restart</span>" };
        assert!(card(body, &s.slug()).contains(pill), "{}", s.title);
    }
    // A folded restart setting waiting for one: shown, and the fold opens.
    sqlx::query("INSERT INTO operator_settings (key, value, updated_by) VALUES ('community.reindex_retry_secs', '99', 'cli')").execute(p).await.expect("row");
    let waiting = get(&alice, &url("/admin/members")).await;
    assert!(card(&waiting.body, "community-reindex_retry_secs").contains("In use until a restart"), "{}", waiting.body);
    assert!(waiting.body.contains("<details class=\"advanced\" id=\"advanced\" open>"), "{}", waiting.body);
    sqlx::query("DELETE FROM operator_settings").execute(p).await.expect("clear");
    // A folded value set aside opens it too, and so does a changed app.toml.
    sqlx::query("INSERT INTO operator_settings (key, value, updated_by, set_aside) VALUES ('community.never_ready_expiry_days', '5', 'cli', '{\"problem\": \"no\"}')")
        .execute(p)
        .await
        .expect("row");
    assert!(get(&alice, &url("/admin/members")).await.body.contains("id=\"advanced\" open>"));
    sqlx::query("UPDATE operator_settings SET set_aside = NULL, file_notice = true, file_value = '{\"value\": 9, \"explicit\": true}'").execute(p).await.expect("notice");
    let notice = get(&alice, &url("/admin/members")).await;
    assert!(notice.body.contains("id=\"advanced\" open>") && notice.body.contains("Keep my value"), "{}", notice.body);
    h.db.drop_db().await;
}

/// The one save, from Members and from an old group address: a change
/// queues one `settings set`; nothing changed and a bad value say so with
/// Back to the page; a setting with a form of its own is refused; a form
/// holding only a folded setting saves it.
#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn saving_from_members() {
    let Some(h) = harness("admin_homes_save").await else {
        return;
    };
    let p = h.pool();
    let alice = complete_setup(&h, ALICE).await;
    let url = |path: &str| format!("{}{path}", h.base);
    let members = get(&alice, &url("/admin/members")).await;
    let csrf = csrf_of(&members);
    let limit = field(&members.body, "community.max_members").expect("limit");
    assert_eq!(limit, "1000");
    let save = url("/admin/settings/save");
    let back = ("back", "/admin/members");

    // A change: one command, with the form's version.
    let saved = post(&alice, &save, &[("csrf", &csrf), back, ("community.max_members", "4321"), ("version:community.max_members", "0"), ("community.auto_enroll", "off")]).await;
    assert_eq!(saved.status, 303, "{}", saved.body);
    assert!(location(&saved).starts_with("/admin/commands/"), "{}", location(&saved));
    assert_eq!(
        queued(p).await.last(),
        Some(&("settings set".to_string(), json!({"changes": [{"key": "community.max_members", "value": 4321, "base_version": 0}]})))
    );
    let before = queued(p).await.len();
    // Nothing changed: Back to Members, from the form's field or the page
    // the browser came from; with neither, All settings.
    let same = post(&alice, &save, &[("csrf", &csrf), back, ("community.max_members", "1000")]).await;
    assert_eq!(same.status, 200, "{}", same.body);
    assert!(same.body.contains("Nothing changed"), "{}", same.body);
    assert_eq!(link_before(&same.body, "Back to Members"), "/admin/members");
    let referred = post_from(&alice, &save, &[("csrf", &csrf), ("community.max_members", "1000")], &format!("https://{HOST}/admin/members?page=1")).await;
    assert_eq!(link_before(&referred.body, "Back to Members"), "/admin/members?page=1");
    let bare = post(&alice, &save, &[("csrf", &csrf), ("community.max_members", "1000")]).await;
    assert_eq!(link_before(&bare.body, "Back to All settings"), "/admin/settings");
    // A value the form refuses: nothing queued, Back to Members, even
    // without a `back`.
    let bad = post(&alice, &save, &[("csrf", &csrf), ("community.max_members", "lots")]).await;
    assert_eq!(bad.status, 400, "{}", bad.body);
    assert!(bad.body.contains("Not saved") && bad.body.contains("Member limit: enter a whole number"), "{}", bad.body);
    assert_eq!(link_before(&bad.body, "Back to Members"), "/admin/members");
    // A setting with a form of its own is refused whole.
    let own = post(&alice, &save, &[("csrf", &csrf), back, ("refract.enabled", "on"), ("community.max_members", "5")]).await;
    assert_eq!(own.status, 400, "{}", own.body);
    assert!(own.body.contains("That setting is changed on its own page."), "{}", own.body);
    assert_eq!(queued(p).await.len(), before, "nothing queued");
    // A folded setting alone.
    let folded = post(&alice, &save, &[("csrf", &csrf), back, ("community.never_ready_expiry_days", "12"), ("version:community.never_ready_expiry_days", "0")]).await;
    assert_eq!(folded.status, 303, "{}", folded.body);
    assert_eq!(queued(p).await.last().map(|q| q.1.clone()), Some(json!({"changes": [{"key": "community.never_ready_expiry_days", "value": 12, "base_version": 0}]})));
    // The old group address saves the same way, any setting, also once its
    // page redirects.
    assert_eq!(get(&alice, &url("/admin/settings/access")).await.status, 308);
    let old = post(&alice, &url("/admin/settings/access"), &[("csrf", &csrf), ("community.max_members", "1234"), ("version:community.max_members", "0")]).await;
    assert_eq!(old.status, 303, "{}", old.body);
    assert_eq!(queued(p).await.last().map(|q| q.1.clone()), Some(json!({"changes": [{"key": "community.max_members", "value": 1234, "base_version": 0}]})));
    assert_eq!(get(&alice, &url("/admin/settings/blocks")).await.status, 308);
    let old = post(&alice, &url("/admin/settings/blocks"), &[("csrf", &csrf), ("farsight.memory.max_total", "4321"), ("version:farsight.memory.max_total", "0")]).await;
    assert_eq!(old.status, 303, "{}", old.body);
    assert_eq!(queued(p).await.last().map(|q| q.1.clone()), Some(json!({"changes": [{"key": "farsight.memory.max_total", "value": 4321, "base_version": 0}]})));
    h.db.drop_db().await;
}

/// Blocks, Communities and Thread Lens hold their settings, the rarely
/// changed ones folded. Turning the fallback on from Blocks asks ON, keeps
/// Cancel on Blocks through a wrong word and queues on the right one; a
/// typed reset from Blocks asks OFF; keep works from there too.
#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn moderation_pages_hold_their_settings() {
    let Some(h) = harness("admin_homes_moderation").await else {
        return;
    };
    let p = h.pool();
    let alice = complete_setup(&h, ALICE).await;
    let url = |path: &str| format!("{}{path}", h.base);
    let pages: [(&str, &[&str], &[&str]); 3] = [
        (
            "/admin/blocks",
            &["farsight.fallback.enabled", "farsight.fallback.network", "farsight.memory.enabled", "farsight.memory.retention_hours", "farsight.memory.max_per_viewer", "farsight.memory.max_total"],
            &["farsight.rate_per_sec", "farsight.burst"],
        ),
        ("/admin/communities", &["refract.visitor_viewer_state"], &["rate_limits.refract.per_sec", "rate_limits.refract.burst"]),
        ("/admin/thread-lens", &["threadlens.access"], &["rate_limits.thread_lens.per_sec", "rate_limits.thread_lens.burst"]),
    ];
    for (path, everyday, advanced) in pages {
        let page = get(&alice, &url(path)).await;
        assert_eq!(page.status, 200, "{path}: {}", page.body);
        let body = &page.body;
        assert!(body.contains("<form id=\"settings\" class=\"stack\" method=\"post\" action=\"/admin/settings/save\">"), "{path}: {body}");
        assert!(body.contains("<details class=\"advanced\" id=\"advanced\"><summary>Advanced</summary>"), "{path}: {body}");
        for key in everyday {
            assert!(!folded(body, key), "{path}: {key} is folded");
        }
        for key in advanced {
            assert!(folded(body, key), "{path}: {key} is not folded");
        }
        let opened = get(&alice, &url(&format!("{path}?open=advanced"))).await;
        assert!(opened.body.contains("<details class=\"advanced\" id=\"advanced\" open>"), "{path}: {}", opened.body);
    }
    // Health keeps Farsight to one line, which leads to Blocks.
    let health = get(&alice, &url("/admin/health")).await;
    let line = &health.body[health.body.find("<span class=\"l\">Farsight (blocks)</span>").expect("farsight line")..];
    let line = &line[..line.find("</div>").expect("line end")];
    assert!(line.ends_with(" — <a href=\"/admin/blocks\">Blocks</a></span>"), "{line}");
    assert_eq!(health.body.matches("Farsight (blocks)").count(), 1, "{}", health.body);
    let blocks = get(&alice, &url("/admin/blocks")).await;
    assert!(!blocks.body.contains("/admin/settings/blocks") && !blocks.body.contains("Fallback and memory settings"), "{}", blocks.body);
    let csrf = csrf_of(&blocks);
    let save = url("/admin/settings/save");
    let before = queued(p).await.len();

    // The fallback on: asked first, Cancel back to Blocks, then queued.
    let ask = post(&alice, &save, &[("csrf", &csrf), ("back", "/admin/blocks"), ("farsight.fallback.enabled", "on"), ("version:farsight.fallback.enabled", "0")]).await;
    assert_eq!(ask.status, 200, "{}", ask.body);
    assert!(ask.body.contains("action=\"/admin/settings/save\"") && ask.body.contains("<code>ON</code>"), "{}", ask.body);
    assert_eq!(field(&ask.body, "back").as_deref(), Some("/admin/blocks"));
    assert_eq!(field(&ask.body, "farsight.fallback.enabled").as_deref(), Some("on"));
    assert_eq!(link_before(&ask.body, "Cancel"), "/admin/blocks");
    assert_eq!(crate::admin::test_support::current_item(&ask.body), Some(("Blocks".to_string(), "page".to_string())));
    let wrong = post(
        &alice,
        &save,
        &[("csrf", &csrf), ("back", "/admin/blocks"), ("farsight.fallback.enabled", "on"), ("version:farsight.fallback.enabled", "0"), ("confirm", "yes"), ("typed", "nope")],
    )
    .await;
    assert_eq!(wrong.status, 400, "{}", wrong.body);
    assert_eq!(field(&wrong.body, "back").as_deref(), Some("/admin/blocks"));
    assert_eq!(link_before(&wrong.body, "Cancel"), "/admin/blocks");
    assert_eq!(queued(p).await.len(), before, "nothing queued unconfirmed");
    let right = post(
        &alice,
        &save,
        &[("csrf", &csrf), ("back", "/admin/blocks"), ("farsight.fallback.enabled", "on"), ("version:farsight.fallback.enabled", "0"), ("confirm", "yes"), ("typed", "ON")],
    )
    .await;
    assert_eq!(right.status, 303, "{}", right.body);
    assert_eq!(queued(p).await.last().map(|q| q.1.clone()), Some(json!({"changes": [{"key": "farsight.fallback.enabled", "value": true, "base_version": 0}]})));

    // A typed reset from Blocks: the memory saved on, app.toml's off.
    sqlx::query("INSERT INTO operator_settings (key, value, updated_by) VALUES ('farsight.memory.enabled', 'true', 'cli')").execute(p).await.expect("row");
    let blocks = get(&alice, &url("/admin/blocks")).await;
    assert!(blocks.body.contains("<form id=\"reset-farsight.memory.enabled\" method=\"post\" action=\"/admin/settings/reset\">"), "{}", blocks.body);
    let before = queued(p).await.len();
    let reset = post(&alice, &url("/admin/settings/reset"), &[("csrf", &csrf), ("key", "farsight.memory.enabled"), ("version", "1"), ("back", "/admin/blocks")]).await;
    assert_eq!(reset.status, 200, "{}", reset.body);
    assert!(reset.body.contains("<code>OFF</code>"), "{}", reset.body);
    assert_eq!(link_before(&reset.body, "Cancel"), "/admin/blocks");
    assert_eq!(queued(p).await.len(), before, "a typed reset asks first");
    let done = post(&alice, &url("/admin/settings/reset"), &[("csrf", &csrf), ("key", "farsight.memory.enabled"), ("version", "1"), ("confirm", "yes"), ("typed", "OFF")]).await;
    assert_eq!(done.status, 303, "{}", done.body);
    assert_eq!(queued(p).await.last().map(|q| q.1.clone()), Some(json!({"changes": [{"key": "farsight.memory.enabled", "reset": true, "base_version": 1}]})));

    // Keep, from Blocks, once app.toml changed under a saved value.
    sqlx::query("INSERT INTO operator_settings (key, value, updated_by, file_notice, file_value) VALUES ('farsight.memory.retention_hours', '48', 'cli', true, '{\"value\": 12, \"explicit\": true}')")
        .execute(p)
        .await
        .expect("notice");
    let blocks = get(&alice, &url("/admin/blocks")).await;
    assert!(blocks.body.contains("Keep my value"), "{}", blocks.body);
    let home = get(&alice, &url("/admin")).await;
    assert!(home.body.contains("href=\"/admin/blocks#s-farsight-memory-retention_hours\""), "{}", home.body);
    let keep = post(&alice, &url("/admin/settings/keep"), &[("csrf", &csrf), ("key", "farsight.memory.retention_hours"), ("version", "1")]).await;
    assert_eq!(keep.status, 303, "{}", keep.body);
    assert_eq!(queued(p).await.last().map(|q| q.1.clone()), Some(json!({"changes": [{"key": "farsight.memory.retention_hours", "value": 48, "base_version": 1}]})));
    h.db.drop_db().await;
}

/// Indexing and Storage hold their settings, Storage's rarer ones folded.
/// The indexing switch asks OFF, keeps Cancel on Indexing, and queues on the
/// right word, also through its old address; its command page leads back.
/// Health, Jobs and Home keep one line each, leading to the two pages.
#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn data_pages_hold_their_settings() {
    let Some(h) = harness("admin_homes_data").await else {
        return;
    };
    let p = h.pool();
    let alice = complete_setup(&h, ALICE).await;
    let url = |path: &str| format!("{}{path}", h.base);
    let storage = get(&alice, &url("/admin/storage")).await;
    assert_eq!(storage.status, 200, "{}", storage.body);
    let body = &storage.body;
    assert!(body.contains("<form id=\"settings\" class=\"stack\" method=\"post\" action=\"/admin/settings/save\">"), "{body}");
    assert!(body.contains("<details class=\"advanced\" id=\"advanced\"><summary>Advanced</summary>"), "{body}");
    for key in ["storage.budget_bytes", "retention.notification_days", "retention.max_age_days", "retention.run_at_utc", "scope.reply_quote_days", "scope.like_repost_days"] {
        assert!(!folded(body, key), "{key} is folded");
    }
    for key in ["retention.tombstone_days", "scope.rebuild_grace_days", "scope.sweep_max_purges"] {
        assert!(folded(body, key), "{key} is not folded");
    }
    assert!(get(&alice, &url("/admin/storage?open=advanced")).await.body.contains("<details class=\"advanced\" id=\"advanced\" open>"));
    let indexing = get(&alice, &url("/admin/indexing")).await;
    assert_eq!(indexing.status, 200, "{}", indexing.body);
    let body = &indexing.body;
    assert!(body.contains("<form class=\"stack\" method=\"post\" action=\"/admin/indexing/switch\">") && body.contains("technical name: indexing.public_scope"), "{body}");
    assert!(body.contains("<span class=\"l\">Now</span><span class=\"v\">on</span>") && !folded(body, "indexing.fill_max_wait_hours"), "{body}");
    assert!(!body.contains("<details class=\"advanced\"") && !body.contains("Accounts left to refill"), "{body}");
    let csrf = csrf_of(&indexing);

    // The switch to Kryphocron only: asked, Cancel back to Indexing, then
    // queued; the old address takes it too.
    let switch = url("/admin/indexing/switch");
    let before = queued(p).await.len();
    let ask = post_from(&alice, &switch, &[("csrf", &csrf), ("value", "kryphocron_only")], &format!("https://{HOST}/admin/indexing")).await;
    assert_eq!(ask.status, 200, "{}", ask.body);
    assert!(ask.body.contains("<code>OFF</code>") && ask.body.contains("action=\"/admin/indexing/switch\""), "{}", ask.body);
    assert_eq!(link_before(&ask.body, "Cancel"), "/admin/indexing");
    assert_eq!(crate::admin::test_support::current_item(&ask.body), Some(("Indexing".to_string(), "page".to_string())));
    let wrong = post(&alice, &switch, &[("csrf", &csrf), ("value", "kryphocron_only"), ("confirm", "yes"), ("typed", "nope"), ("back", "/admin/indexing")]).await;
    assert_eq!(wrong.status, 400, "{}", wrong.body);
    assert_eq!(link_before(&wrong.body, "Cancel"), "/admin/indexing");
    let bad = post(&alice, &switch, &[("csrf", &csrf), ("value", "everything")]).await;
    assert_eq!(bad.status, 400, "{}", bad.body);
    assert_eq!(link_before(&bad.body, "Indexing"), "/admin/indexing");
    assert_eq!(queued(p).await.len(), before, "nothing queued unconfirmed");
    let right = post(&alice, &switch, &[("csrf", &csrf), ("value", "kryphocron_only"), ("confirm", "yes"), ("typed", "OFF")]).await;
    assert_eq!(right.status, 303, "{}", right.body);
    assert_eq!(queued(p).await.last().cloned(), Some(("indexing public-scope".to_string(), json!({"value": "kryphocron_only"}))));
    let command = get(&alice, &url(location(&right))).await;
    assert!(command.body.contains("Set indexing to Kryphocron only"), "{}", command.body);
    assert!(command.body.contains("<a class=\"button\" href=\"/admin/indexing\">Back to Indexing</a>"), "{}", command.body);
    let old = post(&alice, &url("/admin/settings/public-scope"), &[("csrf", &csrf), ("value", "community")]).await;
    assert_eq!(old.status, 303, "{}", old.body);
    assert_eq!(queued(p).await.last().cloned(), Some(("indexing public-scope".to_string(), json!({"value": "community"}))));

    // Saving from Storage, and through the old group address once it
    // redirects.
    let save = url("/admin/settings/save");
    let same = post(&alice, &save, &[("csrf", &csrf), ("back", "/admin/storage"), ("retention.notification_days", "90")]).await;
    assert!(same.body.contains("Nothing changed"), "{}", same.body);
    assert_eq!(link_before(&same.body, "Back to Storage"), "/admin/storage");
    let saved = post(&alice, &save, &[("csrf", &csrf), ("back", "/admin/storage"), ("scope.sweep_max_purges", "7"), ("version:scope.sweep_max_purges", "0")]).await;
    assert_eq!(saved.status, 303, "{}", saved.body);
    assert_eq!(queued(p).await.last().map(|q| q.1.clone()), Some(json!({"changes": [{"key": "scope.sweep_max_purges", "value": 7, "base_version": 0}]})));
    assert_eq!(get(&alice, &url("/admin/settings/storage")).await.status, 308);
    let old = post(&alice, &url("/admin/settings/storage"), &[("csrf", &csrf), ("retention.notification_days", "12"), ("version:retention.notification_days", "0")]).await;
    assert_eq!(old.status, 303, "{}", old.body);
    assert_eq!(queued(p).await.last().map(|q| q.1.clone()), Some(json!({"changes": [{"key": "retention.notification_days", "value": 12, "base_version": 0}]})));

    // Health: one line each, leading to the page; the rest is on Storage.
    let health = get(&alice, &url("/admin/health")).await;
    for line in [
        "<span class=\"l\">Public data</span><span class=\"v\">on — <a href=\"/admin/indexing\">Indexing</a></span>",
        "<span class=\"l\">Database (of its budget)</span><span class=\"v\">unknown — <a href=\"/admin/storage\">Storage</a></span>",
    ] {
        assert!(health.body.contains(line), "{line}: {}", health.body);
    }
    for gone in ["Account ids used", "Dead rows in posts", "Post records left to compress", "<h3>Stored records</h3>", "Redis memory</span>"] {
        assert!(!health.body.contains(gone), "{gone}: {}", health.body);
    }
    // The jobs' clean-up timing is on Storage.
    let jobs = get(&alice, &url("/admin/jobs")).await;
    assert!(jobs.body.contains("are on <a href=\"/admin/storage\">Storage</a>."), "{}", jobs.body);
    // Home's tiles lead to both.
    let home = get(&alice, &url("/admin")).await;
    for href in ["<a href=\"/admin/indexing\">", "<a href=\"/admin/storage\">"] {
        assert!(home.body.contains(href), "{href}: {}", home.body);
    }

    // While a refill runs: Jobs' line leads to Indexing, which shows the
    // accounts left and when the longest wait ends.
    {
        let mut sets = h.scope.write_guard().await;
        h.scope.set_public(&mut sets, crate::indexing::PublicState { mode: crate::indexing::PublicMode::Filling, generation: 2, last_fill_since: None });
    }
    let jobs = get(&alice, &url("/admin/jobs")).await;
    assert!(jobs.body.contains("Public requests come back when it is done. — <a href=\"/admin/indexing\">Indexing</a></div>"), "{}", jobs.body);
    let filling = get(&alice, &url("/admin/indexing")).await;
    assert!(filling.body.contains("Accounts left to refill") && filling.body.contains("24 hours from when every account is queued"), "{}", filling.body);
    sqlx::query(
        "INSERT INTO worker_state (key, value) VALUES ('public_transition', jsonb_build_object('fill', jsonb_build_object('waiting_since', to_char(now() AT TIME ZONE 'UTC', 'YYYY-MM-DD\"T\"HH24:MI:SS\"Z\"')))) \
         ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value",
    )
    .execute(p)
    .await
    .expect("transition");
    let waiting = get(&alice, &url("/admin/indexing")).await;
    assert!(waiting.body.contains("Waiting for the last accounts since") && waiting.body.contains("(in 24 hours)"), "{}", waiting.body);
    h.db.drop_db().await;
}

/// Reset and keep from Members, a typed reset's Cancel kept through a
/// wrong word, and each file notice on Home leading to the setting.
#[tokio::test]
async fn reset_and_keep_from_members() {
    let Some(h) = harness("admin_homes_reset").await else {
        return;
    };
    let p = h.pool();
    let alice = complete_setup(&h, ALICE).await;
    let url = |path: &str| format!("{}{path}", h.base);
    sqlx::query("INSERT INTO operator_settings (key, value, updated_by) VALUES ('community.max_members', '77', 'cli')").execute(p).await.expect("row");
    let members = get(&alice, &url("/admin/members")).await;
    let csrf = csrf_of(&members);
    assert!(members.body.contains("<form id=\"reset-community.max_members\" method=\"post\" action=\"/admin/settings/reset\">"), "{}", members.body);
    assert!(members.body.contains("form=\"reset-community.max_members\">Use the default (1000 members)</button>"), "{}", members.body);
    let reset = post(&alice, &url("/admin/settings/reset"), &[("csrf", &csrf), ("key", "community.max_members"), ("version", "1")]).await;
    assert_eq!(reset.status, 303, "{}", reset.body);
    assert_eq!(queued(p).await.last().map(|q| q.1.clone()), Some(json!({"changes": [{"key": "community.max_members", "reset": true, "base_version": 1}]})));
    // app.toml changed under it: Home leads to its card, and keep keeps it.
    sqlx::query("UPDATE operator_settings SET file_notice = true, file_value = '{\"value\": 99, \"explicit\": true}' WHERE key = 'community.max_members'").execute(p).await.expect("notice");
    let home = get(&alice, &url("/admin")).await;
    assert!(home.body.contains("href=\"/admin/members#s-community-max_members\""), "{}", home.body);
    let keep = post(&alice, &url("/admin/settings/keep"), &[("csrf", &csrf), ("key", "community.max_members"), ("version", "1")]).await;
    assert_eq!(keep.status, 303, "{}", keep.body);
    assert_eq!(queued(p).await.last().map(|q| q.1.clone()), Some(json!({"changes": [{"key": "community.max_members", "value": 77, "base_version": 1}]})));
    // A folded one opens its fold; a form of its own leads to its form.
    sqlx::query(
        "INSERT INTO operator_settings (key, value, updated_by, file_notice, file_value) VALUES \
         ('community.reindex_retry_secs', '99', 'cli', true, '{\"value\": 5, \"explicit\": true}'), \
         ('refract.communities', '[]', 'cli', true, '{\"value\": [], \"explicit\": true}')",
    )
    .execute(p)
    .await
    .expect("notices");
    let home = get(&alice, &url("/admin")).await;
    assert!(home.body.contains("href=\"/admin/members?open=advanced#s-community-reindex_retry_secs\""), "{}", home.body);
    assert!(home.body.contains("href=\"/admin/communities#refract-communities\""), "{}", home.body);
    // A typed reset asked from Members: Cancel goes back there, also after
    // a wrong word, and the sidebar marks Members.
    sqlx::query("INSERT INTO operator_settings (key, value, updated_by) VALUES ('farsight.memory.enabled', 'true', 'cli')").execute(p).await.expect("row");
    let before = queued(p).await.len();
    let form = [("csrf", csrf.as_str()), ("key", "farsight.memory.enabled"), ("version", "1")];
    let asked = post_from(&alice, &url("/admin/settings/reset"), &form, &format!("https://{HOST}/admin/members")).await;
    assert_eq!(asked.status, 200, "{}", asked.body);
    assert!(asked.body.contains("<code>OFF</code>"), "{}", asked.body);
    assert_eq!(link_before(&asked.body, "Cancel"), "/admin/members");
    assert_eq!(field(&asked.body, "back").as_deref(), Some("/admin/members"));
    assert_eq!(crate::admin::test_support::current_item(&asked.body), Some(("Members".to_string(), "page".to_string())));
    let wrong = post(&alice, &url("/admin/settings/reset"), &[("csrf", &csrf), ("key", "farsight.memory.enabled"), ("version", "1"), ("confirm", "yes"), ("typed", "nope"), ("back", "/admin/members")]).await;
    assert_eq!(wrong.status, 400, "{}", wrong.body);
    assert_eq!(link_before(&wrong.body, "Cancel"), "/admin/members");
    assert_eq!(queued(p).await.len(), before, "an unconfirmed typed reset queues nothing");
    h.db.drop_db().await;
}

/// A settings change's command page leads back to the page of its first
/// setting, its card, and its fold when folded; a stale version is sent as
/// the form had it, for the executor to refuse.
#[tokio::test]
async fn the_command_page_leads_back() {
    let Some(h) = harness("admin_homes_follow").await else {
        return;
    };
    let p = h.pool();
    let alice = complete_setup(&h, ALICE).await;
    let url = |path: &str| format!("{}{path}", h.base);
    sqlx::query("INSERT INTO operator_settings (key, value, version, updated_by) VALUES ('community.max_members', '77', 2, 'cli')").execute(p).await.expect("row");
    let csrf = csrf_of(&get(&alice, &url("/admin/members")).await);
    for (key, value, follow, name) in [
        ("community.max_members", "5", "/admin/members#s-community-max_members", "Back to Members"),
        ("community.reindex_retry_secs", "7", "/admin/members?open=advanced#s-community-reindex_retry_secs", "Back to Members"),
        ("threadlens.access", "members", "/admin/thread-lens#s-threadlens-access", "Back to Thread Lens"),
        ("farsight.burst", "77", "/admin/blocks?open=advanced#s-farsight-rate_per_sec", "Back to Blocks"),
        ("rate_limits.refract.burst", "77", "/admin/communities?open=advanced#s-rate_limits-refract-per_sec", "Back to Communities"),
        ("indexing.fill_max_wait_hours", "5", "/admin/indexing#s-indexing-fill_max_wait_hours", "Back to Indexing"),
        ("retention.notification_days", "9", "/admin/storage#s-retention-notification_days", "Back to Storage"),
        ("scope.sweep_max_purges", "9", "/admin/storage?open=advanced#s-scope-sweep_max_purges", "Back to Storage"),
    ] {
        let version = format!("version:{key}");
        let saved = post(&alice, &url("/admin/settings/save"), &[("csrf", &csrf), (key, value), (&version, "0")]).await;
        assert_eq!(saved.status, 303, "{key}: {}", saved.body);
        assert_eq!(queued(p).await.last().map(|q| q.1["changes"][0]["base_version"].clone()), Some(json!(0)), "{key}: the form's version, stale or not");
        let page = get(&alice, &url(location(&saved))).await;
        assert!(page.body.contains(&format!("<a class=\"button\" href=\"{follow}\">{name}</a>")), "{key}: {}", page.body);
    }
    h.db.drop_db().await;
}

/// All settings lists the pages that hold their settings, and each group
/// with settings left, summed up by those settings alone.
#[tokio::test]
async fn all_settings_lists_where_settings_are() {
    let Some(h) = harness("admin_homes_index").await else {
        return;
    };
    let alice = complete_setup(&h, ALICE).await;
    let index = get(&alice, &format!("{}/admin/settings", h.base)).await;
    let body = &index.body;
    let own = &body[body.find("<h2>On their own pages</h2>").expect("own pages")..];
    let own = &own[..own.find("</div>").expect("card end")];
    let hrefs: Vec<&str> = own.match_indices("href=\"").map(|(i, _)| &own[i + 6..i + 6 + own[i + 6..].find('"').expect("end")]).collect();
    assert_eq!(hrefs, ["/admin/members", "/admin/blocks", "/admin/communities", "/admin/thread-lens", "/admin/indexing", "/admin/storage"], "{own}");
    assert!(own.contains(">Communities · Refract</a>") && own.contains(">Blocks · Farsight</a>"), "{own}");
    let networks: Vec<String> = Vec::new();
    for g in Group::ALL {
        let card = format!("<h2><a href=\"/admin/settings/{}\">{}</a></h2>", g.slug(), g.title());
        assert_eq!(body.contains(&card), !view::sections_at(Place::Group(g), &networks).is_empty(), "{}", g.slug());
    }
    assert!(!body.contains("Who can use Prism") && !body.contains("Blocks (Farsight)") && !body.contains("/admin/settings/blocks"), "{body}");
    assert!(!body.contains("What Prism stores") && !body.contains("/admin/settings/storage"), "{body}");
    // A group that lost some settings names only those left.
    assert!(body.contains("<h2><a href=\"/admin/settings/limits\">Limits</a></h2>"), "{body}");
    let limits = &body[body.find("/admin/settings/limits").expect("limits")..];
    let limits = &limits[..limits.find("</p>").expect("summary")];
    assert!(limits.contains("Visitors not signed in") && !limits.contains("Thread Lens") && !limits.contains("Accounts kept from other communities"), "{limits}");
    h.db.drop_db().await;
}

/// Look up shows an account's Refract state, with its page; an account
/// Refract does not know shows nothing of it.
#[tokio::test]
async fn look_up_shows_refract() {
    let Some(h) = harness("admin_homes_lookup").await else {
        return;
    };
    let alice = complete_setup(&h, ALICE).await;
    sqlx::query("INSERT INTO refract_dids (did, community, pds_host, state, source, since) VALUES ($1, 'community-a', 'pds.community-a.example', 'restored', 'admin', now())")
        .bind(BOB)
        .execute(h.pool())
        .await
        .expect("refract");
    h.identity.set(BOB, PDS);
    h.identity.set(CAROL, PDS);
    let bob = get(&alice, &format!("{}/admin/lookup?q={BOB}", h.base)).await;
    assert_eq!(bob.status, 200, "{}", bob.body);
    let card = &bob.body[bob.body.find("<h3>Communities · Refract</h3>").expect("card")..];
    let card = &card[..card.find("See the account</a>").map_or(card.len(), |e| e + "See the account</a>".len())];
    assert!(card.contains("restored: kept and served") && card.contains("community-a"), "{card}");
    assert!(card.contains(&format!("<a href=\"/admin/communities/accounts/{BOB}\">See the account</a>")), "{card}");
    let carol = get(&alice, &format!("{}/admin/lookup?q={CAROL}", h.base)).await;
    assert_eq!(carol.status, 200, "{}", carol.body);
    assert!(!carol.body.contains("<h3>Communities · Refract</h3>"), "{}", carol.body);
    h.db.drop_db().await;
}
