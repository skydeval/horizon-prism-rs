//! Settings on their feature pages over HTTP: every key renders on exactly
//! one page; Members holds its settings, with the Advanced fold; the one
//! save, reset and keep work from it and from the old group address; the
//! command page leads back; All settings finds every setting and offers the
//! restart; Look up shows an account's Refract state.

use serde_json::{Value, json};

use super::*;
use crate::admin::menu::{MENU, OLD_GROUP_SLUGS, PAGES, PageId, TABS};
use crate::admin::settings::registry;
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

/// Every key is on exactly one page, its home, with forms of its own on
/// Communities; a folded key sits inside the Advanced fold; no old group
/// page is left.
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
    for slug in OLD_GROUP_SLUGS {
        let path = format!("/admin/settings/{slug}");
        assert_eq!(get(&alice, &url(&path)).await.status, 308, "{path}");
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

/// Private posts holds its settings, the timing floors, the retry hints and
/// the membership answer's lifetime folded; it saves from there and through
/// the old group address once that redirects.
#[tokio::test]
async fn kryphocron_page_holds_its_settings() {
    let Some(h) = harness("admin_homes_private").await else {
        return;
    };
    let p = h.pool();
    let alice = complete_setup(&h, ALICE).await;
    let url = |path: &str| format!("{}{path}", h.base);
    let page = get(&alice, &url("/admin/private")).await;
    assert_eq!(page.status, 200, "{}", page.body);
    let body = &page.body;
    assert!(body.contains("<h1>Private posts</h1>") && body.contains("<title>Private posts · kryphocron · Horizon-Prism admin</title>"), "{body}");
    assert!(body.contains("<form id=\"settings\" class=\"stack\" method=\"post\" action=\"/admin/settings/save\">"), "{body}");
    assert!(body.contains("<details class=\"advanced\" id=\"advanced\"><summary>Advanced</summary>"), "{body}");
    for key in ["private_read.max_chain", "sync.head_max_age_secs", "private_read.head_fallback", "private_notify.backfill_max_age_hours", "private_search.max_age_days"] {
        assert!(!folded(body, key), "{key} is folded");
    }
    // The twelve floors and the three others, folded.
    let networks: Vec<String> = Vec::new();
    let mut folds = 0;
    for s in view::sections_at(Place::Page(crate::admin::menu::PageId::Private), &networks) {
        for f in &s.fields {
            assert_eq!(folded(body, &f.key), s.advanced, "{}", f.key);
            folds += usize::from(s.advanced);
            assert_eq!(s.advanced, f.key.starts_with("private_read.floor_ms.") || f.key.starts_with("private_read.degraded_retry_") || f.key == "private_read.membership_valid_for_secs", "{}", f.key);
        }
    }
    assert_eq!(folds, 15);
    // Twenty settings with their facts stay a short page.
    eprintln!("/admin/private: {} bytes of HTML", body.len());
    assert!(body.len() < 150_000, "{} bytes", body.len());
    assert!(get(&alice, &url("/admin/private?open=advanced")).await.body.contains("<details class=\"advanced\" id=\"advanced\" open>"));
    let csrf = csrf_of(&page);

    // Saving from the page: nothing changed leads back to it; a change
    // queues one setting.
    let save = url("/admin/settings/save");
    let depth = field(body, "private_read.max_chain").expect("depth");
    let same = post(&alice, &save, &[("csrf", &csrf), ("back", "/admin/private"), ("private_read.max_chain", &depth)]).await;
    assert!(same.body.contains("Nothing changed"), "{}", same.body);
    assert_eq!(link_before(&same.body, "Back to Private posts"), "/admin/private");
    let saved = post(&alice, &save, &[("csrf", &csrf), ("back", "/admin/private"), ("private_read.max_chain", "12"), ("version:private_read.max_chain", "0")]).await;
    assert_eq!(saved.status, 303, "{}", saved.body);
    assert_eq!(queued(p).await.last().map(|q| q.1.clone()), Some(json!({"changes": [{"key": "private_read.max_chain", "value": 12, "base_version": 0}]})));
    // The old group address redirects to the settings, and still saves.
    let old = get(&alice, &url("/admin/settings/private")).await;
    assert_eq!(old.status, 308);
    assert_eq!(location(&old), "/admin/private#settings");
    let alias = post(&alice, &url("/admin/settings/private"), &[("csrf", &csrf), ("private_read.membership_valid_for_secs", "120"), ("version:private_read.membership_valid_for_secs", "0")]).await;
    assert_eq!(alias.status, 303, "{}", alias.body);
    assert_eq!(queued(p).await.last().map(|q| q.1.clone()), Some(json!({"changes": [{"key": "private_read.membership_valid_for_secs", "value": 120, "base_version": 0}]})));
    h.db.drop_db().await;
}

/// What apps see, Extra services and Public page each hold their settings,
/// none folded; each saves from its page, and the old group addresses
/// redirect to the page itself and still save; Public page links the page
/// while it is on.
#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn apps_pages_hold_their_settings() {
    let mut file = test_support::no_auto_join();
    file.public_page.enabled = false;
    file.services.insert("push".to_string(), crate::config::ServiceConfig { proxy_url: Some("https://push.example.com".to_string()), ..Default::default() });
    let Some(h) = harness_config("admin_homes_apps", quick(), true, health::HealthInputs::default(), file).await else {
        return;
    };
    let p = h.pool();
    let alice = complete_setup(&h, ALICE).await;
    let url = |path: &str| format!("{}{path}", h.base);
    let networks: Vec<String> = Vec::new();
    for (id, keys) in [(PageId::Apps, 11), (PageId::Services, 3), (PageId::PublicPage, 2)] {
        let path = id.page().href;
        let page = get(&alice, &url(path)).await;
        assert_eq!(page.status, 200, "{path}: {}", page.body);
        let body = &page.body;
        assert!(body.contains(&format!("<h1>{}</h1>", id.page().name)), "{path}: {body}");
        assert!(body.contains("<form id=\"settings\" class=\"stack\" method=\"post\" action=\"/admin/settings/save\">"), "{path}: {body}");
        assert!(!body.contains("<details class=\"advanced\""), "{path}: nothing is folded");
        // Only Public page holds more than its settings.
        assert_eq!(body.contains("<h2>Settings</h2>"), id == PageId::PublicPage, "{path}");
        let fields: Vec<String> = view::sections_at(Place::Page(id), &networks).iter().flat_map(|s| s.fields.iter().map(|f| f.key.clone())).collect();
        assert_eq!(fields.len(), keys, "{path}");
        for key in &fields {
            assert!(body.contains(&format!("name=\"{key}\"")), "{path}: {key}");
        }
    }
    // The public page is off: no link to it.
    let public = get(&alice, &url("/admin/public-page")).await;
    assert!(!public.body.contains("View it"), "{}", public.body);
    let csrf = csrf_of(&public);

    // Saving from each page: nothing changed leads back to it; a change
    // queues one setting.
    let save = url("/admin/settings/save");
    for (back, key, same, new, value, name) in [
        ("/admin/apps", "bookmarks.count_floor", "10", "3", json!(3), "Back to What apps see"),
        ("/admin/services", "services.push.state", "disabled", "proxy", json!("proxy"), "Back to Extra services"),
        ("/admin/public-page", "public_page.name", "", "Our server", json!("Our server"), "Back to Public page"),
    ] {
        let version = format!("version:{key}");
        let unchanged = post(&alice, &save, &[("csrf", &csrf), ("back", back), (key, same)]).await;
        assert!(unchanged.body.contains("Nothing changed"), "{key}: {}", unchanged.body);
        assert_eq!(link_before(&unchanged.body, name), back);
        let saved = post(&alice, &save, &[("csrf", &csrf), ("back", back), (key, new), (&version, "0")]).await;
        assert_eq!(saved.status, 303, "{key}: {}", saved.body);
        assert_eq!(queued(p).await.last().map(|q| q.1.clone()), Some(json!({"changes": [{"key": key, "value": value, "base_version": 0}]})), "{key}");
    }
    // The old group addresses go to the pages themselves, and still save.
    for (slug, to, key, value, want) in [
        ("apps", "/admin/apps", "video.playback", "pds_blob", json!("pds_blob")),
        ("apps", "/admin/apps", "public_page.enabled", "on", json!(true)),
        ("services", "/admin/services", "services.push.state", "proxy", json!("proxy")),
    ] {
        let old = get(&alice, &url(&format!("/admin/settings/{slug}"))).await;
        assert_eq!(old.status, 308, "{slug}");
        assert_eq!(location(&old), to);
        let version = format!("version:{key}");
        let alias = post(&alice, &url(&format!("/admin/settings/{slug}")), &[("csrf", &csrf), (key, value), (&version, "0")]).await;
        assert_eq!(alias.status, 303, "{slug} {key}: {}", alias.body);
        assert_eq!(queued(p).await.last().map(|q| q.1.clone()), Some(json!({"changes": [{"key": key, "value": want, "base_version": 0}]})), "{key}");
    }

    // On, the page links to the public page itself.
    h.live.update(|v| v.public_page_enabled = true);
    let on = get(&alice, &url("/admin/public-page")).await;
    assert!(on.body.contains(&format!("<a class=\"button secondary\" href=\"https://{HOST}/\">View it</a>")), "{}", on.body);
    h.db.drop_db().await;
}

/// Request limits is one table: a row per class with its two numbers, each
/// saved alone and reset alone; Thread Lens's and Refract's limits are on
/// their own pages. Account lookups holds lodestar's state, counts and
/// settings, its rarer ones folded; Profiles holds its settings. The old
/// group addresses redirect there and still save.
#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn network_pages_hold_their_settings() {
    let Some(h) = harness("admin_homes_network").await else {
        return;
    };
    let p = h.pool();
    let alice = complete_setup(&h, ALICE).await;
    let url = |path: &str| format!("{}{path}", h.base);
    let limits = get(&alice, &url("/admin/limits")).await;
    assert_eq!(limits.status, 200, "{}", limits.body);
    let body = &limits.body;
    assert!(body.contains("<h1>Request limits</h1>") && !body.contains("<h2>Settings</h2>"), "{body}");
    assert!(body.contains("<form id=\"settings\" class=\"stack\" method=\"post\" action=\"/admin/settings/save\">"), "{body}");
    assert!(body.contains("<div class=\"scroll\">\n<table class=\"limits-table\">") && !body.contains("<details class=\"advanced\""), "{body}");
    let tables = crate::admin::test_support::html::tables(body);
    assert_eq!(tables.len(), 1);
    assert_eq!(tables[0].rows.len(), 11, "one row per class");
    assert_eq!(body.matches("<input type=\"text\" id=\"rate_limits.").count(), 22, "{body}");
    for class in ["thread_lens", "refract"] {
        assert!(!body.contains(&format!("name=\"rate_limits.{class}.")), "{class} is on its own page");
    }
    assert!(body.contains("<span class=\"pill\">Applies after a restart</span>") && body.contains("<summary>What each limits</summary>"), "{body}");
    assert!(!body.contains("class=\"secondary reset\""), "nothing saved, nothing to reset");
    let csrf = csrf_of(&limits);
    // One cell: one key queued.
    let save = url("/admin/settings/save");
    let saved = post(&alice, &save, &[("csrf", &csrf), ("back", "/admin/limits"), ("rate_limits.global.burst", "77"), ("version:rate_limits.global.burst", "0"), ("rate_limits.global.per_sec", &field(body, "rate_limits.global.per_sec").expect("rate"))]).await;
    assert_eq!(saved.status, 303, "{}", saved.body);
    assert_eq!(queued(p).await.last().map(|q| q.1.clone()), Some(json!({"changes": [{"key": "rate_limits.global.burst", "value": 77, "base_version": 0}]})));
    // A saved cell: its own Reset, its row's state, and a reset of it alone.
    sqlx::query("INSERT INTO operator_settings (key, value, updated_by) VALUES ('rate_limits.global.burst', '77', 'cli')").execute(p).await.expect("row");
    let saved = get(&alice, &url("/admin/limits")).await;
    let row = &saved.body[saved.body.find("<tr id=\"s-rate_limits-global-per_sec\">").expect("row")..];
    let row = &row[..row.find("</tr>").expect("row end")];
    assert_eq!(row.matches("class=\"secondary reset\"").count(), 1, "{row}");
    assert!(row.contains("form=\"reset-rate_limits.global.burst\">Reset</button>") && row.contains("saved by the command line"), "{row}");
    assert!(row.contains("<span class=\"pill amber\">Waits for a restart</span>"), "{row}");
    assert_eq!(saved.body.matches("class=\"secondary reset\"").count(), 1, "{}", saved.body);
    let reset = post(&alice, &url("/admin/settings/reset"), &[("csrf", &csrf), ("key", "rate_limits.global.burst"), ("version", "1")]).await;
    assert_eq!(reset.status, 303, "{}", reset.body);
    assert_eq!(queued(p).await.last().map(|q| q.1.clone()), Some(json!({"changes": [{"key": "rate_limits.global.burst", "reset": true, "base_version": 1}]})));

    // Account lookups: lodestar off here, its counts, its settings.
    let lookups = get(&alice, &url("/admin/account-lookups")).await;
    assert_eq!(lookups.status, 200, "{}", lookups.body);
    let body = &lookups.body;
    assert!(body.contains("<title>Account lookups · lodestar · Horizon-Prism admin</title>") && body.contains("<h1>Account lookups</h1>"), "{body}");
    assert!(body.contains("<div class=\"banner\">Lodestar is off: no address was set when Prism started.</div>"), "{body}");
    assert!(body.contains("not set up; Prism asks the PLC directory"), "{body}");
    assert!(!folded(body, "identity.lodestar_url"), "{body}");
    for key in ["identity.lodestar_request_rate_per_sec", "identity.lodestar_request_burst", "identity.lodestar_timeout_ms"] {
        assert!(folded(body, key), "{key} is not folded");
    }
    assert!(get(&alice, &url("/admin/account-lookups?open=advanced")).await.body.contains("<details class=\"advanced\" id=\"advanced\" open>"));
    // Profiles: its settings alone.
    let profiles = get(&alice, &url("/admin/profiles")).await;
    assert_eq!(profiles.status, 200, "{}", profiles.body);
    for key in ["cache.profile_ttl_secs", "cache.profile_negative_ttl_secs", "cache.profile_refresh_max"] {
        assert!(profiles.body.contains(&format!("name=\"{key}\"")) && !folded(&profiles.body, key), "{key}");
    }
    // The old group addresses redirect, and still save.
    for (slug, to, key, value, want) in [
        ("limits", "/admin/limits", "rate_limits.write.burst", "9", json!(9)),
        ("profiles", "/admin/profiles", "cache.profile_refresh_max", "9", json!(9)),
        ("lookups", "/admin/account-lookups#settings", "identity.lodestar_timeout_ms", "777", json!(777)),
    ] {
        let old = get(&alice, &url(&format!("/admin/settings/{slug}"))).await;
        assert_eq!(old.status, 308, "{slug}");
        assert_eq!(location(&old), to);
        let version = format!("version:{key}");
        let alias = post(&alice, &url(&format!("/admin/settings/{slug}")), &[("csrf", &csrf), (key, value), (&version, "0")]).await;
        assert_eq!(alias.status, 303, "{slug} {key}: {}", alias.body);
        assert_eq!(queued(p).await.last().map(|q| q.1.clone()), Some(json!({"changes": [{"key": key, "value": want, "base_version": 0}]})), "{key}");
    }
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
        ("private_read.max_chain", "9", "/admin/private#s-private_read-max_chain", "Back to Private posts"),
        ("private_read.degraded_retry_max_secs", "90", "/admin/private?open=advanced#s-private_read-degraded_retry_min_secs", "Back to Private posts"),
        ("bookmarks.count_floor", "4", "/admin/apps#s-bookmarks-emit_count", "Back to What apps see"),
        ("public_page.name", "Our server", "/admin/public-page#s-public_page-name", "Back to Public page"),
        ("identity.lodestar_url", "https://lodestar.example", "/admin/account-lookups#s-identity-lodestar_url", "Back to Account lookups"),
        ("identity.lodestar_timeout_ms", "900", "/admin/account-lookups?open=advanced#s-identity-lodestar_timeout_ms", "Back to Account lookups"),
        ("cache.profile_refresh_max", "9", "/admin/profiles#s-cache-profile_refresh_max", "Back to Profiles and caching"),
        ("rate_limits.global.burst", "9", "/admin/limits#s-rate_limits-global-per_sec", "Back to Request limits"),
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

/// The search table's rows: each setting's link, technical name and
/// whether it is folded under Advanced.
fn found(body: &str) -> Vec<(String, String, bool)> {
    let Some(start) = body.find("<h2 id=\"found\">") else {
        return Vec::new();
    };
    let table = &body[start..];
    let table = &table[..table.find("</table>").expect("table end")];
    table
        .split("<tr><td")
        .skip(1)
        .map(|row| {
            let href = &row[row.find("href=\"").expect("link") + 6..];
            let href = &href[..href.find('"').expect("link end")];
            let key = &row[row.find("<code>").expect("key") + 6..row.find("</code>").expect("key end")];
            (href.to_string(), key.to_string(), row.contains("<td class=\"state\">Advanced</td>"))
        })
        .collect()
}

/// One list of All settings, from its heading to its table's end.
fn list<'a>(body: &'a str, id: &str) -> &'a str {
    let start = body.find(&format!("<h2 id=\"{id}\">")).unwrap_or_else(|| panic!("no list {id}: {body}"));
    let end = body[start..].find("</table>").map_or(body.len(), |e| start + e);
    &body[start..end]
}

/// All settings: every setting once, by page in the menu's order, each
/// linking where it is changed; the search by words, in any case, and by
/// technical name; the three lists, each linking to its setting; the
/// restart card without a supervisor; and no script.
#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn all_settings_searches_every_setting() {
    let Some(h) = harness("admin_homes_index").await else {
        return;
    };
    let alice = complete_setup(&h, ALICE).await;
    let p = h.pool();
    let url = |q: &str| format!("{}/admin/settings{q}", h.base);
    let ctx = crate::admin::settings::SettingsContext::from_file(test_support::no_auto_join());
    let menu_at = |href: &str| {
        let path = href.split(['?', '#']).next().unwrap_or_default();
        MENU.iter().flat_map(|(_, ids)| ids.iter()).position(|id| id.page().href == path).unwrap_or_else(|| panic!("{href} is no menu page"))
    };

    // No words: every key exactly once, and the public-data switch.
    let index = get(&alice, &url("")).await;
    assert_eq!(index.status, 200, "{}", index.body);
    assert!(index.body.contains("<h2 id=\"found\">Every setting</h2>"), "{}", index.body);
    let all = found(&index.body);
    let mut keys: Vec<String> = all.iter().map(|r| r.1.clone()).collect();
    let mut want = registry::keys(&ctx.networks());
    want.push("indexing.public_scope".to_string());
    assert_eq!(keys.len(), want.len(), "{keys:?}");
    keys.sort();
    want.sort();
    assert_eq!(keys, want);
    // A network's own keys are there, by their technical names.
    assert!(ctx.networks().iter().filter(|n| n.as_str() != crate::config::COMMUNITY_NETWORK).all(|n| all.iter().any(|r| r.1 == format!("constellation.networks[{n}].background_burst"))));
    // Each links where it is changed, the folded ones with the fold open.
    for (link, key, advanced) in &all {
        if key == "indexing.public_scope" {
            assert_eq!(link, "/admin/indexing#public-scope");
            assert!(!advanced);
            continue;
        }
        assert_eq!(*link, view::href(key, &ctx), "{key}");
        let (section, _) = registry::lookup(key, &ctx.networks()).expect("key");
        assert_eq!(*advanced, section.advanced, "{key}");
        assert_eq!(link.contains("?open=advanced"), section.advanced, "{key}");
    }
    // By page, in the menu's order.
    let order: Vec<usize> = all.iter().map(|r| menu_at(&r.0)).collect();
    assert!(order.windows(2).all(|w| w[0] <= w[1]), "{order:?}");
    // The switch's anchor is on Indexing.
    let indexing = get(&alice, &format!("{}/admin/indexing", h.base)).await;
    assert!(indexing.body.contains("id=\"public-scope\""), "{}", indexing.body);

    // Words, from the name, the help or the page.
    let limit = found(&get(&alice, &url("?q=member+limit")).await.body);
    assert!(limit.contains(&("/admin/members#s-community-max_members".to_string(), "community.max_members".to_string(), false)), "{limit:?}");
    // A technical name, matched against technical names only.
    let by_key = found(&get(&alice, &url("?q=max_members")).await.body);
    assert_eq!(by_key, [("/admin/members#s-community-max_members".to_string(), "community.max_members".to_string(), false)]);
    // Farsight's lookup rate: folded, and linked with the fold open.
    let rate = found(&get(&alice, &url("?q=farsight%20rate")).await.body);
    assert!(rate.contains(&("/admin/blocks?open=advanced#s-farsight-rate_per_sec".to_string(), "farsight.rate_per_sec".to_string(), true)), "{rate:?}");
    // In any case.
    let floors = get(&alice, &url("?q=FLOOR")).await;
    assert!(floors.body.contains("<h2 id=\"found\">Settings that match</h2>") && floors.body.contains("value=\"FLOOR\""), "{}", floors.body);
    let floors = found(&floors.body);
    assert!(floors.iter().filter(|r| r.1.starts_with("private_read.floor_ms.")).count() == 12 && floors.iter().all(|r| r.2 || r.1.starts_with("bookmarks.")), "{floors:?}");
    let only = found(&get(&alice, &url("?q=floor_ms")).await.body);
    assert!(only.len() == 12 && only.iter().all(|r| r.1.starts_with("private_read.floor_ms.")), "{only:?}");
    // Nothing found: where everything Prism runs with is.
    let none = get(&alice, &url("?q=zzz")).await;
    assert!(found(&none.body).is_empty() && !none.body.contains("<table"), "{}", none.body);
    assert!(none.body.contains("No setting you can change here matches. Everything Prism runs with is on\n<a href=\"/admin/settings/running\">Running</a>."), "{}", none.body);

    // The three lists, each row linking to where its setting is changed.
    for at in ["waiting", "aside", "file"] {
        assert!(!index.body.contains(&format!("<h2 id=\"{at}\">")), "{at} with nothing in it");
    }
    sqlx::query("INSERT INTO operator_settings (key, value, updated_by) VALUES ('retention.tombstone_days', '3', 'cli')").execute(p).await.expect("waiting");
    sqlx::query("INSERT INTO operator_settings (key, value, updated_by, set_aside) VALUES ('private_read.max_chain', '500', 'cli', '{\"problem\": \"private_read.max_chain must be 1 to 100\"}')")
        .execute(p)
        .await
        .expect("aside");
    sqlx::query("INSERT INTO operator_settings (key, value, updated_by, file_notice, file_value) VALUES ('cache.profile_ttl_secs', '60', 'cli', true, '{\"value\": 30, \"explicit\": true}')")
        .execute(p)
        .await
        .expect("file notice");
    let index = get(&alice, &url("")).await;
    let body = &index.body;
    for (key, at) in [("retention.tombstone_days", "waiting"), ("private_read.max_chain", "aside"), ("cache.profile_ttl_secs", "file")] {
        assert!(list(body, at).contains(&format!("href=\"{}\"", view::href(key, &ctx))), "{at}: {}", list(body, at));
    }
    assert!(list(body, "waiting").contains("href=\"/admin/storage?open=advanced#s-retention-tombstone_days\""), "{body}");
    assert!(list(body, "file").contains("app.toml now says 30 seconds; your saved value 60 seconds still applies."), "{body}");
    // The value now, in words: the saved one.
    assert!(body.contains("<code>cache.profile_ttl_secs</code></td><td class=\"text\"><div class=\"cell-text\">60 seconds</div>"), "{body}");

    // No supervisor: the card says so, and no restart form is drawn.
    let card = &body[body.find("<div class=\"card\" id=\"restart\">").expect("restart card")..];
    let card = &card[..card.find("</div>").expect("card end")];
    assert!(card.contains("<h2>Restart Prism</h2>") && card.contains("Prism cannot restart itself on this server; ask whoever runs it."), "{card}");
    assert!(!body.contains("action=\"/admin/restart\""), "{body}");
    // The read-only pages, linked; no script; no group card is left.
    assert!(body.contains("href=\"/admin/settings/installed\"") && body.contains("href=\"/admin/settings/running\""), "{body}");
    assert!(!body.contains("<script"), "{body}");
    for slug in OLD_GROUP_SLUGS {
        assert!(!body.contains(&format!("/admin/settings/{slug}\"")), "{slug}: {body}");
    }
    assert!(!body.contains("On their own pages") && !body.contains("Who can use Prism") && !body.contains("What Prism stores"), "{body}");
    h.db.drop_db().await;
}

/// Under a supervisor, All settings' restart card holds the restart form,
/// which asks for confirmation first and queues nothing until confirmed.
#[tokio::test]
async fn all_settings_offers_restart_under_a_supervisor() {
    let Some(h) = harness_supervised("admin_homes_restart").await else {
        return;
    };
    let alice = complete_setup(&h, ALICE).await;
    let index = get(&alice, &format!("{}/admin/settings", h.base)).await;
    let body = &index.body;
    let card = &body[body.find("<div class=\"card\" id=\"restart\">").expect("restart card")..];
    let card = &card[..card.find("</div>").expect("card end")];
    assert!(card.contains("<form method=\"post\" action=\"/admin/restart\">") && card.contains(&format!("name=\"csrf\" value=\"{}\"", csrf_of(&index))), "{card}");
    assert!(!card.contains("cannot restart itself"), "{card}");
    let before = queued(h.pool()).await.len();
    let confirm = post(&alice, &format!("{}/admin/restart", h.base), &[("csrf", &csrf_of(&index))]).await;
    assert_eq!(confirm.status, 200, "{}", confirm.body);
    assert!(confirm.body.contains("Prism will be unavailable for about a minute.") && confirm.body.contains("name=\"confirm\""), "{}", confirm.body);
    assert_eq!(queued(h.pool()).await.len(), before);
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
