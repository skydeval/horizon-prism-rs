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
        // Every old group still has settings on its page.
        assert!(!view::sections_at(Place::Group(g), &networks).is_empty(), "{}", g.slug());
        paths.push(format!("/admin/settings/{}", g.slug()));
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
    // The old group address saves the same way, any setting.
    let old = post(&alice, &url("/admin/settings/access"), &[("csrf", &csrf), ("community.max_members", "1234"), ("version:community.max_members", "0")]).await;
    assert_eq!(old.status, 303, "{}", old.body);
    assert_eq!(queued(p).await.last().map(|q| q.1.clone()), Some(json!({"changes": [{"key": "community.max_members", "value": 1234, "base_version": 0}]})));
    // A typed change confirms through the one save.
    let typed = post(&alice, &save, &[("csrf", &csrf), ("back", "/admin/settings/blocks"), ("farsight.fallback.enabled", "on")]).await;
    assert_eq!(typed.status, 200, "{}", typed.body);
    assert!(typed.body.contains("action=\"/admin/settings/save\"") && typed.body.contains("<code>ON</code>"), "{}", typed.body);
    assert_eq!(field(&typed.body, "back").as_deref(), Some("/admin/settings/blocks"));
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
        ("threadlens.access", "members", "/admin/settings/access#s-threadlens-access", "Back to Who can use Prism"),
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
    assert_eq!(hrefs, ["/admin/members", "/admin/communities"], "{own}");
    assert!(own.contains(">Communities · Refract</a>"), "{own}");
    for g in Group::ALL {
        assert!(body.contains(&format!("<h2><a href=\"/admin/settings/{}\">{}</a></h2>", g.slug(), g.title())), "{}", g.slug());
    }
    assert!(body.contains("<h2><a href=\"/admin/settings/access\">Who can use Prism</a></h2>\n<p class=\"muted\">Who may use Thread Lens</p>"), "{body}");
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
