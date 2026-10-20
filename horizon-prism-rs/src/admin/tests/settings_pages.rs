//! The settings pages over HTTP (admin-ui design §6.3, §8; T-A7 for the
//! typed settings, T-A15 for the running configuration): every group
//! renders; a saved form queues one `settings set` with only the changed
//! fields and their versions; a value the form refuses, or an unconfirmed
//! typed change, queues nothing; public data off is typed; the restart is
//! refused without a supervisor; the two banners and Home's notice.

use serde_json::{Value, json};

use super::*;
use crate::admin::settings::registry::Group;

type Queued = (String, Value, Option<String>);

async fn queued(pool: &PgPool) -> Vec<Queued> {
    sqlx::query_as("SELECT command, args, requested_by FROM admin_commands ORDER BY id").fetch_all(pool).await.expect("commands")
}

/// A POST past the one-change-a-second limit (§5.6).
async fn post(b: &Browser, url: &str, form: &[(&str, &str)]) -> Page {
    tokio::time::sleep(Duration::from_millis(1100)).await;
    b.post(url, form).await
}

#[tokio::test]
async fn settings_pages_and_their_forms() {
    let Some(h) = harness("admin_settings_pages").await else {
        return;
    };
    let alice = complete_setup(&h, ALICE).await;
    let p = h.pool();
    let url = |path: &str| format!("{}{path}", h.base);
    for g in Group::ALL {
        let page = alice.get(&url(&format!("/admin/settings/{}", g.slug()))).await;
        assert_eq!(page.status, 200, "{}: {}", g.slug(), page.body);
        assert!(page.body.contains(g.title()) && page.body.contains("technical name"), "{}", g.slug());
    }
    // Past the ten-pages-a-second limit (§5.6).
    tokio::time::sleep(Duration::from_millis(1100)).await;
    let index = alice.get(&url("/admin/settings")).await;
    assert!(index.body.contains("Set when Prism was installed") && index.body.contains("Who can use Prism"), "{}", index.body);
    assert_eq!(alice.get(&url("/admin/settings/installed")).await.status, 200);
    let running = alice.get(&url("/admin/settings/running")).await;
    assert!(running.body.contains("[community]"), "{}", running.body);
    assert_eq!(alice.get(&url("/admin/settings/nowhere")).await.status, 404);

    // A live key, beside an unchanged field: one command with only the
    // change and the form's version, by the operator.
    let page = alice.get(&url("/admin/settings/access")).await;
    assert!(page.body.contains("name=\"threadlens.access\"") && page.body.contains("Applies at once"), "{}", page.body);
    let csrf = csrf_of(&page);
    let saved = post(&alice, &url("/admin/settings/access"), &[("csrf", &csrf), ("threadlens.access", "members"), ("version:threadlens.access", "0"), ("community.max_members", "1000")]).await;
    assert_eq!(saved.status, 303, "{}", saved.body);
    assert!(saved.headers.get(LOCATION).and_then(|v| v.to_str().ok()).is_some_and(|l| l.starts_with("/admin/commands/")));
    assert_eq!(
        queued(p).await.last(),
        Some(&("settings set".to_string(), json!({"changes": [{"key": "threadlens.access", "value": "members", "base_version": 0}]}), Some(ALICE.to_string())))
    );
    let before = queued(p).await.len();
    // A value the form refuses: nothing queued, the problem in words.
    let bad = post(&alice, &url("/admin/settings/private"), &[("csrf", &csrf), ("private_read.max_chain", "500")]).await;
    assert_eq!(bad.status, 400);
    assert!(bad.body.contains("whole number from 1 to 100"), "{}", bad.body);
    // Nothing changed.
    let same = post(&alice, &url("/admin/settings/access"), &[("csrf", &csrf), ("threadlens.access", "signed_in")]).await;
    assert!(same.body.contains("Nothing changed"), "{}", same.body);
    assert_eq!(queued(p).await.len(), before);

    // Typed (§8.7): turning the fallback on needs ON.
    let form = [("csrf", csrf.as_str()), ("farsight.fallback.enabled", "on")];
    let confirm = post(&alice, &url("/admin/settings/blocks"), &form).await;
    assert_eq!(confirm.status, 200);
    assert!(confirm.body.contains("<code>ON</code>"), "{}", confirm.body);
    let wrong = post(&alice, &url("/admin/settings/blocks"), &[("csrf", &csrf), ("farsight.fallback.enabled", "on"), ("confirm", "yes"), ("typed", "nope")]).await;
    assert_eq!(wrong.status, 400);
    assert_eq!(queued(p).await.len(), before, "an unconfirmed typed change queues nothing");
    let typed = post(&alice, &url("/admin/settings/blocks"), &[("csrf", &csrf), ("farsight.fallback.enabled", "on"), ("confirm", "yes"), ("typed", "on")]).await;
    assert_eq!(typed.status, 303, "{}", typed.body);
    assert_eq!(queued(p).await.last().map(|q| q.1["changes"][0]["key"].clone()), Some(json!("farsight.fallback.enabled")));

    // Public data off: its own command, typed OFF.
    let confirm = post(&alice, &url("/admin/settings/public-scope"), &[("csrf", &csrf), ("value", "off")]).await;
    assert!(confirm.body.contains("<code>OFF</code>"), "{}", confirm.body);
    let off = post(&alice, &url("/admin/settings/public-scope"), &[("csrf", &csrf), ("value", "off"), ("confirm", "yes"), ("typed", "OFF")]).await;
    assert_eq!(off.status, 303, "{}", off.body);
    assert_eq!(queued(p).await.last().map(|q| (q.0.clone(), q.1.clone())), Some(("indexing public-scope".to_string(), json!({"value": "off"}))));

    // No supervisor here: the restart is refused and nothing is queued.
    let before = queued(p).await.len();
    let restart = post(&alice, &url("/admin/restart"), &[("csrf", &csrf)]).await;
    assert_eq!(restart.status, 409);
    assert!(restart.body.contains("cannot restart itself"), "{}", restart.body);
    assert_eq!(queued(p).await.len(), before);

    // The banners: a saved restart key not in use yet, then a set-aside one.
    sqlx::query("INSERT INTO operator_settings (key, value, updated_by) VALUES ('retention.tombstone_days', '3', 'cli')").execute(p).await.expect("row");
    let members = alice.get(&url("/admin/members")).await;
    assert!(members.body.contains("Changes waiting for a restart: 1 saved change"), "{}", members.body);
    assert!(!members.body.contains("Restart Prism"), "no restart button without a supervisor");
    sqlx::query("INSERT INTO operator_settings (key, value, updated_by, set_aside) VALUES ('private_read.max_chain', '500', 'cli', '{\"problem\": \"private_read.max_chain must be 1 to 100\"}')")
        .execute(p)
        .await
        .expect("row");
    let members = alice.get(&url("/admin/members")).await;
    assert!(members.body.contains("Some saved settings could not be used: 1 saved setting"), "{}", members.body);
    let index = alice.get(&url("/admin/settings")).await;
    assert!(index.body.contains("must be 1 to 100") && index.body.contains("In use until a restart: 7 days"), "{}", index.body);
    // The setting's own page says both.
    let storage = alice.get(&url("/admin/settings/storage")).await;
    assert!(storage.body.contains("In use until a restart") && storage.body.contains("Use the default (7 days)"), "{}", storage.body);

    // Reset: one change with no value.
    let reset = post(&alice, &url("/admin/settings/reset"), &[("csrf", &csrf), ("key", "retention.tombstone_days"), ("version", "1")]).await;
    assert_eq!(reset.status, 303, "{}", reset.body);
    assert_eq!(queued(p).await.last().map(|q| q.1.clone()), Some(json!({"changes": [{"key": "retention.tombstone_days", "reset": true, "base_version": 1}]})));

    // app.toml changed under a saved value: Home says so, and the page
    // offers to keep it or take the file's.
    sqlx::query("UPDATE operator_settings SET file_notice = true, file_value = '{\"value\": 9, \"explicit\": true}' WHERE key = 'retention.tombstone_days'")
        .execute(p)
        .await
        .expect("notice");
    let home = alice.get(&url("/admin")).await;
    assert!(home.body.contains("app.toml now says 9 days; your saved value 3 days still applies."), "{}", home.body);
    let storage = alice.get(&url("/admin/settings/storage")).await;
    assert!(storage.body.contains("Keep my value") && storage.body.contains("Use the file's value"), "{}", storage.body);
    let keep = post(&alice, &url("/admin/settings/keep"), &[("csrf", &csrf), ("key", "retention.tombstone_days"), ("version", "1")]).await;
    assert_eq!(keep.status, 303, "{}", keep.body);
    assert_eq!(queued(p).await.last().map(|q| q.1.clone()), Some(json!({"changes": [{"key": "retention.tombstone_days", "value": 3, "base_version": 1}]})));
    // A forged form changes nothing (T-A7).
    let before = queued(p).await.len();
    let forged = post(&alice, &url("/admin/settings/access"), &[("csrf", "forged"), ("threadlens.access", "members")]).await;
    assert_eq!(forged.status, 403);
    assert_eq!(queued(p).await.len(), before);
    h.db.drop_db().await;
}
