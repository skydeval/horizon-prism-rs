//! Slice S5 over HTTP (admin-ui design §6.2, §6.9, §6.10): the per-member
//! actions (sync again, Farsight read and forget, list clean-up, the last
//! two typed), the Labels page (filters, add, typed take-down and removal,
//! post links read for their path), and Look up (identity afresh both
//! ways, scope holds, blocks under the member-side rule, the private
//! tier's counts, labels, jobs and actions).

use super::*;
use crate::admin::health::HealthInputs;

/// A page past the ten-pages-a-second limit.
async fn get(b: &Browser, url: &str) -> Page {
    tokio::time::sleep(Duration::from_millis(150)).await;
    b.get(url).await
}

/// A change past the one-a-second limit.
async fn post(b: &Browser, url: &str, form: &[(&str, &str)]) -> Page {
    tokio::time::sleep(Duration::from_millis(1100)).await;
    b.post(url, form).await
}

/// The newest command's name and arguments, and who asked.
async fn newest(pool: &PgPool) -> (String, Value, Option<String>) {
    sqlx::query_as("SELECT command, args, requested_by FROM admin_commands ORDER BY id DESC LIMIT 1").fetch_one(pool).await.expect("command")
}

const BOB_HANDLE: &str = "bob.example.com";

/// Bob as a member with a stored handle.
async fn bob_member(h: &Harness) {
    let p = h.pool();
    sqlx::query("INSERT INTO actors (did, handle, pds_endpoint) VALUES ($1, $2, 'https://pds.bob.example')").bind(BOB).bind(BOB_HANDLE).execute(p).await.expect("actor");
    sqlx::query("INSERT INTO members (did, source, ready_at, first_ready_at) VALUES ($1, 'admin', now(), now())").bind(BOB).execute(p).await.expect("member");
}

#[tokio::test]
async fn member_actions() {
    let (farsight, _fake) = crate::farsight::lookup::tests::engine("").await;
    let inputs = HealthInputs { farsight: Some(farsight), ..HealthInputs::default() };
    let Some(h) = harness_full("admin_s5_member_actions", quick(), true, inputs).await else {
        return;
    };
    let p = h.pool();
    let alice = complete_setup(&h, ALICE).await;
    bob_member(&h).await;
    let page = get(&alice, &format!("{}/admin/members", h.base)).await;
    assert_eq!(page.status, 200, "{}", page.body);
    for text in ["Sync again", "Ask Farsight to read again", "Clean up lists…", "/admin/lookup?q=did:plc:bbbb"] {
        assert!(page.body.contains(text), "{text}: {}", page.body);
    }
    // Farsight's memory is off: nothing to forget.
    assert!(!page.body.contains("Forget remembered blocks…"), "{}", page.body);
    let csrf = csrf_of(&page);
    let url = |path: &str| format!("{}/admin/members/{path}", h.base);

    // Sync again and Farsight's read: no confirmation, one command each.
    let done = post(&alice, &url("resync"), &[("csrf", &csrf), ("did", BOB)]).await;
    assert_eq!(done.status, 303, "{}", done.body);
    let (command, args, by) = newest(p).await;
    assert_eq!((command.as_str(), args["did"].as_str(), args["depth"].is_null(), by.as_deref()), ("backfill", Some(BOB), true, Some(ALICE)));
    let done = post(&alice, &url("farsight-prime"), &[("csrf", &csrf), ("did", BOB)]).await;
    assert_eq!(done.status, 303, "{}", done.body);
    let (command, args, _) = newest(p).await;
    assert_eq!((command.as_str(), args["force"].as_bool()), ("farsight prime", Some(true)));

    // List clean-up and forgetting delete data: typed, the member's handle.
    for (path, name) in [("lists-purge", "lists purge"), ("farsight-forget", "farsight forget")] {
        let before = count(p, "SELECT count(*) FROM admin_commands").await;
        let confirm = post(&alice, &url(path), &[("csrf", &csrf), ("did", BOB)]).await;
        assert_eq!(confirm.status, 200, "{path}: {}", confirm.body);
        assert!(confirm.body.contains(&format!("type <code>{BOB_HANDLE}</code>")), "{path}: {}", confirm.body);
        let wrong = post(&alice, &url(path), &[("csrf", &csrf), ("did", BOB), ("confirm", "yes"), ("typed", "bbbbbbbb")]).await;
        assert_eq!(wrong.status, 400, "{path}");
        assert_eq!(count(p, "SELECT count(*) FROM admin_commands").await, before, "{path}: nothing queued unconfirmed");
        let done = post(&alice, &url(path), &[("csrf", &csrf), ("did", BOB), ("confirm", "yes"), ("typed", "@Bob.Example.com")]).await;
        assert_eq!(done.status, 303, "{path}: {}", done.body);
        let (command, args, by) = newest(p).await;
        assert_eq!((command.as_str(), args["did"].as_str(), by.as_deref()), (name, Some(BOB), Some(ALICE)));
    }
    // A command's page says what it did in words.
    let location = done_location(&alice, &h, &csrf).await;
    let shown = get(&alice, &format!("{}{location}", h.base)).await;
    assert!(shown.body.contains("Sync did:plc:bbbbbbbbbbbbbbbbbbbbbbbb again"), "{}", shown.body);
    h.db.drop_db().await;
}

/// One more sync, for its command page's address.
async fn done_location(b: &Browser, h: &Harness, csrf: &str) -> String {
    let done = post(b, &format!("{}/admin/members/resync", h.base), &[("csrf", csrf), ("did", BOB)]).await;
    done.headers.get(LOCATION).and_then(|v| v.to_str().ok()).expect("location").to_string()
}

#[tokio::test]
async fn labels_page() {
    let Some(h) = harness("admin_s5_labels").await else {
        return;
    };
    let p = h.pool();
    let alice = complete_setup(&h, ALICE).await;
    bob_member(&h).await;
    h.identity.handles.lock().expect("lock").insert(BOB_HANDLE.to_string(), BOB.to_string());
    let page = get(&alice, &format!("{}/admin/labels", h.base)).await;
    assert_eq!(page.status, 200, "{}", page.body);
    for text in ["aria-current=\"page\">Labels</a>", "Take down", "as if it did not exist, everywhere", "Graphic (violence or gore)", "No labels."] {
        assert!(page.body.contains(text), "{text}: {}", page.body);
    }
    let csrf = csrf_of(&page);
    let add = format!("{}/admin/labels/add", h.base);

    // A post's link is read for its path; the handle is resolved.
    let done = post(&alice, &add, &[("csrf", &csrf), ("subject", "https://bsky.app/profile/bob.example.com/post/3kpost"), ("val", "porn")]).await;
    assert_eq!(done.status, 303, "{}", done.body);
    let (command, args, by) = newest(p).await;
    assert_eq!(command, "label add");
    assert_eq!(args, json!({"subject": format!("at://{BOB}/app.bsky.feed.post/3kpost"), "val": "porn"}));
    assert_eq!(by.as_deref(), Some(ALICE));
    // Unreadable input and unknown values change nothing.
    let before = count(p, "SELECT count(*) FROM admin_commands").await;
    let bad = post(&alice, &add, &[("csrf", &csrf), ("subject", "https://example.com/somewhere"), ("val", "porn")]).await;
    assert!(bad.status == 400 && bad.body.contains("cannot read that link"), "{}", bad.body);
    let bad = post(&alice, &add, &[("csrf", &csrf), ("subject", BOB), ("val", "spam")]).await;
    assert_eq!(bad.status, 400);
    // Taking down is typed.
    let confirm = post(&alice, &add, &[("csrf", &csrf), ("subject", BOB_HANDLE), ("val", "!takedown")]).await;
    assert!(confirm.status == 200 && confirm.body.contains(&format!("type <code>{BOB_HANDLE}</code>")), "{}", confirm.body);
    assert_eq!(field(&confirm.body, "subject").as_deref(), Some(BOB), "the confirmation carries the DID");
    let wrong = post(&alice, &add, &[("csrf", &csrf), ("subject", BOB), ("val", "!takedown"), ("confirm", "yes"), ("typed", "nope")]).await;
    assert_eq!(wrong.status, 400);
    assert_eq!(count(p, "SELECT count(*) FROM admin_commands").await, before);
    let done = post(&alice, &add, &[("csrf", &csrf), ("subject", BOB), ("val", "!takedown"), ("confirm", "yes"), ("typed", BOB_HANDLE)]).await;
    assert_eq!(done.status, 303, "{}", done.body);
    assert_eq!(newest(p).await.1, json!({"subject": BOB, "val": "!takedown"}));

    // The list, filtered by account (its records too) and by label.
    for (subject, val) in [(BOB.to_string(), "!hide"), (format!("at://{BOB}/app.bsky.feed.post/3k"), "porn"), (CAROL.to_string(), "nudity")] {
        sqlx::query("INSERT INTO labels (subject, val) VALUES ($1, $2)").bind(subject).bind(val).execute(p).await.expect("label");
    }
    let bobs = get(&alice, &format!("{}/admin/labels?q=bob.example.com", h.base)).await;
    assert!(bobs.body.contains("Hide") && bobs.body.contains("post of <strong>bob.example.com</strong>"), "{}", bobs.body);
    assert!(!bobs.body.contains(CAROL), "{}", bobs.body);
    let nudity = get(&alice, &format!("{}/admin/labels?val=nudity", h.base)).await;
    assert!(nudity.body.contains(CAROL) && !nudity.body.contains(&format!("<code>{BOB}</code>")), "{}", nudity.body);
    let unknown = get(&alice, &format!("{}/admin/labels?q=nobody.example.com", h.base)).await;
    assert!(unknown.body.contains("could not be found"), "{}", unknown.body);

    // Removing deletes a row: typed (no handle stored: the DID's end).
    let remove = format!("{}/admin/labels/remove", h.base);
    let confirm = post(&alice, &remove, &[("csrf", &csrf), ("subject", CAROL), ("val", "nudity")]).await;
    assert!(confirm.status == 200 && confirm.body.contains("type <code>cccccccc</code>"), "{}", confirm.body);
    let done = post(&alice, &remove, &[("csrf", &csrf), ("subject", CAROL), ("val", "nudity"), ("confirm", "yes"), ("typed", "CCCCCCCC")]).await;
    assert_eq!(done.status, 303, "{}", done.body);
    let (command, args, _) = newest(p).await;
    assert_eq!((command.as_str(), args), ("label remove", json!({"subject": CAROL, "val": "nudity"})));
    // A handle is never taken as a stored subject.
    let refused = post(&alice, &remove, &[("csrf", &csrf), ("subject", BOB_HANDLE), ("val", "porn")]).await;
    assert_eq!(refused.status, 400);
    // The audit log names labels in words.
    let audit = get(&alice, &format!("{}/admin/audit", h.base)).await;
    assert!(audit.body.contains("Remove label &quot;Nudity (not sexual)&quot; from") || audit.body.contains("Remove label &#34;Nudity (not sexual)&#34; from"), "{}", audit.body);
    h.db.drop_db().await;
}

/// Refract's Communities page (refract design §9.1): the list with its
/// counts, a disable that would make restored accounts pending asks first
/// and then saves the whole list, and pending ends are kept from here.
#[tokio::test]
async fn communities_page() {
    let Some(h) = harness("admin_communities").await else {
        return;
    };
    let p = h.pool();
    let alice = complete_setup(&h, ALICE).await;
    sqlx::query("INSERT INTO refract_dids (did, community, pds_host, state, source) VALUES ($1, 'community-a', 'community-a.example', 'restored', 'fill')")
        .bind(BOB)
        .execute(p)
        .await
        .expect("row");
    sqlx::query("INSERT INTO refract_pending (scope) VALUES ('feature')").execute(p).await.expect("pending");
    let page = get(&alice, &format!("{}/admin/communities", h.base)).await;
    assert_eq!(page.status, 200, "{}", page.body);
    for text in ["aria-current=\"page\">Communities</a>", "<strong>community-a</strong>", "Pending ends", "Add a community"] {
        assert!(page.body.contains(text), "{text}: {}", page.body);
    }
    let csrf = csrf_of(&page);
    let toggle = format!("{}/admin/communities/toggle", h.base);
    let before = count(p, "SELECT count(*) FROM admin_commands").await;
    let confirm = post(&alice, &toggle, &[("csrf", &csrf), ("name", "community-a")]).await;
    assert!(confirm.status == 200 && confirm.body.contains("1 restored account will be hidden"), "{}", confirm.body);
    assert_eq!(count(p, "SELECT count(*) FROM admin_commands").await, before, "nothing queued unconfirmed");
    let done = post(&alice, &toggle, &[("csrf", &csrf), ("name", "community-a"), ("confirm", "yes")]).await;
    assert_eq!(done.status, 303, "{}", done.body);
    let (command, args, by) = newest(p).await;
    assert_eq!((command.as_str(), by.as_deref()), ("settings set", Some(ALICE)));
    let change = &args["changes"][0];
    assert_eq!(change["key"], "refract.communities");
    assert_eq!(change["value"][0]["name"], "community-a");
    assert_eq!(change["value"][0]["enabled"], false);
    let keep = post(&alice, &format!("{}/admin/communities/keep", h.base), &[("csrf", &csrf), ("scope", "feature"), ("keep", "true")]).await;
    assert_eq!(keep.status, 303, "{}", keep.body);
    let (command, args, _) = newest(p).await;
    assert_eq!((command.as_str(), args), ("refract keep", json!({"scope": "feature", "keep": true})));
    h.db.drop_db().await;
}

/// The Refract accounts page (refract design §9): the list with its state
/// filter and history progress; adding by handle and by DID (refused while
/// Refract is off); an account's page; check again; a removal typed with
/// the handle and offered only for restored accounts; the command's page
/// linking back; pending ends released from here.
#[tokio::test]
async fn refract_accounts_page() {
    const DAN: &str = "did:plc:dddddddddddddddddddddddd";
    let Some(h) = harness("admin_refract_accounts").await else {
        return;
    };
    let p = h.pool();
    let alice = complete_setup(&h, ALICE).await;
    sqlx::query("INSERT INTO actors (did, handle, pds_endpoint) VALUES ($1, $2, 'https://community-a.example')").bind(BOB).bind(BOB_HANDLE).execute(p).await.expect("actor");
    let verdict = json!({"at": "2026-10-22T00:00:00Z", "rules": [{"rule": 1, "result": "pass", "detail": "AccountTakedown"}]});
    sqlx::query(
        "INSERT INTO refract_dids (did, community, pds_host, state, source, since, last_verdict) VALUES ($1, 'community-a', 'community-a.example', 'restored', 'fill', now(), $2)",
    )
    .bind(BOB)
    .bind(&verdict)
    .execute(p)
    .await
    .expect("bob");
    sqlx::query("INSERT INTO refract_dids (did, community, pds_host, state, source) VALUES ($1, 'community-a', 'community-a.example', 'rejected', 'admin')")
        .bind(CAROL)
        .execute(p)
        .await
        .expect("carol");
    let cursor = json!({"collection": "app.bsky.feed.post", "cursor": "c", "done": ["app.bsky.actor.profile"]});
    sqlx::query("INSERT INTO backfill_jobs (did, depth, state, refract_cursor) VALUES ($1, 'refract', 'running', $2)").bind(BOB).bind(&cursor).execute(p).await.expect("job");
    sqlx::query("INSERT INTO refract_events (did, from_state, to_state, reason) VALUES ($1, 'candidate', 'restored', 'rules')").bind(BOB).execute(p).await.expect("event");
    sqlx::query("INSERT INTO refract_pending (scope, keep) VALUES ('feature', true)").execute(p).await.expect("pending");
    sqlx::query("INSERT INTO refract_pending_dids (scope, did) VALUES ('feature', $1)").bind(BOB).execute(p).await.expect("pending did");
    let list = format!("{}/admin/refract", h.base);
    let url = |path: &str| format!("{}/admin/refract/{path}", h.base);

    // Off: everything listed, said plainly, and nothing can be added.
    let page = get(&alice, &list).await;
    assert_eq!(page.status, 200, "{}", page.body);
    for text in [
        "aria-current=\"page\">Refract accounts</a>",
        "Refract is off",
        BOB_HANDLE,
        CAROL,
        "fetching, 1 collection fetched, now app.bsky.feed.post",
        "Pending ends",
        ">Release</button>",
        "<main class=\"wide\">",
        "<table class=\"dense\">",
    ] {
        assert!(page.body.contains(text), "{text}: {}", page.body);
    }
    assert!(!page.body.contains("action=\"/admin/refract/add\""), "{}", page.body);
    let csrf = csrf_of(&page);
    let before = count(p, "SELECT count(*) FROM admin_commands").await;
    let refused = post(&alice, &url("add"), &[("csrf", &csrf), ("input", DAN)]).await;
    assert!(refused.status == 400 && refused.body.contains("Refract is off"), "{}", refused.body);
    assert_eq!(count(p, "SELECT count(*) FROM admin_commands").await, before, "nothing queued while off");
    let restored = get(&alice, &format!("{list}?state=restored")).await;
    assert!(restored.body.contains(BOB) && !restored.body.contains(CAROL), "{}", restored.body);

    // On: add by handle and by DID; a bad one is refused.
    h.live.update(|v| v.refract_enabled = true);
    let page = get(&alice, &list).await;
    assert!(page.body.contains("action=\"/admin/refract/add\"") && !page.body.contains("Refract is off"), "{}", page.body);
    h.identity.handles.lock().expect("lock").insert("dan.example.com".to_string(), DAN.to_string());
    let added = post(&alice, &url("add"), &[("csrf", &csrf), ("input", "@Dan.example.com")]).await;
    assert_eq!(added.status, 303, "{}", added.body);
    let (command, args, by) = newest(p).await;
    assert_eq!((command.as_str(), args, by.as_deref()), ("refract check", json!({"did": DAN}), Some(ALICE)));
    let added = post(&alice, &url("add"), &[("csrf", &csrf), ("input", CAROL)]).await;
    assert_eq!(added.status, 303, "{}", added.body);
    assert_eq!(newest(p).await.1, json!({"did": CAROL}));
    let before = count(p, "SELECT count(*) FROM admin_commands").await;
    let bad = post(&alice, &url("add"), &[("csrf", &csrf), ("input", "not a handle")]).await;
    assert_eq!(bad.status, 400, "{}", bad.body);
    assert_eq!(count(p, "SELECT count(*) FROM admin_commands").await, before);

    // The account's page: what `admin refract show` gives.
    let detail = get(&alice, &url(BOB)).await;
    assert_eq!(detail.status, 200, "{}", detail.body);
    for text in [
        "restored: kept and served",
        "1. Bluesky withholds it",
        "AccountTakedown",
        "Hidden while a pending end runs",
        "fetching, 1 collection fetched",
        "rules",
        "Check again",
        "Remove…",
    ] {
        assert!(detail.body.contains(text), "{text}: {}", detail.body);
    }
    let rejected = get(&alice, &url(CAROL)).await;
    assert!(rejected.body.contains("Only a restored account") && !rejected.body.contains("Remove…"), "{}", rejected.body);
    assert_eq!(get(&alice, &url("did:plc:nobodynobodynobodynobody")).await.status, 404);

    // Check again.
    let checked = post(&alice, &url("check"), &[("csrf", &csrf), ("did", BOB)]).await;
    assert_eq!(checked.status, 303, "{}", checked.body);
    assert_eq!(newest(p).await.0, "refract check");

    // Remove: the confirmation, the typed handle, then `refract end`.
    let before = count(p, "SELECT count(*) FROM admin_commands").await;
    let ask = post(&alice, &url("end"), &[("csrf", &csrf), ("did", BOB), ("reason", "spam")]).await;
    assert_eq!(ask.status, 200, "{}", ask.body);
    assert!(ask.body.contains(&format!("type <code>{BOB_HANDLE}</code>")) && ask.body.contains("that point at it (their likes, follows, replies and quotes) are kept"), "{}", ask.body);
    let wrong = post(&alice, &url("end"), &[("csrf", &csrf), ("did", BOB), ("reason", "spam"), ("confirm", "yes"), ("typed", "bbbbbbbb")]).await;
    assert_eq!(wrong.status, 400, "{}", wrong.body);
    let missing = post(&alice, &url("end"), &[("csrf", &csrf), ("did", BOB), ("confirm", "yes")]).await;
    assert_eq!(missing.status, 400, "{}", missing.body);
    let not_restored = post(&alice, &url("end"), &[("csrf", &csrf), ("did", CAROL), ("confirm", "yes"), ("typed", "cccccccc")]).await;
    assert_eq!(not_restored.status, 400, "{}", not_restored.body);
    assert_eq!(count(p, "SELECT count(*) FROM admin_commands").await, before, "nothing queued unconfirmed");
    let done = post(&alice, &url("end"), &[("csrf", &csrf), ("did", BOB), ("reason", "spam"), ("confirm", "yes"), ("typed", "@Bob.Example.com")]).await;
    assert_eq!(done.status, 303, "{}", done.body);
    let (command, args, by) = newest(p).await;
    assert_eq!((command.as_str(), args, by.as_deref()), ("refract end", json!({"did": BOB, "reason": "spam"}), Some(ALICE)));

    // Once run, the command's page links to the account.
    let id: i64 = sqlx::query_scalar("SELECT max(id) FROM admin_commands").fetch_one(p).await.expect("id");
    let waiting = get(&alice, &format!("{}/admin/commands/{id}", h.base)).await;
    assert!(!waiting.body.contains("See the account"), "{}", waiting.body);
    sqlx::query("UPDATE admin_commands SET state = 'done', result = '{}' WHERE id = $1").bind(id).execute(p).await.expect("done");
    let ran = get(&alice, &format!("{}/admin/commands/{id}", h.base)).await;
    assert!(ran.body.contains(&format!("href=\"/admin/refract/{BOB}\"")), "{}", ran.body);

    // Pending ends: release.
    let release = post(&alice, &format!("{}/admin/communities/keep", h.base), &[("csrf", &csrf), ("scope", "feature"), ("keep", "false")]).await;
    assert_eq!(release.status, 303, "{}", release.body);
    assert_eq!(newest(p).await, ("refract keep".to_string(), json!({"scope": "feature", "keep": false}), Some(ALICE.to_string())));
    h.db.drop_db().await;
}

#[tokio::test]
async fn look_up() {
    let Some(h) = harness("admin_s5_lookup").await else {
        return;
    };
    let p = h.pool();
    let alice = complete_setup(&h, ALICE).await;
    bob_member(&h).await;
    h.identity.set(BOB, "https://pds.bob.example");
    h.identity.handles.lock().expect("lock").insert(BOB_HANDLE.to_string(), BOB.to_string());
    h.identity.claims.lock().expect("lock").insert(BOB.to_string(), BOB_HANDLE.to_string());
    let reasons = crate::aggregations::REASON_MEMBER | crate::aggregations::REASON_FOLLOWED;
    sqlx::query("INSERT INTO scope_dids (did, reasons) VALUES ($1, $2)").bind(BOB).bind(reasons).execute(p).await.expect("scope");
    sqlx::query("INSERT INTO sync_state (did) VALUES ($1)").bind(BOB).execute(p).await.expect("sync");
    sqlx::query("INSERT INTO backfill_jobs (did, depth, state, attempts, last_error) VALUES ($1, 'member', 'failed', 3, 'repo unavailable')").bind(BOB).execute(p).await.expect("job");
    sqlx::query("INSERT INTO labels (subject, val) VALUES ($1, 'porn')").bind(format!("at://{BOB}/app.bsky.feed.post/3kp")).execute(p).await.expect("label");
    sqlx::query("INSERT INTO blocks (uri, author_did, subject_did, rev, created_at) VALUES ($1, $2, $3, '3k', now())")
        .bind(format!("at://{BOB}/app.bsky.graph.block/3kb"))
        .bind(BOB)
        .bind(CAROL)
        .execute(p)
        .await
        .expect("block");
    {
        let mut sets = h.scope.write_guard().await;
        sets.members.insert(BOB.to_string());
        sets.scope.insert(BOB.to_string());
    }

    let empty = get(&alice, &format!("{}/admin/lookup", h.base)).await;
    assert!(empty.status == 200 && empty.body.contains("aria-current=\"page\">Look up</a>"), "{}", empty.body);
    let page = get(&alice, &format!("{}/admin/lookup?q=%40bob.example.com", h.base)).await;
    assert_eq!(page.status, 200, "{}", page.body);
    for text in [
        "<h2>bob.example.com</h2>",
        "bob.example.com (checked both ways)",
        "pds.bob.example",
        "from the PLC directory",
        "It is a member.",
        "A member follows it.",
        "Prism follows this account</span><span class=\"v\">yes",
        "Receiving its updates</span><span class=\"v\">yes",
        "Blocks it made</span><span class=\"v\">1",
        "Blocks of it</span><span class=\"v\">0",
        "all its blocks and every block of it are kept",
        "Private posts stored</span><span class=\"v\">0",
        "With a video",
        "Members&#39; first syncs",
        "repo unavailable",
        "Porn",
        "Clean up lists…",
        "Remove member…",
    ] {
        assert!(page.body.contains(text) || page.body.contains(&text.replace("&#39;", "&#x27;")), "{text}: {}", page.body);
    }
    assert!(!page.body.contains("A member replied"), "{}", page.body);
    assert!(!page.body.contains("Ask Farsight"), "no Farsight here: {}", page.body);

    // A post's link: the post's own facts, and its author.
    let post_page = get(&alice, &format!("{}/admin/lookup?q=https%3A%2F%2Fbsky.app%2Fprofile%2Fbob.example.com%2Fpost%2F3kp", h.base)).await;
    for text in ["The post or record", "Prism holds this post</span><span class=\"v\">no", "Its labels</span><span class=\"v\">Porn", "Add a label to this post"] {
        assert!(post_page.body.contains(text), "{text}: {}", post_page.body);
    }
    assert_eq!(field(&post_page.body, "subject").as_deref(), Some(format!("at://{BOB}/app.bsky.feed.post/3kp").as_str()));

    // An account Prism knows nothing of, and one that cannot be found.
    let carol = get(&alice, &format!("{}/admin/lookup?q={CAROL}", h.base)).await;
    for text in ["could not be looked up just now", "nothing about this account yet", "Prism follows this account</span><span class=\"v\">no", "Add as a member", "only its blocks of members"] {
        assert!(carol.body.contains(text), "{text}: {}", carol.body);
    }
    let nobody = get(&alice, &format!("{}/admin/lookup?q=nobody.example.com", h.base)).await;
    assert!(nobody.body.contains("could not be found"), "{}", nobody.body);

    // Without Farsight, its actions are refused in words.
    let csrf = csrf_of(&page);
    let refused = post(&alice, &format!("{}/admin/members/farsight-prime", h.base), &[("csrf", &csrf), ("did", BOB)]).await;
    assert!(refused.status == 400 && refused.body.contains("Farsight is not set up"), "{}", refused.body);
    h.db.drop_db().await;
}
