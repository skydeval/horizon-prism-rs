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
