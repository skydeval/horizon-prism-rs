//! Private activity subscriptions, the fan-out (private-activity-subs
//! design §5): per-kind times (T-AS3), first store only (T-AS4), Horizon's
//! three (T-AS5), the public rules (T-AS6), transient narrowing (T-AS7,
//! T-AS7b to T-AS7d), definitive narrowing (T-AS8), the write's re-checks
//! (T-AS9), the worker's mechanics (T-AS13) and read time (T-AS10). Each attempt is driven by
//! [`Fanout::run_uri`], with no sleeping.

use super::*;
use crate::private_tier::fanout::{Fanout, Finish, queue_row};
use crate::test_support::hooks;

const AUDIENCE_RKEY: &str = "3kaudience22";

fn scope_of(f: &Fixture) -> ScopeState {
    ScopeState::new(f.sets.clone(), ScopePolicy::MembersAndOneHop, 10_000)
}

/// The worker with a watermark `age` old (the freshness bound is 120 s).
fn worker_aged(f: &Fixture, age: u64) -> Fanout {
    Fanout::new(&f.state(Some(Duration::from_secs(age))), scope_of(f), 120)
}

fn worker(f: &Fixture) -> Fanout {
    worker_aged(f, 1)
}

/// `who` subscribes to `subject` for the kinds given, on a minute ago.
async fn subscribe(pool: &PgPool, who: &str, subject: &str, post: bool, reply: bool) {
    let since = |on: bool| if on { "now() - interval '1 minute'" } else { "NULL" };
    exec(pool, &format!(
        "INSERT INTO private_activity_subscriptions (subscriber_did, subject_did, post_since, reply_since) \
         VALUES ('{who}', '{subject}', {}, {}) ON CONFLICT (subscriber_did, subject_did) DO UPDATE \
         SET post_since = EXCLUDED.post_since, reply_since = EXCLUDED.reply_since",
        since(post),
        since(reply)
    ))
    .await;
}

async fn backfilled(pool: &PgPool, did: &str) {
    exec(pool, &format!("UPDATE sync_state SET last_resync_at = now() WHERE did = '{did}'")).await;
}

/// A post record by `author` created now.
async fn fresh_record(f: &Fixture, author: &str, rkey: &str, audience: &str, reply: Option<(&str, &str)>) -> Value {
    let mut record = f.post_record(author, rkey, audience, reply).await;
    record["createdAt"] = json!(Utc::now().to_rfc3339_opts(SecondsFormat::Millis, true));
    record
}

/// A post created now, first stored by `source`.
async fn fresh_post_from(f: &Fixture, source: WriteSource, author: &str, rkey: &str, audience: &str, reply: Option<(&str, &str)>) -> String {
    let record = fresh_record(f, author, rkey, audience, reply).await;
    f.apply_from(source, author, Action::Create, NSID_POST_PRIVATE, rkey, REV, Some(record)).await;
    format!("at://{author}/{NSID_POST_PRIVATE}/{rkey}")
}

async fn fresh_post(f: &Fixture, author: &str, rkey: &str, audience: &str, reply: Option<(&str, &str)>) -> String {
    fresh_post_from(f, WriteSource::Firehose, author, rkey, audience, reply).await
}

/// The recipients of `uri`'s `subscribed-post` rows.
async fn recipients(pool: &PgPool, uri: &str) -> Vec<String> {
    sqlx::query_scalar("SELECT recipient_did FROM private_notifications WHERE record_uri = $1 AND reason = 'subscribed-post' ORDER BY 1")
        .bind(uri)
        .fetch_all(pool)
        .await
        .expect("rows")
}

async fn run(w: &Fanout, uri: &str) -> Finish {
    w.run_uri(uri).await.expect("run").expect("queued")
}

fn notified(rows: u64) -> Finish {
    Finish::Done { outcome: "notified", rows }
}

/// Make `uri`'s queue row `secs` seconds old.
async fn aged(pool: &PgPool, uri: &str, secs: u32) {
    exec(pool, &format!("UPDATE private_post_fanout SET first_at = now() - interval '{secs} seconds' WHERE uri = '{uri}'")).await;
}

/// Alice's world: the fixture in Off, Alice's audience `AUDIENCE_RKEY` in
/// `mode` with `members`, Alice backfilled, Bob, Carol and Dave following
/// her, everyone ready.
async fn world(pool: &PgPool, test: &str, mode: &str, members: &[&str]) -> (Fixture, String) {
    let f = fixture_in(pool, test, PublicMode::Off).await;
    let aud = f.audience(ALICE, AUDIENCE_RKEY, Some(mode), members).await;
    backfilled(pool, ALICE).await;
    for who in [BOB, CAROL, DAVE] {
        exec(pool, &follow_sql(who, ALICE)).await;
    }
    f.ready_all().await;
    (f, aud)
}

/// T-AS3: Posts and Replies each have their own on-since time: a reply
/// written before Replies went on does not notify, a top-level post written
/// then does.
#[tokio::test]
async fn t_as3_each_kind_has_its_own_time() {
    let test = "t_as3_fanout_kinds";
    let Some(db) = fresh_db(test).await else {
        return;
    };
    let (f, aud) = world(&db.pool, test, "list", &[BOB]).await;
    exec(&f.pool, &format!(
        "INSERT INTO private_activity_subscriptions (subscriber_did, subject_did, post_since, reply_since) \
         VALUES ('{BOB}', '{ALICE}', now() - interval '10 minutes', now() + interval '1 minute')"
    ))
    .await;
    let w = worker(&f);
    let top = fresh_post(&f, ALICE, "3kas3top2222", &aud, None).await;
    let reply = fresh_post(&f, ALICE, "3kas3rep2222", &aud, Some((&top, &top))).await;
    assert_eq!(run(&w, &top).await, notified(1));
    assert_eq!(run(&w, &reply).await, Finish::Done { outcome: "no_candidates", rows: 0 });
    assert_eq!(recipients(&f.pool, &top).await, [BOB]);
    assert!(recipients(&f.pool, &reply).await.is_empty());
    // Replies on since before the next reply: it notifies.
    exec(&f.pool, &format!("UPDATE private_activity_subscriptions SET reply_since = now() - interval '1 minute' WHERE subscriber_did = '{BOB}'")).await;
    let later = fresh_post(&f, ALICE, "3kas3rep3333", &aud, Some((&top, &top))).await;
    assert_eq!(run(&w, &later).await, notified(1));
    db.drop_db().await;
}

/// T-AS4: the firehose, `requestRecord` and catch-up queue a first store;
/// backfill does not; an update does not; an old post does not; a post
/// nobody subscribes to is queued all the same.
#[tokio::test]
async fn t_as4_first_store_only() {
    let test = "t_as4_fanout_first_store";
    let Some(db) = fresh_db(test).await else {
        return;
    };
    let (f, aud) = world(&db.pool, test, "list", &[BOB]).await;
    for (source, rkey) in
        [(WriteSource::Firehose, "3kas4fh22222"), (WriteSource::IndexRequest, "3kas4rr22222"), (WriteSource::CatchUp, "3kas4cu22222")]
    {
        let uri = fresh_post_from(&f, source, ALICE, rkey, &aud, None).await;
        assert_eq!(queue_row(&f.pool, &uri).await, Some((0, None, false)), "{source:?}");
    }
    let back = fresh_post_from(&f, WriteSource::Backfill, ALICE, "3kas4bf22222", &aud, None).await;
    assert_eq!(queue_row(&f.pool, &back).await, None, "backfill");
    // An update at a newer rev (an audience change, a rotation): stored, not
    // queued.
    let record = fresh_record(&f, ALICE, "3kas4bf22222", &aud, None).await;
    f.apply_from(WriteSource::Firehose, ALICE, Action::Update, NSID_POST_PRIVATE, "3kas4bf22222", REV2, Some(record)).await;
    let rev: String = sqlx::query_scalar("SELECT rev FROM private_posts WHERE uri = $1").bind(&back).fetch_one(&f.pool).await.expect("rev");
    assert_eq!(rev, REV2);
    assert_eq!(queue_row(&f.pool, &back).await, None, "update");
    // Older than the notify age (the fixture's createdAt is weeks ago).
    let old = f.private_post(ALICE, "3kas4old2222", &aud, None).await;
    assert_eq!(queue_row(&f.pool, &old).await, None, "old");
    let subs: i64 = sqlx::query_scalar("SELECT count(*) FROM private_activity_subscriptions").fetch_one(&f.pool).await.expect("subs");
    assert_eq!(subs, 0, "the three queued with no subscriber at all");
    db.drop_db().await;
}

/// T-AS5 (Horizon's three): a subscriber outside a list audience gets no
/// row, still none once the author adds them to the list, and its
/// `listNotifications {reasons: [subscribed-post], limit: 1}` has no item
/// and no cursor; an in-list subscriber gets exactly one row, as H§3.1.
#[tokio::test]
async fn t_as5_only_the_audience_is_notified() {
    let test = "t_as5_fanout_audience";
    let Some(db) = fresh_db(test).await else {
        return;
    };
    let (f, aud) = world(&db.pool, test, "list", &[BOB]).await;
    subscribe(&f.pool, BOB, ALICE, true, false).await;
    subscribe(&f.pool, CAROL, ALICE, true, false).await;
    let w = worker(&f);
    let uri = fresh_post(&f, ALICE, "3kas5post222", &aud, None).await;
    assert_eq!(run(&w, &uri).await, notified(1));
    type Row = (String, String, String, Option<String>, String, String, Option<String>, bool);
    let rows: Vec<Row> = sqlx::query_as(
        "SELECT recipient_did, author_did, reason, reason_subject, record_uri, record_cid, record_root, public_record IS NULL \
         FROM private_notifications WHERE record_uri = $1",
    )
    .bind(&uri)
    .fetch_all(&f.pool)
    .await
    .expect("rows");
    assert_eq!(rows, [(BOB.into(), ALICE.into(), "subscribed-post".into(), None, uri.clone(), "bafy3kas5post222".into(), None, true)]);
    assert_eq!(queue_row(&f.pool, &uri).await, Some((1, Some(vec![]), true)), "Carol decided, not pending");

    // Alice adds Carol to the list: nothing runs again, and a forced
    // attempt decides no one.
    let members = json!({"$type": NSID_AUDIENCE, "mode": "list", "members": [BOB, CAROL], "createdAt": "2026-09-26T00:00:00Z"});
    f.apply_op(ALICE, Action::Update, NSID_AUDIENCE, AUDIENCE_RKEY, REV2, Some(members)).await;
    assert_eq!(w.run_uri(&uri).await.expect("run"), None, "done");
    exec(&f.pool, &format!("UPDATE private_post_fanout SET done_at = NULL WHERE uri = '{uri}'")).await;
    assert_eq!(run(&w, &uri).await, Finish::Done { outcome: "no_candidates", rows: 0 });
    assert_eq!(recipients(&f.pool, &uri).await, [BOB]);

    let mut app = f.app.clone();
    app.limits = roomy_limits();
    let base = serve(f.state_of(app, Some(Duration::from_secs(1)), Duration::ZERO)).await;
    let (_, page, _) = json_of(&base, LIST_NOTIFICATIONS, "reasons=subscribed-post&limit=1", CAROL).await;
    assert_eq!(page["notifications"], json!([]), "{page}");
    assert!(page.get("cursor").is_none(), "{page}");
    // Carol reads the post now: only the write-time narrowing kept the row
    // away.
    assert_eq!(posts_of(&base, CAROL, &[uri.as_str()]).await.1.len(), 1);
    db.drop_db().await;
}

/// T-AS6: the public fan-out's rules, each alone: no row.
#[tokio::test]
async fn t_as6_the_public_rules() {
    let test = "t_as6_fanout_public_rules";
    let Some(db) = fresh_db(test).await else {
        return;
    };
    let (f, aud) = world(&db.pool, test, "everyone", &[]).await;
    subscribe(&f.pool, BOB, ALICE, true, false).await;
    let w = worker(&f);
    let first = fresh_post(&f, ALICE, "3kas6base222", &aud, None).await;
    assert_eq!(run(&w, &first).await, notified(1), "the control");
    let declaration = |allow: &str| {
        format!(
            "INSERT INTO actor_self_records (uri, did, collection, cid, rev, record) VALUES \
             ('at://{ALICE}/app.bsky.notification.declaration/self', '{ALICE}', 'app.bsky.notification.declaration', 'bafyd', '3k', \
              '{{\"allowSubscriptions\": \"{allow}\"}}')"
        )
    };
    let undeclare = "DELETE FROM actor_self_records".to_string();
    let sub = |set: &str| format!("UPDATE private_activity_subscriptions SET {set} WHERE subscriber_did = '{BOB}'");
    let restore = sub("post_since = now() - interval '1 minute', reply_since = NULL");
    // (case, the subscriber with no row, setup ({uri} the post), teardown,
    // a reply).
    let cases: Vec<(&str, &str, String, String, bool)> = vec![
        ("a reply with only Posts on", BOB, String::new(), String::new(), true),
        ("a post with only Replies on", BOB, sub("post_since = NULL, reply_since = now() - interval '1 minute'"), restore.clone(), false),
        ("subscribed after the post's second", BOB, sub("post_since = now() + interval '5 seconds'"), restore.clone(), false),
        (
            "the author itself",
            ALICE,
            format!("INSERT INTO private_activity_subscriptions (subscriber_did, subject_did, post_since) VALUES ('{ALICE}', '{ALICE}', now() - interval '1 minute')"),
            format!("DELETE FROM private_activity_subscriptions WHERE subscriber_did = '{ALICE}'"),
            false,
        ),
        (
            "not a member",
            BOB,
            format!("DELETE FROM members WHERE did = '{BOB}'"),
            format!("INSERT INTO members (did, source, ready_at) VALUES ('{BOB}', 'admin', now())"),
            false,
        ),
        ("declaration none", BOB, declaration("none"), undeclare.clone(), false),
        ("declaration mutuals, no follow back", BOB, declaration("mutuals"), undeclare.clone(), false),
        ("no declaration, no follow", BOB, format!("DELETE FROM follows WHERE author_did = '{BOB}'"), follow_sql(BOB, ALICE), false),
        (
            "the author never backfilled",
            BOB,
            format!("UPDATE sync_state SET last_resync_at = NULL WHERE did = '{ALICE}'"),
            format!("UPDATE sync_state SET last_resync_at = now() WHERE did = '{ALICE}'"),
            false,
        ),
        (
            "another row for the record",
            BOB,
            format!(
                "INSERT INTO private_notifications (recipient_did, author_did, reason, record_uri, record_cid) \
                 VALUES ('{BOB}', '{ALICE}', 'mention', '{{uri}}', 'bafy')"
            ),
            String::new(),
            false,
        ),
        ("a muted thread", BOB, format!("INSERT INTO thread_mutes (viewer_did, root_uri) VALUES ('{BOB}', '{{uri}}')"), String::new(), false),
    ];
    for (i, (case, who, setup, teardown, reply)) in cases.into_iter().enumerate() {
        let rkey = format!("3kas6case{i:02}2");
        let parent = reply.then_some((first.as_str(), first.as_str()));
        let uri = fresh_post(&f, ALICE, &rkey, &aud, parent).await;
        if !setup.is_empty() {
            exec(&f.pool, &setup.replace("{uri}", &uri)).await;
        }
        run(&w, &uri).await;
        assert!(!recipients(&f.pool, &uri).await.iter().any(|r| r == who), "{case}");
        if !teardown.is_empty() {
            exec(&f.pool, &teardown).await;
        }
    }
    let last = fresh_post(&f, ALICE, "3kas6last222", &aud, None).await;
    assert_eq!(run(&w, &last).await, notified(1), "every case undone");
    db.drop_db().await;
}

/// Asserts `uri`'s attempt by `w` defers with no row and `pending`.
async fn deferred(f: &Fixture, w: &Fanout, uri: &str, pending: Option<Vec<String>>, case: &str) {
    assert_eq!(run(w, uri).await, Finish::Retry { rows: 0, pending }, "{case}");
    assert!(recipients(&f.pool, uri).await.is_empty(), "{case}");
}

/// T-AS7: each transient state defers, the post or the subscriber, and the
/// row is written once it clears; one never cleared ends `gave_up` at the
/// bound with no row; a reply by a writer outside G gives up the same way.
#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn t_as7_transient_states_defer() {
    let test = "t_as7_fanout_transient";
    let Some(db) = fresh_db(test).await else {
        return;
    };
    let (f, aud) = world(&db.pool, test, "list", &[BOB, CAROL]).await;
    let pool = f.pool.clone();
    subscribe(&pool, BOB, ALICE, true, true).await;
    let w = worker(&f);
    let bob = Some(vec![BOB.to_string()]);

    // The watermark stale: the post waits.
    let uri = fresh_post(&f, ALICE, "3kas7wm22222", &aud, None).await;
    deferred(&f, &worker_aged(&f, 600), &uri, None, "watermark").await;
    assert_eq!(run(&w, &uri).await, notified(1));

    // The author not owner-ready.
    let uri = fresh_post(&f, ALICE, "3kas7own2222", &aud, None).await;
    exec(&pool, &format!("DELETE FROM private_owner_ready WHERE did = '{ALICE}'")).await;
    deferred(&f, &w, &uri, None, "author not ready").await;
    f.ready(ALICE).await;
    assert_eq!(run(&w, &uri).await, notified(1));

    // The audience owner not ready: Alice's reply in Bob's thread, under
    // Bob's audience; Carol subscribes (Bob has the reply notification).
    let bob_aud = f.audience(BOB, "3kaudbob2222", Some("everyone"), &[]).await;
    let bob_root = f.private_post(BOB, "3kas7bobroot", &bob_aud, None).await;
    f.ready(BOB).await;
    subscribe(&pool, CAROL, ALICE, true, true).await;
    let uri = fresh_post(&f, ALICE, "3kas7aud2222", &bob_aud, Some((&bob_root, &bob_root))).await;
    exec(&pool, &format!("DELETE FROM private_owner_ready WHERE did = '{BOB}'")).await;
    deferred(&f, &w, &uri, None, "audience owner not ready").await;
    f.ready(BOB).await;
    assert_eq!(run(&w, &uri).await, notified(1));
    assert_eq!(recipients(&pool, &uri).await, [CAROL]);
    exec(&pool, &format!("DELETE FROM private_activity_subscriptions WHERE subscriber_did = '{CAROL}'")).await;

    // The author's head behind.
    let uri = fresh_post(&f, ALICE, "3kas7head222", &aud, None).await;
    exec(&pool, &format!("UPDATE sync_state SET head_rev = '3kzzzzzzzzzz3' WHERE did = '{ALICE}'")).await;
    deferred(&f, &w, &uri, None, "author head").await;
    exec(&pool, &format!("UPDATE sync_state SET head_rev = '{REV}' WHERE did = '{ALICE}'")).await;
    assert_eq!(run(&w, &uri).await, notified(1));

    // The audience record not stored yet (Q-3).
    let missing = format!("at://{ALICE}/{NSID_AUDIENCE}/3kaudlate222");
    let uri = fresh_post(&f, ALICE, "3kas7late222", &missing, None).await;
    deferred(&f, &w, &uri, None, "audience not stored").await;
    f.audience(ALICE, "3kaudlate222", Some("list"), &[BOB]).await;
    assert_eq!(run(&w, &uri).await, notified(1));

    // The subscriber not ready, then its head behind: it alone waits.
    let uri = fresh_post(&f, ALICE, "3kas7subr222", &aud, None).await;
    exec(&pool, &format!("UPDATE members SET ready_at = NULL WHERE did = '{BOB}'")).await;
    deferred(&f, &w, &uri, bob.clone(), "subscriber not ready").await;
    exec(&pool, &format!("UPDATE members SET ready_at = now() WHERE did = '{BOB}'")).await;
    assert_eq!(run(&w, &uri).await, notified(1));
    let uri = fresh_post(&f, ALICE, "3kas7subh222", &aud, None).await;
    exec(&pool, &format!("UPDATE sync_state SET head_rev = '3kzzzzzzzzzz3' WHERE did = '{BOB}'")).await;
    deferred(&f, &w, &uri, bob.clone(), "subscriber head").await;
    exec(&pool, &format!("UPDATE sync_state SET head_rev = '{REV}' WHERE did = '{BOB}'")).await;
    assert_eq!(run(&w, &uri).await, notified(1));

    // Never cleared: the bound ends it, with no row.
    let uri = fresh_post(&f, ALICE, "3kas7never22", &aud, None).await;
    exec(&pool, &format!("UPDATE members SET ready_at = NULL WHERE did = '{BOB}'")).await;
    deferred(&f, &w, &uri, bob.clone(), "first attempt").await;
    aged(&pool, &uri, 110).await;
    assert_eq!(run(&w, &uri).await, Finish::Done { outcome: "gave_up", rows: 0 });
    assert_eq!(queue_row(&pool, &uri).await.map(|r| r.2), Some(true));
    exec(&pool, &format!("UPDATE members SET ready_at = now() WHERE did = '{BOB}'")).await;
    assert_eq!(w.run_uri(&uri).await.expect("run"), None, "no later attempt");
    assert!(recipients(&pool, &uri).await.is_empty());

    // A reply by a writer outside G never becomes servable: given up.
    backfilled(&pool, CAROL).await;
    exec(&pool, &follow_sql(BOB, CAROL)).await;
    subscribe(&pool, BOB, CAROL, true, true).await;
    exec(&pool, &format!("UPDATE private_audiences SET members = ARRAY['{BOB}'] WHERE uri = '{aud}'")).await;
    let root = fresh_post(&f, ALICE, "3kas7root222", &aud, None).await;
    let uri = fresh_post(&f, CAROL, "3kas7out2222", &aud, Some((&root, &root))).await;
    deferred(&f, &w, &uri, None, "writer outside G").await;
    aged(&pool, &uri, 110).await;
    assert_eq!(run(&w, &uri).await, Finish::Done { outcome: "gave_up", rows: 0 });
    db.drop_db().await;
}

/// T-AS7b: a row claimed 121 s after its first store is `expired`, a first
/// attempt or a retry, with no row even for a subscriber in the audience;
/// one claimed before the bound runs.
#[tokio::test]
async fn t_as7b_the_absolute_bound() {
    let test = "t_as7b_fanout_bound";
    let Some(db) = fresh_db(test).await else {
        return;
    };
    let (f, aud) = world(&db.pool, test, "list", &[BOB]).await;
    subscribe(&f.pool, BOB, ALICE, true, false).await;
    let w = worker(&f);
    let first = fresh_post(&f, ALICE, "3kas7bfirst2", &aud, None).await;
    aged(&f.pool, &first, 121).await;
    assert_eq!(run(&w, &first).await, Finish::Done { outcome: "expired", rows: 0 });
    let retry = fresh_post(&f, ALICE, "3kas7bretry2", &aud, None).await;
    exec(&f.pool, &format!("UPDATE private_post_fanout SET attempts = 2, pending = ARRAY['{BOB}'] WHERE uri = '{retry}'")).await;
    aged(&f.pool, &retry, 121).await;
    assert_eq!(run(&w, &retry).await, Finish::Done { outcome: "expired", rows: 0 });
    for uri in [&first, &retry] {
        assert!(recipients(&f.pool, uri).await.is_empty());
    }
    let inside = fresh_post(&f, ALICE, "3kas7binside", &aud, None).await;
    aged(&f.pool, &inside, 115).await;
    assert_eq!(run(&w, &inside).await, notified(1));
    db.drop_db().await;
}

/// T-AS7c: under O's own `following` audience with O's follows not
/// verified, the post waits and no subscriber is decided; verified, the
/// followed subscriber gets the row and one O does not follow is refused
/// for good.
#[tokio::test]
async fn t_as7c_following_waits_for_verification() {
    let test = "t_as7c_fanout_following";
    let Some(db) = fresh_db(test).await else {
        return;
    };
    let (f, aud) = world(&db.pool, test, "following", &[]).await;
    exec(&f.pool, &follow_sql(ALICE, BOB)).await;
    subscribe(&f.pool, BOB, ALICE, true, false).await;
    subscribe(&f.pool, CAROL, ALICE, true, false).await;
    set_follows_verified(&f.pool, ALICE, false).await;
    let w = worker(&f);
    let uri = fresh_post(&f, ALICE, "3kas7cpost22", &aud, None).await;
    deferred(&f, &w, &uri, None, "following unverified").await;
    set_follows_verified(&f.pool, ALICE, true).await;
    assert_eq!(run(&w, &uri).await, notified(1));
    assert_eq!(recipients(&f.pool, &uri).await, [BOB]);
    let again = fresh_post(&f, ALICE, "3kas7cagain2", &aud, None).await;
    assert_eq!(run(&w, &again).await, notified(1), "verified from the start");
    assert_eq!(queue_row(&f.pool, &again).await, Some((1, Some(vec![]), true)), "Carol refused, not pending");
    db.drop_db().await;
}

/// T-AS7d: an attempt starting 118 s after the store, held before its
/// verdicts, is cut at the bound, writes nothing and runs no more.
#[tokio::test]
async fn t_as7d_the_attempt_ends_at_the_bound() {
    let test = "t_as7d_fanout_cut";
    let Some(db) = fresh_db(test).await else {
        return;
    };
    let (f, aud) = world(&db.pool, test, "list", &[BOB]).await;
    subscribe(&f.pool, BOB, ALICE, true, false).await;
    let w = worker(&f);
    let uri = fresh_post(&f, ALICE, "3kas7dpost22", &aud, None).await;
    aged(&f.pool, &uri, 118).await;
    let held = hooks::arm(hooks::FANOUT_BEFORE_VERDICT, &uri);
    let start = Instant::now();
    assert_eq!(run(&w, &uri).await, Finish::Done { outcome: "gave_up", rows: 0 });
    assert!(start.elapsed() < Duration::from_secs(10), "{:?}", start.elapsed());
    assert_eq!(held.hits(), 1);
    drop(held);
    assert!(recipients(&f.pool, &uri).await.is_empty());
    assert_eq!(w.run_uri(&uri).await.expect("run"), None);
    db.drop_db().await;
}

/// T-AS8: what fact 8 refuses is refused for good: blocks either way
/// (direct, private, list), a block with the thread's owner, a `followers`
/// audience the subscriber does not follow. A `!hide` label on the post is
/// a post-level fault: it waits and gives up, with no row.
#[tokio::test]
async fn t_as8_definitive_refusals() {
    let test = "t_as8_fanout_definitive";
    let Some(db) = fresh_db(test).await else {
        return;
    };
    let (f, aud) = world(&db.pool, test, "everyone", &[]).await;
    let pool = f.pool.clone();
    subscribe(&pool, BOB, ALICE, true, true).await;
    let w = worker(&f);
    // Dave's thread under Dave's audiences, Alice a writer in each.
    exec(&pool, &follow_sql(ALICE, DAVE)).await;
    let dave_everyone = f.audience(DAVE, "3kauddave222", Some("everyone"), &[]).await;
    let dave_followers = f.audience(DAVE, "3kauddavef22", Some("followers"), &[]).await;
    let dave_root = f.private_post(DAVE, "3kas8dave222", &dave_everyone, None).await;
    let dave_froot = f.private_post(DAVE, "3kas8davef22", &dave_followers, None).await;
    f.ready(DAVE).await;
    // (case, setup, teardown, the thread replied in, the audience).
    type Case<'a> = (&'a str, String, String, Option<(&'a str, &'a str)>, &'a str);
    let cases: Vec<Case> = vec![
        ("Bob blocks Alice", block_sql(BOB, ALICE), "DELETE FROM blocks".into(), None, aud.as_str()),
        ("Alice blocks Bob", block_sql(ALICE, BOB), "DELETE FROM blocks".into(), None, aud.as_str()),
        ("a list block", list_block_sql(BOB, CAROL, "3kmodlist222", &[ALICE]), "DELETE FROM list_references".into(), None, aud.as_str()),
        (
            "Bob blocks the thread's owner",
            block_sql(BOB, DAVE),
            "DELETE FROM blocks".into(),
            Some((dave_root.as_str(), dave_root.as_str())),
            dave_everyone.as_str(),
        ),
        (
            "followers, Bob does not follow Dave",
            String::new(),
            String::new(),
            Some((dave_froot.as_str(), dave_froot.as_str())),
            dave_followers.as_str(),
        ),
    ];
    for (i, (case, setup, teardown, reply, audience)) in cases.into_iter().enumerate() {
        if !setup.is_empty() {
            exec(&pool, &setup).await;
        }
        let uri = fresh_post(&f, ALICE, &format!("3kas8case{i:02}2"), audience, reply).await;
        assert_eq!(run(&w, &uri).await, notified(0), "{case}");
        assert_eq!(queue_row(&pool, &uri).await, Some((1, Some(vec![]), true)), "{case}: not pending");
        assert!(recipients(&pool, &uri).await.is_empty(), "{case}");
        if !teardown.is_empty() {
            exec(&pool, &teardown).await;
        }
    }
    // The control: with Bob following Dave, the followers reply notifies.
    exec(&pool, &follow_sql(BOB, DAVE)).await;
    let uri = fresh_post(&f, ALICE, "3kas8ctrl222", &dave_followers, Some((&dave_froot, &dave_froot))).await;
    assert_eq!(run(&w, &uri).await, notified(1));
    // `!hide` on the post: no one reads it, so it waits, then gives up.
    let uri = fresh_post(&f, ALICE, "3kas8hide222", &aud, None).await;
    exec(&pool, &format!("INSERT INTO labels (subject, val) VALUES ('{uri}', '!hide')")).await;
    deferred(&f, &w, &uri, None, "!hide").await;
    aged(&pool, &uri, 110).await;
    assert_eq!(run(&w, &uri).await, Finish::Done { outcome: "gave_up", rows: 0 });
    db.drop_db().await;
}

/// Run `uri`'s attempt held before its write, until [`release`].
async fn held_at_write(w: &Fanout, uri: &str) -> (hooks::Armed, tokio::task::JoinHandle<Finish>) {
    let armed = hooks::arm(hooks::FANOUT_BEFORE_WRITE, uri);
    let (w, u) = (w.clone(), uri.to_string());
    let task = tokio::spawn(async move { w.run_uri(&u).await.expect("run").expect("queued") });
    armed.reached().await;
    (armed, task)
}

async fn release(held: (hooks::Armed, tokio::task::JoinHandle<Finish>)) -> Finish {
    held.0.release();
    held.1.await.expect("join")
}

/// T-AS9: what changes between the verdict and the write is checked again:
/// the post deleted (no orphan), unsubscribed, the kind turned off and on,
/// a block, the member removed: no row.
#[tokio::test]
async fn t_as9_the_write_checks_again() {
    let test = "t_as9_fanout_recheck";
    let Some(db) = fresh_db(test).await else {
        return;
    };
    let (f, aud) = world(&db.pool, test, "list", &[BOB]).await;
    let pool = f.pool.clone();
    let w = worker(&f);
    let changes = [
        ("unsubscribed", format!("DELETE FROM private_activity_subscriptions WHERE subscriber_did = '{BOB}'")),
        // On again in a later second than the post's (the rule's precision).
        (
            "off and on again",
            format!("UPDATE private_activity_subscriptions SET post_since = now() + interval '1 second' WHERE subscriber_did = '{BOB}'"),
        ),
        ("a block", block_sql(ALICE, BOB)),
        ("the member removed", format!("DELETE FROM members WHERE did = '{BOB}'")),
    ];
    for (i, (case, change)) in changes.iter().enumerate() {
        subscribe(&pool, BOB, ALICE, true, false).await;
        let uri = fresh_post(&f, ALICE, &format!("3kas9case{i:02}2"), &aud, None).await;
        let held = held_at_write(&w, &uri).await;
        exec(&pool, change).await;
        assert_eq!(release(held).await, notified(0), "{case}");
        assert!(recipients(&pool, &uri).await.is_empty(), "{case}");
    }
    exec(&pool, "DELETE FROM blocks").await;
    exec(&pool, &format!("INSERT INTO members (did, source, ready_at) VALUES ('{BOB}', 'admin', now())")).await;
    subscribe(&pool, BOB, ALICE, true, false).await;
    // The post deleted: no row, and no orphan.
    let uri = fresh_post(&f, ALICE, "3kas9gone222", &aud, None).await;
    let held = held_at_write(&w, &uri).await;
    f.apply_op(ALICE, Action::Delete, NSID_POST_PRIVATE, "3kas9gone222", REV2, None).await;
    assert_eq!(release(held).await, notified(0));
    let orphans: i64 = sqlx::query_scalar("SELECT count(*) FROM private_notifications WHERE record_uri = $1").bind(&uri).fetch_one(&pool).await.expect("n");
    assert_eq!(orphans, 0);
    // The control: nothing changed, the row is written.
    let uri = fresh_post(&f, ALICE, "3kas9ctrl222", &aud, None).await;
    let held = held_at_write(&w, &uri).await;
    assert_eq!(release(held).await, notified(1));
    db.drop_db().await;
}

/// T-AS13: a crashed run is due again after its lease; an attempt held
/// past its limit ends deferred; no more rows are claimed than slots; a
/// notify wakes the worker with no poll; done rows are swept after two
/// days; a post re-stored after its delete does not fan out again.
#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn t_as13_worker_mechanics() {
    use crate::db::retention::{RetentionSettings, RetentionTask};

    let test = "t_as13_fanout_worker";
    let Some(db) = fresh_db(test).await else {
        return;
    };
    let (f, aud) = world(&db.pool, test, "list", &[BOB]).await;
    let pool = f.pool.clone();
    subscribe(&pool, BOB, ALICE, true, false).await;
    let w = worker(&f);

    // A crash: the row stays leased, then is due again.
    let uri = fresh_post(&f, ALICE, "3kas13crash2", &aud, None).await;
    let crash = hooks::arm_times(hooks::FANOUT_CRASH, &uri, 1);
    assert_eq!(run(&w, &uri).await, Finish::Crashed);
    drop(crash);
    assert_eq!(queue_row(&pool, &uri).await, Some((1, None, false)));
    assert!(w.claim_due(2).await.expect("claim").is_empty(), "leased");
    exec(&pool, &format!("UPDATE private_post_fanout SET not_before = now() - interval '1 second' WHERE uri = '{uri}'")).await;
    let claimed = w.claim_due(2).await.expect("claim");
    assert_eq!(claimed.iter().map(|c| (c.uri.as_str(), c.attempts)).collect::<Vec<_>>(), [(uri.as_str(), 2)]);
    assert_eq!(w.run_claimed(claimed[0].clone()).await.expect("run"), notified(1));

    // Held past the attempt limit: deferred, nothing decided.
    let uri = fresh_post(&f, ALICE, "3kas13held22", &aud, None).await;
    let held = hooks::arm(hooks::FANOUT_BEFORE_VERDICT, &uri);
    let short = w.clone().with_attempt_limit(Duration::from_secs(1));
    assert_eq!(run(&short, &uri).await, Finish::Retry { rows: 0, pending: None });
    drop(held);
    assert!(recipients(&pool, &uri).await.is_empty());

    // No more claimed than slots.
    exec(&pool, "UPDATE private_post_fanout SET done_at = now()").await;
    for i in 0..3 {
        fresh_post(&f, ALICE, &format!("3kas13slot{i}2"), &aud, None).await;
    }
    exec(&pool, "UPDATE private_post_fanout SET not_before = now() - interval '1 second' WHERE done_at IS NULL").await;
    assert_eq!(w.claim_due(2).await.expect("claim").len(), 2);
    assert_eq!(w.claim_due(2).await.expect("claim").len(), 1, "the third; the first two are leased");
    assert_eq!(w.claim_due(1).await.expect("claim").len(), 0);

    // A notify wakes the worker; the poll does not.
    exec(&pool, "UPDATE private_post_fanout SET done_at = now()").await;
    let running = w.clone().with_poll(Duration::from_secs(3600)).spawn();
    tokio::time::sleep(Duration::from_millis(500)).await;
    let gone = format!("at://{ALICE}/{NSID_POST_PRIVATE}/3kas13gone22");
    exec(&pool, &format!("INSERT INTO private_post_fanout (uri, author_did, not_before) VALUES ('{gone}', '{ALICE}', now())")).await;
    tokio::time::sleep(Duration::from_millis(1500)).await;
    assert_eq!(queue_row(&pool, &gone).await, Some((0, None, false)), "no notify, no run");
    exec(&pool, "SELECT pg_notify('private_fanout', '')").await;
    let mut woke = false;
    for _ in 0..50 {
        if queue_row(&pool, &gone).await.is_some_and(|r| r.2) {
            woke = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    running.abort();
    assert!(woke, "the notify woke the worker");

    // Done rows older than two days are swept.
    exec(&pool, &format!(
        "INSERT INTO private_post_fanout (uri, author_did, not_before, done_at) VALUES \
         ('at://{ALICE}/{NSID_POST_PRIVATE}/3kold', '{ALICE}', now(), now() - interval '3 days'), \
         ('at://{ALICE}/{NSID_POST_PRIVATE}/3krecent', '{ALICE}', now(), now() - interval '1 day')"
    ))
    .await;
    let scope = scope_of(&f);
    let settings = RetentionSettings { tombstone_days: 7, notification_days: 90, max_age_days: 0, trend_aggregate_hours: 72 };
    let report = crate::private_tier::retention::PrivateRetention::new(scope.clone()).run(&pool, &scope, &settings).await.expect("retention");
    assert!(report.deleted.contains(&("private_post_fanout", 1)), "{report:?}");
    assert_eq!(queue_row(&pool, &format!("at://{ALICE}/{NSID_POST_PRIVATE}/3kold")).await, None);
    assert!(queue_row(&pool, &format!("at://{ALICE}/{NSID_POST_PRIVATE}/3krecent")).await.is_some());

    // Deleted and stored again within two days: no second fan-out.
    let uri = fresh_post(&f, ALICE, "3kas13again2", &aud, None).await;
    assert_eq!(run(&w, &uri).await, notified(1));
    f.apply_op(ALICE, Action::Delete, NSID_POST_PRIVATE, "3kas13again2", REV2, None).await;
    assert!(recipients(&pool, &uri).await.is_empty(), "the delete took the row");
    let record = fresh_record(&f, ALICE, "3kas13again2", &aud, None).await;
    f.apply_op(ALICE, Action::Create, NSID_POST_PRIVATE, "3kas13again2", REV3, Some(record)).await;
    let stored: bool = sqlx::query_scalar("SELECT EXISTS (SELECT 1 FROM private_posts WHERE uri = $1)").bind(&uri).fetch_one(&pool).await.expect("stored");
    assert!(stored, "stored again");
    assert_eq!(w.run_uri(&uri).await.expect("run"), None, "its queue row is done");
    assert!(recipients(&pool, &uri).await.is_empty());
    db.drop_db().await;
}

/// T-AS10: a `subscribed-post` row lists under All and under its reason,
/// `getUnreadCount {reasons: [subscribed-post]}` counts it, `subscribedPost`
/// `list: false` hides it from both, and so does a later mute of the
/// author.
#[tokio::test]
async fn t_as10_read_time() {
    let test = "t_as10_fanout_read";
    let Some(db) = fresh_db(test).await else {
        return;
    };
    let (f, aud) = world(&db.pool, test, "list", &[BOB]).await;
    subscribe(&f.pool, BOB, ALICE, true, false).await;
    let uri = fresh_post(&f, ALICE, "3kas10post22", &aud, None).await;
    assert_eq!(run(&worker(&f), &uri).await, notified(1));
    let mut app = f.app.clone();
    app.limits = roomy_limits();
    let base = serve(f.state_of(app, Some(Duration::from_secs(1)), Duration::ZERO)).await;
    let listed = |query: &'static str| {
        let base = base.clone();
        async move {
            let (_, page, _) = json_of(&base, LIST_NOTIFICATIONS, query, BOB).await;
            page["notifications"]
                .as_array()
                .expect("notifications")
                .iter()
                .map(|n| (n["reason"].as_str().expect("reason").to_string(), n["uri"].as_str().expect("uri").to_string()))
                .collect::<Vec<_>>()
        }
    };
    let counted = |query: &'static str| {
        let base = base.clone();
        async move { json_of(&base, GET_UNREAD_COUNT, query, BOB).await.1["count"].as_i64().expect("count") }
    };
    let one = vec![("subscribed-post".to_string(), uri.clone())];
    assert_eq!(listed("").await, one);
    assert_eq!(listed("reasons=subscribed-post").await, one);
    assert_eq!(counted("reasons=subscribed-post").await, 1, "the reason filter knows subscribed-post");
    assert_eq!(counted("").await, 1);
    let (_, page, _) = json_of(&base, LIST_NOTIFICATIONS, "", BOB).await;
    assert_eq!(page["notifications"][0]["record"]["$type"], json!(NSID_POST_PRIVATE), "the post, read for Bob: {page}");

    let put = |list: bool| {
        let base = base.clone();
        async move {
            let (status, body) =
                super::post(&base, "app.nearhorizon.private.putNotificationPreferences", &json!({"subscribedPost": {"list": list, "push": true}}), BOB).await;
            assert_eq!(status, 200, "{body}");
        }
    };
    put(false).await;
    assert!(listed("").await.is_empty());
    assert!(listed("reasons=subscribed-post").await.is_empty());
    assert_eq!((counted("").await, counted("reasons=subscribed-post").await), (0, 0));
    put(true).await;
    assert_eq!(listed("").await, one);
    exec(&f.pool, &format!("INSERT INTO mutes (viewer_did, subject_did, scope) VALUES ('{BOB}', '{ALICE}', 'full')")).await;
    assert!(listed("").await.is_empty());
    assert_eq!(counted("reasons=subscribed-post").await, 0);
    db.drop_db().await;
}
