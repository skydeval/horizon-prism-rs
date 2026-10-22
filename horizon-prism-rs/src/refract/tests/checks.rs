//! The checker and the labels against fakes (refract design §13: T-4,
//! T-5, T-8b, T-8c, T-8j, T-8k, T-8l, T-8v, T-8ao, T-12, T-12c, T-12-r4).

use chrono::{TimeDelta, Utc};
use serde_json::json;

use super::harness::{Harness, LABELER, OTHER, OTHER_LABELER, SUBJECT, tier1, tier2};
use crate::aggregations::ScopeSets;
use crate::refract::checker::Claimed;
use crate::test_support::fresh_db;

async fn claimed(h: &Harness, did: &str) -> Claimed {
    sqlx::query_as(
        "SELECT did, state, version, due_seq, community, pds_host, attempts, last_verdict, grace_until, unverified_since, \
           awaiting_labels_since, candidate_since FROM refract_dids WHERE did = $1",
    )
    .bind(did)
    .fetch_one(&h.pool)
    .await
    .expect("row")
}

/// T-4 end to end, tier 1 with the labeler read through the AppView: a
/// candidate that passes every rule is restored (event, backfill job,
/// `actors` facts, R-live); a gone account label from the AppView ends it;
/// an unknown leaves the last good state.
#[tokio::test]
async fn t4_tier1_candidate_restored_then_ended_by_a_label() {
    let Some(db) = fresh_db("refract_t4_tier1").await else {
        return;
    };
    let h = Harness::new(&db.pool, ScopeSets::default(), vec![tier1()]).await;
    h.all_pass();
    h.row(SUBJECT, "sky", "candidate").await;
    assert_eq!(h.check(SUBJECT).await, 1);
    assert_eq!(h.state(SUBJECT).await.as_deref(), Some("restored"));
    assert_eq!(h.count("SELECT count(*) FROM refract_events WHERE to_state = 'restored'").await, 1);
    assert_eq!(h.count("SELECT count(*) FROM backfill_jobs WHERE depth = 'refract' AND state = 'queued'").await, 1);
    assert_eq!(h.count("SELECT count(*) FROM actors WHERE account_status = 'active' AND pds_endpoint IS NOT NULL").await, 1);
    assert_eq!(h.sets().await.restored.get(SUBJECT).map(String::as_str), Some("sky"));
    let calls = h.calls();
    assert!(calls.contains(&"bluesky.getProfile".to_string()) && calls.contains(&"pds.getRepoStatus".to_string()), "{calls:?}");
    // An unknown keeps the last good state, with backoff.
    h.set(|w| w.bluesky = Some((502, json!({}))));
    h.check(SUBJECT).await;
    assert_eq!(h.state(SUBJECT).await.as_deref(), Some("restored"));
    let (attempts, next): (i32, chrono::DateTime<Utc>) =
        sqlx::query_as("SELECT attempts, next_check_at FROM refract_dids WHERE did = $1").bind(SUBJECT).fetch_one(&h.pool).await.expect("row");
    assert_eq!(attempts, 1);
    assert!(next > Utc::now() + TimeDelta::minutes(4));
    // The community's labeler takes the account down: ending.
    h.all_pass();
    h.set(|w| w.community = Some((200, json!({"did": SUBJECT, "labels": [{"src": LABELER, "uri": SUBJECT, "val": "!takedown", "cts": "2026-10-22T00:00:00Z"}]}))));
    h.check(SUBJECT).await;
    assert_eq!(h.state(SUBJECT).await.as_deref(), Some("ending"));
    let reason: Option<String> = sqlx::query_scalar("SELECT ended_reason FROM refract_dids WHERE did = $1").bind(SUBJECT).fetch_one(&h.pool).await.expect("r");
    assert_eq!(reason.as_deref(), Some("rule_4"));
    assert_eq!(h.count("SELECT count(*) FROM backfill_jobs WHERE depth = 'refract' AND state = 'cancelled'").await, 1, "the refract job is cancelled");
    let sets = h.sets().await;
    assert!(!sets.restored.contains_key(SUBJECT));
    assert!(sets.refract_hidden.accounts[SUBJECT].ending);
    db.drop_db().await;
}

/// T-8j, T-8k, T-8v: a candidate's checks write no `actors` row; the
/// checker never claims `rejected`, `ending` or a pending DID; a pending
/// DID whose Bluesky ban lifted is not ended by a check.
#[tokio::test]
async fn t8j_t8k_t8v_what_the_checker_claims_and_writes() {
    let Some(db) = fresh_db("refract_t8jkv").await else {
        return;
    };
    let h = Harness::new(&db.pool, ScopeSets::default(), vec![tier1()]).await;
    h.all_pass();
    h.set(|w| w.bluesky = Some((502, json!({}))));
    h.row(SUBJECT, "sky", "candidate").await;
    h.check(SUBJECT).await;
    assert_eq!(h.state(SUBJECT).await.as_deref(), Some("candidate"));
    assert_eq!(h.count("SELECT count(*) FROM actors").await, 0, "no actors row for a candidate");
    for state in ["rejected", "ending"] {
        sqlx::query("UPDATE refract_dids SET state = $2 WHERE did = $1").bind(SUBJECT).bind(state).execute(&h.pool).await.expect("state");
        assert_eq!(h.check(SUBJECT).await, 0, "{state} is never claimed");
    }
    // A pending restored DID, Bluesky now serving it: never claimed.
    sqlx::query("UPDATE refract_dids SET state = 'restored' WHERE did = $1").bind(SUBJECT).execute(&h.pool).await.expect("state");
    sqlx::query("INSERT INTO refract_pending (scope) VALUES ('feature')").execute(&h.pool).await.expect("scope");
    sqlx::query("INSERT INTO refract_pending_dids (scope, did) VALUES ('feature', $1)").bind(SUBJECT).execute(&h.pool).await.expect("pending");
    h.set(|w| w.bluesky = Some((200, json!({"did": SUBJECT}))));
    assert_eq!(h.check(SUBJECT).await, 0);
    assert_eq!(h.state(SUBJECT).await.as_deref(), Some("restored"));
    db.drop_db().await;
}

/// T-5 and T-8c: a verdict computed against a stale row (version moved)
/// writes nothing; a "due now" during an in-flight check leaves
/// `next_check_at = now`.
#[tokio::test]
async fn t5_t8c_stale_verdicts_and_due_now_during_a_check() {
    let Some(db) = fresh_db("refract_t5_t8c").await else {
        return;
    };
    let h = Harness::new(&db.pool, ScopeSets::default(), vec![tier1()]).await;
    h.all_pass();
    h.row(SUBJECT, "sky", "candidate").await;
    let row = claimed(&h, SUBJECT).await;
    sqlx::query("UPDATE refract_dids SET version = version + 1 WHERE did = $1").bind(SUBJECT).execute(&h.pool).await.expect("bump");
    h.refract.check(row).await.expect("check");
    assert_eq!(h.state(SUBJECT).await.as_deref(), Some("candidate"), "a stale verdict is a no-op");
    assert_eq!(h.count("SELECT count(*) FROM refract_events").await, 0);
    // Due now while a check of a restored DID is in flight.
    sqlx::query("UPDATE refract_dids SET state = 'restored' WHERE did = $1").bind(SUBJECT).execute(&h.pool).await.expect("state");
    let row = claimed(&h, SUBJECT).await;
    crate::refract::make_due(&h.pool, &[SUBJECT.to_string()]).await.expect("due");
    h.refract.check(row).await.expect("check");
    let late: bool = sqlx::query_scalar("SELECT next_check_at <= now() FROM refract_dids WHERE did = $1").bind(SUBJECT).fetch_one(&h.pool).await.expect("next");
    assert!(late, "due again at once");
    // Due now leaves a rejected row's re-eligibility alone (T-8q).
    sqlx::query("UPDATE refract_dids SET state = 'rejected', next_check_at = now() + interval '1 day' WHERE did = $1").bind(SUBJECT).execute(&h.pool).await.expect("state");
    assert_eq!(crate::refract::make_due(&h.pool, &[SUBJECT.to_string()]).await.expect("due"), 0);
    db.drop_db().await;
}

/// T-12 (labeler source): the targeted fetch stores signed labels; a
/// negation removes; an expired gone label counts for nothing; a gone
/// label failing verification is stored unverified, hides at once and
/// makes rule 4 unknown (T-8b); a non-gone one failing is dropped; a
/// record gone label hides the record and does not end the restore; only
/// the home labeler's labels are kept (T-12c).
#[tokio::test]
async fn t12_labeler_source_labels() {
    let Some(db) = fresh_db("refract_t12").await else {
        return;
    };
    let h = Harness::new(&db.pool, ScopeSets::default(), vec![tier2()]).await;
    h.all_pass();
    let post = format!("at://{SUBJECT}/app.bsky.feed.post/3k");
    let later = (Utc::now() + TimeDelta::days(1)).to_rfc3339();
    let earlier = (Utc::now() - TimeDelta::days(1)).to_rfc3339();
    let labels = vec![
        h.signed(json!({"ver": 1, "src": LABELER, "uri": post, "val": "!hide", "cts": "2026-10-22T00:00:00.000Z", "exp": later})),
        h.signed(json!({"ver": 1, "src": LABELER, "uri": SUBJECT, "val": "spam", "cts": "2026-10-22T00:00:00.000Z"})),
        h.signed(json!({"ver": 1, "src": LABELER, "uri": SUBJECT, "val": "!takedown", "cts": "2026-10-22T00:00:00.000Z", "exp": earlier})),
        h.signed(json!({"ver": 1, "src": LABELER, "uri": SUBJECT, "val": "porn", "cts": "2026-10-22T00:00:00.000Z"})),
        h.signed(json!({"ver": 1, "src": LABELER, "uri": SUBJECT, "val": "porn", "neg": true, "cts": "2026-10-22T00:01:00.000Z"})),
        // Another labeler's, and a forged non-gone one: dropped.
        h.signed(json!({"ver": 1, "src": OTHER_LABELER, "uri": SUBJECT, "val": "!takedown", "cts": "2026-10-22T00:00:00.000Z"})),
        json!({"ver": 1, "src": LABELER, "uri": SUBJECT, "val": "nudity", "cts": "2026-10-22T00:00:00.000Z", "sig": {"$bytes": "AAAA"}}),
    ];
    h.set(|w| w.labels = labels);
    h.row(SUBJECT, "north", "candidate").await;
    h.check(SUBJECT).await;
    assert_eq!(h.state(SUBJECT).await.as_deref(), Some("restored"), "an expired gone label and a record label do not end it");
    let stored: Vec<(String, String, bool, bool)> =
        sqlx::query_as("SELECT subject, val, gone, verified FROM refract_labels ORDER BY subject COLLATE \"C\", val COLLATE \"C\"").fetch_all(&h.pool).await.expect("labels");
    assert_eq!(
        stored,
        vec![
            (post.clone(), "!hide".into(), true, true),
            (SUBJECT.into(), "!takedown".into(), true, true),
            (SUBJECT.into(), "spam".into(), false, true),
        ],
        "the negated label is gone; another labeler's and a forged one are not stored"
    );
    let sets = h.sets().await;
    assert!(sets.refract_hidden.records.contains_key(&post), "the record is hidden");
    assert!(!sets.refract_hidden.accounts.get(SUBJECT).is_some_and(|r| r.active(Utc::now())), "an expired account label hides nothing");
    // A gone account label whose signature fails: stored unverified,
    // hidden at once, rule 4 unknown, `unverified_since` set.
    h.set(|w| w.labels = vec![json!({"ver": 1, "src": LABELER, "uri": SUBJECT, "val": "!takedown", "cts": "2026-10-22T00:02:00.000Z", "sig": {"$bytes": "AAAA"}})]);
    h.check(SUBJECT).await;
    assert_eq!(h.state(SUBJECT).await.as_deref(), Some("restored"));
    let (verified, since): (bool, Option<chrono::DateTime<Utc>>) = sqlx::query_as(
        "SELECT l.verified, d.unverified_since FROM refract_labels l JOIN refract_dids d ON d.did = l.did WHERE l.subject = $1 AND l.val = '!takedown'",
    )
    .bind(SUBJECT)
    .fetch_one(&h.pool)
    .await
    .expect("label");
    assert!(!verified && since.is_some());
    assert!(h.sets().await.refract_hidden.accounts[SUBJECT].active(Utc::now()), "fail closed: hidden at once");
    // Past `unverified_max_hours`: ending.
    sqlx::query("UPDATE refract_dids SET unverified_since = now() - interval '73 hours' WHERE did = $1").bind(SUBJECT).execute(&h.pool).await.expect("age");
    h.check(SUBJECT).await;
    assert_eq!(h.state(SUBJECT).await.as_deref(), Some("ending"));
    db.drop_db().await;
}

/// T-12-r4 (`appview` source): the profile-record labels are stored as
/// record labels; a missing `atproto-content-labelers` header makes the
/// labels unknown; a label whose `src` is another labeler is dropped; an
/// unsigned label is accepted, a signed one is verified.
#[tokio::test]
async fn t12_r4_appview_source_labels() {
    let Some(db) = fresh_db("refract_t12_r4").await else {
        return;
    };
    let h = Harness::new(&db.pool, ScopeSets::default(), vec![tier1()]).await;
    h.all_pass();
    let profile = format!("at://{SUBJECT}/app.bsky.actor.profile/self");
    let forged = json!({"src": LABELER, "uri": SUBJECT, "val": "spam", "cts": "2026-10-22T00:00:00Z", "sig": {"$bytes": "AAAA"}});
    h.set(|w| {
        w.community = Some((
            200,
            json!({"did": SUBJECT, "labels": [
                {"src": LABELER, "uri": profile, "val": "!hide", "cts": "2026-10-22T00:00:00Z"},
                {"src": OTHER_LABELER, "uri": SUBJECT, "val": "!takedown", "cts": "2026-10-22T00:00:00Z"},
                forged
            ]}),
        ));
        w.names_labeler = false;
    });
    h.row(SUBJECT, "sky", "candidate").await;
    h.check(SUBJECT).await;
    assert_eq!(h.state(SUBJECT).await.as_deref(), Some("candidate"), "no header: the labels are unknown");
    assert_eq!(h.count("SELECT count(*) FROM refract_labels").await, 0);
    h.set(|w| w.names_labeler = true);
    h.check(SUBJECT).await;
    assert_eq!(h.state(SUBJECT).await.as_deref(), Some("restored"), "another labeler's takedown never counts");
    let stored: Vec<(String, String)> = sqlx::query_as("SELECT subject, val FROM refract_labels").fetch_all(&h.pool).await.expect("labels");
    assert_eq!(stored, vec![(profile.clone(), "!hide".to_string())], "the unsigned record label is kept, the forged one dropped");
    assert!(h.sets().await.refract_hidden.records.contains_key(&profile));
    let profile_hidden = {
        let snap = crate::refract::RefractSnapshot::of(&h.sets().await);
        let check = crate::refract::RefractCheck::default();
        crate::refract::RefractView { snapshot: &snap, check: &check }.record_hidden(&profile)
    };
    assert!(profile_hidden, "a non-member outside S: the profile record is hidden by its label");
    db.drop_db().await;
}

/// T-12c: a DID moving between communities drops the old labeler's rows;
/// a gone label stored from another community's labeler never ends it.
#[tokio::test]
async fn t12c_labels_follow_the_home_community() {
    let Some(db) = fresh_db("refract_t12c").await else {
        return;
    };
    let mut north = tier2();
    north.pds_patterns = vec!["pds.north.invalid".into()];
    let mut sky = tier1();
    sky.labeler_did = Some(OTHER_LABELER.into());
    let h = Harness::new(&db.pool, ScopeSets::default(), vec![north, sky]).await;
    h.all_pass();
    h.row(SUBJECT, "north", "restored").await;
    sqlx::query("INSERT INTO refract_labels (labeler_did, subject, val, did, gone, verified) VALUES ($1, $2, '!takedown', $2, true, true)")
        .bind(LABELER)
        .bind(SUBJECT)
        .execute(&h.pool)
        .await
        .expect("label");
    // Its PDS (the fake, 127.0.0.1) now matches `sky`: the old labeler's
    // rows go, the community is rewritten, and it stays restored.
    h.check(SUBJECT).await;
    assert_eq!(h.state(SUBJECT).await.as_deref(), Some("restored"));
    let community: String = sqlx::query_scalar("SELECT community FROM refract_dids WHERE did = $1").bind(SUBJECT).fetch_one(&h.pool).await.expect("c");
    assert_eq!(community, "sky");
    assert_eq!(h.count("SELECT count(*) FROM refract_labels WHERE labeler_did = 'did:plc:labeleraaaaaaaaaaaaaaaaa'").await, 0);
    db.drop_db().await;
}

/// T-8ao: a label write racing a purge's end writes nothing (the row is
/// gone); T-8l: an event for a candidate-only DID only makes it due.
#[tokio::test]
async fn t8ao_t8l_label_writes_need_the_row() {
    let Some(db) = fresh_db("refract_t8ao").await else {
        return;
    };
    let mut sets = ScopeSets::default();
    std::sync::Arc::make_mut(&mut sets.restored).insert(SUBJECT.to_string(), "north".to_string());
    let h = Harness::new(&db.pool, sets, vec![tier2()]).await;
    h.set(|w| w.labels = vec![json!({"src": LABELER, "uri": SUBJECT, "val": "!takedown", "cts": "2026-10-22T00:00:00Z"})]);
    // First poll fast-forwards and stores nothing.
    assert_eq!(h.refract.poll(&tier2()).await.expect("poll"), "fast_forwarded");
    sqlx::query("DELETE FROM worker_state").execute(&h.pool).await.expect("cursor");
    sqlx::query("INSERT INTO worker_state (key, value) VALUES ('refract_label_cursor:did:plc:labeleraaaaaaaaaaaaaaaaa', '\"0\"')")
        .execute(&h.pool)
        .await
        .expect("cursor");
    // Memory still names the DID, but its row is gone (a purge ended).
    h.refract.poll(&tier2()).await.expect("poll");
    assert_eq!(h.count("SELECT count(*) FROM refract_labels").await, 0, "no orphan row");
    // With its row the poll stores it and makes it due.
    h.row(SUBJECT, "north", "restored").await;
    sqlx::query("UPDATE refract_dids SET next_check_at = now() + interval '1 day' WHERE did = $1").bind(SUBJECT).execute(&h.pool).await.expect("later");
    sqlx::query("UPDATE worker_state SET value = '\"0\"'").execute(&h.pool).await.expect("cursor");
    h.refract.poll(&tier2()).await.expect("poll");
    assert_eq!(h.count("SELECT count(*) FROM refract_labels").await, 1);
    assert_eq!(h.count("SELECT count(*) FROM refract_dids WHERE next_check_at <= now()").await, 1, "a gone account label makes it due");
    // T-8l.
    h.row(OTHER, "north", "candidate").await;
    assert!(crate::refract::make_due_on_event(&h.pool, OTHER).await.expect("event"), "candidate-only: nothing else is written");
    sqlx::query("INSERT INTO actors (did) VALUES ($1)").bind(OTHER).execute(&h.pool).await.expect("actor");
    assert!(!crate::refract::make_due_on_event(&h.pool, OTHER).await.expect("event"));
    assert!(!crate::refract::make_due_on_event(&h.pool, "did:plc:nobody").await.expect("event"));
    db.drop_db().await;
}
