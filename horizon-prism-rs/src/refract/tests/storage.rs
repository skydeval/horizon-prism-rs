//! Storage of restored DIDs (refract design §5, §6.2, §13: T-1, T-3, T-6,
//! T-7, T-7b, T-8, T-8ab, T-9, T-10, T-10b).

use std::sync::Arc;

use serde_json::{Value, json};
use sqlx::PgPool;

use super::harness::{Harness, SUBJECT, tier1};
use crate::aggregations::{ScopeSets, ScopeState};
use crate::config::ScopePolicy;
use crate::firehose::writer::{Action, ApplyOptions, OpOutcome, RawOp, WriteSource, Writer};
use crate::indexing::PublicMode;
use crate::private_tier::DisabledPrivateTier;
use crate::test_support::fresh_db;

const MEMBER: &str = "did:plc:memberaaaaaaaaaaaaaaaaaa";
const STRANGER: &str = "did:plc:strangeraaaaaaaaaaaaaaaa";
const CID: &str = "bafyreie5737gdxlw5i64vzichcalba3z2v5n6icifvx5xytvske7mr3hpm";

fn options() -> ApplyOptions {
    ApplyOptions { advance_applied_rev: false, dead_letter_readiness: false, attempts: 1, live: true, subscription_rules: false, source: WriteSource::Firehose }
}

fn create(collection: &str, rkey: &str, record: Value) -> RawOp {
    RawOp { action: Action::Create, collection: collection.into(), rkey: rkey.into(), cid: Some(CID.into()), record: Some(record) }
}

fn post(text: &str, reply_to: Option<&str>) -> Value {
    let mut v = json!({"$type": "app.bsky.feed.post", "text": text, "createdAt": "2026-10-22T00:00:00.000Z"});
    if let Some(parent) = reply_to {
        v["reply"] = json!({"root": {"uri": parent, "cid": CID}, "parent": {"uri": parent, "cid": CID}});
    }
    v
}

fn like(subject: &str) -> Value {
    json!({"$type": "app.bsky.feed.like", "subject": {"uri": subject, "cid": CID}, "createdAt": "2026-10-22T00:00:00.000Z"})
}

fn follow(subject: &str) -> Value {
    json!({"$type": "app.bsky.graph.follow", "subject": subject, "createdAt": "2026-10-22T00:00:00.000Z"})
}

fn sets(mode: PublicMode, restored: bool, member: bool, in_s: bool) -> ScopeSets {
    let mut s = ScopeSets::default();
    s.public.mode = mode;
    s.members.insert(MEMBER.into());
    s.scope.insert(MEMBER.into());
    if restored {
        Arc::make_mut(&mut s.restored).insert(SUBJECT.into(), "sky".into());
    }
    if member {
        s.members.insert(SUBJECT.into());
    }
    if member || in_s {
        s.scope.insert(SUBJECT.into());
    }
    s
}

async fn count(p: &PgPool, sql: &str) -> i64 {
    sqlx::query_scalar(sql).fetch_one(p).await.expect("count")
}

async fn exec(p: &PgPool, sql: &str) {
    sqlx::query(sql).execute(p).await.expect("exec");
}

/// The member's `members` row: the purge and the sweep read the table.
async fn member_row(p: &PgPool) {
    exec(p, "INSERT INTO members (did, source) VALUES ('did:plc:memberaaaaaaaaaaaaaaaaaa', 'admin') ON CONFLICT DO NOTHING").await;
}

fn writer(p: &PgPool, sets: ScopeSets) -> Writer {
    let scope = ScopeState::new(sets, ScopePolicy::MembersAndOneHop, 10_000);
    Writer::new(p.clone(), scope, Arc::new(DisabledPrivateTier))
}

/// The rows a restored non-member writes: a post, a reply to a member, a
/// like of a member's post, a like of a stranger's post, a follow of the
/// member and of a stranger, a profile.
async fn write_history(w: &Writer) {
    let member_post = format!("at://{MEMBER}/app.bsky.feed.post/3kmember");
    let stranger_post = format!("at://{STRANGER}/app.bsky.feed.post/3kstranger");
    let ops = vec![
        create("app.bsky.feed.post", "3kpost", post("hello", None)),
        create("app.bsky.feed.post", "3kreply", post("hi member", Some(&member_post))),
        create("app.bsky.feed.like", "3klikem", like(&member_post)),
        create("app.bsky.feed.like", "3klikes", like(&stranger_post)),
        create("app.bsky.graph.follow", "3kfollowm", follow(MEMBER)),
        create("app.bsky.graph.follow", "3kfollows", follow(STRANGER)),
        create("app.bsky.actor.profile", "self", json!({"$type": "app.bsky.actor.profile", "displayName": "Restored"})),
    ];
    let report = w.apply(SUBJECT, "3krev0000000a", &ops, options()).await.expect("apply");
    assert!(report.outcomes.iter().all(|o| *o == OpOutcome::Written), "{report:?}");
}

/// T-1, T-3: in an off mode a restored non-member's records are stored (a
/// full `posts` row); a candidate's (not in R-live) are not; a member in R
/// gets full post rows and its references.
#[tokio::test]
async fn t1_admission_through_the_writer() {
    let Some(db) = fresh_db("refract_t1_writer").await else {
        return;
    };
    let p = &db.pool;
    let w = writer(p, sets(PublicMode::Off, true, false, false));
    write_history(&w).await;
    assert_eq!(count(p, "SELECT count(*) FROM posts").await, 2);
    assert_eq!(count(p, "SELECT count(*) FROM likes").await, 2);
    assert_eq!(count(p, "SELECT count(*) FROM follows").await, 2);
    assert_eq!(count(p, "SELECT count(*) FROM actors WHERE profile_record IS NOT NULL").await, 1);
    // A candidate (not in R-live): refused under the off rule.
    let w = writer(p, sets(PublicMode::Off, false, false, false));
    let report = w.apply(SUBJECT, "3krev0000000b", &[create("app.bsky.feed.post", "3knew", post("x", None))], options()).await.expect("apply");
    assert!(matches!(report.outcomes[0], OpOutcome::Rejected(_)), "{report:?}");
    // A member in R: a full row and its reference.
    let w = writer(p, sets(PublicMode::Off, true, true, true));
    w.apply(SUBJECT, "3krev0000000c", &[create("app.bsky.feed.post", "3kmem", post("member post", None))], options()).await.expect("apply");
    assert_eq!(count(p, "SELECT count(*) FROM posts WHERE uri LIKE '%3kmem'").await, 1);
    assert_eq!(count(p, "SELECT count(*) FROM member_post_refs WHERE uri LIKE '%3kmem'").await, 1);
    db.drop_db().await;
}

/// T-9: a delete of a restored DID's record that finds no row still writes
/// a tombstone, so a later (backfilled) write of it is refused.
#[tokio::test]
async fn t9_a_delete_before_the_backfill_wins() {
    let Some(db) = fresh_db("refract_t9_tombstone").await else {
        return;
    };
    let p = &db.pool;
    let w = writer(p, sets(PublicMode::Off, true, false, false));
    let delete = RawOp { action: Action::Delete, collection: "app.bsky.feed.post".into(), rkey: "3kgone".into(), cid: None, record: None };
    w.apply(SUBJECT, "3krev0000000z", &[delete], options()).await.expect("delete");
    assert_eq!(count(p, "SELECT count(*) FROM record_tombstones").await, 1);
    let backfilled = ApplyOptions { live: false, source: WriteSource::Backfill, ..options() };
    let report = w.apply(SUBJECT, "3krev0000000a", &[create("app.bsky.feed.post", "3kgone", post("old", None))], backfilled).await.expect("write");
    assert_eq!(report.outcomes, [OpOutcome::Unchanged]);
    assert_eq!(count(p, "SELECT count(*) FROM posts").await, 0);
    db.drop_db().await;
}

async fn ending(h: &Harness) {
    exec(&h.pool, "INSERT INTO refract_dids (did, community, pds_host, state, source) VALUES ('did:plc:subjectaaaaaaaaaaaaaaaaa', 'sky', '127.0.0.1', 'ending', 'fill')").await;
}

/// T-7, T-8: off mode, a non-member outside S: after the Refract purge no
/// exception row authored by it remains; its follow of a member stays;
/// members' rows about it stay (D-7); no `purging_dids` row is written; a
/// purge of another kind makes it wait; the `refract_dids` row goes at the
/// end.
#[tokio::test]
async fn t7_t8_purge_off_mode_outside_s() {
    let Some(db) = fresh_db("refract_t7_purge_off").await else {
        return;
    };
    let p = &db.pool;
    member_row(p).await;
    let w = writer(p, sets(PublicMode::Off, true, false, false));
    write_history(&w).await;
    let ops = [
        create("app.bsky.feed.like", "3kmlike", like(&format!("at://{SUBJECT}/app.bsky.feed.post/3kpost"))),
        create("app.bsky.graph.follow", "3kmfollow", follow(SUBJECT)),
    ];
    w.apply(MEMBER, "3krev0000000a", &ops, options()).await.expect("member");
    let h = Harness::new(p, sets(PublicMode::Off, false, false, false), vec![tier1()]).await;
    ending(&h).await;
    exec(p, "INSERT INTO purging_dids (did, kind) VALUES ('did:plc:subjectaaaaaaaaaaaaaaaaa', 'out_of_scope')").await;
    assert!(!h.refract.purge(SUBJECT).await.expect("purge"));
    assert_eq!(count(p, "SELECT count(*) FROM posts").await, 2, "nothing deleted while it waits");
    exec(p, "DELETE FROM purging_dids").await;
    assert!(h.refract.purge_next().await.expect("purge"));
    assert_eq!(count(p, "SELECT count(*) FROM posts").await, 0);
    assert_eq!(count(p, &format!("SELECT count(*) FROM likes WHERE author_did = '{SUBJECT}'")).await, 0);
    assert_eq!(count(p, &format!("SELECT count(*) FROM follows WHERE author_did = '{SUBJECT}'")).await, 1, "its follow of a member stays");
    assert_eq!(count(p, &format!("SELECT count(*) FROM likes WHERE author_did = '{MEMBER}'")).await, 1, "a member's like of it stays (D-7)");
    assert_eq!(count(p, &format!("SELECT count(*) FROM follows WHERE author_did = '{MEMBER}'")).await, 1);
    assert_eq!(count(p, "SELECT count(*) FROM actors WHERE profile_record IS NOT NULL").await, 0, "its profile goes");
    assert_eq!(count(p, "SELECT count(*) FROM refract_dids").await, 0);
    assert_eq!(count(p, "SELECT count(*) FROM purging_dids").await, 0, "no purging_dids row was written");
    assert_eq!(count(p, "SELECT count(*) FROM refract_events WHERE to_state = 'ended'").await, 1);
    db.drop_db().await;
}

/// T-7b: On mode, a non-member outside S: its reply to a member, its like
/// of a member's post and its follow of a member survive (rule 2); its
/// other rows go.
#[tokio::test]
async fn t7b_purge_on_mode_keeps_rule_2_rows() {
    let Some(db) = fresh_db("refract_t7b_purge_on").await else {
        return;
    };
    let p = &db.pool;
    member_row(p).await;
    let w = writer(p, sets(PublicMode::On, true, false, false));
    write_history(&w).await;
    let h = Harness::new(p, sets(PublicMode::On, false, false, false), vec![tier1()]).await;
    ending(&h).await;
    assert!(h.refract.purge(SUBJECT).await.expect("purge"));
    let posts: Vec<String> = sqlx::query_scalar("SELECT uri FROM posts").fetch_all(p).await.expect("posts");
    assert_eq!(posts, [format!("at://{SUBJECT}/app.bsky.feed.post/3kreply")]);
    let likes: Vec<String> = sqlx::query_scalar("SELECT subject_uri FROM likes").fetch_all(p).await.expect("likes");
    assert_eq!(likes, [format!("at://{MEMBER}/app.bsky.feed.post/3kmember")]);
    let follows: Vec<String> = sqlx::query_scalar("SELECT subject_did FROM follows").fetch_all(p).await.expect("follows");
    assert_eq!(follows, [MEMBER]);
    db.drop_db().await;
}

/// T-8ab, T-6: off mode, a non-member in S keeps its profile and its
/// follow of a member; the rest goes. A member in R that ends loses only
/// its `posts` rows; its references and likes stay.
#[tokio::test]
async fn t8ab_t6_purge_of_s_dids_and_members() {
    let Some(db) = fresh_db("refract_t8ab").await else {
        return;
    };
    let p = &db.pool;
    member_row(p).await;
    let w = writer(p, sets(PublicMode::Off, true, false, true));
    write_history(&w).await;
    let h = Harness::new(p, sets(PublicMode::Off, false, false, true), vec![tier1()]).await;
    ending(&h).await;
    assert!(h.refract.purge(SUBJECT).await.expect("purge"));
    assert_eq!(count(p, "SELECT count(*) FROM posts").await, 0);
    assert_eq!(count(p, "SELECT count(*) FROM likes").await, 0);
    assert_eq!(count(p, "SELECT count(*) FROM follows").await, 1, "the follow of the member");
    assert_eq!(count(p, "SELECT count(*) FROM actors WHERE profile_record IS NOT NULL").await, 1, "an S DID keeps its profile");
    for t in ["posts", "likes", "follows", "member_post_refs", "actors", "record_tombstones"] {
        exec(p, &format!("DELETE FROM {t}")).await;
    }
    let w = writer(p, sets(PublicMode::Off, true, true, true));
    write_history(&w).await;
    assert_eq!(count(p, "SELECT count(*) FROM posts").await, 2);
    let h = Harness::new(p, sets(PublicMode::Off, false, true, true), vec![tier1()]).await;
    ending(&h).await;
    assert!(h.refract.purge(SUBJECT).await.expect("purge"));
    assert_eq!(count(p, "SELECT count(*) FROM posts").await, 0);
    assert_eq!(count(p, "SELECT count(*) FROM member_post_refs").await, 2, "its references stay");
    assert_eq!(count(p, "SELECT count(*) FROM likes").await, 2, "a member's likes stay");
    db.drop_db().await;
}

/// T-10, T-10b: the on→off sweep's predicates spare R's rows (and their
/// `post_counts`), and take them once the DID leaves R.
#[tokio::test]
async fn t10_the_sweep_spares_r() {
    let Some(db) = fresh_db("refract_t10_sweep").await else {
        return;
    };
    let p = &db.pool;
    member_row(p).await;
    let w = writer(p, sets(PublicMode::On, true, false, false));
    write_history(&w).await;
    exec(p, &format!("INSERT INTO post_counts (post_uri) VALUES ('at://{SUBJECT}/app.bsky.feed.post/3kpost') ON CONFLICT DO NOTHING")).await;
    exec(p, "INSERT INTO refract_dids (did, community, state, source) VALUES ('did:plc:subjectaaaaaaaaaaaaaaaaa', 'sky', 'restored', 'fill')").await;
    let deletable = |state: &'static str| async move {
        exec(p, &format!("UPDATE refract_dids SET state = '{state}'")).await;
        let mut n = 0;
        for t in crate::indexing::public_sweep::TABLES.iter().filter(|t| ["posts", "likes", "reposts", "follows", "post_counts"].contains(&t.name)) {
            let mine = if t.name == "post_counts" { format!("split_part(t.post_uri, '/', 3) = '{SUBJECT}'") } else { format!("t.author_did = '{SUBJECT}'") };
            n += count(p, &format!("SELECT count(*) FROM {} t WHERE {mine} AND {}", t.name, t.delete_when)).await;
        }
        n
    };
    assert_eq!(deletable("restored").await, 0, "every row of R is spared");
    assert_eq!(deletable("grace").await, 0);
    // Out of R: the post, the reply, both likes, the follow of a
    // non-member and the post's counts (the follow of the member is the
    // off rule's to keep).
    assert_eq!(deletable("ending").await, 6);
    db.drop_db().await;
}

/// T-6, §4.3: removing a member spares a restored DID's like and follow of
/// it; an `AccountDeleted` purge of the restored DID deletes every Refract
/// row of it and drops it from memory.
#[tokio::test]
async fn t6_member_removal_spares_r_and_account_deleted_forgets() {
    let Some(db) = fresh_db("refract_t6_member_removed").await else {
        return;
    };
    let p = &db.pool;
    member_row(p).await;
    exec(p, "INSERT INTO refract_dids (did, community, state, source) VALUES ('did:plc:subjectaaaaaaaaaaaaaaaaa', 'sky', 'restored', 'fill')").await;
    exec(p, "INSERT INTO refract_labels (labeler_did, subject, val, did, gone, verified) VALUES ('did:plc:l', 'did:plc:subjectaaaaaaaaaaaaaaaaa', 'x', 'did:plc:subjectaaaaaaaaaaaaaaaaa', false, true)").await;
    let s = sets(PublicMode::Off, true, false, false);
    let scope = ScopeState::new(s, ScopePolicy::MembersAndOneHop, 10_000);
    let w = Writer::new(p.clone(), scope.clone(), Arc::new(DisabledPrivateTier));
    write_history(&w).await;
    let purger = crate::aggregations::Purger::new(p.clone(), scope.clone(), Arc::new(DisabledPrivateTier));
    purger.purge(MEMBER, crate::aggregations::PurgeKind::MemberRemoved).await.expect("member removed");
    assert_eq!(count(p, &format!("SELECT count(*) FROM likes WHERE author_did = '{SUBJECT}'")).await, 2, "its likes stay");
    assert_eq!(count(p, &format!("SELECT count(*) FROM follows WHERE author_did = '{SUBJECT}'")).await, 2, "its follows stay");
    assert_eq!(count(p, &format!("SELECT count(*) FROM posts WHERE author_did = '{SUBJECT}'")).await, 2, "its reply to the member stays");
    purger.purge(SUBJECT, crate::aggregations::PurgeKind::AccountDeleted).await.expect("account deleted");
    assert_eq!(count(p, "SELECT count(*) FROM refract_dids").await, 0);
    assert_eq!(count(p, "SELECT count(*) FROM refract_labels").await, 0);
    assert!(!scope.snapshot().await.restored.contains_key(SUBJECT));
    db.drop_db().await;
}
