//! Member-blocks-only design (§6, §9): private blocks are stored only with
//! a member side. Written through the real writer and private tier.

use serde_json::json;

use super::*;
use crate::firehose::classifier::NSID_PRIVATE_BLOCK;

/// A non-member, in S: its private blocks are kept only when they name a
/// member.
const STRANGER: &str = "did:plc:ssssssssssssssssssssssss";
const OTHER: &str = "did:plc:oooooooooooooooooooooooo";

fn block_record(subject: &str) -> Value {
    json!({"$type": NSID_PRIVATE_BLOCK, "subject": subject, "createdAt": "2026-09-26T00:00:00Z"})
}

/// The writer with `ALICE` a member and `STRANGER` in S, not a member.
fn writer(pool: &PgPool) -> Writer {
    let mut sets = ScopeSets::default();
    sets.members.insert(ALICE.into());
    sets.scope.insert(ALICE.into());
    sets.scope.insert(STRANGER.into());
    let scope = ScopeState::new(sets, ScopePolicy::MembersAndOneHop, 10_000);
    Writer::new(pool.clone(), scope, Arc::new(KryphocronPrivateTier))
}

async fn apply(w: &Writer, repo: &str, action: Action, rkey: &str, rev: &str, subject: &str) -> OpOutcome {
    let op = RawOp {
        action,
        collection: NSID_PRIVATE_BLOCK.into(),
        rkey: rkey.into(),
        cid: Some(format!("bafy{rkey}{rev}")),
        record: Some(block_record(subject)),
    };
    let options = ApplyOptions {
        advance_applied_rev: false,
        dead_letter_readiness: false,
        attempts: 1,
        live: false,
        subscription_rules: false,
        source: WriteSource::Firehose,
    };
    let report = w.apply(repo, rev, &[op], options).await.expect("apply");
    report.outcomes.into_iter().next().expect("one outcome")
}

async fn private_blocks(pool: &PgPool) -> Vec<(String, String)> {
    sqlx::query_as("SELECT author_did, subject_did FROM private_blocks ORDER BY author_did, subject_did")
        .fetch_all(pool)
        .await
        .expect("private blocks")
}

/// T-MB1, T-MB4b (member-blocks-only design §6, §9): a scoped non-member's
/// private block of a non-member is not stored, one of a member is; updated
/// to name a non-member, the stored row goes and a private tombstone at the
/// update's rev refuses the older create.
#[tokio::test]
async fn private_blocks_need_a_member_side() {
    let Some(db) = fresh_db("mb_private_blocks").await else {
        return;
    };
    let p = &db.pool;
    let w = writer(p);
    assert_eq!(apply(&w, STRANGER, Action::Create, "3kpb1", "3kaaaaaaaaaa1", OTHER).await, OpOutcome::Rejected("no_member_side"));
    assert_eq!(apply(&w, ALICE, Action::Create, "3kpb2", "3kaaaaaaaaaa1", OTHER).await, OpOutcome::Written);
    assert_eq!(apply(&w, STRANGER, Action::Create, "3kpb3", "3kaaaaaaaaaa1", ALICE).await, OpOutcome::Written);
    assert_eq!(private_blocks(p).await, [(ALICE.to_string(), OTHER.to_string()), (STRANGER.to_string(), ALICE.to_string())]);

    // The stranger's block of Alice, edited to name a non-member.
    assert_eq!(apply(&w, STRANGER, Action::Update, "3kpb3", "3kaaaaaaaaaa2", OTHER).await, OpOutcome::Rejected("no_member_side"));
    assert_eq!(private_blocks(p).await, [(ALICE.to_string(), OTHER.to_string())]);
    let uri = format!("at://{STRANGER}/{NSID_PRIVATE_BLOCK}/3kpb3");
    let tomb: String = sqlx::query_scalar("SELECT rev FROM private_tombstones WHERE uri = $1").bind(&uri).fetch_one(p).await.expect("tombstone");
    assert_eq!(tomb, "3kaaaaaaaaaa2");
    // The older create, replayed, stores nothing.
    assert_eq!(apply(&w, STRANGER, Action::Create, "3kpb3", "3kaaaaaaaaaa1", ALICE).await, OpOutcome::Unchanged);
    assert_eq!(private_blocks(p).await, [(ALICE.to_string(), OTHER.to_string())]);
    db.drop_db().await;
}

async fn insert_block(pool: &PgPool, author: &str, rkey: &str, subject: &str) {
    exec(pool, &format!(
        "INSERT INTO private_blocks (uri, author_did, subject_did, rev, created_at) \
         VALUES ('at://{author}/{NSID_PRIVATE_BLOCK}/{rkey}', '{author}', '{subject}', '3k', now())"
    ))
    .await;
}

/// T-MB4 (member-blocks-only design §8): private retention deletes exactly
/// the private blocks with no member side, whatever the author's scope, and
/// an owner that is not a member (a `member_removed` purge's window) loses
/// its blocks of non-members.
#[tokio::test]
async fn retention_keeps_private_blocks_with_a_member_side() {
    use crate::db::retention::{RetentionSettings, RetentionTask};

    let Some(db) = fresh_db("mb_private_retention").await else {
        return;
    };
    let p = &db.pool;
    const OWNER: &str = "did:plc:wwwwwwwwwwwwwwwwwwwwwwww";
    const OUTSIDE: &str = "did:plc:uuuuuuuuuuuuuuuuuuuuuuuu";
    exec(p, &format!("INSERT INTO members (did, source) VALUES ('{ALICE}', 'admin')")).await;
    exec(p, &format!("INSERT INTO scope_dids (did, reasons) VALUES ('{ALICE}', 1), ('{STRANGER}', 4), ('{OWNER}', 1)")).await;
    insert_block(p, STRANGER, "sn", OTHER).await; // in S, no member side: goes
    insert_block(p, STRANGER, "sm", ALICE).await; // names a member: stays
    insert_block(p, ALICE, "mn", OTHER).await; // a member's: stays
    insert_block(p, OUTSIDE, "on", OTHER).await; // out of S, no member side: goes
    insert_block(p, OWNER, "wn", OTHER).await; // an owner, not a member: goes
    let mut sets = ScopeSets::default();
    sets.members.insert(ALICE.into());
    for d in [ALICE, STRANGER, OWNER] {
        sets.scope.insert(d.into());
    }
    sets.private_owners.insert(OWNER.into());
    let scope = ScopeState::new(sets, ScopePolicy::MembersAndOneHop, 10_000);
    let settings = RetentionSettings { tombstone_days: 7, notification_days: 90, max_age_days: 0, trend_aggregate_hours: 72 };
    let report = crate::private_tier::retention::PrivateRetention::new(scope.clone()).run(p, &scope, &settings).await.expect("retention");
    assert!(report.deleted.contains(&("private_blocks", 3)), "{report:?}");
    assert_eq!(private_blocks(p).await, [(ALICE.to_string(), OTHER.to_string()), (STRANGER.to_string(), ALICE.to_string())]);
    db.drop_db().await;
}

/// T-MB5 (member-blocks-only design §7): a `member_removed` purge deletes
/// private blocks of the removed member by non-members and its own private
/// blocks of non-members; one between it and another member stays.
#[tokio::test]
async fn member_removed_keeps_private_blocks_with_a_member_side() {
    use crate::aggregations::{PurgeKind, Purger};

    let Some(db) = fresh_db("mb_private_member_removed").await else {
        return;
    };
    let p = &db.pool;
    for d in [ALICE, BOB] {
        exec(p, &format!("INSERT INTO members (did, source) VALUES ('{d}', 'admin')")).await;
        exec(p, &format!("INSERT INTO scope_dids (did, reasons) VALUES ('{d}', 1)")).await;
    }
    exec(p, &format!("INSERT INTO scope_dids (did, reasons) VALUES ('{STRANGER}', 4)")).await;
    insert_block(p, ALICE, "ab", BOB).await; // member to member: stays
    insert_block(p, ALICE, "ao", OTHER).await; // its own of a non-member: goes
    insert_block(p, STRANGER, "sa", ALICE).await; // a non-member's of it: goes
    insert_block(p, BOB, "ba", ALICE).await; // another member's of it: stays
    let mut sets = ScopeSets::default();
    for d in [ALICE, BOB] {
        sets.members.insert(d.into());
        sets.scope.insert(d.into());
    }
    sets.scope.insert(STRANGER.into());
    let scope = ScopeState::new(sets, ScopePolicy::MembersAndOneHop, 10_000);
    let purger = Purger::new(p.clone(), scope, Arc::new(KryphocronPrivateTier));
    purger.purge(ALICE, PurgeKind::MemberRemoved).await.expect("purge");
    assert_eq!(private_blocks(p).await, [(ALICE.to_string(), BOB.to_string()), (BOB.to_string(), ALICE.to_string())]);
    db.drop_db().await;
}
