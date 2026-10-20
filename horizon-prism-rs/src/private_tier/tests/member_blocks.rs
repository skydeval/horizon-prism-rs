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
