//! Private blocks are not indexed (skydeval, 2026-10-24): a public block
//! already keeps the two accounts apart, so `tools.kryphocron.graph.block`
//! is skipped like any unproduced private NSID. Only `app.bsky.graph.block`
//! and members' list blocks count. Written through the real writer and
//! private tier.

use serde_json::json;

use super::*;

const PRIVATE_BLOCK: &str = "tools.kryphocron.graph.block";

/// The writer with `ALICE` and `BOB` members.
fn writer(pool: &PgPool) -> Writer {
    let mut sets = ScopeSets::default();
    for d in [ALICE, BOB] {
        sets.members.insert(d.into());
        sets.scope.insert(d.into());
    }
    let scope = ScopeState::new(sets, ScopePolicy::MembersAndOneHop, 10_000);
    Writer::new(pool.clone(), scope, Arc::new(KryphocronPrivateTier))
}

async fn apply(w: &Writer, action: Action, rev: &str) -> OpOutcome {
    let record = (action != Action::Delete).then(|| json!({"$type": PRIVATE_BLOCK, "subject": BOB, "createdAt": "2026-09-26T00:00:00Z"}));
    let op = RawOp {
        action,
        collection: PRIVATE_BLOCK.into(),
        rkey: "3kpb1".into(),
        cid: record.as_ref().map(|_| format!("bafy3kpb1{rev}")),
        record,
    };
    let options = ApplyOptions {
        advance_applied_rev: false,
        dead_letter_readiness: false,
        attempts: 1,
        live: false,
        subscription_rules: false,
        source: WriteSource::Firehose,
    };
    let report = w.apply(ALICE, rev, &[op], options).await.expect("apply");
    assert!(report.failed.is_empty(), "{:?}", report.failed);
    report.outcomes.into_iter().next().expect("one outcome")
}

/// A member's private block of another member, created, updated and
/// deleted, is skipped each time: nothing is stored, public or private, and
/// no private tombstone is written.
#[tokio::test]
async fn private_blocks_are_skipped() {
    let Some(db) = fresh_db("no_private_blocks").await else {
        return;
    };
    let p = &db.pool;
    let w = writer(p);
    for (action, rev) in [(Action::Create, "3kaaaaaaaaaa1"), (Action::Update, "3kaaaaaaaaaa2"), (Action::Delete, "3kaaaaaaaaaa3")] {
        assert_eq!(apply(&w, action, rev).await, OpOutcome::Skipped("kryphocron_unproduced"), "{action:?}");
    }
    let blocks: i64 = sqlx::query_scalar("SELECT count(*) FROM blocks").fetch_one(p).await.expect("blocks");
    let tombstones: i64 = sqlx::query_scalar("SELECT count(*) FROM private_tombstones").fetch_one(p).await.expect("tombstones");
    assert_eq!((blocks, tombstones), (0, 0));
    let table: Option<String> = sqlx::query_scalar("SELECT to_regclass('private_blocks')::text").fetch_one(p).await.expect("regclass");
    assert_eq!(table, None, "migration 0045 drops the table");
    db.drop_db().await;
}
