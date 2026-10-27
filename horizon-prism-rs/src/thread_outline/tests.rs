//! Thread outline tests (thread outline design §7): T-O2, T-O3's account
//! status and holes, T-O4's writer, T-O5's coverage and freshness, T-O6 and
//! T-O14's lock. The consumer's side is in `firehose/consumer/tests/outline.rs`.
//!
//! Postgres-backed tests run when `HPRS_TEST_DATABASE_URL` is set; the
//! writer's also need `HPRS_TEST_REDIS_URL`.

use std::collections::BTreeMap;
use std::time::{Duration, Instant};

use chrono::{DateTime, TimeDelta, Utc};
use redis::aio::ConnectionManager;
use serde_json::json;
use sqlx::{Connection, PgConnection, PgPool};
use tokio::task::JoinHandle;

use super::flush::{Progress, apply};
use super::id::StreamId;
use super::ops::{HoleReason, Key, Op, PostOp, tid_value};
use super::*;
use crate::config::ThreadOutlineConfig;
use crate::firehose::watermark::Watermarks;
use crate::test_support::{TestDb, fresh_db, hooks};

pub(crate) const A: &str = "did:plc:outlineaaaaaaaaaaaaaaaaa";
pub(crate) const B: &str = "did:plc:outlinebbbbbbbbbbbbbbbbb";
const C: &str = "did:plc:outlineccccccccccccccccc";

/// A valid TID, distinct per `n` and ordered by it.
pub(crate) fn tid(n: u64) -> String {
    const ALPHABET: &[u8] = b"234567abcdefghijklmnopqrstuvwxyz";
    let v: u64 = (1_760_000_000_000_000u64 << 10) + n;
    (0..13).map(|i| ALPHABET[usize::try_from((v >> (60 - 5 * i)) & 31).expect("digit")] as char).collect()
}

fn t(n: u64) -> i64 {
    tid_value(&tid(n)).expect("tid")
}

fn key(did: &str, n: u64) -> Key {
    Key { did: did.to_string(), rkey: t(n) }
}

fn post(author: &str, n: u64, parent: Option<Key>, rev: i64) -> PostOp {
    let root = parent.clone();
    PostOp { author: author.to_string(), rkey: t(n), parent, root, created_at: Utc::now(), rev }
}

fn id(n: u64) -> StreamId {
    StreamId::new(1_760_000_000_000 + n, 0)
}

/// One stored outline row, by DID and TID number.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Row {
    pub parent: Option<(String, i64)>,
    pub rev: i64,
    pub deleted: bool,
    pub xmin: String,
}

type RowTuple = (Option<String>, Option<i64>, i64, bool, String);

pub(crate) async fn row(pool: &PgPool, did: &str, rkey: i64) -> Option<Row> {
    let r: Option<RowTuple> = sqlx::query_as(
        "SELECT pa.did, o.parent_rkey, o.rev, o.deleted_at IS NOT NULL, o.xmin::text FROM outline_posts o \
         JOIN outline_actors a ON a.id = o.author LEFT JOIN outline_actors pa ON pa.id = o.parent_author \
         WHERE a.did = $1 AND o.rkey = $2",
    )
    .bind(did)
    .bind(rkey)
    .fetch_optional(pool)
    .await
    .expect("row");
    r.map(|(pd, pr, rev, deleted, xmin)| Row { parent: pd.zip(pr), rev, deleted, xmin })
}

pub(crate) async fn holes(pool: &PgPool) -> Vec<(String, i64)> {
    sqlx::query_as("SELECT a.did, h.rkey FROM outline_holes h JOIN outline_actors a ON a.id = h.author ORDER BY 1, 2")
        .fetch_all(pool)
        .await
        .expect("holes")
}

async fn count(pool: &PgPool, table: &str) -> i64 {
    sqlx::query_scalar(&format!("SELECT count(*) FROM {table}")).fetch_one(pool).await.expect("count")
}

async fn conn(db: &TestDb) -> PgConnection {
    PgConnection::connect_with(&db.pool.connect_options()).await.expect("connect")
}

async fn flush_ops(c: &mut PgConnection, ops: Vec<(StreamId, Op)>) {
    apply(c, &ops, &Progress::default()).await.expect("flush");
}

// T-O2: rev rules.

/// Three copies of a create give one row and one tuple, in one batch and
/// across batches.
#[tokio::test]
async fn t_o2_three_copies_give_one_row_and_one_tuple() {
    let Some(db) = fresh_db("to2_three_copies").await else {
        return;
    };
    let mut c = conn(&db).await;
    let p = post(A, 1, None, 10);
    flush_ops(&mut c, vec![(id(1), Op::Create(p.clone())), (id(2), Op::Create(p.clone())), (id(3), Op::Create(p.clone()))]).await;
    let first = row(&db.pool, A, t(1)).await.expect("row");
    flush_ops(&mut c, vec![(id(4), Op::Create(p.clone()))]).await;
    flush_ops(&mut c, vec![(id(5), Op::Create(p))]).await;
    let after = row(&db.pool, A, t(1)).await.expect("row");
    assert_eq!(after.xmin, first.xmin, "a duplicate copy writes no tuple");
    assert_eq!(count(&db.pool, "outline_posts").await, 1);
    db.drop_db().await;
}

/// Delete then a late copy of the create stays deleted, across batches and
/// in one batch; a higher-rev re-create revives.
#[tokio::test]
async fn t_o2_a_delete_beats_late_copies_and_a_newer_create_revives() {
    let Some(db) = fresh_db("to2_delete_late_copy").await else {
        return;
    };
    let mut c = conn(&db).await;
    let parent = key(B, 9);
    // Across batches.
    let p = post(A, 1, Some(parent.clone()), 10);
    flush_ops(&mut c, vec![(id(1), Op::Create(p.clone()))]).await;
    flush_ops(&mut c, vec![(id(2), Op::Delete { author: A.into(), rkey: t(1), rev: 20 })]).await;
    flush_ops(&mut c, vec![(id(3), Op::Create(p))]).await;
    let r = row(&db.pool, A, t(1)).await.expect("row");
    assert!(r.deleted);
    assert_eq!(r.rev, 20);
    assert_eq!(r.parent, Some((B.to_string(), parent.rkey)), "the row keeps its parent");
    // In one batch.
    let q = post(A, 2, Some(parent.clone()), 10);
    flush_ops(
        &mut c,
        vec![(id(4), Op::Create(q.clone())), (id(5), Op::Delete { author: A.into(), rkey: t(2), rev: 20 }), (id(6), Op::Create(q))],
    )
    .await;
    let r = row(&db.pool, A, t(2)).await.expect("a create folded away by a delete is still inserted");
    assert!(r.deleted);
    assert_eq!((r.rev, r.parent), (20, Some((B.to_string(), parent.rkey))));
    // A delete beats a create of the same rev across batches too, and a
    // copy of it writes no tuple.
    flush_ops(&mut c, vec![(id(8), Op::Create(post(A, 3, None, 10)))]).await;
    flush_ops(&mut c, vec![(id(9), Op::Delete { author: A.into(), rkey: t(3), rev: 10 })]).await;
    let r = row(&db.pool, A, t(3)).await.expect("row");
    assert!(r.deleted, "same rev: the delete wins");
    flush_ops(&mut c, vec![(id(10), Op::Delete { author: A.into(), rkey: t(3), rev: 10 })]).await;
    assert_eq!(row(&db.pool, A, t(3)).await.expect("row").xmin, r.xmin);
    // A higher-rev re-create revives.
    flush_ops(&mut c, vec![(id(7), Op::Create(post(A, 1, None, 30)))]).await;
    let r = row(&db.pool, A, t(1)).await.expect("row");
    assert!(!r.deleted);
    assert_eq!((r.rev, r.parent), (30, None));
    db.drop_db().await;
}

/// A stale update changes nothing; an update of an unknown key inserts
/// nothing; a create and an update in one batch give the update's fields.
#[tokio::test]
async fn t_o2_updates() {
    let Some(db) = fresh_db("to2_updates").await else {
        return;
    };
    let mut c = conn(&db).await;
    flush_ops(&mut c, vec![(id(1), Op::Create(post(A, 1, Some(key(B, 1)), 10)))]).await;
    let before = row(&db.pool, A, t(1)).await.expect("row");
    flush_ops(&mut c, vec![(id(2), Op::Update(post(A, 1, Some(key(C, 1)), 5)))]).await;
    let after = row(&db.pool, A, t(1)).await.expect("row");
    assert_eq!(after, before, "a stale update writes nothing, not even a tuple");

    flush_ops(&mut c, vec![(id(3), Op::Update(post(A, 2, Some(key(B, 1)), 50)))]).await;
    assert_eq!(row(&db.pool, A, t(2)).await, None, "an update never inserts");

    flush_ops(&mut c, vec![(id(4), Op::Create(post(A, 3, Some(key(B, 1)), 10))), (id(5), Op::Update(post(A, 3, Some(key(C, 2)), 11)))]).await;
    let r = row(&db.pool, A, t(3)).await.expect("row");
    assert_eq!((r.rev, r.parent), (11, Some((C.to_string(), t(2)))));
    assert_eq!(count(&db.pool, "outline_posts").await, 2);
    db.drop_db().await;
}

/// A delete of a key never seen, then its create, stays deleted, across
/// batches and in one batch, and keeps the create's parent.
#[tokio::test]
async fn t_o2_an_unseen_delete_refuses_its_create() {
    let Some(db) = fresh_db("to2_unseen_delete").await else {
        return;
    };
    let mut c = conn(&db).await;
    flush_ops(&mut c, vec![(id(1), Op::Delete { author: A.into(), rkey: t(1), rev: 20 })]).await;
    assert_eq!(count(&db.pool, "outline_deleted").await, 1);
    assert_eq!(row(&db.pool, A, t(1)).await, None);
    flush_ops(&mut c, vec![(id(2), Op::Create(post(A, 1, Some(key(B, 1)), 10)))]).await;
    let r = row(&db.pool, A, t(1)).await.expect("the refused create is inserted, deleted");
    assert!(r.deleted);
    assert_eq!((r.rev, r.parent.clone()), (20, Some((B.to_string(), t(1)))));
    assert_eq!(count(&db.pool, "outline_deleted").await, 0, "the row now carries the delete");
    // A later copy changes nothing.
    flush_ops(&mut c, vec![(id(3), Op::Create(post(A, 1, Some(key(B, 1)), 10)))]).await;
    assert_eq!(row(&db.pool, A, t(1)).await.expect("row"), r);
    // In one batch.
    flush_ops(&mut c, vec![(id(4), Op::Delete { author: A.into(), rkey: t(2), rev: 20 }), (id(5), Op::Create(post(A, 2, Some(key(B, 1)), 10)))]).await;
    let r = row(&db.pool, A, t(2)).await.expect("row");
    assert!(r.deleted);
    assert_eq!(r.parent, Some((B.to_string(), t(1))));
    db.drop_db().await;
}

// T-O3: holes and account status.

/// Holes are stored once; a bad-rev delete makes a hole at its stored row's
/// parent, and nothing without a row.
#[tokio::test]
async fn t_o3_holes_and_bad_rev_deletes() {
    let Some(db) = fresh_db("to3_holes").await else {
        return;
    };
    let mut c = conn(&db).await;
    flush_ops(&mut c, vec![(id(1), Op::Create(post(A, 1, Some(key(B, 7)), 10)))]).await;
    flush_ops(
        &mut c,
        vec![
            (id(2), Op::Hole { node: key(C, 3), reason: HoleReason::LeftOut }),
            (id(3), Op::Hole { node: key(C, 3), reason: HoleReason::BadRev }),
            (id(4), Op::HoleAtParentOf { node: key(A, 1) }),
            (id(5), Op::HoleAtParentOf { node: key(A, 99) }),
        ],
    )
    .await;
    assert_eq!(holes(&db.pool).await, [(B.to_string(), t(7)), (C.to_string(), t(3))]);
    assert!(!row(&db.pool, A, t(1)).await.expect("row").deleted, "a hole touches no post");
    db.drop_db().await;
}

fn status(did: &str, relay: &str, active: bool, seq: i64) -> Op {
    Op::Status { did: did.into(), relay: relay.into(), active, seq }
}

async fn gone(pool: &PgPool) -> Vec<String> {
    let mut v: Vec<String> = gone_dids(pool, &[A.to_string(), B.to_string(), C.to_string()]).await.expect("gone").into_iter().collect();
    v.sort();
    v
}

/// R4-2, R5-1: per relay, the highest seq wins; gone only when every
/// reporting relay says inactive; status never touches posts.
#[tokio::test]
async fn t_o3_account_status() {
    let Some(db) = fresh_db("to3_account_status").await else {
        return;
    };
    let mut c = conn(&db).await;
    flush_ops(&mut c, vec![(id(1), Op::Create(post(A, 1, None, 10))), (id(2), Op::Create(post(B, 1, None, 10)))]).await;
    // A: takedown then reactivation from one relay: not gone.
    flush_ops(&mut c, vec![(id(3), status(A, "bsky", false, 5))]).await;
    assert_eq!(gone(&db.pool).await, [A.to_string()]);
    flush_ops(&mut c, vec![(id(4), status(A, "bsky", true, 6))]).await;
    assert_eq!(gone(&db.pool).await, Vec::<String>::new());
    // B: the relay's late copy with a lower seq does not win.
    flush_ops(&mut c, vec![(id(5), status(B, "bsky", false, 10)), (id(6), status(B, "bsky", true, 9))]).await;
    flush_ops(&mut c, vec![(id(7), status(B, "bsky", true, 8))]).await;
    assert_eq!(gone(&db.pool).await, [B.to_string()]);
    // C: one relay's takedown with another reporting active: not gone.
    flush_ops(&mut c, vec![(id(8), status(C, "bsky", false, 3)), (id(9), status(C, "microcosm", true, 900))]).await;
    assert!(!gone(&db.pool).await.contains(&C.to_string()));
    // The gone author's posts stay live.
    let r = row(&db.pool, B, t(1)).await.expect("row");
    assert!(!r.deleted);
    db.drop_db().await;
}

// T-O4: the writer's progress and the stream.

async fn stored_through_text(pool: &PgPool) -> Option<String> {
    sqlx::query_scalar("SELECT value #>> '{}' FROM worker_state WHERE key = $1")
        .bind(KEY_THROUGH)
        .fetch_optional(pool)
        .await
        .expect("through")
}

/// `through` compares as `(ms, seq)` in SQL too, and a stale `Through`
/// never moves it back.
#[tokio::test]
async fn t_o4_through_is_monotonic_as_two_numbers() {
    let Some(db) = fresh_db("to4_through_monotonic").await else {
        return;
    };
    let mut c = conn(&db).await;
    let at = |through: StreamId| Progress { through: Some(through), ..Progress::default() };
    apply(&mut c, &[], &at(StreamId::new(5, 9))).await.expect("flush");
    apply(&mut c, &[], &at(StreamId::new(5, 10))).await.expect("flush");
    assert_eq!(stored_through_text(&db.pool).await.as_deref(), Some("5-10"));
    let back = apply(&mut c, &[], &at(StreamId::new(5, 9))).await.expect("flush");
    assert_eq!(back.through, Some(StreamId::new(5, 10)), "a stale Through never moves it back");
    apply(&mut c, &[], &at(StreamId::new(4, u64::MAX))).await.expect("flush");
    assert_eq!(stored_through_text(&db.pool).await.as_deref(), Some("5-10"));
    apply(&mut c, &[], &at(StreamId::new(5, u64::MAX))).await.expect("flush");
    assert_eq!(stored_through_text(&db.pool).await.as_deref(), Some("5-18446744073709551615"));
    // A stream reset rebases it.
    apply(&mut c, &[], &Progress { rebase: Some(StreamId::new(3, 0)), ..Progress::default() }).await.expect("flush");
    assert_eq!(stored_through_text(&db.pool).await.as_deref(), Some("3-0"));
    db.drop_db().await;
}

// T-O5: coverage.

/// Each gap cause's value (R3-4); an operator gap writes none.
#[test]
fn t_o5_gap_values_by_cause() {
    use crate::firehose::gap::GapCause;
    assert_eq!(gap_value(GapCause::Trim, Some("1760000000123-4"), Some(9)), Some(1_760_000_000_123));
    for cause in [GapCause::Pending, GapCause::InterruptedClaim, GapCause::GroupRecreated] {
        assert_eq!(gap_value(cause, Some("1-0"), Some(42)), Some(42), "{cause:?}");
    }
    assert_eq!(gap_value(GapCause::Operator, Some("1-0"), Some(42)), None);
}

/// The gap transaction writes the outline's value with GREATEST, writes
/// nothing without one, and `complete_since` never moves back, stored or in
/// memory.
#[tokio::test]
async fn t_o5_the_gap_transaction_moves_complete_since_forward_only() {
    use std::sync::Arc;

    use crate::aggregations::{ScopeSets, ScopeState};
    use crate::config::ScopePolicy;
    use crate::firehose::gap::{GapCause, run_gap_transaction, run_gap_transaction_clearing};
    use crate::private_tier::{DisabledPrivateTier, PrivateTier};
    let Some(db) = fresh_db("to5_gap_complete_since").await else {
        return;
    };
    let scope = ScopeState::new(ScopeSets::default(), ScopePolicy::MembersAndOneHop, 10);
    let private: Arc<dyn PrivateTier> = Arc::new(DisabledPrivateTier);
    run_gap_transaction(&db.pool, &scope, private.as_ref(), GapCause::Operator, false).await.expect("gap");
    assert_eq!(stored_progress(&db.pool).await.expect("progress").1, None, "no value, no write");
    run_gap_transaction_clearing(&db.pool, &scope, private.as_ref(), GapCause::Trim, false, false, Some(2000)).await.expect("gap");
    run_gap_transaction_clearing(&db.pool, &scope, private.as_ref(), GapCause::Pending, false, false, Some(1000)).await.expect("gap");
    assert_eq!(stored_progress(&db.pool).await.expect("progress").1, Some(2000));
    let outline = Outline::new(ThreadOutlineConfig::default());
    outline.raise_complete_since(2000);
    outline.raise_complete_since(1000);
    assert_eq!(outline.complete_since_ms(), Some(2000));
    db.drop_db().await;
}

/// Settle, the root's TID time and the window edge each make a thread
/// incomplete (§3.3).
#[test]
fn t_o5_thread_completeness() {
    let cfg = ThreadOutlineConfig { settle_secs: 900, window_days: 30, ..ThreadOutlineConfig::default() };
    let outline = Outline::new(cfg);
    let now = Utc::now();
    // TIDs whose time is `at`.
    let rkey_at = |at: DateTime<Utc>| at.timestamp_micros() << 10;
    let since = now - TimeDelta::days(10);
    let root = |seen: DateTime<Utc>, tid_at: DateTime<Utc>| RootFacts { seen_at: seen, rkey: rkey_at(tid_at) };
    let ok = root(since + TimeDelta::hours(1), since + TimeDelta::hours(1));
    assert!(!outline.thread_complete(ok, now), "no coverage yet");
    outline.raise_complete_since(since.timestamp_millis());
    assert!(outline.thread_complete(ok, now));
    // Seen within the settle.
    assert!(!outline.thread_complete(root(since + TimeDelta::seconds(899), since), now));
    assert!(outline.thread_complete(root(since + TimeDelta::seconds(901), since), now));
    // A TID time more than the settle before coverage began.
    assert!(!outline.thread_complete(root(since + TimeDelta::hours(1), since - TimeDelta::seconds(901)), now));
    assert!(outline.thread_complete(root(since + TimeDelta::hours(1), since - TimeDelta::seconds(899)), now));
    // Seen before the window's edge less a day.
    let old_since = now - TimeDelta::days(40);
    let o2 = Outline::new(ThreadOutlineConfig { settle_secs: 900, window_days: 30, ..ThreadOutlineConfig::default() });
    o2.raise_complete_since(old_since.timestamp_millis());
    assert!(!o2.thread_complete(root(now - TimeDelta::days(29) - TimeDelta::hours(1), now - TimeDelta::days(29) - TimeDelta::hours(1)), now));
    assert!(o2.thread_complete(root(now - TimeDelta::days(28), now - TimeDelta::days(28)), now));
}

/// A lagging, replaying or behind outline, or one with no live relay, is
/// not fresh; one fast-clock commit does not make a catching-up relay
/// live (R3-2, R4-5, R5-4).
#[test]
fn t_o5_freshness() {
    let outline = Outline::new(ThreadOutlineConfig { fresh_secs: 30, ..ThreadOutlineConfig::default() });
    let now: i64 = 1_760_000_000_000;
    let s = outline.shared();
    assert_eq!(outline.freshness_at(now), Freshness::Inactive);
    s.set_active(true);
    assert_eq!(outline.freshness_at(now), Freshness::Resuming, "a Resume is awaited from the start");
    s.set_resumed(s.want_epoch());
    s.set_replaying(true);
    assert_eq!(outline.freshness_at(now), Freshness::Replaying);
    s.set_replaying(false);
    assert_eq!(outline.freshness_at(now), Freshness::Behind, "nothing written through yet");
    s.set_through_ms(now - 31_000);
    assert_eq!(outline.freshness_at(now), Freshness::Behind);
    s.set_through_ms(now - 1000);
    assert_eq!(outline.freshness_at(now), Freshness::NoLiveRelay);
    // A sidecar catching up: 100 commits ten minutes old, then one from a
    // PDS whose clock runs ahead.
    for _ in 0..LIVENESS_RING {
        outline.observe_relay("bsky", now - 600_000);
    }
    outline.observe_relay("bsky", now);
    assert_eq!(outline.freshness_at(now), Freshness::NoLiveRelay);
    for _ in 0..(LIVENESS_RING / 2 + 1) {
        outline.observe_relay("bsky", now - 2000);
    }
    assert_eq!(outline.freshness_at(now), Freshness::Fresh);
    // A bump makes it stale again until the next Resume.
    s.bump_for(Why::Lag);
    assert_eq!(outline.freshness_at(now), Freshness::Resuming);
    s.set_active(false);
    assert_eq!(outline.freshness_at(now), Freshness::Inactive);
    // The state is a metric: one-hot `outline_freshness{state}`.
    let recorder = metrics_exporter_prometheus::PrometheusBuilder::new().build_recorder();
    let handle = recorder.handle();
    let _local = metrics::set_default_local_recorder(&recorder);
    Freshness::Replaying.record();
    let text = handle.render();
    assert_eq!(crate::test_support::series(&text, METRIC_FRESHNESS, &["state=\"replaying\""]), Some(1.0));
    assert_eq!(crate::test_support::series(&text, METRIC_FRESHNESS, &["state=\"fresh\""]), Some(0.0));
}

// T-O6: retention.

/// Rows past the window go in composite-key batches of 10,000; newer rows
/// stay.
#[tokio::test]
async fn t_o6_retention_in_composite_key_batches() {
    use crate::db::retention::{RetentionSettings, RetentionTask, ScopeGuard};
    let Some(db) = fresh_db("to6_retention").await else {
        return;
    };
    let task = retention::OutlineRetention::new(30);
    for step in task.steps() {
        assert!(step.sql.contains("WHERE (author, rkey) IN (SELECT"), "{}", step.sql);
        assert!(step.sql.contains("LIMIT 10000"), "{}", step.sql);
    }
    sqlx::query("INSERT INTO outline_actors (did) VALUES ($1)").bind(A).execute(&db.pool).await.expect("actor");
    // 10,050 old posts (two batches) and 5 new ones.
    sqlx::query(
        "INSERT INTO outline_posts (author, rkey, created_at, seen_at, rev) \
         SELECT 1, g, now(), now() - CASE WHEN g <= 10050 THEN interval '31 days' ELSE interval '29 days' END, 1 \
         FROM generate_series(1, 10055) g",
    )
    .execute(&db.pool)
    .await
    .expect("posts");
    for (table, sql) in [
        ("outline_deleted", "INSERT INTO outline_deleted (author, rkey, rev, seen_at) VALUES (1, 1, 1, now() - interval '31 days'), (1, 2, 1, now())"),
        ("outline_holes", "INSERT INTO outline_holes (author, rkey, seen_at) VALUES (1, 1, now() - interval '31 days'), (1, 2, now())"),
    ] {
        sqlx::query(sql).execute(&db.pool).await.expect(table);
    }
    let scope = crate::aggregations::ScopeState::new(Default::default(), crate::config::ScopePolicy::MembersAndOneHop, 10);
    let guard: &dyn ScopeGuard = &scope;
    let settings = RetentionSettings { tombstone_days: 7, notification_days: 90, max_age_days: 0, trend_aggregate_hours: 72 };
    let report = task.run(&db.pool, guard, &settings).await.expect("retention");
    assert_eq!(task.name(), "thread_outline");
    assert_eq!(report.deleted, [("outline_posts", 10_050), ("outline_deleted", 1), ("outline_holes", 1)]);
    assert_eq!(count(&db.pool, "outline_posts").await, 5);
    let kept: Vec<i64> = sqlx::query_scalar("SELECT rkey FROM outline_holes UNION ALL SELECT rkey FROM outline_deleted").fetch_all(&db.pool).await.expect("kept");
    assert_eq!(kept, [2, 2]);
    db.drop_db().await;
}

// The writer, against a real Redis stream.

pub(crate) const REV: &str = "3kaaaaaaaaa22";

/// A commit entry's fields: one post per `(rkey, parent)`.
pub(crate) fn post_commit(repo: &str, posts: &[(&str, Option<(&str, &str)>)]) -> Vec<(&'static str, String)> {
    let ops: Vec<serde_json::Value> = posts
        .iter()
        .map(|(rkey, parent)| {
            let mut record = json!({"$type": "app.bsky.feed.post", "text": "", "createdAt": Utc::now().to_rfc3339()});
            if let Some((did, prkey)) = parent {
                let uri = format!("at://{did}/app.bsky.feed.post/{prkey}");
                record["reply"] = json!({"parent": {"uri": uri, "cid": "bafyp"}, "root": {"uri": uri, "cid": "bafyp"}});
            }
            json!({"action": "create", "path": format!("app.bsky.feed.post/{rkey}"), "cid": "bafyop", "record": record})
        })
        .collect();
    let data = json!({"repo": repo, "rev": REV, "ops": ops, "time": Utc::now().to_rfc3339(),
        "_relay": "wss://bsky.network/xrpc/com.atproto.sync.subscribeRepos"});
    vec![("type", "commit".to_string()), ("data", data.to_string()), ("seq", "1".to_string())]
}

pub(crate) async fn xadd(redis: &mut ConnectionManager, stream: &str, fields: &[(&str, String)]) -> StreamId {
    let mut cmd = redis::cmd("XADD");
    cmd.arg(stream).arg("*");
    for (k, v) in fields {
        cmd.arg(*k).arg(v);
    }
    let id: String = cmd.query_async(redis).await.expect("xadd");
    StreamId::parse(&id).expect("id")
}

pub(crate) async fn set_progress(pool: &PgPool, through: Option<StreamId>, complete_since: Option<i64>) {
    if let Some(t) = through {
        sqlx::query("INSERT INTO worker_state (key, value, updated_at) VALUES ($1, to_jsonb($2::text), now())")
            .bind(KEY_THROUGH)
            .bind(t.to_string())
            .execute(pool)
            .await
            .expect("through");
    }
    if let Some(ms) = complete_since {
        sqlx::query("INSERT INTO worker_state (key, value, updated_at) VALUES ($1, to_jsonb($2::bigint), now())")
            .bind(KEY_COMPLETE_SINCE)
            .bind(ms)
            .execute(pool)
            .await
            .expect("complete_since");
    }
}

/// Poll `cond`, which may `.await`, every 20 ms until it holds or `within`
/// has passed; whether it held.
macro_rules! eventually {
    ($within:expr, $cond:expr) => {{
        let deadline = Instant::now() + $within;
        loop {
            if $cond {
                break true;
            }
            if Instant::now() >= deadline {
                break false;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }};
}

struct Rig {
    db: TestDb,
    redis: ConnectionManager,
    stream: String,
    outline: Outline,
    tx: tokio::sync::mpsc::Sender<Msg>,
    task: JoinHandle<()>,
}

impl Rig {
    async fn new(test: &str) -> Option<Self> {
        let Ok(url) = std::env::var("HPRS_TEST_REDIS_URL") else {
            eprintln!("HPRS_TEST_REDIS_URL unset; skipping outline writer test {test}");
            return None;
        };
        let db = fresh_db(test).await?;
        let client = redis::Client::open(url).expect("redis url");
        let redis = ConnectionManager::new(client).await.expect("connect redis");
        let stream = format!("test:outline:{}", rand::random::<u32>());
        let outline = Outline::new(ThreadOutlineConfig { enabled: true, flush_ms: 20, ..ThreadOutlineConfig::default() });
        let (sender, rx) = outline.channel();
        let tx = sender.tx.clone();
        let task = spawn_writer(&outline, &db.pool, &redis, &stream, rx);
        Some(Self { db, redis, stream, outline, tx, task })
    }

    async fn add(&mut self, repo: &str, posts: &[(&str, Option<(&str, &str)>)]) -> StreamId {
        xadd(&mut self.redis, &self.stream, &post_commit(repo, posts)).await
    }

    async fn send(&self, msg: Msg) {
        self.tx.send(msg).await.expect("send");
    }

    async fn resume(&self, first_id: StreamId) {
        self.send(Msg::Resume { first_id, epoch: self.outline.shared().want_epoch(), reset: false }).await;
    }

    async fn active(&self) {
        assert!(eventually!(Duration::from_secs(10), self.outline.active()), "the writer takes its lock");
    }

    async fn through(&self) -> Option<StreamId> {
        stored_progress(&self.db.pool).await.expect("progress").0
    }

    async fn complete_since(&self) -> Option<i64> {
        stored_progress(&self.db.pool).await.expect("progress").1
    }

    /// Stop the writer, and wait until its lock is released.
    async fn stop(&mut self) {
        self.task.abort();
        let pool = self.db.pool.clone();
        assert!(eventually!(Duration::from_secs(10), lock_holders(&pool).await == 0), "the lock is released");
    }

    /// A new writer on the same database and stream, as after a restart.
    fn restart(&mut self) {
        self.outline = Outline::new(self.outline.config().clone());
        let (sender, rx) = self.outline.channel();
        self.tx = sender.tx.clone();
        self.task = spawn_writer(&self.outline, &self.db.pool, &self.redis, &self.stream, rx);
    }

    async fn cleanup(mut self) {
        self.task.abort();
        let _: Result<(), redis::RedisError> = redis::cmd("DEL").arg(&self.stream).query_async(&mut self.redis).await;
        self.db.drop_db().await;
    }
}

async fn lock_holders(pool: &PgPool) -> i64 {
    sqlx::query_scalar(
        "SELECT count(*) FROM pg_locks WHERE locktype = 'advisory' AND granted \
         AND database = (SELECT oid FROM pg_database WHERE datname = current_database())",
    )
    .fetch_one(pool)
    .await
    .expect("locks")
}

fn spawn_writer(outline: &Outline, pool: &PgPool, redis: &ConnectionManager, stream: &str, rx: tokio::sync::mpsc::Receiver<Msg>) -> JoinHandle<()> {
    let watermarks = Watermarks::new(BTreeMap::new(), Vec::new());
    writer::Writer::new(outline.clone(), pool.clone(), redis.clone(), stream.to_string(), watermarks, rx)
        .tuned(Duration::from_millis(200), 2)
        .spawn()
}

/// Startup replays `(through, first_id)` with no hole; a restart whose
/// `first_id ≤ through + 1` replays nothing and leaves `complete_since`
/// alone.
#[tokio::test]
async fn t_o4_startup_replays_the_range_and_a_restart_nothing() {
    let Some(mut rig) = Rig::new("to4_startup_replay").await else {
        return;
    };
    let mut ids = Vec::new();
    for n in 1..=5 {
        ids.push(rig.add(A, &[(&tid(n), None)]).await);
    }
    set_progress(&rig.db.pool, Some(ids[0]), Some(1000)).await;
    rig.active().await;
    rig.resume(ids[4]).await;
    assert!(eventually!(Duration::from_secs(10), rig.through().await == Some(ids[4].prev())));
    let present: Vec<bool> = {
        let mut v = Vec::new();
        for n in 1..=5 {
            v.push(row(&rig.db.pool, A, t(n)).await.is_some());
        }
        v
    };
    assert_eq!(present, [false, true, true, true, false], "exactly (through, first_id)");
    assert_eq!(rig.complete_since().await, Some(1000));
    assert_eq!(holes(&rig.db.pool).await, []);

    // A restart: the pending entries come back at or below `through + 1`.
    rig.stop().await;
    rig.restart();
    rig.active().await;
    let through = rig.through().await.expect("through");
    rig.resume(through.next()).await;
    rig.send(Msg::Commit { id: ids[2], ops: vec![Op::Create(post(A, 3, None, 99))] }).await;
    rig.send(Msg::Through(ids[2])).await;
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(rig.through().await, Some(through));
    assert_eq!(rig.complete_since().await, Some(1000));
    assert_eq!(row(&rig.db.pool, A, t(3)).await.expect("row").rev, tid_value(REV).expect("rev"), "a repeat is skipped");
    assert_eq!(rig.outline.shared().resumed_epoch(), rig.outline.shared().want_epoch());
    rig.cleanup().await;
}

/// A range trimmed from the start moves `complete_since` to `first_id`'s
/// ms; one trimmed partway through a replay stops it there. Neither
/// writes an `outline_holes` row.
#[tokio::test]
async fn t_o4_trimmed_ranges_move_complete_since() {
    let Some(mut rig) = Rig::new("to4_trimmed").await else {
        return;
    };
    let mut ids = Vec::new();
    for n in 1..=7 {
        ids.push(rig.add(A, &[(&tid(n), None)]).await);
    }
    set_progress(&rig.db.pool, Some(ids[0]), Some(1000)).await;
    rig.active().await;
    // Partway: the first chunk (2 entries) is written, then the head moves
    // past the rest.
    let between = hooks::arm(hooks::OUTLINE_REPLAY_BETWEEN_CHUNKS, rig.db.name());
    rig.resume(ids[6]).await;
    between.reached().await;
    let _: i64 = redis::cmd("XTRIM").arg(&rig.stream).arg("MINID").arg(ids[5].to_string()).query_async(&mut rig.redis).await.expect("xtrim");
    between.release();
    assert!(eventually!(Duration::from_secs(10), rig.through().await == Some(ids[6].prev())));
    assert_eq!(rig.complete_since().await, Some(ids[6].ms_i64()));
    let mut present = Vec::new();
    for n in 1..=7 {
        present.push(row(&rig.db.pool, A, t(n)).await.is_some());
    }
    assert_eq!(present, [false, true, true, false, false, false, false]);
    assert_eq!(count(&rig.db.pool, "outline_holes").await, 0);
    drop(between);

    // From the start: the range's first entries are gone already.
    let more: Vec<StreamId> = {
        let mut v = Vec::new();
        for n in 8..=10 {
            v.push(rig.add(A, &[(&tid(n), None)]).await);
        }
        v
    };
    let _: i64 = redis::cmd("XTRIM").arg(&rig.stream).arg("MINID").arg(more[1].to_string()).query_async(&mut rig.redis).await.expect("xtrim");
    // `through` is ids[6] − 1, now before the stream's head.
    rig.resume(more[2]).await;
    assert!(eventually!(Duration::from_secs(10), rig.complete_since().await == Some(more[2].ms_i64())));
    assert_eq!(rig.through().await, Some(more[2].prev()));
    assert_eq!(row(&rig.db.pool, A, t(9)).await, None, "nothing after a trimmed head is replayed");
    assert_eq!(count(&rig.db.pool, "outline_holes").await, 0);

    // A stream reset (§3.3 step 4): a new stream may begin below
    // `through`, which is rebased to `first_id − 1`. The reset rides on a
    // `Resume` the writer discards as stale; the next one still rebases.
    let since = rig.complete_since().await;
    let want = rig.outline.shared().want_epoch();
    rig.send(Msg::Resume { first_id: StreamId::new(5, 0), epoch: want + 7, reset: true }).await;
    rig.send(Msg::Resume { first_id: StreamId::new(5, 0), epoch: want, reset: false }).await;
    assert!(eventually!(Duration::from_secs(10), rig.through().await == Some(StreamId::new(4, u64::MAX))));
    assert_eq!(rig.complete_since().await, since, "complete_since never moves back");
    rig.cleanup().await;
}

/// First switch-on: no `through` sets it to `first_id − 1` and
/// `complete_since` to `first_id`'s ms, replaying nothing (R3-5). A
/// transient flush failure retries with no hole; a deterministic one
/// re-flushes op by op and records a hole for the op that fails.
#[tokio::test]
async fn t_o4_first_switch_on_and_flush_errors() {
    let Some(mut rig) = Rig::new("to4_flush_errors").await else {
        return;
    };
    let first = rig.add(A, &[(&tid(1), None)]).await;
    rig.active().await;
    rig.resume(first).await;
    assert!(eventually!(Duration::from_secs(10), rig.through().await == Some(first.prev())));
    assert_eq!(rig.complete_since().await, Some(first.ms_i64()));
    assert_eq!(row(&rig.db.pool, A, t(1)).await, None, "nothing is replayed");

    let transient = hooks::arm_times(hooks::OUTLINE_FLUSH_TRANSIENT, rig.db.name(), 2);
    rig.send(Msg::Commit { id: first, ops: vec![Op::Create(post(A, 1, None, 10))] }).await;
    rig.send(Msg::Through(first)).await;
    assert!(eventually!(Duration::from_secs(10), rig.through().await == Some(first)));
    assert_eq!(transient.hits(), 2, "it failed twice before it wrote");
    assert!(row(&rig.db.pool, A, t(1)).await.is_some());
    assert_eq!(holes(&rig.db.pool).await, []);
    drop(transient);

    let bad = hooks::arm(hooks::OUTLINE_FLUSH_DETERMINISTIC, &format!("{}/{B}", rig.db.name()));
    let next = StreamId::new(first.ms + 1, 0);
    rig.send(Msg::Commit { id: next, ops: vec![Op::Create(post(A, 2, None, 10)), Op::Create(post(B, 1, Some(key(A, 2)), 10))] }).await;
    rig.send(Msg::Through(next)).await;
    assert!(eventually!(Duration::from_secs(10), rig.through().await == Some(next)));
    assert!(row(&rig.db.pool, A, t(2)).await.is_some(), "the op that could be written was");
    assert_eq!(row(&rig.db.pool, B, t(1)).await, None);
    assert_eq!(holes(&rig.db.pool).await, [(A.to_string(), t(2))], "a hole at the failed op's parent");
    drop(bad);
    rig.cleanup().await;
}

/// T-O14: a lock held elsewhere keeps the writer inactive, dropping its
/// queue; once it is taken, the next `Resume` acts as trimmed. A lost lock
/// connection with no other holder is retaken within the retry, and keeps
/// `complete_since`: the next `Resume` replays what was dropped.
#[tokio::test]
async fn t_o14_the_lock() {
    let Some(mut rig) = Rig::new("to14_lock").await else {
        return;
    };
    rig.stop().await;
    assert_eq!(writer::LOCK_RETRY, Duration::from_secs(60), "retaken within a minute");
    let e1 = rig.add(A, &[(&tid(1), None)]).await;
    set_progress(&rig.db.pool, Some(e1), Some(1000)).await;
    // Held elsewhere.
    let mut other = conn(&rig.db).await;
    assert!(sqlx::query_scalar::<_, bool>(writer::LOCK_SQL).fetch_one(&mut other).await.expect("lock"));
    rig.restart();
    tokio::time::sleep(Duration::from_millis(400)).await;
    assert!(!rig.outline.active());
    assert_eq!(rig.outline.freshness(), Freshness::Inactive);
    rig.send(Msg::Commit { id: e1.next(), ops: vec![Op::Create(post(A, 50, None, 10))] }).await;
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(rig.tx.capacity(), usize::try_from(rig.outline.config().queue_messages).expect("cap"), "the inactive writer drops its queue");
    other.close().await.expect("close");
    rig.active().await;
    let e2 = rig.add(A, &[(&tid(2), None)]).await;
    let e3 = rig.add(A, &[(&tid(3), None)]).await;
    rig.resume(e3).await;
    assert!(eventually!(Duration::from_secs(10), rig.complete_since().await == Some(e3.ms_i64())), "a contended lock is a coverage break");
    assert_eq!(rig.through().await, Some(e3.prev()));
    assert_eq!(row(&rig.db.pool, A, t(2)).await, None, "nothing replayed: as trimmed");
    assert_eq!(row(&rig.db.pool, A, t(50)).await, None);
    let _ = e2;

    // The lock connection drops, with no other holder.
    let since = rig.complete_since().await;
    assert_eq!(lock_holders(&rig.db.pool).await, 1);
    let pid: i32 = sqlx::query_scalar(
        "SELECT pid FROM pg_locks WHERE locktype = 'advisory' AND granted \
         AND database = (SELECT oid FROM pg_database WHERE datname = current_database())",
    )
    .fetch_one(&rig.db.pool)
    .await
    .expect("lock holder");
    let _: bool = sqlx::query_scalar("SELECT pg_terminate_backend($1)").bind(pid).fetch_one(&rig.db.pool).await.expect("terminate");
    let e4 = rig.add(A, &[(&tid(4), None)]).await;
    // The writer learns of it at its next write.
    rig.send(Msg::Commit { id: e4, ops: vec![Op::Create(post(A, 4, None, 10))] }).await;
    rig.send(Msg::Through(e4)).await;
    assert!(eventually!(Duration::from_secs(5), !rig.outline.active()), "a failed write on a dead session is a lost lock");
    let lost = Instant::now();
    assert!(eventually!(Duration::from_secs(5), rig.outline.active()));
    assert!(lost.elapsed() < Duration::from_secs(2), "retaken at the next retry");
    assert_eq!(rig.outline.freshness(), Freshness::Resuming, "a Resume of the new epoch is awaited");
    let e5 = rig.add(A, &[(&tid(5), None)]).await;
    rig.resume(e5).await;
    assert!(eventually!(Duration::from_secs(10), rig.through().await == Some(e5.prev())));
    assert!(row(&rig.db.pool, A, t(4)).await.is_some(), "the dropped entry is replayed");
    assert_eq!(rig.complete_since().await, since, "no other writer: coverage holds");
    rig.cleanup().await;
}

/// T-O14: two processes on one database. The second writer finds the lock
/// held; once the first stops, it takes it, and its first `Resume` moves
/// `complete_since`.
#[tokio::test]
async fn t_o14_an_overlap_moves_complete_since() {
    let Some(mut rig) = Rig::new("to14_overlap").await else {
        return;
    };
    let e1 = rig.add(A, &[(&tid(1), None)]).await;
    set_progress(&rig.db.pool, Some(e1), Some(1000)).await;
    rig.active().await;
    let second = Outline::new(rig.outline.config().clone());
    let (s2, rx2) = second.channel();
    let task2 = spawn_writer(&second, &rig.db.pool, &rig.redis, &rig.stream, rx2);
    tokio::time::sleep(Duration::from_millis(400)).await;
    assert!(!second.active(), "the lock is held by the first");
    // The first writer moves `through`, then stops.
    let e2 = rig.add(A, &[(&tid(2), None)]).await;
    rig.resume(e2).await;
    assert!(eventually!(Duration::from_secs(10), rig.through().await == Some(e2.prev())));
    // Its lock goes with its connection; the second may take it at once.
    rig.task.abort();
    assert!(eventually!(Duration::from_secs(5), second.active()));
    let e3 = rig.add(A, &[(&tid(3), None)]).await;
    s2.tx.send(Msg::Resume { first_id: e3, epoch: second.shared().want_epoch(), reset: false }).await.expect("send");
    assert!(eventually!(Duration::from_secs(10), rig.complete_since().await == Some(e3.ms_i64())));
    task2.abort();
    rig.cleanup().await;
}
