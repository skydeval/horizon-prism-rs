//! The thread outline through the consumer (thread outline design §3.1,
//! §7): T-O1, T-O3's refused entries, T-O4's full channel, T-O5's gap
//! values and T-O14's inactive writer and disabled outline, against a real
//! Postgres and Redis.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use serde_json::json;
use tokio::task::JoinHandle;

use super::super::*;
use super::live::{Fixture, Options, OUTSIDER, connect, drive};
use crate::config::ThreadOutlineConfig;
use crate::firehose::classifier::NSID_POST;
use crate::firehose::writer::{Action, RawOp};
use crate::indexing::PublicMode;
use crate::test_support::hooks;
use crate::thread_outline::id::StreamId;
use crate::thread_outline::ops::tid_value;
use crate::thread_outline::tests::{A, B, holes, post_commit, row, set_progress, tid};
use crate::thread_outline::{Freshness, Outline, stored_progress};

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

fn t(n: u64) -> i64 {
    tid_value(&tid(n)).expect("tid")
}

/// A consumer that hands its commits' outline ops to a running outline
/// writer with a queue of `queue` messages.
async fn outline_consumer(f: &Fixture, queue: u32) -> (Consumer, Outline, JoinHandle<()>) {
    let (mut c, _) = f.consumer().await;
    c.writer = c.writer.clone().with_outline(true);
    let outline = Outline::new(ThreadOutlineConfig { enabled: true, queue_messages: queue, flush_ms: 20, ..ThreadOutlineConfig::default() });
    let (sender, rx) = outline.channel();
    let watermarks = Watermarks::new(f.cfg.relay_labels.clone(), Vec::new());
    let writer = crate::thread_outline::writer::Writer::new(outline.clone(), f.db.pool.clone(), connect(&f.url).await, f.cfg.stream_key.clone(), watermarks, rx)
        .tuned(Duration::from_millis(200), 250)
        .spawn();
    assert!(eventually!(Duration::from_secs(10), outline.active()), "the writer takes its lock");
    (c.with_outline(sender), outline, writer)
}

async fn present(pool: &sqlx::PgPool, did: &str, ns: &[u64]) -> Vec<bool> {
    let mut v = Vec::new();
    for n in ns {
        v.push(row(pool, did, t(*n)).await.is_some());
    }
    v
}

async fn table_count(pool: &sqlx::PgPool, table: &str) -> i64 {
    sqlx::query_scalar(&format!("SELECT count(*) FROM {table}")).fetch_one(pool).await.expect("count")
}

/// T-O1: with `public_scope = off`, a firehose post by a DID outside every
/// scope reaches the outline and not `posts`.
#[tokio::test]
async fn t_o1_firehose_posts_of_any_did_reach_the_outline_not_posts() {
    let Some(mut f) = Fixture::new("to1_any_did", Options { mode: PublicMode::Off, ..Options::default() }).await else {
        return;
    };
    let (mut c, _outline, writer) = outline_consumer(&f, 100).await;
    let root = tid(1);
    let reply = tid(2);
    f.add(&post_commit(OUTSIDER, &[(&root, None)]), None).await;
    f.add(&post_commit(OUTSIDER, &[(&reply, Some((OUTSIDER, &root)))]), None).await;
    c.startup().await.expect("startup");
    assert!(c.run_until(|c| c.throughput.processed >= 2, Duration::from_secs(10)).await);
    assert!(eventually!(Duration::from_secs(10), row(&f.db.pool, OUTSIDER, t(2)).await.is_some()));
    assert_eq!(row(&f.db.pool, OUTSIDER, t(2)).await.expect("row").parent, Some((OUTSIDER.to_string(), t(1))));
    assert_eq!(table_count(&f.db.pool, "posts").await, 0, "content is not indexed");
    writer.abort();
    f.cleanup().await;
}

/// T-O1: other write sources collect nothing, nor a writer with the
/// outline off.
#[tokio::test]
async fn t_o1_other_write_sources_reach_nothing() {
    let Some(f) = Fixture::new("to1_other_sources", Options::default()).await else {
        return;
    };
    let op = RawOp {
        action: Action::Create,
        collection: NSID_POST.to_string(),
        rkey: tid(1),
        cid: Some("bafyop".into()),
        record: Some(json!({"text": "", "createdAt": chrono::Utc::now().to_rfc3339()})),
    };
    let options = |source| ApplyOptions { advance_applied_rev: false, dead_letter_readiness: false, attempts: 1, live: false, subscription_rules: false, source };
    let on = f.writer.clone().with_outline(true);
    for source in [WriteSource::IndexRequest, WriteSource::Backfill, WriteSource::CatchUp] {
        let report = on.apply(OUTSIDER, "3kaaaaaaaaa22", std::slice::from_ref(&op), options(source)).await.expect("apply");
        assert!(report.outline.is_empty(), "{source:?}");
    }
    let off = f.writer.clone().apply(OUTSIDER, "3kaaaaaaaaa22", std::slice::from_ref(&op), options(WriteSource::Firehose)).await.expect("apply");
    assert!(off.outline.is_empty());
    let fire = on.apply(OUTSIDER, "3kaaaaaaaaa22", std::slice::from_ref(&op), options(WriteSource::Firehose)).await.expect("apply");
    assert_eq!(fire.outline.len(), 1);
    f.cleanup().await;
}

/// T-O3: a bad-rev commit's reply and a reply in a commit dead-lettered
/// for another malformed op each record a hole at their parent; an
/// `#account` takedown records the relay's status.
#[tokio::test]
async fn t_o3_refused_entries_and_account_status() {
    let Some(mut f) = Fixture::new("to3_refused_entries", Options::default()).await else {
        return;
    };
    let (mut c, _outline, writer) = outline_consumer(&f, 100).await;
    let relay = "wss://bsky.network/xrpc/com.atproto.sync.subscribeRepos";
    let reply = |parent: u64| {
        json!({"action": "create", "path": format!("{NSID_POST}/{}", tid(50 + parent)), "cid": "bafyop",
            "record": {"text": "", "reply": {"parent": {"uri": format!("at://{B}/{NSID_POST}/{}", tid(parent)), "cid": "c"},
                                             "root": {"uri": format!("at://{B}/{NSID_POST}/{}", tid(parent)), "cid": "c"}}}})
    };
    let entry = |data: serde_json::Value| vec![("type", "commit".to_string()), ("data", data.to_string()), ("seq", "1".to_string())];
    // A first entry for the writer's first `Resume`.
    f.add(&post_commit(OUTSIDER, &[(&tid(1), None)]), None).await;
    f.add(&entry(json!({"repo": A, "rev": "nope", "ops": [reply(9)], "_relay": relay})), None).await;
    f.add(&entry(json!({"repo": A, "rev": "3kaaaaaaaaa22", "ops": [reply(8), {"action": "move", "path": "a/b"}], "_relay": relay})), None).await;
    let account = json!({"did": A, "active": false, "status": "takendown", "_relay": relay});
    f.add(&[("type", "account".to_string()), ("data", account.to_string()), ("seq", "5".to_string())], None).await;
    c.startup().await.expect("startup");
    assert!(c.run_until(|c| c.throughput.processed >= 4, Duration::from_secs(10)).await);
    assert!(eventually!(Duration::from_secs(10), holes(&f.db.pool).await.len() == 2));
    assert_eq!(holes(&f.db.pool).await, [(B.to_string(), t(8)), (B.to_string(), t(9))]);
    assert!(eventually!(Duration::from_secs(5), crate::thread_outline::gone_dids(&f.db.pool, &[A.to_string()]).await.expect("gone").contains(A)));
    let relays: Vec<(String, bool, i64)> = sqlx::query_as("SELECT relay, active, seq FROM outline_gone").fetch_all(&f.db.pool).await.expect("gone");
    assert_eq!(relays, [("bsky".to_string(), false, 5)]);
    writer.abort();
    f.cleanup().await;
}

/// T-O4: with the writer held and its queue full, the consumer goes on
/// (ingest never blocked); each commit's ops are sent whole or not at all;
/// the next `Resume` replays exactly the skipped entries, with none lost and
/// no hole.
#[tokio::test]
async fn t_o4_a_full_channel_then_resume_replays_exactly_the_skipped_entries() {
    let Some(mut f) = Fixture::new("to4_full_channel", Options::default()).await else {
        return;
    };
    let e0 = f.add_n(1, None).await.remove(0);
    set_progress(&f.db.pool, StreamId::parse(&e0), Some(1000)).await;
    // Room for a `Resume`, a commit and a `Through`, and no more.
    let (mut c, outline, writer) = outline_consumer(&f, 3).await;
    let held = hooks::arm(hooks::OUTLINE_WRITER_PAUSE, f.key());
    for n in 1..=10u64 {
        f.add(&post_commit(A, &[(&tid(2 * n), None), (&tid(2 * n + 1), None)]), None).await;
    }
    c.startup().await.expect("startup");
    assert!(c.run_until(|c| c.throughput.processed >= 11, Duration::from_secs(10)).await, "the consumer never waits on the outline");
    assert!(hooks::holding(hooks::OUTLINE_WRITER_PAUSE, f.key()), "while the writer was held");
    assert_eq!(outline.freshness(), Freshness::Resuming);
    held.release();
    tokio::time::sleep(Duration::from_millis(300)).await;
    for n in 1..=10u64 {
        let p = present(&f.db.pool, A, &[2 * n, 2 * n + 1]).await;
        assert!(p[0] == p[1], "commit {n}: all of its ops or none: {p:?}");
    }
    f.add(&post_commit(A, &[(&tid(22), None), (&tid(23), None)]), None).await;
    assert!(c.run_until(|c| c.throughput.processed >= 12, Duration::from_secs(10)).await);
    let all: Vec<u64> = (2..=23).collect();
    let replayed = eventually!(Duration::from_secs(10), present(&f.db.pool, A, &all).await.iter().all(|p| *p));
    assert!(
        replayed,
        "every skipped entry replayed: {:?} {:?} {:?} want {} resumed {}",
        present(&f.db.pool, A, &all).await,
        stored_progress(&f.db.pool).await.expect("progress"),
        outline.freshness(),
        outline.shared().want_epoch(),
        outline.shared().resumed_epoch()
    );
    assert_eq!(holes(&f.db.pool).await, []);
    assert_eq!(stored_progress(&f.db.pool).await.expect("progress").1, Some(1000), "nothing lost: coverage holds");
    drop(held);
    writer.abort();
    f.cleanup().await;
}

/// T-O5: a trim gap writes the ms of its last generated id into
/// `complete_since`; an interrupted claim writes Redis's clock.
#[tokio::test]
async fn t_o5_gaps_move_complete_since() {
    let Some(mut f) = Fixture::new("to5_gaps", Options::default()).await else {
        return;
    };
    gap::set_claim_in_progress(&f.db.pool, true).await.expect("claim");
    f.add_n(3, Some(3)).await;
    let (mut c, outline, writer) = outline_consumer(&f, 100).await;
    let before = crate::thread_outline::redis_time_ms(&mut f.redis).await.expect("time");
    c.startup().await.expect("startup");
    let after = crate::thread_outline::redis_time_ms(&mut f.redis).await.expect("time");
    let since = stored_progress(&f.db.pool).await.expect("progress").1.expect("written by the claim's gap");
    assert!((before..=after).contains(&since), "{before} <= {since} <= {after}");
    assert!(c.run_until(|c| c.throughput.processed >= 3, Duration::from_secs(10)).await);
    let ids = f.add_n(5, Some(3)).await;
    let last = StreamId::parse(ids.last().expect("id")).expect("id");
    assert!(c.run_until(|c| c.test.record.lock().expect("record").gaps.iter().any(|g| g.1 == GapCause::Trim), Duration::from_secs(10)).await);
    assert_eq!(stored_progress(&f.db.pool).await.expect("progress").1, Some(last.ms_i64()));
    assert!(outline.complete_since_ms() >= Some(last.ms_i64()));
    writer.abort();
    f.cleanup().await;
}

/// T-O14: the writer goes inactive and back while the consumer waits on an
/// idle stream. The next `Resume` of the new epoch replays what it
/// dropped; with no other writer, coverage holds.
#[tokio::test]
async fn t_o14_the_writer_goes_inactive_and_back_while_the_consumer_waits() {
    let Some(mut f) = Fixture::new("to14_inactive_and_back", Options::default()).await else {
        return;
    };
    let e0 = f.add_n(1, None).await.remove(0);
    set_progress(&f.db.pool, StreamId::parse(&e0), Some(1000)).await;
    let (c, outline, writer) = outline_consumer(&f, 100).await;
    let stop = Arc::new(AtomicBool::new(false));
    let driven = drive(c, Arc::clone(&stop), Duration::from_secs(60));
    let e1 = f.add(&post_commit(A, &[(&tid(1), None)]), None).await;
    let e1 = StreamId::parse(&e1).expect("id");
    assert!(eventually!(Duration::from_secs(10), stored_progress(&f.db.pool).await.expect("progress").0 == Some(e1)));
    assert!(row(&f.db.pool, A, t(1)).await.is_some());
    let pid: i32 = sqlx::query_scalar(
        "SELECT pid FROM pg_locks WHERE locktype = 'advisory' AND granted \
         AND database = (SELECT oid FROM pg_database WHERE datname = current_database())",
    )
    .fetch_one(&f.db.pool)
    .await
    .expect("lock holder");
    let _: bool = sqlx::query_scalar("SELECT pg_terminate_backend($1)").bind(pid).fetch_one(&f.db.pool).await.expect("terminate");
    f.add(&post_commit(A, &[(&tid(2), None)]), None).await;
    assert!(eventually!(Duration::from_secs(10), !outline.active()), "its next write finds the lock gone");
    assert!(eventually!(Duration::from_secs(10), outline.active()), "and retakes it");
    assert_eq!(outline.freshness(), Freshness::Resuming, "the consumer, idle, has sent no Resume yet");
    assert_eq!(row(&f.db.pool, A, t(2)).await, None, "dropped while inactive");
    f.add(&post_commit(A, &[(&tid(3), None)]), None).await;
    assert!(eventually!(Duration::from_secs(10), present(&f.db.pool, A, &[1, 2, 3]).await == [true, true, true]), "nothing is lost silently");
    assert_eq!(stored_progress(&f.db.pool).await.expect("progress").1, Some(1000));
    stop.store(true, Ordering::Relaxed);
    let _ = driven.await;
    writer.abort();
    f.cleanup().await;
}

/// T-O14: disabled, the consumer writes no outline row and a gap no
/// coverage; switching off deletes `through` and `complete_since`.
#[tokio::test]
async fn t_o14_disabled_writes_nothing() {
    let Some(mut f) = Fixture::new("to14_disabled", Options::default()).await else {
        return;
    };
    let (mut c, _) = f.consumer().await;
    f.add(&post_commit(A, &[(&tid(1), None)]), None).await;
    f.add_n(2, Some(3)).await;
    c.startup().await.expect("startup");
    assert!(c.run_until(|c| c.throughput.processed >= 3, Duration::from_secs(10)).await);
    f.add_n(5, Some(3)).await;
    assert!(c.run_until(|c| !c.test.record.lock().expect("record").gaps.is_empty(), Duration::from_secs(10)).await);
    assert_eq!(table_count(&f.db.pool, "outline_posts").await, 0);
    assert_eq!(table_count(&f.db.pool, "outline_actors").await, 0);
    assert_eq!(stored_progress(&f.db.pool).await.expect("progress"), (None, None), "a gap writes no coverage");
    set_progress(&f.db.pool, Some(StreamId::new(5, 0)), Some(1000)).await;
    assert_eq!(crate::thread_outline::switch_off(&f.db.pool).await.expect("switch off"), 2);
    assert_eq!(stored_progress(&f.db.pool).await.expect("progress"), (None, None));
    f.cleanup().await;
}
