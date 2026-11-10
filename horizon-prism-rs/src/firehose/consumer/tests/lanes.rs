//! Writer lanes and dedupe through the consumer's own loop, against a real
//! Postgres and Redis: a repo's entries apply in order in one lane while
//! the other lanes go on; entries are acked, booked and counted in the
//! low-water entry only once they committed; a gap waits for every lane; a
//! conflicting transaction is retried in its lane; a duplicate copy is
//! dropped once its original committed, and a lane's failure loses nothing.

use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::time::{Duration, Instant};

use serde_json::json;

use super::super::*;
use super::live::{Fixture, Options, drive, recorder};
use super::rendered;
use crate::config::Dedupe;
use crate::test_support::{hooks, posts};

const BSKY: &str = "wss://bsky.network/xrpc/com.atproto.sync.subscribeRepos";
const MICROCOSM: &str = "wss://relay.fire.hose.cam/xrpc/com.atproto.sync.subscribeRepos";

/// Member DIDs, each in a different one of `lanes` lanes.
fn repos_in_distinct_lanes(lanes: usize, n: usize) -> Vec<String> {
    let mut out: Vec<String> = Vec::new();
    for c in 'a'..='z' {
        let did = format!("did:plc:lane{c}aaaaaaaaaaaaaaaaaaa");
        if out.iter().all(|d| lane_of(d, lanes) != lane_of(&did, lanes)) {
            out.push(did);
        }
        if out.len() == n {
            return out;
        }
    }
    panic!("no {n} DIDs in distinct lanes");
}

fn post_op(rkey: &str) -> serde_json::Value {
    json!({"action": "create", "path": format!("app.bsky.feed.post/{rkey}"), "cid": "bafyop",
           "record": {"$type": "app.bsky.feed.post", "text": "lanes", "createdAt": chrono::Utc::now().to_rfc3339()}})
}

/// A post commit of `repo` at `rev`, from `relay`.
fn post_commit(repo: &str, rev: &str, rkey: &str, relay: &str) -> Vec<(&'static str, String)> {
    let data = json!({"repo": repo, "rev": rev, "ops": [post_op(rkey)], "time": chrono::Utc::now().to_rfc3339(), "_relay": relay});
    vec![("type", "commit".to_string()), ("data", data.to_string()), ("seq", "1".to_string())]
}

fn post_uri(repo: &str, rkey: &str) -> String {
    format!("at://{repo}/app.bsky.feed.post/{rkey}")
}

async fn pending(f: &mut Fixture) -> u64 {
    let reply: redis::Value = redis::cmd("XPENDING").arg(&f.cfg.stream_key).arg(&f.cfg.group).query_async(&mut f.redis).await.expect("xpending");
    match reply {
        redis::Value::Array(parts) => parts.first().and_then(as_u64).unwrap_or(0),
        _ => 0,
    }
}

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
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }};
}

/// One repo's lane held on its first entry: its second entry waits, another
/// lane's repo is written meanwhile, nothing is acked or booked and the
/// low-water entry stays on the held entry; once released, both apply in
/// order and everything is acked, with the watermark and low-water entry
/// at the last entry.
#[tokio::test]
async fn a_held_lane_holds_its_repo_and_the_low_water_entry_while_the_others_apply() {
    let repos = repos_in_distinct_lanes(4, 2);
    let (a, b) = (repos[0].clone(), repos[1].clone());
    let opts = Options { members: repos.clone(), ..Options::default() };
    let Some(mut f) = Fixture::new("fl_held_lane", opts).await else {
        return;
    };
    f.cfg.writer_lanes = 4;
    let first = f.add(&post_commit(&a, "3kaaaaaaaaab2", "3kaaaaaaaaab2", BSKY), None).await;
    f.add(&post_commit(&b, "3kaaaaaaaaab2", "3kaaaaaaaaab2", BSKY), None).await;
    f.add(&post_commit(&a, "3kaaaaaaaaab3", "3kaaaaaaaaab3", BSKY), None).await;
    let last = f.add(&post_commit(&b, "3kaaaaaaaaab3", "3kaaaaaaaaab3", BSKY), None).await;
    let held = hooks::arm(hooks::LANE_BEFORE_ENTRY, &format!("{}/{a}", f.key()));
    let (c, _) = f.consumer().await;
    let low_water = c.low_water();
    let watermarks = c.watermarks.clone();
    let stop = Arc::new(AtomicBool::new(false));
    let task = drive(c, Arc::clone(&stop), Duration::from_secs(30));
    held.reached().await;
    let pool = f.db.pool.clone();
    assert!(
        eventually!(Duration::from_secs(10), posts::stored(&pool, &post_uri(&b, "3kaaaaaaaaab3")).await),
        "another lane applies while one is held"
    );
    assert!(!posts::stored(&pool, &post_uri(&a, "3kaaaaaaaaab2")).await);
    assert!(!posts::stored(&pool, &post_uri(&a, "3kaaaaaaaaab3")).await, "the held repo's later entry waits for its first");
    assert_eq!(pending(&mut f).await, 4, "nothing is acked before the entries before it committed");
    assert_eq!(low_water.get().as_deref(), Some(first.as_str()), "the low-water entry is the held one");
    assert_eq!(watermarks.get("bsky"), None, "no watermark passes an entry that did not commit");
    held.release();
    assert!(eventually!(Duration::from_secs(10), pending(&mut f).await == 0));
    assert!(posts::stored(&pool, &post_uri(&a, "3kaaaaaaaaab2")).await && posts::stored(&pool, &post_uri(&a, "3kaaaaaaaaab3")).await);
    assert_eq!(low_water.get().as_deref(), Some(last.as_str()), "nothing in flight: the last entry read");
    assert!(watermarks.get("bsky").is_some());
    stop.store(true, std::sync::atomic::Ordering::Relaxed);
    let c = task.await.expect("consumer");
    assert_eq!(c.throughput.processed, 4);
    assert_eq!(c.throughput.by_path, [0, 4, 0, 0]);
    drop(held);
    f.cleanup().await;
}

/// A trim gap found while a lane is held runs its transaction only once
/// every lane has finished its entries.
#[tokio::test]
async fn a_gap_waits_for_every_lane() {
    let repos = repos_in_distinct_lanes(4, 2);
    let a = repos[0].clone();
    let opts = Options { members: repos.clone(), batch_size: 4, ..Options::default() };
    let Some(mut f) = Fixture::new("fl_gap_drains", opts).await else {
        return;
    };
    f.cfg.writer_lanes = 4;
    f.add(&post_commit(&a, "3kaaaaaaaaab2", "3kaaaaaaaaab2", BSKY), None).await;
    f.add_n(3, None).await;
    let held = hooks::arm(hooks::LANE_BEFORE_ENTRY, &format!("{}/{a}", f.key()));
    let (c, _) = f.consumer().await;
    let log = Arc::clone(&c.test.record);
    let stop = Arc::new(AtomicBool::new(false));
    let task = drive(c, Arc::clone(&stop), Duration::from_secs(30));
    held.reached().await;
    // Trim everything the consumer has not read yet while the lane is held.
    f.add_n(50, Some(5)).await;
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(log.lock().expect("record").gaps.is_empty(), "no gap transaction while a lane is in flight");
    let released = Instant::now();
    held.release();
    assert!(eventually!(Duration::from_secs(10), !log.lock().expect("record").gaps.is_empty()));
    let rec = log.lock().expect("record").clone();
    assert!(rec.gaps[0].0 > released, "the gap ran after the held lane finished");
    let first_after = rec.processed.iter().position(|(t, _)| *t > rec.gaps[0].0);
    assert!(rec.processed.len() >= 4 && first_after.is_none_or(|i| i >= 4), "the round before the gap was booked first: {:?}", rec.processed);
    stop.store(true, std::sync::atomic::Ordering::Relaxed);
    task.await.expect("consumer");
    assert!(posts::stored(&f.db.pool, &post_uri(&a, "3kaaaaaaaaab2")).await);
    drop(held);
    f.cleanup().await;
}

/// A deadlock is retried in its lane at once: the entry is applied once,
/// with no backoff and no re-read of the pending list.
#[tokio::test]
async fn a_conflict_is_retried_in_its_lane() {
    let (rec, handle) = recorder();
    let _local = metrics::set_default_local_recorder(&rec);
    let repos = repos_in_distinct_lanes(4, 2);
    let a = repos[0].clone();
    let opts = Options { members: repos.clone(), ..Options::default() };
    let Some(mut f) = Fixture::new("fl_conflict_retried", opts).await else {
        return;
    };
    f.cfg.writer_lanes = 4;
    f.add(&post_commit(&a, "3kaaaaaaaaab2", "3kaaaaaaaaab2", BSKY), None).await;
    f.add(&post_commit(&repos[1], "3kaaaaaaaaab2", "3kaaaaaaaaab2", BSKY), None).await;
    let conflict = hooks::arm_times(hooks::WRITER_CONFLICT, &format!("{}/{a}", f.key()), 2);
    let (mut c, _) = f.consumer().await;
    c.startup().await.expect("startup");
    let started = Instant::now();
    assert!(c.run_until(|c| c.throughput.processed >= 2, Duration::from_secs(10)).await);
    assert!(started.elapsed() < BACKOFF_MIN * 20, "no transient backoff");
    assert_eq!(conflict.hits(), 2);
    assert_eq!(c.throughput.by_path, [0, 2, 0, 0], "each entry applied once");
    assert!(posts::stored(&f.db.pool, &post_uri(&a, "3kaaaaaaaaab2")).await);
    let lane = lane_of(&a, 4).to_string();
    let lane_label = format!("lane=\"{lane}\"");
    assert_eq!(rendered(&handle, METRIC_LANE_RETRIES, &[&lane_label, "code=\"40P01\""]), Some(2.0));
    assert_eq!(pending(&mut f).await, 0);
    drop(conflict);
    f.cleanup().await;
}

/// Dedupe on: a second relay's copy of an applied commit is dropped and
/// acked, in the same round (in its lane, after the original committed) or
/// a later one; another rev of the same repo is applied; a copy the window
/// forgot is applied again.
#[tokio::test]
async fn a_copy_of_an_applied_commit_is_dropped_once_the_original_committed() {
    let (rec, handle) = recorder();
    let _local = metrics::set_default_local_recorder(&rec);
    let repos = repos_in_distinct_lanes(4, 2);
    let a = repos[0].clone();
    let opts = Options { members: repos.clone(), ..Options::default() };
    let Some(mut f) = Fixture::new("fl_dedupe_drops", opts).await else {
        return;
    };
    f.cfg.writer_lanes = 4;
    f.cfg.dedupe = Dedupe::On;
    f.cfg.dedupe_window_secs = 2;
    // The original and its copy in one round; the copy waits in the same
    // lane while the original is held.
    f.add(&post_commit(&a, "3kaaaaaaaaab2", "3kaaaaaaaaab2", BSKY), None).await;
    f.add(&post_commit(&a, "3kaaaaaaaaab2", "3kaaaaaaaaab2", MICROCOSM), None).await;
    let held = hooks::arm(hooks::LANE_BEFORE_ENTRY, &format!("{}/{a}", f.key()));
    let (c, _) = f.consumer().await;
    let watermarks = c.watermarks.clone();
    let stop = Arc::new(AtomicBool::new(false));
    let task = drive(c, Arc::clone(&stop), Duration::from_secs(30));
    held.reached().await;
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert_eq!(pending(&mut f).await, 2, "the copy is not acked before its original committed");
    held.release();
    assert!(eventually!(Duration::from_secs(10), pending(&mut f).await == 0));
    assert!(watermarks.get("microcosm").is_some(), "a dropped copy's relay still moves its watermark");
    // A later round: the copy is dropped from the window, another rev applies.
    f.add(&post_commit(&a, "3kaaaaaaaaab2", "3kaaaaaaaaab2", MICROCOSM), None).await;
    f.add(&post_commit(&a, "3kaaaaaaaaab3", "3kaaaaaaaaab3", MICROCOSM), None).await;
    assert!(eventually!(Duration::from_secs(10), pending(&mut f).await == 0 && posts::stored(&f.db.pool, &post_uri(&a, "3kaaaaaaaaab3")).await));
    // Past the window the same copy is applied again.
    tokio::time::sleep(Duration::from_secs(3)).await;
    f.add(&post_commit(&a, "3kaaaaaaaaab2", "3kaaaaaaaaab2", BSKY), None).await;
    assert!(eventually!(Duration::from_secs(10), pending(&mut f).await == 0));
    stop.store(true, std::sync::atomic::Ordering::Relaxed);
    let c = task.await.expect("consumer");
    assert_eq!(c.throughput.processed, 5);
    assert_eq!(c.throughput.deduped, 2);
    assert_eq!(c.throughput.by_path, [2, 3, 0, 0], "two copies dropped with no transaction, three applied");
    assert_eq!(rendered(&handle, crate::firehose::dedupe::METRIC_DROPPED, &["relay=\"microcosm\""]), Some(2.0));
    drop(held);
    f.cleanup().await;
}

/// Dedupe off (the default outside full): every copy is applied.
#[tokio::test]
async fn without_dedupe_every_copy_is_applied() {
    let repos = repos_in_distinct_lanes(4, 1);
    let opts = Options { members: repos.clone(), ..Options::default() };
    let Some(mut f) = Fixture::new("fl_dedupe_off", opts).await else {
        return;
    };
    f.cfg.writer_lanes = 4;
    f.add(&post_commit(&repos[0], "3kaaaaaaaaab2", "3kaaaaaaaaab2", BSKY), None).await;
    f.add(&post_commit(&repos[0], "3kaaaaaaaaab2", "3kaaaaaaaaab2", MICROCOSM), None).await;
    let (mut c, _) = f.consumer().await;
    c.startup().await.expect("startup");
    assert!(c.run_until(|c| c.throughput.processed >= 2, Duration::from_secs(10)).await);
    assert_eq!((c.throughput.deduped, c.throughput.by_path), (0, [0, 2, 0, 0]));
    f.cleanup().await;
}

/// A lane that fails past its retries stops the round: what committed
/// before the failed entry in stream order is acked, everything from it on
/// is read again from the pending list and applied, its duplicate copy
/// included, and nothing is lost.
#[tokio::test]
async fn a_lane_failure_loses_nothing() {
    let repos = repos_in_distinct_lanes(4, 3);
    let (a, b, d) = (repos[0].clone(), repos[1].clone(), repos[2].clone());
    let opts = Options { members: repos.clone(), ..Options::default() };
    let Some(mut f) = Fixture::new("fl_lane_failure", opts).await else {
        return;
    };
    f.cfg.writer_lanes = 4;
    f.cfg.dedupe = Dedupe::On;
    f.add(&post_commit(&b, "3kaaaaaaaaab2", "3kaaaaaaaaab2", BSKY), None).await;
    f.add(&post_commit(&a, "3kaaaaaaaaab2", "3kaaaaaaaaab2", BSKY), None).await;
    f.add(&post_commit(&d, "3kaaaaaaaaab2", "3kaaaaaaaaab2", BSKY), None).await;
    f.add(&post_commit(&a, "3kaaaaaaaaab2", "3kaaaaaaaaab2", MICROCOSM), None).await;
    f.add(&post_commit(&a, "3kaaaaaaaaab3", "3kaaaaaaaaab3", BSKY), None).await;
    let failing = hooks::arm_times(hooks::WRITER_CONFLICT, &format!("{}/{a}", f.key()), (CONFLICT_RETRIES + 1) as usize);
    let (mut c, _) = f.consumer().await;
    c.startup().await.expect("startup");
    assert!(c.run_until(|c| c.throughput.processed >= 5 && failing.hits() > CONFLICT_RETRIES as usize, Duration::from_secs(20)).await);
    for (repo, rkey) in [(&b, "3kaaaaaaaaab2"), (&a, "3kaaaaaaaaab2"), (&d, "3kaaaaaaaaab2"), (&a, "3kaaaaaaaaab3")] {
        assert!(posts::stored(&f.db.pool, &post_uri(repo, rkey)).await, "{repo} {rkey}");
    }
    assert_eq!(pending(&mut f).await, 0);
    // b was acked in the failed round; a's entries and d's (after a's in
    // stream order) were read again; a's second copy was dropped then.
    assert_eq!(c.throughput.deduped, 1);
    assert!(c.throughput.processed >= 5);
    drop(failing);
    f.cleanup().await;
}

/// Three relays deliver the same commits: two copies of every three are
/// dropped, each relay's copies and the applied (first) copies are counted,
/// and status's share covers only the batches read with dedupe on.
#[tokio::test]
async fn three_relays_of_the_same_commits_drop_two_copies_of_three() {
    const THIRD: &str = "wss://relay.third.example/xrpc/com.atproto.sync.subscribeRepos";
    let (rec, handle) = recorder();
    let _local = metrics::set_default_local_recorder(&rec);
    let repos = repos_in_distinct_lanes(4, 3);
    let opts = Options { members: repos.clone(), ..Options::default() };
    let Some(mut f) = Fixture::new("fl_dedupe_three", opts).await else {
        return;
    };
    f.cfg.writer_lanes = 4;
    f.cfg.relay_labels.insert(THIRD.to_string(), "third".to_string());
    f.cfg.dedupe = Dedupe::Off;
    // Off: both copies of one commit apply, and status counts neither.
    f.add(&post_commit(&repos[0], "3kaaaaaaaaab2", "3kaaaaaaaaab2", BSKY), None).await;
    f.add(&post_commit(&repos[0], "3kaaaaaaaaab2", "3kaaaaaaaaab2", MICROCOSM), None).await;
    let (mut c, _) = f.consumer().await;
    c.startup().await.expect("startup");
    assert!(c.run_until(|c| c.throughput.processed >= 2, Duration::from_secs(10)).await);
    assert_eq!(c.throughput.deduped, 0);
    // On: three commits, three copies each.
    c.cfg.dedupe = Dedupe::On;
    for (i, repo) in repos.iter().enumerate() {
        let rev = format!("3kaaaaaaaaac{}", i + 2);
        for relay in [BSKY, MICROCOSM, THIRD] {
            f.add(&post_commit(repo, &rev, &rev, relay), None).await;
        }
    }
    assert!(c.run_until(|c| c.throughput.processed >= 11, Duration::from_secs(10)).await);
    assert_eq!(c.throughput.deduped, 6, "two of every three copies dropped");
    assert_eq!(c.throughput.by_path, [6, 5, 0, 0], "five applied: two before dedupe, three after");
    for (i, repo) in repos.iter().enumerate() {
        let rkey = format!("3kaaaaaaaaac{}", i + 2);
        assert!(posts::stored(&f.db.pool, &post_uri(repo, &rkey)).await);
    }
    let copies: f64 = ["bsky", "microcosm", "third"]
        .iter()
        .map(|r| rendered(&handle, crate::firehose::dedupe::METRIC_COPIES, &[&format!("relay=\"{r}\"")]).unwrap_or(0.0))
        .sum();
    assert_eq!(copies, 9.0, "every keyed copy is counted by its relay");
    let first: f64 = ["bsky", "microcosm", "third"]
        .iter()
        .map(|r| rendered(&handle, crate::firehose::dedupe::METRIC_FIRST, &[&format!("relay=\"{r}\"")]).unwrap_or(0.0))
        .sum();
    assert_eq!(first, 3.0, "one applied copy per commit");
    let w = c.tiers.board().writer();
    assert_eq!((w.window_entries, w.window_deduped), (9, 6), "the share covers only the batches read with dedupe on");
    assert_eq!((w.entries, w.deduped), (11, 6));
    assert_eq!(pending(&mut f).await, 0);
    f.cleanup().await;
}
