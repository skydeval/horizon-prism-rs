//! The consumer against a real Postgres and Redis (firehose throughput design
//! §5.2): a fixture that builds a `Consumer` on a fresh database and a random
//! stream key, a producer, and the F1, F2, F4 and F6 tests that drive the
//! consumer's own loop. They run when `HPRS_TEST_DATABASE_URL` and
//! `HPRS_TEST_REDIS_URL` are set, on the current-thread runtime: the local
//! metrics recorder is thread-local.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use metrics_exporter_prometheus::{PrometheusBuilder, PrometheusHandle, PrometheusRecorder};
use redis::Value;
use redis::aio::ConnectionManager;
use serde_json::json;
use tokio::task::JoinHandle;

use super::super::*;
use super::rendered;
use crate::aggregations::{ScopeSets, ScopeState};
use crate::config::ScopePolicy;
use crate::firehose::coverage::CoverageTracker;
use crate::indexing::{PublicMode, PublicState};
use crate::private_tier::DisabledPrivateTier;
use crate::test_support::{TestDb, fresh_db, hooks};

pub(super) const MEMBER: &str = "did:plc:fhmemberaaaaaaaaaaaaaaaa";
pub(super) const OUTSIDER: &str = "did:plc:fhoutsideraaaaaaaaaaaaa";
const REV: &str = "3kaaaaaaaaa22";

/// Replace any database path in a Redis URL with `/<db>`; never append.
pub(super) fn redis_url_with_db(url: &str, db: u32) -> String {
    let (scheme, rest) = url.split_once("://").unwrap_or(("redis", url));
    let authority = rest.split('/').next().unwrap_or(rest);
    format!("{scheme}://{authority}/{db}")
}

#[test]
fn redis_url_with_db_replaces_any_path() {
    assert_eq!(redis_url_with_db("redis://h:6379", 15), "redis://h:6379/15");
    assert_eq!(redis_url_with_db("redis://h:6379/", 15), "redis://h:6379/15");
    assert_eq!(redis_url_with_db("redis://h:6379/0", 15), "redis://h:6379/15");
    assert_eq!(redis_url_with_db("redis://user:secret@h:6379/3", 15), "redis://user:secret@h:6379/15");
}

pub(super) async fn connect(url: &str) -> ConnectionManager {
    let client = redis::Client::open(url).expect("redis url");
    ConnectionManager::new(client).await.expect("connect redis")
}

pub(super) struct Options {
    pub batch_size: u32,
    pub window: Duration,
    pub entry_delay: Duration,
    /// Create the group before anything is added (§5.2); `false` leaves it
    /// absent.
    pub create_group: bool,
    /// A Redis database index for the fixture's connections.
    pub redis_db: Option<u32>,
    pub members: Vec<String>,
    pub mode: PublicMode,
    pub stream_prefix: &'static str,
}

impl Default for Options {
    fn default() -> Self {
        Self {
            batch_size: 20,
            window: Duration::from_secs(4),
            entry_delay: Duration::ZERO,
            create_group: true,
            redis_db: None,
            members: vec![MEMBER.to_string()],
            mode: PublicMode::On,
            stream_prefix: "test:fh",
        }
    }
}

pub(super) struct Fixture {
    pub db: TestDb,
    pub url: String,
    /// The test's own connection, on the fixture's database index.
    pub redis: ConnectionManager,
    pub cfg: FirehoseConfig,
    pub writer: Writer,
    pub status: Arc<ConsumerStatus>,
    window: Duration,
    entry_delay: Duration,
}

impl Fixture {
    pub async fn new(test: &str, opts: Options) -> Option<Self> {
        let Ok(base) = std::env::var("HPRS_TEST_REDIS_URL") else {
            eprintln!("HPRS_TEST_REDIS_URL unset; skipping consumer test {test}");
            return None;
        };
        let db = fresh_db(test).await?;
        let url = match opts.redis_db {
            Some(n) => redis_url_with_db(&base, n),
            None => base,
        };
        let mut redis = connect(&url).await;
        scripts::load_all(&mut redis).await.expect("load scripts");
        let tag = rand::random::<u32>();
        let cfg = FirehoseConfig {
            stream_key: format!("{}:{tag}", opts.stream_prefix),
            dead_letter_stream: format!("{}:{tag}:dead", opts.stream_prefix),
            batch_size: opts.batch_size,
            ..FirehoseConfig::default()
        };
        let mut sets = ScopeSets::default();
        for m in &opts.members {
            sqlx::query("INSERT INTO members (did, source, ready_at) VALUES ($1, 'admin', now())")
                .bind(m)
                .execute(&db.pool)
                .await
                .expect("member");
            sets.members.insert(m.clone());
            sets.scope.insert(m.clone());
        }
        let scope = ScopeState::new(sets, ScopePolicy::MembersAndOneHop, 10_000);
        {
            let mut guard = scope.write_guard().await;
            scope.set_public(&mut guard, PublicState { mode: opts.mode, generation: 1, last_fill_since: None });
        }
        let writer = Writer::new(db.pool.clone(), scope, Arc::new(DisabledPrivateTier));
        if opts.create_group {
            // On the absent stream: MKSTREAM, the group at 0-0, ENTRIESREAD
            // 0, so entries a test adds are delivered (§5.2).
            assert!(scripts::group_create(&mut redis, &cfg.stream_key, &cfg.group).await.expect("create group"));
        }
        Some(Self {
            db,
            url,
            redis,
            cfg,
            writer,
            status: Arc::default(),
            window: opts.window,
            entry_delay: opts.entry_delay,
        })
    }

    /// A consumer on the fixture's database and stream, with a connection
    /// of its own; and that connection's client id.
    pub async fn consumer(&self) -> (Consumer, i64) {
        let conn = connect(&self.url).await;
        let mut probe = conn.clone();
        let id: i64 = redis::cmd("CLIENT").arg("ID").query_async(&mut probe).await.expect("client id");
        let labels: Vec<String> = self.cfg.relay_labels.values().cloned().collect();
        let (coverage, _task) = CoverageTracker::new(labels, Duration::from_secs(600), 1_000_000).spawn();
        let watermarks = Watermarks::new(self.cfg.relay_labels.clone(), self.cfg.private_relays.clone());
        let mut c = Consumer::new(self.cfg.clone(), conn, self.writer.clone(), watermarks, coverage, Arc::clone(&self.status));
        c.throughput = Throughput::new(self.window);
        c.test.entry_delay = self.entry_delay;
        (c, id)
    }

    pub fn key(&self) -> &str {
        self.db.name()
    }

    /// The F2 tests' input: [`spawn_producer`] at 600 a second with
    /// `MAXLEN 300`, held while this fixture's consumer is in a gap
    /// transaction or held at `BEFORE_REANCHOR`.
    pub fn f2_producer(&self, within: Duration) -> JoinHandle<u64> {
        let status = Arc::clone(&self.status);
        let key = self.key().to_string();
        let hold = move || status.gap_blocked() || hooks::holding(hooks::BEFORE_REANCHOR, &key);
        spawn_producer(self.url.clone(), self.cfg.stream_key.clone(), 600, 300, within, hold)
    }

    /// `XADD`, with an exact `MAXLEN` when given; the new id.
    pub async fn add(&mut self, fields: &[(&str, String)], maxlen: Option<u64>) -> String {
        let mut cmd = redis::cmd("XADD");
        cmd.arg(&self.cfg.stream_key);
        if let Some(m) = maxlen {
            cmd.arg("MAXLEN").arg(m);
        }
        cmd.arg("*");
        for (k, v) in fields {
            cmd.arg(*k).arg(v);
        }
        cmd.query_async(&mut self.redis).await.expect("xadd")
    }

    pub async fn add_commit(&mut self, repo: &str, ops: serde_json::Value) -> String {
        self.add(&commit(repo, ops), None).await
    }

    /// `n` commits with nothing to write, added atomically (`MULTI`): a
    /// consumer that recreates its group after a wipe sees all of them or
    /// none, never a refill cut in two.
    pub async fn add_n(&mut self, n: usize, maxlen: Option<u64>) -> Vec<String> {
        if n == 0 {
            return Vec::new();
        }
        let mut pipe = redis::pipe();
        pipe.atomic();
        for _ in 0..n {
            let cmd = pipe.cmd("XADD").arg(&self.cfg.stream_key);
            if let Some(m) = maxlen {
                cmd.arg("MAXLEN").arg(m);
            }
            cmd.arg("*");
            for (k, v) in &commit(OUTSIDER, json!([])) {
                cmd.arg(*k).arg(v);
            }
        }
        pipe.query_async(&mut self.redis).await.expect("xadd")
    }

    pub async fn del_stream(&mut self) {
        let _: () = redis::cmd("DEL").arg(&self.cfg.stream_key).query_async(&mut self.redis).await.expect("del");
    }

    pub async fn persist_watermark(&self) {
        sqlx::query("INSERT INTO worker_state (key, value, updated_at) VALUES ('watermark:bsky', to_jsonb(now()::text), now())")
            .execute(&self.db.pool)
            .await
            .expect("watermark");
    }

    pub async fn epoch(&self) -> i64 {
        let mut conn = self.db.pool.acquire().await.expect("conn");
        gap::current_epoch(&mut conn).await.expect("epoch")
    }

    pub async fn owed(&self) -> bool {
        gap::recreation_gap_owed(&self.db.pool).await.expect("owed")
    }

    pub async fn group_exists(&mut self) -> bool {
        self.group_field("name").await.is_some()
    }

    /// A field of the group in `XINFO GROUPS`; `None` without the group or
    /// the stream.
    pub async fn group_field(&mut self, name: &str) -> Option<Value> {
        let reply: Result<Value, redis::RedisError> =
            redis::cmd("XINFO").arg("GROUPS").arg(&self.cfg.stream_key).query_async(&mut self.redis).await;
        reply.ok().and_then(|groups| group_field(&groups, &self.cfg.group, name))
    }

    pub async fn stream_u64(&mut self, name: &str) -> Option<u64> {
        let reply: Value = redis::cmd("XINFO").arg("STREAM").arg(&self.cfg.stream_key).query_async(&mut self.redis).await.ok()?;
        flat_u64(&reply, name)
    }

    pub async fn cleanup(mut self) {
        let _: Result<(), redis::RedisError> = redis::cmd("DEL")
            .arg(&self.cfg.stream_key)
            .arg(&self.cfg.dead_letter_stream)
            .query_async(&mut self.redis)
            .await;
        self.db.drop_db().await;
    }
}

/// A commit entry's fields.
pub(super) fn commit(repo: &str, ops: serde_json::Value) -> Vec<(&'static str, String)> {
    let data = json!({"repo": repo, "rev": REV, "ops": ops, "time": chrono::Utc::now().to_rfc3339()});
    vec![("type", "commit".to_string()), ("data", data.to_string()), ("seq", "1".to_string())]
}

pub(super) fn recorder() -> (PrometheusRecorder, PrometheusHandle) {
    let recorder = PrometheusBuilder::new().build_recorder();
    let handle = recorder.handle();
    (recorder, handle)
}

/// Startup, retried until it succeeds, then the loop until `stop` is set or
/// `within` has passed, in a task of its own; the consumer comes back.
pub(super) fn drive(mut c: Consumer, stop: Arc<AtomicBool>, within: Duration) -> JoinHandle<Consumer> {
    tokio::spawn(async move {
        let deadline = Instant::now() + within;
        while c.startup().await.is_err() && Instant::now() < deadline {
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        let left = deadline.saturating_duration_since(Instant::now());
        c.run_until(move |_| stop.load(Ordering::Relaxed), left).await;
        c
    })
}

/// Poll `cond`, which may `.await`, every 10 ms until it holds or `within`
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
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }};
}

/// `XADD`s commits with nothing to write at `rate` per second, in 10 ms
/// ticks, with an exact `MAXLEN`, for `within`. The number added.
///
/// No tick adds while `hold` is true. The F2 tests hold their input while
/// the consumer is in a gap transaction or held by the test before
/// `reanchor`: on this test's scale (a 300-entry stream, half a second at
/// 600 a second) those pauses, which the suite's load on the test database
/// and its scheduler stretches, can take a whole span, so whether a gap
/// came within a span of the last would be decided by the machine's load
/// and not by the consumer's rate. In production a span is minutes. A
/// missed tick is skipped, not made up in a burst: a test thread kept off
/// the CPU stops consumer and input alike.
pub(super) fn spawn_producer(
    url: String,
    stream: String,
    rate: u64,
    maxlen: u64,
    within: Duration,
    hold: impl Fn() -> bool + Send + 'static,
) -> JoinHandle<u64> {
    tokio::spawn(async move {
        let mut c = connect(&url).await;
        let per_tick = rate / 100;
        let mut tick = tokio::time::interval(Duration::from_millis(10));
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        let end = Instant::now() + within;
        let mut added = 0;
        while Instant::now() < end {
            tick.tick().await;
            if hold() {
                continue;
            }
            let mut pipe = redis::pipe();
            for _ in 0..per_tick {
                let fields = commit(OUTSIDER, json!([]));
                let cmd = pipe.cmd("XADD").arg(&stream).arg("MAXLEN").arg(maxlen).arg("*");
                for (k, v) in &fields {
                    cmd.arg(*k).arg(v);
                }
                cmd.ignore();
            }
            pipe.query_async::<()>(&mut c).await.expect("produce");
            added += per_tick;
        }
        added
    })
}

/// A stream id as `(ms, seq)`, for ordering.
pub(super) fn id_key(id: &str) -> (u64, u64) {
    let (ms, seq) = id.split_once('-').expect("stream id");
    (ms.parse().expect("ms"), seq.parse().expect("seq"))
}

fn record(c: &Consumer) -> std::sync::MutexGuard<'_, Record> {
    c.test.record.lock().expect("record")
}

/// One timed and booked step, in `mode`.
async fn turn(c: &mut Consumer, mode: Mode) {
    let mut mode = mode;
    let mut backoff = BACKOFF_MIN;
    c.turn(&mut mode, &mut backoff).await;
}

/// The decisions booked in `firehose_reanchor_decisions_total`, by reason.
fn decision_series(handle: &PrometheusHandle) -> Vec<(Reason, f64)> {
    Reason::ALL
        .iter()
        .map(|r| (*r, rendered(handle, METRIC_REANCHOR_DECISIONS, &[&format!("reason=\"{}\"", r.as_str())]).unwrap_or(0.0)))
        .collect()
}

/// Both views of the decisions agree: the exported counter and the record.
fn assert_decisions_agree(handle: &PrometheusHandle, reanchors: &[ReanchorRecord]) {
    for (reason, n) in decision_series(handle) {
        #[allow(clippy::cast_precision_loss)]
        let recorded = reanchors.iter().filter(|r| r.reason == reason).count() as f64;
        assert_eq!(n, recorded, "{}", reason.as_str());
    }
}

// F1 (§1.6).

/// An invalid op that could matter (it names a member) is dead-lettered
/// without a transaction.
#[tokio::test]
async fn invalid_op_without_a_transaction_is_dead_lettered() {
    let Some(mut f) = Fixture::new("fh_invalid_op_dead_lettered", Options::default()).await else {
        return;
    };
    let like = json!([{"action": "create", "path": "app.bsky.feed.like/bad rkey!", "cid": null,
        "record": {"subject": {"uri": format!("at://{MEMBER}/app.bsky.feed.post/1"), "cid": "c"}}}]);
    f.add_commit(OUTSIDER, like).await;
    let (mut c, _) = f.consumer().await;
    c.startup().await.expect("startup");
    assert!(c.run_until(|c| c.throughput.processed >= 1, Duration::from_secs(5)).await);
    assert_eq!(c.throughput.by_path, [1, 0, 0, 0], "no transaction");
    let dead: u64 = redis::cmd("XLEN").arg(&f.cfg.dead_letter_stream).query_async(&mut f.redis).await.expect("xlen");
    assert_eq!(dead, 1);
    let pending: Value = redis::cmd("XPENDING").arg(&f.cfg.stream_key).arg(&f.cfg.group).query_async(&mut f.redis).await.expect("xpending");
    let Value::Array(parts) = pending else {
        panic!("XPENDING {pending:?}");
    };
    assert_eq!(parts.first().and_then(as_u64), Some(0), "acked");
    f.cleanup().await;
}

/// T-AW11 (actionable-warnings design §6): malformed entries that could not
/// matter are acked, counted and not dead-lettered; those that could matter
/// are dead-lettered as before.
#[tokio::test]
async fn malformed_entries_that_could_not_matter_are_dropped() {
    let (rec, handle) = recorder();
    let _local = metrics::set_default_local_recorder(&rec);
    let Some(mut f) = Fixture::new("fh_malformed_dropped", Options::default()).await else {
        return;
    };
    let bad_rev = |repo: &str, ops: serde_json::Value| {
        let data = json!({"repo": repo, "rev": "3lxcday8ef4p4", "ops": ops});
        vec![("type", "commit".to_string()), ("data", data.to_string()), ("seq", "1".to_string())]
    };
    let like_of = |author: &str| {
        json!([{"action": "create", "path": "app.bsky.feed.like/3kaaaaaaaaaaa", "cid": "bafy",
            "record": {"subject": {"uri": format!("at://{author}/app.bsky.feed.post/3kbbbbbbbbbbb"), "cid": "bafy"}, "createdAt": "2026-10-20T00:00:00Z"}}])
    };
    let broken_like = |named: &str| {
        json!([{"action": "create", "path": "app.bsky.feed.like/3kaaaaaaaaaaa", "cid": "bafy",
            "record": {"subject": {"uri": "not-a-uri", "x": named}}}])
    };
    // Dropped: a stranger's bad-rev like of a stranger, an event naming no
    // DID, a stranger's broken like naming nobody.
    f.add(&bad_rev(OUTSIDER, like_of("did:plc:nobody")), None).await;
    f.add(&[("type", "account".into()), ("data", json!({"did": "bob"}).to_string()), ("seq", "2".into())], None).await;
    f.add_commit(OUTSIDER, broken_like("did:plc:nobody")).await;
    // Dead-lettered: the same from a member, a bad-rev like of a member's
    // post (admitted under On), a broken like naming a member.
    f.add(&bad_rev(MEMBER, like_of("did:plc:nobody")), None).await;
    f.add(&bad_rev(OUTSIDER, like_of(MEMBER)), None).await;
    f.add_commit(OUTSIDER, broken_like(MEMBER)).await;
    let (mut c, _) = f.consumer().await;
    c.startup().await.expect("startup");
    assert!(c.run_until(|c| c.throughput.processed >= 6, Duration::from_secs(5)).await);
    // Paths: no_tx, tx, identity, dead_letter.
    assert_eq!(c.throughput.by_path, [4, 0, 0, 2], "the dropped entries and the op-level ones ran no transaction");
    let dead: Vec<(String, Vec<(String, String)>)> =
        redis::cmd("XRANGE").arg(&f.cfg.dead_letter_stream).arg("-").arg("+").query_async(&mut f.redis).await.expect("xrange");
    let repos: Vec<String> = dead
        .iter()
        .map(|(_, fields)| fields.iter().find(|(k, _)| k == "repo").map(|(_, v)| v.clone()).unwrap_or_default())
        .collect();
    assert_eq!(repos, [MEMBER, OUTSIDER, OUTSIDER], "{dead:?}");
    for (check, n) in [("rev", 1.0), ("did", 1.0), ("record", 1.0)] {
        assert_eq!(rendered(&handle, crate::firehose::deadletter::METRIC_MALFORMED_DROPPED, &[&format!("check=\"{check}\"")]), Some(n), "{check}");
    }
    let pending: Value = redis::cmd("XPENDING").arg(&f.cfg.stream_key).arg(&f.cfg.group).query_async(&mut f.redis).await.expect("xpending");
    let Value::Array(parts) = pending else {
        panic!("XPENDING {pending:?}");
    };
    assert_eq!(parts.first().and_then(as_u64), Some(0), "all acked");
    f.cleanup().await;
}

// F4 (§4.5).

#[tokio::test]
async fn entries_processed_counts_every_path() {
    let (rec, handle) = recorder();
    let _local = metrics::set_default_local_recorder(&rec);
    let Some(mut f) = Fixture::new("fh_entries_processed_every_path", Options::default()).await else {
        return;
    };
    let (mut c, _) = f.consumer().await;
    // Registered before any entry.
    for path in EntryPath::ALL {
        assert_eq!(rendered(&handle, METRIC_PROCESSED, &[&format!("path=\"{}\"", path.as_str())]), Some(0.0), "{}", path.as_str());
    }
    for mode in ["oldest", "tail"] {
        assert_eq!(rendered(&handle, METRIC_REANCHOR, &[&format!("mode=\"{mode}\"")]), Some(0.0), "{mode}");
    }
    for (reason, n) in decision_series(&handle) {
        assert_eq!(n, 0.0, "{}", reason.as_str());
        assert!(rendered(&handle, METRIC_REANCHOR_DECISIONS, &[&format!("reason=\"{}\"", reason.as_str())]).is_some());
    }
    assert_eq!(rendered(&handle, METRIC_GROUP_PRESENT, &[]), Some(1.0));

    f.add_commit(OUTSIDER, json!([])).await;
    f.add_commit(MEMBER, json!([{"action": "create", "path": "app.bsky.feed.generator/3kgen", "cid": null, "record": {}}])).await;
    f.add(&[("type", "identity".into()), ("data", json!({"did": OUTSIDER}).to_string()), ("seq", "2".into())], None).await;
    f.add(&[("type", "bogus".into()), ("data", "{}".into()), ("seq", "3".into())], None).await;
    c.startup().await.expect("startup");
    assert!(c.run_until(|c| c.throughput.processed >= 4, Duration::from_secs(5)).await);
    assert_eq!(c.throughput.by_path, [1, 1, 1, 1]);
    for path in EntryPath::ALL {
        assert_eq!(rendered(&handle, METRIC_PROCESSED, &[&format!("path=\"{}\"", path.as_str())]), Some(1.0), "{}", path.as_str());
    }
    f.cleanup().await;
}

#[tokio::test]
async fn busy_books_successful_steps_only() {
    let Some(mut f) = Fixture::new("fh_busy_successful_steps", Options::default()).await else {
        return;
    };
    let (mut c, _) = f.consumer().await;
    c.startup().await.expect("startup");

    // Idle, blocking: almost nothing is busy.
    let before = c.throughput.busy;
    c.run_until(|_| false, Duration::from_secs(3)).await;
    assert!(c.throughput.busy - before < Duration::from_millis(100), "{:?}", c.throughput.busy - before);

    // A trim gap: its transaction's time is busy, and none of it backlog.
    f.add_n(10, Some(5)).await;
    let (busy, backlog) = (c.throughput.busy, c.throughput.backlog_busy);
    turn(&mut c, Mode::New).await;
    let gap_time = record(&c).gaps.last().expect("a gap").3;
    assert_eq!(record(&c).reanchors.len(), 1);
    assert!(c.throughput.busy - busy >= gap_time, "{:?} < {gap_time:?}", c.throughput.busy - busy);
    assert_eq!(c.throughput.backlog_busy, backlog);

    // A failed step books nothing and clears the window.
    turn(&mut c, Mode::New).await;
    assert!(c.throughput.samples() > 0);
    let hang = hooks::arm_delay(hooks::WRITER_HANG, f.key(), Duration::from_secs(1), Some(1));
    f.add_n(1, None).await;
    let busy = c.throughput.busy;
    turn(&mut c, Mode::New).await;
    assert_eq!(hang.hits(), 1);
    assert_eq!(c.throughput.busy, busy);
    assert_eq!(c.throughput.samples(), 0);
    drop(hang);
    f.cleanup().await;
}

#[tokio::test]
async fn backlog_counters_count_full_gap_free_batches_only() {
    let Some(mut f) = Fixture::new("fh_backlog_full_batches", Options::default()).await else {
        return;
    };
    f.add_n(100, None).await;
    let (mut c, _) = f.consumer().await;
    c.startup().await.expect("startup");
    assert!(c.run_until(|c| c.throughput.processed >= 100, Duration::from_secs(10)).await);
    for i in 1..=5 {
        f.add_n(1, None).await;
        assert!(c.run_until(|c| c.throughput.processed >= 100 + i, Duration::from_secs(5)).await);
    }
    assert_eq!((c.throughput.backlog_processed, c.throughput.processed), (100, 105));
    assert!(c.throughput.backlog_busy < c.throughput.busy);

    // A trim gap whose re-anchor returns a full batch: not backlog.
    f.add_n(60, Some(30)).await;
    let (processed, busy) = (c.throughput.backlog_processed, c.throughput.backlog_busy);
    turn(&mut c, Mode::New).await;
    assert_eq!(record(&c).reanchors.len(), 1);
    assert_eq!(record(&c).batches.last().map(|b| b.1), Some(20));
    assert_eq!((c.throughput.backlog_processed, c.throughput.backlog_busy), (processed, busy));
    f.cleanup().await;
}

#[tokio::test]
async fn sampler_reports_a_missing_group() {
    let (rec, handle) = recorder();
    let _local = metrics::set_default_local_recorder(&rec);
    let opts = Options { create_group: false, ..Options::default() };
    let Some(mut f) = Fixture::new("fh_sampler_missing_group", opts).await else {
        return;
    };
    f.add_n(10, None).await;
    let sampler = spawn_lag_sampler(f.redis.clone(), f.cfg.clone(), Arc::default(), Duration::from_millis(100));
    tokio::time::sleep(Duration::from_millis(250)).await;
    assert_eq!(rendered(&handle, METRIC_GROUP_PRESENT, &[]), Some(0.0));
    assert_eq!(rendered(&handle, METRIC_LAG, &[]), Some(10.0), "the stream's length");
    assert_eq!(rendered(&handle, METRIC_PENDING, &[]), Some(0.0));
    let added = rendered(&handle, METRIC_STREAM_ADDED, &[]).unwrap_or(0.0);
    f.add_n(5, None).await;
    tokio::time::sleep(Duration::from_millis(250)).await;
    assert_eq!(rendered(&handle, METRIC_STREAM_ADDED, &[]), Some(added + 5.0), "the input keeps counting");
    assert!(scripts::group_create(&mut f.redis, &f.cfg.stream_key, &f.cfg.group).await.expect("create"));
    tokio::time::sleep(Duration::from_millis(250)).await;
    assert_eq!(rendered(&handle, METRIC_GROUP_PRESENT, &[]), Some(1.0));
    sampler.abort();
    f.cleanup().await;
}

// F2 (§2.6).

#[tokio::test]
async fn a_slow_consumer_tail_anchors_after_the_window() {
    let (rec, handle) = recorder();
    let _local = metrics::set_default_local_recorder(&rec);
    // About 200 entries a second against 600.
    let opts = Options { entry_delay: Duration::from_millis(5), ..Options::default() };
    let Some(mut f) = Fixture::new("fh_slow_consumer_tail", opts).await else {
        return;
    };
    let (mut c, _) = f.consumer().await;
    c.startup().await.expect("startup");
    let start = Instant::now();
    let producer = f.f2_producer(Duration::from_secs(10));
    let at_five = {
        let url = f.url.clone();
        let stream = f.cfg.stream_key.clone();
        tokio::spawn(async move {
            tokio::time::sleep_until((start + Duration::from_secs(5)).into()).await;
            let mut c = connect(&url).await;
            let info: Value = redis::cmd("XINFO").arg("STREAM").arg(&stream).query_async(&mut c).await.expect("xinfo");
            flat_u64(&info, "entries-added").expect("entries-added")
        })
    };
    c.run_until(|_| false, Duration::from_secs(10)).await;
    producer.await.expect("producer");
    let added_at_five = at_five.await.expect("sample");
    let added_at_end = f.stream_u64("entries-added").await.expect("entries-added");

    let rec = record(&c).clone();
    let reanchors = rec.reanchors.clone();
    assert!(!reanchors.is_empty());
    for r in reanchors.iter().filter(|r| r.at < start + Duration::from_secs(4)) {
        assert_eq!(r.mode, ReanchorMode::Oldest, "{r:?}");
        assert!(matches!(r.reason, Reason::NoPrev | Reason::Span | Reason::NoWindow), "{r:?}");
    }
    assert!(reanchors.iter().any(|r| r.at > start + Duration::from_secs(4) && r.mode == ReanchorMode::Tail), "{reanchors:?}");
    let late: Vec<&ReanchorRecord> = reanchors.iter().filter(|r| r.at >= start + Duration::from_secs(5)).collect();
    let spans = (added_at_end - added_at_five).div_ceil(300);
    assert!(late.len() as u64 <= 2 * spans + 2, "{} re-anchors for {spans} spans", late.len());
    for (i, r) in late.iter().enumerate() {
        match (r.mode, r.reason) {
            // A tail decision; the script falls back to oldest when more
            // than a span arrived during the gap transaction (`tail_trimmed`),
            // which a loaded test database can make take that long.
            (ReanchorMode::Tail, Reason::Tail) | (ReanchorMode::Oldest, Reason::TailTrimmed) => {
                assert!(r.busy_share.expect("a window") >= 0.9, "{r:?}");
                assert!(r.processed_per_added.expect("a window") < 1.0, "{r:?}");
            }
            (ReanchorMode::Oldest, Reason::Span) => {
                if let Some(next) = late.get(i + 1) {
                    assert!(
                        matches!(next.reason, Reason::Tail | Reason::TailTrimmed),
                        "an oldest anchor is followed by a tail decision: {late:?}"
                    );
                }
            }
            other => panic!("unexpected decision {other:?}: {late:?}"),
        }
    }
    // Every applied entry is above the anchor before it.
    for (t, id) in &rec.processed {
        if let Some(r) = reanchors.iter().rev().find(|r| r.at <= *t) {
            assert!(id_key(id) > id_key(&r.anchor), "{id} at or below {}", r.anchor);
        }
    }
    assert_decisions_agree(&handle, &reanchors);
    f.cleanup().await;
}

#[tokio::test]
async fn a_stall_after_an_oldest_anchor_stays_oldest() {
    let (rec, handle) = recorder();
    let _local = metrics::set_default_local_recorder(&rec);
    // Unthrottled: far faster than the 600 a second it is fed, even with
    // the rest of the suite running beside it.
    let opts = Options::default();
    let Some(f) = Fixture::new("fh_stall_stays_oldest", opts).await else {
        return;
    };
    let (c, _) = f.consumer().await;
    let log = Arc::clone(&c.test.record);
    let start = Instant::now();
    let producer = f.f2_producer(Duration::from_secs(10));
    let stop = Arc::new(AtomicBool::new(false));
    let task = drive(c, Arc::clone(&stop), Duration::from_secs(15));
    // The first re-anchor holds before `reanchor` while the second stall is
    // armed, so the stall falls in its first batch.
    let before = hooks::arm(hooks::BEFORE_REANCHOR, f.key());
    tokio::time::sleep_until((start + Duration::from_secs(6)).into()).await;
    let first = hooks::arm_delay(hooks::CONSUMER_STALL, f.key(), Duration::from_secs(1), Some(1));
    before.reached().await;
    assert_eq!(first.hits(), 1);
    drop(first);
    let _second = hooks::arm_delay(hooks::CONSUMER_STALL, f.key(), Duration::from_millis(150), Some(1));
    before.release();
    assert!(eventually!(Duration::from_secs(5), log.lock().expect("record").reanchors.len() >= 2));
    tokio::time::sleep_until((start + Duration::from_secs(9)).into()).await;
    stop.store(true, Ordering::Relaxed);
    let c = task.await.expect("consumer");
    producer.await.expect("producer");

    let rec = record(&c).clone();
    let r = &rec.reanchors;
    assert_eq!(r.len(), 2, "no further gap: {r:?}");
    assert_eq!((r[0].mode, r[0].reason), (ReanchorMode::Oldest, Reason::NoPrev));
    assert_eq!((r[1].mode, r[1].reason), (ReanchorMode::Oldest, Reason::NotSaturated), "{r:?}");
    assert!(r[1].busy_share.expect("a window") < 0.9);
    let caught_up = rec
        .batches
        .iter()
        .any(|(t, n)| *t > r[1].at && *t < start + Duration::from_secs(9) && *n < 20);
    assert!(caught_up, "a read returned fewer than a batch by 9 s");
    assert_decisions_agree(&handle, r);
    drop(before);
    f.cleanup().await;
}

/// One run of the hung-database test: the decisions before the first trim
/// gap after the hang, and from it on.
async fn hung_database_run(name: &str, ring_reset: bool) -> Option<(Vec<ReanchorRecord>, Vec<ReanchorRecord>)> {
    let f = Fixture::new(name, Options::default()).await?;
    let (mut c, _) = f.consumer().await;
    c.test.ring_reset = ring_reset;
    let log = Arc::clone(&c.test.record);
    let producer = f.f2_producer(Duration::from_secs(14));
    let hang = hooks::arm_delay(hooks::WRITER_HANG, f.key(), Duration::from_secs(1), None);
    let stop = Arc::new(AtomicBool::new(false));
    let task = drive(c, Arc::clone(&stop), Duration::from_secs(20));
    // Meanwhile trim gaps re-anchor, and each first batch fails in `apply`.
    tokio::time::sleep(Duration::from_secs(6)).await;
    let before = hooks::arm(hooks::BEFORE_REANCHOR, f.key());
    drop(hang);
    // The first trim gap after the hang: a 150 ms stall in its first batch
    // forces a second gap within the span.
    before.reached().await;
    let stall = hooks::arm_delay(hooks::CONSUMER_STALL, f.key(), Duration::from_millis(150), Some(1));
    let released = Instant::now();
    before.release();
    let after = |r: &Record| r.reanchors.iter().filter(|r| r.at >= released).count();
    assert!(eventually!(Duration::from_secs(8), after(&log.lock().expect("record")) >= 2));
    stop.store(true, Ordering::Relaxed);
    let c = task.await.expect("consumer");
    producer.abort();
    let all = record(&c).reanchors.clone();
    drop((stall, before));
    f.cleanup().await;
    Some(all.into_iter().partition(|r| r.at < released))
}

#[tokio::test]
async fn a_hung_database_stays_oldest() {
    let (rec, handle) = recorder();
    let _local = metrics::set_default_local_recorder(&rec);
    let Some((during, r)) = hung_database_run("fh_hung_database_oldest", true).await else {
        return;
    };
    assert_eq!((r[0].mode, r[1].mode), (ReanchorMode::Oldest, ReanchorMode::Oldest), "{r:?}");
    assert_eq!(r[1].reason, Reason::NoWindow, "{r:?}");
    let every: Vec<ReanchorRecord> = during.into_iter().chain(r).collect();
    assert_decisions_agree(&handle, &every);
    // Without the reset the window reaches back before the hang, so the
    // second decision passes condition 2 and is made by a later one: the
    // test can fail.
    let (_, r) = hung_database_run("fh_hung_database_no_reset", false).await.expect("database");
    assert_ne!(r[1].reason, Reason::NoWindow, "{r:?}");
    assert!(r[1].busy_share.is_some(), "{r:?}");
}

// F6 (§6.5).

#[tokio::test]
async fn a_failed_recreation_gap_stays_owed() {
    let opts = Options { create_group: false, ..Options::default() };
    let Some(mut f) = Fixture::new("fh_failed_recreation_gap_owed", opts).await else {
        return;
    };
    f.persist_watermark().await;
    let (mut c, _) = f.consumer().await;
    let fail = hooks::arm_times(hooks::GAP_TX_FAILS, f.key(), 1);
    assert!(c.startup().await.is_err());
    assert!(f.group_exists().await);
    assert!(f.owed().await);
    assert_eq!(f.epoch().await, 0);
    assert!(f.status.gap_blocked(), "blocked from the failed attempt");
    let e1 = f.add_commit(OUTSIDER, json!([])).await;
    c.startup().await.expect("startup");
    assert_eq!(fail.hits(), 1);
    assert_eq!(f.epoch().await, 1);
    assert!(!f.owed().await);
    assert!(!f.status.gap_blocked(), "until the commit");
    let ready: Option<chrono::DateTime<chrono::Utc>> =
        sqlx::query_scalar("SELECT ready_at FROM members WHERE did = $1").bind(MEMBER).fetch_one(&f.db.pool).await.expect("member");
    assert_eq!(ready, None);
    assert!(c.run_until(|c| c.throughput.processed >= 1, Duration::from_secs(5)).await);
    let rec = record(&c).clone();
    assert_eq!(rec.processed[0].1, e1);
    assert!(rec.gaps[0].0 < rec.processed[0].0, "E1 processed after the gap");
    f.cleanup().await;
}

#[tokio::test]
async fn recreation_from_a_deleted_stream() {
    let opts = Options { create_group: false, ..Options::default() };
    let Some(mut f) = Fixture::new("fh_recreation_deleted_stream", opts).await else {
        return;
    };
    f.persist_watermark().await;
    let (mut c, _) = f.consumer().await;
    c.startup().await.expect("startup");
    assert_eq!(f.epoch().await, 1);
    assert_eq!(f.group_field("last-delivered-id").await.as_ref().and_then(scripts::string).as_deref(), Some("0-0"));
    f.add_n(5, None).await;
    assert!(c.run_until(|c| c.throughput.processed >= 5, Duration::from_secs(5)).await);
    let read = f.group_field("entries-read").await.as_ref().and_then(as_u64);
    let lag = f.group_field("lag").await.as_ref().and_then(as_u64).expect("lag");
    assert_eq!(read, Some(f.stream_u64("entries-added").await.expect("added") - lag));
    assert_eq!(lag, 0);
    assert_eq!(f.epoch().await, 1, "one gap");
    f.cleanup().await;
}

#[tokio::test]
async fn a_crash_after_the_create_runs_the_gap_before_any_read() {
    let opts = Options { create_group: false, ..Options::default() };
    let Some(mut f) = Fixture::new("fh_crash_after_create", opts).await else {
        return;
    };
    f.persist_watermark().await;
    let hold = hooks::arm(hooks::AFTER_RECREATION_CREATE, f.key());
    let (mut first, _) = f.consumer().await;
    let task = tokio::spawn(async move {
        let _ = first.startup().await;
    });
    hold.reached().await;
    let e1 = f.add_commit(OUTSIDER, json!([])).await;
    task.abort();
    let _ = task.await;
    drop(hold);
    assert_eq!(f.epoch().await, 0);
    assert!(f.owed().await);

    let (mut c, _) = f.consumer().await;
    c.startup().await.expect("startup");
    assert_eq!(f.epoch().await, 1);
    assert!(!f.owed().await);
    assert!(c.run_until(|c| c.throughput.processed >= 1, Duration::from_secs(5)).await);
    let rec = record(&c).clone();
    assert_eq!(rec.processed[0].1, e1);
    assert!(rec.gaps[0].0 < rec.processed[0].0);
    f.cleanup().await;
}

#[tokio::test]
async fn a_crash_after_the_flag_before_the_create() {
    let opts = Options { create_group: false, ..Options::default() };
    let Some(mut f) = Fixture::new("fh_crash_after_flag", opts).await else {
        return;
    };
    f.persist_watermark().await;
    let hold = hooks::arm(hooks::AFTER_RECREATION_FLAG, f.key());
    let (mut first, _) = f.consumer().await;
    let task = tokio::spawn(async move {
        let _ = first.startup().await;
    });
    hold.reached().await;
    task.abort();
    let _ = task.await;
    drop(hold);
    assert!(f.owed().await);
    assert!(!f.group_exists().await);

    let (mut c, _) = f.consumer().await;
    c.startup().await.expect("startup");
    assert!(f.group_exists().await);
    assert_eq!(f.epoch().await, 1, "exactly one gap");
    assert!(!f.owed().await);
    f.cleanup().await;
}

#[tokio::test]
async fn a_database_outage_between_create_and_gap_reads_nothing() {
    let opts = Options { create_group: false, ..Options::default() };
    let Some(mut f) = Fixture::new("fh_outage_reads_nothing", opts).await else {
        return;
    };
    f.persist_watermark().await;
    let failing = hooks::arm(hooks::GAP_TX_FAILS, f.key());
    let (c, _) = f.consumer().await;
    let log = Arc::clone(&c.test.record);
    let stop = Arc::new(AtomicBool::new(false));
    let armed_at = Instant::now();
    let task = drive(c, Arc::clone(&stop), Duration::from_secs(20));
    assert!(eventually!(Duration::from_secs(5), f.group_exists().await));
    let mut ids = Vec::new();
    while armed_at.elapsed() < Duration::from_secs(3) {
        ids.extend(f.add_n(1, None).await);
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    assert!(log.lock().expect("record").processed.is_empty(), "nothing is read while the gap is owed");
    assert!(f.owed().await);
    assert_eq!(f.epoch().await, 0);
    drop(failing);
    ids.extend(f.add_n(3, None).await);
    let n = ids.len();
    assert!(eventually!(Duration::from_secs(10), log.lock().expect("record").processed.len() >= n));
    stop.store(true, Ordering::Relaxed);
    let c = task.await.expect("consumer");
    assert_eq!(f.epoch().await, 1);
    assert!(!f.owed().await);
    let got: Vec<String> = record(&c).processed.iter().map(|p| p.1.clone()).collect();
    assert_eq!(got, ids, "every entry added after the create");
    f.cleanup().await;
}

#[tokio::test]
async fn a_second_wipe_before_the_gap_runs_a_second_gap() {
    let Some(mut f) = Fixture::new("fh_second_wipe_before_gap", Options::default()).await else {
        return;
    };
    f.persist_watermark().await;
    let (c, _) = f.consumer().await;
    let log = Arc::clone(&c.test.record);
    let stop = Arc::new(AtomicBool::new(false));
    let task = drive(c, Arc::clone(&stop), Duration::from_secs(20));
    f.add_n(1, None).await;
    assert!(eventually!(Duration::from_secs(5), !log.lock().expect("record").processed.is_empty()));
    // The worker reads on; stream 1 replaces its stream, and its create is
    // held.
    let hold = hooks::arm(hooks::AFTER_RECREATION_CREATE, f.key());
    f.del_stream().await;
    f.add_n(3, None).await;
    hold.reached().await;
    // Stream 2, refilled with more entries than stream 1 had.
    f.del_stream().await;
    f.add_n(6, None).await;
    drop(hold);
    assert!(eventually!(Duration::from_secs(10), f.epoch().await >= 2));
    let ids = f.add_n(2, None).await;
    assert!(eventually!(Duration::from_secs(5), log.lock().expect("record").processed.len() >= 3));
    stop.store(true, Ordering::Relaxed);
    let c = task.await.expect("consumer");
    assert_eq!(f.epoch().await, 2, "one gap per creation");
    assert!(!f.owed().await);
    let got: Vec<String> = record(&c).processed.iter().skip(1).map(|p| p.1.clone()).collect();
    assert_eq!(got, ids);
    // The counter is exact on stream 2.
    let read = f.group_field("entries-read").await.as_ref().and_then(as_u64).expect("entries-read");
    let lag = f.group_field("lag").await.as_ref().and_then(as_u64).expect("lag");
    assert_eq!(read, f.stream_u64("entries-added").await.expect("added") - lag);
    assert_eq!(read, 8);
    f.cleanup().await;
}

#[tokio::test]
async fn a_second_wipe_after_the_gap_runs_a_second_gap() {
    let opts = Options { create_group: false, ..Options::default() };
    let Some(mut f) = Fixture::new("fh_second_wipe_after_gap", opts).await else {
        return;
    };
    f.persist_watermark().await;
    let (c, _) = f.consumer().await;
    let log = Arc::clone(&c.test.record);
    let stop = Arc::new(AtomicBool::new(false));
    let task = drive(c, Arc::clone(&stop), Duration::from_secs(20));
    assert!(eventually!(Duration::from_secs(5), f.epoch().await >= 1));
    f.add_n(2, None).await;
    assert!(eventually!(Duration::from_secs(5), log.lock().expect("record").processed.len() >= 2));
    f.del_stream().await;
    f.add_n(4, None).await;
    assert!(eventually!(Duration::from_secs(10), f.epoch().await >= 2));
    let id = f.add_n(1, None).await;
    assert!(eventually!(Duration::from_secs(5), log.lock().expect("record").processed.len() >= 3));
    stop.store(true, Ordering::Relaxed);
    let c = task.await.expect("consumer");
    assert_eq!(f.epoch().await, 2);
    assert_eq!(record(&c).processed[2].1, id[0]);
    let read = f.group_field("entries-read").await.as_ref().and_then(as_u64).expect("entries-read");
    assert_eq!(read, f.stream_u64("entries-added").await.expect("added"));
    assert_eq!(read, 5);
    assert!(!f.owed().await);
    f.cleanup().await;
}

#[tokio::test]
async fn a_failing_create_does_not_repeat_the_gap() {
    let opts = Options { create_group: false, ..Options::default() };
    let Some(f) = Fixture::new("fh_failing_create_one_gap", opts).await else {
        return;
    };
    f.persist_watermark().await;
    let fails = hooks::arm_times(hooks::GROUP_CREATE_FAILS, f.key(), 2);
    let (mut c, _) = f.consumer().await;
    for _ in 0..2 {
        assert!(c.startup().await.is_err());
        assert_eq!(f.epoch().await, 0);
        assert!(f.status.gap_blocked(), "/xrpc/_health shows the gap owed");
    }
    c.startup().await.expect("startup");
    assert_eq!(fails.hits(), 2);
    assert_eq!(f.epoch().await, 1, "one gap, after the successful create");
    assert!(!f.status.gap_blocked());
    f.cleanup().await;
}

/// §5.2: the only test on Redis database 15, and the only one that runs
/// `FLUSHDB`.
#[tokio::test]
async fn a_lost_group_in_pending_mode_is_recreated() {
    let opts = Options { redis_db: Some(15), ..Options::default() };
    let Some(mut f) = Fixture::new("fh_lost_group_pending_mode", opts).await else {
        return;
    };
    f.persist_watermark().await;
    let (c, client) = f.consumer().await;
    let log = Arc::clone(&c.test.record);
    let hang = hooks::arm_delay(hooks::WRITER_HANG, f.key(), Duration::from_secs(1), Some(1));
    f.add_commit(OUTSIDER, json!([])).await;
    let stop = Arc::new(AtomicBool::new(false));
    let task = drive(c, Arc::clone(&stop), Duration::from_secs(30));
    hang.reached().await;
    // While the apply sleeps: Redis's data goes, and the connection drops.
    let _: () = redis::cmd("FLUSHDB").query_async(&mut f.redis).await.expect("flushdb");
    let _: Value = redis::cmd("CLIENT").arg("KILL").arg("ID").arg(client).query_async(&mut f.redis).await.expect("client kill");
    f.add_commit(OUTSIDER, json!([])).await;
    assert!(eventually!(Duration::from_secs(15), f.epoch().await >= 1));
    assert!(f.group_exists().await);
    let id = f.add_commit(OUTSIDER, json!([])).await;
    assert!(
        eventually!(Duration::from_secs(10), log.lock().expect("record").processed.iter().any(|p| p.1 == id))
    );
    stop.store(true, Ordering::Relaxed);
    let c = task.await.expect("consumer");
    let rec = record(&c).clone();
    assert!(rec.no_groups.iter().any(|(mode, _)| *mode == "pending"), "{:?}", rec.no_groups);
    drop(hang);
    let _: () = redis::cmd("FLUSHDB").query_async(&mut f.redis).await.expect("flushdb");
    f.cleanup().await;
}

#[tokio::test]
async fn group_present_tracks_a_held_absence() {
    let (rec, handle) = recorder();
    let _local = metrics::set_default_local_recorder(&rec);
    let Some(mut f) = Fixture::new("fh_group_present_held_absence", Options::default()).await else {
        return;
    };
    let (c, _) = f.consumer().await;
    let sampler = spawn_lag_sampler(f.redis.clone(), f.cfg.clone(), c.throughput_shared(), Duration::from_millis(100));
    let hold = hooks::arm(hooks::AFTER_RECREATION_FLAG, f.key());
    let stop = Arc::new(AtomicBool::new(false));
    let task = drive(c, Arc::clone(&stop), Duration::from_secs(20));
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(rendered(&handle, METRIC_GROUP_PRESENT, &[]), Some(1.0));
    f.del_stream().await;
    hold.reached().await;
    let held = Instant::now();
    assert!(eventually!(Duration::from_millis(400), rendered(&handle, METRIC_GROUP_PRESENT, &[]) == Some(0.0)));
    assert!(held.elapsed() < Duration::from_millis(500));
    tokio::time::sleep_until((held + Duration::from_millis(500)).into()).await;
    hold.release();
    assert!(eventually!(Duration::from_millis(250), rendered(&handle, METRIC_GROUP_PRESENT, &[]) == Some(1.0)));
    stop.store(true, Ordering::Relaxed);
    let _ = task.await;
    sampler.abort();
    drop(hold);
    f.cleanup().await;
}

/// Which way the stream is lost while `reanchor` waits.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Lost {
    /// Deleted: `reanchor` answers `ERR no such key script: …`.
    Absent,
    /// Deleted and one entry added: `XGROUP SETID` answers `NOGROUP`.
    Recreated,
}

#[tokio::test]
async fn a_lost_group_during_reanchor_is_recreated() {
    let (rec, handle) = recorder();
    let _local = metrics::set_default_local_recorder(&rec);
    for lost in [Lost::Absent, Lost::Recreated] {
        let Some(mut f) = Fixture::new(&format!("fh_lost_group_reanchor_{lost:?}"), Options::default()).await else {
            return;
        };
        f.persist_watermark().await;
        f.add_n(10, Some(5)).await;
        let hold = hooks::arm(hooks::BEFORE_REANCHOR, f.key());
        let (c, _) = f.consumer().await;
        let log = Arc::clone(&c.test.record);
        let stop = Arc::new(AtomicBool::new(false));
        let task = drive(c, Arc::clone(&stop), Duration::from_secs(20));
        hold.reached().await;
        assert_eq!(f.epoch().await, 1, "the trim gap committed");
        f.del_stream().await;
        if lost == Lost::Recreated {
            f.add_n(1, None).await;
        }
        hold.release();
        assert!(eventually!(Duration::from_secs(10), f.epoch().await >= 2), "{lost:?}");
        let id = f.add_n(1, None).await;
        assert!(
            eventually!(Duration::from_secs(5), log.lock().expect("record").processed.iter().any(|p| p.1 == id[0])),
            "{lost:?}: the worker resumes"
        );
        stop.store(true, Ordering::Relaxed);
        let c = task.await.expect("consumer");
        let rec = record(&c).clone();
        let (_, error) = rec.no_groups.first().expect("a missing group");
        match lost {
            Lost::Absent => assert!(error.contains("no such key"), "{error}"),
            Lost::Recreated => assert!(error.contains("NOGROUP"), "{error}"),
        }
        assert!(rec.reanchors.is_empty(), "no decision is booked for the failed reanchor");
        assert_eq!(f.epoch().await, 2);
        drop(hold);
        f.cleanup().await;
    }
    assert!(decision_series(&handle).iter().all(|(_, n)| *n == 0.0));
}

#[tokio::test]
async fn recreation_clears_reanchor_state_before_the_create() {
    let Some(mut f) = Fixture::new("fh_recreation_clears_state", Options::default()).await else {
        return;
    };
    f.add_n(10, Some(5)).await;
    let (mut c, _) = f.consumer().await;
    c.startup().await.expect("startup");
    assert!(c.run_until(|c| c.prev_reanchor_added.is_some(), Duration::from_secs(5)).await);
    assert!(c.throughput.samples() > 0);
    f.del_stream().await;
    assert!(c.run_until(|c| !record(c).at_recreation_flag.is_empty(), Duration::from_secs(5)).await);
    assert_eq!(record(&c).at_recreation_flag[0], (None, 0));
    f.cleanup().await;
}

#[tokio::test]
async fn no_gap_on_first_ever_creation() {
    let opts = Options { create_group: false, ..Options::default() };
    let Some(mut f) = Fixture::new("fh_no_gap_first_creation", opts).await else {
        return;
    };
    f.add_n(3, None).await;
    let (mut c, _) = f.consumer().await;
    c.startup().await.expect("startup");
    assert_eq!(f.epoch().await, 0);
    assert!(!f.owed().await);
    let id = f.add_n(1, None).await;
    assert!(c.run_until(|c| c.throughput.processed >= 1, Duration::from_secs(5)).await);
    c.run_until(|_| false, Duration::from_millis(1500)).await;
    let got: Vec<String> = record(&c).processed.iter().map(|p| p.1.clone()).collect();
    assert_eq!(got, id, "history is not delivered");
    assert_eq!(f.epoch().await, 0);
    f.cleanup().await;
}
