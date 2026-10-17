//! M1 (firehose throughput design §5.3): a corpus exported read-only from a
//! running stack's stream, and the relative bench over it. Both are
//! `#[ignore]`d and run alone, each under its own filter, in a release
//! build:
//!
//! - `export_corpus` reads `HPRS_BENCH_SOURCE_REDIS_URL` (the stack's Redis,
//!   by container name), `HPRS_BENCH_MEMBERS` (comma-separated) and
//!   `HPRS_BENCH_CORPUS` (a path inside the target volume). It connects to no
//!   database and only reads (`XREVRANGE`).
//! - `throughput_corpus` reads `HPRS_BENCH_CORPUS` and reports `C0` (every
//!   commit on the transaction path, `WRITER_FORCE_TX` armed for `*`) and
//!   `C1` (as built), under `PublicMode::Off` and then `community_scoped`.

use std::io::{BufRead, BufWriter, Write};
use std::time::{Duration, Instant};

use redis::Value;
use serde_json::json;

use super::super::*;
use super::live::{Fixture, Options, connect};
use crate::firehose::stream::parse_entry;
use crate::indexing::PublicMode;
use crate::test_support::hooks;

const SOURCE_STREAM: &str = "firehose:events";
const CORPUS_ENTRIES: usize = 100_000;
/// §II-5.1's relays, so `label_for` maps each copy to its label.
const RELAYS: [&str; 3] = [
    "wss://bsky.network/xrpc/com.atproto.sync.subscribeRepos",
    "wss://relay.fire.hose.cam/xrpc/com.atproto.sync.subscribeRepos",
    "wss://relay.community-a.example/xrpc/com.atproto.sync.subscribeRepos",
];
/// How far apart the three copies of an entry sit in the bench stream.
const COPY_OFFSET: usize = 200;

fn env(name: &str) -> String {
    std::env::var(name).unwrap_or_else(|_| panic!("{name} is required"))
}

#[tokio::test]
#[ignore = "M1: exports a corpus from a running stack; set HPRS_BENCH_*"]
async fn export_corpus() {
    let source = env("HPRS_BENCH_SOURCE_REDIS_URL");
    let members: Vec<String> = env("HPRS_BENCH_MEMBERS").split(',').map(|s| s.trim().to_string()).filter(|s| !s.is_empty()).collect();
    let out = env("HPRS_BENCH_CORPUS");
    let mut c = connect(&source).await;
    let mut entries = Vec::with_capacity(CORPUS_ENTRIES);
    let mut end = "+".to_string();
    while entries.len() < CORPUS_ENTRIES {
        let reply: Value = redis::cmd("XREVRANGE")
            .arg(SOURCE_STREAM)
            .arg(&end)
            .arg("-")
            .arg("COUNT")
            .arg(1000)
            .query_async(&mut c)
            .await
            .expect("xrevrange");
        let Value::Array(page) = reply else {
            panic!("XREVRANGE {reply:?}");
        };
        if page.is_empty() {
            break;
        }
        for e in page {
            entries.push(parse_entry(e).expect("entry"));
        }
        end = format!("({}", entries.last().expect("entry").id);
    }
    entries.truncate(CORPUS_ENTRIES);
    entries.reverse();
    let mut w = BufWriter::new(std::fs::File::create(&out).expect("corpus file"));
    writeln!(w, "{}", json!({"members": members})).expect("write");
    for e in &entries {
        let line = json!({
            "type": e.field("type").unwrap_or_default(),
            "data": e.field("data").unwrap_or_default(),
            "seq": e.field("seq").unwrap_or_default(),
        });
        writeln!(w, "{line}").expect("write");
    }
    w.flush().expect("flush");
    println!("exported {} entries, {} to {}", entries.len(), entries.first().map_or("", |e| e.id.as_str()), out);
}

struct Corpus {
    members: Vec<String>,
    entries: Vec<(String, String, String)>,
}

fn read_corpus() -> Corpus {
    let path = env("HPRS_BENCH_CORPUS");
    let file = std::io::BufReader::new(std::fs::File::open(&path).expect("corpus"));
    let mut lines = file.lines();
    let header: serde_json::Value = serde_json::from_str(&lines.next().expect("header").expect("read")).expect("header");
    let members = header["members"].as_array().expect("members").iter().filter_map(|m| m.as_str().map(str::to_string)).collect();
    let entries = lines
        .map(|l| {
            let v: serde_json::Value = serde_json::from_str(&l.expect("read")).expect("line");
            let s = |k: &str| v[k].as_str().unwrap_or_default().to_string();
            (s("type"), s("data"), s("seq"))
        })
        .collect();
    Corpus { members, entries }
}

/// `data` with `_relay` set, when it is a JSON object.
fn with_relay(data: &str, relay: &str) -> String {
    match serde_json::from_str::<serde_json::Value>(data) {
        Ok(serde_json::Value::Object(mut m)) => {
            m.insert("_relay".into(), json!(relay));
            serde_json::Value::Object(m).to_string()
        }
        _ => data.to_string(),
    }
}

/// Every commit's database transactions so far, once idle backends have
/// reported (`PGSTAT_IDLE_INTERVAL` is 10 s).
async fn xact_commit(pool: &sqlx::PgPool) -> i64 {
    tokio::time::sleep(Duration::from_secs(11)).await;
    sqlx::query_scalar("SELECT xact_commit FROM pg_stat_database WHERE datname = current_database()")
        .fetch_one(pool)
        .await
        .expect("pg_stat_database")
}

#[derive(Debug)]
struct Run {
    capacity: f64,
    processed: u64,
    backlog_processed: u64,
    backlog_busy: Duration,
    by_path: [u64; 4],
    xact: i64,
    elapsed: Duration,
}

async fn run(corpus: &Corpus, mode: PublicMode, force: bool) -> Run {
    let opts = Options {
        batch_size: 500,
        window: WINDOW,
        members: corpus.members.clone(),
        mode,
        stream_prefix: "bench",
        ..Options::default()
    };
    let mut f = Fixture::new("fh_bench_throughput", opts).await.expect("HPRS_TEST_DATABASE_URL and HPRS_TEST_REDIS_URL");
    // Three copies of each entry, one per relay, `COPY_OFFSET` positions
    // apart; the group already exists, so all of them are delivered.
    let mut order: Vec<(usize, usize, usize)> = (0..corpus.entries.len())
        .flat_map(|i| (0..3).map(move |k| (3 * i + COPY_OFFSET * k, k, i)))
        .collect();
    order.sort_unstable();
    for chunk in order.chunks(1000) {
        let mut pipe = redis::pipe();
        for &(_, k, i) in chunk {
            let (ty, data, seq) = &corpus.entries[i];
            pipe.cmd("XADD")
                .arg(&f.cfg.stream_key)
                .arg("MAXLEN")
                .arg(2_000_000)
                .arg("*")
                .arg("type")
                .arg(ty)
                .arg("data")
                .arg(with_relay(data, RELAYS[k]))
                .arg("seq")
                .arg(seq)
                .ignore();
        }
        pipe.query_async::<()>(&mut f.redis).await.expect("preload");
    }
    let total = order.len() as u64;
    let before = xact_commit(&f.db.pool).await;
    let (mut c, _) = f.consumer().await;
    let force = force.then(|| hooks::arm(hooks::WRITER_FORCE_TX, "*"));
    c.startup().await.expect("startup");
    let started = Instant::now();
    assert!(c.run_until(|c| c.throughput.processed >= total, Duration::from_secs(3600)).await, "drained");
    let elapsed = started.elapsed();
    drop(force);
    {
        let rec = record(&c);
        assert!(rec.gaps.is_empty() && rec.reanchors.is_empty(), "no gaps");
    }
    let xact = xact_commit(&f.db.pool).await - before;
    let t = c.throughput();
    let run = Run {
        capacity: ratio(t.backlog_processed, micros(t.backlog_busy)) * 1e6,
        processed: t.processed,
        backlog_processed: t.backlog_processed,
        backlog_busy: t.backlog_busy,
        by_path: t.by_path,
        xact,
        elapsed,
    };
    drop(c);
    f.cleanup().await;
    run
}

fn record(c: &Consumer) -> std::sync::MutexGuard<'_, Record> {
    c.test.record.lock().expect("record")
}

/// Commits a run makes outside the op paths: fixture setup, seeding and
/// teardown. Measured at 33 in both builds (C0: 300 033 commits for 300 000
/// entries), rounded up to 40.
const SETUP_XACTS: i64 = 40;

#[tokio::test]
#[ignore = "M1: the relative bench; set HPRS_BENCH_CORPUS and run alone"]
async fn throughput_corpus() {
    let corpus = read_corpus();
    println!("corpus: {} entries, {} members", corpus.entries.len(), corpus.members.len());
    let mut gate = None;
    for (mode, label) in [(PublicMode::Off, "off"), (PublicMode::On, "community_scoped")] {
        let c0 = run(&corpus, mode, true).await;
        let c1 = run(&corpus, mode, false).await;
        for (name, r) in [("C0", &c0), ("C1", &c1)] {
            println!(
                "M1 {label} {name}: backlog capacity {:.0} entries/s ({} entries in {:.3} s busy); processed {} in {:.1} s; \
                 paths no_tx {} tx {} identity {} dead_letter {}; xact_commit delta {}",
                r.capacity,
                r.backlog_processed,
                r.backlog_busy.as_secs_f64(),
                r.processed,
                r.elapsed.as_secs_f64(),
                r.by_path[0],
                r.by_path[1],
                r.by_path[2],
                r.by_path[3],
                r.xact,
            );
        }
        let bound = i64::try_from(c1.by_path[1] + c1.by_path[2]).expect("count") + SETUP_XACTS;
        println!("M1 {label}: C1/C0 = {:.2}; xact_commit bound for C1 {bound}", c1.capacity / c0.capacity);
        if mode == PublicMode::Off {
            gate = Some((c0, c1, bound));
        }
    }
    let (c0, c1, bound) = gate.expect("off run");
    assert!(c1.xact <= bound, "C1 xact_commit {} above its bound {bound}", c1.xact);
    assert!(c1.capacity >= 9_000.0 / 1_900.0 * c0.capacity, "C1 {:.0}/s below 4.7 × C0 {:.0}/s", c1.capacity, c0.capacity);
}
