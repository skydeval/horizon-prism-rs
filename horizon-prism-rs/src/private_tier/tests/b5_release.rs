//! B-5 part (3) on the deploy build (private-threads amendment 1 §2). Every
//! test here is ignored and run by hand on the reference host, against a
//! `serve` binary built by `docker/Dockerfile`'s command:
//!
//! - **The loader** ([`load`], `b5_release_load`): one fresh database per
//!   fixture and configuration, `b5r_<fixture>_<config>`, with the base
//!   snapshot, the fixture, fresh heads for every actor, a seeded
//!   `did_cache` row for every member (amendment 1 §2.2), and the run's
//!   `app.toml`.
//! - **The feeder** ([`feed`], `b5_release_feed`): a `commit` entry for an
//!   untracked repo every 5 s, so the `bench` relay's watermark stays fresh,
//!   and every 300 s fresh heads and fresh `did_cache.fetched_at`.
//! - **The client** (`b5_release_floors`, and `b5_stale_head_chain` on the
//!   `bench-heads` build): per database, start `serve`, check the run is
//!   valid and that auth runs in full, then each case at concurrency 8 and
//!   alone, gated on the server's own `private_read_unfloored_seconds`.
//!
//! Run: `cargo build --release --locked --bin horizon-prism-rs`, and for the
//! stale-head chain the same with `--features bench-heads` into
//! `HPRS_B5_BENCH_BIN`'s target dir; then
//! `cargo test --release b5_release_floors -- --ignored --nocapture` and
//! `b5_stale_head_chain`, with `HPRS_TEST_DATABASE_URL` and
//! `HPRS_TEST_REDIS_URL` set. `serve` listens on 127.0.0.1:3030 and :9091,
//! so one run at a time.

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};

use futures::StreamExt;
use k256::ecdsa::SigningKey;
use redis::aio::ConnectionManager;

use super::b5::{
    Bulk, Case, Part3, audience_mode_row, base_snapshot, budget_of, member, percentile, ready_dids, samples,
};
use super::*;
use crate::firehose::gap;
use crate::kryphocron_glue::service_key::multikey;

/// The run's `firehose.stream_key` (the default).
const STREAM_KEY: &str = "firehose:events";
/// The bench relay's `_relay` URL, labelled `bench`, the run's only
/// private relay.
const BENCH_RELAY: &str = "wss://bench.invalid/xrpc/com.atproto.sync.subscribeRepos";
/// The feeder's repo: tracked by nothing, so its commits only advance the
/// watermark.
const FEED_REPO: &str = "did:plc:benchfeedbenchfeedbenchf";
const HTTP: &str = "http://127.0.0.1:3030";
const METRICS: &str = "http://127.0.0.1:9091/metrics";
/// A rev every seeded head is at.
const APPLIED: &str = "3kzzzzzzzzzz2";
/// How often the feeder refreshes heads and `did_cache.fetched_at`.
const REFRESH: Duration = Duration::from_secs(300);

/// The deploy build's binary: `HPRS_B5_SERVE_BIN`, else the release target.
fn serve_bin() -> PathBuf {
    std::env::var("HPRS_B5_SERVE_BIN").map_or_else(
        |_| Path::new(env!("CARGO_MANIFEST_DIR")).join("target/release/horizon-prism-rs"),
        PathBuf::from,
    )
}

/// The `bench-heads` build's binary: `HPRS_B5_BENCH_BIN`, else
/// `target/bench-heads/release`.
fn bench_bin() -> PathBuf {
    std::env::var("HPRS_B5_BENCH_BIN").map_or_else(
        |_| Path::new(env!("CARGO_MANIFEST_DIR")).join("target/bench-heads/release/horizon-prism-rs"),
        PathBuf::from,
    )
}

/// The key `did`'s tokens are signed with, and its seeded document names:
/// the readers' test keys, the stale-head authors' extra keys, and for every
/// other member a key of its own from its DID.
fn key_for(did: &str, extra: &[String]) -> SigningKey {
    match did {
        ALICE => signing_key(ALICE_KEY),
        BOB => signing_key(BOB_KEY),
        CAROL => signing_key(CAROL_KEY),
        DAVE => signing_key(DAVE_KEY),
        _ => {
            if let Some(i) = extra.iter().position(|d| d == did) {
                return signing_key(u8::try_from(i).expect("small") + EXTRA_KEYS);
            }
            let mut bytes = [7u8; 32];
            bytes[0] = 2;
            for (i, b) in did.bytes().rev().take(24).enumerate() {
                bytes[1 + i] ^= b;
            }
            SigningKey::from_slice(&bytes).expect("key")
        }
    }
}

/// The DID document of `key`'s holder, as `StubFetcher::doc` builds it.
fn document(did: &str, key: &SigningKey) -> Value {
    let compressed = key.verifying_key().to_encoded_point(true).as_bytes().to_vec();
    json!({
        "id": did,
        "verificationMethod": [{
            "id": format!("{did}#atproto"),
            "type": "Multikey",
            "controller": did,
            "publicKeyMultibase": multikey(&compressed),
        }]
    })
}

/// A TID of now.
fn tid_now() -> String {
    const ALPHABET: &[u8] = b"234567abcdefghijklmnopqrstuvwxyz";
    let micros = u64::try_from(Utc::now().timestamp_micros()).expect("after the epoch");
    let mut v = (micros << 10) | u64::from(rand::random::<u16>() & 0x3ff);
    let mut out = [0u8; 13];
    for slot in out.iter_mut().rev() {
        *slot = ALPHABET[usize::try_from(v & 31).expect("small")];
        v >>= 5;
    }
    String::from_utf8(out.to_vec()).expect("ascii")
}

/// One database of the run.
struct Loaded {
    name: String,
    title: String,
    url: String,
    pool: PgPool,
    admin: PgPool,
    cases: Vec<Case>,
    config: PathBuf,
    data_dir: PathBuf,
    /// DIDs the stale-head chain keeps stale; the feeder skips them.
    stale: Vec<String>,
    not_ready: i64,
    gap_epoch: Option<String>,
}

impl Loaded {
    async fn drop_db(self) {
        self.pool.close().await;
        let _ = sqlx::query(&format!("DROP DATABASE IF EXISTS {} WITH (FORCE)", self.name)).execute(&self.admin).await;
    }
}

/// Which database: a part (3) fixture at a cap, or the stale-head chain.
#[derive(Clone, Copy)]
enum Which {
    Fixture(Part3, u32),
    Stale { failing: bool },
}

/// The stale-head chain's 15 authors, above the base snapshot's members.
fn stale_authors() -> Vec<String> {
    (6001..=6015).map(member).collect()
}

/// The loader (amendment 1 §2.3, 1): a fresh database `b5r_<fixture>_<config>`
/// with every migration, the readers, the base snapshot and the fixture,
/// fresh heads for every actor, `did_cache` rows for every member,
/// statistics, and the run's `app.toml`. `None` without a database.
async fn load(which: Which) -> Option<Loaded> {
    let admin_url = std::env::var("HPRS_TEST_DATABASE_URL").ok()?;
    let (name, title, cap) = match which {
        Which::Fixture(f, cap) => {
            let config = if f.caps().len() > 1 { format!("cap{cap}") } else { "default".to_string() };
            let title = if f.caps().len() > 1 { format!("{}, cap {cap}", f.title()) } else { f.title().to_string() };
            (format!("b5r_{}_{config}", f.slug()), title, cap)
        }
        Which::Stale { failing } => {
            let config = if failing { "failing" } else { "answering" };
            (format!("b5r_stale_{config}"), format!("16-deep stale-head chain, {config}"), CAP)
        }
    };
    let t0 = Instant::now();
    let admin = PgPool::connect(&admin_url).await.expect("admin");
    exec(&admin, &format!("DROP DATABASE IF EXISTS {name} WITH (FORCE)")).await;
    exec(&admin, &format!("CREATE DATABASE {name}")).await;
    let url = format!("{}/{name}", admin_url.rsplit_once('/').expect("database path").0);
    let pool = PgPool::connect(&url).await.expect("connect");
    crate::db::migrations::MIGRATOR.run(&pool).await.expect("migrations");

    let extra = if matches!(which, Which::Stale { .. }) { stale_authors() } else { Vec::new() };
    let mut readers: Vec<String> = [ALICE, BOB, CAROL, DAVE].map(String::from).into();
    readers.extend(extra.iter().cloned());
    for did in &readers {
        actor(&pool, did, &format!("{}.test", &did[8..]), &did[8..]).await;
        exec(&pool, &format!("INSERT INTO members (did, source, ready_at) VALUES ('{did}', 'admin', now())")).await;
        exec(&pool, &format!("INSERT INTO scope_dids (did, reasons) VALUES ('{did}', 1)")).await;
    }
    for did in [BOB, CAROL, DAVE] {
        exec(&pool, &follow_sql(did, ALICE)).await;
    }
    base_snapshot(&pool).await;
    ready_dids(&pool, &[ALICE.into()]).await;

    let mut stale = Vec::new();
    let mut heads_file = None;
    let cases = match which {
        Which::Fixture(f, _) => f.build(&pool).await,
        Which::Stale { failing } => {
            // `b5_stale_head_chain` as S12 wrote it through ingest, written
            // directly: Alice's root under her `followers` audience, a
            // chain of 15 replies by 15 authors who follow her, and 17
            // stale DIDs on 17 hosts.
            let g = audience_mode_row(&pool, ALICE, "b5stale", "followers", &[]).await;
            for a in &extra {
                exec(&pool, &follow_sql(a, ALICE)).await;
            }
            let mut b = Bulk::new("b5h");
            let root = b.post(ALICE, &g, None);
            let mut parent = root.clone();
            for a in &extra {
                parent = b.post(a, &g, Some((&root, &parent)));
            }
            b.write(&pool).await;
            ready_dids(&pool, &extra).await;
            stale = vec![ALICE.to_string(), DAVE.to_string()];
            stale.extend(extra.iter().cloned());
            for (i, d) in stale.iter().enumerate() {
                exec(&pool, &format!("UPDATE actors SET pds_endpoint = 'https://bench-{i}.invalid' WHERE did = '{d}'")).await;
            }
            let text: String = stale
                .iter()
                .map(|d| format!("{d} {}\n", if failing && *d == extra[9] { "fail" } else { APPLIED }))
                .collect();
            let path = data_dir(&format!("b5r-heads-{name}")).join("heads.txt");
            std::fs::write(&path, text).expect("heads file");
            heads_file = Some(path);
            vec![("getPostThread at the bottom", THREAD, format!("uri={parent}"), true)]
        }
    };

    // Fresh, applied heads for every actor, readers included (§4 of the
    // parent: an upsert, so no actor is left without a row).
    exec(&pool, &format!(
        "INSERT INTO sync_state (did, applied_rev, head_rev, head_checked_at) SELECT did, '{APPLIED}', '{APPLIED}', now() FROM actors \
         ON CONFLICT (did) DO UPDATE SET applied_rev = '{APPLIED}', head_rev = '{APPLIED}', head_checked_at = now()"
    )).await;
    // A deployment past its first day: the one-time v3 re-sync is done, and
    // every tracked DID's daily `authz` is queued for later, so `serve`
    // starts no backfill (each would fail against plc.invalid and write to
    // the database during the measurements).
    exec(&pool, "INSERT INTO worker_state (key, value) VALUES ('v3_collections_resync', '{\"phase\": \"done\"}'::jsonb) ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value").await;
    exec(&pool, "INSERT INTO backfill_jobs (did, depth, state, not_before) \
                 SELECT did, d, 'queued', now() + interval '30 days' FROM scope_dids, unnest(ARRAY['authz', 'resync']) d \
                 ON CONFLICT (did, depth) DO NOTHING").await;
    // Every member's DID document (amendment 1 §2.2).
    let dids: Vec<String> = sqlx::query_scalar("SELECT did FROM members").fetch_all(&pool).await.expect("members");
    let docs: Vec<Value> = dids.iter().map(|d| document(d, &key_for(d, &extra))).collect();
    sqlx::query(
        "INSERT INTO did_cache (did, doc, fetched_at, stale_at, expires_at) \
         SELECT d, doc, now(), now() + interval '7 days', now() + interval '8 days' FROM unnest($1::text[], $2::jsonb[]) AS t(d, doc)",
    )
    .bind(&dids)
    .bind(&docs)
    .execute(&pool)
    .await
    .expect("did_cache");
    exec(&pool, "ANALYZE").await;
    let not_ready: i64 = sqlx::query_scalar("SELECT count(*) FROM members WHERE ready_at IS NULL").fetch_one(&pool).await.expect("count");
    let gap_epoch = gap_epoch(&pool).await;

    let data_dir = data_dir(&format!("b5r-{name}"));
    let config = data_dir.join("app.toml");
    std::fs::write(&config, app_toml(cap, heads_file.as_deref())).expect("app.toml");
    println!("B-5 (3) loaded {name} ({title}): {} members, in {:?}", dids.len(), t0.elapsed());
    Some(Loaded {
        name,
        title,
        url,
        pool,
        admin,
        cases,
        config,
        data_dir,
        stale,
        not_ready,
        gap_epoch,
    })
}

async fn gap_epoch(pool: &PgPool) -> Option<String> {
    sqlx::query_scalar("SELECT value #>> '{}' FROM worker_state WHERE key = $1")
        .bind(gap::KEY_GAP_EPOCH)
        .fetch_optional(pool)
        .await
        .expect("gap_epoch")
}

/// The run's `app.toml` (amendment 1 §2.3): no PLC, heads up to 600 s,
/// rate limits above the load, the `bench` relay, the default floors of 0
/// so the client's samples are not padded (the gate reads the server's
/// unfloored histogram), and for the stale-head chain its heads file.
fn app_toml(cap: u32, heads_file: Option<&Path>) -> String {
    let mut toml = format!(
        "[http]\nbind = \"127.0.0.1:3030\"\n\n\
         [metrics]\nbind = \"127.0.0.1:9091\"\n\n\
         [identity]\nplc_url = \"https://plc.invalid\"\n\n\
         [sync]\nhead_max_age_secs = 600\n\n\
         [private_read]\nmax_chain = {cap}\n\n\
         [firehose]\nstream_key = \"{STREAM_KEY}\"\nprivate_relays = [\"bench\"]\n\n\
         [firehose.relay_labels]\n\"{BENCH_RELAY}\" = \"bench\"\n\n\
         [rate_limits]\nprivate = {{ per_sec = 10000.0, burst = 20000 }}\nglobal = {{ per_sec = 10000.0, burst = 20000 }}\n\n\
         [auth]\nverify_rate_per_sec = 10000.0\nverify_burst = 20000\nmember_verify_rate_per_sec = 10000.0\nmember_verify_burst = 20000\n"
    );
    if let Some(path) = heads_file {
        toml.push_str(&format!("\n[bench]\nheads_file = \"{}\"\n", path.display()));
    }
    toml
}

/// The feeder (amendment 1 §2.3, 2), until `stop`: a commit entry for an
/// untracked repo every 5 s, and every 300 s fresh heads for every actor
/// outside `stale` and fresh `did_cache.fetched_at` for every member.
async fn feed(pool: PgPool, redis_url: String, stale: Vec<String>, stop: Arc<AtomicBool>) {
    let mut redis = ConnectionManager::new(redis::Client::open(redis_url).expect("redis url")).await.expect("redis");
    let mut last_refresh = Instant::now();
    let mut seq = 0u64;
    while !stop.load(Ordering::SeqCst) {
        seq += 1;
        let data = json!({"repo": FEED_REPO, "rev": tid_now(), "ops": [], "time": Utc::now().to_rfc3339_opts(SecondsFormat::Millis, true), "_relay": BENCH_RELAY});
        let _: String = redis::cmd("XADD")
            .arg(STREAM_KEY)
            .arg("*")
            .arg("type")
            .arg("commit")
            .arg("seq")
            .arg(seq)
            .arg("data")
            .arg(data.to_string())
            .query_async(&mut redis)
            .await
            .expect("xadd");
        if last_refresh.elapsed() >= REFRESH {
            refresh(&pool, &stale).await;
            last_refresh = Instant::now();
        }
        for _ in 0..10 {
            if stop.load(Ordering::SeqCst) {
                return;
            }
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
    }
}

async fn refresh(pool: &PgPool, stale: &[String]) {
    sqlx::query("UPDATE sync_state SET head_checked_at = now() WHERE NOT (did = ANY($1))")
        .bind(stale)
        .execute(pool)
        .await
        .expect("refresh heads");
    exec(pool, "UPDATE did_cache SET fetched_at = now() WHERE did IN (SELECT did FROM members)").await;
}

/// A running `serve`, killed on drop.
struct Serve {
    child: tokio::process::Child,
    log: PathBuf,
}

impl Serve {
    async fn start(bin: &Path, db: &Loaded, redis_url: &str) -> Self {
        let log = db.data_dir.join("serve.log");
        let out = std::fs::File::create(&log).expect("log");
        let err = out.try_clone().expect("log");
        let key = |b: u8| format!("{b:02x}").repeat(32);
        let child = tokio::process::Command::new(bin)
            .arg("serve")
            .env("APP_PUBLIC_HOSTNAME", AUDIENCE_HOST)
            .env("APP_DATABASE_URL", &db.url)
            .env("APP_REDIS_URL", redis_url)
            .env("APP_CONFIG_PATH", &db.config)
            .env("APP_DATA_DIR", &db.data_dir)
            .env("APP_SERVICE_SIGNING_KEY_HEX", key(0x17))
            .env("APP_CORRELATION_KEY_HEX", key(0x29))
            .env("APP_THREADLENS_CURSOR_KEY_HEX", key(0x3b))
            .env("APP_PRIVATE_CURSOR_KEY_HEX", key(0x4d))
            .env("APP_ALLOW_STARTUP_MIGRATE", "0")
            .env("APP_LOG_LEVEL", "warn")
            .stdout(out)
            .stderr(err)
            .kill_on_drop(true)
            .spawn()
            .unwrap_or_else(|e| panic!("start {}: {e}", bin.display()));
        Self { child, log }
    }

    fn tail(&self) -> String {
        let text = std::fs::read_to_string(&self.log).unwrap_or_default();
        let lines: Vec<&str> = text.lines().collect();
        lines[lines.len().saturating_sub(30)..].join("\n")
    }

    async fn stop(mut self) {
        let _ = self.child.kill().await;
    }
}

/// `/metrics` as text.
async fn metrics(client: &reqwest::Client) -> String {
    client.get(METRICS).send().await.expect("metrics").text().await.expect("metrics body")
}

/// The sum of every series `name` whose labels include one of `any` (or
/// every series, when `any` is empty); 0 when there is none.
fn sum_of(text: &str, name: &str, any: &[&str]) -> f64 {
    text.lines()
        .filter(|l| l.strip_prefix(name).is_some_and(|rest| rest.starts_with('{') || rest.starts_with(' ')))
        .filter(|l| any.is_empty() || any.iter().any(|a| l.contains(a)))
        .filter_map(|l| l.rsplit(' ').next().and_then(|v| v.parse::<f64>().ok()))
        .sum()
}

/// `endpoint`'s cumulative buckets of `private_read_unfloored_seconds`:
/// `(le, count)`, ascending, `+Inf` last.
fn buckets(text: &str, endpoint: &str) -> Vec<(f64, f64)> {
    let prefix = format!("{}_bucket{{", super::super::handlers::METRIC_UNFLOORED);
    let label = format!("endpoint=\"{endpoint}\"");
    let mut out: Vec<(f64, f64)> = text
        .lines()
        .filter(|l| l.starts_with(&prefix) && l.contains(&label))
        .filter_map(|l| {
            let le = l.split("le=\"").nth(1)?.split('"').next()?;
            let le = if le == "+Inf" { f64::INFINITY } else { le.parse().ok()? };
            Some((le, l.rsplit(' ').next()?.parse().ok()?))
        })
        .collect();
    out.sort_by(|a, b| a.0.total_cmp(&b.0));
    out
}

/// The p99 of the observations between two bucket readings, interpolated
/// within its bucket as Prometheus' `histogram_quantile` does, with the
/// bucket's bounds; `None` with no observation.
fn server_p99(before: &[(f64, f64)], after: &[(f64, f64)]) -> Option<(Duration, f64, f64)> {
    let diff: Vec<(f64, f64)> = after
        .iter()
        .map(|(le, n)| (*le, n - before.iter().find(|(l, _)| l == le).map_or(0.0, |(_, m)| *m)))
        .collect();
    let total = diff.last()?.1;
    if total <= 0.0 {
        return None;
    }
    let target = total * 0.99;
    let (mut lower, mut below) = (0.0, 0.0);
    for (le, cum) in &diff {
        if *cum >= target {
            if le.is_infinite() {
                return Some((Duration::from_secs_f64(lower), lower, *le));
            }
            let at = lower + (le - lower) * (target - below) / (cum - below).max(f64::MIN_POSITIVE);
            return Some((Duration::from_secs_f64(at), lower, *le));
        }
        (lower, below) = (*le, *cum);
    }
    None
}

/// GET with a token for `did` signed by `key`, minted for this service and
/// `method`: status, body and the client's time.
async fn get_as(client: &reqwest::Client, method: &str, query: &str, did: &str, key: &SigningKey) -> (u16, String, Duration) {
    let token = mint(key, did, &format!("did:web:{AUDIENCE_HOST}"), Some(method));
    let start = Instant::now();
    let r = client
        .get(format!("{HTTP}/xrpc/{method}?{query}"))
        .header("authorization", format!("Bearer {token}"))
        .send()
        .await
        .expect("request");
    let status = r.status().as_u16();
    let body = r.text().await.expect("body");
    (status, body, start.elapsed())
}

async fn sample_as(client: &reqwest::Client, method: &str, query: &str, n: usize, concurrency: usize) -> Vec<Duration> {
    let key = signing_key(DAVE_KEY);
    let mut took: Vec<Duration> = futures::stream::iter(0..n)
        .map(|_| {
            let key = key.clone();
            async move {
                let (status, body, t) = get_as(client, method, query, DAVE, &key).await;
                assert_eq!(status, 200, "{method}?{query}: {body}");
                t
            }
        })
        .buffer_unordered(concurrency)
        .collect()
        .await;
    took.sort();
    took
}

fn ms(d: Duration) -> f64 {
    d.as_secs_f64() * 1000.0
}

/// The run's validity (amendment 1 §2.3): no trim gap, the loader's count
/// of unready members and `gap_epoch`, no recreation gap owed.
async fn validity(db: &Loaded, text: &str, when: &str) -> Vec<String> {
    let mut out = Vec::new();
    let gaps = sum_of(text, gap::METRIC_GAPS, &[]);
    if gaps != 0.0 {
        out.push(format!("{}: {gaps} trim gaps {when}", db.title));
    }
    let not_ready: i64 = sqlx::query_scalar("SELECT count(*) FROM members WHERE ready_at IS NULL").fetch_one(&db.pool).await.expect("count");
    if not_ready != db.not_ready {
        out.push(format!("{}: {not_ready} unready members {when}, the loader's {}", db.title, db.not_ready));
    }
    let epoch = gap_epoch(&db.pool).await;
    if epoch != db.gap_epoch {
        out.push(format!("{}: gap_epoch {epoch:?} {when}, the loader's {:?}", db.title, db.gap_epoch));
    }
    if gap::recreation_gap_owed(&db.pool).await.expect("owed") {
        out.push(format!("{}: a recreation gap is owed {when}", db.title));
    }
    out
}

const HEAD_FETCHES: [&str; 3] = ["result=\"read_fetch\"", "result=\"error\"", "result=\"read_budget_exhausted\""];
const FETCH_FAILURES: &str = "jwt_fetch_cell_failures_total";

/// The client (amendment 1 §2.3, 3) over one loaded database: its
/// failures, or why the run is void.
#[allow(clippy::too_many_lines)]
async fn run(db: &Loaded, bin: &Path, concurrency: usize, samples_c8: usize) -> Vec<String> {
    let redis_url = std::env::var("HPRS_TEST_REDIS_URL").expect("HPRS_TEST_REDIS_URL");
    let mut redis = ConnectionManager::new(redis::Client::open(redis_url.clone()).expect("url")).await.expect("redis");
    let _: () = redis::cmd("FLUSHALL").query_async(&mut redis).await.expect("flush");
    let stop = Arc::new(AtomicBool::new(false));
    let feeder = tokio::spawn(feed(db.pool.clone(), redis_url.clone(), db.stale.clone(), Arc::clone(&stop)));
    let serve = Serve::start(bin, db, &redis_url).await;
    let client = reqwest::Client::new();
    let mut failures = Vec::new();

    // Wait for the bench watermark: the group starts at the stream's tail,
    // so the feeder's next entry sets it.
    let t0 = Instant::now();
    loop {
        let text = client.get(METRICS).send().await.ok().map(|r| r.text());
        let fresh = match text {
            Some(body) => body.await.ok().and_then(|t| {
                t.lines()
                    .find(|l| l.starts_with("firehose_worker_delay_seconds{") && l.contains("relay=\"bench\""))
                    .and_then(|l| l.rsplit(' ').next().and_then(|v| v.parse::<f64>().ok()))
            }),
            None => None,
        };
        if fresh.is_some_and(|d| d < 30.0) {
            break;
        }
        assert!(t0.elapsed() < Duration::from_secs(120), "{}: no fresh bench watermark in 120 s; serve log:\n{}", db.title, serve.tail());
        tokio::time::sleep(Duration::from_millis(250)).await;
    }

    let text = metrics(&client).await;
    let void = validity(db, &text, "at start").await;
    if !void.is_empty() {
        stop.store(true, Ordering::SeqCst);
        serve.stop().await;
        let _ = feeder.await;
        return void.into_iter().map(|v| format!("void: {v}")).collect();
    }
    let heads_before = sum_of(&text, "sync_head_check_total", &HEAD_FETCHES);

    // Auth (amendment 1 §2.3, replacing T-61): the reader's first request
    // verifies from the seeded row, with no fetch.
    let (label, method, query, _) = &db.cases[0];
    let (status, body, _) = get_as(&client, method, query, DAVE, &signing_key(DAVE_KEY)).await;
    let after_first = sum_of(&metrics(&client).await, FETCH_FAILURES, &[]);
    if status != 200 || after_first != 0.0 {
        failures.push(format!("{}: auth, the reader's first request ({label}) answered {status} with {after_first} fetch failures: {body}", db.title));
    }
    // A token for the reader's DID signed by another key: 401, and exactly
    // one failed fetch (the rotation retry against plc.invalid).
    let (status, _, _) = get_as(&client, method, query, DAVE, &signing_key(99)).await;
    let after_wrong = sum_of(&metrics(&client).await, FETCH_FAILURES, &[]);
    let expected_failures = match status {
        401 if after_wrong == 1.0 => {
            println!("  B-5 (3) {:<52} auth: first request 200 with 0 fetch failures; wrong key 401 with 1", db.title);
            1.0
        }
        429 => {
            println!("  B-5 (3) {:<52} auth: the wrong-key check could not run (429)", db.title);
            after_wrong
        }
        other => {
            failures.push(format!("{}: auth, the wrong-key request answered {other} with {after_wrong} fetch failures", db.title));
            after_wrong
        }
    };

    for (label, method, query, gated) in &db.cases {
        let endpoint = method.rsplit('.').next().expect("method");
        let budget = budget_of(method);
        let before = buckets(&metrics(&client).await, endpoint);
        let (took, alone) = if db.stale.is_empty() {
            let took = sample_as(&client, method, query, samples_c8, concurrency).await;
            let after = metrics(&client).await;
            let alone = sample_as(&client, method, query, samples_c8.min(100), 1).await;
            (took, (after, alone))
        } else {
            // The stale-head chain: one at a time, every stale head reset to
            // the epoch before each sample.
            let mut took = Vec::new();
            let key = signing_key(DAVE_KEY);
            for _ in 0..samples_c8 {
                sqlx::query("UPDATE sync_state SET head_checked_at = to_timestamp(0) WHERE did = ANY($1)")
                    .bind(&db.stale)
                    .execute(&db.pool)
                    .await
                    .expect("stale");
                let (status, body, t) = get_as(&client, method, query, DAVE, &key).await;
                assert_eq!(status, 200, "{body}");
                took.push(t);
            }
            took.sort();
            let after = metrics(&client).await;
            let alone = took.clone();
            (took, (after, alone))
        };
        let (after, alone) = alone;
        let after = buckets(&after, endpoint);
        let client_p99 = percentile(&took, 99);
        let Some((p99, lower, upper)) = server_p99(&before, &after) else {
            failures.push(format!("{}, {label}: no server observations", db.title));
            continue;
        };
        // A client p99 more than a bucket above the server's is the
        // harness's own overhead.
        let next_bound = after.iter().map(|(le, _)| *le).find(|le| *le > upper).unwrap_or(f64::INFINITY);
        let overhead = client_p99.as_secs_f64() > next_bound;
        println!(
            "  B-5 (3) {:<52} {label:<36} alone p50 {:>6.1} ms  c{concurrency} p50 {:>7.1} ms  server p99 {:>7.1} ms [{:.0}, {:.0}]  client p99 {:>7.1} ms{}  budget {:>5} ms  {}",
            db.title,
            ms(percentile(&alone, 50)),
            ms(percentile(&took, 50)),
            ms(p99),
            lower * 1000.0,
            upper * 1000.0,
            ms(client_p99),
            if overhead { " (harness overhead)" } else { "" },
            budget.as_millis(),
            match (gated, p99 < budget) {
                (false, _) => "reported",
                (true, true) => "ok",
                (true, false) => "OVER",
            }
        );
        if *gated && p99 >= budget {
            failures.push(format!("{}, {label}: server p99 {p99:?} over its budget {budget:?}", db.title));
        }
    }

    let text = metrics(&client).await;
    let heads_after = sum_of(&text, "sync_head_check_total", &HEAD_FETCHES);
    if db.stale.is_empty() && heads_after != heads_before {
        failures.push(format!("{}: {} head fetches in the bench regime", db.title, heads_after - heads_before));
    }
    let fetch_failures = sum_of(&text, FETCH_FAILURES, &[]);
    if fetch_failures != expected_failures {
        failures.push(format!("{}: {fetch_failures} JWT fetch failures at the end, {expected_failures} after the auth check", db.title));
    }
    failures.extend(validity(db, &text, "at the end").await.into_iter().map(|v| format!("void: {v}")));
    let failed_jobs: i64 = sqlx::query_scalar("SELECT count(*) FROM backfill_jobs WHERE last_error IS NOT NULL")
        .fetch_one(&db.pool)
        .await
        .expect("jobs");
    println!("  B-5 (3) {:<52} backfill jobs that ran and failed during the run: {failed_jobs}", db.title);
    stop.store(true, Ordering::SeqCst);
    serve.stop().await;
    let _ = feeder.await;
    failures
}

/// The loader alone: every database of the run, left in place, with each
/// one's `app.toml` path printed.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "B-5 part (3) loader; run by hand"]
async fn b5_release_load() {
    for fixture in Part3::ALL {
        for cap in fixture.caps() {
            let Some(db) = load(Which::Fixture(fixture, *cap)).await else {
                return;
            };
            println!("  {} -> {}", db.name, db.config.display());
        }
    }
    for failing in [false, true] {
        let Some(db) = load(Which::Stale { failing }).await else {
            return;
        };
        println!("  {} -> {}", db.name, db.config.display());
    }
}

/// The feeder alone, against `HPRS_B5_FEED_DATABASE` (a database the loader
/// made) for `HPRS_B5_FEED_SECS` (default 3 600).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "B-5 part (3) feeder; run by hand beside serve"]
async fn b5_release_feed() {
    let (Ok(admin_url), Ok(redis_url), Ok(name)) =
        (std::env::var("HPRS_TEST_DATABASE_URL"), std::env::var("HPRS_TEST_REDIS_URL"), std::env::var("HPRS_B5_FEED_DATABASE"))
    else {
        return;
    };
    let secs = std::env::var("HPRS_B5_FEED_SECS").ok().and_then(|s| s.parse().ok()).unwrap_or(3600);
    let pool = PgPool::connect(&format!("{}/{name}", admin_url.rsplit_once('/').expect("path").0)).await.expect("connect");
    let stale: Vec<String> = if name.starts_with("b5r_stale") {
        let mut s = vec![ALICE.to_string(), DAVE.to_string()];
        s.extend(stale_authors());
        s
    } else {
        Vec::new()
    };
    let stop = Arc::new(AtomicBool::new(false));
    let feeder = tokio::spawn(feed(pool, redis_url, stale, Arc::clone(&stop)));
    tokio::time::sleep(Duration::from_secs(secs)).await;
    stop.store(true, Ordering::SeqCst);
    feeder.await.expect("feeder");
}

/// Part (3) on the deploy build: every fixture of §2.4 but the stale-head
/// chain, each in its own fresh database, 1 000 samples (`HPRS_B5_SAMPLES`)
/// at concurrency 8 and 100 alone, gated on the server's histogram.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore = "B-5 part (3) on the deploy build; run with --release --ignored --nocapture"]
async fn b5_release_floors() {
    let bin = serve_bin();
    assert!(bin.exists(), "no deploy build at {}", bin.display());
    let only = std::env::var("HPRS_B5_ONLY").ok();
    println!("B-5 part (3) on {}: {} samples per case at concurrency 8, 100 alone", bin.display(), samples());
    let mut failures = Vec::new();
    for fixture in Part3::ALL {
        if only.as_deref().is_some_and(|o| !o.split(',').any(|s| s == fixture.slug())) {
            continue;
        }
        for cap in fixture.caps() {
            let Some(db) = load(Which::Fixture(fixture, *cap)).await else {
                return;
            };
            failures.extend(run(&db, &bin, 8, samples()).await);
            db.drop_db().await;
        }
    }
    assert!(failures.is_empty(), "{} failing cases:\n{}", failures.len(), failures.join("\n"));
}

/// The stale-head chain (§4 part (3); amendment 1 §2.3) on the
/// `bench-heads` build: Alice's root and a chain of 15 replies by 15
/// authors, 17 DIDs against a budget of 16, every head stale before each
/// sample and answered by `BenchHeadSource` after 450 ms. `getPostThread` at
/// the bottom, at concurrency 1, under its 1 200 ms budget; answering, and
/// with parent 5's author failing. `HPRS_B5_STALE_SAMPLES` samples (default
/// `HPRS_B5_SAMPLES`).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "B-5 stale-head chain on the bench-heads build; run with --release --ignored --nocapture"]
async fn b5_stale_head_chain() {
    let bin = bench_bin();
    assert!(bin.exists(), "no bench-heads build at {}", bin.display());
    let n = std::env::var("HPRS_B5_STALE_SAMPLES").ok().and_then(|s| s.parse().ok()).unwrap_or_else(samples);
    let mut failures = Vec::new();
    for failing in [false, true] {
        let Some(db) = load(Which::Stale { failing }).await else {
            return;
        };
        failures.extend(run(&db, &bin, 1, n).await);
        db.drop_db().await;
    }
    assert!(failures.is_empty(), "{} failing cases:\n{}", failures.len(), failures.join("\n"));
}

/// Diagnosis: the full `EXPLAIN (ANALYZE, BUFFERS)` of the statements behind
/// part (3)'s slow cases, each on its fixture's freshly loaded database:
/// the unread count (distinct parents), the timeline's first batch (V
/// outside G, and posts that fail the prefilter), the reply counts of the
/// node flood, and the quote counts of X. `HPRS_B5_ONLY` picks fixtures.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "B-5 part (3) plan diagnosis; run by hand"]
async fn b5_release_plans() {
    let only = std::env::var("HPRS_B5_ONLY").ok();
    let none: Option<DateTime<Utc>> = None;
    for fixture in [Part3::Distinct, Part3::Outside, Part3::PrefilterFails, Part3::Floods, Part3::FailingQuotes] {
        if only.as_deref().is_some_and(|o| !o.split(',').any(|s| s == fixture.slug())) {
            continue;
        }
        let Some(db) = load(Which::Fixture(fixture, CAP)).await else {
            return;
        };
        // The URIs of the fixture's first getPosts case.
        let uris: Vec<String> = db
            .cases
            .iter()
            .find(|c| c.1 == POSTS)
            .map(|c| c.2.split('&').filter_map(|p| p.strip_prefix("uris=")).map(String::from).collect())
            .unwrap_or_default();
        let (what, sql) = match fixture {
            Part3::Distinct => ("unread", schema::unread_statement(CAP)),
            Part3::Floods => ("reply counts", schema::reply_counts_statement(CAP)),
            Part3::FailingQuotes => ("quote counts", schema::quote_counts_statement(CAP)),
            _ => ("timeline", schema::timeline_statement(CAP)),
        };
        let text = format!("EXPLAIN (ANALYZE, BUFFERS) {sql}");
        let q = sqlx::query_scalar::<_, String>(&text).bind(DAVE);
        let q = match what {
            "unread" => q,
            "timeline" => q.bind(none).bind("").bind(51i64),
            _ => q.bind(&uris),
        };
        let plan: Vec<String> = q.fetch_all(&db.pool).await.expect("explain");
        println!("==== {}, {what}: {}\n{}", fixture.title(), plan.last().cloned().unwrap_or_default(), plan.join("\n"));
        db.drop_db().await;
    }
}
