//! `admin indexing full-check`: advice on whether this machine keeps up
//! with the full network, before anyone switches to it.
//!
//! It builds a scratch database beside the configured one (dropped at the
//! end), seeds it with tables of a stated size, and applies a stated number
//! of commits through the writer with full admission and every trigger on,
//! in a mix like the network's (mostly likes, then reposts, posts and
//! follows, some deletes and blocks), a share of them on one viral post and
//! one much-followed account. It does so at each lane count asked for, and
//! times one range of the counts repair full uses. It compares the apply
//! rates with the relays' rate, read from the stream's own counter, less
//! the copies dedupe would drop, estimated from the stream's newest
//! entries. Nothing in the configured database or the stream changes.

use std::collections::HashSet;
use std::fmt::Write as _;
use std::sync::Arc;
use std::time::{Duration, Instant};

use chrono::Utc;
use serde_json::json;
use sqlx::PgPool;

use crate::aggregations::{ScopeSets, ScopeState};
use crate::config::{AppConfig, FullGroup, ScopePolicy};
use crate::firehose::consumer::{CONFLICT_RETRIES, lane_of};
use crate::firehose::stream::{Event, decode, parse_entry};
use crate::firehose::tier::CopyTier;
use crate::firehose::writer::{Action, ApplyOptions, LiveCommit, RawOp, WriteSource, Writer};
use crate::indexing::{Breadth, PublicMode, PublicState};

/// What to seed and apply.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Params {
    pub accounts: u32,
    pub posts: u32,
    pub likes: u32,
    pub follows: u32,
    /// Commits applied per lane count.
    pub commits: u32,
    pub lanes: Vec<u32>,
    /// Commits per writer transaction.
    pub lane_batch: u32,
    /// The bulk arm writes quiet likes, reposts and follows.
    pub bulk_arm: bool,
    /// How long the relays' rate is sampled for.
    pub sample_secs: u32,
    /// Keep the scratch database for a look afterwards.
    pub keep: bool,
}

impl Default for Params {
    fn default() -> Self {
        Self { accounts: 200_000, posts: 500_000, likes: 2_000_000, follows: 500_000, commits: 20_000, lanes: vec![1, 2, 4, 8], lane_batch: 64, bulk_arm: true, sample_secs: 10, keep: false }
    }
}

/// One lane count's run.
#[derive(Debug, Clone, PartialEq)]
pub struct LaneRun {
    pub lanes: u32,
    pub commits: u32,
    pub secs: f64,
    pub per_sec: f64,
    pub retries: u64,
    /// Lane time per commit in each writer phase, in seconds.
    pub phases: Vec<(String, f64)>,
    /// What the scratch database's sessions waited on, sampled.
    pub waits: Vec<(crate::db::waits::WaitClass, u64)>,
    /// The counts folder beside the lanes: what it folded.
    pub counts: Option<FolderRun>,
}

/// The counts folder during one run.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct FolderRun {
    /// Log rows folded during the run, a second.
    pub per_sec: f64,
    /// How far counts were behind, the 99th percentile of samples every
    /// 100 ms, in seconds.
    pub behind_p99: f64,
    /// The settle after the run: its time, and the rows it folded.
    pub settle_secs: f64,
    pub settled_rows: i64,
}

/// The 99th percentile of `v`.
fn p99(mut v: Vec<f64>) -> f64 {
    if v.is_empty() {
        return 0.0;
    }
    v.sort_by(f64::total_cmp);
    #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss, clippy::cast_precision_loss)]
    let i = ((v.len() as f64 * 0.99).ceil() as usize).clamp(1, v.len()) - 1;
    v[i]
}

/// The CLI process's metrics, read back for the profile; none is installed
/// before the check, so it installs its own.
fn recorder() -> Option<metrics_exporter_prometheus::PrometheusHandle> {
    static HANDLE: std::sync::OnceLock<Option<metrics_exporter_prometheus::PrometheusHandle>> = std::sync::OnceLock::new();
    HANDLE
        .get_or_init(|| {
            let recorder = metrics_exporter_prometheus::PrometheusBuilder::new().build_recorder();
            let handle = recorder.handle();
            metrics::set_global_recorder(recorder).ok().map(|()| handle)
        })
        .clone()
}

/// Each writer phase's seconds so far.
fn phase_sums(handle: Option<&metrics_exporter_prometheus::PrometheusHandle>) -> std::collections::BTreeMap<String, f64> {
    let Some(h) = handle else {
        return std::collections::BTreeMap::new();
    };
    let name = format!("{}_sum", crate::firehose::writer::METRIC_PHASE);
    crate::admin::health::parse(&h.render(), |n| n == name)
        .into_iter()
        .filter_map(|s| Some((s.label("phase")?.to_string(), s.value)))
        .collect()
}

/// The relays' input, from the stream.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Input {
    /// Entries a second, every relay summed.
    pub per_sec: f64,
    /// Of the newest entries' commits, the share dedupe would keep.
    pub unique_share: f64,
    /// Entries in that sample.
    pub sampled: usize,
}

/// A seeded account's DID: its number in hex, the digits mapped to base32
/// letters (the SQL that seeds them does the same).
#[must_use]
pub fn seeded_did(n: u64) -> String {
    let hex = format!("{n:024x}");
    let mapped: String = hex.chars().map(|c| if c.is_ascii_digit() { char::from(b'q' + (c as u8 - b'0')) } else { c }).collect();
    format!("did:plc:{mapped}")
}

/// Run the check; its report in words.
///
/// # Errors
///
/// A database or Redis failure, said in words.
pub async fn run(cfg: &AppConfig, params: &Params) -> Result<String, String> {
    let mut out = String::new();
    let input = match relay_input(cfg, params.sample_secs).await {
        Ok(i) => Some(i),
        Err(e) => {
            let _ = writeln!(out, "The relays' rate could not be read from the stream ({e}); the apply rates below stand alone.");
            None
        }
    };
    let admin = crate::db::connect(cfg).await.map_err(|e| format!("database connect: {e}"))?;
    let current: String = sqlx::query_scalar("SELECT current_database()::text").fetch_one(&admin).await.map_err(|e| e.to_string())?;
    let scratch = format!("{current}_full_check");
    for sql in [format!("DROP DATABASE IF EXISTS {scratch} WITH (FORCE)"), format!("CREATE DATABASE {scratch}")] {
        sqlx::query(&sql)
            .execute(&admin)
            .await
            .map_err(|e| format!("making the scratch database {scratch} failed ({e}); full-check needs the database user to be allowed to create a database"))?;
    }
    let result = check_in(cfg, &scratch, params, input).await;
    if !params.keep {
        let _ = sqlx::query(&format!("DROP DATABASE IF EXISTS {scratch} WITH (FORCE)")).execute(&admin).await;
    }
    let (report, last) = result?;
    // The last result, for Health and full's confirm page.
    if let Err(e) = sqlx::query(
        "INSERT INTO worker_state (key, value, updated_at) VALUES ($1, $2, now()) \
         ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value, updated_at = now()",
    )
    .bind(LAST_KEY)
    .bind(&last)
    .execute(&admin)
    .await
    {
        let _ = writeln!(out, "The result could not be kept for Health ({e}).");
    }
    out.push_str(&report);
    for line in crate::indexing::full::postgres_warnings(&admin).await {
        let _ = writeln!(out, "{line}");
    }
    if params.keep {
        let _ = writeln!(out, "The scratch database {scratch} was kept.");
    }
    Ok(out)
}

async fn check_in(cfg: &AppConfig, scratch: &str, params: &Params, input: Option<Input>) -> Result<(String, serde_json::Value), String> {
    let options = crate::db::pool::connect_options(cfg.env.database_url.expose(), cfg.file.db.lock_timeout_secs)
        .map_err(|e| e.to_string())?
        .database(scratch);
    let pool = crate::db::pool::pool_options().connect_with(options).await.map_err(|e| format!("scratch database connect: {e}"))?;
    crate::db::migrations::MIGRATOR.run(&pool).await.map_err(|e| format!("scratch database migrations: {e}"))?;
    let seeded = Instant::now();
    seed(&pool, params).await.map_err(|e| format!("seeding: {e}"))?;
    let seed_secs = seeded.elapsed().as_secs_f64();
    // The seed's counts, folded before the runs: one large settle.
    let settling = Instant::now();
    let seed_fold = crate::counts::settle(&pool).await.map_err(|e| format!("folding the seed's counts: {e}"))?;
    let seed_fold = (seed_fold, settling.elapsed());
    let writer = full_writer(&pool).await.with_bulk_arm(params.bulk_arm);
    let mut runs = Vec::new();
    let handle = recorder();
    for (i, lanes) in params.lanes.iter().copied().filter(|l| *l > 0).enumerate() {
        let commits = workload(params, u64::try_from(i).unwrap_or(0));
        let before = phase_sums(handle.as_ref());
        // What the database waits on, sampled while the run goes.
        let tally = std::sync::Arc::new(crate::db::waits::WaitTally::default());
        // The counts folder runs beside the lanes, as in serve.
        let status = std::sync::Arc::new(crate::counts::FoldStatus::new(Duration::from_millis(cfg.file.counts.fold_interval_ms)));
        let folder = crate::counts::Folder::new(pool.clone(), &cfg.file.counts, std::sync::Arc::clone(&status)).spawn();
        let behind = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
        let sampling = {
            let (pool, tally, status, behind) = (pool.clone(), std::sync::Arc::clone(&tally), std::sync::Arc::clone(&status), std::sync::Arc::clone(&behind));
            tokio::spawn(async move {
                loop {
                    let _ = crate::db::waits::sample(&pool, &tally).await;
                    if let Some(b) = status.view().behind {
                        behind.lock().unwrap_or_else(std::sync::PoisonError::into_inner).push(b.as_secs_f64());
                    }
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
            })
        };
        let mut run = apply(&writer, commits, lanes, params.lane_batch).await;
        sampling.abort();
        folder.abort();
        let _ = folder.await;
        let settling = Instant::now();
        let settled = crate::counts::settle(&pool).await.map_err(|e| format!("folding the run's counts: {e}"))?;
        let settle_secs = settling.elapsed().as_secs_f64();
        if let Ok(run) = &mut run {
            #[allow(clippy::cast_precision_loss)]
            let per_sec = status.folded() as f64 / run.secs.max(1e-9);
            let samples = std::mem::take(&mut *behind.lock().unwrap_or_else(std::sync::PoisonError::into_inner));
            run.counts = Some(FolderRun { per_sec, behind_p99: p99(samples), settle_secs, settled_rows: settled.folded() });
            let after = phase_sums(handle.as_ref());
            let n = f64::from(run.commits.max(1));
            run.phases = crate::firehose::writer::Phase::ALL
                .iter()
                .map(|p| {
                    let k = p.as_str();
                    (k.to_string(), (after.get(k).copied().unwrap_or(0.0) - before.get(k).copied().unwrap_or(0.0)).max(0.0) / n)
                })
                .collect();
            run.waits = tally.snapshot().into_iter().collect();
        }
        runs.push(run?);
    }
    let range = crate::aggregations::rebuild::repair_range(&pool, 1, 1 + crate::aggregations::rebuild::KEYSET_RANGE)
        .await
        .map_err(|e| format!("counts repair range: {e}"))?;
    let size: i64 = sqlx::query_scalar("SELECT pg_database_size(current_database())").fetch_one(&pool).await.map_err(|e| e.to_string())?;
    pool.close().await;
    let last = last_result(input.as_ref(), &runs);
    let mut report = words(params, input, &runs, seed_secs, size, range.slowest);
    let _ = writeln!(
        report,
        "The seed's counts: {} log rows folded in one settle of {} slices, {:.1} s.",
        seed_fold.0.folded(),
        seed_fold.0.slices.max(seed_fold.0.tag_slices),
        seed_fold.1.as_secs_f64()
    );
    Ok((report, last))
}

/// The `worker_state` key of the last full-check's result.
pub const LAST_KEY: &str = "full_check_last";

/// The last result as kept: when, the rate the relays need applied, and
/// each lane count's rate.
fn last_result(input: Option<&Input>, runs: &[LaneRun]) -> serde_json::Value {
    let need = input.map(|i| i.per_sec * i.unique_share);
    let lanes: Vec<serde_json::Value> = runs.iter().map(|r| serde_json::json!({ "lanes": r.lanes, "per_sec": r.per_sec })).collect();
    let advice = need.and_then(|n| runs.iter().filter(|r| r.per_sec >= n * 1.25).map(|r| r.lanes).min());
    serde_json::json!({ "at": chrono::Utc::now().to_rfc3339(), "needed_per_sec": need, "lanes": lanes, "advised_lanes": advice })
}

/// The last full-check's result in a sentence, if one was kept.
///
/// # Errors
///
/// Database errors.
pub async fn last_words(pool: &sqlx::PgPool) -> Result<Option<String>, sqlx::Error> {
    let v: Option<serde_json::Value> = sqlx::query_scalar("SELECT value FROM worker_state WHERE key = $1").bind(LAST_KEY).fetch_optional(pool).await?;
    Ok(v.map(|v| last_sentence(&v)))
}

/// A kept result in a sentence.
#[must_use]
pub fn last_sentence(v: &serde_json::Value) -> String {
    let at = v.get("at").and_then(serde_json::Value::as_str).and_then(|t| chrono::DateTime::parse_from_rfc3339(t).ok()).map(|t| t.format("%Y-%m-%d %H:%M UTC").to_string());
    let lanes: Vec<String> = v
        .get("lanes")
        .and_then(serde_json::Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(|r| Some(format!("{} lanes {:.0}/s", r.get("lanes")?.as_u64()?, r.get("per_sec")?.as_f64()?)))
        .collect();
    let need = v.get("needed_per_sec").and_then(serde_json::Value::as_f64).map_or_else(|| "the relays' rate was not read".to_string(), |n| format!("about {n:.0} a second needed"));
    let advice = v.get("advised_lanes").and_then(serde_json::Value::as_u64).map_or_else(|| "no lane count tried kept up with headroom".to_string(), |l| format!("advice: {l} lanes or more"));
    format!("Last full-check {}: {}; {need}; {advice}.", at.unwrap_or_else(|| "at an unknown time".to_string()), if lanes.is_empty() { "no runs".to_string() } else { lanes.join(", ") })
}

/// The relays' summed rate over `secs`, and the share of the newest commit
/// entries that are not a copy of another in the sample.
async fn relay_input(cfg: &AppConfig, secs: u32) -> Result<Input, String> {
    let client = redis::Client::open(cfg.env.redis_url.as_str()).map_err(|e| e.to_string())?;
    let mut conn = redis::aio::ConnectionManager::new(client).await.map_err(|e| e.to_string())?;
    let stream = &cfg.file.firehose.stream_key;
    let added = |info: &redis::Value| -> Option<u64> {
        let redis::Value::Array(kv) = info else { return None };
        kv.chunks(2).find_map(|p| match p {
            [redis::Value::BulkString(k), redis::Value::Int(v)] if k == b"entries-added" => u64::try_from(*v).ok(),
            [redis::Value::SimpleString(k), redis::Value::Int(v)] if k == "entries-added" => u64::try_from(*v).ok(),
            _ => None,
        })
    };
    let first: redis::Value = redis::cmd("XINFO").arg("STREAM").arg(stream).query_async(&mut conn).await.map_err(|e| e.to_string())?;
    let t0 = Instant::now();
    tokio::time::sleep(Duration::from_secs(u64::from(secs.max(1)))).await;
    let second: redis::Value = redis::cmd("XINFO").arg("STREAM").arg(stream).query_async(&mut conn).await.map_err(|e| e.to_string())?;
    let (a, b) = (added(&first).ok_or("no entries-added")?, added(&second).ok_or("no entries-added")?);
    #[allow(clippy::cast_precision_loss)]
    let per_sec = b.saturating_sub(a) as f64 / t0.elapsed().as_secs_f64();
    let reply: redis::Value = redis::cmd("XREVRANGE").arg(stream).arg("+").arg("-").arg("COUNT").arg(5000).query_async(&mut conn).await.map_err(|e| e.to_string())?;
    let entries: Vec<_> = match reply {
        redis::Value::Array(items) => items.into_iter().filter_map(|v| parse_entry(v).ok()).collect(),
        _ => Vec::new(),
    };
    let (mut commits, mut seen) = (0usize, HashSet::new());
    for e in &entries {
        if let Ok(Event::Commit(c)) = decode(e) {
            commits += 1;
            seen.insert((c.repo, c.rev));
        }
    }
    #[allow(clippy::cast_precision_loss)]
    let unique_share = if commits == 0 { 1.0 } else { seen.len() as f64 / commits as f64 };
    Ok(Input { per_sec, unique_share, sampled: entries.len() })
}

/// Seed the scratch tables in SQL, triggers on, so the count tables and
/// every index are as large as the tables say.
async fn seed(pool: &PgPool, p: &Params) -> Result<(), sqlx::Error> {
    let record = format!(r#"{{"$type":"app.bsky.feed.post","text":"seed","createdAt":"{}"}}"#, Utc::now().to_rfc3339());
    sqlx::query(
        "INSERT INTO dids (did) SELECT 'did:plc:' || translate(lpad(to_hex(g), 24, '0'), '0123456789', 'qrstuvwxyz') \
         FROM generate_series(1, $1::bigint) g ON CONFLICT (did) DO NOTHING",
    )
    .bind(i64::from(p.accounts.max(2)))
    .execute(pool)
    .await?;
    // Accounts n = 1..accounts have ids base + n - 1.
    let base: i32 = sqlx::query_scalar("SELECT id FROM dids WHERE did = $1")
        .bind(seeded_did(1))
        .fetch_optional(pool)
        .await?
        .unwrap_or(1);
    let n = i64::from(p.accounts.max(2));
    sqlx::query(
        "INSERT INTO posts (author, rkey, cid, rev, created_at, record) \
         SELECT $1 + (g % $2), g, '\\x00'::bytea || convert_to('bafyseed', 'UTF8'), g, now() - make_interval(secs => g), \
                '\\x00'::bytea || convert_to($3, 'UTF8') \
         FROM generate_series(1, $4::bigint) g ON CONFLICT DO NOTHING",
    )
    .bind(base)
    .bind(n)
    .bind(&record)
    .bind(i64::from(p.posts.max(1)))
    .execute(pool)
    .await?;
    // A like of seeded post m names author base + m % n, key m.
    sqlx::query(
        "INSERT INTO likes (author, rkey, subj_author, subj_coll, subj_rkey, subj_cid, rev, created_at) \
         SELECT $1 + (g % $2), g, $1 + (m % $2), 1, m, '\\x00'::bytea || convert_to('bafyseed', 'UTF8'), g, now() - make_interval(secs => g) \
         FROM generate_series(1, $3::bigint) g, LATERAL (SELECT 1 + (g * 7919) % $4 AS m) s ON CONFLICT DO NOTHING",
    )
    .bind(base)
    .bind(n)
    .bind(i64::from(p.likes))
    .bind(i64::from(p.posts.max(1)))
    .execute(pool)
    .await?;
    sqlx::query(
        "INSERT INTO follows (author, rkey, subject, rev, created_at) \
         SELECT $1 + (g % $2), g, $1 + ((g * 31337) % $2), g, now() - make_interval(secs => g) \
         FROM generate_series(1, $3::bigint) g ON CONFLICT DO NOTHING",
    )
    .bind(base)
    .bind(n)
    .bind(i64::from(p.follows))
    .execute(pool)
    .await?;
    sqlx::query("ANALYZE").execute(pool).await?;
    Ok(())
}

/// A writer on the scratch database admitting every account, as full does.
async fn full_writer(pool: &PgPool) -> Writer {
    let mut sets = ScopeSets::default();
    sets.full.kinds = FullGroup::ALL.to_vec();
    sets.full.takedown_relays = vec!["bsky".to_string()];
    sets.full.status_relays = vec!["bsky".to_string()];
    let scope = ScopeState::new(sets, ScopePolicy::MembersAndOneHop, 10_000);
    {
        let mut guard = scope.write_guard().await;
        scope.set_public(&mut guard, PublicState { mode: PublicMode::On, breadth: Breadth::Full, generation: 1, ..PublicState::default() });
    }
    Writer::new(pool.clone(), scope, Arc::new(crate::private_tier::DisabledPrivateTier))
}

/// One synthetic commit: its repo, rev and ops.
type Synthetic = (String, String, Vec<RawOp>);

/// The commits of run `round`: the network's mix by kind (a measured relay day:
/// likes 68 %, reposts 11 %, posts 10 %, follows 6 %, deletes 4 %, blocks
/// 1 %), a fifth of the likes and reposts on one viral post and a fifth of
/// the follows of one account, authors spread over the seeded accounts.
#[must_use]
pub fn workload(p: &Params, round: u64) -> Vec<Synthetic> {
    let accounts = u64::from(p.accounts.max(2));
    let posts = u64::from(p.posts.max(1));
    let viral = format!("at://{}/app.bsky.feed.post/{}", seeded_did(1 + (1 % accounts)), crate::thread_outline::ops::tid_string(1));
    let famous = seeded_did(2);
    let start = Utc::now().timestamp_micros().max(0).unsigned_abs() << 10;
    let created = Utc::now().to_rfc3339();
    (0..u64::from(p.commits))
        .map(|i| {
            let tid = crate::thread_outline::ops::tid_string(i64::try_from(start + (round << 32) + i).unwrap_or(i64::MAX));
            let author = seeded_did(1 + (i.wrapping_mul(2_654_435_761) % accounts));
            let subject_post = |salt: u64| {
                let m = 1 + (i.wrapping_mul(salt) % posts);
                format!("at://{}/app.bsky.feed.post/{}", seeded_did(1 + m % accounts), crate::thread_outline::ops::tid_string(i64::try_from(m).unwrap_or(1)))
            };
            let op = |collection: &str, record: serde_json::Value| RawOp {
                action: Action::Create,
                collection: collection.to_string(),
                rkey: tid.clone(),
                cid: Some("bafycheck".to_string()),
                record: Some(record),
            };
            let pick = i % 100;
            let ops = match pick {
                0..=67 => {
                    let subject = if pick < 14 { viral.clone() } else { subject_post(7919) };
                    vec![op("app.bsky.feed.like", json!({"$type": "app.bsky.feed.like", "subject": {"uri": subject, "cid": "bafyseed"}, "createdAt": created}))]
                }
                68..=78 => {
                    let subject = if pick < 71 { viral.clone() } else { subject_post(104_729) };
                    vec![op("app.bsky.feed.repost", json!({"$type": "app.bsky.feed.repost", "subject": {"uri": subject, "cid": "bafyseed"}, "createdAt": created}))]
                }
                79..=88 => {
                    let mut record = json!({"$type": "app.bsky.feed.post", "text": "full-check", "createdAt": created});
                    if pick < 83 {
                        let parent = subject_post(15_485_863);
                        record["reply"] = json!({"root": {"uri": parent, "cid": "bafyseed"}, "parent": {"uri": parent, "cid": "bafyseed"}});
                    }
                    vec![op("app.bsky.feed.post", record)]
                }
                89..=94 => {
                    let subject = if pick < 91 { famous.clone() } else { seeded_did(1 + i.wrapping_mul(31) % accounts) };
                    vec![op("app.bsky.graph.follow", json!({"$type": "app.bsky.graph.follow", "subject": subject, "createdAt": created}))]
                }
                95..=98 => vec![RawOp { action: Action::Delete, collection: "app.bsky.feed.like".to_string(), rkey: crate::thread_outline::ops::tid_string(i64::try_from(i + 1).unwrap_or(1)), cid: None, record: None }],
                _ => vec![op("app.bsky.graph.block", json!({"$type": "app.bsky.graph.block", "subject": seeded_did(1 + i.wrapping_mul(17) % accounts), "createdAt": created}))],
            };
            (author, tid, ops)
        })
        .collect()
}

/// Apply `commits` with `lanes` lanes, each repo always on one lane, in
/// writer transactions of at most `batch` commits; the rate is commits over
/// wall time.
async fn apply(writer: &Writer, commits: Vec<Synthetic>, lanes: u32, batch: u32) -> Result<LaneRun, String> {
    let batch = usize::try_from(batch).unwrap_or(1).max(1);
    let n = usize::try_from(lanes).unwrap_or(1).max(1);
    let total = u32::try_from(commits.len()).unwrap_or(u32::MAX);
    let mut queues: Vec<Vec<Synthetic>> = (0..n).map(|_| Vec::new()).collect();
    for c in commits {
        queues[lane_of(&c.0, n)].push(c);
    }
    let options = ApplyOptions { advance_applied_rev: true, dead_letter_readiness: true, attempts: 5, live: true, subscription_rules: true, source: WriteSource::Firehose };
    let started = Instant::now();
    let mut tasks = Vec::with_capacity(n);
    for queue in queues {
        let writer = writer.clone();
        tasks.push(tokio::spawn(async move {
            let mut retries = 0u64;
            let items: Vec<LiveCommit<'_>> = queue
                .iter()
                .map(|(repo, rev, ops)| LiveCommit { relay: Some("bsky"), repo, rev, since: None, ops, options, tier: CopyTier::Whole, stream: None })
                .collect();
            let mut pos = 0;
            while pos < items.len() {
                let end = (pos + batch).min(items.len());
                let mut tries = 0;
                loop {
                    match writer.apply_batch(&items[pos..end], batch).await {
                        Err(t) if tries < CONFLICT_RETRIES && t.conflict().is_some() => {
                            tries += 1;
                            retries += 1;
                            tokio::time::sleep(Duration::from_millis(5 * u64::from(tries))).await;
                        }
                        Err(t) => return Err(t.to_string()),
                        Ok(reports) => {
                            pos += reports.len().max(1);
                            break;
                        }
                    }
                }
            }
            Ok(retries)
        }));
    }
    let mut retries = 0;
    for t in tasks {
        retries += t.await.map_err(|e| e.to_string())??;
    }
    let secs = started.elapsed().as_secs_f64();
    Ok(LaneRun { lanes, commits: total, secs, per_sec: f64::from(total) / secs.max(1e-9), retries, phases: Vec::new(), waits: Vec::new(), counts: None })
}

fn words(p: &Params, input: Option<Input>, runs: &[LaneRun], seed_secs: f64, size: i64, range: Duration) -> String {
    let mut out = String::new();
    #[allow(clippy::cast_precision_loss)]
    let mb = size as f64 / 1e6;
    let _ = writeln!(
        out,
        "Full check, advice only. Seeded {} accounts, {} posts, {} likes and {} follows in {seed_secs:.0} s ({mb:.0} MB), then applied {} commits per run with full admission and triggers on, up to {} in one transaction, the bulk arm {}.",
        p.accounts, p.posts, p.likes, p.follows, p.commits, p.lane_batch, if p.bulk_arm { "on" } else { "off" }
    );
    let need = input.map(|i| i.per_sec * i.unique_share);
    if let Some(i) = input {
        let _ = writeln!(
            out,
            "The relays send {:.0} entries a second in all; of the newest {} entries' commits, dedupe keeps {:.0} %, so about {:.0} a second need applying.",
            i.per_sec,
            i.sampled,
            i.unique_share * 100.0,
            i.per_sec * i.unique_share
        );
    }
    for r in runs {
        let verdict = match need {
            Some(n) if n > 0.0 && r.per_sec >= n * 1.25 => format!(": keeps up, {:.0} % headroom", (r.per_sec / n - 1.0) * 100.0),
            Some(n) if n > 0.0 && r.per_sec >= n => ": keeps up, with little headroom".to_string(),
            Some(n) if n > 0.0 => format!(": falls behind ({:.0} % of what is needed)", r.per_sec / n * 100.0),
            _ => String::new(),
        };
        let _ = writeln!(out, "  writer_lanes = {}: {:.0} commits a second ({} in {:.1} s, {} retried after a conflict){verdict}", r.lanes, r.per_sec, r.commits, r.secs, r.retries);
        if r.phases.iter().any(|(_, s)| *s > 0.0) {
            let phases: Vec<String> = r.phases.iter().filter(|(_, s)| *s > 0.0).map(|(p, s)| format!("{p} {:.2} ms", s * 1000.0)).collect();
            let _ = writeln!(out, "    lane time per commit: {}", phases.join(", "));
        }
        let total: u64 = r.waits.iter().map(|(_, n)| n).sum();
        if total > 0 {
            #[allow(clippy::cast_precision_loss)]
            let waits: Vec<String> = r.waits.iter().filter(|(_, n)| *n > 0).map(|(c, n)| format!("{} {:.0} %", c.as_str(), *n as f64 * 100.0 / total as f64)).collect();
            let _ = writeln!(out, "    database sessions: {}", waits.join(", "));
        }
        if let Some(c) = r.counts {
            let _ = writeln!(
                out,
                "    counts folder: {:.0} changes a second, behind {:.1} s at p99; the settle after the run folded {} rows in {:.2} s",
                c.per_sec, c.behind_p99, c.settled_rows, c.settle_secs
            );
        }
    }
    if let Some(n) = need {
        match runs.iter().filter(|r| r.per_sec >= n * 1.25).map(|r| r.lanes).min() {
            Some(l) => {
                let _ = writeln!(out, "Advice: writer_lanes = {l} or more. A live run is the real test: `admin indexing full-trial start`.");
            }
            None => {
                let _ = writeln!(out, "Advice: no lane count tried keeps up with headroom; full would fall behind the relays on this machine.");
            }
        }
    }
    let _ = writeln!(out, "One range of the counts repair full uses ({} accounts) took {} ms.", crate::aggregations::rebuild::KEYSET_RANGE, range.as_millis());
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn seeded_dids_are_valid_and_match_the_sql() {
        let did = seeded_did(1);
        assert_eq!(did, "did:plc:qqqqqqqqqqqqqqqqqqqqqqqr");
        assert!(proto_blue::syntax::Did::is_valid(&did), "{did}");
        assert_eq!(seeded_did(0xab), "did:plc:qqqqqqqqqqqqqqqqqqqqqqab");
    }

    #[test]
    fn the_workload_has_the_networks_mix() {
        let p = Params { commits: 1000, accounts: 100, posts: 100, ..Params::default() };
        let w = workload(&p, 0);
        assert_eq!(w.len(), 1000);
        let count = |c: &str| w.iter().filter(|(_, _, ops)| ops[0].collection == c && ops[0].action == Action::Create).count();
        assert_eq!(count("app.bsky.feed.like"), 680);
        assert_eq!(count("app.bsky.feed.repost"), 110);
        assert_eq!(count("app.bsky.feed.post"), 100);
        assert_eq!(count("app.bsky.graph.follow"), 60);
        assert_eq!(count("app.bsky.graph.block"), 10);
        let revs: HashSet<&String> = w.iter().map(|(_, rev, _)| rev).collect();
        assert_eq!(revs.len(), 1000, "every commit has its own rev");
        assert!(w.iter().all(|(_, rev, _)| proto_blue::syntax::Tid::is_valid(rev)));
        let next = workload(&p, 1);
        assert!(next.iter().zip(&w).all(|(a, b)| a.1 > b.1), "a later round writes newer records");
    }

    /// The whole check against a scratch database: it seeds, applies at two
    /// lane counts with no failure, times a repair range, and drops the
    /// scratch database.
    #[tokio::test]
    async fn the_check_runs_against_a_scratch_database() {
        let Some(db) = crate::test_support::fresh_db("full_check_scratch").await else {
            return;
        };
        let p = Params { accounts: 500, posts: 1000, likes: 3000, follows: 1000, commits: 400, lanes: vec![1, 4], lane_batch: 64, bulk_arm: true, sample_secs: 1, keep: false };
        let writer = {
            seed(&db.pool, &p).await.expect("seed");
            full_writer(&db.pool).await
        };
        let likes: i64 = sqlx::query_scalar("SELECT count(*) FROM likes").fetch_one(&db.pool).await.expect("likes");
        assert_eq!(likes, 3000);
        crate::counts::before_read(&db.pool).await;
        let counted: Option<i64> = sqlx::query_scalar("SELECT sum(like_count)::bigint FROM post_counts").fetch_one(&db.pool).await.expect("counts");
        assert!(counted.unwrap_or(0) > 0, "the triggers counted the seeded likes");
        for (i, lanes) in [1u32, 4].into_iter().enumerate() {
            let run = apply(&writer, workload(&p, i as u64), lanes, p.lane_batch).await.expect("apply");
            assert_eq!(run.commits, 400);
            assert!(run.per_sec > 0.0);
        }
        let viral: i64 = sqlx::query_scalar("SELECT count(*) FROM likes WHERE subj_author = (SELECT id FROM dids WHERE did = $1)")
            .bind(seeded_did(2))
            .fetch_one(&db.pool)
            .await
            .expect("viral");
        assert!(viral >= 2 * 56, "the viral post took its share of likes: {viral}");
        let text = words(&p, Some(Input { per_sec: 100.0, unique_share: 0.5, sampled: 10 }), &[LaneRun { lanes: 4, commits: 400, secs: 1.0, per_sec: 400.0, retries: 0, phases: vec![("commit".into(), 0.002)], waits: vec![(crate::db::waits::WaitClass::Wal, 3)], counts: Some(FolderRun { per_sec: 1200.0, behind_p99: 1.4, settle_secs: 0.2, settled_rows: 30 }) }], 1.0, 1_000_000, Duration::from_millis(5));
        assert!(text.contains("about 50 a second need applying") && text.contains("writer_lanes = 4 or more"), "{text}");
        assert!(text.contains("lane time per commit: commit 2.00 ms") && text.contains("database sessions: wal 100 %"), "{text}");
        assert!(text.contains("counts folder: 1200 changes a second, behind 1.4 s at p99; the settle after the run folded 30 rows in 0.20 s"), "{text}");
        // The kept result reads back as a sentence.
        let last = last_result(Some(&Input { per_sec: 100.0, unique_share: 0.5, sampled: 10 }), &[LaneRun { lanes: 1, commits: 400, secs: 10.0, per_sec: 40.0, retries: 0, phases: Vec::new(), waits: Vec::new(), counts: None }, LaneRun { lanes: 4, commits: 400, secs: 1.0, per_sec: 400.0, retries: 0, phases: Vec::new(), waits: Vec::new(), counts: None }]);
        let sentence = last_sentence(&last);
        assert!(sentence.contains("1 lanes 40/s, 4 lanes 400/s") && sentence.contains("about 50 a second needed") && sentence.contains("advice: 4 lanes or more"), "{sentence}");
        db.drop_db().await;
    }
}
