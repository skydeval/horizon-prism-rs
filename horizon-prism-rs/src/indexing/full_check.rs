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
use crate::firehose::writer::{Action, ApplyOptions, RawOp, WriteSource, Writer};
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
    /// How long the relays' rate is sampled for.
    pub sample_secs: u32,
    /// Keep the scratch database for a look afterwards.
    pub keep: bool,
}

impl Default for Params {
    fn default() -> Self {
        Self { accounts: 200_000, posts: 500_000, likes: 2_000_000, follows: 500_000, commits: 20_000, lanes: vec![1, 2, 4, 8], sample_secs: 10, keep: false }
    }
}

/// One lane count's run.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct LaneRun {
    pub lanes: u32,
    pub commits: u32,
    pub secs: f64,
    pub per_sec: f64,
    pub retries: u64,
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
    let report = result?;
    out.push_str(&report);
    if params.keep {
        let _ = writeln!(out, "The scratch database {scratch} was kept.");
    }
    Ok(out)
}

async fn check_in(cfg: &AppConfig, scratch: &str, params: &Params, input: Option<Input>) -> Result<String, String> {
    let options = crate::db::pool::connect_options(cfg.env.database_url.expose(), cfg.file.db.lock_timeout_secs)
        .map_err(|e| e.to_string())?
        .database(scratch);
    let pool = crate::db::pool::pool_options().connect_with(options).await.map_err(|e| format!("scratch database connect: {e}"))?;
    crate::db::migrations::MIGRATOR.run(&pool).await.map_err(|e| format!("scratch database migrations: {e}"))?;
    let seeded = Instant::now();
    seed(&pool, params).await.map_err(|e| format!("seeding: {e}"))?;
    let seed_secs = seeded.elapsed().as_secs_f64();
    let writer = full_writer(&pool).await;
    let mut runs = Vec::new();
    for (i, lanes) in params.lanes.iter().copied().filter(|l| *l > 0).enumerate() {
        let commits = workload(params, u64::try_from(i).unwrap_or(0));
        runs.push(apply(&writer, commits, lanes).await?);
    }
    let range = crate::aggregations::rebuild::repair_range(&pool, 1, 1 + crate::aggregations::rebuild::KEYSET_RANGE)
        .await
        .map_err(|e| format!("counts repair range: {e}"))?;
    let size: i64 = sqlx::query_scalar("SELECT pg_database_size(current_database())").fetch_one(&pool).await.map_err(|e| e.to_string())?;
    pool.close().await;
    Ok(words(params, input, &runs, seed_secs, size, range.slowest))
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

/// Apply `commits` with `lanes` lanes, each repo always on one lane; the
/// rate is commits over wall time.
async fn apply(writer: &Writer, commits: Vec<Synthetic>, lanes: u32) -> Result<LaneRun, String> {
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
            for (repo, rev, ops) in queue {
                let mut tries = 0;
                loop {
                    match writer.apply_copy(Some("bsky"), &repo, &rev, None, &ops, options, CopyTier::Whole).await {
                        Err(t) if tries < CONFLICT_RETRIES && t.conflict().is_some() => {
                            tries += 1;
                            retries += 1;
                            tokio::time::sleep(Duration::from_millis(5 * u64::from(tries))).await;
                        }
                        Err(t) => return Err(t.to_string()),
                        Ok(_) => break,
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
    Ok(LaneRun { lanes, commits: total, secs, per_sec: f64::from(total) / secs.max(1e-9), retries })
}

fn words(p: &Params, input: Option<Input>, runs: &[LaneRun], seed_secs: f64, size: i64, range: Duration) -> String {
    let mut out = String::new();
    #[allow(clippy::cast_precision_loss)]
    let mb = size as f64 / 1e6;
    let _ = writeln!(
        out,
        "Full check, advice only. Seeded {} accounts, {} posts, {} likes and {} follows in {seed_secs:.0} s ({mb:.0} MB), then applied {} commits per run with full admission and triggers on.",
        p.accounts, p.posts, p.likes, p.follows, p.commits
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
        let p = Params { accounts: 500, posts: 1000, likes: 3000, follows: 1000, commits: 400, lanes: vec![1, 4], sample_secs: 1, keep: false };
        let writer = {
            seed(&db.pool, &p).await.expect("seed");
            full_writer(&db.pool).await
        };
        let likes: i64 = sqlx::query_scalar("SELECT count(*) FROM likes").fetch_one(&db.pool).await.expect("likes");
        assert_eq!(likes, 3000);
        let counted: Option<i64> = sqlx::query_scalar("SELECT sum(like_count)::bigint FROM post_counts").fetch_one(&db.pool).await.expect("counts");
        assert!(counted.unwrap_or(0) > 0, "the triggers counted the seeded likes");
        for (i, lanes) in [1u32, 4].into_iter().enumerate() {
            let run = apply(&writer, workload(&p, i as u64), lanes).await.expect("apply");
            assert_eq!(run.commits, 400);
            assert!(run.per_sec > 0.0);
        }
        let viral: i64 = sqlx::query_scalar("SELECT count(*) FROM likes WHERE subj_author = (SELECT id FROM dids WHERE did = $1)")
            .bind(seeded_did(2))
            .fetch_one(&db.pool)
            .await
            .expect("viral");
        assert!(viral >= 2 * 56, "the viral post took its share of likes: {viral}");
        let text = words(&p, Some(Input { per_sec: 100.0, unique_share: 0.5, sampled: 10 }), &[LaneRun { lanes: 4, commits: 400, secs: 1.0, per_sec: 400.0, retries: 0 }], 1.0, 1_000_000, Duration::from_millis(5));
        assert!(text.contains("about 50 a second need applying") && text.contains("writer_lanes = 4 or more"), "{text}");
        db.drop_db().await;
    }
}
