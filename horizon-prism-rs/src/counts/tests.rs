//! The count log and its folder: triggers only log; a fold applies each
//! log row once, likes and reposts by account; a crash or a failed fold
//! loses and doubles nothing; two folders fold one at a time; lanes never
//! wait on a count row; old tag hours are dropped; a large log is folded
//! in slices; the options and Health's words.

use std::sync::Arc;
use std::time::{Duration, Instant};

use sqlx::PgPool;

use super::*;
use crate::test_support::{fresh_db, hooks};

/// Count rows that differ from a recompute from the rows (the counts
/// repair's statements, read only), all-zero rows left out, in both count
/// tables; and the log's rows.
pub(crate) async fn differences(pool: &PgPool) -> (i64, i64) {
    let posts: i64 = sqlx::query_scalar(
        "WITH t AS ( \
           SELECT a, k, sum(l)::int AS l, sum(r)::int AS r, sum(p)::int AS p, sum(q)::int AS q FROM ( \
             SELECT subj_author AS a, subj_rkey AS k, count(DISTINCT author) AS l, 0 AS r, 0 AS p, 0 AS q FROM likes WHERE subj_coll = 1 GROUP BY 1, 2 \
             UNION ALL SELECT subj_author, subj_rkey, 0, count(DISTINCT author), 0, 0 FROM reposts WHERE subj_coll = 1 GROUP BY 1, 2 \
             UNION ALL SELECT parent_author, parent_rkey, 0, 0, 1, 0 FROM posts WHERE parent_coll = 1 \
             UNION ALL SELECT quote_author, quote_rkey, 0, 0, 0, 1 FROM posts WHERE quote_coll = 1) s GROUP BY a, k), \
         want AS (SELECT a, k, l, r, p, q FROM t WHERE l + r + p + q > 0), \
         got AS (SELECT author, rkey, like_count, repost_count, reply_count, quote_count FROM post_counts \
                 WHERE like_count + repost_count + reply_count + quote_count > 0) \
         SELECT (SELECT count(*) FROM (SELECT * FROM want EXCEPT SELECT * FROM got) x) + (SELECT count(*) FROM (SELECT * FROM got EXCEPT SELECT * FROM want) y)",
    )
    .fetch_one(pool)
    .await
    .expect("post count differences");
    let actors: i64 = sqlx::query_scalar(
        "WITH t AS ( \
           SELECT d, sum(fr)::int AS fr, sum(fs)::int AS fs, sum(p)::int AS p FROM ( \
             SELECT subject AS d, 1 AS fr, 0 AS fs, 0 AS p FROM follows \
             UNION ALL SELECT author, 0, 1, 0 FROM follows \
             UNION ALL SELECT author, 0, 0, 1 FROM posts) s GROUP BY d), \
         want AS (SELECT d, fr, fs, p FROM t WHERE fr + fs + p > 0), \
         got AS (SELECT id, followers_count, follows_count, posts_count FROM actor_counts WHERE followers_count + follows_count + posts_count > 0) \
         SELECT (SELECT count(*) FROM (SELECT * FROM want EXCEPT SELECT * FROM got) x) + (SELECT count(*) FROM (SELECT * FROM got EXCEPT SELECT * FROM want) y)",
    )
    .fetch_one(pool)
    .await
    .expect("actor count differences");
    let log: i64 = sqlx::query_scalar("SELECT (SELECT count(*) FROM count_log) + (SELECT count(*) FROM count_log_tags)").fetch_one(pool).await.expect("log");
    (posts + actors, log)
}

/// `n` accounts from `base` on: their DIDs and ids, interned.
async fn accounts(pool: &PgPool, base: u64, n: usize) -> (Vec<String>, Vec<i32>) {
    let dids: Vec<String> = (0..n as u64).map(|i| crate::indexing::full_check::seeded_did(base + i)).collect();
    let ids = crate::db::ids::Ids::new(pool.clone()).account_ids(&dids).await.expect("intern");
    let out = dids.iter().map(|d| ids[d]).collect();
    (dids, out)
}

/// Insert a like of post (`sa`, `sr`) by `author`, key `rkey`.
async fn like(pool: &PgPool, author: i32, rkey: i64, sa: i32, sr: i64) {
    sqlx::query("INSERT INTO likes (author, rkey, subj_author, subj_coll, subj_rkey, rev, created_at) VALUES ($1, $2, $3, 1, $4, 1, now() - interval '30 days')")
        .bind(author)
        .bind(rkey)
        .bind(sa)
        .bind(sr)
        .execute(pool)
        .await
        .expect("like");
}

async fn unlike(pool: &PgPool, author: i32, rkey: i64) {
    sqlx::query("DELETE FROM likes WHERE author = $1 AND rkey = $2").bind(author).bind(rkey).execute(pool).await.expect("unlike");
}

async fn like_count(pool: &PgPool, sa: i32, sr: i64) -> Option<i32> {
    sqlx::query_scalar("SELECT like_count FROM post_counts WHERE author = $1 AND rkey = $2").bind(sa).bind(sr).fetch_optional(pool).await.expect("count")
}

fn db_key(pool: &PgPool) -> String {
    pool.connect_options().get_database().unwrap_or_default().to_string()
}

#[test]
fn slices_are_the_smallest_power_of_two_that_fits() {
    assert_eq!(slices_for(0, 100_000), 1);
    assert_eq!(slices_for(100_000, 100_000), 1);
    assert_eq!(slices_for(100_001, 100_000), 2);
    assert_eq!(slices_for(500_000, 100_000), 8);
    assert_eq!(slices_for(i64::MAX, 1000), MAX_SLICES);
    assert_eq!(span(Duration::from_millis(1200)), "1.2 s");
    assert_eq!(span(Duration::from_secs(240)), "4 minutes");
    assert_eq!(interval_words(Duration::from_millis(1000)), "1 s");
    assert_eq!(interval_words(Duration::from_millis(250)), "250 ms");
}

/// Every count trigger appends to the log and none touches a count table:
/// only the fold writes `post_counts`, `actor_counts` and
/// `tag_activity_hourly` in the database's functions, and nothing in the
/// code disables triggers or truncates the tables the counts come from.
#[tokio::test]
async fn count_triggers_only_log_and_nothing_disables_them() {
    let Some(db) = fresh_db("counts_triggers_only_log").await else {
        return;
    };
    let p = &db.pool;
    let writers: Vec<String> = sqlx::query_scalar(
        "SELECT p.proname::text FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace WHERE n.nspname = 'public' \
           AND p.prosrc ~* '(insert into|update|delete from)\\s+(post_counts|actor_counts|tag_activity_hourly)' ORDER BY 1",
    )
    .fetch_all(p)
    .await
    .expect("functions");
    assert_eq!(writers, ["prism_fold_counts"], "only the fold writes count rows");
    let logging: Vec<String> = sqlx::query_scalar(
        "SELECT DISTINCT p.proname::text FROM pg_trigger t JOIN pg_proc p ON p.oid = t.tgfoid JOIN pg_class c ON c.oid = t.tgrelid \
         WHERE NOT t.tgisinternal AND c.relname IN ('likes', 'reposts', 'posts', 'follows', 'post_search') AND p.proname LIKE ANY (ARRAY['count_%', 'tag_activity_%']) ORDER BY 1",
    )
    .fetch_all(p)
    .await
    .expect("triggers");
    assert_eq!(logging, ["count_follows", "count_interaction_accounts", "count_posts", "tag_activity_engagement", "tag_activity_own", "tag_activity_replies"]);
    // A like, a follow and a tagged post log their changes and leave the
    // count tables as they were.
    let lag = hooks::arm(hooks::COUNTS_LAG, db.name());
    let (dids, ids) = accounts(p, 100, 3).await;
    like(p, ids[0], 1, ids[1], 7).await;
    crate::test_support::graph::follow(p, &dids[0], &dids[1], "3kaaaaaaaaaf1").await;
    let record = serde_json::json!({"$type": "app.bsky.feed.post", "text": "x #trigtag", "tags": ["trigtag"], "createdAt": chrono::Utc::now().to_rfc3339()});
    crate::test_support::posts::put(p, &dids[2], "3kaaaaaaaaap1", &record, None).await;
    let counted: (i64, i64, i64) = sqlx::query_as("SELECT (SELECT count(*) FROM post_counts), (SELECT count(*) FROM actor_counts), (SELECT count(*) FROM tag_activity_hourly)")
        .fetch_one(p)
        .await
        .expect("counts");
    assert_eq!(counted, (0, 0, 0), "no count row was written");
    let logged: (i64, i64) = sqlx::query_as("SELECT (SELECT count(*) FROM count_log), (SELECT count(*) FROM count_log_tags)").fetch_one(p).await.expect("log");
    assert!(logged.0 >= 4 && logged.1 >= 1, "{logged:?}");
    drop(lag);
    // The code: no trigger switched off, no source table truncated.
    fn walk(dir: &std::path::Path, out: &mut Vec<(String, String)>) {
        for e in std::fs::read_dir(dir).expect("dir").flatten() {
            let path = e.path();
            if path.is_dir() {
                walk(&path, out);
            } else if path.extension().is_some_and(|x| x == "rs" || x == "sql") {
                out.push((path.display().to_string(), std::fs::read_to_string(&path).unwrap_or_default().to_lowercase()));
            }
        }
    }
    let mut files = Vec::new();
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"));
    walk(&root.join("src"), &mut files);
    for (name, text) in &files {
        if name.ends_with("counts/tests.rs") {
            continue;
        }
        // Replica mode skips triggers: only where no counted table is written.
        if text.contains("session_replication_role") {
            for table in ["into likes", "into reposts", "into posts", "into follows", "into post_search"] {
                assert!(!text.contains(table), "{name} writes {table:?} in replica mode");
            }
        }
        for needle in ["disable trigger", "truncate likes", "truncate reposts", "truncate posts", "truncate follows", "truncate post_search", "truncate table likes", "truncate table posts"] {
            assert!(!text.contains(needle), "{name} contains {needle:?}");
        }
    }
    db.drop_db().await;
}

/// Likes and unlikes of one account on a viral post across several folds,
/// a follow, unfollow and follow, a reply and its delete: whenever folded,
/// the counts are the recompute's, the like counted once, and the log
/// empties.
#[tokio::test]
async fn folds_at_any_points_count_like_a_recompute() {
    let Some(db) = fresh_db("counts_fold_points").await else {
        return;
    };
    let p = &db.pool;
    let lag = hooks::arm(hooks::COUNTS_LAG, db.name());
    let (_, ids) = accounts(p, 200, 4).await;
    let (viral, fan, other) = ((ids[0], 1_i64), ids[1], ids[2]);
    for round in 0..4_i64 {
        like(p, fan, 10 + round, viral.0, viral.1).await;
        like(p, other, 20 + round, viral.0, viral.1).await;
        settle(p).await.expect("settle");
        unlike(p, fan, 10 + round).await;
        if round % 2 == 0 {
            settle(p).await.expect("settle");
        }
        unlike(p, other, 20 + round).await;
        like(p, fan, 30 + round, viral.0, viral.1).await;
    }
    settle(p).await.expect("settle");
    assert_eq!(like_count(p, viral.0, viral.1).await, Some(1), "one account's like, however often it liked and unliked");
    assert_eq!(differences(p).await, (0, 0));
    drop(lag);
    db.drop_db().await;
}

/// A writer commits while no folder runs; a new folder on a new pool
/// folds once and the counts are right. A fold that fails after its
/// statement rolls back, and the next applies each row once.
#[tokio::test]
async fn a_crash_or_a_failed_fold_loses_and_doubles_nothing() {
    let Some(db) = fresh_db("counts_crash").await else {
        return;
    };
    let p = &db.pool;
    let lag = hooks::arm(hooks::COUNTS_LAG, db.name());
    let (_, ids) = accounts(p, 300, 3).await;
    like(p, ids[1], 1, ids[0], 5).await;
    like(p, ids[2], 2, ids[0], 5).await;
    // A new pool, as after a restart.
    let fresh = crate::db::pool::pool_options_sized(2).connect_with((*p.connect_options()).clone()).await.expect("pool");
    let r = round(&fresh, false, 100_000).await.expect("round");
    assert_eq!(r.log_rows, 2);
    assert_eq!(like_count(p, ids[0], 5).await, Some(2));
    // A failing fold rolls back: the rows stay in the log, the count as it was.
    like(p, ids[1], 3, ids[0], 6).await;
    let fails = hooks::arm_times(hooks::FOLD_FAILS, &db_key(p), 1);
    assert!(round(&fresh, false, 100_000).await.is_err());
    assert_eq!(fails.hits(), 1);
    assert_eq!(like_count(p, ids[0], 6).await, None);
    assert_eq!(differences(p).await.1, 1, "the failed fold's row is still in the log");
    settle(&fresh).await.expect("settle");
    assert_eq!(like_count(p, ids[0], 6).await, Some(1));
    assert_eq!(differences(p).await, (0, 0));
    drop(fails);
    drop(lag);
    fresh.close().await;
    db.drop_db().await;
}

/// Two folders on two pools and eight writers for 10 s: the folders fold
/// one at a time (a fold finds the lock taken and tries again), and after
/// a settle the counts are exact and the log is empty.
#[tokio::test]
async fn two_folders_and_eight_writers_keep_counts_exact() {
    let Some(db) = fresh_db("counts_two_folders").await else {
        return;
    };
    let p = &db.pool;
    let recorder = metrics_exporter_prometheus::PrometheusBuilder::new().build_recorder();
    let handle = recorder.handle();
    let _local = metrics::set_default_local_recorder(&recorder);
    let lag = hooks::arm(hooks::COUNTS_LAG, db.name());
    let (dids, ids) = accounts(p, 400, 12).await;
    let cfg = crate::config::CountsConfig { fold_interval_ms: 10, ..crate::config::CountsConfig::default() };
    let mut folders = Vec::new();
    let mut pools = Vec::new();
    for _ in 0..2 {
        let pool = crate::db::pool::pool_options_sized(4).connect_with((*p.connect_options()).clone()).await.expect("pool");
        folders.push(Folder::new(pool.clone(), &cfg, Arc::new(FoldStatus::default())).spawn());
        pools.push(pool);
    }
    let until = Instant::now() + Duration::from_secs(10);
    let mut writers = Vec::new();
    for w in 0..8_usize {
        let (p, ids, dids) = (p.clone(), ids.clone(), dids.clone());
        writers.push(tokio::spawn(async move {
            let mut n: i64 = 0;
            while Instant::now() < until {
                n += 1;
                let author = ids[4 + w];
                let post = n % 3;
                like(&p, author, n, ids[0], post).await;
                if n % 4 == 0 {
                    unlike(&p, author, n).await;
                }
                if n % 5 == 0 {
                    let subject = &dids[usize::try_from(n % 3).unwrap_or(0)];
                    crate::test_support::graph::follow(&p, &dids[4 + w], subject, &crate::thread_outline::ops::tid_string(1_700_000_000_000_000 + n)).await;
                }
            }
        }));
    }
    for w in writers {
        w.await.expect("writer");
    }
    for f in folders {
        f.abort();
    }
    settle(p).await.expect("settle");
    assert_eq!(differences(p).await, (0, 0));
    let skipped = crate::test_support::series(&handle.render(), METRIC_SKIPPED, &[]).unwrap_or(0.0);
    assert!(skipped > 0.0, "a fold found the other folder holding the lock");
    drop(lag);
    for pool in pools {
        pool.close().await;
    }
    db.drop_db().await;
}

/// Eight lanes whose batches like one post and follow one account commit
/// with no lock wait (a folder running beside them), and the counts are
/// right after a settle.
#[tokio::test]
async fn eight_lanes_on_one_hot_post_wait_on_no_lock() {
    use crate::firehose::writer::{Action, ApplyOptions, LiveCommit, RawOp, WriteSource, Writer};
    let Some(db) = fresh_db("counts_eight_lanes").await else {
        return;
    };
    let p = &db.pool;
    let lag = hooks::arm(hooks::COUNTS_LAG, db.name());
    let hot_author = "did:plc:lanehotaaaaaaaaaaaaaaaaa";
    let hot = crate::test_support::posts::plain(p, hot_author, "3kaaaaaaaaah1", "hot").await;
    let mut sets = crate::aggregations::ScopeSets::default();
    sets.full.kinds = crate::config::FullGroup::ALL.to_vec();
    let scope = crate::aggregations::ScopeState::new(sets, crate::config::ScopePolicy::MembersAndOneHop, 10_000);
    {
        let mut guard = scope.write_guard().await;
        scope.set_public(&mut guard, crate::indexing::PublicState { mode: crate::indexing::PublicMode::On, breadth: crate::indexing::Breadth::Full, generation: 1, ..crate::indexing::PublicState::default() });
    }
    let writer = Writer::new(p.clone(), scope, Arc::new(crate::private_tier::DisabledPrivateTier)).with_bulk_arm(true);
    let folder = Folder::new(p.clone(), &crate::config::CountsConfig::default(), Arc::new(FoldStatus::default())).spawn();
    let options = ApplyOptions { advance_applied_rev: true, dead_letter_readiness: true, attempts: 2, live: true, subscription_rules: true, source: WriteSource::Firehose };
    let tid = |n: i64| crate::thread_outline::ops::tid_string(1_700_000_000_000_000 + n);
    let mut lanes = Vec::new();
    for lane in 0..8_i64 {
        let (writer, hot) = (writer.clone(), hot.clone());
        lanes.push(tokio::spawn(async move {
            let mut corpus: Vec<(String, String, Vec<RawOp>)> = Vec::new();
            for i in 0..64_i64 {
                let n = lane * 1000 + i;
                let repo = crate::indexing::full_check::seeded_did(10_000 + u64::try_from(n).unwrap_or(0));
                let like = serde_json::json!({"$type": "app.bsky.feed.like", "subject": {"uri": hot, "cid": "bafyhot"}, "createdAt": chrono::Utc::now().to_rfc3339()});
                let follow = serde_json::json!({"$type": "app.bsky.graph.follow", "subject": hot_author, "createdAt": chrono::Utc::now().to_rfc3339()});
                corpus.push((
                    repo,
                    tid(n),
                    vec![
                        RawOp { action: Action::Create, collection: "app.bsky.feed.like".into(), rkey: tid(n), cid: Some("bafyl".into()), record: Some(like) },
                        RawOp { action: Action::Create, collection: "app.bsky.graph.follow".into(), rkey: tid(n), cid: Some("bafyf".into()), record: Some(follow) },
                    ],
                ));
            }
            let items: Vec<LiveCommit<'_>> = corpus
                .iter()
                .map(|(repo, rev, ops)| LiveCommit { relay: Some("bsky"), repo, rev, since: None, ops, options, tier: crate::firehose::tier::CopyTier::Whole, stream: None })
                .collect();
            let mut pos = 0;
            while pos < items.len() {
                pos += writer.apply_batch(&items[pos..], 8).await.expect("batch").len();
            }
        }));
    }
    // Sample the database's sessions while the lanes run: none waits on a
    // heavyweight lock but a relation's extension.
    let mut waits = 0;
    while lanes.iter().any(|l| !l.is_finished()) {
        let n: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM pg_stat_activity WHERE datname = current_database() AND pid <> pg_backend_pid() \
               AND wait_event_type = 'Lock' AND wait_event <> 'extend'",
        )
        .fetch_one(p)
        .await
        .expect("activity");
        waits += n;
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    for l in lanes {
        l.await.expect("lane");
    }
    folder.abort();
    assert_eq!(waits, 0, "no session waited on a lock");
    settle(p).await.expect("settle");
    assert_eq!(crate::test_support::interactions::post_counts(p, &hot).await.map(|c| c.0), Some(512));
    let followers: i32 = sqlx::query_scalar("SELECT followers_count FROM actor_counts WHERE id = (SELECT id FROM dids WHERE did = $1)").bind(hot_author).fetch_one(p).await.expect("followers");
    assert_eq!(followers, 512);
    assert_eq!(differences(p).await, (0, 0));
    drop(lag);
    db.drop_db().await;
}

/// A pending tag change whose hour has passed the trends cutoff is dropped
/// by the fold; a recent one is applied.
#[tokio::test]
async fn a_fold_drops_tag_changes_of_hours_past_the_cutoff() {
    let Some(db) = fresh_db("counts_old_tag_hours").await else {
        return;
    };
    let p = &db.pool;
    sqlx::query(
        "INSERT INTO count_log_tags (tag, hour, dposts, dlikes, dreposts, dreplies) VALUES \
           ('oldtag', date_trunc('hour', now() - interval '100 hours'), 1, 0, 0, 0), \
           ('newtag', date_trunc('hour', now()), 1, 2, 0, 0)",
    )
    .execute(p)
    .await
    .expect("log");
    let r = settle(p).await.expect("settle");
    assert_eq!(r.tag_rows, 2);
    let rows: Vec<(String, i32, i32)> = sqlx::query_as("SELECT tag, posts, likes FROM tag_activity_hourly ORDER BY tag").fetch_all(p).await.expect("tags");
    assert_eq!(rows, [("newtag".to_string(), 1, 2)]);
    assert_eq!(differences(p).await.1, 0);
    db.drop_db().await;
}

/// 500 000 log rows from a held-back folder: the round takes 8 slices at
/// 100 000 rows a slice, each its own fold, a direct writer waiting for the
/// lock meanwhile gets it, and the counts are exact after.
#[tokio::test]
async fn a_large_log_is_folded_in_slices() {
    let Some(db) = fresh_db("counts_slices").await else {
        return;
    };
    let p = &db.pool;
    let recorder = metrics_exporter_prometheus::PrometheusBuilder::new().build_recorder();
    let handle = recorder.handle();
    let _local = metrics::set_default_local_recorder(&recorder);
    let lag = hooks::arm(hooks::COUNTS_LAG, db.name());
    let (_, ids) = accounts(p, 500, 64).await;
    // 500 000 likes in one statement, each its own (post, account) pair:
    // one log row each.
    sqlx::query(
        "INSERT INTO likes (author, rkey, subj_author, subj_coll, subj_rkey, rev, created_at) \
         SELECT ($1::int[])[1 + (g % 32)], g, ($1::int[])[33 + ((g / 32) % 32)], 1, g, 1, now() - interval '30 days' FROM generate_series(1, 500000) g",
    )
    .bind(&ids)
    .execute(p)
    .await
    .expect("likes");
    let (_, logged) = differences(p).await;
    assert_eq!(logged, 500_000);
    // A direct writer that wants the lock while the round runs.
    let direct = {
        let p = p.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(20)).await;
            let mut tx = p.begin().await.expect("begin");
            lock(&mut tx).await.expect("the lock, between two slices");
            tx.commit().await.expect("commit");
            Instant::now()
        })
    };
    let r = round(p, false, 100_000).await.expect("round");
    let ended = Instant::now();
    assert_eq!(r.slices, 8);
    assert_eq!(r.log_rows, 500_000);
    let got_lock = direct.await.expect("direct writer");
    assert!(got_lock < ended, "the direct writer got the lock during the round");
    let folds = crate::test_support::series(&handle.render(), &format!("{METRIC_FOLD_SECONDS}_count"), &[]).unwrap_or(0.0);
    assert!((folds - 8.0).abs() < f64::EPSILON, "one fold transaction a slice: {folds}");
    assert_eq!(differences(p).await, (0, 0));
    drop(lag);
    db.drop_db().await;
}

/// Health's Counts line: not folded yet; up to the lag; a long interval;
/// behind with each named cause; failing.
#[tokio::test]
async fn the_counts_line_says_how_far_behind_and_why() {
    let Some(db) = fresh_db("counts_health").await else {
        return;
    };
    let p = &db.pool;
    let status = FoldStatus::new(Duration::from_secs(1));
    let now = Instant::now();
    let at = |d: u64| now + Duration::from_secs(d);
    assert_eq!(health_at(&status, p, now, OLD_SNAPSHOT).await, Health::NoData("Counts have not been folded yet since start.".to_string()));
    status.done(now - Duration::from_millis(1200), &Round::default());
    assert_eq!(health_at(&status, p, now, OLD_SNAPSHOT).await, Health::Ok("Counts are up to 1.2 s behind ingest (folded every 1 s).".to_string()));
    let long = FoldStatus::new(Duration::from_secs(120));
    long.done(now, &Round::default());
    let Health::Warning(text) = health_at(&long, p, now, OLD_SNAPSHOT).await else { panic!("a long interval warns") };
    assert!(text.contains("counts will be up to 2 minutes behind"), "{text}");
    // Behind for a minute: the rates, the log's size, and a cause.
    let behind = FoldStatus::new(Duration::from_secs(1));
    behind.done(now - Duration::from_secs(300), &Round { log_before: 1000, log_rows: 900, ..Round::default() });
    behind.done(now - Duration::from_secs(240), &Round { log_before: 1500, log_rows: 900, ..Round::default() });
    let _ = health_at(&behind, p, now, OLD_SNAPSHOT).await;
    let Health::Warning(text) = health_at(&behind, p, at(61), OLD_SNAPSHOT).await else { panic!("behind warns") };
    assert!(text.starts_with("Counts are 5 minutes behind ingest: the counts folder folds 15 changes a second, ingest makes 23."), "{text}");
    assert!(text.contains("Nothing is lost; counts catch up when ingest slows.") && text.contains("The log holds 1,500 changes"), "{text}");
    {
        let _held = holding(p, "a counts repair is running");
        let Health::Warning(text) = health_at(&behind, p, at(62), OLD_SNAPSHOT).await else { panic!("behind warns") };
        assert!(text.contains("Now a counts repair is running."), "{text}");
    }
    let mut other = p.begin().await.expect("begin");
    lock(&mut other).await.expect("lock");
    let Health::Warning(text) = health_at(&behind, p, at(63), OLD_SNAPSHOT).await else { panic!("behind warns") };
    assert!(text.contains("Another database session holds the counts lock."), "{text}");
    other.rollback().await.expect("rollback");
    let mut old = p.begin().await.expect("begin");
    sqlx::query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ").execute(&mut *old).await.expect("rr");
    sqlx::query("SELECT count(*) FROM count_log").execute(&mut *old).await.expect("snapshot");
    tokio::time::sleep(Duration::from_millis(50)).await;
    let Health::Warning(text) = health_at(&behind, p, at(64), Duration::from_millis(10)).await else { panic!("behind warns") };
    assert!(text.contains("An old open transaction keeps the log from being cleaned."), "{text}");
    old.rollback().await.expect("rollback");
    behind.failed("connection refused");
    let Health::Warning(text) = health_at(&behind, p, Instant::now() + Duration::from_secs(180), OLD_SNAPSHOT).await else { panic!("failing warns") };
    assert!(text.starts_with("The counts folder has failed for 3 minutes: connection refused. Changes wait in the log; nothing is lost."), "{text}");
    db.drop_db().await;
}

/// The direct writers' classes against a racing writer, paused between the
/// settle and the statement: a key deleter drops the key's pending rows,
/// leaving no row, as counting at once and then deleting would have; a
/// recomputer leaves the right value, counts nothing twice, and calls a row
/// repaired only when the stored value and its pending change were wrong;
/// the whole-table repair holds writers' log inserts and leaves the log
/// empty.
#[tokio::test]
async fn direct_writers_race_a_writer_and_count_once() {
    let Some(db) = fresh_db("counts_direct_writers").await else {
        return;
    };
    let p = &db.pool;
    let (_, ids) = accounts(p, 600, 4).await;
    let (author, fan, other) = (ids[0], ids[1], ids[2]);
    // Key deleter: a like committed after the settle goes with the row.
    like(p, fan, 1, author, 10).await;
    settle(p).await.expect("settle");
    let lag = hooks::arm(hooks::COUNTS_LAG, db.name());
    like(p, other, 2, author, 10).await;
    let mut tx = p.begin().await.expect("begin");
    lock(&mut tx).await.expect("lock");
    let select = format!("SELECT author, rkey FROM post_counts WHERE author = {author} AND rkey = 10");
    let n = sqlx::query(&crate::db::retention::key_delete_sql("post_counts", &select)).execute(&mut *tx).await.expect("delete").rows_affected();
    tx.commit().await.expect("commit");
    assert_eq!(n, 1);
    assert_eq!(differences(p).await.1, 0, "the key's pending like went with its row");
    settle(p).await.expect("settle");
    assert_eq!(like_count(p, author, 10).await, None, "no row, as at once");
    drop(lag);
    // Recomputer: the keyset repair, paused after its settle while a like
    // commits; afterwards the count is right and a later fold adds nothing.
    sqlx::query("DELETE FROM likes").execute(p).await.expect("clear");
    settle(p).await.expect("settle");
    like(p, fan, 3, author, 11).await;
    settle(p).await.expect("settle");
    let paused = hooks::arm(hooks::COUNTS_AFTER_SETTLE, db.name());
    let repair = {
        let p = p.clone();
        tokio::spawn(async move { crate::aggregations::rebuild::repair_counts_keyset(&p).await })
    };
    paused.reached().await;
    let lag = hooks::arm(hooks::COUNTS_LAG, db.name());
    like(p, other, 4, author, 11).await;
    paused.release();
    let report = repair.await.expect("task").expect("repair");
    assert_eq!(report.updated, 0, "the stored value plus its pending change was right: nothing to repair ({report:?})");
    drop(paused);
    drop(lag);
    settle(p).await.expect("settle");
    assert_eq!(like_count(p, author, 11).await, Some(2));
    assert_eq!(differences(p).await, (0, 0));
    // A wrong stored value is repaired and counted as such.
    sqlx::query("UPDATE post_counts SET like_count = 7 WHERE author = $1 AND rkey = 11").bind(author).execute(p).await.expect("corrupt");
    let report = crate::aggregations::rebuild::repair_counts_keyset(p).await.expect("repair");
    assert!(report.updated >= 1, "{report:?}");
    assert_eq!(like_count(p, author, 11).await, Some(2));
    // The whole-table repair holds a writer's log insert until it commits,
    // and leaves the log empty.
    let lag = hooks::arm(hooks::COUNTS_LAG, db.name());
    let mut held = p.begin().await.expect("begin");
    lock(&mut held).await.expect("lock");
    sqlx::query("LOCK TABLE count_log IN SHARE ROW EXCLUSIVE MODE").execute(&mut *held).await.expect("hold the log");
    let writer = {
        let p = p.clone();
        tokio::spawn(async move { like(&p, fan, 5, author, 12).await })
    };
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(!writer.is_finished(), "the writer's log insert waits for the table lock");
    held.rollback().await.expect("release");
    writer.await.expect("writer");
    crate::aggregations::rebuild::repair_counts(p).await.expect("repair");
    assert_eq!(differences(p).await, (0, 0), "the repair recomputed and emptied the log");
    drop(lag);
    db.drop_db().await;
}

/// Lock order: a key deleter under a DID lock, keyset ranges, settles and
/// the folder run together with eight writers for 5 s: no deadlock and no
/// lock timeout, and the counts are exact after. A fold that fails every
/// time does not stop the keyset repair, which removes its ranges' log
/// rows and leaves exact counts.
#[tokio::test]
async fn direct_writers_settles_and_the_folder_run_with_writers_without_deadlock() {
    let Some(db) = fresh_db("counts_lock_order").await else {
        return;
    };
    let p = &db.pool;
    let lag = hooks::arm(hooks::COUNTS_LAG, db.name());
    let (dids, ids) = accounts(p, 700, 16).await;
    let folder = Folder::new(p.clone(), &crate::config::CountsConfig { fold_interval_ms: 20, ..crate::config::CountsConfig::default() }, Arc::new(FoldStatus::default())).spawn();
    let until = Instant::now() + Duration::from_secs(5);
    let failed = Arc::new(Mutex::new(Vec::<String>::new()));
    let note = |failed: &Arc<Mutex<Vec<String>>>, r: Result<(), sqlx::Error>| {
        if let Err(e) = r {
            failed.lock().unwrap_or_else(PoisonError::into_inner).push(e.to_string());
        }
    };
    let mut tasks = Vec::new();
    for w in 0..8_usize {
        let (p, ids) = (p.clone(), ids.clone());
        tasks.push(tokio::spawn(async move {
            let mut n: i64 = 0;
            while Instant::now() < until {
                n += 1;
                like(&p, ids[8 + w], n, ids[0], n % 5).await;
                if n % 3 == 0 {
                    unlike(&p, ids[8 + w], n).await;
                }
            }
        }));
    }
    // A key deleter of posts no writer likes, under a DID lock.
    {
        let (p, dids, ids, failed) = (p.clone(), dids.clone(), ids.clone(), Arc::clone(&failed));
        tasks.push(tokio::spawn(async move {
            let mut n: i64 = 100;
            while Instant::now() < until {
                n += 1;
                sqlx::query("INSERT INTO post_counts (author, rkey, like_count) VALUES ($1, $2, 0) ON CONFLICT DO NOTHING").bind(ids[1]).bind(n).execute(&p).await.expect("row");
                let r = async {
                    let mut tx = p.begin().await?;
                    sqlx::query(crate::db::retention::LOCK_DID_SQL).bind(&dids[1]).execute(&mut *tx).await?;
                    lock(&mut tx).await?;
                    let select = format!("SELECT author, rkey FROM post_counts WHERE author = {} AND rkey = {n}", ids[1]);
                    sqlx::query(&crate::db::retention::key_delete_sql("post_counts", &select)).execute(&mut *tx).await?;
                    tx.commit().await
                }
                .await;
                note(&failed, r);
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        }));
    }
    // Keyset ranges and settles.
    {
        let (p, failed) = (p.clone(), Arc::clone(&failed));
        tasks.push(tokio::spawn(async move {
            while Instant::now() < until {
                note(&failed, crate::aggregations::rebuild::repair_range(&p, 0, i32::MAX).await.map(|_| ()));
                note(&failed, settle(&p).await.map(|_| ()));
            }
        }));
    }
    for t in tasks {
        t.await.expect("task");
    }
    folder.abort();
    let failed = failed.lock().unwrap_or_else(PoisonError::into_inner).clone();
    assert!(failed.is_empty(), "no deadlock, no lock timeout: {failed:?}");
    settle(p).await.expect("settle");
    assert_eq!(differences(p).await, (0, 0));
    // Folds failing every time: the keyset repair still recomputes.
    like(p, ids[9], 999_999, ids[0], 1).await;
    let fails = hooks::arm(hooks::FOLD_FAILS, &db_key(p));
    assert!(settle(p).await.is_err());
    sqlx::query("UPDATE post_counts SET like_count = 50 WHERE author = $1").bind(ids[0]).execute(p).await.expect("corrupt");
    crate::aggregations::rebuild::repair_counts_keyset(p).await.expect("the repair goes on without the fold");
    assert_eq!(differences(p).await, (0, 0), "exact counts and no log rows of its ranges");
    drop(fails);
    drop(lag);
    db.drop_db().await;
}

/// A trial's minute samples the most the counts were behind: the folder of
/// the database notes how far behind it was at each round, and a take
/// returns the most since the last and starts again.
#[tokio::test]
async fn how_far_behind_is_taken_for_the_trial_samples() {
    let Some(db) = fresh_db("counts_behind_taken").await else {
        return;
    };
    let p = &db.pool;
    assert_eq!(take_behind(p), None, "no folder for this database yet");
    let status = Arc::new(FoldStatus::default());
    let folder = Folder::new(p.clone(), &crate::config::CountsConfig { fold_interval_ms: 50, ..crate::config::CountsConfig::default() }, Arc::clone(&status)).spawn();
    let deadline = Instant::now() + Duration::from_secs(10);
    while status.view().behind.is_none() && Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    tokio::time::sleep(Duration::from_millis(200)).await;
    let first = take_behind(p).expect("sampled");
    assert!(first > 0.0 && first < 5.0, "{first}");
    folder.abort();
    let _ = folder.await;
    tokio::time::sleep(Duration::from_millis(300)).await;
    let held = take_behind(p).expect("sampled");
    assert!(held >= 0.3, "a stopped folder's counts fall behind: {held}");
    db.drop_db().await;
}

/// A database at migration 0068 (what 0.2.1 leaves, its counts made at
/// once) migrates to the count log: the counts stay as they were, the
/// deferral and its fail-safe are gone, and from then on a like is logged
/// and counted by a fold.
#[tokio::test]
async fn a_database_at_0068_migrates_to_the_count_log() {
    let Some(db) = crate::test_support::empty_db("counts_from_0068").await else {
        return;
    };
    let p = &db.pool;
    let dir = std::env::temp_dir().join(format!("hprs-migrations-0068-{}", db.name()));
    std::fs::create_dir_all(&dir).expect("dir");
    for e in std::fs::read_dir(concat!(env!("CARGO_MANIFEST_DIR"), "/migrations")).expect("migrations").flatten() {
        let name = e.file_name().to_string_lossy().to_string();
        if name.as_str() < "0069" {
            std::fs::copy(e.path(), dir.join(&name)).expect("copy");
        }
    }
    let old = sqlx::migrate::Migrator::new(dir.as_path()).await.expect("the 0.2.1 migrations");
    old.run(p).await.expect("migrate to 0068");
    let id = |did: String| async move { sqlx::query_scalar::<_, i32>("INSERT INTO dids (did) VALUES ($1) RETURNING id").bind(did).fetch_one(p).await.expect("did") };
    let (a, b, c) = (id(crate::indexing::full_check::seeded_did(1)).await, id(crate::indexing::full_check::seeded_did(2)).await, id(crate::indexing::full_check::seeded_did(3)).await);
    like(p, b, 1, a, 5).await;
    sqlx::query("INSERT INTO follows (author, rkey, subject, rev, created_at) VALUES ($1, 1, $2, 1, now())").bind(b).bind(a).execute(p).await.expect("follow");
    sqlx::query("INSERT INTO worker_state (key, value, updated_at) VALUES ('writer_counts_left', '2'::jsonb, now())").execute(p).await.expect("fail-safe count");
    assert_eq!(like_count(p, a, 5).await, Some(1), "0.2.1 counts at once");
    crate::db::migrations::MIGRATOR.run(p).await.expect("migrate to 0069");
    assert_eq!(like_count(p, a, 5).await, Some(1), "the counts stay");
    let followers: i32 = sqlx::query_scalar("SELECT followers_count FROM actor_counts WHERE id = $1").bind(a).fetch_one(p).await.expect("followers");
    assert_eq!(followers, 1);
    let gone: (bool, bool, bool, i64) = sqlx::query_as(
        "SELECT to_regproc('prism_defer_counts') IS NULL, to_regproc('prism_apply_counts') IS NULL, to_regproc('adjust_account_counts') IS NULL, \
           (SELECT count(*) FROM worker_state WHERE key = 'writer_counts_left')",
    )
    .fetch_one(p)
    .await
    .expect("dropped");
    assert_eq!(gone, (true, true, true, 0));
    let _: Vec<Option<f64>> = sqlx::query_scalar("SELECT counts_behind FROM full_trial_samples").fetch_all(p).await.expect("the samples' new column");
    like(p, c, 2, a, 5).await;
    assert_eq!((like_count(p, a, 5).await, differences(p).await.1), (Some(1), 1), "logged, not counted at once");
    settle(p).await.expect("settle");
    assert_eq!(like_count(p, a, 5).await, Some(2));
    assert_eq!(differences(p).await, (0, 0));
    let _ = std::fs::remove_dir_all(&dir);
    db.drop_db().await;
}
