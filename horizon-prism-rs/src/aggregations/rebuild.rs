//! Scope rebuild (design-v2 §9.4, §9.5), run by `admin scope rebuild` and
//! weekly by the scheduler.
//!
//! 1. Recompute S from `members`, `follows` and the interaction tables under
//!    the active policy (public tier only; the follow cap applies per member).
//! 2. Upsert the confirmed DIDs into `scope_dids` with `justified_at = now()`,
//!    in batches that take each DID's shared lock before inserting (§6.11.7);
//!    a DID new to S gets a `scope` backfill.
//! 3. Purge (`out_of_scope`, two-phase) every DID whose `justified_at` is
//!    older than `scope.rebuild_grace_days`.
//! 4. Recompute `post_counts` and `actor_counts` from the base tables.
//! 5. Refresh the in-memory per-member follow counts.
//!
//! The weekly run also enqueues `resync` jobs, 1 DID/s, for in-scope DIDs
//! whose last re-sync is older than 7 days (§5.7, §9.4).

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use chrono::{DateTime, TimeDelta, Utc};
use sqlx::PgPool;
use tokio::task::JoinHandle;

use super::policy::{REASON_FOLLOWED, REASON_INTERACTED, REASON_MEMBER, grows_on_follow, grows_on_interaction};
use super::purge::Purger;
use super::scope_state::{PurgeKind, ScopeState};
use crate::db::locks::lock_dids_shared;
use crate::firehose::jobs::Depth;
use crate::private_tier::PrivateTier;

/// DIDs per upsert transaction (bounded advisory-lock count, §6.11.7).
pub const UPSERT_BATCH: usize = 500;
/// The weekly schedule is checked this often; the run itself is weekly.
pub const SCHEDULE_CHECK: Duration = Duration::from_secs(3600);
/// Period of the scheduled rebuild.
pub const WEEK: TimeDelta = TimeDelta::days(7);
/// `worker_state` key recording the last scheduled rebuild.
pub const KEY_LAST_REBUILD: &str = "last_scope_rebuild";

/// What a rebuild did (or, for a dry run, would do).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RebuildReport {
    pub justified: usize,
    pub added: usize,
    pub purged: usize,
    pub resync_enqueued: u64,
}

/// The rebuild runner.
#[derive(Clone)]
pub struct Rebuild {
    pool: PgPool,
    scope: ScopeState,
    purger: Purger,
    grace_days: u32,
}

impl Rebuild {
    #[must_use]
    pub fn new(pool: PgPool, scope: ScopeState, private: Arc<dyn PrivateTier>, grace_days: u32) -> Self {
        let purger = Purger::new(pool.clone(), scope.clone(), private);
        Self {
            pool,
            scope,
            purger,
            grace_days,
        }
    }

    /// Set `lists.sync_min_interval_secs` on the rebuild's purges.
    #[must_use]
    pub fn with_sync_min_interval_secs(mut self, secs: u32) -> Self {
        self.purger = self.purger.with_sync_min_interval_secs(secs);
        self
    }

    /// Step 1: DID → reason bits under the active policy.
    ///
    /// # Errors
    ///
    /// Database errors.
    pub async fn compute(&self) -> Result<BTreeMap<String, i16>, sqlx::Error> {
        let policy = self.scope.policy();
        let mut out: BTreeMap<String, i16> = BTreeMap::new();
        let members: Vec<String> = sqlx::query_scalar("SELECT did FROM members").fetch_all(&self.pool).await?;
        for m in members {
            *out.entry(m).or_insert(0) |= REASON_MEMBER;
        }
        if grows_on_follow(policy) {
            // Each member's earliest follows up to the cap (§9.4).
            let cap = i64::from(self.scope.max_follow_scope_per_member());
            let followed: Vec<String> = sqlx::query_scalar(
                "SELECT DISTINCT subject_did FROM ( \
                   SELECT f.subject_did, row_number() OVER (PARTITION BY f.author_did ORDER BY f.created_at, f.uri) AS n \
                   FROM follows f JOIN members m ON m.did = f.author_did) t WHERE n <= $1",
            )
            .bind(cap)
            .fetch_all(&self.pool)
            .await?;
            for d in followed {
                *out.entry(d).or_insert(0) |= REASON_FOLLOWED;
            }
        }
        if grows_on_interaction(policy) {
            // Members' replies and quotes come from their post references,
            // which exist in every public-indexing mode (public-indexing
            // design §4.1); `posts` holds none while it is off.
            let interacted: Vec<String> = sqlx::query_scalar(
                "SELECT DISTINCT d FROM ( \
                   SELECT split_part(l.subject_uri, '/', 3) AS d FROM likes l JOIN members m ON m.did = l.author_did \
                     WHERE l.subject_uri LIKE 'at://%' \
                   UNION ALL SELECT split_part(r.subject_uri, '/', 3) FROM reposts r JOIN members m ON m.did = r.author_did \
                     WHERE r.subject_uri LIKE 'at://%' \
                   UNION ALL SELECT p.reply_parent_did FROM member_post_refs p JOIN members m ON m.did = p.author_did \
                   UNION ALL SELECT p.reply_root_did FROM member_post_refs p JOIN members m ON m.did = p.author_did \
                   UNION ALL SELECT p.quote_did FROM member_post_refs p JOIN members m ON m.did = p.author_did \
                 ) t WHERE d IS NOT NULL",
            )
            .fetch_all(&self.pool)
            .await?;
            for d in interacted.into_iter().filter(|d| d.starts_with("did:")) {
                *out.entry(d).or_insert(0) |= REASON_INTERACTED;
            }
        }
        Ok(out)
    }

    /// Run a rebuild. `dry_run` computes and counts without writing.
    ///
    /// # Errors
    ///
    /// Database errors; purges already finished stay finished.
    pub async fn run(&self, dry_run: bool) -> Result<RebuildReport, sqlx::Error> {
        let started: DateTime<Utc> = sqlx::query_scalar("SELECT now()").fetch_one(&self.pool).await?;
        let justified = self.compute().await?;
        let mut report = RebuildReport {
            justified: justified.len(),
            ..RebuildReport::default()
        };
        if dry_run {
            let current: Vec<String> = sqlx::query_scalar("SELECT did FROM scope_dids").fetch_all(&self.pool).await?;
            report.added = justified.keys().filter(|d| !current.contains(d)).count();
            report.purged = self.expired(started).await?.len();
            return Ok(report);
        }

        // Step 2, in batches under shared DID locks.
        let entries: Vec<(String, i16)> = justified.into_iter().collect();
        for chunk in entries.chunks(UPSERT_BATCH) {
            let added = self.upsert_batch(chunk).await?;
            report.added += added.len();
            if !added.is_empty() {
                let mut sets = self.scope.write_guard().await;
                for d in added {
                    sets.scope.insert(d);
                }
            }
        }

        // Step 3: each expired DID is purged independently.
        for did in self.expired(started).await? {
            match self.purger.purge_waiting(&did, PurgeKind::OutOfScope).await {
                Ok(()) => report.purged += 1,
                Err(error) => tracing::warn!(event = "scope_purge_failed", did, %error, "out-of-scope purge failed"),
            }
        }

        // Step 4 and 5.
        self.repair_counts().await?;
        self.reload_follow_counts().await?;
        tracing::info!(
            event = "scope_rebuild",
            justified = report.justified,
            added = report.added,
            purged = report.purged,
            "scope rebuild finished"
        );
        Ok(report)
    }

    async fn upsert_batch(&self, chunk: &[(String, i16)]) -> Result<Vec<String>, sqlx::Error> {
        let dids: Vec<String> = chunk.iter().map(|(d, _)| d.clone()).collect();
        let reasons: Vec<i16> = chunk.iter().map(|(_, r)| *r).collect();
        let guard = self.scope.read_guard().await;
        let mut tx = self.pool.begin().await?;
        // Every write path that touches scope_dids takes the subject's
        // shared DID lock before inserting (§6.11.7 step 2).
        lock_dids_shared(&mut tx, &dids).await?;
        let rows: Vec<(String, bool)> = sqlx::query_as(
            "INSERT INTO scope_dids (did, reasons, justified_at) \
             SELECT d, r, now() FROM unnest($1::text[], $2::int2[]) AS t(d, r) \
             ON CONFLICT (did) DO UPDATE SET reasons = EXCLUDED.reasons, justified_at = now() \
             RETURNING did, (xmax = 0)",
        )
        .bind(&dids)
        .bind(&reasons)
        .fetch_all(&mut *tx)
        .await?;
        let added: Vec<String> = rows.into_iter().filter(|(_, new)| *new).map(|(d, _)| d).collect();
        crate::firehose::jobs::enqueue_many(&mut tx, &added, Depth::Scope).await?;
        tx.commit().await?;
        drop(guard);
        Ok(added)
    }

    /// DIDs not justified since `started` minus the grace, members excluded.
    async fn expired(&self, started: DateTime<Utc>) -> Result<Vec<String>, sqlx::Error> {
        let cutoff = started - TimeDelta::days(i64::from(self.grace_days));
        sqlx::query_scalar(
            "SELECT s.did FROM scope_dids s WHERE s.justified_at < $1 \
             AND NOT EXISTS (SELECT 1 FROM members m WHERE m.did = s.did) ORDER BY s.did",
        )
        .bind(cutoff)
        .fetch_all(&self.pool)
        .await
    }

    /// §9.5 repair; see [`repair_counts`].
    ///
    /// # Errors
    ///
    /// Database errors.
    pub async fn repair_counts(&self) -> Result<(), sqlx::Error> {
        repair_counts(&self.pool).await
    }

    async fn reload_follow_counts(&self) -> Result<(), sqlx::Error> {
        let rows: Vec<(String, i64)> = sqlx::query_as(
            "SELECT f.author_did, count(*) FROM follows f \
             WHERE EXISTS (SELECT 1 FROM members m WHERE m.did = f.author_did) GROUP BY f.author_did",
        )
        .fetch_all(&self.pool)
        .await?;
        let mut sets = self.scope.write_guard().await;
        sets.member_follows = rows
            .into_iter()
            .map(|(d, n)| (d, u32::try_from(n).unwrap_or(u32::MAX)))
            .collect();
        Ok(())
    }

    /// Enqueue `resync` for in-scope DIDs not re-synced for 7 days, spaced
    /// 1 s apart. A DID's last re-sync is its `sync_state.last_resync_at`
    /// (tracked DIDs) or its last completed `member`/`scope`/`resync` job.
    /// Jobs already queued or running are left alone.
    ///
    /// # Errors
    ///
    /// Database errors.
    pub async fn enqueue_stale_resyncs(&self) -> Result<u64, sqlx::Error> {
        Ok(sqlx::query(
            "INSERT INTO backfill_jobs (did, depth, state, attempts, not_before, updated_at) \
             SELECT did, 'resync', 'queued', 0, now() + make_interval(secs => (row_number() OVER (ORDER BY did))::float8), now() \
             FROM scope_dids s \
             WHERE NOT EXISTS (SELECT 1 FROM sync_state y WHERE y.did = s.did AND y.last_resync_at > now() - interval '7 days') \
               AND NOT EXISTS (SELECT 1 FROM backfill_jobs j WHERE j.did = s.did \
                   AND j.depth IN ('member', 'scope', 'resync') AND j.state = 'done' \
                   AND j.updated_at > now() - interval '7 days') \
               AND NOT EXISTS (SELECT 1 FROM purging_dids p WHERE p.did = s.did) \
             ON CONFLICT (did, depth) DO UPDATE SET state = 'queued', attempts = 0, \
               not_before = EXCLUDED.not_before, last_error = NULL, updated_at = now() \
             WHERE backfill_jobs.state NOT IN ('queued', 'running')",
        )
        .execute(&self.pool)
        .await?
        .rows_affected())
    }

    /// Hourly check; runs the rebuild and stale re-syncs when the last
    /// scheduled run is a week old (persisted, so restarts do not postpone it).
    #[must_use]
    pub fn spawn_weekly(self) -> JoinHandle<()> {
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(SCHEDULE_CHECK);
            loop {
                tick.tick().await;
                match self.due().await {
                    Ok(true) => {
                        match self.run(false).await {
                            Ok(_) => {}
                            Err(error) => {
                                tracing::warn!(event = "scope_rebuild_failed", %error, "weekly scope rebuild failed");
                                continue;
                            }
                        }
                        match self.enqueue_stale_resyncs().await {
                            Ok(n) => tracing::info!(event = "weekly_resync_enqueued", jobs = n, "stale re-syncs enqueued"),
                            Err(error) => tracing::warn!(event = "weekly_resync_failed", %error, "enqueueing re-syncs failed"),
                        }
                        if let Err(error) = self.mark_ran().await {
                            tracing::warn!(event = "scope_rebuild_mark_failed", %error, "recording the rebuild time failed");
                        }
                    }
                    Ok(false) => {}
                    Err(error) => tracing::warn!(event = "scope_rebuild_schedule_failed", %error, "schedule check failed"),
                }
            }
        })
    }

    async fn due(&self) -> Result<bool, sqlx::Error> {
        let last: Option<DateTime<Utc>> =
            sqlx::query_scalar("SELECT (value #>> '{}')::timestamptz FROM worker_state WHERE key = $1")
                .bind(KEY_LAST_REBUILD)
                .fetch_optional(&self.pool)
                .await?;
        match last {
            Some(t) => Ok(Utc::now() - t >= WEEK),
            None => {
                // First start: schedule a week from now instead of rebuilding
                // immediately.
                self.mark_ran().await?;
                Ok(false)
            }
        }
    }

    async fn mark_ran(&self) -> Result<(), sqlx::Error> {
        sqlx::query(
            "INSERT INTO worker_state (key, value, updated_at) VALUES ($1, to_jsonb(now()::text), now()) \
             ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value, updated_at = now()",
        )
        .bind(KEY_LAST_REBUILD)
        .execute(&self.pool)
        .await?;
        Ok(())
    }
}

/// §9.5 repair: recompute both count tables from the base tables. The
/// share-row-exclusive locks hold count-trigger writers for the duration,
/// so no increment is lost or doubled. A function of the pool, so the end
/// of a public-indexing fill runs it too (public-indexing design §8.5).
///
/// # Errors
///
/// Database errors.
pub async fn repair_counts(pool: &PgPool) -> Result<(), sqlx::Error> {
    let mut tx = pool.begin().await?;
    sqlx::query("LOCK TABLE post_counts, actor_counts IN SHARE ROW EXCLUSIVE MODE")
        .execute(&mut *tx)
        .await?;
    sqlx::query("DELETE FROM post_counts").execute(&mut *tx).await?;
    sqlx::query(
        "INSERT INTO post_counts (post_uri, like_count, repost_count, reply_count, quote_count) \
         SELECT u, sum(l)::int, sum(r)::int, sum(p)::int, sum(q)::int FROM ( \
           SELECT subject_uri AS u, 1 AS l, 0 AS r, 0 AS p, 0 AS q FROM likes \
           UNION ALL SELECT subject_uri, 0, 1, 0, 0 FROM reposts \
           UNION ALL SELECT reply_parent_uri, 0, 0, 1, 0 FROM posts WHERE reply_parent_uri IS NOT NULL \
           UNION ALL SELECT quote_uri, 0, 0, 0, 1 FROM posts WHERE quote_uri IS NOT NULL \
         ) t GROUP BY u",
    )
    .execute(&mut *tx)
    .await?;
    sqlx::query("DELETE FROM actor_counts").execute(&mut *tx).await?;
    sqlx::query(
        "INSERT INTO actor_counts (did, followers_count, follows_count, posts_count) \
         SELECT d, sum(fr)::int, sum(fs)::int, sum(p)::int FROM ( \
           SELECT subject_did AS d, 1 AS fr, 0 AS fs, 0 AS p FROM follows \
           UNION ALL SELECT author_did, 0, 1, 0 FROM follows \
           UNION ALL SELECT author_did, 0, 0, 1 FROM posts \
         ) t GROUP BY d",
    )
    .execute(&mut *tx)
    .await?;
    tx.commit().await
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::aggregations::ScopeSets;
    use crate::config::ScopePolicy;
    use crate::private_tier::DisabledPrivateTier;
    use crate::test_support::fresh_db;

    const MEMBER: &str = "did:plc:aaaaaaaaaaaaaaaaaaaaaaaa";
    const FOLLOWED: &str = "did:plc:bbbbbbbbbbbbbbbbbbbbbbbb";
    const LIKED: &str = "did:plc:cccccccccccccccccccccccc";
    const STALE: &str = "did:plc:dddddddddddddddddddddddd";

    async fn exec(pool: &PgPool, sql: &str) {
        sqlx::query(sql).execute(pool).await.expect(sql);
    }

    #[tokio::test]
    async fn rebuild_justifies_adds_purges_and_repairs() {
        let Some(db) = fresh_db("scope_rebuild").await else {
            return;
        };
        let p = &db.pool;
        exec(p, "INSERT INTO members (did, source) VALUES ('did:plc:aaaaaaaaaaaaaaaaaaaaaaaa', 'admin')").await;
        exec(p, "INSERT INTO follows (uri, author_did, subject_did, rev, created_at) VALUES \
                 ('at://did:plc:aaaaaaaaaaaaaaaaaaaaaaaa/app.bsky.graph.follow/1', 'did:plc:aaaaaaaaaaaaaaaaaaaaaaaa', \
                  'did:plc:bbbbbbbbbbbbbbbbbbbbbbbb', '3k', now())").await;
        exec(p, "INSERT INTO likes (uri, author_did, subject_uri, rev, created_at) VALUES \
                 ('at://did:plc:aaaaaaaaaaaaaaaaaaaaaaaa/app.bsky.feed.like/1', 'did:plc:aaaaaaaaaaaaaaaaaaaaaaaa', \
                  'at://did:plc:cccccccccccccccccccccccc/app.bsky.feed.post/1', '3k', now())").await;
        // STALE was justified long ago and nothing justifies it now.
        exec(p, "INSERT INTO scope_dids (did, reasons, justified_at) VALUES \
                 ('did:plc:dddddddddddddddddddddddd', 2, now() - interval '30 days')").await;
        // A corrupted count to be repaired.
        exec(p, "UPDATE post_counts SET like_count = 99").await;

        let mut sets = ScopeSets::default();
        sets.members.insert(MEMBER.into());
        sets.scope.insert(MEMBER.into());
        sets.scope.insert(STALE.into());
        let scope = ScopeState::new(sets, ScopePolicy::MembersAndOneHop, 10);
        let rebuild = Rebuild::new(p.clone(), scope.clone(), Arc::new(DisabledPrivateTier), 7);

        let computed = rebuild.compute().await.expect("compute");
        assert_eq!(computed.get(MEMBER), Some(&REASON_MEMBER));
        assert_eq!(computed.get(FOLLOWED), Some(&REASON_FOLLOWED));
        assert_eq!(computed.get(LIKED), Some(&REASON_INTERACTED));
        assert_eq!(computed.len(), 3);

        let dry = rebuild.run(true).await.expect("dry run");
        assert_eq!((dry.added, dry.purged), (3, 1));
        let rows: i64 = sqlx::query_scalar("SELECT count(*) FROM scope_dids").fetch_one(p).await.expect("count");
        assert_eq!(rows, 1, "a dry run writes nothing");

        let report = rebuild.run(false).await.expect("rebuild");
        assert_eq!((report.added, report.purged), (3, 1));
        let dids: Vec<String> = sqlx::query_scalar("SELECT did FROM scope_dids ORDER BY did")
            .fetch_all(p)
            .await
            .expect("dids");
        assert_eq!(dids, [MEMBER, FOLLOWED, LIKED]);
        let snap = scope.snapshot().await;
        assert!(snap.in_scope(FOLLOWED) && snap.in_scope(LIKED) && !snap.in_scope(STALE));
        assert_eq!(snap.member_follows.get(MEMBER), Some(&1));
        let jobs: i64 = sqlx::query_scalar("SELECT count(*) FROM backfill_jobs WHERE depth = 'scope'")
            .fetch_one(p)
            .await
            .expect("jobs");
        assert_eq!(jobs, 3);
        let likes: i32 = sqlx::query_scalar("SELECT like_count FROM post_counts WHERE post_uri = $1")
            .bind(format!("at://{LIKED}/app.bsky.feed.post/1"))
            .fetch_one(p)
            .await
            .expect("post_counts");
        assert_eq!(likes, 1);

        // Nobody has been re-synced: every in-scope DID gets a resync job.
        exec(p, "UPDATE backfill_jobs SET state = 'done', updated_at = now() - interval '8 days'").await;
        assert_eq!(rebuild.enqueue_stale_resyncs().await.expect("resync"), 3);
        db.drop_db().await;
    }

    /// T-17 (public-indexing design §4.1): under the off rule a member's
    /// reply, written grow-only, puts its subject in S, and the rebuild
    /// keeps it there past the grace period from the reference alone.
    #[tokio::test]
    async fn a_reply_justifies_its_subject_from_the_reference() {
        use serde_json::json;

        use crate::firehose::writer::{Action, ApplyOptions, OpOutcome, RawOp, WriteSource, Writer};
        use crate::indexing::{PublicMode, PublicState};

        let Some(db) = fresh_db("scope_rebuild_refs").await else {
            return;
        };
        let p = &db.pool;
        exec(p, "INSERT INTO members (did, source) VALUES ('did:plc:aaaaaaaaaaaaaaaaaaaaaaaa', 'admin')").await;
        let mut sets = ScopeSets::default();
        sets.members.insert(MEMBER.into());
        sets.scope.insert(MEMBER.into());
        let scope = ScopeState::new(sets, ScopePolicy::MembersAndOneHop, 10);
        {
            let mut s = scope.write_guard().await;
            scope.set_public(&mut s, PublicState { mode: PublicMode::Off, generation: 1, last_fill_since: None });
        }
        let writer = Writer::new(p.clone(), scope.clone(), Arc::new(DisabledPrivateTier));
        let parent = format!("at://{LIKED}/app.bsky.feed.post/1");
        let quoted = format!("at://{FOLLOWED}/app.bsky.feed.post/1");
        let reply = RawOp {
            action: Action::Create,
            collection: "app.bsky.feed.post".into(),
            rkey: "3kreply".into(),
            cid: Some("bafyreply".into()),
            record: Some(json!({"text": "hi", "reply": {"root": {"uri": parent, "cid": "c"}, "parent": {"uri": parent, "cid": "c"}},
                                "embed": {"$type": "app.bsky.embed.record", "record": {"uri": quoted, "cid": "c"}}})),
        };
        let options = ApplyOptions {
            advance_applied_rev: false,
            dead_letter_readiness: false,
            attempts: 1,
            live: true,
            subscription_rules: true,
            source: WriteSource::Firehose,
        };
        let report = writer.apply(MEMBER, "3kaaaaaaaaaaa", &[reply], options).await.expect("apply");
        assert_eq!(report.outcomes, [OpOutcome::GrowOnly]);
        let posts: i64 = sqlx::query_scalar("SELECT count(*) FROM posts").fetch_one(p).await.expect("count");
        assert_eq!(posts, 0);
        assert!(scope.snapshot().await.in_scope(LIKED));

        // Long past the grace period, only the reference justifies them.
        exec(p, "UPDATE scope_dids SET justified_at = now() - interval '30 days'").await;
        let rebuild = Rebuild::new(p.clone(), scope.clone(), Arc::new(DisabledPrivateTier), 7);
        let computed = rebuild.compute().await.expect("compute");
        assert_eq!(computed.get(LIKED), Some(&REASON_INTERACTED));
        assert_eq!(computed.get(FOLLOWED), Some(&REASON_INTERACTED));
        let report = rebuild.run(false).await.expect("rebuild");
        assert_eq!(report.purged, 0);
        let snap = scope.snapshot().await;
        assert!(snap.in_scope(LIKED) && snap.in_scope(FOLLOWED));
        // The mode survives a reload of the sets.
        scope.reload(p, &DisabledPrivateTier).await.expect("reload");
        assert_eq!(scope.snapshot().await.public.mode, PublicMode::Off);
        db.drop_db().await;
    }

    /// T-17: migration 0021 fills the references from members' stored
    /// posts, and the rebuild reads from them the DIDs it read from `posts`.
    #[tokio::test]
    async fn migration_0021_refs_match_the_old_query() {
        let Some(db) = crate::test_support::empty_db("migration_0021").await else {
            return;
        };
        let p = &db.pool;
        let migrations: Vec<_> = crate::db::migrations::MIGRATOR.iter().filter(|m| !m.migration_type.is_down_migration()).collect();
        let at = migrations.iter().position(|m| m.version == 21).expect("0021");
        for m in &migrations[..at] {
            sqlx::raw_sql(&m.sql).execute(p).await.unwrap_or_else(|e| panic!("{}: {e}", m.version));
        }
        exec(p, "INSERT INTO members (did, source) VALUES ('did:plc:aaaaaaaaaaaaaaaaaaaaaaaa', 'admin')").await;
        let post = |author: &str, rkey: &str, parent: Option<&str>, root: Option<&str>, quote: Option<&str>| {
            let q = |v: Option<&str>| v.map_or("NULL".to_string(), |u| format!("'{u}'"));
            format!(
                "INSERT INTO posts (uri, author_did, rkey, cid, rev, record, created_at, reply_parent_uri, reply_root_uri, quote_uri) VALUES \
                 ('at://{author}/app.bsky.feed.post/{rkey}', '{author}', '{rkey}', 'c', '3k{rkey}', '{{}}', now(), {}, {}, {})",
                q(parent),
                q(root),
                q(quote)
            )
        };
        let liked = format!("at://{LIKED}/app.bsky.feed.post/1");
        let followed = format!("at://{FOLLOWED}/app.bsky.feed.post/1");
        let stale = format!("at://{STALE}/app.bsky.feed.post/1");
        for sql in [
            post(MEMBER, "1", Some(&liked), Some(&followed), None),
            post(MEMBER, "2", None, None, Some(&stale)),
            post(MEMBER, "3", None, None, None),
            // A handle in a reference is kept as written, as before.
            post(MEMBER, "4", Some("at://alice.example/app.bsky.feed.post/1"), None, None),
            // Not a member's: no reference.
            post(STALE, "5", Some(&liked), None, None),
        ] {
            exec(p, &sql).await;
        }
        let old: Vec<String> = sqlx::query_scalar(
            "SELECT DISTINCT split_part(u, '/', 3) FROM ( \
               SELECT p.reply_parent_uri AS u FROM posts p JOIN members m ON m.did = p.author_did \
               UNION ALL SELECT p.reply_root_uri FROM posts p JOIN members m ON m.did = p.author_did \
               UNION ALL SELECT p.quote_uri FROM posts p JOIN members m ON m.did = p.author_did \
             ) t WHERE u LIKE 'at://%' ORDER BY 1",
        )
        .fetch_all(p)
        .await
        .expect("old");
        sqlx::raw_sql(&migrations[at].sql).execute(p).await.expect("0021 applies over existing rows");
        let new: Vec<String> = sqlx::query_scalar(
            "SELECT DISTINCT d FROM ( \
               SELECT p.reply_parent_did AS d FROM member_post_refs p JOIN members m ON m.did = p.author_did \
               UNION ALL SELECT p.reply_root_did FROM member_post_refs p JOIN members m ON m.did = p.author_did \
               UNION ALL SELECT p.quote_did FROM member_post_refs p JOIN members m ON m.did = p.author_did \
             ) t WHERE d IS NOT NULL ORDER BY 1",
        )
        .fetch_all(p)
        .await
        .expect("new");
        assert_eq!(new, old);
        assert_eq!(old.len(), 4);
        let refs: Vec<(String, String)> = sqlx::query_as("SELECT uri, rev FROM member_post_refs ORDER BY uri").fetch_all(p).await.expect("refs");
        assert_eq!(refs.len(), 4);
        assert!(refs.iter().all(|(u, r)| u.starts_with(&format!("at://{MEMBER}/")) && r.starts_with("3k")));
        db.drop_db().await;
    }
}
