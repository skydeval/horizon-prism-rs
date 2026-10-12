//! Two-phase purges (design-v2 §6.11.7).
//!
//! 1. **Marker step**, one short transaction: the `ScopeState` write guard,
//!    then the DID's **exclusive** advisory lock (waiting out in-flight
//!    writers that hold its shared lock); insert `purging_dids`; delete the
//!    membership/scope rows the kind requires; clear readiness; cancel the
//!    DID's jobs. After commit the in-memory sets follow.
//! 2. **Batched deletion** of the kind's deletion set, 10 000 rows per
//!    transaction, each under a read guard and the DID's shared lock. Never
//!    per-URI locks, never under the write guard.
//! 3. **Purge end**, one transaction: owner re-evaluation, delete the
//!    `purging_dids` row, reset readiness-capable jobs, and enqueue `authz`
//!    if the DID is still tracked.
//!
//! A purge interrupted by a crash leaves `purging_dids` set; `serve` resumes
//! the batches at startup ([`Purger::resume_all`]).

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use sqlx::{PgConnection, PgPool};

use super::scope_state::{PurgeKind, ScopeState};
use crate::db::locks::{lock_did_exclusive, lock_dids_shared};
use crate::firehose::jobs::{self, Depth};
use crate::firehose::writer::SENTINEL_REV;
use crate::identity::AccountLifecycle;
use crate::private_tier::PrivateTier;
use crate::subscriptions::applier::Applier;
use crate::subscriptions::references::drop_member;
use crate::subscriptions::{jobs as list_sync, triggers};

/// Rows per deletion batch (§6.11.7).
pub const BATCH: i64 = 10_000;
/// Re-attempt interval for a marker step blocked by a running purge.
pub const OVERLAP_RETRY: Duration = Duration::from_secs(10);

/// A purge failure.
#[derive(Debug, thiserror::Error)]
pub enum PurgeError {
    #[error("database: {0}")]
    Db(#[from] sqlx::Error),
    /// Another purge of this DID is running (§6.11.7 overlapping purges).
    #[error("a {0} purge of this DID is already running")]
    Busy(&'static str),
}

/// The purge runner.
#[derive(Clone)]
pub struct Purger {
    pool: PgPool,
    scope: ScopeState,
    private: Arc<dyn PrivateTier>,
    /// `lists.sync_min_interval_secs`, for the list sync a purge end owes.
    sync_min_interval_secs: u32,
    /// Told when a purge removes a member's list references.
    applier: Option<Applier>,
}

impl Purger {
    #[must_use]
    pub fn new(pool: PgPool, scope: ScopeState, private: Arc<dyn PrivateTier>) -> Self {
        Self { pool, scope, private, sync_min_interval_secs: 600, applier: None }
    }

    /// Set the applier to tell when a purge removes a member's list
    /// references.
    #[must_use]
    pub fn with_applier(mut self, applier: Applier) -> Self {
        self.applier = Some(applier);
        self
    }

    /// Set `lists.sync_min_interval_secs`.
    #[must_use]
    pub fn with_sync_min_interval_secs(mut self, secs: u32) -> Self {
        self.sync_min_interval_secs = secs;
        self
    }

    /// Run a whole purge. A purge of the same kind already in progress is
    /// resumed; one of another kind is [`PurgeError::Busy`].
    ///
    /// # Errors
    ///
    /// Database errors (the purge resumes at the next attempt or startup).
    pub async fn purge(&self, did: &str, kind: PurgeKind) -> Result<(), PurgeError> {
        match self.marker(did, kind).await {
            Ok(()) => {}
            Err(PurgeError::Busy(running)) if running == kind.as_str() => {}
            Err(e) => return Err(e),
        }
        self.run_batches(did, kind).await?;
        self.end(did).await
    }

    /// Run a purge, re-attempting the marker every 10 s while another kind
    /// of purge of the DID runs (§6.11.7).
    ///
    /// # Errors
    ///
    /// Database errors.
    pub async fn purge_waiting(&self, did: &str, kind: PurgeKind) -> Result<(), PurgeError> {
        loop {
            match self.purge(did, kind).await {
                Err(PurgeError::Busy(_)) => tokio::time::sleep(OVERLAP_RETRY).await,
                other => return other,
            }
        }
    }

    /// The marker step.
    ///
    /// # Errors
    ///
    /// [`PurgeError::Busy`] when a purge of the DID is running; database errors.
    pub async fn marker(&self, did: &str, kind: PurgeKind) -> Result<(), PurgeError> {
        let mut sets = self.scope.write_guard().await;
        let mut tx = self.pool.begin().await?;
        lock_did_exclusive(&mut tx, did).await?;
        let running: Option<String> = sqlx::query_scalar("SELECT kind FROM purging_dids WHERE did = $1")
            .bind(did)
            .fetch_optional(&mut *tx)
            .await?;
        if let Some(running) = running {
            let running = PurgeKind::parse(&running).map_or("unknown", PurgeKind::as_str);
            return Err(PurgeError::Busy(running));
        }
        sqlx::query("INSERT INTO purging_dids (did, kind) VALUES ($1, $2)")
            .bind(did)
            .bind(kind.as_str())
            .execute(&mut *tx)
            .await?;
        match kind {
            PurgeKind::MemberRemoved => {
                sqlx::query("DELETE FROM members WHERE did = $1").bind(did).execute(&mut *tx).await?;
            }
            PurgeKind::AccountDeleted => {
                for table in ["members", "scope_dids", "sync_state"] {
                    sqlx::query(&format!("DELETE FROM {table} WHERE did = $1"))
                        .bind(did)
                        .execute(&mut *tx)
                        .await?;
                }
            }
            PurgeKind::OutOfScope => {
                sqlx::query("DELETE FROM scope_dids WHERE did = $1").bind(did).execute(&mut *tx).await?;
            }
        }
        // Rule 4 (§4.3): someone who stops being a member references no
        // list. The lists it referenced are marked, and the list phase
        // releases those nobody else references.
        let dropped = match kind {
            PurgeKind::MemberRemoved | PurgeKind::AccountDeleted => drop_member(&mut tx, did).await?,
            PurgeKind::OutOfScope => false,
        };
        // Readiness, for every kind.
        self.private.clear_owner_readiness(&mut tx, did).await?;
        sqlx::query("UPDATE members SET ready_at = NULL WHERE did = $1")
            .bind(did)
            .execute(&mut *tx)
            .await?;
        sqlx::query(
            "UPDATE backfill_jobs SET state = 'cancelled', updated_at = now() \
             WHERE did = $1 AND state IN ('queued', 'running')",
        )
        .bind(did)
        .execute(&mut *tx)
        .await?;
        tx.commit().await?;

        sets.purging.insert(did.to_string(), kind);
        match kind {
            PurgeKind::AccountDeleted => {
                sets.members.remove(did);
                sets.scope.remove(did);
                sets.private_owners.remove(did);
                sets.member_follows.remove(did);
            }
            PurgeKind::MemberRemoved => {
                sets.members.remove(did);
                sets.member_follows.remove(did);
            }
            PurgeKind::OutOfScope => {
                sets.scope.remove(did);
                sets.private_owners.remove(did);
            }
        }
        drop(sets);
        if let Some(applier) = &self.applier {
            // What waited on a sync for this member waits no longer.
            if kind != PurgeKind::OutOfScope {
                applier.forget_member(did);
            }
            if dropped {
                applier.signal();
            }
        }
        tracing::info!(event = "purge_marked", did, kind = kind.as_str(), "purge marker committed");
        Ok(())
    }

    /// Delete the kind's deletion set in batches until nothing is left.
    ///
    /// # Errors
    ///
    /// Database errors.
    pub async fn run_batches(&self, did: &str, kind: PurgeKind) -> Result<u64, PurgeError> {
        let mut total = 0;
        loop {
            let guard = self.scope.read_guard().await;
            let owners: Vec<String> = guard.private_owners.iter().cloned().collect();
            let mut tx = self.pool.begin().await?;
            lock_dids_shared(&mut tx, &[did.to_string()]).await?;
            let n = self.batch(&mut tx, did, kind, &owners).await?;
            tx.commit().await?;
            drop(guard);
            total += n;
            if n == 0 {
                break;
            }
        }
        tracing::info!(event = "purge_batches_done", did, kind = kind.as_str(), rows = total, "purge deletion finished");
        Ok(total)
    }

    /// One batch: up to [`BATCH`] rows per statement, returning the rows
    /// deleted by this transaction.
    async fn batch(
        &self,
        conn: &mut PgConnection,
        did: &str,
        kind: PurgeKind,
        owners: &[String],
    ) -> Result<u64, sqlx::Error> {
        let mut n = 0;
        match kind {
            PurgeKind::AccountDeleted => {
                for (table, column) in [
                    ("feed_items", "originator_did"),
                    ("posts", "author_did"),
                    ("likes", "author_did"),
                    ("reposts", "author_did"),
                    ("follows", "author_did"),
                    ("blocks", "author_did"),
                    // The record tables design-v3 added (§3.1).
                    ("lists", "creator_did"),
                    ("list_items", "author_did"),
                    ("list_blocks", "author_did"),
                    ("list_optouts", "author_did"),
                    ("starter_packs", "creator_did"),
                    ("labeler_services", "creator_did"),
                    ("actor_self_records", "did"),
                    ("threadgates", "author_did"),
                    ("postgates", "author_did"),
                    ("nearhorizon_poll_posts", "author_did"),
                ] {
                    n += delete_batch(conn, table, "uri", &format!("{column} = $1"), did).await?;
                }
                // Derived counts follow their pack row.
                n += delete_batch(conn, "starter_pack_counts", "pack_uri", "split_part(pack_uri, '/', 3) = $1", did).await?;
                n += delete_batch(conn, "notifications", "id", "author_did = $1 OR recipient_did = $1", did).await?;
                n += delete_batch(conn, "mutes", "ctid", "viewer_did = $1", did).await?;
                n += viewer_private_batch(conn, did).await?;
                // Subscriptions others hold on the deleted account go too.
                n += delete_batch(conn, "activity_subscriptions", "ctid", "subject_did = $1", did).await?;
                for table in ["notification_seen", "actor_counts"] {
                    n += sqlx::query(&format!("DELETE FROM {table} WHERE did = $1"))
                        .bind(did)
                        .execute(&mut *conn)
                        .await?
                        .rows_affected();
                }
                // The identity row stays (account_status = 'deleted'); its
                // authored profile goes.
                n += sqlx::query(
                    "UPDATE actors SET display_name = NULL, description = NULL, avatar_cid = NULL, \
                     banner_cid = NULL, profile_record = NULL, profile_rev = NULL, profile_cid = NULL, \
                     pronouns = NULL, website = NULL, joined_via_starter_pack_uri = NULL, \
                     joined_via_starter_pack_cid = NULL, profile_created_at = NULL \
                     WHERE did = $1 AND profile_record IS NOT NULL",
                )
                .bind(did)
                .execute(&mut *conn)
                .await?
                .rows_affected();
                n += self.private.purge_did_batch(conn, did, BATCH).await?;
            }
            PurgeKind::OutOfScope => {
                n += self.private.purge_out_of_scope_batch(conn, did, BATCH).await?;
            }
            PurgeKind::MemberRemoved => {
                n += delete_batch(conn, "mutes", "ctid", "viewer_did = $1", did).await?;
                n += viewer_private_batch(conn, did).await?;
                n += sqlx::query("DELETE FROM notification_seen WHERE did = $1")
                    .bind(did)
                    .execute(&mut *conn)
                    .await?
                    .rows_affected();
                n += delete_batch(conn, "notifications", "id", "recipient_did = $1", did).await?;
                n += member_removed_public_batch(conn, did, owners).await?;
                n += self.private.purge_member_removed_batch(conn, did, BATCH).await?;
            }
        }
        Ok(n)
    }

    /// Purge end (§6.11.7).
    ///
    /// # Errors
    ///
    /// Database errors.
    pub async fn end(&self, did: &str) -> Result<(), PurgeError> {
        let guard = self.scope.read_guard().await;
        let was_owner = guard.is_owner(did);
        let mut tx = self.pool.begin().await?;
        let applied = self.private.reevaluate_owner(&mut tx, did).await?;
        sqlx::query("DELETE FROM purging_dids WHERE did = $1")
            .bind(did)
            .execute(&mut *tx)
            .await?;
        // Rule 4 (§4.3): while the marker stood nothing the DID did to its
        // lists was copied, so a DID that still owns a subscribed list is
        // owed a list sync. Its owner row is taken before any job row.
        triggers::request_if_active(&mut tx, did, list_sync::LOW, self.sync_min_interval_secs).await?;
        sqlx::query(
            "UPDATE backfill_jobs SET state = 'queued', not_before = now(), updated_at = now() \
             WHERE did = $1 AND depth IN ('member', 'private', 'authz') AND state IN ('queued', 'running')",
        )
        .bind(did)
        .execute(&mut *tx)
        .await?;
        let is_member: bool = sqlx::query_scalar("SELECT EXISTS (SELECT 1 FROM members WHERE did = $1)")
            .bind(did)
            .fetch_one(&mut *tx)
            .await?;
        let is_owner = (was_owner || applied.owners_joined.iter().any(|d| d == did))
            && !applied.owners_left.iter().any(|d| d == did);
        if is_member || is_owner {
            jobs::enqueue(&mut tx, did, Depth::Authz).await?;
        }
        tx.commit().await?;
        drop(guard);

        let mut sets = self.scope.write_guard().await;
        sets.purging.remove(did);
        for d in applied.owners_joined {
            sets.private_owners.insert(d);
        }
        for d in &applied.owners_left {
            sets.private_owners.remove(d);
        }
        drop(sets);
        tracing::info!(event = "purge_ended", did, tracked = is_member || is_owner, "purge finished");
        Ok(())
    }

    /// Resume every purge left in `purging_dids` (startup).
    ///
    /// # Errors
    ///
    /// Database errors reading the list; individual purges log and continue.
    pub async fn resume_all(&self) -> Result<usize, sqlx::Error> {
        let rows: Vec<(String, String)> = sqlx::query_as("SELECT did, kind FROM purging_dids ORDER BY started_at")
            .fetch_all(&self.pool)
            .await?;
        let mut resumed = 0;
        for (did, kind) in rows {
            let Some(kind) = PurgeKind::parse(&kind) else {
                continue;
            };
            let result = async {
                self.run_batches(&did, kind).await?;
                self.end(&did).await
            }
            .await;
            match result {
                Ok(()) => resumed += 1,
                Err(error) => tracing::warn!(event = "purge_resume_failed", did, %error, "resuming purge failed"),
            }
        }
        Ok(resumed)
    }
}

/// One batch over the AppView-private tables design-v3 added (§3.1): state a
/// member's own calls wrote, deleted by the `member_removed` and
/// `account_deleted` purges.
async fn viewer_private_batch(conn: &mut PgConnection, did: &str) -> Result<u64, sqlx::Error> {
    let mut n = 0;
    for (table, predicate) in [
        ("list_mutes", "viewer_did = $1"),
        ("thread_mutes", "viewer_did = $1"),
        ("bookmarks", "viewer_did = $1"),
        ("drafts", "owner_did = $1"),
        ("activity_subscriptions", "subscriber_did = $1"),
        ("notification_preferences_v2", "did = $1"),
    ] {
        n += delete_batch(conn, table, "ctid", predicate, did).await?;
    }
    Ok(n)
}

/// `DELETE FROM table WHERE key IN (SELECT key … LIMIT BATCH)`.
async fn delete_batch(
    conn: &mut PgConnection,
    table: &str,
    key: &str,
    predicate: &str,
    did: &str,
) -> Result<u64, sqlx::Error> {
    let sql = format!(
        "DELETE FROM {table} WHERE {key} IN (SELECT {key} FROM {table} WHERE {predicate} LIMIT {BATCH})"
    );
    Ok(sqlx::query(&sql).bind(did).execute(conn).await?.rows_affected())
}

/// Class-M public rows admitted only because `did` was a member (§6.11.7):
/// rule-2 interactions on its content by authors outside S, and inbound-block
/// sentinel rows. A post that also names another current member as parent,
/// root or quote author stays.
async fn member_removed_public_batch(conn: &mut PgConnection, did: &str, owners: &[String]) -> Result<u64, sqlx::Error> {
    const AUTHOR_OF: &str = "split_part(%s, '/', 3)";
    let author_of = |col: &str| AUTHOR_OF.replace("%s", col);
    let out_of_scope = "NOT EXISTS (SELECT 1 FROM scope_dids s WHERE s.did = t.author_did)";
    let mut n = 0;
    for table in ["likes", "reposts"] {
        let sql = format!(
            "DELETE FROM {table} WHERE uri IN (SELECT uri FROM {table} t \
             WHERE {} = $1 AND {out_of_scope} LIMIT {BATCH})",
            author_of("t.subject_uri")
        );
        n += sqlx::query(&sql).bind(did).execute(&mut *conn).await?.rows_affected();
    }
    let other_member = |col: &str| {
        format!(
            "EXISTS (SELECT 1 FROM members m WHERE m.did = {} AND m.did <> $1)",
            author_of(col)
        )
    };
    let sql = format!(
        "DELETE FROM posts WHERE uri IN (SELECT uri FROM posts t WHERE {out_of_scope} \
         AND ({p} = $1 OR {r} = $1 OR {q} = $1) \
         AND NOT ({op} OR {or} OR {oq}) LIMIT {BATCH})",
        p = author_of("t.reply_parent_uri"),
        r = author_of("t.reply_root_uri"),
        q = author_of("t.quote_uri"),
        op = other_member("t.reply_parent_uri"),
        or = other_member("t.reply_root_uri"),
        oq = other_member("t.quote_uri"),
    );
    n += sqlx::query(&sql).bind(did).execute(&mut *conn).await?.rows_affected();
    let sql = format!(
        "DELETE FROM follows WHERE uri IN (SELECT uri FROM follows t WHERE subject_did = $1 AND {out_of_scope} LIMIT {BATCH})"
    );
    n += sqlx::query(&sql).bind(did).execute(&mut *conn).await?.rows_affected();
    // Blocks: rule-2 rows (unless the author is a private owner, rule 3) and
    // every inbound-block sentinel row for this member.
    let sql = format!(
        "DELETE FROM blocks WHERE uri IN (SELECT uri FROM blocks t WHERE subject_did = $1 \
         AND (t.rev = $2 OR ({out_of_scope} AND NOT (t.author_did = ANY($3)))) LIMIT {BATCH})"
    );
    n += sqlx::query(&sql)
        .bind(did)
        .bind(SENTINEL_REV)
        .bind(owners)
        .execute(&mut *conn)
        .await?
        .rows_affected();
    Ok(n)
}

/// The identity refresher's `deleted` hook: the `account_deleted` purge.
pub struct PurgeLifecycle {
    purger: Purger,
}

impl PurgeLifecycle {
    #[must_use]
    pub fn new(purger: Purger) -> Self {
        Self { purger }
    }
}

#[async_trait]
impl AccountLifecycle for PurgeLifecycle {
    async fn account_deleted(&self, did: &str) -> Result<(), String> {
        // Busy (another kind running) is returned as an error so the
        // refresher retries with its backoff instead of blocking.
        self.purger
            .purge(did, PurgeKind::AccountDeleted)
            .await
            .map_err(|e| e.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::aggregations::ScopeSets;
    use crate::config::ScopePolicy;
    use crate::private_tier::DisabledPrivateTier;
    use crate::test_support::fresh_db;

    const MEMBER: &str = "did:plc:aaaaaaaaaaaaaaaaaaaaaaaa";
    const OTHER_MEMBER: &str = "did:plc:bbbbbbbbbbbbbbbbbbbbbbbb";
    const OUTSIDER: &str = "did:plc:cccccccccccccccccccccccc";
    const SCOPED: &str = "did:plc:dddddddddddddddddddddddd";

    async fn exec(pool: &PgPool, sql: &str) {
        sqlx::query(sql).execute(pool).await.expect(sql);
    }

    fn post_uri(did: &str, rkey: &str) -> String {
        format!("at://{did}/app.bsky.feed.post/{rkey}")
    }

    async fn count(pool: &PgPool, sql: &str) -> i64 {
        sqlx::query_scalar(sql).fetch_one(pool).await.expect(sql)
    }

    #[tokio::test]
    async fn member_removed_purges_only_what_membership_admitted() {
        let Some(db) = fresh_db("purge_member_removed").await else {
            return;
        };
        let p = &db.pool;
        for (did, member) in [(MEMBER, true), (OTHER_MEMBER, true), (SCOPED, false)] {
            sqlx::query("INSERT INTO scope_dids (did, reasons) VALUES ($1, 1)")
                .bind(did)
                .execute(p)
                .await
                .expect("scope");
            if member {
                sqlx::query("INSERT INTO members (did, source, ready_at) VALUES ($1, 'admin', now())")
                    .bind(did)
                    .execute(p)
                    .await
                    .expect("member");
            }
        }
        let insert_like = |uri: String, author: &'static str, subject: String| async move {
            sqlx::query(
                "INSERT INTO likes (uri, author_did, subject_uri, rev, created_at) VALUES ($1, $2, $3, '3kaaaaaaaaaaa', now())",
            )
            .bind(uri)
            .bind(author)
            .bind(subject)
            .execute(p)
            .await
            .expect("like");
        };
        // An outsider's like of the member's post (class M) goes; an in-scope
        // author's like stays.
        insert_like(format!("at://{OUTSIDER}/app.bsky.feed.like/1"), OUTSIDER, post_uri(MEMBER, "1")).await;
        insert_like(format!("at://{SCOPED}/app.bsky.feed.like/1"), SCOPED, post_uri(MEMBER, "1")).await;
        // An outsider reply whose root author is another member stays.
        for (rkey, root) in [("1", MEMBER), ("2", OTHER_MEMBER)] {
            sqlx::query(
                "INSERT INTO posts (uri, author_did, rkey, cid, rev, reply_parent_uri, reply_root_uri, record, created_at) \
                 VALUES ($1, $2, $3, 'c', '3kaaaaaaaaaaa', $4, $5, '{}'::jsonb, now())",
            )
            .bind(post_uri(OUTSIDER, rkey))
            .bind(OUTSIDER)
            .bind(rkey)
            .bind(post_uri(MEMBER, "1"))
            .bind(post_uri(root, "1"))
            .execute(p)
            .await
            .expect("post");
        }
        // A sentinel inbound block by an in-scope author goes; a real one stays.
        for (rkey, rev) in [("1", SENTINEL_REV), ("2", "3kaaaaaaaaaaa")] {
            sqlx::query(
                "INSERT INTO blocks (uri, author_did, subject_did, rev, created_at) VALUES ($1, $2, $3, $4, now())",
            )
            .bind(format!("at://{SCOPED}/app.bsky.graph.block/{rkey}"))
            .bind(SCOPED)
            .bind(MEMBER)
            .bind(rev)
            .execute(p)
            .await
            .expect("block");
        }
        exec(p, "INSERT INTO notification_seen (did, seen_at) VALUES ('did:plc:aaaaaaaaaaaaaaaaaaaaaaaa', now())").await;
        exec(p, "INSERT INTO backfill_jobs (did, depth, state) VALUES ('did:plc:aaaaaaaaaaaaaaaaaaaaaaaa', 'resync', 'queued')").await;
        // The member's own AppView-private state goes; another member's
        // stays. Its public records (a list) stay too: only membership ends.
        for viewer in [MEMBER, OTHER_MEMBER] {
            for sql in [
                "INSERT INTO list_mutes (viewer_did, list_uri) VALUES ($1, 'at://x/app.bsky.graph.list/1')",
                "INSERT INTO thread_mutes (viewer_did, root_uri) VALUES ($1, 'at://x/app.bsky.feed.post/1')",
                "INSERT INTO bookmarks (viewer_did, post_uri, post_cid) VALUES ($1, 'at://x/app.bsky.feed.post/1', 'c')",
                "INSERT INTO drafts (owner_did, id, draft) VALUES ($1, '3k', '{}')",
                "INSERT INTO activity_subscriptions (subscriber_did, subject_did, post, reply) VALUES ($1, 'did:plc:x', true, false)",
                "INSERT INTO notification_preferences_v2 (did, prefs) VALUES ($1, '{}')",
                "INSERT INTO lists (uri, creator_did, rkey, cid, rev, name, purpose, record, created_at) \
                 VALUES ('at://' || $1 || '/app.bsky.graph.list/1', $1, '1', 'c', '3k', 'n', 'p', '{}', now())",
            ] {
                sqlx::query(sql).bind(viewer).execute(p).await.expect(sql);
            }
        }

        let mut sets = ScopeSets::default();
        for d in [MEMBER, OTHER_MEMBER] {
            sets.members.insert(d.into());
        }
        for d in [MEMBER, OTHER_MEMBER, SCOPED] {
            sets.scope.insert(d.into());
        }
        let scope = ScopeState::new(sets, ScopePolicy::MembersAndOneHop, 10);
        let purger = Purger::new(p.clone(), scope.clone(), Arc::new(DisabledPrivateTier));

        purger.marker(MEMBER, PurgeKind::MemberRemoved).await.expect("marker");
        assert!(matches!(
            purger.marker(MEMBER, PurgeKind::OutOfScope).await,
            Err(PurgeError::Busy("member_removed"))
        ));
        let snap = scope.snapshot().await;
        assert!(!snap.is_member(MEMBER));
        assert_eq!(snap.purge_kind(MEMBER), Some(PurgeKind::MemberRemoved));
        assert_eq!(
            count(p, "SELECT count(*) FROM backfill_jobs WHERE state = 'cancelled'").await,
            1
        );

        purger.run_batches(MEMBER, PurgeKind::MemberRemoved).await.expect("batches");
        assert_eq!(count(p, "SELECT count(*) FROM likes").await, 1);
        assert_eq!(count(p, "SELECT count(*) FROM likes WHERE author_did = 'did:plc:dddddddddddddddddddddddd'").await, 1);
        assert_eq!(count(p, "SELECT count(*) FROM posts").await, 1);
        assert_eq!(count(p, "SELECT count(*) FROM blocks").await, 1);
        assert_eq!(count(p, "SELECT count(*) FROM blocks WHERE rev = '3kaaaaaaaaaaa'").await, 1);
        assert_eq!(count(p, "SELECT count(*) FROM notification_seen").await, 0);
        for table in ["list_mutes", "thread_mutes", "bookmarks", "drafts", "activity_subscriptions", "notification_preferences_v2"] {
            assert_eq!(count(p, &format!("SELECT count(*) FROM {table}")).await, 1, "{table}: only the other member's row is left");
        }
        assert_eq!(count(p, "SELECT count(*) FROM lists").await, 2);

        purger.end(MEMBER).await.expect("end");
        assert_eq!(count(p, "SELECT count(*) FROM purging_dids").await, 0);
        assert_eq!(scope.snapshot().await.purge_kind(MEMBER), None);
        // Not tracked any more: no authz job.
        assert_eq!(
            count(p, "SELECT count(*) FROM backfill_jobs WHERE depth = 'authz'").await,
            0
        );
        db.drop_db().await;
    }

    #[tokio::test]
    async fn account_deleted_removes_everything_authored_and_resumes_after_a_crash() {
        let Some(db) = fresh_db("purge_account_deleted").await else {
            return;
        };
        let p = &db.pool;
        exec(p, "INSERT INTO scope_dids (did, reasons) VALUES ('did:plc:dddddddddddddddddddddddd', 2)").await;
        exec(p, "INSERT INTO actors (did, display_name, profile_record, profile_rev) VALUES ('did:plc:dddddddddddddddddddddddd', 'D', '{}'::jsonb, '3k')").await;
        sqlx::query(
            "INSERT INTO posts (uri, author_did, rkey, cid, rev, record, created_at) VALUES ($1, $2, '1', 'c', '3k', '{}'::jsonb, now())",
        )
        .bind(post_uri(SCOPED, "1"))
        .bind(SCOPED)
        .execute(p)
        .await
        .expect("post");
        // Every v3 record table, a pack's derived counts, the account's own
        // private state, and a subscription someone else holds on it.
        let authored = [
            "INSERT INTO lists (uri, creator_did, rkey, cid, rev, name, purpose, record, created_at) VALUES ('at://' || $1 || '/l/1', $1, '1', 'c', '3k', 'n', 'p', '{}', now())",
            "INSERT INTO list_items (uri, author_did, rkey, cid, rev, list_uri, subject_did, created_at) VALUES ('at://' || $1 || '/li/1', $1, '1', 'c', '3k', 'l', 'did:plc:x', now())",
            "INSERT INTO list_blocks (uri, author_did, rkey, cid, rev, list_uri, created_at) VALUES ('at://' || $1 || '/lb/1', $1, '1', 'c', '3k', 'l', now())",
            "INSERT INTO list_optouts (uri, author_did, rkey, cid, rev, list_uri, created_at) VALUES ('at://' || $1 || '/lo/1', $1, '1', 'c', '3k', 'l', now())",
            "INSERT INTO starter_packs (uri, creator_did, rkey, cid, rev, name, list_uri, record, created_at) VALUES ('at://' || $1 || '/sp/1', $1, '1', 'c', '3k', 'n', 'l', '{}', now())",
            "INSERT INTO starter_pack_counts (pack_uri, all_time_source, refreshed_at) VALUES ('at://' || $1 || '/sp/1', 'local', now())",
            "INSERT INTO labeler_services (uri, creator_did, cid, rev, policies, record, created_at) VALUES ('at://' || $1 || '/ls/self', $1, 'c', '3k', '{}', '{}', now())",
            "INSERT INTO actor_self_records (uri, did, collection, cid, rev, record) VALUES ('at://' || $1 || '/s/self', $1, 's', 'c', '3k', '{}')",
            "INSERT INTO threadgates (uri, author_did, rkey, cid, rev, post_uri, record, created_at) VALUES ('at://' || $1 || '/tg/1', $1, '1', 'c', '3k', 'p', '{}', now())",
            "INSERT INTO postgates (uri, author_did, rkey, cid, rev, post_uri, record, created_at) VALUES ('at://' || $1 || '/pg/1', $1, '1', 'c', '3k', 'p', '{}', now())",
            "INSERT INTO nearhorizon_poll_posts (uri, author_did, rkey, cid, rev, text, options, visibility, record, created_at) VALUES ('at://' || $1 || '/pp/1', $1, '1', 'c', '3k', 't', '[]', 'public', '{}', now())",
            "INSERT INTO bookmarks (viewer_did, post_uri, post_cid) VALUES ($1, 'at://x/app.bsky.feed.post/1', 'c')",
            "INSERT INTO activity_subscriptions (subscriber_did, subject_did, post, reply) VALUES ('did:plc:x', $1, true, false)",
        ];
        for sql in authored {
            sqlx::query(sql).bind(SCOPED).execute(p).await.expect(sql);
        }
        let mut sets = ScopeSets::default();
        sets.scope.insert(SCOPED.into());
        let scope = ScopeState::new(sets, ScopePolicy::MembersAndOneHop, 10);
        let purger = Purger::new(p.clone(), scope.clone(), Arc::new(DisabledPrivateTier));

        // Crash after the marker: resume finishes the purge.
        purger.marker(SCOPED, PurgeKind::AccountDeleted).await.expect("marker");
        assert!(!scope.snapshot().await.in_scope(SCOPED));
        assert_eq!(count(p, "SELECT count(*) FROM scope_dids").await, 0);
        assert_eq!(purger.resume_all().await.expect("resume"), 1);
        assert_eq!(count(p, "SELECT count(*) FROM posts").await, 0);
        assert_eq!(count(p, "SELECT count(*) FROM purging_dids").await, 0);
        assert_eq!(count(p, "SELECT count(*) FROM actors WHERE profile_record IS NULL").await, 1);
        for table in [
            "lists",
            "list_items",
            "list_blocks",
            "list_optouts",
            "starter_packs",
            "starter_pack_counts",
            "labeler_services",
            "actor_self_records",
            "threadgates",
            "postgates",
            "nearhorizon_poll_posts",
            "bookmarks",
            "activity_subscriptions",
        ] {
            assert_eq!(count(p, &format!("SELECT count(*) FROM {table}")).await, 0, "{table}");
        }

        // Idempotent through the lifecycle hook.
        PurgeLifecycle::new(purger).account_deleted(SCOPED).await.expect("again");
        db.drop_db().await;
    }

    /// Rule 4: the purge cancels the owner's list sync, and its end
    /// requests one when the owner still has a subscribed list.
    #[tokio::test]
    async fn purge_end_requests_a_list_sync_for_an_owner_with_a_subscribed_list() {
        use crate::subscriptions::list_phase;
        use crate::subscriptions::references::{Budget, mute_list};

        let Some(db) = fresh_db("purge_list_sync").await else {
            return;
        };
        let p = &db.pool;
        let list = format!("at://{SCOPED}/app.bsky.graph.list/3klist1");
        mute_list(p, MEMBER, &list, Budget { max_references: 5, max_owners: 5 }).await.expect("mute");
        list_phase::apply(p, &list, 600).await.expect("apply");
        let scope = ScopeState::new(ScopeSets::default(), ScopePolicy::MembersAndOneHop, 10);
        let purger = Purger::new(p.clone(), scope, Arc::new(DisabledPrivateTier));

        purger.marker(SCOPED, PurgeKind::AccountDeleted).await.expect("marker");
        assert_eq!(count(p, "SELECT count(*) FROM backfill_jobs WHERE depth = 'list_sync' AND state = 'cancelled'").await, 1);
        assert_eq!(purger.resume_all().await.expect("resume"), 1);
        assert_eq!(count(p, "SELECT count(*) FROM backfill_jobs WHERE depth = 'list_sync' AND state = 'queued'").await, 1);
        assert_eq!(count(p, "SELECT count(*) FROM subscribed_owners WHERE owner_did = 'did:plc:dddddddddddddddddddddddd' AND resync_requested_at IS NOT NULL").await, 1);

        // An account with no subscribed list is owed nothing.
        purger.purge(OUTSIDER, PurgeKind::AccountDeleted).await.expect("purge");
        assert_eq!(count(p, "SELECT count(*) FROM backfill_jobs WHERE did = 'did:plc:cccccccccccccccccccccccc'").await, 0);
        assert_eq!(count(p, "SELECT count(*) FROM subscribed_owners").await, 1);

        // A removed member references no list any more; the list it
        // referenced is marked for the list phase.
        exec(p, "DELETE FROM list_reference_changes").await;
        purger.purge(MEMBER, PurgeKind::MemberRemoved).await.expect("purge");
        assert_eq!(count(p, "SELECT count(*) FROM list_references").await, 0);
        assert_eq!(count(p, "SELECT count(*) FROM list_reference_changes").await, 1);
        db.drop_db().await;
    }
}
