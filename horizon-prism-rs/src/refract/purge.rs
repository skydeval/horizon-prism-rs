//! The Refract purge (refract design §6.2): a runner of its own, not a
//! `PurgeKind`. `refract_dids.state = 'ending'` is its marker; no
//! `purging_dids` row is written, so no member-wide purge effect runs.
//!
//! Each batch runs under the read guard and the DID's shared lock, 10,000
//! rows per statement, with the mode, S and membership read per batch, and
//! deletes only the exception rows (§5.1): what the restored rule admitted
//! that no other rule would admit now. Rows other authors wrote, members'
//! likes, follows, replies and quotes of the DID among them, are never
//! deleted (D-7). The end, in one transaction, deletes the DID's Refract
//! rows; then it leaves `refract_hidden`.

use std::time::Duration;

use sqlx::PgConnection;

use super::Refract;
use crate::db::locks::lock_dids_shared;

/// Rows per statement of a batch.
pub const BATCH: i64 = 10_000;
/// `refract_purge_rows_total{table}`.
pub const METRIC_ROWS: &str = "refract_purge_rows_total";
/// The runner's poll when nothing is ending.
pub const IDLE_POLL: Duration = Duration::from_secs(10);

/// The posts of `t` whose reply parent, root or quote author is a current
/// member (rule 2 keeps them in On and Filling).
const POST_NAMES_MEMBER: &str = "EXISTS (SELECT 1 FROM members m WHERE m.did IN (split_part(t.reply_parent_uri, '/', 3), \
     split_part(t.reply_root_uri, '/', 3), split_part(t.quote_uri, '/', 3)))";
const SUBJECT_AUTHOR_MEMBER: &str = "EXISTS (SELECT 1 FROM members m WHERE m.did = split_part(t.subject_uri, '/', 3))";
const FOLLOWS_MEMBER: &str = "EXISTS (SELECT 1 FROM members m WHERE m.did = t.subject_did)";

async fn delete(conn: &mut PgConnection, table: &str, key: &str, predicate: &str, did: &str) -> Result<u64, sqlx::Error> {
    let sql = format!("DELETE FROM {table} WHERE {key} IN (SELECT {key} FROM {table} t WHERE {predicate} LIMIT {BATCH})");
    let n = sqlx::query(&sql).bind(did).execute(&mut *conn).await?.rows_affected();
    if n > 0 {
        metrics::counter!(METRIC_ROWS, "table" => table.to_string()).increment(n);
    }
    Ok(n)
}

/// One batch of the deletes (§6.2), for `did` with these facts read under
/// this batch's guard.
async fn batch(conn: &mut PgConnection, did: &str, member: bool, in_s: bool, off: bool) -> Result<u64, sqlx::Error> {
    let mut n = 0;
    match (member, off) {
        // A member: its posts in off modes (its references stay).
        (true, true) => n += delete(conn, "posts", "uri", "t.author_did = $1", did).await?,
        // In On and Filling rule 1 keeps everything of a member or an S DID.
        (true, false) => {}
        (false, false) if in_s => {}
        (false, true) => {
            for (table, col) in [
                ("posts", "author_did"),
                ("likes", "author_did"),
                ("reposts", "author_did"),
                ("lists", "creator_did"),
                ("list_items", "author_did"),
                ("list_optouts", "author_did"),
                ("threadgates", "author_did"),
                ("postgates", "author_did"),
            ] {
                n += delete(conn, table, "uri", &format!("t.{col} = $1"), did).await?;
            }
            n += delete(conn, "follows", "uri", &format!("t.author_did = $1 AND NOT {FOLLOWS_MEMBER}"), did).await?;
            if !in_s {
                n += profile(conn, did).await?;
            }
        }
        (false, false) => {
            n += delete(conn, "posts", "uri", &format!("t.author_did = $1 AND NOT {POST_NAMES_MEMBER}"), did).await?;
            for table in ["likes", "reposts"] {
                n += delete(conn, table, "uri", &format!("t.author_did = $1 AND NOT {SUBJECT_AUTHOR_MEMBER}"), did).await?;
            }
            n += delete(conn, "follows", "uri", &format!("t.author_did = $1 AND NOT {FOLLOWS_MEMBER}"), did).await?;
            for (table, col) in [
                ("lists", "creator_did"),
                ("list_items", "author_did"),
                ("list_optouts", "author_did"),
                ("threadgates", "author_did"),
                ("postgates", "author_did"),
            ] {
                n += delete(conn, table, "uri", &format!("t.{col} = $1"), did).await?;
            }
            n += profile(conn, did).await?;
        }
    }
    // Always: the DID's Refract notifications, and its public ones whose
    // record no longer exists.
    n += delete(conn, "refract_notifications", "id", "t.author_did = $1", did).await?;
    n += delete(
        conn,
        "notifications",
        "id",
        "t.author_did = $1 AND NOT CASE split_part(t.record_uri, '/', 4) \
           WHEN 'app.bsky.feed.post' THEN EXISTS (SELECT 1 FROM posts x WHERE x.uri = t.record_uri) \
           WHEN 'app.bsky.feed.like' THEN EXISTS (SELECT 1 FROM likes x WHERE x.uri = t.record_uri) \
           WHEN 'app.bsky.feed.repost' THEN EXISTS (SELECT 1 FROM reposts x WHERE x.uri = t.record_uri) \
           WHEN 'app.bsky.graph.follow' THEN EXISTS (SELECT 1 FROM follows x WHERE x.uri = t.record_uri) \
           WHEN 'app.bsky.actor.profile' THEN EXISTS (SELECT 1 FROM actors x WHERE x.did = t.author_did AND x.profile_record IS NOT NULL) \
           ELSE true END",
        did,
    )
    .await?;
    Ok(n)
}

/// The self records and the profile columns of a DID outside S.
async fn profile(conn: &mut PgConnection, did: &str) -> Result<u64, sqlx::Error> {
    let mut n = delete(conn, "actor_self_records", "uri", "t.did = $1", did).await?;
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
    Ok(n)
}

/// Delete every Refract row of `did` (the `AccountDeleted` marker, §4.3),
/// inside the caller's transaction.
///
/// # Errors
///
/// Database errors.
pub async fn forget_account(conn: &mut PgConnection, did: &str) -> Result<(), sqlx::Error> {
    for sql in [
        "DELETE FROM refract_dids WHERE did = $1",
        "DELETE FROM refract_labels WHERE did = $1",
        "DELETE FROM refract_record_reads WHERE did = $1",
        "DELETE FROM refract_pending_dids WHERE did = $1",
        "DELETE FROM refract_notifications WHERE author_did = $1 OR recipient_did = $1",
    ] {
        sqlx::query(sql).bind(did).execute(&mut *conn).await?;
    }
    drop_empty_scopes(conn).await
}

async fn drop_empty_scopes(conn: &mut PgConnection) -> Result<(), sqlx::Error> {
    sqlx::query("DELETE FROM refract_pending p WHERE p.scope <> 'feature' AND NOT EXISTS (SELECT 1 FROM refract_pending_dids d WHERE d.scope = p.scope)")
        .execute(&mut *conn)
        .await?;
    Ok(())
}

impl Refract {
    /// Run the purge runner until aborted: every `ending` row, one DID at a
    /// time; at startup it resumes them all (there is no other marker).
    pub async fn run_purger(self) {
        loop {
            let worked = match self.purge_next().await {
                Ok(w) => w,
                Err(error) => {
                    tracing::warn!(event = "refract_purge_failed", %error, "a Refract purge batch failed; retried");
                    false
                }
            };
            if !worked {
                tokio::time::sleep(IDLE_POLL).await;
            }
        }
    }

    /// Purge the oldest `ending` DID not under a `purging_dids` row.
    /// Returns whether one was purged.
    ///
    /// # Errors
    ///
    /// Database errors.
    pub async fn purge_next(&self) -> Result<bool, sqlx::Error> {
        let did: Option<String> = sqlx::query_scalar(
            "SELECT d.did FROM refract_dids d WHERE d.state = 'ending' \
             AND NOT EXISTS (SELECT 1 FROM purging_dids p WHERE p.did = d.did) ORDER BY d.updated_at LIMIT 1",
        )
        .fetch_optional(&self.inner.pool)
        .await?;
        let Some(did) = did else {
            return Ok(false);
        };
        self.purge(&did).await
    }

    /// The batches and the end of one DID's Refract purge. Returns whether
    /// it ended (`false`: a `purging_dids` row appeared, so it waits).
    ///
    /// # Errors
    ///
    /// Database errors.
    pub async fn purge(&self, did: &str) -> Result<bool, sqlx::Error> {
        let mut total = 0;
        loop {
            let guard = self.inner.scope.read_guard().await;
            let (member, in_s, off) = (guard.is_member(did), guard.in_scope(did), guard.public.mode.off_rule());
            let mut tx = self.inner.pool.begin().await?;
            lock_dids_shared(&mut tx, &[did.to_string()]).await?;
            // Another purge of the DID: its batches pause until it ends.
            let other: bool = sqlx::query_scalar("SELECT EXISTS (SELECT 1 FROM purging_dids WHERE did = $1)").bind(did).fetch_one(&mut *tx).await?;
            if other {
                return Ok(false);
            }
            let ending: bool = sqlx::query_scalar("SELECT EXISTS (SELECT 1 FROM refract_dids WHERE did = $1 AND state = 'ending')").bind(did).fetch_one(&mut *tx).await?;
            if !ending {
                return Ok(false);
            }
            let n = batch(&mut tx, did, member, in_s, off).await?;
            tx.commit().await?;
            drop(guard);
            total += n;
            if n == 0 {
                break;
            }
        }
        // The end.
        let communities = self.communities();
        let mut sets = self.inner.scope.write_guard().await;
        let mut tx = self.inner.pool.begin().await?;
        let gone = sqlx::query("DELETE FROM refract_dids WHERE did = $1 AND state = 'ending'").bind(did).execute(&mut *tx).await?.rows_affected();
        if gone == 0 {
            tx.rollback().await?;
            return Ok(false);
        }
        for sql in [
            "DELETE FROM refract_labels WHERE did = $1",
            "DELETE FROM refract_pending_dids WHERE did = $1",
            "DELETE FROM refract_record_reads WHERE uri LIKE 'at://' || $1 || '/%' OR did = $1",
            "UPDATE backfill_jobs SET refract_cursor = NULL WHERE did = $1 AND depth = 'refract'",
            "INSERT INTO refract_events (did, from_state, to_state, reason) VALUES ($1, 'ending', 'ended', 'purged')",
        ] {
            sqlx::query(sql).bind(did).execute(&mut *tx).await?;
        }
        drop_empty_scopes(&mut tx).await?;
        tx.commit().await?;
        super::state::refresh(&self.inner.pool, &mut sets, Some(&[did.to_string()]), &communities).await?;
        drop(sets);
        tracing::info!(event = "refract_purge_ended", did, rows = total, "a Refract purge finished");
        Ok(true)
    }
}
