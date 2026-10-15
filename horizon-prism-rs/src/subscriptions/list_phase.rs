//! The list phase and the release of a list nobody references
//! (design-v3 §4.3 "Member phase and list phase", "Release").
//!
//! The member phase leaves a change marker for each list whose admitted
//! references changed. The list phase reads the markers and, per list, in
//! one transaction:
//! 1. reads the list's markers (no lock);
//! 2. takes the list's `subscribed_lists` row without waiting, creates it
//!    when an admitted reference exists and there is none, and sets its
//!    state: `active` while an admitted reference exists, `unreferenced`
//!    otherwise;
//! 3. takes the owner's `subscribed_owners` row, creating it if missing;
//! 4. enqueues the owner's `list_sync` when a reference waits on one;
//! 5. deletes exactly the markers it read.
//!
//! It never writes `list_references` and never waits on a list row, so no
//! member's request can wait on it and one busy list cannot hold up
//! another.
//!
//! An `unreferenced` list is released at once: its items are deleted in
//! batches, then its row. There is no grace period.

use std::time::Instant;

use sqlx::PgPool;

use super::applier::Waiting;
use super::jobs;
use crate::db::locks::uri_author;

/// Items deleted per release transaction.
pub const RELEASE_BATCH: i64 = 10_000;

/// A list's state after its list-phase transaction.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ListState {
    Active,
    Unreferenced,
}

/// What one list-phase transaction did.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Applied {
    /// The transaction committed. `state` is the row's state now (`None`:
    /// the list has no row), and `waiting` the `(member, source)` pairs of
    /// its references that are owed a run.
    Done { state: Option<ListState>, waiting: Vec<(String, String)> },
    /// The list's row is held by another transaction (a sync batch, a
    /// release batch), or a concurrent insert won. Nothing was changed and
    /// no marker was deleted: try again shortly.
    Busy,
}

/// The list-phase transaction for `list`, never waiting on its row.
///
/// # Errors
///
/// Database errors.
pub async fn apply(pool: &PgPool, list: &str, sync_min_interval_secs: u32) -> Result<Applied, sqlx::Error> {
    apply_taking(pool, list, sync_min_interval_secs, false).await
}

/// The same transaction for a list that has been busy for a while: it
/// waits for the row, up to 5 s (`55P03` past that, for the caller to
/// retry). Run outside the applier's loop, by a task of its own, it holds
/// nothing a member's request can wait on.
///
/// # Errors
///
/// Database errors, a lock timeout among them.
pub async fn apply_waiting(pool: &PgPool, list: &str, sync_min_interval_secs: u32) -> Result<Applied, sqlx::Error> {
    apply_taking(pool, list, sync_min_interval_secs, true).await
}

async fn apply_taking(pool: &PgPool, list: &str, sync_min_interval_secs: u32, wait: bool) -> Result<Applied, sqlx::Error> {
    let owner = uri_author(list).unwrap_or_default();
    let mut tx = pool.begin().await?;
    if wait {
        sqlx::query("SET LOCAL lock_timeout = '5s'").execute(&mut *tx).await?;
    }
    // 1. The markers this pass answers for. Read before the `EXISTS` below,
    // so every change they stand for is visible to it.
    let markers: Vec<i64> = sqlx::query_scalar("SELECT seq FROM list_reference_changes WHERE list_uri = $1")
        .bind(list)
        .fetch_all(&mut *tx)
        .await?;
    // 2. The row, without waiting for it.
    let take = if wait { "FOR UPDATE" } else { "FOR UPDATE SKIP LOCKED" };
    let row: Option<String> = sqlx::query_scalar(&format!("SELECT state FROM subscribed_lists WHERE list_uri = $1 {take}"))
        .bind(list)
        .fetch_optional(&mut *tx)
        .await?;
    if row.is_none() && !wait {
        // "No row" or "row locked": a plain read tells which.
        let held: bool = sqlx::query_scalar("SELECT EXISTS (SELECT 1 FROM subscribed_lists WHERE list_uri = $1)")
            .bind(list)
            .fetch_one(&mut *tx)
            .await?;
        if held {
            return Ok(Applied::Busy);
        }
    }
    let referenced: bool = sqlx::query_scalar("SELECT EXISTS (SELECT 1 FROM list_references WHERE list_uri = $1 AND admitted)")
        .bind(list)
        .fetch_one(&mut *tx)
        .await?;
    let state = match (row.as_deref(), referenced) {
        (None, false) => None,
        (None, true) => {
            let inserted: Option<String> = sqlx::query_scalar(
                "INSERT INTO subscribed_lists (list_uri, owner_did, state, item_count) VALUES ($1, $2, 'active', 0) \
                 ON CONFLICT (list_uri) DO NOTHING RETURNING list_uri",
            )
            .bind(list)
            .bind(owner)
            .fetch_optional(&mut *tx)
            .await?;
            if inserted.is_none() {
                return Ok(Applied::Busy);
            }
            Some(ListState::Active)
        }
        (Some("active"), true) => Some(ListState::Active),
        (Some(_), true) => {
            // Re-subscribed while being released: a new instance of the
            // row, which nothing applies from until a sync has served it.
            sqlx::query(
                "UPDATE subscribed_lists SET state = 'active', state_changed_at = now(), activated_at = now(), synced_at = NULL, \
                   purpose = NULL, purpose_rev = NULL, unlisted_since = NULL WHERE list_uri = $1",
            )
            .bind(list)
            .execute(&mut *tx)
            .await?;
            Some(ListState::Active)
        }
        (Some("active"), false) => {
            sqlx::query("UPDATE subscribed_lists SET state = 'unreferenced', state_changed_at = now() WHERE list_uri = $1")
                .bind(list)
                .execute(&mut *tx)
                .await?;
            Some(ListState::Unreferenced)
        }
        (Some(_), false) => Some(ListState::Unreferenced),
    };
    let mut waiting: Vec<(String, String)> = Vec::new();
    if state.is_some() {
        // 3. The owner's row: created, or locked.
        sqlx::query("INSERT INTO subscribed_owners (owner_did) VALUES ($1) ON CONFLICT (owner_did) DO UPDATE SET owner_did = EXCLUDED.owner_did")
            .bind(owner)
            .execute(&mut *tx)
            .await?;
    }
    if state == Some(ListState::Active) {
        // 4. A sync, when a reference waits on one.
        waiting = sqlx::query_as(
            "SELECT member_did, source FROM list_references \
             WHERE list_uri = $1 AND admitted AND NOT effective AND unlisted_at IS NULL ORDER BY member_did, source",
        )
        .bind(list)
        .fetch_all(&mut *tx)
        .await?;
        if !waiting.is_empty() {
            jobs::enqueue(&mut tx, owner, jobs::HIGH, sync_min_interval_secs).await?;
        }
    }
    // 5. Only the markers read in step 1.
    sqlx::query("DELETE FROM list_reference_changes WHERE seq = ANY($1)")
        .bind(&markers)
        .execute(&mut *tx)
        .await?;
    #[cfg(test)]
    crate::test_support::hooks::pause(crate::test_support::hooks::LIST_PHASE_BEFORE_COMMIT, list).await;
    tx.commit().await?;
    Ok(Applied::Done { state, waiting })
}

/// The lists with a pending marker, oldest marker first.
///
/// # Errors
///
/// Database errors.
pub async fn marked(pool: &PgPool) -> Result<Vec<String>, sqlx::Error> {
    sqlx::query_scalar("SELECT list_uri FROM list_reference_changes GROUP BY list_uri ORDER BY min(seq)")
        .fetch_all(pool)
        .await
}

/// The lists a full reconcile must look at, marker or not (design-v3 §4.3
/// "Crash safety"): a list with an admitted reference and no row; a row
/// whose state disagrees with whether an admitted reference exists; and
/// every `unreferenced` row, whose release may not be running.
///
/// # Errors
///
/// Database errors.
pub async fn unreconciled(pool: &PgPool) -> Result<Vec<String>, sqlx::Error> {
    sqlx::query_scalar(
        "SELECT DISTINCT r.list_uri FROM list_references r \
           WHERE r.admitted AND NOT EXISTS (SELECT 1 FROM subscribed_lists s WHERE s.list_uri = r.list_uri AND s.state = 'active') \
         UNION \
         SELECT s.list_uri FROM subscribed_lists s \
           WHERE s.state = 'unreferenced' \
              OR NOT EXISTS (SELECT 1 FROM list_references r WHERE r.list_uri = s.list_uri AND r.admitted) \
         ORDER BY 1",
    )
    .fetch_all(pool)
    .await
}

/// How a release ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Released {
    /// The items and the row are gone.
    Gone { items: u64 },
    /// The list is referenced again, or a marker for it is pending: the
    /// release stopped, and the list phase decides.
    Stopped { items: u64 },
}

/// Release `list`: delete its copied items in batches, then its row. Each
/// batch re-locks the row and stops if the row is `active` again, if an
/// admitted reference to the list exists, or if a marker for it is pending
/// (the list phase may not have flipped the row back yet). The last
/// transaction also cancels the owner's queued `list_sync`, at a new
/// generation, when this was the owner's last list (R3-151). The owner's
/// row is kept: it carries the pacing a later subscription must find
/// unchanged.
///
/// This leaves no waiting set to trim; the applier's release is
/// [`release_with`].
///
/// # Errors
///
/// Database errors.
pub async fn release(pool: &PgPool, list: &str) -> Result<Released, sqlx::Error> {
    release_with(pool, list, &Waiting::default()).await
}

/// [`release`], which also drops from `waiting` the owner's pairs last
/// added before the cancel when this was the owner's last list (N-1501):
/// their references no longer wait on a sync. A pair added after the
/// cancel belongs to an enqueue that queues the row again, and is kept.
///
/// # Errors
///
/// Database errors.
pub async fn release_with(pool: &PgPool, list: &str, waiting: &Waiting) -> Result<Released, sqlx::Error> {
    let owner = uri_author(list).unwrap_or_default();
    let mut items = 0;
    loop {
        let mut tx = pool.begin().await?;
        let state: Option<String> = sqlx::query_scalar("SELECT state FROM subscribed_lists WHERE list_uri = $1 FOR UPDATE")
            .bind(list)
            .fetch_optional(&mut *tx)
            .await?;
        let wanted: bool = sqlx::query_scalar(
            "SELECT EXISTS (SELECT 1 FROM list_references WHERE list_uri = $1 AND admitted) \
                 OR EXISTS (SELECT 1 FROM list_reference_changes WHERE list_uri = $1)",
        )
        .bind(list)
        .fetch_one(&mut *tx)
        .await?;
        match state.as_deref() {
            None => return Ok(Released::Gone { items }),
            Some("unreferenced") if !wanted => {}
            Some(_) => return Ok(Released::Stopped { items }),
        }
        let deleted = sqlx::query(
            "DELETE FROM subscribed_list_items WHERE ctid IN (SELECT ctid FROM subscribed_list_items WHERE list_uri = $1 LIMIT $2)",
        )
        .bind(list)
        .bind(RELEASE_BATCH)
        .execute(&mut *tx)
        .await?
        .rows_affected();
        items += deleted;
        if deleted > 0 {
            tx.commit().await?;
            #[cfg(test)]
            crate::test_support::hooks::pause(crate::test_support::hooks::RELEASE_BETWEEN_BATCHES, list).await;
            continue;
        }
        // The final transaction: the owner's row (lock step 3), the list's
        // row, and the owner's queued job if no list of the owner is left.
        sqlx::query("SELECT 1 FROM subscribed_owners WHERE owner_did = $1 FOR UPDATE")
            .bind(owner)
            .execute(&mut *tx)
            .await?;
        sqlx::query("DELETE FROM subscribed_lists WHERE list_uri = $1")
            .bind(list)
            .execute(&mut *tx)
            .await?;
        // A new statement: its snapshot follows the owner lock. A list of
        // the owner committed while this waited for the lock is seen here.
        let last: bool = sqlx::query_scalar("SELECT NOT EXISTS (SELECT 1 FROM subscribed_lists WHERE owner_did = $1)")
            .bind(owner)
            .fetch_one(&mut *tx)
            .await?;
        // Taken before the cancel: a pair added later belongs to an enqueue
        // that waited on the owner's row and commits after this.
        let cancelled_at = Instant::now();
        if last {
            sqlx::query(
                "UPDATE backfill_jobs SET state = 'cancelled', generation = generation + 1, updated_at = now() \
                 WHERE did = $1 AND depth = $2 AND state = 'queued'",
            )
            .bind(owner)
            .bind(jobs::DEPTH)
            .execute(&mut *tx)
            .await?;
        }
        tx.commit().await?;
        if last {
            waiting.forget_before(owner, cancelled_at);
        }
        return Ok(Released::Gone { items });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::subscriptions::references::{Budget, Muted, mute_list, unmute_list};
    use crate::test_support::fresh_db;

    const ME: &str = "did:plc:mmmmmmmmmmmmmmmmmmmmmmmm";
    const OTHER: &str = "did:plc:oooooooooooooooooooooooo";
    const OWNER: &str = "did:plc:aaaaaaaaaaaaaaaaaaaaaaaa";
    const BUDGET: Budget = Budget { max_references: 5, max_owners: 5 };

    fn list(n: u8) -> String {
        format!("at://{OWNER}/app.bsky.graph.list/3klist{n}")
    }

    async fn row(pool: &PgPool, list: &str) -> Option<(String, bool)> {
        sqlx::query_as("SELECT state, synced_at IS NULL FROM subscribed_lists WHERE list_uri = $1")
            .bind(list)
            .fetch_optional(pool)
            .await
            .expect("row")
    }

    async fn job(pool: &PgPool) -> Option<(String, i16, bool)> {
        sqlx::query_as("SELECT state, priority, rerun_requested FROM backfill_jobs WHERE did = $1 AND depth = 'list_sync'")
            .bind(OWNER)
            .fetch_optional(pool)
            .await
            .expect("job")
    }

    /// B-73, B-74: a mute makes its list active and queues a sync; the
    /// last unmute makes it unreferenced, and the release removes it and
    /// the queued job.
    #[tokio::test]
    async fn markers_become_rows_and_a_released_list_leaves_nothing() {
        let Some(db) = fresh_db("subscriptions_list_phase").await else {
            return;
        };
        let p = &db.pool;
        let l = list(1);
        // No marker, no reference: nothing to do.
        assert_eq!(apply(p, &l, 600).await.expect("db"), Applied::Done { state: None, waiting: Vec::new() });

        assert_eq!(mute_list(p, ME, &l, BUDGET).await.expect("db"), Muted::Added);
        assert_eq!(marked(p).await.expect("db"), std::slice::from_ref(&l));
        let applied = apply(p, &l, 600).await.expect("db");
        assert_eq!(applied, Applied::Done { state: Some(ListState::Active), waiting: vec![(ME.to_string(), "mute".to_string())] });
        assert_eq!(row(p, &l).await, Some(("active".to_string(), true)));
        assert_eq!(job(p).await, Some(("queued".to_string(), jobs::HIGH, false)));
        assert!(marked(p).await.expect("db").is_empty());
        let owners: i64 = sqlx::query_scalar("SELECT count(*) FROM subscribed_owners WHERE owner_did = $1").bind(OWNER).fetch_one(p).await.expect("owners");
        assert_eq!(owners, 1);

        // A second member's mute of the same list changes no state.
        mute_list(p, OTHER, &l, BUDGET).await.expect("db");
        apply(p, &l, 600).await.expect("db");
        // The first unmute leaves it active; the last makes it unreferenced.
        unmute_list(p, ME, &l, BUDGET).await.expect("db");
        assert!(matches!(apply(p, &l, 600).await.expect("db"), Applied::Done { state: Some(ListState::Active), .. }));
        unmute_list(p, OTHER, &l, BUDGET).await.expect("db");
        assert_eq!(apply(p, &l, 600).await.expect("db"), Applied::Done { state: Some(ListState::Unreferenced), waiting: Vec::new() });
        assert_eq!(row(p, &l).await.map(|(state, _)| state), Some("unreferenced".to_string()));

        // Release: the items in batches, the row, and the queued job. The
        // owner's row stays.
        sqlx::query(
            "INSERT INTO subscribed_list_items (uri, owner_did, rkey, list_uri, subject_did, cid, rev) \
             SELECT 'at://' || $1 || '/app.bsky.graph.listitem/' || n, $1, n::text, $2, 'did:plc:s' || n, 'bafy', '3k' FROM generate_series(1, 3) n",
        )
        .bind(OWNER)
        .bind(&l)
        .execute(p)
        .await
        .expect("items");
        assert_eq!(release(p, &l).await.expect("db"), Released::Gone { items: 3 });
        assert_eq!(row(p, &l).await, None);
        assert_eq!(job(p).await.map(|(state, ..)| state), Some("cancelled".to_string()));
        let owners: i64 = sqlx::query_scalar("SELECT count(*) FROM subscribed_owners WHERE owner_did = $1").bind(OWNER).fetch_one(p).await.expect("owners");
        assert_eq!(owners, 1);
        // Releasing again finds nothing.
        assert_eq!(release(p, &l).await.expect("db"), Released::Gone { items: 0 });
        db.drop_db().await;
    }

    /// A re-subscription during release stops the release, and the list
    /// phase makes a new instance of the row.
    #[tokio::test]
    async fn a_resubscription_stops_a_release() {
        let Some(db) = fresh_db("subscriptions_resubscribe").await else {
            return;
        };
        let p = &db.pool;
        let l = list(1);
        mute_list(p, ME, &l, BUDGET).await.expect("db");
        apply(p, &l, 600).await.expect("db");
        sqlx::query("UPDATE subscribed_lists SET synced_at = now(), purpose = 'app.bsky.graph.defs#modlist'").execute(p).await.expect("synced");
        let before: chrono::DateTime<chrono::Utc> = sqlx::query_scalar("SELECT activated_at FROM subscribed_lists").fetch_one(p).await.expect("at");
        unmute_list(p, ME, &l, BUDGET).await.expect("db");
        apply(p, &l, 600).await.expect("db");
        // Muted again before the release ran: its marker stops the release.
        mute_list(p, ME, &l, BUDGET).await.expect("db");
        assert_eq!(release(p, &l).await.expect("db"), Released::Stopped { items: 0 });
        assert!(matches!(apply(p, &l, 600).await.expect("db"), Applied::Done { state: Some(ListState::Active), .. }));
        let (after, unsynced, purpose): (chrono::DateTime<chrono::Utc>, bool, Option<String>) =
            sqlx::query_as("SELECT activated_at, synced_at IS NULL, purpose FROM subscribed_lists").fetch_one(p).await.expect("row");
        assert!(after > before && unsynced && purpose.is_none());
        // An active list is not released.
        assert_eq!(release(p, &l).await.expect("db"), Released::Stopped { items: 0 });

        // A row held by another transaction is skipped, markers kept.
        unmute_list(p, ME, &l, BUDGET).await.expect("db");
        let mut holder = p.begin().await.expect("tx");
        sqlx::query("SELECT 1 FROM subscribed_lists WHERE list_uri = $1 FOR UPDATE").bind(&l).execute(&mut *holder).await.expect("hold");
        assert_eq!(apply(p, &l, 600).await.expect("db"), Applied::Busy);
        assert_eq!(marked(p).await.expect("db"), std::slice::from_ref(&l));
        holder.rollback().await.expect("release");
        assert!(matches!(apply(p, &l, 600).await.expect("db"), Applied::Done { state: Some(ListState::Unreferenced), .. }));
        db.drop_db().await;
    }

    /// An enqueue never supersedes a running job, and never makes a queued
    /// one later.
    #[tokio::test]
    async fn an_enqueue_respects_the_jobs_state() {
        let Some(db) = fresh_db("subscriptions_enqueue").await else {
            return;
        };
        let p = &db.pool;
        let enqueue = |priority: i16| async move {
            let mut conn = p.acquire().await.expect("conn");
            jobs::enqueue(&mut conn, OWNER, priority, 600).await.expect("enqueue");
        };
        enqueue(jobs::LOW).await;
        assert_eq!(job(p).await, Some(("queued".to_string(), jobs::LOW, false)));
        // A high enqueue raises a queued row's priority; a low one after it
        // does not lower it.
        enqueue(jobs::HIGH).await;
        enqueue(jobs::LOW).await;
        assert_eq!(job(p).await, Some(("queued".to_string(), jobs::HIGH, false)));
        // Running: left running, a rerun asked for.
        sqlx::query("UPDATE backfill_jobs SET state = 'running', priority = 1").execute(p).await.expect("running");
        enqueue(jobs::HIGH).await;
        assert_eq!(job(p).await, Some(("running".to_string(), jobs::HIGH, true)));
        // Done: queued again at the enqueue's own priority, not before the
        // owner's interval has passed.
        sqlx::query("UPDATE backfill_jobs SET state = 'done'").execute(p).await.expect("done");
        sqlx::query("INSERT INTO subscribed_owners (owner_did, last_run_ended_at) VALUES ($1, now())").bind(OWNER).execute(p).await.expect("owner");
        enqueue(jobs::LOW).await;
        assert_eq!(job(p).await, Some(("queued".to_string(), jobs::LOW, false)));
        let wait: f64 = sqlx::query_scalar("SELECT extract(epoch FROM not_before - now())::float8 FROM backfill_jobs").fetch_one(p).await.expect("wait");
        assert!((590.0..=600.0).contains(&wait), "{wait}");
        db.drop_db().await;
    }
}
