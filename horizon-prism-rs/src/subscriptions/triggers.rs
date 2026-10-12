//! The `list_sync` enqueues that the list phase does not make (design-v3
//! §4.3 "When a sync is enqueued"): the recovery sweep, the weekly run,
//! account and identity events for owners that look gone, and requested
//! runs (the end of a purge, `admin lists sync`).
//!
//! Each takes the owner's `subscribed_owners` row in its own statement
//! (lock step 3) and computes the earliest start in a later one, so the
//! start is never paced against a stale copy of that row.

use std::time::Duration;

use sqlx::{PgConnection, PgPool};

use super::applier::Waiting;
use super::jobs;

/// `worker_state` key recording the last weekly run.
const WEEKLY_KEY: &str = "list_sync_weekly_ran_at";

/// Enqueue `owner`'s sync. With `request`, record that a run is owed
/// whatever the tables show (claim-time cause 6): the owner's row is
/// created if missing and stamped with the clock's time, not the
/// transaction's, so two requests always differ.
///
/// # Errors
///
/// Database errors.
pub async fn enqueue(pool: &PgPool, owner: &str, priority: i16, min_interval_secs: u32, request: bool) -> Result<(), sqlx::Error> {
    let mut tx = pool.begin().await?;
    enqueue_in(&mut tx, owner, priority, min_interval_secs, request).await?;
    tx.commit().await
}

/// [`enqueue`], inside the caller's transaction.
async fn enqueue_in(conn: &mut PgConnection, owner: &str, priority: i16, min_interval_secs: u32, request: bool) -> Result<(), sqlx::Error> {
    if request {
        sqlx::query(
            "INSERT INTO subscribed_owners (owner_did, resync_requested_at) VALUES ($1, clock_timestamp()) \
             ON CONFLICT (owner_did) DO UPDATE SET resync_requested_at = clock_timestamp()",
        )
        .bind(owner)
        .execute(&mut *conn)
        .await?;
    } else {
        sqlx::query("SELECT 1 FROM subscribed_owners WHERE owner_did = $1 FOR SHARE").bind(owner).execute(&mut *conn).await?;
    }
    jobs::enqueue(conn, owner, priority, min_interval_secs).await
}

/// Request a run for `owner` if it still has an active list, inside the
/// caller's transaction: the end of a purge of the owner, whose changes
/// were withheld while the purge stood, and `admin lists sync`. Returns
/// whether a run was requested.
///
/// # Errors
///
/// Database errors.
pub async fn request_if_active(conn: &mut PgConnection, owner: &str, priority: i16, min_interval_secs: u32) -> Result<bool, sqlx::Error> {
    let active: bool = sqlx::query_scalar("SELECT EXISTS (SELECT 1 FROM subscribed_lists WHERE owner_did = $1 AND state = 'active')")
        .bind(owner)
        .fetch_one(&mut *conn)
        .await?;
    if active {
        enqueue_in(conn, owner, priority, min_interval_secs, true).await?;
    }
    Ok(active)
}

/// An `#account` or `#identity` event for `owner`: a low-priority run, but
/// only for an owner that is suspected gone or gone. An ordinary owner's
/// events enqueue nothing, or an outside account could keep the lanes busy
/// by emitting events. Returns whether a run was enqueued.
///
/// # Errors
///
/// Database errors.
pub async fn owner_event(pool: &PgPool, owner: &str, min_interval_secs: u32) -> Result<bool, sqlx::Error> {
    let mut tx = pool.begin().await?;
    let looks_gone: Option<bool> =
        sqlx::query_scalar("SELECT gone_suspect_at IS NOT NULL OR owner_gone FROM subscribed_owners WHERE owner_did = $1 FOR SHARE")
            .bind(owner)
            .fetch_optional(&mut *tx)
            .await?;
    if looks_gone != Some(true) {
        return Ok(false);
    }
    jobs::enqueue(&mut tx, owner, jobs::LOW, min_interval_secs).await?;
    tx.commit().await?;
    Ok(true)
}

/// The recovery sweep: at startup and every retry-scheduler run.
///
/// It rebuilds the waiting sets from the tables: the pair of every
/// reference that is owed a run to an active list, whatever its owner's
/// state. A pair already there keeps its time.
///
/// It then enqueues every owner that is not gone, not suspected (its own
/// re-queue covers it) and not recently cut, and that has an active list
/// with a reference owed a run (high priority) or never synced (low),
/// whatever its job row's state. Returns how many owners were enqueued.
///
/// # Errors
///
/// Database errors.
pub async fn recovery_sweep(pool: &PgPool, waiting: &Waiting, min_interval_secs: u32) -> Result<u64, sqlx::Error> {
    let owed: Vec<(String, String, String)> = sqlx::query_as(
        "SELECT DISTINCT l.owner_did, r.member_did, r.source FROM subscribed_lists l JOIN list_references r ON r.list_uri = l.list_uri \
         WHERE l.state = 'active' AND r.admitted AND NOT r.effective AND r.unlisted_at IS NULL",
    )
    .fetch_all(pool)
    .await?;
    for (owner, member, source) in &owed {
        waiting.add_if_absent(owner, member, source);
    }
    let owners: Vec<(String, bool)> = sqlx::query_as(
        "SELECT l.owner_did, bool_or(EXISTS (SELECT 1 FROM list_references r WHERE r.list_uri = l.list_uri \
                                              AND r.admitted AND NOT r.effective AND r.unlisted_at IS NULL)) \
         FROM subscribed_lists l LEFT JOIN subscribed_owners o ON o.owner_did = l.owner_did \
         WHERE l.state = 'active' \
           AND NOT COALESCE(o.owner_gone, false) AND o.gone_suspect_at IS NULL AND NOT COALESCE(o.cut_until > now(), false) \
           AND (l.synced_at IS NULL OR EXISTS (SELECT 1 FROM list_references r WHERE r.list_uri = l.list_uri \
                                                AND r.admitted AND NOT r.effective AND r.unlisted_at IS NULL)) \
         GROUP BY l.owner_did ORDER BY l.owner_did",
    )
    .fetch_all(pool)
    .await?;
    let mut n = 0;
    for (owner, owed) in owners {
        enqueue(pool, &owner, if owed { jobs::HIGH } else { jobs::LOW }, min_interval_secs, false).await?;
        n += 1;
    }
    Ok(n)
}

/// The owners the weekly run enqueues: every owner with an active list
/// whose last complete listing is missing or more than 7 days old.
///
/// # Errors
///
/// Database errors.
pub async fn weekly_owners(pool: &PgPool) -> Result<Vec<String>, sqlx::Error> {
    sqlx::query_scalar(
        "SELECT DISTINCT l.owner_did FROM subscribed_lists l LEFT JOIN subscribed_owners o ON o.owner_did = l.owner_did \
         WHERE l.state = 'active' AND (o.last_completed_at IS NULL OR o.last_completed_at < now() - interval '7 days') \
         ORDER BY l.owner_did",
    )
    .fetch_all(pool)
    .await
}

/// The weekly run, when it is due: a low-priority run for each of
/// [`weekly_owners`], `spacing` apart. It repairs changes lost to a gap in
/// the event stream and looks again at owners that are gone. Returns how
/// many owners were enqueued, or `None` when the week is not up.
///
/// # Errors
///
/// Database errors.
pub async fn weekly(pool: &PgPool, min_interval_secs: u32, spacing: Duration) -> Result<Option<u64>, sqlx::Error> {
    let due: bool = sqlx::query_scalar(
        "SELECT COALESCE((SELECT (value #>> '{}')::timestamptz < now() - interval '7 days' FROM worker_state WHERE key = $1), true)",
    )
    .bind(WEEKLY_KEY)
    .fetch_one(pool)
    .await?;
    if !due {
        return Ok(None);
    }
    let mut n = 0;
    for owner in weekly_owners(pool).await? {
        enqueue(pool, &owner, jobs::LOW, min_interval_secs, false).await?;
        n += 1;
        tokio::time::sleep(spacing).await;
    }
    sqlx::query(
        "INSERT INTO worker_state (key, value, updated_at) VALUES ($1, to_jsonb(now()::text), now()) \
         ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value, updated_at = now()",
    )
    .bind(WEEKLY_KEY)
    .execute(pool)
    .await?;
    Ok(Some(n))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::subscriptions::list_phase;
    use crate::subscriptions::references::{Budget, mute_list};
    use crate::test_support::fresh_db;

    const A: &str = "did:plc:aaaaaaaaaaaaaaaaaaaaaaaa";
    const B: &str = "did:plc:bbbbbbbbbbbbbbbbbbbbbbbb";
    const M1: &str = "did:plc:mmmmmmmmmmmmmmmmmmmmmmm1";

    fn list(owner: &str) -> String {
        format!("at://{owner}/app.bsky.graph.list/3klist1")
    }

    #[tokio::test]
    async fn the_triggers_enqueue_only_what_they_should() {
        let Some(db) = fresh_db("subscriptions_triggers").await else {
            return;
        };
        let p = &db.pool;
        let budget = Budget { max_references: 5, max_owners: 5 };
        let job = |owner: &'static str| async move {
            sqlx::query_as::<_, (String, i16)>("SELECT state, priority FROM backfill_jobs WHERE did = $1").bind(owner).fetch_optional(p).await.expect("job")
        };
        let clear = || async move {
            sqlx::query("DELETE FROM backfill_jobs").execute(p).await.expect("clear");
        };
        for owner in [A, B] {
            mute_list(p, M1, &list(owner), budget).await.expect("mute");
            list_phase::apply(p, &list(owner), 600).await.expect("apply");
        }
        clear().await;

        // The sweep finds both owners' waiting references again, and
        // rebuilds the waiting sets.
        let waiting = Waiting::default();
        assert_eq!(recovery_sweep(p, &waiting, 600).await.expect("sweep"), 2);
        assert_eq!(job(A).await, Some(("queued".to_string(), jobs::HIGH)));
        assert_eq!(waiting.of(B), vec![(M1.to_string(), "mute".to_string())]);

        // Not a suspected owner, nor one that was cut; a list never synced
        // with nothing waiting is a low-priority run.
        clear().await;
        sqlx::query("UPDATE subscribed_owners SET gone_suspect_at = now() WHERE owner_did = $1").bind(A).execute(p).await.expect("suspect");
        sqlx::query("UPDATE list_references SET effective = true").execute(p).await.expect("effective");
        assert_eq!(recovery_sweep(p, &waiting, 600).await.expect("sweep"), 1);
        assert_eq!((job(A).await, job(B).await), (None, Some(("queued".to_string(), jobs::LOW))));
        clear().await;
        sqlx::query("UPDATE subscribed_owners SET cut_until = now() + interval '1 day' WHERE owner_did = $1").bind(B).execute(p).await.expect("cut");
        assert_eq!(recovery_sweep(p, &waiting, 600).await.expect("sweep"), 0);

        // An event enqueues only an owner that looks gone.
        assert!(owner_event(p, A, 600).await.expect("event"));
        assert!(!owner_event(p, B, 600).await.expect("event"));
        assert!(!owner_event(p, M1, 600).await.expect("event"));
        assert_eq!((job(A).await, job(B).await), (Some(("queued".to_string(), jobs::LOW)), None));

        // The weekly run takes owners with no complete listing in a week,
        // once a week.
        clear().await;
        sqlx::query("UPDATE subscribed_owners SET last_completed_at = now() WHERE owner_did = $1").bind(A).execute(p).await.expect("completed");
        assert_eq!(weekly(p, 600, Duration::ZERO).await.expect("weekly"), Some(1));
        assert_eq!((job(A).await.is_some(), job(B).await.is_some()), (false, true));
        assert_eq!(weekly(p, 600, Duration::ZERO).await.expect("weekly"), None);

        // A requested run is recorded on the owner's row, and only for an
        // owner with an active list.
        let mut conn = p.acquire().await.expect("conn");
        assert!(request_if_active(&mut conn, A, jobs::LOW, 600).await.expect("request"));
        assert!(!request_if_active(&mut conn, M1, jobs::LOW, 600).await.expect("request"));
        drop(conn);
        let requested: Vec<String> = sqlx::query_scalar("SELECT owner_did FROM subscribed_owners WHERE resync_requested_at IS NOT NULL").fetch_all(p).await.expect("requested");
        assert_eq!(requested, vec![A.to_string()]);

        // An interrupted run is a run end: the owner's row is written and
        // the row comes back paced, at a new generation.
        sqlx::query("UPDATE backfill_jobs SET state = 'running' WHERE did = $1").bind(A).execute(p).await.expect("running");
        assert!(!jobs::interrupt(p, A, 600, Some(3600)).await.expect("young"));
        assert!(jobs::interrupt(p, A, 600, None).await.expect("interrupt"));
        let row: (String, i64, bool) = sqlx::query_as("SELECT state, generation, not_before > now() + interval '9 minutes' FROM backfill_jobs WHERE did = $1").bind(A).fetch_one(p).await.expect("job");
        assert_eq!(row, ("queued".to_string(), 1, true));
        assert!(!jobs::interrupt(p, A, 600, None).await.expect("nothing running"));
        db.drop_db().await;
    }
}
