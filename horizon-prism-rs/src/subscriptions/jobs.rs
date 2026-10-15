//! The `list_sync` job row of a list owner: when it may start, and how an
//! enqueue changes it (design-v3 §4.3 "Sync scheduling", "Pacing and
//! claiming").
//!
//! One job per owner, `(owner DID, 'list_sync')`, covers every active list
//! of that owner. An enqueue never supersedes a live row: it never changes
//! a running row's state or generation.

use chrono::{DateTime, TimeDelta, Utc};
use sqlx::PgConnection;

pub const DEPTH: &str = "list_sync";
/// Enqueues that make a reference wait on the sync.
pub const HIGH: i16 = 0;
/// Weekly repairs and owner rechecks.
pub const LOW: i16 = 1;

/// The owner state an earliest start is computed from.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, sqlx::FromRow)]
pub struct OwnerPacing {
    pub last_run_ended_at: Option<DateTime<Utc>>,
    pub gone_suspect_at: Option<DateTime<Utc>>,
    pub owner_gone: bool,
    pub slow_until: Option<DateTime<Utc>>,
    pub cut_until: Option<DateTime<Utc>>,
}

/// The earliest time the owner's next run may start. It depends on owner
/// state only, never on a job's priority:
/// - normally `last_run_ended_at + min_interval`;
/// - while the owner is suspected gone, not before `gone_suspect_at + 24 h`;
/// - while it is slow or was recently cut, not before
///   `last_run_ended_at + 24 h`.
#[must_use]
pub fn earliest_start(owner: &OwnerPacing, min_interval_secs: u32, now: DateTime<Utc>) -> DateTime<Utc> {
    let day = TimeDelta::hours(24);
    let mut earliest = now;
    if let Some(ended) = owner.last_run_ended_at {
        earliest = earliest.max(ended + TimeDelta::seconds(i64::from(min_interval_secs)));
        let backing_off = owner.slow_until.is_some_and(|t| t > now) || owner.cut_until.is_some_and(|t| t > now);
        if backing_off {
            earliest = earliest.max(ended + day);
        }
    }
    if let Some(suspected) = owner.gone_suspect_at.filter(|_| !owner.owner_gone) {
        earliest = earliest.max(suspected + day);
    }
    earliest
}

/// Enqueue the owner's `list_sync` at `priority`. The caller holds the
/// owner's `subscribed_owners` row (lock step 3), so the pacing it read is
/// current; this takes the job row (lock step 6).
///
/// - No row: a queued row at the earliest start.
/// - Running: the run is left alone; a rerun is asked for.
/// - Queued: `not_before` moves no later than the earliest start.
/// - Done, failed or cancelled: queued again at the earliest start.
///
/// The priority only ever rises on a live row. A live row's generation is
/// never changed here; a done, failed or cancelled row queued again moves
/// to a new one (design-v4 §II-5.10 "Generations").
///
/// # Errors
///
/// Database errors.
pub async fn enqueue(conn: &mut PgConnection, owner: &str, priority: i16, min_interval_secs: u32) -> Result<(), sqlx::Error> {
    let pacing: OwnerPacing = sqlx::query_as(
        "SELECT last_run_ended_at, gone_suspect_at, owner_gone, slow_until, cut_until FROM subscribed_owners WHERE owner_did = $1",
    )
    .bind(owner)
    .fetch_optional(&mut *conn)
    .await?
    .unwrap_or_default();
    let earliest = earliest_start(&pacing, min_interval_secs, Utc::now());
    sqlx::query(
        "INSERT INTO backfill_jobs AS j (did, depth, state, attempts, not_before, priority, updated_at) \
         VALUES ($1, $2, 'queued', 0, $3, $4, now()) \
         ON CONFLICT (did, depth) DO UPDATE SET \
           rerun_requested = CASE j.state WHEN 'running' THEN true WHEN 'queued' THEN j.rerun_requested ELSE false END, \
           priority = CASE WHEN j.state IN ('running', 'queued') THEN LEAST(j.priority, EXCLUDED.priority) ELSE EXCLUDED.priority END, \
           not_before = CASE j.state WHEN 'running' THEN j.not_before WHEN 'queued' THEN LEAST(j.not_before, EXCLUDED.not_before) \
                        ELSE EXCLUDED.not_before END, \
           attempts = CASE WHEN j.state IN ('running', 'queued') THEN j.attempts ELSE 0 END, \
           last_error = CASE WHEN j.state IN ('running', 'queued') THEN j.last_error END, \
           generation = CASE WHEN j.state IN ('running', 'queued') THEN j.generation ELSE j.generation + 1 END, \
           state = CASE j.state WHEN 'running' THEN 'running' ELSE 'queued' END, \
           updated_at = now()",
    )
    .bind(owner)
    .bind(DEPTH)
    .bind(earliest)
    .bind(priority)
    .execute(conn)
    .await?;
    Ok(())
}

/// How a run ended, for its job row.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RunEnd<'a> {
    /// A completion (also the owner-gone one), or a run that found no
    /// active list: `done`, unless a rerun was asked for meanwhile.
    Done,
    /// A cut, or a run that stood down (the owner is suspected gone, or is
    /// being purged): queued again at the owner's earliest start.
    Requeue,
    /// A failure: retried with backoff, like any backfill job.
    Failed(&'a str),
}

/// What a cut does to the owner's pacing.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Cut {
    /// Too many records, too long: the owner is large. A week of daily
    /// runs at most.
    Large,
    /// Slow answers or tiny pages: the same, on the slow lane.
    Slow,
}

/// Record a run's end on the owner's row (lock step 3): always, whether or
/// not the owner still has a list, so a later subscription finds the same
/// earliest start whoever held the lists before. A completion writes its
/// own, fuller row instead.
///
/// # Errors
///
/// Database errors.
pub async fn record_run_end(conn: &mut PgConnection, owner: &str, cut: Option<Cut>) -> Result<(), sqlx::Error> {
    sqlx::query(
        "INSERT INTO subscribed_owners AS o (owner_did, last_run_ended_at, cut_until, slow_until) \
         VALUES ($1, now(), CASE WHEN $2 THEN now() + interval '7 days' END, CASE WHEN $3 THEN now() + interval '7 days' END) \
         ON CONFLICT (owner_did) DO UPDATE SET last_run_ended_at = now(), \
           cut_until = COALESCE(EXCLUDED.cut_until, o.cut_until), slow_until = COALESCE(EXCLUDED.slow_until, o.slow_until)",
    )
    .bind(owner)
    .bind(cut.is_some())
    .bind(cut == Some(Cut::Slow))
    .execute(conn)
    .await?;
    Ok(())
}

/// Settle the job row of a run that ended (lock step 6), after the owner's
/// row was written in the same transaction. Only a row still `running` at
/// `generation` is touched: a superseded run settles nothing. Returns
/// whether it was.
///
/// - Every `not_before` written is at or after the owner's earliest start.
/// - A re-queue happens only while the owner still has a list row.
/// - A rerun asked for during the run is honoured by a completion, an empty
///   run and a failure; a cut's or a stand-down's own re-queue stands in
///   for it. The priority a waiting reference raised is kept.
///
/// # Errors
///
/// Database errors.
pub async fn settle(
    conn: &mut PgConnection,
    owner: &str,
    generation: i64,
    end: RunEnd<'_>,
    min_interval_secs: u32,
) -> Result<bool, sqlx::Error> {
    #[cfg(test)]
    if crate::test_support::hooks::injected(crate::test_support::hooks::SETTLE_FAILS, owner) {
        return Err(sqlx::Error::Protocol("injected settle failure".into()));
    }
    let running: Option<(bool, i32)> = sqlx::query_as(
        "SELECT rerun_requested, attempts FROM backfill_jobs WHERE did = $1 AND depth = $2 AND state = 'running' AND generation = $3 FOR UPDATE",
    )
    .bind(owner)
    .bind(DEPTH)
    .bind(generation)
    .fetch_optional(&mut *conn)
    .await?;
    let Some((rerun, attempts)) = running else {
        return Ok(false);
    };
    let pacing: OwnerPacing = sqlx::query_as(
        "SELECT last_run_ended_at, gone_suspect_at, owner_gone, slow_until, cut_until FROM subscribed_owners WHERE owner_did = $1",
    )
    .bind(owner)
    .fetch_optional(&mut *conn)
    .await?
    .unwrap_or_default();
    let now = Utc::now();
    let earliest = earliest_start(&pacing, min_interval_secs, now);
    let has_list: bool = sqlx::query_scalar("SELECT EXISTS (SELECT 1 FROM subscribed_lists WHERE owner_did = $1)")
        .bind(owner)
        .fetch_one(&mut *conn)
        .await?;
    // (state, not_before, attempts, last_error)
    let (state, not_before, attempts, error): (&str, DateTime<Utc>, i32, Option<String>) = match end {
        RunEnd::Done if rerun && has_list => ("queued", earliest, 0, None),
        RunEnd::Done => ("done", now, 0, None),
        RunEnd::Requeue if has_list => ("queued", earliest, 0, None),
        RunEnd::Requeue => ("done", now, 0, None),
        RunEnd::Failed(_) if rerun && has_list => ("queued", earliest, 0, None),
        RunEnd::Failed(error) => {
            let attempt = attempts + 1;
            let state = if attempt >= crate::firehose::backfill::MAX_ATTEMPTS { "failed" } else { "queued" };
            let retry = now + crate::firehose::backfill::backoff(attempt);
            (state, retry.max(earliest), attempt, Some(error.chars().take(512).collect()))
        }
    };
    sqlx::query(
        "UPDATE backfill_jobs SET state = $3, not_before = $4, attempts = $5, last_error = $6, rerun_requested = false, updated_at = now() \
         WHERE did = $1 AND depth = $2",
    )
    .bind(owner)
    .bind(DEPTH)
    .bind(state)
    .bind(not_before)
    .bind(attempts)
    .bind(error)
    .execute(conn)
    .await?;
    Ok(true)
}

/// Queue again a row left `running` with no run behind it (the process
/// stopped, or the run died): a run end like any other, so the owner's
/// row is written first and the new start is paced. The generation moves
/// on, so nothing of the old run can settle the row. With `older_than_secs`
/// only a row that has been `running` at least that long is touched.
/// Returns whether a row was.
///
/// # Errors
///
/// Database errors.
pub async fn interrupt(pool: &sqlx::PgPool, owner: &str, min_interval_secs: u32, older_than_secs: Option<u32>) -> Result<bool, sqlx::Error> {
    let mut tx = pool.begin().await?;
    record_run_end(&mut tx, owner, None).await?;
    let pacing: OwnerPacing =
        sqlx::query_as("SELECT last_run_ended_at, gone_suspect_at, owner_gone, slow_until, cut_until FROM subscribed_owners WHERE owner_did = $1")
            .bind(owner)
            .fetch_optional(&mut *tx)
            .await?
            .unwrap_or_default();
    let earliest = earliest_start(&pacing, min_interval_secs, Utc::now());
    let n = sqlx::query(
        "UPDATE backfill_jobs SET state = 'queued', generation = generation + 1, not_before = $3, updated_at = now() \
         WHERE did = $1 AND depth = $2 AND state = 'running' \
           AND ($4::float8 IS NULL OR updated_at < now() - make_interval(secs => $4::float8))",
    )
    .bind(owner)
    .bind(DEPTH)
    .bind(earliest)
    .bind(older_than_secs.map(f64::from))
    .execute(&mut *tx)
    .await?
    .rows_affected();
    if n == 0 {
        // Nothing was running after all: the owner's row stays as it was.
        tx.rollback().await?;
        return Ok(false);
    }
    tx.commit().await?;
    Ok(true)
}

/// End a run that is not a completion: the owner's row, then the job row,
/// in one transaction.
///
/// # Errors
///
/// Database errors.
pub async fn end_run(
    pool: &sqlx::PgPool,
    owner: &str,
    generation: i64,
    end: RunEnd<'_>,
    cut: Option<Cut>,
    min_interval_secs: u32,
) -> Result<bool, sqlx::Error> {
    let mut tx = pool.begin().await?;
    record_run_end(&mut tx, owner, cut).await?;
    let settled = settle(&mut tx, owner, generation, end, min_interval_secs).await?;
    tx.commit().await?;
    Ok(settled)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::fresh_db;

    const OWNER: &str = "did:plc:aaaaaaaaaaaaaaaaaaaaaaaa";

    /// Every run end writes the owner's row; the job row follows the rules
    /// for that kind of end, and a superseded run settles nothing.
    #[tokio::test]
    async fn run_ends_settle_the_job_row() {
        let Some(db) = fresh_db("subscriptions_run_ends").await else {
            return;
        };
        let p = &db.pool;
        let running = |rerun: bool| async move {
            sqlx::query(
                "INSERT INTO backfill_jobs (did, depth, state, rerun_requested, attempts) VALUES ($1, 'list_sync', 'running', $2, 0) \
                 ON CONFLICT (did, depth) DO UPDATE SET state = 'running', rerun_requested = $2, attempts = 0, not_before = now()",
            )
            .bind(OWNER)
            .bind(rerun)
            .execute(p)
            .await
            .expect("job");
        };
        let job = || async {
            sqlx::query_as::<_, (String, bool, i32, f64)>(
                "SELECT state, rerun_requested, attempts, extract(epoch FROM not_before - now())::float8 FROM backfill_jobs WHERE did = $1",
            )
            .bind(OWNER)
            .fetch_one(p)
            .await
            .expect("job")
        };
        let list = |present: bool| async move {
            sqlx::query("DELETE FROM subscribed_lists").execute(p).await.expect("clear");
            if present {
                sqlx::query("INSERT INTO subscribed_lists (list_uri, owner_did, state) VALUES ('at://x/app.bsky.graph.list/1', $1, 'active')")
                    .bind(OWNER)
                    .execute(p)
                    .await
                    .expect("list");
            }
        };

        // Done with no rerun asked for: done. With one, and a list: queued
        // an interval after the run's end, the flag cleared.
        list(true).await;
        running(false).await;
        assert!(end_run(p, OWNER, 0, RunEnd::Done, None, 600).await.expect("db"));
        assert_eq!(job().await.0, "done");
        running(true).await;
        end_run(p, OWNER, 0, RunEnd::Done, None, 600).await.expect("db");
        let (state, rerun, _, wait) = job().await;
        assert!(state == "queued" && !rerun && (590.0..=600.0).contains(&wait), "{state} {rerun} {wait}");
        // A cut: queued a day after the run's end, and the owner backs off
        // for a week; a slow cut also marks it slow.
        running(false).await;
        end_run(p, OWNER, 0, RunEnd::Requeue, Some(Cut::Slow), 600).await.expect("db");
        let (state, _, _, wait) = job().await;
        assert!(state == "queued" && (86_300.0..=86_400.0).contains(&wait), "{state} {wait}");
        let (cut, slow): (bool, bool) = sqlx::query_as("SELECT cut_until > now() + interval '6 days', slow_until > now() + interval '6 days' FROM subscribed_owners")
            .fetch_one(p)
            .await
            .expect("owner");
        assert!(cut && slow);
        // A failure: retried no sooner than the owner's earliest start, and
        // counted; with no list left, a stand-down just ends.
        sqlx::query("UPDATE subscribed_owners SET cut_until = NULL, slow_until = NULL").execute(p).await.expect("clear");
        running(false).await;
        end_run(p, OWNER, 0, RunEnd::Failed("boom"), None, 600).await.expect("db");
        let (state, _, attempts, wait) = job().await;
        assert!(state == "queued" && attempts == 1 && (590.0..=600.0).contains(&wait), "{state} {attempts} {wait}");
        list(false).await;
        running(true).await;
        end_run(p, OWNER, 0, RunEnd::Requeue, None, 600).await.expect("db");
        assert_eq!(job().await.0, "done");
        // A run whose job moved on settles nothing, but its end is recorded.
        running(false).await;
        assert!(!end_run(p, OWNER, 7, RunEnd::Done, None, 600).await.expect("db"));
        assert_eq!(job().await.0, "running");
        db.drop_db().await;
    }

    #[test]
    fn the_earliest_start_follows_owner_state_only() {
        let now = Utc::now();
        let ago = |secs: i64| Some(now - TimeDelta::seconds(secs));
        let ahead = Some(now + TimeDelta::days(3));
        // A new owner, or one whose interval has passed, may start now.
        assert_eq!(earliest_start(&OwnerPacing::default(), 600, now), now);
        let rested = OwnerPacing { last_run_ended_at: ago(700), ..OwnerPacing::default() };
        assert_eq!(earliest_start(&rested, 600, now), now);
        // Within the interval: its end.
        let recent = OwnerPacing { last_run_ended_at: ago(100), ..OwnerPacing::default() };
        assert_eq!(earliest_start(&recent, 600, now), now + TimeDelta::seconds(500));
        // Slow or recently cut: a day after the last run.
        for backing_off in [
            OwnerPacing { last_run_ended_at: ago(100), slow_until: ahead, ..OwnerPacing::default() },
            OwnerPacing { last_run_ended_at: ago(100), cut_until: ahead, ..OwnerPacing::default() },
        ] {
            assert_eq!(earliest_start(&backing_off, 600, now), now + TimeDelta::hours(24) - TimeDelta::seconds(100));
        }
        // An expired backoff no longer counts.
        let expired = OwnerPacing { last_run_ended_at: ago(700), cut_until: ago(1), ..OwnerPacing::default() };
        assert_eq!(earliest_start(&expired, 600, now), now);
        // Suspected gone: a day after the suspicion began; once confirmed
        // gone the suspicion no longer delays it.
        let suspected = OwnerPacing { gone_suspect_at: ago(3600), ..OwnerPacing::default() };
        assert_eq!(earliest_start(&suspected, 600, now), now + TimeDelta::hours(23));
        let gone = OwnerPacing { owner_gone: true, ..suspected };
        assert_eq!(earliest_start(&gone, 600, now), now);
    }
}
