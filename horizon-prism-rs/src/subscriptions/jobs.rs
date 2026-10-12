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
/// The priority only ever rises on a live row. The generation is never
/// changed here.
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

#[cfg(test)]
mod tests {
    use super::*;

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
