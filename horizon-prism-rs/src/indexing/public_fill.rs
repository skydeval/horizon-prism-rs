//! The fill (public-indexing design §8.3–§8.5): the pass that gives every
//! member, then every other S DID, a `resync` with a staggered due time,
//! and the checker that turns public indexing On when those jobs are done
//! or the bound fires.
//!
//! Every input to a due time — `part`, `after`, `enqueued` and `since` — is
//! committed with the batch that used it, so a restart resumes exactly.

use std::time::{Duration, Instant};

use chrono::{DateTime, TimeDelta, Utc};
use serde_json::Value;

use super::public_mode::{FillPart, FillStage, FillState, Phase, PublicIndexing, PublicMode, PublicState, Spawn, Transition, g64, now, replace_if};
use crate::aggregations::rebuild::repair_counts;
use crate::firehose::jobs;

/// `public_fill_jobs_remaining`.
pub const METRIC_REMAINING: &str = "public_fill_jobs_remaining";
/// `public_fill_incomplete_jobs`.
pub const METRIC_INCOMPLETE: &str = "public_fill_incomplete_jobs";
/// `public_fill_repair_seconds`.
pub const METRIC_REPAIR_SECONDS: &str = "public_fill_repair_seconds";

/// The jobs the fill waits on (§8.5): resyncs of members and S DIDs that
/// are fresh, due, running or in backoff. A failed or unavailable job, and one the
/// 6-hourly loop re-queued (`attempts = 0` with `last_error` set), are not.
const REMAINING: &str = "SELECT count(*) FROM backfill_jobs j \
     WHERE j.depth = 'resync' AND j.state IN ('queued', 'running') \
       AND (j.last_error IS NULL OR j.attempts > 0) \
       AND (EXISTS (SELECT 1 FROM members m WHERE m.did = j.did) \
            OR EXISTS (SELECT 1 FROM scope_dids s WHERE s.did = j.did))";

/// What one fill batch did.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum FillBatch {
    /// The in-memory generation is not the task's.
    Exit,
    /// The conditional write missed; the enqueues rolled back.
    Superseded,
    /// Committed; the pass goes on from this state.
    Continue(FillState),
    /// Committed in stage `waiting`; the checker takes over.
    Waiting(FillState),
}

/// Whether the bound fires (§8.5): `fill_max_wait` after `waiting_since`.
#[must_use]
pub fn bound_fired(now: DateTime<Utc>, waiting_since: DateTime<Utc>, fill_max_wait: Duration) -> bool {
    TimeDelta::from_std(fill_max_wait).is_ok_and(|max| now >= waiting_since + max)
}

/// The i-th DID's due time: `since + (enqueued + i)` seconds.
fn due(since: DateTime<Utc>, enqueued: u64, i: usize) -> DateTime<Utc> {
    let offset = enqueued.saturating_add(u64::try_from(i).unwrap_or(u64::MAX));
    since + TimeDelta::seconds(i64::try_from(offset).unwrap_or(i64::MAX))
}

impl PublicIndexing {
    /// One batch of §8.3, steps 1–7.
    pub(super) async fn fill_batch(&self, generation: u64, fill: &FillState) -> Result<FillBatch, sqlx::Error> {
        // 1. The guard before the connection.
        let guard = self.scope.read_guard().await;
        if guard.public.generation != generation {
            return Ok(FillBatch::Exit);
        }
        let mut tx = self.pool.begin().await?;
        // 3. Up to a batch of DIDs after `after`, in the part's order.
        let sql = match fill.part {
            FillPart::Members => "SELECT did FROM members WHERE did > $1 ORDER BY did LIMIT $2",
            FillPart::Scope => {
                "SELECT s.did FROM scope_dids s WHERE s.did > $1 \
                 AND NOT EXISTS (SELECT 1 FROM members m WHERE m.did = s.did) ORDER BY s.did LIMIT $2"
            }
        };
        let dids: Vec<String> = sqlx::query_scalar(sql)
            .bind(&fill.after)
            .bind(i64::from(self.settings.fill_batch))
            .fetch_all(&mut *tx)
            .await?;
        let mut next = fill.clone();
        if let Some(last) = dids.last() {
            // 5. Staggered due times; a queued resync is left alone.
            let not_befores: Vec<DateTime<Utc>> = (0..dids.len()).map(|i| due(fill.since, fill.enqueued, i)).collect();
            jobs::enqueue_staggered(&mut tx, &dids, &not_befores).await?;
            next.after.clone_from(last);
            next.enqueued += u64::try_from(dids.len()).unwrap_or(u64::MAX);
        } else {
            // 4. The part is done.
            match fill.part {
                FillPart::Members => {
                    next.part = FillPart::Scope;
                    next.after = String::new();
                }
                FillPart::Scope => {
                    next.stage = FillStage::Waiting;
                    next.waiting_since = Some(now().max(due(fill.since, fill.enqueued, 0)));
                }
            }
        }
        self.pause("fill_write").await;
        // 6. The new state, conditional on the phase and the generation.
        let n = sqlx::query(
            "UPDATE worker_state SET value = jsonb_set(value, '{fill}', $3), updated_at = now() \
             WHERE key = 'public_transition' AND value->>'phase' = $1 AND (value->>'generation')::bigint = $2",
        )
        .bind(Phase::Filling.as_str())
        .bind(g64(generation))
        .bind(serde_json::to_value(&next).unwrap_or(Value::Null))
        .execute(&mut *tx)
        .await?
        .rows_affected();
        if n == 0 {
            return Ok(FillBatch::Superseded);
        }
        // 7.
        tx.commit().await?;
        drop(guard);
        Ok(if next.stage == FillStage::Waiting { FillBatch::Waiting(next) } else { FillBatch::Continue(next) })
    }

    /// The fill pass: batches until stage `waiting`, then the checker.
    pub(super) async fn run_fill(self, generation: u64, mut fill: FillState) {
        loop {
            match self.fill_batch(generation, &fill).await {
                Ok(FillBatch::Exit) => return,
                Ok(FillBatch::Superseded) => return Self::superseded("fill", generation),
                Ok(FillBatch::Continue(next)) => fill = next,
                Ok(FillBatch::Waiting(next)) => {
                    tracing::info!(event = "public_fill_enqueued", generation, jobs = next.enqueued, "every DID has its resync; waiting for them");
                    if let Some(waiting_since) = next.waiting_since {
                        drop(self.spawn(Spawn::Checker { generation, waiting_since }));
                    }
                    return;
                }
                Err(error) => {
                    tracing::error!(event = "public_fill_failed", generation, %error, "fill batch failed; retrying");
                    tokio::time::sleep(self.settings.failure_pause).await;
                }
            }
        }
    }

    /// The jobs the fill still waits on.
    pub(super) async fn fill_remaining(&self) -> Result<i64, sqlx::Error> {
        sqlx::query_scalar(REMAINING).fetch_one(&self.pool).await
    }

    /// The checker (§8.5): every `check_every`, exits on a generation
    /// mismatch; when no job is left, or the bound fires, repairs the
    /// counts and turns On.
    pub(super) async fn run_checker(self, generation: u64, waiting_since: DateTime<Utc>) {
        loop {
            if self.scope.read_guard().await.public.generation != generation {
                return;
            }
            match self.fill_remaining().await {
                Ok(remaining) => {
                    #[allow(clippy::cast_precision_loss)]
                    metrics::gauge!(METRIC_REMAINING).set(remaining as f64);
                    self.progress(|p| p.fill_remaining = Some(remaining));
                    let bound = bound_fired(now(), waiting_since, self.settings.fill_max_wait);
                    if remaining == 0 || bound {
                        if remaining > 0 {
                            #[allow(clippy::cast_precision_loss)]
                            metrics::gauge!(METRIC_INCOMPLETE).set(remaining as f64);
                            self.progress(|p| p.fill_incomplete = Some(remaining));
                            tracing::warn!(event = "public_fill_incomplete", generation, jobs = remaining, "the fill bound fired; public endpoints are served while the last resyncs finish");
                        }
                        match self.finish_fill(generation, true).await {
                            Ok(()) => return,
                            Err(error) => tracing::error!(event = "public_fill_failed", generation, %error, "ending the fill failed; retrying"),
                        }
                    }
                }
                Err(error) => tracing::warn!(event = "public_fill_check_failed", generation, %error, "counting the fill's jobs failed"),
            }
            tokio::time::sleep(self.settings.check_every).await;
        }
    }

    /// §8.5's end: `repair_counts` outside the guard, then On under the
    /// write guard, conditional on the phase and the generation.
    /// `check_memory` is false only in tests, to show the database barrier
    /// holds by itself.
    pub(super) async fn finish_fill(&self, generation: u64, check_memory: bool) -> Result<(), sqlx::Error> {
        if check_memory && self.scope.read_guard().await.public.generation != generation {
            return Ok(());
        }
        #[cfg(test)]
        self.hooks.repairs.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        let started = Instant::now();
        repair_counts(&self.pool).await?;
        metrics::histogram!(METRIC_REPAIR_SECONDS).record(started.elapsed().as_secs_f64());
        let mut sets = self.scope.write_guard().await;
        if check_memory && sets.public.generation != generation {
            return Ok(());
        }
        let next = generation + 1;
        let last_fill_since = sets.public.last_fill_since;
        let t = Transition::settled(Phase::On, next, now(), last_fill_since);
        let mut tx = self.pool.begin().await?;
        if !replace_if(&mut tx, Phase::Filling, generation, &t).await? {
            Self::superseded("checker", generation);
            return Ok(());
        }
        tx.commit().await?;
        self.set_state(&mut sets, PublicState { mode: PublicMode::On, generation: next, last_fill_since });
        drop(sets);
        tracing::info!(event = "public_fill_done", generation = next, repair_secs = started.elapsed().as_secs_f64(), "public indexing is on");
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::indexing::public_mode::parse_ts;

    /// T-20: the bound fires no earlier than `fill_max_wait` after
    /// `waiting_since`.
    #[test]
    fn the_bound_counts_from_waiting_since() {
        let ws = parse_ts(&serde_json::json!("2026-10-14T12:00:00Z")).expect("ts");
        let max = Duration::from_secs(60);
        assert!(!bound_fired(ws, ws, max));
        assert!(!bound_fired(ws + TimeDelta::seconds(59), ws, max));
        assert!(bound_fired(ws + TimeDelta::seconds(60), ws, max));
    }

    #[test]
    fn due_times_are_since_plus_enqueued_plus_i() {
        let since = parse_ts(&serde_json::json!("2026-10-14T12:00:00Z")).expect("ts");
        assert_eq!(due(since, 0, 0), since);
        assert_eq!(due(since, 3, 2), since + TimeDelta::seconds(5));
    }
}
