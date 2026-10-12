//! Claiming the next `list_sync` run (design-v3 §4.3 "A separate claim",
//! "Claim-time cause check", "Fair claims").
//!
//! `list_sync` rows are never taken by the general backfill claim. Each
//! lane (fast, and slow for owners that answer slowly) is claimed in one
//! transaction of three statements:
//! 1. an advisory lock, so one claim runs at a time;
//! 2. the lane's queued, due rows are locked;
//! 3. with those locks held, each row's **cause** is read. A row with no
//!    cause is set `done` without a run: an enqueue made for a reference
//!    that has since been withdrawn must not cause a run, or a member could
//!    tell from the timing of its own next mute that someone else
//!    references the same owner. Of the rows that have a cause, one is
//!    picked fairly and set `running`.
//!
//! **Fairness.** A participant is one member's mutes, or one member's
//! blocks, waiting on a sync; plus two shared participants for work nobody
//! waits on ("none": weekly repairs, rechecks) and for the daily runs of
//! owners that were cut ("cut"). The pick goes to the participant that has
//! held the least lane time lately, and then to that participant's best
//! row. So no member can crowd out another, or the repairs, however many
//! owners it names and however long their runs are.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use chrono::{DateTime, Utc};
use sqlx::PgPool;

use super::applier::Waiting;
use super::jobs;

pub const METRIC_CAUSELESS: &str = "list_sync_causeless_total";

/// Which lane a claim is for.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Lane {
    Fast,
    /// Owners whose `slow_until` is in the future.
    Slow,
}

impl Lane {
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Fast => "fast",
            Self::Slow => "slow",
        }
    }
}

/// Who a run is charged to.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum Participant {
    /// One member's references of one source that wait on a sync.
    Pair { member: String, source: String },
    /// Owners that were cut or are slow, with nothing waiting on them.
    Cut,
    /// Everything else nobody waits on.
    None,
}

/// Each participant's runs on a lane: when each began and how long it
/// took, or `None` while it still runs.
type Held = HashMap<(Lane, Participant), Vec<(Instant, Option<Duration>)>>;

/// Lane time each participant has held lately. In-process only.
#[derive(Debug, Default, Clone)]
pub struct LaneTime {
    held: Arc<Mutex<Held>>,
}

/// A run in progress. Dropping it, by any path, ends the run's lane time.
#[derive(Debug)]
pub struct Holding {
    time: LaneTime,
    key: (Lane, Participant),
    started: Instant,
}

impl Drop for Holding {
    fn drop(&mut self) {
        let mut held = self.time.held.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(run) = held.get_mut(&self.key).and_then(|runs| runs.iter_mut().find(|(started, lasted)| *started == self.started && lasted.is_none())) {
            run.1 = Some(self.started.elapsed());
        }
    }
}

impl LaneTime {
    /// Record a run of `participant` on `lane` that began at `started` and
    /// lasted `lasted`.
    pub fn record(&self, lane: Lane, participant: &Participant, started: Instant, lasted: Duration) {
        let mut held = self.held.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        held.entry((lane, participant.clone())).or_default().push((started, Some(lasted)));
    }

    /// Begin a run of `participant` on `lane`. Its time counts from now,
    /// while it runs and after.
    #[must_use]
    pub fn begin(&self, lane: Lane, participant: &Participant) -> Holding {
        let started = Instant::now();
        let key = (lane, participant.clone());
        let mut held = self.held.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        held.entry(key.clone()).or_default().push((started, None));
        Holding { time: self.clone(), key, started }
    }

    /// The time `participant` held `lane` within the last `window`, a run
    /// still in progress included, and when it last began a run.
    fn usage(&self, lane: Lane, participant: &Participant, window: Duration, now: Instant) -> (Duration, Option<Instant>) {
        let mut held = self.held.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        let Some(runs) = held.get_mut(&(lane, participant.clone())) else {
            return (Duration::ZERO, None);
        };
        runs.retain(|(started, lasted)| lasted.is_none_or(|lasted| now.saturating_duration_since(*started + lasted) < window));
        let total = runs.iter().map(|(started, lasted)| lasted.unwrap_or_else(|| now.saturating_duration_since(*started))).sum();
        (total, runs.iter().map(|(started, _)| *started).max())
    }

    /// Drop everything recorded for `member` (a purge of that member).
    pub fn forget_member(&self, member: &str) {
        let mut held = self.held.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        held.retain(|(_, participant), _| !matches!(participant, Participant::Pair { member: m, .. } if m == member));
    }
}

/// A due row with a cause.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Candidate {
    owner: String,
    priority: i16,
    not_before: DateTime<Utc>,
    /// The owner is cut or slow.
    backing_off: bool,
    generation: i64,
}

/// The participant to serve and its best row.
///
/// Each row is a candidate of every pair waiting on its owner. A
/// low-priority row, and a row nobody waits on, is also a candidate of
/// "cut" or "none", so a stale pair can never hold an owner's repairs out
/// of the shared participants. A pair's rows are ordered by priority, then
/// by how long they have been due; the shared participants' rows only by
/// how long they have been due.
fn pick<'a>(
    candidates: &'a [Candidate],
    waiting: &Waiting,
    usage: impl Fn(&Participant) -> (Duration, Option<Instant>),
) -> Option<(Participant, &'a Candidate)> {
    let mut best: HashMap<Participant, &Candidate> = HashMap::new();
    for row in candidates {
        let pairs = waiting.of(&row.owner);
        let shared = if row.backing_off { Participant::Cut } else { Participant::None };
        let mut of: Vec<Participant> = pairs.into_iter().map(|(member, source)| Participant::Pair { member, source }).collect();
        if of.is_empty() || row.priority != jobs::HIGH {
            of.push(shared);
        }
        for participant in of {
            let better = |held: &&Candidate| match participant {
                Participant::Pair { .. } => (row.priority, row.not_before, &row.owner) < (held.priority, held.not_before, &held.owner),
                Participant::Cut | Participant::None => (row.not_before, &row.owner) < (held.not_before, &held.owner),
            };
            if best.get(&participant).is_none_or(better) {
                best.insert(participant, row);
            }
        }
    }
    // Least lane time first; then whoever was served longest ago (never
    // served comes first); then a fixed order, so the choice is stable.
    best.into_iter().min_by_key(|(participant, _)| {
        let (held, last) = usage(participant);
        (held, last.is_some(), last, participant.clone())
    })
}

/// A claimed run.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Claimed {
    pub owner: String,
    pub generation: i64,
    pub participant: Participant,
}

/// One due row as locked, with its owner's state.
type Due = (String, i16, DateTime<Utc>, i64);
/// A due row's causes: the owner, whether it has any, whether it backs off.
type Cause = (String, bool, bool);

/// Claim one run on `lane`, if a due row with a cause exists.
///
/// # Errors
///
/// Database errors.
pub async fn claim(pool: &PgPool, lane: Lane, waiting: &Waiting, time: &LaneTime, window: Duration) -> Result<Option<Claimed>, sqlx::Error> {
    let began = Instant::now();
    let mut tx = pool.begin().await?;
    sqlx::query("SELECT pg_advisory_xact_lock(hashtextextended('claim:list_sync', 0))").execute(&mut *tx).await?;
    // A new statement, so its snapshot follows the lock wait. The fast lane
    // leaves slow owners to the slow lane.
    let due: Vec<Due> = sqlx::query_as(
        "SELECT j.did, j.priority, j.not_before, j.generation FROM backfill_jobs j \
         WHERE j.depth = $1 AND j.state = 'queued' AND j.not_before <= now() \
           AND $2 = EXISTS (SELECT 1 FROM subscribed_owners o WHERE o.owner_did = j.did AND o.slow_until > now()) \
         ORDER BY j.did FOR UPDATE OF j SKIP LOCKED",
    )
    .bind(jobs::DEPTH)
    .bind(lane == Lane::Slow)
    .fetch_all(&mut *tx)
    .await?;
    if due.is_empty() {
        return Ok(None);
    }
    // A further statement, after the row locks: a list phase's enqueue for
    // one of these owners either committed before this snapshot, so its
    // cause is seen, or waits on the row and then queues it again.
    let owners: Vec<&String> = due.iter().map(|(owner, ..)| owner).collect();
    let causes: Vec<Cause> = sqlx::query_as(
        // The unset timestamps compare to NULL, hence the COALESCE.
        "SELECT d.did, COALESCE( \
           EXISTS (SELECT 1 FROM subscribed_lists l WHERE l.owner_did = d.did AND l.state = 'active') \
           AND ( EXISTS (SELECT 1 FROM subscribed_lists l JOIN list_references r ON r.list_uri = l.list_uri \
                         WHERE l.owner_did = d.did AND l.state = 'active' AND r.admitted AND NOT r.effective AND r.unlisted_at IS NULL) \
              OR EXISTS (SELECT 1 FROM subscribed_lists l WHERE l.owner_did = d.did AND l.state = 'active' AND l.synced_at IS NULL) \
              OR o.owner_did IS NULL OR o.last_completed_at IS NULL OR o.last_completed_at < now() - interval '7 days' \
              OR o.gone_suspect_at IS NOT NULL OR o.owner_gone \
              OR o.cut_until > now() OR o.slow_until > now() \
              OR o.resync_requested_at IS NOT NULL ), false), \
           COALESCE(o.cut_until > now() OR o.slow_until > now(), false) \
         FROM unnest($1::text[]) AS d(did) LEFT JOIN subscribed_owners o ON o.owner_did = d.did",
    )
    .bind(&owners)
    .fetch_all(&mut *tx)
    .await?;
    let causes: HashMap<String, (bool, bool)> = causes.into_iter().map(|(owner, has, backing_off)| (owner, (has, backing_off))).collect();
    let mut candidates = Vec::new();
    let mut causeless: Vec<String> = Vec::new();
    for (owner, priority, not_before, generation) in due {
        match causes.get(&owner).copied().unwrap_or((false, false)) {
            (true, backing_off) => candidates.push(Candidate { owner, priority, not_before, backing_off, generation }),
            (false, _) => causeless.push(owner),
        }
    }
    // No run starts for a row without a cause: no run end is recorded, so
    // the owner's pacing does not move.
    sqlx::query("UPDATE backfill_jobs SET state = 'done', generation = generation + 1, rerun_requested = false, updated_at = now() WHERE depth = $1 AND did = ANY($2)")
        .bind(jobs::DEPTH)
        .bind(&causeless)
        .execute(&mut *tx)
        .await?;
    let now = Instant::now();
    let picked = pick(&candidates, waiting, |participant| time.usage(lane, participant, window, now));
    let claimed = match picked {
        None => None,
        Some((participant, row)) => {
            sqlx::query("UPDATE backfill_jobs SET state = 'running', updated_at = now() WHERE depth = $1 AND did = $2")
                .bind(jobs::DEPTH)
                .bind(&row.owner)
                .execute(&mut *tx)
                .await?;
            Some(Claimed { owner: row.owner.clone(), generation: row.generation, participant })
        }
    };
    tx.commit().await?;
    if !causeless.is_empty() {
        metrics::counter!(METRIC_CAUSELESS).increment(u64::try_from(causeless.len()).unwrap_or(u64::MAX));
        // Their references no longer wait. A pair added since this claim
        // began belongs to an enqueue that is about to queue the row again.
        for owner in &causeless {
            waiting.forget_before(owner, began);
        }
    }
    Ok(claimed)
}

#[cfg(test)]
mod tests {
    use chrono::TimeDelta;

    use super::*;
    use crate::subscriptions::list_phase;
    use crate::subscriptions::references::{Budget, mute_list, unmute_list};
    use crate::test_support::fresh_db;

    const A: &str = "did:plc:aaaaaaaaaaaaaaaaaaaaaaaa";
    const B: &str = "did:plc:bbbbbbbbbbbbbbbbbbbbbbbb";
    const M1: &str = "did:plc:mmmmmmmmmmmmmmmmmmmmmmm1";
    const M2: &str = "did:plc:mmmmmmmmmmmmmmmmmmmmmmm2";
    const WINDOW: Duration = Duration::from_secs(3600);

    fn row(owner: &str, priority: i16, due_secs_ago: i64, backing_off: bool) -> Candidate {
        Candidate {
            owner: owner.to_string(),
            priority,
            not_before: DateTime::<Utc>::UNIX_EPOCH + TimeDelta::seconds(1_000_000 - due_secs_ago),
            backing_off,
            generation: 0,
        }
    }

    fn pair(member: &str) -> Participant {
        Participant::Pair { member: member.to_string(), source: "mute".to_string() }
    }

    #[test]
    fn the_pick_is_fair_between_participants() {
        let waiting = Waiting::default();
        waiting.add(A, M1, "mute");
        waiting.add(B, M2, "mute");
        let rows = [row(A, jobs::HIGH, 10, false), row(B, jobs::HIGH, 5, false), row("did:plc:cccc", jobs::LOW, 500, false)];
        let time = LaneTime::default();
        let now = Instant::now();
        let usage = |p: &Participant| time.usage(Lane::Fast, p, WINDOW, now);
        // Nobody has run: the fixed order decides, and each participant's
        // row is its own.
        let (first, picked) = pick(&rows, &waiting, usage).expect("pick");
        assert_eq!((first.clone(), picked.owner.as_str()), (pair(M1), A));
        // M1 has held the lane: the others come first, M1 last.
        time.record(Lane::Fast, &pair(M1), now.checked_sub(Duration::from_secs(60)).expect("instant"), Duration::from_secs(50));
        let (next, picked) = pick(&rows, &waiting, usage).expect("pick");
        assert_eq!((next, picked.owner.as_str()), (pair(M2), B));
        time.record(Lane::Fast, &pair(M2), now.checked_sub(Duration::from_secs(30)).expect("instant"), Duration::from_secs(20));
        // The repair nobody waits on is not starved by the two members.
        let (next, picked) = pick(&rows, &waiting, usage).expect("pick");
        assert_eq!((next, picked.owner.as_str()), (Participant::None, "did:plc:cccc"));
        time.record(Lane::Fast, &Participant::None, now, Duration::from_secs(40));
        // By time, not by count: M2 held 20 s, less than M1's 50 s.
        assert_eq!(pick(&rows, &waiting, usage).expect("pick").0, pair(M2));
        // The slow lane keeps its own account.
        assert_eq!(time.usage(Lane::Slow, &pair(M1), WINDOW, now).0, Duration::ZERO);
        // Time outside the window no longer counts.
        assert_eq!(time.usage(Lane::Fast, &pair(M1), Duration::from_secs(5), now).0, Duration::ZERO);

        // A low-priority row of an owner someone waits on is also a
        // candidate of the shared participant; a cut owner's of "cut".
        let rows = [row(A, jobs::LOW, 10, true)];
        let fresh = LaneTime::default();
        fresh.record(Lane::Fast, &pair(M1), now, Duration::from_secs(1));
        let (who, _) = pick(&rows, &waiting, |p| fresh.usage(Lane::Fast, p, WINDOW, now)).expect("pick");
        assert_eq!(who, Participant::Cut);
        assert!(pick(&[], &waiting, usage).is_none());
        // A purge of a member drops its account.
        time.forget_member(M1);
        assert_eq!(time.usage(Lane::Fast, &pair(M1), WINDOW, now).0, Duration::ZERO);
    }

    fn list(owner: &str) -> String {
        format!("at://{owner}/app.bsky.graph.list/3klist1")
    }

    /// B-74: a queued row whose reference was withdrawn ends without a run
    /// and without moving the owner's pacing; a row with a cause is
    /// claimed; the slow lane takes only slow owners.
    #[tokio::test]
    async fn a_claim_needs_a_cause() {
        let Some(db) = fresh_db("subscriptions_claim").await else {
            return;
        };
        let p = &db.pool;
        let budget = Budget { max_references: 5, max_owners: 5 };
        let (waiting, time) = (Waiting::default(), LaneTime::default());
        let take = |lane: Lane| {
            let (waiting, time) = (&waiting, &time);
            async move { claim(p, lane, waiting, time, WINDOW).await.expect("claim") }
        };
        let job = |owner: &'static str| async move {
            sqlx::query_as::<_, (String, i64)>("SELECT state, generation FROM backfill_jobs WHERE did = $1").bind(owner).fetch_one(p).await.expect("job")
        };
        assert_eq!(take(Lane::Fast).await, None);

        // A mute queues the owner's sync; the mute is withdrawn before the
        // claim. The owner completed a run recently, so nothing else is a
        // cause.
        mute_list(p, M1, &list(A), budget).await.expect("mute");
        list_phase::apply(p, &list(A), 600).await.expect("apply");
        waiting.add(A, M1, "mute");
        sqlx::query("UPDATE subscribed_owners SET last_completed_at = now()").execute(p).await.expect("completed");
        sqlx::query("UPDATE subscribed_lists SET synced_at = now()").execute(p).await.expect("synced");
        sqlx::query("UPDATE list_references SET effective = true").execute(p).await.expect("effective");
        assert_eq!(take(Lane::Fast).await, None);
        assert_eq!(job(A).await, ("done".to_string(), 1));
        assert!(waiting.of(A).is_empty());
        let ended: bool = sqlx::query_scalar("SELECT last_run_ended_at IS NULL FROM subscribed_owners").fetch_one(p).await.expect("owner");
        assert!(ended, "no run end is recorded for a row that had no cause");

        // A reference that waits is a cause.
        mute_list(p, M2, &list(A), budget).await.expect("mute");
        list_phase::apply(p, &list(A), 600).await.expect("apply");
        waiting.add(A, M2, "mute");
        let claimed = take(Lane::Fast).await.expect("claimed");
        assert_eq!((claimed.owner.as_str(), claimed.generation, &claimed.participant), (A, 1, &pair(M2)));
        assert_eq!(job(A).await.0, "running");
        // A running row is not claimed twice.
        assert_eq!(take(Lane::Fast).await, None);

        // An owner with no active list never runs, whatever else holds.
        unmute_list(p, M1, &list(A), budget).await.expect("unmute");
        mute_list(p, M1, &list(B), budget).await.expect("mute");
        list_phase::apply(p, &list(B), 600).await.expect("apply");
        // B is slow: only the slow lane takes it.
        sqlx::query("UPDATE subscribed_owners SET slow_until = now() + interval '1 day' WHERE owner_did = $1").bind(B).execute(p).await.expect("slow");
        assert_eq!(take(Lane::Fast).await, None);
        assert_eq!(take(Lane::Slow).await.map(|c| c.owner), Some(B.to_string()));
        db.drop_db().await;
    }
}
