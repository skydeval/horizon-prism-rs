//! One `list_sync` run from its claim to its end (design-v3 §4.3 "The
//! run").
//!
//! 1. The snapshot. A run for an owner that is being purged stands down;
//!    one that finds no active list ends.
//! 2. The owner's status, asked once. An owner that is not there is first
//!    *suspected*: nothing is listed, nothing changes for members, and the
//!    run comes back a day later. Only the same answer a day or more after
//!    the suspicion began, or a tombstone in the PLC directory, confirms it,
//!    and then the owner's lists are emptied.
//! 3. The two listings, within the per-owner bounds.
//! 4. The purposes, the reconcile batches, and the completion; or, for a
//!    listing that was cut, the additions only and a retry a day later.
//!
//! Every way out records the run's end on the owner's row and settles the
//! job, so the owner's pacing never depends on how a run ended.

use async_trait::async_trait;
use sqlx::PgPool;

use super::copy::Limits;
use super::fetch::{self, Bounds, CutReason, FetchError, OwnerRecords};
use super::jobs::{self, Cut, RunEnd};
use super::sync::{self, Stopped};
use crate::aggregations::ScopeState;

pub const METRIC_RUNS: &str = "list_sync_runs_total";
pub const METRIC_CUT: &str = "list_sync_run_cut_total";
pub const METRIC_TRUNCATED: &str = "list_sync_truncated_total";

/// What the owner's account is, as far as one look can tell.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OwnerStatus {
    /// The repository is active, at revision `rev`.
    Active { rev: String },
    /// The PLC directory says the identity was deleted: gone at once.
    Tombstoned,
    /// Not active, with the key the suspicion is held under: the PDS URL,
    /// `did:web`, or `none`. A changed key (a migration) starts the
    /// suspicion over rather than confirming it.
    NotActive { key: String },
}

/// Where a run learns about an owner and reads its records.
#[async_trait]
pub trait OwnerSource: Send + Sync {
    /// The owner's status. `Err` is a transient failure (a timeout, a
    /// server error): it says nothing, and the run is retried.
    async fn status(&self, owner: &str) -> Result<OwnerStatus, String>;
    /// The owner's records, read at the PDS `status` found, for the run
    /// claimed at `generation`.
    async fn records(&self, owner: &str, generation: i64) -> Result<Box<dyn OwnerRecords>, String>;
}

/// What a run needs from the configuration.
#[derive(Debug, Clone, Copy)]
pub struct Settings {
    pub max_items: u32,
    /// `lists.subscription_tombstones_max_per_owner`.
    pub tombstones_max_per_owner: u32,
    pub min_interval_secs: u32,
    pub bounds: Bounds,
}

/// How a run ended.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Outcome {
    Completed,
    /// The owner has no active list.
    Empty,
    Cut(CutReason),
    /// The owner looks gone; the run comes back a day after the suspicion
    /// began.
    Suspected,
    OwnerGone,
    /// The owner is being purged: nothing was listed.
    Purging,
    /// The job was superseded: nothing more was written.
    Superseded,
    Failed(String),
}

impl Outcome {
    #[must_use]
    pub fn label(&self) -> &'static str {
        match self {
            Self::Completed => "completed",
            Self::Empty => "empty",
            Self::Cut(_) => "cut",
            Self::Suspected => "suspected",
            Self::OwnerGone => "owner_gone",
            Self::Purging => "purging",
            Self::Superseded => "superseded",
            Self::Failed(_) => "failed",
        }
    }
}

/// A suspicion is confirmed only this long after it began.
const CONFIRM_AFTER: &str = "24 hours";

/// The run. `lane` only labels metrics.
pub async fn run(
    pool: &PgPool,
    scope: &ScopeState,
    source: &dyn OwnerSource,
    owner: &str,
    generation: i64,
    settings: Settings,
    lane: &'static str,
) -> Outcome {
    let outcome = match attempt(pool, scope, source, owner, generation, settings, lane).await {
        Ok(outcome) => outcome,
        Err(Stopped::Superseded) => Outcome::Superseded,
        Err(Stopped::Db(error)) => {
            // Best effort: if this fails too, the stuck-row check finds it.
            let message = error.to_string();
            let _ = jobs::end_run(pool, owner, generation, RunEnd::Failed(&message), None, settings.min_interval_secs).await;
            Outcome::Failed(message)
        }
    };
    metrics::counter!(METRIC_RUNS, "outcome" => outcome.label(), "lane" => lane).increment(1);
    outcome
}

async fn attempt(
    pool: &PgPool,
    scope: &ScopeState,
    source: &dyn OwnerSource,
    owner: &str,
    generation: i64,
    settings: Settings,
    lane: &'static str,
) -> Result<Outcome, Stopped> {
    let pause = settings.min_interval_secs;
    let end = |end: RunEnd<'static>, cut: Option<Cut>| async move { jobs::end_run(pool, owner, generation, end, cut, pause).await };
    // 1. A purge withholds the owner's changes: a listing taken now could
    // not be trusted, so nothing is listed.
    if scope.read_guard().await.purge_kind(owner).is_some() {
        end(RunEnd::Requeue, None).await?;
        return Ok(Outcome::Purging);
    }
    let snap = sync::snapshot(pool, owner, generation).await?;
    if snap.lists.is_empty() {
        end(RunEnd::Done, None).await?;
        return Ok(Outcome::Empty);
    }
    // 2. The owner's status, once.
    let rev = match source.status(owner).await {
        Err(error) => {
            jobs::end_run(pool, owner, generation, RunEnd::Failed(&error), None, pause).await?;
            return Ok(Outcome::Failed(error));
        }
        Ok(OwnerStatus::Tombstoned) => {
            sync::owner_gone(pool, &snap, pause).await?;
            return Ok(Outcome::OwnerGone);
        }
        Ok(OwnerStatus::NotActive { key }) => {
            // Confirmed: the same answer, a day or more after it was first
            // given. Otherwise the suspicion begins, or begins again.
            let confirmed: bool = sqlx::query_scalar(&format!(
                "SELECT EXISTS (SELECT 1 FROM subscribed_owners WHERE owner_did = $1 AND gone_suspect_pds = $2 \
                   AND gone_suspect_at <= now() - interval '{CONFIRM_AFTER}')"
            ))
            .bind(owner)
            .bind(&key)
            .fetch_one(pool)
            .await?;
            if confirmed {
                sync::owner_gone(pool, &snap, pause).await?;
                return Ok(Outcome::OwnerGone);
            }
            let mut tx = pool.begin().await?;
            jobs::record_run_end(&mut tx, owner, None).await?;
            sqlx::query(
                "UPDATE subscribed_owners SET gone_suspect_at = CASE WHEN gone_suspect_pds IS NOT DISTINCT FROM $2 \
                   THEN COALESCE(gone_suspect_at, now()) ELSE now() END, gone_suspect_pds = $2 WHERE owner_did = $1",
            )
            .bind(owner)
            .bind(&key)
            .execute(&mut *tx)
            .await?;
            jobs::settle(&mut tx, owner, generation, RunEnd::Requeue, pause).await?;
            tx.commit().await?;
            return Ok(Outcome::Suspected);
        }
        Ok(OwnerStatus::Active { rev }) => rev,
    };
    sqlx::query("UPDATE subscribed_owners SET gone_suspect_at = NULL, gone_suspect_pds = NULL WHERE owner_did = $1 AND gone_suspect_at IS NOT NULL")
        .bind(owner)
        .execute(pool)
        .await?;
    // 3. The listings.
    let records = match source.records(owner, generation).await {
        Ok(records) => records,
        Err(error) => {
            jobs::end_run(pool, owner, generation, RunEnd::Failed(&error), None, pause).await?;
            return Ok(Outcome::Failed(error));
        }
    };
    let fetched = match fetch::fetch(records.as_ref(), &rev, settings.bounds).await {
        Ok(fetched) => fetched,
        Err(FetchError::Superseded) => return Err(Stopped::Superseded),
        Err(FetchError::Failed(error)) => {
            jobs::end_run(pool, owner, generation, RunEnd::Failed(&error), None, pause).await?;
            return Ok(Outcome::Failed(error));
        }
        // The list records were listed whole before the failure: what
        // they say about each list's purpose is written (step 3a), and
        // the run fails and is retried for the items.
        Err(FetchError::ItemsFailed { error, listing }) => {
            sync::write_purposes(pool, &snap, &listing).await?;
            jobs::end_run(pool, owner, generation, RunEnd::Failed(&error), None, pause).await?;
            return Ok(Outcome::Failed(error));
        }
    };
    // 4. After the last request to the owner's server: the purposes, then
    // the items.
    let listed = sync::write_purposes(pool, &snap, &fetched.listing).await?;
    let limits = Limits { max_items: settings.max_items, tombstones_max_per_owner: settings.tombstones_max_per_owner };
    let served = sync::reconcile(pool, &snap, &fetched.listing, limits).await?;
    // The run may have written and evicted tombstones of this owner.
    scope.reload_owner_tombstones(pool, owner).await?;
    match fetched.cut {
        None => {
            sync::complete(pool, &snap, &fetched.listing, &served, &listed, settings.max_items, pause).await?;
            Ok(Outcome::Completed)
        }
        Some(reason) => {
            tracing::info!(event = "list_sync_truncated", owner, reason = reason.as_str(), "a list owner's listing was cut short");
            metrics::counter!(METRIC_TRUNCATED).increment(1);
            metrics::counter!(METRIC_CUT, "reason" => reason.as_str(), "lane" => lane).increment(1);
            let cut = if reason.slow() { Cut::Slow } else { Cut::Large };
            end(RunEnd::Requeue, Some(cut)).await?;
            Ok(Outcome::Cut(reason))
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;
    use std::time::Duration;

    use serde_json::json;

    use super::*;
    use crate::aggregations::{PurgeKind, ScopeSets};
    use crate::config::ScopePolicy;
    use crate::firehose::backfill::ListedRecord;
    use crate::subscriptions::fetch::{NSID_LIST, Page};
    use crate::subscriptions::list_phase;
    use crate::subscriptions::references::{Budget, mute_list};
    use crate::test_support::fresh_db;

    const ME: &str = "did:plc:mmmmmmmmmmmmmmmmmmmmmmmm";
    const OWNER: &str = "did:plc:aaaaaaaaaaaaaaaaaaaaaaaa";
    const BUDGET: Budget = Budget { max_references: 5, max_owners: 5 };

    /// An owner whose status is whatever the test last set, with one
    /// moderation list of `items` items.
    struct Fake {
        status: Mutex<Result<OwnerStatus, String>>,
        items: usize,
    }

    struct Records {
        items: usize,
    }

    #[async_trait]
    impl OwnerRecords for Records {
        async fn page(&self, collection: &str, cursor: Option<&str>) -> Result<Page, String> {
            let list = format!("at://{OWNER}/{NSID_LIST}/3klist1");
            let from: usize = cursor.and_then(|c| c.parse().ok()).unwrap_or(0);
            let (total, per_page) = if collection == NSID_LIST { (1, 10) } else { (self.items, 2) };
            let to = (from + per_page).min(total);
            let records = (from..to)
                .map(|n| ListedRecord {
                    uri: if collection == NSID_LIST { list.clone() } else { format!("at://{OWNER}/{collection}/3k{n:03}") },
                    cid: "bafy".into(),
                    value: if collection == NSID_LIST {
                        json!({"purpose": "app.bsky.graph.defs#modlist"})
                    } else {
                        json!({"list": list, "subject": format!("did:plc:s{n}")})
                    },
                })
                .collect();
            Ok(Page { records, cursor: (to < total).then(|| to.to_string()), call_time: Duration::from_millis(1) })
        }
    }

    #[async_trait]
    impl OwnerSource for Fake {
        async fn status(&self, _owner: &str) -> Result<OwnerStatus, String> {
            self.status.lock().expect("status").clone()
        }
        async fn records(&self, _owner: &str, _generation: i64) -> Result<Box<dyn OwnerRecords>, String> {
            Ok(Box::new(Records { items: self.items }))
        }
    }

    fn settings(max_pages: u32) -> Settings {
        Settings {
            max_items: 100,
            tombstones_max_per_owner: 10_000,
            min_interval_secs: 600,
            bounds: Bounds {
                max_records: 1000,
                max_pages,
                probe_call: Duration::from_secs(60),
                min_records_per_call_sec: 200,
                max_call: Duration::from_secs(900),
                max_wall: Duration::from_secs(3600),
            },
        }
    }

    /// The owner's state and the member's reference after a run:
    /// `(job state, copied items, effective, owner_gone, suspected)`.
    async fn state(pool: &PgPool) -> (String, i64, bool, bool, bool) {
        sqlx::query_as(
            "SELECT (SELECT state FROM backfill_jobs WHERE did = $1), (SELECT count(*) FROM subscribed_list_items), \
               (SELECT effective FROM list_references), COALESCE((SELECT owner_gone FROM subscribed_owners), false), \
               COALESCE((SELECT gone_suspect_at IS NOT NULL FROM subscribed_owners), false)",
        )
        .bind(OWNER)
        .fetch_one(pool)
        .await
        .expect("state")
    }

    /// B-88, B-89: the ways a run ends.
    #[tokio::test]
    async fn a_run_ends_one_way_or_another() {
        let Some(db) = fresh_db("subscriptions_run").await else {
            return;
        };
        let p = &db.pool;
        let scope = ScopeState::new(ScopeSets::default(), ScopePolicy::MembersAndOneHop, 10_000);
        let list = format!("at://{OWNER}/{NSID_LIST}/3klist1");
        let fake = Fake { status: Mutex::new(Err("timeout".into())), items: 5 };
        let set = |status: Result<OwnerStatus, String>| *fake.status.lock().expect("status") = status;
        let claim = || async {
            sqlx::query_scalar::<_, i64>("UPDATE backfill_jobs SET state = 'running', attempts = 0 WHERE did = $1 RETURNING generation")
                .bind(OWNER)
                .fetch_one(p)
                .await
                .expect("claim")
        };
        let go = |max_pages: u32| {
            let (scope, fake) = (&scope, &fake);
            async move { run(p, scope, fake, OWNER, claim().await, settings(max_pages), "fast").await }
        };
        // No active list: the run ends without asking anything.
        sqlx::query("INSERT INTO backfill_jobs (did, depth, state) VALUES ($1, 'list_sync', 'queued')").bind(OWNER).execute(p).await.expect("job");
        assert_eq!(go(100).await, Outcome::Empty);

        mute_list(p, ME, &list, BUDGET).await.expect("mute");
        list_phase::apply(p, &list, 600).await.expect("list phase");
        // A status that could not be had: the run fails and will retry.
        assert_eq!(go(100).await, Outcome::Failed("timeout".into()));
        assert_eq!(state(p).await, ("queued".to_string(), 0, false, false, false));
        // A listing cut short: what was read is kept, nothing takes effect,
        // and the owner backs off.
        set(Ok(OwnerStatus::Active { rev: "3kaaa1".into() }));
        assert_eq!(go(2).await, Outcome::Cut(CutReason::Pages));
        let (job, copied, effective, ..) = state(p).await;
        assert!(job == "queued" && copied == 4 && !effective, "{job} {copied} {effective}");
        // A whole run.
        assert_eq!(go(100).await, Outcome::Completed);
        assert_eq!(state(p).await, ("done".to_string(), 5, true, false, false));
        let backing_off: bool = sqlx::query_scalar("SELECT cut_until IS NOT NULL OR slow_until IS NOT NULL FROM subscribed_owners").fetch_one(p).await.expect("owner");
        assert!(!backing_off, "a complete listing clears the backoffs");

        // The owner stops answering as an account: suspected first. The
        // stored items keep applying.
        set(Ok(OwnerStatus::NotActive { key: "https://pds.one".into() }));
        assert_eq!(go(100).await, Outcome::Suspected);
        assert_eq!(state(p).await, ("queued".to_string(), 5, true, false, true));
        // The same answer again, within a day: still only suspected.
        assert_eq!(go(100).await, Outcome::Suspected);
        // Another PDS (a migration): the suspicion starts over.
        sqlx::query("UPDATE subscribed_owners SET gone_suspect_at = now() - interval '25 hours'").execute(p).await.expect("age");
        set(Ok(OwnerStatus::NotActive { key: "https://pds.two".into() }));
        assert_eq!(go(100).await, Outcome::Suspected);
        // Active again: the suspicion is cleared.
        set(Ok(OwnerStatus::Active { rev: "3kaaa2".into() }));
        assert_eq!(go(100).await, Outcome::Completed);
        assert!(!state(p).await.4);
        // Not active, and the same a day later: confirmed gone. Its lists
        // are emptied and are no moderation lists.
        set(Ok(OwnerStatus::NotActive { key: "https://pds.two".into() }));
        assert_eq!(go(100).await, Outcome::Suspected);
        sqlx::query("UPDATE subscribed_owners SET gone_suspect_at = now() - interval '25 hours'").execute(p).await.expect("age");
        assert_eq!(go(100).await, Outcome::OwnerGone);
        assert_eq!(state(p).await, ("done".to_string(), 0, true, true, true));
        let purpose: Option<String> = sqlx::query_scalar("SELECT purpose FROM subscribed_lists").fetch_one(p).await.expect("purpose");
        assert_eq!(purpose, None);
        // Back again: a normal completion clears `owner_gone`.
        set(Ok(OwnerStatus::Active { rev: "3kaaa3".into() }));
        assert_eq!(go(100).await, Outcome::Completed);
        assert_eq!(state(p).await, ("done".to_string(), 5, true, false, false));
        // A PLC tombstone confirms at once.
        set(Ok(OwnerStatus::Tombstoned));
        assert_eq!(go(100).await, Outcome::OwnerGone);

        // An owner being purged is not listed; a superseded run writes
        // nothing.
        scope.write_guard().await.purging.insert(OWNER.to_string(), PurgeKind::AccountDeleted);
        set(Ok(OwnerStatus::Active { rev: "3kaaa4".into() }));
        assert_eq!(go(100).await, Outcome::Purging);
        scope.write_guard().await.purging.clear();
        let stale = claim().await - 1;
        assert_eq!(run(p, &scope, &fake, OWNER, stale, settings(100), "fast").await, Outcome::Superseded);
        db.drop_db().await;
    }
}
