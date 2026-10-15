//! The reference applier: the background task that runs the list phase
//! and hands unreferenced lists to their release (design-v3 §4.3 "List
//! phase", "Release", "Crash safety").
//!
//! It runs when a member phase signals it, at startup, and every 60 s in
//! any case. A list whose row is held by another transaction is skipped and
//! tried again after a second; one that stays busy for 30 s gets a task of
//! its own that waits for the row. Releases run in tasks of their own, one
//! per list at a time, so a long release never holds up the applier.
//!
//! After each commit the in-memory sets are set from the rows, and the
//! `(member, source)` pair of every reference that waits on a sync is
//! added to its owner's waiting set, which the sync claim reads to share
//! its time fairly.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use sqlx::PgPool;
use tokio::sync::Notify;

use super::claim::LaneTime;
use super::list_phase::{self, Applied, ListState};
use crate::aggregations::ScopeState;
use crate::db::locks::uri_author;

/// The fallback period: markers are looked for this often with no signal.
pub const FALLBACK: Duration = Duration::from_secs(60);
/// A busy list is tried again after this long.
pub const RETRY: Duration = Duration::from_secs(1);
/// A list busy for this long is handed to a task that waits for its row.
pub const HAND_OVER: Duration = Duration::from_secs(30);

pub const METRIC_RELEASING: &str = "subscribed_lists_releasing";

/// By owner: the `(member, source)` pairs whose references wait on that
/// owner's sync, each with the time it was last added. In-process only: a
/// stored copy would record a private mute outside the tables a purge
/// reaches.
#[derive(Debug, Default, Clone)]
pub struct Waiting {
    owners: Arc<Mutex<HashMap<String, Pairs>>>,
}

/// One owner's waiting `(member, source)` pairs, with their latest add.
type Pairs = HashMap<(String, String), Instant>;

impl Waiting {
    /// Add a pair, or refresh the time of one already there.
    pub fn add(&self, owner: &str, member: &str, source: &str) {
        let mut owners = self.owners.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        owners
            .entry(owner.to_string())
            .or_default()
            .insert((member.to_string(), source.to_string()), Instant::now());
    }

    /// Add a pair unless it is already there; its time is then kept.
    pub fn add_if_absent(&self, owner: &str, member: &str, source: &str) {
        let mut owners = self.owners.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        owners
            .entry(owner.to_string())
            .or_default()
            .entry((member.to_string(), source.to_string()))
            .or_insert_with(Instant::now);
    }

    /// The pairs waiting on `owner`, sorted.
    #[must_use]
    pub fn of(&self, owner: &str) -> Vec<(String, String)> {
        let owners = self.owners.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        let mut pairs: Vec<(String, String)> = owners.get(owner).map(|set| set.keys().cloned().collect()).unwrap_or_default();
        pairs.sort_unstable();
        pairs
    }

    /// Drop the pairs of `owner` last added before `before`: their
    /// references no longer wait. Pairs added since are kept.
    pub fn forget_before(&self, owner: &str, before: Instant) {
        let mut owners = self.owners.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(set) = owners.get_mut(owner) {
            set.retain(|_, added| *added >= before);
            if set.is_empty() {
                owners.remove(owner);
            }
        }
    }

    /// Drop every pair of `member` (a purge of that member).
    pub fn forget_member(&self, member: &str) {
        let mut owners = self.owners.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        owners.retain(|_, set| {
            set.retain(|(m, _), _| m != member);
            !set.is_empty()
        });
    }
}

/// The applier. Cheap to clone.
#[derive(Clone)]
pub struct Applier {
    pool: PgPool,
    scope: ScopeState,
    signal: Arc<Notify>,
    waiting: Waiting,
    /// The sync lanes' fairness account. It lives here, beside the
    /// waiting sets, so that a purge can clear a member from both.
    lane_time: LaneTime,
    sync_min_interval_secs: u32,
    /// Lists whose release is queued or running.
    releasing: Arc<Mutex<HashSet<String>>>,
    /// Lists handed to a waiter task.
    handed_over: Arc<Mutex<HashSet<String>>>,
}

impl std::fmt::Debug for Applier {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Applier").finish_non_exhaustive()
    }
}

impl Applier {
    #[must_use]
    pub fn new(pool: PgPool, scope: ScopeState, sync_min_interval_secs: u32) -> Self {
        Self {
            pool,
            scope,
            signal: Arc::new(Notify::new()),
            waiting: Waiting::default(),
            lane_time: LaneTime::default(),
            sync_min_interval_secs,
            releasing: Arc::default(),
            handed_over: Arc::default(),
        }
    }

    /// The waiting sets, shared with the sync claim.
    #[must_use]
    pub fn waiting(&self) -> &Waiting {
        &self.waiting
    }

    /// The sync lanes' fairness account, shared with the sync claim.
    #[must_use]
    pub fn lane_time(&self) -> &LaneTime {
        &self.lane_time
    }

    /// Forget everything held in memory about `member` (a purge of that
    /// member): what waited on a sync for it, and its share of the lanes.
    pub fn forget_member(&self, member: &str) {
        self.waiting.forget_member(member);
        self.lane_time.forget_member(member);
    }

    /// Tell the applier that a member phase committed markers.
    pub fn signal(&self) {
        self.signal.notify_one();
    }

    /// What follows a committed list-phase transaction for `list`.
    async fn settled(&self, list: &str, state: Option<ListState>, waiting: Vec<(String, String)>) -> Result<(), sqlx::Error> {
        self.scope.refresh_subscribed(&self.pool, std::slice::from_ref(&list.to_string())).await?;
        if state == Some(ListState::Unreferenced) {
            self.start_release(list);
        }
        if !waiting.is_empty() {
            // Under the read guard, so a purge that just cleared a member's
            // pairs cannot have them added back.
            let sets = self.scope.read_guard().await;
            let owner = uri_author(list).unwrap_or_default();
            for (member, source) in waiting {
                if sets.purge_kind(&member).is_none() {
                    self.waiting.add(owner, &member, &source);
                }
            }
        }
        Ok(())
    }

    /// Release `list` in a task of its own, unless one is already queued or
    /// running for it.
    fn start_release(&self, list: &str) {
        let fresh = self.releasing.lock().unwrap_or_else(std::sync::PoisonError::into_inner).insert(list.to_string());
        if !fresh {
            return;
        }
        let (this, list) = (self.clone(), list.to_string());
        tokio::spawn(async move {
            this.release(&list).await;
        });
    }

    /// Release `list` and set the in-memory sets from what is left.
    async fn release(&self, list: &str) {
        let result = list_phase::release_with(&self.pool, list, &self.waiting).await;
        self.releasing.lock().unwrap_or_else(std::sync::PoisonError::into_inner).remove(list);
        match result {
            Ok(_) => {
                if let Err(error) = self.scope.refresh_subscribed(&self.pool, std::slice::from_ref(&list.to_string())).await {
                    tracing::warn!(event = "list_release_refresh_failed", %error, "could not refresh the subscribed sets after a release");
                }
            }
            // The full reconcile finds the row again.
            Err(error) => tracing::warn!(event = "list_release_failed", %error, "releasing a list failed; the reconcile retries it"),
        }
    }

    /// The daily R4 job's safety net (design-v4 §14.1 R4, §4.3 Release):
    /// release every `unreferenced` row, whatever its age, here and now,
    /// except one whose release is already queued or running. Returns how
    /// many lists it released.
    ///
    /// # Errors
    ///
    /// Database errors listing the rows; a failed release is logged, and
    /// the next reconcile or daily run tries it again.
    pub async fn release_unreferenced(&self) -> Result<u64, sqlx::Error> {
        let lists: Vec<String> = sqlx::query_scalar("SELECT list_uri FROM subscribed_lists WHERE state = 'unreferenced' ORDER BY list_uri")
            .fetch_all(&self.pool)
            .await?;
        let mut n = 0;
        for list in lists {
            let fresh = self.releasing.lock().unwrap_or_else(std::sync::PoisonError::into_inner).insert(list.clone());
            if fresh {
                self.release(&list).await;
                n += 1;
            }
        }
        Ok(n)
    }

    /// Run the list phase for each of `lists`. Returns the lists whose row
    /// was busy.
    ///
    /// # Errors
    ///
    /// Database errors.
    pub async fn apply_all(&self, lists: &[String]) -> Result<Vec<String>, sqlx::Error> {
        let mut busy = Vec::new();
        for list in lists {
            if self.handed_over.lock().unwrap_or_else(std::sync::PoisonError::into_inner).contains(list) {
                continue;
            }
            match list_phase::apply(&self.pool, list, self.sync_min_interval_secs).await? {
                Applied::Done { state, waiting } => self.settled(list, state, waiting).await?,
                Applied::Busy => busy.push(list.clone()),
            }
        }
        Ok(busy)
    }

    /// One pass over the lists with a pending marker, oldest first.
    ///
    /// # Errors
    ///
    /// Database errors.
    pub async fn run_once(&self) -> Result<Vec<String>, sqlx::Error> {
        let lists = list_phase::marked(&self.pool).await?;
        self.apply_all(&lists).await
    }

    /// The full reconcile: every list whose row and references disagree,
    /// marker or not, and every list left unreferenced.
    ///
    /// # Errors
    ///
    /// Database errors.
    pub async fn reconcile(&self) -> Result<Vec<String>, sqlx::Error> {
        let lists = list_phase::unreconciled(&self.pool).await?;
        self.apply_all(&lists).await
    }

    /// Hand a list that stayed busy to a task that waits for its row.
    fn hand_over(&self, list: &str) {
        let fresh = self.handed_over.lock().unwrap_or_else(std::sync::PoisonError::into_inner).insert(list.to_string());
        if !fresh {
            return;
        }
        let (this, list) = (self.clone(), list.to_string());
        tokio::spawn(async move {
            loop {
                match list_phase::apply_waiting(&this.pool, &list, this.sync_min_interval_secs).await {
                    Ok(Applied::Done { state, waiting }) => {
                        if let Err(error) = this.settled(&list, state, waiting).await {
                            tracing::warn!(event = "list_phase_settle_failed", %error, "list phase follow-up failed");
                        }
                        break;
                    }
                    // A concurrent insert won, or the wait timed out.
                    Ok(Applied::Busy) | Err(_) => tokio::time::sleep(RETRY).await,
                }
            }
            this.handed_over.lock().unwrap_or_else(std::sync::PoisonError::into_inner).remove(&list);
        });
    }

    /// Run until the task is aborted: a reconcile at startup, then a pass
    /// on every signal and every [`FALLBACK`], with busy lists retried
    /// every [`RETRY`] and handed over after [`HAND_OVER`].
    pub async fn run(self) {
        let mut busy_since: HashMap<String, Instant> = HashMap::new();
        let mut first = true;
        loop {
            let pass = if std::mem::take(&mut first) { self.reconcile().await } else { self.run_once().await };
            match pass {
                Ok(busy) => {
                    busy_since.retain(|list, _| busy.contains(list));
                    for list in busy {
                        let since = *busy_since.entry(list.clone()).or_insert_with(Instant::now);
                        if since.elapsed() >= HAND_OVER {
                            self.hand_over(&list);
                            busy_since.remove(&list);
                        }
                    }
                }
                Err(error) => tracing::warn!(event = "list_phase_failed", %error, "list phase pass failed; it runs again"),
            }
            let releasing = self.releasing.lock().unwrap_or_else(std::sync::PoisonError::into_inner).len();
            metrics::gauge!(METRIC_RELEASING).set(u32::try_from(releasing).unwrap_or(u32::MAX));
            let wait = if busy_since.is_empty() { FALLBACK } else { RETRY };
            tokio::select! {
                () = self.signal.notified() => {}
                () = tokio::time::sleep(wait) => {}
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::aggregations::ScopeSets;
    use crate::config::ScopePolicy;
    use crate::subscriptions::references::{Budget, mute_list, unmute_list};
    use crate::test_support::fresh_db;

    const ME: &str = "did:plc:mmmmmmmmmmmmmmmmmmmmmmmm";
    const OWNER: &str = "did:plc:aaaaaaaaaaaaaaaaaaaaaaaa";
    const BUDGET: Budget = Budget { max_references: 5, max_owners: 5 };

    fn list(n: u8) -> String {
        format!("at://{OWNER}/app.bsky.graph.list/3klist{n}")
    }

    async fn released(pool: &PgPool, list: &str) {
        for _ in 0..200 {
            let left: bool = sqlx::query_scalar("SELECT EXISTS (SELECT 1 FROM subscribed_lists WHERE list_uri = $1)")
                .bind(list)
                .fetch_one(pool)
                .await
                .expect("row");
            if !left {
                return;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        panic!("the release of {list} did not finish");
    }

    /// The applier keeps the in-memory sets and the waiting pairs in step
    /// with the rows, releases an unreferenced list, and a reconcile
    /// repairs what a lost marker left behind.
    #[tokio::test]
    async fn a_pass_sets_memory_from_the_rows() {
        let Some(db) = fresh_db("subscriptions_applier").await else {
            return;
        };
        let p = &db.pool;
        let scope = ScopeState::new(ScopeSets::default(), ScopePolicy::MembersAndOneHop, 10_000);
        let applier = Applier::new(p.clone(), scope.clone(), 600);
        let (a, b) = (list(1), list(2));

        mute_list(p, ME, &a, BUDGET).await.expect("db");
        assert!(applier.run_once().await.expect("db").is_empty());
        let sets = scope.snapshot().await;
        assert!(sets.subscribed.is_active(&a) && sets.subscribed.tracks(OWNER) && !sets.subscribed.is_active(&b));
        assert_eq!(applier.waiting().of(OWNER), [(ME.to_string(), "mute".to_string())]);

        // A marker lost before the applier ran: the reconcile finds the
        // admitted reference with no row.
        mute_list(p, ME, &b, BUDGET).await.expect("db");
        sqlx::query("DELETE FROM list_reference_changes").execute(p).await.expect("lose the marker");
        assert!(applier.run_once().await.expect("db").is_empty());
        assert!(!scope.snapshot().await.subscribed.is_active(&b));
        applier.reconcile().await.expect("db");
        assert!(scope.snapshot().await.subscribed.is_active(&b));

        // The last unmute: the list leaves `L_sub` at once and is released;
        // the owner stays tracked while its other list is subscribed.
        unmute_list(p, ME, &a, BUDGET).await.expect("db");
        applier.run_once().await.expect("db");
        assert!(!scope.snapshot().await.subscribed.is_active(&a));
        released(p, &a).await;
        assert!(scope.snapshot().await.subscribed.tracks(OWNER));
        unmute_list(p, ME, &b, BUDGET).await.expect("db");
        applier.run_once().await.expect("db");
        released(p, &b).await;
        // The release task sets memory after its commit.
        for _ in 0..200 {
            if !scope.snapshot().await.subscribed.tracks(OWNER) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        assert!(!scope.snapshot().await.subscribed.tracks(OWNER));

        // The waiting pairs: forgotten by time, and by member.
        applier.waiting().forget_before(OWNER, Instant::now());
        assert!(applier.waiting().of(OWNER).is_empty());
        applier.waiting().add(OWNER, ME, "block");
        applier.waiting().forget_member(ME);
        assert!(applier.waiting().of(OWNER).is_empty());
        db.drop_db().await;
    }
}
