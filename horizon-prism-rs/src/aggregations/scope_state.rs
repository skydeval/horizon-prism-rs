//! `ScopeState` (design-v2 §9.3): the in-memory members, scope set S,
//! private-owner set and purging set, behind one tokio `RwLock`.
//!
//! Lock discipline (§6.11.7): ordinary writers hold a read guard for their
//! whole transaction, taken before a pool connection. Purge markers and member
//! add/remove hold the write guard. In-memory changes are applied only after
//! the transaction that wrote them commits, under a briefly held write guard
//! with no database locks.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::sync::atomic::{AtomicU8, Ordering};

use async_trait::async_trait;
use sqlx::PgPool;
use tokio::sync::{OwnedRwLockReadGuard, OwnedRwLockWriteGuard, RwLock};

use crate::config::{ScopePolicy, ScopeWindows};
use crate::db::locks::uri_author;
use crate::db::{ScopeGuard, ScopeReadGuard};
use crate::identity::PersistencePolicy;
use crate::indexing::{Breadth, PublicMode, PublicState};
use crate::private_tier::PrivateTier;

/// A purge kind (§6.11.7).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum PurgeKind {
    AccountDeleted,
    OutOfScope,
    MemberRemoved,
}

impl PurgeKind {
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::AccountDeleted => "account_deleted",
            Self::OutOfScope => "out_of_scope",
            Self::MemberRemoved => "member_removed",
        }
    }

    #[must_use]
    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "account_deleted" => Some(Self::AccountDeleted),
            "out_of_scope" => Some(Self::OutOfScope),
            "member_removed" => Some(Self::MemberRemoved),
            _ => None,
        }
    }
}

/// The four sets, plus the per-member follow counts behind the follow cap.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ScopeSets {
    pub members: HashSet<String>,
    /// The scope set S (members are always in S).
    pub scope: HashSet<String>,
    pub private_owners: HashSet<String>,
    pub purging: HashMap<String, PurgeKind>,
    /// Follow rows authored by each member (§9.4 follow cap).
    pub member_follows: HashMap<String, u32>,
    /// Rule 4's in-memory sets (design-v3 §4.3 "The copy rule").
    pub subscribed: Subscribed,
    /// The public-indexing mode, its generation and the start of the last
    /// fill (public-indexing design §3.5). Set only under the write guard,
    /// through [`ScopeState::set_public`], which keeps the mirror in step.
    pub public: PublicState,
    /// Refract's R-live (refract design, Terms): restored DIDs not pending
    /// removal, to their home community's name; empty while the feature is
    /// off. Behind an `Arc` so a request snapshot copies it cheaply.
    pub restored: Arc<HashMap<String, String>>,
    /// Refract candidates (state `candidate`), to their home community.
    pub candidates: HashMap<String, String>,
    /// What the gate hides for Refract (refract design §4.2), swapped on
    /// change.
    pub refract_hidden: Arc<crate::refract::RefractHidden>,
    /// What full indexing admits: the enabled groups, and the accounts
    /// excluded after their Refract restoration ended.
    pub full: FullSets,
    /// A full trial riding the transition, as stored. Set only under the
    /// write guard, through [`ScopeState::set_trial`].
    pub trial: Option<crate::indexing::trial::Trial>,
}

/// Full indexing's admission inputs.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct FullSets {
    /// `[full] kinds`, set at startup.
    pub kinds: Vec<crate::config::FullGroup>,
    /// Accounts whose Refract restoration ended: shared by every snapshot,
    /// changed without the scope lock after the write that changes the
    /// table commits.
    pub excluded: Arc<ExcludedSet>,
    /// The takedown relays whose non-active statuses hide an account and
    /// keep its records out, under `suspended_accounts = "refract_only"`;
    /// empty under `"all"`.
    pub takedown_relays: Vec<String>,
    /// Every takedown relay, whatever `suspended_accounts` says: their
    /// statuses are recorded.
    pub status_relays: Vec<String>,
    /// Accounts whose deletion purge runs without the scope write guard:
    /// writers re-read it after taking their DID locks.
    pub purge_markers: Arc<ExcludedSet>,
    /// Accounts members read whose history the crawl has not marked: moved
    /// up the crawl queue (`[full] on_demand = "members"`).
    pub on_demand: Option<Arc<crate::indexing::crawl::OnDemand>>,
    /// A history cutoff is set (`[full_crawl] skip_older_than_days` or
    /// `[retention] max_age_days`): crawled accounts outside S and the
    /// members, and interaction data, keep reading today's sources, so an
    /// aged-out record never reads as missing.
    pub history_cutoff: bool,
    /// Every account's blocks and list blocks are stored, lifting Rule M
    /// (`[full] blocks_source = "local"` with the `blocks` and `lists`
    /// groups on).
    pub blocks_local: bool,
    /// Counts read locally once their group's history is complete
    /// (`[full] counts = "auto"`).
    pub counts_auto: bool,
    /// Third-party labels merged into views in breadth Full.
    pub labels: Option<Arc<crate::indexing::labels::ExternalLabels>>,
}

/// Whether full indexing stores every account's blocks and list blocks:
/// `blocks_source = "local"` with both the `blocks` and `lists` groups on.
/// Otherwise blocks keep a member side.
#[must_use]
pub fn blocks_lifted(full: &crate::config::FullConfig) -> bool {
    use crate::config::FullGroup;
    full.blocks_source == crate::config::BlocksSource::Local
        && full.kinds.contains(&FullGroup::Blocks)
        && full.kinds.contains(&FullGroup::Lists)
}

/// A small concurrent set of DIDs (bounded by the Refract communities).
#[derive(Default)]
pub struct ExcludedSet(std::sync::RwLock<HashSet<String>>);

impl ExcludedSet {
    fn read(&self) -> std::sync::RwLockReadGuard<'_, HashSet<String>> {
        self.0.read().unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    fn write(&self) -> std::sync::RwLockWriteGuard<'_, HashSet<String>> {
        self.0.write().unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    #[must_use]
    pub fn contains(&self, did: &str) -> bool {
        self.read().contains(did)
    }

    pub fn insert(&self, did: &str) {
        self.write().insert(did.to_string());
    }

    pub fn remove(&self, did: &str) {
        self.write().remove(did);
    }

    /// Replace the whole set.
    pub fn replace(&self, dids: HashSet<String>) {
        *self.write() = dids;
    }

    #[must_use]
    pub fn len(&self) -> usize {
        self.read().len()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.read().is_empty()
    }

    /// Bytes the set holds, roughly, for Health.
    #[must_use]
    pub fn bytes(&self) -> usize {
        self.read().iter().map(|d| d.len() + 48).sum()
    }
}

impl std::fmt::Debug for ExcludedSet {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_set().entries(self.read().iter()).finish()
    }
}

impl PartialEq for ExcludedSet {
    fn eq(&self, other: &Self) -> bool {
        std::ptr::eq(self, other) || *self.read() == *other.read()
    }
}

impl Eq for ExcludedSet {}

/// What the copy rule looks up for every list-item write: which lists are
/// subscribed, whose changes are tracked, and how full each is. Set only
/// from the `subscribed_lists` rows themselves, after the commit that
/// changed them.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Subscribed {
    /// `L_sub`: each `active` list, with its copied-item count.
    pub lists: HashMap<String, u32>,
    /// `O_sub`: each owner with at least one `subscribed_lists` row
    /// (active or being released), with its tombstone count.
    pub owners: HashMap<String, u32>,
}

impl Subscribed {
    #[must_use]
    pub fn is_active(&self, list: &str) -> bool {
        self.lists.contains_key(list)
    }

    #[must_use]
    pub fn tracks(&self, owner: &str) -> bool {
        self.owners.contains_key(owner)
    }
}

impl ScopeSets {
    /// The own-repo rule: whether `did`'s own public records are read from
    /// the local copy. In community, the mode is On and `did` is in S. In
    /// full the same for S and the members; any other account would need a
    /// current crawl mark of the record's group, which only the crawl
    /// writes, so it reads today's sources. While narrowing, never.
    #[must_use]
    pub fn own_repo_local(&self, did: &str) -> bool {
        self.public.mode.is_on() && (self.public.breadth != Breadth::Narrowing || self.trial_exit()) && self.in_scope(did)
    }

    /// A trial's narrowing: it never changes S's rows, so S reads local as
    /// in community.
    #[must_use]
    pub fn trial_exit(&self) -> bool {
        self.trial.as_ref().is_some_and(|t| t.state == crate::indexing::trial::TrialState::Ending)
    }

    /// The start mode's admission view while a trial records: in breadth
    /// Full, with the trial running.
    #[must_use]
    pub fn recording(&self) -> Option<crate::indexing::trial::Recording> {
        let t = self.trial.as_ref().filter(|t| t.state == crate::indexing::trial::TrialState::Running)?;
        if self.public.breadth != Breadth::Full {
            return None;
        }
        let (mode, breadth) = t.start_view();
        Some(crate::indexing::trial::Recording { mode, breadth, from: t.from })
    }

    /// The own-repo rule for a stored handle: local for S and the members
    /// in every mode, as before full, and never while narrowing. Any other
    /// account would need a crawl mark, so it reads today's source.
    #[must_use]
    pub fn own_handle_local(&self, did: &str) -> bool {
        (self.public.breadth != Breadth::Narrowing || self.trial_exit()) && (self.in_scope(did) || self.is_member(did))
    }

    #[must_use]
    pub fn is_member(&self, did: &str) -> bool {
        self.members.contains(did)
    }

    #[must_use]
    pub fn in_scope(&self, did: &str) -> bool {
        self.scope.contains(did)
    }

    #[must_use]
    pub fn is_owner(&self, did: &str) -> bool {
        self.private_owners.contains(did)
    }

    #[must_use]
    pub fn purge_kind(&self, did: &str) -> Option<PurgeKind> {
        self.purging.get(did).copied()
    }

    /// Tracked DIDs = members ∪ private owners (§6.11.1).
    #[must_use]
    pub fn is_tracked(&self, did: &str) -> bool {
        self.is_member(did) || self.is_owner(did)
    }

    /// Whether `did` is in Refract's R-live.
    #[must_use]
    pub fn is_restored(&self, did: &str) -> bool {
        self.restored.contains_key(did)
    }

    /// Every tracked DID.
    #[must_use]
    pub fn tracked(&self) -> Vec<String> {
        let mut all: HashSet<&String> = self.members.iter().collect();
        all.extend(self.private_owners.iter());
        let mut out: Vec<String> = all.into_iter().cloned().collect();
        out.sort();
        out
    }
}

/// The shared scope state.
#[derive(Clone)]
pub struct ScopeState {
    sets: Arc<RwLock<ScopeSets>>,
    policy: ScopePolicy,
    max_follow_scope_per_member: u32,
    /// How long an interaction holds its subject (scope-recency design §5).
    windows: ScopeWindows,
    /// The DIDs a member's write named as a hold since the current sweep
    /// began (scope-recency design §3.2). Locked only for an insert, a
    /// lookup or a clear, never across an `.await`; `reload` leaves it.
    touched: Arc<std::sync::Mutex<HashSet<String>>>,
    /// `ScopeSets.public.mode`, readable without the guard (the endpoint
    /// gate, public-indexing design §3.5). Stored under the write guard.
    public_mirror: Arc<AtomicU8>,
    /// `ScopeSets.public.breadth`, likewise.
    breadth_mirror: Arc<AtomicU8>,
    /// Whether a trial's exit runs, likewise.
    trial_exit_mirror: Arc<std::sync::atomic::AtomicBool>,
    /// A full trial's counters, which writers add to.
    trial_counters: Arc<crate::indexing::trial::TrialCounters>,
}

impl ScopeState {
    #[must_use]
    pub fn new(sets: ScopeSets, policy: ScopePolicy, max_follow_scope_per_member: u32) -> Self {
        let mirror = AtomicU8::new(sets.public.mode.as_u8());
        let breadth = AtomicU8::new(sets.public.breadth.as_u8());
        Self {
            sets: Arc::new(RwLock::new(sets)),
            policy,
            max_follow_scope_per_member,
            windows: ScopeWindows::default(),
            touched: Arc::default(),
            public_mirror: Arc::new(mirror),
            breadth_mirror: Arc::new(breadth),
            trial_exit_mirror: Arc::default(),
            trial_counters: Arc::default(),
        }
    }

    fn touched_set(&self) -> std::sync::MutexGuard<'_, HashSet<String>> {
        self.touched.lock().unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// Record holds a member's write named. Writers call it under the read
    /// guard, before their commit (scope-recency design §3.2).
    pub fn touch(&self, dids: &[String]) {
        if dids.is_empty() {
            return;
        }
        self.touched_set().extend(dids.iter().cloned());
    }

    /// Whether a member's write named `did` as a hold since the sweep began.
    #[must_use]
    pub fn is_touched(&self, did: &str) -> bool {
        self.touched_set().contains(did)
    }

    /// Start a sweep's record. The sweep calls it under the write guard.
    pub fn clear_touched(&self) {
        self.touched_set().clear();
    }

    /// Set the interaction windows (`scope.reply_quote_days`,
    /// `scope.like_repost_days`); the defaults otherwise.
    #[must_use]
    pub fn with_windows(mut self, windows: ScopeWindows) -> Self {
        self.windows = windows;
        self
    }

    #[must_use]
    pub fn windows(&self) -> ScopeWindows {
        self.windows
    }

    /// Set the public-indexing state and its mirror. `sets` is the write
    /// guard the caller holds.
    pub fn set_public(&self, sets: &mut ScopeSets, public: PublicState) {
        sets.public = public;
        self.public_mirror.store(public.mode.as_u8(), Ordering::Release);
        self.breadth_mirror.store(public.breadth.as_u8(), Ordering::Release);
    }

    /// Set the trial and its mirror. `sets` is the write guard the caller
    /// holds.
    pub fn set_trial(&self, sets: &mut ScopeSets, trial: Option<crate::indexing::trial::Trial>) {
        sets.trial = trial;
        self.trial_exit_mirror.store(sets.trial_exit(), Ordering::Release);
    }

    /// A full trial's counters.
    #[must_use]
    pub fn trial_counters(&self) -> Arc<crate::indexing::trial::TrialCounters> {
        Arc::clone(&self.trial_counters)
    }

    /// The public-indexing mode, read from the mirror without the guard.
    #[must_use]
    pub fn public_mode(&self) -> PublicMode {
        PublicMode::from_u8(self.public_mirror.load(Ordering::Acquire))
    }

    /// Set full indexing's groups and read the accounts excluded after
    /// their Refract restoration ended.
    ///
    /// # Errors
    ///
    /// Any database error.
    /// The crawl's inputs on the read path: the on-demand queue (none when
    /// `[full] on_demand = "off"`) and whether a history cutoff is set.
    /// Third-party labels for views in breadth Full.
    pub async fn set_full_labels(&self, labels: Arc<crate::indexing::labels::ExternalLabels>) {
        self.write_guard().await.full.labels = Some(labels);
    }

    pub async fn set_crawl_reads(&self, on_demand: Option<Arc<crate::indexing::crawl::OnDemand>>, history_cutoff: bool) {
        let mut sets = self.write_guard().await;
        sets.full.on_demand = on_demand;
        sets.full.history_cutoff = history_cutoff;
    }

    pub async fn load_full(&self, pool: &PgPool, full: &crate::config::FullConfig, relay_labels: &[String]) -> Result<(), sqlx::Error> {
        let excluded: Vec<String> = sqlx::query_scalar("SELECT d.did FROM refract_excluded x JOIN dids d ON d.id = x.account")
            .fetch_all(pool)
            .await?;
        let mut relays = Vec::new();
        for r in &full.takedown_relays {
            if relay_labels.contains(r) {
                relays.push(r.clone());
            } else {
                tracing::warn!(event = "full_takedown_relay_set_aside", relay = %r, "full.takedown_relays: not a label in firehose.relay_labels; set aside");
            }
        }
        if relays.is_empty() {
            tracing::warn!(event = "full_no_takedown_relay", "full.takedown_relays names no configured relay: suspended_accounts has no effect");
        }
        let blocks_local = blocks_lifted(full);
        if full.blocks_source == crate::config::BlocksSource::Local && !blocks_local {
            tracing::warn!(
                event = "full_blocks_source_set_aside",
                "full.blocks_source = \"local\" needs the blocks and lists groups in full.kinds; set aside: blocks keep a member side, and inbound list blocks are read from Farsight when it is configured"
            );
        }
        let mut sets = self.write_guard().await;
        sets.full.kinds = full.kinds.clone();
        sets.full.blocks_local = blocks_local;
        sets.full.counts_auto = full.counts == crate::config::FullCounts::Auto;
        sets.full.excluded.replace(excluded.into_iter().collect());
        sets.full.takedown_relays = if full.suspended_accounts == crate::config::SuspendedAccounts::RefractOnly { relays.clone() } else { Vec::new() };
        sets.full.status_relays = relays;
        Ok(())
    }

    /// Whether S's public data is read locally at all: the mode is On and
    /// the breadth is not Narrowing (the own-repo rule's mode test), read
    /// from the mirrors without the guard.
    #[must_use]
    pub fn public_reads_local(&self) -> bool {
        self.public_mode().is_on() && (self.public_breadth() != Breadth::Narrowing || self.trial_exit_mirror.load(Ordering::Acquire))
    }

    /// The public-indexing breadth, read from the mirror without the guard.
    #[must_use]
    pub fn public_breadth(&self) -> Breadth {
        Breadth::from_u8(self.breadth_mirror.load(Ordering::Acquire))
    }

    /// The takedown relays whose non-active statuses hide an account in an
    /// SQL read ([`crate::moderation::hidden_status_sql`]): empty outside
    /// full and under `suspended_accounts = "all"`.
    pub async fn hidden_relays(&self) -> Vec<String> {
        if self.public_breadth() != Breadth::Full {
            return Vec::new();
        }
        let sets = self.read_guard().await;
        if sets.public.breadth == Breadth::Full { sets.full.takedown_relays.clone() } else { Vec::new() }
    }

    /// Load the sets from the database at startup (§9.3).
    ///
    /// # Errors
    ///
    /// Any database error.
    pub async fn load(
        pool: &PgPool,
        private: &dyn PrivateTier,
        policy: ScopePolicy,
        max_follow_scope_per_member: u32,
    ) -> Result<Self, sqlx::Error> {
        let sets = load_sets(pool, private).await?;
        Ok(Self::new(sets, policy, max_follow_scope_per_member))
    }

    #[must_use]
    pub fn policy(&self) -> ScopePolicy {
        self.policy
    }

    #[must_use]
    pub fn max_follow_scope_per_member(&self) -> u32 {
        self.max_follow_scope_per_member
    }

    /// The read guard ordinary writers hold for a whole transaction.
    pub async fn read_guard(&self) -> OwnedRwLockReadGuard<ScopeSets> {
        Arc::clone(&self.sets).read_owned().await
    }

    /// The write guard for purge markers, member add/remove and post-commit
    /// in-memory updates.
    pub async fn write_guard(&self) -> OwnedRwLockWriteGuard<ScopeSets> {
        Arc::clone(&self.sets).write_owned().await
    }

    /// A clone of the current sets (diagnostics and tests).
    pub async fn snapshot(&self) -> ScopeSets {
        self.sets.read().await.clone()
    }

    /// Set `L_sub` and `O_sub` for `lists` and their owners from the rows as
    /// they are now (design-v3 §4.3): called after a commit that changed a
    /// `subscribed_lists` row. The rows are re-read under the write guard,
    /// not taken from the caller's transaction, so commits that finish in
    /// another order still end at the same sets.
    ///
    /// # Errors
    ///
    /// Any database error; the sets are unchanged on error.
    pub async fn refresh_subscribed(&self, pool: &PgPool, lists: &[String]) -> Result<(), sqlx::Error> {
        if lists.is_empty() {
            return Ok(());
        }
        let owners: Vec<String> = lists.iter().filter_map(|l| uri_author(l).map(str::to_string)).collect();
        let mut sets = self.write_guard().await;
        let (active, tracked) = read_subscribed(pool, Some((lists, &owners))).await?;
        for list in lists {
            sets.subscribed.lists.remove(list);
        }
        sets.subscribed.lists.extend(active);
        for owner in &owners {
            if !tracked.contains_key(owner) {
                sets.subscribed.owners.remove(owner);
            }
        }
        // An owner already tracked keeps its in-memory tombstone count; one
        // that enters gets its count from the table.
        for (owner, tombstones) in tracked {
            sets.subscribed.owners.entry(owner).or_insert(tombstones);
        }
        Ok(())
    }

    /// Read every tracked owner's tombstone count again, after rows were
    /// deleted by age (design-v3 §4.3 "Per-owner tombstone cap"). The
    /// table is read under the write guard, so no writer's own count is
    /// lost between the read and the set.
    ///
    /// # Errors
    ///
    /// Any database error; the counts are unchanged on error.
    pub async fn reload_tombstone_counts(&self, pool: &PgPool) -> Result<(), sqlx::Error> {
        let mut sets = self.write_guard().await;
        let (_, tracked) = read_subscribed(pool, None).await?;
        for (owner, count) in &mut sets.subscribed.owners {
            if let Some(fresh) = tracked.get(owner) {
                *count = *fresh;
            }
        }
        Ok(())
    }

    /// Read one tracked owner's tombstone count again, after a list sync
    /// wrote or evicted tombstones of that owner.
    ///
    /// # Errors
    ///
    /// Any database error; the count is unchanged on error.
    pub async fn reload_owner_tombstones(&self, pool: &PgPool, owner: &str) -> Result<(), sqlx::Error> {
        let mut sets = self.write_guard().await;
        let fresh: i64 = sqlx::query_scalar("SELECT count(*) FROM subscription_tombstones WHERE owner_did = $1")
            .bind(owner)
            .fetch_one(pool)
            .await?;
        if let Some(count) = sets.subscribed.owners.get_mut(owner) {
            *count = u32::try_from(fresh).unwrap_or(u32::MAX);
        }
        Ok(())
    }

    /// Replace every set with a fresh load (after a rebuild). The
    /// public-indexing state is not a set and is kept. Refract's sets are
    /// kept as they are under the write guard: every Refract change is
    /// applied in memory in commit order under that guard (refract design
    /// §4.2), so they already match the committed rows.
    ///
    /// # Errors
    ///
    /// Any database error; the sets are unchanged on error.
    pub async fn reload(&self, pool: &PgPool, private: &dyn PrivateTier) -> Result<(), sqlx::Error> {
        let mut fresh = load_sets(pool, private).await?;
        let mut sets = self.write_guard().await;
        fresh.public = sets.public;
        fresh.full = sets.full.clone();
        fresh.trial = sets.trial.clone();
        fresh.restored = Arc::clone(&sets.restored);
        fresh.candidates = std::mem::take(&mut sets.candidates);
        fresh.refract_hidden = Arc::clone(&sets.refract_hidden);
        *sets = fresh;
        Ok(())
    }
}

/// The active lists with their item counts, and the tracked owners with
/// their tombstone counts: all of them, or those among `only`'s lists and
/// owners.
async fn read_subscribed(
    pool: &PgPool,
    only: Option<(&[String], &[String])>,
) -> Result<(HashMap<String, u32>, HashMap<String, u32>), sqlx::Error> {
    let (lists, owners) = only.map_or((None, None), |(l, o)| (Some(l), Some(o)));
    let active: Vec<(String, i32)> = sqlx::query_as(
        "SELECT list_uri, item_count FROM subscribed_lists WHERE state = 'active' AND ($1::text[] IS NULL OR list_uri = ANY($1))",
    )
    .bind(lists)
    .fetch_all(pool)
    .await?;
    let tracked: Vec<(String, i64)> = sqlx::query_as(
        "SELECT o.owner_did, (SELECT count(*) FROM subscription_tombstones t WHERE t.owner_did = o.owner_did) \
         FROM (SELECT DISTINCT owner_did FROM subscribed_lists WHERE $1::text[] IS NULL OR owner_did = ANY($1)) o",
    )
    .bind(owners)
    .fetch_all(pool)
    .await?;
    Ok((
        active.into_iter().map(|(l, n)| (l, u32::try_from(n).unwrap_or(0))).collect(),
        tracked.into_iter().map(|(o, n)| (o, u32::try_from(n).unwrap_or(u32::MAX))).collect(),
    ))
}

async fn load_sets(pool: &PgPool, private: &dyn PrivateTier) -> Result<ScopeSets, sqlx::Error> {
    let members: Vec<String> = sqlx::query_scalar("SELECT did FROM members").fetch_all(pool).await?;
    let scope: Vec<String> = sqlx::query_scalar("SELECT did FROM scope_dids").fetch_all(pool).await?;
    let purging: Vec<(String, String)> = sqlx::query_as("SELECT did, kind FROM purging_dids")
        .fetch_all(pool)
        .await?;
    let follows: Vec<(String, i64)> = sqlx::query_as(
        "SELECT fa.did, count(*) FROM follows f JOIN dids fa ON fa.id = f.author \
         WHERE EXISTS (SELECT 1 FROM members m WHERE m.did = fa.did) GROUP BY fa.did",
    )
    .fetch_all(pool)
    .await?;
    let private_owners = private.load_owners(pool).await?;
    let (lists, owners) = read_subscribed(pool, None).await?;
    let mut sets = ScopeSets {
        subscribed: Subscribed { lists, owners },
        members: members.into_iter().collect(),
        scope: scope.into_iter().collect(),
        private_owners,
        purging: purging
            .into_iter()
            .filter_map(|(did, kind)| PurgeKind::parse(&kind).map(|k| (did, k)))
            .collect(),
        member_follows: follows
            .into_iter()
            .map(|(did, n)| (did, u32::try_from(n).unwrap_or(u32::MAX)))
            .collect(),
        public: PublicState::default(),
        // Refract's sets are loaded with its configuration, after its
        // startup clearing (refract design §4.2), by `refract::load`.
        restored: Arc::default(),
        candidates: HashMap::new(),
        refract_hidden: Arc::default(),
        full: FullSets::default(),
        trial: None,
    };
    // Members are always in S (reason bit 1), even before a rebuild wrote it.
    let members: Vec<String> = sets.members.iter().cloned().collect();
    sets.scope.extend(members);
    Ok(sets)
}

/// Retention's per-batch read guard (§6.11.7).
#[async_trait]
impl ScopeGuard for ScopeState {
    async fn read(&self) -> ScopeReadGuard {
        ScopeReadGuard::new(self.read_guard().await)
    }
}

/// The `did_cache` persistence set (§6.7): members ∪ S. Called synchronously
/// from identity code; if a write guard is held at that instant the answer is
/// "not persisted", which only means the document stays in the bounded
/// in-process cache. In full every document just resolved is persisted:
/// retention removes it once it expires, so the table holds S, the members,
/// R and the DIDs resolved recently.
impl PersistencePolicy for ScopeState {
    fn in_persistence_set(&self, did: &str) -> bool {
        if self.public_breadth() == Breadth::Full {
            return true;
        }
        // Refract's R is treated like S (refract design §5.4).
        self.sets
            .try_read()
            .map(|s| s.is_member(did) || s.in_scope(did) || s.is_restored(did))
            .unwrap_or(false)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn state() -> ScopeState {
        let mut sets = ScopeSets::default();
        sets.members.insert("did:plc:m".into());
        sets.scope.insert("did:plc:m".into());
        sets.scope.insert("did:plc:s".into());
        sets.private_owners.insert("did:plc:o".into());
        ScopeState::new(sets, ScopePolicy::MembersAndOneHop, 10_000)
    }

    #[test]
    fn purge_kinds_round_trip() {
        for k in [PurgeKind::AccountDeleted, PurgeKind::OutOfScope, PurgeKind::MemberRemoved] {
            assert_eq!(PurgeKind::parse(k.as_str()), Some(k));
        }
        assert_eq!(PurgeKind::parse("other"), None);
    }

    #[tokio::test]
    async fn tracked_is_members_and_owners() {
        let s = state().snapshot().await;
        assert!(s.is_tracked("did:plc:m"));
        assert!(s.is_tracked("did:plc:o"));
        assert!(!s.is_tracked("did:plc:s"));
        assert_eq!(s.tracked(), ["did:plc:m", "did:plc:o"]);
    }

    #[tokio::test]
    async fn persistence_set_is_members_and_scope() {
        let st = state();
        assert!(st.in_persistence_set("did:plc:m"));
        assert!(st.in_persistence_set("did:plc:s"));
        assert!(!st.in_persistence_set("did:plc:o"));
        assert!(!st.in_persistence_set("did:plc:x"));
    }

    #[tokio::test]
    async fn persistence_answers_no_while_a_writer_holds_the_guard() {
        let st = state();
        let w = st.write_guard().await;
        assert!(!st.in_persistence_set("did:plc:m"));
        drop(w);
        assert!(st.in_persistence_set("did:plc:m"));
    }

    /// T-1: `ScopeSets::default().public` is `(On, 0, None)`, and the
    /// mirror follows every set.
    #[tokio::test]
    async fn public_state_defaults_to_on_and_the_mirror_follows() {
        assert_eq!(ScopeSets::default().public, PublicState { mode: PublicMode::On, generation: 0, last_fill_since: None, ..PublicState::default() });
        let st = state();
        assert_eq!(st.public_mode(), PublicMode::On);
        let off = PublicState { mode: PublicMode::Off, generation: 3, last_fill_since: None, ..PublicState::default() };
        {
            let mut sets = st.write_guard().await;
            st.set_public(&mut sets, off);
        }
        assert_eq!(st.public_mode(), PublicMode::Off);
        assert_eq!(st.snapshot().await.public, off);
    }

    #[tokio::test]
    async fn scope_guard_excludes_writers() {
        let st = state();
        let g = ScopeGuard::read(&st).await;
        assert!(st.sets.try_write().is_err());
        drop(g);
        assert!(st.sets.try_write().is_ok());
    }
}
