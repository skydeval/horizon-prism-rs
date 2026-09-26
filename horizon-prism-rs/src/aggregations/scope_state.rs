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

use async_trait::async_trait;
use sqlx::PgPool;
use tokio::sync::{OwnedRwLockReadGuard, OwnedRwLockWriteGuard, RwLock};

use crate::config::ScopePolicy;
use crate::db::{ScopeGuard, ScopeReadGuard};
use crate::identity::PersistencePolicy;
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
}

impl ScopeSets {
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
}

impl ScopeState {
    #[must_use]
    pub fn new(sets: ScopeSets, policy: ScopePolicy, max_follow_scope_per_member: u32) -> Self {
        Self {
            sets: Arc::new(RwLock::new(sets)),
            policy,
            max_follow_scope_per_member,
        }
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

    /// Replace every set with a fresh load (after a rebuild).
    ///
    /// # Errors
    ///
    /// Any database error; the sets are unchanged on error.
    pub async fn reload(&self, pool: &PgPool, private: &dyn PrivateTier) -> Result<(), sqlx::Error> {
        let fresh = load_sets(pool, private).await?;
        *self.write_guard().await = fresh;
        Ok(())
    }
}

async fn load_sets(pool: &PgPool, private: &dyn PrivateTier) -> Result<ScopeSets, sqlx::Error> {
    let members: Vec<String> = sqlx::query_scalar("SELECT did FROM members").fetch_all(pool).await?;
    let scope: Vec<String> = sqlx::query_scalar("SELECT did FROM scope_dids").fetch_all(pool).await?;
    let purging: Vec<(String, String)> = sqlx::query_as("SELECT did, kind FROM purging_dids")
        .fetch_all(pool)
        .await?;
    let follows: Vec<(String, i64)> = sqlx::query_as(
        "SELECT f.author_did, count(*) FROM follows f \
         WHERE EXISTS (SELECT 1 FROM members m WHERE m.did = f.author_did) GROUP BY f.author_did",
    )
    .fetch_all(pool)
    .await?;
    let private_owners = private.load_owners(pool).await?;
    let mut sets = ScopeSets {
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
/// in-process cache.
impl PersistencePolicy for ScopeState {
    fn in_persistence_set(&self, did: &str) -> bool {
        self.sets
            .try_read()
            .map(|s| s.is_member(did) || s.in_scope(did))
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

    #[tokio::test]
    async fn scope_guard_excludes_writers() {
        let st = state();
        let g = ScopeGuard::read(&st).await;
        assert!(st.sets.try_write().is_err());
        drop(g);
        assert!(st.sets.try_write().is_ok());
    }
}
