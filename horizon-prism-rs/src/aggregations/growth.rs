//! Scope growth (design-v2 §9.4).
//!
//! Growth applies on every admitted write path, public tier only. When an
//! admitted record is a member's follow (policies with follows) or a member's
//! like, repost, reply or quote (policies with interactions), its subject DIDs
//! join S:
//! 1. [`plan`] names the subjects before the transaction, so the writer can
//!    take each subject's **shared DID lock**, sorted with its author locks
//!    (§6.11.7 step 2). Retention takes the exclusive lock on a DID before
//!    deleting its rows, so a subject is never half-deleted while it re-enters.
//! 2. [`apply`] upserts `scope_dids` inside the transaction and enqueues a
//!    `scope` backfill for each new DID.
//! 3. After commit, [`commit_in_memory`] adds the new DIDs to S under a briefly
//!    held write guard.

use sqlx::PgConnection;

use super::admit::{AdmissionInput, RecordKind};
use super::policy::{REASON_FOLLOWED, REASON_INTERACTED, grows_on_follow, grows_on_interaction};
use super::scope_state::{ScopeSets, ScopeState};
use crate::config::ScopePolicy;

/// Subjects that an admitted record adds to S, with their reason bit.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct GrowthPlan {
    pub subjects: Vec<(String, i16)>,
}

impl GrowthPlan {
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.subjects.is_empty()
    }

    /// The DIDs whose shared locks the writer must hold.
    #[must_use]
    pub fn lock_dids(&self) -> Vec<String> {
        self.subjects.iter().map(|(d, _)| d.clone()).collect()
    }
}

/// Plan growth for an op about to be admitted. Only members' public records
/// grow scope; subjects already in S are skipped (the in-memory set cannot lag
/// the table while a writer holds the read guard, because removals happen
/// under the write guard).
#[must_use]
pub fn plan(
    input: &AdmissionInput,
    sets: &ScopeSets,
    policy: ScopePolicy,
    max_follow_scope_per_member: u32,
) -> GrowthPlan {
    if input.kind.is_private() || !sets.is_member(&input.author) {
        return GrowthPlan::default();
    }
    let reason = match input.kind {
        RecordKind::Follow if grows_on_follow(policy) => {
            let follows = sets.member_follows.get(&input.author).copied().unwrap_or(0);
            if follows >= max_follow_scope_per_member {
                return GrowthPlan::default();
            }
            REASON_FOLLOWED
        }
        RecordKind::Like | RecordKind::Repost | RecordKind::Post if grows_on_interaction(policy) => {
            REASON_INTERACTED
        }
        _ => return GrowthPlan::default(),
    };
    let mut subjects: Vec<(String, i16)> = Vec::new();
    for s in &input.subjects {
        if !sets.in_scope(s) && !subjects.iter().any(|(d, _)| d == s) {
            subjects.push((s.clone(), reason));
        }
    }
    GrowthPlan { subjects }
}

/// Write the planned growth inside the caller's transaction, which already
/// holds each subject's shared DID lock. Returns the DIDs newly added.
///
/// # Errors
///
/// Any database error.
pub async fn apply(conn: &mut PgConnection, plan: &GrowthPlan) -> Result<Vec<String>, sqlx::Error> {
    let mut added = Vec::new();
    for (did, reason) in &plan.subjects {
        let inserted: bool = sqlx::query_scalar(
            "INSERT INTO scope_dids (did, reasons) VALUES ($1, $2) \
             ON CONFLICT (did) DO UPDATE SET reasons = scope_dids.reasons | EXCLUDED.reasons \
             RETURNING (xmax = 0)",
        )
        .bind(did)
        .bind(reason)
        .fetch_one(&mut *conn)
        .await?;
        if inserted {
            crate::firehose::jobs::enqueue(&mut *conn, did, crate::firehose::jobs::Depth::Scope).await?;
            added.push(did.clone());
        }
    }
    Ok(added)
}

/// Add committed growth to the in-memory S, and count a member's new follow
/// toward the follow cap. Call after commit, with no database locks held.
pub async fn commit_in_memory(state: &ScopeState, added: &[String], new_member_follow: Option<&str>) {
    if added.is_empty() && new_member_follow.is_none() {
        return;
    }
    let mut sets = state.write_guard().await;
    for did in added {
        sets.scope.insert(did.clone());
    }
    if let Some(member) = new_member_follow {
        *sets.member_follows.entry(member.to_string()).or_insert(0) += 1;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const MEMBER: &str = "did:plc:member";

    fn sets() -> ScopeSets {
        let mut s = ScopeSets::default();
        s.members.insert(MEMBER.into());
        s.scope.insert(MEMBER.into());
        s.scope.insert("did:plc:already".into());
        s
    }

    fn input(author: &str, kind: RecordKind, subjects: &[&str]) -> AdmissionInput {
        AdmissionInput {
            author: author.into(),
            kind,
            subjects: subjects.iter().map(|s| (*s).to_string()).collect(),
        }
    }

    #[test]
    fn member_follow_grows_under_follow_policies() {
        let p = plan(&input(MEMBER, RecordKind::Follow, &["did:plc:new"]), &sets(), ScopePolicy::MembersAndOneHop, 10);
        assert_eq!(p.subjects, [("did:plc:new".to_string(), REASON_FOLLOWED)]);
        let none = plan(&input(MEMBER, RecordKind::Follow, &["did:plc:new"]), &sets(), ScopePolicy::MembersAndInteractions, 10);
        assert!(none.is_empty());
    }

    #[test]
    fn member_interactions_grow_under_interaction_policies() {
        for kind in [RecordKind::Like, RecordKind::Repost, RecordKind::Post] {
            let p = plan(&input(MEMBER, kind, &["did:plc:a", "did:plc:b", "did:plc:a"]), &sets(), ScopePolicy::MembersAndOneHop, 10);
            assert_eq!(p.lock_dids(), ["did:plc:a", "did:plc:b"], "{kind:?}");
            assert!(p.subjects.iter().all(|(_, r)| *r == REASON_INTERACTED));
        }
        assert!(plan(&input(MEMBER, RecordKind::Like, &["did:plc:a"]), &sets(), ScopePolicy::MembersAndFollows, 10).is_empty());
    }

    #[test]
    fn no_growth_for_non_members_private_records_blocks_or_known_subjects() {
        let s = sets();
        let p = ScopePolicy::MembersAndOneHop;
        assert!(plan(&input("did:plc:x", RecordKind::Follow, &["did:plc:new"]), &s, p, 10).is_empty());
        assert!(plan(&input(MEMBER, RecordKind::PrivatePost, &["did:plc:new"]), &s, p, 10).is_empty());
        assert!(plan(&input(MEMBER, RecordKind::Block, &["did:plc:new"]), &s, p, 10).is_empty());
        assert!(plan(&input(MEMBER, RecordKind::Follow, &["did:plc:already"]), &s, p, 10).is_empty());
    }

    #[test]
    fn follow_cap_stops_follow_growth() {
        let mut s = sets();
        s.member_follows.insert(MEMBER.into(), 10);
        assert!(plan(&input(MEMBER, RecordKind::Follow, &["did:plc:new"]), &s, ScopePolicy::MembersAndOneHop, 10).is_empty());
        // Interactions are not capped.
        assert!(!plan(&input(MEMBER, RecordKind::Like, &["did:plc:new"]), &s, ScopePolicy::MembersAndOneHop, 10).is_empty());
    }
}
