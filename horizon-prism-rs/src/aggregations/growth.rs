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

use chrono::{DateTime, TimeDelta, Utc};
use sqlx::PgConnection;

use super::admit::{AdmissionInput, RecordKind};
use super::policy::{REASON_FOLLOWED, REASON_INTERACTED, grows_on_follow, grows_on_interaction};
use super::scope_state::{ScopeSets, ScopeState};
use crate::config::{ScopePolicy, ScopeWindows};

/// Subjects that an admitted record adds to S, with their reason bit.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct GrowthPlan {
    pub subjects: Vec<(String, i16)>,
    /// Every DID the record names as a hold, in S or not: what the writer
    /// records in the touched set (scope-recency design §3.2).
    pub holds: Vec<String>,
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

/// When the record was made and when it is written, for the interaction
/// windows (scope-recency design §3.1).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Recency {
    /// The record's `createdAt`, clamped (`records::created_at`).
    pub created_at: DateTime<Utc>,
    /// The write's own time.
    pub now: DateTime<Utc>,
    pub windows: ScopeWindows,
    /// The `kind` label a stale post is counted under: `reply` or `quote`.
    pub post_label: &'static str,
}

impl Recency {
    /// Whether an interaction of `kind` made at `created_at` is still
    /// fresh, and the label it is counted under when it is not.
    fn fresh(&self, kind: RecordKind) -> (bool, &'static str) {
        let (days, label) = match kind {
            RecordKind::Like => (self.windows.like_repost_days, "like"),
            RecordKind::Repost => (self.windows.like_repost_days, "repost"),
            _ => (self.windows.reply_quote_days, self.post_label),
        };
        let fresh = days > 0 && self.created_at >= self.now - TimeDelta::days(i64::from(days));
        (fresh, label)
    }
}

/// Plan growth for an op about to be admitted. Only members' public records
/// grow scope; subjects already in S are skipped (the in-memory set cannot lag
/// the table while a writer holds the read guard, because removals happen
/// under the write guard). A like or repost grows S only within the like
/// window of its `createdAt`, a reply or quote only within the reply window;
/// a stale one is stored as before and grows nothing (scope-recency design
/// §3.1). A subject that is not a DID never grows S.
#[must_use]
pub fn plan(
    input: &AdmissionInput,
    sets: &ScopeSets,
    policy: ScopePolicy,
    max_follow_scope_per_member: u32,
    recency: &Recency,
) -> GrowthPlan {
    if input.kind.is_private() || !sets.is_member(&input.author) {
        return GrowthPlan::default();
    }
    let (reason, grows) = match input.kind {
        // A follow past the cap grows nothing, but is still a hold the
        // sweep's marker must see (scope-recency design §3.2).
        RecordKind::Follow if grows_on_follow(policy) => {
            let follows = sets.member_follows.get(&input.author).copied().unwrap_or(0);
            (REASON_FOLLOWED, follows < max_follow_scope_per_member)
        }
        RecordKind::Like | RecordKind::Repost | RecordKind::Post if grows_on_interaction(policy) => {
            if input.subjects.is_empty() {
                return GrowthPlan::default();
            }
            let (fresh, label) = recency.fresh(input.kind);
            if !fresh {
                metrics::counter!(METRIC_GROWTH_STALE, "kind" => label).increment(1);
                return GrowthPlan::default();
            }
            (REASON_INTERACTED, true)
        }
        _ => return GrowthPlan::default(),
    };
    let mut plan = GrowthPlan::default();
    for s in &input.subjects {
        if !s.starts_with("did:") || plan.holds.contains(s) {
            continue;
        }
        plan.holds.push(s.clone());
        if grows && !sets.in_scope(s) {
            plan.subjects.push((s.clone(), reason));
        }
    }
    plan
}

/// Interactions too old to grow S, by kind (scope-recency design §7).
pub const METRIC_GROWTH_STALE: &str = "scope_growth_stale_total";

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
            public_post_subject: false,
        }
    }

    /// A record made `age` before it is written, under the default windows.
    fn aged(age: TimeDelta) -> Recency {
        let now = Utc::now();
        Recency { created_at: now - age, now, windows: ScopeWindows::default(), post_label: "reply" }
    }

    fn fresh() -> Recency {
        aged(TimeDelta::zero())
    }

    #[test]
    fn member_follow_grows_under_follow_policies() {
        let p = plan(&input(MEMBER, RecordKind::Follow, &["did:plc:new"]), &sets(), ScopePolicy::MembersAndOneHop, 10, &fresh());
        assert_eq!(p.subjects, [("did:plc:new".to_string(), REASON_FOLLOWED)]);
        let none = plan(&input(MEMBER, RecordKind::Follow, &["did:plc:new"]), &sets(), ScopePolicy::MembersAndInteractions, 10, &fresh());
        assert!(none.is_empty());
    }

    #[test]
    fn member_interactions_grow_under_interaction_policies() {
        for kind in [RecordKind::Like, RecordKind::Repost, RecordKind::Post] {
            let p = plan(&input(MEMBER, kind, &["did:plc:a", "did:plc:b", "did:plc:a"]), &sets(), ScopePolicy::MembersAndOneHop, 10, &fresh());
            assert_eq!(p.lock_dids(), ["did:plc:a", "did:plc:b"], "{kind:?}");
            assert!(p.subjects.iter().all(|(_, r)| *r == REASON_INTERACTED));
        }
        assert!(plan(&input(MEMBER, RecordKind::Like, &["did:plc:a"]), &sets(), ScopePolicy::MembersAndFollows, 10, &fresh()).is_empty());
    }

    #[test]
    fn no_growth_for_non_members_private_records_blocks_or_known_subjects() {
        let s = sets();
        let p = ScopePolicy::MembersAndOneHop;
        let r = fresh();
        assert!(plan(&input("did:plc:x", RecordKind::Follow, &["did:plc:new"]), &s, p, 10, &r).is_empty());
        assert!(plan(&input(MEMBER, RecordKind::PrivatePost, &["did:plc:new"]), &s, p, 10, &r).is_empty());
        assert!(plan(&input(MEMBER, RecordKind::Block, &["did:plc:new"]), &s, p, 10, &r).is_empty());
        assert!(plan(&input(MEMBER, RecordKind::Follow, &["did:plc:already"]), &s, p, 10, &r).is_empty());
    }

    #[test]
    fn follow_cap_stops_follow_growth() {
        let mut s = sets();
        s.member_follows.insert(MEMBER.into(), 10);
        assert!(plan(&input(MEMBER, RecordKind::Follow, &["did:plc:new"]), &s, ScopePolicy::MembersAndOneHop, 10, &fresh()).is_empty());
        // Interactions are not capped.
        assert!(!plan(&input(MEMBER, RecordKind::Like, &["did:plc:new"]), &s, ScopePolicy::MembersAndOneHop, 10, &fresh()).is_empty());
    }

    /// T-R1: a like or repost grows S within the like window of its
    /// `createdAt`, a reply or quote within the reply window; a window of 0
    /// turns the kind off; follows have no window.
    #[test]
    fn interactions_grow_only_within_their_window() {
        let recorder = metrics_exporter_prometheus::PrometheusBuilder::new().build_recorder();
        let handle = recorder.handle();
        let _local = metrics::set_default_local_recorder(&recorder);
        let s = sets();
        let p = ScopePolicy::MembersAndOneHop;
        let grows = |kind: RecordKind, r: &Recency| !plan(&input(MEMBER, kind, &["did:plc:new"]), &s, p, 10, r).is_empty();
        let day = TimeDelta::days(1);
        let hour = TimeDelta::hours(1);
        for kind in [RecordKind::Like, RecordKind::Repost] {
            assert!(!grows(kind, &aged(TimeDelta::days(30) + day)), "{kind:?} past the window");
            assert!(grows(kind, &aged(TimeDelta::days(30) - hour)), "{kind:?} inside the window");
        }
        assert!(!grows(RecordKind::Post, &aged(TimeDelta::days(90) + day)));
        assert!(grows(RecordKind::Post, &aged(TimeDelta::days(90) - hour)));
        // A 60-day-old like is stale, a 60-day-old reply fresh.
        assert!(!grows(RecordKind::Like, &aged(TimeDelta::days(60))));
        assert!(grows(RecordKind::Post, &aged(TimeDelta::days(60))));
        // A window of 0 turns the kind off, even for a record made now.
        let mut off = fresh();
        off.windows.like_repost_days = 0;
        assert!(!grows(RecordKind::Like, &off) && !grows(RecordKind::Repost, &off));
        assert!(grows(RecordKind::Post, &off));
        off.windows.reply_quote_days = 0;
        assert!(!grows(RecordKind::Post, &off));
        // Follows are not dated.
        assert!(grows(RecordKind::Follow, &aged(TimeDelta::days(3650))));
        // Each refusal is counted by kind.
        let text = handle.render();
        for (kind, n) in [("like", 3.0), ("repost", 2.0), ("reply", 2.0)] {
            let label = format!("kind=\"{kind}\"");
            assert_eq!(crate::test_support::series(&text, METRIC_GROWTH_STALE, &[&label]), Some(n), "{kind}:\n{text}");
        }
    }

    /// Scope recency §3.2: the holds a write names, in S or not, capped
    /// follows included; a stale interaction names none.
    #[test]
    fn holds_name_every_did_subject() {
        let mut s = sets();
        let p = ScopePolicy::MembersAndOneHop;
        let plan_of = |s: &ScopeSets, kind, subjects: &[&str], r: &Recency| plan(&input(MEMBER, kind, subjects), s, p, 10, r);
        let liked = plan_of(&s, RecordKind::Like, &["did:plc:already", "alice.example"], &fresh());
        assert_eq!(liked.holds, ["did:plc:already"]);
        assert!(liked.subjects.is_empty());
        assert!(plan_of(&s, RecordKind::Like, &["did:plc:new"], &aged(TimeDelta::days(40))).holds.is_empty());
        s.member_follows.insert(MEMBER.into(), 10);
        let capped = plan_of(&s, RecordKind::Follow, &["did:plc:new"], &fresh());
        assert_eq!((capped.holds.as_slice(), capped.is_empty()), (["did:plc:new".to_string()].as_slice(), true));
    }

    /// T-R6d: a subject written as a handle never grows S.
    #[test]
    fn a_subject_that_is_not_a_did_never_grows() {
        let p = plan(
            &input(MEMBER, RecordKind::Like, &["alice.example", "did:plc:new"]),
            &sets(),
            ScopePolicy::MembersAndOneHop,
            10,
            &fresh(),
        );
        assert_eq!(p.lock_dids(), ["did:plc:new"]);
        assert!(plan(&input(MEMBER, RecordKind::Like, &["alice.example"]), &sets(), ScopePolicy::MembersAndOneHop, 10, &fresh()).is_empty());
    }
}
