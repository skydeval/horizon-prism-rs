//! The scope policy (design-v2 §9.2) and the scope predicate.

use super::scope_state::ScopeSets;
use crate::config::ScopePolicy;

/// `scope_dids.reasons` bit: the DID is a member.
pub const REASON_MEMBER: i16 = 1;
/// `scope_dids.reasons` bit: followed by a member.
pub const REASON_FOLLOWED: i16 = 2;
/// `scope_dids.reasons` bit: a member liked, reposted, replied to or quoted it.
pub const REASON_INTERACTED: i16 = 4;

/// `scope_dids.reasons` bit: a member's active activity subscription holds
/// it (scope-recency design §3.6). Never a reason to add a DID.
pub const REASON_SUBSCRIBED: i16 = 8;

/// Whether a member's active activity subscription holds its subject in S
/// under `policy`: every policy but `members_only` (scope-recency design
/// §3.6). A subscription never adds its subject.
#[must_use]
pub fn holds_on_subscription(policy: ScopePolicy) -> bool {
    policy != ScopePolicy::MembersOnly
}

/// Whether a member's follow adds its subject to S under `policy`.
#[must_use]
pub fn grows_on_follow(policy: ScopePolicy) -> bool {
    matches!(
        policy,
        ScopePolicy::MembersAndFollows | ScopePolicy::MembersAndOneHop
    )
}

/// Whether a member's like, repost, reply or quote adds its subject to S.
#[must_use]
pub fn grows_on_interaction(policy: ScopePolicy) -> bool {
    matches!(
        policy,
        ScopePolicy::MembersAndInteractions | ScopePolicy::MembersAndOneHop
    )
}

/// The scope predicate: `did ∈ S`. The policy is embodied in how S grows and
/// is rebuilt; membership of S is the same test for every record type.
#[must_use]
pub fn in_index_scope(did: &str, sets: &ScopeSets) -> bool {
    sets.scope.contains(did)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn policy_growth_matrix() {
        use ScopePolicy::*;
        assert!(!grows_on_follow(MembersOnly) && !grows_on_interaction(MembersOnly));
        assert!(grows_on_follow(MembersAndFollows) && !grows_on_interaction(MembersAndFollows));
        assert!(!grows_on_follow(MembersAndInteractions) && grows_on_interaction(MembersAndInteractions));
        assert!(grows_on_follow(MembersAndOneHop) && grows_on_interaction(MembersAndOneHop));
        assert!(!holds_on_subscription(MembersOnly));
        assert!([MembersAndFollows, MembersAndInteractions, MembersAndOneHop].into_iter().all(holds_on_subscription));
    }

    #[test]
    fn scope_predicate_is_set_membership() {
        let mut sets = ScopeSets::default();
        sets.scope.insert("did:plc:a".into());
        assert!(in_index_scope("did:plc:a", &sets));
        assert!(!in_index_scope("did:plc:b", &sets));
    }
}
