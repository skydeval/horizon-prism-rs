//! The admission predicate (design-v2 §9.3): the only function that decides
//! whether a classified record op is stored. The worker, backfill and
//! `index.requestRecord` all call it; there is no other writer.

use super::scope_state::{PurgeKind, ScopeSets};

/// The record kinds v1 indexes (§5.4).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum RecordKind {
    Post,
    Like,
    Repost,
    Follow,
    Block,
    Profile,
    /// A record of one of the tables design-v3 added (§3.2), named by its
    /// table. Admitted by rule 1 alone: it has no rule-2 subjects.
    Table(&'static str),
    PrivatePost,
    PrivateAudience,
    PrivateBlock,
}

impl RecordKind {
    /// Private-tier kinds (`Kryphocron(Private)`).
    #[must_use]
    pub fn is_private(self) -> bool {
        matches!(
            self,
            Self::PrivatePost | Self::PrivateAudience | Self::PrivateBlock
        )
    }

    /// Block records of either namespace (§9.3 rule 3).
    #[must_use]
    pub fn is_block(self) -> bool {
        matches!(self, Self::Block | Self::PrivateBlock)
    }
}

/// What admission needs to know about one op.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AdmissionInput {
    /// The record's author (the repo DID).
    pub author: String,
    pub kind: RecordKind,
    /// Rule-2 subject DIDs (§9.3 table): the author of a liked or reposted
    /// post; the authors of a reply's parent and root; the author of a quoted
    /// post; a follow's or block's subject. Empty for other records.
    pub subjects: Vec<String>,
}

/// Why an op was admitted.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AdmitRule {
    /// Rule 1: author ∈ S.
    AuthorInScope,
    /// Rule 2: inbound interaction whose subject is a member.
    InboundToMember,
    /// Rule 3: a block by a private owner.
    OwnerBlock,
}

/// Why an op was rejected.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RejectReason {
    /// The purging set forbids it for this purge kind (§6.11.7).
    Purging(PurgeKind),
    /// No rule admits it.
    NotInScope,
}

impl RejectReason {
    /// Metric label.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Purging(k) => k.as_str(),
            Self::NotInScope => "not_in_scope",
        }
    }
}

/// The admission decision.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Admission {
    Admit(AdmitRule),
    Reject(RejectReason),
}

impl Admission {
    #[must_use]
    pub fn is_admitted(self) -> bool {
        matches!(self, Self::Admit(_))
    }
}

/// Decide admission for a create or update (§9.3). Deletes are never
/// admission-checked (§5.5).
#[must_use]
pub fn admit(input: &AdmissionInput, sets: &ScopeSets) -> Admission {
    // Purge-kind rejections (§6.11.7 table) come first.
    match sets.purge_kind(&input.author) {
        Some(PurgeKind::AccountDeleted) => {
            return Admission::Reject(RejectReason::Purging(PurgeKind::AccountDeleted));
        }
        Some(PurgeKind::OutOfScope)
            if matches!(input.kind, RecordKind::PrivatePost | RecordKind::PrivateAudience) =>
        {
            return Admission::Reject(RejectReason::Purging(PurgeKind::OutOfScope));
        }
        _ => {}
    }
    // Rule 1.
    if sets.in_scope(&input.author) {
        return Admission::Admit(AdmitRule::AuthorInScope);
    }
    // Rule 2. A member being purged as `member_removed` is not a subject
    // (its removal already dropped it from `members`; this keeps the rule
    // explicit). Another current member subject still admits.
    let member_subject = input.subjects.iter().any(|s| {
        sets.is_member(s) && sets.purge_kind(s) != Some(PurgeKind::MemberRemoved)
    });
    if member_subject {
        return Admission::Admit(AdmitRule::InboundToMember);
    }
    let removed_subject = input
        .subjects
        .iter()
        .any(|s| sets.purge_kind(s) == Some(PurgeKind::MemberRemoved));
    // Rule 3.
    if input.kind.is_block() && sets.is_owner(&input.author) {
        return Admission::Admit(AdmitRule::OwnerBlock);
    }
    if removed_subject {
        return Admission::Reject(RejectReason::Purging(PurgeKind::MemberRemoved));
    }
    Admission::Reject(RejectReason::NotInScope)
}

#[cfg(test)]
mod tests {
    use super::*;

    const MEMBER: &str = "did:plc:member";
    const SCOPED: &str = "did:plc:scoped";
    const OWNER: &str = "did:plc:owner";
    const STRANGER: &str = "did:plc:stranger";
    const OTHER: &str = "did:plc:other";

    fn sets() -> ScopeSets {
        let mut s = ScopeSets::default();
        s.members.insert(MEMBER.into());
        s.scope.insert(MEMBER.into());
        s.scope.insert(SCOPED.into());
        s.private_owners.insert(OWNER.into());
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
    fn rule_1_author_in_scope() {
        for kind in [RecordKind::Post, RecordKind::Like, RecordKind::Profile, RecordKind::PrivatePost] {
            assert_eq!(
                admit(&input(SCOPED, kind, &[]), &sets()),
                Admission::Admit(AdmitRule::AuthorInScope)
            );
        }
    }

    #[test]
    fn rule_2_inbound_interactions_on_member_content() {
        for kind in [
            RecordKind::Like,
            RecordKind::Repost,
            RecordKind::Post,
            RecordKind::Follow,
            RecordKind::Block,
            RecordKind::PrivatePost,
            RecordKind::PrivateBlock,
        ] {
            assert_eq!(
                admit(&input(STRANGER, kind, &[OTHER, MEMBER]), &sets()),
                Admission::Admit(AdmitRule::InboundToMember),
                "{kind:?}"
            );
        }
    }

    #[test]
    fn rule_3_owner_blocks_only() {
        assert_eq!(
            admit(&input(OWNER, RecordKind::Block, &[STRANGER]), &sets()),
            Admission::Admit(AdmitRule::OwnerBlock)
        );
        assert_eq!(
            admit(&input(OWNER, RecordKind::PrivateBlock, &[STRANGER]), &sets()),
            Admission::Admit(AdmitRule::OwnerBlock)
        );
        assert_eq!(
            admit(&input(OWNER, RecordKind::Like, &[STRANGER]), &sets()),
            Admission::Reject(RejectReason::NotInScope)
        );
    }

    #[test]
    fn everything_else_is_rejected() {
        assert_eq!(
            admit(&input(STRANGER, RecordKind::Post, &[OTHER]), &sets()),
            Admission::Reject(RejectReason::NotInScope)
        );
        assert_eq!(
            admit(&input(STRANGER, RecordKind::Profile, &[]), &sets()),
            Admission::Reject(RejectReason::NotInScope)
        );
    }

    #[test]
    fn account_deleted_rejects_every_authored_write() {
        let mut s = sets();
        s.purging.insert(SCOPED.into(), PurgeKind::AccountDeleted);
        assert_eq!(
            admit(&input(SCOPED, RecordKind::Post, &[MEMBER]), &s),
            Admission::Reject(RejectReason::Purging(PurgeKind::AccountDeleted))
        );
    }

    #[test]
    fn out_of_scope_rejects_only_private_post_and_audience_records() {
        let mut s = sets();
        s.purging.insert(STRANGER.into(), PurgeKind::OutOfScope);
        assert_eq!(
            admit(&input(STRANGER, RecordKind::PrivatePost, &[MEMBER]), &s),
            Admission::Reject(RejectReason::Purging(PurgeKind::OutOfScope))
        );
        assert_eq!(
            admit(&input(STRANGER, RecordKind::PrivateAudience, &[]), &s),
            Admission::Reject(RejectReason::Purging(PurgeKind::OutOfScope))
        );
        assert_eq!(
            admit(&input(STRANGER, RecordKind::Like, &[MEMBER]), &s),
            Admission::Admit(AdmitRule::InboundToMember)
        );
    }

    #[test]
    fn member_removed_rejects_inbound_only_when_it_is_the_only_member_subject() {
        let mut s = sets();
        s.members.remove(MEMBER);
        s.purging.insert(MEMBER.into(), PurgeKind::MemberRemoved);
        s.members.insert(OTHER.into());
        assert_eq!(
            admit(&input(STRANGER, RecordKind::Like, &[MEMBER]), &s),
            Admission::Reject(RejectReason::Purging(PurgeKind::MemberRemoved))
        );
        // A reply whose root author is another current member is admitted.
        assert_eq!(
            admit(&input(STRANGER, RecordKind::Post, &[MEMBER, OTHER]), &s),
            Admission::Admit(AdmitRule::InboundToMember)
        );
        // The removed member's own writes are not rejected (rule 1 while in S).
        s.scope.insert(MEMBER.into());
        assert_eq!(
            admit(&input(MEMBER, RecordKind::Block, &[STRANGER]), &s),
            Admission::Admit(AdmitRule::AuthorInScope)
        );
    }
}
