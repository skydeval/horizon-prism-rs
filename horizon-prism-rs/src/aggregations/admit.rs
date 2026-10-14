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
    /// The off rule (public-indexing design §5) stores no such record.
    PublicOff,
}

impl RejectReason {
    /// Metric label.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Purging(k) => k.as_str(),
            Self::NotInScope => "not_in_scope",
            Self::PublicOff => "public_off",
        }
    }
}

/// The admission decision.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Admission {
    Admit(AdmitRule),
    Reject(RejectReason),
    /// Under the off rule, a member's post: no `posts` row, only its
    /// reference and its scope growth (public-indexing design §4.1).
    GrowOnly,
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
    // The off rule, in the off modes only (public-indexing design §5).
    if sets.public.mode.off_rule()
        && let Some(decision) = off_rule(input, sets)
    {
        return decision;
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

/// The off rule's table (public-indexing design §5): `Some` decides the op,
/// `None` leaves it to the rules as they stand. Blocks, profiles, the self
/// records other than the labeler service, and the private kinds are
/// decided as always.
fn off_rule(input: &AdmissionInput, sets: &ScopeSets) -> Option<Admission> {
    let off = Some(Admission::Reject(RejectReason::PublicOff));
    let author_member = sets.is_member(&input.author);
    match input.kind {
        RecordKind::Block
        | RecordKind::Profile
        | RecordKind::Table("actor_self_records")
        | RecordKind::PrivatePost
        | RecordKind::PrivateAudience
        | RecordKind::PrivateBlock => None,
        RecordKind::Follow => {
            let subject_member = input.subjects.iter().any(|s| sets.is_member(s));
            if author_member || subject_member { None } else { off }
        }
        RecordKind::Like
        | RecordKind::Repost
        | RecordKind::Table("lists" | "list_items" | "list_optouts" | "list_blocks") => {
            if author_member { None } else { off }
        }
        RecordKind::Post => {
            if author_member { Some(Admission::GrowOnly) } else { off }
        }
        // Threadgates, postgates, starter packs, the labeler service and
        // poll posts.
        RecordKind::Table(_) => off,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::indexing::PublicMode;

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

    fn with_mode(mode: PublicMode) -> ScopeSets {
        let mut s = sets();
        s.public.mode = mode;
        s
    }

    const OFF: Admission = Admission::Reject(RejectReason::PublicOff);

    /// T-2: the off rule's table, in each of the off modes.
    #[test]
    fn the_off_rule_keeps_only_the_needed_set() {
        for mode in [PublicMode::Draining, PublicMode::Off, PublicMode::Held] {
            let s = with_mode(mode);
            let a = |author: &str, kind: RecordKind, subjects: &[&str]| admit(&input(author, kind, subjects), &s);
            // Posts: a member's grows only; anyone else's is not stored.
            assert_eq!(a(MEMBER, RecordKind::Post, &[STRANGER]), Admission::GrowOnly, "{mode:?}");
            assert_eq!(a(SCOPED, RecordKind::Post, &[MEMBER]), OFF);
            assert_eq!(a(STRANGER, RecordKind::Post, &[MEMBER]), OFF);
            // Likes and reposts: a member's only.
            assert_eq!(a(MEMBER, RecordKind::Like, &[STRANGER]), Admission::Admit(AdmitRule::AuthorInScope));
            assert_eq!(a(SCOPED, RecordKind::Like, &[MEMBER]), OFF);
            assert_eq!(a(SCOPED, RecordKind::Repost, &[OTHER]), OFF);
            // Follows: a member at either end.
            assert_eq!(a(STRANGER, RecordKind::Follow, &[MEMBER]), Admission::Admit(AdmitRule::InboundToMember));
            assert_eq!(a(MEMBER, RecordKind::Follow, &[STRANGER]), Admission::Admit(AdmitRule::AuthorInScope));
            assert_eq!(a(SCOPED, RecordKind::Follow, &[OTHER]), OFF);
            // Blocks and S profiles, as today.
            assert_eq!(a(SCOPED, RecordKind::Block, &[OTHER]), Admission::Admit(AdmitRule::AuthorInScope));
            assert_eq!(a(STRANGER, RecordKind::Block, &[MEMBER]), Admission::Admit(AdmitRule::InboundToMember));
            assert_eq!(a(OWNER, RecordKind::Block, &[STRANGER]), Admission::Admit(AdmitRule::OwnerBlock));
            assert_eq!(a(SCOPED, RecordKind::Profile, &[]), Admission::Admit(AdmitRule::AuthorInScope));
            assert_eq!(a(STRANGER, RecordKind::Profile, &[]), Admission::Reject(RejectReason::NotInScope));
            assert_eq!(a(SCOPED, RecordKind::Table("actor_self_records"), &[]), Admission::Admit(AdmitRule::AuthorInScope));
            // Members' list rows only.
            for table in ["lists", "list_items", "list_optouts", "list_blocks"] {
                assert_eq!(a(MEMBER, RecordKind::Table(table), &[]), Admission::Admit(AdmitRule::AuthorInScope), "{table}");
                assert_eq!(a(SCOPED, RecordKind::Table(table), &[]), OFF, "{table}");
            }
            // Never stored, a member's included.
            for table in ["threadgates", "postgates", "starter_packs", "labeler_services", "nearhorizon_poll_posts"] {
                assert_eq!(a(MEMBER, RecordKind::Table(table), &[]), OFF, "{table}");
            }
            // Private kinds are unchanged.
            assert_eq!(a(SCOPED, RecordKind::PrivatePost, &[]), Admission::Admit(AdmitRule::AuthorInScope));
            assert_eq!(a(STRANGER, RecordKind::PrivatePost, &[MEMBER]), Admission::Admit(AdmitRule::InboundToMember));
            assert_eq!(a(STRANGER, RecordKind::PrivateAudience, &[]), Admission::Reject(RejectReason::NotInScope));
            // The rejection is counted under its own label.
            assert_eq!(RejectReason::PublicOff.as_str(), "public_off");
        }
    }

    /// T-2: purge-kind rejections come before the off rule.
    #[test]
    fn purge_rejections_come_before_the_off_rule() {
        let mut s = with_mode(PublicMode::Off);
        s.purging.insert(MEMBER.into(), PurgeKind::AccountDeleted);
        assert_eq!(
            admit(&input(MEMBER, RecordKind::Post, &[]), &s),
            Admission::Reject(RejectReason::Purging(PurgeKind::AccountDeleted))
        );
        assert!(!Admission::GrowOnly.is_admitted());
    }

    /// T-2: in Filling and On every input gets today's answer.
    #[test]
    fn filling_and_on_admit_as_today() {
        let kinds = [
            RecordKind::Post,
            RecordKind::Like,
            RecordKind::Repost,
            RecordKind::Follow,
            RecordKind::Block,
            RecordKind::Profile,
            RecordKind::Table("threadgates"),
            RecordKind::Table("labeler_services"),
            RecordKind::Table("lists"),
            RecordKind::PrivatePost,
        ];
        for mode in [PublicMode::Filling, PublicMode::On] {
            let s = with_mode(mode);
            for author in [MEMBER, SCOPED, OWNER, STRANGER] {
                for kind in kinds {
                    for subjects in [&[][..], &[MEMBER][..], &[OTHER][..]] {
                        let i = input(author, kind, subjects);
                        assert_eq!(admit(&i, &s), admit(&i, &sets()), "{mode:?} {author} {kind:?} {subjects:?}");
                    }
                }
            }
        }
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
