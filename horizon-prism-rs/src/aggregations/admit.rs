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
    /// A stock `app.bsky.feed.threadgate` or `postgate` whose `post` is a
    /// `postPrivate` URI (private posting §4.4, P-18). Its one subject is the
    /// gated post's author.
    PrivateGate,
}

impl RecordKind {
    /// Private-tier kinds (`Kryphocron(Private)`, and gates of private posts).
    #[must_use]
    pub fn is_private(self) -> bool {
        matches!(
            self,
            Self::PrivatePost | Self::PrivateAudience | Self::PrivateBlock | Self::PrivateGate
        )
    }

    /// Block records of either namespace (§9.3 rule 3).
    #[must_use]
    pub fn is_block(self) -> bool {
        matches!(self, Self::Block | Self::PrivateBlock)
    }

    /// The kinds Rule M decides (member-blocks-only design §2): direct and
    /// private blocks, and list blocks.
    #[must_use]
    pub fn is_block_kind(self) -> bool {
        self.is_block() || self == Self::Table("list_blocks")
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
    /// Rule 3: a private owner's gate on its own private post (private
    /// posting §4.4).
    OwnerGate,
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
    /// Rule M (member-blocks-only design §2): a block kind whose author is
    /// not a member and that names no member.
    NoMemberSide,
}

impl RejectReason {
    /// Metric label.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Purging(k) => k.as_str(),
            Self::NotInScope => "not_in_scope",
            Self::PublicOff => "public_off",
            Self::NoMemberSide => "no_member_side",
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
            if matches!(
                input.kind,
                RecordKind::PrivatePost | RecordKind::PrivateAudience | RecordKind::PrivateGate
            ) =>
        {
            return Admission::Reject(RejectReason::Purging(PurgeKind::OutOfScope));
        }
        _ => {}
    }
    // Rule M (member-blocks-only design §2), in every mode and before the
    // off rule: a block kind is stored only with a member side.
    if input.kind.is_block_kind() {
        return block_rule(input, sets);
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
    // Rule 3 for gates (private posting §4.4): an owner's gate naming the
    // owner's own private post, whose author is the op's one subject.
    if input.kind == RecordKind::PrivateGate
        && sets.is_owner(&input.author)
        && input.subjects.len() == 1
        && input.subjects[0] == input.author
    {
        return Admission::Admit(AdmitRule::OwnerGate);
    }
    if removed_subject {
        return Admission::Reject(RejectReason::Purging(PurgeKind::MemberRemoved));
    }
    Admission::Reject(RejectReason::NotInScope)
}

/// Rule M (member-blocks-only design §3): a direct, private or list block is
/// stored when its author is a member, whoever it names, or when it names a
/// member not being removed. Being in S, or a private owner, admits no block.
/// A list block names a list, not an account, so it has no subjects and only
/// the first arm admits it.
fn block_rule(input: &AdmissionInput, sets: &ScopeSets) -> Admission {
    if sets.is_member(&input.author) {
        return Admission::Admit(AdmitRule::AuthorInScope);
    }
    if input
        .subjects
        .iter()
        .any(|s| sets.is_member(s) && sets.purge_kind(s) != Some(PurgeKind::MemberRemoved))
    {
        return Admission::Admit(AdmitRule::InboundToMember);
    }
    if input.subjects.iter().any(|s| sets.purge_kind(s) == Some(PurgeKind::MemberRemoved)) {
        return Admission::Reject(RejectReason::Purging(PurgeKind::MemberRemoved));
    }
    Admission::Reject(RejectReason::NoMemberSide)
}

/// The off rule's table (public-indexing design §5): `Some` decides the op,
/// `None` leaves it to the rules as they stand. Profiles, the self records
/// other than the labeler service, and the private kinds are decided as
/// always. The block kinds never reach it: Rule M decides them first.
fn off_rule(input: &AdmissionInput, sets: &ScopeSets) -> Option<Admission> {
    let off = Some(Admission::Reject(RejectReason::PublicOff));
    let author_member = sets.is_member(&input.author);
    match input.kind {
        RecordKind::Block
        | RecordKind::Profile
        | RecordKind::Table("actor_self_records")
        | RecordKind::PrivatePost
        | RecordKind::PrivateAudience
        | RecordKind::PrivateBlock
        | RecordKind::PrivateGate => None,
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

    /// Member-blocks-only design §2: rule 3 keeps its gate arm only. An
    /// owner that is not a member (the window of a `member_removed` purge)
    /// stores no block of a non-member.
    #[test]
    fn rule_3_admits_no_block() {
        for kind in [RecordKind::Block, RecordKind::PrivateBlock] {
            assert_eq!(
                admit(&input(OWNER, kind, &[STRANGER]), &sets()),
                Admission::Reject(RejectReason::NoMemberSide),
                "{kind:?}"
            );
            assert_eq!(
                admit(&input(OWNER, kind, &[MEMBER]), &sets()),
                Admission::Admit(AdmitRule::InboundToMember),
                "{kind:?}"
            );
        }
        assert_eq!(
            admit(&input(OWNER, RecordKind::Like, &[STRANGER]), &sets()),
            Admission::Reject(RejectReason::NotInScope)
        );
    }

    /// T-MB1 (member-blocks-only design §12): Rule M in every
    /// public-indexing mode.
    #[test]
    fn rule_m_stores_a_block_only_with_a_member_side() {
        const LIST_BLOCK: RecordKind = RecordKind::Table("list_blocks");
        for mode in [PublicMode::On, PublicMode::Filling, PublicMode::Draining, PublicMode::Off, PublicMode::Held] {
            let s = with_mode(mode);
            let a = |author: &str, kind: RecordKind, subjects: &[&str]| admit(&input(author, kind, subjects), &s);
            // A member's block of anyone, list blocks included.
            for kind in [RecordKind::Block, RecordKind::PrivateBlock, LIST_BLOCK] {
                assert_eq!(a(MEMBER, kind, &[STRANGER]), Admission::Admit(AdmitRule::AuthorInScope), "{mode:?} {kind:?}");
            }
            assert_eq!(a(MEMBER, LIST_BLOCK, &[]), Admission::Admit(AdmitRule::AuthorInScope), "{mode:?}");
            for kind in [RecordKind::Block, RecordKind::PrivateBlock] {
                // A scoped non-member's block of a non-member: not stored.
                assert_eq!(a(SCOPED, kind, &[OTHER]), Admission::Reject(RejectReason::NoMemberSide), "{mode:?} {kind:?}");
                assert_eq!(a(STRANGER, kind, &[OTHER]), Admission::Reject(RejectReason::NoMemberSide), "{mode:?} {kind:?}");
                // Of a member: stored.
                assert_eq!(a(SCOPED, kind, &[MEMBER]), Admission::Admit(AdmitRule::InboundToMember), "{mode:?} {kind:?}");
                assert_eq!(a(STRANGER, kind, &[MEMBER]), Admission::Admit(AdmitRule::InboundToMember), "{mode:?} {kind:?}");
            }
            // A non-member's list block, in every mode.
            assert_eq!(a(SCOPED, LIST_BLOCK, &[]), Admission::Reject(RejectReason::NoMemberSide), "{mode:?}");
            assert_eq!(a(STRANGER, LIST_BLOCK, &[]), Admission::Reject(RejectReason::NoMemberSide), "{mode:?}");
            // A block naming only a member under `member_removed`.
            let mut removed = s.clone();
            removed.members.remove(MEMBER);
            removed.purging.insert(MEMBER.into(), PurgeKind::MemberRemoved);
            assert_eq!(
                admit(&input(STRANGER, RecordKind::Block, &[MEMBER]), &removed),
                Admission::Reject(RejectReason::Purging(PurgeKind::MemberRemoved)),
                "{mode:?}"
            );
            // An `account_deleted` member's block.
            let mut deleted = s.clone();
            deleted.purging.insert(MEMBER.into(), PurgeKind::AccountDeleted);
            assert_eq!(
                admit(&input(MEMBER, RecordKind::Block, &[OTHER]), &deleted),
                Admission::Reject(RejectReason::Purging(PurgeKind::AccountDeleted)),
                "{mode:?}"
            );
        }
        assert_eq!(RejectReason::NoMemberSide.as_str(), "no_member_side");
        assert!(LIST_BLOCK.is_block_kind() && RecordKind::Block.is_block_kind() && RecordKind::PrivateBlock.is_block_kind());
        assert!(!RecordKind::Table("list_items").is_block_kind() && !RecordKind::Follow.is_block_kind());
    }

    /// Private posting §4.4 (review 11, finding 6): an owner's gate on its
    /// own private post is admitted by rule 3; on another's post it is not.
    /// The off rule leaves gates of private posts to the rules, and an
    /// `out_of_scope` purge rejects them.
    #[test]
    fn rule_3_owner_gates_on_own_posts() {
        for mode in [PublicMode::On, PublicMode::Off] {
            let s = with_mode(mode);
            assert_eq!(
                admit(&input(OWNER, RecordKind::PrivateGate, &[OWNER]), &s),
                Admission::Admit(AdmitRule::OwnerGate),
                "{mode:?}"
            );
            assert_eq!(
                admit(&input(OWNER, RecordKind::PrivateGate, &[STRANGER]), &s),
                Admission::Reject(RejectReason::NotInScope),
                "{mode:?}"
            );
            assert_eq!(
                admit(&input(STRANGER, RecordKind::PrivateGate, &[STRANGER]), &s),
                Admission::Reject(RejectReason::NotInScope),
                "{mode:?}"
            );
            // Rule 1, in the off modes too.
            assert_eq!(
                admit(&input(SCOPED, RecordKind::PrivateGate, &[SCOPED]), &s),
                Admission::Admit(AdmitRule::AuthorInScope),
                "{mode:?}"
            );
            // The public gate tables stay off under the off rule.
            if mode == PublicMode::Off {
                assert_eq!(admit(&input(MEMBER, RecordKind::Table("threadgates"), &[]), &s), OFF);
            }
        }
        let mut s = sets();
        s.purging.insert(OWNER.into(), PurgeKind::OutOfScope);
        assert_eq!(
            admit(&input(OWNER, RecordKind::PrivateGate, &[OWNER]), &s),
            Admission::Reject(RejectReason::Purging(PurgeKind::OutOfScope))
        );
        assert!(RecordKind::PrivateGate.is_private());
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
            // Blocks by Rule M; S profiles, as today.
            assert_eq!(a(SCOPED, RecordKind::Block, &[OTHER]), Admission::Reject(RejectReason::NoMemberSide));
            assert_eq!(a(STRANGER, RecordKind::Block, &[MEMBER]), Admission::Admit(AdmitRule::InboundToMember));
            assert_eq!(a(OWNER, RecordKind::Block, &[STRANGER]), Admission::Reject(RejectReason::NoMemberSide));
            assert_eq!(a(SCOPED, RecordKind::Profile, &[]), Admission::Admit(AdmitRule::AuthorInScope));
            assert_eq!(a(STRANGER, RecordKind::Profile, &[]), Admission::Reject(RejectReason::NotInScope));
            assert_eq!(a(SCOPED, RecordKind::Table("actor_self_records"), &[]), Admission::Admit(AdmitRule::AuthorInScope));
            // Members' list rows only; a list block by Rule M.
            for table in ["lists", "list_items", "list_optouts", "list_blocks"] {
                assert_eq!(a(MEMBER, RecordKind::Table(table), &[]), Admission::Admit(AdmitRule::AuthorInScope), "{table}");
                let refused = if table == "list_blocks" { Admission::Reject(RejectReason::NoMemberSide) } else { OFF };
                assert_eq!(a(SCOPED, RecordKind::Table(table), &[]), refused, "{table}");
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
        // The removed member's own writes are not rejected (rule 1 while in
        // S), its blocks aside: it is no longer a member, so Rule M stores
        // only those naming a member.
        s.scope.insert(MEMBER.into());
        assert_eq!(
            admit(&input(MEMBER, RecordKind::Like, &[STRANGER]), &s),
            Admission::Admit(AdmitRule::AuthorInScope)
        );
        assert_eq!(
            admit(&input(MEMBER, RecordKind::Block, &[STRANGER]), &s),
            Admission::Reject(RejectReason::NoMemberSide)
        );
        assert_eq!(
            admit(&input(MEMBER, RecordKind::Block, &[OTHER]), &s),
            Admission::Admit(AdmitRule::InboundToMember)
        );
    }
}
