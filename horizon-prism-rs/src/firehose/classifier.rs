//! Record classification (design-v2 §5.4).
//!
//! 1. `kryphocron_lexicons::Tier::from_nsid` answers for the closed
//!    `tools.kryphocron.*` registry: `Kryphocron(tier)`.
//! 2. `NotRegistered` is not an error; classification falls through.
//! 3. The v1 ecosystem NSIDs map to `Ecosystem(kind)`.
//! 4. Everything else is `Unindexed`.
//!
//! The routing on [`RecordClass`] is a `match` with no default arm, so a
//! private-tier op can only reach `private_tier` and an ecosystem op can only
//! reach public tables.

use kryphocron_lexicons::Tier;

use crate::aggregations::RecordKind;

pub const NSID_POST: &str = "app.bsky.feed.post";
pub const NSID_LIKE: &str = "app.bsky.feed.like";
pub const NSID_REPOST: &str = "app.bsky.feed.repost";
pub const NSID_FOLLOW: &str = "app.bsky.graph.follow";
pub const NSID_BLOCK: &str = "app.bsky.graph.block";
pub const NSID_PROFILE: &str = "app.bsky.actor.profile";
pub const NSID_LIST: &str = "app.bsky.graph.list";
pub const NSID_LISTITEM: &str = "app.bsky.graph.listitem";
pub const NSID_LISTBLOCK: &str = "app.bsky.graph.listblock";
pub const NSID_LIST_OPTOUT: &str = "app.bsky.graph.referencelistoptout";
pub const NSID_STARTER_PACK: &str = "app.bsky.graph.starterpack";
pub const NSID_LABELER_SERVICE: &str = "app.bsky.labeler.service";
pub const NSID_NOTIFICATION_DECLARATION: &str = "app.bsky.notification.declaration";
pub const NSID_ACTOR_STATUS: &str = "app.bsky.actor.status";
pub const NSID_CONTENT_VISIBILITY: &str = "app.bsky.actor.contentVisibilityDeclaration";
pub const NSID_THREADGATE: &str = "app.bsky.feed.threadgate";
pub const NSID_POSTGATE: &str = "app.bsky.feed.postgate";
pub const NSID_POLL_POST: &str = "app.nearhorizon.poll.post";
/// The four collections of pronouns and plural systems (plural-pronouns
/// design §0): the first three at rkey `self` only.
pub const NSID_PRONOUNS: &str = "app.nearhorizon.actor.pronouns";
pub const NSID_PLURAL_SYSTEM: &str = "app.nearhorizon.plural.system";
pub const NSID_PLURAL_FRONT: &str = "app.nearhorizon.plural.front";
pub const NSID_PLURAL_MEMBER: &str = "app.nearhorizon.plural.member";
/// The plural collections, the self-keyed three first.
pub const PLURAL_COLLECTIONS: [&str; 4] = [NSID_PRONOUNS, NSID_PLURAL_SYSTEM, NSID_PLURAL_FRONT, NSID_PLURAL_MEMBER];
pub const NSID_POST_PRIVATE: &str = "tools.kryphocron.feed.postPrivate";
pub const NSID_AUDIENCE: &str = "tools.kryphocron.policy.audience";

/// Ecosystem record kinds indexed in v1.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum EcosystemKind {
    Post,
    Like,
    Repost,
    Follow,
    Block,
    /// `app.bsky.actor.profile`, rkey `self` only.
    Profile,
    // The collections design-v3 added (§3.2).
    List,
    ListItem,
    ListBlock,
    ListOptout,
    StarterPack,
    /// `app.bsky.labeler.service`, rkey `self` only.
    LabelerService,
    /// `app.bsky.notification.declaration`, `app.bsky.actor.status` or
    /// `app.bsky.actor.contentVisibilityDeclaration`, rkey `self` only.
    SelfRecord,
    Threadgate,
    Postgate,
    PollPost,
    /// A record of [`PLURAL_COLLECTIONS`] (plural-pronouns design §2.2),
    /// stored for any author in `plural_records`.
    Plural,
}

/// Kryphocron records Prism indexes (§5.4 table). Private blocks
/// (`tools.kryphocron.graph.block`) are not among them since skydeval's
/// decision of 2026-10-24: a public block already keeps the two accounts
/// apart, so the NSID is skipped like any unproduced one. Nor are audiences
/// (`tools.kryphocron.policy.audience`): this AppView holds them, written
/// through its own methods (private-audiences design §4.1), so a repository
/// copy is skipped the same way.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum KryphocronKind {
    PostPrivate,
}

/// The classification of one op.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecordClass {
    /// A registered `tools.kryphocron.*` NSID. `kind` is `None` for registry
    /// NSIDs with no producer (skipped as `kryphocron_unproduced`).
    Kryphocron { tier: Tier, kind: Option<KryphocronKind> },
    Ecosystem(EcosystemKind),
    Unindexed,
}

impl RecordClass {
    /// The admission kind of a class that is written, if any.
    #[must_use]
    pub fn record_kind(self) -> Option<RecordKind> {
        match self {
            Self::Ecosystem(k) => Some(match k {
                EcosystemKind::Post => RecordKind::Post,
                EcosystemKind::Like => RecordKind::Like,
                EcosystemKind::Repost => RecordKind::Repost,
                EcosystemKind::Follow => RecordKind::Follow,
                EcosystemKind::Block => RecordKind::Block,
                EcosystemKind::Profile => RecordKind::Profile,
                other => RecordKind::Table(super::rows::table(other)?),
            }),
            Self::Kryphocron {
                tier: Tier::Private,
                kind: Some(k),
            } => Some(match k {
                KryphocronKind::PostPrivate => RecordKind::PrivatePost,
            }),
            Self::Kryphocron { .. } | Self::Unindexed => None,
        }
    }
}

/// Classify a collection NSID (and, for profiles, the rkey).
#[must_use]
pub fn classify(nsid: &str, rkey: &str) -> RecordClass {
    if let Ok(parsed) = kryphocron_lexicons::Nsid::new(nsid)
        && let Ok(tier) = Tier::from_nsid(&parsed)
    {
        let kind = match (tier, nsid) {
            (Tier::Private, NSID_POST_PRIVATE) => Some(KryphocronKind::PostPrivate),
            _ => None,
        };
        return RecordClass::Kryphocron { tier, kind };
    }
    match nsid {
        NSID_POST => RecordClass::Ecosystem(EcosystemKind::Post),
        NSID_LIKE => RecordClass::Ecosystem(EcosystemKind::Like),
        NSID_REPOST => RecordClass::Ecosystem(EcosystemKind::Repost),
        NSID_FOLLOW => RecordClass::Ecosystem(EcosystemKind::Follow),
        NSID_BLOCK => RecordClass::Ecosystem(EcosystemKind::Block),
        NSID_PROFILE if rkey == "self" => RecordClass::Ecosystem(EcosystemKind::Profile),
        NSID_LIST => RecordClass::Ecosystem(EcosystemKind::List),
        NSID_LISTITEM => RecordClass::Ecosystem(EcosystemKind::ListItem),
        NSID_LISTBLOCK => RecordClass::Ecosystem(EcosystemKind::ListBlock),
        NSID_LIST_OPTOUT => RecordClass::Ecosystem(EcosystemKind::ListOptout),
        NSID_STARTER_PACK => RecordClass::Ecosystem(EcosystemKind::StarterPack),
        NSID_THREADGATE => RecordClass::Ecosystem(EcosystemKind::Threadgate),
        NSID_POSTGATE => RecordClass::Ecosystem(EcosystemKind::Postgate),
        NSID_POLL_POST => RecordClass::Ecosystem(EcosystemKind::PollPost),
        NSID_PRONOUNS | NSID_PLURAL_SYSTEM | NSID_PLURAL_FRONT if rkey == "self" => RecordClass::Ecosystem(EcosystemKind::Plural),
        NSID_PLURAL_MEMBER => RecordClass::Ecosystem(EcosystemKind::Plural),
        // rkey-self records accept only rkey `self`, like profiles (§3.2).
        NSID_LABELER_SERVICE if rkey == "self" => RecordClass::Ecosystem(EcosystemKind::LabelerService),
        NSID_NOTIFICATION_DECLARATION | NSID_ACTOR_STATUS | NSID_CONTENT_VISIBILITY if rkey == "self" => {
            RecordClass::Ecosystem(EcosystemKind::SelfRecord)
        }
        _ => RecordClass::Unindexed,
    }
}

/// `firehose_ops_skipped_total{reason}` label for a class that is not written.
#[must_use]
pub fn skip_reason(class: RecordClass) -> Option<&'static str> {
    match class {
        RecordClass::Unindexed => Some("unindexed"),
        RecordClass::Kryphocron { kind: None, .. }
        | RecordClass::Kryphocron {
            tier: Tier::Public, ..
        } => Some("kryphocron_unproduced"),
        RecordClass::Kryphocron { .. } | RecordClass::Ecosystem(_) => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ecosystem_nsids() {
        assert_eq!(classify(NSID_POST, "3k"), RecordClass::Ecosystem(EcosystemKind::Post));
        assert_eq!(classify(NSID_LIKE, "3k"), RecordClass::Ecosystem(EcosystemKind::Like));
        assert_eq!(classify(NSID_REPOST, "3k"), RecordClass::Ecosystem(EcosystemKind::Repost));
        assert_eq!(classify(NSID_FOLLOW, "3k"), RecordClass::Ecosystem(EcosystemKind::Follow));
        assert_eq!(classify(NSID_BLOCK, "3k"), RecordClass::Ecosystem(EcosystemKind::Block));
        assert_eq!(classify(NSID_PROFILE, "self"), RecordClass::Ecosystem(EcosystemKind::Profile));
    }

    #[test]
    fn profile_other_rkeys_and_other_nsids_are_unindexed() {
        assert_eq!(classify(NSID_PROFILE, "3k"), RecordClass::Unindexed);
        assert_eq!(classify("app.bsky.feed.generator", "3k"), RecordClass::Unindexed);
        assert_eq!(classify("app.bsky.graph.verification", "3k"), RecordClass::Unindexed);
        assert_eq!(classify("app.nearhorizon.poll.vote", "3k"), RecordClass::Unindexed);
        assert_eq!(classify("not an nsid", "3k"), RecordClass::Unindexed);
        assert_eq!(skip_reason(RecordClass::Unindexed), Some("unindexed"));
    }

    /// design-v3 §3.2: the twelve collections v3 indexes.
    #[test]
    fn v3_collections_and_their_tables() {
        for (nsid, kind, table) in [
            (NSID_LIST, EcosystemKind::List, "lists"),
            (NSID_LISTITEM, EcosystemKind::ListItem, "list_items"),
            (NSID_LISTBLOCK, EcosystemKind::ListBlock, "list_blocks"),
            (NSID_LIST_OPTOUT, EcosystemKind::ListOptout, "list_optouts"),
            (NSID_STARTER_PACK, EcosystemKind::StarterPack, "starter_packs"),
            (NSID_THREADGATE, EcosystemKind::Threadgate, "threadgates"),
            (NSID_POSTGATE, EcosystemKind::Postgate, "postgates"),
            (NSID_POLL_POST, EcosystemKind::PollPost, "nearhorizon_poll_posts"),
        ] {
            let c = classify(nsid, "3kaaaaaaaaaaa");
            assert_eq!(c, RecordClass::Ecosystem(kind), "{nsid}");
            assert_eq!(c.record_kind(), Some(RecordKind::Table(table)), "{nsid}");
        }
        // rkey-self records accept only `self`.
        for (nsid, kind, table) in [
            (NSID_LABELER_SERVICE, EcosystemKind::LabelerService, "labeler_services"),
            (NSID_NOTIFICATION_DECLARATION, EcosystemKind::SelfRecord, "actor_self_records"),
            (NSID_ACTOR_STATUS, EcosystemKind::SelfRecord, "actor_self_records"),
            (NSID_CONTENT_VISIBILITY, EcosystemKind::SelfRecord, "actor_self_records"),
        ] {
            assert_eq!(classify(nsid, "self").record_kind(), Some(RecordKind::Table(table)), "{nsid}");
            assert_eq!(classify(nsid, "self"), RecordClass::Ecosystem(kind));
            assert_eq!(classify(nsid, "3kaaaaaaaaaaa"), RecordClass::Unindexed, "{nsid}");
        }
    }

    /// T-P1 (plural-pronouns design §2.2): the four plural collections, the
    /// self-keyed three at `self` only.
    #[test]
    fn plural_collections_and_their_table() {
        for nsid in PLURAL_COLLECTIONS {
            let c = classify(nsid, "self");
            assert_eq!(c, RecordClass::Ecosystem(EcosystemKind::Plural), "{nsid}");
            assert_eq!(c.record_kind(), Some(RecordKind::Table("plural_records")), "{nsid}");
        }
        for nsid in [NSID_PRONOUNS, NSID_PLURAL_SYSTEM, NSID_PLURAL_FRONT] {
            assert_eq!(classify(nsid, "3kaaaaaaaaaaa"), RecordClass::Unindexed, "{nsid}");
        }
        assert_eq!(classify(NSID_PLURAL_MEMBER, "3kaaaaaaaaaaa"), RecordClass::Ecosystem(EcosystemKind::Plural));
        assert_eq!(classify("app.nearhorizon.plural.fronters", "3k"), RecordClass::Unindexed);
    }

    #[test]
    fn produced_kryphocron_nsids_are_private() {
        let c = classify(NSID_POST_PRIVATE, "3k");
        assert_eq!(
            c,
            RecordClass::Kryphocron {
                tier: Tier::Private,
                kind: Some(KryphocronKind::PostPrivate)
            }
        );
        assert_eq!(c.record_kind(), Some(RecordKind::PrivatePost));
        assert_eq!(skip_reason(c), None);
    }

    #[test]
    fn unproduced_kryphocron_nsids_are_skipped() {
        for nsid in [
            "tools.kryphocron.feed.postPublic",
            "tools.kryphocron.feed.like",
            "tools.kryphocron.feed.repost",
            "tools.kryphocron.feed.threadgate",
            "tools.kryphocron.graph.mute",
            // Private blocks, no longer indexed (skydeval, 2026-10-24).
            "tools.kryphocron.graph.block",
            // Audiences, held by this AppView (private-audiences design).
            NSID_AUDIENCE,
        ] {
            let c = classify(nsid, "3k");
            assert!(matches!(c, RecordClass::Kryphocron { .. }), "{nsid}");
            assert_eq!(c.record_kind(), None, "{nsid}");
            assert_eq!(skip_reason(c), Some("kryphocron_unproduced"), "{nsid}");
        }
    }

    #[test]
    fn unregistered_kryphocron_namespace_falls_through() {
        assert_eq!(classify("tools.kryphocron.feed.unknownThing", "3k"), RecordClass::Unindexed);
    }
}
