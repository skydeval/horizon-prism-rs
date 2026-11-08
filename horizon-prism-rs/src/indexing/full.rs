//! Full indexing's record groups: which collections each one stores for
//! every account.

use std::collections::BTreeSet;

use crate::config::FullGroup;
use crate::firehose::classifier::{
    NSID_ACTOR_STATUS, NSID_BLOCK, NSID_CONTENT_VISIBILITY, NSID_FOLLOW, NSID_LABELER_SERVICE, NSID_LIKE, NSID_LIST,
    NSID_LIST_OPTOUT, NSID_LISTBLOCK, NSID_LISTITEM, NSID_NOTIFICATION_DECLARATION, NSID_POLL_POST, NSID_POST,
    NSID_POSTGATE, NSID_PROFILE, NSID_REPOST, NSID_STARTER_PACK, NSID_THREADGATE, PLURAL_COLLECTIONS,
};

/// The collections of one group.
#[must_use]
pub fn collections(group: FullGroup) -> &'static [&'static str] {
    match group {
        FullGroup::Posts => &[NSID_POST, NSID_THREADGATE, NSID_POSTGATE],
        FullGroup::Likes => &[NSID_LIKE],
        FullGroup::Reposts => &[NSID_REPOST],
        FullGroup::Follows => &[NSID_FOLLOW],
        FullGroup::Blocks => &[NSID_BLOCK, NSID_LISTBLOCK],
        FullGroup::Lists => &[NSID_LIST, NSID_LISTITEM, NSID_LIST_OPTOUT],
        FullGroup::Profiles => &[
            NSID_PROFILE,
            NSID_LABELER_SERVICE,
            NSID_NOTIFICATION_DECLARATION,
            NSID_ACTOR_STATUS,
            NSID_CONTENT_VISIBILITY,
        ],
        FullGroup::StarterPacks => &[NSID_STARTER_PACK],
        FullGroup::Polls => &[NSID_POLL_POST],
    }
}

/// The group a collection belongs to, if any.
#[must_use]
pub fn group_of(nsid: &str) -> Option<FullGroup> {
    FullGroup::ALL.into_iter().find(|g| collections(*g).contains(&nsid))
}

/// The full set: every collection of every enabled group, plus the plural
/// and pronoun records, which are kept for any author.
#[must_use]
pub fn full_set(kinds: &[FullGroup]) -> BTreeSet<String> {
    kinds
        .iter()
        .flat_map(|g| collections(*g).iter())
        .chain(PLURAL_COLLECTIONS.iter())
        .map(|s| (*s).to_string())
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::firehose::classifier::STORED_SET;

    /// Every group's collections are stored collections, no collection is
    /// in two groups, and the full set of every group is the stored set
    /// without the private collection.
    #[test]
    fn groups_partition_the_public_stored_set() {
        let mut seen = BTreeSet::new();
        for g in FullGroup::ALL {
            for c in collections(g) {
                assert!(STORED_SET.contains(c), "{c}");
                assert!(seen.insert(*c), "{c} is in two groups");
                assert_eq!(group_of(c), Some(g));
            }
        }
        let all = full_set(&FullGroup::ALL);
        let public: BTreeSet<String> = STORED_SET
            .iter()
            .filter(|c| !c.starts_with("tools.kryphocron."))
            .map(|s| (*s).to_string())
            .collect();
        assert_eq!(all, public);
        let posts_only = full_set(&[FullGroup::Posts]);
        assert!(posts_only.contains(NSID_POST) && posts_only.contains(PLURAL_COLLECTIONS[0]));
        assert!(!posts_only.contains(NSID_LIKE));
    }
}
