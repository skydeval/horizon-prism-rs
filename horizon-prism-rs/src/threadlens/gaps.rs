//! The server gap taxonomy (design-v3 §5.15.15; design-v2 §10.4).
//!
//! Thread Lens is viewer-relative (proposed locked decision 38): the
//! server enforces the viewer's blocks, so a gap can say which way a block
//! runs. A gap never carries a handle, a name, text, counts or a time:
//! the URI, which names the author's DID, is all a client gets.

/// Why a post is not shown.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Gap {
    /// The viewer blocks the author, directly or through a list.
    BlockedByYou,
    /// The author blocks the viewer, directly or through a list.
    BlockingYou,
    /// The server did not fetch the post because it could not check the
    /// viewer's blocks for its author.
    BlockStateUnknown,
    /// A block the ordinary thread view reported that was not looked past:
    /// a block between other parties, or one whose author's blocks with
    /// the viewer could not be checked. Never from `resolve`.
    Blocked,
    /// A local tombstone, or Slingshot `RecordNotFound` for an author whose
    /// account is locally known to be `active`.
    Deleted,
    /// Every source missed.
    NotFound,
    /// No source answered: every one was transient.
    UpstreamUnavailable,
    /// An upstream view with no record content, or upstream moderation
    /// labels `!takedown` or `!hide`. A `!warn` post is a post node, with
    /// its upstream labels removed (Thread Lens amendment 1 §1).
    ContentStripped,
    /// A private-tier URI: emitted with no lookup at all (§10.5).
    TierHidden,
}

impl Gap {
    /// Every variant (the complete server taxonomy).
    pub const ALL: [Self; 9] = [
        Self::BlockedByYou,
        Self::BlockingYou,
        Self::BlockStateUnknown,
        Self::Blocked,
        Self::Deleted,
        Self::NotFound,
        Self::UpstreamUnavailable,
        Self::ContentStripped,
        Self::TierHidden,
    ];

    /// A block between the viewer and the author. The walks go through
    /// such a gap: the thread around a blocked author's post stays.
    #[must_use]
    pub fn known_block(self) -> bool {
        matches!(self, Self::BlockedByYou | Self::BlockingYou)
    }

    /// The lexicon value.
    #[must_use]
    pub fn label(self) -> &'static str {
        match self {
            Self::BlockedByYou => "blocked-by-you",
            Self::BlockingYou => "blocking-you",
            Self::BlockStateUnknown => "block-state-unknown",
            Self::Blocked => "blocked",
            Self::Deleted => "deleted",
            Self::NotFound => "not-found",
            Self::UpstreamUnavailable => "upstream-unavailable",
            Self::ContentStripped => "content-stripped",
            Self::TierHidden => "tier-hidden",
        }
    }
}

/// How the AppView cascade went for one URI.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CascadeOutcome {
    /// Not consulted (community home, or a `#blockedPost` resolution).
    Skipped,
    /// Consulted; no network answered (all transient, rate-limited or open).
    AllTransient,
    /// Some network answered with a miss or a withheld response.
    Answered,
}

/// How Slingshot went for one URI.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SlingshotOutcome {
    NotFound,
    /// No usable answer (transient, or an unusable body).
    Unavailable,
}

/// §10.4 classification of an unresolved URI (not for `Blocked`, which only
/// `getThread` assigns, nor `ContentStripped`/`TierHidden`, which the ladder
/// assigns directly).
#[must_use]
pub fn classify(tombstoned: bool, cascade: CascadeOutcome, slingshot: SlingshotOutcome, author_active: bool) -> Gap {
    if tombstoned || (slingshot == SlingshotOutcome::NotFound && author_active) {
        return Gap::Deleted;
    }
    if slingshot == SlingshotOutcome::Unavailable && cascade != CascadeOutcome::Answered {
        return Gap::UpstreamUnavailable;
    }
    Gap::NotFound
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_taxonomy() {
        let labels: Vec<&str> = Gap::ALL.iter().map(|g| g.label()).collect();
        assert_eq!(
            labels,
            [
                "blocked-by-you",
                "blocking-you",
                "block-state-unknown",
                "blocked",
                "deleted",
                "not-found",
                "upstream-unavailable",
                "content-stripped",
                "tier-hidden"
            ]
        );
        assert!(Gap::BlockedByYou.known_block() && Gap::BlockingYou.known_block());
        assert!(!Gap::Blocked.known_block() && !Gap::BlockStateUnknown.known_block());
    }

    #[test]
    fn classification_table() {
        use CascadeOutcome::*;
        use SlingshotOutcome::*;
        assert_eq!(classify(true, Answered, Unavailable, false), Gap::Deleted, "tombstone");
        assert_eq!(classify(false, Answered, NotFound, true), Gap::Deleted, "RecordNotFound + active author");
        assert_eq!(classify(false, Answered, NotFound, false), Gap::NotFound, "account status unknown");
        assert_eq!(classify(false, AllTransient, Unavailable, true), Gap::UpstreamUnavailable);
        assert_eq!(classify(false, Skipped, Unavailable, false), Gap::UpstreamUnavailable);
        assert_eq!(classify(false, Answered, Unavailable, false), Gap::NotFound, "a miss is an answer");
        assert_eq!(classify(false, AllTransient, NotFound, false), Gap::NotFound);
    }
}
