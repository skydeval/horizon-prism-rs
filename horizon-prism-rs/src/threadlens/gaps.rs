//! The server gap taxonomy (design-v2 §10.2, §10.4).
//!
//! The block-party rule (§10.3) is enforced by construction: there is no
//! `blocked-by-you` or `blocking-you` variant. Those are client-side
//! classifications made from the client's own block graph; the server never
//! knows the viewer's blocks. `Blocked` means a block between **other**
//! parties, visible in an upstream structure fetched with no viewer.

/// Why a post is not shown.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Gap {
    /// Produced only by `getThread`: an upstream `#blockedPost` that local
    /// data and Slingshot did not resolve.
    Blocked,
    /// A local tombstone, or Slingshot `RecordNotFound` for an author whose
    /// account is locally known to be `active`.
    Deleted,
    /// Every source missed.
    NotFound,
    /// No source answered: every one was transient.
    UpstreamUnavailable,
    /// An upstream view with no record content, or upstream moderation
    /// labels (`!takedown`, `!hide`, `!warn`).
    ContentStripped,
    /// A private-tier URI: emitted with no lookup at all (§10.5).
    TierHidden,
}

impl Gap {
    /// Every variant (the complete server taxonomy).
    pub const ALL: [Self; 6] = [
        Self::Blocked,
        Self::Deleted,
        Self::NotFound,
        Self::UpstreamUnavailable,
        Self::ContentStripped,
        Self::TierHidden,
    ];

    /// The lexicon value.
    #[must_use]
    pub fn label(self) -> &'static str {
        match self {
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
    fn taxonomy_excludes_viewer_relative_blocks() {
        let labels: Vec<&str> = Gap::ALL.iter().map(|g| g.label()).collect();
        assert_eq!(
            labels,
            ["blocked", "deleted", "not-found", "upstream-unavailable", "content-stripped", "tier-hidden"]
        );
        for forbidden in ["blocked-by-you", "blocking-you"] {
            assert!(!labels.contains(&forbidden));
        }
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
