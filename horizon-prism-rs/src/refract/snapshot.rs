//! What the moderation gate hides for Refract (refract design §4.2, §7.4).
//!
//! - [`RefractHidden`] is the stored set, kept in `ScopeSets` behind an
//!   `Arc` and swapped on change. It is stored **unfiltered**: the split by
//!   membership and S is made per request, because S changes at runtime.
//! - [`RefractSnapshot`] is what one request's gate reads, built once per
//!   request from the stored set, the mode and each stored DID's S and
//!   member bits ([`Captured`]), and shared by every clone of the request.
//! - [`RefractCheck`] is the request's serve-time label check (§4.5): URIs it
//!   found hidden and un-hidden. It overrides `records` only.
//! - [`RefractView`] is the pair the gate's free functions take.
//!
//! "Refract hides only what it added": an S DID's or a member's profile,
//! kept records and private posts are never hidden here.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, LazyLock};

use chrono::{DateTime, Utc};

use crate::aggregations::ScopeSets;
use crate::db::locks::uri_author;
use crate::firehose::backfill::OFF_SELF_RECORDS;
use crate::hydration::normalize::is_private_uri;

pub use crate::firehose::classifier::{NSID_FOLLOW, NSID_POST, NSID_PROFILE};

/// Why a DID in R (or `ending`) is hidden for Refract (§4.2).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct HideReasons {
    /// State `ending`: its Refract purge runs or is due.
    pub ending: bool,
    /// Pending removal in a `refract_pending` scope (§4.6).
    pub pending: bool,
    /// Cleared from every pending scope, its first targeted label fetch not
    /// yet committed (§4.6 "Clearing").
    pub awaiting_labels: bool,
    /// A stored gone label on the account from its own community's labeler:
    /// `Some(None)` never expires, `Some(Some(t))` counts until `t`.
    pub gone_label: Option<Option<DateTime<Utc>>>,
}

impl HideReasons {
    /// Whether any reason counts at `now`.
    #[must_use]
    pub fn active(&self, now: DateTime<Utc>) -> bool {
        self.ending || self.pending || self.awaiting_labels || self.gone_label.is_some_and(|exp| exp.is_none_or(|t| t > now))
    }

    /// No reason at all: the entry can be dropped.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        !self.ending && !self.pending && !self.awaiting_labels && self.gone_label.is_none()
    }

    /// Add a gone label expiring at `exp` (`None`: never), keeping the
    /// latest expiry of all.
    pub fn add_gone(&mut self, exp: Option<DateTime<Utc>>) {
        self.gone_label = Some(match (self.gone_label, exp) {
            (None, e) => e,
            (Some(None), _) | (Some(Some(_)), None) => None,
            (Some(Some(a)), Some(b)) => Some(a.max(b)),
        });
    }
}

/// The latest of two record-label expiries (`None`: never).
fn later(a: Option<DateTime<Utc>>, b: Option<DateTime<Utc>>) -> Option<DateTime<Utc>> {
    match (a, b) {
        (Some(a), Some(b)) => Some(a.max(b)),
        _ => None,
    }
}

/// The stored Refract hide set (`ScopeSets.refract_hidden`).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RefractHidden {
    /// DIDs in R or `ending` with at least one reason. Members and
    /// non-members alike: the snapshot splits them by membership (the
    /// design's `dids` and `records_of`).
    pub accounts: HashMap<String, HideReasons>,
    /// at-URIs with a stored gone record label of a DID in R or `ending`,
    /// with the label's expiry (`None`: never).
    pub records: HashMap<String, Option<DateTime<Utc>>>,
}

impl RefractHidden {
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.accounts.is_empty() && self.records.is_empty()
    }

    /// Change `did`'s reasons; an entry left with none is dropped.
    pub fn update(&mut self, did: &str, f: impl FnOnce(&mut HideReasons)) {
        let entry = self.accounts.entry(did.to_string()).or_default();
        f(entry);
        if entry.is_empty() {
            self.accounts.remove(did);
        }
    }

    /// Add a record hide.
    pub fn add_record(&mut self, uri: &str, exp: Option<DateTime<Utc>>) {
        let merged = match self.records.get(uri) {
            Some(was) => later(*was, exp),
            None => exp,
        };
        self.records.insert(uri.to_string(), merged);
    }

    /// Drop every gone label of `did` (account and records), keeping its
    /// other reasons.
    pub fn clear_labels(&mut self, did: &str) {
        self.update(did, |r| r.gone_label = None);
        self.records.retain(|uri, _| uri_author(uri) != Some(did));
    }

    /// Drop everything of `did`.
    pub fn forget(&mut self, did: &str) {
        self.accounts.remove(did);
        self.records.retain(|uri, _| uri_author(uri) != Some(did));
    }

    /// Whether `did` has the `awaiting_labels` reason.
    #[must_use]
    pub fn awaiting(&self, did: &str) -> bool {
        self.accounts.get(did).is_some_and(|r| r.awaiting_labels)
    }

    /// Whether `did` has the `pending` reason.
    #[must_use]
    pub fn pending(&self, did: &str) -> bool {
        self.accounts.get(did).is_some_and(|r| r.pending)
    }
}

/// Which records of a DID in `by_collection` are hidden (§4.2).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CollectionRule {
    /// A non-member in S, in an off mode: every collection except its
    /// profile, the off self records and follows (kept by rule 1 and the
    /// off rule).
    AllButKept,
    /// A member: its posts only.
    PostsOnly,
}

impl CollectionRule {
    /// Whether this rule hides a record of `collection`.
    #[must_use]
    pub fn hides(self, collection: &str) -> bool {
        match self {
            Self::PostsOnly => collection == NSID_POST,
            Self::AllButKept => !(collection == NSID_PROFILE || collection == NSID_FOLLOW || OFF_SELF_RECORDS.contains(&collection)),
        }
    }
}

/// The collection of an at-URI.
#[must_use]
pub fn collection(uri: &str) -> Option<&str> {
    uri.strip_prefix("at://")?.split('/').nth(1)
}

/// The snapshot's outputs (§4.2 "The snapshot").
#[derive(Debug, Clone, Default)]
pub struct RefractSnapshot {
    /// Non-members not in S with an active reason, in every mode: hidden as
    /// an account.
    pub wholesale: HashSet<String>,
    /// In off modes only: non-members in S, and members, with an active
    /// reason, and the records each rule hides.
    pub by_collection: HashMap<String, CollectionRule>,
    /// Records with an active gone record label: all of them in off modes;
    /// in On and Filling only those whose author is neither a member nor in
    /// S.
    pub records: HashSet<String>,
    /// DIDs waiting for their first label fetch (§4.6): `fill` reports them
    /// in `pending`, and they never carry the marker.
    pub awaiting: HashSet<String>,
    /// R-live, DID to home community name, when the snapshot was built.
    pub restored: Arc<HashMap<String, String>>,
}

/// What a snapshot is computed from, copied under a short read guard so
/// the guard is never held for the computation (§4.2).
#[derive(Debug, Clone)]
pub struct Captured {
    hidden: Arc<RefractHidden>,
    restored: Arc<HashMap<String, String>>,
    off: bool,
    /// For each DID the stored set names (accounts and record authors):
    /// `(in S, member)`.
    bits: HashMap<String, (bool, bool)>,
}

impl Captured {
    /// Copy what the snapshot needs from the sets (under the caller's
    /// guard).
    #[must_use]
    pub fn of(sets: &ScopeSets) -> Self {
        let hidden = Arc::clone(&sets.refract_hidden);
        let mut bits = HashMap::new();
        let names = hidden.accounts.keys().map(String::as_str).chain(hidden.records.keys().filter_map(|u| uri_author(u)));
        for did in names {
            bits.entry(did.to_string()).or_insert_with(|| (sets.in_scope(did), sets.is_member(did)));
        }
        Self {
            hidden,
            restored: Arc::clone(&sets.restored),
            off: sets.public.mode.off_rule(),
            bits,
        }
    }

    /// The snapshot at `now`.
    #[must_use]
    pub fn compute(self, now: DateTime<Utc>) -> RefractSnapshot {
        let mut out = RefractSnapshot {
            restored: self.restored,
            ..RefractSnapshot::default()
        };
        for (did, reasons) in &self.hidden.accounts {
            if reasons.awaiting_labels {
                out.awaiting.insert(did.clone());
            }
            if !reasons.active(now) {
                continue;
            }
            let (in_s, member) = self.bits.get(did).copied().unwrap_or_default();
            if member {
                if self.off {
                    out.by_collection.insert(did.clone(), CollectionRule::PostsOnly);
                }
            } else if !in_s {
                out.wholesale.insert(did.clone());
            } else if self.off {
                out.by_collection.insert(did.clone(), CollectionRule::AllButKept);
            }
        }
        for (uri, exp) in &self.hidden.records {
            if exp.is_some_and(|t| t <= now) {
                continue;
            }
            let author = uri_author(uri).unwrap_or_default();
            let (in_s, member) = self.bits.get(author).copied().unwrap_or_default();
            if self.off || !(in_s || member) {
                out.records.insert(uri.clone());
            }
        }
        out
    }
}

impl RefractSnapshot {
    /// Build the snapshot of `sets` now (tests, and callers that hold no
    /// guard-sensitive state).
    #[must_use]
    pub fn of(sets: &ScopeSets) -> Self {
        Captured::of(sets).compute(Utc::now())
    }

    /// Whether `did` is in R-live by this snapshot.
    #[must_use]
    pub fn is_restored(&self, did: &str) -> bool {
        self.restored.contains_key(did)
    }
}

/// A request's serve-time label check (§4.5): the URIs it read from the
/// community AppView and found hidden or un-hidden.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RefractCheck {
    pub hidden: HashSet<String>,
    pub unhidden: HashSet<String>,
}

impl RefractCheck {
    /// Record a read: `hidden` true adds it to the hidden set.
    pub fn note(&mut self, uri: &str, hidden: bool) {
        if hidden {
            self.unhidden.remove(uri);
            self.hidden.insert(uri.to_string());
        } else {
            self.hidden.remove(uri);
            self.unhidden.insert(uri.to_string());
        }
    }
}

/// The pair the gate reads (§4.5, §7.4).
#[derive(Debug, Clone, Copy)]
pub struct RefractView<'a> {
    pub snapshot: &'a RefractSnapshot,
    pub check: &'a RefractCheck,
}

static EMPTY_SNAPSHOT: LazyLock<RefractSnapshot> = LazyLock::new(RefractSnapshot::default);
static EMPTY_CHECK: LazyLock<RefractCheck> = LazyLock::new(RefractCheck::default);

impl RefractView<'static> {
    /// Hides nothing: the private pipeline (Refract never hides a private
    /// post) and tests.
    #[must_use]
    pub fn empty() -> Self {
        Self {
            snapshot: &EMPTY_SNAPSHOT,
            check: &EMPTY_CHECK,
        }
    }
}

impl RefractView<'_> {
    /// Whether the account `did` is hidden as an account (`wholesale`).
    #[must_use]
    pub fn account_hidden(&self, did: &str) -> bool {
        self.snapshot.wholesale.contains(did)
    }

    /// Whether the record `uri` is hidden for Refract, its author's account
    /// aside: by its author's collection rule, or by a gone record label
    /// (`records` ∪ `check.hidden`, minus `check.unhidden`). A private-tier
    /// record never is.
    #[must_use]
    pub fn record_hidden(&self, uri: &str) -> bool {
        if !uri.starts_with("at://") || is_private_uri(uri) {
            return false;
        }
        let author = uri_author(uri).unwrap_or_default();
        if let (Some(rule), Some(collection)) = (self.snapshot.by_collection.get(author), collection(uri))
            && rule.hides(collection)
        {
            return true;
        }
        (self.snapshot.records.contains(uri) || self.check.hidden.contains(uri)) && !self.check.unhidden.contains(uri)
    }
}

#[cfg(test)]
mod tests {
    use chrono::Duration;

    use super::*;
    use crate::indexing::PublicMode;

    const S_NON_MEMBER: &str = "did:plc:snonmember";
    const OUTSIDER: &str = "did:plc:outsider";
    const MEMBER: &str = "did:plc:member";

    fn post(did: &str) -> String {
        format!("at://{did}/app.bsky.feed.post/3k")
    }

    fn sets(mode: PublicMode) -> ScopeSets {
        let mut s = ScopeSets::default();
        s.public.mode = mode;
        s.members.insert(MEMBER.into());
        s.scope.insert(MEMBER.into());
        s.scope.insert(S_NON_MEMBER.into());
        let mut hidden = RefractHidden::default();
        for did in [S_NON_MEMBER, OUTSIDER, MEMBER] {
            hidden.update(did, |r| r.ending = true);
        }
        s.refract_hidden = Arc::new(hidden);
        s
    }

    /// §4.2: the three outputs per mode, by membership and S.
    #[test]
    fn outputs_split_by_membership_s_and_mode() {
        for mode in [PublicMode::Off, PublicMode::Draining, PublicMode::Held] {
            let snap = RefractSnapshot::of(&sets(mode));
            assert_eq!(snap.wholesale, HashSet::from([OUTSIDER.to_string()]), "{mode:?}");
            assert_eq!(snap.by_collection.get(S_NON_MEMBER), Some(&CollectionRule::AllButKept));
            assert_eq!(snap.by_collection.get(MEMBER), Some(&CollectionRule::PostsOnly));
            let view = RefractView { snapshot: &snap, check: &RefractCheck::default() };
            assert!(view.record_hidden(&post(S_NON_MEMBER)));
            assert!(!view.record_hidden(&format!("at://{S_NON_MEMBER}/app.bsky.actor.profile/self")));
            assert!(!view.record_hidden(&format!("at://{S_NON_MEMBER}/app.bsky.graph.follow/3k")));
            assert!(!view.record_hidden(&format!("at://{S_NON_MEMBER}/{}/self", OFF_SELF_RECORDS[0])));
            assert!(view.record_hidden(&format!("at://{S_NON_MEMBER}/app.bsky.feed.like/3k")));
            assert!(view.record_hidden(&post(MEMBER)));
            assert!(!view.record_hidden(&format!("at://{MEMBER}/app.bsky.feed.like/3k")));
            assert!(!view.account_hidden(MEMBER) && !view.account_hidden(S_NON_MEMBER) && view.account_hidden(OUTSIDER));
        }
        for mode in [PublicMode::On, PublicMode::Filling] {
            let snap = RefractSnapshot::of(&sets(mode));
            assert_eq!(snap.wholesale, HashSet::from([OUTSIDER.to_string()]), "{mode:?}");
            assert!(snap.by_collection.is_empty(), "{mode:?}: rule 1 keeps everything an S DID or member has");
        }
    }

    /// §4.2: record labels per mode; expiry; the check's precedence.
    #[test]
    fn records_expiry_and_the_check() {
        let now = Utc::now();
        let mut s = sets(PublicMode::On);
        let mut hidden = RefractHidden::default();
        hidden.add_record(&post(OUTSIDER), None);
        hidden.add_record(&post(S_NON_MEMBER), None);
        hidden.add_record(&format!("at://{OUTSIDER}/app.bsky.feed.post/old"), Some(now - Duration::hours(1)));
        s.refract_hidden = Arc::new(hidden);
        let snap = Captured::of(&s).compute(now);
        assert_eq!(snap.records, HashSet::from([post(OUTSIDER)]), "On: only records of authors outside S and members; expired ones never");
        s.public.mode = PublicMode::Off;
        let snap = Captured::of(&s).compute(now);
        assert_eq!(snap.records.len(), 2);
        let mut check = RefractCheck::default();
        check.note(&post(OUTSIDER), false);
        check.note(&format!("at://{OUTSIDER}/app.bsky.feed.post/new"), true);
        let view = RefractView { snapshot: &snap, check: &check };
        assert!(!view.record_hidden(&post(OUTSIDER)), "un-hidden by the check");
        assert!(view.record_hidden(&format!("at://{OUTSIDER}/app.bsky.feed.post/new")), "hidden by the check");
        // The check never overrides a collection rule.
        let mut s = sets(PublicMode::Off);
        s.refract_hidden = Arc::new({
            let mut h = RefractHidden::default();
            h.update(MEMBER, |r| r.pending = true);
            h
        });
        let snap = RefractSnapshot::of(&s);
        let mut check = RefractCheck::default();
        check.note(&post(MEMBER), false);
        assert!(RefractView { snapshot: &snap, check: &check }.record_hidden(&post(MEMBER)));
    }

    /// Gone labels count until they expire; reasons merge.
    #[test]
    fn reasons() {
        let now = Utc::now();
        let mut r = HideReasons::default();
        assert!(r.is_empty() && !r.active(now));
        r.add_gone(Some(now - Duration::hours(1)));
        assert!(!r.active(now) && !r.is_empty());
        r.add_gone(Some(now + Duration::hours(1)));
        assert!(r.active(now));
        r.add_gone(None);
        assert_eq!(r.gone_label, Some(None));
        r.add_gone(Some(now));
        assert_eq!(r.gone_label, Some(None), "never expiring wins");
        let mut h = RefractHidden::default();
        h.update(OUTSIDER, |r| r.awaiting_labels = true);
        h.add_record(&post(OUTSIDER), None);
        assert!(h.awaiting(OUTSIDER));
        h.clear_labels(OUTSIDER);
        assert!(h.records.is_empty() && h.awaiting(OUTSIDER));
        h.update(OUTSIDER, |r| r.awaiting_labels = false);
        assert!(h.is_empty());
    }

    /// A private-tier record is never hidden for Refract, nor is anything
    /// by the empty view.
    #[test]
    fn tier_records_and_the_empty_view() {
        let snap = RefractSnapshot::of(&sets(PublicMode::Off));
        let view = RefractView { snapshot: &snap, check: &RefractCheck::default() };
        assert!(!view.record_hidden(&format!("at://{S_NON_MEMBER}/tools.kryphocron.feed.postPrivate/3k")));
        assert!(!RefractView::empty().record_hidden(&post(OUTSIDER)));
        assert!(!RefractView::empty().account_hidden(OUTSIDER));
    }
}
