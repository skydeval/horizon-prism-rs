//! The live feed's tap (admin-ui-live-feed design §4): what the writer
//! tells the admin UI's live feed about every op it applied.
//!
//! - [`FeedTap::observe`] runs once per `Writer::apply_from` call, after
//!   commit and outside the scope guard. It always counts each op in a
//!   fixed table of atomics (§6.3). Only while a stream is open, and only
//!   for a (bucket group, kind) some stream wants, does it build a [`Line`]
//!   and send it on a lossy broadcast channel. It never awaits, never
//!   blocks and never waits for a receiver.
//! - [`FeedOp`] is the writer's view of one op. Its one field that can carry
//!   record content is the public-post snippet, filled only by
//!   `writer::feed_view` (§6.1).
//!
//! This module depends on nothing in the admin UI's module; the admin UI's hub
//! (`src/admin/live/`) owns the stream side.

use std::num::NonZeroU32;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use chrono::{DateTime, Utc};
use governor::{DefaultDirectRateLimiter, Quota, RateLimiter};
use tokio::sync::broadcast;

use super::classifier::{EcosystemKind, RecordClass};
use super::stream::IdentityKind;
use super::writer::{Action, WriteSource};
use crate::aggregations::RecordKind;

/// Lines held for a slow receiver before it skips (§4.3).
pub const BROADCAST_CAPACITY: usize = 4096;
/// Lines per second per (bucket group, kind) (§4.3).
pub const BUCKET_PER_SEC: u32 = 100;
/// Characters of a snippet, the ellipsis included (§5.2).
pub const SNIPPET_CHARS: usize = 100;
/// Metric: lines not shown, by why.
pub const METRIC_NOT_SHOWN: &str = "admin_live_lines_not_shown_total";

/// What a line or a count is about (§5.2). The wire slugs use hyphens
/// (§6.2).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Kind {
    Post,
    Reply,
    Like,
    Repost,
    Follow,
    Block,
    Profile,
    List,
    ListItem,
    ListBlock,
    ListOptout,
    StarterPack,
    Labeler,
    Threadgate,
    Postgate,
    Poll,
    Setting,
    PrivatePost,
    Audience,
    PrivateBlock,
    PrivateGate,
    Identity,
    Account,
    Other,
}

/// The number of kinds.
pub const KINDS: usize = 24;

impl Kind {
    /// Every kind, in table order.
    pub const ALL: [Self; KINDS] = [
        Self::Post,
        Self::Reply,
        Self::Like,
        Self::Repost,
        Self::Follow,
        Self::Block,
        Self::Profile,
        Self::List,
        Self::ListItem,
        Self::ListBlock,
        Self::ListOptout,
        Self::StarterPack,
        Self::Labeler,
        Self::Threadgate,
        Self::Postgate,
        Self::Poll,
        Self::Setting,
        Self::PrivatePost,
        Self::Audience,
        Self::PrivateBlock,
        Self::PrivateGate,
        Self::Identity,
        Self::Account,
        Self::Other,
    ];

    #[must_use]
    pub fn index(self) -> usize {
        self as usize
    }

    /// The wire slug.
    #[must_use]
    pub fn slug(self) -> &'static str {
        match self {
            Self::Post => "post",
            Self::Reply => "reply",
            Self::Like => "like",
            Self::Repost => "repost",
            Self::Follow => "follow",
            Self::Block => "block",
            Self::Profile => "profile",
            Self::List => "list",
            Self::ListItem => "list-item",
            Self::ListBlock => "list-block",
            Self::ListOptout => "list-optout",
            Self::StarterPack => "starter-pack",
            Self::Labeler => "labeler",
            Self::Threadgate => "threadgate",
            Self::Postgate => "postgate",
            Self::Poll => "poll",
            Self::Setting => "setting",
            Self::PrivatePost => "private-post",
            Self::Audience => "audience",
            Self::PrivateBlock => "private-block",
            Self::PrivateGate => "private-post-gate",
            Self::Identity => "identity",
            Self::Account => "account",
            Self::Other => "other",
        }
    }

    /// The kind in words, as pages show it.
    #[must_use]
    pub fn words(self) -> &'static str {
        match self {
            Self::ListItem => "list item",
            Self::ListBlock => "list block",
            Self::ListOptout => "list opt-out",
            Self::StarterPack => "starter pack",
            Self::PrivatePost => "private post",
            Self::PrivateBlock => "private block",
            Self::PrivateGate => "private-post gate",
            other => other.slug(),
        }
    }

    /// The kind filter this kind answers to (§5.1): `post`, `reply`,
    /// `like`, `repost`, `follow`, `block`, `profile`, `list`, `private`,
    /// `identity` or `other`.
    #[must_use]
    pub fn filter(self) -> &'static str {
        match self {
            Self::Post => "post",
            Self::Reply => "reply",
            Self::Like => "like",
            Self::Repost => "repost",
            Self::Follow => "follow",
            Self::Block => "block",
            Self::Profile => "profile",
            Self::List | Self::ListItem | Self::ListBlock | Self::ListOptout => "list",
            Self::PrivatePost | Self::Audience | Self::PrivateBlock | Self::PrivateGate => "private",
            Self::Identity | Self::Account => "identity",
            Self::StarterPack | Self::Labeler | Self::Threadgate | Self::Postgate | Self::Poll | Self::Setting | Self::Other => "other",
        }
    }

    /// The kind of an admission kind. A public table's records are named
    /// by [`Kind::from_class`], so `Table` is `Other` here.
    #[must_use]
    pub fn from_record_kind(kind: RecordKind) -> Self {
        match kind {
            RecordKind::Post => Self::Post,
            RecordKind::Like => Self::Like,
            RecordKind::Repost => Self::Repost,
            RecordKind::Follow => Self::Follow,
            RecordKind::Block => Self::Block,
            RecordKind::Profile => Self::Profile,
            RecordKind::PrivatePost => Self::PrivatePost,
            RecordKind::PrivateAudience => Self::Audience,
            RecordKind::PrivateBlock => Self::PrivateBlock,
            RecordKind::PrivateGate => Self::PrivateGate,
            RecordKind::Table(_) => Self::Other,
        }
    }

    /// The kind of a classified collection.
    #[must_use]
    pub fn from_class(class: RecordClass) -> Self {
        match class {
            RecordClass::Ecosystem(k) => match k {
                EcosystemKind::Post => Self::Post,
                EcosystemKind::Like => Self::Like,
                EcosystemKind::Repost => Self::Repost,
                EcosystemKind::Follow => Self::Follow,
                EcosystemKind::Block => Self::Block,
                EcosystemKind::Profile => Self::Profile,
                EcosystemKind::List => Self::List,
                EcosystemKind::ListItem => Self::ListItem,
                EcosystemKind::ListBlock => Self::ListBlock,
                EcosystemKind::ListOptout => Self::ListOptout,
                EcosystemKind::StarterPack => Self::StarterPack,
                EcosystemKind::LabelerService => Self::Labeler,
                EcosystemKind::SelfRecord => Self::Setting,
                EcosystemKind::Threadgate => Self::Threadgate,
                EcosystemKind::Postgate => Self::Postgate,
                EcosystemKind::PollPost => Self::Poll,
            },
            class => class.record_kind().map_or(Self::Other, Self::from_record_kind),
        }
    }
}

/// An op's fate as the feed counts it (§6.3): written, reference only,
/// unchanged, failed, or one slot per skip and reject reason.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Outcome {
    Written,
    ReferenceOnly,
    Unchanged,
    Failed,
    Unindexed,
    KryphocronUnproduced,
    NotInScope,
    PublicOff,
    AccountDeleted,
    OutOfScope,
    MemberRemoved,
    /// A reason this table does not know; never expected.
    OtherReason,
}

/// The number of outcomes.
pub const OUTCOMES: usize = 12;

impl Outcome {
    /// Every outcome, in table order.
    pub const ALL: [Self; OUTCOMES] = [
        Self::Written,
        Self::ReferenceOnly,
        Self::Unchanged,
        Self::Failed,
        Self::Unindexed,
        Self::KryphocronUnproduced,
        Self::NotInScope,
        Self::PublicOff,
        Self::AccountDeleted,
        Self::OutOfScope,
        Self::MemberRemoved,
        Self::OtherReason,
    ];

    #[must_use]
    pub fn index(self) -> usize {
        self as usize
    }

    /// The slot of a skip or reject reason (`firehose_ops_skipped_total`'s
    /// `reason`); an unknown one is [`Outcome::OtherReason`], never a panic.
    #[must_use]
    pub fn from_reason(reason: &str) -> Self {
        match reason {
            "unindexed" => Self::Unindexed,
            "kryphocron_unproduced" => Self::KryphocronUnproduced,
            "not_in_scope" => Self::NotInScope,
            "public_off" => Self::PublicOff,
            "account_deleted" => Self::AccountDeleted,
            "out_of_scope" => Self::OutOfScope,
            "member_removed" => Self::MemberRemoved,
            _ => Self::OtherReason,
        }
    }

    #[must_use]
    pub fn slug(self) -> &'static str {
        match self {
            Self::Written => "written",
            Self::ReferenceOnly => "reference_only",
            Self::Unchanged => "unchanged",
            Self::Failed => "failed",
            Self::Unindexed => "unindexed",
            Self::KryphocronUnproduced => "kryphocron_unproduced",
            Self::NotInScope => "not_in_scope",
            Self::PublicOff => "public_off",
            Self::AccountDeleted => "account_deleted",
            Self::OutOfScope => "out_of_scope",
            Self::MemberRemoved => "member_removed",
            Self::OtherReason => "other",
        }
    }

    /// Indexed: written, or stored as a reference.
    #[must_use]
    pub fn indexed(self) -> bool {
        matches!(self, Self::Written | Self::ReferenceOnly)
    }

    /// Skipped or rejected, by a reason.
    #[must_use]
    pub fn is_reason(self) -> bool {
        !matches!(self, Self::Written | Self::ReferenceOnly | Self::Unchanged | Self::Failed)
    }
}

/// The write path a line came from (§5.2). Not the two bucket groups.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Source {
    Network,
    Backfill,
    Requested,
    CatchUp,
}

/// The number of sources.
pub const SOURCES: usize = 4;
/// The number of bucket groups: the network, and everything else (§4.3).
pub const GROUPS: usize = 2;

impl Source {
    pub const ALL: [Self; SOURCES] = [Self::Network, Self::Backfill, Self::Requested, Self::CatchUp];

    #[must_use]
    pub fn index(self) -> usize {
        self as usize
    }

    #[must_use]
    pub fn slug(self) -> &'static str {
        match self {
            Self::Network => "network",
            Self::Backfill => "backfill",
            Self::Requested => "requested",
            Self::CatchUp => "catch-up",
        }
    }

    /// The bucket group: 0 for the network, 1 for the rest.
    #[must_use]
    pub fn group(self) -> usize {
        usize::from(self != Self::Network)
    }
}

impl From<WriteSource> for Source {
    fn from(s: WriteSource) -> Self {
        match s {
            WriteSource::Firehose => Self::Network,
            WriteSource::Backfill => Self::Backfill,
            WriteSource::IndexRequest => Self::Requested,
            WriteSource::CatchUp => Self::CatchUp,
        }
    }
}

/// The `wanted` bit of a (bucket group, kind).
#[must_use]
pub fn wanted_bit(group: usize, kind: Kind) -> u64 {
    1u64 << (group * KINDS + kind.index())
}

/// Every (bucket group, kind).
pub const WANT_ALL: u64 = (1u64 << (GROUPS * KINDS)) - 1;

/// One op as the writer reports it to the tap. `text` is the snippet's
/// source: a written public post's `text`, and nothing else (§6.1).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FeedOp<'a> {
    pub kind: Kind,
    pub action: Action,
    pub outcome: Outcome,
    pub text: Option<&'a str>,
}

/// What a line says happened.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LineAction {
    Created,
    Updated,
    Deleted,
    /// A member's post stored as a reference under public-off.
    ReferenceOnly,
}

impl LineAction {
    #[must_use]
    pub fn words(self) -> &'static str {
        match self {
            Self::Created => "created",
            Self::Updated => "updated",
            Self::Deleted => "deleted",
            Self::ReferenceOnly => "reference only",
        }
    }
}

/// One line of the feed (§5.2). Only the author, never a subject.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Line {
    pub at: DateTime<Utc>,
    pub source: Source,
    /// The relay label of a network line.
    pub relay: Option<String>,
    pub action: LineAction,
    pub kind: Kind,
    /// The author.
    pub did: String,
    /// A written public post's cleaned first characters (C-F1).
    pub text: Option<String>,
}

/// A public post's text as a line shows it (§5.2): newlines and tabs as
/// spaces, control and bidirectional-override characters removed, at most
/// [`SNIPPET_CHARS`] characters with the ellipsis, cut on a character
/// boundary.
#[must_use]
pub fn snippet(text: &str) -> String {
    let cleaned = text.chars().filter_map(|c| match c {
        '\n' | '\r' | '\t' => Some(' '),
        '\u{202A}'..='\u{202E}' | '\u{2066}'..='\u{2069}' | '\u{200E}' | '\u{200F}' | '\u{061C}' => None,
        c if c.is_control() => None,
        c => Some(c),
    });
    let mut out = String::new();
    let mut n = 0;
    let mut chars = cleaned.peekable();
    while let Some(c) = chars.next() {
        if n == SNIPPET_CHARS - 1 && chars.peek().is_some() {
            out.push('…');
            break;
        }
        out.push(c);
        n += 1;
    }
    out
}

/// Counts read from the table at one moment.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Totals {
    /// `[source][kind][outcome]`, flattened.
    pub ops: Vec<u64>,
    /// Identity events: `[identity or account][received, queued]`.
    pub identity: [[u64; 2]; 2],
}

impl Default for Totals {
    fn default() -> Self {
        Self { ops: vec![0; SOURCES * KINDS * OUTCOMES], identity: [[0; 2]; 2] }
    }
}

fn slot(source: Source, kind: Kind, outcome: Outcome) -> usize {
    (source.index() * KINDS + kind.index()) * OUTCOMES + outcome.index()
}

impl Totals {
    /// One cell.
    #[must_use]
    pub fn get(&self, source: Source, kind: Kind, outcome: Outcome) -> u64 {
        self.ops[slot(source, kind, outcome)]
    }

    /// `self - earlier`, cell by cell.
    #[must_use]
    pub fn since(&self, earlier: &Self) -> Self {
        let ops = self.ops.iter().zip(&earlier.ops).map(|(a, b)| a.saturating_sub(*b)).collect();
        let mut identity = [[0; 2]; 2];
        for (i, row) in identity.iter_mut().enumerate() {
            for (j, cell) in row.iter_mut().enumerate() {
                *cell = self.identity[i][j].saturating_sub(earlier.identity[i][j]);
            }
        }
        Self { ops, identity }
    }

    /// Over every source and kind, for one outcome.
    #[must_use]
    pub fn outcome(&self, outcome: Outcome) -> u64 {
        Source::ALL
            .iter()
            .flat_map(|s| Kind::ALL.iter().map(move |k| self.get(*s, *k, outcome)))
            .sum()
    }

    /// Indexed (written or reference only) per kind, over every source.
    #[must_use]
    pub fn indexed_by_kind(&self) -> Vec<(Kind, u64)> {
        Kind::ALL
            .iter()
            .map(|k| {
                let n = Source::ALL
                    .iter()
                    .map(|s| self.get(*s, *k, Outcome::Written) + self.get(*s, *k, Outcome::ReferenceOnly))
                    .sum();
                (*k, n)
            })
            .collect()
    }

    /// Indexed per source, over every kind.
    #[must_use]
    pub fn indexed_by_source(&self) -> Vec<(Source, u64)> {
        Source::ALL
            .iter()
            .map(|s| {
                let n = Kind::ALL
                    .iter()
                    .map(|k| self.get(*s, *k, Outcome::Written) + self.get(*s, *k, Outcome::ReferenceOnly))
                    .sum();
                (*s, n)
            })
            .collect()
    }

    /// Skipped and rejected per reason.
    #[must_use]
    pub fn reasons(&self) -> Vec<(Outcome, u64)> {
        Outcome::ALL.iter().filter(|o| o.is_reason()).map(|o| (*o, self.outcome(*o))).collect()
    }

    /// Every indexed op.
    #[must_use]
    pub fn indexed(&self) -> u64 {
        self.outcome(Outcome::Written) + self.outcome(Outcome::ReferenceOnly)
    }

    /// Every skipped or rejected op.
    #[must_use]
    pub fn skipped(&self) -> u64 {
        self.reasons().iter().map(|(_, n)| n).sum()
    }

    /// Identity and account events received.
    #[must_use]
    pub fn identity_events(&self) -> u64 {
        self.identity[0][0] + self.identity[1][0]
    }
}

struct Inner {
    ops: Box<[AtomicU64]>,
    identity: [[AtomicU64; 2]; 2],
    /// A stream is open.
    active: AtomicBool,
    /// The union of the open streams' (bucket group, kind) bits.
    wanted: AtomicU64,
    /// One per (bucket group, kind), [`BUCKET_PER_SEC`] each.
    buckets: Vec<DefaultDirectRateLimiter>,
    sender: broadcast::Sender<Arc<Line>>,
    snippets: bool,
    started: DateTime<Utc>,
    #[cfg(test)]
    built: AtomicU64,
}

/// The tap. Cheap to clone; every clone shares one table and one channel.
#[derive(Clone)]
pub struct FeedTap(Arc<Inner>);

impl std::fmt::Debug for FeedTap {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FeedTap").field("snippets", &self.0.snippets).finish_non_exhaustive()
    }
}

impl FeedTap {
    /// A tap; `snippets` is `admin.live_feed_snippets` (C-F1).
    #[must_use]
    pub fn new(snippets: bool) -> Self {
        let per_sec = Quota::per_second(NonZeroU32::new(BUCKET_PER_SEC).unwrap_or(NonZeroU32::MIN));
        let (sender, _) = broadcast::channel(BROADCAST_CAPACITY);
        Self(Arc::new(Inner {
            ops: (0..SOURCES * KINDS * OUTCOMES).map(|_| AtomicU64::new(0)).collect(),
            identity: Default::default(),
            active: AtomicBool::new(false),
            wanted: AtomicU64::new(0),
            buckets: (0..GROUPS * KINDS).map(|_| RateLimiter::direct(per_sec)).collect(),
            sender,
            snippets,
            started: Utc::now(),
            #[cfg(test)]
            built: AtomicU64::new(0),
        }))
    }

    /// When this process's counting began.
    #[must_use]
    pub fn started(&self) -> DateTime<Utc> {
        self.0.started
    }

    /// Whether lines carry snippets.
    #[must_use]
    pub fn snippets(&self) -> bool {
        self.0.snippets
    }

    /// A receiver of the lines sent from now on.
    #[must_use]
    pub fn subscribe(&self) -> broadcast::Receiver<Arc<Line>> {
        self.0.sender.subscribe()
    }

    /// Set the open streams' union of wanted bits; `0` means no stream is
    /// open, and lines stop being built.
    pub fn set_wanted(&self, wanted: u64) {
        self.0.wanted.store(wanted, Ordering::Relaxed);
        self.0.active.store(wanted != 0, Ordering::Relaxed);
    }

    /// Lines built since start (tests: T-F1).
    #[cfg(test)]
    #[must_use]
    pub fn lines_built(&self) -> u64 {
        self.0.built.load(Ordering::Relaxed)
    }

    /// The broadcast's receivers now.
    #[must_use]
    pub fn receivers(&self) -> usize {
        self.0.sender.receiver_count()
    }

    /// The counters now.
    #[must_use]
    pub fn totals(&self) -> Totals {
        let mut identity = [[0; 2]; 2];
        for (i, row) in identity.iter_mut().enumerate() {
            for (j, cell) in row.iter_mut().enumerate() {
                *cell = self.0.identity[i][j].load(Ordering::Relaxed);
            }
        }
        Totals { ops: self.0.ops.iter().map(|a| a.load(Ordering::Relaxed)).collect(), identity }
    }

    /// One `apply_from` call's ops (§4.2, §4.3).
    pub fn observe<'a>(&self, ops: impl Iterator<Item = FeedOp<'a>>, source: WriteSource, relay: Option<&str>, repo: &str) {
        let inner = &*self.0;
        let source = Source::from(source);
        let active = inner.active.load(Ordering::Relaxed);
        let wanted = if active { inner.wanted.load(Ordering::Relaxed) } else { 0 };
        for op in ops {
            inner.ops[slot(source, op.kind, op.outcome)].fetch_add(1, Ordering::Relaxed);
            if !active {
                continue;
            }
            let action = match (op.outcome, op.action) {
                (Outcome::Written, Action::Create) => LineAction::Created,
                (Outcome::Written, Action::Update) => LineAction::Updated,
                (Outcome::Written, Action::Delete) => LineAction::Deleted,
                (Outcome::ReferenceOnly, _) => LineAction::ReferenceOnly,
                _ => continue,
            };
            let text = if inner.snippets && op.outcome == Outcome::Written { op.text } else { None };
            self.offer(source, op.kind, action, wanted, relay, repo, text);
        }
    }

    /// An identity, account, handle or tombstone event (§4.1): counted
    /// always; a line only when Prism knew the DID and queued a refresh.
    pub fn identity(&self, kind: IdentityKind, did: &str, queued: bool, relay: Option<&str>) {
        let inner = &*self.0;
        let (row, kind, action) = match kind {
            IdentityKind::Identity | IdentityKind::Handle => (0, Kind::Identity, LineAction::Updated),
            IdentityKind::Account => (1, Kind::Account, LineAction::Updated),
            IdentityKind::Tombstone => (1, Kind::Account, LineAction::Deleted),
        };
        inner.identity[row][0].fetch_add(1, Ordering::Relaxed);
        if !queued {
            return;
        }
        inner.identity[row][1].fetch_add(1, Ordering::Relaxed);
        if !inner.active.load(Ordering::Relaxed) {
            return;
        }
        let wanted = inner.wanted.load(Ordering::Relaxed);
        self.offer(Source::Network, kind, action, wanted, relay, did, None);
    }

    #[allow(clippy::too_many_arguments)]
    fn offer(&self, source: Source, kind: Kind, action: LineAction, wanted: u64, relay: Option<&str>, did: &str, text: Option<&str>) {
        let inner = &*self.0;
        if wanted & wanted_bit(source.group(), kind) == 0 {
            return;
        }
        if inner.buckets[source.group() * KINDS + kind.index()].check().is_err() {
            metrics::counter!(METRIC_NOT_SHOWN, "why" => "source_rate").increment(1);
            return;
        }
        #[cfg(test)]
        inner.built.fetch_add(1, Ordering::Relaxed);
        let line = Line {
            at: Utc::now(),
            source,
            relay: if source == Source::Network { relay.map(str::to_string) } else { None },
            action,
            kind,
            did: did.to_string(),
            text: text.map(snippet),
        };
        // No receiver is not an error: the line is dropped.
        let _ = inner.sender.send(Arc::new(line));
    }
}

/// Test-only ops, for the stream tests: the one constructor of a
/// [`FeedOp`] outside `writer.rs` (T-F10 allows it here, under
/// `cfg(test)`).
#[cfg(test)]
pub(crate) mod testing {
    use super::*;

    /// A written create of `kind`, with `text`.
    #[must_use]
    pub fn written(kind: Kind, text: Option<&str>) -> FeedOp<'_> {
        FeedOp { kind, action: Action::Create, outcome: Outcome::Written, text }
    }
}

#[cfg(test)]
mod tests {
    use std::time::{Duration, Instant};

    use super::testing::written;
    use super::*;
    use crate::aggregations::{PurgeKind, RejectReason};
    use crate::firehose::classifier::{
        NSID_AUDIENCE, NSID_BLOCK, NSID_POST, NSID_POST_PRIVATE, NSID_PRIVATE_BLOCK, classify, skip_reason,
    };

    #[test]
    fn snippets_are_cleaned_and_cut() {
        assert_eq!(snippet("a\nb\tc\u{202E}d\u{2066}e\u{7}f"), "a b cdef");
        let long = "é".repeat(150);
        let s = snippet(&long);
        assert_eq!(s.chars().count(), SNIPPET_CHARS);
        assert!(s.ends_with('…'));
        assert_eq!(snippet(&"x".repeat(100)).chars().count(), 100, "exactly 100 is not cut");
        assert!(!snippet(&"x".repeat(100)).ends_with('…'));
    }

    /// §6.3: every reject reason, skip reason and record kind has a slot;
    /// an unknown reason lands in "other".
    #[test]
    fn every_reason_and_kind_has_a_slot() {
        for r in [
            RejectReason::NotInScope,
            RejectReason::PublicOff,
            RejectReason::Purging(PurgeKind::AccountDeleted),
            RejectReason::Purging(PurgeKind::OutOfScope),
            RejectReason::Purging(PurgeKind::MemberRemoved),
        ] {
            assert_ne!(Outcome::from_reason(r.as_str()), Outcome::OtherReason, "{r:?}");
        }
        for (nsid, rkey) in [("app.bsky.feed.generator", "3k"), ("tools.kryphocron.feed.postPublic", "3k")] {
            let reason = skip_reason(classify(nsid, rkey)).expect("skipped");
            assert_ne!(Outcome::from_reason(reason), Outcome::OtherReason, "{reason}");
        }
        assert_eq!(Outcome::from_reason("something new"), Outcome::OtherReason);
        for k in [
            RecordKind::Post,
            RecordKind::Like,
            RecordKind::Repost,
            RecordKind::Follow,
            RecordKind::Block,
            RecordKind::Profile,
            RecordKind::PrivatePost,
            RecordKind::PrivateAudience,
            RecordKind::PrivateBlock,
            RecordKind::PrivateGate,
        ] {
            assert_ne!(Kind::from_record_kind(k), Kind::Other, "{k:?}");
        }
        for nsid in [
            "app.bsky.graph.list",
            "app.bsky.graph.listitem",
            "app.bsky.graph.listblock",
            "app.bsky.graph.referencelistoptout",
            "app.bsky.graph.starterpack",
            "app.bsky.feed.threadgate",
            "app.bsky.feed.postgate",
            "app.nearhorizon.poll.post",
        ] {
            assert_ne!(Kind::from_class(classify(nsid, "3k")), Kind::Other, "{nsid}");
        }
        for nsid in ["app.bsky.labeler.service", "app.bsky.actor.status", "app.bsky.actor.profile"] {
            assert_ne!(Kind::from_class(classify(nsid, "self")), Kind::Other, "{nsid}");
        }
        assert_eq!(Kind::from_class(classify(NSID_POST, "3k")), Kind::Post);
        assert_eq!(Kind::from_class(classify(NSID_BLOCK, "3k")), Kind::Block);
        assert_eq!(Kind::from_class(classify(NSID_POST_PRIVATE, "3k")), Kind::PrivatePost);
        assert_eq!(Kind::from_class(classify(NSID_AUDIENCE, "3k")), Kind::Audience);
        assert_eq!(Kind::from_class(classify(NSID_PRIVATE_BLOCK, "3k")), Kind::PrivateBlock);
        // Table order matches the indexes, and slugs are distinct.
        for (i, k) in Kind::ALL.iter().enumerate() {
            assert_eq!(k.index(), i);
        }
        for (i, o) in Outcome::ALL.iter().enumerate() {
            assert_eq!(o.index(), i);
        }
        let mut slugs: Vec<&str> = Kind::ALL.iter().map(|k| k.slug()).collect();
        slugs.sort_unstable();
        slugs.dedup();
        assert_eq!(slugs.len(), KINDS);
    }

    /// §6.2: no kind slug can match the image build's private-table check.
    #[test]
    fn slugs_stay_clear_of_the_private_table_check() {
        for k in Kind::ALL {
            assert!(!k.slug().contains('_'), "{}", k.slug());
        }
    }

    /// T-F2: `observe` is a plain function; a receiver that never reads
    /// costs nothing but its own lag. 100 000 calls from two tasks finish
    /// within a second, and the idle receiver's next read is `Lagged`.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn t_f2_a_slow_receiver_drops_and_never_blocks() {
        let tap = FeedTap::new(true);
        let mut idle = tap.subscribe();
        tap.set_wanted(WANT_ALL);
        let started = Instant::now();
        let tasks: Vec<_> = (0..2)
            .map(|t| {
                let tap = tap.clone();
                tokio::spawn(async move {
                    for i in 0..50_000 {
                        let kind = Kind::ALL[(i + t) % KINDS];
                        let source = if i % 2 == 0 { WriteSource::Firehose } else { WriteSource::Backfill };
                        tap.observe(std::iter::once(written(kind, Some("text"))), source, Some("bsky"), "did:plc:a");
                    }
                })
            })
            .collect();
        for t in tasks {
            t.await.expect("task");
        }
        assert!(started.elapsed() < Duration::from_secs(1), "{:?}", started.elapsed());
        let built = tap.lines_built();
        assert!(built > BROADCAST_CAPACITY as u64, "every bucket's burst at least: {built}");
        assert!(built <= (GROUPS * KINDS) as u64 * u64::from(BUCKET_PER_SEC) * 3, "the buckets bound the lines: {built}");
        assert!(matches!(idle.try_recv(), Err(broadcast::error::TryRecvError::Lagged(_))));
        assert_eq!(tap.totals().outcome(Outcome::Written), 100_000, "every op counted");
    }

    /// T-F1 (unit part): with no stream open, nothing is built or sent.
    #[test]
    fn nothing_is_built_without_a_stream() {
        let tap = FeedTap::new(true);
        for i in 0..10_000 {
            tap.observe(std::iter::once(written(Kind::ALL[i % KINDS], Some("x"))), WriteSource::Firehose, None, "did:plc:a");
        }
        assert_eq!(tap.lines_built(), 0);
        assert_eq!(tap.receivers(), 0);
        assert_eq!(tap.totals().indexed(), 10_000);
    }

    fn rs_files(dir: &std::path::Path, out: &mut Vec<std::path::PathBuf>) {
        for entry in std::fs::read_dir(dir).expect("dir") {
            let path = entry.expect("entry").path();
            if path.is_dir() {
                rs_files(&path, out);
            } else if path.extension().is_some_and(|e| e == "rs") {
                out.push(path);
            }
        }
    }

    /// T-F10: the tap sits below the admin UI, `FeedOp`'s one content field
    /// is the snippet, and only `writer.rs` builds a `FeedOp` (and this
    /// file's test helper).
    #[test]
    fn t_f10_layering() {
        let src = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
        let me = std::fs::read_to_string(src.join("firehose/feed.rs")).expect("feed.rs");
        assert!(!me.contains(&format!("crate::{}", "admin")) && !me.contains(&format!("super::super::{}", "admin")));
        // The struct's fields: one may borrow text, and it is `text`.
        let def = &me[me.find("pub struct FeedOp<'a> {").expect("definition")..];
        let def = &def[..def.find('}').expect("end")];
        let fields: Vec<(&str, &str)> = def
            .lines()
            .skip(1)
            .filter_map(|l| l.trim().strip_prefix("pub ")?.split_once(": "))
            .collect();
        assert_eq!(fields.iter().map(|(n, _)| *n).collect::<Vec<_>>(), ["kind", "action", "outcome", "text"]);
        assert!(fields.iter().all(|(n, t)| (*n == "text") == t.contains("str")), "{fields:?}");
        // Constructors.
        let literal = format!("{} {{", "FeedOp");
        let testing_at = me.find("pub(crate) mod testing {").expect("testing module");
        let mut files = Vec::new();
        rs_files(&src, &mut files);
        let mut writer_builds = 0;
        for file in files {
            let text = std::fs::read_to_string(&file).expect("read");
            for (at, _) in text.match_indices(&literal) {
                let name = file.strip_prefix(&src).expect("under src").to_string_lossy().replace('\\', "/");
                match name.as_str() {
                    "firehose/writer.rs" => writer_builds += 1,
                    "firehose/feed.rs" => assert!(at > testing_at, "feed.rs builds a FeedOp outside its test helper"),
                    other => panic!("{other} builds a FeedOp"),
                }
            }
        }
        assert_eq!(writer_builds, 1, "feed_view is the one constructor");
    }

    /// §4.3: an unwanted kind spends no token of a wanted one.
    #[test]
    fn unwanted_kinds_spend_no_tokens() {
        let tap = FeedTap::new(false);
        let mut rx = tap.subscribe();
        tap.set_wanted(wanted_bit(0, Kind::Block));
        for _ in 0..5_000 {
            tap.observe(std::iter::once(written(Kind::Like, None)), WriteSource::Firehose, None, "did:plc:a");
        }
        for _ in 0..20 {
            tap.observe(std::iter::once(written(Kind::Block, None)), WriteSource::Firehose, None, "did:plc:a");
        }
        // Another group's blocks are not wanted either.
        tap.observe(std::iter::once(written(Kind::Block, None)), WriteSource::Backfill, None, "did:plc:a");
        let mut got = 0;
        while let Ok(line) = rx.try_recv() {
            assert_eq!((line.kind, line.source), (Kind::Block, Source::Network));
            got += 1;
        }
        assert_eq!(got, 20);
    }
}
