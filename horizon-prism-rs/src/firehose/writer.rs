//! The per-record last-writer-wins write path (design-v2 §5.5), shared by the
//! firehose worker, backfill, `index.requestRecord` and `deadletter replay`.
//!
//! One call covers one stream entry (a commit, possibly many ops) or one
//! backfilled record, in one transaction, with the §6.11.7 lock order:
//! 1. the `ScopeState` read guard, before any pool connection;
//! 2. sorted shared DID locks on the author and on every growth subject;
//! 3. sorted per-URI locks on every URI written;
//! 4. row locks from statements and triggers.
//!
//! Each op runs in its own savepoint (§6.11.6). A deterministic failure is
//! retried in place, then reported as failed; a transient failure aborts the
//! whole call so the entry is retried. In-memory scope changes are applied
//! after commit.

use std::sync::Arc;

use chrono::{DateTime, Utc};
use serde_json::Value;
use sqlx::{Connection, PgConnection, PgPool};

use super::classifier::{EcosystemKind, NSID_LISTBLOCK, RecordClass, classify, skip_reason};
use super::jobs::{self, Depth};
use super::normalize::normalize_record;
use super::records::{GraphRecord, PostRecord, ProfileRecord, RecordError, SubjectRecord, private_subjects};
use super::rows::{self, Cell, Row};
use crate::aggregations::growth::{self, GrowthPlan};
use crate::aggregations::{Admission, AdmissionInput, RecordKind, ScopeSets, ScopeState, admit};
use crate::db::locks::{lock_dids_shared, lock_uris, uri_author};
use crate::hydration::normalize::is_private_uri;
use crate::private_tier::{IngestError, PrivateOp, PrivateTier, PublicNotify, QuoteRef};
use crate::subscriptions::applier::Applier;
use crate::subscriptions::copy;
use crate::subscriptions::fetch::{NSID_LIST, NSID_LISTITEM};
use crate::subscriptions::references::{Budget, rederive_block};

/// The lowest possible TID string: inbound-block rows lose to any real rev
/// under LWW (§5.10).
pub const SENTINEL_REV: &str = "2222222222222";
/// Metric: ops not written because of their class.
pub const METRIC_SKIPPED: &str = "firehose_ops_skipped_total";
/// Metric: LWW skips.
pub const METRIC_LWW_SKIPPED: &str = "lww_skipped";

/// A commit op's action.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Action {
    Create,
    Update,
    Delete,
}

impl Action {
    #[must_use]
    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "create" => Some(Self::Create),
            "update" => Some(Self::Update),
            "delete" => Some(Self::Delete),
            _ => None,
        }
    }

    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Create => "create",
            Self::Update => "update",
            Self::Delete => "delete",
        }
    }
}

/// One op as received.
#[derive(Debug, Clone, PartialEq)]
pub struct RawOp {
    pub action: Action,
    pub collection: String,
    pub rkey: String,
    pub cid: Option<String>,
    pub record: Option<Value>,
}

impl RawOp {
    #[must_use]
    pub fn path(&self) -> String {
        format!("{}/{}", self.collection, self.rkey)
    }
}

/// Per-call options.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ApplyOptions {
    /// Advance `sync_state.applied_rev` for a tracked repo (firehose worker
    /// only, §6.11.3).
    pub advance_applied_rev: bool,
    /// Failed ops of a tracked repo or a `tools.kryphocron.*` path clear the
    /// DID's readiness and enqueue `authz` in this transaction (§6.11.6).
    pub dead_letter_readiness: bool,
    /// In-place attempts for a deterministic failure (`firehose.deterministic_attempts`).
    pub attempts: u32,
    /// The commit comes from the firehose as it happens. Only such a write
    /// generates `subscribed-post` notifications (design-v3 §5.6): backfill,
    /// `requestRecord` and replay would announce history.
    pub live: bool,
    /// Run rule 4's owner-side rules (design-v3 §4.3): the copy rule and
    /// the purpose rule, for a repo that owns a subscribed list. The
    /// firehose, backfill and dead-letter replay set it; `requestRecord`
    /// does not, so what it does for a list owner is the same whether or
    /// not any member subscribes to one of the owner's lists.
    pub subscription_rules: bool,
    /// Which path writes. A `Backfill` write of a record created before
    /// the last fill began writes no notification (public-indexing design
    /// §8.3).
    pub source: WriteSource,
}

/// The write path an [`ApplyOptions`] belongs to.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WriteSource {
    /// The firehose worker.
    Firehose,
    /// `index.requestRecord` and dead-letter replay.
    IndexRequest,
    /// The backfill worker.
    Backfill,
    /// `requestRecord`'s catch-up listing (private posting §3.8). Not live:
    /// private notifications follow the 48 h rule (§4.8).
    CatchUp,
}

/// What happened to one op.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OpOutcome {
    Written,
    /// LWW kept the existing state (older or equal rev, or tombstoned).
    Unchanged,
    /// Under the off rule, a member's post: its reference and its scope
    /// growth were written, and no `posts` row (public-indexing design
    /// §4.1).
    GrowOnly,
    Rejected(&'static str),
    Skipped(&'static str),
    Failed(String),
}

/// An op that failed deterministically after its attempts.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FailedOp {
    pub index: usize,
    pub action: Action,
    pub path: String,
    pub error: String,
}

/// The result of one call.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ApplyReport {
    pub outcomes: Vec<OpOutcome>,
    pub failed: Vec<FailedOp>,
    /// Readiness was cleared and `authz` enqueued (§6.11.6).
    pub readiness_cleared: bool,
    /// The call opened a transaction: false for a commit with nothing to
    /// write (§5.5 "Nothing to write").
    pub transaction: bool,
}

/// A failure that must abort the call and retry it later (§5.3 transient).
#[derive(Debug, thiserror::Error)]
#[error("transient write failure: {0}")]
pub struct Transient(pub String);

impl From<sqlx::Error> for Transient {
    fn from(e: sqlx::Error) -> Self {
        Self(e.to_string())
    }
}

/// An op-level failure.
#[derive(Debug)]
enum OpError {
    Transient(sqlx::Error),
    Deterministic(String),
}

impl From<sqlx::Error> for OpError {
    fn from(e: sqlx::Error) -> Self {
        if is_deterministic(&e) {
            Self::Deterministic(e.to_string())
        } else {
            Self::Transient(e)
        }
    }
}

/// A malformed private record is deterministic (dead-lettered, #235).
impl From<IngestError> for OpError {
    fn from(e: IngestError) -> Self {
        match e {
            IngestError::Db(e) => e.into(),
            IngestError::Malformed(msg) => Self::Deterministic(msg),
        }
    }
}

/// SQLSTATE classes 22 (data exception) and 23 (integrity) are deterministic
/// (§5.3); everything else, including connection loss and 40001, 40P01,
/// 55P03, 57014 and 53200, is transient.
fn is_deterministic(e: &sqlx::Error) -> bool {
    match e {
        sqlx::Error::Database(db) => db
            .code()
            .is_some_and(|c| c.starts_with("22") || c.starts_with("23")),
        _ => false,
    }
}

/// The typed body of an upsert.
#[derive(Debug, Clone, PartialEq)]
enum Body {
    Post(Box<PostRecord>),
    Like(SubjectRecord),
    Repost(SubjectRecord),
    Follow(GraphRecord),
    Block(GraphRecord),
    Profile(ProfileRecord),
    /// A record of one of the tables design-v3 added (§3.2).
    Row(Row),
    /// A private-tier record; the facade parses it.
    Private,
    /// A threadgate or postgate naming a `postPrivate` URI (private posting
    /// §4.4): the private tier stores it, in every public-indexing mode.
    PrivateGate,
}

/// The op a savepoint attempt runs.
#[derive(Clone, Copy)]
enum OpTarget<'a> {
    Upsert(&'a Upsert, &'a GrowthPlan),
    /// A member's post under the off rule (public-indexing design §4.1).
    GrowOnly(&'a Upsert, &'a GrowthPlan),
    Delete(&'a Delete),
    /// A follow of a tracked repo that admission rejected, with the reason
    /// (following-audience design §4.2): the old row goes, and a tombstone
    /// at the op's rev refuses any older write of it.
    RejectedFollow(&'a Upsert, &'static str),
}

#[derive(Debug, Clone, PartialEq)]
struct Upsert {
    uri: String,
    rkey: String,
    cid: String,
    collection: String,
    body: Body,
    input: AdmissionInput,
    record: Value,
    /// The op created the record (not an update).
    created: bool,
    /// The record's `createdAt` as `records::created_at` parses it.
    created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq)]
struct Delete {
    uri: String,
    kind: RecordKind,
}

#[derive(Debug, Clone, PartialEq)]
enum Prepared {
    Skip(&'static str),
    Invalid(String),
    Upsert(Box<Upsert>),
    Delete(Delete),
}

impl Prepared {
    fn uri(&self) -> Option<&str> {
        match self {
            Self::Upsert(u) => Some(&u.uri),
            Self::Delete(d) => Some(&d.uri),
            Self::Skip(_) | Self::Invalid(_) => None,
        }
    }
}

fn prepare(repo: &str, op: &RawOp, now: DateTime<Utc>) -> Prepared {
    let class = classify(&op.collection, &op.rkey);
    if let Some(reason) = skip_reason(class) {
        return Prepared::Skip(reason);
    }
    let Some(kind) = class.record_kind() else {
        return Prepared::Skip("unindexed");
    };
    if !proto_blue::syntax::RecordKey::is_valid(&op.rkey) {
        return Prepared::Invalid(format!("invalid rkey {:?}", op.rkey));
    }
    let uri = format!("at://{repo}/{}/{}", op.collection, op.rkey);
    if op.action == Action::Delete {
        return Prepared::Delete(Delete { uri, kind });
    }
    let (Some(record), Some(cid)) = (&op.record, &op.cid) else {
        return Prepared::Invalid("create/update without record or cid".to_string());
    };
    match build_upsert(repo, class, kind, uri, op, cid, record, now) {
        Ok(Some(u)) => Prepared::Upsert(Box::new(u)),
        // A listitem naming a list outside its own repo (§3.2).
        Ok(None) => Prepared::Skip("unindexed"),
        Err(e) => Prepared::Invalid(e.0),
    }
}

/// One op's outcome when it runs no SQL, or `None` when it needs the
/// transaction. A skip or a rejection bumps `firehose_ops_skipped_total`
/// here, exactly once. A rejected follow by a tracked repo is counted and
/// still returns `None`: it takes the rejected-follow path (following-audience
/// design §4.2).
fn outcome_without_sql(p: &Prepared, a: Option<Admission>, tracked: bool) -> Option<OpOutcome> {
    match (p, a) {
        (Prepared::Skip(reason), _) => {
            metrics::counter!(METRIC_SKIPPED, "reason" => *reason).increment(1);
            Some(OpOutcome::Skipped(reason))
        }
        (Prepared::Invalid(e), _) => Some(OpOutcome::Failed(e.clone())),
        (Prepared::Upsert(u), Some(Admission::Reject(reason))) => {
            metrics::counter!(METRIC_SKIPPED, "reason" => reason.as_str()).increment(1);
            if matches!(u.body, Body::Follow(_)) && tracked {
                None
            } else {
                Some(OpOutcome::Rejected(reason.as_str()))
            }
        }
        _ => None,
    }
}

/// §5.5 "Nothing to write" (firehose throughput design §1.2): the repo is
/// not tracked and owns no subscribed list under rule 4, every op is
/// skipped, invalid or rejected, and no invalid op has a `tools.kryphocron.*`
/// path that would clear readiness. Such a transaction would run nothing
/// but its locks.
fn nothing_to_write(
    prepared: &[Prepared],
    admissions: &[Option<Admission>],
    ops: &[RawOp],
    owner_side: bool,
    tracked: bool,
    options: ApplyOptions,
) -> bool {
    let clears_readiness = options.dead_letter_readiness
        && prepared
            .iter()
            .zip(ops)
            .any(|(p, op)| matches!(p, Prepared::Invalid(_)) && op.path().starts_with("tools.kryphocron."));
    !owner_side
        && !tracked
        && !clears_readiness
        && prepared.iter().zip(admissions).all(|(p, a)| match p {
            Prepared::Skip(_) | Prepared::Invalid(_) => true,
            Prepared::Upsert(_) => matches!(a, Some(Admission::Reject(_))),
            Prepared::Delete(_) => false,
        })
}

/// The report the transaction would have returned for a commit with nothing
/// to write: the same outcomes, failed ops and skip counters.
fn report_without_writes(prepared: &[Prepared], admissions: &[Option<Admission>], ops: &[RawOp]) -> ApplyReport {
    let mut report = ApplyReport::default();
    for (i, (p, op)) in prepared.iter().zip(ops).enumerate() {
        // An untracked repo: every op is decided here.
        let outcome = outcome_without_sql(p, admissions[i], false).unwrap_or_else(|| unreachable!("nothing to write"));
        if let OpOutcome::Failed(error) = &outcome {
            report.failed.push(FailedOp {
                index: i,
                action: op.action,
                path: op.path(),
                error: error.clone(),
            });
        }
        report.outcomes.push(outcome);
    }
    report
}

/// The author of the `postPrivate` post a gate row names (private posting
/// §4.4): the gate's one admission subject. `None` for any other row,
/// including a gate on a public post.
fn private_gate_subject(row: &Row) -> Option<String> {
    if !matches!(row.table, "threadgates" | "postgates") {
        return None;
    }
    let post = row.cells.iter().find_map(|(name, cell)| match (name, cell) {
        (&"post_uri", Cell::Text(Some(uri))) => Some(uri.as_str()),
        _ => None,
    })?;
    if post.split('/').nth(3) != Some(super::classifier::NSID_POST_PRIVATE) {
        return None;
    }
    uri_author(post).map(str::to_string)
}

#[allow(clippy::too_many_arguments)]
fn build_upsert(
    repo: &str,
    class: RecordClass,
    mut kind: RecordKind,
    uri: String,
    op: &RawOp,
    cid: &str,
    record: &Value,
    now: DateTime<Utc>,
) -> Result<Option<Upsert>, RecordError> {
    let normalized = normalize_record(record);
    let (body, subjects) = match class {
        RecordClass::Ecosystem(EcosystemKind::Post) => {
            let p = PostRecord::parse(record, now)?;
            let mut subjects = Vec::new();
            for u in [
                p.reply_parent.as_ref().map(|r| r.uri.as_str()),
                p.reply_root.as_ref().map(|r| r.uri.as_str()),
                p.quote_uri.as_deref(),
            ]
            .into_iter()
            .flatten()
            {
                if let Some(a) = uri_author(u)
                    && !subjects.iter().any(|s: &String| s == a)
                {
                    subjects.push(a.to_string());
                }
            }
            (Body::Post(Box::new(p)), subjects)
        }
        RecordClass::Ecosystem(EcosystemKind::Like) => {
            let s = SubjectRecord::parse(record, now)?;
            let subjects = uri_author(&s.subject.uri).map(str::to_string).into_iter().collect();
            (Body::Like(s), subjects)
        }
        RecordClass::Ecosystem(EcosystemKind::Repost) => {
            let s = SubjectRecord::parse(record, now)?;
            let subjects = uri_author(&s.subject.uri).map(str::to_string).into_iter().collect();
            (Body::Repost(s), subjects)
        }
        RecordClass::Ecosystem(EcosystemKind::Follow) => {
            let g = GraphRecord::parse(record, now)?;
            let subjects = vec![g.subject_did.clone()];
            (Body::Follow(g), subjects)
        }
        RecordClass::Ecosystem(EcosystemKind::Block) => {
            let g = GraphRecord::parse(record, now)?;
            let subjects = vec![g.subject_did.clone()];
            (Body::Block(g), subjects)
        }
        RecordClass::Ecosystem(EcosystemKind::Profile) => (Body::Profile(ProfileRecord::parse(record, now)), Vec::new()),
        // The v3 record tables: rule 1 only, so no subjects.
        RecordClass::Ecosystem(other) => match rows::row(other, repo, &op.collection, record, &normalized, now)? {
            // A gate on a private post goes to the private tier, admitted
            // with the post's author as its one subject (rule 3, §4.4).
            Some(row) => match private_gate_subject(&row) {
                Some(author) => {
                    kind = RecordKind::PrivateGate;
                    (Body::PrivateGate, vec![author])
                }
                None => (Body::Row(row), Vec::new()),
            },
            None => return Ok(None),
        },
        RecordClass::Kryphocron { kind: Some(k), .. } => (Body::Private, private_subjects(k, record)?),
        RecordClass::Kryphocron { kind: None, .. } | RecordClass::Unindexed => {
            return Err(RecordError("not an indexed class".to_string()));
        }
    };
    Ok(Some(Upsert {
        uri,
        rkey: op.rkey.clone(),
        cid: cid.to_string(),
        collection: op.collection.clone(),
        body,
        created: op.action == Action::Create,
        created_at: super::records::created_at(record, now),
        input: AdmissionInput {
            author: repo.to_string(),
            kind,
            subjects,
        },
        record: normalized,
    }))
}

/// The canonical gates of a post: the threadgate and postgate in the post
/// author's repo with the post's rkey (design-v3 §3.2), for a public or a
/// `postPrivate` post (private posting §4.4). Empty when `post_uri` is
/// neither.
fn canonical_gates(post_uri: &str) -> Vec<String> {
    let Some((prefix, rkey)) = post_uri.rsplit_once('/') else {
        return Vec::new();
    };
    let Some(repo) = [super::classifier::NSID_POST, super::classifier::NSID_POST_PRIVATE]
        .into_iter()
        .find_map(|nsid| prefix.strip_suffix(nsid).and_then(|p| p.strip_suffix('/')))
    else {
        return Vec::new();
    };
    [super::classifier::NSID_THREADGATE, super::classifier::NSID_POSTGATE]
        .iter()
        .map(|nsid| format!("{repo}/{nsid}/{rkey}"))
        .collect()
}

/// Changes to apply to `ScopeState` after commit.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
struct MemoryEffects {
    growth_added: Vec<String>,
    new_member_follow: Option<String>,
    member_follow_removed: Option<String>,
    owners_joined: Vec<String>,
    owners_left: Vec<String>,
    /// A member's list-block references were derived again.
    list_references: bool,
}

impl MemoryEffects {
    fn merge(&mut self, other: Self) {
        self.list_references |= other.list_references;
        self.growth_added.extend(other.growth_added);
        if other.new_member_follow.is_some() {
            self.new_member_follow = other.new_member_follow;
        }
        if other.member_follow_removed.is_some() {
            self.member_follow_removed = other.member_follow_removed;
        }
        self.owners_joined.extend(other.owners_joined);
        self.owners_left.extend(other.owners_left);
    }
}

/// A notification to insert (§7.7).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Notification {
    pub recipient: String,
    pub reason: &'static str,
    pub reason_subject: Option<String>,
}

/// Notifications for an admitted public record written by `author` (§7.7):
/// recipients are members other than the author, one reason per recipient,
/// with reply taking precedence over quote, and quote over mention. A
/// subject that is a private post is skipped: such a record notifies only
/// in the private tier (private posting §4.8, #717).
fn notifications_for(author: &str, body: &Body, sets: &ScopeSets) -> Vec<Notification> {
    let mut out: Vec<Notification> = Vec::new();
    let mut push = |recipient: Option<&str>, reason: &'static str, subject: Option<String>| {
        if let Some(r) = recipient
            && !subject.as_deref().is_some_and(is_private_uri)
            && r != author
            && sets.is_member(r)
            && !out.iter().any(|n| n.recipient == r)
        {
            out.push(Notification {
                recipient: r.to_string(),
                reason,
                reason_subject: subject,
            });
        }
    };
    match body {
        Body::Like(s) => push(uri_author(&s.subject.uri), "like", Some(s.subject.uri.clone())),
        Body::Repost(s) => push(uri_author(&s.subject.uri), "repost", Some(s.subject.uri.clone())),
        Body::Follow(g) => push(Some(&g.subject_did), "follow", None),
        Body::Post(p) => {
            if let Some(parent) = &p.reply_parent {
                push(uri_author(&parent.uri), "reply", Some(parent.uri.clone()));
            }
            if let Some(root) = &p.reply_root {
                push(uri_author(&root.uri), "reply", Some(root.uri.clone()));
            }
            if let Some(q) = &p.quote_uri {
                push(uri_author(q), "quote", Some(q.clone()));
            }
            for m in &p.mention_dids {
                push(Some(m), "mention", None);
            }
        }
        Body::Block(_) | Body::Profile(_) | Body::Row(_) | Body::Private | Body::PrivateGate => {}
    }
    out
}

/// The writer.
#[derive(Clone)]
pub struct Writer {
    pool: PgPool,
    scope: ScopeState,
    private: Arc<dyn PrivateTier>,
    /// `notifications.subscribed_post_max_age_secs`.
    subscribed_post_max_age: chrono::Duration,
    /// `private_notify.backfill_max_age_hours` (private posting §4.8, C-11).
    private_notify_max_age: chrono::Duration,
    /// Rule 4's caps (`lists.subscribed_max_items`,
    /// `lists.subscription_tombstones_max_per_owner`).
    subscription_limits: copy::Limits,
    /// A member's budget of list blocks (`lists.subscribed_max_per_member`,
    /// `lists.max_owners_per_member`).
    list_budget: Budget,
    /// Told after a commit that changed a member's list-block references,
    /// so the list phase follows at once rather than at its next poll.
    applier: Option<Applier>,
}

impl Writer {
    #[must_use]
    pub fn new(pool: PgPool, scope: ScopeState, private: Arc<dyn PrivateTier>) -> Self {
        Self {
            pool,
            scope,
            private,
            subscribed_post_max_age: chrono::Duration::seconds(86_400),
            private_notify_max_age: chrono::Duration::hours(48),
            subscription_limits: copy::Limits { max_items: 50_000, tombstones_max_per_owner: 10_000 },
            list_budget: Budget { max_references: 25, max_owners: 10 },
            applier: None,
        }
    }

    /// Set a member's list-block budget, and the applier to tell when a
    /// member's references change.
    #[must_use]
    pub fn with_list_references(mut self, budget: Budget, applier: Applier) -> Self {
        self.list_budget = budget;
        self.applier = Some(applier);
        self
    }

    /// Set rule 4's caps.
    #[must_use]
    pub fn with_subscription_limits(mut self, limits: copy::Limits) -> Self {
        self.subscription_limits = limits;
        self
    }

    /// How old a post may be and still be announced to its author's
    /// subscribers (`notifications.subscribed_post_max_age_secs`).
    #[must_use]
    pub fn with_subscribed_post_max_age_secs(mut self, secs: u32) -> Self {
        self.subscribed_post_max_age = chrono::Duration::seconds(i64::from(secs));
        self
    }

    /// How old a record written by any path but the live firehose may be
    /// and still notify privately (`private_notify.backfill_max_age_hours`,
    /// private posting §4.8, C-11).
    #[must_use]
    pub fn with_private_notify_max_age_hours(mut self, hours: u32) -> Self {
        self.private_notify_max_age = chrono::Duration::hours(i64::from(hours));
        self
    }

    #[must_use]
    pub fn pool(&self) -> &PgPool {
        &self.pool
    }

    #[must_use]
    pub fn scope(&self) -> &ScopeState {
        &self.scope
    }

    #[must_use]
    pub fn private(&self) -> &Arc<dyn PrivateTier> {
        &self.private
    }

    /// Apply one commit's ops for `repo` at `rev` in one transaction.
    ///
    /// # Errors
    ///
    /// [`Transient`] when the call must be retried whole.
    pub async fn apply(
        &self,
        repo: &str,
        rev: &str,
        ops: &[RawOp],
        options: ApplyOptions,
    ) -> Result<ApplyReport, Transient> {
        // Firehose throughput design §2.4: a hung database, before the
        // nothing-to-write check so it holds every entry alike.
        #[cfg(test)]
        if let Some(db) = self.pool.connect_options().get_database()
            && crate::test_support::hooks::delay(crate::test_support::hooks::WRITER_HANG, db).await
        {
            return Err(Transient("injected hang".into()));
        }
        let now = Utc::now();
        let prepared: Vec<Prepared> = ops.iter().map(|op| prepare(repo, op, now)).collect();

        // 1. Read guard before any connection.
        let guard = self.scope.read_guard().await;
        let sets: &ScopeSets = &guard;

        // Admission and growth plans from the guarded sets.
        let mut admissions: Vec<Option<Admission>> = Vec::with_capacity(prepared.len());
        let mut plans: Vec<GrowthPlan> = Vec::with_capacity(prepared.len());
        let mut lock_dids: Vec<String> = vec![repo.to_string()];
        for p in &prepared {
            if let Prepared::Upsert(u) = p {
                let a = admit(&u.input, sets);
                // A grow-only post grows scope as an admitted one does, so
                // its subjects' shared locks join the same sorted pass.
                let plan = if matches!(a, Admission::Admit(_) | Admission::GrowOnly) {
                    growth::plan(
                        &u.input,
                        sets,
                        self.scope.policy(),
                        self.scope.max_follow_scope_per_member(),
                    )
                } else {
                    GrowthPlan::default()
                };
                lock_dids.extend(plan.lock_dids());
                admissions.push(Some(a));
                plans.push(plan);
            } else {
                admissions.push(None);
                plans.push(GrowthPlan::default());
            }
        }
        let mut lock_uri_list: Vec<String> = prepared.iter().filter_map(|p| p.uri().map(str::to_string)).collect();
        // A post delete also deletes the post's canonical gates (design-v3
        // §14.1), so their URIs are locked with it, in the same sorted pass.
        // A private post's delete deletes its private gates (private posting
        // §4.4).
        for p in &prepared {
            if let Prepared::Delete(d) = p
                && matches!(d.kind, RecordKind::Post | RecordKind::PrivatePost)
            {
                lock_uri_list.extend(canonical_gates(&d.uri));
            }
        }
        // Rule 4 (§4.3): the owner-side rules run for a repo that owns a
        // subscribed list. The copy rule writes by list-item URI whatever
        // became of the op itself, so those URIs are locked too.
        let owner_side = options.subscription_rules && sets.subscribed.tracks(repo);
        if owner_side {
            let items = ops.iter().filter(|op| op.collection == NSID_LISTITEM && proto_blue::syntax::RecordKey::is_valid(&op.rkey));
            lock_uri_list.extend(items.map(|op| format!("at://{repo}/{NSID_LISTITEM}/{}", op.rkey)));
        }

        // §5.5 "Nothing to write": a commit whose transaction would run no
        // statement besides its locks opens none.
        let tracked = sets.is_tracked(repo);
        #[cfg(test)]
        let forced = crate::test_support::hooks::injected(crate::test_support::hooks::WRITER_FORCE_TX, repo)
            || crate::test_support::hooks::injected(crate::test_support::hooks::WRITER_FORCE_TX, "*");
        #[cfg(not(test))]
        let forced = false;
        if !forced && nothing_to_write(&prepared, &admissions, ops, owner_side, tracked, options) {
            let report = report_without_writes(&prepared, &admissions, ops);
            drop(guard);
            return Ok(report);
        }

        let mut tx = self.pool.begin().await?;
        // 2 and 3: DID locks, then per-URI locks.
        lock_dids_shared(&mut tx, &lock_dids).await?;
        lock_uris(&mut tx, &lock_uri_list).await?;

        let mut report = ApplyReport { transaction: true, ..ApplyReport::default() };
        let mut effects = MemoryEffects::default();
        let mut copied = copy::Effects::default();
        for (i, (p, op)) in prepared.iter().zip(ops).enumerate() {
            if owner_side {
                self.owner_side(&mut tx, repo, rev, op, sets, &mut copied).await?;
            }
            let decided = outcome_without_sql(p, admissions[i], tracked);
            let outcome = match (p, decided) {
                (_, Some(outcome)) => outcome,
                (Prepared::Skip(_) | Prepared::Invalid(_), None) => unreachable!("decided without SQL"),
                (Prepared::Upsert(u), None) => match admissions[i] {
                    // `outcome_without_sql` left it here: a follow of a
                    // tracked repo, from any source. An unfollow written as
                    // an edit must not leave the old subject stored
                    // (following-audience design §4.2). Already counted.
                    Some(Admission::Reject(reason)) => {
                        let target = OpTarget::RejectedFollow(u, reason.as_str());
                        self.run_with_attempts(&mut tx, options, target, repo, rev, sets, &mut effects)
                            .await?
                    }
                    Some(Admission::GrowOnly) => {
                        let target = OpTarget::GrowOnly(u, &plans[i]);
                        self.run_with_attempts(&mut tx, options, target, repo, rev, sets, &mut effects)
                            .await?
                    }
                    // Reached by `Admit` only.
                    _ => {
                        let target = OpTarget::Upsert(u, &plans[i]);
                        self.run_with_attempts(&mut tx, options, target, repo, rev, sets, &mut effects)
                            .await?
                    }
                },
                (Prepared::Delete(d), None) => {
                    self.run_with_attempts(&mut tx, options, OpTarget::Delete(d), repo, rev, sets, &mut effects)
                        .await?
                }
            };
            if let OpOutcome::Failed(error) = &outcome {
                report.failed.push(FailedOp {
                    index: i,
                    action: op.action,
                    path: op.path(),
                    error: error.clone(),
                });
            }
            report.outcomes.push(outcome);
        }

        // §6.11.6: failed ops of a tracked repo or a kryphocron path.
        if options.dead_letter_readiness
            && !report.failed.is_empty()
            && (sets.is_tracked(repo)
                || report.failed.iter().any(|f| f.path.starts_with("tools.kryphocron.")))
        {
            clear_readiness(&mut tx, self.private.as_ref(), repo).await?;
            report.readiness_cleared = true;
        }
        // §6.11.3: the worker advances applied_rev for a tracked repo once
        // every op committed or failed, whatever the ops were.
        if options.advance_applied_rev && sets.is_tracked(repo) {
            advance_applied_rev(&mut tx, repo, rev, true).await?;
        }
        tx.commit().await?;
        drop(guard);
        self.commit_effects(effects).await;
        self.commit_copied(copied).await;
        Ok(report)
    }

    /// Rule 4's owner-side rules for one op of a repo in `O_sub` (design-v3
    /// §4.3): the purpose rule for a list record, the copy rule for a list
    /// item. They run in a savepoint of their own and never change the
    /// op's outcome: a deterministic database error rolls back that
    /// savepoint only and is counted.
    async fn owner_side(
        &self,
        tx: &mut PgConnection,
        repo: &str,
        rev: &str,
        op: &RawOp,
        sets: &ScopeSets,
        copied: &mut copy::Effects,
    ) -> Result<(), Transient> {
        let (is_list, is_item) = (op.collection == NSID_LIST, op.collection == NSID_LISTITEM);
        if !(is_list || is_item) || !proto_blue::syntax::RecordKey::is_valid(&op.rkey) {
            return Ok(());
        }
        let uri = format!("at://{repo}/{}/{}", op.collection, op.rkey);
        let mut sp = tx.begin().await?;
        let nothing = Ok(copy::Effects::default());
        let result = if is_list {
            // A create or update with no record body writes nothing.
            let list_op = match (op.action, &op.record) {
                (Action::Delete, _) => Some(copy::ListOp::Delete),
                (_, Some(record)) => Some(copy::ListOp::Put(record)),
                (_, None) => None,
            };
            match list_op {
                Some(list_op) => copy::list_purpose(&mut sp, &uri, rev, list_op).await.map(|()| copy::Effects::default()),
                None => nothing,
            }
        } else {
            let ctx = copy::Ctx { sets, earlier: copied, limits: self.subscription_limits };
            match (op.action, &op.record, &op.cid) {
                (Action::Delete, ..) => copy::item_delete(&mut sp, ctx, repo, &uri, rev).await,
                (_, Some(record), Some(cid)) => {
                    let item = copy::Item { owner: repo, uri: &uri, rkey: &op.rkey, cid, rev, record };
                    copy::item_upsert(&mut sp, ctx, item).await
                }
                _ => nothing,
            }
        };
        match result {
            Ok(eff) => {
                sp.commit().await?;
                copied.merge(eff);
            }
            Err(error) if is_deterministic(&error) => {
                sp.rollback().await?;
                let metric = if is_list { copy::METRIC_PURPOSE_ERRORS } else { copy::METRIC_COPY_ERRORS };
                metrics::counter!(metric).increment(1);
                tracing::warn!(event = "subscription_rule_write_failed", %error, "an owner-side subscription write failed");
            }
            Err(error) => return Err(error.into()),
        }
        Ok(())
    }

    /// After a commit that changed the subscription copy: the owners'
    /// tombstone counts move by what was inserted and evicted, and the
    /// touched lists' item counts are read again from their rows.
    async fn commit_copied(&self, copied: copy::Effects) {
        if copied.is_empty() {
            return;
        }
        if !copied.tombstones.is_empty() {
            let mut sets = self.scope.write_guard().await;
            for (owner, delta) in &copied.tombstones {
                if let Some(count) = sets.subscribed.owners.get_mut(owner) {
                    *count = u32::try_from((i64::from(*count) + delta).max(0)).unwrap_or(u32::MAX);
                }
            }
        }
        let mut touched = copied.touched;
        touched.sort_unstable();
        touched.dedup();
        if let Err(error) = self.scope.refresh_subscribed(&self.pool, &touched).await {
            tracing::warn!(event = "subscribed_counts_refresh_failed", %error, "re-reading subscribed list counts failed");
        }
    }

    /// The Constellation inbound-block pass's writes (§5.10): blocks of
    /// `member` by `(blocker did, rkey)`, with the sentinel rev and
    /// `created_at = indexed_at = now()`. One batch is one transaction under
    /// the ordinary-writer lock order: read guard, the shared DID lock of each
    /// blocker, sorted per-URI locks. Each row passes admission (rule 2);
    /// tombstoned URIs are skipped and existing rows are left alone.
    /// Returns the rows inserted.
    ///
    /// # Errors
    ///
    /// Any database error (transient for the job).
    pub async fn apply_inbound_blocks(&self, member: &str, blockers: &[(String, String)]) -> Result<u64, sqlx::Error> {
        let guard = self.scope.read_guard().await;
        let sets: &ScopeSets = &guard;
        let mut dids: Vec<String> = Vec::new();
        let mut uris: Vec<String> = Vec::new();
        for (did, rkey) in blockers {
            let input = AdmissionInput {
                author: did.clone(),
                kind: RecordKind::Block,
                subjects: vec![member.to_string()],
            };
            if !proto_blue::syntax::Did::is_valid(did) || !proto_blue::syntax::RecordKey::is_valid(rkey) {
                continue;
            }
            if inbound_block_admitted(admit(&input, sets)) {
                dids.push(did.clone());
                uris.push(format!("at://{did}/{}/{rkey}", super::classifier::NSID_BLOCK));
            }
        }
        if uris.is_empty() {
            return Ok(0);
        }
        let mut tx = self.pool.begin().await?;
        lock_dids_shared(&mut tx, &dids).await?;
        lock_uris(&mut tx, &uris).await?;
        let inserted = sqlx::query(
            "INSERT INTO blocks (uri, author_did, subject_did, rev, created_at, indexed_at) \
             SELECT u, d, $3, $4, now(), now() FROM unnest($1::text[], $2::text[]) AS t(u, d) \
             WHERE NOT EXISTS (SELECT 1 FROM record_tombstones rt WHERE rt.uri = t.u) \
             ON CONFLICT (uri) DO NOTHING",
        )
        .bind(&uris)
        .bind(&dids)
        .bind(member)
        .bind(SENTINEL_REV)
        .execute(&mut *tx)
        .await?
        .rows_affected();
        tx.commit().await?;
        drop(guard);
        Ok(inserted)
    }

    /// Run one op in savepoints until it succeeds, fails transiently (abort)
    /// or exhausts its deterministic attempts.
    #[allow(clippy::too_many_arguments)]
    async fn run_with_attempts(
        &self,
        tx: &mut PgConnection,
        options: ApplyOptions,
        target: OpTarget<'_>,
        repo: &str,
        rev: &str,
        sets: &ScopeSets,
        effects: &mut MemoryEffects,
    ) -> Result<OpOutcome, Transient> {
        let mut last_error = String::new();
        for _ in 0..options.attempts.max(1) {
            let mut sp = tx.begin().await?;
            let result = match target {
                OpTarget::Upsert(u, plan) => self.apply_upsert(&mut sp, u, plan, rev, sets, options).await,
                OpTarget::GrowOnly(u, plan) => self.apply_grow_only(&mut sp, u, plan, rev, options).await,
                OpTarget::Delete(d) => self.apply_delete(&mut sp, d, repo, rev, sets).await,
                OpTarget::RejectedFollow(u, reason) => self.apply_rejected_follow(&mut sp, u, reason, repo, rev, sets).await,
            };
            match result {
                Ok((outcome, eff)) => {
                    sp.commit().await?;
                    effects.merge(eff);
                    return Ok(outcome);
                }
                Err(OpError::Transient(e)) => return Err(e.into()),
                Err(OpError::Deterministic(msg)) => {
                    sp.rollback().await?;
                    last_error = msg;
                }
            }
        }
        Ok(OpOutcome::Failed(last_error))
    }

    /// A member's post under the off rule (public-indexing design §4.1):
    /// its reference and its growth, no `posts` row, no feed item and no
    /// public notification. Its quote of or reply to a private post notifies
    /// privately (private posting §4.8). There is no not-newer return, so
    /// growth always runs; it is idempotent.
    async fn apply_grow_only(
        &self,
        conn: &mut PgConnection,
        u: &Upsert,
        plan: &GrowthPlan,
        rev: &str,
        options: ApplyOptions,
    ) -> Result<(OpOutcome, MemoryEffects), OpError> {
        let mut eff = MemoryEffects::default();
        if tombstoned(conn, &u.uri, rev).await? {
            metrics::counter!(METRIC_LWW_SKIPPED, "reason" => "tombstoned").increment(1);
            return Ok((OpOutcome::Unchanged, eff));
        }
        if let Body::Post(p) = &u.body {
            upsert_post_ref(conn, u, p, rev).await?;
            self.private.upsert_quote_ref(conn, &quote_ref(u, p, rev)).await?;
        }
        if let Some(n) = self.public_notify(u, options) {
            self.private.notify_public(conn, &n).await?;
        }
        eff.growth_added = growth::apply(conn, plan).await?;
        Ok((OpOutcome::GrowOnly, eff))
    }

    /// What a public record naming a private post hands the private tier
    /// (private posting §4.8): a like or repost of one, or a post replying
    /// to or quoting one. `None` when it names none. The private tier
    /// checks that the author and each recipient are members.
    fn public_notify(&self, u: &Upsert, options: ApplyOptions) -> Option<PublicNotify> {
        let private = |uri: &str| is_private_uri(uri).then(|| uri.to_string());
        let (reasons, record_root, public_record): (Vec<(&'static str, String)>, Option<String>, Option<Value>) = match &u.body {
            Body::Like(s) => (private(&s.subject.uri).map(|s| ("like", s)).into_iter().collect(), None, None),
            Body::Repost(s) => (private(&s.subject.uri).map(|s| ("repost", s)).into_iter().collect(), None, None),
            Body::Post(p) => {
                let reasons = [
                    ("reply", p.reply_parent.as_ref().map(|r| r.uri.as_str())),
                    ("reply", p.reply_root.as_ref().map(|r| r.uri.as_str())),
                    ("quote", p.quote_uri.as_deref()),
                ]
                .into_iter()
                .filter_map(|(reason, uri)| uri.and_then(private).map(|s| (reason, s)))
                .collect();
                (reasons, p.reply_root.as_ref().map(|r| r.uri.clone()), Some(u.record.clone()))
            }
            _ => return None,
        };
        if reasons.is_empty() {
            return None;
        }
        Some(PublicNotify {
            record_uri: u.uri.clone(),
            record_cid: u.cid.clone(),
            author_did: u.input.author.clone(),
            reasons,
            record_root,
            public_record,
            source: options.source,
            created_at: u.created_at,
            notify_max_age: self.private_notify_max_age,
        })
    }

    async fn apply_upsert(
        &self,
        conn: &mut PgConnection,
        u: &Upsert,
        plan: &GrowthPlan,
        rev: &str,
        sets: &ScopeSets,
        options: ApplyOptions,
    ) -> Result<(OpOutcome, MemoryEffects), OpError> {
        let live = options.live;
        let mut eff = MemoryEffects::default();
        let author = u.input.author.as_str();
        if matches!(u.body, Body::Private | Body::PrivateGate) {
            let op = PrivateOp {
                uri: u.uri.clone(),
                author_did: author.to_string(),
                nsid: u.collection.clone(),
                rkey: u.rkey.clone(),
                cid: u.cid.clone(),
                rev: rev.to_string(),
                record: u.record.clone(),
                source: options.source,
                created_at: u.created_at,
                notify_max_age: self.private_notify_max_age,
            };
            let applied = if u.body == Body::PrivateGate {
                self.private.apply_gate(conn, &op).await?
            } else {
                self.private.apply(conn, &op).await?
            };
            eff.owners_joined = applied.owners_joined;
            eff.owners_left = applied.owners_left;
            let outcome = if applied.changed { OpOutcome::Written } else { OpOutcome::Unchanged };
            return Ok((outcome, eff));
        }
        if tombstoned(conn, &u.uri, rev).await? {
            metrics::counter!(METRIC_LWW_SKIPPED, "reason" => "tombstoned").increment(1);
            return Ok((OpOutcome::Unchanged, eff));
        }
        // Rule 4 (§4.3): a member's list block is a reference to the list.
        // An update may name another list, so the one it named is read
        // before the write.
        let member_list_block = u.collection == NSID_LISTBLOCK && sets.is_member(author);
        let list_before = if member_list_block { list_block_target(conn, &u.uri).await? } else { None };
        let written = match &u.body {
            Body::Post(p) => upsert_post(conn, u, p, rev).await?,
            Body::Like(s) => upsert_subject(conn, "likes", u, s, rev).await?,
            Body::Repost(s) => upsert_subject(conn, "reposts", u, s, rev).await?,
            Body::Follow(g) => {
                #[cfg(test)]
                if crate::test_support::hooks::injected(crate::test_support::hooks::FOLLOW_WRITE_FAILS, &u.uri) {
                    return Err(OpError::Deterministic("injected follow write failure".into()));
                }
                // A write from anything but the firehose may come from a
                // listing older than a newer fact that left no tombstone
                // (following-audience design §4.4): the stored subject is
                // read first, on the URI already locked.
                let verifies = options.source != WriteSource::Firehose && sets.is_tracked(author);
                let before: Option<String> = if verifies {
                    sqlx::query_scalar("SELECT subject_did FROM follows WHERE uri = $1")
                        .bind(&u.uri)
                        .fetch_optional(&mut *conn)
                        .await?
                } else {
                    None
                };
                let inserted = upsert_graph(conn, "follows", u, g, rev).await?;
                if inserted == Some(true) && sets.is_member(author) {
                    eff.new_member_follow = Some(author.to_string());
                }
                let changed = match inserted {
                    Some(true) => true,
                    Some(false) => before.as_deref() != Some(g.subject_did.as_str()),
                    None => false,
                };
                if verifies && changed {
                    self.follows_changed(conn, author).await?;
                }
                inserted.is_some()
            }
            Body::Block(g) => upsert_graph(conn, "blocks", u, g, rev).await?.is_some(),
            Body::Profile(p) => upsert_profile(conn, u, p, rev).await?,
            Body::Row(row) => upsert_row(conn, u, row, rev).await?,
            Body::Private | Body::PrivateGate => false,
        };
        // A member's post has a reference in every mode, whatever became
        // of the row (public-indexing design §4.1); it has its own rev check.
        // So does its quote of a private post (private posting §4.7).
        if let Body::Post(p) = &u.body
            && sets.is_member(author)
        {
            upsert_post_ref(conn, u, p, rev).await?;
            self.private.upsert_quote_ref(conn, &quote_ref(u, p, rev)).await?;
        }
        if !written {
            metrics::counter!(METRIC_LWW_SKIPPED, "reason" => "older_rev").increment(1);
            return Ok((OpOutcome::Unchanged, eff));
        }
        // A public record naming a private post notifies privately, in
        // every mode, outside the `notify` flag below (private posting §4.8).
        if let Some(n) = self.public_notify(u, options) {
            self.private.notify_public(conn, &n).await?;
        }
        // Under the off rule nothing notifies and no feed item is written
        // (public-indexing design §5). A backfilled record created before
        // the last fill began notifies nobody, in every mode (§8.3).
        let off = sets.public.mode.off_rule();
        let before_fill = options.source == WriteSource::Backfill
            && sets.public.last_fill_since.is_some_and(|since| u.created_at < since);
        let notify = !off && !before_fill;
        if member_list_block {
            let list_after = list_block_target(conn, &u.uri).await?;
            for list in [&list_before, &list_after].into_iter().flatten() {
                rederive_block(conn, author, list, self.list_budget).await?;
            }
            eff.list_references = true;
        }
        // §5.12: feed items only for public records whose author is in S.
        if !off && sets.in_scope(author) {
            match &u.body {
                Body::Post(p) => {
                    let kind = if p.reply_parent.is_some() { "reply" } else { "post" };
                    upsert_feed_item(conn, &u.uri, author, &u.uri, kind, p.created_at).await?;
                }
                Body::Repost(s) => {
                    upsert_feed_item(conn, &u.uri, author, &s.subject.uri, "repost", s.created_at).await?;
                }
                _ => {}
            }
        }
        if notify {
            self.notify(conn, u, sets, live).await?;
        }
        eff.growth_added = growth::apply(conn, plan).await?;
        Ok((OpOutcome::Written, eff))
    }

    /// Every notification an applied upsert writes (§7.7, §8.6, design-v3
    /// §5.6).
    async fn notify(&self, conn: &mut PgConnection, u: &Upsert, sets: &ScopeSets, live: bool) -> Result<(), OpError> {
        let author = u.input.author.as_str();
        // The thread a reply belongs to, for the recipients' thread mutes.
        let root = match &u.body {
            Body::Post(p) => p.reply_root.as_ref().map(|r| r.uri.as_str()),
            _ => None,
        };
        for n in notifications_for(author, &u.body, sets) {
            insert_notification(conn, &n, author, &u.uri, &u.cid, root).await?;
        }
        // After the reasons above, which take precedence (§8.6). No via
        // reason is written for a private subject: its record would name the
        // private post (private posting §4.8, #717).
        match &u.body {
            Body::Like(s) | Body::Repost(s) => {
                let reason = if matches!(u.body, Body::Like(_)) { "like-via-repost" } else { "repost-via-repost" };
                if !is_private_uri(&s.subject.uri)
                    && let Some(via) = &s.via
                    && via.uri.contains("/app.bsky.feed.repost/")
                    && let Some(reposter) = uri_author(&via.uri)
                    && reposter != author
                    && sets.is_member(reposter)
                {
                    insert_via_notification(conn, reason, author, &u.uri, &u.cid, &via.uri, &s.subject.uri).await?;
                }
            }
            // A follow made through a starter pack names it (§8.6).
            Body::Follow(g) => {
                if let Some(via) = &g.via
                    && via.uri.contains("/app.bsky.graph.starterpack/")
                {
                    sqlx::query("UPDATE notifications SET starter_pack_uri = $1 WHERE record_uri = $2 AND reason = 'follow'")
                        .bind(&via.uri)
                        .bind(&u.uri)
                        .execute(&mut *conn)
                        .await?;
                }
            }
            // `starterpack-joined` (§8.6): only a profile created just now,
            // seen on the firehose, tells the pack's creator. An edit, a
            // backfill or a replay never does, so an old join is not
            // announced again.
            Body::Profile(p) => {
                if live
                    && u.created
                    && let Some(pack) = &p.joined_via_starter_pack
                    && pack.uri.contains("/app.bsky.graph.starterpack/")
                    && let Some(creator) = uri_author(&pack.uri)
                    && creator != author
                    && sets.is_member(creator)
                    && p.created_at.is_none_or(|at| Utc::now().signed_duration_since(at) <= self.subscribed_post_max_age)
                {
                    let n = Notification {
                        recipient: creator.to_string(),
                        reason: "starterpack-joined",
                        reason_subject: Some(pack.uri.clone()),
                    };
                    insert_notification(conn, &n, author, &u.uri, &u.cid, None).await?;
                }
            }
            _ => {}
        }
        if live
            && let Body::Post(p) = &u.body
            && sets.in_scope(author)
            && Utc::now().signed_duration_since(p.created_at) <= self.subscribed_post_max_age
        {
            insert_subscribed_post(conn, author, &u.uri, &u.cid, p.reply_parent.is_some(), p.created_at, root).await?;
        }
        Ok(())
    }

    async fn apply_delete(
        &self,
        conn: &mut PgConnection,
        d: &Delete,
        repo: &str,
        rev: &str,
        sets: &ScopeSets,
    ) -> Result<(OpOutcome, MemoryEffects), OpError> {
        let mut eff = MemoryEffects::default();
        let author_in_scope = sets.in_scope(repo);
        let removed = match d.kind {
            // A delete carries no record, so it never classifies as a gate.
            RecordKind::PrivatePost | RecordKind::PrivateAudience | RecordKind::PrivateBlock | RecordKind::PrivateGate => {
                let applied = self.private.delete(conn, &d.uri, rev, author_in_scope).await?;
                eff.owners_joined = applied.owners_joined;
                eff.owners_left = applied.owners_left;
                let outcome = if applied.changed { OpOutcome::Written } else { OpOutcome::Unchanged };
                return Ok((outcome, eff));
            }
            RecordKind::Profile => delete_profile(conn, repo, rev).await?,
            RecordKind::Post => {
                // The reference goes by its own rev, whatever became of the
                // row (public-indexing design §4.1).
                delete_row(conn, "member_post_refs", &d.uri, rev).await?;
                self.private.delete_quote_ref(conn, &d.uri, rev).await?;
                let removed = delete_row(conn, "posts", &d.uri, rev).await?;
                if removed {
                    // The gates of a post that is gone gate nothing.
                    for (table, gate) in ["threadgates", "postgates"].into_iter().zip(canonical_gates(&d.uri)) {
                        sqlx::query(&format!("DELETE FROM {table} WHERE uri = $1"))
                            .bind(gate)
                            .execute(&mut *conn)
                            .await?;
                    }
                }
                removed
            }
            RecordKind::Like => delete_row(conn, "likes", &d.uri, rev).await?,
            RecordKind::Repost => delete_row(conn, "reposts", &d.uri, rev).await?,
            RecordKind::Follow => {
                #[cfg(test)]
                if crate::test_support::hooks::injected(crate::test_support::hooks::FOLLOW_WRITE_FAILS, &d.uri) {
                    return Err(OpError::Deterministic("injected follow write failure".into()));
                }
                let removed = delete_row(conn, "follows", &d.uri, rev).await?;
                if removed && sets.is_member(repo) {
                    eff.member_follow_removed = Some(repo.to_string());
                }
                removed
            }
            RecordKind::Block => delete_row(conn, "blocks", &d.uri, rev).await?,
            // Rule 4 (§4.3): the reference a member's list block stood for
            // is derived again from the member's remaining list blocks.
            RecordKind::Table("list_blocks") if sets.is_member(repo) => {
                let list = list_block_target(conn, &d.uri).await?;
                let removed = delete_row(conn, "list_blocks", &d.uri, rev).await?;
                if removed && let Some(list) = list {
                    rederive_block(conn, repo, &list, self.list_budget).await?;
                    eff.list_references = true;
                }
                removed
            }
            // A gate delete does not say which post kind it gated, so the
            // private tier deletes its copy too (private posting §4.4).
            RecordKind::Table(table @ ("threadgates" | "postgates")) => {
                let private = self.private.delete_gate(conn, &d.uri, rev, author_in_scope).await?;
                delete_row(conn, table, &d.uri, rev).await? || private.changed
            }
            RecordKind::Table(table) => delete_row(conn, table, &d.uri, rev).await?,
        };
        // Tombstone iff a row was removed, or the author is in S (§5.5); a
        // follow of any tracked repo too, so that a listing's older write
        // cannot restore an unfollow that found no row (following-audience
        // design §4.1).
        if removed || author_in_scope || (d.kind == RecordKind::Follow && sets.is_tracked(repo)) {
            sqlx::query(
                "INSERT INTO record_tombstones (uri, rev) VALUES ($1, $2) \
                 ON CONFLICT (uri) DO UPDATE SET rev = GREATEST(record_tombstones.rev, EXCLUDED.rev), deleted_at = now()",
            )
            .bind(&d.uri)
            .bind(rev)
            .execute(&mut *conn)
            .await?;
        }
        sqlx::query("DELETE FROM feed_items WHERE uri = $1")
            .bind(&d.uri)
            .execute(&mut *conn)
            .await?;
        sqlx::query("DELETE FROM notifications WHERE record_uri = $1")
            .bind(&d.uri)
            .execute(&mut *conn)
            .await?;
        // Its private notifications too, in the same savepoint, so a
        // rollback undoes both (private posting §4.8).
        self.private.delete_notifications(conn, &d.uri).await?;
        Ok((if removed { OpOutcome::Written } else { OpOutcome::Unchanged }, eff))
    }

    /// A follow write of a tracked repo from anything but the firehose that
    /// inserted a row or changed its subject (following-audience design
    /// §4.4), in the write's savepoint, `jobs` before `sync_state` (§3.3):
    /// for a following owner, an `authz` a minute on, leaving a queued or
    /// running one alone; then the flag cleared and the counter replaced, so
    /// a newer pass that never saw this row cannot leave the flag set
    /// beside it.
    async fn follows_changed(&self, conn: &mut PgConnection, author: &str) -> Result<(), sqlx::Error> {
        if self.private.owns_following_audience(&mut *conn, author).await? {
            jobs::enqueue_if_idle(&mut *conn, author, Depth::Authz, Utc::now() + chrono::TimeDelta::minutes(1)).await?;
        }
        clear_follows_verified(conn, author).await
    }

    /// A follow of a tracked repo that admission rejected, at its rev U
    /// (following-audience design §4.2), from any source: the stored row
    /// goes by last-writer-wins, with its notifications, and a tombstone at
    /// U refuses every write of the URI at or below U, so an older listing
    /// cannot store the old subject again. The tombstone only moves
    /// forward, so a pass at an unchanged rev writes nothing. The outcome
    /// stays `Rejected`: the rule only removes rows and refuses older
    /// writes.
    async fn apply_rejected_follow(
        &self,
        conn: &mut PgConnection,
        u: &Upsert,
        reason: &'static str,
        repo: &str,
        rev: &str,
        sets: &ScopeSets,
    ) -> Result<(OpOutcome, MemoryEffects), OpError> {
        let mut eff = MemoryEffects::default();
        #[cfg(test)]
        if crate::test_support::hooks::injected(crate::test_support::hooks::FOLLOW_WRITE_FAILS, &u.uri) {
            return Err(OpError::Deterministic("injected follow write failure".into()));
        }
        let removed = delete_row(conn, "follows", &u.uri, rev).await?;
        if removed && sets.is_member(repo) {
            eff.member_follow_removed = Some(repo.to_string());
        }
        sqlx::query(
            "INSERT INTO record_tombstones (uri, rev) VALUES ($1, $2) \
             ON CONFLICT (uri) DO UPDATE SET rev = EXCLUDED.rev, deleted_at = now() \
             WHERE record_tombstones.rev < EXCLUDED.rev",
        )
        .bind(&u.uri)
        .bind(rev)
        .execute(&mut *conn)
        .await?;
        if removed {
            sqlx::query("DELETE FROM notifications WHERE record_uri = $1")
                .bind(&u.uri)
                .execute(&mut *conn)
                .await?;
            self.private.delete_notifications(conn, &u.uri).await?;
        }
        Ok((OpOutcome::Rejected(reason), eff))
    }

    /// Apply post-commit in-memory changes under a brief write guard.
    async fn commit_effects(&self, eff: MemoryEffects) {
        if eff.list_references
            && let Some(applier) = &self.applier
        {
            applier.signal();
        }
        growth::commit_in_memory(&self.scope, &eff.growth_added, eff.new_member_follow.as_deref()).await;
        if eff.owners_joined.is_empty() && eff.owners_left.is_empty() && eff.member_follow_removed.is_none() {
            return;
        }
        let mut sets = self.scope.write_guard().await;
        for d in eff.owners_joined {
            sets.private_owners.insert(d);
        }
        for d in &eff.owners_left {
            sets.private_owners.remove(d);
        }
        if let Some(m) = eff.member_follow_removed
            && let Some(n) = sets.member_follows.get_mut(&m)
        {
            *n = n.saturating_sub(1);
        }
    }
}

/// Whether the inbound-block pass writes a block with this admission, a
/// rejection being counted. A block is never grow-only (public-indexing
/// design §4.1); were it one, it would not be written.
fn inbound_block_admitted(admission: Admission) -> bool {
    debug_assert!(admission != Admission::GrowOnly, "a block is never grow-only");
    match admission {
        Admission::Admit(_) => true,
        Admission::Reject(reason) => {
            metrics::counter!(METRIC_SKIPPED, "reason" => reason.as_str()).increment(1);
            false
        }
        Admission::GrowOnly => false,
    }
}

/// The list a stored list block names. A subject in any other collection
/// makes no reference.
async fn list_block_target(conn: &mut PgConnection, uri: &str) -> Result<Option<String>, sqlx::Error> {
    let subject: Option<String> = sqlx::query_scalar("SELECT list_uri FROM list_blocks WHERE uri = $1").bind(uri).fetch_optional(conn).await?;
    Ok(subject.filter(|s| s.split('/').nth(3) == Some(NSID_LIST)))
}

async fn tombstoned(conn: &mut PgConnection, uri: &str, rev: &str) -> Result<bool, sqlx::Error> {
    sqlx::query_scalar("SELECT EXISTS (SELECT 1 FROM record_tombstones WHERE uri = $1 AND rev >= $2)")
        .bind(uri)
        .bind(rev)
        .fetch_one(conn)
        .await
}

async fn upsert_post(conn: &mut PgConnection, u: &Upsert, p: &PostRecord, rev: &str) -> Result<bool, sqlx::Error> {
    let n = sqlx::query(
        "INSERT INTO posts (uri, author_did, rkey, cid, rev, text, langs, \
           reply_root_uri, reply_root_cid, reply_parent_uri, reply_parent_cid, quote_uri, \
           embed, facets, mention_dids, self_labels, record, created_at, \
           tags, link_domains, link_urls, lang_prefixes, embed_type, media_type, search_tsv) \
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, \
           $19, $20, $21, $22, $23, $24, to_tsvector('simple', $6)) \
         ON CONFLICT (uri) DO UPDATE SET author_did = EXCLUDED.author_did, rkey = EXCLUDED.rkey, \
           cid = EXCLUDED.cid, rev = EXCLUDED.rev, text = EXCLUDED.text, langs = EXCLUDED.langs, \
           reply_root_uri = EXCLUDED.reply_root_uri, reply_root_cid = EXCLUDED.reply_root_cid, \
           reply_parent_uri = EXCLUDED.reply_parent_uri, reply_parent_cid = EXCLUDED.reply_parent_cid, \
           quote_uri = EXCLUDED.quote_uri, embed = EXCLUDED.embed, facets = EXCLUDED.facets, \
           mention_dids = EXCLUDED.mention_dids, self_labels = EXCLUDED.self_labels, \
           record = EXCLUDED.record, created_at = EXCLUDED.created_at, \
           tags = EXCLUDED.tags, link_domains = EXCLUDED.link_domains, link_urls = EXCLUDED.link_urls, \
           lang_prefixes = EXCLUDED.lang_prefixes, embed_type = EXCLUDED.embed_type, \
           media_type = EXCLUDED.media_type, search_tsv = EXCLUDED.search_tsv \
         WHERE posts.rev < EXCLUDED.rev",
    )
    .bind(&u.uri)
    .bind(&u.input.author)
    .bind(&u.rkey)
    .bind(&u.cid)
    .bind(rev)
    .bind(&p.text)
    .bind(&p.langs)
    .bind(p.reply_root.as_ref().map(|r| r.uri.as_str()))
    .bind(p.reply_root.as_ref().and_then(|r| r.cid.as_deref()))
    .bind(p.reply_parent.as_ref().map(|r| r.uri.as_str()))
    .bind(p.reply_parent.as_ref().and_then(|r| r.cid.as_deref()))
    .bind(p.quote_uri.as_deref())
    .bind(p.embed.as_ref())
    .bind(p.facets.as_ref())
    .bind(&p.mention_dids)
    .bind(&p.self_labels)
    .bind(&u.record)
    .bind(p.created_at)
    .bind(&p.extract.tags)
    .bind(&p.extract.link_domains)
    .bind(&p.extract.link_urls)
    .bind(&p.extract.lang_prefixes)
    .bind(p.extract.embed_type.as_deref())
    .bind(p.media_type)
    .execute(conn)
    .await?
    .rows_affected();
    Ok(n > 0)
}

/// The last-writer-wins upsert of a member's post reference
/// (public-indexing design §4.1): the DIDs the post replies to and quotes,
/// as the scope rebuild reads them.
async fn upsert_post_ref(conn: &mut PgConnection, u: &Upsert, p: &PostRecord, rev: &str) -> Result<(), sqlx::Error> {
    sqlx::query(
        "INSERT INTO member_post_refs (uri, author_did, rev, reply_parent_did, reply_root_did, quote_did, created_at) \
         VALUES ($1, $2, $3, $4, $5, $6, $7) \
         ON CONFLICT (uri) DO UPDATE SET author_did = EXCLUDED.author_did, rev = EXCLUDED.rev, \
           reply_parent_did = EXCLUDED.reply_parent_did, reply_root_did = EXCLUDED.reply_root_did, \
           quote_did = EXCLUDED.quote_did, created_at = EXCLUDED.created_at \
         WHERE member_post_refs.rev < EXCLUDED.rev",
    )
    .bind(&u.uri)
    .bind(&u.input.author)
    .bind(rev)
    .bind(p.reply_parent.as_ref().and_then(|r| uri_author(&r.uri)))
    .bind(p.reply_root.as_ref().and_then(|r| uri_author(&r.uri)))
    .bind(p.quote_uri.as_deref().and_then(uri_author))
    .bind(p.created_at)
    .execute(conn)
    .await?;
    Ok(())
}

/// A member's post as the private tier keeps its quote of a private post
/// (private posting §4.7): the quoted `postPrivate` URI, or `None` when it
/// quotes none, so an update that drops the quote removes the reference.
fn quote_ref(u: &Upsert, p: &PostRecord, rev: &str) -> QuoteRef {
    QuoteRef {
        uri: u.uri.clone(),
        author_did: u.input.author.clone(),
        cid: u.cid.clone(),
        rev: rev.to_string(),
        quoted_uri: p.quote_uri.clone().filter(|q| is_private_uri(q)),
        created_at: p.created_at,
    }
}

async fn upsert_subject(
    conn: &mut PgConnection,
    table: &'static str,
    u: &Upsert,
    s: &SubjectRecord,
    rev: &str,
) -> Result<bool, sqlx::Error> {
    let sql = format!(
        "INSERT INTO {table} (uri, author_did, subject_uri, subject_cid, rev, created_at, via_uri, via_cid) \
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8) \
         ON CONFLICT (uri) DO UPDATE SET author_did = EXCLUDED.author_did, subject_uri = EXCLUDED.subject_uri, \
           subject_cid = EXCLUDED.subject_cid, rev = EXCLUDED.rev, created_at = EXCLUDED.created_at, \
           via_uri = EXCLUDED.via_uri, via_cid = EXCLUDED.via_cid \
         WHERE {table}.rev < EXCLUDED.rev"
    );
    let n = sqlx::query(&sql)
        .bind(&u.uri)
        .bind(&u.input.author)
        .bind(&s.subject.uri)
        .bind(s.subject.cid.as_deref())
        .bind(rev)
        .bind(s.created_at)
        .bind(s.via.as_ref().map(|v| v.uri.as_str()))
        .bind(s.via.as_ref().and_then(|v| v.cid.as_deref()))
        .execute(conn)
        .await?
        .rows_affected();
    Ok(n > 0)
}

/// Returns `Some(true)` when inserted, `Some(false)` when updated, `None`
/// when LWW kept the existing row.
async fn upsert_graph(
    conn: &mut PgConnection,
    table: &'static str,
    u: &Upsert,
    g: &GraphRecord,
    rev: &str,
) -> Result<Option<bool>, sqlx::Error> {
    // Only follows carry `via` (the starter pack that led to them); blocks
    // have no such columns.
    let (via_columns, via_values, via_set) = if table == "follows" {
        (", via_uri, via_cid", ", $6, $7", ", via_uri = EXCLUDED.via_uri, via_cid = EXCLUDED.via_cid")
    } else {
        ("", "", "")
    };
    let sql = format!(
        "INSERT INTO {table} (uri, author_did, subject_did, rev, created_at{via_columns}) VALUES ($1, $2, $3, $4, $5{via_values}) \
         ON CONFLICT (uri) DO UPDATE SET author_did = EXCLUDED.author_did, subject_did = EXCLUDED.subject_did, \
           rev = EXCLUDED.rev, created_at = EXCLUDED.created_at{via_set} \
         WHERE {table}.rev < EXCLUDED.rev \
         RETURNING (xmax = 0)"
    );
    let query = sqlx::query_scalar(&sql)
        .bind(&u.uri)
        .bind(&u.input.author)
        .bind(&g.subject_did)
        .bind(rev)
        .bind(g.created_at);
    let query = if table == "follows" {
        query
            .bind(g.via.as_ref().map(|v| v.uri.as_str()))
            .bind(g.via.as_ref().and_then(|v| v.cid.as_deref()))
    } else {
        query
    };
    query.fetch_optional(conn).await
}

async fn upsert_profile(
    conn: &mut PgConnection,
    u: &Upsert,
    p: &ProfileRecord,
    rev: &str,
) -> Result<bool, sqlx::Error> {
    // A newer rev replaces the profile. An equal rev only fills a missing
    // `profile_cid` (design-v3 §12.2): rows written before the column
    // existed get their CID from a re-sync whose rev equals the stored one,
    // and nothing else of the row changes in that case.
    let n = sqlx::query(
        "INSERT INTO actors AS a (did, display_name, description, avatar_cid, banner_cid, profile_rev, profile_record, \
           profile_cid, pronouns, website, joined_via_starter_pack_uri, joined_via_starter_pack_cid, profile_created_at) \
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13) \
         ON CONFLICT (did) DO UPDATE SET \
           display_name = CASE WHEN a.profile_rev = EXCLUDED.profile_rev THEN a.display_name ELSE EXCLUDED.display_name END, \
           description = CASE WHEN a.profile_rev = EXCLUDED.profile_rev THEN a.description ELSE EXCLUDED.description END, \
           avatar_cid = CASE WHEN a.profile_rev = EXCLUDED.profile_rev THEN a.avatar_cid ELSE EXCLUDED.avatar_cid END, \
           banner_cid = CASE WHEN a.profile_rev = EXCLUDED.profile_rev THEN a.banner_cid ELSE EXCLUDED.banner_cid END, \
           profile_record = CASE WHEN a.profile_rev = EXCLUDED.profile_rev THEN a.profile_record ELSE EXCLUDED.profile_record END, \
           pronouns = CASE WHEN a.profile_rev = EXCLUDED.profile_rev THEN a.pronouns ELSE EXCLUDED.pronouns END, \
           website = CASE WHEN a.profile_rev = EXCLUDED.profile_rev THEN a.website ELSE EXCLUDED.website END, \
           joined_via_starter_pack_uri = CASE WHEN a.profile_rev = EXCLUDED.profile_rev \
             THEN a.joined_via_starter_pack_uri ELSE EXCLUDED.joined_via_starter_pack_uri END, \
           joined_via_starter_pack_cid = CASE WHEN a.profile_rev = EXCLUDED.profile_rev \
             THEN a.joined_via_starter_pack_cid ELSE EXCLUDED.joined_via_starter_pack_cid END, \
           profile_created_at = CASE WHEN a.profile_rev = EXCLUDED.profile_rev \
             THEN a.profile_created_at ELSE EXCLUDED.profile_created_at END, \
           profile_cid = EXCLUDED.profile_cid, profile_rev = EXCLUDED.profile_rev \
         WHERE a.profile_rev IS NULL OR a.profile_rev < EXCLUDED.profile_rev \
            OR (a.profile_cid IS NULL AND a.profile_record IS NOT NULL AND a.profile_rev = EXCLUDED.profile_rev)",
    )
    .bind(&u.input.author)
    .bind(p.display_name.as_deref())
    .bind(p.description.as_deref())
    .bind(p.avatar_cid.as_deref())
    .bind(p.banner_cid.as_deref())
    .bind(rev)
    .bind(&u.record)
    .bind(&u.cid)
    .bind(p.pronouns.as_deref())
    .bind(p.website.as_deref())
    .bind(p.joined_via_starter_pack.as_ref().map(|s| s.uri.as_str()))
    .bind(p.joined_via_starter_pack.as_ref().and_then(|s| s.cid.as_deref()))
    .bind(p.created_at)
    .execute(conn)
    .await?
    .rows_affected();
    Ok(n > 0)
}

/// The last-writer-wins upsert of a v3 record table (§II-5.5): every column
/// of the row is set, and an older or equal rev changes nothing.
async fn upsert_row(conn: &mut PgConnection, u: &Upsert, row: &Row, rev: &str) -> Result<bool, sqlx::Error> {
    let table = row.table;
    let mut columns: Vec<&str> = vec!["uri", row.owner_column];
    if row.has_rkey {
        columns.push("rkey");
    }
    columns.extend(["cid", "rev"]);
    columns.extend(row.cells.iter().map(|(name, _)| *name));
    let placeholders: Vec<String> = (1..=columns.len()).map(|i| format!("${i}")).collect();
    let updates: Vec<String> = columns.iter().skip(1).map(|c| format!("{c} = EXCLUDED.{c}")).collect();
    let sql = format!(
        "INSERT INTO {table} ({}) VALUES ({}) ON CONFLICT (uri) DO UPDATE SET {} WHERE {table}.rev < EXCLUDED.rev",
        columns.join(", "),
        placeholders.join(", "),
        updates.join(", "),
    );
    let mut query = sqlx::query(&sql).bind(&u.uri).bind(&u.input.author);
    if row.has_rkey {
        query = query.bind(&u.rkey);
    }
    query = query.bind(&u.cid).bind(rev);
    for (_, cell) in &row.cells {
        query = match cell {
            Cell::Text(v) => query.bind(v.as_deref()),
            Cell::TextArray(v) => query.bind(v),
            Cell::Json(v) => query.bind(v.as_ref()),
            Cell::Bool(v) => query.bind(*v),
            Cell::Time(v) => query.bind(*v),
        };
    }
    Ok(query.execute(conn).await?.rows_affected() > 0)
}

async fn delete_row(conn: &mut PgConnection, table: &'static str, uri: &str, rev: &str) -> Result<bool, sqlx::Error> {
    let sql = format!("DELETE FROM {table} WHERE uri = $1 AND rev <= $2");
    Ok(sqlx::query(&sql)
        .bind(uri)
        .bind(rev)
        .execute(conn)
        .await?
        .rows_affected()
        > 0)
}

async fn delete_profile(conn: &mut PgConnection, did: &str, rev: &str) -> Result<bool, sqlx::Error> {
    Ok(sqlx::query(
        "UPDATE actors SET display_name = NULL, description = NULL, avatar_cid = NULL, \
           banner_cid = NULL, profile_record = NULL, profile_cid = NULL, pronouns = NULL, website = NULL, \
           joined_via_starter_pack_uri = NULL, joined_via_starter_pack_cid = NULL, profile_created_at = NULL, \
           profile_rev = $2 \
         WHERE did = $1 AND profile_record IS NOT NULL AND (profile_rev IS NULL OR profile_rev <= $2)",
    )
    .bind(did)
    .bind(rev)
    .execute(conn)
    .await?
    .rows_affected()
        > 0)
}

async fn upsert_feed_item(
    conn: &mut PgConnection,
    uri: &str,
    originator: &str,
    post_uri: &str,
    kind: &str,
    sort_at: DateTime<Utc>,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        "INSERT INTO feed_items (uri, originator_did, post_uri, kind, sort_at) VALUES ($1, $2, $3, $4, $5) \
         ON CONFLICT (uri) DO UPDATE SET originator_did = EXCLUDED.originator_did, \
           post_uri = EXCLUDED.post_uri, kind = EXCLUDED.kind, sort_at = EXCLUDED.sort_at",
    )
    .bind(uri)
    .bind(originator)
    .bind(post_uri)
    .bind(kind)
    .bind(sort_at)
    .execute(conn)
    .await?;
    Ok(())
}

/// Whether recipient `$1` muted the thread of a notification whose record
/// is `$2`, whose record's thread root is `$3` and whose subject is `$4`:
/// the record or the subject is the muted root, or a post under it
/// (design-v3 §5.3 `muteThread`; B-59). The same rule as the read-time
/// exclusion in `hydration::notifications`.
const THREAD_MUTED: &str = "EXISTS (SELECT 1 FROM thread_mutes t WHERE t.viewer_did = $1 AND t.root_uri IN ( \
       $2::text, $3::text, $4::text, (SELECT p.reply_root_uri FROM posts p WHERE p.uri = $4::text)))";

/// Insert one notification, unless its recipient muted the thread it is in
/// (`record_root`: the thread root a reply names). A muted thread writes
/// nothing, so unmuting it brings back nothing (B-59).
async fn insert_notification(
    conn: &mut PgConnection,
    n: &Notification,
    author: &str,
    record_uri: &str,
    record_cid: &str,
    record_root: Option<&str>,
) -> Result<(), sqlx::Error> {
    sqlx::query(&format!(
        "INSERT INTO notifications (recipient_did, author_did, reason, reason_subject, record_uri, record_cid) \
         SELECT $1, $5, $6, $4, $2, $7 WHERE NOT {THREAD_MUTED} ON CONFLICT DO NOTHING"
    ))
    .bind(&n.recipient)
    .bind(record_uri)
    .bind(record_root)
    .bind(n.reason_subject.as_deref())
    .bind(author)
    .bind(n.reason)
    .bind(record_cid)
    .execute(conn)
    .await?;
    Ok(())
}

/// `like-via-repost` / `repost-via-repost` (design-v3 §8.6): a like or
/// repost made from someone's repost notifies that reposter. Only when the
/// repost is stored here and reposts the same subject, and not when the
/// reposter was already notified of this record (as the subject's author).
async fn insert_via_notification(
    conn: &mut PgConnection,
    reason: &'static str,
    author: &str,
    record_uri: &str,
    record_cid: &str,
    via: &str,
    subject: &str,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        "INSERT INTO notifications (recipient_did, author_did, reason, reason_subject, record_uri, record_cid) \
         SELECT r.author_did, $1, $2, r.uri, $3, $4 FROM reposts r \
         WHERE r.uri = $5 AND r.subject_uri = $6 \
           AND NOT EXISTS (SELECT 1 FROM notifications n WHERE n.recipient_did = r.author_did AND n.record_uri = $3) \
         ON CONFLICT DO NOTHING",
    )
    .bind(author)
    .bind(reason)
    .bind(record_uri)
    .bind(record_cid)
    .bind(via)
    .bind(subject)
    .execute(conn)
    .await?;
    Ok(())
}

/// `subscribed-post` notifications for a post by `author` just written from
/// the firehose (design-v3 §5.6): one for each member subscribed to the
/// author for this kind of post (`reply` or a top-level post), where
/// - the subscription is older than the post, at the second precision
///   clients stamp `createdAt` with: one made during the post's second
///   counts;
/// - the author has been backfilled once, so its declaration is stored if
///   it has one;
/// - the author's declaration still allows the subscriber (no record means
///   followers; a subscriber it refuses is skipped and its row is kept);
/// - no other notification for this record went to that member.
async fn insert_subscribed_post(
    conn: &mut PgConnection,
    author: &str,
    record_uri: &str,
    record_cid: &str,
    reply: bool,
    created_at: DateTime<Utc>,
    record_root: Option<&str>,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        "INSERT INTO notifications (recipient_did, author_did, reason, record_uri, record_cid) \
         SELECT s.subscriber_did, $1, 'subscribed-post', $2, $3 \
         FROM activity_subscriptions s \
         CROSS JOIN (SELECT COALESCE((SELECT record->>'allowSubscriptions' FROM actor_self_records \
                                      WHERE did = $1 AND collection = 'app.bsky.notification.declaration'), \
                                     'followers') AS allow) d \
         WHERE s.subject_did = $1 AND s.subscriber_did <> $1 \
           AND CASE WHEN $4 THEN s.reply ELSE s.post END \
           AND s.created_at < date_trunc('second', $5::timestamptz) + interval '1 second' \
           AND EXISTS (SELECT 1 FROM members m WHERE m.did = s.subscriber_did) \
           AND EXISTS (SELECT 1 FROM sync_state y WHERE y.did = $1 AND y.last_resync_at IS NOT NULL) \
           AND d.allow IN ('followers', 'mutuals') \
           AND EXISTS (SELECT 1 FROM follows f WHERE f.author_did = s.subscriber_did AND f.subject_did = $1) \
           AND (d.allow <> 'mutuals' \
                OR EXISTS (SELECT 1 FROM follows f WHERE f.author_did = $1 AND f.subject_did = s.subscriber_did)) \
           AND NOT EXISTS (SELECT 1 FROM notifications n WHERE n.recipient_did = s.subscriber_did AND n.record_uri = $2) \
           AND NOT EXISTS (SELECT 1 FROM thread_mutes t WHERE t.viewer_did = s.subscriber_did AND t.root_uri IN ($2, $6::text)) \
         ON CONFLICT DO NOTHING",
    )
    .bind(author)
    .bind(record_uri)
    .bind(record_cid)
    .bind(reply)
    .bind(created_at)
    .bind(record_root)
    .execute(conn)
    .await?;
    Ok(())
}

/// Clear one DID's readiness and enqueue `authz` (§6.11.2, §6.11.6).
///
/// # Errors
///
/// Any database error.
pub async fn clear_readiness(
    conn: &mut PgConnection,
    private: &dyn PrivateTier,
    did: &str,
) -> Result<(), sqlx::Error> {
    sqlx::query("UPDATE members SET ready_at = NULL WHERE did = $1")
        .bind(did)
        .execute(&mut *conn)
        .await?;
    private.clear_owner_readiness(&mut *conn, did).await?;
    jobs::enqueue(&mut *conn, did, Depth::Authz).await?;
    // Last, after the job row: the order every writer of these rows uses
    // (`members` → jobs → `sync_state`; following-audience design §3.3).
    clear_follows_verified(conn, did).await
}

/// Clear one DID's follow-verification flag and replace its counter with a
/// fresh sequence value (following-audience design §3.3), so a run that
/// read the counter before this can set nothing. A DID with no row has
/// nothing to stop: a run in flight created its row before reading.
///
/// # Errors
///
/// Any database error.
pub async fn clear_follows_verified(conn: &mut PgConnection, did: &str) -> Result<(), sqlx::Error> {
    sqlx::query(
        "UPDATE sync_state SET follows_verified_at = NULL, follows_clears = nextval('follows_clears_seq') WHERE did = $1",
    )
    .bind(did)
    .execute(conn)
    .await?;
    Ok(())
}

/// Make sure the DID's `sync_state` row exists and read its counter, in one
/// statement that always returns a row (following-audience design §3.2): a
/// run that may verify follows reads it before its first listing. The
/// no-op update locks the row for the statement and makes `RETURNING`
/// yield the stored row.
///
/// # Errors
///
/// Any database error.
pub async fn read_follows_clears(conn: &mut PgConnection, did: &str) -> Result<i64, sqlx::Error> {
    sqlx::query_scalar(
        "INSERT INTO sync_state (did) VALUES ($1) \
         ON CONFLICT (did) DO UPDATE SET did = EXCLUDED.did \
         RETURNING follows_clears",
    )
    .bind(did)
    .fetch_one(conn)
    .await
}

/// `applied_rev = GREATEST(applied_rev, rev)` (§6.11.3). `firehose`: the
/// firehose's own advance, which also raises `firehose_rev`, the position
/// a follow-verification flag waits for (following-audience design §3.2).
///
/// # Errors
///
/// Any database error.
pub async fn advance_applied_rev(conn: &mut PgConnection, did: &str, rev: &str, firehose: bool) -> Result<(), sqlx::Error> {
    sqlx::query(
        "INSERT INTO sync_state (did, applied_rev, firehose_rev) VALUES ($1, $2, CASE WHEN $3 THEN $2 END) \
         ON CONFLICT (did) DO UPDATE SET applied_rev = GREATEST(sync_state.applied_rev, EXCLUDED.applied_rev), \
           firehose_rev = CASE WHEN $3 THEN GREATEST(sync_state.firehose_rev, EXCLUDED.applied_rev) \
                               ELSE sync_state.firehose_rev END",
    )
    .bind(did)
    .bind(rev)
    .bind(firehose)
    .execute(conn)
    .await?;
    Ok(())
}

/// A settle's `applied_rev` advance with the follow-verification flag
/// (following-audience design §3.2), as catch-up writes it. `pass`:
/// `Some(clears)` for a complete follows pass whose run read `clears` at its
/// start; `None` for no complete pass. The flag is set only when the stored
/// counter still equals that reading, compared under the row lock; every
/// other case, the `INSERT` branch included, writes NULL.
/// `follows_verified_rev` is the `applied_rev` this leaves behind. Returns
/// whether the flag was set.
///
/// # Errors
///
/// Any database error.
pub async fn advance_applied_rev_with_follows(
    conn: &mut PgConnection,
    did: &str,
    rev: &str,
    pass: Option<i64>,
) -> Result<bool, sqlx::Error> {
    sqlx::query_scalar(
        "INSERT INTO sync_state (did, applied_rev) VALUES ($1, $2) \
         ON CONFLICT (did) DO UPDATE SET \
           applied_rev = GREATEST(sync_state.applied_rev, EXCLUDED.applied_rev), \
           follows_verified_at = CASE WHEN $3 AND sync_state.follows_clears = $4 THEN now() END, \
           follows_verified_rev = GREATEST(sync_state.applied_rev, EXCLUDED.applied_rev) \
         RETURNING follows_verified_at IS NOT NULL",
    )
    .bind(did)
    .bind(rev)
    .bind(pass.is_some())
    .bind(pass.unwrap_or(0))
    .fetch_one(conn)
    .await
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    const MEMBER: &str = "did:plc:member";

    fn sets() -> ScopeSets {
        let mut s = ScopeSets::default();
        s.members.insert(MEMBER.into());
        s.members.insert("did:plc:m2".into());
        s.scope.insert(MEMBER.into());
        s
    }

    fn op(action: Action, collection: &str, rkey: &str, record: Option<Value>) -> RawOp {
        RawOp {
            action,
            collection: collection.into(),
            rkey: rkey.into(),
            cid: record.as_ref().map(|_| "bafycid".to_string()),
            record,
        }
    }

    #[test]
    fn prepare_classifies_and_validates() {
        let now = Utc::now();
        assert_eq!(
            prepare("did:plc:a", &op(Action::Create, "app.bsky.feed.generator", "3k", Some(json!({}))), now),
            Prepared::Skip("unindexed")
        );
        // A listitem naming a list outside its own repo is unindexed too.
        let foreign = json!({"list": "at://did:plc:b/app.bsky.graph.list/3k", "subject": "did:plc:c"});
        assert_eq!(
            prepare("did:plc:a", &op(Action::Create, "app.bsky.graph.listitem", "3k", Some(foreign)), now),
            Prepared::Skip("unindexed")
        );
        assert!(matches!(
            prepare("did:plc:a", &op(Action::Create, "app.bsky.feed.like", "3k", Some(json!({"subject": 1}))), now),
            Prepared::Invalid(_)
        ));
        assert!(matches!(
            prepare("did:plc:a", &op(Action::Create, "app.bsky.feed.like", "bad rkey!", Some(json!({}))), now),
            Prepared::Invalid(_)
        ));
        match prepare("did:plc:a", &op(Action::Delete, "app.bsky.feed.post", "3k", None), now) {
            Prepared::Delete(d) => {
                assert_eq!(d.uri, "at://did:plc:a/app.bsky.feed.post/3k");
                assert_eq!(d.kind, RecordKind::Post);
            }
            other => panic!("expected delete, got {other:?}"),
        }
    }

    #[test]
    fn prepare_builds_admission_subjects() {
        let now = Utc::now();
        let reply = json!({"text": "r", "reply": {
            "root": {"uri": "at://did:plc:root/app.bsky.feed.post/1", "cid": "c"},
            "parent": {"uri": "at://did:plc:parent/app.bsky.feed.post/2", "cid": "c"}}});
        match prepare("did:plc:a", &op(Action::Create, "app.bsky.feed.post", "3k", Some(reply)), now) {
            Prepared::Upsert(u) => assert_eq!(u.input.subjects, ["did:plc:parent", "did:plc:root"]),
            other => panic!("expected upsert, got {other:?}"),
        }
        let follow = json!({"subject": "did:plc:b", "createdAt": "2026-01-01T00:00:00Z"});
        match prepare("did:plc:a", &op(Action::Create, "app.bsky.graph.follow", "3k", Some(follow)), now) {
            Prepared::Upsert(u) => {
                assert_eq!(u.input.kind, RecordKind::Follow);
                assert_eq!(u.input.subjects, ["did:plc:b"]);
            }
            other => panic!("expected upsert, got {other:?}"),
        }
    }

    #[test]
    fn notification_rules() {
        let s = sets();
        let like = Body::Like(SubjectRecord {
            subject: super::super::records::StrongRef {
                uri: format!("at://{MEMBER}/app.bsky.feed.post/1"),
                cid: None,
            },
            via: None,
            created_at: Utc::now(),
        });
        let n = notifications_for("did:plc:x", &like, &s);
        assert_eq!(n.len(), 1);
        assert_eq!((n[0].recipient.as_str(), n[0].reason), (MEMBER, "like"));
        // No self-notification, no non-member recipients.
        assert!(notifications_for(MEMBER, &like, &s).is_empty());

        let post = Body::Post(Box::new(
            PostRecord::parse(
                &json!({"text": "t",
                    "reply": {"root": {"uri": "at://did:plc:m2/app.bsky.feed.post/1"},
                              "parent": {"uri": format!("at://{MEMBER}/app.bsky.feed.post/2")}},
                    "facets": [{"features": [
                        {"$type": "app.bsky.richtext.facet#mention", "did": MEMBER},
                        {"$type": "app.bsky.richtext.facet#mention", "did": "did:plc:outsider"}]}]}),
                Utc::now(),
            )
            .expect("post"),
        ));
        let n = notifications_for("did:plc:x", &post, &s);
        // Reply beats mention for MEMBER; root author gets its own reply.
        assert_eq!(
            n.iter().map(|x| (x.recipient.as_str(), x.reason)).collect::<Vec<_>>(),
            [(MEMBER, "reply"), ("did:plc:m2", "reply")]
        );
    }

    #[test]
    fn deterministic_sqlstates() {
        assert!(!is_deterministic(&sqlx::Error::PoolTimedOut));
    }

    /// T-3 (public-indexing design §4.1): the inbound-block pass never
    /// writes a block it was told is grow-only. A debug build stops there.
    #[test]
    #[cfg_attr(debug_assertions, should_panic(expected = "a block is never grow-only"))]
    fn a_grow_only_block_is_never_written() {
        assert!(!inbound_block_admitted(Admission::GrowOnly));
    }

    /// Postgres-backed tests (see `crate::test_support`).
    mod pg {
        use std::time::Duration;

        use super::*;
        use crate::config::ScopePolicy;
        use crate::db::locks::lock_did_exclusive;
        use crate::indexing::{PublicMode, PublicState};
        use crate::private_tier::DisabledPrivateTier;
        use crate::test_support::fresh_db;

        const AUTHOR: &str = "did:plc:aaaaaaaaaaaaaaaaaaaaaaaa";
        const SUBJECT: &str = "did:plc:bbbbbbbbbbbbbbbbbbbbbbbb";
        const RKEY: &str = "3kaaaaaaaaaaa";

        const OPTIONS: ApplyOptions = ApplyOptions {
            advance_applied_rev: true,
            dead_letter_readiness: true,
            attempts: 5,
            live: true,
            subscription_rules: true,
            source: WriteSource::Firehose,
        };

        fn writer(pool: &PgPool, member: bool) -> Writer {
            let mut sets = ScopeSets::default();
            sets.scope.insert(AUTHOR.into());
            if member {
                sets.members.insert(AUTHOR.into());
            }
            let scope = ScopeState::new(sets, ScopePolicy::MembersAndOneHop, 10_000);
            Writer::new(pool.clone(), scope, Arc::new(DisabledPrivateTier))
        }

        fn post_op(action: Action, text: &str) -> RawOp {
            RawOp {
                action,
                collection: "app.bsky.feed.post".into(),
                rkey: RKEY.into(),
                cid: (action != Action::Delete).then(|| format!("bafy{text}")),
                record: (action != Action::Delete).then(|| json!({"text": text, "createdAt": "2026-09-26T00:00:00Z"})),
            }
        }

        fn rev(n: usize) -> String {
            format!("3kaaaaaaaa{n:03}")
        }

        async fn post_rev(pool: &PgPool) -> Option<(String, String)> {
            sqlx::query_as("SELECT rev, text FROM posts WHERE uri = $1")
                .bind(format!("at://{AUTHOR}/app.bsky.feed.post/{RKEY}"))
                .fetch_optional(pool)
                .await
                .expect("query post")
        }

        /// design-v3 §4.3: a list owner outside scope has its list items
        /// copied for the members who reference the list, and nothing is
        /// written to the public tables. With `subscription_rules` off
        /// (`requestRecord`) nothing of rule 4 is written at all.
        #[tokio::test]
        async fn the_owner_side_rules_run_beside_admission() {
            use crate::subscriptions::list_phase;
            use crate::subscriptions::references::{Budget, mute_list};

            let Some(db) = fresh_db("writer_owner_side").await else {
                return;
            };
            let p = &db.pool;
            let owner = "did:plc:oooooooooooooooooooooooo";
            let list = format!("at://{owner}/app.bsky.graph.list/3klist");
            mute_list(p, AUTHOR, &list, Budget { max_references: 5, max_owners: 5 }).await.expect("mute");
            list_phase::apply(p, &list, 600).await.expect("apply");
            let scope = ScopeState::new(ScopeSets::default(), ScopePolicy::MembersAndOneHop, 10_000);
            scope.refresh_subscribed(p, std::slice::from_ref(&list)).await.expect("subscribed");
            let w = Writer::new(p.clone(), scope.clone(), Arc::new(DisabledPrivateTier));
            let op = |action: Action, collection: &str, rkey: &str, record: Value| RawOp {
                action,
                collection: collection.into(),
                rkey: rkey.into(),
                cid: (action != Action::Delete).then(|| format!("bafy{rkey}")),
                record: (action != Action::Delete).then_some(record),
            };
            let count = |sql: &'static str| async move { sqlx::query_scalar::<_, i64>(sql).fetch_one(p).await.expect(sql) };
            let now = "2026-09-26T00:00:00Z";
            let item = |rkey: &str| op(Action::Create, "app.bsky.graph.listitem", rkey, json!({"list": list, "subject": SUBJECT, "createdAt": now}));
            let record = op(
                Action::Create,
                "app.bsky.graph.list",
                "3klist",
                json!({"name": "n", "purpose": "app.bsky.graph.defs#modlist", "createdAt": now}),
            );

            // Off: the owner's write leaves no trace in rule 4's tables.
            let quiet = ApplyOptions { subscription_rules: false, ..OPTIONS };
            let r = w.apply(owner, &rev(1), &[record.clone(), item("3kone")], quiet).await.expect("apply");
            assert!(r.outcomes.iter().all(|o| matches!(o, OpOutcome::Rejected(_))), "{:?}", r.outcomes);
            assert_eq!(count("SELECT count(*) FROM subscribed_list_items").await, 0);
            assert_eq!(count("SELECT count(*) FROM subscribed_lists WHERE purpose IS NOT NULL").await, 0);

            // On: the same ops are still refused for the public tables, and
            // the copy and the purpose are written.
            let r = w.apply(owner, &rev(2), &[record, item("3kone"), item("3ktwo")], OPTIONS).await.expect("apply");
            assert!(r.outcomes.iter().all(|o| matches!(o, OpOutcome::Rejected(_))), "{:?}", r.outcomes);
            assert!(r.failed.is_empty());
            assert_eq!(count("SELECT count(*) FROM subscribed_list_items").await, 2);
            assert_eq!(count("SELECT count(*) FROM subscribed_lists WHERE purpose = 'app.bsky.graph.defs#modlist'").await, 1);
            assert_eq!(count("SELECT count(*) FROM list_items").await + count("SELECT count(*) FROM lists").await, 0);
            assert_eq!(scope.snapshot().await.subscribed.lists.get(&list), Some(&2));

            // A delete removes the copy and leaves a tombstone; the public
            // tombstone table learns nothing; the counts follow.
            let gone = op(Action::Delete, "app.bsky.graph.listitem", "3kone", Value::Null);
            w.apply(owner, &rev(3), &[gone], OPTIONS).await.expect("apply");
            assert_eq!(count("SELECT count(*) FROM subscribed_list_items").await, 1);
            assert_eq!(count("SELECT count(*) FROM subscription_tombstones").await, 1);
            assert_eq!(count("SELECT count(*) FROM record_tombstones").await, 0);
            let snap = scope.snapshot().await;
            assert_eq!((snap.subscribed.lists.get(&list), snap.subscribed.owners.get(owner)), (Some(&1), Some(&1)));
            // The late create of the deleted item is not copied again.
            w.apply(owner, &rev(2), &[item("3kone")], OPTIONS).await.expect("apply");
            assert_eq!(count("SELECT count(*) FROM subscribed_list_items").await, 1);
            // Someone who owns no subscribed list is untouched by all this.
            w.apply(SUBJECT, &rev(1), &[op(Action::Delete, "app.bsky.graph.listitem", "3kx", Value::Null)], OPTIONS).await.expect("apply");
            assert_eq!(count("SELECT count(*) FROM subscription_tombstones").await, 1);
            db.drop_db().await;
        }

        /// design-v3 §4.3: a member's list block is a reference to the
        /// list, on every write path; it follows the record when the
        /// record is changed to name another list, and goes with it.
        #[tokio::test]
        async fn a_members_list_block_is_a_reference() {
            let Some(db) = fresh_db("writer_list_block_reference").await else {
                return;
            };
            let p = &db.pool;
            let mut sets = ScopeSets::default();
            sets.scope.insert(AUTHOR.into());
            sets.members.insert(AUTHOR.into());
            let w = Writer::new(p.clone(), ScopeState::new(sets, ScopePolicy::MembersAndOneHop, 10_000), Arc::new(DisabledPrivateTier));
            let list = |n: u32| format!("at://{SUBJECT}/app.bsky.graph.list/3klist{n}");
            let block = |action: Action, n: u32| RawOp {
                action,
                collection: "app.bsky.graph.listblock".into(),
                rkey: "3kblock".into(),
                cid: (action != Action::Delete).then(|| format!("bafy{n}")),
                record: (action != Action::Delete).then(|| json!({"subject": list(n), "createdAt": "2026-09-26T00:00:00Z"})),
            };
            let references = || async move {
                sqlx::query_as::<_, (String, String, bool)>("SELECT list_uri, source, admitted FROM list_references ORDER BY list_uri")
                    .fetch_all(p)
                    .await
                    .expect("references")
            };
            // `requestRecord` runs the member phase too.
            let request = ApplyOptions { subscription_rules: false, ..OPTIONS };
            w.apply(AUTHOR, &rev(1), &[block(Action::Create, 1)], request).await.expect("apply");
            assert_eq!(references().await, vec![(list(1), "block".to_string(), true)]);
            w.apply(AUTHOR, &rev(2), &[block(Action::Update, 2)], OPTIONS).await.expect("apply");
            assert_eq!(references().await, vec![(list(2), "block".to_string(), true)]);
            w.apply(AUTHOR, &rev(3), &[block(Action::Delete, 2)], OPTIONS).await.expect("apply");
            assert!(references().await.is_empty());
            // Each change left a marker for the list phase.
            let marked: i64 = sqlx::query_scalar("SELECT count(DISTINCT list_uri) FROM list_reference_changes").fetch_one(p).await.expect("markers");
            assert_eq!(marked, 2);
            // Someone who is not a member gets no reference.
            w.apply(SUBJECT, &rev(1), &[block(Action::Create, 1)], OPTIONS).await.expect("apply");
            assert!(references().await.is_empty());
            db.drop_db().await;
        }

        /// design-v3 §8.6: a like made from a member's repost notifies the
        /// reposter, only when the repost is stored and has the same
        /// subject; a follow through a starter pack keeps the pack.
        #[tokio::test]
        async fn via_reasons_and_the_follows_starter_pack() {
            let Some(db) = fresh_db("writer_via_reasons").await else {
                return;
            };
            let p = &db.pool;
            let liker = "did:plc:cccccccccccccccccccccccc";
            let mut sets = ScopeSets::default();
            sets.scope.insert(AUTHOR.into());
            sets.scope.insert(liker.into());
            sets.members.insert(AUTHOR.into());
            let w = Writer::new(p.clone(), ScopeState::new(sets, ScopePolicy::MembersAndOneHop, 10_000), Arc::new(DisabledPrivateTier));
            let op = |collection: &str, rkey: &str, record: Value| RawOp {
                action: Action::Create,
                collection: collection.into(),
                rkey: rkey.into(),
                cid: Some(format!("bafy{rkey}")),
                record: Some(record),
            };
            let post = format!("at://{SUBJECT}/app.bsky.feed.post/3kpost");
            let other = format!("at://{SUBJECT}/app.bsky.feed.post/3kother");
            let repost = format!("at://{AUTHOR}/app.bsky.feed.repost/3krepost");
            let subject = |uri: &str| json!({"uri": uri, "cid": "bafysubject"});
            let now = "2026-09-26T00:00:00Z";
            // The member reposts a stranger's post.
            let r = op("app.bsky.feed.repost", "3krepost", json!({"subject": subject(&post), "createdAt": now}));
            w.apply(AUTHOR, &rev(1), &[r], OPTIONS).await.expect("apply");
            // A like made from that repost, and one whose `via` names the
            // repost but likes another post.
            let via = json!({"uri": repost, "cid": "bafy3krepost"});
            let like = op("app.bsky.feed.like", "3klike", json!({"subject": subject(&post), "via": via, "createdAt": now}));
            let stray = op("app.bsky.feed.like", "3kstray", json!({"subject": subject(&other), "via": via, "createdAt": now}));
            w.apply(liker, &rev(1), &[like, stray], OPTIONS).await.expect("apply");
            let got: Vec<(String, Option<String>, String)> =
                sqlx::query_as("SELECT reason, reason_subject, record_uri FROM notifications WHERE recipient_did = $1 ORDER BY id")
                    .bind(AUTHOR)
                    .fetch_all(p)
                    .await
                    .expect("notifications");
            assert_eq!(got, [("like-via-repost".to_string(), Some(repost.clone()), format!("at://{liker}/app.bsky.feed.like/3klike"))]);
            // A follow made through a starter pack.
            let pack = format!("at://{SUBJECT}/app.bsky.graph.starterpack/3kpack");
            let follow = op(
                "app.bsky.graph.follow",
                "3kfollow",
                json!({"subject": AUTHOR, "via": {"uri": pack, "cid": "bafypack"}, "createdAt": now}),
            );
            w.apply(liker, &rev(2), &[follow], OPTIONS).await.expect("apply");
            let stored: Option<String> = sqlx::query_scalar("SELECT starter_pack_uri FROM notifications WHERE reason = 'follow'")
                .fetch_one(p)
                .await
                .expect("follow notification");
            assert_eq!(stored, Some(pack));
            // Joining through the member's starter pack: a profile created
            // live tells the member; an edit, or a backfilled create, does
            // not.
            let mine = format!("at://{AUTHOR}/app.bsky.graph.starterpack/3kmine");
            let profile = |name: &str, action: Action| RawOp {
                action,
                collection: "app.bsky.actor.profile".into(),
                rkey: "self".into(),
                cid: Some(format!("bafy{name}")),
                record: Some(json!({"displayName": name, "joinedViaStarterPack": {"uri": mine, "cid": "bafymine"},
                                    "createdAt": Utc::now().to_rfc3339()})),
            };
            let joined = || async {
                sqlx::query_scalar::<_, i64>("SELECT count(*) FROM notifications WHERE reason = 'starterpack-joined' AND recipient_did = $1 AND reason_subject = $2")
                    .bind(AUTHOR)
                    .bind(&mine)
                    .fetch_one(p)
                    .await
                    .expect("count")
            };
            let backfill = ApplyOptions { live: false, source: WriteSource::Backfill, ..OPTIONS };
            w.apply(liker, &rev(3), &[profile("first", Action::Create)], backfill).await.expect("apply");
            assert_eq!(joined().await, 0);
            w.apply(liker, &rev(4), &[profile("edited", Action::Update)], OPTIONS).await.expect("apply");
            assert_eq!(joined().await, 0);
            w.apply(liker, &rev(5), &[profile("again", Action::Create)], OPTIONS).await.expect("apply");
            assert_eq!(joined().await, 1);
            db.drop_db().await;
        }

        /// B-59 (design-v3 §5.3): a reply in, and a like of a post in, a
        /// thread the member muted writes no notification row, so unmuting
        /// brings none back; a reply after the unmute notifies again.
        #[tokio::test]
        async fn a_muted_thread_writes_no_notification() {
            let Some(db) = fresh_db("writer_thread_mute").await else {
                return;
            };
            let p = &db.pool;
            let other = "did:plc:cccccccccccccccccccccccc";
            let mut sets = ScopeSets::default();
            sets.scope.insert(AUTHOR.into());
            sets.scope.insert(other.into());
            sets.members.insert(AUTHOR.into());
            let w = Writer::new(p.clone(), ScopeState::new(sets, ScopePolicy::MembersAndOneHop, 10_000), Arc::new(DisabledPrivateTier));
            let op = |collection: &str, rkey: &str, record: Value| RawOp {
                action: Action::Create,
                collection: collection.into(),
                rkey: rkey.into(),
                cid: Some(format!("bafy{rkey}")),
                record: Some(record),
            };
            let now = "2026-09-26T00:00:00Z";
            let root = format!("at://{AUTHOR}/app.bsky.feed.post/3kroot");
            let mine = format!("at://{AUTHOR}/app.bsky.feed.post/3kmine");
            let strong = |uri: &str| json!({"uri": uri, "cid": "bafyx"});
            let reply_to = |rkey: &str, parent: &str| {
                op("app.bsky.feed.post", rkey, json!({"text": rkey, "createdAt": now, "reply": {"root": strong(&root), "parent": strong(parent)}}))
            };
            w.apply(AUTHOR, &rev(1), &[op("app.bsky.feed.post", "3kroot", json!({"text": "root", "createdAt": now}))], OPTIONS).await.expect("apply");
            // The member's own reply under its root, then the mute.
            w.apply(AUTHOR, &rev(2), &[reply_to("3kmine", &root)], OPTIONS).await.expect("apply");
            exec_sql(p, &format!("INSERT INTO thread_mutes (viewer_did, root_uri) VALUES ('{AUTHOR}', '{root}')")).await;
            // A reply to the root, a reply to the member's reply, a like of
            // the member's reply and a like of the root.
            let muted = [
                reply_to("3kreply", &root),
                reply_to("3kdeep", &mine),
                op("app.bsky.feed.like", "3klike", json!({"subject": strong(&mine), "createdAt": now})),
                op("app.bsky.feed.like", "3krootlike", json!({"subject": strong(&root), "createdAt": now})),
            ];
            w.apply(other, &rev(1), &muted, OPTIONS).await.expect("apply");
            let rows = || async {
                sqlx::query_scalar::<_, String>("SELECT record_uri FROM notifications WHERE recipient_did = $1 ORDER BY id")
                    .bind(AUTHOR)
                    .fetch_all(p)
                    .await
                    .expect("notifications")
            };
            assert_eq!(rows().await, Vec::<String>::new());
            // Unmuted: nothing comes back, and a new reply notifies.
            exec_sql(p, "DELETE FROM thread_mutes").await;
            assert_eq!(rows().await, Vec::<String>::new());
            w.apply(other, &rev(2), &[reply_to("3kafter", &root)], OPTIONS).await.expect("apply");
            assert_eq!(rows().await, [format!("at://{other}/app.bsky.feed.post/3kafter")]);
            db.drop_db().await;
        }

        async fn exec_sql(pool: &PgPool, sql: &str) {
            sqlx::query(sql).execute(pool).await.expect("exec");
        }

        /// B-43: `subscribed-post` comes only from a live write of a fresh
        /// post, for a subscription the author's declaration allows.
        #[tokio::test]
        async fn a_subscribed_authors_post_notifies_its_subscribers() {
            let Some(db) = fresh_db("writer_subscribed_post").await else {
                return;
            };
            let p = &db.pool;
            let w = writer(p, false);
            let exec = |sql: String| async move {
                sqlx::query(&sql).execute(p).await.expect("setup");
            };
            exec(format!("INSERT INTO members (did, source, ready_at) VALUES ('{SUBJECT}', 'admin', now())")).await;
            exec(format!("INSERT INTO sync_state (did, last_resync_at) VALUES ('{AUTHOR}', now())")).await;
            exec(format!(
                "INSERT INTO follows (uri, author_did, subject_did, rev, created_at) VALUES \
                 ('at://{SUBJECT}/app.bsky.graph.follow/1', '{SUBJECT}', '{AUTHOR}', '3k', now())"
            ))
            .await;
            exec(format!(
                "INSERT INTO activity_subscriptions (subscriber_did, subject_did, post, reply, created_at) VALUES \
                 ('{SUBJECT}', '{AUTHOR}', true, false, now() - interval '1 hour')"
            ))
            .await;
            let post = |rkey: &str, created: String, reply: bool| {
                let mut record = json!({"text": rkey, "createdAt": created});
                if reply {
                    let parent = json!({"uri": format!("at://{AUTHOR}/app.bsky.feed.post/3kfirst"), "cid": "bafyfirst"});
                    record["reply"] = json!({"root": parent, "parent": parent});
                }
                RawOp {
                    action: Action::Create,
                    collection: "app.bsky.feed.post".into(),
                    rkey: rkey.into(),
                    cid: Some(format!("bafy{rkey}")),
                    record: Some(record),
                }
            };
            let fresh = || Utc::now().to_rfc3339();
            let notified = || async {
                sqlx::query_scalar::<_, String>(
                    "SELECT record_uri FROM notifications WHERE recipient_did = $1 AND reason = 'subscribed-post' ORDER BY id",
                )
                .bind(SUBJECT)
                .fetch_all(p)
                .await
                .expect("notifications")
            };
            let backfill = ApplyOptions { live: false, source: WriteSource::Backfill, ..OPTIONS };

            // Not from the firehose; a reply, which the subscription leaves
            // out; a post older than the limit: nothing.
            w.apply(AUTHOR, &rev(1), &[post("3kfirst", fresh(), false)], backfill).await.expect("apply");
            w.apply(AUTHOR, &rev(2), &[post("3kreply", fresh(), true)], OPTIONS).await.expect("apply");
            w.apply(AUTHOR, &rev(3), &[post("3kold", "2026-01-01T00:00:00Z".into(), false)], OPTIONS).await.expect("apply");
            assert!(notified().await.is_empty());
            // A fresh post from the firehose: one notification.
            w.apply(AUTHOR, &rev(4), &[post("3kfresh", fresh(), false)], OPTIONS).await.expect("apply");
            assert_eq!(notified().await, [format!("at://{AUTHOR}/app.bsky.feed.post/3kfresh")]);
            // The author's declaration now refuses: skipped, the row kept.
            exec(format!(
                "INSERT INTO actor_self_records (uri, did, collection, cid, rev, record) VALUES \
                 ('at://{AUTHOR}/app.bsky.notification.declaration/self', '{AUTHOR}', 'app.bsky.notification.declaration', \
                  'bafyd', '3k', '{{\"allowSubscriptions\": \"none\"}}')"
            ))
            .await;
            w.apply(AUTHOR, &rev(5), &[post("3krefused", fresh(), false)], OPTIONS).await.expect("apply");
            assert_eq!(notified().await.len(), 1);
            let kept: i64 = sqlx::query_scalar("SELECT count(*) FROM activity_subscriptions").fetch_one(p).await.expect("count");
            assert_eq!(kept, 1);
            // An author never backfilled announces nothing.
            exec("DELETE FROM actor_self_records".into()).await;
            exec("UPDATE sync_state SET last_resync_at = NULL".into()).await;
            w.apply(AUTHOR, &rev(6), &[post("3kunsynced", fresh(), false)], OPTIONS).await.expect("apply");
            assert_eq!(notified().await.len(), 1);
            db.drop_db().await;
        }

        /// B-43: clients stamp `createdAt` to the second, so a post made in
        /// the second its subscription was created in is announced; one
        /// from the second before is not.
        #[tokio::test]
        async fn a_post_in_the_subscriptions_own_second_is_announced() {
            let Some(db) = fresh_db("writer_subscribed_same_second").await else {
                return;
            };
            let p = &db.pool;
            let w = writer(p, false);
            let exec = |sql: String| async move {
                sqlx::query(&sql).execute(p).await.expect("setup");
            };
            exec(format!("INSERT INTO members (did, source, ready_at) VALUES ('{SUBJECT}', 'admin', now())")).await;
            exec(format!("INSERT INTO sync_state (did, last_resync_at) VALUES ('{AUTHOR}', now())")).await;
            exec(format!(
                "INSERT INTO follows (uri, author_did, subject_did, rev, created_at) VALUES \
                 ('at://{SUBJECT}/app.bsky.graph.follow/1', '{SUBJECT}', '{AUTHOR}', '3k', now())"
            ))
            .await;
            // The subscription is made 700 ms into a whole second.
            let second = DateTime::from_timestamp(Utc::now().timestamp() - 10, 0).expect("second");
            let subscribed = second + chrono::TimeDelta::milliseconds(700);
            exec(format!(
                "INSERT INTO activity_subscriptions (subscriber_did, subject_did, post, reply, created_at) VALUES \
                 ('{SUBJECT}', '{AUTHOR}', true, false, '{}')",
                subscribed.to_rfc3339()
            ))
            .await;
            let post = |rkey: &str, created: DateTime<Utc>| RawOp {
                action: Action::Create,
                collection: "app.bsky.feed.post".into(),
                rkey: rkey.into(),
                cid: Some(format!("bafy{rkey}")),
                record: Some(json!({"text": rkey, "createdAt": created.to_rfc3339_opts(chrono::SecondsFormat::Secs, true)})),
            };
            let notified = || async {
                sqlx::query_scalar::<_, String>(
                    "SELECT record_uri FROM notifications WHERE recipient_did = $1 AND reason = 'subscribed-post' ORDER BY id",
                )
                .bind(SUBJECT)
                .fetch_all(p)
                .await
                .expect("notifications")
            };
            w.apply(AUTHOR, &rev(1), &[post("3kbefore", second - chrono::TimeDelta::seconds(1))], OPTIONS).await.expect("apply");
            assert!(notified().await.is_empty(), "a post from the second before is not announced");
            w.apply(AUTHOR, &rev(2), &[post("3ksame", second)], OPTIONS).await.expect("apply");
            assert_eq!(notified().await, [format!("at://{AUTHOR}/app.bsky.feed.post/3ksame")]);
            db.drop_db().await;
        }

        /// design-v3 §3.1, §3.2: the columns the v3 migrations added.
        #[tokio::test]
        async fn new_columns_are_extracted_on_write() {
            let Some(db) = fresh_db("writer_new_columns").await else {
                return;
            };
            let p = &db.pool;
            let w = writer(p, false);
            let raw = |collection: &str, rkey: &str, cid: &str, record: Value| RawOp {
                action: Action::Create,
                collection: collection.into(),
                rkey: rkey.into(),
                cid: Some(cid.into()),
                record: Some(record),
            };
            let via = json!({"uri": "at://did:plc:pack/app.bsky.graph.starterpack/1", "cid": "bafyvia"});
            let ops = [
                raw(
                    "app.bsky.feed.post",
                    RKEY,
                    "bafypost",
                    json!({
                        "text": "Hello Rust world", "langs": ["en-US"], "tags": ["Cats"],
                        "facets": [{"features": [
                            {"$type": "app.bsky.richtext.facet#tag", "tag": "rust"},
                            {"$type": "app.bsky.richtext.facet#link", "uri": "https://News.Example.com/a#x"},
                        ]}],
                        "embed": {"$type": "app.bsky.embed.video", "video": {}},
                    }),
                ),
                raw("app.bsky.feed.like", "3kl", "bafylike", json!({"subject": {"uri": format!("at://{SUBJECT}/app.bsky.feed.post/1"), "cid": "c"}, "via": via})),
                raw("app.bsky.graph.follow", "3kf", "bafyfollow", json!({"subject": SUBJECT, "via": via})),
                raw("app.bsky.graph.block", "3kb", "bafyblock", json!({"subject": SUBJECT})),
                raw(
                    "app.bsky.actor.profile",
                    "self",
                    "bafyprofile",
                    json!({"displayName": "A", "pronouns": "it/its", "website": "https://a.example",
                           "joinedViaStarterPack": via, "createdAt": "2026-01-01T00:00:00Z"}),
                ),
            ];
            let report = w.apply(AUTHOR, &rev(1), &ops, OPTIONS).await.expect("apply");
            assert!(report.failed.is_empty(), "{:?}", report.failed);

            type PostRow = (Vec<String>, Vec<String>, Vec<String>, Vec<String>, Option<String>, Option<String>, bool);
            let post: PostRow = sqlx::query_as(
                "SELECT tags, link_domains, link_urls, lang_prefixes, embed_type, media_type, \
                        search_tsv @@ websearch_to_tsquery('simple', 'rust world') FROM posts",
            )
            .fetch_one(p)
            .await
            .expect("post");
            assert_eq!(post.0, ["cats", "rust"]);
            assert_eq!(post.1, ["example.com", "news.example.com"]);
            assert_eq!(post.2, ["https://news.example.com/a"]);
            assert_eq!(post.3, ["en", "en-us"]);
            assert_eq!((post.4.as_deref(), post.5.as_deref(), post.6), (Some("app.bsky.embed.video"), Some("video"), true));
            // The tag trigger saw the tags the writer stored.
            let tagged: i64 = sqlx::query_scalar("SELECT count(*) FROM tag_activity_hourly WHERE posts = 1")
                .fetch_one(p)
                .await
                .expect("tags");
            assert_eq!(tagged, 2);

            let like_via: (Option<String>, Option<String>) = sqlx::query_as("SELECT via_uri, via_cid FROM likes").fetch_one(p).await.expect("like");
            assert_eq!(like_via, (Some("at://did:plc:pack/app.bsky.graph.starterpack/1".into()), Some("bafyvia".into())));
            let follow_via: Option<String> = sqlx::query_scalar("SELECT via_uri FROM follows").fetch_one(p).await.expect("follow");
            assert_eq!(follow_via.as_deref(), Some("at://did:plc:pack/app.bsky.graph.starterpack/1"));
            let blocks: i64 = sqlx::query_scalar("SELECT count(*) FROM blocks").fetch_one(p).await.expect("block");
            assert_eq!(blocks, 1);

            type ProfileRow = (Option<String>, Option<String>, Option<String>, Option<String>, Option<String>, bool);
            let profile_sql = "SELECT profile_cid, pronouns, website, joined_via_starter_pack_uri, display_name, \
                               profile_created_at = '2026-01-01T00:00:00Z' FROM actors WHERE did = $1";
            let profile: ProfileRow = sqlx::query_as(profile_sql).bind(AUTHOR).fetch_one(p).await.expect("profile");
            assert_eq!(profile.0.as_deref(), Some("bafyprofile"));
            assert_eq!((profile.1.as_deref(), profile.2.as_deref()), (Some("it/its"), Some("https://a.example")));
            assert_eq!(profile.3.as_deref(), Some("at://did:plc:pack/app.bsky.graph.starterpack/1"));
            assert!(profile.5);

            // A row written before `profile_cid` existed gets it from a
            // re-sync at the same rev, and nothing else of the row changes.
            sqlx::query("UPDATE actors SET profile_cid = NULL WHERE did = $1").bind(AUTHOR).execute(p).await.expect("clear");
            let resync = [raw("app.bsky.actor.profile", "self", "bafyprofile", json!({"displayName": "Changed"}))];
            w.apply(AUTHOR, &rev(1), &resync, OPTIONS).await.expect("apply");
            let profile: ProfileRow = sqlx::query_as(profile_sql).bind(AUTHOR).fetch_one(p).await.expect("profile");
            assert_eq!((profile.0.as_deref(), profile.4.as_deref(), profile.1.as_deref()), (Some("bafyprofile"), Some("A"), Some("it/its")));
            // With the CID present, an equal rev changes nothing.
            let again = [raw("app.bsky.actor.profile", "self", "bafyother", json!({"displayName": "Changed"}))];
            w.apply(AUTHOR, &rev(1), &again, OPTIONS).await.expect("apply");
            let profile: ProfileRow = sqlx::query_as(profile_sql).bind(AUTHOR).fetch_one(p).await.expect("profile");
            assert_eq!((profile.0.as_deref(), profile.4.as_deref()), (Some("bafyprofile"), Some("A")));
            db.drop_db().await;
        }

        /// design-v3 §3.2: the twelve collections v3 indexes are written,
        /// updated by rev and deleted like every other record.
        #[tokio::test]
        async fn v3_collections_are_written_updated_and_deleted() {
            let Some(db) = fresh_db("writer_v3_collections").await else {
                return;
            };
            let p = &db.pool;
            let w = writer(p, false);
            let raw = |action: Action, collection: &str, rkey: &str, record: Value| RawOp {
                action,
                collection: collection.into(),
                rkey: rkey.into(),
                cid: (action != Action::Delete).then(|| format!("bafy{rkey}")),
                record: (action != Action::Delete).then_some(record),
            };
            let create = |collection: &str, rkey: &str, record: Value| raw(Action::Create, collection, rkey, record);
            let list = format!("at://{AUTHOR}/app.bsky.graph.list/3klist");
            let foreign = format!("at://{SUBJECT}/app.bsky.graph.list/3klist");
            let post = format!("at://{AUTHOR}/app.bsky.feed.post/3kpost");
            let ops = [
                create("app.bsky.graph.list", "3klist", json!({"name": "Mods", "purpose": "app.bsky.graph.defs#modlist"})),
                create("app.bsky.graph.listitem", "3kitem", json!({"list": list, "subject": SUBJECT})),
                // Names a list in another repo: not indexed.
                create("app.bsky.graph.listitem", "3kforeign", json!({"list": foreign, "subject": SUBJECT})),
                create("app.bsky.graph.listblock", "3kblock", json!({"subject": foreign})),
                create("app.bsky.graph.referencelistoptout", "3kopt", json!({"subject": foreign})),
                create("app.bsky.graph.starterpack", "3kpack", json!({"name": "Starter", "list": list, "description": "new here"})),
                create("app.bsky.labeler.service", "self", json!({"policies": {"labelValues": ["spam"]}})),
                create("app.bsky.notification.declaration", "self", json!({"allowSubscriptions": "followers"})),
                create("app.bsky.actor.status", "self", json!({"status": "app.bsky.actor.status#live"})),
                // Only rkey `self` is indexed for a self record.
                create("app.bsky.actor.status", "3kother", json!({"status": "x"})),
                create("app.bsky.feed.threadgate", "3kpost", json!({"post": post, "allow": []})),
                create("app.bsky.feed.postgate", "3kpost", json!({"post": post, "detachedEmbeddingUris": ["at://x/y/z"]})),
                create(
                    "app.nearhorizon.poll.post",
                    "3kpoll",
                    json!({"text": "which?", "visibility": "public", "options": [{"id": "a", "label": "A"}, {"id": "b", "label": "B"}]}),
                ),
                // A list with no purpose is a broken record.
                create("app.bsky.graph.list", "3kbroken", json!({"name": "x"})),
            ];
            let report = w.apply(AUTHOR, &rev(1), &ops, OPTIONS).await.expect("apply");
            let skipped: Vec<usize> = report.outcomes.iter().enumerate().filter(|(_, o)| **o == OpOutcome::Skipped("unindexed")).map(|(i, _)| i).collect();
            assert_eq!(skipped, [2, 9]);
            assert_eq!(report.failed.iter().map(|f| f.index).collect::<Vec<_>>(), [13]);
            assert_eq!(report.outcomes.iter().filter(|o| **o == OpOutcome::Written).count(), 11);

            let count = |table: &'static str| async move {
                let n: i64 = sqlx::query_scalar(&format!("SELECT count(*) FROM {table}")).fetch_one(p).await.expect(table);
                n
            };
            for (table, rows) in [
                ("lists", 1),
                ("list_items", 1),
                ("list_blocks", 1),
                ("list_optouts", 1),
                ("starter_packs", 1),
                ("labeler_services", 1),
                ("actor_self_records", 2),
                ("threadgates", 1),
                ("postgates", 1),
                ("nearhorizon_poll_posts", 1),
            ] {
                assert_eq!(count(table).await, rows, "{table}");
            }
            let gate: (String, Option<Value>, Vec<String>) =
                sqlx::query_as("SELECT post_uri, allow, hidden_replies FROM threadgates").fetch_one(p).await.expect("gate");
            assert_eq!(gate, (post.clone(), Some(json!([])), Vec::new()));
            let pack: (String, bool) = sqlx::query_as("SELECT creator_did, search_tsv @@ to_tsquery('simple', 'starter') FROM starter_packs")
                .fetch_one(p)
                .await
                .expect("pack");
            assert_eq!(pack, (AUTHOR.to_string(), true));

            // An older or equal rev changes nothing; a newer one replaces.
            let rename = |name: &str| [raw(Action::Update, "app.bsky.graph.list", "3klist", json!({"name": name, "purpose": "app.bsky.graph.defs#curatelist"}))];
            let r = w.apply(AUTHOR, &rev(1), &rename("Stale"), OPTIONS).await.expect("apply");
            assert_eq!(r.outcomes, [OpOutcome::Unchanged]);
            let r = w.apply(AUTHOR, &rev(2), &rename("Renamed"), OPTIONS).await.expect("apply");
            assert_eq!(r.outcomes, [OpOutcome::Written]);
            let name: (String, String) = sqlx::query_as("SELECT name, purpose FROM lists").fetch_one(p).await.expect("list");
            assert_eq!(name, ("Renamed".to_string(), "app.bsky.graph.defs#curatelist".to_string()));

            // Deletes remove the row and leave a tombstone that stops an
            // older write from bringing it back.
            let deletes = [
                raw(Action::Delete, "app.bsky.graph.listitem", "3kitem", Value::Null),
                raw(Action::Delete, "app.bsky.actor.status", "self", Value::Null),
                raw(Action::Delete, "app.bsky.feed.threadgate", "3kpost", Value::Null),
            ];
            let r = w.apply(AUTHOR, &rev(3), &deletes, OPTIONS).await.expect("apply");
            assert_eq!(r.outcomes, [OpOutcome::Written, OpOutcome::Written, OpOutcome::Written]);
            assert_eq!((count("list_items").await, count("actor_self_records").await, count("threadgates").await), (0, 1, 0));
            let back = [create("app.bsky.graph.listitem", "3kitem", json!({"list": list, "subject": SUBJECT}))];
            let r = w.apply(AUTHOR, &rev(2), &back, OPTIONS).await.expect("apply");
            assert_eq!(r.outcomes, [OpOutcome::Unchanged]);
            assert_eq!(count("list_items").await, 0);

            // Deleting a post deletes its canonical gates with it.
            let gated = [
                create("app.bsky.feed.post", "3kgated", json!({"text": "gated"})),
                create("app.bsky.feed.threadgate", "3kgated", json!({"post": format!("at://{AUTHOR}/app.bsky.feed.post/3kgated")})),
                create("app.bsky.feed.postgate", "3kgated", json!({"post": format!("at://{AUTHOR}/app.bsky.feed.post/3kgated")})),
            ];
            w.apply(AUTHOR, &rev(4), &gated, OPTIONS).await.expect("apply");
            assert_eq!((count("threadgates").await, count("postgates").await), (1, 2));
            let gone = [raw(Action::Delete, "app.bsky.feed.post", "3kgated", Value::Null)];
            w.apply(AUTHOR, &rev(5), &gone, OPTIONS).await.expect("apply");
            assert_eq!((count("threadgates").await, count("postgates").await), (0, 1), "the other post's postgate stays");

            // An author outside the scope is not admitted for these.
            let stranger = [create("app.bsky.graph.list", "3klist", json!({"name": "Theirs", "purpose": "app.bsky.graph.defs#modlist"}))];
            let r = w.apply(SUBJECT, &rev(1), &stranger, OPTIONS).await.expect("apply");
            assert_eq!(r.outcomes, [OpOutcome::Rejected("not_in_scope")]);
            db.drop_db().await;
        }

        #[tokio::test]
        async fn lww_under_concurrent_writes() {
            let Some(db) = fresh_db("lww_under_concurrent_writes").await else {
                return;
            };
            let w = writer(&db.pool, false);
            // 30 updates of one URI at distinct revs, applied concurrently in a
            // scrambled order: the highest rev wins regardless of arrival.
            let mut order: Vec<usize> = (1..=30).collect();
            order.reverse();
            order.swap(3, 17);
            let tasks: Vec<_> = order
                .into_iter()
                .map(|n| {
                    let w = w.clone();
                    tokio::spawn(async move {
                        w.apply(AUTHOR, &rev(n), &[post_op(Action::Update, &format!("v{n}"))], OPTIONS)
                            .await
                            .expect("apply")
                    })
                })
                .collect();
            for t in tasks {
                t.await.expect("join");
            }
            assert_eq!(post_rev(&db.pool).await, Some((rev(30), "v30".into())));

            // A delete below the current rev removes nothing, and writes a
            // tombstone only because the author is in S.
            let r = w.apply(AUTHOR, &rev(10), &[post_op(Action::Delete, "")], OPTIONS).await.expect("apply");
            assert_eq!(r.outcomes, [OpOutcome::Unchanged]);
            assert!(post_rev(&db.pool).await.is_some());

            // Concurrent delete at rev 40 and re-create at rev 35: the delete
            // wins either way (the create is older, or it is tombstoned).
            let (rev_del, rev_create) = (rev(40), rev(35));
            let (del_ops, create_ops) = ([post_op(Action::Delete, "")], [post_op(Action::Create, "late")]);
            let (a, b) = tokio::join!(
                w.apply(AUTHOR, &rev_del, &del_ops, OPTIONS),
                w.apply(AUTHOR, &rev_create, &create_ops, OPTIONS),
            );
            a.expect("delete");
            b.expect("create");
            assert_eq!(post_rev(&db.pool).await, None);
            let tomb: String = sqlx::query_scalar("SELECT rev FROM record_tombstones WHERE uri = $1")
                .bind(format!("at://{AUTHOR}/app.bsky.feed.post/{RKEY}"))
                .fetch_one(&db.pool)
                .await
                .expect("tombstone");
            assert_eq!(tomb, rev(40));

            // A newer create after the tombstone is written again.
            w.apply(AUTHOR, &rev(50), &[post_op(Action::Create, "again")], OPTIONS).await.expect("apply");
            assert_eq!(post_rev(&db.pool).await, Some((rev(50), "again".into())));
            db.drop_db().await;
        }

        #[tokio::test]
        async fn growth_takes_the_subject_did_lock_before_insert() {
            let Some(db) = fresh_db("growth_takes_the_subject_did_lock_before_insert").await else {
                return;
            };
            let w = writer(&db.pool, true);
            // Retention's exclusive lock on the subject, held open.
            let mut holder = db.pool.begin().await.expect("begin");
            lock_did_exclusive(&mut holder, SUBJECT).await.expect("lock");

            let follow = RawOp {
                action: Action::Create,
                collection: "app.bsky.graph.follow".into(),
                rkey: RKEY.into(),
                cid: Some("bafyfollow".into()),
                record: Some(json!({"subject": SUBJECT, "createdAt": "2026-09-26T00:00:00Z"})),
            };
            let task = {
                let w = w.clone();
                tokio::spawn(async move { w.apply(AUTHOR, &rev(1), &[follow], OPTIONS).await })
            };
            tokio::time::sleep(Duration::from_millis(300)).await;
            assert!(!task.is_finished(), "writer must wait for the subject's DID lock");
            let in_scope: bool = sqlx::query_scalar("SELECT EXISTS (SELECT 1 FROM scope_dids WHERE did = $1)")
                .bind(SUBJECT)
                .fetch_one(&mut *holder)
                .await
                .expect("query");
            assert!(!in_scope);
            let follows: i64 = sqlx::query_scalar("SELECT count(*) FROM follows")
                .fetch_one(&mut *holder)
                .await
                .expect("query");
            assert_eq!(follows, 0, "nothing is written before the lock is granted");
            holder.rollback().await.expect("release");

            let report = task.await.expect("join").expect("apply");
            assert_eq!(report.outcomes, [OpOutcome::Written]);
            let reasons: i16 = sqlx::query_scalar("SELECT reasons FROM scope_dids WHERE did = $1")
                .bind(SUBJECT)
                .fetch_one(&db.pool)
                .await
                .expect("scope row");
            assert_eq!(reasons, crate::aggregations::REASON_FOLLOWED);
            let job: String = sqlx::query_scalar("SELECT depth FROM backfill_jobs WHERE did = $1")
                .bind(SUBJECT)
                .fetch_one(&db.pool)
                .await
                .expect("scope job");
            assert_eq!(job, "scope");
            let sets = w.scope().snapshot().await;
            assert!(sets.in_scope(SUBJECT));
            assert_eq!(sets.member_follows.get(AUTHOR), Some(&1));
            // A member's follow notifies nobody here (the subject is not a member).
            let notes: i64 = sqlx::query_scalar("SELECT count(*) FROM notifications")
                .fetch_one(&db.pool)
                .await
                .expect("query");
            assert_eq!(notes, 0);
            db.drop_db().await;
        }

        #[tokio::test]
        async fn feed_items_notifications_and_applied_rev() {
            let Some(db) = fresh_db("feed_items_notifications_and_applied_rev").await else {
                return;
            };
            let w = writer(&db.pool, true);
            let other = "did:plc:cccccccccccccccccccccccc";
            // A non-member in S replies to the member's post.
            {
                let mut s = w.scope().write_guard().await;
                s.scope.insert(other.into());
            }
            let reply = RawOp {
                action: Action::Create,
                collection: "app.bsky.feed.post".into(),
                rkey: RKEY.into(),
                cid: Some("bafyreply".into()),
                record: Some(json!({"text": "hi", "reply": {
                    "root": {"uri": format!("at://{AUTHOR}/app.bsky.feed.post/1"), "cid": "c"},
                    "parent": {"uri": format!("at://{AUTHOR}/app.bsky.feed.post/1"), "cid": "c"}}})),
            };
            let r = w.apply(other, &rev(1), &[reply], OPTIONS).await.expect("apply");
            assert_eq!(r.outcomes, [OpOutcome::Written]);
            let kind: String = sqlx::query_scalar("SELECT kind FROM feed_items")
                .fetch_one(&db.pool)
                .await
                .expect("feed item");
            assert_eq!(kind, "reply");
            let (recipient, reason): (String, String) =
                sqlx::query_as("SELECT recipient_did, reason FROM notifications")
                    .fetch_one(&db.pool)
                    .await
                    .expect("notification");
            assert_eq!((recipient.as_str(), reason.as_str()), (AUTHOR, "reply"));
            // `other` is untracked: no sync_state row.
            let rows: i64 = sqlx::query_scalar("SELECT count(*) FROM sync_state")
                .fetch_one(&db.pool)
                .await
                .expect("query");
            assert_eq!(rows, 0);

            // The member's own write advances applied_rev.
            w.apply(AUTHOR, &rev(7), &[post_op(Action::Create, "mine")], OPTIONS).await.expect("apply");
            let applied: String = sqlx::query_scalar("SELECT applied_rev FROM sync_state WHERE did = $1")
                .bind(AUTHOR)
                .fetch_one(&db.pool)
                .await
                .expect("sync_state");
            assert_eq!(applied, rev(7));

            // Deleting the reply removes its feed item and notification.
            let del = RawOp {
                action: Action::Delete,
                collection: "app.bsky.feed.post".into(),
                rkey: RKEY.into(),
                cid: None,
                record: None,
            };
            w.apply(other, &rev(2), &[del], OPTIONS).await.expect("apply");
            let left: i64 = sqlx::query_scalar(
                "SELECT (SELECT count(*) FROM notifications) + (SELECT count(*) FROM feed_items WHERE originator_did = $1)",
            )
            .bind(other)
            .fetch_one(&db.pool)
            .await
            .expect("query");
            assert_eq!(left, 0);
            db.drop_db().await;
        }

        async fn set_public(w: &Writer, mode: PublicMode, last_fill_since: Option<DateTime<Utc>>) {
            let mut sets = w.scope().write_guard().await;
            w.scope().set_public(&mut sets, PublicState { mode, generation: 1, last_fill_since });
        }

        async fn count(pool: &PgPool, sql: &str) -> i64 {
            sqlx::query_scalar(sql).fetch_one(pool).await.expect(sql)
        }

        /// T-3, T-17 (public-indexing design §4.1): under the off rule a
        /// member's reply writes its reference and grows scope under the
        /// subject's shared DID lock, and writes no post.
        #[tokio::test]
        async fn a_members_post_under_the_off_rule_is_grow_only() {
            let Some(db) = fresh_db("writer_grow_only").await else {
                return;
            };
            let p = &db.pool;
            let w = writer(p, true);
            set_public(&w, PublicMode::Off, None).await;
            let reply = |rkey: &str| RawOp {
                action: Action::Create,
                collection: "app.bsky.feed.post".into(),
                rkey: rkey.into(),
                cid: Some(format!("bafy{rkey}")),
                record: Some(json!({"text": "hi", "createdAt": "2026-09-26T00:00:00Z", "reply": {
                    "root": {"uri": format!("at://{SUBJECT}/app.bsky.feed.post/1"), "cid": "c"},
                    "parent": {"uri": format!("at://{SUBJECT}/app.bsky.feed.post/1"), "cid": "c"}}})),
            };
            // Retention's exclusive lock on the subject, held open: the
            // writer waits for it.
            let mut holder = p.begin().await.expect("begin");
            lock_did_exclusive(&mut holder, SUBJECT).await.expect("lock");
            let task = {
                let w = w.clone();
                let op = reply(RKEY);
                tokio::spawn(async move { w.apply(AUTHOR, &rev(2), &[op], OPTIONS).await })
            };
            tokio::time::sleep(Duration::from_millis(300)).await;
            assert!(!task.is_finished(), "a grow-only write waits for the subject's DID lock");
            holder.rollback().await.expect("release");
            let report = task.await.expect("join").expect("apply");
            assert_eq!(report.outcomes, [OpOutcome::GrowOnly]);

            assert_eq!(count(p, "SELECT count(*) FROM posts").await, 0);
            let r: (String, String, Option<String>, Option<String>, Option<String>) =
                sqlx::query_as("SELECT author_did, rev, reply_parent_did, reply_root_did, quote_did FROM member_post_refs")
                    .fetch_one(p)
                    .await
                    .expect("ref");
            assert_eq!(r, (AUTHOR.to_string(), rev(2), Some(SUBJECT.to_string()), Some(SUBJECT.to_string()), None));
            let reasons: i16 = sqlx::query_scalar("SELECT reasons FROM scope_dids WHERE did = $1").bind(SUBJECT).fetch_one(p).await.expect("scope");
            assert_eq!(reasons, crate::aggregations::REASON_INTERACTED);
            assert_eq!(count(p, "SELECT count(*) FROM backfill_jobs WHERE depth = 'scope'").await, 1);
            assert!(w.scope().snapshot().await.in_scope(SUBJECT));
            assert_eq!(count(p, "SELECT (SELECT count(*) FROM notifications) + (SELECT count(*) FROM feed_items)").await, 0);

            // An equal rev runs growth again, as a no-op.
            let r = w.apply(AUTHOR, &rev(2), &[reply(RKEY)], OPTIONS).await.expect("apply");
            assert_eq!(r.outcomes, [OpOutcome::GrowOnly]);
            assert_eq!(count(p, "SELECT count(*) FROM member_post_refs").await, 1);
            // A tombstoned URI writes no reference.
            sqlx::query("INSERT INTO record_tombstones (uri, rev) VALUES ($1, $2)")
                .bind(format!("at://{AUTHOR}/app.bsky.feed.post/3kgone"))
                .bind(rev(9))
                .execute(p)
                .await
                .expect("tombstone");
            let r = w.apply(AUTHOR, &rev(3), &[reply("3kgone")], OPTIONS).await.expect("apply");
            assert_eq!(r.outcomes, [OpOutcome::Unchanged]);
            assert_eq!(count(p, "SELECT count(*) FROM member_post_refs").await, 1);
            // A stranger's post is not stored at all.
            let r = w.apply(SUBJECT, &rev(1), &[reply("3kstranger")], OPTIONS).await.expect("apply");
            assert_eq!(r.outcomes, [OpOutcome::Rejected("public_off")]);
            // `requestRecord` sees the same grow-only write (T-18).
            let request = ApplyOptions { live: false, subscription_rules: false, source: WriteSource::IndexRequest, ..OPTIONS };
            let r = w.apply(AUTHOR, &rev(4), &[reply("3krequested")], request).await.expect("apply");
            assert_eq!(r.outcomes, [OpOutcome::GrowOnly]);
            assert_eq!(count(p, "SELECT count(*) FROM posts").await, 0);

            // A delete with an older rev leaves the reference; a newer one
            // deletes it (T-17).
            let delete = RawOp { action: Action::Delete, collection: "app.bsky.feed.post".into(), rkey: RKEY.into(), cid: None, record: None };
            w.apply(AUTHOR, &rev(1), std::slice::from_ref(&delete), OPTIONS).await.expect("apply");
            assert_eq!(count(p, &format!("SELECT count(*) FROM member_post_refs WHERE uri = 'at://{AUTHOR}/app.bsky.feed.post/{RKEY}'")).await, 1);
            w.apply(AUTHOR, &rev(5), &[delete], OPTIONS).await.expect("apply");
            assert_eq!(count(p, &format!("SELECT count(*) FROM member_post_refs WHERE uri = 'at://{AUTHOR}/app.bsky.feed.post/{RKEY}'")).await, 0);
            db.drop_db().await;
        }

        /// public-indexing design §4.1: in On a member's post writes its row
        /// and its reference; a non-member's writes no reference.
        #[tokio::test]
        async fn a_members_post_has_a_reference_in_every_mode() {
            let Some(db) = fresh_db("writer_post_refs_on").await else {
                return;
            };
            let p = &db.pool;
            let w = writer(p, true);
            w.apply(AUTHOR, &rev(1), &[post_op(Action::Create, "mine")], OPTIONS).await.expect("apply");
            assert_eq!(count(p, "SELECT count(*) FROM posts").await, 1);
            assert_eq!(count(p, "SELECT count(*) FROM member_post_refs").await, 1);
            // A not-newer write still keeps the reference current with its
            // own rev check, which an older rev does not pass.
            w.apply(AUTHOR, &rev(0), &[post_op(Action::Update, "older")], OPTIONS).await.expect("apply");
            let r: String = sqlx::query_scalar("SELECT rev FROM member_post_refs").fetch_one(p).await.expect("ref");
            assert_eq!(r, rev(1));
            // The same author, in S but no member.
            let other = writer(p, false);
            let theirs = RawOp {
                action: Action::Create,
                collection: "app.bsky.feed.post".into(),
                rkey: "3ktheirs".into(),
                cid: Some("bafytheirs".into()),
                record: Some(json!({"text": "theirs"})),
            };
            let r = other.apply(AUTHOR, &rev(2), &[theirs], OPTIONS).await.expect("apply");
            assert_eq!(r.outcomes, [OpOutcome::Written]);
            assert_eq!(count(p, "SELECT count(*) FROM posts").await, 2);
            assert_eq!(count(p, "SELECT count(*) FROM member_post_refs").await, 1);
            db.drop_db().await;
        }

        /// T-4 (public-indexing design §5, §8.3): what notifies, by mode,
        /// source and `createdAt` against the last fill.
        #[tokio::test]
        async fn side_effects_follow_the_mode_and_the_last_fill() {
            let Some(db) = fresh_db("writer_side_effects_by_mode").await else {
                return;
            };
            let p = &db.pool;
            let w = writer(p, true);
            let liker = "did:plc:cccccccccccccccccccccccc";
            w.scope().write_guard().await.scope.insert(liker.into());
            let firehose = OPTIONS;
            let backfill = ApplyOptions { live: false, source: WriteSource::Backfill, ..OPTIONS };
            let request = ApplyOptions { live: false, subscription_rules: false, source: WriteSource::IndexRequest, ..OPTIONS };
            let fill = Utc::now() - chrono::Duration::hours(1);
            let day_before = (fill - chrono::Duration::days(1)).to_rfc3339();
            let minute_after = (fill + chrono::Duration::minutes(1)).to_rfc3339();
            let like = |rkey: &str, created: Option<&str>| {
                let mut record = json!({"subject": {"uri": format!("at://{AUTHOR}/app.bsky.feed.post/1"), "cid": "c"}});
                if let Some(at) = created {
                    record["createdAt"] = json!(at);
                }
                RawOp { action: Action::Create, collection: "app.bsky.feed.like".into(), rkey: rkey.into(), cid: Some(format!("bafy{rkey}")), record: Some(record) }
            };
            let notified = |rkey: &'static str| async move {
                sqlx::query_scalar::<_, i64>("SELECT count(*) FROM notifications WHERE record_uri = $1")
                    .bind(format!("at://{liker}/app.bsky.feed.like/{rkey}"))
                    .fetch_one(p)
                    .await
                    .expect("count")
            };

            // Off: a stranger's follow of a member is stored and notifies
            // nobody; a member's repost writes no feed item.
            set_public(&w, PublicMode::Off, None).await;
            let follow = RawOp {
                action: Action::Create,
                collection: "app.bsky.graph.follow".into(),
                rkey: "3kfollow".into(),
                cid: Some("bafyfollow".into()),
                record: Some(json!({"subject": AUTHOR, "createdAt": Utc::now().to_rfc3339()})),
            };
            let r = w.apply(SUBJECT, &rev(1), &[follow], firehose).await.expect("apply");
            assert_eq!(r.outcomes, [OpOutcome::Written]);
            let repost = RawOp {
                action: Action::Create,
                collection: "app.bsky.feed.repost".into(),
                rkey: "3krepost".into(),
                cid: Some("bafyrepost".into()),
                record: Some(json!({"subject": {"uri": format!("at://{SUBJECT}/app.bsky.feed.post/1"), "cid": "c"}, "createdAt": Utc::now().to_rfc3339()})),
            };
            let r = w.apply(AUTHOR, &rev(1), &[repost], firehose).await.expect("apply");
            assert_eq!(r.outcomes, [OpOutcome::Written]);
            assert_eq!(count(p, "SELECT count(*) FROM follows").await + count(p, "SELECT count(*) FROM reposts").await, 2);
            assert_eq!(count(p, "SELECT (SELECT count(*) FROM notifications) + (SELECT count(*) FROM feed_items)").await, 0);

            // Filling, with last_fill_since = fill.since = T.
            set_public(&w, PublicMode::Filling, Some(fill)).await;
            w.apply(liker, &rev(1), &[like("3kold", Some(&day_before))], backfill).await.expect("apply");
            assert_eq!(notified("3kold").await, 0);
            w.apply(liker, &rev(1), &[like("3knew", Some(&minute_after))], backfill).await.expect("apply");
            assert_eq!(notified("3knew").await, 1);
            // Backfilled first, then the firehose at an equal rev: once.
            w.apply(liker, &rev(1), &[like("3kboth", Some(&minute_after))], backfill).await.expect("apply");
            let r = w.apply(liker, &rev(1), &[like("3kboth", Some(&minute_after))], firehose).await.expect("apply");
            assert_eq!(r.outcomes, [OpOutcome::Unchanged]);
            assert_eq!(notified("3kboth").await, 1);
            w.apply(liker, &rev(1), &[like("3klive", Some(&day_before))], firehose).await.expect("apply");
            w.apply(liker, &rev(1), &[like("3kasked", Some(&day_before))], request).await.expect("apply");
            assert_eq!((notified("3klive").await, notified("3kasked").await), (1, 1));

            // On after a fill.
            set_public(&w, PublicMode::On, Some(fill)).await;
            w.apply(liker, &rev(1), &[like("3konold", Some(&day_before))], backfill).await.expect("apply");
            w.apply(liker, &rev(1), &[like("3konnew", Some(&minute_after))], backfill).await.expect("apply");
            w.apply(liker, &rev(1), &[like("3konlive", Some(&day_before))], firehose).await.expect("apply");
            w.apply(liker, &rev(1), &[like("3kondate", None)], backfill).await.expect("apply");
            assert_eq!(
                (notified("3konold").await, notified("3konnew").await, notified("3konlive").await, notified("3kondate").await),
                (0, 1, 1, 1)
            );

            // On, never filled: as today.
            set_public(&w, PublicMode::On, None).await;
            w.apply(liker, &rev(1), &[like("3knever", Some(&day_before))], backfill).await.expect("apply");
            assert_eq!(notified("3knever").await, 1);
            db.drop_db().await;
        }

        #[tokio::test]
        async fn failed_op_of_a_tracked_repo_clears_readiness() {
            let Some(db) = fresh_db("failed_op_of_a_tracked_repo_clears_readiness").await else {
                return;
            };
            sqlx::query("INSERT INTO members (did, source, ready_at) VALUES ($1, 'admin', now())")
                .bind(AUTHOR)
                .execute(&db.pool)
                .await
                .expect("member");
            let w = writer(&db.pool, true);
            let bad = RawOp {
                action: Action::Create,
                collection: "app.bsky.feed.like".into(),
                rkey: RKEY.into(),
                cid: Some("bafy".into()),
                record: Some(json!({"subject": "nope"})),
            };
            let r = w.apply(AUTHOR, &rev(1), &[bad, post_op(Action::Create, "ok")], OPTIONS).await.expect("apply");
            assert_eq!(r.failed.len(), 1);
            assert_eq!(r.outcomes[1], OpOutcome::Written);
            assert!(r.readiness_cleared);
            let ready: Option<DateTime<Utc>> = sqlx::query_scalar("SELECT ready_at FROM members WHERE did = $1")
                .bind(AUTHOR)
                .fetch_one(&db.pool)
                .await
                .expect("member");
            assert_eq!(ready, None);
            let depth: String = sqlx::query_scalar("SELECT depth FROM backfill_jobs WHERE did = $1")
                .bind(AUTHOR)
                .fetch_one(&db.pool)
                .await
                .expect("job");
            assert_eq!(depth, "authz");
            db.drop_db().await;
        }

        // Firehose throughput F1 (§1.6): no transaction for a commit that
        // can write nothing.

        fn raw(action: Action, collection: &str, rkey: &str, record: Option<Value>) -> RawOp {
            RawOp {
                action,
                collection: collection.into(),
                rkey: rkey.into(),
                cid: record.as_ref().map(|_| format!("bafy{rkey}")),
                record,
            }
        }

        fn like_of(uri: &str) -> Value {
            json!({"subject": {"uri": uri, "cid": "c"}, "createdAt": "2026-09-26T00:00:00Z"})
        }

        fn follow_of(did: &str) -> Value {
            json!({"subject": did, "createdAt": "2026-09-26T00:00:00Z"})
        }

        /// Every `firehose_ops_skipped_total` series, as rendered.
        fn skipped_series(handle: &metrics_exporter_prometheus::PrometheusHandle) -> Vec<String> {
            let mut lines: Vec<String> = handle
                .render()
                .lines()
                .filter(|l| l.starts_with("firehose_ops_skipped_total{"))
                .map(str::to_string)
                .collect();
            lines.sort();
            lines
        }

        /// T-F1-a: the pool's only connection is held, so any `begin()`
        /// would wait; the call returns at once with the transaction path's
        /// report and counters.
        #[tokio::test]
        async fn nothing_to_write_opens_no_transaction() {
            let Some(db) = fresh_db("f1_nothing_to_write_opens_no_transaction").await else {
                return;
            };
            let recorder = metrics_exporter_prometheus::PrometheusBuilder::new().build_recorder();
            let handle = recorder.handle();
            let _local = metrics::set_default_local_recorder(&recorder);
            let one = sqlx::postgres::PgPoolOptions::new()
                .max_connections(1)
                .connect_with((*db.pool.connect_options()).clone())
                .await
                .expect("pool");
            let held = one.acquire().await.expect("hold the only connection");
            let w = Writer::new(one.clone(), ScopeState::new(ScopeSets::default(), ScopePolicy::MembersAndOneHop, 10_000), Arc::new(DisabledPrivateTier));
            set_public(&w, PublicMode::Off, None).await;
            let stranger = "did:plc:f1aaaaaaaaaaaaaaaaaaaaaa";
            let ops = [
                raw(Action::Create, "app.bsky.feed.post", "3kpost", Some(json!({"text": "t", "createdAt": "2026-09-26T00:00:00Z"}))),
                raw(Action::Create, "app.bsky.feed.like", "3klike", Some(like_of(&format!("at://{SUBJECT}/app.bsky.feed.post/1")))),
                raw(Action::Create, "app.bsky.graph.follow", "3kfollow", Some(follow_of(SUBJECT))),
                raw(Action::Create, "app.bsky.feed.generator", "3kgen", Some(json!({}))),
            ];
            let report = tokio::time::timeout(Duration::from_secs(1), w.apply(stranger, &rev(1), &ops, OPTIONS))
                .await
                .expect("returns without a connection")
                .expect("apply");
            assert!(!report.transaction);
            assert_eq!(
                report.outcomes,
                [
                    OpOutcome::Rejected("public_off"),
                    OpOutcome::Rejected("public_off"),
                    OpOutcome::Rejected("public_off"),
                    OpOutcome::Skipped("unindexed")
                ]
            );
            let text = handle.render();
            assert_eq!(crate::test_support::series(&text, METRIC_SKIPPED, &["reason=\"public_off\""]), Some(3.0));
            assert_eq!(crate::test_support::series(&text, METRIC_SKIPPED, &["reason=\"unindexed\""]), Some(1.0));
            drop(held);
            one.close().await;
            db.drop_db().await;
        }

        /// One case of the equivalence table.
        struct Case {
            name: &'static str,
            repo: &'static str,
            members: &'static [&'static str],
            owners: &'static [&'static str],
            scope: &'static [&'static str],
            off: bool,
            /// The repo owns a list a member subscribes to (rule 4).
            subscribed: bool,
            ops: Vec<RawOp>,
            transaction: bool,
        }

        const COUNTED: [&str; 12] = [
            "SELECT count(*) FROM posts",
            "SELECT count(*) FROM likes",
            "SELECT count(*) FROM follows",
            "SELECT count(*) FROM record_tombstones",
            "SELECT count(*) FROM member_post_refs",
            "SELECT count(*) FROM notifications",
            "SELECT count(*) FROM feed_items",
            "SELECT count(*) FROM scope_dids",
            "SELECT count(*) FROM sync_state",
            "SELECT count(*) FROM backfill_jobs",
            "SELECT count(*) FROM subscribed_list_items",
            "SELECT count(*) FROM members WHERE ready_at IS NULL",
        ];

        /// Run a case on a fresh database, normally or under
        /// `WRITER_FORCE_TX` for its repo: the report, the skip series and
        /// the row counts.
        async fn run_case(case: &Case, force: bool) -> Option<(ApplyReport, Vec<String>, Vec<i64>)> {
            use crate::subscriptions::list_phase;
            use crate::subscriptions::references::{Budget, mute_list};
            use crate::test_support::hooks;

            let db = fresh_db(&format!("f1_case_{}", case.name)).await?;
            let p = &db.pool;
            for m in case.members {
                sqlx::query("INSERT INTO members (did, source, ready_at) VALUES ($1, 'admin', now())")
                    .bind(m)
                    .execute(p)
                    .await
                    .expect("member");
            }
            let mut sets = ScopeSets::default();
            for m in case.members {
                sets.members.insert((*m).into());
                sets.scope.insert((*m).into());
            }
            for o in case.owners {
                sets.private_owners.insert((*o).into());
            }
            for s in case.scope {
                sets.scope.insert((*s).into());
            }
            let scope = ScopeState::new(sets, ScopePolicy::MembersAndOneHop, 10_000);
            if case.subscribed {
                let list = format!("at://{}/app.bsky.graph.list/3klist", case.repo);
                mute_list(p, AUTHOR, &list, Budget { max_references: 5, max_owners: 5 }).await.expect("mute");
                list_phase::apply(p, &list, 600).await.expect("apply");
                scope.refresh_subscribed(p, std::slice::from_ref(&list)).await.expect("subscribed");
            }
            let w = Writer::new(p.clone(), scope, Arc::new(DisabledPrivateTier));
            if case.off {
                set_public(&w, PublicMode::Off, None).await;
            }
            let recorder = metrics_exporter_prometheus::PrometheusBuilder::new().build_recorder();
            let handle = recorder.handle();
            let report = {
                let _local = metrics::set_default_local_recorder(&recorder);
                let _force = force.then(|| hooks::arm(hooks::WRITER_FORCE_TX, case.repo));
                w.apply(case.repo, &rev(2), &case.ops, OPTIONS).await.expect("apply")
            };
            let mut counts = Vec::new();
            for sql in COUNTED {
                counts.push(count(p, sql).await);
            }
            let skipped = skipped_series(&handle);
            db.drop_db().await;
            Some((report, skipped, counts))
        }

        /// T-F1-b: each case once normally and once on the transaction path
        /// (`WRITER_FORCE_TX`), against fresh databases: the same report,
        /// counters and rows.
        #[tokio::test]
        async fn nothing_to_write_matches_the_transaction_path() {
            const MEMBER_CASE: &str = "did:plc:f1caselllllllllllllllll";
            let post = |rkey: &str| raw(Action::Create, "app.bsky.feed.post", rkey, Some(json!({"text": "t", "createdAt": "2026-09-26T00:00:00Z"})));
            let cases = vec![
                Case { name: "out_of_scope_create", repo: "did:plc:f1caseaaaaaaaaaaaaaaaaaa", members: &[], owners: &[], scope: &[], off: false, subscribed: false, ops: vec![post("3kone")], transaction: false },
                Case { name: "unindexed_only", repo: "did:plc:f1casebbbbbbbbbbbbbbbbbb", members: &[], owners: &[], scope: &[], off: false, subscribed: false, ops: vec![raw(Action::Create, "app.bsky.feed.generator", "3kgen", Some(json!({})))], transaction: false },
                Case { name: "empty_commit", repo: "did:plc:f1casecccccccccccccccccc", members: &[], owners: &[], scope: &[], off: false, subscribed: false, ops: vec![], transaction: false },
                Case { name: "invalid_like_rkey", repo: "did:plc:f1caseddddddddddddddddddd", members: &[], owners: &[], scope: &[], off: false, subscribed: false, ops: vec![raw(Action::Create, "app.bsky.feed.like", "bad rkey!", Some(like_of("at://did:plc:x/app.bsky.feed.post/1")))], transaction: false },
                Case { name: "invalid_kryphocron_rkey", repo: "did:plc:f1caseeeeeeeeeeeeeeeeeee", members: &[], owners: &[], scope: &[], off: false, subscribed: false, ops: vec![raw(Action::Create, "tools.kryphocron.feed.postPrivate", "bad rkey!", Some(json!({})))], transaction: true },
                Case { name: "out_of_scope_delete", repo: "did:plc:f1caseffffffffffffffffff", members: &[], owners: &[], scope: &[], off: false, subscribed: false, ops: vec![raw(Action::Delete, "app.bsky.feed.post", "3kone", None)], transaction: true },
                Case {
                    name: "members_rejected_op",
                    repo: MEMBER_CASE,
                    members: &[MEMBER_CASE],
                    owners: &[],
                    scope: &[],
                    off: true,
                    subscribed: false,
                    ops: vec![raw(Action::Create, "app.bsky.feed.threadgate", "3kgate", Some(json!({"post": format!("at://{MEMBER_CASE}/app.bsky.feed.post/3kgate"), "createdAt": "2026-09-26T00:00:00Z"})))],
                    transaction: true,
                },
                Case { name: "owners_unindexed_only", repo: "did:plc:f1casegggggggggggggggggg", members: &[], owners: &["did:plc:f1casegggggggggggggggggg"], scope: &[], off: false, subscribed: false, ops: vec![raw(Action::Create, "app.bsky.feed.generator", "3kgen", Some(json!({})))], transaction: true },
                Case {
                    name: "listitem_by_subscribed_owner",
                    repo: "did:plc:f1casehhhhhhhhhhhhhhhhhh",
                    members: &[],
                    owners: &[],
                    scope: &[],
                    off: false,
                    subscribed: true,
                    ops: vec![raw(
                        Action::Create,
                        "app.bsky.graph.listitem",
                        "3kitem",
                        Some(json!({"list": "at://did:plc:f1casehhhhhhhhhhhhhhhhhh/app.bsky.graph.list/3klist", "subject": SUBJECT, "createdAt": "2026-09-26T00:00:00Z"})),
                    )],
                    transaction: true,
                },
                Case {
                    name: "grow_only_reply",
                    repo: "did:plc:f1caseiiiiiiiiiiiiiiiiii",
                    members: &["did:plc:f1caseiiiiiiiiiiiiiiiiii"],
                    owners: &[],
                    scope: &[],
                    off: true,
                    subscribed: false,
                    ops: vec![raw(Action::Create, "app.bsky.feed.post", "3kreply", Some(json!({"text": "r", "createdAt": "2026-09-26T00:00:00Z", "reply": {
                        "root": {"uri": format!("at://{SUBJECT}/app.bsky.feed.post/1"), "cid": "c"},
                        "parent": {"uri": format!("at://{SUBJECT}/app.bsky.feed.post/1"), "cid": "c"}}})))],
                    transaction: true,
                },
                Case {
                    name: "admitted_like",
                    repo: "did:plc:f1casejjjjjjjjjjjjjjjjjj",
                    members: &["did:plc:f1casekkkkkkkkkkkkkkkkkk"],
                    owners: &[],
                    scope: &[],
                    off: false,
                    subscribed: false,
                    ops: vec![raw(Action::Create, "app.bsky.feed.like", "3klike", Some(like_of("at://did:plc:f1casekkkkkkkkkkkkkkkkkk/app.bsky.feed.post/1")))],
                    transaction: true,
                },
            ];
            for case in &cases {
                let Some((normal, skipped, rows)) = run_case(case, false).await else {
                    return;
                };
                let (forced, forced_skipped, forced_rows) = run_case(case, true).await.expect("database");
                assert_eq!(normal.transaction, case.transaction, "{}", case.name);
                assert!(forced.transaction, "{}", case.name);
                assert_eq!(
                    ApplyReport { transaction: false, ..normal.clone() },
                    ApplyReport { transaction: false, ..forced },
                    "{}",
                    case.name
                );
                assert_eq!(skipped, forced_skipped, "{}", case.name);
                assert_eq!(rows, forced_rows, "{}", case.name);
                if case.name == "invalid_kryphocron_rkey" {
                    assert!(normal.readiness_cleared, "{}", case.name);
                }
            }
        }

        /// T-F1-c: a private owner (tracked, not in scope) whose follow
        /// admission rejects still takes S10's path: the row goes, a
        /// tombstone refuses the older rev, and the skip is counted once.
        #[tokio::test]
        async fn a_tracked_repos_rejected_follow_still_takes_the_s10_path() {
            let Some(db) = fresh_db("f1_rejected_follow_s10").await else {
                return;
            };
            let p = &db.pool;
            let owner = "did:plc:f1ownermmmmmmmmmmmmmmmmm";
            let mut sets = ScopeSets::default();
            sets.members.insert(SUBJECT.into());
            sets.scope.insert(SUBJECT.into());
            sets.private_owners.insert(owner.into());
            let w = Writer::new(p.clone(), ScopeState::new(sets, ScopePolicy::MembersAndOneHop, 10_000), Arc::new(DisabledPrivateTier));
            let recorder = metrics_exporter_prometheus::PrometheusBuilder::new().build_recorder();
            let handle = recorder.handle();
            let _local = metrics::set_default_local_recorder(&recorder);
            let follow = |subject: &str| raw(Action::Create, "app.bsky.graph.follow", "3kfollow", Some(follow_of(subject)));
            let uri = format!("at://{owner}/app.bsky.graph.follow/3kfollow");

            // 1. Following a member: admitted and stored.
            let r = w.apply(owner, &rev(1), &[follow(SUBJECT)], OPTIONS).await.expect("apply");
            assert_eq!(r.outcomes, [OpOutcome::Written]);
            assert_eq!(count(p, "SELECT count(*) FROM follows").await, 1);
            let skipped = |h: &metrics_exporter_prometheus::PrometheusHandle| {
                crate::test_support::series(&h.render(), METRIC_SKIPPED, &["reason=\"not_in_scope\""]).unwrap_or(0.0)
            };
            let before = skipped(&handle);

            // 2. The same URI at rev 2 names someone out of scope.
            let mut update = follow("did:plc:f1outsideoooooooooooooo");
            update.action = Action::Update;
            let r = w.apply(owner, &rev(2), &[update], OPTIONS).await.expect("apply");
            assert_eq!(r.outcomes, [OpOutcome::Rejected("not_in_scope")]);
            assert!(r.transaction);
            assert_eq!(count(p, "SELECT count(*) FROM follows").await, 0);
            let tomb: String = sqlx::query_scalar("SELECT rev FROM record_tombstones WHERE uri = $1").bind(&uri).fetch_one(p).await.expect("tombstone");
            assert_eq!(tomb, rev(2));
            let verified: Option<DateTime<Utc>> = sqlx::query_scalar("SELECT follows_verified_at FROM sync_state WHERE did = $1")
                .bind(owner)
                .fetch_one(p)
                .await
                .expect("sync_state");
            assert_eq!(verified, None);
            assert_eq!(skipped(&handle) - before, 1.0);

            // 3. Replaying rev 1 stores nothing.
            w.apply(owner, &rev(1), &[follow(SUBJECT)], OPTIONS).await.expect("apply");
            assert_eq!(count(p, "SELECT count(*) FROM follows").await, 0);
            db.drop_db().await;
        }

        /// T-F1-d: a member's commit with nothing admitted still advances
        /// both revs.
        #[tokio::test]
        async fn tracked_repo_with_nothing_admitted_still_advances_firehose_rev() {
            let Some(db) = fresh_db("f1_tracked_advances_firehose_rev").await else {
                return;
            };
            let w = writer(&db.pool, true);
            let r = w
                .apply(AUTHOR, &rev(7), &[raw(Action::Create, "app.bsky.feed.generator", "3kgen", Some(json!({})))], OPTIONS)
                .await
                .expect("apply");
            assert!(r.transaction);
            let revs: (Option<String>, Option<String>) = sqlx::query_as("SELECT applied_rev, firehose_rev FROM sync_state WHERE did = $1")
                .bind(AUTHOR)
                .fetch_one(&db.pool)
                .await
                .expect("sync_state");
            assert_eq!(revs, (Some(rev(7)), Some(rev(7))));
            db.drop_db().await;
        }

        /// T-F1-e: an untracked repo's invalid kryphocron op clears its
        /// readiness and enqueues `authz`, in a transaction.
        #[tokio::test]
        async fn kryphocron_invalid_op_still_clears_readiness() {
            let Some(db) = fresh_db("f1_kryphocron_invalid_clears").await else {
                return;
            };
            let w = writer(&db.pool, false);
            let stranger = "did:plc:f1kryphocronnnnnnnnnnnnn";
            let r = w
                .apply(stranger, &rev(1), &[raw(Action::Create, "tools.kryphocron.feed.postPrivate", "bad rkey!", Some(json!({})))], OPTIONS)
                .await
                .expect("apply");
            assert!(r.transaction);
            assert!(r.readiness_cleared);
            assert_eq!(r.failed.len(), 1);
            let depth: String = sqlx::query_scalar("SELECT depth FROM backfill_jobs WHERE did = $1")
                .bind(stranger)
                .fetch_one(&db.pool)
                .await
                .expect("job");
            assert_eq!(depth, "authz");
            db.drop_db().await;
        }
    }
}
