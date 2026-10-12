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

use super::classifier::{EcosystemKind, RecordClass, classify, skip_reason};
use super::jobs::{self, Depth};
use super::normalize::normalize_record;
use super::records::{GraphRecord, PostRecord, ProfileRecord, RecordError, SubjectRecord, private_subjects};
use super::rows::{self, Cell, Row};
use crate::aggregations::growth::{self, GrowthPlan};
use crate::aggregations::{Admission, AdmissionInput, RecordKind, ScopeSets, ScopeState, admit};
use crate::db::locks::{lock_dids_shared, lock_uris, uri_author};
use crate::private_tier::{IngestError, PrivateOp, PrivateTier};

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
}

/// What happened to one op.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OpOutcome {
    Written,
    /// LWW kept the existing state (older or equal rev, or tombstoned).
    Unchanged,
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
}

/// The op a savepoint attempt runs.
#[derive(Clone, Copy)]
enum OpTarget<'a> {
    Upsert(&'a Upsert, &'a GrowthPlan),
    Delete(&'a Delete),
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

#[allow(clippy::too_many_arguments)]
fn build_upsert(
    repo: &str,
    class: RecordClass,
    kind: RecordKind,
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
            Some(row) => (Body::Row(row), Vec::new()),
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
        input: AdmissionInput {
            author: repo.to_string(),
            kind,
            subjects,
        },
        record: normalized,
    }))
}

/// The canonical gates of a post: the threadgate and postgate in the post
/// author's repo with the post's rkey (design-v3 §3.2). Empty when `post_uri`
/// is not a post URI.
fn canonical_gates(post_uri: &str) -> Vec<String> {
    let Some((prefix, rkey)) = post_uri.rsplit_once('/') else {
        return Vec::new();
    };
    let Some(repo) = prefix.strip_suffix(super::classifier::NSID_POST).and_then(|p| p.strip_suffix('/')) else {
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
}

impl MemoryEffects {
    fn merge(&mut self, other: Self) {
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
/// with reply taking precedence over quote, and quote over mention.
fn notifications_for(author: &str, body: &Body, sets: &ScopeSets) -> Vec<Notification> {
    let mut out: Vec<Notification> = Vec::new();
    let mut push = |recipient: Option<&str>, reason: &'static str, subject: Option<String>| {
        if let Some(r) = recipient
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
        Body::Block(_) | Body::Profile(_) | Body::Row(_) | Body::Private => {}
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
}

impl Writer {
    #[must_use]
    pub fn new(pool: PgPool, scope: ScopeState, private: Arc<dyn PrivateTier>) -> Self {
        Self {
            pool,
            scope,
            private,
            subscribed_post_max_age: chrono::Duration::seconds(86_400),
        }
    }

    /// How old a post may be and still be announced to its author's
    /// subscribers (`notifications.subscribed_post_max_age_secs`).
    #[must_use]
    pub fn with_subscribed_post_max_age_secs(mut self, secs: u32) -> Self {
        self.subscribed_post_max_age = chrono::Duration::seconds(i64::from(secs));
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
                let plan = if a.is_admitted() {
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
        for p in &prepared {
            if let Prepared::Delete(d) = p
                && d.kind == RecordKind::Post
            {
                lock_uri_list.extend(canonical_gates(&d.uri));
            }
        }

        let mut tx = self.pool.begin().await?;
        // 2 and 3: DID locks, then per-URI locks.
        lock_dids_shared(&mut tx, &lock_dids).await?;
        lock_uris(&mut tx, &lock_uri_list).await?;

        let mut report = ApplyReport::default();
        let mut effects = MemoryEffects::default();
        for (i, (p, op)) in prepared.iter().zip(ops).enumerate() {
            let outcome = match p {
                Prepared::Skip(reason) => {
                    metrics::counter!(METRIC_SKIPPED, "reason" => *reason).increment(1);
                    OpOutcome::Skipped(reason)
                }
                Prepared::Invalid(e) => OpOutcome::Failed(e.clone()),
                Prepared::Upsert(u) => match admissions[i] {
                    Some(Admission::Reject(reason)) => {
                        metrics::counter!(METRIC_SKIPPED, "reason" => reason.as_str()).increment(1);
                        OpOutcome::Rejected(reason.as_str())
                    }
                    _ => {
                        let target = OpTarget::Upsert(u, &plans[i]);
                        self.run_with_attempts(&mut tx, options, target, repo, rev, sets, &mut effects)
                            .await?
                    }
                },
                Prepared::Delete(d) => {
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
            advance_applied_rev(&mut tx, repo, rev).await?;
        }
        tx.commit().await?;
        drop(guard);
        self.commit_effects(effects).await;
        Ok(report)
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
            match admit(&input, sets) {
                Admission::Admit(_) => {
                    dids.push(did.clone());
                    uris.push(format!("at://{did}/{}/{rkey}", super::classifier::NSID_BLOCK));
                }
                Admission::Reject(reason) => {
                    metrics::counter!(METRIC_SKIPPED, "reason" => reason.as_str()).increment(1);
                }
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
                OpTarget::Upsert(u, plan) => self.apply_upsert(&mut sp, u, plan, rev, sets, options.live).await,
                OpTarget::Delete(d) => self.apply_delete(&mut sp, d, repo, rev, sets).await,
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

    async fn apply_upsert(
        &self,
        conn: &mut PgConnection,
        u: &Upsert,
        plan: &GrowthPlan,
        rev: &str,
        sets: &ScopeSets,
        live: bool,
    ) -> Result<(OpOutcome, MemoryEffects), OpError> {
        let mut eff = MemoryEffects::default();
        let author = u.input.author.as_str();
        if u.body == Body::Private {
            let applied = self
                .private
                .apply(
                    conn,
                    &PrivateOp {
                        uri: u.uri.clone(),
                        author_did: author.to_string(),
                        nsid: u.collection.clone(),
                        rkey: u.rkey.clone(),
                        cid: u.cid.clone(),
                        rev: rev.to_string(),
                        record: u.record.clone(),
                    },
                )
                .await?;
            eff.owners_joined = applied.owners_joined;
            eff.owners_left = applied.owners_left;
            let outcome = if applied.changed { OpOutcome::Written } else { OpOutcome::Unchanged };
            return Ok((outcome, eff));
        }
        if tombstoned(conn, &u.uri, rev).await? {
            metrics::counter!(METRIC_LWW_SKIPPED, "reason" => "tombstoned").increment(1);
            return Ok((OpOutcome::Unchanged, eff));
        }
        let written = match &u.body {
            Body::Post(p) => upsert_post(conn, u, p, rev).await?,
            Body::Like(s) => upsert_subject(conn, "likes", u, s, rev).await?,
            Body::Repost(s) => upsert_subject(conn, "reposts", u, s, rev).await?,
            Body::Follow(g) => {
                let inserted = upsert_graph(conn, "follows", u, g, rev).await?;
                if inserted == Some(true) && sets.is_member(author) {
                    eff.new_member_follow = Some(author.to_string());
                }
                inserted.is_some()
            }
            Body::Block(g) => upsert_graph(conn, "blocks", u, g, rev).await?.is_some(),
            Body::Profile(p) => upsert_profile(conn, u, p, rev).await?,
            Body::Row(row) => upsert_row(conn, u, row, rev).await?,
            Body::Private => false,
        };
        if !written {
            metrics::counter!(METRIC_LWW_SKIPPED, "reason" => "older_rev").increment(1);
            return Ok((OpOutcome::Unchanged, eff));
        }
        // §5.12: feed items only for public records whose author is in S.
        if sets.in_scope(author) {
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
        for n in notifications_for(author, &u.body, sets) {
            insert_notification(conn, &n, author, &u.uri, &u.cid).await?;
        }
        // After the reasons above, which take precedence (§8.6).
        match &u.body {
            Body::Like(s) | Body::Repost(s) => {
                let reason = if matches!(u.body, Body::Like(_)) { "like-via-repost" } else { "repost-via-repost" };
                if let Some(via) = &s.via
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
                    insert_notification(conn, &n, author, &u.uri, &u.cid).await?;
                }
            }
            _ => {}
        }
        if live
            && let Body::Post(p) = &u.body
            && sets.in_scope(author)
            && Utc::now().signed_duration_since(p.created_at) <= self.subscribed_post_max_age
        {
            insert_subscribed_post(conn, author, &u.uri, &u.cid, p.reply_parent.is_some(), p.created_at).await?;
        }
        eff.growth_added = growth::apply(conn, plan).await?;
        Ok((OpOutcome::Written, eff))
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
            RecordKind::PrivatePost | RecordKind::PrivateAudience | RecordKind::PrivateBlock => {
                let applied = self.private.delete(conn, &d.uri, rev, author_in_scope).await?;
                eff.owners_joined = applied.owners_joined;
                eff.owners_left = applied.owners_left;
                let outcome = if applied.changed { OpOutcome::Written } else { OpOutcome::Unchanged };
                return Ok((outcome, eff));
            }
            RecordKind::Profile => delete_profile(conn, repo, rev).await?,
            RecordKind::Post => {
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
                let removed = delete_row(conn, "follows", &d.uri, rev).await?;
                if removed && sets.is_member(repo) {
                    eff.member_follow_removed = Some(repo.to_string());
                }
                removed
            }
            RecordKind::Block => delete_row(conn, "blocks", &d.uri, rev).await?,
            RecordKind::Table(table) => delete_row(conn, table, &d.uri, rev).await?,
        };
        // Tombstone iff a row was removed, or the author is in S (§5.5).
        if removed || author_in_scope {
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
        Ok((if removed { OpOutcome::Written } else { OpOutcome::Unchanged }, eff))
    }

    /// Apply post-commit in-memory changes under a brief write guard.
    async fn commit_effects(&self, eff: MemoryEffects) {
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

async fn insert_notification(
    conn: &mut PgConnection,
    n: &Notification,
    author: &str,
    record_uri: &str,
    record_cid: &str,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        "INSERT INTO notifications (recipient_did, author_did, reason, reason_subject, record_uri, record_cid) \
         VALUES ($1, $2, $3, $4, $5, $6) ON CONFLICT DO NOTHING",
    )
    .bind(&n.recipient)
    .bind(author)
    .bind(n.reason)
    .bind(n.reason_subject.as_deref())
    .bind(record_uri)
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
/// - the subscription is older than the post;
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
           AND s.created_at < $5 \
           AND EXISTS (SELECT 1 FROM members m WHERE m.did = s.subscriber_did) \
           AND EXISTS (SELECT 1 FROM sync_state y WHERE y.did = $1 AND y.last_resync_at IS NOT NULL) \
           AND d.allow IN ('followers', 'mutuals') \
           AND EXISTS (SELECT 1 FROM follows f WHERE f.author_did = s.subscriber_did AND f.subject_did = $1) \
           AND (d.allow <> 'mutuals' \
                OR EXISTS (SELECT 1 FROM follows f WHERE f.author_did = $1 AND f.subject_did = s.subscriber_did)) \
           AND NOT EXISTS (SELECT 1 FROM notifications n WHERE n.recipient_did = s.subscriber_did AND n.record_uri = $2) \
         ON CONFLICT DO NOTHING",
    )
    .bind(author)
    .bind(record_uri)
    .bind(record_cid)
    .bind(reply)
    .bind(created_at)
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
    jobs::enqueue(&mut *conn, did, Depth::Authz).await
}

/// `applied_rev = GREATEST(applied_rev, rev)` (§6.11.3).
///
/// # Errors
///
/// Any database error.
pub async fn advance_applied_rev(conn: &mut PgConnection, did: &str, rev: &str) -> Result<(), sqlx::Error> {
    sqlx::query(
        "INSERT INTO sync_state (did, applied_rev) VALUES ($1, $2) \
         ON CONFLICT (did) DO UPDATE SET applied_rev = GREATEST(sync_state.applied_rev, EXCLUDED.applied_rev)",
    )
    .bind(did)
    .bind(rev)
    .execute(conn)
    .await?;
    Ok(())
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

    /// Postgres-backed tests (see `crate::test_support`).
    mod pg {
        use std::time::Duration;

        use super::*;
        use crate::config::ScopePolicy;
        use crate::db::locks::lock_did_exclusive;
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
            let backfill = ApplyOptions { live: false, ..OPTIONS };
            w.apply(liker, &rev(3), &[profile("first", Action::Create)], backfill).await.expect("apply");
            assert_eq!(joined().await, 0);
            w.apply(liker, &rev(4), &[profile("edited", Action::Update)], OPTIONS).await.expect("apply");
            assert_eq!(joined().await, 0);
            w.apply(liker, &rev(5), &[profile("again", Action::Create)], OPTIONS).await.expect("apply");
            assert_eq!(joined().await, 1);
            db.drop_db().await;
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
            let backfill = ApplyOptions { live: false, ..OPTIONS };

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
    }
}
