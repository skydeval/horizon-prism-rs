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
use crate::aggregations::growth::{self, GrowthPlan};
use crate::aggregations::{Admission, AdmissionInput, RecordKind, ScopeSets, ScopeState, admit};
use crate::db::locks::{lock_dids_shared, lock_uris, uri_author};
use crate::private_tier::{PrivateOp, PrivateTier};

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
        Ok(u) => Prepared::Upsert(Box::new(u)),
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
) -> Result<Upsert, RecordError> {
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
        RecordClass::Ecosystem(EcosystemKind::Profile) => (Body::Profile(ProfileRecord::parse(record)), Vec::new()),
        RecordClass::Kryphocron { kind: Some(k), .. } => (Body::Private, private_subjects(k, record)?),
        RecordClass::Kryphocron { kind: None, .. } | RecordClass::Unindexed => {
            return Err(RecordError("not an indexed class".to_string()));
        }
    };
    Ok(Upsert {
        uri,
        rkey: op.rkey.clone(),
        cid: cid.to_string(),
        collection: op.collection.clone(),
        body,
        input: AdmissionInput {
            author: repo.to_string(),
            kind,
            subjects,
        },
        record: normalize_record(record),
    })
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
        Body::Block(_) | Body::Profile(_) | Body::Private => {}
    }
    out
}

/// The writer.
#[derive(Clone)]
pub struct Writer {
    pool: PgPool,
    scope: ScopeState,
    private: Arc<dyn PrivateTier>,
}

impl Writer {
    #[must_use]
    pub fn new(pool: PgPool, scope: ScopeState, private: Arc<dyn PrivateTier>) -> Self {
        Self { pool, scope, private }
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
        let lock_uri_list: Vec<String> = prepared.iter().filter_map(|p| p.uri().map(str::to_string)).collect();

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
                        self.run_with_attempts(&mut tx, options.attempts, target, repo, rev, sets, &mut effects)
                            .await?
                    }
                },
                Prepared::Delete(d) => {
                    self.run_with_attempts(&mut tx, options.attempts, OpTarget::Delete(d), repo, rev, sets, &mut effects)
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

    /// Run one op in savepoints until it succeeds, fails transiently (abort)
    /// or exhausts its deterministic attempts.
    #[allow(clippy::too_many_arguments)]
    async fn run_with_attempts(
        &self,
        tx: &mut PgConnection,
        attempts: u32,
        target: OpTarget<'_>,
        repo: &str,
        rev: &str,
        sets: &ScopeSets,
        effects: &mut MemoryEffects,
    ) -> Result<OpOutcome, Transient> {
        let mut last_error = String::new();
        for _ in 0..attempts.max(1) {
            let mut sp = tx.begin().await?;
            let result = match target {
                OpTarget::Upsert(u, plan) => self.apply_upsert(&mut sp, u, plan, rev, sets).await,
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
            RecordKind::Post => delete_row(conn, "posts", &d.uri, rev).await?,
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
           embed, facets, mention_dids, self_labels, record, created_at) \
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18) \
         ON CONFLICT (uri) DO UPDATE SET author_did = EXCLUDED.author_did, rkey = EXCLUDED.rkey, \
           cid = EXCLUDED.cid, rev = EXCLUDED.rev, text = EXCLUDED.text, langs = EXCLUDED.langs, \
           reply_root_uri = EXCLUDED.reply_root_uri, reply_root_cid = EXCLUDED.reply_root_cid, \
           reply_parent_uri = EXCLUDED.reply_parent_uri, reply_parent_cid = EXCLUDED.reply_parent_cid, \
           quote_uri = EXCLUDED.quote_uri, embed = EXCLUDED.embed, facets = EXCLUDED.facets, \
           mention_dids = EXCLUDED.mention_dids, self_labels = EXCLUDED.self_labels, \
           record = EXCLUDED.record, created_at = EXCLUDED.created_at \
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
        "INSERT INTO {table} (uri, author_did, subject_uri, subject_cid, rev, created_at) \
         VALUES ($1, $2, $3, $4, $5, $6) \
         ON CONFLICT (uri) DO UPDATE SET author_did = EXCLUDED.author_did, subject_uri = EXCLUDED.subject_uri, \
           subject_cid = EXCLUDED.subject_cid, rev = EXCLUDED.rev, created_at = EXCLUDED.created_at \
         WHERE {table}.rev < EXCLUDED.rev"
    );
    let n = sqlx::query(&sql)
        .bind(&u.uri)
        .bind(&u.input.author)
        .bind(&s.subject.uri)
        .bind(s.subject.cid.as_deref())
        .bind(rev)
        .bind(s.created_at)
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
    let sql = format!(
        "INSERT INTO {table} (uri, author_did, subject_did, rev, created_at) VALUES ($1, $2, $3, $4, $5) \
         ON CONFLICT (uri) DO UPDATE SET author_did = EXCLUDED.author_did, subject_did = EXCLUDED.subject_did, \
           rev = EXCLUDED.rev, created_at = EXCLUDED.created_at \
         WHERE {table}.rev < EXCLUDED.rev \
         RETURNING (xmax = 0)"
    );
    sqlx::query_scalar(&sql)
        .bind(&u.uri)
        .bind(&u.input.author)
        .bind(&g.subject_did)
        .bind(rev)
        .bind(g.created_at)
        .fetch_optional(conn)
        .await
}

async fn upsert_profile(
    conn: &mut PgConnection,
    u: &Upsert,
    p: &ProfileRecord,
    rev: &str,
) -> Result<bool, sqlx::Error> {
    let n = sqlx::query(
        "INSERT INTO actors (did, display_name, description, avatar_cid, banner_cid, profile_rev, profile_record) \
         VALUES ($1, $2, $3, $4, $5, $6, $7) \
         ON CONFLICT (did) DO UPDATE SET display_name = EXCLUDED.display_name, \
           description = EXCLUDED.description, avatar_cid = EXCLUDED.avatar_cid, \
           banner_cid = EXCLUDED.banner_cid, profile_rev = EXCLUDED.profile_rev, \
           profile_record = EXCLUDED.profile_record \
         WHERE actors.profile_rev IS NULL OR actors.profile_rev < EXCLUDED.profile_rev",
    )
    .bind(&u.input.author)
    .bind(p.display_name.as_deref())
    .bind(p.description.as_deref())
    .bind(p.avatar_cid.as_deref())
    .bind(p.banner_cid.as_deref())
    .bind(rev)
    .bind(&u.record)
    .execute(conn)
    .await?
    .rows_affected();
    Ok(n > 0)
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
           banner_cid = NULL, profile_record = NULL, profile_rev = $2 \
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
            prepare("did:plc:a", &op(Action::Create, "app.bsky.graph.list", "3k", Some(json!({}))), now),
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
