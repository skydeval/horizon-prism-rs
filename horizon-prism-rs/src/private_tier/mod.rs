//! The private tier (design-v2 §3.2, §4.5, §6.3, §6.4, §7.2, §7.9): the only
//! module that touches the private tables or decodes private content.
//!
//! - Internals (`schema`, `oracle`, `pipeline`, `emit`) are private to this
//!   module; their items are `pub(super)`, so nothing outside `private_tier`
//!   can name a private-table query, a `RecordAuthz` or a `PrivateEmit`.
//! - The `pub(crate)` surface is `ingest`, `readiness`, `owners`, `handlers`
//!   and `retention`. The rest of the crate reaches ingest, readiness, owner
//!   and purge operations through the [`PrivateTier`] facade, whose concrete
//!   implementation is [`KryphocronPrivateTier`].
//! - Content-returning queries take `&RecordAuthz` only (no URI parameter),
//!   and a private row reaches a response only as a `PrivateEmit` built from
//!   the same witness.
//!
//! Sealed: outside this module, neither the content queries nor the
//! witness nor the emit types can be named.
//!
//! ```compile_fail
//! use horizon_prism_rs::private_tier::schema::encoded;
//! ```
//!
//! ```compile_fail
//! use horizon_prism_rs::private_tier::pipeline::RecordAuthz;
//! ```
//!
//! ```compile_fail
//! use horizon_prism_rs::private_tier::emit::PrivateEmit;
//! ```

mod cursor;
mod emit;
pub(crate) mod handlers;
pub(crate) mod ingest;
mod oracle;
pub(crate) mod owners;
mod pipeline;
pub(crate) mod readiness;
pub(crate) mod retention;
mod schema;
#[cfg(test)]
mod tests;

use std::collections::{HashMap, HashSet};

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use sqlx::{PgConnection, PgPool};

use crate::firehose::writer::WriteSource;

/// A private-tier record op that passed admission (§9.3).
#[derive(Debug, Clone, PartialEq)]
pub struct PrivateOp {
    pub uri: String,
    pub author_did: String,
    /// `tools.kryphocron.feed.postPrivate`, `tools.kryphocron.policy.audience`
    /// or `tools.kryphocron.graph.block`.
    pub nsid: String,
    pub rkey: String,
    pub cid: String,
    /// Commit rev (§5.5 LWW).
    pub rev: String,
    /// The record as received; `firehose::normalize` helpers decode its fields.
    pub record: serde_json::Value,
    /// The write path (private posting §4.8): only the live firehose
    /// notifies for a record of any age.
    pub source: WriteSource,
    /// The record's `createdAt` as `records::created_at` parses it.
    pub created_at: DateTime<Utc>,
    /// `private_notify.backfill_max_age_hours` (C-11): a write that is not
    /// live notifies only for a record created within it.
    pub notify_max_age: chrono::Duration,
}

/// Whether a write notifies (private posting §4.8, C-11): a live firehose
/// write always does; any other source (backfill, catch-up, `requestRecord`,
/// dead-letter replay) only for a record created within `max_age`.
#[must_use]
pub fn notifies(source: WriteSource, created_at: DateTime<Utc>, max_age: chrono::Duration) -> bool {
    source == WriteSource::Firehose || Utc::now().signed_duration_since(created_at) <= max_age
}

/// A public record naming a private post (private posting §4.8), as the
/// writer hands it to [`PrivateTier::notify_public`]: a like or repost of a
/// private post, or a public post replying to or quoting one.
#[derive(Debug, Clone, PartialEq)]
pub struct PublicNotify {
    /// The like, repost or `app.bsky.feed.post`.
    pub record_uri: String,
    pub record_cid: String,
    pub author_did: String,
    /// `(reason, subject)` in precedence order, each subject a `postPrivate`
    /// URI: `like` or `repost`; or `reply` (parent, then root) and `quote`.
    /// The recipient is the subject's author.
    pub reasons: Vec<(&'static str, String)>,
    /// The thread root the post claims, for thread mutes.
    pub record_root: Option<String>,
    /// A post's record, kept with the row: under the off rule no `posts`
    /// row exists to rebuild it from. `None` for a like or repost.
    pub public_record: Option<serde_json::Value>,
    pub source: WriteSource,
    pub created_at: DateTime<Utc>,
    pub notify_max_age: chrono::Duration,
}

/// A member's public post and the private post it quotes (private posting
/// §4.7), as the writer hands it to [`PrivateTier::upsert_quote_ref`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QuoteRef {
    /// The quoting `app.bsky.feed.post`.
    pub uri: String,
    pub author_did: String,
    pub cid: String,
    /// Commit rev (LWW).
    pub rev: String,
    /// The `postPrivate` URI it quotes; `None` when it quotes none.
    pub quoted_uri: Option<String>,
    pub created_at: DateTime<Utc>,
}

/// What an ingest call changed.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Applied {
    /// A row was inserted, updated or deleted.
    pub changed: bool,
    /// DIDs that are private owners after this transaction (§6.11.1). The
    /// caller adds them to the in-memory owner set after commit (idempotent).
    pub owners_joined: Vec<String>,
    /// DIDs that are not private owners after this transaction.
    pub owners_left: Vec<String>,
}

/// An ingest failure.
#[derive(Debug, thiserror::Error)]
pub enum IngestError {
    #[error(transparent)]
    Db(#[from] sqlx::Error),
    /// A field Aurora-Locus did not validate on write is malformed (#235).
    /// Deterministic: the op is dead-lettered by the firehose discipline.
    #[error("malformed private record: {0}")]
    Malformed(String),
}

/// The `features` strings `app.nearhorizon.actor.getMembership` lists
/// (private posting §4.1): one per private family this build serves. Each
/// slice appends its own string when it ships, in the order of §4.1's table.
pub const FEATURES: &[&str] = &[
    // The private read methods (served before S1).
    "private",
    // P-1 and P-3 fields on views (S2).
    "private.embeds",
    // P-8 bookmarks and `viewer.bookmarked` (S3).
    "private.bookmarks",
    // P-18 threadgates and postgates on private posts (S4).
    "private.gates",
    // P-2 counts and viewer state, P-5 likes, reposts, quotes and repost
    // reasons, P-16 thread mutes (S5).
    "private.interactions",
    // P-9 private notifications (S6).
    "private.notifications",
    // P-11 `requestRecord` catch-up and `caughtUp` (S7).
    "private.freshIndex",
    // P-17 sealed cursors, P-20 `filter`, P-6/P-14 `getReplies` (S8).
    "private.paging",
    // Rule 4's anchored walk (S0, listed from S1).
    "private.crossPdsReplies",
    // P-7 `searchPosts`, a bounded scan at query time (S11).
    "private.search",
];

/// The private tier as the rest of the crate may use it.
#[async_trait]
pub trait PrivateTier: Send + Sync {
    /// The `features` this tier serves: [`FEATURES`], or none when the
    /// tier is disabled.
    fn features(&self) -> &'static [&'static str];

    /// `owners::load`: the private-owner set for `ScopeState` (§9.3 rule 3).
    /// DIDs only.
    async fn load_owners(&self, pool: &PgPool) -> Result<HashSet<String>, sqlx::Error>;

    /// `ingest::apply`: LWW create/update of an admitted private record, with
    /// owner re-evaluation and `private` job enqueue (§5.5, §6.11.1).
    async fn apply(&self, conn: &mut PgConnection, op: &PrivateOp) -> Result<Applied, IngestError>;

    /// `ingest::delete`: LWW delete (§5.5). `author_in_scope` feeds the
    /// tombstone rule.
    async fn delete(
        &self,
        conn: &mut PgConnection,
        uri: &str,
        rev: &str,
        author_in_scope: bool,
    ) -> Result<Applied, sqlx::Error>;

    /// `ingest::apply_gate` (private posting §4.4): LWW create/update of an
    /// admitted threadgate or postgate whose `post` is a `postPrivate` URI.
    async fn apply_gate(&self, conn: &mut PgConnection, op: &PrivateOp) -> Result<Applied, IngestError>;

    /// `ingest::delete_gate`: the private copy of a deleted threadgate or
    /// postgate, if any (a delete does not say which post kind it gated).
    async fn delete_gate(
        &self,
        conn: &mut PgConnection,
        uri: &str,
        rev: &str,
        author_in_scope: bool,
    ) -> Result<Applied, sqlx::Error>;

    /// The record keys of `did`'s stored private posts: backfill keeps the
    /// gates of these (private posting §4.4).
    async fn post_rkeys(&self, pool: &PgPool, did: &str) -> Result<HashSet<String>, sqlx::Error>;

    /// `ingest::upsert_quote_ref` (private posting §4.7): a member's public
    /// post at `quote.uri` that quotes the `postPrivate` post
    /// `quote.quoted_uri`, last writer winning on rev. `quoted_uri` `None`
    /// (the post quotes no private post, or no longer does) removes an
    /// older reference.
    async fn upsert_quote_ref(&self, conn: &mut PgConnection, quote: &QuoteRef) -> Result<(), sqlx::Error>;

    /// `ingest::delete_quote_ref`: a public post's delete takes its quote
    /// reference with it, by rev, as `member_post_refs`.
    async fn delete_quote_ref(&self, conn: &mut PgConnection, uri: &str, rev: &str) -> Result<(), sqlx::Error>;

    /// `ingest::notify_public` (private posting §4.8): the private
    /// notifications of a member's public record naming a private post, on
    /// the op's own connection, in every public-indexing mode. Each
    /// recipient is a member other than the author.
    async fn notify_public(&self, conn: &mut PgConnection, n: &PublicNotify) -> Result<(), sqlx::Error>;

    /// `ingest::delete_notifications`: the private notifications of a
    /// deleted public record, on the op's own connection and savepoint.
    async fn delete_notifications(&self, conn: &mut PgConnection, record_uri: &str) -> Result<(), sqlx::Error>;

    /// `readiness::clear_all`: the gap transaction's owner-readiness wipe (§6.11.4).
    async fn clear_all_readiness(&self, conn: &mut PgConnection) -> Result<(), sqlx::Error>;

    /// Clear one DID's owner readiness (dead letter, divergence, purge marker).
    async fn clear_owner_readiness(&self, conn: &mut PgConnection, did: &str) -> Result<(), sqlx::Error>;

    /// `readiness::mark_owner_ready` (§5.10). A no-op for a non-owner.
    async fn mark_owner_ready(&self, conn: &mut PgConnection, did: &str) -> Result<(), sqlx::Error>;

    /// Whether `did`'s owner readiness is set, on the caller's connection
    /// (private posting §3.8: catch-up's checks and settle).
    async fn owner_ready(&self, conn: &mut PgConnection, did: &str) -> Result<bool, sqlx::Error>;

    /// URIs of `did`'s local rows in `nsid` with `rev < below_rev`: the
    /// reconcile candidates for a private collection (§5.10).
    async fn local_uris(
        &self,
        conn: &mut PgConnection,
        did: &str,
        nsid: &str,
        below_rev: &str,
    ) -> Result<Vec<String>, sqlx::Error>;

    /// URI → CID of `did`'s stored rows in `nsid` where the row keeps a CID
    /// (private posts and audiences); empty for any other collection.
    /// Catch-up skips a listed record whose CID is stored (private posting
    /// §3.8).
    async fn stored_cids(&self, conn: &mut PgConnection, did: &str, nsid: &str) -> Result<HashMap<String, String>, sqlx::Error>;

    /// `ingest::purge_did` (account deletion), at most `limit` rows. Returns
    /// the rows deleted; the caller repeats until 0.
    async fn purge_did_batch(&self, conn: &mut PgConnection, did: &str, limit: i64) -> Result<u64, sqlx::Error>;

    /// `ingest::purge_out_of_scope`: the DID's own private posts and audiences,
    /// except class-M replies to members (§9.4), at most `limit` rows.
    async fn purge_out_of_scope_batch(&self, conn: &mut PgConnection, did: &str, limit: i64)
    -> Result<u64, sqlx::Error>;

    /// The private part of a `member_removed` purge: class-M private rows
    /// admitted only because `did` was a member (§6.11.7), at most `limit`.
    async fn purge_member_removed_batch(
        &self,
        conn: &mut PgConnection,
        did: &str,
        limit: i64,
    ) -> Result<u64, sqlx::Error>;

    /// `owners::reevaluate` (§6.11.1).
    async fn reevaluate_owner(&self, conn: &mut PgConnection, did: &str) -> Result<Applied, sqlx::Error>;

    /// Record an eligibility confirmation (`confirmed`) or its failure, which
    /// removes eligibility (§6.11.1).
    async fn record_eligibility(&self, conn: &mut PgConnection, did: &str, confirmed: bool) -> Result<(), sqlx::Error>;

    /// When the DID's community-PDS eligibility was last confirmed.
    async fn eligibility_confirmed_at(&self, conn: &mut PgConnection, did: &str)
    -> Result<Option<DateTime<Utc>>, sqlx::Error>;

    /// `ingest::enqueue_display_refill` (private posting §3.1): once, under
    /// the `worker_state` key `marker`, a `private` job for every DID with
    /// rows stored before migration 0023. `None` when already done.
    async fn enqueue_display_refill(&self, pool: &PgPool, marker: &str) -> Result<Option<u64>, sqlx::Error>;
}

/// The private tier.
#[derive(Debug, Clone, Copy, Default)]
pub struct KryphocronPrivateTier;

#[async_trait]
impl PrivateTier for KryphocronPrivateTier {
    fn features(&self) -> &'static [&'static str] {
        FEATURES
    }

    async fn load_owners(&self, pool: &PgPool) -> Result<HashSet<String>, sqlx::Error> {
        owners::load(pool).await
    }

    async fn apply(&self, conn: &mut PgConnection, op: &PrivateOp) -> Result<Applied, IngestError> {
        ingest::apply(conn, op).await
    }

    async fn delete(
        &self,
        conn: &mut PgConnection,
        uri: &str,
        rev: &str,
        author_in_scope: bool,
    ) -> Result<Applied, sqlx::Error> {
        ingest::delete(conn, uri, rev, author_in_scope).await
    }

    async fn apply_gate(&self, conn: &mut PgConnection, op: &PrivateOp) -> Result<Applied, IngestError> {
        ingest::apply_gate(conn, op).await
    }

    async fn delete_gate(
        &self,
        conn: &mut PgConnection,
        uri: &str,
        rev: &str,
        author_in_scope: bool,
    ) -> Result<Applied, sqlx::Error> {
        ingest::delete_gate(conn, uri, rev, author_in_scope).await
    }

    async fn post_rkeys(&self, pool: &PgPool, did: &str) -> Result<HashSet<String>, sqlx::Error> {
        ingest::post_rkeys(pool, did).await
    }

    async fn upsert_quote_ref(&self, conn: &mut PgConnection, quote: &QuoteRef) -> Result<(), sqlx::Error> {
        ingest::upsert_quote_ref(conn, quote).await
    }

    async fn delete_quote_ref(&self, conn: &mut PgConnection, uri: &str, rev: &str) -> Result<(), sqlx::Error> {
        ingest::delete_quote_ref(conn, uri, rev).await
    }

    async fn notify_public(&self, conn: &mut PgConnection, n: &PublicNotify) -> Result<(), sqlx::Error> {
        ingest::notify_public(conn, n).await
    }

    async fn delete_notifications(&self, conn: &mut PgConnection, record_uri: &str) -> Result<(), sqlx::Error> {
        ingest::delete_notifications(conn, record_uri).await
    }

    async fn clear_all_readiness(&self, conn: &mut PgConnection) -> Result<(), sqlx::Error> {
        readiness::clear_all(conn).await
    }

    async fn clear_owner_readiness(&self, conn: &mut PgConnection, did: &str) -> Result<(), sqlx::Error> {
        readiness::clear_owner(conn, did).await
    }

    async fn mark_owner_ready(&self, conn: &mut PgConnection, did: &str) -> Result<(), sqlx::Error> {
        readiness::mark_owner_ready(conn, did).await
    }

    async fn owner_ready(&self, conn: &mut PgConnection, did: &str) -> Result<bool, sqlx::Error> {
        readiness::owner_ready_on(conn, did).await
    }

    async fn local_uris(
        &self,
        conn: &mut PgConnection,
        did: &str,
        nsid: &str,
        below_rev: &str,
    ) -> Result<Vec<String>, sqlx::Error> {
        ingest::local_uris(conn, did, nsid, below_rev).await
    }

    async fn stored_cids(&self, conn: &mut PgConnection, did: &str, nsid: &str) -> Result<HashMap<String, String>, sqlx::Error> {
        ingest::stored_cids(conn, did, nsid).await
    }

    async fn purge_did_batch(&self, conn: &mut PgConnection, did: &str, limit: i64) -> Result<u64, sqlx::Error> {
        ingest::purge_did(conn, did, limit).await
    }

    async fn purge_out_of_scope_batch(
        &self,
        conn: &mut PgConnection,
        did: &str,
        limit: i64,
    ) -> Result<u64, sqlx::Error> {
        ingest::purge_out_of_scope(conn, did, limit).await
    }

    async fn purge_member_removed_batch(
        &self,
        conn: &mut PgConnection,
        did: &str,
        limit: i64,
    ) -> Result<u64, sqlx::Error> {
        ingest::purge_member_removed(conn, did, limit).await
    }

    async fn reevaluate_owner(&self, conn: &mut PgConnection, did: &str) -> Result<Applied, sqlx::Error> {
        owners::reevaluate(conn, did).await
    }

    async fn record_eligibility(&self, conn: &mut PgConnection, did: &str, confirmed: bool) -> Result<(), sqlx::Error> {
        owners::record_eligibility(conn, did, confirmed).await
    }

    async fn eligibility_confirmed_at(
        &self,
        conn: &mut PgConnection,
        did: &str,
    ) -> Result<Option<DateTime<Utc>>, sqlx::Error> {
        owners::eligibility_confirmed_at(conn, did).await
    }

    async fn enqueue_display_refill(&self, pool: &PgPool, marker: &str) -> Result<Option<u64>, sqlx::Error> {
        ingest::enqueue_display_refill(pool, marker).await
    }
}

/// A private tier that admits nothing and reports no owners (tests of other
/// modules). Private-tier ops are skipped and counted.
#[derive(Debug, Clone, Copy, Default)]
pub struct DisabledPrivateTier;

/// Metric: private-tier ops skipped because the private tier is disabled.
pub const METRIC_PRIVATE_SKIPPED: &str = "private_tier_ops_skipped_total";

#[async_trait]
impl PrivateTier for DisabledPrivateTier {
    fn features(&self) -> &'static [&'static str] {
        &[]
    }

    async fn load_owners(&self, _pool: &PgPool) -> Result<HashSet<String>, sqlx::Error> {
        Ok(HashSet::new())
    }

    async fn apply(&self, _conn: &mut PgConnection, _op: &PrivateOp) -> Result<Applied, IngestError> {
        metrics::counter!(METRIC_PRIVATE_SKIPPED, "op" => "apply").increment(1);
        Ok(Applied::default())
    }

    async fn delete(
        &self,
        _conn: &mut PgConnection,
        _uri: &str,
        _rev: &str,
        _author_in_scope: bool,
    ) -> Result<Applied, sqlx::Error> {
        metrics::counter!(METRIC_PRIVATE_SKIPPED, "op" => "delete").increment(1);
        Ok(Applied::default())
    }

    async fn apply_gate(&self, _conn: &mut PgConnection, _op: &PrivateOp) -> Result<Applied, IngestError> {
        metrics::counter!(METRIC_PRIVATE_SKIPPED, "op" => "apply_gate").increment(1);
        Ok(Applied::default())
    }

    async fn delete_gate(
        &self,
        _conn: &mut PgConnection,
        _uri: &str,
        _rev: &str,
        _author_in_scope: bool,
    ) -> Result<Applied, sqlx::Error> {
        Ok(Applied::default())
    }

    async fn post_rkeys(&self, _pool: &PgPool, _did: &str) -> Result<HashSet<String>, sqlx::Error> {
        Ok(HashSet::new())
    }

    async fn upsert_quote_ref(&self, _conn: &mut PgConnection, _quote: &QuoteRef) -> Result<(), sqlx::Error> {
        Ok(())
    }

    async fn delete_quote_ref(&self, _conn: &mut PgConnection, _uri: &str, _rev: &str) -> Result<(), sqlx::Error> {
        Ok(())
    }

    async fn notify_public(&self, _conn: &mut PgConnection, _n: &PublicNotify) -> Result<(), sqlx::Error> {
        Ok(())
    }

    async fn delete_notifications(&self, _conn: &mut PgConnection, _record_uri: &str) -> Result<(), sqlx::Error> {
        Ok(())
    }

    async fn clear_all_readiness(&self, _conn: &mut PgConnection) -> Result<(), sqlx::Error> {
        Ok(())
    }

    async fn clear_owner_readiness(&self, _conn: &mut PgConnection, _did: &str) -> Result<(), sqlx::Error> {
        Ok(())
    }

    async fn mark_owner_ready(&self, _conn: &mut PgConnection, _did: &str) -> Result<(), sqlx::Error> {
        Ok(())
    }

    async fn owner_ready(&self, _conn: &mut PgConnection, _did: &str) -> Result<bool, sqlx::Error> {
        Ok(false)
    }

    async fn local_uris(
        &self,
        _conn: &mut PgConnection,
        _did: &str,
        _nsid: &str,
        _below_rev: &str,
    ) -> Result<Vec<String>, sqlx::Error> {
        Ok(Vec::new())
    }

    async fn stored_cids(&self, _conn: &mut PgConnection, _did: &str, _nsid: &str) -> Result<HashMap<String, String>, sqlx::Error> {
        Ok(HashMap::new())
    }

    async fn purge_did_batch(&self, _conn: &mut PgConnection, _did: &str, _limit: i64) -> Result<u64, sqlx::Error> {
        Ok(0)
    }

    async fn purge_out_of_scope_batch(
        &self,
        _conn: &mut PgConnection,
        _did: &str,
        _limit: i64,
    ) -> Result<u64, sqlx::Error> {
        Ok(0)
    }

    async fn purge_member_removed_batch(
        &self,
        _conn: &mut PgConnection,
        _did: &str,
        _limit: i64,
    ) -> Result<u64, sqlx::Error> {
        Ok(0)
    }

    async fn reevaluate_owner(&self, _conn: &mut PgConnection, _did: &str) -> Result<Applied, sqlx::Error> {
        Ok(Applied::default())
    }

    async fn record_eligibility(&self, _conn: &mut PgConnection, _did: &str, _confirmed: bool) -> Result<(), sqlx::Error> {
        Ok(())
    }

    async fn eligibility_confirmed_at(
        &self,
        _conn: &mut PgConnection,
        _did: &str,
    ) -> Result<Option<DateTime<Utc>>, sqlx::Error> {
        Ok(None)
    }

    async fn enqueue_display_refill(&self, _pool: &PgPool, _marker: &str) -> Result<Option<u64>, sqlx::Error> {
        Ok(None)
    }
}
