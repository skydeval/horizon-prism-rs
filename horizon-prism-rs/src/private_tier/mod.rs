//! The private tier (design-v2 §3.2, §4.5, §6.3, §6.4, §7.2, §7.9): the only
//! module that touches the private tables or decodes private content.
//!
//! - Internals (`schema`, `oracle`, `pipeline`, `emit`) are private to this
//!   module; their items are `pub(super)`, so nothing outside `private_tier`
//!   can name a private-table query, a `RecordAuthz` or a `PrivateEmit`.
//! - The `pub(crate)` surface is `ingest`, `readiness`, `owners`, `handlers`,
//!   `retention` and `stats` (the admin stats page's labels and counts). The rest of the crate reaches ingest, readiness, owner
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
//! use horizon_prism_rs::private_tier::schema::snapshot;
//! ```
//!
//! ```compile_fail
//! use horizon_prism_rs::private_tier::pipeline::RecordAuthz;
//! ```
//!
//! ```compile_fail
//! use horizon_prism_rs::private_tier::emit::PrivateEmit;
//! ```

mod clear_fields;
mod content;
mod cursor;
mod emit;
pub(crate) mod gauges;
pub(crate) mod handlers;
pub(crate) mod ingest;
mod media;
mod oracle;
pub(crate) mod owners;
mod pipeline;
pub(crate) mod readiness;
pub(crate) mod retention;
mod schema;
pub(crate) mod stats;
#[cfg(test)]
pub(crate) mod test_rows;
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
    /// The op created the record (not an update): only a live create
    /// notifies for a private post of any age (private-embeds design §6).
    pub created: bool,
    /// The record's `createdAt` as `records::created_at` parses it.
    pub created_at: DateTime<Utc>,
    /// `private_notify.backfill_max_age_hours` (C-11): a write that is not
    /// live notifies only for a record created within it.
    pub notify_max_age: chrono::Duration,
}

/// Whether a public record's write notifies (private posting §4.8, C-11): a
/// live firehose write always does; any other source (backfill, catch-up,
/// `requestRecord`, dead-letter replay) only for a record created within
/// `max_age`.
#[must_use]
pub fn notifies(source: WriteSource, created_at: DateTime<Utc>, max_age: chrono::Duration) -> bool {
    source == WriteSource::Firehose || Utc::now().signed_duration_since(created_at) <= max_age
}

/// Whether a private post's write notifies (private-embeds design §6 rule
/// 1): a live firehose **create** at any age; every other write, a live
/// update included, only for a post created within `max_age`. A PDS
/// rewriting its old posts so notifies no one again.
#[must_use]
pub fn notifies_post(source: WriteSource, created: bool, created_at: DateTime<Utc>, max_age: chrono::Duration) -> bool {
    (source == WriteSource::Firehose && created) || Utc::now().signed_duration_since(created_at) <= max_age
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
    // Rule 4's anchored walk (S0, listed from S1; the read rule of S12
    // since).
    "private.crossPdsReplies",
    // P-4 the `following` audience mode (S10).
    "private.following",
    // P-7 `searchPosts`, a bounded scan at query time (S11).
    "private.search",
    // P-21 to P-23: a reply is read by its thread root's (or branch root's)
    // current audience, walked up to `private_read.max_chain` rows; paged
    // reads capped at twice their limit (S12).
    "private.threadAudience",
    // P-30 posts_with_media and posts_with_video on getAuthorFeed
    // (private-media-filters design).
    "private.mediaFilters",
    // get/putNotificationPreferences, honoured by listNotifications and
    // getUnreadCount (private notification preferences design).
    "private.notificationPreferences",
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

    /// `ingest::replayed_since_listing` (replay-readiness design §4.2): any
    /// record catch-up skipped by CID (`uris[i]` with `cids[i]`) is now
    /// stored present, below `rev`, with another CID. On the caller's
    /// connection, inside catch-up's raise under the exclusive DID lock.
    async fn replayed_since_listing(
        &self,
        conn: &mut PgConnection,
        nsid: &str,
        uris: &[String],
        cids: &[String],
        rev: &str,
    ) -> Result<bool, sqlx::Error>;

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

    /// `ingest::enqueue_display_refill` (private posting §3.1): once, under
    /// the `worker_state` key `marker`, a `private` job for every DID with
    /// rows stored before migration 0023. `None` when already done.
    async fn enqueue_display_refill(&self, pool: &PgPool, marker: &str) -> Result<Option<u64>, sqlx::Error>;

    /// `owners::owns_following_audience` (following-audience design §3.5):
    /// `did` is a private owner now and has a `following` audience row, on
    /// the caller's connection.
    async fn owns_following_audience(&self, conn: &mut PgConnection, did: &str) -> Result<bool, sqlx::Error>;

    /// `owners::enqueue_following_verification` (following-audience design
    /// §3.6): at each start, an `authz` job for every DID that owns a
    /// `following` audience and whose follows are not verified. Returns the
    /// jobs written.
    async fn enqueue_following_verification(&self, pool: &PgPool) -> Result<u64, sqlx::Error>;

    /// `gauges::sample`: `private_owners_not_ready_total` and
    /// `private_tier_last_event_age_seconds`, sampled with the backfill
    /// gauges.
    async fn sample_gauges(&self, pool: &PgPool) -> Result<(), sqlx::Error>;
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

    async fn replayed_since_listing(
        &self,
        conn: &mut PgConnection,
        nsid: &str,
        uris: &[String],
        cids: &[String],
        rev: &str,
    ) -> Result<bool, sqlx::Error> {
        ingest::replayed_since_listing(conn, nsid, uris, cids, rev).await
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

    async fn enqueue_display_refill(&self, pool: &PgPool, marker: &str) -> Result<Option<u64>, sqlx::Error> {
        ingest::enqueue_display_refill(pool, marker).await
    }

    async fn owns_following_audience(&self, conn: &mut PgConnection, did: &str) -> Result<bool, sqlx::Error> {
        owners::owns_following_audience(conn, did).await
    }

    async fn enqueue_following_verification(&self, pool: &PgPool) -> Result<u64, sqlx::Error> {
        owners::enqueue_following_verification(pool).await
    }

    async fn sample_gauges(&self, pool: &PgPool) -> Result<(), sqlx::Error> {
        gauges::sample(pool).await
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

    async fn replayed_since_listing(
        &self,
        _conn: &mut PgConnection,
        _nsid: &str,
        _uris: &[String],
        _cids: &[String],
        _rev: &str,
    ) -> Result<bool, sqlx::Error> {
        Ok(false)
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

    async fn enqueue_display_refill(&self, _pool: &PgPool, _marker: &str) -> Result<Option<u64>, sqlx::Error> {
        Ok(None)
    }

    async fn owns_following_audience(&self, _conn: &mut PgConnection, _did: &str) -> Result<bool, sqlx::Error> {
        Ok(false)
    }

    async fn enqueue_following_verification(&self, _pool: &PgPool) -> Result<u64, sqlx::Error> {
        Ok(0)
    }

    /// No owners and no events: both gauges are 0.
    async fn sample_gauges(&self, _pool: &PgPool) -> Result<(), sqlx::Error> {
        metrics::gauge!(gauges::METRIC_OWNERS_NOT_READY).set(0.0);
        metrics::gauge!(gauges::METRIC_LAST_EVENT_AGE).set(0.0);
        Ok(())
    }
}
