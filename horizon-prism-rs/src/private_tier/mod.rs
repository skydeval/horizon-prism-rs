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

use std::collections::HashSet;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use sqlx::{PgConnection, PgPool};

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
    // Rule 4's anchored walk (S0, listed from S1).
    "private.crossPdsReplies",
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

    /// `readiness::clear_all`: the gap transaction's owner-readiness wipe (§6.11.4).
    async fn clear_all_readiness(&self, conn: &mut PgConnection) -> Result<(), sqlx::Error>;

    /// Clear one DID's owner readiness (dead letter, divergence, purge marker).
    async fn clear_owner_readiness(&self, conn: &mut PgConnection, did: &str) -> Result<(), sqlx::Error>;

    /// `readiness::mark_owner_ready` (§5.10). A no-op for a non-owner.
    async fn mark_owner_ready(&self, conn: &mut PgConnection, did: &str) -> Result<(), sqlx::Error>;

    /// URIs of `did`'s local rows in `nsid` with `rev < below_rev`: the
    /// reconcile candidates for a private collection (§5.10).
    async fn local_uris(
        &self,
        conn: &mut PgConnection,
        did: &str,
        nsid: &str,
        below_rev: &str,
    ) -> Result<Vec<String>, sqlx::Error>;

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

    async fn clear_all_readiness(&self, conn: &mut PgConnection) -> Result<(), sqlx::Error> {
        readiness::clear_all(conn).await
    }

    async fn clear_owner_readiness(&self, conn: &mut PgConnection, did: &str) -> Result<(), sqlx::Error> {
        readiness::clear_owner(conn, did).await
    }

    async fn mark_owner_ready(&self, conn: &mut PgConnection, did: &str) -> Result<(), sqlx::Error> {
        readiness::mark_owner_ready(conn, did).await
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

    async fn clear_all_readiness(&self, _conn: &mut PgConnection) -> Result<(), sqlx::Error> {
        Ok(())
    }

    async fn clear_owner_readiness(&self, _conn: &mut PgConnection, _did: &str) -> Result<(), sqlx::Error> {
        Ok(())
    }

    async fn mark_owner_ready(&self, _conn: &mut PgConnection, _did: &str) -> Result<(), sqlx::Error> {
        Ok(())
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
