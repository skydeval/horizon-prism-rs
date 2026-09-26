//! The private tier's `pub(crate)` surface as seen by the rest of the crate
//! (design-v2 §3.2).
//!
//! Only `private_tier` touches the private tables (§4.2). Everything else
//! (the firehose writer, backfill, the gap transaction, purges, readiness)
//! calls through [`PrivateTier`]. Item 6 provides the concrete
//! implementation; until then [`DisabledPrivateTier`] is wired in: it admits
//! nothing into the private tables and reports no private owners, so private
//! reads keep failing closed.

use std::collections::HashSet;

use async_trait::async_trait;
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
    /// DIDs that became private owners in this transaction (§6.11.1). The
    /// caller adds them to the in-memory owner set after commit.
    pub owners_joined: Vec<String>,
    /// DIDs that stopped being private owners in this transaction.
    pub owners_left: Vec<String>,
}

/// The private tier as the rest of the crate may use it.
#[async_trait]
pub trait PrivateTier: Send + Sync {
    /// The private-owner set for `ScopeState` (§9.3 rule 3). DIDs only.
    async fn load_owners(&self, pool: &PgPool) -> Result<HashSet<String>, sqlx::Error>;

    /// `ingest::apply`: LWW create/update of an admitted private record, with
    /// owner re-evaluation and `private` job enqueue (§5.5, §6.11.1).
    async fn apply(&self, conn: &mut PgConnection, op: &PrivateOp) -> Result<Applied, sqlx::Error>;

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

    /// `readiness::mark_owner_ready` (§5.10).
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

    /// The private part of a `member_removed` purge: class-M private replies
    /// admitted only because `did` was a member (§6.11.7), at most `limit`.
    async fn purge_member_removed_batch(
        &self,
        conn: &mut PgConnection,
        did: &str,
        limit: i64,
    ) -> Result<u64, sqlx::Error>;

    /// `owners::reevaluate` (§6.11.1), used at purge end.
    async fn reevaluate_owner(&self, conn: &mut PgConnection, did: &str) -> Result<Applied, sqlx::Error>;
}

/// Pre-item-6 wiring: private-tier ops are skipped and counted.
#[derive(Debug, Clone, Copy, Default)]
pub struct DisabledPrivateTier;

/// Metric: private-tier ops skipped because the private tier is not wired.
pub const METRIC_PRIVATE_SKIPPED: &str = "private_tier_ops_skipped_total";

#[async_trait]
impl PrivateTier for DisabledPrivateTier {
    async fn load_owners(&self, _pool: &PgPool) -> Result<HashSet<String>, sqlx::Error> {
        Ok(HashSet::new())
    }

    async fn apply(&self, _conn: &mut PgConnection, _op: &PrivateOp) -> Result<Applied, sqlx::Error> {
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
}
