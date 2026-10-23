//! The public root gate fetches (public-root-gates design §4.2): one fetch
//! of a public root's threadgate from its author's PDS, queued in
//! `private_root_gate_fetches` when a private reply is the root's only
//! stored one and no copy of its gate is stored (`ingest::apply`), and
//! seeded by migration 0042.
//!
//! The worker leases due rows and runs `index_record` for each: resolve the
//! author's PDS, `getLatestCommit` (rev R), `getRecord`, then the normal
//! write path at R, where admission and `ingest::apply_root_gate` keep the
//! gate. Calls run under the host's backfill budget.

use std::sync::Arc;
use std::time::Duration;

use sqlx::PgPool;

use crate::firehose::backfill::BackfillSource;
use crate::firehose::classifier::{NSID_POST, NSID_THREADGATE};
use crate::firehose::index_request::{IndexStatus, index_record};
use crate::firehose::writer::Writer;

/// How often the worker looks for due fetches.
pub const POLL: Duration = Duration::from_secs(10);

/// Rows leased per pass.
pub const BATCH: i64 = 8;

/// How long a lease holds a row before another pass may take it, in
/// minutes.
pub const LEASE_MINUTES: i32 = 10;

/// Attempts before a fetch is given up.
pub const MAX_ATTEMPTS: i32 = 8;

/// The longest wait between attempts, in minutes.
const MAX_BACKOFF_MINUTES: i32 = 360;

/// Metric: fetches by `outcome` (`indexed`, `unchanged`, `deleted`,
/// `out_of_scope`, `failed`, `gave_up`).
pub const METRIC_FETCHES: &str = "private_root_gate_fetches_total";

/// The fetch worker.
#[derive(Clone)]
pub struct RootGateFetches {
    pool: PgPool,
    writer: Writer,
    source: Arc<dyn BackfillSource>,
    attempts: u32,
}

impl RootGateFetches {
    /// `attempts`: `firehose.deterministic_attempts`, for each write.
    #[must_use]
    pub fn new(pool: PgPool, writer: Writer, source: Arc<dyn BackfillSource>, attempts: u32) -> Self {
        Self { pool, writer, source, attempts }
    }

    /// Run the worker until the process ends.
    #[must_use]
    pub fn spawn(self) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            loop {
                if let Err(error) = self.pass().await {
                    tracing::warn!(event = "root_gate_fetch_pass_failed", %error, "a public root gate fetch pass failed");
                }
                tokio::time::sleep(POLL).await;
            }
        })
    }

    /// One pass: lease up to [`BATCH`] due rows and fetch each. Returns the
    /// rows fetched.
    ///
    /// # Errors
    ///
    /// Database errors.
    pub async fn pass(&self) -> Result<usize, sqlx::Error> {
        let leased: Vec<(String, i32)> = sqlx::query_as(
            "UPDATE private_root_gate_fetches f SET next_at = now() + make_interval(mins => $2) \
             FROM (SELECT root_uri FROM private_root_gate_fetches WHERE done_at IS NULL AND next_at <= now() \
                   ORDER BY next_at LIMIT $1 FOR UPDATE SKIP LOCKED) due \
             WHERE f.root_uri = due.root_uri RETURNING f.root_uri, f.attempts",
        )
        .bind(BATCH)
        .bind(LEASE_MINUTES)
        .fetch_all(&self.pool)
        .await?;
        for (root, attempts) in &leased {
            self.fetch(root, *attempts).await?;
        }
        Ok(leased.len())
    }

    /// Fetch one root's gate and record the outcome.
    async fn fetch(&self, root: &str, attempts: i32) -> Result<(), sqlx::Error> {
        let Some((repo, rkey)) = root_parts(root) else {
            // Not a root this worker can fetch: done, with the reason.
            return self.give_up(root, "not an app.bsky.feed.post URI with a DID").await;
        };
        match index_record(self.source.as_ref(), &self.writer, repo, NSID_THREADGATE, rkey, self.attempts, false).await {
            Ok(indexed) => {
                let outcome = match indexed.status {
                    IndexStatus::Indexed => "indexed",
                    IndexStatus::Deleted => "deleted",
                    IndexStatus::OutOfScope => "out_of_scope",
                    IndexStatus::Unchanged => "unchanged",
                };
                metrics::counter!(METRIC_FETCHES, "outcome" => outcome).increment(1);
                sqlx::query("UPDATE private_root_gate_fetches SET done_at = now(), last_error = NULL WHERE root_uri = $1")
                    .bind(root)
                    .execute(&self.pool)
                    .await?;
            }
            Err(error) => {
                let error = error.to_string();
                if attempts + 1 >= MAX_ATTEMPTS {
                    return self.give_up(root, &error).await;
                }
                metrics::counter!(METRIC_FETCHES, "outcome" => "failed").increment(1);
                tracing::debug!(event = "root_gate_fetch_failed", root, %error, "a public root gate fetch failed; retried later");
                sqlx::query(
                    "UPDATE private_root_gate_fetches SET attempts = attempts + 1, last_error = $2, \
                       next_at = now() + make_interval(mins => LEAST($3, 1 << LEAST(attempts + 1, 16))) \
                     WHERE root_uri = $1",
                )
                .bind(root)
                .bind(&error)
                .bind(MAX_BACKOFF_MINUTES)
                .execute(&self.pool)
                .await?;
            }
        }
        Ok(())
    }

    async fn give_up(&self, root: &str, error: &str) -> Result<(), sqlx::Error> {
        metrics::counter!(METRIC_FETCHES, "outcome" => "gave_up").increment(1);
        tracing::info!(event = "root_gate_fetch_gave_up", root, error, "a public root gate fetch was given up");
        sqlx::query(
            "UPDATE private_root_gate_fetches SET attempts = attempts + 1, last_error = $2, done_at = now() WHERE root_uri = $1",
        )
        .bind(root)
        .bind(error)
        .execute(&self.pool)
        .await?;
        Ok(())
    }
}

/// The repo and rkey of an `at://did:…/app.bsky.feed.post/<rkey>` URI.
fn root_parts(root: &str) -> Option<(&str, &str)> {
    let mut parts = root.strip_prefix("at://")?.split('/');
    match (parts.next(), parts.next(), parts.next(), parts.next()) {
        (Some(did), Some(NSID_POST), Some(rkey), None) if did.starts_with("did:") && !rkey.is_empty() => Some((did, rkey)),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn root_parts_takes_did_post_uris_only() {
        assert_eq!(root_parts("at://did:plc:a/app.bsky.feed.post/3k"), Some(("did:plc:a", "3k")));
        for bad in [
            "at://alice.test/app.bsky.feed.post/3k",
            "at://did:plc:a/tools.kryphocron.feed.postPrivate/3k",
            "at://did:plc:a/app.bsky.feed.post/",
            "at://did:plc:a/app.bsky.feed.post/3k/x",
            "https://did:plc:a/app.bsky.feed.post/3k",
        ] {
            assert_eq!(root_parts(bad), None, "{bad}");
        }
    }
}
