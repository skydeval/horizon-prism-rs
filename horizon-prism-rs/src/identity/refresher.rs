//! The identity refresher (design-v2 §5.6).
//!
//! It consumes `identity_refresh` rows written by the firehose worker for
//! identity, account, handle and tombstone events, with concurrency 4. Its
//! outbound calls go through `net`; the firehose loop never waits on it.
//!
//! Per DID, in order:
//! 1. **Always:** re-resolve the DID document bypassing the cache, then update
//!    `did_cache` and `actors.pds_endpoint`. PLC 410 means `deleted`
//!    immediately. did:web 404/410 records a suspicion that becomes `deleted`
//!    only if unchanged 24 h later.
//! 2. **`identity`, `handle`:** verify the handle bidirectionally.
//! 3. **`account`, `tombstone`:** `getRepoStatus` on the just-resolved PDS.
//!    `RepoNotFound`/404 records a suspicion against that endpoint; the DID
//!    becomes `deleted` only when a later check names the same endpoint, it
//!    still answers `RepoNotFound`, and 24 h have passed.
//!
//! A transition to `deleted` calls [`AccountLifecycle::account_deleted`], which
//! runs the §6.11.7 `account_deleted` purge. Failures retry with backoff
//! (30 s, 2 m, 10 m, 1 h) up to 8 attempts.

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use chrono::{DateTime, TimeDelta, Utc};
use futures::StreamExt;
use serde::Deserialize;
use sqlx::PgPool;
use tokio::task::JoinHandle;

use super::resolver::{DidTarget, IdentityResolver, extract_info, parse_did};
use super::{IdentityError, ResolveIntent};
use crate::net::{BodyCap, RequestOptions};

/// `identity_refresh.reasons` bits (§4.2).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RefreshReason;

impl RefreshReason {
    pub const IDENTITY: i16 = 1;
    pub const ACCOUNT: i16 = 2;
    pub const HANDLE: i16 = 4;
    pub const TOMBSTONE: i16 = 8;
    pub const STALE: i16 = 16;
}

/// Concurrent refreshes (§5.6).
pub const CONCURRENCY: usize = 4;
/// Attempts before a row is dropped (§5.6).
pub const MAX_ATTEMPTS: i32 = 8;
/// Deletion suspicion window (§5.6).
pub const SUSPICION_WINDOW: TimeDelta = TimeDelta::hours(24);
/// Re-check interval while a suspicion is open (§5.6).
pub const SUSPICION_RECHECK: Duration = Duration::from_secs(3600);
/// A claimed row is hidden from other claims for this long.
pub const CLAIM_LEASE: Duration = Duration::from_secs(600);
/// Poll interval when the queue is idle.
pub const IDLE_POLL: Duration = Duration::from_secs(5);
/// Rows claimed per poll.
pub const CLAIM_BATCH: i64 = 32;
/// `deletion_suspect_pds` marker for did:web documents (§5.6).
pub const DID_WEB_SUSPECT: &str = "did:web";

/// Metric: refresh outcomes, labelled by outcome.
pub const METRIC_OUTCOMES: &str = "identity_refresh_total";
/// Metric: applied `actors.account_status` changes, labelled by the new
/// status.
pub const METRIC_STATUS_CHANGES: &str = "account_status_changes_total";

/// Retry delay after the `attempt`-th consecutive failure (1-based): 30 s,
/// 2 m, 10 m, then 1 h.
#[must_use]
pub fn backoff(attempt: i32) -> Duration {
    match attempt {
        i32::MIN..=1 => Duration::from_secs(30),
        2 => Duration::from_secs(120),
        3 => Duration::from_secs(600),
        _ => Duration::from_secs(3600),
    }
}

/// The result of a deletion-suspicion check.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Suspicion {
    /// The same source still reports the DID missing after the window.
    Deleted,
    /// Suspected since the given time; re-check later.
    Open { since: DateTime<Utc> },
}

/// Advance a suspicion. `source` is the endpoint that answered "missing"
/// (the PDS endpoint, or [`DID_WEB_SUSPECT`]). A different source than the
/// recorded one restarts the window: an account that moved is never purged.
#[must_use]
pub fn advance_suspicion(
    prev_source: Option<&str>,
    prev_since: Option<DateTime<Utc>>,
    source: &str,
    now: DateTime<Utc>,
) -> Suspicion {
    match (prev_source, prev_since) {
        (Some(p), Some(since)) if p == source => {
            if now - since >= SUSPICION_WINDOW {
                Suspicion::Deleted
            } else {
                Suspicion::Open { since }
            }
        }
        _ => Suspicion::Open { since: now },
    }
}

/// `com.atproto.sync.getRepoStatus` output.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct RepoStatus {
    pub active: bool,
    #[serde(default)]
    pub status: Option<String>,
}

/// `actors.account_status` from a `getRepoStatus` answer. An inactive repo
/// with no status is recorded as `deactivated`.
#[must_use]
pub fn account_status(status: &RepoStatus) -> String {
    if status.active {
        "active".to_string()
    } else {
        status
            .status
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map_or_else(|| "deactivated".to_string(), str::to_ascii_lowercase)
    }
}

/// XRPC error body.
#[derive(Debug, Deserialize)]
struct XrpcError {
    #[serde(default)]
    error: Option<String>,
}

/// True for the answers §5.6 treats as "repo not found".
#[must_use]
pub fn is_repo_not_found(status: u16, body: &[u8]) -> bool {
    status == 404
        || (status == 400
            && serde_json::from_slice::<XrpcError>(body)
                .ok()
                .and_then(|e| e.error)
                .is_some_and(|e| e == "RepoNotFound"))
}

/// Hook run when a DID transitions to `deleted`: the §6.11.7 `account_deleted`
/// purge of every row authored by the DID, in every table. It must be
/// idempotent; a failure is retried with the refresh's backoff.
#[async_trait]
pub trait AccountLifecycle: Send + Sync {
    async fn account_deleted(&self, did: &str) -> Result<(), String>;
}

/// What happened to one claimed row.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Outcome {
    Done,
    /// Keep the row and look again after the delay (open suspicion).
    Recheck(Duration),
    Deleted,
}

#[derive(sqlx::FromRow)]
struct Claimed {
    did: String,
    reasons: i16,
    attempts: i32,
}

#[derive(sqlx::FromRow)]
struct ActorState {
    deletion_suspect_at: Option<DateTime<Utc>>,
    deletion_suspect_pds: Option<String>,
}

/// The refresher task.
#[derive(Clone)]
pub struct IdentityRefresher {
    pool: PgPool,
    resolver: IdentityResolver,
    lifecycle: Arc<dyn AccountLifecycle>,
}

fn secs(d: Duration) -> f64 {
    d.as_secs_f64()
}

impl IdentityRefresher {
    #[must_use]
    pub fn new(pool: PgPool, resolver: IdentityResolver, lifecycle: Arc<dyn AccountLifecycle>) -> Self {
        Self {
            pool,
            resolver,
            lifecycle,
        }
    }

    /// Spawn the worker loop. Abort the handle to stop it.
    #[must_use]
    pub fn spawn(self) -> JoinHandle<()> {
        tokio::spawn(async move {
            loop {
                let processed = match self.run_batch().await {
                    Ok(n) => n,
                    Err(error) => {
                        tracing::warn!(event = "identity_refresh_claim_failed", %error, "claiming identity_refresh rows failed");
                        0
                    }
                };
                if processed < usize::try_from(CLAIM_BATCH).unwrap_or(usize::MAX) {
                    tokio::time::sleep(IDLE_POLL).await;
                }
            }
        })
    }

    /// Claim due rows and process them with concurrency 4.
    ///
    /// # Errors
    ///
    /// Only claim failures; per-row failures are recorded on the row.
    pub async fn run_batch(&self) -> Result<usize, sqlx::Error> {
        let claimed = sqlx::query_as::<_, Claimed>(
            "UPDATE identity_refresh r SET not_before = now() + make_interval(secs => $2) \
             FROM (SELECT did FROM identity_refresh WHERE not_before <= now() \
                   ORDER BY not_before LIMIT $1 FOR UPDATE SKIP LOCKED) due \
             WHERE r.did = due.did \
             RETURNING r.did, r.reasons, r.attempts",
        )
        .bind(CLAIM_BATCH)
        .bind(secs(CLAIM_LEASE))
        .fetch_all(&self.pool)
        .await?;
        let n = claimed.len();
        futures::stream::iter(claimed)
            .for_each_concurrent(CONCURRENCY, |row| async move { self.handle(row).await })
            .await;
        Ok(n)
    }

    async fn handle(&self, row: Claimed) {
        let result = self.process(&row.did, row.reasons).await;
        let settle = match result {
            Ok(Outcome::Done) => {
                metrics::counter!(METRIC_OUTCOMES, "outcome" => "done").increment(1);
                self.finish(&row.did, row.reasons).await
            }
            Ok(Outcome::Deleted) => {
                metrics::counter!(METRIC_OUTCOMES, "outcome" => "deleted").increment(1);
                self.finish(&row.did, row.reasons).await
            }
            Ok(Outcome::Recheck(delay)) => {
                metrics::counter!(METRIC_OUTCOMES, "outcome" => "recheck").increment(1);
                self.reschedule(&row.did, delay, 0).await
            }
            Err(error) => {
                let attempt = row.attempts.saturating_add(1);
                if attempt >= MAX_ATTEMPTS {
                    metrics::counter!(METRIC_OUTCOMES, "outcome" => "gave_up").increment(1);
                    tracing::warn!(event = "identity_refresh_gave_up", did = %row.did, attempt, %error, "identity refresh failed too often; dropping");
                    self.drop_row(&row.did).await
                } else {
                    metrics::counter!(METRIC_OUTCOMES, "outcome" => "retry").increment(1);
                    tracing::debug!(event = "identity_refresh_retry", did = %row.did, attempt, %error, "identity refresh failed; retrying");
                    self.reschedule(&row.did, backoff(attempt), attempt).await
                }
            }
        };
        if let Err(error) = settle {
            tracing::warn!(event = "identity_refresh_settle_failed", did = %row.did, %error, "could not update identity_refresh; the lease expires and the row is retried");
        }
    }

    /// Delete the row if no reason bit arrived while it was processed;
    /// otherwise make it due again so the new bits are handled.
    async fn finish(&self, did: &str, reasons: i16) -> Result<(), sqlx::Error> {
        let deleted = sqlx::query("DELETE FROM identity_refresh WHERE did = $1 AND reasons = $2")
            .bind(did)
            .bind(reasons)
            .execute(&self.pool)
            .await?
            .rows_affected();
        if deleted == 0 {
            sqlx::query("UPDATE identity_refresh SET not_before = now(), attempts = 0 WHERE did = $1")
                .bind(did)
                .execute(&self.pool)
                .await?;
        }
        Ok(())
    }

    async fn reschedule(&self, did: &str, delay: Duration, attempts: i32) -> Result<(), sqlx::Error> {
        sqlx::query(
            "UPDATE identity_refresh SET not_before = now() + make_interval(secs => $2), attempts = $3 \
             WHERE did = $1",
        )
        .bind(did)
        .bind(secs(delay))
        .bind(attempts)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    async fn drop_row(&self, did: &str) -> Result<(), sqlx::Error> {
        sqlx::query("DELETE FROM identity_refresh WHERE did = $1")
            .bind(did)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    async fn actor_state(&self, did: &str) -> Result<Option<ActorState>, sqlx::Error> {
        sqlx::query_as::<_, ActorState>(
            "SELECT deletion_suspect_at, deletion_suspect_pds FROM actors WHERE did = $1",
        )
        .bind(did)
        .fetch_optional(&self.pool)
        .await
    }

    async fn process(&self, did: &str, reasons: i16) -> Result<Outcome, IdentityError> {
        let target = parse_did(did)?;
        let prev = self.actor_state(did).await?;
        let prev_source = prev.as_ref().and_then(|p| p.deletion_suspect_pds.as_deref());
        let prev_since = prev.as_ref().and_then(|p| p.deletion_suspect_at);
        let now = Utc::now();

        // 1. Always: re-resolve, bypassing the cache. The refresher only sees
        //    DIDs already tracked (in scope, members, or with an actors row),
        //    so the document is persisted.
        let fetched = match self.resolver.refresh_now(did, ResolveIntent::AdmittedAuthor).await {
            Ok(f) => f,
            Err(IdentityError::Tombstoned(_)) if target == DidTarget::Plc => {
                return self.mark_deleted(did, "plc_tombstone").await;
            }
            Err(IdentityError::NotFound(_) | IdentityError::Gone(_)) if matches!(target, DidTarget::Web { .. }) => {
                return match advance_suspicion(prev_source, prev_since, DID_WEB_SUSPECT, now) {
                    Suspicion::Deleted => self.mark_deleted(did, "did_web_missing_24h").await,
                    Suspicion::Open { since } => {
                        self.set_suspicion(did, since, DID_WEB_SUSPECT).await?;
                        Ok(Outcome::Recheck(SUSPICION_RECHECK))
                    }
                };
            }
            Err(error) => return Err(error),
        };
        let info = extract_info(&fetched.doc, did);
        self.update_endpoint(did, info.pds_endpoint.as_deref(), prev_source == Some(DID_WEB_SUSPECT))
            .await?;

        // 2. Handle verification.
        if reasons & (RefreshReason::IDENTITY | RefreshReason::HANDLE) != 0 {
            let verified = self
                .resolver
                .verify_handle(did, info.claimed_handle.as_deref())
                .await;
            sqlx::query("UPDATE actors SET handle = $2, handle_checked_at = now() WHERE did = $1")
                .bind(did)
                .bind(verified)
                .execute(&self.pool)
                .await?;
        }

        // 3. Account status from the just-resolved PDS.
        if reasons & (RefreshReason::ACCOUNT | RefreshReason::TOMBSTONE) != 0 {
            let Some(endpoint) = info.pds_endpoint.as_deref() else {
                return Err(IdentityError::MalformedDocument {
                    did: did.to_string(),
                    reason: "no #atproto_pds service to ask for the repo status".to_string(),
                });
            };
            return self.check_repo_status(did, endpoint, prev_source, prev_since, now).await;
        }
        Ok(Outcome::Done)
    }

    async fn check_repo_status(
        &self,
        did: &str,
        endpoint: &str,
        prev_source: Option<&str>,
        prev_since: Option<DateTime<Utc>>,
        now: DateTime<Utc>,
    ) -> Result<Outcome, IdentityError> {
        let url = format!("{endpoint}/xrpc/com.atproto.sync.getRepoStatus?did={did}");
        let response = self
            .resolver
            .net()
            .get(&url, &RequestOptions::new(BodyCap::REPO_STATUS))
            .await?;
        let status = response.status.as_u16();
        if response.is_success() {
            let repo: RepoStatus = response.json()?;
            let account = account_status(&repo);
            let before: Option<Option<String>> = sqlx::query_scalar(
                "UPDATE actors a SET account_status = $2, status_checked_at = now(), \
                 deletion_suspect_at = NULL, deletion_suspect_pds = NULL \
                 FROM (SELECT did, account_status FROM actors WHERE did = $1 FOR UPDATE) old \
                 WHERE a.did = old.did RETURNING old.account_status",
            )
            .bind(did)
            .bind(&account)
            .fetch_optional(&self.pool)
            .await?;
            // Each applied change is logged, so a PDS operator's status
            // events can be matched to what this AppView did with them.
            if let Some(before) = before
                && before.as_deref() != Some(account.as_str())
            {
                metrics::counter!(METRIC_STATUS_CHANGES, "to" => account.clone()).increment(1);
                tracing::info!(
                    event = "account_status_changed",
                    did,
                    from = before.as_deref().unwrap_or("unknown"),
                    to = %account,
                    "account status changed"
                );
            }
            if account == "deleted" {
                return self.mark_deleted(did, "pds_status_deleted").await;
            }
            return Ok(Outcome::Done);
        }
        if is_repo_not_found(status, &response.body) {
            return match advance_suspicion(prev_source, prev_since, endpoint, now) {
                Suspicion::Deleted => self.mark_deleted(did, "repo_not_found_24h").await,
                Suspicion::Open { since } => {
                    self.set_suspicion(did, since, endpoint).await?;
                    Ok(Outcome::Recheck(SUSPICION_RECHECK))
                }
            };
        }
        Err(IdentityError::Upstream {
            did: did.to_string(),
            status,
        })
    }

    async fn update_endpoint(
        &self,
        did: &str,
        endpoint: Option<&str>,
        clear_did_web_suspicion: bool,
    ) -> Result<(), sqlx::Error> {
        sqlx::query(
            "INSERT INTO actors (did, pds_endpoint) VALUES ($1, $2) \
             ON CONFLICT (did) DO UPDATE SET pds_endpoint = EXCLUDED.pds_endpoint",
        )
        .bind(did)
        .bind(endpoint)
        .execute(&self.pool)
        .await?;
        if clear_did_web_suspicion {
            sqlx::query(
                "UPDATE actors SET deletion_suspect_at = NULL, deletion_suspect_pds = NULL \
                 WHERE did = $1 AND deletion_suspect_pds = $2",
            )
            .bind(did)
            .bind(DID_WEB_SUSPECT)
            .execute(&self.pool)
            .await?;
        }
        Ok(())
    }

    async fn set_suspicion(&self, did: &str, since: DateTime<Utc>, source: &str) -> Result<(), sqlx::Error> {
        sqlx::query(
            "INSERT INTO actors (did, deletion_suspect_at, deletion_suspect_pds) VALUES ($1, $2, $3) \
             ON CONFLICT (did) DO UPDATE SET deletion_suspect_at = EXCLUDED.deletion_suspect_at, \
             deletion_suspect_pds = EXCLUDED.deletion_suspect_pds",
        )
        .bind(did)
        .bind(since)
        .bind(source)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    async fn mark_deleted(&self, did: &str, cause: &'static str) -> Result<Outcome, IdentityError> {
        sqlx::query(
            "INSERT INTO actors (did, account_status, status_checked_at) VALUES ($1, 'deleted', now()) \
             ON CONFLICT (did) DO UPDATE SET account_status = 'deleted', status_checked_at = now()",
        )
        .bind(did)
        .execute(&self.pool)
        .await?;
        tracing::info!(event = "account_deleted", did, cause, "DID transitioned to deleted; purging");
        self.lifecycle
            .account_deleted(did)
            .await
            .map_err(|reason| IdentityError::Upstream {
                did: format!("{did} (purge failed: {reason})"),
                status: 503,
            })?;
        Ok(Outcome::Deleted)
    }
}

#[cfg(test)]
mod tests {
    use chrono::TimeZone;

    use super::*;

    fn t(day: u32, hour: u32) -> DateTime<Utc> {
        Utc.with_ymd_and_hms(2026, 9, day, hour, 0, 0).single().expect("valid")
    }

    #[test]
    fn backoff_schedule() {
        assert_eq!(backoff(1), Duration::from_secs(30));
        assert_eq!(backoff(2), Duration::from_secs(120));
        assert_eq!(backoff(3), Duration::from_secs(600));
        assert_eq!(backoff(4), Duration::from_secs(3600));
        assert_eq!(backoff(7), Duration::from_secs(3600));
        assert_eq!(MAX_ATTEMPTS, 8);
        assert_eq!(CONCURRENCY, 4);
    }

    #[test]
    fn first_missing_answer_opens_a_suspicion() {
        assert_eq!(
            advance_suspicion(None, None, "https://pds.example", t(1, 0)),
            Suspicion::Open { since: t(1, 0) }
        );
    }

    #[test]
    fn same_source_within_24h_stays_open() {
        assert_eq!(
            advance_suspicion(Some("https://pds.example"), Some(t(1, 0)), "https://pds.example", t(1, 23)),
            Suspicion::Open { since: t(1, 0) }
        );
    }

    #[test]
    fn same_source_after_24h_is_deleted() {
        assert_eq!(
            advance_suspicion(Some("https://pds.example"), Some(t(1, 0)), "https://pds.example", t(2, 0)),
            Suspicion::Deleted
        );
        assert_eq!(
            advance_suspicion(Some(DID_WEB_SUSPECT), Some(t(1, 0)), DID_WEB_SUSPECT, t(3, 0)),
            Suspicion::Deleted
        );
    }

    #[test]
    fn a_moved_account_restarts_the_window() {
        assert_eq!(
            advance_suspicion(Some("https://old.example"), Some(t(1, 0)), "https://new.example", t(5, 0)),
            Suspicion::Open { since: t(5, 0) }
        );
    }

    #[test]
    fn account_status_mapping() {
        let s = |active: bool, status: Option<&str>| {
            account_status(&RepoStatus {
                active,
                status: status.map(str::to_string),
            })
        };
        assert_eq!(s(true, None), "active");
        assert_eq!(s(true, Some("takendown")), "active");
        assert_eq!(s(false, Some("takendown")), "takendown");
        assert_eq!(s(false, Some("Suspended")), "suspended");
        assert_eq!(s(false, Some("deleted")), "deleted");
        assert_eq!(s(false, None), "deactivated");
    }

    #[test]
    fn repo_not_found_detection() {
        assert!(is_repo_not_found(404, b""));
        assert!(is_repo_not_found(400, br#"{"error":"RepoNotFound","message":"x"}"#));
        assert!(!is_repo_not_found(400, br#"{"error":"InvalidRequest"}"#));
        assert!(!is_repo_not_found(500, br#"{"error":"RepoNotFound"}"#));
        assert!(!is_repo_not_found(400, b"not json"));
    }

    #[test]
    fn repo_status_parses() {
        let s: RepoStatus = serde_json::from_str(r#"{"did":"did:plc:x","active":false,"status":"deactivated","rev":"3k"}"#)
            .expect("parses");
        assert!(!s.active);
        assert_eq!(s.status.as_deref(), Some("deactivated"));
    }

    #[test]
    fn reason_bits_match_schema() {
        assert_eq!(
            [
                RefreshReason::IDENTITY,
                RefreshReason::ACCOUNT,
                RefreshReason::HANDLE,
                RefreshReason::TOMBSTONE,
                RefreshReason::STALE
            ],
            [1, 2, 4, 8, 16]
        );
    }
}
