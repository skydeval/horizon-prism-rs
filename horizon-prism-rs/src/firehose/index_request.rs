//! The index-request procedure (design-v2 §5.9), shared by
//! `app.nearhorizon.index.requestRecord` and `admin deadletter replay`:
//! 1. resolve the repo's PDS (§6.7);
//! 2. `getLatestCommit` gives rev R, fetched first so R lower-bounds the
//!    record state;
//! 3. `getRecord`;
//! 4. the normal write path (§5.4, §5.5, §9.3) with rev R: found →
//!    create/update, `RecordNotFound` → delete.
//!
//! `requestRecord` then runs [`catch_up`] (private posting §3.8): a bounded
//! full listing of the caller's four authorization collections at R, which
//! may advance the caller's `applied_rev` to R as an `authz` job does.
//! design-v4 §II-6.11.3's "exactly two writers" of `applied_rev` are three:
//! the firehose worker, full-authorization jobs, and this catch-up, which is
//! a full-authorization listing of one repository.

use std::collections::{HashMap, HashSet};
use std::time::{Duration, Instant};

use sqlx::PgConnection;

use super::backfill::{AUTHORIZATION_NSIDS, BackfillSource, ListPage, ListedRecord, local_uris};
use super::gap;
use super::writer::{Action, ApplyOptions, OpOutcome, RawOp, WriteSource, Writer, advance_applied_rev};
use crate::private_tier::PrivateTier;

/// What the write path did (`requestRecord` output `status`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IndexStatus {
    Indexed,
    Deleted,
    /// Admission (§9.3) rejected or skipped the record.
    OutOfScope,
    /// LWW skipped the write: the same or a newer version is indexed.
    Unchanged,
}

impl IndexStatus {
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Indexed => "indexed",
            Self::Deleted => "deleted",
            Self::OutOfScope => "out_of_scope",
            Self::Unchanged => "unchanged",
        }
    }
}

/// The procedure's result.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Indexed {
    pub rev: String,
    pub status: IndexStatus,
    /// The repo's PDS endpoint, as `prepare` resolved it.
    pub pds: String,
}

/// Why the procedure did not complete.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum IndexError {
    /// The PDS could not be resolved or did not answer.
    #[error("fetch failed: {0}")]
    Fetch(String),
    /// The write path failed (transiently, or deterministically after its
    /// attempts).
    #[error("write failed: {0}")]
    Write(String),
}

/// Run §5.9 for one record of `repo`. `subscription_rules` is on for a
/// dead-letter replay and off for `requestRecord` (design-v3 §4.3 "Owner-side
/// rules and their switch").
///
/// # Errors
///
/// [`IndexError`].
pub async fn index_record(
    source: &dyn BackfillSource,
    writer: &Writer,
    repo: &str,
    collection: &str,
    rkey: &str,
    attempts: u32,
    subscription_rules: bool,
) -> Result<Indexed, IndexError> {
    let pds = source.prepare(repo).await.map_err(|e| IndexError::Fetch(e.to_string()))?;
    let rev = source.latest_rev(&pds, repo).await.map_err(IndexError::Fetch)?;
    let record = source.get_record(&pds, repo, collection, rkey).await.map_err(IndexError::Fetch)?;
    let op = match &record {
        Some(r) => RawOp {
            action: Action::Create,
            collection: collection.to_string(),
            rkey: rkey.to_string(),
            cid: Some(r.cid.clone()),
            record: Some(r.value.clone()),
        },
        None => RawOp {
            action: Action::Delete,
            collection: collection.to_string(),
            rkey: rkey.to_string(),
            cid: None,
            record: None,
        },
    };
    let options = ApplyOptions {
        advance_applied_rev: false,
        dead_letter_readiness: false,
        attempts,
        live: false,
        subscription_rules,
        source: WriteSource::IndexRequest,
    };
    let report = writer.apply(repo, &rev, &[op], options).await.map_err(|e| IndexError::Write(e.0))?;
    Ok(Indexed {
        rev,
        status: status(report.outcomes.first(), record.is_some())?,
        pds,
    })
}

/// How long one catch-up may run (private posting §3.8 step 6).
pub const CATCH_UP_DEADLINE: Duration = Duration::from_millis(1500);

/// Metric: catch-up runs by outcome.
pub const METRIC_CATCH_UP: &str = "index_request_catch_up_total";

/// `requestRecord`'s catch-up settings (private posting §3.8).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CatchUpConfig {
    /// `index_request.catch_up_min_interval_secs`.
    pub min_interval: Duration,
    /// `index_request.catch_up_max_pages`: `listRecords` pages over every
    /// collection. The follows pass of a `following` owner (S10) will share
    /// it.
    pub max_pages: u32,
    /// `backfill.reconcile_confirm_limit`.
    pub reconcile_confirm_limit: u32,
    /// `firehose.deterministic_attempts`, for each write.
    pub attempts: u32,
    /// [`CATCH_UP_DEADLINE`].
    pub deadline: Duration,
}

impl CatchUpConfig {
    #[must_use]
    pub fn from_app(cfg: &crate::config::AppConfig) -> Self {
        Self {
            min_interval: Duration::from_secs(cfg.file.index_request.catch_up_min_interval_secs),
            max_pages: cfg.file.index_request.catch_up_max_pages,
            reconcile_confirm_limit: cfg.file.backfill.reconcile_confirm_limit,
            attempts: cfg.file.firehose.deterministic_attempts,
            deadline: CATCH_UP_DEADLINE,
        }
    }
}

/// The catch-up, with the DIDs that ran one within the interval. Cheap to
/// clone; the interval is per process.
#[derive(Clone)]
pub struct CatchUp {
    cfg: CatchUpConfig,
    recent: moka::sync::Cache<String, ()>,
}

impl CatchUp {
    #[must_use]
    pub fn new(cfg: CatchUpConfig) -> Self {
        Self {
            cfg,
            recent: moka::sync::Cache::builder()
                .max_capacity(1_000_000)
                .time_to_live(cfg.min_interval)
                .build(),
        }
    }

    /// Take the DID's interval: false when a catch-up for it began within it.
    fn claim(&self, did: &str) -> bool {
        self.recent.entry_by_ref(did).or_insert(()).is_fresh()
    }
}

/// How a catch-up ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Ended {
    /// `applied_rev` advanced to R.
    Settled,
    /// Not run (step 1): the reason.
    Skipped(&'static str),
    /// Run, and nothing settled: the reason.
    Unsettled(&'static str),
}

/// Run `requestRecord`'s catch-up for the caller `did` at `rev`, the rev
/// [`index_record`] fetched (private posting §3.8), and answer `caughtUp`:
/// whether the caller's `applied_rev ≥ rev` afterwards. It never fails the
/// request: an error ends the catch-up with no settle.
///
/// 1. Only for a tracked, ready DID that is not purging, whose `applied_rev`
///    is below `rev`, and that ran no catch-up within the interval.
/// 2. The gap epoch is read.
/// 3. The four authorization collections are listed to the end and each
///    record whose CID is not stored is written at `rev` as
///    [`WriteSource::CatchUp`], with `dead_letter_readiness`: a failed write
///    clears the DID's readiness and queues `authz` in its own transaction,
///    as on the firehose. Then each fully listed collection is reconciled,
///    deletes confirmed with `getRecord`.
/// 4. The follows pass of a `following` owner comes with S10.
/// 5. Settle, under the scope read guard: `applied_rev = GREATEST(…, rev)`
///    only with the epoch unchanged, the DID not purging and still ready,
///    every listing complete, no reconcile candidate unconfirmed, and no
///    write failed. Catch-up never sets readiness.
/// 6. Pages come from the host's catch-up budget, which never waits; at
///    most `max_pages` in all; the whole run stops at the deadline.
pub async fn catch_up(
    source: &dyn BackfillSource,
    writer: &Writer,
    catch_up: &CatchUp,
    did: &str,
    pds: &str,
    rev: &str,
) -> bool {
    let ended = run_catch_up(source, writer, catch_up, did, pds, rev).await.unwrap_or_else(|error| {
        tracing::info!(event = "catch_up_error", did, %error, "catch-up ended with no settle");
        Ended::Unsettled("error")
    });
    let label = match ended {
        Ended::Settled => "settled",
        Ended::Skipped(reason) | Ended::Unsettled(reason) => reason,
    };
    metrics::counter!(METRIC_CATCH_UP, "outcome" => label).increment(1);
    tracing::debug!(event = "catch_up", did, rev, outcome = label, "requestRecord catch-up");
    match applied_rev(writer, did).await {
        Ok(applied) => applied.is_some_and(|a| a.as_str() >= rev),
        Err(error) => {
            tracing::warn!(event = "catch_up_read_failed", did, %error, "reading applied_rev failed");
            false
        }
    }
}

fn db(e: sqlx::Error) -> String {
    e.to_string()
}

async fn run_catch_up(
    source: &dyn BackfillSource,
    writer: &Writer,
    catch_up: &CatchUp,
    did: &str,
    pds: &str,
    rev: &str,
) -> Result<Ended, String> {
    let cfg = &catch_up.cfg;
    let deadline = Instant::now() + cfg.deadline;
    // 1. When. The read guard is never held with a connection taken first.
    let (is_member, is_owner) = {
        let sets = writer.scope().read_guard().await;
        (sets.is_member(did), sets.is_owner(did))
    };
    if !(is_member || is_owner) {
        return Ok(Ended::Skipped("untracked"));
    }
    let epoch = {
        let mut conn = writer.pool().acquire().await.map_err(db)?;
        if !ready(&mut conn, writer.private().as_ref(), did, is_member, is_owner).await.map_err(db)? {
            return Ok(Ended::Skipped("not_ready"));
        }
        if purging(&mut conn, did).await.map_err(db)? {
            return Ok(Ended::Skipped("purging"));
        }
        // 2. The gap epoch, before anything is listed.
        gap::current_epoch(&mut conn).await.map_err(db)?
    };
    if applied_rev(writer, did).await.map_err(db)?.is_some_and(|a| a.as_str() >= rev) {
        return Ok(Ended::Skipped("caught_up"));
    }
    if !catch_up.claim(did) {
        return Ok(Ended::Skipped("interval"));
    }

    // 3. List and write.
    let mut pages: u32 = 0;
    let mut listings: Vec<(&str, HashSet<String>)> = Vec::with_capacity(AUTHORIZATION_NSIDS.len());
    for nsid in AUTHORIZATION_NSIDS {
        let stored = {
            let mut conn = writer.pool().acquire().await.map_err(db)?;
            writer.private().stored_cids(&mut conn, did, nsid).await.map_err(db)?
        };
        let mut listed: HashSet<String> = HashSet::new();
        let mut cursor: Option<String> = None;
        loop {
            if pages >= cfg.max_pages {
                return Ok(Ended::Unsettled("page_cap"));
            }
            let Some(left) = deadline.checked_duration_since(Instant::now()) else {
                return Ok(Ended::Unsettled("deadline"));
            };
            let page = match tokio::time::timeout(left, source.list_records_now(pds, did, nsid, cursor.as_deref())).await {
                Err(_) => return Ok(Ended::Unsettled("deadline")),
                Ok(Ok(None)) => return Ok(Ended::Unsettled("no_token")),
                Ok(Ok(Some(page))) => page,
                Ok(Err(error)) => return Err(error),
            };
            pages += 1;
            let ListPage { records, cursor: next } = page;
            let more = next.is_some() && !records.is_empty();
            if write_page(writer, cfg.attempts, did, rev, nsid, records, &stored, &mut listed).await? {
                return Ok(Ended::Unsettled("write_failed"));
            }
            if !more {
                break;
            }
            cursor = next;
        }
        listings.push((nsid, listed));
    }
    // Reconcile each listing: a stored record not listed is deleted once
    // `getRecord` confirms it is gone, within the bound.
    let mut budget = cfg.reconcile_confirm_limit;
    let run = rand::random::<u64>().to_le_bytes();
    for (nsid, listed) in &listings {
        let mut candidates: Vec<String> = local_uris(writer, did, nsid, rev, false)
            .await
            .map_err(db)?
            .into_iter()
            .filter(|u| !listed.contains(u))
            .collect();
        candidates.sort_by_cached_key(|u| {
            let mut h = blake3::Hasher::new();
            h.update(u.as_bytes());
            h.update(&run);
            *h.finalize().as_bytes()
        });
        for uri in &candidates {
            if budget == 0 {
                return Ok(Ended::Unsettled("reconcile_incomplete"));
            }
            budget -= 1;
            let Some(left) = deadline.checked_duration_since(Instant::now()) else {
                return Ok(Ended::Unsettled("deadline"));
            };
            let rkey = uri.rsplit('/').next().unwrap_or_default();
            let found = match tokio::time::timeout(left, source.get_record(pds, did, nsid, rkey)).await {
                Err(_) => return Ok(Ended::Unsettled("deadline")),
                Ok(found) => found?,
            };
            if found.is_none() {
                let op = RawOp {
                    action: Action::Delete,
                    collection: (*nsid).to_string(),
                    rkey: rkey.to_string(),
                    cid: None,
                    record: None,
                };
                let report = writer.apply(did, rev, &[op], catch_up_options(cfg.attempts)).await.map_err(|e| e.0)?;
                if !report.failed.is_empty() {
                    return Ok(Ended::Unsettled("write_failed"));
                }
            }
        }
    }

    // 5. Settle: the read guard before the connection (§6.11.7).
    let sets = writer.scope().read_guard().await;
    let (is_member, is_owner) = (sets.is_member(did), sets.is_owner(did));
    let mut tx = writer.pool().begin().await.map_err(db)?;
    if gap::current_epoch(&mut tx).await.map_err(db)? != epoch {
        return Ok(Ended::Unsettled("gap_epoch_changed"));
    }
    if purging(&mut tx, did).await.map_err(db)? {
        return Ok(Ended::Unsettled("purging"));
    }
    if !(is_member || is_owner) || !ready(&mut tx, writer.private().as_ref(), did, is_member, is_owner).await.map_err(db)? {
        return Ok(Ended::Unsettled("not_ready"));
    }
    advance_applied_rev(&mut tx, did, rev).await.map_err(db)?;
    tx.commit().await.map_err(db)?;
    drop(sets);
    Ok(Ended::Settled)
}

/// Catch-up's writes (private posting §3.8 step 3): not live, no
/// subscription rules, and a failed op clears readiness as on the firehose.
fn catch_up_options(attempts: u32) -> ApplyOptions {
    ApplyOptions {
        advance_applied_rev: false,
        dead_letter_readiness: true,
        attempts,
        live: false,
        subscription_rules: false,
        source: WriteSource::CatchUp,
    }
}

/// Write one listed page at `rev`, skipping records whose CID is stored.
/// Returns whether any record failed (or was listed outside the repo and
/// collection).
#[allow(clippy::too_many_arguments)]
async fn write_page(
    writer: &Writer,
    attempts: u32,
    did: &str,
    rev: &str,
    nsid: &str,
    records: Vec<ListedRecord>,
    stored: &HashMap<String, String>,
    listed: &mut HashSet<String>,
) -> Result<bool, String> {
    let expected = format!("at://{did}/{nsid}/");
    let mut failed = false;
    let mut ops = Vec::with_capacity(records.len());
    for record in records {
        listed.insert(record.uri.clone());
        if !record.uri.starts_with(&expected) {
            tracing::warn!(event = "catch_up_record_skipped", did, uri = %record.uri, "listed record outside the repo/collection");
            failed = true;
            continue;
        }
        if stored.get(&record.uri) == Some(&record.cid) {
            continue;
        }
        ops.push(RawOp {
            action: Action::Create,
            collection: nsid.to_string(),
            rkey: record.rkey().to_string(),
            cid: Some(record.cid),
            record: Some(record.value),
        });
    }
    if ops.is_empty() {
        return Ok(failed);
    }
    let report = writer.apply(did, rev, &ops, catch_up_options(attempts)).await.map_err(|e| e.0)?;
    for f in &report.failed {
        tracing::warn!(event = "catch_up_record_failed", did, path = %f.path, error = %f.error, "record failed deterministically; readiness cleared");
    }
    Ok(failed || !report.failed.is_empty())
}

/// Readiness for each role the DID holds: a member's `ready_at` and an
/// owner's owner readiness.
async fn ready(
    conn: &mut PgConnection,
    private: &dyn PrivateTier,
    did: &str,
    is_member: bool,
    is_owner: bool,
) -> Result<bool, sqlx::Error> {
    if is_member {
        let ready: bool = sqlx::query_scalar("SELECT EXISTS (SELECT 1 FROM members WHERE did = $1 AND ready_at IS NOT NULL)")
            .bind(did)
            .fetch_one(&mut *conn)
            .await?;
        if !ready {
            return Ok(false);
        }
    }
    if is_owner && !private.owner_ready(conn, did).await? {
        return Ok(false);
    }
    Ok(true)
}

async fn purging(conn: &mut PgConnection, did: &str) -> Result<bool, sqlx::Error> {
    sqlx::query_scalar("SELECT EXISTS (SELECT 1 FROM purging_dids WHERE did = $1)")
        .bind(did)
        .fetch_one(conn)
        .await
}

async fn applied_rev(writer: &Writer, did: &str) -> Result<Option<String>, sqlx::Error> {
    Ok(sqlx::query_scalar::<_, Option<String>>("SELECT applied_rev FROM sync_state WHERE did = $1")
        .bind(did)
        .fetch_optional(writer.pool())
        .await?
        .flatten())
}

/// The `status` of one op's outcome. A member's post under the off rule
/// stores no post, so it is out of scope here (public-indexing design §4.1).
fn status(outcome: Option<&OpOutcome>, found: bool) -> Result<IndexStatus, IndexError> {
    Ok(match outcome {
        Some(OpOutcome::Written) if found => IndexStatus::Indexed,
        Some(OpOutcome::Written) => IndexStatus::Deleted,
        Some(OpOutcome::Rejected(_) | OpOutcome::Skipped(_) | OpOutcome::GrowOnly) => IndexStatus::OutOfScope,
        Some(OpOutcome::Failed(e)) => return Err(IndexError::Write(e.clone())),
        Some(OpOutcome::Unchanged) | None => IndexStatus::Unchanged,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    /// T-22: a grow-only post answers `out_of_scope`.
    #[test]
    fn grow_only_is_out_of_scope() {
        assert_eq!(status(Some(&OpOutcome::GrowOnly), true), Ok(IndexStatus::OutOfScope));
        assert_eq!(status(Some(&OpOutcome::Written), true), Ok(IndexStatus::Indexed));
        assert_eq!(status(Some(&OpOutcome::Written), false), Ok(IndexStatus::Deleted));
        assert_eq!(status(Some(&OpOutcome::Unchanged), true), Ok(IndexStatus::Unchanged));
        assert!(status(Some(&OpOutcome::Failed("x".into())), true).is_err());
    }
}
