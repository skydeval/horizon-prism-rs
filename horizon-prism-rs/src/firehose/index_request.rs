//! The index-request procedure (design-v2 §5.9), shared by
//! `app.nearhorizon.index.requestRecord` and `admin deadletter replay`:
//! 1. resolve the repo's PDS (§6.7);
//! 2. `getLatestCommit` gives rev R, fetched first so R lower-bounds the
//!    record state;
//! 3. `getRecord`;
//! 4. the normal write path (§5.4, §5.5, §9.3) with rev R: found →
//!    create/update, `RecordNotFound` → delete.

use super::backfill::BackfillSource;
use super::writer::{Action, ApplyOptions, OpOutcome, RawOp, Writer};

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
    };
    let report = writer.apply(repo, &rev, &[op], options).await.map_err(|e| IndexError::Write(e.0))?;
    let status = match report.outcomes.first() {
        Some(OpOutcome::Written) if record.is_some() => IndexStatus::Indexed,
        Some(OpOutcome::Written) => IndexStatus::Deleted,
        Some(OpOutcome::Rejected(_) | OpOutcome::Skipped(_)) => IndexStatus::OutOfScope,
        Some(OpOutcome::Failed(e)) => return Err(IndexError::Write(e.clone())),
        Some(OpOutcome::Unchanged) | None => IndexStatus::Unchanged,
    };
    Ok(Indexed { rev, status })
}
