//! The index-request procedure (design-v2 §5.9), shared by
//! `app.nearhorizon.index.requestRecord` and `admin deadletter replay`:
//! 1. resolve the repo's PDS (§6.7);
//! 2. `getLatestCommit` gives rev R, fetched first so R lower-bounds the
//!    record state;
//! 3. `getRecord`;
//! 4. the normal write path (§5.4, §5.5, §9.3) with rev R: found →
//!    create/update, `RecordNotFound` → delete.

use super::backfill::BackfillSource;
use super::writer::{Action, ApplyOptions, OpOutcome, RawOp, WriteSource, Writer};

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
        source: WriteSource::IndexRequest,
    };
    let report = writer.apply(repo, &rev, &[op], options).await.map_err(|e| IndexError::Write(e.0))?;
    Ok(Indexed { rev, status: status(report.outcomes.first(), record.is_some())? })
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
