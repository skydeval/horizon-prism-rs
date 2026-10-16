//! The follows pass (following-audience design §3.5, S10): for an owner of
//! a `following` audience, compare every stored follow with the owner's
//! repository to the end, so that a settle may set the follow-verification
//! flag. Jobs (`member`, `private`, `authz`) and `requestRecord`'s catch-up
//! run this one implementation, each through its own [`FollowsLister`].
//!
//! 2. List `app.bsky.graph.follow` to the end, up to the ceiling.
//! 3. Write what is missing or changed through the writer at R: a listed
//!    follow with no stored row (for a member owner, only among the newest
//!    10 000 by rkey, the member plan's own limit, C-F1); one whose stored
//!    row names another subject; and a delete at R for one whose subject
//!    does not parse but has a stored row. One stored with the same subject
//!    is left alone, so the follow counts do not move.
//! 4. Reconcile **every** stored follow the listing lacks, whatever its rev,
//!    with `getRecord`, under the pass's own budget: found naming the stored
//!    subject, kept; gone with `rev < R`, deleted at R; otherwise the stored
//!    row is read again, and unless the firehose moved it meanwhile, it is a
//!    contradiction: nothing is deleted, and the flag is cleared at once in
//!    its own transaction, whatever path the settle then takes.
//! 5. Complete when the listing reached its end under the ceiling, every
//!    candidate was confirmed, and no write failed or contradicted.
//!
//! A write or delete of a stored row the listing contradicts that fails is
//! a contradiction too (review 4, finding 4); a failed write of a follow
//! that was not stored only makes the pass incomplete.

use std::collections::{HashMap, HashSet};

use async_trait::async_trait;
use chrono::Utc;

use super::backfill::{ListPage, ListedRecord};
use super::classifier::NSID_FOLLOW;
use super::records::GraphRecord;
use super::writer::{Action, ApplyOptions, OpOutcome, RawOp, Writer, clear_follows_verified};

/// `follows_pass_total{outcome}`: passes by how they ended.
pub const METRIC_PASS: &str = "follows_pass_total";
/// `follows_pass_skipped_total`: listed follows whose subject does not parse
/// and that have no stored row; they name nobody.
pub const METRIC_SKIPPED: &str = "follows_pass_skipped_total";

/// What a lister call gave: a value, or a stop that leaves the pass
/// incomplete (catch-up's page cap, deadline or empty budget).
#[derive(Debug)]
pub enum Fetched<T> {
    Got(T),
    Stopped(&'static str),
}

/// Where a pass reads the owner's repository.
#[async_trait]
pub trait FollowsLister: Send {
    /// One `listRecords` page of `app.bsky.graph.follow`.
    async fn page(&mut self, cursor: Option<&str>) -> Result<Fetched<ListPage>, String>;
    /// `getRecord` of one follow; `None` only for an explicit
    /// `RecordNotFound`.
    async fn get_record(&mut self, rkey: &str) -> Result<Fetched<Option<ListedRecord>>, String>;
}

/// A pass's bounds.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PassSettings {
    /// `backfill.follows_pass_ceiling` (C-F2).
    pub ceiling: usize,
    /// For a member owner, the member plan's follow limit: a missing follow
    /// is written only among the newest this many by rkey (C-F1).
    pub newest_only: Option<usize>,
    /// The pass's own `getRecord` budget (`reconcile_confirm_limit`),
    /// separate from the job's.
    pub budget: u32,
    /// Rotates the reconcile order (`hash(uri ‖ run id)`).
    pub run_id: u64,
}

/// How a pass ended.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct PassOutcome {
    /// Every check of step 5 passed: a settle may set the flag.
    pub complete: bool,
    /// A stored row the repository contradicts stays: the flag was cleared
    /// in the pass's own transaction.
    pub contradiction: bool,
    /// Reconcile candidates were left when the budget ran out.
    pub unconfirmed: bool,
    /// The listing reached the ceiling with more to list.
    pub ceiling: bool,
    /// A write of a follow that was not stored failed (or a listed record
    /// lay outside the repository).
    pub write_failed: bool,
    /// The lister stopped: no token, the page cap or the deadline.
    pub stopped: Option<&'static str>,
}

impl PassOutcome {
    /// The metric label, most telling first.
    #[must_use]
    pub fn label(&self) -> &'static str {
        if self.complete {
            "complete"
        } else if self.contradiction {
            "contradiction"
        } else if self.ceiling {
            "ceiling"
        } else if let Some(reason) = self.stopped {
            reason
        } else if self.write_failed {
            "write_failed"
        } else {
            "unconfirmed"
        }
    }
}

/// A stored follow of the owner: subject and rev.
struct Stored {
    subject: String,
    rev: String,
}

/// Run steps 2–5 for `did` at rev `rev`, writing with `options`.
///
/// # Errors
///
/// A listing or `getRecord` failure, or a transient write failure: the
/// caller fails as any listing error does.
pub async fn run(
    writer: &Writer,
    options: ApplyOptions,
    lister: &mut dyn FollowsLister,
    did: &str,
    rev: &str,
    settings: PassSettings,
) -> Result<PassOutcome, String> {
    let mut out = PassOutcome::default();
    // 2. List to the end, up to the ceiling.
    let mut listed: Vec<ListedRecord> = Vec::new();
    let mut cursor: Option<String> = None;
    loop {
        let page = match lister.page(cursor.as_deref()).await? {
            Fetched::Got(page) => page,
            Fetched::Stopped(reason) => {
                out.stopped = Some(reason);
                return Ok(finish(out));
            }
        };
        let more = page.cursor.is_some() && !page.records.is_empty();
        listed.extend(page.records);
        if listed.len() >= settings.ceiling && more {
            tracing::warn!(event = "follows_pass_ceiling", did, listed = listed.len(), "the follows pass hit its ceiling; the flag stays unset");
            out.ceiling = true;
            return Ok(finish(out));
        }
        if !more {
            break;
        }
        cursor = page.cursor;
    }
    #[cfg(test)]
    crate::test_support::hooks::pause_in(crate::test_support::hooks::PASS_AFTER_LISTING, writer.pool(), did).await;
    let stored = stored_follows(writer, did).await.map_err(|e| e.to_string())?;
    let newest: Option<HashSet<String>> = settings.newest_only.map(|n| {
        let mut rkeys: Vec<&ListedRecord> = listed.iter().collect();
        rkeys.sort_by(|a, b| b.rkey().cmp(a.rkey()));
        rkeys.into_iter().take(n).map(|r| r.uri.clone()).collect()
    });

    // 3. Write what is missing or changed.
    let expected = format!("at://{did}/{NSID_FOLLOW}/");
    let now = Utc::now();
    let mut listed_uris: HashSet<&str> = HashSet::with_capacity(listed.len());
    for record in &listed {
        listed_uris.insert(record.uri.as_str());
        if !record.uri.starts_with(&expected) {
            tracing::warn!(event = "follows_pass_record_skipped", did, uri = %record.uri, "listed record outside the repo/collection");
            out.write_failed = true;
            continue;
        }
        let subject = GraphRecord::parse(&record.value, now).ok().map(|g| g.subject_did);
        let row = stored.get(&record.uri);
        let op = match (&subject, row) {
            (Some(_), None) if newest.as_ref().is_some_and(|n| !n.contains(&record.uri)) => continue,
            (Some(_), None) => upsert(record),
            (Some(s), Some(row)) if *s == row.subject => continue,
            (Some(_), Some(_)) => upsert(record),
            (None, Some(_)) => delete(record.rkey()),
            (None, None) => {
                metrics::counter!(METRIC_SKIPPED).increment(1);
                continue;
            }
        };
        if write(writer, options, did, rev, op).await? {
            if row.is_some() {
                contradiction(writer, did, &mut out, &record.uri).await?;
            } else {
                out.write_failed = true;
            }
        }
    }

    // 4. Reconcile every stored follow the listing lacks, whatever its rev.
    let mut candidates: Vec<(&String, &Stored)> = stored.iter().filter(|(uri, _)| !listed_uris.contains(uri.as_str())).collect();
    let run = settings.run_id.to_le_bytes();
    candidates.sort_by_cached_key(|(uri, _)| {
        let mut h = blake3::Hasher::new();
        h.update(uri.as_bytes());
        h.update(&run);
        *h.finalize().as_bytes()
    });
    let mut budget = settings.budget;
    for (uri, row) in candidates {
        if budget == 0 {
            out.unconfirmed = true;
            break;
        }
        budget -= 1;
        let rkey = uri.rsplit('/').next().unwrap_or_default();
        let found = match lister.get_record(rkey).await? {
            Fetched::Got(found) => found,
            Fetched::Stopped(reason) => {
                out.stopped = Some(reason);
                break;
            }
        };
        let found_subject = found.as_ref().map(|r| GraphRecord::parse(&r.value, now).ok().map(|g| g.subject_did));
        match found_subject {
            // Found, naming the stored subject: the listing missed it.
            Some(Some(s)) if s == row.subject => {}
            // Gone, older than R: deleted at R, as reconcile does.
            None if row.rev.as_str() < rev => {
                if write(writer, options, did, rev, delete(rkey)).await? {
                    contradiction(writer, did, &mut out, uri).await?;
                } else {
                    tracing::info!(event = "follows_pass_deleted", did, %uri, "confirmed unfollow reconciled");
                }
            }
            // Found naming another subject or none, or gone at or above R.
            _ => {
                #[cfg(test)]
                crate::test_support::hooks::pause_in(crate::test_support::hooks::PASS_BEFORE_REREAD, writer.pool(), did).await;
                let now_rev: Option<String> = sqlx::query_scalar("SELECT rev FROM follows WHERE uri = $1")
                    .bind(uri)
                    .fetch_optional(writer.pool())
                    .await
                    .map_err(|e| e.to_string())?;
                // The firehose moved it meanwhile: resolved.
                if now_rev.as_deref() == Some(row.rev.as_str()) {
                    contradiction(writer, did, &mut out, uri).await?;
                }
            }
        }
    }
    Ok(finish(out))
}

/// Step 5, and the metric.
fn finish(mut out: PassOutcome) -> PassOutcome {
    out.complete = !out.ceiling && out.stopped.is_none() && !out.unconfirmed && !out.contradiction && !out.write_failed;
    metrics::counter!(METRIC_PASS, "outcome" => out.label()).increment(1);
    out
}

/// A stored row the repository contradicts, or one whose fix failed: the
/// flag is cleared and the counter replaced at once, in a transaction of
/// its own, so no earlier flag survives the pass whatever its settle does.
async fn contradiction(writer: &Writer, did: &str, out: &mut PassOutcome, uri: &str) -> Result<(), String> {
    tracing::info!(event = "follows_pass_contradiction", did, %uri, "a stored follow the repository contradicts; the flag is cleared");
    if !out.contradiction {
        let mut conn = writer.pool().acquire().await.map_err(|e| e.to_string())?;
        clear_follows_verified(&mut conn, did).await.map_err(|e| e.to_string())?;
    }
    out.contradiction = true;
    Ok(())
}

async fn stored_follows(writer: &Writer, did: &str) -> Result<HashMap<String, Stored>, sqlx::Error> {
    let rows: Vec<(String, String, String)> = sqlx::query_as("SELECT uri, subject_did, rev FROM follows WHERE author_did = $1")
        .bind(did)
        .fetch_all(writer.pool())
        .await?;
    Ok(rows.into_iter().map(|(uri, subject, rev)| (uri, Stored { subject, rev })).collect())
}

fn upsert(record: &ListedRecord) -> RawOp {
    RawOp {
        action: Action::Create,
        collection: NSID_FOLLOW.to_string(),
        rkey: record.rkey().to_string(),
        cid: Some(record.cid.clone()),
        record: Some(record.value.clone()),
    }
}

fn delete(rkey: &str) -> RawOp {
    RawOp {
        action: Action::Delete,
        collection: NSID_FOLLOW.to_string(),
        rkey: rkey.to_string(),
        cid: None,
        record: None,
    }
}

/// One op in its own transaction at rev R. Returns whether it failed
/// deterministically after the writer's attempts.
async fn write(writer: &Writer, options: ApplyOptions, did: &str, rev: &str, op: RawOp) -> Result<bool, String> {
    let report = writer.apply(did, rev, &[op], options).await.map_err(|e| e.0)?;
    Ok(matches!(report.outcomes.first(), Some(OpOutcome::Failed(_))))
}
