//! Writer lanes: what one lane applies, and what it hands back.
//!
//! A repo's entries always go to the same lane (`hash(repo) % lanes`), so
//! they apply in stream order; lanes apply side by side. A lane retries a
//! transaction another lane's conflicted with (`40P01`, `40001`) in place;
//! any other failure stops it, and the consumer books only what committed
//! in stream order before the first entry that did not.

use std::collections::HashMap;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::sync::Mutex;
use std::time::Duration;

use chrono::{DateTime, Utc};
use redis::aio::ConnectionManager;
use sqlx::PgPool;

use super::super::deadletter::{self, DeadLetter};
use super::super::dedupe::{self, CopyKey};
use super::super::router::route_identity;
use super::super::stream::{Commit, IdentityKind, Readable, StreamEntry, readable_despite, repo_hint};
use super::super::tier::CopyTier;
use super::super::watermark::{Watermarks, contribution};
use super::super::writer::{ApplyOptions, ApplyReport, LiveCommit, Transient, Writer};
use super::{ConsumerError, EntryPath};
use crate::subscriptions::triggers;

/// Metric: entries handed to each lane in the round in progress.
pub const METRIC_LANE_QUEUE: &str = "firehose_lane_queue_entries";
/// Metric: a lane's in-place retries of a conflicting transaction, by lane
/// and SQLSTATE.
pub const METRIC_LANE_RETRIES: &str = "firehose_lane_retries_total";
/// Metric: the time each lane spent applying. Summed over lanes it can
/// exceed wall time.
pub const METRIC_LANE_BUSY: &str = "firehose_lane_busy_seconds_total";
/// How often a lane retries a conflicting transaction before the failure
/// stops it.
pub const CONFLICT_RETRIES: u32 = 5;
/// Metric: transactions of several commits that failed and were run again
/// one commit per transaction, by lane.
pub const METRIC_LANE_BATCH_SPLITS: &str = "firehose_lane_batch_splits_total";

/// A commit copy's relay watermark contribution.
fn watermark_of(w: &CommitWork<'_>) -> Option<(String, DateTime<Utc>)> {
    contribution(w.entry.id_ms(), w.commit.time).map(|c| (w.label.clone(), c))
}

/// The lane of a repo.
#[must_use]
pub fn lane_of(repo: &str, lanes: usize) -> usize {
    if lanes <= 1 {
        return 0;
    }
    let mut h = DefaultHasher::new();
    repo.hash(&mut h);
    usize::try_from(h.finish() % lanes as u64).unwrap_or(0)
}

/// What an entry still needs once the consumer has read it.
#[derive(Debug)]
pub(super) enum Work {
    /// Nothing more: a duplicate copy, or an entry that writes nothing.
    Done(Done),
    /// An entry that did not decode, with the reason.
    Undecodable(String),
    Sync { did: String, rev: String, tier: CopyTier, key: Option<CopyKey>, label: String },
    Identity { kind: IdentityKind, did: String, relay: Option<String>, active: Option<bool>, status: Option<String>, label: String },
    Commit { commit: Commit, label: String, tier: CopyTier, key: Option<CopyKey> },
}

impl Work {
    /// The DID whose lane applies it.
    pub(super) fn repo(&self, entry: &StreamEntry) -> String {
        match self {
            Self::Commit { commit, .. } => commit.repo.clone(),
            Self::Sync { did, .. } | Self::Identity { did, .. } => did.clone(),
            Self::Undecodable(_) => repo_hint(entry).0,
            Self::Done(_) => String::new(),
        }
    }
}

/// An entry's result, booked in stream order once its round is done.
#[derive(Debug)]
pub(super) struct Done {
    pub path: EntryPath,
    /// Its thread outline ops.
    pub outline: Vec<crate::thread_outline::ops::Op>,
    /// Its relay's watermark contribution.
    pub watermark: Option<(String, DateTime<Utc>)>,
    /// The copy to mark seen.
    pub seen: Option<Seen>,
    /// A duplicate copy, dropped.
    pub duplicate: bool,
}

impl Done {
    pub(super) fn nothing() -> Self {
        Self { path: EntryPath::NoTx, outline: Vec::new(), watermark: None, seen: None, duplicate: false }
    }

    /// A copy of `label`'s another relay's copy already applied: counted,
    /// and its relay's watermark still moves.
    pub(super) fn duplicate(label: &str, watermark: Option<(String, DateTime<Utc>)>) -> Self {
        metrics::counter!(dedupe::METRIC_DROPPED, "relay" => label.to_string()).increment(1);
        Self { watermark, duplicate: true, ..Self::nothing() }
    }
}

/// A committed copy with a key: marked seen when its round is booked.
#[derive(Debug, Clone)]
pub(super) struct Seen {
    pub repo: String,
    pub key: CopyKey,
    /// Its relay's label.
    pub relay: String,
    /// Its stream entry id's milliseconds.
    pub ms: Option<i64>,
}

/// The low-water entry: the oldest entry read and not yet committed or
/// acked, across every lane; when nothing is in flight, the last entry
/// read. `None` before the first read after a start.
#[derive(Debug, Default)]
pub struct LowWater(Mutex<Option<String>>);

impl LowWater {
    #[must_use]
    pub fn get(&self) -> Option<String> {
        self.0.lock().unwrap_or_else(std::sync::PoisonError::into_inner).clone()
    }

    pub(crate) fn set(&self, id: Option<&str>) {
        if let Some(id) = id {
            *self.0.lock().unwrap_or_else(std::sync::PoisonError::into_inner) = Some(id.to_string());
        }
    }
}

/// One lane's own handles.
pub(super) struct Lane {
    pub index: usize,
    pub label: String,
    pub writer: Writer,
    pub pool: PgPool,
    pub redis: ConnectionManager,
    pub dead_letter_stream: String,
    pub dead_letter_maxlen: u64,
    pub options: ApplyOptions,
    pub outline: bool,
    pub watermarks: Watermarks,
    pub system_accounts: crate::system_accounts::State,
    pub sync_min_interval_secs: u32,
    /// The copies this lane committed in the round in progress, by repo:
    /// a later copy of one of them in the same round is a duplicate, since
    /// the consumer marks copies seen only once the round is booked.
    pub seen: HashMap<String, Vec<CopyKey>>,
    /// The most commits applied in one writer transaction.
    pub batch: usize,
}

/// A commit a lane applies: its entry's place in the round, the entry, and
/// what the consumer read from it.
pub(super) struct CommitWork<'e> {
    pub at: usize,
    pub entry: &'e StreamEntry,
    pub commit: Commit,
    pub label: String,
    pub tier: CopyTier,
    pub key: Option<CopyKey>,
}

impl Lane {
    pub(super) fn book_busy(&self, d: Duration) {
        metrics::gauge!(METRIC_LANE_BUSY, "lane" => self.label.clone()).increment(d.as_secs_f64());
        super::super::projection::totals().lanes_busy_us.fetch_add(u64::try_from(d.as_micros()).unwrap_or(u64::MAX), std::sync::atomic::Ordering::Relaxed);
    }

    fn duplicate_here(&self, repo: &str, key: Option<&CopyKey>) -> bool {
        key.is_some_and(|k| self.seen.get(repo).is_some_and(|keys| keys.iter().any(|s| k.dropped_by(s))))
    }

    fn committed(&mut self, repo: &str, key: Option<CopyKey>, relay: &str, ms: Option<i64>) -> Option<Seen> {
        let key = key?;
        self.seen.entry(repo.to_string()).or_default().push(key);
        Some(Seen { repo: repo.to_string(), key, relay: relay.to_string(), ms })
    }

    /// Apply one entry.
    pub(super) async fn run(&mut self, entry: &StreamEntry, work: Work) -> Result<Done, ConsumerError> {
        match work {
            Work::Done(done) => Ok(done),
            Work::Undecodable(error) => self.undecodable(entry, error).await,
            Work::Sync { did, rev, tier, key, label } => {
                if self.duplicate_here(&did, key.as_ref()) {
                    return Ok(Done::duplicate(&label, None));
                }
                // In full, a repo reset is a hole in its chain at once.
                if let Some(ms) = entry.id_ms() {
                    self.writer.apply_sync(&did, &rev, tier, ms).await?;
                }
                let seen = self.committed(&did, key, &label, entry.id_ms());
                Ok(Done { seen, ..Done::nothing() })
            }
            Work::Identity { kind, did, relay, active, status, label } => self.identity(entry, kind, &did, relay.as_deref(), active, status.as_deref(), &label).await,
            Work::Commit { commit, label, tier, key } => {
                let mut out = self.commits(vec![CommitWork { at: 0, entry, commit, label, tier, key }], None).await;
                out.pop().map_or_else(|| Err(ConsumerError::Shape("a writer lane applied nothing".into())), |(_, r)| r)
            }
        }
    }

    async fn undecodable(&mut self, entry: &StreamEntry, error: String) -> Result<Done, ConsumerError> {
        let readable = readable_despite(entry);
        // Thread outline design §2, §3.1: a bad-rev commit's post ops, and a
        // parseable dead letter's, record holes.
        let outline = if self.outline { crate::thread_outline::ops::refused_ops(entry, readable.as_ref()) } else { Vec::new() };
        // A refusal that could not matter is counted and dropped
        // (actionable-warnings design §6): an event naming no DID, or a
        // commit refused for its rev alone whose ops, with a valid rev,
        // would have written nothing.
        let dropped = match readable {
            Some(Readable::NotADid) => Some("did"),
            Some(Readable::Commit { repo, ops }) => (!self.writer.would_write(&repo, &ops).await).then_some("rev"),
            None => None,
        };
        if let Some(check) = dropped {
            metrics::counter!(deadletter::METRIC_MALFORMED_DROPPED, "check" => check).increment(1);
            tracing::debug!(event = "firehose_malformed_dropped", id = %entry.id, check, %error, "a malformed entry that could not matter was dropped");
            return Ok(Done { outline, ..Done::nothing() });
        }
        // Deterministic: dead-letter the whole entry (§6.11.6).
        let (repo, rev) = repo_hint(entry);
        let letter = DeadLetter {
            orig_id: entry.id.clone(),
            entry_type: entry.field("type").unwrap_or_default().to_string(),
            seq: entry.field("seq").unwrap_or_default().to_string(),
            repo,
            rev,
            error,
            data: entry.raw("data").map(<[u8]>::to_vec).unwrap_or_default(),
            ..DeadLetter::default()
        };
        self.dead_letter(&[letter]).await?;
        Ok(Done { path: EntryPath::DeadLetter, outline, ..Done::nothing() })
    }

    #[allow(clippy::too_many_arguments)]
    async fn identity(
        &mut self,
        entry: &StreamEntry,
        kind: IdentityKind,
        did: &str,
        relay: Option<&str>,
        active: Option<bool>,
        status: Option<&str>,
        label: &str,
    ) -> Result<Done, ConsumerError> {
        // In full, a takedown relay's account statuses are recorded.
        if kind == IdentityKind::Account && active.is_some() {
            let (full, relays, hides, groups, restored) = {
                let sets = self.writer.scope().read_guard().await;
                let restored = sets.is_restored(did);
                let hides = sets.full.takedown_relays.iter().any(|r| r.as_str() == label) && !restored && !sets.is_member(did);
                let groups: Vec<String> = sets.full.kinds.iter().map(|g| g.as_str().to_string()).collect();
                (sets.public.breadth == crate::indexing::Breadth::Full, sets.full.status_relays.clone(), hides, groups, restored)
            };
            if full && relays.iter().any(|r| r.as_str() == label) {
                // One transaction for the event's statements.
                let mut tx = self.pool.begin().await?;
                crate::indexing::full::record_relay_status_in(&mut tx, did, label, active, status).await?;
                // A deactivation or a deletion hides the account at once,
                // until the refresher confirms or undoes it; never for an
                // account Refract lists.
                if active == Some(false) && !restored {
                    crate::indexing::full::provisional_status_in(&mut tx, did, status).await?;
                }
                // The crawl follows the account: a deletion or a suspension
                // refuses an in-flight crawl, a return to active queues one.
                crate::indexing::crawl::state::account_status_in(&mut tx, did, active, status, hides, &groups).await?;
                tx.commit().await?;
            } else if full && active == Some(false) && label == super::super::watermark::UNKNOWN_RELAY {
                tracing::warn!(event = "firehose_unaliased_status", relay = ?relay, %did, "an account status from a relay URL with no label or alias; list it in firehose.relay_aliases if it stands in for a takedown relay");
            }
        }
        let outline = match crate::thread_outline::ops::status_relay(&self.watermarks, relay) {
            Some(key) if self.outline => crate::thread_outline::ops::status_op(kind, did, &key, active, entry.seq()).into_iter().collect(),
            _ => Vec::new(),
        };
        let queued = route_identity(&self.pool, kind, did, &self.system_accounts).await?;
        // The live feed counts every event and shows the queued ones
        // (admin-ui-live-feed design §4.1).
        if let Some(tap) = self.writer.feed() {
            tap.identity(kind, did, queued, Some(self.watermarks.label_for(relay)));
        }
        // Rule 4 (§4.3): an owner of a subscribed list that looks gone is
        // looked at again; an ordinary owner's events cause no sync.
        let owner_event = matches!(kind, IdentityKind::Identity | IdentityKind::Account);
        if owner_event && self.writer.scope().read_guard().await.subscribed.tracks(did) {
            triggers::owner_event(&self.pool, did, self.sync_min_interval_secs).await?;
        }
        Ok(Done { path: EntryPath::Identity, outline, ..Done::nothing() })
    }

    /// Apply consecutive commits of this lane in order, in writer
    /// transactions of at most `batch` commits. A copy of a commit applied
    /// earlier in the round, or earlier in the same transaction, is a
    /// duplicate, dropped once its original committed. A conflicting
    /// transaction is retried in place; a transaction of several commits
    /// that still fails is run again one commit per transaction, so a
    /// single bad commit fails alone, and then batches resume. A failed
    /// commit stops the lane: results end at it. `stop`, when another
    /// lane failed, ends the lane before its next transaction.
    pub(super) async fn commits(&mut self, run: Vec<CommitWork<'_>>, stop: Option<&std::sync::atomic::AtomicBool>) -> Vec<(usize, Result<Done, ConsumerError>)> {
        let mut out: Vec<(usize, Result<Done, ConsumerError>)> = Vec::with_capacity(run.len());
        let mut pos = 0;
        // Before this index, commits run one per transaction.
        let mut singles_until = 0;
        while pos < run.len() {
            if stop.is_some_and(|s| s.load(std::sync::atomic::Ordering::Acquire)) {
                break;
            }
            let limit = if pos < singles_until { 1 } else { self.batch.max(1) };
            // The transaction's commits, and copies of them behind them.
            let mut slice: Vec<usize> = Vec::new();
            let mut dups: Vec<usize> = Vec::new();
            let mut next = run.len();
            for (j, w) in run.iter().enumerate().skip(pos) {
                if self.duplicate_here(&w.commit.repo, w.key.as_ref()) {
                    if slice.is_empty() {
                        out.push((w.at, Ok(Done::duplicate(&w.label, watermark_of(w)))));
                        pos = j + 1;
                        continue;
                    }
                    dups.push(j);
                    continue;
                }
                let behind = w.key.is_some_and(|k| slice.iter().any(|&o| run[o].commit.repo == w.commit.repo && run[o].key.is_some_and(|ok| k.dropped_by(&ok))));
                if behind {
                    dups.push(j);
                    continue;
                }
                if slice.len() == limit {
                    next = j;
                    break;
                }
                slice.push(j);
            }
            if slice.is_empty() {
                continue;
            }
            #[cfg(test)]
            if let Some(db) = self.pool.connect_options().get_database() {
                for &j in &slice {
                    crate::test_support::hooks::pause(crate::test_support::hooks::LANE_BEFORE_ENTRY, &format!("{db}/{}", run[j].commit.repo)).await;
                }
            }
            let reports = self.apply(&run, &slice).await;
            match reports {
                Ok(reports) => {
                    let applied = reports.len();
                    for (k, report) in reports.into_iter().enumerate() {
                        let w = &run[slice[k]];
                        let done = self.committed_commit(w, report).await;
                        let failed = done.is_err();
                        out.push((w.at, done));
                        if failed {
                            return out;
                        }
                    }
                    // Where the next transaction starts: the first commit
                    // not applied; copies before it had their original
                    // applied.
                    let resume = slice.get(applied).copied().unwrap_or(next);
                    for &j in dups.iter().filter(|&&j| j < resume) {
                        let w = &run[j];
                        out.push((w.at, Ok(Done::duplicate(&w.label, watermark_of(w)))));
                    }
                    pos = resume;
                }
                Err(e) if slice.len() > 1 => {
                    tracing::debug!(event = "firehose_lane_batch_failed", lane = self.index, commits = slice.len(), error = %e, "a batch failed; its commits run one per transaction");
                    metrics::counter!(METRIC_LANE_BATCH_SPLITS, "lane" => self.label.clone()).increment(1);
                    singles_until = slice.last().map_or(pos, |l| l + 1);
                }
                Err(e) => {
                    out.push((run[slice[0]].at, Err(e.into())));
                    return out;
                }
            }
        }
        out
    }

    /// One applied commit's result: its malformed ops counted or
    /// dead-lettered, its copy marked committed in the round.
    async fn committed_commit(&mut self, w: &CommitWork<'_>, report: ApplyReport) -> Result<Done, ConsumerError> {
        let entry = w.entry;
        // A malformed op that could not matter is counted, not dead-lettered
        // (actionable-warnings design §6.3).
        let dropped = report.failed.iter().filter(|f| !f.matters).count();
        if dropped > 0 {
            metrics::counter!(deadletter::METRIC_MALFORMED_DROPPED, "check" => "record").increment(dropped as u64);
            tracing::debug!(event = "firehose_malformed_dropped", id = %entry.id, check = "record", repo = %w.commit.repo, ops = dropped, "malformed ops that could not matter were dropped");
        }
        if report.failed.iter().any(|f| f.matters) {
            let data = entry.raw("data").map(<[u8]>::to_vec).unwrap_or_default();
            let letters: Vec<DeadLetter> = report
                .failed
                .iter()
                .filter(|f| f.matters)
                .map(|f| DeadLetter {
                    orig_id: entry.id.clone(),
                    entry_type: "commit".to_string(),
                    relay: w.label.clone(),
                    seq: entry.field("seq").unwrap_or_default().to_string(),
                    repo: w.commit.repo.clone(),
                    rev: w.commit.rev.clone(),
                    action: f.action.as_str().to_string(),
                    path: f.path.clone(),
                    error: f.error.clone(),
                    data: data.clone(),
                })
                .collect();
            self.dead_letter(&letters).await?;
        }
        let seen = self.committed(&w.commit.repo, w.key, &w.label, entry.id_ms());
        Ok(Done {
            path: if report.transaction { EntryPath::Tx } else { EntryPath::NoTx },
            outline: report.outline,
            watermark: watermark_of(w),
            seen,
            duplicate: false,
        })
    }

    /// The writer's call for the commits `slice` of `run`, retried in place
    /// when another lane's transaction conflicted with it.
    async fn apply(&self, run: &[CommitWork<'_>], slice: &[usize]) -> Result<Vec<ApplyReport>, Transient> {
        let items: Vec<LiveCommit<'_>> = slice
            .iter()
            .map(|&j| {
                let w = &run[j];
                LiveCommit {
                    relay: Some(&w.label),
                    repo: &w.commit.repo,
                    rev: &w.commit.rev,
                    since: w.commit.since.as_deref(),
                    ops: &w.commit.ops,
                    // A tracked repo's chain is advanced only by a whole
                    // copy: any other copy may have dropped a commit its
                    // filter left out, in every public-indexing mode.
                    options: ApplyOptions { advance_applied_rev: w.tier == CopyTier::Whole, ..self.options },
                    tier: w.tier,
                    stream: w.entry.id_ms().map(|ms| crate::firehose::writer::LiveStream { ms, stamped: w.commit.cap.is_some() }),
                }
            })
            .collect();
        let mut tries = 0;
        loop {
            let result = self.writer.apply_batch(&items, items.len()).await;
            match result {
                Err(t) if tries < CONFLICT_RETRIES && t.conflict().is_some() => {
                    tries += 1;
                    let code = t.conflict().unwrap_or_default().to_string();
                    tracing::debug!(event = "firehose_lane_retry", lane = self.index, repo = %items[0].repo, %code, tries, "a conflicting transaction is retried in place");
                    metrics::counter!(METRIC_LANE_RETRIES, "lane" => self.label.clone(), "code" => code).increment(1);
                    tokio::time::sleep(Duration::from_millis(5 * u64::from(tries))).await;
                }
                other => return other,
            }
        }
    }

    async fn dead_letter(&mut self, letters: &[DeadLetter]) -> Result<(), ConsumerError> {
        deadletter::write(&mut self.redis, &self.dead_letter_stream, self.dead_letter_maxlen, letters).await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_repo_always_has_the_same_lane() {
        assert_eq!(lane_of("did:plc:example1", 1), 0);
        for lanes in [2, 3, 8] {
            let mut used = std::collections::BTreeSet::new();
            for i in 0..200 {
                let repo = format!("did:plc:example{i}");
                let lane = lane_of(&repo, lanes);
                assert!(lane < lanes);
                assert_eq!(lane, lane_of(&repo, lanes));
                used.insert(lane);
            }
            assert_eq!(used.len(), lanes, "every lane gets repos");
        }
    }
}
