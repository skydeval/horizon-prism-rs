//! One claimed account's crawl: its directory entry, its repository (whole,
//! or per collection when too large), verification against its signing
//! key, the records of the claimed groups applied as
//! [`WriteSource::Crawl`] in transactions of `[full_crawl] batch_ops`, the
//! reconcile of stored rows the repository no longer holds, and the guarded
//! `done`. Every account and record key the records name is interned first;
//! a claimer starts no transaction while live ingest is far behind.

use std::collections::{BTreeMap, HashSet};

use chrono::{DateTime, Utc};
use serde_json::Value;

use super::client::{IdentityMiss, RepoFetch};
use super::state::{self, Claim, DoneOutcome, Fetched};
use super::task::Crawler;
use crate::config::FullGroup;
use crate::firehose::classifier::{
    NSID_ACTOR_STATUS, NSID_BLOCK, NSID_CONTENT_VISIBILITY, NSID_FOLLOW, NSID_LABELER_SERVICE, NSID_LIKE, NSID_LIST, NSID_LIST_OPTOUT, NSID_LISTBLOCK,
    NSID_LISTITEM, NSID_NOTIFICATION_DECLARATION, NSID_POLL_POST, NSID_POST, NSID_POSTGATE, NSID_PROFILE, NSID_REPOST, NSID_STARTER_PACK,
    NSID_THREADGATE, PLURAL_COLLECTIONS,
};
use crate::firehose::writer::{Action, ApplyOptions, OpOutcome, RawOp, WriteSource};

/// Accounts or record keys interned in one statement, before applying.
pub const INTERN_CHUNK: usize = 5_000;
/// How often a claimer that waits for live ingest renews its lease, at most.
pub const YIELD_RENEW: std::time::Duration = std::time::Duration::from_secs(60);
/// Metric: seconds claimers waited for live ingest.
pub const METRIC_YIELD_SECONDS: &str = "full_crawl_yield_seconds_total";

/// How one account's crawl ended.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Outcome {
    Done { records: usize, deleted: usize, marks: usize, whole: bool, car_bytes: usize },
    /// The repository was read per collection: applied, not verified.
    Unverified { records: usize },
    Skipped(String),
    Gone(String),
    Failed(String),
    /// The claim no longer held, or the CAR was older than the first
    /// sighting; the row is `pending` again.
    Refused,
}

/// The collections of the claimed groups, and the plural records kept for
/// any author.
fn claimed_collections(groups: &[String]) -> HashSet<&'static str> {
    let mut out: HashSet<&'static str> = groups.iter().filter_map(|g| FullGroup::parse(g)).flat_map(|g| crate::indexing::full::collections(g).iter().copied()).collect();
    out.extend(PLURAL_COLLECTIONS.iter().copied());
    out
}

/// The self records (rkey `self`) of the profiles group.
const SELF_RECORDS: [&str; 5] = [NSID_PROFILE, NSID_LABELER_SERVICE, NSID_NOTIFICATION_DECLARATION, NSID_ACTOR_STATUS, NSID_CONTENT_VISIBILITY];

/// When a record was made: its `createdAt`, else its rkey's TID.
fn made_at(record: &Value, rkey: &str) -> Option<DateTime<Utc>> {
    record
        .get("createdAt")
        .and_then(Value::as_str)
        .and_then(|s| DateTime::parse_from_rfc3339(s).ok())
        .map(|d| d.with_timezone(&Utc))
        .or_else(|| DateTime::from_timestamp_micros(i64::try_from(crate::db::ids::tid_u64(rkey)? >> 10).ok()?))
}

impl Crawler {
    fn options(&self) -> ApplyOptions {
        ApplyOptions { advance_applied_rev: false, dead_letter_readiness: false, attempts: 3, live: false, subscription_rules: true, source: WriteSource::Crawl }
    }

    /// Crawl one claimed account to its end. Errors are database errors;
    /// every other failure is the row's outcome.
    ///
    /// # Errors
    ///
    /// Any database error.
    pub async fn process(&self, c: &Claim, trial_history: bool) -> Result<Outcome, sqlx::Error> {
        // A full trial with history never crawls S or a member: the start
        // mode holds their data.
        if trial_history {
            let sets = self.scope.read_guard().await;
            if sets.in_scope(&c.did) || sets.is_member(&c.did) {
                drop(sets);
                state::settle(&self.pool, c, "skipped", Some("trial")).await?;
                return Ok(Outcome::Skipped("trial".into()));
            }
        }
        // Owing nothing enabled: done at once, nothing fetched.
        if c.groups.is_empty() {
            state::settle(&self.pool, c, "done", None).await?;
            return Ok(Outcome::Done { records: 0, deleted: 0, marks: 0, whole: false, car_bytes: 0 });
        }
        let identity = match self.identity(&c.did, false).await {
            Ok(i) => i,
            Err(IdentityMiss::Gone(reason)) => {
                state::settle(&self.pool, c, "gone", Some("identity")).await?;
                return Ok(Outcome::Gone(reason));
            }
            Err(IdentityMiss::Failed(reason)) => return self.fail(c, &reason).await,
        };
        let fetched = match self.source.get_repo(&identity.pds, &c.did, self.crawl.max_car_bytes).await {
            Ok(f) => f,
            Err(reason) => return self.fail(c, &reason).await,
        };
        let car = match fetched {
            RepoFetch::Car(bytes) => bytes,
            RepoFetch::NotFound => {
                state::settle(&self.pool, c, "gone", Some("repo_not_found")).await?;
                return Ok(Outcome::Gone("repo not found".into()));
            }
            RepoFetch::Inactive(status) => {
                // Hidden under `refract_only` unless Refract lists it; in
                // `"all"`, a PDS that does not serve the repo still has
                // nothing to crawl, so it waits to come back either way.
                state::settle(&self.pool, c, "skipped", Some(&status)).await?;
                return Ok(Outcome::Skipped(status));
            }
            RepoFetch::TooLarge => return self.per_collection(c, &identity.pds, trial_history).await,
        };
        let car_bytes = car.len();
        metrics::histogram!(super::METRIC_CAR_BYTES).record(f64::from(u32::try_from(car_bytes).unwrap_or(u32::MAX)));
        let key = if self.crawl.verify_signatures { identity.key.clone() } else { None };
        if self.crawl.verify_signatures && key.is_none() {
            return self.fail(c, "the DID document names no signing key").await;
        }
        let repo = match proto_blue::repo::verify_repo_car(&car, Some(&c.did), key.as_deref()) {
            Ok(r) => r,
            Err(proto_blue::repo::RepoError::InvalidSignature) => {
                // A key may have rotated since it was read: read the
                // document once more before it counts.
                let again = match self.identity(&c.did, true).await {
                    Ok(i) => i.key,
                    Err(_) => None,
                };
                match again.as_deref().map(|k| proto_blue::repo::verify_repo_car(&car, Some(&c.did), Some(k))) {
                    Some(Ok(r)) => r,
                    _ => return self.fail(c, "bad_signature").await,
                }
            }
            Err(e) => return self.fail(c, &format!("repository: {e}")).await,
        };
        let car_rev = repo.rev().to_string();
        let wanted = claimed_collections(&c.groups);
        let cutoff = (self.crawl.skip_older_than_days > 0).then(|| Utc::now() - chrono::TimeDelta::days(i64::from(self.crawl.skip_older_than_days)));
        let mut present: BTreeMap<&'static str, HashSet<String>> = BTreeMap::new();
        let mut ops: Vec<RawOp> = Vec::new();
        for leaf in repo.mst.leaves() {
            let Some((collection, rkey)) = leaf.key.split_once('/') else {
                continue;
            };
            let Some(&nsid) = wanted.get(collection) else {
                continue;
            };
            present.entry(nsid).or_default().insert(format!("at://{}/{collection}/{rkey}", c.did));
            let record = match repo.get_record(&leaf.value) {
                Ok(Some(v)) => proto_blue::lex_json::lex_to_json(&v),
                _ => continue,
            };
            if cutoff.is_some_and(|cut| made_at(&record, rkey).is_some_and(|at| at < cut)) {
                continue;
            }
            ops.push(RawOp { action: Action::Create, collection: collection.to_string(), rkey: rkey.to_string(), cid: Some(leaf.value.to_string()), record: Some(record) });
        }
        drop(repo);
        drop(car);
        let records = ops.len();
        if !self.write(c, &car_rev, &ops).await? {
            return Ok(Outcome::Refused);
        }
        let deleted = self.reconcile(c, &car_rev, &wanted, &present, trial_history).await?;
        match state::done(&self.pool, c, &Fetched { car_rev, verified: true }).await? {
            DoneOutcome::Done { marks, whole } => Ok(Outcome::Done { records, deleted, marks, whole, car_bytes }),
            DoneOutcome::OlderThanChain | DoneOutcome::Refused => Ok(Outcome::Refused),
        }
    }

    async fn fail(&self, c: &Claim, reason: &str) -> Result<Outcome, sqlx::Error> {
        tracing::debug!(event = "full_crawl_failed", did = %c.did, reason, "a repository could not be crawled");
        state::failed(&self.pool, c, reason, self.crawl.max_attempts).await?;
        Ok(Outcome::Failed(reason.to_string()))
    }

    /// Live ingest's projection; a test may set its own.
    fn live_projection(&self) -> Option<crate::firehose::projection::Projection> {
        #[cfg(test)]
        if let Some(p) = *self.projection.lock().expect("projection") {
            return Some(p);
        }
        crate::firehose::projection::published().get()
    }

    fn batch_ops(&self) -> usize {
        usize::try_from(self.crawl.batch_ops).unwrap_or(1).max(1)
    }

    /// Wait while live ingest is far behind: no transaction starts while its
    /// backlog is above `yield_above_lag_secs` seconds of input, and none
    /// until it falls below a third of that once waiting. The lease is
    /// renewed while waiting. Returns `false` when the claim stopped
    /// holding.
    pub(super) async fn yield_to_live(&self, c: &Claim) -> Result<bool, sqlx::Error> {
        let high = f64::from(self.crawl.yield_above_lag_secs);
        let renew_every = YIELD_RENEW.min(std::time::Duration::from_secs(u64::from(self.crawl.claim_lease_secs / 3).max(1)));
        let mut renewed = std::time::Instant::now();
        let started = std::time::Instant::now();
        loop {
            let waiting = self.progress().waiting_for_live;
            let limit = if waiting { high / 3.0 } else { high };
            let behind = self.live_projection().and_then(|p| p.backlog_secs()).is_some_and(|b| b > limit);
            if !behind {
                if waiting {
                    self.note(|p| p.waiting_for_live = false);
                    tracing::info!(event = "full_crawl_resumed", "live ingest caught up; the crawl goes on");
                }
                metrics::gauge!(METRIC_YIELD_SECONDS).increment(started.elapsed().as_secs_f64());
                return Ok(true);
            }
            if !waiting {
                self.note(|p| p.waiting_for_live = true);
                tracing::info!(event = "full_crawl_yields", "live ingest is behind; the crawl waits for it");
            }
            if renewed.elapsed() >= renew_every {
                if !state::renew(&self.pool, c, self.crawl.claim_lease_secs).await? {
                    return Ok(false);
                }
                renewed = std::time::Instant::now();
            }
            tokio::time::sleep(std::time::Duration::from_millis(250)).await;
        }
    }

    /// Apply `ops` at `rev` in batches, renewing the lease after each. Every
    /// account and key the records name is interned first. Returns `false`
    /// when the claim stopped holding.
    async fn write(&self, c: &Claim, rev: &str, ops: &[RawOp]) -> Result<bool, sqlx::Error> {
        if let Err(e) = self.writer.prime_ids(&c.did, ops, INTERN_CHUNK).await {
            tracing::warn!(event = "full_crawl_write_failed", did = %c.did, error = %e, "interning a crawl's ids failed; the account is retried");
            state::failed(&self.pool, c, "write", self.crawl.max_attempts).await?;
            return Ok(false);
        }
        for batch in ops.chunks(self.batch_ops()) {
            if !self.yield_to_live(c).await? {
                return Ok(false);
            }
            match self.writer.apply(&c.did, rev, batch, self.options()).await {
                Ok(report) => {
                    crate::firehose::projection::totals().crawl_records.fetch_add(batch.len() as u64, std::sync::atomic::Ordering::Relaxed);
                    let failed = report.outcomes.iter().filter(|o| matches!(o, OpOutcome::Failed(_))).count();
                    if failed > 0 {
                        tracing::debug!(event = "full_crawl_ops_failed", did = %c.did, failed, "crawled records failed deterministically and were skipped");
                    }
                }
                Err(e) => {
                    tracing::warn!(event = "full_crawl_write_failed", did = %c.did, error = %e, "a crawl batch failed; the account is retried");
                    state::failed(&self.pool, c, "write", self.crawl.max_attempts).await?;
                    return Ok(false);
                }
            }
            if !state::renew(&self.pool, c, self.crawl.claim_lease_secs).await? {
                return Ok(false);
            }
        }
        Ok(true)
    }

    /// Delete the stored rows of the claimed groups that the repository at
    /// `rev` no longer holds: rows older than `rev` and absent from it, each
    /// with a tombstone at `rev` (the writer's delete in full). In a full
    /// trial, only rows the trial recorded. Never a private row. Returns the
    /// rows deleted.
    async fn reconcile(
        &self,
        c: &Claim,
        rev: &str,
        wanted: &HashSet<&'static str>,
        present: &BTreeMap<&'static str, HashSet<String>>,
        trial_history: bool,
    ) -> Result<usize, sqlx::Error> {
        let empty = HashSet::new();
        let mut deleted = 0;
        let mut gone: Vec<(String, String, String)> = Vec::new();
        for &nsid in wanted {
            let there = present.get(nsid).unwrap_or(&empty);
            let stored: Vec<String> = if SELF_RECORDS.contains(&nsid) {
                self.stored_self(&c.did, nsid, rev).await?.into_iter().collect()
            } else if PLURAL_COLLECTIONS.contains(&nsid) {
                Vec::new()
            } else {
                crate::firehose::backfill::local_uris(&self.writer, &c.did, nsid, rev, false).await?
            };
            for uri in stored.into_iter().filter(|u| !there.contains(u)) {
                if trial_history && !recorded(&self.pool, nsid, &uri).await? {
                    continue;
                }
                let rkey = uri.rsplit('/').next().unwrap_or_default().to_string();
                gone.push((nsid.to_string(), rkey, uri));
            }
        }
        for batch in gone.chunks(self.batch_ops()) {
            if !self.yield_to_live(c).await? {
                break;
            }
            let ops: Vec<RawOp> = batch.iter().map(|(nsid, rkey, _)| RawOp { action: Action::Delete, collection: nsid.clone(), rkey: rkey.clone(), cid: None, record: None }).collect();
            match self.writer.apply(&c.did, rev, &ops, self.options()).await {
                Ok(report) => deleted += report.outcomes.iter().filter(|o| **o == OpOutcome::Written).count(),
                Err(e) => tracing::warn!(event = "full_crawl_reconcile_failed", did = %c.did, error = %e, "a reconcile batch failed"),
            }
            if !state::renew(&self.pool, c, self.crawl.claim_lease_secs).await? {
                break;
            }
        }
        Ok(deleted)
    }

    /// The account's stored self record of `nsid` older than `rev`, as a URI.
    async fn stored_self(&self, did: &str, nsid: &str, rev: &str) -> Result<Option<String>, sqlx::Error> {
        let rev = crate::db::ids::rev_value(rev);
        let found: bool = match nsid {
            NSID_PROFILE => sqlx::query_scalar("SELECT EXISTS (SELECT 1 FROM actors a JOIN dids d ON d.id = a.id WHERE d.did = $1 AND a.profile_record IS NOT NULL AND a.profile_rev < $2)"),
            NSID_LABELER_SERVICE => {
                sqlx::query_scalar("SELECT EXISTS (SELECT 1 FROM labeler_services s JOIN dids d ON d.id = s.author WHERE d.did = $1 AND s.rev < $2)")
            }
            _ => sqlx::query_scalar(
                "SELECT EXISTS (SELECT 1 FROM actor_self_records s JOIN dids d ON d.id = s.author JOIN collections c ON c.id = s.coll \
                 WHERE d.did = $1 AND s.rev < $2 AND c.nsid = $3)",
            ),
        }
        .bind(did)
        .bind(rev)
        .bind(nsid)
        .fetch_one(&self.pool)
        .await?;
        Ok(found.then(|| format!("at://{did}/{nsid}/self")))
    }

    /// A repository too large to fetch whole: each claimed collection is
    /// listed at the rev read first, applied and reconciled against the
    /// listing. It is never verified, so with `verify_signatures` its marks
    /// are not set and it stays incomplete.
    async fn per_collection(&self, c: &Claim, pds: &str, trial_history: bool) -> Result<Outcome, sqlx::Error> {
        let rev = match self.source.latest_rev(pds, &c.did).await {
            Ok(r) => r,
            Err(e) => return self.fail(c, &e).await,
        };
        let wanted = claimed_collections(&c.groups);
        let mut present: BTreeMap<&'static str, HashSet<String>> = BTreeMap::new();
        let mut records = 0;
        for &nsid in &wanted {
            let mut cursor: Option<String> = None;
            loop {
                let page = match self.source.list_records(pds, &c.did, nsid, cursor.as_deref()).await {
                    Ok(p) => p,
                    Err(e) => return self.fail(c, &e).await,
                };
                let ops: Vec<RawOp> = page
                    .records
                    .iter()
                    .filter(|r| r.uri.starts_with(&format!("at://{}/{nsid}/", c.did)))
                    .map(|r| RawOp { action: Action::Create, collection: nsid.to_string(), rkey: r.rkey().to_string(), cid: Some(r.cid.clone()), record: Some(r.value.clone()) })
                    .collect();
                present.entry(nsid).or_default().extend(page.records.iter().map(|r| r.uri.clone()));
                records += ops.len();
                if !self.write(c, &rev, &ops).await? {
                    return Ok(Outcome::Refused);
                }
                match page.cursor {
                    Some(next) if !page.records.is_empty() => cursor = Some(next),
                    _ => break,
                }
            }
        }
        self.reconcile(c, &rev, &wanted, &present, trial_history).await?;
        if self.crawl.verify_signatures {
            state::failed(&self.pool, c, "unverified", 1).await?;
            return Ok(Outcome::Unverified { records });
        }
        Ok(match state::done(&self.pool, c, &Fetched { car_rev: rev, verified: true }).await? {
            DoneOutcome::Done { marks, whole } => Outcome::Done { records, deleted: 0, marks, whole, car_bytes: 0 },
            _ => Outcome::Refused,
        })
    }
}

/// Whether the row `uri` of `nsid` is in a full trial's record.
async fn recorded(pool: &sqlx::PgPool, nsid: &str, uri: &str) -> Result<bool, sqlx::Error> {
    use crate::indexing::trial::TrialTable as T;
    let (tbl, b, c) = match nsid {
        NSID_POST => (T::Posts, "k.rkey", "0"),
        NSID_LIKE => (T::Likes, "k.rkey", "0"),
        NSID_REPOST => (T::Reposts, "k.rkey", "0"),
        NSID_FOLLOW => (T::Follows, "k.rkey", "0"),
        NSID_BLOCK => (T::Blocks, "k.rkey", "0"),
        NSID_LIST => (T::Lists, "k.rkey", "0"),
        NSID_LISTITEM => (T::ListItems, "k.rkey", "0"),
        NSID_LISTBLOCK => (T::ListBlocks, "k.rkey", "0"),
        NSID_LIST_OPTOUT => (T::ListOptouts, "k.rkey", "0"),
        NSID_STARTER_PACK => (T::StarterPacks, "k.rkey", "0"),
        NSID_THREADGATE => (T::Threadgates, "k.rkey", "0"),
        NSID_POSTGATE => (T::Postgates, "k.rkey", "0"),
        NSID_POLL_POST => (T::PollPosts, "k.rkey", "0"),
        NSID_LABELER_SERVICE => (T::LabelerServices, "0", "0"),
        NSID_PROFILE => (T::Actors, "0", "0"),
        NSID_NOTIFICATION_DECLARATION | NSID_ACTOR_STATUS | NSID_CONTENT_VISIBILITY => (T::ActorSelfRecords, "0", "k.coll"),
        _ => return Ok(false),
    };
    sqlx::query_scalar(&format!(
        "SELECT EXISTS (SELECT 1 FROM ref_of($1) k JOIN full_trial_rows r ON r.tbl = $2 AND r.a = k.author AND r.b = {b} AND r.c = {c})"
    ))
    .bind(uri)
    .bind(tbl.code())
    .fetch_one(pool)
    .await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_records_age_is_its_date_or_its_key() {
        let at = made_at(&serde_json::json!({"createdAt": "2026-01-02T03:04:05Z"}), "self").expect("date");
        assert_eq!(at.to_rfc3339(), "2026-01-02T03:04:05+00:00");
        assert!(made_at(&serde_json::json!({}), "3kaaaaaaaaa22").is_some());
        assert!(made_at(&serde_json::json!({}), "self").is_none());
    }

    #[test]
    fn the_claimed_groups_name_their_collections() {
        let c = claimed_collections(&["likes".into(), "profiles".into()]);
        assert!(c.contains(NSID_LIKE) && c.contains(NSID_PROFILE) && c.contains(PLURAL_COLLECTIONS[0]));
        assert!(!c.contains(NSID_POST));
    }
}
