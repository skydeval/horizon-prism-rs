//! `admin audiences check [--dids D,...] [--accept-unchecked D,...]`
//! (private-audiences design §3, slice S0): before the upgrade that stops
//! audience ingest, prove that the stored audience rows match every
//! member's repository.
//!
//! Read-only. It resolves each member's PDS without persisting anything,
//! calls only `getRepoStatus` and `listRecords`, and writes nothing to the
//! database or Redis. The stored rows are read through `private_tier`.
//!
//! One outcome per member:
//! - `clean`: every record ingest would store has a row with its `cid`,
//!   and every row is listed;
//! - `refused`: clean apart from records the parser refuses (listed);
//! - `inactive`: tombstoned, no PDS, repo not found or not active. It
//!   fails when the member has stored rows, unless accepted;
//! - `mismatch`: a missing, extra or differing row. It always fails;
//! - `unchecked`: any other failure to read. It fails unless accepted.
//!
//! The consumer's lag, audience dead letters and open jobs are printed but
//! decide nothing.

use std::sync::Arc;
use std::time::{Duration, Instant};

use chrono::Utc;
use futures::StreamExt;
use redis::aio::ConnectionManager;
use sqlx::PgPool;
use url::Url;

use super::{table, when};
use crate::config::AppConfig;
use crate::firehose::backfill::{ListPage, ListedRecord, PAGE_LIMIT, PDS_TIMEOUT};
use crate::firehose::classifier::NSID_AUDIENCE;
use crate::identity::refresher::{RepoStatus, account_status, is_repo_not_found};
use crate::identity::resolver::extract_info;
use crate::identity::{IdentityError, IdentityResolver, IdentitySettings, PersistencePolicy};
use crate::net::{BodyCap, Intent, NetClient, NetConfig, RequestOptions, Transports};
use crate::private_tier::audiences::{Comparison, check_rows, compare};

/// Members checked at once.
const CONCURRENCY: usize = 8;
/// `listRecords` pages read for one member before giving up (`unchecked`).
const MAX_PAGES: usize = 1000;
/// Dead letters printed, at most; the count is always printed.
const LETTERS_SHOWN: usize = 500;
/// Dead letters read per `XRANGE`.
const LETTER_PAGE: usize = 500;

struct NothingPersists;

impl PersistencePolicy for NothingPersists {
    fn in_persistence_set(&self, _did: &str) -> bool {
        false
    }
}

/// What the check is asked.
#[derive(Debug, Clone, Default)]
pub(crate) struct Options {
    /// Only these members (a final re-check); every member when empty.
    pub dids: Vec<String>,
    /// Members whose `unchecked` or `inactive`-with-rows outcome passes.
    pub accept: Vec<String>,
}

/// A dead letter as printed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Letter {
    pub id: String,
    pub repo: String,
    pub path: String,
    pub error: String,
}

/// A consumer group's lag and pending count; `None` where Redis cannot say.
pub(crate) type LagPending = (Option<i64>, Option<i64>);

/// What is printed beside the comparison and decides nothing.
#[derive(Debug, Clone, Default)]
pub(crate) struct Surround {
    /// `(lag, pending)` of the consumer group, or why it could not be read.
    pub consumer: Option<Result<LagPending, String>>,
    /// Audience dead letters and whole-entry ones, or why they could not
    /// be read.
    pub letters: Option<Result<Vec<Letter>, String>>,
}

/// A repository's state, as the backfill's `prepare` classifies it.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Repo {
    Active(String),
    Inactive(String),
    Unchecked(String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum Outcome {
    Clean,
    Refused,
    Inactive(String),
    Mismatch,
    Unchecked(String),
}

impl Outcome {
    fn name(&self) -> &'static str {
        match self {
            Self::Clean => "clean",
            Self::Refused => "refused",
            Self::Inactive(_) => "inactive",
            Self::Mismatch => "mismatch",
            Self::Unchecked(_) => "unchecked",
        }
    }
}

#[derive(Debug, Clone)]
struct Member {
    did: String,
    outcome: Outcome,
    /// Rows stored for the member.
    rows: usize,
    comparison: Option<Comparison>,
    /// Failing but named with `--accept-unchecked`.
    accepted: bool,
}

impl Member {
    /// Whether this outcome fails the check before any acceptance.
    fn fails_unaccepted(&self) -> bool {
        match self.outcome {
            Outcome::Clean | Outcome::Refused => false,
            Outcome::Mismatch | Outcome::Unchecked(_) => true,
            Outcome::Inactive(_) => self.rows > 0,
        }
    }

    fn fails(&self) -> bool {
        self.fails_unaccepted() && !self.accepted
    }
}

/// The network side of the check: identity and PDS reads that persist
/// nothing.
pub(crate) struct Checker {
    resolver: IdentityResolver,
    net: NetClient,
    timeout: Duration,
}

impl Checker {
    /// A checker over `nets`; each PDS call times out after `timeout`.
    #[must_use]
    pub(crate) fn new(nets: Transports, pool: PgPool, settings: IdentitySettings, timeout: Duration) -> Self {
        let net = nets.internal().client().clone();
        let resolver = IdentityResolver::new(nets, pool, settings, Arc::new(NothingPersists), Intent::Internal);
        Self { resolver, net, timeout }
    }

    async fn get(&self, pds: &str, method: &str, params: &[(&str, &str)], cap: BodyCap) -> Result<crate::net::NetResponse, String> {
        let url = Url::parse_with_params(&format!("{}/xrpc/{method}", pds.trim_end_matches('/')), params)
            .map_err(|e| format!("{method} url: {e}"))?;
        self.net
            .get(url.as_str(), &RequestOptions::new(cap).timeout(self.timeout))
            .await
            .map_err(|e| format!("{method}: {e}"))
    }

    /// The repository's state, classified as `prepare` classifies it
    /// (`firehose::backfill`), with nothing stored.
    async fn repo(&self, did: &str) -> Repo {
        let fetched = match self.resolver.fetch_document(did).await {
            Ok(f) => f,
            Err(IdentityError::Tombstoned(_)) => return Repo::Inactive("tombstoned".to_string()),
            Err(e) => return Repo::Unchecked(format!("resolve: {e}")),
        };
        let Some(pds) = extract_info(&fetched.doc, did).pds_endpoint else {
            return Repo::Inactive("the DID document names no PDS".to_string());
        };
        let response = match self.get(&pds, "com.atproto.sync.getRepoStatus", &[("did", did)], BodyCap::REPO_STATUS).await {
            Ok(r) => r,
            Err(e) => return Repo::Unchecked(e),
        };
        if !response.is_success() {
            let status = response.status.as_u16();
            if is_repo_not_found(status, &response.body) {
                return Repo::Inactive("repo not found at its PDS".to_string());
            }
            return Repo::Unchecked(format!("getRepoStatus: HTTP {status}"));
        }
        let status: RepoStatus = match response.json() {
            Ok(s) => s,
            Err(e) => return Repo::Unchecked(format!("getRepoStatus: {e}")),
        };
        if !status.active {
            return Repo::Inactive(format!("repo inactive ({})", account_status(&status)));
        }
        Repo::Active(pds)
    }

    /// Every listed audience record of `did`.
    async fn list(&self, pds: &str, did: &str) -> Result<Vec<ListedRecord>, String> {
        let limit = PAGE_LIMIT.to_string();
        let mut out = Vec::new();
        let mut cursor: Option<String> = None;
        for _ in 0..MAX_PAGES {
            let mut params = vec![("repo", did), ("collection", NSID_AUDIENCE), ("limit", limit.as_str())];
            if let Some(c) = cursor.as_deref() {
                params.push(("cursor", c));
            }
            let r = self.get(pds, "com.atproto.repo.listRecords", &params, BodyCap::LIST_RECORDS).await?;
            if !r.is_success() {
                return Err(format!("listRecords: HTTP {}", r.status.as_u16()));
            }
            let page: ListPage = r.json().map_err(|e| format!("listRecords: {e}"))?;
            let empty = page.records.is_empty();
            out.extend(page.records);
            match page.cursor {
                // A cursor that does not move would truncate the listing.
                Some(next) if !empty && cursor.as_deref() == Some(next.as_str()) => {
                    return Err("listRecords: the cursor repeated".to_string());
                }
                Some(next) if !empty => cursor = Some(next),
                _ => return Ok(out),
            }
        }
        Err(format!("listRecords: more than {MAX_PAGES} pages"))
    }

    /// One member's outcome. The listing is read before the rows, so an op
    /// the consumer applies meanwhile is more likely to be seen.
    async fn member(&self, pool: &PgPool, did: &str, accept: &[String]) -> Result<Member, String> {
        let listed = match self.repo(did).await {
            Repo::Inactive(why) => Err(Outcome::Inactive(why)),
            Repo::Unchecked(why) => Err(Outcome::Unchecked(why)),
            Repo::Active(pds) => self.list(&pds, did).await.map_err(Outcome::Unchecked),
        };
        let rows = check_rows(pool, did).await.map_err(|e| format!("reading the rows of {did}: {e}"))?;
        let (outcome, comparison) = match listed {
            Err(outcome) => (outcome, None),
            Ok(records) => {
                let c = compare(did, &records, &rows);
                let outcome = if !c.matches() {
                    Outcome::Mismatch
                } else if c.refused.is_empty() {
                    Outcome::Clean
                } else {
                    Outcome::Refused
                };
                (outcome, Some(c))
            }
        };
        let mut member = Member { did: did.to_string(), outcome, rows: rows.len(), comparison, accepted: false };
        member.accepted = member.fails_unaccepted()
            && !matches!(member.outcome, Outcome::Mismatch)
            && accept.iter().any(|a| a.trim() == did);
        Ok(member)
    }
}

fn detail(m: &Member) -> String {
    match (&m.outcome, &m.comparison) {
        (Outcome::Inactive(why) | Outcome::Unchecked(why), _) => why.chars().take(160).collect(),
        (_, Some(c)) => {
            let mut parts = Vec::new();
            for (n, what) in [(c.missing.len(), "missing"), (c.differing.len(), "differing"), (c.extra.len(), "extra"), (c.refused.len(), "refused")] {
                if n > 0 {
                    parts.push(format!("{n} {what}"));
                }
            }
            if parts.is_empty() { "-".to_string() } else { parts.join(", ") }
        }
        _ => "-".to_string(),
    }
}

/// Run the comparison and render the report. Returns the report and
/// whether the check passes.
///
/// # Errors
///
/// A printable message: a database failure.
pub(crate) async fn check(pool: &PgPool, checker: &Checker, options: &Options, surround: Surround) -> Result<(String, bool), String> {
    let started_at = Utc::now();
    let started = Instant::now();
    let all: Vec<String> = sqlx::query_scalar("SELECT did FROM members ORDER BY did")
        .fetch_all(pool)
        .await
        .map_err(|e| format!("reading members: {e}"))?;
    let (dids, not_members): (Vec<String>, Vec<String>) = if options.dids.is_empty() {
        (all, Vec::new())
    } else {
        let mut wanted: Vec<String> = options.dids.iter().map(|d| d.trim().to_string()).filter(|d| !d.is_empty()).collect();
        wanted.sort();
        wanted.dedup();
        wanted.into_iter().partition(|d| all.contains(d))
    };
    let mut members: Vec<Member> = futures::stream::iter(dids.iter())
        .map(|did| checker.member(pool, did, &options.accept))
        .buffer_unordered(CONCURRENCY)
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .collect::<Result<_, _>>()?;
    members.sort_by(|a, b| a.did.cmp(&b.did));
    let jobs: Vec<(String, String, String, i32, chrono::DateTime<Utc>)> = sqlx::query_as(
        "SELECT did, depth, state, attempts, updated_at FROM backfill_jobs \
         WHERE depth IN ('member', 'private', 'authz') AND state IN ('queued', 'running') ORDER BY did, depth",
    )
    .fetch_all(pool)
    .await
    .map_err(|e| format!("reading jobs: {e}"))?;
    let took = started.elapsed();

    let mut out = format!("admin audiences check: started {}\n", when(Some(started_at)));
    out.push_str(&format!("members checked: {}\n\n", members.len()));
    let rows: Vec<Vec<String>> = members
        .iter()
        .map(|m| {
            vec![
                m.did.clone(),
                m.outcome.name().to_string(),
                m.comparison.as_ref().map_or_else(|| "-".to_string(), |c| c.records.to_string()),
                m.rows.to_string(),
                if m.accepted { "accepted" } else if m.fails() { "FAIL" } else { "ok" }.to_string(),
                detail(m),
            ]
        })
        .collect();
    out.push_str(&table(&["did", "outcome", "records", "rows", "verdict", "detail"], &rows));
    out.push('\n');
    for did in &not_members {
        out.push_str(&format!("\nnot a member, not checked: {did}"));
    }
    let mismatched: Vec<Vec<String>> = members
        .iter()
        .filter_map(|m| m.comparison.as_ref().map(|c| (m, c)))
        .flat_map(|(m, c)| {
            [(&c.missing, "missing row"), (&c.differing, "differing cid"), (&c.extra, "row not listed")]
                .into_iter()
                .flat_map(move |(uris, what)| uris.iter().map(move |u| vec![m.did.clone(), what.to_string(), u.clone()]))
        })
        .collect();
    if !mismatched.is_empty() {
        out.push_str("\nmismatches (clear with `admin backfill <did> --depth authz`, wait for the job, run the check again):\n");
        out.push_str(&table(&["did", "problem", "uri"], &mismatched));
        out.push('\n');
    }
    let accepted: Vec<&Member> = members.iter().filter(|m| m.accepted).collect();
    let refused_rows: Vec<&String> = members.iter().filter_map(|m| m.comparison.as_ref()).flat_map(|c| c.refused_with_row.iter()).collect();
    if !accepted.is_empty() || !refused_rows.is_empty() {
        out.push_str("\nunproven, kept as stored:\n");
        for m in &accepted {
            out.push_str(&format!("accepted with --accept-unchecked: {} ({}, {} rows)\n", m.did, m.outcome.name(), m.rows));
        }
        for uri in &refused_rows {
            out.push_str(&format!("refused record with a stored row: {uri}\n"));
        }
    }

    out.push_str("\nprinted only, not pass conditions:\n");
    match &surround.consumer {
        None => out.push_str("consumer: not read\n"),
        Some(Err(e)) => out.push_str(&format!("consumer: could not be read: {e}\n")),
        Some(Ok((lag, pending))) => {
            let show = |v: &Option<i64>| v.map_or_else(|| "nil".to_string(), |v| v.to_string());
            out.push_str(&format!("consumer: lag {}, pending {}\n", show(lag), show(pending)));
        }
    }
    match &surround.letters {
        None => out.push_str("dead letters: not read\n"),
        Some(Err(e)) => out.push_str(&format!("dead letters: could not be read: {e}\n")),
        Some(Ok(letters)) => {
            out.push_str(&format!("audience and whole-entry dead letters: {}\n", letters.len()));
            if !letters.is_empty() {
                let rows: Vec<Vec<String>> = letters
                    .iter()
                    .take(LETTERS_SHOWN)
                    .map(|l| vec![l.id.clone(), l.repo.clone(), if l.path.is_empty() { "(whole entry)".to_string() } else { l.path.clone() }, l.error.chars().take(120).collect()])
                    .collect();
                out.push_str(&table(&["id", "repo", "path", "error"], &rows));
                out.push('\n');
                if letters.len() > LETTERS_SHOWN {
                    out.push_str(&format!("… and {} more\n", letters.len() - LETTERS_SHOWN));
                }
            }
        }
    }
    out.push_str(&format!("open member, private and authz jobs: {}\n", jobs.len()));
    if !jobs.is_empty() {
        let rows: Vec<Vec<String>> = jobs
            .into_iter()
            .map(|(did, depth, state, attempts, at)| vec![did, depth, state, attempts.to_string(), when(Some(at))])
            .collect();
        out.push_str(&table(&["did", "depth", "state", "attempts", "updated_at"], &rows));
        out.push('\n');
    }

    let failing = members.iter().filter(|m| m.fails()).count();
    out.push_str(&format!("\nduration: {:.1} s\n", took.as_secs_f64()));
    // A `--dids` entry that is no member (a typo, say) proves nothing, so it
    // fails the run.
    if failing == 0 && not_members.is_empty() {
        out.push_str("result: PASS");
    } else if failing == 0 {
        out.push_str(&format!("result: FAIL ({} named DIDs are not members)", not_members.len()));
    } else {
        out.push_str(&format!("result: FAIL ({failing} member{} failing)", if failing == 1 { "" } else { "s" }));
    }
    Ok((out, failing == 0 && not_members.is_empty()))
}

/// The audience dead letters (by path) and the whole-entry ones (no path),
/// oldest first.
async fn letters(redis: &mut ConnectionManager, stream: &str) -> Result<Vec<Letter>, String> {
    let prefix = format!("{NSID_AUDIENCE}/");
    let mut out = Vec::new();
    let mut start = "-".to_string();
    loop {
        let reply: redis::Value = redis::cmd("XRANGE")
            .arg(stream)
            .arg(&start)
            .arg("+")
            .arg("COUNT")
            .arg(LETTER_PAGE)
            .query_async(redis)
            .await
            .map_err(|e| e.to_string())?;
        let redis::Value::Array(items) = reply else {
            return Err("unexpected XRANGE reply".to_string());
        };
        let n = items.len();
        let mut last = None;
        for item in items {
            let e = crate::firehose::stream::parse_entry(item)?;
            let path = e.field("path").unwrap_or_default().to_string();
            if path.is_empty() || path.starts_with(&prefix) {
                out.push(Letter {
                    id: e.id.clone(),
                    repo: e.field("repo").unwrap_or_default().to_string(),
                    path,
                    error: e.field("error").unwrap_or_default().to_string(),
                });
            }
            last = Some(e.id);
        }
        match last {
            Some(id) if n >= LETTER_PAGE => start = format!("({id}"),
            _ => return Ok(out),
        }
    }
}

async fn surround(cfg: &AppConfig) -> Surround {
    let mut redis = match super::redis(cfg).await {
        Ok(r) => r,
        Err(e) => {
            return Surround { consumer: Some(Err(e.clone())), letters: Some(Err(e)) };
        }
    };
    let firehose = &cfg.file.firehose;
    let consumer = super::stream_info::read(&mut redis, &firehose.stream_key, &firehose.group)
        .await
        .map(|info| info.group.map_or((None, None), |g| (g.lag, g.pending)));
    let letters = letters(&mut redis, &firehose.dead_letter_stream).await;
    Surround { consumer: Some(consumer), letters: Some(letters) }
}

/// `admin audiences check` with the deployed configuration. Prints the
/// report and returns the exit code: 0 when it passes, 1 when it fails,
/// 78 on a configuration error.
pub async fn run(dids: &[String], accept: &[String]) -> u8 {
    let result = async {
        let cfg = super::config()?;
        let pool = super::pool(&cfg).await?;
        let nets = Transports::new(&NetConfig::from_app(&cfg), &cfg.file.net).map_err(|e| format!("net client: {e}"))?;
        let checker = Checker::new(nets, pool.clone(), IdentitySettings::from_app(&cfg), PDS_TIMEOUT);
        let options = Options { dids: dids.to_vec(), accept: accept.to_vec() };
        let surround = surround(&cfg).await;
        check(&pool, &checker, &options, surround).await
    }
    .await;
    match result {
        Ok((report, pass)) => {
            println!("{report}");
            u8::from(!pass)
        }
        Err(e) => {
            eprintln!("{e}");
            if e.starts_with("config_error") { 78 } else { 1 }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Audience and whole-entry dead letters are read across pages; others
    /// are left out.
    #[tokio::test]
    async fn reads_the_audience_dead_letters() {
        let Ok(url) = std::env::var("HPRS_TEST_REDIS_URL") else {
            return;
        };
        let mut c = ConnectionManager::new(redis::Client::open(url).expect("url")).await.expect("redis");
        let stream = format!("test:dl:aud:{}", rand::random::<u32>());
        let audience = format!("{NSID_AUDIENCE}/3kaaaaaaaaaa2");
        let mut paths = vec!["app.bsky.feed.post/1".to_string(), String::new()];
        paths.extend(std::iter::repeat_n(audience.clone(), LETTER_PAGE));
        for path in &paths {
            let _: String = redis::cmd("XADD")
                .arg(&stream)
                .arg("*")
                .arg("repo")
                .arg("did:plc:a")
                .arg("path")
                .arg(path)
                .arg("error")
                .arg("bad")
                .query_async(&mut c)
                .await
                .expect("xadd");
        }
        let found = letters(&mut c, &stream).await.expect("letters");
        assert_eq!(found.len(), LETTER_PAGE + 1);
        assert_eq!(found[0].path, "");
        assert!(found[1..].iter().all(|l| l.path == audience && l.repo == "did:plc:a"));
        let _: () = redis::cmd("DEL").arg(&stream).query_async(&mut c).await.expect("del");
    }
}
