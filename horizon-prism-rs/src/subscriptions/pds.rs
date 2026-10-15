//! Where a `list_sync` run reads from in production: the list owner's DID
//! document and PDS (design-v3 §4.3 "The run", step 2).
//!
//! - The DID document is fetched fresh, once per run, and is neither stored
//!   nor put in any cache: a list owner is not an account this AppView
//!   indexes, and that some member subscribes to its list must leave no
//!   trace a public request could find.
//! - Every outcome of the status check falls in exactly one class, and
//!   anything not listed as "not active" or "tombstoned" is transient: a
//!   timeout or a server error never starts a suspicion.
//! - PDS calls wait for the host's backfill allowance; that wait is not
//!   counted as time the owner's server took.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use sqlx::PgPool;
use url::Url;

use super::fetch::{OwnerRecords, Page};
use super::jobs;
use super::run::{OwnerSource, OwnerStatus};
use crate::firehose::backfill::{ListPage, PAGE_LIMIT, PDS_TIMEOUT};
use crate::identity::handle_lookup::{Step, classify_failure};
use crate::identity::refresher::RepoStatus;
use crate::identity::resolver::extract_info;
use crate::identity::{IdentityError, IdentityResolver};
use crate::net::{BodyCap, NetClient, NetError, NetResponse, RequestOptions};
use crate::sync::{Class, HostBudgets};

/// The suspicion key of a `did:web` whose document is not there.
const KEY_DID_WEB: &str = "did:web";
/// The suspicion key of a document that names no PDS.
const KEY_NO_PDS: &str = "none";

/// What the DID document step found.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Document {
    /// The PDS the document names.
    Pds(String),
    Status(OwnerStatus),
}

/// Sort the DID-document fetch. `Err` is transient.
fn classify_document(did: &str, fetched: Result<Option<String>, &IdentityError>) -> Result<Document, String> {
    let web = did.starts_with("did:web:");
    let not_active = |key: &str| Ok(Document::Status(OwnerStatus::NotActive { key: key.to_string() }));
    match fetched {
        Ok(Some(pds)) => Ok(Document::Pds(pds)),
        Ok(None) => not_active(KEY_NO_PDS),
        // A did:plc answered with 410 by PLC is gone at once.
        Err(IdentityError::Tombstoned(_)) => Ok(Document::Status(OwnerStatus::Tombstoned)),
        // A did:web whose document is not there, or is not its own (a
        // parked or re-registered domain).
        Err(IdentityError::NotFound(_) | IdentityError::Gone(_) | IdentityError::MalformedDocument { .. }) if web => not_active(KEY_DID_WEB),
        Err(IdentityError::Net(error)) if web && classify_failure(error) == Step::Confirmed => not_active(KEY_DID_WEB),
        Err(error) => Err(format!("resolve: {error}")),
    }
}

/// Sort the `getRepoStatus` call at `pds`. `Ok(None)` is an active
/// repository; `Err` is transient.
fn classify_repo_status(pds: &str, answer: Result<(u16, &[u8]), &NetError>) -> Result<Option<OwnerStatus>, String> {
    let not_active = || Ok(Some(OwnerStatus::NotActive { key: pds.to_string() }));
    match answer {
        // The host is not there to answer: the usual way a self-hosted PDS
        // disappears while the directory still names it.
        Err(error) if classify_failure(error) == Step::Confirmed => not_active(),
        Err(error) => Err(format!("getRepoStatus: {error}")),
        Ok((200..=299, body)) => match serde_json::from_slice::<RepoStatus>(body) {
            Ok(status) if status.active => Ok(None),
            // Deleted, taken down or deactivated; or an answer that is not
            // a repository status at all.
            Ok(_) | Err(_) => not_active(),
        },
        Ok((408 | 429, _)) => Err("getRepoStatus: busy".to_string()),
        // `RepoNotFound`, 404, 410, and any other refusal.
        Ok((400..=499, _)) => not_active(),
        Ok((status, _)) => Err(format!("getRepoStatus: HTTP {status}")),
    }
}

/// The production source. Cheap to clone. `list_sync` is internal work: it
/// resolves owners and reads their PDSes through the internal intent,
/// whatever `resolver` was built with (design-v4 §11 G-9).
#[derive(Clone)]
pub struct PdsOwners {
    pool: PgPool,
    resolver: IdentityResolver,
    net: NetClient,
    budgets: HostBudgets,
    /// The PDS each running owner's status check found: the listing reads
    /// the same one.
    found: Arc<Mutex<HashMap<String, String>>>,
}

impl PdsOwners {
    #[must_use]
    pub fn new(pool: PgPool, resolver: IdentityResolver, budgets: HostBudgets) -> Self {
        let resolver = resolver.with_intent(crate::net::Intent::Internal);
        let net = resolver.net().clone();
        Self {
            pool,
            resolver,
            net,
            budgets,
            found: Arc::default(),
        }
    }
}

/// One PDS call under the host's backfill allowance. Returns the answer
/// and the time the call itself took.
async fn pds_get(
    net: &NetClient,
    budgets: &HostBudgets,
    pds: &str,
    method: &str,
    params: &[(&str, &str)],
    cap: BodyCap,
) -> Result<(Result<NetResponse, NetError>, Duration), String> {
    let url = Url::parse_with_params(&format!("{}/xrpc/{method}", pds.trim_end_matches('/')), params).map_err(|e| format!("{method} url: {e}"))?;
    let host = url.host_str().unwrap_or_default().to_ascii_lowercase();
    budgets.acquire(&host, Class::Backfill).await;
    let started = Instant::now();
    let answer = net.get(url.as_str(), &RequestOptions::new(cap).timeout(PDS_TIMEOUT)).await;
    Ok((answer, started.elapsed()))
}

#[derive(serde::Deserialize)]
struct LatestCommit {
    rev: String,
}

#[async_trait]
impl OwnerSource for PdsOwners {
    async fn status(&self, owner: &str) -> Result<OwnerStatus, String> {
        let fetched = self.resolver.fetch_document(owner).await;
        let named = fetched.as_ref().map(|doc| extract_info(&doc.doc, owner).pds_endpoint);
        let pds = match classify_document(owner, named)? {
            Document::Pds(pds) => pds,
            Document::Status(status) => return Ok(status),
        };
        let (answer, _) = pds_get(&self.net, &self.budgets, &pds, "com.atproto.sync.getRepoStatus", &[("did", owner)], BodyCap::REPO_STATUS).await?;
        let sorted = classify_repo_status(&pds, answer.as_ref().map(|r| (r.status.as_u16(), r.body.as_ref())));
        if let Some(status) = sorted? {
            return Ok(status);
        }
        let (answer, _) =
            pds_get(&self.net, &self.budgets, &pds, "com.atproto.sync.getLatestCommit", &[("did", owner)], BodyCap::REPO_STATUS).await?;
        let response = answer.map_err(|e| format!("getLatestCommit: {e}"))?;
        if !response.is_success() {
            return Err(format!("getLatestCommit: HTTP {}", response.status.as_u16()));
        }
        let commit: LatestCommit = response.json().map_err(|e| format!("getLatestCommit: {e}"))?;
        if !proto_blue::syntax::Tid::is_valid(&commit.rev) {
            return Err(format!("getLatestCommit: invalid rev {:?}", commit.rev));
        }
        self.found.lock().unwrap_or_else(std::sync::PoisonError::into_inner).insert(owner.to_string(), pds);
        Ok(OwnerStatus::Active { rev: commit.rev })
    }

    async fn records(&self, owner: &str, generation: i64) -> Result<Box<dyn OwnerRecords>, String> {
        let pds = self
            .found
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove(owner)
            .ok_or_else(|| "no PDS found for this run".to_string())?;
        Ok(Box::new(PdsRecords {
            pool: self.pool.clone(),
            net: self.net.clone(),
            budgets: self.budgets.clone(),
            pds,
            owner: owner.to_string(),
            generation,
        }))
    }
}

/// One owner's records at its PDS, for one run.
struct PdsRecords {
    pool: PgPool,
    net: NetClient,
    budgets: HostBudgets,
    pds: String,
    owner: String,
    generation: i64,
}

#[async_trait]
impl OwnerRecords for PdsRecords {
    async fn page(&self, collection: &str, cursor: Option<&str>) -> Result<Page, String> {
        let limit = PAGE_LIMIT.to_string();
        let mut params = vec![("repo", self.owner.as_str()), ("collection", collection), ("limit", limit.as_str())];
        if let Some(c) = cursor {
            params.push(("cursor", c));
        }
        let (answer, call_time) = pds_get(&self.net, &self.budgets, &self.pds, "com.atproto.repo.listRecords", &params, BodyCap::LIST_RECORDS).await?;
        let response = answer.map_err(|e| format!("listRecords {collection}: {e}"))?;
        if !response.is_success() {
            return Err(format!("listRecords {collection}: HTTP {}", response.status.as_u16()));
        }
        let page: ListPage = response.json().map_err(|e| format!("listRecords {collection}: {e}"))?;
        Ok(Page {
            records: page.records,
            cursor: page.cursor,
            call_time,
        })
    }

    /// A purge or a reset bumps the job's generation: the run stops within
    /// one page.
    async fn superseded(&self) -> bool {
        let row: Result<Option<(i64, String)>, sqlx::Error> = sqlx::query_as("SELECT generation, state FROM backfill_jobs WHERE did = $1 AND depth = $2")
            .bind(&self.owner)
            .bind(jobs::DEPTH)
            .fetch_optional(&self.pool)
            .await;
        // A read that failed says nothing: the batches check again.
        row.is_ok_and(|row| !row.is_some_and(|(generation, state)| generation == self.generation && state == "running"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const PLC: &str = "did:plc:aaaaaaaaaaaaaaaaaaaaaaaa";
    const WEB: &str = "did:web:lists.example.com";
    const PDS: &str = "https://pds.example.com";

    fn not_active(key: &str) -> OwnerStatus {
        OwnerStatus::NotActive { key: key.to_string() }
    }

    #[test]
    fn the_document_step_has_one_class_per_outcome() {
        let doc = |did: &str, r: Result<Option<String>, IdentityError>| classify_document(did, r.as_ref().map(Clone::clone));
        assert_eq!(doc(PLC, Ok(Some(PDS.into()))), Ok(Document::Pds(PDS.into())));
        assert_eq!(doc(PLC, Ok(None)), Ok(Document::Status(not_active("none"))));
        assert_eq!(doc(PLC, Err(IdentityError::Tombstoned(PLC.into()))), Ok(Document::Status(OwnerStatus::Tombstoned)));
        // A did:web whose document is gone or is not its own.
        for error in [
            IdentityError::NotFound(WEB.into()),
            IdentityError::Gone(WEB.into()),
            IdentityError::MalformedDocument { did: WEB.into(), reason: "another id".into() },
            IdentityError::Net(NetError::BlockedAddress { host: "10.0.0.1".into() }),
        ] {
            assert_eq!(doc(WEB, Err(error)), Ok(Document::Status(not_active("did:web"))));
        }
        // Everything else is transient: a did:plc the directory does not
        // know, a server error, a timeout.
        for (did, error) in [
            (PLC, IdentityError::NotFound(PLC.into())),
            (PLC, IdentityError::MalformedDocument { did: PLC.into(), reason: "x".into() }),
            (WEB, IdentityError::Upstream { did: WEB.into(), status: 503 }),
            (WEB, IdentityError::Net(NetError::Timeout)),
        ] {
            assert!(doc(did, Err(error)).is_err(), "{did}");
        }
    }

    #[test]
    fn the_repo_status_step_has_one_class_per_outcome() {
        let status = |code: u16, body: &str| classify_repo_status(PDS, Ok((code, body.as_bytes())));
        assert_eq!(status(200, r#"{"did":"x","active":true}"#), Ok(None));
        // Not active: deactivated, not found, refused, or not an answer.
        for (code, body) in [
            (200, r#"{"did":"x","active":false,"status":"deactivated"}"#),
            (200, "<html>parked</html>"),
            (400, r#"{"error":"RepoNotFound"}"#),
            (404, ""),
            (410, ""),
            (403, ""),
        ] {
            assert_eq!(status(code, body), Ok(Some(not_active(PDS))), "{code} {body}");
        }
        // Transient: busy, or failing.
        for code in [408, 429, 500, 502, 503] {
            assert!(status(code, "").is_err(), "{code}");
        }
        // The host itself: confirmed absent is "not active"; a timeout is
        // transient.
        let blocked = NetError::BlockedAddress { host: "10.0.0.1".into() };
        assert_eq!(classify_repo_status(PDS, Err(&blocked)), Ok(Some(not_active(PDS))));
        assert!(classify_repo_status(PDS, Err(&NetError::Timeout)).is_err());
    }
}
