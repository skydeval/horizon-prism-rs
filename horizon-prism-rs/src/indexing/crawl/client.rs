//! What the crawl reads from outside: relays' `listRepos` and
//! `getRepoStatus`, a PDS's `getRepo` (or `listRecords` for a repository
//! too large to fetch whole), and the account's directory entry, each on
//! the crawl's own budget.

use std::collections::HashMap;
use std::sync::Mutex;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use serde::Deserialize;
use serde_json::Value;
use url::Url;

use crate::firehose::backfill::ListPage;
use crate::identity::{IdentityError, IdentityResolver};
use crate::net::{BodyCap, NetClient, NetError, RequestOptions};

/// One `listRepos` entry.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct ListedRepo {
    pub did: String,
    #[serde(default)]
    pub head: Option<String>,
    #[serde(default)]
    pub rev: Option<String>,
    #[serde(default)]
    pub active: Option<bool>,
    #[serde(default)]
    pub status: Option<String>,
}

/// One `listRepos` page.
#[derive(Debug, Clone, Default, PartialEq, Eq, Deserialize)]
pub struct ReposPage {
    #[serde(default)]
    pub repos: Vec<ListedRepo>,
    #[serde(default)]
    pub cursor: Option<String>,
}

/// An account's directory entry, as the crawl uses it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Identity {
    pub pds: String,
    /// The `#atproto` key as a `did:key`.
    pub key: Option<String>,
}

/// Why no directory entry.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum IdentityMiss {
    /// The DID is tombstoned or unknown, or names no PDS: gone.
    Gone(String),
    /// No answer now.
    Failed(String),
}

/// A `getRepo` answer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RepoFetch {
    Car(Vec<u8>),
    /// Larger than `max_car_bytes`.
    TooLarge,
    /// The PDS has no such repository.
    NotFound,
    /// The PDS holds it but does not serve it: `takendown`, `suspended`,
    /// `deactivated`.
    Inactive(String),
}

/// A `getRepoStatus` answer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RepoStatus {
    pub active: bool,
    pub status: Option<String>,
}

/// Everything the crawl fetches.
#[async_trait]
pub trait CrawlSource: Send + Sync {
    /// One page of a relay's `listRepos`; `base` is the relay's HTTPS base.
    async fn list_repos(&self, base: &str, cursor: Option<&str>) -> Result<ReposPage, String>;
    /// A relay's `getRepoStatus` for `did`; `Ok(None)` when it does not
    /// know the account.
    async fn relay_repo_status(&self, base: &str, did: &str) -> Result<Option<RepoStatus>, String>;
    /// The account's PDS and key; `fresh` re-reads past every cache.
    async fn identity(&self, did: &str, fresh: bool) -> Result<Identity, IdentityMiss>;
    async fn get_repo(&self, pds: &str, did: &str, max_bytes: u64) -> Result<RepoFetch, String>;
    async fn latest_rev(&self, pds: &str, did: &str) -> Result<String, String>;
    async fn list_records(&self, pds: &str, did: &str, collection: &str, cursor: Option<&str>) -> Result<ListPage, String>;
}

/// A relay URL of `firehose.relay_labels` as the HTTPS base its XRPC
/// methods are served at.
#[must_use]
pub fn relay_base(url: &str) -> Option<String> {
    let u = Url::parse(url).ok()?;
    let scheme = match u.scheme() {
        "wss" | "https" => "https",
        "ws" | "http" => "http",
        _ => return None,
    };
    let host = u.host_str()?;
    Some(match u.port() {
        Some(p) => format!("{scheme}://{host}:{p}"),
        None => format!("{scheme}://{host}"),
    })
}

/// A pacer: one slot every `1 / rate` seconds, reserved in order.
#[derive(Debug)]
pub struct Pace {
    gap: Duration,
    next: Mutex<Instant>,
}

impl Pace {
    #[must_use]
    pub fn per_sec(rate: u32) -> Self {
        Self { gap: Duration::from_secs_f64(1.0 / f64::from(rate.max(1))), next: Mutex::new(Instant::now()) }
    }

    /// Wait for the next slot.
    pub async fn wait(&self) {
        let at = {
            let mut next = self.next.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
            let now = Instant::now();
            let at = (*next).max(now);
            *next = at + self.gap;
            at
        };
        tokio::time::sleep_until(at.into()).await;
    }

    /// Hold every slot for `d` (a `Retry-After`).
    pub fn hold(&self, d: Duration) {
        let mut next = self.next.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        *next = (*next).max(Instant::now() + d);
    }
}

/// A pacer per host, made on first use.
#[derive(Debug)]
pub struct HostPace {
    rate: u32,
    hosts: Mutex<HashMap<String, std::sync::Arc<Pace>>>,
}

impl HostPace {
    #[must_use]
    pub fn per_sec(rate: u32) -> Self {
        Self { rate, hosts: Mutex::default() }
    }

    pub async fn wait(&self, url: &str) {
        let host = Url::parse(url).ok().and_then(|u| u.host_str().map(str::to_ascii_lowercase)).unwrap_or_default();
        let pace = {
            let mut hosts = self.hosts.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
            // Hosts not used for a while drop out, so the map stays small.
            if hosts.len() > 50_000 {
                hosts.clear();
            }
            std::sync::Arc::clone(hosts.entry(host).or_insert_with(|| std::sync::Arc::new(Pace::per_sec(self.rate))))
        };
        pace.wait().await;
    }
}

/// The production source.
pub struct NetCrawlSource {
    net: NetClient,
    resolver: IdentityResolver,
    /// The crawl's own directory budget.
    plc: Pace,
    /// `per_pds_rate_per_sec` per PDS host; relays use the pacing of the
    /// pass that reads them.
    pds: HostPace,
}

const TIMEOUT: Duration = Duration::from_secs(30);

impl NetCrawlSource {
    #[must_use]
    pub fn new(net: NetClient, resolver: IdentityResolver, plc_rate_per_sec: u32, per_pds_rate_per_sec: u32) -> Self {
        Self { net, resolver, plc: Pace::per_sec(plc_rate_per_sec), pds: HostPace::per_sec(per_pds_rate_per_sec) }
    }

    async fn get(&self, base: &str, method: &str, params: &[(&str, &str)], cap: BodyCap, timeout: Duration) -> Result<crate::net::NetResponse, NetError> {
        let url = Url::parse_with_params(&format!("{}/xrpc/{method}", base.trim_end_matches('/')), params).map_err(|e| NetError::InvalidUrl {
            url: base.to_string(),
            reason: e.to_string(),
        })?;
        self.net.get(url.as_str(), &RequestOptions::new(cap).timeout(timeout)).await
    }
}

fn xrpc_error(body: &[u8]) -> Option<String> {
    serde_json::from_slice::<Value>(body).ok().and_then(|v| v.get("error").and_then(Value::as_str).map(str::to_string))
}

/// The `#atproto` signing key of a DID document, as a `did:key`.
#[must_use]
pub fn signing_key(doc: &Value, did: &str) -> Option<String> {
    doc.get("verificationMethod")
        .and_then(Value::as_array)?
        .iter()
        .find(|v| v.get("id").and_then(Value::as_str).is_some_and(|id| id == "#atproto" || id == format!("{did}#atproto")))
        .and_then(|v| v.get("publicKeyMultibase").and_then(Value::as_str))
        .map(|k| format!("did:key:{k}"))
}

#[async_trait]
impl CrawlSource for NetCrawlSource {
    async fn list_repos(&self, base: &str, cursor: Option<&str>) -> Result<ReposPage, String> {
        let mut params = vec![("limit", "1000")];
        if let Some(c) = cursor {
            params.push(("cursor", c));
        }
        let r = self.get(base, "com.atproto.sync.listRepos", &params, BodyCap::APPVIEW, TIMEOUT).await.map_err(|e| format!("listRepos: {e}"))?;
        if !r.is_success() {
            return Err(format!("listRepos: HTTP {}", r.status.as_u16()));
        }
        r.json().map_err(|e| format!("listRepos: {e}"))
    }

    async fn relay_repo_status(&self, base: &str, did: &str) -> Result<Option<RepoStatus>, String> {
        let r = self
            .get(base, "com.atproto.sync.getRepoStatus", &[("did", did)], BodyCap::REPO_STATUS, TIMEOUT)
            .await
            .map_err(|e| format!("getRepoStatus: {e}"))?;
        if r.is_success() {
            #[derive(Deserialize)]
            struct S {
                active: bool,
                #[serde(default)]
                status: Option<String>,
            }
            let s: S = r.json().map_err(|e| format!("getRepoStatus: {e}"))?;
            return Ok(Some(RepoStatus { active: s.active, status: s.status }));
        }
        if xrpc_error(&r.body).as_deref() == Some("RepoNotFound") {
            return Ok(None);
        }
        Err(format!("getRepoStatus: HTTP {}", r.status.as_u16()))
    }

    async fn identity(&self, did: &str, fresh: bool) -> Result<Identity, IdentityMiss> {
        let _ = fresh;
        self.plc.wait().await;
        let source = if did.starts_with("did:web:") { "did_web" } else { "plc" };
        metrics::counter!(super::METRIC_IDENTITY_READS, "source" => source).increment(1);
        let doc = match self.resolver.fetch_document(did).await {
            Ok(d) => d.doc,
            Err(e @ (IdentityError::Tombstoned(_) | IdentityError::NotFound(_) | IdentityError::Gone(_))) => return Err(IdentityMiss::Gone(e.to_string())),
            Err(IdentityError::Upstream { status: 429, .. }) => {
                // The directory's limit is per address: the crawl's reads
                // wait, so members' and requests' resolutions keep theirs.
                self.plc.hold(Duration::from_secs(60));
                return Err(IdentityMiss::Failed("directory: 429".to_string()));
            }
            Err(e) => return Err(IdentityMiss::Failed(e.to_string())),
        };
        let info = crate::identity::resolver::extract_info(&doc, did);
        let Some(pds) = info.pds_endpoint else {
            return Err(IdentityMiss::Gone("the DID document names no PDS".to_string()));
        };
        Ok(Identity { pds, key: signing_key(&doc, did) })
    }

    async fn get_repo(&self, pds: &str, did: &str, max_bytes: u64) -> Result<RepoFetch, String> {
        self.pds.wait(pds).await;
        let cap = BodyCap(usize::try_from(max_bytes).unwrap_or(usize::MAX));
        match self.get(pds, "com.atproto.sync.getRepo", &[("did", did)], cap, Duration::from_secs(300)).await {
            Ok(r) if r.is_success() => Ok(RepoFetch::Car(r.body)),
            Ok(r) => match xrpc_error(&r.body).as_deref() {
                Some("RepoNotFound") => Ok(RepoFetch::NotFound),
                Some("RepoTakendown") => Ok(RepoFetch::Inactive("takendown".into())),
                Some("RepoSuspended") => Ok(RepoFetch::Inactive("suspended".into())),
                Some("RepoDeactivated") => Ok(RepoFetch::Inactive("deactivated".into())),
                e => Err(format!("getRepo: HTTP {} {e:?}", r.status.as_u16())),
            },
            Err(NetError::BodyTooLarge { .. }) => Ok(RepoFetch::TooLarge),
            Err(e) => Err(format!("getRepo: {e}")),
        }
    }

    async fn latest_rev(&self, pds: &str, did: &str) -> Result<String, String> {
        self.pds.wait(pds).await;
        let r = self
            .get(pds, "com.atproto.sync.getLatestCommit", &[("did", did)], BodyCap::REPO_STATUS, TIMEOUT)
            .await
            .map_err(|e| format!("getLatestCommit: {e}"))?;
        if !r.is_success() {
            return Err(format!("getLatestCommit: HTTP {}", r.status.as_u16()));
        }
        let v: Value = r.json().map_err(|e| format!("getLatestCommit: {e}"))?;
        match v.get("rev").and_then(Value::as_str) {
            Some(rev) if proto_blue::syntax::Tid::is_valid(rev) => Ok(rev.to_string()),
            other => Err(format!("getLatestCommit: invalid rev {other:?}")),
        }
    }

    async fn list_records(&self, pds: &str, did: &str, collection: &str, cursor: Option<&str>) -> Result<ListPage, String> {
        self.pds.wait(pds).await;
        let mut params = vec![("repo", did), ("collection", collection), ("limit", "100")];
        if let Some(c) = cursor {
            params.push(("cursor", c));
        }
        let r = self
            .get(pds, "com.atproto.repo.listRecords", &params, BodyCap::LIST_RECORDS, TIMEOUT)
            .await
            .map_err(|e| format!("listRecords {collection}: {e}"))?;
        if !r.is_success() {
            return Err(format!("listRecords {collection}: HTTP {}", r.status.as_u16()));
        }
        r.json().map_err(|e| format!("listRecords {collection}: {e}"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn relay_bases_from_stream_urls() {
        assert_eq!(relay_base("wss://relay.example").as_deref(), Some("https://relay.example"));
        assert_eq!(relay_base("wss://relay.example/xrpc/com.atproto.sync.subscribeRepos").as_deref(), Some("https://relay.example"));
        assert_eq!(relay_base("ws://127.0.0.1:2470").as_deref(), Some("http://127.0.0.1:2470"));
        assert_eq!(relay_base("other"), None);
    }

    #[test]
    fn the_signing_key_is_the_atproto_method() {
        let doc = serde_json::json!({
            "verificationMethod": [
                {"id": "did:plc:example1#atproto_label", "publicKeyMultibase": "zLabel"},
                {"id": "did:plc:example1#atproto", "publicKeyMultibase": "zRepo"}
            ]
        });
        assert_eq!(signing_key(&doc, "did:plc:example1").as_deref(), Some("did:key:zRepo"));
        assert_eq!(signing_key(&serde_json::json!({}), "did:plc:example1"), None);
    }

    #[tokio::test]
    async fn a_pace_spaces_its_slots() {
        let p = Pace::per_sec(50);
        let start = Instant::now();
        for _ in 0..6 {
            p.wait().await;
        }
        assert!(start.elapsed() >= Duration::from_millis(90), "{:?}", start.elapsed());
    }
}
