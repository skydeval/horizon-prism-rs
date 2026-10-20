//! DID resolution and handle verification (design-v2 §5.6, §6.7).
//!
//! - `did:plc`: `GET {identity.plc_url}/{did}`.
//! - `did:web`: `GET https://{host}/.well-known/did.json`, hostname form only.
//! - Every fetch goes through `net` (§8.5) with the 1 MiB DID-document cap.
//! - The PDS is the service `#atproto_pds` of type `AtprotoPersonalDataServer`.
//! - Handles are verified bidirectionally: the handle must resolve back to the
//!   DID, otherwise it is not used.
//! - Every network call goes through the transport of the resolver's `net`
//!   intent (design-v4 §11 G-9): handle DNS and well-known files, `did:web`
//!   documents and PLC. A request path takes the intent from its request
//!   ([`IdentityResolver::for_request`]); a background job pins its own
//!   ([`IdentityResolver::with_intent`]). The one substitution: a
//!   viewer-private PLC fetch uses the public client, as PLC's host is the
//!   same for every subject (§8.1).

use std::collections::HashSet;
use std::sync::{Arc, Mutex};
use std::time::Instant;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use reqwest::StatusCode;
use serde_json::Value;
use sqlx::PgPool;
use tokio::sync::Semaphore;

use super::cache::{CachedDoc, DidCache, Freshness};
use super::handle_lookup::{HandleProbe, candidate};
use super::lodestar::{DocAnswer, Lodestar};
use super::{IdentityError, IdentitySettings, PersistencePolicy, ResolveIntent, should_persist};
use crate::net::{BodyCap, FetchMode, Intent, NetClient, RequestOptions, Transport, Transports};

/// PDS service id fragment (§6.7).
pub const PDS_SERVICE_ID: &str = "#atproto_pds";
/// PDS service type (§6.7).
pub const PDS_SERVICE_TYPE: &str = "AtprotoPersonalDataServer";
/// Concurrent background refreshes of stale rows.
pub const BACKGROUND_REFRESH_PERMITS: usize = 8;

/// Whether a request-path lookup may use the document caches.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Caches {
    /// `did_cache` and the in-process document cache, read and written.
    Shared,
    /// Neither read nor written: a viewer-private lookup (design-v4 §8.1,
    /// R3-208).
    Bypass,
}

/// A supported DID method and its lookup target.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DidTarget {
    Plc,
    /// Hostname form only (§6.7).
    Web { host: String },
}

/// Parse a `did:plc` or hostname-form `did:web`.
///
/// # Errors
///
/// [`IdentityError::InvalidDid`] for malformed DIDs or path-form `did:web`,
/// and [`IdentityError::UnsupportedMethod`] for other methods.
pub fn parse_did(did: &str) -> Result<DidTarget, IdentityError> {
    let invalid = |reason: &str| IdentityError::InvalidDid {
        did: did.to_string(),
        reason: reason.to_string(),
    };
    if !proto_blue::syntax::Did::is_valid(did) {
        return Err(invalid("not a DID"));
    }
    if let Some(id) = did.strip_prefix("did:plc:") {
        let ok = id.len() == 24
            && id
                .bytes()
                .all(|b| b.is_ascii_lowercase() || (b'2'..=b'7').contains(&b));
        return if ok {
            Ok(DidTarget::Plc)
        } else {
            Err(invalid("did:plc identifiers are 24 base32 characters"))
        };
    }
    if let Some(rest) = did.strip_prefix("did:web:") {
        if rest.contains(':') {
            return Err(invalid("path-form did:web is not accepted"));
        }
        if rest.contains('%') {
            return Err(invalid("did:web with an encoded port is not accepted"));
        }
        crate::config::validate_hostname(rest).map_err(|r| invalid(&r))?;
        return Ok(DidTarget::Web {
            host: rest.to_string(),
        });
    }
    Err(IdentityError::UnsupportedMethod(did.to_string()))
}

/// The document URL for `did`.
#[must_use]
pub fn document_url(did: &str, target: &DidTarget, plc_url: &str) -> String {
    match target {
        DidTarget::Plc => format!("{}/{did}", plc_url.trim_end_matches('/')),
        DidTarget::Web { host } => format!("https://{host}/.well-known/did.json"),
    }
}

/// The start of the PLC directory's message for a tombstoned DID, which it
/// answers with 404.
pub const TOMBSTONE_MESSAGE: &str = "DID not available";

/// Whether a PLC 404 body is the directory's tombstone answer,
/// `{"message": "DID not available: <did>"}`, rather than "not registered".
#[must_use]
pub fn is_tombstone_answer(body: &[u8]) -> bool {
    serde_json::from_slice::<Value>(body)
        .ok()
        .and_then(|v| v.get("message").and_then(Value::as_str).map(|m| m.starts_with(TOMBSTONE_MESSAGE)))
        .unwrap_or(false)
}

/// What the AppView reads from a DID document.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DidDocInfo {
    /// `#atproto_pds` endpoint, when present and an absolute URL.
    pub pds_endpoint: Option<String>,
    /// The first `at://` entry of `alsoKnownAs`, unverified.
    pub claimed_handle: Option<String>,
}

/// Check that the document describes `did`.
///
/// # Errors
///
/// [`IdentityError::MalformedDocument`] when `id` is missing or different.
pub fn check_document_id(doc: &Value, did: &str) -> Result<(), IdentityError> {
    match doc.get("id").and_then(Value::as_str) {
        Some(id) if id == did => Ok(()),
        Some(id) => Err(IdentityError::MalformedDocument {
            did: did.to_string(),
            reason: format!("document id {id:?} does not match"),
        }),
        None => Err(IdentityError::MalformedDocument {
            did: did.to_string(),
            reason: "document has no id".to_string(),
        }),
    }
}

/// A PDS endpoint as the AppView keeps it: an absolute URL with a host,
/// without a trailing `/`. The one rule for a document's endpoint and for
/// one lodestar reports (lodestar-reads design §2.10).
#[must_use]
pub fn normalize_pds(endpoint: &str) -> Option<String> {
    url::Url::parse(endpoint)
        .is_ok_and(|u| u.host_str().is_some())
        .then(|| endpoint.trim_end_matches('/').to_string())
}

/// Extract the PDS endpoint and claimed handle.
#[must_use]
pub fn extract_info(doc: &Value, did: &str) -> DidDocInfo {
    let full_id = format!("{did}{PDS_SERVICE_ID}");
    let pds_endpoint = doc
        .get("service")
        .and_then(Value::as_array)
        .and_then(|services| {
            services.iter().find(|s| {
                let id = s.get("id").and_then(Value::as_str);
                let ty = s.get("type").and_then(Value::as_str);
                matches!(id, Some(i) if i == PDS_SERVICE_ID || i == full_id)
                    && ty == Some(PDS_SERVICE_TYPE)
            })
        })
        .and_then(|s| s.get("serviceEndpoint").and_then(Value::as_str))
        .and_then(normalize_pds);
    let claimed_handle = doc
        .get("alsoKnownAs")
        .and_then(Value::as_array)
        .and_then(|aka| {
            aka.iter()
                .filter_map(Value::as_str)
                .find_map(|a| a.strip_prefix("at://"))
        })
        .map(str::to_ascii_lowercase);
    DidDocInfo {
        pds_endpoint,
        claimed_handle,
    }
}

/// Bidirectional handle check (§5.6): the handle claimed by the DID document
/// must be syntactically valid and resolve back to `did`.
#[must_use]
pub fn verify_bidirectional(did: &str, claimed: Option<&str>, resolved: Option<&str>) -> Option<String> {
    let handle = claimed?.to_ascii_lowercase();
    if !proto_blue::syntax::Handle::is_valid(&handle) {
        return None;
    }
    (resolved? == did).then_some(handle)
}

/// A fetched DID document.
#[derive(Debug, Clone, PartialEq)]
pub struct FetchedDoc {
    /// The body exactly as served.
    pub raw: Vec<u8>,
    pub content_type: Option<String>,
    pub doc: Arc<Value>,
    pub fetched_at: DateTime<Utc>,
}

impl FetchedDoc {
    #[must_use]
    pub fn cached(&self) -> CachedDoc {
        CachedDoc {
            doc: Arc::clone(&self.doc),
            fetched_at: self.fetched_at,
        }
    }
}

/// Resolves handles to DIDs, through the transport it is given.
#[async_trait]
pub trait HandleLookup: Send + Sync {
    async fn resolve_handle(&self, transport: &Transport, handle: &str) -> Option<String>;
}

/// The `_atproto` TXT record and the well-known file, both asked through
/// the caller's transport (its resolver and its client). The TXT record
/// wins when both name a DID, as in the reference resolver.
struct ProbeHandles;

#[async_trait]
impl HandleLookup for ProbeHandles {
    async fn resolve_handle(&self, transport: &Transport, handle: &str) -> Option<String> {
        let (txt, well_known) = HandleProbe::through(transport).run(handle).await;
        candidate(&txt, &well_known).map(str::to_string)
    }
}

struct Inner {
    nets: Transports,
    cache: DidCache,
    settings: IdentitySettings,
    policy: Arc<dyn PersistencePolicy>,
    handles: Arc<dyn HandleLookup>,
    refreshing: Mutex<HashSet<String>>,
    refresh_permits: Arc<Semaphore>,
}

/// Which upstream a request-path read is about to ask, for its `admit`
/// (lodestar-reads design §2.3, §2.11).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Upstream {
    /// Lodestar: a "no" means "skip it", never "no answer".
    Lodestar,
    /// The PLC directory (or a `did:web` host): a "no" is `NoBudget`.
    Plc,
}

/// A request's admission of one network call, asked just before it.
pub type Admit<'a> = &'a (dyn Fn(Upstream, &str) -> bool + Sync);

/// Admits every call (background work and callers with no budget).
pub fn admit_all(_upstream: Upstream, _key: &str) -> bool {
    true
}

/// The cached DID resolver, making its network calls through one `net`
/// intent. Cheap to clone; every intent's view shares the caches.
///
/// A view may carry lodestar ([`IdentityResolver::with_lodestar`]); it is
/// asked only by a view whose intent is `Public`, and only by the read
/// methods. [`IdentityResolver::fetch_document`],
/// [`IdentityResolver::refresh_now`] and [`IdentityResolver::resolve_fresh`]
/// read the PLC directory only, whatever the view, and `did_cache` never
/// holds a lodestar document (lodestar-reads design §2.2, §2.5).
#[derive(Clone)]
pub struct IdentityResolver {
    inner: Arc<Inner>,
    intent: Intent,
    lodestar: Option<Arc<Lodestar>>,
    /// The request's deadline, which a lodestar call never passes.
    deadline: Option<Instant>,
}

impl IdentityResolver {
    /// A resolver over `nets`, making its calls as `intent`.
    #[must_use]
    pub fn new(
        nets: Transports,
        pool: PgPool,
        settings: IdentitySettings,
        policy: Arc<dyn PersistencePolicy>,
        intent: Intent,
    ) -> Self {
        Self::with_handles(nets, pool, settings, policy, Arc::new(ProbeHandles), intent)
    }

    /// Construct with a specific handle backend.
    #[must_use]
    pub fn with_handles(
        nets: Transports,
        pool: PgPool,
        settings: IdentitySettings,
        policy: Arc<dyn PersistencePolicy>,
        handles: Arc<dyn HandleLookup>,
        intent: Intent,
    ) -> Self {
        let cache = DidCache::new(pool, settings.clone());
        Self {
            inner: Arc::new(Inner {
                nets,
                cache,
                settings,
                policy,
                handles,
                refreshing: Mutex::new(HashSet::new()),
                refresh_permits: Arc::new(Semaphore::new(BACKGROUND_REFRESH_PERMITS)),
            }),
            intent,
            lodestar: None,
            deadline: None,
        }
    }

    /// This resolver, making its network calls as `intent` (design-v4
    /// §11 G-9). The caches are the same.
    #[must_use]
    pub fn with_intent(&self, intent: Intent) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
            intent,
            lodestar: self.lodestar.clone(),
            deadline: self.deadline,
        }
    }

    /// This view, reading identities from `lodestar` first on public
    /// request paths (lodestar-reads design §2.2).
    #[must_use]
    pub fn with_lodestar(&self, lodestar: Arc<Lodestar>) -> Self {
        Self {
            lodestar: Some(lodestar),
            ..self.clone()
        }
    }

    /// This view without lodestar: every read goes to the PLC directory.
    /// The key path, the members' keys and the refresher take this view.
    #[must_use]
    pub fn directory_only(&self) -> Self {
        Self {
            lodestar: None,
            ..self.clone()
        }
    }

    /// This view for a request ending at `deadline`: a lodestar call never
    /// runs past it.
    #[must_use]
    pub fn with_deadline(&self, deadline: Instant) -> Self {
        Self {
            deadline: Some(deadline),
            ..self.clone()
        }
    }

    /// Lodestar, when this view may ask it, for a batch (§2.10).
    #[must_use]
    pub fn public_lodestar(&self) -> Option<Arc<Lodestar>> {
        self.lodestar.clone().filter(|_| self.intent == Intent::Public)
    }

    /// Lodestar, when this view may ask it: it carries one and its intent
    /// is `Public` (§2.9).
    fn lodestar(&self) -> Option<&Lodestar> {
        self.lodestar.as_deref().filter(|_| self.intent == Intent::Public)
    }

    /// Lodestar, when the gate is open and `admit` grants the call; each
    /// skip is counted.
    fn ask_lodestar(&self, admit: Admit<'_>, key: &str) -> Option<&Lodestar> {
        let lodestar = self.lodestar()?;
        if !lodestar.gate().is_open() {
            metrics::counter!(super::lodestar::METRIC_SKIPPED, "reason" => "gate_closed").increment(1);
            return None;
        }
        if !admit(Upstream::Lodestar, key) {
            metrics::counter!(super::lodestar::METRIC_SKIPPED, "reason" => "no_budget").increment(1);
            return None;
        }
        Some(lodestar)
    }

    /// This resolver for a fetch made for `req`: the intent is the
    /// request's ([`crate::net::Transports::for_request`]), whatever this
    /// one's is, so a viewer-private lookup never goes through the public
    /// transport (design-v4 §8.1, R3-213).
    #[must_use]
    pub fn for_request(&self, req: &impl FetchMode) -> Self {
        self.with_intent(req.intent())
    }

    /// The intent of this resolver's network calls.
    #[must_use]
    pub fn intent(&self) -> Intent {
        self.intent
    }

    fn transport(&self) -> &Transport {
        self.inner.nets.get(self.intent)
    }

    #[must_use]
    pub fn cache(&self) -> &DidCache {
        &self.inner.cache
    }

    /// The HTTP client of this resolver's intent.
    #[must_use]
    pub fn net(&self) -> &NetClient {
        self.transport().client()
    }

    /// The client a document of `target` is fetched with: this resolver's,
    /// except that a viewer-private PLC fetch uses the public client, as
    /// PLC is a fixed upstream (design-v4 §8.1).
    fn document_client(&self, target: &DidTarget) -> &NetClient {
        match (self.intent, target) {
            (Intent::ViewerPrivate, DidTarget::Plc) => self.inner.nets.public().client(),
            _ => self.net(),
        }
    }

    /// Fetch `did`'s document from the network, bypassing every cache.
    ///
    /// # Errors
    ///
    /// Invalid DIDs, network failures, `NotFound` (404), `Tombstoned` (PLC
    /// 404 `DID not available`, or 410), `Gone` (did:web 410), other non-2xx
    /// statuses as `Upstream`, and documents that are not JSON or describe
    /// another DID.
    pub async fn fetch_document(&self, did: &str) -> Result<FetchedDoc, IdentityError> {
        let target = parse_did(did)?;
        let url = document_url(did, &target, &self.inner.settings.plc_url);
        let response = self
            .document_client(&target)
            .get(&url, &RequestOptions::new(BodyCap::DID_DOCUMENT))
            .await?;
        match response.status {
            s if s.is_success() => {}
            // The directory answers a tombstoned DID with 404 `DID not
            // available` (lodestar-reads design §2.4), not 410.
            StatusCode::NOT_FOUND if target == DidTarget::Plc && is_tombstone_answer(&response.body) => {
                return Err(IdentityError::Tombstoned(did.to_string()));
            }
            StatusCode::NOT_FOUND => return Err(IdentityError::NotFound(did.to_string())),
            StatusCode::GONE => {
                return Err(match target {
                    DidTarget::Plc => IdentityError::Tombstoned(did.to_string()),
                    DidTarget::Web { .. } => IdentityError::Gone(did.to_string()),
                });
            }
            s => {
                return Err(IdentityError::Upstream {
                    did: did.to_string(),
                    status: s.as_u16(),
                });
            }
        }
        let doc: Value =
            serde_json::from_slice(&response.body).map_err(|e| IdentityError::MalformedDocument {
                did: did.to_string(),
                reason: e.to_string(),
            })?;
        check_document_id(&doc, did)?;
        Ok(FetchedDoc {
            raw: response.body,
            content_type: response.content_type,
            doc: Arc::new(doc),
            fetched_at: Utc::now(),
        })
    }

    /// Store a fetched document under the persistence rule (§6.7).
    ///
    /// # Errors
    ///
    /// Any database error.
    pub async fn store(
        &self,
        did: &str,
        fetched: &FetchedDoc,
        intent: ResolveIntent,
    ) -> Result<(), sqlx::Error> {
        let persist = should_persist(intent, self.inner.policy.as_ref(), did);
        self.inner.cache.store(did, fetched.cached(), persist).await
    }

    /// Fetch from the network and store (the refresher's "bypass the cache").
    ///
    /// # Errors
    ///
    /// As [`IdentityResolver::fetch_document`], plus database errors.
    pub async fn refresh_now(
        &self,
        did: &str,
        intent: ResolveIntent,
    ) -> Result<FetchedDoc, IdentityError> {
        let fetched = self.fetch_document(did).await?;
        self.store(did, &fetched, intent).await?;
        Ok(fetched)
    }

    /// Resolve `did` through the caches (off the JWT path): fresh and stale
    /// entries are served, a stale one also triggers a background refresh,
    /// and an expired entry or a miss fetches synchronously.
    ///
    /// # Errors
    ///
    /// As [`IdentityResolver::refresh_now`].
    pub async fn resolve(&self, did: &str, intent: ResolveIntent) -> Result<Arc<Value>, IdentityError> {
        self.resolve_admitted(did, intent, &admit_all).await
    }

    /// [`IdentityResolver::resolve`] for a request: `admit` is asked just
    /// before each network call, with the upstream it would ask; for the
    /// PLC directory a `false` ends the resolution without a call, for
    /// lodestar it only skips lodestar. A cached entry costs nothing.
    ///
    /// With lodestar (a public view that carries it, lodestar-reads design
    /// §2.3): a fresh `did_cache` entry, then the lodestar cache when it is
    /// newer than a stale entry, then the stale entry (with a background
    /// refresh), then lodestar, then the PLC directory.
    ///
    /// # Errors
    ///
    /// As [`IdentityResolver::resolve`], plus [`IdentityError::NoBudget`].
    pub async fn resolve_admitted(&self, did: &str, intent: ResolveIntent, admit: Admit<'_>) -> Result<Arc<Value>, IdentityError> {
        parse_did(did)?;
        let cached = match self.inner.cache.lookup(did).await {
            Ok(found) => found,
            Err(error) => {
                tracing::warn!(event = "did_cache_read_failed", did, %error, "did_cache read failed; fetching");
                None
            }
        };
        let stale = match cached {
            Some((entry, Freshness::Fresh)) => return Ok(entry.doc),
            Some((entry, Freshness::Stale)) => Some(entry),
            Some((_, Freshness::Expired)) | None => None,
        };
        if let Some(lodestar) = self.lodestar() {
            let held = lodestar.docs().get(did).await;
            metrics::counter!(super::cache::METRIC_REQUESTS, "cache" => "lodestar", "result" => if held.is_some() { "hit" } else { "miss" })
                .increment(1);
            if let Some(entry) = held.filter(|e| stale.as_ref().is_none_or(|s| e.fetched_at >= s.fetched_at)) {
                return Ok(entry.doc);
            }
        }
        if let Some(entry) = stale {
            self.spawn_background_refresh(did, intent);
            return Ok(entry.doc);
        }
        Ok(self.refresh_read(did, intent, admit).await?.doc)
    }

    /// A read's fetch (lodestar-reads design §2.3): lodestar first, for a
    /// `did:plc` while the gate is open and `admit` grants it, kept in the
    /// lodestar cache; otherwise, or when lodestar cannot answer, the PLC
    /// directory, stored as [`IdentityResolver::refresh_now`] stores. A
    /// lodestar tombstone is final.
    async fn refresh_read(&self, did: &str, intent: ResolveIntent, admit: Admit<'_>) -> Result<FetchedDoc, IdentityError> {
        if did.starts_with("did:plc:")
            && let Some(lodestar) = self.ask_lodestar(admit, did)
        {
            match lodestar.document(did, self.deadline).await {
                DocAnswer::Doc(fetched) => {
                    lodestar.docs().insert(did.to_string(), fetched.cached()).await;
                    return Ok(fetched);
                }
                DocAnswer::Tombstoned => return Err(IdentityError::Tombstoned(did.to_string())),
                DocAnswer::Fallback => {}
            }
        }
        if !admit(Upstream::Plc, did) {
            return Err(IdentityError::NoBudget(did.to_string()));
        }
        self.refresh_now(did, intent).await
    }

    /// [`IdentityResolver::resolve_admitted`] plus extraction.
    ///
    /// # Errors
    ///
    /// As [`IdentityResolver::resolve_admitted`].
    pub async fn resolve_info_admitted(&self, did: &str, intent: ResolveIntent, admit: Admit<'_>) -> Result<DidDocInfo, IdentityError> {
        let doc = self.resolve_admitted(did, intent, admit).await?;
        Ok(extract_info(&doc, did))
    }

    /// [`IdentityResolver::resolve_info_admitted`] with no cache read and no
    /// cache write: a viewer-private lookup (design-v4 §8.1) leaves neither
    /// `did_cache` nor the in-process document cache touched, even for an
    /// in-scope DID. `admit` is asked just before the fetch. Lodestar is
    /// never asked (§2.9).
    ///
    /// # Errors
    ///
    /// As [`IdentityResolver::fetch_document`], plus
    /// [`IdentityError::NoBudget`].
    pub async fn resolve_info_uncached(&self, did: &str, admit: Admit<'_>) -> Result<DidDocInfo, IdentityError> {
        parse_did(did)?;
        if !admit(Upstream::Plc, did) {
            return Err(IdentityError::NoBudget(did.to_string()));
        }
        Ok(extract_info(&self.fetch_document(did).await?.doc, did))
    }

    /// A document's info for `did` under `caches` (design-v4 §8.1).
    async fn info_with(&self, did: &str, admit: Admit<'_>, caches: Caches) -> Result<DidDocInfo, IdentityError> {
        match caches {
            Caches::Shared => self.resolve_info_admitted(did, ResolveIntent::Lookup, admit).await,
            Caches::Bypass => self.resolve_info_uncached(did, admit).await,
        }
    }

    /// A fresh resolution (admin-ui design §5.4): the document is fetched
    /// from the network whatever the caches hold, through this resolver's
    /// transport, and stored as any lookup's would be.
    ///
    /// # Errors
    ///
    /// As [`IdentityResolver::refresh_now`].
    pub async fn resolve_fresh(&self, did: &str) -> Result<DidDocInfo, IdentityError> {
        let fetched = self.refresh_now(did, ResolveIntent::Lookup).await?;
        Ok(extract_info(&fetched.doc, did))
    }

    /// [`IdentityResolver::resolve`] plus extraction.
    ///
    /// # Errors
    ///
    /// As [`IdentityResolver::resolve`].
    pub async fn resolve_info(&self, did: &str, intent: ResolveIntent) -> Result<DidDocInfo, IdentityError> {
        let doc = self.resolve(did, intent).await?;
        Ok(extract_info(&doc, did))
    }

    /// The DID for `handle`, verified bidirectionally (the DID document
    /// claims the handle back), or `None`.
    pub async fn resolve_handle(&self, handle: &str) -> Option<String> {
        self.resolve_handle_admitted(handle, &admit_all).await
    }

    /// [`IdentityResolver::resolve_handle`] for a request; `admit` as in
    /// [`IdentityResolver::resolve_admitted`].
    pub async fn resolve_handle_admitted(&self, handle: &str, admit: Admit<'_>) -> Option<String> {
        self.resolve_handle_with(handle, admit, Caches::Shared).await
    }

    /// [`IdentityResolver::resolve_handle_admitted`], with the document read
    /// under `caches`. With lodestar, its verified DID is tried first and
    /// kept only when the DID's document claims the handle back; otherwise
    /// the probes run, as without it (lodestar-reads design §2.7).
    pub async fn resolve_handle_with(&self, handle: &str, admit: Admit<'_>, caches: Caches) -> Option<String> {
        let handle = handle.trim().trim_start_matches('@').to_ascii_lowercase();
        if let Some(did) = self.lodestar_handle(&handle, admit, caches).await {
            return Some(did);
        }
        let did = self.inner.handles.resolve_handle(self.transport(), &handle).await?;
        self.claims(&did, &handle, admit, caches).await.then_some(did)
    }

    /// Whether `did`'s document, read under `caches`, claims `handle`.
    async fn claims(&self, did: &str, handle: &str, admit: Admit<'_>, caches: Caches) -> bool {
        match self.info_with(did, admit, caches).await {
            Ok(info) => verify_bidirectional(did, info.claimed_handle.as_deref(), Some(did)).is_some_and(|h| h.eq_ignore_ascii_case(handle)),
            Err(_) => false,
        }
    }

    /// The DID lodestar holds `handle` verified to, kept only when that
    /// DID's document claims the handle back. `None` sends the caller to
    /// the probes: lodestar's "no" and a document that does not claim the
    /// handle (lodestar's lag) both decide nothing (§2.7, review 2 LB1).
    async fn lodestar_handle(&self, handle: &str, admit: Admit<'_>, caches: Caches) -> Option<String> {
        let lodestar = self.ask_lodestar(admit, handle)?;
        let did = lodestar.handle(handle, self.deadline).await?;
        self.claims(&did, handle, admit, caches).await.then_some(did)
    }

    /// The DID of `handle` with the reason kept when there is none
    /// (design-v3 §5.11): the TXT record and the well-known file are both
    /// asked, through this resolver's transport, a DID found is verified
    /// against its document, and "no such handle" is told apart from
    /// "could not find out". `admit` as in
    /// [`IdentityResolver::resolve_admitted`]; the document is read under
    /// `caches`.
    pub async fn resolve_handle_typed(&self, handle: &str, admit: Admit<'_>, caches: Caches) -> super::handle_lookup::HandleOutcome {
        use super::handle_lookup::{HandleOutcome, Step, neither_found};

        // Lodestar's verified DID, when its document claims the handle
        // back; anything else runs the probes below (§2.7).
        if let Some(did) = self.lodestar_handle(handle, admit, caches).await {
            return HandleOutcome::Found(did);
        }
        let (from_txt, from_file) = HandleProbe::through(self.transport()).run(handle).await;
        let Some(did) = candidate(&from_txt, &from_file) else {
            return match neither_found(&from_txt, &from_file) {
                Step::Confirmed => HandleOutcome::NotFound,
                _ => HandleOutcome::Unavailable,
            };
        };
        // The document must name the handle back. A document that could
        // not be fetched decides nothing.
        match self.info_with(did, admit, caches).await {
            Ok(info) if info.claimed_handle.as_deref().is_some_and(|claimed| claimed.eq_ignore_ascii_case(handle)) => {
                HandleOutcome::Found(did.to_string())
            }
            Ok(_) => HandleOutcome::NotFound,
            Err(_) => HandleOutcome::Unavailable,
        }
    }

    /// The verified handle for `did`, or `None`.
    ///
    /// A resolution made with the internal intent (backfill, the identity
    /// refresher) is counted in `handle_resolutions_total{intent =
    /// "internal"}`: `network_ok` when the handle named a DID, and
    /// `network_not_found` when it named none (the probe does not tell a
    /// failure apart from an absence here).
    pub async fn verify_handle(&self, did: &str, claimed: Option<&str>) -> Option<String> {
        self.verify_handle_admitted(did, claimed, &admit_all).await
    }

    /// [`IdentityResolver::verify_handle`] for a request. With lodestar, a
    /// `did:plc` whose account lodestar reports at the tip with `handle`
    /// equal to `claimed` is verified without a probe; any other answer
    /// runs the probe (lodestar-reads design §2.7). Lodestar's negatives
    /// are never taken.
    pub async fn verify_handle_admitted(&self, did: &str, claimed: Option<&str>, admit: Admit<'_>) -> Option<String> {
        let claimed = claimed?;
        if did.starts_with("did:plc:")
            && let Some(lodestar) = self.ask_lodestar(admit, did)
            && lodestar
                .account(did, self.deadline)
                .await
                .and_then(|account| account.handle)
                .is_some_and(|handle| handle.eq_ignore_ascii_case(claimed))
        {
            return verify_bidirectional(did, Some(claimed), Some(did));
        }
        let resolved = self.inner.handles.resolve_handle(self.transport(), claimed).await;
        if self.intent == Intent::Internal {
            let result = if resolved.is_some() { "network_ok" } else { "network_not_found" };
            metrics::counter!(crate::constellation::routing::METRIC_HANDLE_RESOLUTIONS, "intent" => "internal", "result" => result).increment(1);
        }
        verify_bidirectional(did, Some(claimed), resolved.as_deref())
    }

    /// Refresh a stale entry in the background: at most one refresh per DID at
    /// a time and [`BACKGROUND_REFRESH_PERMITS`] overall. Skipped when busy;
    /// the next stale read tries again.
    fn spawn_background_refresh(&self, did: &str, intent: ResolveIntent) {
        let Ok(permit) = Arc::clone(&self.inner.refresh_permits).try_acquire_owned() else {
            return;
        };
        {
            let Ok(mut refreshing) = self.inner.refreshing.lock() else {
                return;
            };
            if !refreshing.insert(did.to_string()) {
                return;
            }
        }
        // Uncharged, as before; with lodestar it is asked first, with no
        // request deadline (the request has moved on).
        let this = Self {
            deadline: None,
            ..self.clone()
        };
        let did = did.to_string();
        tokio::spawn(async move {
            if let Err(error) = this.refresh_read(&did, intent, &admit_all).await {
                tracing::debug!(event = "did_background_refresh_failed", did = %did, %error, "background DID refresh failed");
            }
            if let Ok(mut refreshing) = this.inner.refreshing.lock() {
                refreshing.remove(&did);
            }
            drop(permit);
        });
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    const PLC: &str = "did:plc:ewvi7nxzyoun6zhxrhs64oiz";

    #[test]
    fn parses_did_plc() {
        assert_eq!(parse_did(PLC).expect("valid"), DidTarget::Plc);
        assert_eq!(
            document_url(PLC, &DidTarget::Plc, "https://plc.directory/"),
            format!("https://plc.directory/{PLC}")
        );
        for bad in [
            "did:plc:short",
            "did:plc:EWVI7NXZYOUN6ZHXRHS64OIZ",
            "did:plc:ewvi7nxzyoun6zhxrhs64oi1",
            "did:plc:ewvi7nxzyoun6zhxrhs64oizz",
        ] {
            assert!(matches!(parse_did(bad), Err(IdentityError::InvalidDid { .. })), "{bad}");
        }
    }

    #[test]
    fn parses_hostname_did_web() {
        let target = parse_did("did:web:api.bsky.app").expect("valid");
        assert_eq!(
            target,
            DidTarget::Web {
                host: "api.bsky.app".to_string()
            }
        );
        assert_eq!(
            document_url("did:web:api.bsky.app", &target, "https://plc.directory"),
            "https://api.bsky.app/.well-known/did.json"
        );
    }

    #[test]
    fn rejects_path_form_and_port_did_web() {
        for bad in [
            "did:web:example.com:user:alice",
            "did:web:localhost%3A3000",
            "did:web:Example.com",
            "did:web:localhost",
        ] {
            assert!(matches!(parse_did(bad), Err(IdentityError::InvalidDid { .. })), "{bad}");
        }
    }

    #[test]
    fn rejects_other_methods_and_garbage() {
        assert!(matches!(
            parse_did("did:key:z6MkhaXgBZDvotDkL5257faiztiGiC2QtKLGpbnnEGta2doK"),
            Err(IdentityError::UnsupportedMethod(_))
        ));
        assert!(matches!(parse_did("not-a-did"), Err(IdentityError::InvalidDid { .. })));
    }

    fn doc(services: Value) -> Value {
        json!({
            "id": PLC,
            "alsoKnownAs": ["at://Alice.Example.com"],
            "service": services,
        })
    }

    #[test]
    fn extracts_pds_endpoint_by_id_and_type() {
        let d = doc(json!([
            {"id": "#bsky_notif", "type": "BskyNotificationService", "serviceEndpoint": "https://notif.example"},
            {"id": "#atproto_pds", "type": "AtprotoPersonalDataServer", "serviceEndpoint": "https://pds.example.com/"}
        ]));
        let info = extract_info(&d, PLC);
        assert_eq!(info.pds_endpoint.as_deref(), Some("https://pds.example.com"));
        assert_eq!(info.claimed_handle.as_deref(), Some("alice.example.com"));
    }

    #[test]
    fn accepts_fully_qualified_pds_id() {
        let d = doc(json!([
            {"id": format!("{PLC}#atproto_pds"), "type": "AtprotoPersonalDataServer", "serviceEndpoint": "https://pds.example.com"}
        ]));
        assert_eq!(
            extract_info(&d, PLC).pds_endpoint.as_deref(),
            Some("https://pds.example.com")
        );
    }

    #[test]
    fn ignores_wrong_type_or_bad_endpoint() {
        let wrong_type = doc(json!([
            {"id": "#atproto_pds", "type": "SomethingElse", "serviceEndpoint": "https://pds.example.com"}
        ]));
        assert_eq!(extract_info(&wrong_type, PLC).pds_endpoint, None);
        let bad_endpoint = doc(json!([
            {"id": "#atproto_pds", "type": "AtprotoPersonalDataServer", "serviceEndpoint": "not a url"}
        ]));
        assert_eq!(extract_info(&bad_endpoint, PLC).pds_endpoint, None);
        let other_did = doc(json!([
            {"id": "did:plc:someoneelse#atproto_pds", "type": "AtprotoPersonalDataServer", "serviceEndpoint": "https://pds.example.com"}
        ]));
        assert_eq!(extract_info(&other_did, PLC).pds_endpoint, None);
        assert_eq!(extract_info(&json!({"id": PLC}), PLC), DidDocInfo {
            pds_endpoint: None,
            claimed_handle: None
        });
    }

    #[test]
    fn document_id_must_match() {
        check_document_id(&json!({"id": PLC}), PLC).expect("matches");
        assert!(check_document_id(&json!({"id": "did:plc:other"}), PLC).is_err());
        assert!(check_document_id(&json!({}), PLC).is_err());
    }

    struct NeverPersist;

    impl PersistencePolicy for NeverPersist {
        fn in_persistence_set(&self, _did: &str) -> bool {
            false
        }
    }

    /// B-54 "Every `net` call site names its intent": each view of the
    /// resolver fetches through its own intent's client, a request's view
    /// takes the request's intent, and a viewer-private view uses the public
    /// client for PLC only (design-v4 §8.1, §11 G-9; R3-168, R3-213).
    #[tokio::test]
    async fn each_intent_fetches_through_its_own_client() {
        use crate::xrpc::pools::Requester;

        let pool = PgPool::connect_lazy("postgres://nobody@127.0.0.1:1/none").expect("lazy pool");
        let settings = IdentitySettings {
            plc_url: "https://plc.directory".to_string(),
            fresh: std::time::Duration::from_secs(3600),
            stale_max: std::time::Duration::from_secs(86_400),
        };
        let resolver = IdentityResolver::new(Transports::loopback_for_tests(), pool, settings, Arc::new(NeverPersist), Intent::Internal);
        assert_eq!(resolver.intent(), Intent::Internal);
        let web = DidTarget::Web {
            host: "w.example.com".to_string(),
        };
        for (intent, plc, did_web) in [
            (Intent::Public, Intent::Public, Intent::Public),
            (Intent::Internal, Intent::Internal, Intent::Internal),
            (Intent::ViewerPrivate, Intent::Public, Intent::ViewerPrivate),
        ] {
            let view = resolver.with_intent(intent);
            assert_eq!((view.intent(), view.net().intent(), view.transport().intent()), (intent, intent, intent));
            assert_eq!(view.document_client(&DidTarget::Plc).intent(), plc, "{intent:?}");
            assert_eq!(view.document_client(&web).intent(), did_web, "{intent:?}");
        }
        // A request's view ignores the intent it was built from.
        let req = Requester::test();
        assert_eq!(resolver.for_request(&req).intent(), Intent::Public);
        let private = req.viewer_private("did:plc:vvvvvvvvvvvvvvvvvvvvvvvv");
        assert_eq!(resolver.for_request(&private).intent(), Intent::ViewerPrivate);
        assert_eq!(resolver.with_intent(Intent::Public).for_request(&private).intent(), Intent::ViewerPrivate);
    }

    /// design-v4 §14.2 (R3-155): an internal handle resolution is counted
    /// under `intent="internal"`; a public view's `verify_handle` (a
    /// request path that is not `resolveHandle`) is not counted at all.
    #[tokio::test]
    async fn internal_handle_resolutions_are_counted_apart() {
        struct Fixed;
        #[async_trait]
        impl HandleLookup for Fixed {
            async fn resolve_handle(&self, _transport: &Transport, handle: &str) -> Option<String> {
                (handle == "alice.example.com").then(|| PLC.to_string())
            }
        }
        let recorder = metrics_exporter_prometheus::PrometheusBuilder::new().build_recorder();
        let handle = recorder.handle();
        let _local = metrics::set_default_local_recorder(&recorder);
        let pool = PgPool::connect_lazy("postgres://nobody@127.0.0.1:1/none").expect("lazy pool");
        let settings = IdentitySettings {
            plc_url: "https://plc.directory".to_string(),
            fresh: std::time::Duration::from_secs(3600),
            stale_max: std::time::Duration::from_secs(86_400),
        };
        let resolver = IdentityResolver::with_handles(Transports::loopback_for_tests(), pool, settings, Arc::new(NeverPersist), Arc::new(Fixed), Intent::Internal);
        assert_eq!(resolver.verify_handle(PLC, Some("alice.example.com")).await.as_deref(), Some("alice.example.com"));
        assert_eq!(resolver.verify_handle(PLC, Some("nobody.example.com")).await, None);
        resolver.with_intent(Intent::Public).verify_handle(PLC, Some("alice.example.com")).await;
        let text = handle.render();
        let count = |result: &str| {
            let label = format!("result=\"{result}\"");
            crate::test_support::series(&text, "handle_resolutions_total", &["intent=\"internal\"", &label])
        };
        assert_eq!((count("network_ok"), count("network_not_found")), (Some(1.0), Some(1.0)), "{text}");
        assert!(!text.contains("intent=\"public\""), "{text}");
    }

    /// lodestar-reads T-L2: the directory's 404 `DID not available` is a
    /// tombstone, its 404 `DID not registered` is not, and 410 still is.
    #[test]
    fn the_directorys_tombstone_message_is_recognised() {
        assert!(is_tombstone_answer(format!(r#"{{"message":"DID not available: {PLC}"}}"#).as_bytes()));
        assert!(!is_tombstone_answer(format!(r#"{{"message":"DID not registered: {PLC}"}}"#).as_bytes()));
        assert!(!is_tombstone_answer(b"DID not available"));
        assert!(!is_tombstone_answer(br#"{"error":"DID not available"}"#));
        assert!(!is_tombstone_answer(b""));
    }

    /// lodestar-reads T-L2, through `fetch_document`.
    #[tokio::test]
    async fn a_tombstoned_did_plc_is_tombstoned() {
        use axum::extract::Path;
        use axum::http::StatusCode as S;
        use axum::routing::get;

        async fn plc(Path(did): Path<String>) -> (S, String) {
            match did.as_bytes().get(8) {
                Some(b't') => (S::NOT_FOUND, format!(r#"{{"message":"DID not available: {did}"}}"#)),
                Some(b'n') => (S::NOT_FOUND, format!(r#"{{"message":"DID not registered: {did}"}}"#)),
                Some(b'g') => (S::GONE, String::new()),
                _ => (S::NOT_FOUND, "not json".to_string()),
            }
        }
        let base = crate::test_support::serve_fake(axum::Router::new().route("/{did}", get(plc))).await;
        let pool = PgPool::connect_lazy("postgres://nobody@127.0.0.1:1/none").expect("lazy pool");
        let settings = IdentitySettings {
            plc_url: base,
            fresh: std::time::Duration::from_secs(3600),
            stale_max: std::time::Duration::from_secs(86_400),
        };
        let resolver = IdentityResolver::new(Transports::loopback_for_tests(), pool, settings, Arc::new(NeverPersist), Intent::Internal);
        let did = |c: char| format!("did:plc:{}", c.to_string().repeat(24));
        assert!(matches!(resolver.fetch_document(&did('t')).await, Err(IdentityError::Tombstoned(_))));
        assert!(matches!(resolver.fetch_document(&did('n')).await, Err(IdentityError::NotFound(_))));
        assert!(matches!(resolver.fetch_document(&did('g')).await, Err(IdentityError::Tombstoned(_))));
        assert!(matches!(resolver.fetch_document(&did('x')).await, Err(IdentityError::NotFound(_))));
    }

    #[test]
    fn handle_verification_is_bidirectional() {
        assert_eq!(
            verify_bidirectional(PLC, Some("Alice.Example.com"), Some(PLC)).as_deref(),
            Some("alice.example.com")
        );
        // The handle resolves to someone else.
        assert_eq!(verify_bidirectional(PLC, Some("alice.example.com"), Some("did:plc:other")), None);
        // The handle does not resolve.
        assert_eq!(verify_bidirectional(PLC, Some("alice.example.com"), None), None);
        // No claimed handle, or an invalid one.
        assert_eq!(verify_bidirectional(PLC, None, Some(PLC)), None);
        assert_eq!(verify_bidirectional(PLC, Some("not a handle"), Some(PLC)), None);
    }
}
