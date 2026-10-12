//! DID resolution and handle verification (design-v2 §5.6, §6.7).
//!
//! - `did:plc`: `GET {identity.plc_url}/{did}`.
//! - `did:web`: `GET https://{host}/.well-known/did.json`, hostname form only.
//! - Every fetch goes through `net` (§8.5) with the 1 MiB DID-document cap.
//! - The PDS is the service `#atproto_pds` of type `AtprotoPersonalDataServer`.
//! - Handles are verified bidirectionally: the handle must resolve back to the
//!   DID, otherwise it is not used.

use std::collections::{BTreeMap, HashSet};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use proto_blue::common::fetch::{FetchError, FetchHandler, HttpMethod, HttpRequest, HttpResponse};
use reqwest::StatusCode;
use serde_json::Value;
use sqlx::PgPool;
use tokio::sync::Semaphore;

use super::cache::{CachedDoc, DidCache, Freshness};
use super::{IdentityError, IdentitySettings, PersistencePolicy, ResolveIntent, should_persist};
use crate::net::{BodyCap, NetClient, RequestOptions};

/// PDS service id fragment (§6.7).
pub const PDS_SERVICE_ID: &str = "#atproto_pds";
/// PDS service type (§6.7).
pub const PDS_SERVICE_TYPE: &str = "AtprotoPersonalDataServer";
/// Concurrent background refreshes of stale rows.
pub const BACKGROUND_REFRESH_PERMITS: usize = 8;
/// Handle resolution timeout.
pub const HANDLE_TIMEOUT: Duration = Duration::from_secs(3);
/// Cap for `/.well-known/atproto-did` bodies.
pub const HANDLE_BODY_CAP: BodyCap = BodyCap(64 * 1024);

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
        .filter(|e| url::Url::parse(e).is_ok_and(|u| u.host_str().is_some()))
        .map(|e| e.trim_end_matches('/').to_string());
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

/// Resolves handles to DIDs.
#[async_trait]
pub trait HandleLookup: Send + Sync {
    async fn resolve_handle(&self, handle: &str) -> Option<String>;
}

/// proto-blue `HandleResolver` (DNS TXT and HTTPS), with the HTTPS path
/// carried by `net`.
struct ProtoBlueHandles(proto_blue::identity::HandleResolver);

#[async_trait]
impl HandleLookup for ProtoBlueHandles {
    async fn resolve_handle(&self, handle: &str) -> Option<String> {
        self.0.resolve(handle).await.ok().flatten()
    }
}

/// proto-blue fetch backend over the SSRF-guarded client.
struct NetFetchHandler(NetClient);

#[async_trait]
impl FetchHandler for NetFetchHandler {
    async fn fetch(&self, req: HttpRequest) -> Result<HttpResponse, FetchError> {
        if req.method != HttpMethod::Get {
            return Err(FetchError::Other("only GET is supported".to_string()));
        }
        let options = RequestOptions::new(HANDLE_BODY_CAP).timeout(HANDLE_TIMEOUT);
        let response = self
            .0
            .get(&req.url, &options)
            .await
            .map_err(|e| FetchError::Network(e.to_string()))?;
        let mut headers = BTreeMap::new();
        if let Some(ct) = response.content_type {
            headers.insert("content-type".to_string(), ct);
        }
        Ok(HttpResponse {
            status: response.status.as_u16(),
            headers,
            body: response.body,
        })
    }
}

struct Inner {
    net: NetClient,
    cache: DidCache,
    settings: IdentitySettings,
    policy: Arc<dyn PersistencePolicy>,
    handles: Arc<dyn HandleLookup>,
    refreshing: Mutex<HashSet<String>>,
    refresh_permits: Arc<Semaphore>,
}

/// The cached DID resolver. Cheap to clone.
#[derive(Clone)]
pub struct IdentityResolver {
    inner: Arc<Inner>,
}

impl IdentityResolver {
    #[must_use]
    pub fn new(
        net: NetClient,
        pool: PgPool,
        settings: IdentitySettings,
        policy: Arc<dyn PersistencePolicy>,
    ) -> Self {
        let handles = Arc::new(ProtoBlueHandles(
            proto_blue::identity::HandleResolver::with_fetch_handler(
                u64::try_from(HANDLE_TIMEOUT.as_millis()).unwrap_or(u64::MAX),
                Arc::new(NetFetchHandler(net.clone())),
            ),
        ));
        Self::with_handles(net, pool, settings, policy, handles)
    }

    /// Construct with a specific handle backend.
    #[must_use]
    pub fn with_handles(
        net: NetClient,
        pool: PgPool,
        settings: IdentitySettings,
        policy: Arc<dyn PersistencePolicy>,
        handles: Arc<dyn HandleLookup>,
    ) -> Self {
        let cache = DidCache::new(pool, settings.clone());
        Self {
            inner: Arc::new(Inner {
                net,
                cache,
                settings,
                policy,
                handles,
                refreshing: Mutex::new(HashSet::new()),
                refresh_permits: Arc::new(Semaphore::new(BACKGROUND_REFRESH_PERMITS)),
            }),
        }
    }

    #[must_use]
    pub fn cache(&self) -> &DidCache {
        &self.inner.cache
    }

    #[must_use]
    pub fn net(&self) -> &NetClient {
        &self.inner.net
    }

    /// Fetch `did`'s document from the network, bypassing every cache.
    ///
    /// # Errors
    ///
    /// Invalid DIDs, network failures, `NotFound` (404), `Tombstoned` (PLC
    /// 410), `Gone` (did:web 410), other non-2xx statuses as `Upstream`, and
    /// documents that are not JSON or describe another DID.
    pub async fn fetch_document(&self, did: &str) -> Result<FetchedDoc, IdentityError> {
        let target = parse_did(did)?;
        let url = document_url(did, &target, &self.inner.settings.plc_url);
        let response = self
            .inner
            .net
            .get(&url, &RequestOptions::new(BodyCap::DID_DOCUMENT))
            .await?;
        match response.status {
            s if s.is_success() => {}
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
        self.resolve_admitted(did, intent, &|_| true).await
    }

    /// [`IdentityResolver::resolve`] for a request: `admit` is asked once
    /// with the DID, just before a synchronous network fetch, and a `false`
    /// ends the resolution without one. A cached entry costs nothing.
    ///
    /// # Errors
    ///
    /// As [`IdentityResolver::resolve`], plus [`IdentityError::NoBudget`].
    pub async fn resolve_admitted(
        &self,
        did: &str,
        intent: ResolveIntent,
        admit: &(dyn Fn(&str) -> bool + Sync),
    ) -> Result<Arc<Value>, IdentityError> {
        parse_did(did)?;
        match self.inner.cache.lookup(did).await {
            Ok(Some((entry, Freshness::Fresh))) => return Ok(entry.doc),
            Ok(Some((entry, Freshness::Stale))) => {
                self.spawn_background_refresh(did, intent);
                return Ok(entry.doc);
            }
            Ok(Some((_, Freshness::Expired)) | None) => {}
            Err(error) => {
                tracing::warn!(event = "did_cache_read_failed", did, %error, "did_cache read failed; fetching");
            }
        }
        if !admit(did) {
            return Err(IdentityError::NoBudget(did.to_string()));
        }
        Ok(self.refresh_now(did, intent).await?.doc)
    }

    /// [`IdentityResolver::resolve_admitted`] plus extraction.
    ///
    /// # Errors
    ///
    /// As [`IdentityResolver::resolve_admitted`].
    pub async fn resolve_info_admitted(
        &self,
        did: &str,
        intent: ResolveIntent,
        admit: &(dyn Fn(&str) -> bool + Sync),
    ) -> Result<DidDocInfo, IdentityError> {
        let doc = self.resolve_admitted(did, intent, admit).await?;
        Ok(extract_info(&doc, did))
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
        self.resolve_handle_admitted(handle, &|_| true).await
    }

    /// [`IdentityResolver::resolve_handle`] for a request; `admit` as in
    /// [`IdentityResolver::resolve_admitted`].
    pub async fn resolve_handle_admitted(&self, handle: &str, admit: &(dyn Fn(&str) -> bool + Sync)) -> Option<String> {
        let handle = handle.trim().trim_start_matches('@').to_ascii_lowercase();
        let did = self.inner.handles.resolve_handle(&handle).await?;
        let info = self.resolve_info_admitted(&did, ResolveIntent::Lookup, admit).await.ok()?;
        verify_bidirectional(&did, info.claimed_handle.as_deref(), Some(&did)).filter(|h| h.eq_ignore_ascii_case(&handle))?;
        Some(did)
    }

    /// The DID of `handle` with the reason kept when there is none
    /// (design-v3 §5.11): the TXT record and the well-known file are both
    /// asked, a DID found is verified against its document, and "no such
    /// handle" is told apart from "could not find out". `admit` as in
    /// [`IdentityResolver::resolve_admitted`].
    pub async fn resolve_handle_typed(
        &self,
        txt: &dyn super::handle_lookup::TxtSource,
        handle: &str,
        admit: &(dyn Fn(&str) -> bool + Sync),
    ) -> super::handle_lookup::HandleOutcome {
        use super::handle_lookup::{HandleOutcome, HandleProbe, Step, candidate, neither_found};

        let probe = HandleProbe { txt, net: &self.inner.net };
        let (from_txt, from_file) = probe.run(handle).await;
        let Some(did) = candidate(&from_txt, &from_file) else {
            return match neither_found(&from_txt, &from_file) {
                Step::Confirmed => HandleOutcome::NotFound,
                _ => HandleOutcome::Unavailable,
            };
        };
        // The document must name the handle back. A document that could
        // not be fetched decides nothing.
        match self.resolve_info_admitted(did, ResolveIntent::Lookup, admit).await {
            Ok(info) if info.claimed_handle.as_deref().is_some_and(|claimed| claimed.eq_ignore_ascii_case(handle)) => {
                HandleOutcome::Found(did.to_string())
            }
            Ok(_) => HandleOutcome::NotFound,
            Err(_) => HandleOutcome::Unavailable,
        }
    }

    /// The verified handle for `did`, or `None`.
    pub async fn verify_handle(&self, did: &str, claimed: Option<&str>) -> Option<String> {
        let claimed = claimed?;
        let resolved = self.inner.handles.resolve_handle(claimed).await;
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
        let this = self.clone();
        let did = did.to_string();
        tokio::spawn(async move {
            if let Err(error) = this.refresh_now(&did, intent).await {
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
