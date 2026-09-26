//! Client JWT verification (design-v2 §6.7, §6.8): `verify_request_jwt`, the
//! only caller of `kryphocron::verification::verify_jwt`.
//!
//! - **Resolver.** `DefaultDidResolver<GuardedDidFetcher>` behind an
//!   `ArcSwap`; each request loads one `Arc` and uses it for the first
//!   attempt, the invalidate and the retry.
//! - **Issuer tracker.** kryphocron's resolver cache is unbounded, so a
//!   `moka::sync` LRU of issuer DIDs (capacity
//!   `identity.jwt_issuer_tracker_max`, TTL 2 h) is inserted immediately
//!   before and immediately after each `verify_jwt` call. Evictions
//!   (`Size`, `Expired`, `Explicit`) are queued to a drainer that invalidates
//!   the DID in the current resolver; a full queue sets a flag and the drainer
//!   swaps in a fresh resolver (`jwt_resolver_resets_total`).
//! - **Retry after key rotation.** On `SignatureInvalid` or
//!   `IssuerKeyNotInDocument`, refresh the issuer through the fetch cell,
//!   always invalidate kryphocron's copy, and retry once within the same 3 s
//!   deadline.
//! - **Host checks.** `aud` fragment absent or `bsky_appview`; `lxm` present
//!   and equal to the called method.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use arc_swap::ArcSwap;
use async_trait::async_trait;
use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use kryphocron::resolver::{DefaultDidResolver, DidResolver, HttpDidFetcher};
use kryphocron::verification::{JwtVerificationConfig, JwtVerificationError, VerifiedJwt, verify_jwt};
use kryphocron::{Did, ServiceIdentity, TraceId};
use moka::notification::RemovalCause;
use moka::policy::EvictionPolicy;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

use crate::identity::GuardedDidFetcher;
use crate::identity::fetch_cell::FetchOutcome;

/// Issuer tracker entry lifetime (§6.7).
pub const TRACKER_TTL: Duration = Duration::from_secs(2 * 3600);
/// Eviction queue capacity (§6.7).
pub const EVICTION_QUEUE: usize = 4096;
/// Per-request verification deadline, the §8.5 per-call total (§6.7).
pub const AUTH_DEADLINE: Duration = Duration::from_secs(3);
/// The only service `did.json` publishes (§6.8).
pub const APPVIEW_FRAGMENT: &str = "bsky_appview";

pub const METRIC_RESETS: &str = "jwt_resolver_resets_total";
pub const METRIC_AUTH: &str = "xrpc_auth_total";

/// An authentication failure (§6.8): `401 AuthRequired` or `401 InvalidToken`.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum AuthError {
    #[error("authentication required")]
    Required,
    #[error("invalid token: {0}")]
    Invalid(String),
}

/// The authenticated caller, with the verified token that the private
/// pipeline passes to `ingress::from_xrpc_request` (§6.3 step 6).
#[derive(Debug, Clone)]
pub struct VerifiedCaller {
    pub did: String,
    pub jwt: VerifiedJwt,
}

impl PartialEq for VerifiedCaller {
    fn eq(&self, other: &Self) -> bool {
        self.did == other.did
    }
}

/// The fetcher side of the §6.7 retry: refresh an issuer through the cell.
#[async_trait]
pub trait IssuerRefresh: HttpDidFetcher + Clone + 'static {
    /// `Err` when the cell holds (or just recorded) a failed fetch.
    async fn refresh_issuer(&self, did: &str) -> Result<(), String>;
}

#[async_trait]
impl IssuerRefresh for GuardedDidFetcher {
    async fn refresh_issuer(&self, did: &str) -> Result<(), String> {
        match self.refresh(did).await.as_ref() {
            FetchOutcome::Fetched(_) => Ok(()),
            FetchOutcome::Failed(msg) => Err(msg.clone()),
        }
    }
}

/// The unverified `iss` of a token, as a cache key only. `None` when absent
/// or not a DID: `verify_jwt` then rejects before resolving anything.
fn unverified_iss(token: &str) -> Option<String> {
    let token = token.strip_prefix("Bearer ").unwrap_or(token).trim();
    let payload = token.split('.').nth(1)?;
    let bytes = URL_SAFE_NO_PAD.decode(payload).ok()?;
    let value: serde_json::Value = serde_json::from_slice(&bytes).ok()?;
    let iss = value.get("iss")?.as_str()?;
    Did::new(iss).ok()?;
    Some(iss.to_string())
}

fn trace_id() -> TraceId {
    TraceId::from_bytes(rand::random())
}

/// The verifier. Cheap to clone.
pub struct JwtVerifier<F: IssuerRefresh> {
    identity: ServiceIdentity,
    config: JwtVerificationConfig,
    fetcher: F,
    resolver: Arc<ArcSwap<DefaultDidResolver<F>>>,
    tracker: moka::sync::Cache<String, ()>,
    reset_needed: Arc<AtomicBool>,
}

impl<F: IssuerRefresh> Clone for JwtVerifier<F> {
    fn clone(&self) -> Self {
        Self {
            identity: self.identity.clone(),
            config: self.config.clone(),
            fetcher: self.fetcher.clone(),
            resolver: Arc::clone(&self.resolver),
            tracker: self.tracker.clone(),
            reset_needed: Arc::clone(&self.reset_needed),
        }
    }
}

/// The eviction drainer (§6.7).
pub struct IssuerDrainer<F: IssuerRefresh> {
    rx: mpsc::Receiver<String>,
    fetcher: F,
    resolver: Arc<ArcSwap<DefaultDidResolver<F>>>,
    reset_needed: Arc<AtomicBool>,
}

impl<F: IssuerRefresh> JwtVerifier<F> {
    /// Build the verifier and its drainer. `tracker_capacity` is
    /// `identity.jwt_issuer_tracker_max`.
    #[must_use]
    pub fn new(
        identity: ServiceIdentity,
        fetcher: F,
        tracker_capacity: u64,
        queue_capacity: usize,
    ) -> (Self, IssuerDrainer<F>) {
        let (tx, rx) = mpsc::channel::<String>(queue_capacity.max(1));
        let reset_needed = Arc::new(AtomicBool::new(false));
        let flag = Arc::clone(&reset_needed);
        let tracker = moka::sync::Cache::builder()
            .max_capacity(tracker_capacity)
            .time_to_live(TRACKER_TTL)
            .eviction_policy(EvictionPolicy::lru())
            .eviction_listener(move |did: Arc<String>, (), cause: RemovalCause| {
                if matches!(cause, RemovalCause::Size | RemovalCause::Expired | RemovalCause::Explicit)
                    && tx.try_send(did.as_ref().clone()).is_err()
                {
                    flag.store(true, Ordering::SeqCst);
                }
            })
            .build();
        let resolver = Arc::new(ArcSwap::from_pointee(DefaultDidResolver::new(fetcher.clone())));
        let verifier = Self {
            identity,
            config: JwtVerificationConfig::atproto_service_auth(),
            fetcher: fetcher.clone(),
            resolver: Arc::clone(&resolver),
            tracker,
            reset_needed: Arc::clone(&reset_needed),
        };
        let drainer = IssuerDrainer {
            rx,
            fetcher,
            resolver,
            reset_needed,
        };
        (verifier, drainer)
    }

    /// Apply the tracker's pending evictions (tests).
    #[cfg(test)]
    pub fn run_pending_tasks(&self) {
        self.tracker.run_pending_tasks();
    }

    async fn attempt(
        &self,
        token: &str,
        resolver: &DefaultDidResolver<F>,
        iss: Option<&str>,
        deadline: Instant,
        trace: TraceId,
    ) -> Result<kryphocron::verification::VerifiedJwt, JwtVerificationError> {
        if let Some(iss) = iss {
            self.tracker.insert(iss.to_string(), ());
        }
        let result = verify_jwt(token, &self.identity, resolver, &self.config, deadline, trace).await;
        // Synchronous, with no `.await` since `verify_jwt` returned (§6.7).
        if let Some(iss) = iss {
            self.tracker.insert(iss.to_string(), ());
        }
        result
    }

    /// `verify_request_jwt` (§6.7, §6.8). `authorization` is the raw header
    /// value, if any; `method` is the called XRPC method NSID.
    ///
    /// # Errors
    ///
    /// [`AuthError::Required`] without credentials, [`AuthError::Invalid`]
    /// for anything else after the one rotation retry.
    pub async fn verify_request_jwt(&self, authorization: Option<&str>, method: &str) -> Result<VerifiedCaller, AuthError> {
        let Some(raw) = authorization.map(str::trim).filter(|s| !s.is_empty()) else {
            return Err(AuthError::Required);
        };
        let token = raw.strip_prefix("Bearer ").unwrap_or(raw).trim();
        let iss = unverified_iss(token);
        let resolver = self.resolver.load_full();
        let deadline = Instant::now() + AUTH_DEADLINE;
        let trace = trace_id();
        let first = self.attempt(token, &resolver, iss.as_deref(), deadline, trace).await;
        let verified = match (first, iss.as_deref()) {
            (Ok(v), _) => v,
            (Err(JwtVerificationError::SignatureInvalid | JwtVerificationError::IssuerKeyNotInDocument), Some(iss)) => {
                // Retry after a possible key rotation.
                if let Err(msg) = self.fetcher.refresh_issuer(iss).await {
                    return Err(invalid(&format!("issuer refresh failed: {msg}")));
                }
                if let Ok(did) = Did::new(iss) {
                    resolver.invalidate(&did, trace).await;
                }
                self.attempt(token, &resolver, Some(iss), deadline, trace)
                    .await
                    .map_err(|e| invalid(&e.to_string()))?
            }
            (Err(e), _) => return Err(invalid(&e.to_string())),
        };
        match verified.audience_fragment() {
            None => {}
            Some(APPVIEW_FRAGMENT) => {}
            Some(other) => return Err(invalid(&format!("aud fragment #{other} is not served here"))),
        }
        match verified.lxm() {
            Some(lxm) if lxm == method => {}
            Some(lxm) => return Err(invalid(&format!("lxm {lxm} does not match {method}"))),
            None => return Err(invalid("missing lxm")),
        }
        metrics::counter!(METRIC_AUTH, "result" => "ok").increment(1);
        Ok(VerifiedCaller {
            did: verified.issuer().as_str().to_string(),
            jwt: verified,
        })
    }
}

fn invalid(reason: &str) -> AuthError {
    metrics::counter!(METRIC_AUTH, "result" => "invalid").increment(1);
    AuthError::Invalid(reason.to_string())
}

impl<F: IssuerRefresh> IssuerDrainer<F> {
    fn reset_if_needed(&self) {
        if self.reset_needed.swap(false, Ordering::SeqCst) {
            self.resolver.store(Arc::new(DefaultDidResolver::new(self.fetcher.clone())));
            metrics::counter!(METRIC_RESETS).increment(1);
            tracing::warn!(event = "jwt_resolver_reset", "issuer eviction queue overflowed; resolver replaced");
        }
    }

    async fn handle(&self, did: &str) {
        self.reset_if_needed();
        let resolver = self.resolver.load_full();
        if let Ok(did) = Did::new(did) {
            resolver.invalidate(&did, trace_id()).await;
        }
    }

    /// Handle everything queued now (tests), then check the reset flag.
    #[cfg(test)]
    pub async fn drain_now(&mut self) -> usize {
        let mut n = 0;
        while let Ok(did) = self.rx.try_recv() {
            self.handle(&did).await;
            n += 1;
        }
        self.reset_if_needed();
        n
    }

    #[must_use]
    pub fn spawn(mut self) -> JoinHandle<()> {
        tokio::spawn(async move {
            while let Some(did) = self.rx.recv().await {
                self.handle(&did).await;
            }
        })
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use std::sync::Mutex;
    use std::sync::atomic::AtomicUsize;

    use k256::ecdsa::{Signature, SigningKey, signature::Signer};
    use kryphocron::resolver::{ContentType, DidResolutionError, RawDidDoc};
    use serde_json::json;

    use super::*;
    use crate::kryphocron_glue::service_key::{ServiceKey, multikey};

    pub(crate) const AUDIENCE_HOST: &str = "appview.example.com";

    /// A counting stub `HttpDidFetcher` serving one key per DID from a map.
    #[derive(Clone, Default)]
    pub(crate) struct StubFetcher {
        pub keys: Arc<Mutex<std::collections::HashMap<String, SigningKey>>>,
        pub calls: Arc<AtomicUsize>,
    }

    impl StubFetcher {
        pub fn set_key(&self, did: &str, key: SigningKey) {
            self.keys.lock().expect("lock").insert(did.to_string(), key);
        }

        fn doc(&self, did: &str) -> Result<RawDidDoc, DidResolutionError> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            let keys = self.keys.lock().expect("lock");
            let key = keys.get(did).ok_or(DidResolutionError::NotFound)?;
            let compressed = key.verifying_key().to_encoded_point(true).as_bytes().to_vec();
            let doc = json!({
                "id": did,
                "verificationMethod": [{
                    "id": format!("{did}#atproto"),
                    "type": "Multikey",
                    "controller": did,
                    "publicKeyMultibase": multikey(&compressed),
                }]
            });
            Ok(RawDidDoc {
                bytes: serde_json::to_vec(&doc).expect("json"),
                content_type: ContentType::ApplicationJson,
            })
        }
    }

    #[async_trait]
    impl HttpDidFetcher for StubFetcher {
        async fn fetch_plc(&self, did: &Did, _deadline: Instant) -> Result<RawDidDoc, DidResolutionError> {
            self.doc(did.as_str())
        }
        async fn fetch_web(&self, did: &Did, _deadline: Instant) -> Result<RawDidDoc, DidResolutionError> {
            self.doc(did.as_str())
        }
    }

    #[async_trait]
    impl IssuerRefresh for StubFetcher {
        async fn refresh_issuer(&self, _did: &str) -> Result<(), String> {
            Ok(())
        }
    }

    pub(crate) fn signing_key(seed: u8) -> SigningKey {
        let mut bytes = [seed; 32];
        bytes[0] = 1;
        SigningKey::from_slice(&bytes).expect("key")
    }

    /// Mint an ES256K service JWT.
    pub(crate) fn mint(key: &SigningKey, iss: &str, aud: &str, lxm: Option<&str>) -> String {
        let now = chrono::Utc::now().timestamp();
        let header = URL_SAFE_NO_PAD.encode(br#"{"alg":"ES256K","typ":"JWT"}"#);
        let mut claims = json!({"iss": iss, "aud": aud, "iat": now, "exp": now + 60});
        if let Some(lxm) = lxm {
            claims["lxm"] = json!(lxm);
        }
        let payload = URL_SAFE_NO_PAD.encode(serde_json::to_vec(&claims).expect("json"));
        let signing_input = format!("{header}.{payload}");
        let sig: Signature = key.sign(signing_input.as_bytes());
        let sig = sig.normalize_s().unwrap_or(sig);
        format!("{signing_input}.{}", URL_SAFE_NO_PAD.encode(sig.to_bytes()))
    }

    pub(crate) fn verifier(fetcher: StubFetcher, capacity: u64, queue: usize) -> (JwtVerifier<StubFetcher>, IssuerDrainer<StubFetcher>) {
        let identity = ServiceKey::new(AUDIENCE_HOST, &[7u8; 32])
            .expect("key")
            .service_identity()
            .expect("identity");
        JwtVerifier::new(identity, fetcher, capacity, queue)
    }

    const METHOD: &str = "app.bsky.feed.getTimeline";

    fn aud() -> String {
        format!("did:web:{AUDIENCE_HOST}")
    }

    #[tokio::test]
    async fn accepts_valid_tokens_and_applies_host_checks() {
        let fetcher = StubFetcher::default();
        let did = "did:plc:aaaaaaaaaaaaaaaaaaaaaaaa";
        let key = signing_key(3);
        fetcher.set_key(did, key.clone());
        let (v, _drainer) = verifier(fetcher, 100, 16);

        let ok = mint(&key, did, &aud(), Some(METHOD));
        assert_eq!(
            v.verify_request_jwt(Some(&format!("Bearer {ok}")), METHOD).await.map(|c| c.did),
            Ok(did.to_string())
        );
        let with_fragment = mint(&key, did, &format!("{}#bsky_appview", aud()), Some(METHOD));
        assert!(v.verify_request_jwt(Some(&with_fragment), METHOD).await.is_ok());

        assert_eq!(v.verify_request_jwt(None, METHOD).await, Err(AuthError::Required));
        let wrong_method = mint(&key, did, &aud(), Some("app.bsky.feed.getPosts"));
        assert!(matches!(v.verify_request_jwt(Some(&wrong_method), METHOD).await, Err(AuthError::Invalid(_))));
        let no_lxm = mint(&key, did, &aud(), None);
        assert!(matches!(v.verify_request_jwt(Some(&no_lxm), METHOD).await, Err(AuthError::Invalid(_))));
        let other_fragment = mint(&key, did, &format!("{}#atproto_labeler", aud()), Some(METHOD));
        assert!(matches!(v.verify_request_jwt(Some(&other_fragment), METHOD).await, Err(AuthError::Invalid(_))));
        let other_aud = mint(&key, did, "did:web:elsewhere.example", Some(METHOD));
        assert!(matches!(v.verify_request_jwt(Some(&other_aud), METHOD).await, Err(AuthError::Invalid(_))));
        let forged = mint(&signing_key(9), did, &aud(), Some(METHOD));
        assert!(matches!(v.verify_request_jwt(Some(&forged), METHOD).await, Err(AuthError::Invalid(_))));
    }

    #[tokio::test]
    async fn retries_once_after_key_rotation() {
        let fetcher = StubFetcher::default();
        let did = "did:plc:bbbbbbbbbbbbbbbbbbbbbbbb";
        fetcher.set_key(did, signing_key(4));
        let (v, _drainer) = verifier(fetcher.clone(), 100, 16);
        // Prime kryphocron's cache with the old key.
        let old = mint(&signing_key(4), did, &aud(), Some(METHOD));
        assert!(v.verify_request_jwt(Some(&old), METHOD).await.is_ok());
        // The issuer rotates; a token signed with the new key verifies after
        // one invalidate-and-retry.
        fetcher.set_key(did, signing_key(5));
        let new = mint(&signing_key(5), did, &aud(), Some(METHOD));
        let before = fetcher.calls.load(Ordering::SeqCst);
        assert!(v.verify_request_jwt(Some(&new), METHOD).await.is_ok());
        assert_eq!(fetcher.calls.load(Ordering::SeqCst), before + 1);
    }

    /// §6.7 tracker test: capacity 10, a counting stub replacing
    /// `GuardedDidFetcher`.
    #[tokio::test]
    async fn tracker_evictions_invalidate_and_overflow_resets() {
        let fetcher = StubFetcher::default();
        let dids: Vec<String> = (0..30).map(|i| format!("did:plc:{i:0>24}")).collect();
        for d in &dids {
            fetcher.set_key(d, signing_key(6));
        }
        let (v, mut drainer) = verifier(fetcher.clone(), 10, 64);
        let resolve_all = |range: std::ops::Range<usize>| {
            let v = v.clone();
            let dids = dids.clone();
            async move {
                for d in &dids[range] {
                    let t = mint(&signing_key(6), d, &aud(), Some(METHOD));
                    assert!(v.verify_request_jwt(Some(&t), METHOD).await.is_ok());
                    v.run_pending_tasks();
                }
            }
        };
        resolve_all(0..30).await;
        assert_eq!(fetcher.calls.load(Ordering::SeqCst), 30);
        v.run_pending_tasks();
        assert!(drainer.drain_now().await >= 20);
        // The first 20 were evicted and invalidated: each is fetched again.
        resolve_all(0..20).await;
        assert_eq!(fetcher.calls.load(Ordering::SeqCst), 50);
        // A DID still tracked is served from kryphocron's cache.
        v.run_pending_tasks();
        drainer.drain_now().await;
        resolve_all(19..20).await;
        assert_eq!(fetcher.calls.load(Ordering::SeqCst), 50);

        // Queue capacity 1: overflow sets the flag and the drainer resets.
        let (v1, mut d1) = verifier(fetcher.clone(), 2, 1);
        for d in &dids[..6] {
            let t = mint(&signing_key(6), d, &aud(), Some(METHOD));
            assert!(v1.verify_request_jwt(Some(&t), METHOD).await.is_ok());
            v1.run_pending_tasks();
        }
        assert!(v1.reset_needed.load(Ordering::SeqCst));
        d1.drain_now().await;
        assert!(!v1.reset_needed.load(Ordering::SeqCst));
    }

    #[test]
    fn unverified_iss_is_a_cache_key_only() {
        let t = mint(&signing_key(3), "did:plc:aaaaaaaaaaaaaaaaaaaaaaaa", &aud(), Some(METHOD));
        assert_eq!(unverified_iss(&t).as_deref(), Some("did:plc:aaaaaaaaaaaaaaaaaaaaaaaa"));
        assert_eq!(unverified_iss("garbage"), None);
        let bad = mint(&signing_key(3), "not-a-did", &aud(), Some(METHOD));
        assert_eq!(unverified_iss(&bad), None);
    }
}
