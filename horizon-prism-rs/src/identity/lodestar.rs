//! Lodestar: a read-only mirror of the PLC directory that also verifies
//! every account's handle both ways (lodestar-reads design).
//!
//! - Set by `identity.lodestar_url`; empty is off, and nothing here is
//!   built.
//! - Public request paths ask it first: `GET /{did}` (the directory's
//!   document), `GET /account/{did}` (the verified handle),
//!   `GET /handle/{handle}` and `POST /accounts` (a batch). Any answer it
//!   cannot give falls back to the PLC directory and this app's own
//!   probes, decided by the caller from what these calls return.
//! - **Nothing that authorizes, is stored or changes state reads it** (§2.2):
//!   the key path, admin, membership, the refresher, backfill and
//!   `list_sync` use the base resolver, which has no lodestar.
//! - **The gate** (§2.6): lodestar is asked only while its last good report
//!   (`source.plc` fresh and live) is under [`GOOD_FOR`] old and nothing has
//!   closed it since. A probe every [`PROBE_EVERY`] and every account
//!   answer report; an answer that says lodestar is unreachable, refused or
//!   behind closes it. An answer about one item never does.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::{Duration, Instant};

use chrono::Utc;
use moka::future::Cache;
use moka::policy::EvictionPolicy;
use serde::Deserialize;
use serde_json::{Value, json};
use tokio::task::JoinHandle;

use super::cache::CachedDoc;
use super::resolver::{FetchedDoc, check_document_id, is_tombstone_answer};
use crate::net::{BodyCap, NetClient, NetError, NetResponse, RequestOptions};

/// How often the probe asks (§2.6).
pub const PROBE_EVERY: Duration = Duration::from_secs(15);
/// How long a good report keeps the gate open (§2.6).
pub const GOOD_FOR: Duration = Duration::from_secs(45);
/// A valid `did:plc` that no identity holds: lodestar answers it from its
/// own tables, with its position, and never resolves anything live.
pub const PROBE_DID: &str = "did:plc:aaaaaaaaaaaaaaaaaaaaaaaa";
/// DIDs per `POST /accounts` (lodestar's default limit).
pub const BATCH_MAX: usize = 1000;
/// Lodestar documents kept in process (§2.5).
pub const DOC_CACHE_CAPACITY: u64 = 100_000;

const ACCOUNT_CAP: BodyCap = BodyCap(64 * 1024);
const BATCH_CAP: BodyCap = BodyCap(8 * 1024 * 1024);
const HANDLE_CAP: BodyCap = BodyCap(16 * 1024);

/// `lodestar_calls_total{endpoint, outcome}`.
pub const METRIC_CALLS: &str = "lodestar_calls_total";
/// `lodestar_call_seconds{endpoint}`.
pub const METRIC_SECONDS: &str = "lodestar_call_seconds";
/// `lodestar_skipped_total{reason}`: `gate_closed`, `no_budget`.
pub const METRIC_SKIPPED: &str = "lodestar_skipped_total";
/// `lodestar_fresh`: 1 while the gate is open.
pub const METRIC_FRESH: &str = "lodestar_fresh";
/// `lodestar_batch_filled_total`: summaries a batch filled.
pub const METRIC_BATCH_FILLED: &str = "lodestar_batch_filled_total";

/// Which lodestar call.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Endpoint {
    Document,
    Account,
    Accounts,
    Handle,
    Probe,
}

impl Endpoint {
    #[must_use]
    pub fn label(self) -> &'static str {
        match self {
            Self::Document => "document",
            Self::Account => "account",
            Self::Accounts => "accounts",
            Self::Handle => "handle",
            Self::Probe => "probe",
        }
    }
}

/// How one call ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Outcome {
    /// An answer this app uses.
    Answered,
    /// 404 about the item: not registered, not found, not verified.
    NotFound,
    /// 404 `DID not available`: a tombstone.
    Tombstoned,
    /// Lodestar is not at the directory's tip: `source` not fresh and live,
    /// or 503 while it copies.
    Behind,
    /// 503 `unavailable`: a live resolution was inconclusive.
    Unavailable,
    /// 403: this app is not on the allowlist.
    Forbidden,
    /// 400: the request was refused (an item lodestar will not take, or a
    /// batch over its limit).
    Refused,
    /// Any other status.
    Status,
    Timeout,
    Transport,
    /// An answer that is not what the API documents.
    Malformed,
}

impl Outcome {
    #[must_use]
    pub fn label(self) -> &'static str {
        match self {
            Self::Answered => "answered",
            Self::NotFound => "not_found",
            Self::Tombstoned => "tombstoned",
            Self::Behind => "behind",
            Self::Unavailable => "unavailable",
            Self::Forbidden => "forbidden",
            Self::Refused | Self::Status => "status",
            Self::Timeout => "timeout",
            Self::Transport => "transport",
            Self::Malformed => "malformed",
        }
    }

    /// Whether this outcome says lodestar itself is unreachable, refused
    /// or behind (§2.3, §2.7): an answer about one item never closes the
    /// gate. A `/handle` call resolves live and is slow by nature, so its
    /// timeout and `unavailable` are about that handle.
    #[must_use]
    pub fn closes_gate(self, endpoint: Endpoint) -> bool {
        match self {
            Self::Answered | Self::NotFound | Self::Tombstoned | Self::Refused => false,
            Self::Timeout | Self::Unavailable => endpoint != Endpoint::Handle,
            Self::Behind | Self::Forbidden | Self::Status | Self::Transport | Self::Malformed => true,
        }
    }
}

#[derive(Debug, Default)]
struct GateState {
    open: bool,
    good_until: Option<Instant>,
}

/// Whether lodestar is asked at all (§2.6). Starts closed.
#[derive(Debug, Default)]
pub struct Gate {
    state: Mutex<GateState>,
}

impl Gate {
    /// Open now.
    #[must_use]
    pub fn is_open(&self) -> bool {
        self.is_open_at(Instant::now())
    }

    /// Open at `now`; a good report older than [`GOOD_FOR`] closes it.
    #[must_use]
    pub fn is_open_at(&self, now: Instant) -> bool {
        let mut s = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        if s.open && s.good_until.is_none_or(|until| now >= until) {
            s.open = false;
            drop(s);
            closed("stale_probe");
            return false;
        }
        s.open
    }

    /// A good report at `now`: opens the gate, or keeps it open.
    pub fn good_at(&self, now: Instant) {
        let opened = {
            let mut s = self.state.lock().unwrap_or_else(PoisonError::into_inner);
            s.good_until = Some(now + GOOD_FOR);
            let opened = !s.open;
            s.open = true;
            opened
        };
        if opened {
            metrics::gauge!(METRIC_FRESH).set(1.0);
            tracing::info!(event = "lodestar_gate_opened", "lodestar is at the directory's tip; identity reads ask it first");
        }
    }

    /// Close the gate until the next good report.
    pub fn close(&self, reason: &'static str) {
        let was_open = {
            let mut s = self.state.lock().unwrap_or_else(PoisonError::into_inner);
            std::mem::replace(&mut s.open, false)
        };
        if was_open {
            closed(reason);
        }
    }
}

fn closed(reason: &'static str) {
    metrics::gauge!(METRIC_FRESH).set(0.0);
    tracing::warn!(event = "lodestar_gate_closed", reason, "identity reads go to the PLC directory until lodestar reports again");
}

/// An account as lodestar reports it (its API's account shape, the fields
/// this app reads).
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct Account {
    /// `active`, `tombstoned` or `unresolvable`.
    pub status: String,
    #[serde(default)]
    pub pds: Option<String>,
    /// Present only when lodestar verified it both ways.
    #[serde(default)]
    pub handle: Option<String>,
    #[serde(default)]
    pub claimed_handle: Option<String>,
    #[serde(default)]
    pub handle_status: Option<String>,
}

/// What `GET /{did}` came to.
#[derive(Debug, Clone, PartialEq)]
pub enum DocAnswer {
    Doc(FetchedDoc),
    /// 404 `DID not available`.
    Tombstoned,
    /// Ask the PLC directory.
    Fallback,
}

/// Whether `body`'s `source.plc` says lodestar is at the directory's tip:
/// `Some(true)` fresh and live, `Some(false)` present but not, `None` absent.
fn current(body: &Value) -> Option<bool> {
    let plc = body.get("source")?.get("plc")?;
    let live = plc.get("phase").and_then(Value::as_str) == Some("live");
    let fresh = plc.get("fresh").and_then(Value::as_bool) == Some(true);
    Some(live && fresh)
}

/// The `error` word of an error answer.
fn error_word(body: &[u8]) -> Option<String> {
    serde_json::from_slice::<Value>(body).ok()?.get("error")?.as_str().map(str::to_string)
}

fn failure(error: &NetError) -> Outcome {
    match error {
        NetError::Timeout => Outcome::Timeout,
        NetError::BodyTooLarge { .. } | NetError::Decode(_) => Outcome::Malformed,
        _ => Outcome::Transport,
    }
}

/// A non-2xx status's outcome, outside the cases each call reads itself.
fn status_outcome(status: u16, body: &[u8]) -> Outcome {
    match status {
        400 => Outcome::Refused,
        403 => Outcome::Forbidden,
        404 => Outcome::NotFound,
        503 => match error_word(body).as_deref() {
            Some("syncing") => Outcome::Behind,
            Some("unavailable") => Outcome::Unavailable,
            _ => Outcome::Status,
        },
        _ => Outcome::Status,
    }
}

/// The lodestar client, its gate and its document cache. Built once by
/// `serve` when `identity.lodestar_url` is set.
pub struct Lodestar {
    base: String,
    net: NetClient,
    timeout: Duration,
    gate: Gate,
    docs: Cache<String, CachedDoc>,
    batch_max: usize,
}

impl std::fmt::Debug for Lodestar {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Lodestar").field("base", &self.base).finish_non_exhaustive()
    }
}

impl Lodestar {
    /// A client of `base` (no trailing `/`) through `net`, the public
    /// transport's client: lodestar is a fixed upstream, as the PLC
    /// directory is. Documents are kept `doc_ttl`.
    #[must_use]
    pub fn new(net: NetClient, base: &str, timeout: Duration, doc_ttl: Duration) -> Self {
        metrics::gauge!(METRIC_FRESH).set(0.0);
        Self {
            base: base.trim_end_matches('/').to_string(),
            net,
            timeout,
            gate: Gate::default(),
            docs: Cache::builder()
                .max_capacity(DOC_CACHE_CAPACITY)
                .time_to_live(doc_ttl)
                .eviction_policy(EvictionPolicy::lru())
                .build(),
            batch_max: BATCH_MAX,
        }
    }

    /// A smaller batch limit (tests).
    #[cfg(test)]
    #[must_use]
    pub fn with_batch_max(mut self, n: usize) -> Self {
        self.batch_max = n;
        self
    }

    /// The configured host, for the startup log.
    #[must_use]
    pub fn host(&self) -> String {
        url::Url::parse(&self.base).ok().and_then(|u| u.host_str().map(str::to_string)).unwrap_or_default()
    }

    #[must_use]
    pub fn gate(&self) -> &Gate {
        &self.gate
    }

    /// Lodestar's documents (§2.5), never `did_cache`.
    #[must_use]
    pub fn docs(&self) -> &Cache<String, CachedDoc> {
        &self.docs
    }

    /// DIDs per batch call.
    #[must_use]
    pub fn batch_max(&self) -> usize {
        self.batch_max
    }

    /// One call's limit: the configured time, never past `deadline`.
    /// `None` when the deadline has passed.
    fn limit(&self, deadline: Option<Instant>) -> Option<Duration> {
        match deadline {
            None => Some(self.timeout),
            Some(d) => {
                let left = d.saturating_duration_since(Instant::now());
                (!left.is_zero()).then(|| left.min(self.timeout))
            }
        }
    }

    /// Count a call, and close the gate when its outcome says so.
    fn record(&self, endpoint: Endpoint, outcome: Outcome, elapsed: Duration) {
        metrics::counter!(METRIC_CALLS, "endpoint" => endpoint.label(), "outcome" => outcome.label()).increment(1);
        metrics::histogram!(METRIC_SECONDS, "endpoint" => endpoint.label()).record(elapsed.as_secs_f64());
        if outcome.closes_gate(endpoint) {
            self.gate.close(outcome.label());
        }
        tracing::debug!(event = "lodestar_call", endpoint = endpoint.label(), outcome = outcome.label(), "lodestar call");
    }

    /// A report from an answer's `source`: good opens the gate.
    fn report(&self, current: bool) {
        if current {
            self.gate.good_at(Instant::now());
        }
    }

    async fn get(&self, path: &str, cap: BodyCap, limit: Duration) -> (Result<NetResponse, NetError>, Duration) {
        let started = Instant::now();
        let url = format!("{}/{path}", self.base);
        let answer = self.net.get(&url, &RequestOptions::new(cap).timeout(limit)).await;
        (answer, started.elapsed())
    }

    /// `GET /{did}` for a `did:plc` (§2.3).
    pub async fn document(&self, did: &str, deadline: Option<Instant>) -> DocAnswer {
        let Some(limit) = self.limit(deadline) else {
            return DocAnswer::Fallback;
        };
        let (answer, elapsed) = self.get(did, BodyCap::DID_DOCUMENT, limit).await;
        let (outcome, doc) = match answer {
            Err(error) => (failure(&error), DocAnswer::Fallback),
            Ok(r) if r.is_success() => match serde_json::from_slice::<Value>(&r.body) {
                Ok(doc) if check_document_id(&doc, did).is_ok() => (
                    Outcome::Answered,
                    DocAnswer::Doc(FetchedDoc {
                        raw: r.body,
                        content_type: r.content_type,
                        doc: Arc::new(doc),
                        fetched_at: Utc::now(),
                    }),
                ),
                _ => (Outcome::Malformed, DocAnswer::Fallback),
            },
            Ok(r) if r.status.as_u16() == 404 && is_tombstone_answer(&r.body) => (Outcome::Tombstoned, DocAnswer::Tombstoned),
            // The directory's error shape: 503 while lodestar copies it.
            Ok(r) if r.status.as_u16() == 503 => (Outcome::Behind, DocAnswer::Fallback),
            Ok(r) => (status_outcome(r.status.as_u16(), &r.body), DocAnswer::Fallback),
        };
        self.record(Endpoint::Document, outcome, elapsed);
        doc
    }

    /// An account answer, sorted: its outcome, the account, and whether its
    /// `source` said lodestar is at the tip (`None`: no `source`). A good
    /// `source` reports to the gate. Shared by `/account` and the probe.
    fn sort_account(&self, answer: Result<NetResponse, NetError>) -> (Outcome, Option<Account>, Option<bool>) {
        let r = match answer {
            Err(error) => return (failure(&error), None, None),
            Ok(r) => r,
        };
        let Ok(body) = serde_json::from_slice::<Value>(&r.body) else {
            let outcome = if r.is_success() { Outcome::Malformed } else { status_outcome(r.status.as_u16(), &r.body) };
            return (outcome, None, None);
        };
        let at_tip = current(&body);
        if let Some(at_tip) = at_tip {
            self.report(at_tip);
        }
        let (outcome, account) = match (r.status.as_u16(), at_tip) {
            (_, Some(false)) => (Outcome::Behind, None),
            (200..=299, Some(true)) => match serde_json::from_value::<Account>(body) {
                Ok(account) => (Outcome::Answered, Some(account)),
                Err(_) => (Outcome::Malformed, None),
            },
            (200..=299, None) => (Outcome::Malformed, None),
            (status, _) => (status_outcome(status, &r.body), None),
        };
        (outcome, account, at_tip)
    }

    /// `GET /account/{did}`: the account, only when lodestar is at the
    /// directory's tip (§2.7).
    pub async fn account(&self, did: &str, deadline: Option<Instant>) -> Option<Account> {
        let limit = self.limit(deadline)?;
        let (answer, elapsed) = self.get(&format!("account/{did}"), ACCOUNT_CAP, limit).await;
        let (outcome, account, _) = self.sort_account(answer);
        self.record(Endpoint::Account, outcome, elapsed);
        account
    }

    /// The probe (§2.6): ask about [`PROBE_DID`]; lodestar answers it with
    /// its position (`not_found` while at the tip). Anything but a fresh,
    /// live `source` closes the gate. Returns whether the gate is open.
    pub async fn probe(&self) -> bool {
        let (answer, elapsed) = self.get(&format!("account/{PROBE_DID}"), ACCOUNT_CAP, self.timeout).await;
        let (outcome, _, at_tip) = self.sort_account(answer);
        self.record(Endpoint::Probe, outcome, elapsed);
        if at_tip != Some(true) {
            self.gate.close(if outcome.closes_gate(Endpoint::Probe) { outcome.label() } else { "status" });
        }
        self.gate.is_open()
    }

    /// Probe every [`PROBE_EVERY`], from now on.
    #[must_use]
    pub fn spawn_probe(self: Arc<Self>) -> JoinHandle<()> {
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(PROBE_EVERY);
            tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            loop {
                tick.tick().await;
                self.probe().await;
            }
        })
    }

    /// `POST /accounts` for at most [`Lodestar::batch_max`] DIDs (§2.10):
    /// each DID's account or error word, or `None` when the call failed or
    /// lodestar was not at the tip.
    pub async fn accounts(&self, dids: &[String], deadline: Option<Instant>) -> Option<HashMap<String, Result<Account, String>>> {
        let limit = self.limit(deadline)?;
        let started = Instant::now();
        let url = format!("{}/accounts", self.base);
        let answer = self.net.post_json(&url, &json!({ "dids": dids }), &RequestOptions::new(BATCH_CAP).timeout(limit)).await;
        let elapsed = started.elapsed();
        let (outcome, found) = match answer {
            Err(error) => (failure(&error), None),
            Ok(r) if r.is_success() => match serde_json::from_slice::<Value>(&r.body) {
                Ok(body) => match current(&body) {
                    Some(true) => {
                        self.report(true);
                        let entries = body.get("accounts").and_then(Value::as_object).cloned().unwrap_or_default();
                        let found: HashMap<String, Result<Account, String>> = entries
                            .into_iter()
                            .map(|(did, v)| {
                                let entry = match v.get("error").and_then(Value::as_str) {
                                    Some(word) => Err(word.to_string()),
                                    None => serde_json::from_value::<Account>(v).map_err(|_| "malformed".to_string()),
                                };
                                (did, entry)
                            })
                            .collect();
                        (Outcome::Answered, Some(found))
                    }
                    Some(false) => (Outcome::Behind, None),
                    None => (Outcome::Malformed, None),
                },
                Err(_) => (Outcome::Malformed, None),
            },
            Ok(r) => {
                let outcome = status_outcome(r.status.as_u16(), &r.body);
                if outcome == Outcome::Refused {
                    tracing::warn!(event = "lodestar_batch_refused", dids = dids.len(), "lodestar refused a batch (a smaller batch limit?); those accounts are resolved one by one");
                }
                (outcome, None)
            }
        };
        self.record(Endpoint::Accounts, outcome, elapsed);
        found
    }

    /// `GET /handle/{handle}`: the DID lodestar holds the handle verified
    /// to, or `None` (§2.7).
    pub async fn handle(&self, handle: &str, deadline: Option<Instant>) -> Option<String> {
        let handle = handle.to_ascii_lowercase();
        if !proto_blue::syntax::Handle::is_valid(&handle) {
            return None;
        }
        let limit = self.limit(deadline)?;
        let (answer, elapsed) = self.get(&format!("handle/{handle}"), HANDLE_CAP, limit).await;
        let (outcome, did) = match answer {
            Err(error) => (failure(&error), None),
            Ok(r) if r.is_success() => {
                let did = serde_json::from_slice::<Value>(&r.body)
                    .ok()
                    .and_then(|v| v.get("did").and_then(Value::as_str).map(str::to_string))
                    .filter(|d| proto_blue::syntax::Did::is_valid(d));
                match did {
                    Some(did) => (Outcome::Answered, Some(did)),
                    None => (Outcome::Malformed, None),
                }
            }
            Ok(r) => (status_outcome(r.status.as_u16(), &r.body), None),
        };
        self.record(Endpoint::Handle, outcome, elapsed);
        did
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use axum::extract::{Path, State};
    use axum::http::StatusCode;
    use axum::routing::{get, post};

    use super::*;

    const DID: &str = "did:plc:bbbbbbbbbbbbbbbbbbbbbbbb";

    fn source(fresh: bool, phase: &str) -> Value {
        json!({"plc": {"phase": phase, "fresh": fresh, "cursor": 1}})
    }

    /// What the fake answers, by mode.
    #[derive(Clone, Default)]
    struct Fake {
        mode: Arc<Mutex<&'static str>>,
        calls: Arc<AtomicUsize>,
    }

    impl Fake {
        fn set(&self, mode: &'static str) {
            *self.mode.lock().expect("mode") = mode;
        }
        fn mode(&self) -> &'static str {
            self.calls.fetch_add(1, Ordering::SeqCst);
            *self.mode.lock().expect("mode")
        }
    }

    async fn account(State(f): State<Fake>, Path(did): Path<String>) -> (StatusCode, String) {
        match f.mode() {
            "live" if did == PROBE_DID => (StatusCode::NOT_FOUND, json!({"error": "not_found", "source": source(true, "live")}).to_string()),
            "live" => (
                StatusCode::OK,
                json!({"did": did, "status": "active", "pds": "https://pds.example.com", "handle": "alice.example.com",
                       "claimed_handle": "alice.example.com", "handle_status": "verified", "source": source(true, "live")})
                .to_string(),
            ),
            "stale" => (StatusCode::NOT_FOUND, json!({"error": "not_found", "source": source(false, "live")}).to_string()),
            "backfill" => (StatusCode::SERVICE_UNAVAILABLE, json!({"error": "syncing", "source": source(false, "backfill")}).to_string()),
            "forbidden" => (StatusCode::FORBIDDEN, String::new()),
            "slow" => {
                tokio::time::sleep(Duration::from_millis(400)).await;
                (StatusCode::NOT_FOUND, json!({"error": "not_found", "source": source(true, "live")}).to_string())
            }
            _ => (StatusCode::INTERNAL_SERVER_ERROR, String::new()),
        }
    }

    async fn document(State(f): State<Fake>, Path(did): Path<String>) -> (StatusCode, String) {
        match f.mode() {
            "tombstone" => (StatusCode::NOT_FOUND, json!({"message": format!("DID not available: {did}")}).to_string()),
            "unregistered" => (StatusCode::NOT_FOUND, json!({"message": format!("DID not registered: {did}")}).to_string()),
            "copying" => (StatusCode::SERVICE_UNAVAILABLE, json!({"message": "lodestar is still copying the directory"}).to_string()),
            "other_id" => (StatusCode::OK, json!({"id": "did:plc:cccccccccccccccccccccccc"}).to_string()),
            "forbidden" => (StatusCode::FORBIDDEN, String::new()),
            _ => (StatusCode::OK, json!({"id": did, "alsoKnownAs": ["at://alice.example.com"]}).to_string()),
        }
    }

    async fn handle(State(f): State<Fake>, Path(handle): Path<String>) -> (StatusCode, String) {
        match f.mode() {
            "unavailable" => (StatusCode::SERVICE_UNAVAILABLE, json!({"error": "unavailable"}).to_string()),
            "syncing" => (StatusCode::SERVICE_UNAVAILABLE, json!({"error": "syncing"}).to_string()),
            "missing" => (StatusCode::NOT_FOUND, json!({"error": "not_found"}).to_string()),
            "slow" => {
                tokio::time::sleep(Duration::from_millis(400)).await;
                (StatusCode::OK, json!({"handle": handle, "did": DID}).to_string())
            }
            _ => (StatusCode::OK, json!({"handle": handle, "did": DID}).to_string()),
        }
    }

    async fn accounts(State(f): State<Fake>, axum::Json(body): axum::Json<Value>) -> (StatusCode, String) {
        let dids: Vec<String> = serde_json::from_value(body["dids"].clone()).expect("dids");
        match f.mode() {
            "too_large" => (StatusCode::BAD_REQUEST, json!({"error": "batch_too_large"}).to_string()),
            "stale" => (StatusCode::OK, json!({"accounts": {}, "source": source(false, "live")}).to_string()),
            _ => {
                let mut out = serde_json::Map::new();
                for did in dids {
                    let entry = if did == DID {
                        json!({"did": did, "status": "active", "pds": "https://pds.example.com", "handle_status": "none"})
                    } else {
                        json!({"error": "not_found"})
                    };
                    out.insert(did, entry);
                }
                (StatusCode::OK, json!({"accounts": out, "source": source(true, "live")}).to_string())
            }
        }
    }

    async fn fake(f: &Fake) -> String {
        let router = axum::Router::new()
            .route("/account/{did}", get(account))
            .route("/accounts", post(accounts))
            .route("/handle/{handle}", get(handle))
            .route("/{did}", get(document))
            .with_state(f.clone());
        crate::test_support::serve_fake(router).await
    }

    fn client(base: &str) -> Lodestar {
        Lodestar::new(NetClient::loopback_for_tests(), base, Duration::from_millis(200), Duration::from_secs(3600))
    }

    /// Lodestar-reads T-L6: the gate starts closed; a probe answered with a
    /// fresh, live `source` opens it; a stale or backfilling `source`, a
    /// 403, a 5xx and a timeout close it; a good report expires.
    #[tokio::test]
    async fn the_gate_follows_the_probe() {
        let f = Fake::default();
        let l = client(&fake(&f).await);
        assert!(!l.gate().is_open(), "closed until the first probe");
        for (mode, open) in [
            ("live", true),
            ("stale", false),
            ("live", true),
            ("backfill", false),
            ("live", true),
            ("forbidden", false),
            ("live", true),
            ("broken", false),
            ("live", true),
            ("slow", false),
        ] {
            f.set(mode);
            assert_eq!(l.probe().await, open, "{mode}");
            assert_eq!(l.gate().is_open(), open, "{mode}");
        }
        f.set("live");
        assert!(l.probe().await);
        let now = Instant::now();
        assert!(l.gate().is_open_at(now + GOOD_FOR - Duration::from_secs(1)));
        assert!(!l.gate().is_open_at(now + GOOD_FOR + Duration::from_secs(1)), "a good report expires");
        assert!(!l.gate().is_open(), "and stays closed until the next one");
    }

    /// Lodestar-reads T-L3 at the client: each `/{did}` answer's outcome
    /// and its effect on the gate.
    #[tokio::test]
    async fn document_answers_are_sorted() {
        let f = Fake::default();
        let l = client(&fake(&f).await);
        let open = |l: &Lodestar| l.gate().good_at(Instant::now());
        for (mode, want, gate_open) in [
            ("doc", "doc", true),
            ("tombstone", "tombstoned", true),
            ("unregistered", "fallback", true),
            ("other_id", "fallback", false),
            ("copying", "fallback", false),
            ("forbidden", "fallback", false),
        ] {
            open(&l);
            f.set(mode);
            let got = match l.document(DID, None).await {
                DocAnswer::Doc(d) => {
                    assert_eq!(d.doc["id"], DID);
                    "doc"
                }
                DocAnswer::Tombstoned => "tombstoned",
                DocAnswer::Fallback => "fallback",
            };
            assert_eq!((got, l.gate().is_open()), (want, gate_open), "{mode}");
        }
    }

    /// Lodestar-reads T-L7 at the client: `/handle`'s per-item answers
    /// (404, 503 `unavailable`, a timeout) leave the gate open; 503
    /// `syncing` closes it.
    #[tokio::test]
    async fn handle_answers_close_the_gate_only_when_lodestar_is_behind() {
        let f = Fake::default();
        let l = client(&fake(&f).await);
        l.gate().good_at(Instant::now());
        f.set("ok");
        assert_eq!(l.handle("Alice.Example.com", None).await.as_deref(), Some(DID));
        for mode in ["missing", "unavailable", "slow"] {
            f.set(mode);
            assert_eq!(l.handle("alice.example.com", None).await, None, "{mode}");
            assert!(l.gate().is_open(), "{mode} is about one handle");
        }
        f.set("syncing");
        assert_eq!(l.handle("alice.example.com", None).await, None);
        assert!(!l.gate().is_open(), "syncing: lodestar is behind");
        // An invalid handle is never sent.
        let before = f.calls.load(Ordering::SeqCst);
        assert_eq!(l.handle("not a handle", None).await, None);
        assert_eq!(f.calls.load(Ordering::SeqCst), before);
    }

    /// `/account` is used only from a lodestar at the tip.
    #[tokio::test]
    async fn an_account_is_read_only_at_the_tip() {
        let f = Fake::default();
        let l = client(&fake(&f).await);
        f.set("live");
        let a = l.account(DID, None).await.expect("account");
        assert_eq!((a.handle.as_deref(), a.status.as_str()), (Some("alice.example.com"), "active"));
        assert!(l.gate().is_open(), "a fresh account answer reports");
        f.set("stale");
        assert_eq!(l.account(DID, None).await, None);
        assert!(!l.gate().is_open());
        // Past the deadline nothing is asked.
        let before = f.calls.load(Ordering::SeqCst);
        assert_eq!(l.account(DID, Some(Instant::now())).await, None);
        assert_eq!(f.calls.load(Ordering::SeqCst), before);
    }

    /// The batch: entries by DID; a stale batch is no answer and closes
    /// the gate; a refused batch (over lodestar's limit) leaves it open.
    #[tokio::test]
    async fn a_batch_is_read_at_the_tip_and_a_refusal_keeps_the_gate() {
        let f = Fake::default();
        let l = client(&fake(&f).await);
        f.set("live");
        let other = "did:plc:cccccccccccccccccccccccc".to_string();
        let got = l.accounts(&[DID.to_string(), other.clone()], None).await.expect("batch");
        assert_eq!(got[DID].as_ref().expect("account").pds.as_deref(), Some("https://pds.example.com"));
        assert_eq!(got[&other], Err("not_found".to_string()));
        assert!(l.gate().is_open());
        f.set("too_large");
        assert!(l.accounts(&[DID.to_string()], None).await.is_none());
        assert!(l.gate().is_open(), "a refusal is not an outage");
        f.set("stale");
        assert!(l.accounts(&[DID.to_string()], None).await.is_none());
        assert!(!l.gate().is_open());
    }

    #[test]
    fn outcomes_close_the_gate_as_the_design_says() {
        for e in [Endpoint::Document, Endpoint::Account, Endpoint::Accounts, Endpoint::Probe] {
            assert!(Outcome::Timeout.closes_gate(e));
            assert!(Outcome::Unavailable.closes_gate(e));
        }
        assert!(!Outcome::Timeout.closes_gate(Endpoint::Handle));
        assert!(!Outcome::Unavailable.closes_gate(Endpoint::Handle));
        for e in [Endpoint::Document, Endpoint::Handle] {
            for o in [Outcome::Answered, Outcome::NotFound, Outcome::Tombstoned, Outcome::Refused] {
                assert!(!o.closes_gate(e), "{o:?}");
            }
            for o in [Outcome::Behind, Outcome::Forbidden, Outcome::Status, Outcome::Transport, Outcome::Malformed] {
                assert!(o.closes_gate(e), "{o:?}");
            }
        }
    }
}
