//! Endpoints in use (admin-ui design §10.3): every routed XRPC method's
//! requests since Prism started, by the class of its answer, and when it
//! was last used. Held in memory only and reset at a restart (C-A6 (b)).
//!
//! **Registry.** Every router builds its XRPC paths with [`path`], which
//! records the NSID and the router it belongs to, so the stats page can list
//! the methods that were never used. `serve` builds every router first, then
//! [`Usage::load_registry`] makes one counter per registered NSID; a request
//! touches only atomics. A routed NSID missing from the registry is still
//! counted, under a small locked map.
//!
//! **Counting.** [`count`] is a middleware that `xrpc::router::finalize_with`
//! places between the global limit and CORS. It reads axum's `MatchedPath`,
//! so only routed paths under `/xrpc/` are counted: unrouted NSIDs (their
//! own counter) and `/admin` are not. Each request is also exported as
//! `xrpc_requests_total{nsid, class}`, and its time as
//! `xrpc_request_seconds{nsid}`.

use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::sync::{Arc, LazyLock, Mutex, OnceLock, PoisonError};

use axum::extract::{MatchedPath, Request, State};
use axum::http::StatusCode;
use axum::middleware::Next;
use axum::response::Response;
use chrono::{DateTime, Utc};

/// `xrpc_requests_total{nsid, class}`.
pub const METRIC: &str = "xrpc_requests_total";

/// `xrpc_request_seconds{nsid}`, a histogram (fast-reads design §7.2): a
/// routed request's time from inside the global limit to its response, so
/// authentication is in it and a preflight is not.
pub const METRIC_SECONDS: &str = "xrpc_request_seconds";

/// What kind of answer a request got.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Class {
    Ok,
    ClientError,
    RateLimited,
    /// A 501: a method Prism does not offer, or public data is off.
    NotOffered,
    ServerError,
}

impl Class {
    pub const ALL: [Self; 5] = [Self::Ok, Self::ClientError, Self::RateLimited, Self::NotOffered, Self::ServerError];

    /// The class of a response status.
    #[must_use]
    pub fn of(status: StatusCode) -> Self {
        match status.as_u16() {
            429 => Self::RateLimited,
            501 => Self::NotOffered,
            400..=499 => Self::ClientError,
            500..=599 => Self::ServerError,
            _ => Self::Ok,
        }
    }

    /// The metric's `class` label.
    #[must_use]
    pub fn label(self) -> &'static str {
        match self {
            Self::Ok => "ok",
            Self::ClientError => "client_error",
            Self::RateLimited => "rate_limited",
            Self::NotOffered => "not_offered",
            Self::ServerError => "server_error",
        }
    }

    fn index(self) -> usize {
        self as usize
    }
}

/// NSID → the router that serves it.
static REGISTRY: LazyLock<Mutex<BTreeMap<String, &'static str>>> = LazyLock::new(Mutex::default);

fn registry() -> std::sync::MutexGuard<'static, BTreeMap<String, &'static str>> {
    REGISTRY.lock().unwrap_or_else(PoisonError::into_inner)
}

/// The route path of `nsid`, recorded as served by `router` (`app`,
/// `index`, `private`, `not_offered`, `services`, `threadlens`,
/// `resolve_did`, `health`).
#[must_use]
pub fn path(router: &'static str, nsid: &str) -> String {
    registry().insert(nsid.to_string(), router);
    format!("/xrpc/{nsid}")
}

/// Every registered NSID with its router, in NSID order.
#[must_use]
pub fn registered() -> Vec<(String, &'static str)> {
    registry().iter().map(|(n, r)| (n.clone(), *r)).collect()
}

#[derive(Default)]
struct Counters {
    by_class: [AtomicU64; 5],
    /// Unix milliseconds of the last request; 0 for none.
    last_ms: AtomicI64,
}

impl Counters {
    fn add(&self, class: Class, now_ms: i64) {
        self.by_class[class.index()].fetch_add(1, Ordering::Relaxed);
        self.last_ms.fetch_max(now_ms, Ordering::Relaxed);
    }

    fn read(&self) -> ([u64; 5], i64) {
        (std::array::from_fn(|i| self.by_class[i].load(Ordering::Relaxed)), self.last_ms.load(Ordering::Relaxed))
    }
}

struct Inner {
    /// One counter per registered NSID, built once.
    known: OnceLock<HashMap<String, Counters>>,
    /// Routed NSIDs the registry lacks.
    other: Mutex<HashMap<String, Counters>>,
    started: DateTime<Utc>,
}

/// The counters. Cheap to clone.
#[derive(Clone)]
pub struct Usage(Arc<Inner>);

impl Default for Usage {
    fn default() -> Self {
        Self(Arc::new(Inner { known: OnceLock::new(), other: Mutex::default(), started: Utc::now() }))
    }
}

/// One method as the stats page lists it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Row {
    pub nsid: String,
    /// Requests since the start, by [`Class::ALL`]'s order.
    pub by_class: [u64; 5],
    pub total: u64,
    pub last: Option<DateTime<Utc>>,
    /// Whether a router registered it.
    pub registered: bool,
}

impl Usage {
    /// Make one counter per NSID registered so far. Call it once every
    /// router is built; later calls change nothing.
    pub fn load_registry(&self) {
        let _ = self.0.known.set(registered().into_iter().map(|(n, _)| (n, Counters::default())).collect());
    }

    /// When counting began.
    #[must_use]
    pub fn started(&self) -> DateTime<Utc> {
        self.0.started
    }

    /// Count one request to `nsid`.
    pub fn record(&self, nsid: &str, class: Class) {
        let now = Utc::now().timestamp_millis();
        match self.0.known.get().and_then(|k| k.get(nsid)) {
            Some(c) => c.add(class, now),
            None => self.0.other.lock().unwrap_or_else(PoisonError::into_inner).entry(nsid.to_string()).or_default().add(class, now),
        }
        metrics::counter!(METRIC, "nsid" => nsid.to_string(), "class" => class.label()).increment(1);
    }

    /// Every counted or registered method, in NSID order.
    #[must_use]
    pub fn rows(&self) -> Vec<Row> {
        let row = |nsid: &str, c: &Counters, registered: bool| {
            let (by_class, last_ms) = c.read();
            Row {
                nsid: nsid.to_string(),
                by_class,
                total: by_class.iter().sum(),
                last: (last_ms > 0).then(|| DateTime::from_timestamp_millis(last_ms)).flatten(),
                registered,
            }
        };
        let mut out: Vec<Row> = self.0.known.get().map(|k| k.iter().map(|(n, c)| row(n, c, true)).collect()).unwrap_or_default();
        let registry = registry();
        for (n, c) in self.0.other.lock().unwrap_or_else(PoisonError::into_inner).iter() {
            out.push(row(n, c, registry.contains_key(n)));
        }
        out.sort_by(|a, b| a.nsid.cmp(&b.nsid));
        out
    }
}

/// The counting middleware (§10.3).
pub async fn count(State(usage): State<Usage>, request: Request, next: Next) -> Response {
    let nsid = request
        .extensions()
        .get::<MatchedPath>()
        .and_then(|p| p.as_str().strip_prefix("/xrpc/"))
        .map(str::to_string);
    let start = std::time::Instant::now();
    let response = next.run(request).await;
    if let Some(nsid) = nsid {
        metrics::histogram!(METRIC_SECONDS, "nsid" => nsid.clone()).record(start.elapsed().as_secs_f64());
        usage.record(&nsid, Class::of(response.status()));
    }
    response
}

/// A method's name in words: its last part split at capitals ("getTimeline"
/// is "Get timeline"), and what it belongs to.
#[must_use]
pub fn plain_name(nsid: &str) -> String {
    const GROUPS: &[(&str, &str)] = &[
        ("app.bsky.feed.", "posts and feeds"),
        ("app.bsky.actor.", "profiles"),
        ("app.bsky.graph.", "follows, lists and blocks"),
        ("app.bsky.notification.", "notifications"),
        ("app.bsky.unspecced.", "suggestions and trends"),
        ("app.bsky.bookmark.", "bookmarks"),
        ("app.bsky.draft.", "drafts"),
        ("app.bsky.labeler.", "labelers"),
        ("app.bsky.ageassurance.", "age assurance"),
        ("app.bsky.contact.", "contacts"),
        ("app.bsky.video.", "video"),
        ("app.nearhorizon.private.", "private posts"),
        ("app.nearhorizon.threadLens.", "Thread Lens"),
        ("app.nearhorizon.constellation.", "account lookups"),
        ("app.nearhorizon.", "Horizon"),
        ("com.atproto.", "accounts and records"),
        ("chat.bsky.", "chat"),
    ];
    if nsid == "_health" {
        return "Health check".to_string();
    }
    let method = nsid.rsplit('.').next().unwrap_or(nsid);
    let mut words = String::new();
    for (i, c) in method.chars().enumerate() {
        if i == 0 {
            words.extend(c.to_uppercase());
        } else if c.is_ascii_uppercase() {
            words.push(' ');
            words.push(c.to_ascii_lowercase());
        } else {
            words.push(c);
        }
    }
    match GROUPS.iter().find(|(prefix, _)| nsid.starts_with(prefix)) {
        Some((_, group)) => format!("{words} ({group})"),
        None => words,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn classes_and_names() {
        assert_eq!(Class::of(StatusCode::OK), Class::Ok);
        assert_eq!(Class::of(StatusCode::BAD_REQUEST), Class::ClientError);
        assert_eq!(Class::of(StatusCode::TOO_MANY_REQUESTS), Class::RateLimited);
        assert_eq!(Class::of(StatusCode::NOT_IMPLEMENTED), Class::NotOffered);
        assert_eq!(Class::of(StatusCode::BAD_GATEWAY), Class::ServerError);
        assert_eq!(plain_name("app.bsky.feed.getTimeline"), "Get timeline (posts and feeds)");
        assert_eq!(plain_name("app.nearhorizon.private.getPostThread"), "Get post thread (private posts)");
        assert_eq!(plain_name("_health"), "Health check");
        assert_eq!(plain_name("x.y.doIt"), "Do it");
    }

    /// Registered NSIDs get their counter once loaded; others are counted
    /// apart; the rows say which is which.
    #[test]
    fn counts_known_and_other() {
        let usage = Usage::default();
        let known = path("test", "test.usage.known");
        assert_eq!(known, "/xrpc/test.usage.known");
        usage.load_registry();
        usage.record("test.usage.known", Class::Ok);
        usage.record("test.usage.known", Class::ClientError);
        usage.record("test.usage.unregistered", Class::NotOffered);
        let rows = usage.rows();
        let of = |n: &str| rows.iter().find(|r| r.nsid == n).cloned().expect(n);
        let k = of("test.usage.known");
        assert_eq!((k.by_class, k.total, k.registered, k.last.is_some()), ([1, 1, 0, 0, 0], 2, true, true));
        let o = of("test.usage.unregistered");
        assert_eq!((o.by_class[3], o.registered), (1, false));
    }
}
