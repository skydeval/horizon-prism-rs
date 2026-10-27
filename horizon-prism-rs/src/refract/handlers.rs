//! `app.nearhorizon.refract.*` (refract design §7.1).
//!
//! - **Who may call** (C-3): any signed-in caller with a verified token. A
//!   ready member is served with member viewer state and its Farsight
//!   handle; anyone else, a member that is not ready included, as a
//!   **visitor**: no stored viewer state beyond the request's known
//!   blocks; restored views get the visitor's own like, repost, follow
//!   and `replyDisabled`, looked up per request and kept nowhere (RF-S1).
//!   Readiness errors are never answered here.
//! - **The request is public, never private**: a public `Requester`, never
//!   `HydrationContext::Private`.
//! - **Visitor blocks** follow collect → judge → emit: Farsight's pair
//!   answers for the authors the request names, then for the authors found
//!   in what was built; a pair left unanswered is treated as blocked
//!   (C-9 (b)). With no Farsight configured, visitors get no block
//!   enforcement (C-9 (a)).
//! - **Discovery** (§4.4): `fill` makes candidates of the DIDs it names,
//!   capped per caller, and for visitors only on direct calls (`aud` with
//!   `#bsky_appview`), per client address and in all. A DID whose resolve
//!   found it on no listed community is remembered in memory for a day
//!   ([`NotHosted`]): left out of `pending`, charged no cap. A DID being
//!   resolved, or whose resolve failed in the last few seconds
//!   ([`Resolving`]), is `pending` with no cap and no second resolve.
//! - **Errors**: `NotEnabled` (the feature is off), `AuthRequired`,
//!   `InvalidToken`, `RateLimitExceeded`; `getAuthorFeed`'s and `getAuthorReposts`' actor gate
//!   adds `AccountTakedown`, `AccountDeactivated` and `InvalidRequest`;
//!   the actor searches add `InvalidRequest` (a `q` too long, a malformed
//!   cursor).

use std::collections::{HashMap, HashSet};
use std::net::{IpAddr, SocketAddr};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::{Duration, Instant};

use axum::Json;
use axum::Router;
use axum::extract::{ConnectInfo, RawQuery, State};
use axum::http::{HeaderMap, StatusCode};
use axum::routing::get;
use chrono::Utc;
use serde_json::Value;

use super::{Refract, RefractSnapshot};
use crate::db::locks::uri_author;
use crate::farsight::{Lookup, PairBudget};
use crate::hydration::actor::ProfileKind;
use crate::hydration::normalize::subjects;
use crate::hydration::{KnownBlocks, PublicEmit, Viewer, with_known_blocks};
use crate::identity::ResolveIntent;
use crate::xrpc::AppState;
use crate::xrpc::auth::{Params, authenticate, request_handle, request_ip};
use crate::xrpc::errors::XrpcError;
use crate::xrpc::handlers::{XrpcResult, respond};
use crate::xrpc::pools::Requester;
use crate::xrpc::rate_limit::Family;

pub const FILL: &str = "app.nearhorizon.refract.fill";
pub const GET_REPLIES: &str = "app.nearhorizon.refract.getReplies";
pub const GET_AUTHOR_FEED: &str = "app.nearhorizon.refract.getAuthorFeed";
pub const GET_POST_THREAD: &str = "app.nearhorizon.refract.getPostThread";
pub const GET_TIMELINE: &str = "app.nearhorizon.refract.getTimeline";
pub const LIST_NOTIFICATIONS: &str = "app.nearhorizon.refract.listNotifications";
pub const GET_UNREAD_COUNT: &str = "app.nearhorizon.refract.getUnreadCount";
pub const GET_AUTHOR_REPOSTS: &str = "app.nearhorizon.refract.getAuthorReposts";
pub const SEARCH_ACTORS: &str = "app.nearhorizon.refract.searchActors";
pub const SEARCH_ACTORS_TYPEAHEAD: &str = "app.nearhorizon.refract.searchActorsTypeahead";

/// `fill` takes at most this many DIDs and as many URIs.
pub const FILL_MAX: usize = 25;
/// Subjects not stored here fetched per feed page and per `fill`
/// (refract design §3.3).
pub const FEED_SUBJECT_CAP: usize = 25;
/// `refract_reads_total{method, outcome}`.
pub const METRIC_READS: &str = "refract_reads_total";
/// `refract_read_seconds{method}`, a histogram with the latency buckets.
pub const METRIC_READ_SECONDS: &str = "refract_read_seconds";
/// `refract_candidates_total{source, outcome}`.
pub const METRIC_CANDIDATES: &str = "refract_candidates_total";
/// `refract_visitor_pairs_total{outcome}`.
pub const METRIC_PAIRS: &str = "refract_visitor_pairs_total";
/// The fragment a client's own token carries (design-v4 L3812-3817).
pub const DIRECT_FRAGMENT: &str = "bsky_appview";

/// Handler state.
#[derive(Clone)]
pub struct RefractState {
    pub app: AppState,
    pub refract: Refract,
    pub discovery: Arc<Discovery>,
    /// How long one round of a visitor's Farsight pair answers may take.
    pub round_timeout: Duration,
}

/// The Refract routes, merged into the app by `serve`.
pub fn router(state: RefractState) -> Router {
    Router::new()
        .route(&crate::admin::usage::path("refract", FILL), get(fill))
        .route(&crate::admin::usage::path("refract", GET_POST_THREAD), get(get_post_thread))
        .route(&crate::admin::usage::path("refract", GET_AUTHOR_FEED), get(get_author_feed))
        .route(&crate::admin::usage::path("refract", LIST_NOTIFICATIONS), get(list_notifications))
        .route(&crate::admin::usage::path("refract", GET_TIMELINE), get(get_timeline))
        .route(&crate::admin::usage::path("refract", GET_REPLIES), get(get_replies))
        .route(&crate::admin::usage::path("refract", GET_UNREAD_COUNT), get(get_unread_count))
        .route(&crate::admin::usage::path("refract", GET_AUTHOR_REPOSTS), get(get_author_reposts))
        .route(&crate::admin::usage::path("refract", SEARCH_ACTORS), get(search_actors))
        .route(&crate::admin::usage::path("refract", SEARCH_ACTORS_TYPEAHEAD), get(search_actors_typeahead))
        .route_layer(axum::middleware::from_fn(time_read))
        .with_state(state)
}

/// `refract_read_seconds{method}`: each Refract call's time, sign-in check
/// included, labelled as `refract_reads_total` is (`fill`, `getReplies`…).
async fn time_read(request: axum::extract::Request, next: axum::middleware::Next) -> axum::response::Response {
    let method = request
        .extensions()
        .get::<axum::extract::MatchedPath>()
        .and_then(|p| p.as_str().rsplit('/').next())
        .and_then(|nsid| nsid.strip_prefix("app.nearhorizon.refract."))
        .map(str::to_string);
    let start = std::time::Instant::now();
    let response = next.run(request).await;
    if let Some(method) = method {
        metrics::histogram!(METRIC_READ_SECONDS, "method" => method).record(start.elapsed().as_secs_f64());
    }
    response
}

/// `400 NotEnabled`.
#[must_use]
pub fn not_enabled() -> XrpcError {
    XrpcError::new(StatusCode::BAD_REQUEST, "NotEnabled", "This method is not enabled on this AppView")
}

fn not_found() -> XrpcError {
    XrpcError::new(StatusCode::BAD_REQUEST, "NotFound", "Not found")
}

/// Who is asking, as the gate sorted it.
#[derive(Debug, Clone)]
pub struct Caller {
    pub did: String,
    pub viewer: Viewer,
    pub req: Requester,
    pub ip: IpAddr,
    /// A `members` row exists (ready or not).
    pub member: bool,
    /// The token's `aud` carries `#bsky_appview`: the caller minted it.
    pub direct: bool,
}

impl Caller {
    /// A visitor: not a ready member.
    #[must_use]
    pub fn visitor(&self) -> bool {
        !self.viewer.hydrate
    }
}

/// Authenticate, check the switch, charge the `refract` class per caller
/// DID, and sort the caller (§7.1).
///
/// # Errors
///
/// `NotEnabled`, `AuthRequired`/`InvalidToken`, `RateLimitExceeded`.
pub async fn gate(s: &RefractState, headers: &HeaderMap, peer: SocketAddr, method: &str) -> Result<Caller, XrpcError> {
    if !s.refract.enabled() {
        return Err(not_enabled());
    }
    let authorization = headers.get("authorization").and_then(|v| v.to_str().ok());
    let caller = authenticate(&s.app, authorization, method).await?;
    let ip = request_ip(&s.app, headers, peer);
    s.app.limits.charge(method, Family::Refract, Some(&caller.did), ip)?;
    let row: Option<(bool, bool)> = sqlx::query_as(crate::aggregations::enroll::MEMBER_READINESS_SQL)
        .bind(&caller.did)
        .fetch_optional(s.app.hydrator.pool())
        .await
        .map_err(|e| XrpcError::from(crate::hydration::HydrationError::from(e)))?;
    let ready = row.is_some_and(|(ready, _)| ready);
    let viewer = Viewer { did: Some(caller.did.clone()), hydrate: ready };
    let req = request_handle(viewer.state_did(), ip).for_endpoint(method);
    // A ready member's Farsight handle, as `prepare` attaches it.
    if ready
        && let Some(farsight) = s.app.hydrator.farsight()
        && let Some(selected) = crate::xrpc::backstop::selected()
    {
        selected.attach_farsight(farsight.request(&req));
    }
    Ok(Caller {
        direct: caller.jwt.audience_fragment() == Some(DIRECT_FRAGMENT),
        did: caller.did,
        viewer,
        req,
        ip,
        member: row.is_some(),
    })
}

/// A visitor's blocks (§7.1): Farsight's pair answers, round by round.
pub struct Blocks {
    caller: String,
    lookup: Option<Lookup>,
    timeout: Duration,
    judged: HashSet<String>,
    known: KnownBlocks,
}

impl Blocks {
    /// Blocks for `caller`: a visitor's Farsight lookup when Farsight is
    /// configured; none for a ready member (its tables and handle apply).
    #[must_use]
    pub fn new(s: &RefractState, caller: &Caller) -> Option<Self> {
        if !caller.visitor() {
            return None;
        }
        let lookup = s.app.hydrator.farsight().map(|f| {
            let lookup = f.request(&caller.req);
            lookup.set_visitor();
            lookup
        });
        Some(Self { caller: caller.did.clone(), lookup, timeout: s.round_timeout, judged: HashSet::new(), known: KnownBlocks::default() })
    }

    /// Judge the authors of `dids` not judged yet. Returns whether any new
    /// block (or unanswered pair) was found.
    pub async fn round(&mut self, dids: &[String]) -> bool {
        let fresh: Vec<String> = dids.iter().filter(|d| **d != self.caller && !self.judged.contains(*d)).cloned().collect::<HashSet<_>>().into_iter().collect();
        if fresh.is_empty() {
            return false;
        }
        self.judged.extend(fresh.iter().cloned());
        // With no Farsight, no block source at all (C-9 (a)).
        let Some(lookup) = &self.lookup else {
            return false;
        };
        let got = lookup.pair_answers(&self.caller, &fresh, PairBudget { max_calls: u32::MAX, timeout: self.timeout }, None).await;
        let mut changed = false;
        for did in fresh {
            match got.answers.get(&did) {
                Some(answer) => {
                    metrics::counter!(METRIC_PAIRS, "outcome" => "answered").increment(1);
                    if answer.inbound.is_block() {
                        changed = true;
                        self.known.of_viewer.insert(did.clone());
                    }
                    if answer.outbound.is_block() {
                        changed = true;
                        self.known.by_viewer.insert(did);
                    }
                }
                None => {
                    metrics::counter!(METRIC_PAIRS, "outcome" => "unanswered").increment(1);
                    changed = true;
                    self.known.unanswered.insert(did);
                }
            }
        }
        changed
    }

    /// Whether `did` was left unanswered.
    #[must_use]
    pub fn unanswered(&self, did: &str) -> bool {
        self.known.unanswered.contains(did)
    }

    /// Whether `did` blocks the caller, is blocked by it, or was left
    /// unanswered.
    #[must_use]
    pub fn blocked(&self, did: &str) -> bool {
        self.known.of_viewer.contains(did) || self.known.by_viewer.contains(did) || self.known.unanswered.contains(did)
    }

    #[must_use]
    pub fn known(&self) -> Arc<KnownBlocks> {
        Arc::new(self.known.clone())
    }
}

/// Run `build` with the request's blocks: for a visitor, after a first
/// round over `named`, then again when the authors found in what it built
/// bring a new block (collect → judge → emit).
async fn with_blocks<F, Fut>(blocks: &mut Option<Blocks>, named: &[String], build: F) -> Result<Json<PublicEmit<Value>>, XrpcError>
where
    F: Fn() -> Fut,
    Fut: Future<Output = XrpcResult>,
{
    let Some(b) = blocks.as_mut() else {
        return build().await;
    };
    b.round(named).await;
    let first = with_known_blocks(b.known(), build()).await?;
    let (found, _) = subjects(first.0.inner());
    let found: Vec<String> = found.into_iter().collect();
    if b.round(&found).await {
        return with_known_blocks(b.known(), build()).await;
    }
    Ok(first)
}

/// A view-bearing answer's last step: for a visitor, its own viewer state
/// on the restored views and `viewerChecked` (RF-S1, RF-S13; a ready
/// member's views are left as built); then `servedAs` (RF-S10).
async fn visitor_state(s: &RefractState, caller: &Caller, result: XrpcResult) -> XrpcResult {
    let result = match result {
        Ok(Json(body)) if caller.visitor() => s
            .app
            .hydrator
            .refract_visitor_state(&caller.did, &caller.req, body)
            .await
            .map(Json)
            .map_err(Into::into),
        other => other,
    };
    served_as(caller, result)
}

/// RF-S10: every answer names the form it was served in, as the gate
/// decided it.
fn served_as(caller: &Caller, result: XrpcResult) -> XrpcResult {
    result.map(|Json(body)| {
        let form = if caller.visitor() { "visitor" } else { "member" };
        Json(crate::hydration::Hydrator::refract_with(&body, &[("servedAs", serde_json::json!(form))]))
    })
}

/// The in-memory caps on candidate creation (§7.1 "Abuse bounds"), in
/// fixed hourly windows, reset at restart; and the DIDs found on no listed
/// community.
#[derive(Default)]
pub struct Discovery {
    windows: Mutex<Windows>,
    pub not_hosted: NotHosted,
    pub resolving: Resolving,
}

/// How long a DID whose resolve just failed (transient, or no budget) is
/// answered `pending` without a new resolve (RF-S15).
pub const RESOLVE_FAILED_MEMO: Duration = Duration::from_secs(5);
/// An in-flight entry older than this is taken as lost (the resolver's own
/// timeouts end a resolve well before it).
pub const RESOLVE_IN_FLIGHT_MAX: Duration = Duration::from_secs(30);
/// The most DIDs the memo holds; past it a resolve is started unremembered.
pub const RESOLVING_MAX: usize = 10_000;

/// RF-S15: the DIDs a `fill` started resolving and those whose resolve just
/// failed. A DID in it is answered `pending` with no cap charged and no new
/// resolve, so a re-ask neither starts a second resolve nor pays again. In
/// memory only, bounded, lost at restart; judged against one communities
/// list like [`NotHosted`], and cleared by any change of it.
pub struct Resolving {
    inner: Mutex<ResolvingInner>,
    failed_for: Duration,
}

#[derive(Default)]
struct ResolvingInner {
    against: super::Communities,
    entries: HashMap<String, Memo>,
}

#[derive(Debug, Clone, Copy)]
enum Memo {
    /// Started at.
    InFlight(Instant),
    /// Remembered until.
    Failed(Instant),
}

impl Memo {
    fn live(self, now: Instant) -> bool {
        match self {
            Self::InFlight(at) => now.saturating_duration_since(at) < RESOLVE_IN_FLIGHT_MAX,
            Self::Failed(until) => now < until,
        }
    }
}

impl Default for Resolving {
    fn default() -> Self {
        Self::new(RESOLVE_FAILED_MEMO)
    }
}

impl Resolving {
    #[must_use]
    pub fn new(failed_for: Duration) -> Self {
        Self { inner: Mutex::default(), failed_for }
    }

    fn with<T>(&self, now: &super::Communities, f: impl FnOnce(&mut HashMap<String, Memo>) -> T) -> T {
        let mut inner = self.inner.lock().unwrap_or_else(PoisonError::into_inner);
        if inner.against != *now {
            inner.entries.clear();
            inner.against = now.clone();
        }
        f(&mut inner.entries)
    }

    /// Whether `did` is being resolved, or its resolve failed moments ago.
    #[must_use]
    pub fn contains(&self, did: &str, now: &super::Communities) -> bool {
        self.with(now, |m| match m.get(did) {
            Some(e) if e.live(Instant::now()) => true,
            Some(_) => {
                m.remove(did);
                false
            }
            None => false,
        })
    }

    /// Whether `did`'s resolve failed and is remembered now.
    #[cfg(test)]
    #[must_use]
    pub fn failed(&self, did: &str) -> bool {
        let inner = self.inner.lock().unwrap_or_else(PoisonError::into_inner);
        matches!(inner.entries.get(did), Some(e @ Memo::Failed(_)) if e.live(Instant::now()))
    }

    /// Claim `did` for a resolve: false when one is in flight or just
    /// failed (nothing to start); true otherwise, remembered as in flight
    /// when there is room.
    pub fn begin(&self, did: &str, now: &super::Communities) -> bool {
        self.with(now, |m| {
            let at = Instant::now();
            if m.get(did).is_some_and(|e| e.live(at)) {
                return false;
            }
            if room(m, did, at) {
                m.insert(did.to_string(), Memo::InFlight(at));
            }
            true
        })
    }

    /// A resolve of `did`, started under `judged`, ended: remembered for a
    /// moment when it failed, forgotten otherwise.
    pub fn finish(&self, did: &str, judged: &super::Communities, failed: bool) {
        self.with(judged, |m| {
            let at = Instant::now();
            if failed && room(m, did, at) {
                m.insert(did.to_string(), Memo::Failed(at + self.failed_for));
            } else {
                m.remove(did);
            }
        });
    }
}

/// Whether `did` fits in the memo, dropping ended entries when it is full.
fn room(m: &mut HashMap<String, Memo>, did: &str, now: Instant) -> bool {
    if m.contains_key(did) || m.len() < RESOLVING_MAX {
        return true;
    }
    m.retain(|_, e| e.live(now));
    m.len() < RESOLVING_MAX
}

/// How long a not-hosted outcome is remembered.
pub const NOT_HOSTED_TTL: Duration = Duration::from_secs(24 * 3600);
/// The most not-hosted DIDs remembered.
pub const NOT_HOSTED_MAX: u64 = 100_000;

/// DIDs a `fill`'s resolve found on no listed community: for 24 hours a
/// further `fill` leaves them out of `pending` and charges no cap. In
/// memory only, bounded, lost at restart; nothing about them is written
/// anywhere (D-2). Every entry was judged against the communities list in
/// `against`; any change of the list clears them all, so a DID a new or
/// widened entry would host is resolved again.
pub struct NotHosted {
    cache: moka::sync::Cache<String, ()>,
    against: Mutex<super::Communities>,
}

impl Default for NotHosted {
    fn default() -> Self {
        Self::new(NOT_HOSTED_MAX, NOT_HOSTED_TTL)
    }
}

impl NotHosted {
    #[must_use]
    pub fn new(max: u64, ttl: Duration) -> Self {
        Self { cache: moka::sync::Cache::builder().max_capacity(max).time_to_live(ttl).build(), against: Mutex::default() }
    }

    /// Run `f` with the entries judged against `now`, clearing them all
    /// first when they were judged against another list. The lock is held
    /// throughout, so an entry is never kept across a change of the list.
    fn with<T>(&self, now: &super::Communities, f: impl FnOnce(&moka::sync::Cache<String, ()>) -> T) -> T {
        let mut against = self.against.lock().unwrap_or_else(PoisonError::into_inner);
        if *against != *now {
            self.cache.invalidate_all();
            *against = now.clone();
        }
        f(&self.cache)
    }

    /// Whether `did` was found on no community of `now`.
    #[must_use]
    pub fn contains(&self, did: &str, now: &super::Communities) -> bool {
        self.with(now, |c| c.contains_key(did))
    }

    /// Remember that `did` is on no community of `judged`.
    pub fn insert(&self, did: &str, judged: &super::Communities) {
        self.with(judged, |c| c.insert(did.to_string(), ()));
    }
}

#[derive(Default)]
struct Windows {
    hour: i64,
    per_caller: HashMap<String, u32>,
    resolves: HashMap<String, u32>,
    per_ip: HashMap<String, u32>,
    global: u32,
}

/// What a cap counts.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Cap {
    Caller,
    Resolve,
    Ip,
    Global,
}

impl Discovery {
    /// Take one of `cap` for `key` if under `max`.
    pub fn take(&self, cap: Cap, key: &str, max: u32) -> bool {
        let mut w = self.windows.lock().unwrap_or_else(PoisonError::into_inner);
        let hour = Utc::now().timestamp() / 3600;
        if w.hour != hour {
            *w = Windows { hour, ..Windows::default() };
        }
        let n = match cap {
            Cap::Caller => w.per_caller.entry(key.to_string()).or_default(),
            Cap::Resolve => w.resolves.entry(key.to_string()).or_default(),
            Cap::Ip => w.per_ip.entry(key.to_string()).or_default(),
            Cap::Global => &mut w.global,
        };
        if *n >= max {
            return false;
        }
        *n += 1;
        true
    }

    /// How many of `cap` are taken for `key` this hour.
    #[cfg(test)]
    pub fn taken(&self, cap: Cap, key: &str) -> u32 {
        let w = self.windows.lock().unwrap_or_else(PoisonError::into_inner);
        match cap {
            Cap::Caller => w.per_caller.get(key).copied().unwrap_or_default(),
            Cap::Resolve => w.resolves.get(key).copied().unwrap_or_default(),
            Cap::Ip => w.per_ip.get(key).copied().unwrap_or_default(),
            Cap::Global => w.global,
        }
    }

    /// A discovery whose failed resolves are remembered for `failed_for`.
    #[cfg(test)]
    #[must_use]
    pub fn with_failure_memo(failed_for: Duration) -> Self {
        Self { resolving: Resolving::new(failed_for), ..Self::default() }
    }

    /// Give back one of `cap` taken for `key`.
    pub fn give_back(&self, cap: Cap, key: &str) {
        let mut w = self.windows.lock().unwrap_or_else(PoisonError::into_inner);
        let n = match cap {
            Cap::Caller => w.per_caller.get_mut(key),
            Cap::Resolve => w.resolves.get_mut(key),
            Cap::Ip => w.per_ip.get_mut(key),
            Cap::Global => Some(&mut w.global),
        };
        if let Some(n) = n {
            *n = n.saturating_sub(1);
        }
    }
}

/// The address key the backstop uses: an IPv6 address by its /64.
#[must_use]
pub fn ip_key(ip: IpAddr) -> String {
    match ip {
        IpAddr::V4(v4) => v4.to_string(),
        IpAddr::V6(v6) => {
            let s = v6.segments();
            format!("{:x}:{:x}:{:x}:{:x}::/64", s[0], s[1], s[2], s[3])
        }
    }
}

/// What discovery decided for a `fill`'s DIDs.
#[derive(Debug, Default)]
pub struct Discovered {
    /// Accepted for checking, or restored and waiting for first labels.
    pub pending: Vec<String>,
    /// Held back for a reason that isn't final (RF-S11): `capped` (a
    /// discovery cap) or `proxied` (a visitor's bare-`aud` call).
    pub withheld: Vec<(String, &'static str)>,
}

impl RefractState {
    /// Discovery (§4.4) for the DIDs a `fill` names that are not in R-live:
    /// returns `pending`, every one accepted for checking (a candidate
    /// already, or one whose resolve was started now), plus R-live DIDs
    /// waiting for their first label fetch; and `withheld`, those a cap or
    /// a proxied visitor call kept from discovery. Never waits for a
    /// resolve or a check, and reads no `actors` row (G-9).
    ///
    /// # Errors
    ///
    /// Database errors.
    pub async fn discover(&self, caller: &Caller, dids: &[String], snapshot: &RefractSnapshot) -> Result<Discovered, sqlx::Error> {
        let cfg = self.refract.cfg();
        let pool = self.refract.pool();
        let communities = self.refract.communities();
        let mut pending = Vec::new();
        let mut withheld: Vec<(String, &'static str)> = Vec::new();
        let mut started: Vec<(String, tokio::task::JoinHandle<&'static str>)> = Vec::new();
        for did in dids {
            if snapshot.is_restored(did) {
                if snapshot.awaiting.contains(did) {
                    pending.push(did.clone());
                }
                continue;
            }
            let row: Option<(String, bool)> = sqlx::query_as("SELECT state, next_check_at <= now() FROM refract_dids WHERE did = $1")
                .bind(did)
                .fetch_optional(pool)
                .await?;
            match row.as_ref().map(|(s, due)| (s.as_str(), *due)) {
                Some(("candidate", _)) => {
                    pending.push(did.clone());
                    continue;
                }
                Some(("rejected", true)) | None => {}
                // Restored after this request's snapshot was taken (the
                // checker ran in between): not served this time, so
                // `pending`, never a silent omission (RF-S16). Restored
                // and pending removal stays out.
                Some(("restored", _)) => {
                    let removing: bool = sqlx::query_scalar("SELECT EXISTS (SELECT 1 FROM refract_pending_dids WHERE did = $1)")
                        .bind(did)
                        .fetch_one(pool)
                        .await?;
                    if !removing {
                        pending.push(did.clone());
                    }
                    continue;
                }
                // Rejected not yet re-eligible, grace or ending: nothing to
                // start.
                Some(_) => continue,
            }
            // Found on no listed community lately: nothing to start, no cap.
            if self.discovery.not_hosted.contains(did, &communities) {
                metrics::counter!(METRIC_CANDIDATES, "source" => "fill", "outcome" => "not_hosted_cached").increment(1);
                continue;
            }
            // The free checks: no local takedown, not pending removal.
            let blocked: bool = sqlx::query_scalar(
                "SELECT EXISTS (SELECT 1 FROM labels WHERE subject = $1 AND val = '!takedown') \
                 OR EXISTS (SELECT 1 FROM refract_pending_dids WHERE did = $1)",
            )
            .bind(did)
            .fetch_one(pool)
            .await?;
            if blocked {
                metrics::counter!(METRIC_CANDIDATES, "source" => "fill", "outcome" => "known").increment(1);
                continue;
            }
            // Being resolved, or its resolve failed moments ago (RF-S15):
            // pending, no cap, no second resolve.
            if self.discovery.resolving.contains(did, &communities) {
                metrics::counter!(METRIC_CANDIDATES, "source" => "fill", "outcome" => "resolving").increment(1);
                pending.push(did.clone());
                continue;
            }
            if caller.visitor() {
                if !caller.direct {
                    metrics::counter!(METRIC_CANDIDATES, "source" => "fill", "outcome" => "no_fragment").increment(1);
                    withheld.push((did.clone(), "proxied"));
                    continue;
                }
                let ip = ip_key(caller.ip);
                if !self.discovery.take(Cap::Ip, &ip, cfg.visitor_candidates_per_ip_per_hour) {
                    metrics::counter!(METRIC_CANDIDATES, "source" => "fill", "outcome" => "visitor_capped").increment(1);
                    withheld.push((did.clone(), "capped"));
                    continue;
                }
                if !self.discovery.take(Cap::Global, "", cfg.visitor_candidates_per_hour) {
                    self.discovery.give_back(Cap::Ip, &ip);
                    metrics::counter!(METRIC_CANDIDATES, "source" => "fill", "outcome" => "visitor_capped").increment(1);
                    withheld.push((did.clone(), "capped"));
                    continue;
                }
            }
            let give_back_visitor = || {
                if caller.visitor() {
                    self.discovery.give_back(Cap::Ip, &ip_key(caller.ip));
                    self.discovery.give_back(Cap::Global, "");
                }
            };
            if !self.discovery.take(Cap::Caller, &caller.did, cfg.candidates_per_caller_per_hour) {
                metrics::counter!(METRIC_CANDIDATES, "source" => "fill", "outcome" => "capped").increment(1);
                withheld.push((did.clone(), "capped"));
                continue;
            }
            // A resolve the resolver's cache cannot answer counts.
            let cached = self.refract.resolver().cache().lookup(did).await.ok().flatten().is_some();
            if !cached && !self.discovery.take(Cap::Resolve, &caller.did, cfg.resolves_per_caller_per_hour) {
                self.discovery.give_back(Cap::Caller, &caller.did);
                metrics::counter!(METRIC_CANDIDATES, "source" => "fill", "outcome" => "resolve_capped").increment(1);
                withheld.push((did.clone(), "capped"));
                continue;
            }
            // Another request started this DID's resolve meanwhile: the
            // caps go back and it is pending as theirs.
            if !self.discovery.resolving.begin(did, &communities) {
                give_back_visitor();
                self.discovery.give_back(Cap::Caller, &caller.did);
                if !cached {
                    self.discovery.give_back(Cap::Resolve, &caller.did);
                }
                metrics::counter!(METRIC_CANDIDATES, "source" => "fill", "outcome" => "resolving").increment(1);
                pending.push(did.clone());
                continue;
            }
            pending.push(did.clone());
            let refract = self.refract.clone();
            let discovery = Arc::clone(&self.discovery);
            let task_did = did.clone();
            let judged = communities.clone();
            let handle = tokio::spawn(async move {
                let outcome = resolve_and_insert(&refract, &discovery.not_hosted, &task_did).await;
                discovery.resolving.finish(&task_did, &judged, matches!(outcome, "resolve_failed" | "error"));
                metrics::counter!(METRIC_CANDIDATES, "source" => "fill", "outcome" => outcome).increment(1);
                outcome
            });
            started.push((did.clone(), handle));
        }
        // Wait briefly for the resolves this call started (2026-10-27): most
        // finish in tens of milliseconds, and one hosted by no listed
        // community is then answered as final here, not `pending`. A resolve
        // still running at the deadline keeps going and stays `pending`.
        let deadline = tokio::time::Instant::now() + FILL_RESOLVE_WAIT;
        for (did, handle) in started {
            if let Ok(Ok("not_hosted")) = tokio::time::timeout_at(deadline, handle).await {
                pending.retain(|d| *d != did);
            }
        }
        Ok(Discovered { pending, withheld })
    }
}

/// How long `fill` waits for the resolves it started before answering.
const FILL_RESOLVE_WAIT: std::time::Duration = std::time::Duration::from_millis(300);

/// A `fill`'s background resolve of `did`: insert it as a candidate when a
/// listed community hosts it. A definite "hosted by none" (the document
/// names no PDS, or one no enabled entry matches, or the registry has no
/// usable document) is remembered in `not_hosted`; a transient failure or a
/// refusal for another reason (a purge running) is not. Returns the
/// metric's outcome.
async fn resolve_and_insert(refract: &Refract, not_hosted: &NotHosted, did: &str) -> &'static str {
    let communities = refract.communities();
    let host = match refract.resolver().resolve_info(did, ResolveIntent::Lookup).await {
        Ok(info) => info.pds_endpoint.as_deref().and_then(crate::constellation::matcher::pds_host),
        Err(e) if e.is_transient() || matches!(e, crate::identity::IdentityError::NoBudget(_)) => return "resolve_failed",
        Err(_) => None,
    };
    let Some(host) = host.filter(|h| communities.home(h).is_some()) else {
        not_hosted.insert(did, &communities);
        return "not_hosted";
    };
    match refract.insert_candidate(did, &host, "fill").await {
        Ok(true) => "inserted",
        Ok(false) => "not_hosted",
        Err(error) => {
            tracing::warn!(event = "refract_candidate_failed", did, %error, "inserting a candidate failed");
            "error"
        }
    }
}

/// `fill`'s DIDs and at-URIs less the malformed ones and repeats.
fn valid_fill_items(dids: Vec<String>, uris: Vec<String>) -> (Vec<String>, Vec<String>) {
    let mut seen = HashSet::new();
    let dids = dids.into_iter().filter(|d| proto_blue::syntax::Did::is_valid(d) && seen.insert(d.clone())).collect();
    let uris = uris.into_iter().filter(|u| at_uri(u) && seen.insert(u.clone())).collect();
    (dids, uris)
}

fn at_uri(u: &str) -> bool {
    u.starts_with("at://") && uri_author(u).is_some_and(proto_blue::syntax::Did::is_valid) && u.matches('/').count() == 4
}

/// `refract.fill`.
pub async fn fill(State(s): State<RefractState>, ConnectInfo(peer): ConnectInfo<SocketAddr>, headers: HeaderMap, RawQuery(q): RawQuery) -> XrpcResult {
    let caller = gate(&s, &headers, peer, FILL).await?;
    let p = Params::parse(q.as_deref());
    let (dids, uris) = (p.all("dids"), p.all("uris"));
    if dids.len() > FILL_MAX || uris.len() > FILL_MAX {
        return Err(XrpcError::invalid("Error: dids and uris take at most 25 items each"));
    }
    // A malformed item is dropped; the rest are answered.
    let (dids, uris) = valid_fill_items(dids, uris);
    let h = &s.app.hydrator;
    let snapshot = h.refract_snapshot(&caller.req).await;
    let mut named: Vec<String> = dids.clone();
    for u in &uris {
        if let Some(a) = uri_author(u)
            && !named.iter().any(|d| d == a)
        {
            named.push(a.to_string());
        }
    }
    let Discovered { pending, withheld: held } = s.discover(&caller, &named, &snapshot).await.map_err(|e| XrpcError::from(crate::hydration::HydrationError::from(e)))?;
    // RF-S11: a DID held back from discovery, and the posts of it named.
    let mut withheld: Vec<(String, &'static str)> = Vec::new();
    for (did, reason) in &held {
        if dids.contains(did) {
            withheld.push((did.clone(), reason));
        }
        withheld.extend(uris.iter().filter(|u| uri_author(u) == Some(did.as_str())).map(|u| (u.clone(), *reason)));
    }
    let served = |d: &str| snapshot.is_restored(d) && !snapshot.awaiting.contains(d);
    let profiles: Vec<String> = dids.iter().filter(|d| served(d)).cloned().collect();
    let posts: Vec<String> = uris.iter().filter(|u| uri_author(u).is_some_and(served)).cloned().collect();
    s.refract.serve_check(&caller.req, &posts).await;
    let mut blocks = Blocks::new(&s, &caller);
    let authors: Vec<String> = profiles.iter().cloned().chain(posts.iter().filter_map(|u| uri_author(u).map(str::to_string))).collect();
    if let Some(b) = blocks.as_mut() {
        b.round(&authors).await;
    }
    // A restored account left unanswered for a visitor: no profile, and
    // said so (RF-S11).
    let (unanswered, profiles): (Vec<String>, Vec<String>) = profiles.into_iter().partition(|d| blocks.as_ref().is_some_and(|b| b.unanswered(d)));
    withheld.extend(unanswered.into_iter().map(|d| (d, "unanswered")));
    let result = with_blocks(&mut blocks, &authors, || async {
        h.refract_fill(&caller.viewer, &caller.req, &profiles, &posts, &pending, &withheld).await.map(Json).map_err(Into::into)
    })
    .await;
    let result = visitor_state(&s, &caller, result).await;
    metrics::counter!(METRIC_READS, "method" => "fill", "outcome" => if result.is_ok() { "ok" } else { "error" }).increment(1);
    result
}

/// `refract.getPostThread`: as `app.bsky.feed.getPostThread`, for an anchor
/// by a DID in R-live.
pub async fn get_post_thread(State(s): State<RefractState>, ConnectInfo(peer): ConnectInfo<SocketAddr>, headers: HeaderMap, RawQuery(q): RawQuery) -> XrpcResult {
    let caller = gate(&s, &headers, peer, GET_POST_THREAD).await?;
    let p = Params::parse(q.as_deref());
    let uri = p.required("uri")?.to_string();
    let depth = p.u32("depth")?.unwrap_or(6).min(1000);
    let parent_height = p.u32("parentHeight")?.unwrap_or(80).min(1000);
    let h = &s.app.hydrator;
    let snapshot = h.refract_snapshot(&caller.req).await;
    let Some(author) = uri_author(&uri).filter(|a| at_uri(&uri) && snapshot.is_restored(a) && !snapshot.awaiting.contains(*a)).map(str::to_string) else {
        metrics::counter!(METRIC_READS, "method" => "getPostThread", "outcome" => "not_restored").increment(1);
        return Err(not_found());
    };
    s.refract.serve_check(&caller.req, std::slice::from_ref(&uri)).await;
    let mut blocks = Blocks::new(&s, &caller);
    let result = with_blocks(&mut blocks, std::slice::from_ref(&author), || async {
        respond(&caller.req, h.get_post_thread(&caller.viewer, &caller.req, &uri, depth, parent_height).await)
    })
    .await;
    let result = visitor_state(&s, &caller, result).await;
    metrics::counter!(METRIC_READS, "method" => "getPostThread", "outcome" => if result.is_ok() { "ok" } else { "error" }).increment(1);
    result
}

/// `refract.getAuthorFeed`: as `app.bsky.feed.getAuthorFeed`, for a DID in
/// R-live, from its stored posts and reposts (refract design §7.1).
pub async fn get_author_feed(State(s): State<RefractState>, ConnectInfo(peer): ConnectInfo<SocketAddr>, headers: HeaderMap, RawQuery(q): RawQuery) -> XrpcResult {
    let caller = gate(&s, &headers, peer, GET_AUTHOR_FEED).await?;
    let p = Params::parse(q.as_deref());
    let actor = p.required("actor")?.to_string();
    let limit = crate::hydration::feed::clamp_limit(p.u32("limit")?).min(100);
    let filter = p.one("filter").unwrap_or("posts_with_replies").to_string();
    let cursor = p.one("cursor").map(str::to_string);
    let include_pins = p.bool("includePins");
    let h = &s.app.hydrator;
    let hydration = |e: sqlx::Error| XrpcError::from(crate::hydration::HydrationError::from(e));
    let did = h.resolve_actor(&caller.req, &actor).await.map_err(hydration)?;
    let snapshot = h.refract_snapshot(&caller.req).await;
    let Some(did) = did.filter(|d| snapshot.is_restored(d) && !snapshot.awaiting.contains(d)) else {
        metrics::counter!(METRIC_READS, "method" => "getAuthorFeed", "outcome" => "not_restored").increment(1);
        return Err(not_found());
    };
    if let Some((error, message)) = h.gate(&caller.req, &did).await.map_err(hydration)?.profile_error() {
        return Err(XrpcError::new(StatusCode::BAD_REQUEST, error, message));
    }
    let (items, next) = h.refract_author_items(&did, &filter, cursor.as_deref(), limit, include_pins).await.map_err(hydration)?;
    let local: Vec<String> = items.iter().filter(|i| i.kind != "repost").map(|i| i.post_uri.clone()).collect();
    s.refract.serve_check(&caller.req, &local).await;
    let mut blocks = Blocks::new(&s, &caller);
    let mut named: Vec<String> = vec![did.clone()];
    named.extend(items.iter().filter_map(|i| uri_author(&i.post_uri).map(str::to_string)));
    if let Some(b) = blocks.as_mut() {
        b.round(&named).await;
    }
    // A restored actor left unanswered for a visitor: an empty feed.
    let items = if blocks.as_ref().is_some_and(|b| b.unanswered(&did)) { Vec::new() } else { items };
    let next = if items.is_empty() { None } else { next };
    let result = with_blocks(&mut blocks, &named, || async {
        h.refract_feed(&caller.viewer, &caller.req, &items, next.as_deref(), FEED_SUBJECT_CAP).await.map(Json).map_err(Into::into)
    })
    .await;
    let result = visitor_state(&s, &caller, result).await;
    metrics::counter!(METRIC_READS, "method" => "getAuthorFeed", "outcome" => if result.is_ok() { "ok" } else { "error" }).increment(1);
    result
}

/// `refract.getAuthorReposts` (refract-reposts design §3.2): any actor's
/// reposts of restored posts, newest first, for anyone signed in. A visitor
/// blocked either way by the actor, or left unanswered, gets an empty feed.
pub async fn get_author_reposts(State(s): State<RefractState>, ConnectInfo(peer): ConnectInfo<SocketAddr>, headers: HeaderMap, RawQuery(q): RawQuery) -> XrpcResult {
    let caller = gate(&s, &headers, peer, GET_AUTHOR_REPOSTS).await?;
    let p = Params::parse(q.as_deref());
    let actor = p.required("actor")?.to_string();
    let limit = crate::hydration::feed::clamp_limit(p.u32("limit")?).min(100);
    let cursor = p.one("cursor").map(str::to_string);
    let h = &s.app.hydrator;
    let hydration = |e: sqlx::Error| XrpcError::from(crate::hydration::HydrationError::from(e));
    let Some(did) = h.resolve_actor(&caller.req, &actor).await.map_err(hydration)? else {
        metrics::counter!(METRIC_READS, "method" => "getAuthorReposts", "outcome" => "not_found").increment(1);
        return Err(XrpcError::invalid("Profile not found"));
    };
    if let Some((error, message)) = h.gate(&caller.req, &did).await.map_err(hydration)?.profile_error() {
        return Err(XrpcError::new(StatusCode::BAD_REQUEST, error, message));
    }
    let snapshot = h.refract_snapshot(&caller.req).await;
    let (items, next) = h.refract_author_reposts_items(&did, &served(&snapshot), cursor.as_deref(), limit).await.map_err(hydration)?;
    let local: Vec<String> = items.iter().map(|i| i.post_uri.clone()).collect();
    s.refract.serve_check(&caller.req, &local).await;
    let mut blocks = Blocks::new(&s, &caller);
    let mut named: Vec<String> = vec![did.clone()];
    named.extend(items.iter().filter_map(|i| uri_author(&i.post_uri).map(str::to_string)));
    if let Some(b) = blocks.as_mut() {
        b.round(&named).await;
    }
    let items = if blocks.as_ref().is_some_and(|b| b.blocked(&did)) { Vec::new() } else { items };
    let next = if items.is_empty() { None } else { next };
    let result = with_blocks(&mut blocks, &named, || async {
        h.refract_feed(&caller.viewer, &caller.req, &items, next.as_deref(), FEED_SUBJECT_CAP).await.map(Json).map_err(Into::into)
    })
    .await;
    let result = visitor_state(&s, &caller, result).await;
    metrics::counter!(METRIC_READS, "method" => "getAuthorReposts", "outcome" => if result.is_ok() { "ok" } else { "error" }).increment(1);
    result
}

/// `refract.searchActors`: restored accounts whose handle or display name
/// starts with `q`, as `profileView`s, paged.
pub async fn search_actors(State(s): State<RefractState>, ConnectInfo(peer): ConnectInfo<SocketAddr>, headers: HeaderMap, RawQuery(q): RawQuery) -> XrpcResult {
    search(&s, peer, &headers, q.as_deref(), SEARCH_ACTORS).await
}

/// `refract.searchActorsTypeahead`: as `searchActors`, one page of
/// `profileViewBasic`s.
pub async fn search_actors_typeahead(State(s): State<RefractState>, ConnectInfo(peer): ConnectInfo<SocketAddr>, headers: HeaderMap, RawQuery(q): RawQuery) -> XrpcResult {
    search(&s, peer, &headers, q.as_deref(), SEARCH_ACTORS_TYPEAHEAD).await
}

/// The Refract actor searches, for anyone signed in: prefix matches on the
/// handles and display names of R-live (less the DIDs waiting for their
/// first labels), case and diacritics folded, ordered exact handle, handle
/// prefix, display-name prefix, then by handle. The gate, the caller's
/// blocks either way (a visitor's from Farsight, unanswered treated as
/// blocked and listed in `withheld`) and full mutes apply after the page is
/// cut, so a page can be short.
async fn search(s: &RefractState, peer: SocketAddr, headers: &HeaderMap, q: Option<&str>, method: &'static str) -> XrpcResult {
    use crate::hydration::refract::{SEARCH_Q_MAX, SearchKey, search_fold};
    let caller = gate(s, headers, peer, method).await?;
    let typeahead = method == SEARCH_ACTORS_TYPEAHEAD;
    let label = if typeahead { "searchActorsTypeahead" } else { "searchActors" };
    let p = Params::parse(q);
    let limit = p.u32("limit")?.unwrap_or(if typeahead { 10 } else { 25 }).clamp(1, 100);
    let term = p.one("q").unwrap_or_default().trim();
    let term = term.strip_prefix('@').unwrap_or(term);
    if term.chars().count() > SEARCH_Q_MAX {
        return Err(XrpcError::invalid("Error: q takes at most 100 characters"));
    }
    let after = match p.one("cursor").filter(|_| !typeahead) {
        Some(c) => Some(SearchKey::parse(c).ok_or_else(|| XrpcError::invalid("Error: malformed cursor"))?),
        None => None,
    };
    let folded = search_fold(term);
    if folded.is_empty() {
        metrics::counter!(METRIC_READS, "method" => label, "outcome" => "empty").increment(1);
        return visitor_state(s, &caller, Ok(Json(crate::hydration::Hydrator::refract_empty("actors")))).await;
    }
    let h = &s.app.hydrator;
    let snapshot = h.refract_snapshot(&caller.req).await;
    let (dids, next) = h
        .refract_search_page(&served(&snapshot), &folded, after.as_ref(), usize::try_from(limit).unwrap_or(100))
        .await
        .map_err(|e| XrpcError::from(crate::hydration::HydrationError::from(e)))?;
    let next = if typeahead { None } else { next.map(|k| k.cursor()) };
    let mut blocks = Blocks::new(s, &caller);
    if let Some(b) = blocks.as_mut() {
        b.round(&dids).await;
    }
    // RF-S11: an account left unanswered for a visitor is held back, and
    // said so.
    let withheld: Vec<(String, &'static str)> = dids.iter().filter(|d| blocks.as_ref().is_some_and(|b| b.unanswered(d))).map(|d| (d.clone(), "unanswered")).collect();
    let kind = if typeahead { ProfileKind::Basic } else { ProfileKind::View };
    let result = with_blocks(&mut blocks, &dids, || async {
        h.refract_search_actors(&caller.viewer, &caller.req, &dids, kind, next.as_deref(), &withheld).await.map(Json).map_err(Into::into)
    })
    .await;
    let result = visitor_state(s, &caller, result).await;
    metrics::counter!(METRIC_READS, "method" => label, "outcome" => if result.is_ok() { "ok" } else { "error" }).increment(1);
    result
}

/// R-live less the DIDs still waiting for their first labels: what a
/// Refract read serves.
fn served(snapshot: &RefractSnapshot) -> Vec<String> {
    snapshot.restored.keys().filter(|d| !snapshot.awaiting.contains(*d)).cloned().collect()
}

/// `refract.getTimeline` (§7.1): member-only data; a non-member gets an
/// empty feed.
pub async fn get_timeline(State(s): State<RefractState>, ConnectInfo(peer): ConnectInfo<SocketAddr>, headers: HeaderMap, RawQuery(q): RawQuery) -> XrpcResult {
    let caller = gate(&s, &headers, peer, GET_TIMELINE).await?;
    let p = Params::parse(q.as_deref());
    let limit = p.u32("limit")?.unwrap_or(50).clamp(1, 100);
    let cursor = p.one("cursor").map(str::to_string);
    if !caller.member {
        metrics::counter!(METRIC_READS, "method" => "getTimeline", "outcome" => "not_member").increment(1);
        return visitor_state(&s, &caller, Ok(Json(crate::hydration::Hydrator::refract_empty("feed")))).await;
    }
    let h = &s.app.hydrator;
    let hydration = |e: sqlx::Error| XrpcError::from(crate::hydration::HydrationError::from(e));
    let snapshot = h.refract_snapshot(&caller.req).await;
    let (items, next) = h.refract_timeline_items(&caller.did, &served(&snapshot), cursor.as_deref(), limit).await.map_err(hydration)?;
    let local: Vec<String> = items.iter().filter(|i| i.kind != "repost").map(|i| i.post_uri.clone()).collect();
    s.refract.serve_check(&caller.req, &local).await;
    let mut blocks = Blocks::new(&s, &caller);
    let named: Vec<String> = items.iter().flat_map(|i| [i.originator_did.clone()].into_iter().chain(uri_author(&i.post_uri).map(str::to_string))).collect();
    let result = with_blocks(&mut blocks, &named, || async {
        h.refract_feed(&caller.viewer, &caller.req, &items, next.as_deref(), FEED_SUBJECT_CAP).await.map(Json).map_err(Into::into)
    })
    .await;
    let result = visitor_state(&s, &caller, result).await;
    metrics::counter!(METRIC_READS, "method" => "getTimeline", "outcome" => if result.is_ok() { "ok" } else { "error" }).increment(1);
    result
}

/// `refract.getReplies` (§7.1): the restored replies in the thread of
/// `root`, for anyone signed in.
pub async fn get_replies(State(s): State<RefractState>, ConnectInfo(peer): ConnectInfo<SocketAddr>, headers: HeaderMap, RawQuery(q): RawQuery) -> XrpcResult {
    let caller = gate(&s, &headers, peer, GET_REPLIES).await?;
    let p = Params::parse(q.as_deref());
    let root = p.required("root")?.to_string();
    let anchor = p.required("anchor")?.to_string();
    if !at_uri(&root) || !at_uri(&anchor) {
        return Err(XrpcError::invalid("Error: anchor and root must be at-uris"));
    }
    let h = &s.app.hydrator;
    let snapshot = h.refract_snapshot(&caller.req).await;
    let rows = h.refract_reply_rows(&root, &served(&snapshot)).await.map_err(|e| XrpcError::from(crate::hydration::HydrationError::from(e)))?;
    let uris: Vec<String> = rows.iter().map(|(u, _)| u.clone()).collect();
    s.refract.serve_check(&caller.req, &uris).await;
    let mut blocks = Blocks::new(&s, &caller);
    let result = h.refract_replies(&caller.viewer, &caller.req, &root, rows, blocks.as_mut(), s.round_timeout).await.map(Json).map_err(Into::into);
    let result = visitor_state(&s, &caller, result).await;
    metrics::counter!(METRIC_READS, "method" => "getReplies", "outcome" => if result.is_ok() { "ok" } else { "error" }).increment(1);
    result
}

/// `refract.listNotifications` (§5.3, §7.1): member-only data; a
/// non-member gets an empty list.
pub async fn list_notifications(State(s): State<RefractState>, ConnectInfo(peer): ConnectInfo<SocketAddr>, headers: HeaderMap, RawQuery(q): RawQuery) -> XrpcResult {
    let caller = gate(&s, &headers, peer, LIST_NOTIFICATIONS).await?;
    let p = Params::parse(q.as_deref());
    let limit = p.u32("limit")?.unwrap_or(50).clamp(1, 100);
    let cursor = p.one("cursor").map(str::to_string);
    let mut blocks = Blocks::new(&s, &caller);
    let result = respond(
        &caller.req,
        s.app.hydrator.refract_list_notifications(&caller.viewer, &caller.req, caller.member, limit, cursor.as_deref(), blocks.as_mut()).await,
    );
    let result = served_as(&caller, result);
    metrics::counter!(METRIC_READS, "method" => "listNotifications", "outcome" => if result.is_ok() { "ok" } else { "error" }).increment(1);
    result
}

/// `refract.getUnreadCount` (§5.3, §7.1): a non-member counts 0.
pub async fn get_unread_count(State(s): State<RefractState>, ConnectInfo(peer): ConnectInfo<SocketAddr>, headers: HeaderMap, RawQuery(q): RawQuery) -> XrpcResult {
    let caller = gate(&s, &headers, peer, GET_UNREAD_COUNT).await?;
    let p = Params::parse(q.as_deref());
    let seen_at = match p.one("seenAt") {
        Some(t) => Some(
            chrono::DateTime::parse_from_rfc3339(t)
                .map_err(|_| XrpcError::invalid("Error: seenAt must be a datetime"))?
                .with_timezone(&Utc),
        ),
        None => None,
    };
    let mut blocks = Blocks::new(&s, &caller);
    let result = respond(&caller.req, s.app.hydrator.refract_unread_count(&caller.viewer, &caller.req, caller.member, seen_at, blocks.as_mut()).await);
    let result = served_as(&caller, result);
    metrics::counter!(METRIC_READS, "method" => "getUnreadCount", "outcome" => if result.is_ok() { "ok" } else { "error" }).increment(1);
    result
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn caps_and_address_keys() {
        let d = Discovery::default();
        assert!(d.take(Cap::Ip, "a", 2) && d.take(Cap::Ip, "a", 2));
        assert!(!d.take(Cap::Ip, "a", 2));
        assert!(d.take(Cap::Ip, "b", 2), "per key");
        d.give_back(Cap::Ip, "a");
        assert!(d.take(Cap::Ip, "a", 2));
        assert!(d.take(Cap::Global, "", 1) && !d.take(Cap::Global, "x", 1));
        assert_eq!(ip_key("2001:db8:1:2:3:4:5:6".parse().expect("ip")), "2001:db8:1:2::/64");
        assert_eq!(ip_key("192.0.2.1".parse().expect("ip")), "192.0.2.1");
    }

    /// RF-S6: remembered against one communities list; any change of the
    /// list forgets everything, and an outcome judged against an older list
    /// never survives into a newer one.
    #[test]
    fn not_hosted_is_forgotten_when_the_communities_change() {
        let a = super::super::Communities::new(vec![crate::config::RefractCommunity::example()]);
        let b = super::super::Communities::default();
        let n = NotHosted::default();
        assert!(!n.contains("did:plc:x", &a));
        n.insert("did:plc:x", &a);
        assert!(n.contains("did:plc:x", &a) && !n.contains("did:plc:y", &a));
        assert!(!n.contains("did:plc:x", &b), "a change clears");
        assert!(!n.contains("did:plc:x", &a), "and changing back does not restore it");
        // Judged against `a` while `b` is live: gone at the next look.
        n.insert("did:plc:z", &a);
        assert!(!n.contains("did:plc:z", &b));
        // Expiry.
        let short = NotHosted::new(10, Duration::from_millis(50));
        short.insert("did:plc:x", &a);
        std::thread::sleep(Duration::from_millis(120));
        assert!(!short.contains("did:plc:x", &a));
    }

    /// RF-S15: an in-flight DID can't be claimed twice; a failure is
    /// remembered for its moment, a success not at all; a change of the
    /// communities clears both.
    #[test]
    fn resolving_memo() {
        let a = super::super::Communities::new(vec![crate::config::RefractCommunity::example()]);
        let b = super::super::Communities::default();
        let r = Resolving::new(Duration::from_millis(80));
        assert!(!r.contains("did:plc:x", &a));
        assert!(r.begin("did:plc:x", &a));
        assert!(r.contains("did:plc:x", &a) && !r.begin("did:plc:x", &a), "in flight");
        r.finish("did:plc:x", &a, false);
        assert!(!r.contains("did:plc:x", &a), "a success is forgotten");
        assert!(r.begin("did:plc:x", &a));
        r.finish("did:plc:x", &a, true);
        assert!(r.contains("did:plc:x", &a) && !r.begin("did:plc:x", &a), "a failure is remembered");
        std::thread::sleep(Duration::from_millis(150));
        assert!(!r.contains("did:plc:x", &a), "for a moment only");
        assert!(r.begin("did:plc:y", &a));
        assert!(!r.contains("did:plc:y", &b), "a change clears");
        // A failure judged against the old list is not kept into the new one.
        r.finish("did:plc:y", &a, true);
        assert!(!r.contains("did:plc:y", &b));
    }

    /// `fill` drops malformed items and repeats and keeps the rest.
    #[test]
    fn fill_items_drop_malformed() {
        let good = "at://did:plc:aaaaaaaaaaaaaaaaaaaaaaaa/app.bsky.feed.post/3k".to_string();
        let (dids, uris) = valid_fill_items(
            vec!["did:plc:aaaaaaaaaaaaaaaaaaaaaaaa".into(), "not a did".into(), "did:plc:aaaaaaaaaaaaaaaaaaaaaaaa".into()],
            vec![good.clone(), "https://example.com".into(), "at://did:plc:aaaaaaaaaaaaaaaaaaaaaaaa".into()],
        );
        assert_eq!(dids, ["did:plc:aaaaaaaaaaaaaaaaaaaaaaaa"]);
        assert_eq!(uris, [good]);
    }
}
