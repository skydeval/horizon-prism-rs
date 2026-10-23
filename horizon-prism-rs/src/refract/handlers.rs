//! `app.nearhorizon.refract.*` (refract design §7.1).
//!
//! - **Who may call** (C-3): any signed-in caller with a verified token. A
//!   ready member is served with member viewer state and its Farsight
//!   handle; anyone else, a member that is not ready included, as a
//!   **visitor**: no viewer state beyond the request's known blocks.
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
//!   `#bsky_appview`), per client address and in all.
//! - **Errors**: `NotEnabled` (the feature is off) and `AuthRequired`.

use std::collections::{HashMap, HashSet};
use std::net::{IpAddr, SocketAddr};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

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

/// `fill` takes at most this many DIDs and as many URIs.
pub const FILL_MAX: usize = 25;
/// Subjects not stored here fetched per feed page and per `fill`
/// (refract design §3.3).
pub const FEED_SUBJECT_CAP: usize = 25;
/// `refract_reads_total{method, outcome}`.
pub const METRIC_READS: &str = "refract_reads_total";
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
        .with_state(state)
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

/// The in-memory caps on candidate creation (§7.1 "Abuse bounds"), in
/// fixed hourly windows, reset at restart.
#[derive(Default)]
pub struct Discovery {
    windows: Mutex<Windows>,
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

impl RefractState {
    /// Discovery (§4.4) for the DIDs a `fill` names that are not in R-live:
    /// returns `pending`, every one accepted for checking (a candidate
    /// already, or one whose resolve was started now), plus R-live DIDs
    /// waiting for their first label fetch. Never waits for a resolve or a
    /// check, and reads no `actors` row (G-9).
    ///
    /// # Errors
    ///
    /// Database errors.
    pub async fn discover(&self, caller: &Caller, dids: &[String], snapshot: &RefractSnapshot) -> Result<Vec<String>, sqlx::Error> {
        let cfg = self.refract.cfg();
        let pool = self.refract.pool();
        let mut pending = Vec::new();
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
                // Restored but pending removal, rejected not yet
                // re-eligible, or ending: nothing to start.
                Some(_) => continue,
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
            if caller.visitor() {
                if !caller.direct {
                    metrics::counter!(METRIC_CANDIDATES, "source" => "fill", "outcome" => "no_fragment").increment(1);
                    continue;
                }
                let ip = ip_key(caller.ip);
                if !self.discovery.take(Cap::Ip, &ip, cfg.visitor_candidates_per_ip_per_hour) {
                    metrics::counter!(METRIC_CANDIDATES, "source" => "fill", "outcome" => "visitor_capped").increment(1);
                    continue;
                }
                if !self.discovery.take(Cap::Global, "", cfg.visitor_candidates_per_hour) {
                    self.discovery.give_back(Cap::Ip, &ip);
                    metrics::counter!(METRIC_CANDIDATES, "source" => "fill", "outcome" => "visitor_capped").increment(1);
                    continue;
                }
            }
            if !self.discovery.take(Cap::Caller, &caller.did, cfg.candidates_per_caller_per_hour) {
                metrics::counter!(METRIC_CANDIDATES, "source" => "fill", "outcome" => "capped").increment(1);
                continue;
            }
            // A resolve the resolver's cache cannot answer counts.
            let cached = self.refract.resolver().cache().lookup(did).await.ok().flatten().is_some();
            if !cached && !self.discovery.take(Cap::Resolve, &caller.did, cfg.resolves_per_caller_per_hour) {
                self.discovery.give_back(Cap::Caller, &caller.did);
                metrics::counter!(METRIC_CANDIDATES, "source" => "fill", "outcome" => "resolve_capped").increment(1);
                continue;
            }
            pending.push(did.clone());
            let refract = self.refract.clone();
            let did = did.clone();
            tokio::spawn(async move {
                let outcome = match refract.resolver().resolve_info(&did, ResolveIntent::Lookup).await {
                    Ok(info) => match info.pds_endpoint.as_deref().and_then(crate::constellation::matcher::pds_host) {
                        Some(host) => match refract.insert_candidate(&did, &host, "fill").await {
                            Ok(true) => "inserted",
                            Ok(false) => "not_hosted",
                            Err(error) => {
                                tracing::warn!(event = "refract_candidate_failed", did, %error, "inserting a candidate failed");
                                "error"
                            }
                        },
                        None => "not_hosted",
                    },
                    Err(_) => "not_hosted",
                };
                metrics::counter!(METRIC_CANDIDATES, "source" => "fill", "outcome" => outcome).increment(1);
            });
        }
        Ok(pending)
    }
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
    if !dids.iter().all(|d| proto_blue::syntax::Did::is_valid(d)) || !uris.iter().all(|u| at_uri(u)) {
        return Err(XrpcError::invalid("Error: dids must be DIDs and uris at-uris"));
    }
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
    let pending = s.discover(&caller, &named, &snapshot).await.map_err(|e| XrpcError::from(crate::hydration::HydrationError::from(e)))?;
    let served = |d: &str| snapshot.is_restored(d) && !snapshot.awaiting.contains(d);
    let profiles: Vec<String> = dids.iter().filter(|d| served(d)).cloned().collect();
    let posts: Vec<String> = uris.iter().filter(|u| uri_author(u).is_some_and(served)).cloned().collect();
    s.refract.serve_check(&caller.req, &posts).await;
    let mut blocks = Blocks::new(&s, &caller);
    let authors: Vec<String> = profiles.iter().cloned().chain(posts.iter().filter_map(|u| uri_author(u).map(str::to_string))).collect();
    if let Some(b) = blocks.as_mut() {
        b.round(&authors).await;
    }
    // A restored account left unanswered for a visitor: no profile.
    let profiles: Vec<String> = profiles.into_iter().filter(|d| !blocks.as_ref().is_some_and(|b| b.unanswered(d))).collect();
    let result = with_blocks(&mut blocks, &authors, || async {
        h.refract_fill(&caller.viewer, &caller.req, &profiles, &posts, &pending).await.map(Json).map_err(Into::into)
    })
    .await;
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
    metrics::counter!(METRIC_READS, "method" => "getAuthorFeed", "outcome" => if result.is_ok() { "ok" } else { "error" }).increment(1);
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
}
