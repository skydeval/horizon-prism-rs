//! The lookup: who blocks a viewer through a list, among the accounts on a
//! page (design-v3 §5.14.5, §5.14.6, §5.14.10).
//!
//! [`Farsight`] holds what every request shares: the client, the request
//! bucket with its pools and fair shares, the breaker of the read call, and
//! the per-viewer cache of pair answers. A request takes a [`Lookup`] from
//! it, which adds what belongs to that one request: its memo, how many
//! calls it may still make, and how long it may still wait.
//!
//! **No answer.** The feature not configured, the breaker open, an empty
//! pool or share, a spent allowance, a transport error, any status but 200,
//! an unreadable body, an account past the call cap: in each case the
//! affected accounts simply have no inbound list block on this request, and
//! the request goes on. Nothing waits for a token, and nothing here ever
//! turns a response into a 429.

use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::{Duration, Instant};

use futures::future::{BoxFuture, join_all};

use super::answer::{Coverage, PairAnswer};
use super::chunks::{chunks, halves};
use super::client::{CallError, FarsightClient};
use super::did::canonical;
use super::gate::{Effect, GateRead, ReadGate};
use crate::config::FarsightConfig;
use crate::constellation::Class;
use crate::constellation::cascade::SharedBreaker;
use crate::hydration::ReadCache;
use crate::xrpc::pools::{ChargeKey, Pool, Requester, SharedBucket, Shares};

/// `farsight_calls_total{outcome}`.
pub const METRIC_CALLS: &str = "farsight_calls_total";
/// `farsight_coverage_total{level}`: the level of each answered call.
pub const METRIC_COVERAGE: &str = "farsight_coverage_total";
/// `farsight_partial_total{reason}`: `partialFor` entries by reason.
pub const METRIC_PARTIAL: &str = "farsight_partial_total";
/// `farsight_auth_failed_total`: a 401 or 403, so a revoked or
/// under-scoped key is visible.
pub const METRIC_AUTH_FAILED: &str = "farsight_auth_failed_total";
/// `farsight_prime_failed_total{reason}`: `requestBackfill` calls that did
/// not succeed, or were skipped while their breaker was open.
pub const METRIC_PRIME_FAILED: &str = "farsight_prime_failed_total";

#[derive(Debug, Clone, Copy)]
struct Settings {
    timeout: Duration,
    ttl: Duration,
    per_viewer: usize,
    total: usize,
    max_calls: u32,
}

struct Entry {
    answer: PairAnswer,
    expires: Instant,
}

/// One viewer's entries, with the order they were first written in.
#[derive(Default)]
struct ViewerCache {
    entries: HashMap<String, Entry>,
    order: VecDeque<String>,
}

/// The per-viewer cache of pair answers (§5.14.6). An entry is read only
/// for the viewer it was written for. Its lifetime runs from Farsight's
/// answer: a read does not extend it.
#[derive(Default)]
struct Cache {
    viewers: HashMap<String, ViewerCache>,
    total: usize,
}

impl Cache {
    fn get(&mut self, viewer: &str, other: &str, now: Instant) -> Option<PairAnswer> {
        let v = self.viewers.get_mut(viewer)?;
        match v.entries.get(other) {
            Some(e) if e.expires > now => Some(e.answer.clone()),
            Some(_) => {
                v.entries.remove(other);
                self.total -= 1;
                None
            }
            None => None,
        }
    }

    /// Write `new` for the pair under the merge rule, and return what the
    /// entry now holds.
    fn write(&mut self, viewer: &str, other: &str, new: &PairAnswer, now: Instant, s: &Settings) -> PairAnswer {
        let live = self.get(viewer, other, now);
        let (merged, restart) = PairAnswer::written_over(new, live.as_ref());
        let v = self.viewers.entry(viewer.to_string()).or_default();
        let expires = match v.entries.get(other) {
            Some(e) if !restart => e.expires,
            _ => now + s.ttl,
        };
        let fresh = v
            .entries
            .insert(
                other.to_string(),
                Entry {
                    answer: merged.clone(),
                    expires,
                },
            )
            .is_none();
        if fresh {
            v.order.push_back(other.to_string());
            self.total += 1;
        }
        // One viewer's bound: the oldest entries go first.
        while v.entries.len() > s.per_viewer {
            let Some(oldest) = v.order.pop_front() else { break };
            if oldest != other && v.entries.remove(&oldest).is_some() {
                self.total -= 1;
            }
        }
        // Keys that expired stay in `order` until popped; keep it bounded.
        if v.order.len() > 2 * s.per_viewer + 16 {
            v.order.retain(|k| v.entries.contains_key(k));
        }
        // The bound across viewers: other viewers' entries go, in no
        // particular order. An evicted entry is simply absent.
        while self.total > s.total {
            let Some(victim) = self.viewers.keys().find(|k| k.as_str() != viewer).cloned() else {
                break;
            };
            if let Some(gone) = self.viewers.remove(&victim) {
                self.total -= gone.entries.len();
            }
        }
        merged
    }

    fn forget(&mut self, viewer: &str) {
        if let Some(gone) = self.viewers.remove(viewer) {
            self.total -= gone.entries.len();
        }
    }
}

struct Inner {
    client: FarsightClient,
    settings: Settings,
    bucket: SharedBucket,
    /// The read breaker with the state of the run (§5.14.17.2).
    gate: ReadGate,
    /// The breaker of `requestBackfill`, apart from the read breaker.
    prime_breaker: SharedBreaker,
    prime_on_enroll: bool,
    cache: Mutex<Cache>,
}

/// The Farsight lookup engine. Cheap to clone.
#[derive(Clone)]
pub struct Farsight {
    inner: Arc<Inner>,
}

impl std::fmt::Debug for Farsight {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Farsight").finish_non_exhaustive()
    }
}

impl Farsight {
    /// The engine for a validated `[farsight]`, with its request bucket
    /// split into pools and shares like every shared bucket.
    #[must_use]
    pub fn new(client: FarsightClient, cfg: &FarsightConfig, shares: &Shares) -> Self {
        let size = |n: u64| usize::try_from(n).unwrap_or(usize::MAX);
        Self {
            inner: Arc::new(Inner {
                client,
                settings: Settings {
                    timeout: Duration::from_millis(cfg.timeout_ms),
                    ttl: Duration::from_secs(cfg.cache_ttl_secs),
                    per_viewer: size(cfg.cache_entries_per_viewer),
                    total: size(cfg.cache_entries_total),
                    max_calls: cfg.max_calls_per_request,
                },
                bucket: shares.bucket_with_share("farsight", cfg.rate_per_sec.0, cfg.burst, cfg.requester_share.0),
                gate: ReadGate::default(),
                prime_breaker: SharedBreaker::default(),
                prime_on_enroll: cfg.prime_on_enroll,
                cache: Mutex::new(Cache::default()),
            }),
        }
    }

    fn cache(&self) -> std::sync::MutexGuard<'_, Cache> {
        self.inner.cache.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// The handle of one request, charged to `req`'s key.
    #[must_use]
    pub fn request(&self, req: &Requester) -> Lookup {
        Lookup {
            farsight: self.clone(),
            key: req.key().clone(),
            state: Arc::new(Mutex::new(State {
                memo: HashMap::new(),
                calls_left: self.inner.settings.max_calls,
                allowance: self.inner.settings.timeout,
            })),
            cache_only: Arc::new(AtomicBool::new(false)),
        }
    }

    /// Drop a viewer's cached answers (a removed member, a deleted account).
    pub fn forget(&self, viewer: &str) {
        let viewer = canonical(viewer).unwrap_or_else(|| viewer.to_string());
        self.cache().forget(&viewer);
    }

    /// Ask Farsight to read a member's own repository (design-v3 §5.14.11),
    /// so a new member's blocks and list blocks are current there from
    /// enrollment. Best effort: the outcome is counted and nothing waits on
    /// it. It has a breaker of its own, which gates only this call: its
    /// refusals are ordinary during mass enrollment and say nothing about
    /// whether reads work.
    ///
    /// # Errors
    ///
    /// The counted reason.
    pub async fn prime(&self, actor: &str, force: bool) -> Result<(), &'static str> {
        let failed = |reason: &'static str| {
            metrics::counter!(METRIC_PRIME_FAILED, "reason" => reason).increment(1);
            Err(reason)
        };
        let Some(actor) = canonical(actor) else {
            return failed("invalid_did");
        };
        let Some(admission) = self.inner.prime_breaker.admit(Instant::now()) else {
            return failed("breaker_open");
        };
        match self.inner.client.request_backfill(&actor, force).await {
            Ok(()) => {
                admission.record(Class::Hit, Instant::now());
                Ok(())
            }
            Err(reason) => {
                admission.record(Class::Transient, Instant::now());
                failed(reason)
            }
        }
    }

    /// Farsight's status in brief, for `admin farsight status`: its
    /// version, firehose, sweep and coverage as it reports them, and
    /// whether this AppView's read breaker is open.
    ///
    /// # Errors
    ///
    /// A short reason when Farsight did not answer.
    pub async fn status(&self) -> Result<serde_json::Value, &'static str> {
        let stats = self.inner.client.get_stats().await?;
        let mut brief = serde_json::Map::new();
        for key in ["version", "firehose", "sweep", "coverage", "freshness"] {
            if let Some(v) = stats.get(key) {
                brief.insert(key.to_string(), v.clone());
            }
        }
        brief.insert("read_breaker_open".to_string(), serde_json::Value::Bool(self.breaker_open()));
        brief.insert("cached_pair_answers".to_string(), serde_json::json!(self.cached()));
        Ok(serde_json::Value::Object(brief))
    }

    /// Whether enrolling a member should prime Farsight
    /// (`farsight.prime_on_enroll`).
    #[must_use]
    pub fn prime_on_enroll(&self) -> bool {
        self.inner.prime_on_enroll
    }

    /// One read of the gate: whether Farsight is down now (it has answered
    /// this process, its read breaker is open, and nothing but service
    /// failures came since its last answer), and its answer count.
    #[must_use]
    pub fn gate(&self) -> GateRead {
        self.inner.gate.read(Instant::now())
    }

    /// Whether the read breaker is open now.
    #[must_use]
    pub fn breaker_open(&self) -> bool {
        self.inner.gate.is_open(Instant::now())
    }

    /// Cached entries across all viewers.
    #[must_use]
    pub fn cached(&self) -> usize {
        self.cache().total
    }
}

struct State {
    /// What this request knows about each pair, by canonical DID: an
    /// answer, or `None` for "no answer". Once a pair is here, nothing
    /// later in the request changes what is enforced for it.
    memo: HashMap<String, Option<PairAnswer>>,
    /// Chunks the request may still ask about.
    calls_left: u32,
    /// How long the request may still wait on Farsight. The clock runs
    /// only while a call of the request is in flight.
    allowance: Duration,
}

/// One request's handle on Farsight. A hydration path that is given none
/// makes no lookup and reads no cache: a background job, the CLI and the
/// private tier never reach Farsight by omission. Cheap to clone; clones
/// share the request's memo, call cap and allowance.
#[derive(Clone)]
pub struct Lookup {
    farsight: Farsight,
    key: ChargeKey,
    state: Arc<Mutex<State>>,
    cache_only: Arc<AtomicBool>,
}

impl std::fmt::Debug for Lookup {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Lookup").field("key", &self.key).finish_non_exhaustive()
    }
}

/// An asked account with its answer, or `None` for "no answer".
type Outcome = (String, Option<PairAnswer>);

/// What one `checkBlocks` call (or its two halves) gave: an outcome per
/// asked account, and the coverage level of each answered response.
#[derive(Debug, Default)]
struct Called {
    outcomes: Vec<Outcome>,
    levels: Vec<Coverage>,
    /// Every part of the call was answered (both halves, when it was
    /// asked again as two).
    answered: bool,
}

/// What a caller with budgets of its own may spend on one lookup
/// (design-v3 §5.14.9): Thread Lens brings its own, per verdict round.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PairBudget {
    /// `checkBlocks` calls, at most.
    pub max_calls: u32,
    /// How long the calls may take together, and each alone.
    pub timeout: Duration,
}

/// The pair answers of one lookup, keyed as the caller spelled the
/// accounts, and the coverage level of each call that was answered.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PairAnswers {
    pub answers: HashMap<String, PairAnswer>,
    pub levels: Vec<Coverage>,
    /// `checkBlocks` calls the lookup made.
    pub calls: usize,
    /// The lookup made at least one call, every call was answered whole,
    /// and every answer's level was `complete` or `assisted`: Farsight
    /// vouches for the viewer's side (design-v3 §5.15.4).
    pub vouches: bool,
}

impl Lookup {
    /// From here on this request answers from the viewer's cache only and
    /// makes no call. For a listing whose accounts come from the viewer's
    /// private state (its mutes, its bookmarks), so that Farsight never
    /// receives a viewer beside them; and for per-keystroke surfaces.
    pub fn set_cache_only(&self) {
        self.cache_only.store(true, Ordering::Relaxed);
    }

    /// How many accounts this request may still ask about, at most.
    #[must_use]
    pub fn capacity(&self) -> usize {
        usize::try_from(self.state().calls_left).unwrap_or(usize::MAX).saturating_mul(super::chunks::MAX_ACCOUNTS)
    }

    fn state(&self) -> std::sync::MutexGuard<'_, State> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// The ordinary reader: of the accounts on `page`, in page order, the
    /// ones that block `viewer` through a list, each with the list's URI.
    /// The viewer is never its own blocker, and `direct` (accounts with a
    /// direct block either way, which are excluded already) are not asked
    /// about. Keys are the DIDs as `page` spells them.
    pub async fn inbound_list_blocks(&self, viewer: &str, page: &[String], direct: &HashSet<String>) -> HashMap<String, String> {
        let Some(actor) = canonical(viewer) else {
            return HashMap::new();
        };
        // Canonical forms in page order, once each. A DID outside
        // Farsight's grammar has no inbound list block.
        let mut wanted: Vec<String> = Vec::new();
        let mut seen: HashSet<String> = HashSet::new();
        for did in page {
            if did == viewer || direct.contains(did) {
                continue;
            }
            if let Some(c) = canonical(did)
                && c != actor
                && seen.insert(c.clone())
            {
                wanted.push(c);
            }
        }
        let found = self.pairs(&actor, &wanted).await;
        page.iter()
            .filter_map(|did| {
                let list = found.get(&canonical(did)?)?.inbound.list.clone()?;
                Some((did.clone(), list))
            })
            .collect()
    }

    /// `getRelationships`'s reader (§5.14.8): for each of `others`, the
    /// first list through which it blocks `actor`, keyed as `others` spells
    /// it. The second value is false when some account got no answer.
    ///
    /// This is a public endpoint that anyone can call about a member, so it
    /// reads and writes only shared entries of the read-time cache `rt`,
    /// never a viewer's own cache: otherwise it would answer faster exactly
    /// when the member had recently seen the account. It does not skip
    /// accounts with a direct block.
    pub async fn relationship_lists(&self, actor: &str, others: &[String], rt: &ReadCache) -> (HashMap<String, String>, bool) {
        let Some(canonical_actor) = canonical(actor) else {
            return (HashMap::new(), false);
        };
        let inner = &self.farsight.inner;
        let ttl_ms = i64::try_from(inner.settings.ttl.as_millis()).unwrap_or(i64::MAX);
        let key = |other: &str| format!("rt:farsight-pair:{canonical_actor}:{other}");
        let mut wanted: Vec<String> = Vec::new();
        for other in others {
            if let Some(c) = canonical(other)
                && c != canonical_actor
                && !wanted.contains(&c)
            {
                wanted.push(c);
            }
        }
        let now_ms = chrono::Utc::now().timestamp_millis();
        let mut found: HashMap<String, PairAnswer> = HashMap::new();
        let mut ask: Vec<String> = Vec::new();
        for other in &wanted {
            match rt.get_json::<(i64, PairAnswer)>(&key(other)).await {
                Some((at, answer)) if now_ms.saturating_sub(at) < ttl_ms => {
                    found.insert(other.clone(), answer);
                }
                _ => ask.push(other.clone()),
            }
        }
        let mut complete = true;
        if !ask.is_empty() {
            let (plan, allowance) = {
                let mut state = self.state();
                let mut plan = chunks(&canonical_actor, &ask);
                let allowed = usize::try_from(state.calls_left).unwrap_or(usize::MAX);
                complete &= plan.len() <= allowed;
                plan.truncate(allowed);
                state.calls_left -= u32::try_from(plan.len()).unwrap_or(state.calls_left).min(state.calls_left);
                (plan, state.allowance)
            };
            let started = Instant::now();
            let per_call = inner.settings.timeout;
            let outcomes: Vec<Outcome> = join_all(plan.into_iter().map(|chunk| self.call(&canonical_actor, chunk, allowance, started, true, per_call)))
                .await
                .into_iter()
                .flat_map(|called| called.outcomes)
                .collect();
            {
                let mut state = self.state();
                state.allowance = state.allowance.saturating_sub(started.elapsed());
            }
            let at = chrono::Utc::now().timestamp_millis();
            for (other, outcome) in outcomes {
                match outcome {
                    Some(answer) => {
                        rt.put_json(&key(&other), &(at, &answer)).await;
                        found.insert(other, answer);
                    }
                    None => complete = false,
                }
            }
        }
        let lists = others
            .iter()
            .filter_map(|other| {
                let list = found.get(&canonical(other)?)?.inbound.list.clone()?;
                Some((other.clone(), list))
            })
            .collect();
        (lists, complete)
    }

    /// Pair answers for `others` (canonical DIDs, in page order): from the
    /// request's memo, then the viewer's cache, then Farsight. An account
    /// with no answer is absent.
    async fn pairs(&self, actor: &str, others: &[String]) -> HashMap<String, PairAnswer> {
        let inner = &self.farsight.inner;
        let cache_only = self.cache_only.load(Ordering::Relaxed);
        let mut found: HashMap<String, PairAnswer> = HashMap::new();
        let mut ask: Vec<String> = Vec::new();
        let (plan, allowance) = {
            let now = Instant::now();
            let mut state = self.state();
            let mut cache = self.farsight.cache();
            for other in others {
                if let Some(known) = state.memo.get(other) {
                    if let Some(answer) = known {
                        found.insert(other.clone(), answer.clone());
                    }
                } else if let Some(answer) = cache.get(actor, other, now) {
                    state.memo.insert(other.clone(), Some(answer.clone()));
                    found.insert(other.clone(), answer);
                } else {
                    ask.push(other.clone());
                }
            }
            // A cache-only miss is not memoized, so it never suppresses a
            // real lookup later in the request.
            if ask.is_empty() || cache_only {
                return found;
            }
            let mut plan = chunks(actor, &ask);
            let allowed = usize::try_from(state.calls_left).unwrap_or(usize::MAX);
            // Accounts past the call cap get no answer.
            for skipped in plan.iter().skip(allowed).flatten() {
                state.memo.insert(skipped.clone(), None);
            }
            plan.truncate(allowed);
            state.calls_left -= u32::try_from(plan.len()).unwrap_or(state.calls_left).min(state.calls_left);
            (plan, state.allowance)
        };
        let started = Instant::now();
        let per_call = inner.settings.timeout;
        let outcomes: Vec<Outcome> = join_all(plan.into_iter().map(|chunk| self.call(actor, chunk, allowance, started, true, per_call)))
            .await
            .into_iter()
            .flat_map(|called| called.outcomes)
            .collect();
        let now = Instant::now();
        let mut state = self.state();
        state.allowance = state.allowance.saturating_sub(started.elapsed());
        let mut cache = self.farsight.cache();
        for (other, outcome) in outcomes {
            // The memo holds the merged entry, so a block the cache already
            // named is not lost to an answer that does not vouch.
            let merged = outcome.map(|answer| cache.write(actor, &other, &answer, now, &inner.settings));
            if let Some(answer) = &merged {
                found.insert(other.clone(), answer.clone());
            }
            state.memo.insert(other, merged);
        }
        found
    }

    /// The second reader's entry point (design-v3 §5.14.9): the pair
    /// answers, both directions, for `others` as seen from `viewer`, with
    /// the coverage level of each call. Thread Lens reads them under its
    /// own rules, with the budget it brings.
    ///
    /// - It shares the request's memo and the viewer's cache with the
    ///   ordinary reader. A cached entry that vouches settles its account
    ///   with no call. Any other account is sent once per request; if
    ///   Farsight then gives no answer, a cached entry that does not vouch
    ///   is what is returned, so a block it names still applies.
    /// - A DID outside Farsight's grammar gets an answer with no block
    ///   that does not vouch.
    /// - Nothing is removed from `others`: not accounts with a direct
    ///   block, and not `viewer` itself, which a caller may include to
    ///   read the level of a call when nobody else is asked about. There
    ///   is no pair answer for `viewer`.
    /// - `force` names one account that is asked about although an entry
    ///   for it is held.
    pub async fn pair_answers(&self, viewer: &str, others: &[String], budget: PairBudget, force: Option<&str>) -> PairAnswers {
        let mut out = PairAnswers::default();
        let Some(actor) = canonical(viewer) else {
            return out;
        };
        let inner = &self.farsight.inner;
        let forced = force.and_then(canonical);
        let mut wanted: Vec<String> = Vec::new();
        let mut seen: HashSet<String> = HashSet::new();
        for did in others {
            match canonical(did) {
                Some(c) => {
                    if seen.insert(c.clone()) {
                        wanted.push(c);
                    }
                }
                None => {
                    out.answers.insert(did.clone(), PairAnswer::clean(false));
                }
            }
        }
        // The request memo settles an account for the whole request: once
        // it holds anything for one, an answer or "no answer", that
        // account is not sent again by a later round. A cached entry
        // settles one only if it vouches; one that does not is sent, and
        // stands (with any block it names) if Farsight gives no answer.
        let mut ask: Vec<String> = Vec::new();
        let mut standing: HashMap<String, PairAnswer> = HashMap::new();
        {
            let now = Instant::now();
            let mut state = self.state();
            let mut cache = self.farsight.cache();
            for other in &wanted {
                if *other == actor || forced.as_ref() == Some(other) {
                    ask.push(other.clone());
                } else if !state.memo.contains_key(other) {
                    match cache.get(&actor, other, now) {
                        Some(entry) if entry.vouched => {
                            state.memo.insert(other.clone(), Some(entry));
                        }
                        entry => {
                            standing.extend(entry.map(|e| (other.clone(), e)));
                            ask.push(other.clone());
                        }
                    }
                }
            }
        }
        if !ask.is_empty() && !self.cache_only.load(Ordering::Relaxed) {
            let mut plan = chunks(&actor, &ask);
            // An account the budget does not reach is not judged at all:
            // nothing is held for it, and a later round may send it.
            plan.truncate(usize::try_from(budget.max_calls).unwrap_or(usize::MAX));
            let started = Instant::now();
            let called = join_all(plan.into_iter().map(|chunk| self.call(&actor, chunk, budget.timeout, started, true, budget.timeout))).await;
            let now = Instant::now();
            let mut state = self.state();
            let mut cache = self.farsight.cache();
            out.calls = called.len();
            out.vouches = !called.is_empty() && called.iter().all(|call| call.answered && call.levels.iter().all(|level| level.vouches()));
            for call in called {
                out.levels.extend(call.levels);
                for (other, outcome) in call.outcomes {
                    if other == actor {
                        continue;
                    }
                    // An answer is merged into the cached entry, so a block
                    // the cache already named is not lost to one that does
                    // not vouch. With no answer the entry as it stood when
                    // the round was built is held, or "no answer".
                    let held = match outcome {
                        Some(answer) => Some(cache.write(&actor, &other, &answer, now, &inner.settings)),
                        None => standing.remove(&other).or_else(|| state.memo.get(&other).cloned().flatten()),
                    };
                    state.memo.insert(other, held);
                }
            }
        }
        let state = self.state();
        for did in others {
            if let Some(c) = canonical(did)
                && c != actor
                && let Some(Some(answer)) = state.memo.get(&c)
            {
                out.answers.insert(did.clone(), answer.clone());
            }
        }
        drop(state);
        out
    }

    /// One `checkBlocks` call for `chunk`, within what is left of
    /// `allowance` since `started`, and no longer than `per_call`. An
    /// answer over the response cap is asked again once, as two halves
    /// (`split`).
    fn call<'a>(
        &'a self,
        actor: &'a str,
        chunk: Vec<String>,
        allowance: Duration,
        started: Instant,
        split: bool,
        per_call: Duration,
    ) -> BoxFuture<'a, Called> {
        Box::pin(async move {
            let inner = &self.farsight.inner;
            let none = |chunk: Vec<String>, outcome: &'static str| {
                metrics::counter!(METRIC_CALLS, "outcome" => outcome).increment(1);
                Called { outcomes: chunk.into_iter().map(|did| (did, None)).collect(), levels: Vec::new(), answered: false }
            };
            // The admission is the probe-slot guard: dropped on any exit
            // below without a recorded outcome, it gives the slot back.
            let Some(admission) = inner.gate.admit(Instant::now()) else {
                return none(chunk, "breaker_open");
            };
            let Ok(pool) = inner.bucket.draw(&self.key) else {
                return none(chunk, "no_budget");
            };
            let remaining = allowance.saturating_sub(started.elapsed());
            if remaining.is_zero() {
                return none(chunk, "allowance_spent");
            }
            // A call that has its whole timeout and reaches it is a failure
            // of Farsight. One cut short by earlier calls is not. Decided
            // from the allowance this round began with: time has already
            // passed since `started`, so what remains is never the whole.
            // A half asked again after an over-cap answer (`!split`) only
            // has what the first call left, so it is never the whole.
            let whole = split && allowance >= per_call;
            match inner.client.check_blocks(actor, &chunk, remaining.min(per_call)).await {
                Ok(mut answer) => {
                    // An answer, whoever asked: a new, clean run starts.
                    inner.gate.record(admission, Some(Class::Hit), Effect::Answer, Instant::now());
                    metrics::counter!(METRIC_CALLS, "outcome" => "ok").increment(1);
                    metrics::counter!(METRIC_COVERAGE, "level" => answer.coverage.label()).increment(1);
                    for reason in std::mem::take(&mut answer.partial_reasons) {
                        metrics::counter!(METRIC_PARTIAL, "reason" => reason).increment(1);
                    }
                    let outcomes = chunk
                        .into_iter()
                        .map(|did| {
                            let pair = answer.pairs.remove(&did);
                            (did, pair)
                        })
                        .collect();
                    Called { outcomes, levels: vec![answer.coverage], answered: true }
                }
                Err(CallError::TooLarge) => {
                    // Big, not failing: not recorded by the breaker.
                    drop(admission);
                    metrics::counter!(METRIC_CALLS, "outcome" => "too_large").increment(1);
                    match halves(&chunk).filter(|_| split) {
                        Some((a, b)) => {
                            let (a, b) = futures::join!(
                                self.call(actor, a, allowance, started, false, per_call),
                                self.call(actor, b, allowance, started, false, per_call)
                            );
                            Called {
                                outcomes: a.outcomes.into_iter().chain(b.outcomes).collect(),
                                levels: a.levels.into_iter().chain(b.levels).collect(),
                                answered: a.answered && b.answered,
                            }
                        }
                        None => Called { outcomes: chunk.into_iter().map(|did| (did, None)).collect(), levels: Vec::new(), answered: false },
                    }
                }
                Err(error) => {
                    if error.is_auth() {
                        metrics::counter!(METRIC_AUTH_FAILED).increment(1);
                    }
                    let failing = match error {
                        CallError::Timeout => whole,
                        CallError::Transport | CallError::RefusedAddress | CallError::Parse => true,
                        CallError::RateLimited | CallError::Overloaded => true,
                        CallError::Status(status) => status >= 500 || matches!(status, 401 | 403 | 429),
                        CallError::TooLarge => false,
                    };
                    // What the outcome says about Farsight (§5.14.17.2):
                    // only a timeout of a call that had its whole time, a
                    // transport error and a 5xx are service failures.
                    let effect = match error {
                        CallError::Timeout if whole => Effect::ServiceFailure,
                        CallError::Timeout | CallError::TooLarge => Effect::None,
                        CallError::Transport | CallError::Overloaded => Effect::ServiceFailure,
                        CallError::Status(status) if status >= 500 => Effect::ServiceFailure,
                        CallError::RefusedAddress | CallError::RateLimited | CallError::Parse | CallError::Status(_) => Effect::Unclean,
                    };
                    // A failure of a call drawn on the non-member pool is
                    // not recorded, on the breaker or on the run: an
                    // anonymous caller chooses the accounts it asks about,
                    // and could choose slow ones.
                    if pool == Pool::Member {
                        inner.gate.record(admission, failing.then_some(Class::Transient), effect, Instant::now());
                    }
                    none(chunk, error.label())
                }
            }
        })
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use std::sync::atomic::AtomicUsize;

    use axum::Router;
    use axum::extract::{RawQuery, State};
    use axum::http::StatusCode;
    use axum::response::IntoResponse;
    use axum::routing::get;
    use serde_json::{Value, json};

    use super::*;
    use crate::farsight::chunks::CHECK_BLOCKS_PATH;
    use crate::net::NetClient;
    use crate::test_support::serve_fake;

    pub(crate) const V: &str = "did:plc:vvvvvvvvvvvvvvvvvvvvvvvv";
    const A: &str = "did:plc:aaaaaaaaaaaaaaaaaaaaaaaa";
    const B: &str = "did:plc:bbbbbbbbbbbbbbbbbbbbbbbb";
    const C: &str = "did:plc:cccccccccccccccccccccccc";
    pub(crate) const LIST: &str = "at://did:plc:llllllllllllllllllllllll/app.bsky.graph.list/1";

    /// A fake Farsight: who list-blocks the actor, and how it misbehaves.
    #[derive(Default)]
    pub(crate) struct Fake {
        pub blockers: Mutex<HashSet<String>>,
        pub level: Mutex<&'static str>,
        pub status: Mutex<u16>,
        pub delay_ms: Mutex<u64>,
        /// Answers naming more than this many accounts are over the cap.
        pub huge_over: Mutex<usize>,
        pub calls: AtomicUsize,
        pub asked: Mutex<Vec<Vec<String>>>,
    }

    async fn answer(State(fake): State<Arc<Fake>>, RawQuery(q): RawQuery) -> axum::response::Response {
        fake.calls.fetch_add(1, Ordering::SeqCst);
        let others: Vec<String> = url::form_urlencoded::parse(q.unwrap_or_default().as_bytes())
            .filter(|(k, _)| k == "others")
            .map(|(_, v)| v.into_owned())
            .collect();
        fake.asked.lock().expect("asked").push(others.clone());
        let delay = *fake.delay_ms.lock().expect("delay");
        if delay > 0 {
            tokio::time::sleep(Duration::from_millis(delay)).await;
        }
        let status = *fake.status.lock().expect("status");
        if status != 0 && status != 200 {
            return (StatusCode::from_u16(status).expect("status"), "{}").into_response();
        }
        let huge_over = *fake.huge_over.lock().expect("huge");
        if huge_over > 0 && others.len() > huge_over {
            return "x".repeat(5 * 1024 * 1024).into_response();
        }
        let blockers = fake.blockers.lock().expect("blockers");
        let results: Vec<Value> = others
            .iter()
            .filter(|o| blockers.contains(*o))
            .map(|o| json!({"did": o, "blocksActor": {"direct": false, "lists": [LIST]}, "blockedByActor": {"direct": false, "lists": []}}))
            .collect();
        let level = *fake.level.lock().expect("level");
        axum::Json(json!({"actor": V, "results": results, "partialFor": [], "freshness": {"coverage": {"level": if level.is_empty() { "complete" } else { level }}}})).into_response()
    }

    /// An engine over a fake Farsight, with `toml` as its `[farsight]`.
    pub(crate) async fn engine(toml: &str) -> (Farsight, Arc<Fake>) {
        let fake = Arc::new(Fake::default());
        let base = serve_fake(Router::new().route(CHECK_BLOCKS_PATH, get(answer)).with_state(Arc::clone(&fake))).await;
        let cfg = crate::config::FileConfig::parse(&format!("[farsight]\n{toml}")).expect("parse").farsight;
        let client = FarsightClient::new(NetClient::loopback_for_tests(), &base, "fsk_test").expect("client");
        (Farsight::new(client, &cfg, &Shares::for_tests()), fake)
    }

    fn member() -> Requester {
        Requester::new(ChargeKey::member(V))
    }

    fn page(dids: &[&str]) -> Vec<String> {
        dids.iter().map(ToString::to_string).collect()
    }

    fn calls(fake: &Fake) -> usize {
        fake.calls.load(Ordering::SeqCst)
    }

    #[tokio::test]
    async fn answers_come_from_the_memo_then_the_cache_then_farsight() {
        let (farsight, fake) = engine("").await;
        fake.blockers.lock().expect("blockers").insert(A.to_string());
        let none = HashSet::new();
        let request = farsight.request(&member());
        let blocked = request.inbound_list_blocks(V, &page(&[A, B, V, A]), &none).await;
        assert_eq!(blocked, HashMap::from([(A.to_string(), LIST.to_string())]));
        // The viewer is never asked about, and an account once only.
        assert_eq!(fake.asked.lock().expect("asked")[0], [A, B]);
        // The same request again: the memo. A new request: the cache.
        request.inbound_list_blocks(V, &page(&[B, A]), &none).await;
        let again = farsight.request(&member()).inbound_list_blocks(V, &page(&[A, B]), &none).await;
        assert_eq!((again.len(), calls(&fake)), (1, 1));
        // Only the uncached account is asked about, and an account with a
        // direct block is not asked about at all.
        let direct = HashSet::from([B.to_string()]);
        farsight.request(&member()).inbound_list_blocks(V, &page(&[A, B, C]), &direct).await;
        assert_eq!(fake.asked.lock().expect("asked")[1], [C]);
        // Another viewer has a cache of its own.
        let other = "did:plc:wwwwwwwwwwwwwwwwwwwwwwww";
        farsight.request(&Requester::new(ChargeKey::member(other))).inbound_list_blocks(other, &page(&[A]), &none).await;
        assert_eq!(calls(&fake), 3);
        // Forgetting a viewer drops its entries only.
        let before = farsight.cached();
        farsight.forget(V);
        assert_eq!(farsight.cached(), before - 3);
    }

    /// design-v3 §5.14.9: the second reader gets whole pair answers and
    /// the level of each call, under a budget of its own; it shares the
    /// cache with the ordinary reader, asks again about a pair that
    /// neither vouches nor names a block, and may force one call.
    #[tokio::test]
    async fn the_second_reader_gets_pair_answers_and_levels() {
        let (farsight, fake) = engine("").await;
        fake.blockers.lock().expect("blockers").insert(A.to_string());
        let budget = PairBudget { max_calls: 2, timeout: Duration::from_millis(500) };
        let request = farsight.request(&member());
        let got = request.pair_answers(V, &page(&[A, B, "did:example:odd"]), budget, None).await;
        assert_eq!(got.levels, [Coverage::parse(Some("complete"))]);
        assert_eq!(got.answers[A].inbound.list.as_deref(), Some(LIST));
        assert!(got.answers[A].vouched && got.answers[B].vouched && !got.answers[B].any_block());
        // A DID outside Farsight's grammar: no block, not vouched, not sent.
        assert!(!got.answers["did:example:odd"].vouched);
        assert_eq!(fake.asked.lock().expect("asked")[0], [A, B]);
        // Held now: no call, for this reader or the ordinary one.
        let again = farsight.request(&member());
        assert_eq!(again.pair_answers(V, &page(&[A, B]), budget, None).await.answers.len(), 2);
        again.inbound_list_blocks(V, &page(&[A, B]), &HashSet::new()).await;
        assert_eq!(calls(&fake), 1);
        // The viewer may be put among the others, to read a call's level:
        // it is sent, and has no pair answer. One account can be forced.
        let level = again.pair_answers(V, &page(&[V]), budget, None).await;
        assert!(level.answers.is_empty() && level.levels.len() == 1);
        again.pair_answers(V, &page(&[A, B]), budget, Some(B)).await;
        assert_eq!(fake.asked.lock().expect("asked")[2], [B]);

        // An answer that does not vouch and names no block is asked about
        // again; a block it names is kept.
        *fake.level.lock().expect("level") = "partial";
        let request = farsight.request(&member());
        let got = request.pair_answers(V, &page(&[C]), budget, None).await;
        assert!(!got.answers[C].vouched);
        // Within the request it is settled: a later round does not resend.
        request.pair_answers(V, &page(&[C]), budget, None).await;
        assert_eq!(fake.asked.lock().expect("asked").iter().filter(|chunk| chunk.as_slice() == [C]).count(), 1);
        farsight.request(&member()).pair_answers(V, &page(&[C]), budget, None).await;
        assert_eq!(fake.asked.lock().expect("asked").iter().filter(|chunk| chunk.as_slice() == [C]).count(), 2);
        // A cached block that does not vouch is sent again, and stands
        // when Farsight gives no answer.
        fake.blockers.lock().expect("blockers").insert(C.to_string());
        farsight.request(&member()).pair_answers(V, &page(&[C]), budget, None).await;
        *fake.status.lock().expect("status") = 503;
        let down = farsight.request(&member()).pair_answers(V, &page(&[C, B]), budget, None).await;
        assert!(down.levels.is_empty() && down.answers[C].inbound.list.is_some() && !down.answers[C].vouched);
        assert!(down.answers[B].vouched, "a vouching entry needs no call");
        *fake.status.lock().expect("status") = 200;
        // The budget bounds the calls: none allowed, none made, no answer.
        let before = calls(&fake);
        let none = farsight.request(&member()).pair_answers(V, &page(&["did:plc:dddddddddddddddddddddddd"]), PairBudget { max_calls: 0, ..budget }, None).await;
        assert!(none.answers.is_empty() && calls(&fake) == before);
    }

    /// B-81: the prime is counted, and its breaker is apart from the read
    /// breaker.
    #[tokio::test]
    async fn the_prime_has_a_breaker_of_its_own() {
        use axum::routing::post;

        use crate::farsight::client::REQUEST_BACKFILL_PATH;

        let hits = Arc::new(AtomicUsize::new(0));
        let seen = Arc::clone(&hits);
        let app = Router::new().route(
            REQUEST_BACKFILL_PATH,
            post(move |axum::Json(body): axum::Json<Value>| {
                let seen = Arc::clone(&seen);
                async move {
                    seen.fetch_add(1, Ordering::SeqCst);
                    if body["actor"] == A {
                        (StatusCode::ACCEPTED, axum::Json(json!({"enqueued": true}))).into_response()
                    } else {
                        (StatusCode::SERVICE_UNAVAILABLE, axum::Json(json!({"error": "QueueFull"}))).into_response()
                    }
                }
            }),
        );
        let base = serve_fake(app).await;
        let cfg = crate::config::FarsightConfig::default();
        let client = FarsightClient::new(NetClient::loopback_for_tests(), &base, "fsk_test").expect("client");
        let farsight = Farsight::new(client, &cfg, &Shares::for_tests());
        assert!(farsight.prime_on_enroll());
        assert_eq!(farsight.prime(A, false).await, Ok(()));
        assert_eq!(farsight.prime("did:key:z6Mk", false).await, Err("invalid_did"));
        for _ in 0..5 {
            assert_eq!(farsight.prime(B, false).await, Err("queue_full"));
        }
        // Its breaker is open: the next prime is skipped, and reads are not
        // affected.
        assert_eq!(farsight.prime(A, false).await, Err("breaker_open"));
        assert_eq!(hits.load(Ordering::SeqCst), 6);
        assert!(!farsight.breaker_open());
    }

    /// B-80: `getRelationships` reads Farsight through shared entries only.
    #[tokio::test]
    async fn relationships_use_the_shared_cache_and_never_a_viewers() {
        let (farsight, fake) = engine("").await;
        fake.blockers.lock().expect("blockers").insert(A.to_string());
        let rt = ReadCache::new(1024 * 1024);
        let anonymous = Requester::test();
        let (lists, complete) = farsight.request(&anonymous).relationship_lists(V, &page(&[A, B, V]), &rt).await;
        assert_eq!((lists, complete), (HashMap::from([(A.to_string(), LIST.to_string())]), true));
        assert_eq!(fake.asked.lock().expect("asked")[0], [A, B]);
        // A repeat is answered from the shared entries.
        farsight.request(&anonymous).relationship_lists(V, &page(&[B, A]), &rt).await;
        assert_eq!(calls(&fake), 1);
        // Nothing was written to the member's own cache, and the member's
        // render does not read the shared entries: it asks.
        assert_eq!(farsight.cached(), 0);
        farsight.request(&member()).inbound_list_blocks(V, &page(&[A]), &HashSet::new()).await;
        assert_eq!(calls(&fake), 2);
        // And the other way: a member's cached answer does not spare the
        // public endpoint its call.
        farsight.request(&member()).inbound_list_blocks(V, &page(&[C]), &HashSet::new()).await;
        farsight.request(&anonymous).relationship_lists(V, &page(&[C]), &rt).await;
        assert_eq!(calls(&fake), 4);
        // No answer: nothing listed, and the caller is told.
        *fake.status.lock().expect("status") = 503;
        let d = "did:plc:dddddddddddddddddddddddd";
        let (lists, complete) = farsight.request(&anonymous).relationship_lists(V, &page(&[d]), &rt).await;
        assert!(lists.is_empty() && !complete);
    }

    #[tokio::test]
    async fn dids_are_filtered_and_answers_mapped_back_to_their_spelling() {
        let (farsight, fake) = engine("").await;
        fake.blockers.lock().expect("blockers").insert("did:web:blocker.example.com".to_string());
        let spelled = ["did:web:Blocker.Example.COM", "did:key:z6Mk", "did:plc:short", "did:web:blocker.example.com"];
        let blocked = farsight.request(&member()).inbound_list_blocks(V, &page(&spelled), &HashSet::new()).await;
        // One call, in canonical form, without the DIDs Farsight rejects.
        assert_eq!(*fake.asked.lock().expect("asked"), [vec!["did:web:blocker.example.com".to_string()]]);
        assert_eq!(blocked.len(), 2);
        assert!(blocked.contains_key(spelled[0]) && blocked.contains_key(spelled[3]));
        // Nothing to ask: no call.
        farsight.request(&member()).inbound_list_blocks(V, &page(&["did:key:z6Mk", V]), &HashSet::new()).await;
        assert_eq!(calls(&fake), 1);
    }

    #[tokio::test]
    async fn cache_only_makes_no_call_and_the_cap_bounds_a_request() {
        let (farsight, fake) = engine("max_calls_per_request = 2\n").await;
        fake.blockers.lock().expect("blockers").insert(A.to_string());
        let none = HashSet::new();
        let quiet = farsight.request(&member());
        quiet.set_cache_only();
        assert!(quiet.inbound_list_blocks(V, &page(&[A]), &none).await.is_empty());
        assert_eq!(calls(&fake), 0);
        // Once a full request has cached the answer, cache-only reads it.
        farsight.request(&member()).inbound_list_blocks(V, &page(&[A]), &none).await;
        let quiet = farsight.request(&member());
        quiet.set_cache_only();
        assert_eq!(quiet.inbound_list_blocks(V, &page(&[A]), &none).await.len(), 1);
        assert_eq!(calls(&fake), 1);

        // 250 accounts are three chunks; the cap allows two, and the rest
        // get no answer for the whole request.
        let many: Vec<String> = (0..250).map(|i| format!("did:plc:{:a>24}", format!("{i:x}").replace(['0', '1', '8', '9'], "b"))).collect();
        let request = farsight.request(&member());
        request.inbound_list_blocks(V, &many, &none).await;
        assert_eq!(calls(&fake), 3);
        request.inbound_list_blocks(V, &many, &none).await;
        assert_eq!(calls(&fake), 3, "past-the-cap accounts are not asked about again in the request");
    }

    #[tokio::test]
    async fn an_unvouched_answer_never_removes_a_cached_block() {
        let (farsight, fake) = engine("").await;
        fake.blockers.lock().expect("blockers").insert(A.to_string());
        let none = HashSet::new();
        farsight.request(&member()).inbound_list_blocks(V, &page(&[A]), &none).await;
        // Farsight stops reporting the block, at a level that vouches for
        // nothing. A new account on the page forces a call that also names A?
        // No: A is cached, so only B is asked, and A's block stands.
        fake.blockers.lock().expect("blockers").clear();
        *fake.level.lock().expect("level") = "partial";
        let blocked = farsight.request(&member()).inbound_list_blocks(V, &page(&[A, B]), &none).await;
        assert!(blocked.contains_key(A));
        // The write rule itself: an unvouched clean answer over the entry.
        let s = farsight.inner.settings;
        let merged = farsight.cache().write(V, A, &PairAnswer::clean(false), Instant::now(), &s);
        assert!(merged.inbound.list.is_some() && !merged.vouched);
        // A vouched clean answer removes it.
        let merged = farsight.cache().write(V, A, &PairAnswer::clean(true), Instant::now(), &s);
        assert!(!merged.any_block());
    }

    #[tokio::test]
    async fn failures_are_no_answer_and_open_the_breaker_for_members_only() {
        let (farsight, fake) = engine("timeout_ms = 100\n").await;
        *fake.status.lock().expect("status") = 503;
        let none = HashSet::new();
        // An anonymous caller's failures are not recorded.
        let anonymous = Requester::test();
        for i in 0..8 {
            let account = format!("did:plc:{:c>24}", format!("{i:x}").replace(['0', '1', '8', '9'], "d"));
            assert!(farsight.request(&anonymous).inbound_list_blocks(V, &[account], &none).await.is_empty());
        }
        assert!(!farsight.breaker_open());
        // Five failed member calls open it; then no call is made at all.
        for i in 0..5 {
            let account = format!("did:plc:{:e>24}", format!("{i:x}").replace(['0', '1', '8', '9'], "f"));
            farsight.request(&member()).inbound_list_blocks(V, &[account], &none).await;
        }
        assert!(farsight.breaker_open());
        let before = calls(&fake);
        farsight.request(&member()).inbound_list_blocks(V, &page(&[A]), &none).await;
        assert_eq!(calls(&fake), before);
        // Nothing was cached for an unanswered account.
        assert_eq!(farsight.cached(), 0);
    }

    #[tokio::test]
    async fn a_slow_call_uses_the_request_allowance() {
        let (farsight, fake) = engine("timeout_ms = 150\n").await;
        *fake.delay_ms.lock().expect("delay") = 400;
        let none = HashSet::new();
        let request = farsight.request(&member());
        let started = Instant::now();
        assert!(request.inbound_list_blocks(V, &page(&[A]), &none).await.is_empty());
        assert!(started.elapsed() < Duration::from_millis(390));
        // The allowance is spent: the request makes no further call.
        request.inbound_list_blocks(V, &page(&[B]), &none).await;
        assert_eq!(calls(&fake), 1);
    }

    /// A Farsight that hangs is a failing Farsight: each member request's
    /// first call has its whole timeout, and reaching it counts.
    #[tokio::test]
    async fn a_hung_farsight_opens_the_breaker() {
        let (farsight, fake) = engine("timeout_ms = 50\n").await;
        *fake.delay_ms.lock().expect("delay") = 400;
        let none = HashSet::new();
        for i in 0..5 {
            let account = format!("did:plc:{:e>24}", format!("{i:x}").replace(['0', '1', '8', '9'], "f"));
            farsight.request(&member()).inbound_list_blocks(V, &[account], &none).await;
        }
        assert!(farsight.breaker_open());
        let before = calls(&fake);
        farsight.request(&member()).inbound_list_blocks(V, &page(&[A]), &none).await;
        assert_eq!(calls(&fake), before);
        // It never answered this process, so it is not "down": a Farsight
        // that was never reachable is a configuration fault.
        assert!(!farsight.gate().down);
    }

    /// design-v3 §5.14.17.2: Farsight is down once it has answered, its
    /// breaker is open, and only service failures came since. A refusal
    /// such as a 404 in that run keeps it from being "down".
    #[tokio::test]
    async fn farsight_is_down_after_an_answer_and_five_clean_failures() {
        let none = HashSet::new();
        let account = |i: usize| format!("did:plc:{:e>24}", format!("{i:x}").replace(['0', '1', '8', '9'], "f"));
        for refusal in [false, true] {
            let (farsight, fake) = engine("timeout_ms = 50\n").await;
            farsight.request(&member()).inbound_list_blocks(V, &page(&[A]), &none).await;
            assert_eq!(farsight.gate(), GateRead { down: false, answers: 1 });
            if refusal {
                *fake.status.lock().expect("status") = 404;
                farsight.request(&member()).inbound_list_blocks(V, &[account(9)], &none).await;
                *fake.status.lock().expect("status") = 200;
            }
            *fake.delay_ms.lock().expect("delay") = 400;
            for i in 0..5 {
                farsight.request(&member()).inbound_list_blocks(V, &[account(i + 2)], &none).await;
            }
            assert!(farsight.breaker_open());
            assert_eq!(farsight.gate().down, !refusal);
        }
    }

    #[tokio::test]
    async fn an_answer_over_the_cap_is_asked_again_as_two_halves() {
        let (farsight, fake) = engine("timeout_ms = 5000\n").await;
        *fake.huge_over.lock().expect("huge") = 2;
        fake.blockers.lock().expect("blockers").insert(C.to_string());
        let d = "did:plc:dddddddddddddddddddddddd";
        let blocked = farsight.request(&member()).inbound_list_blocks(V, &page(&[A, B, C, d]), &HashSet::new()).await;
        assert_eq!(blocked.len(), 1);
        assert!(blocked.contains_key(C));
        // One call of four, then two of two; the breaker saw no failure.
        assert_eq!(calls(&fake), 3);
        assert!(!farsight.breaker_open());
        assert_eq!(farsight.cached(), 4);
    }
}
