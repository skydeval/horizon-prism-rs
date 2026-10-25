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
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock, PoisonError};
use std::time::{Duration, Instant};

use futures::future::{BoxFuture, join_all};

use super::answer::{Coverage, PairAnswer, Source};
use super::chunks::{chunks, halves};
use super::client::{CallError, FarsightClient};
use super::did::canonical;
use super::fallback::{self, Found, Ledger};
use super::gate::{Effect, GateRead, ReadGate};
use super::memory::{self, Caps};
use crate::appview_clients::AppViewClient;
use crate::config::{FarsightConfig, FarsightFallbackConfig, FarsightMemoryConfig};
use crate::net::NetError;
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
/// `inbound_list_blocks_applied_total`: every inbound list block the
/// ordinary reader applies to a response, whatever its source (Farsight,
/// the fallback or the memory).
pub const METRIC_INBOUND_APPLIED: &str = "inbound_list_blocks_applied_total";

/// How often [`Farsight::run_gate_sampler`] samples the gate's gauges.
const GATE_SAMPLE_EVERY: Duration = Duration::from_secs(1);

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
    /// Who wrote it: Farsight, or the fallback (§5.14.17.6).
    source: Source,
}

/// One viewer's entries (its part of the cache), with the order they were
/// first written in.
#[derive(Default)]
struct ViewerCache {
    entries: HashMap<String, Entry>,
    order: VecDeque<String>,
    /// A ready member's request has written here. A part that only
    /// visitors' requests wrote is a visitor part, and gives way to
    /// members' parts when the cache is full.
    member: bool,
}

/// Whose part of the cache a write goes to, and what kind of request
/// writes it.
#[derive(Debug, Clone, Copy)]
struct Owner<'a> {
    viewer: &'a str,
    /// The request's handle is a ready member's ([`Lookup::set_visitor`]
    /// was not called on it).
    member: bool,
}

/// Viewer parts one sweep step looks at under one hold of the cache's
/// lock, which every lookup takes.
const SWEEP_SLICE: usize = 256;

/// The per-viewer cache of pair answers (§5.14.6). An entry is read only
/// for the viewer it was written for. Its lifetime runs from Farsight's
/// answer: a read does not extend it.
///
/// - A part is a member's or a visitor's. Past the bound across viewers,
///   visitors' parts go first.
/// - A read removes the expired entry it finds, and nothing else. The
///   sweep ([`Farsight::run_cache_sweeper`]) removes the rest, and the
///   parts they leave empty.
#[derive(Default)]
struct Cache {
    viewers: HashMap<String, ViewerCache>,
    /// The viewers whose part is a visitor part. Kept in step with
    /// `viewers` by every place that creates, upgrades or removes a part.
    visitors: HashSet<String>,
    total: usize,
    /// The fallback's ledger (§5.14.17.6), under the cache's lock so that
    /// applying a fallback answer is one step.
    ledger: Ledger,
    /// The memory's copy (§5.14.18.1), under the same lock. Empty and
    /// unread while the memory is off.
    remembered: memory::Copy,
}

impl Cache {
    fn get(&mut self, viewer: &str, other: &str, now: Instant) -> Option<PairAnswer> {
        self.entry(viewer, other, now).map(|(answer, _)| answer)
    }

    /// The live entry for the pair, with who wrote it.
    fn entry(&mut self, viewer: &str, other: &str, now: Instant) -> Option<(PairAnswer, Source)> {
        let v = self.viewers.get_mut(viewer)?;
        match v.entries.get(other) {
            Some(e) if e.expires > now => Some((e.answer.clone(), e.source)),
            Some(_) => {
                v.entries.remove(other);
                self.total -= 1;
                None
            }
            None => None,
        }
    }

    /// Write `new` for the pair under the merge rule, as a member's
    /// request does, and return what the entry now holds.
    #[cfg(test)]
    fn write(&mut self, viewer: &str, other: &str, new: &PairAnswer, now: Instant, s: &Settings) -> PairAnswer {
        self.merge(Owner { viewer, member: true }, other, new, now, s, true)
    }

    /// The merge rule, with the write only if `keep`: a request begun
    /// before a purge gets the answer it would have had and leaves no
    /// entry behind (§5.14.18.2 "One count for everyone").
    fn merge(&mut self, owner: Owner<'_>, other: &str, new: &PairAnswer, now: Instant, s: &Settings, keep: bool) -> PairAnswer {
        let viewer = owner.viewer;
        let live = self.entry(viewer, other, now);
        let (merged, restart, source) = PairAnswer::written_over(new, live.as_ref().map(|(answer, source)| (answer, *source)));
        if !keep {
            return merged;
        }
        let expires = match self.viewers.get(viewer).and_then(|v| v.entries.get(other)) {
            Some(e) if !restart => e.expires,
            _ => now + s.ttl,
        };
        self.store(owner, other, merged.clone(), expires, source, s);
        merged
    }

    /// Put an entry for the pair, within the cache's two bounds.
    ///
    /// The part's mark follows the writer. A member's write marks the part
    /// member, so an account that becomes a ready member is upgraded by
    /// its next write. A visitor's write creates a part as a visitor part
    /// and never demotes one: a member that loses readiness has its part
    /// dropped by the key reload's [`Cache::forget`] anyway.
    fn store(&mut self, owner: Owner<'_>, other: &str, answer: PairAnswer, expires: Instant, source: Source, s: &Settings) {
        let viewer = owner.viewer;
        let created = !self.viewers.contains_key(viewer);
        let v = self.viewers.entry(viewer.to_string()).or_default();
        if owner.member {
            if !v.member {
                v.member = true;
                self.visitors.remove(viewer);
            }
        } else if created {
            self.visitors.insert(viewer.to_string());
        }
        let fresh = v
            .entries
            .insert(
                other.to_string(),
                Entry { answer, expires, source },
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
        // The bound across viewers: other viewers' parts go whole, the
        // visitors' first. A member's part goes only when no visitor's is
        // left, and among the parts of one kind in no particular order. An
        // evicted entry is simply absent.
        while self.total > s.total {
            let other_than = |k: &&String| k.as_str() != viewer;
            let Some(victim) = self.visitors.iter().find(other_than).or_else(|| self.viewers.keys().find(other_than)).cloned() else {
                break;
            };
            self.remove_part(&victim);
        }
    }

    /// Remove a viewer's part whole, keeping the entry total and the
    /// index of visitor parts in step.
    fn remove_part(&mut self, viewer: &str) {
        if let Some(gone) = self.viewers.remove(viewer) {
            self.total -= gone.entries.len();
        }
        self.visitors.remove(viewer);
    }

    /// A viewer's entries, notes and stop go together.
    fn forget(&mut self, viewer: &str) {
        self.remove_part(viewer);
        self.ledger.forget(viewer);
    }

    /// Every viewer that has a part now, for a sweep to work through.
    fn viewer_keys(&self) -> Vec<String> {
        self.viewers.keys().cloned().collect()
    }

    /// One step of the sweep, over the parts of `viewers` that still
    /// exist: every expired entry goes, with its key in the part's
    /// `order`, and a part left empty is deleted. Without that deletion
    /// the cache would keep an empty part, with up to
    /// `cache_entries_per_viewer` key strings, for every account ever
    /// seen, uncounted by `cache_entries_total`. Returns how many entries
    /// went.
    fn sweep(&mut self, viewers: &[String], now: Instant) -> usize {
        let mut swept = 0;
        for viewer in viewers {
            // A part removed since the keys were copied is skipped.
            let Some(v) = self.viewers.get_mut(viewer) else {
                continue;
            };
            let before = v.entries.len();
            v.entries.retain(|_, entry| entry.expires > now);
            let gone = before - v.entries.len();
            self.total -= gone;
            swept += gone;
            // Keys of entries a read removed are still in `order` too.
            if v.order.len() != v.entries.len() {
                v.order.retain(|k| v.entries.contains_key(k));
            }
            if v.entries.is_empty() {
                self.remove_part(viewer);
            }
        }
        swept
    }
}

/// The fallback's parts (§5.14.17): its own client for the AppView it
/// asks, its own bucket, and a breaker of its own.
struct Fallback {
    client: AppViewClient,
    /// One call, and all one request may wait on the fallback.
    timeout: Duration,
    max_calls: u32,
    /// Lifetime of a "blocked" entry and of a ledger note.
    ttl: Duration,
    bucket: SharedBucket,
    breaker: SharedBreaker,
}

/// What [`Farsight::load_memory`] returns. The purge hook is made from it
/// and from nothing else, so a hook cannot exist before the copy is loaded
/// (§5.14.18.3).
pub struct Loaded {
    farsight: Farsight,
    /// Entries loaded; `None` while the memory is off.
    pub entries: Option<usize>,
}

impl Loaded {
    #[must_use]
    pub fn hook(&self) -> PurgeHook {
        PurgeHook(self.farsight.clone())
    }
}

/// What a purge's marker step calls at its tail for a removed member and
/// for a deleted account (§5.14.18.3).
#[derive(Clone)]
pub struct PurgeHook(Farsight);

impl PurgeHook {
    /// Returns nothing: the hook cannot fail a purge.
    pub async fn run(&self, did: &str, account_deleted: bool, member_removed: bool) {
        self.0.purged(did, account_deleted, member_removed).await;
    }
}

/// One row of `learned_inbound_blocks`: viewer, other, list, source and
/// the time it was last confirmed.
type MemoryRow = (String, String, Option<String>, String, chrono::DateTime<chrono::Utc>);

/// The memory's parts (§5.14.18): its bounds, the database it is kept
/// in, and the table lock that orders the writes to its table.
struct Memory {
    pool: sqlx::PgPool,
    caps: Caps,
    /// Held by every write to the table but the purge's batches and the
    /// daily time step. Never taken while the cache's lock is held.
    table: tokio::sync::Mutex<()>,
}

struct Inner {
    /// Set once at startup when `farsight.memory.enabled`, after the copy
    /// was loaded.
    memory: OnceLock<Memory>,
    /// Set once at startup when `farsight.fallback.enabled`.
    fallback: OnceLock<Fallback>,
    /// Raised by every purge of a viewer, so that a request begun before
    /// a purge adds nothing to shared state after it.
    purges: AtomicU64,
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
                memory: OnceLock::new(),
                fallback: OnceLock::new(),
                purges: AtomicU64::new(0),
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

    /// Switch the fallback on for this engine (design-v3 §5.14.17), with
    /// `client` for the AppView that `farsight.fallback.network` names.
    /// Called at most once, at startup, with a validated config.
    pub fn enable_fallback(&self, client: AppViewClient, cfg: &FarsightFallbackConfig, shares: &Shares) {
        let positive = |n: i64| u64::try_from(n).unwrap_or(0);
        let _ = self.inner.fallback.set(Fallback {
            client,
            timeout: Duration::from_millis(positive(cfg.timeout_ms)),
            max_calls: u32::try_from(cfg.max_calls_per_request).unwrap_or(0),
            ttl: Duration::from_secs(positive(cfg.cache_ttl_secs)),
            bucket: shares.bucket_with_share("farsight_fallback", cfg.rate_per_sec.0, u32::try_from(cfg.burst).unwrap_or(0), cfg.requester_share.0),
            breaker: SharedBreaker::default(),
        });
    }

    /// Whether the fallback's own breaker is open now. False when the
    /// fallback is not enabled.
    #[must_use]
    pub fn fallback_breaker_open(&self) -> bool {
        self.inner.fallback.get().is_some_and(|f| f.breaker.is_open(Instant::now()))
    }

    /// Ledger notes held, over every viewer.
    #[must_use]
    pub fn ledger_notes(&self) -> usize {
        self.cache().ledger.len()
    }

    /// The memory's load (design-v3 §5.14.18.1). With the memory on, the
    /// table is loaded into the copy, and from then on blocks are learned,
    /// confirmed, forgotten and read. Called once, at startup, before any
    /// task of `serve` is spawned, so nothing can clear a viewer while
    /// rows read here are not installed: the purge hook is made only from
    /// the value this returns.
    ///
    /// If the load fails the memory stays off for this start, and the rows
    /// stay in the table for the next.
    pub async fn load_memory(&self, pool: &sqlx::PgPool, cfg: &FarsightMemoryConfig) -> Loaded {
        let entries = if cfg.enabled {
            match self.load_rows(pool, cfg).await {
                Ok(entries) => {
                    tracing::info!(event = "farsight_memory_loaded", entries, "remembered blocks loaded");
                    Some(entries)
                }
                Err(error) => {
                    metrics::counter!(memory::METRIC_DROPPED, "reason" => "startup_load").increment(1);
                    tracing::error!(event = "farsight_memory_load_failed", %error, "remembered blocks could not be loaded: the memory is off until the next start");
                    None
                }
            }
        } else {
            None
        };
        Loaded { farsight: self.clone(), entries }
    }

    /// Rows are loaded for current members, within the retention, newest
    /// first, up to the caps. A row the caps leave out is deleted.
    async fn load_rows(&self, pool: &sqlx::PgPool, cfg: &FarsightMemoryConfig) -> Result<usize, sqlx::Error> {
        let positive = |n: i64| usize::try_from(n).unwrap_or(0);
        let caps = Caps {
            per_viewer: positive(cfg.max_per_viewer),
            total: positive(cfg.max_total),
            retention: chrono::TimeDelta::hours(cfg.retention_hours),
        };
        // `members.did` may spell a did:web otherwise than the table does.
        let members: HashSet<String> = sqlx::query_scalar::<_, String>("SELECT did FROM members")
            .fetch_all(pool)
            .await?
            .iter()
            .filter_map(|did| canonical(did))
            .collect();
        let rows: Vec<MemoryRow> = sqlx::query_as(
            "SELECT viewer_did, other_did, list_uri, source, confirmed_at FROM learned_inbound_blocks \
             WHERE confirmed_at > now() - make_interval(hours => $1) ORDER BY confirmed_at DESC",
        )
        .bind(i32::try_from(cfg.retention_hours).unwrap_or(i32::MAX))
        .fetch_all(pool)
        .await?;
        let mut over: (Vec<String>, Vec<String>) = (Vec::new(), Vec::new());
        let loaded = {
            let mut cache = self.cache();
            for (viewer, other, list_uri, source, confirmed_at) in rows {
                // A viewer that is no longer a member: its rows are the
                // purge's and the daily job's to delete.
                if !members.contains(&viewer) {
                    continue;
                }
                let source = if source == "fallback" { Source::Fallback } else { Source::Farsight };
                if !cache.remembered.install(&viewer, &other, list_uri, source, confirmed_at, caps) {
                    over.0.push(viewer);
                    over.1.push(other);
                }
            }
            cache.remembered.len()
        };
        if !over.0.is_empty() {
            sqlx::query("DELETE FROM learned_inbound_blocks b USING unnest($1::text[], $2::text[]) AS o(viewer, other) WHERE b.viewer_did = o.viewer AND b.other_did = o.other")
                .bind(&over.0)
                .bind(&over.1)
                .execute(pool)
                .await?;
        }
        let _ = self.inner.memory.set(Memory { pool: pool.clone(), caps, table: tokio::sync::Mutex::new(()) });
        Ok(loaded)
    }

    /// The memory's writer (§5.14.18.1): it makes the table match the
    /// copy, one pending pair at a time. It is not a queue of operations:
    /// under the table lock it reads what the copy holds for the pair now
    /// and writes that, so it never applies an old value. Each pair is its
    /// own statement. It also sweeps expired entries once an hour.
    pub async fn run_memory_writer(self) {
        let Some(m) = self.inner.memory.get() else {
            return;
        };
        let mut swept = Instant::now();
        loop {
            tokio::time::sleep(Duration::from_secs(1)).await;
            if swept.elapsed() >= Duration::from_secs(3600) {
                swept = Instant::now();
                self.cache().remembered.sweep(chrono::Utc::now(), m.caps);
            }
            let (entries, due) = {
                let cache = self.cache();
                (cache.remembered.len(), cache.remembered.due(Instant::now(), 256))
            };
            #[allow(clippy::cast_precision_loss)]
            metrics::gauge!(memory::METRIC_ENTRIES).set(entries as f64);
            for pair in due {
                let _table = m.table.lock().await;
                let Some((row, failures)) = self.cache().remembered.take(&pair) else {
                    continue;
                };
                let write = async {
                    match &row {
                        Some(entry) => {
                            sqlx::query(
                                "INSERT INTO learned_inbound_blocks (viewer_did, other_did, list_uri, source, confirmed_at) VALUES ($1, $2, $3, $4, $5) \
                                 ON CONFLICT (viewer_did, other_did) DO UPDATE SET list_uri = EXCLUDED.list_uri, source = EXCLUDED.source, confirmed_at = EXCLUDED.confirmed_at",
                            )
                            .bind(&pair.0)
                            .bind(&pair.1)
                            .bind(&entry.list_uri)
                            .bind(if entry.source == Source::Fallback { "fallback" } else { "farsight" })
                            .bind(entry.confirmed_at)
                            .execute(&m.pool)
                            .await
                        }
                        None => {
                            sqlx::query("DELETE FROM learned_inbound_blocks WHERE viewer_did = $1 AND other_did = $2")
                                .bind(&pair.0)
                                .bind(&pair.1)
                                .execute(&m.pool)
                                .await
                        }
                    }
                };
                match tokio::time::timeout(Duration::from_secs(5), write).await {
                    Ok(Ok(_)) => {}
                    // The table refused the row itself: never again.
                    Ok(Err(sqlx::Error::Database(db))) if row.is_some() && db.code().is_some_and(|c| c.starts_with("22") || c.starts_with("23")) => {
                        self.cache().remembered.rejected(&pair);
                    }
                    _ => self.cache().remembered.failed(&pair, row.is_some(), failures, Instant::now()),
                }
            }
        }
    }

    /// One pass of the cache's sweep (§5.14.6): every expired entry goes,
    /// and every part left empty. Returns how many entries went.
    ///
    /// The cache's lock is taken by every lookup, so a pass never holds it
    /// throughout: once to copy the viewer keys, then once for each slice
    /// of them, with other tasks let in between. A part written after the
    /// keys were copied waits for the next pass.
    pub async fn sweep_cache(&self) -> usize {
        let viewers = self.cache().viewer_keys();
        let mut swept = 0;
        for slice in viewers.chunks(SWEEP_SLICE) {
            swept += self.cache().sweep(slice, Instant::now());
            tokio::task::yield_now().await;
        }
        swept
    }

    /// The cache's sweeper: one pass per `farsight.cache_ttl_secs`, for as
    /// long as the process runs. Nothing else removes an entry that is
    /// never read again, which is what a visitor that does not come back
    /// leaves behind.
    pub async fn run_cache_sweeper(self) {
        // Validation keeps the lifetime at a second or more. The floor
        // guards against a busy loop should that ever change.
        let every = self.inner.settings.ttl.max(Duration::from_secs(1));
        loop {
            tokio::time::sleep(every).await;
            self.sweep_cache().await;
        }
    }

    /// Set the gate's gauges, `farsight_down` and
    /// `farsight_fallback_withheld{reason}`, from the gate as it is now.
    pub fn sample_gate(&self) {
        self.inner.gate.sample(Instant::now());
        // The ledger's gauge, sampled with the gate's: 0 with the fallback
        // off, as the ledger is then always empty.
        #[allow(clippy::cast_precision_loss)]
        metrics::gauge!(fallback::METRIC_LEDGER_ENTRIES).set(self.ledger_notes() as f64);
    }

    /// The gate's sampler: the gauges once at once, so every series exists
    /// before the first call, then once a second for as long as the
    /// process runs. They are computed from the gate, not set on outcomes,
    /// because an open period ends by the clock with no event (design-v4
    /// §14.2, FB4-21).
    pub async fn run_gate_sampler(self) {
        loop {
            self.sample_gate();
            tokio::time::sleep(GATE_SAMPLE_EVERY).await;
        }
    }

    /// The purge hook (§5.14.18.3), run at the tail of a purge's marker
    /// step for a removed member and for a deleted account. It cannot
    /// fail the purge: a failed delete is logged, and the rows are left
    /// to the writer and to the purge's batches.
    ///
    /// - If the step removed a member: the purge count rises (so a
    ///   request of the old membership adds nothing afterwards), and the
    ///   viewer's cache entries, ledger, stop and remembered blocks go,
    ///   with their rows.
    /// - For a deleted account: it also goes as the blocker of anyone.
    pub async fn purged(&self, did: &str, account_deleted: bool, member_removed: bool) {
        let did = canonical(did).unwrap_or_else(|| did.to_string());
        let memory = self.inner.memory.get();
        if member_removed {
            {
                let mut cache = self.cache();
                self.inner.purges.fetch_add(1, Ordering::SeqCst);
                cache.forget(&did);
                cache.remembered.clear_viewer(&did);
            }
            if let Some(m) = memory {
                self.delete_rows(m, "viewer_did", &did).await;
            }
        }
        if account_deleted {
            self.cache().remembered.clear_other(&did);
            if let Some(m) = memory {
                self.delete_rows(m, "other_did", &did).await;
            }
        }
    }

    /// `admin farsight forget`: what the purge hook does for a removed
    /// member, without raising the purge count. The viewer's cache
    /// entries, ledger and stop go, and every remembered pair with its
    /// row. Returns how many pairs were remembered.
    ///
    /// # Errors
    ///
    /// The delete's error; the pairs then stay pending for the writer.
    pub async fn forget_memory(&self, viewer: &str) -> Result<usize, String> {
        let viewer = canonical(viewer).unwrap_or_else(|| viewer.to_string());
        let cleared = {
            let mut cache = self.cache();
            cache.forget(&viewer);
            cache.remembered.clear_viewer(&viewer)
        };
        let Some(m) = self.inner.memory.get() else {
            return Ok(0);
        };
        let _table = m.table.lock().await;
        sqlx::query("DELETE FROM learned_inbound_blocks WHERE viewer_did = $1")
            .bind(&viewer)
            .execute(&m.pool)
            .await
            .map_err(|e| e.to_string())?;
        Ok(cleared)
    }

    /// One `DELETE` of a DID's rows under the table lock, the statement
    /// within 5 s.
    async fn delete_rows(&self, m: &Memory, column: &'static str, did: &str) {
        let _table = m.table.lock().await;
        let sql = format!("DELETE FROM learned_inbound_blocks WHERE {column} = $1");
        let delete = sqlx::query(&sql).bind(did).execute(&m.pool);
        match tokio::time::timeout(Duration::from_secs(5), delete).await {
            Ok(Ok(_)) => {}
            _ => {
                metrics::counter!(memory::METRIC_DROPPED, "reason" => "purge_delete").increment(1);
                tracing::warn!(event = "farsight_memory_purge_delete_failed", column, "a purge's delete of remembered blocks failed; the writer and the purge's batches follow");
            }
        }
    }

    /// The memory in brief, with no DID: `None` when it is off.
    #[must_use]
    pub fn memory_stats(&self) -> Option<serde_json::Value> {
        let m = self.inner.memory.get()?;
        let cache = self.cache();
        let (farsight, fallback) = cache.remembered.by_source();
        Some(serde_json::json!({
            "entries": cache.remembered.len(),
            "viewers": cache.remembered.viewers(),
            "from_farsight": farsight,
            "from_fallback": fallback,
            "oldest_confirmed_at": cache.remembered.oldest(),
            "pending": cache.remembered.pending(),
            "retention_hours": m.caps.retention.num_hours(),
            "max_per_viewer": m.caps.per_viewer,
            "max_total": m.caps.total,
        }))
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
                fallback_calls_left: self.inner.fallback.get().map_or(0, |f| f.max_calls),
                fallback_allowance: self.inner.fallback.get().map_or(Duration::ZERO, |f| f.timeout),
            })),
            cache_only: Arc::new(AtomicBool::new(false)),
            no_fallback: Arc::new(AtomicBool::new(false)),
            member: Arc::new(AtomicBool::new(true)),
            purges: self.inner.purges.load(Ordering::SeqCst),
            deadline: req.deadline(),
            endpoint: Arc::from(req.labels().1),
        }
    }

    /// Drop a viewer's cached answers (a removed member, a deleted account).
    pub fn forget(&self, viewer: &str) {
        let viewer = canonical(viewer).unwrap_or_else(|| viewer.to_string());
        // Never the memory's copy: this is called for every member whose
        // key left the in-memory table, a member that only lost readiness
        // included. The purge hook clears the copy.
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

    /// What this AppView itself holds about Farsight, for `admin farsight
    /// status` (§14.3): printed whether or not Farsight answers. Counts
    /// only, and no DID.
    #[must_use]
    pub fn local_status(&self) -> serde_json::Map<String, serde_json::Value> {
        use serde_json::json;
        let (answered, clean) = self.inner.gate.run();
        let fallback = match self.inner.fallback.get() {
            Some(fb) => json!({
                "network": fb.client.network(),
                "appview_url": fb.client.base_url(),
                "breaker_open": fb.breaker.is_open(Instant::now()),
                "ledger_entries": self.ledger_notes(),
            }),
            None => json!("disabled"),
        };
        let mut out = serde_json::Map::new();
        out.insert("answered_since_start".to_string(), json!(answered));
        out.insert("run_clean".to_string(), json!(clean));
        out.insert("down".to_string(), json!(self.gate().down));
        out.insert("fallback".to_string(), fallback);
        out.insert("memory".to_string(), self.memory_stats().unwrap_or_else(|| json!("disabled")));
        out
    }

    /// The memory's table lock, for the daily membership step: `None`
    /// while the memory is off. Taken after a DID's advisory lock, never
    /// before (§5.14.18.1 "One lock order").
    pub(crate) async fn table_lock(&self) -> Option<tokio::sync::MutexGuard<'_, ()>> {
        match self.inner.memory.get() {
            Some(m) => Some(m.table.lock().await),
            None => None,
        }
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
    /// Fallback calls the request may still make, and how long it may
    /// still wait on them. Both zero with the fallback off.
    fallback_calls_left: u32,
    fallback_allowance: Duration,
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
    /// This request never starts a fallback call (§5.14.17.2).
    no_fallback: Arc<AtomicBool>,
    /// The request is a ready member's, or a public one about a member.
    /// True unless [`Lookup::set_visitor`] was called. It is never derived
    /// from `key`: a ready member can be charged by address.
    member: Arc<AtomicBool>,
    /// The purge count when the request began.
    purges: u64,
    /// The request's deadline (`Requester::deadline`): the fallback's calls
    /// end by it.
    deadline: Instant,
    /// The XRPC method being served (`Requester::labels`), for the
    /// `endpoint` label of `farsight_fallback_requests_total`.
    endpoint: Arc<str>,
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
    /// The accounts the fallback may be asked about if Farsight turns out
    /// to be down: those of a call the gate refused, or that ended in a
    /// service failure (§5.14.17.2 "Which accounts").
    eligible: Vec<String>,
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
    /// At least one call had no answer (a timeout, a refusal, a transport
    /// error): why a lookup that made calls did not vouch, when it is not
    /// a partial level.
    pub unanswered: bool,
}

impl Lookup {
    /// From here on this request answers from the viewer's cache only and
    /// makes no call. For a listing whose accounts come from the viewer's
    /// private state (its mutes, its bookmarks), so that Farsight never
    /// receives a viewer beside them; and for per-keystroke surfaces.
    pub fn set_cache_only(&self) {
        self.cache_only.store(true, Ordering::Relaxed);
    }

    /// From here on this request never starts a fallback call, though it
    /// still reads a fallback result an ordinary render already cached.
    /// For every lookup that is not a ready member's ordinary render:
    /// Thread Lens rounds, and the unread count's correction.
    pub fn set_no_fallback(&self) {
        self.no_fallback.store(true, Ordering::Relaxed);
    }

    /// From here on this request is a visitor's: the viewer it asks about
    /// is a signed-in account that is not a ready member (Thread Lens,
    /// under `threadlens.access = "signed_in"`). For such a request:
    /// - nothing is learned, recalled or forgotten in the memory of
    ///   learned blocks, which is for members only;
    /// - its calls draw on the non-member pool, on a share of the viewer's
    ///   own, so their failures never count on the read breaker;
    /// - its calls are never the half-open probe;
    /// - what it caches is marked as a visitor's, and goes first when the
    ///   cache is full.
    ///
    /// There is no way back: a handle is a visitor's for the rest of its
    /// request.
    pub fn set_visitor(&self) {
        self.member.store(false, Ordering::Relaxed);
    }

    fn member(&self) -> bool {
        self.member.load(Ordering::Relaxed)
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
        let applied: HashMap<String, String> = page
            .iter()
            .filter_map(|did| {
                let list = found.get(&canonical(did)?)?.inbound.list.clone()?;
                Some((did.clone(), list))
            })
            .collect();
        metrics::counter!(METRIC_INBOUND_APPLIED).increment(applied.len() as u64);
        applied
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
        // Shared entries only, so an ordinary (never viewer-private) handle.
        let shared = Requester::new(self.key.clone());
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
            match rt.get_json::<(i64, PairAnswer)>(&shared, &key(other)).await {
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
                        rt.put_json(&shared, &key(&other), &(at, &answer)).await;
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

    /// What an applied Farsight answer does to the memory (§5.14.18.2):
    /// an inbound list block is learned or confirmed, unless a member was
    /// purged since the request began; a vouched answer that names none
    /// forgets the pair at once, whatever the purge count. Returns whether
    /// a pair was forgotten. A visitor's request does none of it.
    fn remember(&self, cache: &mut Cache, actor: &str, other: &str, answer: &PairAnswer) -> bool {
        if !self.member() {
            return false;
        }
        let Some(m) = self.farsight.inner.memory.get() else {
            return false;
        };
        match &answer.inbound.list {
            Some(list) => {
                if self.farsight.inner.purges.load(Ordering::SeqCst) == self.purges {
                    cache.remembered.learn(actor, other, Some(list), Source::Farsight, chrono::Utc::now(), m.caps);
                } else {
                    metrics::counter!(memory::METRIC_DROPPED, "reason" => "purge_count").increment(1);
                }
                false
            }
            None => answer.vouched && cache.remembered.forget(actor, other),
        }
    }

    /// The read rule (§5.14.18.2): a remembered block applies to a pair
    /// whenever the pair answer in hand does not vouch. `held` gains the
    /// block; whether anything changed is returned. Nothing is recalled
    /// for a visitor's request.
    fn recall(&self, cache: &mut Cache, actor: &str, other: &str, held: &mut Option<PairAnswer>) -> bool {
        if !self.member() {
            return false;
        }
        let Some(m) = self.farsight.inner.memory.get() else {
            return false;
        };
        if held.as_ref().is_some_and(|a| a.vouched || a.inbound.list.is_some()) {
            return false;
        }
        let Some(list) = cache.remembered.get(actor, other, chrono::Utc::now(), m.caps) else {
            return false;
        };
        let mut answer = held.take().unwrap_or_default();
        answer.inbound.list = Some(list);
        answer.vouched = false;
        *held = Some(answer);
        metrics::counter!(memory::METRIC_APPLIED).increment(1);
        true
    }

    /// The request's own delete of the rows of the pairs it forgot
    /// (§5.14.18.1 "Forget"): so that an unblock Farsight vouched for is
    /// not left to the writer. One `DELETE`, under one deadline of 1 s
    /// that covers the table lock, the connection and the statement. A
    /// pair learned again meanwhile is left alone. If the deadline passes
    /// or the delete fails, the pairs stay pending. A visitor's request
    /// forgets nothing, and deletes nothing.
    async fn delete_forgotten(&self, actor: &str, forgotten: Vec<String>) {
        if !self.member() {
            return;
        }
        let Some(m) = self.farsight.inner.memory.get() else {
            return;
        };
        if forgotten.is_empty() {
            return;
        }
        let delete = async {
            let _table = m.table.lock().await;
            let gone: Vec<String> = {
                let cache = self.farsight.cache();
                forgotten.into_iter().filter(|other| !cache.remembered.holds(actor, other)).collect()
            };
            sqlx::query("DELETE FROM learned_inbound_blocks WHERE viewer_did = $1 AND other_did = ANY($2)")
                .bind(actor)
                .bind(&gone)
                .execute(&m.pool)
                .await
                .map(|_| gone)
        };
        if let Ok(Ok(gone)) = tokio::time::timeout(Duration::from_secs(1), delete).await {
            let mut cache = self.farsight.cache();
            for other in gone {
                if !cache.remembered.holds(actor, &other) {
                    cache.remembered.take(&(actor.to_string(), other));
                }
            }
        }
    }

    /// Pair answers for `others` (canonical DIDs, in page order): the
    /// live answers of [`Self::live_pairs`], then the memory's read rule
    /// for every account whose answer does not vouch.
    async fn pairs(&self, actor: &str, others: &[String]) -> HashMap<String, PairAnswer> {
        let (mut found, forgotten) = self.live_pairs(actor, others).await;
        if self.farsight.inner.memory.get().is_some() {
            let cache_only = self.cache_only.load(Ordering::Relaxed);
            {
                let mut state = self.state();
                let mut cache = self.farsight.cache();
                for other in others {
                    let mut held = found.get(other).cloned();
                    if self.recall(&mut cache, actor, other, &mut held)
                        && let Some(answer) = held
                    {
                        // A cache-only read memoizes nothing.
                        if !cache_only {
                            state.memo.insert(other.clone(), Some(answer.clone()));
                        }
                        found.insert(other.clone(), answer);
                    }
                }
            }
            self.delete_forgotten(actor, forgotten).await;
        }
        found
    }

    /// Live pair answers for `others`: from the request's memo, then the
    /// viewer's cache, then Farsight. An account with no answer is absent.
    /// The second value is the pairs whose remembered block a vouched
    /// answer forgot.
    ///
    /// A cached entry the fallback wrote is a block in hand, but never
    /// stands in for Farsight: its account is still asked about. When the
    /// calls are done and Farsight turns out to be down, the fallback is
    /// asked about the accounts whose calls never got through.
    async fn live_pairs(&self, actor: &str, others: &[String]) -> (HashMap<String, PairAnswer>, Vec<String>) {
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
                    continue;
                }
                match cache.entry(actor, other, now) {
                    Some((answer, Source::Farsight)) => {
                        state.memo.insert(other.clone(), Some(answer.clone()));
                        found.insert(other.clone(), answer);
                    }
                    Some((answer, Source::Fallback)) => {
                        found.insert(other.clone(), answer);
                        ask.push(other.clone());
                    }
                    None => ask.push(other.clone()),
                }
            }
            // A cache-only miss is not memoized, so it never suppresses a
            // real lookup later in the request.
            if ask.is_empty() || cache_only {
                return (found, Vec::new());
            }
            let mut plan = chunks(actor, &ask);
            let allowed = usize::try_from(state.calls_left).unwrap_or(usize::MAX);
            // Accounts past the call cap get no answer beyond a block
            // already in hand.
            for skipped in plan.iter().skip(allowed).flatten() {
                state.memo.insert(skipped.clone(), found.get(skipped).cloned());
            }
            plan.truncate(allowed);
            state.calls_left -= u32::try_from(plan.len()).unwrap_or(state.calls_left).min(state.calls_left);
            (plan, state.allowance)
        };
        let started = Instant::now();
        let per_call = inner.settings.timeout;
        let called: Vec<Called> = join_all(plan.into_iter().map(|chunk| self.call(actor, chunk, allowance, started, true, per_call))).await;
        let mut eligible: Vec<String> = Vec::new();
        let mut forgotten: Vec<String> = Vec::new();
        {
            let now = Instant::now();
            let mut state = self.state();
            state.allowance = state.allowance.saturating_sub(started.elapsed());
            let mut cache = self.farsight.cache();
            let keep = inner.purges.load(Ordering::SeqCst) == self.purges;
            let owner = Owner { viewer: actor, member: self.member() };
            for call in called {
                eligible.extend(call.eligible);
                for (other, outcome) in call.outcomes {
                    if let Some(answer) = &outcome
                        && self.remember(&mut cache, actor, &other, answer)
                    {
                        forgotten.push(other.clone());
                    }
                    // The memo holds the merged entry, so a block the cache
                    // already named is not lost to an answer that does not
                    // vouch; with no answer, a block in hand stands.
                    let merged = outcome.map(|answer| cache.merge(owner, &other, &answer, now, &inner.settings, keep));
                    let held = merged.or_else(|| found.get(&other).cloned());
                    if let Some(answer) = &held {
                        found.insert(other.clone(), answer.clone());
                    }
                    state.memo.insert(other, held);
                }
            }
        }
        // One read of the gate, after the merge (§5.14.17.2 "How a lookup
        // decides"): the answer count it returns is the snapshot a
        // fallback answer is later checked against.
        if let Some(fb) = inner.fallback.get()
            && !eligible.is_empty()
            && !self.no_fallback.load(Ordering::Relaxed)
        {
            let read = inner.gate.read(Instant::now());
            if read.down {
                found.extend(self.ask_fallback(fb, actor, &eligible, read.answers).await);
            }
        }
        (found, forgotten)
    }

    /// Ask the fallback about `eligible`, less what the reductions of
    /// §5.14.17.5 leave out, and apply its answer under §5.14.17.6's two
    /// guards. Returns the blocks it taught this request.
    async fn ask_fallback(&self, fb: &Fallback, actor: &str, eligible: &[String], snapshot: u64) -> HashMap<String, PairAnswer> {
        let inner = &self.farsight.inner;
        let skip = |reason: &'static str| metrics::counter!(fallback::METRIC_SKIPPED, "reason" => reason).increment(1);
        let (plan, allowance) = {
            let now = Instant::now();
            let mut state = self.state();
            let mut cache = self.farsight.cache();
            // The other AppView does not know this viewer: not again yet.
            if cache.ledger.stopped(actor, now, fb.ttl) {
                skip("actor_stop");
                return HashMap::new();
            }
            // Not sent: a pair with a live entry of either source (a block
            // in hand could not be removed by the answer anyway), and a
            // pair told "nothing" lately.
            let mut send: Vec<String> = Vec::new();
            for other in eligible {
                // A remembered block is in hand too: the memory never goes
                // to the other AppView, though it can leave a pair out.
                let remembered = inner.memory.get().is_some_and(|m| cache.remembered.get(actor, other, chrono::Utc::now(), m.caps).is_some());
                let held = remembered || cache.entry(actor, other, now).is_some() || cache.ledger.noted(actor, other, now, fb.ttl);
                if !held && !send.contains(other) {
                    send.push(other.clone());
                }
            }
            let mut plan = fallback::calls(actor, &send);
            let allowed = usize::try_from(state.fallback_calls_left).unwrap_or(usize::MAX);
            // Each call past the request's cap is a call not made.
            for _ in allowed..plan.len() {
                skip("call_cap");
            }
            plan.truncate(allowed);
            state.fallback_calls_left -= u32::try_from(plan.len()).unwrap_or(state.fallback_calls_left).min(state.fallback_calls_left);
            (plan, state.fallback_allowance)
        };
        if plan.is_empty() {
            return HashMap::new();
        }
        if allowance.is_zero() {
            for _ in &plan {
                skip("allowance");
            }
            return HashMap::new();
        }
        // The fallback comes last in a request, and never holds it past
        // the request's own deadline.
        let allowance = allowance.min(self.deadline.saturating_duration_since(Instant::now()));
        if allowance.is_zero() {
            for _ in &plan {
                skip("deadline");
            }
            return HashMap::new();
        }
        let started = Instant::now();
        let answers = join_all(plan.into_iter().map(|call| self.fallback_call(fb, actor, call, allowance))).await;

        // The apply step: one step under the cache's lock.
        let now = Instant::now();
        let mut learned: HashMap<String, PairAnswer> = HashMap::new();
        let mut state = self.state();
        state.fallback_allowance = state.fallback_allowance.saturating_sub(started.elapsed());
        let mut cache = self.farsight.cache();
        // A member was purged since the request began: its own response
        // still uses what it learned, and nothing is left for later ones.
        let purged = inner.purges.load(Ordering::SeqCst) != self.purges;
        // Guard 1: Farsight answered meanwhile. The whole answer goes.
        if inner.gate.read(now).answers != snapshot {
            let answered = answers.iter().filter(|(class, _)| *class == fallback::Class::Ok).count();
            metrics::counter!(fallback::METRIC_DISCARDED).increment(answered as u64);
            return learned;
        }
        for (class, results) in answers {
            if class == fallback::Class::ActorNotFound && !purged {
                cache.ledger.stop(actor, now);
            }
            for (other, result) in results {
                let pair = |label: &'static str| metrics::counter!(fallback::METRIC_PAIRS, "result" => label).increment(1);
                match result {
                    Found::Blocked(list) => {
                        // Guard 2: a fallback result never changes an
                        // entry Farsight wrote.
                        if matches!(cache.entry(actor, &other, now), Some((_, Source::Farsight))) {
                            pair("dropped");
                            continue;
                        }
                        pair("blocked");
                        metrics::counter!(fallback::METRIC_BLOCKS_APPLIED).increment(1);
                        let answer = PairAnswer::from_fallback(list);
                        if !purged {
                            // The fallback only ever runs for a member.
                            let owner = Owner { viewer: actor, member: true };
                            cache.store(owner, &other, answer.clone(), now + fb.ttl, Source::Fallback, &inner.settings);
                            if let Some(m) = inner.memory.get() {
                                cache.remembered.learn(actor, &other, answer.inbound.list.as_deref(), Source::Fallback, chrono::Utc::now(), m.caps);
                            }
                        }
                        state.memo.insert(other.clone(), Some(answer.clone()));
                        learned.insert(other, answer);
                    }
                    nothing @ Found::Nothing(_) => {
                        pair(nothing.label());
                        if !purged {
                            cache.ledger.note(actor, &other, now, inner.settings.per_viewer, inner.settings.total);
                        }
                    }
                }
            }
        }
        learned
    }

    /// One fallback call, within the request's fallback allowance. Every
    /// response is classified, and every class but a cut by the request's
    /// own budget is recorded on the fallback's breaker: nobody but a
    /// ready member reaches it, so no failure is left out.
    async fn fallback_call(&self, fb: &Fallback, actor: &str, call: Vec<String>, allowance: Duration) -> (fallback::Class, HashMap<String, Found>) {
        use fallback::Class as Fb;
        let skipped = |reason: &'static str| {
            metrics::counter!(fallback::METRIC_SKIPPED, "reason" => reason).increment(1);
            (Fb::Budget, HashMap::new())
        };
        let Some(admission) = fb.breaker.admit(Instant::now()) else {
            return skipped("breaker_open");
        };
        // A miss is "no answer" and touches nothing else: never a wait,
        // never a 429, and not counted as an exhausted share.
        if fb.bucket.quiet_draw(&self.key).is_err() {
            return skipped("no_budget");
        }
        let whole = allowance >= fb.timeout;
        let (class, results) = match fb.client.get_relationships_within(actor, &call, allowance.min(fb.timeout)).await {
            Ok(response) => fallback::read(response.status.as_u16(), &response.body, actor, &call),
            Err(NetError::Timeout) if whole => (Fb::Timeout, HashMap::new()),
            Err(NetError::Timeout) => (Fb::Budget, HashMap::new()),
            Err(NetError::BodyTooLarge { .. }) => (Fb::Malformed, HashMap::new()),
            Err(_) => (Fb::Transport, HashMap::new()),
        };
        match class {
            Fb::Ok => admission.record(Class::Hit, Instant::now()),
            Fb::Budget => drop(admission),
            _ => admission.record(Class::Transient, Instant::now()),
        }
        metrics::counter!(fallback::METRIC_REQUESTS, "endpoint" => self.endpoint.to_string(), "class" => class.label()).increment(1);
        (class, results)
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
        // No Thread Lens round starts a fallback call (§5.14.17.7).
        self.set_no_fallback();
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
        let mut forgotten: Vec<String> = Vec::new();
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
            let keep = inner.purges.load(Ordering::SeqCst) == self.purges;
            let owner = Owner { viewer: &actor, member: self.member() };
            out.calls = called.len();
            out.vouches =!called.is_empty() && called.iter().all(|call| call.answered && call.levels.iter().all(|level| level.vouches()));
            out.unanswered = called.iter().any(|call| !call.answered);
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
                    if let Some(answer) = &outcome
                        && self.remember(&mut cache, &actor, &other, answer)
                    {
                        forgotten.push(other.clone());
                    }
                    let held = match outcome {
                        Some(answer) => Some(cache.merge(owner, &other, &answer, now, &inner.settings, keep)),
                        None => standing.remove(&other).or_else(|| state.memo.get(&other).cloned().flatten()),
                    };
                    state.memo.insert(other, held);
                }
            }
        }
        {
            let mut state = self.state();
            let mut cache = self.farsight.cache();
            for did in others {
                let Some(c) = canonical(did).filter(|c| *c != actor) else {
                    continue;
                };
                // The memory's read rule, applied here too: the ordinary
                // lookup does not run during a Thread Lens request. A
                // remembered block is a block and never a vouch.
                let mut held = state.memo.get(&c).cloned().flatten();
                if self.recall(&mut cache, &actor, &c, &mut held) {
                    state.memo.insert(c.clone(), held.clone());
                }
                if let Some(answer) = held {
                    out.answers.insert(did.clone(), answer);
                }
            }
        }
        self.delete_forgotten(&actor, forgotten).await;
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
            let none = |chunk: Vec<String>, outcome: &'static str, eligible: bool| {
                metrics::counter!(METRIC_CALLS, "outcome" => outcome).increment(1);
                Called {
                    eligible: if eligible { chunk.clone() } else { Vec::new() },
                    outcomes: chunk.into_iter().map(|did| (did, None)).collect(),
                    levels: Vec::new(),
                    answered: false,
                }
            };
            let member = self.member();
            // The admission is the probe-slot guard: dropped on any exit
            // below without a recorded outcome, it gives the slot back.
            //
            // A visitor's call is never the half-open probe. Its failure
            // is not recorded (it draws on the non-member pool), so as the
            // probe it would only give the slot back: during an outage
            // visitors would take the probe one after another, the breaker
            // would stay half-open, and members would never get the
            // fallback, which needs it open. So a visitor's call goes out
            // only through a closed breaker.
            let admitted = if member { inner.gate.admit(Instant::now()) } else { inner.gate.admit_unless_probe() };
            let Some(admission) = admitted else {
                return none(chunk, "breaker_open", true);
            };
            // An empty share is counted here, as "no_budget", and nowhere
            // else.
            //
            // The pool follows the handle's member flag, never its key. A
            // visitor draws on the non-member pool, on a share named by
            // the viewer's own DID: visitors behind one address (a PDS)
            // would otherwise have one share between them. That holds
            // also for a member that is not ready and is still charged by
            // DID, whose key would otherwise reach the member pool.
            let drawn = if member { inner.bucket.quiet_draw(&self.key) } else { inner.bucket.quiet_draw_nonmember(&ChargeKey::member(actor)) };
            let Ok(pool) = drawn else {
                return none(chunk, "no_budget", false);
            };
            let remaining = allowance.saturating_sub(started.elapsed());
            if remaining.is_zero() {
                return none(chunk, "allowance_spent", false);
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
                    Called { outcomes, levels: vec![answer.coverage], answered: true, eligible: Vec::new() }
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
                                eligible: a.eligible.into_iter().chain(b.eligible).collect(),
                            }
                        }
                        None => Called { outcomes: chunk.into_iter().map(|did| (did, None)).collect(), levels: Vec::new(), answered: false, eligible: Vec::new() },
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
                    none(chunk, error.label(), pool == Pool::Member && effect == Effect::ServiceFailure)
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
        /// Accounts the actor list-blocks, each with the lists it names in
        /// `blockedByActor`.
        pub blocked_through: Mutex<HashMap<String, Vec<String>>>,
        pub level: Mutex<&'static str>,
        pub status: Mutex<u16>,
        pub delay_ms: Mutex<u64>,
        /// Answers naming more than this many accounts are over the cap.
        pub huge_over: Mutex<usize>,
        pub calls: AtomicUsize,
        pub asked: Mutex<Vec<Vec<String>>>,
        /// `(actor, other)` pairs where `other` list-blocks that actor only.
        pub pair_blocks: Mutex<HashSet<(String, String)>>,
    }

    async fn answer(State(fake): State<Arc<Fake>>, RawQuery(q): RawQuery) -> axum::response::Response {
        fake.calls.fetch_add(1, Ordering::SeqCst);
        let query = q.unwrap_or_default();
        let others: Vec<String> = url::form_urlencoded::parse(query.as_bytes())
            .filter(|(k, _)| k == "others")
            .map(|(_, v)| v.into_owned())
            .collect();
        let actor: String = url::form_urlencoded::parse(query.as_bytes()).find(|(k, _)| k == "actor").map(|(_, v)| v.into_owned()).unwrap_or_default();
        let pairs: HashSet<String> =
            fake.pair_blocks.lock().expect("pairs").iter().filter(|(a, _)| *a == actor).map(|(_, o)| o.clone()).collect();
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
        let through = fake.blocked_through.lock().expect("through");
        let results: Vec<Value> = others
            .iter()
            .filter(|o| blockers.contains(*o) || through.contains_key(*o) || pairs.contains(*o))
            .map(|o| {
                let inbound: Vec<&str> = if blockers.contains(o) || pairs.contains(o) { vec![LIST] } else { Vec::new() };
                let outbound = through.get(o).cloned().unwrap_or_default();
                json!({"did": o, "blocksActor": {"direct": false, "lists": inbound}, "blockedByActor": {"direct": false, "lists": outbound}})
            })
            .collect();
        let level = *fake.level.lock().expect("level");
        let actor = if actor.is_empty() { V.to_string() } else { actor };
        axum::Json(json!({"actor": actor, "results": results, "partialFor": [], "freshness": {"coverage": {"level": if level.is_empty() { "complete" } else { level }}}})).into_response()
    }

    /// An engine over a fake Farsight, with `toml` as its `[farsight]`.
    pub(crate) async fn engine(toml: &str) -> (Farsight, Arc<Fake>) {
        let fake = Arc::new(Fake::default());
        let base = serve_fake(Router::new().route(CHECK_BLOCKS_PATH, get(answer)).with_state(Arc::clone(&fake))).await;
        let cfg = crate::config::FileConfig::parse(&format!("[farsight]\n{toml}")).expect("parse").farsight;
        let client = FarsightClient::new(&crate::net::Transports::loopback_for_tests(), &base, "fsk_test").expect("client");
        (Farsight::new(client, &cfg, &Shares::for_tests()), fake)
    }

    /// A fake AppView answering `getRelationships` for the fallback.
    #[derive(Default)]
    pub(crate) struct FakeAppView {
        /// Accounts it reports as blocking the actor through a list.
        pub blockers: Mutex<HashSet<String>>,
        /// A status other than 200 to answer with, as `ActorNotFound`.
        pub status: Mutex<u16>,
        pub asked: Mutex<Vec<Vec<String>>>,
        /// How long to hold each answer.
        pub delay_ms: Mutex<u64>,
    }

    async fn relationships(State(fake): State<Arc<FakeAppView>>, RawQuery(q): RawQuery) -> axum::response::Response {
        let query = q.unwrap_or_default();
        let param = |name: &str| -> Vec<String> {
            url::form_urlencoded::parse(query.as_bytes()).filter(|(k, _)| k == name).map(|(_, v)| v.into_owned()).collect()
        };
        let others = param("others");
        fake.asked.lock().expect("asked").push(others.clone());
        let delay = *fake.delay_ms.lock().expect("delay");
        if delay > 0 {
            tokio::time::sleep(Duration::from_millis(delay)).await;
        }
        let status = *fake.status.lock().expect("status");
        if status != 0 && status != 200 {
            return (StatusCode::from_u16(status).expect("status"), axum::Json(json!({"error": "ActorNotFound"}))).into_response();
        }
        let blockers = fake.blockers.lock().expect("blockers");
        let entries: Vec<Value> = others
            .iter()
            .map(|o| {
                let mut entry = json!({"$type": "app.bsky.graph.defs#relationship", "did": o});
                if blockers.contains(o) {
                    entry["blockedByList"] = json!(LIST);
                }
                entry
            })
            .collect();
        axum::Json(json!({"actor": param("actor").first(), "relationships": entries})).into_response()
    }

    /// Switch the fallback on for `farsight`, against a fake AppView.
    pub(crate) async fn with_fallback(farsight: &Farsight) -> Arc<FakeAppView> {
        let fake = Arc::new(FakeAppView::default());
        let base = serve_fake(Router::new().route("/xrpc/app.bsky.graph.getRelationships", get(relationships)).with_state(Arc::clone(&fake))).await;
        let client = AppViewClient::new("bluesky", &base, crate::appview_clients::AppViewAuth::Unauth, NetClient::loopback_for_tests());
        farsight.enable_fallback(client, &FarsightFallbackConfig::default(), &Shares::for_tests());
        fake
    }

    /// An account Farsight's grammar accepts, by number.
    fn numbered(i: usize) -> String {
        format!("did:plc:{:e>24}", format!("{i:x}").replace(['0', '1', '8', '9'], "f"))
    }

    /// Farsight answers once, then hangs until its breaker opens: down.
    async fn down(toml: &str) -> (Farsight, Arc<Fake>, Arc<FakeAppView>) {
        let (farsight, fake) = engine(&format!("timeout_ms = 50\n{toml}")).await;
        let upstream = with_fallback(&farsight).await;
        let none = HashSet::new();
        farsight.request(&member()).inbound_list_blocks(V, &page(&[A]), &none).await;
        *fake.delay_ms.lock().expect("delay") = 400;
        for i in 0..5 {
            farsight.request(&member()).inbound_list_blocks(V, &[numbered(i + 2)], &none).await;
        }
        assert!(farsight.gate().down);
        (farsight, fake, upstream)
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

    /// B-90: while Farsight is down, a member's ordinary lookup asks the
    /// fallback about the accounts its calls never got through for. A
    /// "blocked" becomes a block in hand; a "nothing" is only noted, so
    /// the pair is not sent again. Nothing else ever asks.
    #[tokio::test]
    async fn the_fallback_is_asked_only_while_farsight_is_down() {
        let none = HashSet::new();
        // Up: a miss on Farsight's side never reaches the fallback.
        let (farsight, _fake) = engine("").await;
        let upstream = with_fallback(&farsight).await;
        farsight.request(&member()).inbound_list_blocks(V, &page(&[A, B]), &none).await;
        assert!(upstream.asked.lock().expect("asked").is_empty());

        let (farsight, _fake, upstream) = down("").await;
        // The request whose own call was the fifth failure already asked.
        assert_eq!(*upstream.asked.lock().expect("asked"), [vec![numbered(6)]]);
        upstream.blockers.lock().expect("blockers").insert(B.to_string());
        let asked = || upstream.asked.lock().expect("asked").len();

        let blocked = farsight.request(&member()).inbound_list_blocks(V, &page(&[B, C]), &none).await;
        assert_eq!(blocked, HashMap::from([(B.to_string(), LIST.to_string())]));
        assert_eq!(upstream.asked.lock().expect("asked")[1], [B, C]);
        // B's block is in hand and C was told "nothing": neither is sent
        // again, and B still applies.
        let again = farsight.request(&member()).inbound_list_blocks(V, &page(&[B, C]), &none).await;
        assert_eq!((again.len(), asked()), (1, 2));
        assert_eq!(farsight.ledger_notes(), 2, "the fifth request's account, and C");
        // Never vouched: Thread Lens reads it as a block and nothing more.
        let lens = farsight.request(&member());
        let budget = PairBudget { max_calls: 1, timeout: Duration::from_millis(50) };
        let pairs = lens.pair_answers(V, &page(&[B, &numbered(40)]), budget, None).await;
        assert!(pairs.answers[B].inbound.list.is_some() && !pairs.answers[B].vouched && !pairs.vouches);
        // A Thread Lens round, a count, and a cache-only read ask nothing.
        assert_eq!(asked(), 2);
        let quiet = farsight.request(&member());
        quiet.set_no_fallback();
        quiet.inbound_list_blocks(V, &[numbered(41)], &none).await;
        let cached = farsight.request(&member());
        cached.set_cache_only();
        cached.inbound_list_blocks(V, &[numbered(42)], &none).await;
        assert_eq!(asked(), 2);
        // An anonymous caller's request draws on the non-member pool and
        // is charged by address; it has no fallback either way here, and
        // the relationships reader never asks.
        let rt = ReadCache::new(1024);
        farsight.request(&member()).relationship_lists(V, &[numbered(43)], &rt).await;
        assert_eq!(asked(), 2);
        // A purge of the viewer clears its blocks in hand and its notes.
        farsight.forget(V);
        assert_eq!((farsight.cached(), farsight.ledger_notes()), (0, 0));
    }

    /// B-93 (design-v3 §5.14.18): a block a live answer named is
    /// remembered and applies whenever the answer in hand does not vouch;
    /// a vouched clean answer forgets it at once, row included; a purge
    /// and `admin farsight forget` clear a viewer; a request begun before
    /// a purge learns nothing after it.
    #[tokio::test]
    async fn a_learned_block_applies_until_farsight_vouches_it_away() {
        let Some(db) = crate::test_support::fresh_db("farsight_memory_engine").await else {
            return;
        };
        let other_viewer = "did:plc:wwwwwwwwwwwwwwwwwwwwwwww";
        sqlx::query("INSERT INTO members (did, source) VALUES ($1, 'admin')").bind(V).execute(&db.pool).await.expect("member");
        // What an earlier run left: a row of the member, and one of an
        // account that is no longer a member.
        for viewer in [V, other_viewer] {
            sqlx::query("INSERT INTO learned_inbound_blocks (viewer_did, other_did, list_uri, source, confirmed_at) VALUES ($1, $2, $3, 'farsight', now())")
                .bind(viewer)
                .bind(C)
                .bind(LIST)
                .execute(&db.pool)
                .await
                .expect("row");
        }
        let rows = |other: &'static str| {
            let pool = db.pool.clone();
            async move {
                sqlx::query_scalar::<_, i64>("SELECT count(*) FROM learned_inbound_blocks WHERE viewer_did = $1 AND other_did = $2")
                    .bind(V)
                    .bind(other)
                    .fetch_one(&pool)
                    .await
                    .expect("count")
            }
        };
        let entries = |farsight: &Farsight| farsight.memory_stats().expect("memory on")["entries"].as_u64().expect("entries");

        let (farsight, fake) = engine("").await;
        let cfg = crate::config::FileConfig::parse("[farsight]\nurl = \"https://farsight.example.net\"\n[farsight.memory]\nenabled = true\n")
            .expect("parse")
            .farsight
            .memory;
        let loaded = farsight.load_memory(&db.pool, &cfg).await;
        assert_eq!(loaded.entries, Some(1), "the member's row, and not the other's");
        let none = HashSet::new();

        // A cache-only read with nothing cached: the block applies, and
        // nobody is asked.
        let cached = farsight.request(&member());
        cached.set_cache_only();
        let blocked = cached.inbound_list_blocks(V, &page(&[C]), &none).await;
        assert_eq!((blocked.get(C).map(String::as_str), calls(&fake)), (Some(LIST), 0));

        // Farsight answers: A blocks and is learned; C is vouched clean
        // and is forgotten on this very request, its row deleted by it.
        fake.blockers.lock().expect("blockers").insert(A.to_string());
        let blocked = farsight.request(&member()).inbound_list_blocks(V, &page(&[A, C]), &none).await;
        assert_eq!(blocked.keys().collect::<Vec<_>>(), [A]);
        assert_eq!((entries(&farsight), rows(C).await), (1, 0));

        // The 30 s reload clears the cache and never the memory. With
        // Farsight failing, A's block still applies; B has none.
        farsight.forget(V);
        *fake.status.lock().expect("status") = 500;
        let blocked = farsight.request(&member()).inbound_list_blocks(V, &page(&[A, B]), &none).await;
        assert_eq!(blocked, HashMap::from([(A.to_string(), LIST.to_string())]));
        // Thread Lens reads it as a block and never as a vouch.
        let budget = PairBudget { max_calls: 1, timeout: Duration::from_millis(200) };
        let pairs = farsight.request(&member()).pair_answers(V, &page(&[A, B]), budget, None).await;
        assert!(pairs.answers[A].inbound.list.is_some() && !pairs.answers[A].vouched && !pairs.answers.contains_key(B));

        // The writer makes the table match the copy.
        let writer = tokio::spawn(farsight.clone().run_memory_writer());
        for _ in 0..50 {
            if rows(A).await == 1 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        assert_eq!(rows(A).await, 1);
        // The operator's view holds counts and no DID.
        let status = serde_json::Value::Object(farsight.local_status()).to_string();
        assert!(status.contains("\"entries\":1") && !status.contains("did:"), "{status}");

        // A purge that removed the member: the copy and the rows go, and
        // a request of the old membership learns nothing afterwards.
        *fake.status.lock().expect("status") = 200;
        let old = farsight.request(&member());
        loaded.hook().run(V, false, true).await;
        assert_eq!((entries(&farsight), rows(A).await), (0, 0));
        let blocked = old.inbound_list_blocks(V, &page(&[A]), &none).await;
        assert_eq!((blocked.len(), entries(&farsight), farsight.cached()), (1, 0, 0));
        // A request begun after it learns as before.
        fake.blockers.lock().expect("blockers").insert(B.to_string());
        farsight.request(&member()).inbound_list_blocks(V, &page(&[B]), &none).await;
        assert_eq!(entries(&farsight), 1);

        // `admin farsight forget` clears the viewer; a deleted account
        // goes as the blocker of anyone.
        assert_eq!(farsight.forget_memory(V).await, Ok(1));
        assert_eq!(entries(&farsight), 0);
        farsight.request(&member()).inbound_list_blocks(V, &page(&[B]), &none).await;
        assert_eq!(entries(&farsight), 1);
        loaded.hook().run(B, true, false).await;
        assert_eq!(entries(&farsight), 0);

        writer.abort();
        db.drop_db().await;
    }

    /// B-95 (design-v3 §5.14.18.3): the purge itself calls the module. A
    /// member's removal clears its remembered blocks, copy and rows, in
    /// the marker step; the purge of an account that was no member leaves
    /// every viewer's in-flight additions alone.
    #[tokio::test]
    async fn a_purge_clears_the_removed_members_remembered_blocks() {
        use crate::aggregations::scope_state::PurgeKind;
        use crate::aggregations::{Purger, ScopeSets, ScopeState};
        use crate::private_tier::{KryphocronPrivateTier, PrivateTier};

        let Some(db) = crate::test_support::fresh_db("farsight_memory_purge").await else {
            return;
        };
        sqlx::query("INSERT INTO members (did, source, ready_at) VALUES ($1, 'admin', now())").bind(V).execute(&db.pool).await.expect("member");
        sqlx::query("INSERT INTO learned_inbound_blocks (viewer_did, other_did, list_uri, source, confirmed_at) VALUES ($1, $2, $3, 'farsight', now())")
            .bind(V)
            .bind(A)
            .bind(LIST)
            .execute(&db.pool)
            .await
            .expect("row");
        let (farsight, _fake) = engine("").await;
        let cfg = crate::config::FileConfig::parse("[farsight]\nurl = \"https://farsight.example.net\"\n[farsight.memory]\nenabled = true\n")
            .expect("parse")
            .farsight
            .memory;
        let loaded = farsight.load_memory(&db.pool, &cfg).await;
        assert_eq!(loaded.entries, Some(1));
        let private: Arc<dyn PrivateTier> = Arc::new(KryphocronPrivateTier);
        let scope = ScopeState::new(ScopeSets::default(), crate::config::ScopePolicy::MembersAndOneHop, 10_000);
        let purger = Purger::new(db.pool.clone(), scope, private);
        purger.set_hook(loaded.hook());
        let rows = || {
            let pool = db.pool.clone();
            async move { sqlx::query_scalar::<_, i64>("SELECT count(*) FROM learned_inbound_blocks").fetch_one(&pool).await.expect("count") }
        };
        let entries = |farsight: &Farsight| farsight.memory_stats().expect("memory on")["entries"].clone();

        // A deleted account that was no member: the count does not rise,
        // so a request in flight still learns.
        let in_flight = farsight.request(&member());
        purger.marker(B, PurgeKind::AccountDeleted).await.expect("marker");
        assert_eq!(in_flight.purges, farsight.inner.purges.load(Ordering::SeqCst));
        assert_eq!((rows().await, entries(&farsight)), (1, json!(1)));

        // The member's removal: the marker step alone clears both.
        purger.marker(V, PurgeKind::MemberRemoved).await.expect("marker");
        assert_eq!((rows().await, entries(&farsight)), (0, json!(0)));
        assert_ne!(in_flight.purges, farsight.inner.purges.load(Ordering::SeqCst));
        db.drop_db().await;
    }

    /// design-v3 §5.14.17: the fallback comes last in a request and never
    /// holds it past the request's own deadline.
    #[tokio::test]
    async fn a_request_past_its_deadline_asks_the_fallback_nothing() {
        let none = HashSet::new();
        let (farsight, _fake, upstream) = down("").await;
        let before = upstream.asked.lock().expect("asked").len();
        let late = member().with_deadline(Instant::now());
        farsight.request(&late).inbound_list_blocks(V, &[numbered(60)], &none).await;
        assert_eq!(upstream.asked.lock().expect("asked").len(), before);
        // A request with time left asks about the same account.
        farsight.request(&member()).inbound_list_blocks(V, &[numbered(60)], &none).await;
        assert_eq!(upstream.asked.lock().expect("asked").len(), before + 1);
    }

    /// B-91: the per-request cap bounds the calls; an `ActorNotFound`
    /// holds the fallback off for that viewer; five failures open the
    /// fallback's own breaker.
    #[tokio::test]
    async fn the_fallback_has_its_own_bounds_and_breaker() {
        let none = HashSet::new();
        let (farsight, _fake, upstream) = down("").await;
        let before = upstream.asked.lock().expect("asked").len();
        // 70 accounts: two calls of 30 at the default cap, the rest unasked.
        let letter = |n: u8| char::from(b'b' + n);
        let many: Vec<String> = (0..70u8).map(|i| format!("did:plc:{:a>24}", format!("{}{}", letter(i / 20), letter(i % 20)))).collect();
        farsight.request(&member()).inbound_list_blocks(V, &many, &none).await;
        let asked = upstream.asked.lock().expect("asked").clone();
        assert_eq!(asked[before..].iter().map(Vec::len).collect::<Vec<_>>(), [30, 30]);

        // The other AppView does not know the viewer: one call, then none
        // for this viewer; another viewer is still asked.
        let (farsight, _fake, upstream) = down("").await;
        *upstream.status.lock().expect("status") = 400;
        let before = upstream.asked.lock().expect("asked").len();
        farsight.request(&member()).inbound_list_blocks(V, &[numbered(50)], &none).await;
        farsight.request(&member()).inbound_list_blocks(V, &[numbered(51)], &none).await;
        assert_eq!(upstream.asked.lock().expect("asked").len(), before + 1);
        // Five failures in a row open the fallback's breaker for everyone.
        for (i, viewer) in ["did:plc:wwwwwwwwwwwwwwwwwwwwwwww", "did:plc:xxxxxxxxxxxxxxxxxxxxxxxx", "did:plc:yyyyyyyyyyyyyyyyyyyyyyyy", "did:plc:zzzzzzzzzzzzzzzzzzzzzzzz"].into_iter().enumerate() {
            farsight.request(&Requester::new(ChargeKey::member(viewer))).inbound_list_blocks(viewer, &[numbered(60 + i)], &none).await;
        }
        assert!(farsight.fallback_breaker_open());
        let before = upstream.asked.lock().expect("asked").len();
        let other = "did:plc:uuuuuuuuuuuuuuuuuuuuuuuu";
        farsight.request(&Requester::new(ChargeKey::member(other))).inbound_list_blocks(other, &[numbered(70)], &none).await;
        assert_eq!(upstream.asked.lock().expect("asked").len(), before);
    }

    /// design-v4 §14.2, the fallback block: every call by endpoint and
    /// class, every account of an answered call by result, the blocks it
    /// applied, its skips, and the ledger gauge; and
    /// `inbound_list_blocks_applied_total` for every block applied.
    #[tokio::test]
    async fn the_fallback_is_counted_call_by_call_and_pair_by_pair() {
        use crate::test_support::series;
        let recorder = metrics_exporter_prometheus::PrometheusBuilder::new().build_recorder();
        let handle = recorder.handle();
        let _local = metrics::set_default_local_recorder(&recorder);
        let value = |name: &str, labels: &[&str]| series(&handle.render(), name, labels).unwrap_or(0.0);
        let none = HashSet::new();

        // The fifth failing request asked about one account: told "clear".
        let (farsight, _fake, upstream) = down("").await;
        assert_eq!(value(fallback::METRIC_REQUESTS, &["endpoint=\"\"", "class=\"ok\""]), 1.0);
        assert_eq!(value(fallback::METRIC_PAIRS, &["result=\"clear\""]), 1.0);
        // B blocks through a list; C has no entry at all.
        upstream.blockers.lock().expect("blockers").insert(B.to_string());
        let timeline = member().for_endpoint("app.bsky.feed.getTimeline");
        let blocked = farsight.request(&timeline).inbound_list_blocks(V, &page(&[B, C]), &none).await;
        assert_eq!(blocked.len(), 1);
        assert_eq!(value(fallback::METRIC_REQUESTS, &["endpoint=\"app.bsky.feed.getTimeline\"", "class=\"ok\""]), 1.0);
        assert_eq!(value(fallback::METRIC_PAIRS, &["result=\"blocked\""]), 1.0);
        assert_eq!(value(fallback::METRIC_PAIRS, &["result=\"clear\""]), 2.0);
        assert_eq!(value(fallback::METRIC_BLOCKS_APPLIED, &[]), 1.0);
        assert_eq!(value(METRIC_INBOUND_APPLIED, &[]), 1.0);
        // The same block in hand on the next request is applied again,
        // from the cache: not a new fallback block.
        farsight.request(&member()).inbound_list_blocks(V, &page(&[B]), &none).await;
        assert_eq!((value(METRIC_INBOUND_APPLIED, &[]), value(fallback::METRIC_BLOCKS_APPLIED, &[])), (2.0, 1.0));
        // The ledger holds the two "nothing" pairs, sampled with the gate.
        farsight.sample_gate();
        assert_eq!(value(fallback::METRIC_LEDGER_ENTRIES, &[]), 2.0);
        farsight.forget(V);
        farsight.sample_gate();
        assert_eq!(value(fallback::METRIC_LEDGER_ENTRIES, &[]), 0.0);

        // `ActorNotFound`: counted by class, then the viewer's stop.
        *upstream.status.lock().expect("status") = 400;
        farsight.request(&member()).inbound_list_blocks(V, &[numbered(80)], &none).await;
        farsight.request(&member()).inbound_list_blocks(V, &[numbered(81)], &none).await;
        assert_eq!(value(fallback::METRIC_REQUESTS, &["class=\"actor_not_found\""]), 1.0);
        assert_eq!(value(fallback::METRIC_SKIPPED, &["reason=\"actor_stop\""]), 1.0);
        // A request past its deadline makes no call.
        let other = "did:plc:wwwwwwwwwwwwwwwwwwwwwwww";
        let late = Requester::new(ChargeKey::member(other)).with_deadline(Instant::now());
        farsight.request(&late).inbound_list_blocks(other, &[numbered(82)], &none).await;
        assert_eq!(value(fallback::METRIC_SKIPPED, &["reason=\"deadline\""]), 1.0);
        // 90 distinct accounts, three calls of 30, at the cap of two: one
        // call is not made.
        *upstream.status.lock().expect("status") = 200;
        let letter = |n: u8| char::from(b'b' + n);
        let many: Vec<String> = (0..90u8).map(|i| format!("did:plc:{:a>24}", format!("{}{}", letter(i / 20), letter(i % 20)))).collect();
        let third = "did:plc:xxxxxxxxxxxxxxxxxxxxxxxx";
        farsight.request(&Requester::new(ChargeKey::member(third))).inbound_list_blocks(third, &many, &none).await;
        assert_eq!(value(fallback::METRIC_SKIPPED, &["reason=\"call_cap\""]), 1.0);

        // Guard 1: Farsight answers while a fallback call is held. The
        // whole answer is discarded.
        let (farsight, fake, upstream) = down("").await;
        upstream.blockers.lock().expect("blockers").insert(B.to_string());
        *upstream.delay_ms.lock().expect("delay") = 300;
        let held = farsight.request(&member());
        let call = tokio::spawn(async move { held.inbound_list_blocks(V, &page(&[B]), &HashSet::new()).await });
        tokio::time::sleep(Duration::from_millis(100)).await;
        farsight.inner.gate.force_half_open();
        *fake.delay_ms.lock().expect("delay") = 0;
        farsight.request(&member()).inbound_list_blocks(V, &[numbered(90)], &none).await;
        assert!(call.await.expect("call").is_empty());
        assert_eq!(value(fallback::METRIC_DISCARDED, &[]), 1.0);
        assert_eq!(value(fallback::METRIC_PAIRS, &["result=\"blocked\""]), 1.0, "nothing new");

        // Guard 2: an entry Farsight wrote meanwhile drops the "blocked".
        let (farsight, _fake, upstream) = down("").await;
        upstream.blockers.lock().expect("blockers").insert(C.to_string());
        *upstream.delay_ms.lock().expect("delay") = 300;
        let held = farsight.request(&member());
        let call = tokio::spawn(async move { held.inbound_list_blocks(V, &page(&[C]), &HashSet::new()).await });
        tokio::time::sleep(Duration::from_millis(100)).await;
        {
            let settings = farsight.inner.settings;
            let mut cache = farsight.cache();
            let owner = Owner { viewer: V, member: true };
            cache.store(owner, C, PairAnswer::default(), Instant::now() + Duration::from_secs(30), Source::Farsight, &settings);
        }
        assert!(call.await.expect("call").is_empty());
        assert_eq!(value(fallback::METRIC_PAIRS, &["result=\"dropped\""]), 1.0);
        assert_eq!(value(fallback::METRIC_BLOCKS_APPLIED, &[]), 1.0, "nothing new");
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
        let client = FarsightClient::new(&crate::net::Transports::loopback_for_tests(), &base, "fsk_test").expect("client");
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

    /// design-v4 §14.2, B-91 "Never answered": `farsight_down` and
    /// `farsight_fallback_withheld{reason}` as a sample shows them, on an
    /// engine with the fallback on, beside what the fallback is asked.
    #[tokio::test]
    async fn the_gate_gauges_show_down_and_withheld() {
        use super::super::gate::{METRIC_DOWN, METRIC_WITHHELD};
        let recorder = metrics_exporter_prometheus::PrometheusBuilder::new().build_recorder();
        let handle = recorder.handle();
        let _local = metrics::set_default_local_recorder(&recorder);
        let gauges = |farsight: &Farsight| {
            farsight.sample_gate();
            let text = handle.render();
            let value = |name: &str, labels: &[&str]| crate::test_support::series(&text, name, labels).unwrap_or_else(|| panic!("{name} {labels:?} missing:\n{text}"));
            (
                value(METRIC_DOWN, &[]),
                value(METRIC_WITHHELD, &["reason=\"never_answered\""]),
                value(METRIC_WITHHELD, &["reason=\"unclean\""]),
            )
        };
        let none = HashSet::new();

        // Never answered: every series exists at 0 before any call; a hung
        // Farsight opens the breaker, the fallback is never asked, and the
        // gauge says why.
        let (farsight, fake) = engine("timeout_ms = 50\n").await;
        let upstream = with_fallback(&farsight).await;
        assert_eq!(gauges(&farsight), (0.0, 0.0, 0.0));
        *fake.delay_ms.lock().expect("delay") = 400;
        for i in 0..5 {
            farsight.request(&member()).inbound_list_blocks(V, &[numbered(i + 2)], &none).await;
        }
        assert!(farsight.breaker_open() && upstream.asked.lock().expect("asked").is_empty());
        assert_eq!(gauges(&farsight), (0.0, 1.0, 0.0));

        // Down: an answer, then five timeouts.
        let (farsight, _fake, upstream) = down("").await;
        assert!(!upstream.asked.lock().expect("asked").is_empty());
        assert_eq!(gauges(&farsight), (1.0, 0.0, 0.0));

        // Unclean: an answer, then five 401s. The fallback is not asked.
        let (farsight, fake) = engine("timeout_ms = 50\n").await;
        let upstream = with_fallback(&farsight).await;
        farsight.request(&member()).inbound_list_blocks(V, &page(&[A]), &none).await;
        *fake.status.lock().expect("status") = 401;
        for i in 0..5 {
            farsight.request(&member()).inbound_list_blocks(V, &[numbered(i + 2)], &none).await;
        }
        assert!(farsight.breaker_open() && upstream.asked.lock().expect("asked").is_empty());
        assert_eq!(gauges(&farsight), (0.0, 0.0, 1.0));
        // The open period ends by the clock: no event, and both are off.
        farsight.inner.gate.force_half_open();
        assert_eq!(gauges(&farsight), (0.0, 0.0, 0.0));
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

    const W: &str = "did:plc:wwwwwwwwwwwwwwwwwwwwwwww";
    const X: &str = "did:plc:xxxxxxxxxxxxxxxxxxxxxxxx";

    /// A visitor's handle: a Thread Lens caller that is not a ready
    /// member, charged to `req`.
    fn visitor(farsight: &Farsight, req: &Requester) -> Lookup {
        let lookup = farsight.request(req);
        lookup.set_visitor();
        lookup
    }

    fn lens_budget() -> PairBudget {
        PairBudget { max_calls: 2, timeout: Duration::from_millis(1000) }
    }

    /// An account Farsight's grammar accepts, by number below 400. Unlike
    /// [`numbered`], no two numbers give the same account.
    fn distinct(i: usize) -> String {
        let letter = |n: usize| char::from(b'b' + u8::try_from(n % 20).unwrap_or(0));
        format!("did:plc:{:a>24}", format!("{}{}", letter(i / 20), letter(i)))
    }

    /// Whether `viewer`'s part is in the index of visitor parts, and how
    /// the part itself is marked (`None`: there is no part).
    fn marks(farsight: &Farsight, viewer: &str) -> (bool, Option<bool>) {
        let cache = farsight.cache();
        (cache.visitors.contains(viewer), cache.viewers.get(viewer).map(|part| part.member))
    }

    /// Thread Lens access §4.6: a part is a member's or a visitor's by who
    /// wrote it; a member's write upgrades a visitor part and a visitor's
    /// never demotes; at the bound across viewers the visitors' parts go
    /// before any member's.
    #[tokio::test]
    async fn visitors_parts_are_marked_and_evicted_first() {
        let (farsight, _fake) = engine("cache_entries_total = 4\n").await;
        let s = farsight.inner.settings;
        let later = Instant::now() + Duration::from_secs(60);
        let put = |viewer: &str, member: bool, other: &str| {
            farsight.cache().store(Owner { viewer, member }, other, PairAnswer::clean(true), later, Source::Farsight, &s);
        };
        let (m1, m2) = (distinct(1), distinct(2));
        let visitors = || {
            let mut dids: Vec<String> = farsight.cache().visitors.iter().cloned().collect();
            dids.sort();
            dids
        };
        let parts = || farsight.cache().viewers.len();

        // Two visitors and two members, one entry each: the bound.
        put(W, false, A);
        put(&m1, true, A);
        put(X, false, A);
        put(&m2, true, A);
        assert_eq!((visitors(), farsight.cached()), (vec![W.to_string(), X.to_string()], 4));
        // Each further entry of a member removes one visitor's part, and
        // no member's.
        put(&m1, true, B);
        assert_eq!((visitors().len(), parts(), farsight.cached()), (1, 3, 4));
        put(&m2, true, B);
        assert_eq!((visitors().len(), parts(), farsight.cached()), (0, 2, 4));
        assert_eq!((marks(&farsight, &m1), marks(&farsight, &m2)), ((false, Some(true)), (false, Some(true))));
        // With no visitor's part left but the writer's own, a member's
        // part goes: today's rule.
        put(W, false, A);
        assert_eq!((visitors(), parts(), farsight.cached()), (vec![W.to_string()], 2, 3));

        // A member's write upgrades the part a visitor's write created,
        // and takes it out of the index. A later visitor's write does not
        // demote it.
        let (farsight, _fake) = engine("").await;
        let s = farsight.inner.settings;
        let put = |viewer: &str, member: bool, other: &str| {
            farsight.cache().store(Owner { viewer, member }, other, PairAnswer::clean(true), later, Source::Farsight, &s);
        };
        put(W, false, A);
        assert_eq!(marks(&farsight, W), (true, Some(false)));
        put(W, true, B);
        assert_eq!(marks(&farsight, W), (false, Some(true)));
        put(W, false, C);
        assert_eq!(marks(&farsight, W), (false, Some(true)));
        // A part a member's write created is a member's from the start.
        put(X, true, A);
        assert_eq!(marks(&farsight, X), (false, Some(true)));
        // Forgetting a viewer keeps the index in step.
        put(V, false, A);
        assert_eq!(marks(&farsight, V), (true, Some(false)));
        farsight.forget(V);
        assert_eq!(marks(&farsight, V), (false, None));
        assert_eq!(farsight.cached(), 4);
    }

    /// Thread Lens access §4.6, the sweep: expired entries go with their
    /// keys, a part left empty is deleted, a part that is gone is
    /// skipped, and the entry total stays exact.
    #[tokio::test]
    async fn the_sweep_removes_expired_entries_and_empty_parts() {
        let (farsight, _fake) = engine("").await;
        let s = farsight.inner.settings;
        let now = Instant::now();
        let later = now + Duration::from_secs(60);
        let put = |viewer: &str, member: bool, other: &str, expires: Instant| {
            farsight.cache().store(Owner { viewer, member }, other, PairAnswer::clean(true), expires, Source::Farsight, &s);
        };
        // A visitor whose entries have all expired; a member with one
        // expired and one live; a visitor with one live.
        put(W, false, A, now);
        put(W, false, B, now);
        put(V, true, A, now);
        put(V, true, B, later);
        put(X, false, A, later);
        assert_eq!(farsight.cached(), 5);
        let mut keys = farsight.cache().viewer_keys();
        keys.push("did:plc:gggggggggggggggggggggggg".to_string());
        assert_eq!(farsight.cache().sweep(&keys, now), 3);
        {
            let cache = farsight.cache();
            assert_eq!(cache.total, 2);
            assert!(!cache.viewers.contains_key(W) && !cache.visitors.contains(W), "no part at all");
            assert_eq!(cache.viewers[V].order, [B]);
            assert_eq!(cache.viewers[V].entries.len(), 1);
            assert_eq!(cache.visitors.iter().collect::<Vec<_>>(), [X]);
        }
        // An entry a read removed leaves its part empty and its key in
        // `order`; the sweep deletes that part too.
        assert!(farsight.cache().get(X, A, later).is_none());
        assert_eq!((farsight.cached(), farsight.cache().viewers[X].order.len()), (1, 1));
        assert_eq!(farsight.cache().sweep(&keys, later), 1);
        {
            let cache = farsight.cache();
            assert!(cache.viewers.is_empty() && cache.visitors.is_empty());
            assert_eq!(cache.total, 0);
        }

        // The whole pass, on an engine whose entries expire at once: a
        // visitor that never returned has no part afterwards. More
        // viewers than one slice holds, so the pass takes the lock more
        // than once.
        let (farsight, _fake) = engine("cache_ttl_secs = 0\n").await;
        visitor(&farsight, &Requester::test()).pair_answers(W, &page(&[A, B]), lens_budget(), None).await;
        assert!(farsight.cache().visitors.contains(W));
        let s = farsight.inner.settings;
        let past = Instant::now();
        for i in 0..(SWEEP_SLICE + 10) {
            farsight.cache().store(Owner { viewer: &distinct(i), member: i % 2 == 0 }, A, PairAnswer::clean(true), past, Source::Farsight, &s);
        }
        assert_eq!(farsight.cached(), SWEEP_SLICE + 12);
        assert_eq!(farsight.sweep_cache().await, SWEEP_SLICE + 12);
        let cache = farsight.cache();
        assert!(cache.viewers.is_empty() && cache.visitors.is_empty());
        assert_eq!(cache.total, 0);
    }

    /// Thread Lens access §4.7: a visitor's calls draw on the non-member
    /// pool, on a share of the viewer's own, whatever key its handle
    /// carries. The member pool is never touched.
    #[tokio::test]
    async fn a_visitor_draws_on_the_nonmember_pool_by_account() {
        // Each pool: burst 4. A share of one: burst 1.
        let (farsight, fake) = engine("rate_per_sec = 0.1\nburst = 8\nrequester_share = 0.25\n").await;
        let address = Requester::test();
        let ask = |lookup: Lookup, viewer: &'static str, i: usize| async move {
            lookup.pair_answers(viewer, &[distinct(i)], lens_budget(), None).await.calls
        };
        // Two visitors behind one address each have a share.
        ask(visitor(&farsight, &address), W, 2).await;
        ask(visitor(&farsight, &address), X, 3).await;
        assert_eq!(calls(&fake), 2);
        // A visitor's share is spent after one call: no answer, no call.
        let spent = visitor(&farsight, &address).pair_answers(W, &[distinct(4)], lens_budget(), None).await;
        assert!(spent.answers.is_empty() && !spent.vouches);
        assert_eq!(calls(&fake), 2);
        // A visitor whose handle carries a member key (a member that lost
        // readiness and is still charged by DID) draws there too.
        let y = "did:plc:yyyyyyyyyyyyyyyyyyyyyyyy";
        let z = "did:plc:zzzzzzzzzzzzzzzzzzzzzzzz";
        ask(visitor(&farsight, &Requester::new(ChargeKey::member(y))), y, 5).await;
        ask(visitor(&farsight, &address), z, 6).await;
        assert_eq!(calls(&fake), 4);
        // The non-member pool is empty now: a fifth visitor makes no call.
        ask(visitor(&farsight, &address), "did:plc:uuuuuuuuuuuuuuuuuuuuuuuu", 7).await;
        assert_eq!(calls(&fake), 4);
        // The member pool is whole: four members take its four tokens,
        // and a fifth finds both pools empty.
        for (i, did) in [V, A, B, C].into_iter().enumerate() {
            ask(farsight.request(&Requester::new(ChargeKey::member(did))), did, 10 + i).await;
        }
        assert_eq!(calls(&fake), 8);
        let d = "did:plc:dddddddddddddddddddddddd";
        ask(farsight.request(&Requester::new(ChargeKey::member(d))), d, 20).await;
        assert_eq!(calls(&fake), 8);
    }

    /// Thread Lens access §4.8: a visitor's failures never count on the
    /// read breaker, also when its handle carries a member key; and a
    /// visitor's call is never the half-open probe.
    #[tokio::test]
    async fn a_visitor_never_counts_on_the_breaker_and_never_probes() {
        let (farsight, fake) = engine("timeout_ms = 100\n").await;
        let keyed = Requester::new(ChargeKey::member(W));
        let asked = |did: &str| fake.asked.lock().expect("asked").iter().any(|chunk| chunk.iter().any(|d| d == did));
        // Closed: a visitor's call is admitted.
        visitor(&farsight, &keyed).pair_answers(W, &[distinct(2)], lens_budget(), None).await;
        assert_eq!(calls(&fake), 1);

        // Its failures are not recorded, however many.
        *fake.status.lock().expect("status") = 503;
        for i in 0..8 {
            visitor(&farsight, &keyed).pair_answers(W, &[distinct(10 + i)], lens_budget(), None).await;
        }
        assert_eq!(calls(&fake), 9);
        assert!(!farsight.breaker_open());
        // Five failed member calls open it. Then a visitor makes no call.
        for i in 0..5 {
            farsight.request(&member()).pair_answers(V, &[distinct(20 + i)], lens_budget(), None).await;
        }
        assert!(farsight.breaker_open() && farsight.gate().down);
        visitor(&farsight, &keyed).pair_answers(W, &[distinct(30)], lens_budget(), None).await;
        assert_eq!(calls(&fake), 14);

        // Half-open: the visitor's call is refused and leaves the probe.
        // The member's call that follows is the probe; it fails, and the
        // breaker is open again, so Farsight is still "down".
        farsight.inner.gate.force_half_open();
        assert!(!farsight.breaker_open());
        visitor(&farsight, &keyed).pair_answers(W, &[distinct(31)], lens_budget(), None).await;
        assert!(!asked(&distinct(31)) && !farsight.breaker_open(), "refused, and still half-open");
        farsight.request(&member()).pair_answers(V, &[distinct(32)], lens_budget(), None).await;
        assert!(asked(&distinct(32)), "the member's call took the probe");
        assert!(farsight.breaker_open() && farsight.gate().down);

        // Half-open with a member's probe in flight: the visitor's call
        // made meanwhile is refused. The probe succeeds and closes the
        // breaker, and the visitor's next call is admitted.
        farsight.inner.gate.force_half_open();
        *fake.status.lock().expect("status") = 200;
        *fake.delay_ms.lock().expect("delay") = 300;
        let probing = farsight.request(&member());
        let probe_account = [distinct(33)];
        let probe = probing.pair_answers(V, &probe_account, lens_budget(), None);
        let meanwhile = async {
            tokio::time::sleep(Duration::from_millis(100)).await;
            visitor(&farsight, &keyed).pair_answers(W, &[distinct(34)], lens_budget(), None).await
        };
        let (probed, refused) = futures::join!(probe, meanwhile);
        assert!(probed.vouches && asked(&distinct(33)));
        assert!(refused.answers.is_empty() && !asked(&distinct(34)));
        *fake.delay_ms.lock().expect("delay") = 0;
        let admitted = visitor(&farsight, &keyed).pair_answers(W, &[distinct(35)], lens_budget(), None).await;
        assert!(admitted.vouches && asked(&distinct(35)));
    }

    /// Thread Lens access §4.5: the memory of learned blocks is for
    /// members only. A visitor's request learns nothing, is shown nothing
    /// that was remembered, and forgets nothing. A ready member that is
    /// charged by address still learns and recalls.
    #[tokio::test]
    async fn the_memory_is_for_members_only() {
        let Some(db) = crate::test_support::fresh_db("farsight_memory_visitors").await else {
            return;
        };
        sqlx::query("INSERT INTO members (did, source, ready_at) VALUES ($1, 'admin', now())").bind(V).execute(&db.pool).await.expect("member");
        let (farsight, fake) = engine("").await;
        let cfg = crate::config::FileConfig::parse("[farsight]\nurl = \"https://farsight.example.net\"\n[farsight.memory]\nenabled = true\n")
            .expect("parse")
            .farsight
            .memory;
        assert_eq!(farsight.load_memory(&db.pool, &cfg).await.entries, Some(0));
        let stats = |farsight: &Farsight| {
            let stats = farsight.memory_stats().expect("memory on");
            (stats["entries"].clone(), stats["pending"].clone())
        };
        let rows = || {
            let pool = db.pool.clone();
            async move { sqlx::query_scalar::<_, i64>("SELECT count(*) FROM learned_inbound_blocks").fetch_one(&pool).await.expect("count") }
        };
        let address = Requester::test();
        fake.blockers.lock().expect("blockers").insert(A.to_string());

        // A visitor's lens sees an inbound list block: it applies to that
        // request, and leaves no entry and nothing to write.
        let seen = visitor(&farsight, &address).pair_answers(W, &page(&[A]), lens_budget(), None).await;
        assert_eq!(seen.answers[A].inbound.list.as_deref(), Some(LIST));
        assert_eq!(stats(&farsight), (json!(0), json!(0)));
        assert_eq!(rows().await, 0);

        // A ready member charged by address learns it.
        let seen = farsight.request(&address).pair_answers(V, &page(&[A]), lens_budget(), None).await;
        assert!(seen.answers[A].inbound.list.is_some());
        assert_eq!(stats(&farsight).0, json!(1));

        // Farsight fails and the cache is cleared. The member, still
        // charged by address, recalls the block. A visitor's request for
        // the same account (the member, had it lost readiness) does not.
        farsight.forget(V);
        *fake.status.lock().expect("status") = 500;
        let recalled = farsight.request(&address).pair_answers(V, &page(&[A]), lens_budget(), None).await;
        assert!(recalled.answers[A].inbound.list.is_some() && !recalled.answers[A].vouched);
        let unseen = visitor(&farsight, &address).pair_answers(V, &page(&[A]), lens_budget(), None).await;
        assert!(!unseen.answers.contains_key(A));

        // A vouched clean answer to a visitor's request forgets nothing.
        *fake.status.lock().expect("status") = 200;
        fake.blockers.lock().expect("blockers").clear();
        let clean = visitor(&farsight, &address).pair_answers(V, &page(&[A]), lens_budget(), None).await;
        assert!(clean.answers[A].vouched && !clean.answers[A].any_block());
        assert_eq!(stats(&farsight).0, json!(1));
        db.drop_db().await;
    }
}
