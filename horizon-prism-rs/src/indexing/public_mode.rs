//! The public-indexing mode (public-indexing design §3.5), its stored state
//! (§3.2), the startup sequence (§3.3, §3.5) and the one command that
//! changes it (§3.4).
//!
//! The mode lives in `ScopeSets.public`, behind the scope state's lock, so a
//! writer that holds the read guard sees one mode for its whole transaction.
//! The off rule (§5) applies in Draining, Off and Held; reads that need
//! complete public data ask whether the mode is On.
//!
//! Who writes `public_transition` (§3.4): a phase change (a command, S5, the
//! sweep's end, the fill's end) writes the whole value with generation + 1
//! under the write guard; the sweep and the fill write their own progress
//! under the read guard; everything but a command is conditional on the
//! phase and the generation the writer was handed. A background task never
//! re-reads its generation: it compares it with the in-memory one before
//! each unit of work, and exits when they differ or its conditional write
//! misses.

use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use chrono::{DateTime, SecondsFormat, SubsecRound, Utc};
use serde::{Serialize, Serializer};
use serde_json::{Value, json};
use sqlx::{PgConnection, PgPool};
use tokio::task::JoinHandle;

use super::public_sweep;
use crate::aggregations::{ScopeSets, ScopeState};
use crate::config::{IndexingConfig, PublicScope};

/// The `operator_settings` key of the switch (§3.2).
pub const SETTING_KEY: &str = "indexing.public_scope";
/// The `worker_state` key of the transition state (§3.2).
pub const TRANSITION_KEY: &str = "public_transition";

/// `public_indexing_phase{phase}`: 1 for the current mode, 0 otherwise.
pub const METRIC_PHASE: &str = "public_indexing_phase";
/// `public_task_superseded_total{task}` (§3.4).
pub const METRIC_SUPERSEDED: &str = "public_task_superseded_total";
/// `public_sweep_state_reset_total` (§3.5 step 2).
pub const METRIC_SWEEP_RESET: &str = "public_sweep_state_reset_total";

/// One of the five modes (§3.5).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum PublicMode {
    /// Everything is stored and served, as before the switch existed.
    On,
    /// Everything is stored; public endpoints wait for the fill (§8).
    Filling,
    /// The off rule applies and the sweep deletes public data (§7).
    Draining,
    /// The off rule applies; nothing is left to delete.
    Off,
    /// The stored state did not validate: the off rule applies and nothing
    /// is deleted or filled until a command writes a valid state.
    Held,
}

impl PublicMode {
    /// Every mode, in mirror order.
    pub const ALL: [Self; 5] = [Self::On, Self::Filling, Self::Draining, Self::Off, Self::Held];

    /// The off rule (§5) applies: only the needed set (§4) is stored, and
    /// no notification or feed item is written.
    #[must_use]
    pub fn off_rule(self) -> bool {
        matches!(self, Self::Draining | Self::Off | Self::Held)
    }

    /// Public data is complete. Filling is not On.
    #[must_use]
    pub fn is_on(self) -> bool {
        self == Self::On
    }

    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::On => "on",
            Self::Filling => "filling",
            Self::Draining => "draining",
            Self::Off => "off",
            Self::Held => "held",
        }
    }

    /// The value of the lock-free mirror the endpoint gate reads.
    #[must_use]
    pub fn as_u8(self) -> u8 {
        self as u8
    }

    /// The inverse of [`Self::as_u8`]. An unknown value reads as Held,
    /// which serves nothing public and deletes nothing.
    #[must_use]
    pub fn from_u8(v: u8) -> Self {
        match v {
            0 => Self::On,
            1 => Self::Filling,
            2 => Self::Draining,
            3 => Self::Off,
            _ => Self::Held,
        }
    }
}

/// `ScopeSets.public`: the mode, the generation of its phase (§3.4), and
/// the start of the most recent fill (§8.3).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PublicState {
    pub mode: PublicMode,
    pub generation: u64,
    pub last_fill_since: Option<DateTime<Utc>>,
}

/// `(On, 0, None)`: today's behaviour. Startup overwrites it before any
/// writer runs, so production never sees it; tests that build the scope
/// sets by default keep their behaviour (§3.5).
impl Default for PublicState {
    fn default() -> Self {
        Self { mode: PublicMode::On, generation: 0, last_fill_since: None }
    }
}

/// The settings of the switch's background work. Everything but
/// `fill_max_wait` is fixed by the design; the fields exist so tests can
/// run the same code at test speed and size.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PublicModeSettings {
    /// `indexing.fill_max_wait_hours`: how long a fill waits on its last
    /// jobs before it ends anyway (§8.5).
    pub fill_max_wait: Duration,
    /// How often the fill-completion checker looks (§8.5: 30 s).
    pub check_every: Duration,
    /// DIDs per fill batch (§8.3: 1 000).
    pub fill_batch: u32,
    /// Every table's starting window, in place of §7.2's 1 000 and 10 000.
    pub sweep_window: Option<u64>,
    /// The first backoff after a retryable sweep error (§7.2: 1 s,
    /// doubling, capped at 60 s).
    pub retry_base: Duration,
    /// The pause after any other sweep or fill error (§7.2: 60 s).
    pub failure_pause: Duration,
    /// How long the sweep may go without advancing before the stall alarm
    /// (§7.2: 30 minutes).
    pub stall_after: Duration,
}

impl PublicModeSettings {
    #[must_use]
    pub fn from_config(cfg: &IndexingConfig) -> Self {
        Self {
            fill_max_wait: Duration::from_secs(u64::from(cfg.fill_max_wait_hours) * 3600),
            check_every: Duration::from_secs(30),
            fill_batch: 1_000,
            sweep_window: None,
            retry_base: Duration::from_secs(1),
            failure_pause: Duration::from_secs(60),
            stall_after: Duration::from_secs(30 * 60),
        }
    }
}

/// A value of `indexing.public_scope` that can be applied (§3.1).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ScopeValue {
    Off,
    CommunityScoped,
}

impl ScopeValue {
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Off => "off",
            Self::CommunityScoped => "community_scoped",
        }
    }

    #[must_use]
    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "off" => Some(Self::Off),
            "community_scoped" => Some(Self::CommunityScoped),
            _ => None,
        }
    }

    /// The JSON form stored in `operator_settings.value`.
    #[must_use]
    pub fn to_json(self) -> Value {
        Value::String(self.as_str().to_string())
    }

    /// The file's value. `full` never gets here: validation refuses it at
    /// startup (`validate.rs`), so it maps to the safe value.
    #[must_use]
    pub fn from_config(scope: PublicScope) -> Self {
        match scope {
            PublicScope::CommunityScoped => Self::CommunityScoped,
            PublicScope::Off | PublicScope::Full => Self::Off,
        }
    }
}

/// `operator_settings.file_value` (§3.2): the file's value as of the last
/// startup, and whether the key was written in the file.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct FileValue {
    pub value: ScopeValue,
    pub explicit: bool,
}

impl FileValue {
    #[must_use]
    pub fn from_config(cfg: &IndexingConfig) -> Self {
        Self { value: ScopeValue::from_config(cfg.public_scope()), explicit: cfg.public_scope.is_some() }
    }

    #[must_use]
    pub fn to_json(self) -> Value {
        json!({ "value": self.value.as_str(), "explicit": self.explicit })
    }
}

/// A stored phase (§3.2). Held is never stored.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Phase {
    On,
    Filling,
    Draining,
    Off,
}

impl Phase {
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::On => "on",
            Self::Filling => "filling",
            Self::Draining => "draining",
            Self::Off => "off",
        }
    }

    #[must_use]
    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "on" => Some(Self::On),
            "filling" => Some(Self::Filling),
            "draining" => Some(Self::Draining),
            "off" => Some(Self::Off),
            _ => None,
        }
    }
}

/// `public_transition.sweep` (§3.2).
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct SweepState {
    /// A table of §7.4's order.
    pub table: String,
    /// Null before the table's first batch; else its last window key,
    /// encoded by the table's key (§3.2).
    pub after: Value,
    #[serde(serialize_with = "ser_ts")]
    pub last_advanced_at: DateTime<Utc>,
}

impl SweepState {
    /// The sweep at its start: the first table, no cursor.
    #[must_use]
    pub fn first(now: DateTime<Utc>) -> Self {
        Self { table: public_sweep::TABLES[0].name.to_string(), after: Value::Null, last_advanced_at: now }
    }
}

/// `fill.stage` (§3.2).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum FillStage {
    Enqueue,
    Waiting,
}

/// `fill.part` (§3.2, §8.3).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum FillPart {
    Members,
    Scope,
}

/// `public_transition.fill` (§3.2): everything the due times and
/// `waiting_since` depend on, committed with each batch.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct FillState {
    pub stage: FillStage,
    pub part: FillPart,
    /// The last DID handled in `part`; `""` at the start of a part.
    pub after: String,
    /// DIDs handled by the pass so far.
    pub enqueued: u64,
    #[serde(serialize_with = "ser_ts")]
    pub since: DateTime<Utc>,
    #[serde(serialize_with = "ser_opt_ts")]
    pub waiting_since: Option<DateTime<Utc>>,
}

impl FillState {
    /// A fresh fill (§8.2).
    #[must_use]
    pub fn fresh(now: DateTime<Utc>) -> Self {
        Self { stage: FillStage::Enqueue, part: FillPart::Members, after: String::new(), enqueued: 0, since: now, waiting_since: None }
    }
}

/// `worker_state['public_transition']` (§3.2).
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Transition {
    pub phase: Phase,
    pub generation: u64,
    /// When the phase began. Written by every phase change, and not
    /// validated: it is only reported.
    #[serde(serialize_with = "ser_opt_ts")]
    pub since: Option<DateTime<Utc>>,
    pub pending: Option<ScopeValue>,
    #[serde(serialize_with = "ser_opt_ts")]
    pub last_fill_since: Option<DateTime<Utc>>,
    pub sweep: Option<SweepState>,
    pub fill: Option<FillState>,
}

impl Transition {
    /// On or Off, with nothing running.
    #[must_use]
    pub fn settled(phase: Phase, generation: u64, now: DateTime<Utc>, last_fill_since: Option<DateTime<Utc>>) -> Self {
        Self { phase, generation, since: Some(now), pending: None, last_fill_since, sweep: None, fill: None }
    }

    /// §7.1: Draining, the sweep at its first table.
    #[must_use]
    pub fn draining(generation: u64, now: DateTime<Utc>, last_fill_since: Option<DateTime<Utc>>) -> Self {
        Self { sweep: Some(SweepState::first(now)), ..Self::settled(Phase::Draining, generation, now, last_fill_since) }
    }

    /// §8.2: Filling, a fresh fill; `last_fill_since` is `fill.since`.
    #[must_use]
    pub fn filling(generation: u64, now: DateTime<Utc>) -> Self {
        Self { fill: Some(FillState::fresh(now)), ..Self::settled(Phase::Filling, generation, now, Some(now)) }
    }

    /// The stored JSON.
    ///
    /// # Panics
    ///
    /// Never: every field serializes.
    #[must_use]
    pub fn to_json(&self) -> Value {
        serde_json::to_value(self).expect("a transition serializes")
    }
}

/// An RFC 3339 instant in UTC, at the microsecond precision Postgres keeps.
#[must_use]
pub fn rfc3339(t: &DateTime<Utc>) -> String {
    t.to_rfc3339_opts(SecondsFormat::Micros, true)
}

fn ser_ts<S: Serializer>(t: &DateTime<Utc>, s: S) -> Result<S::Ok, S::Error> {
    s.serialize_str(&rfc3339(t))
}

#[allow(clippy::ref_option)] // serde's `serialize_with` passes `&Option<T>`.
fn ser_opt_ts<S: Serializer>(t: &Option<DateTime<Utc>>, s: S) -> Result<S::Ok, S::Error> {
    match t {
        Some(t) => s.serialize_str(&rfc3339(t)),
        None => s.serialize_none(),
    }
}

/// An RFC 3339 string, or `None`.
#[must_use]
pub fn parse_ts(v: &Value) -> Option<DateTime<Utc>> {
    v.as_str()
        .and_then(|s| DateTime::parse_from_rfc3339(s).ok())
        .map(|t| t.with_timezone(&Utc))
}

/// Now, truncated to what Postgres and the stored JSON keep, so a value
/// read back compares equal.
#[must_use]
pub fn now() -> DateTime<Utc> {
    Utc::now().trunc_subsecs(6)
}

/// The stored transition, as validated at startup (§3.5 step 2).
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum Validated {
    Valid(Transition),
    /// The parsed generation (0 when it does not parse) and
    /// `last_fill_since` (null when it does not parse).
    Held { generation: u64, last_fill_since: Option<DateTime<Utc>> },
}

/// §3.5 step 2, as a pure function. Also returns the bad sweep value when
/// step 2.2 reset it; the returned transition then carries the reset sweep.
pub(super) fn validate(raw: &Value, now: DateTime<Utc>) -> (Validated, Option<Value>) {
    // 2.1: the generation and `last_fill_since`.
    let generation = raw.get("generation").and_then(Value::as_u64);
    let last_fill_since = match raw.get("last_fill_since") {
        None | Some(Value::Null) => Ok(None),
        Some(v) => parse_ts(v).map(Some).ok_or(()),
    };
    let (Some(generation), Ok(last_fill_since)) = (generation, last_fill_since) else {
        let held = Validated::Held { generation: generation.unwrap_or(0), last_fill_since: last_fill_since.ok().flatten() };
        return (held, None);
    };
    let held = Validated::Held { generation, last_fill_since };
    let phase = raw.get("phase").and_then(Value::as_str).and_then(Phase::parse);
    // 2.2: a draining sweep state that does not validate is reset.
    let mut reset = None;
    let sweep = if phase == Some(Phase::Draining) {
        let stored = raw.get("sweep").cloned().unwrap_or(Value::Null);
        match public_sweep::valid_state(&stored) {
            Some(s) => Some(s),
            None => {
                reset = Some(stored);
                Some(SweepState::first(now))
            }
        }
    } else {
        None
    };
    // 2.3: a filling fill state must hold.
    let fill = if phase == Some(Phase::Filling) {
        match raw.get("fill").and_then(valid_fill) {
            Some(f) => Some(f),
            None => return (held, reset),
        }
    } else {
        None
    };
    // 2.4: `pending`.
    let pending = match raw.get("pending") {
        None | Some(Value::Null) => None,
        Some(Value::String(s)) if s == "community_scoped" => Some(ScopeValue::CommunityScoped),
        Some(_) => return (held, reset),
    };
    let Some(phase) = phase else {
        // Step 3 maps an unknown phase to Held.
        return (held, reset);
    };
    if pending.is_some() && phase != Phase::Draining {
        return (held, reset);
    }
    let since = raw.get("since").and_then(parse_ts);
    let t = Transition { phase, generation, since, pending, last_fill_since, sweep, fill };
    (Validated::Valid(t), reset)
}

/// §3.5 step 2.3.
fn valid_fill(v: &Value) -> Option<FillState> {
    let stage = match v.get("stage")?.as_str()? {
        "enqueue" => FillStage::Enqueue,
        "waiting" => FillStage::Waiting,
        _ => return None,
    };
    let part = match v.get("part")?.as_str()? {
        "members" => FillPart::Members,
        "scope" => FillPart::Scope,
        _ => return None,
    };
    let after = v.get("after")?.as_str()?.to_string();
    let enqueued = v.get("enqueued")?.as_u64()?;
    let since = parse_ts(v.get("since")?)?;
    let waiting_since = match (stage, v.get("waiting_since")) {
        (FillStage::Enqueue, None | Some(Value::Null)) => None,
        (FillStage::Waiting, Some(w)) => {
            let w = parse_ts(w)?;
            if w < since || part != FillPart::Scope {
                return None;
            }
            Some(w)
        }
        _ => return None,
    };
    Some(FillState { stage, part, after, enqueued, since, waiting_since })
}

/// A task to spawn: what a phase change or startup hands back, with the
/// generation it runs under (§3.4).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Spawn {
    /// The sweep (§7.2), from its stored table and cursor.
    Sweep { generation: u64, sweep: SweepState },
    /// The fill pass (§8.3), from its stored part, cursor and count.
    Fill { generation: u64, fill: FillState },
    /// The fill-completion checker (§8.5), with the stored `waiting_since`.
    Checker { generation: u64, waiting_since: DateTime<Utc> },
}

impl Spawn {
    #[must_use]
    pub fn kind(&self) -> &'static str {
        match self {
            Self::Sweep { .. } => "sweep",
            Self::Fill { .. } => "fill",
            Self::Checker { .. } => "checker",
        }
    }

    #[must_use]
    pub fn generation(&self) -> u64 {
        match self {
            Self::Sweep { generation, .. } | Self::Fill { generation, .. } | Self::Checker { generation, .. } => *generation,
        }
    }
}

/// §3.5 step 3: the mode, and the task that would resume.
pub(super) fn map(value: Option<ScopeValue>, v: &Validated) -> (PublicState, Option<Spawn>) {
    let held = |generation, last_fill_since| (PublicState { mode: PublicMode::Held, generation, last_fill_since }, None);
    let t = match v {
        Validated::Held { generation, last_fill_since } => return held(*generation, *last_fill_since),
        Validated::Valid(t) => t,
    };
    let state = |mode| PublicState { mode, generation: t.generation, last_fill_since: t.last_fill_since };
    let generation = t.generation;
    match (value, t.phase, t.pending, &t.sweep, &t.fill) {
        (Some(ScopeValue::CommunityScoped), Phase::On, None, _, _) => (state(PublicMode::On), None),
        (Some(ScopeValue::CommunityScoped), Phase::Filling, None, _, Some(fill)) => {
            let task = match (fill.stage, fill.waiting_since) {
                (FillStage::Waiting, Some(waiting_since)) => Spawn::Checker { generation, waiting_since },
                _ => Spawn::Fill { generation, fill: fill.clone() },
            };
            (state(PublicMode::Filling), Some(task))
        }
        (Some(ScopeValue::Off), Phase::Draining, _, Some(sweep), _) => {
            (state(PublicMode::Draining), Some(Spawn::Sweep { generation, sweep: sweep.clone() }))
        }
        (Some(ScopeValue::Off), Phase::Off, None, _, _) => (state(PublicMode::Off), None),
        _ => held(t.generation, t.last_fill_since),
    }
}

/// What [`PublicIndexing::apply_scope`] did.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Applied {
    /// `draining`, `filling`, `pending_set`, `pending_cleared` or `no_op`.
    pub action: &'static str,
    pub mode: PublicMode,
    pub generation: u64,
    /// The task a phase change needs. The caller spawns it.
    pub spawn: Option<Spawn>,
}

/// What the background work reports, for health and `admin indexing
/// status` (§10). Kept in memory; the stored state is read alongside.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(super) struct Progress {
    pub sweep_deleted: u64,
    /// The sweep's current window size `n`.
    pub sweep_batch_rows: u64,
    pub sweep_last_error: Option<String>,
    pub sweep_stalled: bool,
    pub fill_remaining: Option<i64>,
    /// Jobs still running when the fill bound fired (§8.5).
    pub fill_incomplete: Option<i64>,
}

/// The switch: its stored state, its in-memory mode, and its background
/// work. Cloned into `serve`'s startup, the admin executor and health.
#[derive(Clone)]
pub struct PublicIndexing {
    pub(super) pool: PgPool,
    pub(super) scope: ScopeState,
    pub(super) settings: PublicModeSettings,
    pub(super) progress: Arc<Mutex<Progress>>,
    #[cfg(test)]
    pub(super) hooks: Arc<hooks::Hooks>,
}

impl PublicIndexing {
    #[must_use]
    pub fn new(pool: PgPool, scope: ScopeState, settings: PublicModeSettings) -> Self {
        Self {
            pool,
            scope,
            settings,
            progress: Arc::default(),
            #[cfg(test)]
            hooks: Arc::default(),
        }
    }

    /// The in-memory mode (the mirror; no guard).
    #[must_use]
    pub fn mode(&self) -> PublicMode {
        self.scope.public_mode()
    }

    pub(super) fn progress(&self, f: impl FnOnce(&mut Progress)) {
        if let Ok(mut p) = self.progress.lock() {
            f(&mut p);
        }
    }

    /// Set `ScopeSets.public`, its mirror and the phase gauge. `sets` is
    /// the write guard the caller holds.
    pub(super) fn set_state(&self, sets: &mut ScopeSets, state: PublicState) {
        self.scope.set_public(sets, state);
        for mode in PublicMode::ALL {
            metrics::gauge!(METRIC_PHASE, "phase" => mode.as_str()).set(if mode == state.mode { 1.0 } else { 0.0 });
        }
    }

    /// Spawn one task (§3.5 step 6, or after a run-time phase change).
    pub fn spawn(&self, task: Spawn) -> JoinHandle<()> {
        self.trace_spawn(task.kind());
        tracing::info!(event = "public_task_spawned", task = task.kind(), generation = task.generation(), "public indexing task started");
        tokio::spawn(self.clone().run(task))
    }

    /// The task's body, boxed so a task can spawn the next one (the sweep's
    /// end spawns the fill; the fill pass spawns the checker).
    fn run(self, task: Spawn) -> Pin<Box<dyn Future<Output = ()> + Send>> {
        Box::pin(async move {
            match task {
                Spawn::Sweep { generation, sweep } => self.run_sweep(generation, sweep).await,
                Spawn::Fill { generation, fill } => self.run_fill(generation, fill).await,
                Spawn::Checker { generation, waiting_since } => self.run_checker(generation, waiting_since).await,
            }
        })
    }

    /// The §3.5 startup sequence: record S1–S4 and S6; validate (sweep
    /// reset; Held); map without spawning; set memory; S5 through
    /// [`Self::apply_scope`]. Returns the one task to spawn. No writer may
    /// run during it.
    ///
    /// # Errors
    ///
    /// Any database error. Startup fails with it.
    pub async fn startup(&self, file: &IndexingConfig) -> Result<Option<Spawn>, sqlx::Error> {
        let f = FileValue::from_config(file);
        // Step 1: read and record.
        let s5 = self.record(f).await?;
        self.trace("record");
        // Step 2: validate.
        let value: Option<String> = sqlx::query_scalar("SELECT value #>> '{}' FROM operator_settings WHERE key = $1")
            .bind(SETTING_KEY)
            .fetch_optional(&self.pool)
            .await?;
        let value = value.as_deref().and_then(ScopeValue::parse);
        let raw: Option<Value> = sqlx::query_scalar("SELECT value FROM worker_state WHERE key = $1")
            .bind(TRANSITION_KEY)
            .fetch_optional(&self.pool)
            .await?;
        let (validated, reset) = validate(&raw.unwrap_or(Value::Null), now());
        if let (Some(bad), Validated::Valid(t)) = (reset, &validated)
            && let Some(sweep) = &t.sweep
        {
            let n = sqlx::query(
                "UPDATE worker_state SET value = jsonb_set(value, '{sweep}', $3), updated_at = now() \
                 WHERE key = 'public_transition' AND value->>'phase' = $1 AND (value->>'generation')::bigint = $2",
            )
            .bind(Phase::Draining.as_str())
            .bind(g64(t.generation))
            .bind(serde_json::to_value(sweep).unwrap_or(Value::Null))
            .execute(&self.pool)
            .await?
            .rows_affected();
            metrics::counter!(METRIC_SWEEP_RESET).increment(1);
            tracing::warn!(event = "public_sweep_state_reset", bad = %bad, written = n == 1, "the stored sweep state did not validate; the sweep restarts at its first table");
        }
        self.trace("validate");
        // Step 3: map, without spawning.
        let (state, mut task) = map(value, &validated);
        if state.mode == PublicMode::Held {
            tracing::error!(event = "public_indexing_held", generation = state.generation, "the stored public-indexing state did not validate: the off rule applies and nothing is deleted or filled until `admin indexing public-scope` writes a valid state");
        }
        self.trace("map");
        // Step 4: set memory.
        {
            let mut sets = self.scope.write_guard().await;
            self.set_state(&mut sets, state);
        }
        self.trace("memory");
        // Step 5: an operator edit, against the mapped mode.
        if let Some(value) = s5 {
            let applied = self.apply_scope(value, "file").await?;
            sqlx::query("UPDATE operator_settings SET file_value = $2, updated_by = 'file', updated_at = now() WHERE key = $1")
                .bind(SETTING_KEY)
                .bind(f.to_json())
                .execute(&self.pool)
                .await?;
            tracing::info!(event = "public_scope_file_applied", value = value.as_str(), action = applied.action, "the edited app.toml value was applied");
            if applied.spawn.is_some() {
                task = applied.spawn;
            }
            self.trace("s5");
        }
        let mode = self.mode();
        tracing::info!(event = "public_indexing_started", mode = mode.as_str(), task = task.as_ref().map(Spawn::kind), "public indexing state loaded");
        Ok(task)
    }

    /// Step 1: classify (§3.3) and write S1, S2, S3 or S6. Returns the
    /// value to apply for S5.
    async fn record(&self, f: FileValue) -> Result<Option<ScopeValue>, sqlx::Error> {
        let row: Option<(Value, Option<Value>)> = sqlx::query_as("SELECT value, file_value FROM operator_settings WHERE key = $1")
            .bind(SETTING_KEY)
            .fetch_optional(&self.pool)
            .await?;
        let not_applied = |stored: &Value| {
            if *stored != f.value.to_json() {
                tracing::warn!(
                    event = "public_scope_file_not_applied",
                    file_value = f.value.as_str(),
                    explicit = f.explicit,
                    running = %stored,
                    "app.toml's public_scope was not applied; run `admin indexing public-scope {}` to apply it",
                    f.value.as_str()
                );
            }
        };
        let Some((stored, file_value)) = row else {
            // S1 or S2: seed both rows.
            let present = public_data_present(&self.pool).await?;
            let (value, updated_by) = if present { (ScopeValue::CommunityScoped, "upgrade") } else { (f.value, "startup") };
            let phase = match value {
                ScopeValue::CommunityScoped => Phase::On,
                ScopeValue::Off => Phase::Off,
            };
            let t = Transition::settled(phase, 1, now(), None);
            let mut tx = self.pool.begin().await?;
            sqlx::query("INSERT INTO operator_settings (key, value, file_value, updated_by) VALUES ($1, $2, $3, $4)")
                .bind(SETTING_KEY)
                .bind(value.to_json())
                .bind(f.to_json())
                .bind(updated_by)
                .execute(&mut *tx)
                .await?;
            upsert_transition(&mut tx, &t).await?;
            tx.commit().await?;
            tracing::info!(event = "public_scope_seeded", case = if present { "upgrade" } else { "fresh" }, value = value.as_str(), "public_scope recorded");
            if present {
                not_applied(&value.to_json());
            }
            return Ok(None);
        };
        let record = async {
            sqlx::query("UPDATE operator_settings SET file_value = $2 WHERE key = $1")
                .bind(SETTING_KEY)
                .bind(f.to_json())
                .execute(&self.pool)
                .await
                .map(|_| ())
        };
        match file_value {
            // S3: first sight.
            None => {
                record.await?;
                not_applied(&stored);
                Ok(None)
            }
            // S4.
            Some(fv) if fv == f.to_json() => Ok(None),
            // S5: an operator edit.
            Some(_) if f.explicit => Ok(Some(f.value)),
            // S6: the key removed, or the default changed.
            Some(_) => {
                record.await?;
                tracing::info!(event = "public_scope_file_recorded", file_value = f.value.as_str(), "app.toml has no public_scope line; the running value stays");
                Ok(None)
            }
        }
    }

    /// The command's transaction (§3.4): `admin indexing public-scope`, the
    /// admin UI, and S5. Runs under the write guard, against the in-memory
    /// mode. Returns the task a phase change needs; the caller spawns it.
    ///
    /// # Errors
    ///
    /// Any database error; nothing changes.
    pub async fn apply_scope(&self, value: ScopeValue, updated_by: &str) -> Result<Applied, sqlx::Error> {
        let mut sets = self.scope.write_guard().await;
        let current = sets.public;
        let next = current.generation + 1;
        let at = now();
        let noop = |action| Applied { action, mode: current.mode, generation: current.generation, spawn: None };
        let change = match (current.mode, value) {
            // A Held exit takes the parsed generation (0 when it did not
            // parse) + 1 and the parsed `last_fill_since`: exactly what
            // memory holds.
            (PublicMode::On | PublicMode::Filling | PublicMode::Held, ScopeValue::Off) => {
                Transition::draining(next, at, current.last_fill_since)
            }
            (PublicMode::Off | PublicMode::Held, ScopeValue::CommunityScoped) => Transition::filling(next, at),
            (PublicMode::Draining, _) => {
                // Set or clear `pending` (§8.1); nothing else changes. A
                // literal, so the JSON null is never an SQL NULL.
                let pending = match value {
                    ScopeValue::CommunityScoped => "'\"community_scoped\"'::jsonb",
                    ScopeValue::Off => "'null'::jsonb",
                };
                let n = sqlx::query(&format!(
                    "UPDATE worker_state SET value = jsonb_set(value, '{{pending}}', {pending}), updated_at = now() \
                     WHERE key = 'public_transition' AND value->>'phase' = $1 AND (value->>'generation')::bigint = $2 \
                       AND coalesce(value->'pending', 'null'::jsonb) <> {pending}"
                ))
                .bind(Phase::Draining.as_str())
                .bind(g64(current.generation))
                .execute(&self.pool)
                .await?
                .rows_affected();
                drop(sets);
                return Ok(noop(match (value, n) {
                    (_, 0) => "no_op",
                    (ScopeValue::CommunityScoped, _) => "pending_set",
                    (ScopeValue::Off, _) => "pending_cleared",
                }));
            }
            (PublicMode::On | PublicMode::Filling, ScopeValue::CommunityScoped) | (PublicMode::Off, ScopeValue::Off) => {
                return Ok(noop("no_op"));
            }
        };
        let mut tx = self.pool.begin().await?;
        sqlx::query(
            "INSERT INTO operator_settings (key, value, updated_by) VALUES ($1, $2, $3) \
             ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value, updated_by = EXCLUDED.updated_by, updated_at = now()",
        )
        .bind(SETTING_KEY)
        .bind(value.to_json())
        .bind(updated_by)
        .execute(&mut *tx)
        .await?;
        upsert_transition(&mut tx, &change).await?;
        tx.commit().await?;
        let (mode, spawn, action) = match (&change.sweep, &change.fill) {
            (Some(sweep), _) => (PublicMode::Draining, Spawn::Sweep { generation: next, sweep: sweep.clone() }, "draining"),
            (_, Some(fill)) => (PublicMode::Filling, Spawn::Fill { generation: next, fill: fill.clone() }, "filling"),
            (None, None) => unreachable!("a command's phase change starts the sweep or the fill"),
        };
        self.set_state(&mut sets, PublicState { mode, generation: next, last_fill_since: change.last_fill_since });
        drop(sets);
        tracing::info!(event = "public_scope_applied", value = value.as_str(), from = current.mode.as_str(), to = mode.as_str(), generation = next, updated_by, "public indexing phase changed");
        Ok(Applied { action, mode, generation: next, spawn: Some(spawn) })
    }

    /// `admin indexing status` and the health block (§10): the row, the
    /// mode, the phase, the generation, `pending`, progress and any error.
    ///
    /// # Errors
    ///
    /// Any database error.
    pub async fn status(&self) -> Result<Value, sqlx::Error> {
        let row: Option<(Value, Option<Value>, DateTime<Utc>, String)> =
            sqlx::query_as("SELECT value, file_value, updated_at, updated_by FROM operator_settings WHERE key = $1")
                .bind(SETTING_KEY)
                .fetch_optional(&self.pool)
                .await?;
        let raw: Value = sqlx::query_scalar("SELECT value FROM worker_state WHERE key = $1")
            .bind(TRANSITION_KEY)
            .fetch_optional(&self.pool)
            .await?
            .unwrap_or(Value::Null);
        let mode = self.mode();
        let p = self.progress.lock().map(|p| p.clone()).unwrap_or_default();
        let field = |k: &str| raw.get(k).cloned().unwrap_or(Value::Null);
        let sweep = match raw.get("sweep") {
            Some(s) if s.is_object() => json!({
                "table": s.get("table"),
                "after": s.get("after"),
                "deleted": p.sweep_deleted,
                "batch_rows": p.sweep_batch_rows,
                "last_advanced_at": s.get("last_advanced_at"),
                "last_error": p.sweep_last_error,
            }),
            _ => Value::Null,
        };
        let fill = match raw.get("fill") {
            Some(f) if f.is_object() => json!({
                "stage": f.get("stage"),
                "part": f.get("part"),
                "enqueued": f.get("enqueued"),
                "remaining": p.fill_remaining,
                "waiting_since": f.get("waiting_since"),
                "incomplete": p.fill_incomplete,
            }),
            _ if p.fill_incomplete.is_some() => json!({ "incomplete": p.fill_incomplete }),
            _ => Value::Null,
        };
        let error = if mode == PublicMode::Held {
            json!("held")
        } else if p.sweep_stalled && mode == PublicMode::Draining {
            json!("sweep_stalled")
        } else {
            Value::Null
        };
        let (value, file, updated_at, updated_by) = match row {
            Some((v, f, at, by)) => (v, f.unwrap_or(Value::Null), json!(rfc3339(&at)), json!(by)),
            None => (Value::Null, Value::Null, Value::Null, Value::Null),
        };
        Ok(json!({
            "value": value,
            "mode": mode.as_str(),
            "phase": field("phase"),
            "generation": field("generation"),
            "pending": field("pending"),
            "since": field("since"),
            "last_fill_since": field("last_fill_since"),
            "file": file,
            "updated_at": updated_at,
            "updated_by": updated_by,
            "sweep": sweep,
            "fill": fill,
            "error": error,
        }))
    }

    /// A conditional write missed although the generations agreed: only a
    /// hand edit of the row can do that (§3.4).
    pub(super) fn superseded(task: &'static str, generation: u64) {
        metrics::counter!(METRIC_SUPERSEDED, "task" => task).increment(1);
        tracing::warn!(event = "public_task_superseded", task, generation, "the stored phase or generation changed under the task; it exits");
    }

    /// Record a startup step (T-14's order hook).
    fn trace(&self, step: &'static str) {
        #[cfg(test)]
        self.hooks.steps.lock().expect("hooks").push(step);
        #[cfg(not(test))]
        let _ = step;
    }

    fn trace_spawn(&self, kind: &'static str) {
        #[cfg(test)]
        self.hooks.spawned.lock().expect("hooks").push(kind);
        #[cfg(not(test))]
        let _ = kind;
    }

    /// A test pause point (T-11, T-21); nothing outside tests.
    pub(super) async fn pause(&self, point: &'static str) {
        #[cfg(test)]
        self.hooks.pause(point).await;
        #[cfg(not(test))]
        let _ = point;
    }
}

/// The whole transition, written unconditionally: S1/S2 and a command's
/// phase change.
async fn upsert_transition(conn: &mut PgConnection, t: &Transition) -> Result<(), sqlx::Error> {
    sqlx::query(
        "INSERT INTO worker_state (key, value, updated_at) VALUES ($1, $2, now()) \
         ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value, updated_at = now()",
    )
    .bind(TRANSITION_KEY)
    .bind(t.to_json())
    .execute(conn)
    .await?;
    Ok(())
}

/// The whole transition, written only if the stored phase and generation
/// are `from` and `generation` (the sweep's end and the fill's end, §3.4).
/// Returns whether it matched.
pub(super) async fn replace_if(conn: &mut PgConnection, from: Phase, generation: u64, t: &Transition) -> Result<bool, sqlx::Error> {
    let n = sqlx::query(
        "UPDATE worker_state SET value = $3, updated_at = now() \
         WHERE key = 'public_transition' AND value->>'phase' = $1 AND (value->>'generation')::bigint = $2",
    )
    .bind(from.as_str())
    .bind(g64(generation))
    .bind(t.to_json())
    .execute(conn)
    .await?
    .rows_affected();
    Ok(n == 1)
}

/// A generation as Postgres's `bigint`.
pub(super) fn g64(generation: u64) -> i64 {
    i64::try_from(generation).unwrap_or(i64::MAX)
}

/// "Public data present" (§3.3): a row exists in any table the sweep
/// empties or thins.
///
/// # Errors
///
/// Any database error.
pub async fn public_data_present(pool: &PgPool) -> Result<bool, sqlx::Error> {
    let any = public_sweep::TABLES
        .iter()
        .map(|t| format!("EXISTS (SELECT 1 FROM {})", t.name))
        .collect::<Vec<_>>()
        .join(" OR ");
    sqlx::query_scalar(&format!("SELECT {any}")).fetch_one(pool).await
}

/// Test hooks: the startup order, the spawn count, pause points, injected
/// errors and the sweep's window sizes.
#[cfg(test)]
pub(super) mod hooks {
    use std::collections::{HashMap, VecDeque};
    use std::sync::Mutex;
    use std::sync::atomic::AtomicUsize;

    use std::sync::Arc;
    use tokio::sync::Notify;

    #[derive(Default)]
    pub struct Hooks {
        pub steps: Mutex<Vec<&'static str>>,
        pub spawned: Mutex<Vec<&'static str>>,
        pub repairs: AtomicUsize,
        /// `(table, n)` of each sweep batch attempt.
        pub windows: Mutex<Vec<(&'static str, u64)>>,
        /// SQLSTATEs raised, one per batch, at the start of the next ones.
        pub inject: Mutex<VecDeque<&'static str>>,
        pauses: Mutex<HashMap<&'static str, Pause>>,
    }

    /// `(reached, resume)`.
    pub type Pause = (Arc<Notify>, Arc<Notify>);

    impl Hooks {
        /// Pause the next pass through `point`. Returns `(reached, resume)`.
        pub fn arm(&self, point: &'static str) -> Pause {
            let pair = (Arc::new(Notify::new()), Arc::new(Notify::new()));
            self.pauses.lock().expect("hooks").insert(point, pair.clone());
            pair
        }

        pub async fn pause(&self, point: &'static str) {
            let armed = self.pauses.lock().expect("hooks").remove(point);
            if let Some((reached, resume)) = armed {
                reached.notify_one();
                resume.notified().await;
            }
        }

        pub fn spawned(&self) -> Vec<&'static str> {
            self.spawned.lock().expect("hooks").clone()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_off_rule_applies_in_the_off_modes_only() {
        let off: Vec<PublicMode> = PublicMode::ALL.into_iter().filter(|m| m.off_rule()).collect();
        assert_eq!(off, [PublicMode::Draining, PublicMode::Off, PublicMode::Held]);
        let on: Vec<PublicMode> = PublicMode::ALL.into_iter().filter(|m| m.is_on()).collect();
        assert_eq!(on, [PublicMode::On]);
    }

    #[test]
    fn the_mirror_round_trips() {
        for m in PublicMode::ALL {
            assert_eq!(PublicMode::from_u8(m.as_u8()), m);
        }
        assert_eq!(PublicMode::from_u8(200), PublicMode::Held);
    }

    /// T-1: the in-memory default is today's behaviour, and the fill bound
    /// defaults to a day.
    #[test]
    fn defaults() {
        assert_eq!(PublicState::default(), PublicState { mode: PublicMode::On, generation: 0, last_fill_since: None });
        let settings = PublicModeSettings::from_config(&IndexingConfig::default());
        assert_eq!(settings.fill_max_wait, Duration::from_secs(24 * 3600));
        assert_eq!(settings.check_every, Duration::from_secs(30));
        assert_eq!(settings.stall_after, Duration::from_secs(1800));
        assert_eq!(settings.fill_batch, 1_000);
    }

    /// §3.2: the stored shape, and the timestamps round-trip.
    #[test]
    fn the_transition_json_has_the_stored_shape() {
        let at = parse_ts(&json!("2026-10-14T12:00:00.123456Z")).expect("ts");
        let t = Transition::filling(4, at);
        let v = t.to_json();
        assert_eq!(v["phase"], "filling");
        assert_eq!(v["generation"], 4);
        assert_eq!(v["pending"], Value::Null);
        assert_eq!(v["last_fill_since"], "2026-10-14T12:00:00.123456Z");
        assert_eq!(v["fill"], json!({"stage": "enqueue", "part": "members", "after": "", "enqueued": 0,
                                     "since": "2026-10-14T12:00:00.123456Z", "waiting_since": null}));
        assert_eq!(validate(&v, at), (Validated::Valid(t), None));
        let d = Transition::draining(5, at, None).to_json();
        assert_eq!(d["sweep"], json!({"table": "feed_items", "after": null, "last_advanced_at": "2026-10-14T12:00:00.123456Z"}));
    }
}
