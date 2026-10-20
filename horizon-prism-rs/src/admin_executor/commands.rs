//! The mutating admin commands (design-v2 §3.3) and their handlers.
//!
//! A command is stored in `admin_commands` as `(command, args jsonb)`; the
//! CLI writes it ([`AdminCommand::to_row`]) and `serve`'s executor parses and
//! runs it ([`Handlers::execute`]) through the same code paths and locks as
//! the rest of `serve`.

use std::path::PathBuf;
use std::sync::Arc;

use redis::aio::ConnectionManager;
use serde_json::{Value, json};
use sqlx::PgPool;

use crate::admin::operators::{self, OperatorIdentity};
pub use crate::admin::operators::Actor;
use crate::admin::settings::live::is_live;
pub use crate::admin::settings::store::Change;
use crate::admin::settings::{LiveSettings, LiveValues, RestartHandle, store};
use crate::config::EnvConfig;

use crate::aggregations::membership::{self, AddOutcome, Denial, add_member, is_denied};
use crate::aggregations::purge::PurgeError;
use crate::aggregations::{MarkerGuard, MarkerOutcome, PurgeKind, Purger, Rebuild, ScopeState};
use crate::db::locks::{lock_did_exclusive, uri_author};
use crate::farsight::Farsight;
use crate::firehose::backfill::BackfillSource;
use crate::firehose::gap::{self, GapCause};
use crate::firehose::index_request::{IndexError, index_record};
use crate::firehose::jobs::{self, Depth};
use crate::firehose::writer::Writer;
use crate::indexing::{PublicIndexing, ScopeValue};
use crate::private_tier::PrivateTier;
use crate::subscriptions::references::{Budget, drop_member};
use crate::subscriptions::{jobs as list_sync, triggers};

/// Label values accepted by `label add` (§7.8). Anything else is rejected.
pub const LABEL_VALUES: [&str; 6] = ["!takedown", "!hide", "porn", "sexual", "nudity", "graphic-media"];

/// One mutating command.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AdminCommand {
    MemberAdd { did: String },
    MemberRemove { did: String },
    /// `member deny <did> [--reason]` (membership design §5.2).
    MemberDeny { did: String, reason: Option<String> },
    /// `member allow <did>` (membership design §5.3).
    MemberAllow { did: String },
    LabelAdd { subject: String, val: String },
    LabelRemove { subject: String, val: String },
    ScopeRebuild { dry_run: bool },
    Backfill { did: String, depth: Option<Depth> },
    /// `resync --private-and-members`.
    Resync,
    DeadletterReplay { id: String },
    DeadletterDrop { id: String },
    /// `deadletter sweep [--dry-run]` (actionable-warnings design §6.6):
    /// remove the listed whole-entry dead letters the firehose would now
    /// drop.
    DeadletterSweep { dry_run: bool },
    /// `farsight status`: Farsight's own report, in brief.
    FarsightStatus,
    /// `farsight prime <did> [--force]`: ask Farsight to read a repository.
    FarsightPrime { did: String, force: bool },
    /// `farsight forget <viewer-did>`: drop one viewer's remembered blocks.
    FarsightForget { did: String },
    /// `lists sync <list-uri>`: sync a subscribed list's owner now.
    ListsSync { uri: String },
    /// `lists purge <did>`: delete the DID's list mutes and the list
    /// records it authored.
    ListsPurge { did: String },
    /// `indexing public-scope <off|community_scoped>`: the public-indexing
    /// switch (public-indexing design §3.4).
    IndexingPublicScope { value: ScopeValue },
    /// `indexing status`: the switch's row, mode, phase and progress.
    IndexingStatus,
    /// `operator add <handle-or-did> [--note]` (admin-ui design §5.7): the
    /// account is resolved afresh here. With `add_host` (the CLI), its
    /// exact host is allowed unless a pattern covers it; without (the UI),
    /// a host the list does not allow fails the command.
    OperatorAdd { input: String, note: Option<String>, add_host: bool },
    /// `operator remove <did>`: never the last operator.
    OperatorRemove { did: String },
    /// `operator host add <pattern>`.
    OperatorHostAdd { pattern: String },
    /// `operator host remove <pattern>`: refused when no operator's last
    /// known host would stay allowed.
    OperatorHostRemove { pattern: String },
    /// `settings set` (admin-ui design §8.3): one group's changes, or one
    /// from the CLI, applied together or not at all.
    SettingsSet { changes: Vec<Change> },
    /// `restart` (admin-ui design §8.5): settled `done`, then `serve` shuts
    /// down as at SIGTERM and exits for its supervisor to start it again.
    Restart,
}

/// The longest operator note accepted.
pub const MAX_NOTE: usize = 300;

/// A command that cannot be parsed or is invalid.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("invalid admin command: {0}")]
pub struct InvalidCommand(pub String);

fn is_did(s: &str) -> bool {
    proto_blue::syntax::Did::is_valid(s)
}

fn is_stream_id(s: &str) -> bool {
    s.split_once('-')
        .is_some_and(|(a, b)| !a.is_empty() && !b.is_empty() && a.bytes().chain(b.bytes()).all(|c| c.is_ascii_digit()))
}

impl AdminCommand {
    /// The stored `(command, args)`.
    #[must_use]
    pub fn to_row(&self) -> (&'static str, Value) {
        match self {
            Self::MemberAdd { did } => ("member add", json!({ "did": did })),
            Self::MemberRemove { did } => ("member remove", json!({ "did": did })),
            Self::MemberDeny { did, reason } => ("member deny", json!({ "did": did, "reason": reason })),
            Self::MemberAllow { did } => ("member allow", json!({ "did": did })),
            Self::LabelAdd { subject, val } => ("label add", json!({ "subject": subject, "val": val })),
            Self::LabelRemove { subject, val } => ("label remove", json!({ "subject": subject, "val": val })),
            Self::ScopeRebuild { dry_run } => ("scope rebuild", json!({ "dry_run": dry_run })),
            Self::Backfill { did, depth } => ("backfill", json!({ "did": did, "depth": depth.map(Depth::as_str) })),
            Self::Resync => ("resync", json!({ "private_and_members": true })),
            Self::DeadletterReplay { id } => ("deadletter replay", json!({ "id": id })),
            Self::DeadletterDrop { id } => ("deadletter drop", json!({ "id": id })),
            Self::DeadletterSweep { dry_run } => ("deadletter sweep", json!({ "dry_run": dry_run })),
            Self::FarsightStatus => ("farsight status", json!({})),
            Self::FarsightPrime { did, force } => ("farsight prime", json!({ "did": did, "force": force })),
            Self::FarsightForget { did } => ("farsight forget", json!({ "did": did })),
            Self::ListsSync { uri } => ("lists sync", json!({ "uri": uri })),
            Self::ListsPurge { did } => ("lists purge", json!({ "did": did })),
            Self::IndexingPublicScope { value } => ("indexing public-scope", json!({ "value": value.as_str() })),
            Self::IndexingStatus => ("indexing status", json!({})),
            Self::OperatorAdd { input, note, add_host } => {
                ("operator add", json!({ "input": input, "note": note, "add_host": add_host }))
            }
            Self::OperatorRemove { did } => ("operator remove", json!({ "did": did })),
            Self::OperatorHostAdd { pattern } => ("operator host add", json!({ "pattern": pattern })),
            Self::OperatorHostRemove { pattern } => ("operator host remove", json!({ "pattern": pattern })),
            Self::SettingsSet { changes } => {
                let changes: Vec<Value> = changes
                    .iter()
                    .map(|c| match &c.value {
                        Some(v) => json!({ "key": c.key, "value": v, "base_version": c.base_version }),
                        None => json!({ "key": c.key, "reset": true, "base_version": c.base_version }),
                    })
                    .collect();
                ("settings set", json!({ "changes": changes }))
            }
            Self::Restart => ("restart", json!({})),
        }
    }

    /// Parse and validate a stored row.
    ///
    /// # Errors
    ///
    /// [`InvalidCommand`] for an unknown command or invalid arguments.
    pub fn parse(command: &str, args: &Value) -> Result<Self, InvalidCommand> {
        let s = |k: &str| {
            args.get(k)
                .and_then(Value::as_str)
                .map(str::to_string)
                .ok_or_else(|| InvalidCommand(format!("{command}: missing {k}")))
        };
        let did = || {
            let d = s("did")?;
            if is_did(&d) { Ok(d) } else { Err(InvalidCommand(format!("{command}: invalid did {d:?}"))) }
        };
        let label = || -> Result<(String, String), InvalidCommand> {
            let subject = s("subject")?;
            if !(is_did(&subject) || uri_author(&subject).is_some_and(is_did)) {
                return Err(InvalidCommand(format!("{command}: subject must be a DID or an at-uri")));
            }
            let val = s("val")?;
            if !LABEL_VALUES.contains(&val.as_str()) {
                return Err(InvalidCommand(format!("{command}: label value {val:?} is not one of {LABEL_VALUES:?}")));
            }
            Ok((subject, val))
        };
        let id = || {
            let id = s("id")?;
            if is_stream_id(&id) { Ok(id) } else { Err(InvalidCommand(format!("{command}: invalid id {id:?}"))) }
        };
        Ok(match command {
            "member add" => Self::MemberAdd { did: did()? },
            "member remove" => Self::MemberRemove { did: did()? },
            "member deny" => Self::MemberDeny {
                did: did()?,
                reason: match args.get("reason") {
                    None | Some(Value::Null) => None,
                    Some(Value::String(r)) => Some(r.clone()),
                    Some(_) => return Err(InvalidCommand(format!("{command}: reason must be a string"))),
                },
            },
            "member allow" => Self::MemberAllow { did: did()? },
            "label add" => {
                let (subject, val) = label()?;
                Self::LabelAdd { subject, val }
            }
            "label remove" => {
                let (subject, val) = label()?;
                Self::LabelRemove { subject, val }
            }
            "scope rebuild" => Self::ScopeRebuild {
                dry_run: args.get("dry_run").and_then(Value::as_bool).unwrap_or(false),
            },
            "backfill" => {
                let depth = match args.get("depth").and_then(Value::as_str) {
                    None => None,
                    Some(d) => Some(Depth::parse(d).ok_or_else(|| InvalidCommand(format!("backfill: unknown depth {d:?}")))?),
                };
                Self::Backfill { did: did()?, depth }
            }
            "resync" => Self::Resync,
            "deadletter replay" => Self::DeadletterReplay { id: id()? },
            "deadletter drop" => Self::DeadletterDrop { id: id()? },
            "deadletter sweep" => Self::DeadletterSweep {
                dry_run: args.get("dry_run").and_then(Value::as_bool).unwrap_or(false),
            },
            "farsight status" => Self::FarsightStatus,
            "farsight prime" => Self::FarsightPrime {
                did: did()?,
                force: args.get("force").and_then(Value::as_bool).unwrap_or(false),
            },
            "farsight forget" => Self::FarsightForget { did: did()? },
            "lists sync" => {
                let uri = s("uri")?;
                let is_list = uri_author(&uri).is_some_and(is_did) && uri.split('/').nth(3) == Some("app.bsky.graph.list");
                if !is_list {
                    return Err(InvalidCommand(format!("{command}: {uri:?} is not a list's at-uri")));
                }
                Self::ListsSync { uri }
            }
            "lists purge" => Self::ListsPurge { did: did()? },
            "indexing public-scope" => {
                let value = s("value")?;
                Self::IndexingPublicScope {
                    value: ScopeValue::parse(&value)
                        .ok_or_else(|| InvalidCommand(format!("{command}: {value:?} is not off or community_scoped")))?,
                }
            }
            "indexing status" => Self::IndexingStatus,
            "operator add" => {
                let input = s("input")?;
                if !(is_did(&input) || (proto_blue::syntax::Handle::is_valid(&input) && input == input.to_ascii_lowercase())) {
                    return Err(InvalidCommand(format!("{command}: {input:?} is not a DID or a lowercase handle")));
                }
                let note = match args.get("note") {
                    None | Some(Value::Null) => None,
                    Some(Value::String(n)) if n.chars().count() <= MAX_NOTE => Some(n.clone()),
                    Some(_) => return Err(InvalidCommand(format!("{command}: note must be text of at most {MAX_NOTE} characters"))),
                };
                Self::OperatorAdd {
                    input,
                    note,
                    add_host: args.get("add_host").and_then(Value::as_bool).unwrap_or(false),
                }
            }
            "operator remove" => Self::OperatorRemove { did: did()? },
            "operator host add" | "operator host remove" => {
                let pattern = s("pattern")?;
                if crate::admin::operators::parse_pattern(&pattern).as_deref() != Ok(pattern.as_str()) {
                    return Err(InvalidCommand(format!("{command}: {pattern:?} is not a lowercase host or *.host pattern")));
                }
                if command == "operator host add" { Self::OperatorHostAdd { pattern } } else { Self::OperatorHostRemove { pattern } }
            }
            "settings set" => Self::SettingsSet { changes: parse_changes(command, args)? },
            "restart" => Self::Restart,
            other => return Err(InvalidCommand(format!("unknown command {other:?}"))),
        })
    }

    /// The DID whose per-DID ordering this command takes part in (§6.11.7).
    #[must_use]
    pub fn did(&self) -> Option<&str> {
        match self {
            Self::MemberAdd { did }
            | Self::MemberRemove { did }
            | Self::MemberDeny { did, .. }
            | Self::MemberAllow { did }
            | Self::Backfill { did, .. }
            | Self::ListsPurge { did } => Some(did),
            Self::LabelAdd { subject, .. } | Self::LabelRemove { subject, .. } => {
                if is_did(subject) { Some(subject) } else { uri_author(subject) }
            }
            // Waits on a purge of the owner, whose end requests the sync
            // itself.
            Self::ListsSync { uri } => uri_author(uri),
            Self::ScopeRebuild { .. }
            | Self::Resync
            | Self::DeadletterReplay { .. }
            | Self::DeadletterDrop { .. }
            | Self::DeadletterSweep { .. }
            | Self::FarsightStatus
            | Self::IndexingPublicScope { .. }
            | Self::IndexingStatus
            | Self::OperatorAdd { .. }
            | Self::OperatorRemove { .. }
            | Self::OperatorHostAdd { .. }
            | Self::OperatorHostRemove { .. }
            | Self::SettingsSet { .. }
            | Self::Restart
            // A prime writes nothing here, so it waits on no purge.
            | Self::FarsightPrime { .. }
            // Its handler waits on a purge of the viewer itself.
            | Self::FarsightForget { .. } => None,
        }
    }
}

/// The most changes one `settings set` carries: a whole group's.
pub const MAX_CHANGES: usize = 200;

/// `settings set`'s changes: each a key of the registry, once, with a
/// value or `reset`, and the version it was made against.
fn parse_changes(command: &str, args: &Value) -> Result<Vec<Change>, InvalidCommand> {
    let bad = |why: &str| InvalidCommand(format!("{command}: {why}"));
    let list = args.get("changes").and_then(Value::as_array).ok_or_else(|| bad("missing changes"))?;
    if list.is_empty() || list.len() > MAX_CHANGES {
        return Err(bad("expected 1 to 200 changes"));
    }
    let mut seen = std::collections::HashSet::new();
    let mut out = Vec::with_capacity(list.len());
    for c in list {
        let key = c.get("key").and_then(Value::as_str).ok_or_else(|| bad("a change has no key"))?;
        if !crate::admin::settings::registry::known(key) {
            return Err(bad(&format!("{key:?} is not a setting that can be changed")));
        }
        if !seen.insert(key) {
            return Err(bad(&format!("{key:?} is changed twice")));
        }
        let base_version = c.get("base_version").and_then(Value::as_i64).filter(|v| *v >= 0).ok_or_else(|| bad("a change has no base_version"))?;
        let value = match (c.get("reset").and_then(Value::as_bool), c.get("value")) {
            (Some(true), None) => None,
            (None | Some(false), Some(v)) => Some(v.clone()),
            _ => return Err(bad(&format!("{key:?} needs a value or reset, not both"))),
        };
        out.push(Change { key: key.to_string(), value, base_version });
    }
    Ok(out)
}

/// An operator change's result: done, a plain-words refusal (the command
/// fails), or a database error.
fn settled<T: Into<Value>>(result: Result<T, Result<String, sqlx::Error>>) -> Result<Outcome, String> {
    match result {
        Ok(v) => Ok(Outcome::Done(v.into())),
        Err(Ok(refusal)) => Ok(Outcome::Failed(refusal)),
        Err(Err(e)) => Err(e.to_string()),
    }
}

/// What executing a command produced.
#[derive(Debug, Clone, PartialEq)]
pub enum Outcome {
    Done(Value),
    /// Leave the command `queued` (a purge of its DID is running).
    Defer,
    Failed(String),
}

/// Everything the handlers need from `serve`.
#[derive(Clone)]
pub struct Handlers {
    pub pool: PgPool,
    pub scope: ScopeState,
    pub private: Arc<dyn PrivateTier>,
    pub writer: Writer,
    pub purger: Purger,
    pub rebuild: Rebuild,
    pub redis: ConnectionManager,
    pub source: Arc<dyn BackfillSource>,
    pub dead_letter_stream: String,
    pub deterministic_attempts: u32,
    /// The block-list index, when `farsight.url` is set.
    pub farsight: Option<Farsight>,
    /// `lists.sync_min_interval_secs`.
    pub sync_min_interval_secs: u32,
    /// A member's budget of list references (design-v3 §4.3).
    pub list_budget: Budget,
    /// The public-indexing switch, which also spawns the task a phase
    /// change needs (public-indexing design §3.4).
    pub public: PublicIndexing,
    /// What the operator commands need (admin-ui design §5.7).
    pub operators: OperatorDeps,
    /// What `settings set` and `restart` need (admin-ui design §8.3, §8.5).
    pub settings: SettingsDeps,
}

/// What `settings set` and `restart` need: `app.toml`'s path, read fresh
/// at each change, the running environment to validate with, the live
/// values to swap, and the restart. Without a path and an environment
/// (tests that do not need them), `settings set` fails.
#[derive(Clone, Default)]
pub struct SettingsDeps {
    pub config_path: Option<PathBuf>,
    pub env: Option<EnvConfig>,
    pub live: Option<LiveSettings>,
    pub restart: RestartHandle,
}

/// What the operator commands need: fresh resolution, and the setup code's
/// file, deleted when the first operator is added.
#[derive(Clone, Default)]
pub struct OperatorDeps {
    pub identity: Option<Arc<dyn OperatorIdentity>>,
    pub setup_code_file: Option<PathBuf>,
}

impl Handlers {
    /// Whether the DID is in `purging_dids`.
    ///
    /// # Errors
    ///
    /// Database errors.
    pub async fn purging(&self, did: &str) -> Result<bool, sqlx::Error> {
        sqlx::query_scalar("SELECT EXISTS (SELECT 1 FROM purging_dids WHERE did = $1)")
            .bind(did)
            .fetch_one(&self.pool)
            .await
    }

    /// Run one command for `actor`, who is recorded as its writer
    /// (admin-ui design §5.8).
    pub async fn execute(&self, cmd: &AdminCommand, actor: &Actor) -> Outcome {
        self.execute_row(None, cmd, actor).await
    }

    /// [`Handlers::execute`] for the queue's row `id`, which a settings
    /// change records in its history.
    pub async fn execute_row(&self, id: Option<i64>, cmd: &AdminCommand, actor: &Actor) -> Outcome {
        let result = match cmd {
            AdminCommand::SettingsSet { changes } => self.settings_set(changes, actor, id).await,
            AdminCommand::Restart => Ok(self.restart(actor)),
            AdminCommand::MemberAdd { did } => self.member_add(did).await,
            AdminCommand::MemberRemove { did } => self.member_remove(did).await,
            AdminCommand::MemberDeny { did, reason } => self.member_deny(did, reason.as_deref(), actor).await,
            AdminCommand::MemberAllow { did } => self.member_allow(did).await,
            AdminCommand::LabelAdd { subject, val } => self.label_add(subject, val).await,
            AdminCommand::LabelRemove { subject, val } => self.label_remove(subject, val).await,
            AdminCommand::ScopeRebuild { dry_run } => self.scope_rebuild(*dry_run).await,
            AdminCommand::Backfill { did, depth } => self.backfill(did, *depth).await,
            AdminCommand::Resync => self.resync().await,
            AdminCommand::DeadletterReplay { id } => self.deadletter_replay(id).await,
            AdminCommand::DeadletterDrop { id } => self.deadletter_drop(id).await,
            AdminCommand::DeadletterSweep { dry_run } => self.deadletter_sweep(*dry_run).await,
            AdminCommand::FarsightStatus => self.farsight_status().await,
            AdminCommand::FarsightPrime { did, force } => self.farsight_prime(did, *force).await,
            AdminCommand::FarsightForget { did } => self.farsight_forget(did).await,
            AdminCommand::ListsSync { uri } => self.lists_sync(uri).await,
            AdminCommand::ListsPurge { did } => self.lists_purge(did).await,
            AdminCommand::IndexingPublicScope { value } => self.indexing_public_scope(*value, actor).await,
            AdminCommand::IndexingStatus => self.public.status().await.map(Outcome::Done).map_err(|e| e.to_string()),
            AdminCommand::OperatorAdd { input, note, add_host } => self.operator_add(input, note.as_deref(), *add_host, actor).await,
            AdminCommand::OperatorRemove { did } => settled(operators::remove(&self.pool, did).await),
            AdminCommand::OperatorHostAdd { pattern } => {
                operators::host_add(&self.pool, pattern, actor).await.map(Outcome::Done).map_err(|e| e.to_string())
            }
            AdminCommand::OperatorHostRemove { pattern } => settled(operators::host_remove(&self.pool, pattern).await),
        };
        result.unwrap_or_else(Outcome::Failed)
    }

    /// `settings set` (admin-ui design §8.3): the transaction of
    /// [`store::set`], then the live values swapped when a live key
    /// changed.
    async fn settings_set(&self, changes: &[Change], actor: &Actor, id: Option<i64>) -> Result<Outcome, String> {
        let (Some(path), Some(env)) = (&self.settings.config_path, &self.settings.env) else {
            return Ok(Outcome::Failed("settings cannot be changed by this process".to_string()));
        };
        match store::set(&self.pool, path, env, changes, actor.label(), id).await.map_err(|e| e.to_string())? {
            Ok(saved) => {
                if changes.iter().any(|c| is_live(&c.key))
                    && let Some(live) = &self.settings.live
                {
                    live.store(LiveValues::from_file(&saved.candidate));
                }
                Ok(Outcome::Done(saved.result))
            }
            Err(refusal) => Ok(Outcome::Failed(refusal)),
        }
    }

    /// `restart` (admin-ui design §8.5): refused unless a supervisor starts
    /// `serve` again. The executor triggers the shutdown once the row is
    /// settled.
    fn restart(&self, actor: &Actor) -> Outcome {
        if !self.settings.env.as_ref().is_some_and(|e| e.supervised) {
            return Outcome::Failed("Prism cannot restart itself on this server; ask whoever runs it to restart Prism".to_string());
        }
        tracing::warn!(event = "admin_restart_requested", by = actor.label(), "a restart was asked for; serve shuts down and its supervisor starts it again");
        Outcome::Done(json!({ "restarting": true }))
    }

    /// `operator add` (admin-ui design §5.7): the account is resolved
    /// afresh, then one transaction adds it (and, with no operator before
    /// it, closes setup); the setup code's file goes after the commit.
    async fn operator_add(&self, input: &str, note: Option<&str>, add_host: bool, actor: &Actor) -> Result<Outcome, String> {
        let identity = self.operators.identity.as_ref().ok_or_else(|| "identity resolution is not available".to_string())?;
        let did = if is_did(input) {
            input.to_string()
        } else {
            match identity.resolve_handle(input).await {
                Some(did) => did,
                None => return Ok(Outcome::Failed(format!("{input} could not be resolved to an account"))),
            }
        };
        let info = match identity.resolve_fresh(&did).await {
            Ok(info) => info,
            Err(e) => return Ok(Outcome::Failed(format!("{did} could not be resolved: {e}"))),
        };
        let Some(endpoint) = info.pds_endpoint.as_deref().and_then(operators::normalize_endpoint) else {
            return Ok(Outcome::Failed(format!("{did} names no server")));
        };
        let Some(host) = operators::endpoint_host(&endpoint) else {
            return Ok(Outcome::Failed(format!("{did}'s server address could not be read")));
        };
        let who = operators::Resolved { did, endpoint, host };
        match operators::add(&self.pool, &who, note, add_host, actor).await {
            Ok((result, closed)) => {
                if closed && let Some(path) = &self.operators.setup_code_file {
                    crate::admin::setup_code::delete_file(path);
                }
                tracing::info!(event = "operator_added", did = %who.did, host = %who.host, by = actor.label(), "admin operator added");
                Ok(Outcome::Done(result))
            }
            Err(failure) => settled::<Value>(Err(failure)),
        }
    }

    /// `indexing public-scope` (public-indexing design §3.4): the command's
    /// transaction, then the task its phase change needs.
    async fn indexing_public_scope(&self, value: ScopeValue, actor: &Actor) -> Result<Outcome, String> {
        let applied = self.public.apply_scope(value, actor.label()).await.map_err(|e| e.to_string())?;
        let task = applied.spawn.map(|task| {
            let kind = task.kind();
            drop(self.public.spawn(task));
            kind
        });
        Ok(Outcome::Done(json!({
            "value": value.as_str(),
            "action": applied.action,
            "mode": applied.mode.as_str(),
            "generation": applied.generation,
            "task": task,
        })))
    }

    /// `lists purge` (design-v3 §14.3): deletes the DID's list mutes and
    /// the list records it authored (lists, list items, list blocks,
    /// opt-outs), and with them its references; the lists it referenced
    /// are marked, and the list phase releases those nobody else
    /// references. A local cleanup, not a takedown: records still in the
    /// DID's repository come back at its next re-sync.
    async fn lists_purge(&self, did: &str) -> Result<Outcome, String> {
        if self.purging(did).await.map_err(|e| e.to_string())? {
            return Ok(Outcome::Defer);
        }
        let db = |e: sqlx::Error| e.to_string();
        // §II-6.11.7: the read guard, then the DID's lock, before any row.
        let guard = self.scope.read_guard().await;
        let mut tx = self.pool.begin().await.map_err(db)?;
        lock_did_exclusive(&mut tx, did).await.map_err(db)?;
        let mut deleted = serde_json::Map::new();
        for (table, column) in [
            ("list_mutes", "viewer_did"),
            ("list_blocks", "author_did"),
            ("list_optouts", "author_did"),
            ("list_items", "author_did"),
            ("lists", "creator_did"),
        ] {
            let n = sqlx::query(&format!("DELETE FROM {table} WHERE {column} = $1"))
                .bind(did)
                .execute(&mut *tx)
                .await
                .map_err(db)?
                .rows_affected();
            deleted.insert(table.to_string(), json!(n));
        }
        // Both kinds of reference lost their source rows.
        drop_member(&mut tx, did).await.map_err(db)?;
        tx.commit().await.map_err(db)?;
        drop(guard);
        Ok(Outcome::Done(json!({ "did": did, "deleted": deleted })))
    }

    /// `lists sync` (design-v3 §4.3, §14.3): a high-priority list sync for
    /// the owner of a subscribed list, recorded on the owner's row so it
    /// runs even when nothing else shows a run is owed. It is still paced
    /// like any other run of that owner.
    async fn lists_sync(&self, uri: &str) -> Result<Outcome, String> {
        let owner = uri_author(uri).ok_or_else(|| "not an at-uri".to_string())?;
        if self.purging(owner).await.map_err(|e| e.to_string())? {
            return Ok(Outcome::Defer);
        }
        let mut tx = self.pool.begin().await.map_err(|e| e.to_string())?;
        let subscribed: bool = sqlx::query_scalar("SELECT EXISTS (SELECT 1 FROM subscribed_lists WHERE list_uri = $1 AND state = 'active')")
            .bind(uri)
            .fetch_one(&mut *tx)
            .await
            .map_err(|e| e.to_string())?;
        if !subscribed {
            return Err("no member references this list".to_string());
        }
        triggers::request_if_active(&mut tx, owner, list_sync::HIGH, self.sync_min_interval_secs)
            .await
            .map_err(|e| e.to_string())?;
        tx.commit().await.map_err(|e| e.to_string())?;
        Ok(Outcome::Done(json!({ "uri": uri, "owner": owner, "requested": true })))
    }

    fn farsight(&self) -> Result<&Farsight, String> {
        self.farsight.as_ref().ok_or_else(|| "farsight.url is not set".to_string())
    }

    /// `farsight status` (design-v3 §5.14.11, §14.3). What this AppView
    /// holds itself is printed whether or not Farsight answers.
    async fn farsight_status(&self) -> Result<Outcome, String> {
        let farsight = self.farsight()?;
        let mut status = match farsight.status().await {
            Ok(Value::Object(brief)) => brief,
            Ok(_) => serde_json::Map::new(),
            Err(reason) => serde_json::Map::from_iter([("farsight_did_not_answer".to_string(), json!(reason))]),
        };
        status.extend(farsight.local_status());
        Ok(Outcome::Done(Value::Object(status)))
    }

    /// `farsight forget` (design-v3 §14.3): removes every remembered pair
    /// of one viewer, as a purge does for a removed member. It waits on a
    /// running purge of the viewer, and a failed delete fails the command:
    /// the pairs are pending by then, so the writer goes on deleting them.
    async fn farsight_forget(&self, did: &str) -> Result<Outcome, String> {
        let farsight = self.farsight()?;
        if self.purging(did).await.map_err(|e| e.to_string())? {
            return Ok(Outcome::Defer);
        }
        let forgotten = farsight.forget_memory(did).await?;
        Ok(Outcome::Done(json!({ "did": did, "forgotten": forgotten })))
    }

    /// `farsight prime`: for a member whose block state on Farsight is
    /// suspected stale.
    async fn farsight_prime(&self, did: &str, force: bool) -> Result<Outcome, String> {
        self.farsight()?
            .prime(did, force)
            .await
            .map_err(|reason| format!("Farsight was not primed: {reason}"))?;
        Ok(Outcome::Done(json!({ "did": did, "requested": true, "force": force })))
    }

    /// `member add` (§9.2, §6.11.1): under the write guard and the DID's
    /// shared lock, insert the member, put it in S (reason bit 1), enqueue a
    /// `member` backfill, and re-evaluate owner status; the in-memory sets
    /// follow after commit. A denied DID fails (membership design §5.4).
    async fn member_add(&self, did: &str) -> Result<Outcome, String> {
        match add_member(&self.pool, &self.scope, &self.private, did, "admin", self.list_budget)
            .await
            .map_err(|e| e.to_string())?
        {
            AddOutcome::Purging => Ok(Outcome::Defer),
            AddOutcome::Denied => Ok(Outcome::Failed("denied; `admin member allow` first".to_string())),
            AddOutcome::AlreadyMember => Ok(Outcome::Done(json!({ "did": did, "status": "already_member" }))),
            AddOutcome::Added { became_owner } => {
                let mut jobs = vec![json!({ "did": did, "depth": "member" })];
                if became_owner {
                    jobs.push(json!({ "did": did, "depth": "private" }));
                }
                Ok(Outcome::Done(json!({ "did": did, "status": "added", "jobs": jobs })))
            }
        }
    }

    /// `member remove`: the `member_removed` marker step runs here (write
    /// guard); the batched deletion and purge end run in the background.
    /// Membership is the only private-owner eligibility (membership design
    /// §2.3), so the DID stops being an owner at the purge end.
    async fn member_remove(&self, did: &str) -> Result<Outcome, String> {
        let is_member: bool = sqlx::query_scalar("SELECT EXISTS (SELECT 1 FROM members WHERE did = $1)")
            .bind(did)
            .fetch_one(&self.pool)
            .await
            .map_err(|e| e.to_string())?;
        if !is_member {
            return Ok(Outcome::Failed(format!("{did} is not a member")));
        }
        match self.purger.marker(did, PurgeKind::MemberRemoved).await {
            Ok(()) => {}
            Err(PurgeError::Busy(_)) => return Ok(Outcome::Defer),
            Err(e) => return Err(e.to_string()),
        }
        self.finish_member_removed(did);
        Ok(Outcome::Done(json!({ "did": did, "status": "removed", "purge": "member_removed" })))
    }

    /// `member deny` (membership design §5.2): the denial and, for a member,
    /// the `member_removed` marker in one transaction (`MarkerGuard::Deny`),
    /// which decides whether the DID is a member. `Marked`: the batches and
    /// the end run in the background, as for `member remove`. `Skipped`:
    /// the denial alone was committed.
    async fn member_deny(&self, did: &str, reason: Option<&str>, actor: &Actor) -> Result<Outcome, String> {
        let db = |e: sqlx::Error| e.to_string();
        // A shortcut: the marker's own `Busy` check is the authority.
        if self.purging(did).await.map_err(db)? {
            return Ok(Outcome::Defer);
        }
        // Only the operator's commands write denials, so this read tells a
        // new denial from an existing one.
        let already = is_denied(&mut *self.pool.acquire().await.map_err(db)?, did).await.map_err(db)?;
        let denial = Denial {
            did: did.to_string(),
            reason: reason.map(str::to_string),
            denied_by: actor.label().to_string(),
        };
        let removed = match self.purger.marker_guarded(did, PurgeKind::MemberRemoved, &MarkerGuard::Deny(denial)).await {
            Ok(MarkerOutcome::Marked) => {
                self.finish_member_removed(did);
                true
            }
            Ok(MarkerOutcome::Skipped | MarkerOutcome::Held(_)) => false,
            Err(PurgeError::Busy(_)) => return Ok(Outcome::Defer),
            Err(e) => return Err(e.to_string()),
        };
        let status = if already { "already_denied" } else { "denied" };
        tracing::info!(event = "member_denied", did, status, removed, "member denied");
        Ok(Outcome::Done(json!({ "did": did, "status": status, "removed": removed })))
    }

    /// `member allow` (membership design §5.3): deletes the denial and adds
    /// nothing. Deferred while the DID is purged, as `member add` is, so
    /// `deny`, `allow` and `add` keep their order.
    async fn member_allow(&self, did: &str) -> Result<Outcome, String> {
        if self.purging(did).await.map_err(|e| e.to_string())? {
            return Ok(Outcome::Defer);
        }
        let allowed = membership::allow(&self.pool, did).await.map_err(|e| e.to_string())?;
        let status = if allowed { "allowed" } else { "not_denied" };
        tracing::info!(event = "member_allowed", did, status, "member denial lifted");
        Ok(Outcome::Done(json!({ "did": did, "status": status })))
    }

    /// The batched deletion and purge end of a `member_removed` purge whose
    /// marker committed, in the background.
    fn finish_member_removed(&self, did: &str) {
        let purger = self.purger.clone();
        let did_owned = did.to_string();
        tokio::spawn(async move {
            let result = async {
                purger.run_batches(&did_owned, PurgeKind::MemberRemoved).await?;
                purger.end(&did_owned).await
            }
            .await;
            if let Err(error) = result {
                tracing::error!(event = "member_remove_purge_failed", did = %did_owned, %error, "member_removed purge failed; it resumes at the next startup");
            }
        });
    }

    async fn label_add(&self, subject: &str, val: &str) -> Result<Outcome, String> {
        let n = sqlx::query("INSERT INTO labels (subject, val) VALUES ($1, $2) ON CONFLICT (subject, val) DO NOTHING")
            .bind(subject)
            .bind(val)
            .execute(&self.pool)
            .await
            .map_err(|e| e.to_string())?
            .rows_affected();
        Ok(Outcome::Done(json!({ "subject": subject, "val": val, "added": n > 0 })))
    }

    async fn label_remove(&self, subject: &str, val: &str) -> Result<Outcome, String> {
        let n = sqlx::query("DELETE FROM labels WHERE subject = $1 AND val = $2")
            .bind(subject)
            .bind(val)
            .execute(&self.pool)
            .await
            .map_err(|e| e.to_string())?
            .rows_affected();
        Ok(Outcome::Done(json!({ "subject": subject, "val": val, "removed": n > 0 })))
    }

    /// `scope rebuild` (scope-recency design §3.3): a dry run computes the
    /// next sweep's counts inline (read-only); a real one runs one sweep,
    /// after any sweep already running and with the same cap and pacing,
    /// then the counts repair, as its own task, and the command is done at
    /// once.
    async fn scope_rebuild(&self, dry_run: bool) -> Result<Outcome, String> {
        if dry_run {
            let r = self.rebuild.sweep(true).await.map_err(|e| e.to_string())?;
            let by_reasons: serde_json::Map<String, serde_json::Value> =
                r.would_purge_by_reasons.iter().map(|(bits, n)| (bits.to_string(), json!(n))).collect();
            return Ok(Outcome::Done(json!({
                "dry_run": true, "justified": r.justified, "would_add": r.added, "would_purge": r.candidates,
                "would_purge_by_reasons": by_reasons, "sweeps": r.sweeps_needed(self.rebuild.max_purges()),
            })));
        }
        let rebuild = self.rebuild.clone();
        tokio::spawn(async move {
            if let Err(error) = rebuild.sweep(false).await {
                tracing::error!(event = "scope_sweep_failed", %error, "admin scope rebuild failed");
                return;
            }
            if let Err(error) = rebuild.repair_counts().await {
                tracing::error!(event = "scope_rebuild_failed", %error, "admin scope rebuild: counts repair failed");
            }
        });
        Ok(Outcome::Done(json!({ "dry_run": false, "scheduled": "scope_sweep" })))
    }

    /// `backfill <did> [--depth]`: enqueue and return the job key. The
    /// default depth is `member` for a member, `scope` otherwise.
    async fn backfill(&self, did: &str, depth: Option<Depth>) -> Result<Outcome, String> {
        let depth = match depth {
            Some(d) => d,
            None if self.scope.read_guard().await.is_member(did) => Depth::Member,
            None => Depth::Scope,
        };
        let mut conn = self.pool.acquire().await.map_err(|e| e.to_string())?;
        jobs::enqueue(&mut conn, did, depth).await.map_err(|e| e.to_string())?;
        Ok(Outcome::Done(json!({ "jobs": [{ "did": did, "depth": depth.as_str() }] })))
    }

    /// `resync --private-and-members` records a gap and runs the §6.11.4 gap
    /// transaction: readiness cleared, `authz` for every tracked DID and
    /// `resync` for every member.
    async fn resync(&self) -> Result<Outcome, String> {
        let snapshot = self.scope.snapshot().await;
        let epoch = gap::run_gap_transaction(&self.pool, &self.scope, self.private.as_ref(), GapCause::Operator, false)
            .await
            .map_err(|e| e.to_string())?;
        Ok(Outcome::Done(json!({
            "gap_epoch": epoch,
            "authz_jobs": snapshot.tracked().len(),
            "resync_jobs": snapshot.members.len()
        })))
    }

    /// `deadletter replay <id>`: re-fetch exactly that `repo`/`path` from the
    /// PDS (§5.9: `getLatestCommit` first, then `getRecord`) and run it through
    /// the normal write path. The dead letter stays listed; `drop` removes it.
    async fn deadletter_replay(&self, id: &str) -> Result<Outcome, String> {
        let mut redis = self.redis.clone();
        let entries: Vec<(String, Vec<(String, String)>)> = redis::cmd("XRANGE")
            .arg(&self.dead_letter_stream)
            .arg(id)
            .arg(id)
            .query_async(&mut redis)
            .await
            .map_err(|e| e.to_string())?;
        let Some((_, fields)) = entries.into_iter().next() else {
            return Ok(Outcome::Failed(format!("no dead letter {id}")));
        };
        let get = |k: &str| fields.iter().find(|(f, _)| f == k).map(|(_, v)| v.clone()).unwrap_or_default();
        let (repo, path) = (get("repo"), get("path"));
        let Some((collection, rkey)) = path.split_once('/').filter(|_| is_did(&repo)) else {
            return Ok(Outcome::Failed("unparseable entry: no repo/path to replay".to_string()));
        };
        let indexed = match index_record(self.source.as_ref(), &self.writer, &repo, collection, rkey, self.deterministic_attempts, true).await {
            Ok(i) => i,
            Err(e @ IndexError::Write(_)) => return Ok(Outcome::Failed(format!("replay failed: {e}"))),
            Err(e) => return Err(e.to_string()),
        };
        let (rev, status) = (indexed.rev, indexed.status.as_str());
        Ok(Outcome::Done(json!({ "id": id, "repo": repo, "path": path, "rev": rev, "status": status })))
    }

    async fn deadletter_drop(&self, id: &str) -> Result<Outcome, String> {
        let mut redis = self.redis.clone();
        let n: i64 = redis::cmd("XDEL")
            .arg(&self.dead_letter_stream)
            .arg(id)
            .query_async(&mut redis)
            .await
            .map_err(|e| e.to_string())?;
        if n == 0 {
            return Ok(Outcome::Failed(format!("no dead letter {id}")));
        }
        Ok(Outcome::Done(json!({ "id": id, "dropped": true })))
    }

    /// `deadletter sweep [--dry-run]` (actionable-warnings design §6.6): read
    /// the dead-letter stream a page at a time and remove every whole-entry
    /// letter (no path) whose payload was kept whole and that the firehose
    /// would now drop, deciding as it does with the scope sets as they are
    /// now. Op letters, truncated and unreadable ones are kept.
    async fn deadletter_sweep(&self, dry_run: bool) -> Result<Outcome, String> {
        let mut redis = self.redis.clone();
        let (mut examined, mut removed, mut kept) = (0u64, 0u64, 0u64);
        let mut start = "-".to_string();
        loop {
            let reply: redis::Value = redis::cmd("XRANGE")
                .arg(&self.dead_letter_stream)
                .arg(&start)
                .arg("+")
                .arg("COUNT")
                .arg(SWEEP_PAGE)
                .query_async(&mut redis)
                .await
                .map_err(|e| e.to_string())?;
            let redis::Value::Array(items) = reply else {
                return Err("unexpected XRANGE reply".to_string());
            };
            let entries = items.into_iter().map(crate::firehose::stream::parse_entry).collect::<Result<Vec<_>, _>>()?;
            let Some(last) = entries.last().map(|e| e.id.clone()) else {
                break;
            };
            let mut gone = Vec::new();
            for e in &entries {
                examined += 1;
                if self.would_drop(e).await {
                    gone.push(e.id.clone());
                } else {
                    kept += 1;
                }
            }
            removed += gone.len() as u64;
            if !dry_run && !gone.is_empty() {
                let _: i64 = redis::cmd("XDEL").arg(&self.dead_letter_stream).arg(&gone).query_async(&mut redis).await.map_err(|e| e.to_string())?;
            }
            if entries.len() < SWEEP_PAGE {
                break;
            }
            start = format!("({last}");
        }
        Ok(Outcome::Done(json!({ "examined": examined, "removed": removed, "kept": kept, "dry_run": dry_run })))
    }

    /// Whether the firehose would now drop this dead letter's entry.
    async fn would_drop(&self, letter: &crate::firehose::stream::StreamEntry) -> bool {
        use crate::firehose::stream::{Readable, StreamEntry, decode, readable_despite};
        if !letter.field("path").unwrap_or_default().is_empty() {
            return false;
        }
        let Some(data) = letter.raw("data_prefix") else {
            return false;
        };
        if letter.field("data_len").and_then(|n| n.parse::<usize>().ok()) != Some(data.len()) {
            return false;
        }
        let entry = StreamEntry {
            id: letter.id.clone(),
            fields: Some(vec![
                ("type".to_string(), letter.raw("type").unwrap_or_default().to_vec()),
                ("data".to_string(), data.to_vec()),
            ]),
        };
        if decode(&entry).is_ok() {
            return false;
        }
        match readable_despite(&entry) {
            Some(Readable::NotADid) => true,
            Some(Readable::Commit { repo, ops }) => !self.writer.would_write(&repo, &ops).await,
            None => false,
        }
    }
}

/// Dead letters read per `XRANGE` by `deadletter sweep`.
pub const SWEEP_PAGE: usize = 500;

#[cfg(test)]
mod tests {
    use super::*;

    const DID: &str = "did:plc:aaaaaaaaaaaaaaaaaaaaaaaa";

    #[test]
    fn rows_round_trip() {
        for cmd in [
            AdminCommand::DeadletterSweep { dry_run: true },
            AdminCommand::DeadletterSweep { dry_run: false },
            AdminCommand::MemberAdd { did: DID.into() },
            AdminCommand::MemberRemove { did: DID.into() },
            AdminCommand::MemberDeny { did: DID.into(), reason: Some("spam".into()) },
            AdminCommand::MemberDeny { did: DID.into(), reason: None },
            AdminCommand::MemberAllow { did: DID.into() },
            AdminCommand::LabelAdd {
                subject: format!("at://{DID}/app.bsky.feed.post/3k"),
                val: "!hide".into(),
            },
            AdminCommand::LabelRemove {
                subject: DID.into(),
                val: "porn".into(),
            },
            AdminCommand::ScopeRebuild { dry_run: true },
            AdminCommand::Backfill {
                did: DID.into(),
                depth: Some(Depth::Authz),
            },
            AdminCommand::Backfill { did: DID.into(), depth: None },
            AdminCommand::Resync,
            AdminCommand::DeadletterReplay { id: "1-0".into() },
            AdminCommand::DeadletterDrop { id: "17-3".into() },
        ] {
            let (command, args) = cmd.to_row();
            assert_eq!(AdminCommand::parse(command, &args), Ok(cmd));
        }
    }

    #[test]
    fn invalid_commands_are_rejected() {
        assert!(AdminCommand::parse("member add", &json!({"did": "bob"})).is_err());
        // The Farsight commands round-trip, and neither waits on a purge.
        for cmd in [
            AdminCommand::FarsightStatus,
            AdminCommand::FarsightPrime { did: DID.to_string(), force: true },
            AdminCommand::FarsightForget { did: DID.to_string() },
        ] {
            let (name, args) = cmd.to_row();
            assert_eq!(AdminCommand::parse(name, &args), Ok(cmd.clone()));
            assert_eq!(cmd.did(), None);
        }
        assert!(AdminCommand::parse("farsight prime", &json!({"did": "bob"})).is_err());
        // `lists sync` takes a list's at-uri and waits on its owner's purge.
        let sync = AdminCommand::ListsSync { uri: format!("at://{DID}/app.bsky.graph.list/3k") };
        let (name, args) = sync.to_row();
        assert_eq!(AdminCommand::parse(name, &args), Ok(sync.clone()));
        assert_eq!(sync.did(), Some(DID));
        assert!(AdminCommand::parse("lists sync", &json!({"uri": format!("at://{DID}/app.bsky.feed.post/3k")})).is_err());
        assert!(AdminCommand::parse("lists sync", &json!({"uri": DID})).is_err());
        assert!(AdminCommand::parse("label add", &json!({"subject": DID, "val": "spam"})).is_err());
        assert!(AdminCommand::parse("label add", &json!({"subject": "https://x", "val": "porn"})).is_err());
        assert!(AdminCommand::parse("backfill", &json!({"did": DID, "depth": "deep"})).is_err());
        assert!(AdminCommand::parse("deadletter drop", &json!({"id": "x"})).is_err());
        assert!(AdminCommand::parse("purge everything", &json!({})).is_err());
        // The public-indexing commands round-trip, wait on no purge, and
        // refuse `full`.
        for cmd in [
            AdminCommand::IndexingPublicScope { value: ScopeValue::Off },
            AdminCommand::IndexingPublicScope { value: ScopeValue::CommunityScoped },
            AdminCommand::IndexingStatus,
        ] {
            let (name, args) = cmd.to_row();
            assert_eq!(AdminCommand::parse(name, &args), Ok(cmd.clone()));
            assert_eq!(cmd.did(), None);
        }
        assert!(AdminCommand::parse("indexing public-scope", &json!({"value": "full"})).is_err());
        // The operator commands (admin-ui design §11) round-trip and wait
        // on no purge; arguments are checked as the design says.
        for cmd in [
            AdminCommand::OperatorAdd { input: DID.into(), note: Some("me".into()), add_host: true },
            AdminCommand::OperatorAdd { input: "alice.example.com".into(), note: None, add_host: false },
            AdminCommand::OperatorRemove { did: DID.into() },
            AdminCommand::OperatorHostAdd { pattern: "*.host.bsky.network".into() },
            AdminCommand::OperatorHostRemove { pattern: "pds.example.com".into() },
        ] {
            let (name, args) = cmd.to_row();
            assert_eq!(AdminCommand::parse(name, &args), Ok(cmd.clone()));
            assert_eq!(cmd.did(), None);
        }
        for (name, args) in [
            ("operator add", json!({"input": "https://pds.example.com"})),
            ("operator add", json!({"input": "Alice.Example.com"})),
            ("operator add", json!({"input": DID, "note": "x".repeat(MAX_NOTE + 1)})),
            ("operator remove", json!({"did": "alice"})),
            ("operator host add", json!({"pattern": "PDS.example.com"})),
            ("operator host add", json!({"pattern": "https://pds.example.com"})),
            ("operator host remove", json!({"pattern": "*.*.example"})),
        ] {
            assert!(AdminCommand::parse(name, &args).is_err(), "{name} {args}");
        }
        // `settings set` and `restart` (admin-ui design §11) round-trip and
        // wait on no purge; only registry keys, each once, with a version.
        for cmd in [
            AdminCommand::SettingsSet {
                changes: vec![
                    Change { key: "threadlens.access".into(), value: Some(json!("members")), base_version: 3 },
                    Change { key: "constellation.networks[bluesky].background_burst".into(), value: None, base_version: 1 },
                    Change { key: "honest_501.public_off_message".into(), value: Some(Value::Null), base_version: 0 },
                ],
            },
            AdminCommand::Restart,
        ] {
            let (name, args) = cmd.to_row();
            assert_eq!(AdminCommand::parse(name, &args), Ok(cmd.clone()));
            assert_eq!(cmd.did(), None);
        }
        for args in [
            json!({"changes": []}),
            json!({"changes": [{"key": "http.bind", "value": "0.0.0.0:1", "base_version": 0}]}),
            json!({"changes": [{"key": "indexing.public_scope", "value": "off", "base_version": 0}]}),
            json!({"changes": [{"key": "threadlens.access", "value": "members"}]}),
            json!({"changes": [{"key": "threadlens.access", "value": "members", "reset": true, "base_version": 0}]}),
            json!({"changes": [{"key": "threadlens.access", "value": "a", "base_version": 0}, {"key": "threadlens.access", "value": "b", "base_version": 0}]}),
        ] {
            assert!(AdminCommand::parse("settings set", &args).is_err(), "{args}");
        }
    }

    #[test]
    fn per_did_keys() {
        let label = AdminCommand::LabelAdd {
            subject: format!("at://{DID}/app.bsky.feed.post/3k"),
            val: "porn".into(),
        };
        assert_eq!(label.did(), Some(DID));
        assert_eq!(AdminCommand::Resync.did(), None);
        // `deny` and `allow` keep their order with `add` (membership
        // design §5.3).
        assert_eq!(AdminCommand::MemberDeny { did: DID.into(), reason: None }.did(), Some(DID));
        assert_eq!(AdminCommand::MemberAllow { did: DID.into() }.did(), Some(DID));
        assert!(AdminCommand::parse("member deny", &json!({"did": DID, "reason": 7})).is_err());
        assert!(AdminCommand::parse("member allow", &json!({"did": "bob"})).is_err());
    }
}
