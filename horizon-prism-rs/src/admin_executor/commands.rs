//! The mutating admin commands (design-v2 §3.3) and their handlers.
//!
//! A command is stored in `admin_commands` as `(command, args jsonb)`; the
//! CLI writes it ([`AdminCommand::to_row`]) and `serve`'s executor parses and
//! runs it ([`Handlers::execute`]) through the same code paths and locks as
//! the rest of `serve`.

use std::sync::Arc;

use chrono::Utc;
use redis::aio::ConnectionManager;
use serde_json::{Value, json};
use sqlx::PgPool;

use crate::aggregations::{PurgeKind, Purger, Rebuild, ScopeState};
use crate::aggregations::membership::{AddOutcome, add_member};
use crate::db::locks::{lock_did_exclusive, uri_author};
use crate::farsight::Farsight;
use crate::firehose::backfill::BackfillSource;
use crate::firehose::gap::{self, GapCause};
use crate::firehose::index_request::{IndexError, index_record};
use crate::firehose::jobs::{self, Depth};
use crate::firehose::writer::Writer;
use crate::identity::eligibility::{self, Confirmation};
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
    LabelAdd { subject: String, val: String },
    LabelRemove { subject: String, val: String },
    ScopeRebuild { dry_run: bool },
    Backfill { did: String, depth: Option<Depth> },
    /// `resync --private-and-members`.
    Resync,
    DeadletterReplay { id: String },
    DeadletterDrop { id: String },
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
}

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
            Self::LabelAdd { subject, val } => ("label add", json!({ "subject": subject, "val": val })),
            Self::LabelRemove { subject, val } => ("label remove", json!({ "subject": subject, "val": val })),
            Self::ScopeRebuild { dry_run } => ("scope rebuild", json!({ "dry_run": dry_run })),
            Self::Backfill { did, depth } => ("backfill", json!({ "did": did, "depth": depth.map(Depth::as_str) })),
            Self::Resync => ("resync", json!({ "private_and_members": true })),
            Self::DeadletterReplay { id } => ("deadletter replay", json!({ "id": id })),
            Self::DeadletterDrop { id } => ("deadletter drop", json!({ "id": id })),
            Self::FarsightStatus => ("farsight status", json!({})),
            Self::FarsightPrime { did, force } => ("farsight prime", json!({ "did": did, "force": force })),
            Self::FarsightForget { did } => ("farsight forget", json!({ "did": did })),
            Self::ListsSync { uri } => ("lists sync", json!({ "uri": uri })),
            Self::ListsPurge { did } => ("lists purge", json!({ "did": did })),
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
            other => return Err(InvalidCommand(format!("unknown command {other:?}"))),
        })
    }

    /// The DID whose per-DID ordering this command takes part in (§6.11.7).
    #[must_use]
    pub fn did(&self) -> Option<&str> {
        match self {
            Self::MemberAdd { did } | Self::MemberRemove { did } | Self::Backfill { did, .. } | Self::ListsPurge { did } => Some(did),
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
            | Self::FarsightStatus
            // A prime writes nothing here, so it waits on no purge.
            | Self::FarsightPrime { .. }
            // Its handler waits on a purge of the viewer itself.
            | Self::FarsightForget { .. } => None,
        }
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
    /// The `community` network's `pds_patterns` (owner eligibility, §6.11.1).
    pub community_patterns: Arc<Vec<String>>,
    /// The block-list index, when `farsight.url` is set.
    pub farsight: Option<Farsight>,
    /// `lists.sync_min_interval_secs`.
    pub sync_min_interval_secs: u32,
    /// A member's budget of list references (design-v3 §4.3).
    pub list_budget: Budget,
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

    /// Run one command.
    pub async fn execute(&self, cmd: &AdminCommand) -> Outcome {
        let result = match cmd {
            AdminCommand::MemberAdd { did } => self.member_add(did).await,
            AdminCommand::MemberRemove { did } => self.member_remove(did).await,
            AdminCommand::LabelAdd { subject, val } => self.label_add(subject, val).await,
            AdminCommand::LabelRemove { subject, val } => self.label_remove(subject, val).await,
            AdminCommand::ScopeRebuild { dry_run } => self.scope_rebuild(*dry_run).await,
            AdminCommand::Backfill { did, depth } => self.backfill(did, *depth).await,
            AdminCommand::Resync => self.resync().await,
            AdminCommand::DeadletterReplay { id } => self.deadletter_replay(id).await,
            AdminCommand::DeadletterDrop { id } => self.deadletter_drop(id).await,
            AdminCommand::FarsightStatus => self.farsight_status().await,
            AdminCommand::FarsightPrime { did, force } => self.farsight_prime(did, *force).await,
            AdminCommand::FarsightForget { did } => self.farsight_forget(did).await,
            AdminCommand::ListsSync { uri } => self.lists_sync(uri).await,
            AdminCommand::ListsPurge { did } => self.lists_purge(did).await,
        };
        result.unwrap_or_else(Outcome::Failed)
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
    /// follow after commit.
    async fn member_add(&self, did: &str) -> Result<Outcome, String> {
        match add_member(&self.pool, &self.scope, &self.private, did, "admin", self.list_budget)
            .await
            .map_err(|e| e.to_string())?
        {
            AddOutcome::Purging => Ok(Outcome::Defer),
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
    ///
    /// A private owner's community-PDS eligibility decides whether it stays
    /// an owner once membership is gone (§6.11.1), so a confirmation older
    /// than 24 h (or none) is refreshed first. The network call runs before
    /// any guard, and its outcome never blocks the removal.
    async fn member_remove(&self, did: &str) -> Result<Outcome, String> {
        let is_member: bool = sqlx::query_scalar("SELECT EXISTS (SELECT 1 FROM members WHERE did = $1)")
            .bind(did)
            .fetch_one(&self.pool)
            .await
            .map_err(|e| e.to_string())?;
        if !is_member {
            return Ok(Outcome::Failed(format!("{did} is not a member")));
        }
        let reconfirmed = self.reconfirm_if_stale(did).await;
        match self.purger.marker(did, PurgeKind::MemberRemoved).await {
            Ok(()) => {}
            Err(crate::aggregations::purge::PurgeError::Busy(_)) => return Ok(Outcome::Defer),
            Err(e) => return Err(e.to_string()),
        }
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
        Ok(Outcome::Done(json!({ "did": did, "status": "removed", "purge": "member_removed", "eligibility": reconfirmed })))
    }

    /// Re-confirm a private owner's eligibility when the last confirmation
    /// is missing or stale. Returns what happened, for the command result.
    async fn reconfirm_if_stale(&self, did: &str) -> &'static str {
        if !self.scope.read_guard().await.is_owner(did) {
            return "not_owner";
        }
        let confirmed_at = match self.pool.acquire().await {
            Ok(mut conn) => self.private.eligibility_confirmed_at(&mut conn, did).await,
            Err(e) => Err(e),
        };
        let confirmed_at = match confirmed_at {
            Ok(t) => t,
            Err(error) => {
                tracing::warn!(event = "member_remove_eligibility_read_failed", did, %error, "eligibility not re-confirmed");
                return "unchecked";
            }
        };
        if !eligibility::is_stale(confirmed_at, Utc::now()) {
            return "fresh";
        }
        let confirmation = eligibility::confirm(self.source.as_ref(), &self.community_patterns, did).await;
        let label = match &confirmation {
            Confirmation::Eligible => "confirmed",
            Confirmation::Ineligible(_) => "ineligible",
            Confirmation::Failed(_) => "failed",
        };
        if let Err(error) = eligibility::apply(&self.pool, &self.scope, &self.private, did, &confirmation).await {
            tracing::warn!(event = "member_remove_eligibility_write_failed", did, %error, "eligibility re-confirmation not recorded");
            return "unchecked";
        }
        tracing::info!(event = "member_remove_eligibility_reconfirmed", did, outcome = label, "stale eligibility re-confirmed before removal");
        label
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

    /// `scope rebuild`: a dry run computes counts inline (read-only); a real
    /// rebuild is scheduled as its own task and the command is done at once.
    async fn scope_rebuild(&self, dry_run: bool) -> Result<Outcome, String> {
        if dry_run {
            let r = self.rebuild.run(true).await.map_err(|e| e.to_string())?;
            return Ok(Outcome::Done(json!({
                "dry_run": true, "justified": r.justified, "would_add": r.added, "would_purge": r.purged
            })));
        }
        let rebuild = self.rebuild.clone();
        tokio::spawn(async move {
            if let Err(error) = rebuild.run(false).await {
                tracing::error!(event = "scope_rebuild_failed", %error, "admin scope rebuild failed");
            }
        });
        Ok(Outcome::Done(json!({ "dry_run": false, "scheduled": "scope_rebuild" })))
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
}

#[cfg(test)]
mod tests {
    use super::*;

    const DID: &str = "did:plc:aaaaaaaaaaaaaaaaaaaaaaaa";

    #[test]
    fn rows_round_trip() {
        for cmd in [
            AdminCommand::MemberAdd { did: DID.into() },
            AdminCommand::MemberRemove { did: DID.into() },
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
    }

    #[test]
    fn per_did_keys() {
        let label = AdminCommand::LabelAdd {
            subject: format!("at://{DID}/app.bsky.feed.post/3k"),
            val: "porn".into(),
        };
        assert_eq!(label.did(), Some(DID));
        assert_eq!(AdminCommand::Resync.did(), None);
    }
}
