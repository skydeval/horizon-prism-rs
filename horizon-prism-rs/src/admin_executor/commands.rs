//! The mutating admin commands (design-v2 §3.3) and their handlers.
//!
//! A command is stored in `admin_commands` as `(command, args jsonb)`; the
//! CLI writes it ([`AdminCommand::to_row`]) and `serve`'s executor parses and
//! runs it ([`Handlers::execute`]) through the same code paths and locks as
//! the rest of `serve`.

use std::sync::Arc;

use redis::aio::ConnectionManager;
use serde_json::{Value, json};
use sqlx::PgPool;

use crate::aggregations::{PurgeKind, Purger, Rebuild, ScopeState};
use crate::aggregations::membership::{AddOutcome, add_member};
use crate::db::locks::uri_author;
use crate::firehose::backfill::BackfillSource;
use crate::firehose::gap::{self, GapCause};
use crate::firehose::jobs::{self, Depth};
use crate::firehose::writer::{Action, ApplyOptions, OpOutcome, RawOp, Writer};
use crate::private_tier::PrivateTier;

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
            other => return Err(InvalidCommand(format!("unknown command {other:?}"))),
        })
    }

    /// The DID whose per-DID ordering this command takes part in (§6.11.7).
    #[must_use]
    pub fn did(&self) -> Option<&str> {
        match self {
            Self::MemberAdd { did } | Self::MemberRemove { did } | Self::Backfill { did, .. } => Some(did),
            Self::LabelAdd { subject, .. } | Self::LabelRemove { subject, .. } => {
                if is_did(subject) { Some(subject) } else { uri_author(subject) }
            }
            Self::ScopeRebuild { .. } | Self::Resync | Self::DeadletterReplay { .. } | Self::DeadletterDrop { .. } => None,
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
        };
        result.unwrap_or_else(Outcome::Failed)
    }

    /// `member add` (§9.2, §6.11.1): under the write guard and the DID's
    /// shared lock, insert the member, put it in S (reason bit 1), enqueue a
    /// `member` backfill, and re-evaluate owner status; the in-memory sets
    /// follow after commit.
    async fn member_add(&self, did: &str) -> Result<Outcome, String> {
        match add_member(&self.pool, &self.scope, &self.private, did, "admin")
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
        Ok(Outcome::Done(json!({ "did": did, "status": "removed", "purge": "member_removed" })))
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
        let pds = self.source.prepare(&repo).await?;
        let rev = self.source.latest_rev(&pds, &repo).await?;
        let record = self.source.get_record(&pds, &repo, collection, rkey).await?;
        let op = match &record {
            Some(r) => RawOp {
                action: Action::Create,
                collection: collection.to_string(),
                rkey: rkey.to_string(),
                cid: Some(r.cid.clone()),
                record: Some(r.value.clone()),
            },
            None => RawOp {
                action: Action::Delete,
                collection: collection.to_string(),
                rkey: rkey.to_string(),
                cid: None,
                record: None,
            },
        };
        let options = ApplyOptions {
            advance_applied_rev: false,
            dead_letter_readiness: false,
            attempts: self.deterministic_attempts,
        };
        let report = self.writer.apply(&repo, &rev, &[op], options).await.map_err(|e| e.to_string())?;
        let status = match report.outcomes.first() {
            Some(OpOutcome::Written) if record.is_some() => "indexed",
            Some(OpOutcome::Written) => "deleted",
            Some(OpOutcome::Unchanged) => "unchanged",
            Some(OpOutcome::Rejected(_) | OpOutcome::Skipped(_)) => "out_of_scope",
            Some(OpOutcome::Failed(e)) => return Ok(Outcome::Failed(format!("replay failed: {e}"))),
            None => "unchanged",
        };
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
