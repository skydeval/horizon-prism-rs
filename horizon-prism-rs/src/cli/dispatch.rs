//! CLI side of the admin command queue (design-v2 §3.3). For each mutating
//! command the CLI:
//! 1. inserts an `admin_commands` row and runs `NOTIFY admin_commands` in the
//!    same transaction;
//! 2. waits for `state ∈ {done, failed}`, polling every 500 ms for up to
//!    `admin.command_timeout_secs` (default 600);
//! 3. prints `result`, and exits non-zero on `failed` or on timeout. On
//!    timeout the command keeps running in `serve`.

use std::time::{Duration, Instant};

use clap::{Args, Subcommand};
use serde_json::Value;
use sqlx::PgPool;

use super::commands;
use crate::admin_executor::{AdminCommand, CHANNEL};
use crate::config::AppConfig;
use crate::firehose::jobs::Depth;
use crate::indexing::ScopeValue;

/// Poll interval while waiting for `serve` (§3.3).
pub const POLL: Duration = Duration::from_millis(500);

/// `admin …`
#[derive(Debug, Args)]
pub struct AdminArgs {
    #[command(subcommand)]
    pub command: AdminCmd,
}

#[derive(Debug, Subcommand)]
pub enum AdminCmd {
    /// Community membership (§9.2).
    Member {
        #[command(subcommand)]
        action: MemberAction,
    },
    /// Local labels (§7.8).
    Label {
        #[command(subcommand)]
        action: LabelAction,
    },
    /// Scope maintenance (§9.4).
    Scope {
        #[command(subcommand)]
        action: ScopeAction,
    },
    /// Enqueue a backfill (§5.10).
    Backfill {
        did: String,
        /// member | scope | private | authz | resync (default: member for a
        /// member, scope otherwise).
        #[arg(long)]
        depth: Option<String>,
    },
    /// Clear readiness and re-sync every member and private owner (§5.7).
    Resync {
        #[arg(long, required = true)]
        private_and_members: bool,
    },
    /// Dead-letter handling (§5.3).
    Deadletter {
        #[command(subcommand)]
        action: DeadletterAction,
    },
    /// Firehose stream state (read-only).
    Stream {
        #[command(subcommand)]
        action: StreamAction,
    },
    /// The admin command queue (read-only).
    Commands {
        #[command(subcommand)]
        action: CommandsAction,
    },
    /// The block-list index, Farsight (design-v3 §5.14.11).
    Farsight {
        #[command(subcommand)]
        action: FarsightAction,
    },
    /// Lists that members mute or block (design-v3 §4.3).
    Lists {
        #[command(subcommand)]
        action: ListsAction,
    },
    /// Public indexing as an operator switch (public-indexing design §3.4).
    Indexing {
        #[command(subcommand)]
        action: IndexingAction,
    },
    /// Print a fresh service signing key, its did:key and a correlation key.
    Keygen,
}

#[derive(Debug, Subcommand)]
pub enum IndexingAction {
    /// Turn public indexing off (its data is deleted in the background) or
    /// back on (it is refilled to backfill depth).
    PublicScope {
        /// off | community_scoped
        value: String,
    },
    /// The setting, the running mode and phase, progress and any error.
    Status,
}

#[derive(Debug, Subcommand)]
pub enum ListsAction {
    /// Read a referenced list's owner again, ahead of its usual turn.
    Sync {
        /// The list's at-uri.
        uri: String,
    },
    /// Delete an account's list mutes and the list records it authored
    /// (a local cleanup: records still in its repository come back).
    Purge { did: String },
}

#[derive(Debug, Subcommand)]
pub enum FarsightAction {
    /// Farsight's own status, in brief.
    Status,
    /// Ask Farsight to read an account's repository again.
    Prime {
        did: String,
        /// Run even if Farsight read it recently.
        #[arg(long)]
        force: bool,
    },
    /// Drop every block remembered for one member (farsight.memory).
    Forget { viewer_did: String },
}

#[derive(Debug, Subcommand)]
pub enum MemberAction {
    Add { did: String },
    Remove { did: String },
    List,
}

#[derive(Debug, Subcommand)]
pub enum LabelAction {
    Add { subject: String, val: String },
    Remove { subject: String, val: String },
    List {
        /// Only labels on this URI or DID.
        #[arg(long)]
        subject: Option<String>,
        /// Only labels with this value.
        #[arg(long)]
        val: Option<String>,
    },
}

#[derive(Debug, Subcommand)]
pub enum StreamAction {
    Info,
}

#[derive(Debug, Subcommand)]
pub enum CommandsAction {
    List {
        /// queued | running | done | failed
        #[arg(long)]
        state: Option<String>,
    },
}

/// `migrate …`
#[derive(Debug, Args)]
pub struct MigrateArgs {
    #[command(subcommand)]
    pub command: MigrateCmd,
}

#[derive(Debug, Subcommand)]
pub enum MigrateCmd {
    /// Applied and pending migrations (read-only).
    Status,
}

#[derive(Debug, Subcommand)]
pub enum ScopeAction {
    Rebuild {
        #[arg(long)]
        dry_run: bool,
    },
}

#[derive(Debug, Subcommand)]
pub enum DeadletterAction {
    Replay { id: String },
    Drop { id: String },
    List {
        #[arg(long, default_value_t = commands::deadletter_list::DEFAULT_LIMIT)]
        limit: u32,
    },
}

/// The final state of a submitted command.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Finished {
    Done(Value),
    Failed(Value),
    TimedOut,
}

impl AdminCmd {
    /// The queued command, validated exactly as the executor will parse it.
    ///
    /// # Errors
    ///
    /// A usage message.
    pub fn to_command(&self) -> Result<AdminCommand, String> {
        let cmd = match self {
            Self::Member { action: MemberAction::List }
            | Self::Label { action: LabelAction::List { .. } }
            | Self::Deadletter { action: DeadletterAction::List { .. } }
            | Self::Stream { .. }
            | Self::Commands { .. }
            | Self::Keygen => return Err("read-only commands run in the CLI".to_string()),
            Self::Member { action: MemberAction::Add { did } } => AdminCommand::MemberAdd { did: did.clone() },
            Self::Member { action: MemberAction::Remove { did } } => AdminCommand::MemberRemove { did: did.clone() },
            Self::Label { action: LabelAction::Add { subject, val } } => AdminCommand::LabelAdd {
                subject: subject.clone(),
                val: val.clone(),
            },
            Self::Label { action: LabelAction::Remove { subject, val } } => AdminCommand::LabelRemove {
                subject: subject.clone(),
                val: val.clone(),
            },
            Self::Scope { action: ScopeAction::Rebuild { dry_run } } => AdminCommand::ScopeRebuild { dry_run: *dry_run },
            Self::Backfill { did, depth } => AdminCommand::Backfill {
                did: did.clone(),
                depth: match depth {
                    None => None,
                    Some(d) => Some(Depth::parse(d).ok_or_else(|| format!("unknown depth {d:?}"))?),
                },
            },
            Self::Resync { .. } => AdminCommand::Resync,
            Self::Deadletter { action: DeadletterAction::Replay { id } } => AdminCommand::DeadletterReplay { id: id.clone() },
            Self::Deadletter { action: DeadletterAction::Drop { id } } => AdminCommand::DeadletterDrop { id: id.clone() },
            Self::Farsight { action: FarsightAction::Status } => AdminCommand::FarsightStatus,
            Self::Farsight { action: FarsightAction::Forget { viewer_did } } => AdminCommand::FarsightForget { did: viewer_did.clone() },
            Self::Farsight { action: FarsightAction::Prime { did, force } } => AdminCommand::FarsightPrime {
                did: did.clone(),
                force: *force,
            },
            Self::Lists { action: ListsAction::Sync { uri } } => AdminCommand::ListsSync { uri: uri.clone() },
            Self::Lists { action: ListsAction::Purge { did } } => AdminCommand::ListsPurge { did: did.clone() },
            Self::Indexing { action: IndexingAction::PublicScope { value } } => AdminCommand::IndexingPublicScope {
                value: ScopeValue::parse(value).ok_or_else(|| format!("public-scope takes off or community_scoped, not {value:?}"))?,
            },
            Self::Indexing { action: IndexingAction::Status } => AdminCommand::IndexingStatus,
        };
        let (command, args) = cmd.to_row();
        AdminCommand::parse(command, &args).map_err(|e| e.to_string())
    }
}

/// Insert the row and notify `serve`, in one transaction. Returns the id.
///
/// # Errors
///
/// Database errors.
pub async fn submit(pool: &PgPool, cmd: &AdminCommand) -> Result<i64, sqlx::Error> {
    let (command, args) = cmd.to_row();
    let mut tx = pool.begin().await?;
    let id: i64 = sqlx::query_scalar("INSERT INTO admin_commands (command, args) VALUES ($1, $2) RETURNING id")
        .bind(command)
        .bind(&args)
        .fetch_one(&mut *tx)
        .await?;
    sqlx::query("SELECT pg_notify($1, $2)")
        .bind(CHANNEL)
        .bind(id.to_string())
        .execute(&mut *tx)
        .await?;
    tx.commit().await?;
    Ok(id)
}

/// Poll until the command is `done` or `failed`, or `timeout` passes.
///
/// # Errors
///
/// Database errors.
pub async fn wait(pool: &PgPool, id: i64, timeout: Duration, poll: Duration) -> Result<Finished, sqlx::Error> {
    let deadline = Instant::now() + timeout;
    loop {
        let (state, result): (String, Option<Value>) =
            sqlx::query_as("SELECT state, result FROM admin_commands WHERE id = $1")
                .bind(id)
                .fetch_one(pool)
                .await?;
        match state.as_str() {
            "done" => return Ok(Finished::Done(result.unwrap_or(Value::Null))),
            "failed" => return Ok(Finished::Failed(result.unwrap_or(Value::Null))),
            _ => {}
        }
        if Instant::now() >= deadline {
            return Ok(Finished::TimedOut);
        }
        tokio::time::sleep(poll).await;
    }
}

/// Print a read-only command's output; `Err` is a printable failure.
fn finish(result: Result<String, String>) -> u8 {
    match result {
        Ok(out) => {
            println!("{out}");
            0
        }
        Err(e) => {
            eprintln!("{e}");
            if e.starts_with("config_error") { 78 } else { 1 }
        }
    }
}

async fn read_only(cmd: &AdminCmd) -> Option<Result<String, String>> {
    use commands::{admin_commands_list, deadletter_list, keygen, label_list, member_list, stream_info};
    if let AdminCmd::Keygen = cmd {
        return Some(Ok(keygen::render()));
    }
    let is_read_only = matches!(
        cmd,
        AdminCmd::Member { action: MemberAction::List }
            | AdminCmd::Label { action: LabelAction::List { .. } }
            | AdminCmd::Deadletter { action: DeadletterAction::List { .. } }
            | AdminCmd::Stream { .. }
            | AdminCmd::Commands { .. }
    );
    if !is_read_only {
        return None;
    }
    Some(
        async {
            let cfg = commands::config()?;
            match cmd {
                AdminCmd::Member { .. } => member_list::render(&commands::pool(&cfg).await?).await.map_err(|e| e.to_string()),
                AdminCmd::Label { action: LabelAction::List { subject, val } } => {
                    label_list::render(&commands::pool(&cfg).await?, subject.as_deref(), val.as_deref())
                        .await
                        .map_err(|e| e.to_string())
                }
                AdminCmd::Commands { action: CommandsAction::List { state } } => {
                    admin_commands_list::render(&commands::pool(&cfg).await?, state.as_deref())
                        .await
                        .map_err(|e| e.to_string())
                }
                AdminCmd::Deadletter { action: DeadletterAction::List { limit } } => {
                    let mut redis = commands::redis(&cfg).await?;
                    deadletter_list::render(&mut redis, &cfg.file.firehose.dead_letter_stream, *limit).await
                }
                AdminCmd::Stream { .. } => {
                    let mut redis = commands::redis(&cfg).await?;
                    stream_info::render(&mut redis, &cfg.file.firehose.stream_key, &cfg.file.firehose.group).await
                }
                _ => Err("not a read-only command".to_string()),
            }
        }
        .await,
    )
}

/// `migrate status` (read-only). Returns the process exit code.
pub async fn run_migrate(args: &MigrateArgs) -> u8 {
    match args.command {
        MigrateCmd::Status => {
            let result = async {
                let cfg = commands::config()?;
                let pool = commands::pool(&cfg).await?;
                commands::migrate_status::render(&pool).await.map_err(|e| e.to_string())
            }
            .await;
            finish(result)
        }
    }
}

/// Run one admin command from the CLI: read-only commands directly,
/// mutating ones through `serve`'s executor. Returns the process exit code.
pub async fn run(args: &AdminArgs) -> u8 {
    if let Some(result) = read_only(&args.command).await {
        return finish(result);
    }
    let cmd = match args.command.to_command() {
        Ok(c) => c,
        Err(e) => {
            eprintln!("{e}");
            return 2;
        }
    };
    let cfg = match AppConfig::load_from_env() {
        Ok(c) => c,
        Err(e) => {
            eprintln!("config_error: {e}");
            return 78;
        }
    };
    let pool = match crate::db::connect(&cfg).await {
        Ok(p) => p,
        Err(e) => {
            eprintln!("database connect: {e}");
            return 1;
        }
    };
    let id = match submit(&pool, &cmd).await {
        Ok(id) => id,
        Err(e) => {
            eprintln!("queueing the command failed: {e}");
            return 1;
        }
    };
    eprintln!("queued admin command {id}; waiting for serve");
    let timeout = Duration::from_secs(cfg.file.admin.command_timeout_secs);
    match wait(&pool, id, timeout, POLL).await {
        Ok(Finished::Done(result)) => {
            println!("{result}");
            0
        }
        Ok(Finished::Failed(result)) => {
            println!("{result}");
            1
        }
        Ok(Finished::TimedOut) => {
            eprintln!(
                "timed out after {}s; command {id} keeps running in serve (see `admin commands list`)",
                timeout.as_secs()
            );
            1
        }
        Err(e) => {
            eprintln!("waiting for the command failed: {e}");
            1
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cli_validates_like_the_executor() {
        let bad = AdminCmd::Label {
            action: LabelAction::Add {
                subject: "did:plc:aaaaaaaaaaaaaaaaaaaaaaaa".into(),
                val: "spam".into(),
            },
        };
        assert!(bad.to_command().expect_err("rejected").contains("!takedown"));
        let ok = AdminCmd::Backfill {
            did: "did:plc:aaaaaaaaaaaaaaaaaaaaaaaa".into(),
            depth: Some("authz".into()),
        };
        assert_eq!(
            ok.to_command(),
            Ok(AdminCommand::Backfill {
                did: "did:plc:aaaaaaaaaaaaaaaaaaaaaaaa".into(),
                depth: Some(Depth::Authz)
            })
        );
        let off = AdminCmd::Indexing { action: IndexingAction::PublicScope { value: "off".into() } };
        assert_eq!(off.to_command(), Ok(AdminCommand::IndexingPublicScope { value: ScopeValue::Off }));
        let full = AdminCmd::Indexing { action: IndexingAction::PublicScope { value: "full".into() } };
        assert!(full.to_command().is_err());
        let status = AdminCmd::Indexing { action: IndexingAction::Status };
        assert_eq!(status.to_command(), Ok(AdminCommand::IndexingStatus));
    }
}
