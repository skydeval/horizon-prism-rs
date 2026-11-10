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
    /// Community membership.
    Member {
        #[command(subcommand)]
        action: MemberAction,
    },
    /// Local labels.
    Label {
        #[command(subcommand)]
        action: LabelAction,
    },
    /// The nightly clean-up of the accounts Prism keeps (Storage, Jobs).
    Scope {
        #[command(subcommand)]
        action: ScopeAction,
    },
    /// Enqueue a backfill.
    Backfill {
        did: String,
        /// member | scope | private | authz | resync (default: member for a
        /// member, scope otherwise).
        #[arg(long)]
        depth: Option<String>,
    },
    /// Clear readiness and re-sync every member and private owner.
    Resync {
        #[arg(long, required = true)]
        private_and_members: bool,
    },
    /// Problems: records Prism could not process.
    Deadletter {
        #[command(subcommand)]
        action: DeadletterAction,
    },
    /// The network stream's state (Relays), read-only.
    Stream {
        #[command(subcommand)]
        action: StreamAction,
    },
    /// The relays' stream readers.
    Firehose {
        #[command(subcommand)]
        action: FirehoseAction,
    },
    /// The admin command queue (read-only).
    Commands {
        #[command(subcommand)]
        action: CommandsAction,
    },
    /// Blocks · Farsight: the block-list index.
    Farsight {
        #[command(subcommand)]
        action: FarsightAction,
    },
    /// Lists that members mute or block.
    Lists {
        #[command(subcommand)]
        action: ListsAction,
    },
    /// Indexing: what Prism indexes, and its progress.
    Indexing {
        #[command(subcommand)]
        action: IndexingAction,
    },
    /// Print a fresh service signing key, its did:key and a correlation key.
    Keygen,
    /// Who may use the web admin UI, and from which servers. The
    /// break-glass when nobody can sign in.
    Operator {
        #[command(subcommand)]
        action: OperatorAction,
    },
    /// Print the first-run setup code while no operator exists (read-only).
    SetupCode,
    /// Account lookups · lodestar (read-only).
    Identity {
        #[command(subcommand)]
        action: IdentityAction,
    },
    /// Every setting the web admin changes (All settings).
    Settings {
        #[command(subcommand)]
        action: SettingsAction,
    },
    /// Restart serve: it shuts down as at SIGTERM and exits for its
    /// supervisor to start it again (needs APP_SUPERVISED=1).
    Restart,
    /// Communities · Refract: accounts kept for the listed communities.
    Refract {
        #[command(subcommand)]
        action: RefractAction,
    },
}

#[derive(Debug, Subcommand)]
pub enum RefractAction {
    /// The switch, the communities and their entry hashes, counts by
    /// state, pending ends, label cursors (read-only).
    Status,
    /// The accounts Refract knows (read-only).
    List {
        /// candidate | rejected | restored | grace | ending
        #[arg(long)]
        state: Option<String>,
    },
    /// One account: its row, last verdict, labels and events (read-only).
    Show { did: String },
    /// Check an account now: due now if known, else a candidate.
    Check { did: String },
    /// End an account that is restored or in grace.
    End {
        did: String,
        #[arg(long)]
        reason: Option<String>,
    },
    /// Hold a pending end (`feature` or `entry:<hash>`).
    Keep { scope: String },
    /// Release a held pending end; its clock starts again.
    Unkeep { scope: String },
}

#[derive(Debug, Subcommand)]
pub enum SettingsAction {
    /// Every setting, with the value saved and app.toml's (read-only).
    List,
    /// One setting in full (read-only).
    Get { key: String },
    /// Save a value; it applies at once or at the next restart. Lists are
    /// separated by commas; JSON is accepted too.
    Set { key: String, value: String },
    /// Remove the saved value: app.toml's value (or the default) applies.
    Reset { key: String },
}

#[derive(Debug, Subcommand)]
pub enum IdentityAction {
    /// Resolve DIDs and handles as with `identity.lodestar_url` empty and
    /// as with it set, side by side. Changes nothing.
    Compare {
        /// Also compare this many DIDs picked at random from `actors`, plus
        /// a few `did:web` and accounts marked deleted.
        #[arg(long, default_value_t = 0)]
        sample: u32,
        /// DIDs and handles to compare.
        inputs: Vec<String>,
    },
}

#[derive(Debug, Subcommand)]
pub enum OperatorAction {
    /// Add an operator; their server's exact host is allowed unless a
    /// pattern already covers it. With no operator yet, this also closes
    /// first-run setup.
    Add {
        /// A handle or DID.
        account: String,
        /// Your own label for them, e.g. a name.
        #[arg(long)]
        note: Option<String>,
    },
    /// Remove an operator (never the last one).
    Remove { did: String },
    /// Operators and allowed servers (read-only).
    List,
    /// The servers operators may sign in from.
    Host {
        #[command(subcommand)]
        action: HostAction,
    },
}

#[derive(Debug, Subcommand)]
pub enum HostAction {
    /// Allow a server: an exact host, or `*.` and a host for every server
    /// under it.
    Add { pattern: String },
    /// Stop allowing a server (refused if no operator could sign in).
    Remove { pattern: String },
}

#[derive(Debug, Subcommand)]
pub enum IndexingAction {
    /// Set what Prism indexes: kryphocron_only (stored public data is
    /// deleted in the background), community (it is refilled) or full (the
    /// whole network, from now on).
    PublicScope {
        /// kryphocron_only | community | full
        value: String,
        /// The confirm word: FULL to switch to full, COMMUNITY to leave it.
        #[arg(long)]
        confirm: Option<String>,
    },
    /// The setting, the running mode and phase, progress and any error.
    Status,
    /// A full network trial: full indexing for a bounded time, then back to
    /// the mode it started from, with what it added removed and checked.
    FullTrial {
        #[command(subcommand)]
        action: FullTrialAction,
    },
    /// Mark a record group's history (or every group's) complete by hand,
    /// before every account was crawled: the accounts not crawled yet stay
    /// on the group's incomplete list and keep reading today's sources.
    FullComplete {
        /// Required: the group is marked complete although accounts are
        /// left.
        #[arg(long)]
        incomplete: bool,
        /// One group (posts, likes, reposts, follows, blocks, lists,
        /// profiles, starter_packs, polls); every group when left out.
        group: Option<String>,
    },
    /// Advice only: whether this machine applies the full network as fast
    /// as the relays send it. Builds a scratch database beside this one
    /// (dropped after), seeds it, applies commits like the network's at
    /// each lane count and compares with the relays' rate.
    FullCheck {
        /// Seeded accounts.
        #[arg(long, default_value_t = 200_000)]
        accounts: u32,
        /// Seeded posts.
        #[arg(long, default_value_t = 500_000)]
        posts: u32,
        /// Seeded likes.
        #[arg(long, default_value_t = 2_000_000)]
        likes: u32,
        /// Seeded follows.
        #[arg(long, default_value_t = 500_000)]
        follows: u32,
        /// Commits applied at each lane count.
        #[arg(long, default_value_t = 20_000)]
        commits: u32,
        /// Lane counts to try, comma-separated.
        #[arg(long, value_delimiter = ',', default_value = "1,2,4,8")]
        lanes: Vec<u32>,
        /// Commits per writer transaction; firehose.lane_batch when not given.
        #[arg(long)]
        lane_batch: Option<u32>,
        /// Write every op one at a time, without the bulk arm.
        #[arg(long)]
        no_bulk_arm: bool,
        /// Keep the scratch database afterwards.
        #[arg(long)]
        keep: bool,
    },
}

#[derive(Debug, Subcommand)]
pub enum FullTrialAction {
    /// Start a trial from community or kryphocron_only. Unset limits take
    /// `[full_trial]`'s.
    Start {
        /// End this many hours after the start (a fraction is allowed).
        #[arg(long)]
        hours: Option<f64>,
        /// End when the database has grown by this many GB (a fraction is
        /// allowed).
        #[arg(long)]
        max_added_gb: Option<f64>,
        /// End when the database volume's free space falls under this share.
        #[arg(long)]
        end_below_free_percent: Option<u32>,
        /// Crawl history too, as `[full_crawl]` says.
        #[arg(long)]
        history: bool,
        /// The confirm word: FULL.
        #[arg(long)]
        confirm: Option<String>,
    },
    /// End the running trial now.
    End {
        /// The confirm word: END.
        #[arg(long)]
        confirm: Option<String>,
    },
    /// Keep full on for real; the trial's record is dropped.
    Keep {
        /// The confirm word: FULL.
        #[arg(long)]
        confirm: Option<String>,
    },
    /// Change the running trial's limits.
    Bounds {
        #[arg(long)]
        hours: Option<f64>,
        #[arg(long)]
        max_added_gb: Option<f64>,
        #[arg(long)]
        end_below_free_percent: Option<u32>,
    },
    /// Run a failed trial's check again.
    Check,
    /// Drop a failed trial's record and keep its result as dismissed.
    Dismiss {
        /// The confirm word: DISMISS.
        #[arg(long)]
        confirm: Option<String>,
    },
    /// The trial, its figures and the last result.
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
    /// Exclude a DID: never auto-enrolled, refused by `add` until `allow`,
    /// and removed if a member.
    Deny {
        did: String,
        /// The operator's note, never shown to the DID.
        #[arg(long)]
        reason: Option<String>,
    },
    /// Lift a denial. Adds nothing.
    Allow { did: String },
    List {
        /// List the denials instead of the members.
        #[arg(long)]
        denied: bool,
    },
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
pub enum FirehoseAction {
    /// Forget the capability a relay's reader last stamped, so a reader
    /// downgraded to one that stamps nothing counts as unfiltered again.
    ForgetCap {
        /// The relay's label (firehose.relay_labels).
        label: String,
    },
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
    /// Remove the listed records the firehose would now set aside
    /// (malformed, from accounts Prism does not keep).
    Sweep {
        #[arg(long)]
        dry_run: bool,
    },
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
            Self::Member { action: MemberAction::List { .. } }
            | Self::Label { action: LabelAction::List { .. } }
            | Self::Deadletter { action: DeadletterAction::List { .. } }
            | Self::Stream { .. }
            | Self::Commands { .. }
            | Self::Operator { action: OperatorAction::List }
            | Self::SetupCode
            | Self::Identity { .. }
            | Self::Settings { action: SettingsAction::List | SettingsAction::Get { .. } }
            | Self::Refract { action: RefractAction::Status | RefractAction::List { .. } | RefractAction::Show { .. } }
            | Self::Indexing { action: IndexingAction::FullCheck { .. } }
            | Self::Keygen => return Err("read-only commands run in the CLI".to_string()),
            // They read the saved row's version first: see `settings_change`.
            Self::Settings { action: SettingsAction::Set { .. } | SettingsAction::Reset { .. } } => {
                return Err("settings set and reset read the database first".to_string());
            }
            Self::Restart => AdminCommand::Restart,
            Self::Operator { action: OperatorAction::Add { account, note } } => AdminCommand::OperatorAdd {
                input: account.trim().trim_start_matches('@').to_ascii_lowercase(),
                note: note.clone(),
                add_host: true,
            },
            Self::Operator { action: OperatorAction::Remove { did } } => AdminCommand::OperatorRemove { did: did.clone() },
            Self::Operator { action: OperatorAction::Host { action: HostAction::Add { pattern } } } => AdminCommand::OperatorHostAdd {
                pattern: crate::admin::operators::parse_pattern(pattern)?,
            },
            Self::Operator { action: OperatorAction::Host { action: HostAction::Remove { pattern } } } => {
                AdminCommand::OperatorHostRemove { pattern: pattern.trim().to_ascii_lowercase() }
            }
            Self::Member { action: MemberAction::Add { did } } => AdminCommand::MemberAdd { did: did.clone() },
            Self::Member { action: MemberAction::Remove { did } } => AdminCommand::MemberRemove { did: did.clone() },
            Self::Member { action: MemberAction::Deny { did, reason } } => AdminCommand::MemberDeny {
                did: did.clone(),
                reason: reason.clone(),
            },
            Self::Member { action: MemberAction::Allow { did } } => AdminCommand::MemberAllow { did: did.clone() },
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
            Self::Deadletter { action: DeadletterAction::Sweep { dry_run } } => AdminCommand::DeadletterSweep { dry_run: *dry_run },
            Self::Farsight { action: FarsightAction::Status } => AdminCommand::FarsightStatus,
            Self::Farsight { action: FarsightAction::Forget { viewer_did } } => AdminCommand::FarsightForget { did: viewer_did.clone() },
            Self::Farsight { action: FarsightAction::Prime { did, force } } => AdminCommand::FarsightPrime {
                did: did.clone(),
                force: *force,
            },
            Self::Lists { action: ListsAction::Sync { uri } } => AdminCommand::ListsSync { uri: uri.clone() },
            Self::Lists { action: ListsAction::Purge { did } } => AdminCommand::ListsPurge { did: did.clone() },
            Self::Indexing { action: IndexingAction::PublicScope { value, .. } } => AdminCommand::IndexingPublicScope {
                value: ScopeValue::parse(value).ok_or_else(|| format!("public-scope takes kryphocron_only, community or full, not {value:?}"))?,
            },
            Self::Indexing { action: IndexingAction::Status } => AdminCommand::IndexingStatus,
            Self::Indexing { action: IndexingAction::FullComplete { incomplete, group } } => {
                if !incomplete {
                    return Err("full-complete marks a group complete with accounts left; pass --incomplete to say so".to_string());
                }
                if let Some(g) = group
                    && crate::config::FullGroup::parse(g).is_none()
                {
                    return Err(format!("{g} is not a record group of full indexing"));
                }
                AdminCommand::FullComplete { group: group.clone() }
            }
            Self::Indexing { action: IndexingAction::FullTrial { action } } => match action {
                FullTrialAction::Start { hours, max_added_gb, end_below_free_percent, history, .. } => AdminCommand::FullTrialStart {
                    hours: hours.map(crate::config::Float),
                    max_added_gb: max_added_gb.map(crate::config::Float),
                    end_below_free_percent: *end_below_free_percent,
                    history: *history,
                },
                FullTrialAction::End { .. } => AdminCommand::FullTrialEnd,
                FullTrialAction::Keep { .. } => AdminCommand::FullTrialKeep,
                FullTrialAction::Bounds { hours, max_added_gb, end_below_free_percent } => AdminCommand::FullTrialBounds {
                    hours: hours.map(crate::config::Float),
                    max_added_gb: max_added_gb.map(crate::config::Float),
                    end_below_free_percent: *end_below_free_percent,
                },
                FullTrialAction::Check => AdminCommand::FullTrialCheck,
                FullTrialAction::Dismiss { .. } => AdminCommand::FullTrialDismiss,
                FullTrialAction::Status => AdminCommand::FullTrialStatus,
            },
            Self::Firehose { action: FirehoseAction::ForgetCap { label } } => AdminCommand::FirehoseForgetCap { label: label.clone() },
            Self::Refract { action: RefractAction::Check { did } } => AdminCommand::RefractCheck { did: did.clone() },
            Self::Refract { action: RefractAction::End { did, reason } } => AdminCommand::RefractEnd { did: did.clone(), reason: reason.clone() },
            Self::Refract { action: RefractAction::Keep { scope } } => AdminCommand::RefractKeep { scope: scope.clone(), keep: true },
            Self::Refract { action: RefractAction::Unkeep { scope } } => AdminCommand::RefractKeep { scope: scope.clone(), keep: false },
        };
        let (command, args) = cmd.to_row();
        AdminCommand::parse(command, &args).map_err(|e| e.to_string())
    }
}

/// The confirm word a switch into or out of full needs: `FULL` to switch to
/// full, after its warnings; `COMMUNITY` to leave full for community.
fn scope_word(value: ScopeValue, running_full: bool) -> Option<&'static str> {
    match value {
        ScopeValue::Full => Some("FULL"),
        ScopeValue::Community if running_full => Some("COMMUNITY"),
        _ => None,
    }
}

/// Ask for the confirm word, showing what the switch means.
async fn confirm_scope(pool: &PgPool, cfg: &crate::config::AppConfig, value: ScopeValue, confirm: Option<&str>) -> Result<(), String> {
    let breadth: Option<String> = sqlx::query_scalar("SELECT value->>'breadth' FROM worker_state WHERE key = 'public_transition'")
        .fetch_optional(pool)
        .await
        .map_err(|e| e.to_string())?
        .flatten();
    let Some(word) = scope_word(value, breadth.as_deref() == Some("full")) else {
        return Ok(());
    };
    if confirm == Some(word) {
        return Ok(());
    }
    let mut text = Vec::new();
    if value == ScopeValue::Full {
        let used: Option<i64> = sqlx::query_scalar("SELECT pg_database_size(current_database())").fetch_one(pool).await.ok();
        let free = crate::indexing::full::database_free_bytes(&cfg.env.data_dir);
        text.push("Switching to full: Prism stores and serves the public network for the enabled record groups.".to_string());
        text.extend(crate::indexing::full::warnings(
            &cfg.file.full.kinds,
            used.and_then(|u| u64::try_from(u).ok()),
            free,
            cfg.file.storage.budget_bytes,
            cfg.file.firehose.writer_lanes,
        ));
        let last = crate::indexing::full_check::last_words(pool).await.map_err(|e| e.to_string())?;
        text.extend(crate::indexing::full::more_warnings(&cfg.file, last));
        text.extend(crate::indexing::full::postgres_warnings(pool).await);
    } else {
        text.push(
            "Leaving full for community: the rows full added are deleted in the background, then S is refilled; public requests answer 501 during that refill."
                .to_string(),
        );
    }
    text.push(format!("Run again with --confirm {word} to switch."));
    Err(text.join("\n"))
}

/// A trial's confirm word: `FULL` to start (after its warnings, bounds and
/// what it does not exercise or restore) or keep, `END` to end, `DISMISS`
/// to dismiss.
async fn confirm_trial(pool: &PgPool, cfg: &crate::config::AppConfig, action: &FullTrialAction) -> Result<(), String> {
    let (word, confirm) = match action {
        FullTrialAction::Start { confirm, .. } | FullTrialAction::Keep { confirm } => ("FULL", confirm),
        FullTrialAction::End { confirm } => ("END", confirm),
        FullTrialAction::Dismiss { confirm } => ("DISMISS", confirm),
        FullTrialAction::Bounds { .. } | FullTrialAction::Check | FullTrialAction::Status => return Ok(()),
    };
    if confirm.as_deref() == Some(word) {
        return Ok(());
    }
    let text = match action {
        FullTrialAction::Start { hours, max_added_gb, end_below_free_percent, history, .. } => {
            let d = &cfg.file.full_trial;
            let bounds = crate::indexing::trial::Bounds {
                hours: hours.unwrap_or(d.hours.0),
                max_added_gb: max_added_gb.unwrap_or(d.max_added_gb.0),
                end_below_free_percent: end_below_free_percent.unwrap_or(d.end_below_free_percent),
                history: *history,
            };
            let phase: Option<String> = sqlx::query_scalar("SELECT value->>'phase' FROM worker_state WHERE key = 'public_transition'")
                .fetch_optional(pool)
                .await
                .map_err(|e| e.to_string())?
                .flatten();
            let from = if phase.as_deref() == Some("off") { ScopeValue::KryphocronOnly } else { ScopeValue::Community };
            let used: Option<i64> = sqlx::query_scalar("SELECT pg_database_size(current_database())").fetch_one(pool).await.ok();
            let free = crate::indexing::trial::volume(&cfg.env.data_dir);
            let mut warnings = crate::indexing::full::warnings(&cfg.file.full.kinds, used.and_then(|u| u64::try_from(u).ok()), free.map(|f| f.0), cfg.file.storage.budget_bytes, cfg.file.firehose.writer_lanes);
            let last = crate::indexing::full_check::last_words(pool).await.map_err(|e| e.to_string())?;
            // A live-only trial subscribes from the tip whatever the option.
            let mut file = cfg.file.clone();
            file.full.label_backfill &= bounds.history;
            warnings.extend(crate::indexing::full::more_warnings(&file, last));
            warnings.extend(crate::indexing::full::postgres_warnings(pool).await);
            // How long the last fill took: its last finished job after its start.
            let last_fill: Option<i64> = sqlx::query_scalar(
                "SELECT EXTRACT(EPOCH FROM (max(updated_at) - min(not_before)))::bigint FROM backfill_jobs WHERE depth = 'resync' AND state = 'done'",
            )
            .fetch_one(pool)
            .await
            .ok()
            .flatten();
            let first = crate::indexing::trial::first_bound(bounds, free, crate::indexing::full::GROWTH_BYTES_PER_DAY);
            crate::indexing::trial::confirm_text(from, bounds, &warnings, last_fill.map(chrono::TimeDelta::seconds), first).join("\n")
        }
        FullTrialAction::Keep { .. } => {
            "Keeping full: the trial's record is dropped and full indexing stays on for real.\nRun again with --confirm FULL to keep it.".to_string()
        }
        FullTrialAction::End { .. } => "Ending the trial: Prism leaves full, removes what it added and checks it.\nRun again with --confirm END to end it.".to_string(),
        _ => "Dismissing drops the failed trial's record; its result is kept as dismissed.\nRun again with --confirm DISMISS to dismiss it.".to_string(),
    };
    Err(text)
}

/// Insert the row and notify `serve`, in one transaction. Returns the id.
///
/// # Errors
///
/// Database errors.
pub async fn submit(pool: &PgPool, cmd: &AdminCommand) -> Result<i64, sqlx::Error> {
    submit_as(pool, cmd, None).await
}

/// [`submit`] for a requester: the operator DID behind a command made in
/// the admin UI, or `None` for the CLI (admin-ui design §5.8).
///
/// # Errors
///
/// Database errors.
pub async fn submit_as(pool: &PgPool, cmd: &AdminCommand, requested_by: Option<&str>) -> Result<i64, sqlx::Error> {
    let (command, args) = cmd.to_row();
    let mut tx = pool.begin().await?;
    let id: i64 = sqlx::query_scalar("INSERT INTO admin_commands (command, args, requested_by) VALUES ($1, $2, $3) RETURNING id")
        .bind(command)
        .bind(&args)
        .bind(requested_by)
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
    use commands::{admin_commands_list, deadletter_list, keygen, label_list, member_list, operator_list, setup_code, stream_info};
    if let AdminCmd::Keygen = cmd {
        return Some(Ok(keygen::render()));
    }
    let is_read_only = matches!(
        cmd,
        AdminCmd::Member { action: MemberAction::List { .. } }
            | AdminCmd::Label { action: LabelAction::List { .. } }
            | AdminCmd::Deadletter { action: DeadletterAction::List { .. } }
            | AdminCmd::Stream { .. }
            | AdminCmd::Commands { .. }
            | AdminCmd::Operator { action: OperatorAction::List }
            | AdminCmd::SetupCode
            | AdminCmd::Identity { .. }
            | AdminCmd::Settings { action: SettingsAction::List | SettingsAction::Get { .. } }
            | AdminCmd::Refract { action: RefractAction::Status | RefractAction::List { .. } | RefractAction::Show { .. } }
            | AdminCmd::Indexing { action: IndexingAction::FullCheck { .. } }
    );
    if !is_read_only {
        return None;
    }
    Some(
        async {
            let cfg = commands::config()?;
            match cmd {
                AdminCmd::Member { action: MemberAction::List { denied } } => {
                    member_list::render(&commands::pool(&cfg).await?, *denied).await.map_err(|e| e.to_string())
                }
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
                AdminCmd::Operator { action: OperatorAction::List } => {
                    operator_list::render(&commands::pool(&cfg).await?).await.map_err(|e| e.to_string())
                }
                AdminCmd::SetupCode => setup_code::render(&commands::pool(&cfg).await?, &cfg.env.data_dir).await,
                AdminCmd::Identity { action: IdentityAction::Compare { sample, inputs } } => {
                    commands::identity_compare::render(&cfg, *sample, inputs).await
                }
                AdminCmd::Settings { action: SettingsAction::List } => {
                    commands::settings::render_list(&commands::pool(&cfg).await?, &cfg.file).await.map_err(|e| e.to_string())
                }
                AdminCmd::Settings { action: SettingsAction::Get { key } } => {
                    commands::settings::render_get(&commands::pool(&cfg).await?, &cfg.file, key).await
                }
                AdminCmd::Refract { action: RefractAction::Status } => commands::refract::render_status(&commands::pool(&cfg).await?, &cfg.file).await,
                AdminCmd::Refract { action: RefractAction::List { state } } => {
                    commands::refract::render_list(&commands::pool(&cfg).await?, state.as_deref()).await
                }
                AdminCmd::Refract { action: RefractAction::Show { did } } => commands::refract::render_show(&commands::pool(&cfg).await?, did).await,
                AdminCmd::Indexing { action: IndexingAction::FullCheck { accounts, posts, likes, follows, commits, lanes, lane_batch, no_bulk_arm, keep } } => {
                    let params = crate::indexing::full_check::Params {
                        accounts: *accounts,
                        posts: *posts,
                        likes: *likes,
                        follows: *follows,
                        commits: *commits,
                        lanes: lanes.clone(),
                        lane_batch: lane_batch.unwrap_or(cfg.file.firehose.lane_batch).max(1),
                        bulk_arm: !*no_bulk_arm && cfg.file.firehose.bulk_arm,
                        keep: *keep,
                        ..crate::indexing::full_check::Params::default()
                    };
                    crate::indexing::full_check::run(&cfg, &params).await
                }
                _ => Err("not a read-only command".to_string()),
            }
        }
        .await,
    )
}

/// `settings set <key> <value>` or `settings reset <key>` as a command:
/// the value read as the form reads it, against the saved row's version.
async fn settings_change(cmd: &AdminCmd, cfg: &AppConfig, pool: &PgPool) -> Result<AdminCommand, String> {
    let (key, value) = match cmd {
        AdminCmd::Settings { action: SettingsAction::Set { key, value } } => (key, Some(commands::settings::parse_value(&cfg.file, key, value)?)),
        AdminCmd::Settings { action: SettingsAction::Reset { key } } => (key, None),
        _ => return Err("not a settings change".to_string()),
    };
    let base_version = commands::settings::version(pool, key).await.map_err(|e| e.to_string())?;
    let cmd = AdminCommand::SettingsSet { changes: vec![crate::admin_executor::Change { key: key.clone(), value, base_version }] };
    let (command, args) = cmd.to_row();
    AdminCommand::parse(command, &args).map_err(|e| e.to_string())
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
    // `settings set` and `reset` are built once the database is open.
    let early = match &args.command {
        AdminCmd::Settings { action: SettingsAction::Set { .. } | SettingsAction::Reset { .. } } => None,
        other => match other.to_command() {
            Ok(c) => Some(c),
            Err(e) => {
                eprintln!("{e}");
                return 2;
            }
        },
    };
    let cfg = match crate::cli::commands::lenient_config() {
        Ok(c) => c,
        Err(e) => {
            eprintln!("{e}");
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
    if let (AdminCmd::Indexing { action: IndexingAction::PublicScope { confirm, .. } }, Some(AdminCommand::IndexingPublicScope { value })) =
        (&args.command, &early)
        && let Err(e) = confirm_scope(&pool, &cfg, *value, confirm.as_deref()).await
    {
        eprintln!("{e}");
        return 2;
    }
    if let AdminCmd::Indexing { action: IndexingAction::FullTrial { action } } = &args.command
        && let Err(e) = confirm_trial(&pool, &cfg, action).await
    {
        eprintln!("{e}");
        return 2;
    }
    let cmd = match early {
        Some(c) => c,
        None => match settings_change(&args.command, &cfg, &pool).await {
            Ok(c) => c,
            Err(e) => {
                eprintln!("{e}");
                return 2;
            }
        },
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
        let off = AdminCmd::Indexing { action: IndexingAction::PublicScope { value: "kryphocron_only".into(), confirm: None } };
        assert_eq!(off.to_command(), Ok(AdminCommand::IndexingPublicScope { value: ScopeValue::KryphocronOnly }));
        let full = AdminCmd::Indexing { action: IndexingAction::PublicScope { value: "full".into(), confirm: None } };
        assert_eq!(full.to_command(), Ok(AdminCommand::IndexingPublicScope { value: ScopeValue::Full }));
        let wide = AdminCmd::Indexing { action: IndexingAction::PublicScope { value: "everything".into(), confirm: None } };
        assert!(wide.to_command().is_err());
        // The confirm words: FULL into full, COMMUNITY out of it.
        assert_eq!(scope_word(ScopeValue::Full, false), Some("FULL"));
        assert_eq!(scope_word(ScopeValue::Community, true), Some("COMMUNITY"));
        assert_eq!(scope_word(ScopeValue::Community, false), None);
        assert_eq!(scope_word(ScopeValue::KryphocronOnly, true), None);
        let status = AdminCmd::Indexing { action: IndexingAction::Status };
        assert_eq!(status.to_command(), Ok(AdminCommand::IndexingStatus));
        let did = "did:plc:aaaaaaaaaaaaaaaaaaaaaaaa";
        let deny = AdminCmd::Member { action: MemberAction::Deny { did: did.into(), reason: Some("spam".into()) } };
        assert_eq!(deny.to_command(), Ok(AdminCommand::MemberDeny { did: did.into(), reason: Some("spam".into()) }));
        let allow = AdminCmd::Member { action: MemberAction::Allow { did: did.into() } };
        assert_eq!(allow.to_command(), Ok(AdminCommand::MemberAllow { did: did.into() }));
        assert!(AdminCmd::Member { action: MemberAction::Deny { did: "bob".into(), reason: None } }.to_command().is_err());
        assert!(AdminCmd::Member { action: MemberAction::List { denied: true } }.to_command().is_err(), "read-only");
        // `operator add` from the CLI allows the account's exact host.
        let add = AdminCmd::Operator { action: OperatorAction::Add { account: "@Alice.Example.com".into(), note: None } };
        assert_eq!(add.to_command(), Ok(AdminCommand::OperatorAdd { input: "alice.example.com".into(), note: None, add_host: true }));
        let host = AdminCmd::Operator { action: OperatorAction::Host { action: HostAction::Add { pattern: "*.Host.Bsky.Network".into() } } };
        assert_eq!(host.to_command(), Ok(AdminCommand::OperatorHostAdd { pattern: "*.host.bsky.network".into() }));
        assert!(AdminCmd::Operator { action: OperatorAction::Host { action: HostAction::Add { pattern: "https://x".into() } } }.to_command().is_err());
        assert!(AdminCmd::Operator { action: OperatorAction::List }.to_command().is_err(), "read-only");
        assert!(AdminCmd::SetupCode.to_command().is_err(), "read-only");
        assert_eq!(AdminCmd::Restart.to_command(), Ok(AdminCommand::Restart));
        assert!(AdminCmd::Settings { action: SettingsAction::List }.to_command().is_err(), "read-only");
        assert!(AdminCmd::Settings { action: SettingsAction::Get { key: "threadlens.access".into() } }.to_command().is_err(), "read-only");
        let keep = AdminCmd::Refract { action: RefractAction::Unkeep { scope: "feature".into() } };
        assert_eq!(keep.to_command(), Ok(AdminCommand::RefractKeep { scope: "feature".into(), keep: false }));
        assert!(AdminCmd::Refract { action: RefractAction::Keep { scope: "nope".into() } }.to_command().is_err());
        assert!(AdminCmd::Refract { action: RefractAction::Status }.to_command().is_err(), "read-only");
    }
}
