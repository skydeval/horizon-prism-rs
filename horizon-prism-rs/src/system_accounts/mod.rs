//! System accounts (system-accounts design, `docs/internal/design/
//! system-accounts.md`): a private, per-account list of the other accounts
//! of a plural system, kept by this AppView the way an AppView keeps mutes.
//! Never a repo record, never in any view, open to every signed-in account.
//!
//! - Owner A's DID is stored plain; a listed account B's DID is never stored
//!   or bound to the database in clear: a keyed hash indexes it and the DID
//!   is sealed with a server key ([`keys`]), bound to A and the index.
//! - Only this module names its tables (`sysacct_`); the Dockerfile's source
//!   check enforces it. The firehose router, the purger and `serve` call the
//!   functions below.
//! - Nothing here logs a DID, an index, a token or a pair; database errors
//!   are logged by SQLSTATE only ([`store::StoreError`]).
//! - The feature is on when `APP_SYSTEM_ACCOUNTS_KEY_HEX` is set and is the
//!   key the stored lists were sealed with. With lists stored and the key
//!   missing or another one, it is **locked** (amendment 1): Prism runs, the
//!   methods answer 501, and account events are kept in `sysacct_pending`
//!   until the right key is back ([`catch_up_task`] then applies them).

pub mod handlers;
mod keys;
mod store;
mod tokens;
#[cfg(test)]
mod tests;

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use sqlx::{PgConnection, PgPool};

pub use keys::Keys;

use crate::config::Secret32;

/// `getMembership.features` while the feature is on, for every caller.
pub const FEATURE: &str = "account.systemAccounts";

pub const GET_SYSTEM_ACCOUNTS: &str = "app.nearhorizon.account.getSystemAccounts";
pub const ADD_SYSTEM_ACCOUNT: &str = "app.nearhorizon.account.addSystemAccount";
pub const REMOVE_SYSTEM_ACCOUNT: &str = "app.nearhorizon.account.removeSystemAccount";
pub const GET_SYSTEM_ACCOUNT_STATUS: &str = "app.nearhorizon.account.getSystemAccountStatus";
pub const LEAVE_SYSTEM_ACCOUNTS: &str = "app.nearhorizon.account.leaveSystemAccounts";
/// The `lxm` of the proof B's PDS mints. A proof-only name: no route.
pub const CONFIRM_SYSTEM_ACCOUNT: &str = "app.nearhorizon.account.confirmSystemAccount";

/// The five routed methods.
pub const METHODS: [&str; 5] = [
    GET_SYSTEM_ACCOUNTS,
    ADD_SYSTEM_ACCOUNT,
    REMOVE_SYSTEM_ACCOUNT,
    GET_SYSTEM_ACCOUNT_STATUS,
    LEAVE_SYSTEM_ACCOUNTS,
];

/// Entries per list (with the owner, 32 accounts).
pub const MAX_ENTRIES: i64 = 31;
/// The longest `exp − iat` of a sensitive token: 60 s and a 5 s margin.
pub const MAX_LIFETIME: Duration = Duration::from_secs(65);
/// A used token is remembered until `exp` plus the verifier's clock skew,
/// after which the verifier refuses it anyway.
pub const TOKEN_MEMORY_SKEW: Duration = Duration::from_secs(30);
/// The largest request body.
pub const MAX_BODY: usize = 8 * 1024;

/// Metric: requests, by `method` and `outcome`.
pub const METRIC_REQUESTS: &str = "system_accounts_requests_total";

/// Metric: the feature's state, 1 for the current `state` label.
pub const METRIC_STATE: &str = "system_accounts_state";

/// Why the feature is locked.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LockReason {
    /// No key, and lists are stored.
    KeyUnset,
    /// Another key than the one the stored lists were sealed with.
    KeyMismatch,
}

/// Whether the five methods serve, shared by every clone: false while the
/// catch-up has account events left (amendment 1 §6), true after.
#[derive(Debug, Clone, Default)]
pub struct Serving(Arc<AtomicBool>);

impl Serving {
    /// A fixed answer (tests, and `getMembership`'s settings by default).
    #[must_use]
    pub fn fixed(on: bool) -> Self {
        Self(Arc::new(AtomicBool::new(on)))
    }

    #[must_use]
    pub fn get(&self) -> bool {
        self.0.load(Ordering::Acquire)
    }

    fn set(&self) {
        self.0.store(true, Ordering::Release);
    }
}

/// The keys and whether the methods serve yet.
#[derive(Debug, Clone)]
pub struct Live {
    pub keys: Keys,
    pub serving: Serving,
}

/// The feature's state for this process (amendment 1 §4).
#[derive(Debug, Clone, Default)]
pub enum State {
    /// No key and nothing stored: the methods answer 501.
    #[default]
    Off,
    /// The right key. The router and the purger use the keys at once; the
    /// methods serve once [`Live::serving`] is set.
    On(Live),
    /// Lists stored, the key missing or another: the methods answer 501,
    /// nothing is sealed or opened, no entry is added or removed (a deleted
    /// owner's own list still goes), and account events are recorded for
    /// the catch-up.
    Locked(LockReason),
}

impl State {
    /// On with these keys and serving (tests and a start with nothing kept).
    #[must_use]
    pub fn serving(keys: Keys) -> Self {
        Self::On(Live { keys, serving: Serving::fixed(true) })
    }

    /// The keys, while on (serving or catching up).
    #[must_use]
    pub fn keys(&self) -> Option<&Keys> {
        match self {
            Self::On(l) => Some(&l.keys),
            _ => None,
        }
    }

    /// Whether the right key is set.
    #[must_use]
    pub fn is_on(&self) -> bool {
        matches!(self, Self::On(_))
    }

    /// Whether the five methods serve now.
    #[must_use]
    pub fn is_serving(&self) -> bool {
        matches!(self, Self::On(l) if l.serving.get())
    }

    /// The serving flag `getMembership` reads (false for good unless on).
    #[must_use]
    pub fn serving_flag(&self) -> Serving {
        match self {
            Self::On(l) => l.serving.clone(),
            _ => Serving::fixed(false),
        }
    }

    #[must_use]
    pub fn is_locked(&self) -> bool {
        matches!(self, Self::Locked(_))
    }

    /// The gauge's `state` label.
    #[must_use]
    pub fn label(&self) -> &'static str {
        match self {
            Self::Off => "off",
            Self::On(l) if l.serving.get() => "on",
            Self::On(_) => "catching_up",
            Self::Locked(LockReason::KeyUnset) => "locked_unset",
            Self::Locked(LockReason::KeyMismatch) => "locked_mismatch",
        }
    }
}

/// Every gauge label, so the Health check reads one series per state.
pub const STATE_LABELS: [&str; 5] = ["off", "on", "catching_up", "locked_unset", "locked_mismatch"];

/// Metric: account events kept for the catch-up, as of its last round.
pub const METRIC_PENDING: &str = "system_accounts_pending";

/// Set the state gauge: 1 for `state`, 0 for the others.
pub fn set_state_gauge(state: &State) {
    for label in STATE_LABELS {
        metrics::gauge!(METRIC_STATE, "state" => label).set(if label == state.label() { 1.0 } else { 0.0 });
    }
}

/// The operator's text for a locked state (log and docs).
#[must_use]
pub fn locked_text(reason: LockReason) -> String {
    let var = crate::config::ENV_SYSTEM_ACCOUNTS_KEY_HEX;
    let what = match reason {
        LockReason::KeyUnset => format!("{var} is not set, and system-account lists are stored"),
        LockReason::KeyMismatch => format!("{var} is not the key the stored system-account lists were sealed with"),
    };
    format!(
        "{what}. Prism is running; system accounts are off until the right key is set (or the sysacct_ tables are emptied, docs/configuration.md). Account deletions seen meanwhile are recorded and applied then."
    )
}

/// The startup check (design §4.3, amendment 1 §4): the feature's state.
/// With the right key and account events kept, the state is on but not
/// serving until [`catch_up_task`] has applied them.
///
/// # Errors
///
/// A database failure (by SQLSTATE). A missing or wrong key is a state,
/// not an error.
pub async fn at_startup(pool: &PgPool, secret: Option<&Secret32>) -> Result<State, String> {
    let (state, kept) = store::startup(pool, secret.map(|s| Keys::new(s.expose())))
        .await
        .map_err(|code| format!("checking the system-accounts key: database error {code}"))?;
    if let State::On(live) = &state {
        if !kept {
            live.serving.set();
        } else {
            tracing::info!(
                event = "system_accounts_catching_up",
                "account events were kept while system accounts were locked; the methods serve once they are applied"
            );
        }
    }
    if let State::Locked(reason) = &state {
        tracing::warn!(event = "system_accounts_locked", reason = state.label(), "{}", locked_text(*reason));
    }
    set_state_gauge(&state);
    Ok(state)
}

/// How often the catch-up takes a step while on.
pub const CATCH_UP_EVERY: Duration = Duration::from_secs(60);

/// The catch-up (amendment 1 §6), while on: a step at once, then every
/// [`CATCH_UP_EVERY`], for as long as the process runs, so rows another
/// (locked) replica keeps are applied too. The methods serve after the
/// first step that leaves no row and waits on no check. A failed step is
/// logged by SQLSTATE and tried again next time; it never stops Prism.
#[must_use]
pub fn catch_up_task(pool: PgPool, state: &State) -> Option<tokio::task::JoinHandle<()>> {
    let State::On(live) = state else {
        return None;
    };
    let live = live.clone();
    let state = state.clone();
    Some(tokio::spawn(async move {
        let mut catch_up = store::CatchUp::default();
        loop {
            match catch_up.step(&pool, &live.keys).await {
                Ok(step) => {
                    #[allow(clippy::cast_precision_loss)]
                    metrics::gauge!(METRIC_PENDING).set(step.left as f64);
                    if step.dropped + step.queued > 0 {
                        tracing::info!(
                            event = "system_accounts_caught_up",
                            dropped = step.dropped,
                            waiting = step.waiting,
                            left = step.left,
                            "applied account events kept while system accounts were locked"
                        );
                    }
                    if step.left == 0 && step.waiting == 0 && !live.serving.get() {
                        live.serving.set();
                        set_state_gauge(&state);
                        tracing::info!(event = "system_accounts_serving", "system accounts serve again");
                    }
                }
                Err(code) => tracing::warn!(event = "system_accounts_catch_up_failed", code, "catch-up step failed; trying again"),
            }
            tokio::time::sleep(CATCH_UP_EVERY).await;
        }
    }))
}

/// Whether `did` owns a list (checked with or without the key: a list row
/// can outlive an enabled period) or, with the key, is on one: the firehose
/// router then queues its identity work (design §6).
///
/// # Errors
///
/// Database errors.
pub async fn holds(pool: &PgPool, state: &State, did: &str) -> Result<bool, sqlx::Error> {
    store::holds(pool, did, state.keys().map(|k| k.index(did))).await
}

/// An identity, account, handle or tombstone event for `did`: kept for the catch-up while
/// locked (amendment 1 §5); nothing otherwise.
///
/// # Errors
///
/// Database errors.
pub async fn note_account_event(pool: &PgPool, state: &State, did: &str) -> Result<(), sqlx::Error> {
    if state.is_locked() {
        store::note_pending(&mut *pool.acquire().await?, did, false).await?;
    }
    Ok(())
}

/// Forget `did` as an owner and as a listed account, in the purger's
/// `account_deleted` marker transaction (design §6). Locked, the owner's
/// list goes and the DID is kept, confirmed, for the catch-up.
///
/// # Errors
///
/// Database errors.
pub async fn forget_account(conn: &mut PgConnection, did: &str, state: &State) -> Result<(), sqlx::Error> {
    store::forget(conn, did, state.keys().map(|k| k.index(did))).await?;
    if state.is_locked() {
        store::note_pending(conn, did, true).await?;
    }
    Ok(())
}
