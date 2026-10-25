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
//! - The feature is on exactly when `APP_SYSTEM_ACCOUNTS_KEY_HEX` is set.

pub mod handlers;
mod keys;
mod store;
mod tokens;
#[cfg(test)]
mod tests;

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

/// The startup check (design §4.3): the keys when the feature is on.
///
/// # Errors
///
/// A message for the operator when the configured key is not the one the
/// stored lists were sealed with, or when no key is set and lists are
/// stored; or a database failure (by SQLSTATE).
pub async fn at_startup(pool: &PgPool, secret: Option<&Secret32>) -> Result<Option<Keys>, String> {
    store::check_key(pool, secret.map(|s| Keys::new(s.expose())))
        .await
        .map_err(|e| match e {
            store::KeyCheck::Unset => format!(
                "{} is not set, and system-account lists are stored: set the key they were sealed with, or empty the sysacct_ tables (docs/configuration.md)",
                crate::config::ENV_SYSTEM_ACCOUNTS_KEY_HEX
            ),
            store::KeyCheck::Mismatch => format!(
                "{} is not the key the stored system-account lists were sealed with: set that key, or empty the sysacct_ tables (docs/configuration.md)",
                crate::config::ENV_SYSTEM_ACCOUNTS_KEY_HEX
            ),
            store::KeyCheck::Db(code) => format!("checking the system-accounts key: database error {code}"),
        })
}

/// Whether `did` owns a list (checked with or without the key: a list row
/// can outlive an enabled period) or, with the key, is on one: the firehose
/// router then queues its identity work (design §6).
///
/// # Errors
///
/// Database errors.
pub async fn holds(pool: &PgPool, keys: Option<&Keys>, did: &str) -> Result<bool, sqlx::Error> {
    store::holds(pool, did, keys.map(|k| k.index(did))).await
}

/// Forget `did` as an owner and as a listed account, in the purger's
/// `account_deleted` marker transaction (design §6).
///
/// # Errors
///
/// Database errors.
pub async fn forget_account(conn: &mut PgConnection, did: &str, keys: Option<&Keys>) -> Result<(), sqlx::Error> {
    store::forget(conn, did, keys.map(|k| k.index(did))).await
}
