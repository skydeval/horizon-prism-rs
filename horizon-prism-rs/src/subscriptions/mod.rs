//! Rule 4: subscribed lists (design-v3 §4.3).
//!
//! A member's own list mute or list block only does anything if this
//! AppView knows the list's items, and a moderation list is usually owned
//! by someone it does not index. So it keeps a private copy of the lists
//! its members reference, in tables no public read path reads, and uses it
//! only to apply those members' own mutes and blocks.
//!
//! Built so far: the member's side ([`references`]), the list phase and
//! the release of an unreferenced list ([`list_phase`], [`applier`]), the
//! job row's enqueue rules ([`jobs`]), the claim ([`claim`]) and the run
//! itself ([`run`], [`sync`], [`fetch`], [`pds`]). Nothing starts them
//! yet. The copy rule and the views that apply a reference are not built,
//! so a stored reference has no effect on any view.

use std::future::Future;
use std::time::Duration;

/// A transaction that loses a deadlock or a serialization conflict runs
/// again this many more times before its error is returned.
const RETRIES: u32 = 5;
pub const METRIC_RETRIES: &str = "list_subscription_tx_retries_total";

/// A deadlock or a serialization failure: the transaction lost to another
/// and can be run again as it is.
#[must_use]
pub fn retryable(error: &sqlx::Error) -> bool {
    matches!(error, sqlx::Error::Database(db) if matches!(db.code().as_deref(), Some("40P01" | "40001")))
}

/// Run one whole rule-4 transaction, again in place when it loses a
/// deadlock (design-v3 §4.3 "Lock order": such a failure is retried in
/// place and never reaches a client or fails a job).
///
/// # Errors
///
/// The transaction's own error, once it is not a lost deadlock or the
/// retries are spent.
pub async fn retrying<T, E, F, Fut>(retry: impl Fn(&E) -> bool, mut transaction: F) -> Result<T, E>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T, E>>,
{
    let mut attempt = 0;
    loop {
        match transaction().await {
            Err(error) if retry(&error) && attempt < RETRIES => {
                attempt += 1;
                metrics::counter!(METRIC_RETRIES).increment(1);
                tokio::time::sleep(Duration::from_millis(50 * u64::from(attempt))).await;
            }
            other => return other,
        }
    }
}

pub mod applier;
pub mod claim;
#[cfg(test)]
mod concurrency_tests;
pub mod copy;
pub mod fetch;
pub mod gauges;
pub mod jobs;
pub mod list_phase;
pub mod pds;
pub mod references;
pub mod run;
pub mod runner;
pub mod sweep;
pub mod sync;
pub mod triggers;
