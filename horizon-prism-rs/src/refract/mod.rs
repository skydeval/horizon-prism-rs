//! Refract (refract design, `docs/internal/refract/refract.md`): accounts
//! Bluesky's AppView withholds stay usable while a community the operator
//! lists still hosts them and has not banned them.
//!
//! - [`snapshot`]: what the moderation gate hides for Refract, and the
//!   per-request snapshot of it (§4.2, §7.4).
//! - [`communities`]: the communities list and its entry hash (§3).
//! - [`state`]: `refract_dids` states and the in-memory sets (§4.2).
//! - [`checker`]: the five rules and the transitions (§4.1, §4.3, §4.4).
//! - [`labels`], [`poll`]: community labels (§4.5).
//! - [`config_task`]: live changes, pending scopes and their delay (§3.2,
//!   §4.6).
//! - [`purge`]: the Refract purge (§6.2).
//! - [`net`]: the outbound calls.

pub mod checker;
pub mod communities;
pub mod config_task;
pub mod handlers;
pub mod labels;
pub mod net;
pub mod poll;
pub mod purge;
pub mod serve_check;
pub mod snapshot;
pub mod state;
#[cfg(test)]
mod tests;

use std::sync::Arc;

use sqlx::{PgConnection, PgPool};
use tokio::sync::Notify;
use tokio::task::JoinHandle;

pub use communities::Communities;
pub use purge::forget_account;
pub use snapshot::{Captured, CollectionRule, HideReasons, RefractCheck, RefractHidden, RefractSnapshot, RefractView};
pub use state::State;

use crate::admin::settings::LiveSettings;
use crate::aggregations::ScopeState;
use crate::config::RefractConfig;
use crate::identity::IdentityResolver;

/// `refract_label_reads_total{community, source, outcome}`.
pub const METRIC_LABEL_READS: &str = "refract_label_reads_total";
/// `refract_dids{state}`.
pub const METRIC_DIDS: &str = "refract_dids";

/// The Refract service: its pool, sets, settings and clients. Cheap to
/// clone.
#[derive(Clone)]
pub struct Refract {
    inner: Arc<Inner>,
}

struct Inner {
    pool: PgPool,
    scope: ScopeState,
    live: LiveSettings,
    /// The keys that take effect at restart.
    cfg: RefractConfig,
    net: net::RefractNet,
    /// Internal, directory only: rule 2's fresh resolve.
    resolver: IdentityResolver,
    labelers: labels::Labelers,
    /// Wakes the checker when something is made due.
    due: Notify,
}

impl Refract {
    #[must_use]
    pub fn new(pool: PgPool, scope: ScopeState, live: LiveSettings, cfg: RefractConfig, net: net::RefractNet, resolver: &IdentityResolver) -> Self {
        let resolver = resolver.with_intent(crate::net::Intent::Internal).directory_only();
        Self {
            inner: Arc::new(Inner {
                pool,
                scope,
                live,
                cfg,
                net,
                labelers: labels::Labelers::new(resolver.clone()),
                resolver,
                due: Notify::new(),
            }),
        }
    }

    #[must_use]
    pub fn pool(&self) -> &PgPool {
        &self.inner.pool
    }

    #[must_use]
    pub fn scope(&self) -> &ScopeState {
        &self.inner.scope
    }

    #[must_use]
    pub fn cfg(&self) -> &RefractConfig {
        &self.inner.cfg
    }

    #[must_use]
    pub fn net(&self) -> &net::RefractNet {
        &self.inner.net
    }

    /// The identity resolver Refract uses (internal, the directory).
    #[must_use]
    pub fn resolver(&self) -> &IdentityResolver {
        &self.inner.resolver
    }

    /// Insert `did`, whose PDS is on `host`, as a candidate due now, or
    /// make a `rejected` row a candidate again (§4.4). Refused (`false`)
    /// when no enabled community hosts it or a purge of it runs; any other
    /// existing row is left alone (`true`: it is known).
    ///
    /// # Errors
    ///
    /// Database errors.
    pub async fn insert_candidate(&self, did: &str, host: &str, source: &str) -> Result<bool, sqlx::Error> {
        let communities = self.communities();
        let Some(home) = communities.home(host) else {
            return Ok(false);
        };
        let mut sets = self.inner.scope.write_guard().await;
        let mut tx = self.inner.pool.begin().await?;
        let purging: bool = sqlx::query_scalar("SELECT EXISTS (SELECT 1 FROM purging_dids WHERE did = $1)").bind(did).fetch_one(&mut *tx).await?;
        if purging {
            tx.rollback().await?;
            return Ok(false);
        }
        let from: Option<Option<String>> = sqlx::query_scalar(
            "WITH old AS (SELECT state FROM refract_dids WHERE did = $1)              INSERT INTO refract_dids (did, community, pds_host, state, source, candidate_since, next_check_at)              VALUES ($1, $2, $3, 'candidate', $4, now(), now())              ON CONFLICT (did) DO UPDATE SET state = 'candidate', community = EXCLUDED.community, pds_host = EXCLUDED.pds_host,                source = EXCLUDED.source, candidate_since = now(), next_check_at = now(), attempts = 0,                version = refract_dids.version + 1, updated_at = now()              WHERE refract_dids.state = 'rejected'              RETURNING (SELECT state FROM old)",
        )
        .bind(did)
        .bind(&home.name)
        .bind(host)
        .bind(source)
        .fetch_optional(&mut *tx)
        .await?;
        let Some(from) = from else {
            tx.rollback().await?;
            return Ok(true);
        };
        sqlx::query("INSERT INTO refract_events (did, from_state, to_state, reason) VALUES ($1, $2, 'candidate', $3)")
            .bind(did)
            .bind(&from)
            .bind(source)
            .execute(&mut *tx)
            .await?;
        tx.commit().await?;
        state::refresh(&self.inner.pool, &mut sets, Some(&[did.to_string()]), &communities).await?;
        drop(sets);
        self.inner.due.notify_one();
        Ok(true)
    }

    /// `refract.enabled`, read live.
    #[must_use]
    pub fn enabled(&self) -> bool {
        self.inner.live.load().refract_enabled
    }

    /// `refract.communities`, read live.
    #[must_use]
    pub fn communities(&self) -> Communities {
        Communities::new(self.inner.live.load().refract_communities.clone())
    }

    /// The startup sequence (§4.2): the config task's clearing commits
    /// first, then the in-memory sets are loaded (the config task loads
    /// them itself, under the write guard, after its commit).
    ///
    /// # Errors
    ///
    /// Database errors.
    pub async fn startup(&self) -> Result<(), sqlx::Error> {
        self.apply_config().await?;
        Ok(())
    }

    /// Spawn the config task, the checker, the label poll and the sweep.
    /// The pending delay is spawned by [`Refract::spawn_delay`] once `serve`
    /// is up.
    #[must_use]
    pub fn spawn(&self) -> Vec<JoinHandle<()>> {
        vec![
            tokio::spawn(self.clone().run_config_task()),
            tokio::spawn(self.clone().run_checker()),
            tokio::spawn(self.clone().run_poller()),
            tokio::spawn(self.clone().run_sweep()),
            tokio::spawn(self.clone().run_purger()),
            tokio::spawn(self.clone().run_gauges()),
        ]
    }

    /// The pending delay and housekeeping (§4.6): never before `serve` is
    /// up.
    #[must_use]
    pub fn spawn_delay(&self) -> JoinHandle<()> {
        tokio::spawn(self.clone().run_delay_task())
    }

    async fn run_gauges(self) {
        loop {
            match sqlx::query_as::<_, (String, i64)>("SELECT state, count(*) FROM refract_dids GROUP BY state").fetch_all(&self.inner.pool).await {
                Ok(rows) => {
                    for s in [State::Candidate, State::Rejected, State::Restored, State::Grace, State::Ending] {
                        let n = rows.iter().find(|(st, _)| st == s.as_str()).map_or(0, |(_, n)| *n);
                        #[allow(clippy::cast_precision_loss)]
                        metrics::gauge!(METRIC_DIDS, "state" => s.as_str()).set(n as f64);
                    }
                }
                Err(error) => tracing::debug!(event = "refract_gauge_failed", %error, "sampling refract_dids failed"),
            }
            tokio::time::sleep(std::time::Duration::from_secs(60)).await;
        }
    }

    /// Make `dids` due now (§4.3 "Due now"): rows in `candidate`,
    /// `restored` or `grace` only.
    ///
    /// # Errors
    ///
    /// Database errors.
    pub async fn make_due(&self, dids: &[String]) -> Result<u64, sqlx::Error> {
        let n = make_due(&self.inner.pool, dids).await?;
        if n > 0 {
            self.inner.due.notify_one();
        }
        Ok(n)
    }

    /// `→ ending` from `restored` or `grace` (§4.3; the pending delay,
    /// `admin refract end`). Returns whether it moved.
    ///
    /// # Errors
    ///
    /// Database errors.
    pub async fn end(&self, did: &str, reason: &str) -> Result<bool, sqlx::Error> {
        let communities = self.communities();
        let mut sets = self.inner.scope.write_guard().await;
        let mut tx = self.inner.pool.begin().await?;
        let from: Option<String> = sqlx::query_scalar(
            "UPDATE refract_dids d SET state = 'ending', version = d.version + 1, ended_reason = $2, updated_at = now() \
             FROM (SELECT did, state FROM refract_dids WHERE did = $1 AND state IN ('restored', 'grace') FOR UPDATE) old \
             WHERE d.did = old.did RETURNING old.state",
        )
        .bind(did)
        .bind(reason)
        .fetch_optional(&mut *tx)
        .await?;
        let Some(from) = from.and_then(|s| State::parse(&s)) else {
            tx.rollback().await?;
            return Ok(false);
        };
        checker::transition_effects(&mut tx, did, from, State::Ending, Some(reason)).await?;
        tx.commit().await?;
        metrics::counter!(checker::METRIC_TRANSITIONS, "from" => from.as_str(), "to" => "ending", "reason" => reason.to_string()).increment(1);
        tracing::info!(event = "refract_transition", did, from = from.as_str(), to = "ending", reason, "Refract state changed");
        state::refresh(&self.inner.pool, &mut sets, Some(&[did.to_string()]), &communities).await?;
        Ok(true)
    }
}

/// The restored marker of a community (refract design §7.3).
#[must_use]
pub fn marker(c: &crate::config::RefractCommunity) -> serde_json::Value {
    let mut m = serde_json::json!({"community": c.name, "tier": c.tier});
    if let Some(l) = &c.labeler_did {
        m["labeler"] = serde_json::json!(l);
    }
    m
}

/// Make `dids` due now (§4.3 "Due now").
///
/// # Errors
///
/// Database errors.
pub async fn make_due(pool: &PgPool, dids: &[String]) -> Result<u64, sqlx::Error> {
    Ok(sqlx::query(
        "UPDATE refract_dids SET due_seq = due_seq + 1, next_check_at = now(), updated_at = now() \
         WHERE did = ANY($1) AND state IN ('candidate', 'restored', 'grace')",
    )
    .bind(dids)
    .execute(pool)
    .await?
    .rows_affected())
}

/// The refresher's hook (§4.4): an `#account`/`#identity` event for a DID
/// with a Refract row makes it due. Returns `true` when the DID's only tie
/// is a `candidate` or `rejected` row (no `actors`, `scope_dids` or
/// `members` row): the refresher then writes nothing else (D-2).
///
/// # Errors
///
/// Database errors.
pub async fn make_due_on_event(pool: &PgPool, did: &str) -> Result<bool, sqlx::Error> {
    let row: Option<(String, bool)> = sqlx::query_as(
        "SELECT state, (EXISTS (SELECT 1 FROM actors WHERE did = $1) OR EXISTS (SELECT 1 FROM scope_dids WHERE did = $1) \
           OR EXISTS (SELECT 1 FROM members WHERE did = $1)) FROM refract_dids WHERE did = $1",
    )
    .bind(did)
    .fetch_optional(pool)
    .await?;
    let Some((state, other_tie)) = row else {
        return Ok(false);
    };
    make_due(pool, &[did.to_string()]).await?;
    Ok(!other_tie && matches!(State::parse(&state), Some(State::Candidate | State::Rejected)))
}

/// Enqueue the `refract` backfill of `did` (§4.3 step 3, §4.6 clearing).
///
/// # Errors
///
/// Database errors.
pub async fn enqueue_backfill(conn: &mut PgConnection, did: &str) -> Result<(), sqlx::Error> {
    crate::firehose::jobs::enqueue(conn, did, crate::firehose::jobs::Depth::Refract).await
}
