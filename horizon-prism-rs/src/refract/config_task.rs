//! Live changes of `refract.enabled` and `refract.communities` (refract
//! design §3.2, §4.6), and the pending-removal delay.
//!
//! One task runs at startup and after every live-settings swap. It reads
//! the latest values and diffs them against the entries stored in
//! `worker_state` (`refract_entries`), so any number of saves come out the
//! same as applying them one by one. Each run is one transaction under the
//! write guard; memory is set from the rows after the commit.

use std::collections::{BTreeMap, HashSet};

use chrono::{DateTime, TimeDelta, Utc};
use serde_json::{Value, json};
use sqlx::PgConnection;

use super::Refract;
use super::communities::{Communities, entry_hash, same_label_reader};
use super::state::ABSENT;
use crate::config::RefractCommunity;
use crate::constellation::matcher::host_matches;

/// `worker_state` key of the entries the last run applied.
pub const ENTRIES_KEY: &str = "refract_entries";
/// The feature scope's name (§4.6).
pub const FEATURE_SCOPE: &str = "feature";
/// `refract_pending_end{scope}`: when each pending scope ends, as a unix
/// time (0 while kept).
pub const METRIC_PENDING_END: &str = "refract_pending_end";
/// How often the pending delay and the housekeeping look.
pub const DELAY_POLL: std::time::Duration = std::time::Duration::from_secs(60);

/// An entry's scope name: keyed by its pre-change hash.
#[must_use]
pub fn entry_scope(hash: &str) -> String {
    format!("entry:{hash}")
}

/// What one run changed.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Applied {
    pub renamed: Vec<(String, String)>,
    pub changed: Vec<String>,
    pub unlisted: usize,
    pub cleared: usize,
    pub candidates_dropped: u64,
}

async fn stored_entries(conn: &mut PgConnection) -> Result<BTreeMap<String, RefractCommunity>, sqlx::Error> {
    let v: Option<Value> = sqlx::query_scalar("SELECT value FROM worker_state WHERE key = $1").bind(ENTRIES_KEY).fetch_optional(&mut *conn).await?;
    Ok(v.and_then(|v| serde_json::from_value(v).ok()).unwrap_or_default())
}

fn matches_any(host: Option<&str>, c: &RefractCommunity) -> bool {
    host.is_some_and(|h| c.pds_patterns.iter().any(|p| host_matches(h, p)))
}

impl Refract {
    /// Run the config task until aborted: once now, then after every
    /// live-settings swap, coalesced.
    pub async fn run_config_task(self) {
        let mut rx = self.inner.live.subscribe();
        loop {
            let seen = *rx.borrow_and_update();
            if let Err(error) = self.apply_config().await {
                tracing::error!(event = "refract_config_failed", %error, "applying the Refract settings failed; retried in a minute");
                tokio::select! {
                    _ = rx.changed() => {}
                    () = tokio::time::sleep(DELAY_POLL) => {}
                }
                continue;
            }
            // A save during the pass is applied by the next one.
            if *rx.borrow() != seen {
                continue;
            }
            if rx.changed().await.is_err() {
                return;
            }
        }
    }

    /// Diff the live values against the stored entries and apply the
    /// difference (§3.2, §4.6).
    ///
    /// # Errors
    ///
    /// Database errors; nothing is changed on error.
    pub async fn apply_config(&self) -> Result<Applied, sqlx::Error> {
        let live = self.inner.live.load();
        let enabled = live.refract_enabled;
        let communities = Communities::new(live.refract_communities.clone());
        let mut out = Applied::default();
        let mut sets = self.inner.scope.write_guard().await;
        let mut tx = self.inner.pool.begin().await?;
        let stored = stored_entries(&mut tx).await?;
        let current: BTreeMap<String, RefractCommunity> = communities.all().iter().map(|c| (c.name.clone(), c.clone())).collect();
        let mut touched: HashSet<String> = HashSet::new();
        // Renames: an entry whose hash a vanished name had.
        let mut renamed_from: HashSet<String> = HashSet::new();
        for (name, c) in &current {
            if stored.contains_key(name) {
                continue;
            }
            let h = entry_hash(c);
            if let Some((old, _)) = stored.iter().find(|(old, e)| !current.contains_key(*old) && !renamed_from.contains(*old) && entry_hash(e) == h) {
                let dids: Vec<String> = sqlx::query_scalar("UPDATE refract_dids SET community = $2, version = version + 1, updated_at = now() WHERE community = $1 RETURNING did")
                    .bind(old)
                    .bind(name)
                    .fetch_all(&mut *tx)
                    .await?;
                touched.extend(dids);
                renamed_from.insert(old.clone());
                out.renamed.push((old.clone(), name.clone()));
            }
        }
        // New or changed entries: their DIDs, by the old or new patterns,
        // are due now; a changed label reader drops the old rows.
        for (name, c) in &current {
            let old = stored.get(name);
            if out.renamed.iter().any(|(_, n)| n == name) || old.is_some_and(|o| entry_hash(o) == entry_hash(c)) {
                continue;
            }
            if let Some(o) = old
                && !same_label_reader(o, c)
            {
                let dids: Vec<String> = sqlx::query_scalar("SELECT did FROM refract_dids WHERE community = $1").bind(name).fetch_all(&mut *tx).await?;
                super::labels::forget(&mut tx, &dids).await?;
            }
            if let Some(l) = c.labeler_did.as_deref() {
                sqlx::query("UPDATE refract_labels SET gone = (val = ANY($2)) WHERE labeler_did = $1 AND val <> $3")
                    .bind(l)
                    .bind(&c.gone_labels)
                    .bind(ABSENT)
                    .execute(&mut *tx)
                    .await?;
            }
            let rows: Vec<(String, Option<String>, String)> = sqlx::query_as("SELECT did, pds_host, community FROM refract_dids WHERE state IN ('candidate', 'restored', 'grace')")
                .fetch_all(&mut *tx)
                .await?;
            // A DID this change unlists is not made due (it goes pending
            // below).
            let due: Vec<String> = rows
                .into_iter()
                .filter(|(_, host, community)| community == name || matches_any(host.as_deref(), c) || old.is_some_and(|o| matches_any(host.as_deref(), o)))
                .filter(|(_, host, _)| host.as_deref().and_then(|h| communities.home(h)).is_some())
                .map(|(did, _, _)| did)
                .collect();
            sqlx::query("UPDATE refract_dids SET due_seq = due_seq + 1, version = version + 1, next_check_at = now(), updated_at = now() WHERE did = ANY($1)")
                .bind(&due)
                .execute(&mut *tx)
                .await?;
            touched.extend(due);
            out.changed.push(name.clone());
        }
        let in_scope_before: HashSet<String> = sqlx::query_scalar("SELECT DISTINCT did FROM refract_pending_dids").fetch_all(&mut *tx).await?.into_iter().collect();
        // The feature scope (§4.6).
        if enabled {
            sqlx::query("DELETE FROM refract_pending WHERE scope = $1").bind(FEATURE_SCOPE).execute(&mut *tx).await?;
        } else {
            sqlx::query("INSERT INTO refract_pending (scope) VALUES ($1) ON CONFLICT (scope) DO NOTHING").bind(FEATURE_SCOPE).execute(&mut *tx).await?;
            sqlx::query(
                "INSERT INTO refract_pending_dids (scope, did) SELECT $1, did FROM refract_dids WHERE state IN ('restored', 'grace') \
                 ON CONFLICT DO NOTHING",
            )
            .bind(FEATURE_SCOPE)
            .execute(&mut *tx)
            .await?;
            // Candidates hold no content (D-2): they go at once.
            out.candidates_dropped += drop_candidates(&mut tx, None).await?;
        }
        // Unlisted DIDs: in R, their stored host matches no enabled entry,
        // and not already pending in an entry scope (whose clock runs).
        let r_rows: Vec<(String, Option<String>, String)> = sqlx::query_as(
            "SELECT d.did, d.pds_host, d.community FROM refract_dids d WHERE d.state IN ('restored', 'grace') \
             AND NOT EXISTS (SELECT 1 FROM refract_pending_dids p WHERE p.did = d.did AND p.scope <> $1)",
        )
        .bind(FEATURE_SCOPE)
        .fetch_all(&mut *tx)
        .await?;
        for (did, host, community) in &r_rows {
            if host.as_deref().and_then(|h| communities.home(h)).is_some() {
                continue;
            }
            // Keyed by the entry's hash before this change.
            let hash = stored.get(community).or_else(|| current.get(community)).map_or_else(|| "unknown".to_string(), entry_hash);
            let scope = entry_scope(&hash);
            sqlx::query("INSERT INTO refract_pending (scope) VALUES ($1) ON CONFLICT (scope) DO NOTHING").bind(&scope).execute(&mut *tx).await?;
            let added = sqlx::query("INSERT INTO refract_pending_dids (scope, did) VALUES ($1, $2) ON CONFLICT DO NOTHING")
                .bind(&scope)
                .bind(did)
                .execute(&mut *tx)
                .await?
                .rows_affected();
            if added > 0 {
                out.unlisted += 1;
                touched.insert(did.clone());
            }
        }
        // Candidates whose host no longer matches an enabled entry.
        let c_rows: Vec<(String, Option<String>)> = sqlx::query_as("SELECT did, pds_host FROM refract_dids WHERE state IN ('candidate', 'rejected')").fetch_all(&mut *tx).await?;
        let gone: Vec<String> = c_rows.into_iter().filter(|(_, h)| h.as_deref().and_then(|h| communities.home(h)).is_none()).map(|(d, _)| d).collect();
        if !gone.is_empty() {
            out.candidates_dropped += drop_candidates(&mut tx, Some(&gone)).await?;
            touched.extend(gone);
        }
        // Clearing: an entry scope's DID whose host matches again leaves it.
        let scoped: Vec<(String, String, Option<String>)> = sqlx::query_as(
            "SELECT p.scope, p.did, d.pds_host FROM refract_pending_dids p JOIN refract_dids d ON d.did = p.did WHERE p.scope <> $1",
        )
        .bind(FEATURE_SCOPE)
        .fetch_all(&mut *tx)
        .await?;
        for (scope, did, host) in scoped {
            if host.as_deref().and_then(|h| communities.home(h)).is_some() {
                sqlx::query("DELETE FROM refract_pending_dids WHERE scope = $1 AND did = $2").bind(&scope).bind(&did).execute(&mut *tx).await?;
            }
        }
        sqlx::query("DELETE FROM refract_pending p WHERE NOT EXISTS (SELECT 1 FROM refract_pending_dids d WHERE d.scope = p.scope) AND p.scope <> $1")
            .bind(FEATURE_SCOPE)
            .execute(&mut *tx)
            .await?;
        let in_scope_after: HashSet<String> = sqlx::query_scalar("SELECT DISTINCT did FROM refract_pending_dids").fetch_all(&mut *tx).await?.into_iter().collect();
        let cleared: Vec<String> = in_scope_before.difference(&in_scope_after).cloned().collect();
        out.cleared = clear(&mut tx, &cleared, &communities).await?;
        touched.extend(cleared);
        let touched: Vec<String> = touched.into_iter().collect();
        sqlx::query("UPDATE refract_dids SET version = version + 1 WHERE did = ANY($1)").bind(&touched).execute(&mut *tx).await?;
        sqlx::query("INSERT INTO worker_state (key, value, updated_at) VALUES ($1, $2, now()) ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value, updated_at = now()")
            .bind(ENTRIES_KEY)
            .bind(json!(current))
            .execute(&mut *tx)
            .await?;
        tx.commit().await?;
        super::state::refresh(&self.inner.pool, &mut sets, None, &communities).await?;
        drop(sets);
        if out != Applied::default() {
            tracing::info!(event = "refract_config_applied", ?out, enabled, "Refract settings applied");
        }
        self.inner.due.notify_one();
        Ok(out)
    }

    /// End every pending DID whose covering scopes are all past the delay
    /// and none kept (§4.6), at most `checker_concurrency` at a time; then
    /// the housekeeping. Never before `serve` is up: `serve` spawns it last.
    pub async fn run_delay_task(self) {
        loop {
            if let Err(error) = self.end_pending().await {
                tracing::warn!(event = "refract_pending_end_failed", %error, "ending pending Refract DIDs failed");
            }
            if let Err(error) = self.housekeeping().await {
                tracing::warn!(event = "refract_housekeeping_failed", %error, "Refract housekeeping failed");
            }
            tokio::time::sleep(DELAY_POLL).await;
        }
    }

    /// One pass of the pending delay. Returns how many DIDs it ended.
    ///
    /// # Errors
    ///
    /// Database errors.
    pub async fn end_pending(&self) -> Result<usize, sqlx::Error> {
        let delay = TimeDelta::hours(i64::from(self.inner.cfg.disable_delay_hours));
        let scopes: Vec<(String, DateTime<Utc>, bool)> = sqlx::query_as("SELECT scope, first_seen, keep FROM refract_pending").fetch_all(&self.inner.pool).await?;
        for (scope, first, keep) in &scopes {
            #[allow(clippy::cast_precision_loss)]
            let end = if *keep { 0.0 } else { (*first + delay).timestamp() as f64 };
            metrics::gauge!(METRIC_PENDING_END, "scope" => scope.clone()).set(end);
        }
        let due: Vec<(String, bool)> = sqlx::query_as(
            "SELECT pd.did, bool_or(p.scope = $2) FROM refract_pending_dids pd JOIN refract_pending p ON p.scope = pd.scope \
             JOIN refract_dids d ON d.did = pd.did AND d.state IN ('restored', 'grace') \
             GROUP BY pd.did HAVING NOT bool_or(p.keep) AND max(p.first_seen) + make_interval(secs => $1) <= now() \
             LIMIT $3",
        )
        .bind(delay.num_seconds() as f64)
        .bind(FEATURE_SCOPE)
        .bind(i64::from(self.inner.cfg.checker_concurrency.max(1)))
        .fetch_all(&self.inner.pool)
        .await?;
        let mut n = 0;
        for (did, feature) in due {
            if self.end(&did, if feature { "feature_disabled" } else { "unlisted" }).await? {
                n += 1;
            }
        }
        Ok(n)
    }

    /// Delete rejected rows past their keep and events past
    /// `refract.event_days`.
    ///
    /// # Errors
    ///
    /// Database errors.
    pub async fn housekeeping(&self) -> Result<(), sqlx::Error> {
        // The weekly resync (refract design §6.1): the same depth again, to
        // repair relay gaps; a queued or running one is left alone.
        let due: Vec<String> = sqlx::query_scalar(
            "SELECT d.did FROM refract_dids d WHERE d.state IN ('restored', 'grace') AND d.resync_at <= now() \
             AND NOT EXISTS (SELECT 1 FROM refract_pending_dids p WHERE p.did = d.did) ORDER BY d.resync_at LIMIT 100",
        )
        .fetch_all(&self.inner.pool)
        .await?;
        for did in due {
            let mut tx = self.inner.pool.begin().await?;
            crate::firehose::jobs::enqueue_if_idle(&mut tx, &did, crate::firehose::jobs::Depth::Refract, Utc::now()).await?;
            sqlx::query("UPDATE refract_dids SET resync_at = now() + make_interval(days => $2) WHERE did = $1")
                .bind(&did)
                .bind(i32::try_from(self.inner.cfg.resync_days).unwrap_or(7))
                .execute(&mut *tx)
                .await?;
            tx.commit().await?;
        }
        let rejected: Vec<String> = sqlx::query_scalar("DELETE FROM refract_dids WHERE state = 'rejected' AND updated_at < now() - make_interval(secs => $1) RETURNING did")
            .bind(super::checker::REJECTED_KEEP.num_seconds() as f64)
            .fetch_all(&self.inner.pool)
            .await?;
        if !rejected.is_empty() {
            let mut conn = self.inner.pool.acquire().await?;
            super::labels::forget(&mut conn, &rejected).await?;
        }
        sqlx::query("DELETE FROM refract_events WHERE at < now() - make_interval(days => $1)")
            .bind(i32::try_from(self.inner.cfg.event_days).unwrap_or(365))
            .execute(&self.inner.pool)
            .await?;
        Ok(())
    }

    /// `admin refract keep` / `unkeep` (§4.6): `unkeep` restarts the clock.
    /// Returns whether the scope exists.
    ///
    /// # Errors
    ///
    /// Database errors.
    pub async fn keep(pool: &sqlx::PgPool, scope: &str, keep: bool) -> Result<bool, sqlx::Error> {
        let n = if keep {
            sqlx::query("UPDATE refract_pending SET keep = true WHERE scope = $1").bind(scope).execute(pool).await?
        } else {
            sqlx::query("UPDATE refract_pending SET keep = false, first_seen = now() WHERE scope = $1").bind(scope).execute(pool).await?
        };
        Ok(n.rows_affected() > 0)
    }
}

/// Delete candidates (all, or `dids`) and rejected rows, with their labels.
async fn drop_candidates(conn: &mut PgConnection, dids: Option<&[String]>) -> Result<u64, sqlx::Error> {
    let gone: Vec<String> = sqlx::query_scalar("DELETE FROM refract_dids WHERE state IN ('candidate', 'rejected') AND ($1::text[] IS NULL OR did = ANY($1)) RETURNING did")
        .bind(dids)
        .fetch_all(&mut *conn)
        .await?;
    super::labels::forget(conn, &gone).await?;
    Ok(gone.len() as u64)
}

/// Clearing (§4.6): DIDs no scope lists any more, still in R, wait for
/// their targeted label fetch (or, with no labeler, are done at once) and
/// are due now. Returns how many.
async fn clear(conn: &mut PgConnection, dids: &[String], communities: &Communities) -> Result<usize, sqlx::Error> {
    let rows: Vec<(String, String)> = sqlx::query_as("SELECT did, community FROM refract_dids WHERE did = ANY($1) AND state IN ('restored', 'grace')")
        .bind(dids)
        .fetch_all(&mut *conn)
        .await?;
    for (did, community) in &rows {
        if communities.labeler_of(community).is_some() {
            sqlx::query("UPDATE refract_dids SET awaiting_labels_since = now() WHERE did = $1").bind(did).execute(&mut *conn).await?;
        } else {
            super::enqueue_backfill(conn, did).await?;
        }
        sqlx::query("UPDATE refract_dids SET due_seq = due_seq + 1, next_check_at = now() WHERE did = $1").bind(did).execute(&mut *conn).await?;
    }
    Ok(rows.len())
}
