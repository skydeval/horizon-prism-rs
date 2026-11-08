//! The `full_crawl` row protocol: claims as leases, lease renewal, the
//! guarded `done`, retries, and the requeues that follow account events.
//!
//! Every write a claimer makes is guarded by its `claim`, the crawl
//! generation and the account's `epoch`, so a stale claimer (its lease
//! passed, a hole or a requeue moved the epoch, full was left) writes
//! nothing.

use std::collections::BTreeMap;

use chrono::TimeDelta;
use sqlx::{PgConnection, PgPool};

use super::continuity;
use super::groups::{self, Groups};
use crate::db::ids::AccountId;

/// A claimed account.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Claim {
    pub account: AccountId,
    pub did: String,
    pub claim: i64,
    pub epoch: i64,
    pub crawl_generation: i64,
    /// The groups the claim fetches: owed and enabled.
    pub groups: Vec<String>,
    /// Every enabled group's generation when the claim was taken.
    pub generations: BTreeMap<String, i64>,
    /// A quiet `uncovered` account's requeue, kept from the claim.
    pub uncovered_requeue: Option<i64>,
}

/// Every row left `fetching` by an earlier process goes back to `pending`
/// (one process: no live claimer exists).
///
/// # Errors
///
/// Any database error.
pub async fn revert_fetching(pool: &PgPool) -> Result<u64, sqlx::Error> {
    Ok(sqlx::query("UPDATE full_crawl SET state = 'pending', next_at = now() WHERE state = 'fetching'").execute(pool).await?.rows_affected())
}

/// Claim the next account due at `crawl_generation`: a `pending` row, or a
/// `fetching` one whose lease passed, oldest `next_at` first; an
/// `uncovered` row only with its one-shot requeue. The claim bumps the
/// claim counter and holds the row for `lease_secs`.
///
/// # Errors
///
/// Any database error.
pub async fn claim_next(pool: &PgPool, crawl_generation: i64, lease_secs: u32) -> Result<Option<Claim>, sqlx::Error> {
    let mut tx = pool.begin().await?;
    let Some(g) = groups::read(&mut tx).await? else {
        return Ok(None);
    };
    if g.crawl_generation != crawl_generation {
        return Ok(None);
    }
    let enabled = g.enabled();
    type Row = (AccountId, String, i64, i64, Vec<String>, Option<i64>);
    let row: Option<Row> = sqlx::query_as(
        "UPDATE full_crawl f SET state = 'fetching', claim = f.claim + 1, generation = $1, \
           groups = CASE WHEN f.generation <> $1 THEN $2 ELSE f.groups END, \
           next_at = now() + make_interval(secs => $3) \
         FROM (SELECT account FROM full_crawl \
               WHERE (state = 'pending' OR state = 'fetching') AND next_at <= now() AND generation = $1 \
                 AND (NOT uncovered OR uncovered_requeue IS NOT NULL) \
               ORDER BY next_at LIMIT 1 FOR UPDATE SKIP LOCKED) n, dids d \
         WHERE f.account = n.account AND d.id = f.account \
         RETURNING f.account, d.did, f.claim, f.epoch, f.groups, f.uncovered_requeue",
    )
    .bind(crawl_generation)
    .bind(&enabled)
    .bind(f64::from(lease_secs))
    .fetch_optional(&mut *tx)
    .await?;
    tx.commit().await?;
    Ok(row.map(|(account, did, claim, epoch, owed, uncovered_requeue)| Claim {
        account,
        did,
        claim,
        epoch,
        crawl_generation,
        groups: owed.into_iter().filter(|o| enabled.contains(o)).collect(),
        generations: g.generations(),
        uncovered_requeue,
    }))
}

/// Renew the claim's lease after an apply batch. `false`: the claim is no
/// longer held, and the claimer stops applying.
///
/// # Errors
///
/// Any database error.
pub async fn renew(pool: &PgPool, c: &Claim, lease_secs: u32) -> Result<bool, sqlx::Error> {
    let n = sqlx::query(
        "UPDATE full_crawl SET next_at = now() + make_interval(secs => $3) WHERE account = $1 AND state = 'fetching' AND claim = $2",
    )
    .bind(c.account)
    .bind(c.claim)
    .bind(f64::from(lease_secs))
    .execute(pool)
    .await?
    .rows_affected();
    Ok(n == 1)
}

const GUARD: &str = "account = $1 AND state = 'fetching' AND claim = $2 AND generation = $3 AND epoch = $4";

/// End a claim with `state` and `reason`, no fetch done: `skipped`
/// (suspended, a trial's S or member), `gone` (deleted), or `done` for a
/// claim that owed nothing. Returns whether the claim still held.
///
/// # Errors
///
/// Any database error.
pub async fn settle(pool: &PgPool, c: &Claim, state: &str, reason: Option<&str>) -> Result<bool, sqlx::Error> {
    let n = sqlx::query(&format!(
        "UPDATE full_crawl SET state = $5, reason = $6, next_at = now(), \
           uncovered_requeue = CASE WHEN uncovered_requeue = $7 THEN NULL ELSE uncovered_requeue END \
         WHERE {GUARD}"
    ))
    .bind(c.account)
    .bind(c.claim)
    .bind(c.crawl_generation)
    .bind(c.epoch)
    .bind(state)
    .bind(reason)
    .bind(c.uncovered_requeue)
    .execute(pool)
    .await?
    .rows_affected();
    if n == 0 {
        release(pool, c).await?;
    }
    Ok(n == 1)
}

/// A refused claim goes back to `pending` at once, guarded by the claim
/// alone.
async fn release(pool: &PgPool, c: &Claim) -> Result<(), sqlx::Error> {
    sqlx::query("UPDATE full_crawl SET state = 'pending', next_at = now() WHERE account = $1 AND state = 'fetching' AND claim = $2")
        .bind(c.account)
        .bind(c.claim)
        .execute(pool)
        .await?;
    Ok(())
}

/// The retry delay after `attempts` failures: an hour, doubling, at most a
/// week.
#[must_use]
pub fn backoff(attempts: i32) -> TimeDelta {
    let hours = 1_i64 << attempts.clamp(1, 8).saturating_sub(1);
    TimeDelta::hours(hours.min(24 * 7))
}

/// A failed fetch: counted, retried with backoff, or after `max_attempts`
/// left `failed` (incomplete). Returns whether the claim still held.
///
/// # Errors
///
/// Any database error.
pub async fn failed(pool: &PgPool, c: &Claim, reason: &str, max_attempts: u32) -> Result<bool, sqlx::Error> {
    let attempts: Option<i32> = sqlx::query_scalar(&format!("SELECT attempts + 1 FROM full_crawl WHERE {GUARD}"))
        .bind(c.account)
        .bind(c.claim)
        .bind(c.crawl_generation)
        .bind(c.epoch)
        .fetch_optional(pool)
        .await?;
    let Some(attempts) = attempts else {
        release(pool, c).await?;
        return Ok(false);
    };
    let stop = i64::from(attempts) >= i64::from(max_attempts);
    let delay = backoff(attempts).num_seconds();
    let n = sqlx::query(&format!(
        "UPDATE full_crawl SET attempts = $5, reason = $6, state = CASE WHEN $7 THEN 'failed' ELSE 'pending' END, \
           next_at = now() + make_interval(secs => $8), \
           uncovered_requeue = CASE WHEN $7 AND uncovered_requeue = $9 THEN NULL ELSE uncovered_requeue END \
         WHERE {GUARD}"
    ))
    .bind(c.account)
    .bind(c.claim)
    .bind(c.crawl_generation)
    .bind(c.epoch)
    .bind(attempts)
    .bind(reason)
    .bind(stop)
    .bind(f64::from(i32::try_from(delay).unwrap_or(i32::MAX)))
    .bind(c.uncovered_requeue)
    .execute(pool)
    .await?
    .rows_affected();
    if n == 0 {
        release(pool, c).await?;
    }
    metrics::counter!(super::METRIC_REPOS, "state" => if stop { "failed" } else { "retry" }, "reason" => reason.to_string()).increment(1);
    Ok(n == 1)
}

/// What a finished fetch brings to its `done`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Fetched {
    pub car_rev: String,
    /// The fetch was verified (or verification is off): its marks may be
    /// written.
    pub verified: bool,
}

/// How a `done` ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DoneOutcome {
    /// Written; with the marks it wrote, and whether `last_rev` rose.
    Done { marks: usize, whole: bool },
    /// The CAR is older than the account's first live sighting: retried.
    OlderThanChain,
    /// The claim no longer held (a hole, a requeue, a lease, a phase
    /// change): the row is `pending` again or another claimer's.
    Refused,
}

/// The guarded `done`, in one transaction: the group histories read
/// share-locked, the conditional update, then the marks only if it
/// returned a row. A crawl that applied every group enabled at its claim
/// and still enabled at the generation the claim saw raises `last_rev` and
/// `chain_rev`; one of fewer groups writes its marks and nothing else. An
/// `uncovered` row gets no marks unless this `done` clears it: the claim
/// carried the account's quiet requeue, the fetch was whole and at least
/// `aside_rev`, and no Aside copy arrived after the requeue.
///
/// # Errors
///
/// Any database error.
pub async fn done(pool: &PgPool, c: &Claim, f: &Fetched) -> Result<DoneOutcome, sqlx::Error> {
    let mut tx = pool.begin().await?;
    let now = groups::read_for_share(&mut tx).await?.unwrap_or_default().generations();
    // The claimed groups whose generation has not moved since the claim.
    let kept: Vec<String> = c.groups.iter().filter(|g| c.generations.get(*g).is_some_and(|at| now.get(*g) == Some(at))).cloned().collect();
    // Whole: every group enabled at the claim, and still enabled at the
    // generation the claim saw, was applied.
    let whole = f.verified && c.generations.iter().filter(|&(g, at)| now.get(g) == Some(at)).all(|(g, _)| c.groups.contains(g));
    let marks: Vec<String> = if f.verified { kept } else { Vec::new() };
    type Row = (bool, Option<String>);
    let row: Option<Row> = sqlx::query_as(&format!(
        "UPDATE full_crawl SET \
           groups = ARRAY(SELECT x FROM unnest(groups) x WHERE NOT x = ANY($6)), \
           state = CASE WHEN cardinality(ARRAY(SELECT x FROM unnest(groups) x WHERE NOT x = ANY($6))) = 0 THEN 'done' ELSE 'pending' END, \
           rev = $5, attempts = 0, reason = CASE WHEN $8 THEN NULL ELSE 'unverified' END, next_at = now(), \
           last_rev = CASE WHEN $7 AND (last_rev IS NULL OR last_rev < $5) THEN $5 ELSE last_rev END, \
           chain_rev = CASE WHEN $7 AND (chain_rev IS NULL OR chain_rev < $5) THEN $5 ELSE chain_rev END, \
           chain_generation = CASE WHEN $7 THEN $3 ELSE chain_generation END, \
           uncovered = CASE WHEN uncovered AND $7 AND uncovered_requeue = $9 AND $5 >= COALESCE(aside_rev, '') \
                              AND COALESCE(aside_seen_ms, 0) <= $9 THEN false ELSE uncovered END, \
           uncovered_requeue = CASE WHEN uncovered_requeue = $9 THEN NULL ELSE uncovered_requeue END \
         WHERE {GUARD} AND (chain_from IS NULL OR $5 >= chain_from) \
         RETURNING uncovered, aside_rev"
    ))
    .bind(c.account)
    .bind(c.claim)
    .bind(c.crawl_generation)
    .bind(c.epoch)
    .bind(&f.car_rev)
    .bind(&marks)
    .bind(whole)
    .bind(f.verified)
    .bind(c.uncovered_requeue)
    .fetch_optional(&mut *tx)
    .await?;
    let Some((uncovered, aside_rev)) = row else {
        tx.rollback().await?;
        return refused(pool, c, &f.car_rev).await;
    };
    if whole {
        continuity::advance(&mut tx, c.account).await?;
        // A whole crawl that reached the Aside rev clears the Aside fields
        // (an `uncovered` row only by clearing `uncovered`, above).
        if !uncovered && aside_rev.as_deref().is_some_and(|a| a <= f.car_rev.as_str()) {
            continuity::clear_aside(&mut tx, c.account).await?;
        }
    }
    let written = if uncovered { 0 } else { marks.len() };
    if !uncovered {
        for g in &marks {
            sqlx::query(
                "INSERT INTO full_crawl_groups (account, grp, group_generation, crawl_generation) VALUES ($1, $2, $3, $4) \
                 ON CONFLICT (account, grp) DO UPDATE SET group_generation = EXCLUDED.group_generation, \
                   crawl_generation = EXCLUDED.crawl_generation, crawled_at = now()",
            )
            .bind(c.account)
            .bind(g)
            .bind(c.generations.get(g).copied().unwrap_or(0))
            .bind(c.crawl_generation)
            .execute(&mut *tx)
            .await?;
        }
    }
    tx.commit().await?;
    metrics::counter!(super::METRIC_REPOS, "state" => "done", "reason" => if f.verified { "verified" } else { "unverified" }).increment(1);
    Ok(DoneOutcome::Done { marks: written, whole })
}

/// A `done` that returned no row: first the CAR-older-than-the-first-
/// sighting retry (at once the first time, then counted with backoff),
/// else the claim-only return to `pending`.
async fn refused(pool: &PgPool, c: &Claim, car_rev: &str) -> Result<DoneOutcome, sqlx::Error> {
    let n = sqlx::query(&format!(
        "UPDATE full_crawl SET state = 'pending', \
           attempts = CASE WHEN reason = 'older_than_chain' THEN attempts + 1 ELSE attempts END, \
           next_at = CASE WHEN reason = 'older_than_chain' THEN now() + make_interval(hours => LEAST(168, 1 << LEAST(attempts, 7))) ELSE now() END, \
           reason = 'older_than_chain' \
         WHERE {GUARD} AND chain_from IS NOT NULL AND $5 < chain_from"
    ))
    .bind(c.account)
    .bind(c.claim)
    .bind(c.crawl_generation)
    .bind(c.epoch)
    .bind(car_rev)
    .execute(pool)
    .await?
    .rows_affected();
    if n == 1 {
        return Ok(DoneOutcome::OlderThanChain);
    }
    release(pool, c).await?;
    metrics::counter!(super::METRIC_REPOS, "state" => "refused", "reason" => "epoch").increment(1);
    Ok(DoneOutcome::Refused)
}

/// Requeue an account: it owes every enabled group again, its epoch moves
/// (so an in-flight crawl's `done` is refused) and its marks go. A
/// `fetching` row keeps its state for its claimer's refused `done`.
///
/// # Errors
///
/// Any database error.
pub async fn requeue(conn: &mut PgConnection, account: AccountId, groups: &[String], states: &[&str]) -> Result<bool, sqlx::Error> {
    let states: Vec<String> = states.iter().map(|s| (*s).to_string()).collect();
    let n = sqlx::query(
        "UPDATE full_crawl SET epoch = epoch + 1, groups = $2, next_at = now(), reason = NULL, attempts = 0, \
           state = CASE WHEN state = 'fetching' THEN state ELSE 'pending' END \
         WHERE account = $1 AND state = ANY($3)",
    )
    .bind(account)
    .bind(groups)
    .bind(&states)
    .execute(&mut *conn)
    .await?
    .rows_affected();
    if n > 0 {
        sqlx::query("DELETE FROM full_crawl_groups WHERE account = $1").bind(account).execute(conn).await?;
    }
    Ok(n > 0)
}

/// An `#account` event of a takedown relay in breadth Full: a deletion
/// leaves the row `gone` and a suspension that hides the account (`hides`:
/// a takedown relay under `refract_only`, not a member and not listed by
/// Refract) leaves it `skipped`, both moving the epoch so a crawl fetched
/// before is refused, and clearing the marks; a return to active, once no
/// takedown relay reports the account inactive, queues a skipped row again.
///
/// # Errors
///
/// Any database error.
pub async fn account_status(pool: &PgPool, did: &str, active: Option<bool>, status: Option<&str>, hides: bool, groups: &[String]) -> Result<(), sqlx::Error> {
    let Some(active) = active else {
        return Ok(());
    };
    let Some(account) = crate::db::ids::account_id(pool, did).await? else {
        return Ok(());
    };
    let mut tx = pool.begin().await?;
    if active {
        let still: bool = sqlx::query_scalar("SELECT EXISTS (SELECT 1 FROM relay_account_status WHERE account = $1)").bind(account).fetch_one(&mut *tx).await?;
        if !still {
            sqlx::query("UPDATE full_crawl SET state = 'pending' WHERE account = $1 AND state = 'skipped' AND reason IN ('suspended', 'takendown', 'deactivated')")
                .bind(account)
                .execute(&mut *tx)
                .await?;
            requeue(&mut tx, account, groups, &["pending"]).await?;
        }
    } else {
        let next = match status {
            Some("deleted") => Some(("gone", "deleted")),
            Some(s @ ("takendown" | "suspended" | "deactivated")) if hides => Some(("skipped", s)),
            _ => None,
        };
        if let Some((state, reason)) = next {
            let n = sqlx::query("UPDATE full_crawl SET state = $2, reason = $3, epoch = epoch + 1 WHERE account = $1 AND state <> 'gone'")
                .bind(account)
                .bind(state)
                .bind(reason)
                .execute(&mut *tx)
                .await?
                .rows_affected();
            if n > 0 {
                sqlx::query("DELETE FROM full_crawl_groups WHERE account = $1").bind(account).execute(&mut *tx).await?;
            }
        }
    }
    tx.commit().await?;
    Ok(())
}

/// Seed rows for `dids` (members, S, listed accounts) at `crawl_generation`
/// owing `groups`, due at `next_at_offset_secs` from now (negative: ahead
/// of the list); an existing row is left alone. A full trial's crawl seeds
/// S and the members as `skipped` with reason `trial`.
///
/// # Errors
///
/// Any database error.
pub async fn seed(
    pool: &PgPool,
    dids: &[String],
    crawl_generation: i64,
    groups: &[String],
    next_at_offset_secs: i64,
    skipped_reason: Option<&str>,
) -> Result<u64, sqlx::Error> {
    if dids.is_empty() {
        return Ok(0);
    }
    sqlx::query("INSERT INTO dids (did) SELECT unnest($1::text[]) ON CONFLICT (did) DO NOTHING").bind(dids).execute(pool).await?;
    let state = if skipped_reason.is_some() { "skipped" } else { "pending" };
    Ok(sqlx::query(
        "INSERT INTO full_crawl (account, generation, groups, state, reason, next_at) \
         SELECT d.id, $2, $3, $4, $5, now() + make_interval(secs => $6) FROM dids d WHERE d.did = ANY($1) \
         ON CONFLICT (account) DO NOTHING",
    )
    .bind(dids)
    .bind(crawl_generation)
    .bind(groups)
    .bind(state)
    .bind(skipped_reason)
    .bind(f64::from(i32::try_from(next_at_offset_secs).unwrap_or(0)))
    .execute(pool)
    .await?
    .rows_affected())
}

/// Move the due time of `dids`' `pending` rows ahead of the list (an
/// on-demand read). Rows in flight, done or elsewhere are untouched, and
/// no epoch moves.
///
/// # Errors
///
/// Any database error.
pub async fn hurry(pool: &PgPool, dids: &[String]) -> Result<u64, sqlx::Error> {
    if dids.is_empty() {
        return Ok(0);
    }
    Ok(sqlx::query(
        "UPDATE full_crawl f SET next_at = LEAST(f.next_at, now() - interval '1 day') FROM dids d \
         WHERE d.id = f.account AND d.did = ANY($1) AND f.state = 'pending'",
    )
    .bind(dids)
    .execute(pool)
    .await?
    .rows_affected())
}

/// The entry step after an interrupted narrowing: every row of an older
/// crawl generation moves to `crawl_generation`, owing every enabled
/// group. A `gone` row stays `gone`; a `skipped` row a takedown relay still
/// reports inactive stays `skipped`; every other row goes to `pending`.
/// In batches. Returns the rows moved.
///
/// # Errors
///
/// Any database error.
pub async fn entry_step(pool: &PgPool, crawl_generation: i64, groups: &[String]) -> Result<u64, sqlx::Error> {
    let mut total = 0;
    loop {
        let n = sqlx::query(
            "UPDATE full_crawl f SET generation = $1, groups = $2, next_at = now(), \
               state = CASE WHEN f.state = 'gone' THEN 'gone' \
                            WHEN f.state = 'skipped' AND EXISTS (SELECT 1 FROM relay_account_status r WHERE r.account = f.account) THEN 'skipped' \
                            ELSE 'pending' END \
             WHERE f.account IN (SELECT account FROM full_crawl WHERE generation <> $1 AND state <> 'fetching' LIMIT 5000)",
        )
        .bind(crawl_generation)
        .bind(groups)
        .execute(pool)
        .await?
        .rows_affected();
        total += n;
        if n == 0 {
            return Ok(total);
        }
    }
}

/// Every group's Complete, at `crawl_generation`: for a group whose owed-
/// set update has finished, Complete when no row of this generation (or an
/// older one) still owes it or is `pending`, `fetching` or `failed` with
/// it owed, and no row of an older generation is left. An `uncovered` row
/// counts as skipped, as do `gone` and `skipped` rows; accounts kept
/// incomplete by hand do not hold a group back. Returns the groups that
/// turned Complete.
///
/// # Errors
///
/// Any database error.
pub async fn complete_groups(pool: &PgPool, crawl_generation: i64) -> Result<Vec<String>, sqlx::Error> {
    let mut tx = pool.begin().await?;
    let Some(mut g) = groups::read_for_update(&mut tx).await? else {
        return Ok(Vec::new());
    };
    if g.crawl_generation != crawl_generation {
        return Ok(Vec::new());
    }
    let mut turned = Vec::new();
    let names: Vec<String> = g.groups.iter().filter(|(_, x)| x.enabled() && !x.complete() && x.union_done).map(|(k, _)| k.clone()).collect();
    for name in names {
        let held: bool = sqlx::query_scalar(
            "SELECT EXISTS (SELECT 1 FROM full_crawl f \
               WHERE NOT f.uncovered AND f.state NOT IN ('gone', 'skipped') \
                 AND (f.generation <> $1 OR $2 = ANY(f.groups)) \
                 AND NOT EXISTS (SELECT 1 FROM full_incomplete i WHERE i.grp = $2 AND i.account = f.account))",
        )
        .bind(crawl_generation)
        .bind(&name)
        .fetch_one(&mut *tx)
        .await?;
        if !held && let Some(x) = g.groups.get_mut(&name) {
            x.state = groups::COMPLETE.to_string();
            turned.push(name);
        }
    }
    if !turned.is_empty() {
        groups::write(&mut tx, &g).await?;
    }
    tx.commit().await?;
    Ok(turned)
}

/// Set every Crawling group Paused, or every Paused one Crawling again.
///
/// # Errors
///
/// Any database error.
pub async fn set_paused(pool: &PgPool, crawl_generation: i64, paused: bool) -> Result<(), sqlx::Error> {
    let mut tx = pool.begin().await?;
    if let Some(mut g) = groups::read_for_update(&mut tx).await?
        && g.crawl_generation == crawl_generation
    {
        let (from, to) = if paused { (groups::CRAWLING, groups::PAUSED) } else { (groups::PAUSED, groups::CRAWLING) };
        let mut changed = false;
        for x in g.groups.values_mut().filter(|x| x.state == from) {
            x.state = to.to_string();
            changed = true;
        }
        if changed {
            groups::write(&mut tx, &g).await?;
        }
    }
    tx.commit().await?;
    Ok(())
}

/// The stored group histories, unlocked.
///
/// # Errors
///
/// Any database error.
pub async fn read_groups(pool: &PgPool) -> Result<Option<Groups>, sqlx::Error> {
    let mut conn = pool.acquire().await?;
    groups::read(&mut conn).await
}
