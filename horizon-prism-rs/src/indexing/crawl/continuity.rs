//! Full indexing's per-account continuity: every Whole or Covered copy of a
//! commit raises the account's `last_rev` and runs the chain rules on
//! `full_crawl.chain_rev` and `full_chain_links`; an Aside copy only
//! records that a relay full cannot follow moved ahead. A hole rebases the
//! chain, bumps the epoch, clears the account's crawl marks and queues it.
//!
//! These statements run last in the writer's transaction, after every write
//! that fires a count trigger, so the lanes, the crawl and the sweeper take
//! locks in one order.

use sqlx::PgConnection;

use crate::db::ids::AccountId;

/// Links kept per account; one more is a hole.
pub const LINKS_PER_ACCOUNT: i64 = 64;

/// Metric: holes, by cause.
pub const METRIC_HOLES: &str = "full_continuity_holes_total";

/// What a Whole or Covered copy of a commit brings.
#[derive(Debug, Clone, Copy)]
pub struct Copy<'a> {
    pub account: AccountId,
    pub rev: &'a str,
    pub since: Option<&'a str>,
    /// The stream-id milliseconds of the entry.
    pub stream_ms: i64,
    pub crawl_generation: i64,
    /// Every enabled group, owed again after a hole.
    pub groups: &'a [String],
    /// A failed op of an enabled group: a hole at once.
    pub failed: bool,
    /// The relay reader stamped the copy. An unstamped (older) reader
    /// sends no `since`, so its copies cannot chain: continuity is reduced
    /// to the re-list's comparison of heads.
    pub stamped: bool,
    /// The commit carries at least one op. A repository's first commit
    /// carries none; some PDS implementations send no `since` on any
    /// commit, and theirs carry ops.
    pub carries_ops: bool,
}

/// What the copy did to the account's row.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Advanced {
    /// The row was created: the account's first sighting in full.
    pub inserted: bool,
    pub holed: bool,
}

/// The upsert every Whole or Covered copy runs first: it takes the row's
/// lock and raises `last_rev`. A new row is `pending` at the current crawl
/// generation, owing every enabled group.
const RAISE_SQL: &str = "INSERT INTO full_crawl AS f (account, generation, groups, last_rev) VALUES ($1, $2, $3, $4) \
     ON CONFLICT (account) DO UPDATE SET last_rev = CASE WHEN f.last_rev IS NULL OR f.last_rev < EXCLUDED.last_rev THEN EXCLUDED.last_rev ELSE f.last_rev END \
     RETURNING (xmax = 0), f.chain_rev, f.chain_generation, f.last_rev, f.aside_rev, f.uncovered";

type Raised = (bool, Option<String>, Option<i64>, Option<String>, Option<String>, bool);

/// Raise `last_rev` to `rev` (creating the row when missing), with the
/// Aside fields' rules: a raise to at least `aside_rev` clears them (and
/// clears `uncovered`, queueing the account once); a raise that stays below
/// restarts the Aside timer at this copy.
async fn raise(conn: &mut PgConnection, c: &Copy<'_>) -> Result<Raised, sqlx::Error> {
    let raised: Raised = sqlx::query_as(RAISE_SQL)
        .bind(c.account)
        .bind(c.crawl_generation)
        .bind(c.groups)
        .bind(c.rev)
        .fetch_one(&mut *conn)
        .await?;
    let (_, _, _, last, aside, uncovered) = &raised;
    if let (Some(last), Some(aside)) = (last, aside) {
        if last >= aside {
            if *uncovered {
                clear_uncovered(conn, c.account, c.groups).await?;
            } else {
                clear_aside(conn, c.account).await?;
            }
        } else if last == c.rev && !*uncovered {
            sqlx::query("UPDATE full_crawl SET aside_ms = $2 WHERE account = $1").bind(c.account).bind(c.stream_ms).execute(&mut *conn).await?;
        }
    }
    Ok(raised)
}

/// Clear the Aside fields of a row that is not `uncovered`.
pub(crate) async fn clear_aside(conn: &mut PgConnection, account: AccountId) -> Result<(), sqlx::Error> {
    sqlx::query("UPDATE full_crawl SET aside_rev = NULL, aside_ms = NULL, aside_seen_ms = NULL, aside_relay = NULL WHERE account = $1 AND NOT uncovered")
        .bind(account)
        .execute(conn)
        .await?;
    Ok(())
}

/// A Whole or Covered relay reached an `uncovered` account again: it owes
/// every enabled group, its epoch moves and it is queued once; a `gone` or
/// `skipped` row keeps its state.
pub(crate) async fn clear_uncovered(conn: &mut PgConnection, account: AccountId, groups: &[String]) -> Result<(), sqlx::Error> {
    sqlx::query(
        "UPDATE full_crawl SET uncovered = false, uncovered_requeue = NULL, aside_rev = NULL, aside_ms = NULL, aside_seen_ms = NULL, aside_relay = NULL, \
           groups = $2, epoch = epoch + 1, next_at = now(), \
           state = CASE WHEN state IN ('gone', 'skipped', 'fetching') THEN state ELSE 'pending' END \
         WHERE account = $1 AND uncovered",
    )
    .bind(account)
    .bind(groups)
    .execute(conn)
    .await?;
    Ok(())
}

/// The continuity statements of one Whole or Covered copy.
///
/// # Errors
///
/// Any database error.
pub async fn on_copy(conn: &mut PgConnection, c: &Copy<'_>) -> Result<Advanced, sqlx::Error> {
    let (inserted, chain_rev, chain_generation, _, _, _) = raise(conn, c).await?;
    let mut out = Advanced { inserted, holed: false };
    // Rule 1: first sight, whoever created the row, or a chain kept from an
    // earlier crawl generation.
    let first = chain_rev.is_none() || chain_generation != Some(c.crawl_generation);
    if first {
        sqlx::query("UPDATE full_crawl SET chain_rev = $2, chain_from = $3, chain_generation = $4 WHERE account = $1")
            .bind(c.account)
            .bind(c.rev)
            .bind(c.since)
            .bind(c.crawl_generation)
            .execute(&mut *conn)
            .await?;
        sqlx::query("DELETE FROM full_chain_links WHERE account = $1").bind(c.account).execute(&mut *conn).await?;
        if c.failed {
            hole(conn, c.account, c.crawl_generation, c.groups, "failed_op").await?;
            out.holed = true;
        }
        return Ok(out);
    }
    if c.failed {
        hole(conn, c.account, c.crawl_generation, c.groups, "failed_op").await?;
        out.holed = true;
        return Ok(out);
    }
    let chain_rev = chain_rev.unwrap_or_default();
    // Rule 2: a late copy of something already chained.
    if c.rev <= chain_rev.as_str() {
        return Ok(out);
    }
    match c.since {
        // An older reader's copy carries no `since`, and some PDS
        // implementations never send one: such a copy is taken as the next
        // commit (the re-list's comparison of heads is the backstop).
        None if !c.stamped || c.carries_ops => {
            sqlx::query("UPDATE full_crawl SET chain_rev = $2 WHERE account = $1").bind(c.account).bind(c.rev).execute(&mut *conn).await?;
            advance(conn, c.account).await?;
        }
        // An op-less commit with no `since` on an established chain: a
        // re-created repo's first commit.
        None => {
            hole(conn, c.account, c.crawl_generation, c.groups, "no_since").await?;
            out.holed = true;
        }
        // Rule 3: it chains.
        Some(since) if since <= chain_rev.as_str() => {
            sqlx::query("UPDATE full_crawl SET chain_rev = $2 WHERE account = $1").bind(c.account).bind(c.rev).execute(&mut *conn).await?;
            advance(conn, c.account).await?;
        }
        // Rule 4: a link, capped.
        Some(since) => {
            sqlx::query("INSERT INTO full_chain_links (account, since, rev, linked_ms) VALUES ($1, $2, $3, $4) ON CONFLICT DO NOTHING")
                .bind(c.account)
                .bind(since)
                .bind(c.rev)
                .bind(c.stream_ms)
                .execute(&mut *conn)
                .await?;
            let links: i64 = sqlx::query_scalar("SELECT count(*) FROM full_chain_links WHERE account = $1").bind(c.account).fetch_one(&mut *conn).await?;
            if links > LINKS_PER_ACCOUNT {
                hole(conn, c.account, c.crawl_generation, c.groups, "links_full").await?;
                out.holed = true;
            }
        }
    }
    Ok(out)
}

/// A `#sync` of a Whole or Covered relay: `last_rev` rises to its rev
/// first, then it is a hole at once, so the rebase lands on the reset.
///
/// # Errors
///
/// Any database error.
pub async fn on_sync(conn: &mut PgConnection, c: &Copy<'_>) -> Result<Advanced, sqlx::Error> {
    let (inserted, ..) = raise(conn, c).await?;
    hole(conn, c.account, c.crawl_generation, c.groups, "sync").await?;
    Ok(Advanced { inserted, holed: true })
}

/// An Aside copy: an existing row whose `last_rev` is below the copy's rev
/// (or NULL) records it. While the row is `uncovered`, `aside_rev` stays
/// frozen and only `aside_seen_ms` moves. A row is never created.
///
/// # Errors
///
/// Any database error.
pub async fn on_aside(conn: &mut PgConnection, account: AccountId, rev: &str, stream_ms: i64, relay: &str) -> Result<(), sqlx::Error> {
    sqlx::query(
        "UPDATE full_crawl SET \
           aside_rev = CASE WHEN uncovered THEN aside_rev WHEN aside_rev IS NULL OR aside_rev < $2 THEN $2 ELSE aside_rev END, \
           aside_relay = CASE WHEN uncovered THEN aside_relay ELSE $4 END, \
           aside_ms = CASE WHEN uncovered THEN aside_ms ELSE COALESCE(aside_ms, $3) END, \
           aside_seen_ms = $3 \
         WHERE account = $1 AND (last_rev IS NULL OR last_rev < $2)",
    )
    .bind(account)
    .bind(rev)
    .bind(stream_ms)
    .bind(relay)
    .execute(conn)
    .await?;
    Ok(())
}

/// Rule 5: after a raise of `chain_rev`, every link whose `since` is at or
/// below it chains in turn; then every link at or below it is dropped.
///
/// # Errors
///
/// Any database error.
pub async fn advance(conn: &mut PgConnection, account: AccountId) -> Result<(), sqlx::Error> {
    for _ in 0..=LINKS_PER_ACCOUNT {
        let moved: Option<String> = sqlx::query_scalar(
            "UPDATE full_crawl f SET chain_rev = n.r \
             FROM (SELECT max(l.rev) AS r FROM full_chain_links l JOIN full_crawl c ON c.account = l.account \
                   WHERE l.account = $1 AND l.since <= c.chain_rev AND l.rev > c.chain_rev) n \
             WHERE f.account = $1 AND n.r IS NOT NULL RETURNING f.chain_rev",
        )
        .bind(account)
        .fetch_optional(&mut *conn)
        .await?;
        if moved.is_none() {
            break;
        }
    }
    sqlx::query("DELETE FROM full_chain_links l USING full_crawl f WHERE l.account = $1 AND f.account = $1 AND l.rev <= f.chain_rev")
        .bind(account)
        .execute(conn)
        .await?;
    Ok(())
}

/// A hole: `chain_rev` rebases to the highest rev the account reached,
/// links at or below it go, the epoch moves once, `chain_from` becomes
/// `last_rev`, the marks are cleared and the account is queued owing every
/// enabled group. A `fetching` row keeps its state (its claimer's `done` is
/// refused by the epoch); a `gone` or `skipped` one too.
///
/// # Errors
///
/// Any database error.
pub async fn hole(conn: &mut PgConnection, account: AccountId, crawl_generation: i64, groups: &[String], cause: &'static str) -> Result<(), sqlx::Error> {
    sqlx::query(
        "UPDATE full_crawl SET chain_rev = COALESCE(last_rev, chain_rev), chain_generation = $2, chain_from = COALESCE(last_rev, chain_from), \
           epoch = epoch + 1, groups = $3, next_at = now(), \
           state = CASE WHEN state IN ('gone', 'skipped', 'fetching') THEN state ELSE 'pending' END \
         WHERE account = $1",
    )
    .bind(account)
    .bind(crawl_generation)
    .bind(groups)
    .execute(&mut *conn)
    .await?;
    advance(conn, account).await?;
    sqlx::query("DELETE FROM full_crawl_groups WHERE account = $1").bind(account).execute(conn).await?;
    metrics::counter!(METRIC_HOLES, "cause" => cause).increment(1);
    Ok(())
}
