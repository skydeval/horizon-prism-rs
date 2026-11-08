//! Changing `[full] kinds` while in full. The kinds set is read at
//! startup; the crawl compares it with the stored histories.
//!
//! - An added group gets a new generation and starts Crawling; a removal
//!   sweep of it still running is stopped in the same write; it is added to
//!   every row's owed set in keyset batches (`done` rows go back to
//!   `pending`), and its `union_done` is set when the last batch commits,
//!   so it never reads Complete before.
//! - A removed group gets a new generation (marks of it never count
//!   again), is taken out of every owed set (a `pending` row left owing
//!   nothing is `done`), and its tables are swept with community's kept
//!   rows (S, members, Refract's R, member-involved rows) in batches, each
//!   conditional on the group's generation, so a re-add stops it.

use std::collections::{BTreeMap, BTreeSet};

use serde_json::{Value, json};
use sqlx::PgPool;

use super::groups::{self, Group};
use crate::config::FullGroup;
use crate::db::retention::{BATCH_PAUSE, BATCH_SIZE, Bind, LOCK_DID_SQL, Step};

/// The `worker_state` key of the kinds changes in progress.
pub const CHANGES_KEY: &str = "full_group_changes";

/// The tables a group's removal sweeps.
fn swept_tables(g: FullGroup) -> &'static [&'static str] {
    match g {
        FullGroup::Posts => &["posts", "post_search", "feed_items", "threadgates", "postgates"],
        FullGroup::Likes => &["likes"],
        FullGroup::Reposts => &["reposts", "feed_items"],
        FullGroup::Follows => &["follows"],
        FullGroup::Blocks => &["blocks", "list_blocks"],
        FullGroup::Lists => &["lists", "list_items", "list_optouts"],
        FullGroup::Profiles => &["actor_self_records", "labeler_services"],
        FullGroup::StarterPacks => &["starter_packs", "starter_pack_counts"],
        FullGroup::Polls => &["nearhorizon_poll_posts"],
    }
}

async fn read_changes(conn: &mut sqlx::PgConnection) -> Result<BTreeMap<String, Value>, sqlx::Error> {
    let v: Option<Value> = sqlx::query_scalar("SELECT value FROM worker_state WHERE key = $1").bind(CHANGES_KEY).fetch_optional(conn).await?;
    Ok(v.and_then(|v| v.as_object().cloned()).map(|m| m.into_iter().collect()).unwrap_or_default())
}

async fn write_changes(conn: &mut sqlx::PgConnection, c: &BTreeMap<String, Value>) -> Result<(), sqlx::Error> {
    let v: serde_json::Map<String, Value> = c.iter().map(|(k, v)| (k.clone(), v.clone())).collect();
    sqlx::query(
        "INSERT INTO worker_state (key, value, updated_at) VALUES ($1, $2, now()) ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value, updated_at = now()",
    )
    .bind(CHANGES_KEY)
    .bind(Value::Object(v))
    .execute(conn)
    .await?;
    Ok(())
}

/// Compare `kinds` with the stored histories at `crawl_generation` and
/// start each change. Returns the groups added and removed.
///
/// # Errors
///
/// Any database error.
pub async fn reconcile_kinds(pool: &PgPool, crawl_generation: i64, kinds: &[FullGroup]) -> Result<(Vec<String>, Vec<String>), sqlx::Error> {
    let mut tx = pool.begin().await?;
    let Some(mut g) = groups::read_for_update(&mut tx).await? else {
        return Ok((Vec::new(), Vec::new()));
    };
    if g.crawl_generation != crawl_generation {
        return Ok((Vec::new(), Vec::new()));
    }
    let wanted: BTreeSet<String> = kinds.iter().map(|k| k.as_str().to_string()).collect();
    let have: BTreeSet<String> = g.enabled().into_iter().collect();
    let added: Vec<String> = wanted.difference(&have).cloned().collect();
    let removed: Vec<String> = have.difference(&wanted).cloned().collect();
    if added.is_empty() && removed.is_empty() {
        return Ok((added, removed));
    }
    let mut changes = read_changes(&mut tx).await?;
    for name in &added {
        let generation = g.groups.get(name).map_or(0, |x| x.generation) + 1;
        g.groups.insert(name.clone(), Group { generation, state: groups::CRAWLING.to_string(), incomplete: false, union_done: false });
        // A removal sweep still running stops here.
        changes.insert(name.clone(), json!({ "op": "add", "generation": generation, "cursor": 0 }));
    }
    for name in &removed {
        let generation = g.groups.get(name).map_or(0, |x| x.generation) + 1;
        g.groups.insert(name.clone(), Group { generation, state: groups::REMOVED.to_string(), incomplete: false, union_done: true });
        changes.insert(name.clone(), json!({ "op": "remove", "generation": generation, "cursor": 0, "step": 0 }));
    }
    groups::write(&mut tx, &g).await?;
    write_changes(&mut tx, &changes).await?;
    tx.commit().await?;
    tracing::info!(event = "full_kinds_changed", ?added, ?removed, "full indexing's groups changed: added groups are crawled, removed ones swept");
    Ok((added, removed))
}

/// The group's generation now, share-locked in the caller's transaction.
async fn generation_now(conn: &mut sqlx::PgConnection, name: &str) -> Result<Option<i64>, sqlx::Error> {
    Ok(groups::read_for_share(conn).await?.and_then(|g| g.groups.get(name).map(|x| x.generation)))
}

/// One batch of every change in progress. Returns whether any work is
/// left.
///
/// # Errors
///
/// Any database error.
pub async fn step(pool: &PgPool, crawl_generation: i64) -> Result<bool, sqlx::Error> {
    let mut conn = pool.acquire().await?;
    let changes = read_changes(&mut conn).await?;
    drop(conn);
    let mut left = false;
    for (name, job) in changes {
        let generation = job.get("generation").and_then(Value::as_i64).unwrap_or(0);
        let cursor = job.get("cursor").and_then(Value::as_i64).unwrap_or(0);
        let op = job.get("op").and_then(Value::as_str).unwrap_or_default().to_string();
        let mut tx = pool.begin().await?;
        // A newer change of the group (a re-add stops a removal) replaced
        // this one: the batch writes nothing.
        if generation_now(&mut tx, &name).await? != Some(generation) {
            tx.rollback().await?;
            continue;
        }
        let batch: Vec<i32> = sqlx::query_scalar("SELECT account FROM full_crawl WHERE account > $1 ORDER BY account LIMIT 5000")
            .bind(cursor)
            .fetch_all(&mut *tx)
            .await?;
        if let Some(&last) = batch.last() {
            if op == "add" {
                sqlx::query(
                    "UPDATE full_crawl SET groups = array_append(groups, $2), state = CASE WHEN state = 'done' THEN 'pending' ELSE state END, next_at = now() \
                     WHERE account = ANY($1) AND NOT $2 = ANY(groups)",
                )
                .bind(&batch)
                .bind(&name)
                .execute(&mut *tx)
                .await?;
            } else {
                sqlx::query(
                    "UPDATE full_crawl SET groups = array_remove(groups, $2), \
                       state = CASE WHEN state = 'pending' AND cardinality(array_remove(groups, $2)) = 0 THEN 'done' ELSE state END \
                     WHERE account = ANY($1) AND $2 = ANY(groups)",
                )
                .bind(&batch)
                .bind(&name)
                .execute(&mut *tx)
                .await?;
            }
            set_job(&mut tx, &name, json!({ "cursor": i64::from(last) })).await?;
            tx.commit().await?;
            left = true;
            continue;
        }
        if op == "add" {
            // The last batch committed: the group may now reach Complete.
            if let Some(mut g) = groups::read_for_update(&mut tx).await?
                && g.crawl_generation == crawl_generation
                && let Some(x) = g.groups.get_mut(&name)
            {
                x.union_done = true;
                groups::write(&mut tx, &g).await?;
            }
            remove_job(&mut tx, &name).await?;
            tx.commit().await?;
            continue;
        }
        tx.commit().await?;
        // The removal's sweep, a step at a time.
        let Some(group) = FullGroup::parse(&name) else {
            let mut tx = pool.begin().await?;
            remove_job(&mut tx, &name).await?;
            tx.commit().await?;
            continue;
        };
        let at = usize::try_from(job.get("step").and_then(Value::as_i64).unwrap_or(0)).unwrap_or(0);
        let tables = swept_tables(group);
        let steps: Vec<Step> = crate::db::retention::scope_steps(crate::private_tier::retention::row_authors(pool).await?)
            .into_iter()
            .filter(|s| tables.contains(&s.table))
            .collect();
        match steps.get(at) {
            Some(s) => {
                if sweep_step(pool, &name, generation, s).await? {
                    let mut tx = pool.begin().await?;
                    if generation_now(&mut tx, &name).await? == Some(generation) {
                        set_job(&mut tx, &name, json!({ "step": i64::try_from(at + 1).unwrap_or(i64::MAX) })).await?;
                    }
                    tx.commit().await?;
                }
                left = true;
            }
            None => {
                let mut tx = pool.begin().await?;
                if generation_now(&mut tx, &name).await? == Some(generation) {
                    remove_job(&mut tx, &name).await?;
                }
                tx.commit().await?;
                tracing::info!(event = "full_group_swept", group = %name, "a removed group's rows were swept");
            }
        }
    }
    Ok(left)
}

async fn set_job(conn: &mut sqlx::PgConnection, name: &str, fields: Value) -> Result<(), sqlx::Error> {
    sqlx::query("UPDATE worker_state SET value = jsonb_set(value, ARRAY[$2], (value -> $2) || $3), updated_at = now() WHERE key = $1")
        .bind(CHANGES_KEY)
        .bind(name)
        .bind(fields)
        .execute(conn)
        .await?;
    Ok(())
}

async fn remove_job(conn: &mut sqlx::PgConnection, name: &str) -> Result<(), sqlx::Error> {
    sqlx::query("UPDATE worker_state SET value = value - $2, updated_at = now() WHERE key = $1").bind(CHANGES_KEY).bind(name).execute(conn).await?;
    Ok(())
}

fn bound<'q>(q: sqlx::query::Query<'q, sqlx::Postgres, sqlx::postgres::PgArguments>, bind: &'q Bind) -> sqlx::query::Query<'q, sqlx::Postgres, sqlx::postgres::PgArguments> {
    match bind {
        Bind::None => q,
        Bind::Days(d) => q.bind(*d),
        Bind::Dids(dids) => q.bind(dids.clone()),
    }
}

/// One retention step over a removed group's table, each batch in a
/// transaction that share-locks the histories and re-tests the group's
/// generation. Returns `false` when a newer change stopped it.
async fn sweep_step(pool: &PgPool, name: &str, generation: i64, step: &Step) -> Result<bool, sqlx::Error> {
    let Some(candidates) = &step.candidates else {
        loop {
            let mut tx = pool.begin().await?;
            if generation_now(&mut tx, name).await? != Some(generation) {
                return Ok(false);
            }
            let n = bound(sqlx::query(&step.sql), &step.bind).execute(&mut *tx).await?.rows_affected();
            tx.commit().await?;
            if n < BATCH_SIZE {
                return Ok(true);
            }
            tokio::time::sleep(BATCH_PAUSE).await;
        }
    };
    let mut seen = BTreeSet::new();
    loop {
        let q = sqlx::query_scalar::<sqlx::Postgres, String>(candidates);
        let q = match &step.bind {
            Bind::None => q,
            Bind::Days(d) => q.bind(*d),
            Bind::Dids(dids) => q.bind(dids.clone()),
        };
        let fresh: Vec<String> = q.fetch_all(pool).await?.into_iter().filter(|d| seen.insert(d.clone())).collect();
        if fresh.is_empty() {
            return Ok(true);
        }
        for did in &fresh {
            loop {
                let mut tx = pool.begin().await?;
                if generation_now(&mut tx, name).await? != Some(generation) {
                    return Ok(false);
                }
                sqlx::query(LOCK_DID_SQL).bind(did).execute(&mut *tx).await?;
                let n = bound(sqlx::query(&step.sql), &step.bind).bind(did).execute(&mut *tx).await?.rows_affected();
                tx.commit().await?;
                if n < BATCH_SIZE {
                    break;
                }
            }
        }
        tokio::time::sleep(BATCH_PAUSE).await;
    }
}
