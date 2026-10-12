//! What a `list_sync` run writes (design-v3 §4.3 "The run"): the snapshot
//! it starts from, and what it does with the owner's listings once it has
//! them. Fetching the listings is not here.
//!
//! - [`snapshot`] (step 1): the owner's active lists, each with its row
//!   instance, and the admitted references that are not effective yet.
//! - [`write_purposes`] (step 3a): each list's purpose from the listing of
//!   the owner's list records. It runs after the run's last request to the
//!   owner's server, so no list row is locked between two requests.
//! - [`reconcile`] (step 4): the copy of each list's items, in batches.
//! - [`complete`] (step 5): counts, the owner's row, and the references
//!   that now take effect.
//!
//! A reference takes effect only through a run that began after it was
//! admitted, served its list and listed its list record. A listing that
//! was cut short adds items and does nothing else.

use std::collections::{HashMap, HashSet};

use chrono::{DateTime, Utc};
use sqlx::{PgConnection, PgPool};

use super::copy::{Limits, METRIC_TOMBSTONES_EVICTED};
use super::jobs;
use crate::db::locks::lock_uris;

/// URIs per reconcile batch: one advisory lock each.
pub const BATCH: usize = 500;
/// A purpose is stored only up to this many bytes.
const MAX_PURPOSE_BYTES: usize = 512;
/// A known purpose is cleared only by a second listing without the record
/// at least this long after the first.
const UNLISTED_CLEARS_AFTER_HOURS: i32 = 24;

pub const METRIC_PURPOSE_WRITE_ERRORS: &str = "list_purpose_write_errors_total";

/// One active list of the owner as the run found it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SnapList {
    pub uri: String,
    /// The row instance: a row with another value is a different
    /// subscription, and the run writes nothing for it.
    pub activated_at: DateTime<Utc>,
    pub purpose_rev: Option<String>,
    /// Not served since it became active: one listing is enough to remove
    /// an item that is not in it.
    pub first_sync: bool,
}

/// An admitted reference that was not effective when the run began.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WaitingRef {
    pub member: String,
    pub list: String,
    pub source: String,
    pub admitted_at: Option<DateTime<Utc>>,
}

/// What a run starts from (`S0`, `E0`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Snapshot {
    pub owner: String,
    /// The job row's generation at the claim: a change supersedes the run.
    pub generation: i64,
    pub lists: Vec<SnapList>,
    pub waiting: Vec<WaitingRef>,
    /// The owner's `resync_requested_at` as the run saw it.
    pub resync_requested_at: Option<DateTime<Utc>>,
}

/// One listed `app.bsky.graph.listitem` record.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ListedItem {
    pub uri: String,
    pub rkey: String,
    pub cid: String,
    pub list_uri: String,
    pub subject_did: String,
}

/// The owner's two listings, taken at repository revision `rev`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Listing {
    pub rev: String,
    /// The owner's list records, by list URI, each with its `purpose` if
    /// it has one. `None` when that listing did not complete.
    pub lists: Option<HashMap<String, Option<String>>>,
    pub items: Vec<ListedItem>,
    /// Whether the item listing reached its end.
    pub items_complete: bool,
}

/// Why a run stopped writing.
#[derive(Debug)]
pub enum Stopped {
    /// The job's generation changed: a purge or a reset superseded the run.
    Superseded,
    Db(sqlx::Error),
}

impl From<sqlx::Error> for Stopped {
    fn from(e: sqlx::Error) -> Self {
        Self::Db(e)
    }
}

/// Step 1, in one database snapshot.
///
/// # Errors
///
/// Database errors.
pub async fn snapshot(pool: &PgPool, owner: &str, generation: i64) -> Result<Snapshot, sqlx::Error> {
    let mut tx = pool.begin().await?;
    sqlx::query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY").execute(&mut *tx).await?;
    let lists: Vec<(String, DateTime<Utc>, Option<String>, bool)> = sqlx::query_as(
        "SELECT list_uri, activated_at, purpose_rev, synced_at IS NULL FROM subscribed_lists \
         WHERE owner_did = $1 AND state = 'active' ORDER BY list_uri",
    )
    .bind(owner)
    .fetch_all(&mut *tx)
    .await?;
    let uris: Vec<&String> = lists.iter().map(|(uri, ..)| uri).collect();
    let waiting: Vec<(String, String, String, Option<DateTime<Utc>>)> = sqlx::query_as(
        "SELECT member_did, list_uri, source, admitted_at FROM list_references \
         WHERE admitted AND NOT effective AND list_uri = ANY($1) ORDER BY member_did, list_uri, source",
    )
    .bind(&uris)
    .fetch_all(&mut *tx)
    .await?;
    let resync_requested_at: Option<Option<DateTime<Utc>>> =
        sqlx::query_scalar("SELECT resync_requested_at FROM subscribed_owners WHERE owner_did = $1")
            .bind(owner)
            .fetch_optional(&mut *tx)
            .await?;
    tx.commit().await?;
    Ok(Snapshot {
        owner: owner.to_string(),
        generation,
        lists: lists
            .into_iter()
            .map(|(uri, activated_at, purpose_rev, first_sync)| SnapList { uri, activated_at, purpose_rev, first_sync })
            .collect(),
        waiting: waiting
            .into_iter()
            .map(|(member, list, source, admitted_at)| WaitingRef { member, list, source, admitted_at })
            .collect(),
        resync_requested_at: resync_requested_at.flatten(),
    })
}

/// A purpose is stored as read only if it has no NUL and is short enough.
pub(super) fn storable(purpose: Option<&str>) -> Option<&str> {
    purpose.filter(|p| !p.contains('\0') && p.len() <= MAX_PURPOSE_BYTES)
}

/// Whether the job is still the run that was claimed: still `running`, at
/// the same generation. A purge cancels the row; a reset or the stuck-row
/// check bumps the generation. Takes the job row `FOR SHARE` (lock step 6).
async fn current(conn: &mut PgConnection, snap: &Snapshot) -> Result<bool, sqlx::Error> {
    let row: Option<(i64, String)> = sqlx::query_as("SELECT generation, state FROM backfill_jobs WHERE did = $1 AND depth = $2 FOR SHARE")
        .bind(&snap.owner)
        .bind(jobs::DEPTH)
        .fetch_optional(conn)
        .await?;
    Ok(row.is_some_and(|(generation, state)| generation == snap.generation && state == "running"))
}

/// Lock `list`'s row and say whether it is still the active instance the
/// run began with.
async fn same_instance(conn: &mut PgConnection, list: &SnapList) -> Result<bool, sqlx::Error> {
    let row: Option<(String, DateTime<Utc>)> = sqlx::query_as("SELECT state, activated_at FROM subscribed_lists WHERE list_uri = $1 FOR UPDATE")
        .bind(&list.uri)
        .fetch_optional(conn)
        .await?;
    Ok(row.is_some_and(|(state, activated_at)| state == "active" && activated_at == list.activated_at))
}

/// A database error the statement itself causes (bad data, a constraint):
/// it would fail again, so it is counted and passed over.
fn deterministic(error: &sqlx::Error) -> bool {
    error
        .as_database_error()
        .and_then(sqlx::error::DatabaseError::code)
        .is_some_and(|code| code.starts_with("22") || code.starts_with("23"))
}

/// Step 3a: one transaction per list. Run only when the listing of list
/// records completed. Returns the lists whose record was **listed**: in the
/// listing, and written without error.
///
/// For each list, the first case that fits:
/// 1. an ingest write newer than the listing landed during the run: nothing;
/// 2. the record was listed: its purpose, at the listing's revision;
/// 3. not listed, for the first time: remember since when;
/// 4. not listed again, a day or more later: the known purpose is cleared.
///
/// # Errors
///
/// [`Stopped`].
pub async fn write_purposes(pool: &PgPool, snap: &Snapshot, listing: &Listing) -> Result<HashSet<String>, Stopped> {
    let mut listed = HashSet::new();
    let Some(records) = &listing.lists else {
        return Ok(listed);
    };
    for list in &snap.lists {
        let record = records.get(&list.uri);
        let written: Result<bool, sqlx::Error> = async {
            let mut tx = pool.begin().await?;
            if !current(&mut tx, snap).await? {
                return Ok(false);
            }
            if !same_instance(&mut tx, list).await? {
                tx.commit().await?;
                return Ok(true);
            }
            let (purpose_rev,): (Option<String>,) = sqlx::query_as("SELECT purpose_rev FROM subscribed_lists WHERE list_uri = $1")
                .bind(&list.uri)
                .fetch_one(&mut *tx)
                .await?;
            let newer_ingest = purpose_rev != list.purpose_rev && purpose_rev.as_deref().is_some_and(|rev| rev > listing.rev.as_str());
            if !newer_ingest {
                match record {
                    Some(purpose) => {
                        sqlx::query("UPDATE subscribed_lists SET purpose = $2, purpose_rev = $3, unlisted_since = NULL WHERE list_uri = $1")
                            .bind(&list.uri)
                            .bind(storable(purpose.as_deref()))
                            .bind(&listing.rev)
                            .execute(&mut *tx)
                            .await?;
                    }
                    None => {
                        sqlx::query(
                            "UPDATE subscribed_lists SET \
                               purpose_rev = CASE WHEN unlisted_since <= now() - make_interval(hours => $3) AND purpose IS NOT NULL \
                                             THEN $2 ELSE purpose_rev END, \
                               purpose = CASE WHEN unlisted_since <= now() - make_interval(hours => $3) THEN NULL ELSE purpose END, \
                               unlisted_since = COALESCE(unlisted_since, now()) \
                             WHERE list_uri = $1",
                        )
                        .bind(&list.uri)
                        .bind(&listing.rev)
                        .bind(UNLISTED_CLEARS_AFTER_HOURS)
                        .execute(&mut *tx)
                        .await?;
                    }
                }
            }
            tx.commit().await?;
            Ok(true)
        }
        .await;
        match written {
            Ok(true) => {
                if record.is_some() {
                    listed.insert(list.uri.clone());
                }
            }
            Ok(false) => return Err(Stopped::Superseded),
            Err(error) if deterministic(&error) => {
                metrics::counter!(METRIC_PURPOSE_WRITE_ERRORS).increment(1);
            }
            Err(error) => return Err(error.into()),
        }
    }
    Ok(listed)
}

/// What one batch does with a URI.
enum Act<'a> {
    /// Listed under this list outside the kept set, or absent on a first
    /// sync: an internal delete.
    Delete,
    /// Listed under a list that is not subscribed: delete, and remember the
    /// move so an older copy cannot bring the item back.
    MovedOut,
    /// Not listed at all: counted, and deleted the second time running.
    Missing,
    /// In the kept set.
    Keep(&'a ListedItem),
}

/// Step 4: bring the copy of each list in `S0` in line with the listing,
/// in batches of at most [`BATCH`] URIs. Returns the lists that were
/// **served**: active and the same row instance at every batch.
///
/// A stored row at the listing's revision or newer is never touched. With
/// a listing that was cut short, items are added and nothing is removed.
///
/// # Errors
///
/// [`Stopped`].
pub async fn reconcile(pool: &PgPool, snap: &Snapshot, listing: &Listing, limits: Limits) -> Result<HashSet<String>, Stopped> {
    let max_items = usize::try_from(limits.max_items).unwrap_or(usize::MAX);
    let in_snapshot: HashSet<&str> = snap.lists.iter().map(|l| l.uri.as_str()).collect();
    // Where each listed item is now, and each list's items by record key.
    let listed_under: HashMap<&str, &str> = listing.items.iter().map(|i| (i.uri.as_str(), i.list_uri.as_str())).collect();
    let mut by_list: HashMap<&str, Vec<&ListedItem>> = HashMap::new();
    for item in &listing.items {
        by_list.entry(item.list_uri.as_str()).or_default().push(item);
    }
    let mut served = HashSet::new();
    for list in &snap.lists {
        let mut kept: Vec<&ListedItem> = by_list.remove(list.uri.as_str()).unwrap_or_default();
        kept.sort_unstable_by(|a, b| a.rkey.cmp(&b.rkey));
        kept.truncate(max_items);
        let kept_uris: HashSet<&str> = kept.iter().map(|i| i.uri.as_str()).collect();
        let stored: Vec<(String, String)> = sqlx::query_as("SELECT uri, rev FROM subscribed_list_items WHERE list_uri = $1")
            .bind(&list.uri)
            .fetch_all(pool)
            .await?;
        let mut acts: Vec<(&str, Act<'_>)> = kept.iter().map(|i| (i.uri.as_str(), Act::Keep(i))).collect();
        if listing.items_complete {
            for (uri, rev) in &stored {
                if rev.as_str() >= listing.rev.as_str() || kept_uris.contains(uri.as_str()) {
                    continue;
                }
                let act = match listed_under.get(uri.as_str()) {
                    // Past the cap of this list, or of another subscribed
                    // list whose kept set it is not in.
                    Some(now) if *now == list.uri || in_snapshot.contains(now) => {
                        // A move into another list's kept set is that
                        // list's upsert; here only what no list keeps.
                        if *now != list.uri && by_list.get(now).is_some_and(|items| keeps(items, uri, max_items)) {
                            continue;
                        }
                        Act::Delete
                    }
                    Some(_) => Act::MovedOut,
                    None if list.first_sync => Act::Delete,
                    None => Act::Missing,
                };
                acts.push((uri.as_str(), act));
            }
        }
        acts.sort_unstable_by(|a, b| a.0.cmp(b.0));
        let mut whole = true;
        for batch in acts.chunks(BATCH) {
            let mut tx = pool.begin().await?;
            let uris: Vec<String> = batch.iter().map(|(uri, _)| (*uri).to_string()).collect();
            lock_uris(&mut tx, &uris).await?;
            if !same_instance(&mut tx, list).await? {
                whole = false;
                break;
            }
            if !current(&mut tx, snap).await? {
                return Err(Stopped::Superseded);
            }
            write_batch(&mut tx, snap, list, listing, batch).await?;
            tx.commit().await?;
        }
        if whole {
            served.insert(list.uri.clone());
        }
    }
    // The tombstones the batches wrote for moved-out items count against
    // the owner's cap like any other: the oldest past it go.
    let evicted = sqlx::query(
        "DELETE FROM subscription_tombstones WHERE owner_did = $1 AND uri IN ( \
           SELECT uri FROM subscription_tombstones WHERE owner_did = $1 ORDER BY deleted_at DESC, uri OFFSET $2)",
    )
    .bind(&snap.owner)
    .bind(i64::from(limits.tombstones_max_per_owner))
    .execute(pool)
    .await?
    .rows_affected();
    if evicted > 0 {
        metrics::counter!(METRIC_TOMBSTONES_EVICTED).increment(evicted);
    }
    Ok(served)
}

/// Whether `uri` is among the `max` smallest record keys of `items`.
fn keeps(items: &[&ListedItem], uri: &str, max: usize) -> bool {
    let Some(item) = items.iter().find(|i| i.uri == uri) else {
        return false;
    };
    items.iter().filter(|i| i.rkey < item.rkey).count() < max
}

async fn write_batch(
    conn: &mut PgConnection,
    snap: &Snapshot,
    list: &SnapList,
    listing: &Listing,
    batch: &[(&str, Act<'_>)],
) -> Result<(), sqlx::Error> {
    let of = |want: fn(&Act<'_>) -> bool| -> Vec<&str> { batch.iter().filter(|(_, act)| want(act)).map(|(uri, _)| *uri).collect() };
    let deleted: Vec<&str> = of(|a| matches!(a, Act::Delete | Act::MovedOut));
    let moved: Vec<&str> = of(|a| matches!(a, Act::MovedOut));
    let missing: Vec<&str> = of(|a| matches!(a, Act::Missing));
    // Internal deletes: no public tombstone, nothing emitted.
    sqlx::query("DELETE FROM subscribed_list_items WHERE uri = ANY($1) AND rev < $2")
        .bind(&deleted)
        .bind(&listing.rev)
        .execute(&mut *conn)
        .await?;
    sqlx::query(
        "INSERT INTO subscription_tombstones (uri, owner_did, rev) SELECT u, $2, $3 FROM unnest($1::text[]) u \
         ON CONFLICT (uri) DO UPDATE SET rev = GREATEST(subscription_tombstones.rev, EXCLUDED.rev), deleted_at = now()",
    )
    .bind(&moved)
    .bind(&snap.owner)
    .bind(&listing.rev)
    .execute(&mut *conn)
    .await?;
    // Absent from two complete listings running.
    sqlx::query("UPDATE subscribed_list_items SET missing_count = missing_count + 1 WHERE uri = ANY($1) AND rev < $2")
        .bind(&missing)
        .bind(&listing.rev)
        .execute(&mut *conn)
        .await?;
    sqlx::query("DELETE FROM subscribed_list_items WHERE uri = ANY($1) AND rev < $2 AND missing_count >= 2")
        .bind(&missing)
        .bind(&listing.rev)
        .execute(&mut *conn)
        .await?;
    // The kept set, last writer wins at the listing's revision. An item
    // deleted publicly, or moved or deleted after the listing, is skipped.
    let kept: Vec<&ListedItem> = batch.iter().filter_map(|(_, act)| if let Act::Keep(item) = act { Some(*item) } else { None }).collect();
    let column = |pick: fn(&ListedItem) -> &str| -> Vec<&str> { kept.iter().map(|i| pick(i)).collect() };
    let uris = column(|i| &i.uri);
    sqlx::query(
        "INSERT INTO subscribed_list_items (uri, list_uri, owner_did, rkey, cid, rev, subject_did) \
         SELECT k.uri, $5, $6, k.rkey, k.cid, $7, k.subject FROM unnest($1::text[], $2::text[], $3::text[], $4::text[]) AS k(uri, rkey, cid, subject) \
         WHERE NOT EXISTS (SELECT 1 FROM record_tombstones t WHERE t.uri = k.uri) \
           AND NOT EXISTS (SELECT 1 FROM subscription_tombstones t WHERE t.uri = k.uri AND t.rev > $7) \
         ON CONFLICT (uri) DO UPDATE SET list_uri = EXCLUDED.list_uri, rkey = EXCLUDED.rkey, cid = EXCLUDED.cid, rev = EXCLUDED.rev, \
           subject_did = EXCLUDED.subject_did, missing_count = 0 \
         WHERE subscribed_list_items.rev < EXCLUDED.rev",
    )
    .bind(&uris)
    .bind(column(|i| &i.rkey))
    .bind(column(|i| &i.cid))
    .bind(column(|i| &i.subject_did))
    .bind(&list.uri)
    .bind(&snap.owner)
    .bind(&listing.rev)
    .execute(&mut *conn)
    .await?;
    // A row listed again is no longer missing, whatever its revision.
    sqlx::query("UPDATE subscribed_list_items SET missing_count = 0 WHERE uri = ANY($1) AND missing_count > 0")
        .bind(&uris)
        .execute(conn)
        .await?;
    Ok(())
}

/// Step 5, for a run whose two listings both completed: one transaction,
/// in lock order (list rows, the owner's row, then the references).
///
/// - Each list still active, the same instance and served gets its count
///   from the rows, its `truncated` flag and `synced_at`.
/// - The owner's row records the completion and clears every backoff. A
///   resync request is cleared only if it is the one the run began with.
/// - A waiting reference whose list was served and whose list record was
///   listed takes effect, if it is still the same admission. One whose
///   record was not listed is stamped, once: it waits for the owner's next
///   run and is no cause for one.
///
/// The job row is settled in the same transaction, after the owner's row:
/// `done`, or queued again when a rerun was asked for during the run.
///
/// # Errors
///
/// [`Stopped`].
pub async fn complete(
    pool: &PgPool,
    snap: &Snapshot,
    listing: &Listing,
    served: &HashSet<String>,
    listed: &HashSet<String>,
    max_items: u32,
    min_interval_secs: u32,
) -> Result<(), Stopped> {
    let mut tx = pool.begin().await?;
    let mut done: HashSet<&str> = HashSet::new();
    for list in &snap.lists {
        if !same_instance(&mut tx, list).await? || !served.contains(&list.uri) {
            continue;
        }
        let total = listing.items.iter().filter(|i| i.list_uri == list.uri).count();
        sqlx::query(
            "UPDATE subscribed_lists SET item_count = (SELECT count(*) FROM subscribed_list_items WHERE list_uri = $1), \
               truncated = $2, synced_at = now() WHERE list_uri = $1",
        )
        .bind(&list.uri)
        .bind(total > usize::try_from(max_items).unwrap_or(usize::MAX))
        .execute(&mut *tx)
        .await?;
        done.insert(list.uri.as_str());
    }
    sqlx::query(
        "INSERT INTO subscribed_owners AS o (owner_did, last_completed_at, last_run_ended_at) VALUES ($1, now(), now()) \
         ON CONFLICT (owner_did) DO UPDATE SET last_completed_at = now(), last_run_ended_at = now(), \
           gone_suspect_at = NULL, gone_suspect_pds = NULL, owner_gone = false, slow_until = NULL, cut_until = NULL, \
           resync_requested_at = CASE WHEN o.resync_requested_at IS NOT DISTINCT FROM $2 THEN NULL ELSE o.resync_requested_at END",
    )
    .bind(&snap.owner)
    .bind(snap.resync_requested_at)
    .execute(&mut *tx)
    .await?;
    // In primary-key order, the order the member phase writes them in.
    for waiting in &snap.waiting {
        let statement = if !listed.contains(&waiting.list) {
            "UPDATE list_references SET unlisted_at = now() \
             WHERE member_did = $1 AND list_uri = $2 AND source = $3 AND admitted AND admitted_at IS NOT DISTINCT FROM $4 AND unlisted_at IS NULL"
        } else if done.contains(waiting.list.as_str()) {
            "UPDATE list_references SET effective = true \
             WHERE member_did = $1 AND list_uri = $2 AND source = $3 AND admitted AND admitted_at IS NOT DISTINCT FROM $4"
        } else {
            continue;
        };
        sqlx::query(statement)
            .bind(&waiting.member)
            .bind(&waiting.list)
            .bind(&waiting.source)
            .bind(waiting.admitted_at)
            .execute(&mut *tx)
            .await?;
    }
    // The job row last (lock step 6). A run that was superseded settles
    // nothing, and none of the above is kept.
    if !jobs::settle(&mut tx, &snap.owner, snap.generation, jobs::RunEnd::Done, min_interval_secs).await? {
        return Err(Stopped::Superseded);
    }
    tx.commit().await?;
    Ok(())
}

/// Items deleted per transaction when an owner is gone.
const GONE_BATCH: i64 = 10_000;

/// The owner-gone completion: the owner's account no longer exists, so its
/// lists are emptied and kept empty.
///
/// - Delete batches: each list's copied items, 10 000 per short
///   transaction, stopping if the job was superseded.
/// - A final transaction: every list gets its count, `synced_at`, and no
///   purpose (a gone owner's lists are not moderation lists); the owner's
///   row says it is gone; the waiting references become effective, on
///   empty lists, so nothing waits on an owner that will never answer; and
///   the job ends.
///
/// # Errors
///
/// [`Stopped`].
pub async fn owner_gone(pool: &PgPool, snap: &Snapshot, min_interval_secs: u32) -> Result<(), Stopped> {
    for list in &snap.lists {
        loop {
            let mut tx = pool.begin().await?;
            if !same_instance(&mut tx, list).await? {
                break;
            }
            if !current(&mut tx, snap).await? {
                return Err(Stopped::Superseded);
            }
            let deleted = sqlx::query(
                "DELETE FROM subscribed_list_items WHERE ctid IN (SELECT ctid FROM subscribed_list_items WHERE list_uri = $1 LIMIT $2)",
            )
            .bind(&list.uri)
            .bind(GONE_BATCH)
            .execute(&mut *tx)
            .await?
            .rows_affected();
            tx.commit().await?;
            if deleted == 0 {
                break;
            }
        }
    }
    let mut tx = pool.begin().await?;
    let mut done: HashSet<&str> = HashSet::new();
    for list in &snap.lists {
        if !same_instance(&mut tx, list).await? {
            continue;
        }
        sqlx::query(
            "UPDATE subscribed_lists SET item_count = (SELECT count(*) FROM subscribed_list_items WHERE list_uri = $1), \
               synced_at = now(), purpose = NULL, purpose_rev = NULL, unlisted_since = NULL WHERE list_uri = $1",
        )
        .bind(&list.uri)
        .execute(&mut *tx)
        .await?;
        done.insert(list.uri.as_str());
    }
    // The suspicion's start is kept: it says since when.
    sqlx::query(
        "INSERT INTO subscribed_owners (owner_did, owner_gone, last_completed_at, last_run_ended_at) VALUES ($1, true, now(), now()) \
         ON CONFLICT (owner_did) DO UPDATE SET owner_gone = true, last_completed_at = now(), last_run_ended_at = now()",
    )
    .bind(&snap.owner)
    .execute(&mut *tx)
    .await?;
    for waiting in snap.waiting.iter().filter(|w| done.contains(w.list.as_str())) {
        sqlx::query(
            "UPDATE list_references SET effective = true \
             WHERE member_did = $1 AND list_uri = $2 AND source = $3 AND admitted AND admitted_at IS NOT DISTINCT FROM $4",
        )
        .bind(&waiting.member)
        .bind(&waiting.list)
        .bind(&waiting.source)
        .bind(waiting.admitted_at)
        .execute(&mut *tx)
        .await?;
    }
    if !jobs::settle(&mut tx, &snap.owner, snap.generation, jobs::RunEnd::Done, min_interval_secs).await? {
        return Err(Stopped::Superseded);
    }
    tx.commit().await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::subscriptions::list_phase;
    use crate::subscriptions::references::{Budget, mute_list};
    use crate::test_support::fresh_db;

    const ME: &str = "did:plc:mmmmmmmmmmmmmmmmmmmmmmmm";
    const OWNER: &str = "did:plc:aaaaaaaaaaaaaaaaaaaaaaaa";
    const MODLIST: &str = "app.bsky.graph.defs#modlist";
    const BUDGET: Budget = Budget { max_references: 5, max_owners: 5 };

    fn list(n: u8) -> String {
        format!("at://{OWNER}/app.bsky.graph.list/3klist{n}")
    }

    fn item(rkey: &str, list: &str) -> ListedItem {
        ListedItem {
            uri: format!("at://{OWNER}/app.bsky.graph.listitem/{rkey}"),
            rkey: rkey.to_string(),
            cid: format!("bafy{rkey}"),
            list_uri: list.to_string(),
            subject_did: format!("did:plc:subject{rkey}"),
        }
    }

    fn listing(rev: &str, lists: &[(&String, Option<&str>)], items: Vec<ListedItem>) -> Listing {
        Listing {
            rev: rev.to_string(),
            lists: Some(lists.iter().map(|(uri, purpose)| ((*uri).clone(), purpose.map(str::to_string))).collect()),
            items,
            items_complete: true,
        }
    }

    async fn subscribe(pool: &PgPool, list: &str) {
        mute_list(pool, ME, list, BUDGET).await.expect("mute");
        list_phase::apply(pool, list, 600).await.expect("list phase");
    }

    /// A whole run over `listing`, as the job will make it.
    async fn run(pool: &PgPool, listing: &Listing, max_items: u32) -> Snapshot {
        // As a claim leaves it.
        let generation: i64 = sqlx::query_scalar("UPDATE backfill_jobs SET state = 'running' WHERE did = $1 AND depth = 'list_sync' RETURNING generation")
            .bind(OWNER)
            .fetch_one(pool)
            .await
            .expect("job");
        let snap = snapshot(pool, OWNER, generation).await.expect("snapshot");
        let listed = write_purposes(pool, &snap, listing).await.expect("purposes");
        let served = reconcile(pool, &snap, listing, Limits { max_items, tombstones_max_per_owner: 10_000 }).await.expect("reconcile");
        if listing.items_complete && listing.lists.is_some() {
            complete(pool, &snap, listing, &served, &listed, max_items, 600).await.expect("complete");
        } else {
            jobs::end_run(pool, OWNER, generation, jobs::RunEnd::Requeue, Some(jobs::Cut::Large), 600).await.expect("cut");
        }
        snap
    }

    async fn copied(pool: &PgPool, list: &str) -> Vec<String> {
        sqlx::query_scalar("SELECT rkey FROM subscribed_list_items WHERE list_uri = $1 ORDER BY rkey")
            .bind(list)
            .fetch_all(pool)
            .await
            .expect("items")
    }

    async fn reference(pool: &PgPool, list: &str) -> (bool, bool) {
        sqlx::query_as("SELECT effective, unlisted_at IS NOT NULL FROM list_references WHERE member_did = $1 AND list_uri = $2")
            .bind(ME)
            .bind(list)
            .fetch_one(pool)
            .await
            .expect("reference")
    }

    async fn list_row(pool: &PgPool, list: &str) -> (Option<String>, i32, bool, bool) {
        sqlx::query_as("SELECT purpose, item_count, truncated, synced_at IS NOT NULL FROM subscribed_lists WHERE list_uri = $1")
            .bind(list)
            .fetch_one(pool)
            .await
            .expect("row")
    }

    /// B-88: a first run copies the kept set, records the purpose and makes
    /// the waiting mute effective; later runs trim, move, and remove an
    /// absent item only at its second absence.
    #[tokio::test]
    async fn a_run_brings_the_copy_in_line() {
        let Some(db) = fresh_db("subscriptions_sync").await else {
            return;
        };
        let p = &db.pool;
        let (a, b) = (list(1), list(2));
        subscribe(p, &a).await;
        assert_eq!(reference(p, &a).await, (false, false));

        // Three items under the list, one under a list nobody subscribes
        // to; the cap is two, so the two smallest record keys are kept.
        let first = listing("3kaaa1", &[(&a, Some(MODLIST))], vec![item("3k1", &a), item("3k3", &a), item("3k2", &a), item("3k9", &b)]);
        let snap = run(p, &first, 2).await;
        assert_eq!((snap.lists.len(), snap.waiting.len(), snap.lists[0].first_sync), (1, 1, true));
        assert_eq!(copied(p, &a).await, ["3k1", "3k2"]);
        assert_eq!(list_row(p, &a).await, (Some(MODLIST.to_string()), 2, true, true));
        assert_eq!(reference(p, &a).await, (true, false));
        let owner: (bool, bool) = sqlx::query_as("SELECT last_completed_at IS NOT NULL, last_run_ended_at IS NOT NULL FROM subscribed_owners")
            .fetch_one(p)
            .await
            .expect("owner");
        assert_eq!(owner, (true, true));

        // The owner removed 3k1: a later complete listing counts it missing
        // once, and removes it the second time. 3k3 now fits and is copied.
        let second = listing("3kaaa2", &[(&a, Some(MODLIST))], vec![item("3k2", &a), item("3k3", &a)]);
        run(p, &second, 2).await;
        assert_eq!(copied(p, &a).await, ["3k1", "3k2", "3k3"]);
        let third = listing("3kaaa3", &[(&a, Some(MODLIST))], vec![item("3k2", &a), item("3k3", &a)]);
        run(p, &third, 2).await;
        assert_eq!(copied(p, &a).await, ["3k2", "3k3"]);
        assert_eq!(list_row(p, &a).await.1, 2);

        // An item moved to a list that is not subscribed is removed at
        // once, and remembered, so an older listing cannot bring it back.
        let moved = listing("3kaaa4", &[(&a, Some(MODLIST))], vec![item("3k2", &a), item("3k3", &b)]);
        run(p, &moved, 2).await;
        assert_eq!(copied(p, &a).await, ["3k2"]);
        run(p, &third, 2).await;
        assert_eq!(copied(p, &a).await, ["3k2"], "an older listing is skipped for a remembered move");

        // A listing that was cut short adds and removes nothing else, and
        // completes nothing.
        let cut = Listing { items_complete: false, ..listing("3kaaa5", &[(&a, Some(MODLIST))], vec![item("3k5", &a)]) };
        sqlx::query("UPDATE subscribed_lists SET synced_at = NULL").execute(p).await.expect("unsynced");
        run(p, &cut, 5).await;
        assert_eq!(copied(p, &a).await, ["3k2", "3k5"]);
        assert!(!list_row(p, &a).await.3);
        db.drop_db().await;
    }

    /// B-88: purposes. A list whose record is not listed does not take
    /// effect and is stamped once; a known purpose survives one listing
    /// without the record and is cleared by a second a day later; a
    /// re-admission during the run is not served by it.
    #[tokio::test]
    async fn a_reference_takes_effect_only_with_its_list_record() {
        let Some(db) = fresh_db("subscriptions_sync_purpose").await else {
            return;
        };
        let p = &db.pool;
        let (a, b) = (list(1), list(2));
        subscribe(p, &a).await;
        subscribe(p, &b).await;
        // Only A's record is listed; B's items are, its record is not.
        let first = listing("3kaaa1", &[(&a, Some("app.bsky.graph.defs#curatelist"))], vec![item("3k1", &a), item("3k2", &b)]);
        run(p, &first, 10).await;
        // A takes effect (and is dormant: not a moderation list). B is
        // served, its items copied, but it waits, stamped.
        assert_eq!(reference(p, &a).await, (true, false));
        assert_eq!(list_row(p, &a).await.0.as_deref(), Some("app.bsky.graph.defs#curatelist"));
        assert_eq!(reference(p, &b).await, (false, true));
        assert_eq!((copied(p, &b).await, list_row(p, &b).await.3), (vec!["3k2".to_string()], true));
        let unlisted: bool = sqlx::query_scalar("SELECT unlisted_since IS NOT NULL FROM subscribed_lists WHERE list_uri = $1").bind(&b).fetch_one(p).await.expect("since");
        assert!(unlisted);
        // A stamped reference is not owed a run: the list phase queues none.
        let applied = list_phase::apply(p, &b, 600).await.expect("apply");
        assert_eq!(applied, list_phase::Applied::Done { state: Some(list_phase::ListState::Active), waiting: Vec::new() });

        // The next run lists B's record: it takes effect.
        let second = listing("3kaaa2", &[(&a, Some(MODLIST)), (&b, Some(MODLIST))], vec![item("3k1", &a), item("3k2", &b)]);
        run(p, &second, 10).await;
        assert!(reference(p, &b).await.0);
        assert_eq!(list_row(p, &a).await.0.as_deref(), Some(MODLIST));

        // A's record goes missing: the purpose stands after one listing, and
        // after a second one within a day; a day later it is cleared.
        let gone = listing("3kaaa3", &[(&b, Some(MODLIST))], vec![item("3k1", &a), item("3k2", &b)]);
        run(p, &gone, 10).await;
        run(p, &Listing { rev: "3kaaa4".into(), ..gone.clone() }, 10).await;
        assert_eq!(list_row(p, &a).await.0.as_deref(), Some(MODLIST));
        sqlx::query("UPDATE subscribed_lists SET unlisted_since = now() - interval '25 hours' WHERE list_uri = $1").bind(&a).execute(p).await.expect("age");
        run(p, &Listing { rev: "3kaaa5".into(), ..gone.clone() }, 10).await;
        assert_eq!(list_row(p, &a).await.0, None);
        // An oversized or absent purpose is stored as none.
        let long = "x".repeat(600);
        let odd = listing("3kaaa6", &[(&a, Some(long.as_str())), (&b, None)], vec![]);
        run(p, &odd, 10).await;
        assert_eq!((list_row(p, &a).await.0, list_row(p, &b).await.0), (None, None));

        // A reference admitted again while a run is under way is not made
        // effective by that run: it is another admission.
        let generation: i64 = sqlx::query_scalar("UPDATE backfill_jobs SET state = 'running' RETURNING generation").fetch_one(p).await.expect("job");
        sqlx::query("UPDATE list_references SET effective = false WHERE list_uri = $1").bind(&a).execute(p).await.expect("waiting");
        let snap = snapshot(p, OWNER, generation).await.expect("snapshot");
        sqlx::query("UPDATE list_references SET admitted_at = now() + interval '1 second' WHERE list_uri = $1").bind(&a).execute(p).await.expect("re-admitted");
        let again = listing("3kaaa7", &[(&a, Some(MODLIST)), (&b, Some(MODLIST))], vec![]);
        let listed = write_purposes(p, &snap, &again).await.expect("purposes");
        let served = reconcile(p, &snap, &again, Limits { max_items: 10, tombstones_max_per_owner: 10_000 }).await.expect("reconcile");
        complete(p, &snap, &again, &served, &listed, 10, 600).await.expect("complete");
        assert!(!reference(p, &a).await.0);
        let state: String = sqlx::query_scalar("SELECT state FROM backfill_jobs").fetch_one(p).await.expect("state");
        assert_eq!(state, "done");
        // A superseded run stops, and its completion keeps nothing.
        sqlx::query("UPDATE backfill_jobs SET state = 'running', generation = generation + 1").execute(p).await.expect("bump");
        assert!(matches!(write_purposes(p, &snap, &again).await, Err(Stopped::Superseded)));
        sqlx::query("UPDATE subscribed_lists SET synced_at = NULL").execute(p).await.expect("unsynced");
        assert!(matches!(complete(p, &snap, &again, &served, &listed, 10, 600).await, Err(Stopped::Superseded)));
        assert!(!list_row(p, &a).await.3);
        db.drop_db().await;
    }
}
