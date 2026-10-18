//! The owner-side rules of rule 4 (design-v3 §4.3 "The copy rule", "The
//! purpose rule"): what a write by the owner of a subscribed list does to
//! the subscription tables.
//!
//! They run on writes whose caller sets `subscription_rules` (the
//! firehose, backfill, dead-letter replay; not `requestRecord`), for a
//! repo in `O_sub`, and independently of admission: the op itself may be
//! written to the public tables, refused, or fail validation.
//!
//! - **Copy rule.** A list item of a subscribed list is copied into
//!   `subscribed_list_items`. A delete, or an update that moves the item
//!   out of every subscribed list, removes the copy and leaves a tombstone
//!   in `subscription_tombstones`, so an older create arriving late (from
//!   a lagging relay, or a listing taken before the delete) is not copied
//!   again.
//! - **Purpose rule.** A list record's `purpose` is kept on its
//!   `subscribed_lists` row, since a member's own list mute or block works
//!   on a moderation list only.
//!
//! Neither changes the op's outcome. Every function here writes inside
//! the caller's savepoint.

use std::collections::HashMap;

use serde_json::Value;
use sqlx::PgConnection;

use super::fetch::NSID_LIST;
use super::sync::storable;
use crate::aggregations::ScopeSets;
use crate::db::locks::uri_author;

pub const METRIC_OVER_CAP: &str = "list_items_over_cap_total";
pub const METRIC_TOMBSTONES_EVICTED: &str = "subscription_tombstones_evicted_total";
/// `list_purpose_write_errors_total{path = "ingest" | "sync"}`.
pub const METRIC_PURPOSE_ERRORS: &str = "list_purpose_write_errors_total";
/// `list_purpose_writes_total{path = "ingest" | "sync", result = "modlist" |
/// "other" | "none"}`: each purpose written to a `subscribed_lists` row, by
/// the purpose stored (design-v4 §4.3 "List purpose").
pub const METRIC_PURPOSE_WRITES: &str = "list_purpose_writes_total";

/// The `result` label of a stored purpose: `modlist`, `other`, or `none`
/// for NULL.
#[must_use]
pub fn purpose_result(stored: Option<&str>) -> &'static str {
    match stored {
        Some(crate::hydration::lists::PURPOSE_MOD) => "modlist",
        Some(_) => "other",
        None => "none",
    }
}
pub const METRIC_COPY_ERRORS: &str = "list_copy_write_errors_total";

/// The caps the rules apply.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Limits {
    /// `lists.subscribed_max_items`.
    pub max_items: u32,
    /// `lists.subscription_tombstones_max_per_owner`.
    pub tombstones_max_per_owner: u32,
}

/// What one commit's owner-side writes did, for the checks of its later
/// ops and for the in-memory sets after it commits.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct Effects {
    /// Items newly copied, by list.
    pub created: HashMap<String, u32>,
    /// Tombstone rows inserted less rows evicted, by owner.
    pub tombstones: HashMap<String, i64>,
    /// Lists whose copy changed: their counts are read again after commit.
    pub touched: Vec<String>,
}

impl Effects {
    pub fn merge(&mut self, other: Self) {
        for (list, n) in other.created {
            *self.created.entry(list).or_default() += n;
        }
        for (owner, n) in other.tombstones {
            *self.tombstones.entry(owner).or_default() += n;
        }
        self.touched.extend(other.touched);
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.created.is_empty() && self.tombstones.is_empty() && self.touched.is_empty()
    }
}

/// What a rule reads besides the op: the in-memory sets, what the commit's
/// earlier ops did, and the caps.
#[derive(Debug, Clone, Copy)]
pub struct Ctx<'a> {
    pub sets: &'a ScopeSets,
    pub earlier: &'a Effects,
    pub limits: Limits,
}

/// The list and subject of a list item record, if the item is the owner's
/// own (its list is in the same repo) and well formed.
fn item_fields<'a>(owner: &str, record: &'a Value) -> Option<(&'a str, &'a str)> {
    let list = record.get("list")?.as_str()?;
    let subject = record.get("subject")?.as_str()?;
    let own = uri_author(list) == Some(owner) && list.split('/').nth(3) == Some(NSID_LIST);
    (own && proto_blue::syntax::Did::is_valid(subject)).then_some((list, subject))
}

/// Write a tombstone for `uri` at `rev`, under the per-owner cap: when the
/// write adds a row and the owner is then over the cap, its oldest
/// tombstone is evicted in the same transaction.
async fn tombstone(conn: &mut PgConnection, ctx: Ctx<'_>, eff: &mut Effects, owner: &str, uri: &str, rev: &str) -> Result<(), sqlx::Error> {
    let Ctx { sets, earlier, limits } = ctx;
    let inserted: bool = sqlx::query_scalar(
        "INSERT INTO subscription_tombstones (uri, owner_did, rev) VALUES ($1, $2, $3) \
         ON CONFLICT (uri) DO UPDATE SET rev = GREATEST(subscription_tombstones.rev, EXCLUDED.rev), deleted_at = now() \
         RETURNING (xmax = 0)",
    )
    .bind(uri)
    .bind(owner)
    .bind(rev)
    .fetch_one(&mut *conn)
    .await?;
    if !inserted {
        return Ok(());
    }
    let held = i64::from(sets.subscribed.owners.get(owner).copied().unwrap_or(0))
        + earlier.tombstones.get(owner).copied().unwrap_or(0)
        + eff.tombstones.get(owner).copied().unwrap_or(0)
        + 1;
    let mut delta = 1;
    if held > i64::from(limits.tombstones_max_per_owner) {
        let evicted = sqlx::query(
            "DELETE FROM subscription_tombstones WHERE uri = \
               (SELECT uri FROM subscription_tombstones WHERE owner_did = $1 ORDER BY deleted_at LIMIT 1)",
        )
        .bind(owner)
        .execute(&mut *conn)
        .await?
        .rows_affected();
        metrics::counter!(METRIC_TOMBSTONES_EVICTED).increment(evicted);
        delta -= i64::try_from(evicted).unwrap_or(0);
    }
    *eff.tombstones.entry(owner.to_string()).or_default() += delta;
    Ok(())
}

/// Remove the copy of `uri` if it is no newer than `rev`. Returns the list
/// it was copied under.
async fn remove(conn: &mut PgConnection, uri: &str, rev: &str) -> Result<Option<String>, sqlx::Error> {
    sqlx::query_scalar("DELETE FROM subscribed_list_items WHERE uri = $1 AND rev <= $2 RETURNING list_uri")
        .bind(uri)
        .bind(rev)
        .fetch_optional(conn)
        .await
}

/// One list item to copy.
#[derive(Debug, Clone, Copy)]
pub struct Item<'a> {
    pub owner: &'a str,
    pub uri: &'a str,
    pub rkey: &'a str,
    pub cid: &'a str,
    pub rev: &'a str,
    pub record: &'a Value,
}

/// The copy rule for a create or update of a list item by `owner`, a repo
/// in `O_sub`.
///
/// # Errors
///
/// Database errors.
pub async fn item_upsert(conn: &mut PgConnection, ctx: Ctx<'_>, item: Item<'_>) -> Result<Effects, sqlx::Error> {
    let Ctx { sets, earlier, limits } = ctx;
    let mut eff = Effects::default();
    let Item { owner, uri, rkey, cid, rev, record } = item;
    // An op no newer than the item's tombstone is stale: the delete or the
    // move it records came after it.
    let stale: bool = sqlx::query_scalar("SELECT EXISTS (SELECT 1 FROM subscription_tombstones WHERE uri = $1 AND rev >= $2)")
        .bind(uri)
        .bind(rev)
        .fetch_one(&mut *conn)
        .await?;
    if stale {
        return Ok(eff);
    }
    let target = item_fields(owner, record).filter(|(list, _)| sets.subscribed.is_active(list));
    let Some((list, subject)) = target else {
        // The item is in no subscribed list (any more): a stored copy was
        // moved out. An internal delete, with a tombstone at this rev so an
        // older listing cannot bring the item back under its old list.
        if let Some(old) = remove(conn, uri, rev).await? {
            tombstone(conn, ctx, &mut eff, owner, uri, rev).await?;
            eff.touched.push(old);
        }
        return Ok(eff);
    };
    // While the owner is being purged its creates and updates are not
    // copied; the listing at the purge's end repairs that.
    if sets.purge_kind(owner).is_some() {
        return Ok(eff);
    }
    let stored: Option<String> = sqlx::query_scalar("SELECT list_uri FROM subscribed_list_items WHERE uri = $1")
        .bind(uri)
        .fetch_optional(&mut *conn)
        .await?;
    // The cap refuses only a record that is not stored yet.
    if stored.is_none() {
        let held = u64::from(sets.subscribed.lists.get(list).copied().unwrap_or(0))
            + u64::from(earlier.created.get(list).copied().unwrap_or(0));
        if held >= u64::from(limits.max_items) {
            metrics::counter!(METRIC_OVER_CAP).increment(1);
            return Ok(eff);
        }
    }
    let written = sqlx::query(
        "INSERT INTO subscribed_list_items (uri, list_uri, owner_did, rkey, cid, rev, subject_did) VALUES ($1, $2, $3, $4, $5, $6, $7) \
         ON CONFLICT (uri) DO UPDATE SET list_uri = EXCLUDED.list_uri, rkey = EXCLUDED.rkey, cid = EXCLUDED.cid, rev = EXCLUDED.rev, \
           subject_did = EXCLUDED.subject_did, missing_count = 0 \
         WHERE subscribed_list_items.rev < EXCLUDED.rev",
    )
    .bind(uri)
    .bind(list)
    .bind(owner)
    .bind(rkey)
    .bind(cid)
    .bind(rev)
    .bind(subject)
    .execute(&mut *conn)
    .await?
    .rows_affected()
        > 0;
    if written {
        if stored.is_none() {
            *eff.created.entry(list.to_string()).or_default() += 1;
        }
        eff.touched.push(list.to_string());
        // A move between two subscribed lists changes both counts.
        eff.touched.extend(stored.filter(|old| old != list));
    }
    Ok(eff)
}

/// The copy rule for a delete of a list item by `owner`, a repo in
/// `O_sub`. A delete carries no body, so the copy is matched by URI.
///
/// # Errors
///
/// Database errors.
pub async fn item_delete(conn: &mut PgConnection, ctx: Ctx<'_>, owner: &str, uri: &str, rev: &str) -> Result<Effects, sqlx::Error> {
    let mut eff = Effects::default();
    if let Some(list) = remove(conn, uri, rev).await? {
        eff.touched.push(list);
    }
    tombstone(conn, ctx, &mut eff, owner, uri, rev).await?;
    Ok(eff)
}

/// What a list-record op says about the list's purpose.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ListOp<'a> {
    /// A create or update with a record body.
    Put(&'a Value),
    Delete,
}

/// The purpose rule for the list record `list_uri`, at `rev`. The row is
/// written only if its stored purpose is older than this op.
///
/// # Errors
///
/// Database errors.
pub async fn list_purpose(conn: &mut PgConnection, list_uri: &str, rev: &str, op: ListOp<'_>) -> Result<(), sqlx::Error> {
    let (purpose, seen) = match op {
        ListOp::Put(record) => (storable(record.get("purpose").and_then(Value::as_str)), true),
        ListOp::Delete => (None, false),
    };
    let written = sqlx::query(
        "UPDATE subscribed_lists SET purpose = $2, purpose_rev = $3, unlisted_since = CASE WHEN $4 THEN NULL ELSE unlisted_since END \
         WHERE list_uri = $1 AND (purpose_rev IS NULL OR purpose_rev < $3)",
    )
    .bind(list_uri)
    .bind(purpose)
    .bind(rev)
    .bind(seen)
    .execute(conn)
    .await?
    .rows_affected();
    if written > 0 {
        metrics::counter!(METRIC_PURPOSE_WRITES, "path" => "ingest", "result" => purpose_result(purpose)).increment(written);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;
    use crate::aggregations::PurgeKind;
    use crate::subscriptions::list_phase;
    use crate::subscriptions::references::{Budget, mute_list};
    use crate::test_support::fresh_db;

    const A: &str = "did:plc:aaaaaaaaaaaaaaaaaaaaaaaa";
    const M1: &str = "did:plc:mmmmmmmmmmmmmmmmmmmmmmm1";
    const SUBJECT: &str = "did:plc:ssssssssssssssssssssssss";
    const LIMITS: Limits = Limits { max_items: 2, tombstones_max_per_owner: 2 };

    fn list(n: u32) -> String {
        format!("at://{A}/app.bsky.graph.list/3klist{n}")
    }

    fn item_uri(n: u32) -> String {
        format!("at://{A}/app.bsky.graph.listitem/3kitem{n}")
    }

    fn ctx<'a>(sets: &'a ScopeSets, earlier: &'a Effects) -> Ctx<'a> {
        Ctx { sets, earlier, limits: LIMITS }
    }

    fn body(list_uri: &str) -> Value {
        json!({ "$type": "app.bsky.graph.listitem", "list": list_uri, "subject": SUBJECT, "createdAt": "2026-01-01T00:00:00Z" })
    }

    #[test]
    fn only_the_owners_own_well_formed_items_are_copied() {
        assert_eq!(item_fields(A, &body(&list(1))), Some((list(1).as_str(), SUBJECT)));
        // Someone else's list, a non-list URI, a bad subject, no subject.
        assert_eq!(item_fields(M1, &body(&list(1))), None);
        assert_eq!(item_fields(A, &body(&format!("at://{A}/app.bsky.feed.post/3k"))), None);
        assert_eq!(item_fields(A, &json!({ "list": list(1), "subject": "bob" })), None);
        assert_eq!(item_fields(A, &json!({ "list": list(1) })), None);
    }

    #[tokio::test]
    async fn the_copy_rule_and_the_purpose_rule() {
        let Some(db) = fresh_db("subscriptions_copy").await else {
            return;
        };
        let recorder = metrics_exporter_prometheus::PrometheusBuilder::new().build_recorder();
        let handle = recorder.handle();
        let _local = metrics::set_default_local_recorder(&recorder);
        let p = &db.pool;
        mute_list(p, M1, &list(1), Budget { max_references: 5, max_owners: 5 }).await.expect("mute");
        list_phase::apply(p, &list(1), 600).await.expect("apply");
        let mut sets = ScopeSets::default();
        sets.subscribed.lists.insert(list(1), 0);
        sets.subscribed.owners.insert(A.to_string(), 0);
        let mut conn = p.acquire().await.expect("conn");
        let count = |sql: &'static str| async move { sqlx::query_scalar::<_, i64>(sql).fetch_one(p).await.expect(sql) };
        let none = Effects::default();
        let put = |n: u32, rev: &'static str, to: String| (item_uri(n), rev, body(&to));

        // A create is copied, and the trigger counts it.
        let (uri, rev, record) = put(1, "3kb", list(1));
        let item = Item { owner: A, uri: &uri, rkey: "3kitem1", cid: "c1", rev, record: &record };
        let eff = item_upsert(&mut conn, ctx(&sets, &none), item).await.expect("copy");
        assert_eq!((eff.created.get(&list(1)), eff.touched.clone()), (Some(&1), vec![list(1)]));
        assert_eq!(count("SELECT item_count::bigint FROM subscribed_lists").await, 1);
        // An older write of the same item changes nothing.
        let old = Item { rev: "3ka", ..item };
        assert!(item_upsert(&mut conn, ctx(&sets, &none), old).await.expect("older").is_empty());

        // At the cap a new item is refused, an update of a stored one is
        // not. The count is the held one plus the batch's own creates.
        sets.subscribed.lists.insert(list(1), 1);
        let (uri2, _, record2) = put(2, "3kb", list(1));
        let second = Item { owner: A, uri: &uri2, rkey: "3kitem2", cid: "c2", rev: "3kb", record: &record2 };
        assert!(item_upsert(&mut conn, ctx(&sets, &eff), second).await.expect("over cap").is_empty());
        assert_eq!(count("SELECT count(*) FROM subscribed_list_items").await, 1);
        let update = Item { cid: "c1b", rev: "3kc", ..item };
        assert_eq!(item_upsert(&mut conn, ctx(&sets, &eff), update).await.expect("update").touched, vec![list(1)]);

        // During a purge of the owner nothing new is copied.
        sets.purging.insert(A.to_string(), PurgeKind::AccountDeleted);
        assert!(item_upsert(&mut conn, ctx(&sets, &none), second).await.expect("purging").is_empty());
        sets.purging.clear();

        // An update that moves the item to a list nobody subscribes to
        // removes the copy and leaves a tombstone at that rev. A stale
        // create then stays out; a newer move back is copied again.
        let (_, _, elsewhere) = put(1, "3kd", list(9));
        let moved = Item { rev: "3kd", record: &elsewhere, ..item };
        let eff = item_upsert(&mut conn, ctx(&sets, &none), moved).await.expect("move out");
        assert_eq!((eff.tombstones.get(A), eff.touched.clone()), (Some(&1), vec![list(1)]));
        assert_eq!(count("SELECT count(*) FROM subscribed_list_items").await, 0);
        assert!(item_upsert(&mut conn, ctx(&sets, &none), Item { rev: "3kd", ..item }).await.expect("stale").is_empty());
        let back = Item { rev: "3ke", ..item };
        assert_eq!(item_upsert(&mut conn, ctx(&sets, &none), back).await.expect("back").created.get(&list(1)), Some(&1));

        // A delete removes the copy; its tombstone is the same row, so the
        // owner's count does not move. A delete of an item never copied
        // still leaves a tombstone.
        let eff = item_delete(&mut conn, ctx(&sets, &none), A, &uri, "3kf").await.expect("delete");
        assert_eq!((eff.tombstones.get(A), eff.touched), (None, vec![list(1)]));
        let eff = item_delete(&mut conn, ctx(&sets, &none), A, &uri2, "3kf").await.expect("delete");
        assert_eq!(eff.tombstones.get(A), Some(&1));
        // Past the per-owner cap the oldest tombstone goes.
        sets.subscribed.owners.insert(A.to_string(), 2);
        let eff = item_delete(&mut conn, ctx(&sets, &none), A, &item_uri(3), "3kf").await.expect("delete");
        assert_eq!(eff.tombstones.get(A), Some(&0));
        assert_eq!(count("SELECT count(*) FROM subscription_tombstones").await, 2);
        assert_eq!(count("SELECT count(*) FROM subscription_tombstones WHERE uri LIKE '%3kitem1'").await, 0);

        // The purpose follows the list record, newest rev only; a purpose
        // that cannot be stored is NULL; a delete clears it.
        let purpose = || async move {
            sqlx::query_as::<_, (Option<String>, Option<String>)>("SELECT purpose, purpose_rev FROM subscribed_lists").fetch_one(p).await.expect("purpose")
        };
        let modlist = json!({ "purpose": "app.bsky.graph.defs#modlist", "name": "x" });
        list_purpose(&mut conn, &list(1), "3kb", ListOp::Put(&modlist)).await.expect("put");
        assert_eq!(purpose().await, (Some("app.bsky.graph.defs#modlist".to_string()), Some("3kb".to_string())));
        list_purpose(&mut conn, &list(1), "3ka", ListOp::Delete).await.expect("older");
        assert_eq!(purpose().await.1, Some("3kb".to_string()));
        list_purpose(&mut conn, &list(1), "3kc", ListOp::Put(&json!({ "purpose": "x".repeat(513) }))).await.expect("too long");
        assert_eq!(purpose().await, (None, Some("3kc".to_string())));
        list_purpose(&mut conn, &list(1), "3kd", ListOp::Put(&modlist)).await.expect("put");
        list_purpose(&mut conn, &list(1), "3ke", ListOp::Delete).await.expect("delete");
        assert_eq!(purpose().await, (None, Some("3ke".to_string())));
        list_purpose(&mut conn, &list(1), "3kf", ListOp::Put(&json!({ "purpose": "app.bsky.graph.defs#curatelist" }))).await.expect("curate");
        // A list with no row: nothing is written, nothing fails.
        list_purpose(&mut conn, &list(9), "3kb", ListOp::Put(&modlist)).await.expect("no row");
        // `list_purpose_writes_total{path="ingest"}`: one per row written,
        // by what it stored; the older delete and the missing row wrote
        // nothing.
        let text = handle.render();
        for (result, n) in [("modlist", 2.0), ("none", 2.0), ("other", 1.0)] {
            let label = format!("result=\"{result}\"");
            assert_eq!(crate::test_support::series(&text, METRIC_PURPOSE_WRITES, &["path=\"ingest\"", &label]), Some(n), "{result}:\n{text}");
        }
        drop(conn);
        db.drop_db().await;
    }
}
