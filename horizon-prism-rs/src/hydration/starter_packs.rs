//! Starter-pack views (design-v3 §5.5): `getStarterPack`, `getStarterPacks`
//! and `getActorStarterPacks`.
//!
//! A pack is one record, so a stored `starter_packs` row is complete in
//! itself and is used whenever it exists; otherwise the record comes from
//! Slingshot. The pack's **list** follows the locally-servable rule of
//! [`super::lists`]: its count and sample come from local rows only when
//! the list's owner is a ready member, and from an upstream AppView
//! otherwise. An actor's packs are listed from local rows only for a ready
//! member, whose packs are all held.
//!
//! `feeds` is omitted until feed generators are served.

use std::collections::HashMap;

use chrono::{DateTime, Utc};
use serde_json::{Value, json};

use sqlx::Acquire;

use super::actor::{ProfileContext, ProfileKind, SHOWN_SIGNED_OUT, too_expensive};
use super::feed::{local_cursor, parse_local_cursor};
use super::search::{METRIC_TIMEOUT, set_timeout, timed_out, viewer_exclusions};
use super::{Hydrator, ViewError, ViewResult, Viewer, ts};
use crate::db::locks::uri_author;
use crate::moderation::{self, Gate};
use crate::xrpc::pools::Requester;

pub const NSID_STARTERPACK: &str = "app.bsky.graph.starterpack";
/// `getStarterPacks` takes at most this many URIs.
pub const MAX_URIS: usize = 25;
/// Items in `listItemsSample`.
const SAMPLE: i64 = 12;

/// Whether `uri` is `at://<did>/app.bsky.graph.starterpack/<rkey>`.
#[must_use]
pub fn is_pack_uri(uri: &str) -> bool {
    let Some(rest) = uri.strip_prefix("at://") else {
        return false;
    };
    let mut parts = rest.split('/');
    let (Some(did), Some(collection), Some(rkey), None) = (parts.next(), parts.next(), parts.next(), parts.next()) else {
        return false;
    };
    did.starts_with("did:") && collection == NSID_STARTERPACK && !rkey.is_empty()
}

fn pack_not_found() -> ViewError {
    ViewError::new("InvalidRequest", "Starter pack not found")
}

#[derive(Debug, Clone, sqlx::FromRow)]
struct PackRow {
    uri: String,
    cid: String,
    list_uri: String,
    record: Value,
    indexed_at: DateTime<Utc>,
    created_at: DateTime<Utc>,
}

const PACK_COLUMNS: &str = "uri, cid, list_uri, record, indexed_at, created_at";

impl PackRow {
    /// From a fetched `{uri, cid, value}` record.
    fn from_record(record: &Value) -> Option<Self> {
        let value = record.get("value")?;
        value.get("name")?.as_str()?;
        let created = value
            .get("createdAt")
            .and_then(Value::as_str)
            .and_then(|t| DateTime::parse_from_rfc3339(t).ok())
            .map_or_else(Utc::now, |t| t.with_timezone(&Utc).min(Utc::now()));
        let mut stored = value.clone();
        stored["$type"] = json!(NSID_STARTERPACK);
        Some(Self {
            uri: record.get("uri")?.as_str()?.to_string(),
            cid: record.get("cid").and_then(Value::as_str).unwrap_or_default().to_string(),
            list_uri: value.get("list")?.as_str()?.to_string(),
            record: stored,
            indexed_at: created,
            created_at: created,
        })
    }
}

/// `(joinedWeekCount, joinedAllTimeCount)` by pack URI.
type Joined = HashMap<String, (i32, i32)>;

impl Hydrator {
    async fn joined_counts(&self, uris: &[String]) -> Result<Joined, sqlx::Error> {
        let rows: Vec<(String, i32, i32)> =
            sqlx::query_as("SELECT pack_uri, joined_week_count, joined_all_time_count FROM starter_pack_counts WHERE pack_uri = ANY($1)")
                .bind(uris)
                .fetch_all(self.pool())
                .await?;
        Ok(rows.into_iter().map(|(uri, week, all)| (uri, (week, all))).collect())
    }

    /// Stored rows for `uris`, then Slingshot for the rest. Hidden packs
    /// and packs of hidden creators are left out.
    async fn load_packs(&self, req: &Requester, uris: &[String]) -> Result<HashMap<String, PackRow>, sqlx::Error> {
        let wanted: Vec<String> = uris.iter().filter(|u| is_pack_uri(u)).cloned().collect();
        let rows: Vec<PackRow> = sqlx::query_as(&format!("SELECT {PACK_COLUMNS} FROM starter_packs WHERE uri = ANY($1)"))
            .bind(&wanted)
            .fetch_all(self.pool())
            .await?;
        let mut out: HashMap<String, PackRow> = rows.into_iter().map(|r| (r.uri.clone(), r)).collect();
        for uri in &wanted {
            if !out.contains_key(uri)
                && let Some(row) = self.upstream().record(req, uri).await.as_ref().and_then(PackRow::from_record)
            {
                out.insert(uri.clone(), row);
            }
        }
        let mut subjects: Vec<String> = out.keys().cloned().collect();
        subjects.extend(out.keys().filter_map(|u| uri_author(u).map(str::to_string)));
        let gates = moderation::gate_many(self.pool(), &subjects).await?;
        let hidden = |s: &str| gates.get(s).copied().unwrap_or(Gate::Visible).is_hidden();
        out.retain(|uri, _| !hidden(uri) && !uri_author(uri).is_some_and(hidden));
        Ok(out)
    }

    /// A `starterPackViewBasic`. `list_count` is the pack's list's distinct
    /// subjects when known.
    fn pack_basic(row: &PackRow, creators: &ProfileContext, list_count: Option<i64>, joined: &Joined, labels: &HashMap<String, Vec<Value>>) -> Value {
        let creator = uri_author(&row.uri).unwrap_or_default();
        let mut v = json!({
            "uri": row.uri,
            "cid": row.cid,
            "record": row.record,
            "creator": creators.view(creator, ProfileKind::Basic),
            "labels": labels.get(&row.uri).cloned().unwrap_or_default(),
            "indexedAt": ts(row.indexed_at),
        });
        if let Some(n) = list_count {
            v["listItemCount"] = json!(n);
        }
        if let Some((week, all)) = joined.get(&row.uri) {
            v["joinedWeekCount"] = json!(week);
            v["joinedAllTimeCount"] = json!(all);
        }
        v
    }

    /// The local item count of each pack's list, for lists that are locally
    /// servable. Other lists have no entry.
    async fn local_list_counts(&self, rows: &[&PackRow]) -> Result<HashMap<String, i64>, sqlx::Error> {
        let mut out = HashMap::new();
        for row in rows {
            if let Some(owner) = uri_author(&row.list_uri)
                && !out.contains_key(&row.list_uri)
                && self.is_ready_member(owner).await?
            {
                out.insert(row.list_uri.clone(), self.list_item_count(&row.list_uri).await?);
            }
        }
        Ok(out)
    }

    /// `app.bsky.graph.getStarterPacks`: a basic view per URI, in input
    /// order; misses are omitted.
    ///
    /// # Errors
    ///
    /// Database errors.
    pub async fn get_starter_packs(&self, viewer: &Viewer, req: &Requester, uris: &[String]) -> ViewResult<Value> {
        if uris.len() > MAX_URIS {
            return Ok(Err(ViewError::new("InvalidRequest", "Error: uris must not have more than 25 elements")));
        }
        let packs = self.load_packs(req, uris).await?;
        let found: Vec<&PackRow> = uris.iter().filter_map(|u| packs.get(u)).collect();
        let creators: Vec<String> = found.iter().filter_map(|r| uri_author(&r.uri).map(str::to_string)).collect();
        let ctx = self.profile_context(viewer, req, &creators, None).await?;
        let pack_uris: Vec<String> = found.iter().map(|r| r.uri.clone()).collect();
        let (joined, labels) = (self.joined_counts(&pack_uris).await?, self.load_labels(&pack_uris).await?);
        let counts = self.local_list_counts(&found).await?;
        let mut seen = std::collections::HashSet::new();
        let views: Vec<Value> = found
            .iter()
            .filter(|r| seen.insert(r.uri.as_str()))
            .map(|r| Self::pack_basic(r, &ctx, counts.get(&r.list_uri).copied(), &joined, &labels))
            .collect();
        Ok(Ok(Self::emit(json!({ "starterPacks": views }))))
    }

    /// `app.bsky.graph.searchStarterPacks` and `searchStarterPacksV2`
    /// (design-v3 §9.5): stored packs whose name or description matches
    /// `q`, of creators that are in scope and active, most joined first and
    /// then by text rank. The viewer's mutes and blocks apply to the
    /// creator, as in every search.
    ///
    /// V1 gives `starterPackViewBasic`s. V2 gives `starterPackView`s
    /// (without `list` and `listItemsSample`, which the lexicon leaves
    /// optional and which would cost a lookup per pack) and `hitsTotal`,
    /// exact and only when small. The cursor is an offset, capped at
    /// `search.max_offset`.
    ///
    /// # Errors
    ///
    /// Database errors other than the search's own timeout.
    pub async fn search_starter_packs(&self, viewer: &Viewer, req: &Requester, q: &str, limit: u32, cursor: Option<&str>, v2: bool) -> ViewResult<Value> {
        let cfg = self.inner.cfg.search.clone();
        let endpoint = if v2 { "searchStarterPacksV2" } else { "searchStarterPacks" };
        let q = q.trim();
        let max_offset = i64::from(cfg.max_offset);
        let offset: i64 = cursor.and_then(|c| c.parse().ok()).unwrap_or(0).max(0);
        if q.is_empty() || offset > max_offset {
            return Ok(Ok(Self::emit(json!({ "starterPacks": [] }))));
        }
        let limit = i64::from(limit.clamp(1, 100));
        let from = format!(
            " FROM starter_packs sp CROSS JOIN LATERAL (SELECT sp.creator_did AS author_did) n \
              LEFT JOIN actors a ON a.did = sp.creator_did \
              WHERE ($1::text IS NULL OR ({exclusions})) \
                AND sp.search_tsv @@ websearch_to_tsquery('simple', $2) \
                AND EXISTS (SELECT 1 FROM scope_dids s WHERE s.did = sp.creator_did) \
                AND COALESCE(a.account_status, 'active') = 'active' AND ($3 OR {SHOWN_SIGNED_OUT})",
            exclusions = viewer_exclusions(),
        );
        let page = format!(
            "SELECT sp.uri{from} \
             ORDER BY COALESCE((SELECT c.joined_all_time_count FROM starter_pack_counts c WHERE c.pack_uri = sp.uri), 0) DESC, \
                      ts_rank_cd(sp.search_tsv, websearch_to_tsquery('simple', $2)) DESC, sp.uri \
             LIMIT $4 OFFSET $5"
        );
        let me = viewer.state_did();
        let signed_in = viewer.did.is_some();
        let mut tx = self.pool().begin().await?;
        set_timeout(&mut tx, cfg.statement_timeout_ms).await?;
        let found = sqlx::query_scalar(&page).bind(me).bind(q).bind(signed_in).bind(limit).bind(offset).fetch_all(&mut *tx).await;
        let uris: Vec<String> = match found {
            Ok(uris) => uris,
            Err(error) if timed_out(&error) => {
                metrics::counter!(METRIC_TIMEOUT, "endpoint" => endpoint).increment(1);
                return Ok(Err(too_expensive()));
            }
            Err(error) => return Err(error.into()),
        };
        // After the page, under a savepoint: its timeout costs the count
        // and nothing else.
        let cap = i64::from(cfg.hits_total_max);
        let mut hits: Option<i64> = None;
        if v2 {
            let count = format!("SELECT count(*) FROM (SELECT 1{from} LIMIT $4) s");
            let mut savepoint = tx.begin().await?;
            set_timeout(&mut savepoint, cfg.hits_total_timeout_ms).await?;
            let counted: Result<i64, sqlx::Error> =
                sqlx::query_scalar(&count).bind(me).bind(q).bind(signed_in).bind(cap + 1).fetch_one(&mut *savepoint).await;
            match counted {
                Ok(n) => {
                    savepoint.commit().await?;
                    hits = Some(n).filter(|n| *n <= cap);
                }
                Err(error) if timed_out(&error) => savepoint.rollback().await?,
                Err(error) => return Err(error.into()),
            }
        }
        tx.commit().await?;

        // Stored rows, the moderation gate, and what only a lookup knows
        // of the viewer's blocks.
        let packs = self.load_packs(req, &uris).await?;
        let found: Vec<&PackRow> = uris.iter().filter_map(|u| packs.get(u)).collect();
        let creators: Vec<String> = found.iter().filter_map(|r| uri_author(&r.uri).map(str::to_string)).collect();
        let ctx = self.profile_context(viewer, req, &creators, None).await?;
        let pack_uris: Vec<String> = found.iter().map(|r| r.uri.clone()).collect();
        let (joined, labels) = (self.joined_counts(&pack_uris).await?, self.load_labels(&pack_uris).await?);
        let counts = if v2 { HashMap::new() } else { self.local_list_counts(&found).await? };
        let views: Vec<Value> = found
            .iter()
            .filter(|r| {
                let creator = uri_author(&r.uri).unwrap_or_default();
                !ctx.viewer.blocked_either_way(creator) && !ctx.viewer.muted.contains(creator)
            })
            .map(|r| Self::pack_basic(r, &ctx, counts.get(&r.list_uri).copied(), &joined, &labels))
            .collect();
        let mut out = json!({ "starterPacks": views });
        if i64::try_from(uris.len()).unwrap_or(0) == limit && offset + limit <= max_offset {
            out["cursor"] = json!((offset + limit).to_string());
        }
        if let Some(hits) = hits {
            out["hitsTotal"] = json!(hits);
        }
        Ok(Ok(Self::emit(out)))
    }

    /// Up to `limit` `starterPackView`s for stored packs, in the order
    /// given, without the packs whose creator the viewer blocks, is blocked
    /// by, or mutes. No `list` and no sample: both are optional, and each
    /// would cost a lookup per pack.
    pub(super) async fn pack_views(&self, viewer: &Viewer, req: &Requester, uris: &[String], limit: usize) -> Result<Vec<Value>, sqlx::Error> {
        let packs = self.load_packs(req, uris).await?;
        let found: Vec<&PackRow> = uris.iter().filter_map(|u| packs.get(u)).collect();
        let creators: Vec<String> = found.iter().filter_map(|r| uri_author(&r.uri).map(str::to_string)).collect();
        let ctx = self.profile_context(viewer, req, &creators, None).await?;
        let pack_uris: Vec<String> = found.iter().map(|r| r.uri.clone()).collect();
        let (joined, labels) = (self.joined_counts(&pack_uris).await?, self.load_labels(&pack_uris).await?);
        Ok(found
            .iter()
            .filter(|r| {
                let creator = uri_author(&r.uri).unwrap_or_default();
                !ctx.viewer.blocked_either_way(creator) && !ctx.viewer.muted.contains(creator)
            })
            .take(limit)
            .map(|r| Self::pack_basic(r, &ctx, None, &joined, &labels))
            .collect())
    }

    /// `app.bsky.graph.getStarterPack`: the pack with its list and a sample
    /// of at most 12 list items.
    ///
    /// # Errors
    ///
    /// Database errors.
    pub async fn get_starter_pack(&self, viewer: &Viewer, req: &Requester, uri: &str) -> ViewResult<Value> {
        let uris = [uri.to_string()];
        let mut packs = self.load_packs(req, &uris).await?;
        let Some(row) = packs.remove(uri) else {
            return Ok(Err(pack_not_found()));
        };
        let creator = uri_author(uri).unwrap_or_default().to_string();
        let list = self.load_list(req, &row.list_uri).await?;
        let local_list = list.as_ref().is_some_and(|(_, local)| *local);

        // The sample and the count: local rows for a locally servable list,
        // an upstream AppView's view of the pack otherwise.
        let (sample, count): (Vec<Value>, Option<i64>) = if local_list {
            let rows: Vec<(String, String)> = sqlx::query_as(
                "SELECT uri, subject_did FROM ( \
                   SELECT DISTINCT ON (subject_did) uri, subject_did, created_at FROM list_items \
                   WHERE list_uri = $1 ORDER BY subject_did, created_at, uri) i \
                 ORDER BY created_at, uri LIMIT $2",
            )
            .bind(&row.list_uri)
            .bind(SAMPLE)
            .fetch_all(self.pool())
            .await?;
            let dids: Vec<String> = rows.iter().map(|(_, d)| d.clone()).collect();
            let gates = moderation::gate_many(self.pool(), &dids).await?;
            let ctx = self.profile_context(viewer, req, &dids, None).await?;
            let sample = rows
                .iter()
                .filter(|(_, d)| !gates.get(d).copied().unwrap_or(Gate::Visible).is_hidden())
                .map(|(item, did)| json!({"uri": item, "subject": ctx.view(did, ProfileKind::View)}))
                .collect();
            (sample, Some(self.list_item_count(&row.list_uri).await?))
        } else if list.is_some()
            && let Some(up) = self.upstream().starter_pack(req, uri).await
        {
            let (up, _) = self.normalized(viewer, up).await?;
            let sample: Vec<Value> = up
                .pointer("/starterPack/listItemsSample")
                .and_then(Value::as_array)
                .map(|items| items.iter().take(usize::try_from(SAMPLE).unwrap_or(12)).cloned().collect())
                .unwrap_or_default();
            let dids: Vec<String> = sample
                .iter()
                .filter_map(|i| i.pointer("/subject/did").and_then(Value::as_str).map(str::to_string))
                .collect();
            let gates = moderation::gate_many(self.pool(), &dids).await?;
            let visible = |item: &Value| {
                item.pointer("/subject/did")
                    .and_then(Value::as_str)
                    .is_some_and(|d| !gates.get(d).copied().unwrap_or(Gate::Visible).is_hidden())
            };
            let count = up.pointer("/starterPack/list/listItemCount").and_then(Value::as_i64);
            (sample.into_iter().filter(visible).collect(), count)
        } else {
            (Vec::new(), None)
        };

        let ctx = self.profile_context(viewer, req, std::slice::from_ref(&creator), None).await?;
        let (joined, labels) = (self.joined_counts(&uris).await?, self.load_labels(&uris).await?);
        let mut view = Self::pack_basic(&row, &ctx, None, &joined, &labels);
        if let Some(obj) = view.as_object_mut() {
            obj.remove("listItemCount");
        }
        if let Some((list_row, _)) = &list {
            let owner = uri_author(&list_row.uri).unwrap_or_default().to_string();
            let owners = self.load_actors(std::slice::from_ref(&owner)).await?;
            let pds = owners.get(&owner).and_then(|r| r.pds_endpoint.as_deref());
            view["list"] = self.list_basic(viewer, list_row, pds, count).await?;
            view["listItemsSample"] = Value::Array(sample);
        }
        Ok(Ok(Self::emit(json!({ "starterPack": view }))))
    }

    /// `app.bsky.graph.getStarterPacksWithMembership`: the (ready member)
    /// viewer's own packs, each with `actor`'s list item when `actor` is on
    /// the pack's list.
    ///
    /// # Errors
    ///
    /// Database errors.
    pub async fn get_starter_packs_with_membership(&self, viewer: &Viewer, req: &Requester, actor: &str, limit: u32, cursor: Option<&str>) -> ViewResult<Value> {
        let Some(me) = viewer.did.clone() else {
            return Ok(Err(ViewError::new("AuthRequired", "Authentication required")));
        };
        let did = match self.subject(req, actor).await? {
            Ok(d) => d,
            Err(e) => return Ok(Err(e)),
        };
        let (at, after) = parse_local_cursor(cursor);
        let rows: Vec<PackRow> = sqlx::query_as(&format!(
            "SELECT {PACK_COLUMNS} FROM starter_packs WHERE creator_did = $1 \
               AND ($2::timestamptz IS NULL OR (created_at, uri) < ($2, $3)) \
             ORDER BY created_at DESC, uri DESC LIMIT $4"
        ))
        .bind(&me)
        .bind(at)
        .bind(after.unwrap_or_default())
        .bind(i64::from(limit))
        .fetch_all(self.pool())
        .await?;
        let next = (rows.len() == usize::try_from(limit).unwrap_or(usize::MAX))
            .then(|| rows.last().map(|r| local_cursor(r.created_at, &r.uri)))
            .flatten();
        let uris: Vec<String> = rows.iter().map(|r| r.uri.clone()).collect();
        let lists: Vec<String> = rows.iter().map(|r| r.list_uri.clone()).collect();
        let member_of = self.memberships(&lists, &did).await?;
        let people = self.profile_context(viewer, req, &[me.clone(), did.clone()], None).await?;
        let (joined, labels) = (self.joined_counts(&uris).await?, self.load_labels(&uris).await?);
        let pds = people.rows.get(&me).and_then(|r| r.pds_endpoint.clone());
        let mut entries = Vec::with_capacity(rows.len());
        for row in &rows {
            let mut pack = Self::pack_basic(row, &people, None, &joined, &labels);
            if let Some((list_row, true)) = self.load_list(req, &row.list_uri).await? {
                let count = self.list_item_count(&row.list_uri).await?;
                pack["list"] = self.list_basic(viewer, &list_row, pds.as_deref(), Some(count)).await?;
            }
            let mut entry = json!({ "starterPack": pack });
            if let Some(item) = member_of.get(&row.list_uri) {
                entry["listItem"] = json!({"uri": item, "subject": people.view(&did, ProfileKind::View)});
            }
            entries.push(entry);
        }
        let mut out = json!({ "starterPacksWithMembership": entries });
        if let Some(c) = next {
            out["cursor"] = json!(c);
        }
        Ok(Ok(Self::emit(out)))
    }

    /// `app.bsky.graph.getActorStarterPacks`.
    ///
    /// # Errors
    ///
    /// Database errors.
    pub async fn get_actor_starter_packs(&self, viewer: &Viewer, req: &Requester, actor: &str, limit: u32, cursor: Option<&str>) -> ViewResult<Value> {
        let did = match self.subject(req, actor).await? {
            Ok(d) => d,
            Err(e) => return Ok(Err(e)),
        };
        if !self.is_ready_member(&did).await? {
            let Some(found) = self.upstream().actor_starter_packs(req, &did, limit, cursor).await else {
                return Ok(Err(ViewError::new("InvalidRequest", "Profile not found")));
            };
            let (mut found, _) = self.normalized(viewer, found).await?;
            let uris: Vec<String> = found["starterPacks"]
                .as_array()
                .into_iter()
                .flatten()
                .filter_map(|p| p.get("uri").and_then(Value::as_str).map(str::to_string))
                .collect();
            let gates = moderation::gate_many(self.pool(), &uris).await?;
            let labels = self.load_labels(&uris).await?;
            if let Some(packs) = found["starterPacks"].as_array_mut() {
                packs.retain(|p| {
                    p.get("uri")
                        .and_then(Value::as_str)
                        .is_some_and(|u| !gates.get(u).copied().unwrap_or(Gate::Visible).is_hidden())
                });
                for p in packs.iter_mut() {
                    let uri = p["uri"].as_str().unwrap_or_default().to_string();
                    p["labels"] = Value::Array(labels.get(&uri).cloned().unwrap_or_default());
                }
            }
            return Ok(Ok(Self::emit(found)));
        }

        let (at, after) = parse_local_cursor(cursor);
        let rows: Vec<PackRow> = sqlx::query_as(&format!(
            "SELECT {PACK_COLUMNS} FROM starter_packs WHERE creator_did = $1 \
               AND ($2::timestamptz IS NULL OR (created_at, uri) < ($2, $3)) \
             ORDER BY created_at DESC, uri DESC LIMIT $4"
        ))
        .bind(&did)
        .bind(at)
        .bind(after.unwrap_or_default())
        .bind(i64::from(limit))
        .fetch_all(self.pool())
        .await?;
        let next = (rows.len() == usize::try_from(limit).unwrap_or(usize::MAX))
            .then(|| rows.last().map(|r| local_cursor(r.created_at, &r.uri)))
            .flatten();
        let uris: Vec<String> = rows.iter().map(|r| r.uri.clone()).collect();
        let gates = moderation::gate_many(self.pool(), &uris).await?;
        let ctx = self.profile_context(viewer, req, std::slice::from_ref(&did), None).await?;
        let (joined, labels) = (self.joined_counts(&uris).await?, self.load_labels(&uris).await?);
        let refs: Vec<&PackRow> = rows.iter().collect();
        let counts = self.local_list_counts(&refs).await?;
        let packs: Vec<Value> = rows
            .iter()
            .filter(|r| !gates.get(&r.uri).copied().unwrap_or(Gate::Visible).is_hidden())
            .map(|r| Self::pack_basic(r, &ctx, counts.get(&r.list_uri).copied(), &joined, &labels))
            .collect();
        let mut out = json!({ "starterPacks": packs });
        if let Some(c) = next {
            out["cursor"] = json!(c);
        }
        Ok(Ok(Self::emit(out)))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::aggregations::ScopeSets;
    use crate::hydration::lists::{NSID_LIST, PURPOSE_REFERENCE};
    use crate::hydration::testing::{FixedUpstream, actor, exec, hydrator, hydrator_with};
    use crate::test_support::fresh_db;

    const ALICE: &str = "did:plc:aaaaaaaaaaaaaaaaaaaaaaaa";
    const BOB: &str = "did:plc:bbbbbbbbbbbbbbbbbbbbbbbb";
    const FAR: &str = "did:plc:zzzzzzzzzzzzzzzzzzzzzzzz";

    fn pack_uri(owner: &str, rkey: &str) -> String {
        format!("at://{owner}/{NSID_STARTERPACK}/{rkey}")
    }

    async fn pack(p: &sqlx::PgPool, owner: &str, rkey: &str, name: &str, list: &str, age_minutes: u32) -> String {
        let uri = pack_uri(owner, rkey);
        let record = json!({"$type": NSID_STARTERPACK, "name": name, "list": list, "createdAt": "2026-01-01T00:00:00Z"});
        sqlx::query(
            "INSERT INTO starter_packs (uri, creator_did, rkey, cid, rev, name, list_uri, record, created_at) \
             VALUES ($1, $2, $3, 'bafypack', '3k', $4, $5, $6, now() - make_interval(mins => $7))",
        )
        .bind(&uri)
        .bind(owner)
        .bind(rkey)
        .bind(name)
        .bind(list)
        .bind(&record)
        .bind(i32::try_from(age_minutes).expect("minutes"))
        .execute(p)
        .await
        .expect("pack");
        uri
    }

    /// B-42 (design-v3 §9.5): packs are found by their text, of creators in
    /// scope only, most joined first; V2 adds the exact total; a member's
    /// mute of the creator leaves the pack out.
    #[tokio::test]
    async fn packs_are_searched_by_text_within_scope() {
        let Some(db) = fresh_db("hydration_packs_search").await else {
            return;
        };
        let p = &db.pool;
        let outside = "did:plc:oooooooooooooooooooooooo";
        for (did, handle) in [(ALICE, "alice.test"), (BOB, "bob.test"), (outside, "out.test")] {
            actor(p, did, handle, handle).await;
        }
        for did in [ALICE, BOB] {
            sqlx::query("INSERT INTO scope_dids (did, reasons) VALUES ($1, 2)").bind(did).execute(p).await.expect("scope");
        }
        exec(p, &format!("INSERT INTO members (did, source, ready_at) VALUES ('{ALICE}', 'admin', now())")).await;
        let list = format!("at://{ALICE}/{NSID_LIST}/sp");
        let a = pack(p, ALICE, "a", "Rust people", &list, 3).await;
        let b = pack(p, BOB, "b", "More rust people", &list, 2).await;
        pack(p, BOB, "c", "Gardeners", &list, 1).await;
        pack(p, outside, "d", "Rust outside the scope", &list, 1).await;
        exec(p, &format!("INSERT INTO starter_pack_counts (pack_uri, joined_week_count, joined_all_time_count, all_time_source, refreshed_at) VALUES ('{b}', 1, 9, 'local', now())")).await;

        let h = hydrator(p, ScopeSets::default());
        let req = Requester::test();
        let found = |view: &Value| view["starterPacks"].as_array().expect("packs").iter().map(|s| s["uri"].as_str().expect("uri").to_string()).collect::<Vec<_>>();
        let anon = Viewer::anonymous();
        let v1 = h.search_starter_packs(&anon, &req, "rust", 25, None, false).await.expect("db").expect("view");
        assert_eq!(found(v1.inner()), [b.clone(), a.clone()]);
        assert!(v1.inner().get("hitsTotal").is_none());
        assert_eq!(v1.inner()["starterPacks"][0]["creator"]["did"], BOB);
        let v2 = h.search_starter_packs(&anon, &req, "rust", 1, None, true).await.expect("db").expect("view");
        assert_eq!((found(v2.inner()), &v2.inner()["hitsTotal"], &v2.inner()["cursor"]), (vec![b.clone()], &json!(2), &json!("1")));
        let next = h.search_starter_packs(&anon, &req, "rust", 1, Some("1"), true).await.expect("db").expect("view");
        assert_eq!(found(next.inner()), std::slice::from_ref(&a));
        // Past the deepest offset there is nothing, and an empty query
        // finds nothing.
        let deep = h.search_starter_packs(&anon, &req, "rust", 1, Some("5000"), true).await.expect("db").expect("view");
        assert!(found(deep.inner()).is_empty());
        assert!(found(h.search_starter_packs(&anon, &req, " ", 25, None, false).await.expect("db").expect("view").inner()).is_empty());

        let me = Viewer { did: Some(ALICE.to_string()), hydrate: true };
        exec(p, &format!("INSERT INTO mutes (viewer_did, subject_did) VALUES ('{ALICE}', '{BOB}')")).await;
        let mine = h.search_starter_packs(&me, &req, "rust", 25, None, true).await.expect("db").expect("view");
        assert_eq!((found(mine.inner()), &mine.inner()["hitsTotal"]), (vec![a], &json!(1)));
        db.drop_db().await;
    }

    /// B-37: packs of a ready member, with the list's count and sample from
    /// local rows.
    #[tokio::test]
    async fn a_ready_members_packs_are_local() {
        let Some(db) = fresh_db("hydration_packs_local").await else {
            return;
        };
        let p = &db.pool;
        actor(p, ALICE, "alice.test", "Alice").await;
        actor(p, BOB, "bob.test", "Bob").await;
        exec(p, &format!("INSERT INTO members (did, source, ready_at) VALUES ('{ALICE}', 'admin', now())")).await;
        let list = format!("at://{ALICE}/{NSID_LIST}/sp");
        exec(p, &format!(
            "INSERT INTO lists (uri, creator_did, rkey, cid, rev, name, purpose, record, created_at) VALUES \
             ('{list}', '{ALICE}', 'sp', 'bafylist', '3k', 'Pack list', '{PURPOSE_REFERENCE}', '{{}}', now())"
        )).await;
        for i in 0..14 {
            // Two items name Bob; the rest name accounts with no row here.
            let subject = if i < 2 { BOB.to_string() } else { format!("did:plc:{:a>24}", i) };
            exec(p, &format!(
                "INSERT INTO list_items (uri, author_did, rkey, cid, rev, list_uri, subject_did, created_at) VALUES \
                 ('at://{ALICE}/app.bsky.graph.listitem/i{i:02}', '{ALICE}', 'i{i:02}', 'bafy', '3k', '{list}', '{subject}', now() - interval '{} minutes')",
                20 - i
            )).await;
        }
        let first = pack(p, ALICE, "p1", "First", &list, 2).await;
        let second = pack(p, ALICE, "p2", "Second", &list, 1).await;
        exec(p, &format!(
            "INSERT INTO starter_pack_counts (pack_uri, joined_week_count, joined_all_time_count, all_time_source, refreshed_at) \
             VALUES ('{first}', 3, 40, 'local', now())"
        )).await;
        let h = hydrator(p, ScopeSets::default());
        let (anon, req) = (Viewer::anonymous(), Requester::test());

        let v = h.get_starter_pack(&anon, &req, &first).await.expect("db").expect("view");
        let sp = &v.inner()["starterPack"];
        assert_eq!((&sp["record"]["name"], &sp["record"]["$type"]), (&json!("First"), &json!(NSID_STARTERPACK)));
        assert_eq!(sp["creator"]["handle"], "alice.test");
        assert_eq!((&sp["joinedWeekCount"], &sp["joinedAllTimeCount"]), (&json!(3), &json!(40)));
        assert_eq!((&sp["list"]["name"], &sp["list"]["listItemCount"]), (&json!("Pack list"), &json!(13)));
        let sample = sp["listItemsSample"].as_array().expect("sample");
        assert_eq!(sample.len(), 12);
        // The oldest distinct subjects first; Bob once.
        assert_eq!(sample[0]["subject"]["did"], BOB);
        assert_eq!(sample.iter().filter(|i| i["subject"]["did"] == BOB).count(), 1);
        assert!(sp.get("feeds").is_none() && sp.get("listItemCount").is_none());

        let v = h.get_starter_packs(&anon, &req, &[second.clone(), pack_uri(ALICE, "none"), first.clone(), "garbage".into()]).await.expect("db").expect("view");
        let packs = v.inner()["starterPacks"].as_array().expect("packs");
        assert_eq!(packs.iter().map(|p| p["record"]["name"].as_str().expect("name")).collect::<Vec<_>>(), ["Second", "First"]);
        assert_eq!(packs[0]["listItemCount"], 13);
        assert!(packs[0].get("joinedAllTimeCount").is_none());
        let too_many: Vec<String> = (0..26).map(|i| pack_uri(ALICE, &i.to_string())).collect();
        assert!(h.get_starter_packs(&anon, &req, &too_many).await.expect("db").is_err());

        let v = h.get_actor_starter_packs(&anon, &req, "alice.test", 1, None).await.expect("db").expect("view");
        assert_eq!(v.inner()["starterPacks"][0]["record"]["name"], "Second");
        let cursor = v.inner()["cursor"].as_str().expect("cursor").to_string();
        let v = h.get_actor_starter_packs(&anon, &req, ALICE, 1, Some(&cursor)).await.expect("db").expect("view");
        assert_eq!(v.inner()["starterPacks"][0]["record"]["name"], "First");

        // B-57: the owner's packs with one actor's membership.
        let owner = Viewer {
            did: Some(ALICE.into()),
            hydrate: true,
        };
        let w = h.get_starter_packs_with_membership(&owner, &req, "bob.test", 50, None).await.expect("db").expect("view");
        let entries = w.inner()["starterPacksWithMembership"].as_array().expect("entries");
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0]["starterPack"]["list"]["listItemCount"], 13);
        assert_eq!(entries[0]["listItem"]["uri"], format!("at://{ALICE}/app.bsky.graph.listitem/i01"));
        let w = h.get_starter_packs_with_membership(&owner, &req, ALICE, 50, None).await.expect("db").expect("view");
        assert!(w.inner()["starterPacksWithMembership"][0].get("listItem").is_none());
        assert!(h.get_starter_packs_with_membership(&anon, &req, BOB, 50, None).await.expect("db").is_err());

        for miss in [pack_uri(ALICE, "none"), format!("at://{ALICE}/app.bsky.graph.list/sp"), "garbage".to_string()] {
            let e = h.get_starter_pack(&anon, &req, &miss).await.expect("db").expect_err("miss");
            assert_eq!(e.message, "Starter pack not found", "{miss}");
        }
        db.drop_db().await;
    }

    /// B-37: a pack this AppView does not store comes from Slingshot, and
    /// its list's sample from an upstream AppView.
    #[tokio::test]
    async fn other_packs_are_read_upstream() {
        let Some(db) = fresh_db("hydration_packs_upstream").await else {
            return;
        };
        let p = &db.pool;
        let list = format!("at://{FAR}/{NSID_LIST}/l");
        let uri = pack_uri(FAR, "p");
        let mut up = FixedUpstream::default();
        up.records.insert(uri.clone(), json!({"uri": uri, "cid": "bafyfar", "value": {"name": "Far pack", "list": list, "createdAt": "2026-01-01T00:00:00Z"}}));
        up.records.insert(list.clone(), json!({"uri": list, "cid": "bafyl", "value": {"name": "Far list", "purpose": PURPOSE_REFERENCE, "createdAt": "2026-01-01T00:00:00Z"}}));
        up.starter_packs.insert(uri.clone(), json!({"starterPack": {
            "uri": uri, "cid": "bafyfar", "record": {"name": "Far pack"},
            "creator": {"did": FAR, "handle": "far.example"},
            "list": {"uri": list, "cid": "bafyl", "name": "Far list", "purpose": PURPOSE_REFERENCE, "listItemCount": 7},
            "listItemsSample": [{"uri": format!("at://{FAR}/app.bsky.graph.listitem/1"), "subject": {"did": BOB, "handle": "bob.example", "viewer": {"muted": true}}}],
        }}));
        up.actor_packs.insert(FAR.to_string(), json!({"starterPacks": [
            {"uri": uri, "cid": "bafyfar", "record": {"name": "Far pack"}, "creator": {"did": FAR, "handle": "far.example"}, "labels": [{"val": "x"}], "indexedAt": "2026-01-01T00:00:00Z"},
        ], "cursor": "more"}));
        let h = hydrator_with(p, ScopeSets::default(), Arc::new(up));
        let (anon, req) = (Viewer::anonymous(), Requester::test());

        let v = h.get_starter_pack(&anon, &req, &uri).await.expect("db").expect("view");
        let sp = &v.inner()["starterPack"];
        assert_eq!((&sp["record"]["name"], &sp["cid"]), (&json!("Far pack"), &json!("bafyfar")));
        assert_eq!((&sp["list"]["name"], &sp["list"]["listItemCount"]), (&json!("Far list"), &json!(7)));
        assert_eq!(sp["listItemsSample"][0]["subject"]["did"], BOB);
        assert_ne!(sp["listItemsSample"][0]["subject"]["viewer"]["muted"], json!(true));

        let v = h.get_starter_packs(&anon, &req, std::slice::from_ref(&uri)).await.expect("db").expect("view");
        assert_eq!(v.inner()["starterPacks"][0]["record"]["name"], "Far pack");
        assert!(v.inner()["starterPacks"][0].get("listItemCount").is_none());

        let v = h.get_actor_starter_packs(&anon, &req, FAR, 50, None).await.expect("db").expect("view");
        assert_eq!(v.inner()["starterPacks"][0]["record"]["name"], "Far pack");
        assert_eq!(v.inner()["starterPacks"][0]["labels"], json!([]));
        assert_eq!(v.inner()["cursor"], "more");
        db.drop_db().await;
    }
}
