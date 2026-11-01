//! List views (design-v3 §5.5): `getList` and `getLists`.
//!
//! **Locally servable.** A public read uses a list's own rows (`lists`,
//! `list_items`) only when the list's owner is a ready member: a member's
//! backfill holds all of its lists and list items, so the rows are complete.
//! Every other list is read as if no row were stored: its record comes from
//! Slingshot and its items from an upstream AppView or the owner's PDS, even
//! when some rows happen to be stored. No public answer then depends on how
//! much of a list was seen live.
//!
//! Nothing here reads the subscription tables (`subscribed_*`,
//! `list_references`): those exist only to apply a member's own list mutes
//! and list blocks.

use std::collections::{HashMap, HashSet};

use chrono::{DateTime, Utc};
use serde_json::{Map, Value, json};

use super::actor::{ProfileContext, ProfileKind};
use super::feed::{local_cursor, parse_local_cursor};
use super::{Hydrator, ViewError, ViewResult, Viewer, blob_url, ts};
use crate::db::locks::uri_author;
use crate::firehose::normalize::blob_cid;
use crate::moderation::Gate;
use crate::xrpc::pools::Requester;

pub const NSID_LIST: &str = "app.bsky.graph.list";
pub const PURPOSE_MOD: &str = "app.bsky.graph.defs#modlist";
pub const PURPOSE_CURATE: &str = "app.bsky.graph.defs#curatelist";
pub const PURPOSE_REFERENCE: &str = "app.bsky.graph.defs#referencelist";

/// A `purposes` value as its full token. The short forms (`modlist`,
/// `curatelist`, `referencelist`) are accepted beside the tokens.
#[must_use]
pub fn purpose_token(value: &str) -> Option<&'static str> {
    [PURPOSE_MOD, PURPOSE_CURATE, PURPOSE_REFERENCE]
        .into_iter()
        .find(|token| *token == value || token.rsplit('#').next() == Some(value))
}

/// Whether `uri` is `at://<did>/app.bsky.graph.list/<rkey>`. Anything else,
/// including every `tools.kryphocron.*` URI, misses with no fetch.
#[must_use]
pub fn is_list_uri(uri: &str) -> bool {
    let Some(rest) = uri.strip_prefix("at://") else {
        return false;
    };
    let mut parts = rest.split('/');
    let (Some(did), Some(collection), Some(rkey), None) = (parts.next(), parts.next(), parts.next(), parts.next()) else {
        return false;
    };
    did.starts_with("did:") && collection == NSID_LIST && !rkey.is_empty()
}

/// A list item on its way out: the listitem URI, the subject's DID, and the
/// subject's profile view.
type ListedItem = (String, String, Value);

fn list_not_found() -> ViewError {
    ViewError::new("InvalidRequest", "List not found")
}

/// What a `listView` is built from: a `lists` row, or a fetched record.
#[derive(Debug, Clone)]
pub(super) struct ListRow {
    pub(super) uri: String,
    cid: String,
    name: String,
    pub(super) purpose: String,
    description: Option<String>,
    description_facets: Option<Value>,
    avatar_cid: Option<String>,
    indexed_at: DateTime<Utc>,
    created_at: DateTime<Utc>,
    /// The stored row's key; none for a fetched record.
    key: Option<(i32, i64)>,
}

/// A stored list as read: URI, CID, purpose, record, `indexed_at`,
/// `created_at`, and its key.
type StoredList = (String, Vec<u8>, String, Vec<u8>, DateTime<Utc>, DateTime<Utc>, i32, i64);

/// SQL: a stored list's columns, as [`StoredList`], from `lists l` joined to
/// its author's `dids` row `d`.
fn list_columns() -> String {
    format!(
        "{} AS uri, l.cid, l.purpose, l.record, l.indexed_at, l.created_at, l.author, l.rkey",
        crate::db::lists::uri_sql("d.did", NSID_LIST, "l.rkey")
    )
}

impl ListRow {
    /// From a fetched `{uri, cid, value}` record. `None` when the value is
    /// not a list record.
    fn from_record(record: &Value) -> Option<Self> {
        let value = record.get("value")?;
        let text = |key: &str| value.get(key).and_then(Value::as_str).map(str::to_string);
        let created = text("createdAt")
            .and_then(|t| DateTime::parse_from_rfc3339(&t).ok())
            .map_or_else(Utc::now, |t| t.with_timezone(&Utc).min(Utc::now()));
        Some(Self {
            uri: record.get("uri")?.as_str()?.to_string(),
            cid: record.get("cid").and_then(Value::as_str).unwrap_or_default().to_string(),
            name: text("name")?,
            purpose: text("purpose")?,
            description: text("description"),
            description_facets: value.get("descriptionFacets").filter(|v| v.is_array()).cloned(),
            avatar_cid: value.get("avatar").and_then(blob_cid),
            indexed_at: created,
            created_at: created,
            key: None,
        })
    }

    /// From a stored row: the fields from its record, as the writer read
    /// them when it stored the row.
    async fn from_stored(pool: &sqlx::PgPool, row: StoredList) -> Result<Self, sqlx::Error> {
        let (uri, cid, purpose, record, indexed_at, created_at, author, rkey) = row;
        let value = crate::db::records::decode_value(pool, &record).await?;
        let text = |key: &str| value.get(key).and_then(Value::as_str).map(str::to_string);
        Ok(Self {
            uri,
            cid: crate::db::ids::cid_from_db(&cid),
            name: text("name").unwrap_or_default(),
            purpose,
            description: text("description"),
            description_facets: value.get("descriptionFacets").filter(|v| !v.is_null()).cloned(),
            avatar_cid: value.get("avatar").and_then(blob_cid),
            indexed_at,
            created_at,
            key: Some((author, rkey)),
        })
    }
}

/// Stored lists from a query over [`list_columns`].
async fn stored_lists(pool: &sqlx::PgPool, rows: Vec<StoredList>) -> Result<Vec<ListRow>, sqlx::Error> {
    let mut out = Vec::with_capacity(rows.len());
    for row in rows {
        out.push(ListRow::from_stored(pool, row).await?);
    }
    Ok(out)
}

/// The record key a local cursor's URI names, for a `(created_at, rkey)`
/// order in one repo; the lowest key when it names none, so the cursor sorts
/// below every row of its time.
pub(super) async fn cursor_rkey(pool: &sqlx::PgPool, after: Option<&str>) -> Result<i64, sqlx::Error> {
    Ok(match after {
        Some(uri) => crate::db::ids::whole_key(pool, uri).await?.map_or(i64::MIN, |(_, _, k)| k),
        None => i64::MIN,
    })
}

/// Distinct subjects of each of `lists` (one author's stored lists), by URI.
async fn item_counts(pool: &sqlx::PgPool, lists: &[ListRow]) -> Result<HashMap<String, i64>, sqlx::Error> {
    let (authors, rkeys): (Vec<i32>, Vec<i64>) = lists.iter().filter_map(|l| l.key).unzip();
    let counts: Vec<(i32, i64, i64)> = sqlx::query_as(
        "SELECT i.author, i.list_rkey, count(DISTINCT i.subject) FROM unnest($1::int[], $2::bigint[]) AS k(author, rkey) \
         JOIN list_items i ON i.author = k.author AND i.list_rkey = k.rkey GROUP BY i.author, i.list_rkey",
    )
    .bind(&authors)
    .bind(&rkeys)
    .fetch_all(pool)
    .await?;
    Ok(lists
        .iter()
        .filter_map(|l| {
            let key = l.key?;
            let n = counts.iter().find(|(a, k, _)| (*a, *k) == key).map_or(0, |(_, _, n)| *n);
            Some((l.uri.clone(), n))
        })
        .collect())
}

/// The viewer's own state on lists: its list mute, its `listblock` record,
/// its reference-list opt-out. Empty for anyone but a ready member.
#[derive(Debug, Default)]
pub(super) struct ListViewer {
    known: bool,
    muted: HashSet<String>,
    blocked: HashMap<String, String>,
    opted_out: HashMap<String, String>,
}

impl ListViewer {
    pub(super) fn view(&self, list: &str) -> Value {
        let mut v = Map::new();
        if self.known {
            v.insert("muted".into(), json!(self.muted.contains(list)));
        }
        if let Some(uri) = self.blocked.get(list) {
            v.insert("blocked".into(), json!(uri));
        }
        if let Some(uri) = self.opted_out.get(list) {
            v.insert("referenceListOptOut".into(), json!(uri));
        }
        Value::Object(v)
    }
}

impl Hydrator {
    /// Whether `did` is a member with readiness set.
    ///
    /// # Errors
    ///
    /// Database errors.
    pub(crate) async fn is_ready_member(&self, did: &str) -> Result<bool, sqlx::Error> {
        sqlx::query_scalar("SELECT EXISTS (SELECT 1 FROM members WHERE did = $1 AND ready_at IS NOT NULL)")
            .bind(did)
            .fetch_one(self.pool())
            .await
    }

    pub(super) async fn list_viewer(&self, viewer: &Viewer, lists: &[String]) -> Result<ListViewer, sqlx::Error> {
        let Some(me) = viewer.state_did() else {
            return Ok(ListViewer::default());
        };
        self.list_viewer_of(me, lists).await
    }

    /// [`Self::list_viewer`] for the viewer `me`, whose state is hydrated.
    pub(super) async fn list_viewer_of(&self, me: &str, lists: &[String]) -> Result<ListViewer, sqlx::Error> {
        let me_id = crate::db::ids::account_id(self.pool(), me).await?;
        let muted: Vec<String> = sqlx::query_scalar("SELECT list_uri FROM list_mutes WHERE viewer_did = $1 AND list_uri = ANY($2)")
            .bind(me)
            .bind(lists)
            .fetch_all(self.pool())
            .await?;
        let mut pairs = Vec::new();
        for (table, coll) in [("list_blocks", crate::db::ids::COLL_LISTBLOCK), ("list_optouts", crate::db::ids::COLL_LIST_OPTOUT)] {
            // The viewer's earliest record for each list: a whole list
            // reference by its key, an odd one by its text.
            let rows: Vec<(String, String)> = sqlx::query_as(&format!(
                "SELECT DISTINCT ON (x.list_uri) x.list_uri, at_uri(x.author, {coll}::smallint, x.rkey) FROM ( \
                   SELECT u.uri AS list_uri, b.author, b.rkey, b.created_at FROM unnest($2::text[]) AS u(uri) \
                     CROSS JOIN LATERAL ref_of(u.uri) k \
                     JOIN {table} b ON b.subj_author = k.author AND b.subj_rkey = k.rkey AND b.subj_coll = k.coll \
                     WHERE b.author = $1 \
                   UNION ALL SELECT o.raw, b.author, b.rkey, b.created_at FROM odd_refs o \
                     JOIN {table} b ON b.author = o.author AND b.rkey = o.rkey AND b.subj_coll = 0 \
                     WHERE o.tbl = {coll} AND o.author = $1 AND o.slot = {slot} AND o.raw = ANY($2)) x \
                 ORDER BY x.list_uri, x.created_at, x.rkey",
                slot = crate::db::ids::Slot::Subject.id()
            ))
            .bind(me_id)
            .bind(lists)
            .fetch_all(self.pool())
            .await?;
            pairs.push(rows.into_iter().collect::<HashMap<_, _>>());
        }
        let opted_out = pairs.pop().unwrap_or_default();
        Ok(ListViewer {
            known: true,
            muted: muted.into_iter().collect(),
            blocked: pairs.pop().unwrap_or_default(),
            opted_out,
        })
    }

    /// A list by URI under the locally-servable rule: its row when the
    /// owner is a ready member, else its record from Slingshot. The flag
    /// says which. Private-context hydration fetches nothing: a list that
    /// is not locally servable has no view there (design-v4 §8.1).
    pub(super) async fn load_list(&self, req: &Requester, list: &str) -> Result<Option<(ListRow, bool)>, sqlx::Error> {
        if !is_list_uri(list) {
            return Ok(None);
        }
        let local = self.is_ready_member(uri_author(list).unwrap_or_default()).await?;
        let row: Option<ListRow> = if local {
            match crate::db::lists::key(self.pool(), list).await? {
                Some((author, rkey)) => {
                    let rows: Vec<StoredList> =
                        sqlx::query_as(&format!("SELECT {} FROM lists l JOIN dids d ON d.id = l.author WHERE l.author = $1 AND l.rkey = $2", list_columns()))
                            .bind(author)
                            .bind(rkey)
                            .fetch_all(self.pool())
                            .await?;
                    stored_lists(self.pool(), rows).await?.pop()
                }
                None => None,
            }
        } else if req.is_local_only() {
            None
        } else {
            self.upstream().record(req, list).await.as_ref().and_then(ListRow::from_record)
        };
        Ok(row.map(|r| (r, local)))
    }

    /// A `listViewBasic` with the viewer's own state. `pds` is the
    /// creator's, for the avatar.
    pub(super) async fn list_basic(&self, viewer: &Viewer, row: &ListRow, pds: Option<&str>, count: Option<i64>) -> Result<Value, sqlx::Error> {
        let uris = [row.uri.clone()];
        let labels = self.load_labels(&uris).await?;
        let list_viewer = self.list_viewer(viewer, &uris).await?;
        let creator = uri_author(&row.uri).unwrap_or_default();
        let mut v = json!({
            "uri": row.uri,
            "cid": row.cid,
            "name": row.name,
            "purpose": row.purpose,
            "labels": labels.get(&row.uri).cloned().unwrap_or_default(),
            "viewer": list_viewer.view(&row.uri),
            "indexedAt": ts(row.indexed_at),
        });
        if let Some(url) = row.avatar_cid.as_deref().and_then(|cid| blob_url(pds, creator, cid)) {
            v["avatar"] = json!(url);
        }
        if let Some(n) = count {
            v["listItemCount"] = json!(n);
        }
        Ok(v)
    }

    /// Distinct subjects of a locally servable list.
    pub(super) async fn list_item_count(&self, list: &str) -> Result<i64, sqlx::Error> {
        crate::db::lists::item_count(self.pool(), list).await
    }

    /// A `listView`. `count` is the number of distinct subjects when known.
    pub(super) fn list_view(row: &ListRow, creators: &ProfileContext, count: Option<i64>, labels: &HashMap<String, Vec<Value>>, viewer: &ListViewer) -> Value {
        let creator = uri_author(&row.uri).unwrap_or_default();
        let mut v = json!({
            "uri": row.uri,
            "cid": row.cid,
            "creator": creators.view(creator, ProfileKind::View),
            "name": row.name,
            "purpose": row.purpose,
            "labels": labels.get(&row.uri).cloned().unwrap_or_default(),
            "viewer": viewer.view(&row.uri),
            "indexedAt": ts(row.indexed_at),
        });
        if let Some(d) = &row.description {
            v["description"] = json!(d);
        }
        if let Some(f) = &row.description_facets {
            v["descriptionFacets"] = f.clone();
        }
        let pds = creators.rows.get(creator).and_then(|r| r.pds_endpoint.as_deref());
        if let Some(url) = row.avatar_cid.as_deref().and_then(|cid| blob_url(pds, creator, cid)) {
            v["avatar"] = json!(url);
        }
        if let Some(n) = count {
            v["listItemCount"] = json!(n);
        }
        v
    }

    /// `app.bsky.graph.getList`.
    ///
    /// # Errors
    ///
    /// Database errors.
    pub async fn get_list(&self, viewer: &Viewer, req: &Requester, list: &str, limit: u32, cursor: Option<&str>) -> ViewResult<Value> {
        if !is_list_uri(list) {
            return Ok(Err(list_not_found()));
        }
        let owner = uri_author(list).unwrap_or_default().to_string();
        if let Some((error, message)) = self.gate(req, &owner).await?.profile_error() {
            return Ok(Err(ViewError::new(error, message)));
        }
        if self.gate(req, list).await?.is_hidden() {
            return Ok(Err(list_not_found()));
        }
        let local = self.is_ready_member(&owner).await?;

        // The list record.
        let Some((row, _)) = self.load_list(req, list).await? else {
            return Ok(Err(list_not_found()));
        };

        // The items: `(listitem uri, subject view)`, with the subject's DID.
        let (mut items, next, count): (Vec<ListedItem>, Option<String>, Option<i64>) = if local {
            let (at, after) = parse_local_cursor(cursor);
            let after_rkey = cursor_rkey(self.pool(), after.as_deref()).await?;
            let (list_author, list_rkey) = row.key.unwrap_or((0, 0));
            // One row per subject: duplicates are stored and ignored here.
            let rows: Vec<(String, String, DateTime<Utc>)> = sqlx::query_as(&format!(
                "SELECT {}, s.did, i.created_at FROM ( \
                   SELECT DISTINCT ON (subject) rkey, subject, created_at FROM list_items \
                   WHERE author = $1 AND list_rkey = $2 ORDER BY subject, created_at DESC, rkey DESC) i \
                 JOIN dids s ON s.id = i.subject, dids d \
                 WHERE d.id = $1 AND ($3::timestamptz IS NULL OR (i.created_at, i.rkey) < ($3, $4)) \
                 ORDER BY i.created_at DESC, i.rkey DESC LIMIT $5",
                crate::db::lists::uri_sql("d.did", crate::firehose::classifier::NSID_LISTITEM, "i.rkey")
            ))
            .bind(list_author)
            .bind(list_rkey)
            .bind(at)
            .bind(after_rkey)
            .bind(i64::from(limit))
            .fetch_all(self.pool())
            .await?;
            let next = (rows.len() == usize::try_from(limit).unwrap_or(usize::MAX))
                .then(|| rows.last().map(|(u, _, t)| local_cursor(*t, u)))
                .flatten();
            let count = self.list_item_count(list).await?;
            let dids: Vec<String> = rows.iter().map(|(_, d, _)| d.clone()).collect();
            let ctx = self.profile_context(viewer, req, &dids, None, super::Tiers::List).await?;
            let items = rows.into_iter().map(|(uri, did, _)| (uri, did.clone(), ctx.view(&did, ProfileKind::View))).collect();
            (items, next, Some(count))
        } else {
            let Some(found) = self.upstream().list_items(req, list, limit, cursor).await else {
                return Ok(Err(list_not_found()));
            };
            let (found, _, _) = self.normalized(viewer, req, found).await?;
            let items = found["items"]
                .as_array()
                .into_iter()
                .flatten()
                .filter_map(|item| {
                    let subject = item.get("subject")?;
                    Some((item.get("uri")?.as_str()?.to_string(), subject.get("did")?.as_str()?.to_string(), subject.clone()))
                })
                .collect();
            (items, found["cursor"].as_str().map(str::to_string), found["listItemCount"].as_i64())
        };

        // Hidden subjects are omitted.
        let dids: Vec<String> = items.iter().map(|(_, d, _)| d.clone()).collect();
        let gates = self.gate_many(req, &dids).await?;
        items.retain(|(_, d, _)| !gates.get(d).copied().unwrap_or(Gate::Visible).is_hidden());

        // A reference list leaves out subjects who opted out, except for
        // its owner, who sees them marked.
        let opted_out: HashSet<String> = if row.purpose == PURPOSE_REFERENCE {
            // The list by its key, read first; an odd subject by its text.
            let key = match row.key {
                Some(key) => Some(key),
                None => crate::db::lists::key(self.pool(), list).await?,
            };
            let (list_author, list_rkey) = key.map_or((None, None), |(a, k)| (Some(a), Some(k)));
            sqlx::query_scalar(&format!(
                "SELECT DISTINCT d.did FROM list_optouts o JOIN dids d ON d.id = o.author \
                 WHERE o.subj_author = $3 AND o.subj_rkey = $4 AND o.subj_coll = {} AND d.did = ANY($2) \
                 UNION SELECT d.did FROM odd_refs r JOIN list_optouts o ON o.author = r.author AND o.rkey = r.rkey AND o.subj_coll = 0 \
                   JOIN dids d ON d.id = o.author \
                 WHERE r.tbl = {} AND r.slot = {} AND r.raw = $1 AND d.did = ANY($2)",
                crate::db::ids::COLL_LIST,
                crate::db::ids::COLL_LIST_OPTOUT,
                crate::db::ids::Slot::Subject.id()
            ))
            .bind(list)
            .bind(&dids)
            .bind(list_author)
            .bind(list_rkey)
            .fetch_all(self.pool())
            .await?
            .into_iter()
            .collect()
        } else {
            HashSet::new()
        };
        let is_owner = viewer.did.as_deref() == Some(owner.as_str());
        let items: Vec<Value> = items
            .into_iter()
            .filter(|(_, did, _)| is_owner || !opted_out.contains(did))
            .map(|(uri, did, subject)| {
                let mut item = json!({"uri": uri, "subject": subject});
                if opted_out.contains(&did) {
                    item["subjectOptedOut"] = json!(true);
                }
                item
            })
            .collect();

        let creators = self.profile_context(viewer, req, std::slice::from_ref(&owner), None, super::Tiers::ListAnd { single: std::slice::from_ref(&owner) }).await?;
        let uris = [row.uri.clone()];
        let labels = self.load_labels(&uris).await?;
        let list_viewer = self.list_viewer(viewer, &uris).await?;
        let mut out = json!({"list": Self::list_view(&row, &creators, count, &labels, &list_viewer), "items": items});
        if let Some(c) = next {
            out["cursor"] = json!(c);
        }
        Ok(Ok(Self::emit(out)))
    }

    /// For each of `lists`, the newest `listitem` URI that names `subject`.
    pub(super) async fn memberships(&self, lists: &[String], subject: &str) -> Result<HashMap<String, String>, sqlx::Error> {
        let rows: Vec<(String, String)> = sqlx::query_as(&format!(
            "SELECT DISTINCT ON (u.uri) u.uri, at_uri(i.author, {}::smallint, i.rkey) \
             FROM unnest($1::text[]) AS u(uri) CROSS JOIN LATERAL ref_of(u.uri) k \
             JOIN list_items i ON k.coll = {} AND i.author = k.author AND i.list_rkey = k.rkey \
             WHERE i.subject = (SELECT s.id FROM dids s WHERE s.did = $2) \
             ORDER BY u.uri, i.created_at DESC, i.rkey DESC",
            crate::db::ids::COLL_LISTITEM,
            crate::db::ids::COLL_LIST
        ))
        .bind(lists)
        .bind(subject)
        .fetch_all(self.pool())
        .await?;
        Ok(rows.into_iter().collect())
    }

    /// `app.bsky.graph.getListsWithMembership`: the (ready member) viewer's
    /// own moderation and curate lists, each with `actor`'s list item when
    /// `actor` is on it. Reference lists are never included.
    ///
    /// # Errors
    ///
    /// Database errors.
    pub async fn get_lists_with_membership(&self, viewer: &Viewer, req: &Requester, actor: &str, purposes: &[String], limit: u32, cursor: Option<&str>) -> ViewResult<Value> {
        let Some(me) = viewer.did.clone() else {
            return Ok(Err(ViewError::new("AuthRequired", "Authentication required")));
        };
        let mut tokens: Vec<String> = Vec::new();
        for p in purposes {
            match purpose_token(p) {
                Some(token) if token != PURPOSE_REFERENCE => tokens.push(token.to_string()),
                _ => return Ok(Err(ViewError::new("InvalidRequest", "Error: purposes must be modlist or curatelist"))),
            }
        }
        if tokens.is_empty() {
            tokens = vec![PURPOSE_MOD.to_string(), PURPOSE_CURATE.to_string()];
        }
        let did = match self.subject(req, actor).await? {
            Ok(d) => d,
            Err(e) => return Ok(Err(e)),
        };
        let (at, after) = parse_local_cursor(cursor);
        let after_rkey = cursor_rkey(self.pool(), after.as_deref()).await?;
        let found: Vec<StoredList> = sqlx::query_as(&format!(
            "SELECT {} FROM lists l JOIN dids d ON d.id = l.author \
             WHERE d.did = $1 AND l.purpose = ANY($5) \
               AND ($2::timestamptz IS NULL OR (l.created_at, l.rkey) < ($2, $3)) \
             ORDER BY l.created_at DESC, l.rkey DESC LIMIT $4",
            list_columns()
        ))
        .bind(&me)
        .bind(at)
        .bind(after_rkey)
        .bind(i64::from(limit))
        .bind(&tokens)
        .fetch_all(self.pool())
        .await?;
        let rows = stored_lists(self.pool(), found).await?;
        let next = (rows.len() == usize::try_from(limit).unwrap_or(usize::MAX))
            .then(|| rows.last().map(|r| local_cursor(r.created_at, &r.uri)))
            .flatten();
        let uris: Vec<String> = rows.iter().map(|r| r.uri.clone()).collect();
        let counts = item_counts(self.pool(), &rows).await?;
        let member_of = self.memberships(&uris, &did).await?;
        let people = self.profile_context(viewer, req, &[me.clone(), did.clone()], None, super::Tiers::List).await?;
        let labels = self.load_labels(&uris).await?;
        let list_viewer = self.list_viewer(viewer, &uris).await?;
        let out_lists: Vec<Value> = rows
            .iter()
            .map(|r| {
                let mut entry = json!({"list": Self::list_view(r, &people, Some(counts.get(&r.uri).copied().unwrap_or(0)), &labels, &list_viewer)});
                if let Some(item) = member_of.get(&r.uri) {
                    entry["listItem"] = json!({"uri": item, "subject": people.view(&did, ProfileKind::View)});
                }
                entry
            })
            .collect();
        let mut out = json!({ "listsWithMembership": out_lists });
        if let Some(c) = next {
            out["cursor"] = json!(c);
        }
        Ok(Ok(Self::emit(out)))
    }

    /// `app.bsky.graph.getLists`. `purposes` may hold short forms or full
    /// tokens; an unknown value is `400 InvalidRequest`.
    ///
    /// # Errors
    ///
    /// Database errors.
    pub async fn get_lists(&self, viewer: &Viewer, req: &Requester, actor: &str, purposes: &[String], limit: u32, cursor: Option<&str>) -> ViewResult<Value> {
        let mut tokens: Vec<String> = Vec::new();
        for p in purposes {
            match purpose_token(p) {
                Some(token) => tokens.push(token.to_string()),
                None => return Ok(Err(ViewError::new("InvalidRequest", "Error: purposes must be modlist, curatelist or referencelist"))),
            }
        }
        let did = match self.subject(req, actor).await? {
            Ok(d) => d,
            Err(e) => return Ok(Err(e)),
        };
        if !self.is_ready_member(&did).await? {
            let Some(found) = self.upstream().lists(req, &did, limit, cursor, &tokens).await else {
                return Ok(Err(ViewError::new("InvalidRequest", "Profile not found")));
            };
            let (mut found, _, _) = self.normalized(viewer, req, found).await?;
            // Hidden lists go, and the viewer's own state replaces upstream's.
            let uris: Vec<String> = found["lists"]
                .as_array()
                .into_iter()
                .flatten()
                .filter_map(|l| l.get("uri").and_then(Value::as_str).map(str::to_string))
                .collect();
            let gates = self.gate_many(req, &uris).await?;
            let list_viewer = self.list_viewer(viewer, &uris).await?;
            if let Some(lists) = found["lists"].as_array_mut() {
                lists.retain(|l| {
                    l.get("uri")
                        .and_then(Value::as_str)
                        .is_some_and(|u| !gates.get(u).copied().unwrap_or(Gate::Visible).is_hidden())
                });
                for l in lists.iter_mut() {
                    let uri = l["uri"].as_str().unwrap_or_default().to_string();
                    l["viewer"] = list_viewer.view(&uri);
                }
            }
            return Ok(Ok(Self::emit(found)));
        }

        let (at, after) = parse_local_cursor(cursor);
        let after_rkey = cursor_rkey(self.pool(), after.as_deref()).await?;
        let found: Vec<StoredList> = sqlx::query_as(&format!(
            "SELECT {} FROM lists l JOIN dids d ON d.id = l.author \
             WHERE d.did = $1 AND (cardinality($5::text[]) = 0 OR l.purpose = ANY($5)) \
               AND ($2::timestamptz IS NULL OR (l.created_at, l.rkey) < ($2, $3)) \
             ORDER BY l.created_at DESC, l.rkey DESC LIMIT $4",
            list_columns()
        ))
        .bind(&did)
        .bind(at)
        .bind(after_rkey)
        .bind(i64::from(limit))
        .bind(&tokens)
        .fetch_all(self.pool())
        .await?;
        let rows = stored_lists(self.pool(), found).await?;
        let next = (rows.len() == usize::try_from(limit).unwrap_or(usize::MAX))
            .then(|| rows.last().map(|r| local_cursor(r.created_at, &r.uri)))
            .flatten();
        let uris: Vec<String> = rows.iter().map(|r| r.uri.clone()).collect();
        let counts = item_counts(self.pool(), &rows).await?;
        let gates = self.gate_many(req, &uris).await?;
        let creators = self.profile_context(viewer, req, std::slice::from_ref(&did), None, super::Tiers::List).await?;
        let labels = self.load_labels(&uris).await?;
        let list_viewer = self.list_viewer(viewer, &uris).await?;
        let lists: Vec<Value> = rows
            .iter()
            .filter(|r| !gates.get(&r.uri).copied().unwrap_or(Gate::Visible).is_hidden())
            .map(|r| Self::list_view(r, &creators, Some(counts.get(&r.uri).copied().unwrap_or(0)), &labels, &list_viewer))
            .collect();
        let mut out = json!({ "lists": lists });
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
    use crate::hydration::testing::{FixedUpstream, actor, exec, hydrator, hydrator_with};
    use crate::test_support::{fresh_db, records};

    const ALICE: &str = "did:plc:aaaaaaaaaaaaaaaaaaaaaaaa";
    const BOB: &str = "did:plc:bbbbbbbbbbbbbbbbbbbbbbbb";
    const CAROL: &str = "did:plc:cccccccccccccccccccccccc";
    const FAR: &str = "did:plc:zzzzzzzzzzzzzzzzzzzzzzzz";

    fn list_uri(owner: &str, rkey: &str) -> String {
        format!("at://{owner}/app.bsky.graph.list/{rkey}")
    }

    fn ago(minutes: u32) -> Option<DateTime<Utc>> {
        Some(Utc::now() - chrono::TimeDelta::minutes(i64::from(minutes)))
    }

    async fn list(p: &sqlx::PgPool, owner: &str, rkey: &str, name: &str, purpose: &str, age_minutes: u32) -> String {
        records::list_at(p, owner, rkey, name, purpose, ago(age_minutes)).await
    }

    async fn item(p: &sqlx::PgPool, list: &str, rkey: &str, subject: &str, age_minutes: u32) {
        let owner = uri_author(list).expect("owner");
        records::list_item_at(p, owner, rkey, list, subject, ago(age_minutes)).await;
    }

    #[test]
    fn purposes_and_list_uris() {
        assert_eq!(purpose_token("modlist"), Some(PURPOSE_MOD));
        assert_eq!(purpose_token(PURPOSE_CURATE), Some(PURPOSE_CURATE));
        assert_eq!(purpose_token("referencelist"), Some(PURPOSE_REFERENCE));
        assert_eq!(purpose_token("feedlist"), None);
        assert_eq!(purpose_token(""), None);
        assert!(is_list_uri(&list_uri(ALICE, "1")));
        for bad in [
            "at://did:plc:aaaaaaaaaaaaaaaaaaaaaaaa/app.bsky.graph.listitem/1",
            "at://did:plc:aaaaaaaaaaaaaaaaaaaaaaaa/tools.kryphocron.graph.list/1",
            "at://alice.test/app.bsky.graph.list/1",
            "at://did:plc:aaaaaaaaaaaaaaaaaaaaaaaa/app.bsky.graph.list",
            "https://example.com",
        ] {
            assert!(!is_list_uri(bad), "{bad}");
        }
    }

    /// B-35: a ready member's lists are served from local rows.
    #[tokio::test]
    async fn a_ready_members_lists_are_local() {
        let Some(db) = fresh_db("hydration_lists_local").await else {
            return;
        };
        let p = &db.pool;
        for (d, h) in [(ALICE, "alice.test"), (BOB, "bob.test"), (CAROL, "carol.test")] {
            actor(p, d, h, h).await;
        }
        exec(p, &format!("INSERT INTO members (did, source, ready_at) VALUES ('{ALICE}', 'admin', now())")).await;
        let mods = list(p, ALICE, "3kmods2222222", "Mods", PURPOSE_MOD, 3).await;
        let refs = list(p, ALICE, "3krefs2222222", "Refs", PURPOSE_REFERENCE, 2).await;
        list(p, ALICE, "3kcur22222222", "Curated", PURPOSE_CURATE, 1).await;
        // Bob is listed twice in one list: one item, one count.
        item(p, &mods, "3kitem222222a", BOB, 5).await;
        item(p, &mods, "3kitem222222b", BOB, 4).await;
        item(p, &mods, "3kitem222222c", CAROL, 3).await;
        item(p, &refs, "3kref2222222a", BOB, 2).await;
        item(p, &refs, "3kref2222222b", CAROL, 1).await;
        records::optout(p, CAROL, "3kopt2222222a", &refs).await;
        // Alice is a member and Bob is in scope: their handles resolve
        // locally (§11 G-9).
        let mut members = ScopeSets::default();
        members.members.insert(ALICE.into());
        members.scope.insert(BOB.into());
        let h = hydrator(p, members);
        let (anon, req) = (Viewer::anonymous(), Requester::test());

        let v = h.get_list(&anon, &req, &mods, 50, None).await.expect("db").expect("view");
        let v = v.inner();
        assert_eq!((&v["list"]["name"], &v["list"]["purpose"], &v["list"]["listItemCount"]), (&json!("Mods"), &json!(PURPOSE_MOD), &json!(2)));
        assert_eq!(v["list"]["creator"]["handle"], "alice.test");
        assert_eq!(v["list"]["description"], "about Mods");
        assert_eq!(v["list"]["viewer"], json!({}));
        let subjects: Vec<&str> = v["items"].as_array().expect("items").iter().map(|i| i["subject"]["did"].as_str().expect("did")).collect();
        assert_eq!(subjects, [CAROL, BOB]);
        assert_eq!(v["items"][1]["uri"], format!("at://{ALICE}/app.bsky.graph.listitem/3kitem222222b"));
        // Paging by the newest item of each subject.
        let first = h.get_list(&anon, &req, &mods, 1, None).await.expect("db").expect("view");
        let cursor = first.inner()["cursor"].as_str().expect("cursor").to_string();
        let second = h.get_list(&anon, &req, &mods, 1, Some(&cursor)).await.expect("db").expect("view");
        assert_eq!(second.inner()["items"][0]["subject"]["did"], BOB);

        // A reference list hides an opted-out subject from everyone but its
        // owner.
        let v = h.get_list(&anon, &req, &refs, 50, None).await.expect("db").expect("view");
        assert_eq!(v.inner()["items"].as_array().map(Vec::len), Some(1));
        let owner = Viewer {
            did: Some(ALICE.into()),
            hydrate: true,
        };
        let v = h.get_list(&owner, &req, &refs, 50, None).await.expect("db").expect("view");
        let opted: Vec<&Value> = v.inner()["items"].as_array().expect("items").iter().filter(|i| i["subjectOptedOut"] == true).collect();
        assert_eq!(opted.len(), 1);
        assert_eq!(opted[0]["subject"]["did"], CAROL);
        assert_eq!(v.inner()["list"]["viewer"]["muted"], false);
        // The opted-out subject sees its own opt-out on the list.
        let carol = Viewer {
            did: Some(CAROL.into()),
            hydrate: true,
        };
        let v = h.get_list(&carol, &req, &refs, 50, None).await.expect("db").expect("view");
        assert_eq!(v.inner()["list"]["viewer"]["referenceListOptOut"], format!("at://{CAROL}/app.bsky.graph.referencelistoptout/3kopt2222222a"));

        // The viewer's own list mute and list block show on the list.
        exec(p, &format!("INSERT INTO list_mutes (viewer_did, list_uri) VALUES ('{CAROL}', '{mods}')")).await;
        records::list_block(p, CAROL, "3kblock22222a", &mods).await;
        let v = h.get_list(&carol, &req, &mods, 50, None).await.expect("db").expect("view");
        assert_eq!(v.inner()["list"]["viewer"], json!({"muted": true, "blocked": format!("at://{CAROL}/app.bsky.graph.listblock/3kblock22222a")}));
        let v = h.get_list(&anon, &req, &mods, 50, None).await.expect("db").expect("view");
        assert_eq!(v.inner()["list"]["viewer"], json!({}));

        // getLists: newest first, with counts; purposes in either form.
        let all = h.get_lists(&anon, &req, "alice.test", &[], 50, None).await.expect("db").expect("view");
        let names: Vec<&str> = all.inner()["lists"].as_array().expect("lists").iter().map(|l| l["name"].as_str().expect("name")).collect();
        assert_eq!(names, ["Curated", "Refs", "Mods"]);
        assert_eq!(all.inner()["lists"][2]["listItemCount"], 2);
        for form in ["modlist", PURPOSE_MOD] {
            let only = h.get_lists(&anon, &req, ALICE, &[form.to_string()], 50, None).await.expect("db").expect("view");
            assert_eq!(only.inner()["lists"].as_array().map(Vec::len), Some(1), "{form}");
        }
        let two = h.get_lists(&anon, &req, ALICE, &["curatelist".into(), "referencelist".into()], 50, None).await.expect("db").expect("view");
        assert_eq!(two.inner()["lists"].as_array().map(Vec::len), Some(2));
        let e = h.get_lists(&anon, &req, ALICE, &["feedlist".into()], 50, None).await.expect("db").expect_err("bad purpose");
        assert_eq!(e.error, "InvalidRequest");
        // B-57: the owner's own lists with one actor's membership; reference
        // lists never appear.
        let w = h.get_lists_with_membership(&owner, &req, "bob.test", &[], 50, None).await.expect("db").expect("view");
        let entries = w.inner()["listsWithMembership"].as_array().expect("entries");
        assert_eq!(entries.iter().map(|e| e["list"]["name"].as_str().expect("name")).collect::<Vec<_>>(), ["Curated", "Mods"]);
        assert!(entries[0].get("listItem").is_none());
        assert_eq!(entries[1]["listItem"]["uri"], format!("at://{ALICE}/app.bsky.graph.listitem/3kitem222222b"));
        assert_eq!(entries[1]["listItem"]["subject"]["did"], BOB);
        let w = h.get_lists_with_membership(&owner, &req, BOB, &["curatelist".into()], 50, None).await.expect("db").expect("view");
        assert_eq!(w.inner()["listsWithMembership"].as_array().map(Vec::len), Some(1));
        assert!(h.get_lists_with_membership(&owner, &req, BOB, &["referencelist".into()], 50, None).await.expect("db").is_err());
        assert!(h.get_lists_with_membership(&anon, &req, BOB, &[], 50, None).await.expect("db").is_err());

        let page = h.get_lists(&anon, &req, ALICE, &[], 2, None).await.expect("db").expect("view");
        let cursor = page.inner()["cursor"].as_str().expect("cursor").to_string();
        let rest = h.get_lists(&anon, &req, ALICE, &[], 2, Some(&cursor)).await.expect("db").expect("view");
        assert_eq!(rest.inner()["lists"][0]["name"], "Mods");

        // Misses: not a list URI, and a list a member does not have.
        for miss in [list_uri(ALICE, "nope"), format!("at://{ALICE}/tools.kryphocron.graph.list/mods"), "garbage".to_string()] {
            let e = h.get_list(&anon, &req, &miss, 50, None).await.expect("db").expect_err("miss");
            assert_eq!((e.error, e.message.as_str()), ("InvalidRequest", "List not found"), "{miss}");
        }
        db.drop_db().await;
    }

    /// B-35: a list feed is local only when the list is served locally and
    /// every member is in scope; the viewer's mutes and blocks apply.
    #[tokio::test]
    async fn list_feeds() {
        use crate::hydration::testing::post;

        let Some(db) = fresh_db("hydration_list_feed").await else {
            return;
        };
        let p = &db.pool;
        for (d, h) in [(ALICE, "alice.test"), (BOB, "bob.test"), (CAROL, "carol.test")] {
            actor(p, d, h, h).await;
        }
        exec(p, &format!("INSERT INTO members (did, source, ready_at) VALUES ('{ALICE}', 'admin', now())")).await;
        let feed_list = list(p, ALICE, "3kfeed2222222", "Feed", PURPOSE_CURATE, 1).await;
        item(p, &feed_list, "3kitem222222a", BOB, 2).await;
        item(p, &feed_list, "3kitem222222b", CAROL, 1).await;
        let bobs = post(p, BOB, "3kbbbbbbbbbb2", "from bob", None, None).await;
        let carols = post(p, CAROL, "3kcccccccccc2", "from carol", None, None).await;
        for (uri, age) in [(&bobs, 2), (&carols, 1)] {
            crate::test_support::posts::put_feed_item(p, uri, uri, "post", chrono::Utc::now() - chrono::TimeDelta::minutes(age)).await;
        }
        let texts = |v: &Value| -> Vec<String> {
            v["feed"].as_array().expect("feed").iter().map(|i| i["post"]["record"]["text"].as_str().expect("text").to_string()).collect()
        };
        let (anon, req) = (Viewer::anonymous(), Requester::test());
        let mut all = ScopeSets::default();
        for d in [ALICE, BOB, CAROL] {
            all.scope.insert(d.into());
        }

        // Every member in scope: local rows, newest first, paged.
        let h = hydrator(p, all.clone());
        let v = h.get_list_feed(&anon, &req, &feed_list, 50, None).await.expect("db").expect("view");
        assert_eq!(texts(v.inner()), ["from carol", "from bob"]);
        let first = h.get_list_feed(&anon, &req, &feed_list, 1, None).await.expect("db").expect("view");
        let cursor = first.inner()["cursor"].as_str().expect("cursor").to_string();
        let second = h.get_list_feed(&anon, &req, &feed_list, 1, Some(&cursor)).await.expect("db").expect("view");
        assert_eq!(texts(second.inner()), ["from bob"]);
        // The viewer's mute drops the muted member's posts after hydration.
        exec(p, &format!("INSERT INTO mutes (viewer_did, subject_did) VALUES ('{ALICE}', '{CAROL}')")).await;
        let owner = Viewer {
            did: Some(ALICE.into()),
            hydrate: true,
        };
        let v = h.get_list_feed(&owner, &req, &feed_list, 50, None).await.expect("db").expect("view");
        assert_eq!(texts(v.inner()), ["from bob"]);

        // A member out of scope: upstream is asked; with no answer, the
        // in-scope members answer, with a cursor of their own.
        let mut some = ScopeSets::default();
        some.scope.insert(BOB.into());
        let partial = hydrator(p, some.clone());
        let v = partial.get_list_feed(&anon, &req, &feed_list, 1, None).await.expect("db").expect("view");
        assert_eq!(texts(v.inner()), ["from bob"]);
        let partial_cursor = v.inner()["cursor"].as_str().expect("cursor").to_string();
        assert!(partial_cursor.starts_with("PL:"));
        let v = partial.get_list_feed(&anon, &req, &feed_list, 1, Some(&partial_cursor)).await.expect("db").expect("view");
        assert_eq!(v.inner()["feed"], json!([]));
        let mut up = FixedUpstream::default();
        up.list_feeds.insert(feed_list.clone(), json!({"feed": [{"post": {
            "uri": format!("at://{CAROL}/app.bsky.feed.post/far"), "cid": "bafyfar", "author": {"did": CAROL, "handle": "carol.test"},
            "record": {"text": "from upstream"}, "indexedAt": "2026-01-01T00:00:00Z"}}], "cursor": "up"}));
        let asked = hydrator_with(p, some, Arc::new(up));
        let v = asked.get_list_feed(&anon, &req, &feed_list, 50, None).await.expect("db").expect("view");
        assert_eq!((texts(v.inner()), &v.inner()["cursor"]), (vec!["from upstream".to_string()], &json!("up")));
        // The muting viewer does not see the muted author upstream either.
        let v = asked.get_list_feed(&owner, &req, &feed_list, 50, None).await.expect("db").expect("view");
        assert_eq!(v.inner()["feed"], json!([]));

        // A list nobody can serve, and a URI that is not a list.
        for miss in [list_uri(BOB, "nope"), "garbage".to_string()] {
            let e = h.get_list_feed(&anon, &req, &miss, 50, None).await.expect("db").expect_err("miss");
            assert_eq!(e.error, "UnknownList", "{miss}");
        }
        db.drop_db().await;
    }

    /// B-35: any other owner's list is read upstream, even when rows are
    /// stored here.
    #[tokio::test]
    async fn other_lists_are_read_upstream_whatever_is_stored() {
        let Some(db) = fresh_db("hydration_lists_upstream").await else {
            return;
        };
        let p = &db.pool;
        actor(p, BOB, "bob.test", "Bob").await;
        // Bob is in scope but not a member: a stored row and one stored
        // item, which must not be what is served.
        let stored = list(p, BOB, "3klist222222a", "Stale name", PURPOSE_CURATE, 1).await;
        item(p, &stored, "3kitem222222a", ALICE, 1).await;
        let mut up = FixedUpstream::default();
        up.records.insert(
            stored.clone(),
            json!({"uri": stored, "cid": "bafyreal", "value": {"$type": NSID_LIST, "name": "Real name", "purpose": PURPOSE_MOD, "createdAt": "2026-01-01T00:00:00Z"}}),
        );
        up.list_items.insert(
            stored.clone(),
            json!({"items": [
                {"uri": format!("at://{BOB}/app.bsky.graph.listitem/a"), "subject": {"did": FAR, "handle": "far.example", "viewer": {"muted": true}, "labels": [{"val": "x"}]}},
                {"uri": format!("at://{BOB}/app.bsky.graph.listitem/b"), "subject": {"did": CAROL, "handle": "carol.example"}},
            ], "cursor": "next", "listItemCount": 40}),
        );
        up.lists.insert(BOB.to_string(), json!({"lists": [
            {"uri": stored, "cid": "bafyreal", "name": "Real name", "purpose": PURPOSE_MOD, "creator": {"did": BOB, "handle": "bob.test"}, "viewer": {"muted": true}, "indexedAt": "2026-01-01T00:00:00Z"},
        ], "cursor": "more"}));
        let mut sets = ScopeSets::default();
        sets.scope.insert(BOB.into());
        let h = hydrator_with(p, sets, Arc::new(up));
        let (anon, req) = (Viewer::anonymous(), Requester::test());

        let v = h.get_list(&anon, &req, &stored, 50, None).await.expect("db").expect("view");
        let v = v.inner();
        assert_eq!((&v["list"]["name"], &v["list"]["cid"], &v["list"]["listItemCount"]), (&json!("Real name"), &json!("bafyreal"), &json!(40)));
        assert_eq!(v["list"]["creator"]["handle"], "bob.test");
        assert_eq!(v["cursor"], "next");
        let items = v["items"].as_array().expect("items");
        assert_eq!(items.len(), 2);
        // Upstream viewer state and labels are replaced by local ones.
        assert_eq!(items[0]["subject"]["did"], FAR);
        assert_ne!(items[0]["subject"]["viewer"]["muted"], json!(true));
        assert!(items[0]["subject"].get("labels").is_none_or(|l| l == &json!([])));

        let v = h.get_lists(&anon, &req, BOB, &[], 50, None).await.expect("db").expect("view");
        assert_eq!(v.inner()["lists"][0]["name"], "Real name");
        assert_eq!(v.inner()["lists"][0]["viewer"], json!({}));
        assert_eq!(v.inner()["cursor"], "more");

        // No record upstream: not found, though a row is stored.
        let other = list(p, BOB, "3klist222222b", "Only stored", PURPOSE_CURATE, 1).await;
        let e = h.get_list(&anon, &req, &other, 50, None).await.expect("db").expect_err("miss");
        assert_eq!(e.message, "List not found");
        db.drop_db().await;
    }
}
