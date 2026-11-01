//! The record tables design-v3 added (§3.1, §3.2): one [`Row`] per record,
//! written by the writer's generic last-writer-wins upsert.
//!
//! | Collection | Table |
//! |---|---|
//! | `app.bsky.graph.list` | `lists` |
//! | `app.bsky.graph.listitem` | `list_items` |
//! | `app.bsky.graph.listblock` | `list_blocks` |
//! | `app.bsky.graph.referencelistoptout` | `list_optouts` |
//! | `app.bsky.graph.starterpack` | `starter_packs` |
//! | `app.bsky.labeler.service` (rkey `self`) | `labeler_services` |
//! | `app.bsky.notification.declaration`, `app.bsky.actor.status`, `app.bsky.actor.contentVisibilityDeclaration` (rkey `self`) | `actor_self_records` |
//! | `app.bsky.feed.threadgate` | `threadgates` |
//! | `app.bsky.feed.postgate` | `postgates` |
//! | `app.nearhorizon.poll.post` | `nearhorizon_poll_posts` |
//!
//! Every one is admitted by rule 1 alone (its author is in scope). A record
//! missing a field its table requires is a deterministic failure, like any
//! unparseable record.
//!
//! These tables are keyed by ids ([`Layout::Keyed`]): a row is
//! `(author, rkey)`, a labeler service `(author)` and a self record
//! `(author, coll)`. A table that shows its record keeps it whole, encoded;
//! the only other columns are what a query filters or searches on, and the
//! references, each as `(*_author, *_coll, *_rkey)`. The plural records keep
//! their own layout, by URI ([`Layout::ByUri`]).
//!
//! **Listitems must name a list in their own repo.** A listitem whose `list`
//! is not `at://<its own repo>/app.bsky.graph.list/<rkey>` with a valid
//! record key is not indexed: [`row`] answers `Ok(None)` and the op is
//! skipped as `unindexed`. Without that, any DID could add subjects to
//! anyone's list.

use chrono::{DateTime, Utc};
use serde_json::Value;

use super::classifier::{EcosystemKind, NSID_LIST};
use super::records::{RecordError, created_at};
use crate::db::ids::{
    AccountId, COLL_LABELER, COLL_LIST, COLL_LIST_OPTOUT, COLL_LISTBLOCK, COLL_LISTITEM, COLL_POLL_POST, COLL_POSTGATE, COLL_STARTER_PACK,
    COLL_THREADGATE, Slot, UriParts, collection_id, uri_parts,
};
use crate::db::locks::uri_author;

/// One column value.
#[derive(Debug, Clone, PartialEq)]
pub enum Cell {
    Text(Option<String>),
    Json(Option<Value>),
    Bool(Option<bool>),
    Time(Option<DateTime<Utc>>),
    /// A reference, as the columns `<name>_author`, `<name>_coll` and
    /// `<name>_rkey`; an odd one keeps its text in `odd_refs` at this slot.
    Ref(Slot, String),
    /// An account, as its `dids` id.
    Account(String),
    /// A record key of the author's own repo, as stored.
    OwnRkey(String),
    /// The `simple` text vector of this text.
    Tsv(String),
}

/// How a keyed table's rows are keyed besides their author.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RowKey {
    /// `(author, rkey)`.
    Rkey,
    /// `(author)`: a record whose key is always `self`.
    Author,
    /// `(author, coll)`: records of several collections whose key is
    /// always `self`.
    Coll,
}

/// How a table stores its rows.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Layout {
    /// By URI, with DID and rev text: `uri`, the owner column, `rkey`,
    /// `cid` and `rev` added by the writer.
    ByUri { owner_column: &'static str },
    /// By ids. The writer adds the key, `cid`, `rev` and, when `record`, the
    /// record encoded. `coll` is the records' collection id, which names an
    /// odd reference's owner table (`odd_refs.tbl`).
    Keyed { coll: i16, key: RowKey, record: bool },
}

/// One row of a v3 record table.
#[derive(Debug, Clone, PartialEq)]
pub struct Row {
    pub table: &'static str,
    pub layout: Layout,
    pub cells: Vec<(&'static str, Cell)>,
}

impl Row {
    /// The URI text of the reference cell `name`.
    #[must_use]
    pub fn reference(&self, name: &str) -> Option<&str> {
        self.cells.iter().find_map(|(n, cell)| match cell {
            Cell::Ref(_, uri) if *n == name => Some(uri.as_str()),
            _ => None,
        })
    }
}

/// The table of the plural collections (plural-pronouns design §2.3).
pub const PLURAL_TABLE: &str = "plural_records";

/// A plural record larger than this, serialized, is stored with `record`
/// NULL (plural-pronouns design §2.6).
pub const PLURAL_RECORD_MAX_BYTES: usize = 64 * 1024;

/// The stored value of a plural record: its normalized JSON, or `None`
/// over [`PLURAL_RECORD_MAX_BYTES`].
#[must_use]
pub fn plural_value(normalized: &Value) -> Option<Value> {
    serde_json::to_vec(normalized)
        .is_ok_and(|b| b.len() <= PLURAL_RECORD_MAX_BYTES)
        .then(|| normalized.clone())
}

/// The table of a v3 record kind, for deletes.
#[must_use]
pub fn table(kind: EcosystemKind) -> Option<&'static str> {
    Some(match kind {
        EcosystemKind::List => "lists",
        EcosystemKind::ListItem => "list_items",
        EcosystemKind::ListBlock => "list_blocks",
        EcosystemKind::ListOptout => "list_optouts",
        EcosystemKind::StarterPack => "starter_packs",
        EcosystemKind::LabelerService => "labeler_services",
        EcosystemKind::SelfRecord => "actor_self_records",
        EcosystemKind::Threadgate => "threadgates",
        EcosystemKind::Postgate => "postgates",
        EcosystemKind::PollPost => "nearhorizon_poll_posts",
        EcosystemKind::Plural => PLURAL_TABLE,
        EcosystemKind::Post
        | EcosystemKind::Like
        | EcosystemKind::Repost
        | EcosystemKind::Follow
        | EcosystemKind::Block
        | EcosystemKind::Profile => return None,
    })
}

/// How a v3 record table keyed by ids is keyed; `None` for one stored by
/// URI (the plural records) or any other name.
#[must_use]
pub fn key_of(table: &str) -> Option<RowKey> {
    Some(match table {
        "lists" | "list_items" | "list_blocks" | "list_optouts" | "starter_packs" | "threadgates" | "postgates" | "nearhorizon_poll_posts" => {
            RowKey::Rkey
        }
        "labeler_services" => RowKey::Author,
        "actor_self_records" => RowKey::Coll,
        _ => return None,
    })
}

/// SQL: the condition that a row of a keyed table has the key `$1` (its
/// author) and `$2` (a bigint: its rkey, or its coll; unused for a table
/// keyed by its author alone).
#[must_use]
pub fn key_where(key: RowKey) -> &'static str {
    match key {
        RowKey::Rkey => "author = $1 AND rkey = $2",
        RowKey::Author => "author = $1 AND $2::bigint IS NOT NULL",
        RowKey::Coll => "author = $1 AND coll = $2",
    }
}

/// The key of the row of a keyed table that `uri` names, read-only: its
/// author and its rkey (or its coll; 0 for a table keyed by its author
/// alone), as [`key_where`] binds them. `None` when the URI names no stored
/// key.
///
/// # Errors
///
/// Database errors.
pub async fn key_for<'e, E: sqlx::PgExecutor<'e>>(e: E, key: RowKey, uri: &str) -> Result<Option<(AccountId, i64)>, sqlx::Error> {
    if key == RowKey::Rkey {
        return Ok(crate::db::ids::whole_key(e, uri).await?.map(|(a, _, k)| (a, k)));
    }
    let UriParts::Whole { did, coll, .. } = uri_parts(uri) else {
        return Ok(None);
    };
    let author = crate::db::ids::account_id(e, &did).await?;
    Ok(author.map(|a| (a, if key == RowKey::Coll { i64::from(coll) } else { 0 })))
}

fn err(msg: impl Into<String>) -> RecordError {
    RecordError(msg.into())
}

fn text(record: &Value, key: &str) -> Option<String> {
    record.get(key).and_then(Value::as_str).map(str::to_string)
}

fn required(record: &Value, key: &str) -> Result<String, RecordError> {
    text(record, key).ok_or_else(|| err(format!("missing {key}")))
}

/// An at-uri field.
fn at_uri(record: &Value, key: &str) -> Result<String, RecordError> {
    text(record, key)
        .filter(|u| uri_author(u).is_some())
        .ok_or_else(|| err(format!("{key} is not an at-uri")))
}

fn json(record: &Value, key: &str) -> Option<Value> {
    record.get(key).filter(|v| !v.is_null()).cloned()
}

/// The record key of `uri` when it is `at://<repo>/app.bsky.graph.list/<rkey>`
/// with a valid record key.
fn own_list(uri: &str, repo: &str) -> Option<String> {
    uri.strip_prefix("at://")
        .and_then(|rest| rest.strip_prefix(repo))
        .and_then(|rest| rest.strip_prefix('/'))
        .and_then(|rest| rest.strip_prefix(NSID_LIST))
        .and_then(|rest| rest.strip_prefix('/'))
        .filter(|rkey| proto_blue::syntax::RecordKey::is_valid(rkey))
        .map(str::to_string)
}

/// A JSON field as SQL's `->>` reads it: a string's text, any other value's
/// JSON text, nothing for null.
fn field_text(record: &Value, key: &str) -> Option<String> {
    match record.get(key)? {
        Value::Null => None,
        Value::String(s) => Some(s.clone()),
        other => Some(other.to_string()),
    }
}

/// The row for a v3 record of `kind` in `repo`.
///
/// - `record` is the record as received; `normalized` is its stored JSON
///   (§II-5.8).
/// - `Ok(None)`: the record is not indexed (a listitem naming a list
///   outside its own repo).
///
/// # Errors
///
/// A record missing a field its table requires.
pub fn row(
    kind: EcosystemKind,
    repo: &str,
    collection: &str,
    record: &Value,
    normalized: &Value,
    now: DateTime<Utc>,
) -> Result<Option<Row>, RecordError> {
    let created = ("created_at", Cell::Time(Some(created_at(record, now))));
    let keyed = |table, coll, record: bool, cells| Row {
        table,
        layout: Layout::Keyed { coll, key: RowKey::Rkey, record },
        cells,
    };
    Ok(Some(match kind {
        EcosystemKind::List => {
            // The name is read from the record; a list must have one.
            required(record, "name")?;
            keyed("lists", COLL_LIST, true, vec![("purpose", Cell::Text(Some(required(record, "purpose")?))), created])
        }
        EcosystemKind::ListItem => {
            let list = at_uri(record, "list")?;
            let Some(list_rkey) = own_list(&list, repo) else {
                return Ok(None);
            };
            let subject = text(record, "subject")
                .filter(|d| proto_blue::syntax::Did::is_valid(d))
                .ok_or_else(|| err("subject is not a DID"))?;
            keyed(
                "list_items",
                COLL_LISTITEM,
                false,
                vec![("list_rkey", Cell::OwnRkey(list_rkey)), ("subject", Cell::Account(subject)), created],
            )
        }
        EcosystemKind::ListBlock => keyed(
            "list_blocks",
            COLL_LISTBLOCK,
            false,
            vec![("subj", Cell::Ref(Slot::Subject, at_uri(record, "subject")?)), created],
        ),
        EcosystemKind::ListOptout => keyed(
            "list_optouts",
            COLL_LIST_OPTOUT,
            false,
            vec![("subj", Cell::Ref(Slot::Subject, at_uri(record, "subject")?)), created],
        ),
        EcosystemKind::StarterPack => {
            let name = required(record, "name")?;
            // As the old generated column read them.
            let search = format!("{name} {}", text(record, "description").unwrap_or_default());
            keyed(
                "starter_packs",
                COLL_STARTER_PACK,
                true,
                vec![("list", Cell::Ref(Slot::List, at_uri(record, "list")?)), ("search_tsv", Cell::Tsv(search)), created],
            )
        }
        EcosystemKind::LabelerService => {
            json(record, "policies").ok_or_else(|| err("missing policies"))?;
            Row {
                table: "labeler_services",
                layout: Layout::Keyed { coll: COLL_LABELER, key: RowKey::Author, record: true },
                cells: vec![created],
            }
        }
        EcosystemKind::SelfRecord => {
            let coll = collection_id(collection).ok_or_else(|| err("not a self record collection"))?;
            let hide = normalized.get("hideFromAlgorithmicRecommendations") == Some(&Value::Bool(true));
            Row {
                table: "actor_self_records",
                layout: Layout::Keyed { coll, key: RowKey::Coll, record: true },
                cells: vec![
                    ("hide_from_recs", Cell::Bool(Some(hide))),
                    ("allow_subscriptions", Cell::Text(field_text(normalized, "allowSubscriptions"))),
                ],
            }
        }
        EcosystemKind::Threadgate => keyed(
            "threadgates",
            COLL_THREADGATE,
            true,
            vec![("post", Cell::Ref(Slot::Subject, at_uri(record, "post")?)), created],
        ),
        EcosystemKind::Postgate => keyed(
            "postgates",
            COLL_POSTGATE,
            true,
            vec![("post", Cell::Ref(Slot::Subject, at_uri(record, "post")?)), created],
        ),
        EcosystemKind::PollPost => {
            json(record, "options").filter(Value::is_array).ok_or_else(|| err("missing options"))?;
            keyed(
                "nearhorizon_poll_posts",
                COLL_POLL_POST,
                true,
                vec![("visibility", Cell::Text(Some(required(record, "visibility")?))), created],
            )
        }
        // Any author's (plural-pronouns design §2.2): the record whole, or
        // NULL over the size bound; `indexed_at` on every write, for the
        // listing's reconcile (§2.4). Not an object: a broken record.
        EcosystemKind::Plural => {
            if !record.is_object() {
                return Err(err("record is not an object"));
            }
            Row {
                table: PLURAL_TABLE,
                layout: Layout::ByUri { owner_column: "did" },
                cells: vec![
                    ("collection", Cell::Text(Some(collection.to_string()))),
                    ("record", Cell::Json(plural_value(normalized))),
                    ("indexed_at", Cell::Time(Some(now))),
                ],
            }
        }
        EcosystemKind::Post
        | EcosystemKind::Like
        | EcosystemKind::Repost
        | EcosystemKind::Follow
        | EcosystemKind::Block
        | EcosystemKind::Profile => return Err(err("not a v3 record table")),
    }))
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    const REPO: &str = "did:plc:aaaaaaaaaaaaaaaaaaaaaaaa";

    fn build(kind: EcosystemKind, collection: &str, record: &Value) -> Result<Option<Row>, RecordError> {
        row(kind, REPO, collection, record, record, Utc::now())
    }

    fn cell<'a>(row: &'a Row, name: &str) -> &'a Cell {
        &row.cells.iter().find(|(n, _)| *n == name).unwrap_or_else(|| panic!("no {name}")).1
    }

    #[test]
    fn a_listitem_must_name_a_list_in_its_own_repo() {
        let item = |list: &str| json!({"list": list, "subject": "did:plc:bbbbbbbbbbbbbbbbbbbbbbbb"});
        let own = build(EcosystemKind::ListItem, "app.bsky.graph.listitem", &item(&format!("at://{REPO}/app.bsky.graph.list/3kaaaaaaaaaaa")))
            .expect("valid")
            .expect("indexed");
        assert_eq!(own.table, "list_items");
        assert_eq!(cell(&own, "subject"), &Cell::Account("did:plc:bbbbbbbbbbbbbbbbbbbbbbbb".into()));
        assert_eq!(cell(&own, "list_rkey"), &Cell::OwnRkey("3kaaaaaaaaaaa".into()));
        for foreign in [
            "at://did:plc:cccccccccccccccccccccccc/app.bsky.graph.list/3k".to_string(),
            // A longer DID with this repo as its prefix is another repo.
            format!("at://{REPO}x/app.bsky.graph.list/3k"),
            format!("at://{REPO}/app.bsky.graph.starterpack/3k"),
            format!("at://{REPO}/app.bsky.graph.list/3k/extra"),
            format!("at://{REPO}/app.bsky.graph.list/"),
            // A key that is not a valid record key names no list.
            format!("at://{REPO}/app.bsky.graph.list/a b"),
        ] {
            assert_eq!(build(EcosystemKind::ListItem, "app.bsky.graph.listitem", &item(&foreign)), Ok(None), "{foreign}");
        }
        // Not an at-uri, or no DID subject: a broken record.
        assert!(build(EcosystemKind::ListItem, "app.bsky.graph.listitem", &item("nope")).is_err());
        let no_subject = json!({"list": format!("at://{REPO}/app.bsky.graph.list/3k"), "subject": "bob"});
        assert!(build(EcosystemKind::ListItem, "app.bsky.graph.listitem", &no_subject).is_err());
    }

    #[test]
    fn rows_carry_their_tables_columns() {
        let list = json!({"name": "Mods", "purpose": "app.bsky.graph.defs#modlist", "description": "d",
            "avatar": {"$type": "blob", "ref": {"$link": "bafyavatar"}},
            "labels": {"values": [{"val": "nudity"}]}});
        let r = build(EcosystemKind::List, "app.bsky.graph.list", &list).expect("valid").expect("row");
        assert_eq!((r.table, r.layout), ("lists", Layout::Keyed { coll: COLL_LIST, key: RowKey::Rkey, record: true }));
        assert_eq!(cell(&r, "purpose"), &Cell::Text(Some("app.bsky.graph.defs#modlist".into())));
        assert!(build(EcosystemKind::List, "app.bsky.graph.list", &json!({"name": "x"})).is_err(), "purpose is required");
        assert!(build(EcosystemKind::List, "app.bsky.graph.list", &json!({"purpose": "x"})).is_err(), "name is required");

        let block = json!({"subject": "at://did:plc:cccccccccccccccccccccccc/app.bsky.graph.list/3k"});
        let r = build(EcosystemKind::ListBlock, "app.bsky.graph.listblock", &block).expect("valid").expect("row");
        assert_eq!(r.reference("subj"), Some("at://did:plc:cccccccccccccccccccccccc/app.bsky.graph.list/3k"));
        assert_eq!(cell(&r, "subj"), &Cell::Ref(Slot::Subject, "at://did:plc:cccccccccccccccccccccccc/app.bsky.graph.list/3k".into()));

        let gate = json!({"post": format!("at://{REPO}/app.bsky.feed.post/3k"), "hiddenReplies": ["at://x/y/z"]});
        let r = build(EcosystemKind::Threadgate, "app.bsky.feed.threadgate", &gate).expect("valid").expect("row");
        assert_eq!(r.reference("post"), Some(format!("at://{REPO}/app.bsky.feed.post/3k").as_str()));
        assert!(matches!(r.layout, Layout::Keyed { record: true, .. }), "a gate keeps its record");

        let pack = json!({"name": "Rust", "description": "people", "list": format!("at://{REPO}/app.bsky.graph.list/3k")});
        let r = build(EcosystemKind::StarterPack, "app.bsky.graph.starterpack", &pack).expect("valid").expect("row");
        assert_eq!(cell(&r, "search_tsv"), &Cell::Tsv("Rust people".into()));

        let own = json!({"allowSubscriptions": "followers"});
        let r = build(EcosystemKind::SelfRecord, "app.bsky.notification.declaration", &own).expect("valid").expect("row");
        assert_eq!((r.table, r.layout), ("actor_self_records", Layout::Keyed { coll: 13, key: RowKey::Coll, record: true }));
        assert_eq!(cell(&r, "allow_subscriptions"), &Cell::Text(Some("followers".into())));
        assert_eq!(cell(&r, "hide_from_recs"), &Cell::Bool(Some(false)));
        let hidden = json!({"hideFromAlgorithmicRecommendations": true});
        let r = build(EcosystemKind::SelfRecord, "app.bsky.actor.contentVisibilityDeclaration", &hidden).expect("valid").expect("row");
        assert_eq!((cell(&r, "hide_from_recs"), cell(&r, "allow_subscriptions")), (&Cell::Bool(Some(true)), &Cell::Text(None)));

        let labeler = build(EcosystemKind::LabelerService, "app.bsky.labeler.service", &json!({"policies": {}})).expect("valid").expect("row");
        assert_eq!(labeler.layout, Layout::Keyed { coll: COLL_LABELER, key: RowKey::Author, record: true });
        assert!(build(EcosystemKind::LabelerService, "app.bsky.labeler.service", &json!({})).is_err(), "policies are required");

        let poll = json!({"text": "which?", "options": [{"id": "a", "label": "A"}, {"id": "b", "label": "B"}],
            "visibility": "public", "multiSelect": true, "deadline": "tomorrow"});
        let r = build(EcosystemKind::PollPost, "app.nearhorizon.poll.post", &poll).expect("valid").expect("row");
        assert_eq!(cell(&r, "visibility"), &Cell::Text(Some("public".into())));
        assert!(build(EcosystemKind::PollPost, "app.nearhorizon.poll.post", &json!({"text": "x", "visibility": "public"})).is_err());

        let plural = build(EcosystemKind::Plural, "app.nearhorizon.plural.member", &json!({"name": "a"})).expect("valid").expect("row");
        assert_eq!(plural.layout, Layout::ByUri { owner_column: "did" });

        for kind in [EcosystemKind::Post, EcosystemKind::Profile] {
            assert_eq!(table(kind), None);
        }
        assert_eq!(table(EcosystemKind::Postgate), Some("postgates"));
        assert_eq!((key_of("postgates"), key_of("labeler_services"), key_of(PLURAL_TABLE)), (Some(RowKey::Rkey), Some(RowKey::Author), None));
        assert_eq!(key_where(RowKey::Coll), "author = $1 AND coll = $2");
    }
}
