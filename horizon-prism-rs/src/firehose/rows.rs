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
//! **Listitems must name a list in their own repo.** A listitem whose `list`
//! is not `at://<its own repo>/app.bsky.graph.list/<rkey>` is not indexed:
//! [`row`] answers `Ok(None)` and the op is skipped as `unindexed`. Without
//! that, any DID could add subjects to anyone's list.

use chrono::{DateTime, Utc};
use serde_json::Value;

use super::classifier::{EcosystemKind, NSID_LIST};
use super::normalize::blob_cid;
use super::records::{RecordError, created_at};
use crate::db::locks::uri_author;

/// One column value.
#[derive(Debug, Clone, PartialEq)]
pub enum Cell {
    Text(Option<String>),
    TextArray(Vec<String>),
    Json(Option<Value>),
    Bool(Option<bool>),
    Time(Option<DateTime<Utc>>),
}

/// One row of a v3 record table. The writer adds `uri`, the owner column,
/// `rkey` (when the table has one), `cid` and `rev`.
#[derive(Debug, Clone, PartialEq)]
pub struct Row {
    pub table: &'static str,
    /// The column holding the record's author.
    pub owner_column: &'static str,
    /// rkey-self tables have no `rkey` column.
    pub has_rkey: bool,
    pub cells: Vec<(&'static str, Cell)>,
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
        EcosystemKind::Post
        | EcosystemKind::Like
        | EcosystemKind::Repost
        | EcosystemKind::Follow
        | EcosystemKind::Block
        | EcosystemKind::Profile => return None,
    })
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

fn strings(record: &Value, key: &str) -> Vec<String> {
    record
        .get(key)
        .and_then(Value::as_array)
        .map(|a| a.iter().filter_map(Value::as_str).map(str::to_string).collect())
        .unwrap_or_default()
}

fn self_labels(record: &Value) -> Vec<String> {
    record
        .pointer("/labels/values")
        .and_then(Value::as_array)
        .map(|vals| vals.iter().filter_map(|v| v.get("val").and_then(Value::as_str)).map(str::to_string).collect())
        .unwrap_or_default()
}

/// Whether `uri` is `at://<repo>/app.bsky.graph.list/<rkey>`.
fn own_list(uri: &str, repo: &str) -> bool {
    uri.strip_prefix("at://")
        .and_then(|rest| rest.strip_prefix(repo))
        .and_then(|rest| rest.strip_prefix('/'))
        .and_then(|rest| rest.strip_prefix(NSID_LIST))
        .and_then(|rest| rest.strip_prefix('/'))
        .is_some_and(|rkey| !rkey.is_empty() && !rkey.contains('/'))
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
    let stored = ("record", Cell::Json(Some(normalized.clone())));
    let tid = |table, owner_column, cells| Row {
        table,
        owner_column,
        has_rkey: true,
        cells,
    };
    Ok(Some(match kind {
        EcosystemKind::List => tid(
            "lists",
            "creator_did",
            vec![
                ("name", Cell::Text(Some(required(record, "name")?))),
                ("purpose", Cell::Text(Some(required(record, "purpose")?))),
                ("description", Cell::Text(text(record, "description"))),
                ("description_facets", Cell::Json(json(record, "descriptionFacets"))),
                ("avatar_cid", Cell::Text(record.get("avatar").and_then(blob_cid))),
                ("self_labels", Cell::TextArray(self_labels(record))),
                stored,
                created,
            ],
        ),
        EcosystemKind::ListItem => {
            let list = at_uri(record, "list")?;
            if !own_list(&list, repo) {
                return Ok(None);
            }
            let subject = text(record, "subject")
                .filter(|d| proto_blue::syntax::Did::is_valid(d))
                .ok_or_else(|| err("subject is not a DID"))?;
            tid(
                "list_items",
                "author_did",
                vec![("list_uri", Cell::Text(Some(list))), ("subject_did", Cell::Text(Some(subject))), created],
            )
        }
        EcosystemKind::ListBlock => tid(
            "list_blocks",
            "author_did",
            vec![("list_uri", Cell::Text(Some(at_uri(record, "subject")?))), created],
        ),
        EcosystemKind::ListOptout => tid(
            "list_optouts",
            "author_did",
            vec![("list_uri", Cell::Text(Some(at_uri(record, "subject")?))), created],
        ),
        EcosystemKind::StarterPack => tid(
            "starter_packs",
            "creator_did",
            vec![
                ("name", Cell::Text(Some(required(record, "name")?))),
                ("description", Cell::Text(text(record, "description"))),
                ("list_uri", Cell::Text(Some(at_uri(record, "list")?))),
                ("feeds", Cell::Json(json(record, "feeds"))),
                stored,
                created,
            ],
        ),
        EcosystemKind::LabelerService => Row {
            table: "labeler_services",
            owner_column: "creator_did",
            has_rkey: false,
            cells: vec![
                ("policies", Cell::Json(Some(json(record, "policies").ok_or_else(|| err("missing policies"))?))),
                ("reason_types", Cell::Json(json(record, "reasonTypes"))),
                ("subject_types", Cell::Json(json(record, "subjectTypes"))),
                ("subject_collections", Cell::Json(json(record, "subjectCollections"))),
                ("self_labels", Cell::TextArray(self_labels(record))),
                stored,
                created,
            ],
        },
        EcosystemKind::SelfRecord => Row {
            table: "actor_self_records",
            owner_column: "did",
            has_rkey: false,
            cells: vec![("collection", Cell::Text(Some(collection.to_string()))), stored],
        },
        EcosystemKind::Threadgate => tid(
            "threadgates",
            "author_did",
            vec![
                ("post_uri", Cell::Text(Some(at_uri(record, "post")?))),
                // Absent: anyone may reply. An empty array: nobody.
                ("allow", Cell::Json(json(record, "allow"))),
                ("hidden_replies", Cell::TextArray(strings(record, "hiddenReplies"))),
                stored,
                created,
            ],
        ),
        EcosystemKind::Postgate => tid(
            "postgates",
            "author_did",
            vec![
                ("post_uri", Cell::Text(Some(at_uri(record, "post")?))),
                ("embedding_rules", Cell::Json(json(record, "embeddingRules"))),
                ("detached_embedding_uris", Cell::TextArray(strings(record, "detachedEmbeddingUris"))),
                stored,
                created,
            ],
        ),
        EcosystemKind::PollPost => {
            let options = json(record, "options").filter(Value::is_array).ok_or_else(|| err("missing options"))?;
            tid(
                "nearhorizon_poll_posts",
                "author_did",
                vec![
                    ("text", Cell::Text(Some(text(record, "text").unwrap_or_default()))),
                    ("options", Cell::Json(Some(options))),
                    ("visibility", Cell::Text(Some(required(record, "visibility")?))),
                    ("multi_select", Cell::Bool(record.get("multiSelect").and_then(Value::as_bool))),
                    ("result_visibility", Cell::Text(text(record, "resultVisibility"))),
                    // A string that is not a datetime is stored as NULL.
                    (
                        "deadline",
                        Cell::Time(
                            text(record, "deadline")
                                .and_then(|s| DateTime::parse_from_rfc3339(&s).ok())
                                .map(|d| d.with_timezone(&Utc)),
                        ),
                    ),
                    stored,
                    created,
                ],
            )
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
        let own = build(EcosystemKind::ListItem, "app.bsky.graph.listitem", &item(&format!("at://{REPO}/app.bsky.graph.list/3k")))
            .expect("valid")
            .expect("indexed");
        assert_eq!(own.table, "list_items");
        assert_eq!(cell(&own, "subject_did"), &Cell::Text(Some("did:plc:bbbbbbbbbbbbbbbbbbbbbbbb".into())));
        for foreign in [
            "at://did:plc:cccccccccccccccccccccccc/app.bsky.graph.list/3k".to_string(),
            // A longer DID with this repo as its prefix is another repo.
            format!("at://{REPO}x/app.bsky.graph.list/3k"),
            format!("at://{REPO}/app.bsky.graph.starterpack/3k"),
            format!("at://{REPO}/app.bsky.graph.list/3k/extra"),
            format!("at://{REPO}/app.bsky.graph.list/"),
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
        assert_eq!((r.table, r.owner_column, r.has_rkey), ("lists", "creator_did", true));
        assert_eq!(cell(&r, "purpose"), &Cell::Text(Some("app.bsky.graph.defs#modlist".into())));
        assert_eq!(cell(&r, "avatar_cid"), &Cell::Text(Some("bafyavatar".into())));
        assert_eq!(cell(&r, "self_labels"), &Cell::TextArray(vec!["nudity".into()]));
        assert!(build(EcosystemKind::List, "app.bsky.graph.list", &json!({"name": "x"})).is_err(), "purpose is required");

        let block = json!({"subject": "at://did:plc:cccccccccccccccccccccccc/app.bsky.graph.list/3k"});
        let r = build(EcosystemKind::ListBlock, "app.bsky.graph.listblock", &block).expect("valid").expect("row");
        assert_eq!(cell(&r, "list_uri"), &Cell::Text(Some("at://did:plc:cccccccccccccccccccccccc/app.bsky.graph.list/3k".into())));

        // A threadgate with no `allow` stores NULL; with `[]`, an empty array.
        let gate = |allow: Option<Value>| {
            let mut g = json!({"post": format!("at://{REPO}/app.bsky.feed.post/3k"), "hiddenReplies": ["at://x/y/z"]});
            if let Some(a) = allow {
                g["allow"] = a;
            }
            build(EcosystemKind::Threadgate, "app.bsky.feed.threadgate", &g).expect("valid").expect("row")
        };
        assert_eq!(cell(&gate(None), "allow"), &Cell::Json(None));
        assert_eq!(cell(&gate(Some(json!([]))), "allow"), &Cell::Json(Some(json!([]))));
        assert_eq!(cell(&gate(None), "hidden_replies"), &Cell::TextArray(vec!["at://x/y/z".into()]));

        let own = json!({"allowSubscriptions": "followers"});
        let r = build(EcosystemKind::SelfRecord, "app.bsky.notification.declaration", &own).expect("valid").expect("row");
        assert_eq!((r.table, r.owner_column, r.has_rkey), ("actor_self_records", "did", false));
        assert_eq!(cell(&r, "collection"), &Cell::Text(Some("app.bsky.notification.declaration".into())));

        let poll = json!({"text": "which?", "options": [{"id": "a", "label": "A"}, {"id": "b", "label": "B"}],
            "visibility": "public", "multiSelect": true, "deadline": "tomorrow"});
        let r = build(EcosystemKind::PollPost, "app.nearhorizon.poll.post", &poll).expect("valid").expect("row");
        assert_eq!(cell(&r, "multi_select"), &Cell::Bool(Some(true)));
        assert_eq!(cell(&r, "deadline"), &Cell::Time(None), "an unparseable deadline is NULL");
        assert!(build(EcosystemKind::PollPost, "app.nearhorizon.poll.post", &json!({"text": "x", "visibility": "public"})).is_err());

        for kind in [EcosystemKind::Post, EcosystemKind::Profile] {
            assert_eq!(table(kind), None);
        }
        assert_eq!(table(EcosystemKind::Postgate), Some("postgates"));
    }
}
