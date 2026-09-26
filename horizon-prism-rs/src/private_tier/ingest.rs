//! Private-tier ingest (design-v2 §3.2, §4.5, §5.5, §6.11.1): LWW writes of
//! admitted `postPrivate`, `policy.audience` and `graph.block` records, with
//! `private_tombstones`, owner re-evaluation, and the purge and reconcile
//! helpers.
//!
//! **Defensive parsing (#235).** Aurora-Locus does not validate
//! `tools.kryphocron.*` records on write, so every field is read defensively:
//! absent optional fields are tolerated (the read path's
//! `validate_record_for_read` rejects structurally invalid records behind the
//! witness), and a field of the wrong type or an undecodable
//! `encodedContent` is [`IngestError::Malformed`], which the firehose
//! dead-letters. Nothing here panics on record content.

use chrono::{DateTime, Utc};
use serde_json::Value;
use sqlx::PgConnection;

use super::{Applied, IngestError, PrivateOp, owners};
use crate::db::locks::uri_author;
use crate::firehose::classifier::{NSID_AUDIENCE, NSID_POST_PRIVATE, NSID_PRIVATE_BLOCK};
use crate::firehose::jobs::Depth;
use crate::firehose::normalize::bytes_field;
use crate::firehose::records::created_at;

/// The stored fields of a `postPrivate` record.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PostPrivateFields {
    pub text: Option<String>,
    pub encoded_content: Option<Vec<u8>>,
    pub encoded_codec: Option<String>,
    pub encoded_generation: Option<String>,
    pub audience_uri: Option<String>,
    pub reply_root_uri: Option<String>,
    pub reply_parent_uri: Option<String>,
    pub created_at: DateTime<Utc>,
}

fn malformed(msg: impl Into<String>) -> IngestError {
    IngestError::Malformed(msg.into())
}

/// An optional string field: absent or null is `None`; any other non-string
/// is malformed.
fn opt_string(record: &Value, key: &str) -> Result<Option<String>, IngestError> {
    match record.get(key) {
        None | Some(Value::Null) => Ok(None),
        Some(Value::String(s)) => Ok(Some(s.clone())),
        Some(_) => Err(malformed(format!("{key} is not a string"))),
    }
}

fn at_uri(v: &Value, what: &str) -> Result<String, IngestError> {
    v.as_str()
        .filter(|u| uri_author(u).is_some_and(proto_blue::syntax::Did::is_valid))
        .map(str::to_string)
        .ok_or_else(|| malformed(format!("{what} is not an at-uri")))
}

impl PostPrivateFields {
    /// Parse a `postPrivate` record defensively.
    ///
    /// # Errors
    ///
    /// [`IngestError::Malformed`] for a field of the wrong type or an
    /// undecodable `encodedContent`.
    pub(crate) fn parse(record: &Value, now: DateTime<Utc>) -> Result<Self, IngestError> {
        if !record.is_object() {
            return Err(malformed("record is not an object"));
        }
        let text = opt_string(record, "text")?;
        let encoded_content = match record.get("encodedContent") {
            None | Some(Value::Null) => None,
            Some(v) => Some(bytes_field(v).ok_or_else(|| malformed("encodedContent is neither hex nor $bytes"))?),
        };
        let encoded_codec = opt_string(record, "encodedContentCodec")?;
        let encoded_generation = opt_string(record, "encodedContentGeneration")?;
        // audienceList: an at-uri string or `{uri}` (strong ref).
        let audience_uri = match record.get("audienceList") {
            None | Some(Value::Null) => None,
            Some(v @ Value::String(_)) => Some(at_uri(v, "audienceList")?),
            Some(Value::Object(m)) => Some(at_uri(m.get("uri").unwrap_or(&Value::Null), "audienceList.uri")?),
            Some(_) => return Err(malformed("audienceList is neither an at-uri nor a ref")),
        };
        let (reply_root_uri, reply_parent_uri) = match record.get("reply") {
            None | Some(Value::Null) => (None, None),
            Some(r @ Value::Object(_)) => {
                let part = |k: &str| {
                    r.get(k)
                        .and_then(|p| p.get("uri"))
                        .ok_or_else(|| malformed(format!("reply.{k}.uri missing")))
                        .and_then(|u| at_uri(u, &format!("reply.{k}.uri")))
                };
                (Some(part("root")?), Some(part("parent")?))
            }
            Some(_) => return Err(malformed("reply is not an object")),
        };
        Ok(Self {
            text,
            encoded_content,
            encoded_codec,
            encoded_generation,
            audience_uri,
            reply_root_uri,
            reply_parent_uri,
            created_at: created_at(record, now),
        })
    }
}

/// The stored fields of a `policy.audience` record.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct AudienceFields {
    pub mode: Option<String>,
    pub members: Vec<String>,
}

impl AudienceFields {
    /// Parse an audience record. Non-DID entries in `members` are dropped
    /// (tolerated); a non-array `members` is malformed.
    ///
    /// # Errors
    ///
    /// [`IngestError::Malformed`].
    pub(crate) fn parse(record: &Value) -> Result<Self, IngestError> {
        let mode = opt_string(record, "mode")?;
        let members = match record.get("members") {
            None | Some(Value::Null) => Vec::new(),
            Some(Value::Array(a)) => a
                .iter()
                .filter_map(|m| m.as_str().or_else(|| m.get("did").and_then(Value::as_str)))
                .filter(|d| proto_blue::syntax::Did::is_valid(d))
                .map(str::to_string)
                .collect(),
            Some(_) => return Err(malformed("members is not an array")),
        };
        Ok(Self { mode, members })
    }
}

fn subject_did(record: &Value) -> Result<String, IngestError> {
    record
        .get("subject")
        .and_then(Value::as_str)
        .filter(|d| proto_blue::syntax::Did::is_valid(d))
        .map(str::to_string)
        .ok_or_else(|| malformed("subject is not a DID"))
}

async fn tombstoned(conn: &mut PgConnection, uri: &str, rev: &str) -> Result<bool, sqlx::Error> {
    sqlx::query_scalar("SELECT EXISTS (SELECT 1 FROM private_tombstones WHERE uri = $1 AND rev >= $2)")
        .bind(uri)
        .bind(rev)
        .fetch_one(conn)
        .await
}

async fn has_owner_rows(conn: &mut PgConnection, did: &str) -> Result<bool, sqlx::Error> {
    sqlx::query_scalar(
        "SELECT EXISTS (SELECT 1 FROM private_posts WHERE author_did = $1) \
             OR EXISTS (SELECT 1 FROM private_audiences WHERE owner_did = $1)",
    )
    .bind(did)
    .fetch_one(conn)
    .await
}

/// Enqueue a `private` job unless one is already queued or running, so a
/// burst of private posts does not keep resetting it.
async fn enqueue_private(conn: &mut PgConnection, did: &str) -> Result<(), sqlx::Error> {
    sqlx::query(
        "INSERT INTO backfill_jobs (did, depth, state, attempts, not_before, updated_at) \
         VALUES ($1, $2, 'queued', 0, now(), now()) \
         ON CONFLICT (did, depth) DO UPDATE SET state = 'queued', attempts = 0, not_before = now(), \
           last_error = NULL, updated_at = now() \
         WHERE backfill_jobs.state NOT IN ('queued', 'running')",
    )
    .bind(did)
    .bind(Depth::Private.as_str())
    .execute(conn)
    .await?;
    Ok(())
}

/// `ingest::apply`: LWW create/update (§5.5) of an admitted private record.
///
/// A `postPrivate` or `audience` row for a DID with no prior rows re-evaluates
/// owner status (§6.11.1): an eligible DID becomes an owner and gets a
/// `private` job; an unconfirmed one gets the `private` job only, which
/// confirms eligibility. Blocks never make their author an owner.
///
/// # Errors
///
/// [`IngestError`].
pub(crate) async fn apply(conn: &mut PgConnection, op: &PrivateOp) -> Result<Applied, IngestError> {
    let now = Utc::now();
    if tombstoned(&mut *conn, &op.uri, &op.rev).await? {
        return Ok(Applied::default());
    }
    let author = op.author_did.as_str();
    let owner_record = op.nsid == NSID_POST_PRIVATE || op.nsid == NSID_AUDIENCE;
    let had_rows = owner_record && has_owner_rows(&mut *conn, author).await?;
    let changed = match op.nsid.as_str() {
        NSID_POST_PRIVATE => {
            let f = PostPrivateFields::parse(&op.record, now)?;
            sqlx::query(
                "INSERT INTO private_posts (uri, author_did, rkey, cid, rev, audience_uri, reply_root_uri, \
                   reply_parent_uri, encoded_content, encoded_codec, encoded_generation, plaintext_text, created_at) \
                 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13) \
                 ON CONFLICT (uri) DO UPDATE SET cid = EXCLUDED.cid, rev = EXCLUDED.rev, \
                   audience_uri = EXCLUDED.audience_uri, reply_root_uri = EXCLUDED.reply_root_uri, \
                   reply_parent_uri = EXCLUDED.reply_parent_uri, encoded_content = EXCLUDED.encoded_content, \
                   encoded_codec = EXCLUDED.encoded_codec, encoded_generation = EXCLUDED.encoded_generation, \
                   plaintext_text = EXCLUDED.plaintext_text, created_at = EXCLUDED.created_at \
                 WHERE private_posts.rev < EXCLUDED.rev",
            )
            .bind(&op.uri)
            .bind(author)
            .bind(&op.rkey)
            .bind(&op.cid)
            .bind(&op.rev)
            .bind(&f.audience_uri)
            .bind(&f.reply_root_uri)
            .bind(&f.reply_parent_uri)
            .bind(&f.encoded_content)
            .bind(&f.encoded_codec)
            .bind(&f.encoded_generation)
            .bind(&f.text)
            .bind(f.created_at)
            .execute(&mut *conn)
            .await?
            .rows_affected()
                > 0
        }
        NSID_AUDIENCE => {
            let f = AudienceFields::parse(&op.record)?;
            sqlx::query(
                "INSERT INTO private_audiences (uri, owner_did, rkey, cid, rev, mode, members) \
                 VALUES ($1, $2, $3, $4, $5, $6, $7) \
                 ON CONFLICT (uri) DO UPDATE SET cid = EXCLUDED.cid, rev = EXCLUDED.rev, \
                   mode = EXCLUDED.mode, members = EXCLUDED.members \
                 WHERE private_audiences.rev < EXCLUDED.rev",
            )
            .bind(&op.uri)
            .bind(author)
            .bind(&op.rkey)
            .bind(&op.cid)
            .bind(&op.rev)
            .bind(&f.mode)
            .bind(&f.members)
            .execute(&mut *conn)
            .await?
            .rows_affected()
                > 0
        }
        NSID_PRIVATE_BLOCK => {
            let subject = subject_did(&op.record)?;
            sqlx::query(
                "INSERT INTO private_blocks (uri, author_did, subject_did, rev, created_at) \
                 VALUES ($1, $2, $3, $4, $5) \
                 ON CONFLICT (uri) DO UPDATE SET subject_did = EXCLUDED.subject_did, rev = EXCLUDED.rev, \
                   created_at = EXCLUDED.created_at \
                 WHERE private_blocks.rev < EXCLUDED.rev",
            )
            .bind(&op.uri)
            .bind(author)
            .bind(&subject)
            .bind(&op.rev)
            .bind(created_at(&op.record, now))
            .execute(&mut *conn)
            .await?
            .rows_affected()
                > 0
        }
        other => return Err(malformed(format!("not a private-tier NSID: {other}"))),
    };
    let mut applied = Applied {
        changed,
        ..Applied::default()
    };
    if changed && owner_record && !had_rows {
        if owners::is_eligible(&mut *conn, author).await? {
            applied = Applied {
                changed,
                ..owners::reevaluate(&mut *conn, author).await?
            };
        }
        enqueue_private(&mut *conn, author).await?;
    }
    Ok(applied)
}

/// The private table and its author column for an at-uri, by NSID.
fn table_of(uri: &str) -> Option<(&'static str, &'static str)> {
    let collection = uri.strip_prefix("at://")?.split('/').nth(1)?;
    match collection {
        NSID_POST_PRIVATE => Some(("private_posts", "author_did")),
        NSID_AUDIENCE => Some(("private_audiences", "owner_did")),
        NSID_PRIVATE_BLOCK => Some(("private_blocks", "author_did")),
        _ => None,
    }
}

/// `ingest::delete`: `DELETE … WHERE rev <= R`; tombstone iff a row was
/// removed or the author is in S (§5.5); owner re-evaluation when a post or
/// audience row went.
///
/// # Errors
///
/// Database errors.
pub(crate) async fn delete(conn: &mut PgConnection, uri: &str, rev: &str, author_in_scope: bool) -> Result<Applied, sqlx::Error> {
    let Some((table, _)) = table_of(uri) else {
        return Ok(Applied::default());
    };
    let removed = sqlx::query(&format!("DELETE FROM {table} WHERE uri = $1 AND rev <= $2"))
        .bind(uri)
        .bind(rev)
        .execute(&mut *conn)
        .await?
        .rows_affected()
        > 0;
    if removed || author_in_scope {
        sqlx::query(
            "INSERT INTO private_tombstones (uri, rev) VALUES ($1, $2) \
             ON CONFLICT (uri) DO UPDATE SET rev = GREATEST(private_tombstones.rev, EXCLUDED.rev), deleted_at = now()",
        )
        .bind(uri)
        .bind(rev)
        .execute(&mut *conn)
        .await?;
    }
    let mut applied = Applied {
        changed: removed,
        ..Applied::default()
    };
    if removed
        && table != "private_blocks"
        && let Some(author) = uri_author(uri)
    {
        applied = Applied {
            changed: removed,
            ..owners::reevaluate(&mut *conn, author).await?
        };
    }
    Ok(applied)
}

/// Reconcile candidates: the DID's rows in `nsid` with `rev < below_rev`.
///
/// # Errors
///
/// Database errors.
pub(crate) async fn local_uris(conn: &mut PgConnection, did: &str, nsid: &str, below_rev: &str) -> Result<Vec<String>, sqlx::Error> {
    let Some((table, col)) = table_of(&format!("at://{did}/{nsid}/x")) else {
        return Ok(Vec::new());
    };
    sqlx::query_scalar(&format!("SELECT uri FROM {table} WHERE {col} = $1 AND rev < $2"))
        .bind(did)
        .bind(below_rev)
        .fetch_all(conn)
        .await
}

/// `ingest::purge_did` (account deletion): every private row authored by the
/// DID and its owner state, at most `limit` rows per table.
///
/// # Errors
///
/// Database errors.
pub(crate) async fn purge_did(conn: &mut PgConnection, did: &str, limit: i64) -> Result<u64, sqlx::Error> {
    let mut n = 0;
    for (table, col) in [
        ("private_posts", "author_did"),
        ("private_audiences", "owner_did"),
        ("private_blocks", "author_did"),
    ] {
        n += sqlx::query(&format!(
            "DELETE FROM {table} WHERE uri IN (SELECT uri FROM {table} WHERE {col} = $1 LIMIT $2)"
        ))
        .bind(did)
        .bind(limit)
        .execute(&mut *conn)
        .await?
        .rows_affected();
    }
    for table in ["private_owner_ready", "private_owner_eligible"] {
        n += sqlx::query(&format!("DELETE FROM {table} WHERE did = $1"))
            .bind(did)
            .execute(&mut *conn)
            .await?
            .rows_affected();
    }
    Ok(n)
}

/// A reply whose parent or root author is a community member (class M).
const REPLY_TO_MEMBER: &str = "EXISTS (SELECT 1 FROM members m WHERE m.did IN \
     (split_part(p.reply_parent_uri, '/', 3), split_part(p.reply_root_uri, '/', 3)))";

/// `ingest::purge_out_of_scope`: the DID's own private posts and audiences,
/// except class-M replies to members (§6.11.7, §9.4).
///
/// # Errors
///
/// Database errors.
pub(crate) async fn purge_out_of_scope(conn: &mut PgConnection, did: &str, limit: i64) -> Result<u64, sqlx::Error> {
    let mut n = sqlx::query(&format!(
        "DELETE FROM private_posts WHERE uri IN (SELECT p.uri FROM private_posts p \
         WHERE p.author_did = $1 AND NOT {REPLY_TO_MEMBER} LIMIT $2)"
    ))
    .bind(did)
    .bind(limit)
    .execute(&mut *conn)
    .await?
    .rows_affected();
    n += sqlx::query(
        "DELETE FROM private_audiences WHERE uri IN (SELECT uri FROM private_audiences WHERE owner_did = $1 LIMIT $2)",
    )
    .bind(did)
    .bind(limit)
    .execute(&mut *conn)
    .await?
    .rows_affected();
    Ok(n)
}

/// The private part of a `member_removed` purge (§6.11.7): private replies by
/// authors outside S admitted only because `did` was a member (no other
/// member among parent and root authors), and rule-2 private blocks of `did`
/// by authors outside S that are not private owners.
///
/// # Errors
///
/// Database errors.
pub(crate) async fn purge_member_removed(conn: &mut PgConnection, did: &str, limit: i64) -> Result<u64, sqlx::Error> {
    let mut n = sqlx::query(
        "DELETE FROM private_posts WHERE uri IN (SELECT p.uri FROM private_posts p \
         WHERE NOT EXISTS (SELECT 1 FROM scope_dids s WHERE s.did = p.author_did) \
           AND $1 IN (split_part(p.reply_parent_uri, '/', 3), split_part(p.reply_root_uri, '/', 3)) \
           AND NOT EXISTS (SELECT 1 FROM members m WHERE m.did <> $1 AND m.did IN \
               (split_part(p.reply_parent_uri, '/', 3), split_part(p.reply_root_uri, '/', 3))) \
         LIMIT $2)",
    )
    .bind(did)
    .bind(limit)
    .execute(&mut *conn)
    .await?
    .rows_affected();
    n += sqlx::query(
        "DELETE FROM private_blocks WHERE uri IN (SELECT b.uri FROM private_blocks b \
         WHERE b.subject_did = $1 \
           AND NOT EXISTS (SELECT 1 FROM scope_dids s WHERE s.did = b.author_did) \
           AND NOT ((EXISTS (SELECT 1 FROM private_posts x WHERE x.author_did = b.author_did) \
                     OR EXISTS (SELECT 1 FROM private_audiences x WHERE x.owner_did = b.author_did)) \
                    AND (EXISTS (SELECT 1 FROM members x WHERE x.did = b.author_did) \
                     OR EXISTS (SELECT 1 FROM private_owner_eligible x WHERE x.did = b.author_did))) \
         LIMIT $2)",
    )
    .bind(did)
    .bind(limit)
    .execute(&mut *conn)
    .await?
    .rows_affected();
    Ok(n)
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    fn now() -> DateTime<Utc> {
        Utc::now()
    }

    #[test]
    fn parses_encoded_and_plaintext_posts_in_both_encodings() {
        let hex = json!({"encodedContent": "00ff", "encodedContentCodec": "laquna/0.2", "encodedContentGeneration": "g1",
                         "audienceList": "at://did:plc:aaaaaaaaaaaaaaaaaaaaaaaa/tools.kryphocron.policy.audience/1",
                         "createdAt": "2026-09-26T12:00:00Z"});
        let f = PostPrivateFields::parse(&hex, now()).expect("hex");
        assert_eq!(f.encoded_content, Some(vec![0, 255]));
        assert_eq!(f.encoded_codec.as_deref(), Some("laquna/0.2"));
        assert!(f.audience_uri.is_some());
        let b64 = json!({"encodedContent": {"$bytes": "AP8"}, "encodedContentCodec": "laquna/0.2",
                         "audienceList": {"uri": "at://did:plc:aaaaaaaaaaaaaaaaaaaaaaaa/tools.kryphocron.policy.audience/1", "cid": "c"}});
        assert_eq!(PostPrivateFields::parse(&b64, now()).expect("b64").encoded_content, Some(vec![0, 255]));
        let plain = json!({"text": "hi"});
        let f = PostPrivateFields::parse(&plain, now()).expect("plain");
        assert_eq!((f.text.as_deref(), f.audience_uri), (Some("hi"), None));
    }

    #[test]
    fn malformed_fields_are_rejected_not_panicked_on() {
        for bad in [
            json!("not an object"),
            json!({"text": 5}),
            json!({"encodedContent": "zz-not-hex"}),
            json!({"encodedContent": 7}),
            json!({"audienceList": 3}),
            json!({"audienceList": "https://not-an-at-uri"}),
            json!({"audienceList": {"cid": "c"}}),
            json!({"reply": {"root": {"uri": "at://did:plc:a/x/1"}}}),
            json!({"reply": "x"}),
            json!({"encodedContentCodec": ["laquna"]}),
        ] {
            assert!(matches!(PostPrivateFields::parse(&bad, now()), Err(IngestError::Malformed(_))), "{bad}");
        }
        // Missing everything is tolerated here; the read path's validation
        // rejects it behind the witness.
        assert!(PostPrivateFields::parse(&json!({}), now()).is_ok());
        // An unparseable createdAt falls back to now.
        assert!(PostPrivateFields::parse(&json!({"text": "t", "createdAt": 5}), now()).is_ok());
    }

    #[test]
    fn audiences_tolerate_junk_members() {
        let a = AudienceFields::parse(&json!({"mode": "list", "members": ["did:plc:aaaaaaaaaaaaaaaaaaaaaaaa", 4, "bob", {"did": "did:plc:bbbbbbbbbbbbbbbbbbbbbbbb"}]}))
            .expect("audience");
        assert_eq!(a.members, ["did:plc:aaaaaaaaaaaaaaaaaaaaaaaa", "did:plc:bbbbbbbbbbbbbbbbbbbbbbbb"]);
        assert!(AudienceFields::parse(&json!({"members": "x"})).is_err());
        assert!(AudienceFields::parse(&json!({})).expect("empty").members.is_empty());
    }
}
