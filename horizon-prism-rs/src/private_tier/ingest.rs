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

use std::collections::HashMap;

use chrono::{DateTime, Utc};
use serde_json::Value;
use sqlx::{PgConnection, PgPool};

use super::{Applied, IngestError, PrivateOp, PublicNotify, QuoteRef, owners};
use crate::db::locks::uri_author;
use crate::firehose::classifier::{
    NSID_AUDIENCE, NSID_POST, NSID_POST_PRIVATE, NSID_POSTGATE, NSID_PRIVATE_BLOCK, NSID_THREADGATE,
};
use crate::firehose::jobs::{self, Depth};
use crate::firehose::normalize::{bytes_field, strong_ref_cid};
use crate::firehose::records::created_at;
use crate::firehose::writer::WriteSource;

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
    /// Strong-ref cids (migration 0002); absent in some records.
    pub reply_root_cid: Option<String>,
    pub reply_parent_cid: Option<String>,
    pub created_at: DateTime<Utc>,
    /// The display fields (private-posting design §4.2, migration 0023),
    /// as received: `facets` an array, `embed` and `labels` objects,
    /// `langs` strings. Not encoded by the PDS.
    pub display: DisplayFields,
}

/// A `postPrivate` record's display fields and what is derived from them
/// (private-posting design §3.1, §4.2).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct DisplayFields {
    pub facets: Option<Value>,
    pub embed: Option<Value>,
    pub langs: Option<Vec<String>>,
    /// The record's `labels` (`#selfLabels`) object.
    pub self_labels: Option<Value>,
    /// The quoted URI, from `embed` in either `$type` form.
    pub quote_uri: Option<String>,
    /// DIDs of the `#mention` facet features, in either form.
    pub mention_dids: Vec<String>,
    /// Fields dropped as the wrong type or too large (C-16), for
    /// [`METRIC_FIELD_DROPPED`].
    pub dropped: Vec<&'static str>,
}

/// Metric: a display field dropped at ingest, by `field` (C-16).
pub(crate) const METRIC_FIELD_DROPPED: &str = "private_ingest_field_dropped_total";
/// A display field larger than this, serialized, is dropped (§4.2).
pub(crate) const DISPLAY_FIELD_MAX_BYTES: usize = 64 * 1024;

/// The mention feature types `mention_dids` reads (§3.1).
const MENTION_TYPES: [&str; 3] = [
    "app.bsky.richtext.facet#mention",
    "tools.kryphocron.feed.postPublic#mention",
    "#mention",
];

impl DisplayFields {
    /// Read the display fields defensively. A field of the wrong type or over
    /// [`DISPLAY_FIELD_MAX_BYTES`] is dropped and named in `dropped`; it
    /// never makes the record malformed (C-16).
    pub(crate) fn parse(record: &Value) -> Self {
        let mut dropped = Vec::new();
        let mut keep = |key: &'static str, ok: fn(&Value) -> bool| -> Option<Value> {
            let v = record.get(key).filter(|v| !v.is_null())?;
            let fits = serde_json::to_vec(v).is_ok_and(|b| b.len() <= DISPLAY_FIELD_MAX_BYTES);
            if ok(v) && fits {
                Some(v.clone())
            } else {
                dropped.push(key);
                None
            }
        };
        let facets = keep("facets", Value::is_array);
        let embed = keep("embed", Value::is_object);
        let langs = keep("langs", |v| v.as_array().is_some_and(|a| a.iter().all(Value::is_string)))
            .map(|v| v.as_array().into_iter().flatten().filter_map(Value::as_str).map(str::to_string).collect());
        let self_labels = keep("labels", Value::is_object);
        let quote_uri = embed
            .as_ref()
            .map(crate::hydration::embeds::normalize_embed)
            .and_then(|e| crate::hydration::embeds::quoted_uri(&e).map(str::to_string))
            .filter(|u| u.starts_with("at://") && uri_author(u).is_some_and(proto_blue::syntax::Did::is_valid));
        let mut mention_dids: Vec<String> = Vec::new();
        for feature in facets
            .iter()
            .flat_map(|f| f.as_array().into_iter().flatten())
            .flat_map(|facet| facet.get("features").and_then(Value::as_array).into_iter().flatten())
        {
            if feature.get("$type").and_then(Value::as_str).is_some_and(|t| MENTION_TYPES.contains(&t))
                && let Some(did) = feature.get("did").and_then(Value::as_str)
                && proto_blue::syntax::Did::is_valid(did)
                && !mention_dids.iter().any(|d| d == did)
            {
                mention_dids.push(did.to_string());
            }
        }
        Self {
            facets,
            embed,
            langs,
            self_labels,
            quote_uri,
            mention_dids,
            dropped,
        }
    }
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
        let (reply_root, reply_parent) = match record.get("reply") {
            None | Some(Value::Null) => ((None, None), (None, None)),
            Some(r @ Value::Object(_)) => {
                let part = |k: &str| -> Result<(Option<String>, Option<String>), IngestError> {
                    let strong_ref = r.get(k).ok_or_else(|| malformed(format!("reply.{k} missing")))?;
                    let uri = strong_ref
                        .get("uri")
                        .ok_or_else(|| malformed(format!("reply.{k}.uri missing")))
                        .and_then(|u| at_uri(u, &format!("reply.{k}.uri")))?;
                    let cid = strong_ref_cid(strong_ref).map_err(|()| malformed(format!("reply.{k}.cid is not a cid")))?;
                    Ok((Some(uri), cid))
                };
                (part("root")?, part("parent")?)
            }
            Some(_) => return Err(malformed("reply is not an object")),
        };
        let ((reply_root_uri, reply_root_cid), (reply_parent_uri, reply_parent_cid)) = (reply_root, reply_parent);
        Ok(Self {
            text,
            encoded_content,
            encoded_codec,
            encoded_generation,
            audience_uri,
            reply_root_uri,
            reply_parent_uri,
            reply_root_cid,
            reply_parent_cid,
            created_at: created_at(record, now),
            display: DisplayFields::parse(record),
        })
    }
}

/// The stored fields of a `policy.audience` record.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct AudienceFields {
    pub mode: Option<String>,
    pub members: Vec<String>,
    /// The operator-facing label (P-3): a string, else `None`. A `name` of
    /// another type is dropped and counted, never malformed (C-16).
    pub name: Option<String>,
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
        let name = match record.get("name") {
            None | Some(Value::Null) => None,
            Some(Value::String(s)) if s.len() <= DISPLAY_FIELD_MAX_BYTES => Some(s.clone()),
            Some(_) => {
                metrics::counter!(METRIC_FIELD_DROPPED, "field" => "name").increment(1);
                None
            }
        };
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
        Ok(Self { mode, members, name })
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
         ON CONFLICT (did, depth) DO UPDATE SET state = 'queued', attempts = 0, generation = backfill_jobs.generation + 1, \
           not_before = now(), last_error = NULL, updated_at = now() \
         WHERE backfill_jobs.state NOT IN ('queued', 'running')",
    )
    .bind(did)
    .bind(Depth::Private.as_str())
    .execute(conn)
    .await?;
    Ok(())
}

/// The display refill (private-posting design §3.1, S2): once, at the first
/// start after migration 0023, a `private` job for every DID with a private
/// post or audience row stored before it (`display_filled` false), queued
/// as [`enqueue_private`] queues one. The job lists the DID's private
/// records to the end and writes them at the repo's latest rev, which fills
/// those rows; an equal or older rev never overwrites a filled row.
///
/// `marker` is the `worker_state` key, written in the same transaction, so
/// the refill is queued exactly once. Returns the jobs queued, or `None`
/// when the marker was already there.
///
/// # Errors
///
/// Database errors; nothing is recorded, and the next start tries again.
pub(crate) async fn enqueue_display_refill(pool: &PgPool, marker: &str) -> Result<Option<u64>, sqlx::Error> {
    let mut tx = pool.begin().await?;
    let first = sqlx::query(
        "INSERT INTO worker_state (key, value, updated_at) VALUES ($1, to_jsonb(now()::text), now()) \
         ON CONFLICT (key) DO NOTHING",
    )
    .bind(marker)
    .execute(&mut *tx)
    .await?
    .rows_affected()
        > 0;
    if !first {
        tx.rollback().await?;
        return Ok(None);
    }
    let queued = sqlx::query(
        "INSERT INTO backfill_jobs (did, depth, state, attempts, not_before, updated_at) \
         SELECT d.did, $1, 'queued', 0, now(), now() FROM ( \
           SELECT author_did AS did FROM private_posts WHERE NOT display_filled \
           UNION SELECT owner_did FROM private_audiences WHERE NOT display_filled) d \
         ON CONFLICT (did, depth) DO UPDATE SET state = 'queued', attempts = 0, generation = backfill_jobs.generation + 1, \
           not_before = now(), last_error = NULL, updated_at = now() \
         WHERE backfill_jobs.state NOT IN ('queued', 'running')",
    )
    .bind(Depth::Private.as_str())
    .execute(&mut *tx)
    .await?
    .rows_affected();
    tx.commit().await?;
    Ok(Some(queued))
}

/// The private-ingest insert, for tests that race it against a run.
#[cfg(test)]
pub(crate) async fn enqueue_private_for_tests(conn: &mut PgConnection, did: &str) -> Result<(), sqlx::Error> {
    enqueue_private(conn, did).await
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
    // An audience written with mode exactly `following` (S10 §3.6).
    let mut following = false;
    let changed = match op.nsid.as_str() {
        NSID_POST_PRIVATE => {
            let f = PostPrivateFields::parse(&op.record, now)?;
            for field in &f.display.dropped {
                metrics::counter!(METRIC_FIELD_DROPPED, "field" => *field).increment(1);
            }
            // A row stored before migration 0023 (`display_filled` false)
            // also takes an op at its own rev: the refill (§3.1).
            let changed = sqlx::query(
                "INSERT INTO private_posts (uri, author_did, rkey, cid, rev, audience_uri, reply_root_uri, \
                   reply_parent_uri, encoded_content, encoded_codec, encoded_generation, plaintext_text, created_at, \
                   reply_root_cid, reply_parent_cid, facets, embed, langs, self_labels, quote_uri, mention_dids, \
                   display_filled) \
                 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, true) \
                 ON CONFLICT (uri) DO UPDATE SET cid = EXCLUDED.cid, rev = EXCLUDED.rev, \
                   audience_uri = EXCLUDED.audience_uri, reply_root_uri = EXCLUDED.reply_root_uri, \
                   reply_parent_uri = EXCLUDED.reply_parent_uri, reply_root_cid = EXCLUDED.reply_root_cid, \
                   reply_parent_cid = EXCLUDED.reply_parent_cid, encoded_content = EXCLUDED.encoded_content, \
                   encoded_codec = EXCLUDED.encoded_codec, encoded_generation = EXCLUDED.encoded_generation, \
                   plaintext_text = EXCLUDED.plaintext_text, created_at = EXCLUDED.created_at, \
                   facets = EXCLUDED.facets, embed = EXCLUDED.embed, langs = EXCLUDED.langs, \
                   self_labels = EXCLUDED.self_labels, quote_uri = EXCLUDED.quote_uri, \
                   mention_dids = EXCLUDED.mention_dids, display_filled = true \
                 WHERE private_posts.rev < EXCLUDED.rev \
                    OR (NOT private_posts.display_filled AND private_posts.rev <= EXCLUDED.rev)",
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
            .bind(&f.reply_root_cid)
            .bind(&f.reply_parent_cid)
            .bind(&f.display.facets)
            .bind(&f.display.embed)
            .bind(&f.display.langs)
            .bind(&f.display.self_labels)
            .bind(&f.display.quote_uri)
            .bind(&f.display.mention_dids)
            .execute(&mut *conn)
            .await?
            .rows_affected()
                > 0;
            // Its notifications, when the row is written: an update that
            // adds recipients adds rows, and the others are kept (§4.8).
            if changed && super::notifies(op.source, op.created_at, op.notify_max_age) {
                notify_post(&mut *conn, op, &f).await?;
            }
            changed
        }
        NSID_AUDIENCE => {
            let f = AudienceFields::parse(&op.record)?;
            following = f.mode.as_deref() == Some("following");
            sqlx::query(
                "INSERT INTO private_audiences (uri, owner_did, rkey, cid, rev, mode, members, name, display_filled) \
                 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, true) \
                 ON CONFLICT (uri) DO UPDATE SET cid = EXCLUDED.cid, rev = EXCLUDED.rev, \
                   mode = EXCLUDED.mode, members = EXCLUDED.members, name = EXCLUDED.name, display_filled = true \
                 WHERE private_audiences.rev < EXCLUDED.rev \
                    OR (NOT private_audiences.display_filled AND private_audiences.rev <= EXCLUDED.rev)",
            )
            .bind(&op.uri)
            .bind(author)
            .bind(&op.rkey)
            .bind(&op.cid)
            .bind(&op.rev)
            .bind(&f.mode)
            .bind(&f.members)
            .bind(&f.name)
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
    // A `following` audience written for an existing owner (following-
    // audience design §3.6): an `authz` run verifies its follows, queued
    // as `enqueue_private` queues a job, with no reset. It tests no flag, so
    // a settle writing NULL beside it cannot leave the owner unqueued. A
    // job's own write is skipped: its pass decision follows its
    // authorization listings, and its settle re-checks. A DID that becomes
    // an owner with this record gets its `private` job above.
    if changed && following && had_rows && op.source != WriteSource::Backfill && owners::is_owner(&mut *conn, author).await? {
        enqueue_authz(&mut *conn, author).await?;
    }
    Ok(applied)
}

/// Enqueue an `authz` job unless one is already queued or running, as
/// [`enqueue_private`] does for `private`.
async fn enqueue_authz(conn: &mut PgConnection, did: &str) -> Result<(), sqlx::Error> {
    jobs::enqueue_if_idle(conn, did, Depth::Authz, Utc::now()).await
}

/// A text field of a gate record that must be an at-uri.
fn gate_post(record: &Value) -> Result<String, IngestError> {
    at_uri(record.get("post").unwrap_or(&Value::Null), "post")
}

/// The strings of an array field; anything else is empty, as the public
/// gate rows read it (`firehose::rows`).
fn gate_strings(record: &Value, key: &str) -> Vec<String> {
    record
        .get(key)
        .and_then(Value::as_array)
        .map(|a| a.iter().filter_map(Value::as_str).map(str::to_string).collect())
        .unwrap_or_default()
}

/// `ingest::apply_gate` (private posting §4.4): LWW create/update of an
/// admitted `app.bsky.feed.threadgate` or `postgate` whose `post` is a
/// `postPrivate` URI, into `private_threadgates` or `private_postgates`,
/// with `private_tombstones` checked. A gate whose rkey or author is not its
/// post's is stored as received and ignored on read.
///
/// # Errors
///
/// [`IngestError::Malformed`] for a gate with no at-uri `post`, or of another
/// collection; database errors.
pub(crate) async fn apply_gate(conn: &mut PgConnection, op: &PrivateOp) -> Result<Applied, IngestError> {
    if tombstoned(&mut *conn, &op.uri, &op.rev).await? {
        return Ok(Applied::default());
    }
    let post = gate_post(&op.record)?;
    let at = created_at(&op.record, Utc::now());
    let field = |key: &str| op.record.get(key).filter(|v| !v.is_null()).cloned();
    let changed = match op.nsid.as_str() {
        NSID_THREADGATE => sqlx::query(
            "INSERT INTO private_threadgates (uri, author_did, rkey, cid, rev, post_uri, allow, hidden_replies, record, created_at) \
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10) \
             ON CONFLICT (uri) DO UPDATE SET cid = EXCLUDED.cid, rev = EXCLUDED.rev, post_uri = EXCLUDED.post_uri, \
               allow = EXCLUDED.allow, hidden_replies = EXCLUDED.hidden_replies, record = EXCLUDED.record, \
               created_at = EXCLUDED.created_at, indexed_at = now() \
             WHERE private_threadgates.rev < EXCLUDED.rev",
        )
        .bind(&op.uri)
        .bind(&op.author_did)
        .bind(&op.rkey)
        .bind(&op.cid)
        .bind(&op.rev)
        .bind(&post)
        .bind(field("allow"))
        .bind(gate_strings(&op.record, "hiddenReplies"))
        .bind(&op.record)
        .bind(at),
        NSID_POSTGATE => sqlx::query(
            "INSERT INTO private_postgates (uri, author_did, rkey, cid, rev, post_uri, embedding_rules, \
               detached_embedding_uris, record, created_at) \
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10) \
             ON CONFLICT (uri) DO UPDATE SET cid = EXCLUDED.cid, rev = EXCLUDED.rev, post_uri = EXCLUDED.post_uri, \
               embedding_rules = EXCLUDED.embedding_rules, detached_embedding_uris = EXCLUDED.detached_embedding_uris, \
               record = EXCLUDED.record, created_at = EXCLUDED.created_at, indexed_at = now() \
             WHERE private_postgates.rev < EXCLUDED.rev",
        )
        .bind(&op.uri)
        .bind(&op.author_did)
        .bind(&op.rkey)
        .bind(&op.cid)
        .bind(&op.rev)
        .bind(&post)
        .bind(field("embeddingRules"))
        .bind(gate_strings(&op.record, "detachedEmbeddingUris"))
        .bind(&op.record)
        .bind(at),
        other => return Err(malformed(format!("not a gate NSID: {other}"))),
    }
    .execute(&mut *conn)
    .await?
    .rows_affected()
        > 0;
    Ok(Applied {
        changed,
        ..Applied::default()
    })
}

/// `ingest::delete_gate`: delete the private copy of a threadgate or
/// postgate at `uri` with `rev <= R`. Tombstone iff a row was removed or the
/// author is in S (§5.5), as [`delete`] does.
///
/// # Errors
///
/// Database errors.
pub(crate) async fn delete_gate(conn: &mut PgConnection, uri: &str, rev: &str, author_in_scope: bool) -> Result<Applied, sqlx::Error> {
    let collection = uri.strip_prefix("at://").and_then(|r| r.split('/').nth(1));
    let table = match collection {
        Some(NSID_THREADGATE) => "private_threadgates",
        Some(NSID_POSTGATE) => "private_postgates",
        _ => return Ok(Applied::default()),
    };
    let removed = sqlx::query(&format!("DELETE FROM {table} WHERE uri = $1 AND rev <= $2"))
        .bind(uri)
        .bind(rev)
        .execute(&mut *conn)
        .await?
        .rows_affected()
        > 0;
    if removed || author_in_scope {
        tombstone(&mut *conn, uri, rev).await?;
    }
    Ok(Applied {
        changed: removed,
        ..Applied::default()
    })
}

/// The record keys of `did`'s stored private posts (backfill keeps their
/// gates, private posting §4.4).
///
/// # Errors
///
/// Database errors.
pub(crate) async fn post_rkeys(pool: &PgPool, did: &str) -> Result<std::collections::HashSet<String>, sqlx::Error> {
    let rkeys: Vec<String> = sqlx::query_scalar("SELECT rkey FROM private_posts WHERE author_did = $1")
        .bind(did)
        .fetch_all(pool)
        .await?;
    Ok(rkeys.into_iter().collect())
}

/// `ingest::upsert_quote_ref` (private posting §4.7): a member's public post
/// that quotes a private post, into `private_quote_refs`, last writer winning
/// on rev. A post that quotes no private post (or no longer does) removes
/// an older reference instead.
///
/// # Errors
///
/// Database errors.
pub(crate) async fn upsert_quote_ref(conn: &mut PgConnection, quote: &QuoteRef) -> Result<(), sqlx::Error> {
    let Some(quoted) = &quote.quoted_uri else {
        sqlx::query("DELETE FROM private_quote_refs WHERE uri = $1 AND rev < $2")
            .bind(&quote.uri)
            .bind(&quote.rev)
            .execute(conn)
            .await?;
        return Ok(());
    };
    sqlx::query(
        "INSERT INTO private_quote_refs (uri, author_did, cid, rev, quoted_uri, created_at) \
         VALUES ($1, $2, $3, $4, $5, $6) \
         ON CONFLICT (uri) DO UPDATE SET author_did = EXCLUDED.author_did, cid = EXCLUDED.cid, rev = EXCLUDED.rev, \
           quoted_uri = EXCLUDED.quoted_uri, created_at = EXCLUDED.created_at \
         WHERE private_quote_refs.rev < EXCLUDED.rev",
    )
    .bind(&quote.uri)
    .bind(&quote.author_did)
    .bind(&quote.cid)
    .bind(&quote.rev)
    .bind(quoted)
    .bind(quote.created_at)
    .execute(conn)
    .await?;
    Ok(())
}

/// `ingest::delete_quote_ref`: the quote reference of a deleted public post,
/// when its rev is not newer than the delete's.
///
/// # Errors
///
/// Database errors.
pub(crate) async fn delete_quote_ref(conn: &mut PgConnection, uri: &str, rev: &str) -> Result<(), sqlx::Error> {
    sqlx::query("DELETE FROM private_quote_refs WHERE uri = $1 AND rev <= $2")
        .bind(uri)
        .bind(rev)
        .execute(conn)
        .await?;
    Ok(())
}

/// Whether `uri` names a post, public or private: a reply's or quote's
/// subject must, so that each recipient's subject is its own post (§4.8).
fn is_post(uri: &str) -> bool {
    matches!(uri.strip_prefix("at://").and_then(|r| r.split('/').nth(1)), Some(NSID_POST | NSID_POST_PRIVATE))
}

/// One private notification to write: `(recipient, reason, reason_subject)`.
type Recipient = (String, &'static str, Option<String>);

/// The recipients of a private post by `author` (§4.8), one reason each, in
/// precedence order: `reply` to the parent's author (subject: the parent),
/// `reply` to the claimed root's author (subject: the root), `quote` to the
/// quoted post's author (subject: the quoted post), `mention` to each
/// mentioned DID (no subject). The author is never one; membership and
/// thread mutes are tested on insert.
fn post_recipients(author: &str, f: &PostPrivateFields) -> Vec<Recipient> {
    let mut out: Vec<Recipient> = Vec::new();
    let mut push = |recipient: Option<&str>, reason: &'static str, subject: Option<&String>| {
        if let Some(r) = recipient
            && r != author
            && !out.iter().any(|(d, ..)| d == r)
        {
            out.push((r.to_string(), reason, subject.cloned()));
        }
    };
    for (reason, uri) in [
        ("reply", f.reply_parent_uri.as_ref()),
        ("reply", f.reply_root_uri.as_ref()),
        ("quote", f.display.quote_uri.as_ref()),
    ] {
        if let Some(uri) = uri.filter(|u| is_post(u)) {
            push(uri_author(uri), reason, Some(uri));
        }
    }
    for did in &f.display.mention_dids {
        push(Some(did), "mention", None);
    }
    out
}

/// Insert one private notification for recipient `$1` unless the recipient
/// is the author, is not a member, or muted the thread: the record (`$5`),
/// the root it claims (`$7`), the subject (`$4`) or the subject's own
/// claimed root is a muted root (`insert_notification`'s rule, §4.8). A
/// muted thread writes nothing, so unmuting brings nothing back.
const INSERT_NOTIFICATION: &str = "INSERT INTO private_notifications \
       (recipient_did, author_did, reason, reason_subject, record_uri, record_cid, record_root, public_record) \
     SELECT $1, $2, $3, $4, $5, $6, $7, $8 \
     WHERE $1 <> $2 \
       AND EXISTS (SELECT 1 FROM members m WHERE m.did = $1) \
       AND NOT EXISTS (SELECT 1 FROM thread_mutes t WHERE t.viewer_did = $1 AND t.root_uri IN ( \
             $5::text, $7::text, $4::text, \
             (SELECT p.reply_root_uri FROM private_posts p WHERE p.uri = $4::text), \
             (SELECT p.reply_root_uri FROM posts p WHERE p.uri = $4::text))) \
     ON CONFLICT DO NOTHING";

#[allow(clippy::too_many_arguments)]
async fn insert_notification(
    conn: &mut PgConnection,
    recipient: &Recipient,
    author: &str,
    record_uri: &str,
    record_cid: &str,
    record_root: Option<&str>,
    public_record: Option<&Value>,
) -> Result<(), sqlx::Error> {
    let (did, reason, subject) = recipient;
    sqlx::query(INSERT_NOTIFICATION)
        .bind(did)
        .bind(author)
        .bind(*reason)
        .bind(subject.as_deref())
        .bind(record_uri)
        .bind(record_cid)
        .bind(record_root)
        .bind(public_record)
        .execute(conn)
        .await?;
    Ok(())
}

/// The private notifications of a written private post (§4.8): see
/// [`post_recipients`]. The thread root is the reply's claimed one (C-18).
async fn notify_post(conn: &mut PgConnection, op: &PrivateOp, f: &PostPrivateFields) -> Result<(), sqlx::Error> {
    for r in post_recipients(&op.author_did, f) {
        insert_notification(&mut *conn, &r, &op.author_did, &op.uri, &op.cid, f.reply_root_uri.as_deref(), None).await?;
    }
    Ok(())
}

/// `ingest::notify_public` (§4.8): the private notifications of a public
/// record naming a private post, written in every public-indexing mode when
/// its author is a member and the write notifies ([`super::notifies`]).
/// Each recipient is the subject's author, a member other than the author,
/// one reason each in the given precedence; the subject is the recipient's
/// own private post.
///
/// # Errors
///
/// Database errors.
pub(crate) async fn notify_public(conn: &mut PgConnection, n: &PublicNotify) -> Result<(), sqlx::Error> {
    if !super::notifies(n.source, n.created_at, n.notify_max_age) {
        return Ok(());
    }
    let member: bool = sqlx::query_scalar("SELECT EXISTS (SELECT 1 FROM members WHERE did = $1)")
        .bind(&n.author_did)
        .fetch_one(&mut *conn)
        .await?;
    if !member {
        return Ok(());
    }
    let mut done: Vec<&str> = Vec::new();
    for (reason, subject) in &n.reasons {
        let Some(recipient) = uri_author(subject) else {
            continue;
        };
        if done.contains(&recipient) {
            continue;
        }
        done.push(recipient);
        let r: Recipient = (recipient.to_string(), *reason, Some(subject.clone()));
        insert_notification(
            &mut *conn,
            &r,
            &n.author_did,
            &n.record_uri,
            &n.record_cid,
            n.record_root.as_deref(),
            n.public_record.as_ref(),
        )
        .await?;
    }
    Ok(())
}

/// `ingest::delete_notifications`: the private notifications whose record is
/// `record_uri` (a deleted record, §4.8).
///
/// # Errors
///
/// Database errors.
pub(crate) async fn delete_notifications(conn: &mut PgConnection, record_uri: &str) -> Result<(), sqlx::Error> {
    sqlx::query("DELETE FROM private_notifications WHERE record_uri = $1")
        .bind(record_uri)
        .execute(conn)
        .await?;
    Ok(())
}

async fn tombstone(conn: &mut PgConnection, uri: &str, rev: &str) -> Result<(), sqlx::Error> {
    sqlx::query(
        "INSERT INTO private_tombstones (uri, rev) VALUES ($1, $2) \
         ON CONFLICT (uri) DO UPDATE SET rev = GREATEST(private_tombstones.rev, EXCLUDED.rev), deleted_at = now()",
    )
    .bind(uri)
    .bind(rev)
    .execute(conn)
    .await?;
    Ok(())
}

/// The canonical private gates of a `postPrivate` post: the threadgate and
/// postgate in its author's repo with its rkey. Deleted with the post (the
/// public rule, `writer.rs`'s post delete).
fn canonical_private_gates(post_uri: &str) -> Option<[(&'static str, String); 2]> {
    let (prefix, rkey) = post_uri.rsplit_once('/')?;
    let repo = prefix.strip_suffix(NSID_POST_PRIVATE)?.strip_suffix('/')?;
    Some([
        ("private_threadgates", format!("{repo}/{NSID_THREADGATE}/{rkey}")),
        ("private_postgates", format!("{repo}/{NSID_POSTGATE}/{rkey}")),
    ])
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
        tombstone(&mut *conn, uri, rev).await?;
    }
    // A deleted private post's notifications go with it, as a public
    // record's do (private posting §4.8).
    if table == "private_posts" {
        delete_notifications(&mut *conn, uri).await?;
    }
    // The gates of a private post that is gone gate nothing (§3.1).
    if removed && let Some(gates) = canonical_private_gates(uri) {
        for (gate_table, gate) in gates {
            sqlx::query(&format!("DELETE FROM {gate_table} WHERE uri = $1"))
                .bind(gate)
                .execute(&mut *conn)
                .await?;
        }
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
    let gate = match nsid {
        NSID_THREADGATE => Some(("private_threadgates", "author_did")),
        NSID_POSTGATE => Some(("private_postgates", "author_did")),
        _ => None,
    };
    let Some((table, col)) = gate.or_else(|| table_of(&format!("at://{did}/{nsid}/x"))) else {
        return Ok(Vec::new());
    };
    sqlx::query_scalar(&format!("SELECT uri FROM {table} WHERE {col} = $1 AND rev < $2"))
        .bind(did)
        .bind(below_rev)
        .fetch_all(conn)
        .await
}

/// `ingest::stored_cids`: URI → CID of `did`'s stored rows in `nsid`, for the
/// collections whose rows keep a CID (private posts and audiences). Catch-up
/// skips a listed record whose CID is stored (private posting §3.8).
///
/// # Errors
///
/// Database errors.
pub(crate) async fn stored_cids(conn: &mut PgConnection, did: &str, nsid: &str) -> Result<HashMap<String, String>, sqlx::Error> {
    let (table, col) = match nsid {
        NSID_POST_PRIVATE => ("private_posts", "author_did"),
        NSID_AUDIENCE => ("private_audiences", "owner_did"),
        _ => return Ok(HashMap::new()),
    };
    let rows: Vec<(String, Option<String>)> = sqlx::query_as(&format!("SELECT uri, cid FROM {table} WHERE {col} = $1"))
        .bind(did)
        .fetch_all(conn)
        .await?;
    Ok(rows.into_iter().filter_map(|(uri, cid)| cid.map(|c| (uri, c))).collect())
}

/// The viewer's own private bookmarks, at most `limit` rows (private posting
/// §3.1: deleted by the `account_deleted` and `member_removed` purges, as
/// `bookmarks` is).
async fn purge_bookmarks(conn: &mut PgConnection, did: &str, limit: i64) -> Result<u64, sqlx::Error> {
    Ok(sqlx::query(
        "DELETE FROM private_bookmarks WHERE ctid IN (SELECT ctid FROM private_bookmarks WHERE viewer_did = $1 LIMIT $2)",
    )
    .bind(did)
    .bind(limit)
    .execute(conn)
    .await?
    .rows_affected())
}

/// The private notifications `did` received and those it authored, and its
/// last-seen row, at most `limit` rows (private posting §3.1: the
/// `account_deleted` and `member_removed` purges). An authored row may hold
/// `public_record`, a copy of the DID's public post.
async fn purge_notifications(conn: &mut PgConnection, did: &str, limit: i64) -> Result<u64, sqlx::Error> {
    let mut n = sqlx::query(
        "DELETE FROM private_notifications WHERE id IN (SELECT id FROM private_notifications \
         WHERE recipient_did = $1 OR author_did = $1 LIMIT $2)",
    )
    .bind(did)
    .bind(limit)
    .execute(&mut *conn)
    .await?
    .rows_affected();
    n += sqlx::query("DELETE FROM private_notification_seen WHERE did = $1")
        .bind(did)
        .execute(&mut *conn)
        .await?
        .rows_affected();
    Ok(n)
}

/// `ingest::purge_did` (account deletion): every private row authored by the
/// DID, its owner state, its private bookmarks and its private
/// notifications, at most `limit` rows per table.
///
/// # Errors
///
/// Database errors.
pub(crate) async fn purge_did(conn: &mut PgConnection, did: &str, limit: i64) -> Result<u64, sqlx::Error> {
    let mut n = purge_bookmarks(conn, did, limit).await?;
    n += purge_notifications(conn, did, limit).await?;
    for (table, col) in [
        ("private_posts", "author_did"),
        ("private_audiences", "owner_did"),
        ("private_blocks", "author_did"),
        ("private_threadgates", "author_did"),
        ("private_postgates", "author_did"),
        ("private_quote_refs", "author_did"),
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
    // The DID's gates go with its private posts (private posting §3.1): any
    // whose post is no longer stored, the kept class-M replies' gates aside.
    for table in ["private_threadgates", "private_postgates"] {
        n += sqlx::query(&format!(
            "DELETE FROM {table} WHERE uri IN (SELECT g.uri FROM {table} g WHERE g.author_did = $1 \
               AND NOT EXISTS (SELECT 1 FROM private_posts p WHERE p.uri = g.post_uri) LIMIT $2)"
        ))
        .bind(did)
        .bind(limit)
        .execute(&mut *conn)
        .await?
        .rows_affected();
    }
    Ok(n)
}

/// The private part of a `member_removed` purge (§6.11.7): private replies by
/// authors outside S admitted only because `did` was a member (no other
/// member among parent and root authors), rule-2 private blocks of `did`
/// by authors outside S that are not private owners, the member's private
/// bookmarks, the member's quote references (private posting §3.1: its
/// later deletes are no longer tracked), and the private notifications it
/// received and authored, for the same reason.
///
/// # Errors
///
/// Database errors.
pub(crate) async fn purge_member_removed(conn: &mut PgConnection, did: &str, limit: i64) -> Result<u64, sqlx::Error> {
    let mut n = purge_bookmarks(conn, did, limit).await?;
    n += purge_notifications(conn, did, limit).await?;
    n += sqlx::query(
        "DELETE FROM private_quote_refs WHERE uri IN (SELECT uri FROM private_quote_refs WHERE author_did = $1 LIMIT $2)",
    )
    .bind(did)
    .bind(limit)
    .execute(&mut *conn)
    .await?
    .rows_affected();
    n += sqlx::query(
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
        let root = "at://did:plc:aaaaaaaaaaaaaaaaaaaaaaaa/app.bsky.feed.post/1";
        let reply = json!({"text": "r", "reply": {"root": {"uri": root, "cid": "bafyroot"}, "parent": {"uri": root, "cid": {"$link": "bafyparent"}}}});
        let f = PostPrivateFields::parse(&reply, now()).expect("reply");
        assert_eq!((f.reply_root_cid.as_deref(), f.reply_parent_cid.as_deref()), (Some("bafyroot"), Some("bafyparent")));
        let no_cids = json!({"reply": {"root": {"uri": root}, "parent": {"uri": root}}});
        let f = PostPrivateFields::parse(&no_cids, now()).expect("cid-less reply");
        assert_eq!((f.reply_parent_uri.as_deref(), f.reply_parent_cid), (Some(root), None));
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
            json!({"reply": {"root": {"uri": "at://did:plc:aaaaaaaaaaaaaaaaaaaaaaaa/app.bsky.feed.post/1", "cid": 5},
                             "parent": {"uri": "at://did:plc:aaaaaaaaaaaaaaaaaaaaaaaa/app.bsky.feed.post/1"}}}),
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

    /// Private posting §3.1, §4.2, C-16: display fields are kept as received
    /// when well typed, dropped (and named) otherwise, and `quote_uri` and
    /// `mention_dids` are derived from either `$type` form.
    #[test]
    fn display_fields_are_read_defensively() {
        let quoted = "at://did:plc:bbbbbbbbbbbbbbbbbbbbbbbb/tools.kryphocron.feed.postPrivate/3kq";
        for (embed, expected) in [
            (json!({"$type": "tools.kryphocron.feed.postPublic#embedRecord", "record": {"uri": quoted, "cid": "c"}}), Some(quoted)),
            (json!({"$type": "app.bsky.embed.record", "record": {"uri": quoted, "cid": "c"}}), Some(quoted)),
            (json!({"$type": "app.bsky.embed.recordWithMedia", "record": {"record": {"uri": quoted}}, "media": {}}), Some(quoted)),
            (json!({"$type": "app.bsky.embed.record", "record": {"uri": "https://not.an.at-uri"}}), None),
            (json!({"$type": "app.bsky.embed.images", "images": []}), None),
        ] {
            let d = DisplayFields::parse(&json!({"embed": embed}));
            assert_eq!(d.quote_uri.as_deref(), expected, "{embed}");
            assert!(d.dropped.is_empty());
        }
        let mention = |t: &str, did: &str| json!({"$type": t, "did": did});
        let d = DisplayFields::parse(&json!({
            "facets": [{"features": [mention("tools.kryphocron.feed.postPublic#mention", "did:plc:cccccccccccccccccccccccc"),
                                     mention("app.bsky.richtext.facet#mention", "did:plc:dddddddddddddddddddddddd"),
                                     mention("app.bsky.richtext.facet#mention", "did:plc:cccccccccccccccccccccccc"),
                                     mention("app.bsky.richtext.facet#mention", "not a did"),
                                     mention("app.bsky.richtext.facet#link", "did:plc:eeeeeeeeeeeeeeeeeeeeeeee")]}],
            "langs": ["en", "fr"],
            "labels": {"values": [{"val": "porn"}]},
        }));
        assert_eq!(d.mention_dids, ["did:plc:cccccccccccccccccccccccc", "did:plc:dddddddddddddddddddddddd"]);
        assert_eq!(d.langs, Some(vec!["en".to_string(), "fr".to_string()]));
        assert!(d.self_labels.is_some() && d.dropped.is_empty());
        // Wrong types: dropped, never malformed.
        let bad = json!({"text": "t", "facets": "x", "embed": [1], "langs": ["en", 2], "labels": "porn"});
        assert_eq!(DisplayFields::parse(&bad).dropped, ["facets", "embed", "langs", "labels"]);
        let parsed = PostPrivateFields::parse(&bad, now()).expect("still readable");
        assert_eq!(parsed.display, DisplayFields { dropped: vec!["facets", "embed", "langs", "labels"], ..DisplayFields::default() });
        // Over 64 KiB serialized: dropped. Null: absent, not dropped.
        let big = json!({"embed": {"$type": "app.bsky.embed.external", "external": {"uri": "x", "title": "t", "description": "d".repeat(70_000)}}, "langs": null});
        let d = DisplayFields::parse(&big);
        assert_eq!((d.embed, d.dropped), (None, vec!["embed"]));
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
