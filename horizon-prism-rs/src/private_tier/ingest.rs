//! Private-tier ingest (design-v2 §3.2, §4.5, §5.5, §6.11.1): LWW writes of
//! admitted `postPrivate` records and private gates, with
//! `private_tombstones`, owner re-evaluation, and the purge and reconcile
//! helpers. Audiences are not ingested: this AppView holds them
//! (`audiences`, private-audiences design).
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

use super::media::{self, MEDIA_KINDS_FIELD};
use super::{Applied, IngestError, PrivateOp, PublicNotify, QuoteRef, clear_fields, owners};
use crate::db::locks::uri_author;
use crate::firehose::classifier::{NSID_POST, NSID_POST_PRIVATE, NSID_POSTGATE, NSID_THREADGATE};
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
    /// The clear `app.nearhorizon.plural.fronters` object, when valid
    /// (plural-pronouns design §1.1): kept as received.
    pub fronters: Option<Value>,
    /// The quoted URI: from the clear `quote` when the record has one,
    /// else from `embed` in either `$type` form (private-embeds design
    /// §4.2).
    pub quote_uri: Option<String>,
    /// The mentioned DIDs: from the clear `mentionDids` when the record has
    /// it, else from the `#mention` facet features, in either form.
    pub mention_dids: Vec<String>,
    /// Where `quote_uri` and `mention_dids` came from, for
    /// [`METRIC_INDEX_SOURCE`].
    pub quote_source: IndexSource,
    pub mentions_source: IndexSource,
    /// The media bits (`media::IMAGE`, `media::VIDEO`): from the clear
    /// `mediaKinds` when the record has it, else from the clear `embed`
    /// (private-media-filters design §3.2).
    pub media_kinds: u8,
    pub media_source: IndexSource,
    /// Fields dropped as the wrong type or too large (C-16), for
    /// [`METRIC_FIELD_DROPPED`].
    pub dropped: Vec<&'static str>,
}

/// Where a `postPrivate` record's quote or mentions were indexed from
/// (private-embeds design §4.2, §9).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) enum IndexSource {
    /// Neither the dedicated field nor a kept clear `embed` / `facets`.
    #[default]
    None,
    /// The clear `embed` or `facets`, as before #504.
    Clear,
    /// The dedicated clear `quote` or `mentionDids` (present, even when
    /// dropped or empty of valid values).
    Dedicated,
}

impl IndexSource {
    fn label(self) -> &'static str {
        match self {
            Self::None => "none",
            Self::Clear => "clear",
            Self::Dedicated => "dedicated",
        }
    }
}

/// Metric: a display field dropped at ingest, by `field` (C-16).
pub(crate) const METRIC_FIELD_DROPPED: &str = "private_ingest_field_dropped_total";
/// Metric: per `postPrivate` op and `field` (`quote`, `mentions`, `media`),
/// the `source` it was indexed from (private-embeds design §9,
/// private-media-filters design §8).
pub(crate) const METRIC_INDEX_SOURCE: &str = "private_ingest_index_source_total";
/// Metric: notifications a private post's write did not send, by `reason`:
/// `old_update` (a live update of a post older than the notify age),
/// `known_recipient` (named by the post's previous version).
pub(crate) const METRIC_NOTIFY_SUPPRESSED: &str = "private_notify_suppressed_total";
/// A display field larger than this, serialized, is dropped (§4.2).
pub(crate) const DISPLAY_FIELD_MAX_BYTES: usize = 64 * 1024;

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
        let fronters = keep(crate::plural::FRONTERS_KEY, valid_fronters);
        // The dedicated clear fields of #504's new form (private-embeds
        // design §4.2). Present (not null), even when dropped here, they
        // decide: a bad one never falls back to `embed` or `facets`.
        let has_quote = record.get("quote").is_some_and(|v| !v.is_null());
        let has_mentions = record.get("mentionDids").is_some_and(|v| !v.is_null());
        let quote = keep("quote", Value::is_object);
        let mentions = keep("mentionDids", Value::is_array);
        // A dedicated field absent: the fact comes from the kept clear
        // `facets` and `embed` through the one derivation the encoder and
        // the read-time check use (private-encode design §5.2).
        let derived = clear_fields::index(&clear_fields::derive(facets.as_ref(), embed.as_ref()));
        let (quote_uri, quote_source) = if has_quote {
            let uri = quote
                .as_ref()
                .and_then(|q| q.get("uri"))
                .and_then(Value::as_str)
                .filter(|u| clear_fields::indexable_quote(u));
            (uri.map(str::to_string), IndexSource::Dedicated)
        } else {
            (derived.quote_uri.clone(), if embed.is_some() { IndexSource::Clear } else { IndexSource::None })
        };
        let mention_dids = if has_mentions {
            let wire = clear_fields::ClearFields {
                mention_dids: mentions.iter().flat_map(|m| m.as_array().into_iter().flatten()).cloned().collect(),
                ..clear_fields::ClearFields::default()
            };
            clear_fields::index(&wire).mention_dids
        } else {
            derived.mention_dids.clone()
        };
        let mentions_source = match (has_mentions, &facets) {
            (true, _) => IndexSource::Dedicated,
            (false, Some(_)) => IndexSource::Clear,
            (false, None) => IndexSource::None,
        };
        // The media marker (private-media-filters design §3.2, §3.3): as
        // `quote`, present (not null) decides, even when dropped or empty.
        let has_media = record.get(MEDIA_KINDS_FIELD).is_some_and(|v| !v.is_null());
        let media_field = keep(MEDIA_KINDS_FIELD, Value::is_array);
        let (media_kinds, media_source) = if has_media {
            let kinds = media_field.as_ref().and_then(Value::as_array).map_or(0, |a| media::kinds_of_field(a));
            (kinds, IndexSource::Dedicated)
        } else if embed.is_some() {
            (derived.media_kinds, IndexSource::Clear)
        } else {
            (0, IndexSource::None)
        };
        Self {
            facets,
            embed,
            langs,
            self_labels,
            fronters,
            quote_uri,
            mention_dids,
            quote_source,
            mentions_source,
            media_kinds,
            media_source,
            dropped,
        }
    }
}

/// A valid fronters value (plural-pronouns design §1.1, strict): an object
/// whose `members` is an array of 1 to 20 objects, each with `id` a valid
/// record key and `status`, when present, a string of at most 320 bytes.
/// Other keys are kept as received (the object is open).
pub(crate) fn valid_fronters(v: &Value) -> bool {
    let Some(members) = v.as_object().and_then(|o| o.get("members")).and_then(Value::as_array) else {
        return false;
    };
    (1..=20).contains(&members.len())
        && members.iter().all(|m| {
            let Some(m) = m.as_object() else { return false };
            let id = m.get("id").and_then(Value::as_str).is_some_and(proto_blue::syntax::RecordKey::is_valid);
            let status = match m.get("status") {
                None => true,
                Some(Value::String(s)) => s.len() <= 320,
                Some(_) => false,
            };
            id && status
        })
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

async fn tombstoned(conn: &mut PgConnection, uri: &str, rev: &str) -> Result<bool, sqlx::Error> {
    sqlx::query_scalar("SELECT EXISTS (SELECT 1 FROM private_tombstones WHERE uri = $1 AND rev >= $2)")
        .bind(uri)
        .bind(rev)
        .fetch_one(conn)
        .await
}

/// Metric: grant-collection upserts refused below their author's listed rev
/// (replay-readiness design §4.3), by `collection`.
pub(crate) const METRIC_BELOW_LISTED: &str = "private_ingest_below_listed_total";

/// Whether an upsert of the grant collection (private posts) is history:
/// tombstoned at or above `rev`, or below its author's listed rev
/// (replay-readiness design §4.3). The listing at that rev already holds
/// the op's effect or its undoing, so the op is never news, from any
/// source. Strictly below: commit L's own op still writes. One statement,
/// on the writer's transaction after its shared DID lock: every raise that
/// committed before the lock is seen, and none commits before the
/// transaction ends.
async fn superseded(conn: &mut PgConnection, op: &PrivateOp) -> Result<bool, sqlx::Error> {
    let (tombstoned, below): (bool, bool) = sqlx::query_as(
        "SELECT EXISTS (SELECT 1 FROM private_tombstones WHERE uri = $1 AND rev >= $2), \
                EXISTS (SELECT 1 FROM sync_state WHERE did = $3 AND post_listed_rev > $2)",
    )
    .bind(&op.uri)
    .bind(&op.rev)
    .bind(&op.author_did)
    .fetch_one(&mut *conn)
    .await?;
    if tombstoned {
        return Ok(true);
    }
    #[cfg(test)]
    if below && crate::test_support::hooks::injected_on(crate::test_support::hooks::LISTED_REV_REFUSAL_OFF, conn, &op.author_did).await {
        return Ok(false);
    }
    if below {
        metrics::counter!(METRIC_BELOW_LISTED, "collection" => op.nsid.clone()).increment(1);
    }
    Ok(below)
}

/// Whether `did` has a private post or audience row (one half of the owner
/// condition).
pub(super) async fn has_owner_rows(conn: &mut PgConnection, did: &str) -> Result<bool, sqlx::Error> {
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
pub(super) async fn enqueue_private(conn: &mut PgConnection, did: &str) -> Result<(), sqlx::Error> {
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
/// post stored before it (`display_filled` false), queued
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
         SELECT DISTINCT d.did, $1, 'queued', 0, now(), now() FROM ( \
           SELECT author_did AS did FROM private_posts WHERE NOT display_filled) d \
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
/// A `postPrivate` row for a member with no prior post or audience
/// re-evaluates owner status (§6.11.1): the member becomes an owner and gets
/// a `private` job. A non-member's row is stored and nothing more
/// (membership design §2.3). Gates never make their author an owner. A
/// post takes the owner lock before it reads the owner rows
/// (private-audiences design §11).
///
/// # Errors
///
/// [`IngestError`].
pub(crate) async fn apply(conn: &mut PgConnection, op: &PrivateOp) -> Result<Applied, IngestError> {
    let now = Utc::now();
    let owner_record = op.nsid == NSID_POST_PRIVATE;
    // A grant collection's op below the listed rev is refused as a
    // tombstoned one is (replay-readiness design §4.3); any other op only by
    // its tombstone.
    let refused = if owner_record { superseded(&mut *conn, op).await? } else { tombstoned(&mut *conn, &op.uri, &op.rev).await? };
    if refused {
        return Ok(Applied::default());
    }
    let author = op.author_did.as_str();
    if owner_record {
        owners::lock(&mut *conn, author).await?;
    }
    let had_rows = owner_record && has_owner_rows(&mut *conn, author).await?;
    let changed = match op.nsid.as_str() {
        NSID_POST_PRIVATE => {
            let f = PostPrivateFields::parse(&op.record, now)?;
            for field in &f.display.dropped {
                metrics::counter!(METRIC_FIELD_DROPPED, "field" => *field).increment(1);
            }
            for (field, source) in
                [("quote", f.display.quote_source), ("mentions", f.display.mentions_source), ("media", f.display.media_source)]
            {
                metrics::counter!(METRIC_INDEX_SOURCE, "field" => field, "source" => source.label()).increment(1);
            }
            // The notification rules (private-embeds design §6): rule 1
            // first, then the previous version's subjects, read under the
            // row lock the upsert takes a moment later anyway.
            let notifies = super::notifies_post(op.source, op.created, op.created_at, op.notify_max_age);
            let previous = if notifies { previous_subjects(&mut *conn, &op.uri).await? } else { None };
            // A row stored before migration 0023 (`display_filled` false)
            // also takes an op at its own rev: the refill (§3.1). The row
            // returned says whether it was inserted (`xmax = 0`) or updated;
            // none, that LWW refused the op (private-activity-subs §5.1).
            let inserted: Option<bool> = sqlx::query_scalar(
                "INSERT INTO private_posts (uri, author_did, rkey, cid, rev, audience_uri, reply_root_uri, \
                   reply_parent_uri, encoded_content, encoded_codec, encoded_generation, plaintext_text, created_at, \
                   reply_root_cid, reply_parent_cid, facets, embed, langs, self_labels, quote_uri, mention_dids, \
                   display_filled, media_kinds, fronters) \
                 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, true, $22, $23) \
                 ON CONFLICT (uri) DO UPDATE SET cid = EXCLUDED.cid, rev = EXCLUDED.rev, \
                   audience_uri = EXCLUDED.audience_uri, reply_root_uri = EXCLUDED.reply_root_uri, \
                   reply_parent_uri = EXCLUDED.reply_parent_uri, reply_root_cid = EXCLUDED.reply_root_cid, \
                   reply_parent_cid = EXCLUDED.reply_parent_cid, encoded_content = EXCLUDED.encoded_content, \
                   encoded_codec = EXCLUDED.encoded_codec, encoded_generation = EXCLUDED.encoded_generation, \
                   plaintext_text = EXCLUDED.plaintext_text, created_at = EXCLUDED.created_at, \
                   facets = EXCLUDED.facets, embed = EXCLUDED.embed, langs = EXCLUDED.langs, \
                   self_labels = EXCLUDED.self_labels, quote_uri = EXCLUDED.quote_uri, \
                   mention_dids = EXCLUDED.mention_dids, display_filled = true, media_kinds = EXCLUDED.media_kinds, \
                   fronters = EXCLUDED.fronters \
                 WHERE private_posts.rev < EXCLUDED.rev \
                    OR (NOT private_posts.display_filled AND private_posts.rev <= EXCLUDED.rev) \
                 RETURNING (xmax = 0)",
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
            .bind(i16::from(f.display.media_kinds))
            .bind(&f.display.fronters)
            .fetch_optional(&mut *conn)
            .await?;
            let changed = inserted.is_some();
            // A reply under a public root that is the root's only stored one
            // queues a fetch of the root's gate (public-root-gates design
            // §4.1).
            if changed && let Some((root, gate)) = public_root(f.reply_root_uri.as_deref()) {
                queue_root_gate_fetch(&mut *conn, &op.uri, root, &gate).await?;
            }
            // The first store of a recent post queues its subscribed-post
            // fan-out, whoever subscribes (private-activity-subs §5.1). An
            // update never does: an audience change, a rotation or a PDS
            // rewrite announces nothing.
            if inserted == Some(true) && super::fans_out(op.source, f.created_at, op.subscribed_max_age) {
                super::fanout::enqueue(&mut *conn, &op.uri, author).await?;
            }
            // Its notifications, when the row is written: an update notifies
            // only the recipients its previous version did not name, and a
            // live update only while the post is recent (private-embeds
            // design §6); rows already written are kept (§4.8).
            if changed && notifies {
                notify_post(&mut *conn, op, &f, previous.as_ref()).await?;
            } else if changed && op.source == WriteSource::Firehose && !op.created {
                metrics::counter!(METRIC_NOTIFY_SUPPRESSED, "reason" => "old_update").increment(1);
            }
            changed
        }
        other => return Err(malformed(format!("not a private-tier NSID: {other}"))),
    };
    let mut applied = Applied {
        changed,
        ..Applied::default()
    };
    // Only a member can become an owner (membership design §2.3): a
    // non-member's first row is stored and triggers nothing.
    if changed && owner_record && !had_rows && owners::is_eligible(&mut *conn, author).await? {
        applied = Applied {
            changed,
            ..owners::reevaluate(&mut *conn, author).await?
        };
        if applied.owners_joined.iter().any(|d| d == author) {
            enqueue_private(&mut *conn, author).await?;
        }
    }
    Ok(applied)
}

/// Enqueue an `authz` job unless one is already queued or running, as
/// [`enqueue_private`] does for `private`: a `following` audience's follow
/// verification (following-audience design §3.6).
pub(super) async fn enqueue_authz(conn: &mut PgConnection, did: &str) -> Result<(), sqlx::Error> {
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

/// `ingest::apply_root_gate` (public-root-gates design §3.3): LWW copy of a
/// public root's valid threadgate into `private_threadgates`, with
/// `private_tombstones` checked, kept only while a private post claims its
/// post as `reply.root`, or when a copy is already stored (so it stays
/// current after the last such reply goes). The writer has checked the
/// gate's shape: a threadgate naming an `app.bsky.feed.post` URI in its own
/// repo with its own rkey.
///
/// # Errors
///
/// [`IngestError::Malformed`] for a gate with no at-uri `post`; database
/// errors.
pub(crate) async fn apply_root_gate(conn: &mut PgConnection, op: &PrivateOp) -> Result<Applied, IngestError> {
    if tombstoned(&mut *conn, &op.uri, &op.rev).await? {
        return Ok(Applied::default());
    }
    let post = gate_post(&op.record)?;
    let at = created_at(&op.record, Utc::now());
    let changed = sqlx::query(
        "INSERT INTO private_threadgates (uri, author_did, rkey, cid, rev, post_uri, allow, hidden_replies, record, created_at) \
         SELECT $1, $2, $3, $4, $5, $6, $7, $8, $9, $10 \
         WHERE EXISTS (SELECT 1 FROM private_posts WHERE reply_root_uri = $6) \
            OR EXISTS (SELECT 1 FROM private_threadgates WHERE uri = $1) \
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
    .bind(op.record.get("allow").filter(|v| !v.is_null()).cloned())
    .bind(gate_strings(&op.record, "hiddenReplies"))
    .bind(&op.record)
    .bind(at)
    .execute(&mut *conn)
    .await?
    .rows_affected()
        > 0;
    Ok(Applied {
        changed,
        ..Applied::default()
    })
}

/// The root a fetch is queued for (public-root-gates design §4.1): a
/// private post's `reply_root_uri` when it is an `app.bsky.feed.post` URI
/// whose authority is a DID, with its canonical threadgate URI.
fn public_root(root: Option<&str>) -> Option<(&str, String)> {
    let root = root?;
    let mut parts = root.strip_prefix("at://")?.split('/');
    match (parts.next(), parts.next(), parts.next(), parts.next()) {
        (Some(did), Some(NSID_POST), Some(rkey), None) if did.starts_with("did:") && !rkey.is_empty() => {
            Some((root, format!("at://{did}/{NSID_THREADGATE}/{rkey}")))
        }
        _ => None,
    }
}

/// Queue the fetch of a public root's gate (public-root-gates design §4.1)
/// when the private post `uri` just written is the only stored one claiming
/// the root and no copy of its gate is stored: a gate written before then
/// was not kept. A pending fetch is left as it is; a done one is queued
/// again.
async fn queue_root_gate_fetch(conn: &mut PgConnection, uri: &str, root: &str, gate: &str) -> Result<(), sqlx::Error> {
    sqlx::query(
        "INSERT INTO private_root_gate_fetches (root_uri) \
         SELECT $1 WHERE NOT EXISTS (SELECT 1 FROM private_posts WHERE reply_root_uri = $1 AND uri <> $2) \
                     AND NOT EXISTS (SELECT 1 FROM private_threadgates WHERE uri = $3) \
         ON CONFLICT (root_uri) DO UPDATE SET queued_at = now(), next_at = now(), attempts = 0, done_at = NULL, \
           last_error = NULL \
         WHERE private_root_gate_fetches.done_at IS NOT NULL",
    )
    .bind(root)
    .bind(uri)
    .bind(gate)
    .execute(conn)
    .await?;
    Ok(())
}

/// `ingest::delete_gate`: delete the private copy of a threadgate or
/// postgate at `uri` with `rev <= R`. Tombstone iff a row was removed or the
/// author is in S (§5.5), as [`delete`] does, or a fetch of the gate is
/// pending, so that a fetch that read it before this delete cannot write it
/// after (public-root-gates design §3.4).
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
    if removed || author_in_scope || (table == "private_threadgates" && fetch_pending(&mut *conn, uri).await?) {
        tombstone(&mut *conn, uri, rev).await?;
    }
    Ok(Applied {
        changed: removed,
        ..Applied::default()
    })
}

/// Whether a fetch of the threadgate at `uri` is pending: one queued for the
/// public post with its repo and rkey (public-root-gates design §3.4).
async fn fetch_pending(conn: &mut PgConnection, gate: &str) -> Result<bool, sqlx::Error> {
    let Some((repo, rkey)) = gate.strip_prefix("at://").and_then(|r| {
        let mut parts = r.split('/');
        Some((parts.next()?, parts.nth(1)?))
    }) else {
        return Ok(false);
    };
    sqlx::query_scalar("SELECT EXISTS (SELECT 1 FROM private_root_gate_fetches WHERE root_uri = $1 AND done_at IS NULL)")
        .bind(format!("at://{repo}/{NSID_POST}/{rkey}"))
        .fetch_one(conn)
        .await
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
fn post_recipients(author: &str, s: &Subjects) -> Vec<Recipient> {
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
        ("reply", s.reply_parent_uri.as_ref()),
        ("reply", s.reply_root_uri.as_ref()),
        ("quote", s.quote_uri.as_ref()),
    ] {
        if let Some(uri) = uri.filter(|u| is_post(u)) {
            push(uri_author(uri), reason, Some(uri));
        }
    }
    for did in &s.mention_dids {
        push(Some(did), "mention", None);
    }
    out
}

/// What a private post's notifications are taken from: its reply parent
/// and root, its quote and its mentions, as stored.
#[derive(Debug, Clone, Default, PartialEq, Eq, sqlx::FromRow)]
struct Subjects {
    reply_parent_uri: Option<String>,
    reply_root_uri: Option<String>,
    quote_uri: Option<String>,
    mention_dids: Vec<String>,
}

impl Subjects {
    fn of(f: &PostPrivateFields) -> Self {
        Self {
            reply_parent_uri: f.reply_parent_uri.clone(),
            reply_root_uri: f.reply_root_uri.clone(),
            quote_uri: f.display.quote_uri.clone(),
            mention_dids: f.display.mention_dids.clone(),
        }
    }
}

/// The stored version's [`Subjects`] of the post at `uri`, locked for the
/// upsert that follows on the same transaction (private-embeds design §6
/// rule 2); `None` when no row is stored.
async fn previous_subjects(conn: &mut PgConnection, uri: &str) -> Result<Option<Subjects>, sqlx::Error> {
    sqlx::query_as(
        "SELECT reply_parent_uri, reply_root_uri, quote_uri, mention_dids FROM private_posts WHERE uri = $1 FOR UPDATE",
    )
    .bind(uri)
    .fetch_optional(conn)
    .await
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
/// A recipient the post's `previous` version also names is not notified
/// (private-embeds design §6 rule 2): the update is not news to them.
async fn notify_post(conn: &mut PgConnection, op: &PrivateOp, f: &PostPrivateFields, previous: Option<&Subjects>) -> Result<(), sqlx::Error> {
    let known: Vec<String> =
        previous.map(|p| post_recipients(&op.author_did, p).into_iter().map(|(did, ..)| did).collect()).unwrap_or_default();
    for r in post_recipients(&op.author_did, &Subjects::of(f)) {
        if known.contains(&r.0) {
            metrics::counter!(METRIC_NOTIFY_SUPPRESSED, "reason" => "known_recipient").increment(1);
            continue;
        }
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

/// The private table and its author column for an at-uri, by NSID: private
/// posts only. An audience URI has none: audiences are never ingested
/// (private-audiences design §4.2), so no delete or reconcile reaches one.
fn table_of(uri: &str) -> Option<(&'static str, &'static str)> {
    let collection = uri.strip_prefix("at://")?.split('/').nth(1)?;
    (collection == NSID_POST_PRIVATE).then_some(("private_posts", "author_did"))
}

/// `ingest::delete`: `DELETE … WHERE rev <= R`; tombstone iff a row was
/// removed or the author is in S (§5.5); owner re-evaluation when a post
/// went. The owner lock comes before the `DELETE` (private-audiences design
/// §11).
///
/// # Errors
///
/// Database errors.
pub(crate) async fn delete(conn: &mut PgConnection, uri: &str, rev: &str, author_in_scope: bool) -> Result<Applied, sqlx::Error> {
    let Some((table, _)) = table_of(uri) else {
        return Ok(Applied::default());
    };
    if let Some(author) = uri_author(uri) {
        owners::lock(&mut *conn, author).await?;
    }
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
    // record's do (private posting §4.8), unless a newer post lives at the
    // URI: they are its own then, and a late copy of an older delete leaves
    // them (#725).
    if table == "private_posts" {
        let newer: bool = sqlx::query_scalar("SELECT EXISTS (SELECT 1 FROM private_posts WHERE uri = $1 AND rev > $2)")
            .bind(uri)
            .bind(rev)
            .fetch_one(&mut *conn)
            .await?;
        if !newer {
            delete_notifications(&mut *conn, uri).await?;
        }
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
    if removed && let Some(author) = uri_author(uri)
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
/// collection whose rows keep a CID (private posts). Catch-up skips a
/// listed record whose CID is stored (private posting §3.8).
///
/// # Errors
///
/// Database errors.
pub(crate) async fn stored_cids(conn: &mut PgConnection, did: &str, nsid: &str) -> Result<HashMap<String, String>, sqlx::Error> {
    if nsid != NSID_POST_PRIVATE {
        return Ok(HashMap::new());
    }
    let rows: Vec<(String, Option<String>)> = sqlx::query_as("SELECT uri, cid FROM private_posts WHERE author_did = $1")
        .bind(did)
        .fetch_all(conn)
        .await?;
    Ok(rows.into_iter().filter_map(|(uri, cid)| cid.map(|c| (uri, c))).collect())
}

/// `ingest::replayed_since_listing` (replay-readiness design §4.2 step 2):
/// whether any record catch-up skipped by CID (`uris[i]` listed with
/// `cids[i]`) is stored in a version older than its listing at `rev`:
/// present, below `rev`, with another CID. A row at or above `rev` is a
/// newer state; a row that is gone only denies. Empty for a collection
/// other than private posts.
///
/// # Errors
///
/// Database errors.
pub(crate) async fn replayed_since_listing(
    conn: &mut PgConnection,
    nsid: &str,
    uris: &[String],
    cids: &[String],
    rev: &str,
) -> Result<bool, sqlx::Error> {
    if nsid != NSID_POST_PRIVATE || uris.is_empty() {
        return Ok(false);
    }
    let table = "private_posts";
    sqlx::query_scalar(&format!(
        "SELECT EXISTS (SELECT 1 FROM unnest($1::text[], $2::text[]) AS t(uri, cid) \
                        JOIN {table} p ON p.uri = t.uri \
                        WHERE p.cid <> t.cid AND p.rev < $3)"
    ))
    .bind(uris)
    .bind(cids)
    .bind(rev)
    .fetch_one(conn)
    .await
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

/// The private notifications `did` received and those it authored, its
/// last-seen row and its notification preferences, at most `limit` rows
/// (private posting §3.1: the `account_deleted` and `member_removed` purges;
/// private notification preferences design §3.1). An authored row may hold
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
    for sql in [
        "DELETE FROM private_notification_seen WHERE did = $1",
        "DELETE FROM private_notification_prefs WHERE did = $1",
    ] {
        n += sqlx::query(sql).bind(did).execute(&mut *conn).await?.rows_affected();
    }
    Ok(n)
}

/// The account's appearance setting, one row at most, live or retained
/// (private-appearance design §3.2: the `account_deleted` purge; the
/// `member_removed` purge retains it instead, amendment 1 §4.3).
async fn purge_appearance(conn: &mut PgConnection, did: &str) -> Result<u64, sqlx::Error> {
    Ok(sqlx::query("DELETE FROM private_appearance WHERE did = $1").bind(did).execute(&mut *conn).await?.rows_affected())
}

/// `ingest::retain_appearance` (private-appearance amendment 1 §4.1): in the
/// removal marker's transaction, after it deleted the `members` row, keep
/// the removed member's appearance row for 7 days from the marker's
/// `now()`. A second removal restarts the 7 days; a row already past them
/// is left to the sweep, never revived. `rev` and `updated_at` stay.
///
/// # Errors
///
/// Database errors.
pub(crate) async fn retain_appearance(conn: &mut PgConnection, did: &str) -> Result<(), sqlx::Error> {
    let n = sqlx::query(&format!(
        "UPDATE private_appearance SET retained_until = now() + {} \
         WHERE did = $1 AND (retained_until IS NULL OR retained_until > now())",
        super::appearance::RETAIN_FOR
    ))
    .bind(did)
    .execute(&mut *conn)
    .await?
    .rows_affected();
    if n > 0 {
        metrics::counter!(super::appearance::METRIC_RETENTION, "event" => "retained").increment(n);
    }
    Ok(())
}

/// The batch backstop (amendment 1 §4.2): a `member_removed` purge whose
/// marker committed without retaining the row (the code before the
/// retention) retains it here, from the purge's start. It matches at most
/// once, so the batch loop still ends; a member's row is never matched.
async fn retain_appearance_backstop(conn: &mut PgConnection, did: &str) -> Result<u64, sqlx::Error> {
    let n = sqlx::query(&format!(
        "UPDATE private_appearance \
            SET retained_until = COALESCE((SELECT started_at FROM purging_dids WHERE did = $1), now()) + {} \
          WHERE did = $1 AND retained_until IS NULL \
            AND NOT EXISTS (SELECT 1 FROM members WHERE did = $1)",
        super::appearance::RETAIN_FOR
    ))
    .bind(did)
    .execute(&mut *conn)
    .await?
    .rows_affected();
    if n > 0 {
        metrics::counter!(super::appearance::METRIC_RETENTION, "event" => "backstop").increment(n);
    }
    Ok(n)
}

/// The private activity subscriptions where `did` is the subscriber or the
/// subject, at most `limit` rows (private-activity-subs design §3.2: the
/// `account_deleted` and `member_removed` purges).
async fn purge_subscriptions(conn: &mut PgConnection, did: &str, limit: i64) -> Result<u64, sqlx::Error> {
    Ok(sqlx::query(
        "DELETE FROM private_activity_subscriptions WHERE ctid IN (SELECT ctid FROM private_activity_subscriptions \
         WHERE subscriber_did = $1 OR subject_did = $1 LIMIT $2)",
    )
    .bind(did)
    .bind(limit)
    .execute(&mut *conn)
    .await?
    .rows_affected())
}

/// `ingest::purge_did` (account deletion): every private row authored by the
/// DID, its owner state, its private bookmarks, its private notifications,
/// the private subscriptions it holds or is the subject of, its posts'
/// subscribed-post queue rows, and its appearance setting, at most `limit`
/// rows per table.
///
/// # Errors
///
/// Database errors.
pub(crate) async fn purge_did(conn: &mut PgConnection, did: &str, limit: i64) -> Result<u64, sqlx::Error> {
    let mut n = purge_bookmarks(conn, did, limit).await?;
    n += purge_notifications(conn, did, limit).await?;
    n += purge_subscriptions(conn, did, limit).await?;
    n += purge_appearance(conn, did).await?;
    // Its posts' subscribed-post queue rows (private-activity-subs §3.2).
    n += sqlx::query("DELETE FROM private_post_fanout WHERE uri IN (SELECT uri FROM private_post_fanout WHERE author_did = $1 LIMIT $2)")
        .bind(did)
        .bind(limit)
        .execute(&mut *conn)
        .await?
        .rows_affected();
    for (table, col) in [
        ("private_posts", "author_did"),
        ("private_audiences", "owner_did"),
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
    n += sqlx::query("DELETE FROM private_owner_ready WHERE did = $1")
        .bind(did)
        .execute(&mut *conn)
        .await?
        .rows_affected();
    Ok(n)
}

/// A post whose reply parent or root author, or whose audience's author, is
/// a community member (class M; the audience's author since the
/// private-threads design §2.5, as admission's subjects).
pub(super) const REPLY_TO_MEMBER: &str = "EXISTS (SELECT 1 FROM members m WHERE m.did IN \
     (split_part(p.reply_parent_uri, '/', 3), split_part(p.reply_root_uri, '/', 3), split_part(p.audience_uri, '/', 3)))";

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
    // Its public roots' gates stay: they are kept whoever wrote them
    // (public-root-gates design §3.5).
    for table in ["private_threadgates", "private_postgates"] {
        n += sqlx::query(&format!(
            "DELETE FROM {table} WHERE uri IN (SELECT g.uri FROM {table} g WHERE g.author_did = $1 \
               AND split_part(g.post_uri, '/', 4) = '{NSID_POST_PRIVATE}' \
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

/// The private part of a `member_removed` purge (§6.11.7): private posts by
/// authors outside S admitted only because `did` was a member (no other
/// member among the parent, root and audience authors), the member's
/// private bookmarks, the member's quote references (private posting §3.1: its
/// later deletes are no longer tracked), and the private notifications it
/// received and authored, for the same reason; and the private
/// subscriptions it holds or is the subject of (private-activity-subs §3.2).
/// Its appearance setting is kept, not deleted: retained by the marker, or
/// here by the backstop (private-appearance amendment 1 §4.2). Its own
/// audiences go, for good: this AppView holds the only copy, and a member
/// who rejoins starts with none (private-audiences design §7, C-7).
///
/// # Errors
///
/// Database errors.
pub(crate) async fn purge_member_removed(conn: &mut PgConnection, did: &str, limit: i64) -> Result<u64, sqlx::Error> {
    let mut n = purge_bookmarks(conn, did, limit).await?;
    n += sqlx::query(
        "DELETE FROM private_audiences WHERE uri IN (SELECT uri FROM private_audiences WHERE owner_did = $1 LIMIT $2)",
    )
    .bind(did)
    .bind(limit)
    .execute(&mut *conn)
    .await?
    .rows_affected();
    n += purge_notifications(conn, did, limit).await?;
    n += purge_subscriptions(conn, did, limit).await?;
    n += retain_appearance_backstop(conn, did).await?;
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
           AND $1 IN (split_part(p.reply_parent_uri, '/', 3), split_part(p.reply_root_uri, '/', 3), \
                      split_part(p.audience_uri, '/', 3)) \
           AND NOT EXISTS (SELECT 1 FROM members m WHERE m.did <> $1 AND m.did IN \
               (split_part(p.reply_parent_uri, '/', 3), split_part(p.reply_root_uri, '/', 3), \
                split_part(p.audience_uri, '/', 3))) \
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
            (json!({"$type": "app.bsky.embed.recordWithMedia", "record": {"record": {"uri": quoted, "cid": "c"}}, "media": {}}), Some(quoted)),
            // A strong ref with no `cid` is not a quote (private-encode
            // design §5.2, E-C7: the PDS's derivation needs both).
            (json!({"$type": "app.bsky.embed.recordWithMedia", "record": {"record": {"uri": quoted}}, "media": {}}), None),
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
        // The dedicated fields (private-embeds design §4.2): wrong types or
        // over 64 KiB are dropped and named, and never fall back.
        let bad = json!({"quote": "at://x", "mentionDids": {"did": "did:plc:cccccccccccccccccccccccc"},
                         "embed": {"$type": "app.bsky.embed.record", "record": {"uri": quoted, "cid": "c"}},
                         "facets": [{"features": [mention("app.bsky.richtext.facet#mention", "did:plc:cccccccccccccccccccccccc")]}]});
        let d = DisplayFields::parse(&bad);
        assert_eq!(d.dropped, ["quote", "mentionDids"]);
        assert_eq!((d.quote_uri, d.mention_dids.len()), (None, 0));
        assert_eq!((d.quote_source, d.mentions_source), (IndexSource::Dedicated, IndexSource::Dedicated));
        assert!(d.embed.is_some() && d.facets.is_some(), "the clear fields are still kept for display");
        let many: Vec<String> = (0..3000).map(|i| format!("did:plc:{i:024}")).collect();
        let d = DisplayFields::parse(&json!({"mentionDids": many}));
        assert_eq!((d.dropped, d.mention_dids.len()), (vec!["mentionDids"], 0));
        // Null is absent: the clear fields index.
        let d = DisplayFields::parse(&json!({"quote": null, "mentionDids": null, "embed": bad["embed"], "facets": bad["facets"]}));
        assert_eq!((d.quote_uri.as_deref(), d.mention_dids.len(), d.dropped.len()), (Some(quoted), 1, 0));
        assert_eq!((d.quote_source, d.mentions_source), (IndexSource::Clear, IndexSource::Clear));
    }

    /// T-E11 (derivation): §4.2's table. The dedicated field, when present,
    /// decides; else the clear one, as before #504; neither, nothing.
    #[test]
    fn quote_and_mentions_come_from_the_dedicated_fields_first() {
        let x = "at://did:plc:bbbbbbbbbbbbbbbbbbbbbbbb/tools.kryphocron.feed.postPrivate/3kx";
        let y = "at://did:plc:dddddddddddddddddddddddd/app.bsky.feed.post/3ky";
        let (c, d) = ("did:plc:cccccccccccccccccccccccc", "did:plc:dddddddddddddddddddddddd");
        let embed_y = json!({"$type": "tools.kryphocron.feed.postPublic#embedRecord", "record": {"uri": y, "cid": "c"}});
        let facets_d = json!([{"features": [{"$type": "#mention", "did": d}]}]);
        for (case, record, quote, mentions, sources) in [
            ("new form", json!({"quote": {"uri": x, "cid": "c"}, "mentionDids": [c, d, c]}), Some(x), vec![c, d], (IndexSource::Dedicated, IndexSource::Dedicated)),
            ("old form", json!({"embed": embed_y, "facets": facets_d}), Some(y), vec![d], (IndexSource::Clear, IndexSource::Clear)),
            ("both: the dedicated fields win", json!({"quote": {"uri": x}, "mentionDids": [c], "embed": embed_y, "facets": facets_d}), Some(x), vec![c], (IndexSource::Dedicated, IndexSource::Dedicated)),
            ("neither", json!({"text": "t"}), None, vec![], (IndexSource::None, IndexSource::None)),
            ("a quote of a bad uri does not fall back", json!({"quote": {"uri": "https://x"}, "embed": embed_y}), None, vec![], (IndexSource::Dedicated, IndexSource::None)),
            ("a quote with no uri", json!({"quote": {"cid": "c"}}), None, vec![], (IndexSource::Dedicated, IndexSource::None)),
            ("no valid DID does not fall back", json!({"mentionDids": ["bob", 5, {"did": c}], "facets": facets_d}), None, vec![], (IndexSource::None, IndexSource::Dedicated)),
            ("valid DIDs among junk", json!({"mentionDids": ["bob", d, 5, c, d]}), None, vec![d, c], (IndexSource::None, IndexSource::Dedicated)),
            ("an empty mentionDids", json!({"mentionDids": [], "facets": facets_d}), None, vec![], (IndexSource::None, IndexSource::Dedicated)),
        ] {
            let parsed = DisplayFields::parse(&record);
            assert_eq!(parsed.quote_uri.as_deref(), quote, "{case}");
            assert_eq!(parsed.mention_dids, mentions, "{case}");
            assert_eq!((parsed.quote_source, parsed.mentions_source), sources, "{case}");
            assert!(parsed.dropped.is_empty(), "{case}");
            assert!(PostPrivateFields::parse(&record, now()).is_ok(), "{case}: never malformed");
        }
    }

    /// T-M6 (private-media-filters design §3.2, §3.3): a present
    /// `mediaKinds` decides, even when dropped or empty, and never falls
    /// back to the clear `embed`; absent or null, the clear `embed` decides,
    /// in either vocabulary; neither, no media. Never malformed.
    #[test]
    fn media_kinds_come_from_the_marker_first() {
        use crate::private_tier::media::{IMAGE, VIDEO, embed_fixtures};
        let video = json!({"$type": "app.bsky.embed.video", "video": {"$type": "blob", "ref": {"$link": "v"}, "mimeType": "video/mp4", "size": 1}});
        let big: Vec<String> = (0..10_000).map(|i| format!("image{i:04}")).collect();
        for (case, record, bits, source, dropped) in [
            ("image", json!({"mediaKinds": ["image"]}), IMAGE, IndexSource::Dedicated, false),
            ("video", json!({"mediaKinds": ["video"]}), VIDEO, IndexSource::Dedicated, false),
            ("both", json!({"mediaKinds": ["image", "video"]}), IMAGE | VIDEO, IndexSource::Dedicated, false),
            ("both, out of order", json!({"mediaKinds": ["video", "image"]}), IMAGE | VIDEO, IndexSource::Dedicated, false),
            ("a duplicate", json!({"mediaKinds": ["image", "image"]}), IMAGE, IndexSource::Dedicated, false),
            ("an unknown kind", json!({"mediaKinds": ["audio"]}), 0, IndexSource::Dedicated, false),
            ("a non-string among kinds", json!({"mediaKinds": [1, "image"]}), IMAGE, IndexSource::Dedicated, false),
            ("empty: no fallback", json!({"mediaKinds": [], "embed": video}), 0, IndexSource::Dedicated, false),
            ("null: absent", json!({"mediaKinds": null, "embed": video}), VIDEO, IndexSource::Clear, false),
            ("a string: dropped", json!({"mediaKinds": "image"}), 0, IndexSource::Dedicated, true),
            ("an object: dropped", json!({"mediaKinds": {"image": true}, "embed": video}), 0, IndexSource::Dedicated, true),
            ("over 64 KiB: dropped", json!({"mediaKinds": big, "embed": video}), 0, IndexSource::Dedicated, true),
            ("the marker wins over a disagreeing embed", json!({"mediaKinds": ["image"], "embed": video}), IMAGE, IndexSource::Dedicated, false),
            ("no marker, the clear embed", json!({"embed": video}), VIDEO, IndexSource::Clear, false),
            ("neither", json!({"text": "t"}), 0, IndexSource::None, false),
        ] {
            let d = DisplayFields::parse(&record);
            assert_eq!((d.media_kinds, d.media_source), (bits, source), "{case}");
            assert_eq!(d.dropped.contains(&"mediaKinds"), dropped, "{case}: {:?}", d.dropped);
            assert!(PostPrivateFields::parse(&record, now()).is_ok(), "{case}: never malformed");
        }
        // The clear-embed path over every embed kind, in both vocabularies.
        for (case, embed, bits) in embed_fixtures() {
            let d = DisplayFields::parse(&json!({"embed": embed}));
            assert_eq!((d.media_kinds, d.media_source), (bits, IndexSource::Clear), "{case}");
        }
    }

    /// The fallback as it stood before private-encode E1, for T-P3: the
    /// quote from the normalized embed's `uri` alone, the mentions from the
    /// three feature types, the media from the read-side classifier.
    fn old_fallback(facets: Option<&Value>, embed: Option<&Value>) -> (Option<String>, Vec<String>, u8) {
        let valid_quote = |u: &str| u.starts_with("at://") && uri_author(u).is_some_and(proto_blue::syntax::Did::is_valid);
        let quote = embed
            .map(crate::hydration::embeds::normalize_embed)
            .and_then(|e| crate::hydration::embeds::quoted_uri(&e).map(str::to_string))
            .filter(|u| valid_quote(u));
        let types = ["app.bsky.richtext.facet#mention", "tools.kryphocron.feed.postPublic#mention", "#mention"];
        let mut mentions: Vec<String> = Vec::new();
        for did in facets
            .iter()
            .flat_map(|f| f.as_array().into_iter().flatten())
            .flat_map(|facet| facet.get("features").and_then(Value::as_array).into_iter().flatten())
            .filter(|feature| feature.get("$type").and_then(Value::as_str).is_some_and(|t| types.contains(&t)))
            .filter_map(|feature| feature.get("did").and_then(Value::as_str))
        {
            if proto_blue::syntax::Did::is_valid(did) && !mentions.iter().any(|d| d == did) {
                mentions.push(did.to_string());
            }
        }
        (quote, mentions, embed.map_or(0, media::kinds_of_embed))
    }

    /// T-P3 (private-encode design §5.2): over every parity fixture and
    /// every media fixture, the fallback through the shared derivation
    /// equals the old one on `mention_dids` and `media_kinds`, and on
    /// `quote_uri` differs only for a quote whose strong ref has no string
    /// `cid`.
    #[test]
    fn t_p3_the_shared_fallback_equals_the_old_one() {
        let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src/private_tier/tests/fixtures/content_parity.json");
        let doc: Value = serde_json::from_str(&std::fs::read_to_string(path).expect("fixture")).expect("json");
        let mut inputs: Vec<(String, Value)> = Vec::new();
        for case in doc["cases"].as_array().expect("cases") {
            let mut record = json!({});
            for key in ["facets", "embed"] {
                if let Some(v) = case.get(key) {
                    record[key] = v.clone();
                }
            }
            inputs.push((case["name"].as_str().expect("name").to_string(), record));
        }
        for (case, embed, _) in crate::private_tier::media::embed_fixtures() {
            inputs.push((format!("media fixture: {case}"), json!({"embed": embed})));
        }
        let mut differed = Vec::new();
        for (case, record) in &inputs {
            let d = DisplayFields::parse(record);
            let (quote, mentions, media) = old_fallback(d.facets.as_ref(), d.embed.as_ref());
            assert_eq!(d.mention_dids, mentions, "{case}");
            assert_eq!(d.media_kinds, media, "{case}");
            if d.quote_uri != quote {
                let embed = crate::hydration::embeds::normalize_embed(d.embed.as_ref().expect("embed"));
                let strong_ref = match embed["$type"].as_str() {
                    Some("app.bsky.embed.record") => &embed["record"],
                    _ => &embed["record"]["record"],
                };
                assert!(!strong_ref["cid"].is_string() && d.quote_uri.is_none(), "{case}: {:?} vs {quote:?}", d.quote_uri);
                differed.push(case.clone());
            }
        }
        assert_eq!(differed, ["quote without a cid", "quote with a non-string cid"], "the cid-less quotes, and only they");
    }

    /// Rule 1 (private-embeds design §6): a live create notifies at any age;
    /// every other write only within the age; `notifies` (public records)
    /// is unchanged.
    #[test]
    fn a_live_update_of_an_old_post_does_not_notify() {
        let max = chrono::Duration::hours(48);
        let (old, new) = (now() - chrono::Duration::days(3), now() - chrono::Duration::hours(1));
        for (source, created, at, expected) in [
            (WriteSource::Firehose, true, old, true),
            (WriteSource::Firehose, false, old, false),
            (WriteSource::Firehose, false, new, true),
            (WriteSource::Backfill, true, old, false),
            (WriteSource::Backfill, false, new, true),
            (WriteSource::CatchUp, true, old, false),
        ] {
            assert_eq!(crate::private_tier::notifies_post(source, created, at, max), expected, "{source:?} {created} {at}");
        }
        assert!(crate::private_tier::notifies(WriteSource::Firehose, old, max), "public records: unchanged");
    }
}
