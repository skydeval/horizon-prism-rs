//! Private-table queries (design-v2 §3.2, §4.5, §6.3, §6.9), `pub(super)`.
//!
//! - **Content** (`encoded`, `plaintext`, `view_meta`, `validation_flags`)
//!   takes `&RecordAuthz` only and reads only its resource: a witness for one
//!   record cannot load another. There is no URI parameter.
//! - **Non-content** queries (oracle facts, candidate prefilters) take DIDs
//!   and URIs and return no private content.

use chrono::{DateTime, Utc};
use sqlx::PgPool;

use super::pipeline::RecordAuthz;

/// Pre-bind facts about a post (non-content): owner and audience URI.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct PostFacts {
    pub owner: String,
    pub audience_uri: Option<String>,
}

/// # Errors
///
/// Database errors.
pub(super) async fn post_facts(pool: &PgPool, uri: &str) -> Result<Option<PostFacts>, sqlx::Error> {
    let row: Option<(String, Option<String>)> =
        sqlx::query_as("SELECT author_did, audience_uri FROM private_posts WHERE uri = $1")
            .bind(uri)
            .fetch_optional(pool)
            .await?;
    Ok(row.map(|(owner, audience_uri)| PostFacts { owner, audience_uri }))
}

/// An audience row (oracle facts).
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct AudienceRow {
    pub owner: String,
    pub mode: Option<String>,
    pub members: Vec<String>,
}

/// # Errors
///
/// Database errors.
pub(super) async fn audience_row(pool: &PgPool, uri: &str) -> Result<Option<AudienceRow>, sqlx::Error> {
    let row: Option<(String, Option<String>, Vec<String>)> =
        sqlx::query_as("SELECT owner_did, mode, members FROM private_audiences WHERE uri = $1")
            .bind(uri)
            .fetch_optional(pool)
            .await?;
    Ok(row.map(|(owner, mode, members)| AudienceRow { owner, mode, members }))
}

/// `(a blocks b, b blocks a)` over `blocks` and `private_blocks` (§6.4).
///
/// # Errors
///
/// Database errors.
pub(super) async fn blocks_between(pool: &PgPool, a: &str, b: &str) -> Result<(bool, bool), sqlx::Error> {
    sqlx::query_as(
        "SELECT \
           EXISTS (SELECT 1 FROM blocks WHERE author_did = $1 AND subject_did = $2) \
             OR EXISTS (SELECT 1 FROM private_blocks WHERE author_did = $1 AND subject_did = $2), \
           EXISTS (SELECT 1 FROM blocks WHERE author_did = $2 AND subject_did = $1) \
             OR EXISTS (SELECT 1 FROM private_blocks WHERE author_did = $2 AND subject_did = $1)",
    )
    .bind(a)
    .bind(b)
    .fetch_one(pool)
    .await
}

/// Authors of private rows (retention inputs, §4.4: their `actors` rows are
/// kept).
///
/// # Errors
///
/// Database errors.
pub(super) async fn row_authors(pool: &PgPool) -> Result<Vec<String>, sqlx::Error> {
    sqlx::query_scalar(
        "SELECT author_did FROM private_posts UNION SELECT owner_did FROM private_audiences \
         UNION SELECT author_did FROM private_blocks",
    )
    .fetch_all(pool)
    .await
}

/// Non-content view metadata, behind the witness.
#[derive(Debug, Clone, PartialEq, Eq, sqlx::FromRow)]
pub(super) struct ViewMeta {
    pub cid: String,
    pub audience_uri: Option<String>,
    pub reply_root_uri: Option<String>,
    pub reply_parent_uri: Option<String>,
    pub reply_root_cid: Option<String>,
    pub reply_parent_cid: Option<String>,
    pub created_at: DateTime<Utc>,
    pub indexed_at: DateTime<Utc>,
}

/// # Errors
///
/// Database errors.
pub(super) async fn view_meta(pool: &PgPool, rauth: &RecordAuthz) -> Result<Option<ViewMeta>, sqlx::Error> {
    sqlx::query_as(
        "SELECT cid, audience_uri, reply_root_uri, reply_parent_uri, reply_root_cid, reply_parent_cid, created_at, indexed_at \
         FROM private_posts WHERE uri = $1",
    )
    .bind(rauth.uri())
    .fetch_optional(pool)
    .await
}

/// Field presence for `RecordValidation::post_private` (§6.3 step 10):
/// `(has_text, has_encoded_content, has_codec, has_generation)`.
///
/// # Errors
///
/// Database errors.
pub(super) async fn validation_flags(
    pool: &PgPool,
    rauth: &RecordAuthz,
) -> Result<Option<(bool, bool, bool, bool)>, sqlx::Error> {
    sqlx::query_as(
        "SELECT plaintext_text IS NOT NULL, encoded_content IS NOT NULL, \
           encoded_codec IS NOT NULL, encoded_generation IS NOT NULL \
         FROM private_posts WHERE uri = $1",
    )
    .bind(rauth.uri())
    .fetch_optional(pool)
    .await
}

/// Encoded content as stored (§4.5).
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct EncodedRow {
    pub content: Vec<u8>,
    pub codec: Option<String>,
    pub generation: Option<String>,
}

/// `schema::encoded(&rauth)`: the record's encoded content, if it has any.
///
/// # Errors
///
/// Database errors.
pub(super) async fn encoded(pool: &PgPool, rauth: &RecordAuthz) -> Result<Option<EncodedRow>, sqlx::Error> {
    type Row = (Option<Vec<u8>>, Option<String>, Option<String>);
    let row: Option<Row> = sqlx::query_as(
        "SELECT encoded_content, encoded_codec, encoded_generation FROM private_posts WHERE uri = $1",
    )
    .bind(rauth.uri())
    .fetch_optional(pool)
    .await?;
    Ok(row.and_then(|(content, codec, generation)| content.map(|content| EncodedRow { content, codec, generation })))
}

/// `schema::plaintext(&rauth)`: `plaintext_text` where the lexicon's XOR rule
/// allowed plaintext.
///
/// # Errors
///
/// Database errors.
pub(super) async fn plaintext(pool: &PgPool, rauth: &RecordAuthz) -> Result<Option<String>, sqlx::Error> {
    sqlx::query_scalar::<_, Option<String>>("SELECT plaintext_text FROM private_posts WHERE uri = $1")
        .bind(rauth.uri())
        .fetch_optional(pool)
        .await
        .map(Option::flatten)
}

/// A block in either direction between `x` and `y` (SQL expressions), over
/// the two sources [`blocks_between`] reads.
fn blocked(x: &str, y: &str) -> String {
    format!(
        "(EXISTS (SELECT 1 FROM blocks b WHERE b.author_did = {x} AND b.subject_did = {y}) \
          OR EXISTS (SELECT 1 FROM blocks b WHERE b.author_did = {y} AND b.subject_did = {x}) \
          OR EXISTS (SELECT 1 FROM private_blocks b WHERE b.author_did = {x} AND b.subject_did = {y}) \
          OR EXISTS (SELECT 1 FROM private_blocks b WHERE b.author_did = {y} AND b.subject_did = {x}))"
    )
}

/// The §6.9 candidate prefilter, mirroring §6.4's rules for the requester
/// (`$1`), as the audience-modes design amends them:
/// - the author is active (the moderation gate denies a non-active
///   author's posts to everyone);
/// - self-authored; or an audience row is present, of the same owner or of
///   an active account on the same PDS endpoint, and its mode admits the
///   requester: `list` (or none) by `members`, `followers` by the
///   requester's stored follow of the audience's owner (who is in its own
///   audience), `everyone` always, and nothing else;
/// - no block between the requester and the author; and, for `followers`
///   and `everyone` under another account's audience, none between that
///   account and the requester or the author.
///
/// A candidate filter only: `bind` stays authoritative. Blocked pairs are
/// left out so that posts nobody can read do not fill a page's window.
fn prefilter() -> String {
    format!(
        "(COALESCE(po.account_status, 'active') = 'active' AND (p.author_did = $1 OR ( \
           a.uri IS NOT NULL \
           AND (a.owner_did = p.author_did \
                OR (ao.pds_endpoint IS NOT NULL AND ao.pds_endpoint = po.pds_endpoint AND ao.account_status = 'active')) \
           AND CASE COALESCE(a.mode, 'list') \
                 WHEN 'list' THEN $1 = ANY(a.members) \
                 WHEN 'followers' THEN a.owner_did = $1 OR EXISTS ( \
                   SELECT 1 FROM follows f WHERE f.author_did = $1 AND f.subject_did = a.owner_did) \
                 WHEN 'everyone' THEN true \
                 ELSE false END \
           AND NOT {viewer_author} \
           AND (COALESCE(a.mode, 'list') NOT IN ('followers', 'everyone') \
                OR a.owner_did = p.author_did \
                OR NOT ((a.owner_did <> $1 AND {viewer_owner}) OR {author_owner})))))",
        viewer_author = blocked("$1", "p.author_did"),
        viewer_owner = blocked("$1", "a.owner_did"),
        author_owner = blocked("p.author_did", "a.owner_did"),
    )
}

/// Whether a follow record by `author` naming `subject` is stored.
///
/// # Errors
///
/// Database errors.
pub(super) async fn follows(pool: &PgPool, author: &str, subject: &str) -> Result<bool, sqlx::Error> {
    sqlx::query_scalar("SELECT EXISTS (SELECT 1 FROM follows WHERE author_did = $1 AND subject_did = $2)")
        .bind(author)
        .bind(subject)
        .fetch_one(pool)
        .await
}

const PREFILTER_FROM: &str = "private_posts p \
     LEFT JOIN private_audiences a ON a.uri = p.audience_uri \
     LEFT JOIN actors po ON po.did = p.author_did \
     LEFT JOIN actors ao ON ao.did = a.owner_did";

/// The URIs among `uris` that pass the prefilter.
///
/// # Errors
///
/// Database errors.
pub(super) async fn prefilter_uris(pool: &PgPool, requester: &str, uris: &[String]) -> Result<Vec<String>, sqlx::Error> {
    sqlx::query_scalar(&format!(
        "SELECT p.uri FROM {PREFILTER_FROM} WHERE p.uri = ANY($2) AND {pre}",
        pre = prefilter()
    ))
    .bind(requester)
    .bind(uris)
    .fetch_all(pool)
    .await
}

/// Up to `limit` replies to `parent` passing the prefilter, oldest first.
///
/// # Errors
///
/// Database errors.
pub(super) async fn reply_candidates(pool: &PgPool, requester: &str, parent: &str, limit: i64) -> Result<Vec<String>, sqlx::Error> {
    sqlx::query_scalar(&format!(
        "SELECT p.uri FROM {PREFILTER_FROM} WHERE p.reply_parent_uri = $2 AND {pre} \
         ORDER BY p.created_at, p.uri LIMIT $3",
        pre = prefilter()
    ))
    .bind(requester)
    .bind(parent)
    .bind(limit)
    .fetch_all(pool)
    .await
}

/// A feed candidate: `(uri, created_at, rkey)`.
pub(super) type FeedCandidate = (String, DateTime<Utc>, String);

/// The actor's posts passing the prefilter, newest first, after the
/// `created_at::rkey` cursor.
///
/// # Errors
///
/// Database errors.
pub(super) async fn author_candidates(
    pool: &PgPool,
    requester: &str,
    author: &str,
    after: Option<(DateTime<Utc>, String)>,
    limit: i64,
) -> Result<Vec<FeedCandidate>, sqlx::Error> {
    let (at, rkey) = after.map_or((None, String::new()), |(a, r)| (Some(a), r));
    sqlx::query_as(&format!(
        "SELECT p.uri, p.created_at, p.rkey FROM {PREFILTER_FROM} \
         WHERE p.author_did = $2 AND {pre} \
           AND ($3::timestamptz IS NULL OR (p.created_at, p.rkey) < ($3, $4)) \
         ORDER BY p.created_at DESC, p.rkey DESC LIMIT $5",
        pre = prefilter()
    ))
    .bind(requester)
    .bind(author)
    .bind(at)
    .bind(rkey)
    .bind(limit)
    .fetch_all(pool)
    .await
}

/// The home timeline's candidates (design-v3 §1.7 item 12): posts passing
/// the prefilter whose author is the requester or an account the requester
/// follows, as this AppView has indexed the follows, newest first, after
/// the `created_at::uri` cursor. The third value is the URI again: across
/// authors a record key is not unique, so the URI breaks ties.
///
/// A mute applies here as it does on the public timeline: an author the
/// requester fully mutes, directly or through a moderation list that
/// applies, is left out.
///
/// # Errors
///
/// Database errors.
pub(super) async fn timeline_candidates(
    pool: &PgPool,
    requester: &str,
    after: Option<(DateTime<Utc>, String)>,
    limit: i64,
) -> Result<Vec<FeedCandidate>, sqlx::Error> {
    let (at, uri) = after.map_or((None, String::new()), |(a, u)| (Some(a), u));
    sqlx::query_as(&format!(
        "SELECT p.uri, p.created_at, p.uri FROM {PREFILTER_FROM} \
         WHERE (p.author_did = $1 OR p.author_did IN (SELECT f.subject_did FROM follows f WHERE f.author_did = $1)) \
           AND {pre} \
           AND NOT EXISTS (SELECT 1 FROM mutes m WHERE m.viewer_did = $1 AND m.subject_did = p.author_did AND m.scope = 'full') \
           AND NOT EXISTS (SELECT 1 FROM list_references r \
                           JOIN subscribed_lists l ON l.list_uri = r.list_uri \
                           JOIN subscribed_list_items i ON i.list_uri = r.list_uri \
                           WHERE r.member_did = $1 AND i.subject_did = p.author_did AND {applies}) \
           AND ($2::timestamptz IS NULL OR (p.created_at, p.uri) < ($2, $3)) \
         ORDER BY p.created_at DESC, p.uri DESC LIMIT $4",
        applies = crate::hydration::APPLIES,
        pre = prefilter(),
    ))
    .bind(requester)
    .bind(at)
    .bind(uri)
    .bind(limit)
    .fetch_all(pool)
    .await
}
