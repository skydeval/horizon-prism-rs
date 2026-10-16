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

/// Pre-bind facts about a post (non-content): owner, audience URI and the
/// parent it replies to (rule 4's walk starts there).
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct PostFacts {
    pub owner: String,
    pub audience_uri: Option<String>,
    pub reply_parent_uri: Option<String>,
}

/// # Errors
///
/// Database errors.
pub(super) async fn post_facts(pool: &PgPool, uri: &str) -> Result<Option<PostFacts>, sqlx::Error> {
    let row: Option<(String, Option<String>, Option<String>)> =
        sqlx::query_as("SELECT author_did, audience_uri, reply_parent_uri FROM private_posts WHERE uri = $1")
            .bind(uri)
            .fetch_optional(pool)
            .await?;
    Ok(row.map(|(owner, audience_uri, reply_parent_uri)| PostFacts {
        owner,
        audience_uri,
        reply_parent_uri,
    }))
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
    /// The audience row's presence, `mode` and `name` (P-3, §4.10).
    pub audience_found: bool,
    pub audience_mode: Option<String>,
    pub audience_name: Option<String>,
}

/// # Errors
///
/// Database errors.
pub(super) async fn view_meta(pool: &PgPool, rauth: &RecordAuthz) -> Result<Option<ViewMeta>, sqlx::Error> {
    sqlx::query_as(
        "SELECT p.cid, p.audience_uri, p.reply_root_uri, p.reply_parent_uri, p.reply_root_cid, p.reply_parent_cid, \
           p.created_at, p.indexed_at, a.uri IS NOT NULL AS audience_found, a.mode AS audience_mode, a.name AS audience_name \
         FROM private_posts p LEFT JOIN private_audiences a ON a.uri = p.audience_uri WHERE p.uri = $1",
    )
    .bind(rauth.uri())
    .fetch_optional(pool)
    .await
}

/// A post's display fields as stored (private-posting design §3.1): content,
/// read only behind the witness.
#[derive(Debug, Clone, Default, PartialEq, Eq, sqlx::FromRow)]
pub(super) struct Display {
    pub facets: Option<serde_json::Value>,
    pub embed: Option<serde_json::Value>,
    pub langs: Option<Vec<String>>,
    pub self_labels: Option<serde_json::Value>,
}

/// `schema::display(&rauth)`: the record's facets, embed, langs and
/// self-labels.
///
/// # Errors
///
/// Database errors.
pub(super) async fn display(pool: &PgPool, rauth: &RecordAuthz) -> Result<Option<Display>, sqlx::Error> {
    sqlx::query_as("SELECT facets, embed, langs, self_labels FROM private_posts WHERE uri = $1")
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

/// The account statuses the moderation gate hides
/// (`moderation::status_reason`), as an SQL list.
const HIDING_STATUSES: &str = "('deactivated', 'takendown', 'suspended', 'deleted')";

/// The moderation gate's rule (`moderation::gate_many`) for the post `uri`
/// by `author` (SQL expressions): no `!takedown` or `!hide` label on the
/// post, no `!takedown` on the author, and an author status the gate does
/// not hide (no `actors` row reads as visible).
fn gate_shows(uri: &str, author: &str, status: &str) -> String {
    format!(
        "NOT EXISTS (SELECT 1 FROM labels l WHERE (l.subject = {uri} AND l.val IN ('!takedown', '!hide')) \
                                              OR (l.subject = {author} AND l.val = '!takedown')) \
         AND COALESCE({status}, 'active') NOT IN {HIDING_STATUSES}"
    )
}

/// Rule 4's cap (private-posting design §3.7): the walk examines at most
/// this many rows. Inlined into the SQL text as a literal.
pub(super) const RULE4_MAX_HOPS: u32 = 8;

/// Rule 4's step test `step_ok(s)` (§3.7) for the walk row `s`: the reader
/// could read the step, so it may anchor or be walked past.
/// - (i) no block either way between the reader and the step's author;
/// - (ii) the moderation gate shows the step;
/// - (iii) a step not by the audience's owner is by a writer who passes
///   conditions 2 and 3 under the audience (condition 3 not under `list`
///   or NULL until S9; `following` is `false` until S10).
fn step_ok(owner: &str, mode: &str, members: &str, reader: &str) -> String {
    format!(
        "NOT {reader_author} \
         AND {gate} \
         AND (s.author_did = {owner} OR ( \
               (COALESCE({mode}, 'list') = 'list' OR NOT {author_owner}) \
               AND CASE COALESCE({mode}, 'list') \
                     WHEN 'list' THEN s.author_did = ANY({members}) \
                     WHEN 'followers' THEN EXISTS (SELECT 1 FROM follows f WHERE f.author_did = s.author_did AND f.subject_did = {owner}) \
                     WHEN 'everyone' THEN true \
                     ELSE false END))",
        reader_author = blocked(reader, "s.author_did"),
        gate = gate_shows("s.uri", "s.author_did", "(SELECT x.account_status FROM actors x WHERE x.did = s.author_did)"),
        author_owner = blocked("s.author_did", owner),
    )
}

/// Rule 4's walk (§3.7) as an SQL boolean. Every argument is an SQL
/// expression; the cap is inlined, so every caller's text carries the same literal.
pub(super) fn anchored(parent: &str, audience: &str, owner: &str, mode: &str, members: &str, reader: &str) -> String {
    anchored_with_cap(parent, audience, owner, mode, members, reader, RULE4_MAX_HOPS)
}

/// [`anchored`] with another cap: only the pre-S0 count query uses one
/// other than [`RULE4_MAX_HOPS`], to separate over-cap chains.
fn anchored_with_cap(parent: &str, audience: &str, owner: &str, mode: &str, members: &str, reader: &str, cap: u32) -> String {
    let step = step_ok(owner, mode, members, reader);
    format!(
        "EXISTS ( \
           WITH RECURSIVE walk(uri, parent, author, audience, hop) AS ( \
             SELECT s.uri, s.reply_parent_uri, s.author_did, s.audience_uri, 1 \
             FROM private_posts s WHERE s.uri = {parent} AND {step} \
             UNION ALL \
             SELECT s.uri, s.reply_parent_uri, s.author_did, s.audience_uri, w.hop + 1 \
             FROM walk w JOIN private_posts s ON s.uri = w.parent \
             WHERE w.hop < {cap} AND w.audience = {audience} AND w.author <> {owner} AND {step} \
           ) \
           SELECT 1 FROM walk w WHERE w.audience = {audience} AND w.author = {owner})"
    )
}

/// The oracle's walk statement (snapshot fact 8): `$1` the post's parent,
/// `$2` its audience URI, `$3` the audience's owner, `$4` and `$5` the
/// audience row's `mode` and `members`, `$6` the requester.
pub(super) fn walk_statement() -> String {
    format!("SELECT {}", anchored("$1", "$2", "$3", "$4", "$5", "$6"))
}

/// Whether the post hangs from a post by `owner` under `audience`, as
/// `reader` may see the chain (§3.7, fact 8). With no parent the walk
/// starts nowhere and the answer is `false`.
///
/// # Errors
///
/// Database errors.
pub(super) async fn anchored_for(
    pool: &PgPool,
    parent: Option<&str>,
    audience: &str,
    owner: &str,
    mode: Option<&str>,
    members: &[String],
    reader: &str,
) -> Result<bool, sqlx::Error> {
    sqlx::query_scalar(&walk_statement())
        .bind(parent)
        .bind(audience)
        .bind(owner)
        .bind(mode)
        .bind(members)
        .bind(reader)
        .fetch_one(pool)
        .await
}

/// Whether `did` has an `actors` row with status `active` (condition 4: no
/// row is not active).
///
/// # Errors
///
/// Database errors.
pub(super) async fn account_active(pool: &PgPool, did: &str) -> Result<bool, sqlx::Error> {
    sqlx::query_scalar("SELECT EXISTS (SELECT 1 FROM actors WHERE did = $1 AND account_status = 'active')")
        .bind(did)
        .fetch_one(pool)
        .await
}

/// The §6.9 candidate prefilter, mirroring §6.4's rules for the requester
/// (`$1`), as the audience-modes and private-posting designs amend them:
/// - the moderation gate shows the post (labels and author status);
/// - self-authored; or an audience row is present and its mode admits the
///   requester: `list` (or none) by `members`, `followers` by the
///   requester's stored follow of the audience's owner (who is in its own
///   audience), `everyone` always, and nothing else;
/// - no block between the requester and the author; and, for `followers`
///   and `everyone` under another account's audience, none between that
///   account and the requester or the author (rule 6);
/// - under another account's audience, rule 4 (private-posting §3.7): that
///   account is active, the author may write there, and the post hangs
///   from that account's post within [`RULE4_MAX_HOPS`] rows. The walk is
///   last, so it runs only for rows that pass everything else.
///
/// A candidate filter only: `bind` stays authoritative. Blocked pairs are
/// left out so that posts nobody can read do not fill a page's window.
pub(super) fn prefilter() -> String {
    format!(
        "({gate} AND (p.author_did = $1 OR ( \
           a.uri IS NOT NULL \
           AND CASE COALESCE(a.mode, 'list') \
                 WHEN 'list' THEN $1 = ANY(a.members) \
                 WHEN 'followers' THEN a.owner_did = $1 OR EXISTS ( \
                   SELECT 1 FROM follows f WHERE f.author_did = $1 AND f.subject_did = a.owner_did) \
                 WHEN 'everyone' THEN true \
                 ELSE false END \
           AND NOT {viewer_author} \
           AND (COALESCE(a.mode, 'list') NOT IN ('followers', 'everyone') \
                OR a.owner_did = p.author_did \
                OR NOT ((a.owner_did <> $1 AND {viewer_owner}) OR {author_owner})) \
           AND (a.owner_did = p.author_did \
                OR (ao.account_status = 'active' \
                    AND (COALESCE(a.mode, 'list') IN ('list') OR NOT {author_owner}) \
                    AND CASE COALESCE(a.mode, 'list') \
                          WHEN 'list' THEN p.author_did = ANY(a.members) \
                          WHEN 'followers' THEN EXISTS ( \
                            SELECT 1 FROM follows f WHERE f.author_did = p.author_did AND f.subject_did = a.owner_did) \
                          WHEN 'everyone' THEN true \
                          ELSE false END \
                    AND {walk})))))",
        gate = gate_shows("p.uri", "p.author_did", "po.account_status"),
        viewer_author = blocked("$1", "p.author_did"),
        viewer_owner = blocked("$1", "a.owner_did"),
        author_owner = blocked("p.author_did", "a.owner_did"),
        walk = anchored("p.reply_parent_uri", "p.audience_uri", "a.owner_did", "a.mode", "a.members", "$1"),
    )
}

/// The pre-S0 count (private-posting design §5, "S0's pre-release count"),
/// run read-only against the deployed database before S0 ships. The reader
/// is `''`, so step test (i) never denies. Printed by the ignored test
/// `pre_s0_count_query`.
#[cfg(test)]
pub(super) fn pre_s0_count_sql() -> String {
    pre_s0_count_sql_with_cap(RULE4_MAX_HOPS)
}

/// [`pre_s0_count_sql`] with another walk cap (64 separates over-cap
/// chains from the rest of `stops_unanchored`).
#[cfg(test)]
pub(super) fn pre_s0_count_sql_with_cap(cap: u32) -> String {
    format!(
        "WITH x AS (
  SELECT p.uri, p.author_did, p.audience_uri, p.reply_parent_uri,
         a.owner_did, COALESCE(a.mode, 'list') AS mode, a.members,
         (ao.pds_endpoint IS NOT NULL AND ao.pds_endpoint = po.pds_endpoint
          AND ao.account_status = 'active') AS served_today,
         EXISTS (SELECT 1 FROM members m WHERE m.did = p.author_did) AS writer_member,
         EXISTS (SELECT 1 FROM members m WHERE m.did = a.owner_did) AS owner_member,
         EXISTS (SELECT 1 FROM follows f WHERE f.author_did = p.author_did AND f.subject_did = a.owner_did) AS writer_follows
  FROM private_posts p
  JOIN private_audiences a ON a.uri = p.audience_uri
  LEFT JOIN actors po ON po.did = p.author_did
  LEFT JOIN actors ao ON ao.did = a.owner_did
  WHERE a.owner_did <> p.author_did
), y AS (
  SELECT x.*, {walk} AS anchored
  FROM x
)
SELECT
  count(*) FILTER (WHERE served_today AND NOT anchored)
    AS stops_unanchored,
  count(*) FILTER (WHERE served_today AND anchored AND mode = 'list' AND NOT author_did = ANY(members))
    AS stops_list_writer_unlisted,
  count(*) FILTER (WHERE served_today AND anchored AND mode = 'followers' AND NOT writer_follows AND writer_member AND owner_member)
    AS stops_followers_member_writer_member_owner,
  count(*) FILTER (WHERE served_today AND anchored AND mode = 'followers' AND NOT writer_follows AND writer_member AND NOT owner_member)
    AS stops_followers_member_writer_nonmember_owner,
  count(*) FILTER (WHERE served_today AND anchored AND mode = 'followers' AND NOT writer_follows AND NOT writer_member AND owner_member)
    AS stops_followers_nonmember_writer_member_owner,
  count(*) FILTER (WHERE served_today AND anchored AND mode = 'followers' AND NOT writer_follows AND NOT writer_member AND NOT owner_member)
    AS stops_followers_nonmember_writer_nonmember_owner,
  count(*) FILTER (WHERE anchored AND mode = 'followers' AND NOT writer_follows AND writer_member)
    AS c5c_member_writer_residual,
  (SELECT count(*) FROM private_posts p2 JOIN actors ac ON ac.did = p2.author_did
    WHERE ac.account_status NOT IN ('active', 'deactivated', 'takendown', 'suspended', 'deleted'))
    AS starts_serving_status
FROM y;",
        walk = anchored_with_cap("x.reply_parent_uri", "x.audience_uri", "x.owner_did", "x.mode", "x.members", "''", cap),
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

/// `(held, existing)`: how many private bookmarks `viewer` holds, and
/// whether one of them is `uri` (P-8's cap, C-12: over `private_bookmarks`
/// alone). The viewer's own rows, which say nothing about any post.
///
/// # Errors
///
/// Database errors.
pub(super) async fn bookmarks_held(pool: &PgPool, viewer: &str, uri: &str) -> Result<(i64, bool), sqlx::Error> {
    sqlx::query_as(
        "SELECT count(*), COALESCE(bool_or(post_uri = $2), false) FROM private_bookmarks WHERE viewer_did = $1",
    )
    .bind(viewer)
    .bind(uri)
    .fetch_one(pool)
    .await
}

/// Keep `viewer`'s bookmark of `uri`: a repeat replaces the CID and keeps
/// the bookmark's place.
///
/// # Errors
///
/// Database errors.
pub(super) async fn put_bookmark(pool: &PgPool, viewer: &str, uri: &str, cid: &str) -> Result<(), sqlx::Error> {
    sqlx::query(
        "INSERT INTO private_bookmarks (viewer_did, post_uri, post_cid) VALUES ($1, $2, $3) \
         ON CONFLICT (viewer_did, post_uri) DO UPDATE SET post_cid = EXCLUDED.post_cid",
    )
    .bind(viewer)
    .bind(uri)
    .bind(cid)
    .execute(pool)
    .await?;
    Ok(())
}

/// Remove `viewer`'s bookmark of `uri`, if any.
///
/// # Errors
///
/// Database errors.
pub(super) async fn delete_bookmark(pool: &PgPool, viewer: &str, uri: &str) -> Result<(), sqlx::Error> {
    sqlx::query("DELETE FROM private_bookmarks WHERE viewer_did = $1 AND post_uri = $2")
        .bind(viewer)
        .bind(uri)
        .execute(pool)
        .await?;
    Ok(())
}

/// A bookmark row: `(post_uri, post_cid, created_at)`.
pub(super) type BookmarkRow = (String, String, DateTime<Utc>);

/// Up to `limit` of `viewer`'s bookmarks, newest first, after the
/// `created_at::post_uri` cursor.
///
/// # Errors
///
/// Database errors.
pub(super) async fn bookmark_rows(
    pool: &PgPool,
    viewer: &str,
    after: Option<(DateTime<Utc>, String)>,
    limit: i64,
) -> Result<Vec<BookmarkRow>, sqlx::Error> {
    let (at, uri) = after.map_or((None, String::new()), |(a, u)| (Some(a), u));
    sqlx::query_as(
        "SELECT post_uri, post_cid, created_at FROM private_bookmarks WHERE viewer_did = $1 \
           AND ($2::timestamptz IS NULL OR (created_at, post_uri) < ($2, $3)) \
         ORDER BY created_at DESC, post_uri DESC LIMIT $4",
    )
    .bind(viewer)
    .bind(at)
    .bind(uri)
    .bind(limit)
    .fetch_all(pool)
    .await
}

/// The URIs among `uris` that `viewer` bookmarked (`viewer.bookmarked`).
///
/// # Errors
///
/// Database errors.
pub(super) async fn bookmarked(pool: &PgPool, viewer: &str, uris: &[String]) -> Result<Vec<String>, sqlx::Error> {
    sqlx::query_scalar("SELECT post_uri FROM private_bookmarks WHERE viewer_did = $1 AND post_uri = ANY($2)")
        .bind(viewer)
        .bind(uris)
        .fetch_all(pool)
        .await
}

/// A private post's valid threadgate (private posting §4.4): the gate in
/// the post author's repo with the post's rkey.
#[derive(Debug, Clone, PartialEq, sqlx::FromRow)]
pub(super) struct ThreadgateRow {
    pub post_uri: String,
    pub uri: String,
    pub cid: String,
    pub record: serde_json::Value,
    /// `None`: the field is absent (anyone may reply); `[]`: nobody.
    pub allow: Option<serde_json::Value>,
    pub hidden_replies: Vec<String>,
}

/// A gate counts only when its author is its post's author and its rkey is
/// its post's rkey (`hydration/thread.rs`'s rule), over the gate row `g`.
const VALID_GATE: &str = "g.author_did = split_part(g.post_uri, '/', 3) AND g.rkey = split_part(g.post_uri, '/', 5)";

/// The valid private threadgates of `posts` (non-content: a gate is a stock
/// public record). Callers use a gate only for a root read for the reader
/// in the request (C-17).
///
/// # Errors
///
/// Database errors.
pub(super) async fn threadgates(pool: &PgPool, posts: &[String]) -> Result<Vec<ThreadgateRow>, sqlx::Error> {
    sqlx::query_as(&format!(
        "SELECT g.post_uri, g.uri, g.cid, g.record, g.allow, g.hidden_replies FROM private_threadgates g \
         WHERE g.post_uri = ANY($1) AND {VALID_GATE}"
    ))
    .bind(posts)
    .fetch_all(pool)
    .await
}

/// A private post's valid postgate: whether it turns embedding off
/// (`app.bsky.feed.postgate#disableRule`) and the quoting URIs it detaches.
#[derive(Debug, Clone, PartialEq, Eq, sqlx::FromRow)]
pub(super) struct PostgateRow {
    pub post_uri: String,
    pub embedding_disabled: bool,
    pub detached: Vec<String>,
}

/// The valid private postgates of `posts`.
///
/// # Errors
///
/// Database errors.
pub(super) async fn postgates(pool: &PgPool, posts: &[String]) -> Result<Vec<PostgateRow>, sqlx::Error> {
    sqlx::query_as(&format!(
        "SELECT g.post_uri, \
           COALESCE(g.embedding_rules @> '[{{\"$type\": \"app.bsky.feed.postgate#disableRule\"}}]'::jsonb, false) AS embedding_disabled, \
           g.detached_embedding_uris AS detached \
         FROM private_postgates g WHERE g.post_uri = ANY($1) AND {VALID_GATE}"
    ))
    .bind(posts)
    .fetch_all(pool)
    .await
}

/// Whether `did` is among the DIDs the private post `uri` mentions
/// (`mention_dids`, a structural fact, §3.1): a `mentionRule`'s input, for a
/// root read for the reader in the request.
///
/// # Errors
///
/// Database errors.
pub(super) async fn mentions(pool: &PgPool, uri: &str, did: &str) -> Result<bool, sqlx::Error> {
    sqlx::query_scalar("SELECT EXISTS (SELECT 1 FROM private_posts WHERE uri = $1 AND $2 = ANY(mention_dids))")
        .bind(uri)
        .bind(did)
        .fetch_one(pool)
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
