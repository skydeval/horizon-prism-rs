//! Private-table queries (design-v2 §3.2, §4.5, §6.3, §6.9), `pub(super)`.
//!
//! - **Content** (`encoded`, `plaintext`, `view_meta`, `validation_flags`)
//!   takes `&RecordAuthz` only and reads only its resource: a witness for one
//!   record cannot load another. There is no URI parameter.
//! - **Non-content** queries (oracle facts, candidate prefilters) take DIDs
//!   and URIs and return no private content.

use std::collections::HashMap;

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

/// A follow by `owner` naming `subject` is stored (SQL expressions;
/// following-audience design §5.1).
fn owner_follows(owner: &str, subject: &str) -> String {
    format!("EXISTS (SELECT 1 FROM follows fo WHERE fo.author_did = {owner} AND fo.subject_did = {subject})")
}

/// The follow-verification flag holds for `owner` (SQL expression;
/// following-audience design §3.1, §5.1): verified, and the firehose has
/// applied its commits through the verified rev. The three columns come
/// from one `sync_state` row inside one `EXISTS`, so they are read together;
/// no row, or a NULL column, reads as not. The one builder the oracle, the
/// prefilter and the walk's step test use.
fn follows_verified(owner: &str) -> String {
    format!(
        "EXISTS (SELECT 1 FROM sync_state fv WHERE fv.did = {owner} \
           AND fv.follows_verified_at IS NOT NULL AND fv.firehose_rev >= fv.follows_verified_rev)"
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
///   conditions 2 and 3 under the audience (condition 3 under every mode
///   since S9, C-6; `following` since S10: the owner's stored follow of
///   the step's author, and the owner's flag).
fn step_ok(owner: &str, mode: &str, members: &str, reader: &str) -> String {
    format!(
        "NOT {reader_author} \
         AND {gate} \
         AND (s.author_did = {owner} OR ( \
               NOT {author_owner} \
               AND CASE COALESCE({mode}, 'list') \
                     WHEN 'list' THEN s.author_did = ANY({members}) \
                     WHEN 'followers' THEN EXISTS (SELECT 1 FROM follows f WHERE f.author_did = s.author_did AND f.subject_did = {owner}) \
                     WHEN 'everyone' THEN true \
                     WHEN 'following' THEN {owner_follows_author} AND {verified} \
                     ELSE false END))",
        reader_author = blocked(reader, "s.author_did"),
        gate = gate_shows("s.uri", "s.author_did", "(SELECT x.account_status FROM actors x WHERE x.did = s.author_did)"),
        author_owner = blocked("s.author_did", owner),
        owner_follows_author = owner_follows(owner, "s.author_did"),
        verified = follows_verified(owner),
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
///   audience), `everyone` always, `following` by the owner's stored
///   follow of the requester with the owner's flag holding (S10; the owner
///   is in its own audience), and nothing else;
/// - no block between the requester and the author; and, under another
///   account's audience in any mode (S9, C-6), none between that account
///   and the requester or the author (rule 6);
/// - under another account's audience, rule 4 (private-posting §3.7): that
///   account is active, the author may write there and is not blocked
///   either way with that account (condition 3), and the post hangs
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
                 WHEN 'following' THEN a.owner_did = $1 OR ({owner_follows_viewer} AND {owner_verified}) \
                 ELSE false END \
           AND NOT {viewer_author} \
           AND (a.owner_did = p.author_did \
                OR NOT ((a.owner_did <> $1 AND {viewer_owner}) OR {author_owner})) \
           AND (a.owner_did = p.author_did \
                OR (ao.account_status = 'active' \
                    AND NOT {author_owner} \
                    AND CASE COALESCE(a.mode, 'list') \
                          WHEN 'list' THEN p.author_did = ANY(a.members) \
                          WHEN 'followers' THEN EXISTS ( \
                            SELECT 1 FROM follows f WHERE f.author_did = p.author_did AND f.subject_did = a.owner_did) \
                          WHEN 'everyone' THEN true \
                          WHEN 'following' THEN {owner_follows_author} AND {owner_verified} \
                          ELSE false END \
                    AND {walk})))))",
        gate = gate_shows("p.uri", "p.author_did", "po.account_status"),
        viewer_author = blocked("$1", "p.author_did"),
        viewer_owner = blocked("$1", "a.owner_did"),
        author_owner = blocked("p.author_did", "a.owner_did"),
        owner_follows_viewer = owner_follows("a.owner_did", "$1"),
        owner_follows_author = owner_follows("a.owner_did", "p.author_did"),
        owner_verified = follows_verified("a.owner_did"),
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

/// [`anchored`] as it stood before S9, for a `list` or NULL audience only:
/// there, S8's `step_ok` (iii) had no condition 3, so a step by a listed
/// writer blocked either way with the owner still passed.
#[cfg(test)]
fn anchored_before_s9_list(parent: &str, audience: &str, owner: &str, mode: &str, members: &str, reader: &str) -> String {
    let condition3 = format!("NOT {} AND CASE", blocked("s.author_did", owner));
    let walk = anchored(parent, audience, owner, mode, members, reader);
    assert_eq!(walk.matches(&condition3).count(), 2, "condition 3 once in each step test");
    walk.replace(&condition3, "CASE")
}

/// The pre-S9 count (private-posting design §5 S9, C-6), run read-only
/// against the deployed database before S9 ships: the cross-owner posts
/// under `list` or NULL audiences that S8 serves and S9 stops serving.
/// Each kind is a post S8 serves to the audience (listed writer, active
/// owner, anchored by S8's walk with reader `''`):
/// - `stops_author_owner_block`: its author and the audience's owner block
///   each other either way; read by its author only after S9;
/// - `stops_blocked_step`: its author is not blocked, but its chain passes
///   through a step by a writer who is, so S9's walk no longer anchors it;
/// - `stops_reader_pairs` and `stops_reader_posts`: (post, listed reader)
///   pairs, and their posts, where the reader and the owner block each
///   other and S8's walk anchored the post for that reader. Expected 0:
///   since S0 the owner's own post fails step test (i) for such a reader.
///
/// S9 starts serving nothing. Printed by the ignored test
/// `pre_s9_count_query`.
#[cfg(test)]
pub(super) fn pre_s9_count_sql() -> String {
    format!(
        "WITH x AS (
  SELECT p.uri, p.author_did, p.audience_uri, p.reply_parent_uri,
         a.owner_did, COALESCE(a.mode, 'list') AS mode, a.members,
         {author_owner} AS author_owner_blocked
  FROM private_posts p
  JOIN private_audiences a ON a.uri = p.audience_uri
  JOIN actors ao ON ao.did = a.owner_did AND ao.account_status = 'active'
  WHERE a.owner_did <> p.author_did
    AND COALESCE(a.mode, 'list') = 'list'
    AND p.author_did = ANY(a.members)
), y AS (
  SELECT x.*, {before} AS served_s8, {after} AS anchored_s9
  FROM x
), r AS (
  SELECT y.uri, m.reader
  FROM y CROSS JOIN LATERAL unnest(y.members) AS m(reader)
  WHERE y.served_s8 AND y.anchored_s9 AND NOT y.author_owner_blocked
    AND m.reader <> y.owner_did AND m.reader <> y.author_did
    AND {reader_owner}
    AND {before_for_reader}
)
SELECT
  count(*) FILTER (WHERE served_s8 AND author_owner_blocked)
    AS stops_author_owner_block,
  count(*) FILTER (WHERE served_s8 AND NOT author_owner_blocked AND NOT anchored_s9)
    AS stops_blocked_step,
  (SELECT count(*) FROM r) AS stops_reader_pairs,
  (SELECT count(DISTINCT uri) FROM r) AS stops_reader_posts
FROM y;",
        author_owner = blocked("p.author_did", "a.owner_did"),
        before = anchored_before_s9_list("x.reply_parent_uri", "x.audience_uri", "x.owner_did", "x.mode", "x.members", "''"),
        after = anchored("x.reply_parent_uri", "x.audience_uri", "x.owner_did", "x.mode", "x.members", "''"),
        reader_owner = blocked("m.reader", "y.owner_did"),
        before_for_reader =
            anchored_before_s9_list("y.reply_parent_uri", "y.audience_uri", "y.owner_did", "y.mode", "y.members", "m.reader"),
    )
}

/// Facts 10–12 (following-audience design §5.4): (the audience's owner
/// `$1` follows the requester `$2` and its flag holds, the owner follows
/// the post's owner `$3` and its flag holds), in one statement so that both
/// pairs come from one snapshot. No committed snapshot holds a stale row
/// beside a flag that holds (invariant F).
pub(super) fn following_facts_statement() -> String {
    format!(
        "SELECT {v} AND {fv}, {a} AND {fv}",
        v = owner_follows("$1", "$2"),
        a = owner_follows("$1", "$3"),
        fv = follows_verified("$1"),
    )
}

/// [`following_facts_statement`] for `owner`, `requester` and the post's
/// owner `author`.
///
/// # Errors
///
/// Database errors.
pub(super) async fn following_facts(pool: &PgPool, owner: &str, requester: &str, author: &str) -> Result<(bool, bool), sqlx::Error> {
    sqlx::query_as(&following_facts_statement())
        .bind(owner)
        .bind(requester)
        .bind(author)
        .fetch_one(pool)
        .await
}

/// [`anchored`] before migration 0028 (following-audience design §10): the
/// flag's columns do not exist yet, so `follows_verified(owner)` reads as
/// `true`, an upper bound that assumes every owner's flag gets set. It
/// appears once in each step test.
#[cfg(test)]
fn anchored_assuming_verified(parent: &str, audience: &str, owner: &str, mode: &str, members: &str, reader: &str) -> String {
    let verified = follows_verified(owner);
    let walk = anchored(parent, audience, owner, mode, members, reader);
    assert_eq!(walk.matches(&verified).count(), 2, "the flag once in each step test");
    walk.replace(&verified, "true")
}

/// The pre-S10 count (following-audience design §10), run read-only against
/// the deployed database before migration 0028. S10 starts serving and
/// stops nothing; each column is an upper bound on what starts:
/// - `following_audiences`, `following_owners` (members, and eligible
///   non-members);
/// - `owners_10k_stored_follows`: owners with at least 10 000 stored
///   follows, a lower bound on those near or over the pass's ceiling;
/// - `own_posts` and `cross_owner_posts` under a `following` audience;
/// - `cross_owner_posts_starting`: cross-owner posts whose writer the owner
///   follows, with no block either way, anchored by the walk;
/// - `own_post_reader_pairs_starting`: (owner's post, ready member the
///   owner follows) pairs with no block either way.
///
/// Printed by the ignored test `pre_s10_count_query`.
#[cfg(test)]
pub(super) fn pre_s10_count_sql() -> String {
    format!(
        "WITH a AS (
  SELECT uri, owner_did FROM private_audiences WHERE mode = 'following'
), o AS (
  SELECT DISTINCT owner_did FROM a
), x AS (
  SELECT p.uri, p.author_did, p.reply_parent_uri, p.audience_uri, a.owner_did
  FROM private_posts p JOIN a ON a.uri = p.audience_uri
)
SELECT
  (SELECT count(*) FROM a) AS following_audiences,
  (SELECT count(*) FROM o) AS following_owners,
  (SELECT count(*) FROM o WHERE EXISTS (SELECT 1 FROM members m WHERE m.did = o.owner_did)) AS following_owners_members,
  (SELECT count(*) FROM o WHERE NOT EXISTS (SELECT 1 FROM members m WHERE m.did = o.owner_did)
                            AND EXISTS (SELECT 1 FROM private_owner_eligible e WHERE e.did = o.owner_did)) AS following_owners_nonmembers,
  (SELECT count(*) FROM o WHERE (SELECT count(*) FROM follows f WHERE f.author_did = o.owner_did) >= 10000)
    AS owners_10k_stored_follows,
  (SELECT count(*) FROM x WHERE x.author_did = x.owner_did) AS own_posts,
  (SELECT count(*) FROM x WHERE x.author_did <> x.owner_did) AS cross_owner_posts,
  (SELECT count(*) FROM x WHERE x.author_did <> x.owner_did
      AND {writer_followed}
      AND NOT {writer_blocked}
      AND {walk}) AS cross_owner_posts_starting,
  (SELECT count(*) FROM x JOIN members m ON m.ready_at IS NOT NULL AND m.did <> x.owner_did
     WHERE x.author_did = x.owner_did AND {reader_followed}
       AND NOT {reader_blocked}) AS own_post_reader_pairs_starting;",
        writer_followed = owner_follows("x.owner_did", "x.author_did"),
        writer_blocked = blocked("x.author_did", "x.owner_did"),
        walk = anchored_assuming_verified("x.reply_parent_uri", "x.audience_uri", "x.owner_did", "'following'", "'{}'::text[]", "''"),
        reader_followed = owner_follows("x.owner_did", "m.did"),
        reader_blocked = blocked("m.did", "x.owner_did"),
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

/// `getReplies`'s candidates (private posting §4.12): up to `limit` private
/// replies to `parent` passing the prefilter, except `hidden`, oldest first
/// by `(created_at, uri)`, after the position `after`.
///
/// # Errors
///
/// Database errors.
pub(super) async fn reply_page_candidates(
    pool: &PgPool,
    requester: &str,
    parent: &str,
    hidden: &[String],
    after: Option<(DateTime<Utc>, String)>,
    limit: i64,
) -> Result<Vec<(String, DateTime<Utc>)>, sqlx::Error> {
    let (at, uri) = after.map_or((None, String::new()), |(a, u)| (Some(a), u));
    sqlx::query_as(&format!(
        "SELECT p.uri, p.created_at FROM {PREFILTER_FROM} \
         WHERE p.reply_parent_uri = $2 AND NOT (p.uri = ANY($3)) AND {pre} \
           AND ($4::timestamptz IS NULL OR (p.created_at, p.uri COLLATE \"C\") > ($4, $5 COLLATE \"C\")) \
         ORDER BY p.created_at, p.uri COLLATE \"C\" LIMIT $6",
        pre = prefilter()
    ))
    .bind(requester)
    .bind(parent)
    .bind(hidden)
    .bind(at)
    .bind(uri)
    .bind(limit)
    .fetch_all(pool)
    .await
}

/// `searchPosts`'s candidates (private posting §4.17): up to `limit` posts
/// passing the prefilter for `requester`, newest first by
/// `(created_at, uri)`, strictly below the pair `(at, uri)` and no older
/// than `lower`. The order is `private_posts_recent`'s, with the column's
/// own collation, so the index serves the scan. Rows that fail the
/// prefilter are read here and skipped; they never count toward `limit`.
///
/// # Errors
///
/// Database errors.
pub(super) async fn search_candidates(
    pool: &PgPool,
    requester: &str,
    at: DateTime<Utc>,
    uri: &str,
    lower: DateTime<Utc>,
    limit: i64,
) -> Result<Vec<(String, DateTime<Utc>)>, sqlx::Error> {
    sqlx::query_as(&format!(
        "SELECT p.uri, p.created_at FROM {PREFILTER_FROM} \
         WHERE (p.created_at, p.uri) < ($2, $3) AND p.created_at >= $4 AND {pre} \
         ORDER BY p.created_at DESC, p.uri DESC LIMIT $5",
        pre = prefilter()
    ))
    .bind(requester)
    .bind(at)
    .bind(uri)
    .bind(lower)
    .bind(limit)
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

/// A feed candidate (private posting §4.7): the private post to read, its
/// sort time and tie key, and for a repost item the repost's URI and its
/// author. A post's own item sorts by the post's `created_at` and its key
/// (the rkey on an author feed, the URI on the timeline); a repost item by
/// the repost's `created_at` and the repost's URI.
#[derive(Debug, Clone, PartialEq, Eq, sqlx::FromRow)]
pub(super) struct FeedCandidate {
    pub uri: String,
    pub created_at: DateTime<Utc>,
    pub key: String,
    pub repost_uri: Option<String>,
    pub reposter: Option<String>,
}

/// The repost items' rows: a repost of a stored private post, with the
/// post's prefilter joins. The `subject_uri` test matches the partial index
/// `reposts_private_subject`.
const REPOST_FROM: &str = "reposts rp \
     JOIN private_posts p ON p.uri = rp.subject_uri \
     LEFT JOIN private_audiences a ON a.uri = p.audience_uri \
     LEFT JOIN actors po ON po.did = p.author_did \
     LEFT JOIN actors ao ON ao.did = a.owner_did";

/// The conditions on a repost item's repost `rp` for the requester `$1`
/// (private posting §4.7, C-2): it reposts a `postPrivate` URI, its author
/// is a member, and no block stands between that author and the requester.
fn repost_item() -> String {
    format!(
        "rp.subject_uri LIKE 'at://%/tools.kryphocron.feed.postPrivate/%' \
         AND EXISTS (SELECT 1 FROM members m WHERE m.did = rp.author_did) \
         AND NOT {blocked}",
        blocked = blocked("$1", "rp.author_did")
    )
}

/// `$1` does not fully mute `did` (an SQL expression), directly or through
/// a moderation list that applies, as on the public timeline.
fn not_muted(did: &str) -> String {
    format!(
        "NOT EXISTS (SELECT 1 FROM mutes m WHERE m.viewer_did = $1 AND m.subject_did = {did} AND m.scope = 'full') \
         AND NOT EXISTS (SELECT 1 FROM list_references r \
                         JOIN subscribed_lists l ON l.list_uri = r.list_uri \
                         JOIN subscribed_list_items i ON i.list_uri = r.list_uri \
                         WHERE r.member_did = $1 AND i.subject_did = {did} AND {applies})",
        applies = crate::hydration::APPLIES,
    )
}

/// The actor's posts passing the prefilter, and the actor's reposts of
/// private posts that pass it (private posting §4.7) when the actor is a
/// member with no block either way with the requester, newest first, after
/// the position `(created_at, key)`.
///
/// With `own_threads` (P-20's `posts_and_author_threads`, §4.14) the
/// actor's own posts are its top-level posts and its replies in threads
/// whose **claimed** root is the actor's (C-18); repost items are kept
/// under both filters, as on the stock feed.
///
/// # Errors
///
/// Database errors.
pub(super) async fn author_candidates(
    pool: &PgPool,
    requester: &str,
    author: &str,
    own_threads: bool,
    after: Option<(DateTime<Utc>, String)>,
    limit: i64,
) -> Result<Vec<FeedCandidate>, sqlx::Error> {
    let (at, key) = after.map_or((None, String::new()), |(a, k)| (Some(a), k));
    let threads = if own_threads {
        "AND (p.reply_parent_uri IS NULL OR split_part(p.reply_root_uri, '/', 3) = $2)"
    } else {
        ""
    };
    sqlx::query_as(&format!(
        "(SELECT p.uri, p.created_at, p.rkey COLLATE \"C\" AS key, NULL::text AS repost_uri, NULL::text AS reposter \
          FROM {PREFILTER_FROM} \
          WHERE p.author_did = $2 {threads} AND {pre} \
            AND ($3::timestamptz IS NULL OR (p.created_at, p.rkey COLLATE \"C\") < ($3, $4 COLLATE \"C\")) \
          ORDER BY p.created_at DESC, p.rkey COLLATE \"C\" DESC LIMIT $5) \
         UNION ALL \
         (SELECT p.uri, rp.created_at, rp.uri COLLATE \"C\", rp.uri, rp.author_did \
          FROM {REPOST_FROM} \
          WHERE rp.author_did = $2 AND {repost} AND {pre} \
            AND ($3::timestamptz IS NULL OR (rp.created_at, rp.uri COLLATE \"C\") < ($3, $4 COLLATE \"C\")) \
          ORDER BY rp.created_at DESC, rp.uri COLLATE \"C\" DESC LIMIT $5) \
         ORDER BY created_at DESC, key DESC LIMIT $5",
        pre = prefilter(),
        repost = repost_item(),
    ))
    .bind(requester)
    .bind(author)
    .bind(at)
    .bind(key)
    .bind(limit)
    .fetch_all(pool)
    .await
}

/// The home timeline's candidates (design-v3 §1.7 item 12; private posting
/// §4.7), newest first, after the `created_at::key` cursor:
/// - posts passing the prefilter whose author is the requester or an
///   account the requester follows, as this AppView has indexed the
///   follows, keyed by URI (across authors a record key is not unique);
/// - repost items: reposts by the requester or an account it follows, a
///   member (C-2), of a private post passing the prefilter, keyed by the
///   repost's URI.
///
/// A mute applies here as it does on the public timeline: a post's author,
/// or a reposter, that the requester fully mutes, directly or through a
/// moderation list that applies, is left out. So is a reposter with a block
/// either way.
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
    let (at, key) = after.map_or((None, String::new()), |(a, k)| (Some(a), k));
    sqlx::query_as(&format!(
        "(SELECT p.uri, p.created_at, p.uri COLLATE \"C\" AS key, NULL::text AS repost_uri, NULL::text AS reposter \
          FROM {PREFILTER_FROM} \
          WHERE (p.author_did = $1 OR p.author_did IN (SELECT f.subject_did FROM follows f WHERE f.author_did = $1)) \
            AND {pre} AND {post_muted} \
            AND ($2::timestamptz IS NULL OR (p.created_at, p.uri COLLATE \"C\") < ($2, $3 COLLATE \"C\")) \
          ORDER BY p.created_at DESC, p.uri COLLATE \"C\" DESC LIMIT $4) \
         UNION ALL \
         (SELECT p.uri, rp.created_at, rp.uri COLLATE \"C\", rp.uri, rp.author_did \
          FROM {REPOST_FROM} \
          WHERE (rp.author_did = $1 OR rp.author_did IN (SELECT f.subject_did FROM follows f WHERE f.author_did = $1)) \
            AND {repost} AND {reposter_muted} AND {pre} \
            AND ($2::timestamptz IS NULL OR (rp.created_at, rp.uri COLLATE \"C\") < ($2, $3 COLLATE \"C\")) \
          ORDER BY rp.created_at DESC, rp.uri COLLATE \"C\" DESC LIMIT $4) \
         ORDER BY created_at DESC, key DESC LIMIT $4",
        pre = prefilter(),
        post_muted = not_muted("p.author_did"),
        repost = repost_item(),
        reposter_muted = not_muted("rp.author_did"),
    ))
    .bind(requester)
    .bind(at)
    .bind(key)
    .bind(limit)
    .fetch_all(pool)
    .await
}

/// The counts on a private post view (private posting §3.4, P-2): each
/// counts only what the reader is entitled to know exists.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(super) struct Counts {
    pub reply: i64,
    pub repost: i64,
    pub like: i64,
    pub quote: i64,
}

/// A like or a repost: the public interaction tables the private views read
/// (by member likers only, C-2).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum Interaction {
    Like,
    Repost,
}

impl Interaction {
    fn table(self) -> &'static str {
        match self {
            Self::Like => "likes",
            Self::Repost => "reposts",
        }
    }
}

/// An interaction row `l` that counts for the reader `$1`: by a member,
/// with no block either way with the reader (§3.4, C-2).
fn counted_interaction() -> String {
    format!(
        "EXISTS (SELECT 1 FROM members m WHERE m.did = l.author_did) AND NOT {blocked}",
        blocked = blocked("$1", "l.author_did")
    )
}

/// A quote reference `q` that counts for the reader `$1`: no block either
/// way between its author and the reader (§3.4).
fn counted_quote_ref() -> String {
    format!("NOT {}", blocked("$1", "q.author_did"))
}

/// The counts of `uris` for `reader`, one query per kind (§3.4). Every join
/// is on a URI (§3.5), so a post's counts survive a CID change.
/// - `replyCount`: private replies passing the prefilter, which carries the
///   moderation test; a reply that fails decode or waits on its owner's
///   readiness counts, since that fails for every reader alike.
/// - `likeCount`, `repostCount`: by member likers with no block either way.
/// - `quoteCount`: private quotes passing the prefilter, and members'
///   public quotes whose author has no block either way.
///
/// # Errors
///
/// Database errors.
pub(super) async fn counts(pool: &PgPool, reader: &str, uris: &[String]) -> Result<HashMap<String, Counts>, sqlx::Error> {
    let mut out: HashMap<String, Counts> = HashMap::new();
    if uris.is_empty() {
        return Ok(out);
    }
    let replies: Vec<(String, i64)> = sqlx::query_as(&format!(
        "SELECT p.reply_parent_uri, count(*) FROM {PREFILTER_FROM} \
         WHERE p.reply_parent_uri = ANY($2) AND {pre} GROUP BY p.reply_parent_uri",
        pre = prefilter()
    ))
    .bind(reader)
    .bind(uris)
    .fetch_all(pool)
    .await?;
    for (uri, n) in replies {
        out.entry(uri).or_default().reply = n;
    }
    for kind in [Interaction::Like, Interaction::Repost] {
        let rows: Vec<(String, i64)> = sqlx::query_as(&format!(
            "SELECT l.subject_uri, count(*) FROM {table} l WHERE l.subject_uri = ANY($2) AND {counted} GROUP BY l.subject_uri",
            table = kind.table(),
            counted = counted_interaction(),
        ))
        .bind(reader)
        .bind(uris)
        .fetch_all(pool)
        .await?;
        for (uri, n) in rows {
            let c = out.entry(uri).or_default();
            match kind {
                Interaction::Like => c.like = n,
                Interaction::Repost => c.repost = n,
            }
        }
    }
    let quotes: Vec<(String, i64)> = sqlx::query_as(&format!(
        "SELECT quoted, count(*) FROM ( \
           SELECT p.quote_uri AS quoted FROM {PREFILTER_FROM} WHERE p.quote_uri = ANY($2) AND {pre} \
           UNION ALL \
           SELECT q.quoted_uri FROM private_quote_refs q WHERE q.quoted_uri = ANY($2) AND {counted} \
         ) x GROUP BY quoted",
        pre = prefilter(),
        counted = counted_quote_ref(),
    ))
    .bind(reader)
    .bind(uris)
    .fetch_all(pool)
    .await?;
    for (uri, n) in quotes {
        out.entry(uri).or_default().quote = n;
    }
    Ok(out)
}

/// The URI of `viewer`'s own like or repost of each of `uris`, the newest
/// when there are several (`viewer.like`, `viewer.repost`, §4.6). The
/// viewer is a member, so its likes are stored in every mode.
///
/// # Errors
///
/// Database errors.
pub(super) async fn own_interactions(
    pool: &PgPool,
    viewer: &str,
    kind: Interaction,
    uris: &[String],
) -> Result<HashMap<String, String>, sqlx::Error> {
    let rows: Vec<(String, String)> = sqlx::query_as(&format!(
        "SELECT DISTINCT ON (subject_uri) subject_uri, uri FROM {table} \
         WHERE author_did = $1 AND subject_uri = ANY($2) \
         ORDER BY subject_uri, created_at DESC, uri DESC",
        table = kind.table()
    ))
    .bind(viewer)
    .bind(uris)
    .fetch_all(pool)
    .await?;
    Ok(rows.into_iter().collect())
}

/// The thread roots among `roots` that `viewer` muted (`thread_mutes`, the
/// viewer's own rows; §4.6, §4.11).
///
/// # Errors
///
/// Database errors.
pub(super) async fn muted_roots(pool: &PgPool, viewer: &str, roots: &[String]) -> Result<Vec<String>, sqlx::Error> {
    sqlx::query_scalar("SELECT root_uri FROM thread_mutes WHERE viewer_did = $1 AND root_uri = ANY($2)")
        .bind(viewer)
        .bind(roots)
        .fetch_all(pool)
        .await
}

/// A liker or reposter of a readable private post: `(author, created_at,
/// indexed_at, uri)`.
pub(super) type InteractionRow = (String, DateTime<Utc>, DateTime<Utc>, String);

/// `getLikes` and `getRepostedBy` (private posting §4.7): the rows on
/// `subject` that count for `reader` (as [`counts`] counts them), newest
/// first, after the plain `created_at::uri` cursor. They are public records
/// of members; the caller has read `subject`.
///
/// # Errors
///
/// Database errors.
pub(super) async fn interactions(
    pool: &PgPool,
    reader: &str,
    kind: Interaction,
    subject: &str,
    after: Option<(DateTime<Utc>, String)>,
    limit: i64,
) -> Result<Vec<InteractionRow>, sqlx::Error> {
    let (at, uri) = after.map_or((None, String::new()), |(a, u)| (Some(a), u));
    sqlx::query_as(&format!(
        "SELECT l.author_did, l.created_at, l.indexed_at, l.uri FROM {table} l \
         WHERE l.subject_uri = $2 AND {counted} \
           AND ($3::timestamptz IS NULL OR (l.created_at, l.uri) < ($3, $4)) \
         ORDER BY l.created_at DESC, l.uri DESC LIMIT $5",
        table = kind.table(),
        counted = counted_interaction(),
    ))
    .bind(reader)
    .bind(subject)
    .bind(at)
    .bind(uri)
    .bind(limit)
    .fetch_all(pool)
    .await
}

/// A `getQuotes` candidate (private posting §4.7): a private quoting post
/// to read (`cid` `None`), or a member's public quoting post, emitted as a
/// reference (`cid` its CID).
#[derive(Debug, Clone, PartialEq, Eq, sqlx::FromRow)]
pub(super) struct QuoteCandidate {
    pub uri: String,
    pub created_at: DateTime<Utc>,
    pub cid: Option<String>,
}

/// The posts quoting `quoted`, newest first, merged by `(created_at, uri)`
/// after the cursor: private posts passing the prefilter, and public quote
/// references whose author has no block either way with `reader`.
///
/// # Errors
///
/// Database errors.
pub(super) async fn quote_candidates(
    pool: &PgPool,
    reader: &str,
    quoted: &str,
    after: Option<(DateTime<Utc>, String)>,
    limit: i64,
) -> Result<Vec<QuoteCandidate>, sqlx::Error> {
    let (at, uri) = after.map_or((None, String::new()), |(a, u)| (Some(a), u));
    sqlx::query_as(&format!(
        "(SELECT p.uri COLLATE \"C\" AS uri, p.created_at, NULL::text AS cid FROM {PREFILTER_FROM} \
          WHERE p.quote_uri = $2 AND {pre} \
            AND ($3::timestamptz IS NULL OR (p.created_at, p.uri COLLATE \"C\") < ($3, $4 COLLATE \"C\")) \
          ORDER BY p.created_at DESC, p.uri COLLATE \"C\" DESC LIMIT $5) \
         UNION ALL \
         (SELECT q.uri COLLATE \"C\", q.created_at, q.cid FROM private_quote_refs q \
          WHERE q.quoted_uri = $2 AND {counted} \
            AND ($3::timestamptz IS NULL OR (q.created_at, q.uri COLLATE \"C\") < ($3, $4 COLLATE \"C\")) \
          ORDER BY q.created_at DESC, q.uri COLLATE \"C\" DESC LIMIT $5) \
         ORDER BY created_at DESC, uri DESC LIMIT $5",
        pre = prefilter(),
        counted = counted_quote_ref(),
    ))
    .bind(reader)
    .bind(quoted)
    .bind(at)
    .bind(uri)
    .bind(limit)
    .fetch_all(pool)
    .await
}

/// A private notification row (private posting §4.8): the caller's own.
#[derive(Debug, Clone, PartialEq, sqlx::FromRow)]
pub(super) struct NotificationRow {
    pub id: i64,
    pub author_did: String,
    pub reason: String,
    pub reason_subject: Option<String>,
    pub record_uri: String,
    pub record_cid: String,
    /// A public post's record; `None` for a private post, a like or a repost.
    pub public_record: Option<serde_json::Value>,
    pub indexed_at: DateTime<Utc>,
}

impl NotificationRow {
    /// The row's record is a private post (`reply`, `quote`, `mention`),
    /// read through the pipeline; any other is a member's public record.
    pub(super) fn is_from_private_post(&self) -> bool {
        self.record_uri.split('/').nth(3) == Some(crate::firehose::classifier::NSID_POST_PRIVATE)
    }
}

/// Up to `limit` of `recipient`'s private notifications, newest first, after
/// the plain `indexed_at::id` cursor, with the `reasons` filter (none: every
/// reason). The scan applies none of the recipient's other state, as the
/// public scan does (`hydration/notifications.rs`).
///
/// # Errors
///
/// Database errors.
pub(super) async fn notification_rows(
    pool: &PgPool,
    recipient: &str,
    after: Option<(DateTime<Utc>, i64)>,
    reasons: &[String],
    limit: i64,
) -> Result<Vec<NotificationRow>, sqlx::Error> {
    let (at, id) = after.map_or((None, 0), |(a, i)| (Some(a), i));
    sqlx::query_as(
        "SELECT n.id, n.author_did, n.reason, n.reason_subject, n.record_uri, n.record_cid, n.public_record, n.indexed_at \
         FROM private_notifications n WHERE n.recipient_did = $1 \
           AND ($2::timestamptz IS NULL OR (n.indexed_at, n.id) < ($2, $3)) \
           AND (cardinality($5::text[]) = 0 OR n.reason = ANY($5)) \
         ORDER BY n.indexed_at DESC, n.id DESC LIMIT $4",
    )
    .bind(recipient)
    .bind(at)
    .bind(id)
    .bind(limit)
    .bind(reasons)
    .fetch_all(pool)
    .await
}

/// What a private notification `n` of the recipient `$1` must pass (§4.8,
/// "Reading", step 2), in the list and the count alike:
/// - no block either way with its author, in `blocks` or `private_blocks`;
/// - no full mute of its author, and no scoped mute of its reason (a repost
///   mute: `repost`; a quote mute: `quote`);
/// - no moderation list of the recipient's that applies names its author;
/// - no muted thread: the record, the root it claims, the subject, or the
///   subject's own claimed root is a muted root.
fn notification_kept() -> String {
    format!(
        "NOT {blocked} \
         AND NOT EXISTS (SELECT 1 FROM mutes m WHERE m.viewer_did = $1 AND m.subject_did = n.author_did \
               AND (m.scope = 'full' \
                 OR (m.scope IN ('reposts', 'reposts_and_quotes') AND n.reason = 'repost') \
                 OR (m.scope IN ('quotes', 'reposts_and_quotes') AND n.reason = 'quote'))) \
         AND NOT EXISTS (SELECT 1 FROM list_references r \
                         JOIN subscribed_lists l ON l.list_uri = r.list_uri \
                         JOIN subscribed_list_items i ON i.list_uri = r.list_uri \
                         WHERE r.member_did = $1 AND i.subject_did = n.author_did AND {applies}) \
         AND NOT EXISTS (SELECT 1 FROM thread_mutes t WHERE t.viewer_did = $1 AND t.root_uri IN ( \
               n.record_uri, n.record_root, n.reason_subject, \
               (SELECT s.reply_root_uri FROM private_posts s WHERE s.uri = n.reason_subject), \
               (SELECT s.reply_root_uri FROM posts s WHERE s.uri = n.reason_subject)))",
        blocked = blocked("$1", "n.author_did"),
        applies = crate::hydration::APPLIES,
    )
}

/// The ids among `ids`, rows of `recipient`, that pass [`notification_kept`].
///
/// # Errors
///
/// Database errors.
pub(super) async fn notifications_kept(pool: &PgPool, recipient: &str, ids: &[i64]) -> Result<Vec<i64>, sqlx::Error> {
    sqlx::query_scalar(&format!(
        "SELECT n.id FROM private_notifications n WHERE n.recipient_did = $1 AND n.id = ANY($2) AND {kept}",
        kept = notification_kept()
    ))
    .bind(recipient)
    .bind(ids)
    .fetch_all(pool)
    .await
}

/// The URIs among `uris` that name a stored private post: a notification
/// from a public record is listed and counted only while its subject is
/// one (§4.8, step 4).
///
/// # Errors
///
/// Database errors.
pub(super) async fn stored_posts(pool: &PgPool, uris: &[String]) -> Result<Vec<String>, sqlx::Error> {
    sqlx::query_scalar("SELECT uri FROM private_posts WHERE uri = ANY($1)")
        .bind(uris)
        .fetch_all(pool)
        .await
}

/// A like or repost row as a notification's record is rebuilt from it:
/// `(uri, subject_uri, subject_cid, created_at)`.
pub(super) type InteractionRecordRow = (String, String, Option<String>, DateTime<Utc>);

/// The like or repost rows among `uris` (public records).
///
/// # Errors
///
/// Database errors.
pub(super) async fn interaction_records(
    pool: &PgPool,
    kind: Interaction,
    uris: &[String],
) -> Result<Vec<InteractionRecordRow>, sqlx::Error> {
    sqlx::query_as(&format!(
        "SELECT uri, subject_uri, subject_cid, created_at FROM {table} WHERE uri = ANY($1)",
        table = kind.table()
    ))
    .bind(uris)
    .fetch_all(pool)
    .await
}

/// When `did` last saw its private notifications.
///
/// # Errors
///
/// Database errors.
pub(super) async fn notification_seen(pool: &PgPool, did: &str) -> Result<Option<DateTime<Utc>>, sqlx::Error> {
    sqlx::query_scalar("SELECT seen_at FROM private_notification_seen WHERE did = $1")
        .bind(did)
        .fetch_optional(pool)
        .await
}

/// `updateSeen`: `seen_at = GREATEST(existing, seen_at)`, clamped to now, as
/// the public method does.
///
/// # Errors
///
/// Database errors.
pub(super) async fn update_seen(pool: &PgPool, did: &str, seen_at: DateTime<Utc>) -> Result<(), sqlx::Error> {
    sqlx::query(
        "INSERT INTO private_notification_seen (did, seen_at) VALUES ($1, LEAST($2, now())) \
         ON CONFLICT (did) DO UPDATE SET seen_at = GREATEST(private_notification_seen.seen_at, LEAST(EXCLUDED.seen_at, now()))",
    )
    .bind(did)
    .bind(seen_at)
    .execute(pool)
    .await?;
    Ok(())
}

/// `getUnreadCount` (§4.8): the recipient's rows after its last-seen time,
/// under [`notification_kept`], counting only what the list would show:
/// - a row from a private post when the post passes the prefilter for the
///   recipient, which carries the moderation test (§3.4), so a mention in a
///   post the recipient cannot read is never counted;
/// - a row from a public record while its subject private post is stored.
///
/// # Errors
///
/// Database errors.
pub(super) async fn unread_count(pool: &PgPool, recipient: &str) -> Result<i64, sqlx::Error> {
    sqlx::query_scalar(&format!(
        "SELECT count(*) FROM private_notifications n WHERE n.recipient_did = $1 AND {kept} \
           AND n.indexed_at > COALESCE((SELECT seen_at FROM private_notification_seen WHERE did = $1), '-infinity') \
           AND CASE WHEN split_part(n.record_uri, '/', 4) = '{post_private}' \
                 THEN EXISTS (SELECT 1 FROM {PREFILTER_FROM} WHERE p.uri = n.record_uri AND {pre}) \
                 ELSE EXISTS (SELECT 1 FROM private_posts s WHERE s.uri = n.reason_subject) END",
        kept = notification_kept(),
        post_private = crate::firehose::classifier::NSID_POST_PRIVATE,
        pre = prefilter(),
    ))
    .bind(recipient)
    .fetch_one(pool)
    .await
}
