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
use crate::firehose::classifier::{NSID_AUDIENCE, NSID_POST_PRIVATE};

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
}

/// # Errors
///
/// Database errors.
pub(super) async fn view_meta(pool: &PgPool, rauth: &RecordAuthz) -> Result<Option<ViewMeta>, sqlx::Error> {
    sqlx::query_as(
        "SELECT p.cid, p.audience_uri, p.reply_root_uri, p.reply_parent_uri, p.reply_root_cid, p.reply_parent_cid, \
           p.created_at, p.indexed_at FROM private_posts p WHERE p.uri = $1",
    )
    .bind(rauth.uri())
    .fetch_optional(pool)
    .await
}

/// The view's audience (P-3, §4.10; private-threads design §2.6): the
/// governing audience's `(mode, name)` while its row is stored. Never its
/// members.
///
/// # Errors
///
/// Database errors.
pub(super) async fn audience_view(pool: &PgPool, uri: &str) -> Result<Option<(Option<String>, Option<String>)>, sqlx::Error> {
    sqlx::query_as("SELECT mode, name FROM private_audiences WHERE uri = $1")
        .bind(uri)
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
/// not hide (no `actors` row reads as visible). The walk's step test uses
/// this form, which PostgreSQL probes through `labels`' subject index inside
/// the recursion; stage 1 uses [`gate`]. Since the shared walk, the one-row
/// form ([`walked_one`]) and HEAD's builders.
fn gate_shows(uri: &str, author: &str, status: &str) -> String {
    format!(
        "NOT EXISTS (SELECT 1 FROM labels l WHERE (l.subject = {uri} AND l.val IN ('!takedown', '!hide')) \
                                              OR (l.subject = {author} AND l.val = '!takedown')) \
         AND COALESCE({status}, 'active') NOT IN {HIDING_STATUSES}"
    )
}

/// [`gate_shows`]'s rule as S12's statements test it: the post's labels and
/// its author's in two lookups, each on one subject, so PostgreSQL can hash
/// each (or probe `labels`' subject index) where one lookup over both
/// subjects scans the table per row. The same predicate.
fn gate(uri: &str, author: &str, status: &str) -> String {
    format!(
        "NOT EXISTS (SELECT 1 FROM labels l WHERE l.subject = {uri} AND l.val IN ('!takedown', '!hide')) \
         AND NOT EXISTS (SELECT 1 FROM labels l WHERE l.subject = {author} AND l.val = '!takedown') \
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

/// HEAD's rule-4 cap before S12 (private-posting design §3.7): the walk
/// examined at most this many rows. Kept with HEAD's builders below for the
/// pre-release counts and the B-5 comparisons (private-threads design §4,
/// §8).
#[cfg(test)]
pub(super) const RULE4_MAX_HOPS: u32 = 8;

/// HEAD's rule-4 step test `step_ok(s)` (§3.7) for the walk row `s`, as it
/// stood before S12: the reader could read the step, so it may anchor or be
/// walked past.
/// - (i) no block either way between the reader and the step's author;
/// - (ii) the moderation gate shows the step;
/// - (iii) a step not by the audience's owner is by a writer who passes
///   conditions 2 and 3 under the audience (condition 3 under every mode
///   since S9, C-6; `following` since S10: the owner's stored follow of
///   the step's author, and the owner's flag).
#[cfg(test)]
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

/// HEAD's rule-4 walk (§3.7) as an SQL boolean, before S12. Every argument
/// is an SQL expression; the cap is inlined.
#[cfg(test)]
pub(super) fn anchored(parent: &str, audience: &str, owner: &str, mode: &str, members: &str, reader: &str) -> String {
    anchored_with_cap(parent, audience, owner, mode, members, reader, RULE4_MAX_HOPS)
}

/// [`anchored`] with another cap: the pre-S0 count query uses one other
/// than [`RULE4_MAX_HOPS`], to separate over-cap chains, and the pre-S12
/// count one to separate depth from every other reason.
#[cfg(test)]
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

// ---- The read rule of the private-threads design (S12, §2, §3.1-§3.2) ----
//
// Every reader-relative statement binds the reader as `$1`. Each candidate
// query is one statement over its own `c` (the key columns `{COLS}` and the
// shape's order keys and outputs): the climb over every chain key and
// descent 1, the branch roots (shared-walk §2.1, §2.2); `s1`, stage 1 per
// row; the walk after stage 1, a thread climb, the author memo and
// descent 2 (shared-walk §2.4); and the shape's final SELECT. The oracle's
// fact 8 is the one-row case of the same statement.

/// The key columns every shape's `c` selects (§3.2), from `private_posts p`.
pub(super) const COLS: &str = "p.uri, p.author_did, p.audience_uri, p.reply_parent_uri, p.reply_root_uri";

/// `x` (an SQL expression, an at-uri or NULL) names a `postPrivate` record,
/// by its collection, `split_part(x, '/', 4)`. NULL does not.
fn is_private(x: &str) -> String {
    format!("COALESCE(split_part({x}, '/', 4) = '{NSID_POST_PRIVATE}', false)")
}

/// The row `r` (an alias with `reply_parent_uri` and `reply_root_uri`) is a
/// top-level post (§2.1).
fn is_top_level(r: &str) -> String {
    format!("({r}.reply_parent_uri IS NULL AND {r}.reply_root_uri IS NULL)")
}

/// `r` is a branch root: a reply whose parent and root are not
/// `postPrivate` records.
fn is_branch_root(r: &str) -> String {
    format!(
        "({r}.reply_parent_uri IS NOT NULL AND {r}.reply_root_uri IS NOT NULL \
          AND NOT {} AND NOT {})",
        is_private(&format!("{r}.reply_parent_uri")),
        is_private(&format!("{r}.reply_root_uri"))
    )
}

/// `r` is a thread reply: its parent and root are `postPrivate` records.
fn is_thread_reply(r: &str) -> String {
    format!(
        "({} AND {})",
        is_private(&format!("{r}.reply_parent_uri")),
        is_private(&format!("{r}.reply_root_uri"))
    )
}

/// `r` is a branch reply: its parent is a `postPrivate` record and its root
/// is not.
fn is_branch_reply(r: &str) -> String {
    format!(
        "({} AND {r}.reply_root_uri IS NOT NULL AND NOT {})",
        is_private(&format!("{r}.reply_parent_uri")),
        is_private(&format!("{r}.reply_root_uri"))
    )
}

/// The URI `x` names an audience record in `o`'s repository.
fn owned(x: &str, o: &str) -> String {
    format!("(split_part({x}, '/', 3) = {o} AND split_part({x}, '/', 4) = '{NSID_AUDIENCE}')")
}

/// The anchor `an` is of `c`'s role's kind (review 13, m1): a top-level
/// post and a branch root anchor on themselves, a thread reply on a
/// top-level post, a branch reply on a branch root. So a thread reply naming
/// itself as its root has no valid anchor.
fn role_kind(c: &str, an: &str) -> String {
    format!(
        "((({tl} OR {brt}) AND {an}.uri = {c}.uri) OR ({tr} AND {an_tl}) OR ({brr} AND {an_brt}))",
        tl = is_top_level(c),
        brt = is_branch_root(c),
        tr = is_thread_reply(c),
        an_tl = is_top_level(an),
        brr = is_branch_reply(c),
        an_brt = is_branch_root(an),
    )
}

/// `c` anchors on itself: a top-level post or a branch root (§2.1).
fn self_anchored() -> String {
    format!("({} OR {})", is_top_level("c"), is_branch_root("c"))
}

/// The anchor of a `c` row that does not anchor on itself: a thread
/// reply's root, a branch reply's branch root (`br` is its expression);
/// NULL otherwise.
fn other_anchor(br: &str) -> String {
    format!(
        "CASE WHEN {tr} THEN c.reply_root_uri WHEN {brr} THEN {br} END",
        tr = is_thread_reply("c"),
        brr = is_branch_reply("c"),
    )
}

/// The key of a `(parent, root)` pair in the per-key statement's maps
/// (`brm`, `pvm`): the two URIs joined by a space, which no URI contains.
fn pair_key(parent: &str, root: &str) -> String {
    format!("({parent} || ' ' || {root})")
}

/// The branch root of `c`'s `(parent, root)` key, from the map `map`
/// (`bdm`, by a climbed row's URI and context; `brm` in the per-key
/// reference).
fn br_of_c(map: &str) -> String {
    format!("((SELECT m FROM {map}) ->> (c.reply_parent_uri || ' ' || c.reply_root_uri))")
}

/// Why the per-key results are maps (B-5 part (3), 2026-10-17): a statement's
/// CTEs carry the planner's estimate of `c`, which a shape's filters can put
/// at 1 row when thousands come back. Joined back to `c` by equality, a CTE
/// is then scanned once per row (a nested loop over two CTE scans, 25M
/// comparisons for 5 000 rows). Each per-key result is instead folded into
/// one `jsonb` object, read once as an init plan, and looked up per row by
/// key, a binary search whatever the estimate.
///
/// The anchors of a statement's rows that do not anchor on themselves, each
/// looked up once (the `akm` map, by the anchor's URI): rows sharing a root
/// or a branch root share one lookup. Their audiences are the `gm` map.
fn anchor_keys(s1_filter: &str, br: &str) -> String {
    format!(
        "SELECT COALESCE(jsonb_object_agg(an.uri, jsonb_build_object( \
                  'author_did', an.author_did, 'audience_uri', an.audience_uri, \
                  'reply_parent_uri', an.reply_parent_uri, 'reply_root_uri', an.reply_root_uri)), '{{}}'::jsonb) AS m \
         FROM (SELECT DISTINCT {other} AS k FROM c {s1_filter}) d \
         JOIN private_posts an ON an.uri = d.k",
        other = other_anchor(br),
    )
}

/// The governing audiences of the anchors in `akm` (the `gm` map, by the
/// audience's URI).
const AUDIENCE_KEYS: &str = "SELECT COALESCE(jsonb_object_agg(g.uri, jsonb_build_object( \
       'owner_did', g.owner_did, 'mode', g.mode, 'members', to_jsonb(g.members))), '{}'::jsonb) AS m \
     FROM private_audiences g \
     WHERE g.uri IN (SELECT DISTINCT v.value ->> 'audience_uri' FROM akm, jsonb_each(akm.m) v)";

/// `d` is listed in G's `members`: G's own array for a row that anchors on
/// itself, else the `gm` entry's (`a.members_j`); `mapped` is false where `a`
/// is a `private_audiences` row (the walk).
fn listed(d: &str, mapped: bool) -> String {
    if mapped {
        format!("COALESCE({d} = ANY(a.members), a.members_j ? {d})")
    } else {
        format!("{d} = ANY(a.members)")
    }
}

/// The FROM fragment joining `c`'s author (`po`), its anchor (`an`) and the
/// governing audience G (`a`) (§3.1). A row that anchors on itself is its
/// own anchor (its row as `c` read it) and G is its own audience, looked up
/// directly; any other row's anchor and G come from the `akm` and `gm` maps,
/// by the anchor's URI. `an` and `a` are the columns either way, as a lookup
/// by the anchor's URI gives them; G's members are `a.members` for the
/// first and `a.members_j` for the second.
fn anchor_join(br: &str) -> String {
    let own = self_anchored();
    let pick = |own_col: &str, map_col: &str, name: &str| format!("CASE WHEN {own} THEN {own_col} ELSE {map_col} END AS {name}");
    format!(
        "LEFT JOIN actors po ON po.did = c.author_did \
         CROSS JOIN LATERAL (SELECT (SELECT m FROM akm) -> {other} AS v) ak \
         CROSS JOIN LATERAL (SELECT (SELECT m FROM gm) -> (ak.v ->> 'audience_uri') AS v) gk \
         LEFT JOIN private_audiences a0 ON a0.uri = CASE WHEN {own} THEN c.audience_uri END \
         CROSS JOIN LATERAL (SELECT {an_uri}, {an_author}, {an_audience}, {an_parent}, {an_root}) an \
         CROSS JOIN LATERAL (SELECT {a_uri}, {a_owner}, {a_mode}, {a_members}, \
           CASE WHEN {own} THEN NULL ELSE gk.v -> 'members' END AS members_j) a",
        other = other_anchor(br),
        an_uri = pick("c.uri", &format!("CASE WHEN ak.v IS NOT NULL THEN {} END", other_anchor(br)), "uri"),
        an_author = pick("c.author_did", "ak.v ->> 'author_did'", "author_did"),
        an_audience = pick("c.audience_uri", "ak.v ->> 'audience_uri'", "audience_uri"),
        an_parent = pick("c.reply_parent_uri", "ak.v ->> 'reply_parent_uri'", "reply_parent_uri"),
        an_root = pick("c.reply_root_uri", "ak.v ->> 'reply_root_uri'", "reply_root_uri"),
        a_uri = pick("a0.uri", "CASE WHEN gk.v IS NOT NULL THEN ak.v ->> 'audience_uri' END", "uri"),
        a_owner = pick("a0.owner_did", "gk.v ->> 'owner_did'", "owner_did"),
        a_mode = pick("a0.mode", "gk.v ->> 'mode'", "mode"),
        a_members = pick("a0.members", "NULL::text[]", "members"),
    )
}

/// O's account status: the joined author's when O wrote `c`, else one
/// lookup.
fn ao_status() -> &'static str {
    "CASE WHEN an.author_did = c.author_did THEN po.account_status \
     ELSE (SELECT x.account_status FROM actors x WHERE x.did = an.author_did) END"
}

/// `d` stands under G (`a` in scope) as O's (`o`) writer (§2.4): by the
/// PDS's write rule for the mode. `verified` is O's follow-verification
/// flag, used only under `following`.
fn standing(d: &str, o: &str, verified: &str, mapped: bool) -> String {
    format!(
        "CASE COALESCE(a.mode, 'list') \
           WHEN 'list' THEN {listed} \
           WHEN 'followers' THEN EXISTS (SELECT 1 FROM follows f WHERE f.author_did = {d} AND f.subject_did = {o}) \
           WHEN 'everyone' THEN true \
           WHEN 'following' THEN {follows} AND {verified} \
           ELSE false END",
        follows = owner_follows(o, d),
        listed = listed(d, mapped),
    )
}

/// Rule 8 (§2.2): the reader `$1` is inside G (`a`), by HEAD's reader arms
/// verbatim: in memory for `list`, one probe for `followers`, two for
/// `following`.
fn reader_inside() -> String {
    format!(
        "CASE COALESCE(a.mode, 'list') \
           WHEN 'list' THEN {listed} \
           WHEN 'followers' THEN a.owner_did = $1 OR EXISTS ( \
             SELECT 1 FROM follows f WHERE f.author_did = $1 AND f.subject_did = a.owner_did) \
           WHEN 'everyone' THEN true \
           WHEN 'following' THEN a.owner_did = $1 OR ({follows} AND {verified}) \
           ELSE false END",
        follows = owner_follows("a.owner_did", "$1"),
        verified = follows_verified("a.owner_did"),
        listed = listed("$1", true),
    )
}

/// Stage 1 (§3.1) for a row `c` by W ≠ V, cheapest term first (review 13,
/// LB1): the anchor and its kind, the owned audiences and G's row, all in
/// memory; rule 8; the gate on the post and rule 4; the anchor's gate;
/// rule 7 against O; condition 4 and W's standing. One `AND` of terms with
/// no side effects, wrapped in `COALESCE(…, false)` by its caller, so its
/// order changes only which terms PostgreSQL evaluates, never its value.
fn stage1_ok() -> String {
    stage1_terms(true).join(" AND ")
}

/// [`stage1_ok`]'s terms, in its order; without rule 8 when `inside` is
/// false (the pre-release count asks with no reader).
fn stage1_terms(inside: bool) -> Vec<String> {
    let mut terms = vec![
        // (1) in memory: a stored anchor of its role's kind (review 13,
        // m1), under its own author's audience; P under O's (5.2); rule 6.
        "an.uri IS NOT NULL".to_string(),
        role_kind("c", "an"),
        owned("an.audience_uri", "an.author_did"),
        owned("c.audience_uri", "an.author_did"),
        "a.uri IS NOT NULL".to_string(),
    ];
    if inside {
        // (2) rule 8, HEAD's reader arms.
        terms.push(reader_inside());
    }
    terms.extend([
        // (3) the gate on P, then rule 4.
        gate("c.uri", "c.author_did", "po.account_status"),
        format!("NOT {}", blocked("$1", "c.author_did")),
        // (4) the anchor's gate; skipped when the anchor is P.
        format!("(an.uri = c.uri OR {})", gate("an.uri", "an.author_did", &format!("({})", ao_status()))),
        // (5) rule 7 against O (V–O, then W–O, also W's condition 3),
        // condition 4 and W's standing.
        format!("(an.author_did = $1 OR an.author_did = c.author_did OR NOT {})", blocked("$1", "an.author_did")),
        format!("(c.author_did = an.author_did OR NOT {})", blocked("c.author_did", "an.author_did")),
        format!(
            "(c.author_did = an.author_did OR (({}) = 'active' AND {}))",
            ao_status(),
            standing("c.author_did", "an.author_did", &follows_verified("an.author_did"), true)
        ),
    ]);
    terms
}

/// [`stage1_terms`] for the T-52 like-with-like check, which re-orders them.
#[cfg(test)]
pub(super) fn stage1_terms_for_tests() -> Vec<String> {
    stage1_terms(true)
}

/// `c` is walked (§3.1): every reply but an owner thread reply.
fn needs_walk() -> String {
    format!(
        "NOT ({} OR {} OR ({} AND c.author_did = an.author_did))",
        is_top_level("c"),
        is_branch_root("c"),
        is_thread_reply("c")
    )
}

/// The branch root the chain from `parent` reaches under `root` (§3.1):
/// the structural lookup, primary keys and the root column only, at most
/// `cap` rows. NULL when none is found. The one-row form only
/// ([`walked_one`]); every other statement uses descent 1.
fn branch_root_of(parent: &str, root: &str, cap: u32) -> String {
    format!(
        "(WITH RECURSIVE up(uri, parent, hop) AS ( \
            SELECT s.uri, s.reply_parent_uri, 1 FROM private_posts s \
            WHERE s.uri = {parent} AND s.reply_root_uri = {root} \
            UNION ALL \
            SELECT s.uri, s.reply_parent_uri, u.hop + 1 \
            FROM up u JOIN private_posts s ON s.uri = u.parent \
            WHERE u.hop < {cap} AND {up_private} AND s.reply_root_uri = {root} \
          ) \
          SELECT up.uri FROM up WHERE NOT {top_private} LIMIT 1)",
        up_private = is_private("u.parent"),
        top_private = is_private("up.parent"),
    )
}

/// The walk (§2.3, §2.4) for the key row `k` (`reply_parent_uri`,
/// `reply_root_uri`, `owner`), with G (`a`) and O's flag (`fv.ok`) in scope:
/// from `k`'s parent through stored rows, at most `cap`, each tested inside
/// the recursion, so the walk ends at its first failing row; true when it
/// reaches its stop. A cycle fails at the cap. The one-row form only
/// ([`walked_one`]); every other statement uses descent 2.
fn parent_verdict(cap: u32) -> String {
    let thread = is_private("k.reply_root_uri");
    let step = format!(
        "(s.reply_root_uri = k.reply_root_uri OR ({thread} AND s.uri = k.reply_root_uri)) \
         AND {owned} \
         AND {gate} \
         AND (s.author_did = k.owner OR ( \
               NOT {reader_author} \
               AND NOT {author_owner} \
               AND {standing}))",
        owned = owned("s.audience_uri", "k.owner"),
        // A key reaches the walk only from a row whose stage 1 passed, and a
        // walked row's anchor is not the row itself, so stage 1 gated the
        // anchor and with it O's status, in this statement's snapshot: a
        // step by O needs no status lookup of its own.
        gate = gate_shows(
            "s.uri",
            "s.author_did",
            "CASE WHEN s.author_did = k.owner THEN NULL ELSE (SELECT x.account_status FROM actors x WHERE x.did = s.author_did) END"
        ),
        reader_author = blocked("$1", "s.author_did"),
        author_owner = blocked("s.author_did", "k.owner"),
        standing = standing("s.author_did", "k.owner", "COALESCE(fv.ok, false)", false),
    );
    // A thread's walk stops at the root or at the first thread reply by O
    // (a "none"-kind row by O is not one); a branch's at the branch root,
    // the first row whose parent is not a `postPrivate` record.
    let stop = |u: &str| {
        let parent_private = is_private(&format!("{u}.parent"));
        format!(
            "CASE WHEN {thread} THEN {u}.uri = k.reply_root_uri OR ({u}.author = k.owner AND {parent_private}) \
                  ELSE NOT {parent_private} END"
        )
    };
    format!(
        "EXISTS ( \
           WITH RECURSIVE up(uri, parent, author, hop) AS ( \
             SELECT s.uri, s.reply_parent_uri, s.author_did, 1 FROM private_posts s \
             WHERE s.uri = k.reply_parent_uri AND {step} \
             UNION ALL \
             SELECT s.uri, s.reply_parent_uri, s.author_did, u.hop + 1 \
             FROM up u JOIN private_posts s ON s.uri = u.parent \
             WHERE u.hop < {cap} AND NOT ({stop_u}) AND {step} \
           ) \
           SELECT 1 FROM up WHERE {stop_up})",
        stop_u = stop("u"),
        stop_up = stop("up"),
    )
}

/// `s1` joined to its key's walk, for a shape's final SELECT.
pub(super) const PV_JOIN: &str = "s1 CROSS JOIN LATERAL (SELECT CASE WHEN s1.reply_parent_uri IS NOT NULL AND s1.reply_root_uri IS NOT NULL \
     THEN ((SELECT m FROM pvm) -> (s1.reply_parent_uri || ' ' || s1.reply_root_uri))::boolean END AS ok) pv";

/// A row's verdict `{pass}` (§3.2): stage 1 (the gate alone for the
/// reader's own row), and the walk when the row is walked.
pub(super) const PASS: &str = "(s1.s1ok AND (s1.author_did = $1 OR NOT s1.walks OR COALESCE(pv.ok, false)))";

/// The most node rows the climbs of one statement may hold (shared-walk
/// §2.5): an internal constant, not a config key.
const CLIMB_NODES_MAX: u32 = 20_000;

/// K (shared-walk §2.5): the most chain start parents one statement may
/// have at the cap `cap`, so that its climbs hold at most about
/// [`CLIMB_NODES_MAX`] node rows. 1 250 at 16, 200 at 100.
pub(super) fn climb_keys(cap: u32) -> usize {
    usize::try_from((CLIMB_NODES_MAX / cap.max(1)).max(1)).unwrap_or(1)
}

/// The row `x` (its URI and root expressions) agrees with the context
/// `ctx` (shared-walk §2, S12 §2.3): a branch's rows claim its root; a
/// thread's rows claim it or are it.
fn agree(uri: &str, root: &str, ctx: &str) -> String {
    format!("({root} = {ctx} OR ({} AND {uri} = {ctx}))", is_private(ctx))
}

/// A branch's stop: the row whose parent (an expression) is not a
/// `postPrivate` record, its branch root.
fn bstop(parent: &str) -> String {
    format!("NOT {}", is_private(parent))
}

/// A thread's stop in the context `ctx` with owner `o`, over the climb's
/// short names of the row `x`: the root itself, or a thread reply by O.
fn tstop(x: &str, ctx: &str, o: &str) -> String {
    format!("({x}.uri = {ctx} OR ({x}.author = {o} AND {}))", is_private(&format!("{x}.parent")))
}

/// The climb (shared-walk §2.1): one recursive CTE `name` from every
/// distinct start parent of the CTE `starts` (`parent`, `root`, `owner`),
/// breadth first, by primary key, each fetched row kept once per context
/// it agrees with, at most `cap` levels; with its maps `{name}_sp` (start
/// parent to its contexts) and `{name}_sk` (the start keys). Node rows have
/// `lvl` NULL; one marker row per level carries the level, which a window
/// over the working table gives every node row of that iteration. It reads
/// `private_posts` only.
fn climb(name: &str, starts: &str, thread: bool, cap: u32) -> String {
    let stop = if thread { tstop("x", "x.ctx", "x.owner") } else { bstop("x.parent") };
    format!(
        "{name}_sp AS MATERIALIZED ( \
           SELECT COALESCE(jsonb_object_agg(g.parent, g.v), '{{}}'::jsonb) AS m FROM ( \
             SELECT s.parent, jsonb_agg(jsonb_build_object('root', s.root, 'owner', s.owner)) AS v \
             FROM {starts} s WHERE s.parent IS NOT NULL AND s.root IS NOT NULL GROUP BY s.parent) g), \
         {name}_sk AS MATERIALIZED ( \
           SELECT COALESCE(jsonb_object_agg(s.parent || ' ' || s.root, true), '{{}}'::jsonb) AS m \
           FROM {starts} s WHERE s.parent IS NOT NULL AND s.root IS NOT NULL), \
         {name}(uri, parent, root, author, audience, ctx, owner, lvl) AS ( \
           SELECT s.uri, s.reply_parent_uri, s.reply_root_uri, s.author_did, s.audience_uri, k.root, k.owner, NULL::int \
           FROM jsonb_each((SELECT m FROM {name}_sp)) d \
           CROSS JOIN LATERAL (SELECT p.uri, p.reply_parent_uri, p.reply_root_uri, p.author_did, p.audience_uri \
                               FROM private_posts p WHERE p.uri = d.key LIMIT 1) s \
           CROSS JOIN LATERAL jsonb_to_recordset(d.value) AS k(root text, owner text) \
           WHERE {agree1} \
           UNION \
           SELECT NULL, NULL, NULL, NULL, NULL, NULL, NULL, 1 \
           UNION \
           SELECT n.* FROM ( \
             SELECT w.*, max(w.lvl) OVER () AS cur, count(w.uri) OVER () AS live FROM {name} w \
           ) x \
           CROSS JOIN LATERAL ( \
             (SELECT p.uri, p.reply_parent_uri, p.reply_root_uri, p.author_did, p.audience_uri, x.ctx, x.owner, NULL::int \
              FROM private_posts p \
              WHERE x.uri IS NOT NULL AND x.cur < {cap} AND NOT {stop} \
                AND NOT ((SELECT m FROM {name}_sk) ? (x.parent || ' ' || x.ctx)) \
                AND p.uri = x.parent AND {agree_p} \
              LIMIT 1) \
             UNION ALL \
             SELECT NULL, NULL, NULL, NULL, NULL, NULL, NULL, x.lvl + 1 \
             WHERE x.uri IS NULL AND x.lvl < {cap} AND x.live > 0 \
           ) n)",
        agree1 = agree("s.uri", "s.reply_root_uri", "k.root"),
        agree_p = agree("p.uri", "p.reply_root_uri", "x.ctx"),
    )
}

/// The child map of the climb `name` (`bkids`, `tkids`): by a parent's
/// URI and context, the node rows below it, over node rows with a parent.
///
/// Here and in `wk` and `am`, rows are grouped or made distinct under the
/// "C" collation (#720, B-5 `Distinct`): the default collation compares
/// long URIs through the locale, which made sorting 40 000 keys cost more
/// than climbing to them. Under a deterministic collation equal strings
/// are equal bytes, so the groups are the same; `wk` and `am` hand their
/// columns back in the default collation.
fn kids(name: &str) -> String {
    format!(
        "SELECT COALESCE(jsonb_object_agg(g.k, g.v), '{{}}'::jsonb) AS m FROM ( \
           SELECT (r.parent || ' ' || r.ctx) COLLATE \"C\" AS k, \
                  jsonb_agg(jsonb_build_object('uri', r.uri, 'parent', r.parent, 'root', r.root, \
                                               'author', r.author, 'audience', r.audience)) AS v \
           FROM {name} r WHERE r.uri IS NOT NULL AND r.parent IS NOT NULL AND r.ctx IS NOT NULL GROUP BY 1) g"
    )
}

/// The walk's row test `ok` (shared-walk §2.4) for the row `e` (short
/// names) in the context `ctx` with O `o` and G `g`: agreement and O's
/// audience in memory, its author's memo entry (`amm`), and the post's own
/// `!takedown` or `!hide` label. NULL reads as false.
fn walk_ok(e: &str, ctx: &str, o: &str, g: &str) -> String {
    format!(
        "COALESCE({agree} AND {owned} \
           AND ((SELECT m FROM amm) ->> ({e}.author || ' ' || {o} || ' ' || {g}))::boolean \
           AND NOT EXISTS (SELECT 1 FROM labels l WHERE l.subject = {e}.uri AND l.val IN ('!takedown', '!hide')), false)",
        agree = agree(&format!("{e}.uri"), &format!("{e}.root"), ctx),
        owned = owned(&format!("{e}.audience"), o),
    )
}

/// One candidate statement (§3.2; shared-walk §2): `c` is the shape's own
/// SELECT of [`COLS`] and its order keys and outputs, `s1_filter` restricts
/// the rows stage 1 reads (`getQuotes`' refs arm is not prefiltered), and
/// `final_` reads [`PV_JOIN`]. Every chain key of the statement shares one
/// climb before stage 1 (branch roots, descent 1) and one after it for
/// walked thread keys; the walk is descent 2 over the climbed rows, with
/// each author's terms evaluated once per `(author, O, G)`.
pub(super) fn walked(c: &str, s1_filter: &str, final_: &str, cap: u32) -> String {
    #[cfg(test)]
    if PER_KEY.with(std::cell::Cell::get) {
        return walked_per_key(c, s1_filter, final_, cap);
    }
    walked_with(c, s1_filter, final_, cap, &stage1_ok())
}

#[cfg(test)]
thread_local! {
    /// Tests only (shared-walk T-60): [`walked`] builds the per-key
    /// reference on this thread while set.
    pub(super) static PER_KEY: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

/// [`walked`] with stage 1's text given.
fn walked_with(c: &str, s1_filter: &str, final_: &str, cap: u32, stage1: &str) -> String {
    let thread_root = |r: &str| is_private(&format!("{r}.root"));
    format!(
        "WITH RECURSIVE c AS MATERIALIZED ({c}), \
         bk AS MATERIALIZED ( \
           SELECT DISTINCT c.reply_parent_uri AS parent, c.reply_root_uri AS root, NULL::text AS owner \
           FROM c WHERE {branch_reply}), \
         {bcl}, \
         bkids AS MATERIALIZED ({bkids}), \
         bd(uri, parent, root, author, audience, ctx, broot, d) AS ( \
           SELECT r.uri, r.parent, r.root, r.author, r.audience, r.ctx, r.uri, 1 \
           FROM bcl r WHERE r.uri IS NOT NULL AND {bstop_r} \
           UNION ALL \
           SELECT e.uri, e.parent, e.root, e.author, e.audience, x.ctx, x.broot, x.d + 1 \
           FROM bd x \
           CROSS JOIN LATERAL jsonb_to_recordset((SELECT m FROM bkids) -> (x.uri || ' ' || x.ctx)) \
                AS e(uri text, parent text, root text, author text, audience text) \
           WHERE x.d < {cap} AND NOT {bstop_e}), \
         bdm AS MATERIALIZED ( \
           SELECT COALESCE(jsonb_object_agg(b.uri || ' ' || b.ctx, b.broot), '{{}}'::jsonb) AS m FROM bd b \
           WHERE b.uri IS NOT NULL AND b.ctx IS NOT NULL), \
         akm AS MATERIALIZED ({anchors}), \
         gm AS MATERIALIZED ({AUDIENCE_KEYS}), \
         s1 AS MATERIALIZED ( \
           SELECT c.*, an.uri AS anchor, an.author_did AS owner, an.audience_uri AS gov, \
                  COALESCE(CASE WHEN c.author_did = $1 THEN {gate} ELSE {stage1} END, false) AS s1ok, \
                  COALESCE({walks}, true) AS walks \
           FROM c {join} {s1_filter}), \
         wk AS MATERIALIZED ( \
           SELECT {wk_cols} FROM ( \
             SELECT DISTINCT {wk_c} \
             FROM s1 WHERE s1.s1ok AND s1.walks AND s1.author_did <> $1) x), \
         wkm AS MATERIALIZED ( \
           SELECT COALESCE(jsonb_object_agg(k.parent || ' ' || k.root, true), '{{}}'::jsonb) AS m FROM wk k \
           WHERE k.parent IS NOT NULL AND k.root IS NOT NULL), \
         wbm AS MATERIALIZED ( \
           SELECT COALESCE(jsonb_object_agg(k.root || ' ' || k.anchor, jsonb_build_object('owner', k.owner, 'gov', k.gov)), '{{}}'::jsonb) AS m \
           FROM wk k WHERE k.root IS NOT NULL AND k.anchor IS NOT NULL AND NOT {wk_thread}), \
         wtm AS MATERIALIZED ( \
           SELECT COALESCE(jsonb_object_agg(k.root, jsonb_build_object('owner', k.owner, 'gov', k.gov)), '{{}}'::jsonb) AS m \
           FROM wk k WHERE k.root IS NOT NULL AND {wk_thread}), \
         tk AS MATERIALIZED (SELECT DISTINCT k.parent, k.root, k.owner FROM wk k WHERE {wk_thread}), \
         {tcl}, \
         tkids AS MATERIALIZED ({tkids}), \
         am AS MATERIALIZED ( \
           SELECT q.d, q.o, q.g, COALESCE( \
             NOT EXISTS (SELECT 1 FROM labels l WHERE l.subject = q.d AND l.val = '!takedown') \
             AND (q.d = q.o OR ( \
               COALESCE((SELECT x.account_status FROM actors x WHERE x.did = q.d), 'active') NOT IN {HIDING_STATUSES} \
               AND NOT {reader_author} \
               AND NOT {author_owner} \
               AND {standing})), false) AS ok \
           FROM (SELECT y.d COLLATE \"default\" AS d, y.o COLLATE \"default\" AS o, y.g COLLATE \"default\" AS g FROM ( \
                 SELECT DISTINCT u.d COLLATE \"C\" AS d, u.o COLLATE \"C\" AS o, u.g COLLATE \"C\" AS g FROM ( \
                   SELECT b.author AS d, v.v ->> 'owner' AS o, v.v ->> 'gov' AS g \
                   FROM bd b CROSS JOIN LATERAL (SELECT (SELECT m FROM wbm) -> (b.ctx || ' ' || b.broot) AS v OFFSET 0) v \
                   WHERE v.v IS NOT NULL \
                   UNION ALL \
                   SELECT r.author, r.owner, (SELECT m FROM wtm) -> r.ctx ->> 'gov' FROM tcl r WHERE r.uri IS NOT NULL) u \
                 WHERE u.d IS NOT NULL AND u.o IS NOT NULL AND u.g IS NOT NULL) y) q \
           LEFT JOIN private_audiences a ON a.uri = q.g \
           LEFT JOIN LATERAL (SELECT CASE WHEN a.mode = 'following' THEN {owner_verified} END AS ok) fv ON true), \
         amm AS MATERIALIZED ( \
           SELECT COALESCE(jsonb_object_agg(am.d || ' ' || am.o || ' ' || am.g, am.ok), '{{}}'::jsonb) AS m FROM am), \
         wd(uri, ctx, owner, gov, d) AS ( \
           SELECT s.uri, s.ctx, s.owner, s.gov, 1 FROM ( \
             SELECT * FROM ( \
               SELECT b.uri, b.root, b.author, b.audience, b.ctx, gv.v ->> 'owner' AS owner, gv.v ->> 'gov' AS gov \
               FROM bd b CROSS JOIN LATERAL (SELECT (SELECT m FROM wbm) -> (b.ctx || ' ' || b.uri) AS v OFFSET 0) gv \
               WHERE b.d = 1 AND gv.v IS NOT NULL \
               UNION ALL \
               SELECT r.uri, r.root, r.author, r.audience, r.ctx, r.owner, (SELECT m FROM wtm) -> r.ctx ->> 'gov' \
               FROM tcl r WHERE r.uri IS NOT NULL AND {tstop_r}) u \
             OFFSET 0) s \
           WHERE {ok_s} \
           UNION ALL \
           SELECT e.uri, x.ctx, x.owner, x.gov, x.d + 1 \
           FROM wd x \
           CROSS JOIN LATERAL jsonb_to_recordset( \
                  (CASE WHEN {x_thread} THEN (SELECT m FROM tkids) ELSE (SELECT m FROM bkids) END) -> (x.uri || ' ' || x.ctx)) \
                AS e(uri text, parent text, root text, author text, audience text) \
           WHERE x.d < {cap} \
             AND NOT (CASE WHEN {x_thread} THEN {tstop_e} ELSE {bstop_e} END) \
             AND {ok_e}), \
         pvm AS MATERIALIZED ( \
           SELECT COALESCE(jsonb_object_agg(w.uri || ' ' || w.ctx, true), '{{}}'::jsonb) AS m FROM wd w \
           WHERE w.uri IS NOT NULL AND w.ctx IS NOT NULL AND (SELECT m FROM wkm) ? (w.uri || ' ' || w.ctx)) \
         {final_}",
        branch_reply = is_branch_reply("c"),
        wk_c = "s1.reply_parent_uri COLLATE \"C\" AS parent, s1.reply_root_uri COLLATE \"C\" AS root, \
                s1.anchor COLLATE \"C\" AS anchor, s1.owner COLLATE \"C\" AS owner, s1.gov COLLATE \"C\" AS gov",
        wk_cols = "x.parent COLLATE \"default\" AS parent, x.root COLLATE \"default\" AS root, \
                   x.anchor COLLATE \"default\" AS anchor, x.owner COLLATE \"default\" AS owner, x.gov COLLATE \"default\" AS gov",
        bcl = climb("bcl", "bk", false, cap),
        bkids = kids("bcl"),
        bstop_r = bstop("r.parent"),
        bstop_e = bstop("e.parent"),
        gate = gate("c.uri", "c.author_did", "po.account_status"),
        walks = needs_walk(),
        anchors = anchor_keys(s1_filter, &br_of_c("bdm")),
        join = anchor_join(&br_of_c("bdm")),
        wk_thread = thread_root("k"),
        tcl = climb("tcl", "tk", true, cap),
        tkids = kids("tcl"),
        reader_author = blocked("$1", "q.d"),
        author_owner = blocked("q.d", "q.o"),
        standing = standing("q.d", "q.o", "COALESCE(fv.ok, false)", false),
        owner_verified = follows_verified("q.o"),
        ok_s = walk_ok("s", "s.ctx", "s.owner", "s.gov"),
        tstop_r = tstop("r", "r.ctx", "r.owner"),
        x_thread = is_private("x.ctx"),
        tstop_e = tstop("e", "x.ctx", "x.owner"),
        ok_e = walk_ok("e", "x.ctx", "x.owner", "x.gov"),
    )
}

/// S12's statement as built before the shared walk (shared-walk T-60's
/// reference): one branch-root lookup and one walk per distinct key, as
/// correlated SubPlans.
#[cfg(test)]
pub(super) fn walked_per_key(c: &str, s1_filter: &str, final_: &str, cap: u32) -> String {
    walked_per_key_with(c, s1_filter, final_, cap, &stage1_ok())
}

/// The one-row form of [`walked`], for fact 8 (#720, B-5's ordinary
/// pages): with at most one row in `c` there is at most one key, so the
/// shared climb has nothing to share, and S12's per-key form gives the same
/// verdict (T-60 holds the two equal on every statement) with a smaller
/// plan. The oracle reads fact 8 once per pipeline read, so the larger
/// plan's executor start-up cost about 0.2 ms a read. Use only where `c`
/// holds at most one row: the per-key walk is not bounded by the chunk rule.
pub(super) fn walked_one(c: &str, s1_filter: &str, final_: &str, cap: u32) -> String {
    walked_per_key_with(c, s1_filter, final_, cap, &stage1_ok())
}

/// [`walked_per_key`] with stage 1's text given.
fn walked_per_key_with(c: &str, s1_filter: &str, final_: &str, cap: u32, stage1: &str) -> String {
    format!(
        "WITH c AS MATERIALIZED ({c}), \
         br AS MATERIALIZED ( \
           SELECT k.parent, k.root, {branch_root} AS branch_root \
           FROM (SELECT DISTINCT c.reply_parent_uri AS parent, c.reply_root_uri AS root \
                 FROM c WHERE {branch_reply}) k), \
         brm AS MATERIALIZED ( \
           SELECT COALESCE(jsonb_object_agg({br_key}, br.branch_root), '{{}}'::jsonb) AS m FROM br), \
         akm AS MATERIALIZED ({anchors}), \
         gm AS MATERIALIZED ({AUDIENCE_KEYS}), \
         s1 AS MATERIALIZED ( \
           SELECT c.*, an.uri AS anchor, an.author_did AS owner, an.audience_uri AS gov, \
                  COALESCE(CASE WHEN c.author_did = $1 THEN {gate} ELSE {stage1} END, false) AS s1ok, \
                  COALESCE({walks}, true) AS walks \
           FROM c {join} {s1_filter}), \
         pv AS MATERIALIZED ( \
           SELECT k.reply_parent_uri, k.reply_root_uri, COALESCE({verdict}, false) AS ok \
           FROM (SELECT DISTINCT s1.reply_parent_uri, s1.reply_root_uri, s1.owner, s1.gov \
                 FROM s1 WHERE s1.s1ok AND s1.walks AND s1.author_did <> $1) k \
           LEFT JOIN private_audiences a ON a.uri = k.gov \
           LEFT JOIN LATERAL (SELECT CASE WHEN a.mode = 'following' THEN {owner_verified} END AS ok) fv ON true), \
         pvm AS MATERIALIZED ( \
           SELECT COALESCE(jsonb_object_agg({pv_key}, pv.ok), '{{}}'::jsonb) AS m FROM pv \
           WHERE pv.reply_parent_uri IS NOT NULL AND pv.reply_root_uri IS NOT NULL) \
         {final_}",
        branch_root = branch_root_of("k.parent", "k.root", cap),
        branch_reply = is_branch_reply("c"),
        gate = gate("c.uri", "c.author_did", "po.account_status"),
        walks = needs_walk(),
        anchors = anchor_keys(s1_filter, &br_of_c("brm")),
        join = anchor_join(&br_of_c("brm")),
        verdict = parent_verdict(cap),
        owner_verified = follows_verified("k.owner"),
        br_key = pair_key("br.parent", "br.root"),
        pv_key = pair_key("pv.reply_parent_uri", "pv.reply_root_uri"),
    )
}

/// The facts the oracle reads from fact 8 (§3.2): O, G, stage 1, the
/// verdict without the reader's own arm, and whether the post has a valid
/// anchor of which its own audience is O's (§2.1, §2.6).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(super) struct Fact8 {
    pub owner: Option<String>,
    pub gov: Option<String>,
    pub s1ok: bool,
    pub verdict: bool,
    pub anchored: bool,
}

/// Fact 8's statement (§3.2): `$1` the reader, `$2` the post. One row
/// whenever the post is stored, with no WHERE on the verdict.
pub(super) fn fact8_statement(cap: u32) -> String {
    let c = format!("SELECT {COLS} FROM private_posts p WHERE p.uri = $2");
    walked_one(&c, "", &fact8_final(), cap)
}

/// Fact 8's final SELECT over [`PV_JOIN`], one row per `s1` row.
pub(super) fn fact8_final() -> String {
    format!(
        "SELECT s1.owner, s1.gov, s1.s1ok, COALESCE(s1.s1ok AND (NOT s1.walks OR pv.ok), false), \
           COALESCE(an.uri IS NOT NULL AND {role} AND {anchor_owned} AND {post_owned} AND {anchor_gate}, false) \
         FROM {PV_JOIN} LEFT JOIN private_posts an ON an.uri = s1.anchor",
        role = role_kind("s1", "an"),
        anchor_owned = owned("an.audience_uri", "an.author_did"),
        post_owned = owned("s1.audience_uri", "an.author_did"),
        anchor_gate = gate("an.uri", "an.author_did", "(SELECT x.account_status FROM actors x WHERE x.did = an.author_did)"),
    )
}

/// Fact 8 for `reader` and the post `uri`; the default when no row is
/// stored.
///
/// # Errors
///
/// Database errors.
pub(super) async fn fact8(pool: &PgPool, reader: &str, uri: &str, cap: u32) -> Result<Fact8, sqlx::Error> {
    type Row = (Option<String>, Option<String>, bool, bool, bool);
    let row: Option<Row> = sqlx::query_as(&fact8_statement(cap)).bind(reader).bind(uri).fetch_optional(pool).await?;
    Ok(row.map_or_else(Fact8::default, |(owner, gov, s1ok, verdict, anchored)| Fact8 {
        owner,
        gov,
        s1ok,
        verdict,
        anchored,
    }))
}

/// The batch loop (§3.2.1): a batched shape's `c` rows are evaluated in
/// batches, in the shape's key order, until `need` rows pass or the rows run
/// out. The first batch is `first` rows, and each later one twice the last,
/// every one at most `max`; the next batch starts after the last `c` row examined, whether
/// it passed or not. Every row passes or fails by the predicate it would face
/// alone, so the rows returned are the first `need` that pass, as
/// row-at-a-time evaluation gives.
///
/// # Errors
///
/// Database errors.
pub(super) async fn passing<R, K, F, Fut>(
    need: usize,
    first: i64,
    max: i64,
    mut after: Option<K>,
    key: impl Fn(&R) -> K,
    mut batch: F,
) -> Result<Vec<R>, sqlx::Error>
where
    F: FnMut(Option<K>, i64) -> Fut,
    Fut: std::future::Future<Output = Result<Vec<(R, bool)>, sqlx::Error>>,
{
    let mut out = Vec::new();
    // No batch is larger than `max` (shared-walk §2.5), the first included.
    let mut size = first.max(1).min(max.max(1));
    while out.len() < need {
        let rows = batch(after.take(), size).await?;
        let examined = rows.len();
        after = rows.last().map(|(r, _)| key(r));
        for (row, pass) in rows {
            if pass && out.len() < need {
                out.push(row);
            }
        }
        if i64::try_from(examined).unwrap_or(i64::MAX) < size || after.is_none() {
            break;
        }
        size = size.saturating_mul(2).min(max.max(1));
    }
    Ok(out)
}

/// The batch sizes of a batched read (`[private_read]`, §3.2.1) and the
/// walk's cap (§2.3).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct ReadBounds {
    /// `private_read.max_chain`.
    pub max_chain: u32,
    /// `private_read.walk_batch_max`, at most K (shared-walk §2.5).
    pub batch_max: i64,
}

impl ReadBounds {
    /// The bounds for `max_chain` and `walk_batch_max`: no batch holds more
    /// than [`climb_keys`] rows, each of which gives at most one start
    /// parent.
    pub(super) fn new(max_chain: u32, walk_batch_max: u32) -> Self {
        let k = i64::try_from(climb_keys(max_chain)).unwrap_or(i64::MAX);
        Self { max_chain, batch_max: i64::from(walk_batch_max.max(1)).min(k) }
    }
}

/// HEAD's §6.9 candidate prefilter before S12, kept for the B-5 and T-54
/// comparisons (private-threads design §4 part (1)), mirroring §6.4's rules
/// for the requester (`$1`), as the audience-modes and private-posting
/// designs amend them:
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
#[cfg(test)]
pub(super) fn head_prefilter() -> String {
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

/// The pre-S12 count (private-threads design §8), run read-only against the
/// deployed database before S12 ships. It compares, post by post, HEAD's
/// verdict and S12's for the audience as a whole, with no reader (`''`,
/// so no reader block denies, and rule 8 is left out): HEAD's is its
/// prefilter's gate and rule 4 at the cap of 8; S12's is stage 1 without
/// rule 8 and the walk at `max_chain` 16, and again at 100.
/// - **Stops** (HEAD serves, S12 does not), each row in the first that
///   fits: `stops_public_parent_private_root` (a public parent and a
///   private root); `stops_deeper_than_cap` (served by S12 at 100);
///   `stops_invalid_anchor` (no valid anchor: a missing, hidden or
///   wrong-kind root or branch root, or one under another account's
///   audience); `stops_own_audience_in_others_thread` (the post names an
///   audience not of the root's author); `stops_not_standing` (the rest: a
///   writer in the chain who does not stand under the root's current
///   audience, or a block with its author).
/// - **Starts** (S12 serves, HEAD does not): `starts_deep` (HEAD serves it
///   at a cap of 100); `starts_moved_thread` (the rest: the post or a step
///   names another of the owner's audiences than its root's current one).
/// - `starts_owner_reply_parent_independent`: owner thread replies S12
///   serves whose parent is missing, hidden by the gate, or of another
///   thread. Their read no longer depends on the parent.
///
/// Printed, with [`pre_s12_owner_audience_sql`], by the ignored test
/// `pre_s12_count_query`.
#[cfg(test)]
pub(super) fn pre_s12_count_sql() -> String {
    let head_may_write = "CASE COALESCE(a0.mode, 'list') \
           WHEN 'list' THEN p.author_did = ANY(a0.members) \
           WHEN 'followers' THEN EXISTS (SELECT 1 FROM follows f WHERE f.author_did = p.author_did AND f.subject_did = a0.owner_did) \
           WHEN 'everyone' THEN true \
           WHEN 'following' THEN {owner_follows} AND {verified} \
           ELSE false END"
        .replace("{owner_follows}", &owner_follows("a0.owner_did", "p.author_did"))
        .replace("{verified}", &follows_verified("a0.owner_did"));
    let head = |cap: u32| {
        format!(
            "(a0.uri IS NOT NULL AND {gate} AND (a0.owner_did = p.author_did OR (ao0.account_status = 'active' \
               AND NOT {author_owner} AND {head_may_write} AND {walk})))",
            gate = gate_shows("p.uri", "p.author_did", "po0.account_status"),
            author_owner = blocked("p.author_did", "a0.owner_did"),
            walk = anchored_with_cap("p.reply_parent_uri", "p.audience_uri", "a0.owner_did", "a0.mode", "a0.members", "''", cap),
        )
    };
    let c = format!(
        "SELECT {COLS}, {head8} AS head_ok, {head100} AS head_deep \
         FROM private_posts p LEFT JOIN private_audiences a0 ON a0.uri = p.audience_uri \
         LEFT JOIN actors po0 ON po0.did = p.author_did LEFT JOIN actors ao0 ON ao0.did = a0.owner_did",
        head8 = head(RULE4_MAX_HOPS),
        head100 = head(100),
    );
    let stage1 = stage1_terms(false).join(" AND ");
    let s12 = "COALESCE(s1.s1ok AND (NOT s1.walks OR pv.ok), false)";
    // The per-key reference on this thread when T-60 asks for it.
    let walked_with = if PER_KEY.with(std::cell::Cell::get) { walked_per_key_with } else { walked_with };
    let at16 = walked_with(
        &c,
        "",
        &format!(
            "SELECT s1.uri, s1.head_ok, s1.head_deep, {s12} AS s12, \
               (NOT {none_parent} AND {none_root}) AS none_kind, \
               COALESCE(an.uri IS NOT NULL AND {role} AND {anchor_owned} AND {anchor_gate}, false) AS anchor_valid, \
               COALESCE({post_owned}, false) AS post_owned, \
               ({thread_reply} AND s1.author_did = s1.owner) AS owner_thread_reply, \
               NOT EXISTS (SELECT 1 FROM private_posts pp WHERE pp.uri = s1.reply_parent_uri \
                 AND (pp.reply_root_uri = s1.reply_root_uri OR pp.uri = s1.reply_root_uri) AND {parent_gate}) AS parent_independent \
             FROM {PV_JOIN} LEFT JOIN private_posts an ON an.uri = s1.anchor",
            none_parent = is_private("s1.reply_parent_uri"),
            none_root = is_private("s1.reply_root_uri"),
            role = role_kind("s1", "an"),
            anchor_owned = owned("an.audience_uri", "an.author_did"),
            anchor_gate = gate_shows("an.uri", "an.author_did", "(SELECT x.account_status FROM actors x WHERE x.did = an.author_did)"),
            post_owned = owned("s1.audience_uri", "an.author_did"),
            thread_reply = is_thread_reply("s1"),
            parent_gate = gate_shows("pp.uri", "pp.author_did", "(SELECT x.account_status FROM actors x WHERE x.did = pp.author_did)"),
        ),
        16,
        &stage1,
    );
    let at100 = walked_with(&c, "", &format!("SELECT s1.uri, {s12} AS s12 FROM {PV_JOIN}"), 100, &stage1);
    format!(
        "WITH x AS ({at16}), deep AS ({at100}), y AS (
  SELECT x.*, deep.s12 AS s12_at_100,
         x.head_ok AND NOT x.s12 AS stop, NOT x.head_ok AND x.s12 AS start
  FROM x JOIN deep ON deep.uri = x.uri
)
SELECT
  count(*) FILTER (WHERE stop AND NOT none_kind AND NOT s12_at_100 AND anchor_valid AND post_owned) AS stops_not_standing,
  count(*) FILTER (WHERE stop AND NOT none_kind AND NOT s12_at_100 AND anchor_valid AND NOT post_owned) AS stops_own_audience_in_others_thread,
  count(*) FILTER (WHERE stop AND NOT none_kind AND NOT s12_at_100 AND NOT anchor_valid) AS stops_invalid_anchor,
  count(*) FILTER (WHERE stop AND none_kind) AS stops_public_parent_private_root,
  count(*) FILTER (WHERE stop AND NOT none_kind AND s12_at_100) AS stops_deeper_than_cap,
  count(*) FILTER (WHERE start AND NOT head_deep) AS starts_moved_thread,
  count(*) FILTER (WHERE start AND head_deep) AS starts_deep,
  count(*) FILTER (WHERE s12 AND owner_thread_reply AND parent_independent) AS starts_owner_reply_parent_independent
FROM y;"
    )
    .replace("$1", "''")
}

/// The pre-S12 owner-audience count (private-threads design §8), verbatim:
/// owner thread replies naming another of the owner's audiences than their
/// root's, and of those under `list` audiences, the narrower and the wider.
#[cfg(test)]
pub(super) fn pre_s12_owner_audience_sql() -> String {
    format!(
        "SELECT
  count(*) AS owner_replies_other_audience,
  count(*) FILTER (WHERE ra.mode IS NOT DISTINCT FROM 'list' AND ta.mode IS NOT DISTINCT FROM 'list'
                     AND cardinality(ra.members) < cardinality(ta.members)) AS list_narrower,
  count(*) FILTER (WHERE ra.mode IS NOT DISTINCT FROM 'list' AND ta.mode IS NOT DISTINCT FROM 'list'
                     AND cardinality(ra.members) > cardinality(ta.members)) AS list_wider
FROM private_posts r
JOIN private_posts t ON t.uri = r.reply_root_uri AND t.reply_parent_uri IS NULL
LEFT JOIN private_audiences ra ON ra.uri = r.audience_uri
LEFT JOIN private_audiences ta ON ta.uri = t.audience_uri
WHERE r.author_did = t.author_did
  AND split_part(r.reply_parent_uri, '/', 4) = '{NSID_POST_PRIVATE}'
  AND r.audience_uri IS DISTINCT FROM t.audience_uri
  AND split_part(r.audience_uri, '/', 3) = t.author_did;"
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

/// HEAD's prefilter joins before S12 (B-5 and T-54 comparisons).
#[cfg(test)]
pub(super) const PREFILTER_FROM: &str = "private_posts p \
     LEFT JOIN private_audiences a ON a.uri = p.audience_uri \
     LEFT JOIN actors po ON po.did = p.author_did \
     LEFT JOIN actors ao ON ao.did = a.owner_did";

/// `prefilter_uris`' statement (§3.2): `$1` the reader, `$2` the URIs.
pub(super) fn prefilter_uris_statement(cap: u32) -> String {
    walked(
        &format!("SELECT {COLS} FROM private_posts p WHERE p.uri = ANY($2)"),
        "",
        &format!("SELECT s1.uri FROM {PV_JOIN} WHERE {PASS}"),
        cap,
    )
}

/// The URIs among `uris` that pass the read rule's SQL mirror for
/// `requester` (the prefilter, §3.2).
///
/// # Errors
///
/// Database errors.
pub(super) async fn prefilter_uris(pool: &PgPool, requester: &str, uris: &[String], cap: u32) -> Result<Vec<String>, sqlx::Error> {
    prefilter_uris_sliced(pool, requester, uris, cap, climb_keys(cap)).await
}

/// [`prefilter_uris`] with K given (shared-walk §2.5): an input of more
/// than `k` URIs is read in slices of `k`, in one [`SLICED`] transaction.
///
/// # Errors
///
/// Database errors.
pub(super) async fn prefilter_uris_sliced(
    pool: &PgPool,
    requester: &str,
    uris: &[String],
    cap: u32,
    k: usize,
) -> Result<Vec<String>, sqlx::Error> {
    if uris.is_empty() {
        return Ok(Vec::new());
    }
    let sql = prefilter_uris_statement(cap);
    if uris.len() <= k {
        return sqlx::query_scalar(&sql).bind(requester).bind(uris).fetch_all(pool).await;
    }
    // Each URI once, so that no row is returned by two slices.
    let mut distinct = uris.to_vec();
    distinct.sort_unstable();
    distinct.dedup();
    let mut tx = pool.begin_with(SLICED).await?;
    let mut out = Vec::new();
    for slice in distinct.chunks(k.max(1)) {
        let rows: Vec<String> = sqlx::query_scalar(&sql).bind(requester).bind(slice).fetch_all(&mut *tx).await?;
        out.extend(rows);
    }
    tx.commit().await?;
    Ok(out)
}

/// How a statement split into slices begins (shared-walk §2.5): every
/// slice and the public term read one snapshot, in one round trip.
const SLICED: &str = "BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY";

/// `getReplies`' page statement (§3.2): `$1` the reader, `$2` the parent,
/// `$3` the hidden replies, `$4`/`$5` the position, `$6` the batch.
pub(super) fn reply_page_statement(cap: u32) -> String {
    walked(
        &format!(
            "SELECT {COLS}, p.created_at FROM private_posts p \
             WHERE p.reply_parent_uri = $2 AND NOT (p.uri = ANY($3)) \
               AND ($4::timestamptz IS NULL OR (p.created_at, p.uri COLLATE \"C\") > ($4, $5 COLLATE \"C\")) \
             ORDER BY p.created_at, p.uri COLLATE \"C\" LIMIT $6"
        ),
        "",
        &format!("SELECT s1.uri, s1.created_at, {PASS} FROM {PV_JOIN} ORDER BY s1.created_at, s1.uri COLLATE \"C\""),
        cap,
    )
}

/// One batch of `getReplies`' page (private posting §4.12): up to `limit`
/// private replies to `parent`, except `hidden`, oldest first by
/// `(created_at, uri)`, after the position `after`, each with its verdict.
///
/// # Errors
///
/// Database errors.
pub(super) async fn reply_page_batch(
    pool: &PgPool,
    requester: &str,
    parent: &str,
    hidden: &[String],
    after: Option<(DateTime<Utc>, String)>,
    limit: i64,
    cap: u32,
) -> Result<Vec<((String, DateTime<Utc>), bool)>, sqlx::Error> {
    let (at, uri) = after.map_or((None, String::new()), |(a, u)| (Some(a), u));
    let rows: Vec<(String, DateTime<Utc>, bool)> = sqlx::query_as(&reply_page_statement(cap))
        .bind(requester)
        .bind(parent)
        .bind(hidden)
        .bind(at)
        .bind(uri)
        .bind(limit)
        .fetch_all(pool)
        .await?;
    Ok(rows.into_iter().map(|(u, at, pass)| ((u, at), pass)).collect())
}

/// `searchPosts`' statement (§3.2): `$1` the reader, `$2`/`$3` the pair,
/// `$4` the lower bound, `$5` the batch. The order is
/// `private_posts_recent`'s, with the column's own collation.
pub(super) fn search_statement(cap: u32) -> String {
    walked(
        &format!(
            "SELECT {COLS}, p.created_at FROM private_posts p \
             WHERE (p.created_at, p.uri) < ($2, $3) AND p.created_at >= $4 \
             ORDER BY p.created_at DESC, p.uri DESC LIMIT $5"
        ),
        "",
        &format!("SELECT s1.uri, s1.created_at, {PASS} FROM {PV_JOIN} ORDER BY s1.created_at DESC, s1.uri DESC"),
        cap,
    )
}

/// One batch of `searchPosts`' candidates (private posting §4.17): up to
/// `limit` posts newest first by `(created_at, uri)`, strictly below the
/// pair `(at, uri)` and no older than `lower`, each with its verdict.
///
/// # Errors
///
/// Database errors.
pub(super) async fn search_batch(
    pool: &PgPool,
    requester: &str,
    (at, uri): (DateTime<Utc>, String),
    lower: DateTime<Utc>,
    limit: i64,
    cap: u32,
) -> Result<Vec<((String, DateTime<Utc>), bool)>, sqlx::Error> {
    let rows: Vec<(String, DateTime<Utc>, bool)> = sqlx::query_as(&search_statement(cap))
        .bind(requester)
        .bind(at)
        .bind(uri)
        .bind(lower)
        .bind(limit)
        .fetch_all(pool)
        .await?;
    Ok(rows.into_iter().map(|(u, at, pass)| ((u, at), pass)).collect())
}

/// The thread expansion's statement (§3.2): `$1` the reader, `$2` the
/// parent, `$3`/`$4` the position, `$5` the batch; HEAD's order.
pub(super) fn reply_candidates_statement(cap: u32) -> String {
    walked(
        &format!(
            "SELECT {COLS}, p.created_at FROM private_posts p \
             WHERE p.reply_parent_uri = $2 \
               AND ($3::timestamptz IS NULL OR (p.created_at, p.uri) > ($3, $4)) \
             ORDER BY p.created_at, p.uri LIMIT $5"
        ),
        "",
        &format!("SELECT s1.uri, s1.created_at, {PASS} FROM {PV_JOIN} ORDER BY s1.created_at, s1.uri"),
        cap,
    )
}

/// One batch of the replies to `parent`, oldest first, after `after`, each
/// with its verdict.
///
/// # Errors
///
/// Database errors.
pub(super) async fn reply_candidates_batch(
    pool: &PgPool,
    requester: &str,
    parent: &str,
    after: Option<(DateTime<Utc>, String)>,
    limit: i64,
    cap: u32,
) -> Result<Vec<((String, DateTime<Utc>), bool)>, sqlx::Error> {
    let (at, uri) = after.map_or((None, String::new()), |(a, u)| (Some(a), u));
    let rows: Vec<(String, DateTime<Utc>, bool)> = sqlx::query_as(&reply_candidates_statement(cap))
        .bind(requester)
        .bind(parent)
        .bind(at)
        .bind(uri)
        .bind(limit)
        .fetch_all(pool)
        .await?;
    Ok(rows.into_iter().map(|(u, at, pass)| ((u, at), pass)).collect())
}

/// Up to `limit` replies to `parent` that pass for `requester`, oldest
/// first, in batches (§3.2.1).
///
/// # Errors
///
/// Database errors.
pub(super) async fn reply_candidates(
    pool: &PgPool,
    requester: &str,
    parent: &str,
    limit: usize,
    bounds: ReadBounds,
) -> Result<Vec<String>, sqlx::Error> {
    let first = i64::try_from(limit).unwrap_or(i64::MAX);
    let rows = passing(limit, first, bounds.batch_max, None, |(u, at): &(String, DateTime<Utc>)| (*at, u.clone()), move |after, n| {
        reply_candidates_batch(pool, requester, parent, after, n, bounds.max_chain)
    })
    .await?;
    Ok(rows.into_iter().map(|(u, _)| u).collect())
}

/// The structural chain of `getPostThread` (§3.4 step 1): the anchor `$1`
/// and its stored ancestors by `reply_parent_uri`, at most `$2` of them,
/// nearest first, as `(uri, parent, root, hop)`. No reader; the anchor's
/// stored `reply_root_uri` comes with hop 0 only. Never exposed.
///
/// # Errors
///
/// Database errors.
pub(super) async fn ancestors(pool: &PgPool, anchor: &str, height: u32) -> Result<Vec<Ancestor>, sqlx::Error> {
    sqlx::query_as(&format!(
        "WITH RECURSIVE up(uri, parent, root, hop) AS ( \
           SELECT s.uri, s.reply_parent_uri, s.reply_root_uri, 0 FROM private_posts s WHERE s.uri = $1 \
           UNION ALL \
           SELECT s.uri, s.reply_parent_uri, NULL::text, u.hop + 1 FROM up u JOIN private_posts s ON s.uri = u.parent \
           WHERE u.hop < $2 AND {private} \
         ) \
         SELECT uri, parent, root, hop FROM up ORDER BY hop",
        private = is_private("u.parent"),
    ))
    .bind(anchor)
    .bind(i32::try_from(height).unwrap_or(i32::MAX))
    .fetch_all(pool)
    .await
}

/// A row of [`ancestors`]: `(uri, parent, root, hop)`.
pub(super) type Ancestor = (String, Option<String>, Option<String>, i32);

/// `getReplies`' claimed-root lookup (§3.4): the stored `reply_root_uri` of
/// `uri`, with no reader. `None` when no row is stored; `Some(None)` for a
/// top-level post.
///
/// # Errors
///
/// Database errors.
pub(super) async fn stored_root(pool: &PgPool, uri: &str) -> Result<Option<Option<String>>, sqlx::Error> {
    sqlx::query_scalar("SELECT reply_root_uri FROM private_posts WHERE uri = $1")
        .bind(uri)
        .fetch_optional(pool)
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

/// A feed shape's final SELECT (§3.2): every `c` row, its verdict, in the
/// merged key order.
fn feed_final() -> String {
    format!(
        "SELECT s1.uri, s1.created_at, s1.key, s1.repost_uri, s1.reposter, {PASS} FROM {PV_JOIN} \
         ORDER BY s1.created_at DESC, s1.key COLLATE \"C\" DESC"
    )
}

/// A feed row with its verdict.
type FeedRow = (String, DateTime<Utc>, String, Option<String>, Option<String>, bool);

fn judged_feed(rows: Vec<FeedRow>) -> Vec<(FeedCandidate, bool)> {
    rows.into_iter()
        .map(|(uri, created_at, key, repost_uri, reposter, pass)| {
            (FeedCandidate { uri, created_at, key, repost_uri, reposter }, pass)
        })
        .collect()
}

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

/// `getAuthorFeed`'s statement (§3.2): `$1` the reader, `$2` the actor,
/// `$3`/`$4` the position, `$5` the batch. `c` is HEAD's two arms, each with
/// its own conditions, keyset, order and limit, under the merged key.
pub(super) fn author_statement(own_threads: bool, cap: u32) -> String {
    let threads = if own_threads {
        "AND (p.reply_parent_uri IS NULL OR split_part(p.reply_root_uri, '/', 3) = $2)"
    } else {
        ""
    };
    let c = format!(
        "(SELECT {COLS}, p.created_at, p.rkey COLLATE \"C\" AS key, NULL::text AS repost_uri, NULL::text AS reposter \
          FROM private_posts p \
          WHERE p.author_did = $2 {threads} \
            AND ($3::timestamptz IS NULL OR (p.created_at, p.rkey COLLATE \"C\") < ($3, $4 COLLATE \"C\")) \
          ORDER BY p.created_at DESC, p.rkey COLLATE \"C\" DESC LIMIT $5) \
         UNION ALL \
         (SELECT {COLS}, rp.created_at, rp.uri COLLATE \"C\", rp.uri, rp.author_did \
          FROM reposts rp JOIN private_posts p ON p.uri = rp.subject_uri \
          WHERE rp.author_did = $2 AND {repost} \
            AND ($3::timestamptz IS NULL OR (rp.created_at, rp.uri COLLATE \"C\") < ($3, $4 COLLATE \"C\")) \
          ORDER BY rp.created_at DESC, rp.uri COLLATE \"C\" DESC LIMIT $5) \
         ORDER BY created_at DESC, key DESC LIMIT $5",
        repost = repost_item(),
    );
    walked(&c, "", &feed_final(), cap)
}

/// One batch of the actor's posts, and the actor's reposts of private posts
/// (private posting §4.7) when the actor is a member with no block either
/// way with the requester, newest first, after the position
/// `(created_at, key)`, each with its verdict (§3.2).
///
/// With `own_threads` (P-20's `posts_and_author_threads`, §4.14) the
/// actor's own posts are its top-level posts and its replies in threads
/// whose **claimed** root is the actor's (C-18); repost items are kept
/// under both filters, as on the stock feed.
///
/// # Errors
///
/// Database errors.
#[allow(clippy::too_many_arguments)]
pub(super) async fn author_batch(
    pool: &PgPool,
    requester: &str,
    author: &str,
    own_threads: bool,
    after: Option<(DateTime<Utc>, String)>,
    limit: i64,
    cap: u32,
) -> Result<Vec<(FeedCandidate, bool)>, sqlx::Error> {
    let (at, key) = after.map_or((None, String::new()), |(a, k)| (Some(a), k));
    let rows: Vec<FeedRow> = sqlx::query_as(&author_statement(own_threads, cap))
        .bind(requester)
        .bind(author)
        .bind(at)
        .bind(key)
        .bind(limit)
        .fetch_all(pool)
        .await?;
    Ok(judged_feed(rows))
}

/// `getTimeline`'s statement (§3.2): `$1` the reader, `$2`/`$3` the
/// position, `$4` the batch.
pub(super) fn timeline_statement(cap: u32) -> String {
    let c = format!(
        "(SELECT {COLS}, p.created_at, p.uri COLLATE \"C\" AS key, NULL::text AS repost_uri, NULL::text AS reposter \
          FROM private_posts p \
          WHERE (p.author_did = $1 OR p.author_did IN (SELECT f.subject_did FROM follows f WHERE f.author_did = $1)) \
            AND {post_muted} \
            AND ($2::timestamptz IS NULL OR (p.created_at, p.uri COLLATE \"C\") < ($2, $3 COLLATE \"C\")) \
          ORDER BY p.created_at DESC, p.uri COLLATE \"C\" DESC LIMIT $4) \
         UNION ALL \
         (SELECT {COLS}, rp.created_at, rp.uri COLLATE \"C\", rp.uri, rp.author_did \
          FROM reposts rp JOIN private_posts p ON p.uri = rp.subject_uri \
          WHERE (rp.author_did = $1 OR rp.author_did IN (SELECT f.subject_did FROM follows f WHERE f.author_did = $1)) \
            AND {repost} AND {reposter_muted} \
            AND ($2::timestamptz IS NULL OR (rp.created_at, rp.uri COLLATE \"C\") < ($2, $3 COLLATE \"C\")) \
          ORDER BY rp.created_at DESC, rp.uri COLLATE \"C\" DESC LIMIT $4) \
         ORDER BY created_at DESC, key DESC LIMIT $4",
        post_muted = not_muted("p.author_did"),
        repost = repost_item(),
        reposter_muted = not_muted("rp.author_did"),
    );
    walked(&c, "", &feed_final(), cap)
}

/// One batch of the home timeline's candidates (design-v3 §1.7 item 12;
/// private posting §4.7), newest first, after the `created_at::key` cursor,
/// each with its verdict (§3.2):
/// - posts whose author is the requester or an account the requester
///   follows, as this AppView has indexed the follows, keyed by URI (across
///   authors a record key is not unique);
/// - repost items: reposts by the requester or an account it follows, a
///   member (C-2), of a stored private post, keyed by the repost's URI.
///
/// A mute applies here as it does on the public timeline: a post's author,
/// or a reposter, that the requester fully mutes, directly or through a
/// moderation list that applies, is left out. So is a reposter with a block
/// either way.
///
/// # Errors
///
/// Database errors.
pub(super) async fn timeline_batch(
    pool: &PgPool,
    requester: &str,
    after: Option<(DateTime<Utc>, String)>,
    limit: i64,
    cap: u32,
) -> Result<Vec<(FeedCandidate, bool)>, sqlx::Error> {
    let (at, key) = after.map_or((None, String::new()), |(a, k)| (Some(a), k));
    let rows: Vec<FeedRow> = sqlx::query_as(&timeline_statement(cap))
        .bind(requester)
        .bind(at)
        .bind(key)
        .bind(limit)
        .fetch_all(pool)
        .await?;
    Ok(judged_feed(rows))
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
/// - `replyCount`: private replies passing the prefilter (§3.2), which
///   carries the moderation test; a reply that fails decode or waits on its
///   owner's readiness counts, since that fails for every reader alike.
/// - `likeCount`, `repostCount`: member likers (reposters) with no block
///   either way, each account once however many records it has (#727).
/// - `quoteCount`: private quotes passing the prefilter, and members'
///   public quotes whose author has no block either way.
///
/// # Errors
///
/// Database errors.
pub(super) async fn counts(pool: &PgPool, reader: &str, uris: &[String], cap: u32) -> Result<HashMap<String, Counts>, sqlx::Error> {
    counts_sliced(pool, reader, uris, cap, climb_keys(cap), None).await
}

/// [`counts`] with K given (shared-walk §2.5). `replyCount` reads `uris`
/// in slices of `k` past `k`; `quoteCount` always takes its keys, then its
/// slices of at most `k`, then the public references, each in one
/// [`SLICED`] transaction. `between`, tests only (T-63), is run on the pool
/// after the first quote slice.
///
/// # Errors
///
/// Database errors.
pub(super) async fn counts_sliced(
    pool: &PgPool,
    reader: &str,
    uris: &[String],
    cap: u32,
    k: usize,
    between: Option<&str>,
) -> Result<HashMap<String, Counts>, sqlx::Error> {
    let mut out: HashMap<String, Counts> = HashMap::new();
    if uris.is_empty() {
        return Ok(out);
    }
    let k = k.max(1);
    let reply_sql = reply_counts_statement(cap);
    let replies: Vec<(String, i64)> = if uris.len() <= k {
        sqlx::query_as(&reply_sql).bind(reader).bind(uris).fetch_all(pool).await?
    } else {
        // Each parent once, so that each parent's replies are all in its
        // own slice.
        let mut distinct = uris.to_vec();
        distinct.sort_unstable();
        distinct.dedup();
        let mut tx = pool.begin_with(SLICED).await?;
        let mut rows = Vec::new();
        for slice in distinct.chunks(k) {
            let part: Vec<(String, i64)> = sqlx::query_as(&reply_sql).bind(reader).bind(slice).fetch_all(&mut *tx).await?;
            rows.extend(part);
        }
        tx.commit().await?;
        rows
    };
    for (uri, n) in replies {
        out.entry(uri).or_default().reply = n;
    }
    for kind in [Interaction::Like, Interaction::Repost] {
        let rows: Vec<(String, i64)> = sqlx::query_as(&format!(
            "SELECT l.subject_uri, count(DISTINCT l.author_did) FROM {table} l WHERE l.subject_uri = ANY($2) AND {counted} GROUP BY l.subject_uri",
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
    // At most K keys: one statement, one snapshot, no slice.
    let bounded: Vec<(Option<String>, i64, bool)> =
        sqlx::query_as(&quote_counts_bounded_statement(cap, k)).bind(reader).bind(uris).fetch_all(pool).await?;
    if !bounded.iter().any(|(_, _, over)| *over) {
        for (uri, n, _) in bounded {
            if let Some(uri) = uri {
                out.entry(uri).or_default().quote = n;
            }
        }
        return Ok(out);
    }
    let mut tx = pool.begin_with(SLICED).await?;
    let keys: Vec<String> = sqlx::query_scalar(QUOTE_KEYS).bind(uris).fetch_all(&mut *tx).await?;
    let slice_sql = quote_counts_slice_statement(cap);
    let mut quotes: HashMap<String, i64> = HashMap::new();
    for (i, slice) in keys.chunks(k).enumerate() {
        let rows: Vec<(String, i64)> = sqlx::query_as(&slice_sql).bind(reader).bind(uris).bind(slice).fetch_all(&mut *tx).await?;
        for (uri, n) in rows {
            *quotes.entry(uri).or_default() += n;
        }
        if i == 0
            && let Some(sql) = between
        {
            sqlx::query(sql).execute(pool).await?;
        }
    }
    let refs: Vec<(String, i64)> = sqlx::query_as(&quote_refs_statement()).bind(reader).bind(uris).fetch_all(&mut *tx).await?;
    tx.commit().await?;
    for (uri, n) in refs {
        *quotes.entry(uri).or_default() += n;
    }
    for (uri, n) in quotes {
        out.entry(uri).or_default().quote = n;
    }
    Ok(out)
}

/// `k` as an SQL literal, kept inside `bigint`.
fn k_literal(k: usize) -> u64 {
    u64::try_from(k).unwrap_or(u64::MAX).min(1 << 40)
}

/// `quoteCount` in one statement when it has at most `k` private keys
/// (#720, B-5's ordinary pages): `c` reads at most `k` + 1 of them, and the
/// final answers HEAD's single statement when `c` holds at most `k`, else
/// one row flagged over, with nothing walked (the gate is a one-time
/// filter). One statement reads one snapshot, and no statement climbs from
/// more than K start parents, so §2.5's rule holds without the slices'
/// round trips. `$1` the reader, `$2` the quoted posts.
pub(super) fn quote_counts_bounded_statement(cap: u32, k: usize) -> String {
    let k = k_literal(k);
    walked(
        &format!("SELECT {COLS}, p.quote_uri FROM private_posts p WHERE p.quote_uri = ANY($2) LIMIT {}", k + 1),
        "",
        &format!(
            "SELECT x.quoted, x.n, false FROM ( \
               SELECT quoted, count(*) AS n FROM ( \
                 SELECT s1.quote_uri AS quoted FROM {PV_JOIN} WHERE {PASS} \
                 UNION ALL \
                 SELECT q.quoted_uri FROM private_quote_refs q WHERE q.quoted_uri = ANY($2) AND {counted} \
               ) y GROUP BY quoted) x \
             WHERE (SELECT count(*) FROM c) <= {k} \
             UNION ALL \
             SELECT NULL, 0, true WHERE (SELECT count(*) FROM c) > {k}",
            counted = counted_quote_ref(),
        ),
        cap,
    )
}

/// `quoteCount`'s keys (shared-walk §2.5): `$1` the quoted posts; one scan
/// of `private_posts_quote`.
const QUOTE_KEYS: &str = "SELECT p.uri FROM private_posts p WHERE p.quote_uri = ANY($1)";

/// One slice of `quoteCount`'s private term (§3.2; shared-walk §2.5): `$1`
/// the reader, `$2` the quoted posts, `$3` the slice of keys.
pub(super) fn quote_counts_slice_statement(cap: u32) -> String {
    walked(
        &format!("SELECT {COLS}, p.quote_uri FROM private_posts p WHERE p.quote_uri = ANY($2) AND p.uri = ANY($3)"),
        "",
        &format!("SELECT s1.quote_uri, count(*) FROM {PV_JOIN} WHERE {PASS} GROUP BY s1.quote_uri"),
        cap,
    )
}

/// `quoteCount`'s public term, HEAD's refs arm in its own statement: `$1`
/// the reader, `$2` the quoted posts.
pub(super) fn quote_refs_statement() -> String {
    format!(
        "SELECT q.quoted_uri, count(*) FROM private_quote_refs q WHERE q.quoted_uri = ANY($2) AND {counted} GROUP BY q.quoted_uri",
        counted = counted_quote_ref(),
    )
}

/// `replyCount`'s statement (§3.2): `$1` the reader, `$2` the parents; one
/// walk per distinct key, so siblings share it.
pub(super) fn reply_counts_statement(cap: u32) -> String {
    walked(
        &format!("SELECT {COLS} FROM private_posts p WHERE p.reply_parent_uri = ANY($2)"),
        "",
        &format!("SELECT s1.reply_parent_uri, count(*) FROM {PV_JOIN} WHERE {PASS} GROUP BY s1.reply_parent_uri"),
        cap,
    )
}

/// `quoteCount`'s single statement as it stood before the slices
/// (shared-walk T-60, T-63's reference): `$1` the reader, `$2` the quoted
/// posts.
#[cfg(test)]
pub(super) fn quote_counts_statement(cap: u32) -> String {
    walked(
        &format!("SELECT {COLS}, p.quote_uri FROM private_posts p WHERE p.quote_uri = ANY($2)"),
        "",
        &format!(
            "SELECT quoted, count(*) FROM ( \
               SELECT s1.quote_uri AS quoted FROM {PV_JOIN} WHERE {PASS} \
               UNION ALL \
               SELECT q.quoted_uri FROM private_quote_refs q WHERE q.quoted_uri = ANY($2) AND {counted} \
             ) x GROUP BY quoted",
            counted = counted_quote_ref(),
        ),
        cap,
    )
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

/// An interaction row `l` that is its author's newest of its subject, so a
/// listing shows each account once, as [`counts`] counts it (#727).
fn newest_of_account(table: &str) -> String {
    format!(
        "NOT EXISTS (SELECT 1 FROM {table} x WHERE x.subject_uri = l.subject_uri AND x.author_did = l.author_did \
           AND (x.created_at, x.uri) > (l.created_at, l.uri))"
    )
}

/// `getLikes` and `getRepostedBy` (private posting §4.7): the rows on
/// `subject` that count for `reader` (as [`counts`] counts them), one per
/// account, its newest, newest first, after the plain `created_at::uri`
/// cursor. They are public records of members; the caller has read
/// `subject`.
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
         WHERE l.subject_uri = $2 AND {counted} AND {newest} \
           AND ($3::timestamptz IS NULL OR (l.created_at, l.uri) < ($3, $4)) \
         ORDER BY l.created_at DESC, l.uri DESC LIMIT $5",
        table = kind.table(),
        counted = counted_interaction(),
        newest = newest_of_account(kind.table()),
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

/// `getQuotes`' statement (§3.2): `$1` the reader, `$2` the quoted post,
/// `$3`/`$4` the position, `$5` the batch. `c` is HEAD's two arms under a
/// tag `src`: the private arm (`'p'`) goes through stage 1 and the walk, and
/// the refs arm (`'q'`) keeps HEAD's own test and is not prefiltered. The
/// refs are merged in SQL.
pub(super) fn quote_statement(cap: u32) -> String {
    let c = format!(
        "(SELECT 'p' AS src, {COLS}, p.created_at, p.uri COLLATE \"C\" AS key, NULL::text AS cid \
          FROM private_posts p WHERE p.quote_uri = $2 \
            AND ($3::timestamptz IS NULL OR (p.created_at, p.uri COLLATE \"C\") < ($3, $4 COLLATE \"C\")) \
          ORDER BY p.created_at DESC, p.uri COLLATE \"C\" DESC LIMIT $5) \
         UNION ALL \
         (SELECT 'q', NULL::text, NULL::text, NULL::text, NULL::text, NULL::text, q.created_at, q.uri COLLATE \"C\", q.cid \
          FROM private_quote_refs q WHERE q.quoted_uri = $2 AND {counted} \
            AND ($3::timestamptz IS NULL OR (q.created_at, q.uri COLLATE \"C\") < ($3, $4 COLLATE \"C\")) \
          ORDER BY q.created_at DESC, q.uri COLLATE \"C\" DESC LIMIT $5) \
         ORDER BY created_at DESC, key DESC LIMIT $5",
        counted = counted_quote_ref(),
    );
    let final_ = format!(
        "SELECT x.key AS uri, x.created_at, x.cid, x.pass FROM ( \
           SELECT s1.key, s1.created_at, s1.cid, {PASS} AS pass FROM {PV_JOIN} \
           UNION ALL \
           SELECT c.key, c.created_at, c.cid, true FROM c WHERE c.src = 'q') x \
         ORDER BY x.created_at DESC, x.key COLLATE \"C\" DESC"
    );
    walked(&c, "WHERE c.src = 'p'", &final_, cap)
}

/// One batch of the posts quoting `quoted`, newest first, merged by
/// `(created_at, uri)` after the cursor, each with its verdict: private
/// posts by the read rule (§3.2), and public quote references whose author
/// has no block either way with `reader`.
///
/// # Errors
///
/// Database errors.
pub(super) async fn quote_batch(
    pool: &PgPool,
    reader: &str,
    quoted: &str,
    after: Option<(DateTime<Utc>, String)>,
    limit: i64,
    cap: u32,
) -> Result<Vec<(QuoteCandidate, bool)>, sqlx::Error> {
    let (at, uri) = after.map_or((None, String::new()), |(a, u)| (Some(a), u));
    let rows: Vec<(String, DateTime<Utc>, Option<String>, bool)> = sqlx::query_as(&quote_statement(cap))
        .bind(reader)
        .bind(quoted)
        .bind(at)
        .bind(uri)
        .bind(limit)
        .fetch_all(pool)
        .await?;
    Ok(rows
        .into_iter()
        .map(|(uri, created_at, cid, pass)| (QuoteCandidate { uri, created_at, cid }, pass))
        .collect())
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

/// The unseen-row condition of `getUnreadCount` for the recipient `$1`.
const UNSEEN: &str = "n.indexed_at > COALESCE((SELECT seen_at FROM private_notification_seen WHERE did = $1), '-infinity')";

/// `getUnreadCount`'s single statement as it stood before the slices
/// (shared-walk T-60, T-63's reference): `$1` the recipient. `c` is the
/// private-post rows of the unseen notifications, one per notification;
/// the second term is HEAD's public-record arm unchanged.
#[cfg(test)]
pub(super) fn unread_statement(cap: u32) -> String {
    let kept = notification_kept();
    walked(
        &format!(
            "SELECT {COLS}, n.id FROM private_notifications n JOIN private_posts p ON p.uri = n.record_uri \
             WHERE n.recipient_did = $1 AND {kept} AND {UNSEEN} \
               AND split_part(n.record_uri, '/', 4) = '{NSID_POST_PRIVATE}'"
        ),
        "",
        &format!(
            "SELECT (SELECT count(*) FROM {PV_JOIN} WHERE {PASS}) \
               + (SELECT count(*) FROM private_notifications n WHERE n.recipient_did = $1 AND {kept} AND {UNSEEN} \
                    AND split_part(n.record_uri, '/', 4) IS DISTINCT FROM '{NSID_POST_PRIVATE}' \
                    AND EXISTS (SELECT 1 FROM private_posts s WHERE s.uri = n.reason_subject))"
        ),
        cap,
    )
}

/// `getUnreadCount` (§4.8): the recipient's rows after its last-seen time,
/// under [`notification_kept`], counting only what the list would show:
/// - a row from a private post when the post passes the prefilter for the
///   recipient (§3.2), which carries the moderation test (§3.4), so a
///   mention in a post the recipient cannot read is never counted;
/// - a row from a public record while its subject private post is stored.
///
/// # Errors
///
/// Database errors.
pub(super) async fn unread_count(pool: &PgPool, recipient: &str, cap: u32) -> Result<i64, sqlx::Error> {
    unread_count_sliced(pool, recipient, cap, climb_keys(cap), None).await
}

/// [`unread_count`] with K given (shared-walk §2.5): the unseen
/// private-post rows' ids in one scan, then the walked statement over
/// slices of at most `k` ids, then the public-record term, in one
/// [`SLICED`] transaction. `between`, tests only (T-63), is run on the pool
/// after the first slice.
///
/// # Errors
///
/// Database errors.
pub(super) async fn unread_count_sliced(
    pool: &PgPool,
    recipient: &str,
    cap: u32,
    k: usize,
    between: Option<&str>,
) -> Result<i64, sqlx::Error> {
    // At most K candidates: one statement, one snapshot, no slice.
    let bounded: Option<i64> = sqlx::query_scalar(&unread_bounded_statement(cap, k)).bind(recipient).fetch_one(pool).await?;
    if let Some(n) = bounded {
        return Ok(n);
    }
    let mut tx = pool.begin_with(SLICED).await?;
    let ids: Vec<i64> = sqlx::query_scalar(&unread_keys_statement()).bind(recipient).fetch_all(&mut *tx).await?;
    let slice_sql = unread_slice_statement(cap);
    let mut total = 0i64;
    for (i, slice) in ids.chunks(k.max(1)).enumerate() {
        let n: i64 = sqlx::query_scalar(&slice_sql).bind(recipient).bind(slice).fetch_one(&mut *tx).await?;
        total += n;
        if i == 0
            && let Some(sql) = between
        {
            sqlx::query(sql).execute(pool).await?;
        }
    }
    let public: i64 = sqlx::query_scalar(&unread_public_statement()).bind(recipient).fetch_one(&mut *tx).await?;
    tx.commit().await?;
    Ok(total + public)
}

/// `getUnreadCount` in one statement when it has at most `k` private-post
/// candidates (#720, as [`quote_counts_bounded_statement`]): `c` reads at
/// most `k` + 1, and the count is NULL, with nothing walked, when it holds
/// more. `$1` the recipient.
pub(super) fn unread_bounded_statement(cap: u32, k: usize) -> String {
    let k = k_literal(k);
    let kept = notification_kept();
    walked(
        &format!(
            "SELECT {COLS}, n.id FROM private_notifications n JOIN private_posts p ON p.uri = n.record_uri \
             WHERE n.recipient_did = $1 AND {kept} AND {UNSEEN} \
               AND split_part(n.record_uri, '/', 4) = '{NSID_POST_PRIVATE}' LIMIT {}",
            k + 1
        ),
        "",
        &format!(
            "SELECT CASE WHEN (SELECT count(*) FROM c) > {k} THEN NULL ELSE \
               (SELECT count(*) FROM {PV_JOIN} WHERE {PASS}) \
               + (SELECT count(*) FROM private_notifications n WHERE n.recipient_did = $1 AND {kept} AND {UNSEEN} \
                    AND split_part(n.record_uri, '/', 4) IS DISTINCT FROM '{NSID_POST_PRIVATE}' \
                    AND EXISTS (SELECT 1 FROM private_posts s WHERE s.uri = n.reason_subject)) END"
        ),
        cap,
    )
}

/// The unseen private-post rows of `getUnreadCount`'s recipient `$1`
/// (shared-walk §2.5): one scan of `private_notifications_recipient`,
/// without [`notification_kept`].
pub(super) fn unread_keys_statement() -> String {
    format!(
        "SELECT n.id FROM private_notifications n WHERE n.recipient_did = $1 AND {UNSEEN} \
           AND split_part(n.record_uri, '/', 4) = '{NSID_POST_PRIVATE}'"
    )
}

/// One slice of `getUnreadCount`'s private term (§3.2; shared-walk §2.5):
/// `$1` the recipient, `$2` the slice of ids; `c` is the private-post rows
/// of the unseen notifications, one per notification, every condition of
/// the single statement kept.
pub(super) fn unread_slice_statement(cap: u32) -> String {
    walked(
        &format!(
            "SELECT {COLS}, n.id FROM private_notifications n JOIN private_posts p ON p.uri = n.record_uri \
             WHERE n.recipient_did = $1 AND n.id = ANY($2) AND {kept} AND {UNSEEN} \
               AND split_part(n.record_uri, '/', 4) = '{NSID_POST_PRIVATE}'",
            kept = notification_kept(),
        ),
        "",
        &format!("SELECT count(*) FROM {PV_JOIN} WHERE {PASS}"),
        cap,
    )
}

/// `getUnreadCount`'s public-record term, HEAD's arm in its own statement:
/// `$1` the recipient.
pub(super) fn unread_public_statement() -> String {
    format!(
        "SELECT count(*) FROM private_notifications n WHERE n.recipient_did = $1 AND {kept} AND {UNSEEN} \
           AND split_part(n.record_uri, '/', 4) IS DISTINCT FROM '{NSID_POST_PRIVATE}' \
           AND EXISTS (SELECT 1 FROM private_posts s WHERE s.uri = n.reason_subject)",
        kept = notification_kept(),
    )
}
