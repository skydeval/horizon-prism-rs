//! Compact `posts` and `feed_items`: the embed kinds the media filters
//! read, the feed item kinds, and the SQL every reader of a post row shares.
//!
//! A post row is `(author, rkey)`; its reply root, reply parent and quote
//! are references `(*_author, *_coll, *_rkey)`, odd ones with their text in
//! `odd_refs` (tbl 1, slots 1 to 3). The search columns live in
//! `post_search`, keyed like `posts`.

use serde_json::Value;

use super::ids::{COLL_POST, OddTable, Slot};

/// `posts.embed_kind`: what a post embeds. Migration 0057 classifies the
/// stored rows with the same table.
pub const EMBED_NONE: i16 = 0;
pub const EMBED_IMAGES: i16 = 1;
pub const EMBED_VIDEO: i16 = 2;
pub const EMBED_GALLERY: i16 = 3;
pub const EMBED_EXTERNAL: i16 = 4;
pub const EMBED_RECORD: i16 = 5;
pub const EMBED_RECORD_IMAGES: i16 = 6;
pub const EMBED_RECORD_VIDEO: i16 = 7;
pub const EMBED_RECORD_GALLERY: i16 = 8;
pub const EMBED_RECORD_EXTERNAL: i16 = 9;
/// A record with media of another kind, or none.
pub const EMBED_RECORD_OTHER: i16 = 10;

/// The kinds the `posts_with_media` filter keeps: images or a gallery, on
/// their own or beside a quoted record.
pub const MEDIA_KINDS: [i16; 4] = [EMBED_IMAGES, EMBED_GALLERY, EMBED_RECORD_IMAGES, EMBED_RECORD_GALLERY];
/// The kinds the `posts_with_video` filter keeps.
pub const VIDEO_KINDS: [i16; 2] = [EMBED_VIDEO, EMBED_RECORD_VIDEO];
/// The kinds Refract's `posts_with_media` filter keeps: images or video,
/// and any record with media.
pub const REFRACT_MEDIA_KINDS: [i16; 7] =
    [EMBED_IMAGES, EMBED_VIDEO, EMBED_RECORD_IMAGES, EMBED_RECORD_VIDEO, EMBED_RECORD_GALLERY, EMBED_RECORD_EXTERNAL, EMBED_RECORD_OTHER];
/// Every kind that carries images, video or a gallery (search `has:media`).
pub const ANY_MEDIA_KINDS: [i16; 6] = [EMBED_IMAGES, EMBED_VIDEO, EMBED_GALLERY, EMBED_RECORD_IMAGES, EMBED_RECORD_VIDEO, EMBED_RECORD_GALLERY];

/// The kind of a post's `embed` (the record's, absent or null for none).
#[must_use]
pub fn embed_kind(embed: Option<&Value>) -> i16 {
    let Some(embed) = embed else {
        return EMBED_NONE;
    };
    let of = |v: &Value| v.get("$type").and_then(Value::as_str).map(str::to_string);
    match of(embed).as_deref() {
        Some("app.bsky.embed.images") => EMBED_IMAGES,
        Some("app.bsky.embed.video") => EMBED_VIDEO,
        Some("app.bsky.embed.gallery") => EMBED_GALLERY,
        Some("app.bsky.embed.external") => EMBED_EXTERNAL,
        Some("app.bsky.embed.record") => EMBED_RECORD,
        Some("app.bsky.embed.recordWithMedia") => match embed.get("media").and_then(of).as_deref() {
            Some("app.bsky.embed.images") => EMBED_RECORD_IMAGES,
            Some("app.bsky.embed.video") => EMBED_RECORD_VIDEO,
            Some("app.bsky.embed.gallery") => EMBED_RECORD_GALLERY,
            Some("app.bsky.embed.external") => EMBED_RECORD_EXTERNAL,
            _ => EMBED_RECORD_OTHER,
        },
        _ => EMBED_NONE,
    }
}

/// `feed_items.kind`.
pub const FEED_POST: i16 = 1;
pub const FEED_REPLY: i16 = 2;
pub const FEED_REPOST: i16 = 3;

/// A feed item kind's name, as views and filters name it.
#[must_use]
pub fn feed_kind_name(kind: i16) -> &'static str {
    match kind {
        FEED_REPLY => "reply",
        FEED_REPOST => "repost",
        _ => "post",
    }
}

/// A feed item kind from its name.
#[must_use]
pub fn feed_kind(name: &str) -> i16 {
    match name {
        "reply" => FEED_REPLY,
        "repost" => FEED_REPOST,
        _ => FEED_POST,
    }
}

/// SQL: the URI of the post row `alias`.
#[must_use]
pub fn uri_sql(alias: &str) -> String {
    format!("at_uri({alias}.author, {COLL_POST}::smallint, {alias}.rkey)")
}

/// SQL: the URI text of the reference `slot` (root, parent or quote) of the
/// post row `alias`, from its key or, when odd, from `odd_refs`. NULL when
/// the post has none.
#[must_use]
pub fn ref_sql(alias: &str, slot: Slot) -> String {
    let column = match slot {
        Slot::Root => "root",
        Slot::Quote => "quote",
        _ => "parent",
    };
    format!(
        "ref_text({alias}.{column}_author, {alias}.{column}_coll, {alias}.{column}_rkey, {}::smallint, {alias}.author, {alias}.rkey, {}::smallint)",
        OddTable::Posts.id(),
        slot.id()
    )
}

/// SQL: the author DID of the post row `alias`.
#[must_use]
pub fn author_sql(alias: &str) -> String {
    format!("(SELECT d.did FROM dids d WHERE d.id = {alias}.author)")
}

/// SQL: the post rows named by the URI array `$n` (alias `p`, with `u` the
/// URI that named each). A URI that is not a whole stored post names none.
#[must_use]
pub fn rows_named(param: u32) -> String {
    format!(
        "unnest(${param}::text[]) AS u(uri) CROSS JOIN LATERAL ref_of(u.uri) k \
         JOIN posts p ON k.coll = {COLL_POST} AND p.author = k.author AND p.rkey = k.rkey"
    )
}

/// SQL: the columns of a `FeedItem` from the `feed_items` row `f`.
pub const FEED_ITEM_COLUMNS: &str = "at_uri(f.originator, CASE WHEN f.is_repost THEN 3 ELSE 1 END::smallint, f.src_rkey) AS uri, \
     (SELECT d.did FROM dids d WHERE d.id = f.originator) AS originator_did, \
     at_uri(f.post_author, 1::smallint, f.post_rkey) AS post_uri, \
     CASE f.kind WHEN 2 THEN 'reply' WHEN 3 THEN 'repost' ELSE 'post' END AS kind, f.sort_at";

/// SQL: the order of a feed of `feed_items` rows `f`: newest first, ties by
/// key.
pub const FEED_ORDER: &str = "ORDER BY f.sort_at DESC, f.originator DESC, f.is_repost DESC, f.src_rkey DESC";

/// SQL: whether the `feed_items` row `f` comes after a local cursor's
/// position, the time `$at` and the item URI `$uri` (no cursor when `$at` is
/// NULL). A URI that names no key sorts below every row of its time. The
/// bound on the time alone lets an index on it start at the cursor.
#[must_use]
pub fn feed_after(at: u32, uri: u32) -> String {
    format!(
        "(f.sort_at <= COALESCE(${at}::timestamptz, 'infinity') \
         AND (${at}::timestamptz IS NULL OR (f.sort_at, f.originator, f.is_repost, f.src_rkey) < (${at}, \
           COALESCE((SELECT r.author FROM ref_of(${uri}) r), '-2147483648'::integer), \
           COALESCE((SELECT r.coll = 3 FROM ref_of(${uri}) r), false), \
           COALESCE((SELECT r.rkey FROM ref_of(${uri}) r), '-9223372036854775808'::bigint))))"
    )
}

/// SQL: whether the post row `alias` comes before (`<`) or after (`>`) a
/// local cursor's position, the time `$at` and the post URI `$uri`, in
/// `(created_at, author, rkey)` order (no cursor when `$at` is NULL).
#[must_use]
pub fn post_beyond(alias: &str, op: &str, at: u32, uri: u32) -> String {
    let (author, rkey, bound, open) = if op == "<" {
        ("'-2147483648'::integer", "'-9223372036854775808'::bigint", "<=", "infinity")
    } else {
        ("2147483647", "9223372036854775807::bigint", ">=", "-infinity")
    };
    format!(
        "({alias}.created_at {bound} COALESCE(${at}::timestamptz, '{open}') \
         AND (${at}::timestamptz IS NULL OR ({alias}.created_at, {alias}.author, {alias}.rkey) {op} (${at}, \
           COALESCE((SELECT r.author FROM ref_of(${uri}) r), {author}), \
           COALESCE((SELECT r.rkey FROM ref_of(${uri}) r), {rkey}))))"
    )
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn embed_kinds() {
        let of = |e: Value| embed_kind(Some(&e));
        assert_eq!(embed_kind(None), EMBED_NONE);
        assert_eq!(of(json!({"$type": "app.bsky.embed.images"})), EMBED_IMAGES);
        assert_eq!(of(json!({"$type": "app.bsky.embed.video"})), EMBED_VIDEO);
        assert_eq!(of(json!({"$type": "app.bsky.embed.gallery"})), EMBED_GALLERY);
        assert_eq!(of(json!({"$type": "app.bsky.embed.external"})), EMBED_EXTERNAL);
        assert_eq!(of(json!({"$type": "app.bsky.embed.record"})), EMBED_RECORD);
        let with = |media: Value| of(json!({"$type": "app.bsky.embed.recordWithMedia", "media": media}));
        assert_eq!(with(json!({"$type": "app.bsky.embed.images"})), EMBED_RECORD_IMAGES);
        assert_eq!(with(json!({"$type": "app.bsky.embed.video"})), EMBED_RECORD_VIDEO);
        assert_eq!(with(json!({"$type": "app.bsky.embed.gallery"})), EMBED_RECORD_GALLERY);
        assert_eq!(with(json!({"$type": "app.bsky.embed.external"})), EMBED_RECORD_EXTERNAL);
        assert_eq!(with(json!({"$type": "app.example.other"})), EMBED_RECORD_OTHER);
        assert_eq!(of(json!({"$type": "app.example.other"})), EMBED_NONE);
        assert_eq!(of(json!("not an object")), EMBED_NONE);
        for name in ["post", "reply", "repost"] {
            assert_eq!(feed_kind_name(feed_kind(name)), name);
        }
    }
}
