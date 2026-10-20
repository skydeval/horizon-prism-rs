-- Media filters on the private author feed (design private-media-filters.md
-- §2, §4): a private post's media kinds, as bits, for
-- `posts_with_media` (1, images or a gallery) and `posts_with_video` (2).
--
-- - `media_kinds` is a structural fact, like `quote_uri`: ingest derives it
--   from the clear `mediaKinds` field, else from the clear `embed`
--   (`private_tier::media`). 0 is no media, or not known.
-- - The backfill below derives it for stored rows from their clear `embed`,
--   the one place outside the per-record witness that reads `embed`: a
--   one-shot derivation of the same fact ingest derives from the same value,
--   recorded as an exception in private-posting §3.1. Migrations run at
--   start, before the firehose and the handlers. Rows without a clear embed
--   stay 0 until their PDS writes them again with `mediaKinds`.
-- - `private_embed_media_kinds` is the Rust classifier in SQL; tests hold
--   the two equal over every embed fixture. Nothing at run time calls it.
--
-- The ADD COLUMN has a constant default and rewrites no row. The two partial
-- indexes hold only media posts and are built here, not concurrently, as
-- 0023's were.
ALTER TABLE private_posts ADD COLUMN media_kinds smallint NOT NULL DEFAULT 0;

CREATE FUNCTION private_embed_media_kinds(e jsonb) RETURNS smallint LANGUAGE sql IMMUTABLE AS $$
  SELECT CASE
    WHEN k IN ('app.bsky.embed.images', 'app.bsky.embed.gallery',
               'tools.kryphocron.feed.postPublic#embedImage', 'tools.kryphocron.feed.postPublic#embedGallery') THEN 1
    WHEN k IN ('app.bsky.embed.video', 'tools.kryphocron.feed.postPublic#embedVideo') THEN 2
    ELSE 0 END::smallint
  FROM (SELECT CASE WHEN e->>'$type' IN ('app.bsky.embed.recordWithMedia',
                                         'tools.kryphocron.feed.postPublic#embedRecordWithMedia')
                    THEN e->'media'->>'$type' ELSE e->>'$type' END AS k) t
$$;

UPDATE private_posts SET media_kinds = private_embed_media_kinds(embed)
  WHERE embed IS NOT NULL AND private_embed_media_kinds(embed) <> 0;

CREATE INDEX private_posts_author_image ON private_posts (author_did, created_at DESC, rkey COLLATE "C" DESC)
  WHERE (media_kinds & 1) <> 0;
CREATE INDEX private_posts_author_video ON private_posts (author_did, created_at DESC, rkey COLLATE "C" DESC)
  WHERE (media_kinds & 2) <> 0;
