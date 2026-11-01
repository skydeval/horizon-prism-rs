-- Compact storage: `posts`, `post_search`, `feed_items` and
-- `record_tombstones` keyed by interned ids.
--
-- A post row is `(author, rkey)`. Its reply root, reply parent and quote are
-- references `(*_author, *_coll, *_rkey)`; one that `at_uri` would not give
-- back exactly is odd: `*_coll = 0`, its text in `odd_refs`, and `*_author`
-- kept when its authority is a valid DID. The record is kept whole as
-- `dict id ‖ payload`, dict 0 (the JSON text) here; a background job
-- compresses it later. The CID is bytes, the rev an integer in rev order.
-- `embed_kind` names the embed for the media filters and `mention_ids` the
-- mentioned accounts for the threadgate mention rule. The search columns
-- move to `post_search`, keyed like `posts`, kept for the authors in S and
-- members, whose row goes with its post.
--
-- `feed_items` is keyed `(originator, is_repost, src_rkey)` and names its
-- post by key: a row whose post is not a whole post reference is dropped
-- and counted in the log. `record_tombstones` is keyed by the record's
-- `(author, coll, rkey)`.
--
-- One transaction: intern, copy, then indexes and triggers, then the swap.
-- A row that cannot be keyed (an author that is not a valid DID, a record
-- key that is not valid, a URI that is not its author's own record, or a
-- tombstone of a collection outside the fixed list) is left out and counted
-- in the log. Ingest checks all of these, so none are expected.

-- Every valid DID the tables name: authors, the authorities of references,
-- mentioned accounts, feed item originators and tombstoned records' authors.
INSERT INTO dids (did)
SELECT x.d FROM (
  SELECT author_did AS d FROM posts
  UNION SELECT split_part(substr(reply_root_uri, 6), '/', 1) FROM posts WHERE left(reply_root_uri, 5) = 'at://'
  UNION SELECT split_part(substr(reply_parent_uri, 6), '/', 1) FROM posts WHERE left(reply_parent_uri, 5) = 'at://'
  UNION SELECT split_part(substr(quote_uri, 6), '/', 1) FROM posts WHERE left(quote_uri, 5) = 'at://'
  UNION SELECT unnest(mention_dids) FROM posts WHERE mention_dids IS NOT NULL
  UNION SELECT originator_did FROM feed_items
  UNION SELECT split_part(substr(post_uri, 6), '/', 1) FROM feed_items WHERE left(post_uri, 5) = 'at://'
  UNION SELECT split_part(substr(uri, 6), '/', 1) FROM record_tombstones WHERE left(uri, 5) = 'at://'
) x
WHERE did_is_valid(x.d) AND NOT EXISTS (SELECT 1 FROM dids a WHERE a.did = x.d)
ORDER BY x.d
ON CONFLICT (did) DO NOTHING;

-- Every reference text the copy reads that could be whole (three parts, a
-- listed collection).
CREATE TEMP TABLE refs_named ON COMMIT DROP AS
SELECT u AS uri FROM (
  SELECT reply_root_uri AS u FROM posts WHERE reply_root_uri IS NOT NULL
  UNION SELECT reply_parent_uri FROM posts WHERE reply_parent_uri IS NOT NULL
  UNION SELECT quote_uri FROM posts WHERE quote_uri IS NOT NULL
  UNION SELECT post_uri FROM feed_items
) r
WHERE left(u, 5) = 'at://'
  AND cardinality(string_to_array(substr(u, 6), '/')) = 3
  AND split_part(substr(u, 6), '/', 2) IN (SELECT nsid FROM collections);

-- Aliases for every valid record key that is not a TID with its top bit
-- clear: the posts' own keys, the keys of references, feed items' keys and
-- tombstoned records' keys.
INSERT INTO rkey_aliases (author, rkey)
SELECT DISTINCT d.id, k.rkey FROM (
  SELECT author_did AS did, rkey FROM posts
  UNION SELECT split_part(substr(uri, 6), '/', 1), split_part(substr(uri, 6), '/', 3) FROM refs_named
  UNION SELECT originator_did, split_part(uri, '/', 5) FROM feed_items
  UNION SELECT split_part(substr(uri, 6), '/', 1), split_part(substr(uri, 6), '/', 3) FROM record_tombstones
   WHERE left(uri, 5) = 'at://' AND cardinality(string_to_array(substr(uri, 6), '/')) = 3
) k
JOIN dids d ON d.did = k.did
WHERE rkey_is_valid(k.rkey) AND rkey_to_bigint(k.rkey) IS NULL
  AND NOT EXISTS (SELECT 1 FROM rkey_aliases x WHERE x.author = d.id AND x.rkey = k.rkey)
ORDER BY 1, 2
ON CONFLICT (author, rkey) DO NOTHING;

-- The embed's kind, as `db/posts.rs` names it: 0 none (or a kind no filter
-- reads), 1 images, 2 video, 3 gallery, 4 external, 5 record, 6 to 9 a
-- record with images, video, a gallery or an external link, and 10 a record
-- with media of another kind or none.
CREATE FUNCTION pg_temp.embed_kind(e jsonb) RETURNS smallint LANGUAGE sql IMMUTABLE AS $$
  SELECT (CASE e->>'$type'
    WHEN 'app.bsky.embed.images' THEN 1
    WHEN 'app.bsky.embed.video' THEN 2
    WHEN 'app.bsky.embed.gallery' THEN 3
    WHEN 'app.bsky.embed.external' THEN 4
    WHEN 'app.bsky.embed.record' THEN 5
    WHEN 'app.bsky.embed.recordWithMedia' THEN
      CASE e->'media'->>'$type'
        WHEN 'app.bsky.embed.images' THEN 6
        WHEN 'app.bsky.embed.video' THEN 7
        WHEN 'app.bsky.embed.gallery' THEN 8
        WHEN 'app.bsky.embed.external' THEN 9
        ELSE 10 END
    ELSE 0 END)::smallint
$$;

-- The old posts with their new keys.
CREATE TEMP TABLE posts_keyed ON COMMIT DROP AS
SELECT a.id AS author, k.key AS rkey, p.author_did, p.cid, p.rev, p.created_at, p.indexed_at, p.reply_root_uri, p.reply_parent_uri,
       p.quote_uri, p.embed, p.mention_dids, p.record, p.text, p.search_tsv, p.tags, p.link_domains, p.link_urls,
       p.lang_prefixes, tid_is_valid(p.rev) AS rev_valid
FROM posts p
JOIN dids a ON a.did = p.author_did
CROSS JOIN LATERAL (
  SELECT COALESCE(rkey_to_bigint(p.rkey), (SELECT x.id FROM rkey_aliases x WHERE x.author = a.id AND x.rkey = p.rkey)) AS key
) k
WHERE rkey_is_valid(p.rkey) AND k.key IS NOT NULL
  AND p.uri = 'at://' || p.author_did || '/app.bsky.feed.post/' || p.rkey;

-- The new tables, without triggers. Every CID is kept in its exact text
-- form (0x00 then the text) and every record as dict 0.
CREATE TABLE posts_compact (
  author        integer     NOT NULL,
  rkey          bigint      NOT NULL,
  cid           bytea       NOT NULL,
  rev           bigint      NOT NULL,
  created_at    timestamptz NOT NULL,
  indexed_at    timestamptz NOT NULL DEFAULT now(),
  root_author   integer,
  root_coll     smallint,
  root_rkey     bigint,
  parent_author integer,
  parent_coll   smallint,
  parent_rkey   bigint,
  quote_author  integer,
  quote_coll    smallint,
  quote_rkey    bigint,
  embed_kind    smallint    NOT NULL DEFAULT 0,
  mention_ids   integer[],
  record        bytea       NOT NULL
);

INSERT INTO posts_compact (author, rkey, cid, rev, created_at, indexed_at, root_author, root_coll, root_rkey,
  parent_author, parent_coll, parent_rkey, quote_author, quote_coll, quote_rkey, embed_kind, mention_ids, record)
SELECT k.author, k.rkey, '\x00'::bytea || convert_to(k.cid, 'UTF8'), rev_to_bigint(k.rev), k.created_at, k.indexed_at,
       ro.author, CASE WHEN k.reply_root_uri IS NULL THEN NULL ELSE COALESCE(ro.coll, 0) END, ro.rkey,
       pa.author, CASE WHEN k.reply_parent_uri IS NULL THEN NULL ELSE COALESCE(pa.coll, 0) END, pa.rkey,
       qu.author, CASE WHEN k.quote_uri IS NULL THEN NULL ELSE COALESCE(qu.coll, 0) END, qu.rkey,
       pg_temp.embed_kind(k.embed),
       NULLIF(ARRAY(SELECT d.id FROM unnest(k.mention_dids) WITH ORDINALITY m(did, i) JOIN dids d ON d.did = m.did ORDER BY m.i), '{}'),
       '\x00'::bytea || convert_to(k.record::text, 'UTF8')
FROM posts_keyed k
LEFT JOIN LATERAL ref_of(k.reply_root_uri) ro ON true
LEFT JOIN LATERAL ref_of(k.reply_parent_uri) pa ON true
LEFT JOIN LATERAL ref_of(k.quote_uri) qu ON true;

-- The odd texts (tbl 1: posts; slot 1: root, 2: parent, 3: quote).
INSERT INTO odd_refs (tbl, author, rkey, slot, raw)
SELECT 1, k.author, k.rkey, 1, k.reply_root_uri
FROM posts_keyed k JOIN posts_compact c ON c.author = k.author AND c.rkey = k.rkey
WHERE c.root_coll = 0
UNION ALL
SELECT 1, k.author, k.rkey, 2, k.reply_parent_uri
FROM posts_keyed k JOIN posts_compact c ON c.author = k.author AND c.rkey = k.rkey
WHERE c.parent_coll = 0
UNION ALL
SELECT 1, k.author, k.rkey, 3, k.quote_uri
FROM posts_keyed k JOIN posts_compact c ON c.author = k.author AND c.rkey = k.rkey
WHERE c.quote_coll = 0;

-- What search needs, beside the post, for the authors in S and members
-- only. A row the old column backfill never reached gets its text vector
-- here.
CREATE TABLE post_search_compact (
  author        integer     NOT NULL,
  rkey          bigint      NOT NULL,
  created_at    timestamptz NOT NULL,
  text          text        NOT NULL DEFAULT '',
  search_tsv    tsvector,
  tags          text[],
  link_domains  text[],
  link_urls     text[],
  lang_prefixes text[],
  mention_ids   integer[]
);

INSERT INTO post_search_compact (author, rkey, created_at, text, search_tsv, tags, link_domains, link_urls, lang_prefixes, mention_ids)
SELECT k.author, k.rkey, k.created_at, k.text, COALESCE(k.search_tsv, to_tsvector('simple', k.text)),
       k.tags, k.link_domains, k.link_urls, k.lang_prefixes, c.mention_ids
FROM posts_keyed k JOIN posts_compact c ON c.author = k.author AND c.rkey = k.rkey
WHERE EXISTS (SELECT 1 FROM scope_dids s WHERE s.did = k.author_did)
   OR EXISTS (SELECT 1 FROM members m WHERE m.did = k.author_did);

-- Feed items whose post is a whole post reference. Kind 1: post, 2: reply,
-- 3: repost.
CREATE TABLE feed_items_compact (
  originator  integer     NOT NULL,
  is_repost   boolean     NOT NULL,
  src_rkey    bigint      NOT NULL,
  kind        smallint    NOT NULL,
  post_author integer     NOT NULL,
  post_rkey   bigint      NOT NULL,
  sort_at     timestamptz NOT NULL
);

INSERT INTO feed_items_compact (originator, is_repost, src_rkey, kind, post_author, post_rkey, sort_at)
SELECT o.id, f.kind = 'repost', k.key,
       CASE f.kind WHEN 'post' THEN 1 WHEN 'reply' THEN 2 ELSE 3 END,
       p.author, p.rkey, f.sort_at
FROM feed_items f
JOIN dids o ON o.did = f.originator_did
CROSS JOIN LATERAL (SELECT split_part(f.uri, '/', 5) AS text) r
CROSS JOIN LATERAL (
  SELECT COALESCE(rkey_to_bigint(r.text), (SELECT x.id FROM rkey_aliases x WHERE x.author = o.id AND x.rkey = r.text)) AS key
) k
CROSS JOIN LATERAL ref_of(f.post_uri) p
WHERE f.kind IN ('post', 'reply', 'repost') AND rkey_is_valid(r.text) AND k.key IS NOT NULL AND p.coll = 1
  AND f.uri = 'at://' || f.originator_did || CASE WHEN f.kind = 'repost' THEN '/app.bsky.feed.repost/' ELSE '/app.bsky.feed.post/' END || r.text;

CREATE TABLE record_tombstones_compact (
  author     integer     NOT NULL,
  coll       smallint    NOT NULL,
  rkey       bigint      NOT NULL,
  rev        bigint      NOT NULL,
  deleted_at timestamptz NOT NULL DEFAULT now()
);

CREATE TEMP TABLE tombstones_keyed ON COMMIT DROP AS
SELECT a.id AS author, c.id AS coll, k.key AS rkey, t.rev, t.deleted_at, tid_is_valid(t.rev) AS rev_valid
FROM record_tombstones t
CROSS JOIN LATERAL (SELECT split_part(substr(t.uri, 6), '/', 1) AS did, split_part(substr(t.uri, 6), '/', 2) AS nsid,
                           split_part(substr(t.uri, 6), '/', 3) AS rkey) u
JOIN dids a ON a.did = u.did
JOIN collections c ON c.nsid = u.nsid
CROSS JOIN LATERAL (
  SELECT COALESCE(rkey_to_bigint(u.rkey), (SELECT x.id FROM rkey_aliases x WHERE x.author = a.id AND x.rkey = u.rkey)) AS key
) k
WHERE rkey_is_valid(u.rkey) AND k.key IS NOT NULL
  AND t.uri = 'at://' || u.did || '/' || u.nsid || '/' || u.rkey;

INSERT INTO record_tombstones_compact (author, coll, rkey, rev, deleted_at)
SELECT author, coll, rkey, rev_to_bigint(rev), deleted_at FROM tombstones_keyed;

DO $$
DECLARE
  p_old bigint; p_new bigint; p_rev bigint; p_odd bigint; s_new bigint;
  f_old bigint; f_new bigint;
  t_old bigint; t_new bigint; t_rev bigint;
BEGIN
  SELECT count(*) INTO p_old FROM posts;
  SELECT count(*), count(*) FILTER (WHERE NOT rev_valid) INTO p_new, p_rev FROM posts_keyed;
  SELECT count(*) INTO p_odd FROM posts_compact WHERE root_coll = 0 OR parent_coll = 0 OR quote_coll = 0;
  SELECT count(*) INTO s_new FROM post_search_compact;
  SELECT count(*) INTO f_old FROM feed_items;
  SELECT count(*) INTO f_new FROM feed_items_compact;
  SELECT count(*) INTO t_old FROM record_tombstones;
  SELECT count(*), count(*) FILTER (WHERE NOT rev_valid) INTO t_new, t_rev FROM tombstones_keyed;
  RAISE NOTICE 'posts: % rows, % copied, % left out, % revs not TIDs (stored as the lowest), % with odd references', p_old, p_new, p_old - p_new, p_rev, p_odd;
  RAISE NOTICE 'post_search: % rows (posts by authors in S and members)', s_new;
  RAISE NOTICE 'feed_items: % rows, % copied, % dropped (not whole posts or not keyable)', f_old, f_new, f_old - f_new;
  RAISE NOTICE 'record_tombstones: % rows, % copied, % left out, % revs not TIDs (stored as the lowest)', t_old, t_new, t_old - t_new, t_rev;
END $$;

-- The swap. The old tables take their indexes and triggers with them.
DROP TABLE posts;
DROP TABLE feed_items;
DROP TABLE record_tombstones;
ALTER TABLE posts_compact RENAME TO posts;
ALTER TABLE post_search_compact RENAME TO post_search;
ALTER TABLE feed_items_compact RENAME TO feed_items;
ALTER TABLE record_tombstones_compact RENAME TO record_tombstones;

-- A record over 2 KB is compressed already; pglz would not gain.
ALTER TABLE posts ALTER COLUMN record SET STORAGE EXTERNAL;

ALTER TABLE posts ADD CONSTRAINT posts_pkey PRIMARY KEY (author, rkey);
CREATE INDEX posts_author_created ON posts (author, created_at DESC);
-- Retention's age cap.
CREATE INDEX posts_created ON posts (created_at);
CREATE INDEX posts_parent ON posts (parent_author, parent_rkey) WHERE parent_author IS NOT NULL;
CREATE INDEX posts_root   ON posts (root_author, root_rkey) WHERE root_author IS NOT NULL;
CREATE INDEX posts_quote  ON posts (quote_author, quote_rkey) WHERE quote_author IS NOT NULL;

-- Its other indexes are built after start, concurrently (`schema_backfill`).
ALTER TABLE post_search ADD CONSTRAINT post_search_pkey PRIMARY KEY (author, rkey);

ALTER TABLE feed_items ADD CONSTRAINT feed_items_pkey PRIMARY KEY (originator, is_repost, src_rkey);
CREATE INDEX feed_items_originator ON feed_items (originator, sort_at DESC);

ALTER TABLE record_tombstones ADD CONSTRAINT record_tombstones_pkey PRIMARY KEY (author, coll, rkey);

-- An owner row's odd references go with it, and so does its search row.
CREATE TRIGGER posts_odd_refs
  AFTER DELETE ON posts
  FOR EACH ROW WHEN (OLD.root_coll = 0 OR OLD.parent_coll = 0 OR OLD.quote_coll = 0) EXECUTE FUNCTION odd_refs_owner_gone('1');

CREATE FUNCTION post_search_owner_gone() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  DELETE FROM post_search WHERE author = OLD.author AND rkey = OLD.rkey;
  RETURN NULL;
END;
$$;
CREATE TRIGGER posts_search_row
  AFTER DELETE ON posts
  FOR EACH ROW EXECUTE FUNCTION post_search_owner_gone();

-- Post counts on the new keys: a reply or quote counts toward its subject
-- only when that is a whole post reference (coll 1). Posts count toward
-- their author.
CREATE OR REPLACE FUNCTION count_posts() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  IF TG_OP IN ('UPDATE', 'DELETE') THEN
    IF OLD.parent_coll = 1 THEN
      UPDATE post_counts SET reply_count = GREATEST(reply_count - 1, 0)
        WHERE author = OLD.parent_author AND rkey = OLD.parent_rkey;
    END IF;
    IF OLD.quote_coll = 1 THEN
      UPDATE post_counts SET quote_count = GREATEST(quote_count - 1, 0)
        WHERE author = OLD.quote_author AND rkey = OLD.quote_rkey;
    END IF;
    UPDATE actor_counts SET posts_count = GREATEST(posts_count - 1, 0) WHERE id = OLD.author;
  END IF;
  IF TG_OP IN ('INSERT', 'UPDATE') THEN
    IF NEW.parent_coll = 1 THEN
      INSERT INTO post_counts (author, rkey, reply_count) VALUES (NEW.parent_author, NEW.parent_rkey, 1)
        ON CONFLICT (author, rkey) DO UPDATE SET reply_count = post_counts.reply_count + 1;
    END IF;
    IF NEW.quote_coll = 1 THEN
      INSERT INTO post_counts (author, rkey, quote_count) VALUES (NEW.quote_author, NEW.quote_rkey, 1)
        ON CONFLICT (author, rkey) DO UPDATE SET quote_count = post_counts.quote_count + 1;
    END IF;
    INSERT INTO actor_counts (id, posts_count) VALUES (NEW.author, 1)
      ON CONFLICT (id) DO UPDATE SET posts_count = actor_counts.posts_count + 1;
  END IF;
  RETURN NULL;
END;
$$;
CREATE TRIGGER posts_count
  AFTER INSERT OR DELETE OR UPDATE OF author, parent_author, parent_coll, parent_rkey, quote_author, quote_coll, quote_rkey ON posts
  FOR EACH ROW EXECUTE FUNCTION count_posts();

-- Tag activity on the new keys. A post's own tags count from its search
-- row; a reply counts toward its parent's tags, read from the parent's
-- search row; a like or repost toward its subject's.
DROP FUNCTION tag_activity_posts();
DROP FUNCTION tag_deltas_of_post(text[], timestamptz, text, int);
DROP FUNCTION tag_deltas_of_engagement(text, timestamptz, int, text);

-- What a post's tags contribute: 1 post each, times `sign`.
CREATE FUNCTION tag_deltas_of_tags(tags text[], created timestamptz, sign int)
RETURNS tag_delta[] LANGUAGE sql IMMUTABLE AS $$
  SELECT COALESCE(array_agg(ROW(u.tag, date_trunc('hour', created, 'UTC'), sign, 0, 0, 0)::tag_delta), '{}')
  FROM unnest(tags) AS u(tag)
$$;

-- What a reply contributes: 1 reply for each tag of the post it replies to,
-- when that post is recent enough, times `sign`.
CREATE FUNCTION tag_deltas_of_reply(p_author integer, p_rkey bigint, created timestamptz, sign int)
RETURNS tag_delta[] LANGUAGE sql STABLE AS $$
  SELECT COALESCE(array_agg(ROW(u.tag, date_trunc('hour', created, 'UTC'), 0, 0, 0, sign)::tag_delta), '{}')
  FROM post_search s, unnest(s.tags) AS u(tag)
  WHERE s.author = p_author AND s.rkey = p_rkey AND s.created_at >= created - trends_lookback()
$$;

-- What a like or repost contributes: 1 for each tag of its subject post,
-- when that post is recent enough, times `sign`. `counter` names the column.
CREATE FUNCTION tag_deltas_of_engagement(s_author integer, s_rkey bigint, created timestamptz, sign int, counter text)
RETURNS tag_delta[] LANGUAGE sql STABLE AS $$
  SELECT COALESCE(array_agg(ROW(
           u.tag, date_trunc('hour', created, 'UTC'), 0,
           CASE WHEN counter = 'likes' THEN sign ELSE 0 END,
           CASE WHEN counter = 'reposts' THEN sign ELSE 0 END,
           0)::tag_delta), '{}')
  FROM post_search s, unnest(s.tags) AS u(tag)
  WHERE s.author = s_author AND s.rkey = s_rkey AND s.created_at >= created - trends_lookback()
$$;

CREATE FUNCTION tag_activity_own() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
  deltas tag_delta[] := '{}';
BEGIN
  IF TG_OP IN ('DELETE', 'UPDATE') AND OLD.created_at >= trends_cutoff() THEN
    deltas := deltas || tag_deltas_of_tags(OLD.tags, OLD.created_at, -1);
  END IF;
  IF TG_OP IN ('INSERT', 'UPDATE') AND NEW.created_at >= trends_cutoff() THEN
    deltas := deltas || tag_deltas_of_tags(NEW.tags, NEW.created_at, 1);
  END IF;
  PERFORM tag_activity_apply(deltas);
  RETURN NULL;
END $$;

CREATE FUNCTION tag_activity_replies() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
  deltas tag_delta[] := '{}';
BEGIN
  IF TG_OP IN ('DELETE', 'UPDATE') AND OLD.created_at >= trends_cutoff() AND OLD.parent_coll = 1 THEN
    deltas := deltas || tag_deltas_of_reply(OLD.parent_author, OLD.parent_rkey, OLD.created_at, -1);
  END IF;
  IF TG_OP IN ('INSERT', 'UPDATE') AND NEW.created_at >= trends_cutoff() AND NEW.parent_coll = 1 THEN
    deltas := deltas || tag_deltas_of_reply(NEW.parent_author, NEW.parent_rkey, NEW.created_at, 1);
  END IF;
  PERFORM tag_activity_apply(deltas);
  RETURN NULL;
END $$;

CREATE OR REPLACE FUNCTION tag_activity_engagement() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
  deltas tag_delta[] := '{}';
BEGIN
  IF TG_OP IN ('DELETE', 'UPDATE') AND OLD.created_at >= trends_cutoff() AND OLD.subj_coll = 1 THEN
    deltas := deltas || tag_deltas_of_engagement(OLD.subj_author, OLD.subj_rkey, OLD.created_at, -1, TG_ARGV[0]);
  END IF;
  IF TG_OP IN ('INSERT', 'UPDATE') AND NEW.created_at >= trends_cutoff() AND NEW.subj_coll = 1 THEN
    deltas := deltas || tag_deltas_of_engagement(NEW.subj_author, NEW.subj_rkey, NEW.created_at, 1, TG_ARGV[0]);
  END IF;
  PERFORM tag_activity_apply(deltas);
  RETURN NULL;
END $$;

CREATE TRIGGER post_search_tag_activity_insert
  AFTER INSERT ON post_search
  FOR EACH ROW WHEN (NEW.created_at >= trends_cutoff())
  EXECUTE FUNCTION tag_activity_own();
CREATE TRIGGER post_search_tag_activity_delete
  AFTER DELETE ON post_search
  FOR EACH ROW WHEN (OLD.created_at >= trends_cutoff())
  EXECUTE FUNCTION tag_activity_own();
CREATE TRIGGER post_search_tag_activity_update
  AFTER UPDATE OF tags, created_at ON post_search
  FOR EACH ROW WHEN (OLD.created_at >= trends_cutoff() OR NEW.created_at >= trends_cutoff())
  EXECUTE FUNCTION tag_activity_own();

CREATE TRIGGER posts_tag_activity_insert
  AFTER INSERT ON posts
  FOR EACH ROW WHEN (NEW.created_at >= trends_cutoff() AND NEW.parent_coll = 1)
  EXECUTE FUNCTION tag_activity_replies();
CREATE TRIGGER posts_tag_activity_delete
  AFTER DELETE ON posts
  FOR EACH ROW WHEN (OLD.created_at >= trends_cutoff() AND OLD.parent_coll = 1)
  EXECUTE FUNCTION tag_activity_replies();
CREATE TRIGGER posts_tag_activity_update
  AFTER UPDATE OF created_at, parent_author, parent_coll, parent_rkey ON posts
  FOR EACH ROW WHEN ((OLD.created_at >= trends_cutoff() AND OLD.parent_coll = 1) OR (NEW.created_at >= trends_cutoff() AND NEW.parent_coll = 1))
  EXECUTE FUNCTION tag_activity_replies();
