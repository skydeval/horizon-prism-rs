-- Compact storage: `likes`, `reposts`, `post_counts` and `actor_counts`
-- keyed by interned ids.
--
-- A like or repost row is `(author, rkey)`, as follows are. Its subject and
-- its `via` are references `(*_author, *_coll, *_rkey)` with their CIDs as
-- bytes. A reference that `at_uri` would not give back exactly is odd:
-- `*_coll = 0`, its text in `odd_refs`, and `*_author` kept when its
-- authority is a valid DID. `indexed_at` and the text columns are gone.
--
-- `post_counts` is keyed by the post's `(author, rkey)` and holds posts
-- only: a count row of anything else (a like of a list, an odd subject) is
-- dropped and counted in the log. `actor_counts` is keyed by `dids.id`.
--
-- One transaction: intern, copy, then indexes and triggers, then the swap.
-- A like or repost whose author is not a valid DID, whose record key is not
-- valid, or whose URI is not its author's own record cannot be keyed; it is
-- left out and counted in the log. Ingest checks all three, so none are
-- expected.

-- Every valid DID the tables name: authors, the authorities of subjects
-- and `via` references, the authors of counted posts, counted accounts.
INSERT INTO dids (did)
SELECT x.d FROM (
  SELECT author_did AS d FROM likes
  UNION SELECT author_did FROM reposts
  UNION SELECT split_part(substr(subject_uri, 6), '/', 1) FROM likes WHERE left(subject_uri, 5) = 'at://'
  UNION SELECT split_part(substr(subject_uri, 6), '/', 1) FROM reposts WHERE left(subject_uri, 5) = 'at://'
  UNION SELECT split_part(substr(via_uri, 6), '/', 1) FROM likes WHERE left(via_uri, 5) = 'at://'
  UNION SELECT split_part(substr(via_uri, 6), '/', 1) FROM reposts WHERE left(via_uri, 5) = 'at://'
  UNION SELECT split_part(substr(post_uri, 6), '/', 1) FROM post_counts WHERE left(post_uri, 5) = 'at://'
  UNION SELECT did FROM actor_counts
) x
WHERE did_is_valid(x.d) AND NOT EXISTS (SELECT 1 FROM dids a WHERE a.did = x.d)
ORDER BY x.d
ON CONFLICT (did) DO NOTHING;

-- Every reference text the copy reads, with what could make it whole.
CREATE TEMP TABLE refs_named ON COMMIT DROP AS
SELECT u AS uri FROM (
  SELECT subject_uri AS u FROM likes
  UNION SELECT subject_uri FROM reposts
  UNION SELECT via_uri FROM likes WHERE via_uri IS NOT NULL
  UNION SELECT via_uri FROM reposts WHERE via_uri IS NOT NULL
  UNION SELECT post_uri FROM post_counts
) r
WHERE left(u, 5) = 'at://'
  AND cardinality(string_to_array(substr(u, 6), '/')) = 3
  AND split_part(substr(u, 6), '/', 2) IN (SELECT nsid FROM collections);

-- Aliases for every valid record key that is not a TID with its top bit
-- clear: the rows' own keys, and the keys of references that could be
-- whole (three parts, a listed collection).
INSERT INTO rkey_aliases (author, rkey)
SELECT DISTINCT d.id, k.rkey FROM (
  SELECT author_did AS did, split_part(uri, '/', 5) AS rkey FROM likes
  UNION SELECT author_did, split_part(uri, '/', 5) FROM reposts
  UNION SELECT split_part(substr(uri, 6), '/', 1), split_part(substr(uri, 6), '/', 3) FROM refs_named
) k
JOIN dids d ON d.did = k.did
WHERE rkey_is_valid(k.rkey) AND rkey_to_bigint(k.rkey) IS NULL
  AND NOT EXISTS (SELECT 1 FROM rkey_aliases x WHERE x.author = d.id AND x.rkey = k.rkey)
ORDER BY 1, 2
ON CONFLICT (author, rkey) DO NOTHING;

-- The old rows with their new keys.
CREATE TEMP TABLE likes_keyed ON COMMIT DROP AS
SELECT a.id AS author, k.key AS rkey, rev_to_bigint(l.rev) AS rev, l.created_at,
       l.subject_uri, l.subject_cid, l.via_uri, l.via_cid, tid_is_valid(l.rev) AS rev_valid
FROM likes l
JOIN dids a ON a.did = l.author_did
CROSS JOIN LATERAL (SELECT split_part(l.uri, '/', 5) AS text) r
CROSS JOIN LATERAL (
  SELECT COALESCE(rkey_to_bigint(r.text), (SELECT x.id FROM rkey_aliases x WHERE x.author = a.id AND x.rkey = r.text)) AS key
) k
WHERE rkey_is_valid(r.text) AND k.key IS NOT NULL
  AND l.uri = 'at://' || l.author_did || '/app.bsky.feed.like/' || r.text;

CREATE TEMP TABLE reposts_keyed ON COMMIT DROP AS
SELECT a.id AS author, k.key AS rkey, rev_to_bigint(l.rev) AS rev, l.created_at,
       l.subject_uri, l.subject_cid, l.via_uri, l.via_cid, tid_is_valid(l.rev) AS rev_valid
FROM reposts l
JOIN dids a ON a.did = l.author_did
CROSS JOIN LATERAL (SELECT split_part(l.uri, '/', 5) AS text) r
CROSS JOIN LATERAL (
  SELECT COALESCE(rkey_to_bigint(r.text), (SELECT x.id FROM rkey_aliases x WHERE x.author = a.id AND x.rkey = r.text)) AS key
) k
WHERE rkey_is_valid(r.text) AND k.key IS NOT NULL
  AND l.uri = 'at://' || l.author_did || '/app.bsky.feed.repost/' || r.text;

-- The new tables, without triggers, filled from the keyed rows. Every CID
-- is kept in its exact text form (0x00 then the text).
CREATE TABLE likes_compact (
  author      integer     NOT NULL,
  rkey        bigint      NOT NULL,
  subj_author integer,
  subj_coll   smallint    NOT NULL,
  subj_rkey   bigint,
  subj_cid    bytea,
  rev         bigint      NOT NULL,
  created_at  timestamptz NOT NULL,
  via_author  integer,
  via_coll    smallint,
  via_rkey    bigint,
  via_cid     bytea
);
CREATE TABLE reposts_compact (LIKE likes_compact);

INSERT INTO likes_compact (author, rkey, subj_author, subj_coll, subj_rkey, subj_cid, rev, created_at, via_author, via_coll, via_rkey, via_cid)
SELECT k.author, k.rkey,
       s.author, COALESCE(s.coll, 0), s.rkey,
       CASE WHEN k.subject_cid IS NOT NULL THEN '\x00'::bytea || convert_to(k.subject_cid, 'UTF8') END,
       k.rev, k.created_at,
       v.author,
       CASE WHEN k.via_uri IS NULL THEN NULL WHEN v.coll IS NOT NULL THEN v.coll ELSE 0 END,
       v.rkey,
       CASE WHEN k.via_uri IS NOT NULL AND k.via_cid IS NOT NULL THEN '\x00'::bytea || convert_to(k.via_cid, 'UTF8') END
FROM likes_keyed k
CROSS JOIN LATERAL ref_of(k.subject_uri) s
LEFT JOIN LATERAL ref_of(k.via_uri) v ON true;

INSERT INTO reposts_compact (author, rkey, subj_author, subj_coll, subj_rkey, subj_cid, rev, created_at, via_author, via_coll, via_rkey, via_cid)
SELECT k.author, k.rkey,
       s.author, COALESCE(s.coll, 0), s.rkey,
       CASE WHEN k.subject_cid IS NOT NULL THEN '\x00'::bytea || convert_to(k.subject_cid, 'UTF8') END,
       k.rev, k.created_at,
       v.author,
       CASE WHEN k.via_uri IS NULL THEN NULL WHEN v.coll IS NOT NULL THEN v.coll ELSE 0 END,
       v.rkey,
       CASE WHEN k.via_uri IS NOT NULL AND k.via_cid IS NOT NULL THEN '\x00'::bytea || convert_to(k.via_cid, 'UTF8') END
FROM reposts_keyed k
CROSS JOIN LATERAL ref_of(k.subject_uri) s
LEFT JOIN LATERAL ref_of(k.via_uri) v ON true;

-- The odd texts (tbl 2: likes, 3: reposts; slot 4: subject, 5: via).
INSERT INTO odd_refs (tbl, author, rkey, slot, raw)
SELECT 2, k.author, k.rkey, 4, k.subject_uri
FROM likes_keyed k JOIN likes_compact c ON c.author = k.author AND c.rkey = k.rkey
WHERE c.subj_coll = 0
UNION ALL
SELECT 2, k.author, k.rkey, 5, k.via_uri
FROM likes_keyed k JOIN likes_compact c ON c.author = k.author AND c.rkey = k.rkey
WHERE c.via_coll = 0
UNION ALL
SELECT 3, k.author, k.rkey, 4, k.subject_uri
FROM reposts_keyed k JOIN reposts_compact c ON c.author = k.author AND c.rkey = k.rkey
WHERE c.subj_coll = 0
UNION ALL
SELECT 3, k.author, k.rkey, 5, k.via_uri
FROM reposts_keyed k JOIN reposts_compact c ON c.author = k.author AND c.rkey = k.rkey
WHERE c.via_coll = 0;

-- Counts of whole posts only, as they are.
CREATE TABLE post_counts_compact (
  author       integer NOT NULL,
  rkey         bigint  NOT NULL,
  like_count   int NOT NULL DEFAULT 0,
  repost_count int NOT NULL DEFAULT 0,
  reply_count  int NOT NULL DEFAULT 0,
  quote_count  int NOT NULL DEFAULT 0
);
INSERT INTO post_counts_compact (author, rkey, like_count, repost_count, reply_count, quote_count)
SELECT r.author, r.rkey, c.like_count, c.repost_count, c.reply_count, c.quote_count
FROM post_counts c CROSS JOIN LATERAL ref_of(c.post_uri) r
WHERE r.coll = 1;

CREATE TABLE actor_counts_compact (
  id              integer NOT NULL,
  followers_count int NOT NULL DEFAULT 0,
  follows_count   int NOT NULL DEFAULT 0,
  posts_count     int NOT NULL DEFAULT 0
);
INSERT INTO actor_counts_compact (id, followers_count, follows_count, posts_count)
SELECT d.id, c.followers_count, c.follows_count, c.posts_count
FROM actor_counts c JOIN dids d ON d.did = c.did;

DO $$
DECLARE
  l_old bigint; l_new bigint; l_rev bigint; l_odd bigint;
  r_old bigint; r_new bigint; r_rev bigint; r_odd bigint;
  p_old bigint; p_new bigint;
  a_old bigint; a_new bigint;
BEGIN
  SELECT count(*) INTO l_old FROM likes;
  SELECT count(*), count(*) FILTER (WHERE NOT rev_valid) INTO l_new, l_rev FROM likes_keyed;
  SELECT count(*) INTO l_odd FROM likes_compact WHERE subj_coll = 0;
  SELECT count(*) INTO r_old FROM reposts;
  SELECT count(*), count(*) FILTER (WHERE NOT rev_valid) INTO r_new, r_rev FROM reposts_keyed;
  SELECT count(*) INTO r_odd FROM reposts_compact WHERE subj_coll = 0;
  SELECT count(*) INTO p_old FROM post_counts;
  SELECT count(*) INTO p_new FROM post_counts_compact;
  SELECT count(*) INTO a_old FROM actor_counts;
  SELECT count(*) INTO a_new FROM actor_counts_compact;
  RAISE NOTICE 'likes: % rows, % copied, % left out, % revs not TIDs (stored as the lowest), % odd subjects', l_old, l_new, l_old - l_new, l_rev, l_odd;
  RAISE NOTICE 'reposts: % rows, % copied, % left out, % revs not TIDs (stored as the lowest), % odd subjects', r_old, r_new, r_old - r_new, r_rev, r_odd;
  RAISE NOTICE 'post_counts: % rows, % copied, % dropped (not whole posts)', p_old, p_new, p_old - p_new;
  RAISE NOTICE 'actor_counts: % rows, % copied, % dropped (not valid DIDs)', a_old, a_new, a_old - a_new;
END $$;

-- The swap. The old tables take their indexes and triggers with them.
DROP TABLE likes;
DROP TABLE reposts;
DROP TABLE post_counts;
DROP TABLE actor_counts;
ALTER TABLE likes_compact RENAME TO likes;
ALTER TABLE reposts_compact RENAME TO reposts;
ALTER TABLE post_counts_compact RENAME TO post_counts;
ALTER TABLE actor_counts_compact RENAME TO actor_counts;

ALTER TABLE likes ADD CONSTRAINT likes_pkey PRIMARY KEY (author, rkey);
CREATE INDEX likes_author         ON likes (author, created_at DESC);
CREATE INDEX likes_subject        ON likes (subj_author, subj_rkey, subj_coll, created_at DESC);
CREATE INDEX likes_subject_author ON likes (subj_author, subj_rkey, subj_coll, author);
CREATE INDEX likes_via            ON likes (via_author, via_rkey) WHERE via_author IS NOT NULL;

ALTER TABLE reposts ADD CONSTRAINT reposts_pkey PRIMARY KEY (author, rkey);
CREATE INDEX reposts_author         ON reposts (author, created_at DESC);
CREATE INDEX reposts_subject        ON reposts (subj_author, subj_rkey, subj_coll, created_at DESC);
CREATE INDEX reposts_subject_author ON reposts (subj_author, subj_rkey, subj_coll, author);
CREATE INDEX reposts_via            ON reposts (via_author, via_rkey) WHERE via_author IS NOT NULL;
-- The repost items of the private timeline and author feed (27: private
-- posts).
CREATE INDEX reposts_private_subject ON reposts (author, created_at DESC) WHERE subj_coll = 27;
-- Reposts by the subject's author: Refract's purge and spares of reposts of
-- restored posts, and `getTimeline`'s repost arm.
CREATE INDEX reposts_subject_did ON reposts (subj_author, created_at DESC);

ALTER TABLE post_counts ADD CONSTRAINT post_counts_pkey PRIMARY KEY (author, rkey);
ALTER TABLE actor_counts ADD CONSTRAINT actor_counts_pkey PRIMARY KEY (id);

-- A reference's text from its owner row's columns: rebuilt from its key, or
-- read from `odd_refs` when it is odd. NULL when there is none.
CREATE FUNCTION ref_text(author integer, coll smallint, rkey bigint, tbl smallint, owner_author integer, owner_rkey bigint, slot smallint)
  RETURNS text LANGUAGE sql STABLE PARALLEL SAFE
  RETURN CASE WHEN coll = 0 THEN (SELECT o.raw FROM odd_refs o WHERE o.tbl = ref_text.tbl AND o.author = owner_author
                                    AND o.rkey = owner_rkey AND o.slot = ref_text.slot)
              ELSE at_uri(author, coll, rkey) END;

-- Like and repost counts by accounts, on the new keys; only whole posts
-- (coll 1) are counted. The logic of migration 0029 is unchanged: per
-- statement, each (subject, author) pair touched is weighed once.
DROP FUNCTION adjust_account_counts(text, text[], text[], int[]);
CREATE FUNCTION adjust_account_counts(kind text, s_authors int[], s_rkeys bigint[], authors int[], deltas int[])
RETURNS void LANGUAGE plpgsql AS $$
BEGIN
  IF s_authors IS NULL THEN
    RETURN;
  END IF;
  INSERT INTO post_counts (author, rkey)
    SELECT DISTINCT t.a, t.r FROM unnest(s_authors, s_rkeys, deltas) AS t(a, r, d) WHERE t.d > 0 ORDER BY t.a, t.r
    ON CONFLICT (author, rkey) DO NOTHING;
  PERFORM 1 FROM post_counts c
    WHERE (c.author, c.rkey) IN (SELECT * FROM unnest(s_authors, s_rkeys))
    ORDER BY c.author, c.rkey FOR UPDATE;
  IF kind = 'like' THEN
    UPDATE post_counts c SET like_count = GREATEST(c.like_count + x.delta, 0)
      FROM (SELECT p.sa, p.sr, sum((n.cnt > 0)::int - (n.cnt - p.d > 0)::int) AS delta
            FROM unnest(s_authors, s_rkeys, authors, deltas) AS p(sa, sr, a, d)
            CROSS JOIN LATERAL (SELECT count(*) AS cnt FROM likes l
                                WHERE l.subj_author = p.sa AND l.subj_rkey = p.sr AND l.subj_coll = 1 AND l.author = p.a) n
            GROUP BY p.sa, p.sr) x
      WHERE c.author = x.sa AND c.rkey = x.sr AND x.delta <> 0;
  ELSE
    UPDATE post_counts c SET repost_count = GREATEST(c.repost_count + x.delta, 0)
      FROM (SELECT p.sa, p.sr, sum((n.cnt > 0)::int - (n.cnt - p.d > 0)::int) AS delta
            FROM unnest(s_authors, s_rkeys, authors, deltas) AS p(sa, sr, a, d)
            CROSS JOIN LATERAL (SELECT count(*) AS cnt FROM reposts r
                                WHERE r.subj_author = p.sa AND r.subj_rkey = p.sr AND r.subj_coll = 1 AND r.author = p.a) n
            GROUP BY p.sa, p.sr) x
      WHERE c.author = x.sa AND c.rkey = x.sr AND x.delta <> 0;
  END IF;
END;
$$;

-- TG_ARGV[0]: 'like' or 'repost'.
CREATE FUNCTION count_interaction_accounts() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  IF TG_OP = 'INSERT' THEN
    PERFORM adjust_account_counts(TG_ARGV[0], array_agg(sa), array_agg(sr), array_agg(a), array_agg(d))
      FROM (SELECT subj_author AS sa, subj_rkey AS sr, author AS a, count(*)::int AS d FROM new_rows
            WHERE subj_coll = 1 GROUP BY 1, 2, 3) t;
  ELSIF TG_OP = 'DELETE' THEN
    PERFORM adjust_account_counts(TG_ARGV[0], array_agg(sa), array_agg(sr), array_agg(a), array_agg(d))
      FROM (SELECT subj_author AS sa, subj_rkey AS sr, author AS a, -count(*)::int AS d FROM old_rows
            WHERE subj_coll = 1 GROUP BY 1, 2, 3) t;
  ELSE
    PERFORM adjust_account_counts(TG_ARGV[0], array_agg(sa), array_agg(sr), array_agg(a), array_agg(d))
      FROM (SELECT sa, sr, a, sum(d)::int AS d FROM (
              SELECT subj_author AS sa, subj_rkey AS sr, author AS a, 1 AS d FROM new_rows WHERE subj_coll = 1
              UNION ALL SELECT subj_author, subj_rkey, author, -1 FROM old_rows WHERE subj_coll = 1) u
            GROUP BY 1, 2, 3 HAVING sum(d) <> 0) t;
  END IF;
  RETURN NULL;
END;
$$;
DROP FUNCTION count_like_accounts();
DROP FUNCTION count_repost_accounts();

CREATE TRIGGER likes_count_insert AFTER INSERT ON likes
  REFERENCING NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION count_interaction_accounts('like');
CREATE TRIGGER likes_count_delete AFTER DELETE ON likes
  REFERENCING OLD TABLE AS old_rows FOR EACH STATEMENT EXECUTE FUNCTION count_interaction_accounts('like');
CREATE TRIGGER likes_count_update AFTER UPDATE ON likes
  REFERENCING OLD TABLE AS old_rows NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION count_interaction_accounts('like');
CREATE TRIGGER reposts_count_insert AFTER INSERT ON reposts
  REFERENCING NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION count_interaction_accounts('repost');
CREATE TRIGGER reposts_count_delete AFTER DELETE ON reposts
  REFERENCING OLD TABLE AS old_rows FOR EACH STATEMENT EXECUTE FUNCTION count_interaction_accounts('repost');
CREATE TRIGGER reposts_count_update AFTER UPDATE ON reposts
  REFERENCING OLD TABLE AS old_rows NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION count_interaction_accounts('repost');

-- Tag activity of likes and reposts: the subject read as a post's URI, so
-- only a whole post reference adds anything. TG_ARGV[0] names the counter.
CREATE OR REPLACE FUNCTION tag_activity_engagement() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
  deltas tag_delta[] := '{}';
BEGIN
  IF TG_OP IN ('DELETE', 'UPDATE') AND OLD.created_at >= trends_cutoff() AND OLD.subj_coll = 1 THEN
    deltas := deltas || tag_deltas_of_engagement(at_uri(OLD.subj_author, OLD.subj_coll, OLD.subj_rkey), OLD.created_at, -1, TG_ARGV[0]);
  END IF;
  IF TG_OP IN ('INSERT', 'UPDATE') AND NEW.created_at >= trends_cutoff() AND NEW.subj_coll = 1 THEN
    deltas := deltas || tag_deltas_of_engagement(at_uri(NEW.subj_author, NEW.subj_coll, NEW.subj_rkey), NEW.created_at, 1, TG_ARGV[0]);
  END IF;
  PERFORM tag_activity_apply(deltas);
  RETURN NULL;
END $$;

CREATE TRIGGER likes_tag_activity_insert
  AFTER INSERT ON likes
  FOR EACH ROW WHEN (NEW.created_at >= trends_cutoff())
  EXECUTE FUNCTION tag_activity_engagement('likes');
CREATE TRIGGER likes_tag_activity_delete
  AFTER DELETE ON likes
  FOR EACH ROW WHEN (OLD.created_at >= trends_cutoff())
  EXECUTE FUNCTION tag_activity_engagement('likes');
CREATE TRIGGER likes_tag_activity_update
  AFTER UPDATE OF created_at, subj_author, subj_coll, subj_rkey ON likes
  FOR EACH ROW WHEN (OLD.created_at >= trends_cutoff() OR NEW.created_at >= trends_cutoff())
  EXECUTE FUNCTION tag_activity_engagement('likes');
CREATE TRIGGER reposts_tag_activity_insert
  AFTER INSERT ON reposts
  FOR EACH ROW WHEN (NEW.created_at >= trends_cutoff())
  EXECUTE FUNCTION tag_activity_engagement('reposts');
CREATE TRIGGER reposts_tag_activity_delete
  AFTER DELETE ON reposts
  FOR EACH ROW WHEN (OLD.created_at >= trends_cutoff())
  EXECUTE FUNCTION tag_activity_engagement('reposts');
CREATE TRIGGER reposts_tag_activity_update
  AFTER UPDATE OF created_at, subj_author, subj_coll, subj_rkey ON reposts
  FOR EACH ROW WHEN (OLD.created_at >= trends_cutoff() OR NEW.created_at >= trends_cutoff())
  EXECUTE FUNCTION tag_activity_engagement('reposts');

-- An owner row's odd references go with it.
CREATE TRIGGER likes_odd_refs
  AFTER DELETE ON likes
  FOR EACH ROW WHEN (OLD.subj_coll = 0 OR OLD.via_coll = 0) EXECUTE FUNCTION odd_refs_owner_gone('2');
CREATE TRIGGER reposts_odd_refs
  AFTER DELETE ON reposts
  FOR EACH ROW WHEN (OLD.subj_coll = 0 OR OLD.via_coll = 0) EXECUTE FUNCTION odd_refs_owner_gone('3');

-- Post counts from `posts`, whose rows still name references by text: a
-- reply or quote counts toward its subject only when that is a whole post
-- reference whose ids exist (the writer interns them first). Posts count
-- toward their author's `dids` id.
CREATE OR REPLACE FUNCTION count_posts() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  IF TG_OP IN ('UPDATE', 'DELETE') THEN
    IF OLD.reply_parent_uri IS NOT NULL THEN
      UPDATE post_counts c SET reply_count = GREATEST(c.reply_count - 1, 0)
        FROM ref_of(OLD.reply_parent_uri) r WHERE r.coll = 1 AND c.author = r.author AND c.rkey = r.rkey;
    END IF;
    IF OLD.quote_uri IS NOT NULL THEN
      UPDATE post_counts c SET quote_count = GREATEST(c.quote_count - 1, 0)
        FROM ref_of(OLD.quote_uri) r WHERE r.coll = 1 AND c.author = r.author AND c.rkey = r.rkey;
    END IF;
    UPDATE actor_counts SET posts_count = GREATEST(posts_count - 1, 0)
      WHERE id = (SELECT d.id FROM dids d WHERE d.did = OLD.author_did);
  END IF;
  IF TG_OP IN ('INSERT', 'UPDATE') THEN
    IF NEW.reply_parent_uri IS NOT NULL THEN
      INSERT INTO post_counts (author, rkey, reply_count)
        SELECT r.author, r.rkey, 1 FROM ref_of(NEW.reply_parent_uri) r WHERE r.coll = 1
        ON CONFLICT (author, rkey) DO UPDATE SET reply_count = post_counts.reply_count + 1;
    END IF;
    IF NEW.quote_uri IS NOT NULL THEN
      INSERT INTO post_counts (author, rkey, quote_count)
        SELECT r.author, r.rkey, 1 FROM ref_of(NEW.quote_uri) r WHERE r.coll = 1
        ON CONFLICT (author, rkey) DO UPDATE SET quote_count = post_counts.quote_count + 1;
    END IF;
    INSERT INTO actor_counts (id, posts_count) SELECT d.id, 1 FROM dids d WHERE d.did = NEW.author_did
      ON CONFLICT (id) DO UPDATE SET posts_count = actor_counts.posts_count + 1;
  END IF;
  RETURN NULL;
END;
$$;

-- Follow counts, keyed by id.
CREATE OR REPLACE FUNCTION count_follows() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  IF TG_OP IN ('UPDATE', 'DELETE') THEN
    UPDATE actor_counts SET follows_count = GREATEST(follows_count - 1, 0) WHERE id = OLD.author;
    UPDATE actor_counts SET followers_count = GREATEST(followers_count - 1, 0) WHERE id = OLD.subject;
  END IF;
  IF TG_OP IN ('INSERT', 'UPDATE') THEN
    INSERT INTO actor_counts (id, follows_count) VALUES (NEW.author, 1)
      ON CONFLICT (id) DO UPDATE SET follows_count = actor_counts.follows_count + 1;
    INSERT INTO actor_counts (id, followers_count) VALUES (NEW.subject, 1)
      ON CONFLICT (id) DO UPDATE SET followers_count = actor_counts.followers_count + 1;
  END IF;
  RETURN NULL;
END;
$$;
