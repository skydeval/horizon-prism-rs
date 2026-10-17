-- likeCount and repostCount count accounts, not records (#727): an
-- account's several likes (or reposts) of one post count once. Every record
-- is still stored.
--
-- The row-level triggers of 0001 become statement-level ones with
-- transition tables. A row-level AFTER trigger sees the whole statement's
-- changes, so a statement removing two of one account's likes of a post
-- would decrement twice. Per statement, each (subject, author) pair touched
-- is weighed once: it counts after the statement when a row of the pair
-- remains, and counted before when one remained less the statement's net
-- change. A subject's count moves by the pairs that started or stopped
-- counting. An UPDATE that keeps the subject and author nets out and does
-- nothing. The subjects' count rows are locked, in order, before the pairs
-- are read, so a concurrent writer of the same pair is read once committed.
-- Counts still clamp at 0.

CREATE INDEX likes_subject_author ON likes (subject_uri, author_did);
CREATE INDEX reposts_subject_author ON reposts (subject_uri, author_did);

DROP TRIGGER likes_count ON likes;
DROP TRIGGER reposts_count ON reposts;
DROP FUNCTION count_likes();
DROP FUNCTION count_reposts();

-- `kind` 'like' or 'repost'; `deltas` each pair's net row change in the
-- statement.
CREATE FUNCTION adjust_account_counts(kind text, subjects text[], authors text[], deltas int[])
RETURNS void LANGUAGE plpgsql AS $$
BEGIN
  IF subjects IS NULL THEN
    RETURN;
  END IF;
  INSERT INTO post_counts (post_uri)
    SELECT DISTINCT t.s FROM unnest(subjects, deltas) AS t(s, d) WHERE t.d > 0 ORDER BY t.s
    ON CONFLICT (post_uri) DO NOTHING;
  PERFORM 1 FROM post_counts WHERE post_uri = ANY(subjects) ORDER BY post_uri FOR UPDATE;
  IF kind = 'like' THEN
    UPDATE post_counts c SET like_count = GREATEST(c.like_count + x.delta, 0)
      FROM (SELECT p.s, sum((n.cnt > 0)::int - (n.cnt - p.d > 0)::int) AS delta
            FROM unnest(subjects, authors, deltas) AS p(s, a, d)
            CROSS JOIN LATERAL (SELECT count(*) AS cnt FROM likes l
                                WHERE l.subject_uri = p.s AND l.author_did = p.a) n
            GROUP BY p.s) x
      WHERE c.post_uri = x.s AND x.delta <> 0;
  ELSE
    UPDATE post_counts c SET repost_count = GREATEST(c.repost_count + x.delta, 0)
      FROM (SELECT p.s, sum((n.cnt > 0)::int - (n.cnt - p.d > 0)::int) AS delta
            FROM unnest(subjects, authors, deltas) AS p(s, a, d)
            CROSS JOIN LATERAL (SELECT count(*) AS cnt FROM reposts r
                                WHERE r.subject_uri = p.s AND r.author_did = p.a) n
            GROUP BY p.s) x
      WHERE c.post_uri = x.s AND x.delta <> 0;
  END IF;
END;
$$;

CREATE FUNCTION count_like_accounts() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  IF TG_OP = 'INSERT' THEN
    PERFORM adjust_account_counts('like', array_agg(s), array_agg(a), array_agg(d))
      FROM (SELECT subject_uri AS s, author_did AS a, count(*)::int AS d FROM new_rows GROUP BY 1, 2) t;
  ELSIF TG_OP = 'DELETE' THEN
    PERFORM adjust_account_counts('like', array_agg(s), array_agg(a), array_agg(d))
      FROM (SELECT subject_uri AS s, author_did AS a, -count(*)::int AS d FROM old_rows GROUP BY 1, 2) t;
  ELSE
    PERFORM adjust_account_counts('like', array_agg(s), array_agg(a), array_agg(d))
      FROM (SELECT s, a, sum(d)::int AS d FROM (
              SELECT subject_uri AS s, author_did AS a, 1 AS d FROM new_rows
              UNION ALL SELECT subject_uri, author_did, -1 FROM old_rows) u
            GROUP BY 1, 2 HAVING sum(d) <> 0) t;
  END IF;
  RETURN NULL;
END;
$$;

CREATE FUNCTION count_repost_accounts() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  IF TG_OP = 'INSERT' THEN
    PERFORM adjust_account_counts('repost', array_agg(s), array_agg(a), array_agg(d))
      FROM (SELECT subject_uri AS s, author_did AS a, count(*)::int AS d FROM new_rows GROUP BY 1, 2) t;
  ELSIF TG_OP = 'DELETE' THEN
    PERFORM adjust_account_counts('repost', array_agg(s), array_agg(a), array_agg(d))
      FROM (SELECT subject_uri AS s, author_did AS a, -count(*)::int AS d FROM old_rows GROUP BY 1, 2) t;
  ELSE
    PERFORM adjust_account_counts('repost', array_agg(s), array_agg(a), array_agg(d))
      FROM (SELECT s, a, sum(d)::int AS d FROM (
              SELECT subject_uri AS s, author_did AS a, 1 AS d FROM new_rows
              UNION ALL SELECT subject_uri, author_did, -1 FROM old_rows) u
            GROUP BY 1, 2 HAVING sum(d) <> 0) t;
  END IF;
  RETURN NULL;
END;
$$;

CREATE TRIGGER likes_count_insert AFTER INSERT ON likes
  REFERENCING NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION count_like_accounts();
CREATE TRIGGER likes_count_delete AFTER DELETE ON likes
  REFERENCING OLD TABLE AS old_rows FOR EACH STATEMENT EXECUTE FUNCTION count_like_accounts();
CREATE TRIGGER likes_count_update AFTER UPDATE ON likes
  REFERENCING OLD TABLE AS old_rows NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION count_like_accounts();
CREATE TRIGGER reposts_count_insert AFTER INSERT ON reposts
  REFERENCING NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION count_repost_accounts();
CREATE TRIGGER reposts_count_delete AFTER DELETE ON reposts
  REFERENCING OLD TABLE AS old_rows FOR EACH STATEMENT EXECUTE FUNCTION count_repost_accounts();
CREATE TRIGGER reposts_count_update AFTER UPDATE ON reposts
  REFERENCING OLD TABLE AS old_rows NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION count_repost_accounts();

-- Backfill: every existing count again, by accounts.
UPDATE post_counts c SET like_count = COALESCE(l.n, 0), repost_count = COALESCE(r.n, 0)
  FROM post_counts c0
  LEFT JOIN (SELECT subject_uri, count(DISTINCT author_did)::int AS n FROM likes GROUP BY subject_uri) l
    ON l.subject_uri = c0.post_uri
  LEFT JOIN (SELECT subject_uri, count(DISTINCT author_did)::int AS n FROM reposts GROUP BY subject_uri) r
    ON r.subject_uri = c0.post_uri
  WHERE c.post_uri = c0.post_uri
    AND (c.like_count <> COALESCE(l.n, 0) OR c.repost_count <> COALESCE(r.n, 0));
INSERT INTO post_counts (post_uri, like_count, repost_count)
  SELECT u, sum(l)::int, sum(r)::int FROM (
    SELECT subject_uri AS u, count(DISTINCT author_did) AS l, 0 AS r FROM likes GROUP BY subject_uri
    UNION ALL SELECT subject_uri, 0, count(DISTINCT author_did) FROM reposts GROUP BY subject_uri
  ) t GROUP BY u
  ON CONFLICT (post_uri) DO NOTHING;
