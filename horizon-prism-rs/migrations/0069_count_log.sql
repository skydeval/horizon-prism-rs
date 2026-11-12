-- Counts from a log. The count triggers no longer touch `post_counts`,
-- `actor_counts` or `tag_activity_hourly`: they append each change to an
-- insert-only log in the row's own transaction, and a folder applies the
-- log to the count tables every interval (`prism_fold_counts`). For every
-- count key, the count row plus the log rows of that key is what counting
-- at once would have left. Nothing defers any more.
--
-- kind: 1 like, 2 repost (a, b: the subject post; actor: the author),
-- 3 reply, 4 quote (a, b: the post counted), 5 posts, 6 follows,
-- 7 followers (a: the account; b and actor 0). Every row of a key has the
-- key's `a`, so a fold of the rows with `a % slices = slice` keeps a key,
-- and a like's (subject, author) pair, whole.
--
-- No index and no key: writers only insert, so eight writers never share
-- an index page; the folder deletes every visible row of a slice and
-- vacuums the tables itself, and autovacuum does when it is down.
CREATE TABLE count_log (
  kind  smallint NOT NULL,
  a     integer  NOT NULL,
  b     bigint   NOT NULL,
  actor integer  NOT NULL,
  delta integer  NOT NULL
) WITH (autovacuum_vacuum_scale_factor = 0, autovacuum_vacuum_threshold = 10000, autovacuum_vacuum_insert_scale_factor = 0);

CREATE TABLE count_log_tags (
  tag      text        NOT NULL,
  hour     timestamptz NOT NULL,
  dposts   integer     NOT NULL,
  dlikes   integer     NOT NULL,
  dreposts integer     NOT NULL,
  dreplies integer     NOT NULL
) WITH (autovacuum_vacuum_scale_factor = 0, autovacuum_vacuum_threshold = 10000, autovacuum_vacuum_insert_scale_factor = 0);

-- The counts lock: held by a fold, and by every other writer of a count
-- row for its one count statement. The two-int form, apart from the DID and
-- URI locks' one-bigint keys.
CREATE FUNCTION prism_fold_lock() RETURNS void LANGUAGE sql VOLATILE AS $$
  SELECT pg_advisory_xact_lock(1129270868, 1)
$$;

-- One fold: the log rows of one slice applied to the count tables and
-- deleted, in the caller's transaction. Log rows of kinds 1–7 with
-- `a % slices = slice`; tag rows with `(hashtext(tag) & 2147483647) %
-- tag_slices = slice` (`tag_slices` defaults to `slices`); a table whose
-- slice count is not above `slice` is left alone. The lock is waited for
-- with `wait`, else tried: (-1, -1) when another holds it. Returns the log
-- rows and tag rows folded. Call it in READ COMMITTED: each statement then
-- sees every transaction that committed before it began, the previous
-- fold's included.
--
-- Likes and reposts count accounts: the statement that deletes a pair's
-- rows reads the pair's like rows `n` in the same snapshot, so its change
-- is `(n > 0) - (n - d > 0)` with `d` the pair's summed rows. Keys with a
-- raw positive change get a count row; values are clamped at zero.
-- `SELECT prism_fold_counts(true, 1, 0)` folds everything now (psql).
CREATE FUNCTION prism_fold_counts(wait boolean, slices integer, slice integer, tag_slices integer DEFAULT NULL,
                                  OUT log_rows bigint, OUT tag_rows bigint)
LANGUAGE plpgsql VOLATILE AS $$
DECLARE
  ts integer := COALESCE(tag_slices, slices);
  got bigint;
  pa int[]; pb bigint[]; pl int[]; pr int[]; pp int[]; pq int[]; ppos boolean[];
  aa int[]; ap int[]; af int[]; afr int[]; apos boolean[];
BEGIN
  log_rows := 0;
  tag_rows := 0;
  IF wait THEN
    PERFORM prism_fold_lock();
  ELSIF NOT pg_try_advisory_xact_lock(1129270868, 1) THEN
    log_rows := -1;
    tag_rows := -1;
    RETURN;
  END IF;
  IF slice < slices THEN
    -- Post counts: the rows deleted and each key's change, in one snapshot.
    WITH d AS (
      DELETE FROM count_log WHERE kind BETWEEN 1 AND 4 AND (slices <= 1 OR a % slices = slice)
      RETURNING kind, a, b, actor, delta
    ), pairs AS (
      SELECT kind, a, b, actor, sum(delta)::int AS d, bool_or(delta > 0) AS pos FROM d WHERE kind IN (1, 2) GROUP BY 1, 2, 3, 4
    ), ch AS (
      SELECT p.kind, p.a, p.b, CASE WHEN p.d = 0 THEN 0 ELSE (n.cnt > 0)::int - (n.cnt - p.d > 0)::int END AS c, p.pos
      FROM pairs p CROSS JOIN LATERAL (
        SELECT CASE WHEN p.kind = 1
                    THEN (SELECT count(*) FROM likes l WHERE l.subj_author = p.a AND l.subj_rkey = p.b AND l.subj_coll = 1 AND l.author = p.actor)
                    ELSE (SELECT count(*) FROM reposts r WHERE r.subj_author = p.a AND r.subj_rkey = p.b AND r.subj_coll = 1 AND r.author = p.actor)
               END AS cnt) n
      UNION ALL
      SELECT kind, a, b, delta, delta > 0 FROM d WHERE kind IN (3, 4)
    ), k AS (
      SELECT a, b,
             COALESCE(sum(c) FILTER (WHERE kind = 1), 0)::int AS l, COALESCE(sum(c) FILTER (WHERE kind = 2), 0)::int AS r,
             COALESCE(sum(c) FILTER (WHERE kind = 3), 0)::int AS p, COALESCE(sum(c) FILTER (WHERE kind = 4), 0)::int AS q,
             bool_or(pos) AS pos
      FROM ch GROUP BY a, b
    )
    SELECT (SELECT count(*) FROM d), array_agg(a ORDER BY a, b), array_agg(b ORDER BY a, b), array_agg(l ORDER BY a, b),
           array_agg(r ORDER BY a, b), array_agg(p ORDER BY a, b), array_agg(q ORDER BY a, b), array_agg(pos ORDER BY a, b)
      INTO got, pa, pb, pl, pr, pp, pq, ppos
      FROM k;
    log_rows := log_rows + got;
    IF pa IS NOT NULL THEN
      INSERT INTO post_counts (author, rkey)
        SELECT t.a, t.b FROM unnest(pa, pb, ppos) AS t(a, b, pos) WHERE t.pos ORDER BY 1, 2
        ON CONFLICT (author, rkey) DO NOTHING;
      UPDATE post_counts c SET like_count = GREATEST(c.like_count + t.l, 0), repost_count = GREATEST(c.repost_count + t.r, 0),
                               reply_count = GREATEST(c.reply_count + t.p, 0), quote_count = GREATEST(c.quote_count + t.q, 0)
        FROM unnest(pa, pb, pl, pr, pp, pq) AS t(a, b, l, r, p, q)
        WHERE c.author = t.a AND c.rkey = t.b AND (t.l <> 0 OR t.r <> 0 OR t.p <> 0 OR t.q <> 0);
    END IF;
    -- Account counts.
    WITH d AS (
      DELETE FROM count_log WHERE kind BETWEEN 5 AND 7 AND (slices <= 1 OR a % slices = slice)
      RETURNING kind, a, delta
    ), k AS (
      SELECT a, COALESCE(sum(delta) FILTER (WHERE kind = 5), 0)::int AS p, COALESCE(sum(delta) FILTER (WHERE kind = 6), 0)::int AS f,
             COALESCE(sum(delta) FILTER (WHERE kind = 7), 0)::int AS fr, bool_or(delta > 0) AS pos
      FROM d GROUP BY a
    )
    SELECT (SELECT count(*) FROM d), array_agg(a ORDER BY a), array_agg(p ORDER BY a), array_agg(f ORDER BY a),
           array_agg(fr ORDER BY a), array_agg(pos ORDER BY a)
      INTO got, aa, ap, af, afr, apos
      FROM k;
    log_rows := log_rows + got;
    IF aa IS NOT NULL THEN
      INSERT INTO actor_counts (id)
        SELECT t.a FROM unnest(aa, apos) AS t(a, pos) WHERE t.pos ORDER BY 1
        ON CONFLICT (id) DO NOTHING;
      UPDATE actor_counts c SET posts_count = GREATEST(c.posts_count + t.p, 0), follows_count = GREATEST(c.follows_count + t.f, 0),
                                followers_count = GREATEST(c.followers_count + t.fr, 0)
        FROM unnest(aa, ap, af, afr) AS t(a, p, f, fr)
        WHERE c.id = t.a AND (t.p <> 0 OR t.f <> 0 OR t.fr <> 0);
    END IF;
  END IF;
  IF slice < ts THEN
    -- Tags: hours already past the trends cutoff are dropped, as the
    -- retention would delete them once applied.
    WITH d AS (
      DELETE FROM count_log_tags WHERE ts <= 1 OR (hashtext(tag) & 2147483647) % ts = slice
      RETURNING tag, hour, dposts, dlikes, dreposts, dreplies
    ), ins AS (
      INSERT INTO tag_activity_hourly AS t (tag, hour, posts, likes, reposts, replies)
      SELECT d.tag, d.hour, sum(d.dposts), sum(d.dlikes), sum(d.dreposts), sum(d.dreplies)
      FROM d
      WHERE d.hour >= trends_cutoff()
      GROUP BY d.tag, d.hour
      HAVING sum(d.dposts) <> 0 OR sum(d.dlikes) <> 0 OR sum(d.dreposts) <> 0 OR sum(d.dreplies) <> 0
      ORDER BY d.tag, d.hour
      ON CONFLICT (tag, hour) DO UPDATE SET
        posts   = GREATEST(t.posts   + EXCLUDED.posts,   0),
        likes   = GREATEST(t.likes   + EXCLUDED.likes,   0),
        reposts = GREATEST(t.reposts + EXCLUDED.reposts, 0),
        replies = GREATEST(t.replies + EXCLUDED.replies, 0)
      RETURNING 1
    )
    SELECT count(*) INTO got FROM d;
    tag_rows := got;
  END IF;
END $$;

-- Like and repost counts by accounts: each statement's (subject, author)
-- pairs, logged.
CREATE OR REPLACE FUNCTION count_interaction_accounts() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
  k smallint := CASE TG_ARGV[0] WHEN 'like' THEN 1 ELSE 2 END;
BEGIN
  IF TG_OP = 'INSERT' THEN
    INSERT INTO count_log (kind, a, b, actor, delta)
      SELECT k, subj_author, subj_rkey, author, count(*)::int FROM new_rows
      WHERE subj_coll = 1 AND subj_author IS NOT NULL AND subj_rkey IS NOT NULL GROUP BY 2, 3, 4;
  ELSIF TG_OP = 'DELETE' THEN
    INSERT INTO count_log (kind, a, b, actor, delta)
      SELECT k, subj_author, subj_rkey, author, -count(*)::int FROM old_rows
      WHERE subj_coll = 1 AND subj_author IS NOT NULL AND subj_rkey IS NOT NULL GROUP BY 2, 3, 4;
  ELSE
    INSERT INTO count_log (kind, a, b, actor, delta)
      SELECT k, sa, sr, a, sum(d)::int FROM (
        SELECT subj_author AS sa, subj_rkey AS sr, author AS a, 1 AS d FROM new_rows WHERE subj_coll = 1
        UNION ALL SELECT subj_author, subj_rkey, author, -1 FROM old_rows WHERE subj_coll = 1) u
      WHERE sa IS NOT NULL AND sr IS NOT NULL
      GROUP BY 2, 3, 4 HAVING sum(d) <> 0;
  END IF;
  RETURN NULL;
END;
$$;

-- Post counts: a reply or quote toward its whole-post subject, a post
-- toward its author.
CREATE OR REPLACE FUNCTION count_posts() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  IF TG_OP IN ('UPDATE', 'DELETE') THEN
    INSERT INTO count_log (kind, a, b, actor, delta)
      SELECT v.k, v.a, v.b, 0, -1 FROM (VALUES
        (3::smallint, OLD.parent_author, OLD.parent_rkey, OLD.parent_coll = 1),
        (4::smallint, OLD.quote_author, OLD.quote_rkey, OLD.quote_coll = 1),
        (5::smallint, OLD.author, 0::bigint, true)) AS v(k, a, b, on_)
      WHERE v.on_ AND v.a IS NOT NULL AND v.b IS NOT NULL;
  END IF;
  IF TG_OP IN ('INSERT', 'UPDATE') THEN
    INSERT INTO count_log (kind, a, b, actor, delta)
      SELECT v.k, v.a, v.b, 0, 1 FROM (VALUES
        (3::smallint, NEW.parent_author, NEW.parent_rkey, NEW.parent_coll = 1),
        (4::smallint, NEW.quote_author, NEW.quote_rkey, NEW.quote_coll = 1),
        (5::smallint, NEW.author, 0::bigint, true)) AS v(k, a, b, on_)
      WHERE v.on_ AND v.a IS NOT NULL AND v.b IS NOT NULL;
  END IF;
  RETURN NULL;
END;
$$;

-- Follow counts: the author's follows and the subject's followers.
CREATE OR REPLACE FUNCTION count_follows() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  IF TG_OP IN ('UPDATE', 'DELETE') THEN
    INSERT INTO count_log (kind, a, b, actor, delta) VALUES (6, OLD.author, 0, 0, -1), (7, OLD.subject, 0, 0, -1);
  END IF;
  IF TG_OP IN ('INSERT', 'UPDATE') THEN
    INSERT INTO count_log (kind, a, b, actor, delta) VALUES (6, NEW.author, 0, 0, 1), (7, NEW.subject, 0, 0, 1);
  END IF;
  RETURN NULL;
END;
$$;

-- Tag activity: every tag trigger's deltas, summed per (tag, hour), logged.
CREATE OR REPLACE FUNCTION tag_activity_apply(deltas tag_delta[]) RETURNS void LANGUAGE plpgsql AS $$
BEGIN
  INSERT INTO count_log_tags (tag, hour, dposts, dlikes, dreposts, dreplies)
    SELECT d.tag, d.hour, sum(d.dposts), sum(d.dlikes), sum(d.dreposts), sum(d.dreplies)
    FROM unnest(deltas) d
    WHERE d.tag IS NOT NULL AND d.hour IS NOT NULL
    GROUP BY d.tag, d.hour
    HAVING sum(d.dposts) <> 0 OR sum(d.dlikes) <> 0 OR sum(d.dreposts) <> 0 OR sum(d.dreplies) <> 0;
END $$;

-- The deferral of migration 0067 is gone.
DROP FUNCTION prism_apply_counts();
DROP FUNCTION prism_defer_counts();
DROP FUNCTION prism_counts_left();
DROP FUNCTION prism_apply_count_deltas();
DROP FUNCTION prism_counts_deferred();
DROP FUNCTION adjust_account_counts(text, int[], bigint[], int[], int[]);
DELETE FROM worker_state WHERE key = 'writer_counts_left';

-- A full trial's capacity minute: how far counts were behind, at most.
ALTER TABLE full_trial_samples ADD COLUMN counts_behind double precision;
