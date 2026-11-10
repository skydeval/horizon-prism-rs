-- Deferred counts inside a writer transaction. A transaction that defers
-- (`prism_defer_counts()`, which sets `prism.defer_counts` for it) has its
-- count triggers append their deltas to session-temporary tables instead of
-- touching the count tables; `prism_apply_counts()` then applies them once,
-- in one fixed order: `post_counts` by (author, rkey), `actor_counts` by id,
-- `tag_activity_hourly` by (tag, hour). So two writers whose transactions
-- touch the same hot rows in different orders cannot deadlock on them, and
-- a hot row is locked only from that statement to commit. A session that
-- never defers (an operator's psql, every other writer) counts at once, as
-- before. Rows of a rolled-back savepoint go with it.

-- Whether this transaction defers its counts: the setting is on and the
-- session has the tables.
CREATE FUNCTION prism_counts_deferred() RETURNS boolean LANGUAGE plpgsql STABLE AS $$
BEGIN
  RETURN COALESCE(current_setting('prism.defer_counts', true), '') = 'on'
     AND to_regclass('pg_temp.prism_count_deltas') IS NOT NULL;
END $$;

-- Deltas left at commit by a writer that deferred and did not apply: applied
-- then, logged and counted in `worker_state` ('writer_counts_left').
CREATE FUNCTION prism_counts_left() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  IF EXISTS (SELECT 1 FROM pg_temp.prism_count_deltas) OR EXISTS (SELECT 1 FROM pg_temp.prism_tag_deltas) THEN
    PERFORM set_config('prism.defer_counts', 'off', true);
    PERFORM prism_apply_count_deltas();
    RAISE WARNING 'prism: a writer transaction left deferred counts unapplied; they were applied at commit';
    INSERT INTO worker_state (key, value, updated_at) VALUES ('writer_counts_left', '1'::jsonb, now())
      ON CONFLICT (key) DO UPDATE SET value = to_jsonb(COALESCE((worker_state.value #>> '{}')::bigint, 0) + 1), updated_at = now();
  END IF;
  RETURN NULL;
END $$;

-- Defer this transaction's counts: the session's tables, made once, and the
-- setting for this transaction. A marker row arms the check at commit.
CREATE FUNCTION prism_defer_counts() RETURNS void LANGUAGE plpgsql AS $$
BEGIN
  IF to_regclass('pg_temp.prism_count_marker') IS NULL THEN
    -- kind: 1 like, 2 repost (a, b: the subject post; actor: the author),
    -- 3 reply, 4 quote (a, b: the post counted), 5 posts, 6 follows,
    -- 7 followers (a: the account).
    CREATE TEMP TABLE IF NOT EXISTS prism_count_deltas (kind smallint NOT NULL, a integer NOT NULL, b bigint NOT NULL, actor integer NOT NULL, delta integer NOT NULL) ON COMMIT DELETE ROWS;
    CREATE TEMP TABLE IF NOT EXISTS prism_tag_deltas (tag text, hour timestamptz, dposts integer, dlikes integer, dreposts integer, dreplies integer) ON COMMIT DELETE ROWS;
    CREATE TEMP TABLE prism_count_marker (x integer) ON COMMIT DELETE ROWS;
    CREATE CONSTRAINT TRIGGER prism_counts_left AFTER INSERT ON pg_temp.prism_count_marker
      DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION prism_counts_left();
  END IF;
  INSERT INTO pg_temp.prism_count_marker VALUES (1);
  PERFORM set_config('prism.defer_counts', 'on', true);
END $$;

-- The deltas applied, in key order, and emptied. Like and repost counts keep
-- their distinct-account rule: each (subject, author) pair's summed delta is
-- weighed once against the rows as they stand.
CREATE FUNCTION prism_apply_count_deltas() RETURNS void LANGUAGE plpgsql AS $$
BEGIN
  -- post_counts: every key any kind touches, in one ascending pass.
  INSERT INTO post_counts (author, rkey)
    SELECT DISTINCT a, b FROM pg_temp.prism_count_deltas WHERE kind IN (1, 2, 3, 4) AND delta > 0 ORDER BY a, b
    ON CONFLICT (author, rkey) DO NOTHING;
  PERFORM 1 FROM post_counts c
    WHERE (c.author, c.rkey) IN (SELECT a, b FROM pg_temp.prism_count_deltas WHERE kind IN (1, 2, 3, 4))
    ORDER BY c.author, c.rkey FOR UPDATE;
  PERFORM adjust_account_counts('like', array_agg(t.a ORDER BY t.a, t.b, t.actor), array_agg(t.b ORDER BY t.a, t.b, t.actor),
                                array_agg(t.actor ORDER BY t.a, t.b, t.actor), array_agg(t.d ORDER BY t.a, t.b, t.actor))
    FROM (SELECT a, b, actor, sum(delta)::int AS d FROM pg_temp.prism_count_deltas WHERE kind = 1 GROUP BY 1, 2, 3 HAVING sum(delta) <> 0) t;
  PERFORM adjust_account_counts('repost', array_agg(t.a ORDER BY t.a, t.b, t.actor), array_agg(t.b ORDER BY t.a, t.b, t.actor),
                                array_agg(t.actor ORDER BY t.a, t.b, t.actor), array_agg(t.d ORDER BY t.a, t.b, t.actor))
    FROM (SELECT a, b, actor, sum(delta)::int AS d FROM pg_temp.prism_count_deltas WHERE kind = 2 GROUP BY 1, 2, 3 HAVING sum(delta) <> 0) t;
  UPDATE post_counts c SET reply_count = GREATEST(c.reply_count + x.r, 0), quote_count = GREATEST(c.quote_count + x.q, 0)
    FROM (SELECT a, b, COALESCE(sum(delta) FILTER (WHERE kind = 3), 0)::int AS r, COALESCE(sum(delta) FILTER (WHERE kind = 4), 0)::int AS q
          FROM pg_temp.prism_count_deltas WHERE kind IN (3, 4) GROUP BY 1, 2) x
    WHERE c.author = x.a AND c.rkey = x.b AND (x.r <> 0 OR x.q <> 0);
  -- actor_counts, by id.
  INSERT INTO actor_counts (id)
    SELECT DISTINCT a FROM pg_temp.prism_count_deltas WHERE kind IN (5, 6, 7) AND delta > 0 ORDER BY a
    ON CONFLICT (id) DO NOTHING;
  PERFORM 1 FROM actor_counts c WHERE c.id IN (SELECT a FROM pg_temp.prism_count_deltas WHERE kind IN (5, 6, 7)) ORDER BY c.id FOR UPDATE;
  UPDATE actor_counts c SET posts_count = GREATEST(c.posts_count + x.p, 0), follows_count = GREATEST(c.follows_count + x.f, 0),
                            followers_count = GREATEST(c.followers_count + x.r, 0)
    FROM (SELECT a, COALESCE(sum(delta) FILTER (WHERE kind = 5), 0)::int AS p, COALESCE(sum(delta) FILTER (WHERE kind = 6), 0)::int AS f,
                 COALESCE(sum(delta) FILTER (WHERE kind = 7), 0)::int AS r
          FROM pg_temp.prism_count_deltas WHERE kind IN (5, 6, 7) GROUP BY 1) x
    WHERE c.id = x.a AND (x.p <> 0 OR x.f <> 0 OR x.r <> 0);
  -- tag_activity_hourly, by (tag, hour), through the immediate path.
  PERFORM tag_activity_apply(ARRAY(SELECT ROW(t.tag, t.hour, t.dposts, t.dlikes, t.dreposts, t.dreplies)::tag_delta FROM pg_temp.prism_tag_deltas t));
  DELETE FROM pg_temp.prism_count_deltas;
  DELETE FROM pg_temp.prism_tag_deltas;
END $$;

-- The writer's call, once per transaction after its last statement that can
-- fire a count trigger: counts from here on are immediate.
CREATE FUNCTION prism_apply_counts() RETURNS void LANGUAGE plpgsql AS $$
BEGIN
  IF to_regclass('pg_temp.prism_count_deltas') IS NULL THEN
    RETURN;
  END IF;
  PERFORM set_config('prism.defer_counts', 'off', true);
  PERFORM prism_apply_count_deltas();
END $$;

-- Like and repost counts by accounts (migration 0056), deferred when the
-- transaction defers.
CREATE OR REPLACE FUNCTION count_interaction_accounts() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
  k smallint := CASE TG_ARGV[0] WHEN 'like' THEN 1 ELSE 2 END;
BEGIN
  IF prism_counts_deferred() THEN
    IF TG_OP = 'INSERT' THEN
      INSERT INTO pg_temp.prism_count_deltas (kind, a, b, actor, delta)
        SELECT k, subj_author, subj_rkey, author, count(*)::int FROM new_rows WHERE subj_coll = 1 GROUP BY 2, 3, 4;
    ELSIF TG_OP = 'DELETE' THEN
      INSERT INTO pg_temp.prism_count_deltas (kind, a, b, actor, delta)
        SELECT k, subj_author, subj_rkey, author, -count(*)::int FROM old_rows WHERE subj_coll = 1 GROUP BY 2, 3, 4;
    ELSE
      INSERT INTO pg_temp.prism_count_deltas (kind, a, b, actor, delta)
        SELECT k, sa, sr, a, sum(d)::int FROM (
          SELECT subj_author AS sa, subj_rkey AS sr, author AS a, 1 AS d FROM new_rows WHERE subj_coll = 1
          UNION ALL SELECT subj_author, subj_rkey, author, -1 FROM old_rows WHERE subj_coll = 1) u
        GROUP BY 2, 3, 4 HAVING sum(d) <> 0;
    END IF;
    RETURN NULL;
  END IF;
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

-- Post counts (migration 0057), deferred when the transaction defers.
CREATE OR REPLACE FUNCTION count_posts() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  IF prism_counts_deferred() THEN
    IF TG_OP IN ('UPDATE', 'DELETE') THEN
      INSERT INTO pg_temp.prism_count_deltas (kind, a, b, actor, delta)
        SELECT v.k, v.a, v.b, 0, -1 FROM (VALUES
          (3::smallint, OLD.parent_author, OLD.parent_rkey, OLD.parent_coll = 1),
          (4::smallint, OLD.quote_author, OLD.quote_rkey, OLD.quote_coll = 1),
          (5::smallint, OLD.author, 0::bigint, true)) AS v(k, a, b, on_)
        WHERE v.on_;
    END IF;
    IF TG_OP IN ('INSERT', 'UPDATE') THEN
      INSERT INTO pg_temp.prism_count_deltas (kind, a, b, actor, delta)
        SELECT v.k, v.a, v.b, 0, 1 FROM (VALUES
          (3::smallint, NEW.parent_author, NEW.parent_rkey, NEW.parent_coll = 1),
          (4::smallint, NEW.quote_author, NEW.quote_rkey, NEW.quote_coll = 1),
          (5::smallint, NEW.author, 0::bigint, true)) AS v(k, a, b, on_)
        WHERE v.on_;
    END IF;
    RETURN NULL;
  END IF;
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

-- Follow counts (migration 0056), deferred when the transaction defers.
CREATE OR REPLACE FUNCTION count_follows() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  IF prism_counts_deferred() THEN
    IF TG_OP IN ('UPDATE', 'DELETE') THEN
      INSERT INTO pg_temp.prism_count_deltas (kind, a, b, actor, delta) VALUES (6, OLD.author, 0, 0, -1), (7, OLD.subject, 0, 0, -1);
    END IF;
    IF TG_OP IN ('INSERT', 'UPDATE') THEN
      INSERT INTO pg_temp.prism_count_deltas (kind, a, b, actor, delta) VALUES (6, NEW.author, 0, 0, 1), (7, NEW.subject, 0, 0, 1);
    END IF;
    RETURN NULL;
  END IF;
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

-- Tag activity (migration 0016): every tag trigger's deltas pass here; a
-- deferring transaction keeps them for `prism_apply_counts()`.
CREATE OR REPLACE FUNCTION tag_activity_apply(deltas tag_delta[]) RETURNS void LANGUAGE plpgsql AS $$
BEGIN
  IF prism_counts_deferred() THEN
    INSERT INTO pg_temp.prism_tag_deltas (tag, hour, dposts, dlikes, dreposts, dreplies)
      SELECT d.tag, d.hour, d.dposts, d.dlikes, d.dreposts, d.dreplies FROM unnest(deltas) d WHERE d.tag IS NOT NULL;
    RETURN;
  END IF;
  INSERT INTO tag_activity_hourly AS t (tag, hour, posts, likes, reposts, replies)
  SELECT d.tag, d.hour, sum(d.dposts), sum(d.dlikes), sum(d.dreposts), sum(d.dreplies)
  FROM unnest(deltas) d
  WHERE d.tag IS NOT NULL
  GROUP BY d.tag, d.hour
  HAVING sum(d.dposts) <> 0 OR sum(d.dlikes) <> 0 OR sum(d.dreposts) <> 0 OR sum(d.dreplies) <> 0
  ORDER BY d.tag, d.hour
  ON CONFLICT (tag, hour) DO UPDATE SET
    posts   = GREATEST(t.posts   + EXCLUDED.posts,   0),
    likes   = GREATEST(t.likes   + EXCLUDED.likes,   0),
    reposts = GREATEST(t.reposts + EXCLUDED.reposts, 0),
    replies = GREATEST(t.replies + EXCLUDED.replies, 0);
END $$;
