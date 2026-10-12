-- Search and trend inputs on posts, typeahead prefixes on actors, and the
-- hourly tag aggregate with its triggers (design-v3 §3.1, §7.5).
--
-- The posts columns are plain and nullable: the ingest writer fills them,
-- and the post-migration task (§12.2) fills existing rows and then builds
-- the posts indexes. No index on posts, likes or reposts is built here.
ALTER TABLE posts
  ADD COLUMN tags text[],                      -- facet #tags ∪ record .tags: lowercased NFC, no '#', sorted, distinct
  ADD COLUMN link_domains text[],              -- hosts of facet links and external embeds
  ADD COLUMN link_urls text[],                 -- normalized URLs of the same
  ADD COLUMN lang_prefixes text[],             -- lowercased langs ∪ their primary subtags
  ADD COLUMN embed_type text,                  -- $type of the top-level embed
  ADD COLUMN media_type text,                  -- images|video|gallery
  ADD COLUMN search_tsv tsvector;              -- to_tsvector('simple', text)

-- actors is small relative to posts.
CREATE INDEX actors_handle_prefix ON actors (handle text_pattern_ops);
CREATE INDEX actors_name_prefix   ON actors (lower(display_name) text_pattern_ops);

-- ── Trend aggregation (derived, trigger-maintained) ─────────────────────
-- A decrement for an hour with no row inserts a negative count; every
-- reader clamps with GREATEST(col, 0).
CREATE TABLE tag_activity_hourly (
  tag        text NOT NULL,                    -- lowercased, NFC, without '#'
  hour       timestamptz NOT NULL,             -- the hour the counted post, like, repost or reply was created
  posts      int NOT NULL DEFAULT 0,
  likes      int NOT NULL DEFAULT 0,
  reposts    int NOT NULL DEFAULT 0,
  replies    int NOT NULL DEFAULT 0,
  PRIMARY KEY (tag, hour)
);
CREATE INDEX tag_activity_hourly_hour ON tag_activity_hourly (hour DESC);

-- Rows older than this are not aggregated. The setting is read with the
-- missing_ok form, so a session where it is unset still writes, with the
-- default.
CREATE FUNCTION trends_cutoff() RETURNS timestamptz LANGUAGE sql STABLE AS $$
  SELECT now() - COALESCE(NULLIF(current_setting('prism.trends_retention_hours', true), ''), '72')::int * interval '1 hour'
$$;

-- Engagement counts toward a tag only when its subject post was created
-- within this long before the engagement.
CREATE FUNCTION trends_lookback() RETURNS interval LANGUAGE sql STABLE AS $$
  SELECT COALESCE(NULLIF(current_setting('prism.trends_subject_lookback_hours', true), ''), '72')::int * interval '1 hour'
$$;

CREATE TYPE tag_delta AS (tag text, hour timestamptz, dposts int, dlikes int, dreposts int, dreplies int);

-- One upsert per trigger, with deltas aggregated first: each key appears
-- once, so the statement never proposes the same row twice, and keys are
-- taken in ascending order.
CREATE FUNCTION tag_activity_apply(deltas tag_delta[]) RETURNS void LANGUAGE sql AS $$
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
    replies = GREATEST(t.replies + EXCLUDED.replies, 0)
$$;

-- What one posts row contributes, times `sign`: 1 post for each of its own
-- tags, and 1 reply for each tag of its parent, both at the row's hour.
CREATE FUNCTION tag_deltas_of_post(tags text[], created timestamptz, parent_uri text, sign int)
RETURNS tag_delta[] LANGUAGE sql STABLE AS $$
  SELECT COALESCE(array_agg(d), '{}') FROM (
    SELECT ROW(u.tag, date_trunc('hour', created, 'UTC'), sign, 0, 0, 0)::tag_delta AS d
    FROM unnest(tags) AS u(tag)
    UNION ALL
    SELECT ROW(u.tag, date_trunc('hour', created, 'UTC'), 0, 0, 0, sign)::tag_delta
    FROM posts p, unnest(p.tags) AS u(tag)
    WHERE parent_uri IS NOT NULL AND p.uri = parent_uri AND p.created_at >= created - trends_lookback()
  ) s
$$;

CREATE FUNCTION tag_activity_posts() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
  deltas tag_delta[] := '{}';
BEGIN
  IF TG_OP IN ('DELETE', 'UPDATE') AND OLD.created_at >= trends_cutoff() THEN
    deltas := deltas || tag_deltas_of_post(OLD.tags, OLD.created_at, OLD.reply_parent_uri, -1);
  END IF;
  IF TG_OP IN ('INSERT', 'UPDATE') AND NEW.created_at >= trends_cutoff() THEN
    deltas := deltas || tag_deltas_of_post(NEW.tags, NEW.created_at, NEW.reply_parent_uri, 1);
  END IF;
  PERFORM tag_activity_apply(deltas);
  RETURN NULL;
END $$;

-- Each event has its own trigger, so its WHEN clause can skip old rows
-- before the function runs.
CREATE TRIGGER posts_tag_activity_insert
  AFTER INSERT ON posts
  FOR EACH ROW WHEN (NEW.created_at >= trends_cutoff())
  EXECUTE FUNCTION tag_activity_posts();
CREATE TRIGGER posts_tag_activity_delete
  AFTER DELETE ON posts
  FOR EACH ROW WHEN (OLD.created_at >= trends_cutoff())
  EXECUTE FUNCTION tag_activity_posts();
CREATE TRIGGER posts_tag_activity_update
  AFTER UPDATE OF tags, created_at, reply_parent_uri ON posts
  FOR EACH ROW WHEN (OLD.created_at >= trends_cutoff() OR NEW.created_at >= trends_cutoff())
  EXECUTE FUNCTION tag_activity_posts();

-- What one likes or reposts row contributes, times `sign`: 1 for each tag of
-- its subject post, at the row's hour. A subject not stored here adds
-- nothing. TG_ARGV[0] names the counter.
CREATE FUNCTION tag_deltas_of_engagement(subject text, created timestamptz, sign int, counter text)
RETURNS tag_delta[] LANGUAGE sql STABLE AS $$
  SELECT COALESCE(array_agg(ROW(
           u.tag, date_trunc('hour', created, 'UTC'), 0,
           CASE WHEN counter = 'likes' THEN sign ELSE 0 END,
           CASE WHEN counter = 'reposts' THEN sign ELSE 0 END,
           0)::tag_delta), '{}')
  FROM posts p, unnest(p.tags) AS u(tag)
  WHERE p.uri = subject AND p.created_at >= created - trends_lookback()
$$;

CREATE FUNCTION tag_activity_engagement() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
  deltas tag_delta[] := '{}';
BEGIN
  IF TG_OP IN ('DELETE', 'UPDATE') AND OLD.created_at >= trends_cutoff() THEN
    deltas := deltas || tag_deltas_of_engagement(OLD.subject_uri, OLD.created_at, -1, TG_ARGV[0]);
  END IF;
  IF TG_OP IN ('INSERT', 'UPDATE') AND NEW.created_at >= trends_cutoff() THEN
    deltas := deltas || tag_deltas_of_engagement(NEW.subject_uri, NEW.created_at, 1, TG_ARGV[0]);
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
  AFTER UPDATE OF created_at, subject_uri ON likes
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
  AFTER UPDATE OF created_at, subject_uri ON reposts
  FOR EACH ROW WHEN (OLD.created_at >= trends_cutoff() OR NEW.created_at >= trends_cutoff())
  EXECUTE FUNCTION tag_activity_engagement('reposts');
