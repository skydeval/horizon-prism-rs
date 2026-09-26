-- horizon-prism-rs initial schema (design-v2 §4.2, triggers §9.5).
-- Forward-only. The §4.2 DDL is authoritative; every rev, rkey and TID text
-- column uses COLLATE "C" so lexicographic order equals TID order (§4.1).

CREATE EXTENSION IF NOT EXISTS pg_trgm;

-- Identity
CREATE TABLE actors (
  did               text PRIMARY KEY,
  handle            text,                      -- verified handle or NULL
  handle_checked_at timestamptz,
  pds_endpoint      text,                      -- from DID doc #atproto_pds
  account_status    text NOT NULL DEFAULT 'active', -- active|deactivated|takendown|suspended|deleted
  deletion_suspect_at  timestamptz,              -- first RepoNotFound/404 observation (§5.6)
  deletion_suspect_pds text,                     -- PDS endpoint that produced it
  status_checked_at timestamptz,
  display_name      text,
  description       text,
  avatar_cid        text,
  banner_cid        text,
  profile_rev       text COLLATE "C",
  profile_record    jsonb,
  indexed_at        timestamptz NOT NULL DEFAULT now()
);                                              -- retention S
CREATE INDEX actors_handle_trgm ON actors USING gin (handle gin_trgm_ops);
CREATE INDEX actors_name_trgm   ON actors USING gin (display_name gin_trgm_ops);

CREATE TABLE did_cache (
  did          text PRIMARY KEY,
  doc          jsonb NOT NULL,
  fetched_at   timestamptz NOT NULL,
  stale_at     timestamptz NOT NULL,             -- fetched_at + 1h
  expires_at   timestamptz NOT NULL              -- fetched_at + 24h
);                                              -- retention T (expires_at + 7d)

-- Community and scope
CREATE TABLE members (
  did        text PRIMARY KEY,
  source     text NOT NULL,                      -- admin|auto_enroll
  added_at   timestamptz NOT NULL DEFAULT now(),
  ready_at   timestamptz,                        -- set/cleared per §6.11.2; NULL = block state not proven complete (§7.5)
  first_ready_at timestamptz                     -- set once, never cleared; never-ready expiry uses this (§9.2)
);                                              -- retention P
CREATE TABLE scope_dids (
  did        text PRIMARY KEY,
  reasons    smallint NOT NULL,                  -- bit 1 member, 2 followed-by-member, 4 member-interacted
  added_at   timestamptz NOT NULL DEFAULT now(),
  justified_at timestamptz NOT NULL DEFAULT now() -- last time a rebuild confirmed it
);                                              -- retention: rebuild (§9.4)

-- Public content (class Ecosystem, §5.4)
CREATE TABLE posts (
  uri            text PRIMARY KEY,
  author_did     text NOT NULL,
  rkey           text COLLATE "C" NOT NULL,
  cid            text NOT NULL,
  rev            text COLLATE "C" NOT NULL,
  text           text NOT NULL DEFAULT '',
  langs          text[],
  reply_root_uri   text, reply_root_cid   text,
  reply_parent_uri text, reply_parent_cid text,
  quote_uri      text,                           -- app.bsky.embed.record(.WithMedia) target
  embed          jsonb,
  facets         jsonb,
  mention_dids   text[],
  self_labels    text[],
  record         jsonb NOT NULL,                 -- normalized ATProto JSON (§5.8)
  created_at     timestamptz NOT NULL,           -- record createdAt, clamped to indexed_at + 5 min
  indexed_at     timestamptz NOT NULL DEFAULT now()
);                                              -- retention S; inbound replies to members M
CREATE INDEX posts_author_created ON posts (author_did, created_at DESC);
CREATE INDEX posts_parent  ON posts (reply_parent_uri);
CREATE INDEX posts_root    ON posts (reply_root_uri);
CREATE INDEX posts_quote   ON posts (quote_uri);

CREATE TABLE likes (
  uri text PRIMARY KEY, author_did text NOT NULL, subject_uri text NOT NULL,
  subject_cid text, rev text COLLATE "C" NOT NULL,
  created_at timestamptz NOT NULL, indexed_at timestamptz NOT NULL DEFAULT now()
);                                              -- retention S / M
CREATE INDEX likes_subject ON likes (subject_uri, created_at DESC);
CREATE INDEX likes_author  ON likes (author_did, created_at DESC);

CREATE TABLE reposts (LIKE likes INCLUDING ALL); -- identical shape
CREATE TABLE follows (
  uri text PRIMARY KEY, author_did text NOT NULL, subject_did text NOT NULL,
  rev text COLLATE "C" NOT NULL,
  created_at timestamptz NOT NULL, indexed_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX follows_author  ON follows (author_did, created_at DESC);
CREATE INDEX follows_subject ON follows (subject_did, created_at DESC);
CREATE TABLE blocks (LIKE follows INCLUDING ALL); -- app.bsky.graph.block only

CREATE TABLE feed_items (
  uri           text PRIMARY KEY,               -- post uri or repost uri
  originator_did text NOT NULL,
  post_uri      text NOT NULL,
  kind          text NOT NULL,                  -- post|reply|repost
  sort_at       timestamptz NOT NULL
);                                              -- follows its source row
CREATE INDEX feed_items_originator ON feed_items (originator_did, sort_at DESC);

-- Deletion tombstones for LWW (§5.5)
CREATE TABLE record_tombstones (
  uri text PRIMARY KEY, rev text COLLATE "C" NOT NULL,
  deleted_at timestamptz NOT NULL DEFAULT now()
);                                              -- retention T (7 days)

-- Aggregates (trigger-maintained, §9.5)
CREATE TABLE post_counts (
  post_uri text PRIMARY KEY,
  like_count int NOT NULL DEFAULT 0, repost_count int NOT NULL DEFAULT 0,
  reply_count int NOT NULL DEFAULT 0, quote_count int NOT NULL DEFAULT 0
);
CREATE TABLE actor_counts (
  did text PRIMARY KEY,
  followers_count int NOT NULL DEFAULT 0, follows_count int NOT NULL DEFAULT 0,
  posts_count int NOT NULL DEFAULT 0
);
CREATE TABLE constellation_counts (
  target text PRIMARY KEY,                       -- at-uri or did
  likes int, reposts int, replies int, quotes int, followers int,
  fetched_at timestamptz NOT NULL
);                                              -- retention T (unrefreshed 7 days)

-- Viewer-private AppView state
CREATE TABLE mutes (
  viewer_did text NOT NULL, subject_did text NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (viewer_did, subject_did)
);                                              -- retention P
CREATE TABLE notifications (
  id            bigserial PRIMARY KEY,
  recipient_did text NOT NULL,
  author_did    text NOT NULL,
  reason        text NOT NULL,                  -- like|repost|follow|reply|mention|quote
  reason_subject text,
  record_uri    text NOT NULL,
  record_cid    text NOT NULL,
  indexed_at    timestamptz NOT NULL DEFAULT now(),
  UNIQUE (recipient_did, record_uri, reason)
);                                              -- retention T (90 days)
CREATE INDEX notifications_recipient ON notifications (recipient_did, indexed_at DESC, id DESC);
CREATE INDEX notifications_record ON notifications (record_uri);
CREATE TABLE notification_seen (
  did text PRIMARY KEY, seen_at timestamptz NOT NULL
);                                              -- retention P

-- Moderation
CREATE TABLE labels (
  id bigserial PRIMARY KEY,
  subject text NOT NULL,                        -- at-uri or did
  val text NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now(),
  UNIQUE (subject, val)
);                                              -- retention P

-- Private tier (class Kryphocron(Private), §5.4). Queried only inside private_tier (§3.2).
CREATE TABLE private_posts (
  uri              text PRIMARY KEY,
  author_did       text NOT NULL,
  rkey             text COLLATE "C" NOT NULL,
  cid              text NOT NULL,
  rev              text COLLATE "C" NOT NULL,
  audience_uri     text,                         -- audienceList (string or {uri})
  reply_root_uri   text,
  reply_parent_uri text,
  encoded_content  bytea,                        -- as received, hex-decoded (§5.8)
  encoded_codec    text,
  encoded_generation text,
  plaintext_text   text,                         -- lexicon XOR rule permits plaintext
  created_at       timestamptz NOT NULL,
  indexed_at       timestamptz NOT NULL DEFAULT now()
);                                              -- retention S
CREATE INDEX private_posts_author ON private_posts (author_did, created_at DESC);
CREATE INDEX private_posts_parent ON private_posts (reply_parent_uri);
CREATE TABLE private_audiences (
  uri text PRIMARY KEY, owner_did text NOT NULL, rkey text COLLATE "C" NOT NULL,
  cid text NOT NULL, rev text COLLATE "C" NOT NULL,
  mode text,                                    -- NULL treated as 'list' (mirrors Aurora)
  members text[] NOT NULL DEFAULT '{}',
  indexed_at timestamptz NOT NULL DEFAULT now()
);                                              -- retention S
CREATE TABLE private_blocks (
  uri text PRIMARY KEY, author_did text NOT NULL, subject_did text NOT NULL,
  rev text COLLATE "C" NOT NULL, created_at timestamptz NOT NULL,
  indexed_at timestamptz NOT NULL DEFAULT now()
);                                              -- retention S
CREATE INDEX private_blocks_pair ON private_blocks (author_did, subject_did);
CREATE TABLE private_tombstones (                -- LWW tombstones for private-tier URIs (§5.5)
  uri text PRIMARY KEY, rev text COLLATE "C" NOT NULL,
  deleted_at timestamptz NOT NULL DEFAULT now()
);                                              -- retention T (7 days)
CREATE TABLE private_owner_ready (               -- owner readiness (§6.11.2)
  did text PRIMARY KEY, ready_at timestamptz NOT NULL
);                                              -- deleted when readiness is cleared or owner status is lost (§6.11.1)
CREATE TABLE private_owner_eligible (            -- owner eligibility confirmation (§6.11.1)
  did text PRIMARY KEY, confirmed_at timestamptz NOT NULL
);                                              -- refreshed at most daily; deleted with owner status

-- Work queues and worker state
CREATE TABLE backfill_jobs (
  did text NOT NULL, depth text NOT NULL,        -- member|scope|private|authz|resync (§5.10)
  state text NOT NULL DEFAULT 'queued',          -- queued|running|done|failed|cancelled
  gap_epoch bigint,                              -- epoch recorded before getLatestCommit (§6.11.2)
  attempts int NOT NULL DEFAULT 0, not_before timestamptz NOT NULL DEFAULT now(),
  last_error text, updated_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (did, depth)
);                                              -- retention T (done/failed/cancelled 30 days); readiness lives in members.ready_at / private_owner_ready
CREATE TABLE sync_state (                        -- repo-head check for tracked DIDs (§6.11.3)
  did text PRIMARY KEY,
  applied_rev text COLLATE "C",                  -- highest rev applied from any source
  head_rev text COLLATE "C", head_checked_at timestamptz,
  diverged_since timestamptz,
  last_resync_at timestamptz
);                                              -- rows exist only for tracked DIDs; deleted when a DID stops being tracked
CREATE TABLE identity_refresh (
  did text PRIMARY KEY, reasons smallint NOT NULL, -- bitmask: 1 identity, 2 account, 4 handle, 8 tombstone, 16 stale; upsert ORs bits
  not_before timestamptz NOT NULL DEFAULT now(), attempts int NOT NULL DEFAULT 0
);                                              -- rows deleted when processed
CREATE TABLE admin_commands (                    -- CLI → serve command queue (§3.3)
  id bigserial PRIMARY KEY, command text NOT NULL, args jsonb NOT NULL,
  state text NOT NULL DEFAULT 'queued',          -- queued|running|done|failed
  result jsonb, created_at timestamptz NOT NULL DEFAULT now(), finished_at timestamptz
);                                              -- retention T (finished 30 days)
CREATE TABLE purging_dids (                      -- purge in progress (§6.11.7); admission rejects these DIDs
  did text PRIMARY KEY, kind text NOT NULL,      -- account_deleted|out_of_scope|member_removed
  started_at timestamptz NOT NULL DEFAULT now()
);                                              -- row deleted when the batched purge completes
CREATE TABLE worker_state (
  key text PRIMARY KEY, value jsonb NOT NULL, updated_at timestamptz NOT NULL DEFAULT now()
);                                              -- keys: 'watermark:<relay label>' (rfc3339; absent = no entry yet = fails closed), 'gap_epoch' (int), 'claim_in_progress' (bool), 'last_gap' (rfc3339)

-- ---------------------------------------------------------------------------
-- Aggregation triggers (§9.5)
--
-- AFTER INSERT / DELETE / UPDATE OF <subject columns>, row level. Every
-- function applies "decrement for OLD, increment for NEW": the LWW upsert
-- (§5.5) sets every column, so UPDATE OF fires even when a subject column is
-- unchanged, and the pair then cancels out. Increments are upserts;
-- decrements clamp at 0 and touch only existing count rows. An
-- ON CONFLICT ... DO UPDATE ... WHERE that is false updates nothing and fires
-- nothing. private_posts has no count triggers (§9.5).
-- ---------------------------------------------------------------------------

CREATE FUNCTION count_likes() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  IF TG_OP IN ('UPDATE', 'DELETE') THEN
    UPDATE post_counts SET like_count = GREATEST(like_count - 1, 0)
      WHERE post_uri = OLD.subject_uri;
  END IF;
  IF TG_OP IN ('INSERT', 'UPDATE') THEN
    INSERT INTO post_counts (post_uri, like_count) VALUES (NEW.subject_uri, 1)
      ON CONFLICT (post_uri) DO UPDATE SET like_count = post_counts.like_count + 1;
  END IF;
  RETURN NULL;
END;
$$;
CREATE TRIGGER likes_count
  AFTER INSERT OR DELETE OR UPDATE OF subject_uri ON likes
  FOR EACH ROW EXECUTE FUNCTION count_likes();

CREATE FUNCTION count_reposts() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  IF TG_OP IN ('UPDATE', 'DELETE') THEN
    UPDATE post_counts SET repost_count = GREATEST(repost_count - 1, 0)
      WHERE post_uri = OLD.subject_uri;
  END IF;
  IF TG_OP IN ('INSERT', 'UPDATE') THEN
    INSERT INTO post_counts (post_uri, repost_count) VALUES (NEW.subject_uri, 1)
      ON CONFLICT (post_uri) DO UPDATE SET repost_count = post_counts.repost_count + 1;
  END IF;
  RETURN NULL;
END;
$$;
CREATE TRIGGER reposts_count
  AFTER INSERT OR DELETE OR UPDATE OF subject_uri ON reposts
  FOR EACH ROW EXECUTE FUNCTION count_reposts();

CREATE FUNCTION count_posts() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  IF TG_OP IN ('UPDATE', 'DELETE') THEN
    IF OLD.reply_parent_uri IS NOT NULL THEN
      UPDATE post_counts SET reply_count = GREATEST(reply_count - 1, 0)
        WHERE post_uri = OLD.reply_parent_uri;
    END IF;
    IF OLD.quote_uri IS NOT NULL THEN
      UPDATE post_counts SET quote_count = GREATEST(quote_count - 1, 0)
        WHERE post_uri = OLD.quote_uri;
    END IF;
    UPDATE actor_counts SET posts_count = GREATEST(posts_count - 1, 0)
      WHERE did = OLD.author_did;
  END IF;
  IF TG_OP IN ('INSERT', 'UPDATE') THEN
    IF NEW.reply_parent_uri IS NOT NULL THEN
      INSERT INTO post_counts (post_uri, reply_count) VALUES (NEW.reply_parent_uri, 1)
        ON CONFLICT (post_uri) DO UPDATE SET reply_count = post_counts.reply_count + 1;
    END IF;
    IF NEW.quote_uri IS NOT NULL THEN
      INSERT INTO post_counts (post_uri, quote_count) VALUES (NEW.quote_uri, 1)
        ON CONFLICT (post_uri) DO UPDATE SET quote_count = post_counts.quote_count + 1;
    END IF;
    INSERT INTO actor_counts (did, posts_count) VALUES (NEW.author_did, 1)
      ON CONFLICT (did) DO UPDATE SET posts_count = actor_counts.posts_count + 1;
  END IF;
  RETURN NULL;
END;
$$;
CREATE TRIGGER posts_count
  AFTER INSERT OR DELETE OR UPDATE OF reply_parent_uri, quote_uri, author_did ON posts
  FOR EACH ROW EXECUTE FUNCTION count_posts();

CREATE FUNCTION count_follows() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  IF TG_OP IN ('UPDATE', 'DELETE') THEN
    UPDATE actor_counts SET follows_count = GREATEST(follows_count - 1, 0)
      WHERE did = OLD.author_did;
    UPDATE actor_counts SET followers_count = GREATEST(followers_count - 1, 0)
      WHERE did = OLD.subject_did;
  END IF;
  IF TG_OP IN ('INSERT', 'UPDATE') THEN
    INSERT INTO actor_counts (did, follows_count) VALUES (NEW.author_did, 1)
      ON CONFLICT (did) DO UPDATE SET follows_count = actor_counts.follows_count + 1;
    INSERT INTO actor_counts (did, followers_count) VALUES (NEW.subject_did, 1)
      ON CONFLICT (did) DO UPDATE SET followers_count = actor_counts.followers_count + 1;
  END IF;
  RETURN NULL;
END;
$$;
CREATE TRIGGER follows_count
  AFTER INSERT OR DELETE OR UPDATE OF author_did, subject_did ON follows
  FOR EACH ROW EXECUTE FUNCTION count_follows();
