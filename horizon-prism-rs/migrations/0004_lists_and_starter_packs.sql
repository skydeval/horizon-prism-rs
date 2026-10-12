-- Lists, list blocks and opt-outs, the rule-4 subscription tables, list
-- mutes, and starter packs (design-v3 §3.1, §4.3). New tables only.

-- ── Lists (app.bsky.graph.list / listitem / listblock) ──────────────────
CREATE TABLE lists (
  uri          text PRIMARY KEY,
  creator_did  text NOT NULL,
  rkey         text COLLATE "C" NOT NULL,
  cid          text NOT NULL,
  rev          text COLLATE "C" NOT NULL,
  name         text NOT NULL,
  purpose      text NOT NULL,                  -- full token, e.g. app.bsky.graph.defs#curatelist
  description  text,
  description_facets jsonb,
  avatar_cid   text,
  self_labels  text[],
  record       jsonb NOT NULL,
  created_at   timestamptz NOT NULL,           -- clamped as for posts
  indexed_at   timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX lists_creator ON lists (creator_did, created_at DESC);

-- Duplicate listitems (same list, same subject) are kept as rows and ignored
-- on read.
CREATE TABLE list_items (
  uri          text PRIMARY KEY,
  author_did   text NOT NULL,                  -- = authority(list_uri), enforced by the classifier
  rkey         text COLLATE "C" NOT NULL,
  cid          text NOT NULL,
  rev          text COLLATE "C" NOT NULL,
  list_uri     text NOT NULL,
  subject_did  text NOT NULL,
  created_at   timestamptz NOT NULL,
  indexed_at   timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX list_items_list    ON list_items (list_uri, created_at DESC);
CREATE INDEX list_items_subject ON list_items (subject_did);
CREATE INDEX list_items_author  ON list_items (author_did);

CREATE TABLE list_blocks (                     -- app.bsky.graph.listblock records
  uri          text PRIMARY KEY,
  author_did   text NOT NULL,
  rkey         text COLLATE "C" NOT NULL,
  cid          text NOT NULL,
  rev          text COLLATE "C" NOT NULL,
  list_uri     text NOT NULL,                  -- record .subject
  created_at   timestamptz NOT NULL,
  indexed_at   timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX list_blocks_author ON list_blocks (author_did, created_at DESC);
CREATE INDEX list_blocks_list   ON list_blocks (list_uri);

CREATE TABLE list_optouts (                    -- app.bsky.graph.referencelistoptout
  uri          text PRIMARY KEY,
  author_did   text NOT NULL,
  rkey         text COLLATE "C" NOT NULL,
  cid          text NOT NULL,
  rev          text COLLATE "C" NOT NULL,
  list_uri     text NOT NULL,
  created_at   timestamptz NOT NULL,
  indexed_at   timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX list_optouts_list   ON list_optouts (list_uri, author_did);
CREATE INDEX list_optouts_author ON list_optouts (author_did);

-- ── Rule-4 subscriptions (§4.3). Internal: no public read path reads these
-- six tables. They exist only to apply members' own list mutes and blocks. ──

-- One row per (member, list, source): a member's list block or list mute.
CREATE TABLE list_references (
  member_did  text NOT NULL,
  list_uri    text NOT NULL,
  source      text NOT NULL CHECK (source IN ('block', 'mute')),
  source_uri  text,                            -- the listblock URI for 'block'; NULL for 'mute'
  arrived_at  timestamptz NOT NULL DEFAULT now(),
  admitted    boolean NOT NULL,
  admitted_at timestamptz,
  effective   boolean NOT NULL DEFAULT false,  -- set only by a list_sync that served this admission
  PRIMARY KEY (member_did, list_uri, source)
);
CREATE INDEX list_references_list   ON list_references (list_uri) WHERE admitted;
CREATE INDEX list_references_member ON list_references (member_did, arrived_at, list_uri);

-- Change markers: the member phase inserts them, the list phase drains them.
-- A marker names only the list.
CREATE TABLE list_reference_changes (
  seq      bigserial PRIMARY KEY,
  list_uri text NOT NULL
);
CREATE INDEX list_reference_changes_list ON list_reference_changes (list_uri, seq);

-- One row per list with at least one admitted reference, or being released.
CREATE TABLE subscribed_lists (
  list_uri         text PRIMARY KEY,
  owner_did        text NOT NULL,              -- authority(list_uri)
  state            text NOT NULL CHECK (state IN ('active', 'unreferenced')),
  item_count       int NOT NULL DEFAULT 0,     -- subscribed_list_items rows; see the triggers
  truncated        boolean NOT NULL DEFAULT false,
  synced_at        timestamptz,
  activated_at     timestamptz NOT NULL DEFAULT now(),
  state_changed_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX subscribed_lists_owner ON subscribed_lists (owner_did);
CREATE INDEX subscribed_lists_state ON subscribed_lists (state, state_changed_at);

-- Tombstones for listitem deletes, and moves out of a subscribed list.
CREATE TABLE subscription_tombstones (
  uri        text PRIMARY KEY,
  owner_did  text NOT NULL,                    -- authority(uri); the per-owner cap's key
  rev        text COLLATE "C" NOT NULL,
  deleted_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX subscription_tombstones_owner ON subscription_tombstones (owner_did, deleted_at);

-- One row per owner of a subscribed list: sync bookkeeping and owner-gone
-- suspicion (list owners have no actors row).
CREATE TABLE subscribed_owners (
  owner_did           text PRIMARY KEY,
  last_completed_at   timestamptz,
  resync_requested_at timestamptz,
  last_run_ended_at   timestamptz,
  slow_until          timestamptz,
  cut_until           timestamptz,
  gone_suspect_at     timestamptz,
  gone_suspect_pds    text,                    -- a PDS URL, 'did:web' or 'none'
  owner_gone          boolean NOT NULL DEFAULT false
);

-- The items of subscribed lists, copied from the owner's repo. Separate from
-- list_items, so a subscription never changes a public row.
CREATE TABLE subscribed_list_items (
  uri           text PRIMARY KEY,              -- the listitem record URI
  list_uri      text NOT NULL,
  owner_did     text NOT NULL,
  rkey          text COLLATE "C" NOT NULL,
  cid           text NOT NULL,
  rev           text COLLATE "C" NOT NULL,
  subject_did   text NOT NULL,
  missing_count smallint NOT NULL DEFAULT 0,
  indexed_at    timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX subscribed_list_items_list    ON subscribed_list_items (list_uri, rkey);
CREATE INDEX subscribed_list_items_owner   ON subscribed_list_items (owner_did);
CREATE INDEX subscribed_list_items_subject ON subscribed_list_items (subject_did, list_uri);

-- subscribed_lists.item_count follows subscribed_list_items. Decrements never
-- go below zero, and a list_uri with no subscribed_lists row is a no-op, so
-- no write can fail on the count. Every list_sync completion re-sets it.
CREATE FUNCTION count_subscribed_list_items() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  IF TG_OP IN ('DELETE', 'UPDATE') THEN
    UPDATE subscribed_lists SET item_count = GREATEST(item_count - 1, 0) WHERE list_uri = OLD.list_uri;
  END IF;
  IF TG_OP IN ('INSERT', 'UPDATE') THEN
    UPDATE subscribed_lists SET item_count = item_count + 1 WHERE list_uri = NEW.list_uri;
  END IF;
  RETURN NULL;
END $$;
CREATE TRIGGER subscribed_list_items_count_insert
  AFTER INSERT ON subscribed_list_items
  FOR EACH ROW EXECUTE FUNCTION count_subscribed_list_items();
CREATE TRIGGER subscribed_list_items_count_delete
  AFTER DELETE ON subscribed_list_items
  FOR EACH ROW EXECUTE FUNCTION count_subscribed_list_items();
CREATE TRIGGER subscribed_list_items_count_move
  AFTER UPDATE OF list_uri ON subscribed_list_items
  FOR EACH ROW WHEN (OLD.list_uri IS DISTINCT FROM NEW.list_uri)
  EXECUTE FUNCTION count_subscribed_list_items();

-- AppView-private
CREATE TABLE list_mutes (
  viewer_did text NOT NULL,
  list_uri   text NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (viewer_did, list_uri)
);
CREATE INDEX list_mutes_list ON list_mutes (list_uri);

-- ── Starter packs (app.bsky.graph.starterpack) ─────────────────────────
CREATE TABLE starter_packs (
  uri          text PRIMARY KEY,
  creator_did  text NOT NULL,
  rkey         text COLLATE "C" NOT NULL,
  cid          text NOT NULL,
  rev          text COLLATE "C" NOT NULL,
  name         text NOT NULL,
  description  text,
  list_uri     text NOT NULL,
  feeds        jsonb,                          -- [{uri}], at most 3
  record       jsonb NOT NULL,
  search_tsv   tsvector GENERATED ALWAYS AS
                 (to_tsvector('simple', name || ' ' || coalesce(description, ''))) STORED,
  created_at   timestamptz NOT NULL,
  indexed_at   timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX starter_packs_creator ON starter_packs (creator_did, created_at DESC);
CREATE INDEX starter_packs_list    ON starter_packs (list_uri);
CREATE INDEX starter_packs_search  ON starter_packs USING gin (search_tsv);

CREATE TABLE starter_pack_counts (             -- derived; refreshed by a job
  pack_uri              text PRIMARY KEY,
  joined_week_count     int NOT NULL DEFAULT 0,
  joined_all_time_count int NOT NULL DEFAULT 0,
  all_time_source       text NOT NULL,         -- constellation|local
  all_time_daily        jsonb NOT NULL DEFAULT '[]',
  refreshed_at          timestamptz NOT NULL
);
