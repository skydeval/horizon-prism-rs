-- Private posting S4 (design private-posting.md §3.1, §4.4, P-18): stock
-- `app.bsky.feed.threadgate` and `postgate` records whose `post` is a
-- `tools.kryphocron.feed.postPrivate` URI. They mirror `threadgates` and
-- `postgates` (migration 0014) column for column, and are written by the
-- private tier in every public-indexing mode.
--
-- - A gate counts only when its rkey is its post's rkey and its author is
--   its post's author; other rows are stored and ignored.
-- - Joins are on `post_uri` (§3.5).
--
-- New tables only: no existing row is touched.
CREATE TABLE private_threadgates (
  uri            text PRIMARY KEY,
  author_did     text NOT NULL,
  rkey           text COLLATE "C" NOT NULL,
  cid            text NOT NULL,
  rev            text COLLATE "C" NOT NULL,
  post_uri       text NOT NULL,
  allow          jsonb,                        -- NULL = field absent (anyone may reply); [] = nobody
  hidden_replies text[] NOT NULL DEFAULT '{}',
  record         jsonb NOT NULL,
  created_at     timestamptz NOT NULL,
  indexed_at     timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX private_threadgates_post   ON private_threadgates (post_uri);
CREATE INDEX private_threadgates_author ON private_threadgates (author_did);

CREATE TABLE private_postgates (
  uri            text PRIMARY KEY,
  author_did     text NOT NULL,
  rkey           text COLLATE "C" NOT NULL,
  cid            text NOT NULL,
  rev            text COLLATE "C" NOT NULL,
  post_uri       text NOT NULL,
  embedding_rules jsonb,                       -- [{$type: …#disableRule}] or NULL
  detached_embedding_uris text[] NOT NULL DEFAULT '{}',
  record         jsonb NOT NULL,
  created_at     timestamptz NOT NULL,
  indexed_at     timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX private_postgates_post   ON private_postgates (post_uri);
CREATE INDEX private_postgates_author ON private_postgates (author_did);
