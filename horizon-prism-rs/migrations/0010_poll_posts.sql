-- Nearhorizon polls (app.nearhorizon.poll.post; design-v3 §3.1).
CREATE TABLE nearhorizon_poll_posts (
  uri               text PRIMARY KEY,
  author_did        text NOT NULL,
  rkey              text COLLATE "C" NOT NULL,
  cid               text NOT NULL,
  rev               text COLLATE "C" NOT NULL,
  text              text NOT NULL,
  options           jsonb NOT NULL,            -- [{id, label, media?}], 2..6
  visibility        text NOT NULL,
  multi_select      boolean,
  result_visibility text,
  deadline          timestamptz,               -- NULL when the record's string is not a datetime
  record            jsonb NOT NULL,
  created_at        timestamptz NOT NULL,
  indexed_at        timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX nearhorizon_poll_posts_author ON nearhorizon_poll_posts (author_did, created_at DESC);
