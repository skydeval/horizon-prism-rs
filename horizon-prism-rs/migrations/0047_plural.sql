-- Pronouns and plural systems (plural-pronouns design §1, §2.3; #896).
--
-- - `plural_records`: the current records of four collections, for any
--   DID (`app.nearhorizon.actor.pronouns`, `app.nearhorizon.plural.system`
--   and `app.nearhorizon.plural.front` at rkey `self`, and
--   `app.nearhorizon.plural.member`), written by the firehose and by the
--   per-DID listing. `record` is NULL for a record over the size bound.
--   `indexed_at` is set on every write, so a listing can tell rows written
--   before it began from rows written after.
-- - `plural_repos`: what Prism knows of a DID's repo for these collections:
--   pending | complete | unavailable | failed | gone. `complete` means the
--   repo was listed at `verified_at` and every change since reaches Prism
--   through the firehose, so an absent record is provably absent.
-- - `private_posts.fronters`: a kryphocron post's clear
--   `app.nearhorizon.plural.fronters` value. Every row is marked not
--   filled, so the refill `serve` queues once (worker_state
--   'private_fronters_refill') lists each DID's private records again and
--   fills the new column. A few thousand rows at this release.
CREATE TABLE plural_records (
  uri text PRIMARY KEY,
  did text NOT NULL,
  rkey text COLLATE "C" NOT NULL,
  cid text NOT NULL,
  rev text COLLATE "C" NOT NULL,
  collection text NOT NULL,
  record jsonb,
  indexed_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX plural_records_did ON plural_records (did, collection, rkey);

CREATE TABLE plural_repos (
  did text PRIMARY KEY,
  state text NOT NULL CHECK (state IN ('pending', 'complete', 'unavailable', 'failed', 'gone')),
  verified_at timestamptz,
  next_fill_at timestamptz,
  attempts integer NOT NULL DEFAULT 0,
  members_truncated boolean NOT NULL DEFAULT false,
  last_error text,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX plural_repos_due ON plural_repos (next_fill_at) WHERE next_fill_at IS NOT NULL;

ALTER TABLE private_posts ADD COLUMN fronters jsonb;
UPDATE private_posts SET display_filled = false;
