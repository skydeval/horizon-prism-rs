-- Full indexing's history crawl: per-account chain links, re-list
-- candidates, gaps, accounts kept incomplete by hand, and the crawl's own
-- identity rows. Every table keys accounts by `dids.id`; revs stay text.

-- A live commit whose `since` did not chain: `since -> rev`, written at the
-- stream-id milliseconds of the entry that wrote it.
CREATE TABLE full_chain_links (
  account   integer NOT NULL REFERENCES dids (id),
  since     text COLLATE "C" NOT NULL,
  rev       text COLLATE "C" NOT NULL,
  linked_ms bigint  NOT NULL,
  PRIMARY KEY (account, since, rev)
);
CREATE INDEX full_chain_links_linked ON full_chain_links (linked_ms);

-- A listed head a re-list pass read, compared once the stream has passed
-- the tail read with its page.
CREATE TABLE full_relist_candidates (
  account integer PRIMARY KEY REFERENCES dids (id),
  head    text COLLATE "C" NOT NULL,
  tail_ms bigint  NOT NULL,
  relay   text    NOT NULL,
  page_ms bigint  NOT NULL
);
CREATE INDEX full_relist_candidates_tail ON full_relist_candidates (tail_ms);

-- A source of a gap, resolved when a re-list pass that covers it ends.
CREATE TABLE full_gaps (
  id          bigserial PRIMARY KEY,
  kind        text    NOT NULL CHECK (kind IN ('trim', 'recreated', 'resume', 'continuity', 'periodic')),
  relay       text,
  blocking    boolean NOT NULL,
  opened_at   timestamptz NOT NULL DEFAULT now(),
  -- A `continuity` row is resolved only by a pass that started after the
  -- relay caught up.
  caught_up_at timestamptz,
  resolved_at timestamptz,
  detail      jsonb   NOT NULL DEFAULT '{}'
);
CREATE INDEX full_gaps_open ON full_gaps (opened_at) WHERE resolved_at IS NULL;

-- Accounts a group was marked Complete without, kept on today's sources.
CREATE TABLE full_incomplete (
  grp     text    NOT NULL,
  account integer NOT NULL REFERENCES dids (id),
  PRIMARY KEY (grp, account)
);

-- The crawl's identity rows from the directory's export: never used for
-- anything but the crawl.
CREATE TABLE crawl_identity (
  account     integer PRIMARY KEY REFERENCES dids (id),
  pds         text,
  signing_key text,
  seq         bigint  NOT NULL
);

-- Revs are TIDs, compared in byte order whatever the database's collation.
ALTER TABLE full_crawl
  ALTER COLUMN rev       TYPE text COLLATE "C",
  ALTER COLUMN last_rev  TYPE text COLLATE "C",
  ALTER COLUMN chain_rev TYPE text COLLATE "C",
  ALTER COLUMN chain_from TYPE text COLLATE "C",
  ALTER COLUMN aside_rev TYPE text COLLATE "C";

-- The Aside timer's sweep.
CREATE INDEX full_crawl_aside ON full_crawl (aside_ms) WHERE aside_ms IS NOT NULL AND NOT uncovered;
