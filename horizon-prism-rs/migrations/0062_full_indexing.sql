-- Full indexing: per-account crawl and continuity state, crawl marks, relay
-- account statuses, and accounts excluded after Refract ended.
-- Every table keys accounts by `dids.id`; revs stay text.

-- One row per account full indexing follows. `generation` is the crawl
-- generation the row's state belongs to; `groups` the record groups the
-- row still owes.
CREATE TABLE full_crawl (
  account           integer PRIMARY KEY REFERENCES dids (id),
  generation        bigint  NOT NULL,
  groups            text[]  NOT NULL DEFAULT '{}',
  state             text    NOT NULL DEFAULT 'pending'
                    CHECK (state IN ('pending', 'fetching', 'done', 'failed', 'gone', 'skipped')),
  rev               text,
  attempts          integer NOT NULL DEFAULT 0,
  next_at           timestamptz NOT NULL DEFAULT now(),
  reason            text,
  epoch             bigint  NOT NULL DEFAULT 0,
  claim             bigint  NOT NULL DEFAULT 0,
  last_rev          text,
  chain_rev         text,
  chain_from        text,
  chain_generation  bigint,
  aside_rev         text,
  aside_ms          bigint,
  aside_seen_ms     bigint,
  aside_relay       text,
  uncovered         boolean NOT NULL DEFAULT false,
  uncovered_requeue bigint
);
CREATE INDEX full_crawl_claim ON full_crawl (state, next_at, generation);

-- A crawl mark: the account's crawl applied this group at these
-- generations. It counts only while both are current.
CREATE TABLE full_crawl_groups (
  account          integer NOT NULL REFERENCES dids (id),
  grp              text    NOT NULL,
  group_generation bigint  NOT NULL,
  crawl_generation bigint  NOT NULL,
  crawled_at       timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (account, grp)
);

-- Non-active account statuses a relay reports; a return to active deletes
-- the row.
CREATE TABLE relay_account_status (
  account integer NOT NULL REFERENCES dids (id),
  relay   text    NOT NULL,
  status  text    NOT NULL,
  at      timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (account, relay)
);

-- Accounts whose Refract restoration ended: not admitted by full indexing
-- unless they are members.
CREATE TABLE refract_excluded (
  account    integer PRIMARY KEY REFERENCES dids (id),
  since      timestamptz NOT NULL DEFAULT now(),
  checked_at timestamptz NOT NULL DEFAULT now()
);
