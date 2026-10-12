-- Thread and post gates (design-v3 §3.1).
CREATE TABLE threadgates (
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
CREATE INDEX threadgates_post   ON threadgates (post_uri);
CREATE INDEX threadgates_author ON threadgates (author_did);

CREATE TABLE postgates (
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
CREATE INDEX postgates_post   ON postgates (post_uri);
CREATE INDEX postgates_author ON postgates (author_did);
