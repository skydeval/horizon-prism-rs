-- Thread outline index (docs/internal/design/thread-outline.md §2): the reply
-- structure of every public post the firehose delivers, with no text.
-- Author, record key, parent, root, time and rev, with a mark when deleted.
-- Written by the firehose consumer's outline writer only; read by Thread
-- Lens. The tables are new and empty, so the indexes are built here.

CREATE TABLE outline_actors (
  id   integer GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  did  text NOT NULL UNIQUE
);

CREATE TABLE outline_posts (
  author         integer NOT NULL,
  rkey           bigint  NOT NULL,      -- the TID, decoded
  parent_author  integer,               -- NULL for a thread root
  parent_rkey    bigint,
  root_author    integer,
  root_rkey      bigint,
  created_at     timestamptz NOT NULL,  -- the record's, clamped as posts are
  seen_at        timestamptz NOT NULL,  -- the stream entry's time, set on insert only
  rev            bigint  NOT NULL,      -- the commit rev, decoded
  deleted_at     timestamptz,           -- set by a delete; the row stays
  PRIMARY KEY (author, rkey)
);
CREATE INDEX outline_posts_parent ON outline_posts (parent_author, parent_rkey) WHERE parent_author IS NOT NULL;
CREATE INDEX outline_posts_root ON outline_posts (root_author, root_rkey) INCLUDE (parent_author, parent_rkey) WHERE root_author IS NOT NULL;
CREATE INDEX outline_posts_seen ON outline_posts (seen_at);

-- Deletes of posts whose create was never seen.
CREATE TABLE outline_deleted (
  author  integer NOT NULL,
  rkey    bigint  NOT NULL,
  rev     bigint  NOT NULL,
  seen_at timestamptz NOT NULL,
  PRIMARY KEY (author, rkey)
);

-- Each relay's latest account status for an author; the highest seq wins.
CREATE TABLE outline_gone (
  author  integer NOT NULL,
  relay   text    NOT NULL,
  active  boolean NOT NULL,
  seq     bigint  NOT NULL,
  PRIMARY KEY (author, relay)
);

-- A node known to miss a reply under it.
CREATE TABLE outline_holes (
  author  integer NOT NULL,
  rkey    bigint  NOT NULL,
  seen_at timestamptz NOT NULL,
  PRIMARY KEY (author, rkey)
);
