-- Compact storage: lists and their records, starter packs and their counts,
-- threadgates and postgates, polls, labeler services, self records and
-- members' post references, keyed by interned ids.
--
-- A row is `(author, rkey)`: the author's `dids` id and the record key as its
-- TID value, or a negative `rkey_aliases` id for any other key. A labeler
-- service is keyed by its author alone and a self record by `(author, coll)`:
-- their key is always `self`. CIDs are bytes, revs integers in rev order.
--
-- References are `(*_author, *_coll, *_rkey)`: a list block's and an
-- opt-out's subject, a starter pack's list and a gate's post. One that
-- `at_uri` would not give back exactly is odd: `*_coll = 0`, its text in
-- `odd_refs`, and `*_author` kept when its authority is a valid DID. A list
-- item names a list in its own repo, so it keeps only the list's key. A
-- member's post reference keeps the accounts its post replies to and quotes.
--
-- Records are kept whole as `dict id ‖ payload`, dict 0 (the JSON text)
-- here; a background job compresses them later. Every other copy of a record
-- field is gone, except what a query filters or searches on: a list's
-- purpose, a poll's visibility, a starter pack's search text and a self
-- record's `hideFromAlgorithmicRecommendations` and `allowSubscriptions`.
--
-- One transaction: intern, copy, then the swap, indexes and triggers. A row
-- that cannot be keyed (an author that is not a valid DID, a record key that
-- is not valid, a URI that is not its author's own record, a list item whose
-- list is not a list in its own repo, a self record of a collection outside
-- the fixed list) is left out and counted in the log. Ingest checks all of
-- these, so none are expected. Pack counts of anything but a whole starter
-- pack reference are dropped and counted.

-- Every valid DID the tables name: authors, list item subjects, the
-- authorities of references and counted packs, and the accounts members'
-- posts reply to and quote.
INSERT INTO dids (did)
SELECT x.d FROM (
  SELECT creator_did AS d FROM lists
  UNION SELECT author_did FROM list_items
  UNION SELECT subject_did FROM list_items
  UNION SELECT author_did FROM list_blocks
  UNION SELECT split_part(substr(list_uri, 6), '/', 1) FROM list_blocks WHERE left(list_uri, 5) = 'at://'
  UNION SELECT author_did FROM list_optouts
  UNION SELECT split_part(substr(list_uri, 6), '/', 1) FROM list_optouts WHERE left(list_uri, 5) = 'at://'
  UNION SELECT creator_did FROM starter_packs
  UNION SELECT split_part(substr(list_uri, 6), '/', 1) FROM starter_packs WHERE left(list_uri, 5) = 'at://'
  UNION SELECT split_part(substr(pack_uri, 6), '/', 1) FROM starter_pack_counts WHERE left(pack_uri, 5) = 'at://'
  UNION SELECT author_did FROM threadgates
  UNION SELECT split_part(substr(post_uri, 6), '/', 1) FROM threadgates WHERE left(post_uri, 5) = 'at://'
  UNION SELECT author_did FROM postgates
  UNION SELECT split_part(substr(post_uri, 6), '/', 1) FROM postgates WHERE left(post_uri, 5) = 'at://'
  UNION SELECT author_did FROM nearhorizon_poll_posts
  UNION SELECT creator_did FROM labeler_services
  UNION SELECT did FROM actor_self_records
  UNION SELECT author_did FROM member_post_refs
  UNION SELECT reply_parent_did FROM member_post_refs
  UNION SELECT reply_root_did FROM member_post_refs
  UNION SELECT quote_did FROM member_post_refs
) x
WHERE did_is_valid(x.d) AND NOT EXISTS (SELECT 1 FROM dids a WHERE a.did = x.d)
ORDER BY x.d
ON CONFLICT (did) DO NOTHING;

-- Every reference text the copy reads that could be whole (three parts, a
-- listed collection).
CREATE TEMP TABLE refs_named ON COMMIT DROP AS
SELECT u AS uri FROM (
  SELECT list_uri AS u FROM list_blocks
  UNION SELECT list_uri FROM list_optouts
  UNION SELECT list_uri FROM starter_packs
  UNION SELECT post_uri FROM threadgates
  UNION SELECT post_uri FROM postgates
  UNION SELECT pack_uri FROM starter_pack_counts
) r
WHERE left(u, 5) = 'at://'
  AND cardinality(string_to_array(substr(u, 6), '/')) = 3
  AND split_part(substr(u, 6), '/', 2) IN (SELECT nsid FROM collections);

-- Aliases for every valid record key that is not a TID with its top bit
-- clear: the rows' own keys, the lists list items name, and the keys of
-- references.
INSERT INTO rkey_aliases (author, rkey)
SELECT DISTINCT d.id, k.rkey FROM (
  SELECT creator_did AS did, rkey FROM lists
  UNION SELECT author_did, rkey FROM list_items
  UNION SELECT author_did, split_part(list_uri, '/', 5) FROM list_items
  UNION SELECT author_did, rkey FROM list_blocks
  UNION SELECT author_did, rkey FROM list_optouts
  UNION SELECT creator_did, rkey FROM starter_packs
  UNION SELECT author_did, rkey FROM threadgates
  UNION SELECT author_did, rkey FROM postgates
  UNION SELECT author_did, rkey FROM nearhorizon_poll_posts
  UNION SELECT author_did, split_part(uri, '/', 5) FROM member_post_refs
  UNION SELECT split_part(substr(uri, 6), '/', 1), split_part(substr(uri, 6), '/', 3) FROM refs_named
) k
JOIN dids d ON d.did = k.did
WHERE rkey_is_valid(k.rkey) AND rkey_to_bigint(k.rkey) IS NULL
  AND NOT EXISTS (SELECT 1 FROM rkey_aliases x WHERE x.author = d.id AND x.rkey = k.rkey)
ORDER BY 1, 2
ON CONFLICT (author, rkey) DO NOTHING;

-- A record key of `author`'s repo as stored: its TID value or its alias.
-- NULL for a key that is not valid.
CREATE FUNCTION pg_temp.own_key(author integer, rkey text) RETURNS bigint LANGUAGE sql STABLE AS $$
  SELECT CASE WHEN rkey_is_valid(rkey) THEN
    COALESCE(rkey_to_bigint(rkey), (SELECT x.id FROM rkey_aliases x WHERE x.author = own_key.author AND x.rkey = own_key.rkey)) END
$$;

-- A CID or record in its exact text form, dict 0.
CREATE FUNCTION pg_temp.text_bytes(s text) RETURNS bytea LANGUAGE sql IMMUTABLE AS $$
  SELECT '\x00'::bytea || convert_to(s, 'UTF8')
$$;

-- The old rows with their new keys.
CREATE TEMP TABLE lists_keyed ON COMMIT DROP AS
SELECT a.id AS author, pg_temp.own_key(a.id, l.rkey) AS rkey, l.cid, l.rev, l.purpose, l.record, l.created_at, l.indexed_at
FROM lists l JOIN dids a ON a.did = l.creator_did
WHERE l.uri = 'at://' || l.creator_did || '/app.bsky.graph.list/' || l.rkey;
DELETE FROM lists_keyed WHERE rkey IS NULL;

CREATE TEMP TABLE list_items_keyed ON COMMIT DROP AS
SELECT a.id AS author, pg_temp.own_key(a.id, i.rkey) AS rkey, i.cid, i.rev,
       pg_temp.own_key(a.id, split_part(i.list_uri, '/', 5)) AS list_rkey, s.id AS subject, i.created_at
FROM list_items i JOIN dids a ON a.did = i.author_did JOIN dids s ON s.did = i.subject_did
WHERE i.uri = 'at://' || i.author_did || '/app.bsky.graph.listitem/' || i.rkey
  AND i.list_uri = 'at://' || i.author_did || '/app.bsky.graph.list/' || split_part(i.list_uri, '/', 5);
DELETE FROM list_items_keyed WHERE rkey IS NULL OR list_rkey IS NULL;

CREATE TEMP TABLE list_blocks_keyed ON COMMIT DROP AS
SELECT a.id AS author, pg_temp.own_key(a.id, b.rkey) AS rkey, b.cid, b.rev, b.list_uri, b.created_at
FROM list_blocks b JOIN dids a ON a.did = b.author_did
WHERE b.uri = 'at://' || b.author_did || '/app.bsky.graph.listblock/' || b.rkey;
DELETE FROM list_blocks_keyed WHERE rkey IS NULL;

CREATE TEMP TABLE list_optouts_keyed ON COMMIT DROP AS
SELECT a.id AS author, pg_temp.own_key(a.id, o.rkey) AS rkey, o.cid, o.rev, o.list_uri, o.created_at
FROM list_optouts o JOIN dids a ON a.did = o.author_did
WHERE o.uri = 'at://' || o.author_did || '/app.bsky.graph.referencelistoptout/' || o.rkey;
DELETE FROM list_optouts_keyed WHERE rkey IS NULL;

CREATE TEMP TABLE starter_packs_keyed ON COMMIT DROP AS
SELECT a.id AS author, pg_temp.own_key(a.id, s.rkey) AS rkey, s.cid, s.rev, s.list_uri, s.record, s.search_tsv, s.created_at, s.indexed_at
FROM starter_packs s JOIN dids a ON a.did = s.creator_did
WHERE s.uri = 'at://' || s.creator_did || '/app.bsky.graph.starterpack/' || s.rkey;
DELETE FROM starter_packs_keyed WHERE rkey IS NULL;

CREATE TEMP TABLE threadgates_keyed ON COMMIT DROP AS
SELECT a.id AS author, pg_temp.own_key(a.id, g.rkey) AS rkey, g.cid, g.rev, g.post_uri, g.record, g.created_at
FROM threadgates g JOIN dids a ON a.did = g.author_did
WHERE g.uri = 'at://' || g.author_did || '/app.bsky.feed.threadgate/' || g.rkey;
DELETE FROM threadgates_keyed WHERE rkey IS NULL;

CREATE TEMP TABLE postgates_keyed ON COMMIT DROP AS
SELECT a.id AS author, pg_temp.own_key(a.id, g.rkey) AS rkey, g.cid, g.rev, g.post_uri, g.record, g.created_at
FROM postgates g JOIN dids a ON a.did = g.author_did
WHERE g.uri = 'at://' || g.author_did || '/app.bsky.feed.postgate/' || g.rkey;
DELETE FROM postgates_keyed WHERE rkey IS NULL;

CREATE TEMP TABLE polls_keyed ON COMMIT DROP AS
SELECT a.id AS author, pg_temp.own_key(a.id, p.rkey) AS rkey, p.cid, p.rev, p.visibility, p.record, p.created_at, p.indexed_at
FROM nearhorizon_poll_posts p JOIN dids a ON a.did = p.author_did
WHERE p.uri = 'at://' || p.author_did || '/app.nearhorizon.poll.post/' || p.rkey;
DELETE FROM polls_keyed WHERE rkey IS NULL;

CREATE TEMP TABLE labelers_keyed ON COMMIT DROP AS
SELECT a.id AS author, s.cid, s.rev, s.record, s.created_at, s.indexed_at
FROM labeler_services s JOIN dids a ON a.did = s.creator_did
WHERE s.uri = 'at://' || s.creator_did || '/app.bsky.labeler.service/self';

CREATE TEMP TABLE self_records_keyed ON COMMIT DROP AS
SELECT a.id AS author, c.id AS coll, r.cid, r.rev, r.record, r.indexed_at
FROM actor_self_records r JOIN dids a ON a.did = r.did JOIN collections c ON c.nsid = r.collection
WHERE r.uri = 'at://' || r.did || '/' || r.collection || '/self';

CREATE TEMP TABLE post_refs_keyed ON COMMIT DROP AS
SELECT a.id AS author, pg_temp.own_key(a.id, split_part(m.uri, '/', 5)) AS rkey, m.rev,
       (SELECT d.id FROM dids d WHERE d.did = m.reply_parent_did) AS parent,
       (SELECT d.id FROM dids d WHERE d.did = m.reply_root_did) AS root,
       (SELECT d.id FROM dids d WHERE d.did = m.quote_did) AS quote,
       m.created_at
FROM member_post_refs m JOIN dids a ON a.did = m.author_did
WHERE m.uri = 'at://' || m.author_did || '/app.bsky.feed.post/' || split_part(m.uri, '/', 5);
DELETE FROM post_refs_keyed WHERE rkey IS NULL;

-- The new tables, without triggers, filled from the keyed rows. Every CID
-- is kept in its exact text form and every record as dict 0.
CREATE TABLE lists_compact (
  author     integer     NOT NULL,
  rkey       bigint      NOT NULL,
  cid        bytea       NOT NULL,
  rev        bigint      NOT NULL,
  purpose    text        NOT NULL,
  record     bytea       NOT NULL,
  created_at timestamptz NOT NULL,
  indexed_at timestamptz NOT NULL DEFAULT now()
);
INSERT INTO lists_compact (author, rkey, cid, rev, purpose, record, created_at, indexed_at)
SELECT author, rkey, pg_temp.text_bytes(cid), rev_to_bigint(rev), purpose, pg_temp.text_bytes(record::text), created_at, indexed_at
FROM lists_keyed;

CREATE TABLE list_items_compact (
  author     integer     NOT NULL,
  rkey       bigint      NOT NULL,
  cid        bytea       NOT NULL,
  rev        bigint      NOT NULL,
  list_rkey  bigint      NOT NULL,
  subject    integer     NOT NULL,
  created_at timestamptz NOT NULL
);
INSERT INTO list_items_compact (author, rkey, cid, rev, list_rkey, subject, created_at)
SELECT author, rkey, pg_temp.text_bytes(cid), rev_to_bigint(rev), list_rkey, subject, created_at FROM list_items_keyed;

CREATE TABLE list_blocks_compact (
  author      integer     NOT NULL,
  rkey        bigint      NOT NULL,
  cid         bytea       NOT NULL,
  rev         bigint      NOT NULL,
  subj_author integer,
  subj_coll   smallint    NOT NULL,
  subj_rkey   bigint,
  created_at  timestamptz NOT NULL
);
CREATE TABLE list_optouts_compact (LIKE list_blocks_compact);
INSERT INTO list_blocks_compact (author, rkey, cid, rev, subj_author, subj_coll, subj_rkey, created_at)
SELECT k.author, k.rkey, pg_temp.text_bytes(k.cid), rev_to_bigint(k.rev), s.author, COALESCE(s.coll, 0), s.rkey, k.created_at
FROM list_blocks_keyed k CROSS JOIN LATERAL ref_of(k.list_uri) s;
INSERT INTO list_optouts_compact (author, rkey, cid, rev, subj_author, subj_coll, subj_rkey, created_at)
SELECT k.author, k.rkey, pg_temp.text_bytes(k.cid), rev_to_bigint(k.rev), s.author, COALESCE(s.coll, 0), s.rkey, k.created_at
FROM list_optouts_keyed k CROSS JOIN LATERAL ref_of(k.list_uri) s;

CREATE TABLE starter_packs_compact (
  author      integer     NOT NULL,
  rkey        bigint      NOT NULL,
  cid         bytea       NOT NULL,
  rev         bigint      NOT NULL,
  list_author integer,
  list_coll   smallint    NOT NULL,
  list_rkey   bigint,
  record      bytea       NOT NULL,
  search_tsv  tsvector,
  created_at  timestamptz NOT NULL,
  indexed_at  timestamptz NOT NULL DEFAULT now()
);
INSERT INTO starter_packs_compact (author, rkey, cid, rev, list_author, list_coll, list_rkey, record, search_tsv, created_at, indexed_at)
SELECT k.author, k.rkey, pg_temp.text_bytes(k.cid), rev_to_bigint(k.rev), s.author, COALESCE(s.coll, 0), s.rkey,
       pg_temp.text_bytes(k.record::text), k.search_tsv, k.created_at, k.indexed_at
FROM starter_packs_keyed k CROSS JOIN LATERAL ref_of(k.list_uri) s;

-- Counts of whole starter pack references only, as they are.
CREATE TABLE starter_pack_counts_compact (
  author                integer     NOT NULL,
  rkey                  bigint      NOT NULL,
  joined_week_count     integer     NOT NULL DEFAULT 0,
  joined_all_time_count integer     NOT NULL DEFAULT 0,
  all_time_source       text        NOT NULL,
  all_time_daily        jsonb       NOT NULL DEFAULT '[]'::jsonb,
  refreshed_at          timestamptz NOT NULL
);
INSERT INTO starter_pack_counts_compact (author, rkey, joined_week_count, joined_all_time_count, all_time_source, all_time_daily, refreshed_at)
SELECT r.author, r.rkey, c.joined_week_count, c.joined_all_time_count, c.all_time_source, c.all_time_daily, c.refreshed_at
FROM starter_pack_counts c CROSS JOIN LATERAL ref_of(c.pack_uri) r
WHERE r.coll = 11;

CREATE TABLE threadgates_compact (
  author      integer     NOT NULL,
  rkey        bigint      NOT NULL,
  cid         bytea       NOT NULL,
  rev         bigint      NOT NULL,
  post_author integer,
  post_coll   smallint    NOT NULL,
  post_rkey   bigint,
  record      bytea       NOT NULL,
  created_at  timestamptz NOT NULL
);
CREATE TABLE postgates_compact (LIKE threadgates_compact);
INSERT INTO threadgates_compact (author, rkey, cid, rev, post_author, post_coll, post_rkey, record, created_at)
SELECT k.author, k.rkey, pg_temp.text_bytes(k.cid), rev_to_bigint(k.rev), s.author, COALESCE(s.coll, 0), s.rkey,
       pg_temp.text_bytes(k.record::text), k.created_at
FROM threadgates_keyed k CROSS JOIN LATERAL ref_of(k.post_uri) s;
INSERT INTO postgates_compact (author, rkey, cid, rev, post_author, post_coll, post_rkey, record, created_at)
SELECT k.author, k.rkey, pg_temp.text_bytes(k.cid), rev_to_bigint(k.rev), s.author, COALESCE(s.coll, 0), s.rkey,
       pg_temp.text_bytes(k.record::text), k.created_at
FROM postgates_keyed k CROSS JOIN LATERAL ref_of(k.post_uri) s;

-- The odd texts (tbl: the owner's collection, 9 list blocks, 10 opt-outs,
-- 11 starter packs, 16 threadgates, 17 postgates; slot 4: subject, 6: list).
INSERT INTO odd_refs (tbl, author, rkey, slot, raw)
SELECT 9, k.author, k.rkey, 4, k.list_uri
FROM list_blocks_keyed k JOIN list_blocks_compact c ON c.author = k.author AND c.rkey = k.rkey
WHERE c.subj_coll = 0
UNION ALL
SELECT 10, k.author, k.rkey, 4, k.list_uri
FROM list_optouts_keyed k JOIN list_optouts_compact c ON c.author = k.author AND c.rkey = k.rkey
WHERE c.subj_coll = 0
UNION ALL
SELECT 11, k.author, k.rkey, 6, k.list_uri
FROM starter_packs_keyed k JOIN starter_packs_compact c ON c.author = k.author AND c.rkey = k.rkey
WHERE c.list_coll = 0
UNION ALL
SELECT 16, k.author, k.rkey, 4, k.post_uri
FROM threadgates_keyed k JOIN threadgates_compact c ON c.author = k.author AND c.rkey = k.rkey
WHERE c.post_coll = 0
UNION ALL
SELECT 17, k.author, k.rkey, 4, k.post_uri
FROM postgates_keyed k JOIN postgates_compact c ON c.author = k.author AND c.rkey = k.rkey
WHERE c.post_coll = 0;

CREATE TABLE nearhorizon_poll_posts_compact (
  author     integer     NOT NULL,
  rkey       bigint      NOT NULL,
  cid        bytea       NOT NULL,
  rev        bigint      NOT NULL,
  visibility text        NOT NULL,
  record     bytea       NOT NULL,
  created_at timestamptz NOT NULL,
  indexed_at timestamptz NOT NULL DEFAULT now()
);
INSERT INTO nearhorizon_poll_posts_compact (author, rkey, cid, rev, visibility, record, created_at, indexed_at)
SELECT author, rkey, pg_temp.text_bytes(cid), rev_to_bigint(rev), visibility, pg_temp.text_bytes(record::text), created_at, indexed_at
FROM polls_keyed;

CREATE TABLE labeler_services_compact (
  author     integer     NOT NULL,
  cid        bytea       NOT NULL,
  rev        bigint      NOT NULL,
  record     bytea       NOT NULL,
  created_at timestamptz NOT NULL,
  indexed_at timestamptz NOT NULL DEFAULT now()
);
INSERT INTO labeler_services_compact (author, cid, rev, record, created_at, indexed_at)
SELECT author, pg_temp.text_bytes(cid), rev_to_bigint(rev), pg_temp.text_bytes(record::text), created_at, indexed_at
FROM labelers_keyed;

CREATE TABLE actor_self_records_compact (
  author              integer     NOT NULL,
  coll                smallint    NOT NULL,
  cid                 bytea       NOT NULL,
  rev                 bigint      NOT NULL,
  record              bytea       NOT NULL,
  hide_from_recs      boolean     NOT NULL DEFAULT false,
  allow_subscriptions text,
  indexed_at          timestamptz NOT NULL DEFAULT now()
);
INSERT INTO actor_self_records_compact (author, coll, cid, rev, record, hide_from_recs, allow_subscriptions, indexed_at)
SELECT author, coll, pg_temp.text_bytes(cid), rev_to_bigint(rev), pg_temp.text_bytes(record::text),
       COALESCE(record -> 'hideFromAlgorithmicRecommendations' = 'true'::jsonb, false), record ->> 'allowSubscriptions', indexed_at
FROM self_records_keyed;

CREATE TABLE member_post_refs_compact (
  author     integer     NOT NULL,
  rkey       bigint      NOT NULL,
  rev        bigint      NOT NULL,
  parent     integer,
  root       integer,
  quote      integer,
  created_at timestamptz NOT NULL
);
INSERT INTO member_post_refs_compact (author, rkey, rev, parent, root, quote, created_at)
SELECT author, rkey, rev_to_bigint(rev), parent, root, quote, created_at FROM post_refs_keyed;

DO $$
DECLARE
  t record;
  old_n bigint; new_n bigint; bad_rev bigint;
BEGIN
  FOR t IN SELECT * FROM (VALUES
      ('lists', 'lists_keyed'), ('list_items', 'list_items_keyed'), ('list_blocks', 'list_blocks_keyed'),
      ('list_optouts', 'list_optouts_keyed'), ('starter_packs', 'starter_packs_keyed'), ('threadgates', 'threadgates_keyed'),
      ('postgates', 'postgates_keyed'), ('nearhorizon_poll_posts', 'polls_keyed'), ('labeler_services', 'labelers_keyed'),
      ('actor_self_records', 'self_records_keyed'), ('member_post_refs', 'post_refs_keyed')) v(tbl, keyed)
  LOOP
    EXECUTE format('SELECT count(*) FROM %I', t.tbl) INTO old_n;
    EXECUTE format('SELECT count(*), count(*) FILTER (WHERE NOT tid_is_valid(rev)) FROM %I', t.keyed) INTO new_n, bad_rev;
    RAISE NOTICE '%: % rows, % copied, % left out, % revs not TIDs (stored as the lowest)', t.tbl, old_n, new_n, old_n - new_n, bad_rev;
  END LOOP;
  SELECT count(*) INTO old_n FROM starter_pack_counts;
  SELECT count(*) INTO new_n FROM starter_pack_counts_compact;
  RAISE NOTICE 'starter_pack_counts: % rows, % copied, % dropped (not whole starter pack references)', old_n, new_n, old_n - new_n;
  SELECT count(*) INTO new_n FROM odd_refs WHERE tbl IN (9, 10, 11, 16, 17);
  RAISE NOTICE 'odd references of list blocks, opt-outs, starter packs and gates: %', new_n;
END $$;

-- The swap. The old tables take their indexes with them.
DROP TABLE lists;
DROP TABLE list_items;
DROP TABLE list_blocks;
DROP TABLE list_optouts;
DROP TABLE starter_packs;
DROP TABLE starter_pack_counts;
DROP TABLE threadgates;
DROP TABLE postgates;
DROP TABLE nearhorizon_poll_posts;
DROP TABLE labeler_services;
DROP TABLE actor_self_records;
DROP TABLE member_post_refs;
ALTER TABLE lists_compact RENAME TO lists;
ALTER TABLE list_items_compact RENAME TO list_items;
ALTER TABLE list_blocks_compact RENAME TO list_blocks;
ALTER TABLE list_optouts_compact RENAME TO list_optouts;
ALTER TABLE starter_packs_compact RENAME TO starter_packs;
ALTER TABLE starter_pack_counts_compact RENAME TO starter_pack_counts;
ALTER TABLE threadgates_compact RENAME TO threadgates;
ALTER TABLE postgates_compact RENAME TO postgates;
ALTER TABLE nearhorizon_poll_posts_compact RENAME TO nearhorizon_poll_posts;
ALTER TABLE labeler_services_compact RENAME TO labeler_services;
ALTER TABLE actor_self_records_compact RENAME TO actor_self_records;
ALTER TABLE member_post_refs_compact RENAME TO member_post_refs;

-- A record over 2 KB is compressed already; pglz would not gain.
ALTER TABLE lists ALTER COLUMN record SET STORAGE EXTERNAL;
ALTER TABLE starter_packs ALTER COLUMN record SET STORAGE EXTERNAL;
ALTER TABLE threadgates ALTER COLUMN record SET STORAGE EXTERNAL;
ALTER TABLE postgates ALTER COLUMN record SET STORAGE EXTERNAL;
ALTER TABLE nearhorizon_poll_posts ALTER COLUMN record SET STORAGE EXTERNAL;
ALTER TABLE labeler_services ALTER COLUMN record SET STORAGE EXTERNAL;
ALTER TABLE actor_self_records ALTER COLUMN record SET STORAGE EXTERNAL;

ALTER TABLE lists ADD CONSTRAINT lists_pkey PRIMARY KEY (author, rkey);
CREATE INDEX lists_author ON lists (author, created_at DESC);

ALTER TABLE list_items ADD CONSTRAINT list_items_pkey PRIMARY KEY (author, rkey);
CREATE INDEX list_items_list    ON list_items (author, list_rkey, created_at DESC);
CREATE INDEX list_items_subject ON list_items (subject);

ALTER TABLE list_blocks ADD CONSTRAINT list_blocks_pkey PRIMARY KEY (author, rkey);
CREATE INDEX list_blocks_author  ON list_blocks (author, created_at DESC);
CREATE INDEX list_blocks_subject ON list_blocks (subj_author, subj_rkey) WHERE subj_author IS NOT NULL;

ALTER TABLE list_optouts ADD CONSTRAINT list_optouts_pkey PRIMARY KEY (author, rkey);
CREATE INDEX list_optouts_subject ON list_optouts (subj_author, subj_rkey, author) WHERE subj_author IS NOT NULL;

ALTER TABLE starter_packs ADD CONSTRAINT starter_packs_pkey PRIMARY KEY (author, rkey);
CREATE INDEX starter_packs_author ON starter_packs (author, created_at DESC);
CREATE INDEX starter_packs_list   ON starter_packs (list_author, list_rkey) WHERE list_author IS NOT NULL;
CREATE INDEX starter_packs_search ON starter_packs USING gin (search_tsv);

ALTER TABLE starter_pack_counts ADD CONSTRAINT starter_pack_counts_pkey PRIMARY KEY (author, rkey);

ALTER TABLE threadgates ADD CONSTRAINT threadgates_pkey PRIMARY KEY (author, rkey);
CREATE INDEX threadgates_post ON threadgates (post_author, post_rkey) WHERE post_author IS NOT NULL;

ALTER TABLE postgates ADD CONSTRAINT postgates_pkey PRIMARY KEY (author, rkey);
CREATE INDEX postgates_post ON postgates (post_author, post_rkey) WHERE post_author IS NOT NULL;

ALTER TABLE nearhorizon_poll_posts ADD CONSTRAINT nearhorizon_poll_posts_pkey PRIMARY KEY (author, rkey);
CREATE INDEX nearhorizon_poll_posts_author ON nearhorizon_poll_posts (author, created_at DESC);

ALTER TABLE labeler_services ADD CONSTRAINT labeler_services_pkey PRIMARY KEY (author);

ALTER TABLE actor_self_records ADD CONSTRAINT actor_self_records_pkey PRIMARY KEY (author, coll);

ALTER TABLE member_post_refs ADD CONSTRAINT member_post_refs_pkey PRIMARY KEY (author, rkey);
CREATE INDEX member_post_refs_author_created ON member_post_refs (author, created_at);

-- An owner row's odd references go with it.
CREATE TRIGGER list_blocks_odd_refs
  AFTER DELETE ON list_blocks
  FOR EACH ROW WHEN (OLD.subj_coll = 0) EXECUTE FUNCTION odd_refs_owner_gone('9');
CREATE TRIGGER list_optouts_odd_refs
  AFTER DELETE ON list_optouts
  FOR EACH ROW WHEN (OLD.subj_coll = 0) EXECUTE FUNCTION odd_refs_owner_gone('10');
CREATE TRIGGER starter_packs_odd_refs
  AFTER DELETE ON starter_packs
  FOR EACH ROW WHEN (OLD.list_coll = 0) EXECUTE FUNCTION odd_refs_owner_gone('11');
CREATE TRIGGER threadgates_odd_refs
  AFTER DELETE ON threadgates
  FOR EACH ROW WHEN (OLD.post_coll = 0) EXECUTE FUNCTION odd_refs_owner_gone('16');
CREATE TRIGGER postgates_odd_refs
  AFTER DELETE ON postgates
  FOR EACH ROW WHEN (OLD.post_coll = 0) EXECUTE FUNCTION odd_refs_owner_gone('17');
