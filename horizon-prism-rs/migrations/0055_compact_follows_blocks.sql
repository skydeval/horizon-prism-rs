-- Compact storage: `follows` and `blocks` keyed by interned ids.
--
-- A row is `(author, rkey)`: the author's `dids` id and the record key as
-- its TID value, or a negative `rkey_aliases` id for any other key. The
-- subject is a `dids` id, the rev an integer in rev order, and a follow's
-- `via` a reference `(via_author, via_coll, via_rkey)` with its CID as bytes.
-- A `via` that `at_uri` would not give back exactly is odd: `via_coll = 0`,
-- its text in `odd_refs`, and `via_author` kept when its authority is a
-- valid DID. `indexed_at` and the text columns are gone.
--
-- One transaction: intern, copy, then indexes and triggers, then the swap.
-- A row whose author or subject is not a valid DID, whose record key is not
-- valid, or whose URI is not its author's own record cannot be keyed; it is
-- left out and counted in the log. Ingest checks all three, so none are
-- expected.

-- Every valid DID the two tables name: authors, subjects, and the authority
-- of a follow's `via`.
INSERT INTO dids (did)
SELECT x.d FROM (
  SELECT author_did AS d FROM follows
  UNION SELECT subject_did FROM follows
  UNION SELECT author_did FROM blocks
  UNION SELECT subject_did FROM blocks
  UNION SELECT split_part(substr(via_uri, 6), '/', 1) FROM follows WHERE left(via_uri, 5) = 'at://'
) x
WHERE did_is_valid(x.d) AND NOT EXISTS (SELECT 1 FROM dids a WHERE a.did = x.d)
ORDER BY x.d
ON CONFLICT (did) DO NOTHING;

-- Aliases for every valid record key that is not a TID with its top bit
-- clear: the rows' own keys, and the keys of `via` references that could be
-- whole (three parts, a listed collection).
INSERT INTO rkey_aliases (author, rkey)
SELECT DISTINCT d.id, k.rkey FROM (
  SELECT author_did AS did, split_part(uri, '/', 5) AS rkey FROM follows
  UNION SELECT author_did, split_part(uri, '/', 5) FROM blocks
  UNION SELECT split_part(substr(via_uri, 6), '/', 1), split_part(substr(via_uri, 6), '/', 3) FROM follows
   WHERE left(via_uri, 5) = 'at://'
     AND cardinality(string_to_array(substr(via_uri, 6), '/')) = 3
     AND split_part(substr(via_uri, 6), '/', 2) IN (SELECT nsid FROM collections)
) k
JOIN dids d ON d.did = k.did
WHERE rkey_is_valid(k.rkey) AND rkey_to_bigint(k.rkey) IS NULL
  AND NOT EXISTS (SELECT 1 FROM rkey_aliases x WHERE x.author = d.id AND x.rkey = k.rkey)
ORDER BY 1, 2
ON CONFLICT (author, rkey) DO NOTHING;

-- The old rows with their new keys.
CREATE TEMP TABLE follows_keyed ON COMMIT DROP AS
SELECT a.id AS author, k.key AS rkey, s.id AS subject, rev_to_bigint(f.rev) AS rev, f.created_at,
       f.via_uri, f.via_cid, tid_is_valid(f.rev) AS rev_valid
FROM follows f
JOIN dids a ON a.did = f.author_did
JOIN dids s ON s.did = f.subject_did
CROSS JOIN LATERAL (SELECT split_part(f.uri, '/', 5) AS text) r
CROSS JOIN LATERAL (
  SELECT COALESCE(rkey_to_bigint(r.text), (SELECT x.id FROM rkey_aliases x WHERE x.author = a.id AND x.rkey = r.text)) AS key
) k
WHERE rkey_is_valid(r.text) AND k.key IS NOT NULL
  AND f.uri = 'at://' || f.author_did || '/app.bsky.graph.follow/' || r.text;

CREATE TEMP TABLE blocks_keyed ON COMMIT DROP AS
SELECT a.id AS author, k.key AS rkey, s.id AS subject, rev_to_bigint(b.rev) AS rev, b.created_at,
       tid_is_valid(b.rev) AS rev_valid
FROM blocks b
JOIN dids a ON a.did = b.author_did
JOIN dids s ON s.did = b.subject_did
CROSS JOIN LATERAL (SELECT split_part(b.uri, '/', 5) AS text) r
CROSS JOIN LATERAL (
  SELECT COALESCE(rkey_to_bigint(r.text), (SELECT x.id FROM rkey_aliases x WHERE x.author = a.id AND x.rkey = r.text)) AS key
) k
WHERE rkey_is_valid(r.text) AND k.key IS NOT NULL
  AND b.uri = 'at://' || b.author_did || '/app.bsky.graph.block/' || r.text;

DO $$
DECLARE
  f_old bigint; f_new bigint; f_rev bigint;
  b_old bigint; b_new bigint; b_rev bigint;
BEGIN
  SELECT count(*) INTO f_old FROM follows;
  SELECT count(*), count(*) FILTER (WHERE NOT rev_valid) INTO f_new, f_rev FROM follows_keyed;
  SELECT count(*) INTO b_old FROM blocks;
  SELECT count(*), count(*) FILTER (WHERE NOT rev_valid) INTO b_new, b_rev FROM blocks_keyed;
  RAISE NOTICE 'follows: % rows, % copied, % left out, % revs not TIDs (stored as the lowest)', f_old, f_new, f_old - f_new, f_rev;
  RAISE NOTICE 'blocks: % rows, % copied, % left out, % revs not TIDs (stored as the lowest)', b_old, b_new, b_old - b_new, b_rev;
END $$;

-- The new tables, without triggers, filled from the keyed rows. Every CID
-- is kept in its exact text form (0x00 then the text).
CREATE TABLE follows_compact (
  author     integer     NOT NULL,
  rkey       bigint      NOT NULL,
  subject    integer     NOT NULL,
  rev        bigint      NOT NULL,
  created_at timestamptz NOT NULL,
  via_author integer,
  via_coll   smallint,
  via_rkey   bigint,
  via_cid    bytea
);

INSERT INTO follows_compact (author, rkey, subject, rev, created_at, via_author, via_coll, via_rkey, via_cid)
SELECT k.author, k.rkey, k.subject, k.rev, k.created_at,
       v.author,
       CASE WHEN k.via_uri IS NULL THEN NULL WHEN v.coll IS NOT NULL THEN v.coll ELSE 0 END,
       v.rkey,
       CASE WHEN k.via_uri IS NOT NULL AND k.via_cid IS NOT NULL THEN '\x00'::bytea || convert_to(k.via_cid, 'UTF8') END
FROM follows_keyed k
LEFT JOIN LATERAL ref_of(k.via_uri) v ON true;

-- The odd `via` texts (tbl 4: follows; slot 5: via).
INSERT INTO odd_refs (tbl, author, rkey, slot, raw)
SELECT 4, k.author, k.rkey, 5, k.via_uri
FROM follows_keyed k
JOIN follows_compact c ON c.author = k.author AND c.rkey = k.rkey
WHERE c.via_coll = 0;

CREATE TABLE blocks_compact (
  author     integer     NOT NULL,
  rkey       bigint      NOT NULL,
  subject    integer     NOT NULL,
  rev        bigint      NOT NULL,
  created_at timestamptz NOT NULL
);

INSERT INTO blocks_compact (author, rkey, subject, rev, created_at)
SELECT author, rkey, subject, rev, created_at FROM blocks_keyed;

-- The swap. The old tables take their indexes and triggers with them.
DROP TABLE follows;
DROP TABLE blocks;
ALTER TABLE follows_compact RENAME TO follows;
ALTER TABLE blocks_compact RENAME TO blocks;

ALTER TABLE follows ADD CONSTRAINT follows_pkey PRIMARY KEY (author, rkey);
CREATE INDEX follows_author  ON follows (author, created_at DESC);
CREATE INDEX follows_subject ON follows (subject, created_at DESC);
CREATE INDEX follows_pair    ON follows (author, subject);

ALTER TABLE blocks ADD CONSTRAINT blocks_pkey PRIMARY KEY (author, rkey);
CREATE INDEX blocks_author  ON blocks (author, created_at DESC);
CREATE INDEX blocks_subject ON blocks (subject, created_at DESC);
CREATE INDEX blocks_pair    ON blocks (author, subject);

-- Follow counts, on the new keys; `actor_counts` stays keyed by DID text.
CREATE OR REPLACE FUNCTION count_follows() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  IF TG_OP IN ('UPDATE', 'DELETE') THEN
    UPDATE actor_counts SET follows_count = GREATEST(follows_count - 1, 0)
      WHERE did = (SELECT d.did FROM dids d WHERE d.id = OLD.author);
    UPDATE actor_counts SET followers_count = GREATEST(followers_count - 1, 0)
      WHERE did = (SELECT d.did FROM dids d WHERE d.id = OLD.subject);
  END IF;
  IF TG_OP IN ('INSERT', 'UPDATE') THEN
    INSERT INTO actor_counts (did, follows_count) SELECT d.did, 1 FROM dids d WHERE d.id = NEW.author
      ON CONFLICT (did) DO UPDATE SET follows_count = actor_counts.follows_count + 1;
    INSERT INTO actor_counts (did, followers_count) SELECT d.did, 1 FROM dids d WHERE d.id = NEW.subject
      ON CONFLICT (did) DO UPDATE SET followers_count = actor_counts.followers_count + 1;
  END IF;
  RETURN NULL;
END;
$$;
CREATE TRIGGER follows_count
  AFTER INSERT OR DELETE OR UPDATE OF author, subject ON follows
  FOR EACH ROW EXECUTE FUNCTION count_follows();

-- An owner row's odd references go with it. The argument is the owner
-- table's `odd_refs.tbl`; the trigger's WHEN keeps it to rows that have one.
CREATE FUNCTION odd_refs_owner_gone() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  DELETE FROM odd_refs WHERE tbl = TG_ARGV[0]::smallint AND author = OLD.author AND rkey = OLD.rkey;
  RETURN NULL;
END;
$$;
CREATE TRIGGER follows_odd_refs
  AFTER DELETE ON follows
  FOR EACH ROW WHEN (OLD.via_coll = 0) EXECUTE FUNCTION odd_refs_owner_gone('4');
