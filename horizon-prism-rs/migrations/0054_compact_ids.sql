-- Compact storage: the shared ids and codecs the compact tables use.
-- Nothing reads them yet; later migrations move tables onto them.

-- Accounts are interned. `dids` is the thread outline's actor table,
-- renamed with its ids kept, so outline rows keep naming the same authors.
ALTER TABLE outline_actors RENAME TO dids;
ALTER INDEX outline_actors_pkey RENAME TO dids_pkey;
ALTER INDEX outline_actors_did_key RENAME TO dids_did_key;
ALTER SEQUENCE outline_actors_id_seq RENAME TO dids_id_seq;

-- A fixed list of collections: every NSID the classifier routes plus
-- the reference targets reads use. Data never grows it; a reference to any
-- other collection is odd (`odd_refs`). 0 is reserved for "odd".
CREATE TABLE collections (
  id   smallint PRIMARY KEY CHECK (id > 0),
  nsid text NOT NULL UNIQUE
);
INSERT INTO collections (id, nsid) VALUES
  (1,  'app.bsky.feed.post'),
  (2,  'app.bsky.feed.like'),
  (3,  'app.bsky.feed.repost'),
  (4,  'app.bsky.graph.follow'),
  (5,  'app.bsky.graph.block'),
  (6,  'app.bsky.actor.profile'),
  (7,  'app.bsky.graph.list'),
  (8,  'app.bsky.graph.listitem'),
  (9,  'app.bsky.graph.listblock'),
  (10, 'app.bsky.graph.referencelistoptout'),
  (11, 'app.bsky.graph.starterpack'),
  (12, 'app.bsky.labeler.service'),
  (13, 'app.bsky.notification.declaration'),
  (14, 'app.bsky.actor.status'),
  (15, 'app.bsky.actor.contentVisibilityDeclaration'),
  (16, 'app.bsky.feed.threadgate'),
  (17, 'app.bsky.feed.postgate'),
  (18, 'app.bsky.feed.generator'),
  (19, 'app.bsky.graph.verification'),
  (20, 'app.nearhorizon.poll.post'),
  (21, 'app.nearhorizon.poll.vote'),
  (22, 'app.nearhorizon.actor.pronouns'),
  (23, 'app.nearhorizon.plural.system'),
  (24, 'app.nearhorizon.plural.front'),
  (25, 'app.nearhorizon.plural.member'),
  (26, 'app.nearhorizon.plural.fronters'),
  (27, 'tools.kryphocron.feed.postPrivate'),
  (28, 'tools.kryphocron.feed.postPublic'),
  (29, 'tools.kryphocron.feed.like'),
  (30, 'tools.kryphocron.feed.repost'),
  (31, 'tools.kryphocron.feed.threadgate'),
  (32, 'tools.kryphocron.graph.mute'),
  (33, 'tools.kryphocron.graph.block'),
  (34, 'tools.kryphocron.policy.audience');

-- Record keys that are not TIDs, keyed without the collection so a
-- post and its gates share one id. Ids are negative; TID keys are >= 0.
CREATE SEQUENCE rkey_alias_seq INCREMENT BY -1 MINVALUE -9223372036854775808 MAXVALUE -1 START WITH -1;
CREATE TABLE rkey_aliases (
  id     bigint PRIMARY KEY DEFAULT nextval('rkey_alias_seq'),
  author integer NOT NULL,
  rkey   text NOT NULL,
  UNIQUE (author, rkey)
);
ALTER SEQUENCE rkey_alias_seq OWNED BY rkey_aliases.id;

-- References kept as raw text, one row per odd slot of an owner row.
-- tbl: the owner table's id; slot: which reference.
CREATE TABLE odd_refs (
  tbl    smallint NOT NULL,
  author integer  NOT NULL,
  rkey   bigint   NOT NULL,
  slot   smallint NOT NULL,
  raw    text     NOT NULL,
  PRIMARY KEY (tbl, author, rkey, slot)
);
CREATE INDEX odd_refs_raw ON odd_refs USING hash (raw);

-- Record codec dictionaries. Id 0 means "uncompressed JSON" and has no row.
CREATE TABLE record_dicts (
  id         smallint PRIMARY KEY CHECK (id > 0),
  family     text NOT NULL,
  bytes      bytea NOT NULL,
  trained_at timestamptz NOT NULL DEFAULT now()
);

-- SQL helpers. They must agree with `db/ids.rs` exactly.

-- `Did::is_valid`: one ASCII pattern and a 2048-byte cap.
CREATE FUNCTION did_is_valid(s text) RETURNS boolean
  LANGUAGE sql IMMUTABLE STRICT PARALLEL SAFE
  RETURN octet_length(s) <= 2048 AND s ~ '^did:[a-z]+:[a-zA-Z0-9._:%-]*[a-zA-Z0-9._-]$';

-- `RecordKey::is_valid`: 1 to 512 of these characters, not "." or "..".
-- (Postgres caps a regex repetition count at 255, so the length is separate.)
CREATE FUNCTION rkey_is_valid(s text) RETURNS boolean
  LANGUAGE sql IMMUTABLE STRICT PARALLEL SAFE
  RETURN octet_length(s) BETWEEN 1 AND 512 AND s ~ '^[a-zA-Z0-9_~.:-]+$' AND s NOT IN ('.', '..');

-- `Tid::is_valid`.
CREATE FUNCTION tid_is_valid(s text) RETURNS boolean
  LANGUAGE sql IMMUTABLE STRICT PARALLEL SAFE
  RETURN s ~ '^[234567abcdefghij][234567abcdefghijklmnopqrstuvwxyz]{12}$';

-- A valid TID's 64-bit unsigned value, as numeric (no overflow).
CREATE FUNCTION tid_numeric(s text) RETURNS numeric
  LANGUAGE plpgsql IMMUTABLE STRICT PARALLEL SAFE AS $$
DECLARE
  v numeric := 0;
  i int;
BEGIN
  IF NOT tid_is_valid(s) THEN RETURN NULL; END IF;
  FOR i IN 1..13 LOOP
    v := v * 32 + (strpos('234567abcdefghijklmnopqrstuvwxyz', substr(s, i, 1)) - 1);
  END LOOP;
  RETURN v;
END $$;

-- Rkey map (`tid_value`): the value when the top bit is clear, else NULL
-- (the caller then uses an alias).
CREATE FUNCTION rkey_to_bigint(s text) RETURNS bigint
  LANGUAGE sql IMMUTABLE STRICT PARALLEL SAFE
  RETURN CASE WHEN tid_numeric(s) < 9223372036854775808 THEN tid_numeric(s)::bigint END;

-- Rev map: every valid TID, order kept, value - 2^63; anything else maps to
-- the sentinel's value (the lowest).
CREATE FUNCTION rev_to_bigint(s text) RETURNS bigint
  LANGUAGE sql IMMUTABLE STRICT PARALLEL SAFE
  RETURN COALESCE(tid_numeric(s) - 9223372036854775808, -9223372036854775808)::bigint;

CREATE FUNCTION bigint_to_tid_numeric(v numeric) RETURNS text
  LANGUAGE plpgsql IMMUTABLE STRICT PARALLEL SAFE AS $$
DECLARE
  out text := '';
  i int;
BEGIN
  FOR i IN 1..13 LOOP
    out := substr('234567abcdefghijklmnopqrstuvwxyz', (mod(v, 32))::int + 1, 1) || out;
    v := div(v, 32);
  END LOOP;
  RETURN out;
END $$;

CREATE FUNCTION bigint_to_rkey(v bigint) RETURNS text
  LANGUAGE sql STABLE STRICT PARALLEL SAFE
  RETURN CASE WHEN v >= 0 THEN bigint_to_tid_numeric(v::numeric)
              ELSE (SELECT a.rkey FROM rkey_aliases a WHERE a.id = v) END;

CREATE FUNCTION bigint_to_rev(v bigint) RETURNS text
  LANGUAGE sql IMMUTABLE STRICT PARALLEL SAFE
  RETURN bigint_to_tid_numeric(v::numeric + 9223372036854775808);

-- The URI of a whole reference; NULL when any part is missing.
CREATE FUNCTION at_uri(author integer, coll smallint, rkey bigint) RETURNS text
  LANGUAGE sql STABLE PARALLEL SAFE
  RETURN (SELECT 'at://' || d.did || '/' || c.nsid || '/' || bigint_to_rkey(rkey)
          FROM dids d, collections c WHERE d.id = author AND c.id = coll);

-- A URI as a whole reference, read-only: NULL parts when it is not one that
-- `at_uri` reproduces exactly (an odd reference), or names an unknown DID or
-- an rkey with no alias yet. `author` is set whenever the authority is a known
-- valid DID, as odd references keep it.
CREATE FUNCTION ref_of(uri text, OUT author integer, OUT coll smallint, OUT rkey bigint)
  LANGUAGE plpgsql STABLE PARALLEL SAFE AS $$
DECLARE
  parts text[];
BEGIN
  IF uri IS NULL OR left(uri, 5) <> 'at://' THEN RETURN; END IF;
  parts := string_to_array(substr(uri, 6), '/');
  IF did_is_valid(parts[1]) THEN
    SELECT d.id INTO author FROM dids d WHERE d.did = parts[1];
  END IF;
  IF author IS NULL OR array_length(parts, 1) <> 3 THEN RETURN; END IF;
  SELECT c.id INTO coll FROM collections c WHERE c.nsid = parts[2];
  IF coll IS NULL THEN RETURN; END IF;
  rkey := rkey_to_bigint(parts[3]);
  IF rkey IS NULL THEN
    SELECT a.id INTO rkey FROM rkey_aliases a WHERE a.author = ref_of.author AND a.rkey = parts[3];
  END IF;
  IF rkey IS NULL OR at_uri(author, coll, rkey) IS DISTINCT FROM uri THEN
    coll := NULL;
    rkey := NULL;
  END IF;
END $$;
