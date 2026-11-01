-- Compact storage: `actors` keyed by the account's interned `dids` id.
--
-- The row keeps every column it had, `did` aside: handle, endpoint and
-- status, the profile's fields that queries search, sort or show, and the
-- profile record itself, whole, as `dict id ‖ payload` (dict 0, the JSON
-- text, here; a background job compresses it later). CIDs are bytes, the
-- profile rev an integer in rev order. The profile's self-label values gain
-- a column, `self_labels`, which the reads that hide an account from
-- signed-out viewers test instead of the record.
--
-- One transaction: intern, copy, then the swap and indexes. A row whose DID
-- is not valid cannot be keyed; it is left out and counted in the log.
-- Ingest checks DIDs, so none are expected.

INSERT INTO dids (did)
SELECT a.did FROM actors a
WHERE did_is_valid(a.did) AND NOT EXISTS (SELECT 1 FROM dids d WHERE d.did = a.did)
ORDER BY a.did
ON CONFLICT (did) DO NOTHING;

-- A CID or record in its exact text form, dict 0. (Replaced, not created: an
-- earlier migration in the same session may have made it.)
CREATE OR REPLACE FUNCTION pg_temp.text_bytes(s text) RETURNS bytea LANGUAGE sql IMMUTABLE AS $$
  SELECT '\x00'::bytea || convert_to(s, 'UTF8')
$$;

-- A profile record's self-label values, as the writer reads them: the
-- string `val` of each entry of `labels.values`; NULL when there are none.
CREATE OR REPLACE FUNCTION pg_temp.profile_self_labels(record jsonb) RETURNS text[] LANGUAGE sql IMMUTABLE AS $$
  SELECT NULLIF(ARRAY(
    SELECT v ->> 'val' FROM jsonb_array_elements(
      CASE WHEN jsonb_typeof(record -> 'labels' -> 'values') = 'array' THEN record -> 'labels' -> 'values' ELSE '[]'::jsonb END
    ) v
    WHERE jsonb_typeof(v -> 'val') = 'string'
  ), '{}')
$$;

CREATE TABLE actors_compact (
  id                          integer     NOT NULL,
  handle                      text,
  handle_checked_at           timestamptz,
  pds_endpoint                text,
  account_status              text        NOT NULL DEFAULT 'active',
  deletion_suspect_at         timestamptz,
  deletion_suspect_pds        text,
  status_checked_at           timestamptz,
  display_name                text,
  description                 text,
  avatar_cid                  bytea,
  banner_cid                  bytea,
  profile_rev                 bigint,
  profile_record              bytea,
  indexed_at                  timestamptz NOT NULL DEFAULT now(),
  profile_cid                 bytea,
  pronouns                    text,
  website                     text,
  joined_via_starter_pack_uri text,
  joined_via_starter_pack_cid bytea,
  profile_created_at          timestamptz,
  self_labels                 text[]
);

INSERT INTO actors_compact (id, handle, handle_checked_at, pds_endpoint, account_status, deletion_suspect_at,
  deletion_suspect_pds, status_checked_at, display_name, description, avatar_cid, banner_cid, profile_rev,
  profile_record, indexed_at, profile_cid, pronouns, website, joined_via_starter_pack_uri,
  joined_via_starter_pack_cid, profile_created_at, self_labels)
SELECT d.id, a.handle, a.handle_checked_at, a.pds_endpoint, a.account_status, a.deletion_suspect_at,
  a.deletion_suspect_pds, a.status_checked_at, a.display_name, a.description, pg_temp.text_bytes(a.avatar_cid),
  pg_temp.text_bytes(a.banner_cid), rev_to_bigint(a.profile_rev), pg_temp.text_bytes(a.profile_record::text),
  a.indexed_at, pg_temp.text_bytes(a.profile_cid), a.pronouns, a.website, a.joined_via_starter_pack_uri,
  pg_temp.text_bytes(a.joined_via_starter_pack_cid), a.profile_created_at, pg_temp.profile_self_labels(a.profile_record)
FROM actors a JOIN dids d ON d.did = a.did;

DO $$
DECLARE
  old_n bigint; new_n bigint; bad_rev bigint; labelled bigint;
BEGIN
  SELECT count(*), count(*) FILTER (WHERE profile_rev IS NOT NULL AND NOT tid_is_valid(profile_rev)) INTO old_n, bad_rev FROM actors;
  SELECT count(*), count(*) FILTER (WHERE self_labels IS NOT NULL) INTO new_n, labelled FROM actors_compact;
  RAISE NOTICE 'actors: % rows, % copied, % left out, % revs not TIDs (stored as the lowest), % with self-labels',
    old_n, new_n, old_n - new_n, bad_rev, labelled;
END $$;

-- The swap. The old table takes its indexes with it.
DROP TABLE actors;
ALTER TABLE actors_compact RENAME TO actors;

-- A record over 2 KB is compressed already; pglz would not gain.
ALTER TABLE actors ALTER COLUMN profile_record SET STORAGE EXTERNAL;

ALTER TABLE actors ADD CONSTRAINT actors_pkey PRIMARY KEY (id);
CREATE INDEX actors_handle_prefix ON actors (handle text_pattern_ops);
CREATE INDEX actors_handle_trgm ON actors USING gin (handle gin_trgm_ops);
CREATE INDEX actors_name_prefix ON actors (lower(display_name) text_pattern_ops);
CREATE INDEX actors_name_trgm ON actors USING gin (display_name gin_trgm_ops);
CREATE INDEX actors_joined_via ON actors (joined_via_starter_pack_uri) WHERE joined_via_starter_pack_uri IS NOT NULL;
