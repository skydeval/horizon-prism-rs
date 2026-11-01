-- Faster key text. Every stored rkey and rev read back as text went through
-- a numeric loop in PL/pgSQL, and `at_uri` ran two sub-selects per call.
-- These read the 13 TID characters straight from the bits, in SQL the
-- planner inlines, and look up the DID and collection in PL/pgSQL with
-- cached plans. The text is the same for every value.

-- A TID's text from its 64-bit value: `head` is the first character's 5
-- bits (bits 64 to 60), `v` holds the other 60 in its low bits.
CREATE FUNCTION tid_text(head integer, v bigint) RETURNS text
  LANGUAGE sql IMMUTABLE STRICT PARALLEL SAFE
  RETURN substr('234567abcdefghijklmnopqrstuvwxyz', head + 1, 1)
      || substr('234567abcdefghijklmnopqrstuvwxyz', ((v >> 55) & 31)::int + 1, 1)
      || substr('234567abcdefghijklmnopqrstuvwxyz', ((v >> 50) & 31)::int + 1, 1)
      || substr('234567abcdefghijklmnopqrstuvwxyz', ((v >> 45) & 31)::int + 1, 1)
      || substr('234567abcdefghijklmnopqrstuvwxyz', ((v >> 40) & 31)::int + 1, 1)
      || substr('234567abcdefghijklmnopqrstuvwxyz', ((v >> 35) & 31)::int + 1, 1)
      || substr('234567abcdefghijklmnopqrstuvwxyz', ((v >> 30) & 31)::int + 1, 1)
      || substr('234567abcdefghijklmnopqrstuvwxyz', ((v >> 25) & 31)::int + 1, 1)
      || substr('234567abcdefghijklmnopqrstuvwxyz', ((v >> 20) & 31)::int + 1, 1)
      || substr('234567abcdefghijklmnopqrstuvwxyz', ((v >> 15) & 31)::int + 1, 1)
      || substr('234567abcdefghijklmnopqrstuvwxyz', ((v >> 10) & 31)::int + 1, 1)
      || substr('234567abcdefghijklmnopqrstuvwxyz', ((v >> 5) & 31)::int + 1, 1)
      || substr('234567abcdefghijklmnopqrstuvwxyz', (v & 31)::int + 1, 1);

-- An alias id's rkey text.
CREATE FUNCTION rkey_alias_text(v bigint) RETURNS text
  LANGUAGE sql STABLE STRICT PARALLEL SAFE
  RETURN (SELECT a.rkey FROM rkey_aliases a WHERE a.id = v);

-- An rkey value at or above 0 is the TID itself (its top bit clear); below
-- 0 it is an alias. Not STRICT, so the planner can inline it (NULL still
-- gives NULL).
CREATE OR REPLACE FUNCTION bigint_to_rkey(v bigint) RETURNS text
  LANGUAGE sql STABLE PARALLEL SAFE
  RETURN CASE WHEN v >= 0 THEN tid_text(((v >> 60) & 7)::int, v) ELSE rkey_alias_text(v) END;

-- A rev value is the TID minus 2^63: its bit 63 is the sign bit flipped.
CREATE OR REPLACE FUNCTION bigint_to_rev(v bigint) RETURNS text
  LANGUAGE sql IMMUTABLE PARALLEL SAFE
  RETURN tid_text((CASE WHEN v < 0 THEN 0 ELSE 8 END) | ((v >> 60) & 7)::int, v);

-- The URI of a whole reference; NULL when any part is missing.
CREATE OR REPLACE FUNCTION at_uri(author integer, coll smallint, rkey bigint) RETURNS text
  LANGUAGE plpgsql STABLE PARALLEL SAFE AS $$
DECLARE
  d text;
  n text;
BEGIN
  SELECT x.did INTO d FROM dids x WHERE x.id = author;
  SELECT c.nsid INTO n FROM collections c WHERE c.id = coll;
  RETURN 'at://' || d || '/' || n || '/' || bigint_to_rkey(rkey);
END $$;
