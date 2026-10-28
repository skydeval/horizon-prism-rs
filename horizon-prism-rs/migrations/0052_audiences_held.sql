-- Private audiences held by this AppView (private-audiences design §3).
-- Audiences are no longer read from repositories: every row is written by
-- the four audience methods from now on. Re-runnable (the data tests run it
-- again on a migrated database); step 2 rewrites the timestamps each run.

-- 1. The record's own times, which the methods write.
ALTER TABLE private_audiences ADD COLUMN IF NOT EXISTS created_at timestamptz,
                              ADD COLUMN IF NOT EXISTS updated_at timestamptz;

-- 2. The existing rows: the record's times were never stored, so both read
-- as the first store (`indexed_at`, which no upsert moved).
UPDATE private_audiences SET created_at = indexed_at, updated_at = indexed_at, display_filled = true;

-- 3. Required from now on; the defaults keep direct inserts valid.
ALTER TABLE private_audiences ALTER COLUMN created_at SET NOT NULL,
                              ALTER COLUMN created_at SET DEFAULT now(),
                              ALTER COLUMN updated_at SET NOT NULL,
                              ALTER COLUMN updated_at SET DEFAULT now();

-- 4. listAudiences walks a member's own rows by record key.
CREATE INDEX IF NOT EXISTS private_audiences_owner ON private_audiences (owner_did, rkey);

-- 5. No listing writes an audience any more, so Invariant R no longer
-- needs the audience listed rev.
ALTER TABLE sync_state DROP COLUMN IF EXISTS audience_listed_rev;

-- 6. Rows whose owner is not a member: nothing would ever update them, and
-- they would go live, stale, if the account joined later.
DELETE FROM private_audiences a WHERE NOT EXISTS (SELECT 1 FROM members m WHERE m.did = a.owner_did);

-- 7. What the methods' record check would refuse, so an unchanged round
-- trip passes. An unknown mode already read as no one; `nobody` keeps that.
UPDATE private_audiences SET mode = 'nobody'
 WHERE mode IS NOT NULL AND mode NOT IN ('list', 'everyone', 'followers', 'following', 'nobody');
-- Names trimmed of Unicode White_Space at both ends (as Rust's `str::trim`),
-- written with escapes only so that it does not depend on the database's
-- ctype; a name left empty is no name.
UPDATE private_audiences
   SET name = NULLIF(regexp_replace(name,
         '^[\x09-\x0d\x20\x85\xa0\x1680\x2000-\x200a\x2028\x2029\x202f\x205f\x3000]+|[\x09-\x0d\x20\x85\xa0\x1680\x2000-\x200a\x2028\x2029\x202f\x205f\x3000]+$',
         '', 'g'), '')
 WHERE name IS NOT NULL;
