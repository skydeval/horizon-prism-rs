-- Membership not tied to hosting (design membership.md §2.2, §2.3, §10,
-- #733).
--
-- - `member_denials`: DIDs the operator has excluded. A denied DID is never
--   auto-enrolled, `admin member add` refuses it until `admin member allow`,
--   and `admin member deny` removes a member in the transaction that writes
--   the denial. No committed state has a DID in both `members` and here.
--   Written by the operator only.
-- - The owner rule becomes "has private rows and is a member". Community-PDS
--   eligibility goes: the DIDs it alone made owners stop being owners here,
--   as `owners::reevaluate` would drop them (readiness, then the repo-head
--   state of a DID no longer tracked, the whole `sync_state` row once it is
--   outside S), and their queued or running `private`/`authz` jobs are
--   cancelled with a new generation. The in-memory owner set is loaded from
--   the database at start, so it follows.
CREATE TABLE member_denials (
  did        text PRIMARY KEY,
  reason     text,                       -- operator's note, never shown to the DID
  denied_by  text NOT NULL,              -- 'cli' or an operator DID
  denied_at  timestamptz NOT NULL DEFAULT now()
);

DELETE FROM private_owner_ready r WHERE NOT EXISTS (SELECT 1 FROM members m WHERE m.did = r.did);
UPDATE sync_state s SET applied_rev = NULL, head_rev = NULL, head_checked_at = NULL, diverged_since = NULL,
       follows_verified_at = NULL, follows_clears = nextval('follows_clears_seq')
 WHERE NOT EXISTS (SELECT 1 FROM members m WHERE m.did = s.did)
   AND EXISTS (SELECT 1 FROM private_owner_eligible e WHERE e.did = s.did)
   AND EXISTS (SELECT 1 FROM scope_dids d WHERE d.did = s.did);
DELETE FROM sync_state s
 WHERE NOT EXISTS (SELECT 1 FROM members m WHERE m.did = s.did)
   AND EXISTS (SELECT 1 FROM private_owner_eligible e WHERE e.did = s.did)
   AND NOT EXISTS (SELECT 1 FROM scope_dids d WHERE d.did = s.did);
UPDATE backfill_jobs SET state = 'cancelled', generation = generation + 1, updated_at = now()
 WHERE depth IN ('private', 'authz') AND state IN ('queued', 'running')
   AND NOT EXISTS (SELECT 1 FROM members m WHERE m.did = backfill_jobs.did);
DROP TABLE private_owner_eligible;
ALTER TABLE actors DROP COLUMN eligibility_failure_count, DROP COLUMN eligibility_failing_since;
