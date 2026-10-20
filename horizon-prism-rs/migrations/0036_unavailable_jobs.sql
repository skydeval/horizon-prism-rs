-- Unavailable accounts (design actionable-warnings.md §3, §8): a sync job
-- whose account is not there to read ends in the new state 'unavailable'
-- instead of 'failed'. `backfill_jobs.state` has no CHECK constraint, so the
-- state needs no schema change; this reclassifies the rows already failed.
--
-- 1. Failed jobs whose last error is one of the definitive "not there"
--    answers the code now settles as 'unavailable' (the same text, written
--    by `prepare`). `updated_at` is kept, so retention ages them from when
--    they failed. `list_sync` rows have their own runner and are not
--    touched.
-- 2. The other failed `scope` and `resync` jobs of accounts Prism still
--    follows would not have failed yet under their longer schedule (§4):
--    they are queued again, at a new generation as every reset is. Members'
--    own jobs keep their schedule and their retry loop.
--
-- Both statements leave rows they already moved alone.
UPDATE backfill_jobs SET state = 'unavailable'
 WHERE state = 'failed' AND depth <> 'list_sync'
   AND (last_error = 'repo not found at its PDS'
        OR last_error LIKE 'repo inactive (%)'
        OR last_error = 'DID document names no #atproto_pds'
        OR last_error LIKE 'resolve: DID tombstoned%');

UPDATE backfill_jobs SET state = 'queued', attempts = 0, generation = generation + 1,
       not_before = now(), updated_at = now()
 WHERE state = 'failed' AND depth IN ('scope', 'resync')
   AND EXISTS (SELECT 1 FROM scope_dids s WHERE s.did = backfill_jobs.did);
