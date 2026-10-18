-- Replay below a settle (design replay-readiness.md §4.1, §9, #723): the
-- listed revs.
--
-- - `post_listed_rev`, `audience_listed_rev`: the `getLatestCommit` rev of
--   the last run that listed the DID's private posts (audiences) to the end
--   with no write failing, raised before that run's reconcile. Ingest refuses
--   an upsert of that collection with a rev below it; a settle raises
--   `applied_rev` only to a rev both cover. NULL = none. Both only rise.
--
-- The columns are added with NULL defaults (metadata only, no rewrite). The
-- seed covers the DIDs whose `applied_rev` a settle moved ahead of the
-- firehose: a backlog after the deploy's restart could replay below those
-- settles. Each DID's next full run raises a true listed rev.
ALTER TABLE sync_state
  ADD COLUMN post_listed_rev text COLLATE "C",
  ADD COLUMN audience_listed_rev text COLLATE "C";
UPDATE sync_state SET post_listed_rev = applied_rev, audience_listed_rev = applied_rev
WHERE applied_rev IS NOT NULL AND (firehose_rev IS NULL OR firehose_rev < applied_rev);
