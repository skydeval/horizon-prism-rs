-- The two claim orders of the backfill workers (public-indexing design
-- §8.4): readiness-capable jobs first, then by due time; and due time
-- alone. Both cover only the rows a claim can take.
CREATE INDEX backfill_jobs_claim_priority ON backfill_jobs ((depth IN ('member', 'private', 'authz')) DESC, not_before)
    WHERE state = 'queued' AND depth <> 'list_sync';
CREATE INDEX backfill_jobs_claim_plain ON backfill_jobs (not_before)
    WHERE state = 'queued' AND depth <> 'list_sync';
