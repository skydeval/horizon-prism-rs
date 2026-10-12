-- Backfill job generations, list_sync reruns and priority (design-v3 §3.1,
-- §II-5.10, §4.3). Constant defaults: metadata-only.
ALTER TABLE backfill_jobs ADD COLUMN generation bigint NOT NULL DEFAULT 0;
ALTER TABLE backfill_jobs ADD COLUMN rerun_requested boolean NOT NULL DEFAULT false;
ALTER TABLE backfill_jobs ADD COLUMN priority smallint NOT NULL DEFAULT 0;
