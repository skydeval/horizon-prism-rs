-- Degraded mode (design degraded-mode.md §2.4, #822): each tracked repo's
-- contiguous position.
--
-- - `chain_rev`: a rev r such that every commit of the repo with rev <= r
--   was applied by the firehose or is covered by a settle's listing. Raised
--   by a settle, and by a firehose commit whose `since` chains onto it.
--   NULL = no settle yet. Always <= `applied_rev`.
-- - `chain_behind_since`: when the prober first found `head_rev >
--   chain_rev` (or `chain_rev` NULL); NULL otherwise. The degraded probe's
--   clock.
-- - `sync_chain_links`: commits the firehose applied above `chain_rev`
--   whose `since` did not chain when they arrived, waiting for the chain
--   to reach them. At most 256 per DID; deleted with the DID's row.
--
-- Columns are added with NULL defaults (metadata only, no rewrite). Every
-- DID's chain starts at its next settle.
ALTER TABLE sync_state
  ADD COLUMN chain_rev text COLLATE "C",
  ADD COLUMN chain_behind_since timestamptz;

CREATE TABLE sync_chain_links (
  did text NOT NULL REFERENCES sync_state (did) ON DELETE CASCADE,
  rev text COLLATE "C" NOT NULL,
  since text COLLATE "C" NOT NULL,
  PRIMARY KEY (did, rev)
);
