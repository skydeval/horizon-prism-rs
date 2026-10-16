-- Private posting S10, the `following` audience mode (design
-- following-audience.md §3.1, §8): the follow-verification flag.
--
-- - `follows_verified_at`: set when a settle compared the DID's stored
--   follows with its repository to the end; NULL = not verified.
-- - `follows_verified_rev`: the `applied_rev` that settle left behind.
-- - `firehose_rev`: the highest rev the firehose itself applied for the DID;
--   written only by the firehose's advance.
-- - `follows_clears`: a token a clear replaces with a fresh value from
--   `follows_clears_seq`, so a run that read it before a clear sets nothing.
--   Rows that exist now hold 0, which the sequence never yields.
-- The flag holds when `follows_verified_at IS NOT NULL AND firehose_rev >=
-- follows_verified_rev`, in one row.
--
-- Metadata-only on `sync_state`: every column is added with a constant
-- default, and the sequence default applies to new rows only.
--
-- The `gap_epoch` seed lets a settle read the epoch with `fetch_one` before
-- the first gap; '0' is what a missing row already read as.
CREATE SEQUENCE follows_clears_seq;
ALTER TABLE sync_state
  ADD COLUMN follows_verified_at timestamptz,
  ADD COLUMN follows_verified_rev text COLLATE "C",
  ADD COLUMN firehose_rev text COLLATE "C",
  ADD COLUMN follows_clears bigint NOT NULL DEFAULT 0;
ALTER TABLE sync_state ALTER COLUMN follows_clears SET DEFAULT nextval('follows_clears_seq');
CREATE INDEX private_audiences_following ON private_audiences (owner_did) WHERE mode = 'following';
INSERT INTO worker_state (key, value) VALUES ('gap_epoch', '0'::jsonb) ON CONFLICT (key) DO NOTHING;
