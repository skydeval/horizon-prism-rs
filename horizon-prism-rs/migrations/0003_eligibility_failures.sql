-- Owner-eligibility confirmation failures (design-v2 §6.11.1): consecutive
-- failed getRepoStatus confirmations (network error, 5xx) and when the streak
-- began. Three failures spanning at least 24 h clear eligibility; any
-- definitive answer resets both.
ALTER TABLE actors
  ADD COLUMN eligibility_failure_count integer NOT NULL DEFAULT 0,
  ADD COLUMN eligibility_failing_since timestamptz;
