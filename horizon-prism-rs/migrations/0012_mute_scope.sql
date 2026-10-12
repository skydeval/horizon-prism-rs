-- Scoped mutes (design-v3 §3.1, §5.3; locked decision 32).
--
-- One column, four values, because `muteActor` may set both `onlyReposts`
-- and `onlyQuoteposts`. A constant default is metadata-only in Postgres 16,
-- and every existing row reads as `full`, which is what it is.
--
-- This migration is written ahead of 0004-0011 because scoped mutes are
-- built first (§II-15 item 10); all of 0004-0017 ship as one upgrade.
ALTER TABLE mutes ADD COLUMN scope text NOT NULL DEFAULT 'full'
  CHECK (scope IN ('full', 'reposts', 'quotes', 'reposts_and_quotes'));
