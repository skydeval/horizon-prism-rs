-- A full trial's capacity: one row a minute while it runs, kept with its
-- `full_trials` row as history. Rates are entries a second over the minute:
-- `input` added to the stream, `processed` booked (applied, duplicate, shed
-- or with nothing to write), and of those `deduped`, `shed` and `applied`
-- (ran a transaction); `lanes_busy` the writer lanes' mean busy share;
-- `crawl_records` records the history crawl applied a second; `filling`
-- while S's fill ran.
CREATE TABLE full_trial_samples (
  trial         bigint NOT NULL REFERENCES full_trials (id) ON DELETE CASCADE,
  at            timestamptz NOT NULL,
  input         double precision,
  processed     double precision,
  deduped       double precision,
  shed          double precision,
  applied       double precision,
  lag           bigint,
  pending       bigint,
  time_to_trim  double precision,
  lanes_busy    double precision,
  crawl_records double precision,
  filling       boolean NOT NULL DEFAULT false,
  PRIMARY KEY (trial, at)
);

-- Full sheds strangers' quiet commits while live ingest would otherwise
-- lose unread entries; a blocking gap row says so while it does.
ALTER TABLE full_gaps DROP CONSTRAINT full_gaps_kind_check;
ALTER TABLE full_gaps ADD CONSTRAINT full_gaps_kind_check CHECK (kind IN ('trim', 'recreated', 'resume', 'continuity', 'periodic', 'shed'));
