-- A full trial's record of the fill jobs it touched: S's community fill,
-- run by a trial from kryphocron_only, queues a `resync` for every member
-- and S account. Each row is that job as it was before the fill touched it
-- (`existed` false when the fill created it), and `fill_generation` the
-- generation the fill gave it. The exit puts each job back; emptied with
-- TRUNCATE with the rest of the record.
CREATE TABLE full_trial_jobs (
  did             text        NOT NULL,
  depth           text        NOT NULL,
  existed         boolean     NOT NULL,
  state           text,
  attempts        int,
  not_before      timestamptz,
  last_error      text,
  updated_at      timestamptz,
  generation      bigint,
  gap_epoch       bigint,
  rerun_requested boolean,
  priority        smallint,
  refract_cursor  jsonb,
  fill_generation bigint      NOT NULL,
  PRIMARY KEY (did, depth)
);
