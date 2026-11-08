-- Full indexing trials: one row per trial, kept as history, and the record
-- of every row a running trial stored that the mode it started from would
-- not have stored.

CREATE TABLE full_trials (
  id         bigserial PRIMARY KEY,
  "from"     text NOT NULL CHECK ("from" IN ('community', 'kryphocron_only')),
  "to"       text NOT NULL CHECK ("to" IN ('community', 'kryphocron_only')),
  state      text NOT NULL CHECK (state IN ('measuring', 'running', 'ending', 'checking', 'passed', 'failed',
                                            'dismissed', 'kept', 'superseded', 'abandoned')),
  started_at timestamptz,
  ended_at   timestamptz,
  ended_by   text,
  bounds     jsonb NOT NULL DEFAULT '{}'::jsonb,
  baseline   jsonb NOT NULL DEFAULT '{}'::jsonb,
  result     jsonb
);

-- The record. One trial at a time, so no trial id; emptied with TRUNCATE
-- when the trial's record is dropped. Logged: a crash must not lose it.
-- `tbl` names the table; `a`, `b`, `c` its key (author or originator, rkey
-- or alias id, and 0, a collection id or the repost flag); `rev` the
-- profile rev a trial wrote; `s_*` and `q_*` the subjects whose counts the
-- row moved.
CREATE TABLE full_trial_rows (
  tbl smallint NOT NULL,
  a   integer  NOT NULL,
  b   bigint   NOT NULL,
  c   smallint NOT NULL,
  rev bigint,
  s_a integer,
  s_b bigint,
  q_a integer,
  q_b bigint,
  PRIMARY KEY (tbl, a, b, c)
);
