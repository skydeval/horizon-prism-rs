-- Refract (refract design, docs/internal/refract/refract.md): accounts
-- Bluesky's AppView withholds stay usable while a community the operator
-- lists still hosts them and has not banned them.
--
-- - `refract_dids`: one row per DID the checker knows (§4.2). `state`:
--   candidate | rejected | restored | grace | ending. `version` is bumped
--   by every transition and every live config change that concerns the
--   DID, so a verdict computed against an older row writes nothing.
--   `due_seq` is bumped by every "due now" request. `candidate_since`:
--   when the row last became a candidate (a candidate whose checks stay
--   unknown is rejected 24 h after). `resync_at`: the next weekly resync
--   (§6.1).
-- - `refract_events`: the audit trail, kept `refract.event_days`.
-- - `refract_labels`: labels from a DID's own community's labeler, for
--   candidates, R and `ending` only (D-2). `did` is the subject's DID (the
--   at-URI authority for a record label). `val = '!refract-absent'` is the
--   synthetic omission row (§4.5).
-- - `refract_notifications`: the `notifications` columns, written for
--   member recipients of restored authors' firehose records (§5.3).
-- - `refract_pending` / `refract_pending_dids`: the pending-removal scopes
--   (`feature`, `entry:<hash>`) and the DIDs each covers (§4.6).
-- - `refract_record_reads`: the community AppView reads of restored posts
--   (§4.5): last read, last time it returned the post, confirming
--   omissions since.
-- - `backfill_jobs.refract_cursor`: a `refract` run's saved position
--   (§6.1), and the claim index of the `refract` workers.
CREATE TABLE refract_dids (
  did text PRIMARY KEY,
  community text NOT NULL,
  pds_host text,
  state text NOT NULL CHECK (state IN ('candidate', 'rejected', 'restored', 'grace', 'ending')),
  version bigint NOT NULL DEFAULT 0,
  due_seq bigint NOT NULL DEFAULT 0,
  unverified_since timestamptz,
  awaiting_labels_since timestamptz,
  since timestamptz,
  candidate_since timestamptz,
  checked_at timestamptz,
  next_check_at timestamptz NOT NULL DEFAULT now(),
  attempts integer NOT NULL DEFAULT 0,
  last_verdict jsonb,
  grace_until timestamptz,
  ended_reason text,
  source text NOT NULL CHECK (source IN ('fill', 'admin')),
  resync_at timestamptz,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX refract_dids_due ON refract_dids (next_check_at) WHERE state IN ('candidate', 'restored', 'grace');
CREATE INDEX refract_dids_state ON refract_dids (state);

CREATE TABLE refract_events (
  id bigserial PRIMARY KEY,
  did text NOT NULL,
  at timestamptz NOT NULL DEFAULT now(),
  from_state text,
  to_state text NOT NULL,
  reason text
);
CREATE INDEX refract_events_did ON refract_events (did, at DESC);
CREATE INDEX refract_events_at ON refract_events (at);

CREATE TABLE refract_labels (
  labeler_did text NOT NULL,
  subject text NOT NULL,
  val text NOT NULL,
  did text NOT NULL,
  cts timestamptz,
  exp timestamptz,
  gone boolean NOT NULL,
  verified boolean NOT NULL,
  PRIMARY KEY (labeler_did, subject, val)
);
CREATE INDEX refract_labels_did ON refract_labels (did);

CREATE TABLE refract_notifications (
  id bigserial PRIMARY KEY,
  recipient_did text NOT NULL,
  author_did text NOT NULL,
  reason text NOT NULL,
  reason_subject text,
  record_uri text NOT NULL,
  record_cid text NOT NULL,
  indexed_at timestamptz NOT NULL DEFAULT now(),
  starter_pack_uri text,
  UNIQUE (recipient_did, record_uri, reason)
);
CREATE INDEX refract_notifications_recipient ON refract_notifications (recipient_did, indexed_at DESC, id DESC);
CREATE INDEX refract_notifications_record ON refract_notifications (record_uri);
CREATE INDEX refract_notifications_author ON refract_notifications (author_did);
CREATE INDEX refract_notifications_indexed ON refract_notifications (indexed_at);

CREATE TABLE refract_pending (
  scope text PRIMARY KEY,
  first_seen timestamptz NOT NULL DEFAULT now(),
  keep boolean NOT NULL DEFAULT false
);
CREATE TABLE refract_pending_dids (
  scope text NOT NULL REFERENCES refract_pending (scope) ON DELETE CASCADE,
  did text NOT NULL,
  PRIMARY KEY (scope, did)
);
CREATE INDEX refract_pending_dids_did ON refract_pending_dids (did);

CREATE TABLE refract_record_reads (
  uri text PRIMARY KEY,
  did text NOT NULL,
  read_at timestamptz NOT NULL,
  seen_at timestamptz,
  omissions integer NOT NULL DEFAULT 0
);
CREATE INDEX refract_record_reads_did ON refract_record_reads (did);
CREATE INDEX refract_record_reads_read_at ON refract_record_reads (read_at);

ALTER TABLE backfill_jobs ADD COLUMN refract_cursor jsonb;
CREATE INDEX backfill_jobs_claim_refract ON backfill_jobs (not_before)
    WHERE state = 'queued' AND depth = 'refract';
