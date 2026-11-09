-- Full indexing's third-party labels: what each subscribed labeler issued,
-- one row per (labeler, subject, record version, value). Subjects stay text,
-- as labelers publish them. The newest `cts` per key wins, so a negation or a
-- re-issue replaces the row; a negation is kept as a row with `neg` set.
-- A label whose signature did not verify is kept with `verified` false and
-- never applied.
CREATE TABLE full_labels (
  src      text        NOT NULL,
  uri      text        NOT NULL,
  cid      text        NOT NULL DEFAULT '',
  val      text        NOT NULL,
  neg      boolean     NOT NULL DEFAULT false,
  cts      timestamptz NOT NULL,
  exp      timestamptz,
  sig      bytea,
  verified boolean     NOT NULL,
  PRIMARY KEY (src, uri, cid, val)
);
CREATE INDEX full_labels_uri ON full_labels (uri, src);
CREATE INDEX full_labels_unverified ON full_labels (src, uri) WHERE NOT verified;
