-- Labeler declarations (app.bsky.labeler.service, rkey self; design-v3 §3.1).
CREATE TABLE labeler_services (
  uri          text PRIMARY KEY,               -- at://<did>/app.bsky.labeler.service/self
  creator_did  text NOT NULL UNIQUE,
  cid          text NOT NULL,
  rev          text COLLATE "C" NOT NULL,
  policies     jsonb NOT NULL,
  reason_types jsonb,
  subject_types jsonb,
  subject_collections jsonb,
  self_labels  text[],
  record       jsonb NOT NULL,
  created_at   timestamptz NOT NULL,
  indexed_at   timestamptz NOT NULL DEFAULT now()
);
