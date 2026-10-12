-- Activity subscriptions (design-v3 §3.1). AppView-private.
CREATE TABLE activity_subscriptions (
  subscriber_did text NOT NULL,
  subject_did    text NOT NULL,
  post           boolean NOT NULL,
  reply          boolean NOT NULL,
  created_at     timestamptz NOT NULL DEFAULT now(),
  updated_at     timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (subscriber_did, subject_did),
  CHECK (post OR reply)                        -- {false, false} deletes the row instead
);
CREATE INDEX activity_subscriptions_subject ON activity_subscriptions (subject_did);
