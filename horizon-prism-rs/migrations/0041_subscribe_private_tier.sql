-- Private activity subscriptions (design private-activity-subs.md §3): a
-- member's choice to be notified of an account's new private posts, and the
-- queue of private posts whose subscribed-post notifications are due.
--
-- private_activity_subscriptions: one row per (subscriber, subject). A kind
-- is on while its *_since is set: the time it last went from off to on, which
-- the fan-out compares with the post's createdAt. Both NULL deletes the row
-- instead. Written only by the member's own put; independent of
-- activity_subscriptions (no scope hold, no sweep). Deleted by the
-- account_deleted and member_removed purges where the DID is the subscriber
-- or the subject.
--
-- private_post_fanout: one row per private post first stored within the
-- notify age, written in the store's transaction; the fan-out worker takes it
-- after commit. Kept two days after it is done, so a post fans out once.
--
-- New tables only: no existing row is touched.
CREATE TABLE private_activity_subscriptions (
  subscriber_did text NOT NULL,
  subject_did    text NOT NULL,
  post_since     timestamptz,
  reply_since    timestamptz,
  updated_at     timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (subscriber_did, subject_did),
  CHECK (post_since IS NOT NULL OR reply_since IS NOT NULL)
);
CREATE INDEX private_activity_subscriptions_subject ON private_activity_subscriptions (subject_did);
CREATE INDEX private_activity_subscriptions_listing
  ON private_activity_subscriptions (subscriber_did, updated_at DESC, subject_did DESC);

CREATE TABLE private_post_fanout (
  uri         text PRIMARY KEY,
  author_did  text NOT NULL,
  first_at    timestamptz NOT NULL DEFAULT now(),
  not_before  timestamptz NOT NULL,
  attempts    int NOT NULL DEFAULT 0,
  pending     text[],          -- NULL: no subscriber evaluated yet; else those deferred
  done_at     timestamptz
);
CREATE INDEX private_post_fanout_due ON private_post_fanout (not_before) WHERE done_at IS NULL;
CREATE INDEX private_post_fanout_done ON private_post_fanout (done_at) WHERE done_at IS NOT NULL;
CREATE INDEX private_post_fanout_author ON private_post_fanout (author_did);
