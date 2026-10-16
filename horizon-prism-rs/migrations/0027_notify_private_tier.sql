-- Private posting S6 (design private-posting.md §3.1, §4.8, P-9): private
-- notifications and the member's last-seen time for them.
--
-- - Written by the private tier in every public-indexing mode: from a private
--   post (`reply`, `quote`, `mention`) at ingest, and from a member's public
--   like, repost, quote or reply of a private post (`public_record` holds a
--   post's record, since under the off rule no `posts` row exists).
-- - Each row's `reason_subject` is a post in the recipient's own repository;
--   `record_root` is the thread root the record claims, for thread mutes.
-- - Joins are on `record_uri` and `reason_subject` (§3.5).
-- - Deleted with their record, by the `account_deleted` purge (rows the DID
--   received or authored), the `member_removed` purge (the same) and 90-day
--   retention, as `notifications`.
--
-- New tables only: no existing row is touched.
CREATE TABLE private_notifications (
  id             bigserial PRIMARY KEY,
  recipient_did  text NOT NULL,
  author_did     text NOT NULL,
  reason         text NOT NULL,          -- like|repost|reply|mention|quote
  reason_subject text,
  record_uri     text NOT NULL,
  record_cid     text NOT NULL,
  record_root    text,                   -- thread root of record_uri, for thread mutes
  public_record  jsonb,                  -- the record, when record_uri is a public post
  indexed_at     timestamptz NOT NULL DEFAULT now(),
  UNIQUE (recipient_did, record_uri, reason)
);
CREATE INDEX private_notifications_recipient ON private_notifications (recipient_did, indexed_at DESC, id DESC);
CREATE INDEX private_notifications_record ON private_notifications (record_uri);
-- The purges take the rows a DID authored; retention takes old rows.
CREATE INDEX private_notifications_author ON private_notifications (author_did);
CREATE INDEX private_notifications_indexed ON private_notifications (indexed_at);
CREATE TABLE private_notification_seen (did text PRIMARY KEY, seen_at timestamptz NOT NULL);
