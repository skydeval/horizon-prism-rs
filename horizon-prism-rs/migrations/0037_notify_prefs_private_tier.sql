-- Private notification preferences (design private-notification-prefs.md §3):
-- a member's own choices for its private notifications, read by
-- listNotifications and getUnreadCount at read time.
--
-- - One row per member that has written one; no row means every reason is
--   listed, from everyone.
-- - prefs maps a reason (like, repost, reply, mention, quote) to an
--   app.bsky.notification.defs#filterablePreference; a reason it lacks has
--   the default.
-- - Deleted by the account_deleted and member_removed purges, with the
--   member's private notifications. Never swept by retention.
--
-- New table only: no existing row is touched.
CREATE TABLE private_notification_prefs (
  did        text PRIMARY KEY,
  prefs      jsonb NOT NULL,
  updated_at timestamptz NOT NULL DEFAULT now()
);
