-- Notification preferences v2 (design-v3 §3.1). AppView-private.
CREATE TABLE notification_preferences_v2 (
  did        text PRIMARY KEY,
  prefs      jsonb NOT NULL,                   -- app.bsky.notification.defs#preferences
  updated_at timestamptz NOT NULL DEFAULT now()
);
