-- The purpose of a subscribed list (design-v3 §4.3 "List purpose"): a
-- member's own list block or list mute has an effect only on a moderation
-- list. Nullable columns: metadata-only. No data step: this runs before any
-- code inserts a subscribed_lists row, so no row exists.

-- The purpose read from the list's own record (the full token, e.g.
-- app.bsky.graph.defs#modlist), the rev it was read at, and since when the
-- record has been missing from the owner's listing.
ALTER TABLE subscribed_lists ADD COLUMN purpose text;
ALTER TABLE subscribed_lists ADD COLUMN purpose_rev text COLLATE "C";
ALTER TABLE subscribed_lists ADD COLUMN unlisted_since timestamptz;

-- Set once, when a run completed without listing the record of a reference
-- that was waiting to take effect.
ALTER TABLE list_references ADD COLUMN unlisted_at timestamptz;
