-- Private posting S2 (design private-posting.md §3.1, P-1, P-3): the display
-- fields of a private post, stored as received, and the audience's `name`.
--
-- - `facets`, `embed`, `langs` and `self_labels` are content, read only
--   behind the per-record witness. `quote_uri` and `mention_dids` are
--   structural facts derived at ingest from `embed` and `facets`.
-- - `display_filled` is false on every row stored before this migration.
--   The upsert lets a row that is not filled take an op at an equal rev, and
--   the first start after this migration queues a `private` job for each
--   DID with such rows (worker_state['private_display_refill']), so the
--   listing at the repo's latest rev fills them.
--
-- Every column added is nullable or has a constant default, so the ALTERs
-- rewrite no row. The two indexes are built here, not concurrently: the
-- table holds a few thousand rows at this release (§3.1).
ALTER TABLE private_posts
    ADD COLUMN facets jsonb,
    ADD COLUMN embed jsonb,
    ADD COLUMN langs text[],
    ADD COLUMN self_labels jsonb,
    ADD COLUMN quote_uri text,
    ADD COLUMN mention_dids text[] NOT NULL DEFAULT '{}',
    ADD COLUMN display_filled boolean NOT NULL DEFAULT false;
CREATE INDEX private_posts_quote ON private_posts (quote_uri) WHERE quote_uri IS NOT NULL;
CREATE INDEX private_posts_recent ON private_posts (created_at DESC, uri DESC);
ALTER TABLE private_audiences
    ADD COLUMN name text,
    ADD COLUMN display_filled boolean NOT NULL DEFAULT false;
