-- Private posting S3 (design private-posting.md §3.1, §4.3, P-8): a member's
-- bookmarks of private posts, kept apart from `bookmarks` (C-12: their own
-- cap, counted over this table alone).
--
-- - A row is the viewer's own state, written by `createBookmark` from the
--   URI alone: the post is never read on a write, so a row says nothing
--   about whether the post exists.
-- - Joins are on `post_uri`; `post_cid` is echoed, never compared (§3.5).
-- - `getBookmarks` walks the viewer's rows newest first through the index.
--
-- A new table only: no existing row is touched.
CREATE TABLE private_bookmarks (
    viewer_did text NOT NULL,
    post_uri   text NOT NULL,
    post_cid   text NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (viewer_did, post_uri)
);
CREATE INDEX private_bookmarks_viewer ON private_bookmarks (viewer_did, created_at DESC, post_uri DESC);
