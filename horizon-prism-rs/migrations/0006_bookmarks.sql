-- Bookmarks (design-v3 §3.1). AppView-private: not a repo record.
CREATE TABLE bookmarks (
  viewer_did  text NOT NULL,
  post_uri    text NOT NULL,
  post_cid    text NOT NULL,                   -- createBookmark input cid; bookmarkView.subject
  created_at  timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (viewer_did, post_uri)
);
CREATE INDEX bookmarks_viewer ON bookmarks (viewer_did, created_at DESC, post_uri);
CREATE INDEX bookmarks_post   ON bookmarks (post_uri);
