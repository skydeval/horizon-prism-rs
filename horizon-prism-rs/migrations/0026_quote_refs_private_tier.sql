-- Private posting S5 (design private-posting.md §3.1, §4.7, P-5): a member's
-- public `app.bsky.feed.post` that quotes a `tools.kryphocron.feed.postPrivate`
-- post. Written by the private tier in every public-indexing mode, last writer
-- winning on rev; deleted with the post (by rev, as `member_post_refs`), by
-- the `account_deleted` purge (the author's rows) and by the `member_removed`
-- purge (the DID's rows).
--
-- - `getQuotes` lists these as `#publicPostRef {uri, cid}`; `quoteCount`
--   counts them, by an author with no block either way with the reader.
-- - Private posts that quote a private post are found through
--   `private_posts.quote_uri` instead.
-- - Joins are on `quoted_uri` (§3.5).
--
-- A new table only: no existing row is touched.
CREATE TABLE private_quote_refs (
  uri        text PRIMARY KEY,           -- the quoting app.bsky.feed.post
  author_did text NOT NULL,
  cid        text NOT NULL,
  rev        text COLLATE "C" NOT NULL,
  quoted_uri text NOT NULL,              -- a postPrivate URI
  created_at timestamptz NOT NULL
);
CREATE INDEX private_quote_refs_quoted ON private_quote_refs (quoted_uri, created_at DESC, uri DESC);
CREATE INDEX private_quote_refs_author ON private_quote_refs (author_did);
