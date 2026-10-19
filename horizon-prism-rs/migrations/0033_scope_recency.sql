-- Scope recency (scope-recency design §6): the daily sweep reads members'
-- replies and quotes of the last `scope.reply_quote_days` days, so the
-- reference index carries the date. It replaces the author-only index,
-- which every query that used it can use in this one's place. No row
-- changes.
CREATE INDEX member_post_refs_author_created ON member_post_refs (author_did, created_at);
DROP INDEX member_post_refs_author;
