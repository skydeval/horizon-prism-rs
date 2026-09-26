-- Reply strong refs on private posts (design-v2 §4.2, §7.9): the
-- privatePostView lexicon carries reply.{root,parent}.{uri,cid}. Rows written
-- before this migration keep NULL cids until an `admin backfill --depth
-- resync` refetches them.
ALTER TABLE private_posts
  ADD COLUMN reply_root_cid text,
  ADD COLUMN reply_parent_cid text;
