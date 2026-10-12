-- `via` strongRefs (follow → starter pack; like/repost → repost) and
-- starter-pack attribution on notifications (design-v3 §3.1). Nullable
-- columns: metadata-only. The likes_via and reposts_via indexes are built
-- by the post-migration task (§12.2), not here.
ALTER TABLE follows ADD COLUMN via_uri text, ADD COLUMN via_cid text;
ALTER TABLE likes   ADD COLUMN via_uri text, ADD COLUMN via_cid text;
ALTER TABLE reposts ADD COLUMN via_uri text, ADD COLUMN via_cid text;
ALTER TABLE notifications ADD COLUMN starter_pack_uri text;
