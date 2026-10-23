-- Private blocks removed (skydeval, 2026-10-24): a public block already keeps
-- two accounts apart, so `tools.kryphocron.graph.block` is no longer indexed
-- and no read rule reads it. Only `app.bsky.graph.block` and members' list
-- blocks count. The table and its index go, with any rows it held.
DROP TABLE private_blocks;
