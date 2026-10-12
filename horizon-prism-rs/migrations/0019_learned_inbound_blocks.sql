-- The memory of learned inbound list blocks (design-v3 §5.14.18; proposed
-- locked decision 39). Optional, and off by default: with
-- farsight.memory.enabled = false nothing reads or writes this table, and
-- serve empties it at startup.
--
-- One row per (viewer, other) pair for which a live answer reported that
-- `other` blocks the viewer through a list. Only positives are stored. The
-- direction is always inbound, so there is no direction column.
--
-- AppView-private state: a row says that the viewer rendered the other
-- account, and roughly when. It is read only while rendering for that
-- viewer, never by a public path, and never by the private tier.
CREATE TABLE learned_inbound_blocks (
    viewer_did   text        NOT NULL,   -- a ready member, canonical form
    other_did    text        NOT NULL,   -- the blocker, canonical form
    -- The list the answer named, when it is short and parses as an at-uri.
    list_uri     text        CHECK (list_uri IS NULL OR octet_length(list_uri) <= 512),
    source       text        NOT NULL CHECK (source IN ('farsight', 'fallback')),
    confirmed_at timestamptz NOT NULL,   -- when a live answer last said so
    PRIMARY KEY (viewer_did, other_did)
);
CREATE INDEX learned_inbound_blocks_confirmed ON learned_inbound_blocks (confirmed_at);
CREATE INDEX learned_inbound_blocks_other     ON learned_inbound_blocks (other_did);
