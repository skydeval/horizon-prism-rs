-- Profile fields and the other rkey-self actor records (design-v3 §3.1).
-- Nullable columns: metadata-only. The partial index is empty at creation.
ALTER TABLE actors
  ADD COLUMN profile_cid text,                 -- CID of the stored profile record
  ADD COLUMN pronouns text,
  ADD COLUMN website text,
  ADD COLUMN joined_via_starter_pack_uri text,
  ADD COLUMN joined_via_starter_pack_cid text,
  ADD COLUMN profile_created_at timestamptz;   -- record createdAt, clamped as for posts
CREATE INDEX actors_joined_via ON actors (joined_via_starter_pack_uri)
  WHERE joined_via_starter_pack_uri IS NOT NULL;

-- app.bsky.notification.declaration, app.bsky.actor.status,
-- app.bsky.actor.contentVisibilityDeclaration
CREATE TABLE actor_self_records (
  uri          text PRIMARY KEY,               -- at://<did>/<collection>/self
  did          text NOT NULL,
  collection   text NOT NULL,
  cid          text NOT NULL,
  rev          text COLLATE "C" NOT NULL,
  record       jsonb NOT NULL,
  indexed_at   timestamptz NOT NULL DEFAULT now(),
  UNIQUE (did, collection)
);
