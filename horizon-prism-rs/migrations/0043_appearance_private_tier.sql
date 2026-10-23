-- Private appearance (design private-appearance.md §3): a member's choice of
-- where its appearance is saved, and the appearance itself for 'private' and
-- 'public' ('device' keeps the choice alone).
--
-- - One row per member that has put one; no row means the member never chose.
-- - appearance is an app.nearhorizon.actor.appearance record value, as put:
--   json, not jsonb, so its text (a NUL escape included) is kept as given.
-- - rev changes on every write (strictly increasing per row); clients swap on
--   it. created_at is the first time the choice was set (migratedAt).
-- - Deleted by the account_deleted and member_removed purges. Never swept by
--   retention.
--
-- New table only: no existing row is touched.
CREATE TABLE private_appearance (
  did        text PRIMARY KEY,
  store      text NOT NULL CHECK (store IN ('private', 'public', 'device')),
  appearance json,
  rev        bigint NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  CHECK ((appearance IS NOT NULL) = (store IN ('private', 'public')))
);
