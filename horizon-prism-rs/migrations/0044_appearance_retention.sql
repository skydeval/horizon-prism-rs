-- Private appearance retention (design private-appearance-amendment-1 §3): a
-- removed member's appearance row is kept up to 7 days, for its owner alone,
-- then swept.
--
-- - retained_until: NULL on a live row. Set at removal (in the removal
--   marker's transaction) to now() + 7 days; cleared only by a member's put,
--   which makes the row live again. A row past it is absent to every method
--   and deleted by the daily private retention.
-- - Setting it is not a write: rev and updated_at stay.
--
-- A new nullable column and a partial index: no existing row is touched.
ALTER TABLE private_appearance ADD COLUMN retained_until timestamptz;
CREATE INDEX appearance_retained_idx ON private_appearance (retained_until) WHERE retained_until IS NOT NULL;
