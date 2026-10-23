-- Reposts of restored posts (refract-reposts design §5): when a restored
-- DID's older reposts were filled from Constellation backlinks, once per
-- restore. NULL (every row on deploy, and every new restore) means not yet.
ALTER TABLE refract_dids ADD COLUMN reposts_filled_at timestamptz;
