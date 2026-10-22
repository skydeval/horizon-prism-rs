-- The admin UI's Appearance page (#810): each operator's own look of the
-- admin pages, kept with the operator so it follows them to any device.
--
-- - `theme_mode`: 'system' (follow the device), 'light' or 'dark'.
-- - `theme_scheme`: the colour scheme's name ('sky' is the default).
--
-- No CHECK on either: the pages read an unknown value as the default, so a
-- scheme can be added or dropped without a migration. Removing an operator
-- removes their choice with the row.
ALTER TABLE admin_operators
  ADD COLUMN theme_mode   text NOT NULL DEFAULT 'system',
  ADD COLUMN theme_scheme text NOT NULL DEFAULT 'sky';
