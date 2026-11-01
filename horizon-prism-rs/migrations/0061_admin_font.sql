-- Each operator's font for the admin pages, kept beside their light or dark
-- mode and colour scheme.
--
-- - `theme_font`: 'hyperlegible' (the default), 'system' or 'plex'.
--
-- No CHECK, as with the other two: the pages read an unknown value as the
-- default, so a font can be added or dropped without a migration. `IF NOT
-- EXISTS` lets a database whose migration record was removed to go back to
-- an earlier release, with the column left in place, upgrade again.
ALTER TABLE admin_operators
  ADD COLUMN IF NOT EXISTS theme_font text NOT NULL DEFAULT 'hyperlegible';
