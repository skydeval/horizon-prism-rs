-- The public page's self-check (public-page design §7.2): its OAuth flows
-- share admin_oauth_states under purpose 'check', bound to the browser that
-- started them by the BLAKE3 hash of its pre-session cookie. Rows still
-- live at most 10 minutes (retention T).
ALTER TABLE admin_oauth_states DROP CONSTRAINT admin_oauth_states_purpose_check;
ALTER TABLE admin_oauth_states ADD CONSTRAINT admin_oauth_states_purpose_check CHECK (purpose IN ('login', 'setup', 'check'));
ALTER TABLE admin_oauth_states ADD COLUMN browser_hash bytea;
