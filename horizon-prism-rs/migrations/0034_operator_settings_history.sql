-- Settings saved in the admin UI (admin-ui design §8.2; reserved there as
-- 0033, which scope recency took first). Each saved setting is a row of
-- operator_settings, layered over app.toml at startup. `version` guards a
-- change made from a page that is out of date; `set_aside` says the value
-- could not be used at the last start, and why; `file_notice` marks a
-- setting whose app.toml value changed under a saved one, until the
-- operator keeps it or takes the file's. Every change is kept forever in
-- operator_settings_history.
ALTER TABLE operator_settings
    ADD COLUMN version     bigint  NOT NULL DEFAULT 1,
    ADD COLUMN set_aside   jsonb,
    ADD COLUMN file_notice boolean NOT NULL DEFAULT false;

CREATE TABLE operator_settings_history (
    id         bigserial   PRIMARY KEY,
    key        text        NOT NULL,
    old_value  jsonb,                  -- NULL: there was no saved value
    new_value  jsonb,                  -- NULL: the saved value was removed
    changed_by text        NOT NULL,   -- operator DID, 'cli', 'file' or 'startup'
    changed_at timestamptz NOT NULL DEFAULT now(),
    command_id bigint                  -- the admin_commands row, when there was one
);
CREATE INDEX operator_settings_history_key ON operator_settings_history (key, changed_at DESC);
