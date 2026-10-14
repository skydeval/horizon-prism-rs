-- Operator settings (public-indexing design §3.2): values an operator sets
-- through a command or the file, and that a future admin UI lists. Worker
-- internals stay in worker_state. No row is seeded: startup writes the
-- first one (§3.3).
CREATE TABLE operator_settings (
    key        text        PRIMARY KEY,
    value      jsonb       NOT NULL,   -- the target value
    file_value jsonb,                  -- {"value", "explicit"} as of the last startup; NULL = not seen yet
    updated_at timestamptz NOT NULL DEFAULT now(),
    updated_by text        NOT NULL    -- startup | file | cli | upgrade | admin_ui
);
