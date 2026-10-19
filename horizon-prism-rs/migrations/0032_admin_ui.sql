-- The web admin UI, slice S1 (design admin-ui.md §5, §12; #245, #735).
--
-- - `admin_operators`, `admin_operator_hosts`: who may sign in, and from
--   which PDS hosts. Written only by first-run setup and the operator
--   commands (C-A2: no config key seeds them). An empty host list allows
--   any host (C-A3); the operator list still gates sign-in.
-- - `admin_setup_code`: the hash of the one-time setup code, while no
--   operator exists. The code itself is never stored here.
-- - `admin_sessions`, `admin_oauth_states`, `admin_logins`: sign-in.
-- - `admin_commands.requested_by`: the operator DID behind a command made
--   in the UI; NULL for the CLI.
CREATE TABLE admin_operators (
  did            text PRIMARY KEY,
  added_by       text NOT NULL,          -- 'setup', 'cli', or an operator DID
  added_at       timestamptz NOT NULL DEFAULT now(),
  note           text,                   -- the operator's own label
  pds_endpoint   text,                   -- last PDS endpoint seen in a fresh resolution
  pds_checked_at timestamptz
);                                       -- retention P
CREATE TABLE admin_operator_hosts (
  pattern    text PRIMARY KEY,           -- exact host or *.suffix, lowercased
  added_by   text NOT NULL,
  added_at   timestamptz NOT NULL DEFAULT now()
);                                       -- retention P
CREATE TABLE admin_setup_code (
  id         smallint PRIMARY KEY CHECK (id = 1),
  code_hash  bytea NOT NULL,             -- BLAKE3 of the normalized code
  created_at timestamptz NOT NULL DEFAULT now(),
  expires_at timestamptz NOT NULL
);                                       -- deleted when the first operator is added
CREATE TABLE admin_sessions (
  id_hash      bytea PRIMARY KEY,        -- BLAKE3 of the cookie's 32 bytes
  did          text NOT NULL,
  pds_endpoint text NOT NULL,            -- normalized; any change ends the session
  created_at   timestamptz NOT NULL DEFAULT now(),
  last_seen_at timestamptz NOT NULL DEFAULT now(),
  expires_at   timestamptz NOT NULL,     -- created_at + admin.session_max_secs
  checked_at   timestamptz NOT NULL DEFAULT now()
);                                       -- retention T (expired rows)
CREATE INDEX admin_sessions_did ON admin_sessions (did);
CREATE TABLE admin_oauth_states (
  state           text PRIMARY KEY,
  did             text NOT NULL,
  issuer          text NOT NULL,
  verifier        text NOT NULL,
  dpop_jwk        jsonb NOT NULL,
  purpose         text NOT NULL DEFAULT 'login' CHECK (purpose IN ('login', 'setup')),
  setup_code_hash bytea,                 -- purpose 'setup': the code checked at the start
  pds_endpoint    text NOT NULL,         -- the endpoint the flow started from
  created_at      timestamptz NOT NULL DEFAULT now()
);                                       -- deleted on use; retention T (10 minutes)
CREATE TABLE admin_logins (
  id        bigserial PRIMARY KEY,
  did       text,
  at        timestamptz NOT NULL DEFAULT now(),
  outcome   text NOT NULL,               -- ok, setup, not_operator, host_not_allowed, oauth_failed, session_cut:<reason>
  client_ip text
);                                       -- retention T (90 days)
CREATE INDEX admin_logins_at ON admin_logins (at);
ALTER TABLE admin_commands ADD COLUMN requested_by text;   -- NULL = CLI
