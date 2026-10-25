-- System accounts (system-accounts design §4.1): a private, per-account list
-- of the other accounts of a plural system. Only src/system_accounts/ names
-- these tables (the Dockerfile's source check).
--
-- The owner's DID is plain. A listed account's DID is never stored or bound
-- in clear: member_index is a keyed hash of it, member_sealed the DID sealed
-- with a server key (APP_SYSTEM_ACCOUNTS_KEY_HEX), bound to the owner and the
-- index.

-- One row per list: the monotonic rev, kept while the owner exists so it
-- never goes back, even across an emptied list.
CREATE TABLE sysacct_lists (
    owner_did text PRIMARY KEY,
    rev bigint NOT NULL DEFAULT 0
);

CREATE TABLE sysacct_entries (
    owner_did text NOT NULL REFERENCES sysacct_lists (owner_did) ON DELETE CASCADE,
    member_index bytea NOT NULL,
    member_sealed bytea NOT NULL,
    added_at timestamptz NOT NULL,
    PRIMARY KEY (owner_did, member_index)
);
CREATE INDEX sysacct_entries_member ON sysacct_entries (member_index);

-- The once-only rule for the sensitive tokens: a keyed hash of each token
-- used by a committed write, until it could no longer verify.
CREATE TABLE sysacct_used_tokens (
    token_hash bytea PRIMARY KEY,
    expires_at timestamptz NOT NULL
);
CREATE INDEX sysacct_used_tokens_expiry ON sysacct_used_tokens (expires_at);

-- A value derived from the key, so a start with another key is caught
-- before anything sealed is misread.
CREATE TABLE sysacct_key (
    id boolean PRIMARY KEY DEFAULT true CHECK (id),
    key_check bytea NOT NULL
);
