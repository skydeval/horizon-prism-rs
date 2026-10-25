-- System accounts, amendment 1 (start without the key): account events seen
-- while the feature is locked (lists stored, key missing or wrong), applied
-- when the right key is back. Every identity, account, handle and tombstone
-- event is kept, for any DID, and nothing here marks which of them are on a
-- list: the catch-up works that out in memory. Only src/system_accounts/
-- names this table (the Dockerfile's source check).
CREATE TABLE sysacct_pending (
    did text PRIMARY KEY,
    -- true once Prism's account_deleted purge ran for it while locked
    confirmed boolean NOT NULL DEFAULT false,
    -- the latest event; a catch-up pass deletes exactly the rows it read,
    -- so a later event keeps the row for the next pass
    seen_at timestamptz NOT NULL DEFAULT now()
);

-- When the identity refresher last ran a check on the row to an answer and
-- put it back for a recheck. A claim or a retry after an error never sets
-- it, so the system-accounts catch-up can tell a check that ran from one in
-- progress or failed.
ALTER TABLE identity_refresh ADD COLUMN settled_at timestamptz;
