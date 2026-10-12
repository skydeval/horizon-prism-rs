-- Drafts (design-v3 §3.1). AppView-private: not a repo record.
CREATE TABLE drafts (
  owner_did   text NOT NULL,
  id          text COLLATE "C" NOT NULL,       -- TID minted by the AppView
  draft       jsonb NOT NULL,                  -- app.bsky.draft.defs#draft, validated
  created_at  timestamptz NOT NULL DEFAULT now(),
  updated_at  timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (owner_did, id)
);
CREATE INDEX drafts_owner ON drafts (owner_did, updated_at DESC, id);
