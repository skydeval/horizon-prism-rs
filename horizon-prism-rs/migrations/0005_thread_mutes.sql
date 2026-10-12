-- Thread mutes (design-v3 §3.1). AppView-private.
CREATE TABLE thread_mutes (
  viewer_did text NOT NULL,
  root_uri   text NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (viewer_did, root_uri)
);
