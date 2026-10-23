-- Public root gates (public-root-gates design §3, §4): a public thread
-- root's stock `app.bsky.feed.threadgate`, written by the root's author,
-- is kept in `private_threadgates` while a private post claims the root,
-- so that its `hiddenReplies` hide those private replies.
--
-- - `private_posts_root`: the "a private post claims this root" test of
--   the copy's write, and the fetch queue's.
-- - `private_root_gate_fetches`: one fetch of a root's gate from its
--   author's PDS, queued when a private reply is the root's only stored one
--   and no copy of its gate is stored (a gate written before then was not
--   kept). Seeded here with every public root already claimed, so the
--   worker fetches their gates after the deploy.
--
-- New objects only: no existing row is touched.
CREATE INDEX private_posts_root ON private_posts (reply_root_uri);

CREATE TABLE private_root_gate_fetches (
  root_uri   text PRIMARY KEY,
  queued_at  timestamptz NOT NULL DEFAULT now(),
  next_at    timestamptz NOT NULL DEFAULT now(),
  attempts   integer NOT NULL DEFAULT 0,
  done_at    timestamptz,
  last_error text
);
CREATE INDEX private_root_gate_fetches_due ON private_root_gate_fetches (next_at) WHERE done_at IS NULL;

INSERT INTO private_root_gate_fetches (root_uri)
SELECT DISTINCT reply_root_uri FROM private_posts
WHERE split_part(reply_root_uri, '/', 4) = 'app.bsky.feed.post'
  AND split_part(reply_root_uri, '/', 3) LIKE 'did:%'
ON CONFLICT DO NOTHING;
