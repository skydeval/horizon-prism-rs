-- What the weekly scope rebuild needs of members' posts (public-indexing
-- design §4.1): who each post replies to and quotes, without its text,
-- embeds or record. Written for every member's post in every mode, so the
-- rebuild keeps finding members' replies and quotes when public indexing is
-- off and `posts` holds none.
CREATE TABLE member_post_refs (
    uri              text        PRIMARY KEY,
    author_did       text        NOT NULL,
    rev              text        COLLATE "C" NOT NULL,
    reply_parent_did text,
    reply_root_did   text,
    quote_did        text,
    created_at       timestamptz NOT NULL
);
CREATE INDEX member_post_refs_author ON member_post_refs (author_did);

-- The members' posts already stored. The DIDs are the authors of the
-- referenced at-uris, as the rebuild derived them from `posts`.
INSERT INTO member_post_refs (uri, author_did, rev, reply_parent_did, reply_root_did, quote_did, created_at)
SELECT p.uri, p.author_did, p.rev,
       CASE WHEN p.reply_parent_uri LIKE 'at://%' THEN NULLIF(split_part(p.reply_parent_uri, '/', 3), '') END,
       CASE WHEN p.reply_root_uri LIKE 'at://%' THEN NULLIF(split_part(p.reply_root_uri, '/', 3), '') END,
       CASE WHEN p.quote_uri LIKE 'at://%' THEN NULLIF(split_part(p.quote_uri, '/', 3), '') END,
       p.created_at
FROM posts p JOIN members m ON m.did = p.author_did;
