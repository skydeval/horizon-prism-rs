# Kryphocron

Horizon-Prism serves [kryphocron](https://github.com/skydeval/kryphocron) posts: posts their author shows to a chosen audience. Methods are under `app.nearhorizon.private.*`. Readers are ready members; see [membership](membership.md).

## Records

| Record | NSID | Holds |
|---|---|---|
| Post | `tools.kryphocron.feed.postPrivate` | Content, reply refs, the audience it names, index fields |
| Audience | `tools.kryphocron.policy.audience` | `mode` and, for lists, `members` |

The client writes both to the author's PDS. Horizon-Prism has no write method; records arrive through the relays. After a write, a client can call `app.nearhorizon.index.requestRecord`; once it answers `caughtUp: true`, the post reads at once. Content is decoded only inside an allowed read and never stored decoded.

Only members' posts are served, replies included. Members can be on any PDS.

## Audiences

| Mode | Readers |
|---|---|
| `list` (or none) | The DIDs in `members` |
| `followers` | Members who follow the owner |
| `following` | Members the owner follows |
| `everyone` | Every ready member |

Any other mode admits no one. The owner always reads their own. `following` is owner-only until the owner's follows have been checked against their repository, and stays owner-only above `[backfill] follows_pass_ceiling` follows.

## Who reads a post

- The author, unless moderation hides it.
- A top-level post: its audience.
- A reply: the audience of its thread root now. A kryphocron reply under a public post starts a branch read by the branch root's audience.
- Every writer in the chain up to the thread owner must still qualify under that audience. A writer who stops qualifying is hidden, with what is beneath them. Chains are walked up to `[private_read] max_chain` posts (default 16); deeper replies are author-only.
- Blocks apply both ways between reader and author, reader and thread owner, and writer and thread owner: `app.bsky.graph.block` records and synced moderation-list blocks.
- `!takedown` and `!hide` hide a post from everyone.
- Stock threadgates and postgates apply inside the audience. A public root's threadgate hides the kryphocron replies it lists.

The author's own view carries `notServed`, with reasons, when no reader would be served the post.

## Interactions and notifications

Likes, reposts, quotes and public replies are stock `app.bsky.*` records. Counts include only what the reader may see. Bookmarks are private to the member.

Notification reasons: `reply`, `mention`, `quote`, `like`, `repost`, `subscribed-post`. The recipient must be a member who can read the post; blocks and mutes apply. Members set preferences per reason. Writes that are not live creates notify only for records under `[private_notify] backfill_max_age_hours` (default 48). Rows are kept 90 days.

Members can subscribe to an account's new kryphocron posts (`putActivitySubscription`, `listActivitySubscriptions`), up to `[private_notify] activity_subscriptions_max` accounts (default 1000).

## Other methods

- **Appearance:** `getAppearance` and `putAppearance` store a member's appearance choice (`private`, `public` or `device`, at most 16 KiB), readable only by its owner. Kept 7 days after a member is removed (`releaseAppearance` deletes it early).
- **Search:** `searchPosts` scans readable posts at query time; there is no index of decoded text. Bounded by `[private_search]` `scan_candidates`, `span_hours` and `max_age_days`. A page can be empty and still carry a cursor. `sort: "top"` answers in latest order.
- **Media:** video plays from the blob on the author's PDS, never Bluesky's video service. The author feed takes `posts_with_media` and `posts_with_video`.

## Any PDS

With `[private_any_pds] enabled = true` (the default), posts from any PDS are served and `getMembership` lists `private.anyPds`. A post without encoded content is served to no one. Index fields (`quote`, `mentionDids`, `mediaKinds`) are checked against the content at read; a post that over-claims is author-only.

## Guarantees

- A post the reader may not read leaves no trace in any answer: views, threads, feeds, counts, notifications or search.
- Quotes, repost subjects, bookmarks, notification subjects, replies and search hits each pass the read rule.
- Reads fail closed: reader, author and thread owner must be ready members and the relays fresh, or the read misses or answers degraded; see [degraded mode](degraded-mode.md).
- Reads use only Horizon-Prism's own rows, call no outside service and write no shared cache.
- Membership, audiences and blocks are checked on every request.

## Limits

- Response time can show that a post exists. Per-endpoint latency floors (`[private_read.floor_ms]`) are off by default.
- `getUnreadCount` and `quoteCount` can count a post that later fails its read check.
- Paged reads scan at most twice their `limit`, so a page can be short; keep paging while a cursor comes back.
- Accounts that are not members of this AppView see nothing.
