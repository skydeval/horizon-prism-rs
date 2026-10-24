# Private posts

Methods under `app.nearhorizon.private.*`. Readers are ready members; see [membership](membership.md).

## Purpose

A private post is a post its author shows to a chosen audience instead of everyone. Horizon-Prism decides who may read it, and serves it, with its replies, likes, notifications and search, only to those readers.

A private post is a public post under another NSID. Its content is **encoded, not encrypted**. Encoding is friction: it keeps the text out of stock apps and casual reads. Anyone who holds the record can decode it. Do not post anything that must stay secret.

## Model

### Records and the write path

| Record | NSID | Holds |
|---|---|---|
| Private post | `tools.kryphocron.feed.postPrivate` | Encoded content, reply refs, the audience it names, clear index fields |
| Audience | `tools.kryphocron.policy.audience` | `mode` and, for lists, `members` |

The client writes both to the author's own PDS. Horizon encodes on the client with kryphocron. Horizon-Prism is never in the write path: it has no write or encode method. Records reach Horizon-Prism through the relays, like any other record. A client can call `app.nearhorizon.index.requestRecord` after a write; when it answers `caughtUp: true`, the author's private posts read at once.

Horizon-Prism stores the record as received and never decodes at ingest. It decodes only inside a read, for a reader the read rule has allowed. No decoded text is kept.

### Who owns private posts

Only members' private posts are served, replies included. An account that is not a member, or stops being one, has none of its private posts served. Members can be on any PDS (below).

### Audiences

A post names one audience record of its thread owner. The `mode` decides the readers:

| Mode | Readers |
|---|---|
| `list` (or no mode) | The DIDs in `members` |
| `followers` | Members who follow the audience's owner |
| `following` | Members the owner follows |
| `everyone` | Every ready member of this AppView |

Any other mode admits no one. The owner always reads their own audiences.

`following` is served only once Horizon-Prism has compared the owner's follows with the owner's repository. Until then, those posts are read by the owner alone. An owner with very many follows (over `[backfill] follows_pass_ceiling`) stays owner-only.

### The read rule

- **The author** always reads their own post, unless moderation hides it.
- **A top-level post** is read by its audience.
- **A reply** is read by the audience of its thread root as it is now. A private reply under a public post starts a branch, read by the audience of the branch root.
- **A reply's writer must stand** under that audience now, by the mode's rule (listed, follows the owner, and so on). So must every writer above it in the chain, back to the thread owner. A writer who stops standing is hidden, with what is beneath them.
- **Chains are walked** up to `[private_read] max_chain` posts (default 16). A deeper reply is read by its author only.
- **Blocks apply** both ways between the reader and the author, between the reader and the thread owner, and between a writer and the thread owner. A block is an `app.bsky.graph.block` record, or a member's list block of a moderation list that Horizon-Prism has synced. Private blocks are not used.
- **Moderation** (`!takedown`, `!hide`) hides a post from everyone, its author included.
- **Gates.** Stock threadgates and postgates on a private post apply, inside the audience. A public root's threadgate hides the private replies it lists.

The author's own view carries `notServed` when no reader of the audience would be served the post, with the reasons.

### Interactions

Likes, reposts, quotes and public replies are stock `app.bsky.*` records naming the private post's URI. Counts on a private view count only what the reader may see: replies and private quotes they may read, public quotes, and likes and reposts by members with no block either way. A member's repost of a private post shows in the private timeline and the reposter's private author feed, to readers of the post. Bookmarks of private posts are private to the member.

### Notifications

Reasons: `reply`, `mention`, `quote`, `like`, `repost` and `subscribed-post`. The recipient must be a member, and must be able to read the post for a reply, mention or quote. Blocks, mutes, list mutes and thread mutes apply. Each member sets private notification preferences per reason (`list`, and `include: all | follows`). Deleting the record removes its notification. Rows are kept 90 days.

**Backfill age.** A live create from the firehose notifies at any age. Every other write (backfill, catch-up, `requestRecord`, replay) and any live update notifies only for a record created within `[private_notify] backfill_max_age_hours` (default 48). Old likes are not announced as new, and a PDS rewriting its posts notifies no one again.

### Private activity subscriptions

A member can subscribe to an account's new private posts and replies, separately from the public bell (`putActivitySubscription`, `listActivitySubscriptions`). A new post notifies each subscriber who can read it, seconds after it is first stored. The public bell's rules also apply. A subscriber decided once is not decided again, so someone added to the audience later is not notified. Backfilled posts, and posts older than `[notifications] subscribed_post_max_age_secs` (one day), notify no one. A member may subscribe to at most `[private_notify] activity_subscriptions_max` accounts (default 1000).

### Appearance

`getAppearance` and `putAppearance` keep a member's choice of where its appearance (backdrop and presets, an `app.nearhorizon.actor.appearance` value, at most 16 KiB) is saved: `private`, `public` or `device`. For `private` and `public`, Horizon-Prism keeps the value. It is answered only to its owner.

When a member is removed, the value is **kept 7 days** for the account alone. `getAppearance` with `includeRetained` returns it with `retainedUntil`; `releaseAppearance` deletes it early. A new put as a member makes it live again. After 7 days it is deleted. Deleting the account deletes it at once.

### Private search

`searchPosts` scans, at query time, the private posts the reader may read. There is no index of decoded text. Each page reads at most `[private_search] scan_candidates` readable posts within `span_hours` below its cursor, and search reaches back `max_age_days`. A page can be empty and still carry a cursor; keep paging while one comes back. `sort: "top"` is answered in latest order.

### Media and video

A private post's video plays from the MP4 blob on the author's PDS, with no thumbnail, wherever it appears. It never depends on Bluesky's video service, whatever `[video] playback` says. A video or image whose author's PDS Horizon-Prism does not know gets no view. The private author feed takes `posts_with_media` and `posts_with_video`.

### Private posts from any PDS

While `[private_any_pds] enabled = true` (the default), private posts from any PDS are served, and `getMembership` lists `private.anyPds`. A private post with no encoded content (plaintext) is served to no reader, its author included. The row is kept, so turning the switch off (then restarting) serves it again.

A post's clear index fields (`quote`, `mentionDids`, `mediaKinds`) are checked against its content at read. A post whose fields claim more than its content carries is served to its author only. The author's own view lists `droppedFields` when part of the encoded content was left out of the read.

## Guarantees

- **Nothing beyond the audience.** A private post the reader may not read leaves no trace in any answer that a post that does not exist would not leave: not in views, threads, feeds, counts, likes, quotes, replies, notifications or search.
- **Every post is checked.** Quoted posts, repost subjects, bookmarks, notification subjects, replies and search hits each pass the read rule for this reader.
- **Fail closed.** A read is served only when the reader, the author and the thread owner are ready members, and the private relays are fresh. Otherwise it misses, or answers degraded with a retry hint; see [degraded mode](degraded-mode.md). An unknown mode, missing audience or failed check denies.
- **Isolated.** Private reads use only Horizon-Prism's own rows. They call no outside service and write no shared cache.
- **Checked live.** Membership, audience, standing and blocks are read on every request, not cached from the last answer.

## Limits

- **Encoding is not secrecy.** Anyone with the record, on any relay or PDS, can decode it. The audience rule decides what Horizon-Prism serves, nothing more.
- **Existence is not hidden.** Response time can show that a post exists. Optional per-endpoint latency floors (`[private_read.floor_ms]` in `app.toml`) are off by default.
- `getUnreadCount` and `quoteCount` can count a post that later fails to decode or over-claims its fields.
- Paged reads read at most twice their `limit` in candidates, so a page can be short; keep paging while a cursor comes back.
- Readers, authors and thread owners must be members of this AppView. Accounts elsewhere see nothing.
