# Indexing and scope

## Purpose

Horizon-Prism indexes a community, not the whole network: its
[members](membership.md) and the accounts around them, called **scope**.

Separately, **public indexing** decides whether it also serves public reads
for its members, or only [kryphocron](kryphocron.md), Thread Lens and Horizon's
features.

## Model

### Who is in scope

Members are always in scope. The policy (`scope.policy`) decides who else is:

| Policy | In scope besides members |
|---|---|
| `members_only` | nobody |
| `members_and_follows` | accounts a member follows |
| `members_and_interactions` | accounts a member recently replied to, quoted, liked or reposted |
| `members_and_one_hop` (default) | both of the above |

- **Follows** hold an account for as long as the follow exists. Each member
  counts only toward their earliest `scope.max_follow_scope_per_member` follows
  (default 10 000).
- **Replies and quotes** hold the accounts they name for
  `scope.reply_quote_days` (default 90) after the member's most recent one.
- **Likes and reposts** hold for `scope.like_repost_days` (default 30).
- A window of 0 turns that hold off. Recency uses the record's own
  `createdAt`, so imported history does not pull old contacts into scope.
- Except under `members_only`, an active activity subscription (the member
  still follows the account) keeps its subject in scope; it never adds one.
- Kryphocron records never add an account to scope.

Records involving a member are kept even when their author is outside scope:
replies to and quotes of a member's posts, likes and reposts of them, follows
of a member, and blocks with a member on either side (see
[blocks-and-moderation.md](blocks-and-moderation.md)).

### The nightly scope check

Accounts enter scope as soon as a member's write names them, and leave through
a nightly check (default 03:00 UTC, an hour before retention). It:

1. recomputes who is held, using the windows above;
2. removes each account that nothing holds and that has been unheld for longer
   than `scope.rebuild_grace_days` (default 0, so the next night);
3. removes at most `scope.sweep_max_purges` accounts per night (default
   5 000); the rest go on later nights.

An account a member interacts with again just before removal is kept.
`admin scope rebuild --dry-run` reports what the check would remove.

### Public indexing

`indexing.public_scope` has two working values:

| Value | What Horizon-Prism stores and serves |
|---|---|
| `kryphocron_only` (default for a fresh install) | Only what kryphocron, Thread Lens and Horizon features need. Public `app.bsky.*` reads answer `501 NotImplemented`. |
| `community` | Everything in scope. Horizon-Prism also serves public reads for its members. |
| `full` | The public network as well, for the record groups `[full] kinds` lists (all by default), with each account's history crawled. See [Full network](#full-network) below. |

In `kryphocron_only`, Horizon-Prism keeps:
- every stored block, and follows with a member at either end;
- members' own likes, reposts, lists, list items, list blocks and opt-outs;
- profiles and identity of accounts in scope;
- labels, tombstones, and members' mutes, bookmarks and drafts;
- every kryphocron record;
- for each member's post, only whom it replies to and quotes (no text).

It drops posts, feeds, notifications, other accounts' likes, reposts, follows
and lists, gates, starter packs, polls, labeler services, and the count and
trend tables.

The endpoints kept in `kryphocron_only` are the member's own mutes and blocks, drafts,
labels, handle resolution, and the `app.nearhorizon.*` methods.
[api.md](../api.md) lists each method.

### Changing it

Run `admin indexing public-scope kryphocron_only|community|full`, use the switch
on the admin UI's Indexing page, or set the key in `app.toml` and restart.
`admin indexing status` shows the current phase and progress.

- **`community` → `kryphocron_only`** deletes the dropped data in the background, table by table, in
  small batches. Public endpoints answer 501 at once.
- **`kryphocron_only` → `community`** refills each account in scope to backfill depth (below), with no
  old notifications. Public endpoints answer 501 ("being turned on") until the
  refill ends, at most `indexing.fill_max_wait_hours` (default 24). A refill
  waits for a running deletion to end.
- **An upgrade never switches it to `kryphocron_only`.** A deployment holding public data stays
  `community`; the `app.toml` key applies only when it changed since the
  last start.

- **To `full`** needs the typed word `FULL`. Its confirm page and the CLI show
  the free disk and how long it lasts at full's growth, the storage budget,
  the writer lanes against the relays' rate, the last `admin indexing
  full-check`, and how labels arrive. Nothing refuses it. From
  `kryphocron_only`, S is refilled first, as for `community`.
- **`full` → `community`** needs the typed word `COMMUNITY`. Rows full added
  are deleted in the background, then S is refilled; public requests answer
  501 during that refill, for up to `indexing.fill_max_wait_hours`.
- **`full` → `kryphocron_only`** deletes what both added.

Take a `pg_dump` before switching to `kryphocron_only`: deleted data does not come back in
full. A trial of `full` that removes what it added is in
[operations.md](../operations.md#trying-full).

## Full network

With `indexing.public_scope = "full"`, Horizon-Prism stores the public records
of every account for the record groups in `[full] kinds`, from the relays and
from a crawl of each account's history. Kryphocron is unchanged: it never
widens beyond members.

### Record groups

| Group | Records |
|---|---|
| `posts` | posts, threadgates, postgates |
| `likes` | likes |
| `reposts` | reposts |
| `follows` | follows |
| `blocks` | blocks and list blocks |
| `lists` | lists, list items, list opt-outs |
| `profiles` | profiles and other per-account records |
| `starter_packs` | starter packs |
| `polls` | Horizon polls |

A group left out of `kinds` follows `community`'s rules. Changing `kinds`
takes effect at the next start: an added group's history is crawled, a removed
group's rows of accounts outside scope are deleted.

### Where reads come from

- **An account's own records** are read locally once its history for that
  group is crawled. Until then, and for an account the crawl could not finish,
  they come from where `community` reads them.
- **Interaction data** (counts, likers, reposters, quotes, replies, followers,
  known followers, blocks by list) is read locally once a group's whole
  history is complete, and its counts once they were checked after that.
  Until then it comes from Constellation, the AppViews and Farsight, as in
  `community`. `[full] counts = "constellation"` keeps counts on Constellation.
- **A gap in the stream** (a trim, a lost relay range) sends both back to
  those sources until the relays' listings are compared again.
- With a history cutoff (`[full_crawl] skip_older_than_days` or
  `retention.max_age_days`), interaction data stays on those sources.

### Relays

Full takes public records only from relays whose filter passes every
collection it stores. A relay whose filter leaves one out is set aside for
full, and Health names it. A relay whose reader sends no capability stamp
still works, with less per-account continuity: the periodic re-list
(`[full] relist_every_days`) finds what it missed.

### Blocks

`[full] blocks_source = "local"` (default) stores every account's blocks and
list blocks; it needs the `blocks` and `lists` groups. `"farsight"` keeps
only blocks with a member side and asks Farsight for the rest.

### Suspended accounts

`[full] takedown_relays` (default `bsky`) are the relays whose account
statuses count as a suspension.

- `suspended_accounts = "refract_only"` (default): an account such a relay
  reports suspended or taken down is hidden and not crawled, unless
  [Refract](refract.md) keeps it. After `suspended_purge_days` (default 30) its
  own public records are deleted; records involving members stay.
- `"all"`: such accounts are served while their own server serves them.
- A relay's `deactivated` or `deleted` hides the account at once; its own
  server's answer confirms or undoes it.
- Members are never hidden by a relay's status.

### Labels

`[full] labels = "subscribe"` (default) subscribes to the labelers in
`label_sources` (default Bluesky's moderation labeler) and stores their
labels. A request gets the labels of the labelers it accepts.
`label_backfill` (default off) replays each labeler's history first; for
Bluesky's labeler that is hours and several GB. Off, labels of older posts
and of accounts are read from the labeler per request. `"upstream"` reads
every labeler per request.

### History crawl

`[full_crawl]` lists every account the relays carry and fetches each
repository, verified against the account's key, at `repos_per_sec`. Members
come first, then S, then accounts members read, then the rest. The crawl
pauses when the database volume's free space falls under `[full]
pause_free_percent`; live ingest goes on. With `enabled = false`, full stores
the network as it happens and serves like `community` for older records.

`admin indexing full-complete --incomplete [group]` marks a group's history
complete by hand; the accounts not crawled yet keep reading the other sources.

## What is stored, and why

| Data | Why |
|---|---|
| Members' records, in full depth | Their own views, their kryphocron posts and audiences, readiness |
| Profiles and identity of accounts in scope | Author views on posts members see |
| Posts, likes, reposts of accounts in scope (`community` only) | Public timelines, threads and counts |
| Blocks with a member side | Block enforcement in every read |
| Kryphocron posts and audiences | [kryphocron.md](kryphocron.md) |
| Refract accounts' records | [refract.md](refract.md) |
| Tombstones of deleted records | So a late copy of a deleted record does not come back |

## Retention

A daily job (`retention.run_at_utc`, default 04:00 UTC) deletes, in small
batches:

| What | When |
|---|---|
| Public rows of an account that left scope | The next run after it left, except rows that involve a member (above) |
| Tombstones | After `retention.tombstone_days` (default 7) |
| Notifications | After `retention.notification_days` (default 90) |
| Posts, likes, reposts and feed items older than `retention.max_age_days` | Only when set (default 0, off), and only for accounts that are neither members nor followed by a member. Follows and blocks are never age-capped. It waits for the indexes it reads, which are built in the background after an upgrade. |

In `full`, the groups in `[full] kinds` are not deleted when an account
leaves scope; the other groups follow the rules above.

When an account leaves scope, its kryphocron posts and audiences are removed
at once, except its replies in members' threads.

## Backfill depth

When an account enters scope, Horizon-Prism reads its repository from its PDS once:

| Account | What is read |
|---|---|
| New member | Profile; the most recent 1 000 posts, reposts and likes; up to 10 000 follows; all lists, list items, list blocks and opt-outs; and, in full, the records that decide kryphocron access (blocks, audiences, kryphocron posts) |
| Other account in scope, public indexing on | Profile; its most recent 100 posts and reposts, with their gates |
| Other account in scope, public indexing off | Profile only |

After that, Horizon-Prism follows the account through the firehose (see
[firehose-and-relays.md](firehose-and-relays.md)).

## Guarantees

- Outside `full`, no records of an account outside scope are kept, except
  records that involve a member and Refract's accounts.
- An unfollow or an expired interaction removes the account within about a
  day (plus the grace, if set).
- Members, and records that involve them, are never removed by the scope check
  or the age cap.
- Only the operator turns public indexing off.

## Limits

- Recent history only; older public posts are not fetched.
- An account a member likes about once a month may leave and re-enter scope,
  with a fresh backfill each time.
- Deleted data stays in Postgres's write-ahead log and in backups until those
  age out.
