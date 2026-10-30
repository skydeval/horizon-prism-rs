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
| `off` (default for a fresh install) | Only what kryphocron, Thread Lens and Horizon features need. Public `app.bsky.*` reads answer `501 NotImplemented`. |
| `community_scoped` | Everything in scope. Horizon-Prism also serves public reads for its members. |
| `full` | Not built yet: refused at startup. |

With it **off**, Horizon-Prism keeps:
- every stored block, and follows with a member at either end;
- members' own likes, reposts, lists, list items, list blocks and opt-outs;
- profiles and identity of accounts in scope;
- labels, tombstones, and members' mutes, bookmarks and drafts;
- every kryphocron record;
- for each member's post, only whom it replies to and quotes (no text).

It drops posts, feeds, notifications, other accounts' likes, reposts, follows
and lists, gates, starter packs, polls, labeler services, and the count and
trend tables.

The endpoints kept in `off` are the member's own mutes and blocks, drafts,
labels, handle resolution, and the `app.nearhorizon.*` methods.
[api.md](../api.md) lists each method.

### Changing it

Run `admin indexing public-scope off|community_scoped`, use the switch on the
admin UI's storage settings page, or set the key in `app.toml` and restart.
`admin indexing status` shows the current phase and progress.

- **On → off** deletes the dropped data in the background, table by table, in
  small batches. Public endpoints answer 501 at once.
- **Off → on** refills each account in scope to backfill depth (below), with no
  old notifications. Public endpoints answer 501 ("being turned on") until the
  refill ends, at most `indexing.fill_max_wait_hours` (default 24). A refill
  waits for a running deletion to end.
- **An upgrade never turns it off.** A deployment holding public data stays
  `community_scoped`; the `app.toml` key applies only when it changed since the
  last start.

Take a `pg_dump` before turning it off: deleted data does not come back in
full.

## What is stored, and why

| Data | Why |
|---|---|
| Members' records, in full depth | Their own views, their kryphocron posts and audiences, readiness |
| Profiles and identity of accounts in scope | Author views on posts members see |
| Posts, likes, reposts of accounts in scope (`community_scoped` only) | Public timelines, threads and counts |
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
| Posts, likes, reposts and feed items older than `retention.max_age_days` | Only when set (default 0, off), and only for accounts that are neither members nor followed by a member. Follows and blocks are never age-capped. |

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

- No records of an account outside scope are kept, except records that involve
  a member and Refract's accounts.
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
