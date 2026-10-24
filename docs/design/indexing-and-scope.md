# Indexing and scope

## Purpose

Horizon-Prism does not index the whole network. It indexes a community: its
[members](membership.md) and the accounts around them. This set of accounts is
called **scope**. Scope decides what Horizon-Prism stores, and what it can serve from
its own data.

Separately, the operator decides whether Horizon-Prism also acts as a public AppView
for its members (**public indexing**), or serves only the private tier, Thread
Lens and Horizon's features and leaves public reads to another AppView.

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
- A window of 0 turns that kind of hold off. Recency uses the record's own
  `createdAt`, so an imported history keeps its real dates and does not pull
  old contacts into scope.
- Under every policy except `members_only`, an active activity subscription
  (the member still follows the account) keeps its subject in scope. It never
  adds an account to scope.
- Private records never add an account to scope.

Some records are kept even when their author is outside scope, because they
involve a member: replies to and quotes of a member's posts, likes and reposts
of a member's posts, follows of a member, and blocks with a member on either
side. Blocks are stored only when a member is on one side (see
[blocks-and-moderation.md](blocks-and-moderation.md)).

### The nightly scope check

Accounts are added to scope as soon as a member's write names them. They are
removed by a nightly check, by default at 03:00 UTC, one hour before
retention. The check:

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
| `off` (default for a fresh install) | Only what the private tier, Thread Lens and Horizon features need. Public `app.bsky.*` reads answer `501 NotImplemented`. |
| `community_scoped` | Everything in scope. Horizon-Prism also serves public reads for its members. |
| `full` | Refused at startup. Horizon-Prism never indexes the whole network. |

With it **off**, Horizon-Prism keeps:
- every stored block, and follows with a member at either end;
- members' own likes, reposts, lists, list items, list blocks and opt-outs;
- profiles and identity of accounts in scope;
- labels, tombstones, and members' mutes, bookmarks and drafts;
- the whole private tier;
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
- **Off → on** refills each account in scope to backfill depth (below), not its
  full history, and brings back no old notifications. Public endpoints answer
  501 with a "being turned on" message until the refill ends, or for
  `indexing.fill_max_wait_hours` (default 24) at most. If you turn it on while
  a deletion is still running, the refill starts when the deletion ends.
- **An upgrade never turns it off.** A deployment that already holds public
  data stays `community_scoped` after an upgrade, whatever `app.toml` says. A
  key in `app.toml` is applied only when it changed since the last start.

Take a `pg_dump` before turning it off: data deleted this way does not come
back in full.

## What is stored, and why

| Data | Why |
|---|---|
| Members' records, in full depth | Their own views, their private posts and audiences, readiness |
| Profiles and identity of accounts in scope | Author views on posts members see |
| Posts, likes, reposts of accounts in scope (`community_scoped` only) | Public timelines, threads and counts |
| Blocks with a member side | Block enforcement in every read |
| Private posts and audiences | The private tier ([private-posts.md](private-posts.md)) |
| Restored accounts' records | Refract ([refract.md](refract.md)) |
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

When an account leaves scope, its own private posts and audiences are removed
at once, except its replies in members' threads.

## Backfill depth

When an account enters scope, Horizon-Prism reads its repository from its PDS once:

| Account | What is read |
|---|---|
| New member | Profile; the most recent 1 000 posts, reposts and likes; up to 10 000 follows; all lists, list items, list blocks and opt-outs; and, in full, the records that decide private access (blocks, audiences, private posts) |
| Other account in scope, public indexing on | Profile; its most recent 100 posts and reposts, with their gates |
| Other account in scope, public indexing off | Profile only |

After that, Horizon-Prism follows the account through the firehose (see
[firehose-and-relays.md](firehose-and-relays.md)).

## Guarantees

- Horizon-Prism keeps no records of an account outside scope, except records that
  involve a member and the records of Refract's restored accounts.
- An unfollow or an expired interaction removes the account within about a
  day (plus the grace, if set).
- Members, and records that involve them, are never removed by the scope check
  or the age cap.
- Turning public indexing off never happens on its own; only the operator does
  it.

## Limits

- Horizon-Prism holds recent history only. Older public posts of an account are not
  fetched.
- An account a member likes about once a month may leave and re-enter scope,
  with a fresh backfill each time.
- Deleted data stays in Postgres's write-ahead log and in backups until those
  age out.
