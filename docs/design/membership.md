# Membership

## Purpose

A Horizon-Prism instance serves a community. Membership says who belongs to it. Only
members can read private posts, own private posts that are served, and get
Horizon-Prism's full index of their own account. Horizon-Prism decides membership itself and
enforces it on every read. No PDS is asked.

## Model

### What a member is

A member is an account (a DID) that the operator has admitted. That is the
only membership fact. Where the account is hosted neither grants membership
nor is required for it: members can be on any PDS.

### How an account becomes a member

| Path | Who | Notes |
|---|---|---|
| Operator add | Any account on any PDS | `admin member add <did>`, or the admin UI's members page |
| Auto-enroll (optional, off by default) | Accounts on PDS hosts the operator names | Turned on with `community.auto_enroll = true` (or "Auto-join" in the admin UI) |

With auto-enroll on, an account becomes a member the first time a client
calls `getTimeline`, `listNotifications`, `getUnreadCount` or
`app.nearhorizon.actor.getMembership` for it, if all of these hold:
- it is not denied;
- its PDS host matches `community.enroll_pds_patterns` (empty means the
  community network's hosts);
- its PDS reports the account active;
- the instance is below `community.max_members` (default 1 000);
- fewer than `community.max_auto_enroll_per_hour` (default 50) accounts were
  auto-enrolled in the last hour.

A refusal is remembered for `community.enroll_refusal_cache_secs` (default
3 600).

### Deny and allow

- `admin member deny <did> [--reason <text>]` keeps an account out. A denied
  account is never auto-enrolled, and `member add` refuses it. If it is a
  member, it is removed in the same step. The reason is a note for operators
  and is never shown to the account.
- `admin member allow <did>` lifts the denial. It does not add the account; run
  `member add` afterwards.
- A denial is per DID, so moving to another PDS does not escape it.

### Readiness

After an account is added, Horizon-Prism reads its repository from its PDS (see
[indexing-and-scope.md](indexing-and-scope.md)). The member is **ready** once
every record that decides private access (blocks, audiences, private posts) has
been read in full.

| State | Member-only endpoints answer |
|---|---|
| Not ready yet | `400 NotReady`, with `Retry-After` (`community.not_ready_retry_secs`) |
| Ready | normally |
| Ready before, then indexing interrupted (a relay gap, a repository that diverged) | `503 Reindexing`, with `retryAfter` (`community.reindex_retry_secs`) until it catches up |

A member's own settings (mutes, private appearance) work before readiness.
[degraded-mode.md](degraded-mode.md) covers interruptions.

An **auto-enrolled** member that is still not ready
`community.never_ready_expiry_days` (default 7; 0 = never) after being added
is removed, checked hourly. Members the operator added never expire this way.
Expiry is not a denial: the account may enroll again.

### Removal

`admin member remove <did>`, or a deny, removes a member. In the background
Horizon-Prism then deletes the account's state on Horizon-Prism: mutes, list and thread
mutes, bookmarks, drafts, activity subscriptions and notification settings.

The account's private posts stay stored but are no longer served to anyone.
Replies by members under its audiences are not served either. If it is added
again, all of this is served again once it is ready.

Members cannot remove themselves through Horizon-Prism. They ask the operator.

### `app.nearhorizon.actor.getMembership`

The caller's own membership. Authentication is required; there is no member
gate. Clients call it to decide what to show.

| `status` | Meaning |
|---|---|
| `member` | A ready member |
| `indexing` | A member whose indexing has not finished, or an account just enrolled |
| `reindexing` | A member whose indexing was interrupted after it was ready |
| `notEligible` | Not a member, and not enrolled by this call |

The answer also carries:
- `permissions`: `privateRead` (private reads serve the caller now),
  `privateIndexed` (private posts the caller writes are indexed),
  `threadLens` (Thread Lens serves the caller);
- `features`: the feature ids this instance serves, listed in
  [api.md](../api.md);
- `validForSecs`: how long the client may act on the answer. For `member` it
  is `private_read.membership_valid_for_secs` (default 3 600). For `indexing`
  and `reindexing` it is the retry hint. For `notEligible` it is never longer
  than a refusal is remembered.

The answer is never cached by intermediaries. Each read is still authorized on
its own; `validForSecs` only saves the client a call. The auto-enroll hourly
cap answers `429` with `Retry-After`.

## Guarantees

- Only members own private posts that are served. A non-member's private
  posts may be stored where scope admits them, but no reader is served them
  ([private-posts.md](private-posts.md)).
- Only ready members can read private posts.
- An account is never both a member and denied.
- Hosting never makes an account a member unless the operator turned
  auto-enroll on for that host.
- A removed or denied account loses member access as soon as the command
  runs. Its private posts stop being served when the background removal ends.

## What non-members can do

- Read public endpoints, anonymously or signed in, while public indexing is
  on.
- Use Thread Lens as a visitor when `threadlens.access = "signed_in"` (the
  default); with `members`, Thread Lens serves ready members only.
- Call `getMembership`.
- Ask Horizon-Prism to index their own record with `app.nearhorizon.index.requestRecord`;
  it is stored only if scope admits it.
- Interact with members: their replies, likes, follows and blocks that involve
  a member are indexed.

They cannot read private posts, even when an audience names them.

## Limits

- A member's PDS must be crawled by at least one relay Horizon-Prism reads. Otherwise
  their commits do not reach Horizon-Prism, Horizon-Prism falls behind on their account after
  each commit, and their private reads miss or answer `Reindexing` until it
  catches up. See
  [firehose-and-relays.md](firehose-and-relays.md).
- Members cannot invite others. Only the operator admits accounts.
- Horizon-Prism does not detect spam accounts. With auto-enroll on for a host with
  open signup, the caps and denials are the only controls.
- Horizon writes private posts to the member's own PDS; Horizon-Prism never takes
  writes.
