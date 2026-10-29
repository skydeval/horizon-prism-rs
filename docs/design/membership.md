# Membership

## Purpose

Membership says who belongs to the community. Only members read and own
served [kryphocron](kryphocron.md) posts, and get the full index of their own
account. Horizon-Prism decides membership itself and checks it on every read.

## Model

### What a member is

An account (a DID) the operator has admitted. Members can be on any PDS.

### How an account becomes a member

| Path | Who | Notes |
|---|---|---|
| Operator add | Any account on any PDS | `admin member add <did>`, or the admin UI's members page |
| Auto-enroll (optional, off by default) | Accounts on PDS hosts the operator names | `community.auto_enroll = true` ("Auto-join" in the admin UI) |

With auto-enroll on, an account becomes a member on its first `getTimeline`,
`listNotifications`, `getUnreadCount` or `app.nearhorizon.actor.getMembership`
call, if:
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

- `admin member deny <did> [--reason <text>]` keeps an account out and removes
  it if it is a member. The reason is for operators only.
- `admin member allow <did>` lifts the denial; then run `member add`.
- A denial is per DID, so moving PDS does not escape it.

### Readiness

After an add, Horizon-Prism reads the account's repository (see
[indexing-and-scope.md](indexing-and-scope.md)). The member is **ready** once
every record that decides kryphocron access (blocks, audiences, kryphocron
posts) has been read in full.

| State | Member-only endpoints answer |
|---|---|
| Not ready yet | `400 NotReady`, with `Retry-After` (`community.not_ready_retry_secs`) |
| Ready | normally |
| Ready, then indexing interrupted (a relay gap, a diverged repository) | `503 Reindexing`, with `retryAfter` (`community.reindex_retry_secs`) |

A member's own settings (mutes, appearance) work before readiness. See
[degraded-mode.md](degraded-mode.md) for interruptions.

An **auto-enrolled** member still not ready after
`community.never_ready_expiry_days` (default 7; 0 = never) is removed, checked
hourly. It may enroll again. Operator-added members never expire.

### Removal

`admin member remove <did>`, or a deny, removes a member. In the background
its mutes, list and thread mutes, bookmarks, drafts, activity subscriptions and
notification settings are deleted.

Its kryphocron posts stay stored but are served to no one, and neither are
members' replies under its audiences. Added again, all of it is served again
once it is ready.

Members cannot remove themselves; they ask the operator.

### `app.nearhorizon.actor.getMembership`

The caller's own membership. Signed in, no member gate.

| `status` | Meaning |
|---|---|
| `member` | A ready member |
| `indexing` | A member whose indexing has not finished, or an account just enrolled |
| `reindexing` | A member whose indexing was interrupted after it was ready |
| `notEligible` | Not a member, and not enrolled by this call |

The answer also carries:
- `permissions`: `privateRead` (kryphocron reads serve the caller now),
  `privateIndexed` (the caller's kryphocron posts are indexed), `threadLens`;
- `features`: the feature ids this instance serves ([api.md](../api.md));
- `validForSecs`: how long the client may act on the answer.
  `private_read.membership_valid_for_secs` (default 3 600) for `member`, the
  retry hint for `indexing` and `reindexing`, at most the refusal cache for
  `notEligible`.

The answer is never cached by intermediaries, and each read is still
authorized on its own. The auto-enroll hourly cap answers `429` with
`Retry-After`.

## Guarantees

- Only members own served kryphocron posts. A non-member's may be stored where
  scope admits them, but are served to no one.
- Only ready members read kryphocron posts.
- An account is never both a member and denied.
- Hosting never makes an account a member unless auto-enroll is on for that
  host.
- A removed or denied account loses member access at once; its kryphocron
  posts stop being served when the background removal ends.

## What non-members can do

- Read public endpoints while public indexing is on.
- Use Thread Lens when `threadlens.access = "signed_in"` (the default).
- Call `getMembership`.
- Ask to index their own record with `app.nearhorizon.index.requestRecord`;
  it is stored only if scope admits it.
- Interact with members: their replies, likes, follows and blocks involving a
  member are indexed.

They cannot read kryphocron posts, even when an audience names them.

## Limits

- A member's PDS must be crawled by at least one relay Horizon-Prism reads, or
  their kryphocron reads miss or answer `Reindexing` after each commit. See
  [firehose-and-relays.md](firehose-and-relays.md).
- Only the operator admits accounts.
- There is no spam detection. With auto-enroll on for an open-signup host, the
  caps and denials are the only controls.
- Clients write kryphocron posts to the member's own PDS; Horizon-Prism takes
  no writes.
