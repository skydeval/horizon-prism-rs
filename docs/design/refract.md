# Refract: restored accounts

Optional. Off by default. Methods under `app.nearhorizon.refract.*`.

## Purpose

Bluesky's AppView sometimes takes down an account that its own community still hosts and has not banned. Bluesky clients then show "Post unavailable", empty profiles and silent holes in threads.

Refract keeps such accounts readable through this AppView. The operator lists the communities whose decisions count. An account is **restored** when Bluesky withholds it, a listed community hosts it, and that community has not banned it. Restored accounts are served from what Horizon-Prism stores, and every view of them carries a `restored` marker.

## Model

### Communities

The operator lists communities in `[[refract.communities]]`, or on the admin UI's Communities page.

| Field | Meaning |
|---|---|
| `name` | Unique. Shown in the `restored` marker. |
| `pds_patterns` | PDS hosts the community's accounts live on, e.g. `["pds.example.org", "*.pds.example.org"]`. First match wins, in list order. |
| `tier` | `1`: the community runs an AppView. `2`: it runs a labeler. |
| `appview_network` | Tier 1: the `[[constellation.networks]]` entry for its AppView. |
| `labeler_did` | Its labeler. Required on tier 2, optional on tier 1. |
| `label_source` | `appview`: labels read through the community AppView (tier 1 only). `labeler`: the labeler is read directly. |
| `gone_labels` | Labels from that labeler that end a restore. Default `["!takedown", "!hide"]`. |
| `enabled` | Off: the entry restores nothing. |

An account qualifies only through the PDS named in its DID document. An AppView alone cannot make an account restored. Labels count only from the account's own community's labeler.

The shipped config has `refract.enabled = false` and one example tier 1 entry (its labeler read through its AppView). A tier 2 example is shown commented, with a placeholder labeler such as `did:plc:example`.

### Switching on and off

`refract.enabled` and the community list are live settings: a save in the admin UI (or `admin settings set`) applies at once. Other `[refract]` keys apply at restart.

When the feature is switched off, or an entry is removed, disabled or narrowed so an account no longer matches, the affected accounts are **hidden at once** and **ended after a delay** (`disable_delay_hours`, default 24). Switching back within the delay clears the hold and nothing is purged. The operator can `keep` a pending change to hold it indefinitely. The admin UI warns before a save that would make restored accounts pending. This stops a config mistake from turning into a mass purge.

### Finding and checking accounts

An account becomes a **candidate** only when:

- a client names it in `fill` (for example after Bluesky answered "not found"), or
- the operator adds it (`admin refract check`, or the admin UI's Refract accounts page).

Horizon-Prism's own upstream reads never create candidates. Discovery is capped per caller, per client address for visitors, and in total.

A checker then tests each rule:

1. Bluesky's AppView withholds the account (takedown or suspension).
2. Its PDS, from a fresh identity resolve, matches an enabled community.
3. Its PDS reports the repo `active`. `deactivated` gives a **grace** state instead.
4. The community keeps it: on tier 1 its AppView serves the profile; with a labeler, no unexpired gone label is on the account.
5. No local `!takedown`.

All pass: **restored**. Any fail: rejected (a candidate) or **ended** (a restored account). An answer Horizon-Prism cannot get (an outage, a timeout) keeps the last good state and is retried with backoff. Restored accounts are checked again every `recheck_hours` (default 24), and sooner on an identity or account event, a new gone label, or a change to their community entry.

A deactivated account is hidden at once and kept for `grace_days` (default 30). If it comes back, it is restored again. If not, it ends. An account Bluesky serves again also ends: Bluesky clients can then read it directly.

Record labels from the community hide single posts. They never end a restore.

### What is stored

Refract is a narrow exception to the public-indexing rules. It stores:

- the restored account's own records: posts, likes, reposts, follows, lists, list items, threadgates, postgates, profile; full history, backfilled from its PDS and kept current from the firehose, with a weekly resync;
- reposts, by anyone, of a restored account's public posts (only the repost record), so timelines and profiles can show them. Older reposts are filled once per restore from Constellation backlinks;
- notifications from restored authors to members;
- the community's labels on candidates and restored accounts, and per-post label reads;
- an audit trail of state changes, kept `event_days` (default 365).

Nothing else about anyone is stored because of Refract. Nothing is stored per visitor. Restored content counts toward `storage.budget_bytes` and is exempt from age-based retention.

When a restore ends, a separate purge removes what only Refract kept. Records other accounts wrote that point at the account (members' likes, follows, replies, quotes) are never purged.

### Visitors

Any signed-in caller may use Refract. A ready member gets its own viewer state, from its stored records. Anyone else is a **visitor**:

- `getTimeline`, `listNotifications` and `getUnreadCount` answer empty for non-members;
- the other methods serve visitors in full;
- each answer says `servedAs: member` or `visitor`.

**Visitor viewer state** (the visitor's own like, repost and follow on restored views) is optional and off by default (`refract.visitor_viewer_state`, live). Finding it means sending the reader's account, with each restored subject on screen, to the configured Constellation. Off, no reader account leaves Horizon-Prism and visitor answers carry no such state. On, `viewerChecked` lists which subjects were answered; nothing is stored or cached.

### Blocks for visitors

A visitor has no block records in Horizon-Prism, so blocks between the visitor and each author come from Farsight. A pair Farsight does not answer within the request's budget is **treated as blocked**: the item is dropped or shown as blocked. Without Farsight configured, visitors get no block enforcement on restored content.

Members' blocks, mutes and list blocks apply as on every other read.

## Methods

All need a signed-in caller and are in the `refract` rate class.

| Method | Returns |
|---|---|
| `fill` | Profiles and posts of restored accounts named by DID or URI (up to 25 each); `pending` for accounts being checked; `withheld` for items held back for a reason that is not final. |
| `getAuthorFeed` | A restored account's feed, built from stored posts and reposts. |
| `getAuthorReposts` | Any account's reposts of restored posts. |
| `getPostThread` | A thread anchored on a restored post. |
| `getReplies` | Restored replies in a thread (and other replies under them), with the root's threadgate and blocks applied. |
| `getTimeline` | Posts by restored accounts a member follows, and reposts of restored posts by the member and accounts it follows. |
| `listNotifications`, `getUnreadCount` | A member's notifications from restored authors. |
| `searchActors`, `searchActorsTypeahead` | Restored accounts whose handle or display name starts with `q` (case and diacritics ignored): the exact handle first, then handle matches, then display-name matches, by handle. Shaped as Bluesky's actor searches; `searchActors` pages by cursor. |
| `getCommunities` | The enabled communities in list order, each with its PDS host patterns, lowercased. An account whose PDS host matches none is never restored, so a client can skip `fill` for it. Cached for an hour. |

`getMembership` lists `refract` while the feature is on, and `refract.search` with it for the two searches. With the feature off, the methods answer `NotEnabled`.

## Guarantees

- No account is restored without a listed community hosting it on its PDS.
- A community's gone label, ban or the account's departure ends the restore. A gone label that fails signature checks hides the account at once, and ends the restore if it is still unverified after `unverified_max_hours` (default 72).
- Hidden means hidden on every read path at once, before the purge finishes.
- The `restored` marker never sits on a hidden or blocked view.
- Members' own records are never removed by Refract.
- With visitor viewer state off, no reader identity is sent to any outside service.

## Limits

- Restored content is only as complete as the backfill and the firehose. A missing post falls back to the community AppView or Slingshot.
- A new community label on a post nobody views through Refract can take up to `record_label_sweep_hours` (default 24) to apply.
- With an `appview` label source, a new gone label on an account can take until the next check to apply, unless a read sees it sooner.
- Large accounts take time and storage to backfill. The operator controls exposure through the communities listed.
- Visitors without Farsight get no block enforcement.

See [configuration](../configuration.md) for the `[refract]` keys, [admin](../admin.md) for `admin refract` and the Communities and Refract accounts pages, and [blocks and moderation](blocks-and-moderation.md) and [Thread Lens](thread-lens.md) for how restored posts appear in lenses.
