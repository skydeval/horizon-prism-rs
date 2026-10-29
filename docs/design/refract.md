# Refract

Optional, off by default. Methods under `app.nearhorizon.refract.*`.

Refract keeps accounts readable that Bluesky's AppView has taken down while a community the operator lists still hosts them. Such an account is **restored**: served from Horizon-Prism's own store, with a `restored` marker on every view.

## Communities

Listed in `[[refract.communities]]` or on the admin UI's Communities page.

| Field | Meaning |
|---|---|
| `name` | Unique; shown in the `restored` marker. |
| `pds_patterns` | The community's PDS hosts, e.g. `["pds.example.org", "*.pds.example.org"]`. First match wins. |
| `tier` | `1`: the community runs an AppView. `2`: it runs a labeler. |
| `appview_network` | Tier 1: its `[[constellation.networks]]` entry. |
| `labeler_did` | Required on tier 2, optional on tier 1. |
| `label_source` | `appview` (tier 1 only) or `labeler`. |
| `gone_labels` | Labels that end a restore. Default `["!takedown", "!hide"]`. |
| `enabled` | Off: the entry restores nothing. |

An account qualifies only through the PDS in its DID document, and only its own community's labels count.

`refract.enabled` and the community list apply at once when saved. When a change would stop accounts matching, they are hidden at once and ended after `disable_delay_hours` (default 24). Switching back in time restores them; `keep` holds a pending change.

## Checks

An account is checked only when a client names it in `fill` or the operator adds it (`admin refract check`, or the Refract accounts page). It is restored when:

1. Bluesky's AppView withholds it;
2. its PDS matches an enabled community;
3. its PDS reports it `active` (`deactivated` gives a grace period of `grace_days`, default 30);
4. the community still serves it (tier 1) and no gone label is on it;
5. it has no local `!takedown`.

Failures that cannot be answered keep the last state and are retried. Restored accounts are checked again every `recheck_hours` (default 24) and on identity, account or label events. An account Bluesky serves again ends. Community labels on single posts hide those posts only.

## Storage

Refract stores the restored account's own records (backfilled from its PDS, kept current from the firehose, resynced weekly), reposts of its posts, its notifications to members, the community's labels, and an audit trail (`event_days`, default 365). When a restore ends, what only Refract kept is purged. Other accounts' records are never purged.

## Visitors

Any signed-in caller may use Refract. Non-members are visitors: `getTimeline`, `listNotifications` and `getUnreadCount` answer empty, and answers say `servedAs`. Visitors' own likes, reposts and follows on restored views (`refract.visitor_viewer_state`, off by default) are looked up from Constellation per request and never stored. Blocks for visitors come from Farsight; an unanswered pair is treated as blocked. Without Farsight, visitors get no block checks.

## Methods

All need a signed-in caller and use the `refract` rate class.

| Method | Returns |
|---|---|
| `fill` | Restored profiles and posts by DID or URI (up to 25 each), plus `pending` and `withheld` |
| `getAuthorFeed`, `getAuthorReposts` | A restored account's feed; reposts of restored posts |
| `getPostThread`, `getReplies` | Threads and replies involving restored posts |
| `getTimeline` | Restored posts and reposts from accounts a member follows |
| `listNotifications`, `getUnreadCount` | Notifications from restored authors |
| `searchActors`, `searchActorsTypeahead` | Restored accounts by handle or display-name prefix |
| `getCommunities` | Enabled communities and their PDS patterns (cached an hour) |

`getMembership` lists `refract` and `refract.search` while it is on. Off, the methods answer `NotEnabled`.

See [configuration](../configuration.md) for the `[refract]` keys and [admin](../admin.md) for `admin refract`.
