# API guide

Horizon-Prism speaks XRPC over HTTPS at `https://prism.example.org/xrpc/<method>`. It serves the stock `app.bsky.*` read API for its community, a few `com.atproto.*` methods, and Horizon's `app.nearhorizon.*` extensions. Errors use the atproto shape `{"error", "message"}`.

Lexicons for every `app.nearhorizon.*` method and record live in [`lexicons/app/nearhorizon/`](../lexicons/app/nearhorizon/). The stock lexicons are atproto's own.

Horizon-Prism never writes to a repository. Clients write records to their own PDS. Horizon-Prism reads them from the firehose (see [firehose-and-relays](design/firehose-and-relays.md)).

## Stock methods (`app.bsky.*`)

| Area | What Horizon-Prism serves |
|---|---|
| actor | `getProfile`, `getProfiles`, `searchActors`, `searchActorsTypeahead`, `getSuggestions` |
| feed | `getPosts`, `getPostThread`, `getAuthorFeed`, `getTimeline`, `getListFeed`, `getActorLikes`, `getLikes`, `getRepostedBy`, `getQuotes`, `searchPosts` |
| graph | follows, followers, lists, starter packs, relationships, known followers, suggested follows; mutes and mute lists (read and write); blocks and list blocks (read) |
| notification | `listNotifications`, `getUnreadCount`, `updateSeen`, preferences, activity subscriptions |
| bookmark, draft | create, delete and list bookmarks; create, update, delete and list drafts |
| labeler | `getServices` |
| unspecced | thread v2, suggestions, trends and trending topics, `getConfig`, starter-pack suggestions |

Signed-in stock methods (timeline, notifications, mutes, bookmarks, drafts and the like) serve members only. See [membership](design/membership.md).

**Public indexing off.** An operator can run Horizon-Prism without a public index. Then the public stock reads (profiles, posts, feeds, search, graph, notifications, bookmarks, suggestions, labeler services, `getRecord`) answer `501 NotImplemented` and tell the client to use its public AppView. Mutes, blocks, drafts, labels, handle resolution and every `app.nearhorizon.*` method keep working. While public indexing is being turned on, the same methods answer 501 with a "being turned on" message. See [indexing-and-scope](design/indexing-and-scope.md).

### Not served (`501 NotImplemented`)

These stock methods answer `501 NotImplemented` from a real route, so a client can tell "not here" from "broken":

| Family | Methods | Can be forwarded? |
|---|---|---|
| Feed generators | `getFeed`, `getFeedGenerator(s)`, `getActorFeeds`, `getSuggestedFeeds`, `describeFeedGenerator`, `sendInteractions`, popular/suggested feeds | No |
| Preferences | `actor.getPreferences`, `actor.putPreferences` (these belong to the PDS) | No |
| Reports | `com.atproto.moderation.createReport` | No |
| Video | `app.bsky.video.*` | No |
| External embeds | `embed.getEmbedExternalView` | No |
| Push | `notification.registerPush`, `unregisterPush` | No |
| Age assurance | `app.bsky.ageassurance.*`, unspecced age-assurance methods | Yes, `[services.age_assurance]` |
| Contact import | `app.bsky.contact.*` | Yes, `[services.contact]` (except `sendNotification`) |

An operator can forward the forwardable families to another server with `state = "proxy"` and a `proxy_url`. An operator can also declare custom services for its own NSIDs. See [configuration](configuration.md).

Any other well-formed NSID Horizon-Prism does not know answers `501 MethodNotImplemented`.

## `com.atproto.*`

| Method | Notes |
|---|---|
| `com.atproto.identity.resolveHandle` | Public |
| `com.atproto.label.queryLabels` | Public |
| `com.atproto.repo.getRecord` | Public; needs public indexing on |

## Horizon extensions (`app.nearhorizon.*`)

| Family | Methods | Who may call |
|---|---|---|
| `actor` | `getMembership` | Signed in |
| `private` | `getPosts`, `getPostThread`, `getAuthorFeed`, `getTimeline`, `getReplies`, `searchPosts`, `getLikes`, `getRepostedBy`, `getQuotes`, `createBookmark`, `deleteBookmark`, `getBookmarks`, `listNotifications`, `getUnreadCount`, `updateSeen`, `getNotificationPreferences`, `putNotificationPreferences`, `putActivitySubscription`, `listActivitySubscriptions` | Ready member |
| `private` (appearance) | `getAppearance`, `putAppearance`, `releaseAppearance` | Member (ready or not); a removed member may read and release its own retained appearance |
| `threadLens` | `getThread`, `resolve` (both POST) | Any signed-in account (the default), or ready members only if the operator sets `threadlens.access = "members"` |
| `refract` | `fill`, `getPostThread`, `getAuthorFeed`, `getAuthorReposts`, `getReplies`, `getTimeline`, `listNotifications`, `getUnreadCount`, `searchActors`, `searchActorsTypeahead` | Signed in; non-members are served as visitors |
| `plural` | `getRecords`, `listMembers` | Public |
| `account` | `getSystemAccounts`, `addSystemAccount`, `removeSystemAccount`, `getSystemAccountStatus`, `leaveSystemAccounts` | Signed in, member or not; each answers for the caller only |
| `index` | `requestRecord` (POST) | Signed in, for the caller's own records |
| `feed` | `getAuthorPolls` | Public |
| `constellation` | `resolveDid` | Public |

- **private**: the private tier. A private post is a public post under another record type; its content is encoded, which is friction, not encryption. See [private-posts](design/private-posts.md).
- **threadLens**: whole-thread views that bridge across AppViews and blocks. See [thread-lens](design/thread-lens.md). A post node with `embedWhole: true` has a quote card that is what the caller's own AppView would show, except for the caller's mutes, which the client applies to the quoted author: it came from an upstream AppView's answer for this request, the quoted author has no block with the caller as far as Farsight can tell, and every labeler in `atproto-accept-labelers` was applied by every upstream answer. A labeler the upstream does not recognise counts as not applied, and without Farsight nothing is marked. Without the flag, re-read the post as before.
- **refract**: views of restored accounts that a listed community still hosts. Optional; with it off every method answers `400 NotEnabled`. See [refract](design/refract.md).
- **plural**: pronouns and plural-system records. Optional; with `plural.enabled` off both methods answer 501. See [pronouns-and-plural](design/pronouns-and-plural.md).
- **account**: system accounts, a private list of the other accounts of the caller's plural system, kept like mutes and readable only by the caller. Adding an account takes a proof minted by that account's own server (`lxm` `app.nearhorizon.account.confirmSystemAccount`, a name no route serves). The three writes and the proof take fresh tokens: each lives at most 65 s and is accepted once. A listed account can ask whether any list holds it (yes or no) and leave every list. Every write returns the list's `rev`, which only grows. Optional; off, every method answers 501. While the operator's key is missing or wrong, and for a while after it is back, every method answers 501 with "lists are kept": keep what you hold and try later. See [system-accounts](design/system-accounts.md).
- **index.requestRecord**: ask Horizon-Prism to index a record the caller just wrote, in case no relay delivered it. Horizon-Prism fetches the record from the caller's PDS itself. The URI's repo must be the caller. It answers `status` (`indexed`, `deleted`, `out_of_scope`, `unchanged`) and `caughtUp`.
- **constellation.resolveDid**: a routing hint for a DID: its PDS, handle, home network and home AppView.

## Feature discovery

Call `app.nearhorizon.actor.getMembership` once per session. It returns:

- `status`: `member`, `indexing` (initial indexing running; has `Retry-After`), `reindexing`, or `notEligible` (not now; may change later).
- `permissions`: `privateRead`, `privateIndexed`, `threadLens`, so a client need not derive them.
- `validForSecs`: how long the client may act on the answer.
- `features`: what this Horizon-Prism serves now. Ignore values you do not know.

The call also enrolls an eligible caller, as loading the timeline does.

| Feature | Meaning |
|---|---|
| `private` | The private read methods |
| `private.embeds` | Facets, embeds, langs, labels and audience on private post views |
| `private.bookmarks` | Private bookmark methods and `viewer.bookmarked` |
| `private.gates` | Threadgates and postgates on private posts |
| `private.interactions` | Counts, viewer state, likes, reposts, quotes and thread mutes on private posts |
| `private.notifications` | Private `listNotifications`, `getUnreadCount`, `updateSeen` |
| `private.freshIndex` | `requestRecord` answers `caughtUp`; with `true`, the caller's private posts read at once |
| `private.paging` | Sealed cursors that page past unreadable posts; `filter` on `getAuthorFeed`; `getReplies` |
| `private.crossPdsReplies` | Replies under another account's audience, anchored in the thread, from any PDS |
| `private.following` | The `following` audience mode |
| `private.search` | Private `searchPosts` |
| `private.threadAudience` | A reply is read by its thread root's (or branch root's) audience |
| `private.mediaFilters` | `posts_with_media` and `posts_with_video` on `getAuthorFeed` |
| `private.notificationPreferences` | Private notification preferences, honoured by notifications and unread counts |
| `private.unreadReasons` | `getUnreadCount` takes `reasons` |
| `private.replyContext` | Timeline and author-feed replies carry `reply` (root and parent) |
| `private.notificationSubjects` | Notifications carry `reasonSubjectPost` |
| `private.notServed` | The author's own views say when a post would be served to no reader, and why |
| `private.degraded` | Degraded signals on private reads (see below) |
| `private.reindexing` | `503 Reindexing` and status `reindexing` for an interrupted member |
| `private.listBlocks` | Moderation-list blocks count as blocks in private reads |
| `private.activitySubscriptions` | Private activity subscriptions and `subscribed-post` notifications |
| `private.publicRootGates` | A public root's threadgate hides the private replies it lists |
| `private.appearance` | `getAppearance` and `putAppearance` |
| `private.appearanceRetention` | A removed member's appearance kept 7 days; `releaseAppearance` |
| `private.anyPds` | Private posts from any PDS are served; a post without encoded content is served to no one, so encode in the client |
| `threadLens` | Thread Lens methods are served |
| `threadLens.signedIn` | Thread Lens serves any signed-in account, not only members |
| `threadLens.hydrated` | Thread Lens views carry `servedAs`, viewer state, counts and `replyDisabled` |
| `threadLens.labelers` | Thread Lens honours `atproto-accept-labelers` |
| `threadLens.handleAnchor` | `getThread` takes `at://<handle>/app.bsky.feed.post/<rkey>` (any case), resolves the handle as `resolveHandle` does and answers the DID-form `anchor`. Errors: `HandleNotFound` (400), `UpstreamFailure` (502), 429. Without Lodestar, a handle prism has not stored is resolved by DNS and HTTPS probes, up to about 3 s of the request |
| `refract` | Refract methods are on |
| `refract.search` | `refract.searchActors` and `refract.searchActorsTypeahead` are served (listed with `refract`) |
| `plural.fronters` | Post records in views keep the plural fronters key |
| `plural.read` | `plural.getRecords` and `listMembers` are served |
| `account.systemAccounts` | The five `account.*` system-accounts methods are served, for members and non-members |

## Authentication

Horizon-Prism uses atproto service auth. The caller's PDS signs a short-lived JWT with:

- `iss`: the caller's DID;
- `aud`: `did:web:prism.example.org`, optionally with `#bsky_appview`;
- `lxm`: the method being called.

Most clients get this by calling their PDS with `atproto-proxy: did:web:prism.example.org#bsky_appview`. A client may also ask its PDS for a token and call Horizon-Prism directly. A token for one method does not work for another.

Horizon-Prism publishes its DID document at `/.well-known/did.json`, with the `#bsky_appview` and `#bsky_notif` services.

| Access | Methods |
|---|---|
| Public (no token) | Public stock reads, `resolveHandle`, `queryLabels`, `getRecord`, `getAuthorPolls`, `plural.*`, `constellation.resolveDid`. A token, if sent, must verify. |
| Signed in | `getMembership`, `index.requestRecord`, `refract.*`, `account.*`; `threadLens.*` (default) |
| Member, ready or not | Mute management; `putAppearance` (`getAppearance` and `releaseAppearance` also serve a removed member's own retained row) |
| Ready member | Other signed-in stock methods; `private.*` reads and writes; `threadLens.*` when limited to members |

`GET /xrpc/_health` answers anyone with `ok` and `version`. A caller with a valid service token for `lxm` `_health` gets the detail as well.

## Errors to handle

| Answer | Meaning | Client action |
|---|---|---|
| `401 AuthRequired` / `InvalidToken` | No token, or it did not verify | Get a fresh token |
| `400 NotEnrolled` | Caller is not a member | Call `getMembership` again |
| `400 NotReady` + `Retry-After` | Member's initial indexing is running | Retry after the hint |
| `503 Reindexing` | Member is being re-indexed after an interruption; `retryAfter` in body and header | Keep the member a member; retry |
| `503 Unavailable` | A private thread cannot be served yet; `retryAfter` in body | Retry |
| `degraded`, `retryAfter`, `unavailable` fields | Private reads are behind; the page may be incomplete. `unavailable` names the caller's own posts not yet served | Show what came back; retry later |
| `501 NotImplemented` | Not served here (deferred, or public indexing off) | Use another AppView, or hide the feature |
| `501 MethodNotImplemented` | Unknown method, or an optional family is off | Hide the feature |
| `400 NotEnabled` | Refract is off | Hide Refract |
| `429 RateLimitExceeded` | A rate limit hit; `Retry-After` and `ratelimit-reset` | Wait, then retry |
| `502 UpstreamFailure` | An upstream lookup failed | Retry later |

After `NotEnrolled`, `NotReady`, or a 501 on a method `features` said was served, call `getMembership` again.

## Rate limits

Each request is charged to a token bucket by class: read, search, write, private, thread_lens, index_request, resolve, refract, plural and system_accounts. Anonymous calls are keyed by IP. Signed-in calls are keyed by account. A global per-IP bucket backs them all. Some costly calls take more than one token. Forwarded services have their own buckets.

Every 429 carries `Retry-After`. The operator sets all numbers in `[rate_limits]` and can move a method to another class. See [configuration](configuration.md).
