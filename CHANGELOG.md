# Changelog

All notable changes to horizon-prism-rs are recorded here, in the style of
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/). Chainlink issues are
referenced inline as `#NN`. Every commit that lands functional work updates the
matching section under `[Unreleased]` in the same commit; issues are closed
with a changelog entry (no `--no-changelog`). Entries before item 5 were
backfilled per milestone (§15 implementation order of design-v2), not per
commit.

## [Unreleased]

### Added

- **Item 10: rate limits are config (#669).** Every limit is
  `[rate_limits.<class>]` with `per_sec` (a decimal) and `burst`, with the
  former constants as defaults (design-v3 §10). Startup refuses a `per_sec`
  that is not finite or is outside 0.000001 to 1 000 000, a `burst` below 1,
  and a pair that would overflow the bucket arithmetic. New classes
  `search_anon` (10/s, burst 20) and `search_auth` (30/s, burst 60) now
  charge `searchActors`, which used the read classes before. `resolve` has
  one bucket per endpoint and IP. `rate_limits.endpoint_class.<nsid>` moves
  one served endpoint to another class family (`read`, `search`, `write`,
  `private`, `thread_lens`, `index_request`, `resolve`); an unknown family,
  an NSID this AppView does not serve, or a DID-keyed family on an endpoint
  that accepts anonymous calls is a startup error. `service_proxy_member`
  and `service_proxy_ip` are accepted and validated; nothing charges them
  until service proxying is built.
- **Item 10: upstream calls draw on pools and shares (#669).** Every
  request carries a handle naming who is charged (a ready member by DID,
  everyone else by IP key) and one 6 s deadline that all its cascades
  share, in place of 6 s per cascade (design-v3 §II-8.3). Each outbound
  network's `rate_per_sec`/`burst` bucket, Slingshot
  (`microcosm.slingshot_rate_per_sec`) and request-origin Constellation
  (`microcosm.constellation_rate_per_sec`) are now split into a member pool
  and a non-member pool with per-requester shares. With the defaults a
  single non-member address gets 2.5 calls a second (burst 5) to Bluesky's
  AppView, where every caller shared 20 a second before. An empty share or
  pool is no answer: it stops the cascade and its fallbacks for that item,
  is never recorded by a breaker, and never turns into a not-found.
  - A single-subject request that then finds nothing (`getProfile`,
    `getPostThread`, an actor that does not resolve) answers
    `429 RateLimitExceeded` with `ratelimit-reset`, not "not found".
  - `getLikes`, `getRepostedBy`, `getQuotes` and `getFollowers` answer 429
    when there is no budget for the Constellation listing, and no longer
    fall back to local rows in that case.
  - Posts and profiles inside a list are left out; a thread's parent chain
    stops without a not-found item; counts use the stored or local value;
    Thread Lens reports `upstream-unavailable`.
  - A cascade cut by the request's deadline is no answer too. A call the
    deadline cuts short is not counted against the network's breaker
    (`upstream_budget_cutoff_total{network}`); before, slow requests could
    open a healthy network's breaker.
  - An author feed or follows list read from another AppView or the
    author's PDS answers 429 when it ran out of budget before producing
    anything. A PDS feed that runs out while fetching a reposted post ends
    the page before that item and keeps its cursor behind it, so the next
    page resumes there.
- **Item 10: the pre-auth backstop chooses who a request is charged to
  (#669).** Before the per-requester backstop, a request whose token passes
  the claims pre-check, names a ready member whose signing key is held in
  memory, is admitted by the verification limits and verifies against that
  key is charged to the member's DID: the `global` backstop, and every
  upstream pool and share. Everything else is charged to its IP key
  (design-v3 §II-8.3). So members behind one PDS no longer share that
  PDS's address for the backstop or for upstream budgets, and a forged
  token naming a member spends none of the member's limits. That step
  reads no database and makes no network call.
  - A key under 1 h old authenticates, and the handler does not verify the
    token a second time. A key 1 h to 24 h old only selects the member;
    the handler verifies as before.
  - Ready members' DID documents are held in a table reloaded from
    `did_cache` every 30 s; documents older than 12 h, or missing, are
    re-resolved in the background (`member_key_refresh_failed_total`,
    gauge `member_keys`).
  - A member whose own verification ceiling is empty gets 429 and is
    charged to neither key.
  - Thread Lens, the private endpoints and `index.requestRecord` use the
    same selection.
  Differences from the design text, recorded on #669: the selection is
  passed to handlers through a task-local, not request extensions; the key
  table follows `did_cache` with up to 30 s delay instead of being updated
  on each write; only the table counts as an in-memory key (not
  kryphocron's own cache or the fetch cell).
- **Item 11: migrations 0004–0011 and 0013–0017 (#671).** The schema of
  design-v3 §3.1: lists, list items, list blocks and opt-outs; the rule-4
  subscription tables with `subscribed_lists.item_count` kept by
  triggers; list and thread mutes; starter packs and their counts;
  labeler declarations and the other rkey-self actor records; thread and
  post gates; poll posts; bookmarks, drafts, activity subscriptions and
  notification preferences v2; `via` references on follows, likes and
  reposts; profile columns on `actors`; search and trend columns on
  `posts`; `tag_activity_hourly` with its `posts`, `likes` and `reposts`
  triggers; backfill job generations. Every change is additive, and none
  builds an index on `posts`, `likes` or `reposts`. Nothing writes or
  reads the new tables and columns yet. With 0012 they ship as one
  upgrade.
  - The startup check that every member can refresh its key once per
    window from PLC's issuer-refresh slice is added (`community.max_members`
    against `identity.member_pool_refresh_secs`, design-v3 §13).
- **Item 10: an in-scope author's feed continues from its PDS (G-3,
  #670).** When `getAuthorFeed` runs out of stored rows for an in-scope
  author whose PDS lists records newest first, it emits a
  `P:<oldest stored post rkey>:<oldest stored repost rkey>` cursor and the
  next page comes from the PDS, as it already did for out-of-scope
  authors. On a PDS that lists oldest first, or whose order is unknown,
  the feed ends with the stored rows. A PDS page that the filter empties
  now keeps its cursor, so it no longer ends the feed.
- **Item 10: `getPostThread` shows replies and parents it does not store
  (G-6, #670).** A node's replies are the union of the local rows and the
  replies Constellation lists for it (up to
  `thread.constellation_replies_per_node`, default 50). Replies and
  parents not stored here are fetched from Slingshot through the read-time
  cache, at most `thread.max_remote_fetches` (default 100) per request;
  the node cap is `thread.max_nodes` (default 200). Before, only local
  replies were shown and a parent by an out-of-scope author was a
  not-found item.
  - A listed reply that cannot be fetched, or whose record does not reply
    to that node, is left out, not shown as not-found.
  - A reply or parent by an in-scope author is never fetched: if it is not
    stored, it is gone.
  - A parent there was no budget to fetch ends the chain with no item.
- **Item 10: `getQuotes` shows quoting posts it does not store (G-7,
  #670).** A quoting post Constellation lists and this AppView has not
  indexed is fetched from Slingshot through the read-time cache, at most
  `limit` per page, and placed in listing order. Before, such posts were
  left out. Hidden posts and blocked authors are filtered before any
  fetch.
- **Item 10: `getProfiles` resolves out-of-scope actors (G-1, #670).**
  An actor this AppView does not index is fetched the way `getProfile`
  fetches it (its home AppView and the cascade, then the Slingshot profile
  record), up to 8 at a time, and returned in input order. Before, such
  actors were dropped. One that no source resolves, or that there is no
  upstream budget for, is omitted.
- **Item 10: token-issuer lookups are charged to the request (#669).**
  When verifying a token needs the issuer's DID document from the network,
  the fetch costs the request one token of its identity share and, for a
  `did:plc`, a PLC token (design-v3 §II-8.3). The charge is the first step
  of the fetch itself, so a document already held costs nothing and a
  fetch that finds no budget stores no failure. PLC's member pool keeps
  half of itself as an issuer-refresh slice: on a request charged to an IP
  key, a ready member's document may be fetched from it once per
  `identity.member_pool_refresh_secs` (default 600). So a flood of tokens
  naming members takes at most that slice, and the rest of the member pool
  stays for members' own requests. A request whose issuer lookup found no
  budget answers `429 RateLimitExceeded`, not `401`.
- **Item 10: pre-auth backstop parts (#669).**
  `xrpc::backstop` holds the two parts of design-v3 §II-8.3's backstop
  that need no I/O. The claims pre-check reads a token's unverified
  payload and applies the claim checks the verifier makes after the
  signature (`iss`, `aud` with no fragment or `#bsky_appview`, `lxm`,
  `iat`, `exp`, `nbf`, the validity window, `nonce`), with the verifier's
  own skew and window. `VerifyLimits` is the token lifecycle around a
  pre-backstop verification: failed-verification limits per (IP key,
  issuer) and per IP key, an exemption for a member that verified from
  that IP key against the same key within a day, a per-member ceiling and
  a per-IP-key ceiling. New `[auth]` settings, validated at startup.
- **Item 10: identity lookups and author-PDS listings are charged
  (#669).** Resolving a DID or a handle for a request costs one token of
  the requester's identity share
  (`identity.requester_resolutions_per_sec`, default 5/s, burst 25), and a
  PLC directory fetch it causes also draws on PLC's request bucket
  (`identity.plc_request_rate_per_sec`). A summary (PDS and verified
  handle) is reused for 5 minutes, so a DID named many times is resolved
  and charged once; before, every use repeated the handle check over the
  network. Without budget the lookup has no answer, and `resolveDid`
  answers 429 instead of the unresolvable shape. Every `listRecords` call
  made for a public request draws on its PDS host's own bucket
  (`sync.public_fallback_rate_per_host`, default 25/s, for up to
  `sync.max_public_fallback_hosts` hosts), split into pools and shares
  like the others. It no longer uses the private tier's read-fetch bucket,
  so public traffic cannot drain the private tier's head fetches. Token
  issuer lookups during authentication are not charged yet.
- **Item 10: pools and fair shares (#669).** `xrpc::pools`
  holds the shared upstream bucket of design-v3 §II-8.3: a member pool and
  a non-member pool (`constellation.nonmember_fraction`, default 0.5), and
  inside each a per-requester share (`constellation.requester_share`,
  default 0.25) kept in one LRU cache
  (`constellation.requester_share_max_entries`, default 200 000). A draw
  takes the share token and the pool token as one step, so an empty pool
  does not spend the share. New settings, validated at startup:
  `microcosm.slingshot_rate_per_sec`/`_burst`,
  `microcosm.constellation_rate_per_sec`/`_burst`,
  `identity.plc_request_rate_per_sec`/`_burst` and
  `identity.requester_resolutions_per_sec`/`_burst`.
- **Item 10: IPv6 callers are limited per `/64`.** Every IP-keyed rate
  limit (anonymous reads and search, `resolve`, the per-IP backstop) keys
  an IPv6 address by its `/64`, and an IPv4-mapped address by its IPv4
  address (design-v3 §II-8.3 item 3).
- **Item 10: gallery embeds.** `app.bsky.embed.gallery` is rendered wherever
  an embed is: as a post's embed, as the media of `recordWithMedia` (which
  no longer loses its quoted record), and inside a quoted post's `embeds`
  (design-v3 §8.4; locked decision 33). Every view item carries
  `$type: app.bsky.embed.gallery#viewImage` with `thumbnail`, `fullsize`,
  `alt` and `aspectRatio`; a record item of another type, or one missing
  `alt` or `aspectRatio`, is left out.
- **Item 10: `getAuthorFeed` `posts_with_video` (G-2).** The filter is
  accepted and returns posts whose media is a video, directly or inside
  `recordWithMedia`. One classifier, `media_type`, decides for local rows
  and for records listed from a PDS.
- **Item 10: scoped mutes.** `muteActor` takes `onlyReposts` and
  `onlyQuoteposts` and stores `mutes.scope` (`full`, `reposts`, `quotes`,
  `reposts_and_quotes`; migration 0012). A repeat call replaces the scope
  and keeps the row's `created_at`; `unmuteActor` deletes the row whatever
  its scope; `getMutes` lists full mutes only (design-v3 §5.3; locked
  decision 32). A repost mute removes the subject's reposts from the
  timeline and their `repost` notifications; a quote mute removes their
  quote posts and `quote` notifications. `viewer.muted` is the full mute;
  `mutedOnlyReposts` and `mutedOnlyQuoteposts` are set only when it is
  false. The private-tier mute oracle reads full mutes only.
- **Item 10: the final router, with CORS.** Every route is merged into one
  router, which then gets its fallbacks, the per-IP backstop and, outermost,
  a CORS layer (design-v3 §6.3, §6.5). Every `/xrpc/*` response, errors
  included, carries `Access-Control-Allow-Origin: *`; preflights allow `GET`
  and `POST` with `authorization`, `content-type`, `atproto-proxy` and
  `atproto-accept-labelers`; `X-Prism-Sort-Fallback`, `ratelimit-reset`,
  `Retry-After` and `atproto-content-labelers` are exposed. This covers the
  private, index, Thread Lens, resolve-DID and health routes, so a browser
  client can call the AppView directly with a bearer token.
- **Item 10: router fallback and wrong-verb answers.** An unregistered
  `/xrpc/<nsid>` answers `501 MethodNotImplemented`, a malformed NSID
  `400 InvalidRequest`, and a registered method asked with the wrong verb
  `400 InvalidRequest` "Incorrect HTTP method (X) expected Y". Before, these
  were a bare 404 and an empty 405. Both are charged to `anon_read`.
- **Item 10: honest-501 routes.** The 36 deferred endpoints (feed
  generators, push, preferences, reports, video, external embed views, age
  assurance, contact) answer `501 NotImplemented` from registered routes,
  for their lexicon verb, without authentication (§6.1, §6.2).
  `honest_501.status_url` fills the "See <link> for status." pointer, and an
  empty one drops that sentence; `honest_501.messages.<nsid>` overrides one
  message. `honest_501_total{nsid}`.
- **Item 10: `[services.*]` and `[features]` config.** Deferred external
  services (`contact`, `age_assurance`, `push`, and operator-defined ones
  with their own `methods`) are `disabled`, `proxy` or `serve` (§6.7). This
  build accepts only `disabled`: `proxy`, `serve` and any `features.* = true`
  stop startup with exit 78 and say why. The whole of §6.7.2.3's validation
  is in, including the reserved namespaces and the one-owner-per-NSID check
  for custom services. `features.push`, `features.age_assurance` and
  `features.contact` are removed and name their replacement key.
- **Item 10: `Retry-After` on `NotReady`.** Every `400 NotReady` carries
  `Retry-After` from `community.not_ready_retry_secs` (default 5): a constant
  hint for clients, not an estimate of indexing progress.
- **Item 10: `#bsky_notif` and `/did.json`.** The did:web document lists the
  notification service beside `#bsky_appview`, so a PDS proxying push
  registration reaches the honest 501. `/did.json` serves the same bytes as
  `/.well-known/did.json`, both with `Cache-Control: public, max-age=300`.
- **Item 8.5: pre-Phase B verification tests.** Ascending-PDS author-feed
  fallback (newest `limit` records from at most 10 listing pages, no cursor,
  a `P:` cursor ends the feed) against a mock ascending PDS; eligibility
  streak restarts after a mid-streak success. The Thread Lens block-party
  and `tier-hidden` no-lookup tests re-run green.
- **Item 8.5: `app.nearhorizon.index.requestRecord`.** The §5.9 safety net:
  any authenticated DID may ask the AppView to index a record from its own
  repo; the record is fetched from the caller's PDS (`getLatestCommit`, then
  `getRecord`) and run through the normal write path, answering `indexed`,
  `deleted`, `out_of_scope` or `unchanged`. A URI outside the caller's
  repo and an unsupported collection are distinct `InvalidRequest` messages
  (#232); per-DID 0.5/s, burst 30. `admin deadletter replay` now shares the
  same procedure (`firehose::index_request`).
- **Item 8: upstream views prefer Constellation counts.** After
  normalization, every upstream post view (`likeCount`, `repostCount`,
  `replyCount`, `quoteCount`) and profile view (`followersCount`) takes a
  fresh or stale `constellation_counts` value over the upstream count;
  stale rows and targets with no row are queued on `cc:refresh`, and with no
  row the upstream count stays (never a local zero). Applies to public
  upstream views and Thread Lens. `upstream_view_counts_total{source}`.
- **Item 8: notification, mute and label coverage.** Tests for every
  notification reason's record (`reply`, `mention`, `quote`, `like`,
  `repost`, `follow`), block exclusion in both directions for the list and
  the unread count, `indexed_at::id` paging, unmute idempotence, and local
  label emission and enforcement (`!takedown` hidden from lists and direct
  fetches, `!hide` listed nowhere but returned on direct `getPosts`,
  informational labels on posts, authors, profiles and thread replies).
- **Item 8: mutating admin commands tested through the queue.**
  `backfill` (default `member`/`scope` depth, explicit depth),
  `scope rebuild --dry-run` (counts, no side effects), `resync
  --private-and-members` (gap epoch advanced, member readiness cleared,
  `authz`/`resync` enqueued), `deadletter replay` (record re-fetched and
  admitted, absent record deleted, entry left listed) and `deadletter drop`.
- **Item 7: migration 0002.** `private_posts.reply_root_cid` and
  `reply_parent_cid` (nullable). Rows written earlier keep NULL until an
  `admin backfill --depth resync` refetches them.
- **Item 7: reply cids on private views.** `postPrivate` ingest reads
  `reply.{root,parent}.cid` (string or `$link`; a cid of any other shape is
  malformed) and `privatePostView.record.reply` carries `{uri, cid}` strong
  refs, `{uri}` only for pre-0002 rows.
- **Item 7: eligibility failure handling.** Migration 0003 adds
  `actors.eligibility_failure_count` and `eligibility_failing_since`. A
  failed `getRepoStatus` confirmation (network error, 5xx) keeps eligibility
  and extends the streak; three consecutive failures spanning at least 24 h
  remove it (`eligibility_cleared_after_failures`). Definitive answers (repo
  not found, inactive, no PDS, host outside the community patterns) remove
  it at once, and any answer resets the streak. Backfill `prepare` now
  returns a typed `PrepareError` (`Negative` / `Failed`).
- **Item 7: `admin member remove` re-confirms eligibility.** For a private
  owner whose community-PDS confirmation is missing or older than 24 h, the
  command re-confirms with `getRepoStatus` before the `member_removed`
  marker, so the purge end's owner re-evaluation sees fresh eligibility. The
  outcome never blocks the removal and is reported in the command result
  (`eligibility`).
- **Item 7: Slingshot client.** `microcosm::slingshot` `getRecord` for
  record-level fallback (§8.3 step 6, Thread Lens step 4): through `net`,
  3 s timeout, 256 KiB cap; classified as record, `NotFound`, `Transient` or
  `Invalid`. `net` gains a test-only loopback client for fake upstreams.
- **Item 7: `appview_clients`.** One outbound client per enabled network
  (the local `community` network has none): `unauth`, or `service_jwt` with
  a fresh AppView-signed token per call (`iss` did:web, `aud` =
  `appview_did`, `lxm`, 60 s `exp`, 16-byte `jti`, ES256K low-S). Typed
  `getProfile`, `getPosts`, `getPostThread`, `getAuthorFeed`, `getFollows`;
  3 s per call, 4 MiB cap.
- **Item 7: `constellation` routing.** The routing table from
  `[[constellation.networks]]` and `[constellation.cascade]`: §8.2 PDS
  matching (first match in file order; `community` is local, never an
  outbound call), the per-DID `route` cache (200 000 entries, 1 h, resolved
  DIDs only), and the call plan (home AppView, then the cascade without it,
  disabled networks skipped). §8.3 response classes (`hit`, `miss`,
  `withheld`, `auth-refused`, `transient`); per-network token buckets
  (empty = transient skip) and breakers (5 consecutive failures, 30 s open,
  one half-open probe); 3 s per call, 6 s per cascade.
  `app.nearhorizon.constellation.resolveDid` (public, 10/s burst 20 per IP,
  `cache-control: public, max-age=300`). Rate classes `threadLens` (2/s,
  10 per DID) and `resolveDid` added.
- **Item 7: `RealUpstreamSource`.** Hydration's seam now serves
  out-of-scope subjects: home AppView then the cascade (skipped for the
  `community` network), then record-level fallback. Profiles, posts and
  thread anchors (with their parent chain) fall back to Slingshot records;
  author feeds and follows fall back to the author PDS `listRecords` on the
  read-fetch budget, continuing (`P:` cursor) only on PDSes that list
  descending (order probed once per host, cached 24 h). Upstream views are
  normalized (§8.6: local viewer state and labels, private references
  not-found) and cached in `rt`; upstream post and thread fetches run the
  moderation gate first. Non-indexed handles resolve through the identity
  resolver (bidirectionally verified). `serve` wires the routing table,
  AppView clients, cascade, Slingshot and `resolveDid`.
- **Item 7: Thread Lens.** `app.nearhorizon.threadLens.getThread` and
  `.resolve` (POST, service-JWT auth, 2/s burst 10 per DID). The caller DID
  is used for the auth gate and rate limit only. Hydration runs with no
  viewer at all (never `actor_viewer`/`post_viewer`, every `viewer` is
  `{}`), and the gap taxonomy has no viewer-relative variant. The §10.4
  ladder: `tier-hidden` before any lookup, then the moderation gate, local
  tombstones and posts, home AppView and cascade (`getPosts`), Slingshot,
  and classification (`deleted`, `not-found`, `upstream-unavailable`,
  `content-stripped`). `getThread` takes its structure from upstream
  `getPostThread` (`#blockedPost` stubs → local/Slingshot, else
  `blocked`; `#notFoundPost` → full ladder), falls back to the anchor and
  parent chain, appends local and Constellation replies breadth-first, and
  caps at 200 nodes. Responses are `PublicEmit`s built in
  `hydration::lens`.
- **Item 6: private tier and `app.nearhorizon.private.*`.** `private_tier`
  owns every private table behind a `pub(crate)` facade: defensive
  `postPrivate` / audience parsing with dead-lettering of malformed records
  (#235), LWW ingest, owner set and readiness, purges, and private retention
  (registered ahead of public retention). Reads run the §6.3 pipeline per
  record: moderation gate, owner readiness, repo-head check, a per-record
  oracle snapshot (Block/Audience/Mute, rules 1–7), host freshness against
  `private_watermark`, kryphocron issue/bind, the sealed `RecordAuthz`
  witness, record validation and `decode_record_content`, emitted only
  through `PrivateEmit`. `getPosts`, `getPostThread` (200 nodes,
  `publicParent`) and `getAuthorFeed` (`created_at::rkey` cursor) share one
  uniform miss, per-endpoint timing floors, a 10/s (burst 20) per-DID limit
  and `private_read_outcome_total` / `private_read_unfloored_seconds`. The
  `private` backfill job confirms non-member owner eligibility (§6.11.1).
  `kryphocron_glue` gains panic-contained log audit sinks, the user-sink
  latch reported by `/xrpc/_health`, and validated at-rest hooks from
  `APP_DATA_DIR`; `clippy.toml` forbids the public codec surface.
- **Item 5: `app.bsky.*` XRPC surface and read-only admin CLI.** axum routes
  for `actor.getProfile(s)`, `actor.searchActors`, `feed.getPosts`,
  `getPostThread`, `getAuthorFeed`, `getLikes`, `getRepostedBy`, `getQuotes`,
  `getTimeline`, `graph.getFollows`, `getFollowers`, `getMutes`,
  `muteActor`, `unmuteActor`, `notification.listNotifications`,
  `getUnreadCount`, `updateSeen`, and `/.well-known/did.json` (secp256k1
  Multikey). Callers authenticate through the `kryphocron_glue` JWT path:
  `verify_request_jwt` with the issuer tracker, eviction drainer, rotation
  retry and the `aud`-fragment / `lxm` host checks. Per-IP and per-DID
  `governor` limits with `ratelimit-reset`, trusted-proxy
  `X-Forwarded-For`, `NotEnrolled` / `NotReady` gates, and auto-enroll
  (§9.2) sharing the member-add path with the admin executor. Read-only CLI:
  `admin member list`, `label list`, `deadletter list`, `stream info`,
  `commands list`, `keygen`, and `migrate status`.
- **Item 5: public hydration.** `hydration` builds `app.bsky.*` views from
  local tables behind the sealed `PublicEmit<T>`: profiles (`getProfile(s)`,
  `searchActors`), posts with embeds, author feeds with the §5.12 filters and
  pins, member timelines, threads (parent chain, replies, 200-node cap),
  follows, followers, mutes and notifications. Viewer state only for ready
  members (`blocks` only, never private blocks); §9.6 counts (synchronous
  Constellation for single-object views, `cc:refresh` for lists);
  Constellation-first likes, reposts, quotes and followers with local
  fallback; the §8.6 upstream normalizer; the in-process read-time cache; an
  `UpstreamSource` seam answering a uniform miss until item 7. `moderation`
  adds the §7.10 gate (`!takedown`, `!hide`, account status).
- **Item 4.5: admin executor and head prober.** CLI-to-`serve` command queue
  with `LISTEN/NOTIFY` (member add/remove, labels, scope rebuild, backfill,
  resync, dead-letter replay/drop), purge-aware per-DID deferral and crash
  recovery; per-host repo-head prober with independent probe, read-fetch and
  backfill budgets and divergence detection; `authz_backfill_stuck_total` and
  the `authz_stuck` log for authorization records that can never parse.
- **Item 4: firehose ingest and aggregations.** Redis stream consumer with the
  `read_checked` / `reanchor` / `group_create` Lua scripts, per-relay
  watermarks, gap transaction, relay coverage tracker and dead letters; record
  classifier and defensive record parsing (#233); per-record last-writer-wins
  writer with sorted DID and URI advisory locks; admission predicate, scope
  growth, two-phase purges, weekly scope rebuild; backfill worker with
  reconcile and the Constellation inbound-block pass; Constellation count
  enrichment; `serve` wiring and `/xrpc/_health`.
- **Item 3: foundations.** Configuration (`app.toml` plus `APP_*` overlay),
  Postgres pool, migration 0001 and the exit-78 migration gate, retention and
  storage sampler, SSRF-guarded outbound HTTP (`net`), DID/handle resolution
  with the JWT fetch cell (#234) and the identity refresher.
- **Item 2: lexicons.** `app.nearhorizon.*` lexicon JSON for v1, with
  `audienceList` optional in `privatePostView`.

### Changed

- `getAuthorFeed` `posts_with_media` now means images and galleries. Video
  moved to its own filter, as in the reference AppView, so a client with
  Media and Video tabs does not show a video twice. A `recordWithMedia`
  whose media is an external link no longer counts as media.
- The per-IP backstop is one layer on the final router instead of one per
  sub-router. `GET` and `HEAD` on `/xrpc/_health` stay exempt.
- `app.nearhorizon.private.getAuthorFeed`'s lexicon description names its
  ordering field (`record.createdAt`, clamped to indexing time plus five
  minutes).
- Design-v2 §6.11.3: read-fetch budget 25 per host (ascending-PDS worst
  case); §13 row 185.
- `sync.read_fetch_rate_per_host` defaults to 25 (was 20), in code and in
  `docker/config/app.toml`: the worst-case ascending-PDS author feed needs
  21 read-fetch calls (order probe, 10 post pages, 10 repost pages).
- Dockerfile: the COPY comment no longer describes `clippy.toml` as
  pending; it names the file and the clippy step that enforces it.
- F: requestRecord emits two distinct InvalidRequest messages (#232)
- Design-v2 §6.11.1/§4.2: eligibility failure handling (3 failures over
  24 h), negative answers, streak reset, and the `member remove`
  re-confirmation; reply cids and the eligibility columns in the schema.
- Design-v2 §7.1/§9.6: `getLikes` and `getRepostedBy` page Constellation
  `/links` (per-record timestamps), not `/links/distinct-dids`; counts are
  unchanged.
- I: postPrivate field parsing at the private-tier decode boundary must be defensive (#235)
- Design-v2 clarifications: dead-letter replay leaves the entry in place
  (§5.9); the head prober skips DIDs with an `authz` job in flight (§6.11.3);
  `admin backfill` defaults to `member` depth for members, `scope` otherwise
  (§3.3); item 5 builds the `kryphocron_glue` JWT path and an upstream seam
  (§15). postPrivate field parsing tracked for item 6 (#235).
- `read_checked` computes consumer lag as `entries-added − entries-read` from
  the group counter; Redis 7.4 re-estimates the `XINFO GROUPS` `lag` field.
- `hickory-resolver` pinned to 0.26 so one hickory is in the dependency graph.

### Fixed

- **Item 10: an upstream network could stay unreachable until restart
  (#669).** After a breaker's open period one probing call is let through
  and every other call is refused until that call's result is recorded. A
  request dropped while its probe was in flight (a client disconnecting, a
  timeout further up) never recorded one, so the network was skipped by
  every later cascade until the process restarted. Admitted calls now hold
  the probe slot through a guard that gives it back on any exit without a
  recorded outcome (design-v3 §II-8.3). `SharedBreaker` and `Admission` are
  the reusable form, for the Farsight breaker later.

- **Item 10: `seenAt` on the notification endpoints (G-4, G-5).**
  `getUnreadCount` now reads `seenAt` and, when present, counts from it
  instead of from the stored last-seen time; before, the parameter was
  ignored. `listNotifications` answers `400 InvalidRequest` to a request
  carrying `seenAt`, which the lexicon marks unsupported; before, it was
  accepted and ignored. `priority` stays accepted and ignored on both.

- Retention and scope growth no longer race: scope-dependent deletes run per
  DID under the DID's exclusive advisory lock, and growth takes the subject's
  shared lock before inserting `scope_dids`.

### Security

- A service token whose `iat`, `exp` or `nbf` is too large to be a time is
  rejected as `InvalidToken` before verification (#682). kryphocron 0.4.0
  turns those claims into times with an addition that panics on overflow,
  after the signature has verified, and anyone can sign a token for a DID
  of their own. Such a request used to end in a panicked request task
  instead of a 401.
- `/xrpc/_health` answers anonymous callers with `{"ok","version"}` only;
  the db/redis/audit flags, per-relay watermarks, `private_watermark` and
  `gap_blocked` need a service JWT that verifies with `lxm` `_health` (no
  membership check). An invalid token is answered as anonymous. Status
  codes are unchanged (200/503), and the route stays outside the global
  per-IP bucket except that a request carrying `Authorization` is charged
  to it before verification. Design-v2 §7.1; §13 row 186.
- Authorization-collection records that fail deterministically keep readiness
  unset (fail-closed) and raise the `AuthzBackfillStuck` alert.
