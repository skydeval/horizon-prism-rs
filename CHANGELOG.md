# Changelog

All notable changes to Horizon-Prism-rs (Horizon-Prism for short), grouped by
development phase, newest first. **Read the upgrade notes of every phase you
cross before upgrading.** Every schema migration needs one start with
`APP_ALLOW_STARTUP_MIGRATE=1` (`docker/README.md`, "Schema migrations").

New entries go under `## [Unreleased]` and move into a phase when it closes.

## [Unreleased]

### Added

- **Push for kryphocron notifications** (`[private_push]`, off until `url` and
  `service_did` are set): a new notification from a live write sends its
  recipient's DID, and nothing else, to the push service, in batches, under
  the `push` and `include` preferences and the list's exclusions. Backfill and
  catch-up never push. `getMembership` lists `private.push` while it is on.
  New `private_push_calls_total`, `private_push_dids_total` and
  `private_push_dropped_total` metrics. No migration.

- **Admin UI: Instance page** (`/admin/instance`), for signed-in operators
  only. The custom lexicons are compiled into the binary and served to
  operators at `/admin/lexicons/<path>`.
- **Public page at `/`**: the product line, version, the operator's own text,
  contact, apps, an optional source link, and a live feed of every relay event
  (`GET /live`, server-sent events; `tools.kryphocron.*` commits are left out).
  The admin switches **Public page** and **Public page name** apply at once.
  New `[public_page]` section (`enabled`, `name`, `about`, `contact`,
  `source_url`, `clients`, `live_feed`, `live_max_viewers`). **Proxy:** the
  shipped Caddyfile's `@live` matcher now includes `/live`; a Caddyfile of
  your own needs the same edit.
- A new header mark, served as `/favicon.svg` and `/favicon.ico`.
- **Thread outline index** (`[thread_outline]`, off by default): the reply
  structure of every public post on the firehose, with no text (about 17 GB at
  the default 30-day window). Thread Lens reads parents, replies and cursors
  from it when it is on, fresh and Farsight is configured, and falls back
  otherwise. New `outline_*` metrics and `threadlens_outline_total`.
- Thread Lens: `getThread` takes a handle-form anchor
  (`threadLens.handleAnchor`), and marks quote posts whose card is whole with
  `embedWhole: true`.
- Refract: `getCommunities` (enabled communities and their PDS host patterns)
  and actor search, `searchActors` and `searchActorsTypeahead`
  (`refract.search`).
- **System accounts**: an owner-only list of the other accounts of a plural
  system (`app.nearhorizon.account.*`, feature `account.systemAccounts`),
  sealed at rest, up to 31 accounts, with its own rate class.
- **Audiences held by Horizon-Prism**: `app.nearhorizon.private.createAudience`,
  `updateAudience`, `deleteAudience` and `listAudiences`, owner-only, at most
  200 per member (`private.audiences`).

### Changed

- **The shipped relays, AppViews and communities are Bluesky's only**: relays
  `bsky.network` and `relay.fire.hose.cam`, `cascade.order = ["bluesky"]`,
  and no refract community. An instance that relied on the old defaults sets
  them in its own config.
- **Bluesky's AppView is `api.bsky.app`** (was `public.api.bsky.app`), which
  applies the reader's labelers. Make the same change to the `bluesky`
  network's `appview_url` in your own `app.toml`.
- **`tools.kryphocron.policy.audience` records in repositories are ignored.**
  A member's removal now deletes its audiences for good.
- `refract.fill` waits up to 300 ms for the resolves it starts, so most new
  DIDs are answered as final on the first call.
- Thread Lens keeps one Farsight connection warm, keeps bridging at partial
  coverage, sends `replyDisabled: false` on threads without a threadgate, and
  names a labeler as applied only when every upstream answer applied it.
  `threadlens.verdict_round_timeout_ms` defaults to 800 (was 500).
- Admin UI sessions last 30 days idle and a year in all (were 8 hours and 7
  days).
- A missing or wrong `APP_SYSTEM_ACCOUNTS_KEY_HEX` turns off system accounts
  only; the rest of Horizon-Prism starts.

### Fixed

- The `_health` detail is shown to operators only.
- Self-labels always carry `cts`.
- Authors' self-labels show on posts, profiles and cards.
- Saving notification preferences while being removed no longer leaves them
  behind.

### Upgrade notes

- **Audiences (migration 0052).** First deploy commit "Private audiences S0: admin
  audiences check" and run `admin audiences check` in it until it exits 0 (`docker/README.md` explains
  each outcome). Upgrade a PDS that checks audiences itself first. Then start
  this release straight after with `APP_ALLOW_STARTUP_MIGRATE=1`. 0052 drops
  `sync_state.audience_listed_rev`, so it cannot be rolled back by changing
  the image.
- Migrations 0048 and 0049 (system accounts), 0050 (thread outline), 0051
  (sign-in flows).
- System accounts are off until `APP_SYSTEM_ACCOUNTS_KEY_HEX` is set
  (`admin keygen` prints one). A compose override must pass it through. Back
  the key up once lists are stored.

## Phase 9: Pronouns and plural systems (2026-10-24)

### Added

- `app.nearhorizon.plural.getRecords` and `listMembers`: any account's
  pronouns and plural records, no sign-in needed. Rate classes `plural_anon`
  and `plural_auth`; new `[plural]` section; `plural_*` metrics.
- Kryphocron posts carry their `app.nearhorizon.plural.fronters` key.
- Features `plural.fronters` and `plural.read`.

### Upgrade notes

- Migration 0047.

## Phase 8: Kryphocron extras (2026-10-21 – 2026-10-23)

### Added

- Notification preferences, activity subscriptions, appearance (kept 7 days
  after a member is removed) and public root gates under
  `app.nearhorizon.private.*`.
- The `app.nearhorizon.actor.appearance` lexicon.

### Changed

- A member's list blocks of moderation lists count as blocks in kryphocron
  reads.

### Removed

- `tools.kryphocron.graph.block` records are no longer tracked.

### Upgrade notes

- Migrations 0037, 0041, 0042, 0043, 0044 and 0045 (drops `private_blocks`).

## Phase 7: Refract (2026-10-22 – 2026-10-23)

### Added

- Refract (off by default): accounts Bluesky's AppView withholds stay readable
  while a listed community hosts them and has not banned them. `[refract]`
  config, `app.nearhorizon.refract.*` methods, the Communities and Refract
  accounts admin pages, `admin refract`, and `refract_*` metrics.

### Changed

- The moderation gate also runs after every upstream fetch, so nested posts by
  hidden or blocked authors are no longer served.
- The cascade's `getPosts` asks each author's network concurrently.

### Upgrade notes

- Migrations 0040 and 0046. The post-migration task builds the
  `reposts_subject_did` index.

## Phase 6: Resilience (2026-10-16 – 2026-10-22)

### Added

- Degraded mode: kryphocron reads keep working while a relay is behind and tell
  clients when to retry (`private.degraded`, `private.reindexing`).
- Every `429` carries `Retry-After`.
- Firehose throughput metrics and alert examples.

### Changed

- `firehose.batch_size` defaults to 2000 (was 500).
- Thread reply expansion is capped at 400 reads by default.

### Fixed

- The firehose sidecar no longer replays from a stale cursor after a reconnect.
- A missing consumer group is recognised on every read.
- `sync_state.diverged_since` clears once the applied rev reaches the head.

### Removed

- The disabled `community-b` network entry.

### Upgrade notes

- Migration 0039.
- Rebuild the firehose sidecar images.
- Remove `community-b` from `constellation.cascade.order` if your config lists
  it; otherwise validation fails.

## Phase 5: Thread Lens (2026-10-13 – 2026-10-24)

### Added

- `threadlens.access`: `signed_in` (default) or `members`.
- `hiddenByThreadgate`, `blockingByListUri`, hydrated views, and the reader's
  labelers.
- `threadlens.listing_timeout_ms` (default 1500).

### Changed

- `getThread` is about three times faster on a bridged thread.
- A post labelled `!warn` upstream is served as a post.

### Fixed

- The parent walk continues past a blocked post that is not stored locally.
- Reply expansion no longer re-adds replies the root's author hid.

### Upgrade notes

- Thread Lens now serves every signed-in account; set
  `threadlens.access = "members"` to keep it to members.

## Phase 4: Operator tooling and community scope (2026-10-18 – 2026-10-22)

### Added

- **Web admin UI** under `/admin`, with first-run setup, OAuth sign-in and
  saved settings layered over `app.toml`.
- CLI: `admin operator`, `admin setup-code`, `admin settings`,
  `admin restart`, `admin member deny|allow`, `admin deadletter sweep`,
  `admin identity compare`.
- Denials, never-ready expiry (`community.never_ready_expiry_days`), and
  Lodestar identity reads (`identity.lodestar_url`).

### Changed

- **Only members' kryphocron posts are served.**
- **`community.auto_enroll` defaults to `false`.**
- Scope follows recent activity (`scope.like_repost_days`,
  `scope.reply_quote_days`), with a nightly scope check.
- `scope.rebuild_grace_days` defaults to 0 (was 7) and must be 0 to 30.
- Blocks are stored only with a member on one side.
- Jobs of unavailable accounts end `unavailable` and are queued again when the
  account returns.

### Fixed

- Tombstoned `did:plc` accounts are recognised and purged.
- Admin forms no longer fail with "This form expired".

### Upgrade notes

- **Before migration 0031**, run the queries in `docker/README.md` ("Before
  the 0031 upgrade") and `admin member add` every non-member owner whose
  kryphocron posts should stay served.
- A config that does not set `[community] auto_enroll` stops auto-enrolling.
- The first nightly scope check removes every account nothing holds;
  `admin scope rebuild --dry-run` shows how many.
- An upgraded deployment enters first-run setup (code in the `app` log, or
  `admin operator add`). A Caddyfile of your own proxies `/admin/live`
  without `encode` and with `flush_interval -1`.
- Run `admin deadletter sweep` once.
- Migrations 0031, 0032, 0033, 0034, 0036, 0038.

## Phase 3: Kryphocron posts (2026-10-12 – 2026-10-21)

Horizon-Prism serves [kryphocron](https://github.com/skydeval/kryphocron)
posts (`tools.kryphocron.feed.postPrivate`) to the audience their author names.

### Added

- Audience modes `list`, `followers`, `following`, `everyone`, `nobody`.
- `app.nearhorizon.actor.getMembership`, with the deployment's `features`.
- Timelines, embeds, bookmarks, gates, counts and interactions,
  notifications, threads, search and media filters under
  `app.nearhorizon.private.*`.
- `index.requestRecord` answers `caughtUp`.
- Read-latency metrics.

### Changed

- Posts without encoded content are not served while
  `[private_any_pds] enabled = true` (the default).
- Videos on kryphocron posts play from the author's PDS.
- Read timing floors default to off.
- kryphocron 0.4.4 and kryphocron-lexicons 0.3.1.
- `sync.head_max_age_secs` above 600 is refused at startup.

### Fixed

- Public notifications no longer name a kryphocron post.
- A replayed, since-deleted record can no longer grant read access.

### Upgrade notes

- **Set `APP_PRIVATE_CURSOR_KEY_HEX`** (`openssl rand -hex 32`, the same on
  every replica) first; without it the app exits 78.
- Posts without encoded content stop being served;
  `[private_any_pds] enabled = false` serves them again.
- An `app.toml` that lists `[private_read.floor_ms]` keeps those floors.
  Lower `sync.head_max_age_secs` to 600 or less.
- Migrations 0023–0028, 0030, 0035.

## Phase 2: A complete AppView (2026-10-11 – 2026-10-18)

### Added

- The rest of the `app.bsky.*` read surface: lists, starter packs, graph,
  V2 threads, notification preferences, bookmarks, drafts, search,
  suggestions and trends.
- CORS on every `/xrpc/*` response, and forwarding of deferred services
  (`[services.<name>]`).
- Twelve more indexed collections.
- Members' list mutes and list blocks.
- Farsight for list blocks against members (`farsight.url`).
- Thread Lens v2.
- Rate-limit classes, pools and per-request deadlines.
- `indexing.public_scope = "off"` (the default for a fresh install).

### Changed

- `likeCount` and `repostCount` count accounts, not records.
- `getAuthorFeed`'s `posts_with_media` means images and galleries; video has
  its own filter.
- Metric renames: `farsight_fallback_calls_total` is
  `farsight_fallback_requests_total{endpoint, class}`; `refused_address` is
  `refused`; `handle_resolutions_total` gains `intent`.

### Fixed

- Out-of-range token times are refused instead of panicking.
- `searchActors` keeps to accounts in scope and applies blocks and mutes.
- Accounts outside scope no longer show through public paths.

### Upgrade notes

- Migrations 0004–0017 (a one-time re-sync follows, and `posts` may grow up to
  about twice; see `docker/README.md`, "After the 0004–0017 upgrade"), then
  0018–0022 and 0029.
- An upgraded database with public data stays `community_scoped`.
- Removed keys (`lists.subscribed_max_total`,
  `retention.subscribed_list_grace_days`, `features.push`,
  `features.age_assurance`, `features.contact`) are refused at startup.
- Read `docker/README.md` before turning on Farsight's fallback AppView.

## Phase 1: Foundation (2026-09-26 – 2026-09-28)

### Added

- Firehose ingest, the core `app.bsky.*` read API, the upstream cascade,
  service-JWT auth, the first kryphocron reads and Thread Lens,
  `index.requestRecord`, and a read-only admin CLI.

### Changed

- `sync.read_fetch_rate_per_host` defaults to 25 (was 20).

### Security

- `/xrpc/_health` answers anonymous callers with `ok` and `version` only.

### Upgrade notes

- Migrations 0001–0003.
