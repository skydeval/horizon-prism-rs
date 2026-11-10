# Changelog

All notable changes to Horizon-Prism-rs (Horizon-Prism for short), newest
first: by version from 0.1.0, by development phase before it. **Read the
upgrade notes of every version and phase you cross before upgrading.** Every
schema migration needs one start with `APP_ALLOW_STARTUP_MIGRATE=1`
(`docker/README.md`, "Schema migrations").

New entries go under `## [Unreleased]` and move into a version when it is
released.

## [Unreleased]

### Added

- `[db] pool_size`: `"auto"` (the default) sizes the database pool from the
  writer lanes, crawl claimers and backfill workers, in place of a fixed 20.
  Prism warns when it comes within 5 of the server's `max_connections`, and
  Health when writers wait for a connection.
- Full's warnings, its confirm pages and `full-check` name the Postgres
  settings left at Postgres's defaults that slow full down.
- Full's writer transactions write strangers' likes, reposts and follows
  that notify no member in multi-row statements (`[firehose] bulk_arm`).
- The history crawl applies `[full_crawl] batch_ops` (500) records a
  transaction, interns a repository's accounts and keys first, and waits
  while live ingest is more than `yield_above_lag_secs` (30) behind.
- Full ingest projects when the stream would drop unread entries. A trial
  ends itself with cause `lag` within `[full_trial] trim_margin_minutes`
  (15; `end_on_lag`, or `--no-end-on-lag` per trial), and full sheds
  strangers' quiet commits within `[full] shed_margin_minutes` (10,
  `at_lag = "shed"`), fetching those accounts again whole later; members',
  S's and anything involving them always apply. A trial writes a capacity
  sample a minute and its result says what the server kept up with. Health
  shows full ingest. **Upgrade:** migration 0068.
- Metrics of where full's writers spend their time, of what Prism's own
  database sessions wait on, and of each relay's copies; status's dedupe
  share covers the last five minutes with dedupe on.

### Changed

- Writer lanes apply up to `[firehose] lane_batch` (64) commits in one
  transaction, and a transaction's counts are applied once, in key order,
  so lanes no longer deadlock on hot posts. Commits of members, of tracked
  accounts and of the private tier still run alone. **Upgrade:** migration
  0067.
- **Published image.** `app` runs `ghcr.io/skydeval/horizon-prism-rs`, at the
  version the checkout ships with (`APP_VERSION` in `.env` picks another).
  Get it with `docker compose pull app`; `docker compose build` now builds
  only the relay sidecars. To keep building from source, add
  `COMPOSE_FILE=docker-compose.yml:compose.source.yml` to `.env`. A compose
  override that sets `app`'s image or build keeps working.
- The shipped `docker-compose.yml` runs Postgres with `shared_buffers=512MB`
  (`POSTGRES_SHARED_BUFFERS` in `.env`), `max_wal_size=4GB`
  (`POSTGRES_MAX_WAL_SIZE`), `checkpoint_timeout=15min`,
  `effective_io_concurrency=200` and `random_page_cost=1.1`. Recreate `db`
  to apply.
- The shipped relay sidecar configs (`docker/firehose/*.toml`) turn on
  `publish_sync` and `publish_resume`, which full indexing uses. A sidecar
  config of your own needs the same two lines under `[publisher]`; restart
  the sidecars after the change.

### Fixed

- A full trial started from `kryphocron_only` keeps the post counts and tag
  activity from before the trial, and puts its fill's jobs back as they
  were. Its check now fails if any of these differ. **Upgrade:** migration
  0066.
- In full, kryphocron reads and counts hide an account a takedown relay
  suspended, as public reads do.

## [0.2.0] (2026-11-09)

Full network indexing.

### Added

- **Full network indexing** (`[indexing] public_scope = "full"`): every
  public account and record the relays carry, with the history crawl
  (`[full_crawl]`), third-party labels (`[full]`), and blocks, counts and
  interaction checks read locally once an account group's history is
  complete. Plan for the space: see "Full network" in `docs/operations.md`.
- **Full trials** (`admin indexing full-trial start`, or Try full network on
  the admin Indexing page): a bounded full run that records every row it
  adds, removes them when it ends and checks the result. Limits in
  `[full_trial]`.
- **Writer lanes and relay dedupe** (`[firehose] writer_lanes`, `dedupe`):
  full needs 4 to 8 lanes.
- `admin indexing full-check`, and Full network, crawl, labels,
  suspensions and writer checks on Health; full figures in
  `admin indexing status` and metrics.
- The Blocks page shows Farsight's version.

### Changed

- The horizon-firehose sidecar is 0.2.0. Full uses its `sync` and `resume`
  entries; an older sidecar still works, with a Health warning.

### Known issues

- Full ingest needs fast disk writes: one test host applied about a third of
  what three relays sent. Watch the consumer lag on Health during a trial.

### Upgrade notes

- Migrations 0062 to 0065 (full indexing, trials, history crawl, full
  labels). Back up first: going back to 0.1.0 needs the database restored
  from before the upgrade.
- Update the horizon-firehose image to 0.2.0 (`docker/docker-compose.yml`).

## [0.1.0] (2026-11-02)

Everything before full indexing: the phases below, then this.

### Added

- **Admin fonts.** Appearance offers Atkinson Hyperlegible (default), the
  device's font or IBM Plex. **Upgrade:** start once with
  `APP_ALLOW_STARTUP_MIGRATE=1` (migration 0061). **Going back** to an
  earlier release needs the database restored from before the upgrade, or
  the 0061 row removed from `_sqlx_migrations` and
  `admin_operators.theme_font` dropped.
- **Admin: All settings** searches every setting by words or technical name,
  and offers Restart when Prism runs under a supervisor.
- **Compact storage groundwork.** Shared account ids, a fixed collection
  list and record-key, reference and record codecs, for the smaller storage
  layout coming in the next releases. Nothing is stored differently yet.
  **Upgrade:** start once with `APP_ALLOW_STARTUP_MIGRATE=1` (migration
  0054).
- **Account ids on Health.** An Account ids check and figure show how many
  of the 2,147,483,647 account ids are used, and warn past half. The stored
  records group shows dead rows for posts, profiles and other records, read
  live.

### Changed

- **Admin:** Relays, Account lookups · lodestar, Profiles and caching and
  Request limits pages; About this server keeps only facts about the
  server.
- **Admin:** What apps see, Extra services and Public page have pages of
  their own.
- **Admin:** a kryphocron page holds its settings and figures.
- **Admin:** Indexing and Storage pages; the disk, database and
  public-data checks link to them.
- **Admin:** Blocks · Farsight and Communities · Refract hold their
  settings; pending ends have their own tab; Thread Lens has a page.
- **Admin:** Members holds auto-join and the member limit (was "Most
  members"); Look up shows an account's Refract state.
- **Admin menu.** A sidebar grouped by task, with new page names (Blocks ·
  Farsight, Communities · Refract, About this server, All settings);
  Appearance sits under your name. Old admin addresses redirect. Pages use
  the full width of wide screens, and tables scroll sideways instead of
  breaking short values across lines.
- **No default storage budget.** `[storage] budget_bytes` is no longer
  200 GiB unless set. Until an operator sets one, Health shows an amber
  Storage budget check, `db_budget_ratio` is not exported and no budget
  warnings are logged; the database figures read "(no budget set)". The
  admin UI's field may be left empty for not set. The sample `app.toml`
  ships it commented out. An instance that set it keeps its value.
  **Upgrade:** an instance that relied on the old default has no budget
  until it sets one. The Health check "Database budget" is now "Storage
  budget".
- **Smaller profile table.** Accounts' stored profiles, handles and
  statuses are stored by account id, with the profile record kept whole and
  compressed in the background once 10,000 profiles are stored. In
  `searchActors`, accounts that match equally well may list in a different
  order, so a page can start or end on a different account. An account row
  whose DID is not valid is no longer kept. **Upgrade:** start once with
  `APP_ALLOW_STARTUP_MIGRATE=1` (migration 0060 rewrites the table in one
  transaction; it needs free space for a second copy of it).
- **Smaller list, starter pack, gate, poll, labeler and settings tables.**
  Lists, list entries, list blocks and opt-outs, starter packs and their
  joined counts, threadgates and postgates, polls, labeler services,
  account settings records (notification declaration, status, content
  visibility) and members' post references are stored by account id and
  record key. A record that is shown whole keeps its whole record,
  compressed in the background once 10,000 such records are stored. Lists,
  list entries, starter packs and polls made at the same moment may list in
  a different order. A list entry naming a list whose record key is not
  valid is no longer indexed; a list block whose subject is not a plain
  list address is kept but no longer blocks through that list, nor shows
  in `getListBlocks`; a post reference names only accounts with a
  valid DID. **Upgrade:** start once with `APP_ALLOW_STARTUP_MIGRATE=1`
  (migration 0059 rewrites the twelve tables in one transaction; it needs
  free space for a second copy of them). A public sweep in progress starts
  these tables again.
- **Faster reads of stored keys.** Turning a stored record key, rev or
  reference back into text is several times faster, so jobs that read many
  rows at once no longer slow down. **Upgrade:** start once with
  `APP_ALLOW_STARTUP_MIGRATE=1` (migration 0058).
- **Smaller post tables.** Posts, feed items and deleted-record tombstones
  are stored by account id and record key. A post keeps its whole record,
  compressed in the background once 10,000 posts are stored; search keeps
  its own table, for the accounts Horizon-Prism follows and members only.
  Posts and feed items made at the same moment may list in a different
  order, a repost of anything but a post no longer shows in feeds, and
  trending topics come from followed accounts' posts only. **Upgrade:**
  start once with `APP_ALLOW_STARTUP_MIGRATE=1` (migration 0057 rewrites the
  four tables in one transaction; it needs free space for a second copy of
  them). The search indexes are built again in the background; see
  `docs/operations.md` to return the space the compression leaves.
- **Smaller like, repost and count tables.** Likes, reposts and the post
  and account counts are stored by account id and record key, about a
  quarter of the space. Likes and reposts made at the same moment may list
  in a different order, and the private `getLikes` gives each like's
  `createdAt` as its `indexedAt`. Counts are kept for posts only.
  **Upgrade:** start once with `APP_ALLOW_STARTUP_MIGRATE=1` (migration 0056
  rewrites the four tables in one transaction; it needs free space for a
  second copy of them).
- **Smaller follow and block tables.** Follows and blocks are stored by
  account id and record key, about a quarter of the space. Follows created
  at the same moment may list in a different order. **Upgrade:** start once
  with `APP_ALLOW_STARTUP_MIGRATE=1` (migration 0055 rewrites both tables in
  one transaction; it needs free space for a second copy of them).
- **Indexing modes renamed.** `indexing.public_scope` is now
  `kryphocron_only` (was `off`), `community` (was `community_scoped`) or
  `full` (not built yet). **Upgrade:** change the value in `config/app.toml`
  if it is set, and start once with `APP_ALLOW_STARTUP_MIGRATE=1`: migration
  0053 renames the stored value. `admin indexing public-scope` takes the new
  names.
- **Public page counts are the server's.** The live feed's side column shows
  records received and a second, likes, posts and reposts, blocks and
  identity events, counted since Horizon-Prism started, the same for every
  viewer and kept across reloads. The per-tab counts and the missed and
  skipped notes are gone. Totals are kept across restarts, and each event is
  counted once.
- **A bad setting no longer stops the start.** An unknown, mistyped or
  out-of-range key in `app.toml` is set aside and its default applies; a bad
  feature URL turns that feature off. A missing or malformed
  `APP_PRIVATE_CURSOR_KEY_HEX` or `APP_CORRELATION_KEY_HEX` gets a key made for
  the run; a malformed optional key turns its feature off. If kryphocron's
  at-rest setup fails, kryphocron records are still indexed and encoded posts
  are served to no one until it is fixed. Each is logged (`config_set_aside`),
  counted, and listed on the admin Health page and in a banner. Only the
  hostname, database, Redis, signing key, an unreadable `app.toml`, and the
  core keys listed in `docs/configuration.md` still stop it.

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

- **Thread Lens:** a parent the ordinary thread view reports as blocked, but
  that is deleted or missing, reads as `deleted` or `not-found`, not
  `blocked`. When the chain stops at such a post, the thread's root comes
  first, marked `gapBelow`.
- **Admin:** Cancel on a confirmation returns to the page it came from, not
  Home. The health check for a relay in `firehose.private_relays` says the
  list is set in app.toml and links Running. Account and Look up links work
  for a DID holding `%`.
- The `_health` detail is shown to operators only.
- Self-labels always carry `cts`.
- Authors' self-labels show on posts, profiles and cards.
- Saving notification preferences while being removed no longer leaves them
  behind.
- Kryphocron reads no longer slow down on the smaller follow and block
  tables: their follow and block checks look up both accounts first, so an
  author feed of kryphocron posts takes under 50 ms instead of about 1 s.
- The Refract timeline builds the member's mutes and blocks once and takes
  each part's page before merging, so a page takes about 0.1 s instead of
  several seconds.
- Notifications, search, thread gates and activity subscriptions check a
  follow or block the same way, by looking up each account's id first.

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
