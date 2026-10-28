# Changelog

All notable changes to Horizon-Prism-rs (Horizon-Prism for short), in the
spirit of [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), grouped by
development phase, newest first. **Read the upgrade notes of every phase you
cross before upgrading.** Every schema migration needs one start with
`APP_ALLOW_STARTUP_MIGRATE=1` (`docker/README.md`, "Schema migrations").

Convention: new entries go under `## [Unreleased]` and move into a phase when
that phase closes. Entries say what changed for users and operators; they carry
no tracker references.

## [Unreleased]

### Added

- **Refract community list**: `app.nearhorizon.refract.getCommunities`
  lists the enabled communities and their PDS host patterns, so a client can
  skip `fill` for an author no listed community could host.
- **Thread Lens reads the thread outline when it is on.** With
  `[thread_outline]` enabled, fresh, holding the whole thread, and Farsight
  configured, `getThread` takes the parents and replies from the outline:
  their authors are judged in one round and the posts fetched in one batch,
  and Constellation is read only for the anchor's first page of replies, as
  a cross-check. What readers can see change: under each node, stored
  replies still come first, and the other replies follow oldest first (by
  `createdAt`, then URI) instead of in Constellation's order; a reply none
  of the operator's relays delivered is not found, unless Constellation
  lists it under the anchor, in which case the request falls back to
  Constellation; and the parent chain is bounded by `parentHeight` alone.
  Verdicts, the baseline and the block rules are unchanged. Anything the
  outline cannot vouch for (a thread older than its coverage, a gap it
  knows of, a stored reply it lacks, a tree over `max_tree` or a thread
  over `max_thread`) takes the earlier path for that request. A
  `moreReplies` cursor can now point into the outline's replies; an older
  release refuses such a cursor with a 400, and the client restarts that
  node. New metrics `threadlens_outline_total{use}` (`tree`, `absent`,
  `incomplete`, `hole`, `too_big`, `stale`, `bridging_off`, `crosscheck`,
  `off`, `error`: the reads failed or took over 300 ms past the baseline) and `outline_crosscheck_total{result}` (`match`, `miss`,
  `pending`, `late`); `threadlens_phase_seconds` gains an `outline` phase.
- **Thread outline index** (`[thread_outline]`, off by default). When on,
  Horizon-Prism records the reply structure of every public post the
  firehose delivers: author, record key, parent, root, time and rev, and a
  mark when deleted, with no text. It is network-wide post ingest, for
  every author and whatever `public_scope` says (about 3 million posts a
  day), so it costs about 17 GB of disk at the default 30-day window;
  older rows are deleted by the daily retention run. Thread Lens uses it
  to know a thread's shape up front (above).
  Ingest never waits on it: if its writer falls behind, it catches up from
  the stream, and what the stream no longer holds restarts its coverage.
  One process writes it at a time. Migration 0050 adds its tables, so this
  upgrade needs one start with `APP_ALLOW_STARTUP_MIGRATE=1`. New metrics
  `outline_ops_total{op}`, `outline_holes_total{reason}`,
  `outline_missed_total{reason}`, `outline_replays_total{why}`,
  `outline_flush_seconds`, `outline_queue_ops`, `outline_actor_ids`,
  `outline_writer_active`, `outline_complete_since`,
  `outline_through_age_seconds` and `outline_freshness{state}` (1 for the
  current state: `fresh`, `inactive`, `resuming`, `replaying`, `behind`
  or `no_live_relay`).
- **Thread Lens takes a handle-form anchor.** `getThread` accepts
  `at://<handle>/app.bsky.feed.post/<rkey>` (any case) and resolves the
  handle itself, as `resolveHandle` does, so a client opening a thread from
  a `/profile/<handle>/post/<rkey>` link saves a round trip. The answer's
  `anchor` is always DID-form. A handle that does not resolve is
  `400 HandleNotFound`; a resolver failure `502 UpstreamFailure`.
  `getMembership` lists `threadLens.handleAnchor`. Without Lodestar, a
  handle Horizon-Prism has not stored is resolved by DNS and HTTPS probes,
  which can take up to about 3 s of the request.
- **Thread Lens marks quote posts whose card is whole.** A post node now
  carries `embedWhole: true` when its quote card is what the reader's own
  AppView would show (but for the reader's mutes, which the client applies):
  the post view came from an upstream AppView's answer for this request, the
  quoted post's author has no block with the reader as Farsight vouches, the
  quoted post quotes nothing itself, and every labeler the reader accepts
  was applied. Clients that skip their own re-read for whole views can now
  skip it for these quote posts too. Posts Horizon-Prism builds itself, cards
  for feeds, lists, labelers and starter packs, and quotes of posts the
  upstream could not find are not marked. New metric
  `threadlens_labelers_unapplied_total{network}` counts upstream answers that
  left out a labeler the reader accepted.
- **Refract actor search**: `app.nearhorizon.refract.searchActorsTypeahead
  {q, limit}` and `searchActors {q, limit, cursor}` find restored accounts by
  the start of their handle or display name (case and diacritics ignored),
  shaped as Bluesky's actor searches and with Refract's usual rules; listed in
  `getMembership.features` as `refract.search` while Refract is on.
- **System accounts**: a private list of the other accounts of a plural
  system, kept by Horizon-Prism the way an AppView keeps mutes and readable
  only by its owner. `app.nearhorizon.account.getSystemAccounts`,
  `addSystemAccount {account, proof}`, `removeSystemAccount {account}`,
  `getSystemAccountStatus` and `leaveSystemAccounts`, for any signed-in
  account, member or not; listed in `getMembership.features` as
  `account.systemAccounts`. Adding an account takes a proof from that
  account's own server; the three writes and the proof take tokens that live
  at most 65 s and are accepted once. Listed accounts are sealed at rest and
  never logged; lists go when an account is deleted. Up to 31 accounts per
  list; rate class `[rate_limits.system_accounts]`.

### Changed

- **`refract.fill` answers most new DIDs as final on the first call.** It
  waits up to 300 ms for the identity resolves it starts. A DID hosted by no
  listed community is then left out as final, not listed in `pending`, so a
  client skips its re-ask. A resolve still running at 300 ms stays `pending`
  as before.
- **Thread Lens names a labeler as applied only when every upstream answer
  applied it.** `atproto-content-labelers` on `getThread` and `resolve` used
  to name a labeler if any upstream answer of the request said it applied
  it; now every answer has to (Horizon-Prism's own labeler is still always
  named when accepted). A thread served from the record-level fallback no
  longer counts as fetched with the reader's labelers, so its posts get the
  reader's labels from the label read, as posts Horizon-Prism builds do.
- **Thread Lens says replies are allowed on threads without a threadgate.**
  A thread read from the upstream AppView whose answer names no threadgate
  now carries `viewer.replyDisabled: false` on its posts, instead of leaving
  it out; clients that skip their own re-read when prism's views are whole
  can now skip it for ordinary threads.
- **Bluesky's AppView is now `api.bsky.app`** (was `public.api.bsky.app`) in the
  shipped configuration. The public endpoint applies only Bluesky's own
  labeler and ignores a reader's others, so Thread Lens and other reads could
  never report the reader's labelers as applied, and clients had to re-read
  every post. Operators with their own `config/app.toml` should make the same
  change to the `bluesky` network's `appview_url`.
- **Thread Lens falls back less often.** Horizon-Prism keeps one connection to
  Farsight open (a bare request every 30 s), so a thread never waits for a
  new DNS, TCP and TLS setup, which through a CDN could spend the whole budget
  of a block check and show the thread without bridging. The default
  `threadlens.verdict_round_timeout_ms` is now 800 (was 500). New metric
  `threadlens_bridging_total{state, reason}` says why bridging was on or off
  for each request (`vouched`, `answered_partial`, `unanswered`,
  `no_farsight`).
- **Thread Lens keeps bridging while Farsight reports partial coverage** (for
  example while it repairs a short firehose gap). Bridging is now off only
  when Farsight does not answer in time, refuses or is overloaded. An author
  Farsight flags for that pair (a list it cannot read) is still not bridged,
  and a partial answer still never clears a block Horizon-Prism already
  knows.
- **Admin UI sessions last much longer**: 30 days unused (was 8 hours) and a
  year in all (was 7 days), so an operator can stay signed in like on any
  Bluesky client. A session still ends at once if the operator is removed or
  their account moves to a server that is not allowed. Set
  `admin.session_idle_secs` / `admin.session_max_secs` to shorten them.
- **A system-accounts key problem no longer stops Horizon-Prism.** With lists
  stored and `APP_SYSTEM_ACCOUNTS_KEY_HEX` missing or not the key they were
  sealed with, Horizon-Prism starts and only system accounts are off: the five
  methods answer 501 saying the lists are kept, `getMembership` leaves the
  feature out, the log and the Health page say what to fix, and nothing is
  added to or removed from the lists (a deleted owner's own list still goes).
  Account events seen meanwhile are recorded. With the right key back,
  accounts deleted meanwhile leave every list, the others on a list are
  checked as usual, and the methods serve again once that is done.

### Changed (private audiences held by Horizon-Prism)

- **Members' private audiences are held by Horizon-Prism, not read from
  repositories.** Audience lists are no longer public records: four
  owner-only methods hold them, and `getMembership` lists
  `private.audiences`.
  - `app.nearhorizon.private.createAudience {record}` → `{uri, cid}`: the
    record key is minted here (an input `rkey` is refused). At most 200
    audiences per member (`LimitReached`).
  - `updateAudience {uri, record, swapCid}` → `{uri, cid}`: replaced only if
    `swapCid` is the stored token (`InvalidSwap`). Any mode may become any
    other.
  - `deleteAudience {uri}` → `{}`, idempotent; posts naming the audience
    become readable by their author alone.
  - `listAudiences {limit, cursor}`: the caller's own, newest first; a page
    also ends, with a cursor, once it holds 20 000 members.
  - The record check: `mode` one of `list`, `everyone`, `followers`,
    `following`, `nobody` (absent reads as `list`); `name` 1–64 graphemes and
    at most 640 bytes once trimmed; `members` valid DIDs of at most 256
    bytes, de-duplicated, the caller dropped, at most 1000; `createdAt` and
    `updatedAt` required; a body of at most 512 KiB. `cid` is an opaque
    version token, not a CID.
  - `createAudience` and `updateAudience` need a ready member; `deleteAudience`
    and `listAudiences` any member, so a list can always be withdrawn. All
    four are in the `private` rate class, and answers are `private, no-store`.
  - A change is read at the next request. A member's first private row
    still waits for its `private` job before its posts are served.
- **A `tools.kryphocron.policy.audience` record in a repository is ignored
  everywhere**: the firehose, backfill jobs, catch-up, `requestRecord`
  (`out_of_scope`) and dead-letter replays. Such ops count as
  `firehose_ops_skipped_total{reason="kryphocron_unproduced"}`, and the admin
  live feed shows them as `other`. Jobs and catch-up no longer list
  audiences, and settles are covered by the post listed rev alone.
- **The stored audiences are kept** at their URIs, so no post changes who can
  read it. An existing audience's first token is its old repository CID; its
  `createdAt` and `updatedAt` read as the time it was first stored.
- **A member's removal now deletes its audiences, for good**: Horizon-Prism
  holds the only copy, so a member who is added again starts with none,
  whatever ended the membership. An account deletion deletes them as before.
- The private-tier event-age gauge no longer counts audiences, which are
  not delivered by relays.

### Upgrade notes

- **Private audiences (migration 0052).** Before this upgrade, prove that the
  stored audiences are current, then start it at once:
  1. Deploy the build at commit `d96192c` first, the only one with
     `admin audiences check` (this release removes it). It still reads
     audiences from repositories and needs no migration.
  2. If your members post through a PDS that checks private audiences
     itself, upgrade it first to a release that accepts audiences it cannot
     find in the author's repository.
  3. Pull or build this release.
  4. Pause audience edits, where you can.
  5. In the running `d96192c` container, run `admin audiences check` until it
     exits 0 (it writes nothing). For each member it compares the
     repository's `tools.kryphocron.policy.audience` records with the stored
     audiences and prints one outcome:
     - `clean`, or `refused` (records no ingest would store, listed): passes;
     - `mismatch` (a missing, extra or differing row): fails. Clear it with
       `admin backfill <did> --depth authz`, wait for the job to finish, and
       run the check again;
     - `unchecked` (the repository could not be read): fails. Run again, or
       pass `--accept-unchecked <did>`;
     - `inactive` (tombstoned, no PDS, repository not found or not active):
       passes with no stored audiences, and fails with some unless named
       with `--accept-unchecked <did>`.

     Accepted DIDs, and refused records with a stored row, are printed as
     unproven. `--dids` re-checks only the named members (a DID that is not
     a member fails the run). The consumer's lag, audience and whole-entry
     dead letters and open jobs are printed but never fail it; drop the
     audience dead letters with `admin deadletter drop <id>` once it passes.
  6. Start this release straight after, with `APP_ALLOW_STARTUP_MIGRATE=1`.

  Migration 0052 drops `sync_state.audience_listed_rev`, so it cannot be
  rolled back by changing the image. It deletes the audiences of accounts
  that are not members (they served no one, and would have gone live with
  stale members at a later join), reads an unknown `mode` as `nobody` (as
  it already read), and trims names. An audience whose stored name, member
  count or member DIDs exceed the record check is served as stored, and must
  be fixed in an update before one passes.
- Migrations 0048 (system accounts) and 0049 (account events kept while the
  key is missing): one start with `APP_ALLOW_STARTUP_MIGRATE=1`.
- System accounts are off until `APP_SYSTEM_ACCOUNTS_KEY_HEX` is set (64 hex;
  `admin keygen` prints one). The shipped compose passes it through; a
  compose override must pass it too. Once lists are stored, keep the key and
  back it up: with another key or none, system accounts stay off until it is
  back (the rest of Horizon-Prism runs).

### Fixed

- **The `_health` detail is for operators only.** The full body (db, redis,
  audit guard, relay watermarks, public indexing) went to any account with a
  verifying `_health` service JWT. The watermarks show how far processing has
  got, which an outsider could use to learn about members. Now only an account
  in the operator list gets the detail; everyone else gets `ok` and `version`.
- **Self-labels always carry `cts`.** A self-label on a record with no
  `createdAt` (some profile records have none) went out without `cts`, which
  `com.atproto.label.defs#label` requires, so strict clients refused the
  whole view; in a private thread the reply's parent disappeared. `cts` is
  now the record's `createdAt`, else its indexed time (as Bluesky's AppView
  does), else the Unix epoch.
- **Authors' self-labels now show on posts, profiles and cards.** A post's,
  profile's, or feed/list/labeler/starter-pack card's own content warnings (`com.atproto.label.defs#selfLabels` in the
  record, e.g. nudity or porn) are now in the view's `labels` with `src` the
  author, as Bluesky's AppView shows them; before, prism removed them from
  upstream views and never added them to its own, so clients that moderate
  from `labels` showed such posts without their warning.
- Saving private notification preferences while being removed from the
  community no longer leaves those preferences behind for a later rejoin; the
  save answers `NotEnrolled` instead.

## Phase 9: Pronouns and plural systems (2026-10-24)

Any account's pronouns and plural-system records are readable from
Horizon-Prism, and posts carry their fronters.

### Added

- **`app.nearhorizon.plural.getRecords {actors?, members?}`**: for any account,
  member or not, its `app.nearhorizon.actor.pronouns`, `plural.system`,
  `plural.front` and named `plural.member` records, so a reader's browser never
  asks an author's server. Each answer is `record`, `none` or `unknown`; `none`
  only when the account's repository was listed and the firehose is within
  `plural.max_stream_lag_secs`. Up to 50 accounts and 100 member URIs a call; no
  sign-in needed; short public `cache-control`.
- **`app.nearhorizon.plural.listMembers {actor, limit, cursor}`**: an account's
  members in record-key order, with `status` `complete`, `partial` or `unknown`.
- The four collections are stored from the firehose for any author, in every
  public-indexing mode. The first time an account is asked about, its repository
  is listed once and reconciled; it is listed again after
  `plural.reverify_hours`. Bounded by the `[plural]` concurrency and rate keys.
- **Fronters on private posts**: the clear `app.nearhorizon.plural.fronters` key
  of a private post is validated, stored and returned in its view (and in quotes
  of it); an invalid value is dropped and counted, and the post still ingests.
  Existing private posts are listed again once to fill it. Public post views
  already kept the key.
- Rate classes `plural_anon` (per IP, 1/s, burst 10) and `plural_auth` (per
  account, 5/s, burst 30); a token that does not verify is refused.
- `getMembership.features`: `plural.fronters` (always) and `plural.read` (while
  `plural.enabled`).
- New `[plural]` config section; `enabled = false` answers both methods 501.
  Metrics `plural_*`. Lexicons `app.nearhorizon.plural.defs`, `getRecords`,
  `listMembers`.

### Upgrade notes

- Migration 0047 (`plural_records`, `plural_repos`, `private_posts.fronters`).

## Phase 8: Private extras (2026-10-21 – 2026-10-23)

Notification preferences, activity subscriptions, list blocks and appearance for
the private tier; public roots' hidden replies; private blocks retired.

### Added

- **Private notification preferences**:
  `app.nearhorizon.private.getNotificationPreferences` and
  `putNotificationPreferences`, per reason (`like`, `repost`, `reply`, `quote`,
  `mention`, `subscribedPost`): whether it is listed, and whether only followed
  accounts count. Applied when read, so switching a reason back restores every
  retained notification. Feature `private.notificationPreferences`.
- **Private activity subscriptions**: `app.nearhorizon.private.putActivitySubscription`
  and `listActivitySubscriptions`, separate from the public bell (cap
  `[private_notify] activity_subscriptions_max`, default 1000). A new private
  post notifies each subscriber who can read it with a `subscribed-post` private
  notification, seconds after it is stored; subscribers outside its audience get
  nothing, and nobody is notified twice. `getUnreadCount {reasons}` takes
  `subscribed-post`. Feature `private.activitySubscriptions`.
- **Public root gates**: a public thread root's threadgate is kept (fetched once
  from its author's PDS when needed) while private replies claim that root, and
  its `hiddenReplies` hide those replies, with what is beneath them, from
  `private.getPostThread` and `getReplies`, except for the reply's own author.
  Feature `private.publicRootGates`.
- **Private appearance**: `app.nearhorizon.private.getAppearance` and
  `putAppearance` keep where a member's appearance is saved (`private`, `public`
  or `device`) and the appearance itself (up to 16 KiB), with `rev` swaps.
  Feature `private.appearance`.
- **Appearance retention**: a removed member's appearance is kept 7 days for the
  account alone (`getAppearance {includeRetained}`, `retainedUntil`, and the new
  `releaseAppearance`). Feature `private.appearanceRetention`.
- `lexicons/app/nearhorizon/actor/appearance.json` defines the
  `app.nearhorizon.actor.appearance` record (definition only: clients read and
  write it on the account's PDS).

### Changed

- **List blocks count in the private read rule.** A member's list block of a
  moderation list blocks like a direct block, both ways, wherever private reads
  test one: posts, replies, reposts, notifications and counts. A reply blocked
  this way is read by its author only, with `notServed: ["standing"]`. Feature
  `private.listBlocks`.

### Removed

- **Private blocks** (`tools.kryphocron.graph.block`) are no longer tracked: a
  public block already keeps two accounts apart. They are skipped on ingest;
  only `app.bsky.graph.block` and applying list blocks count as blocks in the
  private tier. The admin pages' private-block figures are gone.

### Upgrade notes

- Migrations 0037 (notification preferences), 0041 (activity subscriptions),
  0042 (public root gates; seeds a one-time fetch queue that drains after
  deploy), 0043 and 0044 (appearance and its retention), 0045 (drops the
  `private_blocks` table and its rows).

## Phase 7: Refract (2026-10-22 – 2026-10-23)

Refract serves accounts that Bluesky's AppView withholds but a configured
community still hosts and has not banned ("restored" accounts), from
Horizon-Prism's own storage.

### Added

- **`[refract]` config**: `enabled` (off by default) and `communities`, both live
  settings validated whole on every save, plus restart keys for checks, labels,
  backfill and discovery caps.
- **The checker**: a candidate is restored when Bluesky withholds it, a fresh
  resolve finds it on an enabled community's PDS, it is active there
  (deactivation starts a 30-day grace), its community has not labelled it gone,
  and it has no local takedown. Labels are signature-checked and polled. Changes
  to the communities apply live, with a delay before restored accounts end.
- **Storage**: a restored account's posts, interactions, graph records, gates and
  profile are admitted in every mode, backfilled in full by
  `refract.backfill_concurrency` dedicated workers, re-synced weekly, and spared
  by the scope sweep and retention. When a restore ends, a dedicated purge
  removes them, never members' rows.
- **Endpoints**, for any signed-in caller (ready members with their viewer state,
  everyone else as a visitor), in their own `refract` rate class:
  `app.nearhorizon.refract.fill`, `getPostThread`, `getAuthorFeed`,
  `getTimeline`, `getReplies`, `listNotifications`, `getUnreadCount` and
  `getAuthorReposts`. Every answer carries `servedAs`; `fill` reports `pending`
  and `withheld` (`unanswered`, `capped`, `proxied`, `blocked`, `unavailable`).
  Feature `refract`.
- Restored profile and post views carry `restored {community, tier, labeler}` in
  every response; their media and video come from their PDS.
- Restored authors' live records notify members. `getReplies` also returns other
  accounts' direct replies to restored replies, under the root's threadgate
  (fail closed) and both sides' blocks.
- Reposts of restored posts are stored from anyone and served on `getTimeline`
  and `getAuthorReposts`; older ones are filled once per restore from
  Constellation backlinks.
- **Visitor viewer state** (a visitor's own likes, reposts, follows and
  `replyDisabled` on restored views) behind the live switch
  `refract.visitor_viewer_state`, **off by default**, since the lookups send the
  reader's DID to Constellation; `viewerChecked` says what was looked up.
- Admin: Communities and Refract accounts pages (add, check, remove, keep or
  release pending ends) and `admin refract status|list|show|check|end|keep|unkeep`.
- Metrics `refract_*`, including `refract_read_seconds{method}`.

### Changed

- **The moderation gate also runs after every upstream fetch**, for every caller:
  nested posts, quotes and embeds by hidden or blocked authors, or with a local
  `!takedown` or `!hide`, are no longer served inside upstream views. This also
  changes public answers with public indexing on.
- The cascade's `getPosts` asks each author's network concurrently (up to 8),
  under the request's deadline.
- Thread Lens fetches a restored author's missing post through its community and
  marks it.

### Fixed

- Restored post views send the earlier of `createdAt` and the indexing time as
  `indexedAt`, so clients no longer show backfilled posts as archived.
- `fill` drops malformed items instead of failing the call, remembers accounts no
  community hosts for 24 hours, reports an account restored mid-request as
  `pending`, no longer charges a repeated resolve twice, and walks cold profiles
  in parallel within the deadline.
- The Refract lexicons declare every error their methods return.

### Upgrade notes

- Migrations 0040 (Refract tables) and 0046 (`refract_dids.reposts_filled_at`;
  accounts already restored are filled after deploy). The post-migration index
  task builds a new large-table index, `reposts_subject_did`.

## Phase 6: Resilience (2026-10-16 – 2026-10-22)

The firehose keeps up under load, private reads keep working while a relay is
behind, and clients are told when to retry.

### Added

- **Degraded mode**, while the private relay is behind:
  - Each tracked repository keeps a `chain_rev` (every commit up to it is held),
    from the `since` the firehose sidecar now sends; a dropped commit is caught
    up by another relay or a settle.
  - Private reads check every repository they depend on against its stored head
    instead of missing as stale (`private_read.head_fallback`, on by default).
  - Private read responses carry `degraded`, `retryAfter` and `unavailable` (the
    caller's own posts not servable yet); `getPostThread` answers
    `503 Unavailable`, and an unshowable quoted private post is
    `#viewUnavailable`. Keys `private_read.degraded_retry_min_secs` and
    `_max_secs`. Feature `private.degraded`.
  - A member whose readiness was lost after it was first ready gets
    `503 Reindexing` with `retryAfter` instead of `400 NotReady`, and
    `getMembership` answers `reindexing` (`community.reindex_retry_secs`).
    Feature `private.reindexing`.
- **Every `429` carries `Retry-After`**, the admin UI's limits included.
- A warning, in the log and on Health, when a private relay has never reported
  or is more than 5 minutes behind, since private reads wait for it.
- Firehose throughput metrics and alert examples (`FirehoseBacklogPersistent`,
  `FirehoseConsumerSlowerThanInput`, `FirehoseGroupMissing`,
  `FirehoseSamplerFailing`).

### Changed

- `firehose.batch_size` defaults to 2000 (was 500): more throughput under a
  backlog, no change at live rates.
- A commit that can write nothing (most of the firehose) opens no transaction.
  After a trim gap the consumer re-anchors at the oldest entry unless it has
  measured itself slower than its input.
- Private `getPostThread` and `getReplies` cap one reply expansion's reads at
  `THREAD_NODE_CAP × private_read.page_reads_factor` (400 by default), whole
  nodes only; a parent past the cap comes back without `replies`, to be read with
  `getReplies`.
- The test suite builds one migrated template database per run (about 40 %
  faster), and load-sensitive tests decide on events rather than timing.

### Fixed

- The firehose sidecar no longer replays from a stale cursor after a keepalive
  reconnect, which held the watermark back and made private reads miss as stale.
- A missing consumer group is recognised on every read, and a recreation gap can
  no longer be lost.
- `sync_state.diverged_since` clears once the applied rev reaches the probed head,
  so a just-ready member no longer shows as diverged or loses readiness early.

### Removed

- The disabled `community-b` network entry (its URL was a PDS, not an AppView),
  from the built-in defaults and `docker/config/app.toml`. The default
  `constellation.cascade.order` is now `["bluesky", "community-a"]`.

### Upgrade notes

- Migration 0039 (`chain_rev`).
- Rebuild the firehose sidecar images: `docker/docker-compose.yml` pins a newer
  horizon-firehose (the stale-cursor fix, then `since` on each commit).
- A config that still lists `community-b` in `constellation.cascade.order` fails
  validation; remove it.

## Phase 5: Thread Lens (2026-10-13 – 2026-10-24)

Thread Lens for every signed-in account, hidden replies flagged, faster hydrated
views, the reader's labelers, and walking up past blocks.

### Added

- **`threadlens.access`**: `signed_in` (default) serves every signed-in account,
  non-members as visitors with their own blocks (from Farsight and stored rows)
  applied; `members` serves ready members only. Live from the admin UI. Visitors
  draw on Farsight's non-member share and never trip its breaker. Features
  `threadLens` and `threadLens.signedIn`.
- `hiddenByThreadgate: true` on nodes the root's threadgate hides (never the root
  or the caller's own posts), with bridging on or off; such replies are kept and
  flagged rather than dropped.
- `blockingByListUri` on a `blocked-by-you` gap whose block comes through one of
  the caller's own list blocks.
- **Hydrated views**: `servedAs` (`member` or `visitor`), a member's own viewer
  state, counts, and `viewer.replyDisabled` decided from the root's threadgate
  and local rows. Feature `threadLens.hydrated`.
- **The reader's labelers**: `atproto-accept-labelers` is honoured for every
  labeler it names, moderation or not (at most 32); views keep those labels
  beside the operator's, and `atproto-content-labelers` names them. Labels only
  add to views. Feature `threadLens.labelers`.
- `threadlens.listing_timeout_ms` (default 1500): a slow reply page no longer
  holds the response; it finishes in the background, and the node keeps its
  `moreReplies` cursor. Metrics `threadlens_phase_seconds{phase}`,
  `threadlens_listing_timeouts_total`.

### Changed

- `getThread` is about three times faster on a bridged thread: one upstream
  fetch, concurrent reply pages, a batched fetch ladder and batched counts.
- A post labelled `!warn` upstream is a post (upstream labels removed), not
  `content-stripped`; `!takedown` and `!hide` still are.
- `getMembership.features` lists the Thread Lens values even with the private
  tier disabled.

### Fixed

- The parent walk goes on up past a blocked post that is not stored locally
  (reading only its `reply.parent.uri`), so the client shows a placeholder for
  the blocked post and the thread above it.
- A blocked top gap carries `parentUri` whenever the walk knows it, even when the
  walk stopped at a bound.
- With bridging on, reply expansion no longer re-adds replies the root's author
  hid as ordinary replies.
- The `blocked` gap's lexicon text says what it means: a block the ordinary view
  reported that was not looked past.

### Upgrade notes

- Thread Lens now serves every signed-in account by default; set
  `threadlens.access = "members"` to keep it to members. An operator with many
  visitors may need to raise `farsight.rate_per_sec`.

## Phase 4: Operator tooling and community scope (2026-10-18 – 2026-10-22)

A web admin UI, membership decided by the operator, scope that follows recent
activity, fewer stored blocks, actionable warnings, and Lodestar identity reads.

### Added

- **Web admin UI** under `/admin`, served by `serve`: first-run setup with a
  one-time code, operator sign-in with ATProto OAuth, CSRF checks and typed
  confirmations for destructive actions. Pages: Home (what needs attention,
  tiles, and live tickers of what the firehose delivers and what is indexed),
  Members (add, remove, deny, allow, re-sync, Farsight and list actions), Health,
  Jobs, Problems, Stats (disks, indexed counts, endpoint usage), Blocks, Labels,
  Look up, Audit log, Operators, Settings, and a per-operator Appearance (light
  or dark, colour scheme).
- **Saved settings**: values saved in the UI are layered over `app.toml` (never
  written), validated whole and kept with their history. A saved value wins over
  the file; a later file change under it is offered to the operator; a saved
  value that no longer validates after an upgrade is set aside at startup. Live
  settings apply at the next request; the rest wait for a restart, which the UI
  and `admin restart` can perform when `APP_SUPERVISED=1`.
- CLI: `admin operator add|remove|list`, `admin operator host add|remove`,
  `admin setup-code`, `admin settings list|get|set|reset`, `admin restart`,
  `admin member deny|allow`, `admin member list --denied`,
  `admin deadletter sweep [--dry-run]`, `admin identity compare`.
- **Denials**: `admin member deny` writes a denial and removes a member in the
  same step; auto-enroll refuses a denied account; `admin member allow` lifts it.
- **Never-ready expiry**: auto-enrolled members that never finish their first
  indexing are removed after `community.never_ready_expiry_days` (7; 0 = off).
  Operator-added and once-ready members are never expired.
- **Lodestar identity reads** (`identity.lodestar_url`, empty = off): public
  request paths read DID documents and verified handles from a Lodestar mirror of
  the PLC directory first, only while it reports itself current, falling back to
  the PLC directory and Horizon-Prism's own handle check; pages that route many
  accounts batch their lookups. Sign-in keys, admin, auto-enrollment, the
  identity refresher, backfill and list syncs never use it.
  `admin identity compare` checks it against the directory.

### Changed

- **Membership is the only owner gate**: hosting on a community PDS no longer
  makes an account a private owner, so only members' private posts are served.
- **`community.auto_enroll` defaults to `false`**: hosting on a community PDS no
  longer makes anyone a member unless the operator turns auto-join on.
- **Scope recency**: a member's like or repost grows scope only within
  `scope.like_repost_days` (30) of its date, a reply or quote within
  `scope.reply_quote_days` (90). A nightly scope check, an hour before retention,
  removes accounts nothing holds any more (membership, a follow within the cap,
  a recent interaction, an activity subscription), at most
  `scope.sweep_max_purges` a night. Activity subscriptions hold scope but never
  grow it. The weekly rebuild keeps only the counts repair and stale re-syncs.
- `scope.rebuild_grace_days` defaults to 0 (was 7) and must be 0 to 30.
- **Blocks are stored only with a member on one side**; retention removes stored
  blocks and list blocks with no member side, and a removed member's blocks of
  non-members.
- Sync jobs of accounts that are deleted, taken down, deactivated or tombstoned
  end `unavailable` after one try and are queued again when the account comes
  back; `scope` and `resync` jobs retry for about 41 hours before failing.
  Health's "Failed jobs" counts only jobs worth acting on.
- Malformed commits and records from accounts Horizon-Prism would not keep are
  counted and dropped instead of dead-lettered.

### Fixed

- A tombstoned `did:plc` (the PLC directory's `404 DID not available`) is
  recognised as tombstoned: the account is marked deleted and purged, and list
  syncs stop retrying it.
- Admin forms no longer fail with "This form expired" (`Referrer-Policy:
  same-origin`).

### Removed

- The community-PDS eligibility confirmation for private owners, with its
  columns and `eligibility_cleared_total`.

### Upgrade notes

- **Before migration 0031**, run the read-only queries in `docker/README.md`
  ("Before the 0031 upgrade") and `admin member add` every non-member owner whose
  private posts should stay served; anyone left off goes dark.
- **Auto-enroll is off unless set**: a deployment whose `app.toml` does not set
  `[community] auto_enroll` stops auto-enrolling at the upgrade. Set
  `auto_enroll = true`, or switch Auto-join on in the UI, to keep it. Existing
  members are unaffected. The first expiry run removes auto-enrolled members that
  waited longer than the window without becoming ready.
- **The first nightly scope check removes every account nothing holds any
  more**, mostly accounts held only by old interactions;
  `admin scope rebuild --dry-run` shows how many. A deployment whose own
  `app.toml` sets `rebuild_grace_days = 7` keeps the old slack until it changes
  it; above 30 the app does not start.
- An upgraded deployment has no operator and enters first-run setup: read the
  code with `docker compose logs app | grep admin_setup_code`, or run
  `admin operator add`. A deployment with its own Caddyfile proxies
  `/admin/live` without `encode` and with `flush_interval -1`. The shipped
  compose file sets `APP_SUPERVISED=1` and mounts the database and Redis volumes
  read-only for the disk figures.
- Run `admin deadletter sweep` once to clear dead letters that are now dropped.
- Migrations 0031 (membership), 0032 (admin UI), 0033 (scope recency index),
  0034 (saved settings), 0036 (unavailable jobs; queues failed `scope` and
  `resync` jobs again), 0038 (admin appearance).

## Phase 3: Private posting (2026-10-12 – 2026-10-21)

The private tier becomes a full posting surface: audiences, content,
interactions, notifications, threads, search, encoded content from any PDS, and
fast reads.

### Added

- **Audiences** `followers`, `everyone`, `nobody` and `following` beside `list`;
  `following` is read by the accounts the owner follows, once those follows are
  verified against the owner's repository. A block either way keeps a reader out
  in every mode.
- `app.nearhorizon.private.getTimeline`, the private home timeline.
- **`app.nearhorizon.actor.getMembership`**: whether the caller is a member
  (`member`, `indexing`, `notEligible`), the `features` this deployment serves,
  `validForSecs`, and `permissions {privateRead, privateIndexed, threadLens}`;
  never cached.
- Private post views carry facets, embeds (stock `#view` shapes; quoted private
  posts read for the same reader), langs, labels and `audience`. Feature
  `private.embeds`.
- Private bookmarks: `createBookmark`, `deleteBookmark`, `getBookmarks` and
  `viewer.bookmarked`. Feature `private.bookmarks`.
- Threadgates and postgates on private posts: `threadgate`, `viewer.replyDisabled`,
  hidden replies, `viewer.embeddingDisabled`, detached quotes. Feature
  `private.gates`.
- Counts and viewer state on private posts (counting only what the reader may
  see), `getLikes`, `getRepostedBy`, `getQuotes`, reposts in timelines and author
  feeds, and thread mutes of private roots. Feature `private.interactions`.
- **Private notifications**: `listNotifications`, `getUnreadCount` (with
  `reasons`) and `updateSeen`. Feed items carry their reply context and
  notifications their subject post, read for the caller, so a client needs no
  extra `getPosts`. Features `private.notifications`, `private.unreadReasons`,
  `private.replyContext`, `private.notificationSubjects`.
- `index.requestRecord` catches the caller's own repository up inline and answers
  `caughtUp`; when `true`, private reads reflect the record at once. Feature
  `private.freshIndex`.
- Sealed private feed cursors; a `filter` on `getAuthorFeed`
  (`posts_with_replies`, `posts_and_author_threads`, `posts_with_media`,
  `posts_with_video`); `app.nearhorizon.private.getReplies`. Features
  `private.paging`, `private.mediaFilters`.
- `app.nearhorizon.private.searchPosts`: a query-time scan of the recent private
  posts the caller may read; no index of decoded text is stored. Feature
  `private.search`.
- **Private threads**: a reply is read by its thread root's current audience (or
  its branch root's, under a public post) while its writer and every writer above
  it may write there, within `[private_read] max_chain` (16) posts. Feature
  `private.threadAudience`.
- **Encoded content** may carry text, facets and embed together. Clear `quote`,
  `mentionDids` and `mediaKinds` fields are indexed without decoding; a post whose
  clear fields claim what its content does not carry is a miss for everyone but
  its author. The author's own view lists `droppedFields`, and `notServed`
  (`placement`, `standing`, `chainCap`, `overClaim`) when no reader would be
  served. Features `private.anyPds`, `private.notServed`.
- Read-latency metrics: `xrpc_request_seconds{nsid}`,
  `private_read_stage_seconds`, `sync_head_check_seconds{path}`,
  `private_read_empty_page_total`.

### Changed

- **Plaintext private posts are no longer served** while
  `[private_any_pds] enabled = true` (the default): a `postPrivate` record without
  `encodedContent` is a miss for every reader, its author included. Encoded
  private posts from any PDS are served.
- A reply under another account's audience is served, on any PDS, only when its
  chain reaches the owner's post; the same-PDS shortcut is gone.
- A private video always plays from its author's PDS; `[video] playback` and
  `cdn_url` govern public videos only.
- Private-read timing floors default to 0 (off).
- A paged private read examines at most `page_reads_factor` × `limit`
  candidates; a short or empty page can carry a cursor, so keep paging while one
  comes back.
- Private read statements walk each chain row once (much faster on long chains),
  and JIT compilation is off on every database connection.
- An update of a private post notifies only new recipients, and only while the
  post is recent.
- The codec stops decoding at the 1 MB payload cap.
- kryphocron 0.4.4 and kryphocron-lexicons 0.3.1; clear fields are derived with
  kryphocron's own function, the one clients encode with.
- CORS preflights are cached for a day.
- `sync.head_max_age_secs` above 600 is refused at startup.

### Fixed

- Public notifications no longer name a private post.
- A replayed, since-deleted audience or private post can no longer be stored
  below a settle and grant read access.
- Readiness can no longer be set after a gap cleared it.

### Upgrade notes

- **Set `APP_PRIVATE_CURSOR_KEY_HEX`** (`openssl rand -hex 32`, the same on every
  replica) before upgrading; without it the app exits 78.
- **Plaintext private posts go dark.** Count them by PDS first:
  `SELECT a.pds_endpoint, count(*) FROM private_posts p LEFT JOIN actors a ON a.did = p.author_did WHERE p.encoded_content IS NULL GROUP BY 1`.
  `[private_any_pds] enabled = false` serves them again.
- Upgrade before a PDS you index starts writing the encoded-content form or
  `mediaKinds`; an older version leaves such posts out, or out of the media
  filters, until they are written again.
- An `app.toml` that lists `[private_read.floor_ms]` keeps those floors until
  the section is removed. Lower `sync.head_max_age_secs` to 600 or less.
- Migrations 0023 (display columns; queues a `private` job per account to fill
  them), 0024 (bookmarks), 0025 (gates), 0026 (quote references), 0027
  (notifications), 0028 (verified follows), 0030 (listed revisions), 0035 (media
  kinds).

## Phase 2: A complete AppView (2026-10-11 – 2026-10-18)

The rest of the `app.bsky.*` read surface, members' list mutes and blocks with
Farsight, Thread Lens v2, upstream budgets, and running without public indexing.

### Added

- **The final router**: CORS on every `/xrpc/*` response (browser clients can
  call directly), `501 MethodNotImplemented` for unknown methods, a proper
  wrong-verb `400`, honest `501 NotImplemented` routes for deferred features,
  `Retry-After` on `NotReady`, and `/did.json` plus `#bsky_notif` in the DID
  document.
- Deferred services (`contact`, `age_assurance`, custom ones) can be forwarded to
  another server (`[services.<name>] state = "proxy"`), with token checks, header
  filtering, rate limits and size caps.
- **Twelve more collections** (lists, list items, list blocks, opt-outs, starter
  packs, labeler declarations, self records, threadgates, postgates, polls),
  with backfill, retention and purges; search, trend, `via` and profile columns,
  filled for old rows by a post-migration task; a one-time re-sync.
- Lists and starter packs: `getList`, `getLists`, `getListFeed`,
  `getListsWithMembership`, `getStarterPack(s)`, `getActorStarterPacks`,
  `getStarterPacksWithMembership`, and joined counts.
- Graph: `getRelationships`, `getKnownFollowers`, `getBlocks`,
  `muteThread`/`unmuteThread`, and scoped mutes (`onlyReposts`, `onlyQuoteposts`).
- Threads: `getPostThreadV2` and `getPostThreadOtherV2`; threadgate views and
  hidden replies; `viewer.replyDisabled`, `viewer.embeddingDisabled` and
  `viewer.knownLikers`; detached quotes; `opThreadPostIndex`/`Count`;
  `!no-unauthenticated` items; `thread_v2.max_parents`. `getPostThread`,
  `getQuotes` and `getProfiles` show posts and actors not stored locally.
- Notifications: preferences (`getPreferences`, `putPreferencesV2`), activity
  subscriptions and `subscribed-post`, `like-via-repost`, `repost-via-repost`,
  `starterpack-joined`, and `seenAt` on `getUnreadCount`.
- Bookmarks, drafts, `label.queryLabels`, `repo.getRecord` (from local rows),
  `getActorLikes` (one's own likes), `app.nearhorizon.feed.getAuthorPolls`,
  `bookmarkCount` (`[bookmarks] emit_count`), `labeler.getServices`,
  `identity.resolveHandle`.
- Search: `searchPosts` and `searchPostsV2` (query operators and V2 filters),
  `searchActorsTypeahead`, `searchStarterPacks` and `searchStarterPacksV2`.
- Suggestions (`getSuggestions`, the `getSuggested*` family,
  `getSuggestedFollowsByActor`, tagged suggestions, `getConfig`),
  `getTrendingTopics` and `getTrends`, all computed from local data.
- Profiles: `status` (gated by `unspecced.live_now`), `pronouns`, `website`,
  `joinedViaStarterPack`, associated counts and `viewer.knownFollowers`. Gallery
  embeds and the `posts_with_video` filter. An author feed continues from a
  descending PDS.
- **Members' list mutes and list blocks**: `muteActorList`, `unmuteActorList`,
  `getListMutes`, `getListBlocks`. Moderation lists a member mutes or blocks are
  synced from the owner's PDS within per-member budgets and apply wherever a
  direct mute or block does. `admin lists sync` and `admin lists purge`.
- **Farsight**: with `farsight.url`, a member's pages learn which accounts block
  the member through a list (`viewer.blockedBy`, `getRelationships.blockedByList`,
  notifications and the unread count); Farsight is primed when a member enrolls;
  an optional fallback AppView while Farsight is down and an optional memory of
  learned blocks, both off by default. `admin farsight status|prime|forget`.
- **Thread Lens v2**: viewer-relative threads in which a block hides only the
  blocked author's posts (as gaps), not the thread around them; posts past blocks
  only while Farsight vouches (`bridging` `off`, `limited`, `full`); sealed
  `repliesCursor` paging.
- **Rate limits and budgets**: `[rate_limits.<class>]` config with
  `endpoint_class` overrides; member and non-member pools with per-requester
  shares for every upstream; one deadline per request; IPv6 keyed by `/64`; a
  pre-auth backstop that charges a verified member to its DID.
- **Public indexing switch**: `indexing.public_scope = "off"` (the default for a
  fresh install) stores only what members need, answers the public `app.bsky.*`
  methods with an honest 501, and keeps the private, Thread Lens and member
  methods served. `admin indexing public-scope` and `admin indexing status`
  switch it, deleting or refilling public data in the background.
- Network clients by intent (`public`, `internal`, `viewer_private`), with
  optional `[net]` nameservers; fetches for a viewer's own mutes, bookmarks and
  subscriptions never touch shared caches.
- Embedded profiles of accounts outside the community come from a bounded
  upstream batch, a negative cache and a background refresh queue.
- Metrics for Farsight and its fallback and memory, list subscriptions, the
  private tier, backfill, the XRPC surface, identity, auth and budgets; alert
  examples in `docker/README.md`.

### Changed

- `getTimeline` and `listNotifications` hydrate before filtering mutes and
  blocks; a page can be short and still carry a cursor.
- `getAuthorFeed`'s `posts_with_media` means images and galleries; video has its
  own filter.
- `getFollows`, `getFollowers` and `getQuotes` send
  `X-Prism-Sort-Fallback: latest` for a `sort` other than `latest`.
- `likeCount` and `repostCount` count accounts, not records.
- Backfill workers past the first claim readiness jobs before resyncs.
- Outbound DNS names are looked up fully qualified.
- Metric renames: `farsight_fallback_calls_total` is
  `farsight_fallback_requests_total{endpoint, class}`; the outcome
  `refused_address` is `refused`; `handle_resolutions_total` gains `intent`.

### Fixed

- A service token with an out-of-range `iat`, `exp` or `nbf` is refused
  (`InvalidToken`) instead of panicking the request (kryphocron 0.4.1).
- An upstream network could stay skipped until a restart after a dropped breaker
  probe.
- A hung Farsight now counts as failing, and the fallback keeps to the request's
  deadline.
- `searchActors` keeps to accounts in scope and members, and applies the viewer's
  blocks and mutes.
- Accounts outside scope render as never seen on public paths, so stored rows of
  private-tier contacts no longer show through embedded profiles, `getProfile`,
  `resolveHandle` or `getRecord`.
- `getPosts` no longer fetches a post upstream once its delete has been seen.
- A late duplicate delete no longer wipes a re-created record's feed items and
  notifications.
- Muted threads write no notifications; `getKnownFollowers` keeps to the request
  deadline.
- A member whose PDS answers a missing optional record with HTTP 404 is set up
  normally.
- Backfill job generations stop a stale run from overwriting a newer one; list
  releases, recovery and retries no longer leave lists waiting.

### Upgrade notes

- Migrations 0004–0017 ship together (new collections and columns; a one-time
  re-sync and index builds follow, and `posts` may grow up to about twice while
  old rows are filled; see `docker/README.md`, "After the 0004–0017 upgrade"),
  then 0018 (list purpose), 0019 (learned blocks), 0020–0022 (public indexing
  switch and member post references), 0029 (counts by account; recounts every
  post).
- An upgraded database with public data stays `community_scoped`; a fresh
  install defaults to `off`.
- Removed config keys (`lists.subscribed_max_total`,
  `retention.subscribed_list_grace_days`, `features.push`,
  `features.age_assurance`, `features.contact`) are refused at startup with a
  message naming their replacement.
- Farsight's fallback tells another AppView which accounts are on a member's
  page; read `docker/README.md` before turning it on.

## Phase 1: Foundation (2026-09-26 – 2026-09-28)

Firehose ingest, the core `app.bsky.*` read API, the upstream cascade, the first
private tier and Thread Lens, and the admin CLI.

### Added

- `app.nearhorizon.*` lexicons.
- Configuration (`app.toml` plus `APP_*` overrides), Postgres with an
  operator-gated migration step, retention, an SSRF-guarded HTTP client, DID and
  handle resolution, and the identity refresher.
- Firehose ingest from a Redis stream: per-relay watermarks, gap handling, dead
  letters, defensive record parsing, last-writer-wins writes, scope admission and
  growth, purges, a weekly scope rebuild, backfill with reconcile, and
  Constellation count enrichment.
- An admin command queue run by `serve` (members, labels, scope rebuild,
  backfill, resync, dead-letter replay and drop) and a repo-head prober.
- The core `app.bsky.*` read API (profiles, posts, threads, author feeds,
  timelines, likes, reposts, quotes, follows, mutes, notifications), service-JWT
  auth, rate limits, enrollment gates, auto-enroll, and the did:web document.
- Hydration from local rows with the moderation gate (`!takedown`, `!hide`,
  account status), Constellation counts, and the upstream normalizer.
- The upstream cascade: per-network AppView clients, PDS-based routing, breakers,
  Slingshot record fallback, author-PDS listings, and
  `app.nearhorizon.constellation.resolveDid`.
- The first private tier: `app.nearhorizon.private.getPosts`, `getPostThread`
  and `getAuthorFeed`, read through an authorization pipeline with one uniform
  miss for anything unreadable.
- The first Thread Lens: `threadLens.getThread` and `resolve`, with a gap for
  each post that cannot be shown.
- `app.nearhorizon.index.requestRecord`: a signed-in account can ask for a record
  of its own to be indexed now.
- Read-only admin CLI: `member list`, `label list`, `deadletter list`,
  `stream info`, `commands list`, `keygen`, `migrate status`.

### Changed

- `sync.read_fetch_rate_per_host` defaults to 25 (was 20).
- `getLikes` and `getRepostedBy` page Constellation's per-record links.

### Fixed

- Retention and scope growth no longer race.
- Consumer lag is computed from the group counters, not Redis 7.4's estimated
  `lag`.

### Security

- `/xrpc/_health` answers anonymous callers with `ok` and `version` only; the
  details need a verified service token.
- Authorization records that fail to parse keep readiness unset and raise an
  alert.

### Upgrade notes

- Migrations 0001–0003.
