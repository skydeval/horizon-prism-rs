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

- Retention and scope growth no longer race: scope-dependent deletes run per
  DID under the DID's exclusive advisory lock, and growth takes the subject's
  shared lock before inserting `scope_dids`.

### Security

- Authorization-collection records that fail deterministically keep readiness
  unset (fail-closed) and raise the `AuthzBackfillStuck` alert.
