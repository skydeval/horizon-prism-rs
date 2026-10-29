# horizon-prism-rs — operator runbook

Deployment runbook for the stack in `docker/`. Run every command from this `docker/` directory unless stated otherwise.

## Services

| Service | Role | Host exposure |
|---|---|---|
| `db` | Postgres 16 | none |
| `redis` | Firehose stream, dead letters, sidecar cursor keys, a capped count-refresh set (AOF, `noeviction`). Request caches live in the app process. | none |
| `app` | horizon-prism-rs (XRPC + worker) | `127.0.0.1:3030` (XRPC), `127.0.0.1:9091` (metrics) |
| `horizon-firehose-bsky` | relay consumer, `bsky.network` | none |
| `horizon-firehose-microcosm` | relay consumer, `relay.fire.hose.cam` | none |
| `caddy` | TLS and reverse proxy | `80`, `443` |

The horizon-firehose image is built from a pinned commit of `https://github.com/skydeval/horizon-firehose.git`; there is no registry image. The build needs outbound network access.

## First-time bring-up

**1. DNS.** Point an A/AAAA record for your AppView hostname at this host. Ports 80 and 443 must be reachable so Caddy can obtain a certificate.

**2. Environment.**

```sh
cp .env.example .env
```

In `.env`, set `APP_PUBLIC_HOSTNAME`, `CADDY_ACME_EMAIL` and `POSTGRES_PASSWORD` (`openssl rand -hex 32`). Leave the key variables as placeholders for now.

**3. Build images.**

```sh
docker compose build
```

**4. Generate keys.** `keygen` reads no config and connects to nothing.

```sh
docker compose run --rm --no-deps app admin keygen
```

Copy the printed `APP_SERVICE_SIGNING_KEY_HEX` and `APP_CORRELATION_KEY_HEX` into `.env` and keep them secret. Changing the signing key changes the AppView's published DID key.

Then make the cursor key for [kryphocron](https://github.com/skydeval/kryphocron) feeds and put it in `.env` as `APP_PRIVATE_CURSOR_KEY_HEX`:

```sh
openssl rand -hex 32
```

It is required: without it, or with a value that is not 64 hex characters, `app` exits with code 78. Give every replica the same value. Rotating it makes open kryphocron feed cursors fail once, and clients start those feeds over.

Optional: set `APP_SYSTEM_ACCOUNTS_KEY_HEX` (from `admin keygen` or `openssl rand -hex 32`) to turn on system accounts. Give every replica the same value and back it up: once lists are stored, a start with another key or none leaves system accounts off until that key is back. See `docs/configuration.md` ("System-accounts key"). A compose override of your own must pass the variable to `app` too.

**5. Community PDS hosts.** Add your community PDS hostnames to the `community` network's `pds_patterns` in `config/app.toml`.

**6. First start with migrations enabled.** On an empty database every migration is pending, and `app` exits with code 78 unless the gate is open. In `.env`, set:

```
APP_ALLOW_STARTUP_MIGRATE=1
```

Then start the stack:

```sh
docker compose up -d
docker compose logs --no-log-prefix app | jq -c 'select(.fields.message == "migrations_applied" or .fields.message == "migrations_pending")'
```

**7. Close the migration gate.** Set `APP_ALLOW_STARTUP_MIGRATE=0` in `.env`, then recreate `app`:

```sh
docker compose up -d app
```

**8. Watch the certificate issue.**

```sh
docker compose logs -f caddy
```

**9. Verify from a machine outside this host.**

```sh
curl -fsS https://<APP_PUBLIC_HOSTNAME>/xrpc/_health
curl -fsS https://<APP_PUBLIC_HOSTNAME>/.well-known/did.json
```

The first must return `"ok":true`. Anonymously it shows only `ok` and `version`; the full body needs a service JWT with `lxm` `_health` from an operator. The second must show `did:web:<APP_PUBLIC_HOSTNAME>`.

**10. Become the operator.** While there is no operator, `app` logs a one-time setup code at every start and every 10 minutes (event `admin_setup_code`):

```sh
docker compose logs app | grep admin_setup_code
```

or print it with `docker compose exec app /app/horizon-prism-rs admin setup-code`. Open `https://<APP_PUBLIC_HOSTNAME>/admin/setup`, enter the code and your handle, and sign in on your own server. You become the first operator, and your server the first one allowed for operator sign-in. The code lasts 24 hours (restart `app` for a new one) and stops working once an operator exists. Anyone who can read the logs can see it, so finish promptly.

**11. Add yourself as a member.**

```sh
docker compose exec app /app/horizon-prism-rs admin member add did:plc:yourdid
```

Auto-enroll is off by default: only accounts you add are members. With it on (`community.auto_enroll = true`, or "Auto-join" in the admin UI), a client enrols an account the first time it loads its timeline or notifications, if its PDS host matches `community.enroll_pds_patterns` and `community.max_members` is not reached. An auto-enrolled member whose first indexing never finishes is removed after `community.never_ready_expiry_days` (7; 0 = never). Members you add are never removed this way.

A new member gets `NotReady` from member-only endpoints until its initial indexing finishes. Check with `admin member list`.

## Relays must crawl the community PDS

Kryphocron records reach this AppView only through the relays. Each community PDS needs federation on and must have requested a crawl from `bsky.network` and `relay.fire.hose.cam`. Check `private_tier_last_event_age_seconds` on `127.0.0.1:9091/metrics` after making a kryphocron post.

**Upgrading to a build with `app.nearhorizon.actor.getMembership`:** set `APP_PRIVATE_CURSOR_KEY_HEX` first (see "Generate keys"); that build exits with code 78 without it.

## Schema migrations (operator-gated)

`app` never migrates unless `APP_ALLOW_STARTUP_MIGRATE=1`. With migrations pending and the gate closed, it exits with code 78 and logs `migrations_pending`.

**1. Back up first.**

```sh
docker compose exec -T db sh -c 'pg_dump -U "$POSTGRES_USER" -d "$POSTGRES_DB" -Fc' > "backup-$(date +%Y%m%dT%H%M%S).dump"
```

**2. Open the gate.** Set `APP_ALLOW_STARTUP_MIGRATE=1` in `.env`, recreate `app` and confirm:

```sh
docker compose up -d app
docker compose logs --no-log-prefix app | jq -c 'select(.fields.message == "migrations_applied")'
```

**3. Close the gate.** Set `APP_ALLOW_STARTUP_MIGRATE=0`, recreate `app`, and check it reports `healthy`:

```sh
docker compose up -d app
docker compose ps app
```

### After the 0004–0017 upgrade

These migrations add tables and columns and build no index on the large tables, so the downtime is short. The rest runs in the background while `app` serves:

1. A task fills the new search and trend columns for older posts. Until it finishes, search and trends miss those posts.
2. The search indexes are built one at a time, without blocking writes. Until then, search is slow.
3. Known accounts are re-synced once, one a second, for the new record types.

Watch `schema_backfill_done` and `search_indexes_ready` in the authenticated `_health` detail, or `schema_backfill_remaining{table}`, `schema_index_pending` and `schema_index_build_failures_total{index}`. Failed index builds are retried.

**Disk.** Step 1 rewrites every old `posts` row; the table and its indexes can grow to about twice their size. Leave that much free space.

### Before the 0031 upgrade

From 0031, only members' kryphocron posts are served. A non-member account on a community PDS keeps its posts, audiences and the replies under them stored, but they are not served until it is added with `admin member add` and its indexing has finished.

List those accounts first (read-only):

```sh
docker compose exec -T db sh -c 'psql -U "$POSTGRES_USER" -d "$POSTGRES_DB"' <<'SQL'
SELECT e.did, a.handle, e.confirmed_at,
  (SELECT count(*) FROM private_posts p WHERE p.author_did = e.did) AS posts,
  (SELECT count(*) FROM private_audiences x WHERE x.owner_did = e.did) AS audiences
FROM private_owner_eligible e LEFT JOIN actors a ON a.did = e.did
WHERE NOT EXISTS (SELECT 1 FROM members m WHERE m.did = e.did)
  AND (EXISTS (SELECT 1 FROM private_posts p WHERE p.author_did = e.did)
       OR EXISTS (SELECT 1 FROM private_audiences x WHERE x.owner_did = e.did))
ORDER BY e.did;
SQL
```

Add each one that should keep being served, then upgrade.

**Restore a backup** (replaces current data):

```sh
docker compose exec -T db sh -c 'pg_restore -U "$POSTGRES_USER" -d "$POSTGRES_DB" --clean --if-exists' < backup-YYYYMMDDTHHMMSS.dump
```

## Web admin UI

`app` serves an admin UI at `https://<APP_PUBLIC_HOSTNAME>/admin`: Home with a live feed, members (readiness, add, remove, deny, allow), settings, operators, and the audit log (every change, who made it, how it ended, and sign-ins). Every change runs as the matching CLI command, with the operator recorded.

- **Settings.** `/admin/settings` has a page per group. Each setting shows what it does, its current value, the default, the value in `config/app.toml`, the last change, and whether it applies at once or after a restart. Saved values live in the database (`operator_settings`) and are layered over `config/app.toml` at every start; the file is never written. A page is checked as a whole before saving, so a value the app would refuse to start with is never saved. History is in `operator_settings_history`.
- **File and saved value disagree.** The saved value wins. If an upgrade or edit changes the file under a saved value, Home lists it and its page offers "Use the file's value" or "Keep my value". A saved value that no longer works after an upgrade is set aside at start (`operator_setting_set_aside`), and every page says so until it is fixed or cleared.
- **Restart.** With `APP_SUPERVISED=1` (set in the shipped compose, with `restart: unless-stopped`), the UI can restart `app`: it shuts down cleanly and exits with code 75. `admin restart` does the same.
- **From the command line.** `admin settings list`, `get <key>`, `set <key> <value>` (comma-separated list or JSON) and `reset <key>`, with the same checks.
- **Stats.** `/admin/stats` shows disk size and free space for the app's data, Postgres and Redis, the database against its budget, WAL, Redis memory and AOF, what is indexed, and requests per XRPC method since start (also `xrpc_requests_total{nsid,class}`). The Health page's Disk check turns amber under 10% or 5 GB free and red under 2%.
- **Disk figures need two read-only mounts.** The shipped compose mounts `db-data` at `/app/volumes/postgres` and `redis-data` at `/app/volumes/redis` in `app`, read-only; the app only calls `statvfs` on them. If your override uses host paths, mount them read-only at the same places. WAL size needs the database user to be a superuser or in `pg_monitor`; otherwise it shows "unknown".
- **The live feed.** Home streams the records the app writes: time, source, action, kind, author, and a public post's first 100 characters (`admin.live_feed_snippets = false` turns those off). Never kryphocron content, audience members or record subjects. Lines are sampled to about 50 a second per viewer; rates are exact. The stream is `/admin/live`: the shipped `Caddyfile` proxies it without compression and flushes at once, and a Caddyfile of your own needs the same. At most 2 streams per session and 8 in all.
- **Who can sign in.** Operators sign in with their own ATProto account (OAuth), only if on the operator list and on an allowed server. An empty allowed list allows any server. Sessions end after 30 days unused or a year (`admin.session_idle_secs`, `admin.session_max_secs`), and at once if the operator is removed or moves server.
- **The CLI break-glass.** `admin operator add <handle-or-did> [--note <text>]` adds an operator and allows their server's host; with no operator yet it also closes setup. Also `admin operator remove <did>` (never the last), `admin operator list`, `admin operator host add <pattern>` and `admin operator host remove <pattern>`. A pattern is a host (`pds.example.com`) or `*.` plus a host (`*.host.bsky.network`).
- **Locked out.** Run `admin operator add` for an account that can sign in. As a last resort, deleting every row of `admin_operators` reopens setup at the next restart.
- **Turning it off.** `admin.web_ui = false` mounts no `/admin` route.
- **Behind a CDN.** `/admin` responses are `Cache-Control: no-store, no-transform`. Keep script-rewriting CDN features (such as Rocket Loader and email obfuscation) off for `/admin*`; the security policy blocks what they inject.

**The public page.** `/` is a read-only page about the instance, with a live feed of relay events at `/live`; see `[public_page]` in `docs/configuration.md`. `/live` is a server-sent event stream: the shipped `Caddyfile`'s `@live` matcher covers it and `/admin/live`, and a Caddyfile of your own needs the same. Bandwidth grows with viewers (`docs/operations.md`).

**Upgrading to the first build with the admin UI:** there is no operator yet, so it enters first-run setup. Read the code from the logs and finish at `/admin/setup`, or run `admin operator add`.

## Admin commands

Commands that change state run inside the running `app`, not in the `docker compose exec` process:
- `member add`, `member remove`, `member deny <did> [--reason <text>]`, `member allow <did>`;
- `operator add`, `operator remove`, `operator host add`, `operator host remove`;
- `settings set`, `settings reset`, `restart`;
- `label add`, `label remove`;
- `scope rebuild`, `backfill`, `resync`;
- `deadletter replay`, `deadletter drop`;
- `farsight status`, `farsight prime`;
- `indexing public-scope`, `indexing status`.

The CLI waits up to `admin.command_timeout_secs` (600 s). A command that times out keeps running; `admin commands list` shows it.

Read-only commands (`… list`, `settings get`, `stream info`, `migrate status`, `keygen`, `setup-code`, `identity compare`) run directly.

### Denying membership

`admin member deny <did>` keeps an account out: it is never auto-enrolled, `member add` refuses it, and a current member is removed. `--reason` is an operator note. `admin member allow <did>` lifts the denial and adds nothing. `admin member list --denied` lists denials.

### Lodestar (identity reads)

Lodestar is an optional PLC directory mirror that also verifies handles both ways. With `[identity] lodestar_url` empty (the default), the app uses `plc_url` and checks handles itself. Set it to the https address of a lodestar you run or trust, and restart. Plain `http` and private addresses are refused.

- **Read from lodestar:** for public requests only, `did:plc` documents, handle verification, handle-to-DID lookups (kept only when the document claims the handle back), and batches of these.
- **Never from lodestar:** sign-in tokens, admin sign-in and commands, membership, account status, backfill, list sync, viewer-private requests and `did:web`. These use `plc_url`.
- **Fallback:** lodestar is asked only while it reports itself at the directory's tip (checked every 15 s). Anything it cannot answer is asked of `plc_url` in the same request. A "not verified" from lodestar is always checked again by the app.
- **Trust:** a wrong lodestar could show wrong handles, hide accounts or point at the wrong server for unindexed accounts. It cannot sign anyone in, grant admin or membership, or change stored data. It sees which accounts public requests ask about.

`admin identity compare [--sample N] [DID|HANDLE ...]` resolves accounts with and without lodestar and prints both with timings; it changes nothing:

```bash
docker compose exec app /app/horizon-prism-rs admin identity compare --sample 40 bsky.app
```

Watch `lodestar_fresh`, `lodestar_calls_total{endpoint,outcome}` (`forbidden`: this server is not admitted; `behind`: catching up) and `lodestar_skipped_total{reason}`.

### Farsight (block lists)

With `[farsight] url` in `config/app.toml` and `FARSIGHT_TOKEN` in `.env`, the app asks Farsight which accounts on a member's page block that member through a block list, and hides them like a direct block. Without an answer, pages load without those blocks. Farsight's operator sees which accounts each member's pages contain, so use one you run or trust.

```sh
docker compose exec app /app/horizon-prism-rs admin farsight status
docker compose exec app /app/horizon-prism-rs admin farsight prime <did> [--force]
```

- `status` prints Farsight's report and whether its breaker is open (`read_breaker_open`; retried after 30 s).
- `prime` asks Farsight to re-read one account's repository, for when a member's blocks look stale. Enrolment does this automatically.

Watch `farsight_calls_total{outcome}`, `farsight_auth_failed_total` (revoked or under-scoped key) and `farsight_coverage_total{level}` (`partial`: still catching up). `farsight_down` is 1 during an outage: it has answered since start, its breaker is open, and only timeouts, transport errors and `5xx` came since. `farsight_fallback_withheld{reason}` is 1 while the breaker is open but it is not down: `never_answered` or `unclean` (a `401`, `429` or `400` came). Both update once a second.

### Farsight's fallback and memory (both off)

`[farsight.fallback]` and `[farsight.memory]` are off by default, need `farsight.url`, and are read at start.

- **The fallback** asks one other AppView, while Farsight is down, which accounts on a member's page block the member through a list.
- **The memory** keeps the list blocks reported for each member, so they keep applying while Farsight is silent and across restarts.

**What the fallback discloses.** Each call tells the other AppView (and any CDN in front of it, or a redirect target) that this deployment is rendering a page for member A containing accounts B1 … Bn, from this deployment's address and `User-Agent`. A page includes search and suggestion results. With the default network, that AppView also receives this deployment's ordinary lookups, so it can link them and learn what the member loaded. Pointing `network` at an AppView you run helps only if it answers from its own data.

**When it asks.** Only while Farsight has answered since start, its breaker is open, and everything since its last answer was a timeout, transport error or `5xx`.
- A Farsight that never answered (wrong URL, revoked key) never turns it on, so it cannot start mid-outage after a restart.
- One `401`, `403`, `429` or `400` during an outage keeps it off for that outage.
- Thread Lens and `getRelationships` never use it. While Farsight is down, Thread Lens bridges nothing past a block.
- Its answers are never treated as complete and never override Farsight's.

**Measure Farsight under load first.** One key gets 16 calls in progress. A call times out when its own time plus its queue wait passes `farsight.timeout_ms` (or `threadlens.verdict_round_timeout_ms` for Thread Lens rounds). Five timeouts in a row bring the fallback on. A timed-out call still holds its place until Farsight finishes it, so size Farsight's places for the key against `farsight.rate_per_sec`, not just the timeout.

**Load on the other AppView:** up to the routing entry's rate plus `farsight.fallback.rate_per_sec`. Only an `auth = "unauth"` routing entry can be named.

**What the memory stores.** One row per (member, account blocking the member through a list): the list, the source and when last confirmed.
- Kept `retention_hours` after last confirmation (plus up to a day before cleanup). A block Farsight says is gone is forgotten at once.
- Backups keep the rows.
- `max_per_viewer` bounds one member, `max_total` the whole table; the oldest go first.
- Members only. `admin farsight forget <did>` clears one member's rows.
- Starting with the memory off empties the table. Migration 0019 adds it regardless.

Fallback metrics: `farsight_fallback_requests_total{endpoint,class}`, `farsight_fallback_skipped_total{reason}` (`no_budget`, `breaker_open`, `call_cap`, `allowance`, `deadline`, `actor_stop`), `farsight_fallback_discarded_total`, `farsight_fallback_pairs_total{result}` (`blocked`, `clear`, `invalid`, `not_found`, `missing`, `dropped`), `farsight_fallback_blocks_applied_total`, `farsight_fallback_ledger_entries`. Memory: `farsight_memory_entries`, `farsight_memory_changes_total{op}`, `farsight_memory_dropped_total{reason}`, `farsight_memory_blocks_applied_total`. `inbound_list_blocks_applied_total` counts list blocks from any source.

### Public indexing (on or off)

`indexing.public_scope` decides whether the app also serves as a public AppView. With it `off`, the app serves only what kryphocron, Thread Lens and Horizon features need, and clients use a public AppView for the rest.

```sh
docker compose exec app /app/horizon-prism-rs admin indexing status
docker compose exec app /app/horizon-prism-rs admin indexing public-scope off
docker compose exec app /app/horizon-prism-rs admin indexing public-scope community_scoped
```

- **Off keeps:** every block; follows with a member at either end; members' likes, reposts, lists, list items, list blocks and opt-outs; profiles and identity in scope; labels, tombstones, mutes, bookmarks, drafts; all kryphocron records; and each member post's reply and quote targets. **Off drops:** posts, feeds, notifications, others' likes, reposts, follows and lists, gates, starter packs, polls, labeler services, and count and trend tables. Public endpoints answer 501.
- **An upgrade never turns it off.** A deployment holding public data stays `community_scoped` whatever `app.toml` says, and logs `public_scope_file_not_applied`. A fresh install starts `off`. To turn an upgraded deployment off, run `admin indexing public-scope off`, or set `public_scope = "off"` in `config/app.toml` and restart after the upgraded build has started once.
- **Only a written key applies,** and only when it changed since the last start.
- **Turning off deletes** the dropped data in the background, in small batches; `status` shows progress. **Turning on refills only to backfill depth.** Take a `pg_dump` first.
- **Disk comes back through autovacuum** as reusable space inside Postgres. No `VACUUM FULL` is run.
- **Turning on:** public endpoints answer 501 ("being turned on") until the fill ends, or `indexing.fill_max_wait_hours` (24). The end recomputes the count tables, which stalls writers like the weekly scope rebuild does.
- **Backfill claims:** worker 0 claims in due order with no list syncs; workers 1+ take `member`, `private` and `authz` jobs first.
- **Notifications:** backfilled records created before the latest fill began do not notify.
- `status` reports `error: "held"` when the stored state is unreadable (nothing is deleted or filled until the next `public-scope` command), and `"sweep_stalled"` after 30 minutes without progress. Also in `_health` as `public_indexing`. Metrics: `public_indexing_phase{phase}`, `public_sweep_stalled`, `public_sweep_deleted_rows_total{table}`, `public_sweep_retries_total{table,reason}`, `public_fill_jobs_remaining`, `public_fill_incomplete_jobs`.

## Log inspection

All services log JSON.

```sh
docker compose logs -f app
```

Errors only:

```sh
docker compose logs --no-log-prefix app | jq -c 'select(.level == "ERROR")'
```

Audit events (kryphocron sinks):

```sh
docker compose logs --no-log-prefix app | jq -c 'select(.target == "audit")'
```

Firehose sidecar liveness (a metrics line every 10 s):

```sh
docker compose logs --no-log-prefix horizon-firehose-bsky | jq -c 'select(.target == "horizon_firehose::metrics")'
```

## Stream, lag and dead letters

```sh
docker compose exec app /app/horizon-prism-rs admin stream info
docker compose exec redis redis-cli XINFO GROUPS firehose:events
```

Dead letters:

```sh
docker compose exec app /app/horizon-prism-rs admin deadletter list
docker compose exec app /app/horizon-prism-rs admin deadletter replay <id>
```

Each dead letter is one failed op (repo, rev, action, path, error, first 4 KiB of payload). `replay` re-fetches that record from its PDS. A dead letter for a member or kryphocron post owner also clears its readiness and re-syncs it.

### A sidecar in a crash loop

Sidecars exit on a stale cursor or a relay protocol error rather than skip ahead; `docker compose ps` shows them restarting and the logs say why. Kryphocron reads fail closed if that relay is in `firehose.private_relays`.

If the relay no longer has the saved cursor, accept the gap. Always do step 1 first.

1. Record the gap and clear readiness **before** the sidecar resumes:

   ```sh
   docker compose exec app /app/horizon-prism-rs admin resync --private-and-members
   ```

2. Compute the cursor key: `firehose:cursor:` plus the unpadded URL-safe base64 of the relay URL in the sidecar's `firehose/*.toml`. For the Bluesky sidecar:

   ```sh
   printf '%s' 'wss://bsky.network/xrpc/com.atproto.sync.subscribeRepos' | base64 | tr -d '\n=' | tr '+/' '-_'
   ```

3. Delete the key. The sidecar restarts from live tip.

   ```sh
   docker compose exec redis redis-cli DEL 'firehose:cursor:<value from step 2>'
   ```

### Silent frame drops

horizon-firehose drops undecodable frames, frames whose decoder panicked, and events over 4 MiB (`on_oversize = "skip_with_log"`). The daily re-sync covers them. Check:

```sh
docker compose logs --no-log-prefix horizon-firehose-bsky horizon-firehose-microcosm \
  | jq -R -c 'fromjson? // empty | select(.fields.message == "CBOR/CAR decode failed; dropping frame" or .fields.message == "decoder panicked on frame (isolated); dropping and continuing" or .fields.message == "dropping oversize event per on_oversize=skip_with_log")'
```

If any appear, run `admin resync --private-and-members` now.

## Sizing

- **`REDIS_MAXMEMORY`:** the stream is capped at about 1 500 000 entries (about 330 s across three relays), roughly 1.5–3 GiB. `6gb` leaves headroom. Redis runs `noeviction`: at the ceiling it rejects new entries, ingest stops and kryphocron reads fail closed.

  Watch `redis_memory_ratio`. To recover, raise `REDIS_MAXMEMORY` and run `docker compose up -d redis`, or trim the stream and accept a gap (the app re-syncs members and owners):

  ```sh
  docker compose exec redis redis-cli XTRIM firehose:events MAXLEN '~' 500000
  ```

- **Stalls:** if all sidecars stall, kryphocron reads fail closed within about 2 minutes and recover once ingest catches up.
- **Disk:** the app warns at 80% and 95% of `storage.budget_bytes` (default 200 GiB).
- **Load on the community PDS:** per PDS host, at most about 25 head checks/s, 20 read-time checks/s and 5 backfill requests/s (`[sync]`, `[backfill]`). At 25/s, about 1100 tracked accounts per host stay fresh. Lower `community.max_members` to reduce it.

## Forwarding a service to another server

Contact matching, age assurance and push are not built in; their endpoints answer `501`. You can forward contact, age assurance or endpoints of your own to a server you run or trust. Push cannot be forwarded.

```toml
[services.contact]
state = "proxy"
proxy_url = "https://contact.example.net"

# Endpoints of your own: name each one and its verb.
[services.eu_region_notices]
state = "proxy"
proxy_url = "http://notices:8080"
allow_private_network = true
methods = { "eu.example.region.getNotices" = "GET" }
```

`config/app.toml` lists the other keys and defaults. Restart `app` after a change.

- **The target receives** the query and body unchanged at `<proxy_url>/xrpc/<method>`, plus the caller's token after the app has checked it (a failing token gets `401` here). Only `Content-Type`, `Content-Encoding`, `Accept-Language` and `atproto-accept-labelers` headers are passed; no cookies or caller IP.
- **The target must** accept service tokens with `aud` `did:web:<APP_PUBLIC_HOSTNAME>` (bare or `#bsky_appview`), so Bluesky's servers will not work; check issuer, signature, expiry and `lxm`; and answer JSON or an empty body. Anything else, a redirect, or over `max_response_bytes` becomes `502`.
- **Private networks.** `proxy_url` must be `https` to a public address. `allow_private_network = true` allows `http://<service name>:<port>` on the compose network; over `http`, every resolved address must be private.
- **Trust.** The target sees members' tokens for those methods (single-method, minutes-long) and could replay them against this AppView's rate limits.
- **Limits.** `[rate_limits.service_proxy_member]`, `[rate_limits.service_proxy_ip]`, `rate_per_sec`, `burst` and `max_in_flight`; over a limit callers get `429`, or `503` at `max_in_flight`. Metrics `service_proxy_requests_total{service, nsid, outcome}` and `service_proxy_duration_seconds{service}`.

`app.bsky.contact.sendNotification` always answers `501`.

## Metrics and alert examples

Scrape `127.0.0.1:9091/metrics`. Example rules:

```yaml
groups:
  - name: horizon-prism-rs
    rules:
      - alert: FirehoseLagHigh
        expr: firehose_consumer_lag_entries > 100000
        for: 5m
      - alert: FirehoseTrimGap
        expr: increase(firehose_trim_gap_total[15m]) > 0
      - alert: DeadLetters
        expr: increase(firehose_dead_letter_total[1h]) > 0
      - alert: StorageBudget
        expr: db_budget_ratio > 0.8
        for: 30m
      # No kryphocron record or delete from the relays for a day.
      # Check that every private relay crawls the community PDSes.
      - alert: PrivateTierIngestStale
        expr: private_tier_last_event_age_seconds > 86400
      - alert: RelayWatermarkBehind
        expr: firehose_worker_delay_seconds > 120
        for: 5m
      - alert: RedisMemoryHigh
        expr: redis_memory_ratio > 0.8
        for: 10m
      - alert: BackfillJobsFailed
        expr: backfill_jobs{state="failed"} > 0
        for: 1h
      - alert: MembersNotReady
        expr: members_not_ready_total > 0
        for: 24h
      # The daily authorization re-check is over its 24 h budget. Raise
      # backfill.per_pds_rate_per_sec or lower community.max_members.
      - alert: AuthzBudgetExceeded
        expr: authz_daily_backlog_seconds > 86400
      # An account's block, audience or kryphocron post fails parsing on
      # every attempt, so it never becomes ready. The `authz_stuck` log names the URI.
      - alert: AuthzBackfillStuck
        expr: authz_backfill_stuck_total > 0
        for: 5m
        annotations:
          summary: "Member {{ $labels.did }} is wedged on a bad authz record — investigate and remove from PDS to unwedge."
      # `following` audience owners waiting for the firehose to apply their
      # latest commit. Short spikes are normal; a floor that never drains
      # usually means a PDS no configured relay crawls.
      - alert: FollowingOwnersWaitingFirehose
        expr: following_owners_waiting_firehose > 0
        for: 6h
      # The consumer has spent over half its time on full batches for 15 min:
      # no margin for a burst. Also logs `firehose_headroom_warn`.
      - alert: FirehoseBacklogPersistent
        expr: rate(firehose_consumer_backlog_busy_seconds_total[5m]) > 0.5
        for: 15m
      - alert: FirehoseConsumerSlowerThanInput
        expr: sum without (path) (rate(firehose_entries_processed_total[5m])) < 0.95 * rate(firehose_stream_entries_added_total[5m])
        for: 5m
      - alert: FirehoseGroupMissing
        expr: firehose_consumer_group_present == 0
        for: 2m
      # Redis is unreachable, hung or stuck (`LOADING`, `BUSY`).
      - alert: FirehoseSamplerFailing
        expr: sum without (call) (rate(firehose_lag_sample_failures_total[5m])) / sum without (call) (rate(firehose_lag_sample_calls_total[5m])) > 0.5
        for: 5m
      - alert: FarsightDown
        expr: farsight_down == 1
        for: 5m
      # Breaker open, fallback held off (`never_answered` or `unclean`).
      - alert: FarsightFallbackWithheld
        expr: farsight_fallback_withheld == 1
        for: 5m
      - alert: FarsightAuthFailed
        expr: increase(farsight_auth_failed_total[15m]) > 0
      # A subscribed-list sync running past 1.25 × lists.sync_stuck_secs.
      - alert: ListSyncStuck
        expr: list_sync_running_age_seconds > 6750
        for: 15m
      # Search indexes still building, or the database is short of resources.
      - alert: SearchTimeouts
        expr: rate(search_timeout_total[5m]) > 0.1
        for: 10m
```

Other series:

- `private_owners_not_ready_total`: kryphocron post owners whose initial indexing has not finished.
- `private_tier_ops_skipped_total{op}`: kryphocron records skipped because this build has kryphocron off.
- `index_request_total{status}`: `requestRecord` calls by status; `indexed` means the relays never delivered the record.
- `index_request_catch_up_total{outcome}`: the catch-up after a `requestRecord`.
- `settle_uncovered_total{path}`: settles whose revision the listed revisions did not cover.
- `backfill_optional_record_unclear_total{collection}`: profile or `self` fetches answered with a bare 404.
- `retention_deleted_rows_total{table}`, `retention_task_failures_total{task}`.
- `storage_sample_errors_total{source}`: failed Postgres or Redis storage samples.
- `identity_refresh_total{outcome}`, `account_status_changes_total{to}`, `member_denials_total`.
- `identity_cache_requests_total{cache,result}`: DID-document cache reads; `cache="lodestar"` is lodestar's.
- `lodestar_calls_total{endpoint,outcome}`, `lodestar_call_seconds{endpoint}`, `lodestar_skipped_total{reason}`, `lodestar_batch_filled_total`, `lodestar_fresh`.
- `handle_resolutions_total{intent,result}`: `public` for `resolveHandle`, `internal` for indexing.
- `jwt_fetch_cell_failures_total`: failed token issuer lookups.
- `xrpc_auth_total{result}`: token checks.
- `auth_claims_precheck_failed_total`: tokens failing the pre-verification claims check.
- `auth_member_verify_exhausted_total`, `auth_budget_exhausted_total`, `member_pool_allowance_exhausted_total`: 429s from verification and issuer-lookup budgets.
- `member_keys`: ready members whose signing key is in memory.
- `upstream_skipped_total{network,reason}`: upstream calls not made (`deadline`, `breaker_open`, `rate_limited`).
- `cascade_request_deadline_exceeded_total`: lookups after the request's 6 s deadline.
- `upstream_view_counts_total{source}`: where counts on upstream views came from.
- `starter_pack_counts_refreshed_total{source}`.
- `viewer_private_cache_entries`, `viewer_private_cache_evictions_total`: the per-member private lookup cache.
- `profile_batch_fetch_total{network,result}`: `getProfiles` calls: `ok`, `budget`, `transient` or `error`.
- `profile_refresh_queue`, `profile_refresh_dropped_total`, `profile_refresh_completed_total`: the background profile refresh (bounded by `cache.profile_refresh_max`, `background_rate_per_sec`).
- `profile_refresh_negative_total`: accounts absent from every network, shown minimal for `cache.profile_negative_ttl_secs`.
- `host_budget_buckets`: per-host budget buckets.
- `sync_head_check_seconds{path}`: head check time; `probe` (background) or `read`.
- `xrpc_request_seconds{nsid}`: request time, authentication included.
- `private_read_stage_seconds{endpoint,stage}`: kryphocron read time by stage (`gate`, `reads`, `context`, `rest`), summing to `private_read_unfloored_seconds`.
- `private_read_empty_page_total{endpoint}`: kryphocron pages with no items and a cursor.
- `sync_divergence_detected_total`: repositories whose head stayed ahead of the index past the grace period.
- `private_read_degraded`: 1 while kryphocron reads are degraded (a private relay never reported, or the slowest is over `kryphocron.oracle_freshness_bound_secs` behind). Logged as `private_read_degraded_started` and `private_read_degraded_ended`.
- `sync_chain_breaks_total`: commits that arrived before the commit they follow. Needs a horizon-firehose that carries `since`.
- `sync_chain_behind`, `sync_chain_links`: repositories not yet seen up to what was indexed, and the commits waiting.
- `private_read_fallback_total{result}`: degraded reads checked by `private_read.head_fallback`: `passed`, `behind`, `untracked`, `climb_cap`, `walk_outside`.
- `private_read_unavailable_total{kind}`: try-again answers: `thread`, `quote`, `listed`.
- `xrpc_reindexing_total{site}`: `503 Reindexing` answers (`private`, `public`, `threadlens`, `view`, `membership`).
- `sync_degraded_authz_enqueued_total`: full syncs queued while degraded.
- `list_subscription_tx_retries_total`, `list_copy_write_errors_total`.
- `list_purpose_writes_total{path,result}`, `list_purpose_write_errors_total{path}`.
- `list_sync_runs_total{outcome,lane}`, `list_sync_stuck_requeued_total`.
- `list_sync_slow_owners`, `list_sync_cut_owners`, `list_sync_weekly_overdue`.
- `xrpc_method_not_implemented_total`, `xrpc_wrong_method_total`.
- `sort_fallback_total{endpoint}`: listings served `latest` because another order was asked.
- `search_duration_seconds{endpoint}`, `trends_refresh_seconds`, `suggestions_refresh_seconds`.
- `refract_reads_total{method,outcome}`, `refract_read_seconds{method}`: Refract calls by outcome and time.
- `refract_candidates_total{source,outcome}`: accounts named for checking; `not_hosted`, `not_hosted_cached` (24 h), `resolve_failed`.
- `refract_viewer_state_total{field,outcome}`: visitor viewer-state lookups (`found`, `none`, `unknown`), only with `refract.visitor_viewer_state` on.
- `refract_repost_fill_total{outcome}`, `refract_repost_fill_rows_total`: the one-time older-repost fill (`done`, `left`, `error`).
- `refract_visitor_pairs_total{outcome}`: visitor block checks, `answered` or `unanswered` (treated as blocked).
- `standin_hosts_build`: 1 only in a test build.
- `public_feed_reader_lag_seconds`, `public_feed_late_total`, `public_feed_undecodable_total`: the public page's live reader.

A latched audit guard makes `/xrpc/_health` return 503 and `app` unhealthy. Restart `app` after reading the logged panic.

## Teardown

Stop everything and keep all data:

```sh
docker compose down
```

**Destructive:** delete all volumes (database, Redis, certificates, app data). Take a `pg_dump` first.

```sh
docker compose down -v
```
