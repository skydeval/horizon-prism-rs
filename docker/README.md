# horizon-prism-rs — operator runbook

This is the deployment runbook for the stack in `docker/`. Design references are to `docs/internal/design/design-v1-*.md` §11. Run every command from this `docker/` directory unless stated otherwise.

## Services

| Service | Role | Host exposure |
|---|---|---|
| `db` | Postgres 16 | none |
| `redis` | Firehose stream, dead letters, sidecar cursor keys, and a capped count-refresh set (AOF, `noeviction`). Request caches live in the app process. | none |
| `app` | horizon-prism-rs (XRPC + worker) | `127.0.0.1:3030` (XRPC), `127.0.0.1:9091` (metrics) |
| `horizon-firehose-bsky` | relay consumer, `bsky.network` | none |
| `horizon-firehose-microcosm` | relay consumer, `relay.fire.hose.cam` | none |
| `horizon-firehose-community-a` | relay consumer, `relay.community-a.example` | none |
| `caddy` | TLS and reverse proxy | `80`, `443` |

The horizon-firehose image is built from a pinned commit of `https://github.com/skydeval/horizon-firehose.git`. No registry image exists. The build needs outbound network access, because its build fetches its own dependencies from GitHub.

## First-time bring-up

**1. DNS.** Point an A/AAAA record for your AppView hostname at this host. Ports 80 and 443 must be reachable from the internet so Caddy can obtain a certificate.

**2. Environment.**

```sh
cp .env.example .env
```

Edit `.env`:
- Set `APP_PUBLIC_HOSTNAME`, `CADDY_ACME_EMAIL`, and `POSTGRES_PASSWORD` (`openssl rand -hex 32`).
- Leave the two key variables as placeholders for now.

**3. Build images.**

```sh
docker compose build
```

**4. Generate keys.** The keygen subcommand reads no config and connects to nothing.

```sh
docker compose run --rm --no-deps app admin keygen
```

Copy the printed `APP_SERVICE_SIGNING_KEY_HEX` and `APP_CORRELATION_KEY_HEX` into `.env`. Keep them secret. Changing the signing key changes the AppView's published DID key.

Then make the private-tier cursor key and put it in `.env` as `APP_PRIVATE_CURSOR_KEY_HEX`:

```sh
openssl rand -hex 32
```

It is required: without it, or with a value that is not 64 hex characters, `app` refuses to start (exit 78). It is never generated at start, because a generated key would break every private-feed cursor at each restart and between replicas. Give every replica the same value. Rotating it makes open private-feed cursors fail once, and clients start those feeds over.

**5. Community PDS hosts.** Add your community PDS hostnames to the `community` network's `pds_patterns` in `config/app.toml`.

**6. First start with migrations enabled.** On an empty database every migration is pending, and `app` refuses to start (exit 78) unless the gate is open. In `.env`, set:

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

The first must return `"ok":true`; anonymously it shows only `ok` and `version`, and the full body (db, redis, audit guard, watermarks) needs a service JWT with `lxm` `_health`. The second must show `did:web:<APP_PUBLIC_HOSTNAME>`.

**10. Add yourself as a member.**

With auto-enroll on, a client also enrols you the first time it loads your timeline or notifications, but only if your PDS host matches `community.enroll_pds_patterns` and `community.max_members` has not been reached. Members on other PDSes must be added here.

A new member gets `NotReady` from member-only endpoints until their initial indexing finishes. Check its state with `admin member list`.

```sh
docker compose exec app /app/horizon-prism-rs admin member add did:plc:yourdid
```

## Private-tier prerequisite

Private-tier records reach this AppView only through the relays. Each community PDS must have federation enabled and must have requested a crawl from `bsky.network` and `relay.fire.hose.cam`.

Check it by watching `private_tier_last_event_age_seconds` on `127.0.0.1:9091/metrics` after creating a private post.

**Upgrading to a build with `app.nearhorizon.actor.getMembership`:** set `APP_PRIVATE_CURSOR_KEY_HEX` in `.env` (`openssl rand -hex 32`, see "Generate keys") **before** upgrading. That build refuses to start without it (exit 78). A deployment with its own compose override must pass the variable to `app` too.

## Schema migrations (operator-gated)

`app` never migrates unless `APP_ALLOW_STARTUP_MIGRATE=1`. With migrations pending and the gate closed, it exits with code 78 and logs `migrations_pending`.

**1. Back up first.** The redirect runs in your shell; `pg_dump` runs inside `db` using that container's own `POSTGRES_*` values.

```sh
docker compose exec -T db sh -c 'pg_dump -U "$POSTGRES_USER" -d "$POSTGRES_DB" -Fc' > "backup-$(date +%Y%m%dT%H%M%S).dump"
```

**2. Open the gate.** Set `APP_ALLOW_STARTUP_MIGRATE=1` in `.env`, then recreate `app` and confirm the migrations applied:

```sh
docker compose up -d app
docker compose logs --no-log-prefix app | jq -c 'select(.fields.message == "migrations_applied")'
```

**3. Close the gate.** Set `APP_ALLOW_STARTUP_MIGRATE=0`, recreate `app`, and confirm a clean boot:

```sh
docker compose up -d app
docker compose ps app
```

`app` must report `healthy`.

### After the 0004–0017 upgrade

Migrations 0004 to 0017 ship together. They add tables and columns and build no index on the large tables, so the downtime in step 2 is short. The rest of the upgrade runs in the background while `app` serves and ingests:

1. **Old rows are filled.** A task fills the new search and trend columns for posts written before the upgrade, in batches. Until it finishes, search and trends miss those posts.
2. **The search indexes are built**, one at a time, without blocking writes. Until they exist, search runs slowly.
3. **Known accounts are re-synced** once, one a second, to fetch the record types this version adds (lists, starter packs, gates and others).

Watch progress with the authenticated `_health` detail (`schema_backfill_done`, `search_indexes_ready`) or the metrics `schema_backfill_remaining{table}` and `schema_index_pending`. A failed index build is retried by itself; `schema_index_build_failures_total{index}` counts the failures.

**Disk.** Step 1 rewrites every old `posts` row. Expect the `posts` table and its existing indexes to grow, up to about twice their size, and leave that much free space before upgrading. Postgres reuses the space afterwards but does not return it to the filesystem.

**Restore a backup** (destructive to current data):

```sh
docker compose exec -T db sh -c 'pg_restore -U "$POSTGRES_USER" -d "$POSTGRES_DB" --clean --if-exists' < backup-YYYYMMDDTHHMMSS.dump
```

## Admin commands

Commands that change state are executed by the running `app` service, not by the `docker compose exec` process you type them in:
- `member add` and `member remove`;
- `label add` and `label remove`;
- `scope rebuild`, `backfill` and `resync`;
- `deadletter replay` and `deadletter drop`;
- `farsight status` and `farsight prime` (below);
- `indexing public-scope` and `indexing status` (below).

The CLI queues the command and waits up to `admin.command_timeout_secs` (600 s) for the result. If it times out, the command keeps running, and `admin commands list` shows its state. `app` must be running for these commands to take effect.

Read-only commands (`… list`, `stream info`, `migrate status`, `keygen`) run directly.

### Farsight (block lists)

Farsight is the block-list index of this stack. When `[farsight] url` is set in `config/app.toml` and `FARSIGHT_TOKEN` in `.env`, the app asks it, as a member loads a page, which accounts on that page block the member through a block list, and hides them as it hides a direct block. When Farsight does not answer, pages load as usual without those blocks. Whoever runs that Farsight sees which accounts each member's pages contain, so use an instance you run or trust.

```sh
docker compose exec app /app/horizon-prism-rs admin farsight status
docker compose exec app /app/horizon-prism-rs admin farsight prime <did> [--force]
```

- `status` prints Farsight's own report in brief, and whether the app has stopped calling it after repeated failures (`read_breaker_open`; it tries again after 30 s).
- `prime` asks Farsight to read one account's repository again. Use it when a member's own blocks look out of date. The app does this by itself when a member is enrolled.

Watch `farsight_calls_total{outcome}` for failures, `farsight_auth_failed_total` for a revoked or under-scoped key, and `farsight_coverage_total{level}`: at `partial`, Farsight is still catching up and some list blocks may be missing.

### Public indexing (on or off)

`indexing.public_scope` decides whether the app also serves as a public AppView. With it `off`, the app indexes and serves only what the private tier, Thread Lens and Horizon features need, and the client uses a public AppView (Bluesky or another) for everything else.

```sh
docker compose exec app /app/horizon-prism-rs admin indexing status
docker compose exec app /app/horizon-prism-rs admin indexing public-scope off
docker compose exec app /app/horizon-prism-rs admin indexing public-scope community_scoped
```

- **What off keeps:** every block; follows with a member at either end; members' own likes, reposts, lists, list items, list blocks and opt-outs; profiles and identity of accounts in scope; labels, tombstones, mutes, bookmarks, drafts; the whole private tier; and, for each member's post, only whom it replies to and quotes. **What off drops:** posts, feeds, notifications, everyone else's likes, reposts, follows and lists, gates, starter packs, polls, labeler services, and the count and trend tables. Public endpoints answer 501.
- **An upgrade never turns it off.** A deployment that already holds public data stays `community_scoped` after the upgrade, whatever `app.toml` says, and logs `public_scope_file_not_applied`. A fresh install with no public data starts `off`. To turn an upgraded deployment off, run `admin indexing public-scope off`, or uncomment `public_scope = "off"` in `config/app.toml` and restart once the upgraded build has started at least once. An edit made in the same deploy as the upgrade is only recorded.
- **The template line stays a comment.** Only a key written in `app.toml` is ever applied, and only when it changed since the last start. Commenting it out again changes nothing.
- **Turning off deletes.** The app deletes the dropped data in the background, table by table, in small batches; `status` shows the table, the rows deleted and when it last advanced. **Turning on refills only to backfill depth:** recent posts and interactions of each account in scope, not the full history, and no old notifications. **Take a `pg_dump` first** (see "Back up first" above).
- **Disk space comes back through autovacuum**, not through the switch: deleted rows become reusable space inside Postgres, and the data stays in WAL and backups until they age out. No `VACUUM FULL` is run.
- **Turning on:** while the resyncs run, public endpoints answer 501 with a "being turned on" message. The fill ends when its jobs are done, or after `indexing.fill_max_wait_hours` (24) at most. **Its end recomputes the count tables, which stalls writers for as long as the weekly scope rebuild's repair does.**
- **Backfill claims:** worker 0 claims jobs in plain due order and runs no list syncs; workers 1 and up give `member`, `private` and `authz` jobs first claim, so new members and gap recovery do not wait behind a resync wave.
- **Notifications:** a record that a backfill writes, and that was created before the most recent fill began, does not notify. Live records always do.
- `status` reports `error: "held"` when the stored state is unreadable (the off rule applies and nothing is deleted or filled until the next `public-scope` command), and `"sweep_stalled"` when the deletion has not advanced for 30 minutes. The same block is in the authenticated `_health` detail as `public_indexing`. Watch `public_indexing_phase{phase}`, `public_sweep_stalled`, `public_sweep_deleted_rows_total{table}`, `public_sweep_retries_total{table,reason}`, `public_fill_jobs_remaining` and `public_fill_incomplete_jobs`.

## Log inspection

All services log JSON.

Follow one service:

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

Firehose sidecar liveness, from its periodic metrics lines every 10 s:

```sh
docker compose logs --no-log-prefix horizon-firehose-bsky | jq -c 'select(.target == "horizon_firehose::metrics")'
```

## Stream, lag and dead letters

Consumer lag, pending count, and stream bounds:

```sh
docker compose exec app /app/horizon-prism-rs admin stream info
```

Raw Redis view:

```sh
docker compose exec redis redis-cli XINFO GROUPS firehose:events
```

Dead letters:

```sh
docker compose exec app /app/horizon-prism-rs admin deadletter list
docker compose exec app /app/horizon-prism-rs admin deadletter replay <id>
```

Each dead letter records one failed op: repo, rev, action, path and error, plus the first 4 KiB of the payload. `replay` re-fetches exactly that record from its PDS. A dead letter for a member or private owner also clears that DID's readiness and re-syncs it.

### A sidecar in a crash loop

The sidecars exit on a stale cursor or a relay protocol error, instead of silently skipping ahead. When this happens, `docker compose ps` shows the sidecar restarting, and its logs show the reason. Private reads fail closed if that relay is one of `firehose.private_relays`.

If the relay no longer has the saved cursor, accept the gap. Always run step 1 first.

1. Record the gap and clear readiness **before** the sidecar can resume:

   ```sh
   docker compose exec app /app/horizon-prism-rs admin resync --private-and-members
   ```

2. Compute the sidecar's cursor key: `firehose:cursor:` plus the unpadded URL-safe base64 of the relay URL in that sidecar's `firehose/*.toml`. For example, for the Community A sidecar:

   ```sh
   printf '%s' 'wss://relay.community-a.example/xrpc/com.atproto.sync.subscribeRepos' | base64 | tr -d '\n=' | tr '+/' '-_'
   ```

3. Delete that key. The sidecar restarts from live tip.

   ```sh
   docker compose exec redis redis-cli DEL 'firehose:cursor:<value from step 2>'
   ```

### Silent frame drops

horizon-firehose drops three kinds of event, and none of them can be configured to stop the sidecar instead:
- frames it cannot decode;
- frames whose decoder panicked;
- events over the 4 MiB size cap (`on_oversize = "skip_with_log"`). A fatal setting would let one crafted commit stop all ingest.

Those drops are covered by the daily re-sync (design §6.11.5). Check for them:

```sh
docker compose logs --no-log-prefix horizon-firehose-bsky horizon-firehose-microcosm horizon-firehose-community-a \
  | jq -R -c 'fromjson? // empty | select(.fields.message == "CBOR/CAR decode failed; dropping frame" or .fields.message == "decoder panicked on frame (isolated); dropping and continuing" or .fields.message == "dropping oversize event per on_oversize=skip_with_log")'
```

`jq -R … fromjson?` skips any non-JSON line instead of aborting.

If any appear, run `admin resync --private-and-members` to re-sync now rather than waiting for the daily pass.

## Sizing

- **`REDIS_MAXMEMORY`:** the stream is capped at about 1 500 000 entries, which is about 330 s across three relays. At a typical 1–2 KiB per entry that is about 1.5–3 GiB. Request caches live inside the app process, not in Redis. `6gb` leaves headroom. Redis runs `noeviction`: at the ceiling it rejects new stream entries, and nothing frees space on its own. The stream is only trimmed by new writes. Ingest stops, and private reads fail closed.

  Watch `redis_memory_ratio`. To recover, either raise `REDIS_MAXMEMORY` in `.env` and run `docker compose up -d redis`, or trim the stream, accepting a gap:

  ```sh
  docker compose exec redis redis-cli XTRIM firehose:events MAXLEN '~' 500000
  ```

  The app detects the gap and re-syncs members and private owners (design §5.7).
- **Private reads during a stall:** if all sidecars stall, private reads fail closed within about 2 minutes (`oracle_freshness_bound`) and recover once ingest catches up.
- **Disk:** the app warns at 80% and 95% of `storage.budget_bytes` in `config/app.toml`, which defaults to 200 GiB.
- **Load on the community PDS:** the app checks each member's and private owner's repo head in the background, and on some private reads. Per PDS host it sends at most about 25 head checks/s, 20 read-time checks/s and 5 backfill requests/s (`[sync]` and `[backfill]` in `config/app.toml`). At the 25/s cap, about 1100 tracked accounts per host can be kept fresh. Lower `community.max_members` if your PDS should see less.

## Forwarding a service to another server

Contact matching, age assurance and push registration are not built into this AppView. By default their endpoints answer `501`. You can forward contact, age assurance, or endpoints of your own, to a server you run or have an agreement with. Push cannot be forwarded in this version.

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

`config/app.toml` lists the other keys (timeouts, size caps, rate) with their defaults. Restart `app` after a change.

**What the target receives.** The request's query and body, unchanged, at `<proxy_url>/xrpc/<method>`. If the caller sent a token, the app checks it first and forwards it unchanged. A token that fails the check is answered with `401` here and is not forwarded. The target does not receive cookies, the caller's IP address or any other header, apart from `Content-Type`, `Content-Encoding`, `Accept-Language` and `atproto-accept-labelers`.

**What the target must do.**

- Accept service tokens whose audience (`aud`) is this AppView's DID, `did:web:<APP_PUBLIC_HOSTNAME>`, with no fragment or with `#bsky_appview`. A caller's PDS signs tokens for this AppView, not for your target. Bluesky's own servers refuse such tokens, so pointing `proxy_url` at Bluesky does not work for signed-in calls.
- Check the token itself: the issuer, the signature, the expiry and the method name (`lxm`).
- Answer at `/xrpc/<method>` with JSON, or with an empty body. Any other content type, a redirect or an answer over `max_response_bytes` reaches the caller as `502`.

**Private networks.** `proxy_url` must be `https` and resolve to a public address. `allow_private_network = true` lifts both rules for that one service, so a sidecar on the compose network can be reached as `http://<service name>:<port>`. Over `http`, every address the name resolves to must be private, or the call fails: a token never crosses a public network unencrypted.

**Trust.** The target sees your members' tokens for the forwarded methods, and their contact or age-assurance data. Each token is good only for the one method it was made for and expires within minutes, but until then the target can replay it to this AppView and use up that member's rate limit. Forward only to a server you trust with that.

**Limits.** Each forwarded service has its own caller limits (`[rate_limits.service_proxy_member]`, `[rate_limits.service_proxy_ip]`), a cap on what the target receives from this AppView in total (`rate_per_sec`, `burst`) and a cap on calls in progress (`max_in_flight`). Over a limit the caller gets `429`, or `503` when `max_in_flight` is reached. `service_proxy_requests_total{service, nsid, outcome}` counts every call by how it ended, and `service_proxy_duration_seconds{service}` times the target.

`app.bsky.contact.sendNotification` always answers `501`: it is a callback from a contact service into the AppView, which this version does not accept. A forwarded contact service therefore cannot create "a contact joined" notifications here.

## Metrics and alert examples

Scrape `127.0.0.1:9091/metrics` from a Prometheus on this host. Example rules:

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
      # The daily authorization re-check (design §6.11.5) has fallen behind its
      # 24 h budget: raise backfill.per_pds_rate_per_sec or lower community.max_members.
      - alert: AuthzBudgetExceeded
        expr: authz_daily_backlog_seconds > 86400
      # A member or private owner is permanently not ready: one of its
      # authorization records (block, audience, private post) fails parsing on
      # every attempt, so readiness is never set (fail-closed, design §5.10).
      # The `authz_stuck` ERROR log names the record URI.
      - alert: AuthzBackfillStuck
        expr: authz_backfill_stuck_total > 0
        for: 5m
        annotations:
          summary: "Member {{ $labels.did }} is wedged on a bad authz record — investigate and remove from PDS to unwedge."
```

A latched audit guard (design §6.5) makes `/xrpc/_health` return 503. `docker compose ps` then shows `app` as unhealthy. Restart `app` after investigating the logged panic.

## Teardown

Stop everything and keep all data:

```sh
docker compose down
```

**Destructive:** stop and delete all volumes (database, Redis stream and cursors, certificates, app data). Take a `pg_dump` first and confirm before running.

```sh
docker compose down -v
```
