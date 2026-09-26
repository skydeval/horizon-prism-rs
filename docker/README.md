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

The first must return `"ok":true`. The second must show `did:web:<APP_PUBLIC_HOSTNAME>`.

**10. Add yourself as a member.**

With auto-enroll on, a client also enrols you the first time it loads your timeline or notifications, but only if your PDS host matches `community.enroll_pds_patterns` and `community.max_members` has not been reached. Members on other PDSes must be added here.

A new member gets `NotReady` from member-only endpoints until their initial indexing finishes. Check its state with `admin member list`.

```sh
docker compose exec app /app/horizon-prism-rs admin member add did:plc:yourdid
```

## Private-tier prerequisite

Private-tier records reach this AppView only through the relays. Each community PDS must have federation enabled and must have requested a crawl from `bsky.network` and `relay.fire.hose.cam`.

Check it by watching `private_tier_last_event_age_seconds` on `127.0.0.1:9091/metrics` after creating a private post.

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

**Restore a backup** (destructive to current data):

```sh
docker compose exec -T db sh -c 'pg_restore -U "$POSTGRES_USER" -d "$POSTGRES_DB" --clean --if-exists' < backup-YYYYMMDDTHHMMSS.dump
```

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

The sidecars exit on a stale cursor, a relay protocol error, or an event over 4 MiB, instead of silently skipping ahead. When this happens, `docker compose ps` shows the sidecar restarting, and its logs show the reason. Private reads fail closed if that relay is one of `firehose.private_relays`.

If the relay no longer has the saved cursor, accept the gap. Always run step 1 first.

1. Record the gap and clear readiness **before** the sidecar can resume:

   ```sh
   docker compose exec app /app/horizon-prism-rs admin resync --private-and-members
   ```

2. Compute the sidecar's cursor key: `firehose:cursor:` plus the unpadded URL-safe base64 of the relay URL in that sidecar's `firehose/*.toml`. For example, for the Community A sidecar:

   ```sh
   printf '%s' 'wss://relay.community-a.example/xrpc/com.atproto.sync.subscribeRepos' | base64 -w0 | tr '+/' '-_' | tr -d '='
   ```

3. Delete that key. The sidecar restarts from live tip.

   ```sh
   docker compose exec redis redis-cli DEL 'firehose:cursor:<value from step 2>'
   ```

### Silent frame drops

horizon-firehose drops frames it cannot decode, with no configuration option to stop instead. Those drops are covered by the daily re-sync (design §6.11.5). Check for them:

```sh
docker compose logs --no-log-prefix horizon-firehose-bsky horizon-firehose-microcosm horizon-firehose-community-a | jq -c 'select(.fields.message == "CBOR/CAR decode failed; dropping frame")'
```

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
