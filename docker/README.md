# horizon-prism-rs — operator runbook

This is the deployment runbook for the stack in `docker/`. Design references are to `docs/internal/design/design-v1-*.md` §11. Run every command from this `docker/` directory unless stated otherwise.

## Services

| Service | Role | Host exposure |
|---|---|---|
| `db` | Postgres 16 | none |
| `redis` | Firehose stream, cursor keys, caches (AOF, `noeviction`) | none |
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

**10. Add yourself as a member.** With auto-enroll on, signing in from a client also does this.

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

## Sizing

- **`REDIS_MAXMEMORY`:** the stream is capped at about 500 000 entries. At a typical 1–2 KiB per entry that is about 0.5–1 GiB, plus read-time caches. `4gb` leaves headroom. Redis runs `noeviction`: at the ceiling, writes fail, and the sidecars retry until space frees. Nothing is silently evicted.
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
```

## Teardown

Stop everything and keep all data:

```sh
docker compose down
```

**Destructive:** stop and delete all volumes (database, Redis stream and cursors, certificates, app data). Take a `pg_dump` first and confirm before running.

```sh
docker compose down -v
```
