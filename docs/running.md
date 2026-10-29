# Running Horizon-Prism

Horizon-Prism ships as a Docker Compose stack in `docker/`. Run every command from that directory. [`docker/README.md`](../docker/README.md) is the full runbook; this page is the short version.

## The stack

| Service | Role | Exposed |
|---|---|---|
| `app` | Horizon-Prism: XRPC server, firehose consumer, background workers | `127.0.0.1:3030` (XRPC), `127.0.0.1:9091` (metrics) |
| `db` | Postgres 16 | no |
| `redis` | Firehose stream, dead letters, sidecar cursors (append-only file, `noeviction`) | no |
| `horizon-firehose-*` | One relay sidecar per relay. The shipped file runs two. | no |
| `caddy` | TLS and reverse proxy | `80`, `443` |

Request caches live inside `app`, not in Redis. See [firehose and relays](design/firehose-and-relays.md) for how the pieces connect.

## First start

1. **DNS.** Point an A/AAAA record for your hostname (for example `prism.example.org`) at the host. Open ports 80 and 443.
2. **Environment.** `cp .env.example .env`. Set `APP_PUBLIC_HOSTNAME`, `CADDY_ACME_EMAIL` and `POSTGRES_PASSWORD` (`openssl rand -hex 32`).
3. **Build.** `docker compose build`. This needs outbound network access.
4. **Keys.** `docker compose run --rm --no-deps app admin keygen` prints `APP_SERVICE_SIGNING_KEY_HEX` and `APP_CORRELATION_KEY_HEX`. Put both in `.env`. Then set `APP_PRIVATE_CURSOR_KEY_HEX` to the output of `openssl rand -hex 32`. Keep all three secret and stable:
   - the signing key is published in the AppView's DID document; changing it changes that key;
   - the cursor key must be the same on every replica; rotating it makes open kryphocron feed cursors fail once.
5. **Community PDS.** Add your community's PDS hostnames to the `community` network's `pds_patterns` in `config/app.toml`. See [configuration](configuration.md).
6. **Migrate and start.** Set `APP_ALLOW_STARTUP_MIGRATE=1` in `.env`, then `docker compose up -d`.
7. **Close the gate.** Set `APP_ALLOW_STARTUP_MIGRATE=0` and run `docker compose up -d app`.
8. **Check from outside the host.**
   ```sh
   curl -fsS https://prism.example.org/xrpc/_health
   curl -fsS https://prism.example.org/.well-known/did.json
   ```
   The first returns `"ok":true`. The second shows `did:web:prism.example.org`.
9. **Become the operator.** While no operator exists, `app` logs a one-time setup code (event `admin_setup_code`). Read it with `docker compose logs app | grep admin_setup_code`, open `https://prism.example.org/admin/setup`, and sign in with your own atproto account. The code lasts 24 hours and stops working once an operator exists. Anyone who can read the logs can see it, so finish this promptly.
10. **Add members.** `docker compose exec app /app/horizon-prism-rs admin member add did:plc:example`, or use the admin UI. A new member is not ready until its first indexing finishes. See [membership](design/membership.md).

**Relays must crawl the community PDS.** [Kryphocron](https://github.com/skydeval/kryphocron) posts arrive only through the relays. Each community PDS needs federation on and must have asked the configured private relays to crawl it.

## Migrations

`app` never changes the schema unless `APP_ALLOW_STARTUP_MIGRATE=1`. With a migration pending and the gate closed, it logs `migrations_pending` and exits with code 78. Take a backup, open the gate for one start, then close it.

`docker compose exec app /app/horizon-prism-rs migrate status` shows what is pending.

## Upgrades

1. Read the [changelog](../CHANGELOG.md) for the versions you skip. Some upgrades need a step before they run (for example a new required environment variable).
2. Back up the database (below).
3. Pull or build the new image.
4. If migrations are pending, open the gate, `docker compose up -d app`, confirm `migrations_applied` in the log, then close the gate and recreate `app` again.
5. Check that `docker compose ps app` shows `healthy`.

Some upgrades finish work in the background while `app` serves: filling new columns, building search indexes, re-syncing known accounts. The authenticated `_health` detail and the `schema_*` metrics show progress. Search and trends may miss older posts until it finishes.

A value saved in the admin UI wins over `config/app.toml`. If an upgrade changes a file value that has a saved value over it, the admin UI asks which to keep.

## Backups

Postgres holds everything that matters. Redis holds only the in-flight stream and cursors, which Horizon-Prism can rebuild by re-syncing.

```sh
docker compose exec -T db sh -c 'pg_dump -U "$POSTGRES_USER" -d "$POSTGRES_DB" -Fc' > backup.dump
```

Restore with `pg_restore --clean --if-exists` into the `db` container. This replaces current data. Also keep a copy of `.env` (the keys) and `config/app.toml`. A backup keeps every row the database held, kryphocron records included.

## Health checks

- `GET /xrpc/_health` returns `ok` and `version` to anyone. The full body (database, Redis, audit guard, relay watermarks, background progress) needs an operator's service token for `_health`.
- The compose healthcheck runs the binary's own `healthcheck` command. `docker compose ps` shows `app` as `healthy` or `unhealthy`.
- If kryphocron's audit guard trips, `_health` returns 503 and `app` turns unhealthy. Read the logged panic, then restart `app`.
- The admin UI's Health page shows the same checks, plus disk space.

## Stop and remove

- `docker compose down` stops everything and keeps the data.
- `docker compose down -v` deletes every volume: database, stream, certificates. Back up first.

See also: [operations](operations.md), [admin](admin.md).
