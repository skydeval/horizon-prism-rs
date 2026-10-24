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

**10. Become the operator.** While the instance has no operator, `app` prints a one-time setup code in its log at every start and every 10 minutes (event `admin_setup_code`). Read it:

```sh
docker compose logs app | grep admin_setup_code
```

or print it again with `docker compose exec app /app/horizon-prism-rs admin setup-code`. Open `https://<APP_PUBLIC_HOSTNAME>/admin/setup`, enter the code and your handle, and sign in on your account's own server. You become the first operator, and your server becomes the first one allowed for operator sign-in. The code is good for 24 hours (restart `app` for a new one) and stops working once an operator exists. Finish this promptly: anyone who can read the container's logs can see the code. See "Web admin UI" below.

**11. Add yourself as a member.**

Auto-enroll is off by default: only the accounts you add are members. If you turn it on (`community.auto_enroll = true`, or "Auto-join" in the admin UI), a client also enrols you the first time it loads your timeline or notifications, but only if your PDS host matches `community.enroll_pds_patterns` and `community.max_members` has not been reached. Members on other PDSes must be added here. An auto-enrolled member whose first indexing never finishes is removed after `community.never_ready_expiry_days` (7; 0 = never), checked hourly; members you add are never removed this way.

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

### Before the 0031 upgrade

From migration 0031 only a member owns private posts that are served. An account hosted on a community PDS that is not a member stops being an owner at the upgrade: its private posts, its audiences and the replies under them are kept but no longer served, until it is added with `admin member add` and its initial indexing has finished.

Before upgrading, list those accounts (read-only):

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

Add each one that should stay an owner with `admin member add`, then upgrade.

**Restore a backup** (destructive to current data):

```sh
docker compose exec -T db sh -c 'pg_restore -U "$POSTGRES_USER" -d "$POSTGRES_DB" --clean --if-exists' < backup-YYYYMMDDTHHMMSS.dump
```

## Web admin UI

`app` serves an admin UI at `https://<APP_PUBLIC_HOSTNAME>/admin`: Home with a live feed of what Prism indexes, members (list with readiness, add, remove, deny, allow), settings, operators, and the audit log (every change, who made it and how it ended, and the sign-ins; `/admin/activity` redirects there). Every change it makes is an admin command, run exactly as the CLI's, with the operator who asked recorded.

- **Settings.** `/admin/settings` has a page per group (who can use Prism, what it stores, private posts, blocks, what apps see, extra services, limits, profiles, account lookups). Each setting shows what it does, the value saved now, the default, the value `config/app.toml` writes if it writes one, the last change, and whether it applies at once or after a restart. A saved value is kept in the database (`operator_settings`) and layered over `config/app.toml` at every start; the file itself is never written. Saving a page checks the whole configuration first, so a value Prism would refuse to start with is never saved. The membership switches, Thread Lens access, `getConfig`'s answers and the trends texts apply at once; everything else is saved at once and applies after a restart, and every page then says how many changes are waiting. Public data has its own switch on the "What Prism stores" page. Two read-only pages show what was set at install (with why it cannot be changed there) and everything Prism is running with, without secrets. Every change is kept in `operator_settings_history`.
- **When the file and a saved value disagree.** A value saved in the UI wins over `config/app.toml`. If an upgrade or an edit changes the file under a saved value, the saved value still applies, Home lists the setting, and its page offers "Use the file's value" or "Keep my value". If a saved value no longer works after an upgrade, Prism starts without it (and the settings that must go with it), logs `operator_setting_set_aside`, and every page says so until it is fixed or cleared in the UI.
- **Restart.** With `APP_SUPERVISED=1` (set in the shipped `docker-compose.yml`, where `restart: unless-stopped` starts `app` again), the UI can restart `app`: it shuts down as it does for `docker compose stop` and exits with code 75. Without it the button says to ask whoever runs the server. `admin restart` does the same from the command line.
- **From the command line.** `admin settings list`, `admin settings get <key>`, `admin settings set <key> <value>` (a list separated by commas, or JSON) and `admin settings reset <key>` do what the pages do, with the same checks.
- **Stats.** `/admin/stats` shows the size, used and free space of the disks holding Prism's data, Postgres and Redis (each disk once), the database against its budget, the write-ahead log, Redis's memory and its append-only file, how much is indexed (exact for members and the accounts Prism follows, Postgres's own estimates marked "about" for the big tables), and every XRPC method's requests since the start by outcome, with the methods never used. Endpoint counts are kept in memory and start again at a restart; each request is also exported as `xrpc_requests_total{nsid,class}`. The Health page's Disk check turns amber under 10% or 5 GB free and red under 2%, and Home shows the fullest disk.
- **Disk figures need two read-only mounts.** The shipped `docker-compose.yml` mounts `db-data` at `/app/volumes/postgres` and `redis-data` at `/app/volumes/redis` in `app`, read-only. Prism only asks those filesystems for their size and free space (`statvfs`); it opens no file there, and Postgres's files are unreadable to its user anyway. If your override puts Postgres's or Redis's data on a host path instead, mount that path read-only at the same place; without a mount the page says Prism cannot see that disk. The write-ahead log's size needs the database user to be a superuser or in `pg_monitor`, as the shipped compose's is; otherwise it shows "unknown".

- **The live feed.** Home shows, as it happens, the records Prism writes from the network, backfill and `requestRecord`: time, source, action, kind, author, and for a public post its first 100 characters (`admin.live_feed_snippets = false` turns those off). Never a private post's content, an audience's members or any record's subject. Lines are sampled to about 50 a second per viewer; the rates beside them are exact. It is a server-sent event stream at `/admin/live`: the shipped `Caddyfile` proxies that path without compression and flushes it at once, and a Caddyfile of your own needs the same block. Watching does not keep a session alive. At most 2 pages per session and 8 in all stream at once.

- **Who can sign in.** Operators sign in with their own ATProto account, on their account's own server (OAuth). Only accounts on the operator list may sign in, and only from a server on the allowed list. An empty allowed list allows any server (the operator list still decides who). A session ends after 8 hours unused or 7 days (`admin.session_idle_secs`, `admin.session_max_secs`), and at once if the operator's account moves to another server.
- **The CLI break-glass.** `admin operator add <handle-or-did> [--note <text>]` adds an operator and allows the exact host of their server (unless a listed pattern already covers it); with no operator yet it also closes first-run setup. `admin operator remove <did>` (never the last one), `admin operator list`, `admin operator host add <pattern>` and `admin operator host remove <pattern>` (refused if no operator could still sign in). A pattern is a host (`pds.example.com`) or `*.` and a host (`*.host.bsky.network`).
- **Locked out.** Run `admin operator add` for an account that can sign in. Setup never reopens on its own. As a last resort, deleting every row of `admin_operators` with SQL reopens first-run setup at the next restart.
- **Turning it off.** `admin.web_ui = false` in `config/app.toml` mounts no `/admin` route; the CLI is then the whole admin surface.
- **Behind a CDN.** Every `/admin` response is sent with `Cache-Control: no-store, no-transform`, and only Home loads a script, one same-origin file. Keep CDN features that inject or rewrite scripts (for example Cloudflare's Rocket Loader and email obfuscation) off for `/admin*`: the page's security policy blocks what they inject, and Home says when its script did not start. The live stream sends a keepalive every 15 seconds, inside a CDN's usual idle limit. Putting `/admin*` behind your CDN's access control as well is optional.

**Upgrading to this version:** an existing deployment has no operator yet, so it enters first-run setup: read the code with `docker compose logs app | grep admin_setup_code` and finish at `/admin/setup`, or run `admin operator add` for your account.

## Admin commands

Commands that change state are executed by the running `app` service, not by the `docker compose exec` process you type them in:
- `member add` and `member remove`;
- `member deny <did> [--reason <text>]` and `member allow <did>` (below);
- `operator add`, `operator remove`, `operator host add` and `operator host remove` (above);
- `settings set`, `settings reset` and `restart` (above);
- `label add` and `label remove`;
- `scope rebuild`, `backfill` and `resync`;
- `deadletter replay` and `deadletter drop`;
- `farsight status` and `farsight prime` (below);
- `indexing public-scope` and `indexing status` (below).

The CLI queues the command and waits up to `admin.command_timeout_secs` (600 s) for the result. If it times out, the command keeps running, and `admin commands list` shows its state. `app` must be running for these commands to take effect.

Read-only commands (`… list`, `settings get`, `stream info`, `migrate status`, `keygen`, `setup-code`, `identity compare`) run directly.

### Denying membership

`admin member deny <did>` keeps an account out. A denied account is never auto-enrolled, `admin member add` refuses it, and if it is a member it is removed in the same step (its removal then finishes in the background, as `member remove` does). `--reason` is a note for operators; the account never sees it. `admin member allow <did>` lifts the denial and adds nothing: run `admin member add` afterwards to admit it. `admin member list --denied` lists the denials.

### Lodestar (identity reads)

Lodestar is a read-only mirror of the PLC directory that also checks every account's handle both ways: the account's DID document claims the handle, and the handle points back to the DID. It is a separate service of this stack, optional: with `[identity] lodestar_url` empty (the default), this app reads identities from `plc_url` and checks handles itself, as it always has. Set `lodestar_url` to the https address of a lodestar you run or trust, and restart. The address must be reachable over https at a public address: plain `http` is refused at startup, and a private network address is refused like any other private host. If lodestar admits callers by address, add this server's outgoing address.

**What it reads from lodestar.** Only ordinary public requests, and only identity:
- a `did:plc` account's DID document (its PDS and claimed handle);
- whether the claimed handle is verified, which saves this app its own DNS and HTTPS check when lodestar says yes;
- which account holds a handle (for `resolveHandle` and handles in request parameters), kept only when that account's document claims the handle back;
- for pages that name many accounts, the same in one batch call.

**What never reads lodestar.** Sign-in tokens (their issuers' signing keys), admin sign-in and admin commands, automatic membership, account status and deletion, backfill and stored data, list syncing, viewer-private requests and `did:web` accounts all use `plc_url` and this app's own checks. Cached DID documents that sign-in tokens are checked against are always the PLC directory's.

**Fallback.** Lodestar is asked only while it reports itself at the directory's tip: the app asks it every 15 s, and every account answer reports too. Anything lodestar cannot answer (an account created seconds ago, a refusal, an error, a timeout, an answer while it is catching up) is asked of `plc_url` and checked by this app, in the same request. When lodestar refuses (403), fails or falls behind, the app stops asking it until it reports at the tip again. Lodestar saying a handle is not verified is never taken: this app checks it itself.

**What you trust it with.** For the reads above, lodestar is taken at its word, as the PLC directory is: a wrong or compromised lodestar could show wrong handles, hide accounts, or point the app at the wrong server for an account it does not index (what it shows from there is not stored). It cannot sign anyone in, grant admin or membership, delete anything, or change stored data. It also learns which accounts and handles this deployment's public requests ask about.

`admin identity compare [--sample N] [DID|HANDLE ...]` resolves accounts both ways, as with the setting empty and as with it set, and prints them side by side with the time each took; it changes nothing. `--sample N` adds N accounts at random from those the app knows. Run it after setting `lodestar_url`:

```bash
docker compose exec app /app/horizon-prism-rs admin identity compare --sample 40 bsky.app
```

Watch `lodestar_fresh` (1 while lodestar is asked), `lodestar_calls_total{endpoint,outcome}` (`forbidden` means this server is not admitted; `behind`, that lodestar is catching up) and `lodestar_skipped_total{reason}`.

### Farsight (block lists)

Farsight is the block-list index of this stack. When `[farsight] url` is set in `config/app.toml` and `FARSIGHT_TOKEN` in `.env`, the app asks it, as a member loads a page, which accounts on that page block the member through a block list, and hides them as it hides a direct block. When Farsight does not answer, pages load as usual without those blocks. Whoever runs that Farsight sees which accounts each member's pages contain, so use an instance you run or trust.

```sh
docker compose exec app /app/horizon-prism-rs admin farsight status
docker compose exec app /app/horizon-prism-rs admin farsight prime <did> [--force]
```

- `status` prints Farsight's own report in brief, and whether the app has stopped calling it after repeated failures (`read_breaker_open`; it tries again after 30 s).
- `prime` asks Farsight to read one account's repository again. Use it when a member's own blocks look out of date. The app does this by itself when a member is enrolled.

Watch `farsight_calls_total{outcome}` for failures, `farsight_auth_failed_total` for a revoked or under-scoped key, and `farsight_coverage_total{level}`: at `partial`, Farsight is still catching up and some list blocks may be missing. `farsight_down` is 1 while Farsight is down: it has answered since `serve` started, its breaker is open, and nothing but timeouts, transport errors and `5xx` came since its last answer. `farsight_fallback_withheld{reason}` is 1 while its breaker is open but it is not down, with `reason` `never_answered` (it has not answered since `serve` started) or `unclean` (something else came since its last answer, such as a `401`, `429` or `400`). Both are read from the breaker once a second, so they go back to 0 when the 30 s open period ends.

### Farsight's fallback and memory (both off)

`[farsight.fallback]` and `[farsight.memory]` in `config/app.toml` are off by default and need `farsight.url`. All twelve of their keys are read when `app` starts; change them and restart.

- **The fallback** asks one other AppView, while Farsight is down, which accounts on a member's page block that member through a list, so those blocks still apply.
- **The memory** keeps the list blocks Farsight (or the fallback) reported for each member, so they keep applying while Farsight gives no answer, and across a restart.

**What the fallback discloses.** Each fallback call tells a third party that this deployment is rendering a page for member A that contains accounts B1 … Bn. The call comes from this deployment's own address and carries its `User-Agent`, which names this host. The other AppView's operator, and any CDN in front of it, can log it; if it answers with a redirect, the host it names gets the same query. A page is any ordinary render: a timeline, a thread, a profile, and also the results of a search or of suggestions, so the accounts sent can be the results of a query the member typed.

The other AppView can also join it with the ordinary profile and thread lookups this deployment sends it. With the default network it is the first network those lookups go to, so within about a second it can receive, from one address, a lookup that names no member and the fallback call that names the member and the page's authors, and learn which thread, profile or feed the member loaded. This is the same kind of disclosure Farsight receives, made to a party you do not run. Pointing `network` at an AppView you run removes the third party only if that AppView answers from its own data; this stack's own AppView does not for someone who is not its member, so it passes the question on.

**When it asks.** Only while all three hold: Farsight has answered since `app` started, its breaker is open, and everything since its last answer was a timeout, a transport error or a `5xx`.
- A Farsight that has never answered this process (a wrong `farsight.url`, a revoked key) never brings it on. After a restart during an outage there is no fallback until Farsight has answered once, so it cannot be switched on in the middle of an outage.
- One `401`, `403`, `429` or `400` during a real outage keeps it off for the rest of that outage.
- A long Farsight outage makes it the steady source for that long.
- Thread Lens and `getRelationships` never ask it, and while Farsight is down Thread Lens shows nothing past a block (bridging is off), with the fallback or without it; it still shows, and flags, replies the thread's author hid. Its answers are that AppView's own view, are never treated as complete, and never override Farsight's.

`farsight_down`, `farsight_fallback_withheld` and `farsight_auth_failed_total` show which state you are in (alert examples below).

**Measure Farsight under load before turning the fallback on.** Farsight gives one key 16 calls in progress. A call counts as a timeout when its own time plus its wait for a place passes the limit it was given: `farsight.timeout_ms` for an ordinary page, `threadlens.verdict_round_timeout_ms` for a Thread Lens round, which sends many calls at once. So what matters is one call's worst-case time multiplied by the calls queued ahead of it. Whoever can push five calls in a row past the limit can bring the fallback on. Measure your own instance end to end, through the network and the proxy, under load. A call this app timed out still holds its place until Farsight finishes it, so raising `farsight.timeout_ms` alone is not enough: size Farsight's places for the key against `farsight.rate_per_sec` as well, or accept that risk.

**Its load on the other AppView.** It sees up to the routing entry's own rate plus `farsight.fallback.rate_per_sec` from this deployment, and may have limits of its own. At the default rate the fallback serves a handful of members active at once. Only a routing entry that needs no sign-in (`auth = "unauth"`) can be named.

**What the memory stores.** One row per (member, account that blocks the member through a list): the list, where the block came from, and when it was last confirmed. That shows which accounts appeared on a member's pages, and roughly when.
- A row is kept `retention_hours` after its last confirmation, so a block lifted meanwhile can keep applying that long; a row can stay on disk up to a day longer before the daily cleanup removes it. A block Farsight later says is gone is forgotten at once.
- Database backups keep the rows for as long as you keep the backups.
- `max_per_viewer` bounds one member, `max_total` the whole memory; in a large community the total is reached first, and the oldest blocks go.
- Rows are kept for members only. `admin farsight forget <did>` clears one member's rows at once.
- Starting `app` with the memory off empties the table.
- Migration 0019 adds the table and is applied like any other, even with both features off: `app` does not start with a migration pending. The table then stays empty.

Watch the fallback with `farsight_fallback_requests_total{endpoint,class}` (every call, by the method being served and how it ended), `farsight_fallback_skipped_total{reason}` (calls not made: `no_budget`, `breaker_open`, `call_cap`, `allowance`, `deadline`, `actor_stop`), `farsight_fallback_discarded_total` (answers dropped because Farsight answered meanwhile), `farsight_fallback_pairs_total{result}` (each account of an answered call: `blocked`, `clear`, `invalid`, `not_found`, `missing`, `dropped`), `farsight_fallback_blocks_applied_total` and the gauge `farsight_fallback_ledger_entries` (accounts it said "nothing" for, held back from being asked again for `cache_ttl_secs`). Watch the memory with `farsight_memory_entries`, `farsight_memory_changes_total{op}`, `farsight_memory_dropped_total{reason}` and `farsight_memory_blocks_applied_total`. `inbound_list_blocks_applied_total` counts every list block applied to a page, from any source.

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
      # No private-tier record or delete has come from the relays for a day.
      # The age counts from the last one the live firehose delivered (at a
      # start, the newest stored private row); it is 0 while there are none.
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
      # The daily authorization re-check (design §6.11.5) has fallen behind its
      # 24 h budget: the oldest due `authz` job has waited more than a day
      # (also logged once as `authz_budget_exceeded`). Raise
      # backfill.per_pds_rate_per_sec or lower community.max_members.
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
      # Owners of a `following` audience whose follows are verified but whose
      # latest commit the firehose has not applied yet: their `following`
      # audiences are read by the owner alone meanwhile. A short spike after
      # commits is normal. A floor that never drains names owners on a PDS no
      # configured relay crawls, or owners waiting on a `#sync`, a hole or a
      # trim gap; each opens at its next commit the firehose applies.
      - alert: FollowingOwnersWaitingFirehose
        expr: following_owners_waiting_firehose > 0
        for: 6h
      # The consumer has spent more than half its time on full, gap-free
      # batches for 15 min: its input is at or near its capacity, or it is
      # recovering from a long stall, and it has no margin for a burst. The
      # same condition logs `firehose_headroom_warn`. Backlog capacity, while
      # there is a backlog to measure, is
      # rate(firehose_entries_processed_backlog_total[5m]) / rate(firehose_consumer_backlog_busy_seconds_total[5m]);
      # it should stay at least twice the input.
      - alert: FirehoseBacklogPersistent
        expr: rate(firehose_consumer_backlog_busy_seconds_total[5m]) > 0.5
        for: 15m
      # The consumer processes less than its stream gains, or does not run.
      - alert: FirehoseConsumerSlowerThanInput
        expr: sum without (path) (rate(firehose_entries_processed_total[5m])) < 0.95 * rate(firehose_stream_entries_added_total[5m])
        for: 5m
      # The consumer group is still missing after the worker should have
      # recreated it, for example a create that keeps failing under `noeviction`.
      - alert: FirehoseGroupMissing
        expr: firehose_consumer_group_present == 0
        for: 2m
      # More than half of the lag sampler's calls failed or timed out: Redis
      # is unreachable, hung or stuck (`LOADING`, `BUSY`).
      - alert: FirehoseSamplerFailing
        expr: sum without (call) (rate(firehose_lag_sample_failures_total[5m])) / sum without (call) (rate(firehose_lag_sample_calls_total[5m])) > 0.5
        for: 5m
      # Farsight has answered since `serve` started, its breaker is open, and
      # nothing but timeouts, transport errors and 5xx came since its last
      # answer: an outage. With the fallback on, it is being asked.
      - alert: FarsightDown
        expr: farsight_down == 1
        for: 5m
      # Farsight's breaker is open and the fallback stays off: it never
      # answered this process (`never_answered`: a wrong `farsight.url` or a
      # key it refuses), or something other than a service failure came since
      # its last answer (`unclean`: a 401, 403, 429 or 400).
      - alert: FarsightFallbackWithheld
        expr: farsight_fallback_withheld == 1
        for: 5m
      - alert: FarsightAuthFailed
        expr: increase(farsight_auth_failed_total[15m]) > 0
      # A subscribed-list sync has been running longer than the stuck-row
      # check allows (1.25 × lists.sync_stuck_secs, 5400 by default): it should
      # have been re-queued. `backfill_jobs{depth="list_sync"}` shows the queue.
      - alert: ListSyncStuck
        expr: list_sync_running_age_seconds > 6750
        for: 15m
      # Searches are stopped by their statement timeout more than once every
      # ten seconds: the search indexes may still be building, or the
      # database is short of resources.
      - alert: SearchTimeouts
        expr: rate(search_timeout_total[5m]) > 0.1
        for: 10m
```

Other series, one line each (the sections above name the rest):

- `private_owners_not_ready_total`: private-post owners whose initial indexing has not finished.
- `private_tier_ops_skipped_total{op}`: private records skipped because the private tier is off in this build.
- `index_request_total{status}`: `requestRecord` calls by the status returned; `indexed` for a record the relays never delivered means the safety net was needed.
- `index_request_catch_up_total{outcome}`: the catch-up a `requestRecord` runs after it, by outcome.
- `settle_uncovered_total{path}`: settles whose revision the listed revisions did not cover, by path.
- `backfill_optional_record_unclear_total{collection}`: profile or `self` record fetches a PDS answered with a bare 404 instead of `RecordNotFound`.
- `retention_deleted_rows_total{table}`: rows retention deleted, by table.
- `retention_task_failures_total{task}`: retention tasks that failed, by task.
- `storage_sample_errors_total{source}`: storage samples (Postgres, Redis) that failed.
- `identity_refresh_total{outcome}`: identity refreshes, by outcome.
- `account_status_changes_total{to}`: account status changes applied, by the new status.
- `member_denials_total`: accounts denied membership.
- `identity_cache_requests_total{cache,result}`: reads of the DID-document caches, `hit` or `miss`; `cache="lodestar"` is lodestar's document cache.
- `lodestar_calls_total{endpoint,outcome}`, `lodestar_call_seconds{endpoint}`, `lodestar_skipped_total{reason}`, `lodestar_batch_filled_total` and `lodestar_fresh`: lodestar's calls and the gate (see "Lodestar (identity reads)").
- `handle_resolutions_total{intent,result}`: handle resolutions; `intent` is `public` for `resolveHandle` and `internal` for indexing work.
- `jwt_fetch_cell_failures_total`: sign-in token issuer lookups that failed.
- `xrpc_auth_total{result}`: sign-in token checks, by result.
- `auth_claims_precheck_failed_total`: tokens whose claims failed the check made before verification; they are charged to their address.
- `auth_member_verify_exhausted_total`: requests answered 429 because a member's own verification limit was spent.
- `auth_budget_exhausted_total`: requests answered 429 because looking up the token's issuer ran out of budget.
- `member_pool_allowance_exhausted_total`: issuer lookups for a member that had used its allowance on the members' part of the PLC budget.
- `member_keys`: ready members whose signing key is held in memory.
- `upstream_skipped_total{network,reason}`: upstream calls not made (`deadline`, `breaker_open`, `rate_limited`).
- `cascade_request_deadline_exceeded_total`: upstream lookups that found the request's 6 s deadline already past.
- `upstream_view_counts_total{source}`: where the counts of a view fetched from another AppView came from.
- `starter_pack_counts_refreshed_total{source}`: starter-pack counts recounted, by source.
- `viewer_private_cache_entries` and `viewer_private_cache_evictions_total`: the per-member cache of private lookups, its size and the entries its bounds pushed out.
- `profile_batch_fetch_total{network,result}`: `getProfiles` calls made for the profiles embedded in a response and for `getProfiles` itself: `ok`, `budget` (cut by the 800 ms window or the request's deadline), `transient` (with refused authentication) or `error`.
- `profile_refresh_queue`, `profile_refresh_dropped_total` and `profile_refresh_completed_total`: accounts waiting for the background profile refresh, accounts refused because the queue held `cache.profile_refresh_max`, and refresh batches finished. Its calls count in `upstream_calls_total{origin="background"}`, at most `background_rate_per_sec` per network.
- `profile_refresh_negative_total`: accounts recorded as absent from every network, which list views then show in the minimal form for `cache.profile_negative_ttl_secs` without asking again.
- `host_budget_buckets`: per-host budget buckets held for the prober, backfill and read fetches.
- `sync_head_check_seconds{path}`: how long a repository head check took; `probe` is the background prober, `read` a private read's head lookup (request time).
- `xrpc_request_seconds{nsid}`: how long each routed XRPC request took, authentication included.
- `private_read_stage_seconds{endpoint,stage}`: a private read's time by stage (`gate`, `reads`, `context`, `rest`); the stages add up to `private_read_unfloored_seconds`.
- `private_read_empty_page_total{endpoint}`: private pages answered with no items and a cursor.
- `sync_divergence_detected_total`: repositories whose head stayed ahead of what was indexed past the grace period; their readiness is cleared and they are checked again.
- `private_read_degraded`: 1 while private reads are degraded (a private relay has never reported, or the slowest one is more than `kryphocron.oracle_freshness_bound_secs` behind), else 0. The start and end are logged as `private_read_degraded_started` (naming the relays behind) and `private_read_degraded_ended`.
- `sync_chain_breaks_total`: commits of tracked repositories that arrived before the commit they follow (a gap or reorder on the relays) and wait until it arrives or the next full sync covers it. Needs a horizon-firehose that carries `since`.
- `sync_chain_behind` and `sync_chain_links`: tracked repositories whose seen-every-commit position is short of what was indexed (or not started yet), and the commits waiting for it. A floor in `sync_chain_behind` names repositories a degraded private read cannot use until their next full sync.
- `private_read_fallback_total{result}`: private reads checked by `private_read.head_fallback` while degraded: `passed`, `behind` (a repository the read depends on has not been seen up to its head), `untracked`, `climb_cap` (more than 64 authors above a reply) and `walk_outside`. Misses also count as `private_read_outcome_total{outcome="fallback_behind"}`.
- `private_read_unavailable_total{kind}`: answers that told a client to try again rather than "not found": `thread` (`getPostThread`'s `503 Unavailable`), `quote` (a quoted private post shown as unavailable) and `listed` (posts named in a response's `unavailable`).
- `xrpc_reindexing_total{site}`: `503 Reindexing` answers to members whose indexing was interrupted after it first finished, by where (`private`, `public`, `threadlens`, `view`), and `getMembership` answers of `reindexing` (`membership`).
- `sync_degraded_authz_enqueued_total`: full syncs queued while degraded for repositories whose seen-every-commit position stayed behind their head past `sync.divergence_grace_secs`; readiness is kept.
- `list_subscription_tx_retries_total`: subscribed-list transactions run again after a deadlock or conflict.
- `list_copy_write_errors_total`: list items that could not be copied for a subscribed list.
- `list_purpose_writes_total{path,result}` and `list_purpose_write_errors_total{path}`: subscribed lists' purposes written (`modlist`, `other`, `none`) and the writes that failed, from the firehose (`ingest`) or a sync (`sync`).
- `list_sync_runs_total{outcome,lane}`: subscribed-list syncs, by outcome and lane.
- `list_sync_stuck_requeued_total`: syncs re-queued by the stuck-row check.
- `list_sync_slow_owners`, `list_sync_cut_owners` and `list_sync_weekly_overdue`: list owners on the slow lane, backing off after a cut, and not fully synced for more than 8 days.
- `xrpc_method_not_implemented_total` and `xrpc_wrong_method_total`: requests for a method this AppView does not have, and for one with the wrong HTTP verb.
- `sort_fallback_total{endpoint}`: listings served in `latest` order because another was asked.
- `search_duration_seconds{endpoint}`, `trends_refresh_seconds` and `suggestions_refresh_seconds`: how long searches, trend refreshes and suggestion refreshes took.
- `refract_reads_total{method,outcome}` and `refract_read_seconds{method}`: Refract calls (`fill`, `getReplies`, `getAuthorFeed`, `getAuthorReposts`, `getPostThread`, `getTimeline`, `listNotifications`, `getUnreadCount`, `searchActors`, `searchActorsTypeahead`; a search with an empty `q` ends `empty`) by how they ended, and how long each took, sign-in check included (a histogram, for p50/p95 per method).
- `refract_candidates_total{source,outcome}`: accounts a `fill` (or the admin) named for checking, by outcome; `not_hosted` is a resolve that found the account on no listed community, `not_hosted_cached` a later `fill` that skipped it because of that (remembered in memory for 24 hours, forgotten when the communities change), `resolve_failed` a resolve that failed for a reason worth trying again.
- `refract_viewer_state_total{field,outcome}`: a visitor's own `like`, `repost`, `following` and `replyDisabled` on restored views, looked up per request: `found`, `none` or `unknown` (no Constellation, no budget, an error); never stored. Only while `refract.visitor_viewer_state` is on (off by default), since the lookups send the reader's account to the configured Constellation.
- `refract_repost_fill_total{outcome}` and `refract_repost_fill_rows_total`: the one-time fill of a restored account's older reposts from Constellation (its newest 200 posts), per account `done`, `left` (it left the restored set mid-fill) or `error` (tried again after 10 minutes), and the rows it wrote.
- `refract_visitor_pairs_total{outcome}`: block checks between a visitor and an author, `answered` or `unanswered` (treated as blocked).
- `standin_hosts_build`: 1 in a test build that can reach private addresses; it never appears in a deploy build.

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
