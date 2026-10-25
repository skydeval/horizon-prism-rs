# Configuration

## How it works

Horizon-Prism reads settings from three places:

| Source | Holds | Changes apply |
|---|---|---|
| `docker/.env` → `APP_*` environment | Hostname, database and Redis URLs, keys, secrets, the migration gate | at restart |
| `docker/config/app.toml` | Every non-secret setting. Every key shown is the default. Unknown keys stop startup. | at restart |
| Admin UI (Settings) | Values saved over `app.toml`, kept in the database | some at once, the rest at restart |

A value saved in the admin UI wins over the file. Horizon-Prism never writes `app.toml`. Saving checks the whole configuration first, so a value it would refuse to start with is never saved. Each setting's page says whether it applies at once or after a restart. `admin settings list|get|set|reset` does the same from the CLI.

## Environment

| Variable | Purpose |
|---|---|
| `APP_PUBLIC_HOSTNAME` | Public DNS name. The AppView's DID is `did:web:<hostname>`. |
| `POSTGRES_USER`, `POSTGRES_PASSWORD`, `POSTGRES_DB` | Database. The password must be hex. |
| `REDIS_MAXMEMORY` | Redis memory ceiling (default `6gb`). |
| `APP_SERVICE_SIGNING_KEY_HEX` | The AppView's signing key, from `admin keygen`. |
| `APP_CORRELATION_KEY_HEX` | Audit correlation key, from `admin keygen`. |
| `APP_PRIVATE_CURSOR_KEY_HEX` | Private-feed cursor key. Required. Same on every replica. |
| `APP_SYSTEM_ACCOUNTS_KEY_HEX` | Optional. Switches system accounts on, and seals and indexes their lists. 64 hex (`admin keygen` prints one, or `openssl rand -hex 32`). Same on every replica. Once lists are stored, keep it: see below. |
| `APP_ALLOW_STARTUP_MIGRATE` | `1` only while applying migrations. |
| `FARSIGHT_TOKEN` | Farsight API key, only if Farsight is used. |
| `CADDY_ACME_EMAIL` | Certificate contact. |
| `APP_LOG_LEVEL` | Log filter, e.g. `info`. |

## Main sections of `app.toml`

| Section | What it sets |
|---|---|
| `[http]`, `[metrics]` | Listen addresses; the one proxy address trusted for client IPs. |
| `[net]` | DNS resolvers for public and background lookups. |
| `[indexing]` | Public indexing: `off` or `community_scoped`. |
| `[scope]` | Which accounts beyond members are indexed, and for how long. |
| `[community]` | Auto-enroll, member cap, readiness retry hints. |
| `[firehose]` | Stream names, batch size, dead letters, which relays are private relays. |
| `[identity]` | PLC directory, caches, optional Lodestar. |
| `[private_read]`, `[private_notify]`, `[private_search]`, `[private_any_pds]` | The private tier. |
| `[sync]`, `[backfill]`, `[index_request]` | Load on PDSes: head checks, backfill rate, catch-up. |
| `[constellation]` | Upstream AppViews by PDS pattern, their rates, and the lookup order. |
| `[microcosm]`, `[counts]` | Constellation and Slingshot URLs; where public counts come from. |
| `[retention]`, `[storage]` | What is deleted and when; the disk budget. |
| `[cache]` | In-process cache sizes. |
| `[rate_limits]`, `[auth]` | Request limits per class; limits on token checks. |
| `[admin]` | Admin UI on/off, session lifetimes, live feed snippets. |
| `[threadlens]` | Who may use Thread Lens and how much one response reads. |
| `[farsight]` | Block-list index (optional), with its fallback and memory. |
| `[refract]` | Restored accounts (optional, off by default). |
| `[plural]` | Pronouns and plural-system reads. |
| `[lists]` | Members' own list mutes and list blocks. |
| `[search]`, `[suggestions]`, `[trends]` | Search limits; suggestion and trend rules. |
| `[video]` | How public video embeds play. |
| `[services.*]` | Forward contact or age assurance to a server you run. |
| `[honest_501]`, `[features]` | Messages for methods not served; features this build lacks. |

The comments in `app.toml` describe every key.

## Setting up a new community

1. **Hostname.** Set `APP_PUBLIC_HOSTNAME=prism.example.org`. Clients address the AppView as `did:web:prism.example.org`. The app publishes its DID document at `/.well-known/did.json`.
2. **Community PDS.** In the `[[constellation.networks]]` entry named `community`, set `pds_patterns`:
   ```toml
   pds_patterns = ["pds.example.org", "*.pds.example.org"]
   ```
   A pattern is an exact host or `*.` plus a suffix. Accounts on these hosts are answered from local data.
3. **Relays.** `[firehose] private_relays` lists the relays that must crawl the community PDS. Private reads wait for the slowest of them. Each label maps to a sidecar's relay URL in `[firehose.relay_labels]`. The sidecar configs are in `docker/firehose/`. See [firehose and relays](design/firehose-and-relays.md).
4. **Members.** Add them with `admin member add` or the admin UI. To let accounts on your PDS join by themselves, set `[community] auto_enroll = true` (off by default). `enroll_pds_patterns` limits which hosts may auto-enroll; empty means the community patterns. `max_members` caps the total. See [membership](design/membership.md).
5. **Public indexing.** A fresh install starts with `public_scope = "off"`: Horizon-Prism serves only what members' private posts, Thread Lens and the Horizon features need, and clients use a public AppView for the rest. Set `community_scoped` to serve public timelines, search and the rest of `app.bsky.*` too. See [indexing and scope](design/indexing-and-scope.md).
6. **Upstreams.** The shipped `[constellation]` entries name Bluesky's public AppView and others. `[constellation.cascade] order` sets the order they are asked for accounts outside your community.

## Optional services

Horizon-Prism runs without any of these. Each is off until configured.

| Service | Setting | What it adds | Without it |
|---|---|---|---|
| Farsight | `[farsight] url` + `FARSIGHT_TOKEN` | Hides accounts that block a member through a block list. Lets Thread Lens show posts past blocks. | Those list blocks are not applied. Thread Lens shows the ordinary thread plus hidden replies, not posts past blocks. A member's own list blocks still work. |
| Farsight fallback and memory | `[farsight.fallback]`, `[farsight.memory]` | Keep list blocks applying while Farsight is down. | List blocks lapse during a Farsight outage. |
| Lodestar | `[identity] lodestar_url` | Faster identity and handle reads from a PLC mirror. | Identities come from `plc_url`, and Horizon-Prism checks handles itself. |
| Constellation, Slingshot | `[microcosm]` | Counts, backlinks and records for content Horizon-Prism does not store; reply lists in threads. | Set `[counts] public_source = "local"` to count only stored data. Threads and counts for outside content are less complete. |
| Refract | `[refract] enabled` and communities | Restored accounts. | Off by default. |

The Farsight fallback sends a member's DID and the accounts on their page to another AppView. Read the warning in `app.toml` before turning it on. Farsight and Lodestar see which accounts your instance asks about, so point them at instances you run or trust.

## Rate limits

Each request class is a token bucket: `per_sec` and `burst`. Anonymous callers are keyed by IP, signed-in callers by account, and a global per-IP bucket backs both. Classes: `anon_read`, `auth_read`, `search_anon`, `search_auth`, `write`, `private`, `thread_lens`, `index_request`, `resolve`, `refract`, `plural_anon`, `plural_auth`, `system_accounts`, plus the service-proxy classes. `[rate_limits.endpoint_class]` moves one method to another class.

## System-accounts key

`APP_SYSTEM_ACCOUNTS_KEY_HEX` is a separate secret on purpose: its lists outlive any rotation of the other keys. Without it the feature is off and nothing is stored. Set it, and `getMembership` lists `account.systemAccounts` for every signed-in account.

Once a list is stored, the key must stay the same. A start with another key, or with none, stops with exit 78 and says why, because the lists could then be neither read nor removed when an account is deleted. There is no rotation in this version. To start over, stop `app` and run, in the database:

```sql
TRUNCATE sysacct_entries, sysacct_lists, sysacct_used_tokens, sysacct_key;
```

Every list is then gone; the next start records the new key.
