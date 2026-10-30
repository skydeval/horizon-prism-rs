# Configuration

## How it works

Horizon-Prism reads settings from three places:

| Source | Holds | Changes apply |
|---|---|---|
| `docker/.env` → `APP_*` environment | Hostname, database and Redis URLs, keys, secrets, the migration gate | at restart |
| `docker/config/app.toml` | Every non-secret setting. Every key shown is the default. Unknown keys stop startup. | at restart |
| Admin UI (Settings) | Values saved over `app.toml`, kept in the database | some at once, the rest at restart |

A value saved in the admin UI wins over the file, which Horizon-Prism never writes. A save is checked against the whole configuration first. Each setting's page says when it applies. `admin settings list|get|set|reset` does the same from the CLI.

## Environment

| Variable | Purpose |
|---|---|
| `APP_PUBLIC_HOSTNAME` | Public DNS name. The AppView's DID is `did:web:<hostname>`. |
| `POSTGRES_USER`, `POSTGRES_PASSWORD`, `POSTGRES_DB` | Database. The password must be hex. |
| `REDIS_MAXMEMORY` | Redis memory ceiling (default `6gb`). |
| `APP_SERVICE_SIGNING_KEY_HEX` | The AppView's signing key, from `admin keygen`. |
| `APP_CORRELATION_KEY_HEX` | Audit correlation key, from `admin keygen`. |
| `APP_PRIVATE_CURSOR_KEY_HEX` | Cursor key for kryphocron feeds. Required. Same on every replica. |
| `APP_SYSTEM_ACCOUNTS_KEY_HEX` | Optional. Switches system accounts on and seals their lists. 64 hex (`admin keygen` or `openssl rand -hex 32`). Same on every replica; keep it once lists exist (below). |
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
| `[private_read]`, `[private_notify]`, `[private_push]`, `[private_search]`, `[private_any_pds]` | [kryphocron](design/kryphocron.md) reads, notifications, push and search. |
| `[sync]`, `[backfill]`, `[index_request]` | Load on PDSes: head checks, backfill rate, catch-up. |
| `[constellation]` | Upstream AppViews by PDS pattern, their rates, and the lookup order. |
| `[microcosm]`, `[counts]` | Constellation and Slingshot URLs; where public counts come from. |
| `[retention]`, `[storage]` | What is deleted and when; the disk budget. |
| `[cache]` | In-process cache sizes. |
| `[rate_limits]`, `[auth]` | Request limits per class; limits on token checks. |
| `[admin]` | Admin UI on/off, session lifetimes, live feed snippets. |
| `[public_page]` | The public page at `/` and its live relay feed. See below. |
| `[threadlens]` | Who may use Thread Lens and how much one response reads. |
| `[thread_outline]` | Who replied to whom, for Thread Lens. Off by default; see below. |
| `[farsight]` | Block-list index (optional), with its fallback and memory. |
| `[refract]` | Refract (optional, off by default). |
| `[plural]` | Pronouns and plural-system reads. |
| `[lists]` | Members' own list mutes and list blocks. |
| `[search]`, `[suggestions]`, `[trends]` | Search limits; suggestion and trend rules. |
| `[video]` | How public video embeds play. |
| `[services.*]` | Forward contact or age assurance to a server you run. |
| `[honest_501]`, `[features]` | Messages for methods not served; features this build lacks. |

The comments in `app.toml` describe every key.

## Setting up a new community

1. **Hostname.** Set `APP_PUBLIC_HOSTNAME=prism.example.org`. Clients address the AppView as `did:web:prism.example.org`; its DID document is at `/.well-known/did.json`.
2. **Community PDS.** In the `[[constellation.networks]]` entry named `community`, set `pds_patterns`:
   ```toml
   pds_patterns = ["pds.example.org", "*.pds.example.org"]
   ```
   A pattern is an exact host or `*.` plus a suffix. Accounts on these hosts are answered from local data.
3. **Relays.** `[firehose] private_relays` lists the relays that must crawl the community PDS; kryphocron reads wait for the slowest. Each label maps to a sidecar's relay URL in `[firehose.relay_labels]`; the sidecar configs are in `docker/firehose/`. See [firehose and relays](design/firehose-and-relays.md).
4. **Members.** Add them with `admin member add` or the admin UI. `[community] auto_enroll = true` (off by default) lets accounts on your PDS join by themselves; `enroll_pds_patterns` limits the hosts (empty means the community patterns), and `max_members` caps the total. See [membership](design/membership.md).
5. **Public indexing.** A fresh install starts with `public_scope = "off"`: only what kryphocron, Thread Lens and the Horizon features need, and clients use a public AppView for the rest. `community_scoped` also serves public timelines, search and the rest of `app.bsky.*`. See [indexing and scope](design/indexing-and-scope.md).
6. **Upstreams.** `[constellation.cascade] order` sets the order upstream AppViews are asked for accounts outside your community.

## Optional services

Horizon-Prism runs without any of these. Each is off until configured.

| Service | Setting | What it adds | Without it |
|---|---|---|---|
| Farsight | `[farsight] url` + `FARSIGHT_TOKEN` | Hides accounts that block a member through a block list. Lets Thread Lens show posts past blocks. | Those list blocks are not applied, and Thread Lens shows no posts past blocks. A member's own list blocks still work. |
| Farsight fallback and memory | `[farsight.fallback]`, `[farsight.memory]` | Keep list blocks applying while Farsight is down. | List blocks lapse during a Farsight outage. |
| Lodestar | `[identity] lodestar_url` | Faster identity and handle reads from a PLC mirror. | Identities come from `plc_url`, and handles are checked locally. |
| Constellation, Slingshot | `[microcosm]` | Counts, backlinks and records for content not stored here; reply lists in threads. | Set `[counts] public_source = "local"`. Threads and counts for outside content are less complete. |
| Refract | `[refract] enabled` and communities | See [Refract](design/refract.md). | Off by default. |
| Push service | `[private_push] url` + `service_did` | New kryphocron notifications wake members' devices. | Apps find them when opened. |

The Farsight fallback sends a member's DID and the accounts on their page to another AppView; read the warning in `app.toml` first. Farsight and Lodestar see which accounts you ask about, so use instances you run or trust. The push service sees which members have new kryphocron notifications and when, by DID alone; it must list this AppView's DID (`did:web:<APP_PUBLIC_HOSTNAME>`) as a sender.

## Thread outline

`[thread_outline]` keeps the reply structure of every public post the firehose delivers (author, record key, parent, root, time, rev, a deleted mark; no text). When it is fresh, holds the whole thread and Farsight is configured, Thread Lens's `getThread` reads parents and replies from it in one batch instead of walking Constellation level by level. Anything it cannot vouch for falls back to Constellation for that request.

It records every author network-wide, whatever `public_scope` says: about 3 million posts a day, about 17 GB at the default 30-day window. The daily retention run deletes older rows.

| Key | Default | Meaning |
|---|---|---|
| `enabled` | `false` | Record the outline. |
| `window_days` | `30` | Days a post's structure is kept (2 to 365). |
| `queue_messages` | `50000` | Messages queued for the outline writer. A full queue never slows ingest; the writer catches up from the stream. |
| `flush_ms` | `250` | How often the writer writes a batch. |
| `settle_secs` | `900` | Threads count as complete only for roots seen this long after coverage began. |
| `fresh_secs` | `30` | How far behind the stream the outline may be and still be used. |
| `max_tree` | `5000` | Rows read under an anchor. A larger subtree skips the outline, remembered for 10 minutes. |
| `max_thread` | `20000` | Rows of one thread scanned to check nothing is missing. A larger thread skips the outline. |
| `enforce_reserve` | `100` | Verdict lookups (of `threadlens.verdict_max_dids`) kept back for embedded posts. |

All keys apply at restart. Switching it off clears its progress; switching it on again starts coverage afresh. One process writes at a time (an advisory lock); a second waits and takes over. Watch `outline_writer_active`, `outline_through_age_seconds` and `outline_missed_total`.

## Public page

`[public_page]` sets the read-only page at `/` (see the [API guide](api.md#the-instance-page)). All keys apply at restart and are read-only in the admin UI.

| Key | Default | Meaning |
|---|---|---|
| `enabled` | `true` | Serve the page. `false`: `/` is a 404. |
| `name` | `""` | The page's title; empty is `APP_PUBLIC_HOSTNAME`. |
| `about` | `""` | Plain text, a blank line starts a paragraph, at most 4,000 characters. |
| `contact` | `""` | One line; an `https:` or `mailto:` value is a link. |
| `source_url` | `""` | A link to the source (`https:`). |
| `live_feed` | `true` | The live relay feed at `/live`. `false` mounts no `/live` and `/` loads no script. |
| `live_max_viewers` | `50` | Live-feed viewers at once, at least 1. Each viewer costs about 0.4 Mbit/s at 800 events a second, about 5.4 during a relay replay ([operations](operations.md#the-public-pages-live-feed)). |
| `clients` | `[]` | Apps that use this instance, as `[[public_page.clients]]` with `name` and an `https:` `url`; at most 20. |

A link that is not `https:` stops the start. The page names no relay, only how many it receives from. The removed keys `self_check`, `policy_url` and `docs_url` stop the start like any unknown key.

## Rate limits

Each request class is a token bucket: `per_sec` and `burst`. Anonymous callers are keyed by IP, signed-in callers by account, and a global per-IP bucket backs both. Classes: `anon_read`, `auth_read`, `search_anon`, `search_auth`, `write`, `private`, `thread_lens`, `index_request`, `resolve`, `refract`, `plural_anon`, `plural_auth`, `system_accounts`, plus the service-proxy classes. `[rate_limits.endpoint_class]` moves one method to another class.

## System-accounts key

`APP_SYSTEM_ACCOUNTS_KEY_HEX` is separate from the other keys so lists survive their rotation. Without it the feature is off and nothing is stored. With it, `getMembership` lists `account.systemAccounts` for every signed-in account.

Once a list is stored, keep the key and back it up. If a start finds lists but no key, or another key, Horizon-Prism starts with only system accounts **locked**:

- the five methods answer 501 ("System accounts are unavailable on this AppView for now; lists are kept"), and `getMembership` leaves `account.systemAccounts` out;
- no entry is read, added or removed (a deleted account's own list is still deleted);
- every identity, account, handle and tombstone event is recorded in `sysacct_pending` (up to a few hundred thousand short rows a day on the full network);
- the log (`system_accounts_locked`) and the Health page ("System accounts", red) say which case it is.

Set the right key and restart. Accounts deleted meanwhile leave every list, the recorded accounts are re-checked by the identity refresher, and the methods serve again once that is done: usually minutes, up to a few hours (Health is amber meanwhile; the start is never held). Every replica needs the same key: one started without it is locked and keeps recording, and a replica starting meanwhile waits to serve until none is locked. Catching up holds the recorded rows in memory, about 100 MB per week the key was missing; after a long lock, give the process room or empty `sysacct_pending` first (deletions seen meanwhile then reach lists only through later events).

A key that is not 64 hex characters stops the start (exit 78). There is no rotation. To start over, stop `app` and run:

```sql
TRUNCATE sysacct_entries, sysacct_lists, sysacct_used_tokens, sysacct_key, sysacct_pending;
```

Every list is then gone; the next start records the new key.
