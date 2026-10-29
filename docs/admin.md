# Administration

Horizon-Prism has two admin surfaces: a web UI at `/admin` and the `admin` CLI. Both run the same admin commands with the same checks. Every change is recorded in the audit log with the operator who made it.

## Web admin UI

### Sign-in

- Operators sign in with their own atproto account, on their account's own server (OAuth). There are no local passwords.
- Only accounts on the operator list may sign in, and only from a server on the allowed-host list. An empty host list allows any server.
- The first operator is created with a one-time setup code (see [running](running.md)). After that, operators add other operators.
- A session ends after 30 days unused or a year in total, and at once if the operator is removed or their account moves to another server.
- `admin.web_ui = false` in `app.toml` removes `/admin` entirely. The CLI then does everything.

Behind a CDN, keep features that inject or rewrite scripts off for `/admin*`. The UI's security policy blocks them.

### Pages

| Page | What it shows or does |
|---|---|
| Home | Status at a glance, and a live feed of the records Horizon-Prism writes (never kryphocron content). |
| Members | List with readiness; add, remove, deny, allow, re-sync. |
| Lookup | Look up an account and what Horizon-Prism holds for it. |
| Labels | Labels the operator applied; add and remove. |
| Settings | One page per group, with the current value, default, file value and when it applies. Also read-only pages for install-time values and the full running configuration (no secrets). |
| Operators | Operators and allowed sign-in hosts. |
| Health | Database, Redis, relays, disk and background checks. |
| Stats | Disk use, database size against its budget, Redis memory, index sizes, requests per method. |
| Jobs | Backfill and sync jobs; retry, scope check. |
| Problems | Dead letters and stream state; replay or drop. |
| Farsight | Farsight's status (when configured). |
| Communities, Refract | Refract (when used). |
| Audit | Every change, who made it, how it ended, and sign-ins. |

With `APP_SUPERVISED=1` (set in the shipped compose file), the UI can restart `app` to apply settings that need a restart.

## CLI

Run it inside the container:

```sh
docker compose exec app /app/horizon-prism-rs admin <group> <command>
```

Commands that change state are queued and executed by the running `app`. The CLI waits for the result (up to `admin.command_timeout_secs`); if it times out, the command keeps running and `admin commands list` shows it. Read-only commands run directly.

| Group | Commands | Purpose |
|---|---|---|
| `member` | `add`, `remove`, `deny`, `allow`, `list` | Membership. `deny` keeps an account out and removes it if it is a member. |
| `operator` | `add`, `remove`, `list`, `host add`, `host remove` | Who may sign in to the admin UI. `operator add` is the break-glass if you are locked out. |
| `settings` | `list`, `get`, `set`, `reset` | Saved settings, as in the UI. |
| `restart` | | Restart `app` under a supervisor. |
| `label` | `add`, `remove`, `list` | Operator labels on accounts and records. |
| `indexing` | `public-scope`, `status` | Turn public indexing on or off; show progress. |
| `scope` | `rebuild` | Run the scope check now. |
| `backfill`, `resync` | | Re-index accounts. `resync --private-and-members` is the recovery step after a relay gap. |
| `deadletter` | `list`, `replay`, `drop`, `sweep` | Records that failed to index. |
| `stream` | `info` | Consumer lag and stream bounds. |
| `commands` | `list` | Queued admin commands and their state. |
| `lists` | `sync`, `purge` | Copies of lists used by members' list mutes and blocks. |
| `farsight` | `status`, `prime`, `forget` | Farsight (optional). |
| `refract` | `status`, `list`, `show`, `check`, `end`, `keep`, `unkeep` | Refract (optional). |
| `identity` | `compare` | Compare identity reads with and without Lodestar. Changes nothing. |
| `keygen` | | Print new signing and correlation keys. Reads no config. |
| `setup-code` | | Print the first-run setup code again. |

Outside `admin`, the binary also has `serve` (the default), `migrate status` and `healthcheck`.

`--help` on any command lists its options.
