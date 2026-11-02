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

The menu groups the pages by task. Appearance sits under your name at the top of the menu.

| Section | Page | What it shows or does |
|---|---|---|
| Overview | Home | Status at a glance, and a live feed of the records Horizon-Prism writes (never kryphocron content). |
| People | Members | List with readiness; add, remove, deny, allow, re-sync; auto-join and the member limit. |
| People | Operators | Operators and allowed sign-in hosts. |
| People | Look up | Look up an account and what Horizon-Prism holds for it, including its Refract state. |
| Moderation | Labels | Labels the operator applied; add and remove. |
| Moderation | Blocks · Farsight | Farsight's status (when configured); the fallback and the memory of blocks. |
| Moderation | Communities · Refract | Refract (when used): the communities and visitors' own likes, an Accounts tab with each account, and a Pending ends tab. |
| Moderation | Thread Lens | Who may use Thread Lens, and its request limit. |
| Data | Indexing | What Horizon-Prism indexes, the switch between kryphocron only and community, and a refill's progress. |
| Data | Storage | Database size against its budget, Redis, account ids, disks and compression; the budget and how long records are kept. |
| Data | Jobs | Backfill and sync jobs; retry, scope check. |
| Data | Problems | Dead letters and stream state; replay or drop. |
| kryphocron | kryphocron | Reads, failed reads and stored counts; how deep threads go, how fresh reads must be, late notifications and how far back search reaches; timing floors under Advanced. |
| Apps | What apps see | Email confirmation, live now, trending topics, texts for features Horizon-Prism does not offer, bookmark counts and where public videos play from. |
| Apps | Extra services | Push notifications, age checks and contact import: passed on to their service or not offered. |
| Apps | Public page | The public page on or off and its name, with a link to it while it is on. |
| System | Health | Database, Redis, relays, disk and background checks. |
| System | Stats | How much is indexed, table sizes, requests per method. |
| System | About this server | Read-only facts about this server. |
| System | Audit log | Every change, who made it, how it ended, and sign-ins. |
| System | All settings | Links to the pages that hold their own settings, and one page per remaining group. Each setting shows its current value, default, file value and when it applies; rarely changed ones sit under Advanced. Also read-only pages for install-time values and the full running configuration (no secrets). |
| Your account | Appearance | Light, dark or the device's mode, a colour scheme and a font, for your account only. |

Old admin addresses redirect to their new pages.

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
