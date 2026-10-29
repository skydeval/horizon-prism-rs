# Degraded mode

## Purpose

What happens when part of ingest falls behind: what clients are told and what the operator sees. When Horizon-Prism cannot show it is current, [kryphocron](kryphocron.md) reads fail closed.

## Model

### When Horizon-Prism is degraded

Horizon-Prism keeps a watermark per relay (see [firehose-and-relays.md](firehose-and-relays.md)). It is **degraded** when any relay in `firehose.private_relays` has never reported, or the slowest one is more than `kryphocron.oracle_freshness_bound_secs` behind (120 s by default). The decision is made once per request, so one response never mixes two answers.

Common causes: a private relay's sidecar in a crash loop, a relay that has stopped crawling, Redis full, or the consumer far behind.

### The head fallback

Normally a kryphocron read relies on the watermark. While degraded, Horizon-Prism checks the read itself instead (`private_read.head_fallback`, on by default): it serves the read only when it holds **every commit**, as an unbroken chain, up to each relevant repository's latest commit as its own PDS reports it. The relevant repositories are the author, the reader, the audience owner and the writers above it in the thread.

- A repository with a broken chain is not used until the missing commit arrives or a re-sync covers it. While degraded, that re-sync is queued after a grace period.
- If the check fails, the read misses.

With the fallback off, every kryphocron read misses until the relays catch up.

### What clients are told

| Situation | Answer |
|---|---|
| Kryphocron list endpoints (feeds, `getPosts`, search, notifications, likes, quotes, replies) while degraded | `degraded: true` and `retryAfter` in the body, and the `Retry-After` header. Same for every caller at that moment. |
| Any of those, degraded or not, when the caller's **own** kryphocron posts could not be served yet | `unavailable: [{uri, reason}]`, with reason `syncing` or `indexing`. Only the caller's own posts. |
| `getPostThread` anchor not served, while degraded | `503 Unavailable` with `retryAfter`, whatever the reason, including posts that do not exist. |
| `getPostThread` anchor not served, not degraded | `503 Unavailable` only for the caller's own post that is temporarily behind; otherwise `400 NotFound`. |
| A quoted kryphocron post not shown, while degraded | `#viewUnavailable` with `retryAfter`, for every such quote. Not degraded: only the caller's own temporarily behind quote; others are `#viewNotFound`. |

`retryAfter` grows with the delay: a quarter of it, between `private_read.degraded_retry_min_secs` (30) and `degraded_retry_max_secs` (300).

### NotReady and Reindexing

- **First indexing:** `400 NotReady`; `getMembership` answers `indexing`.
- **Indexing interrupted after it finished** (a stream gap, a dead letter, the PDS head ahead of what is held): `503 Reindexing` with `retryAfter` (`community.reindex_retry_secs`, 30), and `getMembership` answers `reindexing` with a member's permissions. This applies on kryphocron endpoints, member-only public endpoints and Thread Lens. Clients keep the member signed in and retry.

### Farsight down

Farsight (optional) reports which accounts block a member through a block list.

- If it does not answer, pages load without those list blocks. Direct blocks still apply.
- Thread Lens shows no posts past blocks, but still the ordinary thread and replies hidden by the thread's author.
- Two optional features, both off by default:
  - **fallback:** during an outage, ask one configured AppView instead. That AppView learns which accounts appear on a member's page.
  - **memory:** keep each member's last reported list blocks for a set time, through an outage and a restart.
- See [blocks-and-moderation.md](blocks-and-moderation.md).

### What the operator sees

| Signal | Meaning |
|---|---|
| `private_read_degraded` | 1 while degraded. Start and end are logged, naming the relays behind. |
| `firehose_worker_delay_seconds{relay}` | How far each relay's events are behind. |
| `private_read_fallback_total{result}` | Reads checked by the head fallback, and why any were refused. |
| `sync_chain_behind`, `sync_chain_breaks_total` | Repositories whose commit chain is broken, and how often chains break. |
| `xrpc_reindexing_total`, `members_not_ready_total` | Members being re-indexed or not yet ready. |
| `farsight_down`, `farsight_fallback_withheld` | Farsight outage state. |
| Admin Health page | Private relays behind or never reported. |

## Guarantees

- **Fail closed.** A kryphocron read is served only when its facts are shown current, by the watermark or the head fallback. Otherwise it misses.
- **Never a post the reader may not see.** Degraded mode changes only how freshness is proven and how misses are labelled.
- **No leak through the signal.** `degraded` and `retryAfter` are the same for every caller. While degraded, every unserved thread anchor and quote gets the same answer, whether the post exists or not. Per-post reasons go only to the author.
- `Reindexing` and `getMembership` speak only about the caller's own account.

## Limits

- The head fallback uses PDS heads up to `sync.head_max_age_secs` old (60 s), as normal reads do.
- Account status changes (for example a takedown) carried only by the stalled relay are not seen until it catches up.
- Public likes, reposts, quotes and counts on a kryphocron post are only as fresh as the relays.
- A read that depends on more than 64 thread authors misses while degraded.
- With the fallback off, or a sidecar too old to carry the previous commit's rev, kryphocron reads of active accounts miss for the whole stall.
