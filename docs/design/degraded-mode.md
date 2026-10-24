# Degraded mode

## Purpose

Private reads depend on how current Horizon-Prism is. This doc covers what Horizon-Prism does when part of ingest falls behind, what clients are told, and what the operator sees. The rule throughout: when Horizon-Prism cannot show it is current, private content fails closed.

## Model

### When Horizon-Prism is degraded

Horizon-Prism keeps a watermark per relay (see [firehose-and-relays.md](firehose-and-relays.md)). It is **degraded** when any relay in `firehose.private_relays` has never reported, or the slowest one is more than `kryphocron.oracle_freshness_bound_secs` behind (120 s by default). The decision is made once per request, so one response never mixes two answers.

Common causes: a private relay's sidecar in a crash loop, a relay that has stopped crawling, Redis full, or the consumer far behind.

### The head fallback

Normally a private read relies on the watermark: if every private relay is current, the facts that decide the read are current too.

While degraded, Horizon-Prism checks the specific read instead (`private_read.head_fallback`, on by default). It serves the read only when, for every repository the decision depends on, Horizon-Prism holds **every commit** up to that repository's latest commit as reported by its own PDS. Those repositories are the author, the reader, the audience owner, and the authors of the replies above it in a private thread.

- The latest commit comes from the PDS, not the relays, so a relay stall does not hide it.
- "Every commit" means an unbroken chain of commits, not just the newest one. A commit one relay dropped while another relay delivered the next one is caught.
- A repository whose chain is broken is not used until the missing commit arrives or a re-sync covers it. While degraded, Horizon-Prism queues that re-sync itself after a grace period.
- If the check fails, the read misses as it would without the fallback.

With the fallback off, every private read misses until the relays catch up.

### What clients are told

| Situation | Answer |
|---|---|
| Private list endpoints (feeds, `getPosts`, search, notifications, likes, quotes, replies) while degraded | `degraded: true` and `retryAfter` in the body, and the `Retry-After` header. Same for every caller at that moment. |
| Any of those, degraded or not, when the caller's **own** private posts could not be served yet | `unavailable: [{uri, reason}]`, with reason `syncing` or `indexing`. Only the caller's own posts. |
| `getPostThread` anchor not served, while degraded | `503 Unavailable` with `retryAfter`, whatever the reason, including posts that do not exist. |
| `getPostThread` anchor not served, not degraded | `503 Unavailable` only for the caller's own post that is temporarily behind; otherwise `400 NotFound`. |
| A quoted private post not shown, while degraded | `#viewUnavailable` with `retryAfter`, for every such quote. Not degraded: only the caller's own temporarily behind quote; others are `#viewNotFound`. |

`retryAfter` grows with the delay: a quarter of it, between `private_read.degraded_retry_min_secs` (30) and `degraded_retry_max_secs` (300).

### NotReady and Reindexing

A member's account must finish indexing before member-only endpoints answer.

- **First indexing:** `400 NotReady`, and `getMembership` answers `indexing`.
- **Indexing interrupted after it first finished** (a stream gap, a dead letter, the account's PDS head running ahead of what Horizon-Prism holds): `503 Reindexing` with `retryAfter` (`community.reindex_retry_secs`, 30), and `getMembership` answers `reindexing` with a member's permissions. This applies on private endpoints, member-only public endpoints and Thread Lens. A client should keep the member signed in as a member and retry.

### Farsight down

Farsight is optional. It reports which accounts block a member through a block list.

- If Farsight does not answer, pages load without those list blocks. Direct blocks still apply.
- Thread Lens shows no posts past blocks while Farsight is down. It still shows the ordinary thread and replies hidden by the thread's author.
- Two optional features soften this, both off by default:
  - **fallback:** during a real Farsight outage, ask one configured AppView the same question. This discloses to that AppView which accounts appear on a member's page.
  - **memory:** keep the list blocks Farsight last reported for each member, for a set time, so they keep applying through an outage and a restart.
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

- **Fail closed.** A private read is served only when Horizon-Prism can show the facts that decide it are current: by the watermark, or by the head fallback's per-read check. Otherwise it misses.
- **Never a post the reader may not see.** Degraded mode changes only how freshness is proven and how misses are labelled. It never admits a read the authorization rules refuse.
- **No leak through the signal.** `degraded` and `retryAfter` are global and identical for every caller. While degraded, every private thread anchor and quote that is not served gets the same answer, whether the post exists or not. Per-post reasons go only to the post's author.
- `Reindexing` and `getMembership` speak only about the caller's own account.

## Limits

- The head fallback uses PDS heads up to `sync.head_max_age_secs` old (60 s), as normal reads do.
- Account status changes (for example a takedown) carried only by the stalled relay are not seen until it catches up.
- Public likes, reposts and quotes shown beside a private post, and counts on a served post, are only as fresh as the relays.
- A read that depends on more than 64 thread authors misses while degraded.
- With the fallback off, or a sidecar too old to carry the previous commit's rev, private reads of active accounts miss for the whole stall.
