# Firehose and relays

## Purpose

Horizon-Prism learns about new records from relays. This doc covers how events get from the relays into Postgres, and what happens when a relay or the pipeline drops something.

## Model

### Several relays

- The stack runs one horizon-firehose sidecar per relay. The shipped set is `bsky.network` and `relay.fire.hose.cam`.
- Each relay may miss some repositories or commits. Reading several gives better coverage than any one.
- Each sidecar stays on its one relay. It has no fallback relay, because a cursor belongs to one relay and failing over would resume at the live tip and skip events.
- Horizon-Prism counts commits that only one relay delivered (`firehose_commits_seen_only_by_total{relay}`), so the operator can see what each relay adds.

### Private relays

Private posts and their audiences are ordinary records on the community PDS. Horizon-Prism gets them only through the relays.

- `firehose.private_relays` names the relays that must crawl every community PDS (shipped: `bsky` and `microcosm`).
- Each community PDS must have federation on and must have asked those relays to crawl it.
- Horizon-Prism keeps a **watermark** per relay: how far it has applied that relay's events. Private reads depend on the slowest private relay. If one stalls, private reads go into [degraded mode](degraded-mode.md).
- A private relay that has never reported, or is more than 5 minutes behind, is named in a warning and on the admin Health page.

### The stream

- Sidecars write into one Redis stream. The consumer in the app reads it with a consumer group and acknowledges each entry only after it is written.
- The stream is capped (about 1,500,000 entries, about 5 minutes across three relays). Older entries are trimmed as new ones arrive.
- Redis runs with `noeviction`. If it reaches its memory limit it refuses new entries instead of dropping data silently. Ingest then stops and private reads fail closed until the operator adds memory or trims the stream.

### Duplicates across relays

The same commit usually arrives once per relay. Every write is last-writer-wins on the commit's `rev`: a copy that is not newer than what is stored changes nothing. Deletes leave short-lived tombstones, so a slow relay's late copy of a create cannot bring a deleted record back. The final state does not depend on which relay delivered first.

### Cursors and stale cursors

- Each sidecar saves its relay cursor in Redis and resumes from it after a restart.
- If the relay no longer has that cursor, or the relay breaks protocol, the sidecar **exits** instead of skipping ahead. It restarts and exits again: a visible crash loop. Its watermark stops, so if it is a private relay, private reads degrade.
- Recovery is the operator's choice to accept the gap: first run `admin resync --private-and-members` (this clears readiness and queues re-syncs), then delete the sidecar's cursor key so it resumes from the live tip. See [operations](../operations.md).

### Gaps in the stream

The consumer checks for trimmed entries on every read. A gap is also recorded when the consumer group has to be recreated, or the operator runs `admin resync`. On a gap, Horizon-Prism:
- clears readiness for every member and private-post owner;
- queues an authorization re-sync for every tracked account, and a full re-sync for every member.

Until an account's re-sync finishes, its private reads fail closed. Members who had finished indexing before see `503 Reindexing` instead of `NotReady` (see [degraded-mode.md](degraded-mode.md)). Other in-scope accounts are corrected at their next commit or by the weekly re-sync.

### Chain breaks

Each commit names the previous commit of its repository. Horizon-Prism uses this to tell, per tracked repository, whether it holds every commit up to a point. A commit that arrives before the one it follows waits until the missing one arrives from any relay, or a re-sync covers it. This is what lets private reads go on safely while a relay is behind.

### Dropped frames and the daily re-sync

The sidecars skip frames they cannot decode and events over 4 MiB. Stopping instead would let one crafted commit halt all ingest. To cover these:
- every tracked account's authorization records (blocks, audiences, private posts, and follows for `following` audiences) are listed from its PDS once a day and reconciled;
- each private read also checks the author's, reader's and audience owner's latest commit from their PDS, so a dropped latest commit fails closed.

A missed authorization change in one repository lasts at most a day while the daily budget holds. The operator can run `admin resync --private-and-members` at once.

### Dead letters

- A failing op is retried a few times, then written to a capped dead-letter stream with its repo, rev, path and error. Other ops of the same commit still apply. Ingest never blocks on one bad record.
- A dead letter for a member or private-post owner clears that account's readiness and re-syncs it.
- `admin deadletter replay` fetches the record again from its PDS.

### Throughput

- The consumer applies entries in stream order, one transaction per entry. Commits that can write nothing (accounts out of scope) skip the transaction; that is most of the network.
- The target is a backlog capacity of at least twice the combined input, so a burst or a relay replay does not outrun the stream cap.
- If the consumer falls behind past the cap, entries are trimmed and a gap is recorded. After a gap the consumer resumes from the oldest entry still held, unless it has measured itself slower than its input; then it resumes from the newest, so it can catch up.
- Metrics and alerts cover lag, backlog time, input versus processing rate, and a missing consumer group.

## Guarantees

- No sidecar skips ahead on its own. A lost span of events is either recorded as a gap and re-synced, or visible as a crash loop.
- A gap or a dropped authorization record never widens access: affected accounts' private reads fail closed until re-synced.
- Duplicate and reordered deliveries converge to the same state.

## Limits

- Changes by in-scope accounts that are not members or private-post owners are not re-synced after a gap until their next commit or the weekly re-sync.
- Out-of-scope interactions lost in a gap only affect local counts.
- A missed authorization change can last up to a day if every relay dropped it and it was not the repository's latest commit.
- If no private relay crawls a community PDS, its private posts never arrive. Watch `private_tier_last_event_age_seconds`.
