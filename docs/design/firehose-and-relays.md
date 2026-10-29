# Firehose and relays

## Purpose

Horizon-Prism learns about new records from relays. This doc covers how events get from the relays into Postgres, and what happens when a relay or the pipeline drops something.

## Model

### Several relays

- The stack runs one horizon-firehose sidecar per relay. The shipped set is `bsky.network` and `relay.fire.hose.cam`.
- Each relay may miss some repositories or commits; several give better coverage.
- Each sidecar stays on its one relay, with no fallback: a cursor belongs to one relay.
- `firehose_commits_seen_only_by_total{relay}` counts commits only one relay delivered.

### Private relays

[Kryphocron](https://github.com/skydeval/kryphocron) posts and audiences are ordinary records on the author's PDS, and arrive only through the relays.

- `firehose.private_relays` names the relays that must crawl every community PDS (shipped: `bsky` and `microcosm`). Each community PDS needs federation on and must have asked them to crawl it.
- Horizon-Prism keeps a **watermark** per relay. Kryphocron reads depend on the slowest private relay; if one stalls, they go into [degraded mode](degraded-mode.md).
- A private relay that has never reported, or is more than 5 minutes behind, is named in a warning and on the admin Health page.

### The stream

- Sidecars write into one Redis stream. The consumer in the app reads it with a consumer group and acknowledges each entry only after it is written.
- The stream is capped (about 1,500,000 entries, about 5 minutes of combined input). Older entries are trimmed.
- Redis runs with `noeviction`: at its memory limit it refuses new entries, ingest stops and kryphocron reads fail closed until the operator adds memory or trims the stream.

### Duplicates across relays

Every write is last-writer-wins on the commit's `rev`, so a copy no newer than what is stored changes nothing. Deletes leave short-lived tombstones, so a late copy of a create cannot bring a deleted record back.

### Cursors and stale cursors

- Each sidecar saves its relay cursor in Redis and resumes from it after a restart.
- If the relay no longer has that cursor, or breaks protocol, the sidecar **exits** rather than skipping ahead: a visible crash loop. Its watermark stops, so a private relay's stall degrades kryphocron reads.
- To accept the gap, run `admin resync --private-and-members` first, then delete the sidecar's cursor key so it resumes from the live tip. See [operations](../operations.md).

### Gaps in the stream

The consumer checks for trimmed entries on every read. A gap is also recorded when the consumer group is recreated, or on `admin resync`. On a gap, Horizon-Prism:
- clears readiness for every member and kryphocron post owner;
- queues an authorization re-sync for every tracked account, and a full re-sync for every member.

Until an account's re-sync finishes, its kryphocron reads fail closed; members ready before see `503 Reindexing` (see [degraded-mode.md](degraded-mode.md)). Other in-scope accounts are corrected at their next commit or the weekly re-sync.

### Chain breaks

Each commit names its repository's previous commit, so Horizon-Prism knows per repository whether it holds every commit up to a point. A commit that arrives early waits until the missing one arrives or a re-sync covers it. This keeps kryphocron reads safe while a relay is behind.

### Dropped frames and the daily re-sync

The sidecars skip frames they cannot decode and events over 4 MiB, so one crafted commit cannot halt ingest. To cover these:
- every tracked account's authorization records (blocks, audiences, kryphocron posts, and follows for `following` audiences) are listed from its PDS daily and reconciled;
- each kryphocron read checks the author's, reader's and audience owner's latest commit from their PDS, so a dropped latest commit fails closed.

A missed authorization change lasts at most a day while the daily budget holds; `admin resync --private-and-members` fixes it at once.

### Dead letters

- A failing op is retried a few times, then written to a capped dead-letter stream with its repo, rev, path and error. The commit's other ops still apply.
- A dead letter for a member or kryphocron post owner clears that account's readiness and re-syncs it.
- `admin deadletter replay` fetches the record again from its PDS.

### Throughput

- The consumer applies entries in stream order, one transaction per entry; commits that write nothing (most of the network) skip it.
- The target is at least twice the combined input, so a burst or a relay replay does not outrun the stream cap.
- Past the cap, entries are trimmed and a gap is recorded. The consumer then resumes from the oldest entry held, or from the newest if it measured itself slower than its input.
- Metrics and alerts cover lag, backlog time, input versus processing rate, and a missing consumer group.

## Guarantees

- No sidecar skips ahead on its own. A lost span of events is either recorded as a gap and re-synced, or visible as a crash loop.
- A gap or a dropped authorization record never widens access: affected accounts' kryphocron reads fail closed until re-synced.
- Duplicate and reordered deliveries converge to the same state.

## Limits

- Other in-scope accounts are not re-synced after a gap until their next commit or the weekly re-sync.
- Out-of-scope interactions lost in a gap only affect local counts.
- A missed authorization change can last up to a day if every relay dropped it and it was not the repository's latest commit.
- If no private relay crawls a community PDS, its kryphocron posts never arrive. Watch `private_tier_last_event_age_seconds`.
