# Operations

## Metrics

`app` serves Prometheus metrics at `127.0.0.1:9091/metrics`. Scrape it from the host. [`docker/README.md`](../docker/README.md) has example alert rules and a line for every series.

Key metrics:

| Metric | Watch for |
|---|---|
| `firehose_consumer_lag_entries` | Stream entries not yet processed. Rising for minutes means the consumer is behind. |
| `firehose_worker_delay_seconds` | How far behind live each relay is. |
| `firehose_trim_gap_total` | The stream was trimmed before entries were read. Horizon-Prism re-syncs affected accounts. |
| `firehose_dead_letter_total` | Records that failed to index. |
| `private_read_degraded` | 1 while private reads are degraded. |
| `private_tier_last_event_age_seconds` | Time since the relays last delivered a private record. A day or more: check that the relays crawl your PDS. |
| `members_not_ready_total` | Members still in their first indexing. |
| `backfill_jobs{state}` | Backfill queue; `failed` should stay at 0. |
| `redis_memory_ratio` | Redis memory against `REDIS_MAXMEMORY`. |
| `db_budget_ratio` | Database size against `storage.budget_bytes`. |
| `xrpc_requests_total{nsid,class}`, `xrpc_request_seconds{nsid}` | Requests and latency per method. |
| `upstream_skipped_total{network,reason}` | Upstream calls skipped for rate, deadline or a tripped breaker. |
| `farsight_down`, `farsight_auth_failed_total` | Farsight outage or a bad key (if used). |
| `lodestar_fresh` | 1 while Lodestar is used (if configured). |

## Logs

Every service logs JSON to stdout.

```sh
docker compose logs -f app
docker compose logs --no-log-prefix app | jq -c 'select(.level == "ERROR")'
docker compose logs --no-log-prefix app | jq -c 'select(.target == "audit")'
```

`APP_LOG_LEVEL` sets the filter. Audit events come from kryphocron. The relay sidecars log a metrics line every 10 seconds.

## Degraded mode

Private reads depend on the private relays being current. When the slowest one falls behind (two minutes by default), Horizon-Prism is **degraded**:

- A private read is still served if Horizon-Prism holds every commit the read depends on. Otherwise that post is missing from the answer.
- Responses carry `degraded: true` and a `retryAfter` hint, so clients can say "try again" rather than "not found".
- `private_read_degraded` is 1. The log has `private_read_degraded_started` (naming the relays behind) and `private_read_degraded_ended`.

Public reads are not affected. Degraded mode ends by itself when the relays catch up. See [degraded mode](design/degraded-mode.md).

Related client-visible states:

| Answer | Meaning |
|---|---|
| `400 NotReady` | The member's first indexing has not finished. |
| `503 Reindexing` | The member was ready, but indexing was interrupted (a gap, a divergence) and is being repaired. |

## Common issues

**`app` exits with code 78.** A migration is pending and the gate is closed, or a required key is missing or malformed. The log says which. See [running](running.md).

**A sidecar restarts in a loop.** Usually a stale cursor: the relay no longer has the position the sidecar saved. Run `admin resync --private-and-members` first, then delete that sidecar's cursor key in Redis so it starts from live. The runbook has the exact steps.

**Redis is full.** Redis runs `noeviction`, so at the ceiling it refuses new entries and ingest stops. Raise `REDIS_MAXMEMORY` and recreate `redis`, or trim the stream and accept a gap. Horizon-Prism detects the gap and re-syncs members.

**Private posts never appear.** Check `private_tier_last_event_age_seconds`. The community PDS must have federation on and be crawled by every relay in `firehose.private_relays`. Also check that the author is a ready member.

**A member stays `NotReady`.** See the member's jobs on the admin UI's Members, Lookup or Jobs page. A record that fails to parse on every attempt keeps the member not ready (`authz_backfill_stuck_total`); the `authz_stuck` error log names it.

**Dead letters.** `admin deadletter list`, then `replay` to fetch the record again from its PDS, or `drop`. Replaying one for a member also re-syncs that member.

**Disk filling.** Horizon-Prism warns at 80% and 95% of `storage.budget_bytes`. The admin Health page turns amber under 10% free. Space freed by deletions is reused by Postgres, not returned to the filesystem.

**Slow search after an upgrade.** Search indexes build in the background after some upgrades. `_health` detail shows `search_indexes_ready`.

**Signed-in calls fail with 401.** The caller's token must have `aud` set to `did:web:<your host>` and `lxm` set to the method. Check that `/.well-known/did.json` is reachable and shows your hostname.
