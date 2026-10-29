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
| `private_read_degraded` | 1 while kryphocron reads are degraded. |
| `private_tier_last_event_age_seconds` | Time since the relays last delivered a kryphocron record. A day or more: check that the relays crawl your PDS. |
| `members_not_ready_total` | Members still in their first indexing. |
| `backfill_jobs{state}` | Backfill queue; `failed` should stay at 0. |
| `redis_memory_ratio` | Redis memory against `REDIS_MAXMEMORY`. |
| `db_budget_ratio` | Database size against `storage.budget_bytes`. |
| `xrpc_requests_total{nsid,class}`, `xrpc_request_seconds{nsid}` | Requests and latency per method. |
| `upstream_skipped_total{network,reason}` | Upstream calls skipped for rate, deadline or a tripped breaker. |
| `farsight_down`, `farsight_auth_failed_total` | Farsight outage or a bad key (if used). |
| `lodestar_fresh` | 1 while Lodestar is used (if configured). |
| `outline_writer_active`, `outline_through_age_seconds` | The thread outline's writer holds its lock, and how far behind the stream it is (if enabled). |
| `outline_missed_total{reason}`, `outline_holes_total{reason}` | Posts the thread outline could not record, and places it marked as missing a reply. |
| `outline_freshness{state}`, `outline_replays_total{why}` | Whether Thread Lens may use the outline now (1 for the current state), and how often it caught up from the stream, by cause. |
| `threadlens_outline_total{use}` | How each Thread Lens request that could use the outline was served: `tree`, or why not (`absent`, `incomplete`, `hole`, `too_big`, `stale`, `bridging_off`, `crosscheck`, `off`, `error`: the reads failed or took over 300 ms past the baseline). |
| `outline_crosscheck_total{result}` | The anchor's first Constellation page checked against the outline: `match`, `miss` (the request used Constellation instead), `pending`, `late`. |
| `public_feed_reader_lag_seconds` | How far the public page's live reader is behind the stream's own tail, or how long since it last finished a loop. Past 30 s it skips to the tail. |
| `public_feed_late_total` | Live-feed lines released after their half-second slot: Redis or the host is near saturation. Should stay at 0. |
| `public_feed_undecodable_total` | Stream entries the live reader could not read as an event; they give no line. |

## The public page's live feed

`/` shows every event the relays deliver, about half a second after the relay sidecar wrote it. It has its own reader on the firehose stream (plain reads, no consumer group, its own Redis connection), so it never waits on ingest. Cost: a few percent of a Redis core at the highest measured input. A Redis clock more than a second off the host's is logged as `public_feed_clock_offset`.

**Bandwidth.** Every viewer gets every event; `[public_page] live_max_viewers` (default 50) is the only control. Past it, visitors get "busy". At about 43 bytes a line:

| Events a second | Per viewer | 25 viewers | 50 viewers (default) | 100 viewers | 200 viewers |
|---|---|---|---|---|---|
| 800 | ~0.4 Mbit/s | ~10 Mbit/s | ~20 Mbit/s | ~40 Mbit/s | ~80 Mbit/s |
| 4,500 | ~1.7 Mbit/s | ~42 Mbit/s | ~84 Mbit/s | ~170 Mbit/s | ~340 Mbit/s |
| 11,700 | ~4.2 Mbit/s | ~105 Mbit/s | ~210 Mbit/s | ~420 Mbit/s | ~830 Mbit/s |
| 15,300 (a relay replay) | ~5.4 Mbit/s | ~135 Mbit/s | ~270 Mbit/s | ~540 Mbit/s | ~1,080 Mbit/s |

On a metered or small link, lower the cap or set `live_feed = false`. A viewer that cannot keep up is ended with a count of what it missed. Hidden tabs close their stream.

**Proxy.** `/live` is a server-sent event stream: proxy it uncompressed and flushed at once, like `/admin/live`. The shipped Caddyfile's matcher is `@live path /admin/live /live`.

**Client addresses behind a CDN.** Per-address limits (2 live streams, `anon_read`) use the last `X-Forwarded-For` element from `[http] trusted_proxy`. Behind a CDN that is the edge, unless Caddy overwrites it. In Caddy's global options, and in **every** `reverse_proxy` block:

```
{
	servers {
		trusted_proxies static <the CDN's address ranges>
		client_ip_headers CF-Connecting-IP
	}
}

reverse_proxy app:3030 {
	header_up X-Forwarded-For {client_ip}
}
```

Use the header your CDN sets (`CF-Connecting-IP` is Cloudflare's). Without this, everyone behind one edge shares one address's limits.

## Logs

Every service logs JSON to stdout.

```sh
docker compose logs -f app
docker compose logs --no-log-prefix app | jq -c 'select(.level == "ERROR")'
docker compose logs --no-log-prefix app | jq -c 'select(.target == "audit")'
```

`APP_LOG_LEVEL` sets the filter. Audit events come from kryphocron. The relay sidecars log a metrics line every 10 seconds.

## Degraded mode

Kryphocron reads depend on the private relays being current. When the slowest falls behind (two minutes by default), Horizon-Prism is **degraded**:

- A read is still served if every commit it depends on is held. Otherwise that post is missing from the answer.
- Responses carry `degraded: true` and a `retryAfter` hint.
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

**Kryphocron posts never appear.** Check `private_tier_last_event_age_seconds`. The community PDS must have federation on and be crawled by every relay in `firehose.private_relays`. Also check that the author is a ready member.

**A member stays `NotReady`.** See the member's jobs on the admin UI's Members, Lookup or Jobs page. A record that fails to parse on every attempt keeps the member not ready (`authz_backfill_stuck_total`); the `authz_stuck` error log names it.

**Dead letters.** `admin deadletter list`, then `replay` to fetch the record again from its PDS, or `drop`. Replaying one for a member also re-syncs that member.

**Disk filling.** Horizon-Prism warns at 80% and 95% of `storage.budget_bytes`. The admin Health page turns amber under 10% free. Space freed by deletions is reused by Postgres, not returned to the filesystem.

**Slow search after an upgrade.** Search indexes build in the background after some upgrades. `_health` detail shows `search_indexes_ready`.

**Signed-in calls fail with 401.** The caller's token must have `aud` set to `did:web:<your host>` and `lxm` set to the method. Check that `/.well-known/did.json` is reachable and shows your hostname.
