# Pronouns and plural systems

Methods `app.nearhorizon.plural.getRecords` and `app.nearhorizon.plural.listMembers`. On by default; optional.

## Purpose

Horizon shows pronouns and plural-system details (system, who is fronting, members) next to accounts and posts. These live as records in each author's repository. Without an AppView, a reader's browser would have to fetch them from every author's PDS: slow, and it shows each author's server who is reading.

Horizon-Prism serves these records for **any account**, member or not, **without sign-in**, so a reader's browser never contacts an author's PDS.

## Model

### The records

| Collection | Record key | Holds |
|---|---|---|
| `app.nearhorizon.actor.pronouns` | `self` | the account's pronouns |
| `app.nearhorizon.plural.system` | `self` | the plural system |
| `app.nearhorizon.plural.front` | `self` | who is fronting now |
| `app.nearhorizon.plural.member` | any (Horizon writes TIDs) | one system member |

Horizon-Prism returns each record value as written. It does not interpret the fields.

### Storage

Records of these four collections are stored **from the firehose, for any author, in every public-indexing mode**. Nothing else of the author is stored because of them. Creates, updates and deletes all apply; a delete always leaves a tombstone, so a late listing cannot bring the record back. A record over 64 KiB is stored without its value. A deleted account's records are purged.

Storing happens whatever `plural.enabled` says, so turning serving on needs no catch-up.

### First ask: listing the author's server

The firehose alone cannot prove a record is **absent**: the record may predate Horizon-Prism's stream. So the first time an account is asked about, Horizon-Prism lists its repository once (`listRecords` for each of the four collections; members up to `member_pages` pages of 100) and reconciles the result with what the firehose stored. From then on the stored rows are the repository's state, kept current by the firehose.

- A call waits up to `inline_wait_ms` (default 1.5 s) for listings it started, then answers with what is known.
- A deactivated, taken-down or missing repository answers `unknown`, and its stored records are deleted. A tombstoned DID answers `none`.
- Failed listings retry with backoff, up to every 6 hours.
- **Reverify.** A listed account asked about again after `reverify_hours` (default 24) is listed again in the background. This repairs anything a missed firehose event left behind.
- An account or identity event for an account with stored records lists it again about 30 s later, so deactivations and deletions are seen even if nobody asks.
- Listings are bounded by `fill_concurrency`, `fill_rate_per_sec`, `per_host_rate_per_sec` and `max_pending`, whatever callers do.

Listings send nothing about the reader: the author's PDS sees only Horizon-Prism.

### Answers

Each record gets one of three answers:

| Answer | When |
|---|---|
| `record` | stored; returned with its URI, CID and value |
| `none` | Horizon-Prism listed the repository, has seen every change since, and there is no such record |
| `unknown` | anything else: not listed yet, listing failed, repository not active, account hidden by moderation, record too large, or the firehose is behind |

**Lag guard.** `none` requires the firehose to be within `max_stream_lag_secs` (default 120) of now. Past that delay, nothing is answered as absent; it is `unknown` instead.

## Methods

**`getRecords {actors?, members?}`**: for each DID in `actors`, its `pronouns`, `system` and `front` answers; for each member URI (`at://<did>/app.nearhorizon.plural.member/<rkey>`), its answer. At most 50 accounts in total (member URIs' accounts included) and 100 member URIs. Answers come in request order, each once.

**`listMembers {actor, limit?, cursor?}`**: an account's member records in ascending record-key order, with a `status`:

- `complete`: every member is listed across the pages;
- `partial`: Horizon-Prism cannot promise that (a very large system, the firehose behind, a member it could not show);
- `unknown`: not listed yet, or the account is hidden or unavailable; `members` is empty.

Both are GET queries, served in every public-indexing mode, with CORS like every route. Errors: `InvalidRequest`, `RateLimitExceeded`, `MethodNotImplemented`.

**Caching.** `cache-control: public, max-age=30` when no answer is `unknown`. With any `unknown`: `max-age=5` signed in, `max-age=30` signed out.

**Rate limits.** Both methods are in their own family:

| Caller | Class | Keyed by | Default |
|---|---|---|---|
| signed out | `plural_anon` | client IP (IPv6 per /64) | 1/s, burst 10 |
| valid service token | `plural_auth` | account | 5/s, burst 30 |
| token that does not verify | refused (`InvalidToken`) | | |

**Turning it off.** `plural.enabled = false` makes both methods answer 501 `MethodNotImplemented` and stops the listings. Records are still stored from the firehose. A client should treat 501 (or 404 from an AppView without these methods) as "not served here".

**Discovery.** Signed in, `getMembership.features` lists `plural.read` while enabled. Signed out, the method answering is the discovery.

### Fronters on posts

A post record may carry a top-level `app.nearhorizon.plural.fronters` key: `{members: [{id, status?}]}`, 1 to 20 entries, each `id` a member record key. Every post view Horizon-Prism builds keeps the key as written, for public posts and for private posts alike (on private posts it is stored in the clear beside the encoded payload; an invalid value is dropped and the post still ingests). `getMembership.features` lists `plural.fronters`.

## Guarantees

- A reader's browser never needs to contact an author's PDS for these records.
- `none` is never a guess: it is answered only after a listing, with every change since seen, and the firehose current.
- No reader data is sent to an author's PDS.
- Accounts hidden by moderation answer `unknown`, not their records.

## Limits

- An account's records are complete only from its first listing; until then readers get `record` for what the firehose stored and `unknown` otherwise.
- A missed firehose event can leave a record stale until the next reverify.
- Systems with more than `member_pages × 100` members answer `partial`.
- How fast an account answers shows that someone asked about it before. It does not show who.
- Labels from outside labelers on member records are not served.

See [configuration](../configuration.md) for `[plural]` and `[rate_limits]`, and [private posts](private-posts.md) for the private-post read path.
