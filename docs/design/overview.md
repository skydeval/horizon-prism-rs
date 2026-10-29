# Overview

## Purpose

Horizon-Prism is an atproto AppView for a community. It serves the `app.bsky.*` API to its members, plus Horizon's `app.nearhorizon.*` extensions: [kryphocron](https://github.com/skydeval/kryphocron) posts, Thread Lens, Refract, pronouns and plural, index requests and `getMembership`.

It does not replace the public network. It indexes the community and the accounts the community touches. For anything else it asks other AppViews and public indexes, in an order the operator sets.

Horizon-Prism only reads. It never writes to a repository. Clients such as Horizon write records to their own PDS, and Horizon-Prism picks them up from the relays.

## Model

```
  relays (bsky.network, relay.fire.hose.cam, ...)
     │   one horizon-firehose sidecar per relay
     ▼
  Redis stream  (capped, noeviction)
     │
     ▼
  consumer (in the app) ──► Postgres ◄── worker: backfill, re-syncs ◄── PDSes
                               │
  clients ──► XRPC (app) ──────┘   + in-process caches
                 │
                 ├──► upstream AppViews, in cascade order
                 ├──► Constellation / Slingshot (microcosm)
                 ├──► PLC directory, optional Lodestar
                 └──► optional Farsight (block lists)
```

### Ingest

- Each relay has its own horizon-firehose sidecar. Each sidecar writes decoded events into one Redis stream.
- The consumer runs inside the app. It reads the stream in order and writes to Postgres.
- Every write is last-writer-wins on the record's commit `rev`, so the same commit arriving from several relays is applied once.
- See [firehose-and-relays.md](firehose-and-relays.md).

### Community scope

- **Members** are the accounts the operator adds (or, if auto-enroll is on, accounts on a matching PDS host the first time their client loads a timeline). Members can be on any PDS.
- **Scope** is the members plus the accounts they touch: follows, and accounts they recently replied to, quoted, liked or reposted. The scope policy and its time windows are configurable.
- Horizon-Prism stores records only for accounts in scope, plus the interactions of others with members' content.
- See [indexing-and-scope.md](indexing-and-scope.md) and [membership.md](membership.md).

### The worker

The app runs background workers beside the consumer:
- **Backfill** fills a new member's or new in-scope account's history from its PDS.
- **Re-syncs** list each tracked account's authorization records daily, and re-sync scope weekly. They repair anything the relays dropped.
- **Head checks** keep each tracked account's latest commit from its PDS, so kryphocron reads can prove Horizon-Prism is up to date for that account.

Each PDS host has its own request budget, so a community PDS sees bounded load.

### Read path

- XRPC requests are served from Postgres.
- Hot data (profiles, counts, identity documents, kryphocron lookups) is cached in the app process. Redis holds no request caches.
- Kryphocron posts are checked on every request. See [kryphocron.md](kryphocron.md).

### The upstream cascade

When a request needs an account or record outside scope, Horizon-Prism asks other sources in order:
1. its own index, for accounts in scope;
2. the account's home AppView, matched by its PDS host;
3. the other enabled AppViews, in the configured `cascade.order` (the shipped config lists Bluesky's AppView, `api.bsky.app`; not `public.api.bsky.app`, which applies only Bluesky's own labeler);
4. Slingshot for a single record, or the author's PDS or Constellation for lists.

Constellation also supplies counts and backlinks for content Horizon-Prism does not fully index. Each upstream has its own rate bucket, breaker and per-request deadline. Labels from upstream AppViews are stripped; moderation is local.

### Identity

- DIDs resolve through the PLC directory (`plc.directory` by default) or `did:web`. Handles are verified both ways.
- **Lodestar** is optional. It is a PLC mirror that also verifies handles. With it set, public identity lookups ask it first and fall back to the PLC directory. Sign-in, admin and membership always use the PLC directory.

### kryphocron

The kryphocron library handles authorization and audit:
- it verifies service tokens from callers' PDSes;
- it decides each kryphocron read, and refuses one whose facts are older than a freshness bound;
- it writes audit events. If the audit path fails, the health check reports unhealthy until the app restarts.

## Guarantees

- Horizon-Prism never writes to any repository.
- A kryphocron post is served only to a reader its rules admit, from current facts. Otherwise the read fails closed.
- Duplicate or out-of-order deliveries from several relays converge to the same stored state.
- Every upstream call is rate-limited and time-bounded. One request cannot use an upstream's whole budget.

## Limits

- One app process, one Postgres, one Redis, one consumer task.
- Content outside scope is only as complete as the upstream AppViews and Constellation make it.
- Kryphocron reads depend on relays crawling the community PDS. See [degraded-mode.md](degraded-mode.md).

## Other design docs

- [firehose-and-relays.md](firehose-and-relays.md): relays, the stream, gaps.
- [degraded-mode.md](degraded-mode.md): what happens when ingest falls behind.
- [indexing-and-scope.md](indexing-and-scope.md), [membership.md](membership.md).
- [kryphocron.md](kryphocron.md), [blocks-and-moderation.md](blocks-and-moderation.md).
- [thread-lens.md](thread-lens.md), [refract.md](refract.md), [pronouns-and-plural.md](pronouns-and-plural.md).

Operator guides: [running](../running.md), [configuration](../configuration.md), [operations](../operations.md), [admin](../admin.md), [API](../api.md).
