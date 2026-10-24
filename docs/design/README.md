# Design

Short design notes, one per area. Each says what Horizon-Prism does, what it guarantees, and its limits. Start with the overview.

| Document | Covers |
|---|---|
| [overview.md](overview.md) | Architecture: ingest, storage, the read path, upstream AppViews, community scope. |
| [firehose-and-relays.md](firehose-and-relays.md) | Relays, sidecars, the stream, cursors, gaps, dead letters. |
| [indexing-and-scope.md](indexing-and-scope.md) | Which accounts are indexed, public-indexing modes, what is stored, retention. |
| [membership.md](membership.md) | Members, enrolment, readiness, `getMembership`. |
| [private-posts.md](private-posts.md) | The private tier: audiences, the read rule, guarantees, notifications. |
| [blocks-and-moderation.md](blocks-and-moderation.md) | Blocks, list blocks, mutes, labels, the moderation gate, hidden replies. |
| [thread-lens.md](thread-lens.md) | The thread view that shows what the ordinary view leaves out. |
| [refract.md](refract.md) | Restored accounts. |
| [pronouns-and-plural.md](pronouns-and-plural.md) | Pronouns and plural-system records. |
| [degraded-mode.md](degraded-mode.md) | What happens when a relay or Farsight falls behind. |

Guides for operators are one level up: [running](../running.md), [configuration](../configuration.md), [admin](../admin.md), [API](../api.md), [operations](../operations.md).
