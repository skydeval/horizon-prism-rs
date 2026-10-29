# Blocks and moderation

## Purpose

Horizon-Prism hides content a member should not see: posts by accounts on either side of a block, accounts and posts the operator took down, replies a thread's author hid, and accounts or threads the member muted. This page says what each rule does, where its data comes from, and where it stops.

## Model

### Which blocks Horizon-Prism stores

A block counts only when it is a public `app.bsky.graph.block` record, or a member's own `app.bsky.graph.listblock` that applies (below). Horizon-Prism does not track any other kind of block.

**Member blocks only.** A block is stored only when a member is on one side of it:

| Block | Stored |
|---|---|
| A member blocks anyone | yes |
| Anyone blocks a member | yes |
| A member's own list block | yes |
| A non-member blocks a non-member | no, in every indexing mode |
| A non-member's list block | no (see inbound list blocks) |

Being in scope through follows or interactions never brings in an account's other blocks. Nightly retention deletes any stored block that no longer has a member side, and removing a member deletes its blocks of non-members, others' blocks of it, and its list blocks. A block record edited so that it no longer names a member removes the stored row.

### How a block hides content

Blocks apply for a signed-in, ready member. A direct block applies in both directions.

| Surface | Effect |
|---|---|
| Timeline, list feeds, search, suggestions | the other account's items are left out |
| Notifications and unread count | everything from that account is left out |
| Threads | the post becomes a blocked-post stub; its replies are left out |
| Quotes, embeds, bookmarks | blocked-record views |
| Profile viewer state | `blocking`, `blockedBy`, `blockingByList` |

`getBlocks` lists the member's own direct blocks. `getRelationships` answers a ready member from its stored blocks, and any other account from an upstream AppView first.

### List blocks

**A member's own list blocks** (outbound). A member's `listblock` hides every account on that list from the member. Horizon-Prism keeps a private copy of each list a member blocks or mutes through and applies it once the copy is synced. It applies only when:
- the list is a moderation list (`app.bsky.graph.defs#modlist`). A block of a curation or reference list has no effect;
- the member is within its budget: `subscribed_max_per_member` lists, `max_owners_per_member` owners, and `subscribed_max_items` entries per list. A list block past the budget stays in the member's repo but has no effect.

**Inbound list blocks** (someone blocks a member through a list). Horizon-Prism does not store these. It asks [Farsight](#farsight), the stack's block-list index, as a member loads a page, which accounts on the page block the member through a list, and hides them as it hides a direct inbound block. Farsight's answer is taken as it is: Horizon-Prism does not check the purpose of the lists Farsight reports.

### Farsight

Farsight is **optional** (`[farsight] url`, off by default).

- **Without Farsight**, inbound list blocks are not applied. Direct blocks and members' own list blocks still are.
- **While Farsight does not answer**, pages load as usual without those blocks.
- **Fallback** (`[farsight.fallback]`, off by default): while Farsight is down, ask one other AppView which accounts on the page block the member through a list. That AppView then sees the member's DID beside the page's accounts, which is why it is off. Its answers are never treated as complete and never override Farsight's.
- **Memory** (`[farsight.memory]`, off by default): remember, per member, the list blocks Farsight reported, so they keep applying while Farsight gives no answer and across a restart. Kept for members only and for `retention_hours`; a block Farsight later says is gone is forgotten at once. `admin farsight forget <did>` clears one member.

Whoever runs the Farsight instance sees which accounts each member's pages contain. Use one you run or trust. See [configuration](../configuration.md) and [operations](../operations.md).

### Mutes

| Mute | Effect |
|---|---|
| `muteActor` | hides the account's items from timeline and notifications; marks it muted in threads |
| `muteActor` with `onlyReposts` / `onlyQuoteposts` | hides only that account's reposts or quote posts |
| `muteActorList` | as a full mute for every account on the list; moderation lists only, within the same budget as list blocks |
| `muteThread` | no notifications from the thread; posts in it carry `viewer.threadMuted` |

A list mute takes effect once Horizon-Prism has synced its copy of the list. `getMutes` lists full mutes only. Thread mutes are capped per member (`[thread_mutes] max_per_actor`).

### Labels and the moderation gate

**Operator labels.** The operator adds and removes labels with `admin label add` and `admin label remove`, or the admin UI. Allowed values: `!takedown`, `!hide`, `porn`, `sexual`, `nudity`, `graphic-media`. They are emitted with `src` set to the server's own DID (`did:web:prism.example.org`). `com.atproto.label.queryLabels` reads them; it never returns labels on kryphocron records and cannot list the whole table.

**The moderation gate** runs on every read path before any source is asked: local tables, upstream AppViews, Slingshot, Constellation, PDSes, Thread Lens and kryphocron reads.

| Input | Result |
|---|---|
| `!takedown` on the record or its author | treated as not existing; profiles answer `AccountTakedown`; no upstream fetch |
| author deactivated, taken down, suspended or deleted | treated as not existing |
| `!hide` on the record | left out of lists and threads; a direct `getPosts` still returns it with the label |
| otherwise | shown |

[Refract](refract.md) accounts add their own rules to the gate. On public paths, account status is read only for accounts in scope and members.

**Other labelers.** Labels that an upstream AppView attaches are removed from ordinary views; only the operator's labels are shown. [Thread Lens](thread-lens.md) honours `atproto-accept-labelers` and keeps the labels the named labelers applied. Labels from other labelers only add to a view: they never change what the gate hides. `getServices` describes any labeler. `createReport` answers 501 and points to the PDS and a labeler.

### Hidden replies

A thread root's `app.bsky.feed.threadgate` can list replies in `hiddenReplies`. Horizon-Prism leaves those replies, and what is beneath them, out of `getPostThread` for everyone but their own author. The V2 thread views move them aside with `hiddenByThreadgate`. `getPosts` still returns them. Horizon-Prism applies a gate it has stored, so this needs the root's gate to be indexed. For how a public root's gate hides kryphocron replies, see [kryphocron](kryphocron.md).

## Guarantees

- A ready member's block data is complete: its own blocks and every stored block of it, kept current from the firehose.
- A block between a member and anyone is enforced on every member surface, in both directions.
- A taken-down or hidden subject is not fetched from any source.
- Inbound list blocks never make a page fail: without an answer the page loads without them.
- Nothing is remembered for a non-member.

## Limits

- **Blocks between two non-members are not stored.** Horizon-Prism does not hide a reply because its author and the parent's author block each other (stock "third-party" blocks).
- **Inbound list blocks need Farsight.** They are best effort, apply only to ready members, and are not applied to kryphocron reads, anonymous callers or non-members.
- **Lists past the budget, unsynced lists and non-moderation lists** do nothing for list blocks and list mutes.
- **Ordinary views show only the operator's labels**, not those of the labelers a member subscribes to.
- **No moderation queue and no report intake.** The operator's labels and account status are the only moderation inputs.
