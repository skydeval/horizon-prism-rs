# Thread Lens

## Purpose

An ordinary thread view loses parts of a conversation. A block between two people can cut a thread for everyone who reads it, and a block of your own hides not only that author's posts but the replies above and below them. Thread Lens (`app.nearhorizon.threadLens.*`) is a thread view that keeps the conversation: **a block hides one author's posts, never the thread around them.**

## Model

### Methods

| Method | Does |
|---|---|
| `threadLens.getThread` | the thread around an anchor post: parents, the anchor, replies level by level |
| `threadLens.resolve` | up to 25 post URIs, one item each, in order |

Both are POST and need a signed-in caller. `getMembership.features` lists `threadLens`; `threadLens.signedIn` while every signed-in account may use it; `threadLens.hydrated` (post views are hydrated: `servedAs`, the viewer state it names, counts, and `viewer.replyDisabled` where the root's threadgate decides it); and `threadLens.labelers` (the reader's labelers are honoured, below).

### What it shows beyond the ordinary view

Every response starts from what `getPostThread` returns for the caller. When it can check the caller's blocks (**bridging**), it adds:
- **posts past a block between other people.** A reply the ordinary view stubs because two other accounts block each other is shown;
- **posts past the caller's own block.** The blocked author's posts stay gaps (the client shows only a placeholder), but the replies under them and the parents above them are shown. The parent walk goes on up past a blocked post, and past any further ones, to the start of the thread;
- **replies Horizon-Prism does not store**, found through Constellation and fetched from the upstream AppView or Slingshot.

It also keeps **replies the root author hid** (threadgate `hiddenReplies`) in the response, marked `hiddenByThreadgate`, so the client can collapse them as stock does. A reply is never marked for its own author. This marking happens with bridging on or off.

### Posts and gaps

Each node is a `postNode` (a `postView`) or a `gapNode`. A gap carries only the URI, a reason, and paging links; never a handle, name, text, count or time.

| Gap | Meaning |
|---|---|
| `blocked-by-you` | the caller blocks the author (directly or through a list; `blockingByListUri` names one such list of the caller's) |
| `blocking-you` | the author blocks the caller |
| `block-state-unknown` | the caller's blocks with this author could not be checked, so it was not fetched |
| `blocked` | a block the ordinary view reported that was not looked past |
| `deleted`, `not-found`, `upstream-unavailable`, `content-stripped` | the post is gone, hidden by moderation, or a source failed |
| `tier-hidden` | any private-tier URI, whether or not it exists |

The response's `bridging` is `off` (the ordinary view only), `limited` (some posts held back for want of a check) or `full`.

### Who may use it

`[threadlens] access`, live from the admin UI:

| Value | Served |
|---|---|
| `signed_in` (default) | every signed-in account. Ready members as **member**, everyone else as **visitor** |
| `members` | ready members only; others are refused `NotEnrolled` or `NotReady` |

Nobody may use it signed out. `servedAs` on the response says which.

### What it applies

- **The caller's blocks, in both directions.** Its stored direct blocks, its own list blocks, and what Farsight reports. Nothing by an author with a known block is returned.
- **A member** gets its own viewer state: likes, reposts, thread mutes, follows, mutes and block fields.
- **A visitor** gets `viewer.blockedBy` and `viewer.muted: false` only. Its own blocks still hide authors' posts; they are not named in views. Nothing is remembered for it, and its Farsight checks use the non-member share of the rate.
- **The moderation gate**, as on every view (see [blocks and moderation](blocks-and-moderation.md)).
- **Labels.** The operator's own. When the request carries `atproto-accept-labelers` (feature `threadLens.labelers`), every labeler it names is honoured, moderation labeler or not: the labels those labelers applied are kept beside the operator's, and `atproto-content-labelers` names them. Labels only add to views.

### Farsight

Only [Farsight](blocks-and-moderation.md#farsight) can say that an author has **no** block with the caller, including through a list. Horizon-Prism's own rows are never used to conclude that.

- Bridging is on for a request only when Farsight answers its first round of checks completely. A post is added only for an author Farsight clears.
- **Without Farsight, or while it is down**, every response has `bridging: "off"`: the ordinary thread view with the blocks Horizon-Prism already knows enforced (stored blocks, cached answers, and the member memory if it is on). No post is revealed past a block. Thread Lens never asks the Farsight fallback AppView.
- Thread Lens itself is always served: Farsight changes what it adds, not whether it answers.

### Paging

A thread is not limited; a single response is. One response adds at most `page_nodes` posts beyond the ordinary view (which is never cut). A node with more work carries:
- `moreParents`: call again with that node as anchor, `depth` 0;
- `moreReplies` (with a cursor when there is one): call again with that node as anchor, `parentHeight` 0, and `repliesCursor`.

A marker means work was not done, never that there is a block. The client merges responses by URI. `listing_calls`, `verdict_round_timeout_ms`, `parent_rounds`, `verdict_max_dids` and `listing_timeout_ms` bound one response's reads and checks; see [configuration](../configuration.md).

## Guarantees

- No content of an author with a known block with the caller (either direction) is sent: no post, embed or profile. Embeds of such posts are blocked-record views.
- A post is added beyond the ordinary view only when Farsight has cleared its author for this caller.
- A response always holds at least what `getPostThread` would return, with the caller's known blocks enforced.
- Private-tier records are never read: every private URI is a `tier-hidden` gap, looked up nowhere.
- Upstream sources are asked without the caller's identity.

## Limits

- **Farsight is needed for anything past a block.** Without it, Thread Lens is the ordinary view in a different shape.
- **Visitors' direct blocks of non-members are not stored.** For a visitor they come from Farsight; where Farsight gives no answer, such an author can appear where the ordinary view shows it.
- **Blocks the caller lifted can linger** for a visitor whose unblock was lost in a firehose gap: it hides more than it should, never less.
- **`resolve` items** carry no paging links, and a gap item is never marked `hiddenByThreadgate`.
- **A private root's gate is not applied**; the private tier applies its own (see [private posts](private-posts.md)).
- **Whoever runs Farsight** sees which accounts are in the threads callers open.
