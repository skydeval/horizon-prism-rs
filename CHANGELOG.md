# Changelog

All notable changes to horizon-prism-rs are recorded here, in the style of
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/). Chainlink issues are
referenced inline as `#NN`. Every commit that lands functional work updates the
matching section under `[Unreleased]` in the same commit; issues are closed
with a changelog entry (no `--no-changelog`). Entries before item 5 were
backfilled per milestone (§15 implementation order of design-v2), not per
commit.

## [Unreleased]

### Added

- **Item 12a: one set of blocks for a whole request (#674).** A request
  can supply the blocks it knows beyond local rows
  (`hydration::with_known_blocks`), and every view built for it
  (profiles, posts, embeds, upstream views) then applies them, in the
  direction found. A block with no record here hides its author and
  carries no `viewer.blocking` URI. Thread Lens will supply its verdicts
  this way (design-v3 §5.15.12).
- **Item 12a: Thread Lens verdicts (#674).** `threadlens::verdict`
  decides, for the viewer and each author, one of "you block them",
  "they block you", "clear" and "cannot say", from the viewer's own
  blocks and list blocks, and from Farsight. An author is "clear" only
  when Farsight vouches for it and for the viewer's side of the request;
  a block always counts, vouched or not. Lookups run in rounds, each
  author sent at most once per request, within
  `threadlens.verdict_max_dids`. Thread Lens does not use it yet
  (design-v3 §5.15.4 – §5.15.6).
- **Item 12a: a second way to read Farsight (#674).** The `farsight`
  module can now return, for a viewer and a set of accounts, the whole
  answer per pair (a direct or list block in either direction, and
  whether Farsight vouches for it) and the coverage level of each call,
  under a call and time budget the caller brings. It shares the cache
  with ordinary views, so a page render and a Thread Lens request for
  the same pair cost one lookup. Nothing calls it yet (design-v3
  §5.14.9).
- **Item 12a: Thread Lens v2 settings (#674).** A `[threadlens]`
  section with `page_nodes` (200), `listing_calls` (60),
  `verdict_round_timeout_ms` (500), `parent_rounds` (8) and
  `verdict_max_dids` (600), checked at startup, and an optional
  `APP_THREADLENS_CURSOR_KEY_HEX` that `admin keygen` now prints a value
  for. Nothing reads them yet: Thread Lens still behaves as before
  (design-v3 §5.15, §13).
- **Item 12: purposes survive a failed item listing (#673).** When a
  list sync reads all of an owner's list records and then fails while
  reading the list items, what the list records said about each list's
  purpose is still written before the run is retried.
- **Item 12: a purge clears a member from the sync lanes' accounting
  (#673).** Removing a member or deleting its account now also drops
  what was held in memory about it for scheduling list syncs: what
  waited for it, and the lane time charged to it.
- **Item 12: lost deadlocks are retried in place (#673).** A list
  sync's batch or its final step that loses a deadlock or a
  serialization conflict, for instance to a firehose write by the same
  owner, is run again in place, up to five times, instead of failing the
  run. `muteActorList` and `unmuteActorList` do the same, so such a
  conflict never reaches the client
  (`list_subscription_tx_retries_total`).
- **Item 12: removed list settings say what replaced them (#673).** A
  config that still has `lists.subscribed_max_total` or
  `retention.subscribed_list_grace_days` is refused at startup with a
  message naming the per-member budget keys and the immediate release,
  instead of only "unknown field".
- **Item 12: the tombstone cap covers a sync's own tombstones (#673).**
  The tombstones a list sync writes for items moved out of a list now
  count against `lists.subscription_tombstones_max_per_owner`: after a
  run's writes the owner's oldest tombstones past the cap are removed,
  and the in-memory count is read again.
- **Item 12: `admin lists purge <did>` (#673).** Deletes an account's
  list mutes and the list records it authored (lists, list items, list
  blocks, opt-outs), and its references with them. A local cleanup, not
  a takedown: records still in the account's repository come back at
  its next re-sync (design-v3 §14.3).
- **Item 12: `muteActorList`, `unmuteActorList`, `getListMutes`,
  `getListBlocks` (#673).** A member can mute a moderation list, up to
  `lists.subscribed_max_per_member` lists from at most
  `lists.max_owners_per_member` owners; past that the call is
  `400 InvalidRequest` and nothing is written. A list that is not a
  moderation list, or a URI that is not a list, is `400 InvalidRequest`.
  The two listings show the lists the member mutes and blocks, newest
  first, whether or not they have an effect yet (design-v3 §5.3).
- **Item 12: a member's list mutes and list blocks take effect
  (#673).** Once a list's owner has been synced, the accounts on a
  moderation list the member mutes are treated as fully muted, and those
  on one it blocks as blocked by the member: in the timeline and list
  feeds, notifications and the unread count, threads, and everywhere
  else a direct mute or block already applies. Profile views carry
  `muted: true`, and `mutedByList` / `blockingByList` with the list's
  view: from its row for a ready member's list, from its record for any
  other (left out if the record cannot be read; the mute or block
  applies all the same). A reference past the member's budget, not yet
  synced, or to a list that is not a moderation list changes nothing.
  The members come from the private copy only, and nobody else's views
  are affected (design-v3 §4.3 "List purpose", §5.3).
- **Item 12: budget changes and tombstone clean-up (#673).** At startup
  every member's references are checked again against the configured
  budget, so raising `lists.subscribed_max_per_member` or
  `lists.max_owners_per_member` admits references that waited and
  lowering them stops the newest. Rule 4's tombstones now expire with
  `retention.tombstone_days`, and the recovery sweep deletes those older
  than an hour whose owner no longer has a subscribed list (design-v3
  §4.3 "Tombstones").
- **Item 12: references follow membership (#673).** Adding a member
  (by `admin member add` or auto-enrolment) gives it references for the
  list mutes and list blocks it already has stored, in the order they
  were made and under its budget. Removing a member, or deleting its
  account, deletes all its references in the purge's first step, and
  the lists nobody else references are released (design-v3 §4.3
  "Callers of the member phase").
- **Item 12: a member's list block becomes a reference (#673).** When a
  member's own `app.bsky.graph.listblock` record is written, changed to
  name another list, or deleted, the writer derives that member's
  reference to the list again and tells the list phase, on every write
  path, `requestRecord` included. A list block past the member's budget
  is stored and has no effect (design-v3 §4.3 "Callers of the member
  phase").
- **Item 12: the copy rule and the purpose rule in the writer (#673).**
  When the owner of a list that a member mutes or blocks changes the
  list, the writer now keeps the private copy current: a list item is
  copied, a delete or a move out of the list removes the copy and leaves
  a tombstone so a late, older create is not copied again, and the
  list's purpose is stored on its row. This happens whether or not the
  owner's records are admitted to the public tables, and never changes
  what the public tables or an op's outcome show. Caps:
  `lists.subscribed_max_items` per list (`list_items_over_cap_total`)
  and `lists.subscription_tombstones_max_per_owner`
  (`subscription_tombstones_evicted_total`). The firehose, backfill and
  dead-letter replay run the rules; `requestRecord` does not
  (`ApplyOptions.subscription_rules`; design-v3 §4.3 "The copy rule",
  "The purpose rule").
- **Item 12: the other reasons a list sync starts (#673).**
  `subscriptions::triggers` adds the recovery sweep (at startup and with
  every retry-scheduler run: it finds again every reference still
  waiting on a sync, and every list never synced), the weekly run (a
  low-priority sync for each owner with no complete listing in 7 days,
  one owner a second), a sync on an account or identity event for an
  owner that looks gone (never for an ordinary owner), and requested
  syncs recorded on the owner's row. A sync row left `running` at
  startup, or found stuck, now ends as a run does: the owner's pacing
  moves and the next start is paced. All are on in `serve`: the firehose
  consumer raises the event sync, and the end of a purge of a list
  owner requests one (design-v3 §4.3 "When a sync is enqueued").
- **Item 12: `admin lists sync <list-uri>` (#673).** Asks for a
  high-priority sync of the owner of a list some member mutes or blocks.
  It is still paced like any other sync of that owner, waits while the
  owner is being purged, and fails for a list no member references
  (design-v3 §14.3).
- **Item 12: list syncs on the backfill workers (#673).**
  `subscriptions::runner` lets a backfill worker run a list sync before
  its ordinary work: the fast lane first, then the slow lane, each with
  its own fixed number of permits, tried and never waited for. Lane time
  now counts a run still in progress. A row left `running` with no run
  behind it is queued again at a new generation by a check every quarter
  of `lists.sync_stuck_secs` (`list_sync_stuck_requeued_total`,
  `list_sync_running_age_seconds`), and at startup. `serve` starts the
  applier and gives the lanes to its backfill workers; nothing creates a
  reference yet, so they stay idle (design-v3 §4.3 "Lanes").
- **Item 12: choosing the next list sync (#673).**
  `subscriptions::claim` picks one due sync per lane (fast, and slow for
  owners whose servers answer slowly). A queued sync whose reason has
  gone, such as a mute withdrawn before the run began, ends without a
  run and without moving the owner's pacing
  (`list_sync_causeless_total`). Among the rest the pick goes to whoever
  has held the lane least lately: each member's mutes and each member's
  blocks count separately, beside two shared turns for repairs nobody
  waits on and for owners that were cut. The general backfill worker no
  longer claims or re-queues `list_sync` rows. A run also stops when its
  job row is no longer `running`. Nothing starts a claim yet (design-v3
  §4.3 "A separate claim", "Fair claims").
- **Item 12: list sync against real servers (#673).**
  `subscriptions::pds` checks a list owner's account (a fresh identity
  document that is neither stored nor cached, then the repository's
  status and latest revision at its PDS) and reads its records from that
  same PDS under the host's backfill allowance. Time waiting for that
  allowance is not counted as the owner's server being slow. A timeout
  or a server error never starts a suspicion that the owner is gone
  (design-v3 §4.3 "The run", step 2).
- **Item 12: a whole list sync run, and owners that are gone (#673).**
  `subscriptions::run` takes one run from start to end. An owner being
  purged is not listed. An owner whose account does not answer is first
  only suspected: nothing changes for members, and the run comes back a
  day later; the same answer a day or more after the suspicion began, or
  a tombstone in the PLC directory, confirms it, and the owner's lists
  are then emptied. A move to another PDS starts the suspicion over. A
  listing cut short keeps what was read and retries a day later. Runs
  are counted in `list_sync_runs_total{outcome, lane}`. Nothing starts a
  run yet (design-v3 §4.3 "The run").
- **Item 12: reading a list owner's records within bounds (#673).**
  `subscriptions::fetch` reads the owner's list records, then its list
  items, page by page, and stops at the first limit reached: records,
  pages, slow answers, time spent in calls, or total time. A listing
  stopped early is marked incomplete with the reason; a slow answerer or
  a server of tiny pages is told apart from an owner that is simply
  large. No record is fetched by name (design-v3 §4.3 "Per-owner
  bounds").
- **Item 12: how a list sync run ends (#673).** Every end of a run
  records its time on the list owner's row first, then settles the job:
  a completion is done (or queued again if a sync was asked for while it
  ran); a run cut short is queued a day later and the owner backs off
  for a week; a failure is retried with the usual backoff. No retry is
  ever scheduled earlier than the owner's earliest start, and a run that
  was superseded settles nothing (design-v3 §4.3 "Pacing and claiming").
- **Item 12: what a list sync writes (#673).** `subscriptions::sync`
  takes an owner's listings and brings the private copy in line: it
  keeps each list's oldest entries up to the cap, removes an entry that
  is gone only when two complete listings in a row lack it (one on a
  list's first sync), removes and remembers an entry moved to a list
  nobody subscribes to, and records each list's purpose. A waiting list
  mute or block takes effect only when the run served its list and
  listed the list's own record; otherwise it is marked once and waits
  for the owner's next run. A listing cut short only adds. Fetching the
  listings and scheduling the job are not built yet (design-v3 §4.3
  "The run").
- **Item 12: the reference applier and the in-memory subscribed sets
  (#673).** `subscriptions::applier` is the background task that runs the
  list phase (on a signal, at startup with a full reconcile, and every
  60 s), retries a busy list after a second, gives one that stays busy a
  task that waits for it, and releases unreferenced lists in tasks of
  their own. `ScopeState` now holds which lists are subscribed and whose
  changes are tracked, set from the rows after each commit. The pairs of
  members waiting on an owner's sync are kept in memory only. Not started
  by `serve` yet (design-v3 §4.3).
- **Item 12: `[lists]` settings for members' list mutes and blocks
  (#673).** The per-member budget (`subscribed_max_per_member` 25,
  `max_owners_per_member` 10, `subscribed_max_items` 50 000), the
  tombstone cap, and the list-sync pacing, lanes and per-run bounds, all
  at the design's defaults. Startup refuses lanes that would leave fewer
  than two backfill workers for readiness jobs, and a wall-time bound
  shorter than the other bounds allow. Nothing reads them yet
  (design-v3 §4.3, §13).
- **Item 12: the list phase, release, and sync enqueue rules (#673).**
  `subscriptions::list_phase` turns a change marker into the list's
  `subscribed_lists` row (active while an admitted reference exists,
  unreferenced otherwise), takes the owner's row, and queues the owner's
  `list_sync` when a reference waits on one; it never waits on a list
  row. An unreferenced list is released at once: items in batches, then
  the row, then the owner's queued job if it was the owner's last list.
  `subscriptions::jobs` holds the earliest start an owner's next sync
  may have and the rule that an enqueue never supersedes a running job.
  Nothing runs these yet (design-v3 §4.3).
- **Item 12: list references and the member phase (#673).** New module
  `subscriptions::references`: a member's list mute or list block is a
  reference to a list, admitted while it is inside the member's budget
  (its first references of that kind, naming a bounded number of list
  owners, counted in arrival order from the member's own rows only). The
  member phase writes the member's rows and a change marker in one
  transaction that takes no lock on shared state. Nothing calls it yet
  and no reference has any effect on a view yet (design-v3 §4.3).
- **Item 12: migration `0018_subscribed_list_purpose` (#673).** Four
  optional columns for the rule that a member's own list block or list
  mute has an effect only on a moderation list: a subscribed list's
  purpose, the revision it was read at, since when its record has been
  missing, and a mark on a reference whose list record was not found.
  Nothing reads or writes them yet; the migration ships before the code
  that fills those tables, so it has no data to convert
  (design-v3 §4.3 "List purpose", §12.1).
- **Item 15: `com.atproto.identity.resolveHandle` (#675).** The account a
  handle belongs to. Bad syntax is refused before anything is looked up.
  An indexed account's stored handle is answered at once. Otherwise the
  handle's DNS record and its well-known file are both asked, and the
  account found must name the handle back. "No such handle" is
  `HandleNotFound`; a lookup that could not be completed is
  `502 UpstreamFailure`, never mistaken for "not found". Answers are
  remembered (`[identity] resolve_handle_cache_max`,
  `resolve_handle_ttl_secs`, `resolve_handle_negative_ttl_secs`); a failed
  lookup never is. Counted in `handle_resolutions_total{result}`
  (design-v3 §5.11).
- **Item 15: `app.bsky.labeler.getServices` (#675).** The labeling
  services an app asks about: stored ones from local rows, any other
  from its service record through Slingshot. Each view carries its
  creator's profile, like count, the viewer's like, local labels and
  `indexedAt`; `detailed=true` adds the policies and subject fields. A
  labeler taken down here is left out. At most
  `[labelers] get_services_max_dids` per request (default 50)
  (design-v3 §5.7).
- **Item 15: `bookmarkCount` (#675).** Post views can carry how many
  members bookmarked the post, by `[bookmarks] emit_count`: `never` (the
  default, safe for a small community where a count of 1 points at one
  person), `above_floor` (only from `count_floor`, default 10, never
  shown as a lower number) or `always` (design-v3 §8.2).
- **Item 15: `app.nearhorizon.feed.getAuthorPolls` (#675).** An
  account's public polls, newest first, with a basic author view.
  Non-public polls are never returned. An indexed author's polls come
  from stored rows; anyone else's from their own server, a page at a
  time. `limit` is 1 to 100 (default 25); more is a 400. Lexicon files
  for the endpoint and for `app.nearhorizon.poll.post` are under
  `lexicons/` (design-v3 §5.13).
- **Item 15: `app.bsky.feed.getActorLikes` (#675).** A ready member's own
  likes, newest first; `actor` must be the caller. Liked posts not stored
  here are fetched from Slingshot, at most a page's worth. A liked post
  that is gone or hidden, or whose author has a block with the member,
  is left out (design-v3 §5.4).
- **Item 15: `com.atproto.repo.getRecord` (#675).** Serves public
  records this AppView stores with their JSON (posts, profiles, lists of
  ready members, starter packs, labeler services, gates, public polls,
  and the three self-keyed declaration records), from local tables only.
  A private-tier collection is `RecordNotFound` before any lookup;
  collections kept without their JSON (likes, reposts, follows, blocks,
  list items) are refused by name; a handle is resolved only for an
  indexed account, with no outbound lookup (design-v3 §5.10).
- **Item 15: `com.atproto.label.queryLabels` (#675).** Anyone can read
  the labels this AppView's operator applied, by exact account or record,
  or by a prefix that names one whole account (`at://<did>/…*`), so no
  request can list every label. Up to `[labels] query_max_patterns`
  patterns (default 25), 250 labels a page. Labels on private-tier
  records are never returned. Labels are unsigned (design-v3 §5.12).
- **Item 15: drafts (#675).** `app.bsky.draft.createDraft`, `getDrafts`,
  `updateDraft` and `deleteDraft`. A draft is checked against the
  lexicon (posts, text length, the embed lists) and against
  `[drafts] max_bytes` (default 1 MiB) before it is stored; a member
  keeps at most `[drafts] max_per_actor` (default 1000, then
  `DraftLimitReached`). An update of an id the member does not own
  changes nothing and succeeds. Nothing inside a draft is fetched or
  logged (design-v3 §5.9).
- **Item 15: bookmarks (#675).** `app.bsky.bookmark.createBookmark`,
  `deleteBookmark` and `getBookmarks`. Only posts can be bookmarked;
  any other collection, private-tier URIs included, is
  `UnsupportedCollection`, decided from the URI alone. A member holds at
  most `[bookmarks] max_per_actor` (default 50 000). The listing reads
  stored posts only and makes no fetch and no count refresh; a post not
  stored here, gone or hidden shows as `notFoundPost`, and one with a
  block as `blockedPost`. Post views carry `viewer.bookmarked`
  (design-v3 §5.8, §8.2).
- **Item 14: subscription state on profiles (#675).** Profile views
  carry `viewer.activitySubscription` for a ready member subscribed to
  the account, and detailed profile views carry
  `associated.activitySubscription.allowSubscriptions` from the account's
  notification declaration (`followers` when it has none)
  (design-v3 §8.1).
- **Item 14: `starterpack-joined` notifications (#675).** When an
  account's profile is created, as seen live on the firehose, and names a
  member's starter pack as the one it joined through, that member is
  notified. A profile edit, a backfill or a replay announces nothing. The
  notification shows the joiner's current profile record and its CID
  (design-v3 §8.6).
- **Item 14: `like-via-repost`, `repost-via-repost` and `starterPack`
  (#675).** A like or repost made from a member's repost notifies that
  member, when the repost is stored here and has the same subject, and
  unless they were already notified of it as the post's author. A
  `follow` notification whose follow was made through a starter pack
  carries the pack as `starterPack`. Rebuilt like, repost and follow
  records include their `via` (design-v3 §8.6).
- **Item 14: sweep of subscriptions that can no longer notify (#675).**
  The daily retention run deletes activity subscriptions whose subject is
  no longer indexed here, so they are not shown as active
  (`activity_subscriptions_out_of_scope_total{action="swept"}`;
  design-v3 §5.6).
- **Item 14: `subscribed-post` notifications (#675).** When a post by an
  in-scope, once-backfilled author arrives from the firehose, each member
  subscribed to that author for that kind of post gets a `subscribed-post`
  notification, unless another reason already notified them of it. Posts
  older than `[notifications] subscribed_post_max_age_secs` (default one
  day) or older than the subscription, and posts written by backfill or
  `requestRecord`, announce nothing. The author's declaration is checked
  again each time (design-v3 §5.6).
- **Item 14: activity subscriptions (#675).**
  `app.bsky.notification.putActivitySubscription` and
  `listActivitySubscriptions`. Turning a subscription off always works.
  Turning one on needs a ready member, a subject this AppView indexes,
  room under `[activity_subscriptions] max_per_actor` (default 1000), and
  the subject's permission (its notification declaration; no record means
  followers only). No notification is generated from them yet
  (design-v3 §5.6).
- **Item 14: notification preferences (#675).**
  `app.bsky.notification.getPreferences` and `putPreferencesV2` store a
  member's preferences (13 keys; `chat` is always its default), and they
  are applied: a reason with `list: false` leaves `listNotifications` and
  `getUnreadCount`, and `include: "follows"` keeps a reason only from
  accounts the member follows. `push` has no effect. The legacy
  `putPreferences {priority}` is accepted and stores nothing
  (design-v3 §5.6).
- **Item 13: `viewer.replyDisabled` on `getPosts` (#675).** Stored posts
  returned by `getPosts` carry `viewer.replyDisabled` for a member, from
  the threadgate of each post's thread root, by the same rules as on an
  opened thread (design-v3 §8.2, §8.3).
- **Item 13: `!no-unauthenticated` in the newer thread view (#675).** A
  signed-out viewer of `getPostThreadV2` is not shown posts by an account
  whose profile carries the `!no-unauthenticated` label: the opened post
  and the posts above it become `threadItemNoUnauthenticated` items, and
  replies are left out. Only profiles stored here are read for the label
  (design-v3 §5.1).
- **Item 13: `followingRule` checks Constellation (#675).** For a thread
  whose author lets only accounts they follow reply, a follow that is not
  stored here is looked for among the viewer's followers on Constellation
  (at most `[known_followers] profile_scan_pages` pages, default 2,
  charged to the caller). `viewer.replyDisabled` is then `false` or
  `true`; when the scan cannot finish it is left out, as before
  (design-v3 §8.3).
- **Item 13: thread position on feed items (#675).** A post in a feed
  that is part of its author's own thread (the root, then the author's
  earliest reply to each post in turn) carries `opThreadPostIndex` and
  `opThreadPostCount`. Worked out from stored posts only, for in-scope
  authors, up to `[op_thread] max_walk` posts (default 50; a longer
  thread carries no position; 0 turns it off) (design-v3 §8.2).
- **Item 13: `thread_v2.max_parents` (#675).** How many parents
  `getPostThreadV2` returns above the opened post is now a setting
  (default 80, at most 1000) instead of a fixed number (design-v3 §5.1).
- **Item 13: `threadgate` on post views (#675).** A post with a valid
  threadgate of its own carries it as `threadgate` wherever the post view
  appears, with the lists its rules name (design-v3 §8.2).
- **Item 13: detached quotes (#675).** When the author of a quoted post
  has detached a quote (the post's valid postgate lists the quoting post in
  `detachedEmbeddingUris`), the quote's embedded record renders as
  `app.bsky.embed.record#viewDetached` (design-v3 §8.3).
- **Item 13: `viewer.knownLikers` on the thread anchor (#675).** For a
  ready member: the accounts it follows that liked the post, with a count
  and up to 5 profiles (design-v3 §8.2). Stored likes are joined with a
  scan of the post's likers on Constellation, at most
  `known_likers.scan_max_pages` pages (default 5) of 100, each charged to
  the caller's Constellation share. Accounts with a block either way are
  left out, and the field is left out when nobody is left.
- **Item 13: `viewer.replyDisabled` on the thread anchor (#675).** For a
  ready member, from the root post's threadgate (design-v3 §8.3). The
  root's author may always reply. `allow = []` disables replies. Otherwise
  a rule that lets the viewer in (mentioned in the root, follows the
  root's author, is followed by the root's author, is on a list this
  AppView serves from its own rows) enables them. When no rule lets the
  viewer in and every rule could be checked in full, replies are disabled;
  when one could not (a list held elsewhere, a follow not stored here), the
  field is left out, since the answer is not known. With no gate or no
  `allow`, the field is left out.
- **Item 13: `viewer.embeddingDisabled` (#675).** A ready member sees
  `embeddingDisabled: true` on a post whose author turned quoting off (a
  valid postgate with a `disableRule`, design-v3 §8.3). The author's own
  view does not carry it.
- **Item 13: threadgates in thread views (#675).** `getPostThread` and
  `getPostThreadV2` carry the root post's `threadgate` view, with the lists
  its rules name as `listViewBasic` (design-v3 §5.1, §8.3). A gate counts
  only when its record key is its post's and its author is the post's
  author. A reply the gate hides is left out of both thread views; when it
  is a direct reply of the anchor, `getPostThreadOtherV2` returns it with
  `hiddenByThreadgate`. A hidden reply is not hidden from its own author.
  Replies that break the gate's `allow` rules are not removed.
- **Item 13: `getPostThreadV2` and `getPostThreadOtherV2` (#675).** The
  flat thread view (design-v3 §5.1), built from the same tree
  `getPostThread` assembles, so the endpoints agree on what a thread holds.
  Parents are at negative depths, the anchor at 0, replies below. Siblings
  come in this order: the root's author, the viewer, accounts the viewer
  follows, everyone else; each group by `sort` (`oldest`, `newest`, or
  `top` by like count). Every direct reply of the anchor is returned, and
  at most `branchingFactor` replies of each deeper post, to `below` levels;
  `moreReplies` counts what was known and not returned. A direct reply by
  an account the viewer fully mutes moves to `getPostThreadOtherV2` with
  `mutedByViewer`; a muted reply deeper down is left out of both. The
  contiguous thread by the root's author carries `opThread`,
  `opThreadPostIndex` and `opThreadPostCount`. A missing, private-tier or
  hidden anchor is one `threadItemNotFound` item, and a blocked anchor one
  `threadItemBlocked`. Not built yet: the threadgate view and replies
  hidden by it, and the `!no-unauthenticated` items.
- **Item 12: `admin farsight status` and `admin farsight prime` (#673).**
  `status` prints Farsight's own report in brief, with whether this
  AppView's read breaker is open and how many answers it has cached.
  `prime <did> [--force]` asks Farsight to read one account's repository
  again (design-v3 §5.14.11, §14.3). Both run in `serve` through the
  command queue and fail with a reason when `farsight.url` is not set.
  Operator notes are in `docker/README.md`, "Farsight (block lists)".
- **Item 12: Farsight is primed when a member is enrolled (#673).** With
  `farsight.prime_on_enroll` (default true), each `member` backfill job
  asks Farsight to read the member's own repository, so the member's blocks
  and list blocks are current there at once (design-v3 §5.14.11). The call
  is best effort and never affects the job or readiness. It has a breaker
  of its own, apart from the one for reads.
  `farsight_prime_failed_total{reason}`.
- **Item 12: inbound list blocks in notifications (#673).** An account that
  blocks the viewer through a list sends it no notifications: it is left
  out of `listNotifications`, and `getUnreadCount` subtracts its unread rows
  (design-v3 §5.14.7). The count asks Farsight about the unread rows'
  authors, newest first, up to what one request may ask; authors past that
  are not checked. A member that stops being ready loses its cached
  Farsight answers.
- **Item 12: `getRelationships.blockedByList` (#673).** For a ready-member
  `actor`, each other account that blocks it through a list gets
  `blockedByList`, the list's URI, from one Farsight lookup on the caller's
  share (design-v3 §5.14.8). Answers are cached in shared entries for
  `farsight.cache_ttl_secs`. This public endpoint never reads or writes a
  member's own cached answers, so it cannot show that a member recently
  viewed an account. With no answer from Farsight the field is left out,
  the response still goes out, and `relationships_partial_total` counts it.
- **Item 12: inbound list blocks from Farsight (#673).** With
  `farsight.url` set, a ready member's public requests ask Farsight which
  accounts on the page block the member through a list, and such an account
  is treated as one that blocks the member directly: `viewer.blockedBy` is
  true, and every surface that already hides a direct inbound block hides
  this one (design-v3 §5.14.5). Accounts with a direct block are not asked
  about, and neither is the member itself.
  - Answers are kept per viewer for `farsight.cache_ttl_secs`, and a
    request never asks twice about one account. An answer Farsight does not
    vouch for never removes a cached block; a vouched clean answer does.
  - A request asks about at most `farsight.max_calls_per_request` chunks of
    100 accounts and waits at most `farsight.timeout_ms` in all. Calls draw
    on a bucket of their own, with member and non-member pools and a
    per-requester share. Five failed calls open a breaker, after which no
    call is made for 30 s; an anonymous caller's failures do not count
    toward it. An answer over the size cap is asked again once, as two
    halves.
  - When Farsight gives no answer, for any reason, the page is rendered
    with the blocks this AppView holds itself. Nothing here slows a request
    beyond the allowance or turns it into an error.
  - Private-tier reads never ask Farsight. `getMutes` reads the member's
    cached answers only, so Farsight never sees a member beside its mutes.
  - `farsight_calls_total{outcome}`, `farsight_coverage_total{level}`,
    `farsight_partial_total{reason}`, `farsight_auth_failed_total`.
  - Not built yet: the post-scan filters that drop a blocker's items from
    the timeline, list feeds and notifications before views are built, the
    unread-count correction, `getRelationships.blockedByList`, clearing a
    removed member's cached answers, the prime on enrollment, and the
    `admin farsight` commands.
- **Item 12: the Farsight client (#673).** `[farsight]` config and the
  `FARSIGHT_TOKEN` environment variable (design-v3 §5.14.4). With
  `farsight.url` unset, the default, nothing changes. Startup refuses a
  `url` that is not `https`, or one set without `FARSIGHT_TOKEN`. The
  `farsight` module holds the parts no request uses yet: the DIDs Farsight
  accepts and their canonical form, the split of a lookup into calls of at
  most 100 accounts and 6 KB, the `checkBlocks` call with its failures
  classified, the reading of an answer into one pair answer per account,
  and the rule for writing a new answer over a cached one. No request calls
  Farsight yet.
- **Item 12: `getListFeed` (#673).** The posts and reposts of a list's
  members, newest first (design-v3 §5.5). Local rows answer only when the
  list is served locally and every member is in scope, so that the feed is
  complete. Otherwise an upstream AppView is asked. When none answers, a
  locally served list is answered from its in-scope members, with a cursor
  of its own kind, and `list_feed_partial_total` counts it. The viewer's
  mutes and blocks drop items after hydration. An unknown list is
  `UnknownList`.
- **Item 12: `getKnownFollowers` (#673).** For a ready member: the accounts
  it follows that follow the named account (design-v3 §5.3). Stored follows
  are joined with a scan of the account's followers on Constellation, at
  most `known_followers.scan_max_pages` pages (default 20) of 100, each page
  charged to the caller's Constellation share. A scan that stops early
  gives a shorter list and counts in
  `known_followers_scan_truncated_total`. Accounts with a block in either
  direction are left out. The result is paged from a snapshot with an offset
  cursor. With Constellation unavailable, stored follows alone answer.
- **Item 12: `getRelationships` (#673).** For a ready member, `following`,
  `followedBy`, `blocking` and `blockedBy` come from local rows, which are
  complete for a member (design-v3 §5.3). `blockingByList` is the member's
  `listblock` record for a moderation list that names the other account,
  and only for a list whose owner is a ready member; it is read from public
  rows and never from the subscription tables. Any other actor is asked of
  an upstream AppView; when none answers, local rows answer what they can
  and `relationships_partial_total` counts it. A stored block is read only
  when its author is in scope or its subject is a member. At most 30
  `others`; one that does not resolve is `#notFoundActor`. `blockedByList`
  is not set yet: it needs the block index.
- **Item 12: starter-pack joined counts (#673).** A job recounts, for every
  stored pack, how many accounts joined through it, every
  `starter_packs.counts_refresh_secs` (default 3600, at least 60). The
  all-time number comes from Constellation, or from local profile rows when
  Constellation does not answer. The week number is today's all-time value
  minus the value of 7 days ago when both came from the same source;
  otherwise it is the local count of joiners whose profile is at most 7 days
  old. A count is written only while its pack exists.
  `starter_pack_counts_refreshed_total{source}`.
- **Item 12: `getListsWithMembership` and `getStarterPacksWithMembership`
  (#673).** A ready member's own moderation and curate lists, or its own
  starter packs, each with the named actor's list item when the actor is on
  the list. Reference lists are never included, and asking for them is `400
  InvalidRequest`.
- **Item 12: starter-pack views (#673).** `getStarterPack`,
  `getStarterPacks` (at most 25 URIs, misses omitted) and
  `getActorStarterPacks` (design-v3 §5.5). A stored pack is served from its
  row, any other from Slingshot. An actor's packs are listed from local rows
  only for a ready member; otherwise from an upstream AppView, then the
  owner's PDS. The pack's list follows the same rule as `getList`: its item
  count and its sample of at most 12 members come from local rows only when
  the list's owner is a ready member, and from an upstream AppView
  otherwise. Joined counts are shown when the counts job has stored them;
  that job is not built yet. `feeds` is omitted.
- **Item 12: `getList` and `getLists` (#673).** A list is served from this
  AppView's own rows only when its owner is a ready member, whose lists and
  list items are held in full (design-v3 §5.5). Any other list is read as if
  nothing were stored: its record from Slingshot, its items from an upstream
  AppView, and, when none answers, from the owner's PDS (at most
  `lists.read_listing_pages` pages a request, default 5, with a `P:` cursor
  while the PDS has more). `listView` carries
  the creator, `listItemCount` (distinct subjects, or the upstream's number),
  local labels, and the viewer's own `muted`, `blocked` and
  `referenceListOptOut`. Duplicate items of one subject show once. A
  reference list leaves out subjects who opted out, except for its owner,
  who sees them with `subjectOptedOut`. `getLists` takes `purposes` as short
  forms or full tokens. A URI that is not an `app.bsky.graph.list` misses
  with no fetch.
- **Item 12: thread mutes and `getBlocks` (#673).**
  `app.bsky.graph.muteThread` and `unmuteThread` store the caller's muted
  thread roots (design-v3 §5.3). A muted thread sends the muter no
  notifications: not for a reply in it, and not for a like, repost or quote
  of a post in it. Post views in it carry `viewer.threadMuted: true`,
  including upstream views of replies this AppView does not store. `root`
  must be the at-uri of an `app.bsky.feed.post`; it is not looked up, so the
  call makes no fetch. A member may mute `thread_mutes.max_per_actor`
  threads (default 10 000); past it the call is `400 InvalidRequest`, and a
  repeat of an existing mute still succeeds. `app.bsky.graph.getBlocks`
  lists a ready member's own blocks, newest first, with `viewer.blocking`.
- **Item 11a: deferred services can be forwarded (#672).**
  `[services.<name>] state = "proxy"` is now accepted for `contact`,
  `age_assurance` and operator-defined services (design-v3 §6.7); before,
  it stopped startup. Push still accepts only `disabled`, and `serve` is
  still refused. A request to a forwarded NSID goes to
  `<proxy_url>/xrpc/<nsid>` with its query and body unchanged, and the
  target's status and JSON body come back unchanged.
  - **Tokens.** A request with an `Authorization` header is verified here
    first, with `lxm` equal to the called method and an `aud` fragment that
    is absent or in the service's `aud_fragments` (default `bsky_appview`).
    The header is then forwarded unchanged. One that fails is answered `401
    InvalidToken` here and is never forwarded. A caller the backstop already
    verified is not verified twice, but still meets the service's fragment
    list.
  - **Headers.** Only `Authorization`, `Content-Type` and `Content-Encoding`
    (POST), `Accept-Language` and `atproto-accept-labelers` are sent, with
    this AppView's `User-Agent`. Cookies, `atproto-proxy`, `X-Forwarded-For`
    and every other header are dropped, so the target never sees the
    caller's address. Of the target's headers only `Content-Type`,
    `Content-Encoding`, `Content-Language` and `atproto-content-labelers`
    are relayed, and `X-Content-Type-Options: nosniff` is added.
  - **Limits.** `service_proxy_member` (5/s, burst 20) and
    `service_proxy_ip` (10/s, burst 20) have one bucket per service and per
    key, the key being the one the backstop selected. Each service has a
    request bucket (`rate_per_sec` 20, `burst` 40) with the member and
    non-member pools and per-requester shares of §II-8.3
    (`target_kind="service"`), and `max_in_flight` (64) calls in progress.
    Over a bucket: `429` with `ratelimit-reset`. Over `max_in_flight`: `503
    NotEnoughResources`, not queued. A POST body is read in full first,
    within `max_request_bytes` (`413 PayloadTooLarge`) and
    `body_read_timeout_ms` (`400 InvalidRequest`), so a slow sender holds
    nothing of the service's.
  - **Answers.** JSON within `max_response_bytes`, or an empty body, is
    relayed with the target's status. A redirect, another content type or a
    larger body is `502 UpstreamFailure`; no answer within `timeout_ms` is
    `504 UpstreamTimeout`. These messages never name the target.
  - **Transport.** Each service has its own client: no redirects, the
    system resolver, `https` to public addresses only.
    `allow_private_network = true` permits `http` and loopback, RFC 1918,
    CGNAT and unique-local addresses for that service's host. Over `http`
    every resolved address must be private. Link-local addresses stay
    blocked.
  - `app.bsky.contact.sendNotification` keeps its 501 in every state.
  - Metrics: `service_proxy_requests_total{service, nsid, outcome}`,
    `service_proxy_duration_seconds{service}`, `service_state{service,
    state}`. Operator notes are in `docker/README.md`, "Forwarding a service
    to another server".
- **Item 10: rate limits are config (#669).** Every limit is
  `[rate_limits.<class>]` with `per_sec` (a decimal) and `burst`, with the
  former constants as defaults (design-v3 §10). Startup refuses a `per_sec`
  that is not finite or is outside 0.000001 to 1 000 000, a `burst` below 1,
  and a pair that would overflow the bucket arithmetic. New classes
  `search_anon` (10/s, burst 20) and `search_auth` (30/s, burst 60) now
  charge `searchActors`, which used the read classes before. `resolve` has
  one bucket per endpoint and IP. `rate_limits.endpoint_class.<nsid>` moves
  one served endpoint to another class family (`read`, `search`, `write`,
  `private`, `thread_lens`, `index_request`, `resolve`); an unknown family,
  an NSID this AppView does not serve, or a DID-keyed family on an endpoint
  that accepts anonymous calls is a startup error. `service_proxy_member`
  and `service_proxy_ip` are accepted and validated; nothing charges them
  until service proxying is built.
- **Item 10: upstream calls draw on pools and shares (#669).** Every
  request carries a handle naming who is charged (a ready member by DID,
  everyone else by IP key) and one 6 s deadline that all its cascades
  share, in place of 6 s per cascade (design-v3 §II-8.3). Each outbound
  network's `rate_per_sec`/`burst` bucket, Slingshot
  (`microcosm.slingshot_rate_per_sec`) and request-origin Constellation
  (`microcosm.constellation_rate_per_sec`) are now split into a member pool
  and a non-member pool with per-requester shares. With the defaults a
  single non-member address gets 2.5 calls a second (burst 5) to Bluesky's
  AppView, where every caller shared 20 a second before. An empty share or
  pool is no answer: it stops the cascade and its fallbacks for that item,
  is never recorded by a breaker, and never turns into a not-found.
  - A single-subject request that then finds nothing (`getProfile`,
    `getPostThread`, an actor that does not resolve) answers
    `429 RateLimitExceeded` with `ratelimit-reset`, not "not found".
  - `getLikes`, `getRepostedBy`, `getQuotes` and `getFollowers` answer 429
    when there is no budget for the Constellation listing, and no longer
    fall back to local rows in that case.
  - Posts and profiles inside a list are left out; a thread's parent chain
    stops without a not-found item; counts use the stored or local value;
    Thread Lens reports `upstream-unavailable`.
  - A cascade cut by the request's deadline is no answer too. A call the
    deadline cuts short is not counted against the network's breaker
    (`upstream_budget_cutoff_total{network}`); before, slow requests could
    open a healthy network's breaker.
  - An author feed or follows list read from another AppView or the
    author's PDS answers 429 when it ran out of budget before producing
    anything. A PDS feed that runs out while fetching a reposted post ends
    the page before that item and keeps its cursor behind it, so the next
    page resumes there.
- **Item 10: the pre-auth backstop chooses who a request is charged to
  (#669).** Before the per-requester backstop, a request whose token passes
  the claims pre-check, names a ready member whose signing key is held in
  memory, is admitted by the verification limits and verifies against that
  key is charged to the member's DID: the `global` backstop, and every
  upstream pool and share. Everything else is charged to its IP key
  (design-v3 §II-8.3). So members behind one PDS no longer share that
  PDS's address for the backstop or for upstream budgets, and a forged
  token naming a member spends none of the member's limits. That step
  reads no database and makes no network call.
  - A key under 1 h old authenticates, and the handler does not verify the
    token a second time. A key 1 h to 24 h old only selects the member;
    the handler verifies as before.
  - Ready members' DID documents are held in a table reloaded from
    `did_cache` every 30 s; documents older than 12 h, or missing, are
    re-resolved in the background (`member_key_refresh_failed_total`,
    gauge `member_keys`).
  - A member whose own verification ceiling is empty gets 429 and is
    charged to neither key.
  - Thread Lens, the private endpoints and `index.requestRecord` use the
    same selection.
  Differences from the design text, recorded on #669: the selection is
  passed to handlers through a task-local, not request extensions; the key
  table follows `did_cache` with up to 30 s delay instead of being updated
  on each write; only the table counts as an in-memory key (not
  kryphocron's own cache or the fetch cell).
- **Item 11: retention covers the new tables (#671).** The daily job
  prunes lists, list items, list blocks, opt-outs, starter packs, labeler
  declarations, self records and poll posts once their author leaves
  scope (design-v3 §14.1). Thread and post gates go the same way, unless
  their post row is still kept. Starter-pack counts whose pack is gone
  are swept, and `tag_activity_hourly` rows older than 72 hours are
  deleted. The subscription tables and the viewer-private tables are
  never swept. Deleting a post now deletes its canonical thread gate and
  post gate with it.
- **Item 11: backfills fetch the new collections (#671).** The depth
  table gains them (design-v3 §II-5.10): a member's lists, list items, list
  blocks, opt-outs and starter packs are listed whole, its thread and post
  gates and poll posts with the same limit as its posts; a scoped DID gets
  25 starter packs, 100 poll posts and the gates of its 100 posts; the
  labeler declaration and the three self records are fetched with
  `getRecord`, like the profile. A gate is kept only when its post is
  among the posts the job selected. `private` and `authz` jobs never list
  them, so readiness is unaffected. Reconcile deletes a stored record of
  these collections that the repo no longer has, as it does for posts.
- **Item 11: one-time re-sync after the upgrade (#671).** Once, after
  the upgrade, `serve` enqueues a `resync` job for every member and then
  for every other in-scope DID, one a second, so the new collections and
  each profile's CID are fetched for accounts known before it
  (design-v3 §12.3). Progress is kept in
  `worker_state['v3_collections_resync']`; a restart resumes, and a
  finished pass never runs again.
- **Item 11: the post-migration backfill task (#671).** `serve` starts a
  background task once migrations are applied (design-v3 §12.2).
  - Phase 1 fills the new `posts` columns from each stored record, and the
    new profile columns of `actors`, in keyset batches
    (`schema_backfill.batch_size`, default 10 000, with
    `schema_backfill.pause_ms`, default 100, between them). A batch only
    touches rows still unfilled whose `cid` (or profile rev) is the one it
    read, so it never overwrites what the writer stored meanwhile. Progress
    is kept in `worker_state`, so a restart resumes.
  - Phase 2 then builds `posts_search`, `posts_tags`, `posts_domains`,
    `posts_urls`, `posts_mentions`, `posts_langs`, `posts_created`,
    `likes_via` and `reposts_via`, one at a time, with
    `CREATE INDEX CONCURRENTLY` on a connection of its own with no lock or
    statement timeout. An invalid index left by an interrupted build is
    waited for while a backend still builds it and dropped otherwise. A
    failed build is retried after `schema_index.retry_backoff_secs`
    (default 300), doubling up to an hour.
  - `_health` detail gains `schema_backfill_done` and
    `search_indexes_ready`. Metrics: `schema_backfill_remaining{table}`,
    `schema_index_pending`, `schema_index_build_failures_total{index}`.
  Expect the `posts` heap and its indexes to grow, up to about twice,
  while phase 1 rewrites old rows.
- **Item 11: twelve more collections are indexed (#671).** Lists, list
  items, list blocks, reference-list opt-outs, starter packs, labeler
  declarations, the notification declaration, actor status and
  content-visibility declaration, thread gates, post gates and Nearhorizon
  poll posts are classified and written to their tables, with the same
  last-writer-wins, tombstone and delete rules as every record
  (design-v3 §3.2). All are admitted by rule 1 only: their author must be
  in scope.
  - A list item is indexed only when its `list` is a list in its own
    repo. Otherwise any account could add subjects to another account's
    list.
  - The labeler and the three declaration records are indexed only at
    rkey `self`.
  - `index.requestRecord` accepts these collections.
  - The `account_deleted` purge removes an account's rows from every new
    record table, its derived starter-pack counts, its own private state,
    and subscriptions others hold on it. The `member_removed` purge
    removes the member's private state (list and thread mutes, bookmarks,
    drafts, activity subscriptions, notification preferences).
  No endpoint reads the new tables yet, and the subscription copy of list
  items (rule 4) is item 12.
- **Item 11: the writer fills the new columns (#671).** Every post
  written from now on stores `tags`, `link_domains`, `link_urls`,
  `lang_prefixes`, `embed_type`, `media_type` and `search_tsv`
  (design-v3 §3.1, §9.0): tags are lowercased NFC without `#`, sorted and
  distinct; links are `http`/`https` only, with each link's host and its
  parent domains; languages carry their primary subtag. The arrays are
  `{}`, never NULL, on a written row. Likes, reposts and follows store
  their `via` reference. Profiles store `profile_cid`, `pronouns`,
  `website`, `joinedViaStarterPack` and a clamped `createdAt`; a profile
  write at the stored rev fills a missing `profile_cid` and changes
  nothing else. Rows written before this stay unfilled until the
  post-migration task runs. New dependency: `unicode-normalization`.
- **Item 11: migrations 0004–0011 and 0013–0017 (#671).** The schema of
  design-v3 §3.1: lists, list items, list blocks and opt-outs; the rule-4
  subscription tables with `subscribed_lists.item_count` kept by
  triggers; list and thread mutes; starter packs and their counts;
  labeler declarations and the other rkey-self actor records; thread and
  post gates; poll posts; bookmarks, drafts, activity subscriptions and
  notification preferences v2; `via` references on follows, likes and
  reposts; profile columns on `actors`; search and trend columns on
  `posts`; `tag_activity_hourly` with its `posts`, `likes` and `reposts`
  triggers; backfill job generations. Every change is additive, and none
  builds an index on `posts`, `likes` or `reposts`. Nothing writes or
  reads the new tables and columns yet. With 0012 they ship as one
  upgrade.
  - The startup check that every member can refresh its key once per
    window from PLC's issuer-refresh slice is added (`community.max_members`
    against `identity.member_pool_refresh_secs`, design-v3 §13).
- **Item 10: an in-scope author's feed continues from its PDS (G-3,
  #670).** When `getAuthorFeed` runs out of stored rows for an in-scope
  author whose PDS lists records newest first, it emits a
  `P:<oldest stored post rkey>:<oldest stored repost rkey>` cursor and the
  next page comes from the PDS, as it already did for out-of-scope
  authors. On a PDS that lists oldest first, or whose order is unknown,
  the feed ends with the stored rows. A PDS page that the filter empties
  now keeps its cursor, so it no longer ends the feed.
- **Item 10: `getPostThread` shows replies and parents it does not store
  (G-6, #670).** A node's replies are the union of the local rows and the
  replies Constellation lists for it (up to
  `thread.constellation_replies_per_node`, default 50). Replies and
  parents not stored here are fetched from Slingshot through the read-time
  cache, at most `thread.max_remote_fetches` (default 100) per request;
  the node cap is `thread.max_nodes` (default 200). Before, only local
  replies were shown and a parent by an out-of-scope author was a
  not-found item.
  - A listed reply that cannot be fetched, or whose record does not reply
    to that node, is left out, not shown as not-found.
  - A reply or parent by an in-scope author is never fetched: if it is not
    stored, it is gone.
  - A parent there was no budget to fetch ends the chain with no item.
- **Item 10: `getQuotes` shows quoting posts it does not store (G-7,
  #670).** A quoting post Constellation lists and this AppView has not
  indexed is fetched from Slingshot through the read-time cache, at most
  `limit` per page, and placed in listing order. Before, such posts were
  left out. Hidden posts and blocked authors are filtered before any
  fetch.
- **Item 10: `getProfiles` resolves out-of-scope actors (G-1, #670).**
  An actor this AppView does not index is fetched the way `getProfile`
  fetches it (its home AppView and the cascade, then the Slingshot profile
  record), up to 8 at a time, and returned in input order. Before, such
  actors were dropped. One that no source resolves, or that there is no
  upstream budget for, is omitted.
- **Item 10: token-issuer lookups are charged to the request (#669).**
  When verifying a token needs the issuer's DID document from the network,
  the fetch costs the request one token of its identity share and, for a
  `did:plc`, a PLC token (design-v3 §II-8.3). The charge is the first step
  of the fetch itself, so a document already held costs nothing and a
  fetch that finds no budget stores no failure. PLC's member pool keeps
  half of itself as an issuer-refresh slice: on a request charged to an IP
  key, a ready member's document may be fetched from it once per
  `identity.member_pool_refresh_secs` (default 600). So a flood of tokens
  naming members takes at most that slice, and the rest of the member pool
  stays for members' own requests. A request whose issuer lookup found no
  budget answers `429 RateLimitExceeded`, not `401`.
- **Item 10: pre-auth backstop parts (#669).**
  `xrpc::backstop` holds the two parts of design-v3 §II-8.3's backstop
  that need no I/O. The claims pre-check reads a token's unverified
  payload and applies the claim checks the verifier makes after the
  signature (`iss`, `aud` with no fragment or `#bsky_appview`, `lxm`,
  `iat`, `exp`, `nbf`, the validity window, `nonce`), with the verifier's
  own skew and window. `VerifyLimits` is the token lifecycle around a
  pre-backstop verification: failed-verification limits per (IP key,
  issuer) and per IP key, an exemption for a member that verified from
  that IP key against the same key within a day, a per-member ceiling and
  a per-IP-key ceiling. New `[auth]` settings, validated at startup.
- **Item 10: identity lookups and author-PDS listings are charged
  (#669).** Resolving a DID or a handle for a request costs one token of
  the requester's identity share
  (`identity.requester_resolutions_per_sec`, default 5/s, burst 25), and a
  PLC directory fetch it causes also draws on PLC's request bucket
  (`identity.plc_request_rate_per_sec`). A summary (PDS and verified
  handle) is reused for 5 minutes, so a DID named many times is resolved
  and charged once; before, every use repeated the handle check over the
  network. Without budget the lookup has no answer, and `resolveDid`
  answers 429 instead of the unresolvable shape. Every `listRecords` call
  made for a public request draws on its PDS host's own bucket
  (`sync.public_fallback_rate_per_host`, default 25/s, for up to
  `sync.max_public_fallback_hosts` hosts), split into pools and shares
  like the others. It no longer uses the private tier's read-fetch bucket,
  so public traffic cannot drain the private tier's head fetches. Token
  issuer lookups during authentication are not charged yet.
- **Item 10: pools and fair shares (#669).** `xrpc::pools`
  holds the shared upstream bucket of design-v3 §II-8.3: a member pool and
  a non-member pool (`constellation.nonmember_fraction`, default 0.5), and
  inside each a per-requester share (`constellation.requester_share`,
  default 0.25) kept in one LRU cache
  (`constellation.requester_share_max_entries`, default 200 000). A draw
  takes the share token and the pool token as one step, so an empty pool
  does not spend the share. New settings, validated at startup:
  `microcosm.slingshot_rate_per_sec`/`_burst`,
  `microcosm.constellation_rate_per_sec`/`_burst`,
  `identity.plc_request_rate_per_sec`/`_burst` and
  `identity.requester_resolutions_per_sec`/`_burst`.
- **Item 10: IPv6 callers are limited per `/64`.** Every IP-keyed rate
  limit (anonymous reads and search, `resolve`, the per-IP backstop) keys
  an IPv6 address by its `/64`, and an IPv4-mapped address by its IPv4
  address (design-v3 §II-8.3 item 3).
- **Item 10: gallery embeds.** `app.bsky.embed.gallery` is rendered wherever
  an embed is: as a post's embed, as the media of `recordWithMedia` (which
  no longer loses its quoted record), and inside a quoted post's `embeds`
  (design-v3 §8.4; locked decision 33). Every view item carries
  `$type: app.bsky.embed.gallery#viewImage` with `thumbnail`, `fullsize`,
  `alt` and `aspectRatio`; a record item of another type, or one missing
  `alt` or `aspectRatio`, is left out.
- **Item 10: `getAuthorFeed` `posts_with_video` (G-2).** The filter is
  accepted and returns posts whose media is a video, directly or inside
  `recordWithMedia`. One classifier, `media_type`, decides for local rows
  and for records listed from a PDS.
- **Item 10: scoped mutes.** `muteActor` takes `onlyReposts` and
  `onlyQuoteposts` and stores `mutes.scope` (`full`, `reposts`, `quotes`,
  `reposts_and_quotes`; migration 0012). A repeat call replaces the scope
  and keeps the row's `created_at`; `unmuteActor` deletes the row whatever
  its scope; `getMutes` lists full mutes only (design-v3 §5.3; locked
  decision 32). A repost mute removes the subject's reposts from the
  timeline and their `repost` notifications; a quote mute removes their
  quote posts and `quote` notifications. `viewer.muted` is the full mute;
  `mutedOnlyReposts` and `mutedOnlyQuoteposts` are set only when it is
  false. The private-tier mute oracle reads full mutes only.
- **Item 10: the final router, with CORS.** Every route is merged into one
  router, which then gets its fallbacks, the per-IP backstop and, outermost,
  a CORS layer (design-v3 §6.3, §6.5). Every `/xrpc/*` response, errors
  included, carries `Access-Control-Allow-Origin: *`; preflights allow `GET`
  and `POST` with `authorization`, `content-type`, `atproto-proxy` and
  `atproto-accept-labelers`; `X-Prism-Sort-Fallback`, `ratelimit-reset`,
  `Retry-After` and `atproto-content-labelers` are exposed. This covers the
  private, index, Thread Lens, resolve-DID and health routes, so a browser
  client can call the AppView directly with a bearer token.
- **Item 10: router fallback and wrong-verb answers.** An unregistered
  `/xrpc/<nsid>` answers `501 MethodNotImplemented`, a malformed NSID
  `400 InvalidRequest`, and a registered method asked with the wrong verb
  `400 InvalidRequest` "Incorrect HTTP method (X) expected Y". Before, these
  were a bare 404 and an empty 405. Both are charged to `anon_read`.
- **Item 10: honest-501 routes.** The 36 deferred endpoints (feed
  generators, push, preferences, reports, video, external embed views, age
  assurance, contact) answer `501 NotImplemented` from registered routes,
  for their lexicon verb, without authentication (§6.1, §6.2).
  `honest_501.status_url` fills the "See <link> for status." pointer, and an
  empty one drops that sentence; `honest_501.messages.<nsid>` overrides one
  message. `honest_501_total{nsid}`.
- **Item 10: `[services.*]` and `[features]` config.** Deferred external
  services (`contact`, `age_assurance`, `push`, and operator-defined ones
  with their own `methods`) are `disabled`, `proxy` or `serve` (§6.7). This
  build accepts only `disabled`: `proxy`, `serve` and any `features.* = true`
  stop startup with exit 78 and say why. The whole of §6.7.2.3's validation
  is in, including the reserved namespaces and the one-owner-per-NSID check
  for custom services. `features.push`, `features.age_assurance` and
  `features.contact` are removed and name their replacement key.
- **Item 10: `Retry-After` on `NotReady`.** Every `400 NotReady` carries
  `Retry-After` from `community.not_ready_retry_secs` (default 5): a constant
  hint for clients, not an estimate of indexing progress.
- **Item 10: `#bsky_notif` and `/did.json`.** The did:web document lists the
  notification service beside `#bsky_appview`, so a PDS proxying push
  registration reaches the honest 501. `/did.json` serves the same bytes as
  `/.well-known/did.json`, both with `Cache-Control: public, max-age=300`.
- **Item 8.5: pre-Phase B verification tests.** Ascending-PDS author-feed
  fallback (newest `limit` records from at most 10 listing pages, no cursor,
  a `P:` cursor ends the feed) against a mock ascending PDS; eligibility
  streak restarts after a mid-streak success. The Thread Lens block-party
  and `tier-hidden` no-lookup tests re-run green.
- **Item 8.5: `app.nearhorizon.index.requestRecord`.** The §5.9 safety net:
  any authenticated DID may ask the AppView to index a record from its own
  repo; the record is fetched from the caller's PDS (`getLatestCommit`, then
  `getRecord`) and run through the normal write path, answering `indexed`,
  `deleted`, `out_of_scope` or `unchanged`. A URI outside the caller's
  repo and an unsupported collection are distinct `InvalidRequest` messages
  (#232); per-DID 0.5/s, burst 30. `admin deadletter replay` now shares the
  same procedure (`firehose::index_request`).
- **Item 8: upstream views prefer Constellation counts.** After
  normalization, every upstream post view (`likeCount`, `repostCount`,
  `replyCount`, `quoteCount`) and profile view (`followersCount`) takes a
  fresh or stale `constellation_counts` value over the upstream count;
  stale rows and targets with no row are queued on `cc:refresh`, and with no
  row the upstream count stays (never a local zero). Applies to public
  upstream views and Thread Lens. `upstream_view_counts_total{source}`.
- **Item 8: notification, mute and label coverage.** Tests for every
  notification reason's record (`reply`, `mention`, `quote`, `like`,
  `repost`, `follow`), block exclusion in both directions for the list and
  the unread count, `indexed_at::id` paging, unmute idempotence, and local
  label emission and enforcement (`!takedown` hidden from lists and direct
  fetches, `!hide` listed nowhere but returned on direct `getPosts`,
  informational labels on posts, authors, profiles and thread replies).
- **Item 8: mutating admin commands tested through the queue.**
  `backfill` (default `member`/`scope` depth, explicit depth),
  `scope rebuild --dry-run` (counts, no side effects), `resync
  --private-and-members` (gap epoch advanced, member readiness cleared,
  `authz`/`resync` enqueued), `deadletter replay` (record re-fetched and
  admitted, absent record deleted, entry left listed) and `deadletter drop`.
- **Item 7: migration 0002.** `private_posts.reply_root_cid` and
  `reply_parent_cid` (nullable). Rows written earlier keep NULL until an
  `admin backfill --depth resync` refetches them.
- **Item 7: reply cids on private views.** `postPrivate` ingest reads
  `reply.{root,parent}.cid` (string or `$link`; a cid of any other shape is
  malformed) and `privatePostView.record.reply` carries `{uri, cid}` strong
  refs, `{uri}` only for pre-0002 rows.
- **Item 7: eligibility failure handling.** Migration 0003 adds
  `actors.eligibility_failure_count` and `eligibility_failing_since`. A
  failed `getRepoStatus` confirmation (network error, 5xx) keeps eligibility
  and extends the streak; three consecutive failures spanning at least 24 h
  remove it (`eligibility_cleared_after_failures`). Definitive answers (repo
  not found, inactive, no PDS, host outside the community patterns) remove
  it at once, and any answer resets the streak. Backfill `prepare` now
  returns a typed `PrepareError` (`Negative` / `Failed`).
- **Item 7: `admin member remove` re-confirms eligibility.** For a private
  owner whose community-PDS confirmation is missing or older than 24 h, the
  command re-confirms with `getRepoStatus` before the `member_removed`
  marker, so the purge end's owner re-evaluation sees fresh eligibility. The
  outcome never blocks the removal and is reported in the command result
  (`eligibility`).
- **Item 7: Slingshot client.** `microcosm::slingshot` `getRecord` for
  record-level fallback (§8.3 step 6, Thread Lens step 4): through `net`,
  3 s timeout, 256 KiB cap; classified as record, `NotFound`, `Transient` or
  `Invalid`. `net` gains a test-only loopback client for fake upstreams.
- **Item 7: `appview_clients`.** One outbound client per enabled network
  (the local `community` network has none): `unauth`, or `service_jwt` with
  a fresh AppView-signed token per call (`iss` did:web, `aud` =
  `appview_did`, `lxm`, 60 s `exp`, 16-byte `jti`, ES256K low-S). Typed
  `getProfile`, `getPosts`, `getPostThread`, `getAuthorFeed`, `getFollows`;
  3 s per call, 4 MiB cap.
- **Item 7: `constellation` routing.** The routing table from
  `[[constellation.networks]]` and `[constellation.cascade]`: §8.2 PDS
  matching (first match in file order; `community` is local, never an
  outbound call), the per-DID `route` cache (200 000 entries, 1 h, resolved
  DIDs only), and the call plan (home AppView, then the cascade without it,
  disabled networks skipped). §8.3 response classes (`hit`, `miss`,
  `withheld`, `auth-refused`, `transient`); per-network token buckets
  (empty = transient skip) and breakers (5 consecutive failures, 30 s open,
  one half-open probe); 3 s per call, 6 s per cascade.
  `app.nearhorizon.constellation.resolveDid` (public, 10/s burst 20 per IP,
  `cache-control: public, max-age=300`). Rate classes `threadLens` (2/s,
  10 per DID) and `resolveDid` added.
- **Item 7: `RealUpstreamSource`.** Hydration's seam now serves
  out-of-scope subjects: home AppView then the cascade (skipped for the
  `community` network), then record-level fallback. Profiles, posts and
  thread anchors (with their parent chain) fall back to Slingshot records;
  author feeds and follows fall back to the author PDS `listRecords` on the
  read-fetch budget, continuing (`P:` cursor) only on PDSes that list
  descending (order probed once per host, cached 24 h). Upstream views are
  normalized (§8.6: local viewer state and labels, private references
  not-found) and cached in `rt`; upstream post and thread fetches run the
  moderation gate first. Non-indexed handles resolve through the identity
  resolver (bidirectionally verified). `serve` wires the routing table,
  AppView clients, cascade, Slingshot and `resolveDid`.
- **Item 7: Thread Lens.** `app.nearhorizon.threadLens.getThread` and
  `.resolve` (POST, service-JWT auth, 2/s burst 10 per DID). The caller DID
  is used for the auth gate and rate limit only. Hydration runs with no
  viewer at all (never `actor_viewer`/`post_viewer`, every `viewer` is
  `{}`), and the gap taxonomy has no viewer-relative variant. The §10.4
  ladder: `tier-hidden` before any lookup, then the moderation gate, local
  tombstones and posts, home AppView and cascade (`getPosts`), Slingshot,
  and classification (`deleted`, `not-found`, `upstream-unavailable`,
  `content-stripped`). `getThread` takes its structure from upstream
  `getPostThread` (`#blockedPost` stubs → local/Slingshot, else
  `blocked`; `#notFoundPost` → full ladder), falls back to the anchor and
  parent chain, appends local and Constellation replies breadth-first, and
  caps at 200 nodes. Responses are `PublicEmit`s built in
  `hydration::lens`.
- **Item 6: private tier and `app.nearhorizon.private.*`.** `private_tier`
  owns every private table behind a `pub(crate)` facade: defensive
  `postPrivate` / audience parsing with dead-lettering of malformed records
  (#235), LWW ingest, owner set and readiness, purges, and private retention
  (registered ahead of public retention). Reads run the §6.3 pipeline per
  record: moderation gate, owner readiness, repo-head check, a per-record
  oracle snapshot (Block/Audience/Mute, rules 1–7), host freshness against
  `private_watermark`, kryphocron issue/bind, the sealed `RecordAuthz`
  witness, record validation and `decode_record_content`, emitted only
  through `PrivateEmit`. `getPosts`, `getPostThread` (200 nodes,
  `publicParent`) and `getAuthorFeed` (`created_at::rkey` cursor) share one
  uniform miss, per-endpoint timing floors, a 10/s (burst 20) per-DID limit
  and `private_read_outcome_total` / `private_read_unfloored_seconds`. The
  `private` backfill job confirms non-member owner eligibility (§6.11.1).
  `kryphocron_glue` gains panic-contained log audit sinks, the user-sink
  latch reported by `/xrpc/_health`, and validated at-rest hooks from
  `APP_DATA_DIR`; `clippy.toml` forbids the public codec surface.
- **Item 5: `app.bsky.*` XRPC surface and read-only admin CLI.** axum routes
  for `actor.getProfile(s)`, `actor.searchActors`, `feed.getPosts`,
  `getPostThread`, `getAuthorFeed`, `getLikes`, `getRepostedBy`, `getQuotes`,
  `getTimeline`, `graph.getFollows`, `getFollowers`, `getMutes`,
  `muteActor`, `unmuteActor`, `notification.listNotifications`,
  `getUnreadCount`, `updateSeen`, and `/.well-known/did.json` (secp256k1
  Multikey). Callers authenticate through the `kryphocron_glue` JWT path:
  `verify_request_jwt` with the issuer tracker, eviction drainer, rotation
  retry and the `aud`-fragment / `lxm` host checks. Per-IP and per-DID
  `governor` limits with `ratelimit-reset`, trusted-proxy
  `X-Forwarded-For`, `NotEnrolled` / `NotReady` gates, and auto-enroll
  (§9.2) sharing the member-add path with the admin executor. Read-only CLI:
  `admin member list`, `label list`, `deadletter list`, `stream info`,
  `commands list`, `keygen`, and `migrate status`.
- **Item 5: public hydration.** `hydration` builds `app.bsky.*` views from
  local tables behind the sealed `PublicEmit<T>`: profiles (`getProfile(s)`,
  `searchActors`), posts with embeds, author feeds with the §5.12 filters and
  pins, member timelines, threads (parent chain, replies, 200-node cap),
  follows, followers, mutes and notifications. Viewer state only for ready
  members (`blocks` only, never private blocks); §9.6 counts (synchronous
  Constellation for single-object views, `cc:refresh` for lists);
  Constellation-first likes, reposts, quotes and followers with local
  fallback; the §8.6 upstream normalizer; the in-process read-time cache; an
  `UpstreamSource` seam answering a uniform miss until item 7. `moderation`
  adds the §7.10 gate (`!takedown`, `!hide`, account status).
- **Item 4.5: admin executor and head prober.** CLI-to-`serve` command queue
  with `LISTEN/NOTIFY` (member add/remove, labels, scope rebuild, backfill,
  resync, dead-letter replay/drop), purge-aware per-DID deferral and crash
  recovery; per-host repo-head prober with independent probe, read-fetch and
  backfill budgets and divergence detection; `authz_backfill_stuck_total` and
  the `authz_stuck` log for authorization records that can never parse.
- **Item 4: firehose ingest and aggregations.** Redis stream consumer with the
  `read_checked` / `reanchor` / `group_create` Lua scripts, per-relay
  watermarks, gap transaction, relay coverage tracker and dead letters; record
  classifier and defensive record parsing (#233); per-record last-writer-wins
  writer with sorted DID and URI advisory locks; admission predicate, scope
  growth, two-phase purges, weekly scope rebuild; backfill worker with
  reconcile and the Constellation inbound-block pass; Constellation count
  enrichment; `serve` wiring and `/xrpc/_health`.
- **Item 3: foundations.** Configuration (`app.toml` plus `APP_*` overlay),
  Postgres pool, migration 0001 and the exit-78 migration gate, retention and
  storage sampler, SSRF-guarded outbound HTTP (`net`), DID/handle resolution
  with the JWT fetch cell (#234) and the identity refresher.
- **Item 2: lexicons.** `app.nearhorizon.*` lexicon JSON for v1, with
  `audienceList` optional in `privatePostView`.

### Changed

- **Item 12: Farsight call outcomes are named (#673).**
  `farsight_calls_total{outcome}` now says `rate_limited` (this key's
  allowance on Farsight is spent), `overloaded` (Farsight is up and
  full) and `auth` (the key is refused) where it said `status` for all
  three. They are read from the error name Farsight sends, since it uses
  429 and 503 for two conditions each. What counts as a failure is
  unchanged.
- **Item 15: host names are looked up fully qualified (#675).** Every
  outbound DNS lookup now asks for the name with a trailing dot, so the
  resolver never retries it with a search domain. Inside Docker that
  retry could turn a DNS server failure into "no such name". An address
  is not looked up at all, as before. Groundwork for `resolveHandle`:
  `identity::handle_lookup` sorts a handle's DNS record and well-known
  file into found, confirmed absent, or try again (design-v3 §5.11).
- `getTimeline` does the same as `listNotifications` below: it hydrates
  every scanned item and applies the viewer's mutes and blocks afterwards.
- `listNotifications` hydrates every row it scans and applies the viewer's
  mutes, blocks and thread mutes afterwards, where it filtered in SQL before
  (design-v3 §8.1 "Hydrate before filtering"). What a member's read fetches
  and caches no longer depends on what the member muted. A page can now hold
  fewer than `limit` items while a cursor is still returned.
- `getAuthorFeed` `posts_with_media` now means images and galleries. Video
  moved to its own filter, as in the reference AppView, so a client with
  Media and Video tabs does not show a video twice. A `recordWithMedia`
  whose media is an external link no longer counts as media.
- The per-IP backstop is one layer on the final router instead of one per
  sub-router. `GET` and `HEAD` on `/xrpc/_health` stay exempt.
- `app.nearhorizon.private.getAuthorFeed`'s lexicon description names its
  ordering field (`record.createdAt`, clamped to indexing time plus five
  minutes).
- Design-v2 §6.11.3: read-fetch budget 25 per host (ascending-PDS worst
  case); §13 row 185.
- `sync.read_fetch_rate_per_host` defaults to 25 (was 20), in code and in
  `docker/config/app.toml`: the worst-case ascending-PDS author feed needs
  21 read-fetch calls (order probe, 10 post pages, 10 repost pages).
- Dockerfile: the COPY comment no longer describes `clippy.toml` as
  pending; it names the file and the clippy step that enforces it.
- F: requestRecord emits two distinct InvalidRequest messages (#232)
- Design-v2 §6.11.1/§4.2: eligibility failure handling (3 failures over
  24 h), negative answers, streak reset, and the `member remove`
  re-confirmation; reply cids and the eligibility columns in the schema.
- Design-v2 §7.1/§9.6: `getLikes` and `getRepostedBy` page Constellation
  `/links` (per-record timestamps), not `/links/distinct-dids`; counts are
  unchanged.
- I: postPrivate field parsing at the private-tier decode boundary must be defensive (#235)
- Design-v2 clarifications: dead-letter replay leaves the entry in place
  (§5.9); the head prober skips DIDs with an `authz` job in flight (§6.11.3);
  `admin backfill` defaults to `member` depth for members, `scope` otherwise
  (§3.3); item 5 builds the `kryphocron_glue` JWT path and an upstream seam
  (§15). postPrivate field parsing tracked for item 6 (#235).
- `read_checked` computes consumer lag as `entries-added − entries-read` from
  the group counter; Redis 7.4 re-estimates the `XINFO GROUPS` `lag` field.
- `hickory-resolver` pinned to 0.26 so one hickory is in the dependency graph.

### Fixed

- **Item 12: a Farsight that hangs now counts as failing (#673).** A
  `checkBlocks` call that ran out its whole timeout was meant to count
  towards pausing Farsight calls, but the check compared the time left
  after the clock had started against the full timeout, so it never did:
  with Farsight hanging, every member request waited the full timeout.
  The check now uses the time the request started with. A half of a
  request asked again after a too-large answer only has the time the
  first call left, so its timeout does not count.
- **Item 10: an upstream network could stay unreachable until restart
  (#669).** After a breaker's open period one probing call is let through
  and every other call is refused until that call's result is recorded. A
  request dropped while its probe was in flight (a client disconnecting, a
  timeout further up) never recorded one, so the network was skipped by
  every later cascade until the process restarted. Admitted calls now hold
  the probe slot through a guard that gives it back on any exit without a
  recorded outcome (design-v3 §II-8.3). `SharedBreaker` and `Admission` are
  the reusable form, for the Farsight breaker later.

- **Item 10: `seenAt` on the notification endpoints (G-4, G-5).**
  `getUnreadCount` now reads `seenAt` and, when present, counts from it
  instead of from the stored last-seen time; before, the parameter was
  ignored. `listNotifications` answers `400 InvalidRequest` to a request
  carrying `seenAt`, which the lexicon marks unsupported; before, it was
  accepted and ignored. `priority` stays accepted and ignored on both.

- Retention and scope growth no longer race: scope-dependent deletes run per
  DID under the DID's exclusive advisory lock, and growth takes the subject's
  shared lock before inserting `scope_dids`.

### Security

- A service token whose `iat`, `exp` or `nbf` is too large to be a time is
  rejected as `InvalidToken` before verification (#682). kryphocron 0.4.0
  turns those claims into times with an addition that panics on overflow,
  after the signature has verified, and anyone can sign a token for a DID
  of their own. Such a request used to end in a panicked request task
  instead of a 401.
- `/xrpc/_health` answers anonymous callers with `{"ok","version"}` only;
  the db/redis/audit flags, per-relay watermarks, `private_watermark` and
  `gap_blocked` need a service JWT that verifies with `lxm` `_health` (no
  membership check). An invalid token is answered as anonymous. Status
  codes are unchanged (200/503), and the route stays outside the global
  per-IP bucket except that a request carrying `Authorization` is charged
  to it before verification. Design-v2 §7.1; §13 row 186.
- Authorization-collection records that fail deterministically keep readiness
  unset (fail-closed) and raise the `AuthzBackfillStuck` alert.
