# System accounts

A plural system often runs several accounts. Horizon's "Posting as" and "Act as" let a signed-in account post, like, repost and follow as another account of the same system without switching. System accounts are the list that says which accounts those are.

Horizon-Prism keeps that list the way an AppView keeps mutes: a private setting of the account, stored by the AppView, never a record in any repository and never on any public surface. Sessions stay on each device; Horizon-Prism holds no tokens, only the list. A list entry grants nothing by itself: every write as a listed account is still authorised by that account's own session at its own server.

Names below: **A** owns a list; **B** is an account on it.

## What it does

- Any signed-in account, member or not, may keep a list of up to 31 accounts.
- A adds B by presenting a proof minted by B's own server for B: a service token with `lxm` `app.nearhorizon.account.confirmSystemAccount`. Nobody can add an account whose server does not confirm it.
- A reads, adds and removes. B can ask whether any list holds it, and can leave every list at once.
- Every write gives the list a new `rev`, larger than any before, so a device can tell an older answer from a newer one.
- The feature is optional. An operator switches it on with its own key (`APP_SYSTEM_ACCOUNTS_KEY_HEX`, see [configuration](../configuration.md)); `getMembership` then lists `account.systemAccounts`.

Methods: [API guide](../api.md), family `app.nearhorizon.account`.

## Privacy properties

- **Only A reads A's list.** No other method, feed, notification, moderation view or admin page reads it, and a build check keeps every other part of the code from naming its tables.
- **B learns only yes or no.** `getSystemAccountStatus` never says which lists, nor how many.
- **Sealed at rest.** B's DID is encrypted with a server key, and looked up through a keyed hash. A database dump or backup taken without the key shows A's DID beside opaque bytes, not B, so it does not show the pairs. B's DID is never sent to the database in clear, so a database's own statement log cannot show it either.
- **Not logged.** These methods log no DID, no list entry, no proof and no token, at any level. Metrics carry the method and outcome only. The admin UI counts calls per method like any other, and shows nothing of the lists.
- **Hard deletes.** Remove, leave and account deletion delete rows; nothing is kept behind.
- **Short, single-use tokens.** The proof and the tokens of the three writes must live at most 65 seconds, and each is accepted once. A token used by a request that succeeded is refused; a request refused before it took effect (rate limit, a bad proof, a full list) uses nothing, so a client may resend it.
- **Direct calls.** Clients call these methods directly, not through their PDS, so no PDS sees the list or B's DID; B's server learns only that B was added to some list.

## Account deletion

When an account is deleted, its list goes, and it leaves every list it was on. Deletion is decided as for every account Horizon-Prism tracks: at once for a PLC tombstone or a server that reports the account deleted; after a day for a server that answers "repo not found", or a did:web document that is gone. A deactivated or taken-down account keeps its rows, since it may come back. Leaving a community changes nothing, since lists are not tied to membership.

## Limits

- Horizon-Prism's operator, a compromise of the running service, or anyone with both the database and the key can read the lists. This is the same trust as mutes kept by any AppView, and is the reason for the key being separate from the database.
- Requests pass through whatever terminates TLS in front of Horizon-Prism. The shipped Caddy configuration writes no access log; an operator's own proxy or CDN must not log request bodies or `Authorization`.
- Refreshing a deleted account's identity stores what Horizon-Prism stores for any account it refreshes (its DID document and status), never which list held it.
- A deletion Horizon-Prism never hears about leaves the rows until the next event for that account.
- The key has no rotation in this version.
