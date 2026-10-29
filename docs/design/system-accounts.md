# System accounts

A plural system often runs several accounts. Horizon's "Posting as" and "Act as" let a signed-in account post, like, repost and follow as another account of the same system without switching. System accounts are the list that says which accounts those are.

Horizon-Prism keeps the list the way an AppView keeps mutes: a private setting, never a record in any repository. It holds no sessions or tokens, and an entry grants nothing: every write as a listed account is authorised by that account's own session at its own server.

Names below: **A** owns a list; **B** is an account on it.

## What it does

- Any signed-in account, member or not, may keep a list of up to 31 accounts.
- A adds B by presenting a proof minted by B's own server for B: a service token with `lxm` `app.nearhorizon.account.confirmSystemAccount`. Nobody can add an account whose server does not confirm it.
- A reads, adds and removes. B can ask whether any list holds it, and can leave every list at once.
- Every write gives the list a new `rev`, larger than any before, so a device can tell an older answer from a newer one.
- The feature is optional. An operator switches it on with its own key (`APP_SYSTEM_ACCOUNTS_KEY_HEX`, see [configuration](../configuration.md)); `getMembership` then lists `account.systemAccounts`.

Methods: [API guide](../api.md), family `app.nearhorizon.account`.

## Privacy properties

- **Only A reads A's list.** Nothing else reads it; a build check keeps the rest of the code from naming its tables.
- **B learns only yes or no.** `getSystemAccountStatus` never says which lists, nor how many.
- **Sealed at rest.** B's DID is encrypted with a server key and looked up through a keyed hash, and never sent to the database in clear. A dump without the key shows A's DID beside opaque bytes.
- **Not logged.** No DID, entry, proof or token is logged. Metrics carry the method and outcome only.
- **Hard deletes.** Remove, leave and account deletion delete rows.
- **Short, single-use tokens.** The proof and the write tokens live at most 65 seconds and are accepted once. A request refused before it took effect (rate limit, bad proof, full list) uses nothing and may be resent.
- **Direct calls.** Clients call these methods directly, not through their PDS; B's server learns only that B was added to some list.

## Account deletion

A deleted account's list goes, and it leaves every list it was on. Deletion counts at once for a PLC tombstone or a server that reports the account deleted, and after a day for "repo not found" or a gone did:web document. Deactivated or taken-down accounts keep their rows. Lists are not tied to membership.

## Limits

- The operator, a compromise of the running service, or anyone with both the database and the key can read the lists: the same trust as mutes on any AppView.
- The shipped Caddy configuration writes no access log; your own proxy or CDN must not log request bodies or `Authorization`.
- A deletion Horizon-Prism never hears about leaves the rows until the account's next event.
- The key has no rotation. Lost or changed, the lists are kept but locked until the right key is back (see [configuration](../configuration.md#system-accounts-key)).
