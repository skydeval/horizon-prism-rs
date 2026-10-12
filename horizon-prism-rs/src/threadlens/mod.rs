//! Thread Lens (design-v2 §10): `getThread` and `resolve`, the per-URI fetch
//! ladder, and the server gap taxonomy.
//!
//! **The block-party rule (§10.3) is structural here.**
//! - The caller's DID is used by the handlers' auth gate and rate limit
//!   only; nothing downstream receives it.
//! - Hydration runs with no viewer (`hydration::lens`): no `actor_viewer`,
//!   no `post_viewer`, no block table; every `viewer` is `{}`.
//! - Upstream structure is fetched `unauth` or with the AppView's
//!   `service_jwt`, never a user identity.
//! - The kryphocron `BlockOracle` is never consulted: private-tier URIs are
//!   `tier-hidden` before any lookup (§10.5).
//! - The gap taxonomy has no viewer-relative variant.

pub mod cursor;
pub mod gaps;
pub mod handlers;
pub mod ladder;
pub mod thread;
pub mod verdict;

#[cfg(test)]
mod tests;

pub use gaps::Gap;
pub use handlers::{ThreadLensState, router};
pub use ladder::{Ladder, Mode, Resolved};
