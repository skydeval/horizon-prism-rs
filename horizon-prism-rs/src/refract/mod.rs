//! Refract (refract design, `docs/internal/refract/refract.md`): accounts
//! Bluesky's AppView withholds stay usable while a community the operator
//! lists still hosts them and has not banned them.
//!
//! - [`snapshot`]: what the moderation gate hides for Refract, and the
//!   per-request snapshot of it (§4.2, §7.4).

pub mod snapshot;
#[cfg(test)]
mod tests;

pub use snapshot::{Captured, CollectionRule, HideReasons, RefractCheck, RefractHidden, RefractSnapshot, RefractView};
