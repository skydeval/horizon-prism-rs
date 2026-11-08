//! Public indexing as an operator switch (public-indexing design).

pub mod full;
pub mod narrowing;
pub mod public_fill;
pub mod public_mode;
pub mod public_sweep;
pub mod trial;
mod trial_check;
#[cfg(test)]
mod tests;
#[cfg(test)]
mod trial_tests;

pub use public_mode::{Applied, Breadth, FileValue, PublicIndexing, PublicMode, PublicModeSettings, PublicState, ScopeValue, Spawn};
