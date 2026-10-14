//! Public indexing as an operator switch (public-indexing design).

pub mod public_fill;
pub mod public_mode;
pub mod public_sweep;
#[cfg(test)]
mod tests;

pub use public_mode::{Applied, FileValue, PublicIndexing, PublicMode, PublicModeSettings, PublicState, ScopeValue, Spawn};
