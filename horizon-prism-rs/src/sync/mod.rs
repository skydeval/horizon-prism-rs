//! Repo-head checks for tracked DIDs (design-v2 §6.11.3): the per-host
//! request budgets and the head prober.

#[cfg(feature = "bench-heads")]
pub mod bench_heads;
pub mod budgets;
pub mod head_prober;

pub use budgets::{BudgetRates, Class, HostBudgets};
pub use head_prober::{HeadProber, HeadSource, HeadState, NetHeadSource, ProbeResult, StoredHead};

/// Whether `serve` spawns the background head prober: never under the
/// `bench-heads` feature (private-threads design §4, "the bench regime"),
/// so no background pass refreshes a bench DID between the runner's reset
/// and the request, and nothing probes a bench head against `applied_rev`.
pub const SPAWNS_PROBER: bool = cfg!(not(feature = "bench-heads"));
