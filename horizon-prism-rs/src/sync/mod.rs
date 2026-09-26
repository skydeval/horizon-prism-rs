//! Repo-head checks for tracked DIDs (design-v2 §6.11.3): the per-host
//! request budgets and the head prober.

pub mod budgets;
pub mod head_prober;

pub use budgets::{BudgetRates, Class, HostBudgets};
pub use head_prober::{HeadProber, HeadSource, NetHeadSource, ProbeResult};
