//! Storage layer (design-v2 §4): the Postgres pool, the migration gate,
//! retention and the storage sampler.
//!
//! This module never names a private-tier table (§4.2): private-tier queries
//! live only in `private_tier`, and private retention plugs into the
//! scheduler here as a [`retention::RetentionTask`].

pub mod migrations;
pub mod pool;
pub mod retention;
pub mod sampler;

pub use migrations::{
    GateError, GateOutcome, GatePlan, MIGRATION_GATE_EXIT_CODE, MIGRATOR, enforce_gate,
};
pub use pool::connect;
pub use retention::{
    PublicRetention, RetentionInputs, RetentionReport, RetentionSettings, RetentionTask,
    ScopeGuard, ScopeReadGuard, spawn_scheduler,
};
pub use sampler::StorageSampler;
