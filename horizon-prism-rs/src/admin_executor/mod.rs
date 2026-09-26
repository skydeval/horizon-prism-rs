//! The CLI-to-`serve` command queue (design-v2 §3.3). Mutating admin
//! commands are inserted into `admin_commands` by the CLI
//! (`crate::cli::dispatch`) and executed here, inside `serve`, so the
//! in-memory `ScopeState`, the job workers and the §6.11.7 lock discipline
//! stay authoritative.

pub mod commands;
pub mod executor;

pub use commands::{AdminCommand, Handlers, InvalidCommand, LABEL_VALUES, Outcome};
pub use executor::{AdminExecutor, CHANNEL};
