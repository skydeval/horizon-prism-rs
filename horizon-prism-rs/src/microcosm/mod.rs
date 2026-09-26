//! microcosm.blue clients (design-v2 §3.2, §9.6): Constellation backlinks
//! and Slingshot records.

pub mod constellation;
pub mod slingshot;

pub use constellation::{ConstellationClient, ConstellationError, DidsPage, LinkingRecord, LinksPage};
pub use slingshot::{SlingshotClient, SlingshotError, SlingshotRecord};
