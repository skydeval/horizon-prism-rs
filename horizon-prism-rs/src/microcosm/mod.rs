//! microcosm.blue clients (design-v2 §3.2, §9.6).

pub mod constellation;

pub use constellation::{ConstellationClient, ConstellationError, DidsPage, LinkingRecord, LinksPage};
