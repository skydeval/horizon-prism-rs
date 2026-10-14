//! microcosm.blue clients (design-v2 §3.2, §9.6): Constellation backlinks
//! and Slingshot records.

pub mod constellation;
pub mod slingshot;

pub use constellation::{ConstellationClient, ConstellationError, DidsPage, LinkingRecord, LinksPage};
pub use slingshot::{SlingshotClient, SlingshotError, SlingshotRecord};

/// `microcosm_requests_total{service, origin, endpoint}` (design-v3 §14.2):
/// every Constellation and Slingshot request made, counted here, where it
/// is sent. `origin` is `request` for a call made while serving an XRPC
/// request, whose method is `endpoint`, and `background` (empty
/// `endpoint`) for any other.
pub const METRIC_REQUESTS: &str = "microcosm_requests_total";
