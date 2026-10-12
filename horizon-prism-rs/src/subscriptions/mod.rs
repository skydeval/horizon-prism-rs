//! Rule 4: subscribed lists (design-v3 §4.3).
//!
//! A member's own list mute or list block only does anything if this
//! AppView knows the list's items, and a moderation list is usually owned
//! by someone it does not index. So it keeps a private copy of the lists
//! its members reference, in tables no public read path reads, and uses it
//! only to apply those members' own mutes and blocks.
//!
//! Built so far: the member's side ([`references`]), the list phase and
//! the release of an unreferenced list ([`list_phase`], [`applier`]), the
//! job row's enqueue rules ([`jobs`]), the claim ([`claim`]) and the run
//! itself ([`run`], [`sync`], [`fetch`], [`pds`]). Nothing starts them
//! yet. The copy rule and the views that apply a reference are not built,
//! so a stored reference has no effect on any view.

pub mod applier;
pub mod claim;
pub mod fetch;
pub mod jobs;
pub mod list_phase;
pub mod pds;
pub mod references;
pub mod run;
pub mod runner;
pub mod sync;
