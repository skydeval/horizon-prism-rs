//! `PublicEmit<T>` (design-v2 §7.2): the only type public handlers return.
//! Its constructor is `pub(in crate::hydration)`, so only hydration builders,
//! which read public tables, normalized upstream views and Slingshot or
//! Constellation data, can create one. Handlers can hold it but not build it.

use serde::Serialize;

/// A public-tier response body.
#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(transparent)]
pub struct PublicEmit<T>(T);

impl<T> PublicEmit<T> {
    pub(in crate::hydration) fn new(value: T) -> Self {
        Self(value)
    }

    /// Borrow the wrapped view (tests and logging).
    #[must_use]
    pub fn inner(&self) -> &T {
        &self.0
    }
}
