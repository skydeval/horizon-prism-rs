//! An operator's own look of the admin pages (Settings, Appearance; #810):
//! light, dark or the device's choice, and a colour scheme. Kept per
//! operator in `admin_operators` (migration 0038), so it follows them to
//! any device; read with the session on every signed-in page and written on
//! `<html>` as two classes, so the page draws in its colours from the first
//! paint, with no script and no inline style. Pages without a signed-in
//! operator (sign-in, setup, refusals) use the default.
//!
//! Not an `operator_settings` key and not in the audit log: it changes
//! nothing about this server, only how its pages look to one operator.

use sqlx::PgPool;

/// Light or dark.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum Mode {
    /// Follow the device.
    #[default]
    System,
    Light,
    Dark,
}

impl Mode {
    pub const ALL: [Self; 3] = [Self::System, Self::Light, Self::Dark];

    /// Its stored name, also its class (`mode-…`).
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::System => "system",
            Self::Light => "light",
            Self::Dark => "dark",
        }
    }

    /// Its name on the page.
    #[must_use]
    pub fn label(self) -> &'static str {
        match self {
            Self::System => "Same as the device",
            Self::Light => "Light",
            Self::Dark => "Dark",
        }
    }

    #[must_use]
    pub fn parse(s: &str) -> Option<Self> {
        Self::ALL.into_iter().find(|m| m.as_str() == s)
    }
}

/// A colour scheme: the accent of links, buttons and the current page.
/// Each has a light and a dark form in the stylesheet, both readable on
/// their background.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum Scheme {
    /// The original blue.
    #[default]
    Sky,
    Violet,
    Teal,
    Graphite,
}

impl Scheme {
    pub const ALL: [Self; 4] = [Self::Sky, Self::Violet, Self::Teal, Self::Graphite];

    /// Its stored name, also its class (`scheme-…`).
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Sky => "sky",
            Self::Violet => "violet",
            Self::Teal => "teal",
            Self::Graphite => "graphite",
        }
    }

    /// Its name on the page.
    #[must_use]
    pub fn label(self) -> &'static str {
        match self {
            Self::Sky => "Sky (the default)",
            Self::Violet => "Violet",
            Self::Teal => "Teal",
            Self::Graphite => "Graphite",
        }
    }

    #[must_use]
    pub fn parse(s: &str) -> Option<Self> {
        Self::ALL.into_iter().find(|m| m.as_str() == s)
    }
}

/// One operator's choice.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Theme {
    pub mode: Mode,
    pub scheme: Scheme,
}

impl Theme {
    /// From the stored names; an unknown one is the default.
    #[must_use]
    pub fn from_stored(mode: &str, scheme: &str) -> Self {
        Self { mode: Mode::parse(mode).unwrap_or_default(), scheme: Scheme::parse(scheme).unwrap_or_default() }
    }

    /// The classes on `<html>`.
    #[must_use]
    pub fn class(self) -> String {
        format!("mode-{} scheme-{}", self.mode.as_str(), self.scheme.as_str())
    }
}

/// Save `did`'s choice; `false` when `did` is not an operator.
///
/// # Errors
///
/// Database errors.
pub async fn save(pool: &PgPool, did: &str, theme: Theme) -> Result<bool, sqlx::Error> {
    let done = sqlx::query("UPDATE admin_operators SET theme_mode = $2, theme_scheme = $3 WHERE did = $1")
        .bind(did)
        .bind(theme.mode.as_str())
        .bind(theme.scheme.as_str())
        .execute(pool)
        .await?;
    Ok(done.rows_affected() == 1)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn names_round_trip_and_unknown_is_the_default() {
        for m in Mode::ALL {
            assert_eq!(Mode::parse(m.as_str()), Some(m));
        }
        for s in Scheme::ALL {
            assert_eq!(Scheme::parse(s.as_str()), Some(s));
        }
        assert_eq!(Theme::from_stored("dark", "teal").class(), "mode-dark scheme-teal");
        assert_eq!(Theme::from_stored("purple", "nope"), Theme::default());
        assert_eq!(Theme::default().class(), "mode-system scheme-sky");
    }

    /// Every scheme the page offers has its rules in the stylesheet.
    #[test]
    fn every_scheme_is_styled() {
        for s in Scheme::ALL {
            assert!(crate::admin::CSS.contains(&format!(".scheme-{}", s.as_str())), "{s:?}");
        }
        assert!(crate::admin::CSS.contains(".mode-dark") && crate::admin::CSS.contains(".mode-light"));
    }
}
