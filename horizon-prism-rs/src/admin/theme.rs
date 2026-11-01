//! An operator's own look of the admin pages (Appearance): light, dark or
//! the device's choice, a colour scheme and a font. Kept per operator in
//! `admin_operators` (migrations 0038 and 0061), so it follows them to any
//! device; read with the session on every signed-in page and written on
//! `<html>` as three classes, so the page draws in its colours and font
//! from the first paint, with no script and no inline style. Pages without
//! a signed-in operator (sign-in, setup, refusals) use the device's mode
//! and font and the default scheme, so they load no font file.
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

/// The pages' font: one face for text and one for code, each compiled in
/// (`fonts`), or the device's own.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum Font {
    /// Atkinson Hyperlegible, with JetBrains Mono for code.
    #[default]
    Hyperlegible,
    /// The device's fonts, for text and code.
    System,
    /// IBM Plex Sans, with IBM Plex Mono for code.
    Plex,
}

impl Font {
    pub const ALL: [Self; 3] = [Self::Hyperlegible, Self::System, Self::Plex];

    /// Its stored name, also its class (`font-…`).
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Hyperlegible => "hyperlegible",
            Self::System => "system",
            Self::Plex => "plex",
        }
    }

    /// Its name on the page.
    #[must_use]
    pub fn label(self) -> &'static str {
        match self {
            Self::Hyperlegible => "Atkinson Hyperlegible (the default)",
            Self::System => "Your device's fonts, for text and code",
            Self::Plex => "IBM Plex",
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
    pub font: Font,
}

impl Theme {
    /// From the stored names; an unknown one is the default.
    #[must_use]
    pub fn from_stored(mode: &str, scheme: &str, font: &str) -> Self {
        Self {
            mode: Mode::parse(mode).unwrap_or_default(),
            scheme: Scheme::parse(scheme).unwrap_or_default(),
            font: Font::parse(font).unwrap_or_default(),
        }
    }

    /// The classes on `<html>`.
    #[must_use]
    pub fn class(self) -> String {
        format!("mode-{} scheme-{} font-{}", self.mode.as_str(), self.scheme.as_str(), self.font.as_str())
    }
}

/// Save `did`'s choice; `false` when `did` is not an operator.
///
/// # Errors
///
/// Database errors.
pub async fn save(pool: &PgPool, did: &str, theme: Theme) -> Result<bool, sqlx::Error> {
    let done = sqlx::query("UPDATE admin_operators SET theme_mode = $2, theme_scheme = $3, theme_font = $4 WHERE did = $1")
        .bind(did)
        .bind(theme.mode.as_str())
        .bind(theme.scheme.as_str())
        .bind(theme.font.as_str())
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
        for f in Font::ALL {
            assert_eq!(Font::parse(f.as_str()), Some(f));
        }
        assert_eq!(Theme::from_stored("dark", "teal", "plex").class(), "mode-dark scheme-teal font-plex");
        assert_eq!(Theme::from_stored("purple", "nope", "comic"), Theme::default());
        assert_eq!(Theme::from_stored("light", "violet", "").font, Font::Hyperlegible);
        assert_eq!(Theme::default().class(), "mode-system scheme-sky font-hyperlegible");
    }

    /// Every font has its class in the stylesheet, after the `:root` rules
    /// it overrides at the same specificity, and the label sample after
    /// them, so a label's own font class sets the face it reads.
    #[test]
    fn every_font_is_styled_after_root() {
        let css = crate::admin::CSS;
        let last_root = css.rfind("\n:root").expect(":root");
        let mut last_font = 0;
        for f in Font::ALL {
            let rule = format!("\n.font-{} {{", f.as_str());
            let at = css.find(&rule).unwrap_or_else(|| panic!("{rule:?}"));
            assert_eq!(css.matches(&rule).count(), 1, "{rule:?}");
            assert!(at > last_root, "{rule:?} comes before :root");
            let body = &css[at..at + css[at..].find('}').expect("end")];
            assert!(body.contains("--font-sans:") && body.contains("--font-mono:"), "{body}");
            last_font = last_font.max(at);
        }
        let sample = css.find("\n.font-sample {").expect(".font-sample");
        assert!(sample > last_font, ".font-sample comes before a font rule");
        assert!(css[sample..].starts_with("\n.font-sample { font-family: var(--font-sans); }"));
    }

    /// The migration over an operator row from before it: the row reads
    /// back the default, and running it again changes nothing.
    #[tokio::test]
    async fn the_font_column_defaults_for_existing_operators() {
        let Some(db) = crate::test_support::empty_db("admin_font_column").await else { return };
        let p = &db.pool;
        let migrations: Vec<_> = crate::db::migrations::MIGRATOR.iter().filter(|m| !m.migration_type.is_down_migration()).collect();
        let at = migrations.iter().position(|m| m.version == 61).expect("0061");
        for m in &migrations[..at] {
            sqlx::raw_sql(&m.sql).execute(p).await.unwrap_or_else(|e| panic!("{}: {e}", m.version));
        }
        let did = "did:plc:aaaaaaaaaaaaaaaaaaaaaaaa";
        sqlx::query("INSERT INTO admin_operators (did, added_by, theme_mode, theme_scheme) VALUES ($1, 'cli', 'dark', 'teal')")
            .bind(did)
            .execute(p)
            .await
            .expect("an operator before the font");
        sqlx::raw_sql(&migrations[at].sql).execute(p).await.expect("0061 applies over existing rows");
        let row: (String, String, String) =
            sqlx::query_as("SELECT theme_mode, theme_scheme, theme_font FROM admin_operators WHERE did = $1").bind(did).fetch_one(p).await.expect("row");
        assert_eq!(row, ("dark".to_string(), "teal".to_string(), "hyperlegible".to_string()));
        assert_eq!(Theme::from_stored(&row.0, &row.1, &row.2), Theme { mode: Mode::Dark, scheme: Scheme::Teal, font: Font::Hyperlegible });
        // Again, as after a rollback that removed only the migration record.
        sqlx::raw_sql(&migrations[at].sql).execute(p).await.expect("0061 applies again");
        assert!(save(p, did, Theme { font: Font::Plex, ..Theme::default() }).await.expect("save"));
        let font: String = sqlx::query_scalar("SELECT theme_font FROM admin_operators WHERE did = $1").bind(did).fetch_one(p).await.expect("font");
        assert_eq!(font, "plex");
        db.drop_db().await;
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
