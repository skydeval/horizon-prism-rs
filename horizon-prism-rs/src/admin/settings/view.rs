//! A page's settings as its settings block shows them: one card per section,
//! each with its explanation, whether it applies at once or after a
//! restart, the value saved now, the default, the value `app.toml` writes if
//! it writes one, and the last change. Rarely changed sections are folded
//! under Advanced. Also where each key is changed: its feature page once
//! that page holds its settings, else its old settings group page.

use std::collections::BTreeMap;

use serde_json::Value;

use super::registry::{self, Group, Kind, Section};
use super::store::{self, Row};
use super::{SettingsContext, overlay};
use crate::admin::AdminState;
use crate::admin::menu::{PageId, SETTINGS_LIVE};
use crate::admin::pages::when;

/// A choice of a select.
pub struct Opt {
    pub value: String,
    pub label: String,
    pub selected: bool,
}

/// One setting on its page.
pub struct FieldView {
    pub key: String,
    pub label: String,
    /// `select`, `text` or `textarea`.
    pub input: &'static str,
    pub value: String,
    pub options: Vec<Opt>,
    /// The unit beside a number.
    pub unit: String,
    pub current: String,
    pub default: String,
    /// What `app.toml` writes, when it writes the key.
    pub file: Option<String>,
    pub version: i64,
    /// "by …, on …" when a value is saved.
    pub saved: Option<String>,
    /// Why the saved value could not be used at the last start.
    pub set_aside: Option<String>,
    /// `app.toml` changed under the saved value: its new value.
    pub file_notice: Option<String>,
    /// A restart key whose saved value is not in use yet: what is.
    pub running: Option<String>,
    /// The reset button's words, when a value is saved.
    pub reset: Option<String>,
}

/// Settings explained together: one card.
pub struct SectionView {
    /// The card's id, without the `s-` before it.
    pub slug: String,
    pub title: String,
    pub help: String,
    pub applies: &'static str,
    pub warning: Option<String>,
    pub fields: Vec<FieldView>,
}

/// Where a section renders.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Place {
    /// Its feature page.
    Page(PageId),
    /// Its old settings group page.
    Group(Group),
}

impl Place {
    /// The page's path.
    #[must_use]
    pub fn path(self) -> String {
        match self {
            Self::Page(id) => id.page().href.to_string(),
            Self::Group(g) => format!("/admin/settings/{}", g.slug()),
        }
    }

    /// The page's name.
    #[must_use]
    pub fn name(self) -> &'static str {
        match self {
            Self::Page(id) => id.page().name,
            Self::Group(g) => g.title(),
        }
    }
}

/// Where `section` renders: its feature page once that page holds its
/// settings, and always for a section with forms of its own; else its old
/// group page.
#[must_use]
pub fn place(section: &Section) -> Place {
    if section.own_form || SETTINGS_LIVE.contains(&section.home) { Place::Page(section.home) } else { Place::Group(section.group) }
}

/// The sections a settings block at `at` shows, in the registry's order.
/// Sections with forms of their own are never in a block.
#[must_use]
pub fn sections_at(at: Place, networks: &[String]) -> Vec<Section> {
    registry::sections(networks).into_iter().filter(|s| !s.own_form && place(s) == at).collect()
}

/// Where `key` renders, if it is an editable setting.
#[must_use]
pub fn place_of(key: &str, ctx: &SettingsContext) -> Option<Place> {
    registry::lookup(key, &ctx.networks()).map(|(s, _)| place(&s))
}

/// A link to where `key` is changed: its card on the page it renders on,
/// with the Advanced fold opened for a folded one; a key with forms of its
/// own, to its form on its page.
#[must_use]
pub fn href(key: &str, ctx: &SettingsContext) -> String {
    let Some((section, _)) = registry::lookup(key, &ctx.networks()) else {
        return "/admin/settings".to_string();
    };
    let at = place(&section);
    if section.own_form {
        let anchor = match key {
            "refract.enabled" => "#refract-switch",
            "refract.communities" => "#refract-communities",
            _ => "",
        };
        return format!("{}{anchor}", at.path());
    }
    let open = if section.advanced { "?open=advanced" } else { "" };
    format!("{}{open}#s-{}", at.path(), section.slug())
}

/// Every saved row, by key.
///
/// # Errors
///
/// Database errors.
pub async fn saved_rows(state: &AdminState) -> Result<BTreeMap<String, Row>, sqlx::Error> {
    Ok(store::rows(&state.pool).await?.into_iter().map(|r| (r.key.clone(), r)).collect())
}

/// The saved value of `key`, else the file's.
#[must_use]
pub fn saved_value(key: &str, rows: &BTreeMap<String, Row>, ctx: &SettingsContext) -> Value {
    rows.get(key).map_or_else(|| registry::value_at(&ctx.file_json, key), |r| r.value.clone())
}

/// Who saved a value, in words.
#[must_use]
pub fn by(label: &str) -> String {
    match label {
        "cli" => "the command line".to_string(),
        "file" => "app.toml".to_string(),
        other => other.to_string(),
    }
}

/// One field as its card shows it.
#[must_use]
pub fn field_view(section: &Section, f: &registry::Field, rows: &BTreeMap<String, Row>, ctx: &SettingsContext) -> FieldView {
    let rctx = ctx.registry();
    let row = rows.get(&f.key);
    let current = saved_value(&f.key, rows, ctx);
    let default = registry::value_at(&overlay::json(&crate::config::FileConfig::default()), &f.key);
    let file_explicit = overlay::has(&ctx.file_table, &f.key);
    let file_value = registry::value_at(&ctx.file_json, &f.key);
    let running = registry::value_at(&ctx.running_json, &f.key);
    let words = |v: &Value| registry::words(&f.kind, v, &rctx);
    let mut options: Vec<Opt> = registry::options(&f.kind, &rctx)
        .into_iter()
        .map(|(value, label)| Opt { selected: value == registry::form_text(&f.kind, &current), value, label })
        .collect();
    // A saved or file value the choices do not offer stays selectable.
    if registry::is_choice(&f.kind) && !options.iter().any(|o| o.selected) {
        let text = registry::form_text(&f.kind, &current);
        options.push(Opt { label: text.clone(), value: text, selected: true });
    }
    let unit = match &f.kind {
        Kind::Whole { unit, .. } | Kind::Decimal { unit, .. } => (*unit).to_string(),
        Kind::Gigabytes => "GB".to_string(),
        Kind::TimeOfDay => "UTC".to_string(),
        _ => String::new(),
    };
    let set_aside = row.and_then(|r| r.set_aside.as_ref()).map(|a| a.get("problem").and_then(Value::as_str).unwrap_or("a problem").to_string());
    let waiting = !section.live && set_aside.is_none() && !overlay::same(&current, &running);
    FieldView {
        key: f.key.clone(),
        label: f.label.clone(),
        input: if registry::is_choice(&f.kind) {
            "select"
        } else if f.kind.multiline() {
            "textarea"
        } else {
            "text"
        },
        value: registry::form_text(&f.kind, &current),
        options,
        unit,
        current: words(&current),
        default: words(&default),
        file: file_explicit.then(|| words(&file_value)),
        version: row.map_or(0, |r| r.version),
        saved: row.map(|r| format!("saved by {}, {}", by(&r.updated_by), when(r.updated_at))),
        set_aside,
        file_notice: row.filter(|r| r.file_notice).map(|r| {
            let file = r.file_value.as_ref().and_then(|f| f.get("value")).cloned().unwrap_or(Value::Null);
            words(&file)
        }),
        running: waiting.then(|| words(&running)),
        reset: row.map(|_| if file_explicit { format!("Use app.toml's value ({})", words(&file_value)) } else { format!("Use the default ({})", words(&default)) }),
    }
}

/// A warning a section needs: floors set on only some reads.
#[must_use]
pub fn section_warning(section: &Section, rows: &BTreeMap<String, Row>, ctx: &SettingsContext) -> Option<String> {
    let floors: Vec<i64> = section
        .fields
        .iter()
        .filter(|f| f.key.starts_with("private_read.floor_ms."))
        .map(|f| saved_value(&f.key, rows, ctx).as_i64().unwrap_or(0))
        .collect();
    let set = floors.iter().filter(|ms| **ms > 0).count();
    (set > 0 && set < floors.len()).then(|| {
        "Only some floors are set. The reads without one still answer as soon as they can, so their timing can still hint at what the floors hide.".to_string()
    })
}

/// One section as its card shows it.
#[must_use]
pub fn section_view(s: &Section, rows: &BTreeMap<String, Row>, ctx: &SettingsContext) -> SectionView {
    SectionView {
        slug: s.slug(),
        title: s.title.clone(),
        help: s.help.clone(),
        applies: if s.live { "Applies at once" } else { "Applies after a restart" },
        warning: section_warning(s, rows, ctx),
        fields: s.fields.iter().map(|f| field_view(s, f, rows, ctx)).collect(),
    }
}

/// A page's settings block: one form for every card, the everyday ones
/// first, then the Advanced fold.
pub struct SettingsBlock {
    pub csrf: String,
    pub everyday: Vec<SectionView>,
    pub advanced: Vec<SectionView>,
    /// Whether the Advanced fold is open.
    pub open: bool,
}

impl SettingsBlock {
    /// Every card, everyday first.
    #[must_use]
    pub fn cards(&self) -> Vec<&SectionView> {
        self.everyday.iter().chain(&self.advanced).collect()
    }
}

/// The settings block of `sections`. The Advanced fold is open when the
/// page was asked for it open, or when a folded setting has something to
/// say: a value set aside, a changed `app.toml`, or a value waiting for a
/// restart.
///
/// # Errors
///
/// Database errors.
pub async fn settings_block(state: &AdminState, csrf: &str, sections: &[Section], open_query: bool) -> Result<SettingsBlock, sqlx::Error> {
    let rows = saved_rows(state).await?;
    let ctx = &state.settings;
    let (advanced, everyday): (Vec<&Section>, Vec<&Section>) = sections.iter().partition(|s| s.advanced);
    let advanced: Vec<SectionView> = advanced.into_iter().map(|s| section_view(s, &rows, ctx)).collect();
    let open = open_query || advanced.iter().flat_map(|s| &s.fields).any(|f| f.set_aside.is_some() || f.file_notice.is_some() || f.running.is_some());
    Ok(SettingsBlock { csrf: csrf.to_string(), everyday: everyday.into_iter().map(|s| section_view(s, &rows, ctx)).collect(), advanced, open })
}
