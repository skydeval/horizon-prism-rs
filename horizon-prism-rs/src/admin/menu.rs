//! The admin's pages, their names and where they live: one source for the
//! sidebar, each page's `<title>`, eyebrow and h1, the redirects from old
//! addresses, and where Cancel and Back may go.
//!
//! Every page has a [`PageId`] and a [`PAGES`] row, built or not. Only the
//! pages in [`MENU`] (and Appearance) exist: they are routed, linked and
//! accepted as a way back. A page not built yet answers the plain 404 page.

use std::fmt::Write as _;

/// A page of the admin.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum PageId {
    Home,
    Members,
    Operators,
    LookUp,
    Labels,
    Blocks,
    Communities,
    ThreadLens,
    Indexing,
    Storage,
    Jobs,
    Problems,
    Private,
    Apps,
    Services,
    PublicPage,
    Relays,
    AccountLookups,
    Profiles,
    Limits,
    Health,
    Stats,
    About,
    Audit,
    Settings,
    Appearance,
}

/// A heading of the sidebar.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SectionKey {
    Overview,
    People,
    Moderation,
    Data,
    Private,
    Apps,
    Network,
    System,
}

/// A page: its address, its name, the product it belongs to, and the
/// sidebar heading it sits under (none for Appearance).
#[derive(Debug)]
pub struct Page {
    pub id: PageId,
    pub href: &'static str,
    pub name: &'static str,
    pub tag: Option<&'static str>,
    pub section: Option<SectionKey>,
}

/// The sidebar's headings in order: key (for element ids) and label.
pub const SECTIONS: &[(SectionKey, &str, &str)] = &[
    (SectionKey::Overview, "overview", "Overview"),
    (SectionKey::People, "people", "People"),
    (SectionKey::Moderation, "moderation", "Moderation"),
    (SectionKey::Data, "data", "Data"),
    (SectionKey::Private, "private", "Private posts"),
    (SectionKey::Apps, "apps", "Apps"),
    (SectionKey::Network, "network", "Network"),
    (SectionKey::System, "system", "System"),
];

const fn page(id: PageId, href: &'static str, name: &'static str, tag: Option<&'static str>, section: Option<SectionKey>) -> Page {
    Page { id, href, name, tag, section }
}

/// Every page, in the sidebar's order, Appearance last.
pub const PAGES: &[Page] = &[
    page(PageId::Home, "/admin", "Home", None, Some(SectionKey::Overview)),
    page(PageId::Members, "/admin/members", "Members", None, Some(SectionKey::People)),
    page(PageId::Operators, "/admin/operators", "Operators", None, Some(SectionKey::People)),
    page(PageId::LookUp, "/admin/lookup", "Look up", None, Some(SectionKey::People)),
    page(PageId::Labels, "/admin/labels", "Labels", None, Some(SectionKey::Moderation)),
    page(PageId::Blocks, "/admin/blocks", "Blocks", Some("Farsight"), Some(SectionKey::Moderation)),
    page(PageId::Communities, "/admin/communities", "Communities", Some("Refract"), Some(SectionKey::Moderation)),
    page(PageId::ThreadLens, "/admin/thread-lens", "Thread Lens", None, Some(SectionKey::Moderation)),
    page(PageId::Indexing, "/admin/indexing", "Indexing", None, Some(SectionKey::Data)),
    page(PageId::Storage, "/admin/storage", "Storage", None, Some(SectionKey::Data)),
    page(PageId::Jobs, "/admin/jobs", "Jobs", None, Some(SectionKey::Data)),
    page(PageId::Problems, "/admin/problems", "Problems", None, Some(SectionKey::Data)),
    page(PageId::Private, "/admin/private", "Private posts", Some("kryphocron"), Some(SectionKey::Private)),
    page(PageId::Apps, "/admin/apps", "What apps see", None, Some(SectionKey::Apps)),
    page(PageId::Services, "/admin/services", "Extra services", None, Some(SectionKey::Apps)),
    page(PageId::PublicPage, "/admin/public-page", "Public page", None, Some(SectionKey::Apps)),
    page(PageId::Relays, "/admin/relays", "Relays", None, Some(SectionKey::Network)),
    page(PageId::AccountLookups, "/admin/account-lookups", "Account lookups", Some("lodestar"), Some(SectionKey::Network)),
    page(PageId::Profiles, "/admin/profiles", "Profiles and caching", None, Some(SectionKey::Network)),
    page(PageId::Limits, "/admin/limits", "Request limits", None, Some(SectionKey::Network)),
    page(PageId::Health, "/admin/health", "Health", None, Some(SectionKey::System)),
    page(PageId::Stats, "/admin/stats", "Stats", None, Some(SectionKey::System)),
    page(PageId::About, "/admin/about", "About this server", None, Some(SectionKey::System)),
    page(PageId::Audit, "/admin/audit", "Audit log", None, Some(SectionKey::System)),
    page(PageId::Settings, "/admin/settings", "All settings", None, Some(SectionKey::System)),
    page(PageId::Appearance, "/admin/appearance", "Appearance", None, None),
];

/// The sidebar: the pages built so far, under their headings, in order. A
/// heading with no built page is not drawn.
pub const MENU: &[(SectionKey, &[PageId])] = &[
    (SectionKey::Overview, &[PageId::Home]),
    (SectionKey::People, &[PageId::Members, PageId::Operators, PageId::LookUp]),
    (SectionKey::Moderation, &[PageId::Labels, PageId::Blocks, PageId::Communities, PageId::ThreadLens]),
    (SectionKey::Data, &[PageId::Indexing, PageId::Storage, PageId::Jobs, PageId::Problems]),
    (SectionKey::Private, &[PageId::Private]),
    (SectionKey::Apps, &[PageId::Apps, PageId::Services, PageId::PublicPage]),
    (SectionKey::System, &[PageId::Health, PageId::Stats, PageId::About, PageId::Audit, PageId::Settings]),
];

/// The pages that hold their own settings now. Every other key still
/// renders on its old settings group page, apart from the Communities
/// page's own forms.
pub const SETTINGS_LIVE: &[PageId] = &[
    PageId::Members,
    PageId::Blocks,
    PageId::Communities,
    PageId::ThreadLens,
    PageId::Indexing,
    PageId::Storage,
    PageId::Private,
    PageId::Apps,
    PageId::Services,
    PageId::PublicPage,
];

/// A tab of Communities · Refract.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Tab {
    Communities,
    Accounts,
    Pending,
}

/// The tabs built so far, in order.
pub const TABS: &[Tab] = &[Tab::Communities, Tab::Accounts, Tab::Pending];

impl Tab {
    #[must_use]
    pub fn href(self) -> &'static str {
        match self {
            Self::Communities => "/admin/communities",
            Self::Accounts => "/admin/communities/accounts",
            Self::Pending => "/admin/communities/pending",
        }
    }

    #[must_use]
    pub fn name(self) -> &'static str {
        match self {
            Self::Communities => "Communities",
            Self::Accounts => "Accounts",
            Self::Pending => "Pending ends",
        }
    }

    /// Whether the tab exists now.
    #[must_use]
    pub fn built(self) -> bool {
        TABS.contains(&self)
    }

    /// Its `<title>` text: the page's title, with the tab's name before it
    /// unless the two share the name.
    #[must_use]
    pub fn title(self) -> String {
        let page = PageId::Communities.title();
        match self {
            Self::Communities => page,
            other => format!("{} · {page}", other.name()),
        }
    }
}

/// How a sidebar item is marked.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Current {
    /// Not the page shown.
    No,
    /// The page shown.
    Page,
    /// A sub-page of it is shown.
    Parent,
}

impl Current {
    /// The item's `aria-current` attribute, with its leading space.
    #[must_use]
    pub fn attr(self) -> &'static str {
        match self {
            Self::No => "",
            Self::Page => " aria-current=\"page\"",
            Self::Parent => " aria-current=\"true\"",
        }
    }
}

impl PageId {
    /// Its row of [`PAGES`].
    #[must_use]
    pub fn page(self) -> &'static Page {
        PAGES.iter().find(|p| p.id == self).unwrap_or(&PAGES[0])
    }

    /// Whether the page exists now: in [`MENU`], or Appearance.
    #[must_use]
    pub fn built(self) -> bool {
        self == Self::Appearance || MENU.iter().any(|(_, ids)| ids.contains(&self))
    }

    /// The label of the heading it sits under.
    #[must_use]
    pub fn section_label(self) -> Option<&'static str> {
        let key = self.page().section?;
        SECTIONS.iter().find(|(k, _, _)| *k == key).map(|(_, _, label)| *label)
    }

    /// Its name with its product: "Blocks · Farsight".
    #[must_use]
    pub fn title(self) -> String {
        let p = self.page();
        match p.tag {
            Some(tag) => format!("{} · {tag}", p.name),
            None => p.name.to_string(),
        }
    }

    /// The line above a page's h1: its heading, and its product.
    #[must_use]
    pub fn eyebrow(self) -> String {
        if self == Self::Appearance {
            return "Your account".to_string();
        }
        let label = self.section_label().unwrap_or_default();
        match self.page().tag {
            Some(tag) => format!("{label} · {tag}"),
            None => label.to_string(),
        }
    }

    /// The built page whose href is `path` exactly.
    #[must_use]
    pub fn at(path: &str) -> Option<Self> {
        PAGES.iter().find(|p| p.href == path && p.id.built()).map(|p| p.id)
    }
}

/// The settings groups' old page slugs, `/admin/settings/{slug}`.
pub const OLD_GROUP_SLUGS: &[&str] = &["access", "storage", "private", "blocks", "apps", "services", "limits", "profiles", "lookups"];

/// Old addresses that answer `308` with a new one: the old path, the new
/// path, and the fragment added after the query.
pub const REDIRECTS: &[(&str, &str, &str)] = &[
    ("/admin/farsight", "/admin/blocks", ""),
    ("/admin/refract", "/admin/communities/accounts", ""),
    ("/admin/instance", "/admin/about", ""),
    ("/admin/settings/appearance", "/admin/appearance", ""),
    ("/admin/activity", "/admin/audit", ""),
];

/// Old settings group pages with no settings left on them, `/admin/settings/{slug}`:
/// the slug, the page holding most of what was there, and the fragment.
/// They answer `308` to a GET; a POST to one of the old slugs still saves.
pub const GROUP_REDIRECTS: &[(&str, &str, &str)] = &[
    ("communities", "/admin/communities", ""),
    ("blocks", "/admin/blocks", "settings"),
    ("access", "/admin/members", "settings"),
    ("storage", "/admin/storage", "settings"),
    ("private", "/admin/private", "settings"),
    ("apps", "/admin/apps", ""),
    ("services", "/admin/services", ""),
];

/// Where an old group slug's GET now goes, once its page has no settings
/// left.
#[must_use]
pub fn group_redirect(slug: &str) -> Option<(&'static str, &'static str)> {
    GROUP_REDIRECTS.iter().find(|(s, _, _)| *s == slug).map(|(_, to, fragment)| (*to, *fragment))
}

/// The old account pages, `/admin/refract/{did}`, redirected to
/// [`account_href`].
pub const OLD_ACCOUNT_PREFIX: &str = "/admin/refract/";

/// A DID with each `%` written `%25`: every other character of a valid DID
/// stands for itself in a path segment and a query value, but a `%` would
/// be decoded a second time.
fn escape_did(did: &str) -> String {
    did.replace('%', "%25")
}

/// An account's page on the Accounts tab.
#[must_use]
pub fn account_href(did: &str) -> String {
    format!("/admin/communities/accounts/{}", escape_did(did))
}

/// Look up's page for a DID.
#[must_use]
pub fn lookup_href(did: &str) -> String {
    format!("/admin/lookup?q={}", escape_did(did))
}

/// `%XX` sequences decoded; `None` when one is malformed or the bytes are
/// not UTF-8.
#[must_use]
pub fn percent_decode(s: &str) -> Option<String> {
    let bytes = s.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' {
            let hex = s.get(i + 1..i + 3)?;
            out.push(u8::from_str_radix(hex, 16).ok()?);
            i += 3;
        } else {
            out.push(bytes[i]);
            i += 1;
        }
    }
    String::from_utf8(out).ok()
}

/// Where a path is in the sidebar: the built page it is, or the page whose
/// sub-page it is, and the Communities tab it names. `None` for a path that
/// is neither.
#[must_use]
pub fn locate(path: &str) -> Option<(PageId, Current, Option<Tab>)> {
    let path = path.split_once('?').map_or(path, |(p, _)| p);
    if let Some(tab) = TABS.iter().copied().find(|t| t.href() == path) {
        let mark = if tab == Tab::Communities { Current::Page } else { Current::Parent };
        return Some((PageId::Communities, mark, Some(tab)));
    }
    if let Some(id) = PageId::at(path) {
        return Some((id, Current::Page, None));
    }
    if path.starts_with("/admin/communities/accounts/") {
        return Some((PageId::Communities, Current::Parent, Some(Tab::Accounts)));
    }
    if path.starts_with("/admin/commands/") {
        return Some((PageId::Audit, Current::Parent, None));
    }
    if path.starts_with("/admin/settings/") {
        return Some((PageId::Settings, Current::Parent, None));
    }
    None
}

/// Whether `back` may be where Cancel and Back go: a page of this admin
/// that exists now, written as a plain relative path, with no way to step
/// out of `/admin` or to another site.
#[must_use]
pub fn safe_back(back: &str) -> bool {
    if back.len() > 512 || !back.starts_with("/admin") {
        return false;
    }
    if !matches!(back.as_bytes().get(6), None | Some(b'/' | b'?')) {
        return false;
    }
    if back.contains("//") || back.contains('\\') || back.contains('#') || back.bytes().any(|b| b.is_ascii_control() || b == b' ') {
        return false;
    }
    let lower = back.to_ascii_lowercase();
    if lower.contains("%2e") || lower.contains("%2f") {
        return false;
    }
    let path = back.split_once('?').map_or(back, |(p, _)| p);
    if path.split('/').any(|seg| seg == ".." || seg == ".") {
        return false;
    }
    if PageId::at(path).is_some() || TABS.iter().any(|t| t.href() == path) {
        return true;
    }
    if let Some(did) = path.strip_prefix("/admin/communities/accounts/") {
        return percent_decode(did).is_some_and(|d| proto_blue::syntax::Did::is_valid(&d));
    }
    if let Some(id) = path.strip_prefix("/admin/commands/") {
        return !id.is_empty() && id.bytes().all(|b| b.is_ascii_digit());
    }
    if let Some(slug) = path.strip_prefix("/admin/settings/") {
        return matches!(slug, "installed" | "running") || (OLD_GROUP_SLUGS.contains(&slug) && group_redirect(slug).is_none());
    }
    false
}

/// A redirect's `Location`: the new path, the request's own query, then the
/// fragment.
#[must_use]
pub fn location(to: &str, query: Option<&str>, fragment: &str) -> String {
    let mut out = to.to_string();
    if let Some(q) = query.filter(|q| !q.is_empty()) {
        let _ = write!(out, "?{q}");
    }
    if !fragment.is_empty() {
        let _ = write!(out, "#{fragment}");
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    const ALL: [PageId; 26] = [
        PageId::Home,
        PageId::Members,
        PageId::Operators,
        PageId::LookUp,
        PageId::Labels,
        PageId::Blocks,
        PageId::Communities,
        PageId::ThreadLens,
        PageId::Indexing,
        PageId::Storage,
        PageId::Jobs,
        PageId::Problems,
        PageId::Private,
        PageId::Apps,
        PageId::Services,
        PageId::PublicPage,
        PageId::Relays,
        PageId::AccountLookups,
        PageId::Profiles,
        PageId::Limits,
        PageId::Health,
        PageId::Stats,
        PageId::About,
        PageId::Audit,
        PageId::Settings,
        PageId::Appearance,
    ];

    /// Every page has one row, in the sidebar's order, and no two hrefs
    /// differ by one letter.
    #[test]
    fn every_page_has_one_row() {
        assert_eq!(PAGES.len(), ALL.len());
        for (row, id) in PAGES.iter().zip(ALL) {
            assert_eq!(row.id, id);
            assert_eq!(id.page().id, id);
            assert!(row.href.starts_with("/admin"), "{}", row.href);
        }
        assert_eq!(PageId::Appearance.page().section, None);
        for a in PAGES {
            for b in PAGES {
                if a.id != b.id {
                    assert_ne!(a.href, b.href);
                    let (x, y) = (a.href.trim_end_matches('s'), b.href.trim_end_matches('s'));
                    assert_ne!(x, y, "{} and {}", a.href, b.href);
                }
            }
        }
        // The menu follows the pages' order.
        let menu: Vec<PageId> = MENU.iter().flat_map(|(_, ids)| ids.iter().copied()).collect();
        let order: Vec<PageId> = ALL.iter().copied().filter(|id| menu.contains(id)).collect();
        assert_eq!(menu, order);
        for (key, ids) in MENU {
            for id in *ids {
                assert_eq!(id.page().section, Some(*key), "{id:?}");
            }
        }
        assert_eq!(menu.len(), 21);
    }

    #[test]
    fn names_titles_and_eyebrows() {
        assert_eq!(PageId::Blocks.title(), "Blocks · Farsight");
        assert_eq!(PageId::Blocks.eyebrow(), "Moderation · Farsight");
        assert_eq!(PageId::Members.title(), "Members");
        assert_eq!(PageId::Private.eyebrow(), "Private posts · kryphocron");
        assert_eq!(PageId::Settings.eyebrow(), "System");
        assert_eq!(PageId::Appearance.eyebrow(), "Your account");
        assert_eq!(Tab::Communities.title(), "Communities · Refract");
        assert_eq!(Tab::Accounts.title(), "Accounts · Communities · Refract");
        assert!(PageId::Blocks.built() && PageId::Appearance.built() && PageId::Storage.built() && PageId::Private.built() && PageId::PublicPage.built() && !PageId::Relays.built());
        assert_eq!(PageId::Private.title(), "Private posts · kryphocron");
        assert!(Tab::Accounts.built() && Tab::Pending.built() && PageId::ThreadLens.built());
        assert_eq!(Tab::Pending.title(), "Pending ends · Communities · Refract");
        // A page holds its settings only once it is built.
        assert!(SETTINGS_LIVE.iter().all(|id| id.built()));
    }

    #[test]
    fn dids_keep_their_percent() {
        assert_eq!(account_href("did:plc:abc"), "/admin/communities/accounts/did:plc:abc");
        assert_eq!(account_href("did:web:h%3A8080"), "/admin/communities/accounts/did:web:h%253A8080");
        assert_eq!(lookup_href("did:web:h%3A8080"), "/admin/lookup?q=did:web:h%253A8080");
        assert_eq!(lookup_href("did:plc:bbbb"), "/admin/lookup?q=did:plc:bbbb");
        assert_eq!(percent_decode("did%3Aplc%3Aabc").as_deref(), Some("did:plc:abc"));
        assert_eq!(percent_decode("did:web:%FF"), None);
        assert_eq!(percent_decode("%2"), None);
    }

    #[test]
    fn where_a_path_sits() {
        assert_eq!(locate("/admin/members?page=2"), Some((PageId::Members, Current::Page, None)));
        assert_eq!(locate("/admin/communities"), Some((PageId::Communities, Current::Page, Some(Tab::Communities))));
        assert_eq!(locate("/admin/communities/accounts?state=restored"), Some((PageId::Communities, Current::Parent, Some(Tab::Accounts))));
        assert_eq!(locate("/admin/communities/accounts/did:plc:abc"), Some((PageId::Communities, Current::Parent, Some(Tab::Accounts))));
        assert_eq!(locate("/admin/communities/pending"), Some((PageId::Communities, Current::Parent, Some(Tab::Pending))));
        assert_eq!(locate("/admin/thread-lens"), Some((PageId::ThreadLens, Current::Page, None)));
        assert_eq!(locate("/admin/commands/12"), Some((PageId::Audit, Current::Parent, None)));
        assert_eq!(locate("/admin/settings/running"), Some((PageId::Settings, Current::Parent, None)));
        assert_eq!(locate("/admin/appearance"), Some((PageId::Appearance, Current::Page, None)));
        assert_eq!(locate("/admin/storage"), Some((PageId::Storage, Current::Page, None)));
        assert_eq!(locate("/admin/private"), Some((PageId::Private, Current::Page, None)));
        assert_eq!(locate("/admin/apps"), Some((PageId::Apps, Current::Page, None)));
        assert_eq!(locate("/admin/public-page"), Some((PageId::PublicPage, Current::Page, None)));
        assert_eq!(locate("/admin/relays"), None);
    }

    /// Cancel goes only to a page of this admin that exists now.
    #[test]
    fn safe_back_accepts_only_pages() {
        for ok in [
            "/admin",
            "/admin?x=1",
            "/admin/members?page=2",
            "/admin/communities/accounts/did:plc:aaaaaaaaaaaaaaaaaaaaaaaa",
            "/admin/communities/accounts?state=restored",
            "/admin/commands/12",
            "/admin/settings/running",
            "/admin/settings/limits",
            "/admin/apps",
            "/admin/services",
            "/admin/public-page?open=advanced",
            "/admin/appearance",
            "/admin/communities/pending",
            "/admin/thread-lens",
            "/admin/indexing",
            "/admin/storage?open=advanced",
            "/admin/private?open=advanced",
        ] {
            assert!(safe_back(ok), "{ok}");
        }
        let long = format!("/admin/members?{}", "a".repeat(600));
        for bad in [
            "//evil.example",
            "/\\evil",
            "/admin@evil.example",
            "/adminx",
            "/admin/../x",
            "/admin/%2e%2e/x",
            "/admin/%2E%2E/x",
            "https://evil.example/admin",
            "javascript:alert(1)",
            "/admin/members\r\nx",
            long.as_str(),
            "/admin/members/remove",
            "/admin/communities/accounts/..",
            "/admin/communities/accounts/not-a-did",
            "/admin/commands/12a",
            "/admin/commands/",
            "/admin/relays",
            "/admin/settings/apps",
            "/admin/settings/services",
            "/admin/settings/private",
            "/admin/settings/storage",
            "/admin/settings/access",
            "/admin/settings/blocks",
            "/admin/settings/communities",
            "/admin/settings/nowhere",
            "/admin/members#x",
            "/admin//members",
            "",
        ] {
            assert!(!safe_back(bad), "{bad:?}");
        }
    }

    #[test]
    fn locations_keep_the_query() {
        assert_eq!(location("/admin/blocks", None, ""), "/admin/blocks");
        assert_eq!(location("/admin/blocks", Some(""), "settings"), "/admin/blocks#settings");
        assert_eq!(location("/admin/appearance", Some("saved=1"), ""), "/admin/appearance?saved=1");
        for (old, new, _) in REDIRECTS {
            assert!(PageId::at(old).is_none() && !TABS.iter().any(|t| t.href() == *old), "{old} is a page");
            assert!(PageId::at(new).is_some() || TABS.iter().any(|t| t.href() == *new), "{new} is not a built page");
        }
        for (slug, new, _) in GROUP_REDIRECTS {
            assert!(OLD_GROUP_SLUGS.contains(slug) || *slug == "communities", "{slug}");
            assert!(PageId::at(new).is_some(), "{new} is not a built page");
        }
        assert_eq!(group_redirect("blocks"), Some(("/admin/blocks", "settings")));
        assert_eq!(group_redirect("storage"), Some(("/admin/storage", "settings")));
        assert_eq!(group_redirect("private"), Some(("/admin/private", "settings")));
        assert_eq!(group_redirect("apps"), Some(("/admin/apps", "")));
        assert_eq!(group_redirect("services"), Some(("/admin/services", "")));
        assert_eq!(group_redirect("limits"), None);
    }
}
