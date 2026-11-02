//! The signed-in frame over HTTP and in the stylesheet: the sidebar (its
//! markup and every rule of its look), each page's name, eyebrow and title,
//! the marks of sub-pages, the redirects from old addresses and the forms'
//! old paths, where Cancel goes, the docs' page table, and every table's
//! box and cell classes.

use std::path::Path as FsPath;

use super::*;
use crate::admin::menu::{self, GROUP_REDIRECTS, MENU, PAGES, PageId, REDIRECTS, TABS};
use crate::admin::test_support::css::{self, Rule};
use crate::admin::test_support::html::{self, Table};
use crate::admin::test_support::{current_item, sidebar};

/// A page past the ten-pages-a-second limit.
async fn get(b: &Browser, url: &str) -> Page {
    tokio::time::sleep(Duration::from_millis(120)).await;
    b.get(url).await
}

/// A change past the one-a-second limit.
async fn post(b: &Browser, url: &str, form: &[(&str, &str)]) -> Page {
    tokio::time::sleep(Duration::from_millis(1100)).await;
    b.post(url, form).await
}

/// A change with a `Referer`.
async fn post_from(b: &Browser, url: &str, form: &[(&str, &str)], referer: &str) -> Page {
    tokio::time::sleep(Duration::from_millis(1100)).await;
    b.send(b.http.post(url).header("referer", referer).form(form)).await
}

fn collapse(s: &str) -> String {
    s.split_whitespace().collect::<Vec<_>>().join(" ")
}

fn location(page: &Page) -> Option<&str> {
    page.headers.get(LOCATION).and_then(|v| v.to_str().ok())
}

/// Each signed-in page in the menu: its item alone marked as the page,
/// its h1 its name, its eyebrow its heading and product, its title its
/// name with its product.
#[tokio::test]
async fn every_menu_item_renders() {
    let Some(h) = harness("admin_frame_menu").await else {
        return;
    };
    let alice = complete_setup(&h, ALICE).await;
    for (_, ids) in MENU {
        for id in *ids {
            let p = id.page();
            let page = get(&alice, &format!("{}{}", h.base, p.href)).await;
            assert_eq!(page.status, 200, "{}: {}", p.href, page.body);
            assert_eq!(current_item(&page.body), Some((p.name.to_string(), "page".to_string())), "{}", p.href);
            assert!(page.body.contains(&format!("<h1>{}</h1>", p.name)), "{}: {}", p.href, page.body);
            assert!(page.body.contains(&format!("<p class=\"eyebrow\">{}</p>", id.eyebrow())), "{}: {}", p.href, page.body);
            assert!(page.body.contains(&format!("<title>{} · Horizon-Prism admin</title>", id.title())), "{}: {}", p.href, page.body);
        }
    }
    // A tag makes the title differ from the h1.
    let blocks = get(&alice, &format!("{}/admin/blocks", h.base)).await;
    assert!(blocks.body.contains("<title>Blocks · Farsight · Horizon-Prism admin</title>") && blocks.body.contains("<h1>Blocks</h1>"), "{}", blocks.body);
    // The tabs: each h1 "Communities", their own titles, one tab marked.
    for (path, title, current) in [
        ("/admin/communities", "Communities · Refract", "Communities"),
        ("/admin/communities/accounts", "Accounts · Communities · Refract", "Accounts"),
        ("/admin/communities/pending", "Pending ends · Communities · Refract", "Pending ends"),
    ] {
        let page = get(&alice, &format!("{}{path}", h.base)).await;
        assert_eq!(page.status, 200, "{path}");
        assert!(page.body.contains("<h1>Communities</h1>") && page.body.contains(&format!("<title>{title} · Horizon-Prism admin</title>")), "{path}: {}", page.body);
        assert!(page.body.contains("<p class=\"eyebrow\">Moderation · Refract</p>"), "{path}");
        let tabs = &page.body[page.body.find("<nav class=\"tabs\" aria-label=\"Communities · Refract\">").expect("tabs")..];
        let tabs = &tabs[..tabs.find("</nav>").expect("tabs end")];
        assert_eq!(tabs.matches("aria-current").count(), 1, "{tabs}");
        assert!(tabs.contains(&format!("aria-current=\"page\">{current}</a>")), "{tabs}");
    }
    h.db.drop_db().await;
}

/// A heading of the sidebar: its key, label and items (href, name, tag).
type Section = (&'static str, &'static str, &'static [(&'static str, &'static str, Option<&'static str>)]);

/// The sidebar as Home draws it, item by item: the brand as text, the
/// name's details with Appearance and Sign out, each heading labelling its
/// list, each item a link with its tag.
#[tokio::test]
async fn the_sidebar_markup_is_the_mocks() {
    let Some(h) = harness("admin_frame_sidebar").await else {
        return;
    };
    let alice = complete_setup(&h, ALICE).await;
    let home = get(&alice, &format!("{}/admin", h.base)).await;
    let csrf = csrf_of(&home);
    let sections: &[Section] = &[
        ("overview", "Overview", &[("/admin", "Home", None)]),
        ("people", "People", &[("/admin/members", "Members", None), ("/admin/operators", "Operators", None), ("/admin/lookup", "Look up", None)]),
        (
            "moderation",
            "Moderation",
            &[
                ("/admin/labels", "Labels", None),
                ("/admin/blocks", "Blocks", Some("Farsight")),
                ("/admin/communities", "Communities", Some("Refract")),
                ("/admin/thread-lens", "Thread Lens", None),
            ],
        ),
        ("data", "Data", &[("/admin/jobs", "Jobs", None), ("/admin/problems", "Problems", None)]),
        (
            "system",
            "System",
            &[
                ("/admin/health", "Health", None),
                ("/admin/stats", "Stats", None),
                ("/admin/about", "About this server", None),
                ("/admin/audit", "Audit log", None),
                ("/admin/settings", "All settings", None),
            ],
        ),
    ];
    let mut expected = format!(
        "<nav class=\"sidebar\" aria-label=\"Admin menu\">\n<div class=\"brand\"><span>Horizon-Prism</span>\n\
         <details class=\"who\"><summary><span class=\"who-name\">{ALICE}</span> <span aria-hidden=\"true\">▾</span></summary>\n\
         <div class=\"who-menu\">\n<a class=\"item\" href=\"/admin/appearance\">Appearance</a>\n\
         <form method=\"post\" action=\"/admin/logout\"><input type=\"hidden\" name=\"csrf\" value=\"{csrf}\"><button class=\"item\" type=\"submit\">Sign out</button></form>\n\
         </div>\n</details>\n</div>\n"
    );
    for (key, label, items) in sections {
        expected.push_str(&format!("<p class=\"sec\" id=\"sec-{key}\">{label}</p>\n<ul aria-labelledby=\"sec-{key}\">\n"));
        for (href, name, tag) in *items {
            let current = if *href == "/admin" { " aria-current=\"page\"" } else { "" };
            let tag = tag.map_or(String::new(), |t| format!("<span class=\"tag\">{t}</span>"));
            expected.push_str(&format!("<li><a class=\"item\" href=\"{href}\"{current}><span>{name}</span>{tag}</a></li>\n"));
        }
        expected.push_str("</ul>\n");
    }
    expected.push_str("</nav>");
    assert_eq!(collapse(sidebar(&home.body)), collapse(&expected));
    assert_eq!(sidebar(&home.body).matches('▾').count(), 1);
    assert!(!sidebar(&home.body).contains("<svg"));
    // The skip link is the first element of the body, to main#main.
    let body = &home.body[home.body.find("<body>").expect("body") + "<body>".len()..];
    assert!(body.trim_start().starts_with("<a class=\"skip\" href=\"#main\">Skip to content</a>"), "{body}");
    assert!(home.body.contains("<main id=\"main\">"), "{}", home.body);
    h.db.drop_db().await;
}

/// One rule as the stylesheet must hold it.
fn rule(context: &str, selectors: &str, declarations: &str) -> Rule {
    Rule { context: context.to_string(), selectors: selectors.to_string(), declarations: css::declarations(declarations) }
}

const NARROW: &str = "@media (max-width: 820px)";

/// Every rule of the sidebar's look and of the wide column and its tables.
fn pinned() -> Vec<Rule> {
    vec![
        rule("", ".layout", "display: grid; grid-template-columns: 290px minmax(0, 1fr); gap: 28px; align-items: start; padding: 16px max(16px, env(safe-area-inset-right, 0px)) 48px max(16px, env(safe-area-inset-left, 0px))"),
        rule("", ".layout > main", "max-width: none; margin: 0; padding: 0"),
        rule("", ".sidebar", "background: var(--side-bg); color: var(--side-fg); border-radius: 12px; padding: 18px 0 14px; font-size: 15.2px; line-height: 1.55; position: sticky; top: calc(env(safe-area-inset-top, 0px) + 16px); max-height: calc(100vh - 32px); overflow-y: auto"),
        rule("", ".sidebar .brand", "padding: 0 18px 12px; font-weight: 700; letter-spacing: .02em; border-bottom: 1px solid var(--side-line); margin-bottom: 8px; display: flex; flex-wrap: wrap; justify-content: space-between; align-items: baseline; gap: 8px; position: relative"),
        rule("", ".sidebar .who", "min-width: 0"),
        rule("", ".sidebar .who summary", "display: block; list-style: none; cursor: pointer; color: var(--side-muted); font-weight: 400; font-size: 12.48px; letter-spacing: normal; text-align: right; overflow-wrap: anywhere"),
        rule("", ".sidebar .who summary::-webkit-details-marker", "display: none"),
        rule("", ".sidebar .who summary:focus-visible", "outline: 2px solid var(--side-accent); outline-offset: 2px; border-radius: 4px"),
        rule("", ".sidebar .who-menu", "position: absolute; right: 10px; top: calc(100% - 4px); z-index: 2; min-width: 180px; padding: 4px 0; background: var(--side-bg); border: 1px solid var(--side-line); border-radius: 8px; box-shadow: 0 8px 24px rgba(0, 0, 0, .35); font-size: 15.2px; font-weight: 400; letter-spacing: normal; text-align: left"),
        rule("", ".sidebar .who-menu form", "margin: 0"),
        rule("", ".sidebar .who-menu .item", "padding: 6px 14px"),
        rule("", ".sidebar .sec", "margin: 0; padding: 10px 18px 2px; font-size: 11.52px; letter-spacing: .1em; text-transform: uppercase; color: var(--side-muted)"),
        rule("", ".sidebar ul", "list-style: none; margin: 0; padding: 0"),
        rule("", ".sidebar .item", "all: unset; box-sizing: border-box; display: flex; justify-content: space-between; align-items: center; gap: 8px; width: 100%; padding: 6px 18px 6px 26px; cursor: pointer; color: var(--side-fg); text-decoration: none"),
        rule("", ".sidebar .item:hover, .sidebar .item:focus-visible", "background: var(--side-hover); text-decoration: none"),
        rule("", ".sidebar .item:focus-visible", "outline: 2px solid var(--side-accent); outline-offset: -2px"),
        rule("", ".sidebar .item[aria-current=\"page\"], .sidebar .item[aria-current=\"true\"]", "background: var(--side-current); box-shadow: inset 3px 0 0 var(--side-accent)"),
        rule("", ".sidebar .tag", "font-family: var(--font-mono); font-size: 11.2px; color: var(--side-accent); white-space: nowrap"),
        rule(NARROW, ".layout", "grid-template-columns: minmax(0, 1fr)"),
        rule(NARROW, ".sidebar", "position: static; max-height: none; overflow: visible"),
        rule("", ".skip", "position: absolute; left: 16px; top: -100px; z-index: 100; padding: 8px 12px; border-radius: 6px; background: var(--card); color: var(--accent); font-weight: 600"),
        rule("", ".skip:focus", "top: 16px"),
        rule("", ".eyebrow", "margin: 0; font-size: 12.48px; letter-spacing: .08em; text-transform: uppercase; color: var(--accent); font-weight: 700"),
        rule("", ".layout nav.tabs", "display: flex; flex-wrap: wrap; gap: 0.25rem; margin: 0 0 1.25rem; border-bottom: 1px solid var(--line)"),
        rule("", ".layout nav.tabs a", "display: inline-flex; align-items: center; gap: 0.4rem; padding: 0.45rem 0.85rem; margin-bottom: -1px; border-bottom: 2px solid transparent; color: var(--muted); font-weight: 600; font-size: 0.92rem"),
        rule("", ".layout nav.tabs a:hover", "color: var(--accent); text-decoration: none"),
        rule("", ".layout nav.tabs a[aria-current=\"page\"]", "color: var(--accent); border-bottom-color: var(--accent)"),
        rule("@media (max-width: 1100px)", ".layout .home-row", "grid-template-columns: minmax(0, 1fr); gap: 0"),
        rule("", ".layout main p, .layout main .banner", "max-width: 70ch"),
        rule("", ".layout .stat, .layout .fact", "max-width: 48rem"),
        rule("", ".layout th, .layout td", "overflow-wrap: normal; word-break: normal"),
        rule("", ".layout th", "white-space: nowrap"),
        rule("", ".layout th.num, .layout td.num", "white-space: nowrap; text-align: right; font-variant-numeric: tabular-nums"),
        rule("", ".layout td.when", "white-space: nowrap; font-variant-numeric: tabular-nums"),
        rule("", ".layout td.name, .layout td.state", "white-space: nowrap"),
        rule("", ".layout td.actions", "white-space: nowrap; text-align: right"),
        rule("", ".layout td.list", "min-width: 12rem"),
        rule("", ".layout td.text", "overflow-wrap: anywhere"),
        rule("", ".layout td.text > .cell-text", "min-width: 16rem"),
        rule("", ".layout td .wrap", "white-space: normal"),
        rule("", ".layout .settings-cards", "display: grid; grid-template-columns: repeat(auto-fit, minmax(min(100%, 36rem), 1fr)); gap: 1.25rem; align-items: start"),
        rule("", ".layout .settings-cards > .card", "margin: 0"),
        rule("", ".layout details.advanced", "margin: 1.25rem 0"),
        rule("", ".layout details.advanced > summary", "font-weight: 600; font-size: 0.92rem; margin-bottom: 0.75rem"),
    ]
}

/// Every selector of the stylesheet before the sidebar.
const OLD_SELECTORS: &[&str] = &[
    ":root",
    ".scheme-sky",
    ".scheme-violet",
    ".scheme-teal",
    ".scheme-graphite",
    ".swatch",
    ":root:not(.mode-light)",
    ":root:not(.mode-light) .swatch",
    ":root.mode-dark",
    ":root.mode-dark .swatch",
    "*",
    "body",
    "a",
    "a:hover",
    "a:focus-visible",
    "button:focus-visible",
    "input:focus-visible",
    "select:focus-visible",
    "textarea:focus-visible",
    "header.top",
    "header.top .brand",
    "header.top .brand svg",
    "header.top nav",
    "header.top nav a",
    "header.top nav a:hover",
    r#"header.top nav a[aria-current="page"]"#,
    "header.top .bar-end",
    "header.top .who",
    "main",
    "h1",
    "h2",
    "h3",
    ".card",
    ".card h2:first-child",
    ".grid",
    ".stat",
    ".fact",
    ".stat:last-of-type",
    ".fact:last-of-type",
    ".stat .l",
    ".fact .l",
    ".stat .n",
    ".fact .v",
    ".muted",
    ".ok",
    ".bad",
    ".tech",
    ".banner",
    ".banner.bad",
    ".banner.ok",
    ".scroll",
    "table",
    "th",
    "td",
    "tr:last-child td",
    "tbody tr:hover",
    "td.actions",
    "main.wide",
    "main.wide > p",
    "main.wide > .banner",
    "table.dense th",
    "table.dense td",
    "table.dense td.actions",
    "table.dense td.actions form.inline + form.inline",
    "table.dense td.actions .button + form.inline",
    ".nowrap",
    "code.chip",
    ".chips",
    ".chips code.chip",
    ".counts",
    ".counts a",
    ".counts span",
    ".counts .zero",
    "code",
    "pre",
    "details summary",
    "form.inline",
    "form.stack label",
    "form.stack label:first-of-type",
    "form.stack .hint",
    "form.filters",
    "form.filters input",
    "form.filters select",
    "input[type=text]",
    "select",
    "textarea",
    "input[type=text]:focus",
    "select:focus",
    "textarea:focus",
    "button",
    ".button",
    "button:hover",
    ".button:hover",
    "button.secondary",
    ".button.secondary",
    "button.secondary:hover",
    ".button.secondary:hover",
    "button.danger",
    "td.actions button",
    ".row",
    "[hidden]",
    ".asof",
    ".pill",
    ".pill.on",
    ".pill.off",
    ".pill.red",
    ".pill.amber",
    ".pill.info",
    ".pill.fine",
    ".pill.nodata",
    ".attention",
    ".attention li",
    ".attention li:last-child",
    ".attention .pill",
    ".cmd",
    ".home-row",
    ".home-row > .live",
    ".live .grid",
    ".live-head",
    ".live-head h2",
    ".live-head .pill",
    ".help-btn",
    ".help-btn:hover",
    r#".help-btn[aria-expanded="true"]"#,
    ".live-help",
    ".live-help p",
    ".live-help p:last-child",
    ".live-controls",
    ".live-controls select",
    "#live-pause",
    ".live-since",
    "#live-rates h3",
    "#live-rates .fact",
    "#live-rates .fact .l",
    "#live-rates .fact .v",
    "#live-rates .fact.blank",
    "#tiles .stat",
    "#tiles .stat .l",
    ".live-tickers",
    ".live-tickers h3",
    ".live-list",
    ".live-list li",
    ".live-list li > span",
    ".live-list .t",
    ".live-list .src",
    ".live-list .k",
    ".live-list .o",
    ".live-list .o.ix",
    ".live-list li.dim",
    ".live-list li > .who",
    ".live-list li > .text",
    ".live-list li.enter",
    "#live-notstarted",
    ".pub-live",
    ".pub-live-side .fact .l",
    ".pub-live-side .fact .v",
    ".pub-live-main",
    ".pub-live-head",
    ".pub-live-list",
    ".pub-live-list li",
    ".pub-live-list li > span",
    ".pub-live-list li > span:nth-child(1)",
    ".pub-live-list li > span:nth-child(3)",
    ".pub-live-list li > span:nth-child(4)",
    "#live-since",
    "#live-off",
    "details.more",
    "details.more .menu",
    "td.actions .button",
    ".labels-help .fact .l",
    ".labels-help .fact .v",
    "fieldset.choices",
    "fieldset.choices legend",
    "label.choice",
    "label.choice input",
];

const DARK: &str = "@media (prefers-color-scheme: dark)";

/// A custom property's value in a rule's declarations.
fn token(rules: &[Rule], context: &str, selectors: &str, name: &str) -> Option<String> {
    let r = rules.iter().find(|r| r.context == context && r.selectors == selectors)?;
    r.declarations.split("; ").find_map(|d| d.trim_end_matches(';').strip_prefix(&format!("{name}: ")).map(str::to_string))
}

/// The sidebar's look, rule by rule: each pinned rule once with exactly its
/// declarations; the colour tokens in their blocks; the old top-bar rules
/// gone; and every new selector scoped to the signed-in frame.
#[test]
fn the_sidebar_css_is_the_mocks() {
    let rules = css::rules(CSS);
    for want in pinned() {
        let found: Vec<&Rule> = rules.iter().filter(|r| r.context == want.context && r.selectors == want.selectors).collect();
        assert_eq!(found.len(), 1, "{} {{{}}} occurs {} times", want.context, want.selectors, found.len());
        assert_eq!(found[0].declarations, want.declarations, "{} {}", want.context, want.selectors);
    }
    // The column has no measure of its own, and nothing undoes the floors.
    let layout = rules.iter().find(|r| r.context.is_empty() && r.selectors == ".layout").expect(".layout");
    assert!(!layout.declarations.contains("max-width"));
    assert!(!rules.iter().any(|r| r.selectors.contains(".layout.wide") || r.selectors.contains(".layout .grid > *")));
    for s in [".layout > main", ".layout td.text", ".layout .settings-cards > .card"] {
        assert!(rules.iter().filter(|r| r.selectors == s).all(|r| !r.declarations.contains("min-width")), "{s}");
    }
    // Home's 48rem rule is gone; the public page's stays.
    assert!(!rules.iter().any(|r| r.context == "@media (max-width: 48rem)" && r.selectors == ".home-row"));
    assert_eq!(rules.iter().filter(|r| r.context == "@media (max-width: 48rem)").count(), 1);
    // The panel's colours: three in each mode, the rest once on :root.
    for (context, selectors, bg, fg, muted) in [
        ("", ":root", "#0f172a", "#f8fafc", "#a0a4ac"),
        (DARK, ":root:not(.mode-light)", "#0f0f0f", "#f4f4f4", "#9d9d9d"),
        ("", ":root.mode-dark", "#0f0f0f", "#f4f4f4", "#9d9d9d"),
    ] {
        assert_eq!(token(&rules, context, selectors, "--side-bg").as_deref(), Some(bg), "{selectors}");
        assert_eq!(token(&rules, context, selectors, "--side-fg").as_deref(), Some(fg), "{selectors}");
        assert_eq!(token(&rules, context, selectors, "--side-muted").as_deref(), Some(muted), "{selectors}");
    }
    for (name, value) in [("--side-accent", "var(--s-accent-d)"), ("--side-line", "#ffffff1a"), ("--side-hover", "#ffffff12"), ("--side-current", "#ffffff1c")] {
        assert_eq!(token(&rules, "", ":root", name).as_deref(), Some(value), "{name}");
        assert_eq!(rules.iter().filter(|r| r.declarations.contains(&format!("{name}:"))).count(), 1, "{name}");
    }
    // The old top bar's menu is gone; its brand stays, as it was.
    for gone in ["header.top nav", "header.top .bar-end", "header.top .who"] {
        assert!(!rules.iter().any(|r| r.selectors.split(", ").any(|s| s.starts_with(gone))), "{gone}");
    }
    for (selectors, declarations) in [
        ("header.top", "display: flex; gap: 1.5rem; align-items: center; padding: 0.75rem 1.75rem; border-bottom: 1px solid var(--nav-border); background: var(--nav-bg); flex-wrap: wrap; position: sticky; top: 0; z-index: 50"),
        ("header.top .brand", "display: inline-flex; align-items: center; gap: 0.55rem; font-weight: 700; font-size: 1.15rem; letter-spacing: -0.01em; color: var(--fg-strong); text-decoration: none"),
        ("header.top .brand svg", "color: var(--accent)"),
    ] {
        let found: Vec<&Rule> = rules.iter().filter(|r| r.context.is_empty() && r.selectors == selectors).collect();
        assert_eq!(found.len(), 1, "{selectors}");
        assert_eq!(found[0].declarations, css::declarations(declarations), "{selectors}");
    }
    // Every selector added since is scoped to the frame or is a font's.
    for r in &rules {
        if r.selectors.starts_with('@') {
            assert!(r.selectors == "@font-face" || r.selectors.starts_with("@keyframes"), "{}", r.selectors);
            continue;
        }
        for s in r.selectors.split(',').map(str::trim) {
            if OLD_SELECTORS.contains(&s) {
                continue;
            }
            assert!([".layout", ".sidebar", ".skip", ".eyebrow", ".font-"].iter().any(|p| s.starts_with(p)), "{s} is new and not scoped");
        }
    }
}

/// `#rrggbb` or `#rrggbbaa` as channels 0..=255 and an alpha.
fn hex(s: &str) -> ([f64; 3], f64) {
    let s = s.trim().trim_start_matches('#');
    let c = |i: usize| f64::from(u8::from_str_radix(&s[i..i + 2], 16).expect("hex"));
    let a = if s.len() == 8 { c(6) / 255.0 } else { 1.0 };
    ([c(0), c(2), c(4)], a)
}

/// `top` with alpha laid over the opaque `under`.
fn over(top: &str, under: &str) -> [f64; 3] {
    let ((t, a), (u, _)) = (hex(top), hex(under));
    [0, 1, 2].map(|i| t[i] * a + u[i] * (1.0 - a))
}

fn luminance(c: [f64; 3]) -> f64 {
    let ch = |v: f64| {
        let v = v / 255.0;
        if v <= 0.03928 { v / 12.92 } else { ((v + 0.055) / 1.055).powf(2.4) }
    };
    0.2126 * ch(c[0]) + 0.7152 * ch(c[1]) + 0.0722 * ch(c[2])
}

fn contrast(a: [f64; 3], b: [f64; 3]) -> f64 {
    let (x, y) = (luminance(a), luminance(b));
    (x.max(y) + 0.05) / (x.min(y) + 0.05)
}

/// The panel's text, its muted text and every scheme's accent are legible
/// on the panel, and on the hovered and current items.
#[test]
fn the_sidebar_colours_contrast() {
    let rules = css::rules(CSS);
    let modes = [("", ":root"), (DARK, ":root:not(.mode-light)"), ("", ":root.mode-dark")];
    let accents: Vec<String> = [":root, .scheme-sky", ".scheme-violet", ".scheme-teal", ".scheme-graphite"]
        .iter()
        .map(|s| token(&rules, "", s, "--s-accent-d").unwrap_or_else(|| panic!("{s}")))
        .collect();
    let line = |name: &str| token(&rules, "", ":root", name).expect(name);
    for (context, selectors) in modes {
        let bg = token(&rules, context, selectors, "--side-bg").expect("bg");
        let fg = hex(&token(&rules, context, selectors, "--side-fg").expect("fg")).0;
        let muted = hex(&token(&rules, context, selectors, "--side-muted").expect("muted")).0;
        let panel = hex(&bg).0;
        assert!(contrast(fg, panel) >= 7.0, "{selectors}");
        assert!(contrast(muted, panel) >= 4.5, "{selectors}");
        for item in [line("--side-current"), line("--side-hover")] {
            let back = over(&item, &bg);
            assert!(contrast(fg, back) >= 7.0, "{selectors} {item}");
            for a in &accents {
                assert!(contrast(hex(a).0, back) >= 4.5, "{selectors} {item} {a}");
            }
        }
        for a in &accents {
            assert!(contrast(hex(a).0, panel) >= 4.5, "{selectors} {a}");
        }
    }
}

/// `docs/admin.md`'s page table follows the menu, with Appearance last.
#[test]
fn the_docs_follow_the_menu() {
    let path = FsPath::new(env!("CARGO_MANIFEST_DIR")).join("../docs/admin.md");
    // A build tree with the crate alone skips this, unless
    // HPRS_REQUIRE_REPO_TEXT=1.
    let Ok(text) = std::fs::read_to_string(&path) else {
        assert!(std::env::var("HPRS_REQUIRE_REPO_TEXT").as_deref() != Ok("1"), "{} not found and HPRS_REQUIRE_REPO_TEXT=1", path.display());
        eprintln!("{} not found; skipping the docs' page table check", path.display());
        return;
    };
    assert!(!text.to_lowercase().contains("private posts"), "the docs say kryphocron");
    let start = text.find("| Section | Page |").expect("pages table");
    let rows: Vec<(String, String)> = text[start..]
        .lines()
        .skip(2)
        .take_while(|l| l.starts_with('|'))
        .map(|l| {
            let cells: Vec<&str> = l.split('|').map(str::trim).collect();
            (cells[1].to_string(), cells[2].to_string())
        })
        .collect();
    let mut want: Vec<(String, String)> = MENU
        .iter()
        .flat_map(|(_, ids)| ids.iter())
        .map(|id| {
            if *id == PageId::Private {
                ("kryphocron".to_string(), "kryphocron".to_string())
            } else {
                (id.section_label().unwrap_or_default().to_string(), id.title())
            }
        })
        .collect();
    want.push(("Your account".to_string(), "Appearance".to_string()));
    assert_eq!(rows, want);
    assert!(text.contains("Old admin addresses redirect to their new pages."));
}

/// Every table in every template sits alone in its own scroll box, with no
/// width attribute.
#[test]
fn every_template_table_is_boxed() {
    let dir = FsPath::new(env!("CARGO_MANIFEST_DIR")).join("src/admin/templates");
    for entry in std::fs::read_dir(&dir).expect("templates") {
        let path = entry.expect("entry").path();
        let text = std::fs::read_to_string(&path).expect("template");
        let name = path.display().to_string();
        let mut at = 0;
        while let Some(start) = text[at..].find("<table").map(|s| at + s) {
            let before = text[..start].trim_end();
            assert!(before.ends_with("<div class=\"scroll\">"), "{name}: a table not first in its .scroll");
            let end = text[start..].find("</table>").map(|e| start + e + "</table>".len()).expect("table end");
            assert!(text[end..].trim_start().starts_with("</div>"), "{name}: something after a table in its .scroll");
            at = end;
        }
        for tag in ["<table", "<th", "<td"] {
            for (i, _) in text.match_indices(tag) {
                let open = &text[i..i + text[i..].find('>').expect("tag end")];
                assert!(!open.contains(" width="), "{name}: {open}");
            }
        }
    }
}

/// Seeds every table of the reshaped pages, and the redirects' accounts.
async fn seed(h: &Harness) {
    let p = h.pool();
    let q = |sql: &'static str| sqlx::query(sql);
    sqlx::query("INSERT INTO members (did, source) VALUES ($1, 'admin')").bind(BOB).execute(p).await.expect("member");
    sqlx::query("INSERT INTO member_denials (did, reason, denied_by) VALUES ($1, 'a note', 'cli')").bind(CAROL).execute(p).await.expect("denial");
    sqlx::query("INSERT INTO labels (subject, val) VALUES ($1, 'porn')").bind(format!("at://{BOB}/app.bsky.feed.post/3kp")).execute(p).await.expect("label");
    sqlx::query("INSERT INTO scope_dids (did, reasons) VALUES ($1, 4)").bind(BOB).execute(p).await.expect("scope");
    sqlx::query("INSERT INTO backfill_jobs (did, depth, state, attempts, last_error) VALUES ($1, 'scope', 'failed', 3, $2)")
        .bind(BOB)
        .bind("x".repeat(200))
        .execute(p)
        .await
        .expect("failed job");
    q("INSERT INTO backfill_jobs (did, depth, state, attempts, last_error) VALUES ('did:plc:un1', 'scope', 'unavailable', 1, 'repo inactive (takendown)')")
        .execute(p)
        .await
        .expect("unavailable job");
    sqlx::query(
        "INSERT INTO refract_dids (did, community, pds_host, state, source, since, last_verdict) VALUES ($1, 'community-a', 'pds.community-a.example', 'restored', 'fill', now(), '{}')",
    )
    .bind(BOB)
    .execute(p)
    .await
    .expect("refract");
    for did in ["did:plc:abc", "did:web:h%3A8080"] {
        sqlx::query("INSERT INTO refract_dids (did, community, pds_host, state, source) VALUES ($1, 'community-a', 'pds.community-a.example', 'restored', 'admin')")
            .bind(did)
            .execute(p)
            .await
            .expect("refract did");
    }
    q("INSERT INTO refract_pending (scope) VALUES ('feature')").execute(p).await.expect("pending");
    sqlx::query("INSERT INTO refract_pending_dids (scope, did) VALUES ('feature', $1)").bind(BOB).execute(p).await.expect("pending did");
    sqlx::query("INSERT INTO refract_labels (labeler_did, subject, val, did, gone, verified) VALUES ('did:plc:labelerofcommunitya000', $1, '!takedown', $1, true, true)")
        .bind(BOB)
        .execute(p)
        .await
        .expect("refract label");
    sqlx::query("INSERT INTO refract_events (did, from_state, to_state, reason) VALUES ($1, 'candidate', 'restored', 'rules')").bind(BOB).execute(p).await.expect("event");
    q("INSERT INTO operator_settings (key, value, updated_by) VALUES ('retention.tombstone_days', '3', 'cli')").execute(p).await.expect("waiting");
    q("INSERT INTO operator_settings (key, value, updated_by, set_aside) VALUES ('private_read.max_chain', '500', 'cli', '{\"problem\": \"private_read.max_chain must be 1 to 100\"}')")
        .execute(p)
        .await
        .expect("aside");
    let _ = crate::admin::usage::path("app", "app.bsky.feed.getTimeline");
    let _ = crate::admin::usage::path("test", "test.frame.neverUsed");
    h.usage.load_registry();
    h.usage.record("app.bsky.feed.getTimeline", crate::admin::usage::Class::Ok);
}

/// The configuration of the frame's page tests: two relays and two listed
/// communities, so About's tables have rows.
fn frame_config() -> crate::config::FileConfig {
    let mut file = test_support::no_auto_join();
    file.firehose.relay_labels =
        [("wss://relay-a.example/xrpc/com.atproto.sync.subscribeRepos".to_string(), "relay-a".to_string())].into_iter().collect();
    let mut a = crate::config::RefractCommunity::example();
    a.name = "community-a".to_string();
    a.pds_patterns = (1..=8).map(|n| format!("pds{n}.community-a.example")).collect();
    file.refract.communities = vec![a];
    file
}

/// What one page's tables must be: each by its first header, with every
/// column's class.
type Expected = (&'static str, &'static [(&'static str, &'static [&'static str])]);

const ACCOUNTS: &[&str] = &["name", "name", "state", "when", "when", "when", "text", "actions"];
const PENDING: &[&str] = &["name", "num", "when", "when", "actions"];

/// Every page with tables, by path: `{BOB}` is Bob's DID.
const TABLES: &[Expected] = &[
    (
        "/admin/members",
        &[("Account", &["name", "state", "when", "when", "when", "state", "actions"]), ("Account", &["name", "text", "name", "when", "actions"])],
    ),
    ("/admin/operators", &[("Account", &["name", "text", "name", "when", "actions"]), ("Server", &["name", "text", "when", "actions"])]),
    ("/admin/lookup?q={BOB}", &[("On", &["name", "name", "when", "actions"]), ("Kind", &["state", "state", "num", "text", "when"])]),
    ("/admin/labels", &[("On", &["name", "name", "when", "actions"])]),
    ("/admin/communities", &[("Community", &["name", "list", "name", "list", "list", "actions"])]),
    ("/admin/communities/accounts", &[("Account", ACCOUNTS)]),
    ("/admin/communities/pending", &[("Why", PENDING)]),
    ("/admin/communities/accounts/{BOB}", &[("Labeler", &["name", "name", "name", "when", "when", "state", "state"]), ("When", &["when", "state", "state", "text"])]),
    ("/admin/jobs", &[("Kind", &["state", "num", "num", "num", "num", "num", "num"]), ("Why", &["text", "num"]), ("Account", &["name", "state", "num", "text", "when", "actions"])]),
    ("/admin/health", &[("Check", &["name", "state", "text"])]),
    ("/admin/stats", &[("Request", &["name", "num", "num", "num", "num", "num", "num", "when"]), ("Request", &["name"])]),
    (
        "/admin/about",
        &[("Part", &["name", "state", "text"]), ("Relay", &["name", "list", "state", "when"]), ("Community", &["name", "num", "list", "name", "state"]), ("Method", &["name", "text"])],
    ),
    ("/admin/audit", &[("When", &["when", "name", "text", "text"]), ("When", &["when", "name", "text", "name"])]),
    ("/admin/settings", &[("Setting", &["name", "text"]), ("Setting", &["name", "text"])]),
    ("/admin/settings/installed", &[("What", &["name", "text", "text"])]),
];

const CLASSES: [&str; 7] = ["num", "when", "name", "state", "actions", "list", "text"];

fn check_table(path: &str, i: usize, t: &Table, want: (&str, &[&str])) {
    assert_eq!(t.parent_class, "scroll", "{path} #{i}");
    assert!(t.alone, "{path} #{i}: not alone in its box");
    assert_eq!(t.head.first().map(|c| c.text.as_str()), Some(want.0), "{path} #{i}");
    let head: Vec<&str> = t.head.iter().map(|c| c.classes.iter().find(|k| CLASSES.contains(&k.as_str())).map_or("", String::as_str)).collect();
    assert_eq!(head, want.1, "{path} #{i}: header classes");
    let data: Vec<&Vec<html::Cell>> = t.rows.iter().filter(|r| !(r.len() == 1 && r[0].colspan)).collect();
    assert!(!data.is_empty(), "{path} #{i}: no data row; seed it");
    for row in data {
        assert_eq!(row.len(), want.1.len(), "{path} #{i}: {row:?}");
        for (cell, class) in row.iter().zip(want.1) {
            let ours: Vec<&String> = cell.classes.iter().filter(|k| CLASSES.contains(&k.as_str())).collect();
            assert_eq!(ours.len(), 1, "{path} #{i}: {cell:?}");
            assert_eq!(ours[0], class, "{path} #{i}: {cell:?}");
            assert!(!cell.classes.iter().any(|k| k == "nowrap"), "{path} #{i}: {cell:?}");
            if *class == "text" {
                assert!(cell.cell_text_only, "{path} #{i}: a text cell without its div: {cell:?}");
            }
        }
    }
}

/// Every table on a signed-in page: in its own scroll box, each column's
/// header and cells classed, each text cell in its div; and no page draws a
/// table it should not.
#[tokio::test]
async fn every_table_is_classed() {
    let Some(h) = harness_config("admin_frame_tables", quick(), true, health::HealthInputs::default(), frame_config()).await else {
        return;
    };
    seed(&h).await;
    h.identity.set(BOB, PDS);
    let alice = complete_setup(&h, ALICE).await;
    // A command, for the audit log.
    let members = get(&alice, &format!("{}/admin/members", h.base)).await;
    let resync = post(&alice, &format!("{}/admin/members/resync", h.base), &[("csrf", &csrf_of(&members)), ("did", BOB)]).await;
    assert_eq!(resync.status, 303, "{}", resync.body);
    for (path, tables) in TABLES {
        let path = path.replace("{BOB}", BOB);
        let page = get(&alice, &format!("{}{path}", h.base)).await;
        assert_eq!(page.status, 200, "{path}: {}", page.body);
        let found = html::tables(&page.body);
        assert_eq!(found.len(), tables.len(), "{path}: {} tables", found.len());
        for (i, (t, want)) in found.iter().zip(tables.iter()).enumerate() {
            check_table(&path, i, t, *want);
        }
    }
    h.db.drop_db().await;
}

/// The paths a page of this admin answered before the menu moved: each is a
/// page or a redirect now.
const OLD_PATHS: &[&str] = &[
    "/admin",
    "/admin/members",
    "/admin/health",
    "/admin/jobs",
    "/admin/problems",
    "/admin/stats",
    "/admin/instance",
    "/admin/farsight",
    "/admin/labels",
    "/admin/communities",
    "/admin/refract",
    "/admin/lookup",
    "/admin/audit",
    "/admin/activity",
    "/admin/operators",
    "/admin/settings",
    "/admin/settings/installed",
    "/admin/settings/running",
    "/admin/settings/appearance",
    "/admin/settings/access",
    "/admin/settings/storage",
    "/admin/settings/private",
    "/admin/settings/blocks",
    "/admin/settings/apps",
    "/admin/settings/services",
    "/admin/settings/limits",
    "/admin/settings/profiles",
    "/admin/settings/lookups",
];

/// Whether a path is an old address that redirects now.
fn redirected(path: &str) -> bool {
    REDIRECTS.iter().any(|(old, _, _)| *old == path) || path.strip_prefix("/admin/settings/").is_some_and(|slug| menu::group_redirect(slug).is_some())
}

/// The old paths of moved forms, which no page may post to: each also an
/// old group's page, `/admin/settings/{slug}`.
const ALIASES: &[&str] = &["/admin/refract/add", "/admin/refract/check", "/admin/refract/end", "/admin/settings/appearance"];

/// Whether a form's `action` is an old path kept only for pages loaded
/// before it moved.
fn is_alias(action: &str) -> bool {
    ALIASES.contains(&action) || action.strip_prefix("/admin/settings/").is_some_and(|slug| menu::OLD_GROUP_SLUGS.contains(&slug))
}

/// Every `href` and `action` of a page, as paths, each with whether it is
/// an `action`.
fn targets(body: &str) -> Vec<(bool, String)> {
    let mut out = Vec::new();
    for key in ["href=\"", "action=\""] {
        for (i, _) in body.match_indices(key) {
            let rest = &body[i + key.len()..];
            let v = &rest[..rest.find('"').unwrap_or(0)];
            let path = v.split(['?', '#']).next().unwrap_or_default();
            out.push((key == "action=\"", path.to_string()));
        }
    }
    out
}

/// Each old address answers 308 with its query, for anyone, with the admin
/// headers, and leads to a page; a DID keeps its `%`; anything else is the
/// plain 404 page.
#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn every_old_address_redirects() {
    let mut file = frame_config();
    file.refract.enabled = true;
    let Some(h) = harness_config("admin_frame_redirects", quick(), true, health::HealthInputs::default(), file).await else {
        return;
    };
    seed(&h).await;
    let alice = complete_setup(&h, ALICE).await;
    let stranger = Browser::new();
    let url = |path: &str| format!("{}{path}", h.base);
    let query = "?state=restored&community=community-a.example&page=2";
    for (old, to, q) in [
        ("/admin/farsight", "/admin/blocks", ""),
        ("/admin/refract", "/admin/communities/accounts", query),
        ("/admin/refract", "/admin/communities/accounts", ""),
        ("/admin/instance", "/admin/about", ""),
        ("/admin/settings/appearance", "/admin/appearance", "?saved=1"),
        ("/admin/activity", "/admin/audit", "?who=cli"),
        ("/admin/refract/did%3Aplc%3Aabc", "/admin/communities/accounts/did:plc:abc", ""),
        ("/admin/refract/did:web:h%253A8080", "/admin/communities/accounts/did:web:h%253A8080", ""),
    ] {
        let page = get(&stranger, &url(&format!("{old}{q}"))).await;
        assert_eq!(page.status, 308, "{old}{q}");
        assert_eq!(location(&page), Some(format!("{to}{q}").as_str()), "{old}{q}");
        assert_admin_headers(&page, old);
        assert_eq!(page.headers.get("cache-control").and_then(|v| v.to_str().ok()), Some(NO_STORE), "{old}");
        let followed = get(&alice, &url(&format!("{to}{q}"))).await;
        assert_eq!(followed.status, 200, "{to}{q}: {}", followed.body);
    }
    // Every row of the table is one of these.
    assert_eq!(REDIRECTS.len(), 5);
    // Old group pages with no settings left: their slug's GET only, the
    // query kept before the fragment.
    for (slug, to, fragment) in GROUP_REDIRECTS {
        for q in ["", "?open=advanced"] {
            let old = format!("/admin/settings/{slug}{q}");
            let page = get(&stranger, &url(&old)).await;
            assert_eq!(page.status, 308, "{old}");
            let want = if fragment.is_empty() { format!("{to}{q}") } else { format!("{to}{q}#{fragment}") };
            assert_eq!(location(&page), Some(want.as_str()), "{old}");
            assert_admin_headers(&page, &old);
            assert_eq!(get(&alice, &url(&format!("{to}{q}"))).await.status, 200, "{to}{q}");
        }
    }
    assert_eq!(GROUP_REDIRECTS.iter().map(|(s, _, _)| *s).collect::<Vec<_>>(), ["communities", "blocks", "access"]);
    // The encoded port reaches the account page as one DID.
    let account = get(&alice, &url("/admin/communities/accounts/did:web:h%253A8080")).await;
    assert!(account.body.contains("did:web:h%3A8080") && !account.body.contains("did:web:h:8080"), "{}", account.body);
    assert!(account.body.contains("href=\"/admin/lookup?q=did:web:h%253A8080\""), "{}", account.body);
    let list = get(&alice, &url("/admin/communities/accounts")).await;
    assert!(list.body.contains("href=\"/admin/communities/accounts/did:web:h%253A8080\""), "{}", list.body);
    sqlx::query("INSERT INTO members (did, source) VALUES ('did:web:h%3A8080', 'admin')").execute(h.pool()).await.expect("member");
    let members = get(&alice, &url("/admin/members")).await;
    assert!(members.body.contains("href=\"/admin/lookup?q=did:web:h%253A8080\""), "{}", members.body);
    let looked = get(&alice, &url("/admin/lookup?q=did:web:h%253A8080")).await;
    assert!(looked.body.contains("value=\"did:web:h%3A8080\""), "{}", looked.body);
    // The command page's link for that DID carries it too.
    let checked = post(&alice, &url("/admin/communities/accounts/check"), &[("csrf", &csrf_of(&list)), ("did", "did:web:h%3A8080")]).await;
    assert_eq!(checked.status, 303, "{}", checked.body);
    let command = location(&checked).expect("command").to_string();
    sqlx::query("UPDATE admin_commands SET state = 'done', result = '{}' WHERE state = 'queued'").execute(h.pool()).await.expect("done");
    let ran = get(&alice, &url(&command)).await;
    assert!(ran.body.contains("href=\"/admin/communities/accounts/did:web:h%253A8080\""), "{}", ran.body);
    // Not a DID, or not text: the plain 404 page, without a session.
    for path in [
        "/admin/refract/not%20a%20did",
        "/admin/refract/did:web:%FF",
        "/admin/communities/accounts/did:web:%FF",
        "/admin/settings/%FF",
        "/admin/static/fonts/%FF",
        "/admin/settings/nowhere",
    ] {
        let page = get(&stranger, &url(path)).await;
        assert_eq!(page.status, 404, "{path}");
        assert!(page.body.contains("There is no page here."), "{path}: {}", page.body);
    }
    // A POST to an unknown group saves nothing.
    let settings = get(&alice, &url("/admin/settings")).await;
    let before = count(h.pool(), "SELECT count(*) FROM admin_commands").await;
    let nowhere = post(&alice, &url("/admin/settings/nowhere"), &[("csrf", &csrf_of(&settings)), ("retention.tombstone_days", "9")]).await;
    assert_eq!(nowhere.status, 404);
    assert_eq!(count(h.pool(), "SELECT count(*) FROM admin_commands").await, before);
    // A POST to the old communities slug, never a group page, saves nothing.
    let communities = post(&alice, &url("/admin/settings/communities"), &[("csrf", &csrf_of(&settings)), ("refract.visitor_viewer_state", "off")]).await;
    assert_eq!(communities.status, 404);
    assert_eq!(count(h.pool(), "SELECT count(*) FROM admin_commands").await, before);
    // A group with keys left is still its page.
    let storage = get(&alice, &url("/admin/settings/storage")).await;
    assert_eq!(storage.status, 200);
    assert_eq!(current_item(&storage.body), Some(("All settings".to_string(), "true".to_string())), "{}", storage.body);
    h.db.drop_db().await;
}

/// Only built pages are routed; each old path is a page or a redirect; no
/// page links an old address or posts to an old form path; no page has an
/// inline style, a script but Home's, a tab index, or an old name; and each
/// marks exactly one sidebar element.
#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn only_built_pages_and_no_stale_links() {
    let Some(h) = harness_config("admin_frame_built", quick(), true, health::HealthInputs::default(), frame_config()).await else {
        return;
    };
    seed(&h).await;
    h.identity.set(BOB, PDS);
    let alice = complete_setup(&h, ALICE).await;
    let url = |path: &str| format!("{}{path}", h.base);
    for p in PAGES {
        let page = get(&alice, &url(p.href)).await;
        if p.id.built() {
            assert_eq!(page.status, 200, "{}", p.href);
        } else {
            assert_eq!(page.status, 404, "{} is not built", p.href);
            assert!(page.body.contains("There is no page here."), "{}", p.href);
        }
    }
    for t in [menu::Tab::Communities, menu::Tab::Accounts, menu::Tab::Pending] {
        let page = get(&alice, &url(t.href())).await;
        assert_eq!(page.status, if TABS.contains(&t) { 200 } else { 404 }, "{}", t.href());
    }
    for old in OLD_PATHS {
        let page = get(&alice, &url(old)).await;
        assert!(page.status == 200 || page.status == 308, "{old}: {}", page.status);
        assert_eq!(page.status == 308, redirected(old), "{old}: {}", page.status);
    }
    // The pages to read: every built page and tab, an account page with a
    // pending end, a command page, Installed, Running, a group page, the
    // Refract refusal and "Not known", Appearance's "Not saved" and a
    // confirmation.
    let mut pages: Vec<(String, Page)> = Vec::new();
    for p in PAGES.iter().filter(|p| p.id.built()) {
        pages.push((p.href.to_string(), get(&alice, &url(p.href)).await));
    }
    for path in [
        "/admin/communities/accounts".to_string(),
        menu::account_href(BOB),
        "/admin/communities/accounts/did:plc:nobodynobodynobodynobody".to_string(),
        "/admin/settings/installed".to_string(),
        "/admin/settings/running".to_string(),
        "/admin/settings/storage".to_string(),
        format!("/admin/lookup?q={BOB}"),
    ] {
        pages.push((path.clone(), get(&alice, &url(&path)).await));
    }
    let list = &pages.iter().find(|(p, _)| p == "/admin/communities/accounts").expect("list").1;
    let csrf = csrf_of(list);
    let refused = post(&alice, &url("/admin/communities/accounts/add"), &[("csrf", &csrf), ("input", "not a handle")]).await;
    assert_eq!(refused.status, 400, "{}", refused.body);
    pages.push(("a refusal".to_string(), refused));
    let ended = post(&alice, &url("/admin/communities/accounts/end"), &[("csrf", &csrf), ("did", BOB)]).await;
    assert_eq!(ended.status, 200, "{}", ended.body);
    pages.push(("the end confirmation".to_string(), ended));
    let not_saved = post(&alice, &url("/admin/appearance"), &[("csrf", &csrf), ("mode", "dark"), ("scheme", "teal"), ("font", "comic")]).await;
    assert_eq!(not_saved.status, 400, "{}", not_saved.body);
    pages.push(("not saved".to_string(), not_saved));
    let resync = post(&alice, &url("/admin/members/resync"), &[("csrf", &csrf), ("did", BOB)]).await;
    let command = location(&resync).expect("command").to_string();
    pages.push((command.clone(), get(&alice, &url(&command)).await));
    // A typed settings change's confirmation, from a group page.
    let typed = post(&alice, &url("/admin/settings/save"), &[("csrf", &csrf), ("farsight.fallback.enabled", "on")]).await;
    assert_eq!(typed.status, 200, "{}", typed.body);
    pages.push(("the settings confirmation".to_string(), typed));
    for (path, page) in &pages {
        let body = &page.body;
        for (action, t) in targets(body) {
            assert!(!redirected(&t), "{path} links {t}");
            assert!(!t.starts_with(menu::OLD_ACCOUNT_PREFIX), "{path} links {t}");
            assert!(!(action && is_alias(&t)), "{path} posts to {t}");
        }
        assert!(!body.contains("style="), "{path}: an inline style");
        assert!(!body.contains("tabindex"), "{path}: a tab index");
        assert_eq!(body.contains("<script"), path == "/admin", "{path}: scripts");
        for old in ["Refract accounts", "<h1>Instance</h1>", "Most members", "Limit for auto-join", "Blocks (Farsight)", "Who can use Prism", "Fallback and memory settings"] {
            assert!(!body.contains(old), "{path}: an old name, {old}");
        }
        assert!(current_item(body).is_some(), "{path}: not exactly one marked item: {}", sidebar(body));
    }
    h.db.drop_db().await;
}

/// Sub-pages mark their parent as the section, not the page.
#[tokio::test]
async fn sub_pages_mark_their_parent() {
    let Some(h) = harness_config("admin_frame_parents", quick(), true, health::HealthInputs::default(), frame_config()).await else {
        return;
    };
    seed(&h).await;
    let alice = complete_setup(&h, ALICE).await;
    for (path, item) in [
        ("/admin/communities/accounts".to_string(), "Communities"),
        (menu::account_href(BOB), "Communities"),
        ("/admin/communities/pending".to_string(), "Communities"),
        ("/admin/settings/installed".to_string(), "All settings"),
        ("/admin/settings/running".to_string(), "All settings"),
        ("/admin/settings/storage".to_string(), "All settings"),
    ] {
        let page = get(&alice, &format!("{}{path}", h.base)).await;
        assert_eq!(page.status, 200, "{path}");
        assert_eq!(current_item(&page.body), Some((item.to_string(), "true".to_string())), "{path}");
    }
    let installed = get(&alice, &format!("{}/admin/settings/installed", h.base)).await;
    assert!(installed.body.contains("<a href=\"/admin/settings\">All settings</a>") && installed.body.contains("<p class=\"eyebrow\">System</p>"), "{}", installed.body);
    h.db.drop_db().await;
}

/// Every new and old POST path of the moved forms refuses a missing token,
/// another session's token and a foreign `Origin`, changing nothing.
#[tokio::test]
async fn moved_forms_check_the_token() {
    let mut file = frame_config();
    file.refract.enabled = true;
    let Some(h) = harness_config("admin_frame_csrf", quick(), true, health::HealthInputs::default(), file).await else {
        return;
    };
    seed(&h).await;
    let p = h.pool();
    let alice = complete_setup(&h, ALICE).await;
    sqlx::query("INSERT INTO admin_operators (did, added_by) VALUES ($1, 'cli')").bind(CAROL).execute(p).await.expect("carol");
    h.identity.set(CAROL, PDS);
    let (carol, _) = sign_in(&h, CAROL).await;
    let csrf = csrf_of(&get(&alice, &format!("{}/admin/members", h.base)).await);
    let other = csrf_of(&get(&carol, &format!("{}/admin/members", h.base)).await);
    let theme = || async { sqlx::query_as::<_, (String, String, String)>("SELECT theme_mode, theme_scheme, theme_font FROM admin_operators WHERE did = $1").bind(ALICE).fetch_one(p).await.expect("theme") };
    let before = (count(p, "SELECT count(*) FROM admin_commands").await, theme().await);
    for path in [
        "/admin/appearance",
        "/admin/settings/appearance",
        "/admin/communities/accounts/add",
        "/admin/communities/accounts/check",
        "/admin/communities/accounts/end",
        "/admin/refract/add",
        "/admin/refract/check",
        "/admin/refract/end",
        "/admin/settings/save",
        "/admin/settings/access",
        "/admin/settings/blocks",
        "/admin/communities/keep",
    ] {
        let target = format!("{}{path}", h.base);
        let fields = [
            ("mode", "dark"),
            ("scheme", "teal"),
            ("input", CAROL),
            ("did", BOB),
            ("confirm", "yes"),
            ("typed", "bbbbbbbb"),
            ("community.max_members", "5"),
            ("farsight.memory.max_total", "5"),
            ("scope", "feature"),
            ("keep", "true"),
        ];
        let mut with = vec![("csrf", other.as_str())];
        with.extend(fields);
        let mut own = vec![("csrf", csrf.as_str())];
        own.extend(fields);
        for (i, page) in [
            post(&alice, &target, &fields).await,
            post(&alice, &target, &with).await,
            {
                tokio::time::sleep(Duration::from_millis(1100)).await;
                alice.post_with(&target, &own, "https://evil.example").await
            },
        ]
        .iter()
        .enumerate()
        {
            assert_eq!(page.status, 403, "{path} #{i}: {}", page.body);
        }
    }
    assert_eq!((count(p, "SELECT count(*) FROM admin_commands").await, theme().await), before);
    h.db.drop_db().await;
}

/// Cancel goes back to the page the form was on: the form's `back`, else
/// the `Referer` from this host, else the action's own page; and never to
/// anything else.
#[tokio::test]
async fn cancel_goes_back() {
    let Some(h) = harness("admin_frame_cancel").await else {
        return;
    };
    let p = h.pool();
    let alice = complete_setup(&h, ALICE).await;
    sqlx::query("INSERT INTO members (did, source) VALUES ($1, 'admin')").bind(CAROL).execute(p).await.expect("member");
    let csrf = csrf_of(&get(&alice, &format!("{}/admin/members", h.base)).await);
    let remove = format!("{}/admin/members/remove", h.base);
    let cancel = |body: &str| {
        let at = body.find("\">Cancel</a>").expect("cancel");
        let start = body[..at].rfind("href=\"").expect("href") + "href=\"".len();
        body[start..at].to_string()
    };
    let here = format!("https://{HOST}/admin/members?page=2");
    // From the Referer, kept through a wrong typed word.
    let asked = post_from(&alice, &remove, &[("csrf", &csrf), ("did", CAROL)], &here).await;
    assert_eq!(asked.status, 200, "{}", asked.body);
    assert_eq!(cancel(&asked.body), "/admin/members?page=2");
    assert_eq!(field(&asked.body, "back").as_deref(), Some("/admin/members?page=2"));
    assert_eq!(current_item(&asked.body), Some(("Members".to_string(), "page".to_string())));
    let wrong = post(&alice, &remove, &[("csrf", &csrf), ("did", CAROL), ("confirm", "yes"), ("typed", "nope"), ("back", "/admin/members?page=2")]).await;
    assert_eq!(wrong.status, 400);
    assert_eq!(cancel(&wrong.body), "/admin/members?page=2");
    // The form's own field wins over the Referer, and the sidebar marks it.
    let both = post_from(&alice, &remove, &[("csrf", &csrf), ("did", CAROL), ("back", "/admin/lookup?q=x")], &here).await;
    assert_eq!(cancel(&both.body), "/admin/lookup?q=x");
    assert_eq!(current_item(&both.body), Some(("Look up".to_string(), "page".to_string())));
    // Anything else falls back to the action's page: another host, none, a
    // tampered field, a page not built yet.
    for (back, referer) in [
        (None, Some("https://evil.example/admin/lookup")),
        (None, Some("http://prism.example.com/admin/lookup")),
        (None, None),
        (Some("//evil.example"), None),
        (Some("https://evil.example/admin"), None),
        (Some("/admin/storage"), None),
        (None, Some("https://prism.example.com/admin/storage")),
    ] {
        let mut form = vec![("csrf", csrf.as_str()), ("did", CAROL)];
        if let Some(b) = back {
            form.push(("back", b));
        }
        let page = match referer {
            Some(r) => post_from(&alice, &remove, &form, r).await,
            None => post(&alice, &remove, &form).await,
        };
        assert_eq!(cancel(&page.body), "/admin/members", "{back:?} {referer:?}");
    }
    assert_eq!(count(p, "SELECT count(*) FROM admin_commands").await, 0);
    h.db.drop_db().await;
}

/// Pages before sign-in keep the old top bar and draw no sidebar.
#[tokio::test]
async fn signed_out_pages_keep_the_top_bar() {
    let Some(h) = harness("admin_frame_signed_out").await else {
        return;
    };
    let _alice = complete_setup(&h, ALICE).await;
    let login = Browser::new().get(&format!("{}/admin/login", h.base)).await;
    assert!(login.body.contains("<header class=\"top\">") && login.body.contains("<a class=\"brand\" href=\"/admin\">"), "{}", login.body);
    assert!(!login.body.contains("class=\"sidebar\"") && !login.body.contains("class=\"layout\"") && !login.body.contains("class=\"eyebrow\""), "{}", login.body);
    h.db.drop_db().await;
}
