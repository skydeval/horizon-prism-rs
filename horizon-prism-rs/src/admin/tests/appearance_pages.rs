//! Appearance and the page frame over HTTP: an operator's look (mode,
//! colours and font) is saved for them alone and set on `<html>` of their
//! pages only; its form refuses a missing or another session's token, a
//! foreign `Origin` and an unknown value, changing nothing; a form without
//! a font keeps the stored one; nothing is queued or audited. The fonts and
//! their licences are served to anyone, cached for a year. The top menu's
//! order, Settings last, and the product name.

use super::*;
use crate::admin::fonts::{FONTS, TEXT, WOFF2};

/// A page, under the ten-pages-a-second limit (§5.6).
async fn get(b: &Browser, url: &str) -> Page {
    tokio::time::sleep(Duration::from_millis(120)).await;
    b.get(url).await
}

async fn stored(pool: &PgPool, did: &str) -> (String, String, String) {
    sqlx::query_as("SELECT theme_mode, theme_scheme, theme_font FROM admin_operators WHERE did = $1").bind(did).fetch_one(pool).await.expect("operator")
}

fn look(mode: &str, scheme: &str, font: &str) -> (String, String, String) {
    (mode.to_string(), scheme.to_string(), font.to_string())
}

/// The labels of the top menu, in order.
fn menu(body: &str) -> Vec<String> {
    let start = body.find("<nav>").expect("nav");
    let end = body[start..].find("</nav>").expect("nav end") + start;
    body[start..end].split("</a>").filter(|s| s.contains("<a ")).map(|s| s.rsplit('>').next().unwrap_or_default().to_string()).collect()
}

#[tokio::test]
async fn appearance_is_per_operator_and_on_html() {
    let Some(h) = harness("admin_appearance").await else {
        return;
    };
    let p = h.pool();
    let url = |path: &str| format!("{}{path}", h.base);
    let alice = complete_setup(&h, ALICE).await;
    sqlx::query("INSERT INTO admin_operators (did, added_by) VALUES ($1, 'cli')").bind(BOB).execute(p).await.expect("bob");
    h.identity.set(BOB, PDS);
    let (bob, _) = sign_in(&h, BOB).await;

    // The default before any choice: the device decides, sky, Atkinson
    // Hyperlegible.
    let default_html = "<html lang=\"en\" class=\"mode-system scheme-sky font-hyperlegible\">";
    let members = get(&alice, &url("/admin/members")).await;
    assert!(members.body.contains(default_html), "{}", members.body);
    assert_eq!(stored(p, ALICE).await, look("system", "sky", "hyperlegible"));

    // The page: linked from Settings, under Settings in the menu, the
    // current choice checked.
    let settings = get(&alice, &url("/admin/settings")).await;
    assert!(settings.body.contains("href=\"/admin/settings/appearance\""), "{}", settings.body);
    let page = get(&alice, &url("/admin/settings/appearance")).await;
    assert_eq!(page.status, 200, "{}", page.body);
    assert_admin_headers(&page, "appearance");
    assert!(page.body.contains("aria-current=\"page\">Settings</a>"), "{}", page.body);
    assert!(page.body.contains("value=\"system\" checked") && page.body.contains("value=\"sky\" checked"), "{}", page.body);
    for value in ["light", "dark", "violet", "teal", "graphite", "plex"] {
        assert!(page.body.contains(&format!("value=\"{value}\">")), "{value}: {}", page.body);
    }
    // Each font's label is drawn in that font, the current one checked.
    assert!(page.body.contains("name=\"font\" value=\"hyperlegible\" checked"), "{}", page.body);
    for font in ["hyperlegible", "system", "plex"] {
        assert!(page.body.contains(&format!("<label class=\"choice font-sample font-{font}\"><input type=\"radio\" name=\"font\" value=\"{font}\"")), "{font}: {}", page.body);
    }
    assert!(page.body.contains("Atkinson Hyperlegible (the default)") && page.body.contains("IBM Plex"), "{}", page.body);
    assert!(!page.body.contains("style="), "no inline styles");
    let csrf = csrf_of(&page);
    let bob_csrf = csrf_of(&get(&bob, &url("/admin/members")).await);
    assert_ne!(csrf, bob_csrf);

    // Refused, changing nothing: no token, another session's token, a
    // foreign Origin, an unknown value.
    let save = url("/admin/settings/appearance");
    let refused = [
        alice.post(&save, &[("mode", "dark"), ("scheme", "teal")]).await,
        alice.post(&save, &[("csrf", &bob_csrf), ("mode", "dark"), ("scheme", "teal")]).await,
        alice.post_with(&save, &[("csrf", &csrf), ("mode", "dark"), ("scheme", "teal")], "https://evil.example").await,
    ];
    for (i, page) in refused.iter().enumerate() {
        assert_eq!(page.status, 403, "{i}: {}", page.body);
    }
    assert_eq!(stored(p, ALICE).await, look("system", "sky", "hyperlegible"));
    // An unknown scheme, an unknown font, an empty font.
    let unknowns = [("rainbow", "plex"), ("teal", "comic"), ("teal", "")];
    for (i, (scheme, font)) in unknowns.into_iter().enumerate() {
        tokio::time::sleep(Duration::from_millis(1100)).await;
        let unknown = alice.post(&save, &[("csrf", &csrf), ("mode", "dark"), ("scheme", scheme), ("font", font)]).await;
        assert_eq!(unknown.status, 400, "{i}: {}", unknown.body);
        assert_eq!(stored(p, ALICE).await, look("system", "sky", "hyperlegible"), "{i}");
    }

    // Saved: back to the page, which says so, and every page of Alice's
    // carries it on <html>, Home's watching reload included.
    tokio::time::sleep(Duration::from_millis(1100)).await;
    let saved = alice.post(&save, &[("csrf", &csrf), ("mode", "dark"), ("scheme", "teal"), ("font", "system")]).await;
    assert_eq!(saved.status, 303, "{}", saved.body);
    assert_eq!(saved.headers.get(LOCATION).and_then(|v| v.to_str().ok()), Some("/admin/settings/appearance?saved=1"));
    assert_eq!(stored(p, ALICE).await, look("dark", "teal", "system"));
    let chosen_html = "<html lang=\"en\" class=\"mode-dark scheme-teal font-system\">";
    let back = get(&alice, &url("/admin/settings/appearance?saved=1")).await;
    assert!(back.body.contains("Saved.") && back.body.contains(chosen_html), "{}", back.body);
    assert!(back.body.contains("value=\"dark\" checked") && back.body.contains("value=\"teal\" checked"), "{}", back.body);
    assert!(back.body.contains("name=\"font\" value=\"system\" checked") && !back.body.contains("value=\"hyperlegible\" checked"), "{}", back.body);
    for path in ["/admin", "/admin?auto=1", "/admin/members", "/admin/farsight", "/admin/settings"] {
        let page = get(&alice, &url(path)).await;
        assert_eq!(page.status, 200, "{path}");
        assert!(page.body.contains(chosen_html), "{path}: {}", page.body);
    }

    // A form from before the font choice, mode and colours only: those
    // are saved, and the font stays.
    tokio::time::sleep(Duration::from_millis(1100)).await;
    let two_fields = alice.post(&save, &[("csrf", &csrf), ("mode", "light"), ("scheme", "violet")]).await;
    assert_eq!(two_fields.status, 303, "{}", two_fields.body);
    assert_eq!(stored(p, ALICE).await, look("light", "violet", "system"));
    let page = get(&alice, &url("/admin/members")).await;
    assert!(page.body.contains("<html lang=\"en\" class=\"mode-light scheme-violet font-system\">"), "{}", page.body);

    // Bob is unaffected.
    assert_eq!(stored(p, BOB).await, look("system", "sky", "hyperlegible"));
    let bobs = get(&bob, &url("/admin/members")).await;
    assert!(bobs.body.contains(default_html) && !bobs.body.contains("mode-dark") && !bobs.body.contains("font-system"), "{}", bobs.body);

    // Signed out: no choice applies; the page is drawn in the device's
    // mode and font.
    let stranger = Browser::new();
    let login = stranger.get(&url("/admin/login")).await;
    assert!(login.body.contains("<html lang=\"en\">") && !login.body.contains("font-"), "{}", login.body);

    // The fonts and their licences, to anyone, byte for byte, cached for a
    // year; anything else under the folder is the plain 404 page.
    let raw = reqwest::Client::new();
    for (name, content_type, bytes) in FONTS {
        let r = raw.get(url(&format!("/admin/static/fonts/{name}"))).send().await.expect("font");
        assert_eq!(r.status().as_u16(), 200, "{name}");
        let header = |h: &str| r.headers().get(h).and_then(|v| v.to_str().ok()).unwrap_or_default().to_string();
        assert_eq!(header("content-type"), *content_type, "{name}");
        assert_eq!(header("cache-control"), "public, max-age=31536000, immutable", "{name}");
        assert_eq!(header("content-security-policy"), CSP, "{name}");
        assert_eq!(header("x-content-type-options"), "nosniff", "{name}");
        assert!(*content_type == WOFF2 || *content_type == TEXT, "{name}");
        assert_eq!(r.bytes().await.expect("bytes").as_ref(), *bytes, "{name}");
    }
    for path in ["nothing.woff2", "OFL.txt", "atkinson-hyperlegible-1.006-400.woff", "%FF", "..%2Fadmin.css"] {
        let r = stranger.get(&url(&format!("/admin/static/fonts/{path}"))).await;
        assert_eq!(r.status, 404, "{path}");
        assert!(r.body.contains("There is no page here."), "{path}: {}", r.body);
        assert_eq!(r.headers.get("cache-control").and_then(|v| v.to_str().ok()), Some(NO_STORE), "{path}");
    }

    // A personal look is not a command: nothing queued, nothing audited.
    assert_eq!(count(p, "SELECT count(*) FROM admin_commands").await, 0);
    h.db.drop_db().await;
}

#[tokio::test]
async fn menu_order_and_product_name() {
    let Some(h) = harness("admin_menu_order").await else {
        return;
    };
    let alice = complete_setup(&h, ALICE).await;
    let home = get(&alice, &format!("{}/admin", h.base)).await;
    assert_eq!(home.status, 200, "{}", home.body);
    assert_eq!(
        menu(&home.body),
        ["Home", "Members", "Health", "Jobs", "Problems", "Stats", "Instance", "Blocks", "Labels", "Communities", "Refract accounts", "Look up", "Audit log", "Operators", "Settings"]
    );
    assert!(home.body.contains("aria-current=\"page\">Home</a>"), "{}", home.body);
    // The name: the brand, the title, Home's card.
    assert!(home.body.contains("<span>Horizon-Prism</span>"), "{}", home.body);
    assert!(home.body.contains("Home · Horizon-Prism admin</title>"), "{}", home.body);
    assert!(home.body.contains("<h2>Horizon-Prism</h2>"), "{}", home.body);
    // Blocks: its menu entry and title, Farsight still named in the text.
    let blocks = get(&alice, &format!("{}/admin/farsight", h.base)).await;
    assert!(blocks.body.contains("aria-current=\"page\">Blocks</a>") && blocks.body.contains("<title>Blocks · Horizon-Prism admin</title>"), "{}", blocks.body);
    assert!(!blocks.body.contains("Blocks (Farsight)") && blocks.body.contains("Farsight"), "{}", blocks.body);
    // Sign-out stays after the menu.
    let nav_end = home.body.find("</nav>").expect("nav");
    assert!(home.body[nav_end..].contains("Sign out"));
    // The sign-in and setup pages carry the name too.
    let stranger = Browser::new();
    let login = stranger.get(&format!("{}/admin/login", h.base)).await;
    assert!(login.body.contains("Sign in · Horizon-Prism admin</title>") && login.body.contains("<span>Horizon-Prism</span>"), "{}", login.body);
    h.db.drop_db().await;
}
