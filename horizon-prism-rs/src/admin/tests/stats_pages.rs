//! Stats, Storage and Private posts over HTTP: on Storage the disks (two
//! paths on one filesystem shown once, a missing mount "not visible") and
//! the storage figures; on Private posts the private tier's figures through
//! its own labels; on Stats one line of each, the counts from the sampler's
//! series and the scope sets, endpoint usage with the never-used list; and
//! the Disk check and the fullest-disk tile, both leading to Storage, and
//! Health's private-post line, leading to Private posts.

use super::*;
use crate::admin::health::{HealthInputs, MetricsText};
use crate::admin::usage::Class;

/// A page past the ten-pages-a-second limit.
async fn get(b: &Browser, url: &str) -> Page {
    tokio::time::sleep(Duration::from_millis(150)).await;
    b.get(url).await
}

struct Fixed(String);

impl MetricsText for Fixed {
    fn render(&self) -> String {
        self.0.clone()
    }
}

#[tokio::test]
async fn stats_page_and_the_disk_tile() {
    // The private posts' table is named only through the private tier.
    let private = crate::private_tier::stats::admin_posts_table();
    let metrics = format!(
        "db_table_rows{{table=\"posts\"}} 12345\n\
         db_table_bytes{{table=\"posts\"}} 2000000000\n\
         db_table_rows{{table=\"{private}\"}} 77\n\
         db_table_bytes{{table=\"{private}\"}} 5000000\n\
         db_size_bytes 3000000000\n\
         scope_dids_by_hold{{hold=\"follow\"}} 40\n\
         private_read_content_total{{form=\"content\"}} 7\n"
    );
    let inputs = HealthInputs { metrics: Some(Arc::new(Fixed(metrics))), ..HealthInputs::default() };
    let Some(h) = harness_full("admin_stats_page", quick(), true, inputs).await else {
        return;
    };
    let alice = complete_setup(&h, ALICE).await;
    sqlx::query("INSERT INTO members (did, source, ready_at) VALUES ($1, 'admin', now())").bind(BOB).execute(h.pool()).await.expect("member");

    // Endpoint usage: one method used twice, one registered and never used.
    let _ = crate::admin::usage::path("test", "test.stats.neverUsed");
    let _ = crate::admin::usage::path("app", "app.bsky.feed.getTimeline");
    h.usage.load_registry();
    h.usage.record("app.bsky.feed.getTimeline", Class::Ok);
    h.usage.record("app.bsky.feed.getTimeline", Class::RateLimited);

    let page = get(&alice, &format!("{}/admin/stats", h.base)).await;
    assert_eq!(page.status, 200, "{}", page.body);
    assert_eq!(crate::admin::test_support::current_item(&page.body), Some(("Stats".to_string(), "page".to_string())), "{}", page.body);
    for text in [
        // Storage in one line, leading to its page.
        "Database: 3.0 GB (no budget set) — <a href=\"/admin/storage\">Storage</a>",
        // Counts.
        "about 12,345",
        "Followed by a member",
        "<span class=\"v\">40</span>",
        "not measured yet",
        "Members ready (first sync done)",
        // Private posts in one line, through the private tier's labels,
        // leading to their page.
        "Private posts stored: about 77 — <a href=\"/admin/private\">Private posts</a>",
        "2.0 GB",
        // Blocks and lodestar.
        "Not kept since Prism started (no member involved)",
        "not set up; Prism asks the PLC directory",
        // Endpoints.
        "Get timeline (posts and feeds)",
        "app.bsky.feed.getTimeline",
        "just now",
        "never used since Prism started",
        "test.stats.neverUsed",
    ] {
        assert!(page.body.contains(text), "{text}: {}", page.body);
    }
    assert!(!page.body.contains("<h2>Disks</h2>") && !page.body.contains("Disk holding") && !page.body.contains("Redis memory"), "{}", page.body);
    // The private table is listed under its label, its own name in grey.
    assert!(page.body.contains(&format!("Private posts<br><span class=\"muted tech\">{private}</span>")), "{}", page.body);
    assert!(!page.body.contains("With images") && !page.body.contains("Read in the new form"), "{}", page.body);

    // Private posts: what is stored and how it is read, and the settings.
    let private_page = get(&alice, &format!("{}/admin/private", h.base)).await;
    assert_eq!(private_page.status, 200, "{}", private_page.body);
    for text in [
        "<span class=\"l\">Stored</span><span class=\"v\">about 77</span>",
        "<span class=\"l\">With images</span><span class=\"v\">0</span>",
        "<span class=\"l\">With a video</span><span class=\"v\">0</span>",
        "<span class=\"l\">Read in the new form (text, links and embeds together)</span><span class=\"v\">7</span>",
        "<span class=\"l\">Read in the older form (text only)</span><span class=\"v\">unknown</span>",
        "name=\"private_read.max_chain\"",
    ] {
        assert!(private_page.body.contains(text), "{text}: {}", private_page.body);
    }
    assert!(!private_page.body.contains(private), "{}", private_page.body);

    // Storage: one filesystem, shown once; Redis's mount is missing; the
    // database against its budget.
    let storage = get(&alice, &format!("{}/admin/storage", h.base)).await;
    assert_eq!(storage.status, 200, "{}", storage.body);
    for text in [
        "the database (Postgres)</h3>",
        "Prism cannot see the disk of Redis.",
        "% used",
        "<span class=\"l\">Database (of its budget)</span><span class=\"v\">3.0 GB (no budget set)</span>",
        "Redis memory (of its limit)",
        "Account ids used",
        "Post records left to compress",
        "Dead rows in posts",
        "name=\"storage.budget_bytes\"",
    ] {
        assert!(storage.body.contains(text), "{text}: {}", storage.body);
    }
    assert_eq!(storage.body.matches("<h3>Disk holding").count(), 1, "{}", storage.body);

    // The Disk check on Health, and the fullest disk on Home, lead there.
    let health = get(&alice, &format!("{}/admin/health", h.base)).await;
    assert!(health.body.contains("<td class=\"name\">Disk</td>") && health.body.contains("The disk holding"), "{}", health.body);
    let disk = &health.body[health.body.find("<td class=\"name\">Disk</td>").expect("disk")..];
    let disk = &disk[..disk.find("</tr>").expect("row end")];
    assert!(disk.contains("<a href=\"/admin/storage\">Storage</a>"), "{disk}");
    // Health keeps private posts to one line, which leads to their page.
    assert!(
        health.body.contains("<span class=\"l\">Could not be read (last hour)</span><span class=\"v\">unknown — <a href=\"/admin/private\">Private posts</a></span>"),
        "{}",
        health.body
    );
    assert!(!health.body.contains("Quotes and mentions"), "{}", health.body);
    let home = get(&alice, &format!("{}/admin", h.base)).await;
    assert!(home.body.contains("Fullest disk") && home.body.contains("% used), "), "{}", home.body);
    assert!(home.body.contains("<span class=\"v\"><a href=\"/admin/storage\">"), "{}", home.body);
    h.db.drop_db().await;
}
