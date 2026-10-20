//! Stats over HTTP (admin-ui design §6.5, §10; T-A14): the disks (two
//! paths on one filesystem shown once, a missing mount "not visible"), the
//! storage figures, the counts from the sampler's series and the scope
//! sets, the private tier's figures through its own labels, endpoint usage
//! with the never-used list, and the Disk check and the fullest-disk tile.

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
    for text in [
        "aria-current=\"page\">Stats</a>",
        // One filesystem, shown once; Redis's mount is missing.
        "the database (Postgres)</h3>",
        "Prism cannot see the disk of Redis.",
        "% used",
        // Storage and counts.
        "3.0 GB",
        "about 12,345",
        "Followed by a member",
        "<span class=\"v\">40</span>",
        "not measured yet",
        "Members ready (first sync done)",
        // Private posts through the private tier's labels.
        "about 77",
        "With images",
        "Read in the new form since Prism started</span><span class=\"v\">7</span>",
        "Private posts",
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
    assert_eq!(page.body.matches("<h3>Disk holding").count(), 1, "{}", page.body);
    // The private table is listed under its label, its own name in grey.
    assert!(page.body.contains(&format!("Private posts<br><span class=\"muted tech\">{private}</span>")), "{}", page.body);

    // The Disk check on Health, and the fullest disk on Home.
    let health = get(&alice, &format!("{}/admin/health", h.base)).await;
    assert!(health.body.contains("<td>Disk</td>") && health.body.contains("The disk holding"), "{}", health.body);
    let home = get(&alice, &format!("{}/admin", h.base)).await;
    assert!(home.body.contains("Fullest disk") && home.body.contains("% used), "), "{}", home.body);
    h.db.drop_db().await;
}
