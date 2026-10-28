//! Instance (`/admin/instance`) and the lexicons it serves, over HTTP:
//! operator sign-in, every section from fixture state (relays by label and
//! host, the refracted communities, the notices, the methods, the lexicons,
//! the figures), no live feed, and the lexicon route.

use chrono::{TimeDelta, Utc};

use super::*;
use crate::admin::health::{HealthInputs, MetricsText, Ring, Snapshot};
use crate::config::{LabelSource, RefractCommunity};
use crate::indexing::{PublicMode, PublicState};
use crate::serve::health::AuditLatch;

/// Metrics that say what the test says.
struct Fixed(String);

impl MetricsText for Fixed {
    fn render(&self) -> String {
        self.0.clone()
    }
}

const METRICS: &str = "firehose_worker_delay_seconds{relay=\"relay-a\"} 5\n\
    firehose_worker_delay_seconds{relay=\"relay-b\"} +Inf\n\
    private_read_degraded 1\n\
    firehose_consumer_lag_entries 200000\n\
    db_table_rows{table=\"posts\"} 12345\n\
    db_table_rows{table=\"actors\"} 678\n\
    scope_dids_total 77\n\
    members_total 3\n";

/// A page past the ten-pages-a-second limit.
async fn get(b: &Browser, url: &str) -> Page {
    tokio::time::sleep(Duration::from_millis(150)).await;
    b.get(url).await
}

fn config() -> crate::config::FileConfig {
    let mut file = test_support::no_auto_join();
    file.firehose.relay_labels = [
        ("wss://relay-a.example/xrpc/com.atproto.sync.subscribeRepos", "relay-a"),
        ("wss://relay-b.example/xrpc/com.atproto.sync.subscribeRepos", "relay-b"),
    ]
    .into_iter()
    .map(|(u, l)| (u.to_string(), l.to_string()))
    .collect();
    file.firehose.private_relays = vec!["relay-b".to_string()];
    file.identity.plc_url = "https://plc.example".to_string();
    file.farsight.url = Some("https://blocks.example".to_string());
    file.refract.enabled = true;
    file.refract.communities = vec![
        RefractCommunity {
            name: "community-a".to_string(),
            pds_patterns: vec!["pds.community-a.example".to_string(), "*.community-a.example".to_string()],
            tier: 2,
            appview_network: None,
            labeler_did: Some("did:plc:labelerofcommunitya000".to_string()),
            label_source: Some(LabelSource::Labeler),
            gone_labels: crate::config::default_gone_labels(),
            enabled: true,
        },
        RefractCommunity {
            name: "community-b".to_string(),
            pds_patterns: vec!["community-b.example".to_string()],
            tier: 1,
            appview_network: Some("network-b".to_string()),
            labeler_did: None,
            label_source: None,
            gone_labels: crate::config::default_gone_labels(),
            enabled: false,
        },
    ];
    file
}

/// Six readings a minute apart: the stream gains 6,000 entries a minute.
fn ring() -> Arc<Ring> {
    let ring = Arc::new(Ring::default());
    for m in (0..=5).rev() {
        let added = 6000 * (5 - m);
        ring.push(Snapshot::from_text(&format!("firehose_stream_entries_added_total {added}\n"), Utc::now() - TimeDelta::minutes(m)));
    }
    ring
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn instance_page_and_lexicons() {
    let audit = AuditLatch::default();
    audit.latch();
    let inputs = HealthInputs { metrics: Some(Arc::new(Fixed(METRICS.to_string()))), ring: ring(), audit: Some(audit), schema: Some(Default::default()), ..HealthInputs::default() };
    let Some(h) = harness_config("admin_instance_page", quick(), true, inputs, config()).await else {
        return;
    };
    let url = |path: &str| format!("{}{path}", h.base);
    // A method some router registered, so the page lists it. Not under
    // `app`: t_a12 routes every `app` method of the process-wide registry.
    let _ = crate::admin::usage::path("test", "test.instance.served");
    {
        let mut sets = h.scope.write_guard().await;
        h.scope.set_public(&mut sets, PublicState { mode: PublicMode::Filling, generation: 2, last_fill_since: None });
    }

    // Operator sign-in: a stranger goes to the sign-in page, and so does a
    // session of an account that is not an operator.
    let alice = complete_setup(&h, ALICE).await;
    let stranger = Browser::new();
    for path in ["/admin/instance", "/admin/lexicons/app/nearhorizon/actor/getMembership.json"] {
        let page = get(&stranger, &url(path)).await;
        assert_eq!((page.status, page.headers.get(LOCATION).and_then(|v| v.to_str().ok())), (303, Some("/admin/login")), "{path}");
    }
    let not_operator = test_support::session(h.pool(), BOB, PDS).await;
    tokio::time::sleep(Duration::from_millis(150)).await;
    let refused = stranger.http.get(url("/admin/instance")).header("cookie", &not_operator).send().await.expect("request");
    assert_eq!(
        (refused.status().as_u16(), refused.headers().get(LOCATION).and_then(|v| v.to_str().ok())),
        (401, None),
        "an account that is not an operator"
    );
    assert_eq!(count(h.pool(), &format!("SELECT count(*) FROM admin_sessions WHERE did = '{BOB}'")).await, 0, "its session is cut");

    let page = get(&alice, &url("/admin/instance")).await;
    assert_eq!(page.status, 200, "{}", page.body);
    assert_admin_headers(&page, "/admin/instance");
    let body = &page.body;
    for text in [
        // About.
        "<h1>Instance</h1>",
        "Horizon-Prism, an AT Protocol AppView written in Rust.",
        env!("CARGO_PKG_VERSION"),
        "did:web:prism.example.com",
        // Status: the probes, the audit guard with its cause, each relay by
        // label with its host, the input rate, lookups and block lists.
        "Prism can reach its database.",
        "No data: Redis is not connected to these pages.",
        "Prism stopped because its private-post audit log failed.",
        "<td>relay-a<",
        "<td>relay-a.example</td>",
        "5 s behind",
        "<td>relay-b.example</td>",
        "never reported on this database",
        "private posts wait for it",
        "100.0 events a second, over the last 5 minutes",
        "off (the PLC directory is asked)",
        "The PLC directory is plc.example.",
        "Farsight at blocks.example",
        // Notices.
        "Private posts are catching up",
        "Prism is catching up with the network",
        "Public data is being turned on.",
        "Search is still being prepared.",
        // What it does here.
        "filling: public data is being refilled",
        "This instance's own labels",
        "None: Prism follows no labeler's stream.",
        "community-a's labeler, for its members' restored accounts",
        "did:plc:labelerofcommunitya000",
        "read through its labeler",
        "Refract is on.",
        "<td>community-a</td>",
        "pds.community-a.example",
        "*.community-a.example",
        "<td>community-b</td>",
        "<code class=\"chip\">community-b.example</code>",
        "network-b",
        "Not configured",
        // For developers.
        "test.instance.served",
        "Answered \"not offered here\"</h3>",
        "app.bsky.video.uploadVideo",
        "app.bsky.feed.getTimeline",
        "app.nearhorizon.actor.getMembership",
        "href=\"/admin/lexicons/app/nearhorizon/actor/getMembership.json\"",
        // Figures.
        "about 12,345",
        "about 678",
        "Accounts Prism follows",
    ] {
        assert!(body.contains(text), "{text}: {body}");
    }
    // The served method is under its namespace, not among the 501s.
    let served_at = body.find("test.instance.served").expect("served");
    let not_offered_at = body.find("Answered \"not offered here\"</h3>").expect("heading");
    assert!(served_at < not_offered_at, "{body}");
    assert!(body.contains("<code>test.instance</code>"), "{body}");
    // No live feed: no script, no stream.
    assert!(!body.contains("<script") && !body.contains("/live") && !body.contains("live.js"), "{body}");
    assert!(!page.headers.get("content-security-policy").and_then(|v| v.to_str().ok()).unwrap_or_default().contains("script-src"));

    // A known lexicon is JSON; an unknown path is a 404.
    let lex = get(&alice, &url("/admin/lexicons/app/nearhorizon/actor/getMembership.json")).await;
    assert_eq!(lex.status, 200, "{}", lex.body);
    assert_eq!(lex.headers.get("content-type").and_then(|v| v.to_str().ok()), Some("application/json"));
    let v: Value = serde_json::from_str(&lex.body).expect("json");
    assert_eq!(v["id"], "app.nearhorizon.actor.getMembership");
    assert_admin_headers(&lex, "lexicon");
    for missing in ["/admin/lexicons/app/nearhorizon/actor/nothing.json", "/admin/lexicons/app/nearhorizon/actor"] {
        let page = get(&alice, &url(missing)).await;
        assert_eq!(page.status, 404, "{missing}");
    }
    h.db.drop_db().await;
}
