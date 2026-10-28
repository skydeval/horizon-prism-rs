//! T-F4 (admin-ui-live-feed design §11): private content never reaches
//! the admin UI's live feed. Records are written through the real writer
//! with the real private tier and at-rest hooks, and an operator's stream
//! and Home are read over HTTP.

use std::time::Duration;

use serde_json::{Value, json};

use super::*;
use crate::admin::live::Hub;
use crate::admin::test_support::{OPERATOR, OPERATOR_PDS, Sse, page_id, serve, session};

/// A private block: skipped since they were removed (skydeval,
/// 2026-10-24), and its subject is never on the feed either.
const NSID_PRIVATE_BLOCK: &str = "tools.kryphocron.graph.block";
const PRIVATE_TEXT: &str = "marker-private-text-41f3";
const AUDIENCE_MEMBER: &str = "did:plc:mmmmmmmmmmmmmmmmmmmmmmmm";
const BLOCKED: &str = "did:plc:kkkkkkkkkkkkkkkkkkkkkkkk";
const HIDDEN_REPLY: &str = "at://did:plc:hhhhhhhhhhhhhhhhhhhhhhhh/app.bsky.feed.post/3khiddenmark";
const PUBLIC_TEXT: &str = "marker public text 7c2e";

/// Every `lines` and `counts` payload of `stream` within `within`.
async fn payloads(stream: &mut Sse, within: Duration) -> Vec<(String, String)> {
    stream.collect(within).await.into_iter().filter(|(n, _)| n == "lines" || n == "counts").collect()
}

fn lines(events: &[(String, String)]) -> Vec<Value> {
    events
        .iter()
        .filter(|(n, _)| n == "lines")
        .flat_map(|(_, d)| serde_json::from_str::<Vec<Value>>(d).expect("lines"))
        .collect()
}

#[tokio::test]
async fn t_f4_private_content_never_in_a_line() {
    let Some(db) = fresh_db("live_feed_t_f4").await else {
        return;
    };
    let mut f = fixture(&db.pool, "live_feed_t_f4").await;
    let hub = Hub::new(true);
    f.writer = f.writer.clone().with_feed(hub.tap());
    let mut served = serve(&f.pool, hub.clone(), None).await;
    let cookie = session(&f.pool, OPERATOR, OPERATOR_PDS).await;
    let mut stream = Sse::open(&served.base, &format!("/admin/live?page={}", page_id()), Some(&cookie)).await;
    assert_eq!(stream.status, 200);

    // The private records, each carrying a marker, and a public post.
    let audience = format!("at://{ALICE}/{NSID_AUDIENCE}/3kaudmarker1");
    let audience_record = json!({"$type": NSID_AUDIENCE, "mode": "list", "members": [AUDIENCE_MEMBER], "createdAt": "2026-09-26T00:00:00Z"});
    // Held here; a repository copy through the writer is skipped, and its
    // line is an `other` one (private-audiences design §4.5).
    f.ingest(ALICE, NSID_AUDIENCE, "3kaudmarker1", audience_record.clone()).await;
    let options = ApplyOptions {
        advance_applied_rev: false,
        dead_letter_readiness: false,
        attempts: 1,
        live: false,
        subscription_rules: false,
        source: WriteSource::Firehose,
    };
    let copy = RawOp {
        action: Action::Create,
        collection: NSID_AUDIENCE.into(),
        rkey: "3kaudmarker1".into(),
        cid: Some("bafyaudmarker".into()),
        record: Some(audience_record),
    };
    let report = f.writer.apply(ALICE, REV, &[copy], options).await.expect("apply");
    assert_eq!(report.outcomes, [OpOutcome::Skipped("kryphocron_unproduced")]);
    let encoded = f.encoded_post(ALICE, "3kprivmarker", PRIVATE_TEXT, Some(&audience)).await;
    let ciphertext = encoded["encodedContent"].as_str().expect("hex").to_string();
    f.ingest(ALICE, NSID_POST_PRIVATE, "3kprivmarker", encoded).await;
    f.ingest(ALICE, NSID_PRIVATE_BLOCK, "3kblockmark1", json!({"$type": NSID_PRIVATE_BLOCK, "subject": BLOCKED, "createdAt": "2026-09-26T00:00:00Z"}))
        .await;
    let private_uri = format!("at://{ALICE}/{NSID_POST_PRIVATE}/3kprivmarker");
    f.ingest(
        ALICE,
        "app.bsky.feed.threadgate",
        "3kprivmarker",
        json!({"$type": "app.bsky.feed.threadgate", "post": private_uri, "allow": [], "hiddenReplies": [HIDDEN_REPLY], "createdAt": "2026-09-26T00:00:00Z"}),
    )
    .await;
    f.ingest(BOB, "app.bsky.feed.post", "3kpublicmark", json!({"text": PUBLIC_TEXT, "createdAt": "2026-09-26T00:00:00Z"})).await;

    let events = payloads(&mut stream, Duration::from_millis(2500)).await;
    let home = reqwest::Client::new()
        .get(format!("{}/admin", served.base))
        .header("cookie", &cookie)
        .send()
        .await
        .expect("home");
    assert_eq!(home.status(), 200);
    let home = home.text().await.expect("body");
    let markers: [&str; 6] = [PRIVATE_TEXT, &ciphertext, AUDIENCE_MEMBER, BLOCKED, HIDDEN_REPLY, "3khiddenmark"];
    for (name, data) in &events {
        for m in markers {
            assert!(!data.contains(m), "{name} carries a private marker: {data}");
        }
    }
    for m in markers {
        assert!(!home.contains(m), "Home carries a private marker");
    }
    // The private kinds are on the feed, with the author only.
    let all = lines(&events);
    let skipped = all.iter().find(|l| l["k"] == "other" && l["did"] == ALICE).unwrap_or_else(|| panic!("the audience copy's line: {all:?}"));
    assert!(skipped.get("text").is_none() && skipped.get("o").is_some(), "{skipped}");
    for kind in ["private-post", "private-post-gate"] {
        let line = all.iter().find(|l| l["k"] == kind).unwrap_or_else(|| panic!("a {kind} line: {all:?}"));
        assert_eq!(line["did"], ALICE, "{kind}");
        assert_eq!(line["who"], "alice.test", "{kind}");
        assert!(line.get("text").is_none(), "{kind} has no text");
        let keys: Vec<&String> = line.as_object().expect("object").keys().collect();
        assert!(keys.iter().all(|k| ["n", "t", "src", "relay", "act", "k", "o", "f", "i", "did", "who"].contains(&k.as_str())), "{keys:?}");
    }
    // A line that was not indexed (skipped, rejected, a duplicate) never
    // has text and is never on the indexed ticker.
    assert!(all.iter().filter(|l| l.get("o").is_some()).all(|l| l.get("text").is_none() && l.get("i").is_none()), "{all:?}");
    // The public marker is in its own line's snippet and nowhere else.
    let public: Vec<&Value> = all.iter().filter(|l| l.to_string().contains(PUBLIC_TEXT)).collect();
    assert_eq!(public.len(), 1, "{all:?}");
    assert_eq!((public[0]["k"].as_str(), public[0]["did"].as_str(), public[0]["text"].as_str()), (Some("post"), Some(BOB), Some(PUBLIC_TEXT)));
    assert!(!events.iter().filter(|(n, _)| n == "counts").any(|(_, d)| d.contains(PUBLIC_TEXT)));
    drop(stream);
    // The backlog a reload starts with (second pass) holds the same lines
    // under the same rule: no private marker, text on the public post only.
    let mut reload = Sse::open(&served.base, &format!("/admin/live?page={}", page_id()), Some(&cookie)).await;
    assert_eq!(reload.status, 200);
    let (name, data) = reload.next(Duration::from_secs(3)).await.expect("the backlog");
    assert_eq!(name, "backlog");
    for m in markers {
        assert!(!data.contains(m), "the backlog carries a private marker: {data}");
    }
    let kept: Vec<Value> = serde_json::from_str(&data).expect("lines");
    assert!(kept.iter().any(|l| l["k"] == "private-post"), "{kept:?}");
    assert!(kept.iter().filter(|l| l.get("text").is_some()).all(|l| l["k"] == "post" && l["text"] == PUBLIC_TEXT && l.get("o").is_none()), "{kept:?}");
    drop(reload);
    served.shut_down();

    // Snippets off: no line has a `text` field at all.
    let quiet = Hub::new(false);
    f.writer = f.writer.clone().with_feed(quiet.tap());
    let mut served = serve(&f.pool, quiet.clone(), None).await;
    let mut stream = Sse::open(&served.base, &format!("/admin/live?page={}", page_id()), Some(&cookie)).await;
    assert_eq!(stream.status, 200);
    f.ingest(BOB, "app.bsky.feed.post", "3kpublicmar2", json!({"text": PUBLIC_TEXT, "createdAt": "2026-09-26T00:00:00Z"})).await;
    let events = payloads(&mut stream, Duration::from_millis(1500)).await;
    let all = lines(&events);
    assert!(all.iter().any(|l| l["did"] == BOB && l["k"] == "post"), "{all:?}");
    assert!(all.iter().all(|l| l.get("text").is_none()), "{all:?}");
    assert!(!events.iter().any(|(_, d)| d.contains(PUBLIC_TEXT)));
    drop(stream);
    served.shut_down();
    db.drop_db().await;
}
