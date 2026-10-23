//! Restored profiles and author feeds (refract design §7.1, §7.2, §13:
//! T-8a, T-8o, T-8r, T-8u, T-8x).

use std::sync::Arc;

use serde_json::{Value, json};

use crate::aggregations::ScopeSets;
use crate::hydration::testing::{FixedUpstream, actor, exec, hydrator_with, post};
use crate::hydration::{Hydrator, Viewer};
use crate::indexing::PublicMode;
use crate::test_support::fresh_db;
use crate::xrpc::pools::Requester;

const R: &str = "did:plc:restoredaaaaaaaaaaaaaaaa";
const BARE: &str = "did:plc:bareaaaaaaaaaaaaaaaaaaaa";
const OTHER: &str = "did:plc:otheraaaaaaaaaaaaaaaaaaa";

fn sets() -> ScopeSets {
    let mut s = ScopeSets::default();
    s.public.mode = PublicMode::Off;
    let restored = Arc::make_mut(&mut s.restored);
    restored.insert(R.into(), "community-a".into());
    restored.insert(BARE.into(), "community-a".into());
    s
}

async fn feed(h: &Hydrator, filter: &str) -> Value {
    let (items, next) = h.refract_author_items(R, filter, None, 50, true).await.expect("items");
    h.refract_feed(&Viewer::anonymous(), &Requester::test(), &items, next.as_deref(), 25).await.expect("feed").inner().clone()
}

/// T-8o, T-8u, T-8a: off mode, a non-member in R with ten stored posts:
/// `getAuthorFeed` returns them; the filters apply; its repost of a post
/// Prism does not store shows the subject's view; its reply to a
/// non-member shows the parent; a repost whose subject cannot be fetched is
/// left out; the pinned post comes first; an image's URL is on its PDS.
#[tokio::test]
async fn t8o_t8u_author_feed() {
    let Some(db) = fresh_db("refract_author_feed").await else {
        return;
    };
    let p = &db.pool;
    actor(p, R, "r.test", "Restored").await;
    for i in 0..10 {
        post(p, R, &format!("3kp{i:02}"), "hi", None, None).await;
    }
    let parent = format!("at://{OTHER}/app.bsky.feed.post/3kparent");
    post(p, R, "3kreply", "a reply", Some(&parent), Some(&parent)).await;
    exec(p, &format!("UPDATE posts SET embed = '{{\"$type\": \"app.bsky.embed.images\", \"images\": [{{\"alt\": \"\", \"image\": {{\"$type\": \"blob\", \"ref\": {{\"$link\": \"bafyimg\"}}, \"mimeType\": \"image/png\", \"size\": 1}}}}]}}' WHERE uri = 'at://{R}/app.bsky.feed.post/3kp00'")).await;
    let shown = format!("at://{OTHER}/app.bsky.feed.post/3kshown");
    let gone = format!("at://{OTHER}/app.bsky.feed.post/3kgone");
    for (rkey, subject) in [("3krp1", &shown), ("3krp2", &gone)] {
        exec(p, &format!("INSERT INTO reposts (uri, author_did, subject_uri, rev, created_at) VALUES ('at://{R}/app.bsky.feed.repost/{rkey}', '{R}', '{subject}', '3k', now())")).await;
    }
    exec(p, &format!("UPDATE actors SET profile_record = '{{\"pinnedPost\": {{\"uri\": \"at://{R}/app.bsky.feed.post/3kp05\", \"cid\": \"c\"}}}}'::jsonb WHERE did = '{R}'")).await;
    let pv = |uri: &str| json!({"uri": uri, "cid": "c", "author": {"did": OTHER, "handle": "o.test"}, "record": {"text": "upstream"}, "indexedAt": "2026-01-01T00:00:00Z"});
    let mut up = FixedUpstream::default();
    up.posts.insert(shown.clone(), pv(&shown));
    up.posts.insert(parent.clone(), pv(&parent));
    let h = hydrator_with(p, sets(), Arc::new(up));
    let all = feed(&h, "posts_with_replies").await;
    let items = all["feed"].as_array().expect("feed");
    assert_eq!(items.len(), 12, "the pin, ten other posts, one repost: {all}");
    assert_eq!(items[0]["reason"]["$type"], "app.bsky.feed.defs#reasonPin");
    let repost = items.iter().find(|i| i["reason"]["$type"] == "app.bsky.feed.defs#reasonRepost").expect("repost");
    assert_eq!(repost["post"]["uri"], json!(shown));
    assert!(!all.to_string().contains("3kgone"), "an unfetchable subject drops the repost");
    let reply = items.iter().find(|i| i["post"]["uri"].as_str().is_some_and(|u| u.ends_with("3kreply"))).expect("reply");
    assert_eq!(reply["reply"]["parent"]["record"]["text"], "upstream");
    let image = items.iter().find(|i| i["post"]["uri"].as_str().is_some_and(|u| u.ends_with("3kp00"))).expect("image");
    assert!(image["post"]["embed"]["images"][0]["thumb"].as_str().expect("thumb").starts_with("https://pds.example.com/"), "{image}");
    assert_eq!(image["post"]["restored"]["community"], "community-a");
    let no_replies = feed(&h, "posts_no_replies").await;
    assert_eq!(no_replies["feed"].as_array().expect("feed").len(), 11);
    let media = feed(&h, "posts_with_media").await;
    assert_eq!(media["feed"].as_array().expect("feed").len(), 2, "the image post (and the pin)");
    db.drop_db().await;
}

/// T-8r, T-8x: a restored DID with no stored profile record renders from
/// the remote tiers (Slingshot's record as the last), whatever the tier
/// mode.
#[tokio::test]
async fn t8r_t8x_remote_profiles_for_restored_dids() {
    let Some(db) = fresh_db("refract_remote_profile").await else {
        return;
    };
    let p = &db.pool;
    exec(p, &format!("INSERT INTO actors (did, handle, pds_endpoint) VALUES ('{BARE}', 'bare.test', 'https://pds.example.com')")).await;
    let mut up = FixedUpstream::default();
    up.records.insert(
        format!("at://{BARE}/app.bsky.actor.profile/self"),
        json!({"uri": format!("at://{BARE}/app.bsky.actor.profile/self"), "cid": "c", "value": {"displayName": "From the record", "avatar": {"ref": {"$link": "bafyav"}}}}),
    );
    up.batch_absent = true;
    let h = hydrator_with(p, sets(), Arc::new(up));
    let req = Requester::test();
    let v = h.get_profile(&Viewer::anonymous(), &req, BARE).await.expect("db").expect("ok");
    assert_eq!(v.inner()["displayName"], "From the record");
    assert_eq!(v.inner()["handle"], "bare.test");
    assert!(v.inner()["avatar"].as_str().expect("avatar").starts_with("https://pds.example.com/"));
    assert_eq!(v.inner()["restored"]["community"], "community-a");
    db.drop_db().await;
}
