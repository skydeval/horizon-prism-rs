//! Restored timelines and replies (refract design §7.1, §13: T-12,
//! T-12b, T-14, T-14b).

use std::sync::Arc;
use std::time::Duration;

use serde_json::json;

use crate::aggregations::ScopeSets;
use crate::hydration::testing::{FixedUpstream, actor, exec, hydrator_with, post};
use crate::hydration::{Hydrator, Viewer};
use crate::indexing::PublicMode;
use crate::test_support::fresh_db;
use crate::xrpc::pools::Requester;

const MEMBER: &str = "did:plc:memberaaaaaaaaaaaaaaaaaa";
const R1: &str = "did:plc:restoredoneaaaaaaaaaaaaa";
const R2: &str = "did:plc:restoredtwoaaaaaaaaaaaaa";
const OTHER: &str = "did:plc:otheraaaaaaaaaaaaaaaaaaa";

fn sets() -> ScopeSets {
    let mut s = ScopeSets::default();
    s.public.mode = PublicMode::Off;
    s.members.insert(MEMBER.into());
    s.scope.insert(MEMBER.into());
    let restored = Arc::make_mut(&mut s.restored);
    restored.insert(R1.into(), "community-a".into());
    restored.insert(R2.into(), "community-a".into());
    s
}

fn restored() -> Vec<String> {
    vec![R1.to_string(), R2.to_string()]
}

async fn timeline(h: &Hydrator) -> Vec<String> {
    let (items, next) = h.refract_timeline_items(MEMBER, &restored(), None, 50).await.expect("items");
    let v = h.refract_feed(&Viewer { did: Some(MEMBER.into()), hydrate: true }, &Requester::test(), &items, next.as_deref(), 25).await.expect("feed");
    v.inner()["feed"]
        .as_array()
        .expect("feed")
        .iter()
        .map(|i| {
            let uri = i["post"]["uri"].as_str().unwrap_or_default();
            if i["reason"]["$type"] == "app.bsky.feed.defs#reasonRepost" { format!("repost {uri}") } else { uri.to_string() }
        })
        .collect()
}

/// T-12, T-12b: the member's timeline holds the top-level posts of the
/// restored DIDs it follows, their replies to accounts it follows, and
/// their reposts; not a reply to an account it doesn't follow, nor a
/// restored DID it doesn't follow. A repost mute drops the repost; a full
/// mute of a repost's subject author drops the repost too.
#[tokio::test]
async fn t12_timeline() {
    let Some(db) = fresh_db("refract_timeline").await else {
        return;
    };
    let p = &db.pool;
    for (did, handle) in [(R1, "r1.test"), (R2, "r2.test"), (OTHER, "o.test"), (MEMBER, "m.test")] {
        actor(p, did, handle, handle).await;
    }
    exec(p, &format!("INSERT INTO follows (uri, author_did, subject_did, rev, created_at) VALUES ('at://{MEMBER}/app.bsky.graph.follow/1', '{MEMBER}', '{R1}', '3k', now())")).await;
    let top = post(p, R1, "3ktop", "top", None, None).await;
    let theirs = post(p, OTHER, "3kother", "other", None, None).await;
    let to_other = post(p, R1, "3kreplyother", "reply", Some(&theirs), Some(&theirs)).await;
    let to_self = post(p, R1, "3kreplyself", "self reply", Some(&top), Some(&top)).await;
    let r2_post = post(p, R2, "3kr2", "unfollowed", None, None).await;
    exec(p, &format!("INSERT INTO reposts (uri, author_did, subject_uri, rev, created_at) VALUES ('at://{R1}/app.bsky.feed.repost/3krp', '{R1}', '{r2_post}', '3k', now())")).await;
    let h = hydrator_with(p, sets(), Arc::new(FixedUpstream::default()));
    let got = timeline(&h).await;
    assert!(got.contains(&top) && got.contains(&to_self), "{got:?}");
    assert!(got.contains(&format!("repost {r2_post}")), "{got:?}");
    assert!(!got.contains(&to_other), "a reply to an account the member doesn't follow");
    assert!(!got.contains(&r2_post), "a restored DID the member doesn't follow");
    exec(p, &format!("INSERT INTO mutes (viewer_did, subject_did, scope) VALUES ('{MEMBER}', '{R2}', 'full')")).await;
    assert!(!timeline(&h).await.iter().any(|u| u.starts_with("repost")), "a muted subject author drops the repost");
    exec(p, "DELETE FROM mutes").await;
    exec(p, &format!("INSERT INTO mutes (viewer_did, subject_did, scope) VALUES ('{MEMBER}', '{R1}', 'reposts')")).await;
    let got = timeline(&h).await;
    assert!(!got.iter().any(|u| u.starts_with("repost")) && got.contains(&top), "{got:?}");
    db.drop_db().await;
}

async fn replies(h: &Hydrator, root: &str, viewer: &Viewer) -> Vec<String> {
    let v = h.refract_replies(viewer, &Requester::test(), root, &restored(), None, Duration::from_secs(1)).await.expect("replies");
    v.inner()["posts"].as_array().expect("posts").iter().filter_map(|p| p["uri"].as_str().map(str::to_string)).collect()
}

/// T-14, T-14b: `getReplies` returns the restored replies under the root;
/// a threadgate's `mentionRule` admits only a mentioned replier, and its
/// `hiddenReplies` hides a reply from everyone but its author. A root with
/// no threadgate admits every reply.
#[tokio::test]
async fn t14_replies_and_the_threadgate() {
    let Some(db) = fresh_db("refract_replies").await else {
        return;
    };
    let p = &db.pool;
    let gated = format!("at://{OTHER}/app.bsky.feed.post/3kgatedroot");
    let open = format!("at://{OTHER}/app.bsky.feed.post/3kopenroot");
    let a = post(p, R1, "3ka", "mentioned", Some(&gated), Some(&gated)).await;
    let hidden = post(p, R1, "3kb", "hidden", Some(&a), Some(&gated)).await;
    let c = post(p, R2, "3kc", "not mentioned", Some(&gated), Some(&gated)).await;
    let d = post(p, R2, "3kd", "open", Some(&open), Some(&open)).await;
    let mut up = FixedUpstream::default();
    up.records.insert(
        gated.clone(),
        json!({"uri": gated, "cid": "c", "value": {"text": "hi", "facets": [{"index": {"byteStart": 0, "byteEnd": 2}, "features": [{"$type": "app.bsky.richtext.facet#mention", "did": R1}]}]}}),
    );
    up.records.insert(
        format!("at://{OTHER}/app.bsky.feed.threadgate/3kgatedroot"),
        json!({"uri": "x", "cid": "c", "value": {"post": gated, "allow": [{"$type": "app.bsky.feed.threadgate#mentionRule"}], "hiddenReplies": [hidden.clone()]}}),
    );
    let h = hydrator_with(p, sets(), Arc::new(up));
    let anon = Viewer { did: Some(MEMBER.into()), hydrate: false };
    let got = replies(&h, &gated, &anon).await;
    assert_eq!(got, std::slice::from_ref(&a), "only the mentioned replier, less the hidden reply");
    let author = Viewer { did: Some(R1.into()), hydrate: false };
    let got = replies(&h, &gated, &author).await;
    assert!(got.contains(&hidden), "a hidden reply is not hidden from its author: {got:?}");
    assert!(!got.contains(&c));
    assert_eq!(replies(&h, &open, &anon).await, [d], "no threadgate: every reply");
    db.drop_db().await;
}
