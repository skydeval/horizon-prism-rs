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
    let rows = h.refract_reply_rows(root, &restored()).await.expect("rows");
    let v = h.refract_replies(viewer, &Requester::test(), root, rows, None, Duration::from_secs(1)).await.expect("replies");
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

fn kid(name: &str) -> String {
    format!("did:plc:{name:a<24}")
}

/// A child reply's `postView`, as the cascade returns it.
fn child_view(uri: &str, author: &str, root: &str, parent: &str) -> serde_json::Value {
    json!({
        "uri": uri, "cid": "bafychild", "author": {"did": author, "handle": "kid.test"},
        "record": {"$type": "app.bsky.feed.post", "text": "kid", "createdAt": "2026-10-23T00:00:00.000Z",
                   "reply": {"root": {"uri": root, "cid": "c"}, "parent": {"uri": parent, "cid": "c"}}},
        "replyCount": 0, "repostCount": 0, "likeCount": 0, "quoteCount": 0, "indexedAt": "2026-10-23T00:00:00.000Z"
    })
}

/// RF-S7: `getReplies` adds other accounts' direct replies to a returned
/// restored reply, after the restored ones: not one whose record claims
/// another root, not one the root's threadgate hides, not one whose author
/// and the parent's author block each other, and nothing from a restored
/// reply whose `replyCount` is 0 (not asked).
#[tokio::test]
async fn rf_s7_other_accounts_replies() {
    use crate::hydration::{HydrationConfig, ReadCache};
    let Some(db) = fresh_db("refract_child_replies").await else {
        return;
    };
    let p = &db.pool;
    for (did, handle) in [(R1, "r1.test"), (R2, "r2.test"), (OTHER, "o.test")] {
        actor(p, did, handle, handle).await;
    }
    let root = format!("at://{OTHER}/app.bsky.feed.post/3kroot");
    let a = post(p, R1, "3ka", "replied to", Some(&root), Some(&root)).await;
    let b = post(p, R2, "3kb", "no replies", Some(&root), Some(&root)).await;
    exec(p, &format!("INSERT INTO post_counts (post_uri, reply_count) VALUES ('{a}', 4), ('{b}', 0)")).await;
    let (ok, forged, hidden, blocked, unasked) = (kid("kidone"), kid("kidtwo"), kid("kidthree"), kid("kidfour"), kid("kidfive"));
    let uri = |d: &str| format!("at://{d}/app.bsky.feed.post/3kchild");
    let mut up = FixedUpstream::default();
    up.records.insert(format!("at://{OTHER}/app.bsky.feed.threadgate/3kroot"), json!({"uri": "x", "cid": "c", "value": {"post": root, "hiddenReplies": [uri(&hidden)]}}));
    for d in [&ok, &hidden, &blocked] {
        up.posts.insert(uri(d), child_view(&uri(d), d, &root, &a));
    }
    up.posts.insert(uri(&forged), child_view(&uri(&forged), &forged, &format!("at://{OTHER}/app.bsky.feed.post/3kelsewhere"), &a));
    up.posts.insert(uri(&unasked), child_view(&uri(&unasked), &unasked, &root, &b));
    // Constellation: the children of `a` and of `b`, and who was asked.
    let asked: Arc<std::sync::Mutex<Vec<String>>> = Arc::default();
    let (kids_of_a, kids_of_b) = ([ok.clone(), forged.clone(), hidden.clone(), blocked.clone()], [unasked.clone()]);
    let (a2, b2, seen) = (a.clone(), b.clone(), Arc::clone(&asked));
    let router = axum::Router::new().route(
        "/links",
        axum::routing::get(move |axum::extract::RawQuery(q): axum::extract::RawQuery| {
            let q: std::collections::HashMap<String, String> = url::form_urlencoded::parse(q.unwrap_or_default().as_bytes()).into_owned().collect();
            seen.lock().expect("asked").push(q["target"].clone());
            let kids: Vec<String> = if q["target"] == a2 && q["path"] == ".reply.parent.uri" {
                kids_of_a.to_vec()
            } else if q["target"] == b2 {
                kids_of_b.to_vec()
            } else {
                Vec::new()
            };
            let records: Vec<serde_json::Value> = kids.iter().map(|d| json!({"did": d, "collection": "app.bsky.feed.post", "rkey": "3kchild"})).collect();
            async move { axum::Json(json!({"total": records.len(), "linking_records": records, "cursor": null})) }
        }),
    );
    let base = crate::test_support::serve_fake(router).await;
    let (farsight, fake) = crate::farsight::lookup::tests::engine("").await;
    fake.pair_blocks.lock().expect("pairs").insert((R1.to_string(), blocked.clone()));
    let h = Hydrator::new(
        p.clone(),
        crate::aggregations::ScopeState::new(sets(), crate::config::ScopePolicy::MembersAndOneHop, 10_000),
        Some(crate::microcosm::ConstellationClient::new(crate::net::NetClient::loopback_for_tests(), &base)),
        None,
        Arc::new(up),
        ReadCache::new(1024 * 1024),
        HydrationConfig { farsight: Some(farsight), ..crate::hydration::testing::test_config() },
    );
    let viewer = Viewer { did: Some(MEMBER.into()), hydrate: false };
    let got = replies(&h, &root, &viewer).await;
    assert_eq!(got.len(), 3, "{got:?}");
    assert!(got[..2].contains(&a) && got[..2].contains(&b), "the restored replies first: {got:?}");
    assert_eq!(got[2], uri(&ok), "{got:?}");
    assert!(!asked.lock().expect("asked").contains(&b), "a reply with no replies is not asked about");
    db.drop_db().await;
}

const VISITOR: &str = "did:plc:visitoraaaaaaaaaaaaaaaaa";

/// A Constellation answering `links` filtered to a DID: the visitor liked
/// `liked` and follows R1; for reposts it ignores the filter.
async fn own_links(liked: String) -> String {
    use axum::extract::RawQuery;
    let router = axum::Router::new().route(
        "/links",
        axum::routing::get(move |RawQuery(q): RawQuery| {
            let liked = liked.clone();
            async move {
                let q: std::collections::HashMap<String, String> = url::form_urlencoded::parse(q.unwrap_or_default().as_bytes()).into_owned().collect();
                let (target, collection, did) = (q["target"].as_str(), q["collection"].as_str(), q.get("did").map(String::as_str));
                let records = match (collection, did) {
                    ("app.bsky.feed.like", Some(VISITOR)) if target == liked => json!([{"did": VISITOR, "collection": collection, "rkey": "3klike"}]),
                    ("app.bsky.graph.follow", Some(VISITOR)) if target == R1 => json!([{"did": VISITOR, "collection": collection, "rkey": "3kfollow"}]),
                    ("app.bsky.feed.repost", _) => json!([{"did": OTHER, "collection": collection, "rkey": "3kother"}]),
                    _ => json!([]),
                };
                axum::Json(json!({"total": 1, "linking_records": records, "cursor": null}))
            }
        }),
    );
    crate::test_support::serve_fake(router).await
}

/// RF-S1: a visitor's restored views carry its own like (found), no
/// repost (a server that ignored the filter: unknown, absent), its follow
/// of the restored author, and `replyDisabled` from the root's threadgate
/// (true under a mention rule that doesn't name it, false with no gate).
#[tokio::test]
async fn rf_s1_visitor_viewer_state() {
    let Some(db) = fresh_db("refract_visitor_state").await else {
        return;
    };
    let p = &db.pool;
    for (did, handle) in [(R1, "r1.test"), (R2, "r2.test"), (OTHER, "o.test")] {
        actor(p, did, handle, handle).await;
    }
    let gated = format!("at://{OTHER}/app.bsky.feed.post/3kgatedroot");
    let open = format!("at://{OTHER}/app.bsky.feed.post/3kopenroot");
    let a = post(p, R1, "3ka", "mentioned", Some(&gated), Some(&gated)).await;
    let d = post(p, R2, "3kd", "open", Some(&open), Some(&open)).await;
    let mut up = FixedUpstream::default();
    up.records.insert(
        gated.clone(),
        json!({"uri": gated, "cid": "c", "value": {"text": "hi", "facets": [{"index": {"byteStart": 0, "byteEnd": 2}, "features": [{"$type": "app.bsky.richtext.facet#mention", "did": R1}]}]}}),
    );
    up.records.insert(
        format!("at://{OTHER}/app.bsky.feed.threadgate/3kgatedroot"),
        json!({"uri": "x", "cid": "c", "value": {"post": gated, "allow": [{"$type": "app.bsky.feed.threadgate#mentionRule"}]}}),
    );
    let base = own_links(a.clone()).await;
    let h = crate::hydration::testing::hydrator_full(p, sets(), Arc::new(up), Some(&base));
    let visitor = Viewer { did: Some(VISITOR.into()), hydrate: false };
    let req = Requester::test();
    let rows = h.refract_reply_rows(&gated, &restored()).await.expect("rows");
    let body = h.refract_replies(&visitor, &req, &gated, rows, None, Duration::from_secs(1)).await.expect("replies");
    let out = h.refract_visitor_state(VISITOR, &req, body).await.expect("state");
    let got = &out.inner()["posts"][0];
    assert_eq!(got["uri"], json!(a));
    assert!(got.get("restored").is_some(), "{got}");
    assert_eq!(got["viewer"]["like"], json!(format!("at://{VISITOR}/app.bsky.feed.like/3klike")), "{got}");
    assert!(got["viewer"].get("repost").is_none(), "{got}");
    assert_eq!(got["viewer"]["replyDisabled"], json!(true), "{got}");
    assert_eq!(got["author"]["viewer"]["following"], json!(format!("at://{VISITOR}/app.bsky.graph.follow/3kfollow")), "{got}");
    let rows = h.refract_reply_rows(&open, &restored()).await.expect("rows");
    let body = h.refract_replies(&visitor, &req, &open, rows, None, Duration::from_secs(1)).await.expect("replies");
    let out = h.refract_visitor_state(VISITOR, &req, body).await.expect("state");
    let got = &out.inner()["posts"][0];
    assert_eq!(got["uri"], json!(d));
    assert_eq!(got["viewer"]["replyDisabled"], json!(false), "no gate: {got}");
    assert!(got["viewer"].get("like").is_none() && got["author"]["viewer"].get("following").is_none(), "{got}");
    db.drop_db().await;
}
