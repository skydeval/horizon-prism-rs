//! The gate after the fetch on public paths, and the marker (refract
//! design §7.2, §7.3, §13: T-8ah, T-8ai, T-8ak, T-8al, T-8an, T-8h, T-13).

use std::sync::Arc;

use serde_json::{Value, json};

use crate::aggregations::ScopeSets;
use crate::hydration::testing::{FixedUpstream, actor, exec, hydrator_with, post};
use crate::hydration::{Hydrator, Viewer};
use crate::indexing::PublicMode;
use crate::refract::RefractHidden;
use crate::test_support::fresh_db;
use crate::xrpc::pools::Requester;

const RESTORED: &str = "did:plc:restoredaaaaaaaaaaaaaaaa";
const PENDING: &str = "did:plc:pendingaaaaaaaaaaaaaaaaa";
const OUTSIDE: &str = "did:plc:outsideaaaaaaaaaaaaaaaaa";
const STORED: &str = "did:plc:storedaaaaaaaaaaaaaaaaaa";

fn pv(uri: &str, author: &str) -> Value {
    json!({"$type": "app.bsky.feed.defs#postView", "uri": uri, "cid": "c", "author": {"did": author, "handle": "h.test"}, "record": {"text": "t"}, "indexedAt": "2026-01-01T00:00:00Z"})
}

fn sets() -> ScopeSets {
    let mut s = ScopeSets::default();
    s.public.mode = PublicMode::On;
    s.scope.insert(STORED.into());
    Arc::make_mut(&mut s.restored).insert(RESTORED.into(), "community-a".into());
    let mut hidden = RefractHidden::default();
    hidden.update(PENDING, |r| r.pending = true);
    s.refract_hidden = Arc::new(hidden);
    s
}

fn h(p: &sqlx::PgPool, up: FixedUpstream) -> Hydrator {
    hydrator_with(p, sets(), Arc::new(up))
}

/// T-8ai, T-8al: On mode, a public `getPosts` of posts not stored here: a
/// nested quote of a locally taken-down post renders `viewNotFound`; one of
/// a `!hide` post stays (collapsed, as the local quote path does); a quote
/// of a post by a pending DID renders `viewNotFound`. The top-level post
/// with a local `!hide` comes back collapsed; one with `!takedown` is not
/// fetched.
#[tokio::test]
async fn t8ai_t8al_nested_quotes_on_public_paths() {
    let Some(db) = fresh_db("refract_nested_quotes").await else {
        return;
    };
    let p = &db.pool;
    let q = |n: &str, author: &str| format!("at://{author}/app.bsky.feed.post/{n}");
    let mut up = FixedUpstream::default();
    for (n, quoted) in [("1", q("down", OUTSIDE)), ("2", q("hidden", OUTSIDE)), ("3", q("p", PENDING))] {
        let mut view = pv(&q(n, OUTSIDE), OUTSIDE);
        let author = crate::db::locks::uri_author(&quoted).unwrap_or_default().to_string();
        view["embed"] = json!({"$type": "app.bsky.embed.record#view", "record": {"$type": "app.bsky.embed.record#viewRecord", "uri": quoted, "cid": "c", "author": {"did": author, "handle": "x"}, "value": {"text": "q"}}});
        up.posts.insert(q(n, OUTSIDE), view);
    }
    up.posts.insert(q("collapsed", OUTSIDE), pv(&q("collapsed", OUTSIDE), OUTSIDE));
    up.posts.insert(q("taken", OUTSIDE), pv(&q("taken", OUTSIDE), OUTSIDE));
    exec(p, &format!("INSERT INTO labels (subject, val) VALUES ('{}', '!takedown'), ('{}', '!hide'), ('{}', '!hide'), ('{}', '!takedown')", q("down", OUTSIDE), q("hidden", OUTSIDE), q("collapsed", OUTSIDE), q("taken", OUTSIDE))).await;
    let h = h(p, up);
    let uris: Vec<String> = ["1", "2", "3", "collapsed", "taken"].iter().map(|n| q(n, OUTSIDE)).collect();
    let got = h.get_posts(&Viewer::anonymous(), &Requester::test(), &uris).await.expect("db").expect("ok");
    let posts = got.inner()["posts"].as_array().expect("posts").clone();
    let by = |n: &str| posts.iter().find(|p| p["uri"] == q(n, OUTSIDE)).cloned();
    assert_eq!(by("1").expect("1")["embed"]["record"]["$type"], "app.bsky.embed.record#viewNotFound");
    assert_eq!(by("2").expect("2")["embed"]["record"]["$type"], "app.bsky.embed.record#viewRecord", "collapsed, kept");
    assert_eq!(by("3").expect("3")["embed"]["record"]["$type"], "app.bsky.embed.record#viewNotFound", "a pending DID is hidden");
    assert!(by("collapsed").is_some(), "getPosts keeps a collapsed post");
    assert!(by("taken").is_none(), "a taken-down post is not fetched");
    db.drop_db().await;
}

/// T-8ak: On mode, an upstream `getAuthorFeed` repost of a pending DID's
/// post is dropped; an upstream `getFollows` omits the pending DID.
#[tokio::test]
async fn t8ak_upstream_arrays_are_filtered() {
    let Some(db) = fresh_db("refract_upstream_arrays").await else {
        return;
    };
    let p = &db.pool;
    let mut up = FixedUpstream::default();
    let mine = format!("at://{OUTSIDE}/app.bsky.feed.post/1");
    let theirs = format!("at://{PENDING}/app.bsky.feed.post/1");
    up.feeds.insert(
        (OUTSIDE.to_string(), None),
        json!({"feed": [{"post": pv(&mine, OUTSIDE)}, {"post": pv(&theirs, PENDING), "reason": {"$type": "app.bsky.feed.defs#reasonRepost", "by": {"did": OUTSIDE, "handle": "o"}}}]}),
    );
    up.follows.insert(OUTSIDE.to_string(), json!({"subject": {"did": OUTSIDE, "handle": "o"}, "follows": [{"did": PENDING, "handle": "p"}, {"did": STORED, "handle": "s"}]}));
    let h = h(p, up);
    let (viewer, req) = (Viewer::anonymous(), Requester::test());
    let feed = h.get_author_feed(&viewer, &req, OUTSIDE, 50, None, "posts_with_replies", false).await.expect("db").expect("ok");
    let uris: Vec<&str> = feed.inner()["feed"].as_array().expect("feed").iter().filter_map(|i| i["post"]["uri"].as_str()).collect();
    assert_eq!(uris, [mine.as_str()]);
    let follows = h.get_follows(&viewer, &req, OUTSIDE, 50, None).await.expect("db").expect("ok");
    let dids: Vec<&str> = follows.inner()["follows"].as_array().expect("follows").iter().filter_map(|f| f["did"].as_str()).collect();
    assert_eq!(dids, [STORED]);
    db.drop_db().await;
}

/// T-13, T-8an, T-8h: the marker on a restored DID's profile and post
/// views and on its author; never on a DID awaiting its labels; a restored
/// post with a gone record label is `RecordNotFound` from `getRecord`.
#[tokio::test]
async fn t13_marker_and_get_record() {
    let Some(db) = fresh_db("refract_marker").await else {
        return;
    };
    let p = &db.pool;
    actor(p, RESTORED, "r.test", "Restored").await;
    let uri = post(p, RESTORED, "3kpost", "hello", None, None).await;
    let gone = post(p, RESTORED, "3kgone", "gone", None, None).await;
    let h = h(p, FixedUpstream::default());
    let (viewer, req) = (Viewer::anonymous(), Requester::test());
    let marker = json!({"community": "community-a", "tier": 1, "labeler": "did:plc:cccccccccccccccccccccccc"});
    let profile = h.get_profile(&viewer, &req, RESTORED).await.expect("db").expect("ok");
    assert_eq!(profile.inner()["restored"], marker);
    assert_eq!(profile.inner()["displayName"], "Restored", "built from its stored row (G-9)");
    let posts = h.get_posts(&viewer, &req, std::slice::from_ref(&uri)).await.expect("db").expect("ok");
    assert_eq!(posts.inner()["posts"][0]["restored"], marker);
    assert_eq!(posts.inner()["posts"][0]["author"]["restored"], marker);
    // Awaiting its labels: hidden, and never marked.
    {
        let mut w = h.scope().write_guard().await;
        Arc::make_mut(&mut w.refract_hidden).update(RESTORED, |r| r.awaiting_labels = true);
    }
    let req = Requester::test();
    assert!(h.get_profile(&viewer, &req, RESTORED).await.expect("db").is_err(), "hidden while it awaits its labels");
    // A gone record label: `getRecord` says RecordNotFound.
    {
        let mut w = h.scope().write_guard().await;
        let hidden = Arc::make_mut(&mut w.refract_hidden);
        hidden.update(RESTORED, |r| r.awaiting_labels = false);
        hidden.add_record(&gone, None);
    }
    let rkey = gone.rsplit('/').next().expect("rkey");
    let err = h.repo_get_record(RESTORED, "app.bsky.feed.post", rkey, None).await.expect("db").expect_err("hidden");
    assert_eq!(err.error, "RecordNotFound");
    assert!(h.repo_get_record(RESTORED, "app.bsky.feed.post", "3kpost", None).await.expect("db").is_ok());
    db.drop_db().await;
}
