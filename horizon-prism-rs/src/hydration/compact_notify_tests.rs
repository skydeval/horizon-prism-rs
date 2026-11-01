//! Notifications, likes and `via` over the compact layout's odd references,
//! with the records written through the real writer and read back through
//! the endpoints' hydrator methods.

use std::sync::Arc;

use serde_json::{Value, json};
use sqlx::PgPool;

use super::testing::{actor, hydrator, post};
use super::{Hydrator, Viewer};
use crate::aggregations::{ScopeSets, ScopeState};
use crate::config::ScopePolicy;
use crate::firehose::writer::{Action, ApplyOptions, OpOutcome, RawOp, WriteSource, Writer};
use crate::private_tier::DisabledPrivateTier;
use crate::test_support::fresh_db;
use crate::xrpc::pools::Requester;

/// The member: the notifications' recipient and the viewer.
const ALICE: &str = "did:plc:aaaaaaaaaaaaaaaaaaaaaaaa";
const BOB: &str = "did:plc:bbbbbbbbbbbbbbbbbbbbbbbb";
const CAROL: &str = "did:plc:cccccccccccccccccccccccc";
const DAVE: &str = "did:plc:dddddddddddddddddddddddd";
/// An account stored nowhere.
const ZED: &str = "did:plc:zzzzzzzzzzzzzzzzzzzzzzzz";

const NOW: &str = "2026-09-26T00:00:00Z";

const OPTIONS: ApplyOptions = ApplyOptions {
    advance_applied_rev: true,
    dead_letter_readiness: true,
    attempts: 5,
    live: true,
    subscription_rules: true,
    source: WriteSource::Firehose,
};

/// Everyone in scope; Alice is the one member.
fn sets() -> ScopeSets {
    let mut s = ScopeSets::default();
    for d in [ALICE, BOB, CAROL, DAVE] {
        s.scope.insert(d.into());
    }
    s.members.insert(ALICE.into());
    s
}

fn writer(pool: &PgPool) -> Writer {
    Writer::new(pool.clone(), ScopeState::new(sets(), ScopePolicy::MembersAndOneHop, 10_000), Arc::new(DisabledPrivateTier))
}

/// The `n`th of a run of valid TID revs, in order.
fn rev(n: usize) -> String {
    const ALPHABET: &[u8] = b"234567abcdefghijklmnopqrstuvwxyz";
    let digit = |d: usize| char::from(ALPHABET[d % 32]);
    format!("3maaaaaaaa{}{}{}", digit(n / 1024), digit(n / 32), digit(n))
}

fn create(collection: &str, rkey: &str, record: Value) -> RawOp {
    RawOp {
        action: Action::Create,
        collection: collection.into(),
        rkey: rkey.into(),
        cid: Some(format!("bafy{rkey}")),
        record: Some(record),
    }
}

/// A like of `subject`, made through `via` (`{uri, cid}`) when given.
fn like(rkey: &str, subject: &str, via: Option<(&str, &str)>) -> RawOp {
    let mut record = json!({"subject": {"uri": subject, "cid": "bafysubject"}, "createdAt": NOW});
    if let Some((uri, cid)) = via {
        record["via"] = json!({"uri": uri, "cid": cid});
    }
    create("app.bsky.feed.like", rkey, record)
}

/// A reply under `root` to `parent`.
fn reply(rkey: &str, root: &str, parent: &str) -> RawOp {
    create(
        "app.bsky.feed.post",
        rkey,
        json!({"text": rkey, "createdAt": NOW, "reply": {"root": {"uri": root, "cid": "bafyroot"}, "parent": {"uri": parent, "cid": "bafyparent"}}}),
    )
}

async fn written(w: &Writer, repo: &str, rev: &str, op: RawOp) {
    let report = w.apply(repo, rev, &[op], OPTIONS).await.expect("apply");
    assert_eq!(report.outcomes, [OpOutcome::Written], "{repo} {rev}");
}

fn me() -> Viewer {
    Viewer {
        did: Some(ALICE.into()),
        hydrate: true,
    }
}

async fn actors(p: &PgPool) {
    for (d, h) in [(ALICE, "alice.test"), (BOB, "bob.test"), (CAROL, "carol.test"), (DAVE, "dave.test")] {
        actor(p, d, h, h).await;
    }
}

/// Alice's `listNotifications`.
async fn listed(h: &Hydrator) -> Vec<Value> {
    let l = h.list_notifications(&me(), &Requester::test(), 50, None, &[]).await.expect("db").expect("view");
    l.inner()["notifications"].as_array().expect("list").clone()
}

async fn unread(h: &Hydrator) -> Value {
    h.unread_count(&me()).await.expect("db").expect("view").inner()["count"].clone()
}

/// The DIDs `getLikes` lists for `uri`, newest first.
async fn likers(h: &Hydrator, uri: &str) -> Vec<String> {
    let l = h.get_likes(&Viewer::anonymous(), &Requester::test(), uri, 50, None).await.expect("db").expect("view");
    l.inner()["likes"].as_array().expect("likes").iter().map(|x| x["actor"]["did"].as_str().expect("did").to_string()).collect()
}

/// A like's subject and `via` columns, and the text of each odd one.
type LikeColumns = (Option<i16>, Option<String>, Option<i16>, Option<String>);

async fn like_columns(p: &PgPool, author: &str) -> LikeColumns {
    sqlx::query_as(
        "SELECT l.subj_coll, \
           (SELECT o.raw FROM odd_refs o WHERE o.tbl = 2 AND o.author = l.author AND o.rkey = l.rkey AND o.slot = 4), \
           l.via_coll, \
           (SELECT o.raw FROM odd_refs o WHERE o.tbl = 2 AND o.author = l.author AND o.rkey = l.rkey AND o.slot = 5) \
         FROM likes l WHERE l.author = (SELECT id FROM dids WHERE did = $1)",
    )
    .bind(author)
    .fetch_one(p)
    .await
    .expect("like")
}

/// A like whose `via` is a post, not a repost, is stored whole and comes
/// back in `listNotifications` with the `via` it was written with; it makes
/// no via notification.
#[tokio::test]
async fn a_like_via_a_post_rebuilds_via_as_the_posts_uri() {
    let Some(db) = fresh_db("compact_like_via_post").await else {
        return;
    };
    let p = &db.pool;
    actors(p).await;
    let liked = post(p, ALICE, "3kaaaaaaaaaa2", "liked", None, None).await;
    let via = format!("at://{BOB}/app.bsky.feed.post/3kaaaaaaaaab2");
    let w = writer(p);
    written(&w, CAROL, &rev(1), like("3kaaaaaaaaac2", &liked, Some((&via, "bafyvia")))).await;
    assert_eq!(like_columns(p, CAROL).await, (Some(1), None, Some(1), None), "both references whole");

    let h = hydrator(p, sets());
    let list = listed(&h).await;
    assert_eq!(list.len(), 1, "{list:?}");
    let n = &list[0];
    assert_eq!(n["reason"], "like");
    assert_eq!(n["uri"], json!(format!("at://{CAROL}/app.bsky.feed.like/3kaaaaaaaaac2")));
    assert_eq!(n["record"]["subject"], json!({"uri": liked, "cid": "bafysubject"}));
    assert_eq!(n["record"]["via"], json!({"uri": via, "cid": "bafyvia"}));
    assert_eq!(unread(&h).await, 1);
    db.drop_db().await;
}

/// A like of `at://<did>/app.bsky.feed.post`, with no record key, is odd:
/// its text is kept, it counts toward none of the author's posts, and its
/// notification names the subject by that exact text.
#[tokio::test]
async fn a_like_of_a_post_uri_with_no_rkey_counts_nothing_and_notifies_with_its_text() {
    let Some(db) = fresh_db("compact_like_no_rkey").await else {
        return;
    };
    let p = &db.pool;
    actors(p).await;
    let stored = post(p, ALICE, "3kaaaaaaaaaa2", "a post of hers", None, None).await;
    let odd = format!("at://{ALICE}/app.bsky.feed.post");
    let w = writer(p);
    written(&w, BOB, &rev(1), like("3kaaaaaaaaab2", &odd, None)).await;
    assert_eq!(like_columns(p, BOB).await, (Some(0), Some(odd.clone()), None, None));
    let kept: Option<bool> = sqlx::query_scalar("SELECT l.subj_author = (SELECT id FROM dids WHERE did = $1) FROM likes l")
        .bind(ALICE)
        .fetch_one(p)
        .await
        .expect("author");
    assert_eq!(kept, Some(true), "the subject's author is kept");

    // Counts toward nothing.
    let counted: i64 = sqlx::query_scalar("SELECT count(*) FROM post_counts WHERE like_count <> 0").fetch_one(p).await.expect("counts");
    assert_eq!(counted, 0);
    let h = hydrator(p, sets());
    let req = Requester::test();
    let v = h.get_posts(&Viewer::anonymous(), &req, std::slice::from_ref(&stored)).await.expect("db").expect("view");
    assert_eq!(v.inner()["posts"][0]["likeCount"], 0);
    assert!(likers(&h, &stored).await.is_empty());
    assert_eq!(likers(&h, &odd).await, [BOB], "found by its text");

    // The notification names the subject by its text.
    let list = listed(&h).await;
    assert_eq!(list.len(), 1, "{list:?}");
    assert_eq!(list[0]["reason"], "like");
    assert_eq!(list[0]["reasonSubject"], json!(odd));
    assert_eq!(list[0]["record"]["subject"], json!({"uri": odd, "cid": "bafysubject"}));
    assert_eq!(unread(&h).await, 1);
    db.drop_db().await;
}

/// With no Constellation, `getLikes` of an odd URI lists the likes of that
/// exact text, not those of the post with the same author and key, nor of
/// another odd URI with the same key.
#[tokio::test]
async fn get_likes_of_an_odd_uri_finds_its_likes_and_not_the_posts() {
    let Some(db) = fresh_db("compact_get_likes_odd").await else {
        return;
    };
    let p = &db.pool;
    actors(p).await;
    let liked = post(p, ALICE, "3kaaaaaaaaaa2", "the post", None, None).await;
    let odd = format!("at://{ALICE}/com.example.thing/3kaaaaaaaaaa2");
    let other = format!("at://{ALICE}/com.example.other/3kaaaaaaaaaa2");
    let w = writer(p);
    written(&w, BOB, &rev(1), like("3kaaaaaaaaab2", &odd, None)).await;
    written(&w, CAROL, &rev(1), like("3kaaaaaaaaac2", &liked, None)).await;
    written(&w, DAVE, &rev(1), like("3kaaaaaaaaad2", &other, None)).await;
    let h = hydrator(p, sets());
    assert_eq!(likers(&h, &odd).await, [BOB]);
    assert_eq!(likers(&h, &other).await, [DAVE]);
    assert_eq!(likers(&h, &liked).await, [CAROL]);
    assert_eq!(crate::test_support::interactions::post_counts(p, &liked).await.map(|c| c.0), Some(1), "only the post's own like counts");
    db.drop_db().await;
}

/// A thread whose root is odd (a post URI whose key is not a record key) is
/// muted by that text: a reply under it and a like of a post in it leave
/// `listNotifications` and the count, the posts say so, a like made while
/// muted writes no row, and unmuting brings back what was there.
#[tokio::test]
async fn a_thread_mute_on_an_odd_root_applies() {
    let Some(db) = fresh_db("compact_odd_root_mute").await else {
        return;
    };
    let p = &db.pool;
    actors(p).await;
    let root = format!("at://{ZED}/app.bsky.feed.post/3kaaaaaaaaaa2!");
    let w = writer(p);
    // Alice's reply under the odd root; Bob replies to her, Carol likes hers.
    written(&w, ALICE, &rev(1), reply("3kaaaaaaaaaa3", &root, &root)).await;
    let mine = format!("at://{ALICE}/app.bsky.feed.post/3kaaaaaaaaaa3");
    let stored: (Option<i16>, Option<String>) = sqlx::query_as(&format!(
        "SELECT p.root_coll, ref_text(p.root_author, p.root_coll, p.root_rkey, 1::smallint, p.author, p.rkey, 1::smallint) \
         FROM posts p WHERE {}",
        crate::test_support::posts::key_is("p", &mine)
    ))
    .fetch_one(p)
    .await
    .expect("post");
    assert_eq!(stored, (Some(0), Some(root.clone())), "the root is odd, its text kept");
    written(&w, BOB, &rev(1), reply("3kaaaaaaaaab3", &root, &mine)).await;
    written(&w, CAROL, &rev(1), like("3kaaaaaaaaac3", &mine, None)).await;
    let theirs = format!("at://{BOB}/app.bsky.feed.post/3kaaaaaaaaab3");
    let h = hydrator(p, sets());
    let reasons = |list: &[Value]| -> Vec<String> {
        let mut r: Vec<String> = list.iter().map(|n| n["reason"].as_str().expect("reason").to_string()).collect();
        r.sort_unstable();
        r
    };
    assert_eq!(reasons(&listed(&h).await), ["like", "reply"]);
    assert_eq!(unread(&h).await, 2);

    h.set_thread_mute(&me(), &root, true).await.expect("db").expect("muted");
    assert_eq!(listed(&h).await, Vec::<Value>::new());
    assert_eq!(unread(&h).await, 0);
    let state = h.post_viewer(&me(), &[mine.clone(), theirs.clone()]).await.expect("db");
    assert_eq!((&state[&mine]["threadMuted"], &state[&theirs]["threadMuted"]), (&json!(true), &json!(true)));
    // A like made while muted writes no notification.
    written(&w, DAVE, &rev(1), like("3kaaaaaaaaad3", &mine, None)).await;
    let rows: i64 = sqlx::query_scalar("SELECT count(*) FROM notifications WHERE recipient_did = $1")
        .bind(ALICE)
        .fetch_one(p)
        .await
        .expect("rows");
    assert_eq!(rows, 2);

    h.set_thread_mute(&me(), &root, false).await.expect("db").expect("unmuted");
    assert_eq!(reasons(&listed(&h).await), ["like", "reply"]);
    assert_eq!(unread(&h).await, 2);
    db.drop_db().await;
}

/// A like whose `via.uri` is 3 KB long (a post URI whose key is past the
/// record-key limit) is stored with its text kept whole; its subject still
/// counts, its notification shows the `via` exactly, and its delete takes
/// the text with it.
#[tokio::test]
async fn a_like_with_a_three_kilobyte_via_is_stored_and_its_via_comes_back_whole() {
    let Some(db) = fresh_db("compact_long_via").await else {
        return;
    };
    let p = &db.pool;
    actors(p).await;
    let liked = post(p, ALICE, "3kaaaaaaaaaa2", "liked", None, None).await;
    let via = format!("at://{BOB}/app.bsky.feed.post/{}", "a".repeat(3000));
    assert!(via.len() > 3000);
    let w = writer(p);
    written(&w, CAROL, &rev(1), like("3kaaaaaaaaac2", &liked, Some((&via, "bafyvia")))).await;
    assert_eq!(like_columns(p, CAROL).await, (Some(1), None, Some(0), Some(via.clone())));
    let kept: Option<bool> = sqlx::query_scalar("SELECT l.via_author = (SELECT id FROM dids WHERE did = $1) FROM likes l")
        .bind(BOB)
        .fetch_one(p)
        .await
        .expect("author");
    assert_eq!(kept, Some(true), "the via's author is kept");
    assert_eq!(crate::test_support::interactions::post_counts(p, &liked).await.map(|c| c.0), Some(1));

    let h = hydrator(p, sets());
    let list = listed(&h).await;
    assert_eq!(list.len(), 1, "{list:?}");
    assert_eq!(list[0]["record"]["via"], json!({"uri": via, "cid": "bafyvia"}));
    assert_eq!(list[0]["record"]["subject"]["uri"], json!(liked));

    let gone = RawOp {
        action: Action::Delete,
        collection: "app.bsky.feed.like".into(),
        rkey: "3kaaaaaaaaac2".into(),
        cid: None,
        record: None,
    };
    written(&w, CAROL, &rev(2), gone).await;
    let left: (i64, i64) = sqlx::query_as("SELECT (SELECT count(*) FROM likes), (SELECT count(*) FROM odd_refs)").fetch_one(p).await.expect("left");
    assert_eq!(left, (0, 0));
    assert_eq!(listed(&h).await, Vec::<Value>::new(), "its notification went with it");
    db.drop_db().await;
}
