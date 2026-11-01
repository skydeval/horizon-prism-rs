//! Threads, gates, search and pins over the compact layout's references.

use serde_json::{Value, json};

use super::search::PostSearchParams;
use super::testing::{actor, hydrator, post, post_record};
use super::{Hydrator, Viewer};
use crate::aggregations::ScopeSets;
use crate::test_support::fresh_db;
use crate::xrpc::pools::Requester;

const ALICE: &str = "did:plc:aaaaaaaaaaaaaaaaaaaaaaaa";
const BOB: &str = "did:plc:bbbbbbbbbbbbbbbbbbbbbbbb";
/// The account a root post mentions.
const MEG: &str = "did:plc:mmmmmmmmmmmmmmmmmmmmmmmm";

fn sets() -> ScopeSets {
    let mut s = ScopeSets::default();
    for d in [ALICE, BOB, MEG] {
        s.scope.insert(d.into());
        s.members.insert(d.into());
    }
    s
}

fn signed_in(did: &str) -> Viewer {
    Viewer { did: Some(did.into()), hydrate: true }
}

/// Actor rows for everyone, with their search corpus rows.
async fn accounts(p: &sqlx::PgPool) {
    for (did, handle) in [(ALICE, "alice.test"), (BOB, "bob.test"), (MEG, "meg.test")] {
        actor(p, did, handle, handle).await;
        sqlx::query("INSERT INTO scope_dids (did, reasons) VALUES ($1, 2) ON CONFLICT DO NOTHING").bind(did).execute(p).await.expect("scope");
    }
}

fn sorted(mut v: Vec<String>) -> Vec<String> {
    v.sort();
    v
}

/// A search over the whole corpus for `query`, with `edit` applied.
fn search_params(query: &str, edit: impl FnOnce(&mut PostSearchParams)) -> PostSearchParams {
    let mut p = PostSearchParams { v2: true, all_time: true, query: Some(query.to_string()), limit: 25, ..PostSearchParams::default() };
    edit(&mut p);
    p
}

/// The URIs a search finds, sorted.
async fn found(h: &Hydrator, p: PostSearchParams) -> Vec<String> {
    let v = h.search_posts(&Viewer::anonymous(), &Requester::test(), &p).await.expect("db").expect("view");
    sorted(v.inner()["posts"].as_array().expect("posts").iter().map(|x| x["uri"].as_str().expect("uri").to_string()).collect())
}

/// The URIs of a `getPosts` answer, in order.
fn post_uris(v: &Value) -> Vec<String> {
    v["posts"].as_array().expect("posts").iter().map(|x| x["uri"].as_str().expect("uri").to_string()).collect()
}

/// The post URIs of an author feed, in order.
fn feed_uris(v: &Value) -> Vec<String> {
    v["feed"].as_array().expect("feed").iter().map(|i| i["post"]["uri"].as_str().expect("uri").to_string()).collect()
}

/// A reply whose parent and root are in a collection outside the fixed
/// list is still a reply: search's reply filters, the author feed's
/// no-replies filter and Refract's author feed all treat it as one.
#[tokio::test]
async fn a_reply_to_an_unknown_collection_is_still_a_reply() {
    let Some(db) = fresh_db("compact_ref_odd_parent_reply").await else {
        return;
    };
    let p = &db.pool;
    accounts(p).await;
    let odd = format!("at://{ALICE}/com.example.thing/3kaaaaaaaaaa2");
    let plain = post(p, BOB, "3kaaaaaaaaaa3", "compact plain", None, None).await;
    let reply = post(p, BOB, "3kaaaaaaaaaa4", "compact reply", Some(&odd), Some(&odd)).await;
    let h = hydrator(p, sets());

    assert_eq!(found(&h, search_params("compact", |_| {})).await, sorted(vec![plain.clone(), reply.clone()]));
    assert_eq!(found(&h, search_params("compact", |s| s.replies_only = true)).await, [reply.as_str()]);
    assert_eq!(found(&h, search_params("compact", |s| s.exclude_replies = true)).await, [plain.as_str()]);
    // The parent is found by its text.
    assert_eq!(found(&h, search_params("compact", |s| s.reply_parent = Some(odd.clone()))).await, [reply.as_str()]);

    let (anon, req) = (Viewer::anonymous(), Requester::test());
    let feed = |filter: &'static str| {
        let (h, anon, req) = (&h, &anon, &req);
        async move { sorted(feed_uris(h.get_author_feed(anon, req, BOB, 50, None, filter, false).await.expect("db").expect("view").inner())) }
    };
    assert_eq!(feed("posts_with_replies").await, sorted(vec![plain.clone(), reply.clone()]));
    assert_eq!(feed("posts_no_replies").await, [plain.as_str()]);

    // Refract's author feed reads the post rows themselves.
    let (items, _) = h.refract_author_items(BOB, "posts_no_replies", None, 50, false).await.expect("db");
    assert_eq!(items.iter().map(|i| i.post_uri.clone()).collect::<Vec<_>>(), [plain.as_str()]);
    let (items, _) = h.refract_author_items(BOB, "posts_with_replies", None, 50, false).await.expect("db");
    let mut kinds: Vec<(String, String)> = items.iter().map(|i| (i.post_uri.clone(), i.kind.clone())).collect();
    kinds.sort();
    let mut want = vec![(plain.clone(), "post".to_string()), (reply.clone(), "reply".to_string())];
    want.sort();
    assert_eq!(kinds, want);
    db.drop_db().await;
}

/// A threadgate with the same record key as its post gates the thread
/// whether that key is a TID or not: replies are disabled, the gate is
/// shown, and the replies it hides are hidden.
#[tokio::test]
async fn a_gate_with_a_non_tid_key_gates_its_post_as_a_tid_key_does() {
    let Some(db) = fresh_db("compact_ref_non_tid_gate").await else {
        return;
    };
    let p = &db.pool;
    accounts(p).await;
    let h = hydrator(p, sets());
    let (anon, req, bob) = (Viewer::anonymous(), Requester::test(), signed_in(BOB));
    for (rkey, hidden_key, shown_key) in [("3kaaaaaaaaaa2", "3kaaaaaaaaaa3", "3kaaaaaaaaaa4"), ("abc", "3kaaaaaaaaaa5", "3kaaaaaaaaaa6")] {
        let root = post(p, ALICE, rkey, "root", None, None).await;
        let hidden = post(p, BOB, hidden_key, "hidden reply", Some(&root), Some(&root)).await;
        let shown = post(p, BOB, shown_key, "shown reply", Some(&root), Some(&root)).await;
        let gate = crate::test_support::records::threadgate(p, ALICE, rkey, &root, Some(json!([])), &[hidden.as_str()]).await;
        assert_eq!(gate, format!("at://{ALICE}/app.bsky.feed.threadgate/{rkey}"));

        let t = h.get_post_thread(&bob, &req, &root, 6, 80).await.expect("db").expect("view");
        assert_eq!(t.inner()["thread"]["post"]["uri"], json!(root), "{rkey}");
        assert_eq!(t.inner()["thread"]["post"]["viewer"]["replyDisabled"], json!(true), "{rkey}");
        assert_eq!(t.inner()["threadgate"]["uri"], json!(gate), "{rkey}");
        let t = h.get_post_thread(&anon, &req, &root, 6, 80).await.expect("db").expect("view");
        let replies: Vec<&str> = t.inner()["thread"]["replies"].as_array().expect("replies").iter().filter_map(|r| r["post"]["uri"].as_str()).collect();
        assert_eq!(replies, [shown.as_str()], "{rkey}");

        // getPosts decides from each post's root.
        let v = h.get_posts(&bob, &req, &[root.clone(), shown.clone()]).await.expect("db").expect("view");
        assert_eq!(post_uris(v.inner()), [root.clone(), shown.clone()], "{rkey}");
        for view in v.inner()["posts"].as_array().expect("posts") {
            assert_eq!(view["viewer"]["replyDisabled"], json!(true), "{rkey} {}", view["uri"]);
        }
    }
    db.drop_db().await;
}

/// A threadgate's mention rule on a stored root: the mentioned account may
/// reply, another may not, and the root's author always may.
#[tokio::test]
async fn the_mention_rule_lets_the_mentioned_account_reply() {
    let Some(db) = fresh_db("compact_ref_mention_rule").await else {
        return;
    };
    let p = &db.pool;
    accounts(p).await;
    let record = json!({"$type": "app.bsky.feed.post", "text": "hi meg", "createdAt": "2026-09-26T12:00:00.000Z",
        "facets": [{"index": {"byteStart": 3, "byteEnd": 6}, "features": [{"$type": "app.bsky.richtext.facet#mention", "did": MEG}]}]});
    let root = post_record(p, ALICE, "3kaaaaaaaaaa2", &record).await;
    let reply = post(p, BOB, "3kaaaaaaaaaa3", "a reply", Some(&root), Some(&root)).await;
    crate::test_support::records::threadgate(p, ALICE, "3kaaaaaaaaaa2", &root, Some(json!([{"$type": "app.bsky.feed.threadgate#mentionRule"}])), &[]).await;
    let h = hydrator(p, sets());
    let req = Requester::test();
    for (who, disabled) in [(MEG, false), (BOB, true), (ALICE, false)] {
        let viewer = signed_in(who);
        let t = h.get_post_thread(&viewer, &req, &root, 6, 80).await.expect("db").expect("view");
        assert_eq!(t.inner()["thread"]["post"]["viewer"]["replyDisabled"], json!(disabled), "thread, {who}");
        let v = h.get_posts(&viewer, &req, &[root.clone(), reply.clone()]).await.expect("db").expect("view");
        assert_eq!(post_uris(v.inner()), [root.clone(), reply.clone()]);
        for view in v.inner()["posts"].as_array().expect("posts") {
            assert_eq!(view["viewer"]["replyDisabled"], json!(disabled), "getPosts, {who} {}", view["uri"]);
        }
    }
    db.drop_db().await;
}

/// Search's media filters read the post's embed kind: media is images,
/// video or a gallery, alone or beside a quote; video is video alone or
/// beside a quote.
#[tokio::test]
async fn search_media_filters_follow_the_embed_kind() {
    let Some(db) = fresh_db("compact_ref_search_media").await else {
        return;
    };
    let p = &db.pool;
    accounts(p).await;
    let blob = |cid: &str, mime: &str| json!({"$type": "blob", "ref": {"$link": cid}, "mimeType": mime, "size": 1});
    let with_embed = |rkey: &'static str, embed: Option<Value>| {
        let mut record = json!({"$type": "app.bsky.feed.post", "text": format!("compact {rkey}"), "createdAt": "2026-09-26T12:00:00.000Z"});
        if let Some(embed) = embed {
            record["embed"] = embed;
        }
        async move { post_record(p, BOB, rkey, &record).await }
    };
    let text = with_embed("3kaaaaaaaaaa2", None).await;
    let images = with_embed("3kaaaaaaaaaa3", Some(json!({"$type": "app.bsky.embed.images", "images": [{"image": blob("bafyi1", "image/png"), "alt": "i"}]}))).await;
    let video = with_embed("3kaaaaaaaaaa4", Some(json!({"$type": "app.bsky.embed.video", "video": blob("bafyv1", "video/mp4")}))).await;
    let quote_video = with_embed(
        "3kaaaaaaaaaa5",
        Some(json!({"$type": "app.bsky.embed.recordWithMedia",
            "record": {"$type": "app.bsky.embed.record", "record": {"uri": text, "cid": "bafy3kaaaaaaaaaa2"}},
            "media": {"$type": "app.bsky.embed.video", "video": blob("bafyv2", "video/mp4")}})),
    )
    .await;
    let gallery = with_embed(
        "3kaaaaaaaaaa6",
        Some(json!({"$type": "app.bsky.embed.gallery", "items": [
            {"$type": "app.bsky.embed.gallery#image", "image": blob("bafyg1", "image/png"), "alt": "g", "aspectRatio": {"width": 1, "height": 1}}]})),
    )
    .await;
    let external = with_embed(
        "3kaaaaaaaaaa7",
        Some(json!({"$type": "app.bsky.embed.external", "external": {"uri": "https://example.com/", "title": "t", "description": "d"}})),
    )
    .await;
    let h = hydrator(p, sets());
    assert_eq!(
        found(&h, search_params("compact", |_| {})).await,
        sorted(vec![text.clone(), images.clone(), video.clone(), quote_video.clone(), gallery.clone(), external.clone()])
    );
    assert_eq!(
        found(&h, search_params("compact", |s| s.has_media = true)).await,
        sorted(vec![images.clone(), video.clone(), quote_video.clone(), gallery.clone()])
    );
    assert_eq!(found(&h, search_params("compact", |s| s.has_video = true)).await, sorted(vec![video.clone(), quote_video.clone()]));
    db.drop_db().await;
}

/// A post URI with a trailing slash, or an extra path segment, names no
/// stored post; the exact URI does.
#[tokio::test]
async fn a_post_uri_with_a_trailing_slash_finds_nothing() {
    let Some(db) = fresh_db("compact_ref_trailing_slash").await else {
        return;
    };
    let p = &db.pool;
    accounts(p).await;
    let uri = post(p, ALICE, "3kaaaaaaaaaa2", "the post", None, None).await;
    let h = hydrator(p, sets());
    let (anon, req) = (Viewer::anonymous(), Requester::test());
    let v = h.get_posts(&anon, &req, std::slice::from_ref(&uri)).await.expect("db").expect("view");
    assert_eq!(post_uris(v.inner()), [uri.as_str()]);
    for odd in [format!("{uri}/"), format!("{uri}/x")] {
        let v = h.get_posts(&anon, &req, std::slice::from_ref(&odd)).await.expect("db").expect("view");
        assert_eq!(post_uris(v.inner()), Vec::<String>::new(), "{odd}");
    }
    db.drop_db().await;
}

/// A profile that pins its post's threadgate (same author and key as the
/// post) pins nothing: the post keeps its place, with no pin reason. The
/// post's own URI does pin it.
#[tokio::test]
async fn a_pinned_threadgate_uri_pins_nothing() {
    let Some(db) = fresh_db("compact_ref_pinned_gate").await else {
        return;
    };
    let p = &db.pool;
    accounts(p).await;
    let older = post(p, BOB, "3kaaaaaaaaaa2", "older", None, None).await;
    let newer = post(p, BOB, "3kaaaaaaaaaa3", "newer", None, None).await;
    let gate = crate::test_support::records::threadgate(p, BOB, "3kaaaaaaaaaa2", &older, None, &[]).await;
    let h = hydrator(p, sets());
    let (anon, req) = (Viewer::anonymous(), Requester::test());
    let pin = |uri: String| async move {
        crate::test_support::actors::set_record(p, BOB, &json!({"createdAt": "2026-01-01T00:00:00Z", "pinnedPost": {"uri": uri, "cid": "bafypin"}})).await;
    };
    let feed = || {
        let (h, anon, req) = (&h, &anon, &req);
        async move { h.get_author_feed(anon, req, BOB, 50, None, "posts_with_replies", true).await.expect("db").expect("view").inner().clone() }
    };
    let pinned = |v: &Value| -> Vec<String> {
        v["feed"]
            .as_array()
            .expect("feed")
            .iter()
            .filter(|i| i["reason"]["$type"] == "app.bsky.feed.defs#reasonPin")
            .map(|i| i["post"]["uri"].as_str().expect("uri").to_string())
            .collect()
    };

    pin(older.clone()).await;
    let v = feed().await;
    assert_eq!(feed_uris(&v), [older.clone(), newer.clone()]);
    assert_eq!(pinned(&v), [older.as_str()]);

    pin(gate.clone()).await;
    let v = feed().await;
    assert_eq!(feed_uris(&v), [newer.clone(), older.clone()]);
    assert_eq!(pinned(&v), Vec::<String>::new());
    db.drop_db().await;
}
