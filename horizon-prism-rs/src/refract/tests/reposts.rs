//! Reposts of restored posts (refract-reposts design §10: A-2 to A-7; A-1
//! is in `aggregations::admit`).

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use serde_json::{Value, json};
use sqlx::PgPool;

use super::harness::{Harness, SUBJECT, tier1};
use crate::aggregations::{ScopeSets, ScopeState};
use crate::config::{BackfillConfig, ScopePolicy};
use crate::firehose::backfill::{Backfill, BackfillSource, ListPage, ListedRecord, PrepareError};
use crate::firehose::writer::{Action, ApplyOptions, OpOutcome, RawOp, SENTINEL_REV, WriteSource, Writer};
use crate::hydration::testing::{FixedUpstream, actor, exec, hydrator_with, post};
use crate::hydration::{Hydrator, Viewer};
use crate::indexing::PublicMode;
use crate::microcosm::{LinkingRecord, LinksPage};
use crate::private_tier::DisabledPrivateTier;
use crate::test_support::fresh_db;
use crate::xrpc::pools::Requester;

const MEMBER: &str = "did:plc:memberaaaaaaaaaaaaaaaaaa";
const STRANGER: &str = "did:plc:strangeraaaaaaaaaaaaaaaa";
const UNFOLLOWED: &str = "did:plc:unfollowedaaaaaaaaaaaaaa";
const GHOST: &str = "did:plc:ghostaaaaaaaaaaaaaaaaaaa";
const R2: &str = "did:plc:restoredtwoaaaaaaaaaaaaa";
const OTHER: &str = "did:plc:otheraaaaaaaaaaaaaaaaaaa";
const CID: &str = "bafyreie5737gdxlw5i64vzichcalba3z2v5n6icifvx5xytvske7mr3hpm";

fn options() -> ApplyOptions {
    ApplyOptions { advance_applied_rev: false, dead_letter_readiness: false, attempts: 1, live: true, subscription_rules: false, source: WriteSource::Firehose }
}

fn op(action: Action, collection: &str, rkey: &str, record: Option<Value>) -> RawOp {
    RawOp { action, collection: collection.into(), rkey: rkey.into(), cid: record.as_ref().map(|_| CID.into()), record }
}

fn repost(subject: &str) -> Value {
    json!({"$type": "app.bsky.feed.repost", "subject": {"uri": subject, "cid": CID}, "createdAt": "2026-10-22T00:00:00.000Z"})
}

/// Off mode; SUBJECT and R2 restored (non-members) when `restored`.
fn sets(mode: PublicMode, restored: bool) -> ScopeSets {
    let mut s = ScopeSets::default();
    s.public.mode = mode;
    s.members.insert(MEMBER.into());
    s.scope.insert(MEMBER.into());
    let r = Arc::make_mut(&mut s.restored);
    r.insert(R2.into(), "sky".into());
    if restored {
        r.insert(SUBJECT.into(), "sky".into());
    }
    s
}

fn writer(p: &PgPool, sets: ScopeSets) -> Writer {
    Writer::new(p.clone(), ScopeState::new(sets, ScopePolicy::MembersAndOneHop, 10_000), Arc::new(DisabledPrivateTier))
}

async fn count(p: &PgPool, sql: &str) -> i64 {
    sqlx::query_scalar(sql).fetch_one(p).await.expect("count")
}

/// A-2, A-5, A-6: in off mode anyone's repost of a restored post is stored
/// from the firehose, a repost of a private post is not, and a delete
/// tombstones it; the on→off sweep spares them; when the restored author
/// ends, strangers' reposts of its posts go, a member's and a restored
/// DID's stay.
#[tokio::test]
async fn a2_a5_a6_stored_spared_and_purged() {
    let Some(db) = fresh_db("refract_reposts_store").await else {
        return;
    };
    let p = &db.pool;
    exec(p, &format!("INSERT INTO members (did, source) VALUES ('{MEMBER}', 'admin')")).await;
    exec(p, &format!("INSERT INTO refract_dids (did, community, pds_host, state, source) VALUES ('{R2}', 'sky', '127.0.0.1', 'restored', 'fill')")).await;
    let w = writer(p, sets(PublicMode::Off, true));
    let subject_post = format!("at://{SUBJECT}/app.bsky.feed.post/3kpost");
    let private_post = format!("at://{SUBJECT}/tools.kryphocron.feed.postPrivate/3kpriv");
    let stranger = w
        .apply(
            STRANGER,
            "3krev0000000a",
            &[
                op(Action::Create, "app.bsky.feed.repost", "3krp1", Some(repost(&subject_post))),
                op(Action::Create, "app.bsky.feed.repost", "3krp2", Some(repost(&subject_post))),
                op(Action::Create, "app.bsky.feed.repost", "3krp3", Some(repost(&private_post))),
                op(Action::Create, "app.bsky.feed.like", "3klk1", Some(json!({"subject": {"uri": subject_post, "cid": CID}, "createdAt": "2026-10-22T00:00:00.000Z"}))),
            ],
            options(),
        )
        .await
        .expect("apply");
    assert_eq!(stranger.outcomes[..2], [OpOutcome::Written, OpOutcome::Written], "{stranger:?}");
    assert!(matches!(stranger.outcomes[2], OpOutcome::Rejected(_)), "a private subject: {stranger:?}");
    assert!(matches!(stranger.outcomes[3], OpOutcome::Rejected(_)), "a like is unchanged: {stranger:?}");
    w.apply(MEMBER, "3krev0000000a", &[op(Action::Create, "app.bsky.feed.repost", "3krpm", Some(repost(&subject_post)))], options()).await.expect("member");
    w.apply(R2, "3krev0000000a", &[op(Action::Create, "app.bsky.feed.repost", "3krpr", Some(repost(&subject_post)))], options()).await.expect("r2");
    assert_eq!(count(p, "SELECT count(*) FROM reposts").await, 4);
    // A delete removes the row and tombstones it.
    w.apply(STRANGER, "3krev0000000b", &[op(Action::Delete, "app.bsky.feed.repost", "3krp2", None)], options()).await.expect("delete");
    assert_eq!(count(p, &format!("SELECT count(*) FROM record_tombstones WHERE uri = 'at://{STRANGER}/app.bsky.feed.repost/3krp2'")).await, 1);
    assert_eq!(count(p, "SELECT count(*) FROM reposts").await, 3);
    // The on→off sweep's predicate keeps them while SUBJECT is in R.
    exec(p, &format!("INSERT INTO refract_dids (did, community, pds_host, state, source) VALUES ('{SUBJECT}', 'sky', '127.0.0.1', 'restored', 'fill')")).await;
    let sweep = crate::indexing::public_sweep::TABLES.iter().find(|t| t.name == "reposts").expect("reposts");
    let swept = sqlx::query(&format!("DELETE FROM reposts t WHERE {}", sweep.delete_when)).execute(p).await.expect("sweep").rows_affected();
    assert_eq!(swept, 0, "spared while the subject's author is in R");
    // SUBJECT ends.
    exec(p, &format!("UPDATE refract_dids SET state = 'ending' WHERE did = '{SUBJECT}'")).await;
    let h = Harness::new(p, sets(PublicMode::Off, false), vec![tier1()]).await;
    assert!(h.refract.purge(SUBJECT).await.expect("purge"));
    let left: Vec<String> = sqlx::query_scalar("SELECT author_did FROM reposts ORDER BY author_did").fetch_all(p).await.expect("left");
    assert_eq!(left, [MEMBER.to_string(), R2.to_string()], "a member's (D-7) and a restored DID's stay");
    db.drop_db().await;
}

/// A-5: in On mode an S author's repost of an ending author's post stays
/// (rule 1); a stranger's goes.
#[tokio::test]
async fn a5_on_mode_keeps_s_reposts() {
    let Some(db) = fresh_db("refract_reposts_on").await else {
        return;
    };
    let p = &db.pool;
    let mut s = sets(PublicMode::On, true);
    s.scope.insert(OTHER.into());
    let w = writer(p, s);
    let subject_post = format!("at://{SUBJECT}/app.bsky.feed.post/3kpost");
    for did in [STRANGER, OTHER] {
        w.apply(did, "3krev0000000a", &[op(Action::Create, "app.bsky.feed.repost", "3krp", Some(repost(&subject_post)))], options()).await.expect("apply");
    }
    assert_eq!(count(p, "SELECT count(*) FROM reposts").await, 2);
    exec(p, &format!("INSERT INTO scope_dids (did, reasons) VALUES ('{OTHER}', 2) ON CONFLICT DO NOTHING")).await;
    exec(p, &format!("INSERT INTO refract_dids (did, community, pds_host, state, source) VALUES ('{SUBJECT}', 'sky', '127.0.0.1', 'ending', 'fill')")).await;
    let h = Harness::new(p, sets(PublicMode::On, false), vec![tier1()]).await;
    assert!(h.refract.purge(SUBJECT).await.expect("purge"));
    let left: Vec<String> = sqlx::query_scalar("SELECT author_did FROM reposts").fetch_all(p).await.expect("left");
    assert_eq!(left, [OTHER.to_string()]);
    db.drop_db().await;
}

const R1: &str = SUBJECT;

fn read_sets() -> ScopeSets {
    let mut s = ScopeSets::default();
    s.public.mode = PublicMode::Off;
    s.members.insert(MEMBER.into());
    s.scope.insert(MEMBER.into());
    Arc::make_mut(&mut s.restored).insert(R1.into(), "sky".into());
    s
}

async fn insert_repost(p: &PgPool, author: &str, rkey: &str, subject: &str, at: &str) -> String {
    let uri = format!("at://{author}/app.bsky.feed.repost/{rkey}");
    exec(p, &format!("INSERT INTO reposts (uri, author_did, subject_uri, rev, created_at) VALUES ('{uri}', '{author}', '{subject}', '3k', '{at}')")).await;
    uri
}

fn remote(did: &str, handle: &str) -> Value {
    json!({"did": did, "handle": handle, "displayName": handle})
}

/// `(post uri, reposter)` of each feed item; reposter empty for a post.
fn items(body: &Value) -> Vec<(String, String)> {
    body["feed"]
        .as_array()
        .expect("feed")
        .iter()
        .map(|i| {
            let by = if i["reason"]["$type"] == "app.bsky.feed.defs#reasonRepost" { i["reason"]["by"]["did"].as_str().unwrap_or("?").to_string() } else { String::new() };
            (i["post"]["uri"].as_str().unwrap_or_default().to_string(), by)
        })
        .collect()
}

/// The member's Refract timeline, R1 served.
async fn read(h: &Hydrator) -> Vec<(String, String)> {
    let viewer = Viewer { did: Some(MEMBER.into()), hydrate: true };
    let (page, next) = h.refract_timeline_items(MEMBER, &[R1.to_string()], None, 50).await.expect("items");
    items(h.refract_feed(&viewer, &Requester::test(), &page, next.as_deref(), 25).await.expect("feed").inner())
}

/// A-3: the member's timeline holds followed accounts' and its own reposts
/// of restored posts, in time order with reasons; not an unfollowed
/// account's, not a repost of a post that is not restored, not one by a
/// reposter no tier renders; a full mute of the subject's author or a
/// repost mute of the reposter drops them.
#[tokio::test]
async fn a3_timeline_reposts_of_restored_posts() {
    let Some(db) = fresh_db("refract_reposts_timeline").await else {
        return;
    };
    let p = &db.pool;
    for (did, handle) in [(R1, "r1.test"), (MEMBER, "m.test")] {
        actor(p, did, handle, handle).await;
    }
    for did in [STRANGER, GHOST, UNFOLLOWED] {
        exec(p, &format!("INSERT INTO follows (uri, author_did, subject_did, rev, created_at) VALUES ('at://{MEMBER}/app.bsky.graph.follow/{}', '{MEMBER}', '{did}', '3k', now())", &did[8..20])).await;
    }
    exec(p, &format!("DELETE FROM follows WHERE subject_did = '{UNFOLLOWED}'")).await;
    let restored_post = post(p, R1, "3krpost", "restored", None, None).await;
    let other_post = format!("at://{OTHER}/app.bsky.feed.post/3kother");
    insert_repost(p, STRANGER, "3ka", &restored_post, "2026-10-20T00:00:00Z").await;
    insert_repost(p, MEMBER, "3kb", &restored_post, "2026-10-21T00:00:00Z").await;
    insert_repost(p, UNFOLLOWED, "3kc", &restored_post, "2026-10-22T00:00:00Z").await;
    insert_repost(p, STRANGER, "3kd", &other_post, "2026-10-23T00:00:00Z").await;
    insert_repost(p, GHOST, "3ke", &restored_post, "2026-10-19T00:00:00Z").await;
    let mut up = FixedUpstream::default();
    for (did, handle) in [(STRANGER, "s.test"), (UNFOLLOWED, "u.test")] {
        up.profiles.insert(did.into(), remote(did, handle));
    }
    let h = hydrator_with(p, read_sets(), Arc::new(up));
    let got = read(&h).await;
    assert_eq!(got, [(restored_post.clone(), MEMBER.to_string()), (restored_post.clone(), STRANGER.to_string())], "{got:?}");
    exec(p, &format!("INSERT INTO mutes (viewer_did, subject_did, scope) VALUES ('{MEMBER}', '{STRANGER}', 'reposts')")).await;
    assert_eq!(read(&h).await, [(restored_post.clone(), MEMBER.to_string())], "a repost mute of the reposter");
    exec(p, &format!("INSERT INTO mutes (viewer_did, subject_did, scope) VALUES ('{MEMBER}', '{R1}', 'full')")).await;
    assert!(read(&h).await.is_empty(), "a full mute of the subject's author");
    db.drop_db().await;
}

/// A-4: an actor's reposts of restored posts, newest first, with
/// `reasonRepost` by the actor, paged by cursor; not a repost of a post that
/// is not restored, nor one of a restored author no longer served.
#[tokio::test]
async fn a4_author_reposts() {
    let Some(db) = fresh_db("refract_author_reposts").await else {
        return;
    };
    let p = &db.pool;
    actor(p, R1, "r1.test", "r1.test").await;
    let first = post(p, R1, "3kfirst", "first", None, None).await;
    let second = post(p, R1, "3ksecond", "second", None, None).await;
    let other_post = format!("at://{OTHER}/app.bsky.feed.post/3kother");
    insert_repost(p, STRANGER, "3ka", &first, "2026-10-20T00:00:00Z").await;
    insert_repost(p, STRANGER, "3kb", &second, "2026-10-21T00:00:00Z").await;
    insert_repost(p, STRANGER, "3kc", &other_post, "2026-10-22T00:00:00Z").await;
    let mut up = FixedUpstream::default();
    up.profiles.insert(STRANGER.into(), remote(STRANGER, "s.test"));
    let h = hydrator_with(p, read_sets(), Arc::new(up));
    let served = vec![R1.to_string()];
    let (page, next) = h.refract_author_reposts_items(STRANGER, &served, None, 1).await.expect("page 1");
    assert_eq!(page.iter().map(|i| i.post_uri.clone()).collect::<Vec<_>>(), std::slice::from_ref(&second));
    let (rest, last) = h.refract_author_reposts_items(STRANGER, &served, next.as_deref(), 1).await.expect("page 2");
    assert_eq!(rest.iter().map(|i| i.post_uri.clone()).collect::<Vec<_>>(), std::slice::from_ref(&first));
    let (end, _) = h.refract_author_reposts_items(STRANGER, &served, last.as_deref(), 1).await.expect("page 3");
    assert!(end.is_empty(), "a repost of a post not restored is not listed");
    let (all, _) = h.refract_author_reposts_items(STRANGER, &served, None, 50).await.expect("all");
    let viewer = Viewer { did: Some(MEMBER.into()), hydrate: false };
    let body = h.refract_feed(&viewer, &Requester::test(), &all, None, 25).await.expect("feed");
    assert_eq!(items(body.inner()), [(second, STRANGER.to_string()), (first, STRANGER.to_string())]);
    let (none, _) = h.refract_author_reposts_items(STRANGER, &[], None, 50).await.expect("none");
    assert!(none.is_empty(), "no restored author served");
    db.drop_db().await;
}

/// A source whose Constellation answers repost links per post.
#[derive(Default)]
struct Links {
    by_post: Mutex<HashMap<String, Vec<(String, String)>>>,
    fail: Mutex<bool>,
}

#[async_trait]
impl BackfillSource for Links {
    async fn prepare(&self, _: &str) -> Result<String, PrepareError> {
        Ok("https://pds.example.com".into())
    }

    async fn latest_rev(&self, _: &str, _: &str) -> Result<String, String> {
        Ok("3kaaaaaaaaaaa".into())
    }

    async fn list_records(&self, _: &str, _: &str, _: &str, _: Option<&str>) -> Result<ListPage, String> {
        Ok(ListPage { records: Vec::new(), cursor: None })
    }

    async fn get_record(&self, _: &str, _: &str, _: &str, _: &str) -> Result<Option<ListedRecord>, String> {
        Ok(None)
    }

    async fn inbound_blocks(&self, _: &str, _: Option<&str>) -> Result<LinksPage, String> {
        Ok(LinksPage { total: 0, linking_records: Vec::new(), cursor: None })
    }

    async fn repost_links(&self, post: &str, _: u32) -> Result<LinksPage, String> {
        if *self.fail.lock().expect("fail") {
            return Err("down".into());
        }
        let found = self.by_post.lock().expect("links").get(post).cloned().unwrap_or_default();
        let linking_records = found.into_iter().map(|(did, rkey)| LinkingRecord { did, collection: "app.bsky.feed.repost".into(), rkey }).collect();
        Ok(LinksPage { total: 0, linking_records, cursor: None })
    }
}

/// A-7: the fill turns Constellation links into rows (the rkey's time, the
/// sentinel rev, no subject CID), skips a tombstoned URI, sets
/// `reposts_filled_at` and is not run again; a DID with a `refract` job
/// queued is not claimed; an error leaves the column unset.
#[tokio::test]
async fn a7_the_fill() {
    let Some(db) = fresh_db("refract_repost_fill").await else {
        return;
    };
    let p = &db.pool;
    exec(p, &format!("INSERT INTO refract_dids (did, community, pds_host, state, source, since) VALUES ('{SUBJECT}', 'sky', '127.0.0.1', 'restored', 'fill', now())")).await;
    let target = post(p, SUBJECT, "3kfilled", "filled", None, None).await;
    let tid = "3l2uygzaa2c2a";
    let links = Arc::new(Links::default());
    links.by_post.lock().expect("links").insert(
        target.clone(),
        vec![(STRANGER.into(), tid.into()), (OTHER.into(), "3kgone".into()), ("not a did".into(), "3kbad".into())],
    );
    exec(p, &format!("INSERT INTO record_tombstones (uri, rev) VALUES ('at://{OTHER}/app.bsky.feed.repost/3kgone', '3k')")).await;
    let w = writer(p, sets(PublicMode::Off, true));
    let b = Backfill::new(w, links.clone(), BackfillConfig::default(), 1);
    // A queued refract job holds it back.
    exec(p, &format!("INSERT INTO backfill_jobs (did, depth, state) VALUES ('{SUBJECT}', 'refract', 'queued')")).await;
    assert!(b.repost_fill_once().await.expect("fill").is_none(), "its posts may not be there yet");
    exec(p, "UPDATE backfill_jobs SET state = 'done'").await;
    *links.fail.lock().expect("fail") = true;
    assert_eq!(b.repost_fill_once().await.expect("fill").map(|(_, o)| o), Some("error"));
    assert_eq!(count(p, "SELECT count(*) FROM refract_dids WHERE reposts_filled_at IS NULL").await, 1);
    assert!(b.repost_fill_once().await.expect("fill").is_none(), "a failed DID waits");
    *links.fail.lock().expect("fail") = false;
    let b = Backfill::new(writer(p, sets(PublicMode::Off, true)), links.clone(), BackfillConfig::default(), 1);
    assert_eq!(b.repost_fill_once().await.expect("fill").map(|(_, o)| o), Some("done"));
    let rows: Vec<(String, Option<String>, String, chrono::DateTime<chrono::Utc>)> =
        sqlx::query_as("SELECT uri, subject_cid, rev, created_at FROM reposts").fetch_all(p).await.expect("rows");
    assert_eq!(rows.len(), 1, "the tombstoned and the malformed are skipped: {rows:?}");
    assert_eq!(rows[0].0, format!("at://{STRANGER}/app.bsky.feed.repost/{tid}"));
    assert_eq!((rows[0].1.as_deref(), rows[0].2.as_str()), (None, SENTINEL_REV));
    assert_eq!(Some(rows[0].3), crate::hydration::feed::tid_timestamp(tid));
    assert_eq!(count(p, "SELECT count(*) FROM refract_dids WHERE reposts_filled_at IS NOT NULL").await, 1);
    assert!(b.repost_fill_once().await.expect("fill").is_none(), "once per restore");
    db.drop_db().await;
}
