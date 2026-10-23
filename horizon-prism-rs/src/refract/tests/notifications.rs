//! Refract notifications (refract design §5.3, §7.1, §13: T-11, T-11b,
//! T-11c).

use std::sync::Arc;

use serde_json::{Value, json};
use sqlx::PgPool;

use super::harness::SUBJECT;
use crate::aggregations::{ScopeSets, ScopeState};
use crate::config::ScopePolicy;
use crate::firehose::writer::{Action, ApplyOptions, RawOp, WriteSource, Writer};
use crate::hydration::testing::{FixedUpstream, hydrator_with};
use crate::hydration::{Hydrator, Viewer};
use crate::indexing::PublicMode;
use crate::private_tier::DisabledPrivateTier;
use crate::refract::RefractHidden;
use crate::test_support::fresh_db;
use crate::xrpc::pools::Requester;

const MEMBER: &str = "did:plc:memberaaaaaaaaaaaaaaaaaa";
const CID: &str = "bafyreie5737gdxlw5i64vzichcalba3z2v5n6icifvx5xytvske7mr3hpm";

fn options(source: WriteSource) -> ApplyOptions {
    ApplyOptions { advance_applied_rev: false, dead_letter_readiness: false, attempts: 1, live: source == WriteSource::Firehose, subscription_rules: false, source }
}

fn create(collection: &str, rkey: &str, record: Value) -> RawOp {
    RawOp { action: Action::Create, collection: collection.into(), rkey: rkey.into(), cid: Some(CID.into()), record: Some(record) }
}

fn sets(mode: PublicMode) -> ScopeSets {
    let mut s = ScopeSets::default();
    s.public.mode = mode;
    s.members.insert(MEMBER.into());
    s.scope.insert(MEMBER.into());
    Arc::make_mut(&mut s.restored).insert(SUBJECT.into(), "sky".into());
    s
}

async fn count(p: &PgPool, sql: &str) -> i64 {
    sqlx::query_scalar(sql).fetch_one(p).await.expect("count")
}

fn writer(p: &PgPool, sets: ScopeSets) -> Writer {
    Writer::new(p.clone(), ScopeState::new(sets, ScopePolicy::MembersAndOneHop, 10_000), Arc::new(DisabledPrivateTier))
}

fn member_post() -> String {
    format!("at://{MEMBER}/app.bsky.feed.post/3kmember")
}

/// A restored author's reply to, like of, and follow of a member, from the
/// firehose.
async fn write(w: &Writer) {
    let parent = member_post();
    let ops = vec![
        create(
            "app.bsky.feed.post",
            "3kreply",
            json!({"$type": "app.bsky.feed.post", "text": "hi", "createdAt": "2026-10-22T00:00:00.000Z",
                   "reply": {"root": {"uri": parent, "cid": CID}, "parent": {"uri": parent, "cid": CID}}}),
        ),
        create("app.bsky.feed.like", "3klike", json!({"$type": "app.bsky.feed.like", "subject": {"uri": parent, "cid": CID}, "createdAt": "2026-10-22T00:00:00.000Z"})),
        create("app.bsky.graph.follow", "3kfollow", json!({"$type": "app.bsky.graph.follow", "subject": MEMBER, "createdAt": "2026-10-22T00:00:00.000Z"})),
    ];
    w.apply(SUBJECT, "3krev0000000a", &ops, options(WriteSource::Firehose)).await.expect("apply");
}

async fn list(h: &Hydrator, viewer: &Viewer, member: bool) -> Value {
    h.refract_list_notifications(viewer, &Requester::test(), member, 50, None, None).await.expect("db").expect("ok").inner().clone()
}

async fn unread(h: &Hydrator, viewer: &Viewer, member: bool) -> i64 {
    let v = h.refract_unread_count(viewer, &Requester::test(), member, None, None).await.expect("db").expect("ok");
    v.inner()["count"].as_i64().expect("count")
}

/// T-11: off mode: a restored author's records from the live stream notify
/// the member in `refract_notifications` (and nothing in `notifications`);
/// a backfilled one notifies nobody. The member reads them, with no
/// `isRead`; a non-member gets an empty list and a zero count.
#[tokio::test]
async fn t11_written_from_the_stream_and_read_by_members() {
    let Some(db) = fresh_db("refract_notifications_write").await else {
        return;
    };
    let p = &db.pool;
    let w = writer(p, sets(PublicMode::Off));
    write(&w).await;
    assert_eq!(count(p, "SELECT count(*) FROM refract_notifications WHERE recipient_did = 'did:plc:memberaaaaaaaaaaaaaaaaaa'").await, 3);
    assert_eq!(count(p, "SELECT count(*) FROM notifications").await, 0);
    let backfilled = create("app.bsky.feed.repost", "3krepost", json!({"$type": "app.bsky.feed.repost", "subject": {"uri": member_post(), "cid": CID}, "createdAt": "2026-10-22T00:00:00.000Z"}));
    w.apply(SUBJECT, "3krev0000000b", &[backfilled], options(WriteSource::Backfill)).await.expect("apply");
    assert_eq!(count(p, "SELECT count(*) FROM refract_notifications").await, 3, "a backfill notifies nobody");
    let h = hydrator_with(p, sets(PublicMode::Off), Arc::new(FixedUpstream::default()));
    let me = Viewer { did: Some(MEMBER.into()), hydrate: true };
    let got = list(&h, &me, true).await;
    let items = got["notifications"].as_array().expect("notifications");
    let mut reasons: Vec<&str> = items.iter().filter_map(|n| n["reason"].as_str()).collect();
    reasons.sort_unstable();
    assert_eq!(reasons, ["follow", "like", "reply"], "{got}");
    assert!(items.iter().all(|n| n.get("isRead").is_none()));
    assert_eq!(unread(&h, &me, true).await, 3);
    let stranger = Viewer { did: Some("did:plc:strangeraaaaaaaaaaaaaaaa".into()), hydrate: false };
    assert_eq!(list(&h, &stranger, false).await, json!({"notifications": []}));
    assert_eq!(unread(&h, &stranger, false).await, 0);
    // A delete takes its row with it.
    let delete = RawOp { action: Action::Delete, collection: "app.bsky.feed.like".into(), rkey: "3klike".into(), cid: None, record: None };
    w.apply(SUBJECT, "3krev0000000c", &[delete], options(WriteSource::Firehose)).await.expect("delete");
    assert_eq!(count(p, "SELECT count(*) FROM refract_notifications").await, 2);
    db.drop_db().await;
}

/// T-11b: a record with a gone label, or an author pending removal, is left
/// out of the list (the gate) and of the count (SQL).
#[tokio::test]
async fn t11b_the_gate_and_the_count_agree() {
    let Some(db) = fresh_db("refract_notifications_gate").await else {
        return;
    };
    let p = &db.pool;
    write(&writer(p, sets(PublicMode::Off))).await;
    let mut s = sets(PublicMode::Off);
    let mut hidden = RefractHidden::default();
    hidden.add_record(&format!("at://{SUBJECT}/app.bsky.feed.post/3kreply"), None);
    s.refract_hidden = Arc::new(hidden);
    let h = hydrator_with(p, s, Arc::new(FixedUpstream::default()));
    let me = Viewer { did: Some(MEMBER.into()), hydrate: true };
    let got = list(&h, &me, true).await;
    let reasons: Vec<&str> = got["notifications"].as_array().expect("n").iter().filter_map(|n| n["reason"].as_str()).collect();
    assert!(!reasons.contains(&"reply"), "{got}");
    assert_eq!(reasons.len(), 2);
    assert_eq!(unread(&h, &me, true).await, 2);
    // The author pending removal: hidden wholesale (a non-member outside S).
    {
        let mut w = h.scope().write_guard().await;
        Arc::make_mut(&mut w.refract_hidden).update(SUBJECT, |r| r.pending = true);
    }
    assert_eq!(list(&h, &me, true).await["notifications"], json!([]));
    assert_eq!(unread(&h, &me, true).await, 0);
    db.drop_db().await;
}

/// T-11c: a member that is not ready: its mutes apply in the scan and the
/// count, from its own tables.
#[tokio::test]
async fn t11c_a_member_not_ready_keeps_its_mutes() {
    let Some(db) = fresh_db("refract_notifications_mutes").await else {
        return;
    };
    let p = &db.pool;
    write(&writer(p, sets(PublicMode::Off))).await;
    let h = hydrator_with(p, sets(PublicMode::Off), Arc::new(FixedUpstream::default()));
    let me = Viewer { did: Some(MEMBER.into()), hydrate: false };
    assert_eq!(unread(&h, &me, true).await, 3);
    sqlx::query("INSERT INTO mutes (viewer_did, subject_did, scope) VALUES ($1, $2, 'full')")
        .bind(MEMBER)
        .bind(SUBJECT)
        .execute(p)
        .await
        .expect("mute");
    assert_eq!(list(&h, &me, true).await["notifications"], json!([]));
    assert_eq!(unread(&h, &me, true).await, 0);
    db.drop_db().await;
}
