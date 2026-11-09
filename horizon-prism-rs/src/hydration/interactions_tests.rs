//! The interaction rule on the read path, against a Constellation fake that
//! names an account no local row knows: while a group's history is not
//! complete, or a blocking gap is open, its interaction data comes from
//! Constellation; once it is complete it is read from local rows and
//! Constellation is not asked; counts wait for the counts repair; inbound
//! list blocks are read from stored rows once blocks and lists are both
//! complete.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use axum::routing::get;
use serde_json::{Value, json};

use super::testing::{FixedUpstream, actor, exec, hydrator_full, post};
use super::{CountMode, Hydrator, Viewer};
use crate::aggregations::ScopeSets;
use crate::config::FullGroup;
use crate::indexing::{Breadth, PublicMode};
use crate::test_support::fresh_db;
use crate::xrpc::pools::Requester;

const MEMBER: &str = "did:plc:examplememberaaaaaaaaaa";
const BOB: &str = "did:plc:examplebobbbbbbbbbbbbbbb";
const CAROL: &str = "did:plc:examplecarolccccccccccccc";
/// The account only the Constellation fake names.
const DAVE: &str = "did:plc:exampledavedddddddddddddd";
const GENERATION: u64 = 4;

/// A Constellation fake: every listing names `DAVE`, every count is 50, and
/// every call is counted.
async fn constellation(hits: Arc<AtomicUsize>) -> String {
    let h = |hits: Arc<AtomicUsize>, body: Value| {
        move || {
            let hits = Arc::clone(&hits);
            let body = body.clone();
            async move {
                hits.fetch_add(1, Ordering::SeqCst);
                axum::Json(body)
            }
        }
    };
    let router = axum::Router::new()
        .route(
            "/links",
            get(h(Arc::clone(&hits), json!({"total": 1, "linking_records": [{"did": DAVE, "collection": "app.bsky.feed.post", "rkey": "3kzzzzzzzzzz2"}]}))),
        )
        .route("/links/distinct-dids", get(h(Arc::clone(&hits), json!({"total": 1, "linking_dids": [DAVE]}))))
        .route("/links/count/distinct-dids", get(h(Arc::clone(&hits), json!({"total": 50}))))
        .route(
            "/links/all",
            get(h(
                Arc::clone(&hits),
                json!({"links": {
                    "app.bsky.feed.like": {".subject.uri": {"records": 50, "distinct_dids": 50}},
                    "app.bsky.feed.repost": {".subject.uri": {"records": 50, "distinct_dids": 50}},
                    "app.bsky.feed.post": {".reply.parent.uri": {"records": 50, "distinct_dids": 50}}
                }}),
            )),
        );
    crate::test_support::serve_fake(router).await
}

fn full_sets() -> ScopeSets {
    let mut s = ScopeSets::default();
    s.members.insert(MEMBER.into());
    s.scope.insert(MEMBER.into());
    s.public.mode = PublicMode::On;
    s.public.breadth = Breadth::Full;
    s.public.crawl_generation = GENERATION;
    s.full.kinds = FullGroup::ALL.to_vec();
    s.full.counts_auto = true;
    s.full.blocks_local = true;
    s
}

/// Store every group's history: `complete` groups Complete (their counts
/// repaired when `ready`), the others Crawling.
async fn histories(p: &sqlx::PgPool, complete: &[FullGroup], ready: bool) {
    let groups: serde_json::Map<String, Value> = FullGroup::ALL
        .iter()
        .map(|g| {
            let done = complete.contains(g);
            (g.as_str().to_string(), json!({"generation": 1, "state": if done { "complete" } else { "crawling" }, "incomplete": false, "union_done": true, "counts_ready": done && ready}))
        })
        .collect();
    let value = json!({"crawl_generation": GENERATION, "groups": groups});
    sqlx::query(
        "INSERT INTO worker_state (key, value, updated_at) VALUES ('full_groups', $1, now()) \
         ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value",
    )
    .bind(value)
    .execute(p)
    .await
    .expect("histories");
}

async fn likers(h: &Hydrator, post: &str) -> Vec<String> {
    let out = h.get_likes(&Viewer::anonymous(), &Requester::test(), post, 50, None).await.expect("db").expect("view");
    out.inner()["likes"].as_array().expect("likes").iter().filter_map(|l| l["actor"]["did"].as_str().map(str::to_string)).collect()
}

async fn followers(h: &Hydrator, did: &str) -> Vec<String> {
    let out = h.get_followers(&Viewer::anonymous(), &Requester::test(), did, 50, None).await.expect("db").expect("view");
    out.inner()["followers"].as_array().expect("followers").iter().filter_map(|l| l["did"].as_str().map(str::to_string)).collect()
}

/// Each group's interaction data reads
/// today's sources until its history is complete, then local rows with no
/// Constellation call; a blocking gap sends it back, a periodic re-list
/// does not; a group added again after Complete reads Constellation until
/// it completes again; counts wait for the counts repair; a history cutoff
/// keeps today's sources.
#[tokio::test]
async fn interaction_data_reads_local_once_its_group_is_complete() {
    let Some(db) = fresh_db("hydration_interaction_rule").await else {
        return;
    };
    let p = &db.pool;
    for (d, name) in [(MEMBER, "member"), (BOB, "bob"), (CAROL, "carol"), (DAVE, "dave")] {
        actor(p, d, &format!("{name}.example"), name).await;
    }
    let bob_post = post(p, BOB, "3kaaaaaaaaab2", "bob's post", None, None).await;
    crate::test_support::interactions::like(p, CAROL, "3kaaaaaaaaab3", &bob_post).await;
    crate::test_support::graph::follow(p, CAROL, BOB, "3kaaaaaaaaab4").await;
    post(p, CAROL, "3kaaaaaaaaab5", "carol replies", Some(&bob_post), Some(&bob_post)).await;
    let hits = Arc::new(AtomicUsize::new(0));
    let base = constellation(Arc::clone(&hits)).await;
    let h = hydrator_full(p, full_sets(), Arc::new(FixedUpstream::default()), Some(&base));
    let req = Requester::test();
    let sync = || CountMode::Sync { started: std::time::Instant::now() };

    // Nothing complete: today's sources.
    histories(p, &[], false).await;
    assert_eq!(likers(&h, &bob_post).await, [DAVE]);
    assert_eq!(followers(&h, BOB).await, [DAVE]);
    assert_eq!(h.constellation_counts(&req, std::slice::from_ref(&bob_post), sync()).await[&bob_post].likes, Some(50));
    assert!(hits.load(Ordering::SeqCst) >= 3);

    // Every group complete and its counts repaired: local rows only.
    histories(p, &FullGroup::ALL, true).await;
    hits.store(0, Ordering::SeqCst);
    assert_eq!(likers(&h, &bob_post).await, [CAROL]);
    assert_eq!(followers(&h, BOB).await, [CAROL]);
    assert!(h.constellation_counts(&req, &[bob_post.clone(), BOB.to_string()], sync()).await.is_empty(), "nothing to ask Constellation");
    assert_eq!(h.lens_listed_replies(&req, &bob_post, 10, None).await, Some((Vec::new(), None)), "the local rows are every reply");
    // An upstream view's counts are the local ones, zero included.
    let mut view = json!([
        {"uri": bob_post, "cid": "c", "author": {"did": BOB}, "likeCount": 999, "repostCount": 999, "replyCount": 999, "quoteCount": 999},
        {"did": BOB, "followersCount": 999}
    ]);
    h.enrich_counts(&req, &mut view).await.expect("counts");
    assert_eq!((view[0]["likeCount"].clone(), view[0]["repostCount"].clone(), view[0]["replyCount"].clone(), view[0]["quoteCount"].clone()), (json!(1), json!(0), json!(1), json!(0)));
    assert_eq!(view[1]["followersCount"], 1);
    assert_eq!(hits.load(Ordering::SeqCst), 0, "Constellation was not asked");

    // A blocking gap: today's sources again; a periodic re-list blocks nothing.
    exec(p, "INSERT INTO full_gaps (kind, blocking) VALUES ('trim', true)").await;
    assert_eq!(likers(&h, &bob_post).await, [DAVE]);
    exec(p, "UPDATE full_gaps SET resolved_at = now()").await;
    exec(p, "INSERT INTO full_gaps (kind, blocking) VALUES ('periodic', false)").await;
    hits.store(0, Ordering::SeqCst);
    assert_eq!(likers(&h, &bob_post).await, [CAROL]);
    assert_eq!(hits.load(Ordering::SeqCst), 0);

    // `likes` added again after Complete: its likers and counts read
    // Constellation until it completes again; follows stay local.
    let others: Vec<FullGroup> = FullGroup::ALL.into_iter().filter(|g| *g != FullGroup::Likes).collect();
    histories(p, &others, true).await;
    assert_eq!(likers(&h, &bob_post).await, [DAVE]);
    assert_eq!(followers(&h, BOB).await, [CAROL]);
    let c = h.constellation_counts(&req, std::slice::from_ref(&bob_post), sync()).await;
    assert_eq!((c[&bob_post].likes, c[&bob_post].replies), (Some(50), None), "only the like count from Constellation");

    // Complete but not yet repaired: listings local, counts from Constellation.
    histories(p, &FullGroup::ALL, false).await;
    assert_eq!(followers(&h, BOB).await, [CAROL]);
    hits.store(0, Ordering::SeqCst);
    let c = h.constellation_counts(&req, &[BOB.to_string()], sync()).await;
    assert_eq!(c[BOB].followers, Some(50));
    assert!(hits.load(Ordering::SeqCst) > 0);

    // A history cutoff, `counts = "constellation"`, or community: today's sources.
    histories(p, &FullGroup::ALL, true).await;
    let mut cutoff = full_sets();
    cutoff.full.history_cutoff = true;
    let mut constellation_counts = full_sets();
    constellation_counts.full.counts_auto = false;
    let mut community = full_sets();
    community.public.breadth = Breadth::Community;
    for (what, sets) in [("cutoff", cutoff), ("constellation", constellation_counts), ("community", community)] {
        let other = hydrator_full(p, sets, Arc::new(FixedUpstream::default()), Some(&base));
        let c = other.constellation_counts(&req, std::slice::from_ref(&bob_post), sync()).await;
        assert_eq!(c[&bob_post].likes, Some(50), "{what}");
        let local = what == "constellation";
        assert_eq!(likers(&other, &bob_post).await, [if local { CAROL } else { DAVE }], "{what}");
    }
    db.drop_db().await;
}

/// Inbound list blocks come from stored rows once the
/// blocks and lists groups are both complete: a moderation list the viewer
/// is on blocks; a curation list does not; before that, with no Farsight,
/// nobody blocks the viewer through a list.
#[tokio::test]
async fn inbound_list_blocks_read_from_stored_rows_once_complete() {
    let Some(db) = fresh_db("hydration_local_list_blocks").await else {
        return;
    };
    let p = &db.pool;
    for (d, name) in [(MEMBER, "member"), (BOB, "bob"), (CAROL, "carol"), (DAVE, "dave")] {
        actor(p, d, &format!("{name}.example"), name).await;
    }
    exec(p, &format!("INSERT INTO members (did, source, ready_at) VALUES ('{MEMBER}', 'admin', now()) ON CONFLICT DO NOTHING")).await;
    // Dave's moderation list holds the member; Bob subscribes to it as a
    // block. Carol blocks through Dave's curation list, which also holds
    // the member.
    use crate::test_support::records::{list, list_block, list_item};
    let modlist = list(p, DAVE, "3kaaaaaaaaal2", "mods", "app.bsky.graph.defs#modlist").await;
    list_item(p, DAVE, "3kaaaaaaaaal3", &modlist, MEMBER).await;
    list_block(p, BOB, "3kaaaaaaaaal4", &modlist).await;
    let curated = list(p, DAVE, "3kaaaaaaaaal5", "friends", "app.bsky.graph.defs#curatelist").await;
    list_item(p, DAVE, "3kaaaaaaaaal6", &curated, MEMBER).await;
    list_block(p, CAROL, "3kaaaaaaaaal7", &curated).await;
    let h = hydrator_full(p, full_sets(), Arc::new(FixedUpstream::default()), None);
    let me = Viewer { did: Some(MEMBER.into()), hydrate: true };
    let others = [BOB.to_string(), CAROL.to_string(), DAVE.to_string()];

    histories(p, &[FullGroup::Blocks], true).await;
    let v = h.actor_viewer(&me, &others).await.expect("viewer");
    assert!(v.blocked_by.is_empty(), "lists not complete: no local list block, and no Farsight");

    histories(p, &[FullGroup::Blocks, FullGroup::Lists], true).await;
    let v = h.actor_viewer(&me, &others).await.expect("viewer");
    assert_eq!(v.blocked_by.iter().collect::<Vec<_>>(), [BOB], "a moderation list only");
    assert!(v.blocked_either_way(BOB) && !v.blocked_either_way(CAROL));
    assert_eq!(h.local_inbound_list_blocks(MEMBER, &others).await.expect("db").get(BOB), Some(&modlist));
    let r = h.get_relationships(&Requester::test(), MEMBER, &others).await.expect("db").expect("view");
    let rel = r.inner()["relationships"].as_array().expect("relationships").clone();
    assert_eq!(rel[0]["blockedByList"], json!(modlist));
    assert!(rel[1].get("blockedByList").is_none());

    // Blocks keep a member side (`blocks_source = "farsight"`): never local.
    let mut farsight = full_sets();
    farsight.full.blocks_local = false;
    let h = hydrator_full(p, farsight, Arc::new(FixedUpstream::default()), None);
    assert!(h.actor_viewer(&me, &others).await.expect("viewer").blocked_by.is_empty());
    db.drop_db().await;
}

/// The counts repair after a group turns complete marks it ready, so its
/// counts read locally; a group still crawling is not marked.
#[tokio::test]
async fn the_counts_repair_marks_complete_groups_ready() {
    let Some(db) = fresh_db("full_counts_ready").await else {
        return;
    };
    let p = &db.pool;
    histories(p, &[FullGroup::Likes, FullGroup::Follows], false).await;
    let generation = i64::try_from(GENERATION).expect("generation");
    let pending = crate::indexing::crawl::state::counts_pending(p, generation).await.expect("pending");
    assert_eq!(pending, ["follows", "likes"]);
    let marked = crate::indexing::crawl::state::repair_counts_for_complete(p, generation).await.expect("repair");
    assert_eq!(marked, ["follows", "likes"]);
    assert!(crate::indexing::crawl::state::counts_pending(p, generation).await.expect("pending").is_empty());
    let g = crate::indexing::crawl::state::read_groups(p).await.expect("db").expect("groups");
    assert!(g.groups["likes"].counts_ready && !g.groups["posts"].counts_ready);
    // Another generation's histories are left alone.
    assert!(crate::indexing::crawl::state::repair_counts_for_complete(p, generation + 1).await.expect("repair").is_empty());
    db.drop_db().await;
}

/// Blocks between two accounts neither of which is the viewer's business
/// alone: once full indexing holds every block and list, stored rows
/// answer them each way, direct or through a moderation list, for Thread
/// Lens's verdicts (bridging on with no Farsight), Refract's pair checks
/// and a member's outbound `blockingByList` on a list a non-member owns.
#[tokio::test]
async fn pair_blocks_read_stored_rows_once_blocks_and_lists_are_complete() {
    let Some(db) = fresh_db("hydration_local_pair_blocks").await else {
        return;
    };
    let p = &db.pool;
    for (d, name) in [(MEMBER, "member"), (BOB, "bob"), (CAROL, "carol"), (DAVE, "dave")] {
        actor(p, d, &format!("{name}.example"), name).await;
    }
    exec(p, &format!("INSERT INTO members (did, source, ready_at) VALUES ('{MEMBER}', 'admin', now()) ON CONFLICT DO NOTHING")).await;
    use crate::test_support::records::{list, list_block, list_item};
    // Carol blocks Bob directly; Bob blocks the member through Dave's
    // moderation list; the member blocks Bob through the same list.
    crate::test_support::graph::block(p, CAROL, BOB, "3kaaaaaaaab12").await;
    let modlist = list(p, DAVE, "3kaaaaaaaab13", "mods", "app.bsky.graph.defs#modlist").await;
    list_item(p, DAVE, "3kaaaaaaaab14", &modlist, MEMBER).await;
    list_item(p, DAVE, "3kaaaaaaaab15", &modlist, BOB).await;
    list_block(p, BOB, "3kaaaaaaaab16", &modlist).await;
    let member_block = list_block(p, MEMBER, "3kaaaaaaaab17", &modlist).await;

    let others = [CAROL.to_string(), DAVE.to_string(), MEMBER.to_string(), BOB.to_string()];
    let pairs = super::interactions::local_pair_blocks(p, BOB, &others).await.expect("pairs");
    assert_eq!(pairs.len(), 3, "everyone but the account itself is answered");
    assert!(pairs[CAROL].inbound.direct && !pairs[CAROL].outbound.is_block(), "Carol blocks Bob");
    assert_eq!(pairs[MEMBER].outbound.list.as_deref(), Some(modlist.as_str()), "Bob blocks the member through the list");
    assert_eq!(pairs[MEMBER].inbound.list.as_deref(), Some(modlist.as_str()), "and the member blocks Bob through it");
    assert!(!pairs[DAVE].any_block() && pairs[DAVE].vouched);

    // Thread Lens, with no Farsight: bridging is on once stored rows answer.
    let cfg = crate::config::ThreadLensConfig::default();
    let mut off = crate::threadlens::verdict::Judge::new(p.clone(), BOB, None, &cfg);
    off.baseline(&[CAROL.to_string(), DAVE.to_string()]).await.expect("baseline");
    assert!(!off.bridging(), "no Farsight and no local answer: off");
    let mut judge = crate::threadlens::verdict::Judge::new(p.clone(), BOB, None, &cfg).with_local_blocks(true);
    judge.baseline(&[CAROL.to_string(), DAVE.to_string(), MEMBER.to_string()]).await.expect("baseline");
    assert!(judge.bridging());
    use crate::threadlens::verdict::Verdict;
    assert_eq!(judge.verdict(DAVE), Some(Verdict::Clear));
    assert_eq!(judge.verdict(CAROL), Some(Verdict::BlockingYou));
    assert_eq!(judge.verdict(MEMBER), Some(Verdict::BlockedByYou));
    assert_eq!(judge.blocking_list(MEMBER).as_deref(), Some(modlist.as_str()));

    // A member's outbound list block on a non-member's list: only once
    // blocks and lists are complete.
    let h = hydrator_full(p, full_sets(), Arc::new(FixedUpstream::default()), None);
    let by_list = || async {
        let r = h.get_relationships(&Requester::test(), MEMBER, &[BOB.to_string()]).await.expect("db").expect("view");
        r.inner()["relationships"][0].get("blockingByList").cloned()
    };
    histories(p, &[FullGroup::Blocks], true).await;
    assert_eq!(by_list().await, None, "a list a non-member owns is not served before lists are complete");
    histories(p, &[FullGroup::Blocks, FullGroup::Lists], true).await;
    assert_eq!(by_list().await, Some(json!(member_block)));
    db.drop_db().await;
}
