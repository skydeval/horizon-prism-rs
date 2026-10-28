//! Degraded mode (docs/internal/design/degraded-mode.md §10): the head
//! fallback's never-miss cases (T-DM3, T-DM3b), the climb (T-DM4), bind
//! freshness (T-DM5), the fresh watermark unchanged (T-DM13), threads while
//! degraded (T-DM14) and the author's own read (T-DM16). "Degraded" is a
//! private relay that never reported, so the private watermark is `None`.

use std::collections::HashSet;
use super::*;
use crate::firehose::writer::{ChainStep, advance_applied_rev, chain_commit};
use crate::private_tier::handlers::tests::chain_state;
use crate::private_tier::pipeline;
use crate::test_support::noted::Noted;
use crate::test_support::{hooks, series};

/// Every fixture repo's applied rev, and its chain once [`chain_all`] ran.
const C0: &str = "3kzzzzzzzzzz2";
/// The commit the live relay dropped and the stalled one still carries.
const M: &str = "3l22222222222";
/// The commit after it, which the live relay delivered.
const H: &str = "3l22222222223";

/// How often `name` was noted.
fn count(noted: &[&'static str], name: &str) -> usize {
    noted.iter().filter(|n| **n == name).count()
}

/// A local Prometheus recorder for this thread, which a current-thread test
/// runtime's server tasks report to.
struct Counted {
    handle: metrics_exporter_prometheus::PrometheusHandle,
    _guard: metrics::LocalRecorderGuard<'static>,
}

impl Counted {
    fn new() -> Self {
        let recorder: &'static metrics_exporter_prometheus::PrometheusRecorder =
            Box::leak(Box::new(metrics_exporter_prometheus::PrometheusBuilder::new().build_recorder()));
        let handle = recorder.handle();
        Self { handle, _guard: metrics::set_default_local_recorder(recorder) }
    }

    fn outcome(&self, outcome: &str) -> f64 {
        series(&self.handle.render(), pipeline::METRIC_OUTCOMES, &[&format!("outcome=\"{outcome}\"")]).unwrap_or(0.0)
    }

    fn fallback(&self, result: &str) -> f64 {
        series(&self.handle.render(), pipeline::METRIC_FALLBACK, &[&format!("result=\"{result}\"")]).unwrap_or(0.0)
    }
}

/// [`serve`] with every head checked in the last 50 seconds kept fresh, and
/// the watermark left alone, so a degraded server stays degraded.
async fn serve_heads(state: PrivateState) -> String {
    let pool = state.env.pool.clone();
    tokio::spawn(async move {
        loop {
            let _ = sqlx::query("UPDATE sync_state SET head_checked_at = now() WHERE head_checked_at > now() - interval '50 seconds'")
                .execute(&pool)
                .await;
            tokio::time::sleep(Duration::from_secs(2)).await;
        }
    });
    serve(state).await
}

/// Every repo's chain at its applied rev: the fallback can serve them all.
async fn chain_all(pool: &PgPool) {
    exec(pool, "UPDATE sync_state SET chain_rev = applied_rev").await;
}

/// A hole on the live relay for `x`: its PDS head is [`H`], the live relay
/// delivered `H` (applied, a link), and [`M`] did not arrive.
async fn hole(pool: &PgPool, x: &str) {
    let mut tx = pool.begin().await.expect("tx");
    advance_applied_rev(&mut tx, x, H, true).await.expect("advance");
    assert_eq!(chain_commit(&mut tx, x, H, M).await.expect("chain"), ChainStep::Linked);
    tx.commit().await.expect("commit");
    sqlx::query("UPDATE sync_state SET head_rev = $2, head_checked_at = now() WHERE did = $1")
        .bind(x)
        .bind(H)
        .execute(pool)
        .await
        .expect("head");
}

/// The stalled relay's copy of [`M`] arrives for `x`: it chains, and the
/// advance runs through `H`.
async fn arrive(pool: &PgPool, x: &str) {
    let mut tx = pool.begin().await.expect("tx");
    advance_applied_rev(&mut tx, x, M, true).await.expect("advance");
    assert_eq!(chain_commit(&mut tx, x, M, C0).await.expect("chain"), ChainStep::Chained);
    tx.commit().await.expect("commit");
    let chain: Option<String> = sqlx::query_scalar("SELECT chain_rev FROM sync_state WHERE did = $1").bind(x).fetch_one(pool).await.expect("chain");
    assert_eq!(chain.as_deref(), Some(H));
}

/// A settle at [`H`] for `x` covering everything: the other way a hole
/// mends.
async fn settle(pool: &PgPool, x: &str) {
    exec(pool, &format!("UPDATE sync_state SET post_listed_rev = '{H}' WHERE did = '{x}'")).await;
    let mut tx = pool.begin().await.expect("tx");
    let settled = crate::firehose::writer::advance_applied_rev_with_follows(&mut tx, x, H, None).await.expect("settle");
    tx.commit().await.expect("commit");
    assert!(settled.covered);
}

/// Three servers on one [`Walk`]: degraded with the fallback on, degraded
/// with it off, and fresh.
struct Servers {
    on: String,
    off: String,
    fresh: String,
}

async fn servers(w: &Walk) -> Servers {
    let read_on = PrivateReadConfig::default();
    let read_off = PrivateReadConfig { head_fallback: false, ..PrivateReadConfig::default() };
    Servers {
        on: serve_heads(w.f.state_read(w.f.app.clone(), None, Duration::ZERO, &TEST_CURSOR_KEY, &read_on)).await,
        off: serve_heads(w.f.state_read(w.f.app.clone(), None, Duration::ZERO, &TEST_CURSOR_KEY, &read_off)).await,
        fresh: w.base.clone(),
    }
}

/// Whether `reader` is served `uri` by `getPosts` at `base`.
async fn served(base: &str, reader: &str, uri: &str) -> bool {
    !posts_of(base, reader, &[uri]).await.1.is_empty()
}

/// Whether `reader` is served `uri` as `getPostThread`'s anchor at `base`:
/// HEAD's sequential path reads an anchor that fails the prefilter, so this
/// reaches the pipeline where `getPosts` would not.
async fn thread_served(base: &str, reader: &str, uri: &str) -> bool {
    let (status, body, _) = get(base, THREAD, &format!("uri={uri}"), reader, key_of(reader)).await;
    status == 200 && serde_json::from_str::<Value>(&body).is_ok_and(|v| v["thread"]["post"]["uri"] == uri)
}

/// `n` DIDs of this module's own, `did:plc:` and 24 base-32 characters.
fn dids(tag: &str, n: usize) -> Vec<String> {
    const ALPHABET: &[u8; 32] = b"abcdefghijklmnopqrstuvwxyz234567";
    (0..n)
        .map(|i| format!("did:plc:{tag}{}{}{}", "q".repeat(22 - tag.len()), ALPHABET[i / 32] as char, ALPHABET[i % 32] as char))
        .collect()
}

/// One row of §2.2's table: the change a commit by `x` makes, carried only
/// by the stalled relay.
#[derive(Debug, Clone, Copy)]
enum Change {
    /// The control: an unrelated commit.
    Nothing,
    ReaderBlocksAuthor,
    AuthorBlocksReader,
    OwnerBlocksReader,
    OwnerRemovesReader,
    OwnerChangesMode,
    ReaderUnfollowsOwner,
    OwnerUnfollowsReader,
    ChainAuthorBlocksReader,
    ChainAuthorUnfollowsOwner,
    ChainAuthorDeletesRow,
}

impl Change {
    /// The audience mode the case starts from.
    fn mode(self) -> &'static str {
        match self {
            Self::OwnerRemovesReader => "list",
            Self::OwnerUnfollowsReader => "following",
            _ => "followers",
        }
    }

    /// The repo whose commit carries the change.
    fn repo(self) -> &'static str {
        match self {
            Self::Nothing | Self::OwnerBlocksReader | Self::OwnerRemovesReader | Self::OwnerChangesMode | Self::OwnerUnfollowsReader => ALICE,
            Self::ReaderBlocksAuthor | Self::ReaderUnfollowsOwner => DAVE,
            Self::AuthorBlocksReader => BOB,
            Self::ChainAuthorBlocksReader | Self::ChainAuthorUnfollowsOwner | Self::ChainAuthorDeletesRow => CAROL,
        }
    }

    async fn apply(self, w: &Walk, chain_row: &str) {
        let pool = &w.f.pool;
        match self {
            Self::Nothing => {}
            Self::ReaderBlocksAuthor => exec(pool, &block_sql(DAVE, BOB)).await,
            Self::AuthorBlocksReader => exec(pool, &block_sql(BOB, DAVE)).await,
            Self::OwnerBlocksReader => exec(pool, &block_sql(ALICE, DAVE)).await,
            Self::OwnerRemovesReader => w.edit_audience("3kaudmain222", "list", &[BOB, CAROL], REV2).await,
            Self::OwnerChangesMode => w.edit_audience("3kaudmain222", "list", &[BOB, CAROL], REV2).await,
            Self::ReaderUnfollowsOwner => exec(pool, &format!("DELETE FROM follows WHERE author_did = '{DAVE}' AND subject_did = '{ALICE}'")).await,
            Self::OwnerUnfollowsReader => exec(pool, &format!("DELETE FROM follows WHERE author_did = '{ALICE}' AND subject_did = '{DAVE}'")).await,
            Self::ChainAuthorBlocksReader => exec(pool, &block_sql(CAROL, DAVE)).await,
            Self::ChainAuthorUnfollowsOwner => exec(pool, &format!("DELETE FROM follows WHERE author_did = '{CAROL}' AND subject_did = '{ALICE}'")).await,
            Self::ChainAuthorDeletesRow => {
                let rkey = chain_row.rsplit('/').next().expect("rkey");
                w.f.apply_op(CAROL, Action::Delete, NSID_POST_PRIVATE, rkey, REV2, None).await;
            }
        }
    }
}

/// One T-DM3 case: Alice's root under her audience, Carol's reply to it,
/// Bob's reply to Carol's (P), read by Dave. Degraded, every repo current:
/// a hit. Then the case's change is made by a commit only the stalled relay
/// carries while the live relay delivered the next commit of that repo:
/// with the fallback on P misses as `fallback_behind`, with it off as
/// `stale`, while the fresh baseline (whose head check is `applied_rev ≥
/// head` alone) still serves it. When the stalled copy arrives (or, for
/// the control, a settle runs), P is read under the new facts: the change
/// makes it a miss for the read rule's own reason, never `fallback_behind`;
/// the control hits again.
async fn never_miss_case(change: Change, mend_by_settle: bool) {
    let test = format!("dm3-{change:?}").to_lowercase();
    let mode = change.mode();
    let members: &[&str] = if mode == "list" { &[BOB, CAROL, DAVE] } else { &[] };
    let Some(mut w) = Walk::new_in(&test, PublicMode::On, Layout::SamePds, Some(mode), members, &[], Arc::new(NoHeads)).await else {
        return;
    };
    if mode == "following" {
        for d in [BOB, CAROL, DAVE] {
            exec(&w.f.pool, &follow_sql(ALICE, d)).await;
        }
        set_follows_verified(&w.f.pool, ALICE, true).await;
    }
    let root = w.root.clone();
    let c = w.reply(CAROL, "3kdmcarol222", &root).await;
    let p = w.reply(BOB, "3kdmbob22222", &c).await;
    w.f.ready_all().await;
    chain_all(&w.f.pool).await;
    let s = servers(&w).await;
    let counted = Counted::new();

    let base = (served(&s.on, DAVE, &p).await, served(&s.off, DAVE, &p).await, served(&s.fresh, DAVE, &p).await);
    w.check(base == (true, false, true), format!("{change:?}: before the hole, on/off/fresh {base:?}"));
    w.check(counted.fallback("passed") >= 1.0, format!("{change:?}: the fallback passed"));

    let x = change.repo();
    hole(&w.f.pool, x).await;
    let (behind, stale) = (counted.outcome("fallback_behind"), counted.outcome("stale"));
    let during = (served(&s.on, DAVE, &p).await, served(&s.off, DAVE, &p).await, served(&s.fresh, DAVE, &p).await);
    w.check(during == (false, false, true), format!("{change:?}: during the hole, on/off/fresh {during:?}"));
    w.check(counted.outcome("fallback_behind") == behind + 1.0, format!("{change:?}: the fallback's miss is fallback_behind"));
    w.check(counted.outcome("stale") == stale + 1.0, format!("{change:?}: with the fallback off, stale"));

    // The change, then the mend.
    change.apply(&w, &c).await;
    if mend_by_settle {
        settle(&w.f.pool, x).await;
    } else {
        arrive(&w.f.pool, x).await;
    }
    let behind = counted.outcome("fallback_behind");
    let after = served(&s.on, DAVE, &p).await;
    let expected = matches!(change, Change::Nothing);
    w.check(after == expected, format!("{change:?}: after the mend, served {after}"));
    w.check(counted.outcome("fallback_behind") == behind, format!("{change:?}: after the mend no miss is fallback_behind"));
    w.check(served(&s.fresh, DAVE, &p).await == expected, format!("{change:?}: the fresh read agrees"));
    w.finish().await;
}

/// T-DM3, the control: an unrelated commit; hits again once the stalled
/// copy arrives, and once a settle runs.
#[tokio::test]
async fn tdm3_control() {
    never_miss_case(Change::Nothing, false).await;
    never_miss_case(Change::Nothing, true).await;
}

#[tokio::test]
async fn tdm3_reader_blocks_author() {
    never_miss_case(Change::ReaderBlocksAuthor, false).await;
}

#[tokio::test]
async fn tdm3_author_blocks_reader() {
    never_miss_case(Change::AuthorBlocksReader, false).await;
}

#[tokio::test]
async fn tdm3_owner_blocks_reader() {
    never_miss_case(Change::OwnerBlocksReader, true).await;
}

#[tokio::test]
async fn tdm3_owner_removes_reader_from_a_list() {
    never_miss_case(Change::OwnerRemovesReader, false).await;
}

#[tokio::test]
async fn tdm3_owner_changes_mode() {
    never_miss_case(Change::OwnerChangesMode, false).await;
}

#[tokio::test]
async fn tdm3_reader_unfollows_owner_under_followers() {
    never_miss_case(Change::ReaderUnfollowsOwner, false).await;
}

#[tokio::test]
async fn tdm3_owner_unfollows_reader_under_following() {
    never_miss_case(Change::OwnerUnfollowsReader, false).await;
}

#[tokio::test]
async fn tdm3_chain_author_blocks_reader() {
    never_miss_case(Change::ChainAuthorBlocksReader, false).await;
}

#[tokio::test]
async fn tdm3_chain_author_unfollows_owner() {
    never_miss_case(Change::ChainAuthorUnfollowsOwner, true).await;
}

#[tokio::test]
async fn tdm3_chain_author_deletes_its_row() {
    never_miss_case(Change::ChainAuthorDeletesRow, false).await;
}

/// T-DM3b (review 1, LB1): the order of step 3′ and the snapshot.
/// - Paused after step 3′, Carol (d) blocks Dave in a commit that chains,
///   so Carol stays current: the snapshot holds the block and the read
///   misses for the read rule, not `fallback_behind`.
/// - Paused between the climb and fact 8, a chain row by an author outside
///   the climb appears: `walk_outside`, a miss.
#[tokio::test]
async fn tdm3b_the_snapshot_follows_step_3_prime() {
    let Some(mut w) = Walk::new_in("dm3b-order", PublicMode::On, Layout::SamePds, Some("followers"), &[], &[], Arc::new(NoHeads)).await else {
        return;
    };
    let root = w.root.clone();
    let c = w.reply(CAROL, "3kdmcarol222", &root).await;
    let p = w.reply(BOB, "3kdmbob22222", &c).await;
    w.f.ready_all().await;
    chain_all(&w.f.pool).await;
    let s = servers(&w).await;
    let counted = Counted::new();
    w.check(served(&s.on, DAVE, &p).await, "P is served before".to_string());

    // A block that chains, made while the read is paused after step 3′.
    {
        let armed = hooks::arm(hooks::FALLBACK_AFTER_CHECK, &p);
        let base = s.on.clone();
        let uri = p.clone();
        let read = tokio::spawn(async move { served(&base, DAVE, &uri).await });
        armed.reached().await;
        exec(&w.f.pool, &block_sql(CAROL, DAVE)).await;
        let mut tx = w.f.pool.begin().await.expect("tx");
        advance_applied_rev(&mut tx, CAROL, M, true).await.expect("advance");
        assert_eq!(chain_commit(&mut tx, CAROL, M, C0).await.expect("chain"), ChainStep::Chained);
        tx.commit().await.expect("commit");
        exec(&w.f.pool, &format!("UPDATE sync_state SET head_rev = '{M}', head_checked_at = now() WHERE did = '{CAROL}'")).await;
        let (behind, hits) = (counted.outcome("fallback_behind"), counted.outcome("hit"));
        armed.release();
        let got = read.await.expect("read");
        w.check(!got, "the block the snapshot holds denies the read".to_string());
        w.check(counted.outcome("hit") == hits && counted.outcome("fallback_behind") == behind, "a denial, not a fallback miss".to_string());
    }
    exec(&w.f.pool, &format!("DELETE FROM blocks WHERE author_did = '{CAROL}'")).await;
    w.check(served(&s.on, DAVE, &p).await, "P is served again".to_string());

    // A chain row outside the climb: Bob's reply Q to a post X that is not
    // stored when the climb runs; Carol's X appears before fact 8. Q fails
    // the prefilter until X exists, so it is read as a thread's anchor.
    let x = format!("at://{CAROL}/{NSID_POST_PRIVATE}/3kdmlater222");
    let q = w.reply(BOB, "3kdmbobq2222", &x).await;
    w.f.ready_all().await;
    {
        let armed = hooks::arm(hooks::FALLBACK_BEFORE_FACT8, &q);
        let base = s.on.clone();
        let uri = q.clone();
        let read = tokio::spawn(async move { thread_served(&base, DAVE, &uri).await });
        armed.reached().await;
        let made = w.reply(CAROL, "3kdmlater222", &root).await;
        assert_eq!(made, x);
        let outside = counted.fallback("walk_outside");
        armed.release();
        let got = read.await.expect("read");
        w.check(!got, "a walk that read a repo outside the climb misses".to_string());
        w.check(counted.fallback("walk_outside") == outside + 1.0, "counted walk_outside".to_string());
    }
    // Climbed afresh, the same post reads.
    w.check(served(&s.on, DAVE, &q).await, "Q is served once climbed with X".to_string());
    w.finish().await;
}

/// T-DM4 (degraded-mode design §2.2, §2.6): the climb statement. A thread
/// chain stops at the root, the stop row's author included, or at a thread
/// reply by the root's author; a branch at its branch root; a cycle at the
/// cap; an owner's own thread reply and a top-level post are not walked;
/// siblings share one climb.
#[tokio::test]
async fn tdm4_the_climb() {
    let Some(w) = Walk::new_in("dm4-climb", PublicMode::On, Layout::SamePds, Some("followers"), &[], &[], Arc::new(NoHeads)).await else {
        return;
    };
    let pool = &w.f.pool;
    let root = w.root.clone();
    let c = w.reply(CAROL, "3kdm4c222222", &root).await;
    let b = w.reply(BOB, "3kdm4b222222", &c).await;
    let p = w.reply(DAVE, "3kdm4d222222", &b).await;
    let sibling = w.reply(CAROL, "3kdm4s222222", &b).await;
    // Alice's own thread reply mid-chain: the stop for what hangs below it.
    let own = w.reply(ALICE, "3kdm4a222222", &c).await;
    let below_own = w.reply(BOB, "3kdm4e222222", &own).await;
    // A branch: Alice's branch root under a public post, Carol's reply, Bob's.
    let public = format!("at://{DAVE}/app.bsky.feed.post/3kdm4pub2222");
    let branch_root = w.f.private_post(ALICE, "3kdm4br22222", &w.aud, Some((&public, &public))).await;
    let bc = w.f.private_post(CAROL, "3kdm4bc22222", &w.aud, Some((&public, &branch_root))).await;
    let bb = w.f.private_post(BOB, "3kdm4bb22222", &w.aud, Some((&public, &bc))).await;
    // A cycle under the root: two rows naming each other as parents.
    let y = format!("at://{CAROL}/{NSID_POST_PRIVATE}/3kdm4y222222");
    let z = w.f.private_post(BOB, "3kdm4z222222", &w.aud, Some((&root, &y))).await;
    w.f.private_post(CAROL, "3kdm4y222222", &w.aud, Some((&root, &z))).await;
    let in_cycle = w.reply(DAVE, "3kdm4x222222", &z).await;

    let uris = vec![p.clone(), sibling.clone(), below_own.clone(), bb.clone(), in_cycle.clone(), root.clone(), own.clone(), branch_root.clone()];
    let noted = Noted::start();
    let got = schema::climb_authors(pool, &uris, CAP).await.expect("climb");
    assert_eq!(count(&noted.take(), "climb_authors"), 1, "one statement for the set");
    drop(noted);
    let authors = |u: &str| {
        let mut a = got.get(u).expect(u).authors.clone();
        a.sort();
        a
    };
    let sorted = |v: &[&str]| {
        let mut v: Vec<String> = v.iter().map(|s| s.to_string()).collect();
        v.sort();
        v
    };
    assert_eq!(authors(&p), sorted(&[BOB, CAROL, ALICE]), "thread: B, C and the root");
    assert_eq!(authors(&sibling), authors(&p), "siblings share a climb");
    assert_eq!(authors(&below_own), sorted(&[ALICE]), "stops at the owner's thread reply, included");
    assert_eq!(authors(&bb), sorted(&[CAROL, ALICE]), "branch: to the branch root, included");
    let cycle = authors(&in_cycle);
    assert!(cycle.iter().all(|a| a == BOB || a == CAROL), "the cycle's rows only: {cycle:?}");
    for (u, walked) in [(&root, false), (&own, false), (&branch_root, false), (&p, true), (&bb, true)] {
        let c = got.get(u.as_str()).expect("row");
        assert_eq!(c.walked, walked, "{u}");
        if !walked {
            assert!(c.authors.is_empty(), "{u}");
        }
    }
    // The cap bounds the cycle (cap 2: two rows at most, both authors).
    let capped = schema::climb_authors(pool, std::slice::from_ref(&in_cycle), 2).await.expect("climb");
    assert!(capped[&in_cycle].authors.len() <= 2);
    // Sliced past K: the same answer.
    let k = schema::climb_keys(CAP);
    let many: Vec<String> = uris.iter().cycle().take(k + 3).cloned().collect();
    let sliced = schema::climb_authors(pool, &many, CAP).await.expect("sliced");
    assert_eq!(sliced.len(), got.len());
    w.finish().await;
}

/// T-DM4: a chain through an untracked author misses (`untracked`) with no
/// fetch and the budget unspent; a climb of 65 distinct authors misses
/// (`climb_cap`); one climb statement per `expand` level.
#[tokio::test]
async fn tdm4_untracked_cap_and_levels() {
    let extra = dids("dmf", 66);
    let Some(mut w) = Walk::new_in("dm4-cap", PublicMode::On, Layout::SamePds, Some("followers"), &[], &extra, Arc::new(NoHeads)).await else {
        return;
    };
    for d in &extra {
        exec(&w.f.pool, &follow_sql(d, ALICE)).await;
    }
    let read = PrivateReadConfig { max_chain: 100, ..PrivateReadConfig::default() };
    let root = w.root.clone();
    // 65 distinct authors above Dave's reply.
    let mut parent = root.clone();
    for (i, a) in extra.iter().take(65).enumerate() {
        parent = w.reply(a, &format!("3kdm4l{i:02}22222"), &parent).await;
    }
    let deep = w.reply(BOB, "3kdm4deep2222", &parent).await;
    // An untracked author's row under the root, and Bob's reply to it.
    let untracked = &extra[65];
    let u = w.reply(untracked, "3kdm4u222222", &root).await;
    let below_untracked = w.reply(BOB, "3kdm4ub22222", &u).await;
    w.f.ready_all().await;
    for d in &extra {
        w.f.ready(d).await;
    }
    chain_all(&w.f.pool).await;
    exec(&w.f.pool, &format!("DELETE FROM sync_state WHERE did = '{untracked}'")).await;
    let base = serve_heads(w.f.state_read(w.f.app.clone(), None, Duration::ZERO, &TEST_CURSOR_KEY, &read)).await;
    let fresh = serve_live(w.f.state_bounded(&read)).await;
    let counted = Counted::new();
    w.check(served(&fresh, DAVE, &deep).await, "the 66-deep reply reads fresh".to_string());

    let errors = || series(&counted.handle.render(), "sync_head_check_total", &["result=\"error\""]).unwrap_or(0.0);
    let (untracked_before, errors_before) = (counted.fallback("untracked"), errors());
    w.check(!served(&base, DAVE, &below_untracked).await, "a chain through an untracked author misses".to_string());
    w.check(counted.fallback("untracked") == untracked_before + 1.0, "counted untracked".to_string());
    w.check(errors() == errors_before, "no fetch was tried".to_string());

    let cap_before = counted.fallback("climb_cap");
    w.check(!served(&base, DAVE, &deep).await, "65 distinct climb authors miss".to_string());
    w.check(counted.fallback("climb_cap") == cap_before + 1.0, "counted climb_cap".to_string());

    // One climb statement per level: Dave's thread from the root, two levels
    // of replies (the chain's first two rows).
    let noted = Noted::start();
    let (_, thread) = thread_of(&base, DAVE, &format!("uri={root}&depth=2")).await;
    let climbs = count(&noted.take(), "climb_authors");
    drop(noted);
    w.check(thread["post"]["uri"] == root.as_str(), "the root reads".to_string());
    // The anchor's own climb, then one for each of the two levels.
    w.check(climbs == 3, format!("climb statements {climbs}"));
    w.finish().await;
}

/// T-DM5: under the fallback the oracles' `last_synced_at` is the oldest
/// head time of the repos the read depends on: with `head_max_age_secs`
/// above the bound, a read whose oldest head is past the bound is
/// `bind_denied`, one whose heads are all within it a hit. The startup
/// warning names the setting.
#[tokio::test]
async fn tdm5_bind_freshness() {
    assert!(!crate::serve::warn_head_fallback_bound(&crate::config::FileConfig::default()));
    let mut file = crate::config::FileConfig::default();
    file.sync.head_max_age_secs = 600;
    assert!(crate::serve::warn_head_fallback_bound(&file));
    file.private_read.head_fallback = false;
    assert!(!crate::serve::warn_head_fallback_bound(&file));

    let Some(mut w) = Walk::new_in("dm5-bind", PublicMode::On, Layout::SamePds, Some("followers"), &[], &[], Arc::new(NoHeads)).await else {
        return;
    };
    let root = w.root.clone();
    let p = w.reply(BOB, "3kdm5b222222", &root).await;
    w.f.ready_all().await;
    chain_all(&w.f.pool).await;
    let scope = ScopeState::new(w.f.sets.clone(), ScopePolicy::MembersAndOneHop, 10_000);
    w.f.prober = HeadProber::new(
        w.f.pool.clone(),
        scope,
        Arc::new(KryphocronPrivateTier),
        Arc::new(NoHeads),
        w.f.budgets.clone(),
        SyncConfig { head_max_age_secs: 600, ..SyncConfig::default() },
    );
    let base = serve(w.f.state_read(w.f.app.clone(), None, Duration::ZERO, &TEST_CURSOR_KEY, &PrivateReadConfig::default())).await;
    let counted = Counted::new();
    let pool = w.f.pool.clone();
    let ages = |bob: u32, dave: u32| {
        let pool = pool.clone();
        async move {
            exec(&pool, "UPDATE sync_state SET head_checked_at = now()").await;
            exec(&pool, &format!("UPDATE sync_state SET head_checked_at = now() - interval '{bob} seconds' WHERE did = '{BOB}'")).await;
            exec(&pool, &format!("UPDATE sync_state SET head_checked_at = now() - interval '{dave} seconds' WHERE did = '{DAVE}'")).await;
        }
    };
    ages(100, 30).await;
    w.check(served(&base, DAVE, &p).await, "every head within the bound: a hit".to_string());
    let denied = counted.outcome("bind_denied");
    ages(30, 300).await;
    w.check(!served(&base, DAVE, &p).await, "the reader's head past the bound".to_string());
    w.check(counted.outcome("bind_denied") == denied + 1.0, "bind_denied".to_string());
    ages(300, 30).await;
    w.check(!served(&base, DAVE, &p).await, "the author's head past the bound".to_string());
    w.check(counted.outcome("bind_denied") == denied + 2.0, "bind_denied again".to_string());
    w.finish().await;
}

/// T-DM13: with the watermark fresh, `getPosts`, `getPostThread`,
/// `getAuthorFeed` and `getTimeline` run none of the fallback's statements,
/// and answer the same bytes with the fallback on and off. Degraded, the
/// same reads do run them (each fallback statement notes itself, `noted`).
#[tokio::test]
async fn tdm13_a_fresh_watermark_is_unchanged() {
    let Some(w) = Walk::new_in("dm13-fresh", PublicMode::On, Layout::SamePds, Some("followers"), &[], &[], Arc::new(NoHeads)).await else {
        return;
    };
    let root = w.root.clone();
    let c = w.reply(CAROL, "3kdm13c22222", &root).await;
    let p = w.reply(BOB, "3kdm13b22222", &c).await;
    exec(&w.f.pool, &follow_sql(DAVE, BOB)).await;
    w.f.ready_all().await;
    chain_all(&w.f.pool).await;
    let off = PrivateReadConfig { head_fallback: false, ..PrivateReadConfig::default() };
    let on_fresh = serve_live(w.f.state_bounded(&PrivateReadConfig::default())).await;
    let off_fresh = serve_live(w.f.state_bounded(&off)).await;
    let degraded = serve_heads(w.f.state_read(w.f.app.clone(), None, Duration::ZERO, &TEST_CURSOR_KEY, &PrivateReadConfig::default())).await;
    let calls: Vec<(&str, String)> = vec![
        (POSTS, format!("uris={p}&uris={c}&uris={root}")),
        (THREAD, format!("uri={p}")),
        (FEED, format!("actor={BOB}")),
        (TIMELINE, String::new()),
    ];
    for (method, query) in &calls {
        let noted = Noted::start();
        let (s_on, b_on, _) = get(&on_fresh, method, query, DAVE, DAVE_KEY).await;
        let ran = noted.take();
        let (s_off, b_off, _) = get(&off_fresh, method, query, DAVE, DAVE_KEY).await;
        drop(noted);
        assert_eq!((s_on, s_off), (200, 200), "{method}: {b_on}");
        assert_eq!(b_on, b_off, "{method}: the same bytes");
        assert!(!b_on.contains("\"degraded\"") && !b_on.contains("\"retryAfter\""), "{method}: {b_on}");
        assert!(ran.is_empty(), "{method}: fallback statements ran fresh: {ran:?}");
    }
    // Degraded, the fallback's statements run: the notes are live.
    let noted = Noted::start();
    let (status, body, _) = get(&degraded, POSTS, &format!("uris={p}"), DAVE, DAVE_KEY).await;
    let ran = noted.take();
    drop(noted);
    assert_eq!(status, 200, "{body}");
    for name in ["climb_authors", "fact8_with_authors", "read_head_state", "stored_head"] {
        assert!(count(&ran, name) > 0, "degraded runs {name}: {ran:?}");
    }
    assert!(serde_json::from_str::<Value>(&body).expect("json")["posts"].as_array().is_some_and(|a| a.len() == 1), "{body}");
    w.finish().await;
}

/// T-DM14: threads while degraded take HEAD's sequential path; a 16-deep
/// chain with every head fresh reads through, and the request's budget and
/// head cache are HEAD's sequence (each DID checked once, step 3's DIDs).
#[tokio::test]
async fn tdm14_threads_while_degraded() {
    let extra = dids("dmt", 15);
    let Some(mut w) = Walk::new_in("dm14-threads", PublicMode::On, Layout::SamePds, Some("followers"), &[], &extra, Arc::new(NoHeads)).await else {
        return;
    };
    for d in &extra {
        exec(&w.f.pool, &follow_sql(d, ALICE)).await;
    }
    let mut parent = w.root.clone();
    let mut rows = Vec::new();
    for (i, a) in extra.iter().enumerate() {
        parent = w.reply(a, &format!("3kdm14r{i:02}2222"), &parent).await;
        rows.push(parent.clone());
    }
    w.f.ready_all().await;
    for d in &extra {
        w.f.ready(d).await;
    }
    chain_all(&w.f.pool).await;
    let base = serve_heads(w.f.state_read(w.f.app.clone(), None, Duration::ZERO, &TEST_CURSOR_KEY, &PrivateReadConfig::default())).await;
    let anchor = rows.last().expect("rows").clone();
    let (_, thread) = thread_of(&base, DAVE, &format!("uri={anchor}&depth=0")).await;
    let mut parents = Vec::new();
    let mut node = &thread["parent"];
    while let Some(uri) = node["post"]["uri"].as_str() {
        parents.push(uri.to_string());
        node = &node["parent"];
    }
    let mut expected: Vec<String> = rows.iter().rev().skip(1).cloned().collect();
    expected.push(w.root.clone());
    w.check(parents == expected, format!("the 16-deep chain reads through: {} parents", parents.len()));
    w.check(chain_state(&anchor).is_none(), "the head round was not taken".to_string());
    let (left, cached) = chain_state(&format!("sequential:{anchor}")).expect("the sequential path ran");
    let mut dids: HashSet<String> = extra.iter().cloned().collect();
    dids.insert(ALICE.to_string());
    dids.insert(DAVE.to_string());
    w.check(cached.keys().cloned().collect::<HashSet<_>>() == dids && cached.values().all(|ok| *ok), format!("cached {cached:?}"));
    // Each of the 17 DIDs charged once, the budget of 16 spent; no fetch.
    w.check(left == 0, format!("budget left {left}"));
    w.finish().await;
}

/// T-DM16: the author's own reply naming an audience not owned by its
/// anchor's author is served to its author under the fallback, with W and
/// A current and O's head stale; another reader is not served it.
#[tokio::test]
async fn tdm16_own_read_while_degraded() {
    let Some(mut w) = Walk::new_in("dm16-own", PublicMode::On, Layout::SamePds, Some("followers"), &[], &[], Arc::new(NoHeads)).await else {
        return;
    };
    let root = w.root.clone();
    let carols = w.f.audience(CAROL, "3kdm16aud222", Some("list"), &[BOB, DAVE]).await;
    let p = w.f.private_post(BOB, "3kdm16b22222", &carols, Some((&root, &root))).await;
    w.f.ready_all().await;
    chain_all(&w.f.pool).await;
    exec(&w.f.pool, &format!("UPDATE sync_state SET head_checked_at = to_timestamp(0) WHERE did = '{ALICE}'")).await;
    let base = serve_heads(w.f.state_read(w.f.app.clone(), None, Duration::ZERO, &TEST_CURSOR_KEY, &PrivateReadConfig::default())).await;
    w.check(served(&base, BOB, &p).await, "the author reads its own reply".to_string());
    w.check(!served(&base, DAVE, &p).await, "another reader does not".to_string());
    w.finish().await;
}

// ---- A, B, C: what clients are told (degraded-mode design §3) ----

/// A GET with its `Retry-After` header: status, body, header.
async fn get_with_retry(base: &str, method: &str, query: &str, did: &str) -> (u16, String, Option<String>) {
    let token = mint(&signing_key(key_of(did)), did, &format!("did:web:{AUDIENCE_HOST}"), Some(method));
    let r = reqwest::Client::new()
        .get(format!("{base}/xrpc/{method}?{query}"))
        .header("authorization", format!("Bearer {token}"))
        .send()
        .await
        .expect("request");
    let status = r.status().as_u16();
    let retry = r.headers().get("retry-after").map(|v| v.to_str().expect("ascii").to_string());
    (status, r.text().await.expect("body"), retry)
}

/// The degraded `retryAfter` with no private watermark: the maximum.
const RETRY_MAX: u64 = 300;

/// §3.7's formula: a quarter of the delay, within the bounds; no watermark
/// is the maximum.
#[test]
fn tdm7_retry_after_formula() {
    let now = SystemTime::now();
    let ago = |s: u64| Some(now - Duration::from_secs(s));
    assert_eq!(pipeline::retry_after_secs(None, now, 30, 300), 300);
    assert_eq!(pipeline::retry_after_secs(ago(130), now, 30, 300), 33);
    assert_eq!(pipeline::retry_after_secs(ago(60), now, 30, 300), 30, "early in a stall: the minimum");
    assert_eq!(pipeline::retry_after_secs(ago(3600), now, 30, 300), 300, "an old stall: the maximum");
    assert_eq!(pipeline::retry_after_secs(ago(600), now, 30, 300), 150);
}

/// T-DM6 (B): degraded, every anchor miss of `getPostThread` is the same
/// `503 Unavailable` (an existing readable-but-behind anchor, an existing
/// unreadable one, a nonexistent `postPrivate` URI, one under the caller's
/// DID), with `retryAfter` in the body and the header; a URI of another
/// NSID is `400`. Not degraded: the caller's own anchor missed transiently
/// is `503`; another author's with the same cause, and a nonexistent URI,
/// are byte-identical `400 NotFound`.
#[tokio::test]
async fn tdm6_thread_anchor_misses() {
    let Some(mut w) = Walk::new_in("dm6-anchor", PublicMode::On, Layout::SamePds, Some("followers"), &[], &[], Arc::new(NoHeads)).await else {
        return;
    };
    let root = w.root.clone();
    let c = w.reply(CAROL, "3kdm6c222222", &root).await;
    let behind = w.reply(BOB, "3kdm6b222222", &c).await;
    let carols = w.f.audience(CAROL, "3kdm6aud2222", Some("list"), &[BOB]).await;
    let unreadable = w.f.private_post(CAROL, "3kdm6u222222", &carols, None).await;
    w.f.ready_all().await;
    chain_all(&w.f.pool).await;
    hole(&w.f.pool, CAROL).await;
    let s = servers(&w).await;
    let expected = format!("{{\"error\":\"Unavailable\",\"message\":\"Temporarily unavailable; try again\",\"retryAfter\":{RETRY_MAX}}}");
    for uri in [
        behind.clone(),
        unreadable.clone(),
        format!("at://{BOB}/{NSID_POST_PRIVATE}/3knothere222"),
        format!("at://{DAVE}/{NSID_POST_PRIVATE}/3knothere222"),
    ] {
        let (status, body, retry) = get_with_retry(&s.on, THREAD, &format!("uri={uri}"), DAVE).await;
        w.check(status == 503 && body == expected, format!("degraded {uri}: {status} {body}"));
        w.check(retry.as_deref() == Some(RETRY_MAX.to_string().as_str()), format!("degraded {uri}: Retry-After {retry:?}"));
    }
    let other = format!("at://{BOB}/app.bsky.feed.post/3kdm6pub2222");
    let (status, body, _) = get_with_retry(&s.on, THREAD, &format!("uri={other}"), DAVE).await;
    w.check(status == 400 && body.contains("\"NotFound\""), format!("another NSID: {status} {body}"));
    // With the fallback off the anchor misses `stale`: the same 503.
    let (status, body, _) = get_with_retry(&s.off, THREAD, &format!("uri={behind}"), DAVE).await;
    w.check(status == 503 && body == expected, format!("fallback off: {status} {body}"));

    // Not degraded: Bob's own anchor behind its head (`head_check`).
    let mine = w.f.private_post(BOB, "3kdm6mine222", &w.aud.clone(), None).await;
    exec(&w.f.pool, &format!("UPDATE sync_state SET head_rev = '{H}' WHERE did = '{BOB}'")).await;
    let (status, body, retry) = get_with_retry(&s.fresh, THREAD, &format!("uri={mine}"), BOB).await;
    w.check(status == 503 && body.contains("\"Unavailable\"") && body.contains("\"retryAfter\":30"), format!("own transient: {status} {body}"));
    w.check(retry.as_deref() == Some("30"), format!("own transient Retry-After {retry:?}"));
    let (status_other, other_body, _) = get_with_retry(&s.fresh, THREAD, &format!("uri={mine}"), DAVE).await;
    let (status_none, none_body, _) = get_with_retry(&s.fresh, THREAD, &format!("uri=at://{BOB}/{NSID_POST_PRIVATE}/3knothere222"), DAVE).await;
    w.check(status_other == 400 && other_body == none_body && status_none == 400, format!("another's: {status_other} {other_body} / {none_body}"));
    w.finish().await;
}

/// T-DM7 (A): two callers at one moment get the same `degraded` and
/// `retryAfter`, with `Retry-After` equal to the body's; `unavailable`
/// lists only the caller's own transient misses (never another author's
/// post, never one that missed for good) and is absent when empty; outside
/// degraded mode it is still listed (DM-C3), with no `degraded`.
#[tokio::test]
async fn tdm7_response_signals() {
    let Some(mut w) = Walk::new_in("dm7-signals", PublicMode::On, Layout::SamePds, Some("followers"), &[], &[], Arc::new(NoHeads)).await else {
        return;
    };
    let aud = w.aud.clone();
    let root = w.root.clone();
    // Bob's own posts: one top-level (behind once Bob's chain is), one
    // taken down (a permanent miss).
    let mine = w.f.private_post(BOB, "3kdm7mine222", &aud, None).await;
    let gone = w.f.private_post(BOB, "3kdm7gone222", &aud, None).await;
    w.label(&gone, "!takedown").await;
    let carols = w.f.private_post(CAROL, "3kdm7carol22", &aud, None).await;
    w.f.ready_all().await;
    chain_all(&w.f.pool).await;
    hole(&w.f.pool, BOB).await;
    hole(&w.f.pool, CAROL).await;
    let s = servers(&w).await;
    let query = format!("uris={mine}&uris={gone}&uris={carols}&uris={root}");
    let (sb, bob, rb) = get_with_retry(&s.on, POSTS, &query, BOB).await;
    let (sd, dave, rd) = get_with_retry(&s.on, POSTS, &query, DAVE).await;
    assert_eq!((sb, sd), (200, 200), "{bob} {dave}");
    let (bob, dave): (Value, Value) = (serde_json::from_str(&bob).expect("json"), serde_json::from_str(&dave).expect("json"));
    w.check(bob["degraded"] == true && dave["degraded"] == true, "degraded for both".to_string());
    w.check(bob["retryAfter"] == RETRY_MAX && dave["retryAfter"] == RETRY_MAX, "the same retryAfter".to_string());
    w.check(rb.as_deref() == Some("300") && rd.as_deref() == Some("300"), format!("Retry-After {rb:?} {rd:?}"));
    w.check(bob["unavailable"] == json!([{"uri": mine, "reason": "syncing"}]), format!("Bob's unavailable {}", bob["unavailable"]));
    w.check(dave.get("unavailable").is_none(), format!("Dave's own nothing: {dave}"));
    // Bob is behind, so every read of his misses; Dave reads the root.
    w.check(bob["posts"].as_array().is_some_and(Vec::is_empty), "Bob, behind, reads nothing".to_string());
    w.check(dave["posts"].as_array().is_some_and(|p| p.len() == 1), "Dave reads the root".to_string());

    // Not degraded: still listed, with no degraded or retryAfter. Bob's
    // head past what was applied: `head_check`.
    exec(&w.f.pool, &format!("UPDATE sync_state SET head_rev = '3l22222222224' WHERE did = '{BOB}'")).await;
    let (status, body, retry) = get_with_retry(&s.fresh, POSTS, &format!("uris={mine}"), BOB).await;
    let v: Value = serde_json::from_str(&body).expect("json");
    w.check(status == 200 && v.get("degraded").is_none() && v.get("retryAfter").is_none() && retry.is_none(), format!("fresh: {body} {retry:?}"));
    w.check(v["unavailable"] == json!([{"uri": mine, "reason": "syncing"}]), format!("fresh unavailable {body}"));
    // An owner not ready: `indexing`.
    let mut conn = w.f.pool.acquire().await.expect("conn");
    readiness::clear_owner(&mut conn, CAROL).await.expect("clear");
    drop(conn);
    let (_, body, _) = get_with_retry(&s.fresh, POSTS, &format!("uris={carols}"), CAROL).await;
    let v: Value = serde_json::from_str(&body).expect("json");
    w.check(v["unavailable"] == json!([{"uri": carols, "reason": "indexing"}]), format!("indexing: {body}"));
    w.finish().await;
}

/// T-DM8 (C): degraded, a quote of a readable-but-behind post, of an
/// unreadable one, of a prefilter-failing one and of a nonexistent URI are
/// the same `#viewUnavailable`; not degraded, the caller's own quoted post
/// missed transiently is `#viewUnavailable`, another's is `#viewNotFound`,
/// as for a nonexistent URI.
#[tokio::test]
async fn tdm8_quoted_posts() {
    let Some(mut w) = Walk::new_in("dm8-quotes", PublicMode::On, Layout::SamePds, Some("followers"), &[], &[], Arc::new(NoHeads)).await else {
        return;
    };
    let (aud, root) = (w.aud.clone(), w.root.clone());
    let c = w.reply(CAROL, "3kdm8c222222", &root).await;
    let behind = w.reply(BOB, "3kdm8b222222", &c).await;
    let undecodable = w.f.undecodable(CAROL, "3kdm8u222222", &aud, None).await;
    let carols = w.f.audience(CAROL, "3kdm8aud2222", Some("list"), &[BOB]).await;
    let hidden = w.f.private_post(CAROL, "3kdm8h222222", &carols, None).await;
    let nothing = format!("at://{CAROL}/{NSID_POST_PRIVATE}/3knothere222");
    let quoted = [behind.clone(), undecodable.clone(), hidden.clone(), nothing.clone()];
    let mut quoting = Vec::new();
    for (i, q) in quoted.iter().enumerate() {
        let uri = w.f.post_with(ALICE, &format!("3kdm8q{i}22222"), &aud, json!({"embed": {"$type": "app.bsky.embed.record", "record": {"uri": q, "cid": "bafyany"}}})).await;
        quoting.push(uri);
    }
    w.f.ready_all().await;
    chain_all(&w.f.pool).await;
    hole(&w.f.pool, CAROL).await;
    let s = servers(&w).await;
    let refs: Vec<&str> = quoting.iter().map(String::as_str).collect();
    let (_, posts) = posts_of(&s.on, DAVE, &refs).await;
    w.check(posts.len() == 4, format!("the four quoting posts read: {}", posts.len()));
    for (post, q) in posts.iter().zip(&quoted) {
        let want = json!({"$type": "app.bsky.embed.record#view", "record": {"$type": "app.nearhorizon.private.defs#viewUnavailable", "uri": q, "unavailable": true, "retryAfter": RETRY_MAX}});
        w.check(post["embed"] == want, format!("degraded quote of {q}: {}", post["embed"]));
    }
    let as_text = |p: &Value, q: &str| p["embed"].to_string().replace(q, "URI");
    let texts: Vec<String> = posts.iter().zip(&quoted).map(|(p, q)| as_text(p, q)).collect();
    w.check(texts.windows(2).all(|t| t[0] == t[1]), format!("byte-identical: {texts:?}"));

    // Not degraded. Bob's own post under Carol's audience, Carol not ready:
    // his quote of it is unavailable; Dave's view of Alice's quote of a
    // post of Carol's is not found, as a nonexistent one.
    let bobs = w.f.private_post(BOB, "3kdm8own2222", &carols, None).await;
    let carols_open = w.f.private_post(CAROL, "3kdm8open222", &aud, None).await;
    let bob_quotes = w.f.post_with(BOB, "3kdm8bq22222", &aud, json!({"embed": {"$type": "app.bsky.embed.record", "record": {"uri": bobs, "cid": "bafyany"}}})).await;
    let alice_quotes = w.f.post_with(ALICE, "3kdm8aq22222", &aud, json!({"embed": {"$type": "app.bsky.embed.record", "record": {"uri": carols_open, "cid": "bafyany"}}})).await;
    let alice_quotes_nothing = w.f.post_with(ALICE, "3kdm8an22222", &aud, json!({"embed": {"$type": "app.bsky.embed.record", "record": {"uri": nothing, "cid": "bafyany"}}})).await;
    let mut conn = w.f.pool.acquire().await.expect("conn");
    readiness::clear_owner(&mut conn, CAROL).await.expect("clear");
    drop(conn);
    let (_, mine) = posts_of(&s.fresh, BOB, &[&bob_quotes]).await;
    let want = json!({"$type": "app.nearhorizon.private.defs#viewUnavailable", "uri": bobs, "unavailable": true, "retryAfter": 30});
    w.check(mine.first().is_some_and(|p| p["embed"]["record"] == want), format!("own transient quote: {mine:?}"));
    let (_, theirs) = posts_of(&s.fresh, DAVE, &[&alice_quotes, &alice_quotes_nothing]).await;
    w.check(theirs.len() == 2, format!("Alice's two quoting posts: {}", theirs.len()));
    if theirs.len() == 2 {
        let a = as_text(&theirs[0], &carols_open);
        let b = as_text(&theirs[1], &nothing);
        w.check(a == b && a.contains("viewNotFound"), format!("another's transient quote is not found: {a} / {b}"));
    }
    w.finish().await;
}

/// T-DM10: with `floor_ms.get_post_thread` set, B's 503 and the 400 both
/// wait the floor.
#[tokio::test]
async fn tdm10_the_503_is_floored() {
    let Some(w) = Walk::new_in("dm10-floor", PublicMode::On, Layout::SamePds, Some("followers"), &[], &[], Arc::new(NoHeads)).await else {
        return;
    };
    let floor = Duration::from_millis(400);
    let base = serve_heads(w.f.state_read(w.f.app.clone(), None, floor, &TEST_CURSOR_KEY, &PrivateReadConfig::default())).await;
    let nothing = format!("at://{BOB}/{NSID_POST_PRIVATE}/3knothere222");
    let other = format!("at://{BOB}/app.bsky.feed.post/3knothere222");
    let (s503, body, took503) = get(&base, THREAD, &format!("uri={nothing}"), DAVE, DAVE_KEY).await;
    let (s400, _, took400) = get(&base, THREAD, &format!("uri={other}"), DAVE, DAVE_KEY).await;
    assert_eq!((s503, s400), (503, 400), "{body}");
    assert!(took503 >= floor && took400 >= floor, "{took503:?} {took400:?}");
    w.finish().await;
}

/// T-DM11: `private.degraded` is listed, after `private.notServed`.
#[test]
fn tdm11_the_feature_is_listed() {
    let f = crate::private_tier::FEATURES;
    let at = |name: &str| f.iter().position(|x| *x == name);
    assert!(at("private.degraded").is_some_and(|d| at("private.notServed").is_some_and(|n| d == n + 1)), "{f:?}");
}

// ---- D: readiness cleared after the first time (degraded-mode design §3.5) ----

/// T-DM9 (D), the private gate and the two view-error sites: after a gap
/// transaction a member is `503 Reindexing` with `retryAfter` in the body
/// and the header; a member never ready, and a removed-and-re-added one,
/// are `400 NotReady`; after the settle, 200. D's 503 is a gate error,
/// outside the floor (T-DM10).
#[tokio::test]
async fn tdm9_reindexing_at_the_private_gate_and_the_view_sites() {
    let Some(w) = Walk::new_in("dm9-reindexing", PublicMode::On, Layout::SamePds, Some("followers"), &[], &[], Arc::new(NoHeads)).await else {
        return;
    };
    let pool = w.f.pool.clone();
    exec(&pool, "UPDATE members SET first_ready_at = ready_at").await;
    // Wide, so "answered before the floor" holds under a loaded test run: a
    // floored answer takes at least this long, an unfloored one well under.
    let floor = Duration::from_secs(2);
    let base = serve_live(w.f.state_read(w.f.app.clone(), Some(Duration::from_secs(1)), floor, &TEST_CURSOR_KEY, &PrivateReadConfig::default())).await;
    let root = w.root.clone();
    let scope = ScopeState::new(w.f.sets.clone(), ScopePolicy::MembersAndOneHop, 10_000);
    crate::firehose::gap::run_gap_transaction_clearing(&pool, &scope, &KryphocronPrivateTier, crate::firehose::gap::GapCause::Trim, false, false, None)
        .await
        .expect("gap");
    let expected = "{\"error\":\"Reindexing\",\"message\":\"Account re-indexing after an interruption; try again\",\"retryAfter\":30}";
    let start = Instant::now();
    let (status, body, retry) = get_with_retry(&base, POSTS, &format!("uris={root}"), BOB).await;
    let took = start.elapsed();
    assert_eq!((status, body.as_str(), retry.as_deref()), (503, expected, Some("30")));
    assert!(took < floor, "outside the floor: {took:?}");

    // The view-error sites' own checks.
    let viewer = crate::hydration::Viewer { did: Some(BOB.to_string()), hydrate: false };
    let req = crate::xrpc::auth::request_handle(None, "127.0.0.1".parse().expect("ip"));
    let timeline = w.f.app.hydrator.get_timeline(&viewer, &req, 10, None).await.expect("db");
    let e = crate::xrpc::errors::XrpcError::from(timeline.expect_err("a view error"));
    assert_eq!((e.status.as_u16(), e.error, e.retry_after), (503, "Reindexing", Some(30)));
    let sub = w.f.app.hydrator.put_activity_subscription(&viewer, CAROL, true, false).await.expect("db");
    let e = crate::xrpc::errors::XrpcError::from(sub.expect_err("a view error"));
    assert_eq!((e.status.as_u16(), e.error, e.retry_after), (503, "Reindexing", Some(30)));

    // Never ready: NotReady, at the gate and the view sites alike.
    exec(&pool, &format!("UPDATE members SET first_ready_at = NULL WHERE did = '{BOB}'")).await;
    let (status, body, _) = get_with_retry(&base, POSTS, &format!("uris={root}"), BOB).await;
    assert!(status == 400 && body.contains("\"NotReady\"") && !body.contains("retryAfter"), "{status} {body}");
    let timeline = w.f.app.hydrator.get_timeline(&viewer, &req, 10, None).await.expect("db");
    assert_eq!(timeline.expect_err("a view error").error, "NotReady");

    // Removed and re-added: `first_ready_at` starts again, NotReady.
    // (A removal deletes the row; `add_member` inserts a fresh one.)
    exec(&pool, &format!("DELETE FROM members WHERE did = '{CAROL}'")).await;
    exec(&pool, &format!("INSERT INTO members (did, source) VALUES ('{CAROL}', 'admin')")).await;
    let (status, body, _) = get_with_retry(&base, POSTS, &format!("uris={root}"), CAROL).await;
    assert!(status == 400 && body.contains("\"NotReady\""), "{status} {body}");

    // The settle: ready again, 200.
    exec(&pool, &format!("UPDATE members SET ready_at = now(), first_ready_at = COALESCE(first_ready_at, now()) WHERE did = '{DAVE}'")).await;
    let (status, body, _) = get_with_retry(&base, POSTS, &format!("uris={root}"), DAVE).await;
    assert_eq!(status, 200, "{body}");
    w.finish().await;
}

/// T-DM11: `private.reindexing` is listed, after `private.degraded`.
#[test]
fn tdm11_reindexing_is_listed() {
    let f = crate::private_tier::FEATURES;
    let at = |name: &str| f.iter().position(|x| *x == name);
    assert!(at("private.reindexing").is_some_and(|r| at("private.degraded").is_some_and(|d| r == d + 1)), "{f:?}");
}
