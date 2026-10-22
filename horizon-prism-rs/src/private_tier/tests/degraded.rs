//! Degraded mode (docs/internal/design/degraded-mode.md §10): the head
//! fallback's never-miss cases (T-DM3, T-DM3b), the climb (T-DM4), bind
//! freshness (T-DM5), the fresh watermark unchanged (T-DM13), threads while
//! degraded (T-DM14) and the author's own read (T-DM16). "Degraded" is a
//! private relay that never reported, so the private watermark is `None`.

use std::collections::HashSet;
use std::sync::Mutex as StdMutex;

use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::Layer;

use super::*;
use crate::firehose::writer::{ChainStep, advance_applied_rev, chain_commit};
use crate::private_tier::handlers::tests::chain_state;
use crate::private_tier::pipeline;
use crate::test_support::{hooks, series};

/// Every fixture repo's applied rev, and its chain once [`chain_all`] ran.
const C0: &str = "3kzzzzzzzzzz2";
/// The commit the live relay dropped and the stalled one still carries.
const M: &str = "3l22222222222";
/// The commit after it, which the live relay delivered.
const H: &str = "3l22222222223";

/// Statements sqlx ran on this thread while installed (its `sqlx::query`
/// events): what a request's handlers and pipeline executed.
#[derive(Clone, Default)]
struct Statements(Arc<StdMutex<Vec<String>>>);

#[derive(Default)]
struct Fields {
    summary: String,
    statement: String,
}

impl tracing::field::Visit for Fields {
    fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
        match field.name() {
            "summary" => self.summary = value.to_string(),
            "db.statement" => self.statement = value.to_string(),
            _ => {}
        }
    }

    fn record_debug(&mut self, _field: &tracing::field::Field, _value: &dyn std::fmt::Debug) {}
}

impl<S: tracing::Subscriber> Layer<S> for Statements {
    fn on_event(&self, event: &tracing::Event<'_>, _ctx: tracing_subscriber::layer::Context<'_, S>) {
        if event.metadata().target() != "sqlx::query" {
            return;
        }
        let mut f = Fields::default();
        event.record(&mut f);
        let text = if f.statement.trim().is_empty() { f.summary } else { f.statement.trim().to_string() };
        self.0.lock().expect("statements").push(text);
    }
}

impl Statements {
    /// Capture from now until the guard drops.
    fn capture() -> (Self, tracing::subscriber::DefaultGuard) {
        let s = Self::default();
        let filter = tracing_subscriber::filter::Targets::new().with_target("sqlx::query", tracing::Level::TRACE);
        let subscriber = tracing_subscriber::registry().with(s.clone().with_filter(filter));
        (s, tracing::subscriber::set_default(subscriber))
    }

    fn take(&self) -> Vec<String> {
        std::mem::take(&mut *self.0.lock().expect("statements"))
    }
}

/// The climb statement's text.
fn is_climb(sql: &str) -> bool {
    sql.contains("k AS MATERIALIZED (SELECT DISTINCT p.parent, p.root")
}

/// Fact 8 with its walk's authors.
fn is_fact8_with_authors(sql: &str) -> bool {
    sql.contains("AS authors")
}

/// A statement only the fallback runs: the climb, fact 8 with authors, or
/// a head read with `chain_rev`.
fn fallback_only(sql: &str) -> bool {
    is_climb(sql) || is_fact8_with_authors(sql) || sql.contains("chain_rev")
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
    exec(pool, &format!("UPDATE sync_state SET post_listed_rev = '{H}', audience_listed_rev = '{H}' WHERE did = '{x}'")).await;
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
    let (statements, guard) = Statements::capture();
    let got = schema::climb_authors(pool, &uris, CAP).await.expect("climb");
    drop(guard);
    let climbs = statements.take().into_iter().filter(|s| is_climb(s)).count();
    assert_eq!(climbs, 1, "one statement for the set");
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
    let (statements, guard) = Statements::capture();
    sqlx::query("SELECT 1 AS capture_probe").execute(&w.f.pool).await.expect("probe");
    assert!(statements.take().iter().any(|s| s.contains("capture_probe")), "the capture sees the probe");
    let (_, thread) = thread_of(&base, DAVE, &format!("uri={root}&depth=2")).await;
    drop(guard);
    let climbs = statements.take().into_iter().filter(|s| is_climb(s)).count();
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
/// same reads do run them (the capture sees them).
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
        let (statements, guard) = Statements::capture();
        // A statement of the test's own first: the capture is live.
        sqlx::query("SELECT 1 AS capture_probe").execute(&w.f.pool).await.expect("probe");
        assert!(statements.take().iter().any(|s| s.contains("capture_probe")), "the capture sees the probe");
        let (s_on, b_on, _) = get(&on_fresh, method, query, DAVE, DAVE_KEY).await;
        let ran = statements.take();
        let (s_off, b_off, _) = get(&off_fresh, method, query, DAVE, DAVE_KEY).await;
        drop(guard);
        assert_eq!((s_on, s_off), (200, 200), "{method}: {b_on}");
        assert_eq!(b_on, b_off, "{method}: the same bytes");
        assert!(!b_on.contains("\"degraded\"") && !b_on.contains("\"retryAfter\""), "{method}: {b_on}");
        assert!(!ran.is_empty(), "{method}: the capture sees statements");
        assert!(!ran.iter().any(|s| fallback_only(s)), "{method}: a fallback statement ran fresh");
    }
    // Degraded, the fallback's statements run.
    let (statements, guard) = Statements::capture();
    sqlx::query("SELECT 1 AS capture_probe").execute(&w.f.pool).await.expect("probe");
    let (status, body, _) = get(&degraded, POSTS, &format!("uris={p}"), DAVE, DAVE_KEY).await;
    drop(guard);
    assert_eq!(status, 200, "{body}");
    let ran = statements.take();
    assert!(ran.iter().any(|s| is_climb(s)) && ran.iter().any(|s| is_fact8_with_authors(s)), "degraded runs the climb and fact 8 with authors");
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
