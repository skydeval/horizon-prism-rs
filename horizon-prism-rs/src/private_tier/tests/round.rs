//! S12's T-56 (docs/internal/design/private-threads.md §3.3, §3.4, §6): the
//! read cap, the prefiltered parents, root and quotes, and the head round,
//! with a head source that records its calls.

use std::collections::HashSet;
use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};

use super::*;
use crate::private_tier::handlers::tests::chain_state;
use crate::sync::Class;
use crate::test_support::hooks;

/// A head source that answers every DID with [`REV`] (at or below every
/// fixture `applied_rev`) after `sleep`, fails `failing`, and records each
/// call in start order and the most calls ever in flight.
#[derive(Default)]
struct Recording {
    sleep: Duration,
    calls: Mutex<Vec<String>>,
    in_flight: AtomicUsize,
    peak: AtomicUsize,
    failing: Mutex<HashSet<String>>,
}

#[async_trait]
impl HeadSource for Recording {
    async fn latest_rev(&self, _pds: &str, did: &str, _timeout: Duration) -> Result<String, String> {
        self.calls.lock().expect("calls").push(did.to_string());
        let now = self.in_flight.fetch_add(1, Ordering::SeqCst) + 1;
        self.peak.fetch_max(now, Ordering::SeqCst);
        tokio::time::sleep(self.sleep).await;
        self.in_flight.fetch_sub(1, Ordering::SeqCst);
        if self.failing.lock().expect("failing").contains(did) {
            Err("failing".to_string())
        } else {
            Ok(REV.to_string())
        }
    }
}

impl Recording {
    fn reset(&self) {
        self.calls.lock().expect("calls").clear();
        self.peak.store(0, Ordering::SeqCst);
        self.failing.lock().expect("failing").clear();
    }

    fn calls(&self) -> Vec<String> {
        self.calls.lock().expect("calls").clone()
    }

    fn fail(&self, did: &str) {
        self.failing.lock().expect("failing").insert(did.to_string());
    }
}

/// A DID of this module's own, so hooks keyed by it meet no other test:
/// `did:plc:` and 24 base-32 characters.
fn did(tag: char, i: usize) -> String {
    const ALPHABET: &[u8; 32] = b"abcdefghijklmnopqrstuvwxyz234567";
    format!("did:plc:{tag}{}{}{}", "q".repeat(21), ALPHABET[i / 32] as char, ALPHABET[i % 32] as char)
}

/// T-56's chain: Alice's root T, then R1 by A1 below it, …, Rn by An, every
/// author following Alice under her `followers` audience; `more` members
/// beside them; heads from a [`Recording`] source.
struct Chain {
    w: Walk,
    heads: Arc<Recording>,
    authors: Vec<String>,
    rows: Vec<String>,
    more: Vec<String>,
}

impl Chain {
    async fn new(test: &str, public: PublicMode, layout: Layout, tag: char, depth: usize, more: usize) -> Option<Self> {
        let authors: Vec<String> = (1..=depth).map(|i| did(tag, i)).collect();
        let more: Vec<String> = (0..more).map(|i| did(tag, 100 + i)).collect();
        let mut extra = authors.clone();
        extra.extend(more.iter().cloned());
        let heads = Arc::new(Recording { sleep: Duration::from_millis(40), ..Recording::default() });
        let w = Walk::new_in(test, public, layout, Some("followers"), &[], &extra, Arc::clone(&heads) as Arc<dyn HeadSource>).await?;
        for d in &extra {
            exec(&w.f.pool, &follow_sql(d, ALICE)).await;
        }
        let mut rows = Vec::new();
        let mut parent = w.root.clone();
        for (i, a) in authors.iter().enumerate() {
            parent = w.reply(a, &format!("3kch{tag}{:02}22222", i + 1), &parent).await;
            rows.push(parent.clone());
        }
        // Each DID on its own host, so no host's read bucket drains.
        let mut everyone = vec![ALICE.to_string(), BOB.to_string(), CAROL.to_string(), DAVE.to_string()];
        everyone.extend(extra.iter().cloned());
        for (i, d) in everyone.iter().enumerate() {
            exec(&w.f.pool, &format!("UPDATE actors SET pds_endpoint = 'https://bench-{tag}{i}.invalid' WHERE did = '{d}'")).await;
        }
        let chain = Self { w, heads, authors, rows, more };
        chain.ready().await;
        Some(chain)
    }

    async fn ready(&self) {
        self.w.f.ready_all().await;
        for d in self.authors.iter().chain(&self.more) {
            self.w.f.ready(d).await;
        }
    }

    /// Every head stale, and the recorder reset, before a request.
    async fn stale(&self) {
        exec(&self.w.f.pool, "UPDATE sync_state SET head_checked_at = to_timestamp(0)").await;
        self.heads.reset();
    }

    /// Dave's thread anchored at the deepest row, `depth` levels expanded:
    /// status, body, and the parents' URIs nearest first.
    async fn thread(&self, depth: u32) -> (u16, String, Vec<String>) {
        let anchor = self.rows.last().expect("rows");
        let (status, body, _) = get(&self.w.base, THREAD, &format!("uri={anchor}&depth={depth}"), DAVE, DAVE_KEY).await;
        let mut parents = Vec::new();
        if status == 200 {
            let v: Value = serde_json::from_str(&body).expect("json");
            let mut node = &v["thread"]["parent"];
            while let Some(uri) = node["post"]["uri"].as_str() {
                parents.push(uri.to_string());
                node = &node["parent"];
            }
        }
        (status, body, parents)
    }

    /// The parents a run that stops at parent `i` (1-based, nearest
    /// first) reads: parents 1 to `i - 1`.
    fn parents_before(&self, i: usize) -> Vec<String> {
        self.rows.iter().rev().skip(1).take(i - 1).cloned().collect()
    }
}

/// T-56: one head round in HEAD's charge order. A 15-reply chain below
/// Alice's root has 16 authors and, with Dave, 17 DIDs, every head stale;
/// the budget is 16.
/// - One round covers the anchor and the parents, with no more than
///   `bind_concurrency` fetches in flight; the DID left unfunded is the
///   deepest parent's author, A1, the one HEAD's order leaves unfunded, and
///   both chains end at R1.
/// - A miss at parent 5 (a failed head, a refused bucket token, content
///   that does not decode): the chain ends below parent 5; the budget left
///   is HEAD's minus D's funded members, D being the DIDs HEAD never reached
///   (A9 to A1); the cache holds HEAD's entries plus D's results; the source
///   is called at most 16 times in the whole request, exactly 16 with 20
///   expansion replies by new stale authors; a later reply by one of D's
///   authors reads from the cache, and one by a new stale author misses.
async fn round(test: &str, public: PublicMode, layout: Layout) {
    let Some(mut c) = Chain::new(test, public, layout, 'r', 15, 21).await else {
        return;
    };
    let (anchor, a) = (c.rows[14].clone(), c.authors.clone());
    // Expansion replies to the anchor: 20 by new authors, one by A3 (in D).
    for (i, n) in c.more.iter().take(20).enumerate() {
        c.w.reply(n, &format!("3kexp{i:02}222222"), &anchor).await;
    }
    let by_a3 = c.w.reply(&a[2], "3kexpa3222222", &anchor).await;
    c.ready().await;

    // One round.
    c.stale().await;
    let (status, body, parents) = c.thread(0).await;
    assert_eq!(status, 200, "{body}");
    let calls = c.heads.calls();
    let mut expected: Vec<String> = vec![a[14].clone(), DAVE.to_string(), ALICE.to_string()];
    expected.extend(a[1..14].iter().rev().cloned());
    c.w.check(calls.len() == 16 && calls.iter().collect::<HashSet<_>>() == expected.iter().collect::<HashSet<_>>(), format!("one round funds HEAD's 16: {calls:?}"));
    c.w.check(!calls.contains(&a[0]), "A1 is the unfunded DID".to_string());
    let peak = c.heads.peak.load(Ordering::SeqCst);
    c.w.check(peak > 1 && peak <= 8, format!("peak fetches in flight {peak}"));
    c.w.check(parents == c.parents_before(14), format!("the chain ends at R1: {parents:?}"));

    // A miss at parent 5, R10 by A10; D is A9 to A1, and A9 to A2 are funded.
    let d_funded = 8;
    let head_charged = 8; // A15, Dave, Alice, A14 to A10
    for variant in ["head fails", "token refused"] {
        c.stale().await;
        // A10's host's bucket, kept empty while the request runs.
        let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let drainer = (variant == "token refused").then(|| {
            let (budgets, stop) = (c.w.f.budgets.clone(), Arc::clone(&stop));
            let host = HostBudgets::host_of(&format!("https://bench-r{}.invalid", 4 + 9));
            tokio::spawn(async move {
                while !stop.load(Ordering::SeqCst) {
                    while budgets.try_acquire(&host, Class::ReadFetch) {}
                    tokio::time::sleep(Duration::from_millis(1)).await;
                }
            })
        });
        if variant == "head fails" {
            c.heads.fail(&a[9]);
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
        let (status, body, parents) = c.thread(0).await;
        stop.store(true, Ordering::SeqCst);
        if let Some(d) = drainer {
            d.await.expect("drainer");
        }
        assert_eq!(status, 200, "{body}");
        c.w.check(parents == c.parents_before(5), format!("{variant}: the chain ends below parent 5: {parents:?}"));
        let (left, cached) = chain_state(&anchor).expect("chain state");
        c.w.check(left == 16 - head_charged - d_funded, format!("{variant}: budget left {left}"));
        let mut heads_and_d: HashSet<String> = [DAVE, ALICE].iter().map(|s| s.to_string()).collect();
        heads_and_d.extend(a.iter().cloned());
        c.w.check(cached.keys().cloned().collect::<HashSet<_>>() == heads_and_d, format!("{variant}: cached {:?}", cached.keys()));
        c.w.check(c.heads.calls().len() <= 16, format!("{variant}: {} calls", c.heads.calls().len()));
    }

    // The fetch cap over a whole request, and later reads: with expansion,
    // the source is called exactly 16 times; A3's expansion reply reads from
    // the cache, the new authors' replies miss.
    c.stale().await;
    c.heads.fail(&a[9]);
    let (status, body, _) = c.thread(1).await;
    assert_eq!(status, 200, "{body}");
    c.w.check(c.heads.calls().len() == 16, format!("the fetch cap: {} calls", c.heads.calls().len()));
    let v: Value = serde_json::from_str(&body).expect("json");
    let replies = reply_uris(&v["thread"]);
    c.w.check(replies == [by_a3.clone()], format!("only D's author's reply reads: {replies:?}"));
    c.w.finish().await;
}

#[tokio::test]
async fn t56_one_head_round_in_heads_order() {
    for (public, layout, run) in [(PublicMode::On, Layout::SamePds, "on"), (PublicMode::Off, Layout::OffPds, "off")] {
        round(&format!("t56-round-{run}"), public, layout).await;
    }
}

/// T-56's decode variant: parent 5 does not decode. The chain ends below
/// it, and the budget and the cache are as for a failed head, but parent
/// 5's own DIDs were checked and cached.
#[tokio::test]
async fn t56_a_decode_miss_mid_prefix() {
    let Some(mut c) = Chain::new("t56-decode", PublicMode::On, Layout::SamePds, 'd', 15, 0).await else {
        return;
    };
    let a = c.authors.clone();
    let undecodable = c.w.f.post_record(&a[9], "3kencodedfor", &c.w.aud.clone(), Some((&c.w.root.clone(), &c.rows[8].clone()))).await;
    let rkey = c.rows[9].rsplit('/').next().expect("rkey").to_string();
    c.w.f.apply_op(&a[9], Action::Update, NSID_POST_PRIVATE, &rkey, REV2, Some(undecodable)).await;
    c.ready().await;
    c.stale().await;
    let (status, body, parents) = c.thread(0).await;
    assert_eq!(status, 200, "{body}");
    c.w.check(parents == c.parents_before(5), format!("the chain ends below parent 5: {parents:?}"));
    let (left, cached) = chain_state(&c.rows[14]).expect("chain state");
    c.w.check(left == 0, format!("budget left {left}"));
    c.w.check(cached.len() == 17 && cached.get(&a[9]) == Some(&true), format!("cached {cached:?}"));
    c.w.finish().await;
}

/// T-56: a database error in the round (a hook on parent 5's author's
/// `head_check`): the request answers 200 with the chain ending below
/// parent 5; that DID is charged once and not cached; a later expansion
/// reply by the same author charges and checks again. When the erroring DID
/// is the anchor's, the answer is a nonexistent post's.
#[tokio::test]
async fn t56_a_database_error_in_the_round() {
    let Some(mut c) = Chain::new("t56-db-error", PublicMode::On, Layout::SamePds, 'e', 6, 0).await else {
        return;
    };
    let (anchor, a) = (c.rows[5].clone(), c.authors.clone());
    let by_a1 = c.w.reply(&a[0], "3kexpa1222222", &anchor).await;
    c.ready().await;
    c.stale().await;
    {
        let armed = hooks::arm(hooks::HEAD_CHECK_FAILS, &a[0]);
        let (status, body, parents) = c.thread(0).await;
        assert_eq!(status, 200, "{body}");
        c.w.check(parents == c.parents_before(5), format!("the chain ends below parent 5: {parents:?}"));
        let (left, cached) = chain_state(&anchor).expect("chain state");
        // A6, Dave, Alice, A5 to A1: 8 charged, A1 among them, uncached.
        c.w.check(left == 8 && !cached.contains_key(&a[0]) && cached.len() == 7, format!("budget {left}, cached {cached:?}"));
        c.w.check(armed.hits() == 1, format!("checked once: {}", armed.hits()));
        c.stale().await;
        let (status, body, _) = c.thread(1).await;
        assert_eq!(status, 200, "{body}");
        let v: Value = serde_json::from_str(&body).expect("json");
        c.w.check(!reply_uris(&v["thread"]).contains(&by_a1), "the erroring author's expansion reply misses".to_string());
        c.w.check(armed.hits() == 3, format!("charged and checked again in the expansion: {}", armed.hits()));
    }
    c.stale().await;
    let armed = hooks::arm(hooks::HEAD_CHECK_FAILS, &a[5]);
    let (status, body, _) = c.thread(0).await;
    drop(armed);
    let missing = format!("at://{}/{NSID_POST_PRIVATE}/3knothere222", a[5]);
    let (_, nothing, _) = get(&c.w.base, THREAD, &format!("uri={missing}"), DAVE, DAVE_KEY).await;
    c.w.check(status == 400 && body == nothing, format!("an erroring anchor: {status} {body}"));
    c.w.finish().await;
}

/// T-56: readiness before the round. A parent whose author is not ready
/// cuts the prefix there, and no DID above it is charged or fetched.
#[tokio::test]
async fn t56_readiness_cuts_the_prefix() {
    let Some(mut c) = Chain::new("t56-readiness", PublicMode::On, Layout::SamePds, 'g', 15, 0).await else {
        return;
    };
    let a = c.authors.clone();
    let mut conn = c.w.f.pool.acquire().await.expect("conn");
    readiness::clear_owner(&mut conn, &a[9]).await.expect("clear");
    drop(conn);
    c.stale().await;
    let (status, body, parents) = c.thread(0).await;
    assert_eq!(status, 200, "{body}");
    c.w.check(parents == c.parents_before(5), format!("the prefix is cut at parent 5: {parents:?}"));
    let calls: HashSet<String> = c.heads.calls().into_iter().collect();
    let mut round: HashSet<String> = [a[14].clone(), DAVE.to_string(), ALICE.to_string()].into_iter().collect();
    round.extend(a[10..14].iter().cloned());
    c.w.check(calls == round, format!("the round stops at the cut: {calls:?}"));
    c.w.finish().await;
}

/// T-56: an unreadable parent, root or quoted post spends no head budget,
/// with heads stale: Dave's own reply R (which he reads) has an unreadable
/// parent and an unreadable claimed root, and his own post quotes an
/// unreadable post; their author's head is never fetched. A claimed root
/// whose owner is not ready misses on its own read and charges nothing the
/// round did not.
#[tokio::test]
async fn t56_unreadable_parents_roots_and_quotes_spend_nothing() {
    let Some(mut c) = Chain::new("t56-unreadable", PublicMode::On, Layout::SamePds, 'u', 0, 0).await else {
        return;
    };
    let w = &mut c.w;
    let bob_aud = w.f.audience(BOB, "3kt56bobaud2", Some("list"), &[CAROL]).await;
    let dave_aud = w.f.audience(DAVE, "3kt56daveau2", Some("list"), &[CAROL]).await;
    let x = w.f.private_post(BOB, "3kt56xroot22", &bob_aud, None).await;
    let m = w.f.private_post(BOB, "3kt56middle2", &bob_aud, Some((&x, &x))).await;
    let r = w.f.private_post(DAVE, "3kt56dave222", &dave_aud, Some((&x, &m))).await;
    let quoting = w.f.post_with(DAVE, "3kt56quote22", &dave_aud, quote_of(&m)).await;
    let carol_aud = w.f.audience(CAROL, "3kt56carolau", Some("list"), &[DAVE]).await;
    let y = w.f.private_post(CAROL, "3kt56yroot22", &carol_aud, None).await;
    let r2 = w.f.private_post(DAVE, "3kt56dave2r2", &dave_aud, Some((&y, &y))).await;
    w.f.ready_all().await;
    c.stale().await;
    let (_, t) = thread_of(&c.w.base, DAVE, &format!("uri={r}")).await;
    let (_, p) = posts_of(&c.w.base, DAVE, &[&quoting]).await;
    let calls = c.heads.calls();
    c.w.check(t.get("parent").is_none() && p[0]["embed"]["record"]["$type"] == "app.bsky.embed.record#viewNotFound", format!("{t} {}", p[0]));
    c.w.check(!calls.iter().any(|d| d == BOB), format!("Bob's head is never fetched: {calls:?}"));
    // A root whose owner is not ready: its own read misses, before its head.
    let mut conn = c.w.f.pool.acquire().await.expect("conn");
    readiness::clear_owner(&mut conn, CAROL).await.expect("clear");
    drop(conn);
    c.stale().await;
    let (_, t) = thread_of(&c.w.base, DAVE, &format!("uri={r2}")).await;
    let calls = c.heads.calls();
    c.w.check(t["post"]["uri"] == r2.as_str() && calls == [DAVE.to_string()], format!("{t}: {calls:?}"));
    c.w.finish().await;
}

/// T-56: `getReplies`' one-row root lookup gives HEAD's claimed root (the
/// anchor's view's `reply.root`, or the anchor) for a thread reply, a
/// branch reply and a top-level post.
#[tokio::test]
async fn t56_the_root_lookup_is_the_claimed_root() {
    let Some(w) = walk_on("t56-root-lookup").await else {
        return;
    };
    let (t, aud) = (w.root.clone(), w.aud.clone());
    let x = format!("at://{DAVE}/{PUBLIC_POST}/3kt56public2");
    let thread_reply = w.reply(BOB, "3kt56thread2", &t).await;
    let branch_root = w.f.private_post(ALICE, "3kt56branch2", &aud, Some((&x, &x))).await;
    let branch_reply = w.f.private_post(BOB, "3kt56brreply", &aud, Some((&x, &branch_root))).await;
    w.f.ready_all().await;
    for uri in [&t, &thread_reply, &branch_reply] {
        let (_, p) = posts_of(&w.base, DAVE, &[uri]).await;
        let claimed = p[0]["record"]["reply"]["root"]["uri"].as_str().unwrap_or(uri).to_string();
        let stored = schema::stored_root(&w.f.pool, uri).await.expect("lookup").expect("stored").unwrap_or_else(|| uri.clone());
        assert_eq!(stored, claimed, "{uri}");
    }
    w.finish().await;
}

async fn walk_on(test: &str) -> Option<Walk> {
    Walk::new_in(test, PublicMode::On, Layout::SamePds, Some("followers"), &[], &[], Arc::new(NoHeads)).await
}

/// T-56: the public top. A branch's top private row with a stored public
/// parent shows it, as at HEAD; with `parentHeight` 0 on a branch root, or
/// exactly `parentHeight` private ancestors under the public post, none
/// shows.
#[tokio::test]
async fn t56_the_public_top_respects_parent_height() {
    let Some(w) = walk_on("t56-public-top").await else {
        return;
    };
    let aud = w.aud.clone();
    w.f.ingest(DAVE, PUBLIC_POST, "3kt56public2", json!({"$type": PUBLIC_POST, "text": "public", "createdAt": "2026-09-26T00:00:00Z"})).await;
    let x = format!("at://{DAVE}/{PUBLIC_POST}/3kt56public2");
    let b0 = w.f.private_post(ALICE, "3kt56b0root2", &aud, Some((&x, &x))).await;
    let b1 = w.f.private_post(BOB, "3kt56b1reply", &aud, Some((&x, &b0))).await;
    let b2 = w.f.private_post(CAROL, "3kt56b2reply", &aud, Some((&x, &b1))).await;
    w.f.ready_all().await;
    let top = |t: &Value| -> Vec<String> {
        let mut out = Vec::new();
        let mut node = &t["parent"];
        while let Some(uri) = node["post"]["uri"].as_str() {
            out.push(uri.to_string());
            node = &node["parent"];
        }
        out
    };
    let (_, t) = thread_of(&w.base, DAVE, &format!("uri={b2}")).await;
    assert_eq!(top(&t), [b1.clone(), b0.clone(), x.clone()], "the public top shows: {t}");
    let (_, t) = thread_of(&w.base, DAVE, &format!("uri={b0}&parentHeight=0")).await;
    assert!(t.get("parent").is_none(), "parentHeight 0 on a branch root: {t}");
    let (_, t) = thread_of(&w.base, DAVE, &format!("uri={b2}&parentHeight=2")).await;
    assert_eq!(top(&t), [b1.clone(), b0.clone()], "exactly parentHeight private ancestors: {t}");
    w.finish().await;
}

/// T-56: a parent whose decoded `reply.parent` disagrees with the
/// structural chain ends the chain there (a write between the structural
/// statement and the reads).
#[tokio::test]
async fn t56_a_disagreeing_parent_ends_the_chain() {
    let Some(w) = walk_on("t56-disagree").await else {
        return;
    };
    let t = w.root.clone();
    let r1 = w.reply(BOB, "3kt56dis1222", &t).await;
    let r2 = w.reply(CAROL, "3kt56dis2222", &r1).await;
    let r3 = w.reply(BOB, "3kt56dis3222", &r2).await;
    let r4 = w.reply(CAROL, "3kt56dis4222", &r3).await;
    w.f.ready_all().await;
    let parents = |body: &str| {
        let v: Value = serde_json::from_str(body).expect("json");
        let mut out = Vec::new();
        let mut node = &v["thread"]["parent"];
        while let Some(uri) = node["post"]["uri"].as_str() {
            out.push(uri.to_string());
            node = &node["parent"];
        }
        out
    };
    let (_, body, _) = get(&w.base, THREAD, &format!("uri={r4}"), DAVE, DAVE_KEY).await;
    assert_eq!(parents(&body), [r3.clone(), r2.clone(), r1.clone(), t.clone()]);
    let armed = hooks::arm(hooks::THREAD_AFTER_STRUCTURE, &r4);
    let base = w.base.clone();
    let query = format!("uri={r4}");
    let request = tokio::spawn(async move { get(&base, THREAD, &query, DAVE, DAVE_KEY).await });
    armed.reached().await;
    // R2 now replies to the root: its decoded parent is T, not R1.
    exec(&w.f.pool, &format!("UPDATE private_posts SET reply_parent_uri = '{t}' WHERE uri = '{r2}'")).await;
    armed.release();
    let (status, body, _) = request.await.expect("request");
    assert_eq!(status, 200, "{body}");
    assert_eq!(parents(&body), [r3, r2], "the chain ends at the disagreeing parent");
    drop(armed);
    w.finish().await;
}

/// T-56: the read cap. An author's feed reads at most `limit × 2`
/// candidates a page and ends with a cursor at the last row read; a short
/// page reads exactly `limit × 2`. Rows the reader is not entitled to
/// change no page; a reader an author blocks gets the pages it would get
/// were that author's rows absent.
#[tokio::test]
async fn t56_the_read_cap() {
    let z = did('z', 1);
    let y = did('z', 2);
    let Some(w) = Walk::new_in("t56-read-cap", PublicMode::On, Layout::SamePds, Some("followers"), &[], &[z.clone(), y.clone()], Arc::new(NoHeads)).await
    else {
        return;
    };
    let z_aud = w.f.audience(&z, "3kt56zaud222", Some("list"), &[CAROL, DAVE]).await;
    let z_dave = w.f.audience(&z, "3kt56zdave22", Some("list"), &[DAVE]).await;
    // Newest first by record key: ten undecodable, then two readable.
    let mut readable = Vec::new();
    for i in 0..2 {
        readable.push(w.f.private_post(&z, &format!("3kt56r{i}222222"), &z_aud, None).await);
    }
    readable.reverse();
    for i in 0..10 {
        w.f.undecodable(&z, &format!("3kt56u{i:02}22222"), &z_aud, None).await;
    }
    let y_aud = w.f.audience(&y, "3kt56yaud222", Some("list"), &[DAVE]).await;
    for i in 0..3 {
        w.f.private_post(&y, &format!("3kt56y{i}222222"), &y_aud, None).await;
    }
    w.f.ready_all().await;
    for d in [&z, &y] {
        w.f.ready(d).await;
        exec(&w.f.pool, &follow_sql(DAVE, d)).await;
    }
    let reads = hooks::arm_delay(hooks::PRIVATE_READ, &z, Duration::ZERO, None);
    let (_, first, _) = json_of(&w.base, FEED, &format!("actor={z}&limit=3"), DAVE).await;
    assert_eq!((feed_uris(&first).len(), reads.hits()), (0, 6), "a short page reads exactly limit × 2: {first}");
    let cursor = first["cursor"].as_str().expect("a cursor at the last row read").to_string();
    let (_, second, _) = json_of(&w.base, FEED, &format!("actor={z}&limit=3&cursor={cursor}"), DAVE).await;
    assert_eq!(feed_uris(&second), readable, "the second page starts after the sixth row");
    assert!(second.get("cursor").is_none(), "{second}");
    assert_eq!(reads.hits(), 12);
    drop(reads);

    // Rows Carol is not entitled to change none of her pages.
    let page_trace = |pages: Vec<Value>| pages.iter().map(|p| (feed_uris(p), p.get("cursor").is_some())).collect::<Vec<_>>();
    let before = page_trace(pages(&w.base, FEED, &format!("actor={z}&limit=1"), CAROL, 20).await);
    for i in 0..6 {
        w.f.private_post(&z, &format!("3kt56d{i}222222"), &z_dave, None).await;
    }
    let after = page_trace(pages(&w.base, FEED, &format!("actor={z}&limit=1"), CAROL, 20).await);
    assert_eq!(before, after, "unentitled rows change no page");

    // Y blocks Dave: his timeline is what it would be without Y's rows.
    exec(&w.f.pool, &block_sql(&y, DAVE)).await;
    let blocked = page_trace(pages(&w.base, TIMELINE, "limit=2", DAVE, 30).await);
    exec(&w.f.pool, &format!("DELETE FROM private_posts WHERE author_did = '{y}'")).await;
    exec(&w.f.pool, "DELETE FROM blocks").await;
    let absent = page_trace(pages(&w.base, TIMELINE, "limit=2", DAVE, 30).await);
    assert_eq!(blocked, absent, "a blocking author's rows are as if absent");
    w.finish().await;
}
