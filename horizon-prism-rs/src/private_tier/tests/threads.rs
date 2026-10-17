//! S12, private threads and branches (docs/internal/design/private-threads.md
//! §6): who reads a reply (T-46, T-47), anchor validity (T-48), branches
//! (T-49), the cap (T-50), C-19 (T-51), the view's `audience` (T-53),
//! self-service failures (T-58) and the self arm's gate (T-59). The one
//! builder (T-52) and the pre-release count (T-55) are in `builder.rs`, the
//! read cap and the head round (T-56) in `round.rs`, the row-class ratios
//! (T-54) with B-5's fixtures in `b5.rs`, and T-57 with the config and the
//! bench source.
//!
//! Every data test writes through the writer, reads through the oracle, the
//! prefilter and `getPosts` from one fixture ([`Walk::expect`]), and runs
//! twice: public indexing On with every account on one PDS, and Off with
//! every account but Alice's on another.

use super::*;

/// Not a member and outside S: a writer admission takes only as class M.
const FRANK: &str = "did:plc:ffffffffffffffffffffffff";

/// The two runs of every data test (§6).
const RUNS: [(PublicMode, Layout, &str); 2] = [(PublicMode::On, Layout::SamePds, "on"), (PublicMode::Off, Layout::OffPds, "off")];

/// A [`Walk`] for one run: Alice's audience `mode` of `members` and her root
/// under it, Bob, Carol and Dave following her.
async fn walk(test: &str, public: PublicMode, layout: Layout, mode: &str, members: &[&str]) -> Option<Walk> {
    Walk::new_in(test, public, layout, Some(mode), members, &[], Arc::new(NoHeads)).await
}

impl Walk {
    /// Re-point Alice's root at `audience` (a move), at `rev`.
    pub(super) async fn move_root(&self, audience: &str, rev: &str) {
        let record = self.f.post_record(ALICE, "3kroot222222", audience, None).await;
        self.f.apply_op(ALICE, Action::Update, NSID_POST_PRIVATE, "3kroot222222", rev, Some(record)).await;
    }

    /// Rewrite Alice's audience `rkey` as `mode` of `members`, at `rev`.
    pub(super) async fn edit_audience(&self, rkey: &str, mode: &str, members: &[&str], rev: &str) {
        let record = json!({"$type": NSID_AUDIENCE, "mode": mode, "members": members, "createdAt": "2026-09-26T00:00:00Z"});
        self.f.apply_op(ALICE, Action::Update, NSID_AUDIENCE, rkey, rev, Some(record)).await;
    }

    pub(super) async fn label(&self, subject: &str, val: &str) {
        exec(&self.f.pool, &format!("INSERT INTO labels (subject, val) VALUES ('{subject}', '{val}')")).await;
    }

    /// Record a failure unless `ok`.
    pub(super) fn check(&mut self, ok: bool, what: String) {
        if !ok {
            self.failures.push(what);
        }
    }

    /// Whether `reader` reads `p` on every oracle path (T-47): `getPosts`
    /// with the oracle and the prefilter ([`Walk::expect`]), the
    /// `getPostThread` anchor, the parent chain of `child` (a reply to `p`
    /// the reader reads either way), a quote embed in `quote` (a post the
    /// reader reads quoting `p`), and the `getReplies` anchor (which lists
    /// `child` only when `p` reads).
    #[allow(clippy::too_many_arguments)]
    async fn expect_paths(&mut self, case: &str, reader: &str, p: &str, child: &str, quote: &str, expected: bool) {
        self.expect(case, reader, p, expected).await;
        let (status, body, _) = get(&self.base, THREAD, &format!("uri={p}"), reader, key_of(reader)).await;
        let anchor = status == 200 && serde_json::from_str::<Value>(&body).is_ok_and(|v| v["thread"]["post"]["uri"] == p);
        let (status, body, _) = get(&self.base, THREAD, &format!("uri={child}"), reader, key_of(reader)).await;
        if status != 200 {
            self.failures.push(format!("{case}: {reader} cannot read the child {child} of {p}: {body}"));
            return;
        }
        let chain = serde_json::from_str::<Value>(&body).is_ok_and(|v| v["thread"]["parent"]["post"]["uri"] == p);
        let (_, q) = posts_of(&self.base, reader, &[quote]).await;
        let embedded = q.first().is_some_and(|q| q["embed"]["record"]["$type"] == "app.bsky.embed.record#viewRecord");
        let (_, r, _) = json_of(&self.base, GET_REPLIES, &format!("uri={p}"), reader).await;
        let listed = replies_uris(&r).iter().any(|u| u == child);
        self.check(
            [anchor, chain, embedded, listed] == [expected; 4],
            format!("{case}: {reader} on {p}: expected {expected}, thread anchor {anchor}, chain {chain}, quote {embedded}, getReplies {listed}"),
        );
    }

    /// Moderation hides the author's own `uri` from it (T-59): fact 8's
    /// stage 1 (the gate, for the author's own row), the prefilter and
    /// `getPosts` agree. The oracle's rule 1 is not asked alone: the gate
    /// runs before it in the pipeline.
    async fn expect_hidden_from_self(&mut self, case: &str, author: &str, uri: &str) {
        let fact8 = schema::fact8(&self.f.pool, author, uri, self.cap).await.expect("fact 8");
        let prefilter = schema::prefilter_uris(&self.f.pool, author, &[uri.to_string()], self.cap).await.expect("prefilter");
        let (_, read) = posts_of(&self.base, author, &[uri]).await;
        self.check(
            !fact8.s1ok && prefilter.is_empty() && read.is_empty(),
            format!("{case}: {author}'s own {uri}: fact 8 {fact8:?}, prefilter {prefilter:?}, read {}", read.len()),
        );
    }

    /// T-46's self-reads: `reader` reads its own `uri` through the
    /// `getPostThread` anchor, its author feed and its bookmarks.
    async fn expect_self_reads(&mut self, case: &str, reader: &str, uri: &str) {
        let (status, body, _) = get(&self.base, THREAD, &format!("uri={uri}"), reader, key_of(reader)).await;
        let anchor = status == 200 && serde_json::from_str::<Value>(&body).is_ok_and(|v| v["thread"]["post"]["uri"] == uri);
        let (_, feed, _) = json_of(&self.base, FEED, &format!("actor={reader}"), reader).await;
        let in_feed = feed_uris(&feed).iter().any(|u| u == uri);
        let (status, _) = post(&self.base, CREATE_BOOKMARK, &json!({"uri": uri, "cid": "bafyany"}), reader).await;
        assert_eq!(status, 200);
        let (page, _) = bookmarks_of(&self.base, reader, "").await;
        let bookmarked = page["bookmarks"].as_array().expect("bookmarks").iter().any(|b| b["item"]["uri"] == uri);
        let (status, _) = post(&self.base, DELETE_BOOKMARK, &json!({"uri": uri}), reader).await;
        assert_eq!(status, 200);
        self.check(anchor && in_feed && bookmarked, format!("{case}: {reader}'s self-reads of {uri}: thread {anchor}, author feed {in_feed}, bookmarks {bookmarked}"));
    }
}

/// T-46 (P-21). Alice writes T under `followers`. Bob and Carol reply,
/// Dave replies under Carol, Alice replies Q under Bob, and Carol replies
/// under Bob. Alice moves T to L = {Carol, Dave}.
/// - Carol and Dave read T, Carol's reply, Dave's reply and Q; Bob's reply
///   and the reply under it are hidden; Bob reads his own.
/// - Self-read survives the owner's block either way (skydeval,
///   2026-10-16): Bob reads his reply through `getPosts`, the
///   `getPostThread` anchor, his author feed and his bookmarks, while no
///   one else reads it; only `!takedown` or `!hide` on it, or `!takedown`
///   on Bob, hides it from him (T-59).
/// - Adding Bob back restores everything; an edit of L, or an unfollow
///   under `followers`, hides that writer's reply and what is below it.
async fn t46(test: &str, public: PublicMode, layout: Layout) {
    let Some(mut w) = walk(test, public, layout, "followers", &[]).await else {
        return;
    };
    let t = w.root.clone();
    let rb = w.reply(BOB, "3kt46bob2222", &t).await;
    let rc = w.reply(CAROL, "3kt46carol22", &t).await;
    let rd = w.reply(DAVE, "3kt46dave222", &rc).await;
    let q = w.reply(ALICE, "3kt46owner22", &rb).await;
    let under_b = w.reply(CAROL, "3kt46underb2", &rb).await;
    w.f.ready_all().await;
    for uri in [&rb, &rc, &rd, &q, &under_b] {
        for reader in [BOB, CAROL, DAVE] {
            w.expect("followers, before the move", reader, uri, true).await;
        }
    }
    let self_read_survives = async |w: &mut Walk, phase: &str| {
        for (blocker, blocked) in [(ALICE, BOB), (BOB, ALICE)] {
            exec(&w.f.pool, &block_sql(blocker, blocked)).await;
            let case = format!("{phase}, {blocker} blocks {blocked}");
            w.expect(&case, BOB, &rb, true).await;
            w.expect_self_reads(&case, BOB, &rb).await;
            for reader in [CAROL, DAVE] {
                w.expect(&case, reader, &rb, false).await;
            }
            w.expect(&format!("{case}, below Bob"), DAVE, &under_b, false).await;
            w.expect(&format!("{case}, below Bob, its author"), CAROL, &under_b, true).await;
            exec(&w.f.pool, "DELETE FROM blocks").await;
        }
    };
    self_read_survives(&mut w, "followers").await;
    for (subject, val) in [(rb.as_str(), "!takedown"), (rb.as_str(), "!hide"), (BOB, "!takedown")] {
        w.label(subject, val).await;
        w.expect_hidden_from_self(&format!("{val} on {subject}"), BOB, &rb).await;
        exec(&w.f.pool, "DELETE FROM labels").await;
    }

    // The move.
    let l = w.f.audience(ALICE, "3kt46list222", Some("list"), &[CAROL, DAVE]).await;
    w.move_root(&l, REV2).await;
    for reader in [CAROL, DAVE] {
        for uri in [&t, &rc, &rd, &q] {
            w.expect("after the move", reader, uri, true).await;
        }
        w.expect("after the move, Bob does not stand", reader, &rb, false).await;
    }
    w.expect("after the move, below Bob", DAVE, &under_b, false).await;
    w.expect("after the move, Bob's own reply", BOB, &rb, true).await;
    w.expect_self_reads("after the move", BOB, &rb).await;
    self_read_survives(&mut w, "after the move").await;

    // Bob added back restores everything.
    w.edit_audience("3kt46list222", "list", &[BOB, CAROL, DAVE], REV2).await;
    for reader in [CAROL, DAVE] {
        for uri in [&rb, &under_b, &q] {
            w.expect("Bob added back", reader, uri, true).await;
        }
    }
    // An edit of L drops Carol: her reply, and Dave's below it, are hidden
    // from Dave; Carol reads hers.
    w.edit_audience("3kt46list222", "list", &[BOB, DAVE], REV3).await;
    w.expect("an edit drops Carol", DAVE, &rc, false).await;
    w.expect("an edit drops Carol, below her", BOB, &rd, false).await;
    w.expect("an edit drops Carol, her own", CAROL, &rc, true).await;
    // Under `followers` again, an unfollow does the same.
    w.move_root(&w.aud.clone(), REV3).await;
    w.expect("back under followers", DAVE, &rc, true).await;
    exec(&w.f.pool, &format!("DELETE FROM follows WHERE author_did = '{CAROL}'")).await;
    w.expect("Carol unfollows", DAVE, &rc, false).await;
    w.expect("Carol unfollows, below her", BOB, &rd, false).await;
    w.expect("Carol unfollows, her own", CAROL, &rc, true).await;
    w.finish().await;
}

#[tokio::test]
async fn t46_replies_follow_the_roots_current_audience() {
    for (public, layout, run) in RUNS {
        t46(&format!("t46-{run}"), public, layout).await;
    }
}

/// T-47 (admission), every case on every oracle path ([`Walk::expect_paths`]):
/// a reply under the replier's own audience in another account's thread,
/// and under the replier's own `everyone` audience, are their authors'
/// alone; a reply naming any of the owner's audiences is read exactly while
/// its writer stands; a sock-puppet chain (a step naming another owner's
/// audience) is its authors' alone; an owner reply with a foreign or missing
/// parent is read; owner replies naming a narrower or a wider owner audience
/// are read by exactly G's readers.
async fn t47(test: &str, public: PublicMode, layout: Layout) {
    let Some(mut w) = walk(test, public, layout, "followers", &[]).await else {
        return;
    };
    let (t, aud) = (w.root.clone(), w.aud.clone());
    let bob_list = w.f.audience(BOB, "3kt47boblist", Some("list"), &[CAROL, DAVE]).await;
    let bob_everyone = w.f.audience(BOB, "3kt47bobever", Some("everyone"), &[]).await;
    let carol_aud = w.f.audience(CAROL, "3kt47carolau", Some("list"), &[DAVE, BOB]).await;
    let narrower = w.f.audience(ALICE, "3kt47narrow2", Some("list"), &[DAVE]).await;
    let wider = w.f.audience(ALICE, "3kt47wider22", Some("everyone"), &[]).await;
    let foreign = w.f.private_post(BOB, "3kt47foreign", &bob_list, None).await;
    let ghost = format!("at://{NOBODY}/{NSID_POST_PRIVATE}/x");
    let own_aud = w.f.private_post(BOB, "3kt47ownaud2", &bob_list, Some((&t, &t))).await;
    let own_everyone = w.f.private_post(BOB, "3kt47ownever", &bob_everyone, Some((&t, &t))).await;
    let other_owner_aud = w.f.private_post(BOB, "3kt47otheraud", &narrower, Some((&t, &t))).await;
    let sock = w.f.private_post(BOB, "3kt47sock222", &carol_aud, Some((&t, &t))).await;
    let below_sock = w.f.private_post(CAROL, "3kt47belowsk", &aud, Some((&t, &sock))).await;
    let foreign_parent = w.f.private_post(ALICE, "3kt47forpar2", &aud, Some((&t, &foreign))).await;
    let missing_parent = w.f.private_post(ALICE, "3kt47mispar2", &aud, Some((&t, &ghost))).await;
    let owner_narrower = w.f.private_post(ALICE, "3kt47ownnarr", &narrower, Some((&t, &t))).await;
    let owner_wider = w.f.private_post(ALICE, "3kt47ownwide", &wider, Some((&t, &t))).await;
    // (case, post, its author, read by G's readers)
    let cases = [
        ("own audience in another's thread", own_aud, BOB, false),
        ("own everyone audience", own_everyone, BOB, false),
        ("another owner audience, its writer standing", other_owner_aud, BOB, true),
        ("sock puppet", sock, BOB, false),
        ("below a sock puppet", below_sock, CAROL, false),
        ("owner reply, foreign parent", foreign_parent, ALICE, true),
        ("owner reply, missing parent", missing_parent, ALICE, true),
        ("owner reply, narrower owner audience", owner_narrower, ALICE, true),
        ("owner reply, wider owner audience", owner_wider, ALICE, true),
    ];
    let mut probes = Vec::new();
    for (i, (_, p, ..)) in cases.iter().enumerate() {
        let child = w.f.private_post(ALICE, &format!("3kt47child{i}2"), &aud, Some((&t, p))).await;
        let quote = w.f.post_with(ALICE, &format!("3kt47quote{i}2"), &aud, quote_of(p)).await;
        probes.push((child, quote));
    }
    w.f.ready_all().await;
    for ((case, p, author, read), (child, quote)) in cases.iter().zip(&probes) {
        w.expect_paths(case, author, p, child, quote, true).await;
        for reader in [ALICE, BOB, CAROL, DAVE].into_iter().filter(|r| r != author) {
            w.expect_paths(case, reader, p, child, quote, *read).await;
        }
    }
    // Read exactly while the writer stands.
    exec(&w.f.pool, &format!("DELETE FROM follows WHERE author_did = '{BOB}'")).await;
    w.expect_paths("another owner audience, its writer not standing", DAVE, &cases[2].1, &probes[2].0, &probes[2].1, false).await;
    exec(&w.f.pool, &follow_sql(BOB, ALICE)).await;
    w.expect_paths("another owner audience, its writer standing again", DAVE, &cases[2].1, &probes[2].0, &probes[2].1, true).await;
    // G's readers exactly: Dave, out of G, reads neither owner reply, though
    // the narrower lists him and the wider is `everyone`.
    exec(&w.f.pool, &format!("DELETE FROM follows WHERE author_did = '{DAVE}'")).await;
    for (case, p, ..) in &cases[7..] {
        w.expect(&format!("{case}, outside G"), DAVE, p, false).await;
    }
    w.finish().await;
}

#[tokio::test]
async fn t47_admission_on_every_oracle_path() {
    for (public, layout, run) in RUNS {
        t47(&format!("t47-{run}"), public, layout).await;
    }
}

/// T-48 (anchor validity): every reply, the owner's included, is its
/// author's alone when its root is deleted, never stored, a reply, or under
/// another account's audience; is `!takedown` or `!hide`; has an author with
/// a hiding status; when its parent is public and its root private; and when
/// it names its own URI as its root (review 13, m1), which leaves it out of
/// its parent's replies and `replyCount`, its children with it.
async fn t48(test: &str, public: PublicMode, layout: Layout) {
    let Some(mut w) = walk(test, public, layout, "followers", &[]).await else {
        return;
    };
    let (t, aud) = (w.root.clone(), w.aud.clone());
    let carol_aud = w.f.audience(CAROL, "3kt48carolau", Some("followers"), &[]).await;
    let carol_root = w.f.private_post(CAROL, "3kt48carolrt", &carol_aud, None).await;
    for did in [ALICE, BOB, DAVE] {
        exec(&w.f.pool, &follow_sql(did, CAROL)).await;
    }
    let deleted = w.f.private_post(ALICE, "3kt48deleted", &aud, None).await;
    let never = format!("at://{ALICE}/{NSID_POST_PRIVATE}/3kt48never22");
    let a_reply = w.reply(ALICE, "3kt48areply2", &t).await;
    let elsewhere = w.f.private_post(ALICE, "3kt48elsewhr", &carol_aud, None).await;
    let taken_down = w.f.private_post(ALICE, "3kt48takedwn", &aud, None).await;
    let hidden = w.f.private_post(ALICE, "3kt48hidden2", &aud, None).await;
    let public_parent = format!("at://{DAVE}/{PUBLIC_POST}/3kt48public2");
    // (case, root, parent, audience of the replies)
    let roots = [
        ("root deleted", deleted.clone(), deleted.clone(), aud.clone()),
        ("root never stored", never.clone(), never.clone(), aud.clone()),
        ("root is a reply", a_reply.clone(), a_reply.clone(), aud.clone()),
        ("root under another account's audience", elsewhere.clone(), elsewhere.clone(), aud.clone()),
        ("root !takedown", taken_down.clone(), taken_down.clone(), aud.clone()),
        ("root !hide", hidden.clone(), hidden.clone(), aud.clone()),
        ("root's author deactivated", carol_root.clone(), carol_root.clone(), carol_aud.clone()),
        ("a public parent with a private root", t.clone(), public_parent.clone(), aud.clone()),
    ];
    let mut replies = Vec::new();
    for (i, (case, root, parent, audience)) in roots.iter().enumerate() {
        let owner = if *case == "root's author deactivated" { CAROL } else { ALICE };
        let by_bob = w.f.private_post(BOB, &format!("3kt48bob{i}222"), audience, Some((root, parent))).await;
        let by_owner = w.f.private_post(owner, &format!("3kt48own{i}222"), audience, Some((root, parent))).await;
        replies.push((*case, by_bob, by_owner, owner));
    }
    // A self-rooted thread reply: P names itself as its root and Carol's
    // reply X in Alice's thread as its parent, under Bob's own audience.
    let x = w.reply(CAROL, "3kt48xreply2", &t).await;
    let bob_aud = w.f.audience(BOB, "3kt48bobaud2", Some("followers"), &[]).await;
    let p_uri = format!("at://{BOB}/{NSID_POST_PRIVATE}/3kt48selfrt2");
    let self_rooted = w.f.private_post(BOB, "3kt48selfrt2", &bob_aud, Some((&p_uri, &x))).await;
    let below_self = w.f.private_post(BOB, "3kt48belowsr", &bob_aud, Some((&p_uri, &self_rooted))).await;
    w.f.ready_all().await;
    w.f.apply_op(ALICE, Action::Delete, NSID_POST_PRIVATE, "3kt48deleted", REV2, None).await;
    w.label(&taken_down, "!takedown").await;
    w.label(&hidden, "!hide").await;
    exec(&w.f.pool, &format!("UPDATE actors SET account_status = 'deactivated' WHERE did = '{CAROL}'")).await;
    for (case, by_bob, by_owner, owner) in &replies {
        w.expect(&format!("{case}, Bob's reply, its author"), BOB, by_bob, true).await;
        if *owner == ALICE {
            w.expect(&format!("{case}, the owner's reply, its author"), ALICE, by_owner, true).await;
        }
        for reader in [ALICE, BOB, CAROL, DAVE] {
            if reader != BOB {
                w.expect(&format!("{case}, Bob's reply"), reader, by_bob, false).await;
            }
            if reader != *owner {
                w.expect(&format!("{case}, the owner's reply"), reader, by_owner, false).await;
            }
        }
    }
    exec(&w.f.pool, &format!("UPDATE actors SET account_status = 'active' WHERE did = '{CAROL}'")).await;
    for uri in [&self_rooted, &below_self] {
        w.expect("self-rooted, its author", BOB, uri, true).await;
        for reader in [ALICE, CAROL, DAVE] {
            w.expect("self-rooted", reader, uri, false).await;
        }
    }
    for reader in [ALICE, DAVE] {
        let (_, v, _) = json_of(&w.base, GET_REPLIES, &format!("uri={x}"), reader).await;
        let listed = replies_uris(&v).contains(&self_rooted);
        let (_, p) = posts_of(&w.base, reader, &[&x]).await;
        let count = p[0]["replyCount"].as_i64();
        w.check(!listed && count == Some(0), format!("self-rooted under X for {reader}: listed {listed}, X's replyCount {count:?}"));
    }
    w.finish().await;
}

#[tokio::test]
async fn t48_replies_need_a_valid_anchor() {
    for (public, layout, run) in RUNS {
        t48(&format!("t48-{run}"), public, layout).await;
    }
}

/// T-49 (P-22): a private branch under a public post. Alice's branch root
/// B0 under her `followers` audience, then Bob's B1, Carol's B2 and Alice's
/// B3 below it, are read by her audience; the same whether the public
/// parent is stored, missing or of another collection. A forged branch root
/// (Carol's, naming Alice's audience) and replies to it are their authors'
/// alone. An owner branch reply below a hidden step is hidden.
async fn t49(test: &str, public: PublicMode, layout: Layout) {
    let Some(mut w) = walk(test, public, layout, "followers", &[]).await else {
        return;
    };
    let aud = w.aud.clone();
    let stored = format!("at://{DAVE}/{PUBLIC_POST}/3kt49stored2");
    w.f.ingest(DAVE, PUBLIC_POST, "3kt49stored2", json!({"$type": PUBLIC_POST, "text": "public", "createdAt": "2026-09-26T00:00:00Z"})).await;
    let parents = [
        ("stored", stored),
        ("missing", format!("at://{DAVE}/{PUBLIC_POST}/3kt49missing")),
        ("another collection", format!("at://{DAVE}/app.bsky.feed.like/3kt49like222")),
    ];
    let mut branches = Vec::new();
    for (i, (case, x)) in parents.iter().enumerate() {
        let b0 = w.f.private_post(ALICE, &format!("3kt49b0{i}2222"), &aud, Some((x, x))).await;
        let b1 = w.f.private_post(BOB, &format!("3kt49b1{i}2222"), &aud, Some((x, &b0))).await;
        let b2 = w.f.private_post(CAROL, &format!("3kt49b2{i}2222"), &aud, Some((x, &b1))).await;
        let b3 = w.f.private_post(ALICE, &format!("3kt49b3{i}2222"), &aud, Some((x, &b2))).await;
        branches.push((*case, [b0, b1, b2, b3]));
    }
    let x = parents[0].1.clone();
    let forged = w.f.private_post(CAROL, "3kt49forged2", &aud, Some((&x, &x))).await;
    let to_forged = w.f.private_post(BOB, "3kt49toforgd", &aud, Some((&x, &forged))).await;
    w.f.ready_all().await;
    for (case, rows) in &branches {
        for (j, uri) in rows.iter().enumerate() {
            for reader in [ALICE, BOB, CAROL, DAVE] {
                w.expect(&format!("public parent {case}, B{j}"), reader, uri, true).await;
            }
        }
    }
    for uri in [&forged, &to_forged] {
        for reader in [ALICE, DAVE] {
            w.expect("a forged branch root", reader, uri, false).await;
        }
    }
    w.expect("a forged branch root, its author", CAROL, &forged, true).await;
    w.expect("a reply to a forged branch root, its author", BOB, &to_forged, true).await;
    // Bob stops standing: B1, and B2 and Alice's B3 below it, are hidden.
    exec(&w.f.pool, &format!("DELETE FROM follows WHERE author_did = '{BOB}'")).await;
    for (case, rows) in &branches {
        w.expect(&format!("public parent {case}, B0 above the hidden step"), DAVE, &rows[0], true).await;
        for (j, uri) in rows.iter().enumerate().skip(1) {
            w.expect(&format!("public parent {case}, B{j} at or below the hidden step"), DAVE, uri, false).await;
        }
        w.expect(&format!("public parent {case}, the owner's B3, its author"), ALICE, &rows[3], true).await;
    }
    w.finish().await;
}

#[tokio::test]
async fn t49_branches_under_a_public_post() {
    for (public, layout, run) in RUNS {
        t49(&format!("t49-{run}"), public, layout).await;
    }
}

/// T-49's admission and purges, in Off: a non-member's private branch root
/// under a public post by another non-member, naming a member's audience,
/// is admitted only through the audience's author (the new subject), and
/// the member's removal purges it. The same post under its own author's
/// audience is not admitted at all.
#[tokio::test]
async fn t49_admission_and_purges_follow_the_audience() {
    let Some(db) = fresh_db("t49-admission").await else {
        return;
    };
    let f = fixture_in(&db.pool, "t49-admission", PublicMode::Off).await;
    let aud = f.audience(ALICE, "3kt49aud2222", Some("followers"), &[]).await;
    let x = format!("at://{EVE}/{PUBLIC_POST}/3kt49public2");
    let frank_aud = format!("at://{FRANK}/{NSID_AUDIENCE}/3kt49frank22");
    for (rkey, audience) in [("3kt49viaaud2", &aud), ("3kt49ownaud2", &frank_aud)] {
        let mut record = f.post_record(FRANK, rkey, audience, Some((&x, &x))).await;
        record["text"] = json!(rkey);
        f.ingest(FRANK, NSID_POST_PRIVATE, rkey, record).await;
    }
    let stored: Vec<String> =
        sqlx::query_scalar("SELECT uri FROM private_posts WHERE author_did = $1 ORDER BY uri").bind(FRANK).fetch_all(&f.pool).await.expect("rows");
    assert_eq!(stored, [format!("at://{FRANK}/{NSID_POST_PRIVATE}/3kt49viaaud2")], "admitted through the audience's author only");
    let mut conn = f.pool.acquire().await.expect("conn");
    KryphocronPrivateTier.purge_member_removed_batch(&mut conn, ALICE, 100).await.expect("purge");
    drop(conn);
    let left: i64 = sqlx::query_scalar("SELECT count(*) FROM private_posts WHERE author_did = $1").bind(FRANK).fetch_one(&f.pool).await.expect("count");
    assert_eq!(left, 0, "the member's removal purges a post admitted only through its audience");
    db.drop_db().await;
}

/// T-50 (the cap): at 16, the 16th reply below the root is served and the
/// 17th is not; an owner reply at level 10 serves down to level 26; a
/// branch reply 16 rows below its branch root is served, 17 is not; cycles
/// and dangling chains fail; at 100, 100 and not 101; a change to the cap
/// applies on the next read.
async fn t50(test: &str, public: PublicMode, layout: Layout) {
    let Some(mut w) = walk(test, public, layout, "followers", &[]).await else {
        return;
    };
    let (t, aud) = (w.root.clone(), w.aud.clone());
    let mut chain = Vec::new();
    let mut parent = t.clone();
    for level in 1..=101 {
        let author = if level % 2 == 1 { BOB } else { CAROL };
        parent = w.reply(author, &format!("3kt50l{level:03}222"), &parent).await;
        chain.push(parent.clone());
    }
    // Levels 1-9 by others, Alice's own reply at 10, then 11-27 by others.
    let mut owner_chain = Vec::new();
    let mut parent = t.clone();
    for level in 1..=27 {
        let author = match level {
            10 => ALICE,
            l if l % 2 == 1 => BOB,
            _ => CAROL,
        };
        parent = w.reply(author, &format!("3kt50o{level:03}222"), &parent).await;
        owner_chain.push(parent.clone());
    }
    let x = format!("at://{DAVE}/{PUBLIC_POST}/3kt50public2");
    let branch_root = w.f.private_post(ALICE, "3kt50branch2", &aud, Some((&x, &x))).await;
    let mut branch = Vec::new();
    let mut parent = branch_root.clone();
    for depth in 1..=17 {
        let author = if depth % 2 == 1 { BOB } else { CAROL };
        parent = w.f.private_post(author, &format!("3kt50b{depth:03}222"), &aud, Some((&x, &parent))).await;
        branch.push(parent.clone());
    }
    let cycle_a = format!("at://{BOB}/{NSID_POST_PRIVATE}/3kt50cyclea2");
    let cycle_b = format!("at://{CAROL}/{NSID_POST_PRIVATE}/3kt50cycleb2");
    w.reply(BOB, "3kt50cyclea2", &cycle_b).await;
    w.reply(CAROL, "3kt50cycleb2", &cycle_a).await;
    let into_cycle = w.reply(BOB, "3kt50intocyc", &cycle_a).await;
    let dangling = w.reply(CAROL, "3kt50dangle2", &format!("at://{NOBODY}/{NSID_POST_PRIVATE}/x")).await;
    w.f.ready_all().await;

    w.expect("cap 16, level 16", DAVE, &chain[15], true).await;
    w.expect("cap 16, level 17", DAVE, &chain[16], false).await;
    w.expect("cap 16, below the owner's level 10, level 26", DAVE, &owner_chain[25], true).await;
    w.expect("cap 16, below the owner's level 10, level 27", DAVE, &owner_chain[26], false).await;
    w.expect("cap 16, branch row 16", DAVE, &branch[15], true).await;
    w.expect("cap 16, branch row 17", DAVE, &branch[16], false).await;
    for uri in [&cycle_a, &cycle_b, &into_cycle, &dangling] {
        w.expect("cap 16, a cycle or a dangling chain", DAVE, uri, false).await;
    }
    for (author, uri) in [(BOB, &chain[16]), (BOB, &branch[16]), (BOB, &cycle_a)] {
        w.expect("cap 16, its author", author, uri, true).await;
    }
    w.set_cap(100).await;
    w.expect("cap 100, level 100", DAVE, &chain[99], true).await;
    w.expect("cap 100, level 101", DAVE, &chain[100], false).await;
    w.expect("cap 100, branch row 17", DAVE, &branch[16], true).await;
    for uri in [&cycle_a, &into_cycle, &dangling] {
        w.expect("cap 100, a cycle or a dangling chain", DAVE, uri, false).await;
    }
    w.set_cap(8).await;
    w.expect("cap 8, level 8", DAVE, &chain[7], true).await;
    w.expect("cap 8, level 9", DAVE, &chain[8], false).await;
    w.set_cap(CAP).await;
    w.expect("back at 16, level 16", DAVE, &chain[15], true).await;
    w.finish().await;
}

#[tokio::test]
async fn t50_the_walk_is_capped_by_config() {
    for (public, layout, run) in RUNS {
        t50(&format!("t50-{run}"), public, layout).await;
    }
}

/// How M, a row in the chain above a reply, is unreadable in T-51.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum C19 {
    /// M's writer no longer stands (unfollowed Alice under `followers`).
    NotStanding,
    /// M's writer blocks the reader.
    BlocksReader,
    /// M is `!takedown`.
    Takedown,
    /// O, Alice, has no `actors` row.
    OwnerNoActor,
    /// M claims another root than the reply's.
    RootIdentity,
    /// The reply below M is the owner's: read either way.
    OwnerBelow,
    /// M is the foreign parent of the owner's reply Q2, and a non-owner's
    /// reply R is below Q2: read either way.
    ForeignParentOfOwnerRow,
}

/// T-51 (C-19): Dave's outputs ([`reader_outputs`]) for the reply R below
/// M are byte-identical with M stored but unreadable and with M missing.
async fn c19(test: &str, public: PublicMode, layout: Layout, case: C19) {
    let Some(mut w) = walk(test, public, layout, "followers", &[]).await else {
        return;
    };
    let (t, aud) = (w.root.clone(), w.aud.clone());
    exec(&w.f.pool, &follow_sql(DAVE, CAROL)).await;
    let other_root = w.f.private_post(ALICE, "3kc19other22", &aud, None).await;
    let bob_aud = w.f.audience(BOB, "3kc19bobaud2", Some("list"), &[CAROL]).await;
    let (m, m_rkey) = match case {
        C19::RootIdentity => (w.f.private_post(BOB, "3kc19middle2", &aud, Some((&other_root, &t))).await, "3kc19middle2"),
        C19::ForeignParentOfOwnerRow => (w.f.private_post(BOB, "3kc19middle2", &bob_aud, None).await, "3kc19middle2"),
        _ => (w.reply(BOB, "3kc19middle2", &t).await, "3kc19middle2"),
    };
    let (r, read) = match case {
        C19::OwnerBelow => (w.reply(ALICE, "3kc19leaf222", &m).await, true),
        C19::ForeignParentOfOwnerRow => {
            let q2 = w.reply(ALICE, "3kc19ownerq2", &m).await;
            (w.reply(CAROL, "3kc19leaf222", &q2).await, true)
        }
        _ => (w.reply(CAROL, "3kc19leaf222", &m).await, false),
    };
    w.f.ready_all().await;
    match case {
        C19::NotStanding => exec(&w.f.pool, &format!("DELETE FROM follows WHERE author_did = '{BOB}'")).await,
        C19::BlocksReader => exec(&w.f.pool, &block_sql(BOB, DAVE)).await,
        C19::Takedown => w.label(&m, "!takedown").await,
        C19::OwnerNoActor => exec(&w.f.pool, &format!("DELETE FROM actors WHERE did = '{ALICE}'")).await,
        C19::OwnerBelow => exec(&w.f.pool, &format!("DELETE FROM follows WHERE author_did = '{BOB}'")).await,
        C19::RootIdentity | C19::ForeignParentOfOwnerRow => {}
    }
    w.expect(&format!("{case:?}: M"), DAVE, &m, false).await;
    w.expect(&format!("{case:?}: R, M stored"), DAVE, &r, read && case != C19::OwnerNoActor).await;
    let with_m = vec![("M stored".to_string(), reader_outputs(&w, &r, false).await)];
    w.f.apply_op(BOB, Action::Delete, NSID_POST_PRIVATE, m_rkey, REV2, None).await;
    w.expect(&format!("{case:?}: R, M missing"), DAVE, &r, read && case != C19::OwnerNoActor).await;
    let without_m = vec![("M stored".to_string(), reader_outputs(&w, &r, false).await)];
    compare_fixtures(&mut w.failures, &with_m, &without_m);
    if case == C19::OwnerBelow {
        // Q's own thread view: the chain stops at M either way.
        let (_, t) = thread_of(&w.base, DAVE, &format!("uri={r}")).await;
        w.check(t.get("parent").is_none(), format!("OwnerBelow: Q's thread shows a parent: {t}"));
    }
    w.finish().await;
}

#[tokio::test]
async fn t51_stored_unreadable_and_missing_rows_read_alike() {
    for case in [
        C19::NotStanding,
        C19::BlocksReader,
        C19::Takedown,
        C19::OwnerNoActor,
        C19::RootIdentity,
        C19::OwnerBelow,
        C19::ForeignParentOfOwnerRow,
    ] {
        for (public, layout, run) in RUNS {
            c19(&format!("t51-{case:?}-{run}").to_lowercase(), public, layout, case).await;
        }
    }
}

/// T-53 (the view): `audience` is G, the root's current audience, on a
/// reader's view and on a standing author's own read; it is left out of an
/// outsider's own read, and of a reply under its author's own audience in
/// another account's thread.
async fn t53(test: &str, public: PublicMode, layout: Layout) {
    let Some(mut w) = walk(test, public, layout, "followers", &[]).await else {
        return;
    };
    let (t, aud) = (w.root.clone(), w.aud.clone());
    let narrower = w.f.audience(ALICE, "3kt53narrow2", Some("list"), &[DAVE]).await;
    let bob_aud = w.f.audience(BOB, "3kt53bobaud2", Some("list"), &[DAVE]).await;
    let reply = w.f.private_post(BOB, "3kt53reply22", &narrower, Some((&t, &t))).await;
    let own = w.f.private_post(BOB, "3kt53ownaud2", &bob_aud, Some((&t, &t))).await;
    w.f.ready_all().await;
    let audience = |w: &Walk, reader: &'static str, uri: String| {
        let base = w.base.clone();
        async move { posts_of(&base, reader, &[&uri]).await.1.first().map(|p| p.get("audience").cloned()) }
    };
    let g = Some(Some(json!({"uri": aud, "mode": "followers"})));
    let found = audience(&w, DAVE, reply.clone()).await;
    w.check(found == g, format!("a reader's view: {found:?}"));
    let found = audience(&w, BOB, reply.clone()).await;
    w.check(found == g, format!("a standing author's own read: {found:?}"));
    let found = audience(&w, BOB, own.clone()).await;
    w.check(found == Some(None), format!("a reply under its author's own audience: {found:?}"));
    let found = audience(&w, ALICE, t.clone()).await;
    w.check(found == g, format!("the root: {found:?}"));
    exec(&w.f.pool, &format!("DELETE FROM follows WHERE author_did = '{BOB}'")).await;
    let found = audience(&w, BOB, reply.clone()).await;
    w.check(found == Some(None), format!("an outsider's own read: {found:?}"));
    w.finish().await;
}

#[tokio::test]
async fn t53_the_view_names_the_governing_audience() {
    for (public, layout, run) in RUNS {
        t53(&format!("t53-{run}"), public, layout).await;
    }
}

/// T-58 (self-service failures): the top writer of a chain, Bob, hides the
/// replies below his from Dave when he blocks Dave, deactivates, blocks
/// Alice, unfollows her under `followers`, or is removed from her `list`.
async fn t58(test: &str, public: PublicMode, layout: Layout) {
    let Some(mut w) = walk(test, public, layout, "followers", &[]).await else {
        return;
    };
    let t = w.root.clone();
    let rb = w.reply(BOB, "3kt58bob2222", &t).await;
    let rc = w.reply(CAROL, "3kt58carol22", &rb).await;
    let list = w.f.audience(ALICE, "3kt58list222", Some("list"), &[BOB, CAROL, DAVE]).await;
    let list_root = w.f.private_post(ALICE, "3kt58lstroot", &list, None).await;
    let lb = w.f.private_post(BOB, "3kt58lbob222", &list, Some((&list_root, &list_root))).await;
    let lc = w.f.private_post(CAROL, "3kt58lcarol2", &list, Some((&list_root, &lb))).await;
    w.f.ready_all().await;
    for uri in [&rc, &lc] {
        w.expect("before", DAVE, uri, true).await;
    }
    let undo = "DELETE FROM blocks";
    for (case, apply, revert) in [
        ("blocks the reader", block_sql(BOB, DAVE), undo.to_string()),
        (
            "deactivates",
            format!("UPDATE actors SET account_status = 'deactivated' WHERE did = '{BOB}'"),
            format!("UPDATE actors SET account_status = 'active' WHERE did = '{BOB}'"),
        ),
        ("blocks the owner", block_sql(BOB, ALICE), undo.to_string()),
        ("unfollows", format!("DELETE FROM follows WHERE author_did = '{BOB}'"), follow_sql(BOB, ALICE)),
    ] {
        exec(&w.f.pool, &apply).await;
        w.expect(case, DAVE, &rc, false).await;
        w.expect(&format!("{case}, Carol's own"), CAROL, &rc, true).await;
        exec(&w.f.pool, &revert).await;
        w.expect(&format!("{case}, undone"), DAVE, &rc, true).await;
    }
    w.edit_audience("3kt58list222", "list", &[CAROL, DAVE], REV2).await;
    w.expect("removed", DAVE, &lc, false).await;
    w.finish().await;
}

#[tokio::test]
async fn t58_self_service_failures_hide_what_is_below() {
    for (public, layout, run) in RUNS {
        t58(&format!("t58-{run}"), public, layout).await;
    }
}

/// T-59 (the self arm keeps the gate; review 12, m1): Dave's reply R to X
/// and his private quote Q of X, each `!takedown` and then `!hide`, are out
/// of his `replyCount` and `quoteCount` on X, are not candidates on his
/// author feed, in search or in `getReplies`, and neither fact 8 nor the
/// pipeline reads them for him. Removing the label restores all three.
async fn t59(test: &str, public: PublicMode, layout: Layout) {
    let Some(mut w) = walk(test, public, layout, "followers", &[]).await else {
        return;
    };
    let x = w.root.clone();
    let dave_aud = w.f.audience(DAVE, "3kt59daveau2", Some("list"), &[BOB]).await;
    let r = w.reply(DAVE, "3kt59reply22", &x).await;
    let q = w.f.post_with(DAVE, "3kt59quote22", &dave_aud, quote_of(&x)).await;
    w.f.ready_all().await;
    let state = |w: &Walk| {
        let (base, x, r, q) = (w.base.clone(), x.clone(), r.clone(), q.clone());
        async move {
            let (_, p) = posts_of(&base, DAVE, &[&x]).await;
            let counts = (p[0]["replyCount"].as_i64(), p[0]["quoteCount"].as_i64());
            let (_, feed, _) = json_of(&base, FEED, &format!("actor={DAVE}"), DAVE).await;
            let in_feed = feed_uris(&feed);
            let searched: Vec<String> = pages(&base, SEARCH, "q=3kt59", DAVE, 20).await.iter().flat_map(post_uris).collect();
            let (_, replies, _) = json_of(&base, GET_REPLIES, &format!("uri={x}"), DAVE).await;
            let listed = replies_uris(&replies);
            let has = |v: &[String], u: &str| v.iter().any(|x| x == u);
            (counts, (has(&in_feed, &r), has(&in_feed, &q)), (has(&searched, &r), has(&searched, &q)), has(&listed, &r))
        }
    };
    let readable = state(&w).await;
    w.check(readable == ((Some(1), Some(1)), (true, true), (true, true), true), format!("unlabelled: {readable:?}"));
    for val in ["!takedown", "!hide"] {
        for uri in [&r, &q] {
            w.label(uri, val).await;
        }
        let hidden = state(&w).await;
        w.check(hidden == ((Some(0), Some(0)), (false, false), (false, false), false), format!("{val}: {hidden:?}"));
        // Fact 8's stage 1 for the author's own row is the gate, so it and
        // the pipeline agree; the oracle's rule 1 is not consulted alone.
        for uri in [&r, &q] {
            let fact8 = schema::fact8(&w.f.pool, DAVE, uri, CAP).await.expect("fact 8");
            let prefilter = schema::prefilter_uris(&w.f.pool, DAVE, std::slice::from_ref(uri), CAP).await.expect("prefilter");
            let (_, read) = posts_of(&w.base, DAVE, &[uri]).await;
            w.check(!fact8.s1ok && prefilter.is_empty() && read.is_empty(), format!("{val} on {uri}: fact 8 {fact8:?}, prefilter {prefilter:?}, read {}", read.len()));
        }
        exec(&w.f.pool, "DELETE FROM labels").await;
        let restored = state(&w).await;
        w.check(restored == readable, format!("{val} removed: {restored:?}"));
    }
    w.finish().await;
}

#[tokio::test]
async fn t59_the_self_arm_keeps_the_gate() {
    for (public, layout, run) in RUNS {
        t59(&format!("t59-{run}"), public, layout).await;
    }
}
