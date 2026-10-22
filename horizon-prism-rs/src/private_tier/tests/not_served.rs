//! `notServed` (docs/internal/design/not-served-reason.md §11): the
//! statement against the read rule (T-N1), the cap probe (T-N5), standing
//! (T-N7) and the statement's own checks (T-N12).

use super::*;

/// The reasons the statement gives `author`'s `uri`, and its columns; `None`
/// when it returns no row for it.
async fn reasons_of(pool: &PgPool, author: &str, uri: &str, cap: u32) -> Option<(Vec<&'static str>, schema::NotServedRow)> {
    let rows = schema::not_served_rows(pool, author, &[uri.to_string()], cap).await.expect("not_served");
    rows.get(uri).map(|row| (schema::ThreadFault::of(*row).names(), *row))
}

impl Walk {
    /// T-N1, T-N12: `author`'s `uri` carries exactly `want`; when `clean`
    /// (Dave, M, is inside G with no block with the author, O or a chain
    /// author), the reasons are non-empty exactly when M's read misses (the
    /// oracle and the prefilter, which agree), and `served` is fact 8's
    /// verdict for M; `served` implies `placed` and `standing_ok`; the
    /// per-key form gives the same columns.
    async fn expect_not_served(&mut self, case: &str, author: &str, uri: &str, want: &[&str], clean: bool) {
        let Some((got, row)) = reasons_of(&self.f.pool, author, uri, self.cap).await else {
            self.failures.push(format!("{case}: no row for {author}'s {uri}"));
            return;
        };
        self.check(got == want, format!("{case}: {author}'s {uri}: expected {want:?}, got {got:?} ({row:?})"));
        self.check(!row.served || (row.placed && row.standing_ok), format!("{case}: served without placed and standing_ok: {row:?}"));
        schema::PER_KEY.with(|p| p.set(true));
        let per_key = reasons_of(&self.f.pool, author, uri, self.cap).await.map(|r| r.1);
        schema::PER_KEY.with(|p| p.set(false));
        self.check(per_key == Some(row), format!("{case}: per-key {per_key:?}, shared {row:?}"));
        if clean {
            let oracle = oracle_admits_capped(&self.f.pool, DAVE, uri, self.cap).await;
            let prefilter = !schema::prefilter_uris(&self.f.pool, DAVE, &[uri.to_string()], self.cap).await.expect("prefilter").is_empty();
            self.check(
                oracle == prefilter && got.is_empty() == oracle,
                format!("{case}: {uri}: reasons {got:?}, M's oracle {oracle}, prefilter {prefilter}"),
            );
            let fact8 = schema::fact8(&self.f.pool, DAVE, uri, self.cap).await.expect("fact 8");
            self.check(fact8.verdict == row.served, format!("{case}: {uri}: fact 8 for M {fact8:?}, served {}", row.served));
        }
    }
}

/// The T-N1 fixture set (not-served-reason §11): Alice's `followers` thread
/// T and a `list` thread L, with Bob and Carol writing and Dave as M.
struct Set {
    t: String,
    rb: String,
    rc_under_b: String,
    q_owner: String,
    lb: String,
    lc: String,
    branch_reply: String,
    a12: String,
    /// `(case, author, uri, reasons)` that no phase changes.
    fixed: Vec<(&'static str, &'static str, String, Vec<&'static str>)>,
    bob_empty: String,
}

async fn set(w: &Walk) -> Set {
    let (t, aud) = (w.root.clone(), w.aud.clone());
    let rb = w.reply(BOB, "3knsbob22222", &t).await;
    let rc_under_b = w.reply(CAROL, "3knscarol222", &rb).await;
    let q_owner = w.reply(ALICE, "3knsowner222", &rb).await;
    let list = w.f.audience(ALICE, "3knslist2222", Some("list"), &[BOB, CAROL, DAVE]).await;
    let lroot = w.f.private_post(ALICE, "3knslroot222", &list, None).await;
    let lb = w.f.private_post(BOB, "3knslbob2222", &list, Some((&lroot, &lroot))).await;
    let lc = w.f.private_post(CAROL, "3knslcarol22", &list, Some((&lroot, &lb))).await;
    let x = format!("at://{DAVE}/{PUBLIC_POST}/3knspublic22");
    let branch_root = w.f.private_post(ALICE, "3knsbranch22", &aud, Some((&x, &x))).await;
    let branch_reply = w.f.private_post(CAROL, "3knsbrreply2", &aud, Some((&x, &branch_root))).await;
    let narrower = w.f.audience(ALICE, "3knsnarrow22", Some("list"), &[DAVE]).await;
    let a12 = w.f.private_post(BOB, "3knsa1222222", &narrower, Some((&t, &t))).await;
    let bob_aud = w.f.audience(BOB, "3knsbobaud22", Some("list"), &[CAROL, DAVE]).await;
    let empty = w.f.audience(BOB, "3knsempty222", Some("list"), &[]).await;
    let bob_empty = w.f.private_post(BOB, "3knsbobempt2", &empty, None).await;
    let ghost_aud = format!("at://{ALICE}/{NSID_AUDIENCE}/3knsghostau2");
    let ghost_root = w.f.private_post(ALICE, "3knsghostrt2", &ghost_aud, None).await;
    let never = format!("at://{ALICE}/{NSID_POST_PRIVATE}/3knsnever222");
    let cycle_a = format!("at://{BOB}/{NSID_POST_PRIVATE}/3knscyclea22");
    let cycle_b = format!("at://{CAROL}/{NSID_POST_PRIVATE}/3knscycleb22");
    w.reply(BOB, "3knscyclea22", &cycle_b).await;
    w.reply(CAROL, "3knscycleb22", &cycle_a).await;
    let fixed = vec![
        ("the root", ALICE, t.clone(), vec![]),
        ("a branch root", ALICE, branch_root.clone(), vec![]),
        ("the author's own list", BOB, w.f.private_post(BOB, "3knsbobtop22", &bob_aud, None).await, vec![]),
        ("a top-level post under another's audience", BOB, w.f.private_post(BOB, "3knscrossto2", &aud, None).await, vec!["placement"]),
        ("a reply under its author's own audience", BOB, w.f.private_post(BOB, "3knsownaud22", &bob_aud, Some((&t, &t))).await, vec!["placement"]),
        ("a missing root", BOB, w.f.private_post(BOB, "3knsnoroot22", &aud, Some((&never, &never))).await, vec!["placement"]),
        ("a root that is a reply", BOB, w.f.private_post(BOB, "3knsrootrep2", &aud, Some((&rb, &rb))).await, vec!["placement"]),
        ("kind none: a public parent, a private root", BOB, w.f.private_post(BOB, "3knskindnon2", &aud, Some((&t, &x))).await, vec!["placement"]),
        ("a dangling parent", CAROL, w.reply(CAROL, "3knsdangle22", &format!("at://{NOBODY}/{NSID_POST_PRIVATE}/x")).await, vec!["placement"]),
        ("a cycle", BOB, cycle_a, vec!["placement"]),
        ("into a cycle", CAROL, w.reply(CAROL, "3knsintocyc2", &cycle_b).await, vec!["placement"]),
        ("G's row missing, the owner's root", ALICE, ghost_root.clone(), vec!["placement"]),
        ("G's row missing, a reply", BOB, w.f.private_post(BOB, "3knsghostrp2", &ghost_aud, Some((&ghost_root, &ghost_root))).await, vec!["placement"]),
    ];
    Set { t, rb, rc_under_b, q_owner, lb, lc, branch_reply, a12, fixed, bob_empty }
}

/// `(author, uri, reasons)`.
type Expected = (&'static str, String, Vec<&'static str>);

/// The posts whose reasons change with the state, against the baseline.
fn moving(s: &Set) -> Vec<(&'static str, String)> {
    vec![
        (BOB, s.rb.clone()),
        (CAROL, s.rc_under_b.clone()),
        (ALICE, s.q_owner.clone()),
        (BOB, s.lb.clone()),
        (CAROL, s.lc.clone()),
        (CAROL, s.branch_reply.clone()),
        (BOB, s.a12.clone()),
    ]
}

/// T-N1 (with T-N6's, T-N7's and T-N11's statement-level cases, and
/// T-N12's per-key and fact-8 checks on each): every case's reasons, and
/// their agreement with M's read, in a baseline and under each change of
/// state; each change is undone before the next.
#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn tn1_the_reasons_agree_with_the_read_rule() {
    let Some(mut w) = Walk::new("tn1-not-served", Layout::SamePds, Some("followers"), &[]).await else {
        return;
    };
    let s = set(&w).await;
    w.f.ready_all().await;
    let all_served = |s: &Set| moving(s).into_iter().map(|(a, u)| (a, u, Vec::<&str>::new())).collect::<Vec<_>>();
    // Baseline.
    for (case, author, uri, want) in s.fixed.clone() {
        w.expect_not_served(case, author, &uri, &want, true).await;
    }
    for (author, uri, want) in all_served(&s) {
        w.expect_not_served("baseline", author, &uri, &want, true).await;
    }
    // T-N11: an empty audience is not a fault; M is not inside it.
    w.expect_not_served("an empty list", BOB, &s.bob_empty, &[], false).await;

    // Each change: its SQL, its undo, and the reasons it gives (the rest
    // stay served).
    let unfollow = format!("DELETE FROM follows WHERE author_did = '{BOB}' AND subject_did = '{ALICE}'");
    let private_block = |from: &str, to: &str| {
        format!(
            "INSERT INTO private_blocks (uri, author_did, subject_did, rev, created_at) \
             VALUES ('at://{from}/tools.kryphocron.graph.block/1', '{from}', '{to}', '3k', now())"
        )
    };
    let status = |s: &str| format!("UPDATE actors SET account_status = '{s}' WHERE did = '{ALICE}'");
    let label = |subject: &str, val: &str| format!("INSERT INTO labels (subject, val) VALUES ('{subject}', '{val}')");
    let (unblock, unblock_private) = ("DELETE FROM blocks".to_string(), "DELETE FROM private_blocks".to_string());
    let standing_bob = vec![(BOB, s.rb.clone(), vec!["standing"]), (CAROL, s.rc_under_b.clone(), vec!["placement"]), (BOB, s.a12.clone(), vec!["standing"])];
    // A block between Bob and Alice reaches the list thread too.
    let mut blocked_bob = standing_bob.clone();
    blocked_bob.extend([(BOB, s.lb.clone(), vec!["standing"]), (CAROL, s.lc.clone(), vec!["placement"])]);
    let step_hidden =vec![(BOB, s.rb.clone(), vec!["placement"]), (CAROL, s.rc_under_b.clone(), vec!["placement"])];
    let changes: Vec<(&str, String, String, Vec<Expected>)> = vec![
        ("Bob unfollows Alice", unfollow, follow_sql(BOB, ALICE), standing_bob.clone()),
        ("Alice blocks Bob", block_sql(ALICE, BOB), unblock.clone(), blocked_bob.clone()),
        ("Bob blocks Alice", block_sql(BOB, ALICE), unblock, blocked_bob.clone()),
        ("Alice blocks Bob privately", private_block(ALICE, BOB), unblock_private.clone(), blocked_bob.clone()),
        ("Bob blocks Alice privately", private_block(BOB, ALICE), unblock_private, blocked_bob),
        (
            "Alice deactivated",
            status("deactivated"),
            status("active"),
            vec![
                (BOB, s.rb.clone(), vec!["placement"]),
                (CAROL, s.rc_under_b.clone(), vec!["placement"]),
                (ALICE, s.q_owner.clone(), vec!["placement"]),
                (BOB, s.lb.clone(), vec!["placement"]),
                (CAROL, s.lc.clone(), vec!["placement"]),
                (CAROL, s.branch_reply.clone(), vec!["placement"]),
                (BOB, s.a12.clone(), vec!["placement"]),
            ],
        ),
        (
            "Alice desynchronized",
            status("desynchronized"),
            status("active"),
            vec![
                (BOB, s.rb.clone(), vec!["standing"]),
                (CAROL, s.rc_under_b.clone(), vec!["standing"]),
                (BOB, s.lb.clone(), vec!["standing"]),
                (CAROL, s.lc.clone(), vec!["standing"]),
                (CAROL, s.branch_reply.clone(), vec!["standing"]),
                (BOB, s.a12.clone(), vec!["standing"]),
            ],
        ),
        (
            "!hide on the root",
            label(&s.t, "!hide"),
            "DELETE FROM labels".to_string(),
            vec![
                (BOB, s.rb.clone(), vec!["placement"]),
                (CAROL, s.rc_under_b.clone(), vec!["placement"]),
                (ALICE, s.q_owner.clone(), vec!["placement"]),
                (BOB, s.a12.clone(), vec!["placement"]),
            ],
        ),
        // The step itself is hidden from its author too (no view in the
        // pipeline); the statement, asked directly, calls it `placement`.
        ("!hide on a step", label(&s.rb, "!hide"), "DELETE FROM labels".to_string(), step_hidden.clone()),
        ("!takedown on a step", label(&s.rb, "!takedown"), "DELETE FROM labels".to_string(), step_hidden.clone()),
        (
            "!takedown on O",
            label(ALICE, "!takedown"),
            "DELETE FROM labels".to_string(),
            vec![
                (BOB, s.rb.clone(), vec!["placement"]),
                (CAROL, s.rc_under_b.clone(), vec!["placement"]),
                (ALICE, s.q_owner.clone(), vec!["placement"]),
                (BOB, s.lb.clone(), vec!["placement"]),
                (CAROL, s.lc.clone(), vec!["placement"]),
                (CAROL, s.branch_reply.clone(), vec!["placement"]),
                (BOB, s.a12.clone(), vec!["placement"]),
            ],
        ),
        ("a non-hiding label on the root", label(&s.t, "!warn"), "DELETE FROM labels".to_string(), vec![]),
    ];
    for (case, apply, undo, changed) in changes {
        exec(&w.f.pool, &apply).await;
        for (author, uri, served) in all_served(&s) {
            let want = changed.iter().find(|(_, u, _)| *u == uri).map_or(served, |(_, _, r)| r.clone());
            w.expect_not_served(case, author, &uri, &want, true).await;
        }
        exec(&w.f.pool, &undo).await;
        for (author, uri, want) in all_served(&s) {
            w.expect_not_served(&format!("{case}, undone"), author, &uri, &want, true).await;
        }
    }

    // T-N7: Bob removed from the list thread's audience, and restored.
    w.edit_audience("3knslist2222", "list", &[CAROL, DAVE], REV2).await;
    w.expect_not_served("Bob removed from the list", BOB, &s.lb, &["standing"], true).await;
    w.expect_not_served("Bob removed from the list, below him", CAROL, &s.lc, &["placement"], true).await;
    w.edit_audience("3knslist2222", "list", &[BOB, CAROL, DAVE], REV3).await;
    w.expect_not_served("Bob restored", BOB, &s.lb, &[], true).await;
    w.expect_not_served("Bob restored, below him", CAROL, &s.lc, &[], true).await;

    // W = O is never `standing`, whatever blocks: the owner's thread reply.
    for sql in [block_sql(ALICE, BOB), block_sql(BOB, ALICE)] {
        exec(&w.f.pool, &sql).await;
        w.expect_not_served("the owner's reply under a block", ALICE, &s.q_owner, &[], true).await;
        exec(&w.f.pool, "DELETE FROM blocks").await;
    }

    // The statement reads only the author's rows: another's URI has no row.
    let asked = [s.t.clone(), s.rc_under_b.clone(), s.rb.clone()];
    let others = schema::not_served_rows(&w.f.pool, BOB, &asked, CAP).await.expect("not_served");
    w.check(others.keys().eq([&s.rb]), format!("rows for others' posts: {others:?}"));
    // `not_served` keeps only the posts that are not served.
    let faults = schema::not_served(&w.f.pool, BOB, &asked, CAP).await.expect("not_served");
    w.check(faults.is_empty(), format!("faults while served: {faults:?}"));
    exec(&w.f.pool, &format!("DELETE FROM follows WHERE author_did = '{BOB}'")).await;
    let faults = schema::not_served(&w.f.pool, BOB, &asked, CAP).await.expect("not_served");
    let want = schema::ThreadFault { standing: true, ..Default::default() };
    w.check(faults.len() == 1 && faults.get(&s.rb) == Some(&want), format!("faults after an unfollow: {faults:?}"));
    w.finish().await;
}

/// T-N5 (§4.3): at the cap a thread chain of exactly `max_chain` rows to its
/// stop is served and one row longer is `chainCap`; a branch one row past
/// the cap is `placement` and `chainCap`; a cycle is `placement`. The
/// boundary moves with the cap. Sliced at K 1 and 3, the statement gives
/// the same rows as unsliced.
#[tokio::test]
async fn tn5_chain_cap() {
    let Some(mut w) = Walk::new("tn5-not-served", Layout::SamePds, Some("followers"), &[]).await else {
        return;
    };
    let (t, aud) = (w.root.clone(), w.aud.clone());
    let author = |level: usize| if level % 2 == 1 { BOB } else { CAROL };
    let mut chain = Vec::new();
    let mut parent = t.clone();
    for level in 1..=18 {
        parent = w.reply(author(level), &format!("3kns5l{level:03}22"), &parent).await;
        chain.push(parent.clone());
    }
    let x = format!("at://{DAVE}/{PUBLIC_POST}/3kns5public2");
    let branch_root = w.f.private_post(ALICE, "3kns5branch2", &aud, Some((&x, &x))).await;
    let mut branch = Vec::new();
    let mut parent = branch_root.clone();
    for depth in 1..=17 {
        parent = w.f.private_post(author(depth), &format!("3kns5b{depth:03}22"), &aud, Some((&x, &parent))).await;
        branch.push(parent.clone());
    }
    w.f.ready_all().await;
    w.expect_not_served("level 16", CAROL, &chain[15], &[], true).await;
    w.expect_not_served("level 17", BOB, &chain[16], &["chainCap"], true).await;
    w.expect_not_served("level 18", CAROL, &chain[17], &["chainCap"], true).await;
    w.expect_not_served("branch row 16", CAROL, &branch[15], &[], true).await;
    w.expect_not_served("branch row 17", BOB, &branch[16], &["placement", "chainCap"], true).await;
    // A step fault below the cap is masked by `chainCap` (§4.2).
    w.label(&chain[2], "!hide").await;
    w.expect_not_served("level 17, a hidden step", BOB, &chain[16], &["chainCap"], true).await;
    w.expect_not_served("level 16, a hidden step", CAROL, &chain[15], &["placement"], true).await;
    exec(&w.f.pool, "DELETE FROM labels").await;
    // The boundary moves with the cap.
    w.set_cap(17).await;
    w.expect_not_served("cap 17, level 17", BOB, &chain[16], &[], true).await;
    w.expect_not_served("cap 17, level 18", CAROL, &chain[17], &["chainCap"], true).await;
    w.expect_not_served("cap 17, branch row 17", BOB, &branch[16], &[], true).await;
    w.set_cap(CAP).await;
    // Sliced: every row of both authors' posts, at K 1, 3 and unsliced.
    for who in [BOB, CAROL] {
        let uris: Vec<String> = chain.iter().chain(&branch).cloned().collect();
        let whole = schema::not_served_rows_sliced(&w.f.pool, who, &uris, CAP, usize::MAX).await.expect("whole");
        for k in [1, 3] {
            let sliced = schema::not_served_rows_sliced(&w.f.pool, who, &uris, CAP, k).await.expect("sliced");
            w.check(sliced == whole, format!("{who} at K {k}: {sliced:?} against {whole:?}"));
        }
        w.check(whole.len() == uris.iter().filter(|u| u.contains(who)).count(), format!("{who}: {} rows", whole.len()));
    }
    w.finish().await;
}
