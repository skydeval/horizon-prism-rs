//! `notServed`: the
//! statement against the read rule (T-N1), the cap probe (T-N5), standing
//! (T-N7) and the statement's own checks (T-N12); on the wire, per-reader
//! exclusions never mark (T-N2), never for others (T-N3), `overClaim`
//! (T-N4), labels (T-N6), the statement's runs (T-N8), every endpoint
//! (T-N9), stability (T-N10), served cases (T-N11) and the feature (T-N13).

use metrics_exporter_prometheus::PrometheusHandle;

use super::*;
use crate::private_tier::handlers::METRIC_NOT_SERVED;
use crate::test_support::hooks;

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
    let status = |s: &str| format!("UPDATE actors SET account_status = '{s}' WHERE did = '{ALICE}'");
    let label = |subject: &str, val: &str| format!("INSERT INTO labels (subject, val) VALUES ('{subject}', '{val}')");
    let unblock = "DELETE FROM blocks".to_string();
    let standing_bob = vec![(BOB, s.rb.clone(), vec!["standing"]), (CAROL, s.rc_under_b.clone(), vec!["placement"]), (BOB, s.a12.clone(), vec!["standing"])];
    // A block between Bob and Alice reaches the list thread too.
    let mut blocked_bob = standing_bob.clone();
    blocked_bob.extend([(BOB, s.lb.clone(), vec!["standing"]), (CAROL, s.lc.clone(), vec!["placement"])]);
    let step_hidden =vec![(BOB, s.rb.clone(), vec!["placement"]), (CAROL, s.rc_under_b.clone(), vec!["placement"])];
    let changes: Vec<(&str, String, String, Vec<Expected>)> = vec![
        ("Bob unfollows Alice", unfollow, follow_sql(BOB, ALICE), standing_bob.clone()),
        ("Alice blocks Bob", block_sql(ALICE, BOB), unblock.clone(), blocked_bob.clone()),
        ("Bob blocks Alice", block_sql(BOB, ALICE), unblock, blocked_bob.clone()),
        // A list block either way (private-blocks design, T-PB5).
        ("Alice list-blocks Bob", list_block_sql(ALICE, ALICE, "3kns1list1", &[BOB]), "DELETE FROM list_references".to_string(), blocked_bob.clone()),
        ("Bob list-blocks Alice", list_block_sql(BOB, CAROL, "3kns1list2", &[ALICE]), "DELETE FROM list_references".to_string(), blocked_bob),
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

// ---- On the wire (N2) ----

/// Every private post view of `uri` anywhere in `body`: an object with that
/// `uri` and a `record` (a quoted `#viewRecord` has `value` instead).
fn views_of(body: &Value, uri: &str) -> Vec<Value> {
    let mut out = Vec::new();
    match body {
        Value::Object(m) => {
            if m.get("uri").and_then(Value::as_str) == Some(uri) && m.contains_key("record") {
                out.push(body.clone());
            }
            for v in m.values() {
                out.extend(views_of(v, uri));
            }
        }
        Value::Array(a) => {
            for v in a {
                out.extend(views_of(v, uri));
            }
        }
        _ => {}
    }
    out
}

/// The hook key of `did` in this walk's database.
fn hook_key(w: &Walk, did: &str) -> String {
    format!("{}/{did}", w.f.pool.connect_options().get_database().unwrap_or_default())
}

/// `private_read_not_served_total{reason}`.
fn marked(handle: &PrometheusHandle, reason: &str) -> f64 {
    crate::test_support::series(&handle.render(), METRIC_NOT_SERVED, &[&format!("reason=\"{reason}\"")]).unwrap_or(0.0)
}

/// `reader`'s `getPosts` of `uris`: the raw body.
async fn body_of(w: &Walk, reader: &str, uris: &[&str]) -> String {
    posts_of(&w.base, reader, uris).await.0
}

/// T-N4, T-N9, T-N10 and T-N3's quote: Bob's reply to T with a forged
/// `mentionDids` is `["overClaim"]` on every endpoint that shows it to him
/// (getPosts, getPostThread as anchor, parent and reply, getAuthorFeed and
/// its reply context, getTimeline, getReplies, getBookmarks, searchPosts,
/// a notification's subject), and Dave misses it; with a dropped embed as
/// well, `droppedFields` and `notServed` both; quoted, never; the metric
/// counts it; two reads give the same bytes; a rewrite heals it.
#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn tn4_tn9_over_claim_on_every_endpoint() {
    let Some(mut w) = Walk::new("tn4-not-served", Layout::SamePds, Some("followers"), &[]).await else {
        return;
    };
    let recorder = metrics_exporter_prometheus::PrometheusBuilder::new().build_recorder();
    let handle = recorder.handle();
    let _local = metrics::set_default_local_recorder(&recorder);
    let (t, aud) = (w.root.clone(), w.aud.clone());
    let reply = json!({"root": {"uri": t, "cid": "bafyroot"}, "parent": {"uri": t, "cid": "bafyparent"}});
    let mut rec = w.f.new_form_record(BOB, "3kns4forged2", "forgedwords", None, None, &aud).await;
    rec["reply"] = reply.clone();
    rec["mentionDids"] = json!([CAROL]);
    let forged = w.f.put_post(BOB, "3kns4forged2", rec).await;
    let raw = br#"{"text":"t","embed":[1]}"#.iter().fold(vec![0xFF], |mut v, b| {
        v.push(*b);
        v
    });
    let mut rec = w.f.encoded_bytes(BOB, "3kns4both222", &raw, Some(&aud)).await;
    rec["reply"] = reply.clone();
    rec["mentionDids"] = json!([CAROL]);
    let both = w.f.put_post(BOB, "3kns4both222", rec).await;
    let child = w.reply(BOB, "3kns4child22", &forged).await;
    let bob_aud = w.f.audience(BOB, "3kns4bobaud2", Some("list"), &[CAROL, DAVE]).await;
    let quote = w.f.post_with(BOB, "3kns4quote22", &bob_aud, quote_of(&forged)).await;
    w.f.ingest(CAROL, LIKE, "3kns4like222", subject_record(LIKE, &forged, "2026-10-01T10:00:00Z")).await;
    w.f.ready_all().await;
    assert_eq!(post(&w.base, CREATE_BOOKMARK, &json!({"uri": forged, "cid": "bafyany"}), BOB).await.0, 200);
    let want = json!(["overClaim"]);

    // getPosts: the author's view, and Dave's miss.
    let before = marked(&handle, "over_claim");
    let (body, view) = one_post(&w.base, BOB, &forged).await;
    w.check(view["notServed"] == want, format!("getPosts: {view}"));
    w.check(marked(&handle, "over_claim") == before + 1.0, "the metric counts the view".to_string());
    let (again, _) = one_post(&w.base, BOB, &forged).await;
    w.check(again == body, format!("T-N10: two reads differ:\n{body}\n{again}"));
    w.check(one_post(&w.base, DAVE, &forged).await.1.is_null(), "Dave reads the over-claim".to_string());
    let (_, view) = one_post(&w.base, BOB, &both).await;
    w.check(view["droppedFields"] == json!(["embed"]) && view["notServed"] == want, format!("both markers: {view}"));
    // The child below it is served, unmarked; Dave reads it.
    let (_, view) = one_post(&w.base, BOB, &child).await;
    w.check(!view.is_null() && view.get("notServed").is_none(), format!("the child: {view}"));

    // Every endpoint (T-N9): each view of the post carries the same field.
    let reads: Vec<(&str, &str, String)> = vec![
        ("getPostThread, anchor", THREAD, format!("uri={forged}")),
        ("getPostThread, parent", THREAD, format!("uri={child}")),
        ("getPostThread, reply", THREAD, format!("uri={t}")),
        ("getAuthorFeed, item and reply context", FEED, format!("actor={BOB}&limit=50")),
        ("getTimeline", TIMELINE, "limit=50".to_string()),
        ("getReplies", GET_REPLIES, format!("uri={t}")),
        ("getBookmarks", GET_BOOKMARKS, String::new()),
        ("searchPosts", SEARCH, "q=forgedwords".to_string()),
        ("listNotifications, subject", LIST_NOTIFICATIONS, String::new()),
    ];
    for (what, method, query) in &reads {
        // A search page can be empty with a cursor: every page of it.
        let page = if *method == SEARCH { Value::Array(pages(&w.base, method, query, BOB, 20).await) } else { json_of(&w.base, method, query, BOB).await.1 };
        let views = views_of(&page, &forged);
        w.check(
            !views.is_empty() && views.iter().all(|v| v["notServed"] == want),
            format!("{what}: {} views of {forged}: {page}", views.len()),
        );
    }
    let (_, feed, _) = json_of(&w.base, FEED, &format!("actor={BOB}&limit=50"), BOB).await;
    let item = feed["feed"].as_array().into_iter().flatten().find(|i| i["uri"] == child.as_str()).cloned().unwrap_or_default();
    w.check(item["reply"]["parent"]["notServed"] == want, format!("reply context: {item}"));

    // Quoted, never (T-N3): Bob's own quote of it, and Dave's.
    for reader in [BOB, DAVE] {
        let (body, view) = one_post(&w.base, reader, &quote).await;
        w.check(!view.is_null() && !body.contains("notServed"), format!("{reader}'s quote: {body}"));
    }
    let (_, view) = one_post(&w.base, BOB, &quote).await;
    w.check(view["embed"]["record"]["uri"] == forged.as_str(), format!("Bob reads the quoted post: {view}"));

    // The rewrite heals it (T-D3): no marker, Dave reads it.
    let (_, view) = one_post(&w.base, BOB, &forged).await;
    let record = &view["record"];
    let mut rewrite = w.f.new_form_record(BOB, "3kns4forged2", record["text"].as_str().expect("text"), None, None, &aud).await;
    rewrite["reply"] = reply;
    rewrite["createdAt"] = record["createdAt"].clone();
    w.f.apply_op(BOB, Action::Update, NSID_POST_PRIVATE, "3kns4forged2", REV2, Some(rewrite)).await;
    let (_, view) = one_post(&w.base, BOB, &forged).await;
    w.check(!view.is_null() && view.get("notServed").is_none(), format!("healed: {view}"));
    w.check(!one_post(&w.base, DAVE, &forged).await.1.is_null(), "Dave reads the healed post".to_string());
    w.finish().await;
}

/// T-N2, T-N3, T-N6, T-N7, T-N8, T-N11, T-N13 on the wire, in Alice's
/// `followers` thread T and her `list` thread L.
#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn tn2_tn3_tn6_tn7_tn8_the_marker_is_the_authors_alone() {
    let Some(mut w) = Walk::new("tn2-not-served", Layout::SamePds, Some("followers"), &[]).await else {
        return;
    };
    let recorder = metrics_exporter_prometheus::PrometheusBuilder::new().build_recorder();
    let handle = recorder.handle();
    let _local = metrics::set_default_local_recorder(&recorder);
    let t = w.root.clone();
    let rb = w.reply(BOB, "3kns2bob2222", &t).await;
    let rc = w.reply(CAROL, "3kns2carol22", &t).await;
    let rb2 = w.reply(BOB, "3kns2bobund2", &rc).await;
    let list = w.f.audience(ALICE, "3kns2list222", Some("list"), &[BOB, CAROL, DAVE]).await;
    let lroot = w.f.private_post(ALICE, "3kns2lroot22", &list, None).await;
    let lb = w.f.private_post(BOB, "3kns2lbob222", &list, Some((&lroot, &lroot))).await;
    let narrower = w.f.audience(ALICE, "3kns2narrow2", Some("list"), &[DAVE]).await;
    let a12 = w.f.private_post(BOB, "3kns2a122222", &narrower, Some((&t, &t))).await;
    let empty = w.f.audience(BOB, "3kns2empty22", Some("list"), &[]).await;
    let bob_empty = w.f.private_post(BOB, "3kns2bobemp2", &empty, None).await;
    w.f.ready_all().await;
    let own = [rb.as_str(), rb2.as_str(), lb.as_str()];
    let baseline = body_of(&w, BOB, &own).await;
    w.check(!baseline.contains("notServed") && posts_of(&w.base, BOB, &own).await.1.len() == 3, format!("baseline: {baseline}"));
    for uri in own {
        w.expect("baseline", DAVE, uri, true).await;
    }

    // T-N11: served cases carry nothing.
    let body = body_of(&w, BOB, &[&a12, &bob_empty]).await;
    w.check(!body.contains("notServed") && posts_of(&w.base, BOB, &[&a12, &bob_empty]).await.1.len() == 2, format!("A-12 and an empty list: {body}"));

    // T-N2: per-reader exclusions make Dave miss and change nothing of
    // Bob's views.
    let stale = format!("UPDATE sync_state SET head_checked_at = 'epoch' WHERE did = '{DAVE}'");
    let fresh = format!("UPDATE sync_state SET head_checked_at = now() WHERE did = '{DAVE}'");
    for (case, apply, undo, missed) in [
        ("Dave blocks Bob", block_sql(DAVE, BOB), "DELETE FROM blocks".to_string(), &rb),
        ("Bob blocks Dave", block_sql(BOB, DAVE), "DELETE FROM blocks".to_string(), &rb),
        ("Dave blocks Alice", block_sql(DAVE, ALICE), "DELETE FROM blocks".to_string(), &rb),
        ("Dave blocks a chain author", block_sql(DAVE, CAROL), "DELETE FROM blocks".to_string(), &rb2),
        ("Dave's head stale", stale, fresh, &rb),
    ] {
        exec(&w.f.pool, &apply).await;
        w.check(posts_of(&w.base, DAVE, &[missed]).await.1.is_empty(), format!("{case}: Dave reads {missed}"));
        let body = body_of(&w, BOB, &own).await;
        w.check(body == baseline, format!("{case}: Bob's views changed:\n{baseline}\n{body}"));
        exec(&w.f.pool, &undo).await;
    }
    w.edit_audience("3kns2list222", "list", &[BOB, CAROL], REV2).await;
    w.check(posts_of(&w.base, DAVE, &[&lb]).await.1.is_empty(), "Dave outside L reads Bob's reply".to_string());
    let body = body_of(&w, BOB, &own).await;
    w.check(body == baseline, format!("Dave outside L: Bob's views changed:\n{baseline}\n{body}"));
    w.edit_audience("3kns2list222", "list", &[BOB, CAROL, DAVE], REV3).await;

    // T-N3 and T-N8: another reader's bodies are the ones built with the
    // marker code off, and the statement never runs for them.
    let dave_reads: Vec<(&str, String)> = vec![
        (POSTS, format!("uris={t}&uris={rb}&uris={rc}&uris={rb2}&uris={lb}&uris={a12}")),
        (THREAD, format!("uri={t}")),
        (FEED, format!("actor={BOB}&limit=50")),
    ];
    let runs = hooks::arm(hooks::NOT_SERVED_RUN, &hook_key(&w, DAVE));
    let mut on = Vec::new();
    for (method, query) in &dave_reads {
        on.push(json_of(&w.base, method, query, DAVE).await.0);
    }
    w.check(runs.hits() == 0, format!("the statement ran {} times for Dave", runs.hits()));
    drop(runs);
    let off = hooks::arm(hooks::NOT_SERVED_OFF, &hook_key(&w, DAVE));
    for ((method, query), on) in dave_reads.iter().zip(&on) {
        let (body_off, _, _) = json_of(&w.base, method, query, DAVE).await;
        w.check(&body_off == on, format!("{method}: Dave's body differs with the marker code off"));
        w.check(!on.contains("notServed"), format!("{method}: Dave sees a marker"));
    }
    drop(off);
    // The author: one statement per response that shows an own post, none
    // for a response that shows none.
    let runs = hooks::arm(hooks::NOT_SERVED_RUN, &hook_key(&w, BOB));
    json_of(&w.base, FEED, &format!("actor={BOB}&limit=50"), BOB).await;
    json_of(&w.base, THREAD, &format!("uri={t}"), BOB).await;
    let n = runs.hits();
    posts_of(&w.base, BOB, &[&t, &rc]).await;
    w.check(n == 2 && runs.hits() == 2, format!("Bob's runs: {n}, then {}", runs.hits()));
    drop(runs);

    // T-N7: standing, on the wire. Bob removed from L, then restored.
    w.edit_audience("3kns2list222", "list", &[CAROL, DAVE], "3kaaaaaaaaad2").await;
    let (_, view) = one_post(&w.base, BOB, &lb).await;
    w.check(view["notServed"] == json!(["standing"]), format!("Bob removed from L: {view}"));
    w.check(posts_of(&w.base, DAVE, &[&lb]).await.1.is_empty(), "Dave reads Bob's reply after the removal".to_string());
    w.edit_audience("3kns2list222", "list", &[BOB, CAROL, DAVE], "3kaaaaaaaaae2").await;
    let (_, view) = one_post(&w.base, BOB, &lb).await;
    w.check(!view.is_null() && view.get("notServed").is_none(), format!("Bob restored: {view}"));
    // A block either way, direct or by a list: the same bytes, `standing`;
    // nothing names a block.
    let before = marked(&handle, "standing");
    let mut bodies = Vec::new();
    for (apply, undo) in [
        (block_sql(ALICE, BOB), "DELETE FROM blocks"),
        (block_sql(BOB, ALICE), "DELETE FROM blocks"),
        (list_block_sql(ALICE, ALICE, "3kns7list1", &[BOB]), "DELETE FROM list_references"),
        (list_block_sql(BOB, CAROL, "3kns7list2", &[ALICE]), "DELETE FROM list_references"),
    ] {
        exec(&w.f.pool, &apply).await;
        bodies.push(body_of(&w, BOB, &[&rb]).await);
        exec(&w.f.pool, undo).await;
    }
    let view: Value = serde_json::from_str(&bodies[0]).expect("json");
    w.check(view["posts"][0]["notServed"] == json!(["standing"]), format!("a block: {}", bodies[0]));
    w.check(bodies.iter().all(|b| *b == bodies[0]), format!("the four blocks differ: {bodies:?}"));
    w.check(marked(&handle, "standing") == before + 4.0, "the metric counts standing".to_string());
    // Unfollowing: Bob's view is byte-identical to the blocks' (nothing
    // tells a block from a missing follow), his reply below Carol's is
    // `standing` too; Carol, reading Bob's unserved reply, misses it and
    // sees no marker.
    exec(&w.f.pool, &format!("DELETE FROM follows WHERE author_did = '{BOB}' AND subject_did = '{ALICE}'")).await;
    let unfollowed = body_of(&w, BOB, &[&rb]).await;
    w.check(unfollowed == bodies[0], format!("a block and an unfollow differ:\n{}\n{unfollowed}", bodies[0]));
    let (_, views) = posts_of(&w.base, BOB, &[&rb, &rb2]).await;
    w.check(views.len() == 2 && views.iter().all(|v| v["notServed"] == json!(["standing"])), format!("Bob unfollows: {views:?}"));
    let carols = body_of(&w, CAROL, &[&rb, &rc]).await;
    w.check(!carols.contains("notServed") && !carols.contains(&rb), format!("Carol: {carols}"));
    exec(&w.f.pool, &follow_sql(BOB, ALICE)).await;

    // T-N6: labels.
    for (case, subject, val, want) in [
        ("!hide on the root", t.as_str(), "!hide", Some(json!(["placement"]))),
        ("!takedown on O", ALICE, "!takedown", Some(json!(["placement"]))),
        ("a non-hiding label on the root", t.as_str(), "!warn", None),
    ] {
        w.label(subject, val).await;
        let (_, view) = one_post(&w.base, BOB, &rb).await;
        w.check(!view.is_null() && view.get("notServed").cloned() == want, format!("{case}: {view}"));
        exec(&w.f.pool, "DELETE FROM labels").await;
    }
    w.label(&rb, "!hide").await;
    w.check(one_post(&w.base, BOB, &rb).await.1.is_null(), "!hide on Bob's own reply: he reads it".to_string());
    exec(&w.f.pool, "DELETE FROM labels").await;

    // T-N11: a reply the root's threadgate hides is not marked.
    w.f.ingest(ALICE, THREADGATE, "3kroot222222", json!({"$type": THREADGATE, "post": t, "allow": [], "hiddenReplies": [rb]})).await;
    let (_, view) = one_post(&w.base, BOB, &rb).await;
    w.check(!view.is_null() && view.get("notServed").is_none(), format!("hidden by the threadgate: {view}"));

    // T-N13.
    w.check(crate::private_tier::FEATURES.contains(&"private.notServed"), "the feature is listed".to_string());
    w.finish().await;
}
