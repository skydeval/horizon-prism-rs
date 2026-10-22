//! S12's one builder (docs/internal/design/private-threads.md §3.1, §3.2,
//! T-52) and the pre-release count (§8, T-55): every statement prepares; the
//! batches equal row-at-a-time evaluation, ties included; the order of
//! evaluation; the reorder's equivalence; one walk per key; the guarded
//! V–O term.

use super::*;

/// Fact 8's final over the shared walk (`$2` the post): the statement fact
/// 8's one-row form stands in for.
fn shared_one_row(cap: u32) -> String {
    schema::walked(&format!("SELECT {} FROM private_posts p WHERE p.uri = $2", schema::COLS), "", &schema::fact8_final(), cap)
}

/// Every statement of §3.2 for the cap `cap`, by name.
fn statements(cap: u32) -> Vec<(&'static str, String)> {
    vec![
        ("fact 8", schema::fact8_statement(cap)),
        ("fact 8, shared form", shared_one_row(cap)),
        ("prefilter_uris", schema::prefilter_uris_statement(cap)),
        ("getReplies page", schema::reply_page_statement(cap)),
        ("search", schema::search_statement(cap)),
        ("expansion", schema::reply_candidates_statement(cap)),
        ("author feed", schema::author_statement(schema::AuthorFilter::Replies, cap)),
        ("author threads", schema::author_statement(schema::AuthorFilter::OwnThreads, cap)),
        ("author media", schema::author_statement(schema::AuthorFilter::Media, cap)),
        ("author video", schema::author_statement(schema::AuthorFilter::Video, cap)),
        ("timeline", schema::timeline_statement(cap)),
        ("getQuotes", schema::quote_statement(cap)),
        ("replyCount", schema::reply_counts_statement(cap)),
        ("quoteCount slice", schema::quote_counts_slice_statement(cap)),
        ("getUnreadCount slice", schema::unread_slice_statement(cap)),
        ("quoteCount bounded", schema::quote_counts_bounded_statement(cap, schema::climb_keys(cap))),
        ("getUnreadCount bounded", schema::unread_bounded_statement(cap, schema::climb_keys(cap))),
        ("notServed", schema::not_served_statement(cap)),
    ]
}

/// T-52: every statement `PREPARE`s against the migrated database, which
/// checks every column and alias it names (§3.1's table), and so do the
/// pre-release counts and the slices' plain statements. The climb's window
/// over the working table and its `UNION` are where an unsupported
/// construct would fail (shared-walk §9): they prepare.
#[tokio::test]
async fn t52_every_statement_prepares() {
    let Some(db) = fresh_db("t52-prepare").await else {
        return;
    };
    let mut conn = db.pool.acquire().await.expect("conn");
    let mut all = statements(CAP);
    all.extend(statements(100));
    all.push(("quoteCount refs", schema::quote_refs_statement()));
    all.push(("getUnreadCount keys", schema::unread_keys_statement()));
    all.push(("getUnreadCount public term", schema::unread_public_statement()));
    all.push(("pre-S12 count", schema::pre_s12_count_sql()));
    all.push(("pre-S12 owner audiences", schema::pre_s12_owner_audience_sql()));
    for (i, (name, sql)) in all.iter().enumerate() {
        let text = sql.trim_end_matches(';');
        sqlx::raw_sql(&format!("PREPARE s12_t52_{i} AS {text}"))
            .execute(&mut *conn)
            .await
            .unwrap_or_else(|e| panic!("{name} does not prepare: {e}"));
        sqlx::raw_sql(&format!("DEALLOCATE s12_t52_{i}")).execute(&mut *conn).await.expect("deallocate");
    }
    // Both feed arms meet one anchor join through `c`, and fact 8's final
    // reads the same `s1` and `pv`.
    for (name, sql) in statements(CAP) {
        assert_eq!(sql.matches("(SELECT m FROM akm) -> CASE").count(), 1, "{name}");
    }
    drop(conn);
    db.drop_db().await;
}

/// A mixed fixture for the batch and evaluation checks: thread replies by
/// standing and non-standing writers, owner replies, branch replies, posts
/// under the writer's own audience, a moderated reply and one by a writer
/// with a hiding status, several authors and parents, all under one
/// `createdAt` so that the record keys decide the order.
async fn mixed(w: &Walk) -> Vec<String> {
    let (t, aud) = (w.root.clone(), w.aud.clone());
    let x = format!("at://{DAVE}/{PUBLIC_POST}/3kt52public2");
    let bob_aud = w.f.audience(BOB, "3kt52bobaud2", Some("list"), &[DAVE]).await;
    let mut uris = vec![t.clone()];
    let branch = w.f.private_post(ALICE, "3kt52branch2", &aud, Some((&x, &x))).await;
    uris.push(branch.clone());
    let mut parent = t.clone();
    for i in 0..12 {
        let (author, audience, reply) = match i % 6 {
            0 => (BOB, aud.clone(), (t.clone(), parent.clone())),
            1 => (CAROL, aud.clone(), (t.clone(), parent.clone())),
            2 => (ALICE, aud.clone(), (t.clone(), parent.clone())),
            3 => (BOB, bob_aud.clone(), (t.clone(), t.clone())),
            4 => (CAROL, aud.clone(), (x.clone(), branch.clone())),
            _ => (DAVE, aud.clone(), (t.clone(), parent.clone())),
        };
        let uri = w.f.private_post(author, &format!("3kt52m{i:02}2222"), &audience, Some((&reply.0, &reply.1))).await;
        if i % 3 == 0 {
            parent = uri.clone();
        }
        uris.push(uri);
    }
    w.label(&uris[5], "!hide").await;
    exec(&w.f.pool, &format!("DELETE FROM follows WHERE author_did = '{CAROL}' AND subject_did = '{ALICE}'")).await;
    uris
}

/// T-52 (batch equivalence, §3.2.1): on the mixed fixture, every batched
/// shape returns the same passing rows in the same order with batches of
/// 1, 7 and 1 000 rows at most and first batches of 1 and of the need, as
/// row-at-a-time evaluation (batches of one) does; each passing post row
/// passes alone, and each failing one fails alone; and pages are equal.
#[tokio::test]
async fn t52_batches_are_row_at_a_time() {
    let Some(mut w) = Walk::new_in("t52-batches", PublicMode::On, Layout::SamePds, Some("followers"), &[], &[], Arc::new(NoHeads)).await else {
        return;
    };
    mixed(&w).await;
    w.f.ready_all().await;
    let owned_pool = w.f.pool.clone();
    let pool = &owned_pool;
    let cases = [(1, 1), (1, 7), (1, 1000), (0, 1), (0, 7), (0, 1000)];
    for need in [3usize, 50] {
        let mut runs: Vec<Vec<schema::FeedCandidate>> = Vec::new();
        for (first, max) in cases {
            let first = if first == 0 { i64::try_from(need).expect("need") } else { first };
            let key = |c: &schema::FeedCandidate| (c.created_at, c.key.clone());
            let rows = schema::passing(need, first, max, None, key, move |after, n| schema::timeline_batch(pool, DAVE, after, n, CAP))
                .await
                .expect("timeline");
            runs.push(rows);
        }
        for (run, (first, max)) in runs.iter().zip(cases) {
            let uris: Vec<&str> = run.iter().map(|c| c.uri.as_str()).collect();
            let reference: Vec<&str> = runs[0].iter().map(|c| c.uri.as_str()).collect();
            w.check(uris == reference, format!("need {need}, first {first}, max {max}: {uris:?} against {reference:?}"));
        }
    }
    // Each row alone: the full single-row passes agree with the prefilter
    // of that row alone.
    let all = schema::passing(1000, 1, 1, None, |c: &schema::FeedCandidate| (c.created_at, c.key.clone()), move |after, n| {
        schema::timeline_batch(pool, DAVE, after, n, CAP)
    })
    .await
    .expect("timeline");
    for c in all.iter().filter(|c| c.repost_uri.is_none()) {
        let alone = schema::prefilter_uris(pool, DAVE, std::slice::from_ref(&c.uri), CAP).await.expect("prefilter");
        w.check(alone.len() == 1, format!("{} passes in the batch but not alone", c.uri));
    }
    // Pages at walk_batch_max 1 and 1 000.
    let mut traces = Vec::new();
    for batch in [1, 1000] {
        let base = serve_live(w.f.state_bounded(&PrivateReadConfig { walk_batch_max: batch, ..PrivateReadConfig::default() })).await;
        let p = pages(&base, TIMELINE, "limit=2", DAVE, 30).await;
        traces.push(p.iter().map(|p| (feed_uris(p), p.get("cursor").is_some())).collect::<Vec<_>>());
    }
    w.check(traces[0] == traces[1], format!("pages differ by batch: {traces:?}"));
    w.finish().await;
}

/// T-52 (collation ties, review 13, m5): posts with one `createdAt` and
/// record keys that sort differently under "C" and under the database's
/// default (mixed case, `_` against `-`), and a `getQuotes` page mixing
/// private quotes and public references: paging one at a time at every
/// batch size neither skips nor repeats, in HEAD's order (descending by
/// the key's bytes).
#[tokio::test]
async fn t52_ties_page_in_byte_order() {
    let Some(mut w) = Walk::new_in("t52-ties", PublicMode::On, Layout::SamePds, Some("followers"), &[], &[], Arc::new(NoHeads)).await else {
        return;
    };
    let (t, aud) = (w.root.clone(), w.aud.clone());
    let keys = ["3kTie-a22222", "3ktie_a22222", "3kTIE_b22222", "3ktie-b22222", "3kTie_c22222", "3ktiE-c22222"];
    let mut posts = Vec::new();
    for k in keys {
        posts.push(w.f.post_with(ALICE, k, &aud, quote_of(&t)).await);
    }
    for (who, k) in [(BOB, "3kRef-a22222"), (CAROL, "3kref_a22222")] {
        w.f.ingest(who, PUBLIC_POST, k, public_quote(&t, "2026-09-26T12:00:00.000Z")).await;
    }
    w.f.ready_all().await;
    let mut expected_feed: Vec<String> = posts.iter().cloned().chain([t.clone()]).collect();
    expected_feed.sort_by(|a, b| {
        let key = |u: &str| u.rsplit('/').next().expect("rkey").as_bytes().to_vec();
        key(b).cmp(&key(a))
    });
    let mut expected_quotes: Vec<String> = posts.clone();
    expected_quotes.extend([format!("at://{BOB}/{PUBLIC_POST}/3kRef-a22222"), format!("at://{CAROL}/{PUBLIC_POST}/3kref_a22222")]);
    expected_quotes.sort_by(|a, b| b.as_bytes().cmp(a.as_bytes()));
    for batch in [1, 7, 1000] {
        let base = serve_live(w.f.state_bounded(&PrivateReadConfig { walk_batch_max: batch, ..PrivateReadConfig::default() })).await;
        let feed: Vec<String> = pages(&base, FEED, &format!("actor={ALICE}&limit=1"), DAVE, 30).await.iter().flat_map(feed_uris).collect();
        w.check(feed == expected_feed, format!("batch {batch}, the author feed: {feed:?}"));
        let quotes: Vec<String> = pages(&base, GET_QUOTES, &format!("uri={t}&limit=1"), DAVE, 30)
            .await
            .iter()
            .flat_map(|p| p["posts"].as_array().cloned().unwrap_or_default())
            .map(|q| q["uri"].as_str().expect("uri").to_string())
            .collect();
        w.check(quotes == expected_quotes, format!("batch {batch}, getQuotes: {quotes:?}"));
    }

    w.finish().await;
}

/// T-52: the reader–author block (rule 4) misses on every kind, either
/// way, with no `:146` skip for a post under its author's own audience; and
/// a self-block record of O changes nothing, for `inside` and the guarded
/// `blocked` agree with rules 6 to 8.
#[tokio::test]
async fn t52_the_reader_author_block_on_every_kind() {
    let Some(mut w) = Walk::new_in("t52-blocks", PublicMode::On, Layout::OffPds, Some("followers"), &[], &[], Arc::new(NoHeads)).await else {
        return;
    };
    let (t, aud) = (w.root.clone(), w.aud.clone());
    let x = format!("at://{CAROL}/{PUBLIC_POST}/3kt52public2");
    let bob_aud = w.f.audience(BOB, "3kt52bobaud2", Some("everyone"), &[]).await;
    let kinds = [
        ("owner top-level", ALICE, t.clone()),
        ("owner thread reply", ALICE, w.reply(ALICE, "3kt52owner22", &t).await),
        ("cross-owner thread reply", BOB, w.reply(BOB, "3kt52cross22", &t).await),
        ("branch root", ALICE, w.f.private_post(ALICE, "3kt52broot22", &aud, Some((&x, &x))).await),
        ("own audience top-level", BOB, w.f.private_post(BOB, "3kt52bobtop2", &bob_aud, None).await),
    ];
    let branch_root = kinds[3].2.clone();
    let branch_reply = w.f.private_post(BOB, "3kt52brrepl", &aud, Some((&x, &branch_root))).await;
    w.f.ready_all().await;
    let mut all: Vec<(&str, &str, String)> = kinds.iter().map(|(k, a, u)| (*k, *a, u.clone())).collect();
    all.push(("branch reply", BOB, branch_reply));
    for (kind, author, uri) in &all {
        w.expect(&format!("{kind}, no block"), DAVE, uri, true).await;
        for (blocker, blocked) in [(DAVE, *author), (*author, DAVE)] {
            exec(&w.f.pool, &block_sql(blocker, blocked)).await;
            w.expect(&format!("{kind}, {blocker} blocks {blocked}"), DAVE, uri, false).await;
            exec(&w.f.pool, "DELETE FROM blocks").await;
        }
    }
    exec(&w.f.pool, &block_sql(ALICE, ALICE)).await;
    for (kind, _, uri) in &all {
        w.expect(&format!("{kind}, O blocks itself"), DAVE, uri, true).await;
    }
    w.expect("O blocks itself, O reads a reply", ALICE, &all[2].2, true).await;
    w.finish().await;
}

/// The relations an `EXPLAIN (ANALYZE, FORMAT JSON)` subtree read: each
/// node's relation, with its actual loops.
fn relations_read(node: &Value, out: &mut Vec<(String, u64)>) {
    if let Some(rel) = node["Relation Name"].as_str() {
        out.push((rel.to_string(), node["Actual Loops"].as_u64().unwrap_or(0)));
    }
    for child in node["Plans"].as_array().into_iter().flatten() {
        relations_read(child, out);
    }
}

/// The subtree of the CTE `name` in a JSON plan.
fn cte<'a>(node: &'a Value, name: &str) -> Option<&'a Value> {
    if node["Subplan Name"] == format!("CTE {name}") {
        return Some(node);
    }
    node["Plans"].as_array().into_iter().flatten().find_map(|c| cte(c, name))
}

/// `EXPLAIN (ANALYZE, FORMAT JSON)` of `sql` with the reader `$1` and the
/// second parameter `uris`: the top plan.
async fn explain(pool: &PgPool, sql: &str, reader: &str, uris: &[String]) -> Value {
    let plan: Value = sqlx::query_scalar(&format!("EXPLAIN (ANALYZE, FORMAT JSON) {sql}"))
        .bind(reader)
        .bind(uris)
        .fetch_one(pool)
        .await
        .expect("explain");
    plan[0]["Plan"].clone()
}

/// T-52 (order of evaluation, review 13, LB1; scoped per review 14, m2): on
/// `prefilter_uris`' single post arm, a reader outside G costs no `blocks`,
/// `private_blocks` or `labels` probe in `s1`, and rows the gate hides cost
/// no block probe. On owner rows, rule 7's V–O term is guarded: 4 block
/// probes a row (rule 4's pair), not 8; with V blocking O, an owner row
/// still misses, through rule 4. The branch climb (`bcl`) reads
/// `private_posts` only (shared-walk T-52).
#[tokio::test]
async fn t52_the_cheapest_terms_run_first() {
    let Some(mut w) = Walk::new_in("t52-order", PublicMode::On, Layout::SamePds, Some("list"), &[BOB], &[], Arc::new(NoHeads)).await else {
        return;
    };
    let aud = w.aud.clone();
    let mut rows = Vec::new();
    for i in 0..5 {
        rows.push(w.f.private_post(ALICE, &format!("3kt52o{i}222222"), &aud, None).await);
    }
    w.f.ready_all().await;
    let pool = w.f.pool.clone();
    let sql = schema::prefilter_uris_statement(CAP);
    let probes = |plan: &Value| {
        let mut read = Vec::new();
        relations_read(cte(plan, "s1").expect("CTE s1"), &mut read);
        read
    };
    // Dave is outside G (`list` of Bob): no block or label probe.
    let read = probes(&explain(&pool, &sql, DAVE, &rows).await);
    w.check(!read.iter().any(|(r, loops)| ["blocks", "private_blocks", "labels"].contains(&r.as_str()) && *loops > 0), format!("outside G: {read:?}"));
    // Every row hidden from Bob, inside G: no block probe.
    for uri in &rows {
        w.label(uri, "!hide").await;
    }
    let read = probes(&explain(&pool, &sql, BOB, &rows).await);
    w.check(!read.iter().any(|(r, loops)| ["blocks", "private_blocks"].contains(&r.as_str()) && *loops > 0), format!("gated: {read:?}"));
    exec(&pool, "DELETE FROM labels").await;
    // The branch climb reads nothing reader-relative (shared-walk T-52).
    let x = format!("at://{CAROL}/{PUBLIC_POST}/3kt52clim");
    let mut branch = Vec::new();
    for i in 0..5 {
        let p = branch.last().cloned().unwrap_or_else(|| x.clone());
        branch.push(w.f.private_post(ALICE, &format!("3kt52c{i}222222"), &aud, Some((&x, &p))).await);
    }
    let plan = explain(&pool, &sql, DAVE, &branch).await;
    let mut read = Vec::new();
    relations_read(cte(&plan, "bcl").expect("CTE bcl"), &mut read);
    w.check(
        read.iter().all(|(r, _)| r == "private_posts") && read.iter().any(|(_, loops)| *loops > 0),
        format!("the branch climb reads: {read:?}"),
    );
    // Owner rows: the block lookups that run are rule 4's four (PostgreSQL
    // may hash each one, running it once for every row); rule 7's V–O four
    // never run.
    let read = probes(&explain(&pool, &sql, BOB, &rows).await);
    let blocks_run = read.iter().filter(|(r, loops)| (r == "blocks" || r == "private_blocks") && *loops > 0).count();
    w.check(blocks_run == 4, format!("owner rows: {blocks_run} block lookups ran: {read:?}"));
    exec(&pool, &block_sql(BOB, ALICE)).await;
    let passing = schema::prefilter_uris(&pool, BOB, &rows, CAP).await.expect("prefilter");
    w.check(passing.is_empty(), format!("V blocking O misses through rule 4: {passing:?}"));
    w.finish().await;
}

/// T-52 (like with like, review 14, m1; review 15, m5): on the mixed
/// fixture with `!hide`, `!takedown` and hiding statuses, `s1ok` equals the
/// gate and the same stage-1 terms in another order (v13's shape: the gate
/// in its own column, stage 1 after it), on every row, so the reorder
/// changes no value.
#[tokio::test]
async fn t52_the_reorder_is_an_equivalence() {
    let Some(mut w) = Walk::new_in("t52-like", PublicMode::On, Layout::SamePds, Some("followers"), &[], &[], Arc::new(NoHeads)).await else {
        return;
    };
    let uris = mixed(&w).await;
    w.label(&uris[7], "!takedown").await;
    w.label(BOB, "!takedown").await;
    exec(&w.f.pool, &format!("UPDATE actors SET account_status = 'suspended' WHERE did = '{CAROL}'")).await;
    w.f.ready_all().await;
    let gate = "NOT EXISTS (SELECT 1 FROM labels l WHERE (l.subject = c.uri AND l.val IN ('!takedown', '!hide')) \
                OR (l.subject = c.author_did AND l.val = '!takedown')) \
                AND COALESCE(po.account_status, 'active') NOT IN ('deactivated', 'takendown', 'suspended', 'deleted')";
    let mut terms = schema::stage1_terms_for_tests();
    terms.retain(|t| !t.starts_with("NOT EXISTS (SELECT 1 FROM labels l WHERE l.subject = c.uri"));
    terms.reverse();
    let reference = format!("COALESCE(CASE WHEN c.author_did = $1 THEN {gate} ELSE ({gate}) AND {} END, false)", terms.join(" AND "));
    let final_ = format!(
        "SELECT s1.uri, s1.s1ok, {reference} FROM s1 c \
         LEFT JOIN private_posts an ON an.uri = c.anchor \
         LEFT JOIN LATERAL (SELECT g.*, NULL::jsonb AS members_j FROM private_audiences g WHERE g.uri = an.audience_uri) a ON true \
         LEFT JOIN actors po ON po.did = c.author_did JOIN s1 ON s1.uri = c.uri"
    );
    let sql = schema::walked(&format!("SELECT {} FROM private_posts p WHERE p.uri = ANY($2)", schema::COLS), "", &final_, CAP);
    for reader in [ALICE, BOB, CAROL, DAVE] {
        let rows: Vec<(String, bool, bool)> = sqlx::query_as(&sql).bind(reader).bind(&uris).fetch_all(&w.f.pool).await.expect("compare");
        assert_eq!(rows.len(), uris.len());
        for (uri, s1ok, reference) in rows {
            w.check(s1ok == reference, format!("{reader} on {uri}: s1ok {s1ok}, reference {reference}"));
        }
    }
    w.finish().await;
}

/// The SubPlans in `node` (correlated subqueries run per outer row) whose
/// subtree scans `private_posts`, by node type and loops.
fn walk_subplans(node: &Value, out: &mut Vec<String>) {
    fn reads_posts(node: &Value) -> bool {
        node["Relation Name"] == "private_posts" || node["Plans"].as_array().into_iter().flatten().any(reads_posts)
    }
    if node["Parent Relationship"] == "SubPlan" && reads_posts(node) {
        out.push(format!("{} {} loops {}", node["Subplan Name"], node["Node Type"], node["Actual Loops"]));
    }
    for child in node["Plans"].as_array().into_iter().flatten() {
        walk_subplans(child, out);
    }
}

/// The `Actual Loops` of every scan of `relation` under `node`.
fn loops_of(node: &Value, relation: &str) -> u64 {
    let own = if node["Relation Name"] == relation { node["Actual Loops"].as_u64().unwrap_or(0) } else { 0 };
    own + node["Plans"].as_array().into_iter().flatten().map(|c| loops_of(c, relation)).sum::<u64>()
}

/// T-52 (shared-walk §7, replacing "one walk per key"): `replyCount`'s
/// statement over 5 000 owner branch replies under one node, and over 50
/// replies by others under 7 parents, runs no SubPlan that scans
/// `private_posts`; each climb's primary-key probes are at most its
/// distinct start parents plus its node rows (T-61's bound).
#[tokio::test]
async fn t52_no_walk_runs_per_key() {
    let Some(mut w) = Walk::new_in("t52-per-key", PublicMode::On, Layout::SamePds, Some("followers"), &[], &[], Arc::new(NoHeads)).await else {
        return;
    };
    let (t, aud) = (w.root.clone(), w.aud.clone());
    let x = format!("at://{DAVE}/{PUBLIC_POST}/3kt52public2");
    let node = w.f.private_post(ALICE, "3kt52node222", &aud, Some((&x, &x))).await;
    // The siblings, written in one statement: the plan, not the writer,
    // is under test.
    exec(
        &w.f.pool,
        &format!(
            "INSERT INTO private_posts (uri, author_did, rkey, cid, rev, audience_uri, reply_root_uri, reply_parent_uri, plaintext_text, created_at) \
             SELECT 'at://{ALICE}/{NSID_POST_PRIVATE}/3kt52s' || g, '{ALICE}', '3kt52s' || g, 'bafy' || g, '{REV}', '{aud}', '{x}', '{node}', 's', now() \
             FROM generate_series(1, 5000) g"
        ),
    )
    .await;
    let mut parents = vec![t.clone()];
    for i in 0..6 {
        parents.push(w.reply(ALICE, &format!("3kt52p{i}222222"), &t).await);
    }
    for i in 0..50 {
        let author = if i % 2 == 0 { BOB } else { CAROL };
        w.reply(author, &format!("3kt52k{i:02}22222"), &parents[i % 7]).await;
    }
    w.f.ready_all().await;
    let sql = schema::reply_counts_statement(CAP);
    let counts = "SELECT (SELECT count(*) FROM bcl WHERE uri IS NOT NULL), (SELECT count(DISTINCT parent) FROM bk), \
                  (SELECT count(*) FROM tcl WHERE uri IS NOT NULL), (SELECT count(DISTINCT parent) FROM tk)";
    let c = format!("SELECT {} FROM private_posts p WHERE p.reply_parent_uri = ANY($2)", schema::COLS);
    for (case, keys) in [("5 000 siblings", vec![node.clone()]), ("50 rows over 7 parents", parents.clone())] {
        let plan = explain(&w.f.pool, &sql, DAVE, &keys).await;
        let mut subplans = Vec::new();
        walk_subplans(&plan, &mut subplans);
        w.check(subplans.is_empty(), format!("{case}: SubPlans scanning private_posts: {subplans:?}"));
        let (bn, bs, tn, ts): (i64, i64, i64, i64) =
            sqlx::query_as(&schema::walked(&c, "", counts, CAP)).bind(DAVE).bind(&keys).fetch_one(&w.f.pool).await.expect("climb counts");
        for (name, nodes, starts) in [("bcl", bn, bs), ("tcl", tn, ts)] {
            let probes = cte(&plan, name).map_or(0, |n| loops_of(n, "private_posts"));
            let bound = u64::try_from(nodes + starts).expect("counts");
            w.check(probes <= bound, format!("{case}: {name} made {probes} probes over {starts} start parents and {nodes} node rows"));
        }
    }
    w.finish().await;
}

/// T-52 (the empty climb, shared-walk §7): on fact 8 for a top-level post
/// both climbs make no probe and return their marker row alone, so
/// `private_posts` is read only as before the climb. (Fact 8 itself now
/// runs the one-row form; this is the shared walk over the same row.)
#[tokio::test]
async fn t52_the_empty_climb() {
    let Some(mut w) = Walk::new_in("t52-empty", PublicMode::On, Layout::SamePds, Some("followers"), &[], &[], Arc::new(NoHeads)).await else {
        return;
    };
    let root = w.root.clone();
    let pool = w.f.pool.clone();
    let plan: Value = sqlx::query_scalar(&format!("EXPLAIN (ANALYZE, FORMAT JSON) {}", shared_one_row(CAP)))
        .bind(DAVE)
        .bind(&root)
        .fetch_one(&pool)
        .await
        .expect("explain");
    let plan = plan[0]["Plan"].clone();
    for name in ["bcl", "tcl"] {
        let node = cte(&plan, name);
        let probes = node.map_or(0, |n| loops_of(n, "private_posts"));
        let rows = node.and_then(|n| n["Actual Rows"].as_u64());
        w.check(probes == 0, format!("{name} probed private_posts {probes} times"));
        w.check(rows.is_none_or(|r| r <= 1), format!("{name} returned {rows:?} rows"));
    }
    let c = format!("SELECT {} FROM private_posts p WHERE p.uri = $2", schema::COLS);
    let (bn, bm, tn, tm): (i64, i64, i64, i64) = sqlx::query_as(&schema::walked(
        &c,
        "",
        "SELECT (SELECT count(*) FROM bcl WHERE uri IS NOT NULL), (SELECT count(*) FROM bcl WHERE uri IS NULL), \
                (SELECT count(*) FROM tcl WHERE uri IS NOT NULL), (SELECT count(*) FROM tcl WHERE uri IS NULL)",
        CAP,
    ))
    .bind(DAVE)
    .bind(&root)
    .fetch_one(&pool)
    .await
    .expect("empty climbs");
    w.check((bn, bm, tn, tm) == (0, 1, 0, 1), format!("the empty climbs: {:?}", (bn, bm, tn, tm)));
    // The rest are today's: `c`'s row and the final's anchor lookup.
    println!("T-52 empty climb: fact 8 on a top-level post scanned private_posts {} times", loops_of(&plan, "private_posts"));
    w.finish().await;
}

/// Prints the pre-S12 count queries (private-threads design §8) for
/// skydeval to run read-only in `psql` against the deployed database:
/// `cargo test pre_s12_count_query -- --ignored --nocapture`.
#[test]
#[ignore = "prints the pre-S12 count queries; run with --ignored --nocapture"]
fn pre_s12_count_query() {
    println!("-- S12 pre-release count: what S12 stops and starts serving, by kind\n{}\n", schema::pre_s12_count_sql());
    println!("-- S12 pre-release count: owner thread replies naming another owner audience\n{}", schema::pre_s12_owner_audience_sql());
}

/// T-55: the pre-S12 count queries run, and count each kind once. Alice's
/// root T moves from `list` A (Bob, Carol, Dave) to `list` B (Carol, Dave):
/// - stops: Bob's reply under A below Alice's reply under A (he does not
///   stand under B); Bob's reply under his own audience; Alice's reply to a
///   deleted root; Alice's post with a public parent and T as root; Alice's
///   17th branch reply below her branch root;
/// - starts: Carol's reply under A (the moved thread); the 9th of a chain
///   of others' replies under B; Alice's reply whose parent is missing;
/// - Alice's reply under A, a wider list than B's.
#[tokio::test]
async fn pre_s12_count_query_counts_each_kind() {
    let Some(w) = Walk::new_in("pre-s12-count", PublicMode::On, Layout::SamePds, Some("list"), &[BOB, CAROL, DAVE], &[], Arc::new(NoHeads)).await else {
        return;
    };
    let (t, a) = (w.root.clone(), w.aud.clone());
    let b = w.f.audience(ALICE, "3kpre12aud2b", Some("list"), &[CAROL, DAVE]).await;
    let bob_aud = w.f.audience(BOB, "3kpre12bobau", Some("list"), &[CAROL]).await;
    let q = w.reply(ALICE, "3kpre12q2222", &t).await;
    w.reply(BOB, "3kpre12r2222", &q).await;
    w.f.private_post(BOB, "3kpre12own22", &bob_aud, Some((&t, &t))).await;
    let d = w.f.private_post(ALICE, "3kpre12dead2", &a, None).await;
    w.f.private_post(ALICE, "3kpre12todd2", &a, Some((&d, &d))).await;
    let public = format!("at://{DAVE}/{PUBLIC_POST}/3kpre12pub22");
    w.f.private_post(ALICE, "3kpre12pubp2", &a, Some((&t, &public))).await;
    let x = format!("at://{DAVE}/{PUBLIC_POST}/3kpre12brx22");
    let mut parent = w.f.private_post(ALICE, "3kpre12b0002", &a, Some((&x, &x))).await;
    for i in 1..=17 {
        parent = w.f.private_post(ALICE, &format!("3kpre12b{i:03}2"), &a, Some((&x, &parent))).await;
    }
    w.f.apply_op(ALICE, Action::Delete, NSID_POST_PRIVATE, "3kpre12dead2", REV2, None).await;
    w.move_root(&b, REV2).await;
    w.reply(CAROL, "3kpre12moved", &t).await;
    let mut parent = t.clone();
    for i in 1..=9 {
        let author = if i % 2 == 0 { CAROL } else { DAVE };
        parent = w.f.private_post(author, &format!("3kpre12d{i:03}2"), &b, Some((&t, &parent))).await;
    }
    w.f.private_post(ALICE, "3kpre12ghost", &b, Some((&t, &format!("at://{NOBODY}/{NSID_POST_PRIVATE}/x")))).await;
    let counts: (i64, i64, i64, i64, i64, i64, i64, i64) =
        sqlx::query_as(&schema::pre_s12_count_sql()).fetch_one(&w.f.pool).await.expect("count query");
    assert_eq!(counts, (1, 1, 1, 1, 1, 1, 1, 1));
    let owner: (i64, i64, i64) = sqlx::query_as(&schema::pre_s12_owner_audience_sql()).fetch_one(&w.f.pool).await.expect("owner query");
    assert_eq!(owner, (1, 0, 1));
    w.finish().await;
}
