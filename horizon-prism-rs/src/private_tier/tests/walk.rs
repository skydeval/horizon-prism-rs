//! The shared walk (#720): T-60, the
//! statement equals the per-key walk it replaced on seeded random fixtures;
//! T-61, the climb's bound; T-62, immunity to the planner's estimate of
//! `c`; T-63, slices in one snapshot.

use std::collections::{BTreeMap, BTreeSet, HashMap};

use super::b5::{Part3, member};
use super::*;

/// SplitMix64: a seeded generator, so a failing fixture can be rebuilt
/// from the seed the test prints.
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    /// Uniform in `0..n` (0 when `n` is 0).
    fn below(&mut self, n: usize) -> usize {
        let n64 = u64::try_from(n.max(1)).unwrap_or(1);
        usize::try_from(self.next() % n64).unwrap_or(0)
    }

    /// Uniform in `lo..=hi`.
    fn between(&mut self, lo: usize, hi: usize) -> usize {
        lo + self.below(hi.saturating_sub(lo) + 1)
    }

    fn chance(&mut self, percent: u64) -> bool {
        self.next() % 100 < percent
    }

    fn pick<T: Clone>(&mut self, items: &[T]) -> T {
        items[self.below(items.len())].clone()
    }
}

/// One stored row of a generated fixture.
#[derive(Clone)]
struct Row {
    uri: String,
    author: String,
    audience: Option<String>,
    root: Option<String>,
    parent: Option<String>,
}

/// A generated fixture: its authors (every one a reader), audiences, rows,
/// and the rows with children (`getReplies` and the expansion's parents).
struct Generated {
    authors: Vec<String>,
    rows: Vec<Row>,
}

/// The tables a fixture writes, emptied before the next.
const T60_TABLES: &str = "private_posts, private_audiences, blocks, labels, follows, sync_state, \
    sync_chain_links, private_notifications, private_notification_seen, private_quote_refs, \
    list_references, subscribed_lists, subscribed_list_items";

/// T-60's fixture generator (shared-walk §7): authors with every audience
/// mode, random list members, follows, verification flags, blocks,
/// labels, hiding statuses and missing `actors` rows; threads,
/// branches (several under one public root, combs, lines) up to `depth`;
/// foreign, bogus and self roots, several roots on one parent, missing
/// parents, cycles with and without a stop, NULL audiences, public parents
/// under private roots, rows with a root and no parent and the reverse;
/// quotes and public quote references, and notifications of both kinds.
#[allow(clippy::too_many_lines)]
async fn generate(pool: &PgPool, rng: &mut Rng, tag: &str, depth: usize) -> Generated {
    exec(pool, &format!("TRUNCATE {T60_TABLES}")).await;
    let n = rng.between(6, 10);
    let authors: Vec<String> = (0..n).map(|i| member(7001 + i64::try_from(i).expect("small"))).collect();
    exec(pool, "DELETE FROM actors").await;
    for a in &authors {
        if !rng.chance(15) {
            actor(pool, a, &format!("{}.test", &a[8..]), "t60").await;
            if rng.chance(10) {
                let status = rng.pick(&["suspended", "deactivated", "takendown"]);
                exec(pool, &format!("UPDATE actors SET account_status = '{status}' WHERE did = '{a}'")).await;
            }
        }
        let holds = rng.chance(60);
        let fresh = rng.chance(80);
        exec(pool, &format!(
            "INSERT INTO sync_state (did, applied_rev, head_rev, head_checked_at, follows_verified_at, follows_verified_rev, firehose_rev) \
             VALUES ('{a}', '3kb', '3kb', now(), {at}, '3kb', '{fh}')",
            at = if holds { "now()" } else { "NULL" },
            fh = if fresh { "3kc" } else { "3ka" },
        )).await;
    }
    // Audiences: one or two per author, of every mode.
    let mut audiences: BTreeMap<String, Vec<String>> = BTreeMap::new();
    for (i, a) in authors.iter().enumerate() {
        for j in 0..rng.between(1, 2) {
            let uri = format!("at://{a}/{NSID_AUDIENCE}/t60g{i}{j}");
            let mode = rng.pick(&[Some("list"), Some("list"), Some("followers"), Some("everyone"), Some("following"), None, Some("bogus")]);
            let members: Vec<String> = authors.iter().filter(|_| rng.chance(50)).cloned().collect();
            sqlx::query(
                "INSERT INTO private_audiences (uri, owner_did, rkey, cid, rev, mode, members, display_filled) \
                 VALUES ($1, $2, $3, 'bafyt60', $4, $5, $6, true)",
            )
            .bind(&uri)
            .bind(a)
            .bind(uri.rsplit('/').next().expect("rkey"))
            .bind(REV)
            .bind(mode)
            .bind(&members)
            .execute(pool)
            .await
            .expect("audience");
            audiences.entry(a.clone()).or_default().push(uri);
        }
    }
    for a in &authors {
        for b in &authors {
            if a != b && rng.chance(30) {
                exec(pool, &follow_sql(a, b)).await;
            }
            if rng.chance(5) {
                exec(pool, &format!(
                    "INSERT INTO blocks (uri, author_did, subject_did, rev, created_at) \
                     VALUES ('at://{a}/app.bsky.graph.block/t60{}', '{a}', '{b}', '3k', now()) ON CONFLICT DO NOTHING",
                    &b[8..]
                )).await;
            }
        }
        if rng.chance(4) {
            exec(pool, &format!("INSERT INTO labels (subject, val) VALUES ('{a}', '!takedown') ON CONFLICT DO NOTHING")).await;
        }
    }

    let mut rows: Vec<Row> = Vec::new();
    let mut named = 0usize;
    let mut name = |author: &str| {
        named += 1;
        format!("at://{author}/{NSID_POST_PRIVATE}/{tag}{named:05}")
    };
    let own_aud = |rng: &mut Rng, author: &str| -> Option<String> {
        if rng.chance(5) {
            None
        } else if rng.chance(8) {
            Some(rng.pick(&audiences.values().flatten().cloned().collect::<Vec<_>>()))
        } else {
            Some(rng.pick(&audiences[author]))
        }
    };
    let bogus = |rng: &mut Rng| format!("at://{}/{NSID_POST_PRIVATE}/t60bogus{}", member(7999), rng.below(5));
    // Threads, each a root and chains below it.
    let mut roots: Vec<(String, String, Option<String>)> = Vec::new();
    for _ in 0..rng.between(2, 4) {
        let author = rng.pick(&authors);
        let uri = name(&author);
        let aud = own_aud(rng, &author);
        rows.push(Row { uri: uri.clone(), author: author.clone(), audience: aud.clone(), root: None, parent: None });
        roots.push((uri, author, aud));
    }
    for _ in 0..rng.between(1, 3) {
        let author = rng.pick(&authors);
        let uri = name(&author);
        let aud = own_aud(rng, &author);
        rows.push(Row { uri, author, audience: aud, root: None, parent: None });
    }
    let writers: Vec<String> = authors.iter().take(4).cloned().collect();
    for (root, owner, root_aud) in roots.clone() {
        let mut thread: Vec<String> = vec![root.clone()];
        for _ in 0..rng.between(1, 3) {
            let mut parent = root.clone();
            for _ in 0..rng.between(1, depth) {
                let author = if rng.chance(25) { owner.clone() } else { rng.pick(&writers) };
                let uri = name(&author);
                let audience = if rng.chance(80) { root_aud.clone() } else { own_aud(rng, &author) };
                let claimed = match rng.below(20) {
                    0 => Some(bogus(rng)),
                    1 => Some(uri.clone()),
                    2 => Some(rng.pick(&rows.iter().map(|r| r.uri.clone()).collect::<Vec<_>>())),
                    _ => Some(root.clone()),
                };
                if rng.chance(10) {
                    parent = rng.pick(&thread);
                }
                rows.push(Row { uri: uri.clone(), author, audience, root: claimed, parent: Some(parent.clone()) });
                thread.push(uri.clone());
                parent = uri;
            }
        }
    }
    // Branches: one or two public roots, several branch roots under each,
    // each a spine with leaf chains (a comb, or a line with none).
    let publics: Vec<String> = (0..rng.between(1, 2)).map(|j| format!("at://{}/{PUBLIC_POST}/t60x{tag}{j}", member(7998))).collect();
    let mut branch_nodes: Vec<(String, String)> = Vec::new();
    for x in &publics {
        for _ in 0..rng.between(1, 3) {
            let owner = rng.pick(&authors);
            let aud = own_aud(rng, &owner);
            let top = name(&owner);
            rows.push(Row { uri: top.clone(), author: owner.clone(), audience: aud.clone(), root: Some(x.clone()), parent: Some(x.clone()) });
            branch_nodes.push((top.clone(), x.clone()));
            let mut parent = top;
            for _ in 0..rng.between(1, depth) {
                let author = if rng.chance(30) { owner.clone() } else { rng.pick(&writers) };
                let uri = name(&author);
                let audience = if rng.chance(85) { aud.clone() } else { own_aud(rng, &author) };
                let claimed = if rng.chance(5) { bogus(rng) } else { x.clone() };
                rows.push(Row { uri: uri.clone(), author: author.clone(), audience: audience.clone(), root: Some(claimed), parent: Some(parent.clone()) });
                branch_nodes.push((uri.clone(), x.clone()));
                if rng.chance(25) {
                    let mut leaf_parent = uri.clone();
                    for _ in 0..rng.between(1, 4) {
                        let leaf_author = rng.pick(&writers);
                        let leaf = name(&leaf_author);
                        rows.push(Row { uri: leaf.clone(), author: leaf_author, audience: audience.clone(), root: Some(x.clone()), parent: Some(leaf_parent) });
                        leaf_parent = leaf;
                    }
                }
                parent = uri;
            }
        }
    }
    // The odd shapes.
    let any_root = |rng: &mut Rng| {
        if rng.chance(50) || branch_nodes.is_empty() { roots[rng.below(roots.len())].0.clone() } else { rng.pick(&publics) }
    };
    for _ in 0..rng.between(2, 6) {
        let author = rng.pick(&authors);
        let audience = own_aud(rng, &author);
        let uri = name(&author);
        let existing: Vec<String> = rows.iter().map(|r| r.uri.clone()).collect();
        let (root, parent) = match rng.below(6) {
            // A missing parent.
            0 => (Some(any_root(rng)), Some(bogus(rng))),
            // A public parent under a private root.
            1 => (Some(roots[0].0.clone()), Some(rng.pick(&publics))),
            // A root and no parent; a parent and no root.
            2 => (Some(any_root(rng)), None),
            3 => (None, Some(rng.pick(&existing))),
            // Several roots on one parent: another claimed root on a
            // parent someone already replied to.
            4 => (Some(any_root(rng)), Some(rng.pick(&existing))),
            _ => (Some(uri.clone()), Some(rng.pick(&existing))),
        };
        rows.push(Row { uri, author, audience, root, parent });
    }
    // Cycles of one to four rows, under a thread or a branch; with a stop
    // when one row is by the thread's owner.
    for _ in 0..rng.between(0, 2) {
        let k = rng.between(1, 4);
        let (root, owner, root_aud) = rng.pick(&roots);
        let thread = rng.chance(60);
        let ctx = if thread { root.clone() } else { rng.pick(&publics) };
        let mut cyc: Vec<Row> = Vec::new();
        for i in 0..k {
            let author = if thread && i == 0 && rng.chance(50) { owner.clone() } else { rng.pick(&writers) };
            let uri = name(&author);
            cyc.push(Row { uri, author, audience: root_aud.clone(), root: Some(ctx.clone()), parent: None });
        }
        for i in 0..k {
            cyc[i].parent = Some(cyc[(i + 1) % k].uri.clone());
        }
        // A key below the cycle.
        let tail_author = rng.pick(&writers);
        let tail = name(&tail_author);
        let tail_parent = cyc[0].uri.clone();
        rows.extend(cyc);
        rows.push(Row { uri: tail, author: tail_author, audience: root_aud.clone(), root: Some(ctx), parent: Some(tail_parent) });
    }

    // Write the rows, then quotes, labels and notifications.
    let col = |f: &dyn Fn(&Row) -> Option<String>| rows.iter().map(f).collect::<Vec<Option<String>>>();
    sqlx::query(&format!(
        "INSERT INTO private_posts (uri, author_did, rkey, cid, rev, audience_uri, reply_root_uri, reply_parent_uri, \
           plaintext_text, created_at, display_filled) \
         SELECT u, a, split_part(u, '/', 5), 'bafyt60', '{REV}', aud, root, parent, 't60', \
                now() - make_interval(secs => ord::double precision / 1000), true \
         FROM unnest($1::text[], $2::text[], $3::text[], $4::text[], $5::text[]) WITH ORDINALITY AS t(u, a, aud, root, parent, ord)"
    ))
    .bind(col(&|r| Some(r.uri.clone())))
    .bind(col(&|r| Some(r.author.clone())))
    .bind(col(&|r| r.audience.clone()))
    .bind(col(&|r| r.root.clone()))
    .bind(col(&|r| r.parent.clone()))
    .execute(pool)
    .await
    .expect("rows");
    let uris: Vec<String> = rows.iter().map(|r| r.uri.clone()).collect();
    for r in &rows {
        if rng.chance(15) {
            exec(pool, &format!("UPDATE private_posts SET quote_uri = '{}' WHERE uri = '{}'", rng.pick(&uris), r.uri)).await;
        }
        if rng.chance(4) {
            let val = rng.pick(&["!hide", "!takedown"]);
            exec(pool, &format!("INSERT INTO labels (subject, val) VALUES ('{}', '{val}') ON CONFLICT DO NOTHING", r.uri)).await;
        }
    }
    for i in 0..rng.between(0, 3) {
        let author = rng.pick(&authors);
        exec(pool, &format!(
            "INSERT INTO private_quote_refs (uri, author_did, cid, rev, quoted_uri, created_at) \
             VALUES ('at://{author}/{PUBLIC_POST}/t60q{i}', '{author}', 'bafyq', '3k', '{}', now())",
            rng.pick(&uris)
        )).await;
    }
    for a in &authors {
        for i in 0..rng.between(0, 6) {
            let subject = if rng.chance(85) { rng.pick(&uris) } else { bogus(rng) };
            if rng.chance(60) {
                let reason = rng.pick(&["mention", "reply", "quote"]);
                exec(pool, &format!(
                    "INSERT INTO private_notifications (recipient_did, author_did, reason, record_uri, record_cid) \
                     SELECT '{a}', p.author_did, '{reason}', p.uri, p.cid FROM private_posts p WHERE p.uri = '{subject}' ON CONFLICT DO NOTHING"
                )).await;
            } else {
                let from = rng.pick(&authors);
                exec(pool, &format!(
                    "INSERT INTO private_notifications (recipient_did, author_did, reason, reason_subject, record_uri, record_cid) \
                     VALUES ('{a}', '{from}', 'like', '{subject}', 'at://{from}/app.bsky.feed.like/t60n{i}', 'bafyn') ON CONFLICT DO NOTHING"
                )).await;
            }
        }
        match rng.below(3) {
            0 => exec(pool, &format!("INSERT INTO private_notification_seen (did, seen_at) VALUES ('{a}', now() - interval '1 day')")).await,
            1 => exec(pool, &format!("INSERT INTO private_notification_seen (did, seen_at) VALUES ('{a}', now() + interval '1 day')")).await,
            _ => {}
        }
    }
    // List blocks (private-blocks design §10), drawn last so the rest of a
    // seed's fixture is as before: an author's block list naming random
    // authors (itself among them at times), some not yet synced.
    for (i, a) in authors.iter().enumerate() {
        if rng.chance(25) {
            let subjects: Vec<&str> = authors.iter().filter(|_| rng.chance(30)).map(String::as_str).collect();
            if !subjects.is_empty() {
                exec(pool, &list_block_sql(a, a, &format!("t60l{i}"), &subjects)).await;
                if rng.chance(20) {
                    exec(pool, &format!("UPDATE list_references SET effective = false WHERE member_did = '{a}'")).await;
                }
            }
        }
    }
    Generated { authors, rows }
}

/// Run `f` with the per-key reference, then with the shared walk.
macro_rules! both {
    ($e:expr) => {{
        schema::PER_KEY.with(|p| p.set(true));
        let reference = $e;
        schema::PER_KEY.with(|p| p.set(false));
        let shared = $e;
        (reference, shared)
    }};
}

/// Every stored URI's fact-8 outputs for `reader`, in one statement: fact
/// 8's own statement over every row, with the row's URI first.
async fn fact8_all(pool: &PgPool, reader: &str, uris: &[String], cap: u32) -> BTreeMap<String, String> {
    let sql = schema::walked(
        &format!("SELECT {} FROM private_posts p WHERE p.uri = ANY($2)", schema::COLS),
        "",
        &schema::fact8_final().replacen("SELECT s1.owner, s1.gov, s1.s1ok,", "SELECT s1.uri, s1.owner, s1.gov, s1.s1ok,", 1),
        cap,
    );
    type Row = (String, Option<String>, Option<String>, bool, bool, bool);
    let rows: Vec<Row> = sqlx::query_as(&sql).bind(reader).bind(uris).fetch_all(pool).await.expect("fact 8 over every row");
    rows.into_iter().map(|r| (r.0.clone(), format!("{r:?}"))).collect()
}

/// `bdm` against `brm` on every branch key of `c`, and `pvm`'s true
/// entries against `pv`'s: the mismatches.
async fn maps_agree(pool: &PgPool, reader: &str, uris: &[String], cap: u32) -> Vec<String> {
    let c = format!("SELECT {} FROM private_posts p WHERE p.uri = ANY($2)", schema::COLS);
    let reference: (Value, Value) = sqlx::query_as(&schema::walked_per_key(&c, "", "SELECT (SELECT m FROM brm), (SELECT m FROM pvm)", cap))
        .bind(reader)
        .bind(uris)
        .fetch_one(pool)
        .await
        .expect("per-key maps");
    let shared: (Value, Value) = sqlx::query_as(&schema::walked(&c, "", "SELECT (SELECT m FROM bdm), (SELECT m FROM pvm)", cap))
        .bind(reader)
        .bind(uris)
        .fetch_one(pool)
        .await
        .expect("shared maps");
    let mut out = Vec::new();
    for (key, br) in reference.0.as_object().into_iter().flatten() {
        let bd = shared.0.get(key).cloned().unwrap_or(Value::Null);
        if *br != bd {
            out.push(format!("branch root of {key}: per-key {br}, shared {bd}"));
        }
    }
    let trues = |m: &Value| m.as_object().into_iter().flatten().filter(|(_, v)| **v == Value::Bool(true)).map(|(k, _)| k.clone()).collect::<BTreeSet<_>>();
    let (a, b) = (trues(&reference.1), trues(&shared.1));
    if a != b {
        out.push(format!("walk verdicts: per-key {a:?}, shared {b:?}"));
    }
    out
}

/// One batched shape of §3.2, run through the batch loop with `need`,
/// `first` and `max`: its rows as text.
#[allow(clippy::too_many_arguments)]
async fn batched(pool: &PgPool, shape: usize, reader: &str, g: &Generated, need: usize, first: i64, max: i64, cap: u32, pick: usize) -> String {
    let some = |i: usize| g.rows[i % g.rows.len()].uri.clone();
    let parent = g.rows.iter().filter_map(|r| r.parent.clone()).nth(pick % g.rows.len().max(1)).unwrap_or_else(|| some(pick));
    let feed_key = |c: &schema::FeedCandidate| (c.created_at, c.key.clone());
    let pair_key = |(u, at): &(String, DateTime<Utc>)| (*at, u.clone());
    match shape % 7 {
        0 => format!("{:?}", schema::passing(need, first, max, None, feed_key, |after, n| schema::timeline_batch(pool, reader, after, n, cap)).await.expect("timeline")),
        1 | 2 => {
            let actor = g.authors[pick % g.authors.len()].clone();
            // Every filter (P-20, P-30), in a fixed order.
            let mut out = String::new();
            for filter in schema::AuthorFilter::ALL {
                let rows = schema::passing(need, first, max, None, feed_key, |after, n| schema::author_batch(pool, reader, &actor, filter, after, n, cap))
                    .await
                    .expect("author feed");
                out.push_str(&format!("{filter:?}: {rows:?}; "));
            }
            out
        }
        3 => format!("{:?}", schema::passing(need, first, max, None, pair_key, |after, n| schema::reply_page_batch(pool, reader, &parent, &[], after, n, cap)).await.expect("replies")),
        4 => format!("{:?}", schema::passing(need, first, max, None, pair_key, |after, n| schema::reply_candidates_batch(pool, reader, &parent, after, n, cap)).await.expect("expansion")),
        5 => {
            let start = (Utc::now() + chrono::Duration::hours(1), String::new());
            let lower = Utc::now() - chrono::Duration::days(1);
            format!(
                "{:?}",
                schema::passing(need, first, max, Some(start.clone()), pair_key, |after: Option<(DateTime<Utc>, String)>, n| {
                    let from = after.unwrap_or_else(|| start.clone());
                    schema::search_batch(pool, reader, from, lower, n, cap)
                })
                .await
                .expect("search")
            )
        }
        _ => {
            let quoted = some(pick);
            let key = |c: &schema::QuoteCandidate| (c.created_at, c.uri.clone());
            format!("{:?}", schema::passing(need, first, max, None, key, |after, n| schema::quote_batch(pool, reader, &quoted, after, n, cap)).await.expect("quotes"))
        }
    }
}

/// Fact 8's five outputs as a row.
type Fact8Row = (Option<String>, Option<String>, bool, bool, bool);

/// The pre-S12 count's eight columns.
type PreS12 = (i64, i64, i64, i64, i64, i64, i64, i64);

/// The fixture count of T-60: `HPRS_T60_FIXTURES`, else 300.
fn t60_fixtures() -> usize {
    std::env::var("HPRS_T60_FIXTURES").ok().and_then(|s| s.parse().ok()).unwrap_or(300)
}

/// T-60 (shared-walk §7): on seeded random fixtures (the seed printed;
/// `HPRS_T60_SEED` replays one), the shared walk and the per-key walk it
/// replaced agree, at caps 1, 2, 3, 5 and 16 on fixtures up to 48 deep and
/// at 16 and 100 on deep ones (every 50th, up to 300 deep):
/// - for every author as reader: `prefilter_uris` over every row; fact 8's
///   five outputs for every row; `bdm` against `brm` and `pvm` against `pv`;
/// - rotating through the readers, two a fixture and cap: one batched shape
///   (all seven in turn) at batch sizes 1, 7, the need and 1 000, and at K
///   forced to 1 and 3; `replyCount` and `quoteCount` (through
///   [`schema::counts_sliced`]) and `unread`, unchunked and at K 1 and 3;
///   fact 8's one-row form against the shared walk on two rows;
/// - the pre-S12 count, once a fixture;
/// - and no statement of either build raises an error.
#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn t60_the_shared_walk_equals_the_per_key_walk() {
    let Some(db) = fresh_db("t60-shared-walk").await else {
        return;
    };
    let pool = db.pool.clone();
    let seed = std::env::var("HPRS_T60_SEED")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or_else(|| u64::try_from(Utc::now().timestamp_micros()).unwrap_or(720));
    println!("T-60 seed {seed}");
    let mut rng = Rng(seed);
    let mut failures: Vec<String> = Vec::new();
    let small_caps = [1u32, 2, 3, 5, 16];
    let configs: [(i64, i64); 6] = [(1, 1), (7, 7), (0, 0), (0, 1000), (0, 1), (0, 3)];
    let fixtures = t60_fixtures();
    let mut rows_seen = 0usize;
    for f in 0..fixtures {
        let deep = f % 50 == 49;
        let caps: Vec<u32> = if deep { vec![16, 100] } else { vec![small_caps[f % small_caps.len()]] };
        let depth = if deep { rng.between(120, 300) } else { usize::try_from((caps[0] * 3).min(48)).expect("small") };
        let g = generate(&pool, &mut rng, &format!("t60f{f}"), depth).await;
        rows_seen += g.rows.len();
        let uris: Vec<String> = g.rows.iter().map(|r| r.uri.clone()).collect();
        let at = |what: &str, cap: u32, reader: &str| format!("seed {seed}, fixture {f}, cap {cap}, reader {reader}: {what}");
        let (a, b): (PreS12, PreS12) = both!(sqlx::query_as(&schema::pre_s12_count_sql()).fetch_one(&pool).await.expect("pre-S12 count"));
        if a != b {
            failures.push(format!("seed {seed}, fixture {f}: pre-S12 count per-key {a:?}, shared {b:?}"));
        }
        for &cap in &caps {
            for (ri, reader) in g.authors.iter().enumerate() {
                let (mut a, mut b) = both!(schema::prefilter_uris_sliced(&pool, reader, &uris, cap, usize::MAX).await.expect("prefilter"));
                a.sort();
                b.sort();
                if a != b {
                    failures.push(at(&format!("prefilter per-key {a:?}, shared {b:?}"), cap, reader));
                }
                let (a, b) = both!(fact8_all(&pool, reader, &uris, cap).await);
                if a != b {
                    let diff: Vec<String> = a.iter().filter(|(k, v)| b.get(*k) != Some(*v)).map(|(k, v)| format!("{k}: {v} / {:?}", b.get(k))).collect();
                    failures.push(at(&format!("fact 8 {diff:?}"), cap, reader));
                }
                for m in maps_agree(&pool, reader, &uris, cap).await {
                    failures.push(at(&m, cap, reader));
                }
                // The rotating checks, two readers a fixture and cap.
                if ri % g.authors.len().div_ceil(2).max(1) != f % 2 {
                    continue;
                }
                let turn = f + ri;
                let need = if turn % 2 == 0 { 3 } else { 11 };
                let (first, max) = configs[turn % configs.len()];
                let first = if first == 0 { i64::try_from(need).expect("need") } else { first };
                let max = if max == 0 { first } else { max };
                let pick = rng.below(1000);
                let (a, b) = both!(batched(&pool, turn, reader, &g, need, first, max, cap, pick).await);
                if a != b {
                    failures.push(at(&format!("shape {} (need {need}, first {first}, max {max}): per-key {a}, shared {b}", turn % 7), cap, reader));
                }
                let k = [usize::MAX, 1, 3][turn % 3];
                schema::PER_KEY.with(|p| p.set(true));
                let replies: Vec<(String, i64)> =
                    sqlx::query_as(&schema::reply_counts_statement(cap)).bind(reader).bind(&uris).fetch_all(&pool).await.expect("per-key replies");
                let quotes: Vec<(String, i64)> =
                    sqlx::query_as(&schema::quote_counts_statement(cap)).bind(reader).bind(&uris).fetch_all(&pool).await.expect("per-key quotes");
                let unread: i64 = sqlx::query_scalar(&schema::unread_statement(cap)).bind(reader).fetch_one(&pool).await.expect("per-key unread");
                schema::PER_KEY.with(|p| p.set(false));
                let counts = schema::counts_sliced(&pool, reader, &uris, cap, k, None).await.expect("counts");
                let got_replies: BTreeMap<String, i64> = counts.iter().filter(|(_, c)| c.reply > 0).map(|(u, c)| (u.clone(), c.reply)).collect();
                let got_quotes: BTreeMap<String, i64> = counts.iter().filter(|(_, c)| c.quote > 0).map(|(u, c)| (u.clone(), c.quote)).collect();
                let want_replies: BTreeMap<String, i64> = replies.into_iter().filter(|(_, n)| *n > 0).collect();
                let want_quotes: BTreeMap<String, i64> = quotes.into_iter().filter(|(_, n)| *n > 0).collect();
                if got_replies != want_replies || got_quotes != want_quotes {
                    failures.push(at(&format!("counts at K {k}: replies {want_replies:?} / {got_replies:?}, quotes {want_quotes:?} / {got_quotes:?}"), cap, reader));
                }
                let got_unread = schema::unread_count_sliced(&pool, reader, cap, k, None).await.expect("unread");
                if got_unread != unread {
                    failures.push(at(&format!("unread at K {k}: per-key {unread}, shared {got_unread}"), cap, reader));
                }
                for _ in 0..2 {
                    let uri = rng.pick(&uris);
                    // Fact 8's one-row form against the shared walk over the
                    // same one row.
                    let one = schema::fact8(&pool, reader, &uri, cap).await.expect("fact 8");
                    let c = format!("SELECT {} FROM private_posts p WHERE p.uri = $2", schema::COLS);
                    let shared: Option<Fact8Row> =
                        sqlx::query_as(&schema::walked(&c, "", &schema::fact8_final(), cap)).bind(reader).bind(&uri).fetch_optional(&pool).await.expect("shared fact 8");
                    let shared = shared.map_or_else(schema::Fact8::default, |(owner, gov, s1ok, verdict, anchored)| schema::Fact8 { owner, gov, s1ok, verdict, anchored });
                    if one != shared {
                        failures.push(at(&format!("fact 8 alone on {uri}: one-row form {one:?}, shared {shared:?}"), cap, reader));
                    }
                }
            }
        }
    }
    println!("T-60: {fixtures} fixtures, {rows_seen} rows, {} mismatches", failures.len());
    db.drop_db().await;
    assert!(failures.is_empty(), "{} mismatches:\n{}", failures.len(), failures.iter().take(40).cloned().collect::<Vec<_>>().join("\n"));
}

/// T-DM12 (degraded-mode design §2.5, §2.6): on T-60's generated fixtures
/// (`HPRS_TDM12_FIXTURES`, else 40), for every author as reader and every
/// row, `fact8_with_authors` gives `fact8`'s five outputs, and its
/// `walk_authors` are the authors of the walk's passing rows other than O:
/// within the structural climb's authors less O, and equal to them when
/// the walk passed (a passing walk visits every row of the climb), empty
/// for the reader's own row and for a row that is not walked.
#[tokio::test]
async fn tdm12_fact8_with_authors_is_fact8_and_its_walk() {
    let Some(db) = fresh_db("tdm12-walk-authors").await else {
        return;
    };
    let pool = db.pool.clone();
    let seed = std::env::var("HPRS_T60_SEED")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or_else(|| u64::try_from(Utc::now().timestamp_micros()).unwrap_or(812));
    println!("T-DM12 seed {seed}");
    let fixtures = std::env::var("HPRS_TDM12_FIXTURES").ok().and_then(|s| s.parse().ok()).unwrap_or(40);
    let mut rng = Rng(seed);
    let mut failures: Vec<String> = Vec::new();
    let (mut compared, mut equal_checked) = (0usize, 0usize);
    for f in 0..fixtures {
        let cap = [3u32, 5, 16][f % 3];
        let g = generate(&pool, &mut rng, &format!("tdm12f{f}"), usize::try_from(cap * 2).expect("small")).await;
        let uris: Vec<String> = g.rows.iter().map(|r| r.uri.clone()).collect();
        let climbs = schema::climb_authors(&pool, &uris, cap).await.expect("climb");
        for reader in &g.authors {
            for row in &g.rows {
                let plain = schema::fact8(&pool, reader, &row.uri, cap).await.expect("fact 8");
                let (with, authors) = schema::fact8_with_authors(&pool, reader, &row.uri, cap).await.expect("fact 8 with authors");
                let at = format!("seed {seed}, fixture {f}, cap {cap}, reader {reader}, {}", row.uri);
                compared += 1;
                if plain != with {
                    failures.push(format!("{at}: fact 8 {plain:?}, with authors {with:?}"));
                }
                let mut authors = authors;
                authors.sort();
                authors.dedup();
                let climb = climbs.get(&row.uri);
                let owner = with.owner.clone().unwrap_or_default();
                let mut climbed: Vec<String> = climb.map(|c| c.authors.iter().filter(|a| **a != owner).cloned().collect()).unwrap_or_default();
                climbed.sort();
                climbed.dedup();
                if &row.author == reader || !climb.is_some_and(|c| c.walked) {
                    if !authors.is_empty() {
                        failures.push(format!("{at}: not walked for this reader, yet walk authors {authors:?}"));
                    }
                    continue;
                }
                if !authors.iter().all(|a| climbed.contains(a)) {
                    failures.push(format!("{at}: walk authors {authors:?} outside the climb {climbed:?}"));
                }
                if with.verdict && with.s1ok {
                    equal_checked += 1;
                    if authors != climbed {
                        failures.push(format!("{at}: a passing walk's authors {authors:?}, the climb's {climbed:?}"));
                    }
                }
            }
        }
    }
    println!("T-DM12: {compared} reads compared, {equal_checked} passing walks checked equal, {} mismatches", failures.len());
    db.drop_db().await;
    assert!(equal_checked > 0, "no passing walk was checked: the equality proves nothing");
    assert!(failures.is_empty(), "{} mismatches:\n{}", failures.len(), failures.iter().take(40).cloned().collect::<Vec<_>>().join("\n"));
}

/// A climb's counts in one statement over `c` (the rows `$2`, reader `$1`):
/// its node rows and distinct start parents.
async fn climb_counts(pool: &PgPool, reader: &str, uris: &[String], cap: u32, climb: &str, starts: &str) -> (i64, i64) {
    let c = format!("SELECT {} FROM private_posts p WHERE p.uri = ANY($2)", schema::COLS);
    let final_ = format!("SELECT (SELECT count(*) FROM {climb} WHERE uri IS NOT NULL), (SELECT count(DISTINCT parent) FROM {starts})");
    sqlx::query_as(&schema::walked(&c, "", &final_, cap)).bind(reader).bind(uris).fetch_one(pool).await.expect("climb counts")
}

/// The node rows the climb must hold for the keys of the rows `$1`: each
/// key's lineage within the cap, by agreement and its kind's stop, distinct
/// by URI and context. Independent of the statement. `thread` keys take
/// their root's author as O.
async fn lineage_rows(pool: &PgPool, uris: &[String], cap: u32, thread: bool) -> i64 {
    let kind = if thread {
        "split_part(p.reply_parent_uri, '/', 4) = 'tools.kryphocron.feed.postPrivate' AND split_part(p.reply_root_uri, '/', 4) = 'tools.kryphocron.feed.postPrivate'"
    } else {
        "split_part(p.reply_parent_uri, '/', 4) = 'tools.kryphocron.feed.postPrivate' AND split_part(p.reply_root_uri, '/', 4) IS DISTINCT FROM 'tools.kryphocron.feed.postPrivate' AND p.reply_root_uri IS NOT NULL"
    };
    let stop = if thread {
        "(u.uri = u.ctx OR (u.author = u.owner AND COALESCE(split_part(u.parent, '/', 4) = 'tools.kryphocron.feed.postPrivate', false)))"
    } else {
        "NOT COALESCE(split_part(u.parent, '/', 4) = 'tools.kryphocron.feed.postPrivate', false)"
    };
    let agree = |x: &str, ctx: &str| {
        if thread { format!("({x}.reply_root_uri = {ctx} OR {x}.uri = {ctx})") } else { format!("{x}.reply_root_uri = {ctx}") }
    };
    sqlx::query_scalar(&format!(
        "WITH RECURSIVE k AS ( \
           SELECT DISTINCT p.reply_parent_uri AS parent, p.reply_root_uri AS root, \
                  (SELECT r.author_did FROM private_posts r WHERE r.uri = p.reply_root_uri) AS owner \
           FROM private_posts p WHERE p.uri = ANY($1) AND {kind}), \
         up(uri, parent, author, ctx, owner, hop) AS ( \
           SELECT s.uri, s.reply_parent_uri, s.author_did, k.root, k.owner, 1 FROM k JOIN private_posts s ON s.uri = k.parent \
           WHERE {a1} \
           UNION ALL \
           SELECT s.uri, s.reply_parent_uri, s.author_did, u.ctx, u.owner, u.hop + 1 FROM up u JOIN private_posts s ON s.uri = u.parent \
           WHERE u.hop < {cap} AND NOT {stop} AND {a2}) \
         SELECT count(*) FROM (SELECT DISTINCT uri, ctx FROM up) d",
        a1 = agree("s", "k.root"),
        a2 = agree("s", "u.ctx"),
    ))
    .bind(uris)
    .fetch_one(pool)
    .await
    .expect("lineage rows")
}

/// The `Actual Loops` of every scan of `relation` in the subtree of the
/// CTE `name`.
fn scan_loops(plan: &Value, name: &str, relation: &str) -> u64 {
    fn sum(node: &Value, relation: &str) -> u64 {
        let own = if node["Relation Name"] == relation { node["Actual Loops"].as_u64().unwrap_or(0) } else { 0 };
        own + node["Plans"].as_array().into_iter().flatten().map(|c| sum(c, relation)).sum::<u64>()
    }
    fn find<'a>(node: &'a Value, name: &str) -> Option<&'a Value> {
        if node["Subplan Name"] == format!("CTE {name}") {
            return Some(node);
        }
        node["Plans"].as_array().into_iter().flatten().find_map(|c| find(c, name))
    }
    find(plan, name).map_or(0, |n| sum(n, relation))
}

/// `EXPLAIN (ANALYZE, FORMAT JSON)` of `sql` with `$1` the reader and `$2`
/// `uris`: the top plan.
async fn explain(pool: &PgPool, sql: &str, reader: &str, uris: &[String]) -> Value {
    let plan: Value = sqlx::query_scalar(&format!("EXPLAIN (ANALYZE, FORMAT JSON) {sql}"))
        .bind(reader)
        .bind(uris)
        .fetch_one(pool)
        .await
        .expect("explain");
    plan[0]["Plan"].clone()
}

/// T-61 (shared-walk §7): on a line, a comb whose leaves meet the spine at
/// varying offsets, disjoint chains, a parent with 1 000 claimed roots, and
/// a thread line of standing writers, at caps 16 and 100: under each climb
/// the primary-key probes number at most its distinct start parents plus
/// its node rows, and its node rows are the keys' lineages within the cap.
#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn t61_the_climb_is_bounded() {
    let Some(mut w) = Walk::new_in("t61-climb", PublicMode::On, Layout::SamePds, Some("list"), &[BOB, CAROL, DAVE], &[], Arc::new(NoHeads)).await else {
        return;
    };
    let (t, aud) = (w.root.clone(), w.aud.clone());
    let pool = w.f.pool.clone();
    let mut b = super::b5::Bulk::new("t61");
    let x = |i: usize| format!("at://{CAROL}/{PUBLIC_POST}/t61x{i}");
    let mut shapes: Vec<(&str, Vec<String>, bool)> = Vec::new();
    // A line of 300 rows, every row a key.
    let mut parent = b.post(ALICE, &aud, Some((&x(0), &x(0))));
    let mut line = Vec::new();
    for _ in 0..300 {
        parent = b.post(BOB, &aud, Some((&x(0), &parent)));
        line.push(parent.clone());
    }
    shapes.push(("line", line, false));
    // A comb: a spine of 300, a leaf chain of (i mod 7) + 1 off every
    // fifth spine row, the last leaf of each a key.
    let mut spine = b.post(ALICE, &aud, Some((&x(1), &x(1))));
    let mut comb = Vec::new();
    for i in 0..300 {
        spine = b.post(BOB, &aud, Some((&x(1), &spine)));
        if i % 5 == 0 {
            let mut leaf = spine.clone();
            for _ in 0..=(i / 5) % 7 {
                leaf = b.post(CAROL, &aud, Some((&x(1), &leaf)));
            }
            comb.push(leaf);
        }
    }
    shapes.push(("comb", comb, false));
    // Disjoint chains: 30 of 40 rows, the bottom of each a key.
    let mut disjoint = Vec::new();
    for c in 0..30 {
        let mut p = b.post(ALICE, &aud, Some((&x(10 + c), &x(10 + c))));
        for _ in 0..40 {
            p = b.post(BOB, &aud, Some((&x(10 + c), &p)));
        }
        disjoint.push(p);
    }
    shapes.push(("disjoint chains", disjoint, false));
    // A thread line of 200 standing writers' replies under Alice's root.
    let mut p = t.clone();
    let mut thread = Vec::new();
    for i in 0..200 {
        p = b.post(if i % 2 == 0 { BOB } else { CAROL }, &aud, Some((&t, &p)));
        thread.push(p.clone());
    }
    shapes.push(("thread line", thread, true));
    // One parent, a branch row 50 deep, with 1 000 claimed roots: one the
    // true root, the rest bogus.
    let mut deep = b.post(ALICE, &aud, Some((&x(99), &x(99))));
    for _ in 0..50 {
        deep = b.post(BOB, &aud, Some((&x(99), &deep)));
    }
    let mut claimed = vec![b.post(CAROL, &aud, Some((&x(99), &deep)))];
    for i in 0..999 {
        claimed.push(b.post(CAROL, &aud, Some((&x(100 + i), &deep))));
    }
    shapes.push(("1 000 claimed roots on one parent", claimed, false));
    b.write(&pool).await;
    w.f.ready_all().await;
    exec(&pool, "ANALYZE").await;
    for cap in [16u32, 100] {
        for (name, keys, thread) in &shapes {
            let (climb, starts) = if *thread { ("tcl", "tk") } else { ("bcl", "bk") };
            let (nodes, start_parents) = climb_counts(&pool, DAVE, keys, cap, climb, starts).await;
            let expected = lineage_rows(&pool, keys, cap, *thread).await;
            let plan = explain(&pool, &schema::prefilter_uris_statement(cap), DAVE, keys).await;
            let probes = scan_loops(&plan, climb, "private_posts");
            println!("T-61 {name}, cap {cap}: {} keys, {start_parents} start parents, {nodes} node rows ({expected} expected), {probes} probes", keys.len());
            w.check(nodes == expected && nodes > 0, format!("{name}, cap {cap}: {nodes} node rows, {expected} expected"));
            let bound = u64::try_from(start_parents + nodes).expect("counts");
            w.check(probes <= bound && probes > 0, format!("{name}, cap {cap}: {probes} probes over {start_parents} + {nodes}"));
        }
    }
    w.finish().await;
}

/// Whether some Nested Loop in `node` has a CTE Scan or WorkTable Scan on
/// its inner side (through single-child nodes) with Actual Loops over 1:
/// the offending nodes.
fn rescanned(node: &Value, out: &mut Vec<String>) {
    if node["Node Type"] == "Nested Loop"
        && let Some(inner) = node["Plans"].as_array().and_then(|p| p.iter().find(|c| c["Parent Relationship"] == "Inner"))
    {
        let mut n = inner;
        loop {
            if matches!(n["Node Type"].as_str(), Some("CTE Scan" | "WorkTable Scan")) && n["Actual Loops"].as_u64().unwrap_or(0) > 1 {
                out.push(format!("{} {} loops {}", n["Node Type"], n["CTE Name"], n["Actual Loops"]));
                break;
            }
            match n["Plans"].as_array() {
                Some(p) if p.len() == 1 => n = &p[0],
                _ => break,
            }
        }
    }
    for child in node["Plans"].as_array().into_iter().flatten() {
        rescanned(child, out);
    }
}

/// The CTE `c`'s planned and actual rows in a JSON plan.
fn c_estimate(node: &Value) -> Option<(u64, u64)> {
    if node["Subplan Name"] == "CTE c" {
        return Some((node["Plan Rows"].as_u64().unwrap_or(0), node["Actual Rows"].as_u64().unwrap_or(0)));
    }
    node["Plans"].as_array().into_iter().flatten().find_map(c_estimate)
}

/// T-62 (shared-walk §7): on the `Distinct` fixture after `ANALYZE` (the
/// unread slice's `c`, estimated at 1 row against 5 000 at 7e2d324) and on
/// the `Chain` fixture's first timeline batch, no Nested Loop rescans a
/// CTE or the working table on its inner side. The `c` estimate is printed
/// against its actual rows.
#[tokio::test]
async fn t62_no_plan_rescans_a_cte() {
    let Some(mut w) = Walk::new_in("t62-estimates", PublicMode::On, Layout::SamePds, Some("followers"), &[], &[], Arc::new(NoHeads)).await else {
        return;
    };
    let pool = w.f.pool.clone();
    Part3::Distinct.build(&pool).await;
    Part3::Chain.build(&pool).await;
    w.f.ready_all().await;
    exec(&pool, "ANALYZE").await;
    let ids: Vec<i64> = sqlx::query_scalar(&schema::unread_keys_statement()).bind(DAVE).fetch_all(&pool).await.expect("ids");
    let k = schema::climb_keys(CAP);
    let slice: Vec<i64> = ids.iter().take(k).copied().collect();
    let plan: Value = sqlx::query_scalar(&format!("EXPLAIN (ANALYZE, FORMAT JSON) {}", schema::unread_slice_statement(CAP)))
        .bind(DAVE)
        .bind(&slice)
        .fetch_one(&pool)
        .await
        .expect("explain unread");
    let mut bad = Vec::new();
    rescanned(&plan[0]["Plan"], &mut bad);
    println!("T-62 unread slice ({} ids): c estimated {:?} (planned, actual)", slice.len(), c_estimate(&plan[0]["Plan"]));
    w.check(bad.is_empty() && !slice.is_empty(), format!("unread slice rescans: {bad:?}"));
    for cap in [16u32, 100] {
        let none: Option<DateTime<Utc>> = None;
        let plan: Value = sqlx::query_scalar(&format!("EXPLAIN (ANALYZE, FORMAT JSON) {}", schema::timeline_statement(cap)))
            .bind(DAVE)
            .bind(none)
            .bind("")
            .bind(i64::try_from(schema::climb_keys(cap)).expect("k"))
            .fetch_one(&pool)
            .await
            .expect("explain timeline");
        let mut bad = Vec::new();
        rescanned(&plan[0]["Plan"], &mut bad);
        println!("T-62 timeline batch, cap {cap}: c estimated {:?} (planned, actual)", c_estimate(&plan[0]["Plan"]));
        w.check(bad.is_empty(), format!("timeline at cap {cap} rescans: {bad:?}"));
    }
    w.finish().await;
}

/// T-63 (shared-walk §7): with K forced to 1, 3 and 7, the sliced
/// `quoteCount`, `unread`, `replyCount` and `prefilter_uris` equal the
/// single statement, also with no private key at all; `notification_kept`
/// runs once per candidate across the slices; and a block written between
/// two slices changes nothing, since every slice reads the first one's
/// snapshot.
#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn t63_slices_equal_the_single_statement() {
    let Some(mut w) = Walk::new_in("t63-slices", PublicMode::On, Layout::SamePds, Some("followers"), &[], &[], Arc::new(NoHeads)).await else {
        return;
    };
    let (t, aud) = (w.root.clone(), w.aud.clone());
    let pool = w.f.pool.clone();
    let x = format!("at://{CAROL}/{PUBLIC_POST}/t63x");
    let branch = w.f.private_post(ALICE, "3kt63branch2", &aud, Some((&x, &x))).await;
    let mut posts = vec![t.clone(), branch.clone()];
    let mut parent = t.clone();
    for i in 0..12 {
        let author = [BOB, CAROL, ALICE][i % 3];
        let uri = if i % 4 == 3 {
            w.f.private_post(author, &format!("3kt63b{i:02}2222"), &aud, Some((&x, &branch))).await
        } else {
            w.f.private_post(author, &format!("3kt63t{i:02}2222"), &aud, Some((&t, &parent))).await
        };
        if i % 2 == 0 {
            parent = uri.clone();
        }
        posts.push(uri);
    }
    for (i, p) in posts.iter().enumerate().skip(2) {
        exec(&pool, &format!("UPDATE private_posts SET quote_uri = '{}' WHERE uri = '{p}'", posts[i % 2])).await;
    }
    for (i, who) in [BOB, CAROL].iter().enumerate() {
        exec(&pool, &format!(
            "INSERT INTO private_quote_refs (uri, author_did, cid, rev, quoted_uri, created_at) \
             VALUES ('at://{who}/{PUBLIC_POST}/t63q{i}', '{who}', 'bafyq', '3k', '{t}', now())"
        )).await;
    }
    exec(&pool, &format!(
        "INSERT INTO private_notifications (recipient_did, author_did, reason, record_uri, record_cid) \
         SELECT '{DAVE}', author_did, 'mention', uri, cid FROM private_posts WHERE uri <> '{t}'"
    )).await;
    exec(&pool, &format!(
        "INSERT INTO private_notifications (recipient_did, author_did, reason, reason_subject, record_uri, record_cid) \
         VALUES ('{DAVE}', '{BOB}', 'like', '{t}', 'at://{BOB}/app.bsky.feed.like/t63l', 'bafyl')"
    )).await;
    w.f.ready_all().await;

    // The single statements.
    let single_unread = |reader: &'static str| {
        let pool = pool.clone();
        async move { sqlx::query_scalar::<_, i64>(&schema::unread_statement(CAP)).bind(reader).fetch_one(&pool).await.expect("unread") }
    };
    let single_counts = |uris: Vec<String>| {
        let pool = pool.clone();
        async move {
            let r: Vec<(String, i64)> = sqlx::query_as(&schema::reply_counts_statement(CAP)).bind(DAVE).bind(&uris).fetch_all(&pool).await.expect("replies");
            let q: Vec<(String, i64)> = sqlx::query_as(&schema::quote_counts_statement(CAP)).bind(DAVE).bind(&uris).fetch_all(&pool).await.expect("quotes");
            (r.into_iter().collect::<BTreeMap<_, _>>(), q.into_iter().collect::<BTreeMap<_, _>>())
        }
    };
    let split = |c: &HashMap<String, schema::Counts>| {
        let r: BTreeMap<String, i64> = c.iter().filter(|(_, v)| v.reply > 0).map(|(u, v)| (u.clone(), v.reply)).collect();
        let q: BTreeMap<String, i64> = c.iter().filter(|(_, v)| v.quote > 0).map(|(u, v)| (u.clone(), v.quote)).collect();
        (r, q)
    };
    let mut single_prefilter = schema::prefilter_uris_sliced(&pool, DAVE, &posts, CAP, usize::MAX).await.expect("prefilter");
    single_prefilter.sort();
    let want_unread = single_unread(DAVE).await;
    let want_counts = single_counts(posts.clone()).await;
    w.check(want_unread > 1 && !want_counts.1.is_empty(), format!("the fixture counts something: {want_unread}, {want_counts:?}"));
    for k in [1usize, 3, 7] {
        let got = schema::unread_count_sliced(&pool, DAVE, CAP, k, None).await.expect("sliced unread");
        w.check(got == want_unread, format!("unread at K {k}: {got}, single {want_unread}"));
        let got = split(&schema::counts_sliced(&pool, DAVE, &posts, CAP, k, None).await.expect("sliced counts"));
        w.check(got == want_counts, format!("counts at K {k}: {got:?}, single {want_counts:?}"));
        let mut got = schema::prefilter_uris_sliced(&pool, DAVE, &posts, CAP, k).await.expect("sliced prefilter");
        got.sort();
        w.check(got == single_prefilter, format!("prefilter at K {k}: {got:?}, single {single_prefilter:?}"));
    }
    // No private key: Eve's notifications are a public record's only;
    // Bob's post has public quote references only.
    exec(&pool, &format!(
        "INSERT INTO private_notifications (recipient_did, author_did, reason, reason_subject, record_uri, record_cid) \
         VALUES ('{EVE}', '{BOB}', 'like', '{t}', 'at://{BOB}/app.bsky.feed.like/t63c', 'bafyc')"
    )).await;
    let lone = w.f.private_post(BOB, "3kt63lone222", &aud, None).await;
    exec(&pool, &format!(
        "INSERT INTO private_quote_refs (uri, author_did, cid, rev, quoted_uri, created_at) \
         VALUES ('at://{CAROL}/{PUBLIC_POST}/t63lone', '{CAROL}', 'bafyq', '3k', '{lone}', now())"
    )).await;
    let want = sqlx::query_scalar::<_, i64>(&schema::unread_statement(CAP)).bind(EVE).fetch_one(&pool).await.expect("unread");
    let lone_counts = single_counts(vec![lone.clone()]).await;
    for k in [1usize, 3] {
        let got = schema::unread_count_sliced(&pool, EVE, CAP, k, None).await.expect("unread");
        w.check(got == want && want == 1, format!("public-only unread at K {k}: {got}, single {want}"));
        let got = split(&schema::counts_sliced(&pool, DAVE, std::slice::from_ref(&lone), CAP, k, None).await.expect("counts"));
        w.check(got == lone_counts && got.1.get(&lone) == Some(&1), format!("public-only quotes at K {k}: {got:?}, single {lone_counts:?}"));
    }
    // `notification_kept` at most once per candidate: the thread-mute
    // probe's loops over the slices (the key scan reads no such term).
    let ids: Vec<i64> = sqlx::query_scalar(&schema::unread_keys_statement()).bind(DAVE).fetch_all(&pool).await.expect("ids");
    let mut loops = 0u64;
    for slice in ids.chunks(3) {
        let plan: Value = sqlx::query_scalar(&format!("EXPLAIN (ANALYZE, FORMAT JSON) {}", schema::unread_slice_statement(CAP)))
            .bind(DAVE)
            .bind(slice)
            .fetch_one(&pool)
            .await
            .expect("explain");
        fn sum(node: &Value) -> u64 {
            let own = if node["Relation Name"] == "thread_mutes" { node["Actual Loops"].as_u64().unwrap_or(0) } else { 0 };
            own + node["Plans"].as_array().into_iter().flatten().map(sum).sum::<u64>()
        }
        loops += sum(&plan[0]["Plan"]);
    }
    let candidates = u64::try_from(ids.len()).expect("ids");
    println!("T-63: notification_kept's thread-mute probe ran {loops} times over {candidates} candidates");
    w.check(loops <= candidates && candidates > 3, format!("notification_kept ran {loops} times over {candidates} candidates"));
    w.check(!schema::unread_keys_statement().contains("thread_mutes"), "the key scan reads notification_kept".to_string());
    // A block written between two slices: every slice reads the first
    // statement's snapshot, so the result is the single statement's before
    // the write.
    let block = block_sql(DAVE, BOB);
    for k in [1usize, 3] {
        let got = schema::unread_count_sliced(&pool, DAVE, CAP, k, Some(&block)).await.expect("unread");
        w.check(got == want_unread, format!("unread at K {k} with a block between slices: {got}, before {want_unread}"));
        exec(&pool, "DELETE FROM blocks").await;
        let got = split(&schema::counts_sliced(&pool, DAVE, &posts, CAP, k, Some(&block)).await.expect("counts"));
        w.check(got == want_counts, format!("counts at K {k} with a block between slices: {got:?}, before {want_counts:?}"));
        exec(&pool, "DELETE FROM blocks").await;
    }
    exec(&pool, &block).await;
    let after = single_unread(DAVE).await;
    w.check(after < want_unread, format!("the block changes the single statement: {after} against {want_unread}"));
    w.finish().await;
}
