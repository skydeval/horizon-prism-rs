//! B-5, S12's release gate (docs/internal/design/private-threads.md §4,
//! C-T14 (a)), and T-54. Every test here is ignored: each builds the
//! synthetic snapshot (50 000 private posts, 5 000 members, 2 000
//! audiences) and its fixtures in a fresh database, and prints what it
//! measures. Run on the reference host with a release build:
//! `cargo test --release b5_ -- --ignored --nocapture --test-threads 1`,
//! and with `--features bench-heads` for the stale-head chain.
//!
//! - **Part (1), T-54** (`b5_row_class_ratios`): per row class, the shared
//!   and local buffers of S12's statement per `c` row against HEAD's own
//!   candidate statement (its prefilter as it stood before S12, kept in
//!   `schema`) over the same rows, at §3.5's bounds.
//! - **Part (3)** (`b5_absolute_floors`): each endpoint's p99, unfloored and
//!   timed at the client, under its floor, on the base snapshot, the new
//!   shapes and the named adversarial fixtures, at concurrency 8; heads
//!   fresh, and no head fetched (a counting source must see no call).
//! - **The stale-head chain** (`b5_stale_head_chain`, `bench-heads` only):
//!   `getPostThread` at the bottom of a 16-deep chain of 16 authors whose
//!   heads are all stale, answered by `BenchHeadSource` after 450 ms, at
//!   concurrency 1, under 1 200 ms.
//! - **Part (2)**, per request against a HEAD build, is not run here: it
//!   needs HEAD's handlers serving the same database, which a test of this
//!   build cannot load.

use std::sync::atomic::{AtomicUsize, Ordering};

use futures::StreamExt;

use super::*;
use crate::config::PrivateReadFloors;

/// The base snapshot (§4, "Host and data").
const BASE_MEMBERS: i64 = 5_000;
const BASE_AUDIENCES: i64 = 2_000;
const BASE_POSTS: i64 = 50_000;

/// B-5's member DID `i`: `did:plc:` and 24 base-32 letters (hex digits
/// mapped to `k`–`t`). [`MEMBER_SQL`] is the same in SQL over `g`.
fn member(i: i64) -> String {
    let letters: String = format!("{i:024x}")
        .chars()
        .map(|c| match c {
            '0'..='9' => char::from(b'k' + (c as u8 - b'0')),
            other => other,
        })
        .collect();
    format!("did:plc:{letters}")
}

const MEMBER_SQL: &str = "('did:plc:' || translate(lpad(to_hex(g), 24, '0'), '0123456789', 'klmnopqrst'))";

/// Samples per endpoint per fixture: `HPRS_B5_SAMPLES`, else 1 000.
fn samples() -> usize {
    std::env::var("HPRS_B5_SAMPLES").ok().and_then(|s| s.parse().ok()).unwrap_or(1000)
}

/// A head source that answers nothing and counts its calls: part (3)'s
/// fixtures keep every head fresh, so any call is a failure of the regime.
#[derive(Default)]
struct Counting(AtomicUsize);

#[async_trait]
impl HeadSource for Counting {
    async fn latest_rev(&self, _pds: &str, _did: &str, _timeout: Duration) -> Result<String, String> {
        self.0.fetch_add(1, Ordering::SeqCst);
        Err("no fetch in the bench regime".into())
    }
}

/// Write the base snapshot and refresh the planner's statistics.
async fn base_snapshot(pool: &PgPool) {
    let t0 = std::time::Instant::now();
    let m = MEMBER_SQL;
    for sql in [
        format!(
            "INSERT INTO actors (did, handle, display_name, pds_endpoint, profile_record) \
             SELECT {m}, 'b5m' || g || '.test', 'b5m' || g, 'https://pds.example.com', '{{\"createdAt\":\"2026-01-01T00:00:00Z\"}}'::jsonb \
             FROM generate_series(1, {BASE_MEMBERS}) g"
        ),
        format!("INSERT INTO members (did, source, ready_at) SELECT {m}, 'admin', now() FROM generate_series(1, {BASE_MEMBERS}) g"),
        format!("INSERT INTO scope_dids (did, reasons) SELECT {m}, 1 FROM generate_series(1, {BASE_MEMBERS}) g"),
        format!(
            "INSERT INTO sync_state (did, applied_rev, head_rev, head_checked_at) \
             SELECT {m}, '3kzzzzzzzzzz2', '{REV}', now() FROM generate_series(1, {BASE_MEMBERS}) g"
        ),
        // Each audience lists the four readers and 47 members.
        format!(
            "INSERT INTO private_audiences (uri, owner_did, rkey, cid, rev, mode, members, display_filled) \
             SELECT 'at://' || d || '/{NSID_AUDIENCE}/b5aud', d, 'b5aud', 'bafyb5', '{REV}', 'list', \
                    ARRAY['{ALICE}', '{BOB}', '{CAROL}', '{DAVE}'] || ARRAY( \
                      SELECT 'did:plc:' || translate(lpad(to_hex((s.g * 7 + k) % {BASE_MEMBERS} + 1), 24, '0'), '0123456789', 'klmnopqrst') \
                      FROM generate_series(1, 47) k), true \
             FROM (SELECT g, {m} AS d FROM generate_series(1, {BASE_AUDIENCES}) g) s"
        ),
        // Posts by the audiences' owners, under their own audience: seven in
        // ten top-level, the rest replies in the author's first thread.
        format!(
            "INSERT INTO private_posts (uri, author_did, rkey, cid, rev, audience_uri, reply_root_uri, reply_parent_uri, \
               plaintext_text, created_at, display_filled) \
             SELECT 'at://' || d || '/{NSID_POST_PRIVATE}/b5p' || g, d, 'b5p' || g, 'bafyb5', '{REV}', \
                    'at://' || d || '/{NSID_AUDIENCE}/b5aud', \
                    CASE WHEN g > {BASE_AUDIENCES} AND g % 10 >= 7 THEN 'at://' || d || '/{NSID_POST_PRIVATE}/b5p' || ((g - 1) % {BASE_AUDIENCES} + 1) END, \
                    CASE WHEN g > {BASE_AUDIENCES} AND g % 10 >= 7 THEN 'at://' || d || '/{NSID_POST_PRIVATE}/b5p' || (g - {BASE_AUDIENCES}) END, \
                    'b5 base post ' || g, now() - make_interval(secs => g), true \
             FROM (SELECT g, 'did:plc:' || translate(lpad(to_hex((g - 1) % {BASE_AUDIENCES} + 1), 24, '0'), '0123456789', 'klmnopqrst') AS d \
                   FROM generate_series(1, {BASE_POSTS}) g) s"
        ),
        format!("INSERT INTO private_owner_ready (did, ready_at) SELECT {m}, now() FROM generate_series(1, {BASE_AUDIENCES}) g"),
        format!(
            "INSERT INTO follows (uri, author_did, subject_did, rev, created_at) \
             SELECT 'at://{DAVE}/app.bsky.graph.follow/b5f' || g, '{DAVE}', {m}, '3k', now() FROM generate_series(1, 200) g"
        ),
    ] {
        exec(pool, &sql).await;
    }
    exec(pool, "ANALYZE").await;
    println!("B-5 base snapshot: {BASE_MEMBERS} members, {BASE_AUDIENCES} audiences, {BASE_POSTS} private posts in {:?}", t0.elapsed());
}

/// Rows written in one statement each: B-5's fixtures, as plaintext posts.
struct Bulk {
    tag: String,
    rows: Vec<[Option<String>; 5]>,
}

impl Bulk {
    fn new(tag: &str) -> Self {
        Self { tag: tag.to_string(), rows: Vec::new() }
    }

    /// A post by `author` under `audience`, replying to `(root, parent)`.
    fn post(&mut self, author: &str, audience: &str, reply: Option<(&str, &str)>) -> String {
        let uri = format!("at://{author}/{NSID_POST_PRIVATE}/{}{:06}", self.tag, self.rows.len());
        let (root, parent) = reply.map_or((None, None), |(r, p)| (Some(r.to_string()), Some(p.to_string())));
        self.rows.push([Some(uri.clone()), Some(author.to_string()), Some(audience.to_string()), root, parent]);
        uri
    }

    async fn write(&mut self, pool: &PgPool) {
        let col = |i: usize| self.rows.iter().map(|r| r[i].clone()).collect::<Vec<Option<String>>>();
        sqlx::query(&format!(
            "INSERT INTO private_posts (uri, author_did, rkey, cid, rev, audience_uri, reply_root_uri, reply_parent_uri, \
               plaintext_text, created_at, display_filled) \
             SELECT u, a, split_part(u, '/', 5), 'bafyb5', '{REV}', aud, root, parent, 'b5 ' || split_part(u, '/', 5), \
                    now() - make_interval(secs => ord::double precision / 1000), true \
             FROM unnest($1::text[], $2::text[], $3::text[], $4::text[], $5::text[]) WITH ORDINALITY AS t(u, a, aud, root, parent, ord)"
        ))
        .bind(col(0))
        .bind(col(1))
        .bind(col(2))
        .bind(col(3))
        .bind(col(4))
        .execute(pool)
        .await
        .expect("bulk");
        self.rows.clear();
    }
}

/// An audience row of `owner`'s, `list` of `members`.
async fn audience_row(pool: &PgPool, owner: &str, rkey: &str, members: &[String]) -> String {
    let uri = format!("at://{owner}/{NSID_AUDIENCE}/{rkey}");
    sqlx::query(
        "INSERT INTO private_audiences (uri, owner_did, rkey, cid, rev, mode, members, display_filled) \
         VALUES ($1, $2, $3, 'bafyb5', $4, 'list', $5, true)",
    )
    .bind(&uri)
    .bind(owner)
    .bind(rkey)
    .bind(REV)
    .bind(members)
    .execute(pool)
    .await
    .expect("audience");
    uri
}

/// The buffers per `c` row of `sql` (reader `$1`, URIs `$2`): shared and
/// local, hit and read, of the whole statement, on its second run.
async fn buffers_per_row(pool: &PgPool, sql: &str, reader: &str, uris: &[String]) -> f64 {
    let mut total = 0.0;
    for _ in 0..2 {
        let plan: Value = sqlx::query_scalar(&format!("EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) {sql}"))
            .bind(reader)
            .bind(uris)
            .fetch_one(pool)
            .await
            .expect("explain");
        let p = &plan[0]["Plan"];
        total = ["Shared Hit Blocks", "Shared Read Blocks", "Local Hit Blocks", "Local Read Blocks"]
            .iter()
            .map(|k| p[*k].as_f64().unwrap_or(0.0))
            .sum();
    }
    #[allow(clippy::cast_precision_loss)]
    let rows = uris.len() as f64;
    total / rows
}

/// §3.5's bound rule: the probe ratio plus 0.25, rounded up to 0.1.
fn bound_of(ratio: f64) -> f64 {
    ((ratio + 0.25) * 10.0).ceil() / 10.0
}

/// One row class of §3.5: its rows, the reader, whether they pass, and
/// its bound.
struct Class {
    name: String,
    rows: Vec<String>,
    passes: bool,
    bound: f64,
    cap: u32,
}

/// Part (1) and T-54 (§3.5, §4): every row class, passing and failing, on
/// the base snapshot, S12's buffers per row against HEAD's, at the
/// table's bounds; and the formula `4 + min(k, max_chain)` of a branch
/// reply with the reader outside G under distinct parents, at k = 4, 8 and
/// 16, and past the cap at 16 and 100.
#[tokio::test]
#[ignore = "B-5 part (1) and T-54; run with --release --ignored --nocapture"]
async fn b5_row_class_ratios() {
    let Some(mut w) = Walk::new_in("b5-classes", PublicMode::On, Layout::SamePds, Some("followers"), &[], &[], Arc::new(NoHeads)).await else {
        return;
    };
    let pool = w.f.pool.clone();
    base_snapshot(&pool).await;
    let others: Vec<String> = (3001..=3020).map(member).collect();
    let mut inside: Vec<String> = vec![DAVE.into(), BOB.into(), CAROL.into()];
    inside.extend(others.iter().cloned());
    let outside: Vec<String> = inside.iter().filter(|d| d.as_str() != DAVE).cloned().collect();
    let g_in = audience_row(&pool, ALICE, "b5gin", &inside).await;
    let g_out = audience_row(&pool, ALICE, "b5gout", &outside).await;
    let blocker = member(4000);
    let blocker_aud = audience_row(&pool, &blocker, "b5blk", &inside).await;
    exec(&pool, &block_sql(DAVE, &blocker)).await;
    const N: usize = 400;
    let mut b = Bulk::new("b5c");
    let x = |i: usize| format!("at://{CAROL}/{PUBLIC_POST}/b5x{i}");
    let alt = |d: usize| if d % 2 == 1 { BOB } else { CAROL };
    let mut classes: Vec<Class> = Vec::new();
    let mut add = |name: &str, rows: Vec<String>, passes: bool, ratio: f64, cap: u32| {
        classes.push(Class { name: name.to_string(), rows, passes, bound: bound_of(ratio), cap });
    };

    // Passing classes, V inside G.
    let mut rows: Vec<String> = (0..N / 2).map(|_| b.post(ALICE, &g_in, None)).collect();
    rows.extend((0..N / 2).map(|i| b.post(ALICE, &g_in, Some((&x(i), &x(i))))));
    add("top-level, branch root", rows, true, 1.0, CAP);
    let t = b.post(ALICE, &g_in, None);
    add("owner thread reply", (0..N).map(|_| b.post(ALICE, &g_in, Some((&t, &t)))).collect(), true, 1.2, CAP);
    let rows = (0..N)
        .map(|_| {
            let root = b.post(ALICE, &g_in, None);
            let mut parent = root.clone();
            for d in 1..8 {
                parent = b.post(alt(d), &g_in, Some((&root, &parent)));
            }
            b.post(BOB, &g_in, Some((&root, &parent)))
        })
        .collect();
    add("cross-owner thread reply, k = 8, distinct parents", rows, true, 1.0, CAP);
    let root = b.post(ALICE, &g_in, None);
    let mut parent = root.clone();
    for d in 1..8 {
        parent = b.post(alt(d), &g_in, Some((&root, &parent)));
    }
    add("cross-owner thread reply, siblings", (0..N).map(|_| b.post(BOB, &g_in, Some((&root, &parent)))).collect(), true, 1.0, CAP);
    let mut rows = Vec::new();
    for g in 0..N / 50 {
        let top = b.post(ALICE, &g_in, Some((&x(1000 + g), &x(1000 + g))));
        let mut parent = top;
        for other in &others[..7] {
            parent = b.post(other, &g_in, Some((&x(1000 + g), &parent)));
        }
        rows.extend((0..50).map(|_| b.post(ALICE, &g_in, Some((&x(1000 + g), &parent)))));
    }
    add("owner branch reply, 50 siblings, depth 8, 7 other authors", rows, true, 1.39, CAP);
    for (n, ratio, label) in [(4, 8.5, "n = 4, alternating O and B"), (7, 10.9, "n = 7")] {
        let rows = (0..N)
            .map(|i| {
                let xi = x(2000 + i + 1000 * n);
                let mut parent = b.post(ALICE, &g_in, Some((&xi, &xi)));
                for d in 1..8 {
                    let author = if n == 4 { if d % 2 == 1 { BOB } else { ALICE } } else { others[d - 1].as_str() };
                    parent = b.post(author, &g_in, Some((&xi, &parent)));
                }
                b.post(ALICE, &g_in, Some((&xi, &parent)))
            })
            .collect();
        add(&format!("owner branch reply, distinct parents, depth 8, {label}"), rows, true, ratio, CAP);
    }
    let branch_chain = |b: &mut Bulk, xi: &str, aud: &str, depth: usize, last: &str| {
        let mut parent = b.post(ALICE, aud, Some((xi, xi)));
        for d in 1..depth {
            parent = b.post(&others[(d - 1) % others.len()], aud, Some((xi, &parent)));
        }
        b.post(last, aud, Some((xi, &parent)))
    };
    let rows = (0..N).map(|i| branch_chain(&mut b, &x(10_000 + i), &g_in, 8, BOB)).collect();
    add("non-owner branch reply, k = 8, distinct parents", rows, true, 1.04, CAP);

    // Failing classes.
    add("failing: owner row, V outside G", (0..N).map(|_| b.post(ALICE, &g_out, None)).collect(), false, 0.67, CAP);
    let t_out = b.post(ALICE, &g_out, None);
    add("failing: cross-owner thread reply, V outside G", (0..N).map(|_| b.post(BOB, &g_out, Some((&t_out, &t_out)))).collect(), false, 0.67, CAP);
    let mut rows = Vec::new();
    for g in 0..N / 50 {
        let xg = x(20_000 + g);
        let mut parent = b.post(ALICE, &g_out, Some((&xg, &xg)));
        for other in &others[..7] {
            parent = b.post(other, &g_out, Some((&xg, &parent)));
        }
        rows.extend((0..50).map(|_| b.post(BOB, &g_out, Some((&xg, &parent)))));
    }
    add("failing: branch reply, V outside G, 50 siblings, depth 8", rows, false, 0.7, CAP);
    for k in [4usize, 8, 16] {
        #[allow(clippy::cast_precision_loss)]
        let ratio = (4.0 + k.min(16) as f64) / 6.0;
        let rows = (0..N).map(|i| branch_chain(&mut b, &x(30_000 + 1000 * k + i), &g_out, k, BOB)).collect();
        add(&format!("failing: branch reply, V outside G, distinct parents, depth {k}"), rows, false, ratio, CAP);
    }
    let hidden: Vec<String> = (0..N).map(|_| b.post(ALICE, &g_in, None)).collect();
    add("failing: rows the gate hides, V inside G", hidden.clone(), false, 1.0, CAP);
    add("failing: author and V block each other, V inside G", (0..N).map(|_| b.post(&blocker, &blocker_aud, None)).collect(), false, 1.0, CAP);
    // Past the cap: one branch chain of 160 rows, the reader outside G;
    // rows 121 to 160 run the branch-root lookup to the cap.
    let deep_x = x(90_000);
    let mut parent = b.post(ALICE, &g_out, Some((&deep_x, &deep_x)));
    let mut deep = Vec::new();
    for d in 1..=160 {
        parent = b.post(&others[d % others.len()], &g_out, Some((&deep_x, &parent)));
        if d > 120 {
            deep.push(parent.clone());
        }
    }
    for cap in [16u32, 100] {
        add(&format!("failing: past the cap at {cap}, V outside G"), deep.clone(), false, (4.0 + f64::from(cap)) / 6.0, cap);
    }
    b.write(&pool).await;
    for uri in &hidden {
        exec(&pool, &format!("INSERT INTO labels (subject, val) VALUES ('{uri}', '!hide')")).await;
    }
    exec(&pool, "ANALYZE").await;

    let head = |n: usize| format!("SELECT p.uri FROM {} WHERE p.uri = ANY($2) AND {} LIMIT {n}", schema::PREFILTER_FROM, schema::head_prefilter());
    println!("B-5 part (1) / T-54: buffers per row, S12 against HEAD, reader Dave");
    for c in &classes {
        let s12 = buffers_per_row(&pool, &schema::prefilter_uris_statement(c.cap), DAVE, &c.rows).await;
        let base = buffers_per_row(&pool, &head(c.rows.len()), DAVE, &c.rows).await;
        let ratio = s12 / base;
        let served = schema::prefilter_uris(&pool, DAVE, &c.rows, c.cap).await.expect("prefilter").len();
        println!(
            "  {:<70} rows {:>4}  S12 {:>7.2}  HEAD {:>6.2}  ratio {:>5.2}  bound {:>5.1}  {}",
            c.name,
            c.rows.len(),
            s12,
            base,
            ratio,
            c.bound,
            if ratio <= c.bound { "ok" } else { "OVER" }
        );
        // A class over its bound prints both plans, for the record.
        if ratio > c.bound {
            for (which, sql) in [("S12", schema::prefilter_uris_statement(c.cap)), ("HEAD", head(c.rows.len()))] {
                let plan: Vec<String> = sqlx::query_scalar(&format!("EXPLAIN (ANALYZE, BUFFERS) {sql}"))
                    .bind(DAVE)
                    .bind(&c.rows)
                    .fetch_all(&pool)
                    .await
                    .expect("plan");
                println!("---- {which} plan, {}\n{}", c.name, plan.join("\n"));
            }
        }
        w.check(ratio <= c.bound, format!("{}: ratio {ratio:.2} over its bound {}", c.name, c.bound));
        w.check((served == c.rows.len()) == c.passes && (served == 0) != c.passes, format!("{}: {served} of {} served", c.name, c.rows.len()));
    }
    w.finish().await;
}

/// Each endpoint's samples for `reader`, at `concurrency`: the client-side
/// time of each 200, sorted.
async fn sample(base: &str, method: &str, query: &str, reader: &str, n: usize, concurrency: usize) -> Vec<Duration> {
    let mut took: Vec<Duration> = futures::stream::iter(0..n)
        .map(|_| async move {
            let (status, body, t) = get(base, method, query, reader, key_of(reader)).await;
            assert_eq!(status, 200, "{method}?{query}: {body}");
            t
        })
        .buffer_unordered(concurrency)
        .collect()
        .await;
    took.sort();
    took
}

fn percentile(sorted: &[Duration], p: usize) -> Duration {
    sorted[(sorted.len() * p / 100).min(sorted.len() - 1)]
}

/// An endpoint's floor from the default `[private_read.floor_ms]`.
fn floor_of(method: &str) -> Duration {
    let f = PrivateReadFloors::default();
    let ms = match method {
        POSTS => f.get_posts,
        THREAD => f.get_post_thread,
        FEED => f.get_author_feed,
        TIMELINE => f.get_timeline,
        GET_BOOKMARKS => f.get_bookmarks,
        GET_LIKES => f.get_likes,
        GET_REPOSTED_BY => f.get_reposted_by,
        GET_QUOTES => f.get_quotes,
        LIST_NOTIFICATIONS => f.list_notifications,
        GET_UNREAD_COUNT => f.get_unread_count,
        GET_REPLIES => f.get_replies,
        _ => f.search_posts,
    };
    Duration::from_millis(ms)
}

/// One part (3) fixture's run: each `(label, method, query, gated)` sampled
/// against a fresh server, printed with its floor; a gated p99 over its
/// floor is a failure.
async fn run_fixture(w: &mut Walk, fixture: &str, read: &PrivateReadConfig, heads: &Counting, cases: &[(&str, &str, String, bool)]) {
    let base = serve_live(w.f.state_bounded(read)).await;
    let n = samples();
    for (label, method, query, gated) in cases {
        exec(&w.f.pool, "UPDATE sync_state SET head_checked_at = now()").await;
        let took = sample(&base, method, query, DAVE, n, 8).await;
        let (p50, p99) = (percentile(&took, 50), percentile(&took, 99));
        // One request at a time: what the request costs without the
        // in-process harness's own queueing.
        let alone = sample(&base, method, query, DAVE, n.min(100), 1).await;
        let floor = floor_of(method);
        println!(
            "  B-5 (3) {fixture:<46} {label:<34} alone p50 {:>6.1} ms  c8 p50 {:>7.1} ms  p99 {:>7.1} ms  floor {:>5} ms  {}",
            percentile(&alone, 50).as_secs_f64() * 1000.0,
            p50.as_secs_f64() * 1000.0,
            p99.as_secs_f64() * 1000.0,
            floor.as_millis(),
            match (gated, p99 < floor) {
                (false, _) => "reported",
                (true, true) => "ok",
                (true, false) => "OVER",
            }
        );
        if *gated {
            w.check(p99 < floor, format!("{fixture}, {label}: p99 {p99:?} over its floor {floor:?}"));
        }
    }
    let calls = heads.0.load(Ordering::SeqCst);
    w.check(calls == 0, format!("{fixture}: {calls} head fetches in the bench regime"));
}

/// A fresh database with the base snapshot, the readers and a counting
/// head source.
async fn part3_walk(test: &str) -> Option<(Walk, Arc<Counting>)> {
    let heads = Arc::new(Counting::default());
    let w = Walk::new_in(test, PublicMode::On, Layout::SamePds, Some("followers"), &[], &[], Arc::clone(&heads) as Arc<dyn HeadSource>).await?;
    base_snapshot(&w.f.pool).await;
    w.f.ready_all().await;
    Some((w, heads))
}

/// Mark `dids` owners ready (B-5's fixture authors write no audience row of
/// their own unless named).
async fn ready_dids(pool: &PgPool, dids: &[String]) {
    sqlx::query("INSERT INTO private_owner_ready (did, ready_at) SELECT unnest($1::text[]), now() ON CONFLICT DO NOTHING")
        .bind(dids)
        .execute(pool)
        .await
        .expect("ready");
}

/// Part (3): each endpoint's p99 under its floor on the base snapshot, the
/// new shapes and the non-adversarial fixtures, and the named adversarial
/// fixtures reported (gated where §4 gates them).
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore = "B-5 part (3); run with --release --ignored --nocapture"]
async fn b5_absolute_floors() {
    let default_read = PrivateReadConfig::default();
    println!("B-5 part (3): {} samples per endpoint, concurrency 8, heads fresh", samples());
    // Every fixture runs, and the failures are reported together.
    let mut failures: Vec<String> = Vec::new();

    // 1. The base snapshot: all twelve floored endpoints.
    let Some((mut w, heads)) = part3_walk("b5-base").await else {
        return;
    };
    let pool = w.f.pool.clone();
    let p1 = format!("at://{}/{NSID_POST_PRIVATE}/b5p1", member(1));
    // Post 6 007 is member 7's third reply in its first thread.
    let p1_reply = format!("at://{}/{NSID_POST_PRIVATE}/b5p6007", member(7));
    for (i, liker) in (1..=100).map(member).enumerate() {
        for table in ["likes", "reposts"] {
            if table == "reposts" && i >= 50 {
                continue;
            }
            exec(&pool, &format!("INSERT INTO {table} (uri, author_did, subject_uri, subject_cid, rev, created_at) VALUES ('at://{liker}/x/b5{table}{i}', '{liker}', '{p1}', 'bafyb5', '3k', now())")).await;
        }
    }
    let mut b = Bulk::new("b5q");
    for i in 1..=20 {
        let q = b.post(&member(i), &format!("at://{}/{NSID_AUDIENCE}/b5aud", member(i)), None);
        b.write(&pool).await;
        exec(&pool, &format!("UPDATE private_posts SET quote_uri = '{p1}' WHERE uri = '{q}'")).await;
    }
    exec(&pool, &format!(
        "INSERT INTO private_bookmarks (viewer_did, post_uri, post_cid) SELECT '{DAVE}', uri, cid FROM private_posts WHERE uri LIKE '%/b5p%' ORDER BY uri LIMIT 50"
    )).await;
    exec(&pool, &format!(
        "INSERT INTO private_notifications (recipient_did, author_did, reason, record_uri, record_cid) \
         SELECT '{DAVE}', author_did, 'mention', uri, cid FROM private_posts WHERE uri LIKE '%/b5p%' ORDER BY uri LIMIT 200"
    )).await;
    exec(&pool, "ANALYZE").await;
    let posts25: Vec<String> = (1..=25).map(|i| format!("uris=at://{}/{NSID_POST_PRIVATE}/b5p{i}", member(i))).collect();
    let cases = [
        ("getPosts (25)", POSTS, posts25.join("&"), true),
        ("getPostThread (a thread root)", THREAD, format!("uri={p1}"), true),
        ("getAuthorFeed", FEED, format!("actor={}", member(1)), true),
        ("getTimeline", TIMELINE, String::new(), true),
        ("getReplies", GET_REPLIES, format!("uri={p1}"), true),
        ("getQuotes", GET_QUOTES, format!("uri={p1}"), true),
        ("getLikes", GET_LIKES, format!("uri={p1}"), true),
        ("getRepostedBy", GET_REPOSTED_BY, format!("uri={p1}"), true),
        ("getBookmarks", GET_BOOKMARKS, String::new(), true),
        ("listNotifications", LIST_NOTIFICATIONS, String::new(), true),
        ("getUnreadCount", GET_UNREAD_COUNT, String::new(), true),
        ("searchPosts", SEARCH, "q=b5".to_string(), true),
        ("getPostThread (a reply)", THREAD, format!("uri={p1_reply}"), true),
    ];
    run_fixture(&mut w, "base snapshot", &default_read, &heads, &cases).await;
    failures.extend(w.drain().await);

    // 2. A 16-deep thread and a 16-deep branch of readable posts by 16
    // authors, read at the bottom.
    let Some((mut w, heads)) = part3_walk("b5-deep").await else {
        return;
    };
    let pool = w.f.pool.clone();
    let authors: Vec<String> = (3001..=3015).map(member).collect();
    let mut members: Vec<String> = vec![DAVE.into(), BOB.into(), CAROL.into()];
    members.extend(authors.iter().cloned());
    let g = audience_row(&pool, ALICE, "b5deep", &members).await;
    let mut b = Bulk::new("b5d");
    let root = b.post(ALICE, &g, None);
    let mut thread_bottom = root.clone();
    for a in &authors {
        thread_bottom = b.post(a, &g, Some((&root, &thread_bottom)));
    }
    thread_bottom = b.post(BOB, &g, Some((&root, &thread_bottom)));
    let x = format!("at://{CAROL}/{PUBLIC_POST}/b5deepx");
    let mut branch_bottom = b.post(ALICE, &g, Some((&x, &x)));
    for a in &authors {
        branch_bottom = b.post(a, &g, Some((&x, &branch_bottom)));
    }
    branch_bottom = b.post(BOB, &g, Some((&x, &branch_bottom)));
    b.write(&pool).await;
    let mut ready: Vec<String> = authors.clone();
    ready.extend([ALICE.into(), BOB.into()]);
    ready_dids(&pool, &ready).await;
    let cases = [
        ("getPostThread, thread bottom", THREAD, format!("uri={thread_bottom}"), true),
        ("getPostThread, branch bottom", THREAD, format!("uri={branch_bottom}"), true),
    ];
    run_fixture(&mut w, "16-deep thread and branch, 16 authors", &default_read, &heads, &cases).await;
    failures.extend(w.drain().await);

    // 3. An 8-deep thread with a feed page, a search page and a getReplies
    // page on it.
    let Some((mut w, heads)) = part3_walk("b5-eight").await else {
        return;
    };
    let pool = w.f.pool.clone();
    let g = audience_row(&pool, ALICE, "b5eight", &[DAVE.into(), BOB.into(), CAROL.into()]).await;
    let mut b = Bulk::new("b5e");
    let root = b.post(ALICE, &g, None);
    let mut node = root.clone();
    for d in 1..8 {
        node = b.post(if d % 2 == 1 { BOB } else { CAROL }, &g, Some((&root, &node)));
    }
    for _ in 0..60 {
        b.post(BOB, &g, Some((&root, &node)));
    }
    b.write(&pool).await;
    ready_dids(&pool, &[ALICE.into(), BOB.into(), CAROL.into()]).await;
    exec(&pool, &follow_sql(DAVE, BOB)).await;
    let cases = [
        ("getAuthorFeed of the replier", FEED, format!("actor={BOB}"), true),
        ("getTimeline", TIMELINE, String::new(), true),
        ("searchPosts", SEARCH, "q=b5e".to_string(), true),
        ("getReplies (60 at depth 8)", GET_REPLIES, format!("uri={node}"), true),
    ];
    run_fixture(&mut w, "8-deep thread", &default_read, &heads, &cases).await;
    failures.extend(w.drain().await);

    // 4. A `list` G of 10 000 members.
    let Some((mut w, heads)) = part3_walk("b5-big-list").await else {
        return;
    };
    let pool = w.f.pool.clone();
    let mut big: Vec<String> = (1..=9_997).map(member).collect();
    big.extend([DAVE.into(), BOB.into(), CAROL.into()]);
    let g = audience_row(&pool, ALICE, "b5big", &big).await;
    let mut b = Bulk::new("b5l");
    let mut uris = Vec::new();
    for i in 0..50 {
        let root = b.post(ALICE, &g, None);
        uris.push(root.clone());
        for _ in 0..(i % 4) {
            b.post(BOB, &g, Some((&root, &root)));
        }
    }
    b.write(&pool).await;
    ready_dids(&pool, &[ALICE.into(), BOB.into()]).await;
    let posts25: Vec<String> = uris.iter().take(25).map(|u| format!("uris={u}")).collect();
    let cases = [
        ("getPosts (25)", POSTS, posts25.join("&"), true),
        ("getAuthorFeed", FEED, format!("actor={ALICE}"), true),
        ("getTimeline", TIMELINE, String::new(), true),
        ("getPostThread", THREAD, format!("uri={}", uris[3]), true),
    ];
    run_fixture(&mut w, "a list G of 10 000 members", &default_read, &heads, &cases).await;
    failures.extend(w.drain().await);

    // 5. A timeline and an author feed over branch conversations by followed
    // authors, V outside each branch root's audience, every class row at
    // depth k = 16.
    let Some((mut w, heads)) = part3_walk("b5-outside").await else {
        return;
    };
    let pool = w.f.pool.clone();
    let mut b = Bulk::new("b5o");
    let writers: Vec<String> = (3101..=3120).map(member).collect();
    for (i, wr) in writers.iter().enumerate() {
        let g = audience_row(&pool, wr, "b5out", &[BOB.into(), CAROL.into()]).await;
        exec(&pool, &follow_sql(DAVE, wr)).await;
        for c in 0..5 {
            let x = format!("at://{CAROL}/{PUBLIC_POST}/b5o{i}x{c}");
            let mut parent = b.post(wr, &g, Some((&x, &x)));
            for _ in 1..=16 {
                parent = b.post(wr, &g, Some((&x, &parent)));
            }
        }
    }
    b.write(&pool).await;
    ready_dids(&pool, &writers).await;
    let cases = [
        ("getTimeline", TIMELINE, String::new(), true),
        ("getAuthorFeed", FEED, format!("actor={}", writers[0]), true),
    ];
    run_fixture(&mut w, "branch conversations, V outside, k = 16", &default_read, &heads, &cases).await;
    failures.extend(w.drain().await);

    // 6. O's author feed and a timeline following O, over 8-deep branch
    // conversations alternating O and B, V inside G, every O row at depth 8.
    let Some((mut w, heads)) = part3_walk("b5-alternating").await else {
        return;
    };
    let pool = w.f.pool.clone();
    let g = audience_row(&pool, ALICE, "b5alt", &[DAVE.into(), BOB.into(), CAROL.into()]).await;
    let mut b = Bulk::new("b5a");
    for c in 0..60 {
        let x = format!("at://{CAROL}/{PUBLIC_POST}/b5ax{c}");
        let mut parent = b.post(ALICE, &g, Some((&x, &x)));
        for d in 1..=8 {
            parent = b.post(if d % 2 == 1 { BOB } else { ALICE }, &g, Some((&x, &parent)));
        }
    }
    b.write(&pool).await;
    ready_dids(&pool, &[ALICE.into(), BOB.into()]).await;
    let cases = [
        ("getAuthorFeed of O", FEED, format!("actor={ALICE}"), true),
        ("getTimeline following O", TIMELINE, String::new(), true),
    ];
    run_fixture(&mut w, "8-deep branches alternating O and B, V inside", &default_read, &heads, &cases).await;
    failures.extend(w.drain().await);

    // Adversarial: 5 000 owner branch replies under one node, each notifying
    // V; and 5 000 under distinct parents at depth 8 under V's own public
    // post, each notifying V (gated at getUnreadCount's floor).
    let Some((mut w, heads)) = part3_walk("b5-floods").await else {
        return;
    };
    let pool = w.f.pool.clone();
    let g = audience_row(&pool, ALICE, "b5flood", &[DAVE.into(), BOB.into(), CAROL.into()]).await;
    let mut b = Bulk::new("b5s");
    let x = format!("at://{DAVE}/{PUBLIC_POST}/b5floodx");
    let mut node = b.post(ALICE, &g, Some((&x, &x)));
    let top = node.clone();
    for d in 1..8 {
        node = b.post(if d % 2 == 1 { BOB } else { CAROL }, &g, Some((&x, &node)));
    }
    for _ in 0..5_000 {
        b.post(ALICE, &g, Some((&x, &node)));
    }
    b.write(&pool).await;
    ready_dids(&pool, &[ALICE.into(), BOB.into(), CAROL.into()]).await;
    exec(&pool, &format!(
        "INSERT INTO private_notifications (recipient_did, author_did, reason, reason_subject, record_uri, record_cid) \
         SELECT '{DAVE}', author_did, 'reply', '{x}', uri, cid FROM private_posts WHERE reply_parent_uri = '{node}'"
    )).await;
    exec(&pool, "ANALYZE").await;
    let cases = [
        ("replyCount (getPosts of the node)", POSTS, format!("uris={node}"), false),
        ("getUnreadCount", GET_UNREAD_COUNT, String::new(), false),
        ("getPostThread of the branch root", THREAD, format!("uri={top}&depth=2"), false),
    ];
    run_fixture(&mut w, "5 000 owner branch replies under one node", &default_read, &heads, &cases).await;
    failures.extend(w.drain().await);

    let Some((mut w, heads)) = part3_walk("b5-distinct").await else {
        return;
    };
    let pool = w.f.pool.clone();
    let g = audience_row(&pool, ALICE, "b5dist", &[DAVE.into(), BOB.into(), CAROL.into()]).await;
    let mut b = Bulk::new("b5t");
    for i in 0..5_000 {
        let x = format!("at://{DAVE}/{PUBLIC_POST}/b5tx{i}");
        let mut parent = b.post(ALICE, &g, Some((&x, &x)));
        for d in 1..8 {
            parent = b.post(if d % 2 == 1 { BOB } else { ALICE }, &g, Some((&x, &parent)));
        }
        b.post(ALICE, &g, Some((&x, &parent)));
        if b.rows.len() > 20_000 {
            b.write(&pool).await;
        }
    }
    b.write(&pool).await;
    ready_dids(&pool, &[ALICE.into(), BOB.into()]).await;
    exec(&pool, &format!(
        "INSERT INTO private_notifications (recipient_did, author_did, reason, reason_subject, record_uri, record_cid) \
         SELECT '{DAVE}', p.author_did, 'reply', p.reply_root_uri, p.uri, p.cid FROM private_posts p \
         WHERE p.uri LIKE 'at://{ALICE}/%/b5t%' AND p.reply_parent_uri IS NOT NULL \
           AND NOT EXISTS (SELECT 1 FROM private_posts c WHERE c.reply_parent_uri = p.uri)"
    )).await;
    exec(&pool, "ANALYZE").await;
    let cases = [("getUnreadCount (gated)", GET_UNREAD_COUNT, String::new(), true)];
    run_fixture(&mut w, "5 000 owner branch replies, distinct parents", &default_read, &heads, &cases).await;
    failures.extend(w.drain().await);

    // Adversarial, named: a 5 000-row branch chain under an audience V is
    // outside, at `max_chain` 16 and 100.
    let Some((mut w, heads)) = part3_walk("b5-chain").await else {
        return;
    };
    let pool = w.f.pool.clone();
    let writer = member(3500);
    let g = audience_row(&pool, &writer, "b5chain", &[BOB.into()]).await;
    let mut b = Bulk::new("b5k");
    let x = format!("at://{CAROL}/{PUBLIC_POST}/b5chainx");
    let mut parent = b.post(&writer, &g, Some((&x, &x)));
    for _ in 1..5_000 {
        parent = b.post(&writer, &g, Some((&x, &parent)));
    }
    b.write(&pool).await;
    ready_dids(&pool, std::slice::from_ref(&writer)).await;
    exec(&pool, &follow_sql(DAVE, &writer)).await;
    exec(&pool, "ANALYZE").await;
    for cap in [16, 100] {
        let read = PrivateReadConfig { max_chain: cap, ..PrivateReadConfig::default() };
        let cases = [
            ("getTimeline", TIMELINE, String::new(), false),
            ("getAuthorFeed", FEED, format!("actor={writer}"), false),
        ];
        run_fixture(&mut w, &format!("5 000-row branch chain, V outside, cap {cap}"), &read, &heads, &cases).await;
    }
    failures.extend(w.drain().await);
    assert!(failures.is_empty(), "{} failing cases:\n{}", failures.len(), failures.join("\n"));
}

impl Walk {
    /// This walk's failures, its database dropped.
    async fn drain(self) -> Vec<String> {
        let failures = self.failures;
        self.db.drop_db().await;
        failures
    }
}

/// The stale-head chain (§4 part (3), `bench-heads` only): Alice's root and
/// a chain of 15 replies by 15 authors below it, 16 authors and Dave, 17
/// DIDs against a budget of 16, every head stale before each sample and
/// answered by `BenchHeadSource` after 450 ms. `getPostThread` at the bottom,
/// at concurrency 1, under its 1 200 ms floor; answering, and with parent
/// 5's author failing.
#[cfg(feature = "bench-heads")]
#[tokio::test]
#[ignore = "B-5 stale-head chain; run with --release --features bench-heads --ignored --nocapture"]
async fn b5_stale_head_chain() {
    use crate::sync::bench_heads::BenchHeadSource;
    let authors: Vec<String> = (6001..=6015).map(member).collect();
    let mut stale: Vec<String> = vec![ALICE.into(), DAVE.into()];
    stale.extend(authors.iter().cloned());
    for failing in [false, true] {
        let text: String = stale
            .iter()
            .map(|d| format!("{d} {}\n", if failing && *d == authors[9] { "fail" } else { "3kzzzzzzzzzz2" }))
            .collect();
        let source = BenchHeadSource::parse(&text).expect("heads");
        let test = if failing { "b5-stale-failing" } else { "b5-stale" };
        let Some(mut w) = Walk::new_in(test, PublicMode::On, Layout::SamePds, Some("followers"), &[], &authors, Arc::new(source)).await else {
            return;
        };
        let pool = w.f.pool.clone();
        for a in &authors {
            exec(&pool, &follow_sql(a, ALICE)).await;
        }
        let mut parent = w.root.clone();
        for (i, a) in authors.iter().enumerate() {
            parent = w.reply(a, &format!("3kstale{i:02}222"), &parent).await;
        }
        let bottom = parent;
        w.f.ready_all().await;
        for a in &authors {
            w.f.ready(a).await;
        }
        for (i, d) in stale.iter().enumerate() {
            exec(&pool, &format!("UPDATE actors SET pds_endpoint = 'https://bench-{i}.invalid' WHERE did = '{d}'")).await;
        }
        let base = serve_live(w.f.state_bounded(&PrivateReadConfig::default())).await;
        let n = std::env::var("HPRS_B5_STALE_SAMPLES").ok().and_then(|s| s.parse().ok()).unwrap_or(samples());
        let mut took = Vec::new();
        let stale_sql = format!("UPDATE sync_state SET head_checked_at = to_timestamp(0) WHERE did = ANY('{{{}}}')", stale.join(","));
        for _ in 0..n {
            exec(&pool, &stale_sql).await;
            let (status, body, t) = get(&base, THREAD, &format!("uri={bottom}"), DAVE, DAVE_KEY).await;
            assert_eq!(status, 200, "{body}");
            took.push(t);
        }
        took.sort();
        let (p50, p99) = (percentile(&took, 50), percentile(&took, 99));
        println!(
            "  B-5 (3) stale-head chain, {}: getPostThread p50 {:.1} ms, p99 {:.1} ms over {n} samples, floor 1200 ms",
            if failing { "parent 5's author failing" } else { "answering" },
            p50.as_secs_f64() * 1000.0,
            p99.as_secs_f64() * 1000.0
        );
        w.check(p99 < Duration::from_millis(1200), format!("stale-head chain (failing {failing}): p99 {p99:?}"));
        w.finish().await;
    }
}
