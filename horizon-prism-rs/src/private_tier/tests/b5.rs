//! B-5, S12's release gate (docs/internal/design/private-threads.md §4,
//! C-T14 (a)), and T-54. Every test here is ignored: each builds the
//! synthetic snapshot (50 000 private posts, 5 000 members, 2 000
//! audiences) and its fixtures in a fresh database, and prints what it
//! measures. Run on the reference host with a release build:
//! `cargo test --release b5_ -- --ignored --nocapture --test-threads 1`.
//!
//! - **Part (1), T-54** (`b5_row_class_ratios`): per row class, the shared
//!   and local buffers of S12's statement per `c` row against HEAD's own
//!   candidate statement (its prefilter as it stood before S12, kept in
//!   `schema`) over the same rows, at §3.5's bounds, and at the accepted
//!   bounds of amendment 1 §1 for the five classes handed to #720.
//! - **Part (3)'s fixtures** ([`Part3`]): built here, run on the deploy build
//!   by `b5_release` (amendment 1 §2), and in process by
//!   `b5_absolute_floors` (each endpoint's p99 at the client against its
//!   B-5 budget, at concurrency 8, heads fresh, and no head fetched).
//! - **The stale-head chain** runs on the `bench-heads` build, in
//!   `b5_release::b5_stale_head_chain`.
//! - **Part (2)**, per request against a HEAD build, is not run here: it
//!   needs HEAD's handlers serving the same database, which a test of this
//!   build cannot load.

use std::sync::atomic::{AtomicUsize, Ordering};

use futures::StreamExt;

use super::*;

/// The base snapshot (§4, "Host and data").
const BASE_MEMBERS: i64 = 5_000;
const BASE_AUDIENCES: i64 = 2_000;
const BASE_POSTS: i64 = 50_000;

/// B-5's member DID `i`: `did:plc:` and 24 base-32 letters (hex digits
/// mapped to `k`–`t`). [`MEMBER_SQL`] is the same in SQL over `g`.
pub(super) fn member(i: i64) -> String {
    let letters: String = format!("{i:024x}")
        .chars()
        .map(|c| match c {
            '0'..='9' => char::from(b'k' + (c as u8 - b'0')),
            other => other,
        })
        .collect();
    format!("did:plc:{letters}")
}

pub(super) const MEMBER_SQL: &str = "('did:plc:' || translate(lpad(to_hex(g), 24, '0'), '0123456789', 'klmnopqrst'))";

/// Samples per endpoint per fixture: `HPRS_B5_SAMPLES`, else 1 000.
pub(super) fn samples() -> usize {
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
pub(super) async fn base_snapshot(pool: &PgPool) {
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
pub(super) struct Bulk {
    tag: String,
    pub(super) rows: Vec<[Option<String>; 5]>,
    /// Posts named so far, across writes: each URI is unique.
    named: usize,
}

impl Bulk {
    pub(super) fn new(tag: &str) -> Self {
        Self { tag: tag.to_string(), rows: Vec::new(), named: 0 }
    }

    /// A post by `author` under `audience`, replying to `(root, parent)`.
    pub(super) fn post(&mut self, author: &str, audience: &str, reply: Option<(&str, &str)>) -> String {
        let uri = format!("at://{author}/{NSID_POST_PRIVATE}/{}{:06}", self.tag, self.named);
        self.named += 1;
        let (root, parent) = reply.map_or((None, None), |(r, p)| (Some(r.to_string()), Some(p.to_string())));
        self.rows.push([Some(uri.clone()), Some(author.to_string()), Some(audience.to_string()), root, parent]);
        uri
    }

    pub(super) async fn write(&mut self, pool: &PgPool) {
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
pub(super) async fn audience_row(pool: &PgPool, owner: &str, rkey: &str, members: &[String]) -> String {
    audience_mode_row(pool, owner, rkey, "list", members).await
}

/// An audience row of `owner`'s in `mode`, listing `members`.
pub(super) async fn audience_mode_row(pool: &PgPool, owner: &str, rkey: &str, mode: &str, members: &[String]) -> String {
    let uri = format!("at://{owner}/{NSID_AUDIENCE}/{rkey}");
    sqlx::query(
        "INSERT INTO private_audiences (uri, owner_did, rkey, cid, rev, mode, members, display_filled) \
         VALUES ($1, $2, $3, 'bafyb5', $4, $5, $6, true)",
    )
    .bind(&uri)
    .bind(owner)
    .bind(rkey)
    .bind(REV)
    .bind(mode)
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
/// its bound. An accepted class (amendment 1 §1, #720) carries §3.5's
/// predicted ratio beside its accepted bound.
struct Class {
    name: String,
    rows: Vec<String>,
    passes: bool,
    bound: f64,
    cap: u32,
    predicted: Option<f64>,
}

/// The accepted bounds (private-threads amendment 1 §1, handed to #720):
/// S12's measured ratio at adc4a00 × 1.1, rounded up to 0.1. A branch
/// reply with the reader outside G under distinct parents, at depth k.
fn accepted_depth_bound(k: usize) -> f64 {
    match k {
        4 => 4.5,
        8 => 7.0,
        _ => 11.5,
    }
}

/// The past-cap flood's bound at `max_chain` `cap` under the shared walk
/// (shared-walk §5.1; amendment 1 §1.2 accepted 8.3 and 45.8).
fn shared_cap_bound(cap: u32) -> f64 {
    if cap == 16 { 2.1 } else { 3.3 }
}

/// Part (1) and T-54 (§3.5, §4): every row class, passing and failing, on
/// the base snapshot, S12's buffers per row against HEAD's, at the
/// table's bounds. The branch reply with the reader outside G under
/// distinct parents (k = 4, 8 and 16) is an accepted cost at its measured
/// bounds (amendment 1 §1, #720): each prints its ratio, §3.5's predicted
/// ratio and the accepted bound. The shared walk (shared-walk §5.1) bounds
/// owner branch replies under distinct parents at 3.1, the past-cap flood
/// at 2.1 and 3.3, and adds the comb and the past-cap thread chain.
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
        classes.push(Class { name: name.to_string(), rows, passes, bound: bound_of(ratio), cap, predicted: None });
    };
    let mut accepted: Vec<Class> = Vec::new();

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
    // Shared-walk §5.1: bound 3.1 at both n (were 8.8 and 11.2); §5.1's
    // ratio is 2.8, and 2.84 + 0.25 rounds up to 3.1.
    for (n, ratio, label) in [(4, 2.84, "n = 4, alternating O and B"), (7, 2.84, "n = 7")] {
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
        let predicted = (4.0 + k.min(16) as f64) / 6.0;
        let rows = (0..N).map(|i| branch_chain(&mut b, &x(30_000 + 1000 * k + i), &g_out, k, BOB)).collect();
        accepted.push(Class {
            name: format!("failing: branch reply, V outside G, distinct parents, depth {k}"),
            rows,
            passes: false,
            bound: accepted_depth_bound(k),
            cap: CAP,
            predicted: Some(predicted),
        });
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
    // Shared-walk §5.1: 2.1 at 16 and 3.3 at 100 (were the accepted 8.3
    // and 45.8).
    for cap in [16u32, 100] {
        classes.push(Class {
            name: format!("failing: past the cap at {cap}, V outside G"),
            rows: deep.clone(),
            passes: false,
            bound: shared_cap_bound(cap),
            cap,
            predicted: None,
        });
    }
    // New (shared-walk §5.1): a thread chain of 160 replies by standing
    // writers under Alice's root, V inside G; rows 121 to 160 are past the
    // cap at 16 and 100, and fail.
    let thread_root = b.post(ALICE, &g_in, None);
    let mut parent = thread_root.clone();
    let mut thread_deep = Vec::new();
    for d in 1..=160 {
        parent = b.post(&others[d % others.len()], &g_in, Some((&thread_root, &parent)));
        if d > 120 {
            thread_deep.push(parent.clone());
        }
    }
    for cap in [16u32, 100] {
        classes.push(Class {
            name: format!("failing: thread chain past the cap at {cap}, standing writers, V inside G"),
            rows: thread_deep.clone(),
            passes: false,
            bound: 1.0,
            cap,
            predicted: None,
        });
    }
    // New (shared-walk §5.1): a comb, V outside G: a spine of 200 below a
    // branch root, a leaf chain of (i mod 7) + 1 rows off every fifth spine
    // row, the last leaf row of each a class row. Its bound is computed from
    // its own climb below.
    let comb_x = x(95_000);
    let mut spine = b.post(ALICE, &g_out, Some((&comb_x, &comb_x)));
    let mut comb = Vec::new();
    for row in 1..=200 {
        spine = b.post(&others[row % others.len()], &g_out, Some((&comb_x, &spine)));
        if row % 5 == 1 {
            let mut leaf = spine.clone();
            for _ in 0..=((row - 1) / 5) % 7 {
                leaf = b.post(BOB, &g_out, Some((&comb_x, &leaf)));
            }
            comb.push(leaf);
        }
    }
    classes.extend(accepted);
    b.write(&pool).await;
    for uri in &hidden {
        exec(&pool, &format!("INSERT INTO labels (subject, val) VALUES ('{uri}', '!hide')")).await;
    }
    exec(&pool, "ANALYZE").await;
    // The comb's bound (shared-walk §5.1): its climb's primary-key probes
    // over its rows, in buffers as §5.1 converts the past-cap class (a
    // failing row's 6.5, about 4 a probe, against HEAD's 6.8), plus 0.25.
    let plan: Value = sqlx::query_scalar(&format!("EXPLAIN (ANALYZE, FORMAT JSON) {}", schema::prefilter_uris_statement(CAP)))
        .bind(DAVE)
        .bind(&comb)
        .fetch_one(&pool)
        .await
        .expect("comb plan");
    fn climb_probes(node: &Value, inside: bool) -> u64 {
        let inside = inside || node["Subplan Name"] == "CTE bcl";
        let own = if inside && node["Relation Name"] == "private_posts" { node["Actual Loops"].as_u64().unwrap_or(0) } else { 0 };
        own + node["Plans"].as_array().into_iter().flatten().map(|c| climb_probes(c, inside)).sum::<u64>()
    }
    #[allow(clippy::cast_precision_loss)]
    let comb_ratio = (6.5 + 4.0 * climb_probes(&plan[0]["Plan"], false) as f64 / comb.len() as f64) / 6.8;
    println!("T-54 comb: {} rows, climb probes {}, probe ratio {comb_ratio:.2}", comb.len(), climb_probes(&plan[0]["Plan"], false));
    classes.push(Class {
        name: "failing: comb, V outside G".into(),
        rows: comb.clone(),
        passes: false,
        bound: bound_of(comb_ratio),
        cap: CAP,
        predicted: None,
    });

    let head = |n: usize| format!("SELECT p.uri FROM {} WHERE p.uri = ANY($2) AND {} LIMIT {n}", schema::PREFILTER_FROM, schema::head_prefilter());
    println!("B-5 part (1) / T-54: buffers per row, S12 against HEAD, reader Dave");
    for c in &classes {
        let s12 = buffers_per_row(&pool, &schema::prefilter_uris_statement(c.cap), DAVE, &c.rows).await;
        let base = buffers_per_row(&pool, &head(c.rows.len()), DAVE, &c.rows).await;
        let ratio = s12 / base;
        let served = schema::prefilter_uris(&pool, DAVE, &c.rows, c.cap).await.expect("prefilter").len();
        println!(
            "  {:<70} rows {:>4}  S12 {:>7.2}  HEAD {:>6.2}  ratio {:>5.2}  bound {:>5.1}  {}{}",
            c.name,
            c.rows.len(),
            s12,
            base,
            ratio,
            c.bound,
            if ratio <= c.bound { "ok" } else { "OVER" },
            c.predicted.map_or_else(String::new, |p| format!("  accepted (#720), §3.5 predicted {p:.2}"))
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

pub(super) fn percentile(sorted: &[Duration], p: usize) -> Duration {
    sorted[(sorted.len() * p / 100).min(sorted.len() - 1)]
}

/// B-5's latency budget per endpoint, in milliseconds (timing-floors design
/// §3.2): the floors' previous defaults, kept as the bound part (3) gates
/// on now that floors are 0 by default. C-T14's absolute bound guards
/// latency at these numbers.
pub(super) const B5_BUDGET_MS: [(&str, u64); 12] = [
    (POSTS, 1000),
    (THREAD, 1200),
    (FEED, 1200),
    (TIMELINE, 1200),
    (GET_BOOKMARKS, 1200),
    (GET_LIKES, 800),
    (GET_REPOSTED_BY, 800),
    (GET_QUOTES, 1200),
    (LIST_NOTIFICATIONS, 1200),
    (GET_UNREAD_COUNT, 300),
    (GET_REPLIES, 1200),
    (SEARCH, 1500),
];

/// An endpoint's budget from [`B5_BUDGET_MS`].
pub(super) fn budget_of(method: &str) -> Duration {
    let (_, ms) = B5_BUDGET_MS
        .iter()
        .find(|(m, _)| *m == method)
        .unwrap_or_else(|| panic!("no B-5 budget for {method}"));
    Duration::from_millis(*ms)
}

/// One part (3) fixture's run: each `(label, method, query, gated)` sampled
/// against a fresh server, printed with its budget; a gated p99 over its
/// budget is a failure.
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
        let budget = budget_of(method);
        println!(
            "  B-5 (3) {fixture:<46} {label:<34} alone p50 {:>6.1} ms  c8 p50 {:>7.1} ms  p99 {:>7.1} ms  budget {:>5} ms  {}",
            percentile(&alone, 50).as_secs_f64() * 1000.0,
            p50.as_secs_f64() * 1000.0,
            p99.as_secs_f64() * 1000.0,
            budget.as_millis(),
            match (gated, p99 < budget) {
                (false, _) => "reported",
                (true, true) => "ok",
                (true, false) => "OVER",
            }
        );
        if *gated {
            w.check(p99 < budget, format!("{fixture}, {label}: p99 {p99:?} over its budget {budget:?}"));
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
pub(super) async fn ready_dids(pool: &PgPool, dids: &[String]) {
    sqlx::query("INSERT INTO private_owner_ready (did, ready_at) SELECT unnest($1::text[]), now() ON CONFLICT DO NOTHING")
        .bind(dids)
        .execute(pool)
        .await
        .expect("ready");
}

/// One part (3) case: its label, method, query, and whether its p99 is
/// gated at the endpoint's budget (otherwise reported).
pub(super) type Case = (&'static str, &'static str, String, bool);

/// Part (3)'s fixtures (§4; amendment 1 §2.4), each loaded into its own
/// fresh database beside the base snapshot and the readers.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum Part3 {
    Base,
    Deep,
    Eight,
    BigList,
    Outside,
    Alternating,
    Floods,
    Distinct,
    Chain,
    SelfService,
    FailingQuotes,
    UndecodableCandidates,
    UndecodableChildren,
    Likes,
    PrefilterFails,
    ThreadChain,
    Comb,
}

impl Part3 {
    /// The ordinary fixtures, the shared walk's no-regression gate
    /// (shared-walk §7).
    pub(super) const ORDINARY: [Self; 5] = [Self::Base, Self::Deep, Self::Eight, Self::BigList, Self::Alternating];

    /// Every fixture but the stale-head chain, which needs `bench-heads`.
    pub(super) const ALL: [Self; 17] = [
        Self::Base,
        Self::Deep,
        Self::Eight,
        Self::BigList,
        Self::Outside,
        Self::Alternating,
        Self::Floods,
        Self::Distinct,
        Self::Chain,
        Self::SelfService,
        Self::FailingQuotes,
        Self::UndecodableCandidates,
        Self::UndecodableChildren,
        Self::Likes,
        Self::PrefilterFails,
        Self::ThreadChain,
        Self::Comb,
    ];

    /// A database-safe name.
    pub(super) fn slug(self) -> &'static str {
        match self {
            Self::Base => "base",
            Self::Deep => "deep",
            Self::Eight => "eight",
            Self::BigList => "biglist",
            Self::Outside => "outside",
            Self::Alternating => "alternating",
            Self::Floods => "floods",
            Self::Distinct => "distinct",
            Self::Chain => "chain",
            Self::SelfService => "selfservice",
            Self::FailingQuotes => "failingquotes",
            Self::UndecodableCandidates => "undeccands",
            Self::UndecodableChildren => "undecchildren",
            Self::Likes => "likes",
            Self::PrefilterFails => "prefilterfails",
            Self::ThreadChain => "threadchain",
            Self::Comb => "comb",
        }
    }

    pub(super) fn title(self) -> &'static str {
        match self {
            Self::Base => "base snapshot",
            Self::Deep => "16-deep thread and branch, 16 authors",
            Self::Eight => "8-deep thread",
            Self::BigList => "a list G of 10 000 members",
            Self::Outside => "branch conversations, V outside, k = 16",
            Self::Alternating => "8-deep branches alternating O and B, V inside",
            Self::Floods => "5 000 owner branch replies under one node",
            Self::Distinct => "5 000 owner branch replies, distinct parents",
            Self::Chain => "5 000-row branch chain, V outside",
            Self::SelfService => "T-58's self-service failures",
            Self::FailingQuotes => "5 000 failing quotes of X",
            Self::UndecodableCandidates => "limit x 8 + 1 undecodable candidates",
            Self::UndecodableChildren => "48 undecodable children per expanded node",
            Self::Likes => "50 000 like records",
            Self::PrefilterFails => "5 000 posts that fail the prefilter",
            Self::ThreadChain => "5 000-row thread chain of a standing writer, V inside",
            Self::Comb => "a 5 000-row branch spine with 1 000 leaf chains, V outside",
        }
    }

    /// The walk caps (`private_read.max_chain`) the fixture runs at.
    pub(super) fn caps(self) -> &'static [u32] {
        match self {
            Self::Chain | Self::ThreadChain => &[16, 100],
            Self::Comb => &[100],
            _ => &[CAP],
        }
    }

    /// The alone p50 the fixture is gated at instead of its p99, which is
    /// reported (shared-walk §7: `Distinct` at 1.0 s, this design's claim).
    pub(super) fn alone_gate(self) -> Option<Duration> {
        (self == Self::Distinct).then_some(Duration::from_secs(1))
    }

    /// Write the fixture over the base snapshot and return its cases.
    #[allow(clippy::too_many_lines)]
    pub(super) async fn build(self, pool: &PgPool) -> Vec<Case> {
        match self {
            // All twelve floored endpoints.
            Self::Base => {
                let p1 = p1();
                // Post 6 007 is member 7's third reply in its first thread.
                let p1_reply = format!("at://{}/{NSID_POST_PRIVATE}/b5p6007", member(7));
                for (i, liker) in (1..=100).map(member).enumerate() {
                    for table in ["likes", "reposts"] {
                        if table == "reposts" && i >= 50 {
                            continue;
                        }
                        exec(pool, &format!("INSERT INTO {table} (uri, author_did, subject_uri, subject_cid, rev, created_at) VALUES ('at://{liker}/x/b5{table}{i}', '{liker}', '{p1}', 'bafyb5', '3k', now())")).await;
                    }
                }
                let mut b = Bulk::new("b5q");
                for i in 1..=20 {
                    let q = b.post(&member(i), &format!("at://{}/{NSID_AUDIENCE}/b5aud", member(i)), None);
                    b.write(pool).await;
                    exec(pool, &format!("UPDATE private_posts SET quote_uri = '{p1}' WHERE uri = '{q}'")).await;
                }
                exec(pool, &format!(
                    "INSERT INTO private_bookmarks (viewer_did, post_uri, post_cid) SELECT '{DAVE}', uri, cid FROM private_posts WHERE uri LIKE '%/b5p%' ORDER BY uri LIMIT 50"
                )).await;
                exec(pool, &format!(
                    "INSERT INTO private_notifications (recipient_did, author_did, reason, record_uri, record_cid) \
                     SELECT '{DAVE}', author_did, 'mention', uri, cid FROM private_posts WHERE uri LIKE '%/b5p%' ORDER BY uri LIMIT 200"
                )).await;
                let posts25: Vec<String> = (1..=25).map(|i| format!("uris=at://{}/{NSID_POST_PRIVATE}/b5p{i}", member(i))).collect();
                vec![
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
                ]
            }
            // A 16-deep thread and a 16-deep branch of readable posts by 16
            // authors, read at the bottom.
            Self::Deep => {
                let authors: Vec<String> = (3001..=3015).map(member).collect();
                let mut members: Vec<String> = vec![DAVE.into(), BOB.into(), CAROL.into()];
                members.extend(authors.iter().cloned());
                let g = audience_row(pool, ALICE, "b5deep", &members).await;
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
                b.write(pool).await;
                let mut ready: Vec<String> = authors.clone();
                ready.extend([ALICE.into(), BOB.into()]);
                ready_dids(pool, &ready).await;
                vec![
                    ("getPostThread, thread bottom", THREAD, format!("uri={thread_bottom}"), true),
                    ("getPostThread, branch bottom", THREAD, format!("uri={branch_bottom}"), true),
                ]
            }
            // An 8-deep thread with a feed page, a search page and a
            // getReplies page on it.
            Self::Eight => {
                let g = audience_row(pool, ALICE, "b5eight", &[DAVE.into(), BOB.into(), CAROL.into()]).await;
                let mut b = Bulk::new("b5e");
                let root = b.post(ALICE, &g, None);
                let mut node = root.clone();
                for d in 1..8 {
                    node = b.post(if d % 2 == 1 { BOB } else { CAROL }, &g, Some((&root, &node)));
                }
                for _ in 0..60 {
                    b.post(BOB, &g, Some((&root, &node)));
                }
                b.write(pool).await;
                ready_dids(pool, &[ALICE.into(), BOB.into(), CAROL.into()]).await;
                exec(pool, &follow_sql(DAVE, BOB)).await;
                vec![
                    ("getAuthorFeed of the replier", FEED, format!("actor={BOB}"), true),
                    ("getTimeline", TIMELINE, String::new(), true),
                    ("searchPosts", SEARCH, "q=b5e".to_string(), true),
                    ("getReplies (60 at depth 8)", GET_REPLIES, format!("uri={node}"), true),
                ]
            }
            // A `list` G of 10 000 members.
            Self::BigList => {
                let mut big: Vec<String> = (1..=9_997).map(member).collect();
                big.extend([DAVE.into(), BOB.into(), CAROL.into()]);
                let g = audience_row(pool, ALICE, "b5big", &big).await;
                let mut b = Bulk::new("b5l");
                let mut uris = Vec::new();
                for i in 0..50 {
                    let root = b.post(ALICE, &g, None);
                    uris.push(root.clone());
                    for _ in 0..(i % 4) {
                        b.post(BOB, &g, Some((&root, &root)));
                    }
                }
                b.write(pool).await;
                ready_dids(pool, &[ALICE.into(), BOB.into()]).await;
                let posts25: Vec<String> = uris.iter().take(25).map(|u| format!("uris={u}")).collect();
                vec![
                    ("getPosts (25)", POSTS, posts25.join("&"), true),
                    ("getAuthorFeed", FEED, format!("actor={ALICE}"), true),
                    ("getTimeline", TIMELINE, String::new(), true),
                    ("getPostThread", THREAD, format!("uri={}", uris[3]), true),
                ]
            }
            // A timeline and an author feed over branch conversations by
            // followed authors, V outside each branch root's audience, every
            // class row at depth k = 16.
            Self::Outside => {
                let mut b = Bulk::new("b5o");
                let writers: Vec<String> = (3101..=3120).map(member).collect();
                for (i, wr) in writers.iter().enumerate() {
                    let g = audience_row(pool, wr, "b5out", &[BOB.into(), CAROL.into()]).await;
                    exec(pool, &follow_sql(DAVE, wr)).await;
                    for c in 0..5 {
                        let x = format!("at://{CAROL}/{PUBLIC_POST}/b5o{i}x{c}");
                        let mut parent = b.post(wr, &g, Some((&x, &x)));
                        for _ in 1..=16 {
                            parent = b.post(wr, &g, Some((&x, &parent)));
                        }
                    }
                }
                b.write(pool).await;
                ready_dids(pool, &writers).await;
                vec![
                    ("getTimeline", TIMELINE, String::new(), true),
                    ("getAuthorFeed", FEED, format!("actor={}", writers[0]), true),
                ]
            }
            // O's author feed and a timeline following O, over 8-deep branch
            // conversations alternating O and B, V inside G, every O row at
            // depth 8.
            Self::Alternating => {
                let g = audience_row(pool, ALICE, "b5alt", &[DAVE.into(), BOB.into(), CAROL.into()]).await;
                let mut b = Bulk::new("b5a");
                for c in 0..60 {
                    let x = format!("at://{CAROL}/{PUBLIC_POST}/b5ax{c}");
                    let mut parent = b.post(ALICE, &g, Some((&x, &x)));
                    for d in 1..=8 {
                        parent = b.post(if d % 2 == 1 { BOB } else { ALICE }, &g, Some((&x, &parent)));
                    }
                }
                b.write(pool).await;
                ready_dids(pool, &[ALICE.into(), BOB.into()]).await;
                vec![
                    ("getAuthorFeed of O", FEED, format!("actor={ALICE}"), true),
                    ("getTimeline following O", TIMELINE, String::new(), true),
                ]
            }
            // Adversarial: 5 000 owner branch replies under one node, each
            // notifying V.
            Self::Floods => {
                let g = audience_row(pool, ALICE, "b5flood", &[DAVE.into(), BOB.into(), CAROL.into()]).await;
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
                b.write(pool).await;
                ready_dids(pool, &[ALICE.into(), BOB.into(), CAROL.into()]).await;
                exec(pool, &format!(
                    "INSERT INTO private_notifications (recipient_did, author_did, reason, reason_subject, record_uri, record_cid) \
                     SELECT '{DAVE}', author_did, 'reply', '{x}', uri, cid FROM private_posts WHERE reply_parent_uri = '{node}'"
                )).await;
                vec![
                    ("replyCount (getPosts of the node)", POSTS, format!("uris={node}"), false),
                    ("getUnreadCount", GET_UNREAD_COUNT, String::new(), false),
                    ("getPostThread of the branch root", THREAD, format!("uri={top}&depth=2"), false),
                ]
            }
            // 5 000 under distinct parents at depth 8 under V's own public
            // post, each notifying V (gated at getUnreadCount's budget).
            Self::Distinct => {
                let g = audience_row(pool, ALICE, "b5dist", &[DAVE.into(), BOB.into(), CAROL.into()]).await;
                let mut b = Bulk::new("b5t");
                for i in 0..5_000 {
                    let x = format!("at://{DAVE}/{PUBLIC_POST}/b5tx{i}");
                    let mut parent = b.post(ALICE, &g, Some((&x, &x)));
                    for d in 1..8 {
                        parent = b.post(if d % 2 == 1 { BOB } else { ALICE }, &g, Some((&x, &parent)));
                    }
                    b.post(ALICE, &g, Some((&x, &parent)));
                    if b.rows.len() > 20_000 {
                        b.write(pool).await;
                    }
                }
                b.write(pool).await;
                ready_dids(pool, &[ALICE.into(), BOB.into()]).await;
                exec(pool, &format!(
                    "INSERT INTO private_notifications (recipient_did, author_did, reason, reason_subject, record_uri, record_cid) \
                     SELECT '{DAVE}', p.author_did, 'reply', p.reply_root_uri, p.uri, p.cid FROM private_posts p \
                     WHERE p.uri LIKE 'at://{ALICE}/%/b5t%' AND p.reply_parent_uri IS NOT NULL \
                       AND NOT EXISTS (SELECT 1 FROM private_posts c WHERE c.reply_parent_uri = p.uri)"
                )).await;
                // The p99 is reported against the 300 ms budget; the alone p50
                // is gated at 1.0 s ([`Part3::alone_gate`], shared-walk §7).
                vec![("getUnreadCount", GET_UNREAD_COUNT, String::new(), false)]
            }
            // Adversarial, named: a 5 000-row branch chain under an audience
            // V is outside, at `max_chain` 16 and 100; gated since the shared
            // walk (§7).
            Self::Chain => {
                let writer = member(3500);
                let g = audience_row(pool, &writer, "b5chain", &[BOB.into()]).await;
                let mut b = Bulk::new("b5k");
                let x = format!("at://{CAROL}/{PUBLIC_POST}/b5chainx");
                let mut parent = b.post(&writer, &g, Some((&x, &x)));
                for _ in 1..5_000 {
                    parent = b.post(&writer, &g, Some((&x, &parent)));
                }
                b.write(pool).await;
                ready_dids(pool, std::slice::from_ref(&writer)).await;
                exec(pool, &follow_sql(DAVE, &writer)).await;
                vec![
                    ("getTimeline", TIMELINE, String::new(), true),
                    ("getAuthorFeed", FEED, format!("actor={writer}"), true),
                ]
            }
            // Adversarial (shared-walk §7): a 5 000-row thread chain under
            // Alice's root, every row by a writer standing under her `list`,
            // V inside G, no owner reply within the cap; at 16 and 100.
            Self::ThreadChain => {
                let writer = member(3700);
                let g = audience_row(pool, ALICE, "b5tchain", &[DAVE.into(), BOB.into(), writer.clone()]).await;
                let mut b = Bulk::new("b5w");
                let root = b.post(ALICE, &g, None);
                let mut parent = root.clone();
                for _ in 0..5_000 {
                    parent = b.post(&writer, &g, Some((&root, &parent)));
                }
                // The deepest rows newest: a feed examines the rows past
                // the cap, which fail, before the ones that pass.
                b.rows.reverse();
                b.write(pool).await;
                ready_dids(pool, &[ALICE.into(), writer.clone()]).await;
                exec(pool, &follow_sql(DAVE, &writer)).await;
                vec![
                    ("getTimeline", TIMELINE, String::new(), true),
                    ("getAuthorFeed", FEED, format!("actor={writer}"), true),
                ]
            }
            // Adversarial (shared-walk §7): a branch root under Carol's
            // public post, under an audience of Bob alone, a spine of 5 000
            // branch replies below it, and off spine row 5i + 1 a leaf chain
            // of (i mod 7) + 1 rows, all by one writer Dave follows; V
            // outside. Every row fails, by intent.
            Self::Comb => {
                let writer = member(3600);
                let g = audience_row(pool, &writer, "b5comb", &[BOB.into()]).await;
                let mut b = Bulk::new("b5m");
                let x = format!("at://{CAROL}/{PUBLIC_POST}/b5combx");
                let mut spine = b.post(&writer, &g, Some((&x, &x)));
                for row in 1..=5_000 {
                    spine = b.post(&writer, &g, Some((&x, &spine)));
                    if row % 5 == 1 {
                        let mut leaf = spine.clone();
                        for _ in 0..=((row - 1) / 5) % 7 {
                            leaf = b.post(&writer, &g, Some((&x, &leaf)));
                        }
                    }
                }
                b.write(pool).await;
                ready_dids(pool, std::slice::from_ref(&writer)).await;
                exec(pool, &follow_sql(DAVE, &writer)).await;
                vec![
                    ("getTimeline", TIMELINE, String::new(), true),
                    ("getAuthorFeed", FEED, format!("actor={writer}"), true),
                ]
            }
            // T-58's self-service failures: 200 threads per top writer, each
            // Alice's root, the writer's reply and Carol's reply below it,
            // and every top writer failed one way: blocks V, deactivated,
            // blocks O, not following O under `followers`, removed from the
            // `list`. Dave follows Alice and Carol.
            Self::SelfService => {
                let writers: Vec<String> = (3201..=3205).map(member).collect();
                let mut listed: Vec<String> = vec![CAROL.into(), DAVE.into()];
                listed.extend(writers[..4].iter().cloned());
                let g = audience_row(pool, ALICE, "b5ss", &listed).await;
                let gf = audience_mode_row(pool, ALICE, "b5ssf", "followers", &[]).await;
                let mut b = Bulk::new("b5v");
                let mut failing = Vec::new();
                let mut roots = Vec::new();
                for (i, w) in writers.iter().enumerate() {
                    let aud = if i == 3 { &gf } else { &g };
                    for _ in 0..200 {
                        let root = b.post(ALICE, aud, None);
                        let top = b.post(w, aud, Some((&root, &root)));
                        failing.push(b.post(CAROL, aud, Some((&root, &top))));
                        roots.push(root);
                    }
                }
                b.write(pool).await;
                exec(pool, &block_sql(&writers[0], DAVE)).await;
                exec(pool, &format!("UPDATE actors SET account_status = 'deactivated' WHERE did = '{}'", writers[1])).await;
                exec(pool, &block_sql(&writers[2], ALICE)).await;
                let mut ready: Vec<String> = writers.clone();
                ready.extend([ALICE.into(), CAROL.into()]);
                ready_dids(pool, &ready).await;
                exec(pool, &follow_sql(DAVE, CAROL)).await;
                let posts25: Vec<String> = failing.iter().step_by(40).take(25).map(|u| format!("uris={u}")).collect();
                vec![
                    ("getTimeline", TIMELINE, String::new(), false),
                    ("getAuthorFeed of the reply writer", FEED, format!("actor={CAROL}"), false),
                    ("getPosts (25 failing)", POSTS, posts25.join("&"), false),
                    ("getPostThread of a root", THREAD, format!("uri={}", roots[0]), false),
                ]
            }
            // 5 000 private quotes of X, each under an audience V is outside.
            Self::FailingQuotes => {
                let p1 = p1();
                let writer = member(3300);
                let g = audience_row(pool, &writer, "b5fq", &[BOB.into()]).await;
                let mut b = Bulk::new("b5fq");
                for _ in 0..5_000 {
                    b.post(&writer, &g, None);
                }
                b.write(pool).await;
                exec(pool, &format!("UPDATE private_posts SET quote_uri = '{p1}' WHERE uri LIKE 'at://{writer}/%/b5fq%'")).await;
                ready_dids(pool, std::slice::from_ref(&writer)).await;
                vec![
                    ("getQuotes of X", GET_QUOTES, format!("uri={p1}"), false),
                    ("quoteCount (getPosts of X)", POSTS, format!("uris={p1}"), false),
                ]
            }
            // `limit × 8 + 1` undecodable candidates, the newest, before 20
            // readable ones: Bob's posts on his feed and Dave's timeline, and
            // as many replies to one node.
            Self::UndecodableCandidates => {
                const UNDECODABLE: usize = 50 * 8 + 1;
                let g = audience_row(pool, BOB, "b5und", &[DAVE.into(), CAROL.into()]).await;
                let mut b = Bulk::new("b5uz");
                let node = b.post(BOB, &g, None);
                b.write(pool).await;
                let mut u = Bulk::new("b5uu");
                for _ in 0..UNDECODABLE {
                    u.post(BOB, &g, None);
                    u.post(BOB, &g, Some((&node, &node)));
                }
                u.write(pool).await;
                let mut r = Bulk::new("b5ur");
                for _ in 0..20 {
                    r.post(BOB, &g, None);
                    r.post(BOB, &g, Some((&node, &node)));
                }
                r.write(pool).await;
                exec(pool, &format!("UPDATE private_posts SET created_at = created_at - interval '1 hour' WHERE uri LIKE 'at://{BOB}/%/b5ur%' OR uri = '{node}'")).await;
                make_undecodable(pool, &format!("at://{BOB}/%/b5uu%")).await;
                ready_dids(pool, &[BOB.into()]).await;
                exec(pool, &follow_sql(DAVE, BOB)).await;
                vec![
                    ("getAuthorFeed (limit 50)", FEED, format!("actor={BOB}&limit=50"), false),
                    ("getTimeline (limit 50)", TIMELINE, "limit=50".to_string(), false),
                    ("getReplies (limit 50)", GET_REPLIES, format!("uri={node}&limit=50"), false),
                ]
            }
            // A thread whose every expanded node has 48 undecodable children
            // beside its 2 readable ones, four levels deep.
            Self::UndecodableChildren => {
                let g = audience_row(pool, ALICE, "b5uc", &[DAVE.into(), BOB.into(), CAROL.into()]).await;
                let mut b = Bulk::new("b5ca");
                let mut u = Bulk::new("b5cu");
                let root = b.post(ALICE, &g, None);
                let mut level = vec![root.clone()];
                for _ in 0..4 {
                    let mut next = Vec::new();
                    for node in &level {
                        for _ in 0..48 {
                            u.post(BOB, &g, Some((&root, node)));
                        }
                        next.push(b.post(BOB, &g, Some((&root, node))));
                        next.push(b.post(CAROL, &g, Some((&root, node))));
                    }
                    level = next;
                }
                b.write(pool).await;
                u.write(pool).await;
                make_undecodable(pool, &format!("at://{BOB}/%/b5cu%")).await;
                ready_dids(pool, &[ALICE.into(), BOB.into(), CAROL.into()]).await;
                vec![("getPostThread of the root", THREAD, format!("uri={root}"), false)]
            }
            // 50 000 like records on one post.
            Self::Likes => {
                let p1 = p1();
                exec(pool, &format!(
                    "INSERT INTO likes (uri, author_did, subject_uri, subject_cid, rev, created_at) \
                     SELECT 'at://' || d || '/app.bsky.feed.like/b5lk', d, '{p1}', 'bafyb5', '3k', now() - make_interval(secs => g) \
                     FROM (SELECT g, {MEMBER_SQL} AS d FROM generate_series(10001, 60000) g) s"
                )).await;
                vec![
                    ("getLikes", GET_LIKES, format!("uri={p1}"), false),
                    ("likeCount (getPosts of the post)", POSTS, format!("uris={p1}"), false),
                ]
            }
            // 5 000 posts that fail the prefilter, the newest: owner rows and
            // cross-owner thread replies, V outside G, by followed authors.
            Self::PrefilterFails => {
                let writer = member(3400);
                let g = audience_row(pool, &writer, "b5pf", &[BOB.into()]).await;
                let mut b = Bulk::new("b5pf");
                let t = b.post(&writer, &g, None);
                let mut failing = Vec::new();
                for _ in 0..2_500 {
                    failing.push(b.post(&writer, &g, None));
                    failing.push(b.post(BOB, &g, Some((&t, &t))));
                }
                b.write(pool).await;
                ready_dids(pool, &[writer.clone(), BOB.into()]).await;
                exec(pool, &follow_sql(DAVE, &writer)).await;
                exec(pool, &follow_sql(DAVE, BOB)).await;
                let posts25: Vec<String> = failing.iter().take(25).map(|u| format!("uris={u}")).collect();
                vec![
                    ("getTimeline", TIMELINE, String::new(), false),
                    ("getAuthorFeed of the owner", FEED, format!("actor={writer}"), false),
                    ("getAuthorFeed of the replier", FEED, format!("actor={BOB}"), false),
                    ("searchPosts", SEARCH, "q=b5".to_string(), false),
                    ("getPosts (25 failing)", POSTS, posts25.join("&"), false),
                ]
            }
        }
    }
}

/// Member 1's first post, which Dave reads.
fn p1() -> String {
    format!("at://{}/{NSID_POST_PRIVATE}/b5p1", member(1))
}

/// Make the posts matching `pattern` (a LIKE pattern on the URI)
/// undecodable: content encoded with this process's at-rest keys for
/// another record, which no server decodes. Each passes the prefilter and
/// misses in the pipeline.
pub(super) async fn make_undecodable(pool: &PgPool, pattern: &str) {
    let hooks = at_rest_hooks(data_dir("b5-undecodable")).expect("at-rest hooks");
    let glue = PipelineGlue::new(GlueSinks::log(CorrelationKey::from_bytes([9; 32])), hooks);
    let nsid = Nsid::new(NSID_POST_PRIVATE).expect("nsid");
    let did = Did::new(BOB).expect("did");
    let ctx = RecordContentContext::new(
        nsid.clone(),
        RecordKey::new("3kencodedfor").expect("rkey"),
        did.clone(),
        None,
        did.clone(),
        TargetRepresentation::structural_only(StructuralRepresentation::Resource { did, nsid }),
        TraceId::from_bytes([1; 16]),
        Default::default(),
    );
    let encoded = kryphocron::encode_record_content(
        glue.at_rest_hooks(),
        glue.user_sink(),
        b"undecodable",
        &ctx,
        Instant::now() + Duration::from_secs(2),
        SystemTime::now(),
    )
    .await
    .expect("encode");
    sqlx::query(
        "UPDATE private_posts SET plaintext_text = NULL, encoded_content = $2, encoded_codec = $3, encoded_generation = $4 \
         WHERE uri LIKE $1",
    )
    .bind(pattern)
    .bind(encoded.content.clone())
    .bind(encoded.codec.as_str())
    .bind(encoded.generation.as_ref().map(|g| g.as_str().to_string()))
    .execute(pool)
    .await
    .expect("undecodable");
}

/// Part (3) in process (§4): each endpoint's p99 under its budget on the
/// base snapshot, the new shapes and the non-adversarial fixtures, and the
/// named adversarial fixtures reported (gated where §4 gates them). Since
/// amendment 1, B-5's part (3) runs on the deploy build
/// (`b5_release::b5_release_floors`); this in-process run stays as a quick
/// look at the same fixtures.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore = "B-5 part (3) in process; run with --release --ignored --nocapture"]
async fn b5_absolute_floors() {
    println!("B-5 part (3): {} samples per endpoint, concurrency 8, heads fresh", samples());
    // Every fixture runs, and the failures are reported together.
    let mut failures: Vec<String> = Vec::new();
    for fixture in Part3::ALL {
        let Some((mut w, heads)) = part3_walk(&format!("b5-{}", fixture.slug())).await else {
            return;
        };
        let cases = fixture.build(&w.f.pool).await;
        exec(&w.f.pool, "ANALYZE").await;
        for cap in fixture.caps() {
            let read = PrivateReadConfig { max_chain: *cap, ..PrivateReadConfig::default() };
            let title = if fixture.caps().len() > 1 { format!("{}, cap {cap}", fixture.title()) } else { fixture.title().to_string() };
            run_fixture(&mut w, &title, &read, &heads, &cases).await;
        }
        failures.extend(w.drain().await);
    }
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
