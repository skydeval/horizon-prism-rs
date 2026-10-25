//! The live feed over HTTP (admin-ui-live-feed design §11): T-F2's stream
//! part, T-F3, T-F5 to T-F9 and T-F11. T-F4 is in the private tier's
//! tests, where its fixtures are; T-F1 in the writer's; T-F10 in the
//! tap's.

use serde_json::Value;

use super::*;
use crate::admin::test_support::{Sse, page_id, session};
use crate::firehose::feed::testing::written;
use crate::firehose::feed::{BROADCAST_CAPACITY, BUCKET_PER_SEC, KINDS, Kind};
use crate::firehose::router::route_identity;
use crate::firehose::stream::IdentityKind;
use crate::firehose::writer::{Action, ApplyOptions, RawOp, WriteSource, Writer};
use crate::indexing::{PublicMode, PublicState};
use crate::private_tier::DisabledPrivateTier;

const AUTHOR: &str = "did:plc:a1a1a1a1a1a1a1a1a1a1a1a1";
const NO_HANDLE: &str = "did:plc:a2a2a2a2a2a2a2a2a2a2a2a2";
const OUTSIDER: &str = "did:plc:zzzzzzzzzzzzzzzzzzzzzzzz";

const OPTIONS: ApplyOptions = ApplyOptions {
    advance_applied_rev: false,
    dead_letter_readiness: false,
    attempts: 1,
    live: true,
    subscription_rules: false,
    source: WriteSource::Firehose,
};

fn live_path(page: &str, query: &str) -> String {
    format!("/admin/live?page={page}{query}")
}

async fn open(h: &Harness, cookie: &str, page: &str, query: &str) -> Sse {
    Sse::open(&h.base, &live_path(page, query), Some(cookie)).await
}

/// A writer on the harness's database, telling its hub.
fn writer(h: &Harness, sets: ScopeSets) -> Writer {
    Writer::new(h.pool().clone(), ScopeState::new(sets, ScopePolicy::MembersAndOneHop, 10_000), Arc::new(DisabledPrivateTier)).with_feed(h.hub.tap())
}

fn in_scope(dids: &[&str]) -> ScopeSets {
    let mut sets = ScopeSets::default();
    for d in dids {
        sets.scope.insert((*d).to_string());
    }
    sets
}

fn op(action: Action, collection: &str, rkey: &str, record: Option<Value>) -> RawOp {
    RawOp { action, collection: collection.into(), rkey: rkey.into(), cid: record.as_ref().map(|_| format!("bafy{rkey}")), record }
}

fn post(rkey: &str, text: &str) -> RawOp {
    op(Action::Create, "app.bsky.feed.post", rkey, Some(json!({"text": text, "createdAt": "2026-10-19T00:00:00Z"})))
}

/// Every line of the `lines` events, and the summed `counts`.
fn lines_of(events: &[(String, String)]) -> Vec<Value> {
    events
        .iter()
        .filter(|(n, _)| n == "lines")
        .flat_map(|(_, d)| serde_json::from_str::<Vec<Value>>(d).expect("lines"))
        .collect()
}

fn counts_of(events: &[(String, String)]) -> Vec<Value> {
    events.iter().filter(|(n, _)| n == "counts").map(|(_, d)| serde_json::from_str(d).expect("counts")).collect()
}

async fn last_seen(pool: &PgPool, cookie: &str) -> chrono::DateTime<chrono::Utc> {
    sqlx::query_scalar("SELECT last_seen_at FROM admin_sessions WHERE id_hash = $1")
        .bind(test_support::id_hash(cookie))
        .fetch_one(pool)
        .await
        .expect("last_seen")
}

async fn get(h: &Harness, path: &str, cookie: &str) -> Page {
    let b = Browser::new();
    b.send(b.http.get(format!("{}{path}", h.base)).header("cookie", cookie)).await
}

/// Wait until the hub counts `n` open streams.
async fn streams(h: &Harness, n: usize) {
    for _ in 0..100 {
        if h.hub.open_streams() == n {
            return;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert_eq!(h.hub.open_streams(), n);
}

/// T-F3: refusals at open are never redirects; the stream's headers;
/// watching does not move `last_seen_at`; sign-out, removal and a PDS
/// change end an open stream at the next check.
#[tokio::test]
async fn t_f3_session_at_open_and_while_open() {
    let Some(h) = harness("admin_t_f3").await else {
        return;
    };
    let p = h.pool();
    let _alice = complete_setup(&h, ALICE).await;
    let page = page_id();
    let none = Sse::open(&h.base, &live_path(&page, ""), None).await;
    assert_eq!((none.status, none.header("location").as_str()), (401, ""), "no cookie: 401, never a redirect");
    let cookie = session(p, ALICE, PDS).await;
    assert_eq!(open(&h, &cookie, "XYZ", "").await.status, 400, "a malformed page");
    assert_eq!(open(&h, &cookie, &page.to_uppercase(), "").await.status, 400, "upper case is malformed");
    let expired = session(p, ALICE, PDS).await;
    sqlx::query("UPDATE admin_sessions SET expires_at = now() - interval '1 second' WHERE id_hash = $1")
        .bind(test_support::id_hash(&expired))
        .execute(p)
        .await
        .expect("expire");
    let refused = open(&h, &expired, &page, "").await;
    assert_eq!((refused.status, refused.header("location").as_str()), (401, ""), "an expired session");

    // Open: the stream's own headers.
    sqlx::query("UPDATE admin_sessions SET last_seen_at = now() - interval '1 hour' WHERE id_hash = $1")
        .bind(test_support::id_hash(&cookie))
        .execute(p)
        .await
        .expect("age");
    let before = last_seen(p, &cookie).await;
    let mut s = open(&h, &cookie, &page, "").await;
    assert_eq!(s.status, 200);
    assert!(s.header("content-type").starts_with("text/event-stream"), "{}", s.header("content-type"));
    assert_eq!(s.header("cache-control"), "no-store, no-transform", "not axum's no-cache");
    assert_eq!(s.header("x-accel-buffering"), "no");
    assert_eq!(s.header("content-security-policy"), CSP);
    assert!(s.next(Duration::from_secs(2)).await.is_some(), "events flow");
    // Neither the stream, the tiles nor Home's reload with JavaScript off
    // moves last_seen_at.
    let tiles = get(&h, "/admin/tiles", &cookie).await;
    assert_eq!(tiles.status, 200);
    assert!(tiles.body.contains("id=\"tiles\"") && tiles.body.contains("As of"), "{}", tiles.body);
    assert!(tiles.body.contains("<h2>Members</h2>") && !tiles.body.contains("Recent changes"), "{}", tiles.body);
    assert_headers_with(&tiles.headers, "tiles", CSP);
    assert_eq!(get(&h, "/admin?auto=1", &cookie).await.status, 200);
    assert_eq!(last_seen(p, &cookie).await, before, "watching is not activity");
    assert_eq!(get(&h, "/admin", &cookie).await.status, 200);
    assert!(last_seen(p, &cookie).await > before, "a page load is");

    // Sign-out in another request: the stream ends within a check.
    auth_end(p, &cookie).await;
    assert_eq!(s.end_reason(Duration::from_secs(3)).await.as_deref(), Some("signed_out"));
    assert!(s.next(Duration::from_secs(2)).await.is_none(), "the stream closes after end");

    // An operator removed: not_operator.
    sqlx::query("INSERT INTO admin_operators (did, added_by) VALUES ($1, 'cli')").bind(BOB).execute(p).await.expect("bob");
    h.identity.set(BOB, PDS);
    let bob = session(p, BOB, PDS).await;
    let mut s = open(&h, &bob, &page_id(), "").await;
    assert_eq!(s.status, 200);
    sqlx::query("DELETE FROM admin_operators WHERE did = $1").bind(BOB).execute(p).await.expect("remove");
    assert_eq!(s.end_reason(Duration::from_secs(3)).await.as_deref(), Some("not_operator"));

    // A PDS change at the next fresh resolution: pds_changed, and the
    // session is deleted.
    let moving = session(p, ALICE, PDS).await;
    let mut s = open(&h, &moving, &page_id(), "").await;
    assert_eq!(s.status, 200);
    h.identity.set(ALICE, "https://moved.example.com");
    sqlx::query("UPDATE admin_sessions SET checked_at = now() - interval '6 minutes' WHERE id_hash = $1")
        .bind(test_support::id_hash(&moving))
        .execute(p)
        .await
        .expect("age");
    assert_eq!(s.end_reason(Duration::from_secs(3)).await.as_deref(), Some("pds_changed"));
    let left: i64 = sqlx::query_scalar("SELECT count(*) FROM admin_sessions WHERE id_hash = $1")
        .bind(test_support::id_hash(&moving))
        .fetch_one(p)
        .await
        .expect("count");
    assert_eq!(left, 0, "a cut-off deletes the session");
    h.db.drop_db().await;
}

/// Sign-out as `end_session` does it: the row goes.
async fn auth_end(pool: &PgPool, cookie: &str) {
    sqlx::query("DELETE FROM admin_sessions WHERE id_hash = $1").bind(test_support::id_hash(cookie)).execute(pool).await.expect("sign out");
}

/// T-F3: with the minute's check far away, the expiry timer alone ends
/// the stream at the session's idle and absolute limits, and re-arms when
/// a page load in another tab moved `last_seen_at`.
#[tokio::test]
async fn t_f3_expiry_timers() {
    let timing = live::Timing { check: Duration::from_secs(60), ..quick() };
    let Some(h) = harness_with("admin_t_f3_timers", timing, true).await else {
        return;
    };
    let p = h.pool();
    let _alice = complete_setup(&h, ALICE).await;
    let set = |sql: &'static str, cookie: String| async move {
        sqlx::query(sql).bind(test_support::id_hash(&cookie)).execute(p).await.expect(sql);
    };
    // Absolute expiry in 1.5 s.
    let a = session(p, ALICE, PDS).await;
    set("UPDATE admin_sessions SET expires_at = now() + interval '1500 milliseconds' WHERE id_hash = $1", a.clone()).await;
    let started = Instant::now();
    let mut s = open(&h, &a, &page_id(), "").await;
    assert_eq!(s.status, 200);
    assert_eq!(s.end_reason(Duration::from_secs(4)).await.as_deref(), Some("expired"));
    assert!(started.elapsed() >= Duration::from_millis(1200), "at the instant, not before: {:?}", started.elapsed());
    // Idle: only an open Home tab, so it ends at the idle limit.
    let b = session(p, ALICE, PDS).await;
    set("UPDATE admin_sessions SET last_seen_at = now() - interval '8 hours' + interval '1500 milliseconds' WHERE id_hash = $1", b.clone()).await;
    let mut s = open(&h, &b, &page_id(), "").await;
    assert_eq!(s.status, 200);
    let tiles = get(&h, "/admin/tiles", &b).await;
    assert_eq!(tiles.status, 200);
    assert_eq!(s.end_reason(Duration::from_secs(4)).await.as_deref(), Some("expired"));
    // A page load in another tab moves last_seen_at: the timer re-reads the
    // row and re-arms instead of ending.
    let c = session(p, ALICE, PDS).await;
    set("UPDATE admin_sessions SET last_seen_at = now() - interval '8 hours' + interval '1500 milliseconds' WHERE id_hash = $1", c.clone()).await;
    let mut s = open(&h, &c, &page_id(), "").await;
    assert_eq!(s.status, 200);
    assert_eq!(get(&h, "/admin/members", &c).await.status, 200);
    let events = s.collect(Duration::from_secs(3)).await;
    assert!(!events.iter().any(|(n, _)| n == "end"), "{events:?}");
    assert!(!s.ended());
    h.db.drop_db().await;
}

/// T-F5: Home's `200` alone has the live policy.
#[tokio::test]
async fn t_f5_csp_per_page() {
    let Some(h) = harness("admin_t_f5").await else {
        return;
    };
    let p = h.pool();
    let alice = complete_setup(&h, ALICE).await;
    let cookie = format!("{}={}", auth::SESSION_COOKIE, alice.cookie(auth::SESSION_COOKIE).expect("cookie"));
    let home = get(&h, "/admin", &cookie).await;
    assert_eq!(home.status, 200);
    assert_headers_with(&home.headers, "home", CSP_LIVE);
    for path in ["/admin/members", "/admin/audit", "/admin/tiles", "/admin/operators", "/admin/nowhere", "/admin/activity", JS_HREF.as_str(), CSS_HREF.as_str()] {
        assert_headers_with(&get(&h, path, &cookie).await.headers, path, CSP);
    }
    let s = open(&h, &cookie, &page_id(), "").await;
    assert_eq!(s.status, 200);
    assert_headers_with(&s.headers, "stream", CSP);
    drop(s);
    let js = get(&h, JS_HREF.as_str(), &cookie).await;
    assert_eq!(js.headers.get("content-type").and_then(|v| v.to_str().ok()), Some("text/javascript; charset=utf-8"));
    let wrong = alice.send(alice.http.post(format!("{}/admin", h.base)).header("cookie", &cookie)).await;
    assert_eq!(wrong.status, 405);
    assert_headers_with(&wrong.headers, "405", CSP);
    let stranger = Browser::new();
    let redirect = stranger.get(&format!("{}/admin", h.base)).await;
    assert_eq!(redirect.status, 303);
    assert_headers_with(&redirect.headers, "redirect", CSP);
    // Home's error answer has the plain policy.
    sqlx::query("ALTER TABLE member_denials RENAME TO member_denials_away").execute(p).await.expect("break");
    let broken = get(&h, "/admin", &cookie).await;
    assert_eq!(broken.status, 500);
    assert_headers_with(&broken.headers, "home error", CSP);
    sqlx::query("ALTER TABLE member_denials_away RENAME TO member_denials").execute(p).await.expect("mend");
    h.db.drop_db().await;
}

/// T-F6: the same page replaces its own stream and no other; a third page
/// ends the session's oldest; the ninth stream is `429` and ends nobody; a
/// closed stream frees its slot; Home with JavaScript off.
#[tokio::test]
async fn t_f6_caps() {
    let Some(h) = harness("admin_t_f6").await else {
        return;
    };
    let p = h.pool();
    let _alice = complete_setup(&h, ALICE).await;
    let s1 = session(p, ALICE, PDS).await;
    let (pa, pb, pc) = (page_id(), page_id(), page_id());
    let mut a1 = open(&h, &s1, &pa, "").await;
    let mut b = open(&h, &s1, &pb, "").await;
    let mut a2 = open(&h, &s1, &pa, "").await;
    assert_eq!(a2.status, 200);
    assert_eq!(a1.end_reason(Duration::from_secs(2)).await.as_deref(), Some("replaced"));
    let events = b.collect(Duration::from_millis(800)).await;
    assert!(events.iter().any(|(n, _)| n == "counts") && !events.iter().any(|(n, _)| n == "end"), "{events:?}");
    let c = open(&h, &s1, &pc, "").await;
    assert_eq!(c.status, 200);
    assert_eq!(b.end_reason(Duration::from_secs(2)).await.as_deref(), Some("replaced"), "the oldest page");
    streams(&h, 2).await;
    // Three more sessions, two pages each: eight in all.
    let mut others = Vec::new();
    for _ in 0..3 {
        let s = session(p, ALICE, PDS).await;
        for _ in 0..2 {
            let o = open(&h, &s, &page_id(), "").await;
            assert_eq!(o.status, 200);
            others.push(o);
        }
    }
    streams(&h, MAX).await;
    let s9 = session(p, ALICE, PDS).await;
    let ninth = open(&h, &s9, &page_id(), "").await;
    assert_eq!(ninth.status, 429);
    // The cap's 429 names a retry: the soonest a reserved slot can free.
    let secs: u64 = ninth.header("retry-after").parse().expect("Retry-After");
    assert_eq!(secs, crate::xrpc::rate_limit::retry_after_secs(h.hub.timing().reserve));
    assert_eq!(h.hub.open_streams(), MAX, "nobody was ended");
    let events = a2.collect(Duration::from_millis(700)).await;
    assert!(!events.iter().any(|(n, _)| n == "end"), "{events:?}");
    // A closed stream frees its slot.
    drop(others.pop());
    streams(&h, MAX - 1).await;
    let ninth = open(&h, &s9, &page_id(), "").await;
    assert_eq!(ninth.status, 200);
    drop((c, a2, others, ninth));
    h.db.drop_db().await;
}

const MAX: usize = live::MAX_STREAMS;

/// T-F6: every stream ends with `renew` and its page reconnects at once.
#[tokio::test]
async fn t_f6_renewal() {
    let timing = live::Timing { renew: Duration::from_millis(800), ..quick() };
    let Some(h) = harness_with("admin_t_f6_renew", timing, true).await else {
        return;
    };
    let p = h.pool();
    let _alice = complete_setup(&h, ALICE).await;
    let s = session(p, ALICE, PDS).await;
    let page = page_id();
    let mut first = open(&h, &s, &page, "").await;
    assert_eq!(first.end_reason(Duration::from_secs(3)).await.as_deref(), Some("renew"));
    let again = open(&h, &s, &page, "").await;
    assert_eq!(again.status, 200, "the renewed page is admitted");
    h.db.drop_db().await;
}

/// T-F7: shutdown ends an open stream with `shutting_down`, and the
/// server returns within a second.
#[tokio::test]
async fn t_f7_shutdown() {
    let Some(h) = harness("admin_t_f7").await else {
        return;
    };
    let p = h.pool();
    let _alice = complete_setup(&h, ALICE).await;
    let cookie = session(p, ALICE, PDS).await;
    let mut s = open(&h, &cookie, &page_id(), "").await;
    assert_eq!(s.status, 200);
    assert!(s.next(Duration::from_secs(1)).await.is_some());
    let started = Instant::now();
    let tx = h.shutdown.lock().expect("lock").take().expect("tx");
    tx.send(()).expect("shutdown");
    assert_eq!(s.end_reason(Duration::from_secs(1)).await.as_deref(), Some("shutting_down"));
    let served = h.served.lock().expect("lock").take().expect("task");
    tokio::time::timeout(Duration::from_secs(1), served).await.expect("serve returns within 1 s").expect("join");
    assert!(started.elapsed() < Duration::from_secs(1));
    h.db.drop_db().await;
}

/// T-F8: lines as they read, and the filters.
#[tokio::test]
async fn t_f8_lines() {
    let Some(h) = harness("admin_t_f8").await else {
        return;
    };
    let p = h.pool();
    let _alice = complete_setup(&h, ALICE).await;
    sqlx::query("INSERT INTO actors (did, handle) VALUES ($1, 'author.test')").bind(AUTHOR).execute(p).await.expect("actor");
    let cookie = session(p, ALICE, PDS).await;
    let mut all = open(&h, &cookie, &page_id(), "").await;
    let mut blocks = open(&h, &cookie, &page_id(), "&kinds=block").await;
    assert_eq!((all.status, blocks.status), (200, 200));
    let w = writer(&h, in_scope(&[AUTHOR, NO_HANDLE]));
    let long = format!("first line\nsecond\tline \u{202E}{}", "é".repeat(200));
    w.apply_from(Some("bsky"), AUTHOR, "3kaaaaaaaaa22", &[post("3kpost1aaaaaa", &long)], OPTIONS).await.expect("apply");
    w.apply(NO_HANDLE, "3kaaaaaaaaa22", &[post("3kpost2aaaaaa", "short")], OPTIONS).await.expect("apply");
    w.apply(AUTHOR, "3kaaaaaaaaa23", &[op(Action::Delete, "app.bsky.feed.post", "3kpost1aaaaaa", None)], OPTIONS).await.expect("apply");
    let block = |rkey: &str| op(Action::Create, "app.bsky.graph.block", rkey, Some(json!({"subject": OUTSIDER, "createdAt": "2026-10-19T00:00:00Z"})));
    w.apply(AUTHOR, "3kaaaaaaaaa24", &[block("3kblock1aaaaa")], OPTIONS).await.expect("apply");
    let rejected = w.apply(OUTSIDER, "3kaaaaaaaaa22", &[post("3kpost3aaaaaa", "outside")], OPTIONS).await.expect("apply");
    assert!(matches!(rejected.outcomes[0], crate::firehose::writer::OpOutcome::Rejected("not_in_scope")));
    // Under the off rule, a member's post: reference only, no snippet.
    let mut off = in_scope(&[NO_HANDLE]);
    off.members.insert(NO_HANDLE.to_string());
    off.public = PublicState { mode: PublicMode::Off, ..PublicState::default() };
    writer(&h, off).apply(NO_HANDLE, "3kaaaaaaaaa25", &[post("3kpost4aaaaaa", "kept as a reference")], OPTIONS).await.expect("apply");
    // An identity event for an unknown DID: counted, no line.
    let before = h.hub.tap().totals().identity_events();
    let queued = route_identity(p, IdentityKind::Handle, OUTSIDER, &crate::system_accounts::State::Off).await.expect("route");
    assert!(!queued);
    h.hub.tap().identity(IdentityKind::Handle, OUTSIDER, queued, Some("bsky"));
    assert_eq!(h.hub.tap().totals().identity_events(), before + 1);

    let events = all.collect(Duration::from_millis(1500)).await;
    let lines = lines_of(&events);
    let by = |did: &str, act: &str| lines.iter().find(|l| l["did"] == did && l["act"] == act).cloned();
    let first = by(AUTHOR, "created").expect("the author's post");
    assert_eq!((first["who"].as_str(), first["k"].as_str(), first["src"].as_str(), first["relay"].as_str()), (Some("author.test"), Some("post"), Some("network"), Some("bsky")));
    let text = first["text"].as_str().expect("a snippet");
    assert!(text.chars().count() <= 100 && text.ends_with('…'), "{text}");
    assert!(!text.contains('\n') && !text.contains('\t') && !text.contains('\u{202E}'), "{text}");
    assert!(text.starts_with("first line second line "), "{text}");
    let second = by(NO_HANDLE, "created").expect("the other post");
    assert!(second.get("who").is_none(), "no stored handle: the DID");
    assert_eq!(second["text"], "short");
    assert!(second.get("relay").is_none(), "no relay label: network alone");
    assert!(by(AUTHOR, "deleted").is_some(), "{lines:?}");
    let reference = by(NO_HANDLE, "reference only").expect("the reference");
    assert!(reference.get("text").is_none());
    // A rejected op is a line on the firehose ticker alone, marked by its
    // reason, never with text (Changed by skydeval 2026-10-19); an unknown
    // DID's identity event is still no line.
    let outside: Vec<&Value> = lines.iter().filter(|l| l["did"] == OUTSIDER).collect();
    assert_eq!(outside.len(), 1, "{lines:?}");
    assert_eq!((outside[0]["o"].as_str(), outside[0]["k"].as_str(), outside[0]["f"].as_u64()), (Some("not_in_scope"), Some("post"), Some(1)));
    assert!(outside[0].get("i").is_none() && outside[0].get("text").is_none(), "{:?}", outside[0]);
    assert_eq!((first["f"].as_u64(), first["i"].as_u64()), (Some(1), Some(1)), "an indexed post is on both tickers");
    assert!(lines.iter().filter(|l| l.get("o").is_some()).all(|l| l.get("text").is_none() && l.get("i").is_none()), "{lines:?}");
    let not_in_scope: u64 = counts_of(&events).iter().filter_map(|c| c["reasons"]["not_in_scope"].as_u64()).sum();
    assert!(not_in_scope >= 1, "{events:?}");
    // kinds=block: only the block.
    let blocked = lines_of(&blocks.collect(Duration::from_millis(600)).await);
    assert_eq!(blocked.len(), 1, "{blocked:?}");
    assert_eq!(blocked[0]["k"], "block");
    assert!(blocked[0].get("text").is_none(), "only a post has a snippet");

    // 5 000 likes and 20 blocks a second: a kinds=block stream receives
    // every block (the likes are unwanted and spend no block token).
    drop(all);
    streams(&h, 1).await;
    for _ in 0..20 {
        for _ in 0..250 {
            h.hub.tap().observe(std::iter::once(written(Kind::Like, None)), WriteSource::Firehose, None, AUTHOR);
        }
        h.hub.tap().observe(std::iter::once(written(Kind::Block, None)), WriteSource::Firehose, None, AUTHOR);
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let blocked = lines_of(&blocks.collect(Duration::from_millis(1200)).await);
    assert_eq!(blocked.len(), 20, "every block line");
    h.db.drop_db().await;
}

/// The backlog's lines: the first event of a stream, which must be one.
async fn backlog_of(s: &mut Sse) -> Vec<Value> {
    let (name, data) = s.next(Duration::from_secs(3)).await.expect("an event");
    assert_eq!(name, "backlog", "the backlog comes first: {data}");
    serde_json::from_str(&data).expect("backlog")
}

fn seq(l: &Value) -> u64 {
    l["n"].as_u64().expect("every line has a number")
}

/// T-F12 the backlog (Changed by skydeval 2026-10-19, second pass): a new
/// stream starts with each ticker's newest lines. "Indexed here" fills with
/// nobody watching, "On the firehose" only while someone does and keeps its
/// lines after; the stream's filter applies; a line that was not indexed
/// carries no text; a reconnect's backlog and live lines never repeat a
/// line under a new number, and live lines start past the backlog.
#[tokio::test]
async fn t_f12_backlog() {
    let Some(h) = harness("admin_t_f12").await else {
        return;
    };
    let p = h.pool();
    let _alice = complete_setup(&h, ALICE).await;
    let cookie = session(p, ALICE, PDS).await;
    // The block names a member, so Rule M stores it (member-blocks-only
    // design §2).
    const BLOCKED_MEMBER: &str = "did:plc:mmmmmmmmmmmmmmmmmmmmmmmm";
    let mut sets = in_scope(&[AUTHOR]);
    sets.members.insert(BLOCKED_MEMBER.to_string());
    let w = writer(&h, sets);
    let block = |rkey: &str| op(Action::Create, "app.bsky.graph.block", rkey, Some(json!({"subject": BLOCKED_MEMBER, "createdAt": "2026-10-19T00:00:00Z"})));

    // Nobody watching: an indexed post and a rejected one.
    w.apply_from(Some("bsky"), AUTHOR, "3kaaaaaaaaa22", &[post("3kpost1aaaaaa", "kept for a reload")], OPTIONS).await.expect("apply");
    w.apply(OUTSIDER, "3kaaaaaaaaa22", &[post("3kpost2aaaaaa", "outside-marker-1")], OPTIONS).await.expect("apply");
    assert_eq!(h.hub.tap().arriving_built(), 0, "no firehose line with nobody watching");
    let mut first = open(&h, &cookie, &page_id(), "").await;
    assert_eq!(first.status, 200);
    let backlog = backlog_of(&mut first).await;
    assert_eq!(backlog.len(), 1, "{backlog:?}");
    assert_eq!((backlog[0]["did"].as_str(), backlog[0]["k"].as_str(), backlog[0]["text"].as_str()), (Some(AUTHOR), Some("post"), Some("kept for a reload")));
    assert_eq!((backlog[0]["i"].as_u64(), backlog[0].get("f")), (Some(1), None), "on the indexed ticker only");

    // Watched: a rejected post with text, and a block; then nobody again.
    w.apply(OUTSIDER, "3kaaaaaaaaa23", &[post("3kpost3aaaaaa", "outside-marker-2")], OPTIONS).await.expect("apply");
    w.apply(AUTHOR, "3kaaaaaaaaa24", &[block("3kblock1aaaaa")], OPTIONS).await.expect("apply");
    let live = lines_of(&first.collect(Duration::from_millis(800)).await);
    assert_eq!(live.len(), 2, "{live:?}");
    drop(first);
    streams(&h, 0).await;

    // A reload: the firehose lines from before, the indexed ones too.
    let page = page_id();
    let mut second = open(&h, &cookie, &page, "").await;
    let backlog = backlog_of(&mut second).await;
    let numbers: Vec<u64> = backlog.iter().map(seq).collect();
    assert!(numbers.windows(2).all(|w| w[0] < w[1]), "oldest first: {numbers:?}");
    let outside = backlog.iter().find(|l| l["did"] == OUTSIDER).expect("the rejected post, kept after the stream closed");
    assert_eq!((outside["o"].as_str(), outside["f"].as_u64()), (Some("not_in_scope"), Some(1)));
    assert!(outside.get("text").is_none() && outside.get("i").is_none(), "{outside}");
    assert!(!backlog.iter().any(|l| l.to_string().contains("outside-marker")), "{backlog:?}");
    let blocked = backlog.iter().find(|l| l["k"] == "block").expect("the block");
    assert_eq!((blocked["f"].as_u64(), blocked["i"].as_u64()), (Some(1), Some(1)), "kept for both tickers, sent once");
    for l in &live {
        assert!(backlog.iter().any(|b| seq(b) == seq(l)), "the backlog repeats {l} under its own number");
    }
    // Every line not indexed in the backlog has no text.
    assert!(backlog.iter().filter(|l| l.get("o").is_some()).all(|l| l.get("text").is_none() && l.get("i").is_none()), "{backlog:?}");

    // The stream's filter applies to the backlog.
    let mut blocks = open(&h, &cookie, &page_id(), "&kinds=block").await;
    let filtered = backlog_of(&mut blocks).await;
    assert_eq!(filtered.iter().map(|l| l["k"].as_str()).collect::<Vec<_>>(), [Some("block")], "{filtered:?}");
    drop(blocks);

    // A reconnect of the same page: the backlog holds what the page saw,
    // under the same numbers, and live lines start past it.
    w.apply(AUTHOR, "3kaaaaaaaaa25", &[post("3kpost4aaaaaa", "while open")], OPTIONS).await.expect("apply");
    let seen = lines_of(&second.collect(Duration::from_millis(800)).await);
    let while_open = seen.iter().find(|l| l["text"] == "while open").expect("the live post");
    let mut again = open(&h, &cookie, &page, "").await;
    assert_eq!(second.end_reason(Duration::from_secs(2)).await.as_deref(), Some("replaced"));
    let backlog = backlog_of(&mut again).await;
    let repeated = backlog.iter().find(|l| l["text"] == "while open").expect("in the backlog");
    assert_eq!(seq(repeated), seq(while_open), "the same number, so the page takes it once");
    let top = backlog.iter().map(seq).max().expect("lines");
    w.apply(AUTHOR, "3kaaaaaaaaa26", &[post("3kpost5aaaaaa", "after the reconnect")], OPTIONS).await.expect("apply");
    let after = lines_of(&again.collect(Duration::from_millis(800)).await);
    assert!(!after.is_empty() && after.iter().all(|l| seq(l) > top), "{after:?}");
    assert!(!after.iter().any(|l| l["text"] == "while open"), "{after:?}");
    h.db.drop_db().await;
}

/// T-F2 (stream part): a client that reads nothing never holds up the
/// writer; what its stream did not show is counted exactly.
#[tokio::test]
async fn t_f2_a_stream_that_is_not_read() {
    let Some(h) = harness("admin_t_f2").await else {
        return;
    };
    let p = h.pool();
    let _alice = complete_setup(&h, ALICE).await;
    let cookie = session(p, ALICE, PDS).await;
    let mut s = open(&h, &cookie, &page_id(), "").await;
    assert_eq!(s.status, 200);
    streams(&h, 1).await;
    // Every bucket's burst at once, with the stream's task not running in
    // between: more than the broadcast holds.
    for _ in 0..BUCKET_PER_SEC {
        for kind in Kind::ALL {
            for source in [WriteSource::Firehose, WriteSource::Backfill] {
                h.hub.tap().observe(std::iter::once(written(kind, None)), source, None, AUTHOR);
            }
        }
    }
    assert_eq!(h.hub.tap().lines_built(), (2 * KINDS) as u64 * u64::from(BUCKET_PER_SEC));
    assert!(h.hub.tap().lines_built() > BROADCAST_CAPACITY as u64);
    // A following write completes while the stream lags.
    let w = writer(&h, in_scope(&[AUTHOR]));
    tokio::time::timeout(Duration::from_secs(5), w.apply(AUTHOR, "3kaaaaaaaaa22", &[post("3kpostzaaaaaa", "after")], OPTIONS))
        .await
        .expect("apply is not held up")
        .expect("apply");
    let events = s.collect(Duration::from_millis(2500)).await;
    let shown = lines_of(&events).len() as u64;
    let counted: u64 = counts_of(&events).iter().map(|c| c["shown"].as_u64().unwrap_or(0) + c["not_shown"].as_u64().unwrap_or(0)).sum();
    let built = h.hub.tap().lines_built();
    assert_eq!(counted, built, "shown plus not shown is every line built");
    assert!(shown < built, "most were not shown");
    // The server's cap, per ticker: at most LINES_PER_SEC a second each.
    let lines = lines_of(&events);
    for lane in ["f", "i"] {
        let n = lines.iter().filter(|l| l.get(lane).is_some()).count() as u64;
        assert!(n <= u64::from(live::LINES_PER_SEC) * 4, "{lane}: {n}");
    }
    h.db.drop_db().await;
}

/// T-F9: the audit log, the old address's `308` with its query, and its
/// place in the navigation.
#[tokio::test]
async fn t_f9_audit_log() {
    let Some(h) = harness("admin_t_f9").await else {
        return;
    };
    let alice = complete_setup(&h, ALICE).await;
    let audit = alice.get(&format!("{}/admin/audit", h.base)).await;
    assert_eq!(audit.status, 200);
    assert!(audit.body.contains("<h1>Audit log</h1>") && audit.body.contains("Sign-ins"), "{}", audit.body);
    let moved = alice.get(&format!("{}/admin/activity?who=cli", h.base)).await;
    assert_eq!(moved.status, 308);
    assert_eq!(moved.headers.get(LOCATION).and_then(|v| v.to_str().ok()), Some("/admin/audit?who=cli"));
    let bare = alice.get(&format!("{}/admin/activity", h.base)).await;
    assert_eq!((bare.status, bare.headers.get(LOCATION).and_then(|v| v.to_str().ok())), (308, Some("/admin/audit")));
    let nav = &audit.body[audit.body.find("<nav>").expect("nav")..audit.body.find("</nav>").expect("nav end")];
    // Settings is last since #810; the audit log is still in the menu.
    let last = nav.rfind("<a ").expect("a link");
    assert!(nav[last..].contains("/admin/settings") && nav.contains("aria-current=\"page\">Audit log</a>"), "{nav}");
    assert!(!audit.body.contains(">Activity<"));
    h.db.drop_db().await;
}

/// T-F11: Home says, until its script runs, that the feed did not start,
/// and shows when its tiles are from; the script hides the notice first.
#[tokio::test]
async fn t_f11_script_not_started() {
    let Some(h) = harness("admin_t_f11").await else {
        return;
    };
    let alice = complete_setup(&h, ALICE).await;
    let home = alice.get(&format!("{}/admin", h.base)).await;
    assert_eq!(home.status, 200);
    let notice = home.body.find("id=\"live-notstarted\"").expect("the notice");
    let tag = &home.body[home.body[..notice].rfind('<').expect("tag")..notice + home.body[notice..].find('>').expect("tag end")];
    assert!(!tag.contains("hidden"), "visible by default: {tag}");
    assert!(home.body.contains("The live feed did not start."));
    assert!(home.body.contains("As of "));
    // One row: the Members column first, then the live feed; no list of
    // recent changes.
    let row = home.body.find("<div class=\"home-row\">").expect("the row");
    let members = home.body.find("<h2>Members</h2>").expect("Members");
    let live = home.body.find("id=\"live\"").expect("the feed");
    assert!(row < members && members < live, "{}", home.body);
    assert!(!home.body.contains("Recent changes"), "{}", home.body);
    assert!(JS.contains("'first via '"), "the relay reads as the first to deliver");
    // Two fixed-height tickers (Changed by skydeval 2026-10-19), paced by
    // one constant, with a short queue and lines removed past the bottom.
    let arriving = home.body.find("On the firehose").expect("the firehose ticker");
    let indexed = home.body.find("Indexed here").expect("the indexed ticker");
    assert!(arriving < indexed);
    assert!(JS.contains("var PACE_MS = 200;") && JS.contains("}, PACE_MS);"), "one pace constant");
    assert!(JS.contains("var QUEUE_MAX = 20;") && JS.contains("queue.splice(0, queue.length - QUEUE_MAX)"), "the newest wait");
    assert!(JS.contains("var ROWS_KEPT = 16;") && JS.contains("while (list.childNodes.length > ROWS_KEPT) list.removeChild(list.lastChild);"));
    assert!(JS.contains("if (paused ||"), "pause freezes the tickers");
    let rule = &CSS[CSS.find(".live-list {").expect("the list rule")..];
    let rule = &rule[..rule.find('}').expect("rule end")];
    assert!(rule.contains("overflow: hidden;") && rule.contains("height: calc("), "{rule}");
    assert!(!CSS.contains("overflow-y"), "no scrollbar");
    assert!(home.body.replace("&#x2f;", "/").contains(&format!("<script src=\"{}\" defer></script>", *JS_HREF)), "{}", home.body);
    assert!(home.body.contains("<noscript><meta http-equiv=\"refresh\" content=\"30;url=/admin?auto=1\"></noscript>"));
    assert!(home.body.contains("The live feed needs JavaScript."));
    assert!(!home.body.contains("style="), "no inline style");
    let hide = JS.find("notice.hidden = true").expect("the hide call");
    let first_code = JS.find("var notice").expect("start");
    assert!(first_code < hide && hide < JS.find("var PACE_MS").expect("rest"), "the first act");
    assert!(!JS.contains("innerHTML") && !JS.contains(".style") && !JS.contains("eval("), "textContent and classes only");
    // Totals with JavaScript off: the server renders the figures.
    h.hub.tap().observe(std::iter::once(written(Kind::Post, None)), WriteSource::Firehose, None, AUTHOR);
    let home = alice.get(&format!("{}/admin?auto=1", h.base)).await;
    assert!(home.body.contains("<p class=\"muted live-since\">Since ") && home.body.contains("<span class=\"l\">post</span>"), "{}", home.body);
    assert_eq!(stat_rows(&home.body), [live::STAT_SLOTS; 3], "one kind: blank rows fill the columns");
    // Five kinds: still three rows a column, the last "+3 more" naming the rest.
    for kind in [Kind::Like, Kind::Follow, Kind::Repost, Kind::Block] {
        h.hub.tap().observe(std::iter::once(written(kind, None)), WriteSource::Firehose, None, AUTHOR);
    }
    let home = alice.get(&format!("{}/admin?auto=1", h.base)).await;
    assert_eq!(stat_rows(&home.body), [live::STAT_SLOTS; 3], "{}", home.body);
    assert!(home.body.contains("<span class=\"l\">+3 more</span>"), "{}", home.body);
    assert!(home.body.contains("<div class=\"fact\" title=\""), "the rest in a tooltip: {}", home.body);
    h.db.drop_db().await;
}

/// The figure rows of each of the live section's three columns.
fn stat_rows(body: &str) -> Vec<usize> {
    let rates = &body[body.find("id=\"live-rates\"").expect("rates")..];
    let rates = &rates[..rates.find("id=\"live-controls\"").expect("controls")];
    rates.split("<h3>").skip(1).map(|col| col.matches("<div class=\"fact").count()).collect()
}

/// Nothing on Home moves when events arrive (Changed by skydeval
/// 2026-10-19, second pass), and the help is a real button over the card.
#[tokio::test]
async fn t_f13_a_stable_card_and_its_help() {
    let Some(h) = harness("admin_t_f13").await else {
        return;
    };
    let alice = complete_setup(&h, ALICE).await;
    let home = alice.get(&format!("{}/admin", h.base)).await;
    assert_eq!(home.status, 200);
    let body = &home.body;
    // The figures: three columns of the same rows, whatever is counted.
    assert_eq!(stat_rows(body), [live::STAT_SLOTS; 3], "{body}");
    assert!(JS.contains(&format!("var SLOTS = {};", live::STAT_SLOTS)), "the script lays out the same rows");
    assert!(JS.contains("'+' + rest.length + ' more'") && JS.contains("r.row.title = tip"), "+N more, the rest in a tooltip");
    assert!(JS.contains("if (stats) return stats;"), "the figures are built once, then their text changes");
    // The short "since" line: one line, numbers that keep their width.
    let since = &CSS[CSS.find(".live-since {").expect("since rule")..];
    let since = &since[..since.find('}').expect("end")];
    assert!(since.contains("white-space: nowrap;") && since.contains("height: 1.4em;") && since.contains("tabular-nums"), "{since}");
    assert!(!body.contains("Since Prism started ("), "no long timestamp");
    for rule in ["#live-rates .fact {", "#live-rates .fact .v {"] {
        assert!(CSS.contains(rule), "{rule}");
    }
    assert!(CSS[CSS.find("#live-rates .fact {").expect("fact")..].starts_with("#live-rates .fact { height: "), "a fixed row height");
    assert!(CSS.contains(".live-head .pill { min-width:"), "the status pill keeps its width");
    assert!(CSS.contains("#live-pause { min-width:"), "Pause and Resume keep the controls still");
    // The sampling sentence moved into the help.
    assert!(!JS.contains("The tickers show up to") && !body.contains("The tickers show up to"));
    // The help: a real button that says what it controls, hidden until the
    // script runs; a panel over the card, closed by Escape.
    let btn = &body[body.find("<button class=\"help-btn\"").expect("the help button")..];
    let btn = &btn[..btn.find('>').expect("end")];
    for attr in ["type=\"button\"", "id=\"live-help-btn\"", "aria-expanded=\"false\"", "aria-controls=\"live-help\"", "aria-label=", " hidden"] {
        assert!(btn.contains(attr), "{attr}: {btn}");
    }
    let panel = &body[body.find("id=\"live-help\"").expect("the panel")..];
    let panel_tag = &panel[..panel.find('>').expect("end")];
    assert!(panel_tag.contains("hidden"), "closed at first: {panel_tag}");
    let panel = &panel[..panel.find("</div>").expect("panel end")];
    for words in ["On the firehose", "Indexed here", "a sample", "first via", "duplicate", "public data off", "not in scope"] {
        assert!(panel.contains(words), "{words}: {panel}");
    }
    let rule = &CSS[CSS.find(".live-help {").expect("help rule")..];
    let rule = &rule[..rule.find('}').expect("end")];
    assert!(rule.contains("position: absolute;"), "over the card, not in it: {rule}");
    assert!(JS.contains("e.key === 'Escape'") && JS.contains("helpBtn.setAttribute('aria-expanded'") && JS.contains("helpBtn.hidden = false;"));
    // Tickers: a fixed header and box; backlog lines appear without the slide.
    let head = &CSS[CSS.find(".live-tickers h3 {").expect("ticker head")..];
    assert!(head[..head.find('}').expect("end")].contains("height: 1.5em;"));
    assert!(JS.contains("lineEl(queue.shift(), withText, true)") && JS.contains("lineEl(queue.shift(), withText, false)"));
    // The backlog matches the page's queue, so a reload fills a ticker.
    assert!(JS.contains(&format!("var QUEUE_MAX = {};", crate::firehose::feed::BACKLOG_LINES)));
    assert!(JS.contains("es.addEventListener('backlog', onBacklog);") && JS.contains("if (l.n <= last) return false;"), "a line is taken once");
    // The tiles keep their height as their figures change.
    assert!(CSS.contains("#tiles .stat { flex-wrap: nowrap; }"));
    assert!(!body.contains("style="), "no inline style");
    h.db.drop_db().await;
}

/// The private-table names the image build refuses outside the private
/// tier appear in no file of the live feed (§6.2).
#[test]
fn the_live_feed_names_no_private_table() {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
    for file in ["firehose/feed.rs", "admin/live/mod.rs", "admin/live/stream.rs", "admin/static/live.js", "admin/templates/home.html"] {
        let text = std::fs::read_to_string(root.join(file)).expect(file);
        for table in ["posts", "audiences", "blocks", "tombstones", "threadgates", "postgates", "notifications", "bookmarks"] {
            assert!(!text.contains(&format!("private_{table}")), "{file} names private_{table}");
        }
    }
}
