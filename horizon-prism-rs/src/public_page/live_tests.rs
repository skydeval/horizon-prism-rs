//! The live feed against a real Redis (public-page design §10): T-P5's relay
//! lines and input, T-P16, T-P16a, T-P17's no-consumer and group cases,
//! T-P18, T-P19 at full rate, T-P19a, T-P22, T-P23 and T-P24. They run when
//! `HPRS_TEST_REDIS_URL` is set, each on a stream key of its own. Timings
//! the design gives in tens of seconds are shortened through the settings.

use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

use redis::aio::ConnectionManager;
use serde_json::{Value, json};

use super::decode::Labels;
use super::live::tests::lines_of;
use super::live::{FeedSettings, PublicFeed};
use super::reader::{Probe, ReaderSettings, local_ms};
use super::status::FeedHandle;
use super::tests::{app_state, finalized, page_state, serve_app, snapshot};
use super::viewers::{LiveState, ViewerSettings, Viewers};
use super::*;

const RELAY_A: &str = "wss://zqrelayalpha.zqmarkhost.invalid/xrpc/com.atproto.sync.subscribeRepos";
const RELAY_B: &str = "wss://ZQRelayBeta.zqmarkhost.invalid";

pub(super) async fn redis() -> Option<(ConnectionManager, String)> {
    let Ok(url) = std::env::var("HPRS_TEST_REDIS_URL") else {
        eprintln!("HPRS_TEST_REDIS_URL unset; skipping a live feed test");
        return None;
    };
    let client = redis::Client::open(url.as_str()).expect("redis url");
    Some((ConnectionManager::new(client).await.expect("redis"), url))
}

fn labels() -> Labels {
    Labels::new(&super::relay_hosts(&super::tests::marker_relays()))
}

fn fast() -> ReaderSettings {
    ReaderSettings {
        behind: Duration::from_secs(3),
        backoff_first: Duration::from_millis(200),
        backoff_max: Duration::from_secs(1),
        timeout: Duration::from_secs(1),
        ..ReaderSettings::default()
    }
}

/// A feed on its own stream key.
pub(super) struct Rig {
    pub conn: ConnectionManager,
    pub key: String,
    pub feed: PublicFeed,
    pub summary: FeedHandle,
    pub probe: Arc<Probe>,
    tasks: Vec<tokio::task::JoinHandle<()>>,
}

impl Drop for Rig {
    fn drop(&mut self) {
        for t in &self.tasks {
            t.abort();
        }
    }
}

pub(super) async fn rig_with(conn: ConnectionManager, rs: ReaderSettings, key: Option<String>) -> Rig {
    let key = key.unwrap_or_else(|| format!("test:pp:{}", rand::random::<u32>()));
    let summary: FeedHandle = Arc::default();
    let probe = Arc::new(Probe::default());
    let (feed, tasks) = PublicFeed::start(conn.clone(), &key, labels(), Arc::clone(&summary), rs, FeedSettings::default(), Arc::clone(&probe));
    Rig { conn, key, feed, summary, probe, tasks }
}

async fn rig() -> Option<Rig> {
    let (conn, _) = redis().await?;
    Some(rig_with(conn, fast(), None).await)
}

pub(super) fn commit(repo: &str, ops: &Value, relay: &str, time: Option<String>) -> Vec<(String, String)> {
    let mut data = json!({"repo": repo, "rev": "3kaaaaaaaaa22", "ops": ops, "_relay": relay});
    if let Some(t) = time {
        data["time"] = json!(t);
    }
    vec![("type".to_string(), "commit".to_string()), ("data".to_string(), data.to_string())]
}

pub(super) fn event(ty: &str, data: &Value) -> Vec<(String, String)> {
    vec![("type".to_string(), ty.to_string()), ("data".to_string(), data.to_string())]
}

pub(super) fn like(rkey: &str) -> Value {
    json!([{"action": "create", "path": format!("app.bsky.feed.like/{rkey}"), "cid": "bafy", "record": {"subject": {}}}])
}

pub(super) async fn xadd(conn: &mut ConnectionManager, key: &str, id: &str, fields: &[(String, String)]) -> String {
    let mut cmd = redis::cmd("XADD");
    cmd.arg(key).arg(id);
    for (k, v) in fields {
        cmd.arg(k).arg(v);
    }
    cmd.query_async(conn).await.expect("xadd")
}

/// Wait until the reader has anchored (its first empty poll).
async fn anchored(r: &Rig) {
    tokio::time::timeout(Duration::from_secs(5), r.probe.polled_empty.notified()).await.expect("the reader polls");
}

/// Every `lines` event's lines from `rx` within `within`, until `want` lines.
async fn collect_lines(rx: &mut tokio::sync::broadcast::Receiver<Arc<super::live::Frame>>, want: usize, within: Duration) -> (Vec<(i64, String, String, String)>, Vec<Value>) {
    let until = tokio::time::Instant::now() + within;
    let mut lines = Vec::new();
    let mut counts = Vec::new();
    while lines.len() < want {
        match tokio::time::timeout_at(until, rx.recv()).await {
            Ok(Ok(f)) => {
                // No live frame names a relay.
                let raw = String::from_utf8_lossy(&f.bytes);
                assert_eq!(super::tests::relay_marker_in(&raw), None, "{raw}");
                assert!(!raw.to_lowercase().contains("zqrelayunlisted"), "{raw}");
                let (name, v) = super::live::tests::data(&f.bytes);
                match name.as_str() {
                    "lines" => lines.extend(lines_of(&v)),
                    "counts" => counts.push(v),
                    _ => {}
                }
            }
            Ok(Err(tokio::sync::broadcast::error::RecvError::Lagged(n))) => panic!("lagged {n}"),
            _ => break,
        }
    }
    (lines, counts)
}

async fn del(conn: &mut ConnectionManager, key: &str) {
    let _: () = redis::cmd("DEL").arg(key).query_async(conn).await.expect("del");
}

/// T-P16 and T-P16a: every public line is relay fields only, decoded through
/// the event's dictionaries; `tools.kryphocron.*` commits, mixed ones
/// included, give no line and still count; empty commits give none.
#[tokio::test]
async fn t_p16_relay_fields_only_and_kryphocron_hidden() {
    let Some(mut r) = rig().await else {
        return;
    };
    let (mut rx, _, _, _) = r.feed.subscribe();
    anchored(&r).await;
    let k = r.key.clone();
    let c = &mut r.conn;
    // Ops whatever Prism would do with them, a post with a marker text, an
    // identity event for two DIDs, an inactive account.
    let post = json!([{"action": "create", "path": "app.bsky.feed.post/1", "record": {"text": "MARKER-TEXT"}}]);
    let entries = vec![
        commit("did:plc:like0aaaaaaaaaaaaaaaaaaa", &like("1"), RELAY_A, None),
        commit("did:plc:privaaaaaaaaaaaaaaaaaaaa", &json!([{"action": "create", "path": "tools.kryphocron.feed.postPrivate/1"}]), RELAY_A, None),
        commit("did:plc:like1aaaaaaaaaaaaaaaaaaa", &like("2"), RELAY_B, None),
        commit(
            "did:plc:mixedaaaaaaaaaaaaaaaaaaa",
            &json!([{"action": "create", "path": "app.bsky.feed.like/3"}, {"action": "create", "path": "tools.kryphocron.graph.block/1"}]),
            RELAY_A,
            None,
        ),
        commit("did:plc:unregaaaaaaaaaaaaaaaaaaa", &json!([{"action": "delete", "path": "tools.kryphocron.x.y/1"}]), RELAY_A, None),
        commit("did:plc:postaaaaaaaaaaaaaaaaaaaa", &post, "wss://zqrelayunlisted.zqmarkhost.invalid", None),
        commit("did:plc:like2aaaaaaaaaaaaaaaaaaa", &like("4"), RELAY_B, None),
        commit("did:plc:emptyaaaaaaaaaaaaaaaaaaa", &json!([]), RELAY_A, None),
        event("identity", &json!({"did": "did:plc:knownaaaaaaaaaaaaaaaaaaa", "_relay": RELAY_A})),
        event("identity", &json!({"did": "did:plc:unknownaaaaaaaaaaaaaaaaa", "_relay": RELAY_A})),
        event("account", &json!({"did": "did:plc:acctaaaaaaaaaaaaaaaaaaaa", "_relay": RELAY_B, "active": false})),
    ];
    for e in &entries {
        xadd(c, &k, "*", e).await;
    }
    let (lines, _) = collect_lines(&mut rx, 7, Duration::from_secs(10)).await;
    let dids: Vec<&str> = lines.iter().map(|l| l.3.as_str()).collect();
    assert_eq!(
        dids,
        vec![
            "did:plc:like0aaaaaaaaaaaaaaaaaaa",
            "did:plc:like1aaaaaaaaaaaaaaaaaaa",
            "did:plc:postaaaaaaaaaaaaaaaaaaaa",
            "did:plc:like2aaaaaaaaaaaaaaaaaaa",
            "did:plc:knownaaaaaaaaaaaaaaaaaaa",
            "did:plc:unknownaaaaaaaaaaaaaaaaa",
            "did:plc:acctaaaaaaaaaaaaaaaaaaaa",
        ],
        "exactly the eligible entries, in order"
    );
    for l in &lines {
        assert!(["c", "u", "d", "a", "i", ""].contains(&l.1.as_str()), "{l:?}");
    }
    assert_eq!((lines[0].2.as_str(), lines[0].1.as_str()), ("app.bsky.feed.like", "c"));
    assert_eq!(lines[2].2.as_str(), "app.bsky.feed.post");
    assert_eq!((lines[6].2.as_str(), lines[6].1.as_str()), ("#account", "i"));
    // The two identity lines differ only in the DID and the time.
    assert_eq!((&lines[4].1, &lines[4].2), (&lines[5].1, &lines[5].2));
    // No payload carries a relay URL, the text or an outcome.
    let (_, backlog, _, _) = r.feed.subscribe();
    let text = String::from_utf8_lossy(&backlog).to_string();
    assert_eq!(super::tests::relay_marker_in(&text), None, "{text}");
    assert!(!text.to_lowercase().contains("zqrelayunlisted"), "{text}");
    for marker in ["MARKER-TEXT", "not_in_scope", "reference", "who", "\"o\""] {
        assert!(!text.contains(marker), "{marker}: {text}");
    }
    // The arrivals by kind count the shown ops only: no hidden op has a kind.
    tokio::time::sleep(Duration::from_millis(2500)).await;
    let s = r.summary.load();
    let total: u64 = s.totals.iter().map(|(_, n)| n).sum();
    assert_eq!(total, 5, "{:?}", s.totals);
    assert!(!s.totals.iter().any(|(k, _)| k.contains("private")), "{:?}", s.totals);
    assert_eq!(s.events_total, 3);
    del(&mut r.conn, &k).await;
}

/// T-P5 (relay lines through a test stream) and T-P17's "no consumer at
/// all": 300 commits from relay A in one entry second with delays of
/// 2.00 to 2.04 s and one an hour old read "2.0 s behind"; a future time is
/// never negative; relay B, never seen, is "not heard from yet". Each is
/// named by its host.
#[tokio::test]
async fn t_p5_relay_delays_from_the_stream() {
    let Some(mut r) = rig().await else {
        return;
    };
    anchored(&r).await;
    let k = r.key.clone();
    // Entries with explicit ids in one second, about now.
    let second = (local_ms() / 1000 + 2) * 1000;
    let time = |ms: i64| chrono::DateTime::from_timestamp_millis(ms).expect("time").to_rfc3339_opts(chrono::SecondsFormat::Millis, true);
    let mut pipe = redis::pipe();
    for i in 0..300i64 {
        // Every entry's id is `second`; its delay 2.00 s to 2.04 s.
        let fields = commit("did:plc:aaaaaaaaaaaaaaaaaaaaaaaa", &like(&i.to_string()), RELAY_A, Some(time(second - 2000 - i % 41)));
        let c = pipe.cmd("XADD").arg(&k).arg(format!("{second}-{i}"));
        for (f, v) in &fields {
            c.arg(f).arg(v);
        }
    }
    let fields = commit("did:plc:aaaaaaaaaaaaaaaaaaaaaaaa", &like("old"), RELAY_A, Some(time(second - 3_600_000)));
    let c = pipe.cmd("XADD").arg(&k).arg(format!("{second}-300"));
    for (f, v) in &fields {
        c.arg(f).arg(v);
    }
    let _: () = pipe.query_async(&mut r.conn).await.expect("xadd");
    let fields = commit("did:plc:aaaaaaaaaaaaaaaaaaaaaaaa", &like("future"), RELAY_A, Some(time(second + 600_000)));
    xadd(&mut r.conn, &k, &format!("{}-0", second + 1000), &fields).await;
    // The first second is published at its end + 500 ms.
    let deadline = Instant::now() + Duration::from_secs(8);
    loop {
        let s = r.summary.load();
        if s.newest_published_sec.is_some_and(|n| n >= second / 1000) {
            let configured = status::relay_list(&super::tests::marker_relays());
            let built = status::build(&super::tests::observed(&s), &configured);
            assert_eq!(built.relays, "Receiving from 1 of 3 relays, 2.0 s behind the network", "{s:?}");
            break;
        }
        assert!(Instant::now() < deadline, "the second was published");
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    // The commit with a future time: 0 ms, never negative.
    let deadline = Instant::now() + Duration::from_secs(8);
    loop {
        let s = r.summary.load();
        if s.newest_published_sec.is_some_and(|n| n > second / 1000) {
            assert_eq!(s.relays["zqrelayalpha.zqmarkhost.invalid"].delay, Some(status::Delay::Ms(0)), "{s:?}");
            break;
        }
        assert!(Instant::now() < deadline);
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    del(&mut r.conn, &k).await;
}

/// T-P5, the input: entries at a steady rate; the summary's input is the
/// relays' true count a second within 1%, "unknown" before two readings.
#[tokio::test]
async fn t_p5_input_rate() {
    let Some(mut r) = rig().await else {
        return;
    };
    assert_eq!(r.summary.load().input_per_sec, None, "unknown before two readings");
    anchored(&r).await;
    let k = r.key.clone();
    let rate = 4500u64;
    let started = Instant::now();
    let mut added = 0u64;
    let mut tick = tokio::time::interval(Duration::from_millis(10));
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Burst);
    while started.elapsed() < Duration::from_secs(13) {
        tick.tick().await;
        let due = rate * u64::try_from(started.elapsed().as_millis()).expect("ms") / 1000;
        let mut pipe = redis::pipe();
        while added < due {
            let fields = commit("did:plc:aaaaaaaaaaaaaaaaaaaaaaaa", &json!([]), RELAY_A, None);
            let c = pipe.cmd("XADD").arg(&k).arg("*");
            for (f, v) in &fields {
                c.arg(f).arg(v);
            }
            c.ignore();
            added += 1;
        }
        let _: () = pipe.query_async(&mut r.conn).await.expect("xadd");
    }
    let got = r.summary.load().input_per_sec.expect("a rate");
    #[allow(clippy::cast_precision_loss)]
    let want = rate as f64;
    assert!((got - want).abs() / want < 0.01, "{got} vs {want}");
    del(&mut r.conn, &k).await;
}

/// T-P19a: entries added before the anchor never appear; every one after,
/// the first included, does; an entry added right after an empty poll is
/// read; ids `1-9` then `1-10` both give lines.
#[tokio::test]
async fn t_p19a_start_and_no_loss() {
    let Some((mut conn, _)) = redis().await else {
        return;
    };
    let key = format!("test:pp:{}", rand::random::<u32>());
    xadd(&mut conn, &key, "*", &commit("did:plc:beforeaaaaaaaaaaaaaaaaaa", &like("1"), RELAY_A, None)).await;
    let r = rig_with(conn, fast(), Some(key.clone())).await;
    let (mut rx, _, _, _) = r.feed.subscribe();
    let mut c = r.conn.clone();
    tokio::time::timeout(Duration::from_secs(5), r.probe.polled_empty.notified()).await.expect("empty poll");
    xadd(&mut c, &key, "*", &commit("did:plc:firstaaaaaaaaaaaaaaaaaaa", &like("2"), RELAY_A, None)).await;
    tokio::time::timeout(Duration::from_secs(5), r.probe.polled_empty.notified()).await.expect("empty poll");
    // Right after an empty poll returns.
    xadd(&mut c, &key, "*", &commit("did:plc:afteraaaaaaaaaaaaaaaaaaa", &like("3"), RELAY_A, None)).await;
    let (lines, _) = collect_lines(&mut rx, 2, Duration::from_secs(5)).await;
    let dids: Vec<&str> = lines.iter().map(|l| l.3.as_str()).collect();
    assert_eq!(dids, vec!["did:plc:firstaaaaaaaaaaaaaaaaaaa", "did:plc:afteraaaaaaaaaaaaaaaaaaa"]);
    del(&mut c, &key).await;

    // Each id once: `1-9` and `1-10` on a stream of their own.
    let key = format!("test:pp:{}", rand::random::<u32>());
    let r = rig_with(c.clone(), fast(), Some(key.clone())).await;
    let (mut rx, _, _, _) = r.feed.subscribe();
    tokio::time::timeout(Duration::from_secs(5), r.probe.polled_empty.notified()).await.expect("empty poll");
    let now = local_ms();
    xadd(&mut c, &key, &format!("{now}-9"), &commit("did:plc:nineaaaaaaaaaaaaaaaaaaaa", &like("1"), RELAY_A, None)).await;
    xadd(&mut c, &key, &format!("{now}-10"), &commit("did:plc:tenaaaaaaaaaaaaaaaaaaaaa", &like("2"), RELAY_A, None)).await;
    let (lines, _) = collect_lines(&mut rx, 3, Duration::from_secs(3)).await;
    let dids: Vec<&str> = lines.iter().map(|l| l.3.as_str()).collect();
    assert_eq!(dids, vec!["did:plc:nineaaaaaaaaaaaaaaaaaaaa", "did:plc:tenaaaaaaaaaaaaaaaaaaaaa"]);
    del(&mut c, &key).await;
}

/// Lines and `skipped` frames from `rx`, until `skips` skips and `after`
/// lines after the last of them, or `within`.
async fn until_skips(rx: &mut tokio::sync::broadcast::Receiver<Arc<super::live::Frame>>, within: Duration) -> Vec<(String, Value)> {
    let until = tokio::time::Instant::now() + within;
    let mut out = Vec::new();
    while let Ok(Ok(f)) = tokio::time::timeout_at(until, rx.recv()).await {
        let (name, v) = super::live::tests::data(&f.bytes);
        if name != "counts" {
            out.push((name, v));
        }
    }
    out
}

/// T-P19a: silence is not a skip; a paused reader past the lag bound skips
/// once, `from` the last line and `to` the new anchor, at 200 a second and at
/// 10 a second; then lines are live.
#[tokio::test]
async fn t_p19a_silence_and_falling_behind() {
    let Some(mut r) = rig().await else {
        return;
    };
    let (mut rx, _, _, _) = r.feed.subscribe();
    anchored(&r).await;
    let k = r.key.clone();
    // Silence for longer than the lag bound (3 s here): no `skipped`.
    tokio::time::sleep(Duration::from_secs(5)).await;
    xadd(&mut r.conn, &k, "*", &commit("did:plc:quietaaaaaaaaaaaaaaaaaaa", &like("1"), RELAY_A, None)).await;
    let frames = until_skips(&mut rx, Duration::from_secs(2)).await;
    assert!(frames.iter().all(|(n, _)| n != "skipped"), "{frames:?}");
    assert_eq!(frames.iter().filter(|(n, _)| n == "lines").flat_map(|(_, v)| lines_of(v)).count(), 1);
    for per_sec in [200u64, 10] {
        let mut last_line = None;
        // A few lines, then the pause.
        for i in 0..3 {
            let id = xadd(&mut r.conn, &k, "*", &commit("did:plc:beforepauseaaaaaaaaaaaaa", &like(&i.to_string()), RELAY_A, None)).await;
            last_line = Some(super::reader::parse_id(&id).expect("id").0);
        }
        tokio::time::sleep(Duration::from_millis(800)).await;
        r.probe.paused.store(true, Ordering::Relaxed);
        let started = Instant::now();
        while started.elapsed() < Duration::from_secs(4) {
            xadd(&mut r.conn, &k, "*", &commit("did:plc:duringpauseaaaaaaaaaaaaa", &like("x"), RELAY_A, None)).await;
            tokio::time::sleep(Duration::from_millis(1000 / per_sec)).await;
        }
        r.probe.paused.store(false, Ordering::Relaxed);
        tokio::time::sleep(Duration::from_millis(1500)).await;
        xadd(&mut r.conn, &k, "*", &commit("did:plc:liveagainaaaaaaaaaaaaaaa", &like("y"), RELAY_A, None)).await;
        let frames = until_skips(&mut rx, Duration::from_secs(3)).await;
        let skipped: Vec<&Value> = frames.iter().filter(|(n, _)| n == "skipped").map(|(_, v)| v).collect();
        assert_eq!(skipped.len(), 1, "{per_sec}/s: {frames:?}");
        assert_eq!(skipped[0]["from"].as_i64(), last_line, "{per_sec}/s");
        assert_eq!(skipped[0]["why"], "behind");
        let after: Vec<String> = frames
            .iter()
            .skip_while(|(n, _)| n != "skipped")
            .filter(|(n, _)| n == "lines")
            .flat_map(|(_, v)| lines_of(v))
            .map(|l| l.3)
            .collect();
        assert!(after.iter().all(|d| d != "did:plc:duringpauseaaaaaaaaaaaaa"), "nothing between is published");
        assert!(after.iter().any(|d| d == "did:plc:liveagainaaaaaaaaaaaaaaa"), "{per_sec}/s: {after:?}");
        let to = skipped[0]["to"].as_i64().expect("to");
        assert!(to >= last_line.expect("line"), "{to}");
    }
    del(&mut r.conn, &k).await;
}

/// T-P19a: a trim of unread entries under the lag bound skips once; a
/// deleted key is no skip; a recreated stream below the reader's id is a
/// `reset`.
#[tokio::test]
async fn t_p19a_trim_and_reset() {
    let Some((conn, _)) = redis().await else {
        return;
    };
    let rs = ReaderSettings { behind: Duration::from_secs(60), ..fast() };
    let mut r = rig_with(conn, rs, None).await;
    let (mut rx, _, _, _) = r.feed.subscribe();
    anchored(&r).await;
    let k = r.key.clone();
    xadd(&mut r.conn, &k, "*", &commit("did:plc:firstaaaaaaaaaaaaaaaaaaa", &like("1"), RELAY_A, None)).await;
    tokio::time::sleep(Duration::from_millis(500)).await;
    r.probe.paused.store(true, Ordering::Relaxed);
    for i in 0..400 {
        let fields = commit("did:plc:trimmedaaaaaaaaaaaaaaaaa", &like(&i.to_string()), RELAY_A, None);
        let mut cmd = redis::cmd("XADD");
        cmd.arg(&k).arg("MAXLEN").arg(300).arg("*");
        for (f, v) in &fields {
            cmd.arg(f).arg(v);
        }
        let _: String = cmd.query_async(&mut r.conn).await.expect("xadd");
    }
    r.probe.paused.store(false, Ordering::Relaxed);
    let frames = until_skips(&mut rx, Duration::from_secs(3)).await;
    let skipped: Vec<&Value> = frames.iter().filter(|(n, _)| n == "skipped").map(|(_, v)| v).collect();
    assert_eq!(skipped.len(), 1, "{frames:?}");
    assert_eq!(skipped[0]["why"], "behind");

    // The key deleted: no `skipped` while it is missing.
    del(&mut r.conn, &k).await;
    let frames = until_skips(&mut rx, Duration::from_secs(2)).await;
    assert!(frames.iter().all(|(n, _)| n != "skipped"), "{frames:?}");
    // Recreated with ids below the reader's: one `reset`, then new lines.
    xadd(&mut r.conn, &k, "1-1", &commit("did:plc:recreatedaaaaaaaaaaaaaaa", &like("1"), RELAY_A, None)).await;
    let frames = until_skips(&mut rx, Duration::from_secs(3)).await;
    let skipped: Vec<&Value> = frames.iter().filter(|(n, _)| n == "skipped").map(|(_, v)| v).collect();
    assert_eq!(skipped.len(), 1, "{frames:?}");
    assert_eq!(skipped[0]["why"], "reset");
    xadd(&mut r.conn, &k, "*", &commit("did:plc:newaaaaaaaaaaaaaaaaaaaaa", &like("2"), RELAY_A, None)).await;
    let frames = until_skips(&mut rx, Duration::from_secs(2)).await;
    assert!(frames.iter().filter(|(n, _)| n == "lines").flat_map(|(_, v)| lines_of(v)).any(|l| l.3 == "did:plc:newaaaaaaaaaaaaaaaaaaaaa"));
    del(&mut r.conn, &k).await;
}

/// A TCP proxy to Redis that the test can hold: while held it accepts and
/// forwards nothing.
async fn proxy(target: String, held: Arc<std::sync::atomic::AtomicBool>) -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().expect("addr");
    tokio::spawn(async move {
        loop {
            let Ok((client, _)) = listener.accept().await else {
                return;
            };
            let target = target.clone();
            let held = Arc::clone(&held);
            tokio::spawn(async move {
                let Ok(server) = tokio::net::TcpStream::connect(&target).await else {
                    return;
                };
                let (cr, cw) = client.into_split();
                let (sr, sw) = server.into_split();
                let pipe = |mut from: tokio::net::tcp::OwnedReadHalf, mut to: tokio::net::tcp::OwnedWriteHalf, held: Arc<std::sync::atomic::AtomicBool>| async move {
                    let mut buf = vec![0u8; 64 * 1024];
                    loop {
                        use tokio::io::{AsyncReadExt, AsyncWriteExt};
                        let Ok(n) = from.read(&mut buf).await else {
                            return;
                        };
                        if n == 0 {
                            return;
                        }
                        while held.load(Ordering::Relaxed) {
                            tokio::time::sleep(Duration::from_millis(20)).await;
                        }
                        if to.write_all(&buf[..n]).await.is_err() {
                            return;
                        }
                    }
                };
                let a = tokio::spawn(pipe(cr, sw, Arc::clone(&held)));
                let b = tokio::spawn(pipe(sr, cw, held));
                let _ = (a.await, b.await);
            });
        }
    });
    format!("redis://{addr}")
}

/// T-P19a: a Redis that stops answering for a few seconds: the reader times
/// out, its lag metric grows, and on recovery nothing is lost and nothing is
/// skipped; held past the lag bound while entries flow, one `skipped`.
#[tokio::test]
async fn t_p19a_hung_redis() {
    let Some((mut direct, url)) = redis().await else {
        return;
    };
    let authority = url.split("://").nth(1).expect("url").split('/').next().expect("authority").to_string();
    let held = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let via = proxy(authority, Arc::clone(&held)).await;
    let conn = ConnectionManager::new(redis::Client::open(via.as_str()).expect("url")).await.expect("proxy");
    let r = rig_with(conn, fast(), None).await;
    let (mut rx, _, _, _) = r.feed.subscribe();
    anchored(&r).await;
    let k = r.key.clone();
    held.store(true, Ordering::Relaxed);
    for i in 0..5 {
        xadd(&mut direct, &k, "*", &commit("did:plc:heldaaaaaaaaaaaaaaaaaaaa", &like(&i.to_string()), RELAY_A, None)).await;
    }
    tokio::time::sleep(Duration::from_millis(2500)).await;
    held.store(false, Ordering::Relaxed);
    let frames = until_skips(&mut rx, Duration::from_secs(4)).await;
    assert!(frames.iter().all(|(n, _)| n != "skipped"), "{frames:?}");
    assert_eq!(frames.iter().filter(|(n, _)| n == "lines").flat_map(|(_, v)| lines_of(v)).count(), 5, "nothing lost");
    // Held past the bound (3 s) while entries flow: one `skipped`.
    held.store(true, Ordering::Relaxed);
    let started = Instant::now();
    while started.elapsed() < Duration::from_secs(5) {
        xadd(&mut direct, &k, "*", &commit("did:plc:flowaaaaaaaaaaaaaaaaaaaa", &like("x"), RELAY_A, None)).await;
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    held.store(false, Ordering::Relaxed);
    let frames = until_skips(&mut rx, Duration::from_secs(6)).await;
    let skipped: Vec<&Value> = frames.iter().filter(|(n, _)| n == "skipped").map(|(_, v)| v).collect();
    assert_eq!(skipped.len(), 1, "{frames:?}");
    assert_eq!(skipped[0]["why"], "behind");
    del(&mut direct, &k).await;
}

/// T-P19a: undecodable entries give no line, count in the metric and the
/// input, and not in `events`.
#[tokio::test]
async fn t_p19a_undecodable() {
    let recorder = metrics_exporter_prometheus::PrometheusBuilder::new().build_recorder();
    let handle = recorder.handle();
    let _local = metrics::set_default_local_recorder(&recorder);
    let Some(mut r) = rig().await else {
        return;
    };
    let (mut rx, _, _, _) = r.feed.subscribe();
    anchored(&r).await;
    let k = r.key.clone();
    xadd(&mut r.conn, &k, "*", &[("data".to_string(), "{}".to_string())]).await;
    xadd(&mut r.conn, &k, "*", &event("labels", &json!({"did": "did:plc:a"}))).await;
    xadd(&mut r.conn, &k, "*", &[("type".to_string(), "commit".to_string()), ("data".to_string(), "{nope".to_string())]).await;
    xadd(&mut r.conn, &k, "*", &commit("did:plc:a", &json!([{"action": "create"}]), RELAY_A, None)).await;
    xadd(&mut r.conn, &k, "*", &commit("did:plc:marker1aaaaaaaaaaaaaaaaa", &like("1"), RELAY_A, None)).await;
    let (lines, _) = collect_lines(&mut rx, 1, Duration::from_secs(5)).await;
    assert_eq!(lines.len(), 1);
    tokio::time::sleep(Duration::from_millis(2200)).await;
    assert_eq!(crate::test_support::series(&handle.render(), super::reader::METRIC_UNDECODABLE, &[]), Some(4.0));
    assert_eq!(r.summary.load().events_total, 0);
    del(&mut r.conn, &k).await;
}

/// T-P17: with the consumer's group on the stream and stopped, the reader
/// over 10,000 entries leaves the group's position, pending and entries
/// read as they were, and adds no consumer.
#[tokio::test]
async fn t_p17_the_group_is_untouched() {
    let Some(mut r) = rig().await else {
        return;
    };
    let (mut rx, _, _, _) = r.feed.subscribe();
    anchored(&r).await;
    let k = r.key.clone();
    let group = "prism";
    let _: () = redis::cmd(concat!("XGR", "OUP")).arg("CREATE").arg(&k).arg(group).arg("$").arg("MKSTREAM").query_async(&mut r.conn).await.expect("group");
    let groups = |v: &redis::Value| format!("{v:?}");
    let before: redis::Value = redis::cmd("XINFO").arg("GROUPS").arg(&k).query_async(&mut r.conn).await.expect("groups");
    for chunk in 0..10 {
        let mut pipe = redis::pipe();
        for i in 0..1000 {
            let fields = commit("did:plc:aaaaaaaaaaaaaaaaaaaaaaaa", &like(&format!("{chunk}-{i}")), RELAY_A, None);
            let c = pipe.cmd("XADD").arg(&k).arg("*");
            for (f, v) in &fields {
                c.arg(f).arg(v);
            }
            c.ignore();
        }
        let _: () = pipe.query_async(&mut r.conn).await.expect("xadd");
    }
    let (lines, _) = collect_lines(&mut rx, 10_000, Duration::from_secs(20)).await;
    assert_eq!(lines.len(), 10_000, "every entry, without a consumer");
    let after: redis::Value = redis::cmd("XINFO").arg("GROUPS").arg(&k).query_async(&mut r.conn).await.expect("groups");
    // `lag` moves with the stream's own length; position, pending and
    // entries read do not.
    let fields = |v: &redis::Value| -> Vec<(String, String)> {
        let redis::Value::Array(list) = v else { panic!("{v:?}") };
        let redis::Value::Array(flat) = &list[0] else { panic!("{v:?}") };
        flat.chunks(2)
            .map(|c| (format!("{:?}", c[0]), format!("{:?}", c[1])))
            .filter(|(k, _)| ["last-delivered-id", "pending", "entries-read", "consumers"].iter().any(|n| k.contains(n)))
            .collect()
    };
    assert_eq!(fields(&before), fields(&after), "{} / {}", groups(&before), groups(&after));
    let consumers: redis::Value = redis::cmd("XINFO").arg("CONSUMERS").arg(&k).arg(group).query_async(&mut r.conn).await.expect("consumers");
    assert_eq!(consumers, redis::Value::Array(Vec::new()));
    del(&mut r.conn, &k).await;
}

/// T-P18's stress and T-P19's full rate: 15,300 entries a second for 10 s;
/// every entry is in exactly one `lines` event, none over 256 KB; the
/// totals equal the ops added and the published arrivals plus the late ones.
/// The reader is paused once for 2 s to make entries late.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn t_p19_full_rate_and_exact_totals() {
    let Some(mut r) = rig().await else {
        return;
    };
    anchored(&r).await;
    let k = r.key.clone();
    let rate = 15_300u64;
    let started = Instant::now();
    let mut added = 0u64;
    let mut tick = tokio::time::interval(Duration::from_millis(10));
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Burst);
    // Set to the number added once the producer stops.
    let target = Arc::new(std::sync::atomic::AtomicU64::new(u64::MAX));
    let reader = tokio::spawn({
        let mut rx = r.feed.subscribe().0;
        let target = Arc::clone(&target);
        async move {
            let mut seen = Vec::new();
            let mut arrivals = 0u64;
            let mut biggest = 0usize;
            let mut last_totals = 0u64;
            let deadline = tokio::time::Instant::now() + Duration::from_secs(40);
            loop {
                let want = target.load(Ordering::Acquire);
                if seen.len() as u64 >= want && last_totals >= want {
                    break;
                }
                let Ok(Ok(f)) = tokio::time::timeout_at(deadline, rx.recv()).await else {
                    break;
                };
                biggest = biggest.max(f.bytes.len());
                let (name, v) = super::live::tests::data(&f.bytes);
                match name.as_str() {
                    "lines" => seen.extend(lines_of(&v).into_iter().map(|l| l.3)),
                    "counts" => {
                        arrivals += v["arrivals"].as_object().expect("arrivals").values().map(|n| n.as_u64().expect("n")).sum::<u64>();
                        last_totals = v["totals"].as_object().expect("totals").values().map(|n| n.as_u64().expect("n")).sum();
                    }
                    _ => {}
                }
            }
            (seen, arrivals, biggest, last_totals)
        }
    });
    let mut paused_once = false;
    while started.elapsed() < Duration::from_secs(10) {
        tick.tick().await;
        if !paused_once && started.elapsed() > Duration::from_secs(4) {
            paused_once = true;
            r.probe.paused.store(true, Ordering::Relaxed);
            let probe = Arc::clone(&r.probe);
            tokio::spawn(async move {
                tokio::time::sleep(Duration::from_secs(2)).await;
                probe.paused.store(false, Ordering::Relaxed);
            });
        }
        let due = rate * u64::try_from(started.elapsed().as_millis()).expect("ms") / 1000;
        let mut pipe = redis::pipe();
        while added < due {
            let fields = commit(&format!("did:plc:{added:024}"), &like("1"), RELAY_A, None);
            let c = pipe.cmd("XADD").arg(&k).arg("*");
            for (f, v) in &fields {
                c.arg(f).arg(v);
            }
            c.ignore();
            added += 1;
        }
        let _: () = pipe.query_async(&mut r.conn).await.expect("xadd");
    }
    target.store(added, Ordering::Release);
    let (seen, arrivals, biggest, last_totals) = reader.await.expect("reader");
    assert_eq!(seen.len() as u64, added, "every entry once");
    let distinct: std::collections::BTreeSet<&String> = seen.iter().collect();
    assert_eq!(distinct.len() as u64, added);
    assert!(biggest <= 256 * 1024, "{biggest}");
    assert_eq!(last_totals, added, "totals exact");
    assert_eq!(arrivals + r.feed.late_ops(), added, "published arrivals plus late");
    assert!(r.feed.late_ops() > 0, "the pause made some late");
    del(&mut r.conn, &k).await;
}

/// The lean decode's microbenchmark (§6.2): under 5 µs a ~680 B commit on
/// the build host, in a release build; the full decode recorded beside it.
#[test]
fn lean_decode_microbenchmark() {
    let data = json!({
        "repo": "did:plc:aaaaaaaaaaaaaaaaaaaaaaaa", "rev": "3kaaaaaaaaa22", "since": "3kaaaaaaaaa21",
        "time": "2026-10-27T12:00:00.000Z", "_relay": RELAY_A,
        "ops": [{"action": "create", "path": "app.bsky.feed.post/3kaaaaaaaaa22", "cid": {"$link": "bafyreiaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"},
            "record": {"$type": "app.bsky.feed.post", "text": "x".repeat(380), "createdAt": "2026-10-27T12:00:00.000Z", "langs": ["en"]}}]
    })
    .to_string();
    let entry = crate::firehose::stream::StreamEntry {
        id: "1761573802000-0".to_string(),
        fields: Some(vec![("type".to_string(), b"commit".to_vec()), ("data".to_string(), data.clone().into_bytes())]),
    };
    assert!((600..900).contains(&data.len()), "{}", data.len());
    let labels = labels();
    let n = 20_000u32;
    let start = Instant::now();
    for _ in 0..n {
        std::hint::black_box(super::decode::decode(std::hint::black_box(&entry), &labels));
    }
    let lean = start.elapsed() / n;
    let start = Instant::now();
    for _ in 0..n {
        let _ = std::hint::black_box(crate::firehose::stream::decode(std::hint::black_box(&entry)));
    }
    let full = start.elapsed() / n;
    eprintln!("lean decode {lean:?} an entry; full decode {full:?}");
    if !cfg!(debug_assertions) {
        assert!(lean < Duration::from_micros(5), "lean {lean:?}, full {full:?}");
    }
}

/// T-P19a: 30 s at the highest rate of §6.5's table is under the shipped
/// `max_stream_len`. The file sits beside the crate; a build tree with the
/// crate alone skips this, unless `HPRS_REQUIRE_REPO_TEXT=1`.
#[test]
fn trim_arithmetic() {
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../docker/firehose/bsky.toml");
    let Ok(text) = std::fs::read_to_string(&path) else {
        assert!(std::env::var("HPRS_REQUIRE_REPO_TEXT").as_deref() != Ok("1"), "{} not found", path.display());
        eprintln!("{} not found; skipping", path.display());
        return;
    };
    let v: toml::Table = text.parse().expect("toml");
    let max = v["redis"]["max_stream_len"].as_integer().expect("max_stream_len");
    assert!(30 * 15_300 < max, "{max}");
}

/// T-P20 over HTTP, T-P22 and T-P23: `/live` with no admin UI, its headers
/// and refusals; with the live feed off, no `/live`, no feed section, no
/// script, and no `script-src`.
#[tokio::test]
async fn t_p20_t_p22_t_p23_live_route() {
    let Some(r) = rig().await else {
        return;
    };
    let pool = super::tests::lazy_pool();
    let (state, _drainer) = app_state(&pool);
    let p = PublicPageConfig::default();
    // The status built from relays whose labels are markers.
    let relays = status::relay_list(&super::tests::marker_relays());
    let built = status::build(&super::tests::observed(&r.summary.load()), &relays);
    let page = page_state(built, &p, RateLimits::new());
    let live = LiveState {
        feed: r.feed.clone(),
        viewers: Arc::new(Viewers::new(ViewerSettings { max: 3, ..ViewerSettings::with_max(3) })),
        page: page.clone(),
        hostname: super::tests::HOST.to_string(),
    };
    let base = serve_app(finalized(&state, Some(router(page.clone(), Some(live))))).await;
    let client = reqwest::Client::new();
    let id = || hex::encode(rand::random::<[u8; 16]>());
    let open = |headers: Vec<(&'static str, &'static str)>| {
        let mut req = client.get(format!("{base}/live?page={}", id()));
        for (k, v) in headers {
            req = req.header(k, v);
        }
        req.send()
    };
    let ok = open(vec![("sec-fetch-site", "same-origin"), ("sec-fetch-mode", "cors"), ("sec-fetch-dest", "empty")]).await.expect("open");
    assert_eq!(ok.status().as_u16(), 200);
    assert_eq!(ok.headers()["content-type"], "text/event-stream");
    assert_eq!(ok.headers()["cache-control"], super::viewers::STREAM_CACHE);
    assert_eq!(ok.headers()["x-accel-buffering"], "no");
    assert_eq!(ok.headers()["content-security-policy"], CSP);
    let mut ok = ok;
    let first = tokio::time::timeout(Duration::from_secs(2), ok.chunk()).await.expect("chunk").expect("body").expect("bytes");
    assert!(String::from_utf8_lossy(&first).starts_with("retry: 5000"), "{first:?}");
    for bad in [vec![("sec-fetch-site", "cross-site")], vec![("sec-fetch-site", "same-origin"), ("sec-fetch-mode", "cors"), ("sec-fetch-dest", "image")], vec![("origin", "https://evil.example")]] {
        assert_eq!(open(bad).await.expect("open").status().as_u16(), 403);
    }
    let plain = open(Vec::new()).await.expect("curl");
    assert_eq!(plain.status().as_u16(), 200, "a non-browser client");
    let bad_id = client.get(format!("{base}/live?page=XYZ")).send().await.expect("bad");
    assert_eq!(bad_id.status().as_u16(), 400);
    // Over the cap: still 200, `end`/`busy`. (The test client is one
    // address, so the per-address rule answers first.)
    let busy = open(Vec::new()).await.expect("busy").text().await.expect("body");
    assert_eq!(busy, "retry: 30000\n\nevent: end\ndata: {\"reason\":\"busy\"}\n\n");
    // No admin UI here: `/admin` and `/admin/live` are the plain 404.
    for path in ["/admin", "/admin/live"] {
        assert_eq!(client.get(format!("{base}{path}")).send().await.expect("admin").status().as_u16(), 404);
    }
    // `/` with the feed on: the section, the script, `CSP_LIVE`.
    let home = client.get(format!("{base}/")).send().await.expect("home");
    assert_eq!(home.headers()["content-security-policy"], CSP_LIVE);
    let body = home.text().await.expect("body");
    for needle in ["id=\"live-list\"", "id=\"live-arrivals\"", "id=\"live-received\"", "id=\"live-notes\"", &format!("<script src=\"{}\" defer></script>", *JS_HREF)] {
        assert!(body.contains(needle), "{needle}");
    }
    // No status block, and nothing of any relay named.
    assert!(!body.contains("<span class=\"l\">Relays</span>"), "{body}");
    assert_eq!(super::tests::relay_marker_in(&body), None, "{body}");
    let js = client.get(format!("{base}{}", *JS_HREF)).send().await.expect("js");
    assert_eq!(js.headers()["cache-control"], IMMUTABLE);
    assert_eq!(js.text().await.expect("js"), JS);

    // Off.
    let off = PublicPageConfig { live_feed: false, ..PublicPageConfig::default() };
    let page = page_state(snapshot(), &off, RateLimits::new());
    let base = serve_app(finalized(&state, Some(router(page, None)))).await;
    assert_eq!(client.get(format!("{base}/live?page={}", id())).send().await.expect("live").status().as_u16(), 404);
    let home = client.get(format!("{base}/")).send().await.expect("home");
    let csp = home.headers()["content-security-policy"].to_str().expect("csp").to_string();
    assert!(!csp.contains("script-src"), "{csp}");
    let body = home.text().await.expect("body");
    assert!(!body.contains("<script") && !body.contains("id=\"live\""), "{body}");
    drop(ok);
}

/// T-P19 over HTTP: a viewer that joins while events flow sees no `e`
/// twice; with nobody watching for a while, a returning viewer's hole is
/// exactly the lines published meanwhile.
#[tokio::test]
async fn t_p19_joins_and_holes() {
    let Some(mut r) = rig().await else {
        return;
    };
    anchored(&r).await;
    let k = r.key.clone();
    let producer = {
        let mut c = r.conn.clone();
        let k = k.clone();
        tokio::spawn(async move {
            let started = Instant::now();
            let mut n = 0u64;
            while started.elapsed() < Duration::from_secs(9) {
                xadd(&mut c, &k, "*", &commit("did:plc:aaaaaaaaaaaaaaaaaaaaaaaa", &like(&n.to_string()), RELAY_A, None)).await;
                n += 1;
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
    };
    tokio::time::sleep(Duration::from_secs(1)).await;
    // Join mid-flow: the backlog's `e`, then events above it only.
    let (mut rx, backlog, e0, c0) = r.feed.subscribe();
    let (_, b) = super::live::tests::data(&backlog);
    assert_eq!((b["e"].as_u64(), b["c"].as_u64()), (Some(e0), Some(c0)));
    let mut last_e = e0;
    let mut first_c = None;
    let mut c_left = c0;
    let until = tokio::time::Instant::now() + Duration::from_secs(2);
    while let Ok(Ok(f)) = tokio::time::timeout_at(until, rx.recv()).await {
        assert!(f.e > last_e, "no e twice");
        last_e = f.e;
        let (name, v) = super::live::tests::data(&f.bytes);
        if name == "lines" {
            let n = v["l"].as_array().expect("l").len() as u64;
            let c = v["c"].as_u64().expect("c");
            if first_c.is_none() {
                first_c = Some(c - n);
            }
            c_left = c;
        }
    }
    assert_eq!(first_c, Some(c0), "no line counted twice across the join");
    drop(rx);
    // Nobody watching for 3 s.
    tokio::time::sleep(Duration::from_secs(3)).await;
    let (mut rx, _, _, _) = r.feed.subscribe();
    let published_meanwhile;
    loop {
        let f = tokio::time::timeout(Duration::from_secs(3), rx.recv()).await.expect("frame").expect("frame");
        let (name, v) = super::live::tests::data(&f.bytes);
        if name == "lines" {
            let n = v["l"].as_array().expect("l").len() as u64;
            published_meanwhile = v["c"].as_u64().expect("c") - n - c_left;
            break;
        }
    }
    // The released lines' own ledger: exactly those between the last line
    // this viewer saw and the first it sees now.
    let released = r.feed.released();
    let ledger = released.iter().skip(usize::try_from(c_left).expect("c")).take(usize::try_from(published_meanwhile).expect("n")).count() as u64;
    assert!(published_meanwhile > 0, "lines were published while nobody watched");
    assert_eq!(ledger, published_meanwhile);
    producer.await.expect("producer");
    del(&mut r.conn, &k).await;
}

/// T-P24: the script, read as text (the tests run no browser).
#[test]
fn t_p24_the_script() {
    let js = JS;
    for needle in [
        "crypto.getRandomValues",
        "new EventSource('/live?page=' + page)",
        "textContent",
        "requestAnimationFrame(render)",
        "list.childNodes.length > KEEP",
        "pending = [];",
        "'visibilitychange'",
        "es.close()",
        "if (ev.e <= backlogE) { return; }",
        "readyState === EventSource.CLOSED",
        "page = newId();",
        "events not received while reconnecting",
        "events not received while Horizon-Prism restarted",
        "the live reader fell behind",
        "the event stream was reset",
        "events missed: your connection could not keep up with the full feed",
        "reconnect(30000)",
        "10 * 60 * 1000",
        "The live feed is busy right now. The rest of this page is current.",
        "The live feed stopped. Reload the page.",
    ] {
        assert!(js.contains(needle), "{needle}");
    }
    for absent in ["ev.r[", "r: ev", "innerHTML", ".style", "setAttribute('style'", "location.reload", "catching up", "not_shown", "'behind'", "ev.behind"] {
        assert!(!js.contains(absent), "{absent}");
    }
    // `backlog` empties pending; a first stream shows no hole row.
    let backlog = js.split("on('backlog'").nth(1).expect("backlog handler");
    assert!(backlog.split("});").next().expect("body").contains("pending = [];"));
    assert!(js.contains("if (lastC !== null)"), "no hole row without a stored c");
    // `es.close()` before every reconnect and stop.
    for f in ["function stop(", "function reconnect(", "function backOff(", "function open("] {
        let body = js.split(f).nth(1).expect(f);
        let first_line = body.lines().nth(1).expect("line");
        assert!(first_line.contains("es.close()"), "{f}: {first_line}");
    }
    // The page template: the list, the counts column, the notes, the
    // noscript line and the "did not start" notice; no totals line, no
    // status block, and nothing about private posts.
    let page = render_live();
    for needle in ["id=\"live-list\"", "id=\"live-arrivals\"", "id=\"live-received\"", "id=\"live-notes\"", "<noscript>", "id=\"live-off\""] {
        assert!(page.contains(needle), "{needle}");
    }
    let lower = page.to_lowercase();
    for gone in ["id=\"live-totals\"", "id=\"live-since\"", "id=\"status\"", "kryphocron", "private", "member", "communit"] {
        assert!(!lower.contains(gone), "{gone}");
    }
}

fn render_live() -> String {
    super::tests::render(&snapshot(), &PublicPageConfig::default())
}

/// T-P15 (the feed's part): `src/public_page/` names neither the consumer's
/// group commands nor Prism's own feed state.
#[test]
fn t_p15_the_reader_touches_no_group() {
    let forbidden = [
        concat!("XREAD", "GROUP"),
        concat!("XA", "CK"),
        concat!("XCL", "AIM"),
        concat!("XAUTO", "CLAIM"),
        concat!("XGR", "OUP"),
        concat!("XSET", "ID"),
        concat!("Feed", "Tap"),
        concat!("Water", "marks"),
    ];
    for (path, text) in super::tests::source_words() {
        for f in forbidden {
            assert!(!text.contains(f), "{path} names {f}");
        }
    }
}
