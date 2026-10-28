//! Public page T-P17: the public live feed's delivery does not move with
//! the consumer's work. The same entries, with explicit ids, are added in
//! real time beside a running consumer, once with the member's commits
//! slowed in the writer (`WRITER_DELAY_REPO`, 2 s each, so the consumer ends
//! far behind) and once without; the published lines, their order, the tick
//! each is released at relative to its entry time, and every second's
//! arrivals are identical, and nothing is late. Then a burst of blocks
//! naming a member (which the consumer stores) and naming an outsider
//! (which opens no transaction): every line at its entry-time tick.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use serde_json::{Value, json};

use super::live::{Fixture, MEMBER, OUTSIDER, Options, drive, recorder};
use crate::public_page::decode::Labels;
use crate::public_page::live::{FeedSettings, PublicFeed};
use crate::public_page::reader::{Probe, ReaderSettings};
use crate::test_support::hooks;

/// What one run published: `(did, entry ms − base, release tick − entry
/// ms)` per line, and `(second − base second, arrivals)` per counts event.
type Run = (Vec<(String, i64, i64)>, Vec<(i64, Value)>);

fn fields(repo: &str, ops: &Value) -> Vec<(String, String)> {
    let data = json!({"repo": repo, "rev": "3kaaaaaaaaa22", "ops": ops, "time": chrono::Utc::now().to_rfc3339()});
    vec![("type".to_string(), "commit".to_string()), ("data".to_string(), data.to_string()), ("seq".to_string(), "1".to_string())]
}

/// Add `entries` at `base + d` each, in real time, each no more than
/// 100 ms before its millisecond; collect what the feed publishes.
async fn run(f: &mut Fixture, entries: &[(i64, Vec<(String, String)>)], delay_member: bool) -> (Run, usize) {
    let (consumer, _) = f.consumer().await;
    let stop = Arc::new(AtomicBool::new(false));
    let driven = drive(consumer, Arc::clone(&stop), Duration::from_secs(60));
    let armed = delay_member.then(|| hooks::arm_delay(hooks::WRITER_DELAY_REPO, &format!("{}/{MEMBER}", f.key()), Duration::from_secs(2), None));
    let summary = Arc::default();
    let (feed, tasks) = PublicFeed::start(
        f.redis.clone(),
        &f.cfg.stream_key,
        Labels::new(&crate::public_page::relay_hosts(&f.cfg.relay_labels)),
        summary,
        ReaderSettings::default(),
        FeedSettings::default(),
        Arc::new(Probe::default()),
    );
    let (mut rx, _, _, _) = feed.subscribe();
    tokio::time::sleep(Duration::from_millis(500)).await;
    let now = crate::thread_outline::redis_time_ms(&mut f.redis).await.expect("time");
    let base = (now / 1000 + 2) * 1000;
    let offset = now - crate::public_page::reader::local_ms();
    for (d, fl) in entries {
        let at = base + d;
        loop {
            let redis_now = crate::public_page::reader::local_ms() + offset;
            if redis_now >= at - 100 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(u64::try_from(at - 100 - redis_now).unwrap_or(1).min(50))).await;
        }
        let mut cmd = redis::cmd("XADD");
        cmd.arg(&f.cfg.stream_key).arg(format!("{at}-0"));
        for (k, v) in fl {
            cmd.arg(k).arg(v);
        }
        let _: String = cmd.query_async(&mut f.redis).await.expect("xadd");
    }
    let last = entries.last().map_or(0, |(d, _)| *d);
    let mut counts = Vec::new();
    // Every entry is in by now: 3 s more for the last releases.
    let until = tokio::time::Instant::now() + Duration::from_secs(3);
    while let Ok(Ok(frame)) = tokio::time::timeout_at(until, rx.recv()).await {
        let text = String::from_utf8_lossy(&frame.bytes).to_string();
        if let Some(data) = text.strip_prefix("event: counts\ndata: ") {
            let v: Value = serde_json::from_str(data.trim_end()).expect("json");
            let t = v["t"].as_i64().expect("t");
            if (base / 1000..=(base + last) / 1000).contains(&t) {
                counts.push((t - base / 1000, v["arrivals"].clone()));
            }
        }
    }
    let lines = feed.released().into_iter().map(|(ms, tick, did)| (did, ms - base, tick - ms)).collect();
    // How many of the member's commits the writer had slowed by now.
    let slowed = armed.as_ref().map_or(0, hooks::Armed::hits);
    stop.store(true, Ordering::Relaxed);
    drop(armed);
    for t in tasks {
        t.abort();
    }
    let _ = driven.await;
    ((lines, counts), slowed)
}

fn planned(ms_after_base: i64, tick_after_ms: i64) -> bool {
    // Every base is a whole second, so the tick rule is the same relative
    // to the base: the first 100 ms tick at or after `entry + 500`.
    let entry = ms_after_base;
    let tick = ((entry + 500 + 99).div_euclid(100)) * 100;
    tick - entry == tick_after_ms
}

#[tokio::test]
async fn t_p17_delivery_does_not_move_with_the_consumer() {
    let (rec, handle) = recorder();
    let _local = metrics::set_default_local_recorder(&rec);
    let Some(mut f) = Fixture::new("pp_t_p17", Options::default()).await else {
        return;
    };
    // 300 entries over 6 s, one in ten from the tracked member.
    let entries: Vec<(i64, Vec<(String, String)>)> = (0..300)
        .map(|i| {
            let repo = if i % 10 == 0 { MEMBER } else { OUTSIDER };
            (i * 20 + 7, fields(repo, &json!([{"action": "create", "path": format!("app.bsky.feed.like/3kaaaaaaa{i:04}"), "record": {"subject": {"uri": "at://did:plc:x/app.bsky.feed.post/1", "cid": "bafy"}, "createdAt": "2026-10-27T00:00:00Z"}}])))
        })
        .collect();
    let (slowed, hits) = run(&mut f, &entries, true).await;
    // The consumer was really held: at 2 s a commit it got through only a
    // few of the member's 30 by the time the feed had published them all.
    assert!((1..30).contains(&hits), "{hits} slowed commits");
    let (plain, _) = run(&mut f, &entries, false).await;
    assert_eq!(slowed.0.len(), 300, "every entry a line");
    assert_eq!(slowed.0, plain.0, "lines, order and release ticks");
    assert_eq!(slowed.1, plain.1, "every second's arrivals");
    assert!(slowed.0.iter().all(|(_, ms, tick)| planned(*ms, *tick)), "every line at its entry-time tick");
    // Each published second's arrivals are the ops of its entries.
    let total: u64 = slowed.1.iter().flat_map(|(_, v)| v.as_object().expect("arrivals").values().map(|n| n.as_u64().expect("n"))).sum();
    assert_eq!(total, 300);

    // Rule M: blocks naming the member, then an outsider, interleaved.
    let blocks: Vec<(i64, Vec<(String, String)>)> = (0..200)
        .map(|i| {
            let subject = if i % 2 == 0 { MEMBER } else { "did:plc:fhuntrackedaaaaaaaaaaa" };
            (i * 20 + 3, fields(OUTSIDER, &json!([{"action": "create", "path": format!("app.bsky.graph.block/3kbbbbbbb{i:04}"), "record": {"subject": subject, "createdAt": "2026-10-27T00:00:00Z"}}])))
        })
        .collect();
    let (burst, _) = run(&mut f, &blocks, false).await;
    assert_eq!(burst.0.len(), 200);
    assert!(burst.0.iter().all(|(_, ms, tick)| planned(*ms, *tick)), "{:?}", burst.0.iter().find(|(_, ms, tick)| !planned(*ms, *tick)));
    let late = crate::test_support::series(&handle.render(), crate::public_page::live::METRIC_LATE, &[]).unwrap_or(0.0);
    assert!(late == 0.0, "{late} late lines");
    f.cleanup().await;
}
