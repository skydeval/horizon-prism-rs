//! Health, Jobs, Problems, Blocks (Farsight) and Home's tiles over HTTP
//! (admin-ui design §6.1, §6.4, §6.6-§6.8, §9; T-A7 for their typed
//! actions, T-A13 "no data" when a series is absent): the checks from the
//! snapshot ring, the key figures, failed jobs and "try again", the
//! clean-up's check and typed run, the typed re-sync, the dead letters and
//! their actions, Farsight not set up, and Home's attention list.

use chrono::{TimeDelta, Utc};
use serde_json::{Value, json};

use super::*;
use crate::admin::health::{HealthInputs, MetricsText, Ring, Snapshot};

type Queued = (String, Value, Option<String>);

async fn queued(pool: &PgPool) -> Vec<Queued> {
    sqlx::query_as("SELECT command, args, requested_by FROM admin_commands ORDER BY id").fetch_all(pool).await.expect("commands")
}

/// A POST past the one-change-a-second limit (§5.6).
async fn post(b: &Browser, url: &str, form: &[(&str, &str)]) -> Page {
    tokio::time::sleep(Duration::from_millis(1100)).await;
    b.post(url, form).await
}

/// A page past the ten-pages-a-second limit.
async fn get(b: &Browser, url: &str) -> Page {
    tokio::time::sleep(Duration::from_millis(150)).await;
    b.get(url).await
}

/// Metrics that say what the test says.
struct Fixed(String);

impl MetricsText for Fixed {
    fn render(&self) -> String {
        self.0.clone()
    }
}

const METRICS: &str = "firehose_consumer_group_present 1\n\
    firehose_consumer_lag_entries 200000\n\
    firehose_worker_delay_seconds{relay=\"bsky\"} 4\n\
    firehose_entries_processed_total{path=\"live\"} 1000\n\
    db_budget_ratio 0.85\n\
    db_size_bytes 1000000000\n\
    redis_memory_ratio 0.1\n\
    private_read_content_total{form=\"content\"} 7\n";

/// Five minutes of the same readings, ending now.
fn ring() -> Arc<Ring> {
    let ring = Arc::new(Ring::default());
    for m in (0..=5).rev() {
        ring.push(Snapshot::from_text(METRICS, Utc::now() - TimeDelta::minutes(m)));
    }
    ring
}

fn last_command(q: &[Queued]) -> (String, Value, Option<String>) {
    q.last().cloned().expect("a command")
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn health_jobs_and_home() {
    let inputs = HealthInputs { metrics: Some(Arc::new(Fixed(METRICS.to_string()))), ring: ring(), ..HealthInputs::default() };
    let Some(h) = harness_full("admin_health_pages", quick(), true, inputs).await else {
        return;
    };
    let alice = complete_setup(&h, ALICE).await;
    let p = h.pool();
    let url = |path: &str| format!("{}{path}", h.base);

    // Health: amber from the ring, "no data" where a series is absent, and
    // the figures in words.
    let health = get(&alice, &url("/admin/health")).await;
    assert_eq!(health.status, 200, "{}", health.body);
    for text in [
        "Prism is 200,000 records behind the network.",
        "The database is at 85% of its budget.",
        "Prism is reading the network stream.",
        "bsky is up to date",
        "No data: none have been counted since Prism started.",
        "Records waiting",
        "200,000",
        "1.0 GB of",
        "Read in the new form",
        "url=/admin/health?auto=1",
    ] {
        assert!(health.body.contains(text), "{text}: {}", health.body);
    }
    // Its own reload is a poll, and still counts the settings banners.
    let auto = get(&alice, &url("/admin/health?auto=1")).await;
    assert_eq!(auto.status, 200);

    // Jobs: a failed scope job of an account Prism follows can be tried
    // again; a list sync cannot. A failed job of an account Prism no longer
    // follows is counted, not listed, and not on Health; unavailable jobs are
    // a column and a reasons table (actionable-warnings design §5).
    sqlx::query("INSERT INTO scope_dids (did, reasons) VALUES ($1, 4), ('did:plc:un1', 4)").bind(BOB).execute(p).await.expect("scope");
    for (did, depth, state, error) in [
        (BOB, "scope", "failed", "getRepoStatus: HTTP 502"),
        (CAROL, "list_sync", "failed", "repo not found"),
        ("did:plc:gone", "scope", "failed", "getRecord: HTTP 500"),
        ("did:plc:un1", "scope", "unavailable", "repo inactive (takendown)"),
        ("did:plc:un2", "scope", "unavailable", "resolve: DID tombstoned: did:plc:un2"),
        ("did:plc:un3", "resync", "unavailable", "resolve: DID tombstoned: did:plc:un3"),
    ] {
        sqlx::query("INSERT INTO backfill_jobs (did, depth, state, attempts, last_error) VALUES ($1, $2, $3, 3, $4)")
            .bind(did)
            .bind(depth)
            .bind(state)
            .bind(error)
            .execute(p)
            .await
            .expect("job");
    }
    let jobs = get(&alice, &url("/admin/jobs")).await;
    assert_eq!(jobs.status, 200, "{}", jobs.body);
    for text in [
        "Accounts around members",
        "Subscribed lists",
        "getRepoStatus: HTTP 502",
        "<th>Unavailable</th>",
        "Accounts Prism could not read",
        "<td>Deleted from the directory</td><td>2</td>",
        "<td>Taken down by its server</td><td>1</td>",
        "2 more failed jobs are for accounts Prism no longer follows.",
    ] {
        assert!(jobs.body.contains(text), "{text}: {}", jobs.body);
    }
    assert!(!jobs.body.contains("getRecord: HTTP 500"), "a job Prism would not run again is not listed: {}", jobs.body);
    assert_eq!(jobs.body.matches("action=\"/admin/jobs/retry\"").count(), 1, "only the scope job: {}", jobs.body);
    assert!(jobs.body.contains("Clean-up of the accounts Prism follows"), "{}", jobs.body);
    let csrf = csrf_of(&jobs);
    let retry = post(&alice, &url("/admin/jobs/retry"), &[("csrf", &csrf), ("did", BOB), ("depth", "scope")]).await;
    assert_eq!(retry.status, 303, "{}", retry.body);
    assert_eq!(last_command(&queued(p).await), ("backfill".to_string(), json!({"did": BOB, "depth": "scope"}), Some(ALICE.to_string())));
    let before = queued(p).await.len();
    let refused = post(&alice, &url("/admin/jobs/retry"), &[("csrf", &csrf), ("did", CAROL), ("depth", "list_sync")]).await;
    assert_eq!(refused.status, 400);
    assert_eq!(queued(p).await.len(), before);

    // The clean-up: a check changes nothing and needs no confirmation; a
    // real run is typed.
    let check = post(&alice, &url("/admin/jobs/scope-check"), &[("csrf", &csrf)]).await;
    assert_eq!(check.status, 303, "{}", check.body);
    assert_eq!(last_command(&queued(p).await).1, json!({"dry_run": true}));
    let confirm = post(&alice, &url("/admin/jobs/scope-rebuild"), &[("csrf", &csrf)]).await;
    assert!(confirm.body.contains("<code>REBUILD</code>"), "{}", confirm.body);
    let before = queued(p).await.len();
    let wrong = post(&alice, &url("/admin/jobs/scope-rebuild"), &[("csrf", &csrf), ("confirm", "yes"), ("typed", "nope")]).await;
    assert_eq!(wrong.status, 400);
    assert_eq!(queued(p).await.len(), before, "an unconfirmed clean-up queues nothing");
    let run = post(&alice, &url("/admin/jobs/scope-rebuild"), &[("csrf", &csrf), ("confirm", "yes"), ("typed", "rebuild")]).await;
    assert_eq!(run.status, 303, "{}", run.body);
    assert_eq!(last_command(&queued(p).await).1, json!({"dry_run": false}));

    // The re-sync is typed too.
    let confirm = post(&alice, &url("/admin/health/resync"), &[("csrf", &csrf)]).await;
    assert!(confirm.body.contains("<code>RESYNC</code>"), "{}", confirm.body);
    let resync = post(&alice, &url("/admin/health/resync"), &[("csrf", &csrf), ("confirm", "yes"), ("typed", "RESYNC")]).await;
    assert_eq!(resync.status, 303, "{}", resync.body);
    assert_eq!(last_command(&queued(p).await).0, "resync");

    // Home: what needs attention, worst first, and the tiles; the fragment
    // carries both.
    let home = get(&alice, &url("/admin")).await;
    assert!(
        home.body.contains("Needs attention") && home.body.contains("records behind the network") && home.body.contains("1 sync job failed after trying again several times."),
        "{}",
        home.body
    );
    // Health counts the same one.
    let health = get(&alice, &url("/admin/health")).await;
    assert!(health.body.contains("1 sync job failed after trying again several times."), "{}", health.body);
    assert!(home.body.contains("Network stream") && home.body.contains("up to date") && home.body.contains("Last changes") && home.body.contains("Re-sync private data and members"), "the last changes name the re-sync: {}", home.body);
    let tiles = get(&alice, &url("/admin/tiles")).await;
    assert!(tiles.body.contains("id=\"attention\"") && tiles.body.contains("id=\"tiles\"") && tiles.body.contains("Needs attention"), "{}", tiles.body);

    // Blocks: not set up here.
    let farsight = get(&alice, &url("/admin/farsight")).await;
    assert_eq!(farsight.status, 200);
    assert!(farsight.body.contains("Farsight is not set up"), "{}", farsight.body);

    // Problems without Redis: said plainly.
    let problems = get(&alice, &url("/admin/problems")).await;
    assert_eq!(problems.status, 200);
    assert!(problems.body.contains("could not read this list") && problems.body.contains("state is unknown"), "{}", problems.body);

    // A forged form changes nothing (T-A7).
    let before = queued(p).await.len();
    let forged = post(&alice, &url("/admin/jobs/scope-check"), &[("csrf", "forged")]).await;
    assert_eq!(forged.status, 403);
    assert_eq!(queued(p).await.len(), before);
    h.db.drop_db().await;
}

/// With no metrics at all, every check that reads them says "no data",
/// never fine, and the figures say "unknown".
#[tokio::test]
async fn no_metrics_means_no_data() {
    let Some(h) = harness("admin_health_no_metrics").await else {
        return;
    };
    let alice = complete_setup(&h, ALICE).await;
    let health = get(&alice, &format!("{}/admin/health", h.base)).await;
    assert_eq!(health.status, 200);
    assert!(health.body.contains("Prism started less than five minutes ago"), "{}", health.body);
    assert!(!health.body.contains("Prism is reading the network stream"), "{}", health.body);
    assert!(health.body.contains("No data: Prism needs two minutes of readings first."), "{}", health.body);
    assert!(health.body.contains("unknown"), "{}", health.body);
    let home = get(&alice, &format!("{}/admin", h.base)).await;
    assert!(home.body.contains("no data yet"), "{}", home.body);
    h.db.drop_db().await;
}

/// Problems lists a dead letter; "try again" and "dismiss" (confirmed)
/// queue their commands.
#[tokio::test]
async fn problems_lists_dead_letters() {
    let Ok(redis_url) = std::env::var("HPRS_TEST_REDIS_URL") else {
        eprintln!("HPRS_TEST_REDIS_URL unset; skipping the problems page test");
        return;
    };
    let mut redis = redis::aio::ConnectionManager::new(redis::Client::open(redis_url).expect("client")).await.expect("redis");
    let inputs = HealthInputs { redis: Some(redis.clone()), ..HealthInputs::default() };
    let Some(h) = harness_full("admin_problems_page", quick(), true, inputs).await else {
        return;
    };
    let alice = complete_setup(&h, ALICE).await;
    let stream = crate::config::FileConfig::default().firehose.dead_letter_stream;
    let path = format!("app.bsky.feed.post/{}", rand::random::<u32>());
    let id: String = redis::cmd("XADD")
        .arg(&stream)
        .arg("*")
        .arg("type")
        .arg("commit")
        .arg("repo")
        .arg(BOB)
        .arg("path")
        .arg(&path)
        .arg("error")
        .arg("record did not parse")
        .query_async(&mut redis)
        .await
        .expect("xadd");
    let page = get(&alice, &format!("{}/admin/problems", h.base)).await;
    assert_eq!(page.status, 200);
    assert!(page.body.contains(&path) && page.body.contains("record did not parse") && page.body.contains(&id), "{}", page.body);
    let csrf = csrf_of(&page);
    let replay = post(&alice, &format!("{}/admin/problems/replay", h.base), &[("csrf", &csrf), ("id", &id)]).await;
    assert_eq!(replay.status, 303, "{}", replay.body);
    assert_eq!(last_command(&queued(h.pool()).await).1, json!({"id": id}));
    let confirm = post(&alice, &format!("{}/admin/problems/dismiss", h.base), &[("csrf", &csrf), ("id", &id)]).await;
    assert!(confirm.body.contains("Dismiss a record"), "{}", confirm.body);
    let dismiss = post(&alice, &format!("{}/admin/problems/dismiss", h.base), &[("csrf", &csrf), ("id", &id), ("confirm", "yes")]).await;
    assert_eq!(dismiss.status, 303, "{}", dismiss.body);
    assert_eq!(last_command(&queued(h.pool()).await), ("deadletter drop".to_string(), json!({"id": id}), Some(ALICE.to_string())));
    let bad = post(&alice, &format!("{}/admin/problems/replay", h.base), &[("csrf", &csrf), ("id", "nope")]).await;
    assert_eq!(bad.status, 400);
    let _: i64 = redis::cmd("XDEL").arg(&stream).arg(&id).query_async(&mut redis).await.expect("xdel");
    h.db.drop_db().await;
}
