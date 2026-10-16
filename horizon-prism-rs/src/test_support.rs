//! Shared helpers for Postgres-backed tests. They run when
//! `HPRS_TEST_DATABASE_URL` names a server the tests may create databases on;
//! otherwise [`fresh_db`] returns `None` and the test passes without doing
//! anything.

use sqlx::PgPool;

pub const ENV: &str = "HPRS_TEST_DATABASE_URL";

/// A migrated, uniquely named database, dropped by [`TestDb::drop_db`].
pub struct TestDb {
    admin: PgPool,
    pub pool: PgPool,
    name: String,
}

impl TestDb {
    /// The database's name: with a DID, the key of a [`hooks::pause_in`]
    /// site (`<name>/<did>`).
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }

    pub async fn drop_db(self) {
        self.pool.close().await;
        let _ = sqlx::query(&format!("DROP DATABASE IF EXISTS {} WITH (FORCE)", self.name))
            .execute(&self.admin)
            .await;
    }
}

/// Create and migrate a fresh database, or `None` when the env var is unset.
pub async fn fresh_db(test: &str) -> Option<TestDb> {
    let db = empty_db(test).await?;
    crate::db::migrations::MIGRATOR
        .run(&db.pool)
        .await
        .expect("apply migrations");
    Some(db)
}

/// Serve `router` on an ephemeral loopback port (fake upstreams), returning
/// its `http://127.0.0.1:<port>` base URL. Use with
/// `NetClient::loopback_for_tests`.
pub async fn serve_fake(router: axum::Router) -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().expect("addr");
    tokio::spawn(async move {
        axum::serve(listener, router).await.expect("fake upstream");
    });
    format!("http://{addr}")
}

/// In a Prometheus rendering, the value of the series `name` whose labels
/// include every one of `labels` (each `key="value"`), if there is one.
/// With no `labels`, an unlabelled series matches too.
/// Read metrics with a recorder set by `metrics::set_default_local_recorder`
/// in a `#[tokio::test]`, whose runtime runs on the test's thread.
#[must_use]
pub fn series(text: &str, name: &str, labels: &[&str]) -> Option<f64> {
    text.lines()
        .filter(|l| {
            l.strip_prefix(name)
                .is_some_and(|rest| rest.starts_with('{') || (labels.is_empty() && rest.starts_with(' ')))
        })
        .find(|l| labels.iter().all(|label| l.contains(label)))
        .and_then(|l| l.rsplit(' ').next())
        .and_then(|v| v.parse().ok())
}

/// The backend left idle inside a transaction in this test's database
/// whose last statement began with `last_statement`: a transaction a pause
/// point holds open. `None` when there is none.
pub async fn idle_in_transaction_pid(pool: &PgPool, last_statement: &str) -> Option<i32> {
    sqlx::query_scalar(
        "SELECT pid FROM pg_stat_activity \
         WHERE datname = current_database() AND state = 'idle in transaction' AND pid <> pg_backend_pid() \
           AND starts_with(query, $1) ORDER BY pid LIMIT 1",
    )
    .bind(last_statement)
    .fetch_optional(pool)
    .await
    .expect("pg_stat_activity")
}

/// Whether a backend in this test's database is waiting on a lock in a
/// statement that began with `statement`.
pub async fn waits_on_lock(pool: &PgPool, statement: &str) -> bool {
    sqlx::query_scalar(
        "SELECT EXISTS (SELECT 1 FROM pg_stat_activity \
         WHERE datname = current_database() AND wait_event_type = 'Lock' AND pid <> pg_backend_pid() \
           AND starts_with(query, $1))",
    )
    .bind(statement)
    .fetch_one(pool)
    .await
    .expect("pg_stat_activity")
}

/// The relations on which backend `pid` holds a lock of any mode. A row
/// lock shows here too: taking one also takes a lock on its relation.
pub async fn locked_relations(pool: &PgPool, pid: i32) -> Vec<String> {
    sqlx::query_scalar(
        "SELECT DISTINCT c.relname::text FROM pg_locks l JOIN pg_class c ON c.oid = l.relation \
         WHERE l.pid = $1 AND l.locktype IN ('relation', 'tuple') ORDER BY 1",
    )
    .bind(pid)
    .fetch_all(pool)
    .await
    .expect("pg_locks")
}

/// Whether backend `pid` holds the advisory lock keyed
/// `hashtextextended(key, 0)`, as `pg_advisory_xact_lock` takes it.
pub async fn holds_advisory(pool: &PgPool, pid: i32, key: &str) -> bool {
    sqlx::query_scalar(
        "SELECT EXISTS (SELECT 1 FROM pg_locks l, (SELECT hashtextextended($2, 0) AS k) h \
         WHERE l.pid = $1 AND l.locktype = 'advisory' AND l.granted AND l.objsubid = 1 \
           AND l.classid::bigint = ((h.k >> 32) & 4294967295) AND l.objid::bigint = (h.k & 4294967295))",
    )
    .bind(pid)
    .bind(key)
    .fetch_one(pool)
    .await
    .expect("pg_locks")
}

/// Pause points and injected errors for forced interleavings (Phase B
/// B-65, B-74). Production code calls [`hooks::pause`] or
/// [`hooks::injected`] at a named site, behind `#[cfg(test)]`, so a release
/// build has no trace of them. A site does something only while a test has
/// armed its name for the key the site passes (an owner, a list, a member):
/// tests run in parallel, and a key only one test uses keeps a hook from
/// touching any other. Unarmed, a site is one atomic load.
pub mod hooks {
    use std::collections::HashMap;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex, OnceLock, PoisonError};
    use std::time::Duration;

    use tokio::sync::{Semaphore, watch};

    /// B-65 forced interleavings (R3-138, R3-144), B-74: a `list_sync`
    /// reconcile batch, holding its list's `subscribed_lists` row
    /// `FOR UPDATE` and its URIs' locks, before it writes. Key: the list.
    pub const BATCH_HOLDS_LIST_ROW: &str = "b65_r3_138_batch_holds_list_row";
    /// B-65 `list_sync` rerun and the concurrency cap (R3-137, R3-145): a
    /// run after its snapshot (`S0`, `E0`), before it asks the owner's
    /// server anything. Key: the owner.
    pub const RUN_AFTER_SNAPSHOT: &str = "b65_r3_137_run_after_snapshot";
    /// B-65 stuck row (R3-169): `subscriptions::jobs::settle` fails before
    /// it touches the job row. Key: the owner.
    pub const SETTLE_FAILS: &str = "b65_r3_169_settle_fails";
    /// B-74 item 12: a `muteActorList` or `unmuteActorList` member phase,
    /// its writes done, before its commit. Key: the member.
    pub const MEMBER_PHASE_BEFORE_COMMIT: &str = "b74_member_phase_before_commit";
    /// B-74 LM-10: a release between two committed item batches. Key: the
    /// list.
    pub const RELEASE_BETWEEN_BATCHES: &str = "b74_lm_10_release_between_batches";
    /// B-65 owner-row race (R3-159): the daily sweep's owner-row delete,
    /// after its `FOR UPDATE` of the owner's row and before its delete.
    /// Key: the owner.
    pub const SWEEP_OWNER_LOCKED: &str = "b65_r3_159_sweep_owner_locked";
    /// B-65 owner-row race (R3-159), the reverse order: a list-phase
    /// transaction, its writes done (the owner's row taken), before its
    /// commit. Key: the list.
    pub const LIST_PHASE_BEFORE_COMMIT: &str = "b65_r3_159_list_phase_before_commit";
    /// #715: the recovery sweep, its owed references read, before it adds
    /// a pair to the owner's waiting set. Key: the owner.
    pub const RECOVERY_SWEEP_BEFORE_ADD: &str = "i715_recovery_sweep_before_add";
    /// #715: a purge, its batches done, before its end. Key: the DID.
    pub const PURGE_BEFORE_END: &str = "i715_purge_before_end";
    /// S10, #722: a job's or catch-up's settle, before its transaction and
    /// the gap lock. Key: `<database>/<did>` ([`pause_in`]).
    pub const SETTLE_BEFORE_GAP_LOCK: &str = "s10_settle_before_gap_lock";
    /// S10, #722: a job's or catch-up's settle holding the gap lock shared,
    /// its epoch read, before anything else. Key: `<database>/<did>`.
    pub const SETTLE_HOLDS_GAP_LOCK: &str = "s10_settle_holds_gap_lock";
    /// S10: a job's settle, its own row locked and its follow-up decided
    /// (the re-check read and queued), before it writes readiness or the
    /// flag. Key: `<database>/<did>`.
    pub const SETTLE_AFTER_FOLLOW_UP: &str = "s10_settle_after_follow_up";
    /// S10: a follow write or delete fails deterministically, on every
    /// attempt. Key: the follow's URI (tests use rkeys of their own).
    pub const FOLLOW_WRITE_FAILS: &str = "s10_follow_write_fails";
    /// S10: the head prober's divergence path, its `sync_state` row written,
    /// before it clears readiness. Key: `<database>/<did>`.
    pub const PROBER_HOLDS_SYNC_STATE: &str = "s10_prober_holds_sync_state";
    /// S10: the follows pass, its listing done, before it writes. Key:
    /// `<database>/<did>`.
    pub const PASS_AFTER_LISTING: &str = "s10_pass_after_listing";
    /// S10: the follows pass, a reconcile candidate confirmed with
    /// `getRecord`, before it re-reads the stored row. Key:
    /// `<database>/<did>`.
    pub const PASS_BEFORE_REREAD: &str = "s10_pass_before_reread";
    /// Firehose throughput F1: `Writer::apply` opens its transaction even
    /// for a commit with nothing to write. Key: the repo, or `*` for every
    /// repo (the solo bench only).
    pub const WRITER_FORCE_TX: &str = "fh_writer_force_tx";
    /// Firehose throughput §2.4: `Writer::apply`, before anything else,
    /// sleeps for the armed delay and fails transiently ([`delay`]). Key:
    /// the database.
    pub const WRITER_HANG: &str = "fh_writer_hang";
    /// Firehose throughput §2.4: the consumer sleeps for the armed delay
    /// before it processes an entry ([`delay`]). Key: the database.
    pub const CONSUMER_STALL: &str = "fh_consumer_stall";
    /// Firehose throughput §2.4: a trim gap's transaction committed and its
    /// anchor decided, before `reanchor`. Key: the database.
    pub const BEFORE_REANCHOR: &str = "fh_before_reanchor";
    /// Firehose throughput F6: the gap transaction fails before it begins.
    /// Key: the database.
    pub const GAP_TX_FAILS: &str = "fh_gap_tx_fails";
    /// Firehose throughput F6: the recreation gap marked owed, before the
    /// group is created. Key: the database.
    pub const AFTER_RECREATION_FLAG: &str = "fh_after_recreation_flag";
    /// Firehose throughput F6: the group created, before the recreation
    /// gap. Key: the database.
    pub const AFTER_RECREATION_CREATE: &str = "fh_after_recreation_create";
    /// Firehose throughput F6: the group-creation script fails. Key: the
    /// database.
    pub const GROUP_CREATE_FAILS: &str = "fh_group_create_fails";

    /// How long [`Armed::reached`] waits before the test fails.
    const REACH_WITHIN: Duration = Duration::from_secs(20);

    static ARMED: AtomicUsize = AtomicUsize::new(0);

    struct Point {
        hits: watch::Sender<usize>,
        /// Closed on release: every wait at the site then passes.
        gate: Semaphore,
        /// A [`delay`] site's sleep.
        delay: Option<Duration>,
        /// The site fires this many times at most.
        limit: Option<usize>,
    }

    impl Point {
        /// Count a hit unless the limit is reached; whether it was counted.
        fn hit(&self) -> bool {
            let mut fired = false;
            self.hits.send_if_modified(|n| {
                fired = self.limit.is_none_or(|limit| *n < limit);
                if fired {
                    *n += 1;
                }
                fired
            });
            fired
        }
    }

    type Points = HashMap<&'static str, HashMap<String, Arc<Point>>>;

    fn points() -> &'static Mutex<Points> {
        static POINTS: OnceLock<Mutex<Points>> = OnceLock::new();
        POINTS.get_or_init(Mutex::default)
    }

    fn lookup(name: &'static str, key: &str) -> Option<Arc<Point>> {
        if ARMED.load(Ordering::Acquire) == 0 {
            return None;
        }
        let points = points().lock().unwrap_or_else(PoisonError::into_inner);
        points.get(name).and_then(|keys| keys.get(key)).cloned()
    }

    /// A pause site keyed `<database>/<did>`: S10's sites name DIDs every
    /// fixture shares, so each test's database tells them apart. Unarmed,
    /// it reads nothing.
    pub async fn pause_in(name: &'static str, pool: &sqlx::PgPool, did: &str) {
        if ARMED.load(Ordering::Acquire) == 0 {
            return;
        }
        let db: String = sqlx::query_scalar("SELECT current_database()::text").fetch_one(pool).await.unwrap_or_default();
        pause(name, &format!("{db}/{did}")).await;
    }

    /// A pause site: when `name` is armed for `key`, count the hit and wait
    /// for the test's release.
    pub async fn pause(name: &'static str, key: &str) {
        let Some(point) = lookup(name, key) else {
            return;
        };
        if point.hit() {
            let _ = point.gate.acquire().await;
        }
    }

    /// An error site: whether `name` is armed for `key`, so the site fails
    /// as if its statement had. Counts the hit. Armed with [`arm_times`],
    /// it fires that many times and then no more.
    #[must_use]
    pub fn injected(name: &'static str, key: &str) -> bool {
        lookup(name, key).is_some_and(|point| point.hit())
    }

    /// A delay site: when `name` is armed for `key` with [`arm_delay`],
    /// count the hit, then sleep for the armed delay with
    /// `tokio::time::sleep`, so other tasks on the test's thread run. The
    /// hit is counted when the sleep begins, so [`Armed::reached`] tells the
    /// test it has. Returns whether it slept.
    pub async fn delay(name: &'static str, key: &str) -> bool {
        let Some(point) = lookup(name, key) else {
            return false;
        };
        let Some(d) = point.delay else {
            return false;
        };
        if !point.hit() {
            return false;
        }
        tokio::time::sleep(d).await;
        true
    }

    /// An armed hook. Dropping it, a failed test's unwinding included,
    /// releases any wait at the site and disarms it.
    pub struct Armed {
        name: &'static str,
        key: String,
        point: Arc<Point>,
    }

    /// Arm `name` for `key`. One arming per name and key at a time.
    #[must_use]
    pub fn arm(name: &'static str, key: &str) -> Armed {
        arm_point(name, key, None, None)
    }

    /// Arm `name` for `key` to fire `times` times and then no more.
    #[must_use]
    pub fn arm_times(name: &'static str, key: &str, times: usize) -> Armed {
        arm_point(name, key, None, Some(times))
    }

    /// Arm the [`delay`] site `name` for `key` with a sleep of `d`, `times`
    /// times at most (`None`: while armed).
    #[must_use]
    pub fn arm_delay(name: &'static str, key: &str, d: Duration, times: Option<usize>) -> Armed {
        arm_point(name, key, Some(d), times)
    }

    fn arm_point(name: &'static str, key: &str, delay: Option<Duration>, limit: Option<usize>) -> Armed {
        let point = Arc::new(Point { hits: watch::Sender::new(0), gate: Semaphore::new(0), delay, limit });
        let mut points = points().lock().unwrap_or_else(PoisonError::into_inner);
        let fresh = points.entry(name).or_default().insert(key.to_string(), Arc::clone(&point)).is_none();
        assert!(fresh, "{name} is already armed for {key}");
        ARMED.fetch_add(1, Ordering::AcqRel);
        Armed { name, key: key.to_string(), point }
    }

    impl Armed {
        /// Wait until the site has been reached `n` times in all.
        pub async fn reached_times(&self, n: usize) {
            let mut hits = self.point.hits.subscribe();
            tokio::time::timeout(REACH_WITHIN, hits.wait_for(|hits| *hits >= n))
                .await
                .unwrap_or_else(|_| panic!("{} was not reached {n} times for {}", self.name, self.key))
                .expect("hook");
        }

        /// Wait until the site has been reached.
        pub async fn reached(&self) {
            self.reached_times(1).await;
        }

        /// How often the site has been reached.
        #[must_use]
        pub fn hits(&self) -> usize {
            *self.point.hits.borrow()
        }

        /// Let every wait at the site, now and later, go on.
        pub fn release(&self) {
            self.point.gate.close();
        }
    }

    impl Drop for Armed {
        fn drop(&mut self) {
            self.release();
            let mut points = points().lock().unwrap_or_else(PoisonError::into_inner);
            if let Some(keys) = points.get_mut(self.name) {
                keys.remove(&self.key);
            }
            ARMED.fetch_sub(1, Ordering::AcqRel);
        }
    }
}

/// A fresh database with no migrations applied (migration tests).
pub async fn empty_db(test: &str) -> Option<TestDb> {
    let Ok(admin_url) = std::env::var(ENV) else {
        eprintln!("{ENV} unset; skipping Postgres-backed test {test}");
        return None;
    };
    let admin = PgPool::connect(&admin_url).await.expect("connect admin database");
    let name = format!("hprs_test_{}", rand::random::<u32>());
    sqlx::query(&format!("CREATE DATABASE {name}"))
        .execute(&admin)
        .await
        .expect("create test database");
    let base = admin_url.rsplit_once('/').expect("URL with a database path").0;
    let pool = PgPool::connect(&format!("{base}/{name}"))
        .await
        .expect("connect test database");
    Some(TestDb { admin, pool, name })
}
