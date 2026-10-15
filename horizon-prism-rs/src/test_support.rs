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
/// Read metrics with a recorder set by `metrics::set_default_local_recorder`
/// in a `#[tokio::test]`, whose runtime runs on the test's thread.
#[must_use]
pub fn series(text: &str, name: &str, labels: &[&str]) -> Option<f64> {
    text.lines()
        .filter(|l| l.strip_prefix(name).is_some_and(|rest| rest.starts_with('{')))
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

    /// How long [`Armed::reached`] waits before the test fails.
    const REACH_WITHIN: Duration = Duration::from_secs(20);

    static ARMED: AtomicUsize = AtomicUsize::new(0);

    struct Point {
        hits: watch::Sender<usize>,
        /// Closed on release: every wait at the site then passes.
        gate: Semaphore,
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

    /// A pause site: when `name` is armed for `key`, count the hit and wait
    /// for the test's release.
    pub async fn pause(name: &'static str, key: &str) {
        let Some(point) = lookup(name, key) else {
            return;
        };
        point.hits.send_modify(|n| *n += 1);
        let _ = point.gate.acquire().await;
    }

    /// An error site: whether `name` is armed for `key`, so the site fails
    /// as if its statement had. Counts the hit.
    #[must_use]
    pub fn injected(name: &'static str, key: &str) -> bool {
        lookup(name, key).is_some_and(|point| {
            point.hits.send_modify(|n| *n += 1);
            true
        })
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
        let point = Arc::new(Point { hits: watch::Sender::new(0), gate: Semaphore::new(0) });
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
