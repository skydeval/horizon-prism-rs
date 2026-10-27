//! Shared helpers for Postgres-backed tests. They run when
//! `HPRS_TEST_DATABASE_URL` names a server the tests may create databases on;
//! otherwise [`fresh_db`] returns `None` and the test passes without doing
//! anything.

use sqlx::PgPool;

pub const ENV: &str = "HPRS_TEST_DATABASE_URL";

/// The statements of the degraded fallback this thread ran while
/// [`Noted::start`] holds (degraded-mode T-DM4, T-DM13): each site notes its
/// name as it executes. A current-thread test runtime runs its servers'
/// tasks on the test's own thread, so a test sees its handlers' statements
/// and no other test's.
pub mod noted {
    use std::cell::RefCell;

    thread_local! {
        static NOTED: RefCell<Option<Vec<&'static str>>> = const { RefCell::new(None) };
    }

    /// Note `name`, when this thread is noting.
    pub fn note(name: &'static str) {
        NOTED.with(|n| {
            if let Some(v) = n.borrow_mut().as_mut() {
                v.push(name);
            }
        });
    }

    /// Noting on this thread until dropped.
    pub struct Noted;

    impl Noted {
        #[must_use]
        pub fn start() -> Self {
            NOTED.with(|n| *n.borrow_mut() = Some(Vec::new()));
            Self
        }

        /// What was noted since the start or the last take.
        #[must_use]
        pub fn take(&self) -> Vec<&'static str> {
            NOTED.with(|n| n.borrow_mut().as_mut().map(std::mem::take).unwrap_or_default())
        }
    }

    impl Drop for Noted {
        fn drop(&mut self) {
            NOTED.with(|n| *n.borrow_mut() = None);
        }
    }
}

/// A uniquely named database, dropped by [`TestDb::drop_db`].
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

/// Create a fresh, fully migrated database, or `None` when the env var is
/// unset. It is a copy of the migrated template ([`template`]), so the
/// migrations run once per test run rather than once per test;
/// `_sqlx_migrations` comes with the copy.
pub async fn fresh_db(test: &str) -> Option<TestDb> {
    let Ok(admin_url) = std::env::var(ENV) else {
        eprintln!("{ENV} unset; skipping Postgres-backed test {test}");
        return None;
    };
    let template = template(&admin_url).await;
    Some(create_db(&admin_url, &format!("TEMPLATE {template}")).await)
}

/// The migrated template database's name, built on first use. Its name
/// carries a hash of the migration set, so a changed set gets a template
/// of its own. Built under a Postgres advisory lock, so test binaries
/// sharing the server build it once; marked a template only when complete,
/// so a build that died part way is rebuilt.
async fn template(admin_url: &str) -> &'static str {
    static TEMPLATE: tokio::sync::OnceCell<String> = tokio::sync::OnceCell::const_new();
    TEMPLATE.get_or_init(|| build_template(admin_url)).await
}

async fn build_template(admin_url: &str) -> String {
    use sqlx::Connection;

    // FNV-1a over each migration's version and checksum: stable across
    // builds, unlike `DefaultHasher`.
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
    for m in crate::db::migrations::MIGRATOR.iter() {
        for byte in m.version.to_le_bytes().iter().chain(m.checksum.iter()) {
            hash ^= u64::from(*byte);
            hash = hash.wrapping_mul(0x0100_0000_01b3);
        }
    }
    let name = format!("hprs_template_{hash:016x}");
    // One connection, not a pool: the cell outlives this test's runtime.
    let mut admin = sqlx::PgConnection::connect(admin_url).await.expect("connect admin database");
    sqlx::query("SELECT pg_advisory_lock(hashtextextended('hprs_test_template', 0))")
        .execute(&mut admin)
        .await
        .expect("template lock");
    let built: bool = sqlx::query_scalar("SELECT EXISTS (SELECT 1 FROM pg_database WHERE datname = $1 AND datistemplate)")
        .bind(&name)
        .fetch_one(&mut admin)
        .await
        .expect("read pg_database");
    if !built {
        for sql in [format!("DROP DATABASE IF EXISTS {name} WITH (FORCE)"), format!("CREATE DATABASE {name}")] {
            sqlx::query(&sql).execute(&mut admin).await.expect("create template database");
        }
        let base = admin_url.rsplit_once('/').expect("URL with a database path").0;
        let mut conn = sqlx::PgConnection::connect(&format!("{base}/{name}")).await.expect("connect template database");
        crate::db::migrations::MIGRATOR.run(&mut conn).await.expect("apply migrations");
        conn.close().await.expect("close template connection");
        // No connections from here on: a copy needs none open to its source.
        sqlx::query(&format!("ALTER DATABASE {name} WITH IS_TEMPLATE true ALLOW_CONNECTIONS false"))
            .execute(&mut admin)
            .await
            .expect("mark template");
    }
    let _ = admin.close().await;
    name
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
    /// Private threads T-56: the private pipeline's `head_check` fails on
    /// the database for a DID. Key: the DID (tests use DIDs of their own).
    pub const HEAD_CHECK_FAILS: &str = "s12_head_check_fails";
    /// Private threads T-56: a read through the private pipeline, counted
    /// ([`delay`] with no delay). Key: the post's author (tests use DIDs of
    /// their own).
    pub const PRIVATE_READ: &str = "s12_private_read";
    /// Private threads T-56: `getPostThread`, its structural chain and
    /// prefilter done, before the round and the reads. Key: the anchor's
    /// URI.
    pub const THREAD_AFTER_STRUCTURE: &str = "s12_thread_after_structure";
    /// #723, the control reruns: ingest refuses no upsert below a listed
    /// rev (the tombstone check alone, as before). Key: `<database>/<did>`,
    /// the record's author ([`injected_on`]).
    pub const LISTED_REV_REFUSAL_OFF: &str = "i723_listed_rev_refusal_off";
    /// #723 T-75's control: catch-up's raise runs without its check of the
    /// records it skipped. Key: `<database>/<did>`.
    pub const LISTED_REV_CHECK_OFF: &str = "i723_listed_rev_check_off";
    /// #723 T-76's control: a run raises a collection's listed rev though a
    /// listed write of it failed. Key: `<database>/<did>`.
    pub const LISTED_REV_FORCE_RAISE: &str = "i723_listed_rev_force_raise";
    /// #723 T-69: a job or catch-up raises no listed rev, so its settle is
    /// uncovered. Key: `<database>/<did>`.
    pub const LISTED_REV_SKIP_RAISE: &str = "i723_listed_rev_skip_raise";
    /// #723 T-64, T-75, T-77: a job's or catch-up's listings written, before
    /// its raise. Key: `<database>/<did>`.
    pub const LISTED_REV_BEFORE_RAISE: &str = "i723_listed_rev_before_raise";
    /// #723 T-64 (c), T-66: a job's or catch-up's raise committed, before
    /// the reconcile. Key: `<database>/<did>`.
    pub const LISTED_REV_AFTER_RAISE: &str = "i723_listed_rev_after_raise";
    /// #723 T-77: catch-up fails after its listings, before its raise. Key:
    /// `<database>/<did>`.
    pub const CATCH_UP_FAILS_BEFORE_RAISE: &str = "i723_catch_up_fails_before_raise";
    /// #723 T-77: catch-up fails after its raise committed, before its
    /// reconciles. Key: `<database>/<did>`.
    pub const CATCH_UP_FAILS_AFTER_RAISE: &str = "i723_catch_up_fails_after_raise";

    /// Private-encode T-P6: a private read, its row snapshot taken, before
    /// it decodes and builds the view. Key: the post's URI.
    pub const PRIVATE_SNAPSHOT_TAKEN: &str = "pe_private_snapshot_taken";
    /// Private-encode T-D2: the author's `droppedFields` marker is not set,
    /// as if its code were removed ([`injected`]). Key: the post's URI.
    pub const DROPPED_MARKER_OFF: &str = "pe_dropped_marker_off";
    /// Not-served-reason T-N3: the author's `notServed` marker is not set,
    /// neither `overClaim` in the pipeline nor the thread reasons in
    /// `decorate`, as if its code were removed ([`injected_in`]). Key:
    /// `<database>/<caller>`.
    pub const NOT_SERVED_OFF: &str = "ns_not_served_off";
    /// Not-served-reason T-N3, T-N8: a counting site, hit once each time
    /// `decorate` runs the `notServed` statement ([`injected_in`]). Key:
    /// `<database>/<caller>`.
    pub const NOT_SERVED_RUN: &str = "ns_not_served_run";

    /// Scope recency T-R5, T-R6, T-R6b, T-R8, T-R16: the daily sweep, its
    /// holds computed and written and its candidates chosen, before the
    /// first candidate's marker. Key: `<database>/sweep` ([`pause_in`]).
    pub const SWEEP_AFTER_COMPUTE: &str = "sr_sweep_after_compute";
    /// Scope recency T-R16: `putActivitySubscription`, holding the scope
    /// read guard after its second scope check, before its upsert. Key: the
    /// subject.
    pub const SUBSCRIBE_HOLDS_GUARD: &str = "sr_subscribe_holds_guard";
    /// Membership T-M7: the never-ready expiry, its candidates selected,
    /// before the first candidate's marker. Key: `<database>/expiry`
    /// ([`pause_in`]).
    pub const EXPIRY_AFTER_SELECT: &str = "m_expiry_after_select";
    /// The private pipeline under the degraded fallback, after step 3′
    /// found every repo Current and before the snapshot (degraded-mode
    /// T-DM3b). Key: the post's URI.
    pub const FALLBACK_AFTER_CHECK: &str = "dm_fallback_after_check";
    /// The snapshot under the degraded fallback, before fact 8 (after the
    /// climb). Key: the post's URI.
    pub const FALLBACK_BEFORE_FACT8: &str = "dm_fallback_before_fact8";

    /// The subscribed-post fan-out (private-activity-subs T-AS13): a
    /// claimed run stops before it settles its row, as a crash would. Key:
    /// the post's URI.
    pub const FANOUT_CRASH: &str = "as_fanout_crash";
    /// The fan-out, its post-level checks passed, before the subscribers'
    /// verdicts (T-AS7d, T-AS13). Key: the post's URI.
    pub const FANOUT_BEFORE_VERDICT: &str = "as_fanout_before_verdict";
    /// The fan-out, its verdicts decided, before its write (T-AS9). Key:
    /// the post's URI.
    pub const FANOUT_BEFORE_WRITE: &str = "as_fanout_before_write";

    /// Private appearance T-AP10: `putAppearance`'s write, its transaction
    /// begun, before the DID's shared lock. Key: `<database>/<did>`
    /// ([`pause_in`]).
    pub const APPEARANCE_BEFORE_LOCK: &str = "ap_put_before_lock";
    /// Private appearance T-AP10: `putAppearance`'s write holding the DID's
    /// shared lock, before its membership check. Key: `<database>/<did>`.
    pub const APPEARANCE_AFTER_LOCK: &str = "ap_put_after_lock";
    /// Private notification preferences T-N11: `putNotificationPreferences`'
    /// write, its transaction begun, before the DID's shared lock. Key:
    /// `<database>/<did>` ([`pause_in`]).
    pub const NOTIFICATION_PREFS_BEFORE_LOCK: &str = "np_put_before_lock";
    /// Private notification preferences T-N11: the write holding the DID's
    /// shared lock, before its membership check. Key: `<database>/<did>`.
    pub const NOTIFICATION_PREFS_AFTER_LOCK: &str = "np_put_after_lock";
    /// Thread outline (T-O4): a flush fails transiently, keyed by the
    /// database's name.
    pub const OUTLINE_FLUSH_TRANSIENT: &str = "to_flush_transient";
    /// A flush holding a create by `<database>/<did>` fails
    /// deterministically.
    pub const OUTLINE_FLUSH_DETERMINISTIC: &str = "to_flush_deterministic";
    /// The outline writer, before it handles a message.
    pub const OUTLINE_WRITER_PAUSE: &str = "to_writer_pause";
    /// The outline's replay, between chunks.
    pub const OUTLINE_REPLAY_BETWEEN_CHUNKS: &str = "to_replay_between_chunks";

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

    /// An error site keyed `<database>/<did>`, the database named by the
    /// pool's options, as [`injected`].
    #[must_use]
    pub fn injected_in(name: &'static str, pool: &sqlx::PgPool, did: &str) -> bool {
        if ARMED.load(Ordering::Acquire) == 0 {
            return false;
        }
        let db = pool.connect_options().get_database().unwrap_or_default().to_string();
        injected(name, &format!("{db}/{did}"))
    }

    /// An error site keyed `<database>/<did>` on a connection the caller
    /// holds, its own transaction included. Unarmed, it reads nothing.
    pub async fn injected_on(name: &'static str, conn: &mut sqlx::PgConnection, did: &str) -> bool {
        if ARMED.load(Ordering::Acquire) == 0 {
            return false;
        }
        let db: String = sqlx::query_scalar("SELECT current_database()::text").fetch_one(conn).await.unwrap_or_default();
        injected(name, &format!("{db}/{did}"))
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

    /// Whether a wait at the pause site `name`, armed for `key`, is being
    /// held: reached and not yet released.
    #[must_use]
    pub fn holding(name: &'static str, key: &str) -> bool {
        lookup(name, key).is_some_and(|point| *point.hits.borrow() > 0 && !point.gate.is_closed())
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
    Some(create_db(&admin_url, "").await)
}

/// A uniquely named database, created with `options` (a template or none).
async fn create_db(admin_url: &str, options: &str) -> TestDb {
    let admin = PgPool::connect(admin_url).await.expect("connect admin database");
    let name = format!("hprs_test_{}", rand::random::<u32>());
    sqlx::query(&format!("CREATE DATABASE {name} {options}"))
        .execute(&admin)
        .await
        .expect("create test database");
    // As every deployed pool runs (`db::pool`): no JIT. The shared walk's
    // recursive CTEs carry estimates far past `jit_above_cost`. A template
    // does not pass on its settings, so each database gets its own.
    sqlx::query(&format!("ALTER DATABASE {name} SET jit = off"))
        .execute(&admin)
        .await
        .expect("jit off");
    let base = admin_url.rsplit_once('/').expect("URL with a database path").0;
    let pool = PgPool::connect(&format!("{base}/{name}"))
        .await
        .expect("connect test database");
    TestDb { admin, pool, name }
}
