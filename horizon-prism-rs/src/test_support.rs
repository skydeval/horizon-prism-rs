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
    /// Public page T-P17: `Writer::apply_commit` sleeps for the armed delay
    /// at its top and goes on, never failing ([`delay`]). Key:
    /// `<database>/<repo>`.
    pub const WRITER_DELAY_REPO: &str = "pp_writer_delay_repo";
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
    /// Private audiences T-A8: `createAudience`'s or `updateAudience`'s
    /// write, past the gate, before the scope guard and the locks. Key:
    /// `<database>/<did>` ([`pause_in`]).
    pub const AUDIENCE_BEFORE_LOCK: &str = "pa_write_before_lock";
    /// Private audiences T-A4: counts `updateAudience`'s writes reached.
    /// Key: `<database>/<did>` ([`injected_in`]).
    pub const AUDIENCE_UPDATE_REACHED: &str = "pa_update_reached";
    /// Private audiences T-A16: `deleteAudience`'s write, its owner
    /// re-evaluated, before its commit. Key: `<database>/<did>`.
    pub const AUDIENCE_DELETE_BEFORE_COMMIT: &str = "pa_delete_before_commit";
    /// Private audiences T-A16: `deleteAudience`, its read guard dropped,
    /// before it asks for the write guard. Key: `<database>/<did>`.
    pub const AUDIENCE_BEFORE_WRITE_GUARD: &str = "pa_delete_before_write_guard";
    /// Private audiences T-A16: the in-memory owner set taken from the
    /// transaction's report, not from the database. Key: `<database>/<did>`.
    pub const OWNER_SET_FROM_REPORT: &str = "pa_owner_set_from_report";
    /// Private audiences T-A16: `owners::lock` taken as a no-op. Key:
    /// `<database>/<did>` ([`injected_on`]).
    pub const OWNER_LOCK_OFF: &str = "pa_owner_lock_off";
    /// Private audiences T-A16: the writer, just after it takes a
    /// commit's owner lock. Key: `<database>/<did>`.
    pub const WRITER_AFTER_OWNER_LOCK: &str = "pa_writer_after_owner_lock";
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

/// Test rows of the id-keyed graph tables, written from DID text and record
/// key text as ingest would store them.
pub mod graph {
    use chrono::{DateTime, Utc};
    use sqlx::PgPool;

    use crate::db::ids::{Ids, rev_value};

    /// A row to write: `table` is `follows` or `blocks`.
    #[derive(Debug, Clone)]
    pub struct Edge<'a> {
        pub table: &'static str,
        pub author: &'a str,
        pub subject: &'a str,
        pub rkey: &'a str,
        pub rev: &'a str,
        pub created_at: Option<DateTime<Utc>>,
    }

    /// The rev rows get unless a test names one: the lowest TID above the
    /// inbound-block sentinel, so any real write wins and the row is not
    /// taken for a sentinel row. (A rev that is not a TID is stored as the
    /// sentinel's value.)
    pub const REV: &str = "2222222222223";

    impl<'a> Edge<'a> {
        /// A follow at [`REV`], created now.
        #[must_use]
        pub fn follow(author: &'a str, subject: &'a str, rkey: &'a str) -> Self {
            Self { table: "follows", author, subject, rkey, rev: REV, created_at: None }
        }

        /// A block at [`REV`], created now.
        #[must_use]
        pub fn block(author: &'a str, subject: &'a str, rkey: &'a str) -> Self {
            Self { table: "blocks", author, subject, rkey, rev: REV, created_at: None }
        }

        #[must_use]
        pub fn rev(mut self, rev: &'a str) -> Self {
            self.rev = rev;
            self
        }

        #[must_use]
        pub fn at(mut self, created_at: DateTime<Utc>) -> Self {
            self.created_at = Some(created_at);
            self
        }

        /// Write the row, interning its DIDs and key first.
        ///
        /// # Panics
        ///
        /// On a database error or a DID or key that is not valid.
        pub async fn put(&self, pool: &PgPool) {
            let ids = Ids::new(pool.clone());
            let accounts = ids.account_ids(&[self.author.to_string(), self.subject.to_string()]).await.expect("intern");
            let author = accounts[self.author];
            let rkey = ids.rkey_ids(&[(author, self.rkey.to_string())]).await.expect("alias")[&(author, self.rkey.to_string())];
            sqlx::query(&format!(
                "INSERT INTO {} (author, rkey, subject, rev, created_at) VALUES ($1, $2, $3, $4, COALESCE($5, now()))",
                self.table
            ))
            .bind(author)
            .bind(rkey)
            .bind(accounts[self.subject])
            .bind(rev_value(self.rev))
            .bind(self.created_at)
            .execute(pool)
            .await
            .expect("graph row");
        }
    }

    /// A follow at [`REV`], created now.
    pub async fn follow(pool: &PgPool, author: &str, subject: &str, rkey: &str) {
        Edge::follow(author, subject, rkey).put(pool).await;
    }

    /// A block at [`REV`], created now.
    pub async fn block(pool: &PgPool, author: &str, subject: &str, rkey: &str) {
        Edge::block(author, subject, rkey).put(pool).await;
    }

    /// The URI of every row of `table`, ordered by URI text.
    pub async fn uris(pool: &PgPool, table: &str) -> Vec<String> {
        let coll = if table == "follows" { 4 } else { 5 };
        sqlx::query_scalar(&format!("SELECT at_uri(author, {coll}::smallint, rkey) FROM {table} ORDER BY 1"))
            .fetch_all(pool)
            .await
            .expect("graph uris")
    }

    /// Every row of `table` as `(author DID, subject DID)`, ordered.
    pub async fn pairs(pool: &PgPool, table: &str) -> Vec<(String, String)> {
        sqlx::query_as(&format!(
            "SELECT a.did, s.did FROM {table} t JOIN dids a ON a.id = t.author JOIN dids s ON s.id = t.subject ORDER BY 1, 2"
        ))
        .fetch_all(pool)
        .await
        .expect("graph pairs")
    }
}

/// Test rows of the id-keyed likes and reposts and of the count tables,
/// written from DID text and URI text as ingest would store them.
pub mod interactions {
    use chrono::{DateTime, Utc};
    use sqlx::PgPool;

    use crate::db::ids::{Ids, OddTable, Slot, StoredRef, cid_to_db, ref_did, ref_rkey, rev_value, stored_ref};

    pub use super::graph::REV;

    /// A like or repost to write.
    #[derive(Debug, Clone)]
    pub struct Interaction<'a> {
        pub table: &'static str,
        pub author: &'a str,
        pub rkey: &'a str,
        pub subject: &'a str,
        pub subject_cid: Option<&'a str>,
        pub via: Option<(&'a str, Option<&'a str>)>,
        pub rev: &'a str,
        pub created_at: Option<DateTime<Utc>>,
    }

    impl<'a> Interaction<'a> {
        /// A like at [`REV`], created now, with no CID.
        #[must_use]
        pub fn like(author: &'a str, rkey: &'a str, subject: &'a str) -> Self {
            Self { table: "likes", author, rkey, subject, subject_cid: None, via: None, rev: REV, created_at: None }
        }

        /// A repost at [`REV`], created now, with no CID.
        #[must_use]
        pub fn repost(author: &'a str, rkey: &'a str, subject: &'a str) -> Self {
            Self { table: "reposts", ..Self::like(author, rkey, subject) }
        }

        #[must_use]
        pub fn cid(mut self, cid: &'a str) -> Self {
            self.subject_cid = Some(cid);
            self
        }

        #[must_use]
        pub fn at(mut self, created_at: DateTime<Utc>) -> Self {
            self.created_at = Some(created_at);
            self
        }

        /// The row's URI.
        #[must_use]
        pub fn uri(&self) -> String {
            let nsid = if self.table == "likes" { "app.bsky.feed.like" } else { "app.bsky.feed.repost" };
            format!("at://{}/{nsid}/{}", self.author, self.rkey)
        }

        /// Write the row, interning its ids first; an odd subject or `via`
        /// keeps its text in `odd_refs`. Returns the row's URI.
        ///
        /// # Panics
        ///
        /// On a database error or an author or key that is not valid.
        pub async fn put(&self, pool: &PgPool) -> String {
            let ids = Ids::new(pool.clone());
            let via_uri = self.via.map(|(u, _)| u);
            let mut dids = vec![self.author.to_string()];
            dids.extend(ref_did(self.subject));
            dids.extend(via_uri.and_then(ref_did));
            let accounts = ids.account_ids(&dids).await.expect("intern");
            let author = accounts[self.author];
            let mut keys = vec![(author, self.rkey.to_string())];
            for uri in std::iter::once(self.subject).chain(via_uri) {
                if let Some((did, rkey)) = ref_rkey(uri)
                    && let Some(a) = accounts.get(&did)
                {
                    keys.push((*a, rkey));
                }
            }
            let rkeys = ids.rkey_ids(&keys).await.expect("alias");
            let rkey = rkeys[&(author, self.rkey.to_string())];
            let subject = stored_ref(self.subject, &accounts, &rkeys);
            let via = via_uri.map(|u| stored_ref(u, &accounts, &rkeys));
            let (sa, sc, sr) = subject.columns();
            let (va, vc, vr) = via.as_ref().map_or((None, None, None), |v| {
                let (a, c, r) = v.columns();
                (a, Some(c), r)
            });
            sqlx::query(&format!(
                "INSERT INTO {} (author, rkey, subj_author, subj_coll, subj_rkey, subj_cid, rev, created_at, \
                   via_author, via_coll, via_rkey, via_cid) \
                 VALUES ($1, $2, $3, $4, $5, $6, $7, COALESCE($8, now()), $9, $10, $11, $12)",
                self.table
            ))
            .bind(author)
            .bind(rkey)
            .bind(sa)
            .bind(sc)
            .bind(sr)
            .bind(self.subject_cid.map(cid_to_db))
            .bind(rev_value(self.rev))
            .bind(self.created_at)
            .bind(va)
            .bind(vc)
            .bind(vr)
            .bind(self.via.and_then(|(_, c)| c).map(cid_to_db))
            .execute(pool)
            .await
            .expect("interaction row");
            let tbl = if self.table == "likes" { OddTable::Likes } else { OddTable::Reposts };
            for (slot, r) in [(Slot::Subject, Some(&subject)), (Slot::Via, via.as_ref())] {
                if let Some(StoredRef::Odd { raw, .. }) = r {
                    sqlx::query("INSERT INTO odd_refs (tbl, author, rkey, slot, raw) VALUES ($1, $2, $3, $4, $5)")
                        .bind(tbl.id())
                        .bind(author)
                        .bind(rkey)
                        .bind(slot.id())
                        .bind(raw)
                        .execute(pool)
                        .await
                        .expect("odd ref");
                }
            }
            self.uri()
        }
    }

    /// The CTEs that intern every DID and record key `parts` names, as
    /// `d(id, did)` and `ka(id, author, rkey)`. `parts` is a SELECT of
    /// `(did, rkey)` text pairs (a NULL rkey interns the DID only). A test's
    /// own fixture: it interns inside the statement, which only an
    /// autocommit test write may do.
    fn intern_ctes(parts: &str) -> String {
        format!(
            "x AS ({parts}), \
             nd AS (INSERT INTO dids (did) SELECT DISTINCT x.did FROM x \
                    WHERE did_is_valid(x.did) AND NOT EXISTS (SELECT 1 FROM dids o WHERE o.did = x.did) \
                    ON CONFLICT (did) DO NOTHING RETURNING id, did), \
             d AS (SELECT id, did FROM nd UNION ALL SELECT id, did FROM dids WHERE did IN (SELECT did FROM x)), \
             na AS (INSERT INTO rkey_aliases (author, rkey) SELECT DISTINCT d.id, x.rkey FROM x JOIN d ON d.did = x.did \
                    WHERE x.rkey IS NOT NULL AND rkey_to_bigint(x.rkey) IS NULL \
                      AND NOT EXISTS (SELECT 1 FROM rkey_aliases o WHERE o.author = d.id AND o.rkey = x.rkey) \
                    ON CONFLICT (author, rkey) DO NOTHING RETURNING id, author, rkey), \
             ka AS (SELECT id, author, rkey FROM na \
                    UNION ALL SELECT o.id, o.author, o.rkey FROM rkey_aliases o JOIN d ON d.id = o.author)"
        )
    }

    /// One SQL statement that stores a like or repost (`table`) of a whole
    /// subject from text, as ingest keys it, at [`REV`]; `created` is an SQL
    /// expression. Every DID and key is interned in the statement.
    #[must_use]
    pub fn row_sql(table: &str, author: &str, rkey: &str, subject: &str, created: &str) -> String {
        let ctes = intern_ctes(&format!(
            "SELECT '{author}'::text AS did, '{rkey}'::text AS rkey \
             UNION ALL SELECT split_part(substr('{subject}', 6), '/', 1), split_part(substr('{subject}', 6), '/', 3)"
        ));
        let key = |did: &str, rkey: &str| {
            format!("COALESCE(rkey_to_bigint({rkey}), (SELECT ka.id FROM ka WHERE ka.author = {did} AND ka.rkey = {rkey} LIMIT 1))")
        };
        format!(
            "WITH {ctes} \
             INSERT INTO {table} (author, rkey, subj_author, subj_coll, subj_rkey, rev, created_at) \
             SELECT da.id, {own}, ds.id, \
                    (SELECT c.id FROM collections c WHERE c.nsid = split_part(substr('{subject}', 6), '/', 2)), {subj}, \
                    rev_to_bigint('{REV}'), {created} \
             FROM d da, d ds WHERE da.did = '{author}' AND ds.did = split_part(substr('{subject}', 6), '/', 1)",
            own = key("da.id", &format!("'{rkey}'")),
            subj = key("ds.id", &format!("split_part(substr('{subject}', 6), '/', 3)")),
        )
    }

    /// One SQL statement that sets a post's four counts from its URI text,
    /// interning its author and key in the statement.
    #[must_use]
    pub fn post_counts_sql(post: &str, like: i32, repost: i32, reply: i32, quote: i32) -> String {
        let ctes = intern_ctes(&format!(
            "SELECT split_part(substr('{post}', 6), '/', 1) AS did, split_part(substr('{post}', 6), '/', 3) AS rkey"
        ));
        format!(
            "WITH {ctes} \
             INSERT INTO post_counts (author, rkey, like_count, repost_count, reply_count, quote_count) \
             SELECT d.id, COALESCE(rkey_to_bigint(split_part(substr('{post}', 6), '/', 3)), \
                      (SELECT ka.id FROM ka WHERE ka.author = d.id AND ka.rkey = split_part(substr('{post}', 6), '/', 3) LIMIT 1)), \
                    {like}, {repost}, {reply}, {quote} \
             FROM d WHERE d.did = split_part(substr('{post}', 6), '/', 1) \
             ON CONFLICT (author, rkey) DO UPDATE SET like_count = EXCLUDED.like_count, repost_count = EXCLUDED.repost_count, \
               reply_count = EXCLUDED.reply_count, quote_count = EXCLUDED.quote_count"
        )
    }

    /// A like at [`REV`], created now. Returns its URI.
    pub async fn like(pool: &PgPool, author: &str, rkey: &str, subject: &str) -> String {
        Interaction::like(author, rkey, subject).put(pool).await
    }

    /// A repost at [`REV`], created now. Returns its URI.
    pub async fn repost(pool: &PgPool, author: &str, rkey: &str, subject: &str) -> String {
        Interaction::repost(author, rkey, subject).put(pool).await
    }

    /// The subject text of every row of `table` (likes or reposts), ordered.
    pub async fn subjects(pool: &PgPool, table: &str) -> Vec<String> {
        let tbl = if table == "likes" { 2 } else { 3 };
        sqlx::query_scalar(&format!(
            "SELECT ref_text(subj_author, subj_coll, subj_rkey, {tbl}::smallint, author, rkey, 4::smallint) FROM {table} ORDER BY 1"
        ))
        .fetch_all(pool)
        .await
        .expect("interaction subjects")
    }

    /// The rows of `table` (likes or reposts) by `did`.
    pub async fn count_by(pool: &PgPool, table: &str, did: &str) -> i64 {
        sqlx::query_scalar(&format!("SELECT count(*) FROM {table} WHERE author = (SELECT id FROM dids WHERE did = $1)"))
            .bind(did)
            .fetch_one(pool)
            .await
            .expect("interaction count")
    }

    /// Intern the authorities and record keys of these URIs, as the writer
    /// does before it stores a record naming them.
    pub async fn intern(pool: &PgPool, uris: &[&str]) {
        let ids = Ids::new(pool.clone());
        let dids: Vec<String> = uris.iter().filter_map(|u| ref_did(u)).collect();
        let accounts = ids.account_ids(&dids).await.expect("intern");
        let keys: Vec<_> = uris
            .iter()
            .filter_map(|u| ref_rkey(u))
            .filter_map(|(did, rkey)| accounts.get(&did).map(|a| (*a, rkey)))
            .collect();
        ids.rkey_ids(&keys).await.expect("alias");
    }

    /// Set a post's four counts `(like, repost, reply, quote)`.
    pub async fn set_post_counts(pool: &PgPool, post: &str, counts: (i32, i32, i32, i32)) {
        intern(pool, &[post]).await;
        sqlx::query(
            "INSERT INTO post_counts (author, rkey, like_count, repost_count, reply_count, quote_count) \
             SELECT r.author, r.rkey, $2, $3, $4, $5 FROM ref_of($1) r WHERE r.coll = 1 \
             ON CONFLICT (author, rkey) DO UPDATE SET like_count = EXCLUDED.like_count, repost_count = EXCLUDED.repost_count, \
               reply_count = EXCLUDED.reply_count, quote_count = EXCLUDED.quote_count",
        )
        .bind(post)
        .bind(counts.0)
        .bind(counts.1)
        .bind(counts.2)
        .bind(counts.3)
        .execute(pool)
        .await
        .expect("post counts");
    }

    /// A post's four counts `(like, repost, reply, quote)`, when it has a row.
    pub async fn post_counts(pool: &PgPool, post: &str) -> Option<(i32, i32, i32, i32)> {
        sqlx::query_as(
            "SELECT c.like_count, c.repost_count, c.reply_count, c.quote_count FROM post_counts c, ref_of($1) r \
             WHERE r.coll = 1 AND c.author = r.author AND c.rkey = r.rkey",
        )
        .bind(post)
        .fetch_optional(pool)
        .await
        .expect("post counts")
    }

    /// Set an account's three counts `(followers, follows, posts)`.
    pub async fn set_actor_counts(pool: &PgPool, did: &str, counts: (i32, i32, i32)) {
        let ids = Ids::new(pool.clone());
        let id = ids.account_ids(&[did.to_string()]).await.expect("intern")[did];
        sqlx::query(
            "INSERT INTO actor_counts (id, followers_count, follows_count, posts_count) VALUES ($1, $2, $3, $4) \
             ON CONFLICT (id) DO UPDATE SET followers_count = EXCLUDED.followers_count, \
               follows_count = EXCLUDED.follows_count, posts_count = EXCLUDED.posts_count",
        )
        .bind(id)
        .bind(counts.0)
        .bind(counts.1)
        .bind(counts.2)
        .execute(pool)
        .await
        .expect("actor counts");
    }
}

/// Test rows of the id-keyed `posts`, `feed_items` and `record_tombstones`,
/// written from URI text as ingest would store them.
pub mod posts {
    use chrono::{DateTime, Utc};
    use serde_json::Value;
    use sqlx::PgPool;

    pub use crate::firehose::writer::fixtures::{put_feed_item, put_post, rewrite_post};

    /// SQL: the post row (alias `alias`, or none) is the one `uri` names.
    #[must_use]
    pub fn key_is(alias: &str, uri: &str) -> String {
        let dot = if alias.is_empty() { String::new() } else { format!("{alias}.") };
        format!("({dot}author, {dot}rkey) = (SELECT r.author, r.rkey FROM ref_of('{uri}') r WHERE r.coll = 1)")
    }

    /// A stored post with this record at rev `REV`, created at
    /// `created_at` (or now), with no feed item. Returns its URI.
    pub async fn put(pool: &PgPool, author: &str, rkey: &str, record: &Value, created_at: Option<DateTime<Utc>>) -> String {
        put_post(pool, author, rkey, &format!("bafy{rkey}"), super::graph::REV, record, Some(created_at.unwrap_or_else(Utc::now))).await
    }

    /// A plain stored post (`text`), created now, with no feed item.
    pub async fn plain(pool: &PgPool, author: &str, rkey: &str, text: &str) -> String {
        let record = serde_json::json!({"$type": "app.bsky.feed.post", "text": text, "createdAt": "2026-09-26T12:00:00.000Z"});
        put(pool, author, rkey, &record, None).await
    }

    /// A stored post's record.
    pub async fn record(pool: &PgPool, uri: &str) -> Option<Value> {
        let stored: Option<Vec<u8>> = sqlx::query_scalar(&format!("SELECT record FROM posts WHERE {}", key_is("", uri)))
            .fetch_optional(pool)
            .await
            .expect("record");
        match stored {
            Some(bytes) => Some(crate::db::records::decode_value(pool, &bytes).await.expect("decode")),
            None => None,
        }
    }

    /// Change a stored post's record with `edit`, as a newer write would.
    pub async fn edit(pool: &PgPool, uri: &str, edit: impl FnOnce(&mut Value)) {
        let mut record = record(pool, uri).await.expect("stored post");
        edit(&mut record);
        rewrite_post(pool, uri, &record).await;
    }

    /// Set a stored post's `created_at` (an SQL expression), on its search
    /// row too.
    pub async fn set_created_at(pool: &PgPool, uri: &str, at: &str) {
        for table in ["posts", "post_search"] {
            sqlx::query(&format!("UPDATE {table} SET created_at = {at} WHERE {}", key_is("", uri)))
                .execute(pool)
                .await
                .expect("created_at");
        }
    }

    /// The URI of every stored post, ordered by text.
    pub async fn uris(pool: &PgPool) -> Vec<String> {
        sqlx::query_scalar("SELECT at_uri(author, 1::smallint, rkey) FROM posts ORDER BY 1").fetch_all(pool).await.expect("posts")
    }

    /// A stored post's text, from its search row.
    pub async fn text(pool: &PgPool, uri: &str) -> Option<String> {
        sqlx::query_scalar(&format!("SELECT text FROM post_search WHERE {}", key_is("", uri)))
            .fetch_optional(pool)
            .await
            .expect("text")
    }

    /// Whether `uri` names a stored post.
    pub async fn stored(pool: &PgPool, uri: &str) -> bool {
        sqlx::query_scalar(&format!("SELECT EXISTS (SELECT 1 FROM posts WHERE {})", key_is("", uri)))
            .fetch_one(pool)
            .await
            .expect("stored")
    }

    /// The stored posts by `did`.
    pub async fn count_by(pool: &PgPool, did: &str) -> i64 {
        sqlx::query_scalar("SELECT count(*) FROM posts WHERE author = (SELECT id FROM dids WHERE did = $1)")
            .bind(did)
            .fetch_one(pool)
            .await
            .expect("count")
    }

    /// Remove the feed item of the record `item`.
    pub async fn delete_feed_item(pool: &PgPool, item: &str) {
        sqlx::query(
            "DELETE FROM feed_items f USING ref_of($1) r \
             WHERE f.originator = r.author AND f.is_repost = (r.coll = 3) AND f.src_rkey = r.rkey",
        )
        .bind(item)
        .execute(pool)
        .await
        .expect("feed item");
    }

    /// Store a tombstone of `uri` at `rev`, interning its author and key.
    pub async fn tombstone(pool: &PgPool, uri: &str, rev: &str) {
        super::interactions::intern(pool, &[uri]).await;
        let mut conn = pool.acquire().await.expect("conn");
        crate::db::tombstones::put(&mut *conn, uri, rev, crate::db::tombstones::Put::Highest).await.expect("tombstone");
    }

    /// The rev of the tombstone of `uri`, if there is one.
    pub async fn tombstone_rev(pool: &PgPool, uri: &str) -> Option<String> {
        let rev: Option<i64> = sqlx::query_scalar(
            "SELECT t.rev FROM record_tombstones t, ref_of($1) k WHERE t.author = k.author AND t.coll = k.coll AND t.rkey = k.rkey",
        )
        .bind(uri)
        .fetch_optional(pool)
        .await
        .expect("tombstone");
        rev.map(crate::db::ids::rev_string)
    }

    /// The `n`th of a run of TID record keys, in order.
    #[must_use]
    pub fn tid(n: usize) -> String {
        const ALPHABET: &[u8] = b"234567abcdefghijklmnopqrstuvwxyz";
        let digit = |d: usize| char::from(ALPHABET[d % 32]);
        format!("3kaaaaaaaa{}{}{}", digit(n / 1024), digit(n / 32), digit(n))
    }

    /// Statements that intern a DID, and an alias for `rkey` when it is not
    /// a TID (SQL-only fixtures, run with `raw_sql`).
    #[must_use]
    pub fn intern_sql(did: &str, rkey: &str) -> String {
        format!(
            "INSERT INTO dids (did) VALUES ('{did}') ON CONFLICT (did) DO NOTHING; \
             INSERT INTO rkey_aliases (author, rkey) SELECT id, '{rkey}' FROM dids WHERE did = '{did}' AND rkey_to_bigint('{rkey}') IS NULL \
               ON CONFLICT (author, rkey) DO NOTHING;"
        )
    }

    /// SQL: the stored key of `rkey` in `did`'s repo, after [`intern_sql`].
    #[must_use]
    pub fn key_sql(did: &str, rkey: &str) -> String {
        format!(
            "COALESCE(rkey_to_bigint('{rkey}'), (SELECT x.id FROM rkey_aliases x WHERE x.author = (SELECT id FROM dids WHERE did = '{did}') AND x.rkey = '{rkey}'))"
        )
    }

    /// Statements that store a bare post row (an empty record, created
    /// now) with no search row.
    #[must_use]
    pub fn row_sql(did: &str, rkey: &str) -> String {
        format!(
            "{} INSERT INTO posts (author, rkey, cid, rev, created_at, record) \
               SELECT id, {}, '\\x00'::bytea || convert_to('bafy{rkey}', 'UTF8'), rev_to_bigint('{}'), now(), \
                      '\\x00'::bytea || convert_to('{{}}', 'UTF8') \
               FROM dids WHERE did = '{did}';",
            intern_sql(did, rkey),
            key_sql(did, rkey),
            super::graph::REV
        )
    }

    /// Statements that store a post's own feed item (kind post, now).
    #[must_use]
    pub fn feed_item_sql(did: &str, rkey: &str) -> String {
        format!(
            "{} INSERT INTO feed_items (originator, is_repost, src_rkey, kind, post_author, post_rkey, sort_at) \
               SELECT id, false, {k}, 1, id, {k}, now() FROM dids WHERE did = '{did}';",
            intern_sql(did, rkey),
            k = key_sql(did, rkey)
        )
    }

    /// Statements that store a tombstone of the record `uri` at `rev`.
    #[must_use]
    pub fn tombstone_sql(uri: &str, rev: &str) -> String {
        let rest = uri.strip_prefix("at://").unwrap_or(uri);
        let mut parts = rest.split('/');
        let (did, nsid, rkey) = (parts.next().unwrap_or(""), parts.next().unwrap_or(""), parts.next().unwrap_or(""));
        format!(
            "{} INSERT INTO record_tombstones (author, coll, rkey, rev) \
               SELECT d.id, c.id, {}, rev_to_bigint('{rev}') FROM dids d, collections c WHERE d.did = '{did}' AND c.nsid = '{nsid}';",
            intern_sql(did, rkey),
            key_sql(did, rkey)
        )
    }
}

/// Test rows of the id-keyed v3 record tables (lists and their records,
/// starter packs, gates, polls, labeler services, self records) and of
/// members' post references, written from their records as ingest would
/// store them.
pub mod records {
    use chrono::{DateTime, Utc};
    use serde_json::{Value, json};
    use sqlx::PgPool;

    pub use super::graph::REV;
    pub use crate::firehose::writer::fixtures::put_record;

    /// A record's `createdAt`, or now's.
    fn created(at: Option<DateTime<Utc>>) -> String {
        at.unwrap_or_else(Utc::now).to_rfc3339_opts(chrono::SecondsFormat::Millis, true)
    }

    /// Store `record` in `collection` at [`REV`]. Returns its URI.
    pub async fn put(pool: &PgPool, author: &str, collection: &str, rkey: &str, record: &Value) -> String {
        put_record(pool, author, collection, rkey, &format!("bafy{rkey}"), REV, record).await
    }

    /// A list, created at `at` (or now).
    pub async fn list_at(pool: &PgPool, owner: &str, rkey: &str, name: &str, purpose: &str, at: Option<DateTime<Utc>>) -> String {
        let record = json!({"$type": "app.bsky.graph.list", "name": name, "purpose": purpose, "description": format!("about {name}"), "createdAt": created(at)});
        put(pool, owner, "app.bsky.graph.list", rkey, &record).await
    }

    /// A list, created now.
    pub async fn list(pool: &PgPool, owner: &str, rkey: &str, name: &str, purpose: &str) -> String {
        list_at(pool, owner, rkey, name, purpose, None).await
    }

    /// A list item of the owner's list `list`, created at `at` (or now).
    pub async fn list_item_at(pool: &PgPool, owner: &str, rkey: &str, list: &str, subject: &str, at: Option<DateTime<Utc>>) -> String {
        let record = json!({"$type": "app.bsky.graph.listitem", "list": list, "subject": subject, "createdAt": created(at)});
        put(pool, owner, "app.bsky.graph.listitem", rkey, &record).await
    }

    /// A list item, created now.
    pub async fn list_item(pool: &PgPool, owner: &str, rkey: &str, list: &str, subject: &str) -> String {
        list_item_at(pool, owner, rkey, list, subject, None).await
    }

    /// A list block of `list`, created at `at` (or now).
    pub async fn list_block_at(pool: &PgPool, author: &str, rkey: &str, list: &str, at: Option<DateTime<Utc>>) -> String {
        let record = json!({"$type": "app.bsky.graph.listblock", "subject": list, "createdAt": created(at)});
        put(pool, author, "app.bsky.graph.listblock", rkey, &record).await
    }

    /// A list block, created now.
    pub async fn list_block(pool: &PgPool, author: &str, rkey: &str, list: &str) -> String {
        list_block_at(pool, author, rkey, list, None).await
    }

    /// A reference-list opt-out of `list`, created now.
    pub async fn optout(pool: &PgPool, author: &str, rkey: &str, list: &str) -> String {
        let record = json!({"$type": "app.bsky.graph.referencelistoptout", "subject": list, "createdAt": created(None)});
        put(pool, author, "app.bsky.graph.referencelistoptout", rkey, &record).await
    }

    /// A starter pack of `list`, created at `at` (or now).
    pub async fn starter_pack_at(pool: &PgPool, owner: &str, rkey: &str, name: &str, list: &str, at: Option<DateTime<Utc>>) -> String {
        let record = json!({"$type": "app.bsky.graph.starterpack", "name": name, "list": list, "createdAt": created(at)});
        put(pool, owner, "app.bsky.graph.starterpack", rkey, &record).await
    }

    /// A starter pack, created now.
    pub async fn starter_pack(pool: &PgPool, owner: &str, rkey: &str, name: &str, list: &str) -> String {
        starter_pack_at(pool, owner, rkey, name, list, None).await
    }

    /// A threadgate at `rkey` naming `post`, with these rules: `allow`
    /// left out when `None`.
    pub async fn threadgate(pool: &PgPool, author: &str, rkey: &str, post: &str, allow: Option<Value>, hidden: &[&str]) -> String {
        let mut record = json!({"$type": "app.bsky.feed.threadgate", "post": post, "hiddenReplies": hidden, "createdAt": created(None)});
        if let Some(allow) = allow {
            record["allow"] = allow;
        }
        put(pool, author, "app.bsky.feed.threadgate", rkey, &record).await
    }

    /// A postgate at `rkey` naming `post`, with these rules and detached
    /// quotes.
    pub async fn postgate(pool: &PgPool, author: &str, rkey: &str, post: &str, rules: Option<Value>, detached: &[&str]) -> String {
        let mut record = json!({"$type": "app.bsky.feed.postgate", "post": post, "detachedEmbeddingUris": detached, "createdAt": created(None)});
        if let Some(rules) = rules {
            record["embeddingRules"] = rules;
        }
        put(pool, author, "app.bsky.feed.postgate", rkey, &record).await
    }

    /// A labeler service with this record.
    pub async fn labeler(pool: &PgPool, did: &str, record: &Value) -> String {
        put(pool, did, "app.bsky.labeler.service", "self", record).await
    }

    /// A self record of `collection` with this record.
    pub async fn self_record(pool: &PgPool, did: &str, collection: &str, record: &Value) -> String {
        put(pool, did, collection, "self", record).await
    }

    /// Set a starter pack's joined counts.
    pub async fn pack_counts(pool: &PgPool, pack: &str, week: i32, all_time: i32) {
        super::interactions::intern(pool, &[pack]).await;
        sqlx::query(
            "INSERT INTO starter_pack_counts (author, rkey, joined_week_count, joined_all_time_count, all_time_source, refreshed_at) \
             SELECT r.author, r.rkey, $2, $3, 'local', now() FROM ref_of($1) r WHERE r.coll = 11 \
             ON CONFLICT (author, rkey) DO UPDATE SET joined_week_count = EXCLUDED.joined_week_count, \
               joined_all_time_count = EXCLUDED.joined_all_time_count",
        )
        .bind(pack)
        .bind(week)
        .bind(all_time)
        .execute(pool)
        .await
        .expect("pack counts");
    }

    /// SQL: the row of the keyed `table` (alias `alias`, or none) is the
    /// one `uri` names.
    #[must_use]
    pub fn key_is(table: &str, alias: &str, uri: &str) -> String {
        let dot = if alias.is_empty() { String::new() } else { format!("{alias}.") };
        match table {
            "labeler_services" => format!("{dot}author = (SELECT r.author FROM ref_of('{uri}') r)"),
            "actor_self_records" => {
                let nsid = uri.split('/').nth(3).unwrap_or_default();
                format!(
                    "({dot}author, {dot}coll) = (SELECT d.id, c.id FROM dids d, collections c WHERE d.did = '{}' AND c.nsid = '{nsid}')",
                    crate::db::locks::uri_author(uri).unwrap_or_default()
                )
            }
            _ => format!("({dot}author, {dot}rkey) = (SELECT r.author, r.rkey FROM ref_of('{uri}') r)"),
        }
    }

    /// The collection of a keyed table's rows.
    fn nsid_of(table: &str) -> &'static str {
        match table {
            "lists" => "app.bsky.graph.list",
            "list_items" => "app.bsky.graph.listitem",
            "list_blocks" => "app.bsky.graph.listblock",
            "list_optouts" => "app.bsky.graph.referencelistoptout",
            "starter_packs" => "app.bsky.graph.starterpack",
            "threadgates" => "app.bsky.feed.threadgate",
            "postgates" => "app.bsky.feed.postgate",
            "nearhorizon_poll_posts" => "app.nearhorizon.poll.post",
            "labeler_services" => "app.bsky.labeler.service",
            "member_post_refs" | "starter_pack_counts" => "",
            other => panic!("not a keyed record table: {other}"),
        }
    }

    /// The URI of every row of the keyed `table`, ordered by text.
    pub async fn uris(pool: &PgPool, table: &str) -> Vec<String> {
        let sql = match table {
            "actor_self_records" => "SELECT 'at://' || d.did || '/' || c.nsid || '/self' FROM actor_self_records t \
                 JOIN dids d ON d.id = t.author JOIN collections c ON c.id = t.coll ORDER BY 1"
                .to_string(),
            "labeler_services" => "SELECT 'at://' || d.did || '/app.bsky.labeler.service/self' FROM labeler_services t \
                 JOIN dids d ON d.id = t.author ORDER BY 1"
                .to_string(),
            "member_post_refs" => "SELECT at_uri(t.author, 1::smallint, t.rkey) FROM member_post_refs t ORDER BY 1".to_string(),
            "starter_pack_counts" => "SELECT at_uri(t.author, 11::smallint, t.rkey) FROM starter_pack_counts t ORDER BY 1".to_string(),
            _ => format!(
                "SELECT {} FROM {table} t JOIN dids d ON d.id = t.author ORDER BY 1",
                crate::db::lists::uri_sql("d.did", nsid_of(table), "t.rkey")
            ),
        };
        sqlx::query_scalar(&sql).fetch_all(pool).await.expect("uris")
    }

    /// A stored record of a keyed table, decoded.
    pub async fn record(pool: &PgPool, table: &str, uri: &str) -> Option<Value> {
        let stored: Option<Vec<u8>> = sqlx::query_scalar(&format!("SELECT record FROM {table} WHERE {}", key_is(table, "", uri)))
            .fetch_optional(pool)
            .await
            .expect("record");
        match stored {
            Some(bytes) => Some(crate::db::records::decode_value(pool, &bytes).await.expect("decode")),
            None => None,
        }
    }

    /// A member's post reference, at `rev`, naming the accounts its post
    /// replies to (`parent`, `root`) and quotes, created at `created` (an
    /// SQL expression).
    pub async fn post_ref(pool: &PgPool, uri: &str, rev: &str, refs: (Option<&str>, Option<&str>, Option<&str>), created: &str) {
        let (parent, root, quote) = refs;
        let named: Vec<String> = [parent, root, quote].into_iter().flatten().map(str::to_string).collect();
        super::interactions::intern(pool, &[uri]).await;
        crate::db::ids::Ids::new(pool.clone()).account_ids(&named).await.expect("intern");
        sqlx::query(&format!(
            "INSERT INTO member_post_refs (author, rkey, rev, parent, root, quote, created_at) \
             SELECT r.author, r.rkey, rev_to_bigint($2), (SELECT id FROM dids WHERE did = $3), (SELECT id FROM dids WHERE did = $4), \
                    (SELECT id FROM dids WHERE did = $5), {created} \
             FROM ref_of($1) r WHERE r.coll = 1"
        ))
        .bind(uri)
        .bind(rev)
        .bind(parent)
        .bind(root)
        .bind(quote)
        .execute(pool)
        .await
        .expect("post ref");
    }

    /// Statements that intern the authority and key of each URI (SQL-only
    /// fixtures, run with `raw_sql`).
    fn intern_uris_sql(uris: &[&str]) -> String {
        uris.iter()
            .filter_map(|u| u.strip_prefix("at://"))
            .map(|rest| {
                let mut parts = rest.split('/');
                let (did, rkey) = (parts.next().unwrap_or(""), parts.nth(1).unwrap_or(""));
                super::posts::intern_sql(did, rkey)
            })
            .collect()
    }

    /// Statements that store a bare row of the keyed `table` by `author` at
    /// `rkey` (ignored for a labeler service; the collection for a self
    /// record), created now, with an empty record where the table keeps
    /// one: `refs` are its references as `(column prefix, URI)` and `extra`
    /// its other columns as `(column, SQL expression)` (SQL-only fixtures,
    /// run with `raw_sql`).
    #[must_use]
    pub fn row_sql(table: &str, author: &str, rkey: &str, refs: &[(&str, &str)], extra: &[(&str, String)]) -> String {
        let uris: Vec<&str> = refs.iter().map(|(_, u)| *u).collect();
        let mut sql = intern_uris_sql(&uris);
        sql += &super::posts::intern_sql(author, rkey);
        let mut columns: Vec<String> = vec!["author".into(), "cid".into(), "rev".into()];
        let mut values: Vec<String> = vec!["a.id".into(), format!("'\\x00'::bytea || convert_to('bafy{rkey}', 'UTF8')"), format!("rev_to_bigint('{REV}')")];
        match table {
            "labeler_services" => {}
            "actor_self_records" => {
                columns.push("coll".into());
                values.push(format!("(SELECT id FROM collections WHERE nsid = '{rkey}')"));
            }
            _ => {
                columns.push("rkey".into());
                values.push(super::posts::key_sql(author, rkey));
            }
        }
        if !matches!(table, "list_items" | "list_blocks" | "list_optouts") {
            columns.push("record".into());
            values.push("'\\x00'::bytea || convert_to('{}', 'UTF8')".into());
        }
        if !matches!(table, "actor_self_records") {
            columns.push("created_at".into());
            values.push("now()".into());
        }
        for (prefix, uri) in refs {
            columns.extend([format!("{prefix}_author"), format!("{prefix}_coll"), format!("{prefix}_rkey")]);
            values.extend([
                format!("(SELECT r.author FROM ref_of('{uri}') r)"),
                format!("COALESCE((SELECT r.coll FROM ref_of('{uri}') r), 0)"),
                format!("(SELECT r.rkey FROM ref_of('{uri}') r)"),
            ]);
        }
        for (column, value) in extra {
            columns.push((*column).to_string());
            values.push(value.clone());
        }
        sql += &format!(
            "INSERT INTO {table} ({}) SELECT {} FROM dids a WHERE a.did = '{author}';",
            columns.join(", "),
            values.join(", ")
        );
        sql
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
