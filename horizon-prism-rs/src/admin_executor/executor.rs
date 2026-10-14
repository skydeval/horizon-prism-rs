//! The admin executor (design-v2 §3.3): `LISTEN admin_commands`, plus a 5 s
//! poll so no notification can be missed. Queued commands run one at a time
//! in `created_at` order.
//!
//! - At startup, rows left `running` by a crash become `failed` with
//!   `{"error":"interrupted"}`.
//! - A command whose DID is being purged is deferred (§6.11.7): it stays
//!   `queued`, as does every later command naming the same DID; other DIDs'
//!   commands proceed, so per-DID order is preserved and one purge never
//!   blocks the queue.

use std::collections::HashSet;
use std::time::Duration;

use serde_json::{Value, json};
use sqlx::PgPool;
use sqlx::postgres::PgListener;
use tokio::task::JoinHandle;

use super::commands::{AdminCommand, Handlers, Outcome};

/// The NOTIFY channel the CLI signals on.
pub const CHANNEL: &str = "admin_commands";
/// Safety-net poll interval.
pub const POLL: Duration = Duration::from_secs(5);

/// One queued row.
#[derive(Debug, Clone, sqlx::FromRow)]
struct Queued {
    id: i64,
    command: String,
    args: Value,
}

/// The executor.
pub struct AdminExecutor {
    pool: PgPool,
    handlers: Handlers,
}

impl AdminExecutor {
    #[must_use]
    pub fn new(handlers: Handlers) -> Self {
        Self {
            pool: handlers.pool.clone(),
            handlers,
        }
    }

    /// Mark rows left `running` by a crash as failed.
    ///
    /// # Errors
    ///
    /// Database errors.
    pub async fn recover(&self) -> Result<u64, sqlx::Error> {
        let n = sqlx::query(
            "UPDATE admin_commands SET state = 'failed', result = '{\"error\":\"interrupted\"}'::jsonb, \
             finished_at = now() WHERE state = 'running'",
        )
        .execute(&self.pool)
        .await?
        .rows_affected();
        if n > 0 {
            tracing::warn!(event = "admin_commands_interrupted", commands = n, "commands left running were marked failed");
        }
        Ok(n)
    }

    /// Run every runnable queued command once, in order. Returns the number
    /// executed (deferred commands are not counted).
    ///
    /// # Errors
    ///
    /// Database errors reading or settling the queue.
    pub async fn drain(&self) -> Result<usize, sqlx::Error> {
        let queued: Vec<Queued> = sqlx::query_as(
            "SELECT id, command, args FROM admin_commands WHERE state = 'queued' ORDER BY created_at, id",
        )
        .fetch_all(&self.pool)
        .await?;
        let mut blocked: HashSet<String> = HashSet::new();
        let mut executed = 0;
        for row in queued {
            let cmd = match AdminCommand::parse(&row.command, &row.args) {
                Ok(c) => c,
                Err(e) => {
                    self.settle(row.id, "failed", &json!({ "error": e.to_string() })).await?;
                    continue;
                }
            };
            if let Some(did) = cmd.did()
                && blocked.contains(did)
            {
                continue;
            }
            // Claim it; a concurrent executor (there is one per serve) would
            // lose here.
            let claimed = sqlx::query("UPDATE admin_commands SET state = 'running' WHERE id = $1 AND state = 'queued'")
                .bind(row.id)
                .execute(&self.pool)
                .await?
                .rows_affected();
            if claimed == 0 {
                continue;
            }
            tracing::info!(event = "admin_command_started", id = row.id, command = %row.command, "admin command started");
            match self.handlers.execute(&cmd).await {
                Outcome::Done(result) => {
                    self.settle(row.id, "done", &result).await?;
                    executed += 1;
                }
                Outcome::Failed(error) => {
                    self.settle(row.id, "failed", &json!({ "error": error })).await?;
                    executed += 1;
                }
                Outcome::Defer => {
                    sqlx::query("UPDATE admin_commands SET state = 'queued' WHERE id = $1")
                        .bind(row.id)
                        .execute(&self.pool)
                        .await?;
                    if let Some(did) = cmd.did() {
                        blocked.insert(did.to_string());
                    }
                    tracing::info!(event = "admin_command_deferred", id = row.id, command = %row.command, "DID is being purged; command deferred");
                }
            }
        }
        Ok(executed)
    }

    async fn settle(&self, id: i64, state: &str, result: &Value) -> Result<(), sqlx::Error> {
        sqlx::query("UPDATE admin_commands SET state = $2, result = $3, finished_at = now() WHERE id = $1")
            .bind(id)
            .bind(state)
            .bind(result)
            .execute(&self.pool)
            .await?;
        tracing::info!(event = "admin_command_finished", id, state, "admin command finished");
        Ok(())
    }

    /// Recover, then listen and poll forever.
    #[must_use]
    pub fn spawn(self) -> JoinHandle<()> {
        tokio::spawn(async move {
            if let Err(error) = self.recover().await {
                tracing::error!(event = "admin_recover_failed", %error, "marking interrupted commands failed");
            }
            let mut listener = match PgListener::connect_with(&self.pool).await {
                Ok(mut l) => match l.listen(CHANNEL).await {
                    Ok(()) => Some(l),
                    Err(error) => {
                        tracing::warn!(event = "admin_listen_failed", %error, "LISTEN failed; polling only");
                        None
                    }
                },
                Err(error) => {
                    tracing::warn!(event = "admin_listen_failed", %error, "LISTEN connection failed; polling only");
                    None
                }
            };
            loop {
                if let Err(error) = self.drain().await {
                    tracing::warn!(event = "admin_drain_failed", %error, "admin command queue pass failed");
                }
                match listener.as_mut() {
                    Some(l) => {
                        tokio::select! {
                            n = l.recv() => {
                                if let Err(error) = n {
                                    tracing::debug!(event = "admin_listen_error", %error, "listener error; it reconnects");
                                }
                            }
                            () = tokio::time::sleep(POLL) => {}
                        }
                    }
                    None => tokio::time::sleep(POLL).await,
                }
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use async_trait::async_trait;
    use redis::aio::ConnectionManager;

    use super::*;
    use crate::aggregations::{PurgeKind, Purger, Rebuild, ScopeSets, ScopeState};
    use crate::cli::dispatch::{Finished, submit, wait};
    use crate::config::ScopePolicy;
    use crate::firehose::backfill::{BackfillSource, ListPage, ListedRecord};
    use crate::firehose::jobs::Depth;
    use crate::firehose::writer::Writer;
    use crate::microcosm::LinksPage;
    use crate::private_tier::{DisabledPrivateTier, PrivateTier};
    use crate::test_support::{TestDb, fresh_db};

    const PURGING: &str = "did:plc:aaaaaaaaaaaaaaaaaaaaaaaa";
    const OTHER: &str = "did:plc:bbbbbbbbbbbbbbbbbbbbbbbb";

    struct NoNetwork;

    #[async_trait]
    impl BackfillSource for NoNetwork {
        async fn prepare(&self, _: &str) -> Result<String, crate::firehose::backfill::PrepareError> {
            Err(crate::firehose::backfill::PrepareError::Failed("no network in tests".into()))
        }
        async fn latest_rev(&self, _: &str, _: &str) -> Result<String, String> {
            Err("no network in tests".into())
        }
        async fn list_records(&self, _: &str, _: &str, _: &str, _: Option<&str>) -> Result<ListPage, String> {
            Err("no network in tests".into())
        }
        async fn get_record(&self, _: &str, _: &str, _: &str, _: &str) -> Result<Option<ListedRecord>, String> {
            Err("no network in tests".into())
        }
        async fn inbound_blocks(&self, _: &str, _: Option<&str>) -> Result<LinksPage, String> {
            Err("no network in tests".into())
        }
    }

    async fn setup(name: &str, sets: ScopeSets) -> Option<(TestDb, AdminExecutor, ScopeState)> {
        setup_with(name, sets, Arc::new(DisabledPrivateTier), Arc::new(NoNetwork), &test_stream()).await
    }

    fn test_stream() -> String {
        format!("test:dl:{}", rand::random::<u32>())
    }

    async fn setup_with(
        name: &str,
        sets: ScopeSets,
        private: Arc<dyn PrivateTier>,
        source: Arc<dyn BackfillSource>,
        dead_letter_stream: &str,
    ) -> Option<(TestDb, AdminExecutor, ScopeState)> {
        let Ok(redis_url) = std::env::var("HPRS_TEST_REDIS_URL") else {
            eprintln!("HPRS_TEST_REDIS_URL unset; skipping executor test");
            return None;
        };
        let db = fresh_db(name).await?;
        let redis = ConnectionManager::new(redis::Client::open(redis_url).expect("url"))
            .await
            .expect("redis");
        let scope = ScopeState::new(sets, ScopePolicy::MembersAndOneHop, 10_000);
        let handlers = Handlers {
            pool: db.pool.clone(),
            scope: scope.clone(),
            private: Arc::clone(&private),
            writer: Writer::new(db.pool.clone(), scope.clone(), Arc::clone(&private)),
            purger: Purger::new(db.pool.clone(), scope.clone(), Arc::clone(&private)),
            rebuild: Rebuild::new(db.pool.clone(), scope.clone(), Arc::clone(&private), 7),
            redis,
            source,
            dead_letter_stream: dead_letter_stream.to_string(),
            deterministic_attempts: 5,
            community_patterns: Arc::new(vec!["*.community.example".to_string()]),
            farsight: None,
            sync_min_interval_secs: 600,
            list_budget: crate::subscriptions::references::Budget { max_references: 25, max_owners: 10 },
            public: crate::indexing::PublicIndexing::new(
                db.pool.clone(),
                scope.clone(),
                crate::indexing::PublicModeSettings::from_config(&crate::config::IndexingConfig::default()),
            ),
        };
        Some((db, AdminExecutor::new(handlers), scope))
    }

    /// A source whose `prepare` confirms a community PDS and records who
    /// asked.
    #[derive(Default)]
    struct CommunityPds {
        asked: std::sync::Mutex<Vec<String>>,
    }

    #[async_trait]
    impl BackfillSource for CommunityPds {
        async fn prepare(&self, did: &str) -> Result<String, crate::firehose::backfill::PrepareError> {
            self.asked.lock().expect("lock").push(did.to_string());
            Ok("https://pds1.community.example".into())
        }
        async fn latest_rev(&self, _: &str, _: &str) -> Result<String, String> {
            Err("unused".into())
        }
        async fn list_records(&self, _: &str, _: &str, _: &str, _: Option<&str>) -> Result<ListPage, String> {
            Err("unused".into())
        }
        async fn get_record(&self, _: &str, _: &str, _: &str, _: &str) -> Result<Option<ListedRecord>, String> {
            Err("unused".into())
        }
        async fn inbound_blocks(&self, _: &str, _: Option<&str>) -> Result<LinksPage, String> {
            Err("unused".into())
        }
    }

    #[tokio::test]
    async fn member_remove_reconfirms_only_stale_eligibility() {
        let (stale, fresh) = (PURGING, OTHER);
        let mut sets = ScopeSets::default();
        for d in [stale, fresh] {
            sets.members.insert(d.into());
            sets.scope.insert(d.into());
            sets.private_owners.insert(d.into());
        }
        let private: Arc<dyn PrivateTier> = Arc::new(crate::private_tier::KryphocronPrivateTier);
        let source = Arc::new(CommunityPds::default());
        let Some((db, exec, scope)) =
            setup_with("admin_reconfirm", sets, Arc::clone(&private), Arc::clone(&source) as Arc<dyn BackfillSource>, &test_stream())
                .await
        else {
            return;
        };
        let p = &db.pool;
        for d in [stale, fresh] {
            sqlx::query("INSERT INTO members (did, source, ready_at) VALUES ($1, 'admin', now())")
                .bind(d)
                .execute(p)
                .await
                .expect("member");
            let mut conn = p.acquire().await.expect("conn");
            let nsid = crate::firehose::classifier::NSID_POST_PRIVATE;
            private
                .apply(
                    &mut conn,
                    &crate::private_tier::PrivateOp {
                        uri: format!("at://{d}/{nsid}/3kpost222222"),
                        author_did: d.into(),
                        nsid: nsid.into(),
                        rkey: "3kpost222222".into(),
                        cid: "bafy".into(),
                        rev: "3kaaaaaaaaaa2".into(),
                        record: json!({"text": "t"}),
                    },
                )
                .await
                .expect("private row");
        }
        // `fresh` was confirmed an hour ago; `stale` never was.
        {
            let mut conn = p.acquire().await.expect("conn");
            private.record_eligibility(&mut conn, fresh, true).await.expect("confirm");
        }
        for d in [stale, fresh] {
            let id = submit(p, &AdminCommand::MemberRemove { did: d.into() }).await.expect("submit");
            exec.drain().await.expect("drain");
            match wait(p, id, Duration::from_secs(5), Duration::from_millis(10)).await.expect("wait") {
                Finished::Done(result) => {
                    let expected = if d == stale { "confirmed" } else { "fresh" };
                    assert_eq!(result["eligibility"], expected, "{d}");
                }
                other => panic!("expected done, got {other:?}"),
            }
        }
        assert_eq!(*source.asked.lock().expect("lock"), vec![stale.to_string()], "only the stale DID is re-confirmed");
        let mut conn = p.acquire().await.expect("conn");
        assert!(private.eligibility_confirmed_at(&mut conn, stale).await.expect("read").is_some());
        drop(conn);
        assert!(scope.read_guard().await.is_owner(stale), "an eligible owner stays an owner");
        db.drop_db().await;
    }

    /// A PDS serving one post (`…/keep`) and nothing else, at a rev newer
    /// than any test row.
    struct ReplayPds;

    #[async_trait]
    impl BackfillSource for ReplayPds {
        async fn prepare(&self, _: &str) -> Result<String, crate::firehose::backfill::PrepareError> {
            Ok("https://pds.example.com".into())
        }
        async fn latest_rev(&self, _: &str, _: &str) -> Result<String, String> {
            Ok("3kzzzzzzzzzz2".into())
        }
        async fn list_records(&self, _: &str, _: &str, _: &str, _: Option<&str>) -> Result<ListPage, String> {
            Err("unused".into())
        }
        async fn get_record(&self, _: &str, did: &str, collection: &str, rkey: &str) -> Result<Option<ListedRecord>, String> {
            Ok((rkey == "keep").then(|| ListedRecord {
                uri: format!("at://{did}/{collection}/{rkey}"),
                cid: "bafykeep".into(),
                value: json!({"$type": "app.bsky.feed.post", "text": "replayed", "createdAt": "2026-09-26T00:00:00Z"}),
            }))
        }
        async fn inbound_blocks(&self, _: &str, _: Option<&str>) -> Result<LinksPage, String> {
            Err("unused".into())
        }
    }

    async fn run(exec: &AdminExecutor, pool: &PgPool, cmd: AdminCommand) -> Finished {
        let id = submit(pool, &cmd).await.expect("submit");
        exec.drain().await.expect("drain");
        wait(pool, id, Duration::from_secs(10), Duration::from_millis(10)).await.expect("wait")
    }

    fn done(f: Finished) -> Value {
        match f {
            Finished::Done(v) => v,
            other => panic!("expected done, got {other:?}"),
        }
    }

    async fn count(pool: &PgPool, sql: &str) -> i64 {
        sqlx::query_scalar(sql).fetch_one(pool).await.expect(sql)
    }

    #[tokio::test]
    async fn mutating_commands_through_the_queue() {
        const MEMBER: &str = PURGING;
        const SCOPED: &str = OTHER;
        let mut sets = ScopeSets::default();
        sets.members.insert(MEMBER.into());
        for d in [MEMBER, SCOPED] {
            sets.scope.insert(d.into());
        }
        let stream = test_stream();
        let Some((db, exec, _scope)) =
            setup_with("admin_mutating", sets, Arc::new(DisabledPrivateTier), Arc::new(ReplayPds), &stream).await
        else {
            return;
        };
        let p = &db.pool;
        sqlx::query("INSERT INTO members (did, source, ready_at) VALUES ($1, 'admin', now())")
            .bind(MEMBER)
            .execute(p)
            .await
            .expect("member");
        for d in [MEMBER, SCOPED] {
            sqlx::query("INSERT INTO scope_dids (did, reasons) VALUES ($1, 1)").bind(d).execute(p).await.expect("scope");
        }

        // backfill: default depth member for a member, scope otherwise;
        // an explicit depth wins.
        for (did, depth, expected) in [(MEMBER, None, "member"), (SCOPED, None, "scope"), (SCOPED, Some(Depth::Authz), "authz")] {
            let r = done(run(&exec, p, AdminCommand::Backfill { did: did.into(), depth }).await);
            assert_eq!(r["jobs"][0]["depth"], expected, "{did}");
            let queued: i64 = sqlx::query_scalar("SELECT count(*) FROM backfill_jobs WHERE did = $1 AND depth = $2 AND state = 'queued'")
                .bind(did)
                .bind(expected)
                .fetch_one(p)
                .await
                .expect("job");
            assert_eq!(queued, 1);
        }

        // scope rebuild --dry-run: counts, and nothing changes.
        let snapshot = "SELECT (SELECT count(*) FROM scope_dids) * 1000000 + (SELECT count(*) FROM purging_dids) * 1000                         + (SELECT count(*) FROM backfill_jobs)";
        let before = count(p, snapshot).await;
        let r = done(run(&exec, p, AdminCommand::ScopeRebuild { dry_run: true }).await);
        assert_eq!(r["dry_run"], true);
        for k in ["justified", "would_add", "would_purge"] {
            assert!(r[k].is_number(), "{k} in {r}");
        }
        assert_eq!(count(p, snapshot).await, before, "a dry run has no side effects");

        // resync: a gap is recorded, member readiness cleared, authz for
        // tracked DIDs and resync for members enqueued.
        let epoch_before = {
            let mut conn = p.acquire().await.expect("conn");
            crate::firehose::gap::current_epoch(&mut conn).await.expect("epoch")
        };
        let r = done(run(&exec, p, AdminCommand::Resync).await);
        assert_eq!(r["gap_epoch"], epoch_before + 1);
        assert_eq!(count(p, "SELECT count(*) FROM members WHERE ready_at IS NOT NULL").await, 0);
        let jobs: Vec<String> = sqlx::query_scalar("SELECT depth FROM backfill_jobs WHERE did = $1 AND state = 'queued' ORDER BY depth")
            .bind(MEMBER)
            .fetch_all(p)
            .await
            .expect("jobs");
        assert!(jobs.contains(&"authz".to_string()) && jobs.contains(&"resync".to_string()), "{jobs:?}");

        // deadletter replay: the record is re-fetched and admitted; an
        // absent record goes through the delete path.
        let gone = crate::hydration::testing::post(p, MEMBER, "gone", "old", None, None).await;
        let mut redis = ConnectionManager::new(redis::Client::open(std::env::var("HPRS_TEST_REDIS_URL").expect("url")).expect("client"))
            .await
            .expect("redis");
        let add = |path: &str| redis::cmd("XADD").arg(&stream).arg("*").arg("repo").arg(MEMBER).arg("path").arg(path).clone();
        let keep_id: String = add("app.bsky.feed.post/keep").query_async(&mut redis).await.expect("xadd");
        let gone_id: String = add("app.bsky.feed.post/gone").query_async(&mut redis).await.expect("xadd");
        let r = done(run(&exec, p, AdminCommand::DeadletterReplay { id: keep_id.clone() }).await);
        assert_eq!(r["status"], "indexed", "{r}");
        let text: String = sqlx::query_scalar("SELECT text FROM posts WHERE uri = $1")
            .bind(format!("at://{MEMBER}/app.bsky.feed.post/keep"))
            .fetch_one(p)
            .await
            .expect("replayed post");
        assert_eq!(text, "replayed");
        let r = done(run(&exec, p, AdminCommand::DeadletterReplay { id: gone_id.clone() }).await);
        assert_eq!(r["status"], "deleted", "{r}");
        let left: i64 = sqlx::query_scalar("SELECT count(*) FROM posts WHERE uri = $1").bind(&gone).fetch_one(p).await.expect("count");
        assert_eq!(left, 0);
        // Replay leaves the entry listed (§5.9); drop removes it.
        let len: i64 = redis::cmd("XLEN").arg(&stream).query_async(&mut redis).await.expect("xlen");
        assert_eq!(len, 2);
        let r = done(run(&exec, p, AdminCommand::DeadletterDrop { id: keep_id.clone() }).await);
        assert_eq!(r["dropped"], true);
        let remaining: Vec<(String, Vec<(String, String)>)> =
            redis::cmd("XRANGE").arg(&stream).arg("-").arg("+").query_async(&mut redis).await.expect("xrange");
        assert_eq!(remaining.iter().map(|(id, _)| id.clone()).collect::<Vec<_>>(), [gone_id]);
        assert!(matches!(run(&exec, p, AdminCommand::DeadletterDrop { id: keep_id }).await, Finished::Failed(_)));
        let _: i64 = redis::cmd("DEL").arg(&stream).query_async(&mut redis).await.expect("del");
        db.drop_db().await;
    }

    /// `indexing public-scope` and `indexing status` go through the queue:
    /// the command changes the phase and spawns the sweep, which ends Off.
    #[tokio::test]
    async fn indexing_commands_through_the_queue() {
        use crate::indexing::ScopeValue;

        let Some((db, exec, scope)) = setup("admin_indexing", ScopeSets::default()).await else {
            return;
        };
        let p = &db.pool;
        let r = done(run(&exec, p, AdminCommand::IndexingPublicScope { value: ScopeValue::Off }).await);
        assert_eq!((&r["action"], &r["mode"], &r["generation"], &r["task"]), (&json!("draining"), &json!("draining"), &json!(1), &json!("sweep")));
        let r = done(run(&exec, p, AdminCommand::IndexingStatus).await);
        assert_eq!(r["value"], "off");
        assert_eq!(r["updated_by"], "cli");
        // The empty database's sweep ends by itself.
        let deadline = std::time::Instant::now() + Duration::from_secs(20);
        while scope.public_mode() != crate::indexing::PublicMode::Off {
            assert!(std::time::Instant::now() < deadline, "the sweep did not end");
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        let r = done(run(&exec, p, AdminCommand::IndexingStatus).await);
        assert_eq!((&r["mode"], &r["phase"], &r["generation"], &r["error"]), (&json!("off"), &json!("off"), &json!(2), &Value::Null));
        let r = done(run(&exec, p, AdminCommand::IndexingPublicScope { value: ScopeValue::Off }).await);
        assert_eq!((&r["action"], &r["task"]), (&json!("no_op"), &Value::Null));
        db.drop_db().await;
    }

    async fn state(pool: &PgPool, id: i64) -> String {
        sqlx::query_scalar("SELECT state FROM admin_commands WHERE id = $1")
            .bind(id)
            .fetch_one(pool)
            .await
            .expect("state")
    }

    #[tokio::test]
    async fn executes_a_queued_command_and_recovers_interrupted_ones() {
        let Some((db, exec, _)) = setup("admin_executes", ScopeSets::default()).await else {
            return;
        };
        let p = &db.pool;
        sqlx::query("INSERT INTO admin_commands (command, args, state) VALUES ('label add', '{}'::jsonb, 'running')")
            .execute(p)
            .await
            .expect("stale row");
        assert_eq!(exec.recover().await.expect("recover"), 1);
        let interrupted: Value = sqlx::query_scalar("SELECT result FROM admin_commands WHERE state = 'failed'")
            .fetch_one(p)
            .await
            .expect("failed row");
        assert_eq!(interrupted, json!({"error": "interrupted"}));

        let id = submit(
            p,
            &AdminCommand::LabelAdd {
                subject: OTHER.into(),
                val: "!takedown".into(),
            },
        )
        .await
        .expect("submit");
        assert_eq!(exec.drain().await.expect("drain"), 1);
        match wait(p, id, Duration::from_secs(1), Duration::from_millis(10)).await.expect("wait") {
            Finished::Done(result) => assert_eq!(result["added"], true),
            other => panic!("expected done, got {other:?}"),
        }
        let labels: i64 = sqlx::query_scalar("SELECT count(*) FROM labels WHERE subject = $1 AND val = '!takedown'")
            .bind(OTHER)
            .fetch_one(p)
            .await
            .expect("labels");
        assert_eq!(labels, 1);

        // An invalid stored command fails instead of wedging the queue.
        sqlx::query("INSERT INTO admin_commands (command, args) VALUES ('label add', '{\"subject\":\"x\",\"val\":\"spam\"}'::jsonb)")
            .execute(p)
            .await
            .expect("bad row");
        exec.drain().await.expect("drain");
        let failed: i64 = sqlx::query_scalar("SELECT count(*) FROM admin_commands WHERE state = 'failed'")
            .fetch_one(p)
            .await
            .expect("count");
        assert_eq!(failed, 2);
        db.drop_db().await;
    }

    /// `lists sync`: a requested, high-priority sync for a subscribed
    /// list's owner; a list nobody references fails.
    #[tokio::test]
    async fn lists_sync_requests_a_run_for_a_subscribed_list() {
        use crate::subscriptions::list_phase;
        use crate::subscriptions::references::{Budget, mute_list};

        let Some((db, exec, _scope)) = setup("admin_lists_sync", ScopeSets::default()).await else {
            return;
        };
        let p = &db.pool;
        let list = format!("at://{OTHER}/app.bsky.graph.list/3klist1");
        let unknown = format!("at://{OTHER}/app.bsky.graph.list/3klist2");
        assert!(matches!(run(&exec, p, AdminCommand::ListsSync { uri: list.clone() }).await, Finished::Failed(_)));
        mute_list(p, PURGING, &list, Budget { max_references: 5, max_owners: 5 }).await.expect("mute");
        list_phase::apply(p, &list, 600).await.expect("apply");
        sqlx::query("UPDATE backfill_jobs SET state = 'done', priority = 1").execute(p).await.expect("done");

        let result = done(run(&exec, p, AdminCommand::ListsSync { uri: list }).await);
        assert_eq!(result["owner"], OTHER);
        assert_eq!(count(p, "SELECT count(*) FROM backfill_jobs WHERE depth = 'list_sync' AND state = 'queued' AND priority = 0").await, 1);
        assert_eq!(count(p, "SELECT count(*) FROM subscribed_owners WHERE resync_requested_at IS NOT NULL").await, 1);
        assert!(matches!(run(&exec, p, AdminCommand::ListsSync { uri: unknown }).await, Finished::Failed(_)));

        // `lists purge`: the member's list mutes and references go, and
        // the list it referenced is marked for the list phase.
        sqlx::query("DELETE FROM list_reference_changes").execute(p).await.expect("markers");
        let result = done(run(&exec, p, AdminCommand::ListsPurge { did: PURGING.into() }).await);
        assert_eq!(result["deleted"]["list_mutes"], 1);
        assert_eq!(count(p, "SELECT count(*) FROM list_references").await, 0);
        assert_eq!(count(p, "SELECT count(*) FROM list_reference_changes").await, 1);
        db.drop_db().await;
    }

    #[tokio::test]
    async fn member_add_is_deferred_while_its_did_is_purged() {
        let mut sets = ScopeSets::default();
        sets.purging.insert(PURGING.into(), PurgeKind::OutOfScope);
        let Some((db, exec, scope)) = setup("admin_defers", sets).await else {
            return;
        };
        let p = &db.pool;
        sqlx::query("INSERT INTO purging_dids (did, kind) VALUES ($1, 'out_of_scope')")
            .bind(PURGING)
            .execute(p)
            .await
            .expect("purging");
        let add = submit(p, &AdminCommand::MemberAdd { did: PURGING.into() }).await.expect("submit");
        let later_same_did = submit(
            p,
            &AdminCommand::LabelAdd {
                subject: PURGING.into(),
                val: "porn".into(),
            },
        )
        .await
        .expect("submit");
        let other_did = submit(p, &AdminCommand::MemberAdd { did: OTHER.into() }).await.expect("submit");

        assert_eq!(exec.drain().await.expect("drain"), 1);
        assert_eq!(state(p, add).await, "queued");
        assert_eq!(state(p, later_same_did).await, "queued", "per-DID order is preserved");
        assert_eq!(state(p, other_did).await, "done", "other DIDs proceed");
        assert!(scope.snapshot().await.is_member(OTHER));

        // Purge ends: both run, in order.
        sqlx::query("DELETE FROM purging_dids").execute(p).await.expect("end");
        scope.write_guard().await.purging.clear();
        assert_eq!(exec.drain().await.expect("drain"), 2);
        assert_eq!(state(p, add).await, "done");
        assert_eq!(state(p, later_same_did).await, "done");
        let snap = scope.snapshot().await;
        assert!(snap.is_member(PURGING) && snap.in_scope(PURGING));
        let (reasons, job): (i16, String) = sqlx::query_as(
            "SELECT s.reasons, j.depth FROM scope_dids s JOIN backfill_jobs j ON j.did = s.did WHERE s.did = $1",
        )
        .bind(PURGING)
        .fetch_one(p)
        .await
        .expect("scope + job");
        assert_eq!((reasons, job.as_str()), (crate::aggregations::REASON_MEMBER, "member"));
        db.drop_db().await;
    }
}
