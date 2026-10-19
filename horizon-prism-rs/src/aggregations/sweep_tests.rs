//! The daily scope sweep (scope-recency design §3.3 – §3.6, §8: T-R3 –
//! T-R16).

use std::sync::Arc;

use chrono::TimeZone;
use serde_json::json;

use super::*;
use crate::aggregations::ScopeSets;
use crate::config::ScopePolicy;
use crate::firehose::writer::{Action, ApplyOptions, RawOp, WriteSource, Writer};
use crate::private_tier::{DisabledPrivateTier, KryphocronPrivateTier, test_rows};
use crate::test_support::{TestDb, fresh_db, hooks};

const M: &str = "did:plc:aaaaaaaaaaaaaaaaaaaaaaaa";
const M2: &str = "did:plc:bbbbbbbbbbbbbbbbbbbbbbbb";
const A: &str = "did:plc:cccccccccccccccccccccccc";
const B: &str = "did:plc:dddddddddddddddddddddddd";
const C: &str = "did:plc:eeeeeeeeeeeeeeeeeeeeeeee";
const D: &str = "did:plc:ffffffffffffffffffffffff";
const E: &str = "did:plc:gggggggggggggggggggggggg";
const X: &str = "did:plc:hhhhhhhhhhhhhhhhhhhhhhhh";
const Y: &str = "did:plc:iiiiiiiiiiiiiiiiiiiiiiii";
const Z: &str = "did:plc:jjjjjjjjjjjjjjjjjjjjjjjj";
const N: &str = "did:plc:kkkkkkkkkkkkkkkkkkkkkkkk";

struct T {
    db: TestDb,
    scope: ScopeState,
    rebuild: Rebuild,
}

impl T {
    fn p(&self) -> &PgPool {
        &self.db.pool
    }

    /// A `scope_dids` row justified a day ago, and the DID in memory.
    async fn row(&self, did: &str, reasons: i16) {
        sqlx::query("INSERT INTO scope_dids (did, reasons, justified_at) VALUES ($1, $2, now() - interval '1 day')")
            .bind(did)
            .bind(reasons)
            .execute(self.p())
            .await
            .expect("row");
        self.scope.write_guard().await.scope.insert(did.to_string());
    }

    async fn in_table(&self, did: &str) -> bool {
        sqlx::query_scalar("SELECT EXISTS (SELECT 1 FROM scope_dids WHERE did = $1)")
            .bind(did)
            .fetch_one(self.p())
            .await
            .expect("row")
    }

    async fn reasons(&self, did: &str) -> i16 {
        sqlx::query_scalar("SELECT reasons FROM scope_dids WHERE did = $1").bind(did).fetch_one(self.p()).await.expect("reasons")
    }

    async fn in_memory(&self, did: &str) -> bool {
        self.scope.snapshot().await.in_scope(did)
    }

    async fn exec(&self, sql: &str) {
        sqlx::query(sql).execute(self.p()).await.expect(sql);
    }

    /// `author` follows `subject`, `minutes_ago` minutes ago.
    async fn follow(&self, author: &str, subject: &str, minutes_ago: i64) {
        self.exec(&format!(
            "INSERT INTO follows (uri, author_did, subject_did, rev, created_at) VALUES \
             ('at://{author}/app.bsky.graph.follow/{}', '{author}', '{subject}', '3k', now() - interval '{minutes_ago} minutes')",
            &subject[8..16]
        ))
        .await;
    }

    async fn unfollow(&self, author: &str, subject: &str) {
        self.exec(&format!("DELETE FROM follows WHERE author_did = '{author}' AND subject_did = '{subject}'")).await;
    }

    /// `author` liked a post of `subject` `days_ago` days ago.
    async fn like(&self, author: &str, subject: &str, days_ago: i64) {
        self.exec(&format!(
            "INSERT INTO likes (uri, author_did, subject_uri, rev, created_at) VALUES \
             ('at://{author}/app.bsky.feed.like/{}', '{author}', 'at://{subject}/app.bsky.feed.post/1', '3k', now() - interval '{days_ago} days')",
            &subject[8..16]
        ))
        .await;
    }

    /// `author` replied to a post of `parent` `days_ago` days ago.
    async fn reply(&self, author: &str, parent: &str, days_ago: i64) {
        self.exec(&format!(
            "INSERT INTO member_post_refs (uri, author_did, rev, reply_parent_did, reply_root_did, created_at) VALUES \
             ('at://{author}/app.bsky.feed.post/{}', '{author}', '3k', '{parent}', '{parent}', now() - interval '{days_ago} days')",
            &parent[8..16]
        ))
        .await;
    }

    async fn subscribe(&self, subscriber: &str, subject: &str) {
        self.exec(&format!(
            "INSERT INTO activity_subscriptions (subscriber_did, subject_did, post, reply) VALUES ('{subscriber}', '{subject}', true, false)"
        ))
        .await;
    }

    fn hook_key(&self) -> String {
        format!("{}/sweep", self.db.name())
    }
}

async fn setup(name: &str, policy: ScopePolicy, cap: u32, private: Arc<dyn PrivateTier>) -> Option<T> {
    let db = fresh_db(name).await?;
    let mut sets = ScopeSets::default();
    for m in [M, M2] {
        sqlx::query("INSERT INTO members (did, source) VALUES ($1, 'admin')").bind(m).execute(&db.pool).await.expect("member");
        sets.members.insert(m.into());
        sets.scope.insert(m.into());
    }
    let scope = ScopeState::new(sets, policy, cap);
    let rebuild = Rebuild::new(db.pool.clone(), scope.clone(), private, 0);
    Some(T { db, scope, rebuild })
}

async fn plain(name: &str) -> Option<T> {
    setup(name, ScopePolicy::MembersAndOneHop, 10, Arc::new(DisabledPrivateTier)).await
}

/// T-R3: only fresh interactions hold; follows and members always do; the
/// reason bits are rewritten.
#[tokio::test]
async fn the_sweep_keeps_what_its_windows_hold() {
    let Some(t) = plain("sweep_windows").await else {
        return;
    };
    t.like(M, A, 31).await;
    t.reply(M, B, 89).await;
    t.reply(M, C, 91).await;
    t.follow(M, D, 60).await;
    for did in [A, B, C, D] {
        t.row(did, REASON_INTERACTED).await;
    }
    let r = t.rebuild.sweep(false).await.expect("sweep");
    assert_eq!((r.purged, r.candidates), (2, 2), "{r:?}");
    assert!(!t.in_table(A).await && !t.in_table(C).await);
    assert!(!t.in_memory(A).await && !t.in_memory(C).await);
    assert_eq!(t.reasons(B).await, REASON_INTERACTED);
    assert_eq!(t.reasons(D).await, REASON_FOLLOWED);
    assert_eq!(t.reasons(M).await, REASON_MEMBER);
    assert!(r.skipped.is_empty(), "{r:?}");
    t.db.drop_db().await;
}

/// T-R4: an unfollow or an unlike ends the hold at the next sweep; another
/// member's follow still holds.
#[tokio::test]
async fn unfollow_and_unlike_apply_at_the_next_sweep() {
    let Some(t) = plain("sweep_unfollow").await else {
        return;
    };
    t.follow(M, X, 60).await;
    t.like(M, Y, 1).await;
    t.follow(M, Z, 60).await;
    t.follow(M2, Z, 60).await;
    for did in [X, Y, Z] {
        t.row(did, REASON_FOLLOWED).await;
    }
    assert_eq!(t.rebuild.sweep(false).await.expect("sweep").purged, 0);
    t.unfollow(M, X).await;
    t.unfollow(M, Z).await;
    t.exec("DELETE FROM likes").await;
    let r = t.rebuild.sweep(false).await.expect("sweep");
    assert_eq!(r.purged, 2, "{r:?}");
    assert!(!t.in_table(X).await && !t.in_table(Y).await && t.in_table(Z).await);
    t.db.drop_db().await;
}

fn like_op(subject: &str) -> RawOp {
    RawOp {
        action: Action::Create,
        collection: "app.bsky.feed.like".into(),
        rkey: "3klikenow".into(),
        cid: Some("bafylike".into()),
        record: Some(json!({
            "subject": {"uri": format!("at://{subject}/app.bsky.feed.post/1"), "cid": "c"},
            "createdAt": chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Millis, true),
        })),
    }
}

fn options() -> ApplyOptions {
    ApplyOptions {
        advance_applied_rev: false,
        dead_letter_readiness: false,
        attempts: 1,
        live: true,
        subscription_rules: true,
        source: WriteSource::Firehose,
    }
}

/// T-R5: a member's like of a candidate after step 2 keeps it: the marker
/// finds it touched, changes nothing, and the next sweep keeps it from the
/// table.
#[tokio::test]
async fn a_hold_written_during_the_sweep_keeps_the_candidate() {
    let Some(t) = plain("sweep_touched").await else {
        return;
    };
    t.like(M, Z, 40).await;
    t.row(Z, REASON_INTERACTED).await;
    let armed = hooks::arm(hooks::SWEEP_AFTER_COMPUTE, &t.hook_key());
    let rebuild = t.rebuild.clone();
    let run = tokio::spawn(async move { rebuild.sweep(false).await });
    armed.reached().await;
    let writer = Writer::new(t.p().clone(), t.scope.clone(), Arc::new(DisabledPrivateTier));
    writer.apply(M, "3kaaaaaaaaaab", &[like_op(Z)], options()).await.expect("like");
    armed.release();
    let r = run.await.expect("join").expect("sweep");
    assert_eq!(r.skipped.get("touched"), Some(&1), "{r:?}");
    assert_eq!(r.purged, 0);
    assert!(t.in_table(Z).await && t.in_memory(Z).await);
    drop(armed);
    let r = t.rebuild.sweep(false).await.expect("sweep");
    assert_eq!((r.candidates, r.purged), (0, 0), "{r:?}");
    assert!(t.in_table(Z).await);
    t.db.drop_db().await;
}

/// T-R6: a candidate that becomes a member before its marker stays.
#[tokio::test]
async fn a_new_member_is_not_purged() {
    let Some(t) = plain("sweep_member_race").await else {
        return;
    };
    t.row(C, REASON_INTERACTED).await;
    let armed = hooks::arm(hooks::SWEEP_AFTER_COMPUTE, &t.hook_key());
    let rebuild = t.rebuild.clone();
    let run = tokio::spawn(async move { rebuild.sweep(false).await });
    armed.reached().await;
    t.exec(&format!("INSERT INTO members (did, source) VALUES ('{C}', 'admin')")).await;
    armed.release();
    let r = run.await.expect("join").expect("sweep");
    assert_eq!(r.skipped.get("member"), Some(&1), "{r:?}");
    assert!(t.in_table(C).await);
    t.db.drop_db().await;
}

/// T-R6b: an unfollow that brings a follow under the cap after step 2
/// keeps that follow's subject (the marker re-reads the follow hold).
#[tokio::test]
async fn a_follow_brought_under_the_cap_keeps_its_subject() {
    let Some(t) = setup("sweep_cap_shift", ScopePolicy::MembersAndOneHop, 2, Arc::new(DisabledPrivateTier)).await else {
        return;
    };
    t.follow(M, A, 180).await;
    t.follow(M, B, 120).await;
    t.follow(M, C, 60).await;
    t.like(M, C, 40).await;
    t.row(C, REASON_INTERACTED).await;
    let armed = hooks::arm(hooks::SWEEP_AFTER_COMPUTE, &t.hook_key());
    let rebuild = t.rebuild.clone();
    let run = tokio::spawn(async move { rebuild.sweep(false).await });
    armed.reached().await;
    t.unfollow(M, A).await;
    armed.release();
    let r = run.await.expect("join").expect("sweep");
    assert_eq!(r.skipped.get("followed"), Some(&1), "{r:?}");
    assert!(t.in_table(C).await && t.in_memory(C).await);
    t.db.drop_db().await;
}

/// T-R6c: the first start records the time and runs nothing; the slot rule.
#[tokio::test]
async fn the_first_sweep_waits_and_late_runs_return_to_the_slot() {
    let at = |d: u32, h: u32, m: u32| chrono::Utc.with_ymd_and_hms(2026, 10, d, h, m, 0).single().expect("time");
    let slot = 3 * 3600;
    // First start before the slot: not that day's slot, the next.
    assert!(!sweep_due(at(1, 2, 30), at(1, 3, 5), slot));
    assert!(sweep_due(at(1, 2, 30), at(2, 3, 5), slot));
    // First start after the slot: the next day's slot, 23.5 hours on.
    assert!(sweep_due(at(1, 3, 30), at(2, 3, 0), slot));
    // A late run at 05:00 is followed at 04:00 (23 hours), then back at 03:00.
    assert!(!sweep_due(at(1, 5, 0), at(2, 3, 5), slot));
    assert!(sweep_due(at(1, 5, 0), at(2, 4, 0), slot));
    assert!(sweep_due(at(2, 4, 0), at(3, 3, 0), slot));
    // A run at the slot runs once a day.
    assert!(sweep_due(at(1, 3, 1), at(2, 3, 2), slot));
    assert!(!sweep_due(at(1, 3, 1), at(1, 3, 30), slot));
    assert_eq!(most_recent_slot(at(2, 2, 0), slot), at(1, 3, 0));

    let Some(t) = plain("sweep_first_start").await else {
        return;
    };
    assert!(!t.rebuild.sweep_is_due().await.expect("due"), "the first start runs nothing");
    let recorded: i64 = sqlx::query_scalar("SELECT count(*) FROM worker_state WHERE key = 'last_scope_sweep'")
        .fetch_one(t.p())
        .await
        .expect("key");
    assert_eq!(recorded, 1);
    assert!(!t.rebuild.sweep_is_due().await.expect("due"), "nor the next check");
    t.db.drop_db().await;
}

/// T-R7: a candidate under another kind of purge is skipped at once and
/// purged by the next sweep; a half-done `out_of_scope` purge is finished.
#[tokio::test]
async fn busy_candidates_are_skipped_and_stuck_purges_finished() {
    let Some(t) = setup("sweep_busy", ScopePolicy::MembersAndOneHop, 10, Arc::new(KryphocronPrivateTier)).await else {
        return;
    };
    t.row(D, REASON_INTERACTED).await;
    t.exec(&format!("INSERT INTO purging_dids (did, kind) VALUES ('{D}', 'member_removed')")).await;
    // E's out_of_scope marker committed (its row gone), its batches did not.
    t.exec(&format!("INSERT INTO purging_dids (did, kind) VALUES ('{E}', 'out_of_scope')")).await;
    test_rows::insert_post(t.p(), E, "3k", None).await;
    let r = t.rebuild.sweep(false).await.expect("sweep");
    assert_eq!(r.skipped.get("busy"), Some(&1), "{r:?}");
    assert_eq!(r.purged, 1, "the stuck purge is finished: {r:?}");
    assert!(t.in_table(D).await);
    let left: Vec<String> = sqlx::query_scalar("SELECT did FROM purging_dids").fetch_all(t.p()).await.expect("purging");
    assert_eq!(left, [D]);
    assert!(test_rows::post_rkeys(t.p()).await.is_empty());
    t.exec("DELETE FROM purging_dids").await;
    let r = t.rebuild.sweep(false).await.expect("sweep");
    assert_eq!(r.purged, 1, "{r:?}");
    assert!(!t.in_table(D).await);
    t.db.drop_db().await;
}

/// T-R8: a second sweep waits for the one running.
#[tokio::test]
async fn sweeps_run_one_at_a_time() {
    let Some(t) = plain("sweep_serial").await else {
        return;
    };
    let armed = hooks::arm(hooks::SWEEP_AFTER_COMPUTE, &t.hook_key());
    let (first, second) = (t.rebuild.clone(), t.rebuild.clone());
    let one = tokio::spawn(async move { first.sweep(false).await });
    armed.reached().await;
    let two = tokio::spawn(async move { second.sweep(false).await });
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(armed.hits(), 1, "the second sweep has not started");
    assert!(!two.is_finished());
    armed.release();
    one.await.expect("join").expect("sweep");
    two.await.expect("join").expect("sweep");
    assert_eq!(armed.hits(), 2);
    t.db.drop_db().await;
}

/// T-R9: the cap stops a sweep; the next one goes on.
#[tokio::test]
async fn the_cap_spreads_the_drop_over_sweeps() {
    let Some(t) = plain("sweep_cap").await else {
        return;
    };
    for did in [A, B, C, D, E] {
        t.row(did, REASON_INTERACTED).await;
    }
    let rebuild = t.rebuild.clone().with_max_purges(2);
    let r = rebuild.sweep(false).await.expect("sweep");
    assert_eq!((r.purged, r.skipped.get("capped").copied()), (2, Some(3)), "{r:?}");
    assert_eq!(r.sweeps_needed(2), 3);
    let r = rebuild.sweep(false).await.expect("sweep");
    assert_eq!(r.purged, 2);
    let r = rebuild.sweep(false).await.expect("sweep");
    assert_eq!(r.purged, 1);
    t.db.drop_db().await;
}

/// T-R10 (and T-17 of the public-indexing design): under the off rule a
/// member's grow-only reply holds its subjects for the reply window through
/// its reference, and no longer.
#[tokio::test]
async fn a_reply_holds_its_subjects_for_the_window_under_the_off_rule() {
    use crate::firehose::writer::OpOutcome;
    use crate::indexing::{PublicMode, PublicState};

    let Some(t) = plain("sweep_off_refs").await else {
        return;
    };
    {
        let mut s = t.scope.write_guard().await;
        t.scope.set_public(&mut s, PublicState { mode: PublicMode::Off, generation: 1, last_fill_since: None });
    }
    let writer = Writer::new(t.p().clone(), t.scope.clone(), Arc::new(DisabledPrivateTier));
    let parent = format!("at://{A}/app.bsky.feed.post/1");
    let quoted = format!("at://{B}/app.bsky.feed.post/1");
    let reply = RawOp {
        action: Action::Create,
        collection: "app.bsky.feed.post".into(),
        rkey: "3kreply".into(),
        cid: Some("bafyreply".into()),
        record: Some(json!({"text": "hi", "reply": {"root": {"uri": parent, "cid": "c"}, "parent": {"uri": parent, "cid": "c"}},
                            "embed": {"$type": "app.bsky.embed.record", "record": {"uri": quoted, "cid": "c"}}})),
    };
    let report = writer.apply(M, "3kaaaaaaaaaaa", &[reply], options()).await.expect("apply");
    assert_eq!(report.outcomes, [OpOutcome::GrowOnly]);
    assert!(t.in_memory(A).await && t.in_memory(B).await);
    t.exec("UPDATE scope_dids SET justified_at = now() - interval '30 days'").await;
    let r = t.rebuild.sweep(false).await.expect("sweep");
    assert_eq!(r.purged, 0, "{r:?}");
    assert_eq!(t.reasons(A).await, REASON_INTERACTED);
    // Past the reply window the reference holds nothing.
    t.exec("UPDATE member_post_refs SET created_at = now() - interval '91 days'").await;
    t.exec("UPDATE scope_dids SET justified_at = now() - interval '1 day'").await;
    let r = t.rebuild.sweep(false).await.expect("sweep");
    assert_eq!(r.purged, 2, "{r:?}");
    assert!(!t.in_memory(A).await && !t.in_memory(B).await);
    // The mode survives a reload of the sets.
    t.scope.reload(t.p(), &DisabledPrivateTier).await.expect("reload");
    assert_eq!(t.scope.snapshot().await.public.mode, PublicMode::Off);
    t.db.drop_db().await;
}

/// T-R11: a non-member candidate's private post goes, its private reply to
/// a member stays (class M); a member's private rows are never touched.
#[tokio::test]
async fn the_sweep_purges_private_rows_as_out_of_scope_does() {
    let Some(t) = setup("sweep_private", ScopePolicy::MembersAndOneHop, 10, Arc::new(KryphocronPrivateTier)).await else {
        return;
    };
    t.row(N, REASON_INTERACTED).await;
    let member_post = format!("at://{M}/app.bsky.feed.post/1");
    for (author, rkey, parent) in [(N, "3kpost", None), (N, "3kreply", Some(member_post.as_str())), (M, "3kmine", None)] {
        test_rows::insert_post(t.p(), author, rkey, parent).await;
    }
    let r = t.rebuild.sweep(false).await.expect("sweep");
    assert_eq!(r.purged, 1, "{r:?}");
    assert_eq!(test_rows::post_rkeys(t.p()).await, ["3kmine", "3kreply"]);
    t.db.drop_db().await;
}

/// T-R12: a dry run writes nothing, clears nothing, and reports the drop
/// by the bits the candidates last had.
#[tokio::test]
async fn a_dry_run_writes_nothing() {
    let Some(t) = plain("sweep_dry_run").await else {
        return;
    };
    t.row(X, REASON_INTERACTED).await;
    t.row(Y, REASON_FOLLOWED).await;
    t.row(Z, REASON_INTERACTED).await;
    t.like(M, Z, 1).await;
    t.scope.touch(&[A.to_string()]);
    let snapshot = "SELECT string_agg(did || ':' || reasons || ':' || justified_at, ',' ORDER BY did) FROM scope_dids";
    let before: Option<String> = sqlx::query_scalar(snapshot).fetch_one(t.p()).await.expect("snapshot");
    let r = t.rebuild.sweep(true).await.expect("dry run");
    assert_eq!((r.candidates, r.purged, r.added), (2, 0, 2), "{r:?}");
    assert_eq!(r.would_purge_by_reasons, BTreeMap::from([(REASON_FOLLOWED, 1), (REASON_INTERACTED, 1)]));
    assert_eq!(r.sweeps_needed(5000), 1);
    let after: Option<String> = sqlx::query_scalar(snapshot).fetch_one(t.p()).await.expect("snapshot");
    assert_eq!(before, after);
    assert!(t.scope.is_touched(A), "a dry run clears nothing");
    let jobs: i64 = sqlx::query_scalar("SELECT count(*) FROM backfill_jobs").fetch_one(t.p()).await.expect("jobs");
    assert_eq!(jobs, 0);
    t.db.drop_db().await;
}

/// T-R14: an active subscription holds its subject, while its subscriber is
/// a member and follows the subject; an unfollow ends it, and retention
/// then deletes the subscription.
#[tokio::test]
async fn an_active_subscription_holds_its_subject() {
    let Some(t) = setup("sweep_subscription_hold", ScopePolicy::MembersAndInteractions, 10, Arc::new(DisabledPrivateTier)).await else {
        return;
    };
    t.like(M, X, 40).await;
    t.follow(M, X, 60).await;
    t.subscribe(M, X).await;
    t.row(X, REASON_INTERACTED).await;
    // A non-member's subscription holds nothing.
    t.follow(N, Y, 60).await;
    t.subscribe(N, Y).await;
    t.row(Y, REASON_INTERACTED).await;
    let r = t.rebuild.sweep(false).await.expect("sweep");
    assert_eq!(r.purged, 1, "{r:?}");
    assert_eq!(t.reasons(X).await, REASON_SUBSCRIBED);
    assert!(!t.in_table(Y).await);
    t.unfollow(M, X).await;
    let r = t.rebuild.sweep(false).await.expect("sweep");
    assert_eq!(r.purged, 1, "{r:?}");
    assert!(!t.in_table(X).await && !t.in_memory(X).await);
    assert_eq!(crate::db::retention::sweep_subscriptions(t.p()).await.expect("retention"), 2);
    t.db.drop_db().await;
}

/// T-R14: under `members_only` a subscription holds nothing.
#[tokio::test]
async fn a_subscription_holds_nothing_under_members_only() {
    let Some(t) = setup("sweep_subscription_members_only", ScopePolicy::MembersOnly, 10, Arc::new(DisabledPrivateTier)).await else {
        return;
    };
    t.follow(M, X, 60).await;
    t.subscribe(M, X).await;
    t.row(X, REASON_FOLLOWED).await;
    let r = t.rebuild.sweep(false).await.expect("sweep");
    assert_eq!(r.purged, 1, "{r:?}");
    assert!(!t.in_table(X).await);
    t.db.drop_db().await;
}

/// T-R15: a subscription never adds its subject to S.
#[tokio::test]
async fn a_subscription_never_adds_its_subject() {
    let Some(t) = setup("sweep_subscription_no_growth", ScopePolicy::MembersAndInteractions, 10, Arc::new(DisabledPrivateTier)).await else {
        return;
    };
    t.follow(M, Y, 60).await;
    t.subscribe(M, Y).await;
    let r = t.rebuild.sweep(false).await.expect("sweep");
    assert_eq!(r.added, 2, "the two members' rows only: {r:?}");
    assert!(!t.in_table(Y).await && !t.in_memory(Y).await);
    let jobs: i64 = sqlx::query_scalar("SELECT count(*) FROM backfill_jobs WHERE did = $1").bind(Y).fetch_one(t.p()).await.expect("jobs");
    assert_eq!(jobs, 0);
    assert_eq!(crate::db::retention::sweep_subscriptions(t.p()).await.expect("retention"), 1);
    t.db.drop_db().await;
}

/// T-R16: a subscription made after step 2 keeps its subject: the marker
/// re-reads the subscription hold.
#[tokio::test]
async fn a_subscription_made_during_the_sweep_keeps_its_subject() {
    let Some(t) = setup("sweep_subscription_race", ScopePolicy::MembersAndInteractions, 10, Arc::new(DisabledPrivateTier)).await else {
        return;
    };
    t.like(M, X, 40).await;
    t.follow(M, X, 60).await;
    t.row(X, REASON_INTERACTED).await;
    let armed = hooks::arm(hooks::SWEEP_AFTER_COMPUTE, &t.hook_key());
    let rebuild = t.rebuild.clone();
    let run = tokio::spawn(async move { rebuild.sweep(false).await });
    armed.reached().await;
    t.subscribe(M, X).await;
    armed.release();
    let r = run.await.expect("join").expect("sweep");
    assert_eq!(r.skipped.get("subscribed"), Some(&1), "{r:?}");
    assert!(t.in_table(X).await && t.in_memory(X).await);
    t.db.drop_db().await;
}
