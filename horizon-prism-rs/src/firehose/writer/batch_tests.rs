//! Batched writer transactions and deferred counts: a batch's counts equal
//! the same commits' one transaction at a time; a deferring transaction
//! that does not apply its counts still counts them at commit; two
//! transactions that touch hot rows in opposite orders do not deadlock once
//! counts are deferred (and do without); a commit that can change the scope
//! sets runs alone; a failure inside a batch commits none of it.

use std::sync::Arc;
use std::time::Duration;

use serde_json::{Value, json};
use sqlx::PgPool;

use super::*;
use crate::config::ScopePolicy;
use crate::indexing::{PublicMode, PublicState};
use crate::test_support::{fresh_db, hooks};

const A: &str = "did:plc:batchaaaaaaaaaaaaaaaaaaa";
const B: &str = "did:plc:batchbbbbbbbbbbbbbbbbbbb";
const C: &str = "did:plc:batchccccccccccccccccccc";
const MEMBER: &str = "did:plc:batchmemberaaaaaaaaaaaaa";

const OPTIONS: ApplyOptions = ApplyOptions {
    advance_applied_rev: true,
    dead_letter_readiness: true,
    attempts: 2,
    live: true,
    subscription_rules: true,
    source: WriteSource::Firehose,
};

/// A writer in community whose S holds `A`, `B`, `C` and the member.
async fn writer(pool: &PgPool) -> Writer {
    let mut sets = ScopeSets::default();
    for d in [A, B, C, MEMBER] {
        sets.scope.insert(d.to_string());
    }
    sets.members.insert(MEMBER.to_string());
    let scope = ScopeState::new(sets, ScopePolicy::MembersAndOneHop, 10_000);
    {
        let mut guard = scope.write_guard().await;
        scope.set_public(&mut guard, PublicState { mode: PublicMode::On, generation: 1, ..PublicState::default() });
    }
    Writer::new(pool.clone(), scope, Arc::new(crate::private_tier::DisabledPrivateTier))
}

fn tid(n: i64) -> String {
    crate::thread_outline::ops::tid_string(1_700_000_000_000_000 + n)
}

fn create(collection: &str, rkey: &str, record: Value) -> RawOp {
    RawOp { action: Action::Create, collection: collection.into(), rkey: rkey.into(), cid: Some("bafybatch".into()), record: Some(record) }
}

fn delete(collection: &str, rkey: &str) -> RawOp {
    RawOp { action: Action::Delete, collection: collection.into(), rkey: rkey.into(), cid: None, record: None }
}

fn post_uri(author: &str, n: i64) -> String {
    format!("at://{author}/app.bsky.feed.post/{}", tid(n))
}

fn now() -> String {
    chrono::Utc::now().to_rfc3339()
}

/// A mixed corpus of commits, in stream order: posts with a tag, likes by
/// two accounts of one post, a like, unlike and like again by one account,
/// reposts, a reply and a quote, follows and an unfollow, a post deleted.
fn corpus() -> Vec<(String, String, Vec<RawOp>)> {
    let mut n = 0;
    let mut next = || {
        n += 1;
        tid(n)
    };
    let mut out: Vec<(String, String, Vec<RawOp>)> = Vec::new();
    for (author, p) in [(A, 1), (A, 2), (B, 3), (C, 4)] {
        let rev = next();
        out.push((author.into(), rev, vec![create("app.bsky.feed.post", &tid(p), json!({"$type": "app.bsky.feed.post", "text": "batch #batchtag", "tags": ["batchtag"], "createdAt": now()}))]));
    }
    let like = |subject: String| json!({"$type": "app.bsky.feed.like", "subject": {"uri": subject, "cid": "bafybatch"}, "createdAt": now()});
    out.push((B.into(), next(), vec![create("app.bsky.feed.like", &tid(100), like(post_uri(A, 1)))]));
    out.push((C.into(), next(), vec![create("app.bsky.feed.like", &tid(101), like(post_uri(A, 1)))]));
    // Like, unlike, like again of A's second post by B: counted once.
    out.push((B.into(), next(), vec![create("app.bsky.feed.like", &tid(102), like(post_uri(A, 2)))]));
    out.push((B.into(), next(), vec![delete("app.bsky.feed.like", &tid(102))]));
    out.push((B.into(), next(), vec![create("app.bsky.feed.like", &tid(103), like(post_uri(A, 2)))]));
    out.push((C.into(), next(), vec![create("app.bsky.feed.repost", &tid(104), json!({"$type": "app.bsky.feed.repost", "subject": {"uri": post_uri(A, 1), "cid": "bafybatch"}, "createdAt": now()}))]));
    out.push((
        C.into(),
        next(),
        vec![create(
            "app.bsky.feed.post",
            &tid(105),
            json!({"$type": "app.bsky.feed.post", "text": "a reply", "reply": {"root": {"uri": post_uri(A, 1), "cid": "bafybatch"}, "parent": {"uri": post_uri(A, 1), "cid": "bafybatch"}}, "createdAt": now()}),
        )],
    ));
    out.push((
        B.into(),
        next(),
        vec![create(
            "app.bsky.feed.post",
            &tid(106),
            json!({"$type": "app.bsky.feed.post", "text": "a quote", "embed": {"$type": "app.bsky.embed.record", "record": {"uri": post_uri(A, 2), "cid": "bafybatch"}}, "createdAt": now()}),
        )],
    ));
    let follow = |subject: &str| json!({"$type": "app.bsky.graph.follow", "subject": subject, "createdAt": now()});
    out.push((B.into(), next(), vec![create("app.bsky.graph.follow", &tid(107), follow(A))]));
    out.push((C.into(), next(), vec![create("app.bsky.graph.follow", &tid(108), follow(A))]));
    out.push((C.into(), next(), vec![delete("app.bsky.graph.follow", &tid(108))]));
    out.push((C.into(), next(), vec![delete("app.bsky.feed.post", &tid(4))]));
    out
}

fn items(corpus: &[(String, String, Vec<RawOp>)]) -> Vec<LiveCommit<'_>> {
    corpus
        .iter()
        .map(|(repo, rev, ops)| LiveCommit { relay: Some("bsky"), repo, rev, since: None, ops, options: OPTIONS, tier: super::super::tier::CopyTier::Whole, stream: None })
        .collect()
}

/// Apply every item, at most `max` to a transaction; the transactions.
async fn apply_all(w: &Writer, items: &[LiveCommit<'_>], max: usize) -> usize {
    let mut pos = 0;
    let mut calls = 0;
    while pos < items.len() {
        let reports = w.apply_batch(&items[pos..], max).await.expect("apply");
        assert!(!reports.is_empty());
        pos += reports.len();
        calls += 1;
    }
    calls
}

/// Every count table's rows by natural key, all-zero rows left out.
async fn counts(pool: &PgPool) -> (Vec<(String, String, i32, i32, i32, i32)>, Vec<(String, i32, i32, i32)>, Vec<(String, i32, i32, i32, i32)>) {
    let posts: Vec<(String, String, i32, i32, i32, i32)> = sqlx::query_as(
        "SELECT d.did, c.rkey::text, c.like_count, c.repost_count, c.reply_count, c.quote_count FROM post_counts c JOIN dids d ON d.id = c.author \
         WHERE c.like_count + c.repost_count + c.reply_count + c.quote_count > 0 ORDER BY 1, 2",
    )
    .fetch_all(pool)
    .await
    .expect("post counts");
    let actors: Vec<(String, i32, i32, i32)> = sqlx::query_as(
        "SELECT d.did, c.posts_count, c.follows_count, c.followers_count FROM actor_counts c JOIN dids d ON d.id = c.id \
         WHERE c.posts_count + c.follows_count + c.followers_count > 0 ORDER BY 1",
    )
    .fetch_all(pool)
    .await
    .expect("actor counts");
    let tags: Vec<(String, i32, i32, i32, i32)> = sqlx::query_as(
        "SELECT tag, posts, likes, reposts, replies FROM tag_activity_hourly WHERE posts + likes + reposts + replies > 0 ORDER BY 1",
    )
    .fetch_all(pool)
    .await
    .expect("tags");
    (posts, actors, tags)
}

/// Counts after one batch equal counts after the same commits one
/// transaction each; a like, unlike and like again by one account moves
/// the count by one; nothing is left in the deltas or counted as left.
#[tokio::test]
async fn counts_after_a_batch_equal_counts_after_the_same_commits_one_at_a_time() {
    let Some(one) = fresh_db("batch_counts_one").await else {
        return;
    };
    let Some(batched) = fresh_db("batch_counts_batched").await else {
        return;
    };
    let corpus = corpus();
    let items = items(&corpus);
    let calls_one = apply_all(&writer(&one.pool).await, &items, 1).await;
    let calls_batched = apply_all(&writer(&batched.pool).await, &items, 64).await;
    assert_eq!(calls_one, items.len());
    assert_eq!(calls_batched, 1, "every commit of the corpus shares one transaction");
    let (a, b) = (counts(&one.pool).await, counts(&batched.pool).await);
    assert_eq!(a, b);
    let a2 = a.0.iter().find(|r| r.0 == A && r.1 == crate::db::ids::rkey_value(&tid(2)).expect("tid").to_string()).expect("A's second post");
    assert_eq!((a2.2, a2.5), (1, 1), "liked once by B after like, unlike and like; quoted once");
    let a1 = a.0.iter().find(|r| r.0 == A && r.1 == crate::db::ids::rkey_value(&tid(1)).expect("tid").to_string()).expect("A's first post");
    assert_eq!((a1.2, a1.3, a1.4), (2, 1, 1));
    let a_actor = a.1.iter().find(|r| r.0 == A).expect("A");
    assert_eq!((a_actor.1, a_actor.3), (2, 1), "two posts, one follower after the unfollow");
    assert!(a.2.iter().any(|t| t.0 == "batchtag" && t.1 == 3 && t.2 == 3 && t.3 == 1 && t.4 == 1), "{:?}", a.2);
    for db in [&one, &batched] {
        let left: Option<Value> = sqlx::query_scalar("SELECT value FROM worker_state WHERE key = 'writer_counts_left'").fetch_optional(&db.pool).await.expect("left");
        assert_eq!(left, None, "every writer transaction applied its own counts");
    }
    one.drop_db().await;
    batched.drop_db().await;
}

/// A transaction that defers and never applies still counts at commit, and
/// counts it; a rolled-back savepoint takes its deltas with it; a session
/// that only sets the flag, with no deltas table, counts at once.
#[tokio::test]
async fn a_deferring_transaction_that_does_not_apply_its_counts_still_counts() {
    let Some(db) = fresh_db("batch_counts_left").await else {
        return;
    };
    let p = &db.pool;
    let w = writer(p).await;
    let corpus = corpus();
    apply_all(&w, &items(&corpus[..4]), 1).await;
    let ids = w.ids().account_ids(&[A.to_string(), B.to_string(), C.to_string()]).await.expect("ids");
    let (a, b, c) = (ids[A], ids[B], ids[C]);
    let post1 = crate::db::ids::rkey_value(&tid(1)).expect("tid");
    let like_count = || async {
        sqlx::query_scalar::<_, i32>("SELECT like_count FROM post_counts WHERE author = $1 AND rkey = $2").bind(a).bind(post1).fetch_optional(p).await.expect("count").unwrap_or(0)
    };
    let insert_like = |author: i32, rkey: i64| {
        sqlx::query("INSERT INTO likes (author, rkey, subj_author, subj_coll, subj_rkey, rev, created_at) VALUES ($1, $2, $3, 1, $4, 1, now())")
            .bind(author)
            .bind(rkey)
            .bind(a)
            .bind(post1)
    };
    // Deferred, a rolled-back savepoint, and no apply: counted at commit.
    let mut conn = p.acquire().await.expect("conn");
    let mut tx = conn.begin().await.expect("begin");
    sqlx::query("SELECT prism_defer_counts()").execute(&mut *tx).await.expect("defer");
    let mut sp = tx.begin().await.expect("savepoint");
    insert_like(c, 900).execute(&mut *sp).await.expect("like");
    sp.rollback().await.expect("rollback");
    insert_like(b, 901).execute(&mut *tx).await.expect("like");
    assert_eq!(like_count().await, 0, "the deferring transaction touched no count row");
    let pending: i64 = sqlx::query_scalar("SELECT count(*) FROM pg_temp.prism_count_deltas").fetch_one(&mut *tx).await.expect("deltas");
    assert_eq!(pending, 1, "the rolled-back like took its delta with it");
    tx.commit().await.expect("commit");
    assert_eq!(like_count().await, 1);
    let left: Option<Value> = sqlx::query_scalar("SELECT value FROM worker_state WHERE key = 'writer_counts_left'").fetch_optional(p).await.expect("left");
    assert_eq!(left, Some(json!(1)), "the commit counted the counts it found left");
    // Applied: nothing left at commit, and a later statement counts at once.
    let mut tx = conn.begin().await.expect("begin");
    sqlx::query("SELECT prism_defer_counts()").execute(&mut *tx).await.expect("defer");
    insert_like(c, 902).execute(&mut *tx).await.expect("like");
    sqlx::query("SELECT prism_apply_counts()").execute(&mut *tx).await.expect("apply");
    insert_like(c, 903).execute(&mut *tx).await.expect("like after the apply");
    tx.commit().await.expect("commit");
    assert_eq!(like_count().await, 2, "C counts once, however many likes");
    let left: Option<Value> = sqlx::query_scalar("SELECT value FROM worker_state WHERE key = 'writer_counts_left'").fetch_optional(p).await.expect("left");
    assert_eq!(left, Some(json!(1)));
    drop(conn);
    // A fresh session with the flag and no table: counted at once.
    let mut other = sqlx::PgConnection::connect_with(&p.connect_options()).await.expect("connect");
    let mut tx = other.begin().await.expect("begin");
    sqlx::query("SET LOCAL prism.defer_counts = on").execute(&mut *tx).await.expect("flag");
    sqlx::query("DELETE FROM likes WHERE author = $1 AND rkey = 901").bind(b).execute(&mut *tx).await.expect("unlike");
    tx.commit().await.expect("commit");
    assert_eq!(like_count().await, 1);
    db.drop_db().await;
}

/// Two transactions like hot posts P and Q in opposite orders. Deferred,
/// neither waits on the other's count rows until it applies, sorted: both
/// commit and the counts are right. Not deferred, the same interleaving
/// deadlocks, so the test bites.
#[tokio::test]
async fn opposite_orders_on_hot_rows_do_not_deadlock_once_counts_are_deferred() {
    let Some(db) = fresh_db("batch_hot_rows").await else {
        return;
    };
    let p = &db.pool;
    let w = writer(p).await;
    let corpus = corpus();
    apply_all(&w, &items(&corpus[..4]), 1).await;
    let ids = w.ids().account_ids(&[A.to_string(), B.to_string(), C.to_string()]).await.expect("ids");
    let (a, b, c) = (ids[A], ids[B], ids[C]);
    let (pp, qq) = (crate::db::ids::rkey_value(&tid(1)).expect("tid"), crate::db::ids::rkey_value(&tid(2)).expect("tid"));
    // Rows for both posts exist, so the immediate triggers update them.
    sqlx::query("INSERT INTO post_counts (author, rkey) VALUES ($1, $2), ($1, $3) ON CONFLICT DO NOTHING").bind(a).bind(pp).bind(qq).execute(p).await.expect("rows");
    // Too old for the trend tallies, so the count rows are the only rows
    // the two transactions share.
    let like = |author: i32, rkey: i64, subject: i64| {
        sqlx::query("INSERT INTO likes (author, rkey, subj_author, subj_coll, subj_rkey, rev, created_at) VALUES ($1, $2, $3, 1, $4, 1, now() - interval '30 days')")
            .bind(author)
            .bind(rkey)
            .bind(a)
            .bind(subject)
    };
    for (round, defer) in [(0i64, true), (1, false)] {
        let mut c1 = sqlx::PgConnection::connect_with(&p.connect_options()).await.expect("connect");
        let mut c2 = sqlx::PgConnection::connect_with(&p.connect_options()).await.expect("connect");
        sqlx::query("SET deadlock_timeout = '100ms'").execute(&mut c1).await.expect("timeout");
        sqlx::query("SET deadlock_timeout = '100ms'").execute(&mut c2).await.expect("timeout");
        let mut t1 = c1.begin().await.expect("begin");
        let mut t2 = c2.begin().await.expect("begin");
        if defer {
            sqlx::query("SELECT prism_defer_counts()").execute(&mut *t1).await.expect("defer");
            sqlx::query("SELECT prism_defer_counts()").execute(&mut *t2).await.expect("defer");
        }
        let k = 1000 + round * 10;
        like(b, k, pp).execute(&mut *t1).await.expect("b likes P");
        like(c, k + 1, qq).execute(&mut *t2).await.expect("c likes Q");
        let second = async {
            let r = like(b, k + 2, qq).execute(&mut *t1).await;
            match r {
                Ok(_) => sqlx::query("SELECT prism_apply_counts()").execute(&mut *t1).await.map(|_| ()),
                Err(e) => Err(e),
            }
        };
        let other = async {
            tokio::time::sleep(Duration::from_millis(50)).await;
            let r = like(c, k + 3, pp).execute(&mut *t2).await;
            match r {
                Ok(_) => sqlx::query("SELECT prism_apply_counts()").execute(&mut *t2).await.map(|_| ()),
                Err(e) => Err(e),
            }
        };
        // Deferred, t2's apply waits for t1's rows: t1 commits first.
        let (r1, r2) = if defer {
            let r1 = second.await;
            let commit1 = async {
                tokio::time::sleep(Duration::from_millis(200)).await;
                t1.commit().await
            };
            let (r2, c1r) = tokio::join!(other, commit1);
            c1r.expect("t1 commits");
            (r1, r2)
        } else {
            tokio::join!(second, other)
        };
        let deadlocked = [&r1, &r2].iter().any(|r| matches!(r, Err(sqlx::Error::Database(e)) if e.code().as_deref() == Some("40P01")));
        if defer {
            assert!(r1.is_ok() && r2.is_ok(), "deferred: {r1:?} {r2:?}");
            t2.commit().await.expect("t2 commits");
            let counts: (i32,) = sqlx::query_as("SELECT like_count FROM post_counts WHERE author = $1 AND rkey = $2").bind(a).bind(pp).fetch_one(p).await.expect("count");
            let counts_q: (i32,) = sqlx::query_as("SELECT like_count FROM post_counts WHERE author = $1 AND rkey = $2").bind(a).bind(qq).fetch_one(p).await.expect("count");
            assert_eq!((counts.0, counts_q.0), (2, 2), "both accounts like both posts");
        } else {
            assert!(deadlocked, "immediate counts deadlock in this order: {r1:?} {r2:?}");
        }
    }
    db.drop_db().await;
}

/// A member's commit (tracked) runs alone, in its place: the batch before
/// it ends there, and the commit after it starts the next.
#[tokio::test]
async fn a_tracked_repos_commit_runs_alone_in_its_place() {
    let Some(db) = fresh_db("batch_tracked_alone").await else {
        return;
    };
    let w = writer(&db.pool).await;
    let post = |n: i64| vec![create("app.bsky.feed.post", &tid(n), json!({"$type": "app.bsky.feed.post", "text": "x", "createdAt": now()}))];
    let corpus: Vec<(String, String, Vec<RawOp>)> = vec![
        (A.into(), tid(1), post(1)),
        (B.into(), tid(2), post(2)),
        (MEMBER.into(), tid(3), post(3)),
        (C.into(), tid(4), post(4)),
    ];
    let items = items(&corpus);
    assert_eq!(w.apply_batch(&items, 64).await.expect("first").len(), 2, "the batch ends before the member's commit");
    assert_eq!(w.apply_batch(&items[2..], 64).await.expect("member").len(), 1, "the member's commit runs alone");
    assert_eq!(w.apply_batch(&items[3..], 64).await.expect("rest").len(), 1);
    for (author, n) in [(A, 1), (B, 2), (MEMBER, 3), (C, 4)] {
        assert!(crate::test_support::posts::stored(&db.pool, &post_uri(author, n)).await, "{author}");
    }
    let applied: Option<String> = sqlx::query_scalar("SELECT applied_rev FROM sync_state WHERE did = $1").bind(MEMBER).fetch_optional(&db.pool).await.expect("sync state").flatten();
    assert_eq!(applied.as_deref(), Some(tid(3).as_str()), "the member's chain advanced in its own transaction");
    db.drop_db().await;
}

/// A failure at a later commit of a batch rolls the whole batch back:
/// nothing of it is stored.
#[tokio::test]
async fn a_failure_inside_a_batch_commits_none_of_it() {
    let Some(db) = fresh_db("batch_failure").await else {
        return;
    };
    let w = writer(&db.pool).await;
    let corpus = corpus();
    let items = items(&corpus);
    let failing = hooks::arm_times(hooks::WRITER_FAIL_IN_BATCH, &format!("{}/{C}", db.pool.connect_options().get_database().unwrap_or_default()), 1);
    assert!(w.apply_batch(&items, 64).await.is_err());
    assert_eq!(failing.hits(), 1);
    assert!(!crate::test_support::posts::stored(&db.pool, &post_uri(A, 1)).await, "the batch's first commit was rolled back");
    let likes: i64 = sqlx::query_scalar("SELECT count(*) FROM likes").fetch_one(&db.pool).await.expect("likes");
    assert_eq!(likes, 0);
    // Once it passes, the whole batch applies.
    assert_eq!(apply_all(&w, &items, 64).await, 1);
    assert!(crate::test_support::posts::stored(&db.pool, &post_uri(A, 1)).await);
    drop(failing);
    db.drop_db().await;
}
