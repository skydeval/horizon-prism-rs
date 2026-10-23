//! Public root gates (docs/internal/design/public-root-gates.md §10): a
//! public thread root's threadgate is kept while a private post claims the
//! root (T-2), fetched once when it may have been missed (T-3), and its
//! `hiddenReplies` hide private replies in `getPostThread` and `getReplies`
//! (T-4); Thread Lens is unchanged (T-5); the feature is listed (T-6).

use std::collections::{HashMap, HashSet};
use std::sync::Mutex;

use crate::firehose::backfill::{BackfillSource, ListPage, ListedRecord, PrepareError};
use crate::microcosm::LinksPage;
use crate::private_tier::root_gates::RootGateFetches;

use super::*;

/// A public root's author: not a member, outside S.
const OP: &str = "did:plc:oooooooooooooooooooooooo";
const REV3: &str = "3kaaaaaaaaac2";
const REV4: &str = "3kaaaaaaaaad2";

fn public(author: &str, rkey: &str) -> String {
    format!("at://{author}/{PUBLIC_POST}/{rkey}")
}

fn gate_of(post: &str, hidden: &[&str]) -> Value {
    json!({"$type": THREADGATE, "post": post, "hiddenReplies": hidden, "createdAt": "2026-10-23T00:00:00Z"})
}

/// The stored copy's `hidden_replies`, if any.
async fn copy_of(pool: &PgPool, gate: &str) -> Option<Vec<String>> {
    sqlx::query_scalar("SELECT hidden_replies FROM private_threadgates WHERE uri = $1")
        .bind(gate)
        .fetch_optional(pool)
        .await
        .expect("copy")
}

/// `(attempts, done)` of a root's fetch row, if any.
async fn fetch_row(pool: &PgPool, root: &str) -> Option<(i32, bool)> {
    sqlx::query_as("SELECT attempts, done_at IS NOT NULL FROM private_root_gate_fetches WHERE root_uri = $1")
        .bind(root)
        .fetch_optional(pool)
        .await
        .expect("fetch row")
}

/// T-2 (§3), in On and in Off: a public root's gate by any author is kept
/// only while a private post claims the root, and then stays current; in On
/// an author in S also keeps its public row; deletes remove it; an
/// `out_of_scope` purge keeps it; an `account_deleted` one removes it.
async fn storage_in(mode: PublicMode, test: &str) {
    let Some(db) = fresh_db(test).await else {
        return;
    };
    let f = fixture_in(&db.pool, test, mode).await;
    let aud = f.audience(BOB, "3kaudbob2222", Some("list"), &[CAROL]).await;
    let p = public(OP, "3kproot22222");
    let gate = format!("at://{OP}/{THREADGATE}/3kproot22222");

    // No private reply claims P: not kept.
    f.ingest(OP, THREADGATE, "3kproot22222", gate_of(&p, &[])).await;
    assert_eq!(copy_of(&f.pool, &gate).await, None, "no private reply yet");
    // Bob's private reply claims P; the next write of the gate is kept.
    let r = f.private_post(BOB, "3kpreply2222", &aud, Some((&p, &p))).await;
    f.apply_op(OP, Action::Update, THREADGATE, "3kproot22222", REV2, Some(gate_of(&p, &[&r]))).await;
    assert_eq!(copy_of(&f.pool, &gate).await, Some(vec![r.clone()]));
    // LWW: an older write is refused.
    f.apply_op(OP, Action::Update, THREADGATE, "3kproot22222", REV, Some(gate_of(&p, &[]))).await;
    assert_eq!(copy_of(&f.pool, &gate).await, Some(vec![r.clone()]));
    // The reply goes; the stored copy stays current.
    f.apply_op(BOB, Action::Delete, NSID_POST_PRIVATE, "3kpreply2222", REV2, None).await;
    f.apply_op(OP, Action::Update, THREADGATE, "3kproot22222", REV3, Some(gate_of(&p, &[]))).await;
    assert_eq!(copy_of(&f.pool, &gate).await, Some(Vec::new()));
    // Not a root's gate: another rkey, another repo, a private post's.
    let q = public(OP, "3kqroot22222");
    f.private_post(BOB, "3kqreply2222", &aud, Some((&q, &q))).await;
    f.ingest(OP, THREADGATE, "3kotherkey22", gate_of(&q, &[])).await;
    f.ingest(NOBODY, THREADGATE, "3kqroot22222", gate_of(&q, &[])).await;
    assert_eq!(copy_of(&f.pool, &format!("at://{OP}/{THREADGATE}/3kotherkey22")).await, None);
    assert_eq!(copy_of(&f.pool, &format!("at://{NOBODY}/{THREADGATE}/3kqroot22222")).await, None);
    // No public row for an author outside S, in any mode.
    assert_eq!(public_gate_count(&f.pool).await, 0);

    // A member's root, Alice in S: in On the public row and the copy; in
    // Off the copy only.
    let a = public(ALICE, "3karoot22222");
    f.private_post(BOB, "3kareply2222", &aud, Some((&a, &a))).await;
    f.ingest(ALICE, THREADGATE, "3karoot22222", gate_of(&a, &[])).await;
    let alice_gate = format!("at://{ALICE}/{THREADGATE}/3karoot22222");
    assert_eq!(copy_of(&f.pool, &alice_gate).await, Some(Vec::new()));
    let public_rows = if mode.off_rule() { 0 } else { 1 };
    assert_eq!(public_gate_count(&f.pool).await, public_rows, "{mode:?}");

    // An `out_of_scope` purge of OP keeps its root gate.
    let mut conn = f.pool.acquire().await.expect("conn");
    while KryphocronPrivateTier.purge_out_of_scope_batch(&mut conn, OP, 100).await.expect("purge") > 0 {}
    drop(conn);
    assert!(copy_of(&f.pool, &gate).await.is_some(), "kept by out_of_scope");
    // A delete removes it.
    f.apply_op(ALICE, Action::Delete, THREADGATE, "3karoot22222", REV2, None).await;
    assert_eq!(copy_of(&f.pool, &alice_gate).await, None);
    assert_eq!(public_gate_count(&f.pool).await, 0);
    // `account_deleted` removes the author's.
    let mut conn = f.pool.acquire().await.expect("conn");
    while KryphocronPrivateTier.purge_did_batch(&mut conn, OP, 100).await.expect("purge") > 0 {}
    drop(conn);
    assert_eq!(copy_of(&f.pool, &gate).await, None);
    db.drop_db().await;
}

#[tokio::test]
async fn t2_root_gates_are_kept_on() {
    storage_in(PublicMode::On, "prg-storage-on").await;
}

#[tokio::test]
async fn t2_root_gates_are_kept_off() {
    storage_in(PublicMode::Off, "prg-storage-off").await;
}

/// One in-memory PDS for the fetch tests: records by URI, one latest rev,
/// and repos whose `prepare` fails.
#[derive(Default)]
struct GatePds {
    records: Mutex<HashMap<String, ListedRecord>>,
    rev: Mutex<String>,
    failing: Mutex<HashSet<String>>,
}

impl GatePds {
    fn put(&self, did: &str, collection: &str, rkey: &str, value: Value) {
        let uri = format!("at://{did}/{collection}/{rkey}");
        self.records.lock().expect("records").insert(uri.clone(), ListedRecord { uri, cid: format!("bafy{rkey}"), value });
    }
}

#[async_trait]
impl BackfillSource for GatePds {
    async fn prepare(&self, did: &str) -> Result<String, PrepareError> {
        if self.failing.lock().expect("failing").contains(did) {
            return Err(PrepareError::Failed("unreachable".into()));
        }
        Ok("https://pds.example.com".into())
    }

    async fn latest_rev(&self, _: &str, _: &str) -> Result<String, String> {
        Ok(self.rev.lock().expect("rev").clone())
    }

    async fn list_records(&self, _: &str, _: &str, _: &str, _: Option<&str>) -> Result<ListPage, String> {
        Ok(ListPage { records: Vec::new(), cursor: None })
    }

    async fn get_record(&self, _: &str, did: &str, collection: &str, rkey: &str) -> Result<Option<ListedRecord>, String> {
        Ok(self.records.lock().expect("records").get(&format!("at://{did}/{collection}/{rkey}")).cloned())
    }

    async fn inbound_blocks(&self, _: &str, _: Option<&str>) -> Result<LinksPage, String> {
        Ok(LinksPage { total: 0, linking_records: Vec::new(), cursor: None })
    }
}

/// T-3 (§4): a reply that is its root's only stored one queues one fetch;
/// the worker writes the gate at the PDS's rev; a gate the PDS does not
/// have writes nothing; failures back off and are given up after 8; a
/// root whose replies all went is queued again by its next reply (review
/// 1, LB-1); a delete while a fetch is pending tombstones the gate.
#[tokio::test]
async fn t3_the_fetch_queue() {
    let Some(db) = fresh_db("prg_fetch").await else {
        return;
    };
    let f = fixture_in(&db.pool, "prg-fetch", PublicMode::Off).await;
    let pds = Arc::new(GatePds::default());
    *pds.rev.lock().expect("rev") = REV2.into();
    let worker = RootGateFetches::new(f.pool.clone(), f.writer.clone(), Arc::clone(&pds) as Arc<dyn BackfillSource>, 1);
    let aud = f.audience(BOB, "3kaudbob2222", Some("list"), &[CAROL]).await;

    // The gate was written before any private reply: on the PDS only.
    let p = public(OP, "3kproot22222");
    let gate = format!("at://{OP}/{THREADGATE}/3kproot22222");
    let r = format!("at://{BOB}/{NSID_POST_PRIVATE}/3kpreply2222");
    pds.put(OP, THREADGATE, "3kproot22222", gate_of(&p, &[&r]));
    assert_eq!(f.private_post(BOB, "3kpreply2222", &aud, Some((&p, &p))).await, r);
    assert_eq!(fetch_row(&f.pool, &p).await, Some((0, false)), "queued");
    f.private_post(CAROL, "3kpreply3333", &aud, Some((&p, &p))).await;
    assert_eq!(worker.pass().await.expect("pass"), 1);
    assert_eq!(copy_of(&f.pool, &gate).await, Some(vec![r.clone()]), "fetched");
    let rev: String = sqlx::query_scalar("SELECT rev FROM private_threadgates WHERE uri = $1").bind(&gate).fetch_one(&f.pool).await.expect("rev");
    assert_eq!(rev, REV2, "at the PDS's rev");
    assert_eq!(fetch_row(&f.pool, &p).await, Some((0, true)));
    // A second reply queues nothing.
    f.private_post(CAROL, "3kpreply4444", &aud, Some((&p, &r))).await;
    assert_eq!(fetch_row(&f.pool, &p).await, Some((0, true)));
    assert_eq!(worker.pass().await.expect("pass"), 0);

    // No gate on the PDS: done, nothing written.
    let n = public(OP, "3knogate2222");
    f.private_post(BOB, "3knreply2222", &aud, Some((&n, &n))).await;
    assert_eq!(worker.pass().await.expect("pass"), 1);
    assert_eq!(fetch_row(&f.pool, &n).await, Some((0, true)));
    assert_eq!(copy_of(&f.pool, &format!("at://{OP}/{THREADGATE}/3knogate2222")).await, None);

    // LB-1: its only reply goes, then the gate hiding the next one comes
    // before that reply is stored, and is dropped; the reply queues the
    // fetch again.
    f.apply_op(BOB, Action::Delete, NSID_POST_PRIVATE, "3knreply2222", REV2, None).await;
    // (The fetch above found no gate at REV2 while it was pending, so the
    // gate is tombstoned at REV2; the author writes it later, at REV3.)
    let next = format!("at://{CAROL}/{NSID_POST_PRIVATE}/3knreply3333");
    f.apply_op(OP, Action::Create, THREADGATE, "3knogate2222", REV3, Some(gate_of(&n, &[&next]))).await;
    assert_eq!(copy_of(&f.pool, &format!("at://{OP}/{THREADGATE}/3knogate2222")).await, None, "dropped");
    pds.put(OP, THREADGATE, "3knogate2222", gate_of(&n, &[&next]));
    *pds.rev.lock().expect("rev") = REV3.into();
    f.private_post(CAROL, "3knreply3333", &aud, Some((&n, &n))).await;
    assert_eq!(fetch_row(&f.pool, &n).await, Some((0, false)), "queued again");
    assert_eq!(worker.pass().await.expect("pass"), 1);
    assert_eq!(copy_of(&f.pool, &format!("at://{OP}/{THREADGATE}/3knogate2222")).await, Some(vec![next]));

    // Failures back off, then are given up.
    const FAILS: &str = "did:plc:ffffffffffffffffffffffff";
    pds.failing.lock().expect("failing").insert(FAILS.into());
    let x = public(FAILS, "3kxroot22222");
    f.private_post(BOB, "3kxreply2222", &aud, Some((&x, &x))).await;
    assert_eq!(worker.pass().await.expect("pass"), 1);
    assert_eq!(fetch_row(&f.pool, &x).await, Some((1, false)));
    let later: bool = sqlx::query_scalar("SELECT next_at > now() + interval '1 minute' FROM private_root_gate_fetches WHERE root_uri = $1")
        .bind(&x)
        .fetch_one(&f.pool)
        .await
        .expect("next_at");
    assert!(later, "backed off");
    assert_eq!(worker.pass().await.expect("pass"), 0, "not due");
    exec(&f.pool, &format!("UPDATE private_root_gate_fetches SET attempts = 7, next_at = now() WHERE root_uri = '{x}'")).await;
    assert_eq!(worker.pass().await.expect("pass"), 1);
    assert_eq!(fetch_row(&f.pool, &x).await, Some((8, true)), "given up");

    // A delete while a fetch is pending tombstones the gate, so the
    // fetch's older read cannot write it.
    let d = public(OP, "3kdroot22222");
    pds.put(OP, THREADGATE, "3kdroot22222", gate_of(&d, &[]));
    f.private_post(BOB, "3kdreply2222", &aud, Some((&d, &d))).await;
    f.apply_op(OP, Action::Delete, THREADGATE, "3kdroot22222", REV3, None).await;
    assert_eq!(worker.pass().await.expect("pass"), 1);
    assert_eq!(copy_of(&f.pool, &format!("at://{OP}/{THREADGATE}/3kdroot22222")).await, None, "tombstoned");
    // A later write than the delete is kept.
    f.apply_op(OP, Action::Create, THREADGATE, "3kdroot22222", REV4, Some(gate_of(&d, &[]))).await;
    assert_eq!(copy_of(&f.pool, &format!("at://{OP}/{THREADGATE}/3kdroot22222")).await, Some(Vec::new()));
    db.drop_db().await;
}
