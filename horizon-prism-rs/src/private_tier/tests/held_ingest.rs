//! Audiences held by this AppView, against every ingest source
//! (private-audiences design §4, T-A10). Built on the S10 harness, whose
//! items this module sees: the firehose's own commits through [`H::fire`],
//! jobs, `requestRecord` and catch-ups from [`Pds`].

use super::*;
use crate::firehose::backfill::plan;
use crate::firehose::index_request::{IndexStatus, index_record};

/// Every stored audience row: `(uri, cid, members)`.
async fn stored(h: &H) -> Vec<(String, String, Vec<String>)> {
    sqlx::query_as("SELECT uri, cid, members FROM private_audiences ORDER BY uri").fetch_all(h.pool()).await.expect("rows")
}

fn audience_op(action: Action, rkey: &str, members: &[&str]) -> RawOp {
    let record = (action != Action::Delete)
        .then(|| json!({"$type": NSID_AUDIENCE, "mode": "list", "members": members, "createdAt": "2026-09-26T00:00:00Z"}));
    RawOp { action, collection: NSID_AUDIENCE.into(), rkey: rkey.into(), cid: record.as_ref().map(|_| format!("bafy{rkey}new")), record }
}

/// T-A10: an audience create, update and delete from the firehose, a
/// member job (whose plan no longer lists the NSID), `requestRecord` and a
/// dead-letter replay (the same `index_record`, answering `out_of_scope`),
/// and a catch-up: none writes or deletes a row, and the rows that exist
/// are untouched, a held row with no repository copy included.
#[tokio::test]
async fn t_a10_no_source_writes_an_audience() {
    let Some(h) = H::new("pa-t-a10", PublicMode::On).await else {
        return;
    };
    let aud = h.audience_of(ALICE, "3kta10aud", "list", &[BOB]).await;
    // Held with no repository copy: no reconcile may take it.
    let mut tx = h.pool().begin().await.expect("tx");
    audiences::seed(&mut tx, ALICE, "3kta10only", "bafyonly", REV, &json!({"mode": "list", "members": [CAROL]}), false).await.expect("seed");
    tx.commit().await.expect("commit");
    let post = h.post(ALICE, "3kta10pst", &aud, None).await;
    h.f.ready_all().await;
    let before = stored(&h).await;
    assert_eq!(before.len(), 2);

    // The firehose.
    for op in [
        audience_op(Action::Create, "3kta10new", &[CAROL]),
        audience_op(Action::Update, "3kta10aud", &[CAROL]),
        audience_op(Action::Delete, "3kta10aud", &[]),
    ] {
        let report = h.fire(ALICE, R1, vec![op]).await;
        assert_eq!(report.outcomes, [OpOutcome::Skipped("kryphocron_unproduced")], "{report:?}");
        assert!(!report.readiness_cleared);
    }
    assert_eq!(stored(&h).await, before);

    // A member job: no plan lists audiences; a changed and a new repository
    // copy write nothing, and a held row its repository lacks stays.
    for depth in [Depth::Member, Depth::Private, Depth::Authz] {
        assert!(!plan(depth, true, PublicMode::On).collections.iter().any(|c| c.nsid == NSID_AUDIENCE), "{depth:?}");
    }
    let changed = json!({"$type": NSID_AUDIENCE, "mode": "list", "members": [DAVE], "createdAt": "2026-09-26T00:00:00Z"});
    h.pds.put_cid(ALICE, NSID_AUDIENCE, "3kta10aud", "bafychanged", changed.clone());
    h.pds.put(ALICE, NSID_AUDIENCE, "3kta10new", changed);
    h.pds.set_rev(ALICE, R2);
    h.clear_jobs().await;
    assert_eq!(h.run(&h.backfill(), ALICE, Depth::Member).await, JobOutcome::Ready);
    assert_eq!(stored(&h).await, before);

    // `requestRecord`, and a dead-letter replay, which calls the same
    // `index_record`: a record, a changed record and a gone one.
    for rkey in ["3kta10new", "3kta10aud", "3kta10only"] {
        let indexed = index_record(h.pds.as_ref(), &h.f.writer, ALICE, NSID_AUDIENCE, rkey, 1, false).await.expect("index");
        assert_eq!(indexed.status, IndexStatus::OutOfScope, "{rkey}");
    }
    assert_eq!(stored(&h).await, before);

    // Catch-up.
    h.pds.set_rev(ALICE, R3);
    assert!(h.catch_up(ALICE, R3).await);
    assert_eq!(stored(&h).await, before);
    // The rows still govern: Bob reads, Carol does not.
    h.fresh_heads().await;
    exec(h.pool(), &format!("UPDATE sync_state SET head_rev = applied_rev, head_checked_at = now() WHERE did = '{ALICE}'")).await;
    assert!(h.reads(BOB, &post).await);
    assert!(!h.reads(CAROL, &post).await);
    h.finish().await;
}
