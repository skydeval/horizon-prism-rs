//! Refract tests that need more than one module (refract design §13).

mod checks;
mod config;
pub(crate) mod harness;
mod storage;

use std::sync::Arc;
use std::time::Duration;

use sqlx::PgPool;

use crate::aggregations::ScopeSets;
use crate::hydration::testing::hydrator;
use crate::refract::RefractHidden;
use crate::xrpc::pools::Requester;

const OUTSIDER: &str = "did:plc:outsider";

/// T-8ap: the snapshot is built once per request and shared by its clones
/// (`viewer_private`, `detached`, private context); no read guard is held
/// past the build, so a writer can take the write guard at once.
#[tokio::test]
async fn t_8ap_snapshot_once_per_request_shared_by_clones() {
    let pool = PgPool::connect_lazy("postgres://nobody@127.0.0.1:1/none").expect("lazy pool");
    let mut sets = ScopeSets::default();
    let mut hidden = RefractHidden::default();
    hidden.update(OUTSIDER, |r| r.ending = true);
    sets.refract_hidden = Arc::new(hidden);
    let h = hydrator(&pool, sets);
    let req = Requester::test();
    let private = req.viewer_private("did:plc:viewer");
    let detached = private.detached();
    assert!(req.shares_refract(&private) && req.shares_refract(&detached));
    assert!(detached.refract_snapshot().is_none(), "built on the first gate call only");
    let first = h.refract_snapshot(&detached).await;
    assert!(first.wholesale.contains(OUTSIDER));
    // No guard is held: a writer gets the write guard at once, and changes
    // the stored set.
    {
        let mut w = tokio::time::timeout(Duration::from_secs(1), h.scope().write_guard()).await.expect("no read guard held");
        w.refract_hidden = Arc::default();
    }
    // Every clone, and the request itself, keeps the snapshot it built.
    for r in [&req, &private, &detached] {
        let again = h.refract_snapshot(r).await;
        assert!(Arc::ptr_eq(&first, &again));
    }
    // A new request sees the change.
    assert!(h.refract_snapshot(&Requester::test()).await.wholesale.is_empty());
    // The check is shared too.
    detached.note_refract_check(|c| c.note("at://did:plc:x/app.bsky.feed.post/1", true));
    assert_eq!(req.refract_check().hidden.len(), 1);
}
