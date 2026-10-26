//! A handle-form anchor in `getThread` (threadlens-handle-anchor design §7):
//! resolved after the cheap checks, as `resolveHandle` resolves, and from
//! then on only the DID form is used.

use async_trait::async_trait;

use super::*;
use crate::hydration::testing::hydrator_with_upstream_and_farsight;
use crate::hydration::upstream::{UpstreamSource, ViewSource};
use crate::identity::handle_lookup::HandleOutcome;
use crate::xrpc::pools::RequesterLimit;

/// An upstream whose handle answers are fixed by name, counting the
/// lookups. Nothing else is found.
struct Handles {
    lookups: AtomicUsize,
    /// One token: the second draw of a request finds it spent.
    spent: RequesterLimit,
}

#[async_trait]
impl UpstreamSource for Handles {
    async fn profile(&self, _req: &Requester, _actor: &str) -> Option<Value> {
        None
    }
    async fn posts(&self, _req: &Requester, _uris: &[String]) -> Vec<Value> {
        Vec::new()
    }
    async fn post_thread(&self, _req: &Requester, _uri: &str, _depth: u32, _parent_height: u32) -> Option<(Value, ViewSource)> {
        None
    }
    async fn author_feed(&self, _req: &Requester, _actor: &str, _limit: u32, _cursor: Option<&str>, _filter: &str) -> Option<Value> {
        None
    }
    async fn follows(&self, _req: &Requester, _actor: &str, _limit: u32, _cursor: Option<&str>) -> Option<Value> {
        None
    }
    async fn handle_outcome(&self, req: &Requester, handle: &str) -> HandleOutcome {
        self.lookups.fetch_add(1, Ordering::SeqCst);
        match handle {
            "far.example" => HandleOutcome::Found(AUTHOR.into()),
            "r1.example" => HandleOutcome::Found(R1.into()),
            "down.example" => HandleOutcome::Unavailable,
            "spent.example" => {
                let _ = req.draw_own(&self.spent);
                let _ = req.draw_own(&self.spent);
                HandleOutcome::Unavailable
            }
            _ => HandleOutcome::NotFound,
        }
    }
}

/// Syntax alone (§1.1): the post collection, a valid handle in any case and
/// a valid record key; nothing else.
#[test]
fn only_a_post_uri_with_a_valid_handle_is_a_handle_form() {
    use super::super::handlers::handle_anchor;

    assert_eq!(handle_anchor("at://Author.TEST/app.bsky.feed.post/3kanchor"), Some(("author.test".into(), "3kanchor".into())));
    for refused in [
        "at://author.test/app.bsky.feed.like/3k",
        "at://author.test/tools.kryphocron.feed.postPrivate/3kpriv",
        "at://author.test/app.bsky.feed.post",
        "at://author.test/app.bsky.feed.post/3k/x",
        "at://author.test/app.bsky.feed.post/..",
        "at://@author.test/app.bsky.feed.post/3k",
        "at://not_a_handle/app.bsky.feed.post/3k",
        "https://author.test/app.bsky.feed.post/3k",
    ] {
        assert_eq!(handle_anchor(refused), None, "{refused}");
    }
}

/// T-H1–T-H5, T-H7–T-H10.
#[tokio::test]
async fn a_handle_form_anchor_is_resolved_and_answered_in_did_form() {
    let Some(db) = fresh_db("threadlens_handle_anchor").await else {
        return;
    };
    let p = &db.pool;
    for (did, handle) in [(AUTHOR, "author.test"), (R1, "r1.test"), (VIEWER_A, "a.test")] {
        actor(p, did, handle, handle).await;
    }
    exec(p, &format!("INSERT INTO members (did, source, ready_at) VALUES ('{VIEWER_A}', 'admin', now())")).await;
    let anchor = post(p, AUTHOR, "3kanchor2222", "anchor", None, None).await;
    let reply = post(p, R1, "3kreply12222", "one", Some(&anchor), Some(&anchor)).await;
    exec(p, &format!("INSERT INTO blocks (uri, author_did, subject_did, rev, created_at) VALUES ('at://{VIEWER_A}/app.bsky.graph.block/3kb', '{VIEWER_A}', '{R1}', '3kaaaaaaaaaa2', now())")).await;
    // The anchor's author is indexed here: its stored handle answers.
    let mut sets = ScopeSets::default();
    sets.scope.insert(AUTHOR.into());
    let handles = Arc::new(Handles {
        lookups: AtomicUsize::new(0),
        spent: RequesterLimit::new("test", 0.001, 1, 1000),
    });
    let (farsight, _fake) = crate::farsight::lookup::tests::engine("").await;
    let h = hydrator_with_upstream_and_farsight(p, sets, Arc::clone(&handles) as Arc<dyn UpstreamSource>, farsight);
    let base = fake_upstream(Arc::new(Calls::default()), None).await;
    let l = ladder(h.clone(), &base, Arc::new(StubLookup::default()));
    // Many calls by one caller: a rate class that never runs out.
    let mut s = state(h, l, ThreadLensAccess::Members);
    s.app.limits = RateLimits::from_config(&crate::config::RateLimitsConfig {
        thread_lens: crate::config::RateClassConfig::new(1000.0, 1000),
        ..crate::config::RateLimitsConfig::default()
    });
    let url = serve(s).await;
    let lookups = || handles.lookups.load(Ordering::SeqCst);
    let u = url.as_str();
    let thread = move |anchor: String| async move {
        let (status, body) = call(u, GET_THREAD, VIEWER_A, 3, &json!({"anchor": anchor, "depth": 6})).await;
        (status, serde_json::from_str::<Value>(&body).unwrap_or_else(|_| json!(body)))
    };

    let (status, baseline) = thread(anchor.clone()).await;
    assert_eq!(status, 200, "{baseline}");
    assert_eq!(summary(&baseline, "nodes"), [(anchor.clone(), None), (reply.clone(), Some("blocked-by-you".to_string()))], "{baseline}");

    // T-H1, T-H9: an indexed account's handle, in any case, answers from
    // its stored row with no lookup; the answer is the DID-form one's.
    for handle in ["author.test", "Author.TEST"] {
        let (status, v) = thread(format!("at://{handle}/app.bsky.feed.post/3kanchor2222")).await;
        assert_eq!(status, 200, "{handle}: {v}");
        assert_eq!(v["anchor"], json!(anchor), "{v}");
        assert_eq!(v["nodes"], baseline["nodes"], "{v}");
    }
    assert_eq!(lookups(), 0);

    // T-H2: a handle not stored here goes to the resolver; same answer.
    let (status, v) = thread("at://far.example/app.bsky.feed.post/3kanchor2222".into()).await;
    assert_eq!((status, &v["anchor"], &v["nodes"]), (200, &json!(anchor), &baseline["nodes"]), "{v}");
    assert_eq!(lookups(), 1);

    // T-H7: the account the viewer blocks: its DID-form anchor and the
    // usual gap, with nothing of the post.
    let (status, v) = thread("at://r1.example/app.bsky.feed.post/3kreply12222".into()).await;
    assert_eq!(status, 200, "{v}");
    assert_eq!(v["anchor"], json!(reply), "{v}");
    // The parent chain comes first, then the anchor.
    assert_eq!(summary(&v, "nodes"), [(anchor.clone(), None), (reply.clone(), Some("blocked-by-you".to_string()))], "{v}");
    assert!(!v.to_string().contains("\"one\""), "{v}");
    assert_eq!(lookups(), 2);

    // T-H3: no such handle, a resolver failure, a spent identity share.
    for (handle, want, name) in [("nobody.example", 400, "HandleNotFound"), ("down.example", 502, "UpstreamFailure"), ("spent.example", 429, "")] {
        let (status, v) = thread(format!("at://{handle}/app.bsky.feed.post/3kanchor2222")).await;
        assert!(status == want && v.to_string().contains(name), "{handle}: {status} {v}");
    }
    assert_eq!(lookups(), 5);

    // T-H3, T-H8: an invalid handle or record key, another collection or a
    // private-tier form: refused at syntax, with no lookup.
    for bad in [
        "at://not_a_handle/app.bsky.feed.post/3kanchor2222",
        "at://far.example/app.bsky.feed.post/..",
        "at://far.example/app.bsky.feed.like/3kanchor2222",
        "at://far.example/tools.kryphocron.feed.postPrivate/3kpriv",
    ] {
        let (status, v) = thread(bad.into()).await;
        assert!(status == 400 && v.to_string().contains("anchor must be an at-uri"), "{bad}: {status} {v}");
    }
    // T-H10: a bad depth is refused before the lookup.
    let (status, _) = call(&url, GET_THREAD, VIEWER_A, 3, &json!({"anchor": "at://far.example/app.bsky.feed.post/3kanchor2222", "depth": 1001})).await;
    assert_eq!(status, 400);
    assert_eq!(lookups(), 5);

    // T-H4: a cursor sealed for the DID form opens under the handle form;
    // one sealed for another node is refused.
    let key = CursorKey::new(&[9; 32]);
    let own = key.seal(&anchor, &super::super::cursor::Position::Listed { cursor: None }).expect("sealed");
    let other = key.seal(&reply, &super::super::cursor::Position::Listed { cursor: None }).expect("sealed");
    let handle_form = "at://author.test/app.bsky.feed.post/3kanchor2222";
    let (status, body) = call(&url, GET_THREAD, VIEWER_A, 3, &json!({"anchor": handle_form, "repliesCursor": own})).await;
    assert_eq!(status, 200, "{body}");
    let (status, body) = call(&url, GET_THREAD, VIEWER_A, 3, &json!({"anchor": handle_form, "repliesCursor": other})).await;
    assert!(status == 400 && body.contains("repliesCursor"), "{body}");

    // T-H5: `resolve` stays DID-only.
    let (status, body) = call(&url, RESOLVE, VIEWER_A, 3, &json!({"uris": [handle_form]})).await;
    assert!(status == 400 && body.contains("InvalidRequest"), "{body}");
    db.drop_db().await;
}
