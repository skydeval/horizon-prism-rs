//! Viewer-private fetches (design-v4 §8.1; R3-188, R3-200, R3-201,
//! R3-208): the in-crate parts of B-53 "Viewer-private fetches" and of
//! B-54's check that no viewer-private fetch writes a shared cache.
//!
//! - Every covered call site reaches the upstream seam with a
//!   viewer-private [`Requester`], and an ordinary request with an ordinary
//!   one ([`FixedUpstream`] records the mode of each call).
//! - A viewer-private count read stores no `constellation_counts` row and
//!   makes no Constellation call.
//! - A source check: a function that takes a [`Requester`] and queries an
//!   AppView-private table makes its fetches viewer-private, so a new call
//!   site of that shape cannot forget to.
//!
//! That the shared caches below the seam (`rt`, `route`, the PDS order
//! cache) stay untouched in this mode is tested beside them, in
//! `upstream.rs` and `cache.rs`.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Instant;

use serde_json::json;

use super::graph::MuteScope;
use super::lists::PURPOSE_MOD;
use super::testing::{FixedUpstream, actor, exec, hydrator_full, hydrator_with, post};
use super::{CountMode, UpstreamSource, Viewer};
use crate::aggregations::ScopeSets;
use crate::test_support::fresh_db;
use crate::xrpc::pools::Requester;

const ALICE: &str = "did:plc:aaaaaaaaaaaaaaaaaaaaaaaa";
const BOB: &str = "did:plc:bbbbbbbbbbbbbbbbbbbbbbbb";
const FAR: &str = "did:plc:ffffffffffffffffffffffff";
const OWNER: &str = "did:plc:oooooooooooooooooooooooo";

fn list_record(uri: &str, name: &str) -> serde_json::Value {
    json!({"uri": uri, "cid": "bafylist", "value": {"$type": "app.bsky.graph.list", "name": name, "purpose": PURPOSE_MOD, "createdAt": "2026-01-01T00:00:00Z"}})
}

/// Every call in `modes` is viewer-private, and there is at least one.
fn all_private(modes: &[(String, bool)], what: &str) {
    assert!(!modes.is_empty(), "{what}: no upstream call was made");
    for (call, private) in modes {
        assert!(*private, "{what}: `{call}` was not viewer-private");
    }
}

/// B-53 "Viewer-private fetches" (R3-188, R3-200), in-crate: each covered
/// call site fetches viewer-private, and the same subject fetched for an
/// ordinary public request is fetched as an ordinary one.
#[tokio::test]
async fn covered_call_sites_fetch_viewer_private() {
    let Some(db) = fresh_db("hydration_viewer_private").await else {
        return;
    };
    let p = &db.pool;
    actor(p, ALICE, "alice.test", "Alice").await;
    actor(p, BOB, "bob.test", "Bob").await;
    exec(p, &format!("INSERT INTO members (did, source, ready_at) VALUES ('{ALICE}', 'admin', now())")).await;
    // L: an outside owner's moderation list, read from its record. G: the
    // list a stored post's threadgate names.
    let muted = format!("at://{OWNER}/app.bsky.graph.list/3kmuted");
    let gate_list = format!("at://{OWNER}/app.bsky.graph.list/3kgate");
    let mut up = FixedUpstream::default();
    up.records.insert(muted.clone(), list_record(&muted, "Muted"));
    up.records.insert(gate_list.clone(), list_record(&gate_list, "Gate"));
    up.profiles.insert(FAR.into(), json!({"did": FAR, "handle": "far.example", "viewer": {}, "labels": []}));
    up.handles.insert("far.example".into(), FAR.into());
    let up = Arc::new(up);
    let h = hydrator_with(p, ScopeSets::default(), Arc::clone(&up) as Arc<dyn UpstreamSource>);
    let me = Viewer { did: Some(ALICE.into()), hydrate: true };
    let req = Requester::test();

    // `muteActorList`'s existence check (R3-188).
    h.set_list_mute(&me, &req, &muted, true).await.expect("db").expect("muted");
    all_private(&up.take_modes(), "muteActorList");

    // `getListMutes`' list views.
    let listed = h.get_list_mutes(&me, &req, 50, None).await.expect("db").expect("mutes");
    assert_eq!(listed.inner()["lists"][0]["uri"], json!(muted));
    all_private(&up.take_modes(), "getListMutes");

    // `muteActor`'s actor resolution, by handle.
    h.set_mute(&me, &req, "far.example", Some(MuteScope::Full)).await.expect("db").expect("muted");
    all_private(&up.take_modes(), "muteActor");
    h.set_mute(&me, &req, "far.example", None).await.expect("db").expect("unmuted");
    all_private(&up.take_modes(), "unmuteActor");
    h.set_mute(&me, &req, "far.example", Some(MuteScope::Full)).await.expect("db").expect("muted");
    up.take_modes();

    // The mute of L applies, so FAR's views name L in `mutedByList`.
    exec(p, &format!("UPDATE list_references SET effective = true WHERE member_did = '{ALICE}' AND list_uri = '{muted}'")).await;
    exec(p, &format!("INSERT INTO subscribed_lists (list_uri, owner_did, state, purpose) VALUES ('{muted}', '{OWNER}', 'active', '{PURPOSE_MOD}')")).await;
    exec(
        p,
        &format!(
            "INSERT INTO subscribed_list_items (uri, list_uri, owner_did, rkey, cid, rev, subject_did) VALUES \
             ('at://{OWNER}/app.bsky.graph.listitem/3kf', '{muted}', '{OWNER}', '3kf', 'c', '3k', '{FAR}')"
        ),
    )
    .await;

    // `getMutes`' entries, with `mutedByList` resolved.
    let mutes = h.get_mutes(&me, &req, 50, None).await.expect("db").expect("mutes");
    assert_eq!(mutes.inner()["mutes"][0]["did"], json!(FAR));
    all_private(&up.take_modes(), "getMutes");

    // An ordinary public view of FAR: the profile is an ordinary fetch;
    // only the list behind `mutedByList` is viewer-private.
    let profile = h.get_profile(&me, &req, FAR).await.expect("db").expect("profile");
    assert_eq!(profile.inner()["viewer"]["mutedByList"]["uri"], json!(muted));
    let modes = up.take_modes();
    assert!(modes.contains(&(format!("profile {FAR}"), false)), "{modes:?}");
    assert!(modes.contains(&(format!("record {muted}"), true)), "{modes:?}");
    assert!(!modes.contains(&(format!("record {muted}"), false)), "{modes:?}");

    // `listActivitySubscriptions`' subjects (R3-200).
    exec(p, &format!("INSERT INTO activity_subscriptions (subscriber_did, subject_did, post, reply) VALUES ('{ALICE}', '{FAR}', true, true)")).await;
    let subs = h.list_activity_subscriptions(&me, &req, 50, None).await.expect("db").expect("subscriptions");
    assert_eq!(subs.inner()["subscriptions"][0]["did"], json!(FAR));
    all_private(&up.take_modes(), "listActivitySubscriptions");

    // `getBookmarks`' items: a stored post whose threadgate names G.
    let gated = post(p, BOB, "3kgated", "gated", None, None).await;
    sqlx::query(
        "INSERT INTO threadgates (uri, author_did, rkey, cid, rev, post_uri, allow, hidden_replies, record, created_at) \
         VALUES ($1, $2, '3kgated', 'bafygate', '3k', $3, $4, $5, '{\"$type\": \"app.bsky.feed.threadgate\"}', now())",
    )
    .bind(format!("at://{BOB}/app.bsky.feed.threadgate/3kgated"))
    .bind(BOB)
    .bind(&gated)
    .bind(json!([{"$type": "app.bsky.feed.threadgate#listRule", "list": gate_list}]))
    .bind(Vec::<String>::new())
    .execute(p)
    .await
    .expect("gate");
    exec(p, &format!("INSERT INTO bookmarks (viewer_did, post_uri, post_cid) VALUES ('{ALICE}', '{gated}', 'bafy')")).await;
    let marks = h.get_bookmarks(&me, &req, 50, None).await.expect("db").expect("bookmarks");
    assert_eq!(marks.inner()["bookmarks"][0]["item"]["threadgate"]["lists"][0]["uri"], json!(gate_list));
    all_private(&up.take_modes(), "getBookmarks");
    // The same post for an anonymous public request: an ordinary fetch.
    h.get_posts(&Viewer::anonymous(), &req, std::slice::from_ref(&gated)).await.expect("db").expect("posts");
    assert_eq!(up.take_modes(), [(format!("record {gate_list}"), false)]);
    db.drop_db().await;
}

/// B-53 "Private-context hydration", B-54 (design-v4 §8.1; R3-113,
/// R3-179): what the private handlers render through
/// `HydrationContext::Private` comes from local rows only. A list behind
/// `mutedByList`, or named by a stored parent's threadgate, that is not
/// locally servable is left out rather than fetched, so no upstream, `rt`
/// or `route` access follows from a private read.
#[tokio::test]
async fn private_context_hydration_fetches_nothing() {
    use super::HydrationContext;
    use super::actor::ProfileKind;

    let Some(db) = fresh_db("hydration_private_context").await else {
        return;
    };
    let p = &db.pool;
    actor(p, ALICE, "alice.test", "Alice").await;
    actor(p, BOB, "bob.test", "Bob").await;
    exec(p, &format!("INSERT INTO members (did, source, ready_at) VALUES ('{ALICE}', 'admin', now())")).await;
    let muted = format!("at://{OWNER}/app.bsky.graph.list/3kmuted");
    let gate_list = format!("at://{OWNER}/app.bsky.graph.list/3kgate");
    let mut up = FixedUpstream::default();
    up.records.insert(muted.clone(), list_record(&muted, "Muted"));
    up.records.insert(gate_list.clone(), list_record(&gate_list, "Gate"));
    let up = Arc::new(up);
    let h = hydrator_with(p, ScopeSets::default(), Arc::clone(&up) as Arc<dyn UpstreamSource>);
    let me = Viewer { did: Some(ALICE.into()), hydrate: true };
    let req = Requester::test();
    // Alice's mute of L applies, and FAR is on L.
    h.set_list_mute(&me, &req, &muted, true).await.expect("db").expect("muted");
    exec(p, &format!("UPDATE list_references SET effective = true WHERE member_did = '{ALICE}' AND list_uri = '{muted}'")).await;
    exec(p, &format!("INSERT INTO subscribed_lists (list_uri, owner_did, state, purpose) VALUES ('{muted}', '{OWNER}', 'active', '{PURPOSE_MOD}')")).await;
    exec(
        p,
        &format!(
            "INSERT INTO subscribed_list_items (uri, list_uri, owner_did, rkey, cid, rev, subject_did) VALUES \
             ('at://{OWNER}/app.bsky.graph.listitem/3kf', '{muted}', '{OWNER}', '3kf', 'c', '3k', '{FAR}')"
        ),
    )
    .await;
    // A stored public post whose threadgate names G.
    let gated = post(p, BOB, "3kgated", "gated", None, None).await;
    sqlx::query(
        "INSERT INTO threadgates (uri, author_did, rkey, cid, rev, post_uri, allow, hidden_replies, record, created_at) \
         VALUES ($1, $2, '3kgated', 'bafygate', '3k', $3, $4, $5, '{\"$type\": \"app.bsky.feed.threadgate\"}', now())",
    )
    .bind(format!("at://{BOB}/app.bsky.feed.threadgate/3kgated"))
    .bind(BOB)
    .bind(&gated)
    .bind(json!([{"$type": "app.bsky.feed.threadgate#listRule", "list": gate_list}]))
    .bind(Vec::<String>::new())
    .execute(p)
    .await
    .expect("gate");
    up.take_modes();

    let ctx = HydrationContext::private(&h, me.clone(), &req);
    let authors = ctx.profiles(&[FAR.to_string(), BOB.to_string()]).await.expect("db");
    let far = authors.view(FAR, ProfileKind::Basic);
    assert_eq!(far["viewer"]["muted"], json!(true), "the mute applies");
    assert!(far["viewer"].get("mutedByList").is_none(), "the list is not fetched: {far}");
    assert_eq!(authors.view(BOB, ProfileKind::Basic)["handle"], "bob.test");
    let parent = ctx.public_post(&gated).await.expect("db").expect("stored");
    assert_eq!(parent.inner()["threadgate"]["lists"], json!([]));
    assert_eq!(up.take_modes(), Vec::<(String, bool)>::new(), "no upstream call");
    // Viewer-private mode alone still fetches (from Slingshot, past the
    // shared caches): private-context hydration is more than that.
    h.profile_context(&me, &req.viewer_private(ALICE), &[FAR.to_string()], None).await.expect("db");
    assert_eq!(up.take_modes(), [(format!("record {muted}"), true)]);
    db.drop_db().await;
}

/// B-53 (R3-201), in-crate: a viewer-private count read uses stored rows
/// but stores none and asks Constellation nothing, whatever mode the
/// caller names; an ordinary one fetches and stores.
#[tokio::test]
async fn a_viewer_private_count_read_writes_no_constellation_counts_row() {
    use axum::routing::get;

    let Some(db) = fresh_db("hydration_viewer_private_counts").await else {
        return;
    };
    let p = &db.pool;
    let calls = Arc::new(AtomicUsize::new(0));
    let seen = Arc::clone(&calls);
    let router = axum::Router::new().route(
        "/links/count/distinct-dids",
        get(move || {
            seen.fetch_add(1, Ordering::SeqCst);
            async { axum::Json(json!({"total": 7})) }
        }),
    );
    let base = crate::test_support::serve_fake(router).await;
    let h = hydrator_full(p, ScopeSets::default(), Arc::new(FixedUpstream::default()), Some(&base));
    let rows = || async { sqlx::query_scalar::<_, i64>("SELECT count(*) FROM constellation_counts").fetch_one(p).await.expect("count") };
    let target = [FAR.to_string()];
    let private = Requester::test().viewer_private(ALICE);

    for mode in [CountMode::Sync { started: Instant::now() }, CountMode::List] {
        let got: HashMap<_, _> = h.constellation_counts(&private, &target, mode).await;
        assert!(got.is_empty());
    }
    assert_eq!((rows().await, calls.load(Ordering::SeqCst)), (0, 0));

    // An ordinary single-object read fetches and stores the row.
    let got = h.constellation_counts(&Requester::test(), &target, CountMode::Sync { started: Instant::now() }).await;
    assert_eq!(got[FAR].followers, Some(7));
    assert_eq!((rows().await, calls.load(Ordering::SeqCst)), (1, 1));
    // A viewer-private read may read the stored row.
    let got = h.constellation_counts(&private, &target, CountMode::Sync { started: Instant::now() }).await;
    assert_eq!(got[FAR].followers, Some(7));
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    db.drop_db().await;
}

/// The AppView-private tables of design-v4 §4.5.
const PRIVATE_TABLES: [&str; 7] = [
    "mutes",
    "list_mutes",
    "thread_mutes",
    "bookmarks",
    "drafts",
    "activity_subscriptions",
    "notification_preferences_v2",
];

/// Calls that write an AppView-private table outside this crate's SQL.
const PRIVATE_WRITERS: [&str; 2] = ["mute_list(", "unmute_list("];

/// Functions that take a [`Requester`] and read a private table, but
/// whose fetches do not follow from the rows read, with the reason.
const EXEMPT: [(&str, &str, &str); 1] = [(
    "feed.rs",
    "post_context_for",
    "bookmark counts for a caller-given subject set; nothing is fetched for the rows read",
)];

/// One function of a source file: its name, signature and body.
struct Function<'a> {
    name: &'a str,
    signature: &'a str,
    body: &'a str,
}

/// The functions of `source` before its first inline test module, split
/// at each line that starts a `fn`.
fn functions(source: &str) -> Vec<Function<'_>> {
    let mut cut = source.len();
    let mut offset = 0;
    let mut lines = source.split_inclusive('\n').peekable();
    while let Some(line) = lines.next() {
        let next = lines.peek().map_or("", |l| l.trim_end());
        if line.trim_end() == "#[cfg(test)]" && next.contains("mod ") && next.ends_with('{') {
            cut = offset;
            break;
        }
        offset += line.len();
    }
    let code = &source[..cut];
    let mut starts: Vec<usize> = Vec::new();
    let mut offset = 0;
    for line in code.split_inclusive('\n') {
        let t = line.trim_start();
        let t = t.strip_prefix("pub(crate) ").or_else(|| t.strip_prefix("pub(super) ")).or_else(|| t.strip_prefix("pub ")).unwrap_or(t);
        let t = t.strip_prefix("async ").unwrap_or(t);
        if t.starts_with("fn ") {
            starts.push(offset);
        }
        offset += line.len();
    }
    starts
        .iter()
        .enumerate()
        .map(|(i, &start)| {
            let end = starts.get(i + 1).copied().unwrap_or(code.len());
            let text = &code[start..end];
            let after = &text[text.find("fn ").unwrap_or(0) + 3..];
            let name = &after[..after.find(|c: char| !(c.is_alphanumeric() || c == '_')).unwrap_or(after.len())];
            let brace = text.find('{').unwrap_or(text.len());
            Function { name, signature: &text[..brace], body: &text[brace..] }
        })
        .collect()
}

/// Whether `body` queries or writes an AppView-private table.
fn touches_private_table(body: &str) -> bool {
    let sql = PRIVATE_TABLES.iter().any(|t| {
        ["FROM", "INTO", "UPDATE", "JOIN"].iter().any(|kw| {
            body.match_indices(&format!("{kw} {t}")).any(|(i, m)| {
                let next = body[i + m.len()..].chars().next();
                !next.is_some_and(|c| c.is_alphanumeric() || c == '_')
            })
        })
    });
    sql || PRIVATE_WRITERS.iter().any(|w| body.contains(w))
}

/// B-54 (§8.1 rule; R3-188, R3-200, R3-201): every function in the public
/// hydration and handler code that takes a [`Requester`] and queries an
/// AppView-private table runs its fetches viewer-private. The rule is a
/// rule, not a list, so this finds the call sites itself.
#[test]
fn every_private_table_query_with_a_requester_fetches_viewer_private() {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
    let mut checked = Vec::new();
    let mut missing = Vec::new();
    for dir in ["hydration", "xrpc/handlers", "xrpc"] {
        let Ok(entries) = std::fs::read_dir(root.join(dir)) else {
            continue;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            let file = path.file_name().and_then(|f| f.to_str()).unwrap_or_default().to_string();
            if path.extension().and_then(|e| e.to_str()) != Some("rs") || file == "viewer_private_tests.rs" {
                continue;
            }
            let source = std::fs::read_to_string(&path).expect("source");
            for f in functions(&source) {
                if !f.signature.contains("&Requester") || !touches_private_table(f.body) {
                    continue;
                }
                if EXEMPT.iter().any(|(ef, en, _)| *ef == file && *en == f.name) {
                    continue;
                }
                checked.push(format!("{file}:{}", f.name));
                if !f.body.contains(".viewer_private(") {
                    missing.push(format!("{file}:{}", f.name));
                }
            }
        }
    }
    assert!(missing.is_empty(), "these query a private table and fetch with a shared Requester: {missing:?}");
    // The known call sites are found, so the check is not vacuous.
    for known in [
        "graph.rs:get_mutes",
        "graph.rs:set_mute",
        "bookmarks.rs:get_bookmarks",
        "list_mutes.rs:get_list_mutes",
        "list_mutes.rs:set_list_mute",
        "activity_subscriptions.rs:list_activity_subscriptions",
    ] {
        assert!(checked.iter().any(|c| c == known), "{known} not found among {checked:?}");
    }
}
