//! The §11 G-9 row rule on public paths (design-v4 §8.1 "Embedded profile
//! views"; R3-143, B-53 "Embedded views", B-54): a DID's `actors` row is
//! read only if the DID is in `scope_dids` or is a member. Any other DID is
//! rendered as if no row existed, whatever is stored for it, so an account
//! whose only tie to this AppView is private (a private owner, the author
//! of a private reply to a member) answers exactly as an unknown DID does.
//!
//! The surfaces a member can point at any DID: `getMutes`, `getBlocks`, the
//! list creators in `getListMutes` and `getListBlocks`, Thread Lens nodes
//! (profiles and the gate), and the gate behind `getProfile`.
//! Private-context hydration keeps its own rule and still renders those
//! rows.

use std::sync::Arc;

use serde_json::{Value, json};

use super::actor::ProfileKind;
use super::graph::MuteScope;
use super::lists::PURPOSE_MOD;
use super::testing::{FixedUpstream, actor, exec, hydrator_with};
use super::{HydrationContext, Hydrator, UpstreamSource, Viewer};
use crate::aggregations::ScopeSets;
use crate::test_support::fresh_db;
use crate::xrpc::pools::Requester;

/// The member who mutes and blocks.
const ALICE: &str = "did:plc:aaaaaaaaaaaaaaaaaaaaaaaa";
/// Another member.
const BOB: &str = "did:plc:bbbbbbbbbbbbbbbbbbbbbbbb";
/// In S, not a member.
const SCOPED: &str = "did:plc:ssssssssssssssssssssssss";
/// A private owner, never in S: its `private` backfill stored its
/// identity and profile.
const OWNER: &str = "did:plc:oooooooooooooooooooooooo";
/// The author of a private reply to a member (class M): the identity
/// refresher stored its PDS and handle.
const REPLIER: &str = "did:plc:rrrrrrrrrrrrrrrrrrrrrrrr";
/// A private owner whose account is deactivated.
const GONE: &str = "did:plc:gggggggggggggggggggggggg";
/// Never seen: no row of any kind.
const UNKNOWN: &str = "did:plc:uuuuuuuuuuuuuuuuuuuuuuuu";

/// What every surface is compared under.
const SUBJECT: &str = "did:plc:subject";

fn list_uri(owner: &str) -> String {
    format!("at://{owner}/app.bsky.graph.list/3kmods")
}

fn post_uri(author: &str) -> String {
    format!("at://{author}/app.bsky.feed.post/3kpost")
}

/// `value` as text with `did` written as [`SUBJECT`], so two subjects'
/// answers compare byte for byte.
fn masked(value: &Value, did: &str) -> String {
    serde_json::to_string(value).expect("json").replace(did, SUBJECT)
}

/// The stored state: members, S, the private owners, and the rows the
/// private pipeline's identity work leaves behind for OWNER, REPLIER and
/// GONE.
async fn world(p: &sqlx::PgPool) -> Hydrator {
    actor(p, ALICE, "alice.test", "Alice").await;
    actor(p, BOB, "bob.test", "Bob").await;
    actor(p, SCOPED, "scoped.test", "Scoped").await;
    exec(p, &format!("INSERT INTO members (did, source, ready_at) VALUES ('{ALICE}', 'admin', now()), ('{BOB}', 'admin', now())")).await;
    // As backfill `prepare` and a `private` depth's profile fetch store a
    // private owner (§5.10).
    for (did, handle, status) in [(OWNER, "owner.test", "active"), (GONE, "gone.test", "deactivated")] {
        exec(
            p,
            &format!(
                "INSERT INTO actors (did, pds_endpoint, handle, handle_checked_at, account_status, status_checked_at, \
                   display_name, description, avatar_cid, pronouns, website, profile_record) \
                 VALUES ('{did}', 'https://pds.example.com', '{handle}', now(), '{status}', now(), \
                   'Private Owner', 'about me', 'bafyavatar', 'they/them', 'https://owner.example', \
                   '{{\"createdAt\":\"2026-01-01T00:00:00Z\"}}'::jsonb)"
            ),
        )
        .await;
        exec(p, &format!("INSERT INTO actor_counts (did, followers_count, follows_count, posts_count) VALUES ('{did}', 5, 6, 7)")).await;
    }
    // As the identity refresher stores a class-M author: its PDS, then its
    // handle once checked.
    exec(p, &format!("INSERT INTO actors (did, pds_endpoint, handle, handle_checked_at) VALUES ('{REPLIER}', 'https://pds.example.com', 'replier.test', now())")).await;
    // A ready member's list is served from its stored row.
    exec(
        p,
        &format!(
            "INSERT INTO lists (uri, creator_did, rkey, cid, rev, name, purpose, record, created_at) VALUES \
             ('{}', '{BOB}', '3kmods', 'bafylist', '3k', 'Mods', '{PURPOSE_MOD}', '{{}}', '2026-01-01T00:00:00Z')",
            list_uri(BOB)
        ),
    )
    .await;

    let mut sets = ScopeSets::default();
    for did in [ALICE, BOB] {
        sets.members.insert(did.into());
        sets.scope.insert(did.into());
    }
    sets.scope.insert(SCOPED.into());
    sets.private_owners.insert(OWNER.into());
    sets.private_owners.insert(GONE.into());

    let mut up = FixedUpstream::default();
    for did in [OWNER, REPLIER, GONE, UNKNOWN, SCOPED, BOB] {
        let uri = list_uri(did);
        up.records.insert(
            uri.clone(),
            json!({"uri": uri, "cid": "bafylist", "value": {"$type": "app.bsky.graph.list", "name": "Mods", "purpose": PURPOSE_MOD, "createdAt": "2026-01-01T00:00:00Z"}}),
        );
    }
    hydrator_with(p, sets, Arc::new(up) as Arc<dyn UpstreamSource>)
}

/// Every public surface a member can aim at `did`, by name, with `did`
/// masked. Each leaves the viewer's state as it found it.
async fn surfaces(h: &Hydrator, did: &str) -> Vec<(&'static str, String)> {
    let p = h.pool();
    let me = Viewer { did: Some(ALICE.into()), hydrate: true };
    let req = Requester::test();
    let mut out = Vec::new();

    h.set_mute(&me, &req, did, Some(MuteScope::Full)).await.expect("db").expect("muted");
    let v = h.get_mutes(&me, &req, 50, None).await.expect("db").expect("mutes");
    out.push(("getMutes", masked(v.inner(), did)));
    h.set_mute(&me, &req, did, None).await.expect("db").expect("unmuted");

    exec(p, &format!("INSERT INTO blocks (uri, author_did, subject_did, rev, created_at) VALUES ('at://{ALICE}/app.bsky.graph.block/3kblock', '{ALICE}', '{did}', '3k', '2026-01-01T00:00:00Z')")).await;
    let v = h.get_blocks(&me, &req, 50, None).await.expect("db").expect("blocks");
    out.push(("getBlocks", masked(v.inner(), did)));
    exec(p, "DELETE FROM blocks").await;

    let list = list_uri(did);
    exec(p, &format!("INSERT INTO list_mutes (viewer_did, list_uri, created_at) VALUES ('{ALICE}', '{list}', '2026-01-01T00:00:00Z')")).await;
    let v = h.get_list_mutes(&me, &req, 50, None).await.expect("db").expect("list mutes");
    out.push(("getListMutes", masked(v.inner(), did)));
    exec(p, "DELETE FROM list_mutes").await;

    exec(
        p,
        &format!(
            "INSERT INTO list_blocks (uri, author_did, rkey, cid, rev, list_uri, created_at) VALUES \
             ('at://{ALICE}/app.bsky.graph.listblock/3klb', '{ALICE}', '3klb', 'bafy', '3k', '{list}', '2026-01-01T00:00:00Z')"
        ),
    )
    .await;
    let v = h.get_list_blocks(&me, &req, 50, None).await.expect("db").expect("list blocks");
    out.push(("getListBlocks", masked(v.inner(), did)));
    exec(p, "DELETE FROM list_blocks").await;

    // Thread Lens: a node's author (no viewer state) and the gate on a
    // node by `did`.
    let lens = h.anonymous_profile_context(&[did.to_string()]).await.expect("db");
    out.push(("Thread Lens author", masked(&lens.view(did, ProfileKind::Basic), did)));
    let visible = h.lens_visible(&req, &post_uri(did)).await.expect("db");
    out.push(("Thread Lens gate", visible.to_string()));

    // `getProfile`'s gate: an account outside S takes the cascade, which
    // knows neither.
    let v = h.get_profile(&me, &req, did).await.expect("db").map(|v| v.inner().clone()).map_err(|e| json!([e.error, e.message]));
    out.push(("getProfile", masked(&v.unwrap_or_else(|e| e), did)));
    out
}

/// B-53 "Embedded views" (R3-143): a privately tied DID answers every
/// surface exactly as a DID never seen does.
#[tokio::test]
async fn a_privately_tied_did_answers_as_an_unknown_one() {
    let Some(db) = fresh_db("hydration_g9_row_filter").await else {
        return;
    };
    let h = world(&db.pool).await;
    let unknown = surfaces(&h, UNKNOWN).await;
    let mut leaks = Vec::new();
    for tied in [OWNER, REPLIER, GONE] {
        for ((surface, got), (_, want)) in surfaces(&h, tied).await.iter().zip(&unknown) {
            if got != want {
                leaks.push(format!("{tied} on {surface}:\n  tied:    {got}\n  unknown: {want}"));
            }
        }
    }
    assert!(leaks.is_empty(), "privately tied DIDs differ from an unknown DID:\n{}", leaks.join("\n"));
    db.drop_db().await;
}

/// The rule hides nothing it should not: members and accounts in S render
/// in full on the same surfaces, and private-context hydration still
/// renders a private owner's row (§8.1 "Private-context hydration").
#[tokio::test]
async fn members_and_scope_render_in_full() {
    let Some(db) = fresh_db("hydration_g9_row_filter_full").await else {
        return;
    };
    let h = world(&db.pool).await;
    for (did, handle, name) in [(BOB, "bob.test", "Bob"), (SCOPED, "scoped.test", "Scoped")] {
        let all = surfaces(&h, did).await;
        let text = |surface: &str| all.iter().find(|(s, _)| *s == surface).map(|(_, t)| t.clone()).expect("surface");
        let mutes: Value = serde_json::from_str(&text("getMutes")).expect("json");
        assert_eq!(mutes["mutes"][0]["handle"], handle, "{did}");
        assert_eq!(mutes["mutes"][0]["displayName"], name, "{did}");
        assert_eq!(mutes["mutes"][0]["createdAt"], "2026-01-01T00:00:00Z", "{did}");
        assert!(mutes["mutes"][0].get("indexedAt").is_some(), "{did}");
        let blocks: Value = serde_json::from_str(&text("getBlocks")).expect("json");
        assert_eq!(blocks["blocks"][0]["handle"], handle, "{did}");
        assert_eq!(blocks["blocks"][0]["viewer"]["blocking"], format!("at://{ALICE}/app.bsky.graph.block/3kblock"));
        for surface in ["getListMutes", "getListBlocks"] {
            let lists: Value = serde_json::from_str(&text(surface)).expect("json");
            assert_eq!(lists["lists"][0]["creator"]["handle"], handle, "{did} {surface}");
            assert_eq!(lists["lists"][0]["creator"]["displayName"], name, "{did} {surface}");
        }
        let lens: Value = serde_json::from_str(&text("Thread Lens author")).expect("json");
        assert_eq!(lens["handle"], handle, "{did}");
        assert_eq!(lens["displayName"], name, "{did}");
    }
    // A deactivated account in S is still hidden by the gate.
    exec(&db.pool, &format!("UPDATE actors SET account_status = 'deactivated' WHERE did = '{SCOPED}'")).await;
    assert!(!h.lens_visible(&Requester::test(), &post_uri(SCOPED)).await.expect("db"));
    let me = Viewer { did: Some(ALICE.into()), hydrate: true };
    let e = h.get_profile(&me, &Requester::test(), SCOPED).await.expect("db").expect_err("hidden");
    assert_eq!(e.error, "AccountDeactivated");

    // Private-context hydration reads every stored row.
    let ctx = HydrationContext::private(&h, me, &Requester::test());
    let authors = ctx.profiles(&[OWNER.to_string(), REPLIER.to_string()]).await.expect("db");
    let owner = authors.view(OWNER, ProfileKind::Basic);
    assert_eq!(owner["handle"], "owner.test");
    assert_eq!(owner["displayName"], "Private Owner");
    assert_eq!(authors.view(REPLIER, ProfileKind::Basic)["handle"], "replier.test");
    db.drop_db().await;
}
