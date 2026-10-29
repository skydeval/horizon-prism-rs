//! Members' list blocks in the private read rule: T-PB1 to T-PB9. A list block that applies
//! counts wherever the rule tests a block, both ways; one that does not
//! apply changes nothing.

use super::*;

/// Undoes every [`list_block_sql`] (the lists and items stay, inert).
const UNBLOCK_LISTS: &str = "DELETE FROM list_references";

/// T-PB1: rule 4 by a list block, on every kind of post, either way: the
/// reader's list naming the author, and the author's naming the reader.
#[tokio::test]
async fn t_pb1_a_list_block_on_every_kind() {
    let Some(mut w) = Walk::new_in("tpb1-kinds", PublicMode::On, Layout::OffPds, Some("followers"), &[], &[], Arc::new(NoHeads)).await else {
        return;
    };
    let (t, aud) = (w.root.clone(), w.aud.clone());
    let x = format!("at://{CAROL}/{PUBLIC_POST}/3kpb1public2");
    let bob_aud = w.f.audience(BOB, "3kpb1bobaud2", Some("everyone"), &[]).await;
    let mut all = vec![
        ("owner top-level", ALICE, t.clone()),
        ("owner thread reply", ALICE, w.reply(ALICE, "3kpb1owner22", &t).await),
        ("cross-owner thread reply", BOB, w.reply(BOB, "3kpb1cross22", &t).await),
        ("own audience top-level", BOB, w.f.private_post(BOB, "3kpb1bobtop2", &bob_aud, None).await),
    ];
    let branch_root = w.f.private_post(ALICE, "3kpb1broot22", &aud, Some((&x, &x))).await;
    all.push(("branch root", ALICE, branch_root.clone()));
    all.push(("branch reply", BOB, w.f.private_post(BOB, "3kpb1brrepl", &aud, Some((&x, &branch_root))).await));
    w.f.ready_all().await;
    for (n, (kind, author, uri)) in all.iter().enumerate() {
        w.expect(&format!("{kind}, no list block"), DAVE, uri, true).await;
        for (i, (member, subject)) in [(DAVE, *author), (*author, DAVE)].into_iter().enumerate() {
            exec(&w.f.pool, &list_block_sql(member, CAROL, &format!("3kpb1l{n}{i}"), &[subject])).await;
            w.expect(&format!("{kind}, {member} list-blocks {subject}"), DAVE, uri, false).await;
            exec(&w.f.pool, UNBLOCK_LISTS).await;
            w.expect(&format!("{kind}, {member}'s list block removed"), DAVE, uri, true).await;
        }
    }
    w.finish().await;
}

/// T-PB2: rule 7 by a list block, under `list`: the owner's list naming a
/// listed reader, the owner's naming the writer, the writer's naming the
/// owner, and a reader's naming the owner.
#[tokio::test]
async fn t_pb2_rule_7_by_a_list_block() {
    let Some(mut w) = Walk::new("tpb2-rule7", Layout::SamePds, Some("list"), &[BOB, CAROL, DAVE]).await else {
        return;
    };
    let root = w.root.clone();
    let reply = w.reply(BOB, "3kpb2reply22", &root).await;
    w.f.ready_all().await;
    for reader in [BOB, CAROL, DAVE] {
        w.expect("no list block", reader, &reply, true).await;
    }
    let cases: [(&str, &str, &str, [bool; 3]); 4] = [
        ("Alice's list names Dave", ALICE, DAVE, [true, true, false]),
        ("Alice's list names Bob", ALICE, BOB, [true, false, false]),
        ("Bob's list names Alice", BOB, ALICE, [true, false, false]),
        ("Dave's list names Alice", DAVE, ALICE, [true, true, false]),
    ];
    for (n, (case, member, subject, reads)) in cases.into_iter().enumerate() {
        exec(&w.f.pool, &list_block_sql(member, CAROL, &format!("3kpb2l{n}"), &[subject])).await;
        for (reader, reads) in [BOB, CAROL, DAVE].into_iter().zip(reads) {
            w.expect(case, reader, &reply, reads).await;
        }
        // The root, Alice's own post, is read by everyone the case does
        // not set against Alice.
        w.expect(&format!("{case}, the root"), DAVE, &root, !(member == DAVE || subject == DAVE)).await;
        exec(&w.f.pool, UNBLOCK_LISTS).await;
    }
    w.finish().await;
}

/// T-PB3: a list block that does not apply changes nothing (a curation
/// list, a reference not admitted or not synced, a list not active, a list
/// mute), and a list naming its own subscriber never blocks it.
#[tokio::test]
async fn t_pb3_only_list_blocks_that_apply() {
    let Some(mut w) = Walk::new("tpb3-applies", Layout::SamePds, Some("followers"), &[]).await else {
        return;
    };
    let root = w.root.clone();
    let reply = w.reply(BOB, "3kpb3reply22", &root).await;
    w.f.ready_all().await;
    let block = list_block_sql(DAVE, CAROL, "3kpb3list222", &[BOB]);
    exec(&w.f.pool, &block).await;
    w.expect("the list block applies", DAVE, &reply, false).await;
    for (case, change, undo) in [
        (
            "a curation list",
            "UPDATE subscribed_lists SET purpose = 'app.bsky.graph.defs#curatelist'",
            "UPDATE subscribed_lists SET purpose = 'app.bsky.graph.defs#modlist'",
        ),
        ("not admitted", "UPDATE list_references SET admitted = false", "UPDATE list_references SET admitted = true"),
        ("not synced", "UPDATE list_references SET effective = false", "UPDATE list_references SET effective = true"),
        ("unreferenced", "UPDATE subscribed_lists SET state = 'unreferenced'", "UPDATE subscribed_lists SET state = 'active'"),
        ("a list mute", "UPDATE list_references SET source = 'mute'", "UPDATE list_references SET source = 'block'"),
    ] {
        exec(&w.f.pool, change).await;
        w.expect(case, DAVE, &reply, true).await;
        exec(&w.f.pool, undo).await;
        w.expect(&format!("{case}, undone"), DAVE, &reply, false).await;
    }
    exec(&w.f.pool, UNBLOCK_LISTS).await;
    // Dave on his own list: his reply in the chain still carries Carol's
    // below it to him, and Bob, who is not on it, reads both.
    let daves = w.reply(DAVE, "3kpb3dave222", &root).await;
    let carols = w.reply(CAROL, "3kpb3carol22", &daves).await;
    w.f.ready_all().await;
    exec(&w.f.pool, &list_block_sql(DAVE, DAVE, "3kpb3self222", &[DAVE])).await;
    for (reader, uri) in [(DAVE, &daves), (DAVE, &carols), (BOB, &carols), (CAROL, &daves)] {
        w.expect("a list naming its own subscriber", reader, uri, true).await;
    }
    w.finish().await;
}

/// T-PB4: the walk. T ← R1 (Carol) ← R2 (Bob): Dave's list naming Carol
/// stops R2 for Dave (the walk passes Carol's step), in the shared walk and
/// the per-key reference; Bob still reads it.
#[tokio::test]
async fn t_pb4_the_walk_passes_a_list_blocked_step() {
    let Some(mut w) = Walk::new("tpb4-walk", Layout::SamePds, Some("followers"), &[]).await else {
        return;
    };
    let root = w.root.clone();
    let r1 = w.reply(CAROL, "3kpb4r12222", &root).await;
    let r2 = w.reply(BOB, "3kpb4r22222", &r1).await;
    w.f.ready_all().await;
    w.expect("no list block", DAVE, &r2, true).await;
    exec(&w.f.pool, &list_block_sql(DAVE, ALICE, "3kpb4list222", &[CAROL])).await;
    w.expect("Dave's list names Carol: R1", DAVE, &r1, false).await;
    w.expect("Dave's list names Carol: R2", DAVE, &r2, false).await;
    w.expect("Bob reads R2", BOB, &r2, true).await;
    for per_key in [true, false] {
        schema::PER_KEY.with(|p| p.set(per_key));
        let passing = schema::prefilter_uris(&w.f.pool, DAVE, std::slice::from_ref(&r2), CAP).await.expect("prefilter");
        schema::PER_KEY.with(|p| p.set(false));
        w.check(passing.is_empty(), format!("per key {per_key}: {passing:?}"));
    }
    // Carol's list naming Dave: the same, from the other side.
    exec(&w.f.pool, UNBLOCK_LISTS).await;
    exec(&w.f.pool, &list_block_sql(CAROL, CAROL, "3kpb4list2r2", &[DAVE])).await;
    w.expect("Carol's list names Dave: R2", DAVE, &r2, false).await;
    w.finish().await;
}

/// T-PB6 and T-PB7: like counts, repost items, notifications and unread
/// counts drop a list-blocked account either way; `blocks_between` reports
/// a list block in its direction.
#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn t_pb6_counts_reposts_and_notifications() {
    let Some(mut w) = Walk::new("tpb6-counts", Layout::SamePds, Some("followers"), &[]).await else {
        return;
    };
    let t = w.root.clone();
    let pool = w.f.pool.clone();
    exec(&pool, &format!(
        "INSERT INTO likes (uri, author_did, subject_uri, subject_cid, rev, created_at) \
         VALUES ('at://{BOB}/app.bsky.feed.like/3kpb6like222', '{BOB}', '{t}', 'bafyt', '3k', now())"
    )).await;
    exec(&pool, &format!(
        "INSERT INTO reposts (uri, author_did, subject_uri, subject_cid, rev, created_at) \
         VALUES ('at://{BOB}/app.bsky.feed.repost/3kpb6repost2', '{BOB}', '{t}', 'bafyt', '3k', now())"
    )).await;
    let id: i64 = sqlx::query_scalar(&format!(
        "INSERT INTO private_notifications (recipient_did, author_did, reason, reason_subject, record_uri, record_cid) \
         VALUES ('{DAVE}', '{BOB}', 'like', '{t}', 'at://{BOB}/app.bsky.feed.like/3kpb6like222', 'bafyl') RETURNING id"
    ))
    .fetch_one(&pool)
    .await
    .expect("notification");
    w.f.ready_all().await;
    let state = move |pool: PgPool, t: String| async move {
        let likes = schema::counts(&pool, DAVE, std::slice::from_ref(&t), CAP).await.expect("counts").get(&t).map_or(0, |c| c.like);
        let feed = schema::author_batch(&pool, DAVE, BOB, schema::AuthorFilter::Replies, None, 50, CAP).await.expect("feed");
        let reposts = feed.iter().filter(|(c, pass)| c.repost_uri.is_some() && *pass).count();
        let kept = schema::notifications_kept(&pool, DAVE, &[id]).await.expect("kept").len();
        let unread = schema::unread_count(&pool, DAVE, CAP).await.expect("unread");
        (likes, reposts, kept, unread)
    };
    let before = state(pool.clone(), t.clone()).await;
    w.check(before == (1, 1, 1, 1), format!("no list block: {before:?}"));
    for (n, (member, subject)) in [(DAVE, BOB), (BOB, DAVE)].into_iter().enumerate() {
        exec(&pool, &list_block_sql(member, CAROL, &format!("3kpb6l{n}"), &[subject])).await;
        let after = state(pool.clone(), t.clone()).await;
        w.check(after == (0, 0, 0, 0), format!("{member}'s list names {subject}: {after:?}"));
        let between = schema::blocks_between(&pool, DAVE, BOB).await.expect("between");
        w.check(between == (member == DAVE, member == BOB), format!("blocks_between with {member}'s list: {between:?}"));
        exec(&pool, UNBLOCK_LISTS).await;
    }
    let after = state(pool.clone(), t.clone()).await;
    w.check(after == before, format!("list blocks removed: {after:?}"));
    // The recipient's list mute still drops the notification, as before.
    exec(&pool, &list_block_sql(DAVE, CAROL, "3kpb6mute222", &[BOB])).await;
    exec(&pool, "UPDATE list_references SET source = 'mute'").await;
    let kept = schema::notifications_kept(&pool, DAVE, &[id]).await.expect("kept");
    w.check(kept.is_empty(), format!("a list mute keeps the notification: {kept:?}"));
    w.finish().await;
}

/// The node types an `EXPLAIN (FORMAT JSON)` plan uses on `relation`.
fn scans_of(node: &Value, relation: &str, out: &mut Vec<String>) {
    if node["Relation Name"] == relation {
        out.push(node["Node Type"].as_str().unwrap_or("").to_string());
    }
    for child in node["Plans"].as_array().into_iter().flatten() {
        scans_of(child, relation, out);
    }
}

/// T-PB8: with sequential scans off, the prefilter and the unread count
/// reach the three subscription tables through their indexes.
#[tokio::test]
async fn t_pb8_list_block_probes_are_index_backed() {
    let Some(w) = Walk::new("tpb8-plans", Layout::SamePds, Some("followers"), &[]).await else {
        return;
    };
    let root = w.root.clone();
    let reply = w.reply(BOB, "3kpb8reply22", &root).await;
    exec(&w.f.pool, &list_block_sql(DAVE, CAROL, "3kpb8list222", &[CAROL])).await;
    w.f.ready_all().await;
    let mut tx = w.f.pool.begin().await.expect("tx");
    sqlx::query("SET LOCAL enable_seqscan = off").execute(&mut *tx).await.expect("seqscan off");
    let prefilter: Value = sqlx::query_scalar(&format!("EXPLAIN (FORMAT JSON) {}", schema::prefilter_uris_statement(CAP)))
        .bind(DAVE)
        .bind(vec![root.clone(), reply.clone()])
        .fetch_one(&mut *tx)
        .await
        .expect("explain prefilter");
    let unread: Value = sqlx::query_scalar(&format!("EXPLAIN (FORMAT JSON) {}", schema::unread_statement(CAP)))
        .bind(DAVE)
        .fetch_one(&mut *tx)
        .await
        .expect("explain unread");
    tx.rollback().await.expect("rollback");
    let mut failures = Vec::new();
    for (name, plan) in [("prefilter", &prefilter), ("unread", &unread)] {
        for relation in ["list_references", "subscribed_lists", "subscribed_list_items"] {
            let mut scans = Vec::new();
            scans_of(&plan[0]["Plan"], relation, &mut scans);
            if scans.is_empty() || scans.iter().any(|s| s == "Seq Scan") {
                failures.push(format!("{name}, {relation}: {scans:?}"));
            }
        }
    }
    w.db.drop_db().await;
    assert!(failures.is_empty(), "{failures:?}");
}

/// T-PB unit checks: the purpose rule has one definition; `blocked` has its
/// four terms (two direct, two list; private blocks removed 2026-10-24); the builder's aliases capture no outer row; the feature.
#[test]
fn t_pb_unit_the_builder() {
    assert_eq!(crate::hydration::applies_as("r", "l"), crate::hydration::APPLIES);
    let b = schema::blocked_for_tests("$1", "l.author_did");
    assert_eq!(b.matches("EXISTS (").count(), 4, "{b}");
    assert!(b.contains("lbi.subject_did = l.author_did") && b.contains("lbr.member_did = l.author_did"), "{b}");
    assert!(b.contains("$1 <> l.author_did") && b.contains("l.author_did <> $1"), "{b}");
    for alias in ["l", "r", "i", "q", "s", "k", "b", "c", "p"] {
        for decl in [format!("list_references {alias} "), format!("subscribed_lists {alias} "), format!("subscribed_list_items {alias} ")] {
            assert!(!b.contains(&decl), "{decl} in {b}");
        }
    }
    let between = schema::blocks_between_statement();
    assert_eq!(between.matches("FROM list_references lbr").count(), 2, "{between}");
    assert!(super::super::FEATURES.contains(&"private.listBlocks"));
}
