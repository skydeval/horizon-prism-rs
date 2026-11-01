//! Compact storage ids and codecs: maps, codecs, interning and the SQL helpers.
//! Postgres-backed tests run when `HPRS_TEST_DATABASE_URL` is set.

use std::collections::HashMap;
use std::sync::Arc;

use super::*;
use crate::test_support::fresh_db;

const A: &str = "did:plc:compactaaaaaaaaaaaaaaaaa";
const B: &str = "did:plc:compactbbbbbbbbbbbbbbbbb";

/// Odd and ordinary inputs both sides must agree on.
const TIDS: [&str; 8] = [
    "2222222222222",
    "3kaaaaaaaaa22",
    "3myp5txuf7s2d",
    "bzzzzzzzzzzzz",
    "c222222222222",
    "jzzzzzzzzzzzz",
    "3ka",
    "kaaaaaaaaaaaa",
];

#[test]
fn rkey_map_keeps_top_bit_tids_out() {
    assert_eq!(rkey_value("2222222222222"), Some(0));
    let v = rkey_value("3myp5txuf7s2d").expect("tid");
    assert_eq!(rkey_string(v).as_deref(), Some("3myp5txuf7s2d"));
    // A first character c..j sets the top bit: valid TID, but an alias.
    assert!(proto_blue::syntax::Tid::is_valid("c222222222222"));
    assert_eq!(rkey_value("c222222222222"), None);
    assert_eq!(rkey_value("3ka"), None);
}

#[test]
fn rev_map_keeps_text_order_over_every_valid_tid() {
    let valid: Vec<&str> = TIDS.iter().copied().filter(|t| proto_blue::syntax::Tid::is_valid(t)).collect();
    let mut by_text = valid.clone();
    by_text.sort_unstable();
    let mut by_value = valid.clone();
    by_value.sort_by_key(|t| rev_value(t));
    assert_eq!(by_text, by_value);
    for t in &valid {
        assert_eq!(rev_string(rev_value(t)), *t);
    }
    // The sentinel is the lowest; anything invalid maps to it.
    assert_eq!(rev_value("2222222222222"), i64::MIN);
    assert_eq!(rev_value("3ka"), i64::MIN);
    assert!(rev_value("c222222222222") > rev_value("bzzzzzzzzzzzz"));
}

#[test]
fn cids_round_trip_canonical_or_not() {
    let canonical = "bafyreidbcoifjdzsr3u5i74kbm427m5yic67grd54s4luhlt4xxntevd6m";
    let stored = cid_to_db(canonical);
    assert_eq!(stored.len(), 36);
    assert_ne!(stored[0], 0);
    assert_eq!(cid_from_db(&stored), canonical);
    for odd in ["bafyfar", "BAFYREIDBCOIFJDZSR3U5I74KBM427M5YIC67GRD54S4LUHLT4XXNTEVD6M", "QmYwAPJzv5CZsnA625s3Xf2nemtYgPpHdWEz79ojWnPbdG", "", "not a cid"] {
        let stored = cid_to_db(odd);
        assert_eq!(stored[0], 0, "{odd}");
        assert_eq!(cid_from_db(&stored), odd);
    }
}

#[test]
fn uri_parts_is_strict() {
    let whole = format!("at://{A}/app.bsky.feed.post/3myp5txuf7s2d");
    assert_eq!(uri_parts(&whole), UriParts::Whole { did: A.into(), coll: 1, rkey: "3myp5txuf7s2d".into() });
    assert_eq!(whole_uri(A, 1, "3myp5txuf7s2d").as_deref(), Some(whole.as_str()));
    for (uri, did) in [
        (format!("at://{A}/app.bsky.feed.post/3myp5txuf7s2d/"), true),
        (format!("at://{A}/app.bsky.feed.post/3myp5txuf7s2d/x"), true),
        (format!("at://{A}/app.bsky.feed.post"), true),
        (format!("at://{A}/com.example.thing/3myp5txuf7s2d"), true),
        (format!("at://{A}/app.bsky.feed.post/bad key"), true),
        ("at://alice.example/app.bsky.feed.post/3myp5txuf7s2d".to_string(), false),
        ("https://example.com/x".to_string(), false),
    ] {
        assert_eq!(uri_parts(&uri), UriParts::Odd { did: did.then(|| A.to_string()) }, "{uri}");
    }
}

#[test]
fn record_codec_round_trips_and_refuses_unknown_dicts() {
    let json = r#"{"$type":"app.bsky.feed.post","text":"hello","createdAt":"2026-10-31T00:00:00Z"}"#;
    let plain = encode_record(json, None).expect("dict 0");
    assert_eq!(plain[0], 0);
    assert_eq!(decode_record(&plain, &HashMap::new()).expect("decode"), json);

    let samples: Vec<Vec<u8>> = (0..2000)
        .map(|i| format!(r#"{{"$type":"app.bsky.feed.post","text":"sample post number {i} with some words","langs":["en"],"createdAt":"2026-10-31T00:00:{:02}Z"}}"#, i % 60).into_bytes())
        .collect();
    let dict = Dict { id: 1, bytes: Arc::new(train_dict(&samples, 16 * 1024).expect("train")) };
    let packed = encode_record(json, Some(&dict)).expect("encode");
    assert_eq!(packed[0], 1);
    let dicts = HashMap::from([(1u8, dict)]);
    assert_eq!(decode_record(&packed, &dicts).expect("decode"), json);
    assert!(matches!(decode_record(&packed, &HashMap::new()), Err(CodecError::MissingDict(1))));
    assert!(matches!(decode_record(&[], &dicts), Err(CodecError::Empty)));
}

#[tokio::test]
async fn sql_helpers_agree_with_rust() {
    let Some(db) = fresh_db("compact_sql_helpers").await else { return };
    let p = &db.pool;
    for t in TIDS {
        let (valid, rkey, rev, rev_back): (bool, Option<i64>, i64, Option<String>) =
            sqlx::query_as("SELECT tid_is_valid($1), rkey_to_bigint($1), rev_to_bigint($1), CASE WHEN tid_is_valid($1) THEN bigint_to_rev(rev_to_bigint($1)) END")
                .bind(t)
                .fetch_one(p)
                .await
                .expect("helpers");
        assert_eq!(valid, proto_blue::syntax::Tid::is_valid(t), "{t}");
        assert_eq!(rkey, rkey_value(t), "{t}");
        assert_eq!(rev, rev_value(t), "{t}");
        assert_eq!(rev_back.as_deref(), valid.then_some(t), "{t}");
        if let Some(v) = rkey {
            let back: String = sqlx::query_scalar("SELECT bigint_to_rkey($1)").bind(v).fetch_one(p).await.expect("rkey back");
            assert_eq!(Some(back), rkey_string(v));
        }
    }
    // The bit-read text matches the numeric one at the edges.
    for v in [0, 1, 31, 32, (1 << 60) - 1, 1 << 60, i64::MAX - 1, i64::MAX, -1, i64::MIN, i64::MIN + 1, -(1 << 60), 1_820_642_958_438_400_004] {
        let (rev, rev_old): (String, String) =
            sqlx::query_as("SELECT bigint_to_rev($1), bigint_to_tid_numeric($1::numeric + 9223372036854775808)").bind(v).fetch_one(p).await.expect("rev text");
        assert_eq!(rev, rev_old, "{v}");
        if v >= 0 {
            let (rkey, rkey_old): (String, String) =
                sqlx::query_as("SELECT bigint_to_rkey($1), bigint_to_tid_numeric($1::numeric)").bind(v).fetch_one(p).await.expect("rkey text");
            assert_eq!(rkey, rkey_old, "{v}");
        }
    }
    let nulls: (Option<String>, Option<String>, Option<String>, Option<String>) =
        sqlx::query_as("SELECT bigint_to_rkey(NULL), bigint_to_rev(NULL), at_uri(NULL, 1::smallint, 0), at_uri(2147483647, 1::smallint, 0)")
            .fetch_one(p)
            .await
            .expect("nulls");
    assert_eq!(nulls, (None, None, None, None));
    let long = "a".repeat(512);
    let too_long = "a".repeat(513);
    for k in ["self", "3myp5txuf7s2d", ".", "..", "...", "a:b~c_d-e.f", "bad key", "", long.as_str(), too_long.as_str()] {
        let ok: Option<bool> = sqlx::query_scalar("SELECT rkey_is_valid($1)").bind(k).fetch_one(p).await.expect("rkey");
        assert_eq!(ok.unwrap_or(false), proto_blue::syntax::RecordKey::is_valid(k), "{k}");
    }
    for d in [A, "did:web:example.com", "did:m:v", "did:m:", "did:UPPER:x", "did:m:v!v", "alice.example"] {
        let ok: bool = sqlx::query_scalar("SELECT did_is_valid($1)").bind(d).fetch_one(p).await.expect("did");
        assert_eq!(ok, proto_blue::syntax::Did::is_valid(d), "{d}");
    }
    db.drop_db().await;
}

#[tokio::test]
async fn seeded_collections_match_the_list() {
    let Some(db) = fresh_db("compact_collections").await else { return };
    let rows: Vec<(i16, String)> = sqlx::query_as("SELECT id, nsid FROM collections ORDER BY id").fetch_all(&db.pool).await.expect("collections");
    let want: Vec<(i16, String)> = COLLECTIONS.iter().map(|(i, n)| (*i, (*n).to_string())).collect();
    assert_eq!(rows, want);
    db.drop_db().await;
}

#[tokio::test]
async fn interning_selects_first_and_survives_a_race() {
    let Some(db) = fresh_db("compact_interning").await else { return };
    let p = &db.pool;
    let ids = Ids::new(p.clone());
    let other = Ids::new(p.clone());
    let batch = vec![A.to_string(), B.to_string(), "alice.example".to_string()];
    let (x, y) = tokio::join!(ids.account_ids(&batch), other.account_ids(&batch));
    let (x, y) = (x.expect("first"), y.expect("second"));
    assert_eq!(x, y, "both writers get the same ids");
    assert_eq!(x.len(), 2, "an invalid DID is not interned");
    let rows: i64 = sqlx::query_scalar("SELECT count(*) FROM dids").fetch_one(p).await.expect("count");
    assert_eq!(rows, 2);

    // A known DID spends no sequence value.
    let before: i64 = sqlx::query_scalar("SELECT last_value FROM dids_id_seq").fetch_one(p).await.expect("seq");
    let fresh = Ids::new(p.clone());
    fresh.account_ids(&batch).await.expect("again");
    let after: i64 = sqlx::query_scalar("SELECT last_value FROM dids_id_seq").fetch_one(p).await.expect("seq");
    assert_eq!(before, after);

    // A writer's transaction that rolls back leaves no cached id missing.
    let mut tx = p.begin().await.expect("tx");
    let c = "did:plc:compactccccccccccccccccc".to_string();
    let got = ids.account_ids(std::slice::from_ref(&c)).await.expect("intern");
    sqlx::query("SELECT 1").execute(&mut *tx).await.expect("work");
    tx.rollback().await.expect("rollback");
    let exists: bool = sqlx::query_scalar("SELECT EXISTS (SELECT 1 FROM dids WHERE id = $1)").bind(got[&c]).fetch_one(p).await.expect("exists");
    assert!(exists, "the id was committed before the writer's transaction");
    db.drop_db().await;
}

/// Migration 0055 over rows in the old shape: every follow and block is
/// keyed and reads back as it was stored, odd `via` texts land in
/// `odd_refs`, counts are copied as they are, a row that cannot be keyed is
/// left out, and the new triggers work.
#[tokio::test]
async fn migration_0055_moves_follows_and_blocks() {
    let Some(db) = crate::test_support::empty_db("migration_0055").await else { return };
    let p = &db.pool;
    let migrations: Vec<_> = crate::db::migrations::MIGRATOR.iter().filter(|m| !m.migration_type.is_down_migration()).collect();
    let at = migrations.iter().position(|m| m.version == 55).expect("0055");
    for m in &migrations[..at] {
        sqlx::raw_sql(&m.sql).execute(p).await.unwrap_or_else(|e| panic!("{}: {e}", m.version));
    }
    let pack = format!("at://{B}/app.bsky.graph.starterpack/3myp5txuf7s2d");
    let odd = format!("at://{B}/com.example.pack/3myp5txuf7s2d/");
    let handle = "at://alice.example/app.bsky.graph.starterpack/self".to_string();
    let follow = |rkey: &str| format!("at://{A}/app.bsky.graph.follow/{rkey}");
    // (rkey, rev, via, via cid)
    let rows: [(&str, &str, Option<&str>, Option<&str>); 5] = [
        ("3myp5txuf7s2d", "3myp5txuf7s2d", Some(pack.as_str()), Some("bafyreidbcoifjdzsr3u5i74kbm427m5yic67grd54s4luhlt4xxntevd6m")),
        ("self", "3myp5txuf7s2e", Some(odd.as_str()), Some("BAFYODD")),
        ("c222222222222", "2222222222222", Some(handle.as_str()), None),
        ("3myp5txuf7s2f", "not-a-tid", None, None),
        ("3myp5txuf7s2g", "3myp5txuf7s2g", None, None),
    ];
    for (rkey, rev, via, cid) in rows {
        sqlx::query("INSERT INTO follows (uri, author_did, subject_did, rev, created_at, via_uri, via_cid) VALUES ($1, $2, $3, $4, '2026-10-01T00:00:00Z', $5, $6)")
            .bind(follow(rkey))
            .bind(A)
            .bind(if rkey == "3myp5txuf7s2g" { A } else { B })
            .bind(rev)
            .bind(via)
            .bind(cid)
            .execute(p)
            .await
            .expect("old follow");
    }
    // Not keyable: a subject that is not a DID, and a URI that is not the
    // author's own record.
    for (uri, subject) in [(follow("3myp5txuf7s2h"), "bob.example"), (format!("at://{B}/app.bsky.graph.follow/3myp5txuf7s2i"), B)] {
        sqlx::query("INSERT INTO follows (uri, author_did, subject_did, rev, created_at) VALUES ($1, $2, $3, '3k', now())")
            .bind(uri)
            .bind(A)
            .bind(subject)
            .execute(p)
            .await
            .expect("odd follow");
    }
    sqlx::query("INSERT INTO blocks (uri, author_did, subject_did, rev, created_at) VALUES ($1, $2, $3, '2222222222222', now()), ($4, $3, $2, '3myp5txuf7s2d', now())")
        .bind(format!("at://{A}/app.bsky.graph.block/3ka"))
        .bind(A)
        .bind(B)
        .bind(format!("at://{B}/app.bsky.graph.block/3myp5txuf7s2d"))
        .execute(p)
        .await
        .expect("old blocks");
    let counts = || async {
        let rows: Vec<(String, i32, i32)> = sqlx::query_as("SELECT did, follows_count, followers_count FROM actor_counts ORDER BY did")
            .fetch_all(p)
            .await
            .expect("counts");
        rows
    };
    let before = counts().await;

    sqlx::raw_sql(&migrations[at].sql).execute(p).await.expect("0055 applies over existing rows");

    assert_eq!(counts().await, before, "counts are copied as they are");
    type Row = (String, String, String, Option<String>, Option<Vec<u8>>, i16);
    let got: Vec<Row> = sqlx::query_as(
        "SELECT at_uri(f.author, 4::smallint, f.rkey), s.did, bigint_to_rev(f.rev), \
           CASE WHEN f.via_coll = 0 THEN (SELECT o.raw FROM odd_refs o WHERE o.tbl = 4 AND o.author = f.author AND o.rkey = f.rkey AND o.slot = 5) \
                WHEN f.via_coll IS NOT NULL THEN at_uri(f.via_author, f.via_coll, f.via_rkey) END, \
           f.via_cid, COALESCE(f.via_coll, -1::smallint) \
         FROM follows f JOIN dids s ON s.id = f.subject ORDER BY at_uri(f.author, 4::smallint, f.rkey) COLLATE \"C\"",
    )
    .fetch_all(p)
    .await
    .expect("follows");
    let want: Vec<Row> = {
        let mut w: Vec<Row> = rows
            .iter()
            .map(|(rkey, rev, via, cid)| {
                let rev = if proto_blue::syntax::Tid::is_valid(rev) { (*rev).to_string() } else { "2222222222222".to_string() };
                let subject = if *rkey == "3myp5txuf7s2g" { A } else { B };
                let coll = match *via {
                    None => -1,
                    Some(v) if v == pack => 11,
                    Some(_) => 0,
                };
                (follow(rkey), subject.to_string(), rev, via.map(str::to_string), cid.map(|c| [&[0u8][..], c.as_bytes()].concat()), coll)
            })
            .collect();
        w.sort();
        w
    };
    assert_eq!(got, want);
    for (_, _, _, _, cid, _) in &got {
        if let Some(c) = cid {
            assert_eq!(c[0], 0, "every CID is copied in its text form");
        }
    }
    let odd_rows: i64 = sqlx::query_scalar("SELECT count(*) FROM odd_refs WHERE tbl = 4 AND slot = 5").fetch_one(p).await.expect("odd");
    assert_eq!(odd_rows, 2);
    // A handle authority keeps no author; a valid DID's odd reference does.
    let authors: Vec<Option<i32>> = sqlx::query_scalar("SELECT via_author FROM follows WHERE via_coll = 0 ORDER BY rkey").fetch_all(p).await.expect("authors");
    assert_eq!(authors.iter().filter(|a| a.is_some()).count(), 1);
    assert!(!sqlx::query_scalar::<_, bool>("SELECT EXISTS (SELECT 1 FROM dids WHERE did = 'bob.example' OR did = 'alice.example')").fetch_one(p).await.expect("dids"));

    let blocks: Vec<(String, String, String)> = sqlx::query_as(
        "SELECT at_uri(b.author, 5::smallint, b.rkey), s.did, bigint_to_rev(b.rev) FROM blocks b JOIN dids s ON s.id = b.subject ORDER BY at_uri(b.author, 5::smallint, b.rkey) COLLATE \"C\"",
    )
    .fetch_all(p)
    .await
    .expect("blocks");
    assert_eq!(
        blocks,
        [
            (format!("at://{A}/app.bsky.graph.block/3ka"), B.to_string(), "2222222222222".to_string()),
            (format!("at://{B}/app.bsky.graph.block/3myp5txuf7s2d"), A.to_string(), "3myp5txuf7s2d".to_string()),
        ]
    );
    let sentinel: i64 = sqlx::query_scalar("SELECT min(rev) FROM blocks").fetch_one(p).await.expect("rev");
    assert_eq!(sentinel, i64::MIN, "the sentinel rev is the lowest value");

    // The new triggers: a follow counts, and an odd one's text goes with it.
    let a: i32 = sqlx::query_scalar("SELECT id FROM dids WHERE did = $1").bind(A).fetch_one(p).await.expect("a");
    let b: i32 = sqlx::query_scalar("SELECT id FROM dids WHERE did = $1").bind(B).fetch_one(p).await.expect("b");
    sqlx::query("INSERT INTO follows (author, rkey, subject, rev, created_at) VALUES ($1, 1, $2, 0, now())").bind(b).bind(a).execute(p).await.expect("new follow");
    let after = counts().await;
    let of = |did: &str| after.iter().find(|r| r.0 == did).map(|r| (r.1, r.2));
    let was = |did: &str| before.iter().find(|r| r.0 == did).map_or((0, 0), |r| (r.1, r.2));
    assert_eq!(of(B), Some((was(B).0 + 1, was(B).1)));
    assert_eq!(of(A), Some((was(A).0, was(A).1 + 1)));
    sqlx::query("DELETE FROM follows WHERE via_coll = 0").execute(p).await.expect("delete odd");
    let left: i64 = sqlx::query_scalar("SELECT count(*) FROM odd_refs").fetch_one(p).await.expect("odd left");
    assert_eq!(left, 0, "odd texts go with their rows");
    db.drop_db().await;
}

/// Migration 0056 over rows in the old shape: every like and repost is
/// keyed and its subject and `via` read back as stored, odd texts land in
/// `odd_refs`, post counts are copied for whole posts only and the rest
/// dropped, account counts are keyed by id, a row that cannot be keyed is
/// left out, and the new triggers work.
#[tokio::test]
async fn migration_0056_moves_likes_reposts_and_counts() {
    const C: &str = "did:plc:compactccccccccccccccccc";
    let Some(db) = crate::test_support::empty_db("migration_0056").await else { return };
    let p = &db.pool;
    let migrations: Vec<_> = crate::db::migrations::MIGRATOR.iter().filter(|m| !m.migration_type.is_down_migration()).collect();
    let at = migrations.iter().position(|m| m.version == 56).expect("0056");
    for m in &migrations[..at] {
        sqlx::raw_sql(&m.sql).execute(p).await.unwrap_or_else(|e| panic!("{}: {e}", m.version));
    }
    let post = |rkey: &str| format!("at://{B}/app.bsky.feed.post/{rkey}");
    let (liked, kept, by_alias) = (post("3myp5txuf7s2d"), post("3myp5txuf7s2h"), post("self"));
    let no_rkey = format!("at://{B}/app.bsky.feed.post");
    let handle = "at://alice.example/app.bsky.feed.post/3myp5txuf7s2d".to_string();
    let list = format!("at://{B}/app.bsky.graph.list/3myp5txuf7s2d");
    let private = format!("at://{B}/tools.kryphocron.feed.postPrivate/3myp5txuf7s2d");
    let via_whole = format!("at://{C}/app.bsky.feed.repost/self");
    let via_odd = format!("at://{B}/com.example.thing/1");
    let canonical = "bafyreidbcoifjdzsr3u5i74kbm427m5yic67grd54s4luhlt4xxntevd6m";
    let like = |rkey: &str| format!("at://{A}/app.bsky.feed.like/{rkey}");
    // (rkey, rev, subject, subject cid, via, via cid, subject coll)
    type Old<'a> = (&'a str, &'a str, &'a str, Option<&'a str>, Option<&'a str>, Option<&'a str>, i16);
    let likes: [Old; 4] = [
        ("3myp5txuf7s2d", "3myp5txuf7s2d", liked.as_str(), Some(canonical), Some(via_whole.as_str()), Some("BAFYVIA"), 1),
        ("self", "3myp5txuf7s2e", no_rkey.as_str(), None, None, None, 0),
        ("3myp5txuf7s2e", "3myp5txuf7s2e", handle.as_str(), Some("bafyodd"), Some(via_odd.as_str()), None, 0),
        ("3myp5txuf7s2f", "not-a-tid", list.as_str(), None, None, None, 7),
    ];
    for (rkey, rev, subject, cid, via, via_cid, _) in likes {
        sqlx::query(
            "INSERT INTO likes (uri, author_did, subject_uri, subject_cid, rev, created_at, via_uri, via_cid) \
             VALUES ($1, $2, $3, $4, $5, '2026-10-01T00:00:00Z', $6, $7)",
        )
        .bind(like(rkey))
        .bind(A)
        .bind(subject)
        .bind(cid)
        .bind(rev)
        .bind(via)
        .bind(via_cid)
        .execute(p)
        .await
        .expect("old like");
    }
    // Not keyable: a URI that is not the author's own record. Its subject's
    // count row is still copied.
    sqlx::query("INSERT INTO likes (uri, author_did, subject_uri, rev, created_at) VALUES ($1, $2, $3, '3k', now())")
        .bind(format!("at://{B}/app.bsky.feed.like/3myp5txuf7s2g"))
        .bind(A)
        .bind(&kept)
        .execute(p)
        .await
        .expect("odd like");
    for (rkey, subject) in [("3myp5txuf7s2d", &private), ("3myp5txuf7s2e", &by_alias)] {
        sqlx::query("INSERT INTO reposts (uri, author_did, subject_uri, rev, created_at) VALUES ($1, $2, $3, '3myp5txuf7s2d', now())")
            .bind(format!("at://{A}/app.bsky.feed.repost/{rkey}"))
            .bind(A)
            .bind(subject)
            .execute(p)
            .await
            .expect("old repost");
    }
    sqlx::query("INSERT INTO actor_counts (did, followers_count, follows_count, posts_count) VALUES ($1, 3, 2, 1), ('bob.example', 1, 1, 1)")
        .bind(C)
        .execute(p)
        .await
        .expect("old actor counts");

    sqlx::raw_sql(&migrations[at].sql).execute(p).await.expect("0056 applies over existing rows");

    type Row = (String, Option<String>, i16, Option<Vec<u8>>, String, Option<String>, Option<Vec<u8>>, bool);
    let got: Vec<Row> = sqlx::query_as(
        "SELECT at_uri(l.author, 2::smallint, l.rkey), \
           ref_text(l.subj_author, l.subj_coll, l.subj_rkey, 2::smallint, l.author, l.rkey, 4::smallint), l.subj_coll, l.subj_cid, \
           bigint_to_rev(l.rev), ref_text(l.via_author, l.via_coll, l.via_rkey, 2::smallint, l.author, l.rkey, 5::smallint), l.via_cid, \
           l.subj_author IS NOT NULL \
         FROM likes l ORDER BY at_uri(l.author, 2::smallint, l.rkey) COLLATE \"C\"",
    )
    .fetch_all(p)
    .await
    .expect("likes");
    let text = |c: Option<&str>| c.map(|c| [&[0u8][..], c.as_bytes()].concat());
    let mut want: Vec<Row> = likes
        .iter()
        .map(|(rkey, rev, subject, cid, via, via_cid, coll)| {
            let rev = if proto_blue::syntax::Tid::is_valid(rev) { (*rev).to_string() } else { "2222222222222".to_string() };
            (like(rkey), Some((*subject).to_string()), *coll, text(*cid), rev, via.map(str::to_string), text(*via_cid), *subject != handle)
        })
        .collect();
    want.sort_by(|a, b| a.0.cmp(&b.0));
    assert_eq!(got, want);
    let odd: Vec<(i16, i16)> = sqlx::query_as("SELECT tbl, slot FROM odd_refs ORDER BY 1, 2").fetch_all(p).await.expect("odd");
    assert_eq!(odd, [(2, 4), (2, 4), (2, 5)], "two odd subjects and one odd via");
    assert!(!sqlx::query_scalar::<_, bool>("SELECT EXISTS (SELECT 1 FROM dids WHERE did IN ('alice.example', 'bob.example'))").fetch_one(p).await.expect("dids"));

    let reposts: Vec<(String, i16)> = sqlx::query_as(
        "SELECT ref_text(r.subj_author, r.subj_coll, r.subj_rkey, 3::smallint, r.author, r.rkey, 4::smallint), r.subj_coll FROM reposts r ORDER BY 2",
    )
    .fetch_all(p)
    .await
    .expect("reposts");
    assert_eq!(reposts, [(by_alias.clone(), 1), (private.clone(), 27)]);

    // Counts of whole posts only: the list, the odd texts and the private
    // post are dropped.
    let count_of = |uri: String| async move {
        sqlx::query_as::<_, (i32, i32)>(
            "SELECT c.like_count, c.repost_count FROM post_counts c, ref_of($1) r WHERE r.coll = 1 AND c.author = r.author AND c.rkey = r.rkey",
        )
        .bind(uri)
        .fetch_optional(p)
        .await
        .expect("count")
    };
    assert_eq!(count_of(liked.clone()).await, Some((1, 0)));
    assert_eq!(count_of(by_alias.clone()).await, Some((0, 1)));
    assert_eq!(count_of(kept.clone()).await, Some((1, 0)));
    let rows: i64 = sqlx::query_scalar("SELECT count(*) FROM post_counts").fetch_one(p).await.expect("rows");
    assert_eq!(rows, 3);
    let actor: Vec<(String, i32, i32, i32)> = sqlx::query_as(
        "SELECT d.did, c.followers_count, c.follows_count, c.posts_count FROM actor_counts c JOIN dids d ON d.id = c.id ORDER BY 1",
    )
    .fetch_all(p)
    .await
    .expect("actor counts");
    assert_eq!(actor, [(C.to_string(), 3, 2, 1)], "an account that is not a valid DID is dropped");

    // The new triggers: a like counts by account, a follow and a reply
    // count, and an odd row's texts go with it.
    let id = |did: &'static str| async move { sqlx::query_scalar::<_, i32>("SELECT id FROM dids WHERE did = $1").bind(did).fetch_one(p).await.expect("id") };
    let (a, b, c) = (id(A).await, id(B).await, id(C).await);
    let liked_rkey = rkey_value("3myp5txuf7s2d").expect("tid");
    sqlx::query("INSERT INTO likes (author, rkey, subj_author, subj_coll, subj_rkey, rev, created_at) VALUES ($1, 1, $2, 1, $3, 0, now())")
        .bind(c)
        .bind(b)
        .bind(liked_rkey)
        .execute(p)
        .await
        .expect("new like");
    assert_eq!(count_of(liked.clone()).await, Some((2, 0)));
    // A like of a list with the post's key leaves the post's count alone.
    sqlx::query("INSERT INTO likes (author, rkey, subj_author, subj_coll, subj_rkey, rev, created_at) VALUES ($1, 2, $2, 7, $3, 0, now())")
        .bind(c)
        .bind(b)
        .bind(liked_rkey)
        .execute(p)
        .await
        .expect("like of a list");
    sqlx::query("INSERT INTO likes (author, rkey, subj_author, subj_coll, subj_rkey, rev, created_at) VALUES ($1, 3, $2, 7, $3, 0, now())")
        .bind(a)
        .bind(b)
        .bind(liked_rkey)
        .execute(p)
        .await
        .expect("another like of a list");
    assert_eq!(count_of(liked.clone()).await, Some((2, 0)));
    sqlx::query("INSERT INTO follows (author, rkey, subject, rev, created_at) VALUES ($1, 1, $2, 0, now())").bind(a).bind(c).execute(p).await.expect("follow");
    let followers: i32 = sqlx::query_scalar("SELECT followers_count FROM actor_counts WHERE id = $1").bind(c).fetch_one(p).await.expect("followers");
    assert_eq!(followers, 4);
    sqlx::query(
        "INSERT INTO posts (uri, author_did, rkey, cid, rev, record, created_at, reply_parent_uri, reply_root_uri) \
         VALUES ($1, $2, '3myp5txuf7s2k', 'c', '3myp5txuf7s2k', '{}'::jsonb, now(), $3, $3)",
    )
    .bind(format!("at://{A}/app.bsky.feed.post/3myp5txuf7s2k"))
    .bind(A)
    .bind(&liked)
    .execute(p)
    .await
    .expect("reply");
    let replies: i32 = sqlx::query_scalar("SELECT reply_count FROM post_counts WHERE author = $1 AND rkey = $2").bind(b).bind(liked_rkey).fetch_one(p).await.expect("replies");
    assert_eq!(replies, 1);
    let posts: i32 = sqlx::query_scalar("SELECT posts_count FROM actor_counts WHERE id = $1").bind(a).fetch_one(p).await.expect("posts");
    assert_eq!(posts, 1);
    sqlx::query("DELETE FROM likes WHERE subj_coll = 0").execute(p).await.expect("delete odd");
    let left: i64 = sqlx::query_scalar("SELECT count(*) FROM odd_refs").fetch_one(p).await.expect("odd left");
    assert_eq!(left, 0, "odd texts go with their rows");
    db.drop_db().await;
}

#[tokio::test]
async fn aliases_are_per_author_and_negative() {
    let Some(db) = fresh_db("compact_aliases").await else { return };
    let p = &db.pool;
    let ids = Ids::new(p.clone());
    let a = ids.account_ids(&[A.to_string()]).await.expect("a")[A];
    let keys = vec![(a, "self".to_string()), (a, "3myp5txuf7s2d".to_string()), (a, "c222222222222".to_string()), (a, "bad key".to_string())];
    let other = Ids::new(p.clone());
    let (x, y) = tokio::join!(ids.rkey_ids(&keys), other.rkey_ids(&keys));
    let (x, y) = (x.expect("first"), y.expect("second"));
    assert_eq!(x, y);
    assert_eq!(x.len(), 3, "an invalid record key gets no id");
    assert!(x[&(a, "self".to_string())] < 0);
    assert!(x[&(a, "c222222222222".to_string())] < 0);
    assert_eq!(x[&(a, "3myp5txuf7s2d".to_string())], rkey_value("3myp5txuf7s2d").expect("tid"));
    let back: String = sqlx::query_scalar("SELECT bigint_to_rkey($1)").bind(x[&(a, "self".to_string())]).fetch_one(p).await.expect("back");
    assert_eq!(back, "self");

    // ref_of agrees with uri_parts and is strict.
    let whole = format!("at://{A}/app.bsky.labeler.service/self");
    let (author, coll, rkey): (Option<i32>, Option<i16>, Option<i64>) = sqlx::query_as("SELECT * FROM ref_of($1)").bind(&whole).fetch_one(p).await.expect("ref_of");
    assert_eq!((author, coll, rkey), (Some(a), Some(12), Some(x[&(a, "self".to_string())])));
    for odd in [format!("{whole}/"), format!("at://{A}/app.bsky.feed.post"), format!("at://{A}/com.example.x/self")] {
        let (author, coll, rkey): (Option<i32>, Option<i16>, Option<i64>) = sqlx::query_as("SELECT * FROM ref_of($1)").bind(&odd).fetch_one(p).await.expect("ref_of");
        assert_eq!((author, coll, rkey), (Some(a), None, None), "{odd}");
    }
    db.drop_db().await;
}

/// Migration 0057: posts, their search rows, feed items and tombstones are
/// keyed by ids; references are whole or odd (their text in `odd_refs`,
/// their author kept when it is a valid DID); records and CIDs are kept in
/// their exact text form; the embed kind agrees with `db::posts`; search rows
/// are kept for S and members only; feed items of anything but a whole post
/// and tombstones outside the fixed collections are dropped; the triggers
/// work on the new keys.
#[tokio::test]
async fn migration_0057_moves_posts_feed_items_and_tombstones() {
    use serde_json::{Value, json};

    let Some(db) = crate::test_support::empty_db("migration_0057").await else { return };
    let p = &db.pool;
    let migrations: Vec<_> = crate::db::migrations::MIGRATOR.iter().filter(|m| !m.migration_type.is_down_migration()).collect();
    let at = migrations.iter().position(|m| m.version == 57).expect("0057");
    for m in &migrations[..at] {
        sqlx::raw_sql(&m.sql).execute(p).await.unwrap_or_else(|e| panic!("{}: {e}", m.version));
    }
    let post = |did: &str, rkey: &str| format!("at://{did}/app.bsky.feed.post/{rkey}");
    let canonical = "bafyreidbcoifjdzsr3u5i74kbm427m5yic67grd54s4luhlt4xxntevd6m";
    sqlx::query("INSERT INTO scope_dids (did, reasons) VALUES ($1, 1)").bind(A).execute(p).await.expect("scope");
    let old_post = |uri: String, author: &'static str, rkey: &'static str, rev: &'static str, refs: (Option<String>, Option<String>, Option<String>), embed: Option<Value>| {
        let (root, parent, quote) = refs;
        async move {
            let record = json!({"$type": "app.bsky.feed.post", "text": format!("text of {rkey}"), "embed": embed.clone()});
            sqlx::query(
                "INSERT INTO posts (uri, author_did, rkey, cid, rev, text, reply_root_uri, reply_parent_uri, quote_uri, embed, \
                   mention_dids, record, created_at, tags) \
                 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, '2026-10-01T00:00:00Z', ARRAY['cats'])",
            )
            .bind(&uri)
            .bind(author)
            .bind(rkey)
            .bind(canonical)
            .bind(rev)
            .bind(format!("text of {rkey}"))
            .bind(root)
            .bind(parent)
            .bind(quote)
            .bind(embed)
            .bind(vec![B.to_string(), "not a did".to_string()])
            .bind(&record)
            .execute(p)
            .await
            .expect("old post");
            record
        }
    };
    let whole = (Some(post(B, "3myp5txuf7s2e")), Some(post(B, "3myp5txuf7s2e")), Some(format!("at://{B}/app.bsky.graph.list/3myp5txuf7s2d")));
    let rwm = json!({"$type": "app.bsky.embed.recordWithMedia", "media": {"$type": "app.bsky.embed.images"}});
    let first = old_post(post(A, "3myp5txuf7s2d"), A, "3myp5txuf7s2d", "3myp5txuf7s2d", whole, Some(rwm.clone())).await;
    let odd_parent = format!("at://{B}/com.example.thing/1");
    let odd_root = "at://alice.example/app.bsky.feed.post/3myp5txuf7s2e".to_string();
    old_post(post(A, "self"), A, "self", "not-a-tid", (Some(odd_root.clone()), Some(odd_parent.clone()), None), None).await;
    old_post(post(B, "3myp5txuf7s2e"), B, "3myp5txuf7s2e", "3myp5txuf7s2e", (None, None, None), None).await;
    // Not keyable: a URI that is not its author's own record.
    old_post(post(B, "3myp5txuf7s2f"), A, "3myp5txuf7s2f", "3myp5txuf7s2f", (None, None, None), None).await;
    // Each embed kind, by B (in neither S nor members: no search row).
    let embeds = [
        json!({"$type": "app.bsky.embed.images"}),
        json!({"$type": "app.bsky.embed.video"}),
        json!({"$type": "app.bsky.embed.gallery"}),
        json!({"$type": "app.bsky.embed.external"}),
        json!({"$type": "app.bsky.embed.record"}),
        json!({"$type": "app.bsky.embed.recordWithMedia", "media": {"$type": "app.bsky.embed.video"}}),
        json!({"$type": "app.bsky.embed.recordWithMedia", "media": {"$type": "app.bsky.embed.gallery"}}),
        json!({"$type": "app.bsky.embed.recordWithMedia", "media": {"$type": "app.bsky.embed.external"}}),
        json!({"$type": "app.bsky.embed.recordWithMedia"}),
        json!({"$type": "app.example.other"}),
    ];
    let embed_rkeys = ["3myp5txuf7s3a", "3myp5txuf7s3b", "3myp5txuf7s3c", "3myp5txuf7s3d", "3myp5txuf7s3e", "3myp5txuf7s3f", "3myp5txuf7s3g", "3myp5txuf7s3h", "3myp5txuf7s3i", "3myp5txuf7s3j"];
    for (rkey, embed) in embed_rkeys.iter().zip(&embeds) {
        old_post(post(B, rkey), B, rkey, "3myp5txuf7s2d", (None, None, None), Some(embed.clone())).await;
    }
    let repost = format!("at://{A}/app.bsky.feed.repost/3myp5txuf7s2f");
    for (uri, post_uri, kind) in [
        (post(A, "3myp5txuf7s2d"), post(A, "3myp5txuf7s2d"), "reply"),
        (repost.clone(), post(B, "3myp5txuf7s2e"), "repost"),
        (format!("at://{A}/app.bsky.feed.repost/3myp5txuf7s2g"), format!("at://{B}/app.bsky.graph.list/3myp5txuf7s2d"), "repost"),
    ] {
        sqlx::query("INSERT INTO feed_items (uri, originator_did, post_uri, kind, sort_at) VALUES ($1, $2, $3, $4, now())")
            .bind(uri)
            .bind(A)
            .bind(post_uri)
            .bind(kind)
            .execute(p)
            .await
            .expect("old feed item");
    }
    for (uri, rev) in [
        (post(A, "3myp5txuf7s2z"), "3myp5txuf7s2z"),
        (format!("at://{A}/app.bsky.actor.profile/self"), "3myp5txuf7s2y"),
        (format!("at://{A}/com.example.thing/1"), "3myp5txuf7s2x"),
    ] {
        sqlx::query("INSERT INTO record_tombstones (uri, rev) VALUES ($1, $2)").bind(uri).bind(rev).execute(p).await.expect("old tombstone");
    }

    sqlx::raw_sql(&migrations[at].sql).execute(p).await.expect("0057 applies over existing rows");

    type Row = (String, String, i16, Option<String>, Option<String>, Option<String>, Option<i16>, Option<i16>, Option<i16>, Option<i32>, Vec<u8>, Vec<u8>, Option<Vec<i32>>);
    let row = |uri: String| async move {
        let r: Row = sqlx::query_as(
            "SELECT at_uri(p.author, 1::smallint, p.rkey), bigint_to_rev(p.rev), p.embed_kind, \
               ref_text(p.root_author, p.root_coll, p.root_rkey, 1::smallint, p.author, p.rkey, 1::smallint), \
               ref_text(p.parent_author, p.parent_coll, p.parent_rkey, 1::smallint, p.author, p.rkey, 2::smallint), \
               ref_text(p.quote_author, p.quote_coll, p.quote_rkey, 1::smallint, p.author, p.rkey, 3::smallint), \
               p.root_coll, p.parent_coll, p.quote_coll, p.parent_author, p.cid, p.record, p.mention_ids \
             FROM posts p, ref_of($1) k WHERE p.author = k.author AND p.rkey = k.rkey",
        )
        .bind(uri)
        .fetch_one(p)
        .await
        .expect("post row");
        r
    };
    let id = |did: &'static str| async move { sqlx::query_scalar::<_, i32>("SELECT id FROM dids WHERE did = $1").bind(did).fetch_one(p).await.expect("id") };
    let (a, b) = (id(A).await, id(B).await);
    let text = |s: &str| [&[0u8][..], s.as_bytes()].concat();

    let r = row(post(A, "3myp5txuf7s2d")).await;
    assert_eq!((r.0.as_str(), r.1.as_str(), r.2), (post(A, "3myp5txuf7s2d").as_str(), "3myp5txuf7s2d", crate::db::posts::embed_kind(Some(&rwm))));
    assert_eq!((r.3, r.4, r.5), (Some(post(B, "3myp5txuf7s2e")), Some(post(B, "3myp5txuf7s2e")), Some(format!("at://{B}/app.bsky.graph.list/3myp5txuf7s2d"))));
    assert_eq!((r.6, r.7, r.8, r.9), (Some(1), Some(1), Some(7), Some(b)));
    assert_eq!(r.10, text(canonical), "the CID in its exact text form");
    assert_eq!(r.11[0], 0, "the record as dict 0");
    let stored: Value = serde_json::from_slice(&r.11[1..]).expect("record JSON");
    assert_eq!(stored, first);
    assert_eq!(r.12, Some(vec![b]), "a mention that is not a valid DID is dropped");

    // Odd references: their text in `odd_refs`, the author kept when the
    // authority is a valid DID; a rev that is not a TID is the lowest.
    let r = row(post(A, "self")).await;
    assert_eq!((r.1.as_str(), r.3, r.4, r.5), ("2222222222222", Some(odd_root.clone()), Some(odd_parent.clone()), None));
    assert_eq!((r.6, r.7, r.8, r.9), (Some(0), Some(0), None, Some(b)));
    let odd: Vec<(i16, i16, String)> = sqlx::query_as("SELECT tbl, slot, raw FROM odd_refs ORDER BY tbl, slot").fetch_all(p).await.expect("odd");
    assert_eq!(odd, [(1, 1, odd_root.clone()), (1, 2, odd_parent.clone())]);

    // Every embed kind, as `db::posts` names it.
    for (rkey, embed) in embed_rkeys.iter().zip(&embeds) {
        assert_eq!(row(post(B, rkey)).await.2, crate::db::posts::embed_kind(Some(embed)), "{embed}");
    }
    let posts: i64 = sqlx::query_scalar("SELECT count(*) FROM posts").fetch_one(p).await.expect("posts");
    assert_eq!(posts, 13, "the unkeyable row is left out");

    // Search rows for S's posts only, the text vector filled.
    let search: Vec<(String, Vec<String>, bool)> = sqlx::query_as(
        "SELECT at_uri(author, 1::smallint, rkey), tags, search_tsv @@ to_tsquery('simple', 'text') FROM post_search ORDER BY 1",
    )
    .fetch_all(p)
    .await
    .expect("search");
    assert_eq!(
        search,
        [(post(A, "3myp5txuf7s2d"), vec!["cats".to_string()], true), (post(A, "self"), vec!["cats".to_string()], true)]
    );

    // Feed items of whole posts only.
    let items: Vec<(i32, bool, i16, String)> = sqlx::query_as(
        "SELECT originator, is_repost, kind, at_uri(post_author, 1::smallint, post_rkey) FROM feed_items ORDER BY kind",
    )
    .fetch_all(p)
    .await
    .expect("feed items");
    assert_eq!(items, [(a, false, 2, post(A, "3myp5txuf7s2d")), (a, true, 3, post(B, "3myp5txuf7s2e"))]);

    // Tombstones in the fixed collections, an alias for `self`.
    let tombstones: Vec<(String, String)> =
        sqlx::query_as("SELECT at_uri(author, coll, rkey), bigint_to_rev(rev) FROM record_tombstones ORDER BY 1").fetch_all(p).await.expect("tombstones");
    assert_eq!(
        tombstones,
        [(format!("at://{A}/app.bsky.actor.profile/self"), "3myp5txuf7s2y".to_string()), (post(A, "3myp5txuf7s2z"), "3myp5txuf7s2z".to_string())]
    );

    // The triggers: a new reply counts toward its parent; a deleted post
    // takes its odd texts and its search row with it.
    let liked_rkey = rkey_value("3myp5txuf7s2e").expect("tid");
    let replies = || async move {
        sqlx::query_scalar::<_, Option<i32>>("SELECT reply_count FROM post_counts WHERE author = $1 AND rkey = $2")
            .bind(b)
            .bind(liked_rkey)
            .fetch_optional(p)
            .await
            .expect("count")
            .flatten()
            .unwrap_or(0)
    };
    let before = replies().await;
    sqlx::query(
        "INSERT INTO posts (author, rkey, cid, rev, created_at, parent_author, parent_coll, parent_rkey, record) \
         VALUES ($1, $2, '\\x00'::bytea, 0, now(), $3, 1, $4, '\\x007b7d'::bytea)",
    )
    .bind(a)
    .bind(rkey_value("3myp5txuf7s4a").expect("tid"))
    .bind(b)
    .bind(liked_rkey)
    .execute(p)
    .await
    .expect("reply");
    assert_eq!(replies().await, before + 1);
    sqlx::query("DELETE FROM posts p USING ref_of($1) k WHERE p.author = k.author AND p.rkey = k.rkey").bind(post(A, "self")).execute(p).await.expect("delete");
    let left: (i64, i64) = sqlx::query_as("SELECT (SELECT count(*) FROM odd_refs), (SELECT count(*) FROM post_search)").fetch_one(p).await.expect("left");
    assert_eq!(left, (0, 1));
    db.drop_db().await;
}

/// Migration 0059: the list, pack, gate, poll, labeler, self-record and
/// post-reference tables are keyed by ids; references are whole or odd
/// (their text in `odd_refs`, their author kept when it is a valid DID);
/// records and CIDs are kept in their exact text form; the fields queries
/// read are copied; rows that cannot be keyed are left out, and pack counts
/// of anything but a whole pack are dropped; an odd reference's text goes
/// with its row.
#[tokio::test]
async fn migration_0059_moves_the_record_tables() {
    use serde_json::{Value, json};

    let Some(db) = crate::test_support::empty_db("migration_0059").await else { return };
    let p = &db.pool;
    let migrations: Vec<_> = crate::db::migrations::MIGRATOR.iter().filter(|m| !m.migration_type.is_down_migration()).collect();
    let at = migrations.iter().position(|m| m.version == 59).expect("0059");
    for m in &migrations[..at] {
        sqlx::raw_sql(&m.sql).execute(p).await.unwrap_or_else(|e| panic!("{}: {e}", m.version));
    }
    let canonical = "bafyreidbcoifjdzsr3u5i74kbm427m5yic67grd54s4luhlt4xxntevd6m";
    let uri = |did: &str, nsid: &str, rkey: &str| format!("at://{did}/{nsid}/{rkey}");
    let list = uri(A, "app.bsky.graph.list", "3myp5txuf7s2d");
    let list_record = json!({"$type": "app.bsky.graph.list", "name": "Mods", "purpose": "app.bsky.graph.defs#modlist"});
    let exec = |sql: String| async move { sqlx::raw_sql(&sql).execute(p).await.unwrap_or_else(|e| panic!("{sql}: {e}")) };
    exec(format!(
        "INSERT INTO lists (uri, creator_did, rkey, cid, rev, name, purpose, record, created_at) VALUES \
           ('{list}', '{A}', '3myp5txuf7s2d', '{canonical}', '3myp5txuf7s2d', 'Mods', 'app.bsky.graph.defs#modlist', '{list_record}', '2026-10-01T00:00:00Z'), \
           ('{}', '{B}', '3myp5txuf7s2e', 'c', '3myp5txuf7s2e', 'Not B''s', 'p', '{{}}', now())",
        uri(A, "app.bsky.graph.list", "3myp5txuf7s2e")
    ))
    .await;
    exec(format!(
        "INSERT INTO list_items (uri, author_did, rkey, cid, rev, list_uri, subject_did, created_at) VALUES \
           ('{}', '{A}', 'self', 'c', 'not-a-tid', '{list}', '{B}', now()), \
           ('{}', '{A}', '3myp5txuf7s2f', 'c', '3myp5txuf7s2f', '{}', '{B}', now())",
        uri(A, "app.bsky.graph.listitem", "self"),
        uri(A, "app.bsky.graph.listitem", "3myp5txuf7s2f"),
        uri(B, "app.bsky.graph.list", "3myp5txuf7s2d")
    ))
    .await;
    let odd_list = "at://alice.example/app.bsky.graph.list/3myp5txuf7s2d";
    exec(format!(
        "INSERT INTO list_blocks (uri, author_did, rkey, cid, rev, list_uri, created_at) VALUES \
           ('{}', '{B}', '3myp5txuf7s2d', 'c', '3myp5txuf7s2d', '{list}', now()), \
           ('{}', '{B}', '3myp5txuf7s2e', 'c', '3myp5txuf7s2e', '{odd_list}', now()); \
         INSERT INTO list_optouts (uri, author_did, rkey, cid, rev, list_uri, created_at) VALUES \
           ('{}', '{B}', '3myp5txuf7s2d', 'c', '3myp5txuf7s2d', '{list}', now())",
        uri(B, "app.bsky.graph.listblock", "3myp5txuf7s2d"),
        uri(B, "app.bsky.graph.listblock", "3myp5txuf7s2e"),
        uri(B, "app.bsky.graph.referencelistoptout", "3myp5txuf7s2d")
    ))
    .await;
    let pack = uri(A, "app.bsky.graph.starterpack", "3myp5txuf7s2d");
    let pack_record = json!({"$type": "app.bsky.graph.starterpack", "name": "Rust", "list": list});
    exec(format!(
        "INSERT INTO starter_packs (uri, creator_did, rkey, cid, rev, name, description, list_uri, record, created_at) VALUES \
           ('{pack}', '{A}', '3myp5txuf7s2d', 'c', '3myp5txuf7s2d', 'Rust', 'people', '{list}', '{pack_record}', now()); \
         INSERT INTO starter_pack_counts (pack_uri, joined_week_count, joined_all_time_count, all_time_source, refreshed_at) VALUES \
           ('{pack}', 2, 5, 'local', now()), ('{list}', 1, 1, 'local', now())"
    ))
    .await;
    let post = uri(A, "app.bsky.feed.post", "3myp5txuf7s2d");
    let gate_record = json!({"$type": "app.bsky.feed.threadgate", "post": post, "allow": []});
    exec(format!(
        "INSERT INTO threadgates (uri, author_did, rkey, cid, rev, post_uri, allow, record, created_at) VALUES \
           ('{}', '{A}', '3myp5txuf7s2d', 'c', '3myp5txuf7s2d', '{post}', '[]', '{gate_record}', now()); \
         INSERT INTO postgates (uri, author_did, rkey, cid, rev, post_uri, record, created_at) VALUES \
           ('{}', '{A}', '3myp5txuf7s2d', 'c', '3myp5txuf7s2d', 'at://{A}/com.example.thing/1', '{{}}', now())",
        uri(A, "app.bsky.feed.threadgate", "3myp5txuf7s2d"),
        uri(A, "app.bsky.feed.postgate", "3myp5txuf7s2d")
    ))
    .await;
    exec(format!(
        "INSERT INTO nearhorizon_poll_posts (uri, author_did, rkey, cid, rev, text, options, visibility, record, created_at) VALUES \
           ('{}', '{A}', '3myp5txuf7s2d', 'c', '3myp5txuf7s2d', 'q', '[]', 'public', '{{\"text\": \"q\"}}', now()); \
         INSERT INTO labeler_services (uri, creator_did, cid, rev, policies, record, created_at) VALUES \
           ('{}', '{A}', 'c', '3myp5txuf7s2d', '{{}}', '{{\"policies\": {{}}}}', now()); \
         INSERT INTO actor_self_records (uri, did, collection, cid, rev, record) VALUES \
           ('{}', '{A}', 'app.bsky.notification.declaration', 'c', '3myp5txuf7s2d', '{{\"allowSubscriptions\": \"mutuals\"}}'), \
           ('{}', '{A}', 'app.bsky.actor.contentVisibilityDeclaration', 'c', '3myp5txuf7s2d', '{{\"hideFromAlgorithmicRecommendations\": true}}'), \
           ('{}', '{A}', 'com.example.self', 'c', '3myp5txuf7s2d', '{{}}')",
        uri(A, "app.nearhorizon.poll.post", "3myp5txuf7s2d"),
        uri(A, "app.bsky.labeler.service", "self"),
        uri(A, "app.bsky.notification.declaration", "self"),
        uri(A, "app.bsky.actor.contentVisibilityDeclaration", "self"),
        uri(A, "com.example.self", "self")
    ))
    .await;
    exec(format!(
        "INSERT INTO member_post_refs (uri, author_did, rev, reply_parent_did, reply_root_did, quote_did, created_at) VALUES \
           ('{post}', '{A}', '3myp5txuf7s2d', '{B}', 'alice.example', NULL, now())"
    ))
    .await;

    sqlx::raw_sql(&migrations[at].sql).execute(p).await.expect("0059 applies over existing rows");

    let id = |did: &'static str| async move { sqlx::query_scalar::<_, i32>("SELECT id FROM dids WHERE did = $1").bind(did).fetch_one(p).await.expect("id") };
    let (a, b) = (id(A).await, id(B).await);
    let text = |s: &str| [&[0u8][..], s.as_bytes()].concat();
    let k = |s: &str| rkey_value(s).expect("tid");

    // Lists: keyed, the CID and record in their text form, the purpose kept;
    // a row that is not its author's own record is left out.
    type ListRow = (i32, i64, Vec<u8>, String, String, Vec<u8>);
    let lists: Vec<ListRow> =
        sqlx::query_as("SELECT author, rkey, cid, bigint_to_rev(rev), purpose, record FROM lists").fetch_all(p).await.expect("lists");
    assert_eq!(lists.len(), 1);
    assert_eq!((lists[0].0, lists[0].1, &lists[0].2, lists[0].3.as_str()), (a, k("3myp5txuf7s2d"), &text(canonical), "3myp5txuf7s2d"));
    assert_eq!(lists[0].4, "app.bsky.graph.defs#modlist");
    assert_eq!(lists[0].5[0], 0, "the record as dict 0");
    assert_eq!(serde_json::from_slice::<Value>(&lists[0].5[1..]).expect("json"), list_record);
    // List items: an alias key, a rev that is not a TID stored as the
    // lowest, the list's key; an item naming another repo's list is left
    // out.
    let items: Vec<(i64, String, i64, i32)> =
        sqlx::query_as("SELECT rkey, bigint_to_rev(rev), list_rkey, subject FROM list_items").fetch_all(p).await.expect("items");
    assert_eq!(items.len(), 1);
    assert!(items[0].0 < 0, "an alias");
    assert_eq!((items[0].1.as_str(), items[0].2, items[0].3), ("2222222222222", k("3myp5txuf7s2d"), b));
    // List blocks and opt-outs: whole and odd subjects.
    let blocks: Vec<(i64, Option<i32>, i16, Option<i64>)> =
        sqlx::query_as("SELECT rkey, subj_author, subj_coll, subj_rkey FROM list_blocks ORDER BY rkey").fetch_all(p).await.expect("blocks");
    assert_eq!(blocks, [(k("3myp5txuf7s2d"), Some(a), 7, Some(k("3myp5txuf7s2d"))), (k("3myp5txuf7s2e"), None, 0, None)]);
    let optouts: i64 = sqlx::query_scalar("SELECT count(*) FROM list_optouts WHERE subj_coll = 7").fetch_one(p).await.expect("optouts");
    assert_eq!(optouts, 1);
    // A starter pack: its list, its search text; its counts, and only its.
    let packs: Vec<(i16, bool)> = sqlx::query_as("SELECT list_coll, search_tsv @@ to_tsquery('simple', 'people') FROM starter_packs")
        .fetch_all(p)
        .await
        .expect("packs");
    assert_eq!(packs, [(7, true)]);
    let counts: Vec<(i32, i64, i32, i32)> =
        sqlx::query_as("SELECT author, rkey, joined_week_count, joined_all_time_count FROM starter_pack_counts").fetch_all(p).await.expect("counts");
    assert_eq!(counts, [(a, k("3myp5txuf7s2d"), 2, 5)]);
    // Gates: a whole post and an odd one.
    let gates: Vec<(i16, Option<i32>, Option<i64>)> =
        sqlx::query_as("SELECT post_coll, post_author, post_rkey FROM threadgates UNION ALL SELECT post_coll, post_author, post_rkey FROM postgates ORDER BY 1 DESC")
            .fetch_all(p)
            .await
            .expect("gates");
    assert_eq!(gates, [(1, Some(a), Some(k("3myp5txuf7s2d"))), (0, Some(a), None)]);
    // Polls keep their visibility; the labeler is keyed by its author.
    let polls: Vec<String> = sqlx::query_scalar("SELECT visibility FROM nearhorizon_poll_posts").fetch_all(p).await.expect("polls");
    assert_eq!(polls, ["public"]);
    let labelers: Vec<i32> = sqlx::query_scalar("SELECT author FROM labeler_services").fetch_all(p).await.expect("labelers");
    assert_eq!(labelers, [a]);
    // Self records keep the fields queries read; a collection outside the
    // fixed list is left out.
    let own: Vec<(i16, bool, Option<String>)> =
        sqlx::query_as("SELECT coll, hide_from_recs, allow_subscriptions FROM actor_self_records ORDER BY coll").fetch_all(p).await.expect("self");
    assert_eq!(own, [(13, false, Some("mutuals".to_string())), (15, true, None)]);
    // A member's post reference names its accounts by id; one that is not
    // a valid DID names none.
    let refs: Vec<(i32, i64, Option<i32>, Option<i32>)> =
        sqlx::query_as("SELECT author, rkey, parent, root FROM member_post_refs").fetch_all(p).await.expect("refs");
    assert_eq!(refs, [(a, k("3myp5txuf7s2d"), Some(b), None)]);

    // The odd texts, by the owner's collection and slot.
    let odd: Vec<(i16, i16, String)> = sqlx::query_as("SELECT tbl, slot, raw FROM odd_refs ORDER BY tbl").fetch_all(p).await.expect("odd");
    assert_eq!(odd, [(9, 4, odd_list.to_string()), (17, 4, format!("at://{A}/com.example.thing/1"))]);
    // They go with their row.
    exec("DELETE FROM list_blocks; DELETE FROM postgates".to_string()).await;
    let left: i64 = sqlx::query_scalar("SELECT count(*) FROM odd_refs").fetch_one(p).await.expect("left");
    assert_eq!(left, 0);
    db.drop_db().await;
}

/// One DID interned outside the writers: created once, read back after,
/// and never for a string that is not a DID.
#[tokio::test]
async fn one_account_is_interned_select_first() {
    let Some(db) = fresh_db("intern_account").await else { return };
    let p = &db.pool;
    let first = intern_account(p, A).await.expect("intern").expect("an id");
    assert_eq!(intern_account(p, A).await.expect("again"), Some(first));
    assert_eq!(account_id(p, A).await.expect("read"), Some(first));
    assert_eq!(intern_account(p, "alice.example").await.expect("handle"), None, "a handle is not interned");
    let n: i64 = sqlx::query_scalar("SELECT count(*) FROM dids WHERE did = 'alice.example'").fetch_one(p).await.expect("count");
    assert_eq!(n, 0);
    db.drop_db().await;
}

/// Migration 0060 over rows of the old shape: each row is keyed by its
/// account's id, interned first; every column is kept, the CIDs and the
/// record in their exact text form, the rev in rev order (one that is not
/// a TID as the lowest); the profile's self-label values gain their column;
/// a row whose DID is not valid is left out.
#[tokio::test]
async fn migration_0060_moves_actors() {
    use serde_json::{Value, json};

    let Some(db) = crate::test_support::empty_db("migration_0060").await else { return };
    let p = &db.pool;
    let migrations: Vec<_> = crate::db::migrations::MIGRATOR.iter().filter(|m| !m.migration_type.is_down_migration()).collect();
    let at = migrations.iter().position(|m| m.version == 60).expect("0060");
    for m in &migrations[..at] {
        sqlx::raw_sql(&m.sql).execute(p).await.unwrap_or_else(|e| panic!("{}: {e}", m.version));
    }
    let canonical = "bafyreidbcoifjdzsr3u5i74kbm427m5yic67grd54s4luhlt4xxntevd6m";
    let pack = format!("at://{B}/app.bsky.graph.starterpack/3myp5txuf7s2d");
    let record = json!({
        "$type": "app.bsky.actor.profile",
        "displayName": "Ann",
        "labels": {"$type": "com.atproto.label.defs#selfLabels", "values": [{"val": "!no-unauthenticated"}, {"val": 7}, {"val": "porn"}]},
        "pinnedPost": {"uri": format!("at://{A}/app.bsky.feed.post/3myp5txuf7s2d"), "cid": canonical},
    });
    // A is known already (an outline row); B is not; C has no profile.
    const C: &str = "did:plc:compactccccccccccccccccc";
    sqlx::raw_sql(&format!(
        "INSERT INTO dids (did) VALUES ('{A}'); \
         INSERT INTO actors (did, handle, handle_checked_at, pds_endpoint, account_status, display_name, description, avatar_cid, banner_cid, \
           profile_rev, profile_record, indexed_at, profile_cid, pronouns, website, joined_via_starter_pack_uri, joined_via_starter_pack_cid, \
           profile_created_at) VALUES \
           ('{A}', 'ann.test', now(), 'https://pds.example.com', 'active', 'Ann', 'about', '{canonical}', 'Bafy', \
            '3myp5txuf7s2d', '{record}', '2026-01-01T00:00:00Z', '{canonical}', 'she/her', 'https://ann.example', '{pack}', '{canonical}', \
            '2026-01-01T00:01:00Z'), \
           ('{B}', 'bea.test', NULL, NULL, 'deactivated', NULL, NULL, NULL, NULL, 'not-a-tid', '{{}}', now(), NULL, NULL, NULL, NULL, NULL, NULL), \
           ('{C}', NULL, NULL, NULL, 'active', NULL, NULL, NULL, NULL, NULL, NULL, now(), NULL, NULL, NULL, NULL, NULL, NULL), \
           ('alice.example', 'x.test', NULL, NULL, 'active', NULL, NULL, NULL, NULL, NULL, NULL, now(), NULL, NULL, NULL, NULL, NULL, NULL)"
    ))
    .execute(p)
    .await
    .expect("old rows");
    let a_before: i32 = sqlx::query_scalar("SELECT id FROM dids WHERE did = $1").bind(A).fetch_one(p).await.expect("a");

    sqlx::raw_sql(&migrations[at].sql).execute(p).await.expect("0060 applies over existing rows");

    let id = |did: &'static str| async move { sqlx::query_scalar::<_, i32>("SELECT id FROM dids WHERE did = $1").bind(did).fetch_one(p).await.expect("id") };
    let (a, b, c) = (id(A).await, id(B).await, id(C).await);
    assert_eq!(a, a_before, "a known DID keeps its id");
    let text = |s: &str| [&[0u8][..], s.as_bytes()].concat();
    type Row = (i32, Option<String>, String, Option<Vec<u8>>, Option<Vec<u8>>, Option<String>, Option<Vec<u8>>, Option<Vec<u8>>, Option<Vec<u8>>, Option<String>, Option<Vec<String>>);
    let rows: Vec<Row> = sqlx::query_as(
        "SELECT id, handle, account_status, avatar_cid, banner_cid, bigint_to_rev(profile_rev), profile_record, profile_cid, \
           joined_via_starter_pack_cid, joined_via_starter_pack_uri, self_labels FROM actors ORDER BY id",
    )
    .fetch_all(p)
    .await
    .expect("actors");
    assert_eq!(rows.len(), 3, "the row whose DID is not valid is left out");
    let ann = rows.iter().find(|r| r.0 == a).expect("A");
    assert_eq!((ann.1.as_deref(), ann.2.as_str()), (Some("ann.test"), "active"));
    assert_eq!((ann.3.as_deref(), ann.4.as_deref()), (Some(&text(canonical)[..]), Some(&text("Bafy")[..])));
    assert_eq!(ann.5.as_deref(), Some("3myp5txuf7s2d"));
    let stored = ann.6.as_ref().expect("record");
    assert_eq!(stored[0], 0, "the record as dict 0");
    assert_eq!(serde_json::from_slice::<Value>(&stored[1..]).expect("json"), record);
    assert_eq!((ann.7.as_deref(), ann.8.as_deref()), (Some(&text(canonical)[..]), Some(&text(canonical)[..])));
    assert_eq!(ann.9.as_deref(), Some(pack.as_str()));
    assert_eq!(ann.10.as_deref(), Some(&["!no-unauthenticated".to_string(), "porn".to_string()][..]), "string values only");
    let bea = rows.iter().find(|r| r.0 == b).expect("B");
    assert_eq!((bea.2.as_str(), bea.5.as_deref(), bea.10.as_ref()), ("deactivated", Some("2222222222222"), None));
    let cee = rows.iter().find(|r| r.0 == c).expect("C");
    assert_eq!((cee.5.as_ref(), cee.6.as_ref(), cee.10.as_ref()), (None, None, None), "no profile, nothing made up");
    let kept: (String, Option<String>, String, String, bool, bool) = sqlx::query_as(
        "SELECT pds_endpoint, description, pronouns, website, indexed_at = '2026-01-01T00:00:00Z', profile_created_at = '2026-01-01T00:01:00Z' \
         FROM actors WHERE id = $1",
    )
    .bind(a)
    .fetch_one(p)
    .await
    .expect("kept");
    assert_eq!(kept, ("https://pds.example.com".into(), Some("about".into()), "she/her".into(), "https://ann.example".into(), true, true));
    // The indexes reads use are there.
    let indexes: Vec<String> = sqlx::query_scalar("SELECT indexname FROM pg_indexes WHERE tablename = 'actors' ORDER BY 1").fetch_all(p).await.expect("indexes");
    assert_eq!(indexes, ["actors_handle_prefix", "actors_handle_trgm", "actors_joined_via", "actors_name_prefix", "actors_name_trgm", "actors_pkey"]);
    db.drop_db().await;
}
