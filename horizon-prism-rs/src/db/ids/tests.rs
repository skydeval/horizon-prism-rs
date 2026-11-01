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
