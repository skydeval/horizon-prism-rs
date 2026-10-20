//! Media kinds on private posts (private-media-filters design): the SQL
//! backfill held to the classifier (T-M7) and run as migration 0035 runs
//! it; rewrites (T-M8); no decode at ingest (T-M11).

use super::embeds::media_kinds_of;
use super::*;
use crate::private_tier::media::{IMAGE, VIDEO, embed_fixtures, kinds_of_embed};

/// A stored post's `media_kinds`.
async fn media_kinds(pool: &PgPool, uri: &str) -> i16 {
    sqlx::query_scalar("SELECT media_kinds FROM private_posts WHERE uri = $1").bind(uri).fetch_one(pool).await.expect("row")
}

/// An image embed in the kryphocron form.
fn image_embed() -> Value {
    json!({"$type": format!("{KLEX}#embedImage"), "images": [{"image": blob("bafyimg1"), "alt": "one"}]})
}

/// A video embed in the stock form.
fn video_embed_stock() -> Value {
    json!({"$type": "app.bsky.embed.video", "video": blob("bafyvideo")})
}

/// T-M7: `private_embed_media_kinds` equals `kinds_of_embed` over every
/// embed fixture; and over a stored `jsonb` value, as the migration runs it.
#[tokio::test]
async fn t_m7_the_backfill_function_equals_the_classifier() {
    let Some(db) = fresh_db("t_m7_media_function").await else {
        return;
    };
    for (case, embed, bits) in embed_fixtures() {
        let sql: i16 =
            sqlx::query_scalar("SELECT private_embed_media_kinds($1)").bind(&embed).fetch_one(&db.pool).await.expect(case);
        assert_eq!(sql, i16::from(kinds_of_embed(&embed)), "{case}");
        assert_eq!(sql, i16::from(bits), "{case}");
    }
    let uri = format!("at://{ALICE}/{NSID_POST_PRIVATE}/3kstored2222");
    sqlx::query(
        "INSERT INTO private_posts (uri, author_did, rkey, cid, rev, plaintext_text, created_at, embed, media_kinds) \
         VALUES ($1, $2, '3kstored2222', 'bafy', $3, 't', now(), $4, 0)",
    )
    .bind(&uri)
    .bind(ALICE)
    .bind(REV)
    .bind(image_embed())
    .execute(&db.pool)
    .await
    .expect("insert");
    assert_eq!(media_kinds(&db.pool, &uri).await, 0);
    exec(&db.pool, &format!("UPDATE private_posts SET media_kinds = private_embed_media_kinds(embed) WHERE uri = '{uri}'")).await;
    assert_eq!(media_kinds(&db.pool, &uri).await, i16::from(IMAGE));
    db.drop_db().await;
}

/// Migration 0035 over rows stored before it: each row with a clear embed
/// of every fixture kind gets its bits; a row with no clear embed (a new-form
/// post) stays 0.
#[tokio::test]
async fn migration_0035_backfills_from_the_clear_embed() {
    let Some(db) = crate::test_support::empty_db("migration-0035-media").await else {
        return;
    };
    let p = &db.pool;
    let migrations: Vec<_> = crate::db::migrations::MIGRATOR.iter().filter(|m| !m.migration_type.is_down_migration()).collect();
    let at = migrations.iter().position(|m| m.version == 35).expect("0035");
    for m in &migrations[..at] {
        sqlx::raw_sql(&m.sql).execute(p).await.unwrap_or_else(|e| panic!("{}: {e}", m.version));
    }
    let fixtures = embed_fixtures();
    let insert = |rkey: String, embed: Option<Value>| async move {
        sqlx::query(
            "INSERT INTO private_posts (uri, author_did, rkey, cid, rev, plaintext_text, created_at, embed) \
             VALUES ($1, $2, $3, 'bafy', $4, 't', now(), $5)",
        )
        .bind(format!("at://{ALICE}/{NSID_POST_PRIVATE}/{rkey}"))
        .bind(ALICE)
        .bind(&rkey)
        .bind(REV)
        .bind(embed)
        .execute(p)
        .await
        .expect("insert");
    };
    for (i, (_, embed, _)) in fixtures.iter().enumerate() {
        insert(format!("3kfixture{i:03}"), Some(embed.clone())).await;
    }
    insert("3knoembed222".to_string(), None).await;
    sqlx::raw_sql(&migrations[at].sql).execute(p).await.expect("0035");
    for (i, (case, _, bits)) in fixtures.iter().enumerate() {
        let uri = format!("at://{ALICE}/{NSID_POST_PRIVATE}/3kfixture{i:03}");
        assert_eq!(media_kinds(p, &uri).await, i16::from(*bits), "{case}");
    }
    assert_eq!(media_kinds(p, &format!("at://{ALICE}/{NSID_POST_PRIVATE}/3knoembed222")).await, 0);
    for m in &migrations[at + 1..] {
        sqlx::raw_sql(&m.sql).execute(p).await.unwrap_or_else(|e| panic!("{}: {e}", m.version));
    }
    db.drop_db().await;
}

/// T-M8: an old-form image post (clear embed) is 1; the PDS's rewrite to the
/// new form with `mediaKinds: ["image"]` and no clear embed keeps 1 and
/// writes no notification; a rewrite without `mediaKinds` makes it 0
/// (§3.5). Each write counts its source.
#[tokio::test]
async fn t_m8_a_rewrite_re_derives_the_bits() {
    let Some(db) = fresh_db("t_m8_media_rewrite").await else {
        return;
    };
    let recorder = metrics_exporter_prometheus::PrometheusBuilder::new().build_recorder();
    let handle = recorder.handle();
    let _local = metrics::set_default_local_recorder(&recorder);
    let f = fixture(&db.pool, "t-m8-rewrite").await;
    let aud = f.audience(ALICE, "3kaudience22", Some("list"), &[BOB]).await;
    let embed = image_embed();
    let old = f.old_form_record(ALICE, "3krewrite222", "words", None, Some(&embed), &aud).await;
    let post = f.put_post(ALICE, "3krewrite222", old).await;
    assert_eq!(media_kinds(&f.pool, &post).await, i16::from(IMAGE));

    let new = f.new_form_record(ALICE, "3krewrite222", "words", None, Some(&embed), &aud).await;
    assert_eq!((new.get("embed"), &new["mediaKinds"]), (None, &json!(["image"])));
    f.apply_op(ALICE, Action::Update, NSID_POST_PRIVATE, "3krewrite222", REV2, Some(new.clone())).await;
    let (cid, embed_stored): (String, Option<Value>) =
        sqlx::query_as("SELECT cid, embed FROM private_posts WHERE uri = $1").bind(&post).fetch_one(&f.pool).await.expect("row");
    assert_eq!((cid, embed_stored), (format!("bafy3krewrite222{REV2}"), None), "the rewrite was written");
    assert_eq!(media_kinds(&f.pool, &post).await, i16::from(IMAGE), "the marker keeps the bit");
    let notified: i64 = sqlx::query_scalar("SELECT count(*) FROM private_notifications WHERE record_uri = $1")
        .bind(&post)
        .fetch_one(&f.pool)
        .await
        .expect("count");
    assert_eq!(notified, 0);

    let mut unmarked = new;
    unmarked.as_object_mut().expect("object").remove("mediaKinds");
    f.apply_op(ALICE, Action::Update, NSID_POST_PRIVATE, "3krewrite222", REV3, Some(unmarked)).await;
    assert_eq!(media_kinds(&f.pool, &post).await, 0, "absent is no media");

    let source = |s: &str| {
        crate::test_support::series(
            &handle.render(),
            crate::private_tier::ingest::METRIC_INDEX_SOURCE,
            &["field=\"media\"", &format!("source=\"{s}\"")],
        )
        .unwrap_or(0.0)
    };
    assert_eq!((source("clear"), source("dedicated"), source("none")), (1.0, 1.0, 1.0), "{}", handle.render());
    db.drop_db().await;
}

/// T-M11: indexing the marker needs no decode. A new-form post marked
/// `["video"]` whose `encodedContent` is not laquna at all is stored with
/// the bit; it reads as a miss.
#[tokio::test]
async fn t_m11_the_marker_needs_no_decode() {
    let Some(db) = fresh_db("t_m11_media_no_decode").await else {
        return;
    };
    let f = fixture(&db.pool, "t-m11-no-decode").await;
    let aud = f.audience(ALICE, "3kaudience22", Some("list"), &[BOB]).await;
    let embed = video_embed_stock();
    let mut record = f.new_form_record(ALICE, "3kgarbage222", "g", None, Some(&embed), &aud).await;
    assert_eq!(record["mediaKinds"], json!(["video"]));
    assert_eq!(media_kinds_of(Some(&embed)), Some(json!(["video"])));
    record["encodedContent"] = json!("00112233445566778899aabbccddeeff");
    let post = f.put_post(ALICE, "3kgarbage222", record).await;
    assert_eq!(media_kinds(&f.pool, &post).await, i16::from(VIDEO));
    f.ready_all().await;
    let base = serve(f.state_floored(Some(Duration::from_secs(1)), Duration::ZERO)).await;
    let (_, views) = posts_of(&base, BOB, &[&post]).await;
    assert!(views.is_empty(), "{views:?}");
    db.drop_db().await;
}
