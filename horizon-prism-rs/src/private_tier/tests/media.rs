//! Media kinds on private posts and the media filters of the author feed
//! (private-media-filters design): the SQL backfill held to the classifier
//! (T-M7) and run as migration 0035 runs it; rewrites (T-M8); no decode at
//! ingest (T-M11); the filters (T-M1 to T-M5, T-M9).

use std::sync::atomic::{AtomicUsize, Ordering};

use super::embeds::media_kinds_of;
use super::*;
use crate::private_tier::media::{IMAGE, VIDEO, embed_fixtures, kinds_of_embed};

const MEDIA: &str = "posts_with_media";
const VIDEO_FILTER: &str = "posts_with_video";

/// Every item URI of `actor`'s feed under `filter` for `reader`, paging
/// `limit` at a time.
async fn media_feed(base: &str, actor: &str, filter: &str, reader: &str, limit: usize) -> Vec<String> {
    pages(base, FEED, &format!("actor={actor}&limit={limit}&filter={filter}"), reader, 100).await.iter().flat_map(feed_uris).collect()
}

/// The pages of `actor`'s feed under `filter`: each page's URIs and whether
/// it carries a cursor.
async fn trace(base: &str, actor: &str, filter: &str, reader: &str, limit: usize) -> Vec<(Vec<String>, bool)> {
    pages(base, FEED, &format!("actor={actor}&limit={limit}&filter={filter}"), reader, 100)
        .await
        .iter()
        .map(|p| (feed_uris(p), p.get("cursor").is_some()))
        .collect()
}

/// `uris` newest first: every fixture post here has one `createdAt`, so the
/// record key's bytes order them.
fn newest_first(mut uris: Vec<String>) -> Vec<String> {
    uris.sort_by(|a, b| b.rsplit('/').next().cmp(&a.rsplit('/').next()));
    uris
}

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
    f.ready_all().await;
    let base = serve(f.state(Some(Duration::from_secs(1)))).await;
    assert_eq!(media_feed(&base, ALICE, MEDIA, BOB, 30).await, std::slice::from_ref(&post), "in the media feed, read from the payload");

    let mut unmarked = new;
    unmarked.as_object_mut().expect("object").remove("mediaKinds");
    f.apply_op(ALICE, Action::Update, NSID_POST_PRIVATE, "3krewrite222", REV3, Some(unmarked)).await;
    assert_eq!(media_kinds(&f.pool, &post).await, 0, "absent is no media");
    assert!(media_feed(&base, ALICE, MEDIA, BOB, 30).await.is_empty(), "out of the media feed (§3.5)");
    assert_eq!(media_feed(&base, ALICE, "posts_with_replies", BOB, 30).await, std::slice::from_ref(&post), "and still in the others");

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

/// The embeds of T-M1 in one vocabulary (`klex`: the kryphocron one), each
/// with its bits; `quoted` is the post the quotes name.
fn feed_kinds(klex: bool, quoted: &str) -> Vec<(&'static str, Option<Value>, u8)> {
    let t = |stock: &str, k: &str| if klex { format!("{KLEX}#{k}") } else { format!("app.bsky.embed.{stock}") };
    let images = json!({"$type": t("images", "embedImage"), "images": [{"image": blob("bafyimg1"), "alt": "a"}]});
    let gallery = if klex {
        json!({"$type": t("gallery", "embedGallery"), "items": [{"$type": format!("{KLEX}#galleryImage"), "image": blob("bafyg1")}]})
    } else {
        json!({"$type": t("gallery", "embedGallery"), "items": [{"$type": "app.bsky.embed.gallery#image", "image": blob("bafyg1")}]})
    };
    let video = json!({"$type": t("video", "embedVideo"), "video": blob("bafyvideo")});
    let quote = json!({"uri": quoted, "cid": "bafyquoted"});
    let rwm = |media: &Value| json!({"$type": t("recordWithMedia", "embedRecordWithMedia"), "record": {"record": quote}, "media": media});
    let mut kinds: Vec<(&'static str, Option<Value>, u8)> =
        ["texta", "textb", "textc", "textd", "texte", "textf"].into_iter().map(|k| (k, None, 0)).collect();
    kinds.extend([
        ("imagea", Some(images.clone()), IMAGE),
        ("imageb", Some(images.clone()), IMAGE),
        ("gallery", Some(gallery), IMAGE),
        ("video", Some(video.clone()), VIDEO),
        ("quotevideo", Some(rwm(&video)), VIDEO),
        ("quoteimages", Some(rwm(&images)), IMAGE),
        ("quote", Some(json!({"$type": t("record", "embedRecord"), "record": quote})), 0),
        ("external", Some(json!({"$type": t("external", "embedExternal"),
            "external": {"uri": "https://example.com/x.gif", "title": "t", "description": "d", "thumb": blob("bafythumb")}})), 0),
        ("replyimages", Some(images), IMAGE),
    ]);
    kinds
}

/// T-M1 (Horizon's done-when, adjusted to stock), T-M2 (repost items) and
/// T-M5 (cursors and values): Alice's text, image, gallery, video,
/// quote-with-video, quote-with-images, plain quote of an image post,
/// external and image-reply posts, new-form with `mediaKinds` and old-form
/// with clear embeds in both vocabularies. Every reader in the audience,
/// the author included, gets the image kinds under `posts_with_media` and
/// the video kinds under `posts_with_video`, in full pages of 2 with a
/// cursor while candidates remain, each exactly once, the last page with
/// none.
#[tokio::test]
async fn t_m1_the_media_filters_follow_stock() {
    let Some(db) = fresh_db("t_m1_media_filters").await else {
        return;
    };
    let f = fixture(&db.pool, "t-m1-filters").await;
    let aud = f.audience(ALICE, "3kaudience22", Some("list"), &[BOB, CAROL]).await;
    let dave_aud = f.audience(DAVE, "3kauddave222", Some("list"), &[ALICE, BOB, CAROL]).await;
    let quoted = f.post_with(DAVE, "3kdaveimage2", &dave_aud, json!({"embed": image_embed()})).await;
    let (mut media, mut video, mut all) = (Vec::new(), Vec::new(), Vec::new());
    for (form, klex) in [("n", true), ("s", false), ("k", true)] {
        let root = format!("at://{ALICE}/{NSID_POST_PRIVATE}/3k{form}texta");
        for (kind, embed, bits) in feed_kinds(klex, &quoted) {
            let rkey = format!("3k{form}{kind}");
            let mut record = if form == "n" {
                f.new_form_record(ALICE, &rkey, kind, None, embed.as_ref(), &aud).await
            } else {
                f.old_form_record(ALICE, &rkey, kind, None, embed.as_ref(), &aud).await
            };
            if kind == "replyimages" {
                record["reply"] = json!({"root": {"uri": root, "cid": "bafyroot"}, "parent": {"uri": root, "cid": "bafyroot"}});
            }
            let uri = f.put_post(ALICE, &rkey, record).await;
            if (bits & IMAGE) != 0 {
                media.push(uri.clone());
            }
            if (bits & VIDEO) != 0 {
                video.push(uri.clone());
            }
            all.push(uri);
        }
    }
    let (media, video, all) = (newest_first(media), newest_first(video), newest_first(all));
    assert_eq!((media.len(), video.len()), (15, 6));
    // The repost item (T-M2): Bob reposts Alice's image post.
    f.ingest(BOB, REPOST, "3krepostimg2", subject_record(REPOST, &media[0], "2026-09-25T00:00:00Z")).await;
    f.ready_all().await;
    let base = serve(f.state(Some(Duration::from_secs(1)))).await;

    for reader in [ALICE, BOB, CAROL] {
        for (filter, expected) in [(MEDIA, &media), (VIDEO_FILTER, &video)] {
            let pages = trace(&base, ALICE, filter, reader, 2).await;
            let seen: Vec<String> = pages.iter().flat_map(|(u, _)| u.clone()).collect();
            assert_eq!(&seen, expected, "{reader}, {filter}");
            let (last, before) = pages.split_last().expect("pages");
            assert!(before.iter().all(|(u, c)| u.len() == 2 && *c), "{reader}, {filter}: full pages with cursors: {pages:?}");
            assert!(!last.1 && !last.0.is_empty(), "{reader}, {filter}: the last page has no cursor: {pages:?}");
        }
        assert_eq!(media_feed(&base, ALICE, "posts_with_replies", reader, 50).await, all, "{reader}: every post otherwise");
    }
    // T-M2: the repost item shows under the default filter only.
    assert_eq!(media_feed(&base, BOB, "posts_with_replies", CAROL, 30).await, &media[..1]);
    for filter in [MEDIA, VIDEO_FILTER] {
        assert!(media_feed(&base, BOB, filter, CAROL, 30).await.is_empty(), "{filter}: no repost items");
    }

    // T-M5: a media cursor is valid only within its filter, actor and caller.
    let (_, first, _) = json_of(&base, FEED, &format!("actor={ALICE}&limit=2&filter={MEDIA}"), BOB).await;
    let cursor = first["cursor"].as_str().expect("cursor").to_string();
    for (case, query, reader) in [
        ("video", format!("actor={ALICE}&limit=2&filter={VIDEO_FILTER}&cursor={cursor}"), BOB),
        ("replies", format!("actor={ALICE}&limit=2&filter=posts_with_replies&cursor={cursor}"), BOB),
        ("absent", format!("actor={ALICE}&limit=2&cursor={cursor}"), BOB),
        ("threads", format!("actor={ALICE}&limit=2&filter=posts_and_author_threads&cursor={cursor}"), BOB),
        ("another actor", format!("actor={DAVE}&limit=2&filter={MEDIA}&cursor={cursor}"), BOB),
        ("another caller", format!("actor={ALICE}&limit=2&filter={MEDIA}&cursor={cursor}"), CAROL),
    ] {
        let (status, body, _) = get(&base, FEED, &query, reader, key_of(reader)).await;
        assert_eq!(status, 400, "{case}: {body}");
        assert!(body.contains("Error: cursor is not valid"), "{case}: {body}");
    }
    let (_, second, _) = json_of(&base, FEED, &format!("actor={ALICE}&limit=2&filter={MEDIA}&cursor={cursor}"), BOB).await;
    assert_eq!(feed_uris(&second), media[2..4]);
    for filter in ["posts_no_replies", "", "posts_with_images", "POSTS_WITH_MEDIA"] {
        let (status, body, _) = get(&base, FEED, &format!("actor={ALICE}&filter={filter}"), BOB, key_of(BOB)).await;
        assert_eq!(status, 400, "{filter}: {body}");
        assert!(
            body.contains("Error: filter must be posts_with_replies, posts_and_author_threads, posts_with_media or posts_with_video"),
            "{filter}: {body}"
        );
    }
    db.drop_db().await;
}

/// T-M3 (readability): an image post under an audience Bob is not in, and
/// an image reply under Bob's audience to a root whose audience excludes
/// him, are absent from his media feed, and his pages and cursors equal
/// those he gets with the two posts deleted. Alice sees both.
#[tokio::test]
async fn t_m3_the_media_feeds_leave_no_trace() {
    let Some(db) = fresh_db("t_m3_media_readability").await else {
        return;
    };
    let f = fixture(&db.pool, "t-m3-readability").await;
    let aud = f.audience(ALICE, "3kaudience22", Some("list"), &[BOB, CAROL]).await;
    let carol_only = f.audience(ALICE, "3kaudcarol22", Some("list"), &[CAROL]).await;
    let dave_aud = f.audience(DAVE, "3kauddave222", Some("list"), &[ALICE]).await;
    let images = json!({"embed": image_embed()});
    let mut readable = Vec::new();
    for i in 0..5 {
        readable.push(f.post_with(ALICE, &format!("3kimage{i}2222"), &aud, images.clone()).await);
    }
    let hidden = f.post_with(ALICE, "3kimage25hid", &carol_only, images.clone()).await;
    let dave_root = f.private_post(DAVE, "3kdaveroot22", &dave_aud, None).await;
    let mut reply = f.record_with(ALICE, "3kimage35rep", &aud, &images).await;
    reply["reply"] = json!({"root": {"uri": dave_root, "cid": "bafyroot"}, "parent": {"uri": dave_root, "cid": "bafyroot"}});
    f.ingest(ALICE, NSID_POST_PRIVATE, "3kimage35rep", reply).await;
    let reply = format!("at://{ALICE}/{NSID_POST_PRIVATE}/3kimage35rep");
    f.ready_all().await;
    let base = serve(f.state(Some(Duration::from_secs(1)))).await;

    let with = trace(&base, ALICE, MEDIA, BOB, 2).await;
    let seen: Vec<String> = with.iter().flat_map(|(u, _)| u.clone()).collect();
    assert_eq!(seen, newest_first(readable.clone()), "{with:?}");
    let alice = media_feed(&base, ALICE, MEDIA, ALICE, 2).await;
    assert!(alice.contains(&hidden) && alice.contains(&reply), "the author sees both: {alice:?}");

    f.apply_op(ALICE, Action::Delete, NSID_POST_PRIVATE, "3kimage25hid", REV2, None).await;
    f.apply_op(ALICE, Action::Delete, NSID_POST_PRIVATE, "3kimage35rep", REV2, None).await;
    assert_eq!(trace(&base, ALICE, MEDIA, BOB, 2).await, with, "no trace of the posts Bob may not read");
    db.drop_db().await;
}

/// T-M4 (paging past text): 60 text posts newer than 5 image posts; with
/// `limit=5` the first page is the 5 image posts, and the batch loop was
/// handed those 5 rows and no text row. Under `enable_seqscan = off` the
/// media statements' plans name their partial indexes.
#[tokio::test]
async fn t_m4_the_media_feed_skips_text_posts() {
    let Some(db) = fresh_db("t_m4_media_paging").await else {
        return;
    };
    let f = fixture(&db.pool, "t-m4-paging").await;
    let aud = f.audience(ALICE, "3kaudience22", Some("list"), &[BOB]).await;
    let mut images = Vec::new();
    for i in 0..5 {
        images.push(f.post_with(ALICE, &format!("3kaimage{i}222"), &aud, json!({"embed": image_embed()})).await);
    }
    for i in 0..60 {
        f.private_post(ALICE, &format!("3kztext{i:02}222"), &aud, None).await;
    }
    f.ready_all().await;
    let base = serve(f.state(Some(Duration::from_secs(1)))).await;
    let (_, page, _) = json_of(&base, FEED, &format!("actor={ALICE}&limit=5&filter={MEDIA}"), BOB).await;
    assert_eq!(feed_uris(&page), newest_first(images.clone()));
    assert!(page.get("cursor").is_none(), "{page}");

    let handed = AtomicUsize::new(0);
    let key = |c: &schema::FeedCandidate| (c.created_at, c.key.clone());
    let pool = &f.pool;
    let rows = schema::passing(11, 11, 1000, None, key, |after, n| {
        let handed = &handed;
        async move {
            let rows = schema::author_batch(pool, BOB, ALICE, schema::AuthorFilter::Media, after, n, CAP).await?;
            handed.fetch_add(rows.len(), Ordering::Relaxed);
            Ok(rows)
        }
    })
    .await
    .expect("passing");
    assert_eq!((rows.len(), handed.load(Ordering::Relaxed)), (5, 5), "no text row examined");

    for (filter, index) in [(schema::AuthorFilter::Media, "private_posts_author_image"), (schema::AuthorFilter::Video, "private_posts_author_video")] {
        let mut tx = f.pool.begin().await.expect("tx");
        exec_on(&mut tx, "SET LOCAL enable_seqscan = off").await;
        let plan: Value = sqlx::query_scalar(&format!("EXPLAIN (FORMAT JSON) {}", schema::author_statement(filter, CAP)))
            .bind(BOB)
            .bind(ALICE)
            .bind(None::<DateTime<Utc>>)
            .bind("")
            .bind(11_i64)
            .fetch_one(&mut *tx)
            .await
            .expect("explain");
        assert!(plan.to_string().contains(index), "{filter:?}: {plan}");
        tx.rollback().await.expect("rollback");
    }
    db.drop_db().await;
}

/// `sql` on the transaction `tx`.
async fn exec_on(tx: &mut sqlx::Transaction<'_, sqlx::Postgres>, sql: &str) {
    sqlx::query(sql).execute(&mut **tx).await.expect(sql);
}

/// T-M9 (the read-side check): a post marked `["image"]` whose payload
/// embed is an external card is not in `posts_with_media`; the mismatch is
/// counted once, and its page still carries a cursor while candidates
/// remain.
#[tokio::test]
async fn t_m9_the_post_as_read_decides() {
    let Some(db) = fresh_db("t_m9_media_mismatch").await else {
        return;
    };
    let recorder = metrics_exporter_prometheus::PrometheusBuilder::new().build_recorder();
    let handle = recorder.handle();
    let _local = metrics::set_default_local_recorder(&recorder);
    let f = fixture(&db.pool, "t-m9-mismatch").await;
    let aud = f.audience(ALICE, "3kaudience22", Some("list"), &[BOB]).await;
    let mut images = Vec::new();
    for i in 0..3 {
        images.push(f.post_with(ALICE, &format!("3kaimage{i}222"), &aud, json!({"embed": image_embed()})).await);
    }
    let external = json!({"$type": format!("{KLEX}#embedExternal"),
        "external": {"uri": "https://example.com", "title": "t", "description": "d", "thumb": blob("bafythumb")}});
    let mut liar = f.new_form_record(ALICE, "3kzmarked222", "x", None, Some(&external), &aud).await;
    assert!(liar.get("mediaKinds").is_none());
    liar["mediaKinds"] = json!(["image"]);
    let liar = f.put_post(ALICE, "3kzmarked222", liar).await;
    assert_eq!(media_kinds(&f.pool, &liar).await, i16::from(IMAGE));
    f.ready_all().await;
    let base = serve(f.state(Some(Duration::from_secs(1)))).await;

    let pages = trace(&base, ALICE, MEDIA, BOB, 1).await;
    assert_eq!(pages[0], (vec![newest_first(images.clone())[0].clone()], true), "{pages:?}");
    let seen: Vec<String> = pages.iter().flat_map(|(u, _)| u.clone()).collect();
    assert_eq!(seen, newest_first(images), "{pages:?}");
    let mismatches = crate::test_support::series(&handle.render(), super::super::handlers::METRIC_MEDIA_MISMATCH, &[&format!("filter=\"{MEDIA}\"")]);
    assert_eq!(mismatches, Some(1.0), "{}", handle.render());
    db.drop_db().await;
}
