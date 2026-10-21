//! The read-time clear-field check (private-encode design §6.2, §6.3,
//! T-P4, T-P5), the one-statement snapshot (§6.1, T-P6) and the author's
//! `droppedFields` marker (§8.4, T-D1 to T-D3).

use metrics_exporter_prometheus::PrometheusHandle;

use super::*;
use crate::private_tier::handlers::{GET_QUOTES, GET_UNREAD_COUNT, LIST_NOTIFICATIONS};
use crate::private_tier::{clear_fields, pipeline};

const IMAGE_BLOB: &str = "bafyimgcheck";

fn check_count(handle: &PrometheusHandle, field: &str, result: &str) -> f64 {
    crate::test_support::series(&handle.render(), pipeline::METRIC_INDEX_CHECK, &[&format!("field=\"{field}\""), &format!("result=\"{result}\"")])
        .unwrap_or(0.0)
}

fn outcome_count(handle: &PrometheusHandle, outcome: &str) -> f64 {
    crate::test_support::series(&handle.render(), pipeline::METRIC_OUTCOMES, &[&format!("outcome=\"{outcome}\"")]).unwrap_or(0.0)
}

fn images() -> Value {
    json!({"$type": "app.bsky.embed.images", "images": [{"image": blob(IMAGE_BLOB), "alt": ""}]})
}

/// An embed of more than 64 KiB that still quotes `quoted` and carries an
/// image as a record-with-media's media.
fn big_embed(quoted: &str) -> Value {
    json!({"$type": "app.bsky.embed.recordWithMedia",
        "record": {"record": {"uri": quoted, "cid": "bafyq"}},
        "media": {"$type": "app.bsky.embed.images", "images": [{"image": blob(IMAGE_BLOB), "alt": "x".repeat(70_000)}]}})
}

/// Facets of more than 64 KiB that mention `did`.
fn big_facets(did: &str) -> Value {
    let mut facets = vec![mention(did)];
    facets.push(json!({"index": {"byteStart": 0, "byteEnd": 1}, "features": [{"$type": "app.bsky.richtext.facet#tag", "tag": "t".repeat(70_000)}]}));
    Value::Array(facets)
}

/// Whether `reader` reads `uri` (a hit).
async fn reads(base: &str, reader: &str, uri: &str) -> bool {
    !one_post(base, reader, uri).await.1.is_null()
}

/// T-P4 (a)–(h) and T-P5: an over-claiming clear field is a miss for every
/// reader but the author; an under-claim is served; a field dropped at read
/// is compared from its raw value, so it neither hides an honest post nor
/// switches the check off.
#[tokio::test]
async fn tp4_tp5_over_claims_miss_for_everyone_but_the_author() {
    let Some(db) = fresh_db("tp4_index_check").await else {
        return;
    };
    let recorder = metrics_exporter_prometheus::PrometheusBuilder::new().build_recorder();
    let handle = recorder.handle();
    let _local = metrics::set_default_local_recorder(&recorder);
    let f = fixture(&db.pool, "tp4-index-check").await;
    let aud = f.audience(ALICE, "3kaudience22", Some("list"), &[BOB, CAROL, DAVE]).await;
    let x = f.post_with(ALICE, "3kquotedx222", &aud, json!({})).await;
    let y = f.post_with(ALICE, "3kquotedy222", &aud, json!({})).await;
    let z = f.post_with(ALICE, "3kquotedz222", &aud, json!({})).await;
    let quote = |uri: &str| json!({"$type": "app.bsky.embed.record", "record": {"uri": uri, "cid": "bafyq"}});

    // (a) `mentionDids` naming Carol, whom the payload does not mention.
    let mut rec = f.new_form_record(ALICE, "3kforgedmen2", "hi dave", Some(&json!([mention(DAVE)])), None, &aud).await;
    rec["mentionDids"] = json!([DAVE, CAROL]);
    let a = f.put_post(ALICE, "3kforgedmen2", rec).await;
    // (b) `quote` naming X, the payload quoting Y.
    let mut rec = f.new_form_record(ALICE, "3kforgedquo2", "quoting y", None, Some(&quote(&y)), &aud).await;
    rec["quote"] = json!({"uri": x, "cid": "bafyq"});
    let b = f.put_post(ALICE, "3kforgedquo2", rec).await;
    // (c) `mediaKinds: ["video"]` on a text post.
    let mut rec = f.new_form_record(ALICE, "3kforgedvid2", "just text", None, None, &aud).await;
    rec["mediaKinds"] = json!(["video"]);
    let c = f.put_post(ALICE, "3kforgedvid2", rec).await;
    // (d) A content-form post with images and no `mediaKinds`: an
    // under-claim.
    let mut rec = f.new_form_record(ALICE, "3kunderimg22", "pictures", None, Some(&images()), &aud).await;
    rec.as_object_mut().expect("object").remove("mediaKinds");
    let d = f.put_post(ALICE, "3kunderimg22", rec).await;
    // (e) An old-form post with a clear embed quoting X and `quote` naming Z.
    let e = f.post_with(ALICE, "3koldforged2", &aud, json!({"embed": quote(&x), "quote": {"uri": z, "cid": "bafyq"}})).await;
    // (f) Honest posts of every kind: match.
    let mut honest = Vec::new();
    for (i, (facets, embed)) in [
        (Some(json!([mention(CAROL)])), Some(images())),
        (None, Some(quote(&y))),
        (Some(json!([mention(DAVE), mention(CAROL)])), Some(json!({"$type": "app.bsky.embed.recordWithMedia", "record": {"record": {"uri": z, "cid": "c"}}, "media": images()}))),
        (None, Some(json!({"$type": "app.bsky.embed.video", "video": blob("bafyvid")}))),
        (None, None),
    ]
    .into_iter()
    .enumerate()
    {
        let rkey = format!("3khonest{i:02}22");
        let rec = f.new_form_record(ALICE, &rkey, "honest", facets.as_ref(), embed.as_ref(), &aud).await;
        honest.push(f.put_post(ALICE, &rkey, rec).await);
    }
    // (g) An honest embed over 64 KiB, and honest facets over 64 KiB, with
    // the clear fields derived from them.
    let rec = f.new_form_record(ALICE, "3kbigembed22", "big", None, Some(&big_embed(&x)), &aud).await;
    assert!(rec.get("quote").is_some() && rec.get("mediaKinds").is_some());
    let g_embed = f.put_post(ALICE, "3kbigembed22", rec).await;
    let rec = f.new_form_record(ALICE, "3kbigfacet22", "big", Some(&big_facets(CAROL)), None, &aud).await;
    assert_eq!(rec["mentionDids"], json!([CAROL]));
    let g_facets = f.put_post(ALICE, "3kbigfacet22", rec).await;
    // (h) Forged `mentionDids` over a payload `facets` that is a string, and
    // over 70 KiB facets with no mention of Carol; a forged `quote` over a
    // wrong-typed embed.
    let payload = |facets: Option<&Value>, embed: Option<&Value>| {
        let mut m = serde_json::Map::new();
        m.insert("text".into(), json!("t"));
        if let Some(v) = facets {
            m.insert("facets".into(), v.clone());
        }
        if let Some(v) = embed {
            m.insert("embed".into(), v.clone());
        }
        let mut out = vec![0xFF];
        out.extend(serde_json::to_vec(&Value::Object(m)).expect("json"));
        out
    };
    let mut rec = f.encoded_bytes(ALICE, "3kstrfacets2", &payload(Some(&json!("a string")), None), Some(&aud)).await;
    rec["mentionDids"] = json!([CAROL]);
    let h1 = f.put_post(ALICE, "3kstrfacets2", rec).await;
    let mut rec = f.encoded_bytes(ALICE, "3kbigforged2", &payload(Some(&big_facets(DAVE)), None), Some(&aud)).await;
    rec["mentionDids"] = json!([DAVE, CAROL]);
    let h2 = f.put_post(ALICE, "3kbigforged2", rec).await;
    let mut rec = f.encoded_bytes(ALICE, "3karrayembed", &payload(None, Some(&json!([quote(&x)]))), Some(&aud)).await;
    rec["quote"] = json!({"uri": x, "cid": "bafyq"});
    let h3 = f.put_post(ALICE, "3karrayembed", rec).await;
    f.ready_all().await;
    let base = serve(f.state_floored(Some(Duration::from_secs(1)), Duration::ZERO)).await;

    // Over-claims: a miss for every other reader, a hit for the author.
    for (case, uri, field) in [("a", &a, "mentions"), ("b", &b, "quote"), ("c", &c, "media"), ("e", &e, "quote"), ("h1", &h1, "mentions"), ("h2", &h2, "mentions"), ("h3", &h3, "quote")] {
        let (over, mismatch) = (check_count(&handle, field, "over"), outcome_count(&handle, "index_mismatch"));
        for reader in [BOB, CAROL, DAVE] {
            assert!(!reads(&base, reader, uri).await, "({case}) {reader} reads an over-claiming post");
        }
        assert!(check_count(&handle, field, "over") >= over + 3.0, "({case}) counted over");
        assert_eq!(outcome_count(&handle, "index_mismatch"), mismatch + 3.0, "({case}) one index_mismatch per miss");
        assert!(reads(&base, ALICE, uri).await, "({case}) the author reads it");
    }
    // (a) The mention notification was written at ingest; the list omits it.
    let written: i64 = sqlx::query_scalar("SELECT count(*) FROM private_notifications WHERE recipient_did = $1 AND record_uri = $2")
        .bind(CAROL)
        .bind(&a)
        .fetch_one(&f.pool)
        .await
        .expect("count");
    assert_eq!(written, 1, "the forged mention notified at ingest");
    let (_, page, _) = json_of(&base, LIST_NOTIFICATIONS, "limit=50", CAROL).await;
    assert!(!page.to_string().contains(&a), "listNotifications omits it: {page}");
    for forged in [&h1, &h2] {
        assert!(!page.to_string().contains(forged.as_str()), "listNotifications omits {forged}");
    }
    let listed = i64::try_from(page["notifications"].as_array().map_or(0, Vec::len)).expect("len");
    // T-P5: getUnreadCount counts with the prefilter, so it still counts
    // the three forged mentions the list leaves out (§18's residual,
    // asserted as it is, so a change to it is deliberate).
    let (_, unread, _) = json_of(&base, GET_UNREAD_COUNT, "", CAROL).await;
    assert_eq!(unread["count"].as_i64().expect("count"), listed + 3, "{unread} vs {page}");
    // (b) X's getQuotes lists the honest quote only; T-P5: X's quoteCount,
    // from the prefilter, also counts the two posts that quote it falsely.
    let (_, quotes, _) = json_of(&base, GET_QUOTES, &format!("uri={x}"), BOB).await;
    let text = quotes.to_string();
    assert!(text.contains(&g_embed) && !text.contains(&b) && !text.contains(&h3), "getQuotes: {quotes}");
    let (_, xv) = one_post(&base, BOB, &x).await;
    assert_eq!(xv["quoteCount"], 3, "the residual: {xv}");

    // (d) An under-claim is served, counted `under`.
    let under = check_count(&handle, "media", "under");
    assert!(reads(&base, BOB, &d).await, "(d) under-claims are served");
    assert_eq!(check_count(&handle, "media", "under"), under + 1.0);
    // (f) Honest posts: a hit for everyone, each field `match`.
    // (A post quoting a private post also reads that post, so the counts
    // rise by at least one; none is ever `over` or `under`.)
    for uri in &honest {
        let count = |result: &'static str| ["quote", "mentions", "media"].map(|fld| check_count(&handle, fld, result));
        let (matched, over, under) = (count("match"), count("over"), count("under"));
        assert!(reads(&base, BOB, uri).await, "{uri}");
        assert!(count("match").iter().zip(matched).all(|(a, b)| *a >= b + 1.0), "{uri}: every field matches");
        assert_eq!((count("over"), count("under")), (over, under), "{uri}");
    }
    // (g) Honest but oversized: a hit without the dropped part, `match`.
    let before = check_count(&handle, "quote", "match");
    let (_, view) = one_post(&base, BOB, &g_embed).await;
    assert!(!view.is_null() && view["record"].get("embed").is_none(), "{view}");
    assert_eq!(check_count(&handle, "quote", "match"), before + 1.0);
    let (_, view) = one_post(&base, BOB, &g_facets).await;
    assert!(!view.is_null() && view["record"].get("facets").is_none(), "{view}");
    db.drop_db().await;
}

/// T-D1 and T-D2 (§8.4): the author's own view lists the payload fields
/// the read left out, `facets` before `embed`, on every read method; no
/// other post and no other reader ever gets the field, and another reader's
/// view is byte-identical to the one built with the marker code off.
#[tokio::test]
async fn td1_td2_dropped_fields_on_the_authors_view_only() {
    let Some(db) = fresh_db("td1_dropped_fields").await else {
        return;
    };
    let f = fixture(&db.pool, "td1-dropped").await;
    let aud = f.audience(ALICE, "3kaudience22", Some("list"), &[BOB, CAROL]).await;
    let x = f.post_with(ALICE, "3kquotedx222", &aud, json!({})).await;
    let payload = |facets: Option<Value>, embed: Option<Value>| {
        let mut m = serde_json::Map::new();
        m.insert("text".into(), json!("t"));
        if let Some(v) = facets {
            m.insert("facets".into(), v);
        }
        if let Some(v) = embed {
            m.insert("embed".into(), v);
        }
        let mut out = vec![0xFF];
        out.extend(serde_json::to_vec(&Value::Object(m)).expect("json"));
        out
    };
    let cases: Vec<(&str, Vec<u8>, Value)> = vec![
        ("3kdropfacet2", payload(Some(json!("a string")), None), json!(["facets"])),
        ("3kdroparray2", payload(None, Some(json!([1, 2]))), json!(["embed"])),
        ("3kdropbig222", payload(None, Some(big_embed(&x))), json!(["embed"])),
        ("3kdropboth22", payload(Some(json!({"not": "an array"})), Some(json!("nope"))), json!(["facets", "embed"])),
    ];
    let mut dropped = Vec::new();
    for (rkey, bytes, want) in &cases {
        let mut rec = f.encoded_bytes(ALICE, rkey, bytes, Some(&aud)).await;
        // The clear fields a correct encoder derives from the raw values.
        if rkey == &"3kdropbig222" {
            rec["quote"] = json!({"uri": x, "cid": "bafyq"});
            rec["mediaKinds"] = json!(["image"]);
        }
        dropped.push((f.put_post(ALICE, rkey, rec).await, want.clone()));
    }
    // Posts with nothing dropped: no marker.
    let mut clean = Vec::new();
    let rec = f.new_form_record(ALICE, "3kcleannew22", "fine", Some(&json!([mention(BOB)])), Some(&images()), &aud).await;
    clean.push(f.put_post(ALICE, "3kcleannew22", rec).await);
    clean.push(f.post_with(ALICE, "3kcleanold22", &aud, json!({"embed": images()})).await);
    let extra = f.encoded_bytes(ALICE, "3kextrakeys2", br#"{"text":"t","other":1,"reply":{}}"#.iter().fold(vec![0xFF], |mut v, b| {
        v.push(*b);
        v
    }).as_slice(), Some(&aud)).await;
    clean.push(f.put_post(ALICE, "3kextrakeys2", extra).await);
    let dupes = f.encoded_bytes(ALICE, "3kdupekeys22", br#"{"text":"a","text":"b"}"#.iter().fold(vec![0xFF], |mut v, b| {
        v.push(*b);
        v
    }).as_slice(), Some(&aud)).await;
    clean.push(f.put_post(ALICE, "3kdupekeys22", dupes).await);
    let legacy = json!({"$type": "app.bsky.embed.images", "images": [{"image": {"cid": "bafylegacy", "mimeType": "image/jpeg"}, "alt": ""}]});
    let rec = f.new_form_record(ALICE, "3klegacyblob", "legacy", None, Some(&legacy), &aud).await;
    clean.push(f.put_post(ALICE, "3klegacyblob", rec).await);
    // A clear embed dropped at ingest (C-16): Prism keeps no trace.
    clean.push(f.post_with(ALICE, "3kingestdrop", &aud, json!({"embed": [1]})).await);
    // Carol's post with a dropped field, which Alice reads as another reader.
    let caud = f.audience(CAROL, "3kcarolaud22", Some("list"), &[ALICE]).await;
    let rec = f.encoded_bytes(CAROL, "3kcaroldrop2", &payload(Some(json!(5)), None), Some(&caud)).await;
    let carols = f.put_post(CAROL, "3kcaroldrop2", rec).await;
    f.ready_all().await;
    let base = serve(f.state_floored(Some(Duration::from_secs(1)), Duration::ZERO)).await;

    for (uri, want) in &dropped {
        let (_, view) = one_post(&base, ALICE, uri).await;
        assert_eq!(view["droppedFields"], *want, "getPosts {uri}: {view}");
        let (_, thread) = thread_of(&base, ALICE, &format!("uri={uri}")).await;
        assert_eq!(thread["post"]["droppedFields"], *want, "getPostThread {uri}");
        // T-D2: Bob's view has none, and equals the view built with the
        // marker code off.
        let (body, view) = one_post(&base, BOB, uri).await;
        assert!(!view.is_null() && view.get("droppedFields").is_none(), "{view}");
        let off = crate::test_support::hooks::arm(crate::test_support::hooks::DROPPED_MARKER_OFF, uri);
        let (body_off, _) = one_post(&base, BOB, uri).await;
        let (_, author_off) = one_post(&base, ALICE, uri).await;
        drop(off);
        let strip = |s: &str| {
            let mut v: Value = serde_json::from_str(s).expect("json");
            for p in v["posts"].as_array_mut().into_iter().flatten() {
                p.as_object_mut().expect("view").remove("indexedAt");
            }
            v.to_string()
        };
        assert_eq!(strip(&body), strip(&body_off), "{uri}: another reader's view is untouched by the marker code");
        assert!(author_off.get("droppedFields").is_none(), "the hook turns the marker off");
    }
    // The author feed and the timeline carry it too.
    let (_, feed, _) = json_of(&base, FEED, &format!("actor={ALICE}&limit=50"), ALICE).await;
    let items = feed["feed"].as_array().expect("feed");
    for (uri, want) in &dropped {
        let item = items.iter().find(|i| i["uri"] == uri.as_str()).unwrap_or_else(|| panic!("{uri} not in {feed}"));
        assert_eq!(item["droppedFields"], *want, "getAuthorFeed {uri}");
    }
    // A self-follow is not needed: the timeline holds the caller's own posts.
    let (_, tl, _) = json_of(&base, "app.nearhorizon.private.getTimeline", "limit=50", ALICE).await;
    let mut seen = 0;
    for (uri, want) in &dropped {
        if let Some(item) = tl["feed"].as_array().into_iter().flatten().find(|i| i["uri"] == uri.as_str()) {
            assert_eq!(item["droppedFields"], *want, "getTimeline {uri}");
            seen += 1;
        }
    }
    assert_eq!(seen, dropped.len(), "the author's timeline holds them: {tl}");
    for uri in &clean {
        let (_, view) = one_post(&base, ALICE, uri).await;
        assert!(!view.is_null() && view.get("droppedFields").is_none(), "{uri}: {view}");
    }
    // The author reading another member's post with a dropped field: none.
    let (_, view) = one_post(&base, ALICE, &carols).await;
    assert!(!view.is_null() && view.get("droppedFields").is_none(), "{view}");
    // A quoted view never carries it, even for the quoted post's author.
    let q = f.post_with(ALICE, "3kquotesdrop", &aud, quote_of(&dropped[0].0)).await;
    let (_, view) = one_post(&base, ALICE, &q).await;
    assert_eq!(view["embed"]["record"]["uri"], dropped[0].0.as_str(), "{view}");
    assert!(!view.to_string().contains("droppedFields"), "{view}");
    db.drop_db().await;
}

/// T-D3 (§8.4 with E-C1): a text-only post with a forged `mentionDids`
/// gives its author a view with no marker; rewriting it from that view with
/// correctly derived clear fields heals it, and a reader then hits. With a
/// dropped `embed` as well, the author's view carries `["embed"]` and
/// readers still miss.
#[tokio::test]
async fn td3_rewriting_from_the_authors_view_heals_an_over_claim() {
    let Some(db) = fresh_db("td3_heal").await else {
        return;
    };
    let f = fixture(&db.pool, "td3-heal").await;
    let aud = f.audience(ALICE, "3kaudience22", Some("list"), &[BOB, CAROL]).await;
    let mut rec = f.new_form_record(ALICE, "3kforgedtxt2", "plain words", None, None, &aud).await;
    rec["mentionDids"] = json!([CAROL]);
    let post = f.put_post(ALICE, "3kforgedtxt2", rec).await;
    let mut both = f.encoded_bytes(ALICE, "3kforgeddrop", br#"{"text":"t","embed":[1]}"#.iter().fold(vec![0xFF], |mut v, b| {
        v.push(*b);
        v
    }).as_slice(), Some(&aud)).await;
    both["mentionDids"] = json!([CAROL]);
    let both = f.put_post(ALICE, "3kforgeddrop", both).await;
    f.ready_all().await;
    let base = serve(f.state_floored(Some(Duration::from_secs(1)), Duration::ZERO)).await;
    let (_, view) = one_post(&base, ALICE, &post).await;
    assert!(view.get("droppedFields").is_none(), "{view}");
    assert!(!reads(&base, BOB, &post).await);
    // The rewrite: the author's view's content, encoded again with the
    // derived clear fields, written at a new rev.
    let record = &view["record"];
    let mut rewrite = f
        .new_form_record(ALICE, "3kforgedtxt2", record["text"].as_str().expect("text"), record.get("facets"), record.get("embed"), &aud)
        .await;
    rewrite["createdAt"] = record["createdAt"].clone();
    f.apply_op(ALICE, Action::Update, NSID_POST_PRIVATE, "3kforgedtxt2", "3kaaaaaaaaab2", Some(rewrite)).await;
    assert!(reads(&base, BOB, &post).await, "healed");
    let (_, view) = one_post(&base, ALICE, &both).await;
    assert_eq!(view["droppedFields"], json!(["embed"]), "{view}");
    assert!(!reads(&base, BOB, &both).await);
    db.drop_db().await;
}

/// The client encoder's parity fixture (Horizon K6-L2): records built as
/// the client builds them with kryphocron 0.4.4 (`PostContent::to_payload`
/// for the payload, `derive_clear_fields(..).apply_to` for the clear
/// fields), encoded with the real hooks and written through the writer.
/// For each, the fields kryphocron derives equal what Prism's read check
/// expects, every field is counted `match`, and a member of the audience
/// reads the post.
#[tokio::test]
async fn tk1_records_the_client_encoder_writes_are_served() {
    use kryphocron::post_content::{PostContent, derive_clear_fields};
    let Some(db) = fresh_db("tk1_client_encoder").await else {
        return;
    };
    let recorder = metrics_exporter_prometheus::PrometheusBuilder::new().build_recorder();
    let handle = recorder.handle();
    let _local = metrics::set_default_local_recorder(&recorder);
    let f = fixture(&db.pool, "tk1-client").await;
    let aud = f.audience(ALICE, "3kaudience22", Some("list"), &[BOB, CAROL]).await;
    let quoted = f.post_with(ALICE, "3kquotedx222", &aud, json!({})).await;
    let strong = json!({"uri": quoted, "cid": "bafyquoted"});
    let video = json!({"$type": "app.bsky.embed.video", "video": {"$type": "blob", "ref": {"$link": "bafyvid"}, "mimeType": "video/mp4", "size": 9}, "alt": "v"});
    let cases: Vec<(&str, Option<Value>, Option<Value>, Value)> = vec![
        ("a mention facet", Some(json!([mention(CAROL)])), None, json!({"mentionDids": [CAROL]})),
        ("a quote", None, Some(json!({"$type": "app.bsky.embed.record", "record": strong})), json!({"quote": strong})),
        ("images", None, Some(images()), json!({"embedBlobs": [blob(IMAGE_BLOB)], "mediaKinds": ["image"]})),
        ("video", None, Some(video.clone()), json!({"embedBlobs": [video["video"]], "mediaKinds": ["video"]})),
        ("record with images", None, Some(json!({"$type": "app.bsky.embed.recordWithMedia", "record": {"record": strong}, "media": images()})),
            json!({"embedBlobs": [blob(IMAGE_BLOB)], "quote": strong, "mediaKinds": ["image"]})),
        ("record with video", None, Some(json!({"$type": "app.bsky.embed.recordWithMedia", "record": {"record": strong}, "media": video})),
            json!({"embedBlobs": [video["video"]], "quote": strong, "mediaKinds": ["video"]})),
        ("unknown extra keys", Some(json!([{"index": {"byteStart": 0, "byteEnd": 1}, "features": [{"$type": "app.nearhorizon.richtext.facet#spoiler", "reason": "x"}, {"$type": "app.bsky.richtext.facet#mention", "did": CAROL, "x-extra": 1}]}])),
            Some(json!({"$type": "app.bsky.embed.images", "images": [{"image": blob(IMAGE_BLOB), "alt": "", "app.nearhorizon.spoiler": {"reason": "flash"}}], "x-unknown": {"k": [1, 2]}})),
            json!({"embedBlobs": [blob(IMAGE_BLOB)], "mentionDids": [CAROL], "mediaKinds": ["image"]})),
    ];
    let mut uris = Vec::new();
    for (i, (case, facets, embed, want)) in cases.iter().enumerate() {
        let rkey = format!("3kclient{i:02}22");
        let content = PostContent { text: (*case).to_string(), facets: facets.clone(), embed: embed.clone() };
        let mut record = f.encoded_bytes(ALICE, &rkey, &content.to_payload(), Some(&aud)).await;
        let fields = derive_clear_fields(facets.as_ref(), embed.as_ref());
        fields.apply_to(record.as_object_mut().expect("record"));
        // What kryphocron derived is what the case names, and what Prism's
        // read check expects from the same content.
        let mut derived = serde_json::Map::new();
        fields.apply_to(&mut derived);
        assert_eq!(Value::Object(derived), *want, "{case}: kryphocron's fields");
        let expected = clear_fields::index(&clear_fields::derive(facets.as_ref(), embed.as_ref()));
        let mut wire = serde_json::Map::new();
        fields.apply_to(&mut wire);
        let from_wire = clear_fields::index(&clear_fields::ClearFields {
            embed_blobs: vec![],
            mention_dids: wire.get("mentionDids").and_then(Value::as_array).cloned().unwrap_or_default(),
            quote: wire.get("quote").cloned(),
            media_kinds: wire.get("mediaKinds").and_then(Value::as_array).cloned().unwrap_or_default(),
        });
        assert_eq!(expected, from_wire, "{case}: what the client wrote is what Prism expects");
        uris.push((*case, f.put_post(ALICE, &rkey, record).await));
    }
    f.ready_all().await;
    let base = serve(f.state_floored(Some(Duration::from_secs(1)), Duration::ZERO)).await;
    for (case, uri) in &uris {
        let count = |result: &'static str| ["quote", "mentions", "media"].map(|fld| check_count(&handle, fld, result));
        let (matched, over, under) = (count("match"), count("over"), count("under"));
        let (_, view) = one_post(&base, BOB, uri).await;
        assert_eq!(view["record"]["text"], *case, "{case}: served to the audience: {view}");
        assert!(count("match").iter().zip(matched).all(|(a, b)| *a >= b + 1.0), "{case}: every field matches");
        assert_eq!((count("over"), count("under")), (over, under), "{case}: no over- or under-claim");
    }
    db.drop_db().await;
}

/// T-G1: `encodedContentGeneration` is client-asserted and decides no
/// access. The same post read with its real mark, a mark dated far in the
/// future, and a mark dated at the epoch, gives the same view to its
/// audience and its author.
#[tokio::test]
async fn tg1_the_generation_mark_decides_no_access() {
    let Some(db) = fresh_db("tg1_generation").await else {
        return;
    };
    let f = fixture(&db.pool, "tg1-generation").await;
    let aud = f.audience(ALICE, "3kaudience22", Some("list"), &[BOB]).await;
    let rec = f.new_form_record(ALICE, "3kgeneration", "marked", None, None, &aud).await;
    let real = rec["encodedContentGeneration"].as_str().expect("mark").to_string();
    let post = f.put_post(ALICE, "3kgeneration", rec).await;
    f.ready_all().await;
    let base = serve(f.state_floored(Some(Duration::from_secs(1)), Duration::ZERO)).await;
    let hex = real.rsplit('/').next().expect("batch").to_string();
    let mut views = Vec::new();
    for mark in [real.clone(), format!("laquna/{:020}/{hex}", 99_999_999_999u64), format!("laquna/{:020}/{hex}", 0)] {
        sqlx::query("UPDATE private_posts SET encoded_generation = $2 WHERE uri = $1").bind(&post).bind(&mark).execute(&f.pool).await.expect("mark");
        for reader in [BOB, ALICE] {
            let (_, mut view) = one_post(&base, reader, &post).await;
            assert_eq!(view["record"]["text"], "marked", "{mark} for {reader}");
            view.as_object_mut().expect("view").remove("indexedAt");
            views.push((reader, view));
        }
    }
    for (reader, view) in &views {
        let first = &views.iter().find(|(r, _)| r == reader).expect("first").1;
        assert_eq!(view, first, "{reader}: the mark changes nothing");
    }
    db.drop_db().await;
}

/// T-P6 (§6.1): an ingest upsert landing after the read took its snapshot
/// and before the view is built changes neither: the view's `cid` and its
/// content are of one version.
#[tokio::test]
async fn tp6_the_view_is_one_version_of_the_row() {
    let Some(db) = fresh_db("tp6_snapshot").await else {
        return;
    };
    let f = fixture(&db.pool, "tp6-snapshot").await;
    let aud = f.audience(ALICE, "3kaudience22", Some("list"), &[BOB]).await;
    let rec = f.new_form_record(ALICE, "3ksnapshot22", "old text", None, None, &aud).await;
    let post = f.put_post(ALICE, "3ksnapshot22", rec).await;
    f.ready_all().await;
    let base = serve(f.state_floored(Some(Duration::from_secs(1)), Duration::ZERO)).await;
    let (_, before) = one_post(&base, BOB, &post).await;
    let old_cid = before["cid"].as_str().expect("cid").to_string();
    let armed = crate::test_support::hooks::arm(crate::test_support::hooks::PRIVATE_SNAPSHOT_TAKEN, &post);
    let reading = tokio::spawn({
        let (base, post) = (base.clone(), post.clone());
        async move { one_post(&base, BOB, &post).await.1 }
    });
    armed.reached().await;
    let newer = f.new_form_record(ALICE, "3ksnapshot22", "new text", None, None, &aud).await;
    f.apply_op(ALICE, Action::Update, NSID_POST_PRIVATE, "3ksnapshot22", "3kaaaaaaaaab2", Some(newer)).await;
    armed.release();
    let view = reading.await.expect("read");
    drop(armed);
    assert_eq!((view["cid"].as_str(), view["record"]["text"].as_str()), (Some(old_cid.as_str()), Some("old text")), "{view}");
    let (_, after) = one_post(&base, BOB, &post).await;
    assert_ne!(after["cid"].as_str(), Some(old_cid.as_str()));
    assert_eq!(after["record"]["text"], "new text");
    db.drop_db().await;
}
