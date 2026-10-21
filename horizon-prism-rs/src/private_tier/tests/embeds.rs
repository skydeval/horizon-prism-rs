//! Private posts whose `encodedContent` carries facets and embed (#504,
//! private-embeds design): the old form held to a golden (T-E2); the new
//! form read as the old (T-E1); hostile payloads (T-E5); the input cap
//! (T-E6); the codec's decode bound (T-S3); search (T-E7); ingest from the
//! clear `quote` and `mentionDids` (T-E3, T-E9, T-E10); rewrites and the
//! notification rules (T-E4, T-E8).

use metrics_exporter_prometheus::PrometheusHandle;

use super::*;
use crate::private_tier::pipeline;

/// The mention feature types a PDS takes `mentionDids` from.
const MENTION_TYPES: [&str; 3] = ["app.bsky.richtext.facet#mention", "tools.kryphocron.feed.postPublic#mention", "#mention"];

/// The new-form payload byte for byte as the PDS's encoder builds it:
/// `0xFF`, then `{"text", "facets"?, "embed"?}` written by `serde_json`.
fn payload(text: &str, facets: Option<&Value>, embed: Option<&Value>) -> Vec<u8> {
    let mut map = serde_json::Map::new();
    map.insert("text".into(), json!(text));
    if let Some(v) = facets {
        map.insert("facets".into(), v.clone());
    }
    if let Some(v) = embed {
        map.insert("embed".into(), v.clone());
    }
    let mut out = vec![0xFF];
    serde_json::to_writer(&mut out, &Value::Object(map)).expect("json");
    out
}

/// `0xFF` followed by `rest`, for payloads no PDS writes.
fn raw_payload(rest: &[u8]) -> Vec<u8> {
    let mut out = vec![0xFF];
    out.extend_from_slice(rest);
    out
}

/// Every blob object in `v`, copied whole (the PDS's `embedBlobs`).
fn blobs_in(v: &Value, out: &mut Vec<Value>) {
    match v {
        Value::Object(m) if m.get("$type").and_then(Value::as_str) == Some("blob") => out.push(v.clone()),
        Value::Object(m) => m.values().for_each(|c| blobs_in(c, out)),
        Value::Array(a) => a.iter().for_each(|c| blobs_in(c, out)),
        _ => {}
    }
}

/// The PDS's `mentionDids`: every mention feature's DID, deduplicated, in
/// first-appearance order.
fn mention_dids_of(facets: Option<&Value>) -> Vec<String> {
    let mut out: Vec<String> = Vec::new();
    for feature in facets
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .flat_map(|f| f.get("features").and_then(Value::as_array).into_iter().flatten())
    {
        if feature.get("$type").and_then(Value::as_str).is_some_and(|t| MENTION_TYPES.contains(&t))
            && let Some(did) = feature.get("did").and_then(Value::as_str)
            && !out.iter().any(|d| d == did)
        {
            out.push(did.to_string());
        }
    }
    out
}

/// The PDS's `quote`: the strong ref of a record or record-with-media
/// embed, in either vocabulary.
fn quote_ref_of(embed: Option<&Value>) -> Option<Value> {
    let e = embed?;
    let r = match e.get("$type")?.as_str()? {
        "app.bsky.embed.record" | "tools.kryphocron.feed.postPublic#embedRecord" => e.get("record")?,
        "app.bsky.embed.recordWithMedia" | "tools.kryphocron.feed.postPublic#embedRecordWithMedia" => e.get("record")?.get("record")?,
        _ => return None,
    };
    Some(json!({"uri": r.get("uri")?, "cid": r.get("cid")?}))
}

/// The PDS's `mediaKinds` (private-media-filters design, the locked shape):
/// `image` for images or a gallery, `video` for a video, at the top level
/// or as a record-with-media's media, in either vocabulary; `None` when the
/// embed has no media. Written out here, apart from Prism's classifier.
pub(super) fn media_kinds_of(embed: Option<&Value>) -> Option<Value> {
    let e = embed?;
    let media = match e.get("$type")?.as_str()? {
        "app.bsky.embed.recordWithMedia" | "tools.kryphocron.feed.postPublic#embedRecordWithMedia" => e.get("media")?,
        _ => e,
    };
    match media.get("$type")?.as_str()? {
        "app.bsky.embed.images"
        | "app.bsky.embed.gallery"
        | "tools.kryphocron.feed.postPublic#embedImage"
        | "tools.kryphocron.feed.postPublic#embedGallery" => Some(json!(["image"])),
        "app.bsky.embed.video" | "tools.kryphocron.feed.postPublic#embedVideo" => Some(json!(["video"])),
        _ => None,
    }
}

impl Fixture {
    /// A new-form record (the design's `encoded_content_post`): the payload
    /// encoded with the real hooks, and `embedBlobs`, `mentionDids`,
    /// `quote` and `mediaKinds` derived from the same facets and embed, each
    /// omitted when empty, as the PDS writes them. Tests that need other
    /// clear fields change the record after.
    pub(super) async fn new_form_record(&self, owner: &str, rkey: &str, text: &str, facets: Option<&Value>, embed: Option<&Value>, audience: &str) -> Value {
        let mut record = self.encoded_bytes(owner, rkey, &payload(text, facets, embed), Some(audience)).await;
        let mut blobs = Vec::new();
        if let Some(e) = embed {
            blobs_in(e, &mut blobs);
        }
        if !blobs.is_empty() {
            record["embedBlobs"] = json!(blobs);
        }
        let mentions = mention_dids_of(facets);
        if !mentions.is_empty() {
            record["mentionDids"] = json!(mentions);
        }
        if let Some(q) = quote_ref_of(embed) {
            record["quote"] = q;
        }
        if let Some(kinds) = media_kinds_of(embed) {
            record["mediaKinds"] = kinds;
        }
        record
    }

    /// An old-form record: `text` encoded, `facets` and `embed` in the clear.
    pub(super) async fn old_form_record(&self, owner: &str, rkey: &str, text: &str, facets: Option<&Value>, embed: Option<&Value>, audience: &str) -> Value {
        let mut record = self.encoded_post(owner, rkey, text, Some(audience)).await;
        if let Some(v) = facets {
            record["facets"] = v.clone();
        }
        if let Some(v) = embed {
            record["embed"] = v.clone();
        }
        record
    }

    /// Ingest `record` as `owner`'s private post `rkey`; its URI.
    pub(super) async fn put_post(&self, owner: &str, rkey: &str, record: Value) -> String {
        self.ingest(owner, NSID_POST_PRIVATE, rkey, record).await;
        format!("at://{owner}/{NSID_POST_PRIVATE}/{rkey}")
    }
}

/// The record parts of a view T-E1 compares: `record` and `embed`.
fn shown(view: &Value) -> (Value, Value) {
    (view["record"].clone(), view.get("embed").cloned().unwrap_or(Value::Null))
}

/// No clear-only field of the new form reaches a body.
fn assert_no_clear_fields(body: &str) {
    for key in ["embedBlobs", "mentionDids", "\"quote\":", "encodedContent", "mediaKinds"] {
        assert!(!body.contains(key), "{key} reached a body: {body}");
    }
}

/// Every `indexedAt` removed, at any depth: the one field of a view that
/// differs from run to run.
fn without_indexed_at(v: &mut Value) {
    match v {
        Value::Object(m) => {
            m.remove("indexedAt");
            for child in m.values_mut() {
                without_indexed_at(child);
            }
        }
        Value::Array(items) => items.iter_mut().for_each(without_indexed_at),
        _ => {}
    }
}

/// A private post's stored columns, as T-E2 compares them: everything but
/// the encoded bytes and the times the database sets.
async fn stored_columns(pool: &PgPool, uri: &str) -> Value {
    sqlx::query_scalar(
        "SELECT jsonb_build_object('uri', uri, 'author_did', author_did, 'rkey', rkey, 'cid', cid, 'rev', rev, \
           'audience_uri', audience_uri, 'reply_root_uri', reply_root_uri, 'reply_parent_uri', reply_parent_uri, \
           'reply_root_cid', reply_root_cid, 'reply_parent_cid', reply_parent_cid, 'encoded_codec', encoded_codec, \
           'has_encoded', encoded_content IS NOT NULL, 'plaintext_text', plaintext_text, 'created_at', created_at, \
           'facets', facets, 'embed', embed, 'langs', langs, 'self_labels', self_labels, 'quote_uri', quote_uri, \
           'mention_dids', mention_dids, 'display_filled', display_filled)::text \
         FROM private_posts WHERE uri = $1",
    )
    .bind(uri)
    .fetch_one(pool)
    .await
    .map(|s: String| serde_json::from_str(&s).expect("json"))
    .expect("row")
}

/// The golden view of T-E2's post, captured from the code before #504.
const GOLDEN_VIEW: &str = r#"{"uri":"at://did:plc:aaaaaaaaaaaaaaaaaaaaaaaa/tools.kryphocron.feed.postPrivate/3kgolden2222","cid":"bafy3kgolden2222","author":{"did":"did:plc:aaaaaaaaaaaaaaaaaaaaaaaa","handle":"alice.test","displayName":"alice.test","createdAt":"2026-01-01T00:00:00Z","viewer":{"muted":false,"blockedBy":false},"labels":[]},"record":{"$type":"tools.kryphocron.feed.postPrivate","text":"3kgolden2222","createdAt":"2026-09-26T12:00:00.000Z","facets":[{"index":{"byteEnd":5,"byteStart":0},"features":[{"did":"did:plc:cccccccccccccccccccccccc","$type":"tools.kryphocron.feed.postPublic#mention"}]},{"index":{"byteEnd":9,"byteStart":6},"features":[{"uri":"https://example.com","$type":"app.bsky.richtext.facet#link"}]}],"embed":{"$type":"tools.kryphocron.feed.postPublic#embedRecordWithMedia","media":{"$type":"tools.kryphocron.feed.postPublic#embedImage","images":[{"alt":"one","image":{"ref":{"$link":"bafyimg1"},"size":1,"$type":"blob","mimeType":"image/jpeg"}}]},"record":{"record":{"cid":"bafy3kquoted2222","uri":"at://did:plc:aaaaaaaaaaaaaaaaaaaaaaaa/tools.kryphocron.feed.postPrivate/3kquoted2222"}}},"langs":["en"],"labels":{"$type":"tools.kryphocron.feed.postPublic#selfLabels","values":[{"val":"nudity"}]},"audienceList":"at://did:plc:aaaaaaaaaaaaaaaaaaaaaaaa/tools.kryphocron.policy.audience/3kaudience22"},"embed":{"$type":"app.bsky.embed.recordWithMedia#view","record":{"$type":"app.bsky.embed.record#view","record":{"$type":"app.bsky.embed.record#viewRecord","uri":"at://did:plc:aaaaaaaaaaaaaaaaaaaaaaaa/tools.kryphocron.feed.postPrivate/3kquoted2222","cid":"bafy3kquoted2222","author":{"did":"did:plc:aaaaaaaaaaaaaaaaaaaaaaaa","handle":"alice.test","displayName":"alice.test","createdAt":"2026-01-01T00:00:00Z","viewer":{"muted":false,"blockedBy":false},"labels":[]},"value":{"$type":"tools.kryphocron.feed.postPrivate","text":"3kquoted2222","createdAt":"2026-09-26T12:00:00.000Z","audienceList":"at://did:plc:aaaaaaaaaaaaaaaaaaaaaaaa/tools.kryphocron.policy.audience/3kaudience22"},"labels":[],"quoteCount":1,"replyCount":0,"repostCount":0,"likeCount":0}},"media":{"$type":"app.bsky.embed.images#view","images":[{"thumb":"https://pds.example.com/xrpc/com.atproto.sync.getBlob?did=did:plc:aaaaaaaaaaaaaaaaaaaaaaaa&cid=bafyimg1","fullsize":"https://pds.example.com/xrpc/com.atproto.sync.getBlob?did=did:plc:aaaaaaaaaaaaaaaaaaaaaaaa&cid=bafyimg1","alt":"one"}]}},"labels":[{"src":"did:plc:aaaaaaaaaaaaaaaaaaaaaaaa","uri":"at://did:plc:aaaaaaaaaaaaaaaaaaaaaaaa/tools.kryphocron.feed.postPrivate/3kgolden2222","cid":"bafy3kgolden2222","val":"nudity","cts":"2026-09-26T12:00:00.000Z"}],"audience":{"uri":"at://did:plc:aaaaaaaaaaaaaaaaaaaaaaaa/tools.kryphocron.policy.audience/3kaudience22","mode":"list"},"replyCount":0,"repostCount":0,"likeCount":0,"quoteCount":0,"viewer":{"bookmarked":false}}"#;
/// The golden stored row of T-E2's post, captured from the code before #504.
const GOLDEN_ROW: &str = r#"{"cid":"bafy3kgolden2222","rev":"3kaaaaaaaaaa2","uri":"at://did:plc:aaaaaaaaaaaaaaaaaaaaaaaa/tools.kryphocron.feed.postPrivate/3kgolden2222","rkey":"3kgolden2222","embed":{"$type":"tools.kryphocron.feed.postPublic#embedRecordWithMedia","media":{"$type":"tools.kryphocron.feed.postPublic#embedImage","images":[{"alt":"one","image":{"ref":{"$link":"bafyimg1"},"size":1,"$type":"blob","mimeType":"image/jpeg"}}]},"record":{"record":{"cid":"bafy3kquoted2222","uri":"at://did:plc:aaaaaaaaaaaaaaaaaaaaaaaa/tools.kryphocron.feed.postPrivate/3kquoted2222"}}},"langs":["en"],"facets":[{"index":{"byteEnd":5,"byteStart":0},"features":[{"did":"did:plc:cccccccccccccccccccccccc","$type":"tools.kryphocron.feed.postPublic#mention"}]},{"index":{"byteEnd":9,"byteStart":6},"features":[{"uri":"https://example.com","$type":"app.bsky.richtext.facet#link"}]}],"quote_uri":"at://did:plc:aaaaaaaaaaaaaaaaaaaaaaaa/tools.kryphocron.feed.postPrivate/3kquoted2222","author_did":"did:plc:aaaaaaaaaaaaaaaaaaaaaaaa","created_at":"2026-09-26T12:00:00+00:00","has_encoded":true,"self_labels":{"$type":"tools.kryphocron.feed.postPublic#selfLabels","values":[{"val":"nudity"}]},"audience_uri":"at://did:plc:aaaaaaaaaaaaaaaaaaaaaaaa/tools.kryphocron.policy.audience/3kaudience22","mention_dids":["did:plc:cccccccccccccccccccccccc"],"encoded_codec":"laquna/0.2","display_filled":true,"plaintext_text":null,"reply_root_cid":null,"reply_root_uri":null,"reply_parent_cid":null,"reply_parent_uri":null}"#;

/// T-E2 (ask 4b): an old-form post with clear facets, a record-with-media
/// embed, langs and self-labels stores the same row and reads the same view
/// as before #504.
#[tokio::test]
async fn te2_old_form_post_is_unchanged() {
    let Some(db) = fresh_db("te2_old_form_golden").await else {
        return;
    };
    let f = fixture(&db.pool, "te2-golden").await;
    let aud = f.audience(ALICE, "3kaudience22", Some("list"), &[BOB]).await;
    let quoted = f.post_with(ALICE, "3kquoted2222", &aud, json!({})).await;
    let facets = json!([
        {"index": {"byteStart": 0, "byteEnd": 5}, "features": [{"$type": format!("{KLEX}#mention"), "did": CAROL}]},
        {"index": {"byteStart": 6, "byteEnd": 9}, "features": [{"$type": "app.bsky.richtext.facet#link", "uri": "https://example.com"}]},
    ]);
    let embed = json!({"$type": format!("{KLEX}#embedRecordWithMedia"),
        "record": {"record": {"uri": quoted, "cid": "bafy3kquoted2222"}},
        "media": {"$type": format!("{KLEX}#embedImage"), "images": [{"image": blob("bafyimg1"), "alt": "one"}]}});
    let post = f
        .post_with(ALICE, "3kgolden2222", &aud, json!({"facets": facets, "embed": embed, "langs": ["en"],
            "labels": {"$type": format!("{KLEX}#selfLabels"), "values": [{"val": "nudity"}]}}))
        .await;
    f.ready_all().await;
    let base = serve(f.state(Some(Duration::from_secs(1)))).await;
    let (_, mut view) = one_post(&base, BOB, &post).await;
    without_indexed_at(&mut view);
    let row = stored_columns(&f.pool, &post).await;
    assert_eq!(view.to_string(), GOLDEN_VIEW, "the view");
    assert_eq!(row.to_string(), GOLDEN_ROW, "the stored row");
    db.drop_db().await;
}

/// Facets with a mention (the kryphocron form) and a link.
fn sample_facets() -> Value {
    json!([
        {"index": {"byteStart": 0, "byteEnd": 5}, "features": [{"$type": format!("{KLEX}#mention"), "did": CAROL}]},
        {"index": {"byteStart": 6, "byteEnd": 9}, "features": [{"$type": "app.bsky.richtext.facet#link", "uri": "https://example.com/zebralink"}]},
    ])
}

/// Two images in the kryphocron form.
fn sample_images() -> Value {
    let image = |cid: &str| json!({"image": blob(cid), "alt": format!("alt {cid}"), "aspectRatio": {"width": 4, "height": 3}});
    json!({"$type": format!("{KLEX}#embedImage"), "images": [image("bafyimg1"), image("bafyimg2")]})
}

/// T-E1 (ask 1): a post of each embed kind, with facets, written once
/// old-form (clear fields) and once new-form: through `getPosts`,
/// `getPostThread`, `getAuthorFeed` and a quoting post's `#viewRecord`, the
/// two views' `record` and `embed` are equal. `embedBlobs`, `mentionDids`
/// and `quote` appear in neither.
#[tokio::test]
async fn te1_the_new_form_reads_as_the_old() {
    let Some(db) = fresh_db("te1_new_form_reads").await else {
        return;
    };
    let f = fixture(&db.pool, "te1-new-form").await;
    let aud = f.audience(ALICE, "3kaudience22", Some("list"), &[BOB]).await;
    let quoted = f.post_with(ALICE, "3kquoted2222", &aud, json!({"embed": sample_images()})).await;
    let quoted_ref = json!({"uri": quoted, "cid": "bafy3kquoted2222"});
    let kinds: Vec<(&str, Value)> = vec![
        ("images", sample_images()),
        ("gallery", json!({"$type": format!("{KLEX}#embedGallery"), "items": [
            {"$type": format!("{KLEX}#galleryImage"), "image": blob("bafyg1"), "alt": "g1", "aspectRatio": {"width": 2, "height": 1}}]})),
        ("video", json!({"$type": format!("{KLEX}#embedVideo"), "video": blob("bafyvideo"), "alt": "a clip",
            "captions": [{"lang": "en", "file": blob("bafycaption")}]})),
        ("external", json!({"$type": format!("{KLEX}#embedExternal"),
            "external": {"uri": "https://example.com", "title": "t", "description": "d", "thumb": blob("bafythumb")}})),
        ("record", json!({"$type": format!("{KLEX}#embedRecord"), "record": quoted_ref})),
        ("stockrecord", json!({"$type": "app.bsky.embed.record", "record": quoted_ref})),
        ("recwithmedia", json!({"$type": format!("{KLEX}#embedRecordWithMedia"), "record": {"record": quoted_ref}, "media": sample_images()})),
    ];
    let facets = sample_facets();
    let mut pairs = Vec::new();
    for (kind, embed) in &kinds {
        let text = format!("a post with {kind}");
        let old = f.old_form_record(ALICE, &format!("3kold{kind}"), &text, Some(&facets), Some(embed), &aud).await;
        let new = f.new_form_record(ALICE, &format!("3knew{kind}"), &text, Some(&facets), Some(embed), &aud).await;
        assert!(new.get("facets").is_none() && new.get("embed").is_none() && new.get("mentionDids").is_some());
        let old = f.put_post(ALICE, &format!("3kold{kind}"), old).await;
        let new = f.put_post(ALICE, &format!("3knew{kind}"), new).await;
        // A post quoting each, so each is also read as a `#viewRecord`.
        let quote = |uri: &str| json!({"embed": {"$type": format!("{KLEX}#embedRecord"), "record": {"uri": uri, "cid": "bafyany"}}});
        let quotes_old = f.post_with(ALICE, &format!("3kqold{kind}"), &aud, quote(&old)).await;
        let quotes_new = f.post_with(ALICE, &format!("3kqnew{kind}"), &aud, quote(&new)).await;
        pairs.push((*kind, old, new, quotes_old, quotes_new));
    }
    // A text-only new-form post, as the PDS writes every post once switched.
    let text_only = f.new_form_record(ALICE, "3knewtextonly", "just words", None, None, &aud).await;
    for key in ["embedBlobs", "mentionDids", "quote", "facets", "embed"] {
        assert!(text_only.get(key).is_none(), "{key}");
    }
    let text_only = f.put_post(ALICE, "3knewtextonly", text_only).await;
    f.ready_all().await;
    let base = serve(f.state(Some(Duration::from_secs(1)))).await;

    // getPosts.
    let uris: Vec<&str> = pairs.iter().flat_map(|(_, o, n, qo, qn)| [o.as_str(), n.as_str(), qo.as_str(), qn.as_str()]).collect();
    let mut posts = Vec::new();
    for chunk in uris.chunks(20) {
        let (body, page) = posts_of(&base, BOB, chunk).await;
        assert_no_clear_fields(&body);
        assert_eq!(page.len(), chunk.len(), "every post reads: {body}");
        posts.extend(page);
    }
    let by_uri = |uri: &str| posts.iter().find(|p| p["uri"] == uri).cloned().expect(uri);
    for (kind, old, new, quotes_old, quotes_new) in &pairs {
        let (o, n) = (by_uri(old), by_uri(new));
        assert_eq!(shown(&o), shown(&n), "getPosts, {kind}");
        assert!(n.get("embed").is_some(), "{kind}: an embed view");
        assert_eq!(n["record"]["facets"], sample_facets(), "{kind}");
        // The quoting posts' `#viewRecord`: the same value and embeds.
        let (qo, qn) = (by_uri(quotes_old), by_uri(quotes_new));
        let (ro, rn) = (&qo["embed"]["record"], &qn["embed"]["record"]);
        assert_eq!(rn["$type"], "app.bsky.embed.record#viewRecord", "{kind}: {qn}");
        assert_eq!((&ro["value"], &ro["embeds"]), (&rn["value"], &rn["embeds"]), "#viewRecord, {kind}");
        assert_eq!(rn["value"]["text"], format!("a post with {kind}"));
    }
    // The record kinds show the quoted post.
    let n = by_uri(&pairs[4].2);
    assert_eq!((n["embed"]["record"]["uri"].as_str(), n["embed"]["record"]["value"]["text"].as_str()), (Some(quoted.as_str()), Some("3kquoted2222")));
    let n = by_uri(&pairs[6].2);
    assert_eq!(n["embed"]["$type"], "app.bsky.embed.recordWithMedia#view");
    assert_eq!(n["embed"]["media"]["images"][1]["thumb"], blob_on_pds(ALICE, "bafyimg2"));

    // getPostThread.
    for (kind, old, new, ..) in &pairs {
        let (body_o, t_o) = thread_of(&base, BOB, &format!("uri={old}")).await;
        let (body_n, t_n) = thread_of(&base, BOB, &format!("uri={new}")).await;
        assert_no_clear_fields(&body_o);
        assert_no_clear_fields(&body_n);
        assert_eq!(shown(&t_o["post"]), shown(&t_n["post"]), "getPostThread, {kind}");
    }

    // getAuthorFeed.
    let feed = pages(&base, FEED, &format!("actor={ALICE}&limit=50"), BOB, 10).await;
    for page in &feed {
        assert_no_clear_fields(&page.to_string());
    }
    let items: Vec<Value> = feed.iter().flat_map(|p| p["feed"].as_array().cloned().unwrap_or_default()).collect();
    let in_feed = |uri: &str| items.iter().find(|i| i["uri"] == uri).cloned().expect(uri);
    for (kind, old, new, ..) in &pairs {
        assert_eq!(shown(&in_feed(old)), shown(&in_feed(new)), "getAuthorFeed, {kind}");
    }

    // The text-only new form reads with its text and nothing else.
    let (_, p) = one_post(&base, BOB, &text_only).await;
    assert_eq!(p["record"]["text"], "just words");
    assert!(p["record"].get("facets").is_none() && p["record"].get("embed").is_none() && p.get("embed").is_none(), "{p}");
    // Nothing of the payload is stored.
    let stored: (Option<Value>, Option<Value>) = sqlx::query_as("SELECT facets, embed FROM private_posts WHERE uri = $1")
        .bind(&pairs[0].2)
        .fetch_one(&f.pool)
        .await
        .expect("row");
    assert_eq!(stored, (None, None));
    db.drop_db().await;
}

/// The outcomes T-E5 tells apart.
const OUTCOMES: [&str; 7] = ["hit", "bad_content", "not_utf8", "codec_mismatch", "malformed", "decode_failed", "no_row"];

fn outcome_counts(handle: &PrometheusHandle) -> Vec<f64> {
    let text = handle.render();
    OUTCOMES
        .iter()
        .map(|o| crate::test_support::series(&text, pipeline::METRIC_OUTCOMES, &[&format!("outcome=\"{o}\"")]).unwrap_or(0.0))
        .collect()
}

/// `getPosts([uri])` for Bob: the one outcome it counted, and the post
/// (`Null` when it was left out).
async fn read_outcome(base: &str, handle: &PrometheusHandle, uri: &str) -> (&'static str, Value) {
    let before = outcome_counts(handle);
    let (_, post) = one_post(base, BOB, uri).await;
    let after = outcome_counts(handle);
    let changed: Vec<&'static str> = OUTCOMES.iter().zip(before.iter().zip(&after)).filter(|(_, (b, a))| a > b).map(|(o, _)| *o).collect();
    assert_eq!(changed.len(), 1, "{uri}: {changed:?}");
    (changed[0], post)
}

/// The content forms counted so far: `(text, content)`.
fn content_forms(handle: &PrometheusHandle) -> (f64, f64) {
    let text = handle.render();
    let form = |f: &str| crate::test_support::series(&text, pipeline::METRIC_CONTENT, &[&format!("form=\"{f}\"")]).unwrap_or(0.0);
    (form("text"), form("content"))
}

/// T-E5 (§5, each row at read: outcome and view) and T-E6 (the input cap).
#[tokio::test]
async fn te5_te6_hostile_payloads_and_the_input_cap() {
    let Some(db) = fresh_db("te5_hostile_payloads").await else {
        return;
    };
    let recorder = metrics_exporter_prometheus::PrometheusBuilder::new().build_recorder();
    let handle = recorder.handle();
    let _local = metrics::set_default_local_recorder(&recorder);
    let f = fixture(&db.pool, "te5-hostile").await;
    let aud = f.audience(ALICE, "3kaudience22", Some("list"), &[BOB]).await;
    let images = sample_images();
    let facets = sample_facets();
    let other_facets = json!([{"index": {"byteStart": 0, "byteEnd": 1}, "features": [{"$type": "app.bsky.richtext.facet#tag", "tag": "x"}]}]);
    let external = json!({"$type": format!("{KLEX}#embedExternal"), "external": {"uri": "https://example.com", "title": "t", "description": "d"}});

    // Misses.
    let mut misses: Vec<(&str, Value, &str)> = Vec::new();
    for (rkey, bytes) in [
        ("3kbadjson222", raw_payload(b"{text")),
        ("3kbadarray22", raw_payload(b"[1]")),
        ("3kbadstring2", raw_payload(b"\"text\"")),
        ("3kbadnumber2", raw_payload(b"5")),
        ("3knotext2222", raw_payload(br#"{"facets": []}"#)),
        ("3ktextnum222", raw_payload(br#"{"text": 5}"#)),
        ("3kffalone222", raw_payload(b"")),
        ("3kffspace222", raw_payload(b"  \n")),
        ("3kdeepjson22", raw_payload(format!(r#"{{"text": "t", "x": {}{}}}"#, "[".repeat(200), "]".repeat(200)).as_bytes())),
    ] {
        misses.push((rkey, f.encoded_bytes(ALICE, rkey, &bytes, Some(&aud)).await, "bad_content"));
    }
    // A payload over the cap now fails in the codec, before it is restored
    // (S3; T-S3 below).
    let big = payload(&"a".repeat(1_000_001), None, None);
    misses.push(("3kbigpayload", f.encoded_bytes(ALICE, "3kbigpayload", &big, Some(&aud)).await, "decode_failed"));
    // An old-form payload that is not UTF-8: today's miss.
    misses.push(("3knotutf8222", f.encoded_bytes(ALICE, "3knotutf8222", &[0xC3, 0x28], Some(&aud)).await, "not_utf8"));
    // A new-form payload under another codec id.
    let mut foreign = f.encoded_bytes(ALICE, "3kforeign222", &payload("t", None, None), Some(&aud)).await;
    foreign["encodedContentCodec"] = json!("foreign/1.0");
    misses.push(("3kforeign222", foreign, "codec_mismatch"));
    // Clear text beside `encodedContent`: the XOR check.
    let mut both = f.encoded_bytes(ALICE, "3kbothtext22", &payload("t", None, None), Some(&aud)).await;
    both["text"] = json!("clear");
    misses.push(("3kbothtext22", both, "malformed"));
    // T-E6: 1 000 001 bytes that are not laquna at all are never decoded
    // (`bad_content`), where a smaller run of the same garbage is decoded
    // and fails (`decode_failed`).
    let garbage = |n: usize| -> String { (0..n).map(|i| format!("{:02x}", (i * 7 + 3) % 251)).collect() };
    for (rkey, n, outcome) in [("3khugeinput2", 1_000_001, "bad_content"), ("3kgarbage222", 1_000, "decode_failed")] {
        let mut record = f.encoded_post(ALICE, rkey, "x", Some(&aud)).await;
        record["encodedContent"] = json!(garbage(n));
        misses.push((rkey, record, outcome));
    }
    let mut miss_uris = Vec::new();
    for (rkey, record, outcome) in misses {
        miss_uris.push((f.put_post(ALICE, rkey, record).await, outcome));
    }

    // Reads.
    let wrong_payload = raw_payload(br#"{"text": "w", "facets": {"a": 1}, "embed": [1]}"#);
    let mut wrong_types = f.encoded_bytes(ALICE, "3kwrongtype2", &wrong_payload, Some(&aud)).await;
    wrong_types["facets"] = facets.clone();
    wrong_types["embed"] = images.clone();
    let wrong_types = f.put_post(ALICE, "3kwrongtype2", wrong_types).await;
    let oversize_embed = json!({"$type": "app.bsky.embed.external", "external": {"uri": "x", "title": "t", "description": "d".repeat(70_000)}});
    let oversize = f.new_form_record(ALICE, "3koversize22", "o", Some(&facets), Some(&oversize_embed), &aud).await;
    let oversize = f.put_post(ALICE, "3koversize22", oversize).await;
    let nulls = raw_payload(br#"{"text": "n", "facets": null, "embed": null}"#);
    let mut null_fields = f.encoded_bytes(ALICE, "3knullfield2", &nulls, Some(&aud)).await;
    null_fields["facets"] = facets.clone();
    null_fields["embed"] = images.clone();
    let null_fields = f.put_post(ALICE, "3knullfield2", null_fields).await;
    let extra = serde_json::to_vec(&json!({"text": "e", "reply": {"root": {"uri": quoted_elsewhere()}, "parent": {"uri": quoted_elsewhere()}},
        "audienceList": "at://did:plc:zzzzzzzzzzzzzzzzzzzzzzzz/tools.kryphocron.policy.audience/x", "langs": ["xx"],
        "labels": {"values": [{"val": "porn"}]}, "quote": {"uri": quoted_elsewhere(), "cid": "c"}, "mentionDids": [DAVE],
        "embedBlobs": [blob("bafyextra")], "$type": "app.bsky.feed.post", "createdAt": "2020-01-01T00:00:00Z"}))
    .expect("json");
    let mut extra_keys = f.encoded_bytes(ALICE, "3kextrakeys2", &raw_payload(&extra), Some(&aud)).await;
    extra_keys["langs"] = json!(["en"]);
    extra_keys["mentionDids"] = json!([CAROL]);
    extra_keys["quote"] = json!({"uri": quoted_elsewhere(), "cid": "c"});
    extra_keys["embedBlobs"] = json!([blob("bafyclear")]);
    let extra_keys = f.put_post(ALICE, "3kextrakeys2", extra_keys).await;
    let mut both_clear = f.new_form_record(ALICE, "3kbothclear2", "b", Some(&facets), Some(&images), &aud).await;
    both_clear["facets"] = other_facets.clone();
    both_clear["embed"] = external.clone();
    let both_clear = f.put_post(ALICE, "3kbothclear2", both_clear).await;
    let mut no_embed = f.new_form_record(ALICE, "3knoembed222", "ne", Some(&facets), None, &aud).await;
    no_embed["embed"] = external.clone();
    no_embed["facets"] = other_facets.clone();
    let no_embed = f.put_post(ALICE, "3knoembed222", no_embed).await;
    let json_text = r#"{"text": "not a payload"}"#;
    let json_like = f.put_post(ALICE, "3kjsonlike22", f.encoded_post(ALICE, "3kjsonlike22", json_text, Some(&aud)).await).await;
    let y_acute = f.put_post(ALICE, "3kyacute2222", f.encoded_post(ALICE, "3kyacute2222", "ÿes", Some(&aud)).await).await;
    let plain = f.put_post(ALICE, "3kplaintext2", json!({"$type": NSID_POST_PRIVATE, "text": "plain", "audienceList": aud})).await;
    f.ready_all().await;
    let base = serve(f.state_floored(Some(Duration::from_secs(1)), Duration::ZERO)).await;

    for (uri, expected) in &miss_uris {
        let (outcome, post) = read_outcome(&base, &handle, uri).await;
        assert_eq!((outcome, post), (*expected, Value::Null), "{uri}");
    }
    let forms = content_forms(&handle);
    let read = |uri: String| {
        let (base, handle) = (base.clone(), handle.clone());
        async move {
            let (outcome, post) = read_outcome(&base, &handle, &uri).await;
            assert_eq!(outcome, "hit", "{uri}");
            assert_no_clear_fields(&post.to_string());
            post
        }
    };
    // Clear index fields the payload does not back are an over-claim since
    // private-encode E2: a miss for every reader but the author, who reads
    // the post as before (§6.3). Each such read decodes, so it is counted
    // under its form.
    let as_author = |uri: String| {
        let (base, handle) = (base.clone(), handle.clone());
        async move {
            let mismatch = || crate::test_support::series(&handle.render(), pipeline::METRIC_OUTCOMES, &["outcome=\"index_mismatch\""]).unwrap_or(0.0);
            let before = mismatch();
            assert!(one_post(&base, BOB, &uri).await.1.is_null(), "{uri}: a miss for Bob");
            assert_eq!(mismatch(), before + 1.0, "{uri}");
            let (_, post) = one_post(&base, ALICE, &uri).await;
            assert!(!post.is_null(), "{uri}: the author reads it");
            assert_no_clear_fields(&post.to_string());
            post
        }
    };
    // A wrong-typed payload field is left out, and the clear one does not
    // stand in for it (the clear fields indexed a mention and an image the
    // payload lacks: an over-claim).
    let p = as_author(wrong_types).await;
    assert_eq!(p["record"]["text"], "w");
    assert!(p["record"].get("facets").is_none() && p["record"].get("embed").is_none() && p.get("embed").is_none(), "{p}");
    // Over 64 KiB: left out; the facets beside it stay.
    let p = read(oversize).await;
    assert!(p["record"].get("embed").is_none() && p.get("embed").is_none(), "{p}");
    assert_eq!(p["record"]["facets"], facets);
    // Null is absent: the clear fields show.
    let p = read(null_fields).await;
    assert_eq!((&p["record"]["facets"], &p["record"]["embed"]), (&facets, &images));
    assert_eq!(p["embed"]["$type"], "app.bsky.embed.images#view");
    // Extra payload keys never reach the view; the clear ones are the
    // columns' or nothing (the clear `mentionDids` and `quote` over-claim).
    let p = as_author(extra_keys).await;
    assert_eq!(p["record"]["text"], "e");
    assert_eq!(p["record"]["langs"], json!(["en"]));
    assert_eq!(p["record"]["audienceList"], aud.as_str());
    assert_eq!(p["record"]["$type"], NSID_POST_PRIVATE);
    assert_eq!(p["record"]["createdAt"], "2026-09-26T12:00:00.000Z");
    for key in ["reply", "labels", "quote", "mentionDids", "embedBlobs"] {
        assert!(p["record"].get(key).is_none(), "{key}: {p}");
    }
    assert_eq!(p["labels"], json!([]));
    // Payload and clear fields both present: the payload's, per key.
    let p = read(both_clear).await;
    assert_eq!((&p["record"]["facets"], &p["record"]["embed"]), (&facets, &images));
    // The payload lacks `embed`: the clear one shows; its facets are the
    // payload's.
    let p = read(no_embed).await;
    assert_eq!((&p["record"]["facets"], &p["record"]["embed"]), (&facets, &external));
    assert_eq!(p["embed"]["$type"], "app.bsky.embed.external#view");
    // Old-form text that looks like JSON, or begins with U+00FF: verbatim.
    assert_eq!(read(json_like).await["record"]["text"], json_text);
    assert_eq!(read(y_acute).await["record"]["text"], "ÿes");
    // Six new-form posts (two of them decoded twice: Bob's miss and the
    // author's read) and two old-form; the plaintext post decodes nothing
    // and is not counted. Since private-encode E4 it is refused (S4, on by
    // default): a miss, outcome `plaintext`; with the switch off, served.
    assert_eq!(content_forms(&handle), (forms.0 + 2.0, forms.1 + 8.0));
    let refused = || crate::test_support::series(&handle.render(), pipeline::METRIC_OUTCOMES, &["outcome=\"plaintext\""]).unwrap_or(0.0);
    let before = refused();
    assert!(one_post(&base, BOB, &plain).await.1.is_null());
    assert_eq!(refused(), before + 1.0);
    let lenient = serve(f.state_floored(Some(Duration::from_secs(1)), Duration::ZERO).with_plaintext_refused(false)).await;
    assert_eq!(one_post(&lenient, BOB, &plain).await.1["record"]["text"], "plain");
    assert_eq!(content_forms(&handle), (forms.0 + 2.0, forms.1 + 8.0));
    db.drop_db().await;
}

/// T-S3 (S3, the codec's decode bound): with the hooks `serve` builds, a
/// payload of exactly `PAYLOAD_MAX_BYTES` reads; one byte more fails in the
/// codec (`decode_failed`; past the codec it would be `bad_content`); and a
/// 64 MiB payload, whose `encodedContent` is far under the input cap, fails
/// in the codec too, without being expanded.
#[tokio::test]
async fn ts3_the_codec_bounds_what_it_decodes() {
    let Some(db) = fresh_db("ts3_decode_bound").await else {
        return;
    };
    let recorder = metrics_exporter_prometheus::PrometheusBuilder::new().build_recorder();
    let handle = recorder.handle();
    let _local = metrics::set_default_local_recorder(&recorder);
    let f = fixture(&db.pool, "ts3-bound").await;
    let aud = f.audience(ALICE, "3kaudience22", Some("list"), &[BOB]).await;
    let max = crate::kryphocron_glue::PAYLOAD_MAX_BYTES;

    let at_cap = vec![b'a'; max];
    let at_cap = f.put_post(ALICE, "3katcap22222", f.encoded_bytes(ALICE, "3katcap22222", &at_cap, Some(&aud)).await).await;
    let over = vec![b'a'; max + 1];
    let over = f.put_post(ALICE, "3kovercap222", f.encoded_bytes(ALICE, "3kovercap222", &over, Some(&aud)).await).await;
    // An RLE bomb: 64 MiB of one byte compresses to a few kilobytes.
    let bomb = f.encoded_bytes(ALICE, "3kbomb222222", &vec![b'a'; 64 << 20], Some(&aud)).await;
    let bomb_input = bomb["encodedContent"].as_str().expect("hex").len() / 2;
    assert!(bomb_input < 64 * 1024, "the bomb is small on the wire: {bomb_input} bytes");
    let bomb = f.put_post(ALICE, "3kbomb222222", bomb).await;
    f.ready_all().await;
    let base = serve(f.state_floored(Some(Duration::from_secs(1)), Duration::ZERO)).await;

    let (outcome, post) = read_outcome(&base, &handle, &at_cap).await;
    assert_eq!(outcome, "hit");
    assert_eq!(post["record"]["text"].as_str().map(str::len), Some(max));
    assert_eq!(read_outcome(&base, &handle, &over).await, ("decode_failed", Value::Null));
    let started = Instant::now();
    assert_eq!(read_outcome(&base, &handle, &bomb).await, ("decode_failed", Value::Null));
    // Unbounded, the bomb is 64 MiB of output; bounded, the frame header
    // alone refuses it.
    assert!(started.elapsed() < Duration::from_secs(10), "{:?}", started.elapsed());
    db.drop_db().await;
}

/// A post URI no fixture stores.
fn quoted_elsewhere() -> String {
    format!("at://{DAVE}/{NSID_POST_PRIVATE}/3kelsewhere2")
}

/// T-E7 (ask 3): `searchPosts` finds a new-form post by a word of its text,
/// and not by its facets, its embed, a `$type` or a mentioned DID.
#[tokio::test]
async fn te7_search_matches_the_restored_text_only() {
    let Some(db) = fresh_db("te7_search_new_form").await else {
        return;
    };
    let f = fixture(&db.pool, "te7-search").await;
    let aud = f.audience(ALICE, "3kaudience22", Some("list"), &[BOB]).await;
    let embed = json!({"$type": format!("{KLEX}#embedExternal"),
        "external": {"uri": "https://example.com/zebrauri", "title": "zebratitle", "description": "zebradesc", "thumb": blob("bafythumb")}});
    let mut record = f.new_form_record(ALICE, "3ksearch2222", "giraffes are tall", Some(&sample_facets()), Some(&embed), &aud).await;
    record["createdAt"] = json!((Utc::now() - chrono::Duration::hours(1)).to_rfc3339_opts(SecondsFormat::Millis, true));
    let post = f.put_post(ALICE, "3ksearch2222", record).await;
    f.ready_all().await;
    let base = f.search_server(SEARCH_FLOOR, &PrivateSearchConfig::default()).await;
    let (_, v, _) = json_of(&base, SEARCH, "q=giraffes", BOB).await;
    assert_eq!(post_uris(&v), std::slice::from_ref(&post), "{v}");
    assert_eq!(v["posts"][0]["record"]["embed"]["external"]["title"], "zebratitle", "the view carries the embed");
    for q in ["zebratitle", "zebradesc", "zebrauri", "zebralink", "embedExternal", "postPublic", CAROL] {
        let (_, v, _) = json_of(&base, SEARCH, &format!("q={}", urlencoding(q)), BOB).await;
        assert!(post_uris(&v).is_empty(), "{q}: {v}");
    }
    db.drop_db().await;
}

// ---- S2: ingest from the clear `quote` and `mentionDids`, and the
// notification rules (private-embeds design §4, §6) ----

/// `createdAt` for a post made `ago` before now.
fn made_ago(ago: chrono::Duration) -> Value {
    json!((Utc::now() - ago).to_rfc3339_opts(SecondsFormat::Millis, true))
}

/// A stored post's `(quote_uri, mention_dids)`.
async fn indexed(pool: &PgPool, uri: &str) -> (Option<String>, Vec<String>) {
    sqlx::query_as("SELECT quote_uri, mention_dids FROM private_posts WHERE uri = $1")
        .bind(uri)
        .fetch_one(pool)
        .await
        .expect("row")
}

/// The private notification rows whose record is `uri`, as `(recipient,
/// reason)`, in the order written.
async fn rows_of_record(pool: &PgPool, uri: &str) -> Vec<(String, String)> {
    sqlx::query_as("SELECT recipient_did, reason FROM private_notifications WHERE record_uri = $1 ORDER BY id")
        .bind(uri)
        .fetch_all(pool)
        .await
        .expect("rows")
}

/// Retention's effect on one record's notifications: they are gone.
async fn drop_rows_of_record(pool: &PgPool, uri: &str) {
    sqlx::query("DELETE FROM private_notifications WHERE record_uri = $1")
        .bind(uri)
        .execute(pool)
        .await
        .expect("delete");
}

/// A facet mentioning each of `dids` in the `$type` form `mention_type`.
fn mentions(mention_type: &str, dids: &[&str]) -> Value {
    Value::Array(
        dids.iter()
            .map(|d| json!({"index": {"byteStart": 0, "byteEnd": 1}, "features": [{"$type": mention_type, "did": d}]}))
            .collect(),
    )
}

/// A record embed of `uri` in the `$type` form `embed_type`.
fn record_embed(embed_type: &str, uri: &str) -> Value {
    json!({"$type": embed_type, "record": {"uri": uri, "cid": "bafyquoted"}})
}

/// Alice's audience of Bob, Carol and Dave, and Dave's post (under his
/// audience of the other three) for her posts to quote.
async fn quoting_setup(f: &Fixture) -> (String, String) {
    let aud = f.audience(ALICE, "3kaudience22", Some("list"), &[BOB, CAROL, DAVE]).await;
    let dave_aud = f.audience(DAVE, "3kauddave222", Some("list"), &[ALICE, BOB, CAROL]).await;
    let quoted = f.private_post(DAVE, "3kdavepost22", &dave_aud, None).await;
    (aud, quoted)
}

/// T-E3 (ask 2), and the unknown-field check: a new-form post (no clear
/// `facets` or `embed`; `embedBlobs`, `mentionDids` and `quote`, which
/// kryphocron-lexicons 0.3.1 does not define) is written, not
/// dead-lettered; `mentionDids` naming a member and `quote` naming a
/// member's private post write `mention` and `quote` notifications and
/// are stored; the quoted post's quote count and `getQuotes` include it;
/// a threadgate `mentionRule` on a new-form root admits the mentioned
/// member's reply.
#[tokio::test]
async fn te3_new_form_posts_index_their_quote_and_mentions() {
    let Some(db) = fresh_db("te3_new_form_ingest").await else {
        return;
    };
    let f = fixture(&db.pool, "te3-ingest").await;
    let (aud, quoted) = quoting_setup(&f).await;
    let facets = mentions(&format!("{KLEX}#mention"), &[CAROL]);
    let embed = json!({"$type": format!("{KLEX}#embedRecordWithMedia"), "record": {"record": {"uri": quoted, "cid": "bafyquoted"}},
        "media": sample_images()});
    let record = f.new_form_record(ALICE, "3knewpost222", "hello", Some(&facets), Some(&embed), &aud).await;
    assert!(record.get("facets").is_none() && record.get("embed").is_none());
    assert_eq!((record["mentionDids"].clone(), record["quote"]["uri"].clone()), (json!([CAROL]), json!(quoted)));
    assert_eq!(record["embedBlobs"].as_array().map(Vec::len), Some(2));
    // Through the firehose writer: `ingest` asserts the op did not fail.
    let report = f
        .writer
        .apply(
            ALICE,
            REV,
            &[RawOp {
                action: Action::Create,
                collection: NSID_POST_PRIVATE.into(),
                rkey: "3knewpost222".into(),
                cid: Some("bafy3knewpost222".into()),
                record: Some(record),
            }],
            ApplyOptions {
                advance_applied_rev: false,
                dead_letter_readiness: false,
                attempts: 1,
                live: false,
                subscription_rules: false,
                source: WriteSource::Firehose,
            },
        )
        .await
        .expect("apply");
    assert!(matches!(report.outcomes[..], [OpOutcome::Written]) && report.failed.is_empty(), "{report:?}");
    let post = format!("at://{ALICE}/{NSID_POST_PRIVATE}/3knewpost222");
    assert_eq!(indexed(&f.pool, &post).await, (Some(quoted.clone()), vec![CAROL.to_string()]));
    assert_eq!(private_rows(&f.pool, CAROL).await, [notified("mention", ALICE, None, &post, false)]);
    assert_eq!(private_rows(&f.pool, DAVE).await, [notified("quote", ALICE, Some(&quoted), &post, false)]);

    // A new-form root mentioning Carol, with a `mentionRule` threadgate.
    let root = f.new_form_record(ALICE, "3knewroot222", "root", Some(&facets), None, &aud).await;
    let root = f.put_post(ALICE, "3knewroot222", root).await;
    f.ingest(ALICE, THREADGATE, "3knewroot222", json!({"$type": THREADGATE, "post": root, "allow": [{"$type": MENTION_RULE}],
        "createdAt": "2026-09-26T12:00:00Z"}))
        .await;
    f.ready_all().await;
    let base = serve(f.state_floored(Some(Duration::from_secs(1)), Duration::ZERO)).await;

    // The quoted post's count and `getQuotes`.
    let (_, quoted_view) = posts_of(&base, BOB, &[&quoted]).await;
    assert_eq!(quoted_view[0]["quoteCount"], 1, "{quoted_view:?}");
    let (_, page, _) = json_of(&base, GET_QUOTES, &format!("uri={quoted}"), BOB).await;
    assert_eq!(page["posts"].as_array().map(|p| p.iter().map(|v| v["uri"].clone()).collect::<Vec<_>>()), Some(vec![json!(post)]), "{page}");
    // The view shows the restored quote and media.
    let (_, p) = posts_of(&base, BOB, &[&post]).await;
    assert_eq!(p[0]["embed"]["record"]["record"]["uri"], quoted.as_str(), "{p:?}");
    // `mentionRule`: Carol, mentioned, may reply; Bob may not.
    let (_, p) = posts_of(&base, CAROL, &[&root]).await;
    assert_eq!(p[0]["viewer"]["replyDisabled"], false, "{p:?}");
    let (_, p) = posts_of(&base, BOB, &[&root]).await;
    assert_eq!(p[0]["viewer"]["replyDisabled"], true, "{p:?}");
    db.drop_db().await;
}

/// T-E4 (ask 4a), one run: an old-form post `age` old mentioning Carol and
/// quoting Dave's post (in the stock or the kryphocron `$type` forms), then
/// rewritten to the new form by a live update (new CID and rev): the
/// indexed quote and mentions are unchanged and no notification is
/// written; with the rows then deleted, as retention does, a second
/// rewrite writes none either.
async fn rewrite_in(test: &str, age: chrono::Duration, kryphocron: bool) {
    let Some(db) = fresh_db(test).await else {
        return;
    };
    let f = fixture(&db.pool, test).await;
    let (aud, quoted) = quoting_setup(&f).await;
    let (mention_type, embed_type) = if kryphocron {
        (format!("{KLEX}#mention"), format!("{KLEX}#embedRecord"))
    } else {
        ("app.bsky.richtext.facet#mention".to_string(), "app.bsky.embed.record".to_string())
    };
    let facets = mentions(&mention_type, &[CAROL]);
    let embed = record_embed(&embed_type, &quoted);
    let mut old = f.old_form_record(ALICE, "3krewrite222", "words", Some(&facets), Some(&embed), &aud).await;
    old["createdAt"] = made_ago(age);
    let post = f.put_post(ALICE, "3krewrite222", old).await;
    let index = (Some(quoted.clone()), vec![CAROL.to_string()]);
    assert_eq!(indexed(&f.pool, &post).await, index);
    let written = vec![(DAVE.to_string(), "quote".to_string()), (CAROL.to_string(), "mention".to_string())];
    assert_eq!(rows_of_record(&f.pool, &post).await, written, "the create notifies");

    let mut new = f.new_form_record(ALICE, "3krewrite222", "words", Some(&facets), Some(&embed), &aud).await;
    new["createdAt"] = made_ago(age);
    f.apply_op(ALICE, Action::Update, NSID_POST_PRIVATE, "3krewrite222", REV2, Some(new.clone())).await;
    let cid: String = sqlx::query_scalar("SELECT cid FROM private_posts WHERE uri = $1").bind(&post).fetch_one(&f.pool).await.expect("cid");
    assert_eq!(cid, format!("bafy3krewrite222{REV2}"), "the rewrite was written");
    assert_eq!(indexed(&f.pool, &post).await, index, "the rewrite keeps the index");
    assert_eq!(rows_of_record(&f.pool, &post).await, written, "and writes no notification");

    drop_rows_of_record(&f.pool, &post).await;
    f.apply_op(ALICE, Action::Update, NSID_POST_PRIVATE, "3krewrite222", REV3, Some(new)).await;
    assert_eq!(indexed(&f.pool, &post).await, index);
    assert!(rows_of_record(&f.pool, &post).await.is_empty(), "a second rewrite notifies no one again");
    db.drop_db().await;
}

#[tokio::test]
async fn te4_rewrite_of_a_recent_post_stock_types() {
    rewrite_in("te4-recent-stock", chrono::Duration::hours(1), false).await;
}

#[tokio::test]
async fn te4_rewrite_of_a_recent_post_kryphocron_types() {
    rewrite_in("te4-recent-klex", chrono::Duration::hours(1), true).await;
}

#[tokio::test]
async fn te4_rewrite_of_an_old_post_stock_types() {
    rewrite_in("te4-old-stock", chrono::Duration::days(3), false).await;
}

#[tokio::test]
async fn te4_rewrite_of_an_old_post_kryphocron_types() {
    rewrite_in("te4-old-klex", chrono::Duration::days(3), true).await;
}

/// Alice's old-form post `rkey` mentioning `dids`, made `ago` before now.
async fn mentioning(f: &Fixture, aud: &str, rkey: &str, dids: &[&str], ago: chrono::Duration) -> Value {
    let mut r = f.old_form_record(ALICE, rkey, "t", Some(&mentions("app.bsky.richtext.facet#mention", dids)), None, aud).await;
    r["createdAt"] = made_ago(ago);
    r
}

/// T-E8 (the notification rules, private-embeds design §6).
#[tokio::test]
async fn te8_updates_notify_only_new_recipients_of_recent_posts() {
    let Some(db) = fresh_db("te8_notify_rules").await else {
        return;
    };
    let recorder = metrics_exporter_prometheus::PrometheusBuilder::new().build_recorder();
    let handle = recorder.handle();
    let _local = metrics::set_default_local_recorder(&recorder);
    let f = fixture(&db.pool, "te8-rules").await;
    let aud = f.audience(ALICE, "3kaudience22", Some("list"), &[BOB, CAROL, DAVE]).await;
    let (old, recent) = (chrono::Duration::days(3), chrono::Duration::hours(1));
    let suppressed = |reason: &str| {
        crate::test_support::series(&handle.render(), crate::private_tier::ingest::METRIC_NOTIFY_SUPPRESSED, &[&format!("reason=\"{reason}\"")])
            .unwrap_or(0.0)
    };
    let recipients = |rows: Vec<(String, String)>| rows.into_iter().map(|(d, _)| d).collect::<Vec<_>>();

    // A live create of a 3-day-old post notifies, as before.
    let p1 = f.put_post(ALICE, "3kte8create2", mentioning(&f, &aud, "3kte8create2", &[CAROL], old).await).await;
    assert_eq!(recipients(rows_of_record(&f.pool, &p1).await), [CAROL]);
    // A live update of it adding Dave notifies no one.
    let updated = mentioning(&f, &aud, "3kte8create2", &[CAROL, DAVE], old).await;
    f.apply_op(ALICE, Action::Update, NSID_POST_PRIVATE, "3kte8create2", REV2, Some(updated)).await;
    assert_eq!(indexed(&f.pool, &p1).await.1, [CAROL, DAVE], "written");
    assert_eq!(recipients(rows_of_record(&f.pool, &p1).await), [CAROL], "Dave is not notified");
    assert_eq!(suppressed("old_update"), 1.0);
    // Nor does a live update of a 3-day-old post Prism holds no row for.
    let norow = mentioning(&f, &aud, "3kte8norow22", &[DAVE], old).await;
    f.apply_op(ALICE, Action::Update, NSID_POST_PRIVATE, "3kte8norow22", REV, Some(norow)).await;
    let p2 = format!("at://{ALICE}/{NSID_POST_PRIVATE}/3kte8norow22");
    assert_eq!(indexed(&f.pool, &p2).await.1, [DAVE], "written");
    assert!(rows_of_record(&f.pool, &p2).await.is_empty());

    // A 1-hour-old post: an update adding Dave notifies Dave alone, even
    // with Carol's first row gone.
    let p3 = f.put_post(ALICE, "3kte8recent2", mentioning(&f, &aud, "3kte8recent2", &[CAROL], recent).await).await;
    assert_eq!(recipients(rows_of_record(&f.pool, &p3).await), [CAROL]);
    drop_rows_of_record(&f.pool, &p3).await;
    let updated = mentioning(&f, &aud, "3kte8recent2", &[CAROL, DAVE], recent).await;
    f.apply_op(ALICE, Action::Update, NSID_POST_PRIVATE, "3kte8recent2", REV2, Some(updated)).await;
    assert_eq!(recipients(rows_of_record(&f.pool, &p3).await), [DAVE], "only the new recipient");
    assert_eq!(suppressed("known_recipient"), 1.0);

    // Eve, named while not a member, joins; an update naming her again and
    // Bob for the first time notifies Bob alone.
    let p4 = f.put_post(ALICE, "3kte8joiner2", mentioning(&f, &aud, "3kte8joiner2", &[EVE], recent).await).await;
    assert!(rows_of_record(&f.pool, &p4).await.is_empty(), "Eve is not a member yet");
    exec(&f.pool, &format!("INSERT INTO members (did, source, ready_at) VALUES ('{EVE}', 'admin', now())")).await;
    let updated = mentioning(&f, &aud, "3kte8joiner2", &[EVE, BOB], recent).await;
    f.apply_op(ALICE, Action::Update, NSID_POST_PRIVATE, "3kte8joiner2", REV2, Some(updated)).await;
    assert_eq!(recipients(rows_of_record(&f.pool, &p4).await), [BOB]);

    // A backfill write of a 3-day-old post notifies no one (C-11).
    let backfilled = mentioning(&f, &aud, "3kte8backfil", &[BOB], old).await;
    f.apply_from(WriteSource::Backfill, ALICE, Action::Create, NSID_POST_PRIVATE, "3kte8backfil", REV, Some(backfilled)).await;
    assert!(rows_of_record(&f.pool, &format!("at://{ALICE}/{NSID_POST_PRIVATE}/3kte8backfil")).await.is_empty());

    // A member's public reply to the 3-day-old post notifies as before.
    f.ingest(BOB, PUBLIC_POST, "3kpubreply22", public_reply(&p1, "2026-10-01T10:00:00Z", None)).await;
    let reply = format!("at://{BOB}/{PUBLIC_POST}/3kpubreply22");
    assert_eq!(rows_of_record(&f.pool, &reply).await, [(ALICE.to_string(), "reply".to_string())]);
    db.drop_db().await;
}

/// T-E9 (§4.3's rules).
#[tokio::test]
async fn te9_clear_fields_index_and_the_payload_displays() {
    let Some(db) = fresh_db("te9_index_rules").await else {
        return;
    };
    let recorder = metrics_exporter_prometheus::PrometheusBuilder::new().build_recorder();
    let handle = recorder.handle();
    let _local = metrics::set_default_local_recorder(&recorder);
    let f = fixture(&db.pool, "te9-rules").await;
    let (aud, x) = quoting_setup(&f).await;
    let y = f.post_with(ALICE, "3kypost22222", &aud, json!({})).await;
    let facets = mentions(&format!("{KLEX}#mention"), &[CAROL]);
    let embed_type = format!("{KLEX}#embedRecord");

    // (a) New form without `quote` or `mentionDids`: nothing indexed, no
    // notification; the view still shows the quote and the facet.
    let mut a = f.new_form_record(ALICE, "3kte9a222222", "a", Some(&facets), Some(&record_embed(&embed_type, &x)), &aud).await;
    a.as_object_mut().expect("object").remove("quote");
    a.as_object_mut().expect("object").remove("mentionDids");
    let a = f.put_post(ALICE, "3kte9a222222", a).await;
    assert_eq!(indexed(&f.pool, &a).await, (None, vec![]));
    assert!(rows_of_record(&f.pool, &a).await.is_empty());
    // (b) `quote` names X, the payload quotes Y.
    let mut b = f.new_form_record(ALICE, "3kte9b222222", "b", None, Some(&record_embed(&embed_type, &y)), &aud).await;
    b["quote"] = json!({"uri": x, "cid": "bafyquoted"});
    let b = f.put_post(ALICE, "3kte9b222222", b).await;
    assert_eq!(indexed(&f.pool, &b).await.0.as_deref(), Some(x.as_str()));
    // (c) An old-form record quoting X in the clear, with `quote` naming Y.
    let mut c = f.old_form_record(ALICE, "3kte9c222222", "c", None, Some(&record_embed(&embed_type, &x)), &aud).await;
    c["quote"] = json!({"uri": y, "cid": "bafyquoted"});
    let c = f.put_post(ALICE, "3kte9c222222", c).await;
    assert_eq!(indexed(&f.pool, &c).await.0.as_deref(), Some(y.as_str()));
    // (d) `quote` a string, `mentionDids` an object: dropped and counted,
    // no fallback to the clear embed and facets, the record stored.
    let dropped = |field: &str| {
        crate::test_support::series(&handle.render(), crate::private_tier::ingest::METRIC_FIELD_DROPPED, &[&format!("field=\"{field}\"")]).unwrap_or(0.0)
    };
    let before = (dropped("quote"), dropped("mentionDids"));
    let mut d = f.old_form_record(ALICE, "3kte9d222222", "d", Some(&facets), Some(&record_embed(&embed_type, &x)), &aud).await;
    d["quote"] = json!(x);
    d["mentionDids"] = json!({"did": CAROL});
    let d = f.put_post(ALICE, "3kte9d222222", d).await;
    assert_eq!(indexed(&f.pool, &d).await, (None, vec![]));
    assert!(rows_of_record(&f.pool, &d).await.is_empty());
    assert_eq!((dropped("quote"), dropped("mentionDids")), (before.0 + 1.0, before.1 + 1.0));
    let source = |field: &str, source: &str| {
        crate::test_support::series(&handle.render(), crate::private_tier::ingest::METRIC_INDEX_SOURCE, &[&format!("field=\"{field}\""), &format!("source=\"{source}\"")])
            .unwrap_or(0.0)
    };
    assert!(source("quote", "dedicated") >= 3.0 && source("mentions", "dedicated") >= 1.0, "{}", handle.render());

    f.ready_all().await;
    let base = serve(f.state_floored(Some(Duration::from_secs(1)), Duration::ZERO)).await;
    // Since private-encode E2, (b) and (c) over-claim (their `quote` names
    // a post the content does not quote): a miss for Bob, served to the
    // author. (a) and (d) under-claim: served.
    let (_, views) = posts_of(&base, BOB, &[&a, &b, &c, &d]).await;
    assert_eq!(views.len(), 2, "{views:?}");
    assert_eq!(views[0]["embed"]["record"]["uri"], x.as_str());
    assert_eq!(views[0]["record"]["facets"], facets);
    assert_eq!(views[1]["record"]["facets"], facets, "the clear fields still display");
    let (_, views) = posts_of(&base, ALICE, &[&b, &c]).await;
    assert_eq!(views.len(), 2, "{views:?}");
    assert_eq!(views[0]["embed"]["record"]["uri"], y.as_str(), "the view follows the payload");
    assert_eq!(views[1]["embed"]["record"]["uri"], x.as_str(), "and an old form's clear embed");
    // Neither over-claim is listed in the quotes of the post it names.
    let quotes = |uri: String| {
        let base = base.clone();
        async move {
            let (_, page, _) = json_of(&base, GET_QUOTES, &format!("uri={uri}"), BOB).await;
            page["posts"].as_array().expect("posts").iter().map(|p| p["uri"].as_str().expect("uri").to_string()).collect::<Vec<_>>()
        }
    };
    assert!(quotes(x.clone()).await.is_empty());
    assert!(quotes(y.clone()).await.is_empty());
    db.drop_db().await;
}

/// T-E10: indexing does not depend on decoding. A new-form record whose
/// `encodedContent` is not laquna at all is stored with its `quote` and
/// `mentionDids` indexed and its notifications written; its reads miss.
#[tokio::test]
async fn te10_indexing_needs_no_decode() {
    let Some(db) = fresh_db("te10_no_decode").await else {
        return;
    };
    let f = fixture(&db.pool, "te10-no-decode").await;
    let (aud, quoted) = quoting_setup(&f).await;
    let mut record = f
        .new_form_record(ALICE, "3kgarbage222", "g", Some(&mentions("#mention", &[CAROL])), Some(&record_embed("app.bsky.embed.record", &quoted)), &aud)
        .await;
    record["encodedContent"] = json!("00112233445566778899aabbccddeeff");
    let post = f.put_post(ALICE, "3kgarbage222", record).await;
    assert_eq!(indexed(&f.pool, &post).await, (Some(quoted.clone()), vec![CAROL.to_string()]));
    assert_eq!(
        rows_of_record(&f.pool, &post).await,
        [(DAVE.to_string(), "quote".to_string()), (CAROL.to_string(), "mention".to_string())]
    );
    f.ready_all().await;
    let base = serve(f.state_floored(Some(Duration::from_secs(1)), Duration::ZERO)).await;
    let (_, views) = posts_of(&base, BOB, &[&post]).await;
    assert!(views.is_empty(), "{views:?}");
    db.drop_db().await;
}
