//! Private posts whose `encodedContent` carries facets and embed (#504,
//! private-embeds design): the old form held to a golden (T-E2).

use super::*;

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
