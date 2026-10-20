//! Media views of post embeds (design-v3 §8.4; private-posting design §4.2
//! and Addendum A), shared by public post views and the private tier's.
//!
//! - [`normalize_embed`] rewrites the kryphocron `$type` forms a private
//!   post may carry (`tools.kryphocron.feed.postPublic#embed*`) to the stock
//!   `app.bsky.embed.*` ones, before any view is built.
//! - [`media_view`] builds `images#view`, `external#view`, `video#view` or
//!   `gallery#view` from a stock embed and the author's PDS endpoint, for a
//!   public post; [`private_media_view`] the same for a private post, whose
//!   video always plays from the PDS (private-video-pds design). Neither
//!   makes a request: every URL is computed.

use serde_json::{Value, json};

use super::blob_url;
use crate::config::{VideoConfig, VideoPlayback};
use crate::firehose::normalize::blob_cid;

pub const EMBED_IMAGES: &str = "app.bsky.embed.images";
pub const EMBED_EXTERNAL: &str = "app.bsky.embed.external";
pub const EMBED_VIDEO: &str = "app.bsky.embed.video";
pub const EMBED_GALLERY: &str = "app.bsky.embed.gallery";
pub const EMBED_RECORD: &str = "app.bsky.embed.record";
pub const EMBED_RECORD_WITH_MEDIA: &str = "app.bsky.embed.recordWithMedia";
/// The one gallery item type [`media_view`] reads.
pub const GALLERY_IMAGE: &str = "app.bsky.embed.gallery#image";
/// kryphocron-lexicons 0.3.1's gallery item.
const KRYPHOCRON_GALLERY_IMAGE: &str = "tools.kryphocron.feed.postPublic#galleryImage";

/// The stock embed type a stored `$type` is treated as (§4.2's table), or
/// `None` for a type no view is built for.
fn stock_type(stored: &str) -> Option<&'static str> {
    Some(match stored {
        EMBED_IMAGES | "tools.kryphocron.feed.postPublic#embedImage" => EMBED_IMAGES,
        EMBED_EXTERNAL | "tools.kryphocron.feed.postPublic#embedExternal" => EMBED_EXTERNAL,
        EMBED_VIDEO | "tools.kryphocron.feed.postPublic#embedVideo" => EMBED_VIDEO,
        EMBED_GALLERY | "tools.kryphocron.feed.postPublic#embedGallery" => EMBED_GALLERY,
        EMBED_RECORD | "tools.kryphocron.feed.postPublic#embedRecord" => EMBED_RECORD,
        EMBED_RECORD_WITH_MEDIA | "tools.kryphocron.feed.postPublic#embedRecordWithMedia" => EMBED_RECORD_WITH_MEDIA,
        _ => return None,
    })
}

/// `embed` with both `$type` forms read as the stock one: the top level,
/// `recordWithMedia`'s `media`, and gallery items (`#galleryImage` becomes
/// `app.bsky.embed.gallery#image`). Anything else is left as stored.
#[must_use]
pub fn normalize_embed(embed: &Value) -> Value {
    let mut out = embed.clone();
    normalize_in_place(&mut out);
    out
}

fn normalize_in_place(embed: &mut Value) {
    let Some(stock) = embed.get("$type").and_then(Value::as_str).and_then(stock_type) else {
        return;
    };
    embed["$type"] = json!(stock);
    match stock {
        EMBED_GALLERY => {
            for item in embed.get_mut("items").and_then(Value::as_array_mut).into_iter().flatten() {
                if item.get("$type").and_then(Value::as_str) == Some(KRYPHOCRON_GALLERY_IMAGE) {
                    item["$type"] = json!(GALLERY_IMAGE);
                }
            }
        }
        EMBED_RECORD_WITH_MEDIA => {
            if let Some(media) = embed.get_mut("media") {
                normalize_in_place(media);
            }
        }
        _ => {}
    }
}

/// The URI a normalized embed quotes: `record.uri` of a `record` embed,
/// `record.record.uri` of a `recordWithMedia` one.
#[must_use]
pub fn quoted_uri(embed: &Value) -> Option<&str> {
    let strong_ref = match embed.get("$type").and_then(Value::as_str)? {
        EMBED_RECORD => embed.get("record")?,
        EMBED_RECORD_WITH_MEDIA => embed.get("record")?.get("record")?,
        _ => return None,
    };
    strong_ref.get("uri")?.as_str()
}

/// A DID as one path segment of a CDN URL.
fn path_segment(did: &str) -> String {
    url::form_urlencoded::byte_serialize(did.as_bytes()).collect()
}

/// Where a video view's `playlist` points.
#[derive(Clone, Copy)]
enum VideoSource<'a> {
    /// HLS and a thumbnail under this CDN base.
    Cdn(&'a str),
    /// The MP4 blob on the author's PDS, with no thumbnail.
    PdsBlob,
}

/// The media view of a public post's stock embed by `did`, whose PDS is
/// `pds`. Images and thumbs are blobs on the author's PDS. A video is played
/// per `video` (Addendum A): HLS and a thumbnail on the CDN, or the MP4 blob.
#[must_use]
pub fn media_view(did: &str, pds: Option<&str>, embed: &Value, video: &VideoConfig) -> Option<Value> {
    let source = match video.playback {
        VideoPlayback::BskyCdn => VideoSource::Cdn(&video.cdn_url),
        VideoPlayback::PdsBlob => VideoSource::PdsBlob,
    };
    build_media_view(did, pds, embed, source)
}

/// The media view of a private post's stock embed: as [`media_view`], but a
/// video always plays from the MP4 blob on the author's PDS, with no
/// thumbnail, whatever `[video] playback` says (private-video-pds design).
/// With no PDS endpoint a video has no view; it never falls back to a CDN.
#[must_use]
pub fn private_media_view(did: &str, pds: Option<&str>, embed: &Value) -> Option<Value> {
    build_media_view(did, pds, embed, VideoSource::PdsBlob)
}

fn build_media_view(did: &str, pds: Option<&str>, embed: &Value, video: VideoSource<'_>) -> Option<Value> {
    match embed.get("$type").and_then(Value::as_str)? {
        EMBED_IMAGES => {
            let images: Vec<Value> = embed
                .get("images")?
                .as_array()?
                .iter()
                .filter_map(|img| {
                    let cid = img.get("image").and_then(blob_cid)?;
                    let mut i = json!({
                        "thumb": blob_url(pds, did, &cid)?,
                        "fullsize": blob_url(pds, did, &cid)?,
                        "alt": img.get("alt").and_then(Value::as_str).unwrap_or_default(),
                    });
                    if let Some(ar) = img.get("aspectRatio") {
                        i["aspectRatio"] = ar.clone();
                    }
                    Some(i)
                })
                .collect();
            Some(json!({"$type": "app.bsky.embed.images#view", "images": images}))
        }
        EMBED_EXTERNAL => {
            let ext = embed.get("external")?;
            let mut e = json!({
                "uri": ext.get("uri").and_then(Value::as_str).unwrap_or_default(),
                "title": ext.get("title").and_then(Value::as_str).unwrap_or_default(),
                "description": ext.get("description").and_then(Value::as_str).unwrap_or_default(),
            });
            if let Some(url) = ext.get("thumb").and_then(blob_cid).and_then(|c| blob_url(pds, did, &c)) {
                e["thumb"] = json!(url);
            }
            Some(json!({"$type": "app.bsky.embed.external#view", "external": e}))
        }
        EMBED_VIDEO => {
            let cid = embed.get("video").and_then(blob_cid)?;
            let mut v = json!({"$type": "app.bsky.embed.video#view", "cid": cid});
            match video {
                VideoSource::Cdn(cdn_url) => {
                    let base = format!("{}/watch/{}/{cid}", cdn_url.trim_end_matches('/'), path_segment(did));
                    v["playlist"] = json!(format!("{base}/playlist.m3u8"));
                    v["thumbnail"] = json!(format!("{base}/thumbnail.jpg"));
                }
                VideoSource::PdsBlob => v["playlist"] = json!(blob_url(pds, did, &cid)?),
            }
            if let Some(alt) = embed.get("alt") {
                v["alt"] = alt.clone();
            }
            if let Some(ar) = embed.get("aspectRatio") {
                v["aspectRatio"] = ar.clone();
            }
            Some(v)
        }
        // design-v3 §8.4. `items` is a union in the lexicon, so every view
        // item carries its `$type` (images#view items do not). Only `#image`
        // record items are read, and an item missing its required `alt` or
        // `aspectRatio` is dropped, not invented.
        EMBED_GALLERY => {
            let items: Vec<Value> = embed
                .get("items")?
                .as_array()?
                .iter()
                .filter(|item| item.get("$type").and_then(Value::as_str) == Some(GALLERY_IMAGE))
                .filter_map(|item| {
                    let cid = item.get("image").and_then(blob_cid)?;
                    Some(json!({
                        "$type": "app.bsky.embed.gallery#viewImage",
                        "thumbnail": blob_url(pds, did, &cid)?,
                        "fullsize": blob_url(pds, did, &cid)?,
                        "alt": item.get("alt").and_then(Value::as_str)?,
                        "aspectRatio": item.get("aspectRatio").filter(|ar| ar.is_object())?.clone(),
                    }))
                })
                .collect();
            Some(json!({"$type": "app.bsky.embed.gallery#view", "items": items}))
        }
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const DID: &str = "did:plc:aaaaaaaaaaaaaaaaaaaaaaaa";
    const PDS: &str = "https://pds.example.com";

    fn blob(cid: &str) -> Value {
        json!({"$type": "blob", "ref": {"$link": cid}, "mimeType": "image/jpeg", "size": 1})
    }

    #[test]
    fn kryphocron_forms_normalize_to_stock() {
        let k = "tools.kryphocron.feed.postPublic";
        for (stored, stock) in [
            (format!("{k}#embedImage"), EMBED_IMAGES),
            (format!("{k}#embedExternal"), EMBED_EXTERNAL),
            (format!("{k}#embedVideo"), EMBED_VIDEO),
            (format!("{k}#embedGallery"), EMBED_GALLERY),
            (format!("{k}#embedRecord"), EMBED_RECORD),
            (format!("{k}#embedRecordWithMedia"), EMBED_RECORD_WITH_MEDIA),
        ] {
            assert_eq!(normalize_embed(&json!({"$type": stored}))["$type"], stock);
            assert_eq!(normalize_embed(&json!({"$type": stock}))["$type"], stock);
        }
        let gallery = normalize_embed(&json!({"$type": format!("{k}#embedGallery"), "items": [
            {"$type": format!("{k}#galleryImage"), "image": blob("bafyg"), "alt": "", "aspectRatio": {"width": 1, "height": 1}},
            {"$type": "something.else", "image": blob("bafyh")},
        ]}));
        assert_eq!(gallery["items"][0]["$type"], GALLERY_IMAGE);
        assert_eq!(gallery["items"][1]["$type"], "something.else");
        let rwm = normalize_embed(&json!({"$type": format!("{k}#embedRecordWithMedia"),
            "record": {"record": {"uri": "at://x/y/z", "cid": "c"}},
            "media": {"$type": format!("{k}#embedImage"), "images": []}}));
        assert_eq!(rwm["media"]["$type"], EMBED_IMAGES);
        assert_eq!(quoted_uri(&rwm), Some("at://x/y/z"));
        assert_eq!(normalize_embed(&json!({"$type": "x.y#z", "a": 1})), json!({"$type": "x.y#z", "a": 1}));
    }

    /// Addendum A: a video under each playback setting.
    #[test]
    fn video_views_follow_the_playback_setting() {
        let embed = json!({"$type": EMBED_VIDEO, "video": blob("bafyvideo"), "alt": "a clip", "aspectRatio": {"width": 16, "height": 9}});
        let cdn = media_view(DID, Some(PDS), &embed, &VideoConfig::default()).expect("cdn view");
        let base = "https://video.bsky.app/watch/did%3Aplc%3Aaaaaaaaaaaaaaaaaaaaaaaaa/bafyvideo";
        assert_eq!(cdn["playlist"], format!("{base}/playlist.m3u8"));
        assert_eq!(cdn["thumbnail"], format!("{base}/thumbnail.jpg"));
        assert_eq!((cdn["alt"].as_str(), cdn["cid"].as_str()), (Some("a clip"), Some("bafyvideo")));
        // No PDS endpoint is needed for the CDN.
        assert!(media_view(DID, None, &embed, &VideoConfig::default()).is_some());
        let blob_cfg = VideoConfig {
            playback: VideoPlayback::PdsBlob,
            ..VideoConfig::default()
        };
        let pds = media_view(DID, Some(PDS), &embed, &blob_cfg).expect("blob view");
        assert_eq!(
            pds["playlist"],
            format!("{PDS}/xrpc/com.atproto.sync.getBlob?did={DID}&cid=bafyvideo")
        );
        assert!(pds.get("thumbnail").is_none());
        let other = VideoConfig {
            cdn_url: "https://cdn.example.org/".into(),
            ..VideoConfig::default()
        };
        let v = media_view(DID, Some(PDS), &embed, &other).expect("view");
        assert!(v["playlist"].as_str().expect("playlist").starts_with("https://cdn.example.org/watch/"));
    }

    /// T-PV0 (private-video-pds design): a private post's video is the MP4
    /// blob on the PDS, with no thumbnail and no CDN fallback; its other
    /// media are the public views.
    #[test]
    fn private_video_views_play_from_the_pds() {
        let embed = json!({"$type": EMBED_VIDEO, "video": blob("bafyvideo"), "alt": "a clip", "aspectRatio": {"width": 16, "height": 9}});
        let v = private_media_view(DID, Some(PDS), &embed).expect("view");
        assert_eq!(
            v,
            json!({
                "$type": "app.bsky.embed.video#view",
                "cid": "bafyvideo",
                "playlist": format!("{PDS}/xrpc/com.atproto.sync.getBlob?did={DID}&cid=bafyvideo"),
                "alt": "a clip",
                "aspectRatio": {"width": 16, "height": 9},
            })
        );
        // No PDS endpoint: no view, never a CDN URL.
        assert!(private_media_view(DID, None, &embed).is_none());
        let others = [
            json!({"$type": EMBED_IMAGES, "images": [{"image": blob("bafyimg"), "alt": "i"}]}),
            json!({"$type": EMBED_EXTERNAL, "external": {"uri": "https://e.example", "title": "t", "description": "d", "thumb": blob("bafyt")}}),
            json!({"$type": EMBED_GALLERY, "items": [{"$type": GALLERY_IMAGE, "image": blob("bafyg"), "alt": "g", "aspectRatio": {"width": 1, "height": 1}}]}),
        ];
        for e in &others {
            let public = media_view(DID, Some(PDS), e, &VideoConfig::default());
            assert!(public.is_some(), "{e}");
            assert_eq!(private_media_view(DID, Some(PDS), e), public, "{e}");
        }
    }
}
