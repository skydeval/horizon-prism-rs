//! A private post's media kinds (private-media-filters design §2, §3): the
//! clear `mediaKinds` field the PDS writes, the bits `private_posts.media_kinds`
//! holds, and the classifier of a clear or read embed. `media_kinds` is a
//! structural fact, like `quote_uri`: non-content queries may read it.

use serde_json::Value;

/// The clear field on `postPrivate` that names the post's media kinds: an
/// array of [`KIND_IMAGE`] and/or [`KIND_VIDEO`], omitted when none.
pub(crate) const MEDIA_KINDS_FIELD: &str = "mediaKinds";
/// The `mediaKinds` value for images or a gallery.
pub(crate) const KIND_IMAGE: &str = "image";
/// The `mediaKinds` value for a video.
pub(crate) const KIND_VIDEO: &str = "video";

/// `media_kinds` bit: images or a gallery, also as a record-with-media's media.
pub(crate) const IMAGE: u8 = 1;
/// `media_kinds` bit: a video, also as a record-with-media's media.
pub(crate) const VIDEO: u8 = 2;

/// The media bits of an embed in either `$type` vocabulary, by the public
/// classifier (`hydration::feed::media_type_of_embed`) on the normalized
/// embed: images or a gallery is [`IMAGE`], a video [`VIDEO`], anything
/// else (external cards, plain quotes, a quoted post's own media) `0`.
/// Migration 0035's `private_embed_media_kinds` is the same test in SQL.
#[must_use]
pub(crate) fn kinds_of_embed(embed: &Value) -> u8 {
    let normalized = crate::hydration::embeds::normalize_embed(embed);
    match crate::hydration::feed::media_type_of_embed(&normalized) {
        Some("images" | "gallery") => IMAGE,
        Some("video") => VIDEO,
        _ => 0,
    }
}

/// The bits of a `mediaKinds` array: each entry exactly [`KIND_IMAGE`] or
/// [`KIND_VIDEO`] sets its bit; other strings, non-strings and duplicates
/// are ignored, and order is not checked (§3.3).
#[must_use]
pub(crate) fn kinds_of_field(kinds: &[Value]) -> u8 {
    kinds.iter().filter_map(Value::as_str).fold(0, |bits, kind| match kind {
        KIND_IMAGE => bits | IMAGE,
        KIND_VIDEO => bits | VIDEO,
        _ => bits,
    })
}

/// Every embed kind in both `$type` vocabularies, with its bits: the
/// fixtures T-M6 parses and T-M7 holds the SQL function to.
#[cfg(test)]
pub(super) fn embed_fixtures() -> Vec<(&'static str, Value, u8)> {
    use serde_json::json;
    const K: &str = "tools.kryphocron.feed.postPublic";
    let blob = |cid: &str| json!({"$type": "blob", "ref": {"$link": cid}, "mimeType": "image/jpeg", "size": 1});
    let quoted = json!({"uri": "at://did:plc:bbbbbbbbbbbbbbbbbbbbbbbb/tools.kryphocron.feed.postPrivate/3kq", "cid": "c"});
    let images = json!({"$type": "app.bsky.embed.images", "images": [{"image": blob("i1"), "alt": ""}]});
    let k_images = json!({"$type": format!("{K}#embedImage"), "images": [{"image": blob("i1"), "alt": ""}]});
    let gallery = json!({"$type": "app.bsky.embed.gallery", "items": [{"$type": "app.bsky.embed.gallery#image", "image": blob("g1")}]});
    let k_gallery = json!({"$type": format!("{K}#embedGallery"), "items": [{"$type": format!("{K}#galleryImage"), "image": blob("g1")}]});
    let video = json!({"$type": "app.bsky.embed.video", "video": blob("v1")});
    let k_video = json!({"$type": format!("{K}#embedVideo"), "video": blob("v1")});
    let external = json!({"$type": "app.bsky.embed.external", "external": {"uri": "https://x", "title": "t", "description": "d", "thumb": blob("t1")}});
    let k_external = json!({"$type": format!("{K}#embedExternal"), "external": {"uri": "https://x.gif", "title": "", "description": "", "thumb": blob("t1")}});
    let rwm = |t: &str, media: &Value| json!({"$type": t, "record": {"record": quoted}, "media": media});
    let k_rwm_type = format!("{K}#embedRecordWithMedia");
    let (s_rwm, k_rwm) = ("app.bsky.embed.recordWithMedia", k_rwm_type.as_str());
    vec![
        ("images", images.clone(), IMAGE),
        ("kryphocron images", k_images.clone(), IMAGE),
        ("gallery", gallery.clone(), IMAGE),
        ("kryphocron gallery", k_gallery.clone(), IMAGE),
        ("video", video.clone(), VIDEO),
        ("kryphocron video", k_video.clone(), VIDEO),
        ("external with thumb", external.clone(), 0),
        ("kryphocron external (a GIF)", k_external.clone(), 0),
        ("record", json!({"$type": "app.bsky.embed.record", "record": quoted}), 0),
        ("kryphocron record", json!({"$type": format!("{K}#embedRecord"), "record": quoted}), 0),
        ("record with images", rwm(s_rwm, &images), IMAGE),
        ("record with gallery", rwm(s_rwm, &gallery), IMAGE),
        ("record with video", rwm(s_rwm, &video), VIDEO),
        ("record with external", rwm(s_rwm, &external), 0),
        ("record with kryphocron video", rwm(s_rwm, &k_video), VIDEO),
        ("kryphocron record with images", rwm(k_rwm, &k_images), IMAGE),
        ("kryphocron record with gallery", rwm(k_rwm, &k_gallery), IMAGE),
        ("kryphocron record with video", rwm(k_rwm, &k_video), VIDEO),
        ("kryphocron record with stock images", rwm(k_rwm, &images), IMAGE),
        ("kryphocron record with external", rwm(k_rwm, &k_external), 0),
        ("record with no media", json!({"$type": s_rwm, "record": {"record": quoted}}), 0),
        ("record with a nested record with images", rwm(s_rwm, &rwm(s_rwm, &images)), 0),
        ("an unknown type", json!({"$type": "app.example.embed.audio", "audio": blob("a1")}), 0),
        ("no type", json!({"images": [{"image": blob("i1")}]}), 0),
        ("a non-string type", json!({"$type": 5}), 0),
    ]
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    /// §3.1: the classifier over every fixture, and §3.3's reading of the
    /// field.
    #[test]
    fn kinds_of_embed_and_of_the_field() {
        for (case, embed, bits) in embed_fixtures() {
            assert_eq!(kinds_of_embed(&embed), bits, "{case}");
        }
        for (field, bits) in [
            (json!(["image"]), IMAGE),
            (json!(["video"]), VIDEO),
            (json!(["image", "video"]), IMAGE | VIDEO),
            (json!(["video", "image"]), IMAGE | VIDEO),
            (json!(["image", "image"]), IMAGE),
            (json!(["audio"]), 0),
            (json!([1, "image"]), IMAGE),
            (json!(["Image", "VIDEO"]), 0),
            (json!([]), 0),
        ] {
            assert_eq!(kinds_of_field(field.as_array().expect("array")), bits, "{field}");
        }
    }
}
