//! Search and trend inputs extracted from a post record (design-v3 §3.1,
//! §9.0 "Normalization"). The ingest writer stores them, the post-migration
//! task fills them for old rows, and the search parser normalizes its
//! parameters with the same functions, so the two always agree.
//!
//! - **tags:** facet `#tag` features ∪ the record's `tags`: lowercased, NFC,
//!   without `#`, deduplicated and sorted.
//! - **links:** facet links and external embeds, `http` and `https` only.
//!   - `link_urls`: the URL with scheme and host lowercased and the fragment
//!     removed;
//!   - `link_domains`: the host (lowercased, IDNA, no port, no leading
//!     `www.`) and each of its parent domains with at least two labels.
//! - **lang_prefixes:** each `langs` value lowercased, plus its primary
//!   subtag.
//!
//! Every array is stored as `{}`, never NULL, once a row is written.

use serde_json::Value;
use unicode_normalization::UnicodeNormalization;
use url::{Host, Url};

/// What the writer stores beside a post's typed fields.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PostExtract {
    pub tags: Vec<String>,
    pub link_domains: Vec<String>,
    pub link_urls: Vec<String>,
    pub lang_prefixes: Vec<String>,
    /// `$type` of the top-level embed.
    pub embed_type: Option<String>,
}

fn sorted(mut v: Vec<String>) -> Vec<String> {
    v.sort();
    v.dedup();
    v
}

/// One tag as stored and as searched: NFC, lowercased, without `#`. `None`
/// when nothing is left.
#[must_use]
pub fn normalize_tag(raw: &str) -> Option<String> {
    let tag: String = raw.trim().trim_start_matches('#').nfc().collect::<String>().to_lowercase();
    (!tag.is_empty()).then_some(tag)
}

/// One URL as stored and as searched: `http` or `https`, scheme and host
/// lowercased, fragment removed. `None` for anything else.
#[must_use]
pub fn normalize_url(raw: &str) -> Option<String> {
    let mut url = Url::parse(raw.trim()).ok()?;
    if !matches!(url.scheme(), "http" | "https") || url.host().is_none() {
        return None;
    }
    url.set_fragment(None);
    Some(url.to_string())
}

/// One host as stored and as searched: lowercased, IDNA, no port, no leading
/// `www.`. Accepts a bare host or a URL.
#[must_use]
pub fn normalize_domain(raw: &str) -> Option<String> {
    let raw = raw.trim();
    let url = if raw.contains("://") { Url::parse(raw) } else { Url::parse(&format!("https://{raw}")) }.ok()?;
    let host = url.host_str()?;
    Some(host.strip_prefix("www.").unwrap_or(host).to_string())
}

/// A link's host and each of its parent domains with at least two labels.
fn domains_of(url: &Url) -> Vec<String> {
    let Some(host) = url.host_str() else {
        return Vec::new();
    };
    let host = host.strip_prefix("www.").unwrap_or(host);
    if !matches!(url.host(), Some(Host::Domain(_))) {
        return vec![host.to_string()];
    }
    let labels: Vec<&str> = host.split('.').collect();
    (0..labels.len().saturating_sub(1).max(1)).map(|i| labels[i..].join(".")).collect()
}

/// Every link the record carries: facet `#link` features, and the external
/// embed (alone or as the media of `recordWithMedia`).
fn raw_links(record: &Value) -> Vec<&str> {
    let mut out: Vec<&str> = Vec::new();
    for facet in record.get("facets").and_then(Value::as_array).into_iter().flatten() {
        for feature in facet.get("features").and_then(Value::as_array).into_iter().flatten() {
            if feature.get("$type").and_then(Value::as_str) == Some("app.bsky.richtext.facet#link")
                && let Some(uri) = feature.get("uri").and_then(Value::as_str)
            {
                out.push(uri);
            }
        }
    }
    let embed = record.get("embed");
    let external = match embed.and_then(|e| e.get("$type")).and_then(Value::as_str) {
        Some("app.bsky.embed.external") => embed.and_then(|e| e.get("external")),
        Some("app.bsky.embed.recordWithMedia") => embed
            .and_then(|e| e.get("media"))
            .filter(|m| m.get("$type").and_then(Value::as_str) == Some("app.bsky.embed.external"))
            .and_then(|m| m.get("external")),
        _ => None,
    };
    if let Some(uri) = external.and_then(|x| x.get("uri")).and_then(Value::as_str) {
        out.push(uri);
    }
    out
}

fn tags_of(record: &Value) -> Vec<String> {
    let mut out: Vec<String> = Vec::new();
    for facet in record.get("facets").and_then(Value::as_array).into_iter().flatten() {
        for feature in facet.get("features").and_then(Value::as_array).into_iter().flatten() {
            if feature.get("$type").and_then(Value::as_str) == Some("app.bsky.richtext.facet#tag") {
                out.extend(feature.get("tag").and_then(Value::as_str).and_then(normalize_tag));
            }
        }
    }
    for tag in record.get("tags").and_then(Value::as_array).into_iter().flatten() {
        out.extend(tag.as_str().and_then(normalize_tag));
    }
    sorted(out)
}

/// Each language lowercased, plus its primary subtag: `en-US` gives `en-us`
/// and `en`.
#[must_use]
pub fn lang_prefixes(langs: &[String]) -> Vec<String> {
    let mut out: Vec<String> = Vec::new();
    for lang in langs {
        let lang = lang.trim().to_lowercase();
        if lang.is_empty() {
            continue;
        }
        if let Some((primary, _)) = lang.split_once('-')
            && !primary.is_empty()
        {
            out.push(primary.to_string());
        }
        out.push(lang);
    }
    sorted(out)
}

impl PostExtract {
    /// Extract from an `app.bsky.feed.post` record (either JSON encoding of
    /// links is the same here: these fields are plain strings).
    #[must_use]
    pub fn of(record: &Value) -> Self {
        let mut link_domains = Vec::new();
        let mut link_urls = Vec::new();
        for raw in raw_links(record) {
            let Some(normalized) = normalize_url(raw) else {
                continue;
            };
            if let Ok(url) = Url::parse(&normalized) {
                link_domains.extend(domains_of(&url));
            }
            link_urls.push(normalized);
        }
        let langs: Vec<String> = record
            .get("langs")
            .and_then(Value::as_array)
            .map(|a| a.iter().filter_map(Value::as_str).map(str::to_string).collect())
            .unwrap_or_default();
        Self {
            tags: tags_of(record),
            link_domains: sorted(link_domains),
            link_urls: sorted(link_urls),
            lang_prefixes: lang_prefixes(&langs),
            embed_type: record
                .get("embed")
                .and_then(|e| e.get("$type"))
                .and_then(Value::as_str)
                .map(str::to_string),
        }
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn tags_are_nfc_lowercase_sorted_and_distinct() {
        let record = json!({
            "tags": ["Rust", "#Cats", "", "caf\u{0065}\u{0301}"],
            "facets": [{"features": [
                {"$type": "app.bsky.richtext.facet#tag", "tag": "rust"},
                {"$type": "app.bsky.richtext.facet#tag", "tag": "Zebra"},
                {"$type": "app.bsky.richtext.facet#mention", "did": "did:plc:x"},
            ]}],
        });
        // The decomposed "café" is stored composed.
        assert_eq!(PostExtract::of(&record).tags, ["caf\u{00e9}", "cats", "rust", "zebra"]);
        assert_eq!(normalize_tag("#"), None);
        assert_eq!(normalize_tag(" #ÅNGSTRÖM "), Some("ångström".to_string()));
    }

    #[test]
    fn links_keep_http_only_and_list_parent_domains() {
        let record = json!({
            "facets": [{"features": [
                {"$type": "app.bsky.richtext.facet#link", "uri": "HTTPS://News.Example.CO.UK:443/Path?q=1#frag"},
                {"$type": "app.bsky.richtext.facet#link", "uri": "at://did:plc:x/app.bsky.feed.post/1"},
                {"$type": "app.bsky.richtext.facet#link", "uri": "mailto:a@example.com"},
                {"$type": "app.bsky.richtext.facet#link", "uri": "http://www.example.org"},
            ]}],
            "embed": {"$type": "app.bsky.embed.recordWithMedia",
                      "media": {"$type": "app.bsky.embed.external", "external": {"uri": "https://192.0.2.7:8080/x"}}},
        });
        let x = PostExtract::of(&record);
        assert_eq!(x.link_urls, ["http://www.example.org/", "https://192.0.2.7:8080/x", "https://news.example.co.uk/Path?q=1"]);
        // The host and each parent with two or more labels; an IP address
        // has no parents; `www.` is dropped.
        assert_eq!(x.link_domains, ["192.0.2.7", "co.uk", "example.co.uk", "example.org", "news.example.co.uk"]);
        assert_eq!(x.embed_type.as_deref(), Some("app.bsky.embed.recordWithMedia"));

        let external = json!({"embed": {"$type": "app.bsky.embed.external", "external": {"uri": "https://a.example/"}}});
        assert_eq!(PostExtract::of(&external).link_domains, ["a.example"]);

        assert_eq!(normalize_domain("WWW.Example.com"), Some("example.com".to_string()));
        assert_eq!(normalize_domain("https://b\u{00fc}cher.example/x"), Some("xn--bcher-kva.example".to_string()));
        assert_eq!(normalize_url("ftp://example.com/"), None);
        assert_eq!(normalize_url("not a url"), None);
    }

    #[test]
    fn language_prefixes_and_empty_arrays() {
        assert_eq!(lang_prefixes(&["en-US".to_string(), "PT".to_string(), "en".to_string()]), ["en", "en-us", "pt"]);
        // A post with none of these still stores empty arrays, not NULLs.
        let plain = PostExtract::of(&json!({"text": "hi"}));
        assert_eq!(plain, PostExtract::default());
    }
}
