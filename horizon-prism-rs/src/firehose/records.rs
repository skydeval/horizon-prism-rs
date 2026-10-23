//! Typed views of the ecosystem records v1 indexes, parsed from either JSON
//! encoding (§5.8), plus the admission subjects of every indexed kind (§9.3).
//!
//! A record that cannot be parsed is a deterministic failure: it is
//! dead-lettered from the firehose and skipped by backfill.

use chrono::{DateTime, TimeDelta, Utc};
use serde_json::Value;

use super::classifier::KryphocronKind;
use super::extract::PostExtract;
use super::normalize::{blob_cid, cid_link};
use crate::db::locks::uri_author;
use crate::hydration::feed::media_type;

/// Future-dated records are clamped to this far past indexing (§5.12).
pub const MAX_FUTURE_SKEW: TimeDelta = TimeDelta::minutes(5);

/// A deterministic record problem.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("invalid record: {0}")]
pub struct RecordError(pub String);

fn err(msg: impl Into<String>) -> RecordError {
    RecordError(msg.into())
}

/// A strong reference `{uri, cid}`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StrongRef {
    pub uri: String,
    pub cid: Option<String>,
}

fn strong_ref(v: Option<&Value>, field: &str) -> Result<StrongRef, RecordError> {
    let v = v.ok_or_else(|| err(format!("missing {field}")))?;
    let uri = v
        .get("uri")
        .and_then(Value::as_str)
        .filter(|u| uri_author(u).is_some())
        .ok_or_else(|| err(format!("{field}.uri is not an at-uri")))?;
    Ok(StrongRef {
        uri: uri.to_string(),
        cid: v.get("cid").and_then(cid_link),
    })
}

/// `createdAt`, clamped to `now + 5 min`; `now` when absent or unparseable.
#[must_use]
pub fn created_at(record: &Value, now: DateTime<Utc>) -> DateTime<Utc> {
    let parsed = record
        .get("createdAt")
        .and_then(Value::as_str)
        .and_then(|s| DateTime::parse_from_rfc3339(s).ok())
        .map(|d| d.with_timezone(&Utc));
    match parsed {
        Some(t) => t.min(now + MAX_FUTURE_SKEW),
        None => now,
    }
}

/// `app.bsky.feed.post`.
#[derive(Debug, Clone, PartialEq)]
pub struct PostRecord {
    pub text: String,
    pub langs: Option<Vec<String>>,
    pub reply_root: Option<StrongRef>,
    pub reply_parent: Option<StrongRef>,
    pub quote_uri: Option<String>,
    pub embed: Option<Value>,
    pub facets: Option<Value>,
    pub mention_dids: Vec<String>,
    pub self_labels: Vec<String>,
    /// Search and trend inputs (design-v3 §3.1).
    pub extract: PostExtract,
    /// `images`, `video` or `gallery` (§8.4).
    pub media_type: Option<&'static str>,
    pub created_at: DateTime<Utc>,
}

impl PostRecord {
    /// Parse a post.
    ///
    /// # Errors
    ///
    /// A reply without valid root and parent refs.
    pub fn parse(record: &Value, now: DateTime<Utc>) -> Result<Self, RecordError> {
        let (reply_root, reply_parent) = match record.get("reply") {
            None | Some(Value::Null) => (None, None),
            Some(r) => (
                Some(strong_ref(r.get("root"), "reply.root")?),
                Some(strong_ref(r.get("parent"), "reply.parent")?),
            ),
        };
        let embed = record.get("embed").filter(|e| !e.is_null()).cloned();
        let quote_uri = embed.as_ref().and_then(quote_target);
        let facets = record.get("facets").filter(|f| !f.is_null()).cloned();
        let mention_dids = facets.as_ref().map(mentions).unwrap_or_default();
        let self_labels = record
            .get("labels")
            .and_then(|l| l.get("values"))
            .and_then(Value::as_array)
            .map(|vals| {
                vals.iter()
                    .filter_map(|v| v.get("val").and_then(Value::as_str))
                    .map(str::to_string)
                    .collect()
            })
            .unwrap_or_default();
        let langs = record.get("langs").and_then(Value::as_array).map(|a| {
            a.iter()
                .filter_map(Value::as_str)
                .map(str::to_string)
                .collect()
        });
        Ok(Self {
            text: record
                .get("text")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_string(),
            langs,
            reply_root,
            reply_parent,
            quote_uri,
            embed,
            facets,
            mention_dids,
            self_labels,
            extract: PostExtract::of(record),
            media_type: media_type(record),
            created_at: created_at(record, now),
        })
    }
}

/// The quoted post of an `app.bsky.embed.record` or `recordWithMedia` embed.
fn quote_target(embed: &Value) -> Option<String> {
    let ty = embed.get("$type").and_then(Value::as_str)?;
    let uri = match ty {
        "app.bsky.embed.record" => embed.get("record")?.get("uri"),
        "app.bsky.embed.recordWithMedia" => embed.get("record")?.get("record")?.get("uri"),
        _ => None,
    }?;
    uri.as_str().filter(|u| uri_author(u).is_some()).map(str::to_string)
}

/// Mention DIDs from `app.bsky.richtext.facet#mention` features.
fn mentions(facets: &Value) -> Vec<String> {
    let mut out: Vec<String> = Vec::new();
    for facet in facets.as_array().into_iter().flatten() {
        for feature in facet.get("features").and_then(Value::as_array).into_iter().flatten() {
            if feature.get("$type").and_then(Value::as_str) == Some("app.bsky.richtext.facet#mention")
                && let Some(did) = feature.get("did").and_then(Value::as_str)
                && proto_blue::syntax::Did::is_valid(did)
                && !out.iter().any(|d| d == did)
            {
                out.push(did.to_string());
            }
        }
    }
    out
}

/// An optional strong reference: absent, or unusable, is `None`. A broken
/// optional field does not fail its record.
fn optional_ref(v: Option<&Value>) -> Option<StrongRef> {
    strong_ref(v.filter(|v| !v.is_null()), "ref").ok()
}

/// `app.bsky.feed.like` / `app.bsky.feed.repost`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SubjectRecord {
    pub subject: StrongRef,
    /// The repost this like or repost came through (design-v3 §3.1).
    pub via: Option<StrongRef>,
    pub created_at: DateTime<Utc>,
}

impl SubjectRecord {
    /// # Errors
    ///
    /// A missing or invalid `subject` strong ref.
    pub fn parse(record: &Value, now: DateTime<Utc>) -> Result<Self, RecordError> {
        Ok(Self {
            subject: strong_ref(record.get("subject"), "subject")?,
            via: optional_ref(record.get("via")),
            created_at: created_at(record, now),
        })
    }
}

/// `app.bsky.graph.follow` / `app.bsky.graph.block`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GraphRecord {
    pub subject_did: String,
    /// The starter pack that led to this follow (design-v3 §3.1).
    pub via: Option<StrongRef>,
    pub created_at: DateTime<Utc>,
}

impl GraphRecord {
    /// # Errors
    ///
    /// A missing or invalid `subject` DID.
    pub fn parse(record: &Value, now: DateTime<Utc>) -> Result<Self, RecordError> {
        let subject = record
            .get("subject")
            .and_then(Value::as_str)
            .filter(|d| proto_blue::syntax::Did::is_valid(d))
            .ok_or_else(|| err("subject is not a DID"))?;
        Ok(Self {
            subject_did: subject.to_string(),
            via: optional_ref(record.get("via")),
            created_at: created_at(record, now),
        })
    }
}

/// `app.bsky.actor.profile` (rkey `self`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProfileRecord {
    pub display_name: Option<String>,
    pub description: Option<String>,
    pub avatar_cid: Option<String>,
    pub banner_cid: Option<String>,
    pub pronouns: Option<String>,
    pub website: Option<String>,
    pub joined_via_starter_pack: Option<StrongRef>,
    /// The record's `createdAt`, clamped as for posts; `None` when the
    /// record has none or it does not parse.
    pub created_at: Option<DateTime<Utc>>,
}

impl ProfileRecord {
    #[must_use]
    pub fn parse(record: &Value, now: DateTime<Utc>) -> Self {
        let text = |k: &str| record.get(k).and_then(Value::as_str).map(str::to_string);
        Self {
            display_name: text("displayName"),
            description: text("description"),
            avatar_cid: record.get("avatar").and_then(blob_cid),
            banner_cid: record.get("banner").and_then(blob_cid),
            pronouns: text("pronouns"),
            website: text("website"),
            joined_via_starter_pack: optional_ref(record.get("joinedViaStarterPack")),
            created_at: record
                .get("createdAt")
                .and_then(Value::as_str)
                .and_then(|s| DateTime::parse_from_rfc3339(s).ok())
                .map(|d| d.with_timezone(&Utc).min(now + MAX_FUTURE_SKEW)),
        }
    }
}

/// Rule-2 subjects of a private-tier record (§9.3): a private post's reply
/// parent and root authors and its `audienceList`'s author (the
/// private-threads design §2.5: a branch reply names its branch root's
/// audience, whose owner is named nowhere else).
#[must_use]
pub fn private_subjects(kind: KryphocronKind, record: &Value) -> Vec<String> {
    match kind {
        KryphocronKind::PostPrivate => {
            let mut out: Vec<String> = Vec::new();
            let mut push = |a: Option<&str>| {
                if let Some(a) = a
                    && !out.iter().any(|d| d == a)
                {
                    out.push(a.to_string());
                }
            };
            if let Some(reply) = record.get("reply").filter(|r| !r.is_null()) {
                for part in ["parent", "root"] {
                    push(reply.get(part).and_then(|p| p.get("uri")).and_then(Value::as_str).and_then(uri_author));
                }
            }
            // `audienceList`: an at-uri, or a strong ref `{uri}`.
            let audience = record.get("audienceList").and_then(|a| a.as_str().or_else(|| a.get("uri").and_then(Value::as_str)));
            push(audience.and_then(uri_author));
            out
        }
        KryphocronKind::Audience => Vec::new(),
    }
}

#[cfg(test)]
mod tests {
    use chrono::TimeZone;
    use serde_json::json;

    use super::*;

    fn now() -> DateTime<Utc> {
        Utc.with_ymd_and_hms(2026, 9, 26, 12, 0, 0).single().expect("valid")
    }

    #[test]
    fn created_at_is_clamped_and_defaulted() {
        assert_eq!(created_at(&json!({"createdAt": "2026-09-26T11:00:00Z"}), now()), now() - TimeDelta::hours(1));
        assert_eq!(created_at(&json!({"createdAt": "2030-01-01T00:00:00Z"}), now()), now() + MAX_FUTURE_SKEW);
        assert_eq!(created_at(&json!({"createdAt": "yesterday"}), now()), now());
        assert_eq!(created_at(&json!({}), now()), now());
    }

    #[test]
    fn parses_a_reply_with_quote_mentions_and_labels() {
        let r = json!({
            "text": "hello",
            "langs": ["en"],
            "createdAt": "2026-09-26T11:59:00Z",
            "reply": {
                "root": {"uri": "at://did:plc:root/app.bsky.feed.post/1", "cid": "bafyroot"},
                "parent": {"uri": "at://did:plc:parent/app.bsky.feed.post/2", "cid": {"$link": "bafyparent"}}
            },
            "embed": {"$type": "app.bsky.embed.recordWithMedia",
                      "record": {"record": {"uri": "at://did:plc:quoted/app.bsky.feed.post/3", "cid": "bafyq"}},
                      "media": {}},
            "facets": [{"index": {}, "features": [
                {"$type": "app.bsky.richtext.facet#mention", "did": "did:plc:mentioned"},
                {"$type": "app.bsky.richtext.facet#link", "uri": "https://x"},
                {"$type": "app.bsky.richtext.facet#mention", "did": "did:plc:mentioned"}
            ]}],
            "labels": {"$type": "com.atproto.label.defs#selfLabels", "values": [{"val": "nudity"}]}
        });
        let p = PostRecord::parse(&r, now()).expect("valid post");
        assert_eq!(p.text, "hello");
        assert_eq!(p.langs, Some(vec!["en".to_string()]));
        assert_eq!(p.reply_root.as_ref().map(|r| r.uri.as_str()), Some("at://did:plc:root/app.bsky.feed.post/1"));
        assert_eq!(p.reply_parent.as_ref().and_then(|r| r.cid.as_deref()), Some("bafyparent"));
        assert_eq!(p.quote_uri.as_deref(), Some("at://did:plc:quoted/app.bsky.feed.post/3"));
        assert_eq!(p.mention_dids, ["did:plc:mentioned"]);
        assert_eq!(p.self_labels, ["nudity"]);
    }

    #[test]
    fn a_broken_reply_is_an_error() {
        let r = json!({"text": "x", "reply": {"root": {"uri": "not-a-uri"}, "parent": {}}});
        assert!(PostRecord::parse(&r, now()).is_err());
    }

    #[test]
    fn simple_quote_embed() {
        let r = json!({"text": "q", "embed": {"$type": "app.bsky.embed.record", "record": {"uri": "at://did:plc:q/app.bsky.feed.post/1", "cid": "c"}}});
        assert_eq!(
            PostRecord::parse(&r, now()).expect("valid").quote_uri.as_deref(),
            Some("at://did:plc:q/app.bsky.feed.post/1")
        );
    }

    #[test]
    fn likes_follows_and_profiles() {
        let like = SubjectRecord::parse(&json!({"subject": {"uri": "at://did:plc:a/app.bsky.feed.post/1", "cid": "c"}, "createdAt": "2026-09-26T11:00:00Z"}), now()).expect("like");
        assert_eq!(like.subject.uri, "at://did:plc:a/app.bsky.feed.post/1");
        assert!(SubjectRecord::parse(&json!({"subject": "at://x"}), now()).is_err());

        let follow = GraphRecord::parse(&json!({"subject": "did:plc:b"}), now()).expect("follow");
        assert_eq!(follow.subject_did, "did:plc:b");
        assert!(GraphRecord::parse(&json!({"subject": "bob"}), now()).is_err());

        // `via` is optional, and a broken one does not fail the record.
        assert_eq!(like.via, None);
        let via = json!({"uri": "at://did:plc:r/app.bsky.feed.repost/9", "cid": "bafyvia"});
        let through = SubjectRecord::parse(&json!({"subject": {"uri": "at://did:plc:a/app.bsky.feed.post/1"}, "via": via}), now()).expect("like");
        assert_eq!(through.via.as_ref().map(|v| (v.uri.as_str(), v.cid.as_deref())), Some(("at://did:plc:r/app.bsky.feed.repost/9", Some("bafyvia"))));
        let pack = json!({"uri": "at://did:plc:p/app.bsky.graph.starterpack/1", "cid": "bafypack"});
        let joined = GraphRecord::parse(&json!({"subject": "did:plc:b", "via": pack}), now()).expect("follow");
        assert_eq!(joined.via.map(|v| v.uri), Some("at://did:plc:p/app.bsky.graph.starterpack/1".to_string()));
        assert_eq!(GraphRecord::parse(&json!({"subject": "did:plc:b", "via": "junk"}), now()).expect("follow").via, None);

        let profile = ProfileRecord::parse(
            &json!({
                "displayName": "B",
                "avatar": {"$type": "blob", "ref": "bafyavatar"},
                "banner": {"$type": "blob", "ref": {"$link": "bafybanner"}},
                "pronouns": "it/its",
                "website": "https://b.example",
                "joinedViaStarterPack": pack,
                "createdAt": "2030-01-01T00:00:00Z"
            }),
            now(),
        );
        assert_eq!(profile.pronouns.as_deref(), Some("it/its"));
        assert_eq!(profile.website.as_deref(), Some("https://b.example"));
        assert_eq!(profile.joined_via_starter_pack.as_ref().and_then(|p| p.cid.as_deref()), Some("bafypack"));
        assert_eq!(profile.created_at, Some(now() + MAX_FUTURE_SKEW), "clamped like a post's");
        assert_eq!(ProfileRecord::parse(&json!({}), now()).created_at, None);
        assert_eq!(profile.display_name.as_deref(), Some("B"));
        assert_eq!(profile.avatar_cid.as_deref(), Some("bafyavatar"));
        assert_eq!(profile.banner_cid.as_deref(), Some("bafybanner"));
        assert_eq!(profile.description, None);
    }

    #[test]
    fn private_record_subjects() {
        let reply = json!({"reply": {"parent": {"uri": "at://did:plc:p/tools.kryphocron.feed.postPrivate/1"},
                                     "root": {"uri": "at://did:plc:r/tools.kryphocron.feed.postPrivate/1"}}});
        assert_eq!(
            private_subjects(KryphocronKind::PostPrivate, &reply),
            ["did:plc:p", "did:plc:r"]
        );
        assert!(private_subjects(KryphocronKind::PostPrivate, &json!({})).is_empty());
        // Private threads §2.5: the audience's author, in either form, once.
        let branch = json!({"reply": {"parent": {"uri": "at://did:plc:p/tools.kryphocron.feed.postPrivate/1"},
                                      "root": {"uri": "at://did:plc:r/app.bsky.feed.post/1"}},
                            "audienceList": "at://did:plc:o/tools.kryphocron.policy.audience/1"});
        assert_eq!(private_subjects(KryphocronKind::PostPrivate, &branch), ["did:plc:p", "did:plc:r", "did:plc:o"]);
        let referenced = json!({"audienceList": {"uri": "at://did:plc:p/tools.kryphocron.policy.audience/1", "cid": "c"},
                                "reply": {"parent": {"uri": "at://did:plc:p/tools.kryphocron.feed.postPrivate/1"},
                                          "root": {"uri": "at://did:plc:p/tools.kryphocron.feed.postPrivate/0"}}});
        assert_eq!(private_subjects(KryphocronKind::PostPrivate, &referenced), ["did:plc:p"]);
        assert!(private_subjects(KryphocronKind::Audience, &json!({"members": []})).is_empty());
    }
}
