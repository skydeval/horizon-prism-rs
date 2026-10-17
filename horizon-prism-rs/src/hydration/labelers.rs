//! `app.bsky.labeler.getServices` (design-v3 §5.7). Clients call it on
//! every session for the labelers they subscribe to, most of which are
//! not indexed here.
//!
//! - A labeler stored here is read from `labeler_services`; any other from
//!   its `app.bsky.labeler.service/self` record through Slingshot and the
//!   read-time cache. A DID with neither is left out.
//! - The creator is a profile view built like any embedded profile, so a
//!   labeler is never left out for want of one.

use std::collections::HashMap;
use std::time::Instant;

use chrono::{DateTime, Utc};
use serde_json::{Value, json};

use super::actor::ProfileKind;
use super::{CountMode, Hydrator, ViewError, ViewResult, Viewer, ts};
use crate::moderation;
use crate::xrpc::pools::Requester;

const NSID_SERVICE: &str = "app.bsky.labeler.service";
/// Labelers whose like count may be fetched while the request waits.
const SYNC_COUNTS: usize = 25;

/// What a labeler view is built from.
struct Service {
    uri: String,
    cid: String,
    indexed_at: DateTime<Utc>,
    policies: Value,
    reason_types: Option<Value>,
    subject_types: Option<Value>,
    subject_collections: Option<Value>,
}

/// A stored labeler: URI, creator, CID, the four record fields,
/// `indexed_at`.
type ServiceRow = (String, String, String, Value, Option<Value>, Option<Value>, Option<Value>, DateTime<Utc>);

fn service_uri(did: &str) -> String {
    format!("at://{did}/{NSID_SERVICE}/self")
}

impl Hydrator {
    /// `app.bsky.labeler.getServices`.
    ///
    /// # Errors
    ///
    /// Database errors.
    pub async fn get_labeler_services(&self, viewer: &Viewer, req: &Requester, dids: &[String], detailed: bool) -> ViewResult<Value> {
        let max = usize::try_from(self.inner.cfg.labelers_max_dids).unwrap_or(usize::MAX);
        if dids.len() > max {
            return Ok(Err(ViewError::new("InvalidRequest", format!("Error: dids must not have more than {max} elements"))));
        }
        let started = Instant::now();
        let mut wanted: Vec<String> = Vec::new();
        for did in dids {
            if proto_blue::syntax::Did::is_valid(did) && !wanted.contains(did) {
                wanted.push(did.clone());
            }
        }
        let rows: Vec<ServiceRow> = sqlx::query_as(
            "SELECT uri, creator_did, cid, policies, reason_types, subject_types, subject_collections, indexed_at \
             FROM labeler_services WHERE creator_did = ANY($1)",
        )
        .bind(&wanted)
        .fetch_all(self.pool())
        .await?;
        let mut services: HashMap<String, Service> = rows
            .into_iter()
            .map(|(uri, creator, cid, policies, reason_types, subject_types, subject_collections, indexed_at)| {
                (creator, Service { uri, cid, indexed_at, policies, reason_types, subject_types, subject_collections })
            })
            .collect();
        // The moderation gate comes before any fetch.
        let uris: Vec<String> = wanted.iter().map(|d| service_uri(d)).collect();
        let gated: Vec<String> = wanted.iter().chain(&uris).cloned().collect();
        let gates = moderation::gate_many(self.pool(), &gated).await?;
        let hidden = |did: &str| [did.to_string(), service_uri(did)].iter().any(|s| gates.get(s).is_some_and(|g| g.is_hidden()));
        for did in &wanted {
            if services.contains_key(did) || hidden(did) {
                continue;
            }
            let Some(record) = self.upstream().record(req, &service_uri(did)).await else { continue };
            let value = &record["value"];
            let field = |name: &str| value.get(name).filter(|v| !v.is_null()).cloned();
            let Some(policies) = field("policies") else { continue };
            services.insert(
                did.clone(),
                Service {
                    uri: service_uri(did),
                    cid: record["cid"].as_str().unwrap_or_default().to_string(),
                    // The time it was fetched: nothing here indexed it.
                    indexed_at: Utc::now(),
                    policies,
                    reason_types: field("reasonTypes"),
                    subject_types: field("subjectTypes"),
                    subject_collections: field("subjectCollections"),
                },
            );
        }
        let shown: Vec<String> = wanted.iter().filter(|d| services.contains_key(*d) && !hidden(d)).cloned().collect();
        let shown_uris: Vec<String> = shown.iter().map(|d| service_uri(d)).collect();
        let creators = self.profile_context(viewer, req, &shown, None).await?;
        let labels = self.load_labels(&shown_uris).await?;
        // Like counts: the first labelers of the request while it waits,
        // the rest from what is cached; the likes stored here otherwise.
        let (first, rest) = shown_uris.split_at(shown_uris.len().min(SYNC_COUNTS));
        let mut remote = self.constellation_counts(req, first, CountMode::Sync { started }).await;
        remote.extend(self.constellation_counts(req, rest, CountMode::List).await);
        // By account, and the viewer's newest like, as for posts (#727).
        let local: HashMap<String, i64> =
            sqlx::query_as("SELECT subject_uri, count(DISTINCT author_did) FROM likes WHERE subject_uri = ANY($1) GROUP BY subject_uri")
                .bind(&shown_uris)
                .fetch_all(self.pool())
                .await?
                .into_iter()
                .collect();
        let mine: HashMap<String, String> = match viewer.state_did() {
            Some(me) => sqlx::query_as(
                "SELECT DISTINCT ON (subject_uri) subject_uri, uri FROM likes WHERE author_did = $1 AND subject_uri = ANY($2) \
                 ORDER BY subject_uri, created_at DESC, uri DESC",
            )
            .bind(me)
            .bind(&shown_uris)
            .fetch_all(self.pool())
            .await?
            .into_iter()
            .collect(),
            None => HashMap::new(),
        };
        let views: Vec<Value> = shown
            .iter()
            .filter_map(|did| {
                let s = services.get(did)?;
                let likes = remote.get(&s.uri).and_then(|c| c.likes).map_or_else(|| local.get(&s.uri).copied().unwrap_or(0), i64::from);
                let mut view = json!({
                    "$type": if detailed { "app.bsky.labeler.defs#labelerViewDetailed" } else { "app.bsky.labeler.defs#labelerView" },
                    "uri": s.uri,
                    "cid": s.cid,
                    "creator": creators.view(did, ProfileKind::View),
                    "likeCount": likes,
                    "viewer": {},
                    "indexedAt": ts(s.indexed_at),
                    "labels": labels.get(&s.uri).cloned().unwrap_or_default(),
                });
                if let Some(like) = mine.get(&s.uri) {
                    view["viewer"]["like"] = json!(like);
                }
                if detailed {
                    view["policies"] = s.policies.clone();
                    for (name, value) in [("reasonTypes", &s.reason_types), ("subjectTypes", &s.subject_types), ("subjectCollections", &s.subject_collections)] {
                        if let Some(value) = value {
                            view[name] = value.clone();
                        }
                    }
                }
                Some(view)
            })
            .collect();
        Ok(Ok(Self::emit(json!({ "views": views }))))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::aggregations::ScopeSets;
    use crate::hydration::testing::{FixedUpstream, actor, exec, hydrator_with};
    use crate::test_support::fresh_db;

    const LOCAL: &str = "did:plc:aaaaaaaaaaaaaaaaaaaaaaaa";
    const FAR: &str = "did:plc:ffffffffffffffffffffffff";
    const NONE: &str = "did:plc:nnnnnnnnnnnnnnnnnnnnnnnn";
    const ME: &str = "did:plc:mmmmmmmmmmmmmmmmmmmmmmmm";

    /// B-47: stored and fetched labelers, the two view shapes, the cap.
    #[tokio::test]
    async fn labelers_stored_here_and_elsewhere() {
        let Some(db) = fresh_db("hydration_labelers").await else {
            return;
        };
        let p = &db.pool;
        actor(p, LOCAL, "labeler.test", "Labeler").await;
        let policies = r#"{"labelValues": ["spam"]}"#;
        exec(p, &format!(
            "INSERT INTO labeler_services (uri, creator_did, cid, rev, policies, reason_types, record, created_at) VALUES \
             ('at://{LOCAL}/{NSID_SERVICE}/self', '{LOCAL}', 'bafylocal', '3k', '{policies}', '[\"com.atproto.moderation.defs#reasonSpam\"]', '{{}}', now())"
        )).await;
        exec(p, &format!(
            "INSERT INTO likes (uri, author_did, subject_uri, subject_cid, rev, created_at) VALUES \
             ('at://{ME}/app.bsky.feed.like/1', '{ME}', 'at://{LOCAL}/{NSID_SERVICE}/self', 'bafylocal', '3k', now())"
        )).await;
        let mut up = FixedUpstream::default();
        up.records.insert(
            service_uri(FAR),
            json!({"uri": service_uri(FAR), "cid": "bafyfar", "value": {"policies": {"labelValues": ["rude"]}, "subjectTypes": ["account"]}}),
        );
        let h = hydrator_with(p, ScopeSets::default(), Arc::new(up));
        let me = Viewer {
            did: Some(ME.into()),
            hydrate: true,
        };
        let req = Requester::test();
        let dids = vec![FAR.to_string(), NONE.to_string(), LOCAL.to_string()];

        // A repeat and a non-DID are left out.
        let odd = vec![LOCAL.to_string(), LOCAL.to_string(), "not a did".to_string()];
        let v = h.get_labeler_services(&me, &req, &odd, false).await.expect("db").expect("view");
        assert_eq!(v.inner()["views"].as_array().map(Vec::len), Some(1));
        // Input order; a DID with no labeler is left out.
        let v = h.get_labeler_services(&me, &req, &dids, false).await.expect("db").expect("view");
        let views = v.inner()["views"].as_array().expect("views");
        assert_eq!(views.iter().map(|x| x["creator"]["did"].as_str().expect("did")).collect::<Vec<_>>(), [FAR, LOCAL]);
        assert!(views.iter().all(|x| x["$type"] == "app.bsky.labeler.defs#labelerView" && x.get("policies").is_none() && x["indexedAt"].is_string()));
        assert_eq!((&views[0]["cid"], &views[0]["likeCount"]), (&json!("bafyfar"), &json!(0)));
        // The stored one: its creator's profile, its likes, the viewer's.
        assert_eq!((&views[1]["creator"]["handle"], &views[1]["likeCount"]), (&json!("labeler.test"), &json!(1)));
        assert_eq!(views[1]["viewer"]["like"], json!(format!("at://{ME}/app.bsky.feed.like/1")));
        // Detailed: the record's fields, only those it has.
        let v = h.get_labeler_services(&Viewer::anonymous(), &req, &dids, true).await.expect("db").expect("view");
        let views = v.inner()["views"].as_array().expect("views");
        assert_eq!(views[0]["$type"], "app.bsky.labeler.defs#labelerViewDetailed");
        assert_eq!((&views[0]["policies"]["labelValues"][0], &views[0]["subjectTypes"]), (&json!("rude"), &json!(["account"])));
        assert!(views[0].get("reasonTypes").is_none() && views[1]["reasonTypes"].is_array());
        assert!(views[1]["viewer"].get("like").is_none());
        // A labeler taken down here is left out, stored or not.
        exec(p, &format!("INSERT INTO labels (subject, val) VALUES ('{FAR}', '!takedown')")).await;
        let v = h.get_labeler_services(&me, &req, &dids, false).await.expect("db").expect("view");
        assert_eq!(v.inner()["views"].as_array().map(Vec::len), Some(1));
        // The cap (3 in tests).
        let many: Vec<String> = (0..4).map(|i| format!("did:plc:{}", "abcd".repeat(6).replacen('a', ["b", "c", "d", "e"][i], 1))).collect();
        assert_eq!(h.get_labeler_services(&me, &req, &many, false).await.expect("db").expect_err("cap").error, "InvalidRequest");
        db.drop_db().await;
    }
}
