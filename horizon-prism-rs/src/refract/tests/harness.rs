//! A fake world for Refract's tests: Bluesky's AppView, a community
//! AppView, and one server playing the PLC directory, the PDS and the
//! labeler. Every answer is set by the test.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use axum::Router;
use axum::extract::{Path, RawQuery, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::IntoResponse;
use axum::routing::get;
use base64::Engine;
use proto_blue::crypto::{K256Keypair, Keypair, Signer};
use serde_json::{Value, json};
use sqlx::PgPool;

use crate::admin::settings::LiveSettings;
use crate::aggregations::{ScopeSets, ScopeState};
use crate::appview_clients::{AppViewAuth, AppViewClient};
use crate::config::{FileConfig, LabelSource, RefractCommunity, ScopePolicy};
use crate::identity::{IdentityResolver, IdentitySettings, PersistencePolicy};
use crate::net::{Intent, NetClient, Transports};
use crate::refract::Refract;
use crate::refract::net::{CONTENT_LABELERS, RefractNet};

pub const SUBJECT: &str = "did:plc:subjectaaaaaaaaaaaaaaaaa";
pub const OTHER: &str = "did:plc:otheraaaaaaaaaaaaaaaaaaa";
pub const LABELER: &str = "did:plc:labeleraaaaaaaaaaaaaaaaa";
pub const OTHER_LABELER: &str = "did:plc:labelerbbbbbbbbbbbbbbbbb";

/// What the fakes answer.
#[derive(Default)]
pub struct World {
    pub pds_base: String,
    /// Bluesky's `getProfile`: (status, body).
    pub bluesky: Option<(u16, Value)>,
    /// The community AppView's `getProfile`: (status, body).
    pub community: Option<(u16, Value)>,
    /// Whether the community AppView names the labeler it applied.
    pub names_labeler: bool,
    /// The community AppView's `getPosts` answer posts, by URI.
    pub posts: HashMap<String, Value>,
    pub repo: Option<(u16, Value)>,
    /// `queryLabels` answers: every label, one page.
    pub labels: Vec<Value>,
    /// The labeler's signing key, multibase.
    pub key: String,
    /// The DID document's PDS for each DID (default: the fake's base).
    pub pds_of: HashMap<String, String>,
    pub calls: Vec<String>,
}

pub type Shared = Arc<Mutex<World>>;

async fn bluesky(State(w): State<Shared>) -> impl IntoResponse {
    let mut w = w.lock().expect("world");
    w.calls.push("bluesky.getProfile".into());
    let (s, b) = w.bluesky.clone().unwrap_or((502, json!({})));
    (StatusCode::from_u16(s).expect("status"), axum::Json(b))
}

async fn community(State(w): State<Shared>, Path(method): Path<String>, headers: HeaderMap, RawQuery(q): RawQuery) -> impl IntoResponse {
    let mut w = w.lock().expect("world");
    w.calls.push(format!("community.{method}"));
    let accepted = headers.get("atproto-accept-labelers").and_then(|v| v.to_str().ok()).map(str::to_string);
    let mut out = HeaderMap::new();
    if w.names_labeler
        && let Some(a) = accepted
    {
        out.insert(CONTENT_LABELERS, a.parse().expect("header"));
    }
    let (s, b) = match method.as_str() {
        "app.bsky.actor.getProfile" => w.community.clone().unwrap_or((502, json!({}))),
        "app.bsky.feed.getPosts" => {
            let wanted: Vec<String> = url::form_urlencoded::parse(q.unwrap_or_default().as_bytes()).filter(|(k, _)| k == "uris").map(|(_, v)| v.to_string()).collect();
            let posts: Vec<Value> = wanted.iter().filter_map(|u| w.posts.get(u).cloned()).collect();
            (200, json!({"posts": posts}))
        }
        _ => (404, json!({})),
    };
    (StatusCode::from_u16(s).expect("status"), out, axum::Json(b))
}

async fn plc(State(w): State<Shared>, Path(did): Path<String>) -> axum::Json<Value> {
    let w = w.lock().expect("world");
    if did == LABELER || did == OTHER_LABELER {
        return axum::Json(json!({
            "id": did,
            "verificationMethod": [{"id": "#atproto_label", "type": "Multikey", "publicKeyMultibase": w.key}],
            "service": [{"id": "#atproto_labeler", "type": "AtprotoLabeler", "serviceEndpoint": w.pds_base}]
        }));
    }
    let pds = w.pds_of.get(&did).cloned().unwrap_or_else(|| w.pds_base.clone());
    axum::Json(json!({"id": did, "service": [{"id": "#atproto_pds", "type": "AtprotoPersonalDataServer", "serviceEndpoint": pds}]}))
}

async fn repo_status(State(w): State<Shared>) -> impl IntoResponse {
    let mut w = w.lock().expect("world");
    w.calls.push("pds.getRepoStatus".into());
    let (s, b) = w.repo.clone().unwrap_or((502, json!({})));
    (StatusCode::from_u16(s).expect("status"), axum::Json(b))
}

async fn query_labels(State(w): State<Shared>, RawQuery(q): RawQuery) -> axum::Json<Value> {
    let mut w = w.lock().expect("world");
    w.calls.push("labeler.queryLabels".into());
    let q = q.unwrap_or_default();
    let cursor = url::form_urlencoded::parse(q.as_bytes()).find(|(k, _)| k == "cursor").map(|(_, v)| v.to_string());
    let n = w.labels.len();
    // One page, then an empty page with the same cursor.
    if cursor.as_deref() == Some(&n.to_string()) {
        return axum::Json(json!({"cursor": n.to_string(), "labels": []}));
    }
    axum::Json(json!({"cursor": n.to_string(), "labels": w.labels}))
}

struct NeverPersist;

impl PersistencePolicy for NeverPersist {
    fn in_persistence_set(&self, _did: &str) -> bool {
        false
    }
}

/// The fakes and a Refract over them.
pub struct Harness {
    pub world: Shared,
    pub key: K256Keypair,
    pub refract: Refract,
    pub live: LiveSettings,
    pub pool: PgPool,
}

/// The tier 1 test community, its labeler read through its AppView.
#[must_use]
pub fn tier1() -> RefractCommunity {
    RefractCommunity {
        name: "sky".into(),
        pds_patterns: vec!["127.0.0.1".into()],
        tier: 1,
        appview_network: Some("skyview".into()),
        labeler_did: Some(LABELER.into()),
        label_source: Some(LabelSource::Appview),
        gone_labels: vec!["!takedown".into(), "!hide".into()],
        enabled: true,
    }
}

/// The tier 2 test community, its labeler read directly.
#[must_use]
pub fn tier2() -> RefractCommunity {
    RefractCommunity {
        name: "north".into(),
        pds_patterns: vec!["127.0.0.1".into()],
        tier: 2,
        appview_network: None,
        labeler_did: Some(LABELER.into()),
        label_source: Some(LabelSource::Labeler),
        gone_labels: vec!["!takedown".into(), "!hide".into()],
        enabled: true,
    }
}

impl Harness {
    pub async fn new(pool: &PgPool, sets: ScopeSets, communities: Vec<RefractCommunity>) -> Self {
        let world: Shared = Arc::default();
        let key = K256Keypair::generate();
        let multikey = proto_blue::crypto::format_multikey("ES256K", &key.public_key_compressed());
        let pds = crate::test_support::serve_fake(
            Router::new()
                .route("/xrpc/com.atproto.sync.getRepoStatus", get(repo_status))
                .route("/xrpc/com.atproto.label.queryLabels", get(query_labels))
                .route("/{did}", get(plc))
                .with_state(Arc::clone(&world)),
        )
        .await;
        let bsky = crate::test_support::serve_fake(Router::new().route("/xrpc/{method}", get(bluesky)).with_state(Arc::clone(&world))).await;
        let comm = crate::test_support::serve_fake(Router::new().route("/xrpc/{method}", get(community)).with_state(Arc::clone(&world))).await;
        {
            let mut w = world.lock().expect("world");
            w.pds_base = pds.clone();
            w.key = multikey;
            w.names_labeler = true;
        }
        let mut file = FileConfig::default();
        file.refract.enabled = true;
        file.refract.communities = communities;
        let live = LiveSettings::new(&file);
        let net = NetClient::loopback_for_tests();
        let appviews = vec![
            AppViewClient::new("bluesky", &bsky, AppViewAuth::Unauth, net.clone()),
            AppViewClient::new("skyview", &comm, AppViewAuth::Unauth, net.clone()),
        ];
        let mut cfg = file.refract.clone();
        // No waiting on buckets in tests.
        cfg.bluesky_rate_per_sec = crate::config::Float(1000.0);
        cfg.per_pds_rate_per_sec = crate::config::Float(1000.0);
        cfg.sweep_rate_per_sec = crate::config::Float(1000.0);
        let settings = IdentitySettings { plc_url: pds, fresh: std::time::Duration::from_secs(3600), stale_max: std::time::Duration::from_secs(86_400) };
        let resolver = IdentityResolver::new(Transports::loopback_for_tests(), pool.clone(), settings, Arc::new(NeverPersist), Intent::Internal);
        let scope = ScopeState::new(sets, ScopePolicy::MembersAndOneHop, 10_000);
        let refract = Refract::new(pool.clone(), scope, live.clone(), cfg.clone(), RefractNet::new(net, appviews, None, &cfg), &resolver);
        Self { world, key, refract, live, pool: pool.clone() }
    }

    pub fn set(&self, f: impl FnOnce(&mut World)) {
        f(&mut self.world.lock().expect("world"));
    }

    pub fn calls(&self) -> Vec<String> {
        std::mem::take(&mut self.world.lock().expect("world").calls)
    }

    /// A label signed with the labeler's key.
    pub fn signed(&self, label: Value) -> Value {
        let bytes = proto_blue::lex_cbor::encode(&proto_blue::lex_json::json_to_lex(&label)).expect("cbor");
        let sig = self.key.sign(&bytes).expect("sign");
        let mut out = label;
        out["sig"] = json!({"$bytes": base64::engine::general_purpose::STANDARD_NO_PAD.encode(sig)});
        out
    }

    /// The happy world: Bluesky withholds, the PDS says active, the
    /// community serves the profile with no labels.
    pub fn all_pass(&self) {
        self.set(|w| {
            w.bluesky = Some((400, json!({"error": "AccountTakedown", "message": "Account has been taken down"})));
            w.repo = Some((200, json!({"did": SUBJECT, "active": true})));
            w.community = Some((200, json!({"did": SUBJECT, "handle": "s.test", "labels": []})));
            w.labels = Vec::new();
        });
    }

    /// Insert a row in `state`, due now, hosted on the fake.
    pub async fn row(&self, did: &str, community: &str, state: &str) {
        sqlx::query(
            "INSERT INTO refract_dids (did, community, pds_host, state, source, candidate_since, next_check_at) \
             VALUES ($1, $2, '127.0.0.1', $3, 'fill', now(), now() - interval '1 second')",
        )
        .bind(did)
        .bind(community)
        .bind(state)
        .execute(&self.pool)
        .await
        .expect("row");
    }

    pub async fn state(&self, did: &str) -> Option<String> {
        sqlx::query_scalar("SELECT state FROM refract_dids WHERE did = $1").bind(did).fetch_optional(&self.pool).await.expect("state")
    }

    /// Make due and run one check pass; returns how many were claimed.
    pub async fn check(&self, did: &str) -> usize {
        sqlx::query("UPDATE refract_dids SET next_check_at = now() - interval '1 second' WHERE did = $1").bind(did).execute(&self.pool).await.expect("due");
        self.refract.check_due().await.expect("check")
    }

    pub async fn sets(&self) -> ScopeSets {
        self.refract.scope().snapshot().await
    }

    pub async fn count(&self, sql: &str) -> i64 {
        sqlx::query_scalar(sql).fetch_one(&self.pool).await.expect("count")
    }
}
