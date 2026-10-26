//! `app.nearhorizon.constellation.resolveDid` (design-v2 §8.7): a public
//! client routing hint.
//!
//! - Query (GET), no auth; rate limit 10/s, burst 20 per IP (§7.4).
//! - A malformed DID is `400 InvalidRequest`; an unresolvable DID is
//!   `{did, pds: null, handle: null, homeNetwork: null, homeAppview: null, …}`.
//! - Responses carry `cache-control: public, max-age=300`.

use std::net::SocketAddr;

use axum::extract::{ConnectInfo, RawQuery, State};
use axum::http::{HeaderMap, HeaderValue, header};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::{Json, Router};

use super::routing::{RoutingDecision, Router as DidRouter};
use crate::hydration::hint::{RoutingHint, routing_hint};
use crate::xrpc::AppState;
use crate::xrpc::auth::{Params, request_handle, request_ip};
use crate::xrpc::errors::XrpcError;
use crate::xrpc::rate_limit::Family;

pub const RESOLVE_DID: &str = "app.nearhorizon.constellation.resolveDid";
/// `cache-control` on every answer (§8.7).
pub const CACHE_CONTROL: &str = "public, max-age=300";

/// Handler state.
#[derive(Clone)]
pub struct ResolveDidState {
    pub app: AppState,
    pub router: DidRouter,
}

/// The `resolveDid` route, merged into the app by `serve`.
pub fn router(state: ResolveDidState) -> Router {
    Router::new()
        .route(&crate::admin::usage::path("resolve_did", RESOLVE_DID), get(resolve_did))
        .with_state(state)
}

pub async fn resolve_did(
    State(s): State<ResolveDidState>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    RawQuery(q): RawQuery,
) -> Result<Response, XrpcError> {
    let ip = request_ip(&s.app, &headers, peer);
    s.app.limits.charge(RESOLVE_DID, Family::Resolve, None, ip)?;
    let req = request_handle(None, ip);
    let params = Params::parse(q.as_deref());
    let did = params.required("did")?;
    if !proto_blue::syntax::Did::is_valid(did) {
        return Err(XrpcError::invalid("Error: did must be a valid did"));
    }
    let summary = s.router.lookup().summary(&req, did).await;
    // No budget for the lookup is a 429, never the unresolvable shape
    // (design-v3 §II-8.3: the DID is this request's primary subject).
    if let (None, Some(exhausted)) = (&summary, req.exhausted()) {
        return Err(exhausted.into());
    }
    let summary = summary.unwrap_or_default();
    let decision = summary
        .pds
        .as_deref()
        .map_or_else(RoutingDecision::unknown, |pds| s.router.table().decide(pds));
    let table = s.router.table();
    let cascade = table.cascade();
    // C4 (public-indexing design §11): unless the mode is On, a community
    // author's public reads go to the cascade, so the hint names its first
    // AppView rather than this one.
    let home_appview = if decision.is_community() && !s.app.hydrator.scope().public_mode().is_on() {
        cascade.first().and_then(|name| table.network(name)).and_then(|n| n.appview_url.clone())
    } else {
        decision.appview_url
    };
    let hint = RoutingHint {
        did: did.to_string(),
        pds: summary.pds,
        handle: summary.handle,
        home_network: decision.network_name,
        home_appview,
        cascade,
        local_indexed: s.app.hydrator.stores_public_of(did).await,
    };
    let mut response = Json(routing_hint(&hint)).into_response();
    response
        .headers_mut()
        .insert(header::CACHE_CONTROL, HeaderValue::from_static(CACHE_CONTROL));
    Ok(response)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use serde_json::{Value, json};
    use tokio::net::TcpListener;

    use super::*;
    use crate::aggregations::ScopeSets;
    use crate::constellation::routing::DidLookup;
    use crate::constellation::routing::tests::{StubLookup, table_with_community};
    use crate::hydration::testing::{hydrator, set_mode};
    use crate::indexing::PublicMode;
    use crate::kryphocron_glue::jwt::tests::{StubFetcher, verifier};
    use crate::test_support::fresh_db;
    use crate::xrpc::RateLimits;

    #[tokio::test]
    async fn hints_for_known_community_and_unknown_dids() {
        let Some(db) = fresh_db("resolve_did").await else {
            return;
        };
        let lookup = Arc::new(StubLookup::default());
        {
            let mut pds = lookup.pds.lock().expect("lock");
            pds.insert("did:plc:bbbbbbbbbbbbbbbbbbbbbbbb".into(), "https://morel.us-east.host.bsky.network".into());
            pds.insert("did:plc:cccccccccccccccccccccccc".into(), "https://pds1.community.example".into());
        }
        let mut sets = ScopeSets::default();
        sets.scope.insert("did:plc:cccccccccccccccccccccccc".into());
        let (auth, _d) = verifier(StubFetcher::default(), 10, 4);
        let app = AppState {
            hydrator: hydrator(&db.pool, sets),
            auth: Arc::new(auth),
            limits: RateLimits::new(),
            enroller: None,
            trusted_proxy: "172.30.0.10".parse().expect("ip"),
            did_document: Arc::new(json!({})),
            membership: Default::default(),
        };
        let hydrator_handle = app.hydrator.clone();
        let state = ResolveDidState {
            app,
            router: DidRouter::new(
                Arc::new(table_with_community(&["*.community.example"])),
                lookup as Arc<dyn DidLookup>,
            ),
        };
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let addr = listener.local_addr().expect("addr");
        tokio::spawn(async move {
            axum::serve(listener, router(state).into_make_service_with_connect_info::<SocketAddr>())
                .await
                .expect("serve");
        });
        let client = reqwest::Client::new();
        let get = |did: &str| {
            let client = client.clone();
            let url = format!("http://{addr}/xrpc/{RESOLVE_DID}?did={did}");
            async move {
                let r = client.get(url).send().await.expect("request");
                let cache = r.headers().get("cache-control").map(|v| v.to_str().expect("ascii").to_string());
                (r.status().as_u16(), cache, r.json::<Value>().await.expect("json"))
            }
        };

        let (status, cache, v) = get("did:plc:bbbbbbbbbbbbbbbbbbbbbbbb").await;
        assert_eq!(status, 200);
        assert_eq!(cache.as_deref(), Some(CACHE_CONTROL));
        assert_eq!(
            v,
            json!({"did": "did:plc:bbbbbbbbbbbbbbbbbbbbbbbb", "pds": "https://morel.us-east.host.bsky.network",
                   "handle": "bbbb.test", "homeNetwork": "bluesky", "homeAppview": "https://api.bsky.app",
                   "cascade": ["bluesky", "community-a"], "capabilities": {"localIndexed": false}})
        );
        let (_, _, v) = get("did:plc:cccccccccccccccccccccccc").await;
        assert_eq!((v["homeNetwork"].clone(), v["homeAppview"].clone()), (json!("community"), Value::Null));
        assert_eq!(v["capabilities"]["localIndexed"], true);
        let (status, _, v) = get("did:plc:uuuuuuuuuuuuuuuuuuuuuuuu").await;
        assert_eq!(status, 200);
        assert_eq!((v["pds"].clone(), v["homeNetwork"].clone(), v["homeAppview"].clone()), (Value::Null, Value::Null, Value::Null));
        assert_eq!(v["cascade"], json!(["bluesky", "community-a"]));
        let (status, _, v) = get("not-a-did").await;
        assert_eq!((status, v["error"].as_str()), (400, Some("InvalidRequest")));

        // T-16, C4 (public-indexing design §9.1, §11): unless the mode is
        // On nothing is `localIndexed`, and a community DID's hint names
        // the first cascade AppView, which its public reads go to.
        for mode in [PublicMode::Filling, PublicMode::Off, PublicMode::Held] {
            set_mode(hydrator_handle.scope(), mode).await;
            let (_, _, v) = get("did:plc:cccccccccccccccccccccccc").await;
            assert_eq!(v["capabilities"]["localIndexed"], false, "{mode:?}");
            assert_eq!((v["homeNetwork"].clone(), v["homeAppview"].clone()), (json!("community"), json!("https://api.bsky.app")), "{mode:?}");
            let (_, _, v) = get("did:plc:bbbbbbbbbbbbbbbbbbbbbbbb").await;
            assert_eq!(v["homeAppview"], "https://api.bsky.app", "{mode:?}");
        }
        set_mode(hydrator_handle.scope(), PublicMode::On).await;
        let (_, _, v) = get("did:plc:cccccccccccccccccccccccc").await;
        assert_eq!((v["homeAppview"].clone(), v["capabilities"]["localIndexed"].clone()), (Value::Null, json!(true)));

        // 10/s, burst 20 per IP.
        let mut limited = false;
        for _ in 0..30 {
            if get("did:plc:bbbbbbbbbbbbbbbbbbbbbbbb").await.0 == 429 {
                limited = true;
                break;
            }
        }
        assert!(limited, "the per-IP bucket applies");
        db.drop_db().await;
    }
}
