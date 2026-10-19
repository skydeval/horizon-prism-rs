//! Test helpers for the live feed (admin-ui-live-feed design §11): an
//! admin router served on a local port with a stub identity, a session
//! made directly in the database, and a reader of server-sent events.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use proto_blue::common::fetch::{FetchError, FetchHandler, HttpRequest, HttpResponse};
use reqwest::header::HeaderMap;
use sqlx::PgPool;
use tokio::net::TcpListener;
use tokio::sync::oneshot;

use super::live::Hub;
use super::operators::OperatorIdentity;
use super::{AdminSettings, AdminState, MembersInfo, router};
use crate::aggregations::{ScopeSets, ScopeState};
use crate::config::ScopePolicy;
use crate::identity::DidDocInfo;

/// The operator of [`serve`]'s sessions.
pub const OPERATOR: &str = "did:plc:oooooooooooooooooooooooo";
/// The operator's server.
pub const OPERATOR_PDS: &str = "https://pds.example.com";

/// Every DID on [`OPERATOR_PDS`].
struct FixedIdentity;

#[async_trait]
impl OperatorIdentity for FixedIdentity {
    async fn resolve_handle(&self, _handle: &str) -> Option<String> {
        None
    }
    async fn resolve_fresh(&self, _did: &str) -> Result<DidDocInfo, String> {
        Ok(DidDocInfo { pds_endpoint: Some(OPERATOR_PDS.to_string()), claimed_handle: None })
    }
}

/// No OAuth server: these tests sign in through the database.
struct NoFetch;

#[async_trait]
impl FetchHandler for NoFetch {
    async fn fetch(&self, _req: HttpRequest) -> Result<HttpResponse, FetchError> {
        Err(FetchError::Network("no network in tests".into()))
    }
}

/// A served admin router.
pub struct Served {
    pub base: String,
    shutdown: Option<oneshot::Sender<()>>,
}

impl Served {
    /// Resolve the graceful-shutdown future, as `serve` does at SIGTERM:
    /// the hub closes first.
    pub fn shut_down(&mut self) {
        if let Some(tx) = self.shutdown.take() {
            let _ = tx.send(());
        }
    }
}

/// The admin router over `pool` with `hub`, its operator [`OPERATOR`],
/// and `identity` (a fixed one when `None`).
pub async fn serve(pool: &PgPool, hub: Hub, identity: Option<Arc<dyn OperatorIdentity>>) -> Served {
    sqlx::query("INSERT INTO admin_operators (did, added_by) VALUES ($1, 'cli') ON CONFLICT DO NOTHING")
        .bind(OPERATOR)
        .execute(pool)
        .await
        .expect("operator");
    let settings = AdminSettings {
        public_hostname: "prism.example.com".to_string(),
        trusted_proxy: "127.0.0.1".parse().expect("ip"),
        session_idle: Duration::from_secs(28_800),
        session_max: Duration::from_secs(604_800),
        setup_code_file: std::env::temp_dir().join(format!("hprs-live-{}", rand::random::<u32>())),
        members: MembersInfo { auto_enroll: false, max_members: 1000, enroll_pds_patterns: Vec::new() },
    };
    let scope = ScopeState::new(ScopeSets::default(), ScopePolicy::MembersAndOneHop, 10_000);
    let identity = identity.unwrap_or_else(|| Arc::new(FixedIdentity));
    let state = AdminState::new(pool.clone(), scope, identity, Arc::new(NoFetch), settings, hub.clone());
    let app = router(state);
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().expect("addr");
    let (tx, rx) = oneshot::channel::<()>();
    tokio::spawn(async move {
        axum::serve(listener, app.into_make_service_with_connect_info::<SocketAddr>())
            .with_graceful_shutdown(async move {
                let _ = rx.await;
                hub.close();
            })
            .await
            .expect("serve");
    });
    Served { base: format!("http://{addr}"), shutdown: Some(tx) }
}

/// A fresh session for `did` on `endpoint`, made in the database; its
/// cookie header value.
pub async fn session(pool: &PgPool, did: &str, endpoint: &str) -> String {
    let bytes = rand::random::<[u8; 32]>();
    sqlx::query("INSERT INTO admin_sessions (id_hash, did, pds_endpoint, expires_at) VALUES ($1, $2, $3, now() + interval '7 days')")
        .bind(blake3::hash(&bytes).as_bytes().as_slice())
        .bind(did)
        .bind(endpoint)
        .execute(pool)
        .await
        .expect("session");
    format!("{}={}", super::auth::SESSION_COOKIE, hex::encode(bytes))
}

/// The `id_hash` of a session cookie header value.
#[must_use]
pub fn id_hash(cookie: &str) -> Vec<u8> {
    let hex_part = cookie.split_once('=').expect("cookie").1;
    let bytes = hex::decode(hex_part).expect("hex");
    blake3::hash(&bytes).as_bytes().to_vec()
}

/// A random page id.
#[must_use]
pub fn page_id() -> String {
    hex::encode(rand::random::<[u8; 16]>())
}

/// An open event stream, read as it arrives.
pub struct Sse {
    pub status: u16,
    pub headers: HeaderMap,
    response: Option<reqwest::Response>,
    buf: String,
}

impl Sse {
    /// `GET base + path` with `cookie`.
    pub async fn open(base: &str, path: &str, cookie: Option<&str>) -> Self {
        let client = reqwest::Client::builder().redirect(reqwest::redirect::Policy::none()).build().expect("client");
        let mut req = client.get(format!("{base}{path}"));
        if let Some(c) = cookie {
            req = req.header("cookie", c);
        }
        let response = req.send().await.expect("request");
        Self { status: response.status().as_u16(), headers: response.headers().clone(), response: Some(response), buf: String::new() }
    }

    /// A header's value.
    #[must_use]
    pub fn header(&self, name: &str) -> String {
        self.headers.get(name).and_then(|v| v.to_str().ok()).unwrap_or_default().to_string()
    }

    /// The next event `(name, data)` within `within`; `None` at a timeout or
    /// the end of the stream. Comments and the `retry` field are skipped.
    pub async fn next(&mut self, within: Duration) -> Option<(String, String)> {
        let until = tokio::time::Instant::now() + within;
        loop {
            while let Some(at) = self.buf.find("\n\n") {
                let block: String = self.buf[..at].to_string();
                self.buf.drain(..at + 2);
                let (mut name, mut data) = (None, Vec::new());
                for line in block.lines() {
                    if let Some(v) = line.strip_prefix("event:") {
                        name = Some(v.trim_start().to_string());
                    } else if let Some(v) = line.strip_prefix("data:") {
                        data.push(v.strip_prefix(' ').unwrap_or(v).to_string());
                    }
                }
                if name.is_some() || !data.is_empty() {
                    return Some((name.unwrap_or_else(|| "message".to_string()), data.join("\n")));
                }
            }
            let response = self.response.as_mut()?;
            match tokio::time::timeout_at(until, response.chunk()).await {
                Ok(Ok(Some(chunk))) => self.buf.push_str(&String::from_utf8_lossy(&chunk).replace("\r\n", "\n")),
                Ok(Ok(None) | Err(_)) => {
                    self.response = None;
                    return None;
                }
                Err(_) => return None,
            }
        }
    }

    /// Every event within `within`, until the stream ends.
    pub async fn collect(&mut self, within: Duration) -> Vec<(String, String)> {
        let until = tokio::time::Instant::now() + within;
        let mut out = Vec::new();
        loop {
            let left = until.saturating_duration_since(tokio::time::Instant::now());
            if left.is_zero() {
                return out;
            }
            match self.next(left).await {
                Some(e) => out.push(e),
                None => return out,
            }
        }
    }

    /// The next `end` event's reason within `within`.
    pub async fn end_reason(&mut self, within: Duration) -> Option<String> {
        let until = tokio::time::Instant::now() + within;
        loop {
            let left = until.saturating_duration_since(tokio::time::Instant::now());
            let (name, data) = self.next(left).await?;
            if name == "end" {
                let v: serde_json::Value = serde_json::from_str(&data).expect("json");
                return v["reason"].as_str().map(str::to_string);
            }
        }
    }

    /// Whether the stream has ended.
    #[must_use]
    pub fn ended(&self) -> bool {
        self.response.is_none()
    }
}
