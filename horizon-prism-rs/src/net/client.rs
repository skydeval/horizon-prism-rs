//! The outbound HTTP client (design-v2 §8.5; design-v4 §II-8.5).
//!
//! Every outbound call (AppViews, PDSes, PLC, did:web, Slingshot, Constellation)
//! goes through a [`NetClient`] of one [`Intent`] (design-v4 §11 G-9; built
//! as a set by [`super::Transports`]):
//! - `https` only, except the configured `identity.plc_url` origin when it is
//!   plain `http` (a test PLC);
//! - DNS answers pass the SSRF guard, and IP-literal hosts are checked directly;
//! - at most 3 redirects, each re-validated by scheme and address;
//! - proxies from the environment are ignored (`no_proxy`);
//! - bodies are read up to a per-call cap;
//! - no inbound header is ever forwarded. Callers pass only the headers they
//!   own (the service-JWT `Authorization` in `appview_clients`).
//!
//! The one exception to the last rule is not this client: a proxied
//! `[services.*]` call goes through [`super::service_proxy`], which forwards
//! an allowlist of the request's headers (design-v3 §6.7.5.6).

use std::sync::Arc;
use std::time::Duration;

use reqwest::header::{CONTENT_TYPE, HeaderMap, HeaderName, HeaderValue};
use reqwest::redirect;
use reqwest::{Method, StatusCode};
use serde::Serialize;
use serde::de::DeserializeOwned;
use url::{Host, Url};

use super::resolver::{AllAddressesBlocked, GuardedResolver, HostLookup, is_blocked};
use crate::config::AppConfig;

/// Redirects followed per call (§8.5).
pub const MAX_REDIRECTS: usize = 3;
/// Connect timeout for every call (§8.3).
pub const CONNECT_TIMEOUT: Duration = Duration::from_secs(1);
/// Default total timeout per call (§8.3).
pub const DEFAULT_TIMEOUT: Duration = Duration::from_secs(3);

/// Whose work an outbound call does (design-v4 §11 G-9 "Transport"; R3-168,
/// R3-213). Each intent has its own client, connection pool and DNS
/// resolver, so one intent's work leaves no in-process state another can
/// time. The fourth intent, `service_proxy`, is a type of its own
/// ([`super::service_proxy::ServiceProxyClient`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Intent {
    /// Public request handlers: actor-parameter resolution, `resolveHandle`,
    /// cascade calls, PDS fallbacks, the auth middleware's issuer lookups
    /// and the background profile refresh.
    Public,
    /// Background work and the private tier: backfill, `authz`, identity
    /// refresh, `list_sync`, the private handlers and pipeline, the prober.
    Internal,
    /// Viewer-private fetches (§8.1) to hosts the subject names: handle DNS
    /// and well-known files, `did:web` documents, subject PDSes. It keeps no
    /// reusable state: no DNS cache, no idle connection, no TLS session.
    ViewerPrivate,
}

impl Intent {
    /// The intent's name, as the design writes it.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Public => "public",
            Self::Internal => "internal",
            Self::ViewerPrivate => "viewer_private",
        }
    }
}

/// A response-body size cap (§8.5).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BodyCap(pub usize);

impl BodyCap {
    /// AppView bodies.
    pub const APPVIEW: Self = Self(4 * 1024 * 1024);
    /// PDS `listRecords` pages.
    pub const LIST_RECORDS: Self = Self(4 * 1024 * 1024);
    /// DID documents.
    pub const DID_DOCUMENT: Self = Self(1024 * 1024);
    /// Slingshot records.
    pub const SLINGSHOT: Self = Self(1024 * 1024);
    /// Constellation responses.
    pub const CONSTELLATION: Self = Self(1024 * 1024);
    /// `getRepoStatus` and `getLatestCommit`.
    pub const REPO_STATUS: Self = Self(1024 * 1024);
    /// PDS `getRecord`.
    pub const GET_RECORD: Self = Self(256 * 1024);

    #[must_use]
    pub fn bytes(self) -> usize {
        self.0
    }
}

/// Settings for [`NetClient`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NetConfig {
    /// `horizon-prism-rs/<version> (+https://<host>)`.
    pub user_agent: String,
    /// Origins (`scheme://host[:port]`) allowed over plain `http`.
    pub http_allowed_origins: Vec<String>,
}

impl NetConfig {
    /// Settings derived from the app configuration.
    #[must_use]
    pub fn from_app(cfg: &AppConfig) -> Self {
        Self::new(&cfg.env.public_hostname, &cfg.file.identity.plc_url)
    }

    /// Settings for `public_hostname`, allowing `plc_url` over `http` if it
    /// uses that scheme.
    #[must_use]
    pub fn new(public_hostname: &str, plc_url: &str) -> Self {
        let http_allowed_origins = Url::parse(plc_url)
            .ok()
            .filter(|u| u.scheme() == "http")
            .map(|u| u.origin().ascii_serialization())
            .into_iter()
            .collect();
        Self {
            user_agent: user_agent(public_hostname),
            http_allowed_origins,
        }
    }
}

/// The §8.5 User-Agent.
#[must_use]
pub fn user_agent(public_hostname: &str) -> String {
    format!(
        "horizon-prism-rs/{} (+https://{public_hostname})",
        env!("CARGO_PKG_VERSION")
    )
}

/// The `User-Agent` of [`NetClient::loopback_for_tests_as`]'s client of
/// `intent`, which a fake upstream records to tell which client called.
#[cfg(test)]
#[must_use]
pub(crate) fn test_user_agent(intent: Intent) -> String {
    format!("prism-test/{}", intent.as_str())
}

/// Outbound call failures.
#[derive(Debug, thiserror::Error)]
pub enum NetError {
    #[error("invalid URL {url:?}: {reason}")]
    InvalidUrl { url: String, reason: String },
    #[error("scheme not allowed for {url:?}: only https (and the configured PLC origin over http)")]
    SchemeNotAllowed { url: String },
    #[error("address {host} is in a blocked range")]
    BlockedAddress { host: String },
    #[error("request timed out")]
    Timeout,
    #[error("connection failed: {0}")]
    Connect(#[source] reqwest::Error),
    #[error("redirect rejected: {0}")]
    Redirect(#[source] reqwest::Error),
    #[error("response body exceeds the {cap} byte cap")]
    BodyTooLarge { cap: usize },
    #[error("transport error: {0}")]
    Transport(#[source] reqwest::Error),
    #[error("response body is not the expected JSON: {0}")]
    Decode(#[from] serde_json::Error),
    #[error("request body could not be encoded: {0}")]
    Encode(String),
    #[error("could not build the HTTP client: {0}")]
    Build(String),
}

impl NetError {
    /// §8.3 `transient` classification at the transport level: timeouts,
    /// connection failures and guard refusals (no address survived). Status
    /// codes are classified by the caller from [`NetResponse::status`].
    #[must_use]
    pub fn is_transient(&self) -> bool {
        matches!(
            self,
            Self::Timeout | Self::Connect(_) | Self::BlockedAddress { .. } | Self::Transport(_)
        )
    }

    fn from_reqwest(e: reqwest::Error) -> Self {
        // A hostname whose every address the resolver guard refused fails
        // inside reqwest as a connect error. The guard's typed error is in
        // its source chain: the same refusal as an IP literal's.
        if let Some(blocked) = guard_refusal(&e) {
            return Self::BlockedAddress { host: blocked.host.clone() };
        }
        if e.is_timeout() {
            Self::Timeout
        } else if e.is_redirect() {
            Self::Redirect(e)
        } else if e.is_connect() {
            Self::Connect(e)
        } else {
            Self::Transport(e)
        }
    }
}

/// The resolver guard's refusal, if it is what failed `error`.
fn guard_refusal(error: &reqwest::Error) -> Option<&AllAddressesBlocked> {
    let mut cause: Option<&(dyn std::error::Error + 'static)> = Some(error);
    while let Some(e) = cause {
        if let Some(blocked) = e.downcast_ref::<AllAddressesBlocked>() {
            return Some(blocked);
        }
        // An `io::Error` can carry its cause inside, off the chain.
        if let Some(blocked) = e
            .downcast_ref::<std::io::Error>()
            .and_then(std::io::Error::get_ref)
            .and_then(|inner| inner.downcast_ref::<AllAddressesBlocked>())
        {
            return Some(blocked);
        }
        cause = e.source();
    }
    None
}

/// A buffered response. Non-2xx statuses are not errors at this layer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NetResponse {
    pub status: StatusCode,
    pub content_type: Option<String>,
    /// The URL after redirects.
    pub final_url: String,
    pub body: Vec<u8>,
}

impl NetResponse {
    #[must_use]
    pub fn is_success(&self) -> bool {
        self.status.is_success()
    }

    /// Decode the body as JSON.
    ///
    /// # Errors
    ///
    /// [`NetError::Decode`] when the body is not valid JSON for `T`.
    pub fn json<T: DeserializeOwned>(&self) -> Result<T, NetError> {
        Ok(serde_json::from_slice(&self.body)?)
    }
}

/// Per-call options.
#[derive(Debug, Clone)]
pub struct RequestOptions {
    /// Total time for the call, including the body.
    pub timeout: Duration,
    pub body_cap: BodyCap,
    /// Headers the caller owns. Never copied from an inbound request.
    pub headers: Vec<(HeaderName, HeaderValue)>,
}

impl RequestOptions {
    /// Default timeout with the given cap and no extra headers.
    #[must_use]
    pub fn new(body_cap: BodyCap) -> Self {
        Self {
            timeout: DEFAULT_TIMEOUT,
            body_cap,
            headers: Vec::new(),
        }
    }

    #[must_use]
    pub fn timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    #[must_use]
    pub fn header(mut self, name: HeaderName, value: HeaderValue) -> Self {
        self.headers.push((name, value));
        self
    }
}

/// Scheme and literal-address policy shared by requests and redirects.
#[derive(Debug, Clone)]
struct UrlPolicy {
    http_allowed_origins: Arc<Vec<String>>,
    /// Test-only: accept `http` and loopback so fake upstreams can be served
    /// on 127.0.0.1. Never set outside `#[cfg(test)]`.
    allow_loopback: bool,
}

impl UrlPolicy {
    fn check(&self, url: &Url) -> Result<(), NetError> {
        if self.allow_loopback {
            return Ok(());
        }
        match url.scheme() {
            "https" => {}
            "http"
                if self
                    .http_allowed_origins
                    .contains(&url.origin().ascii_serialization()) => {}
            _ => {
                return Err(NetError::SchemeNotAllowed {
                    url: url.to_string(),
                });
            }
        }
        let literal = match url.host() {
            None => {
                return Err(NetError::InvalidUrl {
                    url: url.to_string(),
                    reason: "no host".to_string(),
                });
            }
            Some(Host::Domain(_)) => None,
            Some(Host::Ipv4(v4)) => Some(std::net::IpAddr::V4(v4)),
            Some(Host::Ipv6(v6)) => Some(std::net::IpAddr::V6(v6)),
        };
        if let Some(ip) = literal.filter(|ip| is_blocked(*ip)) {
            return Err(NetError::BlockedAddress {
                host: ip.to_string(),
            });
        }
        if !url.username().is_empty() || url.password().is_some() {
            return Err(NetError::InvalidUrl {
                url: url.to_string(),
                reason: "credentials in URL".to_string(),
            });
        }
        Ok(())
    }
}

/// The SSRF-guarded HTTP client of one [`Intent`]. Cheap to clone; clones
/// share the connection pool.
#[derive(Debug, Clone)]
pub struct NetClient {
    http: reqwest::Client,
    policy: UrlPolicy,
    intent: Intent,
}

/// TLS for the `viewer_private` intent (design-v4 §8.1, R3-213): reqwest's
/// own settings (ring, the webpki roots, HTTP/1.1), with session resumption
/// disabled, so no ticket or session ID outlives its connection.
fn no_resumption_tls() -> Result<rustls::ClientConfig, NetError> {
    let mut roots = rustls::RootCertStore::empty();
    roots.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
    let mut tls = rustls::ClientConfig::builder_with_provider(Arc::new(rustls::crypto::ring::default_provider()))
        .with_safe_default_protocol_versions()
        .map_err(|e| NetError::Build(e.to_string()))?
        .with_root_certificates(roots)
        .with_no_client_auth();
    tls.resumption = rustls::client::Resumption::disabled();
    tls.alpn_protocols = vec![b"http/1.1".to_vec()];
    Ok(tls)
}

impl NetClient {
    /// Build the client of `intent` over a specific DNS backend. The
    /// `viewer_private` client keeps no idle connection and resumes no TLS
    /// session (design-v4 §8.1); its lookup must keep no answers either
    /// ([`super::Transports`] builds it so).
    ///
    /// # Errors
    ///
    /// [`NetError::Build`] when TLS setup fails.
    pub fn with_lookup<L: HostLookup>(config: &NetConfig, intent: Intent, lookup: L) -> Result<Self, NetError> {
        let policy = UrlPolicy {
            http_allowed_origins: Arc::new(config.http_allowed_origins.clone()),
            allow_loopback: false,
        };
        let redirect_policy = {
            let policy = policy.clone();
            redirect::Policy::custom(move |attempt| {
                if attempt.previous().len() > MAX_REDIRECTS {
                    return attempt.error(format!("more than {MAX_REDIRECTS} redirects"));
                }
                match policy.check(attempt.url()) {
                    Ok(()) => attempt.follow(),
                    Err(e) => {
                        let reason = e.to_string();
                        attempt.error(reason)
                    }
                }
            })
        };
        let builder = reqwest::Client::builder();
        let builder = match intent {
            Intent::ViewerPrivate => builder.use_preconfigured_tls(no_resumption_tls()?).pool_max_idle_per_host(0),
            Intent::Public | Intent::Internal => builder.use_rustls_tls(),
        };
        let http = builder
            .no_proxy()
            .dns_resolver(Arc::new(GuardedResolver::new(lookup)))
            .redirect(redirect_policy)
            .connect_timeout(CONNECT_TIMEOUT)
            .user_agent(config.user_agent.clone())
            .build()
            .map_err(|e| NetError::Build(e.to_string()))?;
        Ok(Self { http, policy, intent })
    }

    /// The intent this client serves.
    #[must_use]
    pub fn intent(&self) -> Intent {
        self.intent
    }

    /// Test-only client for fake upstreams on `http://127.0.0.1:<port>`: no
    /// scheme or address guard, but the same timeouts and body caps. It is
    /// a `public` client.
    #[cfg(test)]
    pub(crate) fn loopback_for_tests() -> Self {
        let http = reqwest::Client::builder()
            .no_proxy()
            .connect_timeout(CONNECT_TIMEOUT)
            .build()
            .expect("test client");
        Self {
            http,
            policy: UrlPolicy {
                http_allowed_origins: Arc::new(Vec::new()),
                allow_loopback: true,
            },
            intent: Intent::Public,
        }
    }

    /// Test-only loopback client of `intent`, telling its calls apart by
    /// its `User-Agent` ([`test_user_agent`]). Every hostname resolves to
    /// 127.0.0.1, so a subject-named host is reached (and refused) locally.
    #[cfg(test)]
    pub(crate) fn loopback_for_tests_as(intent: Intent) -> Self {
        struct Loopback;
        impl reqwest::dns::Resolve for Loopback {
            fn resolve(&self, _name: reqwest::dns::Name) -> reqwest::dns::Resolving {
                Box::pin(async {
                    let addrs: reqwest::dns::Addrs = Box::new(std::iter::once(std::net::SocketAddr::from(([127, 0, 0, 1], 0))));
                    Ok::<_, super::resolver::BoxError>(addrs)
                })
            }
        }
        let mut builder = reqwest::Client::builder()
            .no_proxy()
            .dns_resolver(Arc::new(Loopback))
            .connect_timeout(CONNECT_TIMEOUT)
            .user_agent(test_user_agent(intent));
        if intent == Intent::ViewerPrivate {
            builder = builder.pool_max_idle_per_host(0);
        }
        Self {
            http: builder.build().expect("test client"),
            policy: UrlPolicy {
                http_allowed_origins: Arc::new(Vec::new()),
                allow_loopback: true,
            },
            intent,
        }
    }

    /// Parse and check a URL against the scheme and literal-address policy.
    ///
    /// # Errors
    ///
    /// [`NetError::InvalidUrl`], [`NetError::SchemeNotAllowed`] or
    /// [`NetError::BlockedAddress`].
    pub fn check_url(&self, url: &str) -> Result<Url, NetError> {
        let parsed = Url::parse(url).map_err(|e| NetError::InvalidUrl {
            url: url.to_string(),
            reason: e.to_string(),
        })?;
        self.policy.check(&parsed)?;
        Ok(parsed)
    }

    /// `GET url`, returning the capped body whatever the status.
    ///
    /// # Errors
    ///
    /// Any [`NetError`] other than [`NetError::Decode`].
    pub async fn get(&self, url: &str, options: &RequestOptions) -> Result<NetResponse, NetError> {
        self.send(Method::GET, url, None, options).await
    }

    /// `GET url` and decode a 2xx JSON body. Non-2xx responses are returned as
    /// `Ok(Err(response))` so the caller can classify them (§8.3).
    ///
    /// # Errors
    ///
    /// Any [`NetError`].
    pub async fn get_json<T: DeserializeOwned>(
        &self,
        url: &str,
        options: &RequestOptions,
    ) -> Result<Result<T, NetResponse>, NetError> {
        let response = self.get(url, options).await?;
        if response.is_success() {
            Ok(Ok(response.json()?))
        } else {
            Ok(Err(response))
        }
    }

    /// `POST url` with a JSON body, returning the capped body whatever the status.
    ///
    /// # Errors
    ///
    /// Any [`NetError`] other than [`NetError::Decode`].
    pub async fn post_json<B: Serialize + ?Sized>(
        &self,
        url: &str,
        body: &B,
        options: &RequestOptions,
    ) -> Result<NetResponse, NetError> {
        let bytes = serde_json::to_vec(body).map_err(|e| NetError::Encode(e.to_string()))?;
        self.send(Method::POST, url, Some(bytes), options).await
    }

    async fn send(
        &self,
        method: Method,
        url: &str,
        json_body: Option<Vec<u8>>,
        options: &RequestOptions,
    ) -> Result<NetResponse, NetError> {
        let parsed = self.check_url(url)?;
        let mut headers = HeaderMap::new();
        for (name, value) in &options.headers {
            headers.insert(name.clone(), value.clone());
        }
        let mut request = self
            .http
            .request(method, parsed)
            .timeout(options.timeout)
            .headers(headers);
        if let Some(bytes) = json_body {
            request = request
                .header(CONTENT_TYPE, HeaderValue::from_static("application/json"))
                .body(bytes);
        }
        let response = request.send().await.map_err(NetError::from_reqwest)?;
        read_capped(response, options.body_cap).await
    }
}

/// Buffer a response body, failing once it exceeds `cap`.
async fn read_capped(mut response: reqwest::Response, cap: BodyCap) -> Result<NetResponse, NetError> {
    let limit = cap.bytes();
    if response
        .content_length()
        .is_some_and(|len| len > limit as u64)
    {
        return Err(NetError::BodyTooLarge { cap: limit });
    }
    let status = response.status();
    let final_url = response.url().to_string();
    let content_type = response
        .headers()
        .get(CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .map(str::to_string);
    let mut body = Vec::new();
    while let Some(chunk) = response.chunk().await.map_err(NetError::from_reqwest)? {
        if body.len() + chunk.len() > limit {
            return Err(NetError::BodyTooLarge { cap: limit });
        }
        body.extend_from_slice(&chunk);
    }
    Ok(NetResponse {
        status,
        content_type,
        final_url,
        body,
    })
}

#[cfg(test)]
mod tests {
    use std::net::IpAddr;

    use futures::future::BoxFuture;

    use super::super::resolver::BoxError;
    use super::*;

    struct NoDns;

    impl HostLookup for NoDns {
        fn lookup(&self, host: String) -> BoxFuture<'static, Result<Vec<IpAddr>, BoxError>> {
            Box::pin(async move { Err(BoxError::from(format!("no DNS in tests: {host}"))) })
        }
    }

    fn client(plc_url: &str) -> NetClient {
        NetClient::with_lookup(&NetConfig::new("appview.example.com", plc_url), Intent::Public, NoDns)
            .expect("client builds")
    }

    /// Every intent builds, the `viewer_private` one with its own TLS
    /// configuration, and each says which it is.
    #[test]
    fn every_intent_builds_and_names_itself() {
        let config = NetConfig::new("appview.example.com", "https://plc.directory");
        for intent in [Intent::Public, Intent::Internal, Intent::ViewerPrivate] {
            let c = NetClient::with_lookup(&config, intent, NoDns).expect("client builds");
            assert_eq!(c.intent(), intent);
        }
        let tls = no_resumption_tls().expect("tls");
        let resumption = format!("{:?}", tls.resumption);
        assert!(resumption.contains("NoClientSessionStorage") && resumption.contains("Disabled"), "{resumption}");
        assert_eq!(tls.alpn_protocols, [b"http/1.1".to_vec()]);
        assert_eq!(
            [Intent::Public, Intent::Internal, Intent::ViewerPrivate].map(Intent::as_str),
            ["public", "internal", "viewer_private"]
        );
    }

    #[test]
    fn user_agent_names_version_and_host() {
        let ua = user_agent("appview.example.com");
        assert!(ua.starts_with("horizon-prism-rs/"));
        assert!(ua.ends_with(" (+https://appview.example.com)"));
    }

    #[test]
    fn https_is_allowed() {
        client("https://plc.directory")
            .check_url("https://bsky.social/xrpc/x")
            .expect("allowed");
    }

    #[test]
    fn plain_http_is_rejected_by_default() {
        let c = client("https://plc.directory");
        assert!(matches!(
            c.check_url("http://plc.directory/did:plc:abc"),
            Err(NetError::SchemeNotAllowed { .. })
        ));
        for url in ["ftp://example.com/x", "file:///etc/passwd", "gopher://example.com"] {
            assert!(
                matches!(c.check_url(url), Err(NetError::SchemeNotAllowed { .. })),
                "{url}"
            );
        }
    }

    #[test]
    fn http_is_allowed_only_for_the_configured_plc_origin() {
        let c = client("http://plc.test.internal:2582");
        c.check_url("http://plc.test.internal:2582/did:plc:abc")
            .expect("test PLC over http");
        assert!(c.check_url("http://plc.test.internal:2583/x").is_err());
        assert!(c.check_url("http://other.internal:2582/x").is_err());
    }

    #[test]
    fn blocked_ip_literals_are_rejected() {
        let c = client("https://plc.directory");
        for url in [
            "https://127.0.0.1/x",
            "https://10.1.2.3/x",
            "https://169.254.169.254/latest/meta-data",
            "https://[::1]/x",
            "https://[::ffff:127.0.0.1]/x",
            "https://[fd00::1]/x",
            "https://0.0.0.0/x",
        ] {
            assert!(
                matches!(c.check_url(url), Err(NetError::BlockedAddress { .. })),
                "{url}"
            );
        }
        c.check_url("https://93.184.216.34/x").expect("public literal");
    }

    #[test]
    fn credentials_in_urls_are_rejected() {
        let c = client("https://plc.directory");
        assert!(matches!(
            c.check_url("https://user:pw@example.com/"),
            Err(NetError::InvalidUrl { .. })
        ));
    }

    #[test]
    fn body_caps_match_design() {
        assert_eq!(BodyCap::APPVIEW.bytes(), 4 * 1024 * 1024);
        assert_eq!(BodyCap::LIST_RECORDS.bytes(), 4 * 1024 * 1024);
        assert_eq!(BodyCap::DID_DOCUMENT.bytes(), 1024 * 1024);
        assert_eq!(BodyCap::SLINGSHOT.bytes(), 1024 * 1024);
        assert_eq!(BodyCap::CONSTELLATION.bytes(), 1024 * 1024);
        assert_eq!(BodyCap::REPO_STATUS.bytes(), 1024 * 1024);
        assert_eq!(BodyCap::GET_RECORD.bytes(), 256 * 1024);
    }

    #[test]
    fn transient_classification() {
        assert!(NetError::Timeout.is_transient());
        assert!(
            NetError::BlockedAddress {
                host: "x".into()
            }
            .is_transient()
        );
        assert!(!NetError::BodyTooLarge { cap: 1 }.is_transient());
        assert!(
            !NetError::SchemeNotAllowed {
                url: "http://x".into()
            }
            .is_transient()
        );
    }

    #[tokio::test]
    async fn blocked_literal_never_reaches_the_network() {
        let c = client("https://plc.directory");
        let err = c
            .get("https://127.0.0.1:9/x", &RequestOptions::new(BodyCap::GET_RECORD))
            .await
            .expect_err("blocked");
        assert!(matches!(err, NetError::BlockedAddress { .. }));
    }

    /// Every name resolves to a private address.
    struct PrivateDns;

    impl HostLookup for PrivateDns {
        fn lookup(&self, _host: String) -> BoxFuture<'static, Result<Vec<IpAddr>, BoxError>> {
            Box::pin(async { Ok(vec![IpAddr::from([10, 0, 0, 5])]) })
        }
    }

    /// A hostname the resolver guard refuses is the same refusal as a
    /// blocked literal, not a connect error (B-81).
    #[tokio::test]
    async fn a_hostname_the_guard_refuses_is_a_blocked_address() {
        let c = NetClient::with_lookup(&NetConfig::new("appview.example.com", "https://plc.directory"), Intent::Public, PrivateDns)
            .expect("client builds");
        let err = c
            .get("https://internal.example/x", &RequestOptions::new(BodyCap::GET_RECORD))
            .await
            .expect_err("refused");
        assert!(
            matches!(&err, NetError::BlockedAddress { host } if host == "internal.example"),
            "{err:?}"
        );
        assert!(err.is_transient());
    }
}
