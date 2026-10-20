//! The `proxy` state of a `[services.*]` block (design-v3 §6.7.4.2): a
//! request to one of the service's NSIDs is forwarded to the operator's
//! `proxy_url`, and the target's answer is relayed.
//!
//! A request runs these steps, in order, after the router layers:
//! 1. the `service_proxy` class limit, keyed by the key the backstop
//!    selected;
//! 2. authentication, if the request carries credentials, with the
//!    service's `aud` fragment allowlist. A failure is answered here and
//!    nothing is forwarded;
//! 3. the request body (POST), within `max_request_bytes` and
//!    `body_read_timeout_ms`. A slow sender holds only its own connection;
//! 4. upstream admission: the service's shared bucket and the requester's
//!    fair share, then a `max_in_flight` permit;
//! 5. the call to `<proxy_url>/xrpc/<nsid>?<query>`. The NSID is the
//!    route's, never parsed from the request, and only an allowlist of
//!    headers is copied;
//! 6. the relay of the target's status and JSON body.
//!
//! The handler validates no lexicon and builds nothing from this AppView's
//! own state: it reads no table and no cache. That is what makes returning
//! raw bytes safe here (§6.7.7.3). Bodies and query strings are never
//! logged.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use axum::Router;
use axum::body::Body;
use axum::extract::{ConnectInfo, Request, State};
use axum::http::header::{
    ACCEPT_LANGUAGE, AUTHORIZATION, CONTENT_ENCODING, CONTENT_LANGUAGE, CONTENT_LENGTH, CONTENT_TYPE,
    X_CONTENT_TYPE_OPTIONS,
};
use axum::http::{HeaderMap, HeaderName, HeaderValue, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use futures::StreamExt;
use tokio::sync::Semaphore;

use super::{ProxySettings, table};
use crate::config::{FileConfig, ServiceState};
use crate::honest_501::{self, Verb};
use crate::net::service_proxy::{Forward, ForwardError, ServiceProxyClient, ServiceProxyConfig, Upstream};
use crate::net::{NetError, user_agent};
use crate::xrpc::AppState;
use crate::xrpc::auth::{authenticate_for, request_handle, request_ip};
use crate::xrpc::errors::XrpcError;
use crate::xrpc::pools::{SharedBucket, Shares};

/// `service_proxy_requests_total{service, nsid, outcome}`.
pub const METRIC_REQUESTS: &str = "service_proxy_requests_total";
/// `service_proxy_duration_seconds{service}`: the upstream round trip.
pub const METRIC_DURATION: &str = "service_proxy_duration_seconds";
/// `service_state{service, state}` = 1, set once at startup.
pub const METRIC_STATE: &str = "service_state";
/// The `target_kind` of a service's shared bucket.
const TARGET_KIND: &str = "service";

static ACCEPT_LABELERS: HeaderName = HeaderName::from_static("atproto-accept-labelers");
static CONTENT_LABELERS: HeaderName = HeaderName::from_static("atproto-content-labelers");

/// How one proxied request ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Outcome {
    Ok2xx,
    Upstream4xx,
    Upstream5xx,
    AuthRejected,
    RateLimited,
    InFlightFull,
    TooLarge,
    BodyRead,
    BadContentType,
    Transport,
    Timeout,
}

impl Outcome {
    fn label(self) -> &'static str {
        match self {
            Self::Ok2xx => "ok_2xx",
            Self::Upstream4xx => "upstream_4xx",
            Self::Upstream5xx => "upstream_5xx",
            Self::AuthRejected => "auth_rejected",
            Self::RateLimited => "rate_limited",
            Self::InFlightFull => "in_flight_full",
            Self::TooLarge => "too_large",
            Self::BodyRead => "body_read",
            Self::BadContentType => "bad_content_type",
            Self::Transport => "transport",
            Self::Timeout => "timeout",
        }
    }
}

/// One service in `proxy` state.
struct Service {
    name: String,
    settings: ProxySettings,
    client: ServiceProxyClient,
    /// The target's request bucket, with pools and fair shares (§6.7.6.3).
    bucket: SharedBucket,
    /// `max_in_flight` permits (§6.7.6.4).
    in_flight: Semaphore,
}

#[derive(Clone)]
struct Route {
    app: AppState,
    service: Arc<Service>,
    /// The NSID this route was registered for.
    nsid: Arc<str>,
    post: bool,
}

/// Why a request body was not read.
enum BodyError {
    TooLarge,
    Timeout,
    Read,
}

/// Read a request body of at most `cap` bytes within `within`.
async fn read_body(body: Body, headers: &HeaderMap, cap: usize, within: Duration) -> Result<Vec<u8>, BodyError> {
    let declared = headers
        .get(CONTENT_LENGTH)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.parse::<u64>().ok());
    if declared.is_some_and(|len| len > cap as u64) {
        return Err(BodyError::TooLarge);
    }
    let read = async {
        let mut stream = body.into_data_stream();
        let mut out = Vec::new();
        while let Some(chunk) = stream.next().await {
            let chunk = chunk.map_err(|_| BodyError::Read)?;
            if out.len() + chunk.len() > cap {
                return Err(BodyError::TooLarge);
            }
            out.extend_from_slice(&chunk);
        }
        Ok(out)
    };
    tokio::time::timeout(within, read).await.unwrap_or(Err(BodyError::Timeout))
}

/// Copy every value of header `name`.
fn copy(from: &HeaderMap, to: &mut HeaderMap, name: &HeaderName) {
    for value in from.get_all(name) {
        to.append(name.clone(), value.clone());
    }
}

/// Whether a response's `Content-Type` is `application/json`, with any
/// parameters.
fn is_json(headers: &HeaderMap) -> bool {
    headers
        .get(CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.split(';').next())
        .is_some_and(|media| media.trim().eq_ignore_ascii_case("application/json"))
}

fn refuse(outcome: Outcome, status: StatusCode, error: &'static str, message: &'static str) -> (Outcome, Response) {
    (outcome, XrpcError::new(status, error, message).into_response())
}

/// The messages of this AppView's own answers never name the target or
/// quote anything it sent (§6.7.7.1).
fn upstream_failure(outcome: Outcome) -> (Outcome, Response) {
    refuse(outcome, StatusCode::BAD_GATEWAY, "UpstreamFailure", "The service could not answer this request")
}

/// §6.7.7: relay the target's answer.
fn relay(up: Upstream) -> (Outcome, Response) {
    let status = up.status;
    // A redirect is never followed, and never relayed.
    if status.is_redirection() || status.is_informational() {
        return upstream_failure(Outcome::Transport);
    }
    let outcome = match status.as_u16() {
        200..=299 => Outcome::Ok2xx,
        400..=499 => Outcome::Upstream4xx,
        _ => Outcome::Upstream5xx,
    };
    let mut response = if up.body.is_empty() {
        // An empty body is relayed with no content headers, whatever the
        // target sent with it.
        status.into_response()
    } else if is_json(&up.headers) {
        let mut response = Response::new(Body::from(up.body));
        *response.status_mut() = status;
        for name in [CONTENT_TYPE, CONTENT_ENCODING, CONTENT_LANGUAGE] {
            copy(&up.headers, response.headers_mut(), &name);
        }
        response
    } else {
        return upstream_failure(Outcome::BadContentType);
    };
    copy(&up.headers, response.headers_mut(), &CONTENT_LABELERS);
    (outcome, response)
}

impl Route {
    async fn run(&self, peer: SocketAddr, request: Request) -> (Outcome, Response) {
        let service = &*self.service;
        let (parts, body) = request.into_parts();
        let ip = request_ip(&self.app, &parts.headers, peer);
        // The backstop's key: a ready member's DID, or the IP key. A DID
        // verified in step 2 never selects a bucket, a share or a pool.
        let req = request_handle(None, ip);

        // 1. The class limit.
        if let Err(limited) = self.app.limits.charge_service(&service.name, req.key()) {
            return (Outcome::RateLimited, XrpcError::from(limited).into_response());
        }

        // 2. Credentials, if any, are verified here and never forwarded
        // unverified.
        let authorization = parts.headers.get(AUTHORIZATION);
        if let Some(value) = authorization {
            let raw = value.to_str().unwrap_or_default();
            let fragments = service.settings.aud_fragments.as_slice();
            if let Err(e) = authenticate_for(&self.app, Some(raw), &self.nsid, Some(fragments)).await {
                let outcome = if e.status == StatusCode::TOO_MANY_REQUESTS {
                    Outcome::RateLimited
                } else {
                    Outcome::AuthRejected
                };
                return (outcome, e.into_response());
            }
        }

        // 3. The body, complete, before anything of the service's is taken.
        let body = if self.post {
            let s = &service.settings;
            match read_body(body, &parts.headers, s.max_request_bytes, s.body_read_timeout).await {
                Ok(bytes) => Some(bytes),
                Err(BodyError::TooLarge) => {
                    return refuse(
                        Outcome::TooLarge,
                        StatusCode::PAYLOAD_TOO_LARGE,
                        "PayloadTooLarge",
                        "request body is too large",
                    );
                }
                Err(BodyError::Timeout) => {
                    return refuse(
                        Outcome::BodyRead,
                        StatusCode::BAD_REQUEST,
                        "InvalidRequest",
                        "request body not received in time",
                    );
                }
                Err(BodyError::Read) => {
                    return refuse(
                        Outcome::BodyRead,
                        StatusCode::BAD_REQUEST,
                        "InvalidRequest",
                        "request body could not be read",
                    );
                }
            }
        } else {
            None
        };

        // 4. Upstream admission: the share and the pool, then a permit.
        if let Err(exhausted) = req.try_draw(&service.bucket) {
            return (Outcome::RateLimited, XrpcError::from(exhausted).into_response());
        }
        let Ok(_permit) = service.in_flight.try_acquire() else {
            return refuse(
                Outcome::InFlightFull,
                StatusCode::SERVICE_UNAVAILABLE,
                "NotEnoughResources",
                "The service is busy; try again shortly",
            );
        };

        // 5. Forward, with the header allowlist (§6.7.5.5).
        let mut headers = HeaderMap::new();
        if let Some(value) = authorization {
            headers.insert(AUTHORIZATION, value.clone());
        }
        if self.post {
            copy(&parts.headers, &mut headers, &CONTENT_TYPE);
            copy(&parts.headers, &mut headers, &CONTENT_ENCODING);
        }
        copy(&parts.headers, &mut headers, &ACCEPT_LANGUAGE);
        copy(&parts.headers, &mut headers, &ACCEPT_LABELERS);
        let started = Instant::now();
        let answer = service
            .client
            .forward(Forward {
                post: self.post,
                nsid: &self.nsid,
                raw_query: parts.uri.query(),
                headers,
                body,
                timeout: service.settings.timeout,
                max_response_bytes: service.settings.max_response_bytes,
            })
            .await;
        metrics::histogram!(METRIC_DURATION, "service" => service.name.clone()).record(started.elapsed().as_secs_f64());

        // 6. Relay.
        match answer {
            Ok(up) => relay(up),
            Err(ForwardError::Timeout) => refuse(
                Outcome::Timeout,
                StatusCode::GATEWAY_TIMEOUT,
                "UpstreamTimeout",
                "The service did not answer in time",
            ),
            Err(ForwardError::Transport) => upstream_failure(Outcome::Transport),
            Err(ForwardError::TooLarge) => upstream_failure(Outcome::TooLarge),
        }
    }
}

async fn handle(State(route): State<Route>, ConnectInfo(peer): ConnectInfo<SocketAddr>, request: Request) -> Response {
    let started = Instant::now();
    let (outcome, mut response) = route.run(peer, request).await;
    response
        .headers_mut()
        .insert(X_CONTENT_TYPE_OPTIONS, HeaderValue::from_static("nosniff"));
    metrics::counter!(
        METRIC_REQUESTS,
        "service" => route.service.name.clone(),
        "nsid" => route.nsid.to_string(),
        "outcome" => outcome.label(),
    )
    .increment(1);
    tracing::debug!(
        event = "service_proxy",
        service = %route.service.name,
        nsid = %route.nsid,
        status = response.status().as_u16(),
        duration_ms = u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX),
        outcome = outcome.label(),
        "proxied request",
    );
    response
}

/// The proxy routes of every service in `proxy` state, each for its verb.
/// A pinned NSID keeps its honest-501 route (§6.7.2.2) and gets none here.
/// Merged into the app before `xrpc::router::finalize`; merging panics if
/// two routes claim one NSID, which validation makes impossible.
///
/// Call it on a validated config. `public_hostname` is
/// `APP_PUBLIC_HOSTNAME`.
///
/// # Errors
///
/// [`NetError`] when a service's client cannot be built.
pub fn router(app: &AppState, cfg: &FileConfig, shares: &Shares, public_hostname: &str) -> Result<Router, NetError> {
    let mut router = Router::new();
    for entry in table(cfg) {
        metrics::gauge!(METRIC_STATE, "service" => entry.name.clone(), "state" => entry.state.as_str()).set(1.0);
        if entry.state != ServiceState::Proxy {
            continue;
        }
        let Some(settings) = cfg.services.get(&entry.name).and_then(|s| ProxySettings::of(&entry.name, s)) else {
            continue;
        };
        let client = ServiceProxyClient::new(&ServiceProxyConfig {
            proxy_url: settings.proxy_url.clone(),
            allow_private_network: settings.allow_private_network,
            user_agent: user_agent(public_hostname),
        })?;
        let service = Arc::new(Service {
            bucket: shares.bucket(TARGET_KIND, settings.rate_per_sec, settings.burst),
            in_flight: Semaphore::new(settings.max_in_flight),
            client,
            settings,
            name: entry.name,
        });
        for (nsid, verb) in entry.methods {
            if honest_501::find(&nsid).is_some_and(|d| d.pinned) {
                continue;
            }
            let state = Route {
                app: app.clone(),
                service: Arc::clone(&service),
                nsid: Arc::from(nsid.as_str()),
                post: verb == Verb::Post,
            };
            let method = match verb {
                Verb::Get => get(handle),
                Verb::Post => post(handle),
            };
            router = router.route(&crate::admin::usage::path("services", &nsid), method.with_state(state));
        }
    }
    Ok(router)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn upstream(status: u16, headers: &[(&'static str, &'static str)], body: &[u8]) -> Upstream {
        let mut map = HeaderMap::new();
        for (name, value) in headers {
            map.append(HeaderName::from_static(name), HeaderValue::from_static(value));
        }
        Upstream {
            status: StatusCode::from_u16(status).expect("status"),
            headers: map,
            body: body.to_vec(),
        }
    }

    async fn bytes(response: Response) -> Vec<u8> {
        axum::body::to_bytes(response.into_body(), 1 << 20).await.expect("body").to_vec()
    }

    #[tokio::test]
    async fn json_answers_are_relayed_byte_for_byte() {
        let body = br#"{"matches": [ ]}"#;
        let headers = [
            ("content-type", "application/json; charset=utf-8"),
            ("content-language", "de"),
            ("atproto-content-labelers", "did:plc:labeler;redact"),
            ("ratelimit-reset", "99"),
            ("set-cookie", "a=b"),
            ("x-target", "1"),
        ];
        for (status, outcome) in [(200, Outcome::Ok2xx), (401, Outcome::Upstream4xx), (429, Outcome::Upstream4xx), (500, Outcome::Upstream5xx)] {
            let (got, response) = relay(upstream(status, &headers, body));
            assert_eq!((got, response.status().as_u16()), (outcome, status));
            let h = response.headers().clone();
            assert_eq!(h[CONTENT_TYPE], "application/json; charset=utf-8");
            assert_eq!(h[CONTENT_LANGUAGE], "de");
            assert_eq!(h["atproto-content-labelers"], "did:plc:labeler;redact");
            // Nothing else of the target's is relayed.
            assert_eq!(h.len(), 3, "{h:?}");
            assert_eq!(bytes(response).await, body);
        }
        assert!(is_json(&upstream(200, &[("content-type", "Application/JSON")], b"").headers));
        assert!(!is_json(&upstream(200, &[("content-type", "application/json-seq")], b"").headers));
    }

    #[tokio::test]
    async fn an_empty_body_is_relayed_with_no_content_headers() {
        let headers = [
            ("content-type", "text/html"),
            ("content-encoding", "gzip"),
            ("content-language", "de"),
            ("atproto-content-labelers", "did:plc:labeler"),
        ];
        for status in [200, 204, 404, 503] {
            let (_, response) = relay(upstream(status, &headers, b""));
            assert_eq!(response.status().as_u16(), status);
            let h = response.headers().clone();
            assert!(h.get(CONTENT_TYPE).is_none() && h.get(CONTENT_ENCODING).is_none() && h.get(CONTENT_LANGUAGE).is_none());
            assert_eq!(h["atproto-content-labelers"], "did:plc:labeler");
            assert!(bytes(response).await.is_empty());
        }
    }

    #[tokio::test]
    async fn other_answers_are_a_502_that_names_nothing() {
        let cases = [
            (upstream(200, &[("content-type", "text/html")], b"<html>target.internal</html>"), Outcome::BadContentType),
            (upstream(200, &[], b"{}"), Outcome::BadContentType),
            (upstream(302, &[("location", "https://elsewhere.example"), ("content-type", "application/json")], b"{}"), Outcome::Transport),
            (upstream(301, &[], b""), Outcome::Transport),
        ];
        for (up, outcome) in cases {
            let (got, response) = relay(up);
            assert_eq!((got, response.status().as_u16()), (outcome, 502));
            assert!(response.headers().get("location").is_none());
            let v: serde_json::Value = serde_json::from_slice(&bytes(response).await).expect("json");
            assert_eq!(v["error"], "UpstreamFailure");
            assert!(!v["message"].as_str().expect("message").contains("target"));
        }
    }

    #[tokio::test]
    async fn request_bodies_are_capped_and_timed() {
        let none = HeaderMap::new();
        let ok = read_body(Body::from(vec![7u8; 100]), &none, 100, Duration::from_secs(1)).await;
        assert_eq!(ok.ok().map(|b| b.len()), Some(100));
        assert!(matches!(
            read_body(Body::from(vec![7u8; 101]), &none, 100, Duration::from_secs(1)).await,
            Err(BodyError::TooLarge)
        ));
        // A declared length over the cap is refused before any read.
        let mut declared = HeaderMap::new();
        declared.insert(CONTENT_LENGTH, HeaderValue::from_static("101"));
        assert!(matches!(
            read_body(Body::empty(), &declared, 100, Duration::from_secs(1)).await,
            Err(BodyError::TooLarge)
        ));
        // A sender that never finishes.
        let stalled = Body::from_stream(futures::stream::pending::<Result<Vec<u8>, std::io::Error>>());
        assert!(matches!(
            read_body(stalled, &none, 100, Duration::from_millis(50)).await,
            Err(BodyError::Timeout)
        ));
        let broken = Body::from_stream(futures::stream::iter([Err::<Vec<u8>, _>(std::io::Error::other("reset"))]));
        assert!(matches!(read_body(broken, &none, 100, Duration::from_secs(1)).await, Err(BodyError::Read)));
    }
}
