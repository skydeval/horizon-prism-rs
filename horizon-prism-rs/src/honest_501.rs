//! Honest-501 routes (design-v3 §6.1, §6.2, §6.6): every deferred endpoint
//! answers `501 NotImplemented` from a registered route, never from the
//! router fallback.
//!
//! The deferral set is compiled in. Operators can change a message and the
//! status link, never the set: that keeps each route's verb known and keeps
//! an operator key from overlapping a core handler.

use std::net::SocketAddr;
use std::sync::Arc;

use axum::Json;
use axum::Router;
use axum::extract::{ConnectInfo, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use serde_json::json;

use crate::config::{FileConfig, ServiceState};
use crate::xrpc::AppState;
use crate::xrpc::auth::request_ip;
use crate::xrpc::errors::XrpcError;
use crate::xrpc::rate_limit::Class;

/// `honest_501_total{nsid}`: demand for each deferral.
pub const METRIC: &str = "honest_501_total";
/// The placeholder for `honest_501.status_url` in a message.
pub const LINK: &str = "<link>";

pub const DEFAULT_MESSAGE: &str = "This endpoint is planned for a future release. See <link> for status.";
const PUSH: &str = "Push notifications are not available from this AppView yet. See <link> for status.";
const PREFERENCES: &str = "Preferences are stored by your PDS. Call this method on your PDS, not on this AppView.";
const REPORT: &str = "This AppView does not accept reports. Send com.atproto.moderation.createReport to your PDS with an atproto-proxy header naming a labeler service (for example `atproto-proxy: <labeler did>#atproto_labeler`).";
const VIDEO: &str = "Video upload and processing are served by a video service, not by this AppView.";
const AGE_ASSURANCE: &str = "Age assurance is not provided by this AppView.";

/// The lexicon verb of a route.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Verb {
    Get,
    Post,
}

impl Verb {
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Get => "GET",
            Self::Post => "POST",
        }
    }

    /// Parse the `"GET"` / `"POST"` of a custom service's `methods` map.
    #[must_use]
    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "GET" => Some(Self::Get),
            "POST" => Some(Self::Post),
            _ => None,
        }
    }
}

/// What decides whether a deferral's 501 route is registered (§6.6).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Owner {
    /// A `features.<name>` flag: registered while the flag is false.
    Feature(&'static str),
    /// A built-in `[services.<name>]` block: registered while it is
    /// `disabled`, or always for a pinned NSID.
    Service(&'static str),
    /// No flag and none planned (PDS, labeler and video-service surfaces).
    Unflagged,
}

/// One deferred endpoint.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Deferral {
    pub nsid: &'static str,
    pub verb: Verb,
    pub owner: Owner,
    pub message: &'static str,
    /// Its 501 route is registered whatever its service's state (§6.7.2.2).
    pub pinned: bool,
}

const fn d(nsid: &'static str, verb: Verb, owner: Owner, message: &'static str) -> Deferral {
    Deferral {
        nsid,
        verb,
        owner,
        message,
        pinned: false,
    }
}

const FEEDS: Owner = Owner::Feature("feed_generators");
const EMBED: Owner = Owner::Feature("embed_external_view");
const PUSH_SERVICE: Owner = Owner::Service("push");
const AGE_SERVICE: Owner = Owner::Service("age_assurance");
const CONTACT_SERVICE: Owner = Owner::Service("contact");

/// The §6.2 table.
pub const DEFERRALS: &[Deferral] = &[
    // D-1, feed generators.
    d("app.bsky.feed.getFeed", Verb::Get, FEEDS, DEFAULT_MESSAGE),
    d("app.bsky.feed.getFeedGenerator", Verb::Get, FEEDS, DEFAULT_MESSAGE),
    d("app.bsky.feed.getFeedGenerators", Verb::Get, FEEDS, DEFAULT_MESSAGE),
    d("app.bsky.feed.getActorFeeds", Verb::Get, FEEDS, DEFAULT_MESSAGE),
    d("app.bsky.feed.getSuggestedFeeds", Verb::Get, FEEDS, DEFAULT_MESSAGE),
    d("app.bsky.feed.describeFeedGenerator", Verb::Get, FEEDS, DEFAULT_MESSAGE),
    d("app.bsky.unspecced.getSuggestedFeeds", Verb::Get, FEEDS, DEFAULT_MESSAGE),
    d("app.bsky.unspecced.getPopularFeedGenerators", Verb::Get, FEEDS, DEFAULT_MESSAGE),
    d("app.bsky.feed.sendInteractions", Verb::Post, FEEDS, DEFAULT_MESSAGE),
    // D-2, push: `[services.push]`.
    d("app.bsky.notification.registerPush", Verb::Post, PUSH_SERVICE, PUSH),
    d("app.bsky.notification.unregisterPush", Verb::Post, PUSH_SERVICE, PUSH),
    // D-4, preferences: a PDS surface.
    d("app.bsky.actor.getPreferences", Verb::Get, Owner::Unflagged, PREFERENCES),
    d("app.bsky.actor.putPreferences", Verb::Post, Owner::Unflagged, PREFERENCES),
    // D-5, reports: a labeler surface.
    d("com.atproto.moderation.createReport", Verb::Post, Owner::Unflagged, REPORT),
    // D-6, video: a video-service surface.
    d("app.bsky.video.getJobStatus", Verb::Get, Owner::Unflagged, VIDEO),
    d("app.bsky.video.getUploadLimits", Verb::Get, Owner::Unflagged, VIDEO),
    d("app.bsky.video.getUploadStatus", Verb::Get, Owner::Unflagged, VIDEO),
    d("app.bsky.video.uploadVideo", Verb::Post, Owner::Unflagged, VIDEO),
    d("app.bsky.video.startUpload", Verb::Post, Owner::Unflagged, VIDEO),
    d("app.bsky.video.uploadPart", Verb::Post, Owner::Unflagged, VIDEO),
    d("app.bsky.video.finishUpload", Verb::Post, Owner::Unflagged, VIDEO),
    d("app.bsky.video.abortUpload", Verb::Post, Owner::Unflagged, VIDEO),
    // D-7, external embed views.
    d("app.bsky.embed.getEmbedExternalView", Verb::Get, EMBED, DEFAULT_MESSAGE),
    // `[services.age_assurance]`.
    d("app.bsky.ageassurance.getState", Verb::Get, AGE_SERVICE, AGE_ASSURANCE),
    d("app.bsky.ageassurance.getConfig", Verb::Get, AGE_SERVICE, AGE_ASSURANCE),
    d("app.bsky.unspecced.getAgeAssuranceState", Verb::Get, AGE_SERVICE, AGE_ASSURANCE),
    d("app.bsky.ageassurance.begin", Verb::Post, AGE_SERVICE, AGE_ASSURANCE),
    d("app.bsky.unspecced.initAgeAssurance", Verb::Post, AGE_SERVICE, AGE_ASSURANCE),
    // `[services.contact]`.
    d("app.bsky.contact.getMatches", Verb::Get, CONTACT_SERVICE, DEFAULT_MESSAGE),
    d("app.bsky.contact.getSyncStatus", Verb::Get, CONTACT_SERVICE, DEFAULT_MESSAGE),
    d("app.bsky.contact.dismissMatch", Verb::Post, CONTACT_SERVICE, DEFAULT_MESSAGE),
    d("app.bsky.contact.importContacts", Verb::Post, CONTACT_SERVICE, DEFAULT_MESSAGE),
    d("app.bsky.contact.removeData", Verb::Post, CONTACT_SERVICE, DEFAULT_MESSAGE),
    // The contact service's inbound callback, not a client method: pinned.
    Deferral {
        pinned: true,
        ..d("app.bsky.contact.sendNotification", Verb::Post, CONTACT_SERVICE, DEFAULT_MESSAGE)
    },
    d("app.bsky.contact.startPhoneVerification", Verb::Post, CONTACT_SERVICE, DEFAULT_MESSAGE),
    d("app.bsky.contact.verifyPhone", Verb::Post, CONTACT_SERVICE, DEFAULT_MESSAGE),
];

/// The deferral for an NSID.
#[must_use]
pub fn find(nsid: &str) -> Option<&'static Deferral> {
    DEFERRALS.iter().find(|d| d.nsid == nsid)
}

/// §6.1's `<link>` rule: with a status URL the placeholder is replaced; with
/// none, every sentence containing it is dropped and the rest is unchanged.
#[must_use]
pub fn render(template: &str, status_url: &str) -> String {
    if !status_url.is_empty() {
        return template.replace(LINK, status_url);
    }
    let kept: String = template.split_inclusive(". ").filter(|s| !s.contains(LINK)).collect();
    kept.trim_end().to_string()
}

/// Whether a deferral's 501 route is registered under this config (§6.6).
#[must_use]
pub fn registered(d: &Deferral, cfg: &FileConfig) -> bool {
    match d.owner {
        Owner::Unflagged => true,
        Owner::Feature(feature) => !cfg.features.enabled(feature),
        Owner::Service(service) => {
            d.pinned
                || cfg
                    .services
                    .get(service)
                    .is_none_or(|s| s.state == ServiceState::Disabled)
        }
    }
}

/// The message a registered deferral answers with: the operator's override
/// if there is one, else the compiled default, with the link rule applied.
#[must_use]
pub fn message(d: &Deferral, cfg: &FileConfig) -> String {
    let template = cfg.honest_501.messages.get(d.nsid).map_or(d.message, String::as_str);
    render(template, &cfg.honest_501.status_url)
}

#[derive(Clone)]
struct Route {
    app: AppState,
    nsid: &'static str,
    message: Arc<str>,
}

/// No authentication: the answer never depends on the caller (§6.1). Rate
/// class `anon_read` by IP for every caller.
async fn answer(State(r): State<Route>, ConnectInfo(peer): ConnectInfo<SocketAddr>, headers: HeaderMap) -> Response {
    let ip = request_ip(&r.app, &headers, peer);
    if let Err(limited) = r.app.limits.check_ip(Class::AnonRead, ip) {
        return XrpcError::from(limited).into_response();
    }
    metrics::counter!(METRIC, "nsid" => r.nsid).increment(1);
    (
        StatusCode::NOT_IMPLEMENTED,
        Json(json!({ "error": "NotImplemented", "message": &*r.message })),
    )
        .into_response()
}

/// The 501 routes this config registers, each for its lexicon verb. Merged
/// into the app before `xrpc::router::finalize`.
pub fn router(app: &AppState, cfg: &FileConfig) -> Router {
    let mut router = Router::new();
    for d in DEFERRALS.iter().filter(|d| registered(d, cfg)) {
        let state = Route {
            app: app.clone(),
            nsid: d.nsid,
            message: Arc::from(message(d, cfg)),
        };
        let method = match d.verb {
            Verb::Get => get(answer),
            Verb::Post => post(answer),
        };
        router = router.route(&format!("/xrpc/{}", d.nsid), method.with_state(state));
    }
    router
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use super::*;

    #[test]
    fn table_shape() {
        assert_eq!(DEFERRALS.len(), 36);
        let unique: BTreeSet<&str> = DEFERRALS.iter().map(|d| d.nsid).collect();
        assert_eq!(unique.len(), DEFERRALS.len(), "no NSID twice");
        let of = |service: &'static str| {
            DEFERRALS
                .iter()
                .filter(|d| d.owner == Owner::Service(service))
                .count()
        };
        assert_eq!((of("contact"), of("age_assurance"), of("push")), (8, 5, 2));
        let pinned: Vec<&str> = DEFERRALS.iter().filter(|d| d.pinned).map(|d| d.nsid).collect();
        assert_eq!(pinned, ["app.bsky.contact.sendNotification"]);
        for d in DEFERRALS {
            assert!(kryphocron_lexicons::Nsid::new(d.nsid).is_ok(), "{}", d.nsid);
            // No deferral sits in this AppView's own namespace.
            assert!(!d.nsid.starts_with("app.nearhorizon."), "{}", d.nsid);
        }
    }

    #[test]
    fn link_rule() {
        let url = "https://status.example";
        assert_eq!(
            render(DEFAULT_MESSAGE, url),
            "This endpoint is planned for a future release. See https://status.example for status."
        );
        assert_eq!(render(DEFAULT_MESSAGE, ""), "This endpoint is planned for a future release.");
        assert_eq!(render(PUSH, ""), "Push notifications are not available from this AppView yet.");
        // A message without the placeholder is unchanged either way.
        assert_eq!(render(AGE_ASSURANCE, ""), AGE_ASSURANCE);
        assert_eq!(render(REPORT, ""), REPORT);
        assert_eq!(render(REPORT, url), REPORT);
        // An operator's override follows the same rule.
        assert_eq!(render("Later. Ask at <link>. Thanks.", ""), "Later. Thanks.");
    }

    #[test]
    fn every_route_is_registered_by_default_and_a_pin_holds() {
        let cfg = FileConfig::default();
        assert!(DEFERRALS.iter().all(|d| registered(d, &cfg)));
        // Were proxying built, a proxied contact service would keep only its
        // pinned 501 route.
        let cfg = FileConfig::parse("[services.contact]\nstate = \"proxy\"\nproxy_url = \"https://c.example\"\n").expect("parse");
        let kept: Vec<&str> = DEFERRALS
            .iter()
            .filter(|d| d.owner == Owner::Service("contact") && registered(d, &cfg))
            .map(|d| d.nsid)
            .collect();
        assert_eq!(kept, ["app.bsky.contact.sendNotification"]);
        assert!(registered(find("app.bsky.notification.registerPush").expect("push"), &cfg));
    }

    #[test]
    fn override_replaces_one_message_only() {
        let cfg = FileConfig::parse(
            "[honest_501]\nstatus_url = \"https://s.example\"\n[honest_501.messages]\n\"app.bsky.feed.getFeed\" = \"Feeds soon. See <link>.\"\n",
        )
        .expect("parse");
        assert_eq!(
            message(find("app.bsky.feed.getFeed").expect("d"), &cfg),
            "Feeds soon. See https://s.example."
        );
        assert_eq!(
            message(find("app.bsky.feed.getFeedGenerator").expect("d"), &cfg),
            "This endpoint is planned for a future release. See https://s.example for status."
        );
    }
}
