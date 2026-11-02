//! About this server (`/admin/about`): read-only facts about this server.
//! What it is, its version and addresses; what it serves, one paragraph
//! each leading to the page that shows it; and, for developers, the methods
//! it serves, those it answers "not offered here", and the custom lexicons.
//! Its status is on Health and on each feature's page.
//!
//! No query runs per request: it is one render of the settings and the
//! compiled-in lists.

use std::collections::BTreeMap;
use std::net::SocketAddr;

use askama::Template;
use axum::extract::{ConnectInfo, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::Response;

use super::indexing::phase_words;
use super::{Nav, ago, css, render, when};
use crate::admin::AdminState;
use crate::admin::auth::{Need, require};
use crate::admin::lexicons::{self, Lexicon};
use crate::admin::menu::PageId;
use crate::admin::usage;
use crate::config::FileConfig;
use crate::indexing::PublicMode;

/// What Horizon-Prism is, the same for every operator.
pub const ABOUT: &str = "Horizon-Prism, an AT Protocol AppView written in Rust.";
/// What this instance does.
pub const DOES: &str = "It reads the AT Protocol network through its relays and answers the apps of its community, with private posts for its members. \
     It never writes to anyone's account: apps write to their own server.";

/// The methods of one namespace.
pub struct MethodGroup {
    pub namespace: String,
    pub methods: Vec<String>,
}

/// A method answered "not offered here".
pub struct NotOffered {
    pub nsid: String,
    pub message: String,
}

/// Everything the page shows, built from state already in memory.
pub struct View {
    pub about: &'static str,
    pub does: &'static str,
    pub version: &'static str,
    pub hostname: String,
    pub service_did: String,
    pub started: String,
    /// What Prism indexes, in a sentence.
    pub indexing: String,
    /// The labels applied here, in a sentence.
    pub labels: String,
    /// The communities whose accounts are kept, in a sentence.
    pub communities: String,
    pub policy: String,
    pub methods: Vec<MethodGroup>,
    pub served: usize,
    pub not_offered: Vec<NotOffered>,
    /// The public methods answered 501 while public data is not on.
    pub public_off: Vec<String>,
    pub lexicons: Vec<Lexicon>,
}

#[derive(Template)]
#[template(path = "instance.html")]
struct InstancePage {
    css: &'static str,
    nav: Option<Nav>,
    title: String,
    v: View,
}

/// What Prism indexes, and the phase it is in.
fn indexing_text(mode: PublicMode) -> String {
    let what = match mode {
        PublicMode::On => "Prism keeps public posts of its members and the accounts around them, and answers public requests.",
        PublicMode::Filling => "Public data is being turned on: Prism is refilling it, and answers public requests when that is done.",
        PublicMode::Off | PublicMode::Draining | PublicMode::Held => {
            "Prism indexes kryphocron only: it keeps only what kryphocron posts and Horizon features need, and apps use their usual AppView for public reads."
        }
    };
    match mode {
        // The sentence says which; a passing state says what it is doing.
        PublicMode::Draining | PublicMode::Held => format!("{what} Public data: {}.", phase_words(mode)),
        PublicMode::On | PublicMode::Filling | PublicMode::Off => what.to_string(),
    }
}

/// Every registered method but the "not offered" ones, by namespace.
fn method_groups(registered: &[(String, &'static str)]) -> (Vec<MethodGroup>, usize) {
    let mut groups: BTreeMap<String, Vec<String>> = BTreeMap::new();
    let mut served = 0;
    for (nsid, router) in registered {
        if *router == "not_offered" {
            continue;
        }
        served += 1;
        let namespace = nsid.rsplit_once('.').map_or_else(|| "other".to_string(), |(ns, _)| ns.to_string());
        groups.entry(namespace).or_default().push(nsid.clone());
    }
    let groups = groups
        .into_iter()
        .map(|(namespace, mut methods)| {
            methods.sort();
            MethodGroup { namespace, methods }
        })
        .collect();
    (groups, served)
}

fn not_offered(cfg: &FileConfig) -> Vec<NotOffered> {
    crate::honest_501::DEFERRALS
        .iter()
        .filter(|d| crate::honest_501::registered(d, cfg))
        .map(|d| NotOffered { nsid: d.nsid.to_string(), message: crate::honest_501::message(d, cfg) })
        .collect()
}

fn plural(n: usize, one: &str, many: &str) -> String {
    format!("{n} {}", if n == 1 { one } else { many })
}

/// The page's content.
#[must_use]
pub fn view(state: &AdminState) -> View {
    let cfg = &state.settings.running;
    let live = state.settings.live.load();
    let mode = state.scope.public_mode();
    let (methods, served) = method_groups(&usage::registered());
    let service_did = format!("did:web:{}", state.public_hostname);
    let labelers = live.refract_communities.iter().filter(|c| c.labeler_did.is_some()).count();
    let labels = if labelers == 0 {
        format!("Prism applies its own labels, as {service_did}, and follows no labeler's stream.")
    } else {
        format!(
            "Prism applies its own labels, as {service_did}, and follows no labeler's stream. Restored accounts also carry the labels of their community's labeler ({}).",
            plural(labelers, "community has one", "communities have one")
        )
    };
    let listed = live.refract_communities.len();
    let enabled = live.refract_communities.iter().filter(|c| c.enabled).count();
    let communities = if live.refract_enabled {
        format!("Refract is on: accounts other communities still host stay readable, for {} ({enabled} in use).", plural(listed, "listed community", "listed communities"))
    } else {
        format!("Refract is off, with {}.", plural(listed, "listed community", "listed communities"))
    };
    let public_off = if mode == PublicMode::On {
        Vec::new()
    } else {
        crate::honest_501::PUBLIC_METHODS.iter().map(|m| m.nsid.to_string()).collect()
    };
    View {
        about: ABOUT,
        does: DOES,
        version: env!("CARGO_PKG_VERSION"),
        hostname: state.public_hostname.clone(),
        service_did,
        started: format!("{} ({})", when(state.settings.started_at), ago(state.settings.started_at)),
        indexing: indexing_text(mode),
        labels,
        communities,
        policy: "Not configured: this build has no moderation policy setting yet.".to_string(),
        methods,
        served,
        not_offered: not_offered(cfg),
        public_off,
        lexicons: lexicons::all(),
    }
}

/// `GET /admin/about`.
pub async fn page(State(state): State<AdminState>, ConnectInfo(peer): ConnectInfo<SocketAddr>, headers: HeaderMap) -> Response {
    let op = match require(&state, &headers, peer, Need::Page, None).await {
        Ok(op) => op,
        Err(response) => return response,
    };
    render(StatusCode::OK, &InstancePage { css: css(), nav: Some(Nav::page(&op, PageId::About)), title: PageId::About.page().name.to_string(), v: view(&state) })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn methods_group_by_namespace_without_the_not_offered() {
        let reg = vec![
            ("a.b.getOne".to_string(), "app"),
            ("a.b.getTwo".to_string(), "index"),
            ("a.c.doIt".to_string(), "private"),
            ("a.b.planned".to_string(), "not_offered"),
            ("_health".to_string(), "health"),
        ];
        let (groups, served) = method_groups(&reg);
        assert_eq!(served, 4);
        let flat: Vec<(String, Vec<String>)> = groups.into_iter().map(|g| (g.namespace, g.methods)).collect();
        assert_eq!(
            flat,
            vec![
                ("a.b".to_string(), vec!["a.b.getOne".to_string(), "a.b.getTwo".to_string()]),
                ("a.c".to_string(), vec!["a.c.doIt".to_string()]),
                ("other".to_string(), vec!["_health".to_string()]),
            ]
        );
    }

    #[test]
    fn indexing_in_words() {
        let text = indexing_text(PublicMode::Off);
        assert!(text.starts_with("Prism indexes kryphocron only") && !text.contains("Public data:"), "{text}");
        assert!(indexing_text(PublicMode::Draining).ends_with("Public data: draining: stored public data is being deleted in the background."));
    }
}
