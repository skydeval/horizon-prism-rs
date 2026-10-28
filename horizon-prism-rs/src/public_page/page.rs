//! `GET /` (public-page design §2, §4.2): one template render from the
//! snapshot and the fixed text. It runs no query and makes no outbound call.

use std::net::SocketAddr;

use askama::Template;
use axum::extract::{ConnectInfo, State};
use axum::http::header::CACHE_CONTROL;
use axum::http::{HeaderMap, HeaderValue, StatusCode};
use axum::response::{Html, IntoResponse, Response};

use super::status::StatusSnapshot;
use super::{CSS_HREF, Fixed, PAGE_CACHE, PageState};

/// The fixed line of the About section (§2.1), before the version.
pub const PRODUCT: &str = "Horizon-Prism, an AT Protocol AppView written in Rust";

/// The page.
#[derive(Template)]
#[template(path = "public_index.html")]
pub struct IndexPage<'a> {
    pub css: &'a str,
    pub title: &'a str,
    pub fixed: &'a Fixed,
    pub version: &'static str,
    pub product: &'static str,
    /// The live feed's section and script.
    pub live: bool,
    pub js: &'a str,
}

/// The page. The snapshot is not shown; it is taken so the page's callers
/// stay as they are.
#[must_use]
pub fn view<'a>(_s: &StatusSnapshot, fixed: &'a Fixed, title: &'a str) -> IndexPage<'a> {
    IndexPage {
        css: CSS_HREF.as_str(),
        title,
        fixed,
        version: env!("CARGO_PKG_VERSION"),
        product: PRODUCT,
        live: fixed.live_feed,
        js: super::JS_HREF.as_str(),
    }
}

/// `GET /`.
pub async fn index(State(s): State<PageState>, ConnectInfo(peer): ConnectInfo<SocketAddr>, headers: HeaderMap) -> Response {
    if let Err(refused) = s.charge(&headers, peer) {
        return refused;
    }
    let snapshot = s.snapshot.load();
    let title = s.title();
    match view(&snapshot, &s.fixed, &title).render() {
        Ok(html) => {
            let mut response = Html(html).into_response();
            response.headers_mut().insert(CACHE_CONTROL, HeaderValue::from_static(PAGE_CACHE));
            if s.fixed.live_feed {
                response.extensions_mut().insert(super::LivePage);
            }
            response
        }
        Err(error) => {
            tracing::error!(event = "public_page_render_failed", %error, "the public page failed to render");
            StatusCode::INTERNAL_SERVER_ERROR.into_response()
        }
    }
}
