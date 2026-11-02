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
use super::{AdminSettings, AdminState, router};
use crate::aggregations::{ScopeSets, ScopeState};
use crate::config::ScopePolicy;
use crate::identity::DidDocInfo;

/// The configuration the admin tests run with: auto-join off.
#[must_use]
pub fn no_auto_join() -> crate::config::FileConfig {
    let mut file = crate::config::FileConfig::default();
    file.community.auto_enroll = false;
    file
}

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
        settings: super::settings::SettingsContext::from_file(no_auto_join()),
        disks: super::stats::DiskPaths::standard(&std::env::temp_dir()),
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

/// The signed-in sidebar of a page, `<nav class="sidebar"` to its `</nav>`;
/// empty when the page has none.
#[must_use]
pub fn sidebar(body: &str) -> &str {
    let Some(start) = body.find("<nav class=\"sidebar\"") else {
        return "";
    };
    let end = body[start..].find("</nav>").map_or(body.len(), |e| start + e + "</nav>".len());
    &body[start..end]
}

/// The one element of the sidebar carrying `aria-current`: its text (a menu
/// item's name, or "Appearance") and the attribute's value. `None` when no
/// element, or more than one, carries it.
#[must_use]
pub fn current_item(body: &str) -> Option<(String, String)> {
    let nav = sidebar(body);
    let marks: Vec<usize> = nav.match_indices(" aria-current=\"").map(|(i, _)| i).collect();
    let [at] = marks.as_slice() else {
        return None;
    };
    let rest = &nav[at + " aria-current=\"".len()..];
    let value = rest[..rest.find('"')?].to_string();
    let after = &rest[rest.find('>')? + 1..];
    let text = after.strip_prefix("<span>").unwrap_or(after);
    let text = text[..text.find('<')?].to_string();
    Some((text, value))
}

/// A hand-written reader of the admin stylesheet, enough for its tests: no
/// CSS parser is a dependency.
pub mod css {
    /// One rule in file order: the enclosing at-rule (whitespace
    /// normalised, `""` at top level), the whole selector list, and the
    /// declarations, each normalised. An `@font-face` or `@keyframes` block
    /// is one opaque entry: its selector is the at-rule's prelude and its
    /// declarations its body.
    #[derive(Debug, Clone, PartialEq, Eq)]
    pub struct Rule {
        pub context: String,
        pub selectors: String,
        pub declarations: String,
    }

    fn normal(s: &str) -> String {
        s.split_whitespace().collect::<Vec<_>>().join(" ")
    }

    /// Declarations as `name: value; …`, each one normalised, in order.
    #[must_use]
    pub fn declarations(body: &str) -> String {
        body.split(';').map(normal).filter(|d| !d.is_empty()).map(|d| format!("{d};")).collect::<Vec<_>>().join(" ")
    }

    fn strip_comments(text: &str) -> String {
        let mut out = String::with_capacity(text.len());
        let mut rest = text;
        while let Some(at) = rest.find("/*") {
            out.push_str(&rest[..at]);
            rest = rest[at + 2..].find("*/").map_or("", |e| &rest[at + 2 + e + 2..]);
        }
        out.push_str(rest);
        out
    }

    /// The block after an opening brace at `open`: its body and the index
    /// just past its closing brace.
    fn block(text: &str, open: usize) -> (&str, usize) {
        let mut depth = 0;
        for (i, c) in text[open..].char_indices() {
            match c {
                '{' => depth += 1,
                '}' => {
                    depth -= 1;
                    if depth == 0 {
                        return (&text[open + 1..open + i], open + i + 1);
                    }
                }
                _ => {}
            }
        }
        (&text[open + 1..], text.len())
    }

    fn read(text: &str, context: &str, out: &mut Vec<Rule>) {
        let mut at = 0;
        while let Some(open) = text[at..].find('{').map(|o| at + o) {
            let prelude = normal(&text[at..open]);
            let (body, next) = block(text, open);
            if prelude.starts_with("@media") || prelude.starts_with("@supports") {
                read(body, &prelude, out);
            } else {
                out.push(Rule { context: context.to_string(), selectors: prelude, declarations: declarations(body) });
            }
            at = next;
        }
    }

    /// Every rule of `text`, in file order.
    #[must_use]
    pub fn rules(text: &str) -> Vec<Rule> {
        let mut out = Vec::new();
        read(&strip_comments(text), "", &mut out);
        out
    }
}

/// A hand-written reader of the admin pages' tables, enough for their
/// tests: no HTML parser is a dependency.
pub mod html {
    /// A cell: its classes and, for a body cell, whether it is a text cell
    /// holding exactly one `div.cell-text` and nothing else.
    #[derive(Debug, Clone, PartialEq, Eq)]
    pub struct Cell {
        pub classes: Vec<String>,
        pub text: String,
        pub colspan: bool,
        pub cell_text_only: bool,
    }

    /// A table in page order.
    #[derive(Debug, Clone)]
    pub struct Table {
        /// The class of the element the table sits in, and whether the
        /// table is all that element holds.
        pub parent_class: String,
        pub alone: bool,
        pub head: Vec<Cell>,
        pub rows: Vec<Vec<Cell>>,
    }

    fn attr(tag: &str, name: &str) -> Option<String> {
        let key = format!(" {name}=\"");
        let at = tag.find(&key)? + key.len();
        Some(tag[at..at + tag[at..].find('"')?].to_string())
    }

    /// Text with the tags taken out, whitespace normalised.
    #[must_use]
    pub fn text_of(s: &str) -> String {
        let mut out = String::new();
        let mut inside = false;
        for c in s.chars() {
            match c {
                '<' => inside = true,
                '>' => inside = false,
                c if !inside => out.push(c),
                _ => {}
            }
        }
        out.split_whitespace().collect::<Vec<_>>().join(" ")
    }

    fn cells(row: &str, tag: &str) -> Vec<Cell> {
        let open = format!("<{tag}");
        let close = format!("</{tag}>");
        let mut out = Vec::new();
        let mut at = 0;
        while let Some(start) = row[at..].find(&open).map(|s| at + s) {
            // `<th` also begins `<thead>`: only the cell's own tag counts.
            if !matches!(row.as_bytes().get(start + open.len()), Some(b'>' | b' ')) {
                at = start + open.len();
                continue;
            }
            let tag_end = start + row[start..].find('>').unwrap_or(0);
            let head = &row[start..=tag_end];
            let end = row[tag_end..].find(&close).map_or(row.len(), |e| tag_end + e);
            let inner = &row[tag_end + 1..end];
            let classes: Vec<String> = attr(head, "class").map(|c| c.split_whitespace().map(str::to_string).collect()).unwrap_or_default();
            let trimmed = inner.trim();
            let cell_text_only = trimmed.starts_with("<div class=\"cell-text\">")
                && trimmed.ends_with("</div>")
                && {
                    // One div: the first closing </div> that balances the
                    // opening one is the last thing in the cell.
                    let mut depth = 0usize;
                    let mut balanced_at = None;
                    let mut i = 0;
                    while i < trimmed.len() {
                        if trimmed[i..].starts_with("<div") {
                            depth += 1;
                        } else if trimmed[i..].starts_with("</div>") {
                            depth -= 1;
                            if depth == 0 {
                                balanced_at = Some(i + "</div>".len());
                                break;
                            }
                        }
                        i += trimmed[i..].chars().next().map_or(1, char::len_utf8);
                    }
                    balanced_at == Some(trimmed.len())
                };
            out.push(Cell { classes, text: text_of(inner), colspan: head.contains(" colspan="), cell_text_only });
            at = end + close.len();
        }
        out
    }

    /// Every `<table` of `body`, in order.
    #[must_use]
    pub fn tables(body: &str) -> Vec<Table> {
        let mut out = Vec::new();
        let mut at = 0;
        while let Some(start) = body[at..].find("<table").map(|s| at + s) {
            let end = body[start..].find("</table>").map_or(body.len(), |e| start + e + "</table>".len());
            let table = &body[start..end];
            // The element just before: the last opening tag before the
            // table, and what follows the table up to its closing tag.
            let before = &body[..start];
            let open_at = before.rfind('<').unwrap_or(0);
            let open = &before[open_at..];
            let parent_class = if open.starts_with("<div") && !open.starts_with("</") { attr(open, "class").unwrap_or_default() } else { String::new() };
            let gap_before = before[open_at + open.find('>').map_or(0, |e| e + 1)..].trim().is_empty();
            let after = body[end..].trim_start();
            let alone = gap_before && after.starts_with("</div>");
            let head = table.find("<thead>").map(|h| {
                let e = table[h..].find("</thead>").map_or(table.len(), |e| h + e);
                cells(&table[h..e], "th")
            });
            let mut rows = Vec::new();
            if let Some(b) = table.find("<tbody>") {
                let body_end = table[b..].find("</tbody>").map_or(table.len(), |e| b + e);
                let tbody = &table[b..body_end];
                let mut r = 0;
                while let Some(s) = tbody[r..].find("<tr").map(|s| r + s) {
                    let e = tbody[s..].find("</tr>").map_or(tbody.len(), |e| s + e);
                    rows.push(cells(&tbody[s..e], "td"));
                    r = e + "</tr>".len();
                }
            }
            out.push(Table { parent_class, alone, head: head.unwrap_or_default(), rows });
            at = end;
        }
        out
    }
}
