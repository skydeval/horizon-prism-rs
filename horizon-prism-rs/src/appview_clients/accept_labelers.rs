//! The reader's labelers on Thread Lens reads (threadlens-hydration design
//! §3.6 (b), skydeval's decision of 2026-10-24).
//!
//! A `threadLens.getThread` or `.resolve` request may carry
//! `atproto-accept-labelers`, as a stock read does. Thread Lens then runs
//! inside [`with_labelers`]:
//! - every `getPostThread` and `getPosts` the cascade makes for it carries
//!   the header, rebuilt from the valid entries only (never the raw value:
//!   nothing else of the request is forwarded, and no reader DID or token
//!   ever is);
//! - normalization keeps the labels those labelers applied, beside Prism's
//!   own;
//! - the response names, in `atproto-content-labelers`, the labelers an
//!   upstream said it applied.
//!
//! Every labeler is forwarded, moderation or not: a labeler is a labeler.
//! The labels only add to views; Prism's gate and Thread Lens's rules decide
//! what is shown, as before.

use std::collections::HashSet;
use std::sync::{Arc, Mutex, PoisonError};

use axum::http::{HeaderMap, HeaderValue};

/// The request header naming the labelers a client accepts.
pub const ACCEPT_LABELERS: &str = "atproto-accept-labelers";
/// The response header naming the labelers that were applied.
pub const CONTENT_LABELERS: &str = "atproto-content-labelers";
/// The longest header read, all its values together. A longer one is
/// ignored whole.
pub const MAX_HEADER_BYTES: usize = 4096;
/// Labelers kept from one header; later entries are dropped.
pub const MAX_LABELERS: usize = 32;

/// One accepted labeler.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Labeler {
    pub did: String,
    /// The `redact` parameter.
    pub redact: bool,
}

impl Labeler {
    fn render(&self) -> String {
        if self.redact { format!("{};redact", self.did) } else { self.did.clone() }
    }
}

/// The labelers a request accepts: valid DIDs, at most [`MAX_LABELERS`],
/// each once, in the header's order.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AcceptLabelers {
    entries: Vec<Labeler>,
}

impl AcceptLabelers {
    /// Read `atproto-accept-labelers` from request headers. `None` when it
    /// is absent, longer than [`MAX_HEADER_BYTES`], not text, or names no
    /// valid DID. An entry that is not a valid DID is skipped; parameters
    /// other than `redact` are dropped.
    #[must_use]
    pub fn from_headers(headers: &HeaderMap) -> Option<Self> {
        let mut raw = String::new();
        for value in headers.get_all(ACCEPT_LABELERS) {
            let text = value.to_str().ok()?;
            if !raw.is_empty() {
                raw.push(',');
            }
            raw.push_str(text);
            if raw.len() > MAX_HEADER_BYTES {
                return None;
            }
        }
        Self::parse(&raw)
    }

    /// Parse a header value (see [`Self::from_headers`]).
    #[must_use]
    pub fn parse(raw: &str) -> Option<Self> {
        if raw.len() > MAX_HEADER_BYTES {
            return None;
        }
        let mut entries: Vec<Labeler> = Vec::new();
        for item in raw.split(',') {
            if entries.len() >= MAX_LABELERS {
                break;
            }
            let mut parts = item.split(';').map(str::trim);
            let did = parts.next().unwrap_or_default();
            if !did.starts_with("did:") || !proto_blue::syntax::Did::is_valid(did) {
                continue;
            }
            let redact = parts.any(|p| p.eq_ignore_ascii_case("redact"));
            match entries.iter_mut().find(|e| e.did == did) {
                Some(e) => e.redact |= redact,
                None => entries.push(Labeler { did: did.to_string(), redact }),
            }
        }
        (!entries.is_empty()).then_some(Self { entries })
    }

    #[must_use]
    pub fn entries(&self) -> &[Labeler] {
        &self.entries
    }

    #[must_use]
    pub fn contains(&self, did: &str) -> bool {
        self.entries.iter().any(|e| e.did == did)
    }

    /// The value forwarded upstream: the valid entries, rebuilt.
    #[must_use]
    pub fn header_value(&self) -> HeaderValue {
        let text = self.entries.iter().map(Labeler::render).collect::<Vec<_>>().join(", ");
        // DIDs are ASCII by their syntax.
        HeaderValue::from_str(&text).unwrap_or_else(|_| HeaderValue::from_static(""))
    }
}

/// The DIDs an `atproto-content-labelers` value names.
#[must_use]
pub fn named_in(headers: &HeaderMap) -> Vec<String> {
    headers
        .get_all(CONTENT_LABELERS)
        .iter()
        .filter_map(|v| v.to_str().ok())
        .flat_map(|v| v.split(','))
        .filter_map(|item| item.split(';').next().map(str::trim))
        .filter(|did| !did.is_empty())
        .map(str::to_string)
        .collect()
}

/// One request's labelers, and what its upstream calls reported.
#[derive(Debug)]
pub struct LabelerScope {
    accept: AcceptLabelers,
    value: HeaderValue,
    /// Accepted labelers an upstream answer named as applied.
    applied: Mutex<HashSet<String>>,
    /// Post URIs whose upstream view, fetched with the header, was
    /// normalized for this request.
    labelled: Mutex<HashSet<String>>,
}

impl LabelerScope {
    #[must_use]
    pub fn new(accept: AcceptLabelers) -> Arc<Self> {
        let value = accept.header_value();
        Arc::new(Self {
            accept,
            value,
            applied: Mutex::new(HashSet::new()),
            labelled: Mutex::new(HashSet::new()),
        })
    }

    #[must_use]
    pub fn accept(&self) -> &AcceptLabelers {
        &self.accept
    }

    /// The header value sent upstream.
    #[must_use]
    pub fn value(&self) -> &HeaderValue {
        &self.value
    }

    /// Record an upstream answer's `atproto-content-labelers`: only the
    /// labelers the request accepted count.
    pub fn record_applied(&self, response: &HeaderMap) {
        let named = named_in(response);
        let mut applied = self.applied.lock().unwrap_or_else(PoisonError::into_inner);
        for did in named {
            if self.accept.contains(&did) {
                applied.insert(did);
            }
        }
    }

    /// Mark a labeler applied by Prism itself (its own labels are always
    /// on its views).
    pub fn mark_applied(&self, did: &str) {
        if self.accept.contains(did) {
            self.applied.lock().unwrap_or_else(PoisonError::into_inner).insert(did.to_string());
        }
    }

    pub fn mark_labelled<I: IntoIterator<Item = String>>(&self, uris: I) {
        self.labelled.lock().unwrap_or_else(PoisonError::into_inner).extend(uris);
    }

    #[must_use]
    pub fn is_labelled(&self, uri: &str) -> bool {
        self.labelled.lock().unwrap_or_else(PoisonError::into_inner).contains(uri)
    }

    /// The response's `atproto-content-labelers`: the accepted labelers
    /// that were applied, in the request's order with its `redact`. `None`
    /// when none was.
    #[must_use]
    pub fn content_labelers(&self) -> Option<HeaderValue> {
        let applied = self.applied.lock().unwrap_or_else(PoisonError::into_inner);
        let named: Vec<String> = self.accept.entries.iter().filter(|e| applied.contains(&e.did)).map(Labeler::render).collect();
        if named.is_empty() {
            return None;
        }
        HeaderValue::from_str(&named.join(", ")).ok()
    }
}

tokio::task_local! {
    static SCOPE: Arc<LabelerScope>;
}

/// Run `work` with `scope`'s labelers applied to the upstream calls and
/// normalizations it makes; with `None`, run it as it is.
pub async fn with_labelers<F: std::future::Future>(scope: Option<Arc<LabelerScope>>, work: F) -> F::Output {
    match scope {
        Some(scope) => SCOPE.scope(scope, work).await,
        None => work.await,
    }
}

/// The labelers of the request being served, if it named any.
#[must_use]
pub fn current() -> Option<Arc<LabelerScope>> {
    SCOPE.try_with(Arc::clone).ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    const A: &str = "did:plc:aaaaaaaaaaaaaaaaaaaaaaaa";
    const B: &str = "did:web:labels.example.com";

    #[test]
    fn parses_valid_entries_and_drops_the_rest() {
        let got = AcceptLabelers::parse(&format!(" {A};redact , not-a-did, did:bad, , {B};other, {A}")).expect("two");
        assert_eq!(got.entries(), [Labeler { did: A.into(), redact: true }, Labeler { did: B.into(), redact: false }]);
        assert_eq!(got.header_value(), format!("{A};redact, {B}").as_str());
        assert!(AcceptLabelers::parse("").is_none());
        assert!(AcceptLabelers::parse("nothing, valid;redact").is_none());
    }

    #[test]
    fn bounds_the_count_and_the_size() {
        let many: Vec<String> = (0..40).map(|i| format!("did:plc:{i:0>24}")).collect();
        let got = AcceptLabelers::parse(&many.join(",")).expect("some");
        assert_eq!(got.entries().len(), MAX_LABELERS);
        assert_eq!(got.entries()[0].did, many[0]);
        let long = format!("{A},{}", "x".repeat(MAX_HEADER_BYTES));
        assert!(AcceptLabelers::parse(&long).is_none());
        // Several header lines are read together, under the same bound.
        let mut h = HeaderMap::new();
        h.append(ACCEPT_LABELERS, HeaderValue::from_static(A));
        h.append(ACCEPT_LABELERS, HeaderValue::from_static(B));
        assert_eq!(AcceptLabelers::from_headers(&h).expect("both").entries().len(), 2);
        h.append(ACCEPT_LABELERS, HeaderValue::from_str(&"y".repeat(MAX_HEADER_BYTES)).expect("ascii"));
        assert!(AcceptLabelers::from_headers(&h).is_none());
        assert!(AcceptLabelers::from_headers(&HeaderMap::new()).is_none());
    }

    #[test]
    fn content_labelers_names_only_accepted_applied_labelers() {
        let scope = LabelerScope::new(AcceptLabelers::parse(&format!("{A};redact, {B}")).expect("two"));
        assert_eq!(scope.content_labelers(), None);
        let mut answer = HeaderMap::new();
        answer.insert(CONTENT_LABELERS, HeaderValue::from_str(&format!("{A};redact, did:plc:notasked")).expect("ascii"));
        scope.record_applied(&answer);
        assert_eq!(scope.content_labelers().expect("one"), format!("{A};redact").as_str());
        scope.mark_applied(B);
        scope.mark_applied("did:plc:notasked");
        assert_eq!(scope.content_labelers().expect("two"), format!("{A};redact, {B}").as_str());
    }
}
