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
//! - the response names, in `atproto-content-labelers`, the labelers every
//!   upstream answer of the request said it applied (the intersection of
//!   the answers), and Prism's own, which Prism applies itself
//!   (threadlens-quoted-posts design §3).
//!
//! Every labeler is forwarded, moderation or not: a labeler is a labeler.
//! The labels only add to views; Prism's gate and Thread Lens's rules decide
//! what is shown, as before.

use std::collections::HashSet;
use std::sync::atomic::{AtomicUsize, Ordering};
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

/// `threadlens_labelers_unapplied_total{network}`: scoped upstream answers
/// that left out a labeler the request accepted (threadlens-quoted-posts
/// design §3). Prism's own labeler is not expected from an upstream.
pub const METRIC_LABELERS_UNAPPLIED: &str = "threadlens_labelers_unapplied_total";

/// One request's labelers, and what its upstream calls reported.
#[derive(Debug)]
pub struct LabelerScope {
    accept: AcceptLabelers,
    value: HeaderValue,
    /// Prism's own labeler DID, given when the scope is created.
    own: String,
    /// The accepted labelers every scoped upstream answer named as applied:
    /// `None` until the first answer, then the intersection.
    upstream_applied: Mutex<Option<HashSet<String>>>,
    /// The accepted labelers Prism applied itself.
    own_applied: Mutex<HashSet<String>>,
    /// Scoped answers that left out an accepted labeler.
    unapplied_answers: AtomicUsize,
    /// Post URIs whose upstream view, fetched with the header, was
    /// normalized for this request.
    labelled: Mutex<HashSet<String>>,
}

impl LabelerScope {
    /// The scope of one request. `own` is Prism's own labeler DID: its
    /// labels are on every view Prism serves, so it is applied from the
    /// start when the request accepts it.
    #[must_use]
    pub fn new(accept: AcceptLabelers, own: &str) -> Arc<Self> {
        let value = accept.header_value();
        let own_applied: HashSet<String> = accept.contains(own).then(|| own.to_string()).into_iter().collect();
        Arc::new(Self {
            accept,
            value,
            own: own.to_string(),
            upstream_applied: Mutex::new(None),
            own_applied: Mutex::new(own_applied),
            unapplied_answers: AtomicUsize::new(0),
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

    /// Record a scoped upstream answer's `atproto-content-labelers`: only
    /// the labelers the request accepted count, and a labeler counts as
    /// applied upstream only while every answer named it (the
    /// intersection). Returns whether the answer left out an accepted
    /// labeler other than Prism's own.
    pub fn record_applied(&self, response: &HeaderMap) -> bool {
        let named: HashSet<String> = named_in(response).into_iter().filter(|did| self.accept.contains(did)).collect();
        let missed = self.accept.entries.iter().any(|e| e.did != self.own && !named.contains(&e.did));
        let mut applied = self.upstream_applied.lock().unwrap_or_else(PoisonError::into_inner);
        *applied = Some(match applied.take() {
            None => named,
            Some(before) => before.intersection(&named).cloned().collect(),
        });
        if missed {
            self.unapplied_answers.fetch_add(1, Ordering::Relaxed);
        }
        missed
    }

    /// Mark a labeler applied by Prism itself (its own labels are always
    /// on its views).
    pub fn mark_applied(&self, did: &str) {
        if self.accept.contains(did) {
            self.own_applied.lock().unwrap_or_else(PoisonError::into_inner).insert(did.to_string());
        }
    }

    /// The scoped answers of this request that left out an accepted
    /// labeler (each also counted on [`METRIC_LABELERS_UNAPPLIED`]).
    #[must_use]
    pub fn unapplied_answers(&self) -> usize {
        self.unapplied_answers.load(Ordering::Relaxed)
    }

    /// The accepted labelers counted as applied: those every scoped
    /// upstream answer named, and Prism's own.
    fn applied(&self) -> HashSet<String> {
        let mut out = self.own_applied.lock().unwrap_or_else(PoisonError::into_inner).clone();
        if let Some(upstream) = &*self.upstream_applied.lock().unwrap_or_else(PoisonError::into_inner) {
            out.extend(upstream.iter().cloned());
        }
        out
    }

    /// Whether every labeler the request accepted counts as applied
    /// (threadlens-quoted-posts design §2 condition 1).
    #[must_use]
    pub fn all_applied(&self) -> bool {
        let applied = self.applied();
        self.accept.entries.iter().all(|e| applied.contains(&e.did))
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
        let applied = self.applied();
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

    const OWN: &str = "did:web:prism.example.com";

    fn answer(named: &str) -> HeaderMap {
        let mut h = HeaderMap::new();
        if !named.is_empty() {
            h.insert(CONTENT_LABELERS, HeaderValue::from_str(named).expect("ascii"));
        }
        h
    }

    #[test]
    fn content_labelers_names_only_accepted_applied_labelers() {
        let scope = LabelerScope::new(AcceptLabelers::parse(&format!("{A};redact, {B}")).expect("two"), OWN);
        assert_eq!(scope.content_labelers(), None);
        assert!(scope.record_applied(&answer(&format!("{A};redact, did:plc:notasked"))), "B left out");
        assert_eq!(scope.content_labelers().expect("one"), format!("{A};redact").as_str());
        assert!(!scope.all_applied());
        scope.mark_applied(B);
        scope.mark_applied("did:plc:notasked");
        assert_eq!(scope.content_labelers().expect("two"), format!("{A};redact, {B}").as_str());
        assert!(scope.all_applied());
    }

    /// T-Q2 (threadlens-quoted-posts design §3): a labeler is applied only
    /// while every scoped answer named it. One answer naming both and one
    /// naming A only: the header names A, not everything is applied, and
    /// one answer is counted as having left one out.
    #[test]
    fn applied_is_the_intersection_of_the_answers() {
        let scope = LabelerScope::new(AcceptLabelers::parse(&format!("{A};redact, {B}")).expect("two"), OWN);
        assert!(!scope.record_applied(&answer(&format!("{A};redact, {B}"))));
        assert!(scope.all_applied());
        assert_eq!(scope.content_labelers().expect("both"), format!("{A};redact, {B}").as_str());
        assert!(scope.record_applied(&answer(A)));
        assert_eq!(scope.content_labelers().expect("one"), format!("{A};redact").as_str());
        assert!(!scope.all_applied());
        assert_eq!(scope.unapplied_answers(), 1);
        // An answer naming both again does not bring B back.
        assert!(!scope.record_applied(&answer(&format!("{A}, {B}"))));
        assert_eq!(scope.content_labelers().expect("one"), format!("{A};redact").as_str());
        // A header-less answer empties the upstream part.
        assert!(scope.record_applied(&answer("")));
        assert_eq!(scope.content_labelers(), None);
        assert_eq!(scope.unapplied_answers(), 2);
    }

    /// T-Q10 and T-Q16 (§3): Prism's own labeler is applied from the
    /// scope's creation, is not expected from an upstream, and stays
    /// applied whatever the later answers name.
    #[test]
    fn prisms_own_labeler_is_applied_from_the_start() {
        let scope = LabelerScope::new(AcceptLabelers::parse(&format!("{A}, {OWN}")).expect("two"), OWN);
        assert_eq!(scope.content_labelers().expect("own"), OWN);
        // The upstream names A and not Prism's own: everything is applied,
        // and the answer left nothing out.
        assert!(!scope.record_applied(&answer(A)));
        assert!(scope.all_applied());
        assert_eq!(scope.content_labelers().expect("both"), format!("{A}, {OWN}").as_str());
        // A label batch's answer without the header narrows the upstream
        // part only.
        assert!(scope.record_applied(&answer("")));
        assert_eq!(scope.content_labelers().expect("own"), OWN);
        assert!(!scope.all_applied());
        // With Prism's own not accepted, it is never named.
        let other = LabelerScope::new(AcceptLabelers::parse(A).expect("one"), OWN);
        assert_eq!(other.content_labelers(), None);
        other.mark_applied(OWN);
        assert_eq!(other.content_labelers(), None);
    }
}
