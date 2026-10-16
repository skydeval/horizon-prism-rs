//! `PrivateEmit` and `PrivateResponse` (design-v2 §7.2).
//!
//! - **Per item.** `PrivateEmit::new(&RecordAuthz, view)` wraps **one**
//!   private record's view and asserts `view.uri == rauth.resource`.
//! - **Per response.** `PrivateResponse` is the only type the private
//!   handlers return. It is built from `PrivateEmit` items and, for a thread,
//!   a public parent obtained from `hydration` as a `PublicEmit`.
//!
//! Both constructors are `pub(in crate::private_tier)`. No `From` conversion
//! exists from any private type into a public one.

use serde::Serialize;
use serde::ser::SerializeMap;
use serde_json::Value;

use super::pipeline::RecordAuthz;
use crate::hydration::PublicEmit;

/// One authorized private record's view.
#[derive(Debug)]
pub struct PrivateEmit<T>(T);

impl PrivateEmit<Value> {
    /// # Panics
    ///
    /// If the view's `uri` is not the witness's resource: a witness for one
    /// record can never emit another.
    pub(in crate::private_tier) fn new(rauth: &RecordAuthz, view: Value) -> Self {
        assert_eq!(
            view.get("uri").and_then(Value::as_str),
            Some(rauth.uri().as_str()),
            "PrivateEmit view does not match its RecordAuthz resource"
        );
        Self(view)
    }

    /// The authorized view (thread assembly reads its reply ref).
    pub(in crate::private_tier) fn view(&self) -> &Value {
        &self.0
    }

    /// Add `key` to the view after the page's reads (private-posting design
    /// §3.3): a value computed for a URI that is already a hit, such as the
    /// record part of `embed` or the operator labels. Content of another
    /// private record reaches it only through that record's own
    /// `PrivateEmit`.
    ///
    /// # Panics
    ///
    /// On `uri`, `cid`, `author`, `record` or `indexedAt`: what the witness
    /// emitted is never replaced.
    pub(in crate::private_tier) fn decorate(&mut self, key: &str, value: Value) {
        assert!(
            !matches!(key, "uri" | "cid" | "author" | "record" | "indexedAt"),
            "PrivateEmit::decorate may not replace {key}"
        );
        if let Value::Object(view) = &mut self.0 {
            view.insert(key.to_string(), value);
        }
    }
}

impl<T: Serialize> Serialize for PrivateEmit<T> {
    fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        self.0.serialize(s)
    }
}

/// A private thread node: a private post, its parent (private node or public
/// parent) and its replies.
#[derive(Debug)]
pub struct ThreadNode {
    pub(in crate::private_tier) post: PrivateEmit<Value>,
    pub(in crate::private_tier) parent: Option<Box<ThreadParent>>,
    pub(in crate::private_tier) replies: Option<Vec<ThreadNode>>,
}

/// A thread node's parent.
#[derive(Debug)]
pub enum ThreadParent {
    Private(ThreadNode),
    /// `app.nearhorizon.private.defs#publicParent`.
    Public(PublicEmit<Value>),
}

impl Serialize for ThreadNode {
    fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        let mut m = s.serialize_map(None)?;
        m.serialize_entry("$type", "app.nearhorizon.private.defs#privateThreadView")?;
        m.serialize_entry("post", &self.post)?;
        if let Some(parent) = &self.parent {
            m.serialize_entry("parent", parent.as_ref())?;
        }
        if let Some(replies) = &self.replies {
            m.serialize_entry("replies", replies)?;
        }
        m.end()
    }
}

impl Serialize for ThreadParent {
    fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        match self {
            Self::Private(node) => node.serialize(s),
            Self::Public(post) => {
                let mut m = s.serialize_map(Some(2))?;
                m.serialize_entry("$type", "app.nearhorizon.private.defs#publicParent")?;
                m.serialize_entry("post", post)?;
                m.end()
            }
        }
    }
}

/// The body of every private handler response.
#[derive(Debug)]
pub enum PrivateResponse {
    Posts(Vec<PrivateEmit<Value>>),
    Thread(ThreadNode),
    Feed { feed: Vec<PrivateEmit<Value>>, cursor: Option<String> },
}

impl PrivateResponse {
    pub(in crate::private_tier) fn posts(posts: Vec<PrivateEmit<Value>>) -> Self {
        Self::Posts(posts)
    }

    pub(in crate::private_tier) fn thread(anchor: ThreadNode) -> Self {
        Self::Thread(anchor)
    }

    pub(in crate::private_tier) fn feed(feed: Vec<PrivateEmit<Value>>, cursor: Option<String>) -> Self {
        Self::Feed { feed, cursor }
    }
}

impl Serialize for PrivateResponse {
    fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        let mut m = s.serialize_map(None)?;
        match self {
            Self::Posts(posts) => m.serialize_entry("posts", posts)?,
            Self::Thread(t) => m.serialize_entry("thread", t)?,
            Self::Feed { feed, cursor } => {
                m.serialize_entry("feed", feed)?;
                if let Some(c) = cursor {
                    m.serialize_entry("cursor", c)?;
                }
            }
        }
        m.end()
    }
}
