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

use super::pipeline::{RecordAuthz, Signals};
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

    /// The same authorized view again, for the same reader in the same
    /// response: a post that is both a feed item and another item's reply
    /// context, or the subject of several notifications (fast-reads design
    /// §5.2). It is never a view for another reader.
    pub(in crate::private_tier) fn duplicate(&self) -> Self {
        Self(self.0.clone())
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

/// `app.nearhorizon.private.defs#bookmarkView` (private posting §4.3): one
/// of the caller's bookmark rows and its post, read through the pipeline.
#[derive(Debug)]
pub struct BookmarkView {
    pub(in crate::private_tier) created_at: String,
    pub(in crate::private_tier) uri: String,
    pub(in crate::private_tier) cid: String,
    pub(in crate::private_tier) item: PrivateEmit<Value>,
}

impl Serialize for BookmarkView {
    fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        let mut m = s.serialize_map(Some(3))?;
        m.serialize_entry("createdAt", &self.created_at)?;
        m.serialize_entry("subject", &serde_json::json!({"uri": self.uri, "cid": self.cid}))?;
        m.serialize_entry("item", &self.item)?;
        m.end()
    }
}

/// One item of `app.nearhorizon.private.getQuotes` (private posting §4.7),
/// each with its `$type`: a private quoting post read through the pipeline,
/// or a public one as `#publicPostRef {uri, cid}` (C-3).
#[derive(Debug)]
pub enum QuoteItem {
    Private(PrivateEmit<Value>),
    Public { uri: String, cid: String },
}

impl Serialize for QuoteItem {
    fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        let mut m = s.serialize_map(None)?;
        match self {
            Self::Private(post) => {
                m.serialize_entry("$type", "app.nearhorizon.private.defs#privatePostView")?;
                if let Value::Object(view) = post.view() {
                    for (k, v) in view {
                        m.serialize_entry(k, v)?;
                    }
                }
            }
            Self::Public { uri, cid } => {
                m.serialize_entry("$type", "app.nearhorizon.private.defs#publicPostRef")?;
                m.serialize_entry("uri", uri)?;
                m.serialize_entry("cid", cid)?;
            }
        }
        m.end()
    }
}

/// One side of a feed item's reply context (fast-reads design §5.2 B-2),
/// each with its `$type`: a private post read for the caller through the
/// pipeline, `app.bsky.feed.defs#notFoundPost` for any private post that
/// did not read (the same for every reason), or a public post by reference.
#[derive(Debug)]
pub enum ContextPost {
    Private(PrivateEmit<Value>),
    NotFound(String),
    Public { uri: String, cid: String },
}

impl Serialize for ContextPost {
    fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        let mut m = s.serialize_map(None)?;
        match self {
            Self::Private(post) => {
                m.serialize_entry("$type", "app.nearhorizon.private.defs#privatePostView")?;
                if let Value::Object(view) = post.view() {
                    for (k, v) in view {
                        m.serialize_entry(k, v)?;
                    }
                }
            }
            Self::NotFound(uri) => {
                m.serialize_entry("$type", "app.bsky.feed.defs#notFoundPost")?;
                m.serialize_entry("uri", uri)?;
                m.serialize_entry("notFound", &true)?;
            }
            Self::Public { uri, cid } => {
                m.serialize_entry("$type", "app.nearhorizon.private.defs#publicPostRef")?;
                m.serialize_entry("uri", uri)?;
                m.serialize_entry("cid", cid)?;
            }
        }
        m.end()
    }
}

/// `app.nearhorizon.private.defs#replyRef`: a feed item's root and parent.
#[derive(Debug)]
pub struct ReplyContext {
    pub(in crate::private_tier) root: ContextPost,
    pub(in crate::private_tier) parent: ContextPost,
}

impl Serialize for ReplyContext {
    fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        let mut m = s.serialize_map(Some(2))?;
        m.serialize_entry("root", &self.root)?;
        m.serialize_entry("parent", &self.parent)?;
        m.end()
    }
}

/// One item of `getTimeline` or `getAuthorFeed`: the post's view, and its
/// reply context when its post is a reply.
#[derive(Debug)]
pub struct FeedItem {
    pub(in crate::private_tier) post: PrivateEmit<Value>,
    pub(in crate::private_tier) reply: Option<ReplyContext>,
}

impl Serialize for FeedItem {
    fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        let mut m = s.serialize_map(None)?;
        if let Value::Object(view) = self.post.view() {
            for (k, v) in view {
                m.serialize_entry(k, v)?;
            }
        }
        if let Some(reply) = &self.reply {
            m.serialize_entry("reply", reply)?;
        }
        m.end()
    }
}

/// What a private notification is about (private posting §4.8).
#[derive(Debug)]
pub enum NotificationRecord {
    /// A private post, read through the pipeline for the recipient: its
    /// `uri`, `cid`, `record` and `labels` come from the witness's view.
    Private(PrivateEmit<Value>),
    /// A member's public like, repost, quote or reply: the stored public
    /// record, and its labels.
    Public { uri: String, cid: String, record: Value, labels: Value },
}

/// One item of `app.nearhorizon.private.listNotifications`, in the shape of
/// `app.bsky.notification.listNotifications#notification`.
#[derive(Debug)]
pub struct NotificationView {
    pub(in crate::private_tier) record: NotificationRecord,
    /// The record author's `profileView` (public data).
    pub(in crate::private_tier) author: Value,
    pub(in crate::private_tier) reason: String,
    pub(in crate::private_tier) reason_subject: Option<String>,
    /// The private post `reason_subject` names, read for the recipient
    /// (fast-reads design §5.2 B-3); absent when it did not read.
    pub(in crate::private_tier) reason_subject_post: Option<PrivateEmit<Value>>,
    pub(in crate::private_tier) is_read: bool,
    pub(in crate::private_tier) indexed_at: String,
}

impl Serialize for NotificationView {
    fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        match &self.record {
            NotificationRecord::Private(post) => {
                let v = post.view();
                self.serialize_with(s, &v["uri"], &v["cid"], &v["record"], v.get("labels").unwrap_or(&Value::Null))
            }
            NotificationRecord::Public { uri, cid, record, labels } => {
                self.serialize_with(s, &Value::from(uri.as_str()), &Value::from(cid.as_str()), record, labels)
            }
        }
    }
}

impl NotificationView {
    fn serialize_with<S: serde::Serializer>(
        &self,
        s: S,
        uri: &Value,
        cid: &Value,
        record: &Value,
        labels: &Value,
    ) -> Result<S::Ok, S::Error> {
        let mut m = s.serialize_map(None)?;
        m.serialize_entry("uri", uri)?;
        m.serialize_entry("cid", cid)?;
        m.serialize_entry("author", &self.author)?;
        m.serialize_entry("reason", &self.reason)?;
        if let Some(subject) = &self.reason_subject {
            m.serialize_entry("reasonSubject", subject)?;
        }
        if let Some(post) = &self.reason_subject_post {
            m.serialize_entry("reasonSubjectPost", post)?;
        }
        m.serialize_entry("record", record)?;
        m.serialize_entry("isRead", &self.is_read)?;
        m.serialize_entry("indexedAt", &self.indexed_at)?;
        if labels.is_null() {
            m.serialize_entry("labels", &Value::Array(Vec::new()))?;
        } else {
            m.serialize_entry("labels", labels)?;
        }
        m.end()
    }
}

/// The body of every private handler response, and its top-level signals
/// (degraded-mode design §3.2).
#[derive(Debug)]
pub struct PrivateResponse {
    body: Body,
    signals: Signals,
}

/// What a private handler answers.
#[derive(Debug)]
enum Body {
    Posts(Vec<PrivateEmit<Value>>),
    Thread(ThreadNode),
    Feed { feed: Vec<FeedItem>, cursor: Option<String> },
    Bookmarks { bookmarks: Vec<BookmarkView>, cursor: Option<String> },
    /// `getLikes`: `likes` are `{indexedAt, createdAt, actor}` of member
    /// likers, public data.
    Likes { uri: String, likes: Vec<Value>, cursor: Option<String> },
    /// `getRepostedBy`: `profileView`s of member reposters, public data.
    RepostedBy { uri: String, reposted_by: Vec<Value>, cursor: Option<String> },
    Quotes { uri: String, posts: Vec<QuoteItem>, cursor: Option<String> },
    /// `getReplies`: one `#privateThreadView` a reply, each with its own
    /// expanded replies (private posting §4.12).
    Replies { replies: Vec<ThreadNode>, cursor: Option<String> },
    /// `listNotifications`: the caller's private notifications.
    Notifications { notifications: Vec<NotificationView>, cursor: Option<String>, seen_at: Option<String> },
    /// `getUnreadCount`.
    UnreadCount(i64),
    /// `getNotificationPreferences` and `putNotificationPreferences`: the
    /// caller's own five preferences, never anything from a post (private
    /// notification preferences design §4).
    NotificationPreferences(Value),
    /// `searchPosts` (private posting §4.17): the page's matches, and the
    /// sealed cursor while the search has span left.
    Search { posts: Vec<PrivateEmit<Value>>, cursor: Option<String> },
    /// `{}`: a write's answer.
    Empty,
}

impl PrivateResponse {
    fn of(body: Body) -> Self {
        Self { body, signals: Signals::default() }
    }

    /// The same response with `signals` at its top level.
    pub(in crate::private_tier) fn with_signals(mut self, signals: Signals) -> Self {
        self.signals = signals;
        self
    }

    /// Its top-level signals.
    pub(in crate::private_tier) fn signals(&self) -> &Signals {
        &self.signals
    }

    pub(in crate::private_tier) fn notifications(
        notifications: Vec<NotificationView>,
        cursor: Option<String>,
        seen_at: Option<String>,
    ) -> Self {
        Self::of(Body::Notifications { notifications, cursor, seen_at })
    }

    pub(in crate::private_tier) fn unread_count(count: i64) -> Self {
        Self::of(Body::UnreadCount(count))
    }

    pub(in crate::private_tier) fn notification_preferences(prefs: Value) -> Self {
        Self::of(Body::NotificationPreferences(prefs))
    }

    pub(in crate::private_tier) fn search(posts: Vec<PrivateEmit<Value>>, cursor: Option<String>) -> Self {
        Self::of(Body::Search { posts, cursor })
    }

    pub(in crate::private_tier) fn likes(uri: String, likes: Vec<Value>, cursor: Option<String>) -> Self {
        Self::of(Body::Likes { uri, likes, cursor })
    }

    pub(in crate::private_tier) fn reposted_by(uri: String, reposted_by: Vec<Value>, cursor: Option<String>) -> Self {
        Self::of(Body::RepostedBy { uri, reposted_by, cursor })
    }

    pub(in crate::private_tier) fn quotes(uri: String, posts: Vec<QuoteItem>, cursor: Option<String>) -> Self {
        Self::of(Body::Quotes { uri, posts, cursor })
    }

    pub(in crate::private_tier) fn replies(replies: Vec<ThreadNode>, cursor: Option<String>) -> Self {
        Self::of(Body::Replies { replies, cursor })
    }

    pub(in crate::private_tier) fn bookmarks(bookmarks: Vec<BookmarkView>, cursor: Option<String>) -> Self {
        Self::of(Body::Bookmarks { bookmarks, cursor })
    }

    pub(in crate::private_tier) fn empty() -> Self {
        Self::of(Body::Empty)
    }

    pub(in crate::private_tier) fn posts(posts: Vec<PrivateEmit<Value>>) -> Self {
        Self::of(Body::Posts(posts))
    }

    pub(in crate::private_tier) fn thread(anchor: ThreadNode) -> Self {
        Self::of(Body::Thread(anchor))
    }

    pub(in crate::private_tier) fn feed(feed: Vec<FeedItem>, cursor: Option<String>) -> Self {
        Self::of(Body::Feed { feed, cursor })
    }

    /// A page with no items and a cursor (fast-reads design §5.2, C-11).
    /// Never true for a response that is not a page.
    pub(in crate::private_tier) fn is_empty_page(&self) -> bool {
        let (empty, cursor) = match &self.body {
            Body::Feed { feed, cursor } => (feed.is_empty(), cursor),
            Body::Bookmarks { bookmarks, cursor } => (bookmarks.is_empty(), cursor),
            Body::Likes { likes, cursor, .. } => (likes.is_empty(), cursor),
            Body::RepostedBy { reposted_by, cursor, .. } => (reposted_by.is_empty(), cursor),
            Body::Quotes { posts, cursor, .. } => (posts.is_empty(), cursor),
            Body::Replies { replies, cursor } => (replies.is_empty(), cursor),
            Body::Notifications { notifications, cursor, .. } => (notifications.is_empty(), cursor),
            Body::Search { posts, cursor } => (posts.is_empty(), cursor),
            Body::Posts(_) | Body::Thread(_) | Body::UnreadCount(_) | Body::NotificationPreferences(_) | Body::Empty => {
                return false;
            }
        };
        empty && cursor.is_some()
    }
}

impl Serialize for PrivateResponse {
    fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        let mut m = s.serialize_map(None)?;
        match &self.body {
            Body::Posts(posts) => m.serialize_entry("posts", posts)?,
            Body::Thread(t) => m.serialize_entry("thread", t)?,
            Body::Feed { feed, cursor } => {
                m.serialize_entry("feed", feed)?;
                if let Some(c) = cursor {
                    m.serialize_entry("cursor", c)?;
                }
            }
            Body::Bookmarks { bookmarks, cursor } => {
                m.serialize_entry("bookmarks", bookmarks)?;
                if let Some(c) = cursor {
                    m.serialize_entry("cursor", c)?;
                }
            }
            Body::Likes { uri, likes, cursor } => {
                m.serialize_entry("uri", uri)?;
                if let Some(c) = cursor {
                    m.serialize_entry("cursor", c)?;
                }
                m.serialize_entry("likes", likes)?;
            }
            Body::RepostedBy { uri, reposted_by, cursor } => {
                m.serialize_entry("uri", uri)?;
                if let Some(c) = cursor {
                    m.serialize_entry("cursor", c)?;
                }
                m.serialize_entry("repostedBy", reposted_by)?;
            }
            Body::Quotes { uri, posts, cursor } => {
                m.serialize_entry("uri", uri)?;
                if let Some(c) = cursor {
                    m.serialize_entry("cursor", c)?;
                }
                m.serialize_entry("posts", posts)?;
            }
            Body::Replies { replies, cursor } => {
                m.serialize_entry("replies", replies)?;
                if let Some(c) = cursor {
                    m.serialize_entry("cursor", c)?;
                }
            }
            Body::Notifications { notifications, cursor, seen_at } => {
                m.serialize_entry("notifications", notifications)?;
                if let Some(c) = cursor {
                    m.serialize_entry("cursor", c)?;
                }
                if let Some(at) = seen_at {
                    m.serialize_entry("seenAt", at)?;
                }
            }
            Body::UnreadCount(count) => m.serialize_entry("count", count)?,
            Body::NotificationPreferences(prefs) => m.serialize_entry("preferences", prefs)?,
            Body::Search { posts, cursor } => {
                m.serialize_entry("posts", posts)?;
                if let Some(c) = cursor {
                    m.serialize_entry("cursor", c)?;
                }
            }
            Body::Empty => {}
        }
        // Degraded-mode design §3.2: the global signal while degraded, and
        // the caller's own transient misses; absent otherwise.
        if self.signals.degraded {
            m.serialize_entry("degraded", &true)?;
            if let Some(secs) = self.signals.retry_after {
                m.serialize_entry("retryAfter", &secs)?;
            }
        }
        if !self.signals.unavailable.is_empty() {
            let items: Vec<Value> =
                self.signals.unavailable.iter().map(|(uri, reason)| serde_json::json!({"uri": uri, "reason": reason})).collect();
            m.serialize_entry("unavailable", &items)?;
        }
        m.end()
    }
}
