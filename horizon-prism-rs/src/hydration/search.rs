//! Post search (design-v3 §9.0 – §9.2): `app.bsky.feed.searchPosts` and
//! `app.bsky.feed.searchPostsV2`, two parameter parsers over one query
//! builder (locked decision 34).
//!
//! - **Corpus:** rows of `posts` whose author is in scope. Private-tier
//!   posts are never in `posts`.
//! - **Text:** `search_tsv @@ websearch_to_tsquery('simple', text)`, which
//!   accepts quoted phrases, `OR` and `-term` and never raises on input.
//! - **Inline operators** in the text (`from:`, `to:`, `mentions:`,
//!   `lang:`, `domain:`, `since:`, `until:`, `#tag`) are taken out of it
//!   and joined to the explicit parameters with AND.
//! - **Handles resolve locally only**: search never makes an outbound
//!   lookup. A handle that does not resolve matches nothing.
//! - **Private-tier values** in a filter are decided from the collection
//!   alone, so a search cannot tell whether a private URI is referenced.
//! - **Budget:** one transaction under a statement timeout; `hitsTotal`
//!   after the page, under a savepoint with a timeout of its own.
//!
//! The viewer's own mutes and blocks are applied in the query, and again
//! with inbound list blocks and the moderation gate after hydration, so a
//! page can be shorter than its limit. The cursor follows the scan.

use chrono::{DateTime, NaiveDate, NaiveDateTime, TimeDelta, Utc};
use serde_json::{Value, json};
use sqlx::{Acquire, Postgres, QueryBuilder};

use super::feed::{local_cursor, parse_local_cursor};
use super::normalize::is_private_uri;
use super::{APPLIES, CountMode, Hydrator, ViewError, ViewResult, Viewer};
use crate::config::CountsSource;
use crate::db::locks::uri_author;
use crate::firehose::extract::{normalize_domain, normalize_tag, normalize_url};
use crate::moderation::Gate;
use crate::xrpc::pools::Requester;

/// `search_timeout_total{endpoint}`: searches stopped by the statement
/// timeout.
pub const METRIC_TIMEOUT: &str = "search_timeout_total";

/// The lexicon's error for a query that cannot be run.
const BAD_QUERY: &str = "BadQueryString";

fn bad(message: impl Into<String>) -> ViewError {
    ViewError::new(BAD_QUERY, message)
}

/// Whether a database error is the statement timeout (SQLSTATE `57014`).
pub(crate) fn timed_out(error: &sqlx::Error) -> bool {
    matches!(error, sqlx::Error::Database(db) if db.code().as_deref() == Some("57014"))
}

/// Start a search's budget in `conn`'s transaction. `SET LOCAL` cannot
/// take a bind parameter and does nothing outside a transaction, so
/// `set_config` is used, with the value bound as text.
pub(crate) async fn set_timeout(conn: &mut sqlx::PgConnection, ms: u64) -> Result<(), sqlx::Error> {
    sqlx::query("SELECT set_config('statement_timeout', $1, true)")
        .bind(ms.to_string())
        .execute(conn)
        .await
        .map(|_| ())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Sort {
    Latest,
    Top,
}

/// How an array column is matched against the values given.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Match {
    /// The row holds every value.
    All,
    /// The row holds at least one.
    Any,
    /// The row holds none; a row with no array is kept.
    None,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum Filter {
    Text(String),
    Authors(Vec<String>),
    NotAuthors(Vec<String>),
    Array(&'static str, Match, Vec<String>),
    Quotes(Vec<String>),
    NotQuotes(Vec<String>),
    HasMedia,
    HasVideo,
    ReplyParent(String),
    ThreadRoot(String),
    RepliesOnly,
    NoReplies,
    Following,
    /// A filter no row can meet.
    Nothing,
}

/// The parameters of either endpoint, as the handler read them. V1's
/// single-valued parameters arrive as arrays of one.
#[derive(Debug, Clone, Default)]
pub struct PostSearchParams {
    pub v2: bool,
    pub query: Option<String>,
    pub sort: Option<String>,
    pub since: Option<String>,
    pub until: Option<String>,
    pub all_time: bool,
    pub authors: Vec<String>,
    pub exclude_authors: Vec<String>,
    pub mentions: Vec<String>,
    pub exclude_mentions: Vec<String>,
    pub domains: Vec<String>,
    pub exclude_domains: Vec<String>,
    pub urls: Vec<String>,
    pub exclude_urls: Vec<String>,
    pub embedded: Vec<String>,
    pub exclude_embedded: Vec<String>,
    pub hashtags: Vec<String>,
    pub exclude_hashtags: Vec<String>,
    pub languages: Vec<String>,
    pub exclude_languages: Vec<String>,
    pub has_media: bool,
    pub has_video: bool,
    pub reply_parent: Option<String>,
    pub thread_root: Option<String>,
    pub replies_only: bool,
    pub exclude_replies: bool,
    pub following: bool,
    pub limit: u32,
    pub cursor: Option<String>,
}

/// One array column's filters: the column, the values to include, the
/// inline operator values, the values to exclude, how the include
/// matches, and how a value is normalized.
type ArrayFilter<'a> = (&'static str, &'a [String], &'a [String], &'a [String], Match, &'a dyn Fn(&str) -> Option<String>);

/// A search ready to run.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Plan {
    filters: Vec<Filter>,
    sort: Sort,
    /// Inclusive lower bound on `created_at`.
    since: Option<DateTime<Utc>>,
    /// Exclusive upper bound.
    until: Option<DateTime<Utc>>,
}

/// The query text split at whitespace, a quoted phrase kept whole.
fn tokens(query: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut current = String::new();
    let mut quoted = false;
    for c in query.chars() {
        if c == '"' {
            quoted = !quoted;
            current.push(c);
        } else if c.is_whitespace() && !quoted {
            if !current.is_empty() {
                out.push(std::mem::take(&mut current));
            }
        } else {
            current.push(c);
        }
    }
    if !current.is_empty() {
        out.push(current);
    }
    out
}

/// A `since` or `until` value: an RFC 3339 datetime, a datetime with no
/// timezone (taken as UTC), or a date alone (00:00 UTC).
fn date(raw: &str) -> Option<DateTime<Utc>> {
    let raw = raw.trim();
    if let Ok(t) = DateTime::parse_from_rfc3339(raw) {
        return Some(t.with_timezone(&Utc));
    }
    if let Ok(t) = NaiveDateTime::parse_from_str(raw, "%Y-%m-%dT%H:%M:%S%.f") {
        return Some(t.and_utc());
    }
    NaiveDate::parse_from_str(raw, "%Y-%m-%d").ok()?.and_hms_opt(0, 0, 0).map(|t| t.and_utc())
}

/// Whether `raw` is a DID or a handle by syntax.
fn is_identifier(raw: &str) -> bool {
    let raw = raw.trim_start_matches('@');
    proto_blue::syntax::Did::is_valid(raw) || proto_blue::syntax::Handle::is_valid(raw)
}

/// What the query text holds beside its words.
#[derive(Debug, Default, PartialEq, Eq)]
struct Operators {
    words: Vec<String>,
    from: Vec<String>,
    mentions: Vec<String>,
    langs: Vec<String>,
    domains: Vec<String>,
    tags: Vec<String>,
    since: Option<DateTime<Utc>>,
    until: Option<DateTime<Utc>>,
}

/// Take the inline operators out of the query text. A malformed operator
/// value is the lexicon's `BadQueryString`.
fn operators(query: &str) -> Result<Operators, ViewError> {
    let mut out = Operators::default();
    for token in tokens(query) {
        if let Some(tag) = token.strip_prefix('#').and_then(normalize_tag) {
            out.tags.push(tag);
            continue;
        }
        let operator = (!token.starts_with('"')).then(|| token.split_once(':')).flatten();
        let Some((key, value)) = operator else {
            out.words.push(token);
            continue;
        };
        let malformed = || bad(format!("malformed operator: {key}:{value}"));
        match key.to_ascii_lowercase().as_str() {
            "from" | "to" | "mentions" => {
                if !is_identifier(value) {
                    return Err(malformed());
                }
                let list = if key.eq_ignore_ascii_case("from") { &mut out.from } else { &mut out.mentions };
                list.push(value.to_string());
            }
            "lang" => {
                if value.is_empty() || !value.chars().all(|c| c.is_ascii_alphanumeric() || c == '-') {
                    return Err(malformed());
                }
                out.langs.push(value.to_ascii_lowercase());
            }
            "domain" => out.domains.push(normalize_domain(value).filter(|_| !value.is_empty()).ok_or_else(malformed)?),
            "since" => {
                let at = date(value).ok_or_else(malformed)?;
                out.since = Some(out.since.map_or(at, |s| s.max(at)));
            }
            "until" => {
                let at = date(value).ok_or_else(malformed)?;
                out.until = Some(out.until.map_or(at, |u| u.min(at)));
            }
            _ => out.words.push(token),
        }
    }
    Ok(out)
}

/// The viewer's own mutes and blocks as a condition on `n`, with the
/// viewer as `$1`: a full mute, a block either way, and a list mute or
/// list block of the viewer's that applies (design-v3 §5.0).
pub(super) fn viewer_exclusions() -> String {
    format!(
        "NOT EXISTS (SELECT 1 FROM mutes m WHERE m.viewer_did = $1 AND m.subject_did = n.author_did AND m.scope = 'full') \
         AND NOT EXISTS (SELECT 1 FROM blocks b WHERE (b.author_did = $1 AND b.subject_did = n.author_did) \
                         OR (b.author_did = n.author_did AND b.subject_did = $1)) \
         AND NOT EXISTS (SELECT 1 FROM list_references r \
                         JOIN subscribed_lists l ON l.list_uri = r.list_uri \
                         JOIN subscribed_list_items i ON i.list_uri = r.list_uri \
                         WHERE r.member_did = $1 AND i.subject_did = n.author_did AND {APPLIES})"
    )
}

impl Plan {
    /// ` FROM posts n WHERE …`: the corpus, the exclusions and the filters.
    /// The viewer is the first bind, which the exclusions and `following`
    /// name as `$1`; so this must be the first thing that binds.
    fn push_from(&self, qb: &mut QueryBuilder<'_, Postgres>, viewer: Option<&str>, anonymous: bool) {
        qb.push(" FROM posts n WHERE (");
        qb.push_bind(viewer.map(str::to_string));
        qb.push("::text IS NULL OR (").push(viewer_exclusions()).push("))");
        qb.push(" AND EXISTS (SELECT 1 FROM scope_dids s WHERE s.did = n.author_did)");
        qb.push(" AND NOT EXISTS (SELECT 1 FROM actors a WHERE a.did = n.author_did AND a.account_status <> 'active')");
        if anonymous {
            qb.push(
                " AND NOT EXISTS (SELECT 1 FROM actors a WHERE a.did = n.author_did \
                   AND a.profile_record -> 'labels' -> 'values' @> '[{\"val\": \"!no-unauthenticated\"}]'::jsonb)",
            );
        }
        if let Some(since) = self.since {
            qb.push(" AND n.created_at >= ").push_bind(since);
        }
        if let Some(until) = self.until {
            qb.push(" AND n.created_at < ").push_bind(until);
        }
        for filter in &self.filters {
            qb.push(" AND ");
            match filter {
                Filter::Text(text) => {
                    qb.push("n.search_tsv @@ websearch_to_tsquery('simple', ").push_bind(text.clone()).push(")");
                }
                Filter::Authors(dids) => {
                    qb.push("n.author_did = ANY(").push_bind(dids.clone()).push(")");
                }
                Filter::NotAuthors(dids) => {
                    qb.push("n.author_did <> ALL(").push_bind(dids.clone()).push(")");
                }
                Filter::Array(column, Match::All, values) => {
                    qb.push(format!("n.{column} @> ")).push_bind(values.clone());
                }
                Filter::Array(column, Match::Any, values) => {
                    qb.push(format!("n.{column} && ")).push_bind(values.clone());
                }
                // `NOT (x && arr)` is NULL for a row with no array, which
                // would drop the row.
                Filter::Array(column, Match::None, values) => {
                    qb.push(format!("NOT COALESCE(n.{column} && ")).push_bind(values.clone()).push(", false)");
                }
                Filter::Quotes(uris) => {
                    qb.push("n.quote_uri = ANY(").push_bind(uris.clone()).push(")");
                }
                Filter::NotQuotes(uris) => {
                    qb.push("NOT COALESCE(n.quote_uri = ANY(").push_bind(uris.clone()).push("), false)");
                }
                Filter::HasMedia => {
                    qb.push("n.media_type IS NOT NULL");
                }
                Filter::HasVideo => {
                    qb.push("n.media_type = 'video'");
                }
                Filter::ReplyParent(uri) => {
                    qb.push("n.reply_parent_uri = ").push_bind(uri.clone());
                }
                Filter::ThreadRoot(uri) => {
                    qb.push("(n.reply_root_uri = ").push_bind(uri.clone()).push(" OR n.uri = ").push_bind(uri.clone()).push(")");
                }
                Filter::RepliesOnly => {
                    qb.push("n.reply_parent_uri IS NOT NULL");
                }
                Filter::NoReplies => {
                    qb.push("n.reply_parent_uri IS NULL");
                }
                Filter::Following => {
                    qb.push("n.author_did IN (SELECT f.subject_did FROM follows f WHERE f.author_did = $1)");
                }
                Filter::Nothing => {
                    qb.push("false");
                }
            }
        }
    }

    fn text(&self) -> Option<&str> {
        self.filters.iter().find_map(|f| match f {
            Filter::Text(text) => Some(text.as_str()),
            _ => None,
        })
    }
}

impl Hydrator {
    /// The DIDs that `identifiers` (DIDs or handles) name, resolved from
    /// this AppView's own rows only: a handle counts when its account is in
    /// scope or a member (design-v3 §9.0 "Handle resolution"). One that
    /// does not resolve is left out. A value that is neither a DID nor a
    /// handle is `InvalidRequest`.
    async fn local_dids(&self, identifiers: &[String]) -> Result<Result<Vec<String>, ViewError>, sqlx::Error> {
        let mut dids: Vec<String> = Vec::new();
        let mut handles: Vec<String> = Vec::new();
        for raw in identifiers {
            let ident = raw.trim().trim_start_matches('@');
            if proto_blue::syntax::Did::is_valid(ident) {
                dids.push(ident.to_string());
            } else if proto_blue::syntax::Handle::is_valid(ident) {
                handles.push(ident.to_ascii_lowercase());
            } else {
                return Ok(Err(ViewError::new("InvalidRequest", format!("{raw:?} is not a DID or a handle"))));
            }
        }
        if !handles.is_empty() {
            let found: Vec<String> = sqlx::query_scalar(
                "SELECT a.did FROM actors a WHERE a.handle = ANY($1) \
                   AND (EXISTS (SELECT 1 FROM scope_dids s WHERE s.did = a.did) \
                     OR EXISTS (SELECT 1 FROM members m WHERE m.did = a.did))",
            )
            .bind(&handles)
            .fetch_all(self.pool())
            .await?;
            dids.extend(found);
        }
        dids.sort();
        dids.dedup();
        Ok(Ok(dids))
    }

    /// Turn the parameters into a plan, or the lexicon error they earn.
    async fn search_plan(&self, viewer: &Viewer, p: &PostSearchParams, now: DateTime<Utc>) -> Result<Result<Plan, ViewError>, sqlx::Error> {
        let cfg = &self.inner.cfg.search;
        let max = usize::try_from(cfg.max_filter_values).unwrap_or(usize::MAX);
        let ops = match operators(p.query.as_deref().unwrap_or_default()) {
            Ok(ops) => ops,
            Err(e) => return Ok(Err(e)),
        };
        // Each array parameter, with the operator values that join it.
        let sizes = [
            p.authors.len() + ops.from.len(),
            p.exclude_authors.len(),
            p.mentions.len() + ops.mentions.len(),
            p.exclude_mentions.len(),
            p.domains.len() + ops.domains.len(),
            p.exclude_domains.len(),
            p.urls.len(),
            p.exclude_urls.len(),
            p.embedded.len(),
            p.exclude_embedded.len(),
            p.hashtags.len() + ops.tags.len(),
            p.exclude_hashtags.len(),
            p.languages.len() + ops.langs.len(),
            p.exclude_languages.len(),
        ];
        if sizes.iter().any(|n| *n > max) {
            return Ok(Err(bad(format!("a filter takes at most {max} values"))));
        }
        let sort = match (p.sort.as_deref(), p.v2) {
            (None, _) | (Some("latest"), false) | (Some("recent"), true) => Sort::Latest,
            (Some("top"), _) => Sort::Top,
            (Some(other), _) => return Ok(Err(ViewError::new("InvalidRequest", format!("unknown sort {other:?}")))),
        };
        let mut since = ops.since;
        let mut until = ops.until;
        for (raw, bound, later) in [(&p.since, &mut since, true), (&p.until, &mut until, false)] {
            let Some(raw) = raw else { continue };
            let Some(at) = date(raw) else {
                return Ok(Err(bad(format!("{raw:?} is not a datetime or a date"))));
            };
            *bound = Some(bound.map_or(at, |b| if later { b.max(at) } else { b.min(at) }));
        }

        let mut filters: Vec<Filter> = Vec::new();
        if !ops.words.is_empty() {
            // Nothing but negations would match nearly every post.
            if ops.words.iter().all(|w| w.starts_with('-')) {
                return Ok(Err(bad("a query needs at least one term that is not negated")));
            }
            filters.push(Filter::Text(ops.words.join(" ")));
        }

        // Accounts. An include set that resolves to nobody matches nothing.
        macro_rules! resolved {
            ($list:expr) => {
                match self.local_dids($list).await? {
                    Ok(dids) => dids,
                    Err(e) => return Ok(Err(e)),
                }
            };
        }
        let some_or_nothing = |values: Vec<String>, some: &dyn Fn(Vec<String>) -> Filter| {
            if values.is_empty() { Filter::Nothing } else { some(values) }
        };
        if !p.authors.is_empty() {
            filters.push(some_or_nothing(resolved!(&p.authors), &Filter::Authors));
        }
        for one in &ops.from {
            filters.push(some_or_nothing(resolved!(std::slice::from_ref(one)), &Filter::Authors));
        }
        let excluded = resolved!(&p.exclude_authors);
        if !excluded.is_empty() {
            filters.push(Filter::NotAuthors(excluded));
        }
        if !p.mentions.is_empty() {
            filters.push(some_or_nothing(resolved!(&p.mentions), &|v| Filter::Array("mention_dids", Match::Any, v)));
        }
        for one in &ops.mentions {
            filters.push(some_or_nothing(resolved!(std::slice::from_ref(one)), &|v| Filter::Array("mention_dids", Match::All, v)));
        }
        let excluded = resolved!(&p.exclude_mentions);
        if !excluded.is_empty() {
            filters.push(Filter::Array("mention_dids", Match::None, excluded));
        }

        // Links, tags and languages, each normalized as it is stored. A
        // value that cannot be stored (an at-uri as a `url`, say) cannot
        // match.
        let normalized = |values: &[String], f: &dyn Fn(&str) -> Option<String>| -> Vec<String> {
            let mut out: Vec<String> = values.iter().filter_map(|v| f(v)).collect();
            out.sort();
            out.dedup();
            out
        };
        let lang = |v: &str| Some(v.trim().to_lowercase()).filter(|l| !l.is_empty());
        // V1's `tag[]` is "all of"; V2's `hashtags[]` is "any of".
        let tags = if p.v2 { Match::Any } else { Match::All };
        let arrays: [ArrayFilter<'_>; 4] = [
            ("link_domains", &p.domains, &ops.domains, &p.exclude_domains, Match::Any, &normalize_domain),
            ("link_urls", &p.urls, &[], &p.exclude_urls, Match::Any, &normalize_url),
            ("tags", &p.hashtags, &ops.tags, &p.exclude_hashtags, tags, &normalize_tag),
            ("lang_prefixes", &p.languages, &ops.langs, &p.exclude_languages, Match::Any, &lang),
        ];
        for (column, include, inline, exclude, mode, normalize) in arrays {
            if !include.is_empty() {
                let values = normalized(include, normalize);
                // "All of" with a value that cannot match is nothing too.
                let short = mode == Match::All && values.len() < include.len();
                filters.push(if values.is_empty() || short { Filter::Nothing } else { Filter::Array(column, mode, values) });
            }
            for one in inline {
                filters.push(Filter::Array(column, Match::All, vec![one.clone()]));
            }
            let values = normalized(exclude, normalize);
            if !values.is_empty() {
                filters.push(Filter::Array(column, Match::None, values));
            }
        }

        // Record embeds and thread position. A private-tier value never
        // reaches a comparison.
        if !p.embedded.is_empty() {
            let public: Vec<String> = p.embedded.iter().filter(|u| !is_private_uri(u)).cloned().collect();
            filters.push(some_or_nothing(public, &Filter::Quotes));
        }
        let excluded: Vec<String> = p.exclude_embedded.iter().filter(|u| !is_private_uri(u)).cloned().collect();
        if !excluded.is_empty() {
            filters.push(Filter::NotQuotes(excluded));
        }
        if let Some(uri) = &p.reply_parent {
            filters.push(if is_private_uri(uri) { Filter::Nothing } else { Filter::ReplyParent(uri.clone()) });
        }
        if let Some(uri) = &p.thread_root {
            filters.push(if is_private_uri(uri) { Filter::Nothing } else { Filter::ThreadRoot(uri.clone()) });
        }
        if p.has_media {
            filters.push(Filter::HasMedia);
        }
        if p.has_video {
            filters.push(Filter::HasVideo);
        }
        match (p.replies_only, p.exclude_replies) {
            (true, true) => return Ok(Err(bad("repliesOnly and excludeReplies cannot both be set"))),
            (true, false) => filters.push(Filter::RepliesOnly),
            (false, true) => filters.push(Filter::NoReplies),
            (false, false) => {}
        }
        if p.following {
            if viewer.state_did().is_none() {
                return Ok(Err(ViewError::new("InvalidRequest", "following needs a signed-in member")));
            }
            filters.push(Filter::Following);
        }
        if filters.is_empty() {
            return Ok(Err(bad("a search needs a query or a filter")));
        }

        // The time window (design-v3 §9.0): an explicit `since` is the
        // lower bound; else V2's `allTime` has none; else it is the window
        // before `until`, or before now.
        if since.is_none() && !(p.v2 && p.all_time) {
            let days = if !p.v2 && sort == Sort::Top { cfg.top_window_days } else { cfg.recent_window_days };
            let end = until.map_or(now, |u| u.min(now));
            since = Some(end - TimeDelta::days(i64::from(days)));
        }
        Ok(Ok(Plan { filters, sort, since, until }))
    }

    /// `app.bsky.feed.searchPosts` and `searchPostsV2`.
    ///
    /// # Errors
    ///
    /// Database errors other than the search's own timeout, which is the
    /// lexicon's `BadQueryString`.
    pub async fn search_posts(&self, viewer: &Viewer, req: &Requester, p: &PostSearchParams) -> ViewResult<Value> {
        let cfg = self.inner.cfg.search.clone();
        let endpoint = if p.v2 { "searchPostsV2" } else { "searchPosts" };
        let plan = match self.search_plan(viewer, p, Utc::now()).await? {
            Ok(plan) => plan,
            Err(e) => return Ok(Err(e)),
        };
        let me = viewer.state_did();
        let anonymous = viewer.did.is_none();
        let limit = i64::from(p.limit.clamp(1, 100));
        let max_offset = i64::from(cfg.max_offset.min(cfg.top_candidates));

        let mut qb: QueryBuilder<'_, Postgres> = QueryBuilder::new("");
        let mut offset = 0;
        match plan.sort {
            Sort::Latest => {
                qb.push("SELECT n.uri, n.created_at");
                plan.push_from(&mut qb, me, anonymous);
                if let (Some(at), Some(uri)) = parse_local_cursor(p.cursor.as_deref()) {
                    qb.push(" AND (n.created_at, n.uri) < (").push_bind(at).push(", ").push_bind(uri).push(")");
                }
                qb.push(" ORDER BY n.created_at DESC, n.uri DESC LIMIT ").push_bind(limit);
            }
            // "Top among the most recent matches": only the newest
            // candidates are scored, from stored counts. No call is made.
            Sort::Top => {
                offset = p.cursor.as_deref().and_then(|c| c.parse::<i64>().ok()).unwrap_or(0).max(0);
                if offset > max_offset {
                    return Ok(Ok(Self::emit(json!({ "posts": [] }))));
                }
                qb.push("WITH c AS (SELECT n.uri, n.created_at, n.search_tsv");
                plan.push_from(&mut qb, me, anonymous);
                qb.push(" ORDER BY n.created_at DESC, n.uri DESC LIMIT ").push_bind(i64::from(cfg.top_candidates));
                qb.push(
                    ") SELECT c.uri, c.created_at FROM c \
                     LEFT JOIN post_counts pc ON pc.post_uri = c.uri \
                     LEFT JOIN constellation_counts cc ON cc.target = c.uri",
                );
                // A stored Constellation count is used only while §II-9.6
                // would still use it.
                if self.inner.cfg.counts.public_source == CountsSource::Local {
                    qb.push(" AND false");
                } else {
                    let stale = i32::try_from(self.inner.cfg.counts.stale_max_secs).unwrap_or(i32::MAX);
                    qb.push(" AND cc.fetched_at >= now() - make_interval(secs => ").push_bind(stale).push(")");
                }
                qb.push(
                    " ORDER BY (1 + CASE WHEN cc.target IS NOT NULL \
                         THEN COALESCE(cc.likes, 0) + COALESCE(cc.reposts, 0) + COALESCE(cc.replies, 0) + COALESCE(cc.quotes, 0) \
                         ELSE COALESCE(pc.like_count + pc.repost_count + pc.reply_count + pc.quote_count, 0) END) * ",
                );
                match plan.text() {
                    Some(text) => {
                        qb.push("COALESCE(ts_rank_cd(c.search_tsv, websearch_to_tsquery('simple', ").push_bind(text.to_string()).push(")), 1)");
                    }
                    None => {
                        qb.push("1");
                    }
                }
                qb.push(" DESC, c.created_at DESC, c.uri DESC LIMIT ").push_bind(limit).push(" OFFSET ").push_bind(offset);
            }
        }

        let mut tx = self.pool().begin().await?;
        set_timeout(&mut tx, cfg.statement_timeout_ms).await?;
        let rows: Vec<(String, DateTime<Utc>)> = match qb.build_query_as().fetch_all(&mut *tx).await {
            Ok(rows) => rows,
            Err(error) if timed_out(&error) => {
                metrics::counter!(METRIC_TIMEOUT, "endpoint" => endpoint).increment(1);
                return Ok(Err(bad("query too expensive")));
            }
            Err(error) => return Err(error.into()),
        };
        // `hitsTotal`: exact, and only when small. It runs after the page
        // under a savepoint, so its timeout costs the count and nothing
        // else.
        let cap = i64::from(cfg.hits_total_max);
        let hits: Option<i64> = {
            let mut count: QueryBuilder<'_, Postgres> = QueryBuilder::new("SELECT count(*) FROM (SELECT 1");
            plan.push_from(&mut count, me, anonymous);
            count.push(" LIMIT ").push_bind(cap + 1).push(") s");
            let mut savepoint = tx.begin().await?;
            set_timeout(&mut savepoint, cfg.hits_total_timeout_ms).await?;
            match count.build_query_scalar::<i64>().fetch_one(&mut *savepoint).await {
                Ok(n) => {
                    savepoint.commit().await?;
                    Some(n).filter(|n| *n <= cap)
                }
                Err(error) if timed_out(&error) => {
                    savepoint.rollback().await?;
                    None
                }
                Err(error) => return Err(error.into()),
            }
        };
        tx.commit().await?;

        let full = i64::try_from(rows.len()).unwrap_or(0) == limit;
        let cursor = match plan.sort {
            Sort::Latest => rows.last().filter(|_| full).map(|(uri, at)| local_cursor(*at, uri)),
            Sort::Top => (full && offset + limit <= max_offset).then(|| (offset + limit).to_string()),
        };
        let uris: Vec<String> = rows.into_iter().map(|(uri, _)| uri).collect();
        // Hydrate, then filter: the moderation gate, and the viewer's
        // blocks and mutes with what only a lookup knows (an inbound list
        // block).
        let ctx = self.post_context(viewer, req, &uris, CountMode::List).await?;
        let state = &ctx.profiles.viewer;
        let posts: Vec<Value> = uris
            .iter()
            .filter(|uri| ctx.gates.get(*uri).copied().unwrap_or(Gate::Visible).listable())
            .filter(|uri| {
                let author = uri_author(uri).unwrap_or_default();
                !state.blocked_either_way(author) && !state.muted.contains(author)
            })
            .filter_map(|uri| ctx.post_view(uri))
            .collect();
        let mut out = json!({ "posts": posts });
        if let Some(cursor) = cursor {
            out["cursor"] = json!(cursor);
        }
        if let Some(hits) = hits {
            out["hitsTotal"] = json!(hits);
        }
        Ok(Ok(Self::emit(out)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::aggregations::ScopeSets;
    use crate::hydration::testing::{actor, exec, hydrator, post};
    use crate::test_support::fresh_db;

    const ALICE: &str = "did:plc:aaaaaaaaaaaaaaaaaaaaaaaa";
    const BOB: &str = "did:plc:bbbbbbbbbbbbbbbbbbbbbbbb";
    const CAROL: &str = "did:plc:cccccccccccccccccccccccc";
    const OUTSIDE: &str = "did:plc:oooooooooooooooooooooooo";

    #[test]
    fn operators_leave_the_text_and_bad_values_are_refused() {
        let ops = operators("rust \"from: me\" from:alice.example.com #Rust lang:EN domain:WWW.Example.com since:2026-01-02 note:x").expect("ok");
        assert_eq!(ops.words, ["rust", "\"from: me\"", "note:x"]);
        assert_eq!((ops.from, ops.tags, ops.langs, ops.domains), (vec!["alice.example.com".to_string()], vec!["rust".to_string()], vec!["en".to_string()], vec!["example.com".to_string()]));
        assert_eq!(ops.since, date("2026-01-02T00:00:00Z"));
        for malformed in ["from:not_a_handle", "since:yesterday", "lang:", "until:2026-13-01"] {
            assert_eq!(operators(malformed).expect_err(malformed).error, BAD_QUERY);
        }
        // A datetime without a zone is UTC; a date alone is midnight UTC.
        assert_eq!(date("2026-03-04T05:06:07"), date("2026-03-04T05:06:07Z"));
        assert!(date("03/04/2026").is_none());
    }

    fn params(query: &str) -> PostSearchParams {
        PostSearchParams { query: Some(query.to_string()), limit: 25, ..PostSearchParams::default() }
    }

    fn uris(view: &Value) -> Vec<String> {
        view["posts"].as_array().expect("posts").iter().map(|p| p["uri"].as_str().expect("uri").rsplit('/').next().expect("rkey").to_string()).collect()
    }

    /// B-38 / B-39 (design-v3 §9.0 – §9.2): text, operators, filters, the
    /// corpus, the viewer's exclusions, the window, and both sorts.
    #[tokio::test]
    async fn posts_are_found_by_text_and_filters_within_scope() {
        let Some(db) = fresh_db("search_posts").await else {
            return;
        };
        let p = &db.pool;
        for (did, handle) in [(ALICE, "alice.example.com"), (BOB, "bob.example.com"), (CAROL, "carol.example.com"), (OUTSIDE, "out.example.com")] {
            actor(p, did, handle, handle).await;
        }
        for did in [ALICE, BOB, CAROL] {
            sqlx::query("INSERT INTO scope_dids (did, reasons) VALUES ($1, 2)").bind(did).execute(p).await.expect("scope");
        }
        exec(p, &format!("INSERT INTO members (did, source, ready_at) VALUES ('{ALICE}', 'admin', now())")).await;
        post(p, ALICE, "3ka", "rust is a fine language", None, None).await;
        let bob_post = post(p, BOB, "3kb", "rust and tokio", None, None).await;
        post(p, BOB, "3kc", "a reply about rust", Some(&bob_post), Some(&bob_post)).await;
        post(p, CAROL, "3kd", "rust from carol", None, None).await;
        post(p, OUTSIDE, "3ke", "rust from outside the scope", None, None).await;
        post(p, BOB, "3kf", "an old post about rust", None, None).await;
        exec(p, "UPDATE posts SET search_tsv = to_tsvector('simple', text), tags = '{}', link_domains = '{}', link_urls = '{}', lang_prefixes = '{en}'").await;
        exec(p, "UPDATE posts SET tags = '{tokio}', link_domains = '{example.org,docs.example.org}', lang_prefixes = '{pt,pt-br}' WHERE rkey = '3kb'").await;
        exec(p, "UPDATE posts SET created_at = now() - interval '90 days' WHERE rkey = '3kf'").await;
        exec(p, "UPDATE posts SET created_at = now() - interval '1 hour' WHERE rkey = '3ka'").await;
        exec(p, "UPDATE posts SET created_at = now() - interval '2 hours' WHERE rkey = '3kb'").await;
        exec(p, "UPDATE posts SET created_at = now() - interval '3 hours' WHERE rkey = '3kc'").await;
        exec(p, "UPDATE posts SET created_at = now() - interval '4 hours' WHERE rkey = '3kd'").await;
        exec(p, &format!("INSERT INTO post_counts (post_uri, like_count) VALUES ('at://{CAROL}/app.bsky.feed.post/3kd', 50)")).await;

        let mut sets = ScopeSets::default();
        for did in [ALICE, BOB, CAROL] {
            sets.scope.insert(did.to_string());
        }
        sets.members.insert(ALICE.to_string());
        let h = hydrator(p, sets);
        let req = Requester::test();
        let anon = Viewer::anonymous();
        let search = |viewer: Viewer, params: PostSearchParams| {
            let h = h.clone();
            let req = req.clone();
            async move { h.search_posts(&viewer, &req, &params).await.expect("db").map(|v| v.inner().clone()) }
        };

        // Newest first, in scope only, inside the recent window.
        let found = search(anon.clone(), params("rust")).await.expect("view");
        assert_eq!(uris(&found), ["3ka", "3kb", "3kc", "3kd"]);
        assert_eq!(found["hitsTotal"], json!(4));
        // The whole corpus, with V2's allTime.
        let all = search(anon.clone(), PostSearchParams { v2: true, all_time: true, ..params("rust") }).await.expect("view");
        assert_eq!(uris(&all).len(), 5);
        // Keyset pages.
        let first = search(anon.clone(), PostSearchParams { limit: 2, ..params("rust") }).await.expect("view");
        let next = search(anon.clone(), PostSearchParams { limit: 2, cursor: first["cursor"].as_str().map(str::to_string), ..params("rust") }).await.expect("view");
        assert_eq!((uris(&first), uris(&next)), (vec!["3ka".to_string(), "3kb".to_string()], vec!["3kc".to_string(), "3kd".to_string()]));

        // Operators and filters join with AND; handles resolve locally.
        assert_eq!(uris(&search(anon.clone(), params("rust from:bob.example.com")).await.expect("view")), ["3kb", "3kc"]);
        assert_eq!(uris(&search(anon.clone(), params("from:nobody.example.com rust")).await.expect("view")), Vec::<String>::new());
        assert_eq!(uris(&search(anon.clone(), params("#tokio")).await.expect("view")), ["3kb"]);
        assert_eq!(uris(&search(anon.clone(), params("rust domain:example.org lang:pt")).await.expect("view")), ["3kb"]);
        assert_eq!(uris(&search(anon.clone(), params("rust -tokio")).await.expect("view")), ["3ka", "3kc", "3kd"]);
        let v2 = |f: &dyn Fn(&mut PostSearchParams)| {
            let mut p = PostSearchParams { v2: true, ..params("rust") };
            f(&mut p);
            p
        };
        assert_eq!(uris(&search(anon.clone(), v2(&|p| p.exclude_replies = true)).await.expect("view")), ["3ka", "3kb", "3kd"]);
        assert_eq!(uris(&search(anon.clone(), v2(&|p| p.thread_root = Some(bob_post.clone()))).await.expect("view")), ["3kb", "3kc"]);
        assert_eq!(uris(&search(anon.clone(), v2(&|p| p.exclude_hashtags = vec!["Tokio".into()])).await.expect("view")), ["3ka", "3kc", "3kd"]);
        assert_eq!(uris(&search(anon.clone(), v2(&|p| p.exclude_authors = vec![BOB.into(), "carol.example.com".into()])).await.expect("view")), ["3ka"]);
        // A private-tier value matches nothing and is never compared.
        let private = "at://did:plc:aaaaaaaaaaaaaaaaaaaaaaaa/tools.kryphocron.feed.post/3kz".to_string();
        assert_eq!(uris(&search(anon.clone(), v2(&|p| p.reply_parent = Some(private.clone()))).await.expect("view")), Vec::<String>::new());
        assert_eq!(uris(&search(anon.clone(), v2(&|p| p.exclude_embedded = vec![private.clone()])).await.expect("view")).len(), 4);

        // Top: the newest candidates, by stored engagement.
        let top = search(anon.clone(), PostSearchParams { sort: Some("top".into()), ..params("rust") }).await.expect("view");
        assert_eq!(uris(&top)[0], "3kd");

        // What is refused, and with which error.
        for (params, error) in [
            (params("-rust"), BAD_QUERY),
            (params(""), BAD_QUERY),
            (v2(&|p| (p.replies_only, p.exclude_replies) = (true, true)), BAD_QUERY),
            (v2(&|p| p.hashtags = (0..26).map(|i| format!("t{i}")).collect()), BAD_QUERY),
            (v2(&|p| p.following = true), "InvalidRequest"),
            (PostSearchParams { since: Some("soon".into()), ..params("rust") }, BAD_QUERY),
        ] {
            assert_eq!(search(anon.clone(), params).await.expect_err("refused").error, error);
        }

        // A ready member: a mute and a block drop their authors, and
        // `following` keeps the accounts the member follows.
        let alice = Viewer { did: Some(ALICE.to_string()), hydrate: true };
        exec(p, &format!("INSERT INTO mutes (viewer_did, subject_did) VALUES ('{ALICE}', '{CAROL}')")).await;
        let found = search(alice.clone(), params("rust")).await.expect("view");
        assert_eq!((uris(&found), &found["hitsTotal"]), (vec!["3ka".to_string(), "3kb".to_string(), "3kc".to_string()], &json!(3)));
        exec(p, &format!("INSERT INTO follows (uri, author_did, subject_did, rev, created_at) VALUES ('at://{ALICE}/app.bsky.graph.follow/1', '{ALICE}', '{BOB}', '3k', now())")).await;
        assert_eq!(uris(&search(alice.clone(), v2(&|p| p.following = true)).await.expect("view")), ["3kb", "3kc"]);

        // Signed out, an author who asks not to be shown is left out.
        exec(p, &format!("UPDATE actors SET profile_record = '{{\"labels\": {{\"values\": [{{\"val\": \"!no-unauthenticated\"}}]}}}}' WHERE did = '{BOB}'")).await;
        assert_eq!(uris(&search(anon.clone(), params("rust")).await.expect("view")), ["3ka", "3kd"]);
        assert_eq!(uris(&search(alice, params("tokio")).await.expect("view")), ["3kb"]);
        db.drop_db().await;
    }
}
