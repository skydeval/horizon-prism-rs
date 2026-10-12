//! `com.atproto.label.queryLabels` (design-v3 §5.12): the HTTP read of the
//! labels this AppView's operator applied.
//!
//! - A pattern is an exact at-uri or DID, or a prefix ending in `*` whose
//!   fixed part holds a whole DID: `at://<did>/…*`. So one pattern reaches
//!   one repository at most, and no request can list the whole table.
//! - Matching is a literal prefix test, never a `LIKE` with the caller's
//!   text: a DID may contain `%` and `_`.
//! - Labels on private-tier records are never returned.
//! - Labels are not signed.

use chrono::{DateTime, Utc};
use proto_blue::syntax::Did;
use serde_json::{Value, json};

use super::{Hydrator, ViewError, ViewResult, ts};

pub const DEFAULT_LIMIT: u32 = 50;
pub const MAX_LIMIT: u32 = 250;

/// The patterns of one request, split by kind.
#[derive(Debug, Default, PartialEq, Eq)]
struct Patterns {
    exact: Vec<String>,
    prefixes: Vec<String>,
}

fn invalid(message: impl Into<String>) -> ViewError {
    ViewError::new("InvalidRequest", message)
}

/// Whether `prefix` starts `at://<a whole DID>/`.
fn holds_a_whole_did(prefix: &str) -> bool {
    prefix
        .strip_prefix("at://")
        .and_then(|rest| rest.split_once('/'))
        .is_some_and(|(did, _)| Did::is_valid(did))
}

fn parse(patterns: &[String], max: usize) -> Result<Patterns, ViewError> {
    if patterns.is_empty() {
        return Err(invalid("Error: Params must have the property \"uriPatterns\""));
    }
    if patterns.len() > max {
        return Err(invalid(format!("Error: uriPatterns must not have more than {max} elements")));
    }
    let mut out = Patterns::default();
    for pattern in patterns {
        // Only a final `*` is a wildcard.
        match pattern.strip_suffix('*') {
            Some(prefix) if holds_a_whole_did(prefix) => out.prefixes.push(prefix.to_string()),
            Some(_) => return Err(invalid("Error: a wildcard pattern must name a whole DID: at://<did>/…*")),
            None if pattern.starts_with("at://") || Did::is_valid(pattern) => out.exact.push(pattern.clone()),
            None => return Err(invalid("Error: uriPatterns must be at-uris or DIDs")),
        }
    }
    Ok(out)
}

impl Hydrator {
    /// `com.atproto.label.queryLabels`.
    ///
    /// # Errors
    ///
    /// Database errors.
    pub async fn query_labels(&self, patterns: &[String], sources: &[String], limit: u32, cursor: Option<&str>) -> ViewResult<Value> {
        let max = usize::try_from(self.inner.cfg.labels_query_max_patterns).unwrap_or(usize::MAX);
        let patterns = match parse(patterns, max) {
            Ok(p) => p,
            Err(e) => return Ok(Err(e)),
        };
        // This AppView holds its own labels only.
        let src = &self.inner.cfg.labeler_did;
        if !sources.is_empty() && !sources.contains(src) {
            return Ok(Ok(Self::emit(json!({ "labels": [] }))));
        }
        let after: i64 = match cursor.map(str::parse) {
            None => 0,
            Some(Ok(id)) => id,
            Some(Err(_)) => return Ok(Err(invalid("Error: cursor is not valid"))),
        };
        let limit = limit.clamp(1, MAX_LIMIT);
        let rows: Vec<(i64, String, String, DateTime<Utc>)> = sqlx::query_as(
            "SELECT id, subject, val, created_at FROM labels l WHERE id > $1 \
               AND (subject = ANY($2) OR EXISTS (SELECT 1 FROM unnest($3::text[]) p WHERE starts_with(l.subject, p))) \
               AND split_part(subject, '/', 4) NOT LIKE 'tools.kryphocron.%' \
             ORDER BY id LIMIT $4",
        )
        .bind(after)
        .bind(&patterns.exact)
        .bind(&patterns.prefixes)
        .bind(i64::from(limit))
        .fetch_all(self.pool())
        .await?;
        let next = (rows.len() == usize::try_from(limit).unwrap_or(usize::MAX))
            .then(|| rows.last().map(|(id, ..)| id.to_string()))
            .flatten();
        let labels: Vec<Value> = rows
            .into_iter()
            .map(|(_, subject, val, cts)| json!({"ver": 1, "src": src, "uri": subject, "val": val, "cts": ts(cts)}))
            .collect();
        let mut out = json!({ "labels": labels });
        if let Some(c) = next {
            out["cursor"] = json!(c);
        }
        Ok(Ok(Self::emit(out)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::aggregations::ScopeSets;
    use crate::hydration::testing::{LABELER, exec, hydrator};
    use crate::test_support::fresh_db;

    const ALICE: &str = "did:plc:aaaaaaaaaaaaaaaaaaaaaaaa";
    const BOB: &str = "did:plc:bbbbbbbbbbbbbbbbbbbbbbbb";

    #[test]
    fn a_wildcard_needs_a_whole_did() {
        let p = |s: &[&str]| parse(&s.iter().map(|x| (*x).to_string()).collect::<Vec<_>>(), 3);
        let ok = p(&[ALICE, &format!("at://{ALICE}/app.bsky.feed.post/1"), &format!("at://{ALICE}/*")]).expect("valid");
        assert_eq!(ok.exact.len(), 2);
        assert_eq!(ok.prefixes, [format!("at://{ALICE}/")]);
        for bad in ["*", "at://did*", "did:plc:*", "at://*", &format!("at://{ALICE}*"), "nonsense", &format!("{ALICE}*")] {
            assert!(p(&[bad]).is_err(), "{bad}");
        }
        assert!(p(&[]).is_err());
        assert!(p(&[ALICE, ALICE, ALICE, ALICE]).is_err());
        // A `*` that is not last is a literal character of an exact URI.
        assert_eq!(p(&[&format!("at://{ALICE}/a*b")]).expect("literal").exact.len(), 1);
    }

    #[tokio::test]
    async fn labels_are_read_by_exact_subject_or_one_repos_prefix() {
        let Some(db) = fresh_db("hydration_query_labels").await else {
            return;
        };
        let p = &db.pool;
        let post = format!("at://{ALICE}/app.bsky.feed.post/3kpost");
        let private = format!("at://{ALICE}/tools.kryphocron.feed.postPrivate/3kprivate");
        // A DID with LIKE's wildcards in it, and a repo it must not match.
        let odd = "did:web:a%_b.example";
        for (subject, val) in [(ALICE, "porn"), (post.as_str(), "!hide"), (private.as_str(), "nudity"), (BOB, "sexual"), (&format!("at://{BOB}/app.bsky.feed.post/1"), "porn")] {
            exec(p, &format!("INSERT INTO labels (subject, val) VALUES ('{subject}', '{val}')")).await;
        }
        let h = hydrator(p, ScopeSets::default());
        let q = |patterns: Vec<String>, sources: Vec<String>, limit: u32, cursor: Option<String>| {
            let h = &h;
            async move { h.query_labels(&patterns, &sources, limit, cursor.as_deref()).await.expect("db") }
        };
        let subjects = |v: &Value| -> Vec<String> {
            v["labels"].as_array().expect("labels").iter().map(|l| l["uri"].as_str().expect("uri").to_string()).collect()
        };

        let exact = q(vec![ALICE.into()], vec![], 50, None).await.expect("view");
        assert_eq!(exact.inner()["labels"], json!([{"ver": 1, "src": LABELER, "uri": ALICE, "val": "porn", "cts": exact.inner()["labels"][0]["cts"]}]));
        // One repo's prefix: its public records only, never another repo's
        // and never a private-tier record, also when that is named exactly.
        let prefix = q(vec![format!("at://{ALICE}/*")], vec![], 50, None).await.expect("view");
        assert_eq!(subjects(prefix.inner()), std::slice::from_ref(&post));
        let named = q(vec![private.clone()], vec![], 50, None).await.expect("view");
        assert_eq!(named.inner()["labels"], json!([]));
        let literal = q(vec![format!("at://{odd}/*")], vec![], 50, None).await.expect("view");
        assert_eq!(literal.inner()["labels"], json!([]));
        // Another source: nothing. This AppView among the sources: as usual.
        let other = q(vec![ALICE.into()], vec!["did:web:elsewhere.example".into()], 50, None).await.expect("view");
        assert_eq!(other.inner()["labels"], json!([]));
        let ours = q(vec![ALICE.into()], vec![LABELER.into()], 50, None).await.expect("view");
        assert_eq!(subjects(ours.inner()), [ALICE]);
        // Paging by row id.
        let both = vec![ALICE.to_string(), BOB.to_string()];
        let first = q(both.clone(), vec![], 1, None).await.expect("view");
        let cursor = first.inner()["cursor"].as_str().expect("cursor").to_string();
        let second = q(both, vec![], 1, Some(cursor)).await.expect("view");
        assert_eq!(subjects(second.inner()), [BOB]);
        // Refusals.
        assert_eq!(q(vec!["did:plc:*".into()], vec![], 50, None).await.expect_err("wildcard").error, "InvalidRequest");
        assert_eq!(q(vec![ALICE.into()], vec![], 50, Some("x".into())).await.expect_err("cursor").error, "InvalidRequest");
        db.drop_db().await;
    }
}
