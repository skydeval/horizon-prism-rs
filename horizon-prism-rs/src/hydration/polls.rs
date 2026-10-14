//! `app.nearhorizon.feed.getAuthorPolls` (design-v3 §5.13): an account's
//! poll posts.
//!
//! - Only polls whose `visibility` is `public` are returned: nothing here
//!   could enforce any other audience.
//! - An indexed author's polls come from stored rows; any other author's
//!   from one page of its PDS listing, continued with a `P:` cursor.
//! - No tallies and no vote state.

use chrono::{DateTime, Utc};
use serde_json::{Value, json};

use super::actor::ProfileKind;
use super::feed::{local_cursor, parse_local_cursor};
use super::{HydrationError, Hydrator, ViewError, ViewResult, Viewer, ts};
use crate::moderation;
use crate::xrpc::pools::Requester;

pub const DEFAULT_LIMIT: u32 = 25;
pub const MAX_LIMIT: u32 = 100;

/// A stored poll: URI, rkey, CID, record, `created_at`, `indexed_at`.
type PollRow = (String, String, String, Value, DateTime<Utc>, DateTime<Utc>);

impl Hydrator {
    /// `app.nearhorizon.feed.getAuthorPolls`.
    ///
    /// # Errors
    ///
    /// Database errors; no budget for the PDS listing.
    pub async fn get_author_polls(&self, viewer: &Viewer, req: &Requester, actor: &str, limit: u32, cursor: Option<&str>) -> ViewResult<Value> {
        if !(1..=MAX_LIMIT).contains(&limit) {
            return Ok(Err(ViewError::new("InvalidRequest", "Error: limit must be between 1 and 100")));
        }
        let Some(did) = self.resolve_actor(req, actor).await? else {
            return Ok(Err(ViewError::new("InvalidRequest", "Profile not found")));
        };
        if let Some((error, message)) = moderation::gate(self.pool(), &did).await?.profile_error() {
            return Ok(Err(ViewError::new(error, message)));
        }
        if !self.stores_public_of(&did).await || cursor.is_some_and(|c| c.starts_with("P:")) {
            return match self.upstream().author_polls(req, &did, limit, cursor).await {
                Some(v) => Ok(Ok(Self::emit(self.normalized(viewer, req, v).await?.0))),
                None => match req.exhausted() {
                    // No budget for the listing: a 429, never an empty page.
                    Some(exhausted) => Err(HydrationError::NoBudget(exhausted)),
                    None => Ok(Ok(Self::emit(json!({ "polls": [] })))),
                },
            };
        }
        let (at, after) = parse_local_cursor(cursor);
        let rows: Vec<PollRow> = sqlx::query_as(
            "SELECT uri, rkey, cid, record, created_at, indexed_at FROM nearhorizon_poll_posts \
             WHERE author_did = $1 AND visibility = 'public' \
               AND ($2::timestamptz IS NULL OR (created_at, rkey) < ($2, $3)) \
             ORDER BY created_at DESC, rkey DESC LIMIT $4",
        )
        .bind(&did)
        .bind(at)
        .bind(after.unwrap_or_default())
        .bind(i64::from(limit))
        .fetch_all(self.pool())
        .await?;
        let next = (rows.len() == usize::try_from(limit).unwrap_or(usize::MAX))
            .then(|| rows.last().map(|(_, rkey, _, _, created, _)| local_cursor(*created, rkey)))
            .flatten();
        let labelled: Vec<String> = rows.iter().map(|(uri, ..)| uri.clone()).collect();
        let gates = moderation::gate_many(self.pool(), &labelled).await?;
        let ctx = self.profile_context(viewer, req, std::slice::from_ref(&did), None).await?;
        let author = ctx.view(&did, ProfileKind::Basic);
        let polls: Vec<Value> = rows
            .into_iter()
            .filter(|(uri, ..)| gates.get(uri).is_none_or(|g| g.listable()))
            .map(|(uri, _, cid, record, _, indexed)| json!({"uri": uri, "cid": cid, "author": author, "record": record, "indexedAt": ts(indexed)}))
            .collect();
        let mut out = json!({ "polls": polls });
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
    use crate::hydration::testing::{actor, exec, hydrator};
    use crate::test_support::fresh_db;

    const ALICE: &str = "did:plc:aaaaaaaaaaaaaaaaaaaaaaaa";

    /// B-63: public polls only, a basic author view, and the limit.
    #[tokio::test]
    async fn only_public_polls_are_listed() {
        let Some(db) = fresh_db("hydration_author_polls").await else {
            return;
        };
        let p = &db.pool;
        actor(p, ALICE, "alice.test", "Alice").await;
        for (n, visibility) in [(1, "public"), (2, "followers"), (3, "public")] {
            exec(p, &format!(
                "INSERT INTO nearhorizon_poll_posts (uri, author_did, rkey, cid, rev, text, options, visibility, record, created_at) VALUES \
                 ('at://{ALICE}/app.nearhorizon.poll.post/3kpoll{n}', '{ALICE}', '3kpoll{n}', 'bafy{n}', '3k', 'q{n}', '[]', '{visibility}', \
                  '{{\"text\": \"q{n}\", \"visibility\": \"{visibility}\"}}', now() + interval '{n} seconds')"
            )).await;
        }
        let mut sets = ScopeSets::default();
        sets.scope.insert(ALICE.into());
        let h = hydrator(p, sets);
        let (anon, req) = (Viewer::anonymous(), Requester::test());
        let v = h.get_author_polls(&anon, &req, "alice.test", 25, None).await.expect("db").expect("view");
        let polls = v.inner()["polls"].as_array().expect("polls");
        assert_eq!(polls.iter().map(|x| x["record"]["text"].as_str().expect("text")).collect::<Vec<_>>(), ["q3", "q1"]);
        assert_eq!((&polls[0]["author"]["handle"], &polls[0]["cid"]), (&json!("alice.test"), &json!("bafy3")));
        assert!(polls[0]["author"].get("description").is_none() && polls[0]["indexedAt"].is_string());
        // Paging over the public ones.
        let page = h.get_author_polls(&anon, &req, ALICE, 1, None).await.expect("db").expect("view");
        let cursor = page.inner()["cursor"].as_str().expect("cursor").to_string();
        let rest = h.get_author_polls(&anon, &req, ALICE, 1, Some(&cursor)).await.expect("db").expect("view");
        assert_eq!(rest.inner()["polls"][0]["record"]["text"], "q1");
        // The limit is bounded.
        for bad in [0, 101] {
            assert_eq!(h.get_author_polls(&anon, &req, ALICE, bad, None).await.expect("db").expect_err("limit").error, "InvalidRequest");
        }
        db.drop_db().await;
    }

    /// T-16 (public-indexing design §9.1): unless the mode is On, an S
    /// author's polls take the PDS path, whatever rows are stored here.
    #[tokio::test]
    async fn polls_take_the_pds_path_unless_the_mode_is_on() {
        use std::sync::Arc;

        use crate::hydration::testing::{FixedUpstream, hydrator_with, set_mode};
        use crate::indexing::PublicMode;

        let Some(db) = fresh_db("hydration_author_polls_off").await else {
            return;
        };
        let p = &db.pool;
        actor(p, ALICE, "alice.test", "Alice").await;
        exec(p, &format!(
            "INSERT INTO nearhorizon_poll_posts (uri, author_did, rkey, cid, rev, text, options, visibility, record, created_at) VALUES \
             ('at://{ALICE}/app.nearhorizon.poll.post/3kpoll1', '{ALICE}', '3kpoll1', 'bafy1', '3k', 'local', '[]', 'public', \
              '{{\"text\": \"local\", \"visibility\": \"public\"}}', now())"
        )).await;
        let mut up = FixedUpstream::default();
        let listed = json!({"uri": format!("at://{ALICE}/app.nearhorizon.poll.post/3kpoll2"), "cid": "bafy2",
                            "author": {"did": ALICE, "handle": "alice.test"}, "record": {"text": "from the pds"}});
        up.polls.insert(ALICE.into(), json!({"polls": [listed]}));
        let mut sets = ScopeSets::default();
        sets.scope.insert(ALICE.into());
        let h = hydrator_with(p, sets, Arc::new(up));
        let (anon, req) = (Viewer::anonymous(), Requester::test());
        let texts = |v: &Value| -> Vec<String> {
            v["polls"].as_array().expect("polls").iter().map(|x| x["record"]["text"].as_str().expect("text").to_string()).collect()
        };
        let on = h.get_author_polls(&anon, &req, ALICE, 25, None).await.expect("db").expect("view");
        assert_eq!(texts(on.inner()), ["local"]);
        for mode in [PublicMode::Filling, PublicMode::Draining, PublicMode::Off, PublicMode::Held] {
            set_mode(h.scope(), mode).await;
            let off = h.get_author_polls(&anon, &req, ALICE, 25, None).await.expect("db").expect("view");
            assert_eq!(texts(off.inner()), ["from the pds"], "{mode:?}");
        }
        db.drop_db().await;
    }
}
