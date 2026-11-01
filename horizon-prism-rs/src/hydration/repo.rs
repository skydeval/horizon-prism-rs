//! `com.atproto.repo.getRecord` (design-v3 §5.10): stored public records,
//! from local tables only. Not a PDS proxy: nothing is fetched.
//!
//! The order of the checks is the point:
//! 1. a private-tier collection is `RecordNotFound` before any lookup, the
//!    same answer as a record that does not exist;
//! 2. a collection whose rows keep no record JSON is refused by its name;
//! 3. everything else that is not held here, is hidden, or would not be
//!    served by another public path is `RecordNotFound`.

use serde_json::{Value, json};

use super::lists::NSID_LIST;
use super::{Hydrator, ViewError, ViewResult};
use crate::moderation;

const NSID_PROFILE: &str = "app.bsky.actor.profile";
const NSID_LABELER: &str = "app.bsky.labeler.service";
const NSID_POLL: &str = "app.nearhorizon.poll.post";

/// Indexed collections whose rows keep extracted columns only: a record
/// rebuilt from them would not match its CID.
const NO_RECORD_JSON: [&str; 7] = [
    "app.bsky.feed.like",
    "app.bsky.feed.repost",
    "app.bsky.graph.follow",
    "app.bsky.graph.block",
    "app.bsky.graph.listitem",
    "app.bsky.graph.listblock",
    "app.bsky.graph.referencelistoptout",
];

/// Collections stored one row per record with `cid` and `record` columns,
/// keyed by ids, with their record encoded (posts apart).
const BY_KEY: [(&str, &str); 5] = [
    (NSID_LIST, "lists"),
    ("app.bsky.graph.starterpack", "starter_packs"),
    (NSID_LABELER, "labeler_services"),
    ("app.bsky.feed.threadgate", "threadgates"),
    ("app.bsky.feed.postgate", "postgates"),
];

/// The collections of `actor_self_records`.
const SELF_RECORDS: [&str; 3] = [
    "app.bsky.notification.declaration",
    "app.bsky.actor.status",
    "app.bsky.actor.contentVisibilityDeclaration",
];

fn not_found() -> ViewError {
    ViewError::new("RecordNotFound", "Could not locate record")
}

impl Hydrator {
    /// The repository's DID: given as one, or the DID of a handle this
    /// AppView has stored for an account it indexes. No lookup leaves here.
    async fn local_repo(&self, repo: &str) -> Result<Option<String>, sqlx::Error> {
        if repo.starts_with("did:") {
            return Ok(proto_blue::syntax::Did::is_valid(repo).then(|| repo.to_string()));
        }
        let did = crate::db::actors::did_of_handle(self.pool(), &repo.to_ascii_lowercase()).await?;
        let Some(did) = did else { return Ok(None) };
        let scope = self.scope().read_guard().await;
        Ok((scope.in_scope(&did) || scope.is_member(&did)).then_some(did))
    }

    /// The stored `(cid, record)` of `uri`, if this collection is served
    /// and the row may be.
    async fn stored_record(&self, did: &str, collection: &str, rkey: &str, uri: &str) -> Result<Option<(String, Value)>, sqlx::Error> {
        let self_keyed = collection == NSID_PROFILE || collection == NSID_LABELER || SELF_RECORDS.contains(&collection);
        if self_keyed && rkey != "self" {
            return Ok(None);
        }
        if collection == NSID_PROFILE {
            // An `actors` row is per-DID state: served for an account in
            // scope or a member only, so a row kept for a private owner or
            // the author of a private reply is never seen (design-v3 §11
            // G-9). A profile whose CID is not known yet is not served.
            if !self.in_scope_or_member(did).await {
                return Ok(None);
            }
            return crate::db::actors::record_with_cid(self.pool(), did).await;
        }
        if SELF_RECORDS.contains(&collection) {
            return self.keyed_record("actor_self_records", uri, "").await;
        }
        if collection == NSID_POLL {
            // As `getAuthorPolls`: only public polls.
            return self.keyed_record("nearhorizon_poll_posts", uri, " AND visibility = 'public'").await;
        }
        if collection == crate::hydration::feed::NSID_POST {
            let row: Option<(Vec<u8>, Vec<u8>)> = sqlx::query_as(&format!("SELECT p.cid, p.record FROM {}", crate::db::posts::rows_named(1)))
                .bind(std::slice::from_ref(&uri.to_string()))
                .fetch_optional(self.pool())
                .await?;
            return match row {
                Some((cid, record)) => Ok(Some((crate::db::ids::cid_from_db(&cid), crate::db::records::decode_value(self.pool(), &record).await?))),
                None => Ok(None),
            };
        }
        let Some((_, table)) = BY_KEY.iter().find(|(nsid, _)| *nsid == collection) else {
            return Ok(None);
        };
        // A list's record is served exactly when every other public path
        // would serve the list from local rows.
        if collection == NSID_LIST && !self.is_ready_member(did).await? {
            return Ok(None);
        }
        self.keyed_record(table, uri, "").await
    }

    /// The stored `(cid, record)` of `uri` in a keyed v3 record table, with
    /// `extra` added to its condition.
    async fn keyed_record(&self, table: &str, uri: &str, extra: &str) -> Result<Option<(String, Value)>, sqlx::Error> {
        let Some(key) = crate::firehose::rows::key_of(table) else {
            return Ok(None);
        };
        let Some((author, k)) = crate::firehose::rows::key_for(self.pool(), key, uri).await? else {
            return Ok(None);
        };
        let row: Option<(Vec<u8>, Vec<u8>)> =
            sqlx::query_as(&format!("SELECT cid, record FROM {table} WHERE {}{extra}", crate::firehose::rows::key_where(key)))
                .bind(author)
                .bind(k)
                .fetch_optional(self.pool())
                .await?;
        match row {
            Some((cid, record)) => Ok(Some((crate::db::ids::cid_from_db(&cid), crate::db::records::decode_value(self.pool(), &record).await?))),
            None => Ok(None),
        }
    }

    /// `com.atproto.repo.getRecord`.
    ///
    /// # Errors
    ///
    /// Database errors.
    pub async fn repo_get_record(&self, repo: &str, collection: &str, rkey: &str, cid: Option<&str>) -> ViewResult<Value> {
        if collection.starts_with("tools.kryphocron.") {
            return Ok(Err(not_found()));
        }
        if NO_RECORD_JSON.contains(&collection) {
            return Ok(Err(ViewError::new("InvalidRequest", "collection not served by getRecord on this AppView")));
        }
        let Some(did) = self.local_repo(repo).await? else {
            return Ok(Err(not_found()));
        };
        let uri = format!("at://{did}/{collection}/{rkey}");
        // Its own Refract snapshot, and no serve-time check (refract design
        // §4.2, §4.5).
        let snapshot = self.refract_snapshot_now().await;
        let check = crate::refract::RefractCheck::default();
        let view = crate::refract::RefractView { snapshot: &snapshot, check: &check };
        let gates = moderation::gate_many(self.pool(), &[uri.clone(), did.clone()], view).await?;
        if gates.values().any(|g| g.is_hidden()) {
            return Ok(Err(not_found()));
        }
        match self.stored_record(&did, collection, rkey, &uri).await? {
            Some((stored, value)) if cid.is_none_or(|wanted| wanted == stored) => {
                Ok(Ok(Self::emit(json!({"uri": uri, "cid": stored, "value": value}))))
            }
            _ => Ok(Err(not_found())),
        }
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

    /// B-53 (`repo.getRecord`), and the collection rules.
    #[tokio::test]
    async fn stored_public_records_are_served_and_nothing_else() {
        let Some(db) = fresh_db("hydration_repo_get_record").await else {
            return;
        };
        let p = &db.pool;
        actor(p, ALICE, "alice.test", "Alice").await;
        actor(p, BOB, "bob.test", "Bob").await;
        let uri = post(p, ALICE, "3kaaaaaaaaaa2", "hello", None, None).await;
        let stored: Vec<u8> = sqlx::query_scalar(&format!("SELECT cid FROM posts WHERE {}", crate::test_support::posts::key_is("", &uri)))
            .fetch_one(p)
            .await
            .expect("cid");
        let cid = crate::db::ids::cid_from_db(&stored);
        let mut sets = ScopeSets::default();
        sets.scope.insert(ALICE.into());
        let h = hydrator(p, sets);
        let get = |repo: &'static str, collection: &'static str, rkey: &'static str, cid: Option<String>| {
            let h = &h;
            async move { h.repo_get_record(repo, collection, rkey, cid.as_deref()).await.expect("db") }
        };
        let error = |r: Result<crate::hydration::PublicEmit<Value>, ViewError>| r.expect_err("error").error;

        // By DID and by the handle of an indexed account.
        let v = get(ALICE, "app.bsky.feed.post", "3kaaaaaaaaaa2", None).await.expect("record");
        assert_eq!((&v.inner()["uri"], &v.inner()["cid"], &v.inner()["value"]["text"]), (&json!(uri), &json!(cid), &json!("hello")));
        get("alice.test", "app.bsky.feed.post", "3kaaaaaaaaaa2", Some(cid.clone())).await.expect("by handle, with its cid");
        // Another CID, a record that is not here, a handle of an account
        // that is not indexed, an unindexed collection: all the same.
        assert_eq!(error(get(ALICE, "app.bsky.feed.post", "3kaaaaaaaaaa2", Some("bafyother".into())).await), "RecordNotFound");
        assert_eq!(error(get(ALICE, "app.bsky.feed.post", "3knever", None).await), "RecordNotFound");
        assert_eq!(error(get("bob.test", "app.bsky.feed.post", "3kaaaaaaaaaa2", None).await), "RecordNotFound");
        assert_eq!(error(get(ALICE, "com.example.thing", "1", None).await), "RecordNotFound");
        // Private tier: the same answer, existing or not, with no lookup.
        assert_eq!(error(get(ALICE, "tools.kryphocron.feed.postPrivate", "3kaaaaaaaaaa2", None).await), "RecordNotFound");
        // Collections kept without their record JSON are refused by name.
        assert_eq!(error(get(ALICE, "app.bsky.feed.like", "1", None).await), "InvalidRequest");
        assert_eq!(error(get("nobody.test", "app.bsky.graph.follow", "1", None).await), "InvalidRequest");

        // The profile needs its CID, and only `self` names it.
        assert_eq!(error(get(ALICE, "app.bsky.actor.profile", "self", None).await), "RecordNotFound");
        exec(p, &crate::test_support::actors::update_sql(ALICE, "profile_cid = '\\x00'::bytea || convert_to('bafyprofile', 'UTF8')")).await;
        let v = get(ALICE, "app.bsky.actor.profile", "self", None).await.expect("profile");
        assert_eq!(v.inner()["cid"], "bafyprofile");
        assert_eq!(error(get(ALICE, "app.bsky.actor.profile", "other", None).await), "RecordNotFound");
        // G-9: Bob's row is kept, but Bob is neither in scope nor a member.
        exec(p, &crate::test_support::actors::update_sql(BOB, "profile_cid = '\\x00'::bytea || convert_to('bafyprofile', 'UTF8')")).await;
        assert_eq!(error(get(BOB, "app.bsky.actor.profile", "self", None).await), "RecordNotFound");

        // A list is served only when its owner is a ready member.
        crate::test_support::records::list(p, ALICE, "3klist2222222", "L", "app.bsky.graph.defs#modlist").await;
        assert_eq!(error(get(ALICE, "app.bsky.graph.list", "3klist2222222", None).await), "RecordNotFound");
        exec(p, &format!("INSERT INTO members (did, source, ready_at) VALUES ('{ALICE}', 'admin', now())")).await;
        let v = get(ALICE, "app.bsky.graph.list", "3klist2222222", None).await.expect("list");
        assert_eq!((&v.inner()["cid"], &v.inner()["value"]["name"]), (&json!("bafy3klist2222222"), &json!("L")));
        // A self record and a public poll; a poll for followers is not served.
        let declaration = json!({"$type": "app.bsky.notification.declaration", "allowSubscriptions": "mutuals"});
        crate::test_support::records::self_record(p, ALICE, "app.bsky.notification.declaration", &declaration).await;
        let v = get(ALICE, "app.bsky.notification.declaration", "self", None).await.expect("self record");
        assert_eq!(v.inner()["value"], declaration);
        for (rkey, visibility) in [("3kpoll222222a", "public"), ("3kpoll222222b", "followers")] {
            let poll = json!({"$type": "app.nearhorizon.poll.post", "text": "q", "options": [], "visibility": visibility});
            crate::test_support::records::put(p, ALICE, "app.nearhorizon.poll.post", rkey, &poll).await;
        }
        get(ALICE, "app.nearhorizon.poll.post", "3kpoll222222a", None).await.expect("public poll");
        assert_eq!(error(get(ALICE, "app.nearhorizon.poll.post", "3kpoll222222b", None).await), "RecordNotFound");

        // Taken down: not found. Collapsed (`!hide`): still a record.
        exec(p, &format!("INSERT INTO labels (subject, val) VALUES ('{uri}', '!hide')")).await;
        get(ALICE, "app.bsky.feed.post", "3kaaaaaaaaaa2", None).await.expect("collapsed is served");
        exec(p, &format!("INSERT INTO labels (subject, val) VALUES ('{ALICE}', '!takedown')")).await;
        assert_eq!(error(get(ALICE, "app.bsky.feed.post", "3kaaaaaaaaaa2", None).await), "RecordNotFound");
        db.drop_db().await;
    }
}
