//! `app.bsky.graph.getRelationships` (design-v3 §5.3): the public
//! relationships between one actor and up to 30 others.
//!
//! - **A ready member's** follows and blocks are held in full, in both
//!   directions, so local rows answer. `blockingByList` is the actor's
//!   `listblock` record whose list names the other account. It is computed
//!   only for a moderation list whose owner is a ready member (a list this
//!   AppView serves from its own rows), from the public `list_blocks` and
//!   `list_items` rows. It never reads the subscription tables, so it cannot
//!   depend on anyone's private list mutes.
//! - **Any other actor** is asked of an upstream AppView. When none
//!   answers, local rows answer what they can and
//!   `relationships_partial_total` counts it.
//!
//! **Rows read.** A local `blocks` row is used only when its author is in
//! scope or its subject is a member. A block stored only because its author
//! holds private posts is never read here: showing it would reveal that.
//!
//! `blockedByList` (another account's list block naming the actor) is read
//! from Farsight for a ready member (§5.14.8). It carries the list's URI.

use std::collections::HashMap;

use serde_json::{Map, Value, json};

use super::lists::PURPOSE_MOD;
use super::{Hydrator, ViewError, ViewResult};
use crate::xrpc::pools::Requester;

/// `others` holds at most this many entries.
pub const MAX_OTHERS: usize = 30;
/// `relationships_partial_total`: answers built from local rows because no
/// upstream answered.
pub const METRIC_PARTIAL: &str = "relationships_partial_total";

type Pairs = HashMap<String, String>;

impl Hydrator {
    /// Record URIs by the other party's DID: rows of `table` from `actor`
    /// to any of `others` (`outbound`), or from any of `others` to `actor`.
    async fn edges(&self, table: &str, actor: &str, others: &[String], outbound: bool) -> Result<Vec<(String, String, String)>, sqlx::Error> {
        let (from, to) = if outbound { ("author_did = $1", "subject_did = ANY($2)") } else { ("subject_did = $1", "author_did = ANY($2)") };
        sqlx::query_as(&format!(
            "SELECT author_did, subject_did, uri FROM {table} WHERE {from} AND {to} ORDER BY created_at, uri"
        ))
        .bind(actor)
        .bind(others)
        .fetch_all(self.pool())
        .await
    }

    /// `app.bsky.graph.getRelationships`.
    ///
    /// # Errors
    ///
    /// Database errors.
    pub async fn get_relationships(&self, req: &Requester, actor: &str, others: &[String]) -> ViewResult<Value> {
        if others.len() > MAX_OTHERS {
            return Ok(Err(ViewError::new("InvalidRequest", "Error: others must not have more than 30 elements")));
        }
        let Some(actor) = self.resolve_actor(req, actor).await? else {
            return Ok(Err(ViewError::new("ActorNotFound", "Actor not found")));
        };
        // Each entry of `others`, as given, with the DID it resolves to.
        let mut resolved: Vec<(String, Option<String>)> = Vec::with_capacity(others.len());
        for other in others {
            resolved.push((other.clone(), self.resolve_actor(req, other).await?));
        }
        let dids: Vec<String> = resolved.iter().filter_map(|(_, d)| d.clone()).filter(|d| *d != actor).collect();

        let member = self.is_ready_member(&actor).await?;
        if !member
            && let Some(found) = self.upstream().relationships(req, &actor, &dids).await
        {
            return Ok(Ok(Self::emit(found)));
        }
        if !member {
            metrics::counter!(METRIC_PARTIAL).increment(1);
        }

        let mut following = Pairs::new();
        let mut followed_by = Pairs::new();
        for (_, subject, uri) in self.edges("follows", &actor, &dids, true).await? {
            following.entry(subject).or_insert(uri);
        }
        for (author, _, uri) in self.edges("follows", &actor, &dids, false).await? {
            followed_by.entry(author).or_insert(uri);
        }
        let mut blocking = Pairs::new();
        let mut blocked_by = Pairs::new();
        {
            let scope = self.scope().read_guard().await;
            let readable = |author: &str, subject: &str| scope.in_scope(author) || scope.is_member(subject);
            for (author, subject, uri) in self.edges("blocks", &actor, &dids, true).await? {
                if readable(&author, &subject) {
                    blocking.entry(subject).or_insert(uri);
                }
            }
            for (author, subject, uri) in self.edges("blocks", &actor, &dids, false).await? {
                if readable(&author, &subject) {
                    blocked_by.entry(author).or_insert(uri);
                }
            }
        }
        // The actor's list blocks, for moderation lists served locally.
        let by_list: Pairs = if member {
            sqlx::query_as::<_, (String, String)>(
                "SELECT DISTINCT ON (i.subject_did) i.subject_did, b.uri \
                 FROM list_blocks b \
                 JOIN lists l ON l.uri = b.list_uri AND l.purpose = $3 \
                 JOIN members m ON m.did = l.creator_did AND m.ready_at IS NOT NULL \
                 JOIN list_items i ON i.list_uri = b.list_uri \
                 WHERE b.author_did = $1 AND i.subject_did = ANY($2) \
                 ORDER BY i.subject_did, b.created_at, b.uri",
            )
            .bind(&actor)
            .bind(&dids)
            .bind(PURPOSE_MOD)
            .fetch_all(self.pool())
            .await?
            .into_iter()
            .collect()
        } else {
            Pairs::new()
        };

        // Inbound list blocks, for a ready member, from Farsight on the
        // caller's share: the list's URI, as the reference returns. With no
        // answer the field is left out and the response still goes out.
        let blocked_by_list: Pairs = match self.farsight().filter(|_| member) {
            Some(farsight) => {
                let (lists, complete) = farsight.request(req).relationship_lists(&actor, &dids, self.cache()).await;
                if !complete {
                    metrics::counter!(METRIC_PARTIAL).increment(1);
                }
                lists
            }
            None => Pairs::new(),
        };

        let relationships: Vec<Value> = resolved
            .iter()
            .map(|(given, did)| {
                let Some(did) = did else {
                    return json!({"$type": "app.bsky.graph.defs#notFoundActor", "actor": given, "notFound": true});
                };
                let mut r = Map::new();
                r.insert("$type".into(), json!("app.bsky.graph.defs#relationship"));
                r.insert("did".into(), json!(did));
                for (key, pairs) in [
                    ("following", &following),
                    ("followedBy", &followed_by),
                    ("blocking", &blocking),
                    ("blockedBy", &blocked_by),
                    ("blockingByList", &by_list),
                    ("blockedByList", &blocked_by_list),
                ] {
                    if let Some(uri) = pairs.get(did) {
                        r.insert(key.into(), json!(uri));
                    }
                }
                Value::Object(r)
            })
            .collect();
        Ok(Ok(Self::emit(json!({"actor": actor, "relationships": relationships}))))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::aggregations::ScopeSets;
    use crate::hydration::lists::PURPOSE_CURATE;
    use crate::hydration::testing::{FixedUpstream, actor, exec, hydrator, hydrator_with};
    use crate::test_support::fresh_db;

    const ALICE: &str = "did:plc:aaaaaaaaaaaaaaaaaaaaaaaa";
    const BOB: &str = "did:plc:bbbbbbbbbbbbbbbbbbbbbbbb";
    const CAROL: &str = "did:plc:cccccccccccccccccccccccc";
    const DAVE: &str = "did:plc:dddddddddddddddddddddddd";
    const OWNER: &str = "did:plc:oooooooooooooooooooooooo";

    async fn edge(p: &sqlx::PgPool, table: &str, author: &str, subject: &str, rkey: &str) -> String {
        let collection = if table == "follows" { "follow" } else { "block" };
        let uri = format!("at://{author}/app.bsky.graph.{collection}/{rkey}");
        exec(p, &format!(
            "INSERT INTO {table} (uri, author_did, subject_did, rev, created_at) VALUES ('{uri}', '{author}', '{subject}', '3k', now())"
        )).await;
        uri
    }

    async fn list_with(p: &sqlx::PgPool, owner: &str, rkey: &str, purpose: &str, subject: &str) -> String {
        let uri = format!("at://{owner}/app.bsky.graph.list/{rkey}");
        exec(p, &format!(
            "INSERT INTO lists (uri, creator_did, rkey, cid, rev, name, purpose, record, created_at) VALUES \
             ('{uri}', '{owner}', '{rkey}', 'bafy', '3k', 'L', '{purpose}', '{{}}', now())"
        )).await;
        exec(p, &format!(
            "INSERT INTO list_items (uri, author_did, rkey, cid, rev, list_uri, subject_did, created_at) VALUES \
             ('at://{owner}/app.bsky.graph.listitem/{rkey}', '{owner}', '{rkey}', 'bafy', '3k', '{uri}', '{subject}', now())"
        )).await;
        uri
    }

    async fn list_block(p: &sqlx::PgPool, author: &str, rkey: &str, list: &str) -> String {
        let uri = format!("at://{author}/app.bsky.graph.listblock/{rkey}");
        exec(p, &format!(
            "INSERT INTO list_blocks (uri, author_did, rkey, cid, rev, list_uri, created_at) VALUES \
             ('{uri}', '{author}', '{rkey}', 'bafy', '3k', '{list}', now())"
        )).await;
        uri
    }

    /// B-55: a ready member's relationships come from local rows.
    #[tokio::test]
    async fn a_ready_members_relationships_are_local() {
        let Some(db) = fresh_db("hydration_relationships").await else {
            return;
        };
        let p = &db.pool;
        for (d, h) in [(ALICE, "alice.test"), (BOB, "bob.test"), (CAROL, "carol.test"), (DAVE, "dave.test")] {
            actor(p, d, h, h).await;
        }
        exec(p, &format!("INSERT INTO members (did, source, ready_at) VALUES ('{ALICE}', 'admin', now()), ('{OWNER}', 'admin', now())")).await;
        let follows = edge(p, "follows", ALICE, BOB, "f1").await;
        let followed = edge(p, "follows", BOB, ALICE, "f2").await;
        let blocks = edge(p, "blocks", ALICE, CAROL, "b1").await;
        let blocked = edge(p, "blocks", DAVE, ALICE, "b2").await;
        // A moderation list of a ready member names Bob; a curate list of
        // the same owner names Carol; a moderation list of a non-member
        // names Dave. Alice list-blocks all three.
        let mods = list_with(p, OWNER, "mods", PURPOSE_MOD, BOB).await;
        let curated = list_with(p, OWNER, "cur", PURPOSE_CURATE, CAROL).await;
        let foreign = list_with(p, BOB, "theirs", PURPOSE_MOD, DAVE).await;
        let by_list = list_block(p, ALICE, "lb1", &mods).await;
        list_block(p, ALICE, "lb2", &curated).await;
        list_block(p, ALICE, "lb3", &foreign).await;
        let mut sets = ScopeSets::default();
        sets.members.insert(ALICE.into());
        for d in [ALICE, BOB, CAROL, DAVE] {
            sets.scope.insert(d.into());
        }
        let h = hydrator(p, sets);
        let req = Requester::test();

        let others = vec!["bob.test".to_string(), CAROL.to_string(), DAVE.to_string(), "nobody.invalid".to_string()];
        let v = h.get_relationships(&req, ALICE, &others).await.expect("db").expect("view");
        let v = v.inner();
        assert_eq!(v["actor"], ALICE);
        let r = v["relationships"].as_array().expect("relationships");
        assert_eq!(r[0], json!({"$type": "app.bsky.graph.defs#relationship", "did": BOB, "following": follows, "followedBy": followed, "blockingByList": by_list}));
        // A curate list does not block, and a list this AppView does not
        // serve locally is left out.
        assert_eq!(r[1], json!({"$type": "app.bsky.graph.defs#relationship", "did": CAROL, "blocking": blocks}));
        assert_eq!(r[2], json!({"$type": "app.bsky.graph.defs#relationship", "did": DAVE, "blockedBy": blocked}));
        assert_eq!(r[3], json!({"$type": "app.bsky.graph.defs#notFoundActor", "actor": "nobody.invalid", "notFound": true}));

        let e = h.get_relationships(&req, "nobody.invalid", &[]).await.expect("db").expect_err("no actor");
        assert_eq!(e.error, "ActorNotFound");
        let many: Vec<String> = (0..31).map(|_| BOB.to_string()).collect();
        assert!(h.get_relationships(&req, ALICE, &many).await.expect("db").is_err());
        db.drop_db().await;
    }

    /// B-55, B-53: any other actor is asked upstream; with no answer, local
    /// rows answer, and a block stored only for a private owner is not read.
    #[tokio::test]
    async fn other_actors_are_asked_upstream_then_answered_in_part() {
        let Some(db) = fresh_db("hydration_relationships_partial").await else {
            return;
        };
        let p = &db.pool;
        actor(p, BOB, "bob.test", "Bob").await;
        // Carol is in scope and blocks Bob; Dave is out of scope (a private
        // owner's block, stored by rule 3) and blocks Bob too.
        let seen = edge(p, "blocks", CAROL, BOB, "b1").await;
        edge(p, "blocks", DAVE, BOB, "b2").await;
        let mut sets = ScopeSets::default();
        sets.scope.insert(BOB.into());
        sets.scope.insert(CAROL.into());
        let others = vec![CAROL.to_string(), DAVE.to_string()];
        let req = Requester::test();

        let partial = hydrator(p, sets.clone());
        let v = partial.get_relationships(&req, BOB, &others).await.expect("db").expect("view");
        let r = v.inner()["relationships"].as_array().expect("relationships").clone();
        assert_eq!(r[0]["blockedBy"], seen);
        assert_eq!(r[1], json!({"$type": "app.bsky.graph.defs#relationship", "did": DAVE}));

        let mut up = FixedUpstream::default();
        let answer = json!({"actor": BOB, "relationships": [{"$type": "app.bsky.graph.defs#relationship", "did": CAROL, "followedBy": "at://x/app.bsky.graph.follow/1"}]});
        up.relationships.insert(BOB.to_string(), answer.clone());
        let asked = hydrator_with(p, sets, Arc::new(up));
        let v = asked.get_relationships(&req, BOB, &others).await.expect("db").expect("view");
        assert_eq!(v.inner(), &answer);
        db.drop_db().await;
    }
}
