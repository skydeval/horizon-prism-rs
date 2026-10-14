//! A member's own list mutes and list blocks (design-v3 §5.3):
//! `muteActorList`, `unmuteActorList`, `getListMutes`, `getListBlocks`.
//!
//! A list mute is private to the member and kept only here. Muting runs
//! the member phase of rule 4 (§4.3): the member's own rows and a change
//! marker, with no lock any other member's request can wait on. The mute
//! starts to hide anything only after the list's owner has been synced.
//!
//! Only a moderation list can be muted. The lexicon does not say so; this
//! AppView does, because hiding the members of a curated or reference list
//! is not what those lists are for.

use chrono::{DateTime, Utc};
use serde_json::{Value, json};

use super::feed::{local_cursor, parse_local_cursor};
use super::lists::{PURPOSE_MOD, is_list_uri};
use super::{HydrationError, Hydrator, ViewError, ViewResult, Viewer};
use crate::db::locks::uri_author;
use crate::moderation::{self, Gate};
use crate::subscriptions::references::{Muted, OverBudget, mute_list, unmute_list};
use crate::subscriptions::{retryable, retrying};
use crate::xrpc::pools::Requester;

fn invalid(message: impl Into<String>) -> ViewError {
    ViewError::new("InvalidRequest", message)
}

impl Hydrator {
    /// `muteActorList` (`mute`) and `unmuteActorList`.
    ///
    /// # Errors
    ///
    /// Database errors.
    pub async fn set_list_mute(&self, viewer: &Viewer, req: &Requester, list: &str, mute: bool) -> ViewResult<Value> {
        let Some(me) = viewer.did.as_deref() else {
            return Ok(Err(ViewError::new("AuthRequired", "Authentication required")));
        };
        // Any other collection is refused from the URI alone, with no fetch.
        if !is_list_uri(list) {
            return Ok(Err(invalid("Error: list must be the at-uri of an app.bsky.graph.list record")));
        }
        let budget = self.inner.cfg.list_budget;
        let changed = if mute {
            let Some((row, _)) = self.load_list(req, list).await? else {
                return Ok(Err(invalid("List not found")));
            };
            if row.purpose != PURPOSE_MOD {
                return Ok(Err(invalid("Only a moderation list can be muted")));
            }
            // A lost deadlock never reaches the client (§4.3 "Lock order").
            match retrying(retryable, || mute_list(self.pool(), me, list, budget)).await? {
                Muted::Added => true,
                Muted::Already => false,
                Muted::Refused(OverBudget::References) => {
                    return Ok(Err(invalid(format!("List mute limit reached: at most {} lists can be muted", budget.max_references))));
                }
                Muted::Refused(OverBudget::Owners) => {
                    return Ok(Err(invalid(format!(
                        "List mute limit reached: muted lists can belong to at most {} different accounts",
                        budget.max_owners
                    ))));
                }
            }
        } else {
            retrying(retryable, || unmute_list(self.pool(), me, list, budget)).await?
        };
        if changed && let Some(applier) = &self.inner.cfg.list_applier {
            applier.signal();
        }
        Ok(Ok(Self::emit(json!({}))))
    }

    /// `listView`s for `lists`, newest first as given. A list that no
    /// longer exists, or is hidden, is left out.
    async fn own_list_views(&self, viewer: &Viewer, req: &Requester, lists: &[String]) -> Result<Vec<Value>, HydrationError> {
        let mut rows = Vec::new();
        for uri in lists {
            if let Some((row, local)) = self.load_list(req, uri).await? {
                let count = if local { Some(self.list_item_count(uri).await?) } else { None };
                rows.push((row, count));
            }
        }
        let uris: Vec<String> = rows.iter().map(|(r, _)| r.uri.clone()).collect();
        let mut owners: Vec<String> = uris.iter().filter_map(|u| uri_author(u).map(str::to_string)).collect();
        owners.sort_unstable();
        owners.dedup();
        let gates = moderation::gate_many(self.pool(), &uris).await?;
        let creators = self.profile_context(viewer, req, &owners, None).await?;
        let labels = self.load_labels(&uris).await?;
        let list_viewer = self.list_viewer(viewer, &uris).await?;
        Ok(rows
            .iter()
            .filter(|(r, _)| !gates.get(&r.uri).copied().unwrap_or(Gate::Visible).is_hidden())
            .map(|(r, count)| Self::list_view(r, &creators, *count, &labels, &list_viewer))
            .collect())
    }

    /// One page of the viewer's own list references of a kind, as
    /// `{lists, cursor?}`. `sql` selects `(list_uri, created_at)` for
    /// `$1 = viewer`, before the cursor `($2, $3)`, limited to `$4`.
    async fn own_lists(&self, viewer: &Viewer, req: &Requester, sql: &str, limit: u32, cursor: Option<&str>) -> ViewResult<Value> {
        let Some(me) = viewer.did.as_deref() else {
            return Ok(Err(ViewError::new("AuthRequired", "Authentication required")));
        };
        let (at, after) = parse_local_cursor(cursor);
        let page: Vec<(String, DateTime<Utc>)> = sqlx::query_as(sql)
            .bind(me)
            .bind(at)
            .bind(after.unwrap_or_default())
            .bind(i64::from(limit))
            .fetch_all(self.pool())
            .await?;
        let next = (page.len() == usize::try_from(limit).unwrap_or(usize::MAX))
            .then(|| page.last().map(|(uri, at)| local_cursor(*at, uri)))
            .flatten();
        let uris: Vec<String> = page.into_iter().map(|(uri, _)| uri).collect();
        let mut out = json!({ "lists": self.own_list_views(viewer, req, &uris).await? });
        if let Some(c) = next {
            out["cursor"] = json!(c);
        }
        Ok(Ok(Self::emit(out)))
    }

    /// `app.bsky.graph.getListMutes`: the lists the viewer mutes, newest
    /// first. Every stored mute is listed, whether or not it has an effect
    /// yet.
    ///
    /// # Errors
    ///
    /// Database errors.
    pub async fn get_list_mutes(&self, viewer: &Viewer, req: &Requester, limit: u32, cursor: Option<&str>) -> ViewResult<Value> {
        let sql = "SELECT list_uri, created_at FROM list_mutes WHERE viewer_did = $1 \
                     AND ($2::timestamptz IS NULL OR (created_at, list_uri) < ($2, $3)) \
                   ORDER BY created_at DESC, list_uri DESC LIMIT $4";
        self.own_lists(viewer, req, sql, limit, cursor).await
    }

    /// `app.bsky.graph.getListBlocks`: the lists the viewer blocks, by its
    /// own `listblock` records, newest first. Two records for one list are
    /// one entry, at the earlier record's time.
    ///
    /// # Errors
    ///
    /// Database errors.
    pub async fn get_list_blocks(&self, viewer: &Viewer, req: &Requester, limit: u32, cursor: Option<&str>) -> ViewResult<Value> {
        let sql = "SELECT list_uri, created_at FROM ( \
                     SELECT list_uri, min(created_at) AS created_at FROM list_blocks \
                     WHERE author_did = $1 AND split_part(list_uri, '/', 4) = 'app.bsky.graph.list' GROUP BY list_uri) b \
                   WHERE ($2::timestamptz IS NULL OR (created_at, list_uri) < ($2, $3)) \
                   ORDER BY created_at DESC, list_uri DESC LIMIT $4";
        self.own_lists(viewer, req, sql, limit, cursor).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::aggregations::ScopeSets;
    use crate::hydration::lists::PURPOSE_CURATE;
    use crate::hydration::testing::{FixedUpstream, actor, exec, hydrator, hydrator_with};
    use crate::test_support::fresh_db;

    const ALICE: &str = "did:plc:aaaaaaaaaaaaaaaaaaaaaaaa";
    const BOB: &str = "did:plc:bbbbbbbbbbbbbbbbbbbbbbbb";

    async fn list(p: &sqlx::PgPool, rkey: &str, purpose: &str) -> String {
        let uri = format!("at://{BOB}/app.bsky.graph.list/{rkey}");
        exec(
            p,
            &format!(
                "INSERT INTO lists (uri, creator_did, rkey, cid, rev, name, purpose, record, created_at) VALUES \
                 ('{uri}', '{BOB}', '{rkey}', 'bafylist', '3k', '{rkey}', '{purpose}', '{{}}', now())"
            ),
        )
        .await;
        uri
    }

    /// A list mute or list block hides its list's members only when it
    /// applies: admitted, served by a sync, on an active moderation list.
    #[tokio::test]
    async fn only_a_reference_that_applies_hides_anyone() {
        const CAROL: &str = "did:plc:cccccccccccccccccccccccc";
        const OWNER: &str = "did:plc:oooooooooooooooooooooooo";
        let Some(db) = fresh_db("hydration_list_mutes_apply").await else {
            return;
        };
        let p = &db.pool;
        for (did, handle) in [(ALICE, "alice.test"), (BOB, "bob.test"), (CAROL, "carol.test")] {
            actor(p, did, handle, handle).await;
        }
        exec(p, &format!("INSERT INTO members (did, source, ready_at) VALUES ('{ALICE}', 'admin', now()), ('{BOB}', 'admin', now())")).await;
        // One list of a ready member (read from its row) and one of an
        // outside owner (read from its record). Carol is on both.
        let local = list(p, "3kmods", PURPOSE_MOD).await;
        let outside = format!("at://{OWNER}/app.bsky.graph.list/3kout");
        let mut up = FixedUpstream::default();
        up.records.insert(
            outside.clone(),
            json!({"uri": outside, "cid": "bafyout", "value": {"$type": "app.bsky.graph.list", "name": "Outside", "purpose": PURPOSE_MOD, "createdAt": "2026-01-01T00:00:00Z"}}),
        );
        let h = hydrator_with(p, ScopeSets::default(), std::sync::Arc::new(up));
        let me = Viewer { did: Some(ALICE.into()), hydrate: true };
        for (uri, owner) in [(&local, BOB), (&outside, OWNER)] {
            exec(p, &format!("INSERT INTO subscribed_lists (list_uri, owner_did, state, purpose) VALUES ('{uri}', '{owner}', 'active', '{PURPOSE_MOD}')")).await;
            exec(
                p,
                &format!(
                    "INSERT INTO subscribed_list_items (uri, list_uri, owner_did, rkey, cid, rev, subject_did) VALUES \
                     ('at://{owner}/app.bsky.graph.listitem/3kc', '{uri}', '{owner}', '3kc', 'c', '3k', '{CAROL}'), \
                     ('at://{owner}/app.bsky.graph.listitem/3ka', '{uri}', '{owner}', '3ka', 'c', '3k', '{ALICE}')"
                ),
            )
            .await;
        }
        let reference = |uri: &String, source: &'static str, effective: bool| {
            let uri = uri.clone();
            async move {
                exec(
                    p,
                    &format!(
                        "INSERT INTO list_references (member_did, list_uri, source, admitted, effective) VALUES \
                         ('{ALICE}', '{uri}', '{source}', true, {effective}) \
                         ON CONFLICT (member_did, list_uri, source) DO UPDATE SET effective = EXCLUDED.effective"
                    ),
                )
                .await;
            }
        };
        let state = || {
            let (h, me) = (&h, &me);
            async move { h.actor_viewer(me, &[CAROL.to_string(), ALICE.to_string()]).await.expect("viewer") }
        };
        exec(
            p,
            &format!(
                "INSERT INTO notifications (recipient_did, author_did, reason, record_uri, record_cid) VALUES \
                 ('{ALICE}', '{CAROL}', 'follow', 'at://{CAROL}/app.bsky.graph.follow/3k', 'bafy')"
            ),
        )
        .await;
        let unread = || {
            let (h, me) = (&h, &me);
            async move { h.unread_count(me).await.expect("db").expect("count").inner()["count"].clone() }
        };

        // Admitted but not yet served by a sync: no effect anywhere.
        reference(&outside, "mute", false).await;
        let s = state().await;
        assert!(!s.muted.contains(CAROL) && !s.blocked_either_way(CAROL));
        assert_eq!(unread().await, json!(1));

        // Served: the mute applies. A profile view names the list, read
        // from its record as any list of an outside owner is.
        reference(&outside, "mute", true).await;
        let s = state().await;
        assert!(s.muted.contains(CAROL) && !s.blocked_either_way(CAROL));
        assert_eq!(s.view(CAROL)["muted"], json!(true));
        let profiles = h.profile_context(&me, &Requester::test(), &[CAROL.to_string()], None).await.expect("profiles");
        let by_list = &profiles.viewer.view(CAROL)["mutedByList"];
        assert_eq!((&by_list["uri"], &by_list["name"], &by_list["viewer"]["muted"]), (&json!(outside), &json!("Outside"), &json!(false)));
        // The viewer's own entry on the list does nothing to the viewer.
        assert!(!s.muted.contains(ALICE));
        assert_eq!(unread().await, json!(0));

        // A list that is not a moderation list (any more) is dormant.
        exec(p, &format!("UPDATE subscribed_lists SET purpose = '{PURPOSE_CURATE}' WHERE list_uri = '{outside}'")).await;
        assert!(!state().await.muted.contains(CAROL));
        assert_eq!(unread().await, json!(1));
        exec(p, &format!("UPDATE subscribed_lists SET purpose = NULL WHERE list_uri = '{outside}'")).await;
        assert!(!state().await.muted.contains(CAROL));

        // A list of a ready member: the view names the list.
        reference(&local, "mute", true).await;
        let s = state().await;
        assert_eq!(s.view(CAROL)["mutedByList"]["uri"], json!(local));
        assert_eq!(s.view(CAROL)["mutedByList"]["name"], json!("3kmods"));
        // A list block applies as a block does, outbound.
        reference(&local, "block", true).await;
        let s = state().await;
        assert!(s.blocked_either_way(CAROL));
        assert_eq!(s.view(CAROL)["blockingByList"]["uri"], json!(local));
        assert_eq!(s.view(CAROL)["blockedBy"], json!(false));
        // A list being released applies no longer.
        exec(p, "UPDATE subscribed_lists SET state = 'unreferenced'").await;
        let s = state().await;
        assert!(!s.muted.contains(CAROL) && !s.blocked_either_way(CAROL));
        // Nobody else's view is touched by Alice's references.
        let bob = h.actor_viewer(&Viewer { did: Some(BOB.into()), hydrate: true }, &[CAROL.to_string()]).await.expect("viewer");
        assert!(!bob.muted.contains(CAROL) && !bob.blocked_either_way(CAROL));

        // Blocks a request knows beyond local rows (Thread Lens) apply to
        // every view it builds, in the direction it found them. A block
        // with no record here hides, and carries no `blocking` URI.
        let known = std::sync::Arc::new(crate::hydration::KnownBlocks {
            by_viewer: [CAROL.to_string()].into(),
            of_viewer: [BOB.to_string(), ALICE.to_string()].into(),
        });
        let s = crate::hydration::with_known_blocks(known, h.actor_viewer(&me, &[CAROL.to_string(), BOB.to_string(), ALICE.to_string()]))
            .await
            .expect("viewer");
        assert!(s.blocked_either_way(CAROL) && s.blocked_either_way(BOB) && !s.blocked_either_way(ALICE));
        assert!(s.view(CAROL).get("blocking").is_none());
        assert_eq!((&s.view(CAROL)["blockedBy"], &s.view(BOB)["blockedBy"]), (&json!(false), &json!(true)));
        // Outside such a request nothing of it remains.
        assert!(!state().await.blocked_either_way(BOB));
        db.drop_db().await;
    }

    /// A Thread Lens visitor (signed in, not a ready member): inside a
    /// request that supplies its blocks, `actor_viewer` returns those
    /// blocks and nothing else, and reads no table. Outside one, and for
    /// an anonymous viewer, it returns nothing, as before.
    #[tokio::test]
    async fn a_visitor_gets_the_requests_blocks_and_no_other_state() {
        const CAROL: &str = "did:plc:cccccccccccccccccccccccc";
        // A pool that cannot connect: any table read would be an error.
        let pool = sqlx::PgPool::connect_lazy("postgres://nobody@127.0.0.1:1/none").expect("lazy pool");
        let h = hydrator(&pool, ScopeSets::default());
        let visitor = Viewer { did: Some(ALICE.into()), hydrate: false };
        let dids = [CAROL.to_string(), BOB.to_string(), ALICE.to_string()];
        // The visitor blocks Carol; Bob blocks the visitor. The visitor's
        // own DID among the blockers is left out, as for a member.
        let known = || {
            std::sync::Arc::new(crate::hydration::KnownBlocks {
                by_viewer: [CAROL.to_string()].into(),
                of_viewer: [BOB.to_string(), ALICE.to_string()].into(),
            })
        };

        let s = crate::hydration::with_known_blocks(known(), h.actor_viewer(&visitor, &dids)).await.expect("no table is read");
        // The inbound blocker is named; the outbound one is treated as
        // blocked without a `blocking` field, which needs a record's URI.
        assert_eq!(s.view(BOB), json!({"muted": false, "blockedBy": true}));
        assert_eq!(s.view(CAROL), json!({"muted": false, "blockedBy": false}));
        assert!(s.blocked_either_way(BOB) && s.blocked_either_way(CAROL) && !s.blocked_either_way(ALICE));
        assert!(s.blocking.is_empty() && s.following.is_empty() && s.followed_by.is_empty() && s.muted.is_empty());

        // Outside such a request a non-member has no viewer state at all.
        let s = h.actor_viewer(&visitor, &dids).await.expect("no table is read");
        assert!(s.blocked_by.is_empty() && !s.blocked_either_way(BOB) && !s.blocked_either_way(CAROL));
        assert_eq!(s.view(BOB), json!({"muted": false, "blockedBy": false}));
        // An anonymous viewer has none inside one either.
        let s = crate::hydration::with_known_blocks(known(), h.actor_viewer(&Viewer::anonymous(), &dids)).await.expect("no table is read");
        assert!(s.blocked_by.is_empty() && !s.blocked_either_way(BOB) && !s.blocked_either_way(CAROL));
    }

    /// A moderation list can be muted, up to the member's budget; any
    /// other list, or anything that is not a list, is refused; the mutes
    /// and the blocks are listed newest first.
    #[tokio::test]
    async fn list_mutes_and_list_blocks() {
        let Some(db) = fresh_db("hydration_list_mutes").await else {
            return;
        };
        let p = &db.pool;
        actor(p, ALICE, "alice.test", "Alice").await;
        actor(p, BOB, "bob.test", "Bob").await;
        // Bob is a ready member, so his lists are read from their rows.
        exec(p, &format!("INSERT INTO members (did, source, ready_at) VALUES ('{BOB}', 'admin', now()), ('{ALICE}', 'admin', now())")).await;
        let mut sets = ScopeSets::default();
        for did in [ALICE, BOB] {
            sets.members.insert(did.into());
            sets.scope.insert(did.into());
        }
        let h = hydrator(p, sets);
        let req = Requester::test();
        let me = Viewer { did: Some(ALICE.into()), hydrate: true };
        let mods = list(p, "3kmods", PURPOSE_MOD).await;
        let more = list(p, "3kmore", PURPOSE_MOD).await;
        let third = list(p, "3kthird", PURPOSE_MOD).await;
        let curated = list(p, "3kcurated", PURPOSE_CURATE).await;
        let count = |sql: &'static str| async move { sqlx::query_scalar::<_, i64>(sql).fetch_one(p).await.expect(sql) };

        let refused = |list: String| {
            let (h, me, req) = (&h, &me, &req);
            async move { h.set_list_mute(me, req, &list, true).await.expect("db").expect_err("refused").error }
        };
        assert_eq!(refused(curated).await, "InvalidRequest");
        assert_eq!(refused(format!("at://{BOB}/app.bsky.graph.list/3kmissing")).await, "InvalidRequest");
        assert_eq!(refused(format!("at://{BOB}/app.bsky.feed.post/3kpost")).await, "InvalidRequest");
        assert_eq!(count("SELECT count(*) FROM list_mutes").await, 0);

        // The test budget is two lists.
        h.set_list_mute(&me, &req, &mods, true).await.expect("db").expect("muted");
        h.set_list_mute(&me, &req, &mods, true).await.expect("db").expect("again, the same answer");
        h.set_list_mute(&me, &req, &more, true).await.expect("db").expect("muted");
        assert_eq!(refused(third.clone()).await, "InvalidRequest");
        assert_eq!(count("SELECT count(*) FROM list_mutes").await, 2);
        assert_eq!(count("SELECT count(*) FROM list_references WHERE source = 'mute' AND admitted").await, 2);

        let listed = h.get_list_mutes(&me, &req, 50, None).await.expect("db").expect("mutes").inner().clone();
        let names: Vec<&str> = listed["lists"].as_array().expect("lists").iter().filter_map(|l| l["name"].as_str()).collect();
        assert_eq!(names, ["3kmore", "3kmods"]);
        assert_eq!(listed["lists"][0]["viewer"]["muted"], json!(true));
        assert_eq!(listed["lists"][0]["creator"]["did"], json!(BOB));
        // Paged one at a time.
        let first = h.get_list_mutes(&me, &req, 1, None).await.expect("db").expect("page").inner().clone();
        let cursor = first["cursor"].as_str().expect("cursor");
        let second = h.get_list_mutes(&me, &req, 1, Some(cursor)).await.expect("db").expect("page").inner().clone();
        assert_eq!(second["lists"][0]["name"], json!("3kmods"));

        // Unmuting frees the budget; a second unmute is the same answer.
        h.set_list_mute(&me, &req, &mods, false).await.expect("db").expect("unmuted");
        h.set_list_mute(&me, &req, &mods, false).await.expect("db").expect("again");
        h.set_list_mute(&me, &req, &third, true).await.expect("db").expect("muted");
        assert_eq!(count("SELECT count(*) FROM list_references WHERE source = 'mute'").await, 2);

        // List blocks: one entry per list, whatever the number of records.
        for (rkey, target) in [("3kb1", &mods), ("3kb2", &mods), ("3kb3", &more)] {
            exec(
                p,
                &format!(
                    "INSERT INTO list_blocks (uri, author_did, rkey, cid, rev, list_uri, created_at) VALUES \
                     ('at://{ALICE}/app.bsky.graph.listblock/{rkey}', '{ALICE}', '{rkey}', 'c', '3k', '{target}', now())"
                ),
            )
            .await;
        }
        let blocked = h.get_list_blocks(&me, &req, 50, None).await.expect("db").expect("blocks").inner().clone();
        assert_eq!(blocked["lists"].as_array().map(Vec::len), Some(2));
        assert!(blocked["lists"][0]["viewer"]["blocked"].is_string());
        // Someone else's mutes are their own.
        let bob = h.get_list_mutes(&Viewer { did: Some(BOB.into()), hydrate: true }, &req, 50, None).await.expect("db").expect("mutes").inner().clone();
        assert_eq!(bob["lists"], json!([]));
        db.drop_db().await;
    }
}
