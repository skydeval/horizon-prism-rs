//! Activity subscriptions (design-v3 §5.6): a member asks to be notified
//! when an account posts. `putActivitySubscription` and
//! `listActivitySubscriptions`; the rows are AppView-private.
//!
//! - Turning a subscription off always works: no check, no readiness.
//! - Turning one on needs a ready member, a subject this AppView indexes
//!   (in scope), room under the member's cap, and the subject's permission:
//!   its `app.bsky.notification.declaration`, where no record means
//!   "followers".
//! - No fetch is made for the declaration. A subject whose declaration is
//!   not stored yet reads as "followers"; the permission is checked again
//!   when a notification would be generated.

use chrono::{DateTime, Utc};
use serde_json::{Value, json};

use super::actor::ProfileKind;
use super::feed::{local_cursor, parse_local_cursor};
use super::{Hydrator, ViewError, ViewResult, Viewer};
use crate::xrpc::pools::Requester;

pub const NSID_DECLARATION: &str = "app.bsky.notification.declaration";
const METRIC_OUT_OF_SCOPE: &str = crate::db::retention::METRIC_SUBSCRIPTIONS_OUT_OF_SCOPE;

fn invalid(message: &str) -> ViewError {
    ViewError::new("InvalidRequest", message)
}

impl Hydrator {
    /// Whether `subject`'s declaration lets `me` subscribe: `followers`
    /// (also with no record) needs `me` to follow the subject, `mutuals`
    /// the follow back as well, `none` nobody.
    pub(crate) async fn may_subscribe(&self, me: &str, subject: &str) -> Result<bool, sqlx::Error> {
        let record: Option<Value> = sqlx::query_scalar("SELECT record FROM actor_self_records WHERE did = $1 AND collection = $2")
            .bind(subject)
            .bind(NSID_DECLARATION)
            .fetch_optional(self.pool())
            .await?;
        let allow = record
            .as_ref()
            .and_then(|r| r.get("allowSubscriptions"))
            .and_then(Value::as_str)
            .unwrap_or("followers");
        let follows = |from: &'_ str, to: &'_ str| {
            let (from, to) = (from.to_string(), to.to_string());
            async move {
                sqlx::query_scalar::<_, bool>("SELECT EXISTS (SELECT 1 FROM follows WHERE author_did = $1 AND subject_did = $2)")
                    .bind(from)
                    .bind(to)
                    .fetch_one(self.pool())
                    .await
            }
        };
        Ok(match allow {
            "followers" => follows(me, subject).await?,
            "mutuals" => follows(me, subject).await? && follows(subject, me).await?,
            // `none`, and any value this version does not know.
            _ => false,
        })
    }

    /// `app.bsky.notification.putActivitySubscription`.
    ///
    /// # Errors
    ///
    /// Database errors.
    pub async fn put_activity_subscription(&self, viewer: &Viewer, subject: &str, post: bool, reply: bool) -> ViewResult<Value> {
        let Some(me) = viewer.did.as_deref() else {
            return Ok(Err(ViewError::new("AuthRequired", "Authentication required")));
        };
        if !subject.starts_with("did:") {
            return Ok(Err(invalid("subject must be a DID")));
        }
        if subject == me {
            return Ok(Err(invalid("Cannot subscribe to own activity")));
        }
        if !post && !reply {
            sqlx::query("DELETE FROM activity_subscriptions WHERE subscriber_did = $1 AND subject_did = $2")
                .bind(me)
                .bind(subject)
                .execute(self.pool())
                .await?;
            return Ok(Ok(Self::emit(json!({ "subject": subject }))));
        }
        // The declaration check reads the member's follows, which are all
        // here only once the member is ready.
        if !viewer.hydrate {
            return Ok(Err(ViewError::new("NotReady", "Account indexing in progress")));
        }
        if !self.in_scope(subject).await {
            metrics::counter!(METRIC_OUT_OF_SCOPE, "action" => "rejected").increment(1);
            return Ok(Err(invalid(
                "this account is not indexed by this service's community, so a subscription would never notify",
            )));
        }
        let (held, existing): (i64, bool) = sqlx::query_as(
            "SELECT count(*), COALESCE(bool_or(subject_did = $2), false) FROM activity_subscriptions WHERE subscriber_did = $1",
        )
        .bind(me)
        .bind(subject)
        .fetch_one(self.pool())
        .await?;
        if !existing && held >= i64::from(self.inner.cfg.activity_subscriptions_max) {
            return Ok(Err(invalid("Activity subscription limit reached")));
        }
        if !self.may_subscribe(me, subject).await? {
            return Ok(Err(invalid("This account does not allow this subscription")));
        }
        // An active subscription holds its subject in scope (scope-recency
        // design §3.6). The scope check and the upsert's commit happen under
        // one read guard, so the daily sweep's marker, which holds the write
        // guard, either sees the row or runs first, and then this request
        // finds the subject gone.
        let guard = self.scope().read_guard().await;
        if !guard.in_scope(subject) {
            drop(guard);
            metrics::counter!(METRIC_OUT_OF_SCOPE, "action" => "rejected").increment(1);
            return Ok(Err(invalid(
                "this account is not indexed by this service's community, so a subscription would never notify",
            )));
        }
        #[cfg(test)]
        crate::test_support::hooks::pause(crate::test_support::hooks::SUBSCRIBE_HOLDS_GUARD, subject).await;
        sqlx::query(
            "INSERT INTO activity_subscriptions (subscriber_did, subject_did, post, reply) VALUES ($1, $2, $3, $4) \
             ON CONFLICT (subscriber_did, subject_did) DO UPDATE SET post = EXCLUDED.post, reply = EXCLUDED.reply, updated_at = now()",
        )
        .bind(me)
        .bind(subject)
        .bind(post)
        .bind(reply)
        .execute(self.pool())
        .await?;
        drop(guard);
        Ok(Ok(Self::emit(json!({"subject": subject, "activitySubscription": {"post": post, "reply": reply}}))))
    }

    /// `app.bsky.notification.listActivitySubscriptions`: the member's
    /// subscriptions, most recently changed first.
    ///
    /// # Errors
    ///
    /// Database errors.
    pub async fn list_activity_subscriptions(&self, viewer: &Viewer, req: &Requester, limit: u32, cursor: Option<&str>) -> ViewResult<Value> {
        let Some(me) = viewer.did.as_deref() else {
            return Ok(Err(ViewError::new("AuthRequired", "Authentication required")));
        };
        // The subjects come from the viewer's subscriptions (design-v4 §8.1).
        let req = &req.viewer_private(me);
        let (at, after) = parse_local_cursor(cursor);
        let rows: Vec<(String, bool, bool, DateTime<Utc>)> = sqlx::query_as(
            "SELECT subject_did, post, reply, updated_at FROM activity_subscriptions WHERE subscriber_did = $1 \
               AND ($2::timestamptz IS NULL OR (updated_at, subject_did) < ($2, $3)) \
             ORDER BY updated_at DESC, subject_did DESC LIMIT $4",
        )
        .bind(me)
        .bind(at)
        .bind(after.unwrap_or_default())
        .bind(i64::from(limit))
        .fetch_all(self.pool())
        .await?;
        let next = (rows.len() == usize::try_from(limit).unwrap_or(usize::MAX))
            .then(|| rows.last().map(|(d, _, _, t)| local_cursor(*t, d)))
            .flatten();
        let dids: Vec<String> = rows.iter().map(|(d, ..)| d.clone()).collect();
        let ctx = self.profile_context(viewer, req, &dids, None, super::Tiers::List).await?;
        let subscriptions: Vec<Value> = rows
            .iter()
            .map(|(did, post, reply, _)| {
                // Set here too: the viewer state of a member that is not
                // ready is not hydrated, and the listing is its own rows.
                let mut view = ctx.view(did, ProfileKind::View);
                view["viewer"]["activitySubscription"] = json!({"post": post, "reply": reply});
                view
            })
            .collect();
        let mut out = json!({ "subscriptions": subscriptions });
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

    const ME: &str = "did:plc:aaaaaaaaaaaaaaaaaaaaaaaa";
    const BOB: &str = "did:plc:bbbbbbbbbbbbbbbbbbbbbbbb";
    const CAROL: &str = "did:plc:cccccccccccccccccccccccc";
    const DAVE: &str = "did:plc:dddddddddddddddddddddddd";
    const OUTSIDE: &str = "did:plc:oooooooooooooooooooooooo";

    /// B-43, B-75: the checks of `putActivitySubscription`, and the listing.
    #[tokio::test]
    async fn subscriptions_follow_the_subjects_declaration() {
        let Some(db) = fresh_db("hydration_activity_subscriptions").await else {
            return;
        };
        let p = &db.pool;
        for (d, h) in [(ME, "me.test"), (BOB, "bob.test"), (CAROL, "carol.test"), (DAVE, "dave.test")] {
            actor(p, d, h, h).await;
        }
        let mut sets = ScopeSets::default();
        for d in [ME, BOB, CAROL, DAVE] {
            sets.scope.insert(d.into());
        }
        let h = hydrator(p, sets);
        let me = Viewer {
            did: Some(ME.into()),
            hydrate: true,
        };
        let req = Requester::test();
        let follow = |from: &'static str, to: &'static str| async move {
            exec(p, &format!(
                "INSERT INTO follows (uri, author_did, subject_did, rev, created_at) VALUES \
                 ('at://{from}/app.bsky.graph.follow/{}', '{from}', '{to}', '3k', now())",
                &to[8..12]
            )).await;
        };
        let declare = |did: &'static str, allow: &'static str| async move {
            exec(p, &format!(
                "INSERT INTO actor_self_records (uri, did, collection, cid, rev, record) VALUES \
                 ('at://{did}/{NSID_DECLARATION}/self', '{did}', '{NSID_DECLARATION}', 'bafyd', '3k', '{{\"allowSubscriptions\": \"{allow}\"}}') \
                 ON CONFLICT (uri) DO UPDATE SET record = EXCLUDED.record"
            )).await;
        };
        let put = |subject: &'static str, post: bool, reply: bool| {
            let (h, me) = (&h, &me);
            async move { h.put_activity_subscription(me, subject, post, reply).await.expect("db") }
        };
        let refused = |r: Result<crate::hydration::PublicEmit<Value>, ViewError>| r.expect_err("refused").error;

        // Oneself, a handle, and an account outside the indexed set.
        assert_eq!(refused(put(ME, true, false).await), "InvalidRequest");
        assert_eq!(refused(put("bob.test", true, false).await), "InvalidRequest");
        assert!(put(OUTSIDE, true, false).await.expect_err("outside").message.contains("not indexed"));
        // No declaration means followers: refused until the member follows.
        assert_eq!(refused(put(BOB, true, false).await), "InvalidRequest");
        follow(ME, BOB).await;
        let ok = put(BOB, true, false).await.expect("subscribed");
        assert_eq!(ok.inner(), &json!({"subject": BOB, "activitySubscription": {"post": true, "reply": false}}));
        // `none` refuses; `mutuals` needs the follow back.
        follow(ME, CAROL).await;
        declare(CAROL, "none").await;
        assert_eq!(refused(put(CAROL, true, true).await), "InvalidRequest");
        declare(CAROL, "mutuals").await;
        assert_eq!(refused(put(CAROL, true, true).await), "InvalidRequest");
        follow(CAROL, ME).await;
        put(CAROL, true, true).await.expect("mutuals");
        // The cap (2 in tests) refuses a new subject, not a change.
        follow(ME, DAVE).await;
        assert!(put(DAVE, true, false).await.expect_err("cap").message.contains("limit"));
        put(BOB, true, true).await.expect("an update at the cap");

        // The listing: most recently changed first, with the settings.
        let l = h.list_activity_subscriptions(&me, &req, 50, None).await.expect("db").expect("view");
        let subs = l.inner()["subscriptions"].as_array().expect("subscriptions");
        assert_eq!(subs.len(), 2);
        assert_eq!((&subs[0]["did"], &subs[0]["viewer"]["activitySubscription"]), (&json!(BOB), &json!({"post": true, "reply": true})));
        let first = h.list_activity_subscriptions(&me, &req, 1, None).await.expect("db").expect("view");
        let cursor = first.inner()["cursor"].as_str().expect("cursor").to_string();
        let second = h.list_activity_subscriptions(&me, &req, 1, Some(&cursor)).await.expect("db").expect("view");
        assert_eq!(second.inner()["subscriptions"][0]["did"], CAROL);

        // Profile views carry both sides: the viewer's subscription, and on
        // a detailed view who the account lets subscribe.
        let profile = h.get_profile(&me, &req, CAROL).await.expect("db").expect("view");
        assert_eq!(profile.inner()["viewer"]["activitySubscription"], json!({"post": true, "reply": true}));
        assert_eq!(profile.inner()["associated"]["activitySubscription"]["allowSubscriptions"], "mutuals");
        let profile = h.get_profile(&me, &req, DAVE).await.expect("db").expect("view");
        assert!(profile.inner()["viewer"].get("activitySubscription").is_none());
        assert_eq!(profile.inner()["associated"]["activitySubscription"]["allowSubscriptions"], "followers");

        // Turning one off needs nothing: not readiness, not scope; and it
        // can be repeated.
        let unready = Viewer {
            did: Some(ME.into()),
            hydrate: false,
        };
        assert_eq!(h.put_activity_subscription(&unready, DAVE, true, false).await.expect("db").expect_err("not ready").error, "NotReady");
        for _ in 0..2 {
            let off = h.put_activity_subscription(&unready, BOB, false, false).await.expect("db").expect("off");
            assert_eq!(off.inner(), &json!({ "subject": BOB }));
        }
        h.put_activity_subscription(&unready, OUTSIDE, false, false).await.expect("db").expect("off, outside");
        let l = h.list_activity_subscriptions(&me, &req, 50, None).await.expect("db").expect("view");
        assert_eq!(l.inner()["subscriptions"].as_array().map(Vec::len), Some(1));
        db.drop_db().await;
    }

    /// Scope recency T-R16: enabling holds the scope read guard from its
    /// scope check through the upsert, so a purge marker (write guard) waits
    /// for the row; and a subject that left S meanwhile is refused.
    #[tokio::test]
    async fn enabling_writes_under_the_scope_read_guard() {
        use crate::test_support::hooks;

        let Some(db) = fresh_db("hydration_subscription_guard").await else {
            return;
        };
        let p = &db.pool;
        for (d, handle) in [(ME, "me.test"), (BOB, "bob.test")] {
            actor(p, d, handle, handle).await;
        }
        exec(p, &format!(
            "INSERT INTO follows (uri, author_did, subject_did, rev, created_at) VALUES \
             ('at://{ME}/app.bsky.graph.follow/1', '{ME}', '{BOB}', '3k', now())"
        ))
        .await;
        let mut sets = ScopeSets::default();
        sets.scope.insert(ME.into());
        sets.scope.insert(BOB.into());
        let h = hydrator(p, sets);
        let me = Viewer {
            did: Some(ME.into()),
            hydrate: true,
        };
        let armed = hooks::arm(hooks::SUBSCRIBE_HOLDS_GUARD, BOB);
        let (h2, me2) = (h.clone(), me.clone());
        let put = tokio::spawn(async move { h2.put_activity_subscription(&me2, BOB, true, false).await });
        armed.reached().await;
        // The writer side of the lock cannot be had while the guard is held.
        assert!(tokio::time::timeout(std::time::Duration::from_millis(200), h.scope().write_guard()).await.is_err());
        armed.release();
        put.await.expect("join").expect("db").expect("subscribed");
        drop(armed);
        let rows: i64 = sqlx::query_scalar("SELECT count(*) FROM activity_subscriptions").fetch_one(p).await.expect("rows");
        assert_eq!(rows, 1);

        // Once BOB has left S the request is refused before any guarded step.
        sqlx::query("DELETE FROM activity_subscriptions").execute(p).await.expect("clear");
        let armed = hooks::arm(hooks::SUBSCRIBE_HOLDS_GUARD, BOB);
        h.scope().write_guard().await.scope.remove(BOB);
        let refused = h.put_activity_subscription(&me, BOB, true, false).await.expect("db").expect_err("refused");
        assert!(refused.message.contains("not indexed"), "{refused:?}");
        assert_eq!(armed.hits(), 0);
        db.drop_db().await;
    }
}
