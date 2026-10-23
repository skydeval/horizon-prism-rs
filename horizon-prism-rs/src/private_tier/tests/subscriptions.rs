//! Private activity subscriptions, the methods (private-activity-subs design
//! §4): put and list (T-AS1), the put's refusals and cap (T-AS2), the purges
//! (T-AS12) and the gate (T-AS14).

use super::*;

const PUT: &str = "app.nearhorizon.private.putActivitySubscription";
const LIST: &str = "app.nearhorizon.private.listActivitySubscriptions";

/// The served private router with roomy limits, no floor, and the cap `max`.
async fn served_capped(f: &Fixture, max: u32) -> String {
    let mut app = f.app.clone();
    app.limits = roomy_limits();
    serve(f.state_of(app, Some(Duration::from_secs(1)), Duration::ZERO).with_activity_subscriptions_max(max)).await
}

fn flags(post: bool, reply: bool) -> Value {
    json!({"post": post, "reply": reply})
}

/// `putActivitySubscription` by `who`: status and body as JSON.
async fn put_sub(base: &str, who: &str, subject: &str, post: bool, reply: bool) -> (u16, Value) {
    let (status, body) = super::post(base, PUT, &json!({"subject": subject, "activitySubscription": flags(post, reply)}), who).await;
    (status, serde_json::from_str(&body).expect("json"))
}

/// Every `(subject, post, reply)` of `who`'s list, paging by `limit`, and
/// the number of pages read.
async fn listed_subs(base: &str, who: &str, limit: u32) -> (Vec<(String, bool, bool)>, usize) {
    let mut out = Vec::new();
    let mut cursor: Option<String> = None;
    for page in 1..=100 {
        let query = cursor.as_ref().map_or_else(|| format!("limit={limit}"), |c| format!("limit={limit}&cursor={}", urlencoding(c)));
        let (_, v, _) = json_of(base, LIST, &query, who).await;
        for item in v["subscriptions"].as_array().expect("subscriptions") {
            let f = &item["activitySubscription"];
            out.push((
                item["subject"].as_str().expect("subject").to_string(),
                f["post"].as_bool().expect("post"),
                f["reply"].as_bool().expect("reply"),
            ));
        }
        match v["cursor"].as_str() {
            Some(c) => cursor = Some(c.to_string()),
            None => return (out, page),
        }
    }
    panic!("no end to the pages");
}

async fn stored_rows(pool: &PgPool) -> Vec<(String, String)> {
    sqlx::query_as("SELECT subscriber_did, subject_did FROM private_activity_subscriptions ORDER BY 1, 2")
        .fetch_all(pool)
        .await
        .expect("rows")
}

/// T-AS1: on and off round trip, off repeatable, the list newest-changed
/// first with a working cursor, `{post, reply}` as stored, each member's
/// own rows only, and the same answer for a subject Prism knows nothing of.
#[tokio::test]
async fn t_as1_put_and_list() {
    let test = "t_as1_subs_put_list";
    let Some(db) = fresh_db(test).await else {
        return;
    };
    let f = fixture_in(&db.pool, test, PublicMode::Off).await;
    f.ready_all().await;
    let base = served_capped(&f, 1000).await;

    let (status, v) = put_sub(&base, BOB, ALICE, true, false).await;
    assert_eq!((status, &v), (200, &json!({"subject": ALICE, "activitySubscription": flags(true, false)})));
    assert_eq!(put_sub(&base, BOB, CAROL, true, true).await.0, 200);
    // A DID that names nothing Prism knows: the same answer as a member's.
    let (status, unknown) = put_sub(&base, BOB, NOBODY, true, false).await;
    assert_eq!(status, 200);
    assert_eq!(serde_json::to_string(&unknown).expect("json").replace(NOBODY, ALICE), serde_json::to_string(&v).expect("json"));

    // Newest-changed first, one page at a time and all at once.
    let want = vec![(NOBODY.to_string(), true, false), (CAROL.to_string(), true, true), (ALICE.to_string(), true, false)];
    let (one_by_one, pages) = listed_subs(&base, BOB, 1).await;
    assert_eq!((one_by_one, pages), (want.clone(), 4), "three full pages, then an empty one with no cursor");
    assert_eq!(listed_subs(&base, BOB, 50).await, (want, 1));

    // A change moves the row to the front and keeps the other kind's time.
    let since: Option<DateTime<Utc>> =
        sqlx::query_scalar(&format!("SELECT post_since FROM private_activity_subscriptions WHERE subscriber_did = '{BOB}' AND subject_did = '{ALICE}'"))
            .fetch_one(&f.pool)
            .await
            .expect("since");
    assert_eq!(put_sub(&base, BOB, ALICE, true, true).await.1["activitySubscription"], flags(true, true));
    let after: Option<DateTime<Utc>> =
        sqlx::query_scalar(&format!("SELECT post_since FROM private_activity_subscriptions WHERE subscriber_did = '{BOB}' AND subject_did = '{ALICE}'"))
            .fetch_one(&f.pool)
            .await
            .expect("since");
    assert_eq!(since, after, "post_since kept while Posts stays on");
    assert_eq!(listed_subs(&base, BOB, 50).await.0[0], (ALICE.to_string(), true, true));

    // Off: the row goes; repeatable.
    for _ in 0..2 {
        assert_eq!(put_sub(&base, BOB, ALICE, false, false).await, (200, json!({"subject": ALICE})));
    }
    assert_eq!(
        listed_subs(&base, BOB, 50).await.0,
        vec![(NOBODY.to_string(), true, false), (CAROL.to_string(), true, true)]
    );

    // Own rows only.
    assert_eq!(listed_subs(&base, ALICE, 50).await.0, vec![]);
    assert_eq!(put_sub(&base, ALICE, BOB, false, true).await.0, 200);
    assert_eq!(listed_subs(&base, ALICE, 50).await.0, vec![(BOB.to_string(), false, true)]);
    assert_eq!(listed_subs(&base, BOB, 50).await.0.len(), 2);
    db.drop_db().await;
}

/// T-AS2: malformed input is 400 `InvalidRequest` with nothing written; the
/// cap refuses a new subject with `LimitReached` and lets a change through;
/// two concurrent puts at one under the cap land exactly one.
#[tokio::test]
async fn t_as2_put_refusals_and_the_cap() {
    let test = "t_as2_subs_refusals";
    let Some(db) = fresh_db(test).await else {
        return;
    };
    let f = fixture_in(&db.pool, test, PublicMode::Off).await;
    f.ready_all().await;
    let base = served_capped(&f, 2).await;
    for (body, message) in [
        (json!({"subject": "bob.test", "activitySubscription": flags(true, false)}), "Error: subject must be a valid did"),
        (json!({"subject": BOB, "activitySubscription": flags(true, false)}), "Error: Cannot subscribe to own activity"),
        (json!({"activitySubscription": flags(true, false)}), "Error: Input must have the property \"subject\""),
        (json!({"subject": ALICE}), "Error: Input must have the property \"activitySubscription\""),
        (json!({"subject": ALICE, "activitySubscription": true}), "Error: Input must have the property \"activitySubscription\""),
        (json!({"subject": ALICE, "activitySubscription": {"post": true}}), "Error: activitySubscription.reply must be a boolean"),
        (json!({"subject": ALICE, "activitySubscription": {"post": "yes", "reply": false}}), "Error: activitySubscription.post must be a boolean"),
        (json!([]), "Error: input must be an object"),
    ] {
        let (status, raw) = super::post(&base, PUT, &body, BOB).await;
        let v: Value = serde_json::from_str(&raw).expect("json");
        assert_eq!((status, v["error"].as_str(), v["message"].as_str()), (400, Some("InvalidRequest"), Some(message)), "{body}");
    }
    assert!(stored_rows(&f.pool).await.is_empty(), "nothing written");

    // The cap (2): a third subject is refused, a change of one held passes.
    assert_eq!(put_sub(&base, BOB, ALICE, true, false).await.0, 200);
    assert_eq!(put_sub(&base, BOB, CAROL, true, false).await.0, 200);
    let (status, v) = put_sub(&base, BOB, DAVE, true, false).await;
    assert_eq!(
        (status, v["error"].as_str(), v["message"].as_str()),
        (400, Some("LimitReached"), Some("Error: private activity subscription limit reached (2)"))
    );
    assert_eq!(put_sub(&base, BOB, ALICE, false, true).await.0, 200);
    assert_eq!(stored_rows(&f.pool).await.len(), 2);
    // Off frees a place.
    assert_eq!(put_sub(&base, BOB, CAROL, false, false).await.0, 200);
    assert_eq!(put_sub(&base, BOB, DAVE, true, true).await.0, 200);

    // Two concurrent puts of new subjects at one under the cap.
    assert_eq!(put_sub(&base, CAROL, ALICE, true, false).await.0, 200);
    let (a, b) = tokio::join!(put_sub(&base, CAROL, BOB, true, false), put_sub(&base, CAROL, DAVE, true, false));
    let mut statuses = [a.0, b.0];
    statuses.sort_unstable();
    assert_eq!(statuses, [200, 400], "{a:?} {b:?}");
    let carol: i64 = sqlx::query_scalar(&format!("SELECT count(*) FROM private_activity_subscriptions WHERE subscriber_did = '{CAROL}'"))
        .fetch_one(&f.pool)
        .await
        .expect("count");
    assert_eq!(carol, 2);
    db.drop_db().await;
}

/// T-AS12: `member_removed` and `account_deleted` take the rows where the
/// DID is the subscriber or the subject, and another member's stay;
/// `account_deleted` takes the author's queue rows.
#[tokio::test]
async fn t_as12_purges_take_subscriptions() {
    let test = "t_as12_subs_purges";
    let Some(db) = fresh_db(test).await else {
        return;
    };
    let f = fixture_in(&db.pool, test, PublicMode::Off).await;
    for (who, subject) in [(BOB, ALICE), (CAROL, BOB), (CAROL, DAVE), (DAVE, ALICE), (ALICE, CAROL)] {
        exec(&f.pool, &format!(
            "INSERT INTO private_activity_subscriptions (subscriber_did, subject_did, post_since) VALUES ('{who}', '{subject}', now())"
        ))
        .await;
    }
    for (author, rkey) in [(DAVE, "3kq1"), (ALICE, "3kq2")] {
        exec(&f.pool, &format!(
            "INSERT INTO private_post_fanout (uri, author_did, not_before) VALUES ('at://{author}/{NSID_POST_PRIVATE}/{rkey}', '{author}', now())"
        ))
        .await;
    }
    let mut conn = f.pool.acquire().await.expect("conn");
    KryphocronPrivateTier.purge_member_removed_batch(&mut conn, BOB, 100).await.expect("purge");
    assert_eq!(
        stored_rows(&f.pool).await,
        [(ALICE.to_string(), CAROL.to_string()), (CAROL.to_string(), DAVE.to_string()), (DAVE.to_string(), ALICE.to_string())]
    );
    // One row per batch: the caller repeats until a batch deletes nothing.
    while KryphocronPrivateTier.purge_did_batch(&mut conn, DAVE, 1).await.expect("purge") > 0 {}
    drop(conn);
    assert_eq!(stored_rows(&f.pool).await, [(ALICE.to_string(), CAROL.to_string())]);
    let queued: Vec<String> = sqlx::query_scalar("SELECT author_did FROM private_post_fanout").fetch_all(&f.pool).await.expect("queue");
    assert_eq!(queued, [ALICE]);
    db.drop_db().await;
}

/// T-AS14: both methods answer the gate's errors: 401, `NotReady`,
/// `503 Reindexing` and `NotEnrolled` (T-3 covers 401 and a served member
/// in every public mode).
#[tokio::test]
async fn t_as14_the_gate() {
    let test = "t_as14_subs_gate";
    let Some(db) = fresh_db(test).await else {
        return;
    };
    let f = fixture_in(&db.pool, test, PublicMode::Off).await;
    f.ready_all().await;
    let base = served_capped(&f, 1000).await;
    let body = json!({"subject": ALICE, "activitySubscription": flags(true, false)});
    for (method, verb) in [(PUT, reqwest::Method::POST), (LIST, reqwest::Method::GET)] {
        let r = reqwest::Client::new().request(verb, format!("{base}/xrpc/{method}")).send().await.expect("request");
        let status = r.status().as_u16();
        let v: Value = r.json().await.expect("json");
        assert_eq!((status, v["error"].as_str()), (401, Some("AuthRequired")), "{method}");
    }
    let both = |who: &'static str| {
        let (base, body) = (base.clone(), body.clone());
        async move {
            let put = super::post(&base, PUT, &body, who).await;
            let (status, list, _) = get(&base, LIST, "", who, key_of(who)).await;
            (put, (status, list))
        }
    };
    // Never ready: NotReady.
    exec(&f.pool, &format!("UPDATE members SET ready_at = NULL, first_ready_at = NULL WHERE did = '{CAROL}'")).await;
    let ((ps, pb), (ls, lb)) = both(CAROL).await;
    assert!(ps == 400 && pb.contains("\"NotReady\"") && ls == 400 && lb.contains("\"NotReady\""), "{pb} {lb}");
    // Ready once, not now: Reindexing.
    exec(&f.pool, &format!("UPDATE members SET ready_at = NULL, first_ready_at = now() WHERE did = '{CAROL}'")).await;
    let ((ps, pb), (ls, lb)) = both(CAROL).await;
    assert!(ps == 503 && pb.contains("\"Reindexing\"") && ls == 503 && lb.contains("\"Reindexing\""), "{pb} {lb}");
    // Not a member: NotEnrolled.
    exec(&f.pool, &format!("DELETE FROM members WHERE did = '{CAROL}'")).await;
    let ((ps, pb), (ls, lb)) = both(CAROL).await;
    assert!(ps == 400 && pb.contains("\"NotEnrolled\"") && ls == 400 && lb.contains("\"NotEnrolled\""), "{pb} {lb}");
    assert!(stored_rows(&f.pool).await.is_empty());
    db.drop_db().await;
}
