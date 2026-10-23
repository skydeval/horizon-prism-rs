//! Private notification preferences (private-notification-prefs design):
//! each reason muted neither lists nor counts (T-N1), `include: follows`
//! read at read time (T-N2), the defaults unchanged (T-N3), the methods
//! (T-N4, T-N7), full pages (T-N5), the counting paths agree (T-N6), the
//! purges (T-N8), and mutes and blocks beside them (T-N10).

use std::collections::BTreeSet;

use super::*;

const GET_PREFS: &str = "app.nearhorizon.private.getNotificationPreferences";
const PUT_PREFS: &str = "app.nearhorizon.private.putNotificationPreferences";

fn pref(include: &str, list: bool) -> Value {
    json!({"include": include, "list": list, "push": true})
}

fn defaults() -> Value {
    let all = pref("all", true);
    json!({"like": all, "mention": all, "quote": all, "reply": all, "repost": all, "subscribedPost": {"list": true, "push": true}})
}

/// The served private router with roomy limits and no floor.
async fn served(f: &Fixture) -> String {
    let mut app = f.app.clone();
    app.limits = roomy_limits();
    serve(f.state_of(app, Some(Duration::from_secs(1)), Duration::ZERO)).await
}

async fn put_prefs(base: &str, who: &str, body: &Value) -> (u16, Value) {
    let (status, body) = post(base, PUT_PREFS, body, who).await;
    (status, serde_json::from_str(&body).expect("json"))
}

async fn get_prefs(base: &str, who: &str) -> Value {
    json_of(base, GET_PREFS, "", who).await.1
}

/// Every `(reason, uri, author)` of `who`'s private notifications, paging
/// to the end.
async fn listed(base: &str, who: &str) -> BTreeSet<(String, String, String)> {
    let mut out = BTreeSet::new();
    let mut cursor: Option<String> = None;
    for _ in 0..100 {
        let query = cursor.as_ref().map_or_else(|| "limit=100".to_string(), |c| format!("limit=100&cursor={}", urlencoding(c)));
        let (_, page, _) = json_of(base, LIST_NOTIFICATIONS, &query, who).await;
        for n in page["notifications"].as_array().expect("notifications") {
            out.insert((
                n["reason"].as_str().expect("reason").to_string(),
                n["uri"].as_str().expect("uri").to_string(),
                n["author"]["did"].as_str().expect("author").to_string(),
            ));
        }
        match page["cursor"].as_str() {
            Some(c) => cursor = Some(c.to_string()),
            None => return out,
        }
    }
    panic!("no end to the pages");
}

async fn counted(base: &str, who: &str) -> i64 {
    json_of(base, GET_UNREAD_COUNT, "", who).await.1["count"].as_i64().expect("count")
}

/// The count by every path: the method (its bounded statement), the
/// sliced path at K 1, 3 and 7, and the single-statement reference. All
/// must agree; the method's answer is returned.
async fn counted_every_way(f: &Fixture, base: &str, who: &str) -> i64 {
    let n = counted(base, who).await;
    for k in [1usize, 3, 7] {
        let sliced = schema::unread_count_sliced(&f.pool, who, CAP, k, None).await.expect("sliced");
        assert_eq!(sliced, n, "sliced at K {k}");
    }
    let single: i64 = sqlx::query_scalar(&schema::unread_statement(CAP)).bind(who).fetch_one(&f.pool).await.expect("single");
    assert_eq!(single, n, "the single statement");
    n
}

/// Alice's post T under her audience of herself, Bob, Carol and Dave, and
/// her private notifications of every reason and both kinds, each readable
/// by her: Bob's private reply, Dave's private quote (under his own
/// audience of Alice), Bob's private mention of her, Carol's and Dave's
/// likes, Carol's repost, Bob's public quote and public reply.
async fn every_reason(f: &Fixture) -> String {
    let aud = f.audience(ALICE, "3kaudience22", Some("list"), &[ALICE, BOB, CAROL, DAVE]).await;
    let aud_bob = f.audience(BOB, "3kaudbob2222", Some("list"), &[ALICE]).await;
    let aud_dave = f.audience(DAVE, "3kauddave222", Some("list"), &[ALICE]).await;
    let t = f.private_post(ALICE, "3ktpost22222", &aud, None).await;
    f.private_post(BOB, "3kreplybob22", &aud, Some((&t, &t))).await;
    f.post_with(DAVE, "3kquote22222", &aud_dave, quote_of(&t)).await;
    f.post_with(BOB, "3kmention222", &aud_bob, json!({"facets": [mention(ALICE)]})).await;
    let at = "2026-10-01T10:00:00Z";
    f.ingest(CAROL, LIKE, "3klikecarol2", subject_record(LIKE, &t, at)).await;
    f.ingest(DAVE, LIKE, "3klikedave22", subject_record(LIKE, &t, at)).await;
    f.ingest(CAROL, REPOST, "3krepcarol22", subject_record(REPOST, &t, at)).await;
    f.ingest(BOB, PUBLIC_POST, "3kpubquote22", public_quote(&t, at)).await;
    f.ingest(BOB, PUBLIC_POST, "3kpubreply22", public_reply(&t, at, None)).await;
    f.ready_all().await;
    t
}

fn without(all: &BTreeSet<(String, String, String)>, keep: impl Fn(&(String, String, String)) -> bool) -> BTreeSet<(String, String, String)> {
    all.iter().filter(|r| keep(r)).cloned().collect()
}

/// T-N1 (Horizon's done-when): each reason muted neither lists nor counts,
/// the others unchanged, by every counting path; listed again, all is back.
/// T-N6: the paths agree throughout, and the key scan reads no preference.
#[tokio::test]
async fn t_n1_each_reason_muted_neither_lists_nor_counts() {
    let test = "t_n1_prefs_mute";
    let Some(db) = fresh_db(test).await else {
        return;
    };
    let f = fixture_in(&db.pool, test, PublicMode::On).await;
    every_reason(&f).await;
    let base = served(&f).await;
    let all = listed(&base, ALICE).await;
    let reasons: BTreeSet<&str> = all.iter().map(|r| r.0.as_str()).collect();
    assert_eq!(reasons, BTreeSet::from(["like", "mention", "quote", "reply", "repost"]), "{all:?}");
    assert_eq!(all.len(), 8, "{all:?}");
    let total = counted_every_way(&f, &base, ALICE).await;
    assert_eq!(total, 8);
    for reason in ["like", "mention", "quote", "reply", "repost"] {
        let (status, body) = put_prefs(&base, ALICE, &json!({reason: pref("all", false)})).await;
        assert_eq!(status, 200, "{body}");
        let kept = without(&all, |r| r.0 != reason);
        assert_eq!(listed(&base, ALICE).await, kept, "{reason} muted");
        assert_eq!(counted_every_way(&f, &base, ALICE).await, i64::try_from(kept.len()).expect("n"), "{reason} muted");
        assert_eq!(put_prefs(&base, ALICE, &json!({reason: pref("all", true)})).await.0, 200);
        assert_eq!(listed(&base, ALICE).await, all, "{reason} listed again");
        assert_eq!(counted_every_way(&f, &base, ALICE).await, total, "{reason} listed again");
    }
    // `list: false` decides over `include: follows`.
    put_prefs(&base, ALICE, &json!({"like": {"include": "follows", "list": false, "push": true}})).await;
    exec(&f.pool, &follow_sql(ALICE, CAROL)).await;
    assert!(listed(&base, ALICE).await.iter().all(|r| r.0 != "like"));
    // The key scan carries none of `notification_kept`, preferences included.
    let keys = schema::unread_keys_statement();
    assert!(!keys.contains("private_notification_prefs") && !keys.contains("thread_mutes"), "{keys}");
    db.drop_db().await;
}

/// T-N2: `include: follows` keeps a reason only from accounts the reader
/// follows as stored, read at read time; other reasons are untouched.
#[tokio::test]
async fn t_n2_follows_filters_at_read_time() {
    let test = "t_n2_prefs_follows";
    let Some(db) = fresh_db(test).await else {
        return;
    };
    let f = fixture_in(&db.pool, test, PublicMode::On).await;
    every_reason(&f).await;
    let base = served(&f).await;
    let all = listed(&base, ALICE).await;
    let likes_by = |set: &BTreeSet<(String, String, String)>| -> BTreeSet<String> {
        set.iter().filter(|r| r.0 == "like").map(|r| r.2.clone()).collect()
    };
    assert_eq!(likes_by(&all), BTreeSet::from([CAROL.to_string(), DAVE.to_string()]));
    put_prefs(&base, ALICE, &json!({"like": pref("follows", true)})).await;
    // Alice follows no one: no like.
    let none = listed(&base, ALICE).await;
    assert!(likes_by(&none).is_empty(), "{none:?}");
    assert_eq!(none, without(&all, |r| r.0 != "like"), "the other reasons, Carol's repost among them");
    assert_eq!(counted_every_way(&f, &base, ALICE).await, 6);
    // She follows Carol: Carol's like.
    exec(&f.pool, &follow_sql(ALICE, CAROL)).await;
    assert_eq!(likes_by(&listed(&base, ALICE).await), BTreeSet::from([CAROL.to_string()]));
    assert_eq!(counted_every_way(&f, &base, ALICE).await, 7);
    // She unfollows Carol and follows Dave: read again, with no write to
    // the notifications.
    let rows: i64 = sqlx::query_scalar("SELECT count(*) FROM private_notifications").fetch_one(&f.pool).await.expect("rows");
    exec(&f.pool, &format!("DELETE FROM follows WHERE author_did = '{ALICE}'")).await;
    exec(&f.pool, &follow_sql(ALICE, DAVE)).await;
    assert_eq!(likes_by(&listed(&base, ALICE).await), BTreeSet::from([DAVE.to_string()]));
    assert_eq!(counted_every_way(&f, &base, ALICE).await, 7);
    let after: i64 = sqlx::query_scalar("SELECT count(*) FROM private_notifications").fetch_one(&f.pool).await.expect("rows");
    assert_eq!(after, rows);
    // Back to everyone.
    put_prefs(&base, ALICE, &json!({"like": pref("all", true)})).await;
    assert_eq!(listed(&base, ALICE).await, all);
    db.drop_db().await;
}

/// T-N3: no row, a row of the defaults, and a row of only unknown keys (and
/// a reason that is not an object) give byte-identical pages and one count.
#[tokio::test]
async fn t_n3_defaults_are_todays_behaviour() {
    let test = "t_n3_prefs_defaults";
    let Some(db) = fresh_db(test).await else {
        return;
    };
    let f = fixture_in(&db.pool, test, PublicMode::On).await;
    every_reason(&f).await;
    let base = served(&f).await;
    let page = |base: String| async move { json_of(&base, LIST_NOTIFICATIONS, "limit=3", ALICE).await.0 };
    let (none, none_count) = (page(base.clone()).await, counted_every_way(&f, &base, ALICE).await);
    assert_eq!(get_prefs(&base, ALICE).await, json!({"preferences": defaults()}));
    let (status, body) = put_prefs(&base, ALICE, &defaults()).await;
    assert_eq!((status, &body), (200, &json!({"preferences": defaults()})));
    assert_eq!((page(base.clone()).await, counted_every_way(&f, &base, ALICE).await), (none.clone(), none_count));
    exec(&f.pool, &format!(
        "UPDATE private_notification_prefs SET prefs = '{{\"follow\": {{\"include\": \"follows\", \"list\": false, \"push\": true}}, \"like\": 5}}' WHERE did = '{ALICE}'"
    ))
    .await;
    assert_eq!((page(base.clone()).await, counted_every_way(&f, &base, ALICE).await), (none, none_count));
    assert_eq!(get_prefs(&base, ALICE).await, json!({"preferences": defaults()}), "a non-object reads as the default");
    db.drop_db().await;
}

/// T-N4 and T-N7: get and put, merge, validation, unknown keys, bad
/// bodies, own-only, concurrent puts, and the gate.
#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn t_n4_get_and_put() {
    let test = "t_n4_prefs_methods";
    let Some(db) = fresh_db(test).await else {
        return;
    };
    let f = fixture_in(&db.pool, test, PublicMode::Off).await;
    f.ready_all().await;
    let base = served(&f).await;
    let stored = |did: &'static str| {
        let pool = f.pool.clone();
        async move {
            sqlx::query_scalar::<_, Value>("SELECT prefs FROM private_notification_prefs WHERE did = $1")
                .bind(did)
                .fetch_optional(&pool)
                .await
                .expect("row")
        }
    };
    assert_eq!(get_prefs(&base, BOB).await, json!({"preferences": defaults()}));

    // Unknown keys and an empty object: 200, the defaults, no row.
    for body in [json!({}), json!({"follow": pref("follows", false), "chat": {"include": "accepted", "push": false}, "other": 1}), json!({"uri": "x", "cid": "y", "seenAt": "2026-10-01T00:00:00Z"})] {
        assert_eq!(put_prefs(&base, BOB, &body).await, (200, json!({"preferences": defaults()})), "{body}");
    }
    assert_eq!(stored(BOB).await, None);

    // A partial put, then another: merged.
    let like = json!({"include": "follows", "list": true, "push": false});
    let mut want = defaults();
    want["like"] = like.clone();
    assert_eq!(put_prefs(&base, BOB, &json!({"like": like})).await, (200, json!({"preferences": want})));
    want["reply"] = pref("all", false);
    assert_eq!(put_prefs(&base, BOB, &json!({"reply": pref("all", false)})).await, (200, json!({"preferences": want.clone()})));
    assert_eq!(get_prefs(&base, BOB).await, json!({"preferences": want.clone()}));
    // Anything else inside a preference is dropped.
    put_prefs(&base, BOB, &json!({"quote": {"include": "all", "list": true, "push": true, "extra": "x".repeat(1000)}})).await;
    assert_eq!(stored(BOB).await.expect("row")["quote"], pref("all", true));

    // Invalid preferences: 400, nothing written.
    let before = stored(BOB).await;
    for (body, message) in [
        (json!({"like": {"include": "all", "list": true}}), "Error: like.push must be a boolean"),
        (json!({"mention": {"include": "mutuals", "list": true, "push": true}}), "Error: mention.include must be one of all, follows"),
        (json!({"repost": {"include": "all", "list": "no", "push": true}}), "Error: repost.list must be a boolean"),
        (json!({"reply": pref("all", true), "quote": 3}), "Error: quote.list must be a boolean"),
    ] {
        let (status, v) = put_prefs(&base, BOB, &body).await;
        assert_eq!((status, v["error"].as_str(), v["message"].as_str()), (400, Some("InvalidRequest"), Some(message)), "{body}");
    }
    assert_eq!(stored(BOB).await, before, "nothing written");
    let (status, v) = put_prefs(&base, CAROL, &json!({"like": {"list": true}})).await;
    assert_eq!(status, 400, "{v}");
    assert_eq!(stored(CAROL).await, None, "no row created by a refused put");

    // Not an object: 400.
    for raw in ["[]", "\"x\"", "{not json", ""] {
        let token = mint(&signing_key(BOB_KEY), BOB, &format!("did:web:{AUDIENCE_HOST}"), Some(PUT_PREFS));
        let r = reqwest::Client::new()
            .post(format!("{base}/xrpc/{PUT_PREFS}"))
            .header("authorization", format!("Bearer {token}"))
            .header("content-type", "application/json")
            .body(raw.to_string())
            .send()
            .await
            .expect("request");
        let status = r.status().as_u16();
        let v: Value = r.json().await.expect("json");
        assert_eq!((status, v["message"].as_str()), (400, Some("Error: input must be an object")), "{raw:?}");
    }

    // Own-only: Bob's writes are not Alice's.
    assert_eq!(get_prefs(&base, ALICE).await, json!({"preferences": defaults()}));

    // Two puts of different keys at once both land.
    let (like_off, repost_follows) = (json!({"like": pref("all", false)}), json!({"repost": pref("follows", true)}));
    let (a, b) = tokio::join!(put_prefs(&base, DAVE, &like_off), put_prefs(&base, DAVE, &repost_follows));
    assert_eq!((a.0, b.0), (200, 200));
    let got = get_prefs(&base, DAVE).await;
    assert_eq!((&got["preferences"]["like"], &got["preferences"]["repost"]), (&pref("all", false), &pref("follows", true)));

    // T-N7: the gate. No auth: 401; not ready: NotReady; not a member:
    // NotEnrolled, before the body is read.
    for (method, url_verb) in [(GET_PREFS, reqwest::Method::GET), (PUT_PREFS, reqwest::Method::POST)] {
        let r = reqwest::Client::new().request(url_verb, format!("{base}/xrpc/{method}")).send().await.expect("request");
        let status = r.status().as_u16();
        let v: Value = r.json().await.expect("json");
        assert_eq!((status, v["error"].as_str()), (401, Some("AuthRequired")), "{method}");
    }
    exec(&f.pool, &format!("UPDATE members SET ready_at = NULL WHERE did = '{CAROL}'")).await;
    let (status, body, _) = get(&base, GET_PREFS, "", CAROL, CAROL_KEY).await;
    assert!(status == 400 && body.contains("NotReady"), "{body}");
    let (status, body) = post(&base, PUT_PREFS, &json!({"like": 1}), CAROL).await;
    assert!(status == 400 && body.contains("NotReady"), "{body}");
    exec(&f.pool, &format!("DELETE FROM members WHERE did = '{CAROL}'")).await;
    let (status, body) = post(&base, PUT_PREFS, &json!({"like": 1}), CAROL).await;
    assert!(status == 400 && body.contains("NotEnrolled"), "{body}");
    let (status, body, _) = get(&base, GET_PREFS, "", CAROL, CAROL_KEY).await;
    assert!(status == 400 && body.contains("NotEnrolled"), "{body}");
    db.drop_db().await;
}

/// T-N5: with a muted reason the scan skips its rows, so a page is filled
/// from what the preferences allow.
#[tokio::test]
async fn t_n5_pages_fill() {
    let test = "t_n5_prefs_pages";
    let Some(db) = fresh_db(test).await else {
        return;
    };
    let f = fixture_in(&db.pool, test, PublicMode::On).await;
    let aud = f.audience(ALICE, "3kaudience22", Some("list"), &[ALICE, BOB, CAROL]).await;
    let t = f.private_post(ALICE, "3ktpost22222", &aud, None).await;
    let mut replies = BTreeSet::new();
    for i in 0..5 {
        replies.insert(f.private_post(BOB, &format!("3kreply{i}22222"), &aud, Some((&t, &t))).await);
    }
    // 60 newer like rows, none of them listable: their likes are not stored.
    exec(&f.pool, &format!(
        "INSERT INTO private_notifications (recipient_did, author_did, reason, reason_subject, record_uri, record_cid, indexed_at) \
         SELECT '{ALICE}', '{CAROL}', 'like', '{t}', 'at://{CAROL}/{LIKE}/3kgone' || g, 'bafy', now() + interval '1 minute' \
         FROM generate_series(1, 60) g"
    ))
    .await;
    f.ready_all().await;
    let base = served(&f).await;
    let (_, page, _) = json_of(&base, LIST_NOTIFICATIONS, "limit=5", ALICE).await;
    assert_eq!(page["notifications"], json!([]), "the scan meets the likes first");
    assert!(page["cursor"].is_string());
    put_prefs(&base, ALICE, &json!({"like": pref("all", false)})).await;
    let (_, page, _) = json_of(&base, LIST_NOTIFICATIONS, "limit=5", ALICE).await;
    let got: BTreeSet<String> = page["notifications"].as_array().expect("items").iter().map(|n| n["uri"].as_str().expect("uri").to_string()).collect();
    assert_eq!(got, replies, "{page}");
    let cursor = page["cursor"].as_str().expect("a full scan carries a cursor");
    let (_, page, _) = json_of(&base, LIST_NOTIFICATIONS, &format!("limit=5&cursor={}", urlencoding(cursor)), ALICE).await;
    assert_eq!(page.get("notifications"), Some(&json!([])), "{page}");
    assert!(page.get("cursor").is_none(), "{page}");
    db.drop_db().await;
}

/// T-N8: the `member_removed` and `account_deleted` purges delete a
/// member's preferences, and only its own.
#[tokio::test]
async fn t_n8_purges_take_the_preferences() {
    let test = "t_n8_prefs_purges";
    let Some(db) = fresh_db(test).await else {
        return;
    };
    let f = fixture_in(&db.pool, test, PublicMode::Off).await;
    f.ready_all().await;
    let base = served(&f).await;
    for who in [ALICE, BOB, CAROL] {
        assert_eq!(put_prefs(&base, who, &json!({"like": pref("all", false)})).await.0, 200);
    }
    let rows = || {
        let pool = f.pool.clone();
        async move {
            sqlx::query_scalar::<_, String>("SELECT did FROM private_notification_prefs ORDER BY did").fetch_all(&pool).await.expect("rows")
        }
    };
    let mut conn = f.pool.acquire().await.expect("conn");
    KryphocronPrivateTier.purge_member_removed_batch(&mut conn, BOB, 100).await.expect("purge");
    assert_eq!(rows().await, [ALICE, CAROL]);
    KryphocronPrivateTier.purge_did_batch(&mut conn, CAROL, 100).await.expect("purge");
    drop(conn);
    assert_eq!(rows().await, [ALICE]);
    db.drop_db().await;
}

/// T-N10: preferences never re-admit what blocks and mutes drop.
#[tokio::test]
async fn t_n10_mutes_and_blocks_beside_preferences() {
    let test = "t_n10_prefs_mutes";
    let Some(db) = fresh_db(test).await else {
        return;
    };
    let f = fixture_in(&db.pool, test, PublicMode::On).await;
    every_reason(&f).await;
    let base = served(&f).await;
    let all = listed(&base, ALICE).await;
    // A reposts-scoped mute of Carol with reposts listed: hidden.
    exec(&f.pool, &format!("INSERT INTO mutes (viewer_did, subject_did, scope) VALUES ('{ALICE}', '{CAROL}', 'reposts')")).await;
    put_prefs(&base, ALICE, &json!({"repost": pref("all", true)})).await;
    assert_eq!(listed(&base, ALICE).await, without(&all, |r| r.0 != "repost"));
    exec(&f.pool, "DELETE FROM mutes").await;
    // Likes from follows only, Alice follows Dave and blocks him: hidden.
    put_prefs(&base, ALICE, &json!({"like": pref("follows", true)})).await;
    exec(&f.pool, &follow_sql(ALICE, DAVE)).await;
    exec(&f.pool, &block_sql(ALICE, DAVE)).await;
    let got = listed(&base, ALICE).await;
    assert!(got.iter().all(|r| r.2 != DAVE), "{got:?}");
    assert_eq!(counted_every_way(&f, &base, ALICE).await, i64::try_from(got.len()).expect("n"));
    db.drop_db().await;
}
