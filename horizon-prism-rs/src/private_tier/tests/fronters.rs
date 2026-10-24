//! T-F1 (plural-pronouns design §1.1): the clear fronters key of a
//! kryphocron post is stored and served in the view's `record`, and in a
//! quoting post's `#viewRecord.value`; an invalid one is dropped, counted,
//! and the post still ingests.

use super::*;
use crate::plural::FRONTERS_KEY;
use crate::private_tier::ingest::{DisplayFields, valid_fronters};

#[test]
fn t_f1_fronters_are_validated_strictly() {
    let ok = json!({"members": [{"id": "3kwren"}, {"id": "3ksparrow", "status": "co-conscious"}], "note": "kept"});
    assert!(valid_fronters(&ok));
    let twenty = json!({"members": (0..20).map(|i| json!({"id": format!("3k{i}")})).collect::<Vec<_>>()});
    assert!(valid_fronters(&twenty));
    for bad in [
        json!({"members": (0..21).map(|i| json!({"id": format!("3k{i}")})).collect::<Vec<_>>()}),
        json!({"members": []}),
        json!({"members": [{"id": "a/b"}]}),
        json!({"members": [{"id": 3}]}),
        json!({"members": [{"name": "Wren"}]}),
        json!({"members": [{"id": "3k", "status": 1}]}),
        json!({"members": [{"id": "3k", "status": "x".repeat(321)}]}),
        json!({"members": ["3k"]}),
        json!([{"id": "3k"}]),
        json!("3k"),
    ] {
        assert!(!valid_fronters(&bad), "{bad}");
        let parsed = DisplayFields::parse(&json!({FRONTERS_KEY: bad}));
        assert_eq!((parsed.fronters, parsed.dropped), (None, vec![FRONTERS_KEY]));
    }
    let parsed = DisplayFields::parse(&json!({FRONTERS_KEY: ok.clone()}));
    assert_eq!((parsed.fronters, parsed.dropped.len()), (Some(ok), 0));
    assert_eq!(DisplayFields::parse(&json!({})).fronters, None);
}

#[tokio::test]
async fn t_f1_fronters_in_the_private_view() {
    let Some(db) = fresh_db("t_f1_private_fronters").await else {
        return;
    };
    let f = fixture(&db.pool, "t-f1-fronters").await;
    let aud = f.audience(ALICE, "3kaudience22", Some("list"), &[BOB]).await;
    let fronters = json!({"members": [{"id": "3kwren"}, {"id": "3ksparrow"}]});
    let with = f.post_with(ALICE, "3kfronters22", &aud, json!({FRONTERS_KEY: fronters})).await;
    let bad = f.post_with(ALICE, "3kbadfront22", &aud, json!({FRONTERS_KEY: {"members": []}})).await;
    let quoting = f
        .post_with(ALICE, "3kquoting222", &aud, json!({"embed": {"$type": format!("{KLEX}#embedRecord"), "record": {"uri": with, "cid": "bafy3kfronters22"}}}))
        .await;
    let stored: Option<Value> = sqlx::query_scalar("SELECT fronters FROM private_posts WHERE uri = $1").bind(&with).fetch_one(&f.pool).await.expect("row");
    assert_eq!(stored.as_ref(), Some(&fronters));
    f.ready_all().await;
    let base = serve(f.state(Some(Duration::from_secs(1)))).await;
    let (_, p) = one_post(&base, BOB, &with).await;
    assert_eq!(p["record"][FRONTERS_KEY], fronters, "{p}");
    let (_, p) = one_post(&base, BOB, &bad).await;
    assert!(!p.is_null(), "an invalid key never makes the post malformed");
    assert!(p["record"].get(FRONTERS_KEY).is_none(), "{p}");
    let (_, q) = one_post(&base, BOB, &quoting).await;
    assert_eq!(q["embed"]["record"]["value"][FRONTERS_KEY], fronters, "{q}");
    // A reader outside the audience is served nothing.
    let (body, p) = one_post(&base, CAROL, &with).await;
    assert!(p.is_null() || p["record"].get(FRONTERS_KEY).is_none(), "{body}");
    assert!(!body.contains("3kwren"), "{body}");
    db.drop_db().await;
}
