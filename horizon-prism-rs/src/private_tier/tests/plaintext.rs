//! S4, plaintext private posts refused (private-encode design §7, slice E4,
//! keyed to `private_any_pds.enabled` since `encodePost` was withdrawn):
//! T-S1 and T-S2.

use metrics_exporter_prometheus::PrometheusHandle;

use super::*;
use crate::private_tier::pipeline;

fn plaintext_misses(handle: &PrometheusHandle) -> f64 {
    crate::test_support::series(&handle.render(), pipeline::METRIC_OUTCOMES, &["outcome=\"plaintext\""]).unwrap_or(0.0)
}

/// T-S1: with the switch on (the default), a plaintext post misses
/// (`plaintext`) for its author and for its audience; a reply under it stays
/// readable; the row is kept. Off, it is served as before.
#[tokio::test]
async fn ts1_plaintext_is_refused_while_any_pds_is_on() {
    let Some(db) = fresh_db("ts1_plaintext").await else {
        return;
    };
    let recorder = metrics_exporter_prometheus::PrometheusBuilder::new().build_recorder();
    let handle = recorder.handle();
    let _local = metrics::set_default_local_recorder(&recorder);
    let f = fixture(&db.pool, "ts1-plaintext").await;
    let aud = f.audience(ALICE, "3kaudience22", Some("list"), &[BOB]).await;
    let plain = f.put_post(ALICE, "3kplaintext2", json!({"$type": NSID_POST_PRIVATE, "text": "plain", "audienceList": aud, "createdAt": "2026-09-26T12:00:00.000Z"})).await;
    let reply = f.private_post(BOB, "3kbobsreply2", &aud, Some((&plain, &plain))).await;
    let encoded = f.private_post(ALICE, "3kencoded222", &aud, None).await;
    f.ready_all().await;
    let on = serve(f.state_floored(Some(Duration::from_secs(1)), Duration::ZERO)).await;
    let before = plaintext_misses(&handle);
    for reader in [ALICE, BOB] {
        assert!(one_post(&on, reader, &plain).await.1.is_null(), "{reader} reads a plaintext post");
    }
    assert_eq!(plaintext_misses(&handle), before + 2.0);
    assert!(!one_post(&on, BOB, &reply).await.1.is_null(), "the refusal does not cascade");
    assert!(!one_post(&on, BOB, &encoded).await.1.is_null(), "an encoded post is served");
    let kept: i64 = sqlx::query_scalar("SELECT count(*) FROM private_posts WHERE uri = $1").bind(&plain).fetch_one(&f.pool).await.expect("count");
    assert_eq!(kept, 1, "the row is kept");
    // Off: served as before, to the author and the audience.
    let off = serve(f.state_floored(Some(Duration::from_secs(1)), Duration::ZERO).with_plaintext_refused(false)).await;
    for reader in [ALICE, BOB] {
        let (_, view) = one_post(&off, reader, &plain).await;
        assert_eq!(view["record"]["text"], "plain", "{reader}: {view}");
    }
    assert_eq!(plaintext_misses(&handle), before + 2.0);
    db.drop_db().await;
}

/// Horizon's PQ-16: `private.anyPds` is listed only by a build that also
/// refuses plaintext, checks the clear fields at read, and sets the
/// author's `droppedFields`. With `private_any_pds.enabled` set as `serve`
/// sets both from it, a listed feature comes with all three; removing any
/// one breaks this test. Off: the feature is not listed and plaintext is
/// served, while the clear-field check and `droppedFields`, which are
/// independent safety, still apply.
#[tokio::test]
async fn any_pds_is_listed_only_with_every_read_rule_it_promises() {
    let Some(db) = fresh_db("pq16_any_pds").await else {
        return;
    };
    let f = fixture(&db.pool, "pq16-any-pds").await;
    let aud = f.audience(ALICE, "3kaudience22", Some("list"), &[BOB, CAROL]).await;
    let plain = f.put_post(ALICE, "3kplaintext2", json!({"$type": NSID_POST_PRIVATE, "text": "plain", "audienceList": aud, "createdAt": "2026-09-26T12:00:00.000Z"})).await;
    let mut forged = f.new_form_record(ALICE, "3kforged2222", "no mention", None, None, &aud).await;
    forged["mentionDids"] = json!([CAROL]);
    let forged = f.put_post(ALICE, "3kforged2222", forged).await;
    let bytes: Vec<u8> = std::iter::once(0xFF).chain(br#"{"text":"t","embed":[1]}"#.iter().copied()).collect();
    let dropped = f.put_post(ALICE, "3kdropped222", f.encoded_bytes(ALICE, "3kdropped222", &bytes, Some(&aud)).await).await;
    f.ready_all().await;
    for enabled in [true, false] {
        // As `serve` builds them, from the one switch.
        let membership = crate::xrpc::handlers::membership::MembershipSettings::new(&crate::private_tier::KryphocronPrivateTier, &PrivateReadConfig::default())
            .with_any_pds(enabled);
        let listed = membership.features().contains(&crate::xrpc::handlers::membership::FEATURE_ANY_PDS);
        assert_eq!(listed, enabled);
        let base = serve(f.state_floored(Some(Duration::from_secs(1)), Duration::ZERO).with_plaintext_refused(enabled)).await;
        // S4 exactly while listed.
        assert_eq!(one_post(&base, BOB, &plain).await.1.is_null(), listed, "plaintext refused iff private.anyPds is listed");
        // The clear-field check and the marker, listed or not.
        assert!(one_post(&base, BOB, &forged).await.1.is_null(), "an over-claim misses for others (enabled {enabled})");
        assert!(!one_post(&base, ALICE, &forged).await.1.is_null(), "and is served to its author (enabled {enabled})");
        assert_eq!(one_post(&base, ALICE, &dropped).await.1["droppedFields"], json!(["embed"]), "enabled {enabled}");
    }
    db.drop_db().await;
}

/// T-S2: converting a plaintext post (§7.3): the client encodes its content
/// at the same rkey and rewrites the record; it is served again.
#[tokio::test]
async fn ts2_a_converted_post_is_served() {
    let Some(db) = fresh_db("ts2_convert").await else {
        return;
    };
    let f = fixture(&db.pool, "ts2-convert").await;
    let aud = f.audience(ALICE, "3kaudience22", Some("list"), &[BOB]).await;
    let facets = json!([mention(BOB)]);
    let plain = f
        .put_post(ALICE, "3kconvert222", json!({"$type": NSID_POST_PRIVATE, "text": "hello bob", "facets": facets, "audienceList": aud, "createdAt": "2026-09-26T12:00:00.000Z"}))
        .await;
    f.ready_all().await;
    let base = serve(f.state_floored(Some(Duration::from_secs(1)), Duration::ZERO)).await;
    assert!(one_post(&base, BOB, &plain).await.1.is_null());
    // The rewrite: the same content encoded at the same URI, the clear
    // `text` and `facets` removed, the derived clear fields added.
    let mut converted = f.new_form_record(ALICE, "3kconvert222", "hello bob", Some(&facets), None, &aud).await;
    converted["createdAt"] = json!("2026-09-26T12:00:00.000Z");
    f.apply_op(ALICE, Action::Update, NSID_POST_PRIVATE, "3kconvert222", "3kaaaaaaaaab2", Some(converted)).await;
    let (_, view) = one_post(&base, BOB, &plain).await;
    assert_eq!((&view["record"]["text"], &view["record"]["facets"]), (&json!("hello bob"), &facets), "{view}");
    db.drop_db().await;
}
