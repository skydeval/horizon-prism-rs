//! Live changes and pending scopes (refract design §3.2, §4.6, §13: T-15,
//! T-12-r1, T-12-r6, T-8d, T-8g, T-8m, T-8n, T-8aa, T-8ae, T-8af).

use chrono::Utc;

use super::harness::{Harness, LABELER, OTHER, SUBJECT, tier1, tier2};
use crate::aggregations::ScopeSets;
use crate::test_support::fresh_db;

async fn scopes(h: &Harness, did: &str) -> Vec<String> {
    sqlx::query_scalar("SELECT scope FROM refract_pending_dids WHERE did = $1 ORDER BY scope").bind(did).fetch_all(&h.pool).await.expect("scopes")
}

/// T-12-r1, T-8d, T-8m: switching the feature off hides R at once (a
/// pending reason, out of R-live) and starts the feature scope's clock,
/// drops candidates; `keep` holds the end; `unkeep` restarts the clock;
/// the end comes only after the delay; switching it back on clears the
/// scope and leaves the DID hidden until its first label fetch.
#[tokio::test]
async fn t12_r1_feature_off_and_on() {
    let Some(db) = fresh_db("refract_feature_switch").await else {
        return;
    };
    let h = Harness::new(&db.pool, ScopeSets::default(), vec![tier1()]).await;
    h.refract.apply_config().await.expect("first run");
    h.row(SUBJECT, "sky", "restored").await;
    h.row(OTHER, "sky", "candidate").await;
    h.refract.apply_config().await.expect("load");
    assert!(h.sets().await.restored.contains_key(SUBJECT));
    h.live.update(|v| v.refract_enabled = false);
    let applied = h.refract.apply_config().await.expect("off");
    assert_eq!(applied.candidates_dropped, 1);
    assert_eq!(scopes(&h, SUBJECT).await, ["feature"]);
    assert_eq!(h.state(OTHER).await, None, "candidates go at once");
    let sets = h.sets().await;
    assert!(!sets.restored.contains_key(SUBJECT), "out of R-live");
    assert!(sets.refract_hidden.pending(SUBJECT), "hidden at once");
    // Not before the delay.
    assert_eq!(h.refract.end_pending().await.expect("delay"), 0);
    sqlx::query("UPDATE refract_pending SET first_seen = now() - interval '25 hours'").execute(&h.pool).await.expect("age");
    assert!(crate::refract::Refract::keep(&h.pool, "feature", true).await.expect("keep"));
    assert_eq!(h.refract.end_pending().await.expect("kept"), 0, "kept: nothing ends");
    assert!(crate::refract::Refract::keep(&h.pool, "feature", false).await.expect("unkeep"));
    assert_eq!(h.refract.end_pending().await.expect("unkept"), 0, "unkeep restarts the clock");
    // Back on: the scope clears, the DID waits for its labels, due now.
    h.live.update(|v| v.refract_enabled = true);
    let applied = h.refract.apply_config().await.expect("on");
    assert_eq!(applied.cleared, 1);
    assert!(scopes(&h, SUBJECT).await.is_empty());
    assert_eq!(h.count("SELECT count(*) FROM refract_pending").await, 0);
    let sets = h.sets().await;
    assert!(sets.restored.contains_key(SUBJECT));
    assert!(sets.refract_hidden.awaiting(SUBJECT), "hidden until its targeted fetch");
    // The check's fetch un-hides it and enqueues its backfill (T-8w).
    h.all_pass();
    h.check(SUBJECT).await;
    let sets = h.sets().await;
    assert!(!sets.refract_hidden.awaiting(SUBJECT));
    assert_eq!(h.count("SELECT count(*) FROM backfill_jobs WHERE depth = 'refract'").await, 1);
    // Off again, past the delay: ended `feature_disabled`.
    h.live.update(|v| v.refract_enabled = false);
    h.refract.apply_config().await.expect("off");
    sqlx::query("UPDATE refract_pending SET first_seen = now() - interval '25 hours'").execute(&h.pool).await.expect("age");
    assert_eq!(h.refract.end_pending().await.expect("delay"), 1);
    let (state, reason): (String, Option<String>) =
        sqlx::query_as("SELECT state, ended_reason FROM refract_dids WHERE did = $1").bind(SUBJECT).fetch_one(&h.pool).await.expect("row");
    assert_eq!((state.as_str(), reason.as_deref()), ("ending", Some("feature_disabled")));
    db.drop_db().await;
}

/// T-8g, T-8n, T-8af: a `pds_patterns` typo unlists R's DIDs (pending, not
/// due, and no check ends them); a rename keeps state; fixing the typo
/// clears them, nothing having ended; creating the entry scope drops
/// candidates whose host no longer matches.
#[tokio::test]
async fn t8g_a_typo_unlists_and_a_fix_clears() {
    let Some(db) = fresh_db("refract_typo").await else {
        return;
    };
    let h = Harness::new(&db.pool, ScopeSets::default(), vec![tier1()]).await;
    h.refract.apply_config().await.expect("first run");
    h.row(SUBJECT, "sky", "restored").await;
    h.row(OTHER, "sky", "candidate").await;
    sqlx::query("UPDATE refract_dids SET next_check_at = now() + interval '1 day'").execute(&h.pool).await.expect("later");
    let hash_before = crate::refract::communities::entry_hash(&tier1());
    // The typo.
    h.live.update(|v| v.refract_communities[0].pds_patterns = vec!["127.0.0.2".into()]);
    let applied = h.refract.apply_config().await.expect("typo");
    assert_eq!(applied.unlisted, 1);
    assert_eq!(scopes(&h, SUBJECT).await, [format!("entry:{hash_before}")], "keyed by the pre-change hash");
    assert_eq!(h.state(OTHER).await, None, "the candidate goes");
    assert_eq!(h.count("SELECT count(*) FROM refract_dids WHERE next_check_at <= now() AND did = 'did:plc:subjectaaaaaaaaaaaaaaaaa'").await, 0, "not made due");
    assert!(h.sets().await.refract_hidden.pending(SUBJECT));
    // A check cannot end it: it is never claimed.
    assert_eq!(h.check(SUBJECT).await, 0);
    // A rename alone keeps the state.
    h.live.update(|v| v.refract_communities[0].name = "sky2".into());
    let applied = h.refract.apply_config().await.expect("rename");
    assert_eq!(applied.renamed, [("sky".to_string(), "sky2".to_string())]);
    let community: String = sqlx::query_scalar("SELECT community FROM refract_dids WHERE did = $1").bind(SUBJECT).fetch_one(&h.pool).await.expect("c");
    assert_eq!(community, "sky2");
    // The fix (and a gone_labels edit in the same save): cleared, nothing
    // ended, waiting for labels.
    h.live.update(|v| {
        v.refract_communities[0].pds_patterns = vec!["127.0.0.1".into()];
        v.refract_communities[0].gone_labels = vec!["!takedown".into()];
    });
    let applied = h.refract.apply_config().await.expect("fix");
    assert_eq!(applied.cleared, 1);
    assert!(scopes(&h, SUBJECT).await.is_empty());
    assert_eq!(h.state(SUBJECT).await.as_deref(), Some("restored"));
    assert!(h.sets().await.refract_hidden.awaiting(SUBJECT));
    db.drop_db().await;
}

/// T-12-r6, T-15: an entry change makes its DIDs due and bumps their
/// version (a check claimed before writes nothing); a `gone_labels` edit
/// updates the stored flags at once, omission rows keep `gone`; a new
/// label reader drops the old rows; two saves apply as one diff.
#[tokio::test]
async fn t12_r6_entry_changes() {
    let Some(db) = fresh_db("refract_entry_changes").await else {
        return;
    };
    let h = Harness::new(&db.pool, ScopeSets::default(), vec![tier2()]).await;
    h.refract.apply_config().await.expect("first run");
    h.row(SUBJECT, "north", "restored").await;
    sqlx::query("UPDATE refract_dids SET next_check_at = now() + interval '1 day'").execute(&h.pool).await.expect("later");
    for (subject, val) in [(SUBJECT.to_string(), "spam"), (SUBJECT.to_string(), "!hide"), (format!("at://{SUBJECT}/app.bsky.feed.post/1"), "!refract-absent")] {
        sqlx::query("INSERT INTO refract_labels (labeler_did, subject, val, did, gone, verified) VALUES ($1, $2, $3, $4, $5, true)")
            .bind(LABELER)
            .bind(&subject)
            .bind(val)
            .bind(SUBJECT)
            .bind(val != "spam")
            .execute(&h.pool)
            .await
            .expect("label");
    }
    let version: i64 = sqlx::query_scalar("SELECT version FROM refract_dids WHERE did = $1").bind(SUBJECT).fetch_one(&h.pool).await.expect("v");
    // Two saves in quick succession: gone_labels gains spam, loses !hide.
    h.live.update(|v| v.refract_communities[0].gone_labels.push("spam".into()));
    h.live.update(|v| v.refract_communities[0].gone_labels.retain(|l| l != "!hide"));
    let applied = h.refract.apply_config().await.expect("edit");
    assert_eq!(applied.changed, ["north"]);
    let flags: Vec<(String, bool)> = sqlx::query_as("SELECT val, gone FROM refract_labels ORDER BY val COLLATE \"C\"").fetch_all(&h.pool).await.expect("flags");
    assert_eq!(flags, vec![("!hide".to_string(), false), ("!refract-absent".to_string(), true), ("spam".to_string(), true)]);
    let (v2, due): (i64, bool) = sqlx::query_as("SELECT version, next_check_at <= now() FROM refract_dids WHERE did = $1").bind(SUBJECT).fetch_one(&h.pool).await.expect("v");
    assert!(v2 > version && due, "due now, version bumped");
    assert!(h.sets().await.refract_hidden.accounts[SUBJECT].active(Utc::now()), "spam now hides the account");
    // A run with nothing new changes nothing.
    assert_eq!(h.refract.apply_config().await.expect("again"), crate::refract::config_task::Applied::default());
    // A new labeler: the old rows go, synthetic included.
    h.live.update(|v| v.refract_communities[0].labeler_did = Some(super::harness::OTHER_LABELER.into()));
    h.refract.apply_config().await.expect("labeler");
    assert_eq!(h.count("SELECT count(*) FROM refract_labels").await, 0);
    db.drop_db().await;
}

/// T-8aa: clearing an entry scope while the feature scope still lists the
/// DID leaves it hidden and enqueues nothing.
#[tokio::test]
async fn t8aa_two_scopes() {
    let Some(db) = fresh_db("refract_two_scopes").await else {
        return;
    };
    let h = Harness::new(&db.pool, ScopeSets::default(), vec![tier1()]).await;
    h.refract.apply_config().await.expect("first run");
    h.row(SUBJECT, "sky", "restored").await;
    h.live.update(|v| v.refract_communities[0].pds_patterns = vec!["127.0.0.2".into()]);
    h.refract.apply_config().await.expect("typo");
    h.live.update(|v| v.refract_enabled = false);
    h.refract.apply_config().await.expect("off");
    assert_eq!(scopes(&h, SUBJECT).await.len(), 2);
    h.live.update(|v| v.refract_communities[0].pds_patterns = vec!["127.0.0.1".into()]);
    let applied = h.refract.apply_config().await.expect("fix");
    assert_eq!(applied.cleared, 0);
    assert_eq!(scopes(&h, SUBJECT).await, ["feature"]);
    assert!(h.sets().await.refract_hidden.pending(SUBJECT));
    assert_eq!(h.count("SELECT count(*) FROM backfill_jobs").await, 0);
    db.drop_db().await;
}
