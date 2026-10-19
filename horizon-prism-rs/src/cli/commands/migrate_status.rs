//! `migrate status`: applied and pending migrations (read-only; never
//! creates `_sqlx_migrations`).

use sqlx::PgPool;

use crate::db::migrations::{MIGRATOR, applied_migrations, known_migrations, plan};

/// Render the migration status.
///
/// # Errors
///
/// Database errors.
pub async fn render(pool: &PgPool) -> Result<String, sqlx::Error> {
    let known = known_migrations(&MIGRATOR);
    let applied = applied_migrations(pool).await?;
    let gate = plan(&known, &applied);
    let mut out = Vec::new();
    for k in &known {
        let state = if gate.pending.iter().any(|p| p.version == k.version) {
            "pending"
        } else if gate.mismatched.contains(&k.version) {
            "applied (checksum mismatch)"
        } else if gate.dirty.contains(&k.version) {
            "failed (dirty)"
        } else {
            "applied"
        };
        out.push(format!("{}  {state}", k.name()));
    }
    for v in &gate.unknown {
        out.push(format!("{v:04}  applied (unknown to this binary)"));
    }
    out.push(if gate.is_inconsistent() {
        "status: inconsistent (serve refuses to start, exit 78)".to_string()
    } else if gate.pending.is_empty() {
        "status: up to date".to_string()
    } else {
        format!("status: {} pending", gate.pending.len())
    });
    Ok(out.join("\n"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::fresh_db;

    #[tokio::test]
    async fn reports_applied_migrations() {
        let Some(db) = fresh_db("cli_migrate_status").await else {
            return;
        };
        let out = render(&db.pool).await.expect("render");
        let names = [
            "0001_initial",
            "0002_private_reply_cids",
            "0003_eligibility_failures",
            "0004_lists_and_starter_packs",
            "0005_thread_mutes",
            "0006_bookmarks",
            "0007_activity_subscriptions",
            "0008_notification_prefs_v2",
            "0009_labeler_services",
            "0010_poll_posts",
            "0011_actor_view_completeness",
            "0012_mute_scope",
            "0013_via_refs",
            "0014_threadgates_and_postgates",
            "0015_drafts",
            "0016_search_and_trends",
            "0017_backfill_generation",
            "0018_subscribed_list_purpose",
            "0019_learned_inbound_blocks",
            "0020_operator_settings",
            "0021_member_post_refs",
            "0022_backfill_claim_indexes",
            // Private posting S2.
            "0023_private_post_display",
            // Private posting S3.
            "0024_bookmarks_private_tier",
            // Private posting S4.
            "0025_private_gates",
            // Private posting S5.
            "0026_quote_refs_private_tier",
            // Private posting S6.
            "0027_notify_private_tier",
            // Private posting S10 (the `following` audience).
            "0028_follows_verified",
            // Like and repost counts by account (#727).
            "0029_distinct_interaction_counts",
            // Replay below a settle: the listed revs (#723).
            "0030_listed_revs",
            // Membership not tied to hosting (#733).
            "0031_membership",
            // The web admin UI, S1 (#245, #735).
            "0032_admin_ui",
        ];
        let expected: Vec<String> = names.iter().map(|n| format!("{n}  applied")).chain(["status: up to date".to_string()]).collect();
        assert_eq!(out, expected.join("\n"));
        db.drop_db().await;
    }
}
