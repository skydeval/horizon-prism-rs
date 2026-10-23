//! What the admin UI's stats page shows of the private tier (admin-ui design
//! §10.2): the private tables' plain names, so the page can label the
//! storage sampler's per-table series without naming a private table itself
//! (the image build's private-table source check), and how many stored
//! private posts carry images or a video (private-media-filters design §2);
//! and one account's private posts, by media kind, for Look up (§6.10).

use sqlx::PgPool;

use super::media::{IMAGE, VIDEO};

/// Each private table and its name on the stats page.
const TABLE_LABELS: &[(&str, &str)] = &[
    ("private_posts", "Private posts"),
    ("private_audiences", "Who may see each private post"),
    ("private_tombstones", "Deleted private records remembered"),
    ("private_owner_ready", "Accounts whose private posts are ready"),
    ("private_owner_eligible", "Accounts checked for private posting"),
    ("private_bookmarks", "Private bookmarks"),
    ("private_threadgates", "Threadgates of private posts, and of public roots with private replies"),
    ("private_postgates", "Who may quote private posts"),
    ("private_quote_refs", "Quotes of private posts"),
    ("private_notifications", "Private notifications"),
    ("private_notification_seen", "Private notifications read"),
    ("private_notification_prefs", "Private notification settings"),
    ("private_activity_subscriptions", "Private activity subscriptions"),
    ("private_post_fanout", "Private subscription fan-out queue"),
    ("private_root_gate_fetches", "Public root threadgate fetches"),
    ("private_appearance", "Private appearance settings"),
];

/// `(table, label)` for every private table.
#[must_use]
pub(crate) fn admin_table_labels() -> &'static [(&'static str, &'static str)] {
    TABLE_LABELS
}

/// The private posts' table, as the sampler's `table` label names it.
#[must_use]
pub(crate) fn admin_posts_table() -> &'static str {
    TABLE_LABELS[0].0
}

/// Stored private posts with images (or a gallery) and with a video. Each
/// count reads only its partial index (migration 0035), never the whole
/// table, under a 2-second statement timeout.
///
/// # Errors
///
/// Database errors, the timeout among them.
pub(crate) async fn media_post_counts(pool: &PgPool) -> Result<(i64, i64), sqlx::Error> {
    let mut tx = pool.begin().await?;
    sqlx::query("SET LOCAL statement_timeout = '2s'").execute(&mut *tx).await?;
    let mut counts = [0_i64; 2];
    for (slot, bit) in counts.iter_mut().zip([IMAGE, VIDEO]) {
        // The bit is written into the text so the predicate is the partial
        // index's own, `(media_kinds & N) <> 0`.
        *slot = sqlx::query_scalar(&format!("SELECT count(*) FROM private_posts WHERE (media_kinds & {bit}) <> 0"))
            .fetch_one(&mut *tx)
            .await?;
    }
    tx.commit().await?;
    Ok((counts[0], counts[1]))
}

/// One account's private records as the admin UI's Look up shows them
/// (admin-ui design §6.10).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct AccountCounts {
    /// Stored private posts by the account.
    pub posts: i64,
    /// Of them, with images (or a gallery).
    pub with_images: i64,
    /// Of them, with a video.
    pub with_video: i64,
}

/// [`AccountCounts`] for `did`. The count reads the author's own index
/// rows, under a 2-second statement timeout.
///
/// # Errors
///
/// Database errors, the timeout among them.
pub(crate) async fn account_counts(pool: &PgPool, did: &str) -> Result<AccountCounts, sqlx::Error> {
    let mut tx = pool.begin().await?;
    sqlx::query("SET LOCAL statement_timeout = '2s'").execute(&mut *tx).await?;
    let (posts, with_images, with_video): (i64, i64, i64) = sqlx::query_as(&format!(
        "SELECT count(*), count(*) FILTER (WHERE (media_kinds & {IMAGE}) <> 0), count(*) FILTER (WHERE (media_kinds & {VIDEO}) <> 0) \
         FROM private_posts WHERE author_did = $1"
    ))
    .bind(did)
    .fetch_one(&mut *tx)
    .await?;
    tx.commit().await?;
    Ok(AccountCounts { posts, with_images, with_video })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::fresh_db;

    /// Every private table of the build's source check has a label, and the
    /// media counts read the bits.
    #[tokio::test]
    async fn labels_cover_the_private_tables_and_media_counts() {
        for table in [
            "posts", "audiences", "tombstones", "owner_ready", "owner_eligible", "bookmarks", "threadgates", "postgates", "quote_refs",
            "notifications", "notification_seen", "notification_prefs", "activity_subscriptions", "post_fanout", "appearance",
        ] {
            let name = format!("private_{table}");
            assert!(admin_table_labels().iter().any(|(t, l)| *t == name && !l.is_empty()), "{name}");
        }
        assert_eq!(admin_posts_table(), "private_posts");
        let Some(db) = fresh_db("private_media_counts").await else {
            return;
        };
        for (i, kinds) in [1_i16, 2, 3, 0].into_iter().enumerate() {
            sqlx::query(
                "INSERT INTO private_posts (uri, author_did, rkey, cid, rev, plaintext_text, created_at, media_kinds) \
                 VALUES ($1, 'did:plc:aaaaaaaaaaaaaaaaaaaaaaaa', $2, 'bafy', '3k', '', now(), $3)",
            )
            .bind(format!("at://did:plc:aaaaaaaaaaaaaaaaaaaaaaaa/app.nearhorizon.feed.postPrivate/3kmedia{i}"))
            .bind(format!("3kmedia{i}"))
            .bind(kinds)
            .execute(&db.pool)
            .await
            .expect("row");
        }
        assert_eq!(media_post_counts(&db.pool).await.expect("counts"), (2, 2));
        // One account's counts: its posts by media kind.
        let mine = account_counts(&db.pool, "did:plc:aaaaaaaaaaaaaaaaaaaaaaaa").await.expect("account");
        assert_eq!(mine, AccountCounts { posts: 4, with_images: 2, with_video: 2 });
        assert_eq!(account_counts(&db.pool, "did:plc:bbbbbbbbbbbbbbbbbbbbbbbb").await.expect("other"), AccountCounts::default());
        db.drop_db().await;
    }
}
