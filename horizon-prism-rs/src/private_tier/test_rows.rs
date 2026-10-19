//! Test-only private rows for tests outside this module (the scope sweep's,
//! scope-recency T-R7 and T-R11), so no private table is named outside
//! `private_tier` (the Dockerfile's source check).

use sqlx::PgPool;

/// Store a bare private post by `author` at `rkey`, replying to
/// `reply_parent` (also its root) when given.
pub(crate) async fn insert_post(pool: &PgPool, author: &str, rkey: &str, reply_parent: Option<&str>) {
    sqlx::query(
        "INSERT INTO private_posts (uri, author_did, rkey, cid, rev, reply_root_uri, reply_parent_uri, plaintext_text, created_at) \
         VALUES ($1, $2, $3, 'bafy', '3kaaaaaaaaaa2', $4, $4, 'x', now())",
    )
    .bind(format!("at://{author}/tools.kryphocron.feed.postPrivate/{rkey}"))
    .bind(author)
    .bind(rkey)
    .bind(reply_parent)
    .execute(pool)
    .await
    .expect("private post");
}

/// The rkeys of every stored private post, sorted.
pub(crate) async fn post_rkeys(pool: &PgPool) -> Vec<String> {
    sqlx::query_scalar("SELECT rkey FROM private_posts ORDER BY rkey")
        .fetch_all(pool)
        .await
        .expect("private posts")
}
