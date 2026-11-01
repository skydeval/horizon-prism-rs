//! `post_search` follows S: an author's stored posts get their search rows
//! when it joins S or becomes a member. The writer writes the rows of posts
//! it stores from then on; [`fill_author`] writes those of the posts stored
//! before, from their records, in keyset batches.
//!
//! Each batch runs under the scope read guard and the author's shared DID
//! lock, and reads the posts rows again under them, so a purge (which holds
//! the exclusive lock) is never undone and an author that left meanwhile
//! gets no rows.

use serde_json::{Value, json};

use super::extract::PostExtract;
use super::writer::Writer;
use crate::db::locks::lock_dids_shared;
use crate::db::records;

/// Posts per batch.
pub const BATCH: i64 = 500;

/// Write the missing search rows of `did`'s stored posts, while it is in S
/// or a member. Returns the rows written.
///
/// # Errors
///
/// Database errors, and a stored record that does not decode.
pub async fn fill_author(writer: &Writer, did: &str) -> Result<u64, sqlx::Error> {
    let pool = writer.pool();
    let Some(author) = sqlx::query_scalar::<_, i32>("SELECT id FROM dids WHERE did = $1").bind(did).fetch_optional(pool).await? else {
        return Ok(0);
    };
    let mut after = i64::MIN;
    let mut total = 0u64;
    loop {
        let guard = writer.scope().read_guard().await;
        if !(guard.in_scope(did) || guard.is_member(did)) {
            return Ok(total);
        }
        let mut tx = pool.begin().await?;
        lock_dids_shared(&mut tx, &[did.to_string()]).await?;
        let rows: Vec<(i64, chrono::DateTime<chrono::Utc>, Vec<u8>)> = sqlx::query_as(
            "SELECT p.rkey, p.created_at, p.record FROM posts p \
             WHERE p.author = $1 AND p.rkey > $2 \
               AND NOT EXISTS (SELECT 1 FROM post_search s WHERE s.author = p.author AND s.rkey = p.rkey) \
             ORDER BY p.rkey LIMIT $3",
        )
        .bind(author)
        .bind(after)
        .bind(BATCH)
        .fetch_all(&mut *tx)
        .await?;
        let Some(last) = rows.last().map(|r| r.0) else {
            tx.commit().await?;
            return Ok(total);
        };
        let mut values: Vec<Value> = Vec::with_capacity(rows.len());
        for (rkey, created_at, stored) in &rows {
            let record = records::decode_value(pool, stored).await?;
            let x = PostExtract::of(&record);
            values.push(json!({
                "rkey": rkey, "created_at": created_at, "text": record.get("text").and_then(Value::as_str).unwrap_or_default(),
                "tags": x.tags, "link_domains": x.link_domains, "link_urls": x.link_urls, "lang_prefixes": x.lang_prefixes,
            }));
        }
        let written = sqlx::query(
            "INSERT INTO post_search (author, rkey, created_at, text, search_tsv, tags, link_domains, link_urls, lang_prefixes, mention_ids) \
             SELECT $1, v.rkey, v.created_at, v.text, to_tsvector('simple', v.text), v.tags, v.link_domains, v.link_urls, \
                    v.lang_prefixes, p.mention_ids \
             FROM jsonb_to_recordset($2::jsonb) AS v(rkey bigint, created_at timestamptz, text text, tags text[], \
                    link_domains text[], link_urls text[], lang_prefixes text[]) \
             JOIN posts p ON p.author = $1 AND p.rkey = v.rkey \
             ON CONFLICT (author, rkey) DO NOTHING",
        )
        .bind(author)
        .bind(Value::Array(values))
        .execute(&mut *tx)
        .await?
        .rows_affected();
        tx.commit().await?;
        drop(guard);
        total += written;
        after = last;
    }
}
