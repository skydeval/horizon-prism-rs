//! Crawl marks on the read path: in breadth Full, an account outside S and
//! the members reads its own records of a group locally only with a
//! current mark for it: both its generations current, no blocking gap
//! open, and not kept incomplete by hand. One batched read per call.

use std::collections::HashSet;

use sqlx::PgPool;

use crate::config::FullGroup;

const MARKED_SQL: &str = "SELECT d.did FROM dids d JOIN full_crawl_groups m ON m.account = d.id \
     WHERE d.did = ANY($1) AND m.grp = $2 AND m.crawl_generation = $3 \
       AND m.group_generation = ( \
         SELECT (w.value -> 'groups' -> $2 ->> 'generation')::bigint FROM worker_state w \
         WHERE w.key = 'full_groups' AND (w.value ->> 'crawl_generation')::bigint = $3 \
           AND (w.value -> 'groups' -> $2 ->> 'state') IS DISTINCT FROM 'removed') \
       AND NOT EXISTS (SELECT 1 FROM full_gaps g WHERE g.resolved_at IS NULL AND g.blocking) \
       AND NOT EXISTS (SELECT 1 FROM full_incomplete i WHERE i.grp = $2 AND i.account = d.id)";

/// Of `dids`, those with a current mark of `group` at `crawl_generation`.
///
/// # Errors
///
/// Any database error.
pub async fn marked(pool: &PgPool, dids: &[String], group: FullGroup, crawl_generation: u64) -> Result<HashSet<String>, sqlx::Error> {
    if dids.is_empty() {
        return Ok(HashSet::new());
    }
    let rows: Vec<String> = sqlx::query_scalar(MARKED_SQL)
        .bind(dids)
        .bind(group.as_str())
        .bind(i64::try_from(crawl_generation).unwrap_or(i64::MAX))
        .fetch_all(pool)
        .await?;
    Ok(rows.into_iter().collect())
}
