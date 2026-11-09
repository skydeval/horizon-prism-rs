//! Each record group's history in `worker_state['full_groups']`: its
//! generation (bumped when the group is added or removed), its state
//! (`crawling`, `paused`, `complete`, or `removed` for a group taken out of
//! `[full] kinds`, which keeps its generation), whether it was marked
//! complete by hand with accounts left over, whether a group add's
//! owed-set update has finished, and whether the counts repair has run
//! since the group turned complete.

use std::collections::BTreeMap;

use serde_json::{Value, json};
use sqlx::PgConnection;

use crate::indexing::full::GROUPS_KEY;

/// One group's history.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Group {
    pub generation: i64,
    pub state: String,
    pub incomplete: bool,
    pub union_done: bool,
    /// The counts repair ran after the group turned complete, so its counts
    /// may be read locally.
    pub counts_ready: bool,
}

impl Group {
    #[must_use]
    pub fn enabled(&self) -> bool {
        self.state != REMOVED
    }

    #[must_use]
    pub fn complete(&self) -> bool {
        self.state == COMPLETE
    }
}

pub const CRAWLING: &str = "crawling";
pub const PAUSED: &str = "paused";
pub const COMPLETE: &str = "complete";
pub const REMOVED: &str = "removed";

/// Every group's history, at the crawl generation it belongs to.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Groups {
    pub crawl_generation: i64,
    pub groups: BTreeMap<String, Group>,
}

impl Groups {
    #[must_use]
    pub fn parse(v: &Value) -> Self {
        let crawl_generation = v.get("crawl_generation").and_then(Value::as_i64).unwrap_or(0);
        let groups = v
            .get("groups")
            .and_then(Value::as_object)
            .map(|m| {
                m.iter()
                    .map(|(k, g)| {
                        (
                            k.clone(),
                            Group {
                                generation: g.get("generation").and_then(Value::as_i64).unwrap_or(0),
                                state: g.get("state").and_then(Value::as_str).unwrap_or(CRAWLING).to_string(),
                                incomplete: g.get("incomplete").and_then(Value::as_bool).unwrap_or(false),
                                union_done: g.get("union_done").and_then(Value::as_bool).unwrap_or(true),
                                counts_ready: g.get("counts_ready").and_then(Value::as_bool).unwrap_or(false),
                            },
                        )
                    })
                    .collect()
            })
            .unwrap_or_default();
        Self { crawl_generation, groups }
    }

    #[must_use]
    pub fn to_json(&self) -> Value {
        let groups: serde_json::Map<String, Value> = self
            .groups
            .iter()
            .map(|(k, g)| (k.clone(), json!({ "generation": g.generation, "state": g.state, "incomplete": g.incomplete, "union_done": g.union_done, "counts_ready": g.counts_ready })))
            .collect();
        json!({ "crawl_generation": self.crawl_generation, "groups": groups })
    }

    /// The enabled groups' names.
    #[must_use]
    pub fn enabled(&self) -> Vec<String> {
        self.groups.iter().filter(|(_, g)| g.enabled()).map(|(k, _)| k.clone()).collect()
    }

    /// Each enabled group's generation.
    #[must_use]
    pub fn generations(&self) -> BTreeMap<String, i64> {
        self.groups.iter().filter(|(_, g)| g.enabled()).map(|(k, g)| (k.clone(), g.generation)).collect()
    }
}

async fn read_with(conn: &mut PgConnection, lock: &str) -> Result<Option<Groups>, sqlx::Error> {
    let v: Option<Value> = sqlx::query_scalar(&format!("SELECT value FROM worker_state WHERE key = $1 {lock}"))
        .bind(GROUPS_KEY)
        .fetch_optional(conn)
        .await?;
    Ok(v.as_ref().map(Groups::parse))
}

/// The stored histories.
///
/// # Errors
///
/// Any database error.
pub async fn read(conn: &mut PgConnection) -> Result<Option<Groups>, sqlx::Error> {
    read_with(conn, "").await
}

/// The stored histories, share-locked: a crawl's `done` reads them so, and
/// a kinds change writes them under an update lock.
///
/// # Errors
///
/// Any database error.
pub async fn read_for_share(conn: &mut PgConnection) -> Result<Option<Groups>, sqlx::Error> {
    read_with(conn, "FOR SHARE").await
}

/// The stored histories, locked for update.
///
/// # Errors
///
/// Any database error.
pub async fn read_for_update(conn: &mut PgConnection) -> Result<Option<Groups>, sqlx::Error> {
    read_with(conn, "FOR UPDATE").await
}

/// Write the histories, only while they still belong to `crawl_generation`
/// (a phase change resets them). Returns whether it wrote.
///
/// # Errors
///
/// Any database error.
pub async fn write(conn: &mut PgConnection, g: &Groups) -> Result<bool, sqlx::Error> {
    let n = sqlx::query(
        "UPDATE worker_state SET value = $2, updated_at = now() WHERE key = $1 AND (value->>'crawl_generation')::bigint = $3",
    )
    .bind(GROUPS_KEY)
    .bind(g.to_json())
    .bind(g.crawl_generation)
    .execute(conn)
    .await?
    .rows_affected();
    Ok(n == 1)
}
