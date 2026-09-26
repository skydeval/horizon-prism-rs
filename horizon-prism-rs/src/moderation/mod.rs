//! The moderation gate (design-v2 §7.10): the single check every read path
//! runs before consulting any source. It reads the local `labels` table and
//! `actors.account_status`.
//!
//! | Input | Gate |
//! |---|---|
//! | `!takedown` on the record URI or its author DID | `Hidden(Takedown)` |
//! | author status `deactivated` / `takendown` / `suspended` | `Hidden(…)` |
//! | author `deleted` | `Hidden(Deleted)` |
//! | `!hide` on the record | `Collapsed` |
//! | otherwise | `Visible` |
//!
//! Account status is known only for DIDs with `actors` rows; for other DIDs
//! the gate is `Visible` unless a label applies.

use std::collections::{HashMap, HashSet};

use sqlx::PgPool;

use crate::db::locks::uri_author;

/// Why a subject is hidden.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HiddenReason {
    Takedown,
    Deactivated,
    Suspended,
    Deleted,
}

/// The gate's verdict for one subject.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Gate {
    Visible,
    /// `!hide`: omitted from lists and threads; a direct `getPosts` returns
    /// it with the label.
    Collapsed,
    Hidden(HiddenReason),
}

impl Gate {
    #[must_use]
    pub fn is_hidden(self) -> bool {
        matches!(self, Self::Hidden(_))
    }

    /// Visible in lists and threads.
    #[must_use]
    pub fn listable(self) -> bool {
        self == Self::Visible
    }

    /// The profile error for a hidden actor (§5.6, §7.10).
    #[must_use]
    pub fn profile_error(self) -> Option<(&'static str, &'static str)> {
        match self {
            Self::Hidden(HiddenReason::Deactivated) => Some(("AccountDeactivated", "Account is deactivated")),
            Self::Hidden(HiddenReason::Takedown | HiddenReason::Suspended) => {
                Some(("AccountTakedown", "Account has been suspended"))
            }
            Self::Hidden(HiddenReason::Deleted) => Some(("InvalidRequest", "Profile not found")),
            Self::Visible | Self::Collapsed => None,
        }
    }
}

fn status_reason(status: &str) -> Option<HiddenReason> {
    match status {
        "deactivated" => Some(HiddenReason::Deactivated),
        "takendown" => Some(HiddenReason::Takedown),
        "suspended" => Some(HiddenReason::Suspended),
        "deleted" => Some(HiddenReason::Deleted),
        _ => None,
    }
}

/// The gate for many subjects (at-URIs or DIDs) in two queries.
///
/// # Errors
///
/// Database errors.
pub async fn gate_many(pool: &PgPool, subjects: &[String]) -> Result<HashMap<String, Gate>, sqlx::Error> {
    let mut authors: HashSet<String> = HashSet::new();
    let mut keys: HashSet<String> = HashSet::new();
    for s in subjects {
        keys.insert(s.clone());
        let did = uri_author(s).unwrap_or(s);
        authors.insert(did.to_string());
        keys.insert(did.to_string());
    }
    let keys: Vec<String> = keys.into_iter().collect();
    let authors_v: Vec<String> = authors.into_iter().collect();
    let labels: Vec<(String, String)> = sqlx::query_as(
        "SELECT subject, val FROM labels WHERE subject = ANY($1) AND val IN ('!takedown', '!hide')",
    )
    .bind(&keys)
    .fetch_all(pool)
    .await?;
    let statuses: Vec<(String, String)> =
        sqlx::query_as("SELECT did, account_status FROM actors WHERE did = ANY($1) AND account_status <> 'active'")
            .bind(&authors_v)
            .fetch_all(pool)
            .await?;
    let takedown: HashSet<&str> = labels.iter().filter(|(_, v)| v == "!takedown").map(|(s, _)| s.as_str()).collect();
    let hide: HashSet<&str> = labels.iter().filter(|(_, v)| v == "!hide").map(|(s, _)| s.as_str()).collect();
    let status: HashMap<&str, HiddenReason> = statuses
        .iter()
        .filter_map(|(d, s)| status_reason(s).map(|r| (d.as_str(), r)))
        .collect();
    Ok(subjects
        .iter()
        .map(|s| {
            let author = uri_author(s).unwrap_or(s);
            let gate = if takedown.contains(s.as_str()) || takedown.contains(author) {
                Gate::Hidden(HiddenReason::Takedown)
            } else if let Some(r) = status.get(author) {
                Gate::Hidden(*r)
            } else if s.starts_with("at://") && hide.contains(s.as_str()) {
                Gate::Collapsed
            } else {
                Gate::Visible
            };
            (s.clone(), gate)
        })
        .collect())
}

/// The gate for one subject.
///
/// # Errors
///
/// Database errors.
pub async fn gate(pool: &PgPool, subject: &str) -> Result<Gate, sqlx::Error> {
    let s = subject.to_string();
    Ok(gate_many(pool, std::slice::from_ref(&s))
        .await?
        .remove(&s)
        .unwrap_or(Gate::Visible))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::fresh_db;

    const A: &str = "did:plc:aaaaaaaaaaaaaaaaaaaaaaaa";
    const B: &str = "did:plc:bbbbbbbbbbbbbbbbbbbbbbbb";
    const C: &str = "did:plc:cccccccccccccccccccccccc";

    #[tokio::test]
    async fn labels_and_account_status() {
        let Some(db) = fresh_db("moderation_gate").await else {
            return;
        };
        let p = &db.pool;
        let post_a = format!("at://{A}/app.bsky.feed.post/1");
        let post_b = format!("at://{B}/app.bsky.feed.post/1");
        let post_c = format!("at://{C}/app.bsky.feed.post/1");
        for (s, v) in [(A, "!takedown"), (post_b.as_str(), "!hide"), (post_c.as_str(), "porn")] {
            sqlx::query("INSERT INTO labels (subject, val) VALUES ($1, $2)")
                .bind(s)
                .bind(v)
                .execute(p)
                .await
                .expect("label");
        }
        sqlx::query("INSERT INTO actors (did, account_status) VALUES ($1, 'deactivated')")
            .bind(C)
            .execute(p)
            .await
            .expect("actor");
        let g = gate_many(p, &[post_a.clone(), post_b.clone(), post_c.clone(), B.to_string(), C.to_string()])
            .await
            .expect("gate");
        assert_eq!(g[&post_a], Gate::Hidden(HiddenReason::Takedown), "author takedown hides the post");
        assert_eq!(g[&post_b], Gate::Collapsed);
        assert_eq!(g[&post_c], Gate::Hidden(HiddenReason::Deactivated));
        assert_eq!(g[B], Gate::Visible);
        assert_eq!(g[C].profile_error().map(|e| e.0), Some("AccountDeactivated"));
        assert_eq!(gate(p, A).await.expect("gate").profile_error().map(|e| e.0), Some("AccountTakedown"));
        db.drop_db().await;
    }
}
