//! Copy tiers: how much of the network one relay's copy of a commit can be
//! trusted to carry, read from the relay reader's capability stamp.
//!
//! - **Whole**: the reader's filter is empty or passes every stored
//!   collection. Only a whole copy advances a tracked repo's chain.
//! - **Covered**: the filter passes every collection full indexing stores
//!   for any account, but not every stored collection.
//! - **Aside**: the filter leaves out a collection full indexing stores.
//!
//! A relay's last stamp is remembered per label (`worker_state['relay_caps']`),
//! so an entry with no stamp after one with a filter (a downgrade) is set
//! aside until the operator forgets the stamp.

use std::collections::{BTreeMap, BTreeSet};

use serde_json::{Value as Json, json};
use sqlx::PgPool;

use super::classifier::STORED_SET;
use super::stream::Cap;
use super::watermark::UNKNOWN_RELAY;

/// The `worker_state` key of the remembered stamps.
pub const RELAY_CAPS_KEY: &str = "relay_caps";

/// A copy's tier.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum CopyTier {
    Whole,
    Covered,
    Aside,
}

impl CopyTier {
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Whole => "whole",
            Self::Covered => "covered",
            Self::Aside => "aside",
        }
    }
}

/// Tiers per relay label, with the stamps last seen.
#[derive(Debug, Clone)]
pub struct Tiers {
    stored: BTreeSet<String>,
    full: BTreeSet<String>,
    old_filtered: BTreeSet<String>,
    /// Label → the record filter of its last stamp (`None`: no filter).
    last: BTreeMap<String, Option<BTreeSet<String>>>,
    /// Label → the tier last logged, so a change is logged once.
    logged: BTreeMap<String, CopyTier>,
}

impl Tiers {
    /// `full_set` is the full set of the enabled groups; `old_filtered` the
    /// labels of `firehose.old_filtered_relays`.
    #[must_use]
    pub fn new(full_set: BTreeSet<String>, old_filtered: impl IntoIterator<Item = String>) -> Self {
        Self {
            stored: STORED_SET.iter().map(|s| (*s).to_string()).collect(),
            full: full_set,
            old_filtered: old_filtered.into_iter().collect(),
            last: BTreeMap::new(),
            logged: BTreeMap::new(),
        }
    }

    /// The tier of a filter.
    fn of_filter(&self, filter: Option<&BTreeSet<String>>) -> CopyTier {
        match filter {
            None => CopyTier::Whole,
            Some(f) if self.stored.is_subset(f) => CopyTier::Whole,
            Some(f) if self.full.is_subset(f) => CopyTier::Covered,
            Some(_) => CopyTier::Aside,
        }
    }

    /// The tier of one copy from the relay labelled `label` (aliases
    /// already resolved), stamped `cap`.
    #[must_use]
    pub fn tier(&self, label: &str, cap: Option<&Cap>) -> CopyTier {
        if let Some(cap) = cap {
            return self.of_filter(cap.record_types.as_ref());
        }
        // No stamp: an older reader.
        if self.old_filtered.contains(label) || (label == UNKNOWN_RELAY && !self.old_filtered.is_empty()) {
            return CopyTier::Aside;
        }
        match self.last.get(label) {
            // A downgrade from a reader that ran a filter.
            Some(Some(_)) => CopyTier::Aside,
            Some(None) | None => CopyTier::Whole,
        }
    }

    /// The full-set collections a stamp's filter leaves out.
    #[must_use]
    pub fn left_out(&self, cap: Option<&Cap>) -> Vec<String> {
        match cap.and_then(|c| c.record_types.as_ref()) {
            Some(f) => self.full.difference(f).cloned().collect(),
            None => Vec::new(),
        }
    }

    /// Remember the stamp of an entry from `label`; an entry with no stamp
    /// changes nothing. Returns whether the remembered stamps changed (and
    /// should be persisted). The tier of `label` is logged when it changes.
    pub fn observe(&mut self, label: &str, cap: Option<&Cap>) -> bool {
        let tier = self.tier(label, cap);
        if self.logged.get(label) != Some(&tier) {
            self.logged.insert(label.to_string(), tier);
            if tier == CopyTier::Aside {
                let left_out = self.left_out(cap);
                tracing::warn!(
                    event = "firehose_relay_aside",
                    relay = %label,
                    left_out = %left_out.join(","),
                    stamped = cap.is_some(),
                    "a relay's copies are set aside for full indexing: its filter leaves out collections full indexing stores, or its reader was downgraded"
                );
            } else {
                tracing::info!(event = "firehose_relay_tier", relay = %label, tier = tier.as_str(), "a relay's copy tier");
            }
        }
        let Some(cap) = cap else {
            return false;
        };
        let filter = cap.record_types.clone();
        if self.last.get(label) == Some(&filter) {
            return false;
        }
        self.last.insert(label.to_string(), filter);
        true
    }

    /// Forget the remembered stamp of `label`.
    pub fn forget(&mut self, label: &str) {
        self.last.remove(label);
        self.logged.remove(label);
    }

    fn to_json(&self) -> Json {
        let map: serde_json::Map<String, Json> = self
            .last
            .iter()
            .map(|(label, f)| (label.clone(), json!({ "record_types": f })))
            .collect();
        Json::Object(map)
    }

    fn load_json(&mut self, v: &Json) {
        let Some(map) = v.as_object() else {
            return;
        };
        for (label, entry) in map {
            let filter = match entry.get("record_types") {
                None | Some(Json::Null) => None,
                Some(Json::Array(items)) => Some(items.iter().filter_map(Json::as_str).map(str::to_string).collect()),
                // Unreadable: a filter, so an unstamped copy stays aside.
                Some(_) => Some(BTreeSet::new()),
            };
            self.last.insert(label.clone(), filter);
        }
    }

    /// Restore the remembered stamps.
    ///
    /// # Errors
    ///
    /// Any database error.
    pub async fn restore(&mut self, pool: &PgPool) -> Result<(), sqlx::Error> {
        let v: Option<Json> = sqlx::query_scalar("SELECT value FROM worker_state WHERE key = $1")
            .bind(RELAY_CAPS_KEY)
            .fetch_optional(pool)
            .await?;
        if let Some(v) = v {
            self.load_json(&v);
        }
        Ok(())
    }

    /// Persist the remembered stamps.
    ///
    /// # Errors
    ///
    /// Any database error.
    pub async fn persist(&self, pool: &PgPool) -> Result<(), sqlx::Error> {
        sqlx::query(
            "INSERT INTO worker_state (key, value, updated_at) VALUES ($1, $2, now()) \
             ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value, updated_at = now()",
        )
        .bind(RELAY_CAPS_KEY)
        .bind(self.to_json())
        .execute(pool)
        .await?;
        Ok(())
    }
}

/// Forget the remembered stamp of `label` in the database, so an unstamped
/// relay reader that was downgraded counts as whole again. Returns whether
/// a stamp was remembered. The consumer reads the stamps at its start.
///
/// # Errors
///
/// Any database error.
pub async fn forget_cap(pool: &PgPool, label: &str) -> Result<bool, sqlx::Error> {
    let n = sqlx::query("UPDATE worker_state SET value = value - $2, updated_at = now() WHERE key = $1 AND value ? $2")
        .bind(RELAY_CAPS_KEY)
        .bind(label)
        .execute(pool)
        .await?
        .rows_affected();
    Ok(n > 0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::FullGroup;
    use crate::indexing::full::full_set;

    fn cap(filter: Option<&[&str]>) -> Cap {
        Cap {
            version: 1,
            caps: vec![Cap::SYNC.into(), Cap::RESUME.into(), Cap::OPLESS_FILTERED.into()],
            record_types: filter.map(|f| f.iter().map(|s| (*s).to_string()).collect()),
        }
    }

    /// Each tier from a stamp: no filter and a filter of the stored set are
    /// whole; one passing the full set but not a member-only collection is
    /// covered; one leaving out a full-set collection is aside.
    #[test]
    fn tiers_from_the_stamp() {
        let t = Tiers::new(full_set(&[FullGroup::Posts]), Vec::new());
        assert_eq!(t.tier("bsky", Some(&cap(None))), CopyTier::Whole);
        assert_eq!(t.tier("bsky", Some(&cap(Some(&STORED_SET)))), CopyTier::Whole);
        let mut covered: Vec<&str> = crate::indexing::full::collections(FullGroup::Posts).to_vec();
        covered.extend(crate::firehose::classifier::PLURAL_COLLECTIONS);
        assert_eq!(t.tier("bsky", Some(&cap(Some(&covered)))), CopyTier::Covered);
        assert_eq!(t.tier("bsky", Some(&cap(Some(&["app.bsky.feed.post"])))), CopyTier::Aside);
        assert_eq!(t.left_out(Some(&cap(Some(&["app.bsky.feed.post"])))).len(), covered.len() - 1);
        // With every group on, a likes-less filter is aside.
        let all = Tiers::new(full_set(&FullGroup::ALL), Vec::new());
        let no_likes: Vec<&str> = STORED_SET.iter().copied().filter(|c| *c != "app.bsky.feed.like").collect();
        assert_eq!(all.tier("bsky", Some(&cap(Some(&no_likes)))), CopyTier::Aside);
        assert_eq!(all.left_out(Some(&cap(Some(&no_likes)))), vec!["app.bsky.feed.like".to_string()]);
    }

    /// Without a stamp: an older reader with an empty filter is whole; one
    /// listed in `old_filtered_relays` is aside, and so is `other` while
    /// that list is not empty; a reader that stamped a filter and then
    /// stopped stamping is aside until its stamp is forgotten.
    #[test]
    fn unstamped_copies() {
        let mut t = Tiers::new(full_set(&FullGroup::ALL), vec!["old".to_string()]);
        assert_eq!(t.tier("bsky", None), CopyTier::Whole);
        assert_eq!(t.tier("old", None), CopyTier::Aside);
        assert_eq!(t.tier(UNKNOWN_RELAY, None), CopyTier::Aside);
        assert_eq!(Tiers::new(full_set(&FullGroup::ALL), Vec::new()).tier(UNKNOWN_RELAY, None), CopyTier::Whole);
        // A stamp with no filter, then none: still whole.
        assert!(t.observe("bsky", Some(&cap(None))));
        assert!(!t.observe("bsky", Some(&cap(None))));
        assert!(!t.observe("bsky", None));
        assert_eq!(t.tier("bsky", None), CopyTier::Whole);
        // A filtered stamp, then a downgrade.
        assert!(t.observe("bsky", Some(&cap(Some(&STORED_SET)))));
        assert_eq!(t.tier("bsky", Some(&cap(Some(&STORED_SET)))), CopyTier::Whole);
        assert_eq!(t.tier("bsky", None), CopyTier::Aside);
        t.forget("bsky");
        assert_eq!(t.tier("bsky", None), CopyTier::Whole);
    }

    /// The remembered stamps survive a restart, and `forget_cap` removes one.
    #[tokio::test]
    async fn stamps_persist_and_forget() {
        let Some(db) = crate::test_support::fresh_db("stamps_persist_and_forget").await else {
            return;
        };
        let pool = &db.pool;
        let mut t = Tiers::new(full_set(&FullGroup::ALL), Vec::new());
        t.observe("bsky", Some(&cap(Some(&["app.bsky.feed.post"]))));
        t.observe("microcosm", Some(&cap(None)));
        t.persist(pool).await.expect("persist");
        let mut back = Tiers::new(full_set(&FullGroup::ALL), Vec::new());
        back.restore(pool).await.expect("restore");
        assert_eq!(back.tier("bsky", None), CopyTier::Aside);
        assert_eq!(back.tier("microcosm", None), CopyTier::Whole);
        assert!(forget_cap(pool, "bsky").await.expect("forget"));
        assert!(!forget_cap(pool, "bsky").await.expect("forget again"));
        let mut again = Tiers::new(full_set(&FullGroup::ALL), Vec::new());
        again.restore(pool).await.expect("restore");
        assert_eq!(again.tier("bsky", None), CopyTier::Whole);
        db.drop_db().await;
    }
}
