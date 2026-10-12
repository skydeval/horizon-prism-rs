//! The in-memory signing keys of ready members (design-v3 §II-8.3 "The
//! ready-member gate").
//!
//! The pre-auth backstop verifies a token only when its unverified `iss` is
//! a ready member whose DID document is held here, so that step reads no
//! database and makes no network call. What a match means depends on the
//! document's age:
//! - under 1 h ([`FRESH`], the fresh-row bound of §II-6.7): the key
//!   authenticates;
//! - 1 h to 24 h ([`STALE_MAX`], `did_cache`'s stale-usable window): the key
//!   only selects the member's DID for charging, and the handler verifies
//!   again under the freshness rule;
//! - older, or absent: no key, and the request is charged to its IP key.
//!
//! **How the table is kept.** A background task reloads it from `members`
//! and `did_cache` every [`RELOAD_EVERY`], and re-resolves every ready
//! member whose document is older than [`REFRESH_AFTER`] or missing. So a
//! member that becomes ready, or whose document is rewritten, is picked up
//! within one reload, and a ready member whose document resolves always has
//! a key under 24 h old. A failed resolution is retried after 1 minute, then
//! 10 minutes, then hourly. The refresh is internal work: it is not charged
//! to any request bucket.

use std::collections::{HashMap, HashSet};
use std::hash::{DefaultHasher, Hash, Hasher};
use std::sync::{Arc, PoisonError, RwLock};
use std::time::{Duration, Instant};

use chrono::{DateTime, Utc};
use serde_json::Value;
use sqlx::PgPool;
use tokio::task::JoinHandle;

use super::backstop::KeyPrint;
use crate::identity::{IdentityResolver, ResolveIntent};

/// A document younger than this authenticates.
pub const FRESH: Duration = Duration::from_secs(3600);
/// A document older than this is no key at all.
pub const STALE_MAX: Duration = Duration::from_secs(24 * 3600);
/// A document older than this is re-resolved in the background.
pub const REFRESH_AFTER: Duration = Duration::from_secs(12 * 3600);
/// How often the table is reloaded.
pub const RELOAD_EVERY: Duration = Duration::from_secs(30);
/// Retry delays after a failed resolution; the last repeats.
const BACKOFF: [Duration; 3] = [Duration::from_secs(60), Duration::from_secs(600), Duration::from_secs(3600)];
/// Members re-resolved per reload, so a restart does not burst at PLC.
const REFRESH_PER_PASS: usize = 20;

pub const METRIC_REFRESH_FAILED: &str = "member_key_refresh_failed_total";
pub const METRIC_KEYS: &str = "member_keys";

/// How far a held key can be trusted.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KeyAge {
    /// Authenticates.
    Fresh,
    /// Selects the member's DID key only.
    Stale,
}

/// A ready member's DID document.
#[derive(Debug, Clone)]
pub struct MemberKey {
    /// The document, as JSON bytes.
    pub doc: Arc<Vec<u8>>,
    pub fetched_at: DateTime<Utc>,
    /// A fingerprint of the document's `#atproto` key.
    pub print: KeyPrint,
}

impl MemberKey {
    /// A key from a DID document, or `None` when it has no `#atproto`
    /// verification method.
    #[must_use]
    pub fn from_document(doc: &Value, fetched_at: DateTime<Utc>) -> Option<Self> {
        let key = doc
            .get("verificationMethod")?
            .as_array()?
            .iter()
            .find(|m| m.get("id").and_then(Value::as_str).is_some_and(|id| id.ends_with("#atproto")))?
            .get("publicKeyMultibase")?
            .as_str()?;
        let mut hasher = DefaultHasher::new();
        key.hash(&mut hasher);
        Some(Self {
            doc: Arc::new(serde_json::to_vec(doc).ok()?),
            fetched_at,
            print: hasher.finish(),
        })
    }

    /// The key's standing at `now`, or `None` past [`STALE_MAX`].
    #[must_use]
    pub fn age(&self, now: DateTime<Utc>) -> Option<KeyAge> {
        let age = now.signed_duration_since(self.fetched_at).to_std().unwrap_or(Duration::ZERO);
        if age < FRESH {
            Some(KeyAge::Fresh)
        } else if age < STALE_MAX {
            Some(KeyAge::Stale)
        } else {
            None
        }
    }
}

/// The table. Cheap to clone.
#[derive(Debug, Clone, Default)]
pub struct MemberKeys {
    keys: Arc<RwLock<HashMap<String, MemberKey>>>,
    /// Every ready member, with or without a document held.
    ready: Arc<RwLock<HashSet<String>>>,
}

impl MemberKeys {
    /// Whether `did` was a ready member at the last reload.
    #[must_use]
    pub fn is_ready_member(&self, did: &str) -> bool {
        self.ready.read().unwrap_or_else(PoisonError::into_inner).contains(did)
    }

    /// Mark `did` a ready member (tests).
    #[cfg(test)]
    pub fn mark_ready(&self, did: &str) {
        self.ready.write().unwrap_or_else(PoisonError::into_inner).insert(did.to_string());
    }

    /// The key held for `did`, whatever its age.
    #[must_use]
    pub fn get(&self, did: &str) -> Option<MemberKey> {
        self.keys.read().unwrap_or_else(PoisonError::into_inner).get(did).cloned()
    }

    /// Whether `did` is a ready member with a document held.
    #[must_use]
    pub fn contains(&self, did: &str) -> bool {
        self.keys.read().unwrap_or_else(PoisonError::into_inner).contains_key(did)
    }

    /// Hold `doc` for `did` (tests, and a document just fetched).
    pub fn insert(&self, did: &str, doc: &Value, fetched_at: DateTime<Utc>) {
        let mut keys = self.keys.write().unwrap_or_else(PoisonError::into_inner);
        match MemberKey::from_document(doc, fetched_at) {
            Some(key) => keys.insert(did.to_string(), key),
            None => keys.remove(did),
        };
    }

    /// Replace the table with the ready members' documents in `did_cache`.
    /// Returns the members that left the table, and the ready members whose
    /// document is missing or older than [`REFRESH_AFTER`].
    ///
    /// # Errors
    ///
    /// Any database error; the table is unchanged on error.
    pub async fn reload(&self, pool: &PgPool) -> Result<(Vec<String>, Vec<String>), sqlx::Error> {
        let rows: Vec<(String, Option<Value>, Option<DateTime<Utc>>)> = sqlx::query_as(
            "SELECT m.did, d.doc, d.fetched_at FROM members m LEFT JOIN did_cache d ON d.did = m.did \
             WHERE m.ready_at IS NOT NULL",
        )
        .fetch_all(pool)
        .await?;
        let now = Utc::now();
        let mut fresh: HashMap<String, MemberKey> = HashMap::with_capacity(rows.len());
        let mut due = Vec::new();
        *self.ready.write().unwrap_or_else(PoisonError::into_inner) = rows.iter().map(|r| r.0.clone()).collect();
        for (did, doc, fetched_at) in rows {
            let key = doc.zip(fetched_at).and_then(|(doc, at)| MemberKey::from_document(&doc, at));
            let old = key.as_ref().is_none_or(|k| {
                now.signed_duration_since(k.fetched_at).to_std().unwrap_or(Duration::ZERO) >= REFRESH_AFTER
            });
            if old {
                due.push(did.clone());
            }
            if let Some(key) = key {
                fresh.insert(did, key);
            }
        }
        metrics::gauge!(METRIC_KEYS).set(fresh.len() as f64);
        let mut keys = self.keys.write().unwrap_or_else(PoisonError::into_inner);
        let gone = keys.keys().filter(|did| !fresh.contains_key(*did)).cloned().collect();
        *keys = fresh;
        Ok((gone, due))
    }

    /// Keep the table current (see the module doc). `forget` is told about
    /// members that left it.
    #[must_use]
    pub fn spawn(self, pool: PgPool, resolver: IdentityResolver, forget: impl Fn(&str) + Send + 'static) -> JoinHandle<()> {
        tokio::spawn(async move {
            // DID → (failures so far, earliest next attempt).
            let mut failed: HashMap<String, (usize, Instant)> = HashMap::new();
            let mut tick = tokio::time::interval(RELOAD_EVERY);
            loop {
                tick.tick().await;
                let (gone, due) = match self.reload(&pool).await {
                    Ok(v) => v,
                    Err(error) => {
                        tracing::warn!(event = "member_keys_reload_failed", %error, "member key reload failed");
                        continue;
                    }
                };
                for did in &gone {
                    forget(did);
                    failed.remove(did);
                }
                let now = Instant::now();
                let ready: Vec<String> = due
                    .into_iter()
                    .filter(|did| failed.get(did).is_none_or(|(_, at)| now >= *at))
                    .take(REFRESH_PER_PASS)
                    .collect();
                for did in ready {
                    match resolver.refresh_now(&did, ResolveIntent::Lookup).await {
                        Ok(fetched) => {
                            failed.remove(&did);
                            self.insert(&did, &fetched.doc, fetched.fetched_at);
                        }
                        Err(error) => {
                            let n = failed.get(&did).map_or(0, |(n, _)| *n);
                            let wait = BACKOFF[n.min(BACKOFF.len() - 1)];
                            failed.insert(did.clone(), (n + 1, Instant::now() + wait));
                            metrics::counter!(METRIC_REFRESH_FAILED).increment(1);
                            tracing::debug!(event = "member_key_refresh_failed", did = %did, %error, "member key refresh failed");
                        }
                    }
                }
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    const MEMBER: &str = "did:plc:aaaaaaaaaaaaaaaaaaaaaaaa";

    fn doc(key: &str) -> Value {
        json!({"id": MEMBER, "verificationMethod": [
            {"id": format!("{MEMBER}#other"), "type": "Multikey", "publicKeyMultibase": "zOther"},
            {"id": format!("{MEMBER}#atproto"), "type": "Multikey", "publicKeyMultibase": key},
        ]})
    }

    #[test]
    fn a_key_needs_an_atproto_method_and_ages_out() {
        let now = Utc::now();
        let key = MemberKey::from_document(&doc("zKeyOne"), now).expect("key");
        assert_ne!(key.print, MemberKey::from_document(&doc("zKeyTwo"), now).expect("key").print);
        assert_eq!(key.print, MemberKey::from_document(&doc("zKeyOne"), now).expect("key").print);
        assert!(MemberKey::from_document(&json!({"id": MEMBER}), now).is_none());
        assert!(MemberKey::from_document(&json!({"id": MEMBER, "verificationMethod": [{"id": "#other"}]}), now).is_none());

        let at = |secs: i64| now + chrono::Duration::seconds(secs);
        assert_eq!(key.age(at(0)), Some(KeyAge::Fresh));
        assert_eq!(key.age(at(3599)), Some(KeyAge::Fresh));
        assert_eq!(key.age(at(3600)), Some(KeyAge::Stale));
        assert_eq!(key.age(at(86_399)), Some(KeyAge::Stale));
        assert_eq!(key.age(at(86_400)), None);

        let keys = MemberKeys::default();
        keys.insert(MEMBER, &doc("zKeyOne"), now);
        assert!(keys.contains(MEMBER));
        // A document without the key clears the entry.
        keys.insert(MEMBER, &json!({"id": MEMBER}), now);
        assert!(keys.get(MEMBER).is_none());
    }

    #[tokio::test]
    async fn reload_holds_ready_members_with_a_document() {
        let Some(db) = crate::test_support::fresh_db("member_keys_reload").await else {
            return;
        };
        let p = &db.pool;
        let (ready, old, bare, pending) = (
            MEMBER,
            "did:plc:bbbbbbbbbbbbbbbbbbbbbbbb",
            "did:plc:cccccccccccccccccccccccc",
            "did:plc:dddddddddddddddddddddddd",
        );
        for (did, is_ready) in [(ready, true), (old, true), (bare, true), (pending, false)] {
            sqlx::query("INSERT INTO members (did, source, ready_at) VALUES ($1, 'admin', CASE WHEN $2 THEN now() END)")
                .bind(did)
                .bind(is_ready)
                .execute(p)
                .await
                .expect("member");
        }
        for (did, age_hours) in [(ready, 0), (old, 13), (pending, 0)] {
            sqlx::query(
                "INSERT INTO did_cache (did, doc, fetched_at, stale_at, expires_at) \
                 VALUES ($1, $2, now() - make_interval(hours => $3), now(), now() + interval '1 day')",
            )
            .bind(did)
            .bind(doc("zKey"))
            .bind(age_hours)
            .execute(p)
            .await
            .expect("did_cache");
        }
        let keys = MemberKeys::default();
        keys.insert("did:plc:eeeeeeeeeeeeeeeeeeeeeeee", &doc("zGone"), Utc::now());
        let (gone, mut due) = keys.reload(p).await.expect("reload");
        due.sort();
        assert_eq!(gone, ["did:plc:eeeeeeeeeeeeeeeeeeeeeeee"]);
        assert_eq!(due, [old, bare], "a 13 h old document and a missing one are due");
        assert_eq!(keys.get(ready).and_then(|k| k.age(Utc::now())), Some(KeyAge::Fresh));
        assert_eq!(keys.get(old).and_then(|k| k.age(Utc::now())), Some(KeyAge::Stale));
        assert!(!keys.contains(bare) && !keys.contains(pending), "no document, or not ready");
        assert!(keys.is_ready_member(bare) && !keys.is_ready_member(pending));
        db.drop_db().await;
    }
}
