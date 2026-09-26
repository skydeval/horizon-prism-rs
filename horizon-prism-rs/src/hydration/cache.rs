//! The `rt` read-time fetch cache (design-v2 §7.6): in-process, never Redis.
//! A `moka` future cache weighted by value bytes, total `cache.read_time_bytes`
//! (default 256 MiB), TTL 300 s; values over 256 KiB are not cached. Request
//! traffic can only evict entries, never grow memory past the bound.

use std::sync::Arc;
use std::time::Duration;

use moka::future::Cache;
use serde::Serialize;
use serde::de::DeserializeOwned;

/// Entry lifetime (§7.6).
pub const TTL: Duration = Duration::from_secs(300);
/// Largest cached value (§7.6).
pub const MAX_VALUE_BYTES: usize = 256 * 1024;

/// The read-time cache. Cheap to clone.
#[derive(Clone)]
pub struct ReadCache {
    cache: Cache<String, Arc<Vec<u8>>>,
}

impl ReadCache {
    #[must_use]
    pub fn new(capacity_bytes: u64) -> Self {
        Self {
            cache: Cache::builder()
                .max_capacity(capacity_bytes)
                .weigher(|k: &String, v: &Arc<Vec<u8>>| u32::try_from(k.len() + v.len()).unwrap_or(u32::MAX))
                .time_to_live(TTL)
                .build(),
        }
    }

    pub async fn get_json<T: DeserializeOwned>(&self, key: &str) -> Option<T> {
        let bytes = self.cache.get(key).await?;
        serde_json::from_slice(&bytes).ok()
    }

    /// Cache a value unless it exceeds [`MAX_VALUE_BYTES`].
    pub async fn put_json<T: Serialize>(&self, key: &str, value: &T) {
        let Ok(bytes) = serde_json::to_vec(value) else {
            return;
        };
        if bytes.len() <= MAX_VALUE_BYTES {
            self.cache.insert(key.to_string(), Arc::new(bytes)).await;
        }
    }

    /// Current weighted size (tests).
    pub async fn weighted_size(&self) -> u64 {
        self.cache.run_pending_tasks().await;
        self.cache.weighted_size()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn round_trip_bound_and_oversize() {
        let c = ReadCache::new(10 * 1024);
        c.put_json("a", &vec![1u32, 2, 3]).await;
        assert_eq!(c.get_json::<Vec<u32>>("a").await, Some(vec![1, 2, 3]));
        c.put_json("big", &"x".repeat(MAX_VALUE_BYTES + 1)).await;
        assert_eq!(c.get_json::<String>("big").await, None);
        for i in 0..100 {
            c.put_json(&format!("k{i}"), &"y".repeat(1000)).await;
        }
        assert!(c.weighted_size().await <= 10 * 1024);
    }
}
