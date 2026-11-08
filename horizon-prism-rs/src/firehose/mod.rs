//! Firehose ingest (design-v2 §5): the Redis stream consumer, classification,
//! normalization, the LWW writer, watermarks, gap handling, relay coverage,
//! dead letters and backfill jobs.

pub mod alert_rules;
pub mod backfill;
pub mod classifier;
pub mod consumer;
pub mod coverage;
pub mod deadletter;
pub mod dedupe;
pub mod extract;
pub mod feed;
pub mod follows_pass;
pub mod gap;
pub mod index_request;
pub mod jobs;
pub mod normalize;
pub mod post_search;
pub mod records;
pub mod router;
pub mod rows;
pub mod scripts;
pub mod stream;
pub mod tier;
pub mod v3_resync;
pub mod watermark;
pub mod writer;
