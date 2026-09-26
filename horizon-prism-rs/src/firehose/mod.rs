//! Firehose ingest (design-v2 §5): the Redis stream consumer, classification,
//! normalization, the LWW writer, watermarks, gap handling, relay coverage,
//! dead letters and backfill jobs.

pub mod backfill;
pub mod classifier;
pub mod consumer;
pub mod coverage;
pub mod deadletter;
pub mod gap;
pub mod index_request;
pub mod jobs;
pub mod normalize;
pub mod records;
pub mod router;
pub mod scripts;
pub mod stream;
pub mod watermark;
pub mod writer;
