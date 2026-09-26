//! Firehose ingest (design-v2 §5): classification, normalization, the LWW
//! writer and backfill jobs.

pub mod classifier;
pub mod jobs;
pub mod normalize;
pub mod records;
pub mod writer;
