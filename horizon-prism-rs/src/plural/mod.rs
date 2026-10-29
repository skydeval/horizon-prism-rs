//! Pronouns and plural systems: the public read of the four
//! collections for any DID, without sign-in, so a reader's browser never
//! asks an author's server.
//!
//! - The firehose stores every record of the four collections, from any
//!   author ([`crate::aggregations::AdmitRule::Plural`], `plural_records`).
//! - The first time a DID is asked about, its repo is listed once
//!   ([`fill`]); from then on an absent record is provably absent.
//! - Every answer is a record, `none` or `unknown` ([`answer`]); `none` only
//!   where the repo was listed and the firehose is current.
//! - [`handlers`]: `app.nearhorizon.plural.getRecords` and `listMembers`.

pub mod fill;
pub mod handlers;
mod reads;
#[cfg(test)]
mod tests;

use chrono::{DateTime, Utc};
use serde_json::{Value, json};

pub use crate::firehose::classifier::{NSID_PLURAL_FRONT, NSID_PLURAL_MEMBER, NSID_PLURAL_SYSTEM, NSID_PRONOUNS, PLURAL_COLLECTIONS};

/// Every post record a view of this AppView carries keeps the fronters key
/// (plural-pronouns design §1.3).
pub const FEATURE_FRONTERS: &str = "plural.fronters";
/// The public read is served (§2.9).
pub const FEATURE_READ: &str = "plural.read";
/// The fronters key on a post record (§0).
pub const FRONTERS_KEY: &str = "app.nearhorizon.plural.fronters";

pub const GET_RECORDS: &str = "app.nearhorizon.plural.getRecords";
pub const LIST_MEMBERS: &str = "app.nearhorizon.plural.listMembers";

/// Metric: reads, by `method` and `outcome`.
pub const METRIC_READS: &str = "plural_reads_total";
/// Metric: read latency, by `method`.
pub const METRIC_READ_SECONDS: &str = "plural_read_seconds";
/// Metric: answers, by `kind` and `status`.
pub const METRIC_ANSWERS: &str = "plural_answers_total";
/// Metric: fills, by `outcome`.
pub const METRIC_FILLS: &str = "plural_fills_total";
/// Metric: fill latency.
pub const METRIC_FILL_SECONDS: &str = "plural_fill_seconds";
/// Metric: `listRecords` calls of fills, by `outcome`.
pub const METRIC_FILL_CALLS: &str = "plural_fill_calls_total";
/// Gauge: `plural_repos` rows, by `state`.
pub const METRIC_REPOS: &str = "plural_repos";

/// What Prism knows of a DID's repo (§2.4).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RepoState {
    Pending,
    Complete,
    Unavailable,
    Failed,
    Gone,
}

impl RepoState {
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Complete => "complete",
            Self::Unavailable => "unavailable",
            Self::Failed => "failed",
            Self::Gone => "gone",
        }
    }

    #[must_use]
    pub fn parse(s: &str) -> Option<Self> {
        Some(match s {
            "pending" => Self::Pending,
            "complete" => Self::Complete,
            "unavailable" => Self::Unavailable,
            "failed" => Self::Failed,
            "gone" => Self::Gone,
            _ => return None,
        })
    }
}

/// One `plural_repos` row.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RepoRow {
    pub state: RepoState,
    pub verified_at: Option<DateTime<Utc>>,
    pub next_fill_at: Option<DateTime<Utc>>,
    pub members_truncated: bool,
}

/// A stored record: its CID and value (`None`: over the size bound).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Stored {
    pub cid: String,
    pub value: Option<Value>,
}

/// One answer (§2.5).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Answer {
    Record { uri: String, cid: String, value: Value },
    None,
    Unknown,
}

impl Answer {
    #[must_use]
    pub fn status(&self) -> &'static str {
        match self {
            Self::Record { .. } => "record",
            Self::None => "none",
            Self::Unknown => "unknown",
        }
    }

    /// The wire form (`#answer`).
    #[must_use]
    pub fn to_json(&self) -> Value {
        match self {
            Self::Record { uri, cid, value } => json!({"status": "record", "uri": uri, "cid": cid, "value": value}),
            Self::None => json!({"status": "none"}),
            Self::Unknown => json!({"status": "unknown"}),
        }
    }
}

/// What one answer is decided from.
#[derive(Debug, Clone, Copy)]
pub struct Facts<'a> {
    /// The gate hides the DID or the record's URI (§2.7).
    pub hidden: bool,
    pub repo: Option<&'a RepoRow>,
    pub stored: Option<&'a Stored>,
    /// The newest relay watermark is within `max_stream_lag_secs`.
    pub fresh: bool,
    /// The record is a member (a truncated listing proves nothing of it).
    pub member: bool,
}

/// The one answer table (§2.5, R2), in its order.
#[must_use]
pub fn answer(uri: &str, f: Facts<'_>) -> Answer {
    if f.hidden {
        return Answer::Unknown;
    }
    let state = f.repo.map(|r| r.state);
    match state {
        Some(RepoState::Unavailable) => return Answer::Unknown,
        Some(RepoState::Gone) => return Answer::None,
        _ => {}
    }
    if let Some(s) = f.stored {
        return match &s.value {
            Some(value) => Answer::Record {
                uri: uri.to_string(),
                cid: s.cid.clone(),
                value: value.clone(),
            },
            None => Answer::Unknown,
        };
    }
    let complete = f.repo.is_some_and(|r| r.state == RepoState::Complete && !(f.member && r.members_truncated));
    if complete && f.fresh { Answer::None } else { Answer::Unknown }
}

/// The URI of a record of `collection` in `did`'s repo.
#[must_use]
pub fn record_uri(did: &str, collection: &str, rkey: &str) -> String {
    format!("at://{did}/{collection}/{rkey}")
}

/// A member URI's DID and rkey, when `uri` is exactly
/// `at://<did>/app.nearhorizon.plural.member/<rkey>` with a valid DID and
/// rkey.
#[must_use]
pub fn parse_member_uri(uri: &str) -> Option<(&str, &str)> {
    let rest = uri.strip_prefix("at://")?;
    let (did, rest) = rest.split_once('/')?;
    let rkey = rest.strip_prefix(NSID_PLURAL_MEMBER)?.strip_prefix('/')?;
    (proto_blue::syntax::Did::is_valid(did) && proto_blue::syntax::RecordKey::is_valid(rkey)).then_some((did, rkey))
}
