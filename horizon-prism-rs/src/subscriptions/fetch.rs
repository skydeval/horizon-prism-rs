//! Reading a list owner's records for a `list_sync` run, within the
//! per-owner bounds (design-v3 §4.3 "The run" steps 2a and 3, "Per-owner
//! bounds").
//!
//! Two listings, one after the other: the owner's `app.bsky.graph.list`
//! records (for each list's purpose), then its `app.bsky.graph.listitem`
//! records. Both are paged to the end. No record is ever fetched by name,
//! so the owner's server cannot tell which of its lists is subscribed.
//!
//! The two listings are counted together and stop at the first bound:
//! too many records, too many pages, answers too slow, too long spent in
//! calls, or too long in all. A listing that stopped early is **cut**: it
//! is incomplete, and the run that reads it only adds items.

use std::collections::HashMap;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use serde_json::Value;

use super::sync::{ListedItem, Listing};
use crate::firehose::backfill::ListedRecord;

pub const NSID_LIST: &str = "app.bsky.graph.list";
pub const NSID_LISTITEM: &str = "app.bsky.graph.listitem";
/// A wall-time cut whose pages averaged fewer records than this is a
/// "pages" cut: the owner serves tiny pages, it is not large.
const SMALL_PAGE_RECORDS: u64 = 50;

/// One page of an owner's records.
#[derive(Debug, Clone, Default)]
pub struct Page {
    pub records: Vec<ListedRecord>,
    pub cursor: Option<String>,
    /// Time spent inside the call itself, not waiting for a turn to make
    /// it: this server's own load must never make an owner look slow.
    pub call_time: Duration,
}

/// Where an owner's records are read from (its PDS in production).
#[async_trait]
pub trait OwnerRecords: Send + Sync {
    /// One `listRecords` page of `collection`.
    async fn page(&self, collection: &str, cursor: Option<&str>) -> Result<Page, String>;
    /// Whether the run should stop: its job was superseded.
    async fn superseded(&self) -> bool {
        false
    }
}

/// The per-owner bounds (`lists.*`).
#[derive(Debug, Clone, Copy)]
pub struct Bounds {
    pub max_records: u64,
    pub max_pages: u32,
    pub probe_call: Duration,
    pub min_records_per_call_sec: u32,
    pub max_call: Duration,
    pub max_wall: Duration,
}

/// Which bound cut a listing.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CutReason {
    Records,
    Pages,
    Throughput,
    Time,
    Wall,
}

impl CutReason {
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Records => "records",
            Self::Pages => "pages",
            Self::Throughput => "throughput",
            Self::Time => "time",
            Self::Wall => "wall",
        }
    }

    /// A slow answerer or a server of tiny pages goes to the slow lane; an
    /// owner that is merely large does not.
    #[must_use]
    pub fn slow(self) -> bool {
        matches!(self, Self::Throughput | Self::Pages)
    }
}

/// Why nothing usable was read.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FetchError {
    /// A call failed: the run fails and is retried.
    Failed(String),
    /// The job was superseded between two pages.
    Superseded,
}

/// What the two listings gave.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Fetched {
    pub listing: Listing,
    /// The bound that stopped the listings early, if one did.
    pub cut: Option<CutReason>,
}

/// The running totals of one run's listings.
struct Meter {
    bounds: Bounds,
    started: Instant,
    records: u64,
    pages: u32,
    call_time: Duration,
}

impl Meter {
    /// The bound that is reached now, given the records and pages of the
    /// listing currently being read.
    fn cut(&self, listing_records: u64, listing_pages: u32) -> Option<CutReason> {
        let b = &self.bounds;
        if self.records > b.max_records {
            return Some(CutReason::Records);
        }
        if self.pages > b.max_pages {
            return Some(CutReason::Pages);
        }
        if self.call_time >= b.probe_call {
            let per_sec = self.records as f64 / self.call_time.as_secs_f64().max(f64::MIN_POSITIVE);
            if per_sec < f64::from(b.min_records_per_call_sec) {
                return Some(CutReason::Throughput);
            }
        }
        if self.call_time >= b.max_call {
            return Some(CutReason::Time);
        }
        if self.started.elapsed() >= b.max_wall {
            let small = listing_records < SMALL_PAGE_RECORDS * u64::from(listing_pages.max(1));
            return Some(if small { CutReason::Pages } else { CutReason::Wall });
        }
        None
    }
}

/// Read one collection to its end or to a bound. Returns its records and
/// the bound that stopped it.
async fn read(source: &dyn OwnerRecords, collection: &str, meter: &mut Meter) -> Result<(Vec<ListedRecord>, Option<CutReason>), FetchError> {
    let mut out = Vec::new();
    let mut cursor: Option<String> = None;
    let (mut records, mut pages) = (0u64, 0u32);
    loop {
        if source.superseded().await {
            return Err(FetchError::Superseded);
        }
        let page = source.page(collection, cursor.as_deref()).await.map_err(FetchError::Failed)?;
        let got = u64::try_from(page.records.len()).unwrap_or(u64::MAX);
        records += got;
        pages += 1;
        meter.records += got;
        meter.pages += 1;
        meter.call_time += page.call_time;
        out.extend(page.records);
        // A page that ends the listing completes it, whatever it cost.
        let Some(next) = page.cursor.filter(|_| got > 0) else {
            return Ok((out, None));
        };
        if let Some(reason) = meter.cut(records, pages) {
            return Ok((out, Some(reason)));
        }
        cursor = Some(next);
    }
}

fn text<'a>(value: &'a Value, name: &str) -> Option<&'a str> {
    value.get(name).and_then(Value::as_str)
}

/// Steps 2a and 3: the owner's list records, then its list items, at
/// repository revision `rev`. A cut in the first listing ends the run's
/// reading there: no purposes, no items.
///
/// # Errors
///
/// [`FetchError`].
pub async fn fetch(source: &dyn OwnerRecords, rev: &str, bounds: Bounds) -> Result<Fetched, FetchError> {
    let mut meter = Meter {
        bounds,
        started: Instant::now(),
        records: 0,
        pages: 0,
        call_time: Duration::ZERO,
    };
    let mut listing = Listing {
        rev: rev.to_string(),
        ..Listing::default()
    };
    let (lists, cut) = read(source, NSID_LIST, &mut meter).await?;
    if cut.is_some() {
        return Ok(Fetched { listing, cut });
    }
    let purposes: HashMap<String, Option<String>> = lists
        .into_iter()
        .map(|record| {
            let purpose = text(&record.value, "purpose").map(str::to_string);
            (record.uri, purpose)
        })
        .collect();
    listing.lists = Some(purposes);
    let (items, cut) = read(source, NSID_LISTITEM, &mut meter).await?;
    listing.items = items
        .into_iter()
        .filter_map(|record| {
            // A list item names its list and its subject; one that does
            // not is not an item of anything.
            let list_uri = text(&record.value, "list")?.to_string();
            let subject_did = text(&record.value, "subject")?.to_string();
            let rkey = record.rkey().to_string();
            Some(ListedItem {
                uri: record.uri,
                rkey,
                cid: record.cid,
                list_uri,
                subject_did,
            })
        })
        .collect();
    listing.items_complete = cut.is_none();
    Ok(Fetched { listing, cut })
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use serde_json::json;

    use super::*;

    const OWNER: &str = "did:plc:aaaaaaaaaaaaaaaaaaaaaaaa";

    /// An owner with `lists` list records and `items` list items, served
    /// `per_page` at a time, each call taking `call_time`.
    struct Fake {
        lists: usize,
        items: usize,
        per_page: usize,
        call_time: Duration,
        fail_at: Option<usize>,
        calls: Mutex<usize>,
    }

    impl Fake {
        fn new(lists: usize, items: usize, per_page: usize) -> Self {
            Self {
                lists,
                items,
                per_page,
                call_time: Duration::from_millis(10),
                fail_at: None,
                calls: Mutex::new(0),
            }
        }
    }

    #[async_trait]
    impl OwnerRecords for Fake {
        async fn page(&self, collection: &str, cursor: Option<&str>) -> Result<Page, String> {
            let call = {
                let mut calls = self.calls.lock().expect("calls");
                *calls += 1;
                *calls
            };
            if self.fail_at == Some(call) {
                return Err("503".into());
            }
            let total = if collection == NSID_LIST { self.lists } else { self.items };
            let from: usize = cursor.and_then(|c| c.parse().ok()).unwrap_or(0);
            let to = (from + self.per_page).min(total);
            let records = (from..to)
                .map(|n| ListedRecord {
                    uri: format!("at://{OWNER}/{collection}/3k{n:04}"),
                    cid: format!("bafy{n}"),
                    value: if collection == NSID_LIST {
                        json!({"purpose": "app.bsky.graph.defs#modlist", "name": "L"})
                    } else {
                        json!({"list": format!("at://{OWNER}/{NSID_LIST}/3k0000"), "subject": format!("did:plc:s{n}")})
                    },
                })
                .collect();
            Ok(Page {
                records,
                cursor: (to < total).then(|| to.to_string()),
                call_time: self.call_time,
            })
        }
    }

    fn bounds() -> Bounds {
        Bounds {
            max_records: 1000,
            max_pages: 100,
            probe_call: Duration::from_secs(60),
            min_records_per_call_sec: 200,
            max_call: Duration::from_secs(900),
            max_wall: Duration::from_secs(3600),
        }
    }

    #[tokio::test]
    async fn both_listings_are_read_to_the_end() {
        let got = fetch(&Fake::new(3, 25, 10), "3krev", bounds()).await.expect("fetched");
        assert_eq!(got.cut, None);
        let lists = got.listing.lists.as_ref().expect("lists");
        assert_eq!(lists.len(), 3);
        assert_eq!(lists[&format!("at://{OWNER}/{NSID_LIST}/3k0000")].as_deref(), Some("app.bsky.graph.defs#modlist"));
        assert_eq!(got.listing.items.len(), 25);
        assert!(got.listing.items_complete && got.listing.rev == "3krev");
        assert_eq!((got.listing.items[0].rkey.as_str(), got.listing.items[0].subject_did.as_str()), ("3k0000", "did:plc:s0"));
        // An owner with nothing at all is a complete, empty listing.
        let empty = fetch(&Fake::new(0, 0, 10), "3krev", bounds()).await.expect("fetched");
        assert!(empty.cut.is_none() && empty.listing.items_complete && empty.listing.lists == Some(HashMap::new()));
    }

    #[tokio::test]
    async fn each_bound_cuts_the_listing() {
        // Records: the two listings are counted together.
        let records = Bounds { max_records: 12, ..bounds() };
        let got = fetch(&Fake::new(3, 25, 10), "r", records).await.expect("fetched");
        assert_eq!(got.cut, Some(CutReason::Records));
        // What was read is kept, as an incomplete listing; the list
        // records, read whole before the cut, still give the purposes.
        assert!(!got.listing.items_complete && got.listing.items.len() == 10 && got.listing.lists.is_some());
        // Pages.
        let pages = Bounds { max_pages: 2, ..bounds() };
        assert_eq!(fetch(&Fake::new(3, 25, 5), "r", pages).await.expect("fetched").cut, Some(CutReason::Pages));
        // Throughput: 10 records a call of one second, against 200 wanted,
        // once the probe time is spent.
        let slow = Fake { call_time: Duration::from_secs(1), ..Fake::new(3, 100, 10) };
        let probe = Bounds { probe_call: Duration::from_secs(3), ..bounds() };
        let got = fetch(&slow, "r", probe).await.expect("fetched");
        assert_eq!(got.cut, Some(CutReason::Throughput));
        assert!(got.cut.is_some_and(CutReason::slow) && !CutReason::Records.slow());
        // Time in calls, with throughput good enough.
        let fast = Fake { call_time: Duration::from_millis(40), ..Fake::new(3, 100, 10) };
        let time = Bounds { max_call: Duration::from_millis(100), ..bounds() };
        assert_eq!(fetch(&fast, "r", time).await.expect("fetched").cut, Some(CutReason::Time));
        // Wall time: full pages are a "wall" cut, tiny pages a "pages" cut.
        let wall = Bounds { max_wall: Duration::ZERO, ..bounds() };
        assert_eq!(fetch(&Fake::new(0, 500, 100), "r", wall).await.expect("fetched").cut, Some(CutReason::Wall));
        assert_eq!(fetch(&Fake::new(0, 500, 5), "r", wall).await.expect("fetched").cut, Some(CutReason::Pages));
        // A cut in the list-record listing: no purposes, and no item is
        // listed at all.
        let early = Bounds { max_pages: 1, ..bounds() };
        let fake = Fake::new(30, 25, 10);
        let got = fetch(&fake, "r", early).await.expect("fetched");
        assert!(got.cut == Some(CutReason::Pages) && got.listing.lists.is_none() && got.listing.items.is_empty());
        assert_eq!(*fake.calls.lock().expect("calls"), 2);
    }

    #[tokio::test]
    async fn a_failed_call_fails_the_fetch() {
        let failing = Fake { fail_at: Some(2), ..Fake::new(3, 25, 10) };
        assert_eq!(fetch(&failing, "r", bounds()).await, Err(FetchError::Failed("503".into())));
    }
}
