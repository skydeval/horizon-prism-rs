//! `admin identity compare [--sample N] [DID|HANDLE ...]` (lodestar-reads
//! design §6.2): for each DID, its resolution as with `identity.lodestar_url`
//! empty (the PLC directory and this app's own handle check) and as with it
//! set (lodestar first, with the same fallbacks), side by side.
//!
//! Read-only: it reads `actors` to pick a sample, writes nothing, and keeps
//! nothing in any cache. It asks lodestar whatever the gate would say, and
//! shows what the gate would have done. It prints DIDs, handles and PDS
//! endpoints, which are public, and nothing else.

use std::sync::Arc;
use std::time::{Duration, Instant};

use serde_json::Value;
use sqlx::PgPool;

use super::table;
use crate::config::AppConfig;
use crate::identity::lodestar::{DocAnswer, Lodestar};
use crate::identity::resolver::{DidTarget, extract_info, parse_did};
use crate::identity::{IdentityError, IdentityResolver, IdentitySettings, PersistencePolicy};
use crate::net::{Intent, NetConfig, Transports};

/// `did:web` DIDs in a sample: one for every this many `did:plc`.
const WEB_SHARE: u32 = 10;
/// Accounts marked deleted in a sample, at most.
const DELETED_MAX: i64 = 5;

struct NothingPersists;

impl PersistencePolicy for NothingPersists {
    fn in_persistence_set(&self, _did: &str) -> bool {
        false
    }
}

/// One side's resolution of a DID.
#[derive(Debug, Clone, PartialEq)]
struct Side {
    /// The document, normalized for comparison; `None` when there is none.
    doc: Option<Value>,
    /// What answered: `plc`, `web`, `lodestar`, `fallback`, `tombstoned`,
    /// `not_found`, `error`; on the lodestar side, a failure after a
    /// fallback is `fallback:<what the directory said>`.
    source: String,
    pds: Option<String>,
    handle: Option<String>,
    /// How the handle was decided: `lodestar`, `probe`, `-`.
    handle_by: &'static str,
    took: Duration,
}

/// A document with its unordered parts in a fixed order: lodestar lists
/// methods and services by name, the directory in its operation's order.
fn normalized(doc: &Value) -> Value {
    let mut doc = doc.clone();
    for key in ["verificationMethod", "service"] {
        if let Some(items) = doc.get_mut(key).and_then(Value::as_array_mut) {
            items.sort_by_key(|item| item.get("id").and_then(Value::as_str).unwrap_or_default().to_string());
        }
    }
    if let Some(context) = doc.get_mut("@context").and_then(Value::as_array_mut) {
        context.sort_by_key(|c| c.as_str().unwrap_or_default().to_string());
    }
    doc
}

fn error_source(error: &IdentityError) -> String {
    match error {
        IdentityError::Tombstoned(_) => "tombstoned",
        IdentityError::NotFound(_) | IdentityError::Gone(_) => "not_found",
        _ => "error",
    }
    .to_string()
}

/// As the setting off: the PLC directory (or the did:web host) and the
/// probe.
async fn off(resolver: &IdentityResolver, did: &str) -> Side {
    let started = Instant::now();
    let web = matches!(parse_did(did), Ok(DidTarget::Web { .. }));
    let (doc, source) = match resolver.fetch_document(did).await {
        Ok(fetched) => (Some((*fetched.doc).clone()), if web { "web" } else { "plc" }.to_string()),
        Err(error) => (None, error_source(&error)),
    };
    let info = doc.as_ref().map(|d| extract_info(d, did));
    let handle = match &info {
        Some(info) => resolver.verify_handle(did, info.claimed_handle.as_deref()).await,
        None => None,
    };
    Side {
        doc: doc.as_ref().map(normalized),
        source,
        pds: info.and_then(|i| i.pds_endpoint),
        handle,
        handle_by: if doc.is_some() { "probe" } else { "-" },
        took: started.elapsed(),
    }
}

/// As the setting on: lodestar's document and verified handle, with the
/// same fallbacks the request path takes; the gate is not consulted.
async fn on(lodestar: &Lodestar, resolver: &IdentityResolver, did: &str, off_side: &Side) -> Side {
    let started = Instant::now();
    if !did.starts_with("did:plc:") {
        // did:web is never asked (§2.8).
        return Side { source: "not_asked".to_string(), ..off_side.clone() };
    }
    let (doc, source) = match lodestar.document(did, None).await {
        DocAnswer::Doc(fetched) => (Some((*fetched.doc).clone()), "lodestar".to_string()),
        DocAnswer::Tombstoned => (None, "tombstoned".to_string()),
        DocAnswer::Fallback => match resolver.fetch_document(did).await {
            Ok(fetched) => (Some((*fetched.doc).clone()), "fallback".to_string()),
            Err(error) => (None, format!("fallback:{}", error_source(&error))),
        },
    };
    let info = doc.as_ref().map(|d| extract_info(d, did));
    let claimed = info.as_ref().and_then(|i| i.claimed_handle.clone());
    let (handle, handle_by) = match &claimed {
        None => (None, "-"),
        Some(claimed) => {
            let verified = lodestar.account(did, None).await.and_then(|a| a.handle).filter(|h| h.eq_ignore_ascii_case(claimed));
            match verified {
                Some(_) => (crate::identity::verify_bidirectional(did, Some(claimed), Some(did)), "lodestar"),
                None => (resolver.verify_handle(did, Some(claimed)).await, "probe"),
            }
        }
    };
    Side {
        doc: doc.as_ref().map(normalized),
        source,
        pds: info.and_then(|i| i.pds_endpoint),
        handle,
        handle_by,
        took: started.elapsed(),
    }
}

fn same<T: PartialEq>(a: &T, b: &T) -> &'static str {
    if a == b { "same" } else { "DIFF" }
}

fn ms(d: Duration) -> String {
    format!("{}", d.as_millis())
}

/// The median and 90th percentile of `times`, in milliseconds.
fn spread(mut times: Vec<Duration>) -> String {
    if times.is_empty() {
        return "-".to_string();
    }
    times.sort();
    let at = |q: f64| {
        let i = ((times.len() - 1) as f64 * q).round() as usize;
        times[i.min(times.len() - 1)].as_millis()
    };
    format!("median {} ms, p90 {} ms", at(0.5), at(0.9))
}

/// The comparison of `dids` and `handles`, as printed.
async fn compare(lodestar: &Lodestar, resolver: &IdentityResolver, mut dids: Vec<String>, handles: &[String]) -> String {
    let mut out = String::new();
    let open = lodestar.probe().await;
    out.push_str(&format!(
        "lodestar probe: {}\n\n",
        if open { "at the directory's tip (the gate would be open)" } else { "not at the tip, or not reachable (the gate would be closed)" }
    ));

    // Handles first: each side's DID, then that DID joins the list.
    if !handles.is_empty() {
        let mut rows = Vec::new();
        for handle in handles {
            let started = Instant::now();
            let off_did = resolver.resolve_handle(handle).await;
            let off_took = started.elapsed();
            let started = Instant::now();
            let on_did = match lodestar.handle(handle, None).await {
                Some(did) => {
                    let claims = match resolver.fetch_document(&did).await {
                        Ok(fetched) => extract_info(&fetched.doc, &did).claimed_handle.is_some_and(|c| c.eq_ignore_ascii_case(handle)),
                        Err(_) => false,
                    };
                    if claims { Some((did, "lodestar")) } else { off_did.clone().map(|d| (d, "probe")) }
                }
                None => off_did.clone().map(|d| (d, "probe")),
            };
            let on_took = started.elapsed();
            let (on_value, by) = on_did.clone().map_or(("-".to_string(), "probe"), |(d, by)| (d, by));
            rows.push(vec![
                handle.clone(),
                off_did.clone().unwrap_or_else(|| "-".to_string()),
                on_value,
                by.to_string(),
                same(&off_did, &on_did.map(|(d, _)| d)).to_string(),
                ms(off_took),
                ms(on_took),
            ]);
            if let Some(did) = off_did
                && !dids.contains(&did)
            {
                dids.push(did);
            }
        }
        out.push_str(&table(&["handle", "off_did", "on_did", "on_by", "did", "off_ms", "on_ms"], &rows));
        out.push_str("\n\n");
    }

    let mut rows = Vec::new();
    let (mut same_all, mut different, mut fell_back) = (0, 0, 0);
    let (mut off_times, mut on_times) = (Vec::new(), Vec::new());
    for did in &dids {
        let gate = if lodestar.gate().is_open() { "open" } else { "closed" };
        let off_side = off(resolver, did).await;
        let on_side = on(lodestar, resolver, did, &off_side).await;
        let doc = same(&off_side.doc, &on_side.doc);
        let pds = same(&off_side.pds, &on_side.pds);
        let handle = same(&off_side.handle, &on_side.handle);
        if [doc, pds, handle].iter().all(|s| *s == "same") {
            same_all += 1;
        } else {
            different += 1;
        }
        if on_side.source.starts_with("fallback") {
            fell_back += 1;
        }
        off_times.push(off_side.took);
        on_times.push(on_side.took);
        rows.push(vec![
            did.clone(),
            off_side.source.clone(),
            on_side.source.clone(),
            doc.to_string(),
            pds.to_string(),
            handle.to_string(),
            on_side.handle_by.to_string(),
            on_side.handle.clone().unwrap_or_else(|| "-".to_string()),
            gate.to_string(),
            ms(off_side.took),
            ms(on_side.took),
        ]);
    }
    out.push_str(&table(&["did", "off", "on", "doc", "pds", "handle", "on_handle_by", "handle_on", "gate", "off_ms", "on_ms"], &rows));
    out.push_str(&format!(
        "\n\n{} DIDs: {same_all} the same, {different} different, {fell_back} where lodestar could not answer.\n\
         off (PLC directory and own checks): {}\n\
         on (lodestar first): {}",
        dids.len(),
        spread(off_times),
        spread(on_times)
    ));
    out
}

/// A sample of DIDs from `actors`: `n` random `did:plc`, one `did:web` per
/// [`WEB_SHARE`], and a few marked deleted.
async fn sample(pool: &PgPool, n: u32) -> Result<Vec<String>, sqlx::Error> {
    let mut dids: Vec<String> = sqlx::query_scalar("SELECT did FROM actors WHERE did LIKE 'did:plc:%' ORDER BY random() LIMIT $1")
        .bind(i64::from(n))
        .fetch_all(pool)
        .await?;
    let web: Vec<String> = sqlx::query_scalar("SELECT did FROM actors WHERE did LIKE 'did:web:%' ORDER BY random() LIMIT $1")
        .bind(i64::from(n.div_ceil(WEB_SHARE)))
        .fetch_all(pool)
        .await?;
    let deleted: Vec<String> =
        sqlx::query_scalar("SELECT did FROM actors WHERE account_status = 'deleted' AND did LIKE 'did:plc:%' ORDER BY random() LIMIT $1")
            .bind(DELETED_MAX)
            .fetch_all(pool)
            .await?;
    for did in web.into_iter().chain(deleted) {
        if !dids.contains(&did) {
            dids.push(did);
        }
    }
    Ok(dids)
}

/// Run the comparison with the deployed configuration.
///
/// # Errors
///
/// A printable message: no `identity.lodestar_url`, no input, a client or
/// database failure.
pub async fn render(cfg: &AppConfig, sample_size: u32, inputs: &[String]) -> Result<String, String> {
    let base = cfg
        .file
        .identity
        .lodestar()
        .ok_or_else(|| "identity.lodestar_url is empty: set it in the config this command reads".to_string())?;
    if sample_size == 0 && inputs.is_empty() {
        return Err("name DIDs or handles, or pass --sample N".to_string());
    }
    let mut dids = Vec::new();
    let mut handles = Vec::new();
    for input in inputs {
        let input = input.trim().trim_start_matches('@');
        if input.starts_with("did:") {
            parse_did(input).map_err(|e| e.to_string())?;
            dids.push(input.to_string());
        } else {
            handles.push(input.to_ascii_lowercase());
        }
    }
    let pool = super::pool(cfg).await?;
    if sample_size > 0 {
        for did in sample(&pool, sample_size).await.map_err(|e| format!("sampling actors: {e}"))? {
            if !dids.contains(&did) {
                dids.push(did);
            }
        }
    }
    let nets = Transports::new(&NetConfig::from_app(cfg), &cfg.file.net).map_err(|e| format!("net client: {e}"))?;
    let identity = &cfg.file.identity;
    let lodestar = Arc::new(Lodestar::new(
        nets.public().client().clone(),
        base,
        Duration::from_millis(identity.lodestar_timeout_ms),
        Duration::from_secs(identity.did_fresh_secs),
    ));
    let resolver = IdentityResolver::new(nets, pool, IdentitySettings::from_app(cfg), Arc::new(NothingPersists), Intent::Public);
    Ok(compare(&lodestar, &resolver, dids, &handles).await)
}

#[cfg(test)]
mod tests {
    use axum::extract::Path;
    use axum::http::StatusCode;
    use axum::routing::get;
    use serde_json::json;

    use super::*;

    const SAME: &str = "did:plc:ssssssssssssssssssssssss";
    const MOVED: &str = "did:plc:mmmmmmmmmmmmmmmmmmmmmmmm";

    fn doc(did: &str, pds: &str, reversed: bool) -> Value {
        let mut services = vec![
            json!({"id": "#atproto_pds", "type": "AtprotoPersonalDataServer", "serviceEndpoint": pds}),
            json!({"id": "#atproto_labeler", "type": "AtprotoLabeler", "serviceEndpoint": "https://mod.example.com"}),
        ];
        if reversed {
            services.reverse();
        }
        json!({"id": did, "alsoKnownAs": ["at://nobody.invalid"], "service": services})
    }

    /// Two lines: one the same apart from service order, one whose PDS
    /// lodestar names differently; a summary counts them.
    #[tokio::test]
    async fn the_comparison_tells_same_from_different() {
        let Some(db) = crate::test_support::fresh_db("identity_compare").await else {
            return;
        };
        async fn lodestar(Path(did): Path<String>) -> (StatusCode, String) {
            let pds = if did == MOVED { "https://old.example.com" } else { "https://pds.example.com" };
            (StatusCode::OK, doc(&did, pds, true).to_string())
        }
        async fn account(Path(_did): Path<String>) -> (StatusCode, String) {
            (StatusCode::NOT_FOUND, json!({"error": "not_found", "source": {"plc": {"phase": "live", "fresh": true}}}).to_string())
        }
        async fn plc(Path(did): Path<String>) -> axum::Json<Value> {
            axum::Json(doc(&did, "https://pds.example.com", false))
        }
        let l_base = crate::test_support::serve_fake(
            axum::Router::new().route("/account/{did}", get(account)).route("/{did}", get(lodestar)),
        )
        .await;
        let plc_base = crate::test_support::serve_fake(axum::Router::new().route("/{did}", get(plc))).await;
        let nets = Transports::loopback_for_tests();
        let l = Lodestar::new(nets.public().client().clone(), &l_base, Duration::from_secs(1), Duration::from_secs(60));
        let settings = IdentitySettings { plc_url: plc_base, fresh: Duration::from_secs(3600), stale_max: Duration::from_secs(86_400) };
        let resolver = IdentityResolver::new(nets, db.pool.clone(), settings, Arc::new(NothingPersists), Intent::Public);
        let out = compare(&l, &resolver, vec![SAME.to_string(), MOVED.to_string()], &[]).await;
        assert!(out.contains("at the directory's tip"), "{out}");
        let line = |did: &str| out.lines().find(|l| l.starts_with(did)).unwrap_or_default().to_string();
        assert!(line(SAME).contains("lodestar  same  same  same"), "{out}");
        assert!(line(MOVED).contains("lodestar  DIFF  DIFF  same"), "{out}");
        assert!(out.contains("2 DIDs: 1 the same, 1 different, 0 where lodestar could not answer."), "{out}");
        db.drop_db().await;
    }

    #[test]
    fn spreads() {
        assert_eq!(spread(Vec::new()), "-");
        let times = (1..=10).map(Duration::from_millis).collect();
        assert_eq!(spread(times), "median 6 ms, p90 9 ms");
    }
}
