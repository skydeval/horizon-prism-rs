//! Operators and allowed hosts (admin-ui design §5.1, §5.7).
//!
//! Operators and hosts live in `admin_operators` and
//! `admin_operator_hosts`, written only by first-run setup and the
//! operator commands (C-A2). A host pattern is a lowercase host or `*.`
//! plus one, matched with [`host_matches`]. An empty host list allows any
//! host (C-A3); the operator list still decides who may sign in.

use async_trait::async_trait;
use serde_json::{Value, json};
use sqlx::{PgConnection, PgPool};

use crate::constellation::matcher::{host_matches, pds_host};
use crate::identity::{DidDocInfo, IdentityResolver};

/// The identity work the admin UI and the operator commands need: handles
/// to DIDs, and fresh DID resolutions (§5.4). Every call goes through the
/// identity resolver's transport, so the network guard applies.
#[async_trait]
pub trait OperatorIdentity: Send + Sync {
    /// The DID a handle names, verified both ways, or `None`.
    async fn resolve_handle(&self, handle: &str) -> Option<String>;
    /// The DID's document, fetched past every cache.
    async fn resolve_fresh(&self, did: &str) -> Result<DidDocInfo, String>;
}

#[async_trait]
impl OperatorIdentity for IdentityResolver {
    async fn resolve_handle(&self, handle: &str) -> Option<String> {
        Self::resolve_handle(self, handle).await
    }

    async fn resolve_fresh(&self, did: &str) -> Result<DidDocInfo, String> {
        Self::resolve_fresh(self, did).await.map_err(|e| e.to_string())
    }
}

/// A PDS endpoint in the one form sessions compare (§5.3): scheme,
/// lowercased host, a port only when not the scheme's own, the path
/// without a trailing slash. `None` for something that is not a URL with
/// a host.
#[must_use]
pub fn normalize_endpoint(endpoint: &str) -> Option<String> {
    let url = url::Url::parse(endpoint.trim()).ok()?;
    let host = url.host_str()?.to_ascii_lowercase();
    let port = url.port().map(|p| format!(":{p}")).unwrap_or_default();
    Some(format!("{}://{host}{port}{}", url.scheme(), url.path().trim_end_matches('/')))
}

/// The host of a normalized endpoint, as patterns see it.
#[must_use]
pub fn endpoint_host(endpoint: &str) -> Option<String> {
    pds_host(endpoint)
}

/// Whether `host` may sign in under `patterns`: any host when there are
/// none (C-A3), otherwise one matching pattern.
#[must_use]
pub fn host_allowed(patterns: &[String], host: &str) -> bool {
    patterns.is_empty() || patterns.iter().any(|p| host_matches(host, p))
}

/// A pattern in its stored form, or why it is not one.
///
/// # Errors
///
/// A plain-words problem.
pub fn parse_pattern(pattern: &str) -> Result<String, String> {
    let lower = pattern.trim().to_ascii_lowercase();
    crate::config::pds_pattern(&lower).map_err(|e| format!("not a server name or *.pattern: {e}"))?;
    Ok(lower)
}

/// Every allowed-host pattern.
///
/// # Errors
///
/// Database errors.
pub async fn patterns(conn: &mut PgConnection) -> Result<Vec<String>, sqlx::Error> {
    sqlx::query_scalar("SELECT pattern FROM admin_operator_hosts ORDER BY pattern").fetch_all(conn).await
}

/// Whether `did` is an operator.
///
/// # Errors
///
/// Database errors.
pub async fn is_operator(pool: &PgPool, did: &str) -> Result<bool, sqlx::Error> {
    sqlx::query_scalar("SELECT EXISTS (SELECT 1 FROM admin_operators WHERE did = $1)").bind(did).fetch_one(pool).await
}

/// Who changed something: the CLI, or an operator in the UI (§5.8).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Actor {
    Cli,
    Operator(String),
}

impl Actor {
    /// The value written to `*_by` columns: `cli` or the operator's DID.
    #[must_use]
    pub fn label(&self) -> &str {
        match self {
            Self::Cli => "cli",
            Self::Operator(did) => did,
        }
    }

    /// From `admin_commands.requested_by`.
    #[must_use]
    pub fn from_requested_by(requested_by: Option<String>) -> Self {
        requested_by.map_or(Self::Cli, Self::Operator)
    }
}

/// An `operator add` whose account was resolved (§5.7).
#[derive(Debug, Clone)]
pub struct Resolved {
    pub did: String,
    /// Normalized.
    pub endpoint: String,
    pub host: String,
}

/// `operator add`'s transaction (§5.7): the operator, its exact host when
/// `add_host` and no pattern covers it, and with no operator before it the
/// setup code's row, all at once. Returns the result and whether setup was
/// closed (the caller then deletes the code's file). With `add_host` off,
/// a host the list does not allow fails the command.
///
/// # Errors
///
/// A plain-words failure (`Err(Ok)`) or a database error (`Err(Err)`).
pub async fn add(
    pool: &PgPool,
    who: &Resolved,
    note: Option<&str>,
    add_host: bool,
    actor: &Actor,
) -> Result<(Value, bool), Result<String, sqlx::Error>> {
    let mut tx = pool.begin().await.map_err(Err)?;
    // One operator change at a time (the setup callback included).
    sqlx::query("LOCK TABLE admin_operators IN SHARE ROW EXCLUSIVE MODE").execute(&mut *tx).await.map_err(Err)?;
    let patterns = patterns(&mut tx).await.map_err(Err)?;
    let covered = patterns.iter().any(|p| host_matches(&who.host, p));
    let host_added = if add_host && !covered {
        sqlx::query("INSERT INTO admin_operator_hosts (pattern, added_by) VALUES ($1, $2) ON CONFLICT DO NOTHING")
            .bind(&who.host)
            .bind(actor.label())
            .execute(&mut *tx)
            .await
            .map_err(Err)?;
        true
    } else {
        if !host_allowed(&patterns, &who.host) {
            return Err(Ok(format!(
                "{} is hosted on {}, which is not on the allowed list; allow that server first",
                who.did, who.host
            )));
        }
        false
    };
    let was_empty: bool = sqlx::query_scalar("SELECT NOT EXISTS (SELECT 1 FROM admin_operators)")
        .fetch_one(&mut *tx)
        .await
        .map_err(Err)?;
    let inserted: bool = sqlx::query_scalar(
        "INSERT INTO admin_operators (did, added_by, note, pds_endpoint, pds_checked_at) VALUES ($1, $2, $3, $4, now()) \
         ON CONFLICT (did) DO UPDATE SET pds_endpoint = EXCLUDED.pds_endpoint, pds_checked_at = now(), \
           note = COALESCE(EXCLUDED.note, admin_operators.note) \
         RETURNING (xmax = 0)",
    )
    .bind(&who.did)
    .bind(actor.label())
    .bind(note)
    .bind(&who.endpoint)
    .fetch_one(&mut *tx)
    .await
    .map_err(Err)?;
    if was_empty {
        sqlx::query("DELETE FROM admin_setup_code").execute(&mut *tx).await.map_err(Err)?;
    }
    tx.commit().await.map_err(Err)?;
    let status = if inserted { "added" } else { "already_operator" };
    Ok((
        json!({ "did": who.did, "status": status, "host": who.host, "host_added": host_added, "setup_closed": was_empty }),
        was_empty,
    ))
}

/// `operator remove` (§5.7): never the last operator. The removed
/// operator's sessions end with it.
///
/// # Errors
///
/// As [`add`].
pub async fn remove(pool: &PgPool, did: &str) -> Result<Value, Result<String, sqlx::Error>> {
    let mut tx = pool.begin().await.map_err(Err)?;
    sqlx::query("LOCK TABLE admin_operators IN SHARE ROW EXCLUSIVE MODE").execute(&mut *tx).await.map_err(Err)?;
    let dids: Vec<String> = sqlx::query_scalar("SELECT did FROM admin_operators").fetch_all(&mut *tx).await.map_err(Err)?;
    if !dids.iter().any(|d| d == did) {
        return Err(Ok(format!("{did} is not an operator")));
    }
    if dids.len() == 1 {
        return Err(Ok("this is the last operator; add another operator first".to_string()));
    }
    sqlx::query("DELETE FROM admin_operators WHERE did = $1").bind(did).execute(&mut *tx).await.map_err(Err)?;
    let sessions = sqlx::query("DELETE FROM admin_sessions WHERE did = $1").bind(did).execute(&mut *tx).await.map_err(Err)?.rows_affected();
    tx.commit().await.map_err(Err)?;
    Ok(json!({ "did": did, "status": "removed", "sessions_ended": sessions }))
}

/// `operator host add`.
///
/// # Errors
///
/// Database errors.
pub async fn host_add(pool: &PgPool, pattern: &str, actor: &Actor) -> Result<Value, sqlx::Error> {
    let n = sqlx::query("INSERT INTO admin_operator_hosts (pattern, added_by) VALUES ($1, $2) ON CONFLICT DO NOTHING")
        .bind(pattern)
        .bind(actor.label())
        .execute(pool)
        .await?
        .rows_affected();
    Ok(json!({ "pattern": pattern, "added": n > 0 }))
}

/// `operator host remove` (§5.7): refused when the hosts left would allow
/// no operator's last known host. Reads only stored endpoints, never the
/// network. Removing the last pattern leaves the list empty, which allows
/// any host (C-A3).
///
/// # Errors
///
/// As [`add`].
pub async fn host_remove(pool: &PgPool, pattern: &str) -> Result<Value, Result<String, sqlx::Error>> {
    let mut tx = pool.begin().await.map_err(Err)?;
    sqlx::query("LOCK TABLE admin_operators IN SHARE ROW EXCLUSIVE MODE").execute(&mut *tx).await.map_err(Err)?;
    let patterns = patterns(&mut tx).await.map_err(Err)?;
    if !patterns.iter().any(|p| p == pattern) {
        return Err(Ok(format!("{pattern} is not on the allowed list")));
    }
    let left: Vec<String> = patterns.into_iter().filter(|p| p != pattern).collect();
    let endpoints: Vec<Option<String>> =
        sqlx::query_scalar("SELECT pds_endpoint FROM admin_operators").fetch_all(&mut *tx).await.map_err(Err)?;
    let someone_left = endpoints
        .iter()
        .flatten()
        .filter_map(|e| endpoint_host(e))
        .any(|host| host_allowed(&left, &host));
    if !left.is_empty() && !someone_left {
        return Err(Ok(format!(
            "removing {pattern} would leave no operator able to sign in; allow another operator's server first"
        )));
    }
    sqlx::query("DELETE FROM admin_operator_hosts WHERE pattern = $1").bind(pattern).execute(&mut *tx).await.map_err(Err)?;
    tx.commit().await.map_err(Err)?;
    Ok(json!({ "pattern": pattern, "removed": true, "any_host": left.is_empty() }))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// T-A5: `*.example.com` matches one or more labels before the suffix,
    /// never the bare suffix or a look-alike; matching ignores case and the
    /// endpoint's port.
    #[test]
    fn patterns_match_as_the_design_says() {
        let p = vec!["*.example.com".to_string()];
        for host in ["a.example.com", "a.b.example.com"] {
            assert!(host_allowed(&p, host), "{host}");
        }
        for host in ["example.com", "evil-example.com", "example.com.evil.net"] {
            assert!(!host_allowed(&p, host), "{host}");
        }
        assert!(host_allowed(&p, "A.Example.COM"));
        let host = endpoint_host(&normalize_endpoint("https://A.Example.com:8443/").expect("url")).expect("host");
        assert_eq!(host, "a.example.com");
        assert!(host_allowed(&p, &host));
        // An empty list allows any host (C-A3).
        assert!(host_allowed(&[], "anything.example"));
        // Exact patterns are exact.
        let exact = vec!["pds.example.org".to_string()];
        assert!(host_allowed(&exact, "pds.example.org"));
        assert!(!host_allowed(&exact, "x.pds.example.org"));
    }

    #[test]
    fn endpoints_normalize_so_cosmetic_changes_are_no_change() {
        let n = |e: &str| normalize_endpoint(e).expect("url");
        assert_eq!(n("https://PDS.example.com/"), "https://pds.example.com");
        assert_eq!(n("https://pds.example.com:443"), "https://pds.example.com");
        assert_eq!(n("https://pds.example.com:8443"), "https://pds.example.com:8443");
        assert_ne!(n("https://pds.example.com"), n("https://other.example.com"));
        assert_eq!(normalize_endpoint("not a url"), None);
    }

    #[test]
    fn patterns_parse_lowercased_and_checked() {
        assert_eq!(parse_pattern(" *.Host.Bsky.Network ").as_deref(), Ok("*.host.bsky.network"));
        assert_eq!(parse_pattern("pds.example.org").as_deref(), Ok("pds.example.org"));
        for bad in ["https://pds.example", "*.", "pds.example:443", "*.*.example", "localhost", ""] {
            assert!(parse_pattern(bad).is_err(), "{bad}");
        }
    }
}
