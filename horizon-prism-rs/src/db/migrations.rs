//! Embedded migrations and the startup gate (design-v2 §4.3).
//!
//! The gate compares the embedded migrations with `_sqlx_migrations` without
//! writing anything. Pending migrations are applied only when
//! `APP_ALLOW_STARTUP_MIGRATE=1`; otherwise, and on any checksum mismatch,
//! startup stops with exit code 78.

use sqlx::PgPool;
use sqlx::migrate::Migrator;

/// Every migration under `migrations/`, embedded at compile time.
pub static MIGRATOR: Migrator = sqlx::migrate!("./migrations");

/// Exit code for every migration-gate refusal (§4.3; `EX_CONFIG`).
pub const MIGRATION_GATE_EXIT_CODE: i32 = 78;

/// An embedded (up) migration.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnownMigration {
    pub version: i64,
    pub description: String,
    pub checksum: Vec<u8>,
}

impl KnownMigration {
    /// `0001_initial` style name for logs.
    #[must_use]
    pub fn name(&self) -> String {
        format!("{:04}_{}", self.version, self.description.replace(' ', "_"))
    }
}

/// A row of `_sqlx_migrations`.
#[derive(Debug, Clone, PartialEq, Eq, sqlx::FromRow)]
pub struct AppliedMigration {
    pub version: i64,
    pub checksum: Vec<u8>,
    pub success: bool,
}

/// The comparison of embedded against applied migrations.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct GatePlan {
    /// Embedded but not applied, in version order.
    pub pending: Vec<KnownMigration>,
    /// Applied with a checksum different from the embedded file.
    pub mismatched: Vec<i64>,
    /// Recorded as failed (`success = false`): a partially applied migration.
    pub dirty: Vec<i64>,
    /// Applied but not embedded: the database is newer than this binary.
    pub unknown: Vec<i64>,
}

impl GatePlan {
    /// True when some applied migration contradicts the embedded set.
    #[must_use]
    pub fn is_inconsistent(&self) -> bool {
        !(self.mismatched.is_empty() && self.dirty.is_empty() && self.unknown.is_empty())
    }
}

/// Outcome of a passed gate.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GateOutcome {
    UpToDate,
    /// Pending migrations were applied (flag set).
    Applied(Vec<String>),
}

/// A gate refusal or failure.
#[derive(Debug, thiserror::Error)]
pub enum GateError {
    #[error("migrations pending and APP_ALLOW_STARTUP_MIGRATE is not 1: {}", .0.join(", "))]
    Pending(Vec<String>),
    #[error(
        "applied migrations disagree with this binary (checksum mismatch {mismatched:?}, dirty {dirty:?}, unknown {unknown:?})"
    )]
    Inconsistent {
        mismatched: Vec<i64>,
        dirty: Vec<i64>,
        unknown: Vec<i64>,
    },
    #[error("applying migrations failed: {0}")]
    Apply(#[from] sqlx::migrate::MigrateError),
    #[error("reading migration state failed: {0}")]
    Database(#[from] sqlx::Error),
}

impl GateError {
    /// Process exit code. Refusals exit 78; a database that cannot be read is
    /// transient and exits 1 so the container restarts.
    #[must_use]
    pub fn exit_code(&self) -> i32 {
        match self {
            Self::Database(_) => 1,
            Self::Pending(_) | Self::Inconsistent { .. } | Self::Apply(_) => {
                MIGRATION_GATE_EXIT_CODE
            }
        }
    }
}

/// The embedded up migrations, in version order.
#[must_use]
pub fn known_migrations(migrator: &Migrator) -> Vec<KnownMigration> {
    let mut known: Vec<KnownMigration> = migrator
        .iter()
        .filter(|m| !m.migration_type.is_down_migration())
        .map(|m| KnownMigration {
            version: m.version,
            description: m.description.to_string(),
            checksum: m.checksum.to_vec(),
        })
        .collect();
    known.sort_by_key(|m| m.version);
    known
}

/// Compare embedded and applied migrations. Pure; see [`GatePlan`].
#[must_use]
pub fn plan(known: &[KnownMigration], applied: &[AppliedMigration]) -> GatePlan {
    let mut out = GatePlan::default();
    for k in known {
        match applied.iter().find(|a| a.version == k.version) {
            None => out.pending.push(k.clone()),
            Some(a) if !a.success => out.dirty.push(a.version),
            Some(a) if a.checksum != k.checksum => out.mismatched.push(a.version),
            Some(_) => {}
        }
    }
    for a in applied {
        if !known.iter().any(|k| k.version == a.version) {
            if a.success {
                out.unknown.push(a.version);
            } else {
                out.dirty.push(a.version);
            }
        }
    }
    out.dirty.sort_unstable();
    out
}

/// Read `_sqlx_migrations` without creating it. An absent table means nothing
/// has been applied.
///
/// # Errors
///
/// Any database error.
pub async fn applied_migrations(pool: &PgPool) -> Result<Vec<AppliedMigration>, sqlx::Error> {
    let table: Option<String> =
        sqlx::query_scalar("SELECT to_regclass('_sqlx_migrations')::text")
            .fetch_one(pool)
            .await?;
    if table.is_none() {
        return Ok(Vec::new());
    }
    sqlx::query_as::<_, AppliedMigration>(
        "SELECT version, checksum, success FROM _sqlx_migrations ORDER BY version",
    )
    .fetch_all(pool)
    .await
}

/// The current gate plan (used by `migrate status`, read-only).
///
/// # Errors
///
/// Any database error.
pub async fn status(pool: &PgPool) -> Result<GatePlan, sqlx::Error> {
    let applied = applied_migrations(pool).await?;
    Ok(plan(&known_migrations(&MIGRATOR), &applied))
}

/// The §4.3 startup gate.
///
/// Logs `migrations_pending` (and refuses) when migrations are pending and
/// `allow_apply` is false; logs `migrations_applied` after applying them when
/// it is true. Any checksum mismatch, dirty or unknown applied migration is
/// always refused.
///
/// # Errors
///
/// [`GateError`]; the caller exits with [`GateError::exit_code`].
pub async fn enforce_gate(pool: &PgPool, allow_apply: bool) -> Result<GateOutcome, GateError> {
    let plan = status(pool).await?;
    if plan.is_inconsistent() {
        tracing::error!(
            event = "migrations_inconsistent",
            mismatched = ?plan.mismatched,
            dirty = ?plan.dirty,
            unknown = ?plan.unknown,
            "applied migrations disagree with the embedded set; refusing to start",
        );
        return Err(GateError::Inconsistent {
            mismatched: plan.mismatched,
            dirty: plan.dirty,
            unknown: plan.unknown,
        });
    }
    if plan.pending.is_empty() {
        return Ok(GateOutcome::UpToDate);
    }
    let names: Vec<String> = plan.pending.iter().map(KnownMigration::name).collect();
    if !allow_apply {
        tracing::error!(
            event = "migrations_pending",
            pending = ?names,
            count = names.len(),
            "migrations are pending and APP_ALLOW_STARTUP_MIGRATE is not 1; refusing to start",
        );
        return Err(GateError::Pending(names));
    }
    MIGRATOR.run(pool).await?;
    tracing::info!(
        event = "migrations_applied",
        applied = ?names,
        count = names.len(),
        "pending migrations applied",
    );
    Ok(GateOutcome::Applied(names))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn known(version: i64, checksum: u8) -> KnownMigration {
        KnownMigration {
            version,
            description: format!("m {version}"),
            checksum: vec![checksum; 4],
        }
    }

    fn applied(version: i64, checksum: u8, success: bool) -> AppliedMigration {
        AppliedMigration {
            version,
            checksum: vec![checksum; 4],
            success,
        }
    }

    #[test]
    fn empty_database_has_everything_pending() {
        let p = plan(&[known(1, 1), known(2, 2)], &[]);
        assert_eq!(p.pending.iter().map(|m| m.version).collect::<Vec<_>>(), [1, 2]);
        assert!(!p.is_inconsistent());
    }

    #[test]
    fn fully_applied_is_clean() {
        let p = plan(&[known(1, 1)], &[applied(1, 1, true)]);
        assert_eq!(p, GatePlan::default());
    }

    #[test]
    fn checksum_mismatch_is_inconsistent() {
        let p = plan(&[known(1, 1), known(2, 2)], &[applied(1, 9, true)]);
        assert_eq!(p.mismatched, [1]);
        assert_eq!(p.pending.len(), 1);
        assert!(p.is_inconsistent());
    }

    #[test]
    fn failed_migration_is_dirty() {
        let p = plan(&[known(1, 1)], &[applied(1, 1, false)]);
        assert_eq!(p.dirty, [1]);
        assert!(p.is_inconsistent());
    }

    #[test]
    fn database_newer_than_binary_is_inconsistent() {
        let p = plan(&[known(1, 1)], &[applied(1, 1, true), applied(2, 2, true)]);
        assert_eq!(p.unknown, [2]);
        assert!(p.is_inconsistent());
    }

    #[test]
    fn embedded_migrations_include_0001() {
        let all = known_migrations(&MIGRATOR);
        let first = all.first().expect("at least one migration");
        assert_eq!(first.version, 1);
        assert_eq!(first.name(), "0001_initial");
        let names: Vec<String> = all.iter().map(|m| m.name()).collect();
        assert_eq!(names[1], "0002_private_reply_cids");
    }

    #[test]
    fn refusals_exit_78() {
        assert_eq!(GateError::Pending(vec!["0001_initial".into()]).exit_code(), 78);
        let inconsistent = GateError::Inconsistent {
            mismatched: vec![1],
            dirty: vec![],
            unknown: vec![],
        };
        assert_eq!(inconsistent.exit_code(), 78);
        assert_eq!(GateError::Database(sqlx::Error::PoolTimedOut).exit_code(), 1);
    }
}
