//! Environment overlay: the `APP_*` variables of design-v2 §11.3.
//!
//! The app reads these variables and nothing else from the environment. They
//! carry secrets and deployment-specific values; every other setting lives in
//! `app.toml`. Reads go through a lookup function so tests never mutate the
//! process environment.

use std::fmt;
use std::path::PathBuf;

use super::ConfigError;

pub const ENV_PUBLIC_HOSTNAME: &str = "APP_PUBLIC_HOSTNAME";
pub const ENV_DATABASE_URL: &str = "APP_DATABASE_URL";
pub const ENV_REDIS_URL: &str = "APP_REDIS_URL";
pub const ENV_CONFIG_PATH: &str = "APP_CONFIG_PATH";
pub const ENV_DATA_DIR: &str = "APP_DATA_DIR";
pub const ENV_SERVICE_SIGNING_KEY_HEX: &str = "APP_SERVICE_SIGNING_KEY_HEX";
pub const ENV_CORRELATION_KEY_HEX: &str = "APP_CORRELATION_KEY_HEX";
/// The key Thread Lens seals its paging cursors with (design-v3 §5.15.14).
/// Optional: without it a key is made at each start.
pub const ENV_THREADLENS_CURSOR_KEY_HEX: &str = "APP_THREADLENS_CURSOR_KEY_HEX";
/// The key the private tier seals its paging cursors with (private posting
/// §3.6, C-14). Required: a generated key would break every private cursor
/// at each restart and between replicas, so it is never made at start.
pub const ENV_PRIVATE_CURSOR_KEY_HEX: &str = "APP_PRIVATE_CURSOR_KEY_HEX";
pub const ENV_ALLOW_STARTUP_MIGRATE: &str = "APP_ALLOW_STARTUP_MIGRATE";
pub const ENV_LOG_LEVEL: &str = "APP_LOG_LEVEL";
/// The one variable without the `APP_` prefix: Farsight's API key.
pub const ENV_FARSIGHT_TOKEN: &str = "FARSIGHT_TOKEN";
/// `1` when a supervisor starts `serve` again after it exits, so the admin
/// UI may restart it (admin-ui design §8.5). The shipped compose sets it.
pub const ENV_SUPERVISED: &str = "APP_SUPERVISED";

/// Default `APP_CONFIG_PATH` inside the image (§11.3).
pub const DEFAULT_CONFIG_PATH: &str = "/app/config/app.toml";
/// Default `APP_DATA_DIR` inside the image (§11.3).
pub const DEFAULT_DATA_DIR: &str = "/app/data";
/// Default `APP_LOG_LEVEL`.
pub const DEFAULT_LOG_LEVEL: &str = "info";

/// A 32-byte secret decoded from 64 hex characters. `Debug` never prints it.
#[derive(Clone, PartialEq, Eq)]
pub struct Secret32([u8; 32]);

impl Secret32 {
    /// Decode exactly 64 hex characters.
    ///
    /// # Errors
    ///
    /// A reason string when the value is not 64 hex characters.
    pub fn from_hex(value: &str) -> Result<Self, String> {
        let trimmed = value.trim();
        if trimmed.len() != 64 {
            return Err(format!("expected 64 hex characters, got {}", trimmed.len()));
        }
        let mut bytes = [0u8; 32];
        hex::decode_to_slice(trimmed, &mut bytes).map_err(|e| format!("not hex: {e}"))?;
        Ok(Self(bytes))
    }

    #[must_use]
    pub fn expose(&self) -> &[u8; 32] {
        &self.0
    }
}

impl fmt::Debug for Secret32 {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("Secret32(<redacted>)")
    }
}

/// A URL that may embed credentials (`APP_DATABASE_URL`). `Debug` redacts it.
#[derive(Clone, PartialEq, Eq)]
pub struct SecretUrl(String);

impl SecretUrl {
    #[must_use]
    pub fn expose(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for SecretUrl {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("SecretUrl(<redacted>)")
    }
}

/// An API key from the environment (`FARSIGHT_TOKEN`). `Debug` redacts it.
#[derive(Clone, PartialEq, Eq)]
pub struct SecretToken(String);

impl SecretToken {
    #[must_use]
    pub fn expose(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for SecretToken {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("SecretToken(<redacted>)")
    }
}

/// Values taken from `APP_*` environment variables.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EnvConfig {
    /// Public DNS name; the AppView DID is `did:web:` + this (§6.8).
    pub public_hostname: String,
    pub database_url: SecretUrl,
    pub redis_url: String,
    pub data_dir: PathBuf,
    pub service_signing_key: Secret32,
    pub correlation_key: Secret32,
    /// `APP_ALLOW_STARTUP_MIGRATE=1` opens the §4.3 migration gate.
    pub allow_startup_migrate: bool,
    /// tracing env-filter directive.
    pub log_level: String,
    /// `FARSIGHT_TOKEN`: the API key for `farsight.url` (design-v3 §5.14.4).
    pub farsight_token: Option<SecretToken>,
    /// `APP_THREADLENS_CURSOR_KEY_HEX`: seals Thread Lens paging cursors.
    /// `None` means a key is generated at start, and cursors do not
    /// outlive the process.
    pub threadlens_cursor_key: Option<Secret32>,
    /// `APP_PRIVATE_CURSOR_KEY_HEX`: seals private-tier paging cursors.
    /// Separate from Thread Lens's key, so either can rotate alone.
    pub private_cursor_key: Secret32,
    /// `APP_SUPERVISED=1`: a supervisor restarts `serve` when it exits, so
    /// a restart from the admin UI brings it back.
    pub supervised: bool,
}

impl EnvConfig {
    /// Read the overlay through `lookup` (normally `std::env::var`).
    ///
    /// # Errors
    ///
    /// [`ConfigError::MissingEnv`] for an absent required variable, and
    /// [`ConfigError::InvalidEnv`] for a malformed one.
    pub fn from_lookup(lookup: &dyn Fn(&str) -> Option<String>) -> Result<Self, ConfigError> {
        let required = |var: &'static str| -> Result<String, ConfigError> {
            match lookup(var) {
                Some(v) if !v.trim().is_empty() => Ok(v),
                _ => Err(ConfigError::MissingEnv(var)),
            }
        };
        let invalid = |var: &'static str, reason: String| ConfigError::InvalidEnv { var, reason };

        let public_hostname = required(ENV_PUBLIC_HOSTNAME)?.trim().to_string();
        validate_hostname(&public_hostname).map_err(|r| invalid(ENV_PUBLIC_HOSTNAME, r))?;

        let database_url = required(ENV_DATABASE_URL)?;
        if !(database_url.starts_with("postgres://") || database_url.starts_with("postgresql://")) {
            return Err(invalid(
                ENV_DATABASE_URL,
                "expected a postgres:// URL".to_string(),
            ));
        }

        let redis_url = required(ENV_REDIS_URL)?;
        if !(redis_url.starts_with("redis://") || redis_url.starts_with("rediss://")) {
            return Err(invalid(ENV_REDIS_URL, "expected a redis:// URL".to_string()));
        }

        let service_signing_key = Secret32::from_hex(&required(ENV_SERVICE_SIGNING_KEY_HEX)?)
            .map_err(|r| invalid(ENV_SERVICE_SIGNING_KEY_HEX, r))?;
        let correlation_key = Secret32::from_hex(&required(ENV_CORRELATION_KEY_HEX)?)
            .map_err(|r| invalid(ENV_CORRELATION_KEY_HEX, r))?;
        let private_cursor_key = Secret32::from_hex(&required(ENV_PRIVATE_CURSOR_KEY_HEX)?)
            .map_err(|r| invalid(ENV_PRIVATE_CURSOR_KEY_HEX, r))?;

        let allow_startup_migrate = match lookup(ENV_ALLOW_STARTUP_MIGRATE).as_deref().map(str::trim) {
            None | Some("") | Some("0") => false,
            Some("1") => true,
            Some(other) => {
                return Err(invalid(
                    ENV_ALLOW_STARTUP_MIGRATE,
                    format!("expected 0 or 1, got {other:?}"),
                ));
            }
        };

        let supervised = match lookup(ENV_SUPERVISED).as_deref().map(str::trim) {
            None | Some("") | Some("0") => false,
            Some("1") => true,
            Some(other) => return Err(invalid(ENV_SUPERVISED, format!("expected 0 or 1, got {other:?}"))),
        };

        let data_dir = lookup(ENV_DATA_DIR)
            .filter(|v| !v.trim().is_empty())
            .map_or_else(|| PathBuf::from(DEFAULT_DATA_DIR), PathBuf::from);
        let log_level = lookup(ENV_LOG_LEVEL)
            .filter(|v| !v.trim().is_empty())
            .unwrap_or_else(|| DEFAULT_LOG_LEVEL.to_string());

        // Optional, but a value that is set must be a whole key.
        let threadlens_cursor_key = match lookup(ENV_THREADLENS_CURSOR_KEY_HEX).filter(|v| !v.trim().is_empty()) {
            None => None,
            Some(hex) => Some(Secret32::from_hex(&hex).map_err(|r| invalid(ENV_THREADLENS_CURSOR_KEY_HEX, r))?),
        };

        Ok(Self {
            threadlens_cursor_key,
            private_cursor_key,
            public_hostname,
            database_url: SecretUrl(database_url),
            redis_url,
            data_dir,
            service_signing_key,
            correlation_key,
            allow_startup_migrate,
            supervised,
            log_level,
            farsight_token: lookup(ENV_FARSIGHT_TOKEN)
                .map(|v| v.trim().to_string())
                .filter(|v| !v.is_empty())
                .map(SecretToken),
        })
    }

    /// `did:web:<public_hostname>` (§6.8).
    #[must_use]
    pub fn service_did(&self) -> String {
        format!("did:web:{}", self.public_hostname)
    }
}

/// The config file path: `APP_CONFIG_PATH`, else the image default.
#[must_use]
pub fn config_path(lookup: &dyn Fn(&str) -> Option<String>) -> PathBuf {
    lookup(ENV_CONFIG_PATH)
        .filter(|v| !v.trim().is_empty())
        .map_or_else(|| PathBuf::from(DEFAULT_CONFIG_PATH), PathBuf::from)
}

/// A bare DNS hostname: lowercase labels, no scheme, port or path.
///
/// # Errors
///
/// A reason string describing the first problem.
pub fn validate_hostname(host: &str) -> Result<(), String> {
    if host.is_empty() || host.len() > 253 {
        return Err("hostname must be 1-253 characters".to_string());
    }
    if host.contains("://") || host.contains('/') || host.contains(':') {
        return Err("expected a bare hostname without scheme, port or path".to_string());
    }
    for label in host.split('.') {
        let ok = !label.is_empty()
            && label.len() <= 63
            && !label.starts_with('-')
            && !label.ends_with('-')
            && label
                .bytes()
                .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-');
        if !ok {
            return Err(format!("invalid hostname label {label:?} (lowercase a-z, 0-9, -)"));
        }
    }
    if !host.contains('.') {
        return Err("hostname must have at least two labels".to_string());
    }
    Ok(())
}
