//! Configuration: `app.toml` plus the `APP_*` environment overlay
//! (design-v2 §3.2, §11.3).
//!
//! Loading order: read the TOML (unknown keys fail), read the environment
//! overlay, then run [`AppConfig::validate`]. Any failure aborts startup.

mod env;
mod sections;
mod validate;

#[cfg(test)]
mod tests;

use std::path::{Path, PathBuf};

pub use env::{
    DEFAULT_CONFIG_PATH, DEFAULT_DATA_DIR, DEFAULT_LOG_LEVEL, ENV_ALLOW_STARTUP_MIGRATE,
    ENV_CONFIG_PATH, ENV_CORRELATION_KEY_HEX, ENV_DATA_DIR, ENV_DATABASE_URL, ENV_LOG_LEVEL,
    ENV_PUBLIC_HOSTNAME, ENV_REDIS_URL, ENV_SERVICE_SIGNING_KEY_HEX, EnvConfig, Secret32,
    SecretToken, SecretUrl, config_path, validate_hostname,
};
pub use sections::{
    ActivitySubscriptionsConfig, AdminConfig, AuthConfig, BookmarksConfig, BackfillConfig, COMMUNITY_NETWORK, CacheConfig, CascadeConfig,
    CommunityConfig, ConstellationConfig, CountsConfig, CountsSource, DbConfig, DraftsConfig, FeaturesConfig,
    FarsightConfig, FileConfig, Float, FirehoseConfig, Honest501Config, HttpConfig, IdentityConfig, IndexingConfig,
    KnownFollowersConfig, KnownLikersConfig, KryphocronConfig, LOCAL_APPVIEW, ListsConfig, MetricsConfig, MicrocosmConfig, NetworkAuth, NetworkConfig, NotificationsConfig, OpThreadConfig,
    PrivateReadConfig, PrivateReadFloors, PublicScope, RateClassConfig, RateLimitsConfig,
    RetentionConfig, SchemaBackfillConfig, SchemaIndexConfig, ScopeConfig, ScopePolicy, ServiceConfig, ServiceState, StorageConfig,
    StarterPacksConfig, SyncConfig, ThreadConfig, ThreadMutesConfig, ThreadV2Config, UtcTimeOfDay,
};
pub use validate::{OTHER_RELAY_LABEL, pds_pattern};

/// Configuration failures. Every variant aborts startup.
#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    #[error("cannot read config file {}: {source}", .path.display())]
    Io {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    /// Includes unknown-key rejections from `deny_unknown_fields`. Boxed to
    /// keep the error small.
    #[error("invalid config file: {0}")]
    Parse(Box<toml::de::Error>),
    #[error("missing required environment variable {0}")]
    MissingEnv(&'static str),
    #[error("invalid environment variable {var}: {reason}")]
    InvalidEnv { var: &'static str, reason: String },
    #[error("config_error: {}", .0.join("; "))]
    Invalid(Vec<String>),
}

impl From<toml::de::Error> for ConfigError {
    fn from(e: toml::de::Error) -> Self {
        Self::Parse(Box::new(e))
    }
}

/// The complete runtime configuration.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AppConfig {
    /// Everything from `app.toml`.
    pub file: FileConfig,
    /// Everything from `APP_*` variables.
    pub env: EnvConfig,
}

impl FileConfig {
    /// Parse `app.toml` contents. Unknown keys are errors.
    ///
    /// # Errors
    ///
    /// [`ConfigError::Parse`] on malformed TOML, a wrong type, or an unknown key.
    pub fn parse(toml_text: &str) -> Result<Self, ConfigError> {
        Ok(toml::from_str(toml_text)?)
    }
}

impl AppConfig {
    /// Read `path`, apply the process environment overlay, and validate.
    ///
    /// # Errors
    ///
    /// Any [`ConfigError`].
    pub fn load(path: &Path) -> Result<Self, ConfigError> {
        Self::load_with(path, &|var| std::env::var(var).ok())
    }

    /// Like [`AppConfig::load`], with the file path taken from
    /// `APP_CONFIG_PATH` (default `/app/config/app.toml`).
    ///
    /// # Errors
    ///
    /// Any [`ConfigError`].
    pub fn load_from_env() -> Result<Self, ConfigError> {
        let lookup = |var: &str| std::env::var(var).ok();
        Self::load_with(&config_path(&lookup), &lookup)
    }

    /// Like [`AppConfig::load`], reading environment variables through `lookup`.
    ///
    /// # Errors
    ///
    /// Any [`ConfigError`].
    pub fn load_with(
        path: &Path,
        lookup: &dyn Fn(&str) -> Option<String>,
    ) -> Result<Self, ConfigError> {
        let text = std::fs::read_to_string(path).map_err(|source| ConfigError::Io {
            path: path.to_path_buf(),
            source,
        })?;
        Self::from_parts(&text, lookup)
    }

    /// Build from TOML text and an environment lookup, then validate.
    ///
    /// # Errors
    ///
    /// Any [`ConfigError`] except [`ConfigError::Io`].
    pub fn from_parts(
        toml_text: &str,
        lookup: &dyn Fn(&str) -> Option<String>,
    ) -> Result<Self, ConfigError> {
        let cfg = Self {
            file: FileConfig::parse(toml_text)?,
            env: EnvConfig::from_lookup(lookup)?,
        };
        cfg.validate()?;
        Ok(cfg)
    }

    /// Check every cross-field rule (design-v2 §5.3, §8.1, §8.5, §9.1 and the
    /// runtime guards in `validate.rs`).
    ///
    /// # Errors
    ///
    /// [`ConfigError::Invalid`] listing every problem found.
    pub fn validate(&self) -> Result<(), ConfigError> {
        let mut problems = validate::problems(&self.file, &self.env.public_hostname);
        crate::farsight::config_problems(&self.file.farsight, self.env.farsight_token.is_some(), &mut problems);
        if problems.is_empty() {
            Ok(())
        } else {
            Err(ConfigError::Invalid(problems))
        }
    }

    /// The AppView's service DID, `did:web:<APP_PUBLIC_HOSTNAME>` (§6.8).
    #[must_use]
    pub fn service_did(&self) -> String {
        self.env.service_did()
    }
}
