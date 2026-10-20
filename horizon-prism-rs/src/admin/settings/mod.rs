//! Settings (admin-ui design §8): the registry of editable keys, the
//! overlay of saved values on `app.toml`, the startup rule and the
//! `settings set` transaction, and the live values.

pub mod live;
pub mod overlay;
pub mod registry;
pub mod store;

#[cfg(test)]
pub(crate) mod tests;

use std::ops::Deref;
use std::sync::Arc;

use chrono::{DateTime, Utc};
use serde_json::Value;

pub use live::{LiveSettings, LiveValues, RestartHandle};

use crate::config::{
    ENV_CORRELATION_KEY_HEX, ENV_DATABASE_URL, ENV_FARSIGHT_TOKEN, ENV_PRIVATE_CURSOR_KEY_HEX, ENV_REDIS_URL, ENV_SERVICE_SIGNING_KEY_HEX,
    ENV_THREADLENS_CURSOR_KEY_HEX, EnvConfig, FileConfig,
};
use crate::identity::lodestar::Lodestar;

/// What the environment holds, as the "set at install" page shows it:
/// secrets only as set or not set (§5.9).
#[derive(Debug, Clone, Default)]
pub struct EnvFacts {
    pub public_hostname: String,
    /// `APP_SUPERVISED=1`: the restart button works.
    pub supervised: bool,
    /// Each secret's variable and whether it is set.
    pub secrets: Vec<(&'static str, bool)>,
}

impl EnvFacts {
    #[must_use]
    pub fn from_env(env: &EnvConfig) -> Self {
        Self {
            public_hostname: env.public_hostname.clone(),
            supervised: env.supervised,
            secrets: vec![
                (ENV_SERVICE_SIGNING_KEY_HEX, true),
                (ENV_CORRELATION_KEY_HEX, true),
                (ENV_PRIVATE_CURSOR_KEY_HEX, true),
                (ENV_THREADLENS_CURSOR_KEY_HEX, env.threadlens_cursor_key.is_some()),
                (ENV_FARSIGHT_TOKEN, env.farsight_token.is_some()),
                (ENV_DATABASE_URL, true),
                (ENV_REDIS_URL, true),
            ],
        }
    }
}

/// What the settings pages and the banners read.
pub struct ContextInner {
    pub live: LiveSettings,
    /// The configuration the process started with: the file, the saved
    /// settings kept at startup, validated.
    pub running: FileConfig,
    pub running_json: Value,
    /// `app.toml` alone, as it was at startup.
    pub file: FileConfig,
    pub file_json: Value,
    pub file_table: toml::Table,
    pub env: EnvFacts,
    /// When this process started, so a restart's page can tell it is back.
    pub started_at: DateTime<Utc>,
    /// Lodestar's gate, when lodestar is set.
    pub lodestar: Option<Arc<Lodestar>>,
}

/// [`ContextInner`], cheap to clone.
#[derive(Clone)]
pub struct SettingsContext(Arc<ContextInner>);

impl Deref for SettingsContext {
    type Target = ContextInner;

    fn deref(&self) -> &ContextInner {
        &self.0
    }
}

impl SettingsContext {
    /// The context of a process that started with `running`, from the file
    /// `file` (`table` its TOML).
    #[must_use]
    pub fn new(running: FileConfig, file: FileConfig, file_table: toml::Table, live: LiveSettings, env: EnvFacts, lodestar: Option<Arc<Lodestar>>) -> Self {
        Self(Arc::new(ContextInner {
            live,
            running_json: overlay::json(&running),
            running,
            file_json: overlay::json(&file),
            file,
            file_table,
            env,
            started_at: Utc::now(),
            lodestar,
        }))
    }

    /// A process started from `file` with nothing saved (tests).
    #[must_use]
    pub fn from_file(file: FileConfig) -> Self {
        let live = LiveSettings::new(&file);
        Self::new(file.clone(), file, toml::Table::new(), live, EnvFacts::default(), None)
    }

    /// The routing table's network names.
    #[must_use]
    pub fn networks(&self) -> Vec<String> {
        self.running.constellation.networks.iter().map(|n| n.name.clone()).collect()
    }

    /// What the registry's forms need.
    #[must_use]
    pub fn registry(&self) -> registry::Context {
        registry::Context {
            networks: self.networks(),
            proxied: self.running.services.iter().filter(|(_, s)| s.proxy_url.is_some()).map(|(name, _)| name.clone()).collect(),
        }
    }
}
