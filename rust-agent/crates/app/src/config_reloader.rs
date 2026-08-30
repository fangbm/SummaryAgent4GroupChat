//! Configuration hot-reload state and validation.

use crate::*;

pub(crate) struct ConfigReloader {
    path: PathBuf,
    config: AgentConfig,
    matcher: TriggerMatcher,
    last_modified: Option<SystemTime>,
    last_failed_modified: Option<SystemTime>,
}

impl ConfigReloader {
    pub(crate) fn load(config_path: &str) -> Result<Self> {
        let path = PathBuf::from(config_path);
        let config = AgentConfig::from_path(&path)
            .with_context(|| format!("loading config {}", path.display()))?;
        let matcher = TriggerMatcher::new(effective_listen_config(&config))
            .context("building trigger matcher")?;
        let last_modified = config_modified_time(&path);
        Ok(Self {
            path,
            config,
            matcher,
            last_modified,
            last_failed_modified: None,
        })
    }

    pub(crate) fn config(&self) -> &AgentConfig {
        &self.config
    }

    pub(crate) fn matcher(&self) -> &TriggerMatcher {
        &self.matcher
    }

    pub(crate) fn reload_if_changed(&mut self) -> Result<bool> {
        let modified = config_modified_time(&self.path);
        if modified.is_none() || modified == self.last_modified {
            return Ok(false);
        }
        if modified == self.last_failed_modified {
            return Ok(false);
        }

        let config = match AgentConfig::from_path(&self.path) {
            Ok(config) => config,
            Err(error) => {
                self.last_failed_modified = modified;
                let message = format!(
                    "config hot reload failed path={} error={}",
                    self.path.display(),
                    error
                );
                warn!(path = %self.path.display(), error = %error, "config hot reload failed");
                append_runtime_log(&self.config, &message);
                return Ok(false);
            }
        };
        let matcher = match TriggerMatcher::new(effective_listen_config(&config)) {
            Ok(matcher) => matcher,
            Err(error) => {
                self.last_failed_modified = modified;
                let message = format!(
                    "config hot reload failed path={} error=building trigger matcher: {}",
                    self.path.display(),
                    error
                );
                warn!(
                    path = %self.path.display(),
                    error = %error,
                    "config hot reload failed while rebuilding trigger matcher"
                );
                append_runtime_log(&self.config, &message);
                return Ok(false);
            }
        };

        self.config = config;
        self.matcher = matcher;
        self.last_modified = modified;
        self.last_failed_modified = None;
        info!(path = %self.path.display(), "config hot reloaded");
        append_runtime_log(
            &self.config,
            &format!(
                "config hot reloaded path={} note=platform connection/listener changes trigger controlled reconnect; storage path and runtime log writer remain startup-scoped",
                self.path.display()
            ),
        );
        Ok(true)
    }
}
