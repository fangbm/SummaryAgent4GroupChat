//! Platform client lifecycle, reconnects, and wxdb watcher restarts.

use crate::*;

#[derive(Debug, Clone, Eq, PartialEq)]
pub(crate) struct PlatformConnectionFingerprint {
    pub(crate) kind: PlatformKindConfig,
    wx_python: String,
    wx_script: String,
    wx_ready_timeout: u64,
    wx_command_timeout: u64,
    wx_groups: Vec<String>,
    wx_cli_executable: String,
    wx_cli_timeout: u64,
    wx_cli_history_timeout: u64,
    wx_cli_temp_dir: String,
    wx_cli_cache_dir: String,
    wx_cli_db_dir: Option<String>,
    wx_cli_group_name_map: Vec<(String, String)>,
    discord_token: Option<String>,
    discord_token_env: String,
    discord_channels: Vec<String>,
    whitelist_rooms: Vec<String>,
}

impl PlatformConnectionFingerprint {
    pub(crate) fn from_config(config: &AgentConfig) -> Self {
        let mut group_name_map = config
            .wx_cli
            .group_name_map
            .iter()
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect::<Vec<_>>();
        group_name_map.sort();
        Self {
            kind: config.platform.kind,
            wx_python: config.wx4py.python_executable.clone(),
            wx_script: config.wx4py.sidecar_script.clone(),
            wx_ready_timeout: config.wx4py.ready_timeout_seconds,
            wx_command_timeout: config.wx4py.command_timeout_seconds,
            wx_groups: config.wx4py.groups.clone(),
            wx_cli_executable: config.wx_cli.executable.clone(),
            wx_cli_timeout: config.wx_cli.timeout_seconds,
            wx_cli_history_timeout: config.wx_cli.history_query_timeout_seconds,
            wx_cli_temp_dir: config.wx_cli.temp_dir.clone(),
            wx_cli_cache_dir: config.wx_cli.cache_dir.clone(),
            wx_cli_db_dir: config.wx_cli.db_dir.clone(),
            wx_cli_group_name_map: group_name_map,
            discord_token: config.discord.token.clone(),
            discord_token_env: config.discord.token_env.clone(),
            discord_channels: config.discord.channels.clone(),
            whitelist_rooms: config.listen.whitelist_rooms.clone(),
        }
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub(crate) struct WxdbWatcherFingerprint {
    enabled: bool,
    pub(crate) rooms: Vec<String>,
    triggers: Vec<String>,
    match_mode: MatchMode,
    whitelist_rooms: Vec<String>,
    blacklist_users: Vec<String>,
    content_types: Vec<String>,
    ignore_self: bool,
    wx_cli_executable: String,
    wx_cli_timeout: u64,
    wx_cli_history_timeout: u64,
    wx_cli_temp_dir: String,
    wx_cli_cache_dir: String,
    wx_cli_db_dir: Option<String>,
    wx_cli_group_name_map: Vec<(String, String)>,
}

impl WxdbWatcherFingerprint {
    pub(crate) fn from_config(config: &AgentConfig) -> Self {
        let listen = effective_listen_config(config);
        let mut group_name_map = config
            .wx_cli
            .group_name_map
            .iter()
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect::<Vec<_>>();
        group_name_map.sort();
        Self {
            enabled: wxdb_watcher::enabled(config),
            rooms: wxdb_watcher::configured_rooms(config),
            triggers: listen.triggers,
            match_mode: listen.match_mode,
            whitelist_rooms: listen.whitelist_rooms,
            blacklist_users: listen.blacklist_users,
            content_types: listen.content_types,
            ignore_self: listen.ignore_self,
            wx_cli_executable: config.wx_cli.executable.clone(),
            wx_cli_timeout: config.wx_cli.timeout_seconds,
            wx_cli_history_timeout: config.wx_cli.history_query_timeout_seconds,
            wx_cli_temp_dir: config.wx_cli.temp_dir.clone(),
            wx_cli_cache_dir: config.wx_cli.cache_dir.clone(),
            wx_cli_db_dir: config.wx_cli.db_dir.clone(),
            wx_cli_group_name_map: group_name_map,
        }
    }
}

pub(crate) struct PlatformRuntime {
    pub(crate) client: Arc<Mutex<PlatformClient>>,
    pub(crate) worker: PlatformWorker,
    pub(crate) rooms: Vec<String>,
    pub(crate) fingerprint: PlatformConnectionFingerprint,
    pub(crate) watcher: WxdbCommandWatcher,
    pub(crate) watcher_fingerprint: WxdbWatcherFingerprint,
    pub(crate) watcher_restart_at: Option<Instant>,
    pub(crate) reconnect_at: Option<Instant>,
}

impl PlatformRuntime {
    pub(crate) async fn start(config: &AgentConfig) -> Result<Self> {
        let client = PlatformClient::start(config).await.with_context(|| {
            format!("starting {} platform client", config.platform.kind.as_str())
        })?;
        let worker = client.worker();
        let rooms = client.configured_rooms(config);
        Ok(Self {
            client: Arc::new(Mutex::new(client)),
            worker,
            rooms,
            fingerprint: PlatformConnectionFingerprint::from_config(config),
            watcher: WxdbCommandWatcher::start(config),
            watcher_fingerprint: WxdbWatcherFingerprint::from_config(config),
            watcher_restart_at: None,
            reconnect_at: None,
        })
    }

    pub(crate) fn request_reconnect(&mut self, config: &AgentConfig, reason: &str) {
        if self.reconnect_at.is_none() {
            append_runtime_log(
                config,
                &format!("platform reconnect scheduled reason={reason} delay_seconds=2"),
            );
            self.reconnect_at = Some(Instant::now() + PLATFORM_RECONNECT_DELAY);
        }
    }

    pub(crate) async fn reconnect_if_due(&mut self, config: &AgentConfig) {
        let Some(reconnect_at) = self.reconnect_at else {
            return;
        };
        if Instant::now() < reconnect_at {
            return;
        }

        match PlatformClient::start(config).await {
            Ok(client) => {
                let worker = client.worker();
                let rooms = client.configured_rooms(config);
                let previous_state_path = self.watcher.state_path().map(Path::to_path_buf);
                self.client = Arc::new(Mutex::new(client));
                self.worker = worker;
                self.rooms = rooms;
                self.fingerprint = PlatformConnectionFingerprint::from_config(config);
                let old_watcher =
                    std::mem::replace(&mut self.watcher, WxdbCommandWatcher::stopped());
                drop(old_watcher);
                self.watcher =
                    WxdbCommandWatcher::start_with_state_path(config, previous_state_path);
                self.watcher_fingerprint = WxdbWatcherFingerprint::from_config(config);
                self.watcher_restart_at = None;
                self.reconnect_at = None;
                info!(
                    platform = self.fingerprint.kind.as_str(),
                    "platform reconnected"
                );
                append_runtime_log(
                    config,
                    &format!(
                        "platform reconnected kind={}",
                        self.fingerprint.kind.as_str()
                    ),
                );
            }
            Err(error) => {
                let message = format_error_chain(&error);
                error!(error = %message, "platform reconnect failed; retrying");
                append_runtime_log(
                    config,
                    &format!("platform reconnect failed error={message}; retrying"),
                );
                self.reconnect_at = Some(Instant::now() + PLATFORM_RECONNECT_DELAY);
            }
        }
    }

    pub(crate) fn note_watcher_disconnected(&mut self, config: &AgentConfig) {
        if self.watcher.enabled() && self.watcher_restart_at.is_none() {
            error!("wxdb command watcher channel disconnected; scheduling restart");
            append_runtime_log(
                config,
                "wxdb command watcher channel disconnected; restart scheduled",
            );
            self.watcher_restart_at = Some(Instant::now() + WXDB_WATCHER_RESTART_DELAY);
        }
    }

    pub(crate) fn restart_watcher_if_due(&mut self, config: &AgentConfig) {
        let Some(restart_at) = self.watcher_restart_at else {
            return;
        };
        if Instant::now() >= restart_at {
            let previous_state_path = self.watcher.state_path().map(Path::to_path_buf);
            let old_watcher = std::mem::replace(&mut self.watcher, WxdbCommandWatcher::stopped());
            drop(old_watcher);
            self.watcher = WxdbCommandWatcher::start_with_state_path(config, previous_state_path);
            self.watcher_fingerprint = WxdbWatcherFingerprint::from_config(config);
            self.watcher_restart_at = None;
            append_runtime_log(
                config,
                "wxdb command watcher restarted after channel disconnect",
            );
        }
    }

    pub(crate) fn restart_watcher(&mut self, config: &AgentConfig, reason: &str) {
        let previous_state_path = self.watcher.state_path().map(Path::to_path_buf);
        let state_preserved = previous_state_path.is_some();
        let old_watcher = std::mem::replace(&mut self.watcher, WxdbCommandWatcher::stopped());
        drop(old_watcher);
        self.watcher = WxdbCommandWatcher::start_with_state_path(config, previous_state_path);
        self.watcher_fingerprint = WxdbWatcherFingerprint::from_config(config);
        self.watcher_restart_at = None;
        append_runtime_log(
            config,
            &format!(
                "wxdb command watcher restarted reason={reason} state_preserved={state_preserved}"
            ),
        );
    }
}
