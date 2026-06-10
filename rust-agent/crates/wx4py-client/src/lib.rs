use std::{
    fs::{self, File},
    io::{BufRead, BufReader, Write},
    path::{Path, PathBuf},
    process::{Child, ChildStdin, Command, ExitStatus, Output, Stdio},
    sync::{
        mpsc::{self, Receiver, RecvTimeoutError},
        Arc, Mutex, OnceLock, TryLockError,
    },
    thread,
    time::{Duration as StdDuration, Instant},
};

use chrono::{DateTime, Duration, NaiveDateTime, TimeZone, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use thiserror::Error;
use wechat_summary_core::config::{ListenConfig, Wx4pyConfig, WxCliConfig};

#[derive(Debug, Error)]
pub enum Wx4pyError {
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("json error: {0}")]
    Json(#[from] serde_json::Error),
    #[error("sidecar failed: {0}")]
    Sidecar(String),
    #[error("wx4py sidecar did not become ready within {0} seconds")]
    ReadyTimeout(u64),
    #[error("wx4py requires at least one group name in [wx4py].groups or listen.whitelist_rooms")]
    MissingGroups,
    #[error("wxdb failed: {0}")]
    WxCli(String),
    #[error("invalid wxdb response: {0}")]
    InvalidWxCli(String),
    #[error("wxdb history query did not finish within {0} seconds")]
    HistoryQueryTimeout(u64),
}

pub type Result<T> = std::result::Result<T, Wx4pyError>;

#[derive(Debug, Clone)]
pub struct WxdbInitReport {
    pub db_dir: String,
    pub before_keys: usize,
    pub imported_legacy_keys: usize,
    pub scanned_keys: usize,
    pub after_keys: usize,
    pub scan_error: Option<String>,
}

pub fn refresh_builtin_wxdb_keys_on_start(
    wx_cli: &WxCliConfig,
) -> Result<Option<Vec<WxdbInitReport>>> {
    if !should_use_builtin_wxdb(wx_cli) {
        return Ok(None);
    }

    let config = builtin_wxdb_runtime_config(wx_cli);
    let reports = wechat_summary_wxdb::refresh_keys(&config)
        .map_err(|error| Wx4pyError::WxCli(format!("builtin wxdb init failed: {error:#}")))?
        .into_iter()
        .map(|report| WxdbInitReport {
            db_dir: report.db_dir.display().to_string(),
            before_keys: report.before_keys,
            imported_legacy_keys: report.imported_legacy_keys,
            scanned_keys: report.scanned_keys,
            after_keys: report.after_keys,
            scan_error: report.scan_error,
        })
        .collect();
    Ok(Some(reports))
}

#[derive(Debug)]
pub struct Wx4pyClient {
    _child: Child,
    stdin: Arc<Mutex<ChildStdin>>,
    receiver: Receiver<SidecarMessage>,
    wx_cli: WxCliConfig,
}

#[derive(Debug, Clone)]
pub struct Wx4pySender {
    stdin: Arc<Mutex<ChildStdin>>,
}

impl Wx4pyClient {
    pub fn start(
        config: &Wx4pyConfig,
        listen: &ListenConfig,
        wx_cli: &WxCliConfig,
    ) -> Result<Self> {
        let groups = listen_groups(config, listen)?;
        stop_wx_cli_daemon_on_start(wx_cli);
        let mut child = Command::new(&config.python_executable)
            .arg(&config.sidecar_script)
            .arg("--ready-timeout-seconds")
            .arg(config.ready_timeout_seconds.to_string())
            .args(groups.iter().flat_map(|group| ["--group", group.as_str()]))
            .env("PYTHONUTF8", "1")
            .env("PYTHONIOENCODING", "utf-8")
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .spawn()
            .map_err(|error| {
                Wx4pyError::Sidecar(format!(
                    "failed to spawn wx4py sidecar python={} script={}: {error}",
                    config.python_executable, config.sidecar_script
                ))
            })?;

        let stdin =
            Arc::new(Mutex::new(child.stdin.take().ok_or_else(|| {
                Wx4pyError::Sidecar("failed to open sidecar stdin".into())
            })?));
        let stdout = child
            .stdout
            .take()
            .ok_or_else(|| Wx4pyError::Sidecar("failed to open sidecar stdout".into()))?;
        let (sender, receiver) = mpsc::channel();

        thread::spawn(move || {
            let mut reader = BufReader::new(stdout);
            let mut buffer = Vec::new();
            loop {
                buffer.clear();
                match reader.read_until(b'\n', &mut buffer) {
                    Ok(0) => break,
                    Ok(_) => {
                        let line = String::from_utf8_lossy(&buffer);
                        let line = line.trim();
                        if line.is_empty() {
                            continue;
                        }
                        match serde_json::from_str::<SidecarMessage>(line) {
                            Ok(message) => {
                                if sender.send(message).is_err() {
                                    break;
                                }
                            }
                            Err(error) => {
                                eprintln!("ignored wx4py sidecar stdout line: {error}: {line}");
                            }
                        }
                    }
                    Err(error) => {
                        let _ = sender.send(SidecarMessage::Error {
                            message: format!("sidecar stdout error: {error}"),
                        });
                        break;
                    }
                }
            }
        });

        let client = Self {
            _child: child,
            stdin,
            receiver,
            wx_cli: wx_cli.clone(),
        };
        client.wait_ready(config.ready_timeout_seconds)?;
        Ok(client)
    }

    pub fn next_event(&self) -> Result<Wx4pyEvent> {
        loop {
            match self.receiver.recv() {
                Ok(SidecarMessage::Event(event)) => return Ok(event),
                Ok(SidecarMessage::Ready { .. }) => continue,
                Ok(SidecarMessage::CommandError { message }) => {
                    eprintln!("wx4py sidecar command error: {message}");
                    continue;
                }
                Ok(SidecarMessage::Error { message }) => return Err(Wx4pyError::Sidecar(message)),
                Err(error) => return Err(Wx4pyError::Sidecar(error.to_string())),
            }
        }
    }

    pub fn next_event_timeout(&self, timeout: StdDuration) -> Result<Option<Wx4pyEvent>> {
        loop {
            match self.receiver.recv_timeout(timeout) {
                Ok(SidecarMessage::Event(event)) => return Ok(Some(event)),
                Ok(SidecarMessage::Ready { .. }) => continue,
                Ok(SidecarMessage::CommandError { message }) => {
                    eprintln!("wx4py sidecar command error: {message}");
                    continue;
                }
                Ok(SidecarMessage::Error { message }) => return Err(Wx4pyError::Sidecar(message)),
                Err(RecvTimeoutError::Timeout) => return Ok(None),
                Err(RecvTimeoutError::Disconnected) => {
                    return Err(Wx4pyError::Sidecar(
                        "wx4py sidecar exited before next event".to_string(),
                    ));
                }
            }
        }
    }

    pub async fn send_text(&self, room_id: &str, text: &str) -> Result<()> {
        self.sender().send_text(room_id, text).await
    }

    pub async fn send_image(&self, room_id: &str, image_path: &str) -> Result<()> {
        self.sender().send_image(room_id, image_path).await
    }

    pub async fn query_text_messages(
        &self,
        room_id: &str,
        room_name: Option<&str>,
        since: DateTime<Utc>,
        until: DateTime<Utc>,
        limit: u32,
        media_decode_limit: Option<usize>,
    ) -> Result<Vec<Wx4pyHistoryMessage>> {
        let chat_name = self.chat_name(room_id, room_name);
        let output = self.temp_output_path(room_id);
        let wx_cli = self.wx_cli.clone();
        let total_timeout_seconds = history_query_timeout_seconds(&wx_cli);
        let (sender, receiver) = mpsc::channel();
        let started = Instant::now();
        tracing::info!(
            room_id,
            chat_name,
            %since,
            %until,
            limit,
            media_decode_limit = ?media_decode_limit,
            timeout_seconds = total_timeout_seconds,
            "wxdb history worker spawning"
        );

        thread::spawn(move || {
            let result = if should_use_builtin_wxdb(&wx_cli) {
                match WXDB_HISTORY_QUERY_LOCK
                    .get_or_init(|| Mutex::new(()))
                    .try_lock()
                {
                    Ok(_guard) => query_text_messages_inner(
                        &wx_cli,
                        &chat_name,
                        since,
                        until,
                        limit,
                        media_decode_limit,
                        &output,
                    ),
                    Err(TryLockError::WouldBlock) => {
                        tracing::warn!(
                            chat_name,
                            "builtin wxdb history worker rejected because another query is still running"
                        );
                        Err(Wx4pyError::WxCli(
                            "previous builtin wxdb history query is still running; wait for it to finish or restart the agent before retrying".to_string(),
                        ))
                    }
                    Err(TryLockError::Poisoned(_)) => Err(Wx4pyError::WxCli(
                        "builtin wxdb history query lock poisoned".to_string(),
                    )),
                }
            } else {
                query_text_messages_inner(
                    &wx_cli,
                    &chat_name,
                    since,
                    until,
                    limit,
                    media_decode_limit,
                    &output,
                )
            };
            let _ = sender.send(result);
        });

        match receiver.recv_timeout(StdDuration::from_secs(total_timeout_seconds)) {
            Ok(result) => {
                match &result {
                    Ok(messages) => tracing::info!(
                        room_id,
                        count = messages.len(),
                        elapsed_ms = started.elapsed().as_millis(),
                        "wxdb history worker completed"
                    ),
                    Err(error) => tracing::warn!(
                        room_id,
                        error = %error,
                        elapsed_ms = started.elapsed().as_millis(),
                        "wxdb history worker failed"
                    ),
                }
                result
            }
            Err(RecvTimeoutError::Timeout) => {
                tracing::warn!(
                    room_id,
                    elapsed_ms = started.elapsed().as_millis(),
                    timeout_seconds = total_timeout_seconds,
                    "wxdb history worker timed out"
                );
                Err(Wx4pyError::HistoryQueryTimeout(total_timeout_seconds))
            }
            Err(RecvTimeoutError::Disconnected) => Err(Wx4pyError::WxCli(
                "wxdb history query worker exited before returning a result".to_string(),
            )),
        }
    }

    fn wait_ready(&self, timeout_seconds: u64) -> Result<()> {
        match self
            .receiver
            .recv_timeout(StdDuration::from_secs(timeout_seconds))
        {
            Ok(SidecarMessage::Ready { ok: true }) => Ok(()),
            Ok(SidecarMessage::Ready { ok: false }) => Err(Wx4pyError::Sidecar(
                "wx4py sidecar reported not ready".to_string(),
            )),
            Ok(SidecarMessage::CommandError { message }) => Err(Wx4pyError::Sidecar(message)),
            Ok(SidecarMessage::Error { message }) => Err(Wx4pyError::Sidecar(message)),
            Ok(SidecarMessage::Event(_)) => Ok(()),
            Err(RecvTimeoutError::Timeout) => Err(Wx4pyError::ReadyTimeout(timeout_seconds)),
            Err(RecvTimeoutError::Disconnected) => Err(Wx4pyError::Sidecar(
                "wx4py sidecar exited before ready".to_string(),
            )),
        }
    }

    pub fn sender(&self) -> Wx4pySender {
        Wx4pySender {
            stdin: Arc::clone(&self.stdin),
        }
    }

    fn chat_name(&self, room_id: &str, room_name: Option<&str>) -> String {
        self.wx_cli
            .group_name_map
            .get(room_id)
            .cloned()
            .or_else(|| room_name.and_then(|name| self.wx_cli.group_name_map.get(name).cloned()))
            .or_else(|| room_name.map(ToOwned::to_owned))
            .unwrap_or_else(|| room_id.to_string())
    }

    fn temp_output_path(&self, room_id: &str) -> PathBuf {
        let safe_room = room_id
            .chars()
            .map(|ch| if ch.is_ascii_alphanumeric() { ch } else { '-' })
            .collect::<String>();
        PathBuf::from(&self.wx_cli.temp_dir).join(format!(
            "wx4py-wxdb-{}-{}.json",
            safe_room,
            Utc::now().timestamp_millis()
        ))
    }
}

static WXDB_HISTORY_QUERY_LOCK: OnceLock<Mutex<()>> = OnceLock::new();

impl Wx4pySender {
    pub async fn send_text(&self, room_id: &str, text: &str) -> Result<()> {
        self.send_command(SidecarCommand::SendText {
            room: room_id.to_string(),
            text: text.to_string(),
        })
    }

    pub async fn send_image(&self, room_id: &str, image_path: &str) -> Result<()> {
        self.send_command(SidecarCommand::SendImage {
            room: room_id.to_string(),
            path: image_path.to_string(),
        })
    }

    fn send_command(&self, command: SidecarCommand) -> Result<()> {
        let mut stdin = self
            .stdin
            .lock()
            .map_err(|_| Wx4pyError::Sidecar("sidecar stdin mutex poisoned".into()))?;
        serde_json::to_writer(&mut *stdin, &command)?;
        stdin.write_all(b"\n")?;
        stdin.flush()?;
        Ok(())
    }
}

fn query_text_messages_inner(
    wx_cli: &WxCliConfig,
    chat_name: &str,
    since: DateTime<Utc>,
    until: DateTime<Utc>,
    limit: u32,
    media_decode_limit: Option<usize>,
    output: &Path,
) -> Result<Vec<Wx4pyHistoryMessage>> {
    if should_use_builtin_wxdb(wx_cli) {
        return query_text_messages_via_builtin_wxdb(
            wx_cli,
            chat_name,
            since,
            until,
            limit,
            media_decode_limit,
        );
    }

    if let Some(parent) = output.parent() {
        fs::create_dir_all(parent)?;
    }
    tracing::info!(
        chat_name,
        %since,
        %until,
        limit,
        "querying wxdb history"
    );
    match query_text_messages_via_history(wx_cli, chat_name, since, until, limit) {
        Ok(messages) if !messages.is_empty() => return Ok(messages),
        Ok(_) => {
            tracing::warn!(
                chat_name,
                %since,
                %until,
                "wxdb history returned no messages; falling back to wxdb export"
            );
        }
        Err(error) => {
            if should_skip_export_after_history_error(&error) {
                return Err(error);
            }
            tracing::warn!(
                chat_name,
                error = %error,
                "wxdb history query failed; falling back to wxdb export"
            );
        }
    }

    query_text_messages_via_export(wx_cli, chat_name, since, until, limit, output)
}

fn should_use_builtin_wxdb(wx_cli: &WxCliConfig) -> bool {
    matches!(
        wx_cli.executable.trim().to_ascii_lowercase().as_str(),
        "builtin" | "internal" | "wxdb-builtin"
    )
}

fn query_text_messages_via_builtin_wxdb(
    wx_cli: &WxCliConfig,
    chat_name: &str,
    since: DateTime<Utc>,
    until: DateTime<Utc>,
    limit: u32,
    media_decode_limit: Option<usize>,
) -> Result<Vec<Wx4pyHistoryMessage>> {
    tracing::info!(
        chat_name,
        %since,
        %until,
        limit,
        media_decode_limit = ?media_decode_limit,
        "querying builtin wxdb history"
    );
    let mut result =
        query_builtin_wxdb_history(wx_cli, chat_name, since, until, limit, media_decode_limit)?;
    if result.messages.is_empty() && until.signed_duration_since(since) >= Duration::minutes(5) {
        tracing::warn!(
            chat_name,
            %since,
            %until,
            "builtin wxdb returned no messages; retrying once after a short delay"
        );
        thread::sleep(StdDuration::from_millis(800));
        result =
            query_builtin_wxdb_history(wx_cli, chat_name, since, until, limit, media_decode_limit)?;
    }

    for warning in &result.meta.warnings {
        tracing::warn!(chat_name, warning = %warning, "builtin wxdb warning");
    }
    tracing::info!(
        chat_name,
        count = result.messages.len(),
        db_dir = ?result.meta.db_dir,
        shards_scanned = result.meta.shards_scanned,
        shards_hit = result.meta.shards_hit,
        unknown_shards = ?result.meta.unknown_shards,
        "builtin wxdb history completed"
    );
    if result.messages.is_empty() {
        if let Some(reason) = empty_builtin_wxdb_result_uncertain_reason(&result) {
            return Err(Wx4pyError::WxCli(reason));
        }
    }

    result
        .messages
        .into_iter()
        .map(|message| {
            let timestamp = Utc
                .timestamp_opt(message.timestamp, 0)
                .single()
                .ok_or_else(|| {
                    Wx4pyError::InvalidWxCli(format!(
                        "invalid wxdb timestamp: {}",
                        message.timestamp
                    ))
                })?;
            Ok(Wx4pyHistoryMessage {
                timestamp,
                sender_id: message
                    .sender_username
                    .clone()
                    .filter(|sender| !sender.is_empty())
                    .unwrap_or_else(|| message.sender.clone()),
                sender_name: (!message.sender.is_empty()).then_some(message.sender),
                content: message.content,
                msg_type: message.msg_type,
                media_path: message
                    .media_path
                    .map(|path| path.to_string_lossy().into_owned()),
                thumbnail_path: message
                    .thumbnail_path
                    .map(|path| path.to_string_lossy().into_owned()),
                decoded_media_path: message
                    .decoded_media_path
                    .map(|path| path.to_string_lossy().into_owned()),
                media_decode_error: message.media_decode_error,
                is_self: false,
            })
        })
        .collect()
}

fn empty_builtin_wxdb_result_uncertain_reason(
    result: &wechat_summary_wxdb::HistoryResult,
) -> Option<String> {
    if !result.messages.is_empty() {
        return None;
    }

    let mut reasons = result.meta.warnings.clone();
    reasons.extend(
        result
            .meta
            .unknown_shards
            .iter()
            .map(|shard| format!("磁盘存在但没有密钥的消息分片: {shard}")),
    );
    if reasons.is_empty() {
        return None;
    }

    const MAX_REASON_CHARS: usize = 900;
    let mut reason = reasons.join(" | ");
    if reason.chars().count() > MAX_REASON_CHARS {
        reason = reason
            .chars()
            .take(MAX_REASON_CHARS)
            .chain("...".chars())
            .collect();
    }
    Some(format!(
        "builtin wxdb returned no messages, but some WeChat database candidates were not fully readable; cannot confirm the chat is empty: {reason}"
    ))
}

fn query_builtin_wxdb_history(
    wx_cli: &WxCliConfig,
    chat_name: &str,
    since: DateTime<Utc>,
    until: DateTime<Utc>,
    limit: u32,
    media_decode_limit: Option<usize>,
) -> Result<wechat_summary_wxdb::HistoryResult> {
    let config = builtin_wxdb_runtime_config(wx_cli);
    wechat_summary_wxdb::query_history_with_config(
        &config,
        wechat_summary_wxdb::HistoryQuery {
            chat_name: chat_name.to_string(),
            since: Some(since),
            until: Some(until),
            limit: limit as usize,
            text_only: false,
            msg_types: vec!["text".to_string(), "image".to_string(), "voice".to_string()],
            media_decode_limit,
        },
    )
    .map_err(|error| Wx4pyError::WxCli(format!("builtin wxdb failed: {error:#}")))
}

fn builtin_wxdb_runtime_config(wx_cli: &WxCliConfig) -> wechat_summary_wxdb::RuntimeConfig {
    let config = wechat_summary_wxdb::RuntimeConfig::load();
    let cache_dir = wx_cli.cache_dir.trim();
    if cache_dir.is_empty() {
        config
    } else {
        config.with_cache_dir(cache_dir)
    }
}

fn should_skip_export_after_history_error(error: &Wx4pyError) -> bool {
    let error = error.to_string();
    error.contains("wx-daemon 启动超时")
}

fn query_text_messages_via_history(
    wx_cli: &WxCliConfig,
    chat_name: &str,
    since: DateTime<Utc>,
    until: DateTime<Utc>,
    limit: u32,
) -> Result<Vec<Wx4pyHistoryMessage>> {
    let mut last_error = None;
    let mut empty_messages = None;
    for candidate in wx_cli_chat_candidates(chat_name) {
        let cmd = build_wx_cli_history_command(wx_cli, &candidate, since, until, limit);
        let output_result = match run_wx_cli_command_with_timeout(&cmd, wx_cli.timeout_seconds) {
            Ok(output) => output,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                return Err(Wx4pyError::WxCli(format!(
                        "wxdb executable not found: {}. Set [wxdb].executable to builtin, wxdb.exe, or an external history command.",
                        cmd[0]
                    )));
            }
            Err(error) if error.kind() == std::io::ErrorKind::TimedOut => {
                return Err(Wx4pyError::WxCli(format!(
                    "wxdb history timed out after {} seconds",
                    wx_cli.timeout_seconds
                )));
            }
            Err(error) => return Err(Wx4pyError::Io(error)),
        };

        if !output_result.status.success() {
            last_error = Some(format!(
                "wxdb history failed for {candidate}: {}",
                command_error_text(&output_result)
            ));
            continue;
        }

        let text = String::from_utf8_lossy(&output_result.stdout);
        let mut messages = match normalize_wx_cli_messages(&text) {
            Ok(messages) => messages,
            Err(error) => {
                last_error = Some(format!(
                    "wxdb history returned invalid JSON for {candidate}: {error}"
                ));
                continue;
            }
        };
        messages.retain(|message| message.timestamp >= since && message.timestamp <= until);
        if messages.is_empty() {
            empty_messages = Some(messages);
            continue;
        }
        return Ok(messages);
    }

    if let Some(messages) = empty_messages {
        return Ok(messages);
    }

    Err(Wx4pyError::WxCli(last_error.unwrap_or_else(|| {
        "wxdb history failed without output".to_string()
    })))
}

fn query_text_messages_via_export(
    wx_cli: &WxCliConfig,
    chat_name: &str,
    since: DateTime<Utc>,
    until: DateTime<Utc>,
    limit: u32,
    output: &Path,
) -> Result<Vec<Wx4pyHistoryMessage>> {
    let mut last_error = None;
    let mut empty_messages = None;
    for candidate in wx_cli_chat_candidates(chat_name) {
        let _ = fs::remove_file(output);
        let cmd = build_wx_cli_export_command(wx_cli, &candidate, since, until, limit, output);
        let output_result = match run_wx_cli_command_with_timeout(&cmd, wx_cli.timeout_seconds) {
            Ok(output) => output,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                return Err(Wx4pyError::WxCli(format!(
                        "wxdb executable not found: {}. Set [wxdb].executable to builtin, wxdb.exe, or an external history command.",
                        cmd[0]
                    )));
            }
            Err(error) if error.kind() == std::io::ErrorKind::TimedOut => {
                return Err(Wx4pyError::WxCli(format!(
                    "wxdb export timed out after {} seconds",
                    wx_cli.timeout_seconds
                )));
            }
            Err(error) => return Err(Wx4pyError::Io(error)),
        };
        if !output_result.status.success() {
            last_error = Some(format!(
                "wxdb export failed for {candidate}: {}",
                command_error_text(&output_result)
            ));
            continue;
        }

        let text = fs::read_to_string(output)?;
        let _ = fs::remove_file(output);
        let mut messages = match normalize_wx_cli_messages(&text) {
            Ok(messages) => messages,
            Err(error) => {
                last_error = Some(format!(
                    "wxdb export returned invalid JSON for {candidate}: {error}"
                ));
                continue;
            }
        };
        messages.retain(|message| message.timestamp >= since && message.timestamp <= until);
        if messages.is_empty() {
            empty_messages = Some(messages);
            continue;
        }
        return Ok(messages);
    }

    if let Some(messages) = empty_messages {
        return Ok(messages);
    }

    Err(Wx4pyError::WxCli(last_error.unwrap_or_else(|| {
        "wxdb export failed without output".to_string()
    })))
}

fn wx_cli_chat_candidates(chat_name: &str) -> Vec<String> {
    vec![chat_name.to_string()]
}

fn history_query_timeout_seconds(config: &WxCliConfig) -> u64 {
    config
        .history_query_timeout_seconds
        .max(config.timeout_seconds.saturating_mul(2).saturating_add(10))
        .max(config.timeout_seconds.saturating_add(1))
        .max(1)
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq)]
#[serde(rename_all = "snake_case", tag = "kind")]
enum SidecarMessage {
    Ready { ok: bool },
    Event(Wx4pyEvent),
    CommandError { message: String },
    Error { message: String },
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "snake_case", tag = "cmd")]
enum SidecarCommand {
    SendText { room: String, text: String },
    SendImage { room: String, path: String },
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq)]
pub struct Wx4pyEvent {
    pub room_id: String,
    #[serde(default)]
    pub room_name: Option<String>,
    pub content: String,
    #[serde(default)]
    pub sender_id: Option<String>,
    #[serde(default)]
    pub sender_name: Option<String>,
    pub timestamp: i64,
}

impl Wx4pyEvent {
    pub fn timestamp(&self) -> Option<DateTime<Utc>> {
        Utc.timestamp_opt(self.timestamp, 0).single()
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct Wx4pyHistoryMessage {
    pub timestamp: DateTime<Utc>,
    pub sender_id: String,
    pub sender_name: Option<String>,
    pub content: String,
    pub msg_type: String,
    pub media_path: Option<String>,
    pub thumbnail_path: Option<String>,
    pub decoded_media_path: Option<String>,
    pub media_decode_error: Option<String>,
    pub is_self: bool,
}

pub fn build_wx_cli_export_command(
    config: &WxCliConfig,
    chat_name: &str,
    since: DateTime<Utc>,
    until: DateTime<Utc>,
    limit: u32,
    output: &Path,
) -> Vec<String> {
    vec![
        config.executable.clone(),
        "export".to_string(),
        chat_name.to_string(),
        "--since".to_string(),
        format_beijing_date(since),
        "--until".to_string(),
        format_beijing_date(until),
        "--format".to_string(),
        config.export_format.clone(),
        "-o".to_string(),
        output.to_string_lossy().to_string(),
        "-n".to_string(),
        limit.to_string(),
    ]
}

pub fn build_wx_cli_history_command(
    config: &WxCliConfig,
    chat_name: &str,
    since: DateTime<Utc>,
    until: DateTime<Utc>,
    limit: u32,
) -> Vec<String> {
    vec![
        config.executable.clone(),
        "history".to_string(),
        chat_name.to_string(),
        "--since".to_string(),
        format_beijing_date(since),
        "--until".to_string(),
        format_beijing_date(until),
        "--type".to_string(),
        "text".to_string(),
        "--json".to_string(),
        "-n".to_string(),
        limit.to_string(),
    ]
}

pub fn build_wx_cli_daemon_stop_command(config: &WxCliConfig) -> Vec<String> {
    vec![
        config.executable.clone(),
        "daemon".to_string(),
        "stop".to_string(),
    ]
}

static WX_CLI_COMMAND_LOCK: OnceLock<Mutex<()>> = OnceLock::new();
const WX_CLI_DAEMON_STOP_TIMEOUT_SECONDS: u64 = 5;

fn stop_wx_cli_daemon_on_start(wx_cli: &WxCliConfig) {
    if should_use_builtin_wxdb(wx_cli) {
        tracing::info!("builtin wxdb selected; no external wxdb daemon to stop");
        return;
    }
    let cmd = build_wx_cli_daemon_stop_command(wx_cli);
    match run_wx_cli_command_with_timeout(&cmd, WX_CLI_DAEMON_STOP_TIMEOUT_SECONDS) {
        Ok(output) if output.status.success() => {
            tracing::info!("stopped wxdb daemon before wx4py startup");
        }
        Ok(output) => {
            tracing::warn!(
                error = %command_error_text(&output),
                "wxdb daemon stop returned a non-success status before startup"
            );
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            tracing::warn!(
                executable = %wx_cli.executable,
                "wxdb executable not found while trying to stop stale daemon"
            );
        }
        Err(error) => {
            tracing::warn!(
                error = %error,
                "failed to stop wxdb daemon before startup"
            );
        }
    }
}

fn run_wx_cli_command_with_timeout(
    cmd: &[String],
    timeout_seconds: u64,
) -> std::io::Result<Output> {
    let _guard = WX_CLI_COMMAND_LOCK
        .get_or_init(|| Mutex::new(()))
        .lock()
        .map_err(|_| std::io::Error::other("wxdb command lock poisoned"))?;
    run_command_with_timeout(cmd, timeout_seconds)
}

fn run_command_with_timeout(cmd: &[String], timeout_seconds: u64) -> std::io::Result<Output> {
    let stdout_path = command_output_temp_path("stdout");
    let stderr_path = command_output_temp_path("stderr");
    let stdout_file = File::create(&stdout_path)?;
    let stderr_file = File::create(&stderr_path)?;
    let mut child = Command::new(&cmd[0])
        .args(&cmd[1..])
        .stdout(Stdio::from(stdout_file))
        .stderr(Stdio::from(stderr_file))
        .spawn()?;
    let child_id = child.id();
    tracing::debug!(
        pid = child_id,
        command = %command_for_log(cmd),
        timeout_seconds,
        "wxdb command started"
    );
    let timeout = StdDuration::from_secs(timeout_seconds.max(1));
    let started = Instant::now();
    loop {
        if let Some(status) = child.try_wait()? {
            return output_from_temp_files(status, &stdout_path, &stderr_path);
        }
        if started.elapsed() >= timeout {
            tracing::warn!(
                pid = child_id,
                command = %command_for_log(cmd),
                timeout_seconds = timeout.as_secs(),
                "wxdb command timed out; terminating process tree"
            );
            terminate_process_tree(&mut child);
            remove_temp_file(&stdout_path);
            remove_temp_file(&stderr_path);
            return Err(std::io::Error::new(
                std::io::ErrorKind::TimedOut,
                format!(
                    "command timed out after {} seconds: {}",
                    timeout.as_secs(),
                    cmd[0]
                ),
            ));
        }
        thread::sleep(StdDuration::from_millis(100));
    }
}

fn command_output_temp_path(stream_name: &str) -> PathBuf {
    let suffix = Utc::now()
        .timestamp_nanos_opt()
        .unwrap_or_else(|| Utc::now().timestamp_micros());
    std::env::temp_dir().join(format!(
        "wx-summary-agent-{}-{suffix}-{stream_name}.tmp",
        std::process::id()
    ))
}

fn output_from_temp_files(
    status: ExitStatus,
    stdout_path: &Path,
    stderr_path: &Path,
) -> std::io::Result<Output> {
    let stdout = read_temp_output(stdout_path)?;
    let stderr = read_temp_output(stderr_path)?;
    remove_temp_file(stdout_path);
    remove_temp_file(stderr_path);
    Ok(Output {
        status,
        stdout,
        stderr,
    })
}

fn read_temp_output(path: &Path) -> std::io::Result<Vec<u8>> {
    let started = Instant::now();
    loop {
        match fs::read(path) {
            Ok(output) => return Ok(output),
            Err(error) if started.elapsed() < StdDuration::from_secs(2) => {
                tracing::warn!(
                    path = %path.display(),
                    error = %error,
                    "wxdb temp output read failed; retrying briefly"
                );
                thread::sleep(StdDuration::from_millis(50));
            }
            Err(error) => return Err(error),
        }
    }
}

fn remove_temp_file(path: &Path) {
    let _ = fs::remove_file(path);
}

fn terminate_process_tree(child: &mut Child) {
    #[cfg(windows)]
    {
        let pid = child.id().to_string();
        let _ = Command::new("taskkill")
            .args(["/PID", &pid, "/T", "/F"])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status();
    }

    let _ = child.kill();
    let started = Instant::now();
    while started.elapsed() < StdDuration::from_secs(2) {
        match child.try_wait() {
            Ok(Some(_)) => return,
            Ok(None) => thread::sleep(StdDuration::from_millis(50)),
            Err(_) => return,
        }
    }
}

fn command_for_log(cmd: &[String]) -> String {
    cmd.join(" ")
}

fn command_error_text(output: &Output) -> String {
    let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
    if !stderr.is_empty() {
        return stderr;
    }
    let stdout = String::from_utf8_lossy(&output.stdout).trim().to_string();
    if !stdout.is_empty() {
        return stdout;
    }
    format!("exit status {}", output.status)
}

pub fn normalize_wx_cli_messages(text: &str) -> Result<Vec<Wx4pyHistoryMessage>> {
    let value: Value = serde_json::from_str(text)?;
    let messages = match value {
        Value::Array(items) => items,
        Value::Object(mut object) => object
            .remove("messages")
            .and_then(|value| value.as_array().cloned())
            .ok_or_else(|| Wx4pyError::InvalidWxCli("missing messages array".into()))?,
        _ => {
            return Err(Wx4pyError::InvalidWxCli(
                "root must be an array or object".into(),
            ))
        }
    };

    let mut normalized = messages
        .into_iter()
        .filter_map(|item| normalize_wx_cli_message(item).transpose())
        .collect::<Result<Vec<_>>>()?;
    normalized.sort_by_key(|message| message.timestamp);
    Ok(normalized)
}

fn normalize_wx_cli_message(value: Value) -> Result<Option<Wx4pyHistoryMessage>> {
    let Some(object) = value.as_object() else {
        return Ok(None);
    };
    let content = string_field(object, &["content", "text", "message"]).unwrap_or_default();
    if content.trim().is_empty() {
        return Ok(None);
    }

    let msg_type = string_field(object, &["type", "msg_type"]).unwrap_or_else(|| "text".into());
    if !is_supported_history_msg_type(&msg_type) {
        return Ok(None);
    }
    let normalized_type = normalize_history_msg_type(&msg_type);

    Ok(Some(Wx4pyHistoryMessage {
        timestamp: parse_timestamp(object)?,
        sender_id: string_field(object, &["sender_id", "sender_username", "sender"])
            .unwrap_or_else(|| "unknown".into()),
        sender_name: string_field(object, &["sender_name", "sender"]),
        content,
        msg_type: normalized_type,
        media_path: string_field(object, &["media_path", "path", "file_path"]),
        thumbnail_path: string_field(object, &["thumbnail_path", "thumb_path", "thumb"]),
        decoded_media_path: string_field(object, &["decoded_media_path", "decoded_path"]),
        media_decode_error: string_field(object, &["media_decode_error", "decode_error"]),
        is_self: bool_field(object, &["is_self", "isSender"]).unwrap_or(false),
    }))
}

fn listen_groups(config: &Wx4pyConfig, listen: &ListenConfig) -> Result<Vec<String>> {
    let groups = if config.groups.is_empty() {
        listen.whitelist_rooms.clone()
    } else {
        config.groups.clone()
    };
    let groups = groups
        .into_iter()
        .filter(|group| !group.trim().is_empty())
        .collect::<Vec<_>>();
    if groups.is_empty() {
        return Err(Wx4pyError::MissingGroups);
    }
    Ok(groups)
}

fn format_beijing_date(value: DateTime<Utc>) -> String {
    (value + Duration::hours(8)).format("%Y-%m-%d").to_string()
}

fn parse_timestamp(object: &serde_json::Map<String, Value>) -> Result<DateTime<Utc>> {
    if let Some(value) = object
        .get("timestamp")
        .or_else(|| object.get("create_time"))
        .or_else(|| object.get("CreateTime"))
        .and_then(Value::as_i64)
    {
        let value = if value > 10_000_000_000 {
            value / 1000
        } else {
            value
        };
        return Utc
            .timestamp_opt(value, 0)
            .single()
            .ok_or_else(|| Wx4pyError::InvalidWxCli(format!("invalid timestamp value: {value}")));
    }

    let text = string_field(object, &["datetime", "time", "created_at"])
        .ok_or_else(|| Wx4pyError::InvalidWxCli("missing timestamp".into()))?;
    parse_datetime_text(&text)
}

fn parse_datetime_text(text: &str) -> Result<DateTime<Utc>> {
    if let Ok(value) = DateTime::parse_from_rfc3339(text) {
        return Ok(value.with_timezone(&Utc));
    }
    let naive = NaiveDateTime::parse_from_str(text, "%Y-%m-%d %H:%M:%S")
        .map_err(|_| Wx4pyError::InvalidWxCli(format!("unsupported timestamp: {text}")))?;
    Ok(Utc.from_utc_datetime(&naive) - Duration::hours(8))
}

fn string_field(object: &serde_json::Map<String, Value>, keys: &[&str]) -> Option<String> {
    keys.iter().find_map(|key| {
        object.get(*key).and_then(|value| match value {
            Value::String(text) => Some(text.clone()),
            Value::Number(number) => Some(number.to_string()),
            _ => None,
        })
    })
}

fn bool_field(object: &serde_json::Map<String, Value>, keys: &[&str]) -> Option<bool> {
    keys.iter().find_map(|key| {
        object.get(*key).and_then(|value| match value {
            Value::Bool(value) => Some(*value),
            Value::Number(number) => number.as_i64().map(|value| value != 0),
            Value::String(text) => match text.as_str() {
                "1" | "true" | "True" => Some(true),
                "0" | "false" | "False" => Some(false),
                _ => None,
            },
            _ => None,
        })
    })
}

fn is_supported_history_msg_type(value: &str) -> bool {
    matches!(
        value.trim().to_ascii_lowercase().as_str(),
        "text" | "1" | "文本" | "文字" | "image" | "img" | "3" | "图片" | "voice" | "语音" | "34"
    )
}

fn normalize_history_msg_type(value: &str) -> String {
    match value.trim().to_ascii_lowercase().as_str() {
        "image" | "img" | "3" | "图片" => "image".to_string(),
        "voice" | "语音" | "34" => "voice".to_string(),
        _ => "text".to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    fn wx_cli_config() -> WxCliConfig {
        WxCliConfig {
            executable: "wx".into(),
            export_format: "json".into(),
            max_messages: None,
            timeout_seconds: 20,
            history_query_timeout_seconds: 45,
            temp_dir: ".\\runtime\\wx-exports".into(),
            cache_dir: String::new(),
            group_name_map: HashMap::new(),
        }
    }

    #[test]
    fn builds_wx_cli_history_command_with_beijing_time_and_text_filter() {
        let cmd = build_wx_cli_history_command(
            &wx_cli_config(),
            "测试群",
            Utc.with_ymd_and_hms(2026, 5, 24, 1, 0, 0).unwrap(),
            Utc.with_ymd_and_hms(2026, 5, 24, 2, 0, 0).unwrap(),
            100,
        );

        assert_eq!(cmd[0], "wx");
        assert_eq!(cmd[1], "history");
        assert_eq!(cmd[2], "测试群");
        assert!(cmd.contains(&"2026-05-24".to_string()));
        assert!(cmd.contains(&"--json".to_string()));
        assert!(cmd.contains(&"--type".to_string()));
        assert!(cmd.contains(&"text".to_string()));
    }

    #[test]
    fn builds_wx_cli_daemon_stop_command() {
        let cmd = build_wx_cli_daemon_stop_command(&wx_cli_config());

        assert_eq!(cmd, ["wx", "daemon", "stop"]);
    }

    #[test]
    fn history_timeout_can_fall_back_to_export() {
        let error = Wx4pyError::WxCli("wxdb history timed out after 20 seconds".into());

        assert!(!should_skip_export_after_history_error(&error));
    }

    #[test]
    fn total_history_timeout_covers_history_and_export_commands() {
        let mut config = wx_cli_config();
        config.timeout_seconds = 20;
        config.history_query_timeout_seconds = 45;

        assert_eq!(history_query_timeout_seconds(&config), 50);
    }

    #[test]
    fn builds_wx_cli_export_command_with_beijing_time() {
        let cmd = build_wx_cli_export_command(
            &wx_cli_config(),
            "测试群",
            Utc.with_ymd_and_hms(2026, 5, 24, 1, 0, 0).unwrap(),
            Utc.with_ymd_and_hms(2026, 5, 24, 2, 0, 0).unwrap(),
            100,
            Path::new("out.json"),
        );

        assert_eq!(cmd[0], "wx");
        assert_eq!(cmd[1], "export");
        assert_eq!(cmd[2], "测试群");
        assert!(cmd.contains(&"2026-05-24".to_string()));
    }

    #[test]
    fn normalizes_wx_cli_messages_from_object_payload() {
        let payload = r#"{
            "messages": [
                {
                    "timestamp": 1716464700,
                    "sender": "Alice",
                    "content": "hello",
                    "type": "text"
                }
            ]
        }"#;

        let messages = normalize_wx_cli_messages(payload).unwrap();

        assert_eq!(messages.len(), 1);
        assert_eq!(messages[0].sender_id, "Alice");
        assert_eq!(messages[0].content, "hello");
    }

    #[test]
    fn normalizes_wx_cli_chinese_text_type() {
        let payload = r#"{
            "messages": [
                {
                    "timestamp": 1780142448,
                    "sender": "fbm",
                    "content": "这段时间没有可总结的文本聊天记录。",
                    "type": "文本"
                }
            ]
        }"#;

        let messages = normalize_wx_cli_messages(payload).unwrap();

        assert_eq!(messages.len(), 1);
        assert_eq!(messages[0].sender_id, "fbm");
        assert_eq!(messages[0].msg_type, "text");
    }

    #[test]
    fn normalizes_wx_cli_image_messages_with_media_paths() {
        let payload = r#"{
            "messages": [
                {
                    "timestamp": 1780471359,
                    "sender": "muzimi",
                    "content": "[图片] local_id=26032",
                    "type": "image",
                    "media_path": "D:\\Temp\\image.dat",
                    "thumbnail_path": "D:\\Temp\\image_t.dat",
                    "decoded_media_path": "D:\\Temp\\decoded.jpg"
                }
            ]
        }"#;

        let messages = normalize_wx_cli_messages(payload).unwrap();

        assert_eq!(messages.len(), 1);
        assert_eq!(messages[0].msg_type, "image");
        assert_eq!(messages[0].content, "[图片] local_id=26032");
        assert_eq!(
            messages[0].media_path.as_deref(),
            Some(r"D:\Temp\image.dat")
        );
        assert_eq!(
            messages[0].thumbnail_path.as_deref(),
            Some(r"D:\Temp\image_t.dat")
        );
        assert_eq!(
            messages[0].decoded_media_path.as_deref(),
            Some(r"D:\Temp\decoded.jpg")
        );
    }

    #[test]
    fn parses_local_wx_cli_datetime_as_beijing_time() {
        let payload = r#"[{
            "time": "2026-05-24 09:00:00",
            "sender": "Alice",
            "content": "hello",
            "type": "text"
        }]"#;

        let messages = normalize_wx_cli_messages(payload).unwrap();

        assert_eq!(
            messages[0].timestamp,
            Utc.with_ymd_and_hms(2026, 5, 24, 1, 0, 0).unwrap()
        );
    }

    #[test]
    fn parses_millisecond_timestamps_and_numeric_text_type() {
        let payload = r#"[{
            "timestamp": 1780057200000,
            "sender": "Alice",
            "content": "hello",
            "type": 1
        }]"#;

        let messages = normalize_wx_cli_messages(payload).unwrap();

        assert_eq!(messages.len(), 1);
        assert_eq!(
            messages[0].timestamp,
            Utc.with_ymd_and_hms(2026, 5, 29, 12, 20, 0).unwrap()
        );
    }

    #[test]
    fn wx_cli_chat_candidates_use_requested_chat_name_only() {
        assert_eq!(wx_cli_chat_candidates("脑机2galgame"), vec!["脑机2galgame"]);
    }
}
