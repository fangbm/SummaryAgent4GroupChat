use std::{
    collections::VecDeque,
    fs::{self, File},
    io::{BufRead, BufReader, Write},
    path::{Path, PathBuf},
    process::{Child, ChildStdin, Command, ExitStatus, Output, Stdio},
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        mpsc::{self, Receiver, RecvTimeoutError, Sender},
        Arc, Mutex, MutexGuard, OnceLock, TryLockError,
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

#[derive(Debug)]
pub struct Wx4pyClient {
    child: Option<Child>,
    transport: Arc<Wx4pyTransport>,
    receiver: Receiver<SidecarMessage>,
    pending_events: Mutex<VecDeque<Wx4pyEvent>>,
    wx_cli: WxCliConfig,
}

#[derive(Debug, Clone)]
pub struct Wx4pySender {
    transport: Arc<Wx4pyTransport>,
}

#[derive(Debug, Clone)]
pub struct Wx4pyHistoryReader {
    wx_cli: WxCliConfig,
}

#[derive(Debug)]
struct Wx4pyTransport {
    stdin: Mutex<Option<ChildStdin>>,
    pending: Mutex<std::collections::HashMap<String, Sender<std::result::Result<(), String>>>>,
    closed: AtomicBool,
    next_request_id: AtomicU64,
    command_timeout: StdDuration,
}

impl Wx4pyClient {
    pub fn start(
        config: &Wx4pyConfig,
        listen: &ListenConfig,
        wx_cli: &WxCliConfig,
    ) -> Result<Self> {
        let groups = listen_groups(config, listen)?;
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

        let stdin = match child.stdin.take() {
            Some(stdin) => stdin,
            None => {
                terminate_and_reap_child(&mut child)?;
                return Err(Wx4pyError::Sidecar("failed to open sidecar stdin".into()));
            }
        };
        let stdout = match child.stdout.take() {
            Some(stdout) => stdout,
            None => {
                terminate_and_reap_child(&mut child)?;
                return Err(Wx4pyError::Sidecar("failed to open sidecar stdout".into()));
            }
        };
        let (sender, receiver) = mpsc::channel();
        let transport = Arc::new(Wx4pyTransport {
            stdin: Mutex::new(Some(stdin)),
            pending: Mutex::new(std::collections::HashMap::new()),
            closed: AtomicBool::new(false),
            next_request_id: AtomicU64::new(1),
            command_timeout: StdDuration::from_secs(config.command_timeout_seconds.max(1)),
        });
        let pending = Arc::clone(&transport);

        thread::spawn(move || {
            let mut reader = BufReader::new(stdout);
            let mut buffer = Vec::new();
            loop {
                buffer.clear();
                match reader.read_until(b'\n', &mut buffer) {
                    Ok(0) => {
                        pending.close("wx4py sidecar stdout closed");
                        break;
                    }
                    Ok(_) => {
                        let line = String::from_utf8_lossy(&buffer);
                        let line = line.trim();
                        if line.is_empty() {
                            continue;
                        }
                        match serde_json::from_str::<SidecarMessage>(line) {
                            Ok(message) => {
                                if !dispatch_sidecar_message(message, &pending.pending, &sender) {
                                    break;
                                }
                            }
                            Err(error) => {
                                eprintln!("ignored wx4py sidecar stdout line: {error}: {line}");
                            }
                        }
                    }
                    Err(error) => {
                        pending.close(format!("wx4py sidecar stdout error: {error}"));
                        let _ = sender.send(SidecarMessage::Error {
                            message: format!("sidecar stdout error: {error}"),
                        });
                        break;
                    }
                }
            }
        });

        let client = Self {
            child: Some(child),
            transport,
            receiver,
            pending_events: Mutex::new(VecDeque::new()),
            wx_cli: wx_cli.clone(),
        };
        client.wait_ready(config.ready_timeout_seconds)?;
        Ok(client)
    }

    pub fn next_event(&self) -> Result<Wx4pyEvent> {
        if let Some(event) = self.take_pending_event()? {
            return Ok(event);
        }
        loop {
            match self.receiver.recv() {
                Ok(SidecarMessage::Event(event)) => return Ok(event),
                Ok(SidecarMessage::Ready { .. }) => continue,
                Ok(SidecarMessage::CommandError {
                    request_id,
                    message,
                }) => {
                    return Err(Wx4pyError::Sidecar(format!(
                        "wx4py sidecar command error request_id={request_id}: {message}"
                    )));
                }
                Ok(SidecarMessage::CommandResult { request_id, ok }) => {
                    return Err(Wx4pyError::Sidecar(format!(
                        "unexpected wx4py command ACK request_id={request_id} ok={ok}"
                    )));
                }
                Ok(SidecarMessage::Error { message }) => return Err(Wx4pyError::Sidecar(message)),
                Err(error) => return Err(Wx4pyError::Sidecar(error.to_string())),
            }
        }
    }

    pub fn next_event_timeout(&self, timeout: StdDuration) -> Result<Option<Wx4pyEvent>> {
        if let Some(event) = self.take_pending_event()? {
            return Ok(Some(event));
        }
        let deadline = Instant::now() + timeout;
        loop {
            let remaining = deadline.saturating_duration_since(Instant::now());
            match self.receiver.recv_timeout(remaining) {
                Ok(SidecarMessage::Event(event)) => return Ok(Some(event)),
                Ok(SidecarMessage::Ready { .. }) => continue,
                Ok(SidecarMessage::CommandError {
                    request_id,
                    message,
                }) => {
                    return Err(Wx4pyError::Sidecar(format!(
                        "wx4py sidecar command error request_id={request_id}: {message}"
                    )));
                }
                Ok(SidecarMessage::CommandResult { request_id, ok }) => {
                    return Err(Wx4pyError::Sidecar(format!(
                        "unexpected wx4py command ACK request_id={request_id} ok={ok}"
                    )));
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

    #[allow(clippy::too_many_arguments)]
    pub async fn query_text_messages(
        &self,
        room_id: &str,
        room_name: Option<&str>,
        since: DateTime<Utc>,
        until: DateTime<Utc>,
        limit: u32,
        media_decode_limit: Option<usize>,
    ) -> Result<Vec<Wx4pyHistoryMessage>> {
        self.history_reader()
            .query_text_messages(
                room_id,
                room_name,
                since,
                until,
                limit,
                media_decode_limit,
                None,
            )
            .await
    }

    pub fn history_reader(&self) -> Wx4pyHistoryReader {
        Wx4pyHistoryReader {
            wx_cli: self.wx_cli.clone(),
        }
    }

    fn wait_ready(&self, timeout_seconds: u64) -> Result<()> {
        wait_ready(&self.receiver, &self.pending_events, timeout_seconds)
    }

    fn take_pending_event(&self) -> Result<Option<Wx4pyEvent>> {
        self.pending_events
            .lock()
            .map(|mut events| events.pop_front())
            .map_err(|_| Wx4pyError::Sidecar("pending wx4py event mutex poisoned".into()))
    }

    pub fn sender(&self) -> Wx4pySender {
        Wx4pySender {
            transport: Arc::clone(&self.transport),
        }
    }

    pub fn shutdown(&mut self) -> Result<()> {
        self.transport.shutdown();
        let Some(mut child) = self.child.take() else {
            return Ok(());
        };
        terminate_and_reap_child(&mut child)?;
        Ok(())
    }
}

impl Drop for Wx4pyClient {
    fn drop(&mut self) {
        let _ = self.shutdown();
    }
}

fn wait_ready(
    receiver: &Receiver<SidecarMessage>,
    pending_events: &Mutex<VecDeque<Wx4pyEvent>>,
    timeout_seconds: u64,
) -> Result<()> {
    let deadline = Instant::now() + StdDuration::from_secs(timeout_seconds);
    loop {
        let remaining = deadline.saturating_duration_since(Instant::now());
        match receiver.recv_timeout(remaining) {
            Ok(SidecarMessage::Ready { ok: true }) => return Ok(()),
            Ok(SidecarMessage::Ready { ok: false }) => {
                return Err(Wx4pyError::Sidecar(
                    "wx4py sidecar reported not ready".to_string(),
                ));
            }
            Ok(SidecarMessage::CommandError {
                request_id,
                message,
            }) => {
                return Err(Wx4pyError::Sidecar(format!(
                    "command error request_id={request_id}: {message}"
                )));
            }
            Ok(SidecarMessage::CommandResult { request_id, ok }) => {
                return Err(Wx4pyError::Sidecar(format!(
                    "unexpected command result request_id={request_id} ok={ok}"
                )));
            }
            Ok(SidecarMessage::Event(event)) => {
                pending_events
                    .lock()
                    .map_err(|_| Wx4pyError::Sidecar("pending wx4py event mutex poisoned".into()))?
                    .push_back(event);
            }
            Ok(SidecarMessage::Error { message }) => {
                return Err(Wx4pyError::Sidecar(message));
            }
            Err(RecvTimeoutError::Timeout) => {
                return Err(Wx4pyError::ReadyTimeout(timeout_seconds));
            }
            Err(RecvTimeoutError::Disconnected) => {
                return Err(Wx4pyError::Sidecar(
                    "wx4py sidecar exited before ready".to_string(),
                ));
            }
        }
    }
}

impl Wx4pyHistoryReader {
    #[allow(clippy::too_many_arguments)]
    pub async fn query_text_messages(
        &self,
        room_id: &str,
        room_name: Option<&str>,
        since: DateTime<Utc>,
        until: DateTime<Utc>,
        limit: u32,
        media_decode_limit: Option<usize>,
        before_local_id: Option<i64>,
    ) -> Result<Vec<Wx4pyHistoryMessage>> {
        let chat_name = self.chat_name(room_id, room_name);
        let output = self.temp_output_path(room_id);
        let wx_cli = self.wx_cli.clone();
        let total_timeout_seconds = history_query_timeout_seconds(&wx_cli);
        let started = Instant::now();
        tracing::debug!(
            room_id,
            chat_name,
            %since,
            %until,
            limit,
            media_decode_limit = ?media_decode_limit,
            timeout_seconds = total_timeout_seconds,
            "wxdb history query queued"
        );
        let result = tokio::task::spawn_blocking(move || {
            query_text_messages_inner(
                &wx_cli,
                &chat_name,
                since,
                until,
                limit,
                media_decode_limit,
                before_local_id,
                &output,
            )
        })
        .await
        .map_err(|error| Wx4pyError::WxCli(format!("wxdb query task failed: {error}")))?;
        match &result {
            Ok(messages) => tracing::debug!(
                room_id,
                count = messages.len(),
                elapsed_ms = started.elapsed().as_millis(),
                "wxdb history query completed"
            ),
            Err(error) => tracing::warn!(
                room_id,
                error = %error,
                elapsed_ms = started.elapsed().as_millis(),
                "wxdb history query failed"
            ),
        }
        result
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

impl Wx4pySender {
    pub async fn send_text(&self, room_id: &str, text: &str) -> Result<()> {
        self.send_command(SidecarCommand::SendText {
            request_id: String::new(),
            room: room_id.to_string(),
            text: text.to_string(),
        })
        .await
    }

    pub async fn send_image(&self, room_id: &str, image_path: &str) -> Result<()> {
        self.send_command(SidecarCommand::SendImage {
            request_id: String::new(),
            room: room_id.to_string(),
            path: image_path.to_string(),
        })
        .await
    }

    pub async fn send_file(&self, room_id: &str, file_path: &str) -> Result<()> {
        self.send_command(SidecarCommand::SendFile {
            request_id: String::new(),
            room: room_id.to_string(),
            path: file_path.to_string(),
        })
        .await
    }

    async fn send_command(&self, mut command: SidecarCommand) -> Result<()> {
        let transport = Arc::clone(&self.transport);
        let request_id = format!(
            "wx4py-{}",
            transport.next_request_id.fetch_add(1, Ordering::Relaxed)
        );
        command.set_request_id(request_id);
        tokio::task::spawn_blocking(move || transport.send_command_blocking(command))
            .await
            .map_err(|error| Wx4pyError::Sidecar(format!("send task failed: {error}")))?
    }
}

#[allow(clippy::too_many_arguments)]
fn query_text_messages_inner(
    wx_cli: &WxCliConfig,
    chat_name: &str,
    since: DateTime<Utc>,
    until: DateTime<Utc>,
    limit: u32,
    media_decode_limit: Option<usize>,
    before_local_id: Option<i64>,
    output: &Path,
) -> Result<Vec<Wx4pyHistoryMessage>> {
    if let Some(parent) = output.parent() {
        fs::create_dir_all(parent)?;
    }
    let deadline = Instant::now() + StdDuration::from_secs(history_query_timeout_seconds(wx_cli));
    tracing::debug!(
        chat_name,
        %since,
        %until,
        limit,
        "querying wxdb history"
    );
    match query_text_messages_via_history(
        wx_cli,
        chat_name,
        since,
        until,
        limit,
        media_decode_limit,
        before_local_id,
        deadline,
    ) {
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

    query_text_messages_via_export(
        wx_cli,
        chat_name,
        since,
        until,
        limit,
        before_local_id,
        deadline,
        output,
    )
}

#[allow(clippy::too_many_arguments)]
pub fn query_external_history_page(
    wx_cli: &WxCliConfig,
    chat_name: &str,
    since: DateTime<Utc>,
    until: DateTime<Utc>,
    limit: u32,
    media_decode_limit: Option<usize>,
    before_local_id: Option<i64>,
) -> Result<Vec<Wx4pyHistoryMessage>> {
    let output = PathBuf::from(&wx_cli.temp_dir).join(format!(
        "summary-agent-history-{}-{}.json",
        std::process::id(),
        Utc::now().timestamp_millis()
    ));
    query_text_messages_inner(
        wx_cli,
        chat_name,
        since,
        until,
        limit,
        media_decode_limit,
        before_local_id,
        &output,
    )
}

fn should_skip_export_after_history_error(error: &Wx4pyError) -> bool {
    let error = error.to_string();
    error.contains("wx-daemon 启动超时")
}

#[allow(clippy::too_many_arguments)]
fn query_text_messages_via_history(
    wx_cli: &WxCliConfig,
    chat_name: &str,
    since: DateTime<Utc>,
    until: DateTime<Utc>,
    limit: u32,
    media_decode_limit: Option<usize>,
    before_local_id: Option<i64>,
    deadline: Instant,
) -> Result<Vec<Wx4pyHistoryMessage>> {
    let mut last_error = None;
    let mut empty_messages = None;
    for candidate in wx_cli_chat_candidates(chat_name) {
        let mut cmd =
            build_wx_cli_history_command(wx_cli, &candidate, since, until, limit, before_local_id);
        if let Some(media_decode_limit) = media_decode_limit {
            cmd.extend([
                "--media-decode-limit".to_string(),
                media_decode_limit.to_string(),
            ]);
        }
        let output_result = match run_wx_cli_command_until(&cmd, deadline, wx_cli) {
            Ok(output) => output,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                return Err(Wx4pyError::WxCli(format!(
                        "external history provider executable not found: {}. Install the provider and set [wxdb].executable to its path.",
                        cmd[0]
                    )));
            }
            Err(error) if error.kind() == std::io::ErrorKind::TimedOut => {
                return Err(Wx4pyError::HistoryQueryTimeout(
                    history_query_timeout_seconds(wx_cli),
                ));
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
        messages
            .retain(|message| message_in_history_window(message, since, until, before_local_id));
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

#[allow(clippy::too_many_arguments)]
fn query_text_messages_via_export(
    wx_cli: &WxCliConfig,
    chat_name: &str,
    since: DateTime<Utc>,
    until: DateTime<Utc>,
    limit: u32,
    before_local_id: Option<i64>,
    deadline: Instant,
    output: &Path,
) -> Result<Vec<Wx4pyHistoryMessage>> {
    if before_local_id.is_some() {
        return Err(Wx4pyError::WxCli(
            "wxdb export does not support --before-local-id; refusing cursor pagination because the export fallback could omit messages".into(),
        ));
    }
    let mut last_error = None;
    let mut empty_messages = None;
    for candidate in wx_cli_chat_candidates(chat_name) {
        let _ = fs::remove_file(output);
        let cmd = build_wx_cli_export_command(wx_cli, &candidate, since, until, limit, output);
        let output_result = match run_wx_cli_command_until(&cmd, deadline, wx_cli) {
            Ok(output) => output,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                return Err(Wx4pyError::WxCli(format!(
                        "external history provider executable not found: {}. Install the provider and set [wxdb].executable to its path.",
                        cmd[0]
                    )));
            }
            Err(error) if error.kind() == std::io::ErrorKind::TimedOut => {
                return Err(Wx4pyError::HistoryQueryTimeout(
                    history_query_timeout_seconds(wx_cli),
                ));
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
        messages
            .retain(|message| message_in_history_window(message, since, until, before_local_id));
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

fn message_in_history_window(
    message: &Wx4pyHistoryMessage,
    since: DateTime<Utc>,
    until: DateTime<Utc>,
    before_local_id: Option<i64>,
) -> bool {
    if message.timestamp < since || message.timestamp > until {
        return false;
    }
    if message.timestamp == until {
        return match before_local_id {
            Some(before_local_id) => message
                .local_id
                .is_some_and(|local_id| local_id < before_local_id),
            None => true,
        };
    }
    true
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
    CommandResult { request_id: String, ok: bool },
    CommandError { request_id: String, message: String },
    Error { message: String },
}

fn dispatch_sidecar_message(
    message: SidecarMessage,
    pending: &Mutex<std::collections::HashMap<String, Sender<std::result::Result<(), String>>>>,
    events: &Sender<SidecarMessage>,
) -> bool {
    match message {
        SidecarMessage::CommandResult { request_id, ok } => {
            let result = if ok {
                Ok(())
            } else {
                Err("sidecar reported command failure".to_string())
            };
            let waiter = pending
                .lock()
                .ok()
                .and_then(|mut waiters| waiters.remove(&request_id));
            if let Some(waiter) = waiter {
                let _ = waiter.send(result);
            } else {
                tracing::warn!(request_id, "ignored late or unknown wx4py command ACK");
            }
            true
        }
        SidecarMessage::CommandError {
            request_id,
            message,
        } => {
            let waiter = pending
                .lock()
                .ok()
                .and_then(|mut waiters| waiters.remove(&request_id));
            if let Some(waiter) = waiter {
                let _ = waiter.send(Err(message));
            } else {
                tracing::warn!(request_id, "ignored late or unknown wx4py command error");
            }
            true
        }
        event => events.send(event).is_ok(),
    }
}

#[derive(Debug, Clone, Serialize)]
#[allow(clippy::enum_variant_names)]
#[serde(rename_all = "snake_case", tag = "cmd")]
enum SidecarCommand {
    SendText {
        request_id: String,
        room: String,
        text: String,
    },
    SendImage {
        request_id: String,
        room: String,
        path: String,
    },
    SendFile {
        request_id: String,
        room: String,
        path: String,
    },
}

impl SidecarCommand {
    fn set_request_id(&mut self, request_id: String) {
        match self {
            Self::SendText { request_id: id, .. }
            | Self::SendImage { request_id: id, .. }
            | Self::SendFile { request_id: id, .. } => *id = request_id,
        }
    }
}

impl Wx4pyTransport {
    fn shutdown(&self) {
        self.close("wx4py sidecar client is shut down");
    }

    fn close(&self, message: impl Into<String>) {
        self.closed.store(true, Ordering::Release);
        self.fail_pending(message);
    }

    fn fail_pending(&self, message: impl Into<String>) {
        let waiters = self
            .pending
            .lock()
            .map(|mut waiters| {
                waiters
                    .drain()
                    .map(|(_, sender)| sender)
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        let message = message.into();
        for waiter in waiters {
            let _ = waiter.send(Err(message.clone()));
        }
    }

    fn send_command_blocking(&self, command: SidecarCommand) -> Result<()> {
        if self.closed.load(Ordering::Acquire) {
            return Err(Wx4pyError::Sidecar(
                "wx4py sidecar client is shut down".into(),
            ));
        }
        let request_id = match &command {
            SidecarCommand::SendText { request_id, .. }
            | SidecarCommand::SendImage { request_id, .. }
            | SidecarCommand::SendFile { request_id, .. } => request_id.clone(),
        };
        let (sender, receiver) = mpsc::channel();
        let pending_commands = {
            let mut pending = self
                .pending
                .lock()
                .map_err(|_| Wx4pyError::Sidecar("pending ACK mutex poisoned".into()))?;
            if self.closed.load(Ordering::Acquire) {
                return Err(Wx4pyError::Sidecar(
                    "wx4py sidecar client is shut down".into(),
                ));
            }
            pending.insert(request_id.clone(), sender);
            pending.len()
        };
        let write_result = (|| -> Result<()> {
            if self.closed.load(Ordering::Acquire) {
                return Err(Wx4pyError::Sidecar(
                    "wx4py sidecar client is shut down".into(),
                ));
            }
            let mut stdin = self
                .stdin
                .lock()
                .map_err(|_| Wx4pyError::Sidecar("sidecar stdin mutex poisoned".into()))?;
            let stdin = stdin
                .as_mut()
                .ok_or_else(|| Wx4pyError::Sidecar("wx4py sidecar stdin is closed".into()))?;
            serde_json::to_writer(&mut *stdin, &command)?;
            stdin.write_all(b"\n")?;
            stdin.flush()?;
            Ok(())
        })();
        if let Err(error) = write_result {
            let _ = self
                .pending
                .lock()
                .map(|mut waiters| waiters.remove(&request_id));
            return Err(error);
        }
        let timeout = queue_aware_command_timeout(self.command_timeout, pending_commands);
        tracing::debug!(
            request_id,
            pending_commands,
            timeout_seconds = timeout.as_secs(),
            "waiting for wx4py command ACK"
        );
        receive_command_ack(receiver, &self.pending, &request_id, timeout)
    }
}

fn queue_aware_command_timeout(base_timeout: StdDuration, pending_commands: usize) -> StdDuration {
    let multiplier = pending_commands.max(1).min(u32::MAX as usize) as u32;
    base_timeout
        .checked_mul(multiplier)
        .unwrap_or(StdDuration::MAX)
}

fn receive_command_ack(
    receiver: Receiver<std::result::Result<(), String>>,
    pending: &Mutex<std::collections::HashMap<String, Sender<std::result::Result<(), String>>>>,
    request_id: &str,
    timeout: StdDuration,
) -> Result<()> {
    match receiver.recv_timeout(timeout) {
        Ok(Ok(())) => Ok(()),
        Ok(Err(message)) => Err(Wx4pyError::Sidecar(format!(
            "command request_id={request_id} failed: {message}"
        ))),
        Err(RecvTimeoutError::Timeout) => {
            let _ = pending.lock().map(|mut waiters| waiters.remove(request_id));
            Err(Wx4pyError::Sidecar(format!(
                    "command request_id={request_id} ACK timeout after {} seconds; delivery indeterminate (the sidecar may have completed it); do not automatically retry",
                    timeout.as_secs()
                )))
        }
        Err(RecvTimeoutError::Disconnected) => {
            let _ = pending.lock().map(|mut waiters| waiters.remove(request_id));
            Err(Wx4pyError::Sidecar(format!(
                "command request_id={request_id} ACK dispatcher disconnected; delivery indeterminate (the sidecar may have completed it); do not automatically retry"
            )))
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq)]
pub struct Wx4pyEvent {
    pub room_id: String,
    #[serde(default)]
    pub room_name: Option<String>,
    #[serde(default)]
    pub stable_id: Option<String>,
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
    pub local_id: Option<i64>,
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
    before_local_id: Option<i64>,
) -> Vec<String> {
    let mut command = vec![
        config.executable.clone(),
        "history".to_string(),
        chat_name.to_string(),
        "--since".to_string(),
        format_beijing_date(since),
        "--until".to_string(),
        format_beijing_date(until),
        "--type".to_string(),
        "all".to_string(),
        "--json".to_string(),
        "-n".to_string(),
        limit.to_string(),
    ];
    if let Some(local_id) = before_local_id {
        command.extend(["--before-local-id".to_string(), local_id.to_string()]);
    }
    command
}

static WX_CLI_COMMAND_LOCK: OnceLock<Mutex<()>> = OnceLock::new();

fn run_wx_cli_command_until(
    cmd: &[String],
    deadline: Instant,
    wx_cli: &WxCliConfig,
) -> std::io::Result<Output> {
    let lock = WX_CLI_COMMAND_LOCK.get_or_init(|| Mutex::new(()));
    run_command_with_deadline_on_lock(lock, cmd, deadline, &wxdb_provider_env(wx_cli))
}

fn run_command_with_deadline_on_lock(
    lock: &Mutex<()>,
    cmd: &[String],
    deadline: Instant,
    envs: &[(String, String)],
) -> std::io::Result<Output> {
    let _guard = lock_with_deadline(lock, deadline)?;
    let remaining = deadline.saturating_duration_since(Instant::now());
    if remaining.is_zero() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::TimedOut,
            "wxdb command deadline elapsed while waiting for the command lock",
        ));
    }
    run_command_with_timeout_env(cmd, remaining, envs)
}

fn wxdb_provider_env(wx_cli: &WxCliConfig) -> Vec<(String, String)> {
    let mut envs = Vec::new();
    if let Some(db_dir) = wx_cli
        .db_dir
        .as_deref()
        .filter(|value| !value.trim().is_empty())
    {
        envs.push(("WXDB_DB_DIR".to_string(), db_dir.trim().to_string()));
    }
    if !wx_cli.cache_dir.trim().is_empty() {
        envs.push((
            "WXDB_CACHE_DIR".to_string(),
            wx_cli.cache_dir.trim().to_string(),
        ));
    }
    envs
}

fn lock_with_deadline<'a>(
    lock: &'a Mutex<()>,
    deadline: Instant,
) -> std::io::Result<MutexGuard<'a, ()>> {
    loop {
        match lock.try_lock() {
            Ok(guard) => return Ok(guard),
            Err(TryLockError::Poisoned(_)) => {
                return Err(std::io::Error::other("wxdb command lock poisoned"));
            }
            Err(TryLockError::WouldBlock) => {
                let remaining = deadline.saturating_duration_since(Instant::now());
                if remaining.is_zero() {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::TimedOut,
                        "wxdb command deadline elapsed while waiting for the command lock",
                    ));
                }
                thread::sleep(remaining.min(StdDuration::from_millis(10)));
            }
        }
    }
}

fn run_command_with_timeout_env(
    cmd: &[String],
    timeout: StdDuration,
    envs: &[(String, String)],
) -> std::io::Result<Output> {
    let stdout_path = command_output_temp_path("stdout");
    let stderr_path = command_output_temp_path("stderr");
    let stdout_file = File::create(&stdout_path)?;
    let stderr_file = File::create(&stderr_path)?;
    let mut command = Command::new(&cmd[0]);
    command.args(&cmd[1..]);
    for (name, value) in envs {
        command.env(name, value);
    }
    let mut child = command
        .stdout(Stdio::from(stdout_file))
        .stderr(Stdio::from(stderr_file))
        .spawn()?;
    let child_id = child.id();
    tracing::debug!(
        pid = child_id,
        command = %command_for_log(cmd),
        timeout_ms = timeout.as_millis(),
        "wxdb command started"
    );
    let started = Instant::now();
    loop {
        if let Some(status) = child.try_wait()? {
            return output_from_temp_files(status, &stdout_path, &stderr_path);
        }
        if started.elapsed() >= timeout {
            tracing::warn!(
                pid = child_id,
                command = %command_for_log(cmd),
                timeout_ms = timeout.as_millis(),
                "wxdb command timed out; terminating process tree"
            );
            terminate_process_tree(&mut child);
            remove_temp_file(&stdout_path);
            remove_temp_file(&stderr_path);
            return Err(std::io::Error::new(
                std::io::ErrorKind::TimedOut,
                format!(
                    "command timed out after {} ms: {}",
                    timeout.as_millis(),
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

fn terminate_and_reap_child(child: &mut Child) -> std::io::Result<()> {
    terminate_process_tree(child);
    child.wait().map(|_| ())
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
    normalized.sort_by(|left, right| {
        left.timestamp
            .cmp(&right.timestamp)
            .then_with(|| left.local_id.cmp(&right.local_id))
    });
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
        local_id: object.get("local_id").and_then(Value::as_i64),
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
            db_dir: None,
            group_name_map: HashMap::new(),
        }
    }

    #[test]
    fn builds_wx_cli_history_command_with_beijing_time_and_all_supported_media() {
        let cmd = build_wx_cli_history_command(
            &wx_cli_config(),
            "测试群",
            Utc.with_ymd_and_hms(2026, 5, 24, 1, 0, 0).unwrap(),
            Utc.with_ymd_and_hms(2026, 5, 24, 2, 0, 0).unwrap(),
            100,
            Some(9001),
        );

        assert_eq!(cmd[0], "wx");
        assert_eq!(cmd[1], "history");
        assert_eq!(cmd[2], "测试群");
        assert!(cmd.contains(&"2026-05-24".to_string()));
        assert!(cmd.contains(&"--json".to_string()));
        assert!(cmd.contains(&"--type".to_string()));
        assert!(cmd.contains(&"all".to_string()));
        let cursor_index = cmd
            .iter()
            .position(|value| value == "--before-local-id")
            .expect("history cursor flag");
        assert_eq!(cmd[cursor_index + 1], "9001");
    }

    #[test]
    fn wait_ready_buffers_events_until_ready() {
        let (sender, receiver) = mpsc::channel();
        let pending_events = Mutex::new(VecDeque::new());
        let event = Wx4pyEvent {
            room_id: "room".into(),
            room_name: Some("Room".into()),
            stable_id: Some("stable-1".into()),
            content: "hello during startup".into(),
            sender_id: Some("sender".into()),
            sender_name: Some("Sender".into()),
            timestamp: 1,
        };
        sender.send(SidecarMessage::Event(event.clone())).unwrap();
        sender.send(SidecarMessage::Ready { ok: true }).unwrap();

        wait_ready(&receiver, &pending_events, 1).unwrap();

        assert_eq!(pending_events.lock().unwrap().pop_front(), Some(event));
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
    fn export_fails_explicitly_when_cursor_pagination_is_requested() {
        let error = query_text_messages_via_export(
            &wx_cli_config(),
            "测试群",
            Utc.with_ymd_and_hms(2026, 5, 24, 1, 0, 0).unwrap(),
            Utc.with_ymd_and_hms(2026, 5, 24, 2, 0, 0).unwrap(),
            100,
            Some(9001),
            Instant::now() + StdDuration::from_secs(1),
            Path::new("out.json"),
        )
        .unwrap_err();

        assert!(error
            .to_string()
            .contains("does not support --before-local-id"));
        assert!(error.to_string().contains("could omit messages"));
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
    fn external_history_cursor_keeps_same_second_messages_stable() {
        let since = Utc.timestamp_opt(100, 0).unwrap();
        let until = Utc.timestamp_opt(200, 0).unwrap();
        let message = |local_id| Wx4pyHistoryMessage {
            local_id: Some(local_id),
            timestamp: until,
            sender_id: "sender".into(),
            sender_name: None,
            content: format!("message-{local_id}"),
            msg_type: "text".into(),
            media_path: None,
            thumbnail_path: None,
            decoded_media_path: None,
            media_decode_error: None,
            is_self: false,
        };

        assert!(message_in_history_window(
            &message(10),
            since,
            until,
            Some(20)
        ));
        assert!(!message_in_history_window(
            &message(20),
            since,
            until,
            Some(20)
        ));
        assert!(!message_in_history_window(
            &message(21),
            since,
            until,
            Some(20)
        ));
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

    #[test]
    fn command_ack_dispatch_matches_concurrent_request_ids() {
        let pending = Mutex::new(std::collections::HashMap::new());
        let (sender_a, receiver_a) = mpsc::channel();
        let (sender_b, receiver_b) = mpsc::channel();
        pending.lock().unwrap().insert("a".to_string(), sender_a);
        pending.lock().unwrap().insert("b".to_string(), sender_b);
        let events = mpsc::channel().0;

        assert!(dispatch_sidecar_message(
            SidecarMessage::CommandResult {
                request_id: "b".to_string(),
                ok: true,
            },
            &pending,
            &events,
        ));
        assert!(dispatch_sidecar_message(
            SidecarMessage::CommandResult {
                request_id: "a".to_string(),
                ok: true,
            },
            &pending,
            &events,
        ));
        assert_eq!(receiver_a.recv().unwrap(), Ok(()));
        assert_eq!(receiver_b.recv().unwrap(), Ok(()));
    }

    #[test]
    fn command_ack_failure_is_delivered_to_matching_waiter() {
        let pending = Mutex::new(std::collections::HashMap::new());
        let (sender, receiver) = mpsc::channel();
        pending.lock().unwrap().insert("failed".to_string(), sender);
        let events = mpsc::channel().0;

        assert!(dispatch_sidecar_message(
            SidecarMessage::CommandError {
                request_id: "failed".to_string(),
                message: "send_message returned false".to_string(),
            },
            &pending,
            &events,
        ));
        assert_eq!(
            receiver.recv().unwrap(),
            Err("send_message returned false".to_string())
        );
    }

    #[test]
    fn command_ack_timeout_removes_waiter_and_marks_delivery_indeterminate() {
        let pending = Mutex::new(std::collections::HashMap::new());
        let (sender, receiver) = mpsc::channel();
        pending
            .lock()
            .unwrap()
            .insert("timed-out".to_string(), sender);

        let error =
            receive_command_ack(receiver, &pending, "timed-out", StdDuration::from_millis(5))
                .unwrap_err();
        assert!(error.to_string().contains("delivery indeterminate"));
        assert!(pending.lock().unwrap().is_empty());

        let events = mpsc::channel().0;
        assert!(dispatch_sidecar_message(
            SidecarMessage::CommandResult {
                request_id: "timed-out".to_string(),
                ok: true,
            },
            &pending,
            &events,
        ));
    }

    #[test]
    fn queue_aware_command_timeout_scales_with_pending_commands() {
        let base = StdDuration::from_secs(30);
        assert_eq!(queue_aware_command_timeout(base, 0), base);
        assert_eq!(
            queue_aware_command_timeout(base, 3),
            StdDuration::from_secs(90)
        );
    }

    #[test]
    fn command_ack_disconnect_removes_waiter_and_marks_delivery_indeterminate() {
        let pending = Mutex::new(std::collections::HashMap::new());
        let (sender, receiver) = mpsc::channel();
        pending.lock().unwrap().insert("closed".to_string(), sender);
        drop(receiver);

        let error = receive_command_ack(
            mpsc::channel().1,
            &pending,
            "closed",
            StdDuration::from_millis(5),
        )
        .unwrap_err();
        assert!(error.to_string().contains("delivery indeterminate"));
        assert!(pending.lock().unwrap().is_empty());
    }

    #[test]
    fn transport_shutdown_fails_pending_sender_and_rejects_new_commands() {
        let transport = Wx4pyTransport {
            stdin: Mutex::new(None),
            pending: Mutex::new(std::collections::HashMap::new()),
            closed: AtomicBool::new(false),
            next_request_id: AtomicU64::new(1),
            command_timeout: StdDuration::from_millis(5),
        };
        let (sender, receiver) = mpsc::channel();
        transport
            .pending
            .lock()
            .unwrap()
            .insert("old-request".into(), sender);

        transport.shutdown();

        assert_eq!(
            receiver.recv_timeout(StdDuration::from_millis(50)).unwrap(),
            Err("wx4py sidecar client is shut down".into())
        );
        let error = transport
            .send_command_blocking(SidecarCommand::SendText {
                request_id: "new-request".into(),
                room: "room".into(),
                text: "text".into(),
            })
            .unwrap_err();
        assert!(error.to_string().contains("client is shut down"));
    }

    #[test]
    fn terminate_and_reap_child_kills_test_child() {
        let mut child = Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "tests::termination_test_child", "--nocapture"])
            .env("WX4PY_CLIENT_TERMINATION_TEST_CHILD", "1")
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .unwrap();
        thread::sleep(StdDuration::from_millis(100));
        assert!(child.try_wait().unwrap().is_none());

        terminate_and_reap_child(&mut child).unwrap();

        assert!(child.try_wait().unwrap().is_some());
    }

    #[test]
    fn termination_test_child() {
        if std::env::var_os("WX4PY_CLIENT_TERMINATION_TEST_CHILD").is_some() {
            loop {
                thread::sleep(StdDuration::from_secs(60));
            }
        }
    }

    #[test]
    fn command_lock_wait_uses_the_same_deadline_as_command_execution() {
        let lock = Arc::new(Mutex::new(()));
        let slow_lock = Arc::clone(&lock);
        let (locked_sender, locked_receiver) = mpsc::channel();
        let slow_request = thread::spawn(move || {
            let _guard = slow_lock.lock().unwrap();
            locked_sender.send(()).unwrap();
            thread::sleep(StdDuration::from_millis(150));
        });
        locked_receiver.recv().unwrap();

        let started = Instant::now();
        let error = run_command_with_deadline_on_lock(
            &lock,
            &["this-command-must-not-start".to_string()],
            started + StdDuration::from_millis(40),
            &[],
        )
        .unwrap_err();
        let lock_wait_elapsed = started.elapsed();

        slow_request.join().unwrap();
        assert_eq!(error.kind(), std::io::ErrorKind::TimedOut);
        assert!(lock_wait_elapsed < StdDuration::from_millis(120));
    }
}
