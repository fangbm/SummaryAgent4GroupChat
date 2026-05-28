use std::{
    collections::HashMap,
    env, fs,
    io::{BufRead, BufReader, Write},
    path::{Path, PathBuf},
    process::{Child, ChildStdin, Command, Output, Stdio},
    sync::{
        mpsc::{self, Receiver, RecvTimeoutError},
        Arc, Mutex,
    },
    thread,
    time::{Duration as StdDuration, Instant},
};

use chrono::{DateTime, Duration, NaiveDateTime, TimeZone, Utc};
use rusqlite::{params, Connection};
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
    #[error("sqlite error: {0}")]
    Sqlite(#[from] rusqlite::Error),
    #[error("sidecar failed: {0}")]
    Sidecar(String),
    #[error("wx4py sidecar did not become ready within {0} seconds")]
    ReadyTimeout(u64),
    #[error("wx4py requires at least one group name in [wx4py].groups or listen.whitelist_rooms")]
    MissingGroups,
    #[error("wx-cli failed: {0}")]
    WxCli(String),
    #[error("invalid wx-cli response: {0}")]
    InvalidWxCli(String),
    #[error("wx-cli cache lookup failed: {0}")]
    WxCliCache(String),
}

pub type Result<T> = std::result::Result<T, Wx4pyError>;

#[derive(Debug)]
pub struct Wx4pyClient {
    _child: Child,
    stdin: Arc<Mutex<ChildStdin>>,
    receiver: Receiver<SidecarMessage>,
    wx_cli: WxCliConfig,
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
            .spawn()?;

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

    pub async fn query_text_messages(
        &self,
        room_id: &str,
        room_name: Option<&str>,
        since: DateTime<Utc>,
        until: DateTime<Utc>,
        limit: u32,
    ) -> Result<Vec<Wx4pyHistoryMessage>> {
        let chat_name = self.chat_name(room_id, room_name);
        let output = self.temp_output_path(room_id);
        if let Some(parent) = output.parent() {
            fs::create_dir_all(parent)?;
        }
        let cmd =
            build_wx_cli_export_command(&self.wx_cli, &chat_name, since, until, limit, &output);
        let output_result = match run_command_with_timeout(&cmd, self.wx_cli.timeout_seconds) {
            Ok(output) => output,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                match query_wx_cli_cache_messages(
                    &chat_name,
                    since,
                    until,
                    limit.min(self.wx_cli.max_messages),
                ) {
                    Ok(messages) => {
                        eprintln!(
                            "wx-cli executable not found ({}); using wx-cli cache fallback with {} messages",
                            cmd[0],
                            messages.len()
                        );
                        return Ok(messages);
                    }
                    Err(cache_error) => {
                        return Err(Wx4pyError::WxCli(format!(
                            "wx-cli executable not found: {}. Set [wx_cli].executable to the absolute path of wx.exe or add it to PATH. cache fallback failed: {}",
                            cmd[0], cache_error
                        )));
                    }
                }
            }
            Err(error) if error.kind() == std::io::ErrorKind::TimedOut => {
                match query_wx_cli_cache_messages(
                    &chat_name,
                    since,
                    until,
                    limit.min(self.wx_cli.max_messages),
                ) {
                    Ok(messages) => {
                        eprintln!(
                            "wx-cli export timed out after {} seconds; using wx-cli cache fallback with {} messages",
                            self.wx_cli.timeout_seconds,
                            messages.len()
                        );
                        return Ok(messages);
                    }
                    Err(cache_error) => {
                        return Err(Wx4pyError::WxCli(format!(
                            "wx-cli export timed out after {} seconds; cache fallback failed: {}",
                            self.wx_cli.timeout_seconds, cache_error
                        )));
                    }
                }
            }
            Err(error) => return Err(Wx4pyError::Io(error)),
        };
        if !output_result.status.success() {
            let cli_error = String::from_utf8_lossy(&output_result.stderr)
                .trim()
                .to_string();
            if let Ok(messages) = query_wx_cli_cache_messages(
                &chat_name,
                since,
                until,
                limit.min(self.wx_cli.max_messages),
            ) {
                eprintln!(
                    "wx-cli export failed; using cache fallback with {} messages: {}",
                    messages.len(),
                    cli_error
                );
                return Ok(messages);
            }
            return Err(Wx4pyError::WxCli(cli_error));
        }

        let text = fs::read_to_string(&output)?;
        let _ = fs::remove_file(&output);
        let mut messages = normalize_wx_cli_messages(&text)?;
        messages.retain(|message| message.timestamp >= since && message.timestamp <= until);
        if messages.is_empty() {
            if let Ok(cache_messages) = query_wx_cli_cache_messages(
                &chat_name,
                since,
                until,
                limit.min(self.wx_cli.max_messages),
            ) {
                eprintln!(
                    "wx-cli export returned empty; using cache fallback with {} messages",
                    cache_messages.len()
                );
                return Ok(cache_messages);
            }
        }
        Ok(messages)
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
            "wx4py-wxcli-{}-{}.json",
            safe_room,
            Utc::now().timestamp_millis()
        ))
    }
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
        limit.min(config.max_messages).to_string(),
    ]
}

fn run_command_with_timeout(cmd: &[String], timeout_seconds: u64) -> std::io::Result<Output> {
    let mut child = Command::new(&cmd[0])
        .args(&cmd[1..])
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()?;
    let timeout = StdDuration::from_secs(timeout_seconds.max(1));
    let started = Instant::now();
    loop {
        if child.try_wait()?.is_some() {
            return child.wait_with_output();
        }
        if started.elapsed() >= timeout {
            let _ = child.kill();
            let _ = child.wait();
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
    if msg_type != "text" {
        return Ok(None);
    }

    Ok(Some(Wx4pyHistoryMessage {
        timestamp: parse_timestamp(object)?,
        sender_id: string_field(object, &["sender_id", "sender_username", "sender"])
            .unwrap_or_else(|| "unknown".into()),
        sender_name: string_field(object, &["sender_name", "sender"]),
        content,
        msg_type,
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

fn query_wx_cli_cache_messages(
    chat_name: &str,
    since: DateTime<Utc>,
    until: DateTime<Utc>,
    limit: u32,
) -> Result<Vec<Wx4pyHistoryMessage>> {
    let cache_dir = wx_cli_cache_dir()?;
    let username = resolve_cache_username(&cache_dir, chat_name)?;
    let message_table = format!("Msg_{:x}", md5::compute(username.as_bytes()));
    let contact_names = load_contact_display_names(&cache_dir)?;

    for db_path in cache_db_paths(&cache_dir)? {
        let conn = match Connection::open(&db_path) {
            Ok(conn) => conn,
            Err(_) => continue,
        };
        if !table_exists(&conn, "Name2Id")? || !table_exists(&conn, &message_table)? {
            continue;
        }
        let sender_map = load_sender_map(&conn)?;
        let mut stmt = conn.prepare(&format!(
            "select real_sender_id, create_time, message_content from [{message_table}]
             where create_time >= ?1 and create_time <= ?2
               and typeof(message_content) = 'text'
             order by create_time asc
             limit ?3"
        ))?;
        let rows = stmt.query_map(
            params![since.timestamp(), until.timestamp(), limit as i64],
            |row| {
                let real_sender_id: i64 = row.get(0)?;
                let create_time: i64 = row.get(1)?;
                let raw_content: String = row.get(2)?;
                Ok((real_sender_id, create_time, raw_content))
            },
        )?;

        let mut messages = Vec::new();
        for row in rows {
            let (real_sender_id, create_time, raw_content) = row?;
            let fallback_sender = sender_map
                .get(&real_sender_id)
                .cloned()
                .unwrap_or_else(|| "unknown".to_string());
            let (sender_id, content) =
                split_group_sender(&raw_content).unwrap_or((fallback_sender, raw_content));
            let sender_name = contact_names
                .get(&sender_id)
                .filter(|name| name.as_str() != sender_id)
                .cloned();
            if content.trim().is_empty() {
                continue;
            }
            let Some(timestamp) = Utc.timestamp_opt(create_time, 0).single() else {
                continue;
            };
            messages.push(Wx4pyHistoryMessage {
                timestamp,
                sender_id,
                sender_name,
                content,
                msg_type: "text".into(),
                is_self: false,
            });
        }
        return Ok(messages);
    }

    Err(Wx4pyError::WxCliCache(format!(
        "message table {message_table} for {username} not found in wx-cli cache"
    )))
}

fn wx_cli_cache_dir() -> Result<PathBuf> {
    let home = env::var("USERPROFILE")
        .or_else(|_| env::var("HOME"))
        .map_err(|_| Wx4pyError::WxCliCache("USERPROFILE/HOME is not set".into()))?;
    let cache_dir = PathBuf::from(home).join(".wx-cli").join("cache");
    if cache_dir.is_dir() {
        Ok(cache_dir)
    } else {
        Err(Wx4pyError::WxCliCache(format!(
            "cache directory does not exist: {}",
            cache_dir.display()
        )))
    }
}

fn resolve_cache_username(cache_dir: &Path, chat_name: &str) -> Result<String> {
    if chat_name.contains("@chatroom") || chat_name.starts_with("wxid_") {
        return Ok(chat_name.to_string());
    }

    for db_path in cache_db_paths(cache_dir)? {
        let conn = match Connection::open(&db_path) {
            Ok(conn) => conn,
            Err(_) => continue,
        };
        if !table_exists(&conn, "contact")? {
            continue;
        }
        let mut stmt = conn.prepare(
            "select username from contact
             where username = ?1 or nick_name = ?1 or remark = ?1
             limit 1",
        )?;
        let mut rows = stmt.query(params![chat_name])?;
        if let Some(row) = rows.next()? {
            return row.get(0).map_err(Into::into);
        }
    }

    Err(Wx4pyError::WxCliCache(format!(
        "chat {chat_name} not found in wx-cli contact cache"
    )))
}

fn cache_db_paths(cache_dir: &Path) -> Result<Vec<PathBuf>> {
    Ok(fs::read_dir(cache_dir)?
        .filter_map(|entry| entry.ok().map(|entry| entry.path()))
        .filter(|path| path.extension().is_some_and(|ext| ext == "db"))
        .collect())
}

fn table_exists(conn: &Connection, table: &str) -> Result<bool> {
    let exists = conn.query_row(
        "select exists(select 1 from sqlite_master where type='table' and name=?1)",
        params![table],
        |row| row.get::<_, i64>(0),
    )?;
    Ok(exists != 0)
}

fn load_sender_map(conn: &Connection) -> Result<HashMap<i64, String>> {
    let mut stmt = conn.prepare("select rowid, user_name from Name2Id")?;
    let rows = stmt.query_map([], |row| Ok((row.get(0)?, row.get(1)?)))?;
    let mut map = HashMap::new();
    for row in rows {
        let (id, username): (i64, String) = row?;
        map.insert(id, username);
    }
    Ok(map)
}

fn load_contact_display_names(cache_dir: &Path) -> Result<HashMap<String, String>> {
    let mut names = HashMap::new();
    for db_path in cache_db_paths(cache_dir)? {
        let conn = match Connection::open(&db_path) {
            Ok(conn) => conn,
            Err(_) => continue,
        };
        if !table_exists(&conn, "contact")? {
            continue;
        }
        let mut stmt = conn.prepare("select username, remark, nick_name, alias from contact")?;
        let rows = stmt.query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, Option<String>>(1)?,
                row.get::<_, Option<String>>(2)?,
                row.get::<_, Option<String>>(3)?,
            ))
        })?;
        for row in rows {
            let (username, remark, nick_name, alias) = row?;
            if let Some(display) =
                display_name_from_contact_fields(&username, remark, nick_name, alias)
            {
                names.insert(username, display);
            }
        }
    }
    Ok(names)
}

fn display_name_from_contact_fields(
    username: &str,
    remark: Option<String>,
    nick_name: Option<String>,
    alias: Option<String>,
) -> Option<String> {
    [remark, nick_name, alias]
        .into_iter()
        .flatten()
        .map(|value| value.trim().to_string())
        .find(|value| !value.is_empty() && value != username)
}

fn split_group_sender(raw_content: &str) -> Option<(String, String)> {
    let (sender, content) = raw_content.split_once(":\n")?;
    if sender.trim().is_empty() || content.trim().is_empty() {
        return None;
    }
    Some((sender.to_string(), content.to_string()))
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    fn wx_cli_config() -> WxCliConfig {
        WxCliConfig {
            executable: "wx".into(),
            export_format: "json".into(),
            max_messages: 500,
            timeout_seconds: 20,
            temp_dir: ".\\runtime\\wx-exports".into(),
            group_name_map: HashMap::new(),
        }
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
    fn contact_display_name_prefers_remark_then_nickname_then_alias() {
        assert_eq!(
            display_name_from_contact_fields(
                "wxid_abc",
                Some("  群友备注  ".to_string()),
                Some("昵称".to_string()),
                Some("alias".to_string()),
            ),
            Some("群友备注".to_string())
        );
        assert_eq!(
            display_name_from_contact_fields(
                "wxid_abc",
                Some("".to_string()),
                Some("昵称".to_string()),
                Some("alias".to_string()),
            ),
            Some("昵称".to_string())
        );
        assert_eq!(
            display_name_from_contact_fields(
                "wxid_abc",
                Some("wxid_abc".to_string()),
                None,
                Some("alias".to_string()),
            ),
            Some("alias".to_string())
        );
    }
}
