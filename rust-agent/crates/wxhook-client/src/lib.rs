use std::{
    io::{ErrorKind, Read, Write},
    net::{TcpListener, TcpStream},
    time::Duration,
};

use chrono::{DateTime, TimeZone, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum WxHookError {
    #[error("http error: {0}")]
    Http(#[from] reqwest::Error),
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("json error: {0}")]
    Json(#[from] serde_json::Error),
    #[error("wxhook API failed: code={code}, msg={msg}")]
    Api { code: i32, msg: String },
    #[error("invalid response: {0}")]
    InvalidResponse(String),
}

pub type Result<T> = std::result::Result<T, WxHookError>;

#[derive(Debug, Clone)]
pub struct WxHookClient {
    base_url: String,
    client: reqwest::Client,
}

impl WxHookClient {
    pub fn new(base_url: impl Into<String>, timeout_ms: u64) -> Result<Self> {
        let client = reqwest::Client::builder()
            .timeout(Duration::from_millis(timeout_ms))
            .build()?;
        Ok(Self {
            base_url: base_url.into().trim_end_matches('/').to_string(),
            client,
        })
    }

    pub async fn check_login(&self) -> Result<bool> {
        let response = self.post_empty("/api/checkLogin").await?;
        Ok(response_is_success(&response))
    }

    pub async fn hook_sync_msg(&self, config: HookSyncMsgRequest) -> Result<ApiResponse> {
        self.post_json("/api/hookSyncMsg", &config).await
    }

    pub async fn unhook_sync_msg(&self) -> Result<ApiResponse> {
        self.post_empty("/api/unhookSyncMsg").await
    }

    pub async fn send_text(&self, wxid: &str, msg: &str) -> Result<ApiResponse> {
        self.post_json(
            "/api/sendTextMsg",
            &serde_json::json!({ "wxid": wxid, "msg": msg }),
        )
        .await
    }

    pub async fn send_image(&self, wxid: &str, image_path: &str) -> Result<ApiResponse> {
        self.post_json(
            "/api/sendImagesMsg",
            &serde_json::json!({ "wxid": wxid, "imagePath": image_path }),
        )
        .await
    }

    pub async fn get_db_info(&self) -> Result<Vec<DbInfo>> {
        let value = self
            .post_value("/api/getDBInfo", &serde_json::json!({}))
            .await?;
        serde_json::from_value(api_data(value)?).map_err(WxHookError::Json)
    }

    pub async fn exec_sql(&self, db_handle: i64, sql: &str) -> Result<ApiResponse> {
        self.post_json(
            "/api/execSql",
            &serde_json::json!({ "dbHandle": db_handle, "sql": sql }),
        )
        .await
    }

    pub async fn query_text_messages(
        &self,
        room_id: &str,
        since_ts: i64,
        until_ts: i64,
        limit: u32,
    ) -> Result<Vec<WxHookHistoryMessage>> {
        let mut messages = Vec::new();
        let sql = chat_history_sql(room_id, since_ts, until_ts, limit);

        for db in self
            .get_db_info()
            .await?
            .into_iter()
            .filter(db_has_msg_table)
        {
            let response = self.exec_sql(db.handle, &sql).await?;
            let rows = SqlRows::from_value(response.data)?;
            for row in rows.iter_rows() {
                if let Some(message) = history_message_from_row(&rows.columns, row) {
                    messages.push(message);
                }
            }
        }

        messages.sort_by_key(|message| message.timestamp);
        messages.dedup_by(|left, right| {
            left.timestamp == right.timestamp
                && left.sender_id == right.sender_id
                && left.content == right.content
        });
        if messages.len() > limit as usize {
            messages = messages.split_off(messages.len() - limit as usize);
        }
        Ok(messages)
    }

    async fn post_empty(&self, path: &str) -> Result<ApiResponse> {
        self.post_json(path, &serde_json::json!({})).await
    }

    async fn post_json<T: Serialize + ?Sized>(&self, path: &str, body: &T) -> Result<ApiResponse> {
        let response = self.post_value(path, body).await?;
        let parsed: ApiResponse = serde_json::from_value(response)?;
        if response_ok(parsed.code) {
            Ok(parsed)
        } else {
            Err(WxHookError::Api {
                code: parsed.code,
                msg: parsed.msg,
            })
        }
    }

    async fn post_value<T: Serialize + ?Sized>(&self, path: &str, body: &T) -> Result<Value> {
        Ok(self
            .client
            .post(format!("{}{}", self.base_url, path))
            .json(body)
            .send()
            .await?
            .error_for_status()?
            .json::<Value>()
            .await?)
    }
}

fn response_ok(code: i32) -> bool {
    matches!(code, 0 | 1 | 2 | 200)
}

fn api_data(value: Value) -> Result<Value> {
    let Some(object) = value.as_object() else {
        return Ok(value);
    };

    let Some(code) = object.get("code").and_then(Value::as_i64) else {
        return Ok(value);
    };

    if !response_ok(code as i32) {
        return Err(WxHookError::Api {
            code: code as i32,
            msg: object
                .get("msg")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_string(),
        });
    }

    Ok(object.get("data").cloned().unwrap_or(Value::Null))
}

fn response_is_success(response: &ApiResponse) -> bool {
    if !response_ok(response.code) {
        return false;
    }

    if let Some(value) = response.data.as_bool() {
        return value;
    }

    if let Some(object) = response.data.as_object() {
        for key in ["status", "success", "isLogin", "login"] {
            if let Some(value) = object.get(key).and_then(Value::as_bool) {
                return value;
            }
        }
    }

    true
}

#[derive(Debug, Clone, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct HookSyncMsgRequest {
    pub ip: String,
    pub port: u16,
    pub enable_http: i32,
    pub url: String,
    pub timeout: u64,
}

impl HookSyncMsgRequest {
    pub fn tcp(ip: impl Into<String>, port: u16, timeout: u64) -> Self {
        Self {
            ip: ip.into(),
            port,
            enable_http: 0,
            url: "http://127.0.0.1:8000".to_string(),
            timeout,
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq)]
pub struct ApiResponse {
    pub code: i32,
    #[serde(default)]
    pub data: Value,
    #[serde(default)]
    pub msg: String,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct WxHookEvent {
    #[serde(default)]
    pub content: Option<Value>,
    #[serde(default)]
    pub create_time: Option<i64>,
    #[serde(default)]
    pub display_full_content: Option<String>,
    #[serde(default)]
    pub from_user: Option<String>,
    #[serde(default)]
    pub msg_id: Option<i64>,
    #[serde(default)]
    pub msg_sequence: Option<i64>,
    #[serde(default)]
    pub pid: Option<i64>,
    #[serde(default)]
    pub signature: Option<Value>,
    #[serde(default)]
    pub to_user: Option<String>,
    #[serde(default, rename = "type")]
    pub msg_type: Option<i32>,
}

impl WxHookEvent {
    pub fn timestamp(&self) -> Option<DateTime<Utc>> {
        Utc.timestamp_opt(self.create_time?, 0).single()
    }

    pub fn room_id(&self) -> Option<&str> {
        self.from_user
            .as_deref()
            .filter(|value| value.ends_with("@chatroom"))
            .or_else(|| {
                self.to_user
                    .as_deref()
                    .filter(|value| value.ends_with("@chatroom"))
            })
    }

    pub fn text_body(&self) -> Option<WxHookTextBody> {
        if self.msg_type != Some(1) {
            return None;
        }
        let raw = value_to_string(self.content.as_ref()?)
            .or_else(|| self.display_full_content.clone())?;
        let (sender, content) = split_group_content(&raw);
        Some(WxHookTextBody { sender, content })
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct WxHookTextBody {
    pub sender: Option<String>,
    pub content: String,
}

#[derive(Debug, Clone, PartialEq)]
pub struct WxHookHistoryMessage {
    pub timestamp: DateTime<Utc>,
    pub sender_id: String,
    pub sender_name: Option<String>,
    pub content: String,
    pub msg_type: String,
    pub is_self: bool,
}

#[derive(Debug)]
pub struct WxHookEventListener {
    listener: TcpListener,
}

impl WxHookEventListener {
    pub fn bind(host: &str, port: u16) -> Result<Self> {
        Ok(Self {
            listener: TcpListener::bind((host, port))?,
        })
    }

    pub fn next_event(&self) -> Result<WxHookEvent> {
        let (mut stream, _) = self.listener.accept()?;
        let data = read_event_payload(&mut stream)?;
        stream.write_all(b"200 OK")?;
        Ok(serde_json::from_slice(&data)?)
    }
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct DbInfo {
    pub database_name: String,
    pub handle: i64,
    #[serde(default)]
    pub tables: Vec<DbTable>,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct DbTable {
    pub name: Option<String>,
    pub rootpage: Option<String>,
    pub sql: Option<String>,
    pub table_name: Option<String>,
}

pub fn chat_history_sql(room_id: &str, since_ts: i64, until_ts: i64, limit: u32) -> String {
    let escaped_room = room_id.replace('\'', "''");
    format!(
        "select CreateTime, StrTalker, StrContent, DisplayContent, Type, IsSender from MSG where StrTalker = '{escaped_room}' and CreateTime >= {since_ts} and CreateTime <= {until_ts} and Type = 1 order by CreateTime asc limit {limit}"
    )
}

#[derive(Debug, Clone, PartialEq)]
struct SqlRows {
    columns: Vec<String>,
    rows: Vec<Vec<Value>>,
}

impl SqlRows {
    fn from_value(value: Value) -> Result<Self> {
        let Value::Array(items) = value else {
            return Err(WxHookError::InvalidResponse(
                "execSql data is not an array".to_string(),
            ));
        };

        if items.is_empty() {
            return Ok(Self {
                columns: Vec::new(),
                rows: Vec::new(),
            });
        }

        if let Some(first_row) = items.first().and_then(Value::as_array) {
            let columns = first_row
                .iter()
                .map(cell_to_string)
                .collect::<Option<Vec<_>>>()
                .ok_or_else(|| {
                    WxHookError::InvalidResponse("execSql header row is invalid".to_string())
                })?;
            let rows = items
                .into_iter()
                .skip(1)
                .filter_map(|item| match item {
                    Value::Array(row) => Some(row),
                    _ => None,
                })
                .collect::<Vec<_>>();
            return Ok(Self { columns, rows });
        }

        if items.first().and_then(Value::as_object).is_some() {
            let mut columns = items
                .iter()
                .filter_map(Value::as_object)
                .flat_map(|object| object.keys().cloned())
                .collect::<Vec<_>>();
            columns.sort();
            columns.dedup();
            let rows = items
                .into_iter()
                .filter_map(|item| {
                    let object = item.as_object()?;
                    Some(
                        columns
                            .iter()
                            .map(|column| object.get(column).cloned().unwrap_or(Value::Null))
                            .collect::<Vec<_>>(),
                    )
                })
                .collect::<Vec<_>>();
            return Ok(Self { columns, rows });
        }

        Err(WxHookError::InvalidResponse(
            "execSql rows are not arrays or objects".to_string(),
        ))
    }

    fn iter_rows(&self) -> impl Iterator<Item = &[Value]> {
        self.rows.iter().map(Vec::as_slice)
    }
}

fn db_has_msg_table(db: &DbInfo) -> bool {
    db.database_name.to_ascii_lowercase().starts_with("msg")
        || db.tables.iter().any(|table| {
            table
                .table_name
                .as_ref()
                .or(table.name.as_ref())
                .is_some_and(|name| name.eq_ignore_ascii_case("MSG"))
        })
}

fn history_message_from_row(columns: &[String], row: &[Value]) -> Option<WxHookHistoryMessage> {
    let create_time = row_value(columns, row, "CreateTime").and_then(cell_to_i64)?;
    let msg_type = row_value(columns, row, "Type")
        .and_then(cell_to_i64)
        .unwrap_or(1);
    if msg_type != 1 {
        return None;
    }

    let is_self = row_value(columns, row, "IsSender")
        .and_then(cell_to_i64)
        .is_some_and(|value| value == 1);
    let raw_content = row_value(columns, row, "StrContent")
        .and_then(cell_to_string)
        .or_else(|| row_value(columns, row, "DisplayContent").and_then(cell_to_string))?;
    let (sender, content) = if is_self {
        (Some("self".to_string()), raw_content.trim().to_string())
    } else {
        split_group_content(&raw_content)
    };
    if content.trim().is_empty() {
        return None;
    }

    Some(WxHookHistoryMessage {
        timestamp: Utc.timestamp_opt(create_time, 0).single()?,
        sender_id: sender.unwrap_or_else(|| "unknown".to_string()),
        sender_name: is_self.then(|| "我".to_string()),
        content,
        msg_type: "text".to_string(),
        is_self,
    })
}

fn row_value<'a>(columns: &[String], row: &'a [Value], name: &str) -> Option<&'a Value> {
    let index = columns
        .iter()
        .position(|column| column.eq_ignore_ascii_case(name))?;
    row.get(index)
}

fn cell_to_string(value: &Value) -> Option<String> {
    match value {
        Value::String(text) => Some(text.clone()),
        Value::Number(number) => Some(number.to_string()),
        Value::Bool(value) => Some(value.to_string()),
        Value::Null => Some(String::new()),
        _ => None,
    }
}

fn cell_to_i64(value: &Value) -> Option<i64> {
    match value {
        Value::Number(number) => number.as_i64(),
        Value::String(text) => text.parse().ok(),
        _ => None,
    }
}

fn read_event_payload(stream: &mut TcpStream) -> Result<Vec<u8>> {
    stream.set_read_timeout(Some(Duration::from_millis(500)))?;
    let mut data = Vec::new();
    let mut buffer = [0_u8; 1024];
    loop {
        match stream.read(&mut buffer) {
            Ok(0) => break,
            Ok(read) => {
                data.extend_from_slice(&buffer[..read]);
                if payload_has_complete_json(&data) || buffer[..read].last() == Some(&b'\n') {
                    break;
                }
            }
            Err(error) if matches!(error.kind(), ErrorKind::WouldBlock | ErrorKind::TimedOut) => {
                if data.is_empty() {
                    return Err(error.into());
                }
                break;
            }
            Err(error) => return Err(error.into()),
        }
    }
    Ok(data)
}

fn payload_has_complete_json(data: &[u8]) -> bool {
    serde_json::from_slice::<Value>(data).is_ok()
}

fn value_to_string(value: &Value) -> Option<String> {
    match value {
        Value::String(text) => Some(text.clone()),
        Value::Object(map) => map
            .get("#text")
            .and_then(value_to_string)
            .or_else(|| map.get("text").and_then(value_to_string)),
        _ => None,
    }
}

fn split_group_content(raw: &str) -> (Option<String>, String) {
    if let Some((sender, content)) = raw.split_once(":\n") {
        if !sender.trim().is_empty() {
            return (Some(sender.trim().to_string()), content.trim().to_string());
        }
    }
    (None, raw.trim().to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{
        net::TcpStream,
        thread,
        time::{Duration, Instant},
    };

    #[test]
    fn hook_request_uses_wxhook_field_names() {
        let value = serde_json::to_value(HookSyncMsgRequest::tcp("127.0.0.1", 18999, 30)).unwrap();
        assert_eq!(value["enableHttp"], 0);
        assert_eq!(value["port"], 18999);
    }

    #[test]
    fn parses_group_text_event() {
        let event: WxHookEvent = serde_json::from_str(
            r#"{"type":1,"fromUser":"123@chatroom","toUser":"self","content":"wxid_a:\n/总结 今天聊了什么","createTime":1716464700}"#,
        )
        .unwrap();
        assert_eq!(event.room_id(), Some("123@chatroom"));
        let body = event.text_body().unwrap();
        assert_eq!(body.sender.as_deref(), Some("wxid_a"));
        assert_eq!(body.content, "/总结 今天聊了什么");
    }

    #[test]
    fn parses_self_sent_group_text_event() {
        let event: WxHookEvent = serde_json::from_str(
            r#"{"type":1,"fromUser":"wxid_self","toUser":"123@chatroom","content":"/总结","createTime":1716464700}"#,
        )
        .unwrap();
        assert_eq!(event.room_id(), Some("123@chatroom"));
        let body = event.text_body().unwrap();
        assert_eq!(body.sender, None);
        assert_eq!(body.content, "/总结");
    }

    #[test]
    fn history_sql_escapes_room_id() {
        let sql = chat_history_sql("room'1@chatroom", 10, 20, 100);
        assert!(sql.contains("room''1@chatroom"));
        assert!(sql.contains("Type = 1"));
        assert!(sql.contains("limit 100"));
    }

    #[test]
    fn unwraps_wrapped_api_data() {
        let value = serde_json::json!({"code": 1, "data": [1, 2], "msg": "success"});
        assert_eq!(api_data(value).unwrap(), serde_json::json!([1, 2]));
    }

    #[test]
    fn parses_exec_sql_header_rows() {
        let rows = SqlRows::from_value(serde_json::json!([
            ["CreateTime", "StrContent", "Type", "IsSender"],
            ["1716464700", "wxid_a:\nhello", "1", "0"]
        ]))
        .unwrap();

        assert_eq!(rows.columns[0], "CreateTime");
        assert_eq!(rows.rows.len(), 1);
    }

    #[test]
    fn parses_history_message_from_sql_row() {
        let rows = SqlRows::from_value(serde_json::json!([
            [
                "CreateTime",
                "StrTalker",
                "StrContent",
                "DisplayContent",
                "Type",
                "IsSender"
            ],
            [
                "1716464700",
                "room@chatroom",
                "wxid_a:\nhello",
                "",
                "1",
                "0"
            ]
        ]))
        .unwrap();

        let message = history_message_from_row(&rows.columns, &rows.rows[0]).unwrap();

        assert_eq!(message.sender_id, "wxid_a");
        assert_eq!(message.content, "hello");
        assert!(!message.is_self);
    }

    #[test]
    fn wxhook_success_codes_include_zero_one_two_and_200() {
        assert!(response_ok(0));
        assert!(response_ok(1));
        assert!(response_ok(2));
        assert!(response_ok(200));
        assert!(!response_ok(500));
    }

    #[test]
    fn check_login_accepts_wxhook_success_without_data() {
        let response = ApiResponse {
            code: 0,
            data: Value::Null,
            msg: "success".to_string(),
        };
        assert!(response_is_success(&response));
    }

    #[test]
    fn check_login_respects_explicit_false_data() {
        let response = ApiResponse {
            code: 0,
            data: Value::Bool(false),
            msg: "success".to_string(),
        };
        assert!(!response_is_success(&response));
    }

    #[test]
    fn reads_complete_json_without_waiting_for_socket_close() {
        let listener = TcpListener::bind(("127.0.0.1", 0)).unwrap();
        let addr = listener.local_addr().unwrap();
        let client = thread::spawn(move || {
            let mut stream = TcpStream::connect(addr).unwrap();
            stream
                .write_all(br#"{"type":1,"fromUser":"room@chatroom","content":"wxid_a:\n/summary","createTime":1716464700}"#)
                .unwrap();
            thread::sleep(Duration::from_secs(2));
        });

        let (mut stream, _) = listener.accept().unwrap();
        let started = Instant::now();
        let payload = read_event_payload(&mut stream).unwrap();

        assert!(started.elapsed() < Duration::from_secs(1));
        assert!(payload_has_complete_json(&payload));
        client.join().unwrap();
    }
}
